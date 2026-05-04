# FSFT Manifest Repair — Low-Level Design

This document is the implementation-level companion to `fsft-manifest-repair.md`. It describes the
exact classes, methods, control flow, data structures, error semantics, and on-disk artifacts
introduced or touched by the offline FILE store-file-tracker manifest repair.

> Scope of this LLD
>
> - One store at a time: `table + region + family`.
> - Offline / operator-driven via the existing `sft` tool.
> - Two repair modes: `disk-only` and `lineage-assisted`.
> - No new RPC, no master integration, no online HBCK plumbing.

---

## 1. Background (just enough to read the code)

For a store using the FILE tracker, store membership is persisted in a small protobuf file under:

```
<root>/data/<ns>/<table>/<region>/<family>/.filelist/{f1|f2}.<seqId>
```

```
<root>/data/<ns>/<table>/<region>/<family>/file1,file2
```

`StoreFileListFile` keeps **two** rotating tracker files (`f1.*`, `f2.*`) per `seqId`. The
selection algorithm at load time:

1. List `.filelist`, group by `seqId` (descending).
2. For the highest `seqId`, try to load up to two files.
3. Tolerate `EOFException` (truncated write).
4. Anything else — checksum, parse, version mismatch, > 2 files at the same `seqId` — bubbles out
  as `IOException` / `DoNotRetryIOException` and the store fails to open.

For FILE SFT, two kinds of entries can exist **only** inside the manifest, not as a placeholder
file in the family directory:

- **Virtual `Reference`** — created during split (when a daughter only owns a half) and merge
(whole-file top reference). The `Reference` payload (`splitkey`, `range`) is stored only in the
manifest entry. `FileBasedStoreFileTracker.createReference` does not touch the FS.
- **Virtual `HFileLink`** — created during split when a daughter can point at the whole parent
file. The link entry is in the manifest and a backref is created in the archive directory, but
no placeholder file lives in the family directory.

Implication: a naive "list the family directory and rebuild" repair is **not** safe for FILE SFT
stores that ever held virtual entries.

---

## 2. Public artifacts

### 2.1 Files added


| Path                                                                                                            | Purpose                                                              |
| --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/storefiletracker/StoreFileListRepair.java`     | Reusable helper that diagnoses, recomputes, and writes the manifest. |
| `hbase-server/src/test/java/org/apache/hadoop/hbase/regionserver/storefiletracker/TestStoreFileListRepair.java` | Focused unit tests.                                                  |
| `dev-support/design-docs/fsft-manifest-repair.md`                                                               | High-level design (already exists).                                  |
| `dev-support/design-docs/fsft-manifest-repair-lld.md`                                                           | This document.                                                       |


### 2.2 Files modified


| Path                                  | Change                                                                                                                                                                              |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `StoreFileListFile.java`              | New package-private `Path writeNew(StoreFileList.Builder)`.                                                                                                                         |
| `StoreFileListFilePrettyPrinter.java` | New CLI flags: `--repair`, `--repair-mode`, `--dry-run`, `--region-offline`, `--force-meta`. New code path that delegates to `StoreFileListRepair.repair(...)` and prints a report. |


No public API changes. All new types are package-private.

---

## 3. Architecture overview

```
                    ┌───────────────────────────────────────────────────┐
   operator ───►    │ StoreFileListFilePrettyPrinter (Tool, sft CLI)    │
   sft --repair...  │  · arg parsing & guards                           │
                    │  · resolve TableDescriptor / ColumnFamilyDescriptor│
                    │  · resolve RegionInfo / HRegionFileSystem         │
                    │  · resolve Lineage (meta scan, lineage-assisted)  │
                    └───────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                    ┌───────────────────────────────────────────────────┐
                    │ StoreFileListRepair.repair(...)                   │
                    │  1) diagnoseTrackerFiles()                        │
                    │  2) loadStoreFilesFromDisk()                      │
                    │  3) loadStoreFilesFromLineage() [optional]        │
                    │  4) unionStoreFileEntries()                       │
                    │  5) isAlreadyHealthy()  → no-op?                  │
                    │  6) StoreFileListFile.writeNew(...)               │
                    └───────────────────────┬───────────────────────────┘
                                            │
                                            ▼
                    ┌───────────────────────────────────────────────────┐
                    │ StoreFileListFile.writeNew(builder)               │
                    │  - pick seqId = max(now, highestSeqId+1)          │
                    │  - write f1.<seqId> with version + crc32          │
                    │  - reset internal load state                      │
                    └───────────────────────────────────────────────────┘
```

The repair does **not** run any region procedure, does not contact any master, and does not
modify `hbase:meta`. It only reads (a) the family directory and any lineage parents on FS, and
(b) `hbase:meta` (read-only) when lineage is requested.

---

## 4. Class-level design

### 4.1 `StoreFileListRepair`

`@InterfaceAudience.Private`, `final class`, package-private. Stateless helper composed of static
methods. Lives next to `StoreFileListFile`.

```
StoreFileListRepair
├── enum Mode { DISK_ONLY, LINEAGE_ASSISTED }
├── static Lineage  Lineage.none()
│                   Lineage.splitParent(RegionInfo)
│                   Lineage.mergeParents(List<RegionInfo>)
├── static class ParentContribution    { RegionInfo parent, Status, int filesContributed }
│   └── enum Status { ARCHIVED, PRESENT_WITH_FILES, PRESENT_NO_FILES }
├── static class TrackerFileDiagnostic { Path, Integer count, String error }
├── static class RepairReport          { diagnostics, diskEntries, lineageEntries,
│                                        manifestEntries, parentContributions,
│                                        writtenManifest, noOp,
│                                        allParentsArchived(), hasUnarchivedParents() }
├── (private) class LineageResult      { entries, parentContributions }
├── (private) class ParentLoadResult   { hfiles, boolean archived }
└── static RepairReport repair(
       Configuration, TableDescriptor, ColumnFamilyDescriptor,
       HRegionFileSystem, Lineage, Mode, boolean dryRun) throws IOException
```

#### Why a static helper rather than an instance class

- The CLI passes complete dependencies in; no construction-time state survives the call.
- The repair is a pure transformation `(FS state, Lineage, Mode) → (RepairReport, FS state')`.
- Easier to test deterministically.

#### Lineage type

Three states:

- `none()` — no lineage; pure disk-only behavior.
- `splitParent(parent)` — child is a daughter of `parent`.
- `mergeParents(parents)` — child is the merged child of `parents`.

States are mutually exclusive in the CLI: split lineage is preferred only if merge lineage is
absent (and vice-versa) — this matches `meta` which never carries both at once for a healthy row.

---

### 4.2 `StoreFileListFile.writeNew(StoreFileList.Builder)`

```
Path writeNew(StoreFileList.Builder builder) throws IOException {
  NavigableMap<Long, List<Path>> seqId2TrackFiles = listFiles();
  long highestSeqId = seqId2TrackFiles.isEmpty()
                      ? -1L
                      : seqId2TrackFiles.firstKey();   // map is reverse-ordered
  long seqId  = max(currentTime(), highestSeqId + 1);
  ensureDir(trackFileDir);
  Path file   = trackFileDir / "f1." + seqId;
  long ts     = max(prevTimestamp + 1, currentTime());
  write(fs, file, builder.setTimestamp(ts).setVersion(VERSION).build());
  prevTimestamp = -1;        // reset so a later update() must re-load first
  nextTrackFile = -1;
  return file;
}
```

#### Invariants


| Invariant                         | Why                                                                                                                                                                                                                                                             |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seqId > highestSeqId`            | The new file is the unambiguous winner of `select(...)` after the next normal load.                                                                                                                                                                             |
| Writes to slot `f1` only          | Any subsequent legitimate `update(...)` will run `load(false)` first (because `nextTrackFile == -1`), pick `f1.<seqId>` as the winner, and rotate to `f2.<seqId>`. The "more than 2 files for the same seqId" exception is impossible because `seqId` is fresh. |
| Old files left in place           | Pruning is delegated to `cleanUpTrackFiles(...)` on the next `load(false)`, which is the moment HBase already owns a consistent view of the new generation.                                                                                                     |
| No mutation of the corrupted file | Defensive: keeps a forensic artifact for operators.                                                                                                                                                                                                             |
| Version + CRC32 written           | Same on-wire format as `update(...)`. Existing readers do not need any change.                                                                                                                                                                                  |


#### Why not reuse `update(...)`

`update(...)` requires a successful `load(false)` first to populate `nextTrackFile` and
`prevTimestamp`. By definition, repair runs because `load(false)` does **not** succeed. `writeNew`
sidesteps that prerequisite and instead establishes a fresh winning generation that the next
`load(false)` will accept.

---

### 4.3 `StoreFileListFilePrettyPrinter` (CLI)

#### New CLI flags


| Flag                                       | Required for          | Behavior                                                                          |
| ------------------------------------------ | --------------------- | --------------------------------------------------------------------------------- |
| `--repair`                                 | Repair                | Selects the repair code path.                                                     |
| `--repair-mode disk-only|lineage-assisted` | Repair                | Defaults to `disk-only`.                                                          |
| `--dry-run`                                | Repair                | Prints report without writing a manifest. Bypasses the offline guard.             |
| `--region-offline`                         | Repair (write)        | Operator acknowledgement that the region is offline. Required unless `--dry-run`. |
| `--force-meta`                             | Repair (`hbase:meta`) | Required only when `targetTableName == hbase:meta`.                               |


#### Pre-flight checks (in order)

```
1. !regionOfflineAck && !dryRun                             ── fail fast
2. isMeta(targetTable) && !forceMeta                        ── fail fast
3. resolve rootDir, tablePath, regionPath, fs               ── once via rootDir.getFileSystem()
4. tableDescriptor = FSTableDescriptors.getTableDescriptorFromFs(...)
   if null            → fail
5. trackerName = StoreFileTrackerFactory.getStoreFileTrackerName(storeConf)
   if not FILE && not MIGRATION → fail (writing a manifest the runtime won't read is dangerous)
6. familyDescriptor exists in tableDescriptor               ── fail if not
7. regionInfo  = HRegionFileSystem.loadRegionInfoFileContent(fs, regionPath)
8. regionFs    = HRegionFileSystem.openRegionFromFileSystem(... readOnly=true)
9. if mode == LINEAGE_ASSISTED:
       lineage = resolveLineage(regionInfo)   // catches IOException, degrades to none()
   else: lineage = Lineage.none()
10. report = StoreFileListRepair.repair(...)
11. printRepairReport(report)
```

#### Lineage resolution (`resolveLineage`)

```
try (Connection c = ConnectionFactory.createConnection(getConf())) {
  // 1. Merge lineage: read child row directly.
  Result child = MetaTableAccessor.getRegionResult(c, regionInfo);
  if (child not empty) {
    List<RegionInfo> mergeParents = CatalogFamilyFormat.getMergeRegions(child.rawCells());
    if (!mergeParents.isEmpty()) return Lineage.mergeParents(mergeParents);
  }
  // 2. Split lineage: scan table region rows, look for a parent that names this region
  //    in its info:splitA / info:splitB qualifiers.
  RegionInfo[] holder = new RegionInfo[1];
  MetaTableAccessor.scanMetaForTableRegions(c, result -> {
    PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
    if (regionInfo.equals(daughters.getFirst())
        || regionInfo.equals(daughters.getSecond())) {
      holder[0] = CatalogFamilyFormat.getRegionInfo(result);
      return false;   // short-circuit
    }
    return true;
  }, regionInfo.getTable());
  return holder[0] != null ? Lineage.splitParent(holder[0]) : Lineage.none();
}
```

Cost: O(regions in table) for split lineage; acceptable for an offline operator tool.

#### Exit codes


| Code | Meaning                                                           |
| ---- | ----------------------------------------------------------------- |
| 0    | Repair completed (manifest written, dry-run completed, or no-op). |
| 1    | Argument parsing error.                                           |
| 2    | Precondition check failed or IO failure during repair.            |


---

## 5. Repair pipeline (detailed)

### 5.1 `repair(...)` body

```
repair(conf, td, cfd, regionFs, lineage, mode, dryRun):
  storeContext      = build(cfd, regionFs)
  storeFileListFile = new StoreFileListFile(storeContext)

  diagnostics       = diagnoseTrackerFiles(storeFileListFile, regionFs, cfd)

  diskEntries       = loadStoreFilesFromDisk(conf, td, cfd, regionFs)

  if mode == LINEAGE_ASSISTED && !lineage.isEmpty():
    lineageEntries  = loadStoreFilesFromLineage(conf, td, cfd, regionFs, lineage)
  else:
    lineageEntries  = []

  manifestEntries   = unionStoreFileEntries(diskEntries, lineageEntries)

  noOp              = isAlreadyHealthy(diagnostics, manifestEntries, storeFileListFile)

  writtenManifest   = null
  if !dryRun && !noOp:
    writtenManifest = storeFileListFile.writeNew(toStoreFileListBuilder(manifestEntries))

  return RepairReport(diagnostics, diskEntries, lineageEntries,
                      manifestEntries, writtenManifest, noOp)
```

### 5.2 `diagnoseTrackerFiles(...)`

```
list .filelist
  if missing → return []
for each FileStatus s matching TRACK_FILE_PATTERN:
  try   storeFileListFile.load(s.path)
        → TrackerFileDiagnostic(path, storeFileCount, null)
  catch IOException
        → TrackerFileDiagnostic(path, null, error.message)
return diagnostics
```

This is the only place where the helper deliberately reads the corrupted file. Errors are
**captured**, not propagated, so the report can show the operator exactly which file is broken.

### 5.3 `loadStoreFilesFromDisk(...)`

Delegates to `DefaultStoreFileTracker.getStoreFiles(family)` which:

- lists the family directory,
- filters with `StoreFileInfo.isValid(...)`,
- builds `StoreFileInfo` per file via `ServerRegionReplicaUtil.getStoreFileInfo(...)`.

This is the same enumeration HBase uses for default-tracker stores, so the rebuilt manifest
matches what a `DefaultStoreFileTracker` would have produced.

### 5.4 `loadStoreFilesFromLineage(...)`

Returns a `LineageResult` that bundles the derived `StoreFileInfo` entries together with a list
of `ParentContribution` records (one per parent) that classify each parent as `ARCHIVED`,
`PRESENT_WITH_FILES`, or `PRESENT_NO_FILES`. This information flows into the `RepairReport` so
the CLI can output a data-loss confidence assessment.

Dispatch table:


| Lineage shape            | Method                                |
| ------------------------ | ------------------------------------- |
| `splitParent` set        | `loadStoreFilesFromSplitParent(...)`  |
| `mergeParents` non-empty | `loadStoreFilesFromMergeParents(...)` |


Both internally call `loadParentHFilesOnly(...)` and inspect `ParentLoadResult.archived` to
populate the `ParentContribution` for each parent.

### 5.5 `loadParentHFilesOnly(...)`

Critical for safety. The parent directory may contain leftover virtual entries, especially if a
prior split was interrupted. We must **never** treat those as inputs to a split/merge simulation.

Returns a `ParentLoadResult` containing both the filtered HFile list and an `archived` flag that
indicates whether the parent region directory was absent (Catalog Janitor already archived it).

```
if !fs.exists(parentRegionDir):
  return ParentLoadResult([], archived=true)   // parent archived by Catalog Janitor
parentRegionFs = HRegionFileSystem.openRegionFromFileSystem(... readOnly=true)
  catch FileNotFoundException        → return ParentLoadResult([], archived=true)
  catch IOException                  → log + return ParentLoadResult([], archived=false)
all = loadStoreFilesFromDisk(parentRegionFs)
filter: drop info.isReference() || HFileLink.isHFileLink(name)
return ParentLoadResult(remaining, archived=false)
```

### 5.6 Split-daughter reconstruction

```
loadStoreFilesFromSplitParent(child, parent):
  top      = decideSplitDaughterIsTop(parent, child)
  splitRow = top ? child.startKey : child.endKey
  if splitRow is empty
    throw IOException                  // refuse to synthesize without a split key
  parentFiles = loadParentHFilesOnly(parent)
  if empty → return []
  for each parentFile:
    derived = simulateSplitStoreFile(parent, child, splitRow, top, parentFile)
    if derived != null: append
  return derived
```

#### `decideSplitDaughterIsTop(parent, child)`

Provable boundary match — strictly:


| Condition                                                | Result                                          |
| -------------------------------------------------------- | ----------------------------------------------- |
| `child.start == parent.start && child.end != parent.end` | bottom (false)                                  |
| `child.end == parent.end && child.start != parent.start` | top (true)                                      |
| both equal                                               | `IOException("same key range as parent")`       |
| neither equal                                            | `IOException("does not share either boundary")` |


No "non-empty start key" heuristic; if it isn't provable it is rejected.

#### `simulateSplitStoreFile(...)`

Mirror of `HRegionFileSystem.splitStoreFile(...)`:

```
storeFile = new HStoreFile(parentInfo, bloomType, CacheConfig.DISABLED)
readerOpened = false
try {
  storeFile.initReader()
  readerOpened = true
  splitKey  = PrivateCellUtil.createFirstOnRow(splitRow)   // ExtendedCell
  firstKey  = storeFile.getFirstKey()
  lastKey   = storeFile.getLastKey()
  if top:
    if !lastKey.isPresent() OR splitKey > lastKey  → outOfRange
    else if firstKey.isPresent() && splitKey <= firstKey
                                                    → createLinkFile = true
  else (bottom):
    if !firstKey.isPresent() OR splitKey < firstKey → outOfRange
    else if lastKey.isPresent() && splitKey >= lastKey
                                                    → createLinkFile = true
} catch IOException e {
  log.warn("skip parent file"); return null
} finally {
  if readerOpened: storeFile.closeStoreFile(true)
}
if outOfRange: return null
if createLinkFile:
  // unwrap if the parent file is itself a link
  hfileName, linkedTable, linkedRegion =
    HFileLink.isHFileLink(parentName)
      ? unwrap(parentName)
      : (parentName, child.getTable(), parent.getEncodedName())
  link = HFileLink.build(conf, linkedTable, linkedRegion, family, hfileName)
  return new StoreFileInfo(conf, fs, childStoreDir/linkName, link)
ref = top ? Reference.createTopReference(splitRow) : createBottomReference(splitRow)
path = childStoreDir / (parentName + "." + parent.getEncodedName())
return new StoreFileInfo(conf, fs, path, ref)
```

Key safety properties:

- Reader is closed **only** if `initReader()` actually opened one.
- Per-parent `IOException` does not abort the repair; the parent file is logged and skipped.
- Plain references include the `parentEncodedName` suffix; this is exactly the format
`splitStoreFile(...)` writes, so an HBase region open will resolve them identically.

### 5.7 Merge-child reconstruction

`HRegionFileSystem.mergeStoreFile(...)` always creates a whole-file top reference. We mirror it
literally:

```
for each mergeParent:
  for each parentFile in loadParentHFilesOnly(mergeParent):
    ref  = Reference.createTopReference(mergeParent.startKey)
    path = childStoreDir / (parentFile.name + "." + mergeParent.encodedName)
    derived.add(new StoreFileInfo(storeConf, fs, path, ref))
```

There is no half-range check here because merge produces a whole-file reference.

### 5.8 `unionStoreFileEntries(disk, lineage)`

```
LinkedHashMap<String, StoreFileInfo> byName
for entry in disk    : byName.put(name, entry)               // disk first
for entry in lineage : if !byName.contains(name): put         // disk wins on collision; log it
return values()
```

Disk precedence rationale: if a daughter has already done some work after split (compaction
output materialized into the family directory), we trust that on-disk evidence over a re-derived
lineage entry of the same name.

### 5.9 No-op detection (`isAlreadyHealthy`)

```
if diagnostics empty                                  → manifestEntries.isEmpty()
                                                        (nothing to write either way)
newest = the diagnostic with the highest filename and no error
if newest is null                                     → false
load(newest.path)
if storeFileCount != manifestEntries.size()           → false
if any entry name not in {manifestEntries names}      → false
return true
```

Best-effort: avoids gratuitous seqId churn when an operator runs `--repair` defensively against
a healthy store. Ignored on any IOException.

### 5.10 `toStoreFileListBuilder(entries)`

```
for info in entries:
  e = StoreFileEntry.newBuilder().setName(info.name).setSize(info.size)
  if info.isReference():
    e.setReference(FSProtos.Reference.newBuilder()
        .setSplitkey(ByteString.copyFrom(info.getReference().getSplitKey()))
        .setRange(info.getReference().convert().getRange())
        .build())
  builder.addStoreFile(e.build())
```

Note: `info.getReference().getSplitKey()` is the **encoded "first on row" cell key**, not the raw
row bytes — this matches `Reference`'s on-disk semantics exactly. Tests round-trip through
`Reference.convert(proto)` to verify.

---

## 6. Sequence diagrams

### 6.1 `disk-only` repair against a corrupted manifest

```
operator                CLI                      Repair                 StoreFileListFile         FS
  │                      │                         │                            │                  │
  │ sft --repair ...     │                         │                            │                  │
  │ --region-offline     │                         │                            │                  │
  │ --repair-mode disk-only                        │                            │                  │
  ├─────────────────────►│                         │                            │                  │
  │                      │ guard: offline=ack ✓    │                            │                  │
  │                      │ load TD/CFD/RegionInfo  │                            │                  │
  │                      ├──────► open regionFs    │                            │                  │
  │                      │                         │                            │                  │
  │                      │ repair(...)             │                            │                  │
  │                      ├────────────────────────►│ diagnose tracker files     │                  │
  │                      │                         ├───────────────────────────►│ list+load .filelist
  │                      │                         │ ◄── corruption diag        │                  │
  │                      │                         │ load disk hfiles via       │                  │
  │                      │                         │ DefaultStoreFileTracker    │                  │
  │                      │                         ├───────────────────────────►│                  │
  │                      │                         │ noOp = false               │                  │
  │                      │                         │ writeNew(builder) ────────►│                  │
  │                      │                         │                            │ write f1.<seqId> │
  │                      │                         │ ◄── writtenManifest path  │                  │
  │                      │ ◄─── RepairReport       │                            │                  │
  │                      │ printRepairReport       │                            │                  │
  │ ◄── stdout summary   │                         │                            │                  │
```

### 6.2 `lineage-assisted` repair on a split daughter

```
operator                CLI                       Repair                                   FS / meta
  │ sft --repair --repair-mode lineage-assisted ...                                            │
  ├─────────────────────►│                                                                     │
  │                      │ guard checks                                                        │
  │                      │ resolveLineage(regionInfo)                                          │
  │                      ├─────────────────────────────────────────────────────────────────►  │ scan meta
  │                      │ ◄── splitParent or mergeParents (or none)                           │
  │                      │ repair(...)                                                          │
  │                      ├─────►│ diagnose                                                     │
  │                      │      │ disk = []   (daughter not yet started)                       │
  │                      │      │ if splitParent.present:                                      │
  │                      │      │   loadStoreFilesFromSplitParent:                             │
  │                      │      │     decideSplitDaughterIsTop                                 │
  │                      │      │     loadParentHFilesOnly(parent) ─► open parentFs           │
  │                      │      │     for each pf: simulateSplitStoreFile(...)                 │
  │                      │      │ union(disk, lineage)                                         │
  │                      │      │ writeNew(...) ─► f1.<seqId>                                  │
  │                      │ ◄── report                                                          │
```

---

## 7. Failure modes & semantics


| Source of failure                                    | Detected where                                   | Outcome                                                                                                                                    |
| ---------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Corrupted latest tracker file                        | `diagnoseTrackerFiles` → diagnostic with `error` | Repair proceeds; new manifest replaces winner.                                                                                             |
| Parent dir missing (archived)                        | `loadParentHFilesOnly` → dir `!exists` or FNF    | `ParentLoadResult([], archived=true)` → `ParentContribution(ARCHIVED)`; lineage contribution = []; report outputs "No data loss expected". |
| Parent open IO error                                 | `loadParentHFilesOnly` catches `IOException`     | `ParentLoadResult([], archived=false)` → `ParentContribution(PRESENT_NO_FILES)` + `WARN` log.                                              |
| Per-parent HFile read error in split simulation      | `simulateSplitStoreFile` catches `IOException`   | That parent file skipped + `WARN` log.                                                                                                     |
| Lineage requested but child not provably a daughter  | `decideSplitDaughterIsTop` throws                | Repair fails fast — fail closed. Operator must re-run with `disk-only` if intentional.                                                     |
| Lineage scan throws                                  | CLI `repairStoreFileList` catches                | Fall back to `Lineage.none()` and continue.                                                                                                |
| Operator forgot `--region-offline`                   | CLI guard                                        | Exit 2 before any FS write.                                                                                                                |
| Operator targets `hbase:meta` without `--force-meta` | CLI guard                                        | Exit 2.                                                                                                                                    |
| Table is not FILE/MIGRATION SFT                      | CLI guard                                        | Exit 2.                                                                                                                                    |
| Manifest already healthy                             | `isAlreadyHealthy`                               | `noOp = true`, no manifest written, exit 0.                                                                                                |
| Dry-run                                              | CLI / repair                                     | No FS write, full report printed, exit 0.                                                                                                  |


### 7.1 Data-loss confidence assessment

When lineage is requested, the report distinguishes two critical scenarios based on parent
archive status. This distinction is grounded in a Catalog Janitor invariant: the janitor
only archives a parent region directory **after** all daughter stores have compacted away their
references to that parent (checked via `sft.hasReferences()`). Therefore:


| Scenario                                | Parent archive status                    | Confidence                                                                                                                        | CLI output                                                                                                                          |
| --------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| All parent regions archived (dir FNF)   | All `ARCHIVED`                           | **High** — daughters already compacted away all split/merge references; no data was lost.                                         | "All parent regions are archived by Catalog Janitor. ... No data loss expected; the disk-only file set is authoritative."           |
| Some/all parent regions unarchived      | At least one `PRESENT_WITH_FILES`        | **Requires admin review** — reconstructed references may reintroduce data that a prior compaction already folded in or discarded. | "WARNING: One or more parent regions still have unarchived HFiles. ... Admin review recommended before bringing the region online." |
| Mixed (some archived, some present)     | Mix of `ARCHIVED` + `PRESENT_WITH_FILES` | Same as above: at least one unarchived parent → warning issued.                                                                   | Same warning as above, with per-parent status detail lines.                                                                         |
| All parent present but no files matched | All `PRESENT_NO_FILES`                   | Informational                                                                                                                     | Per-parent detail: "PRESENT, but no HFiles matched."                                                                                |


The per-parent detail is printed as:

```
--- Parent contribution detail ---
  Parent <encodedName>: ARCHIVED (directory not found).
  Parent <encodedName>: PRESENT, contributed N reference(s)/link(s).
  Parent <encodedName>: PRESENT, but no HFiles matched.
```

Convenience methods on `RepairReport`:

- `allParentsArchived()` — returns `true` when every `ParentContribution` has status `ARCHIVED`.
- `hasUnarchivedParents()` — returns `true` when at least one `ParentContribution` has status
`PRESENT_WITH_FILES`.

---

## 8. Concurrency & ordering

- Repair assumes the region is **offline**. CLI requires `--region-offline` (or `--dry-run`).
- No locking with master/RS is performed.
- `writeNew` is the only mutation. It uses `fs.create(file, true)` (overwrite=true), but the
`seqId` is fresh so collision is impossible.
- After repair, the next normal `load(false)` call (e.g. on region open) will:
  1. List `.filelist` and group by `seqId`.
  2. Find the new `f1.<seqId>` as the newest entry, alone for its seqId.
  3. Select it as the winner.
  4. `cleanUpTrackFiles(...)` will asynchronously delete all older tracker files (including the
    corrupted one). This is HBase's existing post-load cleanup path; we deliberately reuse it
     instead of deleting from inside repair.

---

## 9. Test plan (`TestStoreFileListRepair`)

Small (`SmallTests`) JUnit class in `regionserver.storefiletracker`. Uses
`HBaseCommonTestingUtil` and writes real HFiles via `HFileTestUtil`.


| Test                                                                 | What it proves                                                                                                                                                                                                                                                                                                                                                                |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `testCorruptedManifestIsDiagnosedAndReplaced`                        | A genuinely corrupt CRC tracker file is reported as corrupted in `diagnostics`; a strictly newer manifest is written; new manifest contains the on-disk HFile name.                                                                                                                                                                                                           |
| `testLineageAssistedWithoutLineageFallsBackToDiskOnly`               | With `Lineage.none()`, lineage-assisted matches disk-only.                                                                                                                                                                                                                                                                                                                    |
| `testLineageAssistedSplitRepairAddsReferencesAndLinks`               | For a top daughter, the parent file whose first key ≥ split row is recreated as an `HFileLink` in the manifest, and the parent file whose key range straddles the split row is recreated as a `Reference` with `range=TOP`. The encoded split key round-trips through `Reference.convert(...)`. Also asserts `ParentContribution` is `PRESENT_WITH_FILES` with correct count. |
| `testLineageAssistedSplitBottomDaughterReferenceIsBottom`            | The bottom-daughter path produces `range=BOTTOM`.                                                                                                                                                                                                                                                                                                                             |
| `testLineageAssistedUnionPreservesOnDiskFiles`                       | When both disk entries and lineage entries exist, the union has both with the on-disk file preserved.                                                                                                                                                                                                                                                                         |
| `testLineageAssistedMergeRepairAddsReferences`                       | For two merge parents, both whole-file top references are added to the merged child's manifest. Also asserts both `ParentContribution` records are `PRESENT_WITH_FILES`.                                                                                                                                                                                                      |
| `testLineageAssistedSplitWithArchivedParentProducesNoLineageEntries` | If the parent region directory is gone (FNF), no synthetic references are created. Asserts `ParentContribution` is `ARCHIVED`, `allParentsArchived()` is `true`, `hasUnarchivedParents()` is `false`.                                                                                                                                                                         |
| `testUnarchivedParentReportsPresentWithFiles`                        | When a split parent's region directory still exists with HFiles, `ParentContribution` is `PRESENT_WITH_FILES`, `hasUnarchivedParents()` is `true`, `allParentsArchived()` is `false`.                                                                                                                                                                                         |
| `testMergeWithMixedArchiveStatus`                                    | Two merge parents where one is archived and one is present. Asserts mixed `ParentContribution` statuses: one `ARCHIVED`, one `PRESENT_WITH_FILES`; `allParentsArchived()` is `false`, `hasUnarchivedParents()` is `true`.                                                                                                                                                     |
| `testDryRunDoesNotWriteManifest`                                     | With `dryRun=true` and an existing corrupted file, no new manifest is written and the corrupt file remains.                                                                                                                                                                                                                                                                   |
| `testNoOpWhenManifestAlreadyMatchesDisk`                             | Running `repair` twice in a row results in a no-op the second time.                                                                                                                                                                                                                                                                                                           |
| `testDecideSplitDaughterIsTopThrowsWhenNotADaughter`                 | The fail-closed boundary is enforced.                                                                                                                                                                                                                                                                                                                                         |


All 12 tests pass on Java 17 (`mvn -pl hbase-server -Dtest=TestStoreFileListRepair`).

---

## 10. Operator workflows

### 10.1 Diagnose only

```
sft --table ns:t --region <enc> --columnfamily f \
    --repair --repair-mode disk-only --dry-run
```

Prints:

- which `.filelist` files load and which are corrupted,
- count of disk entries,
- count of lineage entries (always 0 here),
- the recomputed manifest count,
- "Dry-run completed. No new manifest was written."

### 10.2 Apply repair (disk-only)

```
sft --table ns:t --region <enc> --columnfamily f \
    --repair --repair-mode disk-only --region-offline
```

### 10.3 Apply repair (lineage-assisted, recently split daughter)

```
sft --table ns:t --region <enc> --columnfamily f \
    --repair --repair-mode lineage-assisted --region-offline
```

### 10.4 Repairing `hbase:meta` (only if master is offline)

```
sft --table hbase:meta --region <enc> --columnfamily info \
    --repair --repair-mode disk-only --region-offline --force-meta
```

---

## 11. Out of scope (deferred)

- Online HBCK service / RPC integration.
- Cluster-wide scan / batch repair.
- Snapshot manifest as a recovery source.
- Older `.filelist` generation as a recovery source.
- Repair of stores using the `DEFAULT` tracker (no manifest exists; nothing to repair).
- Modifications of `meta` itself (we only read `meta`).

---

## 12. Open questions / future work

1. Should we emit a sidecar journal of `Reference` payloads on FILE SFT split/merge so future
  recovery does not need lineage at all? The chat decided against it for v1; revisit later.
2. Should we expose `--no-op-detection=false` to force-write a fresh seqId even when the existing
  manifest is healthy? Useful for clearing stale older generations. Currently relies on
   `cleanUpTrackFiles` after a future region open.
3. Can we add a confirmation prompt (`y/N`) when the operator omits `--dry-run` for additional
  safety? Currently the explicit `--region-offline` flag is the safety contract.

---

## 13. Quick code map


| Concern                        | File:Line(s)                                                                                                                      |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| Repair entry point             | `StoreFileListRepair.java` → `repair(...)`                                                                                        |
| Diagnose loop                  | `StoreFileListRepair.java` → `diagnoseTrackerFiles(...)`                                                                          |
| Disk listing                   | `StoreFileListRepair.java` → `loadStoreFilesFromDisk(...)` (delegates to `DefaultStoreFileTracker.getStoreFiles`)                 |
| Parent filter (HFiles only)    | `StoreFileListRepair.java` → `loadParentHFilesOnly(...)`                                                                          |
| Split-daughter logic           | `StoreFileListRepair.java` → `loadStoreFilesFromSplitParent(...)`, `simulateSplitStoreFile(...)`, `decideSplitDaughterIsTop(...)` |
| Merge-child logic              | `StoreFileListRepair.java` → `loadStoreFilesFromMergeParents(...)`                                                                |
| Union                          | `StoreFileListRepair.java` → `unionStoreFileEntries(...)`                                                                         |
| No-op detection                | `StoreFileListRepair.java` → `isAlreadyHealthy(...)`                                                                              |
| Manifest write                 | `StoreFileListFile.java` → `writeNew(StoreFileList.Builder)`                                                                      |
| CLI guards                     | `StoreFileListFilePrettyPrinter.java` → `repairStoreFileList()`                                                                   |
| Lineage resolution             | `StoreFileListFilePrettyPrinter.java` → `resolveLineage(RegionInfo)`                                                              |
| Parent archive status tracking | `StoreFileListRepair.java` → `ParentContribution`, `ParentLoadResult`, `LineageResult`                                            |
| Data-loss confidence output    | `StoreFileListFilePrettyPrinter.java` → `printRepairReport(...)` → parent contribution detail + assessment                        |
| Report rendering               | `StoreFileListFilePrettyPrinter.java` → `printRepairReport(...)`                                                                  |


