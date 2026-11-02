# FSFT Manifest Recover Design

## Problem

The FILE store file tracker (FSFT) persists store membership in manifest files under `.filelist`.
If the newest manifest is corrupted in a non-EOF way, `StoreFileListFile.load(...)` fails hard and
region/store open can fail as well.

For FILE SFT, the manifest can in principle reference store members that do not exist as plain
files in the child family directory:

- plain HFiles do exist on disk
- virtual split/merge `Reference`s may exist only in the manifest
- virtual `HFileLink`s may exist only in the manifest plus archive back references

This design adds a single, offline, operator-driven recovery tool that rebuilds a corrupted
manifest **purely from the store directory listing**, plus a non-authoritative data-loss assessment
derived from `hbase:meta` split/merge lineage.

## Design at a glance

- **One surface: an offline CLI** (`hbase sftrecover`). There is no online/in-master recovery path.
- **One reconstruction strategy: disk-only.** The recovered manifest is exactly the set of store
  files physically present under the family directory (HFiles, references, and links that exist on
  disk), filtered by the same rules the `DefaultStoreFileTracker` uses. The tool never synthesizes
  references or `HFileLink`s from split/merge lineage and never injects parent-derived entries.
- **A separate, read-only data-loss assessment.** For user-table regions the tool consults
  `hbase:meta` for split/merge parents and reports whether bringing the region online risks data
  loss, but this assessment never changes the manifest that is written.

All the logic lives in `StoreFileListRecover`; `StoreFileListRecoverTool` is only the CLI surface
(argument parsing, safety acknowledgements, and report formatting).

## Why offline-only

An earlier draft included an online HBCK2-style chained procedure that closed the region, rebuilt
the manifest, and re-opened it. We dropped it:

- **Nothing in the master can truly fence a RegionServer away from the store directory** while a
  manifest is rewritten. The only real quiescence guarantee is that the region is not hosted
  anywhere — which is an operator fact, not something a master RPC can assert. The CLI makes the
  operator acknowledge this explicitly via `--region-offline`.
- **`master:store` is structurally impossible to recover online** — the procedure store *is*
  `master:store`. If its `.filelist` is corrupt, the master JVM aborts during init before
  `ProcedureExecutor` comes up. There is nothing to submit a procedure to. An offline tool is the
  only mechanism that works for this case.
- A single offline tool that handles all three target shapes (user table, `hbase:meta`,
  `master:store`) is far simpler to reason about and to test than a procedure plus an RPC plus a CLI
  that share reconstruction code but diverge on orchestration.

## Targets

The tool can target three structurally different regions.

### User-table region

Standard tables. May split, may merge, may be a snapshot/clone source. `.filelist` can contain
plain HFiles, split-reference files, merge-reference files, and `HFileLink`s. The recovered manifest
is the on-disk file set. Split/merge parents from `hbase:meta` are assessed for data-loss reporting.

### `hbase:meta`

Meta has 1 region by design and **never splits or merges**. Enforced at runtime in
`RegionSplitPolicy.shouldSplit(...)`:

```java
return !region.getRegionInfo().isMetaRegion() && region.isAvailable() ...
```

Meta is also never a snapshot source, so its `.filelist` only ever contains plain HFiles produced
by flushes. There is no catalog lineage to assess, so the tool skips the parent meta-walk for meta.
The tool refuses to touch meta unless `--force-meta` is supplied, because recovering meta is only
valid with the master offline.

### `master:store` (master local region)

Used to persist the master local store (procedure store, region-state store, RS tracker, server
state). Defined in `MasterRegionFactory`:

```java
public static final TableName TABLE_NAME = TableName.valueOf("master:store");
```

`MasterRegion.bootstrap(...)` creates a single hard-coded `RegionInfo`. This region never goes
through `SplitTableRegionProcedure` or `MergeTableRegionsProcedure`, is never assigned via
`AssignmentManager`, is never a snapshot source, and lives entirely inside the master JVM. Its CF
directories only ever contain plain HFiles, so there is no catalog lineage to assess — the tool
skips the parent meta-walk (it uses `MasterRegionFactory.TABLE_NAME` to detect this case).

FILE SFT *is* a supported configuration for `master:store` (the master-store-specific
`hbase.master.store.region.file-tracker.impl` key takes precedence over
`hbase.store.file-tracker.impl`, then `DEFAULT`; `MIGRATION` is rejected, `FILE` is allowed), so
`master:store` corruption from FILE SFT is a real, in-tree-supported failure mode and warrants a
recovery story. This is the case the offline tool is structurally required for: corruption of its
`.filelist` prevents `ProcedureExecutor` from initializing, so no procedure-based recovery flow can
run.

### Per-target behavior

| Target            | Splits | Merges | Parent assessment | Extra acknowledgement |
|-------------------|--------|--------|-------------------|-----------------------|
| User table region | yes    | yes    | yes (from `hbase:meta`) | `--region-offline` (or `--dry-run`) |
| `hbase:meta`      | no     | no     | skipped           | `--force-meta` + `--region-offline` |
| `master:store`    | no     | no     | skipped           | `--region-offline` (master JVM stopped) |

## User-facing shape

`StoreFileListRecoverTool` runs in a fresh JVM, talks to HDFS directly, and does not connect to any
master or RegionServer. It lives in the same family as `hbase wal` / `hbase hfile` / `hbase sft`
(i.e., `Configured implements Tool`).

```
# User table — rebuild the manifest from disk (region must be offline)
hbase sftrecover --table ns:t --region 3d58e... --columnfamily f --region-offline

# User table — dry-run (assess and report only; nothing written)
hbase sftrecover --table ns:t --region 3d58e... --columnfamily f --dry-run

# hbase:meta — master must be stopped first
hbase sftrecover --table hbase:meta --region 1588230740 --columnfamily info \
    --region-offline --force-meta

# master:store — master JVM must be stopped first
hbase sftrecover --table master:store --region <encoded> --columnfamily proc \
    --region-offline
```

CLI inputs:

- `-t`/`--table`, `-r`/`--region`, `-cf`/`--columnfamily`
- `--dry-run` — print the recover result (including the data-loss assessment) without writing a new
  manifest
- `--region-offline` — operator acknowledgement that the target region is offline (not hosted by any
  master/RS). This is the real quiescence guarantee the tool relies on.
- `--force-meta` — allow recovery against `hbase:meta`. Dangerous; only valid with the master
  offline.

CLI exit codes:

- `0` recover completed (manifest written, dry-run completed, or no-op)
- `1` argument parsing error
- `2` precondition check failed or IO failure during recover

## Preconditions

- The operator supplies `--region-offline` (or `--dry-run`). The tool refuses to write a new
  manifest otherwise, because it cannot itself prove the region is not hosted somewhere.
- The target table must use the FILE store-file tracker (or MIGRATION). The tool refuses other
  trackers because a `.filelist` it writes would not be consulted at runtime.
- For `hbase:meta`, `--force-meta` is required, and the operator must have stopped the master.
- For `master:store`, the operator must have stopped **all** master JVMs. A master started against a
  still-corrupt `.filelist` will fail to initialize its `ProcedureExecutor`, so recovery must
  complete before any master is restarted.

## Reconstruction: disk-only

Enumerate the files that currently exist in the child family directory, filter them with the same
rules used by the `DefaultStoreFileTracker` (`tracker.getStoreFiles(...)`), and build a new manifest
from exactly that set. References and links that physically exist on disk are preserved (the
`Reference` body is carried into the manifest entry); nothing is synthesized.

This is the only reconstruction mode. The manifest is always exactly what is on disk.

## Data-loss assessment (reporting only)

For user-table regions the tool resolves split/merge parents from `hbase:meta`:

- merge parents are read from the child row's merge qualifiers
  (`CatalogFamilyFormat.getMergeRegions`)
- otherwise the table's regions are scanned for a split parent that lists this region as a daughter
  (`MetaTableAccessor.getDaughterRegions`)

For each resolved parent the tool classifies its on-disk archive status. Reference files and
`HFileLink`s in the parent directory are excluded from the count, since they do not represent
unarchived parent data:

- **`ARCHIVED`** — the parent region directory was not found. The Catalog Janitor only archives a
  parent after its daughters have compacted away all references, so in normal operation a missing
  parent directory means its data was already propagated into this region. This is an *inference*,
  not a verification: a missing directory is also the on-disk symptom of a parent lost (to HDFS
  corruption or operator error) *before* archival, so the verdict is reported as "likely" and the
  operator is advised to confirm the parent's HFiles exist under the archive if in doubt.
- **`PRESENT_NO_FILES`** — the parent directory exists but carries no unarchived HFiles.
- **`PRESENT_WITH_FILES`** — the parent directory exists and still has unarchived HFiles.

Verdict:

- **All parents archived** → `LIKELY NO DATA LOSS`: the parent directories are missing, inferred to
  mean their data was archived after being compacted into this region. The disk-only manifest is
  authoritative under that inference.
- **Parents present but no unarchived HFiles** → `NO DATA LOSS`: the disk-only manifest is
  authoritative.
- **Any parent `PRESENT_WITH_FILES`** → `POTENTIAL DATA LOSS`: the Catalog Janitor had not finished
  propagating parent data to this region when the manifest was lost, so the disk-only manifest may
  be missing rows. **Manual data recovery may be required** — the operator should review the parent
  regions before bringing this region online.

This assessment is never written into the manifest and never adds entries to it. It only informs the
operator.

### Known limitation

`meta` lineage can be stale (e.g. Catalog Janitor scheduled but not yet finished parent GC). In that
window a parent may show `PRESENT_WITH_FILES` even though it is about to be archived. This is
tolerable because the tool is offline and operator-driven: the recommended workflow is `--dry-run`
first, inspect the report, then apply.

## Manifest write strategy

Recover never rewrites the corrupted file in place. Instead it:

1. diagnoses existing `.filelist` files (loads each; records the entry count or the load error)
2. computes the new store-file set from the on-disk listing
3. writes a brand new, strictly-newer tracker generation under `.filelist` via
   `StoreFileListFile.writeNew(...)`

Older (including corrupted) files are left in place in this phase. They are pruned by
`cleanUpTrackFiles(...)` on the next normal `load(false)` once a region opens, which is the moment
HBase already owns a consistent view of the new generation.

Invariant: the new tracker file uses `seqId = max(now, highestSeqId+1)`. This guarantees:

- the new file wins the `select(...)` race in `StoreFileListFile.load(boolean)`,
- the new file does not collide with any existing seqId, so the `> 2 files for sequence id`
  `DoNotRetryIOException` cannot be triggered.

### No-op detection

If the latest healthy tracker file already exposes the same set of store-file names as the
recomputed manifest, the tool reports `No recover needed` and writes nothing. This avoids
unnecessary seqId churn when the operator runs the tool defensively against a healthy store.

## Safety rules

- Prefer `--dry-run` first.
- Refuse to write a manifest unless `--region-offline` (or `--dry-run`) is supplied.
- Refuse to recover stores that are not configured to use the FILE (or MIGRATION) tracker.
- Refuse `hbase:meta` without `--force-meta`.
- Never synthesize split/merge artifacts. The manifest is always exactly the on-disk file set.
- The split/merge parent assessment is read-only and best-effort: if `hbase:meta` cannot be reached
  or a parent directory cannot be opened, skip that assessment and continue; never abort the
  recover.

## Tests

`TestStoreFileListRecover` (small test, in-process `HBaseCommonTestingUtil`):

- corrupted manifest is diagnosed and replaced with a strictly-newer disk-only generation
- recover with no parents is purely disk-only
- archived split parent → `LIKELY NO DATA LOSS` (`allParentsArchived` true, `hasUnarchivedParents`
  false), and the manifest contains only the child's own on-disk HFile
- unarchived split parent → `PRESENT_WITH_FILES` / `hasUnarchivedParents` true, and the manifest
  still contains no parent-derived entries
- merge with mixed parent status (one archived, one present-with-files)
- dry-run writes nothing and leaves the corrupt file in place
- no-op detection when the current manifest already matches the on-disk set

## Future direction

Out of scope for this phase but worth recording so boundaries are explicit:

- **Bulk recover** wrapper: "recover all corrupted regions in table T". Composes naturally on top of
  the single-store tool.
- **Forbid FILE for `master:store`** going forward: extend the existing `MIGRATION` rejection in
  `MasterRegionFactory.withTrackerConfigs(...)` to also reject `FILE` for fresh bootstraps. Existing
  FILE-imprinted `master:store` regions must keep working, so the check should only fire on
  fresh-bootstrap (TD doesn't yet exist on disk). This is preventive only — anyone already on FILE
  for `master:store` still needs the offline tool as the recovery path. Tracked separately.
