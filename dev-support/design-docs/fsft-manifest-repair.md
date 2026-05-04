# FSFT Manifest Repair Design

## Problem

The FILE store file tracker persists store membership in manifest files under `.filelist`.
If the newest manifest is corrupted in a non-EOF way, `StoreFileListFile.load(...)` fails hard and
region/store open can fail as well.

For FILE SFT, not every store member is guaranteed to exist as a file in the child family
directory:

- plain HFiles do exist on disk
- virtual split/merge `Reference`s may exist only in the manifest
- virtual `HFileLink`s may exist only in the manifest plus archive back references

This design adds two complementary repair flows that share the same core logic but ship in two
different operator surfaces:

1. An **online HBCK2-style chained procedure** (`RepairFsftRegionProcedure`) that closes the
   region as `ABNORMALLY_CLOSED`, rebuilds the manifest, and re-opens the region — all as a
   single durable workflow. Used for user-table regions and `hbase:meta`.
2. An **offline CLI** (`hbase sft --repair`) that runs in a standalone JVM with no master in the
   loop. Used for `master:store`, where the master JVM cannot finish initialisation while the
   manifest is corrupt and so cannot host any RPC handler or procedure executor.

Both surfaces call into the same `StoreFileListRepair` library, so the disk-only and
lineage-assisted reconstruction logic exists in exactly one place.

## Goals

- Repair a corrupted latest `.filelist` generation by writing a new valid generation.
- Support a minimal mode that only uses files which currently exist in the child family directory.
- Support a lineage-assisted mode that can reconstruct split/merge virtual entries when current
  `hbase:meta` lineage still exists and parent files remain at their original locations.
- Keep the repair scoped to one store: `table + region + family`.
- Provide a single durable operator command (procedure) for user-table and meta cases that
  atomically closes → repairs → re-opens the region.
- Provide a master-independent CLI for the `master:store` case.

## Non-Goals

- No fallback to snapshot manifests.
- No fallback to older `.filelist` generations as a repair source.
- No cluster-wide scan or automatic bulk repair.
- No procedure-driven repair for `master:store` — structurally impossible (the procedure store
  *is* `master:store`).

## Targets

The repair tool can target three structurally different regions. Each is verified against the
upstream codebase below.

### User-table region

Standard tables. May split, may merge, may be a snapshot/clone source. `.filelist` can contain
plain HFiles, split-reference files, merge-reference files, and `HFileLink`s. **Both** repair
modes apply.

### `hbase:meta`

Meta has 1 region by design and **never splits or merges**. Enforced at runtime in
`RegionSplitPolicy.shouldSplit(...)` (see hbase-server `RegionSplitPolicy.java:67`):

```java
return !region.getRegionInfo().isMetaRegion() && region.isAvailable() ...
```

There is no UX to override it; meta is also never a snapshot source. Meta's `.filelist` therefore
only ever contains plain HFiles produced by flushes. **Only `disk-only` mode applies** for meta.

Empirical confirmation (mini-cluster bootstrap with `hbase.store.file-tracker.impl=FILE`,
see `TestMetaWithFileBasedStoreFileTracker`):

- Meta's `TableDescriptor` does inherit `TRACKER_IMPL=FILE` if the cluster is freshly bootstrapped
  with FILE as the global default, because `FSTableDescriptors.tryUpdateAndGetMetaTableDescriptor`
  calls `StoreFileTrackerFactory.updateWithTrackerConfigs` only when the meta TD does not already
  exist on disk. On clusters that pre-date the FILE flip, the meta TD keeps whatever tracker was
  imprinted at original bootstrap (typically `DEFAULT`) and runtime config changes do not affect
  it.
- Even with `TRACKER_IMPL=FILE` imprinted, meta CFs only materialize a `.filelist` directory after
  they have flushed at least once. On a freshly started cluster only the namespace CF tends to
  flush (during namespace bootstrap); the other meta CFs (`info`, `rep_barrier`, `table`) have no
  `.filelist` until they have written data.

### `master:store` (master local region)

Used to persist the master local store: procedure store, region-state store, RS tracker, server
state. Defined in `MasterRegionFactory.java:86`:

```java
public static final TableName TABLE_NAME = TableName.valueOf("master:store");
```

`MasterRegion.bootstrap(...)` creates a single hard-coded `RegionInfo` (`MasterRegion.java:307`).
This region is not a normal HBase table — it never goes through `SplitTableRegionProcedure` or
`MergeTableRegionsProcedure`, is never assigned via `AssignmentManager`, is never a snapshot
source, and lives entirely inside the master JVM.

Despite living inside the master JVM, `master:store` is a **real HRegion with HFiles**, not a
WAL-only construct. `MasterRegionFlusherAndCompactor` runs flushes (memstore-size, change-count,
or every 15 minutes per `DEFAULT_FLUSH_INTERVAL_MS`) and major-compacts when the per-store file
count crosses `compactMin` (default 4). The four CFs (`info`, `proc`, `rs`, `state`) accumulate
HFiles under `MasterData/data/master/store/<region>/<cf>/`.

Its CF directories only ever contain plain HFiles. **Only `disk-only` mode applies.**

FILE SFT *is* a supported configuration for `master:store`. See `MasterRegionFactory.java:84`:

```java
public static final String TRACKER_IMPL = "hbase.master.store.region.file-tracker.impl";
```

and the resolution order in `withTrackerConfigs(...)` (`MasterRegionFactory.java:103-114`): the
master-store-specific key takes precedence over `hbase.store.file-tracker.impl`, which takes
precedence over `DEFAULT`. `MIGRATION` is explicitly rejected; `FILE` is explicitly allowed.
There is an existing test (`TestChangeSFTForMasterRegion`) that boots the master with `DEFAULT`,
flips the conf to `FILE`, and asserts the resulting TD imprints `TRACKER_IMPL=FILE`. Therefore
`master:store` corruption from FILE SFT is a real, in-tree-supported failure mode and warrants a
recovery story.

`master:store` is the case the offline CLI is structurally required for, because corruption
of its `.filelist` prevents `ProcedureExecutor` from initializing. No procedure-based recovery
flow can run when the procedure store itself cannot be loaded.

### Per-target mode applicability and surface

| Target            | Splits | Merges | Virtual entries possible | Modes that apply                | Operator surface |
|-------------------|--------|--------|--------------------------|---------------------------------|---------------------------------|
| User table region | yes    | yes    | yes                      | `disk-only`, `lineage-assisted` | `RepairFsftRegionProcedure` (online) |
| `hbase:meta`      | no     | no     | no                       | `disk-only` only                | `RepairFsftRegionProcedure` (online; submitted via the same `assigns`-like RPC path that bypasses `rpcPreCheck`) |
| `master:store`    | no     | no     | no                       | `disk-only` only                | Offline CLI (`hbase sft --repair`); master JVM must be stopped |

Why the surfaces differ:

- For user tables and `hbase:meta`, the active master JVM is up (or at least the
  `ProcedureExecutor` is up — see "Why the procedure path works for stuck-init meta" below). A
  procedure that holds the region lock for the full close→repair→reopen cycle gives us atomic
  recovery with no operator orchestration.
- For `master:store`, the procedure framework is unavailable by construction: the procedure
  store **is** `master:store`. If `master:store`'s `.filelist` is corrupt, the master JVM aborts
  during init before `ProcedureExecutor` initializes. There is no online surface that can run.
  The only mechanism that works is a standalone JVM that opens HDFS directly while the master
  is stopped — i.e., a category-3 tool (alongside `hbase wal`, `hbase hfile`).

## User-Facing Shape

There are two surfaces.

### Online: HBCK2 RPC backed by a chained procedure

For user tables and `hbase:meta`. New `Hbck.repairFsftRegion(...)` API submits a
`RepairFsftRegionProcedure` and returns its proc-id. The HBCK2 client wraps this with an optional
synchronous wait.

```
# User table — apply lineage-assisted repair (submits procedure, prints proc-id)
hbck2 repairFsftRegion --table ns:t --region 3d58e... --family f \
      --mode lineage-assisted

# User table — dry-run (no manifest written, no close-then-reopen)
hbck2 repairFsftRegion --table ns:t --region 3d58e... --family f \
      --mode lineage-assisted --dry-run

# hbase:meta — disk-only only
hbck2 repairFsftRegion --table hbase:meta --region 1588230740 --family info \
      --mode disk-only
```

The procedure itself is documented under **Online Path: `RepairFsftRegionProcedure`** below.

### Offline: standalone `sft --repair` CLI

For `master:store` only. Runs in a fresh JVM, talks to HDFS directly, does not connect to any
master or RegionServer. Exists in the same family as `hbase wal` / `hbase hfile` /
`hbase sft --print` (i.e., `Configured implements Tool`).

```
# master:store — master JVM must be stopped first
hbase sft --repair --table master:store --region <encoded> --columnfamily proc \
    --repair-mode disk-only --master-store-offline --force-master-store
```

CLI inputs:

- `--table`, `--region`, `--columnfamily`, `--repair`
- `--repair-mode disk-only` (only `disk-only` is accepted for the CLI surface; the only target
  is `master:store`, which cannot have virtual entries)
- `--dry-run`
- `--master-store-offline` (operator acknowledgement that the master JVM is stopped)
- `--force-master-store` (operator acknowledgement that this is an irreversible repair on the
  internal master local region)

The CLI refuses to run for any target other than `master:store`. Operators wanting to repair a
user table or meta should use the procedure-backed RPC instead, because that path includes the
atomic close→repair→reopen orchestration.

CLI exit codes:

- `0` repair completed (manifest written, dry-run completed, or no-op)
- `1` argument parsing error
- `2` precondition check failed or IO failure during repair

## Preconditions

### Online (procedure) path

- The target table must use the FILE store-file tracker (or MIGRATION). The handler refuses other
  trackers because writing a `.filelist` would not be consulted at runtime.
- The target table is **not** `master:store` (rejected by the RPC handler — must use the offline
  CLI instead).
- The procedure validates `RegionState` itself; no operator pre-step is required to take the
  region offline. The procedure performs the offline transition (`ABNORMALLY_CLOSED`) under the
  region lock.
- Repairing `hbase:meta` is allowed without a special force flag because the procedure is
  master-driven and meta corruption is rare; the meta-only constraint is `--mode disk-only`
  (lineage-assisted is rejected).

### Offline (CLI) path

- Operator has stopped **all** master JVMs. The CLI requires `--master-store-offline` to make
  this explicit. A new master started against a still-corrupt `.filelist` will fail to
  initialize its `ProcedureExecutor`, so the repair must complete before any master is restarted.
- Target must be `master:store`. The CLI refuses any other table.
- `--force-master-store` is required to acknowledge that this is an irreversible repair on the
  internal master local region.

## Repair Modes

### `disk-only`

Enumerate files that currently exist in the child family directory, filter them with the same rules
used by the default store file tracker, and build a new manifest from that set only.

This mode never synthesizes virtual entries.

### `lineage-assisted`

Start from the `disk-only` file set. If current `hbase:meta` still proves that the target region is
either:

- a split daughter, or
- a merged child

then simulate the original split/merge decision logic against unarchived parent store files and add
the derived child entries to the manifest set.

If no split/merge lineage exists, treat that as the normal happy path and fall back to the exact
same result as `disk-only`.

## Split Reconstruction

When current `meta` still exposes a split parent through `info:splitA` / `info:splitB`:

1. identify whether the target child is the lower or upper daughter
2. derive the split row from the child boundary
3. list parent family store files that still exist in the parent directory
4. simulate `HRegionFileSystem.splitStoreFile(...)`

Per parent file, the simulation decides whether the child should get:

- no entry
- a whole-file `HFileLink`
- a top `Reference`
- a bottom `Reference`

Archived parent files are ignored. Plain references require the original parent path to remain
present.

## Merge Reconstruction

When current `meta` still exposes merge parents through `merge*` qualifiers:

1. list each merge parent family store file that still exists in the parent directory
2. simulate `HRegionFileSystem.mergeStoreFile(...)`

Each eligible parent file contributes a whole-file top `Reference` into the merged child.

Archived parent files are ignored.

## Manifest Write Strategy

Repair never rewrites the corrupted file in place.

Instead it:

1. diagnoses existing `.filelist` files
2. computes a new store file set
3. writes a brand new strictly-newer tracker file under `.filelist` via
   `StoreFileListFile.writeNew(...)`

Older (including corrupted) files are left in place in this phase. They are pruned by
`cleanUpTrackFiles(...)` on the next normal `load(false)` once a region opens, which is the moment
HBase already owns a consistent view of the new generation.

Invariant: the new tracker file uses `seqId = max(now, highestSeqId+1)`. This guarantees:

- the new file wins the `select(...)` race in `StoreFileListFile.load(boolean)`,
- the new file does not collide with any existing seqId, so the
  `> 2 files for sequence id` `DoNotRetryIOException` cannot be triggered.

The repair is a no-op when an existing tracker file already loads cleanly and its store-file name
set matches the recomputed manifest. This avoids unnecessary seqId churn when the operator runs
the tool defensively against a healthy store.

### No-op detection

If `--dry-run` is not set and the latest healthy tracker file already exposes the same set of
store-file names as the recomputed manifest, the tool reports `No repair needed` and writes
nothing.

## Safety Rules

Shared (apply to both surfaces):

- Prefer `--dry-run` first.
- Refuse to repair stores that are not configured to use the FILE (or MIGRATION) tracker.
- Refuse `--mode lineage-assisted` when the target is `hbase:meta` or `master:store`. These
  targets cannot produce split/merge references or `HFileLink`s, so the lineage path is
  meaningless and accepting it would only confuse the operator.
- Only synthesize split/merge artifacts when lineage is still provable from current `meta`.
  - "Provable" means the child boundary uniquely matches the parent boundary on exactly one side.
    If both sides match (same key range as parent) or neither side matches, we refuse.
- If lineage is absent, do not guess; just use the child files found on disk.
- Ignore archived parent files for reconstruction.
- When parent files cannot be opened or read (FNF, IO error, corrupt HFile), skip that parent
  contribution and continue; never abort the whole repair.

Online procedure path:

- Refuse `master:store` (must use the offline CLI).
- The procedure holds the region lock for the full close→repair→reopen flow; concurrent
  `TransitRegionStateProcedure` work is impossible while the lock is held.
- If a stuck `TransitRegionStateProcedure` already holds the lock at submission time, the
  procedure will mark it `bypass=true` (mirroring HBCK2 `bypassProcedure`) and acquire the lock
  before proceeding.

Offline CLI path:

- Refuse any target other than `master:store`.
- Require `--master-store-offline` AND `--force-master-store`.

### Data-loss confidence output

When running in `lineage-assisted` mode, the tool classifies each parent region's archive status
and prints a confidence assessment:

- **All parents archived** (Catalog Janitor has already cleaned them up): the tool prints
  `"All parent regions are archived by Catalog Janitor. No data loss expected."` This is safe
  because the janitor only archives parents after daughters have compacted away all references.
- **Unarchived parents** (parent region dir still exists with HFiles): the tool prints a warning
  that reconstructed references may reintroduce previously-compacted data. Admin review is
  recommended before bringing the region online.
- Per-parent detail lines show the individual status (`ARCHIVED`, `PRESENT with N references`,
  `PRESENT but no HFiles matched`).

### Known limitation

`meta` lineage can be stale: e.g. Catalog Janitor scheduled but did not yet finish parent GC. In
that window, lineage-assisted repair may add references to a parent that is on the verge of being
archived. This is tolerable because the tool is offline and operator-driven. The recommended
workflow is `--dry-run` first, inspect the report, then apply.

## Tests

### `StoreFileListRepair` (shared library)

- disk-only rebuild from child files on disk
- checksum/parse corruption followed by successful repair
- split-daughter reconstruction of both references and links
- merged-child reconstruction of references
- lineage-assisted mode falling back to disk-only when no lineage exists
- dry-run not writing a new manifest
- no-op detection when current manifest already matches recomputed set

### Online procedure path

- end-to-end: corrupt manifest -> submit procedure -> region back online and serving reads
- procedure resumes after master failover during `COMPUTE_NEW_MANIFEST`
- procedure resumes after master failover during `WRITE_NEW_MANIFEST`
- procedure resumes after master failover during `WAIT_FOR_REOPEN` (child TRSP also resumes)
- procedure bypasses a stuck pre-existing TRSP on the same region
- procedure rejects `master:store` (must use CLI)
- procedure rejects `lineage-assisted` for `hbase:meta`
- meta repair: corrupt meta CF, submit procedure, meta back online (covered by an extension of
  `TestMetaWithFileBasedStoreFileTracker` that introduces a fresh-bootstrap-with-FILE cluster,
  forces a flush, corrupts the resulting `.filelist`, and runs the procedure to recover)
- HBCK2 RPC accepts submission while master is stuck on `waitForMetaOnline()`

### Offline CLI path

- CLI rejects targets other than `master:store`
- CLI rejects without `--master-store-offline` and `--force-master-store`
- end-to-end: stop master JVM, corrupt master:store `.filelist`, run CLI, restart master,
  verify master initializes

## Online Path: `RepairFsftRegionProcedure`

A new `StateMachineProcedure<MasterProcedureEnv, RepairFsftRegionState>` that holds the region
lock for the full close→repair→reopen cycle, mirroring `TransitRegionStateProcedure`'s pattern
for atomic region-state transitions.

### State machine

```
ACQUIRE_REGION_LOCK
  -> ENSURE_REGION_ABNORMALLY_CLOSED   (bypass stuck TRSP if any; force RegionState=ABNORMALLY_CLOSED in meta)
  -> COMPUTE_NEW_MANIFEST              (disk-only or lineage-assisted, via StoreFileListRepair)
  -> WRITE_NEW_MANIFEST                (StoreFileListFile.writeNew(seqId, set))
  -> SCHEDULE_REOPEN                   (spawn TransitRegionStateProcedure as child via addChildProcedure)
  -> WAIT_FOR_REOPEN                   (framework handles this for free)
  -> DONE                              (lock released by framework)
```

### Why `ABNORMALLY_CLOSED` and not `CLOSED`

The region was stuck in `OPENING` because manifest load blew up. `CLOSED` would assert "graceful
close completed," which is a lie — the open never completed. `ABNORMALLY_CLOSED` correctly
signals "forcibly terminated, treat next assign as fresh open with recovery semantics" — same
state SCP stamps when an RS dies mid-open. The child `TransitRegionStateProcedure` we spawn
in `SCHEDULE_REOPEN` enters via the existing `ABNORMALLY_CLOSED -> OPENING` edge, so no new code
is needed in TRSP.

If the region is already `CLOSED` (operator pre-set it via `setRegionStateInMeta`), we
upgrade to `ABNORMALLY_CLOSED` so the subsequent assign takes the recovery path. If it's
already `ABNORMALLY_CLOSED`, this is a no-op.

### Persistent state

Stored in the procedure store across master failover:

```
table_name, encoded_region_name, family, repair_mode, dry_run,
optional computed_manifest, optional max_seq_id_seen
```

`computed_manifest` and `max_seq_id_seen` are populated after `COMPUTE_NEW_MANIFEST` and
consumed by `WRITE_NEW_MANIFEST`. If master fails between the two states, we restart from
`COMPUTE_NEW_MANIFEST` (recompute is idempotent — same HDFS state yields same set).

### Idempotency / failover

- `ACQUIRE_REGION_LOCK` is naturally idempotent (lock is durable in proc framework).
- `ENSURE_REGION_ABNORMALLY_CLOSED` no-ops on already-`ABNORMALLY_CLOSED`.
- `COMPUTE_NEW_MANIFEST` is pure-read; safe to redo.
- `WRITE_NEW_MANIFEST` writes a new file with `seqId = max(now, maxSeqIdSeen+1)`. If we wrote and
  then crashed, on resume we re-list, see our own write, see the names match, and short-circuit
  to no-op.
- `SCHEDULE_REOPEN` adds a child TRSP; framework handles the wait.
- Child TRSP failure → parent fails; operator can `bypassProcedure` and re-submit.

### `hbase:meta` particulars

`RepairFsftRegionProcedure` for meta works because:

1. `ProcedureExecutor` initializes before `waitForMetaOnline()` in
   `HMaster.finishActiveMasterInitialization()`, so the procedure store is up even when meta is
   stuck offline.
2. The new `Hbck.repairFsftRegion(...)` RPC handler skips `rpcPreCheck` (matching the
   `assigns`/`unassigns`/`bypassProcedure` pattern) so it accepts submissions during stuck-init.
3. The child `TransitRegionStateProcedure` for meta is the same code path that
   `hbck2 assigns hbase:meta` already exercises today for SCP recovery.

### Why `master:store` cannot use this

The procedure store is `master:store` itself. If `master:store`'s `.filelist` is corrupted, the
master JVM aborts during init before `ProcedureExecutor` can come up. There is nothing to submit
a procedure *to*. This is structural, not a missing feature.

The offline CLI exists for exactly this case — it runs in a fresh JVM with no master in the
loop, opens HDFS directly, writes a new `.filelist` generation, and exits. After that, master
restart succeeds.

## Alternatives Considered

### Sync RPC (no procedure)

Earlier draft: add `Hbck.repairStoreFileList(...)` whose handler runs `StoreFileListRepair`
synchronously on the master, modeled on `fixMeta`. Operator orchestrates
`setRegionStateInMeta(ABNORMALLY_CLOSED)` → `repairStoreFileList` → `assigns` as three separate
HBCK2 calls.

Why we did not pick this:

- **Race window.** Between `setRegionStateInMeta(ABNORMALLY_CLOSED)` and `assigns`, an
  unrelated SCP, chore, or operator action could schedule a `TransitRegionStateProcedure` and
  walk the still-corrupt manifest, producing a fresh stuck-RIT.
- **RPC timeout risk.** Lineage-assisted repair on a large store does heavy HDFS work
  (per-parent-HFile open) that may exceed default RPC timeouts.
- **No automatic failover handling.** Master crash mid-RPC requires the operator to re-run
  the orchestration; the procedure path resumes itself.
- **Three commands vs one.** Operator UX is materially worse.

The sync RPC approach is otherwise reasonable (smaller code surface, matches `fixMeta`
precedent), but the chained procedure trades ~350 LoC for atomic close→repair→reopen with
durable failover, which we judged worth it.

### Procedure-backed for everything (including `master:store`)

Not possible by construction (procedure store is `master:store`). Discarded immediately.

### Offline CLI for user-table and meta as well

Possible but strictly worse than the procedure path: same code surface in the CLI either way,
no atomic close→repair→reopen, requires per-cluster operator JVM with HDFS perms, no master-side
audit log. Kept the CLI scope narrow to `master:store`.

## Future Direction

Out of scope for this phase but worth recording so boundaries are explicit:

- **Bulk repair** parent procedure: "repair all corrupted regions in table T". Composes
  naturally on top of `RepairFsftRegionProcedure`.
- **Forbid FILE for `master:store`** going forward: extend the existing `MIGRATION` rejection
  in `MasterRegionFactory.withTrackerConfigs(...)` to also reject `FILE` for fresh bootstraps.
  Existing FILE-imprinted master:store regions must keep working, so the check should only fire
  on fresh-bootstrap (TD doesn't yet exist on disk). This is preventive only — anyone already on
  FILE for master:store still needs the offline CLI as the recovery path. Tracked separately.
