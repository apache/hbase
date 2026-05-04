# FSFT Manifest Recover Design

## Problem

The FILE store file tracker persists files list in manifest files under `.filelist`.
If the newest manifest is corrupted in a non-EOF way, `StoreFileListFile.load(...)` fails hard and
region/store open can fail as well.

For FILE SFT, not every store member is guaranteed to exist as a file in the child family
directory:

- plain HFiles do exist on disk
- virtual split/merge `Reference`s may exist only in the manifest
- virtual `HFileLink`s may exist only in the manifest plus archive back references

This design adds an offline repair flow that can rebuild a fresh manifest without changing the
normal runtime load semantics.

## Goals

- Recover a corrupted latest `.filelist` generation by writing a new valid generation.
- Support a minimal mode that only uses files which currently exist in the child family directory.
- Support a lineage-assisted mode that can reconstruct split/merge virtual entries when current
  `hbase:meta` lineage still exists and parent files remain at their original locations.


## Non-Goals

- This does not serve as a replacement for data recovery from DR cluster, just a recovery mechasim
- No fallback to older `.filelist` generations as a repair source.

## User-Facing Shape

Extend the existing `sft` tool with a repair path.

Inputs:

- `--table`
- `--region`
- `--columnfamily`
- `--repair`
- `--repair-mode disk-only|lineage-assisted` (default: `disk-only`)
- `--dry-run`
- `--region-offline` (operator acknowledgement that the target region is not hosted)
- `--force-meta` (only required when targeting `hbase:meta`)

Repair requires `table + region + family`. Printing existing manifest contents continues to support
the existing file-based and region-based paths.

Examples:

```
# Inspect what repair would do without writing anything
sft --table ns:t --region 3d58e9067bf23e378e68c071f3dd39eb --columnfamily f \
    --repair --repair-mode lineage-assisted --dry-run

# Apply repair after taking the region offline
sft --table ns:t --region 3d58e9067bf23e378e68c071f3dd39eb --columnfamily f \
    --repair --repair-mode lineage-assisted --region-offline
```

Exit codes:

- `0` repair completed (manifest written, dry-run completed, or no-op)
- `1` argument parsing error
- `2` precondition check failed or IO failure during repair

## Preconditions

- The target region must be **offline** (no master or RegionServer hosting it). The CLI requires
  `--region-offline` (or `--dry-run`) to make this explicit.
- The target table must use the FILE store-file tracker (or MIGRATION). The CLI refuses other
  trackers because writing a `.filelist` would not be consulted at runtime.
- Repairing `hbase:meta` requires `--force-meta` AND should only be attempted with the master
  offline.

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

- Prefer `--dry-run` first.
- Require an explicit repair mode.
- Refuse to write a manifest unless `--region-offline` is provided.
- Refuse `hbase:meta` unless `--force-meta` is provided.
- Refuse to repair stores that are not configured to use the FILE tracker.
- Only synthesize split/merge artifacts when lineage is still provable from current `meta`.
  - "Provable" means the child boundary uniquely matches the parent boundary on exactly one side.
    If both sides match (same key range as parent) or neither side matches, we refuse.
- If lineage is absent, do not guess; just use the child files found on disk.
- Ignore archived parent files for reconstruction.
- When parent files cannot be opened or read (FNF, IO error, corrupt HFile), skip that parent
  contribution and continue; never abort the whole repair.

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

Focused tests should cover:

- disk-only rebuild from child files on disk
- checksum/parse corruption followed by successful repair
- split-daughter reconstruction of both references and links
- merged-child reconstruction of references
- lineage-assisted mode falling back to disk-only when no lineage exists
- dry-run not writing a new manifest
