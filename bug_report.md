# Truncate leaves namespace Region quota usage stale

## Summary

Truncating a table without preserving splits replaces its old Regions with one new primary Region, but the active Master's namespace quota cache retains the old Region count for that table. A later split can be rejected even when the actual Region count would remain within the namespace limit. This is a normal-operation coordination bug; it does not require an injected failure.

## Reproduction and impact

Enable quota support with `hbase.quota.enabled=true`.

1. Create a namespace with `hbase.namespace.quota.maxregions=2` and a table `T` with a split key such as `m`. The table has two primary Regions. Both `hbase:meta` and the Master's namespace quota cache record two Regions for `T`.
2. Disable `T` and run `truncateTable(T, false)`. The truncate procedure generates one new primary Region because `preserveSplits` is false. It removes the old Regions from Meta, adds the new Region, assigns it, and enables the table.
3. After truncate completes, Meta and the actual table contain one primary Region, while the namespace quota cache still records two.
4. Request a split of the new Region at `m`. The split would change actual namespace usage from one Region to two, exactly meeting the limit. `NamespaceStateManager.checkAndUpdateNamespaceRegionCount` instead reads the stale cached count of two, takes its `incr > 0 && regionCount >= maxRegionCount` rejection branch, and `NamespaceAuditor` raises `QuotaExceededException`.

The user-visible effect is that a valid split cannot proceed until the cached usage is reconciled, for example when the Master rebuilds its namespace state from Meta. Other quota decisions can also use this stale count.

## Root cause

`TruncateTableProcedure` coordinates filesystem layout, Meta records, Region assignment, table state, and namespace quota accounting across separate steps. Its `TRUNCATE_TABLE_CLEAR_FS_LAYOUT` state chooses one new Region when `preserveSplits` is false. `TRUNCATE_TABLE_REMOVE_FROM_META` uses `DeleteTableProcedure.deleteAssignmentState`, which clears assignment and table-state data but does not remove or recalculate namespace quota usage. `TRUNCATE_TABLE_ADD_TO_META` uses `CreateTableProcedure.addTableToMeta`, which writes the replacement Regions to Meta but does not run the normal table-creation quota accounting path. Consequently, the Procedure reaches `TRUNCATE_TABLE_ASSIGN_REGIONS` with correct replacement Regions and an old cached count.

This is a missing coordination step between the authoritative Region layout and the Master's in-memory quota ledger. In the two-to-one example, the divergence is `Meta = 1` versus `quota cache = 2`.

## Fix

After the replacement Regions have been written to Meta and before the table is enabled, the truncate procedure sets the namespace quota cache's **absolute per-table count** to the number of default (primary) replicas in its new Region list. Non-default replicas must not count toward the namespace Region quota. An absolute setter is idempotent if the Procedure retries this state, unlike a decrement based on the old count.

This reconciliation does not perform a new quota-limit check: truncate has already replaced the Regions, and a resource-reducing truncate must still complete if an administrator lowered the namespace limit below the table's earlier usage. If the namespace cache is missing, the setter throws `IOException` so the Procedure retries instead of silently completing with stale accounting. System tables remain exempt from namespace quota accounting.

## Validation

The new MiniCluster regression test first failed against the original code: after a two-Region table was truncated to one Region, the cached count remained two (`expected: <1> but was: <2>`). With the fix, the test checks that both the actual and cached counts become one, then uses the public asynchronous split operation and checks that both become two. A second test lowers the namespace limit from two to one before truncate and verifies that the resource-reducing operation still completes and reconciles usage.

The targeted `TestTruncateTableNamespaceQuota` run passed both tests with no failures or errors. `spotless:check` passed for `hbase-server`.
