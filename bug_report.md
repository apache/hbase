<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Space quota release can leave a table disabled after an enable failure

## Reproduction

1. Configure a table space quota with the `DISABLE` violation policy and let the
   table exceed its limit. The quota observer disables the table.
2. Increase the limit so the reported usage is below it.
3. Let `QuotaObserverChore` persist the new, non-violating snapshot, then make
   its `Admin.enableTable` call fail before an enable procedure is submitted.
   A failing `preEnableTable` hook or quota-table read in `HMaster.enableTable`
   can produce this failure.
4. Remove the failure and run the chore again without changing usage or limit.

**Expected:** The next chore retries the enable operation, and the table becomes
enabled.

**Actual before the fix:** The table remains disabled. A public `flush(table)`
fails with `TableNotEnabledException` during the Master's table-state check.

## Cause

In `QuotaObserverChore.updateTableQuota`, the observer first writes the target
snapshot to `hbase:quota`, then replaces its in-memory current snapshot, and
only then calls `enableTableIfNotEnabled`. If enabling fails, both snapshots
say the quota is no longer violated while the table is still `DISABLED`.

On the next run, the computed target equals the in-memory current snapshot, so
the branch containing `enableTableIfNotEnabled` is skipped. The chore's "Will
retry" log message means the chore runs again; it does not retain the failed
table operation. The insufficient-region-reports path has the same ordering.

## Proposed fix

Keep the write to `hbase:quota` before enabling: `HMaster.enableTable` reads
that snapshot and rejects enables while the `DISABLE` violation is recorded.
Move the in-memory `setCurrentState` call after the table enable or disable
operation succeeds, both in `updateTableQuota` and in the insufficient-reports
path. On failure, the old in-memory state retains the transition, allowing the
next chore to retry. The existing enable/disable helpers suppress exceptions
for tables that have already reached the requested state.

The regression test injects one `IOException` from `Admin.enableTable`, checks
that the old snapshot remains current, and verifies that the following update
calls `enableTable` again before advancing the snapshot.

This repair covers retries while the same Master retains its in-memory state.
Recovery across a Master restart in the interval between the persistent
snapshot write and the enable operation would require a durable record of the
pending table-state change.
