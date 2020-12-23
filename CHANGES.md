# HBASE Changelog

<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Be careful doing manual edits in this file. Do not change format
# of release header or remove the below marker. This file is generated.
# DO NOT REMOVE THIS MARKER; FOR INTERPOLATING CHANGES!-->
## Release 2.4.1-rm_test_gcp - Unreleased (as of Wed Dec 23 21:17:01 UTC 2020)
No changes

## Release 2.4.1-rm_test_mac - Unreleased (as of Wed Dec 23 05:56:14 UTC 2020)
No changes

## Release 2.4.1-rm_test-2 - Unreleased (as of Wed Dec 23 04:41:28 UTC 2020)
No changes

## Release 2.4.1-rm_test - Unreleased (as of Wed Dec 23 01:10:53 UTC 2020)
No changes

No changes

No changes

## Release 2.4.0-rm_test - Unreleased (as of Tue Dec 22 19:47:04 UTC 2020)
No changes

## Release 2.4.0 - Unreleased (as of 2020-12-03)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25242](https://issues.apache.org/jira/browse/HBASE-25242) | Add Increment/Append support to RowMutations |  Critical | Client, regionserver |
| [HBASE-25278](https://issues.apache.org/jira/browse/HBASE-25278) | Add option to toggle CACHE\_BLOCKS in count.rb |  Minor | shell |
| [HBASE-18070](https://issues.apache.org/jira/browse/HBASE-18070) | Enable memstore replication for meta replica |  Critical | . |
| [HBASE-24528](https://issues.apache.org/jira/browse/HBASE-24528) | Improve balancer decision observability |  Major | Admin, Balancer, Operability, shell, UI |
| [HBASE-24776](https://issues.apache.org/jira/browse/HBASE-24776) | [hbtop] Support Batch mode |  Major | hbtop |
| [HBASE-24602](https://issues.apache.org/jira/browse/HBASE-24602) | Add Increment and Append support to CheckAndMutate |  Major | . |
| [HBASE-24760](https://issues.apache.org/jira/browse/HBASE-24760) | Add a config hbase.rsgroup.fallback.enable for RSGroup fallback feature |  Major | rsgroup |
| [HBASE-24694](https://issues.apache.org/jira/browse/HBASE-24694) | Support flush a single column family of table |  Major | . |
| [HBASE-24289](https://issues.apache.org/jira/browse/HBASE-24289) | Heterogeneous Storage for Date Tiered Compaction |  Major | Compaction |
| [HBASE-24038](https://issues.apache.org/jira/browse/HBASE-24038) | Add a metric to show the locality of ssd in table.jsp |  Major | metrics |
| [HBASE-8458](https://issues.apache.org/jira/browse/HBASE-8458) | Support for batch version of checkAndMutate() |  Major | Client, regionserver |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25339](https://issues.apache.org/jira/browse/HBASE-25339) | Method parameter and member variable are duplicated in checkSplittable() of SplitTableRegionProcedure |  Minor | . |
| [HBASE-25237](https://issues.apache.org/jira/browse/HBASE-25237) | 'hbase master stop' shuts down the cluster, not the master only |  Major | . |
| [HBASE-25324](https://issues.apache.org/jira/browse/HBASE-25324) | Remove unnecessary array to list conversion in SplitLogManager |  Minor | . |
| [HBASE-25263](https://issues.apache.org/jira/browse/HBASE-25263) | Change encryption key generation algorithm used in the HBase shell |  Major | encryption, shell |
| [HBASE-25323](https://issues.apache.org/jira/browse/HBASE-25323) | Fix potential NPE when the zookeeper path of RegionServerTracker does not exist when start |  Minor | . |
| [HBASE-25281](https://issues.apache.org/jira/browse/HBASE-25281) | Bulkload split hfile too many times due to unreasonable split point |  Minor | tooling |
| [HBASE-25325](https://issues.apache.org/jira/browse/HBASE-25325) | Remove unused class ClusterSchemaException |  Minor | . |
| [HBASE-25213](https://issues.apache.org/jira/browse/HBASE-25213) | Should request Compaction when bulkLoadHFiles is done |  Minor | . |
| [HBASE-24877](https://issues.apache.org/jira/browse/HBASE-24877) | Add option to avoid aborting RS process upon uncaught exceptions happen on replication source |  Major | Replication |
| [HBASE-24664](https://issues.apache.org/jira/browse/HBASE-24664) | Some changing of split region by overall region size rather than only one store size |  Major | regionserver |
| [HBASE-25026](https://issues.apache.org/jira/browse/HBASE-25026) | Create a metric to track full region scans RPCs |  Minor | . |
| [HBASE-25289](https://issues.apache.org/jira/browse/HBASE-25289) | [testing] Clean up resources after tests in rsgroup\_shell\_test.rb |  Major | rsgroup, test |
| [HBASE-25261](https://issues.apache.org/jira/browse/HBASE-25261) | Upgrade Bootstrap to 3.4.1 |  Major | security, UI |
| [HBASE-25272](https://issues.apache.org/jira/browse/HBASE-25272) | Support scan on a specific replica |  Critical | Client, scan |
| [HBASE-25267](https://issues.apache.org/jira/browse/HBASE-25267) | Add SSL keystore type and truststore related configs for HBase RESTServer |  Major | REST |
| [HBASE-25181](https://issues.apache.org/jira/browse/HBASE-25181) | Add options for disabling column family encryption and choosing hash algorithm for wrapped encryption keys. |  Major | encryption |
| [HBASE-25254](https://issues.apache.org/jira/browse/HBASE-25254) | Rewrite TestMultiLogThreshold to remove the LogDelegate in RSRpcServices |  Major | logging, test |
| [HBASE-25252](https://issues.apache.org/jira/browse/HBASE-25252) | Move HMaster inner classes out |  Minor | master |
| [HBASE-25240](https://issues.apache.org/jira/browse/HBASE-25240) | gson format of RpcServer.logResponse is abnormal |  Minor | . |
| [HBASE-25210](https://issues.apache.org/jira/browse/HBASE-25210) | RegionInfo.isOffline is now a duplication with RegionInfo.isSplit |  Major | meta |
| [HBASE-25212](https://issues.apache.org/jira/browse/HBASE-25212) | Optionally abort requests in progress after deciding a region should close |  Major | regionserver |
| [HBASE-24859](https://issues.apache.org/jira/browse/HBASE-24859) | Optimize in-memory representation of mapreduce TableSplit objects |  Major | mapreduce |
| [HBASE-24967](https://issues.apache.org/jira/browse/HBASE-24967) | The table.jsp cost long time to load if the table include closed regions |  Major | UI |
| [HBASE-25167](https://issues.apache.org/jira/browse/HBASE-25167) | Normalizer support for hot config reloading |  Major | master, Normalizer |
| [HBASE-24419](https://issues.apache.org/jira/browse/HBASE-24419) | Normalizer merge plans should consider more than 2 regions when possible |  Major | master, Normalizer |
| [HBASE-25224](https://issues.apache.org/jira/browse/HBASE-25224) | Maximize sleep for checking meta and namespace regions availability |  Major | master |
| [HBASE-25223](https://issues.apache.org/jira/browse/HBASE-25223) | Use try-with-resources statement in snapshot package |  Minor | . |
| [HBASE-25201](https://issues.apache.org/jira/browse/HBASE-25201) | YouAreDeadException should be moved to hbase-server module |  Major | Client |
| [HBASE-25193](https://issues.apache.org/jira/browse/HBASE-25193) | Add support for row prefix and type in the WAL Pretty Printer and some minor fixes |  Minor | wal |
| [HBASE-25128](https://issues.apache.org/jira/browse/HBASE-25128) | RSGroupInfo's toString() and hashCode() does not take into account configuration map. |  Minor | rsgroup |
| [HBASE-24628](https://issues.apache.org/jira/browse/HBASE-24628) | Region normalizer now respects a rate limit |  Major | Normalizer |
| [HBASE-25179](https://issues.apache.org/jira/browse/HBASE-25179) | Assert format is incorrect in HFilePerformanceEvaluation class. |  Minor | Performance, test |
| [HBASE-25065](https://issues.apache.org/jira/browse/HBASE-25065) | WAL archival to be done by a separate thread |  Major | wal |
| [HBASE-14067](https://issues.apache.org/jira/browse/HBASE-14067) | bundle ruby files for hbase shell into a jar. |  Major | shell |
| [HBASE-24875](https://issues.apache.org/jira/browse/HBASE-24875) | Remove the force param for unassign since it dose not take effect any more |  Major | Client |
| [HBASE-24025](https://issues.apache.org/jira/browse/HBASE-24025) | Improve performance of move\_servers\_rsgroup and move\_tables\_rsgroup by using async region move API |  Major | rsgroup |
| [HBASE-25160](https://issues.apache.org/jira/browse/HBASE-25160) | Refactor AccessController and VisibilityController |  Major | . |
| [HBASE-25146](https://issues.apache.org/jira/browse/HBASE-25146) | Add extra logging at info level to HFileCorruptionChecker in order to report progress |  Major | hbck, hbck2 |
| [HBASE-24054](https://issues.apache.org/jira/browse/HBASE-24054) | The Jetty's version number leak occurred while using the thrift service |  Minor | . |
| [HBASE-25091](https://issues.apache.org/jira/browse/HBASE-25091) | Move LogComparator from ReplicationSource to AbstractFSWALProvider#.WALsStartTimeComparator |  Minor | . |
| [HBASE-24981](https://issues.apache.org/jira/browse/HBASE-24981) | Enable table replication fails from 1.x to 2.x if table already exist at peer. |  Major | Replication |
| [HBASE-25109](https://issues.apache.org/jira/browse/HBASE-25109) | Add MR Counters to WALPlayer; currently hard to tell if it is doing anything |  Major | . |
| [HBASE-25082](https://issues.apache.org/jira/browse/HBASE-25082) | Per table WAL metrics: appendCount and appendSize |  Major | metrics |
| [HBASE-25079](https://issues.apache.org/jira/browse/HBASE-25079) | Upgrade Bootstrap to 3.3.7 |  Major | security, UI |
| [HBASE-24976](https://issues.apache.org/jira/browse/HBASE-24976) | REST Server failes to start without any error message |  Major | REST |
| [HBASE-25066](https://issues.apache.org/jira/browse/HBASE-25066) | Use FutureUtils.rethrow in AsyncTableResultScanner to better catch the stack trace |  Major | Client, Scanners |
| [HBASE-25069](https://issues.apache.org/jira/browse/HBASE-25069) |  Display region name instead of encoded region name in HBCK report page. |  Minor | hbck |
| [HBASE-24991](https://issues.apache.org/jira/browse/HBASE-24991) | Replace MovedRegionsCleaner with guava cache |  Minor | . |
| [HBASE-25057](https://issues.apache.org/jira/browse/HBASE-25057) | Fix typo "memeber" |  Trivial | documentation |
| [HBASE-24764](https://issues.apache.org/jira/browse/HBASE-24764) | Add support of adding base peer configs via hbase-site.xml for all replication peers. |  Minor | Replication |
| [HBASE-25037](https://issues.apache.org/jira/browse/HBASE-25037) | Lots of thread pool are changed to non daemon after HBASE-24750 which causes trouble when shutting down |  Major | . |
| [HBASE-24831](https://issues.apache.org/jira/browse/HBASE-24831) | Avoid invoke Counter using reflection  in SnapshotInputFormat |  Major | . |
| [HBASE-25002](https://issues.apache.org/jira/browse/HBASE-25002) | Create simple pattern matching query for retrieving metrics matching the pattern |  Major | . |
| [HBASE-25022](https://issues.apache.org/jira/browse/HBASE-25022) | Remove 'hbase.testing.nocluster' config |  Major | test |
| [HBASE-25006](https://issues.apache.org/jira/browse/HBASE-25006) | Make the cost functions optional for StochastoicBalancer |  Major | . |
| [HBASE-24974](https://issues.apache.org/jira/browse/HBASE-24974) | Provide a flexibility to print only row key and filter for multiple tables in the WALPrettyPrinter |  Minor | wal |
| [HBASE-24994](https://issues.apache.org/jira/browse/HBASE-24994) | Add hedgedReadOpsInCurThread metric |  Minor | metrics |
| [HBASE-25005](https://issues.apache.org/jira/browse/HBASE-25005) | Refactor CatalogJanitor |  Major | master, meta |
| [HBASE-24992](https://issues.apache.org/jira/browse/HBASE-24992) | log after Generator success when running ITBLL |  Trivial | . |
| [HBASE-24937](https://issues.apache.org/jira/browse/HBASE-24937) | table.rb use LocalDateTime to replace Instant |  Minor | shell |
| [HBASE-24940](https://issues.apache.org/jira/browse/HBASE-24940) | runCatalogJanitor() API should return -1 to indicate already running status |  Major | . |
| [HBASE-24973](https://issues.apache.org/jira/browse/HBASE-24973) | Remove read point parameter in method StoreFlush#performFlush and StoreFlush#createScanner |  Minor | . |
| [HBASE-24569](https://issues.apache.org/jira/browse/HBASE-24569) | Get hostAndWeights in addition using localhost if it is null in local mode |  Minor | regionserver |
| [HBASE-24949](https://issues.apache.org/jira/browse/HBASE-24949) | Optimize FSTableDescriptors.get to not always go to fs when cache miss |  Major | master |
| [HBASE-24898](https://issues.apache.org/jira/browse/HBASE-24898) | Use EnvironmentEdge.currentTime() instead of System.currentTimeMillis() in CurrentHourProvider |  Major | tooling |
| [HBASE-24942](https://issues.apache.org/jira/browse/HBASE-24942) | MergeTableRegionsProcedure should not call clean merge region |  Major | proc-v2, Region Assignment |
| [HBASE-24811](https://issues.apache.org/jira/browse/HBASE-24811) | Use class access static field or method |  Minor | . |
| [HBASE-24686](https://issues.apache.org/jira/browse/HBASE-24686) | [LOG] Log improvement in Connection#close |  Major | Client, logging |
| [HBASE-24912](https://issues.apache.org/jira/browse/HBASE-24912) | Enlarge MemstoreFlusherChore/CompactionChecker period for unit test |  Major | . |
| [HBASE-24627](https://issues.apache.org/jira/browse/HBASE-24627) | Normalize one table at a time |  Major | Normalizer |
| [HBASE-24872](https://issues.apache.org/jira/browse/HBASE-24872) | refactor valueOf PoolType |  Minor | Client |
| [HBASE-24854](https://issues.apache.org/jira/browse/HBASE-24854) | Correct the help content of assign and unassign commands in hbase shell |  Minor | shell |
| [HBASE-24750](https://issues.apache.org/jira/browse/HBASE-24750) | All executor service should start using guava ThreadFactory |  Major | . |
| [HBASE-24709](https://issues.apache.org/jira/browse/HBASE-24709) | Support MoveCostFunction use a lower multiplier in offpeak hours |  Major | Balancer |
| [HBASE-24824](https://issues.apache.org/jira/browse/HBASE-24824) | Add more stats in PE for read replica |  Minor | PE, read replicas |
| [HBASE-21721](https://issues.apache.org/jira/browse/HBASE-21721) | FSHLog : reduce write#syncs() times |  Major | . |
| [HBASE-24404](https://issues.apache.org/jira/browse/HBASE-24404) | Support flush a single column family of region |  Major | shell |
| [HBASE-24826](https://issues.apache.org/jira/browse/HBASE-24826) | Add some comments for processlist in hbase shell |  Minor | shell |
| [HBASE-24659](https://issues.apache.org/jira/browse/HBASE-24659) | Calculate FIXED\_OVERHEAD automatically |  Major | . |
| [HBASE-24827](https://issues.apache.org/jira/browse/HBASE-24827) | BackPort HBASE-11554 Remove Reusable poolmap Rpc client type. |  Major | Client |
| [HBASE-24823](https://issues.apache.org/jira/browse/HBASE-24823) | Port HBASE-22762 Print the delta between phases in the split/merge/compact/flush transaction journals to master branch |  Minor | . |
| [HBASE-24795](https://issues.apache.org/jira/browse/HBASE-24795) | RegionMover should deal with unknown (split/merged) regions |  Major | . |
| [HBASE-24821](https://issues.apache.org/jira/browse/HBASE-24821) | Simplify the logic of getRegionInfo in TestFlushFromClient to reduce redundancy code |  Minor | test |
| [HBASE-24704](https://issues.apache.org/jira/browse/HBASE-24704) | Make the Table Schema easier to view even there are multiple families |  Major | UI |
| [HBASE-24695](https://issues.apache.org/jira/browse/HBASE-24695) | FSHLog - close the current WAL file in a background thread |  Major | . |
| [HBASE-24803](https://issues.apache.org/jira/browse/HBASE-24803) | Unify hbase-shell ::Shell::Commands::Command#help behavior |  Minor | shell |
| [HBASE-11686](https://issues.apache.org/jira/browse/HBASE-11686) | Shell code should create a binding / irb workspace instead of polluting the root namespace |  Minor | shell |
| [HBASE-20226](https://issues.apache.org/jira/browse/HBASE-20226) | Performance Improvement Taking Large Snapshots In Remote Filesystems |  Minor | snapshots |
| [HBASE-24669](https://issues.apache.org/jira/browse/HBASE-24669) | Logging of ppid should be consistent across all occurrences |  Minor | Operability, proc-v2 |
| [HBASE-24757](https://issues.apache.org/jira/browse/HBASE-24757) | ReplicationSink should limit the batch rowcount for batch mutations based on hbase.rpc.rows.warning.threshold |  Major | . |
| [HBASE-24777](https://issues.apache.org/jira/browse/HBASE-24777) | InfoServer support ipv6 host and port |  Minor | UI |
| [HBASE-24758](https://issues.apache.org/jira/browse/HBASE-24758) | Avoid flooding replication source RSes logs when no sinks are available |  Major | Replication |
| [HBASE-24743](https://issues.apache.org/jira/browse/HBASE-24743) | Reject to add a peer which replicate to itself earlier |  Major | . |
| [HBASE-24696](https://issues.apache.org/jira/browse/HBASE-24696) | Include JVM information on Web UI under "Software Attributes" |  Minor | UI |
| [HBASE-24747](https://issues.apache.org/jira/browse/HBASE-24747) | Log an ERROR if HBaseSaslRpcServer initialisation fails with an uncaught exception |  Major | . |
| [HBASE-24586](https://issues.apache.org/jira/browse/HBASE-24586) | Add table level locality in table.jsp |  Major | UI |
| [HBASE-24663](https://issues.apache.org/jira/browse/HBASE-24663) | Add procedure process time statistics UI |  Major | . |
| [HBASE-24653](https://issues.apache.org/jira/browse/HBASE-24653) | Show snapshot owner on Master WebUI |  Major | . |
| [HBASE-24431](https://issues.apache.org/jira/browse/HBASE-24431) | RSGroupInfo add configuration map to store something extra |  Major | rsgroup |
| [HBASE-24671](https://issues.apache.org/jira/browse/HBASE-24671) | Add excludefile and designatedfile options to graceful\_stop.sh |  Major | . |
| [HBASE-24560](https://issues.apache.org/jira/browse/HBASE-24560) | Add a new option of designatedfile in RegionMover |  Major | . |
| [HBASE-24382](https://issues.apache.org/jira/browse/HBASE-24382) | Flush partial stores of region filtered by seqId when archive wal due to too many wals |  Major | wal |
| [HBASE-24208](https://issues.apache.org/jira/browse/HBASE-24208) | Remove RS entry from zk draining servers node after RS been stopped |  Major | . |
| [HBASE-24456](https://issues.apache.org/jira/browse/HBASE-24456) | Immutable Scan as unmodifiable subclass or wrapper of Scan |  Major | . |
| [HBASE-24471](https://issues.apache.org/jira/browse/HBASE-24471) | The way we bootstrap meta table is confusing |  Major | master, meta, proc-v2 |
| [HBASE-24350](https://issues.apache.org/jira/browse/HBASE-24350) | HBase table level replication metrics for shippedBytes are always 0 |  Major | Replication |
| [HBASE-24311](https://issues.apache.org/jira/browse/HBASE-24311) | Add more details in MultiVersionConcurrencyControl STUCK log message |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25355](https://issues.apache.org/jira/browse/HBASE-25355) | [Documentation]  Fix spelling error |  Trivial | documentation |
| [HBASE-25230](https://issues.apache.org/jira/browse/HBASE-25230) | Embedded zookeeper server not clean up the old data |  Minor | Zookeeper |
| [HBASE-25349](https://issues.apache.org/jira/browse/HBASE-25349) | [Flakey Tests] branch-2 TestRefreshRecoveredReplication.testReplicationRefreshSource:141 Waiting timed out after [60,000] msec |  Major | flakies |
| [HBASE-25332](https://issues.apache.org/jira/browse/HBASE-25332) | one NPE |  Major | Zookeeper |
| [HBASE-25345](https://issues.apache.org/jira/browse/HBASE-25345) | [Flakey Tests] branch-2 TestReadReplicas#testVerifySecondaryAbilityToReadWithOnFiles |  Major | test |
| [HBASE-25307](https://issues.apache.org/jira/browse/HBASE-25307) | ThreadLocal pooling leads to NullPointerException |  Major | Client |
| [HBASE-25341](https://issues.apache.org/jira/browse/HBASE-25341) | Fix ErrorProne error which causes nightly to fail |  Major | test |
| [HBASE-25330](https://issues.apache.org/jira/browse/HBASE-25330) | RSGroupInfoManagerImpl#moveServers return is not set of servers moved |  Major | rsgroup |
| [HBASE-25321](https://issues.apache.org/jira/browse/HBASE-25321) | The sort icons not shown after Upgrade JQuery to 3.5.1 |  Major | UI |
| [HBASE-24268](https://issues.apache.org/jira/browse/HBASE-24268) | REST and Thrift server do not handle the "doAs" parameter case insensitively |  Minor | REST, Thrift |
| [HBASE-25050](https://issues.apache.org/jira/browse/HBASE-25050) | We initialize Filesystems more than once. |  Minor | . |
| [HBASE-25311](https://issues.apache.org/jira/browse/HBASE-25311) | ui throws NPE |  Major | . |
| [HBASE-25306](https://issues.apache.org/jira/browse/HBASE-25306) | The log in SimpleLoadBalancer#onConfigurationChange is wrong |  Major | . |
| [HBASE-25300](https://issues.apache.org/jira/browse/HBASE-25300) | 'Unknown table hbase:quota' happens when desc table in shell if quota disabled |  Major | shell |
| [HBASE-25255](https://issues.apache.org/jira/browse/HBASE-25255) | Master fails to initialize when creating rs group table |  Critical | master, rsgroup |
| [HBASE-25275](https://issues.apache.org/jira/browse/HBASE-25275) | Upgrade asciidoctor |  Blocker | website |
| [HBASE-25276](https://issues.apache.org/jira/browse/HBASE-25276) | Need to throw the original exception in HRegion#openHRegion |  Major | . |
| [HBASE-20598](https://issues.apache.org/jira/browse/HBASE-20598) | Upgrade to JRuby 9.2 |  Major | dependencies, shell |
| [HBASE-25216](https://issues.apache.org/jira/browse/HBASE-25216) | The client zk syncer should deal with meta replica count change |  Major | master, Zookeeper |
| [HBASE-25238](https://issues.apache.org/jira/browse/HBASE-25238) | Upgrading HBase from 2.2.0 to 2.3.x fails because of “Message missing required fields: state” |  Critical | . |
| [HBASE-25234](https://issues.apache.org/jira/browse/HBASE-25234) | [Upgrade]Incompatibility in reading RS report from 2.1 RS when Master is upgraded to a version containing HBASE-21406 |  Major | . |
| [HBASE-25053](https://issues.apache.org/jira/browse/HBASE-25053) | WAL replay should ignore 0-length files |  Major | master, regionserver |
| [HBASE-25090](https://issues.apache.org/jira/browse/HBASE-25090) | CompactionConfiguration logs unrealistic store file sizes |  Minor | Compaction |
| [HBASE-24977](https://issues.apache.org/jira/browse/HBASE-24977) | Meta table shouldn't be modified as read only |  Major | meta |
| [HBASE-25176](https://issues.apache.org/jira/browse/HBASE-25176) | MasterStoppedException should be moved to hbase-client module |  Major | Client |
| [HBASE-25206](https://issues.apache.org/jira/browse/HBASE-25206) | Data loss can happen if a cloned table loses original split region(delete table) |  Major | proc-v2, Region Assignment, snapshots |
| [HBASE-25207](https://issues.apache.org/jira/browse/HBASE-25207) | Revisit the implementation and usage of RegionStates.include |  Major | Region Assignment |
| [HBASE-25186](https://issues.apache.org/jira/browse/HBASE-25186) | TestMasterRegionOnTwoFileSystems is failing after HBASE-25065 |  Blocker | master |
| [HBASE-25204](https://issues.apache.org/jira/browse/HBASE-25204) | Nightly job failed as  the name of jdk and maven changed |  Major | . |
| [HBASE-25093](https://issues.apache.org/jira/browse/HBASE-25093) | the RSGroupBasedLoadBalancer#retainAssignment throws NPE |  Major | rsgroup |
| [HBASE-25117](https://issues.apache.org/jira/browse/HBASE-25117) | ReplicationSourceShipper thread can not be finished |  Major | . |
| [HBASE-25168](https://issues.apache.org/jira/browse/HBASE-25168) | Unify WAL name timestamp parsers |  Major | . |
| [HBASE-23834](https://issues.apache.org/jira/browse/HBASE-23834) | HBase fails to run on Hadoop 3.3.0/3.2.2/3.1.4 due to jetty version mismatch |  Major | dependencies |
| [HBASE-25165](https://issues.apache.org/jira/browse/HBASE-25165) | Change 'State time' in UI so sorts |  Minor | UI |
| [HBASE-25048](https://issues.apache.org/jira/browse/HBASE-25048) | [HBCK2] Bypassed parent procedures are not updated in store |  Major | hbck2, proc-v2 |
| [HBASE-25147](https://issues.apache.org/jira/browse/HBASE-25147) | Should store the regionNames field in state data for ReopenTableRegionsProcedure |  Major | proc-v2 |
| [HBASE-25115](https://issues.apache.org/jira/browse/HBASE-25115) | HFilePrettyPrinter can't seek to the row which is the first row of a hfile |  Major | HFile, tooling |
| [HBASE-25135](https://issues.apache.org/jira/browse/HBASE-25135) | Convert the internal seperator while emitting the memstore read metrics to # |  Minor | . |
| [HBASE-24665](https://issues.apache.org/jira/browse/HBASE-24665) | MultiWAL :  Avoid rolling of ALL WALs when one of the WAL needs a roll |  Major | wal |
| [HBASE-25096](https://issues.apache.org/jira/browse/HBASE-25096) | WAL size in RegionServer UI is wrong |  Major | . |
| [HBASE-25077](https://issues.apache.org/jira/browse/HBASE-25077) | hbck.jsp page loading fails, logs NPE in master log. |  Major | hbck |
| [HBASE-25088](https://issues.apache.org/jira/browse/HBASE-25088) | CatalogFamilyFormat/MetaTableAccessor.parseRegionInfoFromRegionName incorrectly setEndKey to regionId |  Critical | meta |
| [HBASE-25097](https://issues.apache.org/jira/browse/HBASE-25097) | Wrong RIT page number in Master UI |  Minor | UI |
| [HBASE-24896](https://issues.apache.org/jira/browse/HBASE-24896) | 'Stuck' in static initialization creating RegionInfo instance |  Major | . |
| [HBASE-24956](https://issues.apache.org/jira/browse/HBASE-24956) | ConnectionManager#locateRegionInMeta waits for user region lock indefinitely. |  Major | Client |
| [HBASE-24481](https://issues.apache.org/jira/browse/HBASE-24481) | HBase Rest: Request for region detail of a table which doesn't exits is success(200 success code) instead of 404 |  Minor | . |
| [HBASE-25047](https://issues.apache.org/jira/browse/HBASE-25047) | WAL split edits number is negative in RegionServerUI |  Minor | UI, wal |
| [HBASE-25021](https://issues.apache.org/jira/browse/HBASE-25021) | Nightly job should skip hadoop-2 integration test for master |  Major | build, scripts |
| [HBASE-25012](https://issues.apache.org/jira/browse/HBASE-25012) | HBASE-24359 causes replication missed log of some RemoteException |  Major | Replication |
| [HBASE-25009](https://issues.apache.org/jira/browse/HBASE-25009) | Hbck chore logs wrong message when loading regions from RS report |  Minor | . |
| [HBASE-25014](https://issues.apache.org/jira/browse/HBASE-25014) | ScheduledChore is never triggered when initalDelay \> 1.5\*period |  Major | . |
| [HBASE-25016](https://issues.apache.org/jira/browse/HBASE-25016) | Should close ResultScanner in MetaTableAccessor.scanByRegionEncodedName |  Critical | master, meta |
| [HBASE-24958](https://issues.apache.org/jira/browse/HBASE-24958) | CompactingMemStore.timeOfOldestEdit error update |  Critical | regionserver |
| [HBASE-24995](https://issues.apache.org/jira/browse/HBASE-24995) | MetaFixer fails to fix overlaps when multiple tables have overlaps |  Major | hbck2 |
| [HBASE-24719](https://issues.apache.org/jira/browse/HBASE-24719) | Renaming invalid rsgroup throws NPE instead of proper error message |  Major | . |
| [HBASE-19352](https://issues.apache.org/jira/browse/HBASE-19352) | Port HADOOP-10379: Protect authentication cookies with the HttpOnly and Secure flags |  Major | . |
| [HBASE-24971](https://issues.apache.org/jira/browse/HBASE-24971) | Upgrade JQuery to 3.5.1 |  Major | security, UI |
| [HBASE-24968](https://issues.apache.org/jira/browse/HBASE-24968) | One of static initializers of CellComparatorImpl referring to subclass MetaCellComparator |  Major | . |
| [HBASE-24916](https://issues.apache.org/jira/browse/HBASE-24916) | Region hole contains wrong regions pair when hole is created by first region deletion |  Major | hbck2 |
| [HBASE-24885](https://issues.apache.org/jira/browse/HBASE-24885) | STUCK RIT by hbck2 assigns |  Major | hbck2, Region Assignment |
| [HBASE-24926](https://issues.apache.org/jira/browse/HBASE-24926) | Should call setFailure in MergeTableRegionsProcedure when isMergeable returns false |  Major | master, proc-v2 |
| [HBASE-24884](https://issues.apache.org/jira/browse/HBASE-24884) | BulkLoadHFilesTool/LoadIncrementalHFiles should accept -D options from command line parameters |  Minor | . |
| [HBASE-24583](https://issues.apache.org/jira/browse/HBASE-24583) | Normalizer can't actually merge empty regions when neighbor is larger than average size |  Major | master, Normalizer |
| [HBASE-24844](https://issues.apache.org/jira/browse/HBASE-24844) | Exception on standalone (master) shutdown |  Minor | Zookeeper |
| [HBASE-24856](https://issues.apache.org/jira/browse/HBASE-24856) | Fix error prone error in FlushTableSubprocedure |  Major | . |
| [HBASE-24838](https://issues.apache.org/jira/browse/HBASE-24838) | The pre commit job fails to archive surefire reports |  Critical | build, scripts |
| [HBASE-23157](https://issues.apache.org/jira/browse/HBASE-23157) | WAL unflushed seqId tracking may wrong when Durability.ASYNC\_WAL is used |  Major | regionserver, wal |
| [HBASE-24625](https://issues.apache.org/jira/browse/HBASE-24625) | AsyncFSWAL.getLogFileSizeIfBeingWritten does not return the expected synced file length. |  Critical | Replication, wal |
| [HBASE-24830](https://issues.apache.org/jira/browse/HBASE-24830) | Some tests involving RS crash fail with NullPointerException after HBASE-24632 in branch-2 |  Major | . |
| [HBASE-24788](https://issues.apache.org/jira/browse/HBASE-24788) | Fix the connection leaks on getting hbase admin from unclosed connection |  Major | mapreduce |
| [HBASE-24805](https://issues.apache.org/jira/browse/HBASE-24805) | HBaseTestingUtility.getConnection should be threadsafe |  Major | test |
| [HBASE-24808](https://issues.apache.org/jira/browse/HBASE-24808) | skip empty log cleaner delegate class names (WAS =\> cleaner.CleanerChore: Can NOT create CleanerDelegate= ClassNotFoundException) |  Trivial | . |
| [HBASE-24767](https://issues.apache.org/jira/browse/HBASE-24767) | Change default to false for HBASE-15519 per-user metrics |  Major | metrics |
| [HBASE-24713](https://issues.apache.org/jira/browse/HBASE-24713) | RS startup with FSHLog throws NPE after HBASE-21751 |  Minor | wal |
| [HBASE-24794](https://issues.apache.org/jira/browse/HBASE-24794) | hbase.rowlock.wait.duration should not be \<= 0 |  Minor | regionserver |
| [HBASE-24797](https://issues.apache.org/jira/browse/HBASE-24797) | Move log code out of loop |  Minor | Normalizer |
| [HBASE-24752](https://issues.apache.org/jira/browse/HBASE-24752) | NPE/500 accessing webui on master startup |  Minor | master |
| [HBASE-24766](https://issues.apache.org/jira/browse/HBASE-24766) | Document Remote Procedure Execution |  Major | documentation |
| [HBASE-11676](https://issues.apache.org/jira/browse/HBASE-11676) | Scan FORMATTER is not applied for columns using non-printable name in shell |  Minor | shell |
| [HBASE-24738](https://issues.apache.org/jira/browse/HBASE-24738) | [Shell] processlist command fails with ERROR: Unexpected end of file from server when SSL enabled |  Major | shell |
| [HBASE-24675](https://issues.apache.org/jira/browse/HBASE-24675) | On Master restart all servers are assigned to default rsgroup. |  Major | rsgroup |
| [HBASE-22146](https://issues.apache.org/jira/browse/HBASE-22146) | SpaceQuotaViolationPolicy Disable is not working in Namespace level |  Major | . |
| [HBASE-24742](https://issues.apache.org/jira/browse/HBASE-24742) | Improve performance of SKIP vs SEEK logic |  Major | Performance, regionserver |
| [HBASE-24710](https://issues.apache.org/jira/browse/HBASE-24710) | Incorrect checksum calculation in saveVersion.sh |  Major | scripts |
| [HBASE-24714](https://issues.apache.org/jira/browse/HBASE-24714) | Error message is displayed in the UI of table's compaction state if any region of that table is not open. |  Major | Compaction, UI |
| [HBASE-24748](https://issues.apache.org/jira/browse/HBASE-24748) | Add hbase.master.balancer.stochastic.moveCost.offpeak to doc as support dynamically change |  Minor | documentation |
| [HBASE-24746](https://issues.apache.org/jira/browse/HBASE-24746) | The sort icons overlap the col name in master UI |  Major | UI |
| [HBASE-24721](https://issues.apache.org/jira/browse/HBASE-24721) | rename\_rsgroup overwriting the existing rsgroup. |  Major | . |
| [HBASE-24615](https://issues.apache.org/jira/browse/HBASE-24615) | MutableRangeHistogram#updateSnapshotRangeMetrics doesn't calculate the distribution for last bucket. |  Major | metrics |
| [HBASE-24705](https://issues.apache.org/jira/browse/HBASE-24705) | MetaFixer#fixHoles() does not include the case for read replicas (i.e, replica regions are not created) |  Major | read replicas |
| [HBASE-24720](https://issues.apache.org/jira/browse/HBASE-24720) | Meta replicas not cleaned when disabled |  Minor | read replicas |
| [HBASE-24693](https://issues.apache.org/jira/browse/HBASE-24693) | regioninfo#isLast() has a logic error |  Minor | . |
| [HBASE-23744](https://issues.apache.org/jira/browse/HBASE-23744) | FastPathBalancedQueueRpcExecutor should enforce queue length of 0 |  Minor | . |
| [HBASE-22738](https://issues.apache.org/jira/browse/HBASE-22738) | Fallback to default group to choose RS when there are no RS in current group |  Major | rsgroup |
| [HBASE-23126](https://issues.apache.org/jira/browse/HBASE-23126) | IntegrationTestRSGroup is useless now |  Major | rsgroup |
| [HBASE-24518](https://issues.apache.org/jira/browse/HBASE-24518) | waitForNamespaceOnline() should return false if any region is offline |  Major | . |
| [HBASE-24564](https://issues.apache.org/jira/browse/HBASE-24564) | Make RS abort call idempotent |  Major | regionserver |
| [HBASE-24340](https://issues.apache.org/jira/browse/HBASE-24340) | PerformanceEvaluation options should not mandate any specific order |  Minor | . |
| [HBASE-24130](https://issues.apache.org/jira/browse/HBASE-24130) | rat plugin complains about having an unlicensed file. |  Minor | . |
| [HBASE-24017](https://issues.apache.org/jira/browse/HBASE-24017) | Turn down flakey rerun rate on all but hot branches |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-24015](https://issues.apache.org/jira/browse/HBASE-24015) | Coverage for Assign and Unassign of Regions on RegionServer on failure |  Major | amv2 |
| [HBASE-25156](https://issues.apache.org/jira/browse/HBASE-25156) | TestMasterFailover.testSimpleMasterFailover is flaky |  Major | test |
| [HBASE-24979](https://issues.apache.org/jira/browse/HBASE-24979) | Include batch mutatations in client operation timeout tests |  Major | . |
| [HBASE-24894](https://issues.apache.org/jira/browse/HBASE-24894) | [Flakey Test] TestStochasticLoadBalancer.testMoveCostMultiplier |  Major | Balancer, master, test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25127](https://issues.apache.org/jira/browse/HBASE-25127) | Enhance PerformanceEvaluation to profile meta replica performance. |  Major | . |
| [HBASE-25284](https://issues.apache.org/jira/browse/HBASE-25284) | Check-in "Enable memstore replication..." design |  Major | . |
| [HBASE-25126](https://issues.apache.org/jira/browse/HBASE-25126) | Add load balance logic in hbase-client to distribute read load over meta replica regions. |  Major | . |
| [HBASE-25291](https://issues.apache.org/jira/browse/HBASE-25291) | Document how to enable the meta replica load balance mode for the client and clean up around hbase:meta read replicas |  Major | . |
| [HBASE-25253](https://issues.apache.org/jira/browse/HBASE-25253) | Deprecated master carrys regions related methods and configs |  Major | Balancer, master |
| [HBASE-25203](https://issues.apache.org/jira/browse/HBASE-25203) | Change the reference url to flaky list in our jenkins jobs |  Major | flakies, jenkins |
| [HBASE-25194](https://issues.apache.org/jira/browse/HBASE-25194) | Do not publish workspace in flaky find job |  Major | jenkins |
| [HBASE-25169](https://issues.apache.org/jira/browse/HBASE-25169) | Update documentation about meta region replica |  Major | documentation |
| [HBASE-25164](https://issues.apache.org/jira/browse/HBASE-25164) | Make ModifyTableProcedure support changing meta replica count |  Major | meta, read replicas |
| [HBASE-25162](https://issues.apache.org/jira/browse/HBASE-25162) | Make flaky tests run more aggressively |  Major | jenkins, scripts, test |
| [HBASE-25163](https://issues.apache.org/jira/browse/HBASE-25163) | Increase the timeout value for nightly jobs |  Major | jenkins, scripts, test |
| [HBASE-22976](https://issues.apache.org/jira/browse/HBASE-22976) | [HBCK2] Add RecoveredEditsPlayer |  Major | hbck2, walplayer |
| [HBASE-25124](https://issues.apache.org/jira/browse/HBASE-25124) | Support changing region replica count without disabling table |  Major | meta, proc-v2 |
| [HBASE-23959](https://issues.apache.org/jira/browse/HBASE-23959) | Fix javadoc for JDK11 |  Major | . |
| [HBASE-25151](https://issues.apache.org/jira/browse/HBASE-25151) | warmupRegion frustrates registering WALs on the catalog replicationsource |  Major | read replicas |
| [HBASE-25154](https://issues.apache.org/jira/browse/HBASE-25154) | Set java.io.tmpdir to project build directory to avoid writing std\*deferred files to /tmp |  Major | build, test |
| [HBASE-25121](https://issues.apache.org/jira/browse/HBASE-25121) | Refactor MetaTableAccessor.addRegionsToMeta and its usage places |  Major | meta |
| [HBASE-25055](https://issues.apache.org/jira/browse/HBASE-25055) | Add ReplicationSource for meta WALs; add enable/disable when hbase:meta assigned to RS |  Major | . |
| [HBASE-25133](https://issues.apache.org/jira/browse/HBASE-25133) | Migrate HBase Nightly jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25132](https://issues.apache.org/jira/browse/HBASE-25132) | Migrate flaky test jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25103](https://issues.apache.org/jira/browse/HBASE-25103) | Remove ZNodePaths.metaReplicaZNodes |  Major | . |
| [HBASE-25107](https://issues.apache.org/jira/browse/HBASE-25107) | Migrate flaky reporting jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25068](https://issues.apache.org/jira/browse/HBASE-25068) | Pass WALFactory to Replication so it knows of all WALProviders, not just default/user-space |  Minor | . |
| [HBASE-25067](https://issues.apache.org/jira/browse/HBASE-25067) | Edit of log messages around async WAL Replication; checkstyle fixes; and a bugfix |  Major | . |
| [HBASE-24857](https://issues.apache.org/jira/browse/HBASE-24857) |  Fix several problems when starting webUI |  Minor | canary, UI |
| [HBASE-24964](https://issues.apache.org/jira/browse/HBASE-24964) | Remove MetaTableAccessor.tableExists |  Major | meta |
| [HBASE-24765](https://issues.apache.org/jira/browse/HBASE-24765) | Dynamic master discovery |  Major | Client |
| [HBASE-24945](https://issues.apache.org/jira/browse/HBASE-24945) | Remove MetaTableAccessor.getRegionCount |  Major | mapreduce, meta |
| [HBASE-24944](https://issues.apache.org/jira/browse/HBASE-24944) | Remove MetaTableAccessor.getTableRegionsAndLocations in hbase-rest module |  Major | meta, REST |
| [HBASE-24918](https://issues.apache.org/jira/browse/HBASE-24918) | Make RegionInfo#UNDEFINED IA.Private |  Major | . |
| [HBASE-24806](https://issues.apache.org/jira/browse/HBASE-24806) | Small Updates to Functionality of Shell IRB Workspace |  Major | shell |
| [HBASE-24876](https://issues.apache.org/jira/browse/HBASE-24876) | Fix the flaky job url in hbase-personality.sh |  Major | . |
| [HBASE-24841](https://issues.apache.org/jira/browse/HBASE-24841) | Change the jenkins job urls in our jenkinsfile |  Major | build, scripts |
| [HBASE-24680](https://issues.apache.org/jira/browse/HBASE-24680) | Refactor the checkAndMutate code on the server side |  Major | . |
| [HBASE-24817](https://issues.apache.org/jira/browse/HBASE-24817) | Allow configuring WALEntry filters on ReplicationSource |  Major | Replication, wal |
| [HBASE-24632](https://issues.apache.org/jira/browse/HBASE-24632) | Enable procedure-based log splitting as default in hbase3 |  Major | wal |
| [HBASE-24718](https://issues.apache.org/jira/browse/HBASE-24718) | Generic NamedQueue framework for recent in-memory history (refactor slowlog) |  Major | . |
| [HBASE-24698](https://issues.apache.org/jira/browse/HBASE-24698) | Turn OFF Canary WebUI as default |  Major | canary |
| [HBASE-24650](https://issues.apache.org/jira/browse/HBASE-24650) | Change the return types of the new checkAndMutate methods introduced in HBASE-8458 |  Major | Client |
| [HBASE-24013](https://issues.apache.org/jira/browse/HBASE-24013) | Bump branch-2 version to 2.4.0-SNAPSHOT |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25099](https://issues.apache.org/jira/browse/HBASE-25099) | Change meta replica count by altering meta table descriptor |  Major | meta, read replicas |
| [HBASE-25320](https://issues.apache.org/jira/browse/HBASE-25320) | Upgrade hbase-thirdparty dependency to 3.4.1 |  Blocker | dependencies |
| [HBASE-24640](https://issues.apache.org/jira/browse/HBASE-24640) | Purge use of VisibleForTesting |  Major | community |
| [HBASE-24081](https://issues.apache.org/jira/browse/HBASE-24081) | Provide documentation for running Yetus with HBase |  Major | documentation |
| [HBASE-24667](https://issues.apache.org/jira/browse/HBASE-24667) | Rename configs that support atypical DNS set ups to put them in hbase.unsafe |  Major | conf, Operability |
| [HBASE-25228](https://issues.apache.org/jira/browse/HBASE-25228) | Delete dev-support/jenkins\_precommit\_jira\_yetus.sh |  Minor | build |
| [HBASE-24200](https://issues.apache.org/jira/browse/HBASE-24200) | Upgrade to Yetus 0.12.0 |  Minor | build |
| [HBASE-25120](https://issues.apache.org/jira/browse/HBASE-25120) | Remove the deprecated annotation for MetaTableAccessor.getScanForTableName |  Major | meta |
| [HBASE-25073](https://issues.apache.org/jira/browse/HBASE-25073) | Should not use XXXService.Interface.class.getSimpleName as stub key prefix in AsyncConnectionImpl |  Major | Client |
| [HBASE-25072](https://issues.apache.org/jira/browse/HBASE-25072) | Remove the unnecessary System.out.println in MasterRegistry |  Minor | Client |
| [HBASE-25004](https://issues.apache.org/jira/browse/HBASE-25004) | Log RegionTooBusyException details |  Major | . |
| [HBASE-24993](https://issues.apache.org/jira/browse/HBASE-24993) | Remove OfflineMetaRebuildTestCore |  Major | test |
| [HBASE-24809](https://issues.apache.org/jira/browse/HBASE-24809) | Yetus IA Javadoc links are no longer available |  Minor | . |
| [HBASE-14847](https://issues.apache.org/jira/browse/HBASE-14847) | Add FIFO compaction section to HBase book |  Major | documentation |
| [HBASE-24843](https://issues.apache.org/jira/browse/HBASE-24843) | Sort the constants in \`hbase\_constants.rb\` |  Minor | shell |
| [HBASE-24835](https://issues.apache.org/jira/browse/HBASE-24835) | Normalizer should log a successful run at INFO level |  Minor | Normalizer |
| [HBASE-24779](https://issues.apache.org/jira/browse/HBASE-24779) | Improve insight into replication WAL readers hung on checkQuota |  Minor | Replication |
| [HBASE-24662](https://issues.apache.org/jira/browse/HBASE-24662) | Update DumpClusterStatusAction to notice changes in region server count |  Major | integration tests |
| [HBASE-24658](https://issues.apache.org/jira/browse/HBASE-24658) | Update PolicyBasedChaosMonkey to handle uncaught exceptions |  Minor | integration tests |
| [HBASE-24648](https://issues.apache.org/jira/browse/HBASE-24648) | Remove the legacy 'forceSplit' related code at region server side |  Major | regionserver |
| [HBASE-24492](https://issues.apache.org/jira/browse/HBASE-24492) | ProtobufLogReader.readNext does not need looping |  Minor | Replication, wal |
| [HBASE-22033](https://issues.apache.org/jira/browse/HBASE-22033) | Update to maven-javadoc-plugin 3.2.0 and switch to non-forking aggregate goals |  Major | build, website |


## Release 2.3.0

[CHANGES.md at rel/2.3.0 (e0e1382)](https://github.com/apache/hbase/blob/rel/2.3.0/CHANGES.md)

## Release 2.2.0

[CHANGES.md at rel/2.2.0 (3ec6932)](https://github.com/apache/hbase/blob/rel/2.2.0/CHANGES.md)

## Release 2.1.0

[CHANGES.md at rel/2.1.0 (e1673bb)](https://github.com/apache/hbase/blob/rel/2.1.0/CHANGES.md)

## Release 2.0.0

[CHANGES.md at rel/2.0.0 (7483b11)](https://github.com/apache/hbase/blob/rel/2.0.0/CHANGES.md)

## Release 1.0.0

[CHANGES.txt at rel/1.0.0 (6c98bff)](https://github.com/apache/hbase/blob/rel/1.0.0/CHANGES.txt)
