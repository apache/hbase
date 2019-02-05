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
-->

## Release 2.1.3 - Unreleased (as of 2019-02-04)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | Throw DNRIOE when connection or rpc client is closed |  Major | asyncclient, Client |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21833](https://issues.apache.org/jira/browse/HBASE-21833) | Use NettyAsyncFSWALConfigHelper.setEventLoopConfig to prevent creating too many netty event loop when executing TestHRegion |  Minor | test |
| [HBASE-21634](https://issues.apache.org/jira/browse/HBASE-21634) | Print error message when user uses unacceptable values for LIMIT while setting quotas. |  Minor | . |
| [HBASE-21712](https://issues.apache.org/jira/browse/HBASE-21712) | Make submit-patch.py python3 compatible |  Minor | tooling |
| [HBASE-21590](https://issues.apache.org/jira/browse/HBASE-21590) | Optimize trySkipToNextColumn in StoreScanner a bit |  Critical | Performance, Scanners |
| [HBASE-21297](https://issues.apache.org/jira/browse/HBASE-21297) | ModifyTableProcedure can throw TNDE instead of IOE in case of REGION\_REPLICATION change |  Minor | . |
| [HBASE-21694](https://issues.apache.org/jira/browse/HBASE-21694) | Add append\_peer\_exclude\_tableCFs and remove\_peer\_exclude\_tableCFs shell commands |  Major | . |
| [HBASE-21645](https://issues.apache.org/jira/browse/HBASE-21645) | Perform sanity check and disallow table creation/modification with region replication \< 1 |  Minor | . |
| [HBASE-21662](https://issues.apache.org/jira/browse/HBASE-21662) | Add append\_peer\_exclude\_namespaces and remove\_peer\_exclude\_namespaces shell commands |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21795](https://issues.apache.org/jira/browse/HBASE-21795) | Client application may get stuck (time bound) if a table modify op is called immediately after split op |  Critical | amv2 |
| [HBASE-21840](https://issues.apache.org/jira/browse/HBASE-21840) | TestHRegionWithInMemoryFlush fails with NPE |  Blocker | test |
| [HBASE-21644](https://issues.apache.org/jira/browse/HBASE-21644) | Modify table procedure runs infinitely for a table having region replication \> 1 |  Critical | Admin |
| [HBASE-21775](https://issues.apache.org/jira/browse/HBASE-21775) | The BufferedMutator doesn't ever refresh region location cache |  Major | Client |
| [HBASE-21699](https://issues.apache.org/jira/browse/HBASE-21699) | Create table failed when using  SPLITS\_FILE =\> 'splits.txt' |  Blocker | Client, shell |
| [HBASE-21535](https://issues.apache.org/jira/browse/HBASE-21535) | Zombie Master detector is not working |  Critical | master |
| [HBASE-21754](https://issues.apache.org/jira/browse/HBASE-21754) | ReportRegionStateTransitionRequest should be executed in priority executor |  Major | . |
| [HBASE-21475](https://issues.apache.org/jira/browse/HBASE-21475) | Put mutation (having TTL set) added via co-processor is retrieved even after TTL expires |  Major | Coprocessors |
| [HBASE-21749](https://issues.apache.org/jira/browse/HBASE-21749) | RS UI may throw NPE and make rs-status page inaccessible with multiwal and replication |  Major | Replication, UI |
| [HBASE-21746](https://issues.apache.org/jira/browse/HBASE-21746) | Fix two concern cases in RegionMover |  Major | . |
| [HBASE-21732](https://issues.apache.org/jira/browse/HBASE-21732) | Should call toUpperCase before using Enum.valueOf in some methods for ColumnFamilyDescriptor |  Critical | Client |
| [HBASE-21704](https://issues.apache.org/jira/browse/HBASE-21704) | The implementation of DistributedHBaseCluster.getServerHoldingRegion is incorrect |  Major | . |
| [HBASE-20917](https://issues.apache.org/jira/browse/HBASE-20917) | MetaTableMetrics#stop references uninitialized requestsMap for non-meta region |  Major | meta, metrics |
| [HBASE-21639](https://issues.apache.org/jira/browse/HBASE-21639) | maxHeapUsage value not read properly from config during EntryBuffers initialization |  Minor | . |
| [HBASE-21225](https://issues.apache.org/jira/browse/HBASE-21225) | Having RPC & Space quota on a table/Namespace doesn't allow space quota to be removed using 'NONE' |  Major | . |
| [HBASE-20220](https://issues.apache.org/jira/browse/HBASE-20220) | [RSGroup] Check if table exists in the cluster before moving it to the specified regionserver group |  Major | rsgroup |
| [HBASE-21691](https://issues.apache.org/jira/browse/HBASE-21691) | Fix flaky test TestRecoveredEdits |  Major | . |
| [HBASE-21683](https://issues.apache.org/jira/browse/HBASE-21683) | Reset readsEnabled flag after successfully flushing the primary region |  Critical | read replicas |
| [HBASE-21630](https://issues.apache.org/jira/browse/HBASE-21630) | [shell] Define ENDKEY == STOPROW (we have ENDROW) |  Trivial | shell |
| [HBASE-21547](https://issues.apache.org/jira/browse/HBASE-21547) | Precommit uses master flaky list for other branches |  Major | test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21819](https://issues.apache.org/jira/browse/HBASE-21819) | Generate CHANGES.md and RELEASENOTES.md for 2.1.3 |  Major | documentation, release |
| [HBASE-21834](https://issues.apache.org/jira/browse/HBASE-21834) | Set version as 2.1.3 in branch-2.1 in prep for first RC |  Major | build, release |
| [HBASE-21829](https://issues.apache.org/jira/browse/HBASE-21829) | Use FutureUtils.addListener instead of calling whenComplete directly |  Major | asyncclient, Client |
| [HBASE-21832](https://issues.apache.org/jira/browse/HBASE-21832) | Backport parent "HBASE-21595 Print thread's information and stack traces when RS is aborting forcibly" to branch-2.0/2.1 |  Minor | regionserver |
| [HBASE-21828](https://issues.apache.org/jira/browse/HBASE-21828) | Make sure we do not return CompletionException when locating region |  Major | asyncclient, Client |
| [HBASE-21764](https://issues.apache.org/jira/browse/HBASE-21764) | Size of in-memory compaction thread pool should be configurable |  Major | in-memory-compaction |
| [HBASE-21402](https://issues.apache.org/jira/browse/HBASE-21402) | Backport parent "HBASE-21325 Force to terminate regionserver when abort hang in somewhere" |  Major | . |
| [HBASE-21734](https://issues.apache.org/jira/browse/HBASE-21734) | Some optimization in FilterListWithOR |  Major | . |
| [HBASE-21738](https://issues.apache.org/jira/browse/HBASE-21738) | Remove all the CSLM#size operation in our memstore because it's an quite time consuming. |  Critical | Performance |
| [HBASE-19695](https://issues.apache.org/jira/browse/HBASE-19695) | Handle disabled table for async client |  Major | asyncclient, Client |
| [HBASE-21727](https://issues.apache.org/jira/browse/HBASE-21727) | Simplify documentation around client timeout |  Minor | . |
| [HBASE-21711](https://issues.apache.org/jira/browse/HBASE-21711) | Remove references to git.apache.org/hbase.git |  Critical | . |
| [HBASE-19722](https://issues.apache.org/jira/browse/HBASE-19722) | Meta query statistics metrics source |  Critical | Coprocessors, meta, metrics, Operability |
| [HBASE-21705](https://issues.apache.org/jira/browse/HBASE-21705) | Should treat meta table specially for some methods in AsyncAdmin |  Major | Admin, asyncclient, Client |
| [HBASE-21663](https://issues.apache.org/jira/browse/HBASE-21663) | Add replica scan support |  Major | asyncclient, Client, read replicas |
| [HBASE-21580](https://issues.apache.org/jira/browse/HBASE-21580) | Support getting Hbck instance from AsyncConnection |  Major | asyncclient, Client, hbck2 |
| [HBASE-21698](https://issues.apache.org/jira/browse/HBASE-21698) | Move version in branch-2.1 from 2.1.2 to 2.1.3-SNAPSHOT |  Major | release |
| [HBASE-21682](https://issues.apache.org/jira/browse/HBASE-21682) | Support getting from specific replica |  Major | read replicas |
| [HBASE-17356](https://issues.apache.org/jira/browse/HBASE-17356) | Add replica get support |  Major | Client |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21612](https://issues.apache.org/jira/browse/HBASE-21612) | Add developer debug options in  HBase Config for REST server |  Minor | Operability, REST, scripts |
| [HBASE-21791](https://issues.apache.org/jira/browse/HBASE-21791) | Upgrade thrift dependency to 0.12.0 |  Blocker | Thrift |
| [HBASE-21715](https://issues.apache.org/jira/browse/HBASE-21715) | Do not throw UnsupportedOperationException in ProcedureFuture.get |  Major | Client |
| [HBASE-21731](https://issues.apache.org/jira/browse/HBASE-21731) | Do not need to use ClusterConnection in IntegrationTestBigLinkedListWithVisibility |  Major | . |
| [HBASE-21685](https://issues.apache.org/jira/browse/HBASE-21685) | Change repository urls to Gitbox |  Critical | . |
| [HBASE-21282](https://issues.apache.org/jira/browse/HBASE-21282) | Upgrade to latest jetty 9.2 and 9.3 versions |  Major | dependencies |



## Release 2.1.2 - Unreleased (as of 2018-12-29)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21640](https://issues.apache.org/jira/browse/HBASE-21640) | Remove the TODO when increment zero |  Major | . |
| [HBASE-21631](https://issues.apache.org/jira/browse/HBASE-21631) | list\_quotas should print human readable values for LIMIT |  Minor | shell |
| [HBASE-21635](https://issues.apache.org/jira/browse/HBASE-21635) | Use maven enforcer to ban imports from illegal packages |  Major | build |
| [HBASE-21520](https://issues.apache.org/jira/browse/HBASE-21520) | TestMultiColumnScanner cost long time when using ROWCOL bloom type |  Major | test |
| [HBASE-21590](https://issues.apache.org/jira/browse/HBASE-21590) | Optimize trySkipToNextColumn in StoreScanner a bit |  Critical | Performance, Scanners |
| [HBASE-21554](https://issues.apache.org/jira/browse/HBASE-21554) | Show replication endpoint classname for replication peer on master web UI |  Minor | UI |
| [HBASE-21549](https://issues.apache.org/jira/browse/HBASE-21549) | Add shell command for serial replication peer |  Major | . |
| [HBASE-21567](https://issues.apache.org/jira/browse/HBASE-21567) | Allow overriding configs starting up the shell |  Major | shell |
| [HBASE-21413](https://issues.apache.org/jira/browse/HBASE-21413) | Empty meta log doesn't get split when restart whole cluster |  Major | . |
| [HBASE-21524](https://issues.apache.org/jira/browse/HBASE-21524) | Unnecessary DEBUG log in ConnectionImplementation#isTableEnabled |  Major | Client |
| [HBASE-21511](https://issues.apache.org/jira/browse/HBASE-21511) | Remove in progress snapshot check in SnapshotFileCache#getUnreferencedFiles |  Minor | . |
| [HBASE-21480](https://issues.apache.org/jira/browse/HBASE-21480) | Taking snapshot when RS crashes prevent we bring the regions online |  Major | snapshots |
| [HBASE-21485](https://issues.apache.org/jira/browse/HBASE-21485) | Add more debug logs for remote procedure execution |  Major | proc-v2 |
| [HBASE-21388](https://issues.apache.org/jira/browse/HBASE-21388) | No need to instantiate MemStoreLAB for master which not carry table |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21646](https://issues.apache.org/jira/browse/HBASE-21646) | Flakey TestTableSnapshotInputFormat; DisableTable not completing... |  Major | test |
| [HBASE-21545](https://issues.apache.org/jira/browse/HBASE-21545) | NEW\_VERSION\_BEHAVIOR breaks Get/Scan with specified columns |  Major | API |
| [HBASE-21629](https://issues.apache.org/jira/browse/HBASE-21629) | draining\_servers.rb is broken |  Major | scripts |
| [HBASE-21621](https://issues.apache.org/jira/browse/HBASE-21621) | Reversed scan does not return expected  number of rows |  Critical | scan |
| [HBASE-21620](https://issues.apache.org/jira/browse/HBASE-21620) | Problem in scan query when using more than one column prefix filter in some cases. |  Major | scan |
| [HBASE-21618](https://issues.apache.org/jira/browse/HBASE-21618) | Scan with the same startRow(inclusive=true) and stopRow(inclusive=false) returns one result |  Critical | Client |
| [HBASE-21610](https://issues.apache.org/jira/browse/HBASE-21610) | numOpenConnections metric is set to -1 when zero server channel exist |  Minor | metrics |
| [HBASE-21498](https://issues.apache.org/jira/browse/HBASE-21498) | Master OOM when SplitTableRegionProcedure new CacheConfig and instantiate a new BlockCache |  Major | . |
| [HBASE-21592](https://issues.apache.org/jira/browse/HBASE-21592) | quota.addGetResult(r)  throw  NPE |  Major | . |
| [HBASE-21589](https://issues.apache.org/jira/browse/HBASE-21589) | TestCleanupMetaWAL fails |  Blocker | test, wal |
| [HBASE-21582](https://issues.apache.org/jira/browse/HBASE-21582) | If call HBaseAdmin#snapshotAsync but forget call isSnapshotFinished, then SnapshotHFileCleaner will skip to run every time |  Major | . |
| [HBASE-21568](https://issues.apache.org/jira/browse/HBASE-21568) | Disable use of BlockCache for LoadIncrementalHFiles |  Major | Client |
| [HBASE-21453](https://issues.apache.org/jira/browse/HBASE-21453) | Convert ReadOnlyZKClient to DEBUG instead of INFO |  Major | logging, Zookeeper |
| [HBASE-21559](https://issues.apache.org/jira/browse/HBASE-21559) | The RestoreSnapshotFromClientTestBase related UT are flaky |  Major | . |
| [HBASE-21551](https://issues.apache.org/jira/browse/HBASE-21551) | Memory leak when use scan with STREAM at server side |  Blocker | regionserver |
| [HBASE-21479](https://issues.apache.org/jira/browse/HBASE-21479) | Individual tests in TestHRegionReplayEvents class are failing |  Major | . |
| [HBASE-21518](https://issues.apache.org/jira/browse/HBASE-21518) | TestMasterFailoverWithProcedures is flaky |  Major | . |
| [HBASE-21504](https://issues.apache.org/jira/browse/HBASE-21504) | If enable FIFOCompactionPolicy, a compaction may write a "empty" hfile whose maxTimeStamp is long max. This kind of hfile will never be archived. |  Critical | Compaction |
| [HBASE-21300](https://issues.apache.org/jira/browse/HBASE-21300) | Fix the wrong reference file path when restoring snapshots for tables with MOB columns |  Major | . |
| [HBASE-21492](https://issues.apache.org/jira/browse/HBASE-21492) | CellCodec Written To WAL Before It's Verified |  Critical | wal |
| [HBASE-21507](https://issues.apache.org/jira/browse/HBASE-21507) | Compaction failed when execute AbstractMultiFileWriter.beforeShipped() method |  Major | regionserver |
| [HBASE-21387](https://issues.apache.org/jira/browse/HBASE-21387) | Race condition surrounding in progress snapshot handling in snapshot cache leads to loss of snapshot files |  Major | snapshots |
| [HBASE-21503](https://issues.apache.org/jira/browse/HBASE-21503) | Replication normal source can get stuck due potential race conditions between source wal reader and wal provider initialization threads. |  Blocker | Replication |
| [HBASE-21440](https://issues.apache.org/jira/browse/HBASE-21440) | Assign procedure on the crashed server is not properly interrupted |  Major | . |
| [HBASE-21468](https://issues.apache.org/jira/browse/HBASE-21468) | separate workers for meta table is not working |  Major | . |
| [HBASE-21445](https://issues.apache.org/jira/browse/HBASE-21445) | CopyTable by bulkload will write hfile into yarn's HDFS |  Major | mapreduce |
| [HBASE-21437](https://issues.apache.org/jira/browse/HBASE-21437) | Bypassed procedure throw IllegalArgumentException when its state is WAITING\_TIMEOUT |  Major | . |
| [HBASE-21439](https://issues.apache.org/jira/browse/HBASE-21439) | StochasticLoadBalancer RegionLoads aren’t being used in RegionLoad cost functions |  Major | Balancer |
| [HBASE-20604](https://issues.apache.org/jira/browse/HBASE-20604) | ProtobufLogReader#readNext can incorrectly loop to the same position in the stream until the the WAL is rolled |  Critical | Replication, wal |
| [HBASE-21247](https://issues.apache.org/jira/browse/HBASE-21247) | Custom Meta WAL Provider doesn't default to custom WAL Provider whose configuration value is outside the enums in Providers |  Major | wal |
| [HBASE-21438](https://issues.apache.org/jira/browse/HBASE-21438) | TestAdmin2#testGetProcedures fails due to FailedProcedure inaccessible |  Major | . |
| [HBASE-21425](https://issues.apache.org/jira/browse/HBASE-21425) | 2.1.1 fails to start over 1.x data; namespace not assigned |  Critical | amv2 |
| [HBASE-21407](https://issues.apache.org/jira/browse/HBASE-21407) | Resolve NPE in backup Master UI |  Minor | UI |
| [HBASE-21424](https://issues.apache.org/jira/browse/HBASE-21424) | Change flakies and nightlies so scheduled less often |  Major | build |
| [HBASE-21417](https://issues.apache.org/jira/browse/HBASE-21417) | Pre commit build is broken due to surefire plugin crashes |  Critical | build |
| [HBASE-21371](https://issues.apache.org/jira/browse/HBASE-21371) | Hbase unable to compile against Hadoop trunk (3.3.0-SNAPSHOT) due to license error |  Major | . |
| [HBASE-21356](https://issues.apache.org/jira/browse/HBASE-21356) | bulkLoadHFile API should ensure that rs has the source hfile's write permission |  Major | . |
| [HBASE-21055](https://issues.apache.org/jira/browse/HBASE-21055) | NullPointerException when balanceOverall() but server balance info is null |  Major | Balancer |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20136](https://issues.apache.org/jira/browse/HBASE-20136) | TestKeyValue misses ClassRule and Category annotations |  Minor | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21401](https://issues.apache.org/jira/browse/HBASE-21401) | Sanity check when constructing the KeyValue |  Critical | regionserver |
| [HBASE-21570](https://issues.apache.org/jira/browse/HBASE-21570) | Add write buffer periodic flush support for AsyncBufferedMutator |  Major | asyncclient, Client |
| [HBASE-21566](https://issues.apache.org/jira/browse/HBASE-21566) | Release notes and changes for 2.0.4RC0 and 2.1.2RC0 |  Major | release |
| [HBASE-21490](https://issues.apache.org/jira/browse/HBASE-21490) | WALProcedure may remove proc wal files still with active procedures |  Major | proc-v2 |
| [HBASE-21377](https://issues.apache.org/jira/browse/HBASE-21377) | Add debug log for procedure stack id related operations |  Major | proc-v2 |
| [HBASE-21473](https://issues.apache.org/jira/browse/HBASE-21473) | RowIndexSeekerV1 may return cell with extra two \\x00\\x00 bytes which has no tags |  Major | . |
| [HBASE-21423](https://issues.apache.org/jira/browse/HBASE-21423) | Procedures for meta table/region should be able to execute in separate workers |  Major | . |
| [HBASE-21376](https://issues.apache.org/jira/browse/HBASE-21376) | Add some verbose log to MasterProcedureScheduler |  Major | logging, proc-v2 |
| [HBASE-21442](https://issues.apache.org/jira/browse/HBASE-21442) | Update branch-2.1 for next development cycle |  Major | build |
| [HBASE-21421](https://issues.apache.org/jira/browse/HBASE-21421) | Do not kill RS if reportOnlineRegions fails |  Major | . |
| [HBASE-21314](https://issues.apache.org/jira/browse/HBASE-21314) | The implementation of BitSetNode is not efficient |  Major | proc-v2 |
| [HBASE-21395](https://issues.apache.org/jira/browse/HBASE-21395) | Abort split/merge procedure if there is a table procedure of the same table going on |  Major | . |
| [HBASE-21351](https://issues.apache.org/jira/browse/HBASE-21351) | The force update thread may have race with PE worker when the procedure is rolling back |  Critical | proc-v2 |
| [HBASE-21237](https://issues.apache.org/jira/browse/HBASE-21237) | Use CompatRemoteProcedureResolver to dispatch open/close region requests to RS |  Blocker | . |
| [HBASE-21322](https://issues.apache.org/jira/browse/HBASE-21322) | Add a scheduleServerCrashProcedure() API to HbckService |  Major | hbck2 |
| [HBASE-21375](https://issues.apache.org/jira/browse/HBASE-21375) | Revisit the lock and queue implementation in MasterProcedureScheduler |  Major | proc-v2 |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-18735](https://issues.apache.org/jira/browse/HBASE-18735) | Provide a fast mechanism for shutting down mini cluster |  Major | . |
| [HBASE-21517](https://issues.apache.org/jira/browse/HBASE-21517) | Move the getTableRegionForRow method from HMaster to TestMaster |  Major | test |



## Release 2.1.1 - Released 2018-10-31

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21158](https://issues.apache.org/jira/browse/HBASE-21158) | Empty qualifier cell should not be returned if it does not match QualifierFilter |  Critical | Filters |
| [HBASE-21223](https://issues.apache.org/jira/browse/HBASE-21223) | [amv2] Remove abort\_procedure from shell |  Critical | amv2, hbck2, shell |
| [HBASE-20884](https://issues.apache.org/jira/browse/HBASE-20884) | Replace usage of our Base64 implementation with java.util.Base64 |  Major | . |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20649](https://issues.apache.org/jira/browse/HBASE-20649) | Validate HFiles do not have PREFIX\_TREE DataBlockEncoding |  Minor | Operability, tooling |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21385](https://issues.apache.org/jira/browse/HBASE-21385) | HTable.delete request use rpc call directly instead of AsyncProcess |  Major | . |
| [HBASE-21145](https://issues.apache.org/jira/browse/HBASE-21145) | Backport "HBASE-21126 Add ability for HBase Canary to ignore a configurable number of ZooKeeper down nodes" to branch-2.1 |  Minor | canary, Zookeeper |
| [HBASE-21263](https://issues.apache.org/jira/browse/HBASE-21263) | Mention compression algorithm along with other storefile details |  Minor | . |
| [HBASE-21290](https://issues.apache.org/jira/browse/HBASE-21290) | No need to instantiate BlockCache for master which not carry table |  Major | . |
| [HBASE-21251](https://issues.apache.org/jira/browse/HBASE-21251) | Refactor RegionMover |  Major | Operability |
| [HBASE-21303](https://issues.apache.org/jira/browse/HBASE-21303) | [shell] clear\_deadservers with no args fails |  Major | . |
| [HBASE-21299](https://issues.apache.org/jira/browse/HBASE-21299) | List counts of actual region states in master UI tables section |  Major | UI |
| [HBASE-21289](https://issues.apache.org/jira/browse/HBASE-21289) | Remove the log "'hbase.regionserver.maxlogs' was deprecated." in AbstractFSWAL |  Minor | . |
| [HBASE-21185](https://issues.apache.org/jira/browse/HBASE-21185) | WALPrettyPrinter: Additional useful info to be printed by wal printer tool, for debugability purposes |  Minor | Operability |
| [HBASE-21103](https://issues.apache.org/jira/browse/HBASE-21103) | nightly test cache of yetus install needs to be more thorough in verification |  Major | test |
| [HBASE-20857](https://issues.apache.org/jira/browse/HBASE-20857) | JMX - add Balancer status = enabled / disabled |  Major | API, master, metrics, REST, tooling, Usability |
| [HBASE-21164](https://issues.apache.org/jira/browse/HBASE-21164) | reportForDuty to spew less log if master is initializing |  Minor | regionserver |
| [HBASE-20307](https://issues.apache.org/jira/browse/HBASE-20307) | LoadTestTool prints too much zookeeper logging |  Minor | tooling |
| [HBASE-21155](https://issues.apache.org/jira/browse/HBASE-21155) | Save on a few log strings and some churn in wal splitter by skipping out early if no logs in dir |  Trivial | . |
| [HBASE-21157](https://issues.apache.org/jira/browse/HBASE-21157) | Split TableInputFormatScan to individual tests |  Minor | test |
| [HBASE-21153](https://issues.apache.org/jira/browse/HBASE-21153) | Shaded client jars should always build in relevant phase to avoid confusion |  Major | build |
| [HBASE-20749](https://issues.apache.org/jira/browse/HBASE-20749) | Upgrade our use of checkstyle to 8.6+ |  Minor | build, community |
| [HBASE-20387](https://issues.apache.org/jira/browse/HBASE-20387) | flaky infrastructure should work for all branches |  Critical | test |
| [HBASE-20469](https://issues.apache.org/jira/browse/HBASE-20469) | Directory used for sidelining old recovered edits files should be made configurable |  Minor | . |
| [HBASE-20979](https://issues.apache.org/jira/browse/HBASE-20979) | Flaky test reporting should specify what JSON it needs and handle HTTP errors |  Minor | test |
| [HBASE-20985](https://issues.apache.org/jira/browse/HBASE-20985) | add two attributes when we do normalization |  Major | . |
| [HBASE-20965](https://issues.apache.org/jira/browse/HBASE-20965) | Separate region server report requests to new handlers |  Major | Performance |
| [HBASE-20986](https://issues.apache.org/jira/browse/HBASE-20986) | Separate the config of block size when we do log splitting and write Hlog |  Major | . |
| [HBASE-20856](https://issues.apache.org/jira/browse/HBASE-20856) | PITA having to set WAL provider in two places |  Minor | Operability, wal |
| [HBASE-20935](https://issues.apache.org/jira/browse/HBASE-20935) | HStore.removeCompactedFiles should log in case it is unable to delete a file |  Minor | . |
| [HBASE-20873](https://issues.apache.org/jira/browse/HBASE-20873) | Update doc for Endpoint-based Export |  Minor | documentation |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21380](https://issues.apache.org/jira/browse/HBASE-21380) | TestRSGroups failing |  Major | . |
| [HBASE-21391](https://issues.apache.org/jira/browse/HBASE-21391) | RefreshPeerProcedure should also wait master initialized before executing |  Major | Replication |
| [HBASE-21342](https://issues.apache.org/jira/browse/HBASE-21342) | FileSystem in use may get closed by other bulk load call  in secure bulkLoad |  Major | . |
| [HBASE-21349](https://issues.apache.org/jira/browse/HBASE-21349) | Cluster is going down but CatalogJanitor and Normalizer try to run and fail noisely |  Minor | . |
| [HBASE-21355](https://issues.apache.org/jira/browse/HBASE-21355) | HStore's storeSize is calculated repeatedly which causing the confusing region split |  Blocker | regionserver |
| [HBASE-21334](https://issues.apache.org/jira/browse/HBASE-21334) | TestMergeTableRegionsProcedure is flakey |  Major | amv2, proc-v2, test |
| [HBASE-21178](https://issues.apache.org/jira/browse/HBASE-21178) | [BC break] : Get and Scan operation with a custom converter\_class not working |  Critical | shell |
| [HBASE-21242](https://issues.apache.org/jira/browse/HBASE-21242) | [amv2] Miscellaneous minor log and assign procedure create improvements |  Minor | amv2, Operability |
| [HBASE-21348](https://issues.apache.org/jira/browse/HBASE-21348) | Fix failing TestRegionBypass, broke by HBASE-21291 |  Major | hbck2 |
| [HBASE-21345](https://issues.apache.org/jira/browse/HBASE-21345) | [hbck2] Allow version check to proceed even though master is 'initializing'. |  Major | hbck2 |
| [HBASE-21200](https://issues.apache.org/jira/browse/HBASE-21200) | Memstore flush doesn't finish because of seekToPreviousRow() in memstore scanner. |  Critical | Scanners |
| [HBASE-21292](https://issues.apache.org/jira/browse/HBASE-21292) | IdLock.getLockEntry() may hang if interrupted |  Major | . |
| [HBASE-21335](https://issues.apache.org/jira/browse/HBASE-21335) | Change the default wait time of HBCK2 tool |  Critical | . |
| [HBASE-21291](https://issues.apache.org/jira/browse/HBASE-21291) | Add a test for bypassing stuck state-machine procedures |  Major | . |
| [HBASE-21327](https://issues.apache.org/jira/browse/HBASE-21327) | Fix minor logging issue where we don't report servername if no associated SCP |  Trivial | amv2 |
| [HBASE-21320](https://issues.apache.org/jira/browse/HBASE-21320) | [canary] Cleanup of usage and add commentary |  Major | canary |
| [HBASE-21266](https://issues.apache.org/jira/browse/HBASE-21266) | Not running balancer because processing dead regionservers, but empty dead rs list |  Major | . |
| [HBASE-21260](https://issues.apache.org/jira/browse/HBASE-21260) | The whole balancer plans might be aborted if there are more than one plans to move a same region |  Major | Balancer, master |
| [HBASE-21271](https://issues.apache.org/jira/browse/HBASE-21271) | [amv2] Don't throw UnsupportedOperationException when rollback called on Assign/Unassign; spiral of death |  Major | amv2 |
| [HBASE-21259](https://issues.apache.org/jira/browse/HBASE-21259) | [amv2] Revived deadservers; recreated serverstatenode |  Critical | amv2 |
| [HBASE-21280](https://issues.apache.org/jira/browse/HBASE-21280) | Add anchors for each heading in UI |  Trivial | UI, Usability |
| [HBASE-20764](https://issues.apache.org/jira/browse/HBASE-20764) | build broken when latest commit is gpg signed |  Critical | build |
| [HBASE-21213](https://issues.apache.org/jira/browse/HBASE-21213) | [hbck2] bypass leaves behind state in RegionStates when assign/unassign |  Major | amv2, hbck2 |
| [HBASE-18549](https://issues.apache.org/jira/browse/HBASE-18549) | Unclaimed replication queues can go undetected |  Critical | Replication |
| [HBASE-21248](https://issues.apache.org/jira/browse/HBASE-21248) | Implement exponential backoff when retrying for ModifyPeerProcedure |  Major | proc-v2, Replication |
| [HBASE-21196](https://issues.apache.org/jira/browse/HBASE-21196) | HTableMultiplexer clears the meta cache after every put operation |  Critical | Performance |
| [HBASE-19418](https://issues.apache.org/jira/browse/HBASE-19418) | RANGE\_OF\_DELAY in PeriodicMemstoreFlusher should be configurable. |  Minor | . |
| [HBASE-18451](https://issues.apache.org/jira/browse/HBASE-18451) | PeriodicMemstoreFlusher should inspect the queue before adding a delayed flush request |  Major | regionserver |
| [HBASE-21228](https://issues.apache.org/jira/browse/HBASE-21228) | Memory leak since AbstractFSWAL caches Thread object and never clean later |  Critical | wal |
| [HBASE-21232](https://issues.apache.org/jira/browse/HBASE-21232) | Show table state in Tables view on Master home page |  Major | Operability, UI |
| [HBASE-21212](https://issues.apache.org/jira/browse/HBASE-21212) | Wrong flush time when update flush metric |  Minor | . |
| [HBASE-21208](https://issues.apache.org/jira/browse/HBASE-21208) | Bytes#toShort doesn't work without unsafe |  Critical | . |
| [HBASE-20704](https://issues.apache.org/jira/browse/HBASE-20704) | Sometimes some compacted storefiles are not archived on region close |  Critical | Compaction |
| [HBASE-21203](https://issues.apache.org/jira/browse/HBASE-21203) | TestZKMainServer#testCommandLineWorks won't pass with default 4lw whitelist |  Minor | test, Zookeeper |
| [HBASE-21206](https://issues.apache.org/jira/browse/HBASE-21206) | Scan with batch size may return incomplete cells |  Critical | scan |
| [HBASE-21182](https://issues.apache.org/jira/browse/HBASE-21182) | Failed to execute start-hbase.sh |  Major | . |
| [HBASE-21179](https://issues.apache.org/jira/browse/HBASE-21179) | Fix the number of actions in responseTooSlow log |  Major | logging, rpc |
| [HBASE-21174](https://issues.apache.org/jira/browse/HBASE-21174) | [REST] Failed to parse empty qualifier in TableResource#getScanResource |  Major | REST |
| [HBASE-21181](https://issues.apache.org/jira/browse/HBASE-21181) | Use the same filesystem for wal archive directory and wal directory |  Major | . |
| [HBASE-21144](https://issues.apache.org/jira/browse/HBASE-21144) | AssignmentManager.waitForAssignment is not stable |  Major | amv2, test |
| [HBASE-21143](https://issues.apache.org/jira/browse/HBASE-21143) | Update findbugs-maven-plugin to 3.0.4 |  Major | pom |
| [HBASE-21171](https://issues.apache.org/jira/browse/HBASE-21171) | [amv2] Tool to parse a directory of MasterProcWALs standalone |  Major | amv2, test |
| [HBASE-21001](https://issues.apache.org/jira/browse/HBASE-21001) | ReplicationObserver fails to load in HBase 2.0.0 |  Major | . |
| [HBASE-20741](https://issues.apache.org/jira/browse/HBASE-20741) | Split of a region with replicas creates all daughter regions and its replica in same server |  Major | read replicas |
| [HBASE-21127](https://issues.apache.org/jira/browse/HBASE-21127) | TableRecordReader need to handle cursor result too |  Major | . |
| [HBASE-20892](https://issues.apache.org/jira/browse/HBASE-20892) | [UI] Start / End keys are empty on table.jsp |  Major | . |
| [HBASE-21132](https://issues.apache.org/jira/browse/HBASE-21132) | return wrong result in rest multiget |  Major | . |
| [HBASE-20940](https://issues.apache.org/jira/browse/HBASE-20940) | HStore.cansplit should not allow split to happen if it has references |  Major | . |
| [HBASE-20968](https://issues.apache.org/jira/browse/HBASE-20968) | list\_procedures\_test fails due to no matching regex |  Major | shell, test |
| [HBASE-21030](https://issues.apache.org/jira/browse/HBASE-21030) | Correct javadoc for append operation |  Minor | documentation |
| [HBASE-21088](https://issues.apache.org/jira/browse/HBASE-21088) | HStoreFile should be closed in HStore#hasReferences |  Major | . |
| [HBASE-21120](https://issues.apache.org/jira/browse/HBASE-21120) | MoveRegionProcedure makes no progress; goes to STUCK |  Major | amv2 |
| [HBASE-20772](https://issues.apache.org/jira/browse/HBASE-20772) | Controlled shutdown fills Master log with the disturbing message "No matching procedure found for rit=OPEN, location=ZZZZ, table=YYYYY, region=XXXX transition to CLOSED |  Major | logging |
| [HBASE-20978](https://issues.apache.org/jira/browse/HBASE-20978) | [amv2] Worker terminating UNNATURALLY during MoveRegionProcedure |  Critical | amv2 |
| [HBASE-21078](https://issues.apache.org/jira/browse/HBASE-21078) | [amv2] CODE-BUG NPE in RTP doing Unassign |  Major | amv2 |
| [HBASE-21113](https://issues.apache.org/jira/browse/HBASE-21113) | Apply the branch-2 version of HBASE-21095, The timeout retry logic for several procedures are broken after master restarts |  Major | amv2 |
| [HBASE-21101](https://issues.apache.org/jira/browse/HBASE-21101) | Remove the waitUntilAllRegionsAssigned call after split in TestTruncateTableProcedure |  Major | test |
| [HBASE-20614](https://issues.apache.org/jira/browse/HBASE-20614) | REST scan API with incorrect filter text file throws HTTP 503 Service Unavailable error |  Minor | REST |
| [HBASE-21041](https://issues.apache.org/jira/browse/HBASE-21041) | Memstore's heap size will be decreased to minus zero after flush |  Major | . |
| [HBASE-21031](https://issues.apache.org/jira/browse/HBASE-21031) | Memory leak if replay edits failed during region opening |  Major | . |
| [HBASE-21032](https://issues.apache.org/jira/browse/HBASE-21032) | ScanResponses contain only one cell each |  Major | Performance, Scanners |
| [HBASE-20705](https://issues.apache.org/jira/browse/HBASE-20705) | Having RPC Quota on a table prevents Space quota to be recreated/removed |  Major | . |
| [HBASE-21058](https://issues.apache.org/jira/browse/HBASE-21058) | Nightly tests for branches 1 fail to build ref guide |  Major | documentation |
| [HBASE-21074](https://issues.apache.org/jira/browse/HBASE-21074) | JDK7 branches need to pass "-Dhttps.protocols=TLSv1.2" to maven when building |  Major | build, community, test |
| [HBASE-21062](https://issues.apache.org/jira/browse/HBASE-21062) | WALFactory has misleading notion of "default" |  Major | wal |
| [HBASE-21047](https://issues.apache.org/jira/browse/HBASE-21047) | Object creation of StoreFileScanner thru constructor and close may leave refCount to -1 |  Major | . |
| [HBASE-21005](https://issues.apache.org/jira/browse/HBASE-21005) | Maven site configuration causes downstream projects to get a directory named ${project.basedir} |  Minor | build |
| [HBASE-21029](https://issues.apache.org/jira/browse/HBASE-21029) | Miscount of memstore's heap/offheap size if same cell was put |  Major | . |
| [HBASE-20981](https://issues.apache.org/jira/browse/HBASE-20981) | Rollback stateCount accounting thrown-off when exception out of rollbackState |  Major | amv2 |
| [HBASE-21018](https://issues.apache.org/jira/browse/HBASE-21018) | RS crashed because AsyncFS was unable to update HDFS data encryption key |  Critical | wal |
| [HBASE-21007](https://issues.apache.org/jira/browse/HBASE-21007) | Memory leak in HBase rest server |  Critical | REST |
| [HBASE-20538](https://issues.apache.org/jira/browse/HBASE-20538) | Upgrade our hadoop versions to 2.7.7 and 3.0.3 |  Critical | java, security |
| [HBASE-20565](https://issues.apache.org/jira/browse/HBASE-20565) | ColumnRangeFilter combined with ColumnPaginationFilter can produce incorrect result since 1.4 |  Major | Filters |
| [HBASE-20870](https://issues.apache.org/jira/browse/HBASE-20870) | Wrong HBase root dir in ITBLL's Search Tool |  Minor | integration tests |
| [HBASE-20869](https://issues.apache.org/jira/browse/HBASE-20869) | Endpoint-based Export use incorrect user to write to destination |  Major | Coprocessors |
| [HBASE-20865](https://issues.apache.org/jira/browse/HBASE-20865) | CreateTableProcedure is stuck in retry loop in CREATE\_TABLE\_WRITE\_FS\_LAYOUT state |  Major | amv2 |
| [HBASE-19572](https://issues.apache.org/jira/browse/HBASE-19572) | RegionMover should use the configured default port number and not the one from HConstants |  Major | . |
| [HBASE-20697](https://issues.apache.org/jira/browse/HBASE-20697) | Can't cache All region locations of the specify table by calling table.getRegionLocator().getAllRegionLocations() |  Major | meta |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21261](https://issues.apache.org/jira/browse/HBASE-21261) | Add log4j.properties for hbase-rsgroup tests |  Trivial | . |
| [HBASE-21097](https://issues.apache.org/jira/browse/HBASE-21097) | Flush pressure assertion may fail in testFlushThroughputTuning |  Major | regionserver |
| [HBASE-21161](https://issues.apache.org/jira/browse/HBASE-21161) | Enable the test added in HBASE-20741 that was removed accidentally |  Minor | . |
| [HBASE-21076](https://issues.apache.org/jira/browse/HBASE-21076) | TestTableResource fails with NPE |  Major | REST, test |
| [HBASE-20907](https://issues.apache.org/jira/browse/HBASE-20907) | Fix Intermittent failure on TestProcedurePriority |  Major | . |
| [HBASE-20838](https://issues.apache.org/jira/browse/HBASE-20838) | Include hbase-server in precommit test if CommonFSUtils is changed |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21398](https://issues.apache.org/jira/browse/HBASE-21398) | Copy down docs, amend to suite branch-2.1, and then commit |  Major | . |
| [HBASE-21397](https://issues.apache.org/jira/browse/HBASE-21397) | Set version to 2.1.1 on branch-2.1 in prep for first RC |  Major | . |
| [HBASE-20973](https://issues.apache.org/jira/browse/HBASE-20973) | ArrayIndexOutOfBoundsException when rolling back procedure |  Critical | amv2 |
| [HBASE-21384](https://issues.apache.org/jira/browse/HBASE-21384) | Procedure with holdlock=false should not be restored lock when restarts |  Blocker | . |
| [HBASE-21364](https://issues.apache.org/jira/browse/HBASE-21364) | Procedure holds the lock should put to front of the queue after restart |  Blocker | . |
| [HBASE-21215](https://issues.apache.org/jira/browse/HBASE-21215) | Figure how to invoke hbck2; make it easy to find |  Major | amv2, hbck2 |
| [HBASE-21372](https://issues.apache.org/jira/browse/HBASE-21372) | Set hbase.assignment.maximum.attempts to Long.MAX |  Major | amv2 |
| [HBASE-21363](https://issues.apache.org/jira/browse/HBASE-21363) | Rewrite the buildingHoldCleanupTracker method in WALProcedureStore |  Major | proc-v2 |
| [HBASE-21338](https://issues.apache.org/jira/browse/HBASE-21338) | [balancer] If balancer is an ill-fit for cluster size, it gives little indication |  Major | Balancer, Operability |
| [HBASE-21192](https://issues.apache.org/jira/browse/HBASE-21192) | Add HOW-TO repair damaged AMv2. |  Major | amv2 |
| [HBASE-21073](https://issues.apache.org/jira/browse/HBASE-21073) | "Maintenance mode" master |  Major | amv2, hbck2, master |
| [HBASE-21354](https://issues.apache.org/jira/browse/HBASE-21354) | Procedure may be deleted improperly during master restarts resulting in 'Corrupt' |  Major | . |
| [HBASE-21321](https://issues.apache.org/jira/browse/HBASE-21321) | Backport HBASE-21278 to branch-2.1 and branch-2.0 |  Critical | . |
| [HBASE-21336](https://issues.apache.org/jira/browse/HBASE-21336) | Simplify the implementation of WALProcedureMap |  Major | proc-v2 |
| [HBASE-21323](https://issues.apache.org/jira/browse/HBASE-21323) | Should not skip force updating for a sub procedure even if it has been finished |  Major | proc-v2 |
| [HBASE-21075](https://issues.apache.org/jira/browse/HBASE-21075) | Confirm that we can (rolling) upgrade from 2.0.x and 2.1.x to 2.2.x after HBASE-20881 |  Blocker | amv2, proc-v2 |
| [HBASE-21288](https://issues.apache.org/jira/browse/HBASE-21288) | HostingServer in UnassignProcedure is not accurate |  Major | amv2, Balancer |
| [HBASE-20716](https://issues.apache.org/jira/browse/HBASE-20716) | Unsafe access cleanup |  Critical | Performance |
| [HBASE-21310](https://issues.apache.org/jira/browse/HBASE-21310) | Split TestCloneSnapshotFromClient |  Major | test |
| [HBASE-21311](https://issues.apache.org/jira/browse/HBASE-21311) | Split TestRestoreSnapshotFromClient |  Major | test |
| [HBASE-21315](https://issues.apache.org/jira/browse/HBASE-21315) | The getActiveMinProcId and getActiveMaxProcId of BitSetNode are incorrect if there are no active procedure |  Major | . |
| [HBASE-21309](https://issues.apache.org/jira/browse/HBASE-21309) | Increase the waiting timeout for TestProcedurePriority |  Major | test |
| [HBASE-21254](https://issues.apache.org/jira/browse/HBASE-21254) | Need to find a way to limit the number of proc wal files |  Critical | proc-v2 |
| [HBASE-21250](https://issues.apache.org/jira/browse/HBASE-21250) | Refactor WALProcedureStore and add more comments for better understanding the implementation |  Major | proc-v2 |
| [HBASE-19275](https://issues.apache.org/jira/browse/HBASE-19275) | TestSnapshotFileCache never worked properly |  Major | . |
| [HBASE-21249](https://issues.apache.org/jira/browse/HBASE-21249) | Add jitter for ProcedureUtil.getBackoffTimeMs |  Major | proc-v2 |
| [HBASE-21233](https://issues.apache.org/jira/browse/HBASE-21233) | Allow the procedure implementation to skip persistence of the state after a execution |  Major | Performance, proc-v2 |
| [HBASE-21214](https://issues.apache.org/jira/browse/HBASE-21214) | [hbck2] setTableState just sets hbase:meta state, not in-memory state |  Major | amv2, hbck2 |
| [HBASE-21210](https://issues.apache.org/jira/browse/HBASE-21210) | Add bypassProcedure() API to HBCK2 |  Major | hbase-operator-tools, hbck2 |
| [HBASE-21023](https://issues.apache.org/jira/browse/HBASE-21023) | Add bypassProcedureToCompletion() API to HbckService |  Major | hbck2 |
| [HBASE-21156](https://issues.apache.org/jira/browse/HBASE-21156) | [hbck2] Queue an assign of hbase:meta and bulk assign/unassign |  Critical | hbck2 |
| [HBASE-21169](https://issues.apache.org/jira/browse/HBASE-21169) | Initiate hbck2 tool in hbase-operator-tools repo |  Major | hbck2 |
| [HBASE-21191](https://issues.apache.org/jira/browse/HBASE-21191) | Add a holding-pattern if no assign for meta or namespace (Can happen if masterprocwals have been cleared). |  Major | amv2 |
| [HBASE-21172](https://issues.apache.org/jira/browse/HBASE-21172) | Reimplement the retry backoff logic for ReopenTableRegionsProcedure |  Major | amv2, proc-v2 |
| [HBASE-21189](https://issues.apache.org/jira/browse/HBASE-21189) | flaky job should gather machine stats |  Minor | test |
| [HBASE-21190](https://issues.apache.org/jira/browse/HBASE-21190) | Log files and count of entries in each as we load from the MasterProcWAL store |  Major | amv2 |
| [HBASE-21083](https://issues.apache.org/jira/browse/HBASE-21083) | Introduce a mechanism to bypass the execution of a stuck procedure |  Major | amv2 |
| [HBASE-20941](https://issues.apache.org/jira/browse/HBASE-20941) | Create and implement HbckService in master |  Major | . |
| [HBASE-21072](https://issues.apache.org/jira/browse/HBASE-21072) | Block out HBCK1 in hbase2 |  Major | hbck |
| [HBASE-21093](https://issues.apache.org/jira/browse/HBASE-21093) | Move TestCreateTableProcedure.testMRegions to a separated file |  Major | test |
| [HBASE-21094](https://issues.apache.org/jira/browse/HBASE-21094) | Remove the explicit timeout config for TestTruncateTableProcedure |  Major | test |
| [HBASE-21050](https://issues.apache.org/jira/browse/HBASE-21050) | Exclusive lock may be held by a SUCCESS state procedure forever |  Major | amv2 |
| [HBASE-21044](https://issues.apache.org/jira/browse/HBASE-21044) | Disable flakey TestShell list\_procedures |  Major | test |
| [HBASE-20975](https://issues.apache.org/jira/browse/HBASE-20975) | Lock may not be taken or released while rolling back procedure |  Major | amv2 |
| [HBASE-21025](https://issues.apache.org/jira/browse/HBASE-21025) | Add cache for TableStateManager |  Major | . |
| [HBASE-21012](https://issues.apache.org/jira/browse/HBASE-21012) | Revert the change of serializing TimeRangeTracker |  Critical | . |
| [HBASE-20813](https://issues.apache.org/jira/browse/HBASE-20813) | Remove RPC quotas when the associated table/Namespace is dropped off |  Minor | . |
| [HBASE-20885](https://issues.apache.org/jira/browse/HBASE-20885) | Remove entry for RPC quota from hbase:quota when RPC quota is removed. |  Minor | . |
| [HBASE-20893](https://issues.apache.org/jira/browse/HBASE-20893) | Data loss if splitting region while ServerCrashProcedure executing |  Major | . |
| [HBASE-19369](https://issues.apache.org/jira/browse/HBASE-19369) | HBase Should use Builder Pattern to Create Log Files while using WAL on Erasure Coding |  Major | . |
| [HBASE-20939](https://issues.apache.org/jira/browse/HBASE-20939) | There will be race when we call suspendIfNotReady and then throw ProcedureSuspendedException |  Critical | amv2 |
| [HBASE-20921](https://issues.apache.org/jira/browse/HBASE-20921) | Possible NPE in ReopenTableRegionsProcedure |  Major | amv2 |
| [HBASE-20938](https://issues.apache.org/jira/browse/HBASE-20938) | Set version to 2.1.1-SNAPSHOT for branch-2.1 |  Major | build |
| [HBASE-20867](https://issues.apache.org/jira/browse/HBASE-20867) | RS may get killed while master restarts |  Major | . |
| [HBASE-20878](https://issues.apache.org/jira/browse/HBASE-20878) | Data loss if merging regions while ServerCrashProcedure executing |  Critical | amv2 |
| [HBASE-20846](https://issues.apache.org/jira/browse/HBASE-20846) | Restore procedure locks when master restarts |  Major | . |
| [HBASE-20914](https://issues.apache.org/jira/browse/HBASE-20914) | Trim Master memory usage |  Major | master |
| [HBASE-20853](https://issues.apache.org/jira/browse/HBASE-20853) | Polish "Add defaults to Table Interface so Implementors don't have to" |  Major | API |
| [HBASE-20875](https://issues.apache.org/jira/browse/HBASE-20875) | MemStoreLABImp::copyIntoCell uses 7% CPU when writing |  Major | Performance |
| [HBASE-20860](https://issues.apache.org/jira/browse/HBASE-20860) | Merged region's RIT state may not be cleaned after master restart |  Major | . |
| [HBASE-20847](https://issues.apache.org/jira/browse/HBASE-20847) | The parent procedure of RegionTransitionProcedure may not have the table lock |  Major | proc-v2, Region Assignment |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20152](https://issues.apache.org/jira/browse/HBASE-20152) | [AMv2] DisableTableProcedure versus ServerCrashProcedure |  Major | amv2 |
| [HBASE-20540](https://issues.apache.org/jira/browse/HBASE-20540) | [umbrella] Hadoop 3 compatibility |  Major | . |
| [HBASE-21294](https://issues.apache.org/jira/browse/HBASE-21294) | [2.1] Update bouncycastle dependency. |  Major | dependencies, test |
| [HBASE-21198](https://issues.apache.org/jira/browse/HBASE-21198) | Exclude dependency on net.minidev:json-smart |  Major | . |
| [HBASE-21114](https://issues.apache.org/jira/browse/HBASE-21114) | website should have a copy of 2.1 release docs |  Major | community, documentation |
| [HBASE-21287](https://issues.apache.org/jira/browse/HBASE-21287) | JVMClusterUtil Master initialization wait time not configurable |  Major | test |
| [HBASE-21168](https://issues.apache.org/jira/browse/HBASE-21168) | BloomFilterUtil uses hardcoded randomness |  Trivial | . |
| [HBASE-21125](https://issues.apache.org/jira/browse/HBASE-21125) | Backport 'HBASE-20942 Improve RpcServer TRACE logging' to branch-2.1 |  Major | Operability |
| [HBASE-20482](https://issues.apache.org/jira/browse/HBASE-20482) | Print a link to the ref guide chapter for the shell during startup |  Minor | documentation, shell |



## Release 2.1.0 - Released 2018-07-22

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20691](https://issues.apache.org/jira/browse/HBASE-20691) | Storage policy should allow deferring to HDFS |  Blocker | Filesystem Integration, wal |
| [HBASE-20270](https://issues.apache.org/jira/browse/HBASE-20270) | Turn off command help that follows all errors in shell |  Major | shell |
| [HBASE-20501](https://issues.apache.org/jira/browse/HBASE-20501) | Change the Hadoop minimum version to 2.7.1 |  Blocker | community, documentation |
| [HBASE-20406](https://issues.apache.org/jira/browse/HBASE-20406) | HBase Thrift HTTP - Shouldn't handle TRACE/OPTIONS methods |  Major | security, Thrift |
| [HBASE-20159](https://issues.apache.org/jira/browse/HBASE-20159) | Support using separate ZK quorums for client |  Major | Client, Operability, Zookeeper |
| [HBASE-20148](https://issues.apache.org/jira/browse/HBASE-20148) | Make serial replication as a option for a peer instead of a table |  Major | Replication |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20833](https://issues.apache.org/jira/browse/HBASE-20833) | Modify pre-upgrade coprocessor validator to support table level coprocessors |  Major | Coprocessors |
| [HBASE-15809](https://issues.apache.org/jira/browse/HBASE-15809) | Basic Replication WebUI |  Critical | Replication, UI |
| [HBASE-19735](https://issues.apache.org/jira/browse/HBASE-19735) | Create a minimal "client" tarball installation |  Major | build, Client |
| [HBASE-20656](https://issues.apache.org/jira/browse/HBASE-20656) | Validate pre-2.0 coprocessors against HBase 2.0+ |  Major | tooling |
| [HBASE-20592](https://issues.apache.org/jira/browse/HBASE-20592) | Create a tool to verify tables do not have prefix tree encoding |  Minor | Operability, tooling |
| [HBASE-20046](https://issues.apache.org/jira/browse/HBASE-20046) | Reconsider the implementation for serial replication |  Major | Replication |
| [HBASE-19397](https://issues.apache.org/jira/browse/HBASE-19397) | Design  procedures for ReplicationManager to notify peer change event from master |  Major | proc-v2, Replication |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20806](https://issues.apache.org/jira/browse/HBASE-20806) | Split style journal for flushes and compactions |  Minor | . |
| [HBASE-20474](https://issues.apache.org/jira/browse/HBASE-20474) | Show non-RPC tasks on master/regionserver Web UI  by default |  Major | UI |
| [HBASE-20826](https://issues.apache.org/jira/browse/HBASE-20826) | Truncate responseInfo attributes on RpcServer WARN messages |  Major | rpc |
| [HBASE-20450](https://issues.apache.org/jira/browse/HBASE-20450) | Provide metrics for number of total active, priority and replication rpc handlers |  Major | metrics |
| [HBASE-20810](https://issues.apache.org/jira/browse/HBASE-20810) | Include the procedure id in the exception message in HBaseAdmin for better debugging |  Major | Admin, proc-v2 |
| [HBASE-20040](https://issues.apache.org/jira/browse/HBASE-20040) | Master UI should include "Cluster Key" needed to use the cluster as a replication sink |  Minor | Replication, Usability |
| [HBASE-20095](https://issues.apache.org/jira/browse/HBASE-20095) | Redesign single instance pool in CleanerChore |  Critical | . |
| [HBASE-19164](https://issues.apache.org/jira/browse/HBASE-19164) | Avoid UUID.randomUUID in tests |  Major | test |
| [HBASE-20739](https://issues.apache.org/jira/browse/HBASE-20739) | Add priority for SCP |  Major | Recovery |
| [HBASE-20737](https://issues.apache.org/jira/browse/HBASE-20737) | put collection into ArrayList instead of addAll function |  Trivial | . |
| [HBASE-20695](https://issues.apache.org/jira/browse/HBASE-20695) | Implement table level RegionServer replication metrics |  Minor | metrics |
| [HBASE-20733](https://issues.apache.org/jira/browse/HBASE-20733) | QABot should run checkstyle tests if the checkstyle configs change |  Minor | build, community |
| [HBASE-20625](https://issues.apache.org/jira/browse/HBASE-20625) | refactor some WALCellCodec related code |  Minor | wal |
| [HBASE-19852](https://issues.apache.org/jira/browse/HBASE-19852) | HBase Thrift 1 server SPNEGO Improvements |  Major | Thrift |
| [HBASE-20579](https://issues.apache.org/jira/browse/HBASE-20579) | Improve snapshot manifest copy in ExportSnapshot |  Minor | mapreduce |
| [HBASE-20444](https://issues.apache.org/jira/browse/HBASE-20444) | Improve version comparison logic for HBase specific version string and add unit tests |  Major | util |
| [HBASE-20594](https://issues.apache.org/jira/browse/HBASE-20594) | provide utility to compare old and new descriptors |  Major | . |
| [HBASE-20640](https://issues.apache.org/jira/browse/HBASE-20640) | TestQuotaGlobalsSettingsBypass missing test category and ClassRule |  Critical | test |
| [HBASE-20478](https://issues.apache.org/jira/browse/HBASE-20478) | move import checks from hbaseanti to checkstyle |  Minor | test |
| [HBASE-20548](https://issues.apache.org/jira/browse/HBASE-20548) | Master fails to startup on large clusters, refreshing block distribution |  Major | . |
| [HBASE-20488](https://issues.apache.org/jira/browse/HBASE-20488) | PE tool prints full name in help message |  Minor | shell |
| [HBASE-20567](https://issues.apache.org/jira/browse/HBASE-20567) | Pass both old and new descriptors to pre/post hooks of modify operations for table and namespace |  Major | . |
| [HBASE-20545](https://issues.apache.org/jira/browse/HBASE-20545) | Improve performance of BaseLoadBalancer.retainAssignment |  Major | Balancer |
| [HBASE-16191](https://issues.apache.org/jira/browse/HBASE-16191) | Add stop\_regionserver and stop\_master to shell |  Major | . |
| [HBASE-20536](https://issues.apache.org/jira/browse/HBASE-20536) | Make TestRegionServerAccounting stable and it should not use absolute number |  Minor | . |
| [HBASE-20523](https://issues.apache.org/jira/browse/HBASE-20523) | PE tool should support configuring client side buffering sizes |  Minor | . |
| [HBASE-20527](https://issues.apache.org/jira/browse/HBASE-20527) | Remove unused code in MetaTableAccessor |  Trivial | . |
| [HBASE-20507](https://issues.apache.org/jira/browse/HBASE-20507) | Do not need to call recoverLease on the broken file when we fail to create a wal writer |  Major | wal |
| [HBASE-20484](https://issues.apache.org/jira/browse/HBASE-20484) | Remove the unnecessary autoboxing in FilterListBase |  Trivial | . |
| [HBASE-20327](https://issues.apache.org/jira/browse/HBASE-20327) | When qualifier is not specified, append and incr operation do not work (shell) |  Minor | shell |
| [HBASE-20389](https://issues.apache.org/jira/browse/HBASE-20389) | Move website building flags into a profile |  Minor | build, website |
| [HBASE-20379](https://issues.apache.org/jira/browse/HBASE-20379) | shadedjars yetus plugin should add a footer link |  Major | test |
| [HBASE-20243](https://issues.apache.org/jira/browse/HBASE-20243) | [Shell] Add shell command to create a new table by cloning the existent table |  Minor | shell |
| [HBASE-20286](https://issues.apache.org/jira/browse/HBASE-20286) | Improving shell command compaction\_state |  Minor | shell |
| [HBASE-19488](https://issues.apache.org/jira/browse/HBASE-19488) | Move to using Apache commons CollectionUtils |  Trivial | . |
| [HBASE-20197](https://issues.apache.org/jira/browse/HBASE-20197) | Review of ByteBufferWriterOutputStream.java |  Minor | . |
| [HBASE-20047](https://issues.apache.org/jira/browse/HBASE-20047) | AuthenticationTokenIdentifier should provide a toString |  Minor | Usability |
| [HBASE-19024](https://issues.apache.org/jira/browse/HBASE-19024) | Configurable default durability for synchronous WAL |  Critical | wal |
| [HBASE-19389](https://issues.apache.org/jira/browse/HBASE-19389) | Limit concurrency of put with dense (hundreds) columns to prevent write handler exhausted |  Critical | Performance |
| [HBASE-20186](https://issues.apache.org/jira/browse/HBASE-20186) | Improve RSGroupBasedLoadBalancer#balanceCluster() to be more efficient when calculating cluster state for each rsgroup |  Minor | rsgroup |
| [HBASE-19449](https://issues.apache.org/jira/browse/HBASE-19449) | Minor logging change in HFileArchiver |  Trivial | . |
| [HBASE-20120](https://issues.apache.org/jira/browse/HBASE-20120) | Remove some unused classes/ java files from hbase-server |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20854](https://issues.apache.org/jira/browse/HBASE-20854) | Wrong retries number in RpcRetryingCaller's log message |  Minor | Client, logging |
| [HBASE-20784](https://issues.apache.org/jira/browse/HBASE-20784) | Will lose the SNAPSHOT suffix if we get the version of RS from ServerManager |  Minor | master, UI |
| [HBASE-20822](https://issues.apache.org/jira/browse/HBASE-20822) | TestAsyncNonMetaRegionLocator is flakey |  Major | asyncclient |
| [HBASE-20808](https://issues.apache.org/jira/browse/HBASE-20808) | Wrong shutdown order between Chores and ChoreService |  Minor | . |
| [HBASE-20789](https://issues.apache.org/jira/browse/HBASE-20789) | TestBucketCache#testCacheBlockNextBlockMetadataMissing is flaky |  Major | . |
| [HBASE-20829](https://issues.apache.org/jira/browse/HBASE-20829) | Remove the addFront assertion in MasterProcedureScheduler.doAdd |  Major | Replication |
| [HBASE-20825](https://issues.apache.org/jira/browse/HBASE-20825) | Fix pre and post hooks of CloneSnapshot and RestoreSnapshot for Access checks |  Major | security |
| [HBASE-20812](https://issues.apache.org/jira/browse/HBASE-20812) | Add defaults to Table Interface so implementors don't have to |  Major | . |
| [HBASE-20817](https://issues.apache.org/jira/browse/HBASE-20817) | Infinite loop when executing ReopenTableRegionsProcedure |  Blocker | Region Assignment |
| [HBASE-20792](https://issues.apache.org/jira/browse/HBASE-20792) | info:servername and info:sn inconsistent for OPEN region |  Blocker | Region Assignment |
| [HBASE-20769](https://issues.apache.org/jira/browse/HBASE-20769) | getSplits() has a out of bounds problem in TableSnapshotInputFormatImpl |  Major | . |
| [HBASE-20732](https://issues.apache.org/jira/browse/HBASE-20732) | Shutdown scan pool when master is stopped. |  Minor | . |
| [HBASE-20785](https://issues.apache.org/jira/browse/HBASE-20785) | NPE getting metrics in PE testing scans |  Major | Performance |
| [HBASE-20795](https://issues.apache.org/jira/browse/HBASE-20795) | Allow option in BBKVComparator.compare to do comparison without sequence id |  Major | . |
| [HBASE-20777](https://issues.apache.org/jira/browse/HBASE-20777) | RpcConnection could still remain opened after we shutdown the NettyRpcServer |  Major | rpc |
| [HBASE-20403](https://issues.apache.org/jira/browse/HBASE-20403) | Prefetch sometimes doesn't work with encrypted file system |  Major | . |
| [HBASE-20635](https://issues.apache.org/jira/browse/HBASE-20635) | Support to convert the shaded user permission proto to client user permission object |  Major | . |
| [HBASE-20778](https://issues.apache.org/jira/browse/HBASE-20778) | Make it so WALPE runs on DFS |  Major | test |
| [HBASE-20775](https://issues.apache.org/jira/browse/HBASE-20775) | TestMultiParallel is flakey |  Major | Region Assignment |
| [HBASE-20752](https://issues.apache.org/jira/browse/HBASE-20752) | Make sure the regions are truly reopened after ReopenTableRegionsProcedure |  Major | proc-v2 |
| [HBASE-18622](https://issues.apache.org/jira/browse/HBASE-18622) | Mitigate API compatibility concerns between branch-1 and branch-2 |  Blocker | API |
| [HBASE-20767](https://issues.apache.org/jira/browse/HBASE-20767) | Always close hbaseAdmin along with connection in HBTU |  Major | test |
| [HBASE-20642](https://issues.apache.org/jira/browse/HBASE-20642) | IntegrationTestDDLMasterFailover throws 'InvalidFamilyOperationException |  Major | . |
| [HBASE-20742](https://issues.apache.org/jira/browse/HBASE-20742) | Always create WAL directory for region server |  Major | wal |
| [HBASE-20708](https://issues.apache.org/jira/browse/HBASE-20708) | Remove the usage of RecoverMetaProcedure in master startup |  Blocker | proc-v2, Region Assignment |
| [HBASE-20723](https://issues.apache.org/jira/browse/HBASE-20723) | Custom hbase.wal.dir results in data loss because we write recovered edits into a different place than where the recovering region server looks for them |  Critical | Recovery, wal |
| [HBASE-20681](https://issues.apache.org/jira/browse/HBASE-20681) | IntegrationTestDriver fails after HADOOP-15406 due to missing hamcrest-core |  Major | integration tests |
| [HBASE-20561](https://issues.apache.org/jira/browse/HBASE-20561) | The way we stop a ReplicationSource may cause the RS down |  Major | Replication |
| [HBASE-19377](https://issues.apache.org/jira/browse/HBASE-19377) | Compatibility checker complaining about hash collisions |  Major | community |
| [HBASE-20689](https://issues.apache.org/jira/browse/HBASE-20689) | Docker fails to install rubocop for precommit |  Blocker | build |
| [HBASE-20707](https://issues.apache.org/jira/browse/HBASE-20707) | Move MissingSwitchDefault check from checkstyle to error-prone |  Major | build |
| [HBASE-20699](https://issues.apache.org/jira/browse/HBASE-20699) | QuotaCache should cancel the QuotaRefresherChore service inside its stop() |  Major | . |
| [HBASE-20590](https://issues.apache.org/jira/browse/HBASE-20590) | REST Java client is not able to negotiate with the server in the secure mode |  Critical | REST, security |
| [HBASE-20683](https://issues.apache.org/jira/browse/HBASE-20683) | Incorrect return value for PreUpgradeValidator |  Critical | . |
| [HBASE-20684](https://issues.apache.org/jira/browse/HBASE-20684) | org.apache.hadoop.hbase.client.Scan#setStopRow javadoc uses incorrect method |  Trivial | Client, documentation |
| [HBASE-20678](https://issues.apache.org/jira/browse/HBASE-20678) | NPE in ReplicationSourceManager#NodeFailoverWorker |  Minor | . |
| [HBASE-20670](https://issues.apache.org/jira/browse/HBASE-20670) | NPE in HMaster#isInMaintenanceMode |  Minor | . |
| [HBASE-20634](https://issues.apache.org/jira/browse/HBASE-20634) | Reopen region while server crash can cause the procedure to be stuck |  Critical | . |
| [HBASE-12882](https://issues.apache.org/jira/browse/HBASE-12882) | Log level for org.apache.hadoop.hbase package should be configurable |  Major | . |
| [HBASE-20668](https://issues.apache.org/jira/browse/HBASE-20668) | Avoid permission change if ExportSnapshot's copy fails |  Major | . |
| [HBASE-18116](https://issues.apache.org/jira/browse/HBASE-18116) | Replication source in-memory accounting should not include bulk transfer hfiles |  Major | Replication |
| [HBASE-20602](https://issues.apache.org/jira/browse/HBASE-20602) | hbase.master.quota.observer.ignore property seems to be not taking effect |  Minor | documentation |
| [HBASE-20664](https://issues.apache.org/jira/browse/HBASE-20664) | Variable shared across multiple threads |  Major | . |
| [HBASE-20659](https://issues.apache.org/jira/browse/HBASE-20659) | Implement a reopen table regions procedure |  Major | . |
| [HBASE-20582](https://issues.apache.org/jira/browse/HBASE-20582) | Bump up JRuby version because of some reported vulnerabilities |  Major | dependencies, shell |
| [HBASE-20533](https://issues.apache.org/jira/browse/HBASE-20533) | Fix the flaky TestAssignmentManagerMetrics |  Major | . |
| [HBASE-20597](https://issues.apache.org/jira/browse/HBASE-20597) | Use a lock to serialize access to a shared reference to ZooKeeperWatcher in HBaseReplicationEndpoint |  Minor | Replication |
| [HBASE-20633](https://issues.apache.org/jira/browse/HBASE-20633) | Dropping a table containing a disable violation policy fails to remove the quota upon table delete |  Major | . |
| [HBASE-20645](https://issues.apache.org/jira/browse/HBASE-20645) | Fix security\_available method in security.rb |  Major | . |
| [HBASE-20612](https://issues.apache.org/jira/browse/HBASE-20612) | TestReplicationKillSlaveRSWithSeparateOldWALs sometimes fail because it uses an expired cluster conn |  Major | . |
| [HBASE-20648](https://issues.apache.org/jira/browse/HBASE-20648) | HBASE-19364 "Truncate\_preserve fails with table when replica region \> 1" for master branch |  Major | . |
| [HBASE-20588](https://issues.apache.org/jira/browse/HBASE-20588) | Space quota change after quota violation doesn't seem to take in effect |  Major | regionserver |
| [HBASE-20616](https://issues.apache.org/jira/browse/HBASE-20616) | TruncateTableProcedure is stuck in retry loop in TRUNCATE\_TABLE\_CREATE\_FS\_LAYOUT state |  Major | amv2 |
| [HBASE-20638](https://issues.apache.org/jira/browse/HBASE-20638) | nightly source artifact testing should fail the stage if it's going to report an error on jira |  Major | test |
| [HBASE-20624](https://issues.apache.org/jira/browse/HBASE-20624) | Race in ReplicationSource which causes walEntryFilter being null when creating new shipper |  Major | Replication |
| [HBASE-20601](https://issues.apache.org/jira/browse/HBASE-20601) | Add multiPut support and other miscellaneous to PE |  Minor | tooling |
| [HBASE-20627](https://issues.apache.org/jira/browse/HBASE-20627) | Relocate RS Group pre/post hooks from RSGroupAdminServer to RSGroupAdminEndpoint |  Major | . |
| [HBASE-20591](https://issues.apache.org/jira/browse/HBASE-20591) | nightly job doesn't respect maven options |  Critical | test |
| [HBASE-20560](https://issues.apache.org/jira/browse/HBASE-20560) | Revisit the TestReplicationDroppedTables ut |  Major | . |
| [HBASE-20571](https://issues.apache.org/jira/browse/HBASE-20571) | JMXJsonServlet generates invalid JSON if it has NaN in metrics |  Major | UI |
| [HBASE-20585](https://issues.apache.org/jira/browse/HBASE-20585) | Need to clear peer map when clearing MasterProcedureScheduler |  Major | proc-v2 |
| [HBASE-20457](https://issues.apache.org/jira/browse/HBASE-20457) | Return immediately for a scan rpc call when we want to switch from pread to stream |  Major | scan |
| [HBASE-20447](https://issues.apache.org/jira/browse/HBASE-20447) | Only fail cacheBlock if block collisions aren't related to next block metadata |  Major | BlockCache, BucketCache |
| [HBASE-20544](https://issues.apache.org/jira/browse/HBASE-20544) | downstream HBaseTestingUtility fails with invalid port |  Blocker | test |
| [HBASE-20004](https://issues.apache.org/jira/browse/HBASE-20004) | Client is not able to execute REST queries in a secure cluster |  Minor | REST, security |
| [HBASE-20475](https://issues.apache.org/jira/browse/HBASE-20475) | Fix the flaky TestReplicationDroppedTables unit test. |  Major | . |
| [HBASE-20554](https://issues.apache.org/jira/browse/HBASE-20554) | "WALs outstanding" message from CleanerChore is noisy |  Trivial | . |
| [HBASE-20204](https://issues.apache.org/jira/browse/HBASE-20204) | Add locking to RefreshFileConnections in BucketCache |  Major | BucketCache |
| [HBASE-20485](https://issues.apache.org/jira/browse/HBASE-20485) | Copy constructor of Scan doesn't copy the readType and replicaId |  Minor | . |
| [HBASE-20543](https://issues.apache.org/jira/browse/HBASE-20543) | Fix the flaky TestThriftHttpServer |  Major | . |
| [HBASE-20521](https://issues.apache.org/jira/browse/HBASE-20521) | TableOutputFormat.checkOutputSpecs conf checking sequence cause pig script run fail |  Major | mapreduce |
| [HBASE-20500](https://issues.apache.org/jira/browse/HBASE-20500) | [rsgroup] should keep at least one server in default group |  Major | rsgroup |
| [HBASE-20517](https://issues.apache.org/jira/browse/HBASE-20517) | Fix PerformanceEvaluation 'column' parameter |  Major | test |
| [HBASE-20524](https://issues.apache.org/jira/browse/HBASE-20524) | Need to clear metrics when ReplicationSourceManager refresh replication sources |  Minor | . |
| [HBASE-20476](https://issues.apache.org/jira/browse/HBASE-20476) | Open sequence number could go backwards in AssignProcedure |  Major | Region Assignment |
| [HBASE-20506](https://issues.apache.org/jira/browse/HBASE-20506) | Add doc and test for unused RetryCounter, useful-looking utility |  Minor | . |
| [HBASE-20492](https://issues.apache.org/jira/browse/HBASE-20492) | UnassignProcedure is stuck in retry loop on region stuck in OPENING state |  Critical | amv2 |
| [HBASE-20497](https://issues.apache.org/jira/browse/HBASE-20497) | The getRecoveredQueueStartPos always return 0 in RecoveredReplicationSourceShipper |  Major | Replication |
| [HBASE-18842](https://issues.apache.org/jira/browse/HBASE-18842) | The hbase shell clone\_snaphost command returns bad error message |  Minor | shell |
| [HBASE-20466](https://issues.apache.org/jira/browse/HBASE-20466) | Consistently use override mechanism for exempt classes in CoprocessClassloader |  Major | Coprocessors |
| [HBASE-20006](https://issues.apache.org/jira/browse/HBASE-20006) | TestRestoreSnapshotFromClientWithRegionReplicas is flakey |  Critical | read replicas |
| [HBASE-18059](https://issues.apache.org/jira/browse/HBASE-18059) | The scanner order for memstore scanners are wrong |  Critical | regionserver, scan, Scanners |
| [HBASE-20404](https://issues.apache.org/jira/browse/HBASE-20404) | Ugly cleanerchore complaint that dir is not empty |  Major | master |
| [HBASE-20419](https://issues.apache.org/jira/browse/HBASE-20419) | Fix potential NPE in ZKUtil#listChildrenAndWatchForNewChildren callers |  Major | . |
| [HBASE-20364](https://issues.apache.org/jira/browse/HBASE-20364) | nightly job gives old results or no results for stages that timeout on SCM |  Critical | test |
| [HBASE-20335](https://issues.apache.org/jira/browse/HBASE-20335) | nightly jobs no longer contain machine information |  Critical | test |
| [HBASE-20338](https://issues.apache.org/jira/browse/HBASE-20338) | WALProcedureStore#recoverLease() should have fixed sleeps for retrying rollWriter() |  Major | . |
| [HBASE-20356](https://issues.apache.org/jira/browse/HBASE-20356) | make skipping protoc possible |  Critical | dependencies, thirdparty |
| [HBASE-15291](https://issues.apache.org/jira/browse/HBASE-15291) | FileSystem not closed in secure bulkLoad |  Major | . |
| [HBASE-20068](https://issues.apache.org/jira/browse/HBASE-20068) | Hadoopcheck project health check uses default maven repo instead of yetus managed ones |  Major | community, test |
| [HBASE-20361](https://issues.apache.org/jira/browse/HBASE-20361) | Non-successive TableInputSplits may wrongly be merged by auto balancing feature |  Major | mapreduce |
| [HBASE-20260](https://issues.apache.org/jira/browse/HBASE-20260) | Purge old content from the book for branch-2/master |  Critical | documentation |
| [HBASE-20058](https://issues.apache.org/jira/browse/HBASE-20058) | improper quoting in presplitting command docs |  Minor | documentation |
| [HBASE-19923](https://issues.apache.org/jira/browse/HBASE-19923) | Reset peer state and config when refresh replication source failed |  Major | Replication |
| [HBASE-19748](https://issues.apache.org/jira/browse/HBASE-19748) | TestRegionReplicaFailover and TestRegionReplicaReplicationEndpoint UT hangs |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20667](https://issues.apache.org/jira/browse/HBASE-20667) | Rename TestGlobalThrottler to TestReplicationGlobalThrottler |  Trivial | . |
| [HBASE-20646](https://issues.apache.org/jira/browse/HBASE-20646) | TestWALProcedureStoreOnHDFS failing on branch-1 |  Trivial | . |
| [HBASE-20505](https://issues.apache.org/jira/browse/HBASE-20505) | PE should support multi column family read and write cases |  Minor | . |
| [HBASE-20513](https://issues.apache.org/jira/browse/HBASE-20513) | Collect and emit ScanMetrics in PerformanceEvaluation |  Minor | test |
| [HBASE-20414](https://issues.apache.org/jira/browse/HBASE-20414) | TestLockProcedure#testMultipleLocks may fail on slow machine |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20831](https://issues.apache.org/jira/browse/HBASE-20831) | Copy master doc into branch-2.1 and edit to make it suit 2.1.0 |  Blocker | documentation |
| [HBASE-20839](https://issues.apache.org/jira/browse/HBASE-20839) | Fallback to FSHLog if we can not instantiated AsyncFSWAL when user does not specify AsyncFSWAL explicitly |  Blocker | wal |
| [HBASE-20244](https://issues.apache.org/jira/browse/HBASE-20244) | NoSuchMethodException when retrieving private method decryptEncryptedDataEncryptionKey from DFSClient |  Blocker | wal |
| [HBASE-20193](https://issues.apache.org/jira/browse/HBASE-20193) | Basic Replication Web UI - Regionserver |  Critical | Replication, Usability |
| [HBASE-20489](https://issues.apache.org/jira/browse/HBASE-20489) | Update Reference Guide that CLUSTER\_KEY value is present on the Master UI info page. |  Minor | documentation |
| [HBASE-19722](https://issues.apache.org/jira/browse/HBASE-19722) | Meta query statistics metrics source |  Major | . |
| [HBASE-20781](https://issues.apache.org/jira/browse/HBASE-20781) | Save recalculating families in a WALEdit batch of Cells |  Major | Performance |
| [HBASE-20194](https://issues.apache.org/jira/browse/HBASE-20194) | Basic Replication WebUI - Master |  Critical | Replication, Usability |
| [HBASE-20780](https://issues.apache.org/jira/browse/HBASE-20780) | ServerRpcConnection logging cleanup |  Major | logging, Performance |
| [HBASE-19764](https://issues.apache.org/jira/browse/HBASE-19764) | Fix Checkstyle errors in hbase-endpoint |  Minor | . |
| [HBASE-20710](https://issues.apache.org/jira/browse/HBASE-20710) | extra cloneFamily() in Mutation.add(Cell) |  Minor | regionserver |
| [HBASE-18569](https://issues.apache.org/jira/browse/HBASE-18569) | Add prefetch support for async region locator |  Major | asyncclient, Client |
| [HBASE-20706](https://issues.apache.org/jira/browse/HBASE-20706) | [hack] Don't add known not-OPEN regions in reopen phase of MTP |  Critical | amv2 |
| [HBASE-20334](https://issues.apache.org/jira/browse/HBASE-20334) | add a test that expressly uses both our shaded client and the one from hadoop 3 |  Major | hadoop3, shading |
| [HBASE-20615](https://issues.apache.org/jira/browse/HBASE-20615) | emphasize use of shaded client jars when they're present in an install |  Major | build, Client, Usability |
| [HBASE-20333](https://issues.apache.org/jira/browse/HBASE-20333) | break up shaded client into one with no Hadoop and one that's standalone |  Critical | shading |
| [HBASE-20332](https://issues.apache.org/jira/browse/HBASE-20332) | shaded mapreduce module shouldn't include hadoop |  Critical | mapreduce, shading |
| [HBASE-20722](https://issues.apache.org/jira/browse/HBASE-20722) | Make RegionServerTracker only depend on children changed event |  Major | . |
| [HBASE-20700](https://issues.apache.org/jira/browse/HBASE-20700) | Move meta region when server crash can cause the procedure to be stuck |  Critical | master, proc-v2, Region Assignment |
| [HBASE-20628](https://issues.apache.org/jira/browse/HBASE-20628) | SegmentScanner does over-comparing when one flushing |  Critical | Performance |
| [HBASE-19761](https://issues.apache.org/jira/browse/HBASE-19761) | Fix Checkstyle errors in hbase-zookeeper |  Minor | . |
| [HBASE-19724](https://issues.apache.org/jira/browse/HBASE-19724) | Fix Checkstyle errors in hbase-hadoop2-compat |  Minor | . |
| [HBASE-20518](https://issues.apache.org/jira/browse/HBASE-20518) | Need to serialize the enabled field for UpdatePeerConfigProcedure |  Major | Replication |
| [HBASE-20481](https://issues.apache.org/jira/browse/HBASE-20481) | Replicate entries from same region serially in ReplicationEndpoint for serial replication |  Major | . |
| [HBASE-20378](https://issues.apache.org/jira/browse/HBASE-20378) | Provide a hbck option to cleanup replication barrier for a table |  Major | . |
| [HBASE-20128](https://issues.apache.org/jira/browse/HBASE-20128) | Add new UTs which extends the old replication UTs but set replication scope to SERIAL |  Major | . |
| [HBASE-20417](https://issues.apache.org/jira/browse/HBASE-20417) | Do not read wal entries when peer is disabled |  Major | Replication |
| [HBASE-20294](https://issues.apache.org/jira/browse/HBASE-20294) | Also cleanup last pushed sequence id in ReplicationBarrierCleaner |  Major | Replication |
| [HBASE-20377](https://issues.apache.org/jira/browse/HBASE-20377) | Deal with table in enabling and disabling state when modifying serial replication peer |  Major | Replication |
| [HBASE-20367](https://issues.apache.org/jira/browse/HBASE-20367) | Write a replication barrier for regions when disabling a table |  Major | Replication |
| [HBASE-20296](https://issues.apache.org/jira/browse/HBASE-20296) | Remove last pushed sequence ids when removing tables from a peer |  Major | Replication |
| [HBASE-20285](https://issues.apache.org/jira/browse/HBASE-20285) | Delete all last pushed sequence ids when removing a peer or removing the serial flag for a peer |  Major | Replication |
| [HBASE-20138](https://issues.apache.org/jira/browse/HBASE-20138) | Find a way to deal with the conflicts when updating replication position |  Major | Replication |
| [HBASE-20127](https://issues.apache.org/jira/browse/HBASE-20127) | Add UT for serial replication after failover |  Major | Replication, test |
| [HBASE-20271](https://issues.apache.org/jira/browse/HBASE-20271) | ReplicationSourceWALReader.switched should use the file name instead of the path object directly |  Major | Replication |
| [HBASE-20227](https://issues.apache.org/jira/browse/HBASE-20227) | Add UT for ReplicationUtils.contains method |  Major | Replication, test |
| [HBASE-20147](https://issues.apache.org/jira/browse/HBASE-20147) | Serial replication will be stuck if we create a table with serial replication but add it to a peer after there are region moves |  Major | . |
| [HBASE-20116](https://issues.apache.org/jira/browse/HBASE-20116) | Optimize the region last pushed sequence id layout on zk |  Major | Replication |
| [HBASE-20242](https://issues.apache.org/jira/browse/HBASE-20242) | The open sequence number will grow if we fail to open a region after writing the max sequence id file |  Major | . |
| [HBASE-20155](https://issues.apache.org/jira/browse/HBASE-20155) | update branch-2 version to 2.1.0-SNAPSHOT |  Major | build, community |
| [HBASE-20206](https://issues.apache.org/jira/browse/HBASE-20206) | WALEntryStream should not switch WAL file silently |  Major | Replication |
| [HBASE-20117](https://issues.apache.org/jira/browse/HBASE-20117) | Cleanup the unused replication barriers in meta table |  Major | master, Replication |
| [HBASE-20165](https://issues.apache.org/jira/browse/HBASE-20165) | Shell command to make a normal peer to be a serial replication peer |  Major | . |
| [HBASE-20167](https://issues.apache.org/jira/browse/HBASE-20167) | Optimize the implementation of ReplicationSourceWALReader |  Major | Replication |
| [HBASE-20125](https://issues.apache.org/jira/browse/HBASE-20125) | Add UT for serial replication after region split and merge |  Major | Replication |
| [HBASE-20129](https://issues.apache.org/jira/browse/HBASE-20129) | Add UT for serial replication checker |  Major | Replication |
| [HBASE-20115](https://issues.apache.org/jira/browse/HBASE-20115) | Reimplement serial replication based on the new replication storage layer |  Major | Replication |
| [HBASE-20050](https://issues.apache.org/jira/browse/HBASE-20050) | Reimplement updateReplicationPositions logic in serial replication based on the newly introduced replication storage layer |  Major | . |
| [HBASE-20082](https://issues.apache.org/jira/browse/HBASE-20082) | Fix findbugs errors only on master which are introduced by HBASE-19397 |  Major | findbugs |
| [HBASE-19936](https://issues.apache.org/jira/browse/HBASE-19936) | Introduce a new base class for replication peer procedure |  Major | . |
| [HBASE-19719](https://issues.apache.org/jira/browse/HBASE-19719) | Fix checkstyle issues |  Major | proc-v2, Replication |
| [HBASE-19711](https://issues.apache.org/jira/browse/HBASE-19711) | TestReplicationAdmin.testConcurrentPeerOperations hangs |  Major | proc-v2 |
| [HBASE-19707](https://issues.apache.org/jira/browse/HBASE-19707) | Race in start and terminate of a replication source after we async start replicatione endpoint |  Major | proc-v2, Replication |
| [HBASE-19636](https://issues.apache.org/jira/browse/HBASE-19636) | All rs should already start work with the new peer change when replication peer procedure is finished |  Major | proc-v2, Replication |
| [HBASE-19634](https://issues.apache.org/jira/browse/HBASE-19634) | Add permission check for executeProcedures in AccessController |  Major | proc-v2, Replication |
| [HBASE-19697](https://issues.apache.org/jira/browse/HBASE-19697) | Remove TestReplicationAdminUsingProcedure |  Major | proc-v2, Replication |
| [HBASE-19661](https://issues.apache.org/jira/browse/HBASE-19661) | Replace ReplicationStateZKBase with ZKReplicationStorageBase |  Major | proc-v2, Replication |
| [HBASE-19687](https://issues.apache.org/jira/browse/HBASE-19687) | Move the logic in ReplicationZKNodeCleaner to ReplicationChecker and remove ReplicationZKNodeCleanerChore |  Major | proc-v2, Replication |
| [HBASE-19544](https://issues.apache.org/jira/browse/HBASE-19544) | Add UTs for testing concurrent modifications on replication peer |  Major | proc-v2, Replication, test |
| [HBASE-19686](https://issues.apache.org/jira/browse/HBASE-19686) | Use KeyLocker instead of ReentrantLock in PeerProcedureHandlerImpl |  Major | proc-v2, Replication |
| [HBASE-19623](https://issues.apache.org/jira/browse/HBASE-19623) | Create replication endpoint asynchronously when adding a replication source |  Major | proc-v2, Replication |
| [HBASE-19633](https://issues.apache.org/jira/browse/HBASE-19633) | Clean up the replication queues in the postPeerModification stage when removing a peer |  Major | proc-v2, Replication |
| [HBASE-19622](https://issues.apache.org/jira/browse/HBASE-19622) | Reimplement ReplicationPeers with the new replication storage interface |  Major | proc-v2, Replication |
| [HBASE-19635](https://issues.apache.org/jira/browse/HBASE-19635) | Introduce a thread at RS side to call reportProcedureDone |  Major | proc-v2 |
| [HBASE-19617](https://issues.apache.org/jira/browse/HBASE-19617) | Remove ReplicationQueues, use ReplicationQueueStorage directly |  Major | Replication |
| [HBASE-19642](https://issues.apache.org/jira/browse/HBASE-19642) | Fix locking for peer modification procedure |  Critical | proc-v2, Replication |
| [HBASE-19592](https://issues.apache.org/jira/browse/HBASE-19592) | Add UTs to test retry on update zk failure |  Major | proc-v2, Replication |
| [HBASE-19630](https://issues.apache.org/jira/browse/HBASE-19630) | Add peer cluster key check when add new replication peer |  Major | proc-v2, Replication |
| [HBASE-19573](https://issues.apache.org/jira/browse/HBASE-19573) | Rewrite ReplicationPeer with the new replication storage interface |  Major | proc-v2, Replication |
| [HBASE-19579](https://issues.apache.org/jira/browse/HBASE-19579) | Add peer lock test for shell command list\_locks |  Major | proc-v2, Replication |
| [HBASE-19599](https://issues.apache.org/jira/browse/HBASE-19599) | Remove ReplicationQueuesClient, use ReplicationQueueStorage directly |  Major | Replication |
| [HBASE-19543](https://issues.apache.org/jira/browse/HBASE-19543) | Abstract a replication storage interface to extract the zk specific code |  Major | proc-v2, Replication |
| [HBASE-19525](https://issues.apache.org/jira/browse/HBASE-19525) | RS side changes for moving peer modification from zk watcher to procedure |  Major | proc-v2, Replication |
| [HBASE-19580](https://issues.apache.org/jira/browse/HBASE-19580) | Use slf4j instead of commons-logging in new, just-added Peer Procedure classes |  Major | proc-v2, Replication |
| [HBASE-19520](https://issues.apache.org/jira/browse/HBASE-19520) | Add UTs for the new lock type PEER |  Major | proc-v2 |
| [HBASE-19564](https://issues.apache.org/jira/browse/HBASE-19564) | Procedure id is missing in the response of peer related operations |  Major | proc-v2, Replication |
| [HBASE-19536](https://issues.apache.org/jira/browse/HBASE-19536) | Client side changes for moving peer modification from zk watcher to procedure |  Major | Replication |
| [HBASE-19524](https://issues.apache.org/jira/browse/HBASE-19524) | Master side changes for moving peer modification from zk watcher to procedure |  Major | proc-v2, Replication |
| [HBASE-19216](https://issues.apache.org/jira/browse/HBASE-19216) | Implement a general framework to execute remote procedure on RS |  Major | proc-v2, Replication |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-19997](https://issues.apache.org/jira/browse/HBASE-19997) | [rolling upgrade] 1.x =\> 2.x |  Blocker | . |
| [HBASE-20360](https://issues.apache.org/jira/browse/HBASE-20360) | Further optimization for serial replication |  Major | Replication |
| [HBASE-20862](https://issues.apache.org/jira/browse/HBASE-20862) | Address 2.1.0 Compatibility Report Issues |  Blocker | compatibility |
| [HBASE-20665](https://issues.apache.org/jira/browse/HBASE-20665) | "Already cached block XXX" message should be DEBUG |  Minor | BlockCache |
| [HBASE-20677](https://issues.apache.org/jira/browse/HBASE-20677) | Backport test of HBASE-20566 'Creating a system table after enabling rsgroup feature puts region into RIT' to branch-2 |  Major | . |
| [HBASE-19475](https://issues.apache.org/jira/browse/HBASE-19475) | Extend backporting strategy in documentation |  Trivial | documentation |
| [HBASE-20595](https://issues.apache.org/jira/browse/HBASE-20595) | Remove the concept of 'special tables' from rsgroups |  Major | Region Assignment, rsgroup |
| [HBASE-20415](https://issues.apache.org/jira/browse/HBASE-20415) | branches-2 don't need maven-scala-plugin |  Major | build |
| [HBASE-20112](https://issues.apache.org/jira/browse/HBASE-20112) | Include test results from nightly hadoop3 tests in jenkins test results |  Critical | test |
| [HBASE-17918](https://issues.apache.org/jira/browse/HBASE-17918) | document serial replication |  Critical | documentation, Replication |
| [HBASE-19737](https://issues.apache.org/jira/browse/HBASE-19737) | Manage a HBASE-19397-branch-2 branch and merge it to branch-2 |  Major | proc-v2, Replication |
