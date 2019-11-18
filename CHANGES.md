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
# HBASE Changelog

## Release 2.1.8 - Unreleased (as of 2019-11-18)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-19450](https://issues.apache.org/jira/browse/HBASE-19450) | Add log about average execution time for ScheduledChore |  Minor | Operability |
| [HBASE-23283](https://issues.apache.org/jira/browse/HBASE-23283) | Provide clear and consistent logging about the period of enabled chores |  Minor | Operability |
| [HBASE-23245](https://issues.apache.org/jira/browse/HBASE-23245) | All MutableHistogram implementations should remove maxExpected |  Major | metrics |
| [HBASE-23228](https://issues.apache.org/jira/browse/HBASE-23228) | Allow for jdk8 specific modules on branch-1 in precommit/nightly testing |  Critical | build, test |
| [HBASE-23082](https://issues.apache.org/jira/browse/HBASE-23082) | Backport low-latency snapshot tracking for space quotas to 2.x |  Major | Quotas |
| [HBASE-23238](https://issues.apache.org/jira/browse/HBASE-23238) | Additional test and checks for null references on ScannerCallableWithReplicas |  Minor | . |
| [HBASE-23221](https://issues.apache.org/jira/browse/HBASE-23221) | Polish the WAL interface after HBASE-23181 |  Major | regionserver, wal |
| [HBASE-23207](https://issues.apache.org/jira/browse/HBASE-23207) | Log a region open journal |  Minor | . |
| [HBASE-23172](https://issues.apache.org/jira/browse/HBASE-23172) | HBase Canary region success count metrics reflect column family successes, not region successes |  Minor | canary |
| [HBASE-20626](https://issues.apache.org/jira/browse/HBASE-20626) | Change the value of "Requests Per Second" on WEBUI |  Major | metrics, UI |
| [HBASE-23093](https://issues.apache.org/jira/browse/HBASE-23093) | Avoid Optional Anti-Pattern where possible |  Minor | . |
| [HBASE-23114](https://issues.apache.org/jira/browse/HBASE-23114) | Use archiveArtifacts in Jenkinsfiles |  Trivial | . |
| [HBASE-23095](https://issues.apache.org/jira/browse/HBASE-23095) | Reuse FileStatus in StoreFileInfo |  Major | mob, snapshots |
| [HBASE-23116](https://issues.apache.org/jira/browse/HBASE-23116) | LoadBalancer should log table name when balancing per table |  Minor | . |
| [HBASE-22874](https://issues.apache.org/jira/browse/HBASE-22874) | Define a public interface for Canary and move existing implementation to LimitedPrivate |  Critical | canary |
| [HBASE-23038](https://issues.apache.org/jira/browse/HBASE-23038) | Provide consistent and clear logging about disabling chores |  Minor | master, regionserver |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23294](https://issues.apache.org/jira/browse/HBASE-23294) | ReplicationBarrierCleaner should delete all the barriers for a removed region which does not belong to any serial replication peer |  Major | master, Replication |
| [HBASE-23290](https://issues.apache.org/jira/browse/HBASE-23290) | shell processlist command is broken |  Major | shell |
| [HBASE-18439](https://issues.apache.org/jira/browse/HBASE-18439) | Subclasses of o.a.h.h.chaos.actions.Action all use the same logger |  Minor | integration tests |
| [HBASE-23262](https://issues.apache.org/jira/browse/HBASE-23262) | Cannot load Master UI |  Major | master, UI |
| [HBASE-22980](https://issues.apache.org/jira/browse/HBASE-22980) | HRegionPartioner getPartition() method incorrectly partitions the regions of the table. |  Major | mapreduce |
| [HBASE-21458](https://issues.apache.org/jira/browse/HBASE-21458) | Error: Could not find or load main class org.apache.hadoop.hbase.util.GetJavaProperty |  Minor | build, Client |
| [HBASE-23243](https://issues.apache.org/jira/browse/HBASE-23243) | [pv2] Filter out SUCCESS procedures; on decent-sized cluster, plethora overwhelms problems |  Major | proc-v2, UI |
| [HBASE-23247](https://issues.apache.org/jira/browse/HBASE-23247) | [hbck2] Schedule SCPs for 'Unknown Servers' |  Major | hbck2 |
| [HBASE-23241](https://issues.apache.org/jira/browse/HBASE-23241) | TestExecutorService sometimes fail |  Major | test |
| [HBASE-23244](https://issues.apache.org/jira/browse/HBASE-23244) | NPEs running Canary |  Major | canary |
| [HBASE-23231](https://issues.apache.org/jira/browse/HBASE-23231) | ReplicationSource do not update metrics after refresh |  Major | wal |
| [HBASE-23175](https://issues.apache.org/jira/browse/HBASE-23175) | Yarn unable to acquire delegation token for HBase Spark jobs |  Major | security, spark |
| [HBASE-22739](https://issues.apache.org/jira/browse/HBASE-22739) | ArrayIndexOutOfBoundsException when balance |  Major | Balancer |
| [HBASE-20827](https://issues.apache.org/jira/browse/HBASE-20827) | Add pause when retrying after CallQueueTooBigException for reportRegionStateTransition |  Major | Region Assignment |
| [HBASE-23171](https://issues.apache.org/jira/browse/HBASE-23171) | Ensure region state error messages include server making report (branch-2.1) |  Minor | amv2, Operability |
| [HBASE-23222](https://issues.apache.org/jira/browse/HBASE-23222) | Better logging and mitigation for MOB compaction failures |  Critical | mob |
| [HBASE-23181](https://issues.apache.org/jira/browse/HBASE-23181) | Blocked WAL archive: "LogRoller: Failed to schedule flush of XXXX, because it is not online on us" |  Major | regionserver, wal |
| [HBASE-23193](https://issues.apache.org/jira/browse/HBASE-23193) | ConnectionImplementation.isTableAvailable can not deal with meta table on branch-2.x |  Major | rsgroup, test |
| [HBASE-23177](https://issues.apache.org/jira/browse/HBASE-23177) | If fail to open reference because FNFE, make it plain it is a Reference |  Major | Operability |
| [HBASE-22370](https://issues.apache.org/jira/browse/HBASE-22370) | ByteBuf LEAK ERROR |  Major | rpc, wal |
| [HBASE-23155](https://issues.apache.org/jira/browse/HBASE-23155) | May NPE when concurrent AsyncNonMetaRegionLocator#updateCachedLocationOnError |  Major | asyncclient |
| [HBASE-21540](https://issues.apache.org/jira/browse/HBASE-21540) | when set property  "hbase.systemtables.compacting.memstore.type" to "basic" or "eager" will  cause an exception |  Major | conf |
| [HBASE-23153](https://issues.apache.org/jira/browse/HBASE-23153) | PrimaryRegionCountSkewCostFunction SLB function should implement CostFunction#isNeeded |  Major | . |
| [HBASE-23154](https://issues.apache.org/jira/browse/HBASE-23154) | list\_deadservers return incorrect no of rows |  Minor | shell |
| [HBASE-23115](https://issues.apache.org/jira/browse/HBASE-23115) | Unit change for StoreFileSize and MemStoreSize |  Minor | metrics, UI |
| [HBASE-23138](https://issues.apache.org/jira/browse/HBASE-23138) | Drop\_all table by regex fail from Shell -  Similar to HBASE-23134 |  Major | shell |
| [HBASE-23139](https://issues.apache.org/jira/browse/HBASE-23139) | MapReduce jobs lauched from convenience distribution are nonfunctional |  Blocker | mapreduce |
| [HBASE-23134](https://issues.apache.org/jira/browse/HBASE-23134) | Enable\_all and Disable\_all table by Regex fail from Shell |  Major | shell |
| [HBASE-22903](https://issues.apache.org/jira/browse/HBASE-22903) | alter\_status command is broken |  Major | metrics, shell |
| [HBASE-23094](https://issues.apache.org/jira/browse/HBASE-23094) | Wrong log message in simpleRegionNormaliser while checking if merge is enabled. |  Minor | . |
| [HBASE-23125](https://issues.apache.org/jira/browse/HBASE-23125) | TestRSGroupsAdmin2 is flaky |  Major | test |
| [HBASE-23119](https://issues.apache.org/jira/browse/HBASE-23119) | ArrayIndexOutOfBoundsException in PrivateCellUtil#qualifierStartsWith |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23301](https://issues.apache.org/jira/browse/HBASE-23301) | Generate CHANGES.md and RELEASENOTES.md for 2.1.8 |  Major | documentation |
| [HBASE-23300](https://issues.apache.org/jira/browse/HBASE-23300) | Set version as 2.1.8 in branch-2.1 in prep for first RC of 2.1.8 |  Major | build, pom |
| [HBASE-23136](https://issues.apache.org/jira/browse/HBASE-23136) | PartionedMobFileCompactor bulkloaded files shouldn't get replicated (addressing buklload replication related issue raised in HBASE-22380) |  Critical | . |
| [HBASE-23163](https://issues.apache.org/jira/browse/HBASE-23163) | Refactor HStore.getStorefilesSize related methods |  Major | regionserver |
| [HBASE-23131](https://issues.apache.org/jira/browse/HBASE-23131) | Set version as 2.1.8-SNAPSHOT in branch-2.1 |  Major | build, pom |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23236](https://issues.apache.org/jira/browse/HBASE-23236) | Upgrade to yetus 0.11.1 |  Major | build |
| [HBASE-23250](https://issues.apache.org/jira/browse/HBASE-23250) | Log message about CleanerChore delegate initialization should be at INFO |  Minor | master, Operability |
| [HBASE-23227](https://issues.apache.org/jira/browse/HBASE-23227) | Upgrade jackson-databind to 2.9.10.1 |  Blocker | dependencies, REST, security |
| [HBASE-23053](https://issues.apache.org/jira/browse/HBASE-23053) | Disable concurrent nightly builds |  Minor | build |


## Release 2.1.7 - Unreleased (as of 2019-10-02)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-11062](https://issues.apache.org/jira/browse/HBASE-11062) | hbtop |  Major | hbtop |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22930](https://issues.apache.org/jira/browse/HBASE-22930) | Set unique name to longCompactions/shortCompactions threads |  Minor | . |
| [HBASE-23035](https://issues.apache.org/jira/browse/HBASE-23035) | Retain region to the last RegionServer make the failover slower |  Major | . |
| [HBASE-23075](https://issues.apache.org/jira/browse/HBASE-23075) | Upgrade jackson to version 2.9.10 due to CVE-2019-16335 and CVE-2019-14540 |  Major | dependencies, hbase-connectors, REST, security |
| [HBASE-22975](https://issues.apache.org/jira/browse/HBASE-22975) | Add read and write QPS metrics at server level and table level |  Minor | metrics |
| [HBASE-23058](https://issues.apache.org/jira/browse/HBASE-23058) | Should be "Column Family Name" in table.jsp |  Minor | . |
| [HBASE-23049](https://issues.apache.org/jira/browse/HBASE-23049) | TableDescriptors#getAll should return the tables ordering by the name which contain namespace |  Minor | . |
| [HBASE-23041](https://issues.apache.org/jira/browse/HBASE-23041) | Should not show split parent regions in HBCK report's unknown server part |  Major | . |
| [HBASE-23044](https://issues.apache.org/jira/browse/HBASE-23044) | CatalogJanitor#cleanMergeQualifier may clean wrong parent regions |  Critical | . |
| [HBASE-23037](https://issues.apache.org/jira/browse/HBASE-23037) | Make the split WAL related log more readable |  Minor | . |
| [HBASE-22846](https://issues.apache.org/jira/browse/HBASE-22846) | Internal Error 500 when Using HBASE REST API to Create Namespace. |  Major | hbase-connectors |
| [HBASE-22804](https://issues.apache.org/jira/browse/HBASE-22804) | Provide an API to get list of successful regions and total expected regions in Canary |  Minor | canary |
| [HBASE-22701](https://issues.apache.org/jira/browse/HBASE-22701) | Better handle invalid local directory for DynamicClassLoader |  Major | regionserver |
| [HBASE-22724](https://issues.apache.org/jira/browse/HBASE-22724) | Add a emoji on the vote table for pre commit result on github |  Major | build, test |
| [HBASE-22954](https://issues.apache.org/jira/browse/HBASE-22954) | Whitelist net.java.dev.jna which got pulled in through Hadoop 3.3.0 |  Minor | community, hadoop3 |
| [HBASE-22905](https://issues.apache.org/jira/browse/HBASE-22905) | Avoid temp ByteBuffer allocation in BlockingRpcConnection#writeRequest |  Major | . |
| [HBASE-22962](https://issues.apache.org/jira/browse/HBASE-22962) | Fix typo in javadoc description |  Minor | documentation |
| [HBASE-22872](https://issues.apache.org/jira/browse/HBASE-22872) | Don't create normalization plan unnecesarily when split and merge both are disabled |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23054](https://issues.apache.org/jira/browse/HBASE-23054) | Remove synchronization block from MetaTableMetrics and fix LossyCounting algorithm |  Major | metrics |
| [HBASE-22380](https://issues.apache.org/jira/browse/HBASE-22380) | break circle replication when doing bulkload |  Critical | Replication |
| [HBASE-22965](https://issues.apache.org/jira/browse/HBASE-22965) | RS Crash due to DBE reference to an reused ByteBuff |  Major | . |
| [HBASE-22012](https://issues.apache.org/jira/browse/HBASE-22012) | SpaceQuota DisableTableViolationPolicy will cause cycles of enable/disable table |  Major | . |
| [HBASE-22944](https://issues.apache.org/jira/browse/HBASE-22944) | TableNotFoundException: hbase:quota  is thrown when region server is restarted. |  Minor | Quotas |
| [HBASE-22142](https://issues.apache.org/jira/browse/HBASE-22142) | Space quota: If table inside namespace having space quota is dropped, data size  usage is still considered for the drop table. |  Minor | . |
| [HBASE-22649](https://issues.apache.org/jira/browse/HBASE-22649) | Encode StoreFile path URLs in the UI to handle scenarios where CF contains special characters (like # etc.) |  Major | UI |
| [HBASE-22941](https://issues.apache.org/jira/browse/HBASE-22941) | MetaTableAccessor.getMergeRegions() returns parent regions in random order |  Major | . |
| [HBASE-23051](https://issues.apache.org/jira/browse/HBASE-23051) | Remove unneeded Mockito.mock invocations |  Major | test |
| [HBASE-23005](https://issues.apache.org/jira/browse/HBASE-23005) | Table UI showed exception message when table is disabled |  Minor | . |
| [HBASE-23040](https://issues.apache.org/jira/browse/HBASE-23040) | region mover gives NullPointerException instead of saying a host isn't in the cluster |  Minor | . |
| [HBASE-23043](https://issues.apache.org/jira/browse/HBASE-23043) | TestWALEntryStream times out |  Major | wal |
| [HBASE-22955](https://issues.apache.org/jira/browse/HBASE-22955) | Branches-1 precommit and nightly yetus jobs are using jdk8 for jdk7 jobs |  Major | . |
| [HBASE-22929](https://issues.apache.org/jira/browse/HBASE-22929) | MemStoreLAB  ChunkCreator may memory leak |  Major | . |
| [HBASE-23006](https://issues.apache.org/jira/browse/HBASE-23006) | RSGroupBasedLoadBalancer should also try to place replicas for the same region to different region servers |  Major | Region Assignment, rsgroup |
| [HBASE-23007](https://issues.apache.org/jira/browse/HBASE-23007) | UnsatisfiedLinkError when using hbase-shaded packages under linux |  Critical | shading |
| [HBASE-22013](https://issues.apache.org/jira/browse/HBASE-22013) | SpaceQuotas - getNumRegions() returning wrong number of regions due to region replicas |  Major | . |
| [HBASE-22979](https://issues.apache.org/jira/browse/HBASE-22979) | Call ChunkCreator.initialize in TestHRegionWithInMemoryFlush |  Critical | . |
| [HBASE-22963](https://issues.apache.org/jira/browse/HBASE-22963) | Netty ByteBuf leak in rpc client implementation |  Major | rpc |
| [HBASE-22981](https://issues.apache.org/jira/browse/HBASE-22981) | Remove unused flags for Yetus |  Critical | build |
| [HBASE-22970](https://issues.apache.org/jira/browse/HBASE-22970) | split parents show as overlaps in the HBCK Report |  Major | . |
| [HBASE-22961](https://issues.apache.org/jira/browse/HBASE-22961) | Deprecate hbck1 in core |  Major | hbck |
| [HBASE-22896](https://issues.apache.org/jira/browse/HBASE-22896) | TestHRegion.testFlushMarkersWALFail is flaky |  Minor | . |
| [HBASE-22943](https://issues.apache.org/jira/browse/HBASE-22943) | Various procedures should not cache log trace level |  Minor | proc-v2 |
| [HBASE-22881](https://issues.apache.org/jira/browse/HBASE-22881) | Fix non-daemon threads in hbase server implementation |  Major | master |
| [HBASE-22893](https://issues.apache.org/jira/browse/HBASE-22893) | Change the comment in HBaseClassTestRule to reflect change in default test timeouts |  Trivial | . |
| [HBASE-22928](https://issues.apache.org/jira/browse/HBASE-22928) | ScanMetrics counter update may not happen in case of exception in TableRecordReaderImpl |  Minor | mapreduce |
| [HBASE-22935](https://issues.apache.org/jira/browse/HBASE-22935) | TaskMonitor warns MonitoredRPCHandler task may be stuck when it recently started |  Minor | logging |
| [HBASE-22922](https://issues.apache.org/jira/browse/HBASE-22922) | Only the two first regions are locked in MergeTableRegionsProcedure |  Major | . |
| [HBASE-22852](https://issues.apache.org/jira/browse/HBASE-22852) | hbase nightlies leaking gpg-agents |  Minor | build |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22886](https://issues.apache.org/jira/browse/HBASE-22886) | Code Coverage Improvement: Create Unit Tests for ConnectionId |  Trivial | test |
| [HBASE-22766](https://issues.apache.org/jira/browse/HBASE-22766) | Code Coverage Improvement: Create Unit Tests for ResultStatsUtil |  Trivial | test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23108](https://issues.apache.org/jira/browse/HBASE-23108) | Generate CHANGES.md and RELEASENOTES.md for 2.1.7 |  Major | documentation |
| [HBASE-23097](https://issues.apache.org/jira/browse/HBASE-23097) | Set version as 2.1.7 in branch-2.1 in prep for first RC of 2.1.7 |  Major | build |
| [HBASE-22927](https://issues.apache.org/jira/browse/HBASE-22927) | Upgrade mockito version for Java 11 compatibility |  Major | . |
| [HBASE-22796](https://issues.apache.org/jira/browse/HBASE-22796) | [HBCK2] Add fix of overlaps to fixMeta hbck Service |  Major | . |
| [HBASE-22993](https://issues.apache.org/jira/browse/HBASE-22993) | HBCK report UI showed -1 if hbck chore not running |  Minor | . |
| [HBASE-23014](https://issues.apache.org/jira/browse/HBASE-23014) | Should not show split parent regions in hbck report UI |  Major | . |
| [HBASE-22859](https://issues.apache.org/jira/browse/HBASE-22859) | [HBCK2] Fix the orphan regions on filesystem |  Major | documentation, hbck2 |
| [HBASE-22960](https://issues.apache.org/jira/browse/HBASE-22960) | Set version as 2.1.7-SNAPSHOT in branch-2.1 |  Major | build, pom |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21745](https://issues.apache.org/jira/browse/HBASE-21745) | Make HBCK2 be able to fix issues other than region assignment |  Critical | hbase-operator-tools, hbck2 |
| [HBASE-23023](https://issues.apache.org/jira/browse/HBASE-23023) | upgrade shellcheck used to test in nightly and precommit |  Major | build |


## Release 2.1.6 - Unreleased (as of 2019-08-26)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22690](https://issues.apache.org/jira/browse/HBASE-22690) | Deprecate / Remove OfflineMetaRepair in hbase-2+ |  Major | hbck2 |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-15666](https://issues.apache.org/jira/browse/HBASE-15666) | shaded dependencies for hbase-testing-util |  Critical | test |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20509](https://issues.apache.org/jira/browse/HBASE-20509) | Put List in HashSet directly without using addAll function to improve performance |  Trivial | Performance |
| [HBASE-21996](https://issues.apache.org/jira/browse/HBASE-21996) | Set locale for javadoc |  Major | documentation |
| [HBASE-22464](https://issues.apache.org/jira/browse/HBASE-22464) | Improvements to hbase-vote script |  Trivial | scripts |
| [HBASE-22810](https://issues.apache.org/jira/browse/HBASE-22810) | Initialize an separate ThreadPoolExecutor for taking/restoring snapshot |  Major | . |
| [HBASE-22844](https://issues.apache.org/jira/browse/HBASE-22844) | Fix Checkstyle issues in client snapshot exceptions |  Minor | Client |
| [HBASE-22871](https://issues.apache.org/jira/browse/HBASE-22871) | Move the DirScanPool out and do not use static field |  Major | master |
| [HBASE-22841](https://issues.apache.org/jira/browse/HBASE-22841) | TimeRange's factory functions do not support ranges, only \`allTime\` and \`at\` |  Major | Client |
| [HBASE-22828](https://issues.apache.org/jira/browse/HBASE-22828) | Log a region close journal |  Minor | . |
| [HBASE-22812](https://issues.apache.org/jira/browse/HBASE-22812) | InterfaceAudience annotation in CatalogJanitor uses fully-qualified name |  Minor | . |
| [HBASE-22800](https://issues.apache.org/jira/browse/HBASE-22800) | Add mapreduce dependencies to hbase-shaded-testing-util |  Major | . |
| [HBASE-22731](https://issues.apache.org/jira/browse/HBASE-22731) | ReplicationSource and HBaseInterClusterReplicationEndpoint log messages should include a target Peer identifier |  Minor | Replication |
| [HBASE-22759](https://issues.apache.org/jira/browse/HBASE-22759) | Add user info to AUDITLOG events when doing grant/revoke |  Major | logging, security |
| [HBASE-22785](https://issues.apache.org/jira/browse/HBASE-22785) | Reduce number of Checkstyle issues in client exceptions |  Minor | Client |
| [HBASE-22786](https://issues.apache.org/jira/browse/HBASE-22786) | Fix Checkstyle issues in tests of hbase-client |  Minor | Client |
| [HBASE-22677](https://issues.apache.org/jira/browse/HBASE-22677) | Add unit tests for org.apache.hadoop.hbase.util.ByteRangeUtils and org.apache.hadoop.hbase.util.Classes |  Major | java, test |
| [HBASE-22787](https://issues.apache.org/jira/browse/HBASE-22787) | Clean up of tests in hbase-zookeeper |  Minor | Zookeeper |
| [HBASE-22363](https://issues.apache.org/jira/browse/HBASE-22363) | Remove hardcoded number of read cache block buckets |  Trivial | BlockCache, BucketCache |
| [HBASE-22764](https://issues.apache.org/jira/browse/HBASE-22764) | Fix remaining Checkstyle issues in hbase-rsgroup |  Trivial | rsgroup |
| [HBASE-22763](https://issues.apache.org/jira/browse/HBASE-22763) | Fix remaining Checkstyle issue in hbase-procedure |  Trivial | . |
| [HBASE-22743](https://issues.apache.org/jira/browse/HBASE-22743) | ClientUtils for hbase-examples |  Minor | . |
| [HBASE-22750](https://issues.apache.org/jira/browse/HBASE-22750) | Correct @throws in comment |  Trivial | Client, rpc |
| [HBASE-22702](https://issues.apache.org/jira/browse/HBASE-22702) | [Log] 'Group not found for table' is chatty |  Trivial | . |
| [HBASE-22721](https://issues.apache.org/jira/browse/HBASE-22721) | Refactor HBaseFsck: move the inner class out |  Major | . |
| [HBASE-22692](https://issues.apache.org/jira/browse/HBASE-22692) | Rubocop definition is not used in the /bin directory |  Minor | . |
| [HBASE-22610](https://issues.apache.org/jira/browse/HBASE-22610) | [BucketCache] Rename "hbase.offheapcache.minblocksize" |  Trivial | . |
| [HBASE-22704](https://issues.apache.org/jira/browse/HBASE-22704) | Avoid NPE when access table.jsp and snapshot.jsp but master not finish initialization |  Minor | . |
| [HBASE-22643](https://issues.apache.org/jira/browse/HBASE-22643) | Delete region without archiving only if regiondir is present |  Major | HFile |
| [HBASE-22689](https://issues.apache.org/jira/browse/HBASE-22689) | Line break for fix version in documentation |  Trivial | documentation |
| [HBASE-22638](https://issues.apache.org/jira/browse/HBASE-22638) | Zookeeper Utility enhancements |  Minor | Zookeeper |
| [HBASE-22669](https://issues.apache.org/jira/browse/HBASE-22669) | Add unit tests for org.apache.hadoop.hbase.util.Strings |  Major | java |
| [HBASE-22604](https://issues.apache.org/jira/browse/HBASE-22604) | fix the link in the docs to "Understanding HBase and BigTable" by Jim R. Wilson |  Trivial | documentation |
| [HBASE-22633](https://issues.apache.org/jira/browse/HBASE-22633) | Remove redundant call to substring for ZKReplicationQueueStorage |  Minor | . |
| [HBASE-22595](https://issues.apache.org/jira/browse/HBASE-22595) | Use full qualified name in Checkstyle suppressions |  Trivial | . |
| [HBASE-22616](https://issues.apache.org/jira/browse/HBASE-22616) | responseTooXXX logging for Multi should characterize the component ops |  Minor | . |
| [HBASE-22596](https://issues.apache.org/jira/browse/HBASE-22596) | [Chore] Separate the execution period between CompactionChecker and PeriodicMemStoreFlusher |  Minor | Compaction |
| [HBASE-22561](https://issues.apache.org/jira/browse/HBASE-22561) | modify HFilePrettyPrinter to accept non-hbase.rootdir directories |  Minor | . |
| [HBASE-22344](https://issues.apache.org/jira/browse/HBASE-22344) | Document deprecated public APIs |  Major | API, community, documentation |
| [HBASE-22593](https://issues.apache.org/jira/browse/HBASE-22593) | Add local Jenv file to gitignore |  Trivial | . |
| [HBASE-22160](https://issues.apache.org/jira/browse/HBASE-22160) | Add sorting functionality in regionserver web UI for user regions |  Minor | monitoring, regionserver, UI, Usability |
| [HBASE-22284](https://issues.apache.org/jira/browse/HBASE-22284) | optimization StringBuilder.append of AbstractMemStore.toString |  Trivial | . |
| [HBASE-22523](https://issues.apache.org/jira/browse/HBASE-22523) | Refactor RegionStates#getAssignmentsByTable to make it easy to understand |  Major | . |
| [HBASE-22511](https://issues.apache.org/jira/browse/HBASE-22511) | More missing /rs-status links |  Minor | UI |
| [HBASE-22496](https://issues.apache.org/jira/browse/HBASE-22496) | UnsafeAccess.unsafeCopy should not copy more than UNSAFE\_COPY\_THRESHOLD on each iteration |  Major | . |
| [HBASE-22488](https://issues.apache.org/jira/browse/HBASE-22488) | Cleanup the explicit timeout value for test methods |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22867](https://issues.apache.org/jira/browse/HBASE-22867) | The ForkJoinPool in CleanerChore will spawn thousands of threads in our cluster with thousands table |  Critical | master |
| [HBASE-22904](https://issues.apache.org/jira/browse/HBASE-22904) | NPE occurs when RS send space quota usage report during HMaster init |  Minor | . |
| [HBASE-22806](https://issues.apache.org/jira/browse/HBASE-22806) | Deleted CF are not cleared if memstore contain entries |  Major | API |
| [HBASE-22601](https://issues.apache.org/jira/browse/HBASE-22601) | Misconfigured addition of peers leads to cluster shutdown. |  Major | . |
| [HBASE-22863](https://issues.apache.org/jira/browse/HBASE-22863) | Avoid Jackson versions and dependencies with known CVEs |  Major | dependencies |
| [HBASE-22882](https://issues.apache.org/jira/browse/HBASE-22882) | TestFlushSnapshotFromClient#testConcurrentSnapshottingAttempts is flakey (was written flakey) |  Major | test |
| [HBASE-22870](https://issues.apache.org/jira/browse/HBASE-22870) | reflection fails to access a private nested class |  Major | master |
| [HBASE-22860](https://issues.apache.org/jira/browse/HBASE-22860) | Master's webui returns NPE/HTTP 500 under maintenance mode |  Major | master, UI |
| [HBASE-22856](https://issues.apache.org/jira/browse/HBASE-22856) | HBASE-Find-Flaky-Tests fails with pip error |  Major | build, test |
| [HBASE-22632](https://issues.apache.org/jira/browse/HBASE-22632) | SplitTableRegionProcedure and MergeTableRegionsProcedure should skip store files for unknown column families |  Major | proc-v2 |
| [HBASE-22838](https://issues.apache.org/jira/browse/HBASE-22838) | assembly:single failure: user id or group id 'xxxxx' is too big |  Major | build |
| [HBASE-22840](https://issues.apache.org/jira/browse/HBASE-22840) | Fix backport of HBASE-21325 |  Major | regionserver |
| [HBASE-22417](https://issues.apache.org/jira/browse/HBASE-22417) | DeleteTableProcedure.deleteFromMeta method should remove table from Master's table descriptors cache |  Major | . |
| [HBASE-22115](https://issues.apache.org/jira/browse/HBASE-22115) | HBase RPC aspires to grow an infinite tree of trace scopes; some other places are also unsafe |  Critical | rpc, tracing |
| [HBASE-22539](https://issues.apache.org/jira/browse/HBASE-22539) | WAL corruption due to early DBBs re-use when Durability.ASYNC\_WAL is used |  Blocker | rpc, wal |
| [HBASE-22801](https://issues.apache.org/jira/browse/HBASE-22801) | Maven build issue on Github PRs |  Major | build |
| [HBASE-22793](https://issues.apache.org/jira/browse/HBASE-22793) | RPC server connection is logging user as NULL principal |  Minor | rpc |
| [HBASE-22778](https://issues.apache.org/jira/browse/HBASE-22778) | Upgrade jasckson databind to 2.9.9.2 |  Blocker | dependencies |
| [HBASE-22773](https://issues.apache.org/jira/browse/HBASE-22773) | when set blockSize option in Performance Evaluation tool, error occurs:ERROR: Unrecognized option/command: --blockSize=131072 |  Minor | mapreduce |
| [HBASE-22735](https://issues.apache.org/jira/browse/HBASE-22735) | list\_regions may throw an error if a region is RIT |  Minor | shell |
| [HBASE-22145](https://issues.apache.org/jira/browse/HBASE-22145) | windows hbase-env causes hbase cli/etc to ignore HBASE\_OPTS |  Major | . |
| [HBASE-22758](https://issues.apache.org/jira/browse/HBASE-22758) | Remove the unneccesary info cf deletion in DeleteTableProcedure#deleteFromMeta |  Major | . |
| [HBASE-22751](https://issues.apache.org/jira/browse/HBASE-22751) | table.jsp fails if ugly regions in table |  Major | UI |
| [HBASE-22733](https://issues.apache.org/jira/browse/HBASE-22733) | TestSplitTransactionOnCluster.testMasterRestartAtRegionSplitPendingCatalogJanitor is flakey |  Major | . |
| [HBASE-22715](https://issues.apache.org/jira/browse/HBASE-22715) | All scan requests should be handled by scan handler threads in RWQueueRpcExecutor |  Minor | . |
| [HBASE-22722](https://issues.apache.org/jira/browse/HBASE-22722) | Upgrade jackson databind dependencies to 2.9.9.1 |  Blocker | dependencies |
| [HBASE-22603](https://issues.apache.org/jira/browse/HBASE-22603) | Javadoc Warnings related to @link tag |  Trivial | documentation |
| [HBASE-22720](https://issues.apache.org/jira/browse/HBASE-22720) | Incorrect link for hbase.unittests |  Trivial | documentation |
| [HBASE-22537](https://issues.apache.org/jira/browse/HBASE-22537) | Split happened Replica region can not be deleted after deleting table successfully and restarting RegionServer |  Minor | Region Assignment |
| [HBASE-20368](https://issues.apache.org/jira/browse/HBASE-20368) | Fix RIT stuck when a rsgroup has no online servers but AM's pendingAssginQueue is cleared |  Major | rsgroup |
| [HBASE-22700](https://issues.apache.org/jira/browse/HBASE-22700) | Incorrect timeout in recommended ZooKeeper configuration |  Minor | documentation |
| [HBASE-22661](https://issues.apache.org/jira/browse/HBASE-22661) | list\_regions command in hbase shell is broken |  Major | shell |
| [HBASE-22684](https://issues.apache.org/jira/browse/HBASE-22684) | The log rolling request maybe canceled immediately in LogRoller due to a race |  Major | wal |
| [HBASE-22586](https://issues.apache.org/jira/browse/HBASE-22586) | Javadoc Warnings related to @param tag |  Trivial | documentation |
| [HBASE-22571](https://issues.apache.org/jira/browse/HBASE-22571) | Javadoc Warnings related to @return tag |  Trivial | documentation |
| [HBASE-22681](https://issues.apache.org/jira/browse/HBASE-22681) | The 'assert highestUnsyncedTxid \< entry.getTxid();' in AbstractFWAL.append may fail when using AsyncFSWAL |  Critical | wal |
| [HBASE-22686](https://issues.apache.org/jira/browse/HBASE-22686) | ZkSplitLogWorkerCoordination doesn't allow a regionserver to pick up all of the split work it is capable of |  Major | . |
| [HBASE-22656](https://issues.apache.org/jira/browse/HBASE-22656) | [Metrics]  Tabe metrics 'BatchPut' and 'BatchDelete' are never updated |  Minor | metrics |
| [HBASE-22582](https://issues.apache.org/jira/browse/HBASE-22582) | The Compaction writer may access the lastCell whose memory has been released when appending fileInfo in the final |  Major | Compaction |
| [HBASE-22652](https://issues.apache.org/jira/browse/HBASE-22652) | Flakey TestLockManager; test timed out after 780 seconds |  Major | proc-v2 |
| [HBASE-22637](https://issues.apache.org/jira/browse/HBASE-22637) | fix flaky TestMetaTableMetrics test |  Major | metrics, test |
| [HBASE-13798](https://issues.apache.org/jira/browse/HBASE-13798) | TestFromClientSide\* don't close the Table |  Trivial | test |
| [HBASE-21751](https://issues.apache.org/jira/browse/HBASE-21751) | WAL creation fails during region open may cause region assign forever fail |  Major | . |
| [HBASE-22477](https://issues.apache.org/jira/browse/HBASE-22477) | Throwing exception when meta region is not in OPEN state in client registry may crash a master |  Major | Client, master, meta |
| [HBASE-22169](https://issues.apache.org/jira/browse/HBASE-22169) | Open region failed cause memory leak |  Critical | . |
| [HBASE-22617](https://issues.apache.org/jira/browse/HBASE-22617) | Recovered WAL directories not getting cleaned up |  Blocker | wal |
| [HBASE-22605](https://issues.apache.org/jira/browse/HBASE-22605) | Ref guide includes dev guidance only applicable to EOM versions |  Trivial | documentation |
| [HBASE-22581](https://issues.apache.org/jira/browse/HBASE-22581) | user with "CREATE" permission can grant, but not revoke permissions on created table |  Major | security |
| [HBASE-22565](https://issues.apache.org/jira/browse/HBASE-22565) | Javadoc Warnings: @see cannot be used in inline documentation |  Trivial | documentation |
| [HBASE-22562](https://issues.apache.org/jira/browse/HBASE-22562) | PressureAwareThroughputController#skipControl never invoked |  Trivial | Operability |
| [HBASE-22530](https://issues.apache.org/jira/browse/HBASE-22530) | The metrics of store files count of region are returned to clients incorrectly |  Minor | metrics, regionserver |
| [HBASE-22458](https://issues.apache.org/jira/browse/HBASE-22458) | TestClassFinder fails when run on JDK11 |  Minor | java, test |
| [HBASE-22563](https://issues.apache.org/jira/browse/HBASE-22563) | Reduce retained jobs for Jenkins pipelines |  Major | . |
| [HBASE-22551](https://issues.apache.org/jira/browse/HBASE-22551) | TestMasterOperationsForRegionReplicas is flakey |  Major | read replicas, test |
| [HBASE-22481](https://issues.apache.org/jira/browse/HBASE-22481) | Javadoc Warnings: reference not found |  Trivial | documentation |
| [HBASE-22520](https://issues.apache.org/jira/browse/HBASE-22520) | Avoid possible NPE in HalfStoreFileReader seekBefore() |  Major | . |
| [HBASE-22546](https://issues.apache.org/jira/browse/HBASE-22546) | TestRegionServerHostname#testRegionServerHostname fails reliably for me |  Major | . |
| [HBASE-22534](https://issues.apache.org/jira/browse/HBASE-22534) | TestCellUtil fails when run on JDK11 |  Minor | java, test |
| [HBASE-22536](https://issues.apache.org/jira/browse/HBASE-22536) | TestForeignExceptionSerialization fails when run on JDK11 |  Minor | java |
| [HBASE-22535](https://issues.apache.org/jira/browse/HBASE-22535) | TestShellRSGroups fails when run on JDK11 |  Minor | java, shell |
| [HBASE-22518](https://issues.apache.org/jira/browse/HBASE-22518) | yetus personality is treating branch-1.4 like earlier branches for hadoopcheck |  Major | test |
| [HBASE-22522](https://issues.apache.org/jira/browse/HBASE-22522) | The integration test in master branch's nightly job has error "ERROR: Only found 1050 rows." |  Major | . |
| [HBASE-22490](https://issues.apache.org/jira/browse/HBASE-22490) | Nightly client integration test fails with hadoop-3 |  Major | build |
| [HBASE-22487](https://issues.apache.org/jira/browse/HBASE-22487) | getMostLoadedRegions is unused |  Trivial | regionserver |
| [HBASE-22486](https://issues.apache.org/jira/browse/HBASE-22486) | Fix flaky test TestLockManager |  Major | . |
| [HBASE-22471](https://issues.apache.org/jira/browse/HBASE-22471) | Our nightly jobs for master and branch-2 are still using hadoop-2.7.1 in integration test |  Major | build |
| [HBASE-19893](https://issues.apache.org/jira/browse/HBASE-19893) | restore\_snapshot is broken in master branch when region splits |  Critical | snapshots |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22894](https://issues.apache.org/jira/browse/HBASE-22894) | Move testOpenRegionFailedMemoryLeak to dedicated class |  Major | test |
| [HBASE-22725](https://issues.apache.org/jira/browse/HBASE-22725) | Remove all remaining javadoc warnings |  Trivial | test |
| [HBASE-22615](https://issues.apache.org/jira/browse/HBASE-22615) | Make TestChoreService more robust to timing |  Minor | test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22858](https://issues.apache.org/jira/browse/HBASE-22858) | Add HBCK Report to master's header.jsp |  Minor | master |
| [HBASE-22891](https://issues.apache.org/jira/browse/HBASE-22891) | Use HBaseQA in HBase-PreCommit-GitHub-PR job |  Major | build, scripts |
| [HBASE-22771](https://issues.apache.org/jira/browse/HBASE-22771) | [HBCK2] fixMeta method and server-side support |  Major | hbck2 |
| [HBASE-22848](https://issues.apache.org/jira/browse/HBASE-22848) | Set version as 2.1.6 in branch-2.1 in prep for first RC of 2.1.6 |  Major | build |
| [HBASE-22799](https://issues.apache.org/jira/browse/HBASE-22799) | Generate CHANGES.md and RELEASENOTES.md for 2.1.6 |  Major | documentation |
| [HBASE-22845](https://issues.apache.org/jira/browse/HBASE-22845) | Revert MetaTableAccessor#makePutFromTableState access to public |  Blocker | . |
| [HBASE-22777](https://issues.apache.org/jira/browse/HBASE-22777) | Add a multi-region merge (for fixing overlaps, etc.) |  Major | hbck2, proc-v2 |
| [HBASE-22803](https://issues.apache.org/jira/browse/HBASE-22803) | Modify config value range to enable turning off of the hbck chore |  Major | . |
| [HBASE-22824](https://issues.apache.org/jira/browse/HBASE-22824) | Show filesystem path for the orphans regions on filesystem |  Major | . |
| [HBASE-22808](https://issues.apache.org/jira/browse/HBASE-22808) | HBCK Report showed the offline regions which belong to disabled table |  Major | . |
| [HBASE-22807](https://issues.apache.org/jira/browse/HBASE-22807) | HBCK Report showed wrong orphans regions on FileSystem |  Major | . |
| [HBASE-22805](https://issues.apache.org/jira/browse/HBASE-22805) | Fix broken unit test, TestCatalogJanitorCluster on branch-2.1 and branch-2.0 |  Major | . |
| [HBASE-22737](https://issues.apache.org/jira/browse/HBASE-22737) | Add a new admin method and shell cmd to trigger the hbck chore to run |  Major | . |
| [HBASE-22741](https://issues.apache.org/jira/browse/HBASE-22741) | Show catalogjanitor consistency complaints in new 'HBCK Report' page |  Major | hbck2, UI |
| [HBASE-22723](https://issues.apache.org/jira/browse/HBASE-22723) | Have CatalogJanitor report holes and overlaps; i.e. problems it sees when doing its regular scan of hbase:meta |  Major | . |
| [HBASE-22709](https://issues.apache.org/jira/browse/HBASE-22709) | Add a chore thread in master to do hbck checking and display results in 'HBCK Report' page |  Major | . |
| [HBASE-22742](https://issues.apache.org/jira/browse/HBASE-22742) | [HBCK2] Add more log for hbck operations at master side |  Minor | . |
| [HBASE-22527](https://issues.apache.org/jira/browse/HBASE-22527) | [hbck2] Add a master web ui to show the problematic regions |  Major | hbase-operator-tools, hbck2 |
| [HBASE-22719](https://issues.apache.org/jira/browse/HBASE-22719) | Add debug support for github PR pre commit job |  Major | build |
| [HBASE-7191](https://issues.apache.org/jira/browse/HBASE-7191) | HBCK - Add offline create/fix hbase.version and hbase.id |  Major | hbck |
| [HBASE-22357](https://issues.apache.org/jira/browse/HBASE-22357) | Fix remaining Checkstyle issues in hbase-replication |  Trivial | Replication |
| [HBASE-22554](https://issues.apache.org/jira/browse/HBASE-22554) | Upgrade to surefire 2.22.2 |  Major | test |
| [HBASE-22507](https://issues.apache.org/jira/browse/HBASE-22507) | Backport the pre commit changes in HBASE-22500 to all active branches |  Major | build |
| [HBASE-22316](https://issues.apache.org/jira/browse/HBASE-22316) | Record the stack trace for current thread in FutureUtils.get |  Major | asyncclient, Client |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22833](https://issues.apache.org/jira/browse/HBASE-22833) | MultiRowRangeFilter should provide a method for creating a filter which is functionally equivalent to multiple prefix filters |  Minor | Client |
| [HBASE-22895](https://issues.apache.org/jira/browse/HBASE-22895) | Fix the flakey TestSpaceQuotas |  Major | Quotas, test |
| [HBASE-22910](https://issues.apache.org/jira/browse/HBASE-22910) | Enable TestMultiVersionConcurrencyControl |  Major | test |
| [HBASE-22913](https://issues.apache.org/jira/browse/HBASE-22913) | Use Hadoop label for nightly builds |  Major | build |
| [HBASE-22911](https://issues.apache.org/jira/browse/HBASE-22911) | fewer concurrent github PR builds |  Critical | build |
| [HBASE-21400](https://issues.apache.org/jira/browse/HBASE-21400) | correct spelling error of 'initilize' in comment |  Trivial | documentation |
| [HBASE-22382](https://issues.apache.org/jira/browse/HBASE-22382) | Refactor tests in TestFromClientSide |  Major | test |
| [HBASE-21606](https://issues.apache.org/jira/browse/HBASE-21606) | Document use of the meta table load metrics added in HBASE-19722 |  Critical | documentation, meta, metrics, Operability |
| [HBASE-19230](https://issues.apache.org/jira/browse/HBASE-19230) | Write up fixVersion policy from dev discussion in refguide |  Major | documentation |
| [HBASE-22566](https://issues.apache.org/jira/browse/HBASE-22566) | Call out default compaction throttling for 2.x in Book |  Major | documentation |
| [HBASE-22568](https://issues.apache.org/jira/browse/HBASE-22568) | [2.1] Upgrade to Jetty 9.3.latest and Jackson 2.9.latest |  Major | dependencies |
| [HBASE-21536](https://issues.apache.org/jira/browse/HBASE-21536) | Fix completebulkload usage instructions |  Trivial | documentation, mapreduce |


## Release 2.1.5 - Unreleased (as of 2019-05-28)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21991](https://issues.apache.org/jira/browse/HBASE-21991) | Fix MetaMetrics issues - [Race condition, Faulty remove logic], few improvements |  Major | Coprocessors, metrics |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22148](https://issues.apache.org/jira/browse/HBASE-22148) | Provide an alternative to CellUtil.setTimestamp |  Blocker | API, Coprocessors |
| [HBASE-21815](https://issues.apache.org/jira/browse/HBASE-21815) | Make isTrackingMetrics and getMetrics of ScannerContext public |  Minor | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22467](https://issues.apache.org/jira/browse/HBASE-22467) | WebUI changes to enable Apache Knox UI proxying |  Major | UI |
| [HBASE-22474](https://issues.apache.org/jira/browse/HBASE-22474) | Add --mvn-custom-repo parameter to yetus calls |  Minor | . |
| [HBASE-20305](https://issues.apache.org/jira/browse/HBASE-20305) | Add option to SyncTable that skip deletes on target cluster |  Minor | mapreduce |
| [HBASE-21784](https://issues.apache.org/jira/browse/HBASE-21784) | Dump replication queue should show list of wal files ordered chronologically |  Major | Replication, tooling |
| [HBASE-22384](https://issues.apache.org/jira/browse/HBASE-22384) | Formatting issues in administration section of book |  Minor | community, documentation |
| [HBASE-21658](https://issues.apache.org/jira/browse/HBASE-21658) | Should get the meta replica number from zk instead of config at client side |  Critical | Client |
| [HBASE-22392](https://issues.apache.org/jira/browse/HBASE-22392) | Remove extra/useless + |  Trivial | . |
| [HBASE-20494](https://issues.apache.org/jira/browse/HBASE-20494) | Upgrade com.yammer.metrics dependency |  Major | dependencies |
| [HBASE-22358](https://issues.apache.org/jira/browse/HBASE-22358) | Change rubocop configuration for method length |  Minor | community, shell |
| [HBASE-22379](https://issues.apache.org/jira/browse/HBASE-22379) | Fix Markdown for "Voting on Release Candidates" in book |  Minor | community, documentation |
| [HBASE-22109](https://issues.apache.org/jira/browse/HBASE-22109) | Update hbase shaded content checker after guava update in hadoop branch-3.0 to 27.0-jre |  Minor | . |
| [HBASE-22087](https://issues.apache.org/jira/browse/HBASE-22087) | Update LICENSE/shading for the dependencies from the latest Hadoop trunk |  Minor | hadoop3 |
| [HBASE-22341](https://issues.apache.org/jira/browse/HBASE-22341) | Add explicit guidelines for removing deprecations in book |  Major | API, community, documentation |
| [HBASE-22225](https://issues.apache.org/jira/browse/HBASE-22225) | Profiler tab on Master/RS UI not working w/o comprehensive message |  Minor | UI |
| [HBASE-22291](https://issues.apache.org/jira/browse/HBASE-22291) | Fix recovery of recovered.edits files under root dir |  Major | . |
| [HBASE-20586](https://issues.apache.org/jira/browse/HBASE-20586) | SyncTable tool: Add support for cross-realm remote clusters |  Major | mapreduce, Operability, Replication |
| [HBASE-21257](https://issues.apache.org/jira/browse/HBASE-21257) | misspelled words.[occured -\> occurred] |  Trivial | . |
| [HBASE-22188](https://issues.apache.org/jira/browse/HBASE-22188) | Make TestSplitMerge more stable |  Major | test |
| [HBASE-22097](https://issues.apache.org/jira/browse/HBASE-22097) | Modify the description of split command in shell |  Trivial | shell |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22003](https://issues.apache.org/jira/browse/HBASE-22003) | Fix flaky test TestVerifyReplication.testHBase14905 |  Major | . |
| [HBASE-22441](https://issues.apache.org/jira/browse/HBASE-22441) | BucketCache NullPointerException in cacheBlock |  Major | BucketCache |
| [HBASE-22462](https://issues.apache.org/jira/browse/HBASE-22462) | Should run a 'mvn install' at the end of hadoop check in pre commit job |  Major | build |
| [HBASE-22440](https://issues.apache.org/jira/browse/HBASE-22440) | HRegionServer#getWalGroupsReplicationStatus() throws NPE |  Major | regionserver, Replication |
| [HBASE-22289](https://issues.apache.org/jira/browse/HBASE-22289) | WAL-based log splitting resubmit threshold may result in a task being stuck forever |  Major | . |
| [HBASE-22226](https://issues.apache.org/jira/browse/HBASE-22226) | Incorrect level for headings in asciidoc |  Trivial | documentation |
| [HBASE-22442](https://issues.apache.org/jira/browse/HBASE-22442) | Nightly build is failing with hadoop 3.x |  Major | build, hadoop3 |
| [HBASE-20970](https://issues.apache.org/jira/browse/HBASE-20970) | Update hadoop check versions for hadoop3 in hbase-personality |  Major | build |
| [HBASE-22274](https://issues.apache.org/jira/browse/HBASE-22274) | Cell size limit check on append should consider cell's previous size. |  Minor | . |
| [HBASE-22072](https://issues.apache.org/jira/browse/HBASE-22072) | High read/write intensive regions may cause long crash recovery |  Major | Performance, Recovery |
| [HBASE-20851](https://issues.apache.org/jira/browse/HBASE-20851) | Change rubocop config for max line length of 100 |  Minor | community, shell |
| [HBASE-21467](https://issues.apache.org/jira/browse/HBASE-21467) | Fix flaky test TestCoprocessorClassLoader.testCleanupOldJars |  Minor | . |
| [HBASE-22312](https://issues.apache.org/jira/browse/HBASE-22312) | Hadoop 3 profile for hbase-shaded-mapreduce should like mapreduce as a provided dependency |  Major | mapreduce, shading |
| [HBASE-22314](https://issues.apache.org/jira/browse/HBASE-22314) | shaded byo-hadoop client should list needed hadoop modules as provided scope to avoid inclusion of unnecessary transitive depednencies |  Major | hadoop2, hadoop3, shading |
| [HBASE-22047](https://issues.apache.org/jira/browse/HBASE-22047) | LeaseException in Scan should be retired |  Major | Client, Scanners |
| [HBASE-22190](https://issues.apache.org/jira/browse/HBASE-22190) | SnapshotFileCache may fail to load the correct snapshot file list when there is an on-going snapshot operation |  Blocker | snapshots |
| [HBASE-22354](https://issues.apache.org/jira/browse/HBASE-22354) | master never sets abortRequested, and thus abort timeout doesn't work for it |  Major | . |
| [HBASE-22350](https://issues.apache.org/jira/browse/HBASE-22350) | Rewrite TestClientOperationTimeout so we do not timeout when creating table |  Major | test |
| [HBASE-22340](https://issues.apache.org/jira/browse/HBASE-22340) | Corrupt KeyValue is silently ignored |  Critical | wal |
| [HBASE-22054](https://issues.apache.org/jira/browse/HBASE-22054) | Space Quota: Compaction is not working for super user in case of NO\_WRITES\_COMPACTIONS |  Minor | . |
| [HBASE-22236](https://issues.apache.org/jira/browse/HBASE-22236) | AsyncNonMetaRegionLocator should not cache HRegionLocation with null location |  Major | asyncclient |
| [HBASE-22086](https://issues.apache.org/jira/browse/HBASE-22086) | space quota issue: deleting snapshot doesn't update the usage of table |  Minor | . |
| [HBASE-22298](https://issues.apache.org/jira/browse/HBASE-22298) | branch-2.2 nightly fails "[ForOverride] Method annotated @ForOverride must have protected or package-private visibility" |  Major | . |
| [HBASE-22292](https://issues.apache.org/jira/browse/HBASE-22292) | PreemptiveFastFailInterceptor clean repeatedFailuresMap issue |  Blocker | . |
| [HBASE-22230](https://issues.apache.org/jira/browse/HBASE-22230) | REST Server drops connection on long scans |  Major | . |
| [HBASE-22200](https://issues.apache.org/jira/browse/HBASE-22200) | WALSplitter.hasRecoveredEdits should use same FS instance from WAL region dir |  Major | wal |
| [HBASE-22286](https://issues.apache.org/jira/browse/HBASE-22286) | License handling incorrectly lists CDDL/GPLv2+CE as safe to not aggregate |  Critical | build, community |
| [HBASE-22282](https://issues.apache.org/jira/browse/HBASE-22282) | Should deal with error in the callback of RawAsyncHBaseAdmin.splitRegion methods |  Major | Admin, asyncclient |
| [HBASE-22278](https://issues.apache.org/jira/browse/HBASE-22278) | RawAsyncHBaseAdmin should not use cached region location |  Major | Admin, asyncclient |
| [HBASE-22249](https://issues.apache.org/jira/browse/HBASE-22249) | Rest Server throws NoClassDefFoundError with Java 11 (run-time) |  Major | . |
| [HBASE-22235](https://issues.apache.org/jira/browse/HBASE-22235) | OperationStatus.{SUCCESS\|FAILURE\|NOT\_RUN} are not visible to 3rd party coprocessors |  Major | Coprocessors |
| [HBASE-22144](https://issues.apache.org/jira/browse/HBASE-22144) | MultiRowRangeFilter does not work with reversed scans |  Critical | Filters, Scanners |
| [HBASE-22214](https://issues.apache.org/jira/browse/HBASE-22214) | [2.x] Backport missing filter improvements |  Major | Filters |
| [HBASE-22198](https://issues.apache.org/jira/browse/HBASE-22198) | Fix flakey TestAsyncTableGetMultiThreaded |  Major | test |
| [HBASE-22185](https://issues.apache.org/jira/browse/HBASE-22185) | RAMQueueEntry#writeToCache should freeBlock if any exception encountered instead of the IOException catch block |  Major | . |
| [HBASE-22128](https://issues.apache.org/jira/browse/HBASE-22128) | Move namespace region then master crashed make deadlock |  Critical | amv2 |
| [HBASE-22180](https://issues.apache.org/jira/browse/HBASE-22180) | Make TestBlockEvictionFromClient.testBlockRefCountAfterSplits more stable |  Major | test |
| [HBASE-22179](https://issues.apache.org/jira/browse/HBASE-22179) | Fix RawAsyncHBaseAdmin.getCompactionState |  Major | Admin, asyncclient |
| [HBASE-22177](https://issues.apache.org/jira/browse/HBASE-22177) | Do not recreate IOException in RawAsyncHBaseAdmin.adminCall |  Major | Admin, asyncclient |
| [HBASE-22070](https://issues.apache.org/jira/browse/HBASE-22070) | Checking restoreDir in RestoreSnapshotHelper |  Minor | snapshots |
| [HBASE-20912](https://issues.apache.org/jira/browse/HBASE-20912) | Add import order config in dev support for eclipse |  Major | . |
| [HBASE-21688](https://issues.apache.org/jira/browse/HBASE-21688) | Address WAL filesystem issues |  Major | Filesystem Integration, wal |
| [HBASE-22073](https://issues.apache.org/jira/browse/HBASE-22073) | /rits.jsp throws an exception if no procedure |  Major | UI |
| [HBASE-22123](https://issues.apache.org/jira/browse/HBASE-22123) | REST gateway reports Insufficient permissions exceptions as 404 Not Found |  Minor | REST |
| [HBASE-21135](https://issues.apache.org/jira/browse/HBASE-21135) | Build fails on windows as it fails to parse windows path during license check |  Major | build |
| [HBASE-21781](https://issues.apache.org/jira/browse/HBASE-21781) | list\_deadservers elapsed time is incorrect |  Major | shell |
| [HBASE-22100](https://issues.apache.org/jira/browse/HBASE-22100) | False positive for error prone warnings in pre commit job |  Minor | build |
| [HBASE-22098](https://issues.apache.org/jira/browse/HBASE-22098) | Backport HBASE-18667 "Disable error-prone for hbase-protocol-shaded" to branch-2 |  Major | build |
| [HBASE-20662](https://issues.apache.org/jira/browse/HBASE-20662) | Increasing space quota on a violated table does not remove SpaceViolationPolicy.DISABLE enforcement |  Major | . |
| [HBASE-21619](https://issues.apache.org/jira/browse/HBASE-21619) | Fix warning message caused by incorrect ternary operator evaluation |  Trivial | . |
| [HBASE-22045](https://issues.apache.org/jira/browse/HBASE-22045) | Mutable range histogram reports incorrect outliers |  Major | . |
| [HBASE-21736](https://issues.apache.org/jira/browse/HBASE-21736) | Remove the server from online servers before scheduling SCP for it in hbck |  Major | hbck2, test |
| [HBASE-22006](https://issues.apache.org/jira/browse/HBASE-22006) | Fix branch-2.1 findbugs warning; causes nightly show as failed. |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21963](https://issues.apache.org/jira/browse/HBASE-21963) | Add a script for building and verifying release candidate |  Minor | community, scripts |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22419](https://issues.apache.org/jira/browse/HBASE-22419) | Backport hbase-personality changes in HBASE-22399 and HBASE-20970 to all active branches |  Major | build |
| [HBASE-22445](https://issues.apache.org/jira/browse/HBASE-22445) | Add file info when throw exceptions in HFileReaderImpl |  Major | . |
| [HBASE-22447](https://issues.apache.org/jira/browse/HBASE-22447) | Check refCount before free block in BucketCache |  Major | BucketCache |
| [HBASE-22325](https://issues.apache.org/jira/browse/HBASE-22325) | AsyncRpcRetryingCaller will not schedule retry if we hit a NotServingRegionException but there is no TableName provided |  Major | asyncclient, Client |
| [HBASE-22152](https://issues.apache.org/jira/browse/HBASE-22152) | Create a jenkins file for yetus to processing GitHub PR |  Major | build |
| [HBASE-22135](https://issues.apache.org/jira/browse/HBASE-22135) | AsyncAdmin will not refresh master address |  Major | Admin, asyncclient, Client |
| [HBASE-22101](https://issues.apache.org/jira/browse/HBASE-22101) | AsyncAdmin.isTableAvailable should not throw TableNotFoundException |  Major | Admin, asyncclient, Client |
| [HBASE-22094](https://issues.apache.org/jira/browse/HBASE-22094) | Throw TableNotFoundException if table not exists in AsyncAdmin.compact |  Major | Admin |
| [HBASE-21998](https://issues.apache.org/jira/browse/HBASE-21998) | Move branch-2.1 version from 2.1.4 to 2.1.5-SNAPSHOT |  Major | community |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22449](https://issues.apache.org/jira/browse/HBASE-22449) | https everywhere in Maven metadata |  Minor | . |
| [HBASE-22406](https://issues.apache.org/jira/browse/HBASE-22406) | skip generating rdoc when building gems in our docker image for running yetus |  Critical | build, test |
| [HBASE-22375](https://issues.apache.org/jira/browse/HBASE-22375) | Promote AccessChecker to LimitedPrivate(Coprocessor) |  Minor | Coprocessors, security |
| [HBASE-22359](https://issues.apache.org/jira/browse/HBASE-22359) | Backport of HBASE-21371 misses activation-api license information |  Minor | build, community |
| [HBASE-22174](https://issues.apache.org/jira/browse/HBASE-22174) | Remove error prone from our precommit javac check |  Major | build |
| [HBASE-22231](https://issues.apache.org/jira/browse/HBASE-22231) | Remove unused and \* imports |  Minor | . |
| [HBASE-22304](https://issues.apache.org/jira/browse/HBASE-22304) | Fix remaining Checkstyle issues in hbase-endpoint |  Trivial | . |
| [HBASE-22020](https://issues.apache.org/jira/browse/HBASE-22020) | upgrade to yetus 0.9.0 |  Major | build, community |
| [HBASE-22192](https://issues.apache.org/jira/browse/HBASE-22192) | Delete misc 2.0.5 pom files from branch-2.1 |  Minor | community |
| [HBASE-22203](https://issues.apache.org/jira/browse/HBASE-22203) | Reformat DemoClient.java |  Trivial | . |
| [HBASE-22189](https://issues.apache.org/jira/browse/HBASE-22189) | Remove usage of StoreFile.getModificationTimeStamp |  Trivial | . |
| [HBASE-22131](https://issues.apache.org/jira/browse/HBASE-22131) | Delete the patches in hbase-protocol-shaded module |  Major | build, Protobufs |
| [HBASE-22099](https://issues.apache.org/jira/browse/HBASE-22099) | Backport HBASE-21895 "Error prone upgrade" to branch-2 |  Major | build |
| [HBASE-22042](https://issues.apache.org/jira/browse/HBASE-22042) | Missing @Override annotation for RawAsyncTableImpl.scan |  Major | asyncclient, Client |


## Release 2.1.4 - Unreleased (as of 2019-03-24)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21926](https://issues.apache.org/jira/browse/HBASE-21926) | Profiler servlet |  Major | master, Operability, regionserver |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22032](https://issues.apache.org/jira/browse/HBASE-22032) | KeyValue validation should check for null byte array |  Major | . |
| [HBASE-21667](https://issues.apache.org/jira/browse/HBASE-21667) | Move to latest ASF Parent POM |  Minor | build |
| [HBASE-21871](https://issues.apache.org/jira/browse/HBASE-21871) | Support to specify a peer table name in VerifyReplication tool |  Major | . |
| [HBASE-21867](https://issues.apache.org/jira/browse/HBASE-21867) | Support multi-threads in HFileArchiver |  Major | . |
| [HBASE-21932](https://issues.apache.org/jira/browse/HBASE-21932) | Use Runtime.getRuntime().halt to terminate regionserver when abort timeout |  Major | . |
| [HBASE-21636](https://issues.apache.org/jira/browse/HBASE-21636) | Enhance the shell scan command to support missing scanner specifications like ReadType, IsolationLevel etc. |  Major | shell |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21990](https://issues.apache.org/jira/browse/HBASE-21990) | puppycrawl checkstyle dtds 404... moved to sourceforge |  Major | build |
| [HBASE-21960](https://issues.apache.org/jira/browse/HBASE-21960) | RESTServletContainer not configured for REST Jetty server |  Blocker | REST |
| [HBASE-21915](https://issues.apache.org/jira/browse/HBASE-21915) | FileLink$FileLinkInputStream doesn't implement CanUnbuffer |  Major | Filesystem Integration |
| [HBASE-21983](https://issues.apache.org/jira/browse/HBASE-21983) | Should track the scan metrics in AsyncScanSingleRegionRpcRetryingCaller if scan metrics is enabled |  Major | asyncclient, Client |
| [HBASE-21980](https://issues.apache.org/jira/browse/HBASE-21980) | Fix typo in AbstractTestAsyncTableRegionReplicasRead |  Major | test |
| [HBASE-21961](https://issues.apache.org/jira/browse/HBASE-21961) | Infinite loop in AsyncNonMetaRegionLocator if there is only one region and we tried to locate before a non empty row |  Critical | asyncclient, Client |
| [HBASE-21943](https://issues.apache.org/jira/browse/HBASE-21943) | The usage of RegionLocations.mergeRegionLocations is wrong for async client |  Critical | asyncclient, Client |
| [HBASE-21942](https://issues.apache.org/jira/browse/HBASE-21942) | [UI] requests per second is incorrect in rsgroup page(rsgroup.jsp) |  Minor | . |
| [HBASE-21929](https://issues.apache.org/jira/browse/HBASE-21929) | The checks at the end of TestRpcClientLeaks are not executed |  Major | test |
| [HBASE-21899](https://issues.apache.org/jira/browse/HBASE-21899) | Fix missing variables in slf4j Logger |  Trivial | logging |
| [HBASE-21910](https://issues.apache.org/jira/browse/HBASE-21910) | The nonce implementation is wrong for AsyncTable |  Critical | asyncclient, Client |
| [HBASE-21900](https://issues.apache.org/jira/browse/HBASE-21900) | Infinite loop in AsyncMetaRegionLocator if we can not get the location for meta |  Major | asyncclient, Client |
| [HBASE-21890](https://issues.apache.org/jira/browse/HBASE-21890) | Use execute instead of submit to submit a task in RemoteProcedureDispatcher |  Critical | proc-v2 |
| [HBASE-21889](https://issues.apache.org/jira/browse/HBASE-21889) | Use thrift 0.12.0 when build thrift by compile-thrift profile |  Major | . |
| [HBASE-21854](https://issues.apache.org/jira/browse/HBASE-21854) | Race condition in TestProcedureSkipPersistence |  Minor | proc-v2 |
| [HBASE-18484](https://issues.apache.org/jira/browse/HBASE-18484) | VerifyRep by snapshot  does not work when Yarn / SourceHBase / PeerHBase located in different HDFS clusters |  Major | Replication |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22066](https://issues.apache.org/jira/browse/HBASE-22066) | Add markers to CHANGES.md and RELEASENOTES.md |  Major | . |
| [HBASE-22059](https://issues.apache.org/jira/browse/HBASE-22059) | 2.1 addendum; (check-jar-contents) @ hbase-shaded-check-invariants fails |  Major | . |
| [HBASE-22022](https://issues.apache.org/jira/browse/HBASE-22022) | nightly fails rat check down in the dev-support/hbase\_nightly\_source-artifact.sh check |  Major | . |
| [HBASE-22025](https://issues.apache.org/jira/browse/HBASE-22025) | RAT check fails in nightlies; fails on (old) test data files. |  Major | . |
| [HBASE-21999](https://issues.apache.org/jira/browse/HBASE-21999) | [DEBUG] Exit if git returns empty revision! |  Major | build |
| [HBASE-21997](https://issues.apache.org/jira/browse/HBASE-21997) | Fix hbase-rest findbugs ST\_WRITE\_TO\_STATIC\_FROM\_INSTANCE\_METHOD complaint |  Major | REST |
| [HBASE-21985](https://issues.apache.org/jira/browse/HBASE-21985) | Set version as 2.1.4 in branch-2.1 in prep for first RC |  Major | build, release |
| [HBASE-21984](https://issues.apache.org/jira/browse/HBASE-21984) | Generate CHANGES.md and RELEASENOTES.md for 2.1.4 |  Major | documentation, release |
| [HBASE-21934](https://issues.apache.org/jira/browse/HBASE-21934) | RemoteProcedureDispatcher should track the ongoing dispatched calls |  Blocker | proc-v2 |
| [HBASE-21978](https://issues.apache.org/jira/browse/HBASE-21978) | Should close AsyncRegistry if we fail to get cluster id when creating AsyncConnection |  Major | asyncclient, Client |
| [HBASE-21976](https://issues.apache.org/jira/browse/HBASE-21976) | Deal with RetryImmediatelyException for batching request |  Major | asyncclient, Client |
| [HBASE-21906](https://issues.apache.org/jira/browse/HBASE-21906) | Backport the CallQueueTooBigException related changes in HBASE-21875 to branch-2.1 |  Major | proc-v2 |
| [HBASE-21927](https://issues.apache.org/jira/browse/HBASE-21927) | Always fail the locate request when error occur |  Major | asyncclient, Client |
| [HBASE-21930](https://issues.apache.org/jira/browse/HBASE-21930) | Deal with ScannerResetException when opening region scanner |  Major | asyncclient, Client |
| [HBASE-19889](https://issues.apache.org/jira/browse/HBASE-19889) | Revert Workaround: Purge User API building from branch-2 so can make a beta-1 |  Major | website |
| [HBASE-21897](https://issues.apache.org/jira/browse/HBASE-21897) | Set version to 2.1.4-SNAPSHOT for branch-2.1 |  Major | build |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21057](https://issues.apache.org/jira/browse/HBASE-21057) | upgrade to latest spotbugs |  Minor | community, test |
| [HBASE-21884](https://issues.apache.org/jira/browse/HBASE-21884) | Fix box/unbox findbugs warning in secure bulk load |  Minor | . |


## Release 2.1.3 - Unreleased (as of 2019-02-10)



### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21727](https://issues.apache.org/jira/browse/HBASE-21727) | Simplify documentation around client timeout |  Minor | . |
| [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | Throw DNRIOE when connection or rpc client is closed |  Major | asyncclient, Client |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21201](https://issues.apache.org/jira/browse/HBASE-21201) | Support to run VerifyReplication MR tool without peerid |  Major | hbase-operator-tools |
| [HBASE-21857](https://issues.apache.org/jira/browse/HBASE-21857) | Do not need to check clusterKey if replicationEndpoint is provided when adding a peer |  Major | . |
| [HBASE-21816](https://issues.apache.org/jira/browse/HBASE-21816) | Print source cluster replication config directory |  Trivial | Replication |
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
| [HBASE-21862](https://issues.apache.org/jira/browse/HBASE-21862) | IPCUtil.wrapException should keep the original exception types for all the connection exceptions |  Blocker | . |
| [HBASE-21775](https://issues.apache.org/jira/browse/HBASE-21775) | The BufferedMutator doesn't ever refresh region location cache |  Major | Client |
| [HBASE-21843](https://issues.apache.org/jira/browse/HBASE-21843) | RegionGroupingProvider breaks the meta wal file name pattern which may cause data loss for meta region |  Blocker | wal |
| [HBASE-21795](https://issues.apache.org/jira/browse/HBASE-21795) | Client application may get stuck (time bound) if a table modify op is called immediately after split op |  Critical | amv2 |
| [HBASE-21840](https://issues.apache.org/jira/browse/HBASE-21840) | TestHRegionWithInMemoryFlush fails with NPE |  Blocker | test |
| [HBASE-21644](https://issues.apache.org/jira/browse/HBASE-21644) | Modify table procedure runs infinitely for a table having region replication \> 1 |  Critical | Admin |
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
| [HBASE-21853](https://issues.apache.org/jira/browse/HBASE-21853) | update copyright notices to 2019 |  Major | documentation |
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
