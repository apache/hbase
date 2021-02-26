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

## Release 2.2.0 - Unreleased (as of 2019-06-11)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21991](https://issues.apache.org/jira/browse/HBASE-21991) | Fix MetaMetrics issues - [Race condition, Faulty remove logic], few improvements |  Major | Coprocessors, metrics |
| [HBASE-22399](https://issues.apache.org/jira/browse/HBASE-22399) | Change default hadoop-two.version to 2.8.x and remove the 2.7.x hadoop checks |  Major | build, hadoop2 |
| [HBASE-21082](https://issues.apache.org/jira/browse/HBASE-21082) | Reimplement assign/unassign related procedure metrics |  Critical | amv2, metrics |
| [HBASE-20587](https://issues.apache.org/jira/browse/HBASE-20587) | Replace Jackson with shaded thirdparty gson |  Major | dependencies |
| [HBASE-21727](https://issues.apache.org/jira/browse/HBASE-21727) | Simplify documentation around client timeout |  Minor | . |
| [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | Throw DNRIOE when connection or rpc client is closed |  Major | asyncclient, Client |
| [HBASE-21792](https://issues.apache.org/jira/browse/HBASE-21792) | Mark HTableMultiplexer as deprecated and remove it in 3.0.0 |  Major | Client |
| [HBASE-21657](https://issues.apache.org/jira/browse/HBASE-21657) | PrivateCellUtil#estimatedSerializedSizeOf has been the bottleneck in 100% scan case. |  Major | Performance |
| [HBASE-21560](https://issues.apache.org/jira/browse/HBASE-21560) | Return a new TableDescriptor for MasterObserver#preModifyTable to allow coprocessor modify the TableDescriptor |  Major | Coprocessors |
| [HBASE-21492](https://issues.apache.org/jira/browse/HBASE-21492) | CellCodec Written To WAL Before It's Verified |  Critical | wal |
| [HBASE-21452](https://issues.apache.org/jira/browse/HBASE-21452) | Illegal character in hbase counters group name |  Major | spark |
| [HBASE-21158](https://issues.apache.org/jira/browse/HBASE-21158) | Empty qualifier cell should not be returned if it does not match QualifierFilter |  Critical | Filters |
| [HBASE-21223](https://issues.apache.org/jira/browse/HBASE-21223) | [amv2] Remove abort\_procedure from shell |  Critical | amv2, hbck2, shell |
| [HBASE-20881](https://issues.apache.org/jira/browse/HBASE-20881) | Introduce a region transition procedure to handle all the state transition for a region |  Major | amv2, proc-v2 |
| [HBASE-20884](https://issues.apache.org/jira/browse/HBASE-20884) | Replace usage of our Base64 implementation with java.util.Base64 |  Major | . |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22148](https://issues.apache.org/jira/browse/HBASE-22148) | Provide an alternative to CellUtil.setTimestamp |  Blocker | API, Coprocessors |
| [HBASE-21815](https://issues.apache.org/jira/browse/HBASE-21815) | Make isTrackingMetrics and getMetrics of ScannerContext public |  Minor | . |
| [HBASE-21926](https://issues.apache.org/jira/browse/HBASE-21926) | Profiler servlet |  Major | master, Operability, regionserver |
| [HBASE-20886](https://issues.apache.org/jira/browse/HBASE-20886) | [Auth] Support keytab login in hbase client |  Critical | asyncclient, Client, security |
| [HBASE-17942](https://issues.apache.org/jira/browse/HBASE-17942) | Disable region splits and merges per table |  Major | . |
| [HBASE-21753](https://issues.apache.org/jira/browse/HBASE-21753) | Support getting the locations for all the replicas of a region |  Major | Client |
| [HBASE-20636](https://issues.apache.org/jira/browse/HBASE-20636) | Introduce two bloom filter type : ROWPREFIX\_FIXED\_LENGTH and ROWPREFIX\_DELIMITED |  Major | HFile, regionserver, Scanners |
| [HBASE-20649](https://issues.apache.org/jira/browse/HBASE-20649) | Validate HFiles do not have PREFIX\_TREE DataBlockEncoding |  Minor | Operability, tooling |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22284](https://issues.apache.org/jira/browse/HBASE-22284) | optimization StringBuilder.append of AbstractMemStore.toString |  Trivial | . |
| [HBASE-22523](https://issues.apache.org/jira/browse/HBASE-22523) | Refactor RegionStates#getAssignmentsByTable to make it easy to understand |  Major | . |
| [HBASE-22511](https://issues.apache.org/jira/browse/HBASE-22511) | More missing /rs-status links |  Minor | UI |
| [HBASE-22496](https://issues.apache.org/jira/browse/HBASE-22496) | UnsafeAccess.unsafeCopy should not copy more than UNSAFE\_COPY\_THRESHOLD on each iteration |  Major | . |
| [HBASE-22488](https://issues.apache.org/jira/browse/HBASE-22488) | Cleanup the explicit timeout value for test methods |  Major | . |
| [HBASE-22411](https://issues.apache.org/jira/browse/HBASE-22411) | Refactor codes of moving reigons in RSGroup |  Major | rsgroup |
| [HBASE-22467](https://issues.apache.org/jira/browse/HBASE-22467) | WebUI changes to enable Apache Knox UI proxying |  Major | UI |
| [HBASE-22474](https://issues.apache.org/jira/browse/HBASE-22474) | Add --mvn-custom-repo parameter to yetus calls |  Minor | . |
| [HBASE-20305](https://issues.apache.org/jira/browse/HBASE-20305) | Add option to SyncTable that skip deletes on target cluster |  Minor | mapreduce |
| [HBASE-21784](https://issues.apache.org/jira/browse/HBASE-21784) | Dump replication queue should show list of wal files ordered chronologically |  Major | Replication, tooling |
| [HBASE-22384](https://issues.apache.org/jira/browse/HBASE-22384) | Formatting issues in administration section of book |  Minor | community, documentation |
| [HBASE-21658](https://issues.apache.org/jira/browse/HBASE-21658) | Should get the meta replica number from zk instead of config at client side |  Critical | Client |
| [HBASE-22365](https://issues.apache.org/jira/browse/HBASE-22365) | Region may be opened on two RegionServers |  Blocker | amv2 |
| [HBASE-22392](https://issues.apache.org/jira/browse/HBASE-22392) | Remove extra/useless + |  Trivial | . |
| [HBASE-20494](https://issues.apache.org/jira/browse/HBASE-20494) | Upgrade com.yammer.metrics dependency |  Major | dependencies |
| [HBASE-22358](https://issues.apache.org/jira/browse/HBASE-22358) | Change rubocop configuration for method length |  Minor | community, shell |
| [HBASE-22379](https://issues.apache.org/jira/browse/HBASE-22379) | Fix Markdown for "Voting on Release Candidates" in book |  Minor | community, documentation |
| [HBASE-22109](https://issues.apache.org/jira/browse/HBASE-22109) | Update hbase shaded content checker after guava update in hadoop branch-3.0 to 27.0-jre |  Minor | . |
| [HBASE-22087](https://issues.apache.org/jira/browse/HBASE-22087) | Update LICENSE/shading for the dependencies from the latest Hadoop trunk |  Minor | hadoop3 |
| [HBASE-22341](https://issues.apache.org/jira/browse/HBASE-22341) | Add explicit guidelines for removing deprecations in book |  Major | API, community, documentation |
| [HBASE-22225](https://issues.apache.org/jira/browse/HBASE-22225) | Profiler tab on Master/RS UI not working w/o comprehensive message |  Minor | UI |
| [HBASE-22291](https://issues.apache.org/jira/browse/HBASE-22291) | Fix recovery of recovered.edits files under root dir |  Major | . |
| [HBASE-22283](https://issues.apache.org/jira/browse/HBASE-22283) | Print row and table information when failed to get region location |  Major | Client, logging |
| [HBASE-22296](https://issues.apache.org/jira/browse/HBASE-22296) | Remove TestFromClientSide.testGetStartEndKeysWithRegionReplicas |  Major | test |
| [HBASE-22250](https://issues.apache.org/jira/browse/HBASE-22250) | The same constants used in many places should be placed in constant classes |  Minor | Client, conf, regionserver |
| [HBASE-20586](https://issues.apache.org/jira/browse/HBASE-20586) | SyncTable tool: Add support for cross-realm remote clusters |  Major | mapreduce, Operability, Replication |
| [HBASE-21257](https://issues.apache.org/jira/browse/HBASE-21257) | misspelled words.[occured -\> occurred] |  Trivial | . |
| [HBASE-22193](https://issues.apache.org/jira/browse/HBASE-22193) | Add backoff when region failed open too many times |  Major | . |
| [HBASE-22188](https://issues.apache.org/jira/browse/HBASE-22188) | Make TestSplitMerge more stable |  Major | test |
| [HBASE-22097](https://issues.apache.org/jira/browse/HBASE-22097) | Modify the description of split command in shell |  Trivial | shell |
| [HBASE-21964](https://issues.apache.org/jira/browse/HBASE-21964) | unset Quota by Throttle Type |  Major | master |
| [HBASE-22093](https://issues.apache.org/jira/browse/HBASE-22093) | Combine TestRestoreSnapshotFromClientWithRegionReplicas to CloneSnapshotFromClientAfterSplittingRegionTestBase#testCloneSnapshotAfterSplittingRegion |  Major | . |
| [HBASE-22009](https://issues.apache.org/jira/browse/HBASE-22009) | Improve RSGroupInfoManagerImpl#getDefaultServers() |  Minor | rsgroup |
| [HBASE-22032](https://issues.apache.org/jira/browse/HBASE-22032) | KeyValue validation should check for null byte array |  Major | . |
| [HBASE-21667](https://issues.apache.org/jira/browse/HBASE-21667) | Move to latest ASF Parent POM |  Minor | build |
| [HBASE-21810](https://issues.apache.org/jira/browse/HBASE-21810) | bulkload  support set hfile compression on client |  Major | mapreduce |
| [HBASE-21987](https://issues.apache.org/jira/browse/HBASE-21987) | Simplify RSGroupInfoManagerImpl#flushConfig() for offline mode |  Minor | rsgroup |
| [HBASE-21871](https://issues.apache.org/jira/browse/HBASE-21871) | Support to specify a peer table name in VerifyReplication tool |  Major | . |
| [HBASE-21255](https://issues.apache.org/jira/browse/HBASE-21255) | [acl] Refactor TablePermission into three classes (Global, Namespace, Table) |  Major | . |
| [HBASE-21410](https://issues.apache.org/jira/browse/HBASE-21410) | A helper page that help find all problematic regions and procedures |  Major | . |
| [HBASE-20734](https://issues.apache.org/jira/browse/HBASE-20734) | Colocate recovered edits directory with hbase.wal.dir |  Major | MTTR, Recovery, wal |
| [HBASE-20401](https://issues.apache.org/jira/browse/HBASE-20401) | Make \`MAX\_WAIT\` and \`waitIfNotFinished\` in CleanerContext configurable |  Minor | master |
| [HBASE-21481](https://issues.apache.org/jira/browse/HBASE-21481) | [acl] Superuser's permissions should not be granted or revoked by any non-su global admin |  Major | . |
| [HBASE-21967](https://issues.apache.org/jira/browse/HBASE-21967) | Split TestServerCrashProcedure and TestServerCrashProcedureWithReplicas |  Major | . |
| [HBASE-21867](https://issues.apache.org/jira/browse/HBASE-21867) | Support multi-threads in HFileArchiver |  Major | . |
| [HBASE-21932](https://issues.apache.org/jira/browse/HBASE-21932) | Use Runtime.getRuntime().halt to terminate regionserver when abort timeout |  Major | . |
| [HBASE-21875](https://issues.apache.org/jira/browse/HBASE-21875) | Change the retry logic in RSProcedureDispatcher to 'retry by default, only if xxx' |  Major | proc-v2 |
| [HBASE-21780](https://issues.apache.org/jira/browse/HBASE-21780) | Avoid a wide line on the RegionServer webUI for many ZooKeeper servers |  Minor | UI, Usability |
| [HBASE-21636](https://issues.apache.org/jira/browse/HBASE-21636) | Enhance the shell scan command to support missing scanner specifications like ReadType, IsolationLevel etc. |  Major | shell |
| [HBASE-21857](https://issues.apache.org/jira/browse/HBASE-21857) | Do not need to check clusterKey if replicationEndpoint is provided when adding a peer |  Major | . |
| [HBASE-21201](https://issues.apache.org/jira/browse/HBASE-21201) | Support to run VerifyReplication MR tool without peerid |  Major | hbase-operator-tools |
| [HBASE-21816](https://issues.apache.org/jira/browse/HBASE-21816) | Print source cluster replication config directory |  Trivial | Replication |
| [HBASE-19616](https://issues.apache.org/jira/browse/HBASE-19616) | Review of LogCleaner Class |  Minor | . |
| [HBASE-21830](https://issues.apache.org/jira/browse/HBASE-21830) | Backport HBASE-20577 (Make Log Level page design consistent with the design of other pages in UI) to branch-2 |  Major | UI, Usability |
| [HBASE-21833](https://issues.apache.org/jira/browse/HBASE-21833) | Use NettyAsyncFSWALConfigHelper.setEventLoopConfig to prevent creating too many netty event loop when executing TestHRegion |  Minor | test |
| [HBASE-21634](https://issues.apache.org/jira/browse/HBASE-21634) | Print error message when user uses unacceptable values for LIMIT while setting quotas. |  Minor | . |
| [HBASE-21789](https://issues.apache.org/jira/browse/HBASE-21789) | Rewrite MetaTableAccessor.multiMutate with Table.coprocessorService |  Major | Client, Coprocessors |
| [HBASE-21689](https://issues.apache.org/jira/browse/HBASE-21689) | Make table/namespace specific current quota info available in shell(describe\_namespace & describe) |  Minor | . |
| [HBASE-20215](https://issues.apache.org/jira/browse/HBASE-20215) | Rename CollectionUtils to ConcurrentMapUtils |  Trivial | . |
| [HBASE-21720](https://issues.apache.org/jira/browse/HBASE-21720) | metric to measure how actions are distributed to servers within a MultiAction |  Minor | Client, metrics, monitoring |
| [HBASE-21595](https://issues.apache.org/jira/browse/HBASE-21595) | Print thread's information and stack traces when RS is aborting forcibly |  Minor | regionserver |
| [HBASE-20209](https://issues.apache.org/jira/browse/HBASE-20209) | Do Not Use Both Map containsKey and get Methods in Replication Sink |  Trivial | Replication |
| [HBASE-21712](https://issues.apache.org/jira/browse/HBASE-21712) | Make submit-patch.py python3 compatible |  Minor | tooling |
| [HBASE-21590](https://issues.apache.org/jira/browse/HBASE-21590) | Optimize trySkipToNextColumn in StoreScanner a bit |  Critical | Performance, Scanners |
| [HBASE-21297](https://issues.apache.org/jira/browse/HBASE-21297) | ModifyTableProcedure can throw TNDE instead of IOE in case of REGION\_REPLICATION change |  Minor | . |
| [HBASE-21700](https://issues.apache.org/jira/browse/HBASE-21700) | Simplify the implementation of RSGroupInfoManagerImpl |  Major | rsgroup |
| [HBASE-21694](https://issues.apache.org/jira/browse/HBASE-21694) | Add append\_peer\_exclude\_tableCFs and remove\_peer\_exclude\_tableCFs shell commands |  Major | . |
| [HBASE-21645](https://issues.apache.org/jira/browse/HBASE-21645) | Perform sanity check and disallow table creation/modification with region replication \< 1 |  Minor | . |
| [HBASE-21360](https://issues.apache.org/jira/browse/HBASE-21360) | Disable printing of stack-trace in shell for quotas |  Minor | shell |
| [HBASE-21662](https://issues.apache.org/jira/browse/HBASE-21662) | Add append\_peer\_exclude\_namespaces and remove\_peer\_exclude\_namespaces shell commands |  Major | . |
| [HBASE-21659](https://issues.apache.org/jira/browse/HBASE-21659) | Avoid to load duplicate coprocessors in system config and table descriptor |  Minor | . |
| [HBASE-21642](https://issues.apache.org/jira/browse/HBASE-21642) | CopyTable by reading snapshot and bulkloading will save a lot of time. |  Major | . |
| [HBASE-21643](https://issues.apache.org/jira/browse/HBASE-21643) | Introduce two new region coprocessor method and deprecated postMutationBeforeWAL |  Major | . |
| [HBASE-21640](https://issues.apache.org/jira/browse/HBASE-21640) | Remove the TODO when increment zero |  Major | . |
| [HBASE-21631](https://issues.apache.org/jira/browse/HBASE-21631) | list\_quotas should print human readable values for LIMIT |  Minor | shell |
| [HBASE-21635](https://issues.apache.org/jira/browse/HBASE-21635) | Use maven enforcer to ban imports from illegal packages |  Major | build |
| [HBASE-21514](https://issues.apache.org/jira/browse/HBASE-21514) | Refactor CacheConfig |  Major | . |
| [HBASE-21520](https://issues.apache.org/jira/browse/HBASE-21520) | TestMultiColumnScanner cost long time when using ROWCOL bloom type |  Major | test |
| [HBASE-21554](https://issues.apache.org/jira/browse/HBASE-21554) | Show replication endpoint classname for replication peer on master web UI |  Minor | UI |
| [HBASE-21549](https://issues.apache.org/jira/browse/HBASE-21549) | Add shell command for serial replication peer |  Major | . |
| [HBASE-21283](https://issues.apache.org/jira/browse/HBASE-21283) | Add new shell command 'rit' for listing regions in transition |  Minor | Operability, shell |
| [HBASE-21567](https://issues.apache.org/jira/browse/HBASE-21567) | Allow overriding configs starting up the shell |  Major | shell |
| [HBASE-21413](https://issues.apache.org/jira/browse/HBASE-21413) | Empty meta log doesn't get split when restart whole cluster |  Major | . |
| [HBASE-21524](https://issues.apache.org/jira/browse/HBASE-21524) | Unnecessary DEBUG log in ConnectionImplementation#isTableEnabled |  Major | Client |
| [HBASE-21511](https://issues.apache.org/jira/browse/HBASE-21511) | Remove in progress snapshot check in SnapshotFileCache#getUnreferencedFiles |  Minor | snapshots |
| [HBASE-21480](https://issues.apache.org/jira/browse/HBASE-21480) | Taking snapshot when RS crashes prevent we bring the regions online |  Major | snapshots |
| [HBASE-21485](https://issues.apache.org/jira/browse/HBASE-21485) | Add more debug logs for remote procedure execution |  Major | proc-v2 |
| [HBASE-21328](https://issues.apache.org/jira/browse/HBASE-21328) | add HBASE\_DISABLE\_HADOOP\_CLASSPATH\_LOOKUP switch to hbase-env.sh |  Minor | documentation, Operability |
| [HBASE-19682](https://issues.apache.org/jira/browse/HBASE-19682) | Use Collections.emptyList() For Empty List Values |  Minor | . |
| [HBASE-21388](https://issues.apache.org/jira/browse/HBASE-21388) | No need to instantiate MemStoreLAB for master which not carry table |  Major | . |
| [HBASE-21325](https://issues.apache.org/jira/browse/HBASE-21325) | Force to terminate regionserver when abort hang in somewhere |  Major | . |
| [HBASE-21385](https://issues.apache.org/jira/browse/HBASE-21385) | HTable.delete request use rpc call directly instead of AsyncProcess |  Major | . |
| [HBASE-21318](https://issues.apache.org/jira/browse/HBASE-21318) | Make RefreshHFilesClient runnable |  Minor | HFile |
| [HBASE-21263](https://issues.apache.org/jira/browse/HBASE-21263) | Mention compression algorithm along with other storefile details |  Minor | . |
| [HBASE-21290](https://issues.apache.org/jira/browse/HBASE-21290) | No need to instantiate BlockCache for master which not carry table |  Major | . |
| [HBASE-21256](https://issues.apache.org/jira/browse/HBASE-21256) | Improve IntegrationTestBigLinkedList for testing huge data |  Major | integration tests |
| [HBASE-21251](https://issues.apache.org/jira/browse/HBASE-21251) | Refactor RegionMover |  Major | Operability |
| [HBASE-21303](https://issues.apache.org/jira/browse/HBASE-21303) | [shell] clear\_deadservers with no args fails |  Major | . |
| [HBASE-21098](https://issues.apache.org/jira/browse/HBASE-21098) | Improve Snapshot Performance with Temporary Snapshot Directory when rootDir on S3 |  Major | . |
| [HBASE-21299](https://issues.apache.org/jira/browse/HBASE-21299) | List counts of actual region states in master UI tables section |  Major | UI |
| [HBASE-21289](https://issues.apache.org/jira/browse/HBASE-21289) | Remove the log "'hbase.regionserver.maxlogs' was deprecated." in AbstractFSWAL |  Minor | . |
| [HBASE-21185](https://issues.apache.org/jira/browse/HBASE-21185) | WALPrettyPrinter: Additional useful info to be printed by wal printer tool, for debugability purposes |  Minor | Operability |
| [HBASE-21103](https://issues.apache.org/jira/browse/HBASE-21103) | nightly test cache of yetus install needs to be more thorough in verification |  Major | test |
| [HBASE-21207](https://issues.apache.org/jira/browse/HBASE-21207) | Add client side sorting functionality in master web UI for table and region server details. |  Minor | master, monitoring, UI, Usability |
| [HBASE-20857](https://issues.apache.org/jira/browse/HBASE-20857) | JMX - add Balancer status = enabled / disabled |  Major | API, master, metrics, REST, tooling, Usability |
| [HBASE-21164](https://issues.apache.org/jira/browse/HBASE-21164) | reportForDuty to spew less log if master is initializing |  Minor | regionserver |
| [HBASE-21204](https://issues.apache.org/jira/browse/HBASE-21204) | NPE when scan raw DELETE\_FAMILY\_VERSION and codec is not set |  Major | . |
| [HBASE-20307](https://issues.apache.org/jira/browse/HBASE-20307) | LoadTestTool prints too much zookeeper logging |  Minor | tooling |
| [HBASE-21155](https://issues.apache.org/jira/browse/HBASE-21155) | Save on a few log strings and some churn in wal splitter by skipping out early if no logs in dir |  Trivial | . |
| [HBASE-21129](https://issues.apache.org/jira/browse/HBASE-21129) | Clean up duplicate codes in #equals and #hashCode methods of Filter |  Minor | Filters |
| [HBASE-21157](https://issues.apache.org/jira/browse/HBASE-21157) | Split TableInputFormatScan to individual tests |  Minor | test |
| [HBASE-21107](https://issues.apache.org/jira/browse/HBASE-21107) | add a metrics for netty direct memory |  Minor | IPC/RPC |
| [HBASE-21153](https://issues.apache.org/jira/browse/HBASE-21153) | Shaded client jars should always build in relevant phase to avoid confusion |  Major | build |
| [HBASE-21126](https://issues.apache.org/jira/browse/HBASE-21126) | Add ability for HBase Canary to ignore a configurable number of ZooKeeper down nodes |  Minor | canary, Zookeeper |
| [HBASE-20749](https://issues.apache.org/jira/browse/HBASE-20749) | Upgrade our use of checkstyle to 8.6+ |  Minor | build, community |
| [HBASE-21071](https://issues.apache.org/jira/browse/HBASE-21071) | HBaseTestingUtility::startMiniCluster() to use builder pattern |  Major | test |
| [HBASE-20387](https://issues.apache.org/jira/browse/HBASE-20387) | flaky infrastructure should work for all branches |  Critical | test |
| [HBASE-20469](https://issues.apache.org/jira/browse/HBASE-20469) | Directory used for sidelining old recovered edits files should be made configurable |  Minor | . |
| [HBASE-20979](https://issues.apache.org/jira/browse/HBASE-20979) | Flaky test reporting should specify what JSON it needs and handle HTTP errors |  Minor | test |
| [HBASE-20985](https://issues.apache.org/jira/browse/HBASE-20985) | add two attributes when we do normalization |  Major | . |
| [HBASE-20965](https://issues.apache.org/jira/browse/HBASE-20965) | Separate region server report requests to new handlers |  Major | Performance |
| [HBASE-20845](https://issues.apache.org/jira/browse/HBASE-20845) | Support set the consistency for Gets and Scans in thrift2 |  Major | Thrift |
| [HBASE-20986](https://issues.apache.org/jira/browse/HBASE-20986) | Separate the config of block size when we do log splitting and write Hlog |  Major | . |
| [HBASE-19036](https://issues.apache.org/jira/browse/HBASE-19036) | Add action in Chaos Monkey to restart Active Namenode |  Minor | . |
| [HBASE-20856](https://issues.apache.org/jira/browse/HBASE-20856) | PITA having to set WAL provider in two places |  Minor | Operability, wal |
| [HBASE-20935](https://issues.apache.org/jira/browse/HBASE-20935) | HStore.removeCompactedFiles should log in case it is unable to delete a file |  Minor | . |
| [HBASE-20873](https://issues.apache.org/jira/browse/HBASE-20873) | Update doc for Endpoint-based Export |  Minor | documentation |
| [HBASE-20672](https://issues.apache.org/jira/browse/HBASE-20672) | New metrics ReadRequestRate and WriteRequestRate |  Minor | metrics |
| [HBASE-20617](https://issues.apache.org/jira/browse/HBASE-20617) | Upgrade/remove jetty-jsp |  Minor | . |
| [HBASE-20396](https://issues.apache.org/jira/browse/HBASE-20396) | Remove redundant MBean from thrift JMX |  Major | Thrift |
| [HBASE-20357](https://issues.apache.org/jira/browse/HBASE-20357) | AccessControlClient API Enhancement |  Major | security |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-22563](https://issues.apache.org/jira/browse/HBASE-22563) | Reduce retained jobs for Jenkins pipelines |  Major | . |
| [HBASE-22552](https://issues.apache.org/jira/browse/HBASE-22552) | Rewrite TestEndToEndSplitTransaction.testCanSplitJustAfterASplit |  Major | test |
| [HBASE-22551](https://issues.apache.org/jira/browse/HBASE-22551) | TestMasterOperationsForRegionReplicas is flakey |  Major | read replicas, test |
| [HBASE-22481](https://issues.apache.org/jira/browse/HBASE-22481) | Javadoc Warnings: reference not found |  Trivial | documentation |
| [HBASE-22546](https://issues.apache.org/jira/browse/HBASE-22546) | TestRegionServerHostname#testRegionServerHostname fails reliably for me |  Major | . |
| [HBASE-22534](https://issues.apache.org/jira/browse/HBASE-22534) | TestCellUtil fails when run on JDK11 |  Minor | java, test |
| [HBASE-22536](https://issues.apache.org/jira/browse/HBASE-22536) | TestForeignExceptionSerialization fails when run on JDK11 |  Minor | java |
| [HBASE-22535](https://issues.apache.org/jira/browse/HBASE-22535) | TestShellRSGroups fails when run on JDK11 |  Minor | java, shell |
| [HBASE-22518](https://issues.apache.org/jira/browse/HBASE-22518) | yetus personality is treating branch-1.4 like earlier branches for hadoopcheck |  Major | test |
| [HBASE-22513](https://issues.apache.org/jira/browse/HBASE-22513) | Admin#getQuota does not work correctly if exceedThrottleQuota is set |  Major | Quotas |
| [HBASE-22522](https://issues.apache.org/jira/browse/HBASE-22522) | The integration test in master branch's nightly job has error "ERROR: Only found 1050 rows." |  Major | . |
| [HBASE-22490](https://issues.apache.org/jira/browse/HBASE-22490) | Nightly client integration test fails with hadoop-3 |  Major | build |
| [HBASE-22502](https://issues.apache.org/jira/browse/HBASE-22502) | Purge the logs when we reach the EOF for the last wal file when replication |  Major | . |
| [HBASE-22503](https://issues.apache.org/jira/browse/HBASE-22503) | Failed to upgrade to 2.2+ as the global permission which storaged in zk is not right |  Blocker | . |
| [HBASE-22487](https://issues.apache.org/jira/browse/HBASE-22487) | getMostLoadedRegions is unused |  Trivial | regionserver |
| [HBASE-22485](https://issues.apache.org/jira/browse/HBASE-22485) | Fix failed ut TestClusterRestartFailover |  Major | . |
| [HBASE-22486](https://issues.apache.org/jira/browse/HBASE-22486) | Fix flaky test TestLockManager |  Major | . |
| [HBASE-22471](https://issues.apache.org/jira/browse/HBASE-22471) | Our nightly jobs for master and branch-2 are still using hadoop-2.7.1 in integration test |  Major | build |
| [HBASE-22003](https://issues.apache.org/jira/browse/HBASE-22003) | Fix flaky test TestVerifyReplication.testHBase14905 |  Major | . |
| [HBASE-22441](https://issues.apache.org/jira/browse/HBASE-22441) | BucketCache NullPointerException in cacheBlock |  Major | BucketCache |
| [HBASE-22473](https://issues.apache.org/jira/browse/HBASE-22473) | Split TestSCP |  Major | Recovery, test |
| [HBASE-22456](https://issues.apache.org/jira/browse/HBASE-22456) | Polish TestSplitTransitionOnCluster |  Major | test |
| [HBASE-21800](https://issues.apache.org/jira/browse/HBASE-21800) | RegionServer aborted due to NPE from MetaTableMetrics coprocessor |  Critical | Coprocessors, meta, metrics, Operability |
| [HBASE-22462](https://issues.apache.org/jira/browse/HBASE-22462) | Should run a 'mvn install' at the end of hadoop check in pre commit job |  Major | build |
| [HBASE-22440](https://issues.apache.org/jira/browse/HBASE-22440) | HRegionServer#getWalGroupsReplicationStatus() throws NPE |  Major | regionserver, Replication |
| [HBASE-22226](https://issues.apache.org/jira/browse/HBASE-22226) | Incorrect level for headings in asciidoc |  Trivial | documentation |
| [HBASE-22442](https://issues.apache.org/jira/browse/HBASE-22442) | Nightly build is failing with hadoop 3.x |  Major | build, hadoop3 |
| [HBASE-20970](https://issues.apache.org/jira/browse/HBASE-20970) | Update hadoop check versions for hadoop3 in hbase-personality |  Major | build |
| [HBASE-22424](https://issues.apache.org/jira/browse/HBASE-22424) | Interactions in RSGroup test classes will cause TestRSGroupsAdmin2.testMoveServersAndTables and TestRSGroupsBalance.testGroupBalance flaky |  Major | rsgroup |
| [HBASE-22404](https://issues.apache.org/jira/browse/HBASE-22404) | Open/Close region request may be executed twice when master restart |  Major | . |
| [HBASE-22274](https://issues.apache.org/jira/browse/HBASE-22274) | Cell size limit check on append should consider cell's previous size. |  Minor | . |
| [HBASE-22072](https://issues.apache.org/jira/browse/HBASE-22072) | High read/write intensive regions may cause long crash recovery |  Major | Performance, Recovery |
| [HBASE-22324](https://issues.apache.org/jira/browse/HBASE-22324) |  loss a mass of data when the sequenceId of cells greater than Integer.Max, because MemStoreMergerSegmentsIterator can not merge segments |  Blocker | in-memory-compaction |
| [HBASE-21777](https://issues.apache.org/jira/browse/HBASE-21777) | "Tune compaction throughput" debug messages even when nothing has changed |  Trivial | Compaction |
| [HBASE-22360](https://issues.apache.org/jira/browse/HBASE-22360) | Abort timer doesn't set when abort is called during graceful shutdown process |  Major | regionserver |
| [HBASE-20851](https://issues.apache.org/jira/browse/HBASE-20851) | Change rubocop config for max line length of 100 |  Minor | community, shell |
| [HBASE-21467](https://issues.apache.org/jira/browse/HBASE-21467) | Fix flaky test TestCoprocessorClassLoader.testCleanupOldJars |  Minor | . |
| [HBASE-22312](https://issues.apache.org/jira/browse/HBASE-22312) | Hadoop 3 profile for hbase-shaded-mapreduce should like mapreduce as a provided dependency |  Major | mapreduce, shading |
| [HBASE-22314](https://issues.apache.org/jira/browse/HBASE-22314) | shaded byo-hadoop client should list needed hadoop modules as provided scope to avoid inclusion of unnecessary transitive depednencies |  Major | hadoop2, hadoop3, shading |
| [HBASE-22047](https://issues.apache.org/jira/browse/HBASE-22047) | LeaseException in Scan should be retired |  Major | Client, Scanners |
| [HBASE-22343](https://issues.apache.org/jira/browse/HBASE-22343) | Make procedure retry interval configurable in test |  Major | amv2, test |
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
| [HBASE-22222](https://issues.apache.org/jira/browse/HBASE-22222) | Site build fails after hbase-thirdparty upgrade |  Blocker | website |
| [HBASE-22249](https://issues.apache.org/jira/browse/HBASE-22249) | Rest Server throws NoClassDefFoundError with Java 11 (run-time) |  Major | . |
| [HBASE-22235](https://issues.apache.org/jira/browse/HBASE-22235) | OperationStatus.{SUCCESS\|FAILURE\|NOT\_RUN} are not visible to 3rd party coprocessors |  Major | Coprocessors |
| [HBASE-22207](https://issues.apache.org/jira/browse/HBASE-22207) | Fix flakey TestAssignmentManager.testAssignSocketTimeout |  Major | test |
| [HBASE-22202](https://issues.apache.org/jira/browse/HBASE-22202) | Fix new findbugs issues after we upgrade hbase-thirdparty dependencies |  Major | findbugs |
| [HBASE-22144](https://issues.apache.org/jira/browse/HBASE-22144) | MultiRowRangeFilter does not work with reversed scans |  Critical | Filters, Scanners |
| [HBASE-22198](https://issues.apache.org/jira/browse/HBASE-22198) | Fix flakey TestAsyncTableGetMultiThreaded |  Major | test |
| [HBASE-22185](https://issues.apache.org/jira/browse/HBASE-22185) | RAMQueueEntry#writeToCache should freeBlock if any exception encountered instead of the IOException catch block |  Major | . |
| [HBASE-22163](https://issues.apache.org/jira/browse/HBASE-22163) | Should not archive the compacted store files when region warmup |  Blocker | . |
| [HBASE-22178](https://issues.apache.org/jira/browse/HBASE-22178) | Introduce a createTableAsync with TableDescriptor method in Admin |  Major | Admin |
| [HBASE-22180](https://issues.apache.org/jira/browse/HBASE-22180) | Make TestBlockEvictionFromClient.testBlockRefCountAfterSplits more stable |  Major | test |
| [HBASE-22179](https://issues.apache.org/jira/browse/HBASE-22179) | Fix RawAsyncHBaseAdmin.getCompactionState |  Major | Admin, asyncclient |
| [HBASE-22177](https://issues.apache.org/jira/browse/HBASE-22177) | Do not recreate IOException in RawAsyncHBaseAdmin.adminCall |  Major | Admin, asyncclient |
| [HBASE-22070](https://issues.apache.org/jira/browse/HBASE-22070) | Checking restoreDir in RestoreSnapshotHelper |  Minor | snapshots |
| [HBASE-20912](https://issues.apache.org/jira/browse/HBASE-20912) | Add import order config in dev support for eclipse |  Major | . |
| [HBASE-22133](https://issues.apache.org/jira/browse/HBASE-22133) | Forward port HBASE-22073 "/rits.jsp throws an exception if no procedure" to branch-2.2+ |  Major | UI |
| [HBASE-20911](https://issues.apache.org/jira/browse/HBASE-20911) | correct Swtich/case indentation in formatter template for eclipse |  Major | . |
| [HBASE-21688](https://issues.apache.org/jira/browse/HBASE-21688) | Address WAL filesystem issues |  Major | Filesystem Integration, wal |
| [HBASE-22121](https://issues.apache.org/jira/browse/HBASE-22121) | AsyncAdmin can not deal with non default meta replica |  Major | Admin, asyncclient, Client |
| [HBASE-22115](https://issues.apache.org/jira/browse/HBASE-22115) | HBase RPC aspires to grow an infinite tree of trace scopes; some other places are also unsafe |  Critical | . |
| [HBASE-22123](https://issues.apache.org/jira/browse/HBASE-22123) | REST gateway reports Insufficient permissions exceptions as 404 Not Found |  Minor | REST |
| [HBASE-21135](https://issues.apache.org/jira/browse/HBASE-21135) | Build fails on windows as it fails to parse windows path during license check |  Major | build |
| [HBASE-21781](https://issues.apache.org/jira/browse/HBASE-21781) | list\_deadservers elapsed time is incorrect |  Major | shell |
| [HBASE-22100](https://issues.apache.org/jira/browse/HBASE-22100) | False positive for error prone warnings in pre commit job |  Minor | build |
| [HBASE-22098](https://issues.apache.org/jira/browse/HBASE-22098) | Backport HBASE-18667 "Disable error-prone for hbase-protocol-shaded" to branch-2 |  Major | build |
| [HBASE-20662](https://issues.apache.org/jira/browse/HBASE-20662) | Increasing space quota on a violated table does not remove SpaceViolationPolicy.DISABLE enforcement |  Major | . |
| [HBASE-22057](https://issues.apache.org/jira/browse/HBASE-22057) | Impose upper-bound on size of ZK ops sent in a single multi() |  Major | . |
| [HBASE-22074](https://issues.apache.org/jira/browse/HBASE-22074) | Should use procedure store to persist the state in reportRegionStateTransition |  Blocker | amv2, proc-v2 |
| [HBASE-21619](https://issues.apache.org/jira/browse/HBASE-21619) | Fix warning message caused by incorrect ternary operator evaluation |  Trivial | . |
| [HBASE-22095](https://issues.apache.org/jira/browse/HBASE-22095) | Taking a snapshot fails in local mode |  Major | . |
| [HBASE-22061](https://issues.apache.org/jira/browse/HBASE-22061) | SplitTableRegionProcedure should hold the lock of its daughter regions |  Major | . |
| [HBASE-22045](https://issues.apache.org/jira/browse/HBASE-22045) | Mutable range histogram reports incorrect outliers |  Major | . |
| [HBASE-21736](https://issues.apache.org/jira/browse/HBASE-21736) | Remove the server from online servers before scheduling SCP for it in hbck |  Major | hbck2, test |
| [HBASE-22011](https://issues.apache.org/jira/browse/HBASE-22011) | ThriftUtilities.getFromThrift should set filter when not set columns |  Major | . |
| [HBASE-21990](https://issues.apache.org/jira/browse/HBASE-21990) | puppycrawl checkstyle dtds 404... moved to sourceforge |  Major | build |
| [HBASE-22010](https://issues.apache.org/jira/browse/HBASE-22010) | docs on upgrade from 2.0,2.1 -\> 2.2 renders incorrectly |  Minor | documentation |
| [HBASE-22006](https://issues.apache.org/jira/browse/HBASE-22006) | Fix branch-2.1 findbugs warning; causes nightly show as failed. |  Major | . |
| [HBASE-21960](https://issues.apache.org/jira/browse/HBASE-21960) | RESTServletContainer not configured for REST Jetty server |  Blocker | REST |
| [HBASE-21915](https://issues.apache.org/jira/browse/HBASE-21915) | FileLink$FileLinkInputStream doesn't implement CanUnbuffer |  Major | Filesystem Integration |
| [HBASE-21565](https://issues.apache.org/jira/browse/HBASE-21565) | Delete dead server from dead server list too early leads to concurrent Server Crash Procedures(SCP) for a same server |  Critical | . |
| [HBASE-21740](https://issues.apache.org/jira/browse/HBASE-21740) | NPE happens while shutdown the RS |  Major | . |
| [HBASE-21866](https://issues.apache.org/jira/browse/HBASE-21866) | Do not move the table to null rsgroup when creating an existing table |  Major | proc-v2, rsgroup |
| [HBASE-21983](https://issues.apache.org/jira/browse/HBASE-21983) | Should track the scan metrics in AsyncScanSingleRegionRpcRetryingCaller if scan metrics is enabled |  Major | asyncclient, Client |
| [HBASE-21980](https://issues.apache.org/jira/browse/HBASE-21980) | Fix typo in AbstractTestAsyncTableRegionReplicasRead |  Major | test |
| [HBASE-21487](https://issues.apache.org/jira/browse/HBASE-21487) | Concurrent modify table ops can lead to unexpected results |  Major | . |
| [HBASE-20724](https://issues.apache.org/jira/browse/HBASE-20724) | Sometimes some compacted storefiles are still opened after region failover |  Critical | . |
| [HBASE-21961](https://issues.apache.org/jira/browse/HBASE-21961) | Infinite loop in AsyncNonMetaRegionLocator if there is only one region and we tried to locate before a non empty row |  Critical | asyncclient, Client |
| [HBASE-21943](https://issues.apache.org/jira/browse/HBASE-21943) | The usage of RegionLocations.mergeRegionLocations is wrong for async client |  Critical | asyncclient, Client |
| [HBASE-21947](https://issues.apache.org/jira/browse/HBASE-21947) | TestShell is broken after we remove the jackson dependencies |  Major | dependencies, shell |
| [HBASE-21942](https://issues.apache.org/jira/browse/HBASE-21942) | [UI] requests per second is incorrect in rsgroup page(rsgroup.jsp) |  Minor | . |
| [HBASE-21922](https://issues.apache.org/jira/browse/HBASE-21922) | BloomContext#sanityCheck may failed when use ROWPREFIX\_DELIMITED bloom filter |  Major | . |
| [HBASE-21929](https://issues.apache.org/jira/browse/HBASE-21929) | The checks at the end of TestRpcClientLeaks are not executed |  Major | test |
| [HBASE-21938](https://issues.apache.org/jira/browse/HBASE-21938) | Add a new ClusterMetrics.Option SERVERS\_NAME to only return the live region servers's name without metrics |  Major | . |
| [HBASE-21928](https://issues.apache.org/jira/browse/HBASE-21928) | Deprecated HConstants.META\_QOS |  Major | Client, rpc |
| [HBASE-21899](https://issues.apache.org/jira/browse/HBASE-21899) | Fix missing variables in slf4j Logger |  Trivial | logging |
| [HBASE-21910](https://issues.apache.org/jira/browse/HBASE-21910) | The nonce implementation is wrong for AsyncTable |  Critical | asyncclient, Client |
| [HBASE-21900](https://issues.apache.org/jira/browse/HBASE-21900) | Infinite loop in AsyncMetaRegionLocator if we can not get the location for meta |  Major | asyncclient, Client |
| [HBASE-21890](https://issues.apache.org/jira/browse/HBASE-21890) | Use execute instead of submit to submit a task in RemoteProcedureDispatcher |  Critical | proc-v2 |
| [HBASE-21889](https://issues.apache.org/jira/browse/HBASE-21889) | Use thrift 0.12.0 when build thrift by compile-thrift profile |  Major | . |
| [HBASE-21785](https://issues.apache.org/jira/browse/HBASE-21785) | master reports open regions as RITs and also messes up rit age metric |  Major | . |
| [HBASE-21854](https://issues.apache.org/jira/browse/HBASE-21854) | Race condition in TestProcedureSkipPersistence |  Minor | proc-v2 |
| [HBASE-21862](https://issues.apache.org/jira/browse/HBASE-21862) | IPCUtil.wrapException should keep the original exception types for all the connection exceptions |  Blocker | . |
| [HBASE-18484](https://issues.apache.org/jira/browse/HBASE-18484) | VerifyRep by snapshot  does not work when Yarn / SourceHBase / PeerHBase located in different HDFS clusters |  Major | Replication |
| [HBASE-21775](https://issues.apache.org/jira/browse/HBASE-21775) | The BufferedMutator doesn't ever refresh region location cache |  Major | Client |
| [HBASE-21843](https://issues.apache.org/jira/browse/HBASE-21843) | RegionGroupingProvider breaks the meta wal file name pattern which may cause data loss for meta region |  Blocker | wal |
| [HBASE-21795](https://issues.apache.org/jira/browse/HBASE-21795) | Client application may get stuck (time bound) if a table modify op is called immediately after split op |  Critical | amv2 |
| [HBASE-21840](https://issues.apache.org/jira/browse/HBASE-21840) | TestHRegionWithInMemoryFlush fails with NPE |  Blocker | test |
| [HBASE-21811](https://issues.apache.org/jira/browse/HBASE-21811) | region can be opened on two servers due to race condition with procedures and server reports |  Blocker | amv2 |
| [HBASE-21644](https://issues.apache.org/jira/browse/HBASE-21644) | Modify table procedure runs infinitely for a table having region replication \> 1 |  Critical | Admin |
| [HBASE-21733](https://issues.apache.org/jira/browse/HBASE-21733) | SnapshotQuotaObserverChore should only fetch space quotas |  Major | . |
| [HBASE-21699](https://issues.apache.org/jira/browse/HBASE-21699) | Create table failed when using  SPLITS\_FILE =\> 'splits.txt' |  Blocker | Client, shell |
| [HBASE-21535](https://issues.apache.org/jira/browse/HBASE-21535) | Zombie Master detector is not working |  Critical | master |
| [HBASE-21770](https://issues.apache.org/jira/browse/HBASE-21770) | Should deal with meta table in HRegionLocator.getAllRegionLocations |  Major | Client |
| [HBASE-21754](https://issues.apache.org/jira/browse/HBASE-21754) | ReportRegionStateTransitionRequest should be executed in priority executor |  Major | . |
| [HBASE-21475](https://issues.apache.org/jira/browse/HBASE-21475) | Put mutation (having TTL set) added via co-processor is retrieved even after TTL expires |  Major | Coprocessors |
| [HBASE-21749](https://issues.apache.org/jira/browse/HBASE-21749) | RS UI may throw NPE and make rs-status page inaccessible with multiwal and replication |  Major | Replication, UI |
| [HBASE-21746](https://issues.apache.org/jira/browse/HBASE-21746) | Fix two concern cases in RegionMover |  Major | . |
| [HBASE-21732](https://issues.apache.org/jira/browse/HBASE-21732) | Should call toUpperCase before using Enum.valueOf in some methods for ColumnFamilyDescriptor |  Critical | Client |
| [HBASE-21704](https://issues.apache.org/jira/browse/HBASE-21704) | The implementation of DistributedHBaseCluster.getServerHoldingRegion is incorrect |  Major | . |
| [HBASE-20917](https://issues.apache.org/jira/browse/HBASE-20917) | MetaTableMetrics#stop references uninitialized requestsMap for non-meta region |  Major | meta, metrics |
| [HBASE-21639](https://issues.apache.org/jira/browse/HBASE-21639) | maxHeapUsage value not read properly from config during EntryBuffers initialization |  Minor | . |
| [HBASE-21225](https://issues.apache.org/jira/browse/HBASE-21225) | Having RPC & Space quota on a table/Namespace doesn't allow space quota to be removed using 'NONE' |  Major | . |
| [HBASE-21707](https://issues.apache.org/jira/browse/HBASE-21707) | Fix warnings in hbase-rsgroup module and also make the UTs more stable |  Major | Region Assignment, rsgroup |
| [HBASE-20220](https://issues.apache.org/jira/browse/HBASE-20220) | [RSGroup] Check if table exists in the cluster before moving it to the specified regionserver group |  Major | rsgroup |
| [HBASE-21691](https://issues.apache.org/jira/browse/HBASE-21691) | Fix flaky test TestRecoveredEdits |  Major | . |
| [HBASE-21695](https://issues.apache.org/jira/browse/HBASE-21695) | Fix flaky test TestRegionServerAbortTimeout |  Major | . |
| [HBASE-21614](https://issues.apache.org/jira/browse/HBASE-21614) | RIT recovery with ServerCrashProcedure doesn't account for all regions |  Critical | amv2 |
| [HBASE-21618](https://issues.apache.org/jira/browse/HBASE-21618) | Scan with the same startRow(inclusive=true) and stopRow(inclusive=false) returns one result |  Critical | Client |
| [HBASE-21683](https://issues.apache.org/jira/browse/HBASE-21683) | Reset readsEnabled flag after successfully flushing the primary region |  Critical | read replicas |
| [HBASE-21630](https://issues.apache.org/jira/browse/HBASE-21630) | [shell] Define ENDKEY == STOPROW (we have ENDROW) |  Trivial | shell |
| [HBASE-21547](https://issues.apache.org/jira/browse/HBASE-21547) | Precommit uses master flaky list for other branches |  Major | test |
| [HBASE-21660](https://issues.apache.org/jira/browse/HBASE-21660) | Apply the cell to right memstore for increment/append operation |  Major | . |
| [HBASE-21646](https://issues.apache.org/jira/browse/HBASE-21646) | Flakey TestTableSnapshotInputFormat; DisableTable not completing... |  Major | test |
| [HBASE-21545](https://issues.apache.org/jira/browse/HBASE-21545) | NEW\_VERSION\_BEHAVIOR breaks Get/Scan with specified columns |  Major | API |
| [HBASE-21629](https://issues.apache.org/jira/browse/HBASE-21629) | draining\_servers.rb is broken |  Major | scripts |
| [HBASE-21621](https://issues.apache.org/jira/browse/HBASE-21621) | Reversed scan does not return expected  number of rows |  Critical | Scanners |
| [HBASE-21620](https://issues.apache.org/jira/browse/HBASE-21620) | Problem in scan query when using more than one column prefix filter in some cases. |  Major | Scanners |
| [HBASE-21610](https://issues.apache.org/jira/browse/HBASE-21610) | numOpenConnections metric is set to -1 when zero server channel exist |  Minor | metrics |
| [HBASE-21498](https://issues.apache.org/jira/browse/HBASE-21498) | Master OOM when SplitTableRegionProcedure new CacheConfig and instantiate a new BlockCache |  Major | . |
| [HBASE-21592](https://issues.apache.org/jira/browse/HBASE-21592) | quota.addGetResult(r)  throw  NPE |  Major | . |
| [HBASE-21589](https://issues.apache.org/jira/browse/HBASE-21589) | TestCleanupMetaWAL fails |  Blocker | test, wal |
| [HBASE-21575](https://issues.apache.org/jira/browse/HBASE-21575) | memstore above high watermark message is logged too much |  Minor | logging, regionserver |
| [HBASE-21582](https://issues.apache.org/jira/browse/HBASE-21582) | If call HBaseAdmin#snapshotAsync but forget call isSnapshotFinished, then SnapshotHFileCleaner will skip to run every time |  Major | . |
| [HBASE-21568](https://issues.apache.org/jira/browse/HBASE-21568) | Disable use of BlockCache for LoadIncrementalHFiles |  Major | Client |
| [HBASE-21453](https://issues.apache.org/jira/browse/HBASE-21453) | Convert ReadOnlyZKClient to DEBUG instead of INFO |  Major | logging, Zookeeper |
| [HBASE-21559](https://issues.apache.org/jira/browse/HBASE-21559) | The RestoreSnapshotFromClientTestBase related UT are flaky |  Major | . |
| [HBASE-21551](https://issues.apache.org/jira/browse/HBASE-21551) | Memory leak when use scan with STREAM at server side |  Blocker | regionserver |
| [HBASE-21550](https://issues.apache.org/jira/browse/HBASE-21550) | Add a new method preCreateTableRegionInfos for MasterObserver which allows CPs to modify the TableDescriptor |  Major | Coprocessors |
| [HBASE-21479](https://issues.apache.org/jira/browse/HBASE-21479) | Individual tests in TestHRegionReplayEvents class are failing |  Major | . |
| [HBASE-21518](https://issues.apache.org/jira/browse/HBASE-21518) | TestMasterFailoverWithProcedures is flaky |  Major | . |
| [HBASE-21504](https://issues.apache.org/jira/browse/HBASE-21504) | If enable FIFOCompactionPolicy, a compaction may write a "empty" hfile whose maxTimeStamp is long max. This kind of hfile will never be archived. |  Critical | Compaction |
| [HBASE-21300](https://issues.apache.org/jira/browse/HBASE-21300) | Fix the wrong reference file path when restoring snapshots for tables with MOB columns |  Major | . |
| [HBASE-21507](https://issues.apache.org/jira/browse/HBASE-21507) | Compaction failed when execute AbstractMultiFileWriter.beforeShipped() method |  Major | Compaction, regionserver |
| [HBASE-21387](https://issues.apache.org/jira/browse/HBASE-21387) | Race condition surrounding in progress snapshot handling in snapshot cache leads to loss of snapshot files |  Major | snapshots |
| [HBASE-21503](https://issues.apache.org/jira/browse/HBASE-21503) | Replication normal source can get stuck due potential race conditions between source wal reader and wal provider initialization threads. |  Blocker | Replication |
| [HBASE-21466](https://issues.apache.org/jira/browse/HBASE-21466) | WALProcedureStore uses wrong FileSystem if wal.dir is not under rootdir |  Major | . |
| [HBASE-21445](https://issues.apache.org/jira/browse/HBASE-21445) | CopyTable by bulkload will write hfile into yarn's HDFS |  Major | mapreduce |
| [HBASE-21437](https://issues.apache.org/jira/browse/HBASE-21437) | Bypassed procedure throw IllegalArgumentException when its state is WAITING\_TIMEOUT |  Major | . |
| [HBASE-21439](https://issues.apache.org/jira/browse/HBASE-21439) | StochasticLoadBalancer RegionLoads aren’t being used in RegionLoad cost functions |  Major | Balancer |
| [HBASE-20604](https://issues.apache.org/jira/browse/HBASE-20604) | ProtobufLogReader#readNext can incorrectly loop to the same position in the stream until the the WAL is rolled |  Critical | Replication, wal |
| [HBASE-21247](https://issues.apache.org/jira/browse/HBASE-21247) | Custom Meta WAL Provider doesn't default to custom WAL Provider whose configuration value is outside the enums in Providers |  Major | wal |
| [HBASE-21430](https://issues.apache.org/jira/browse/HBASE-21430) | [hbase-connectors] Move hbase-spark\* modules to hbase-connectors repo |  Major | hbase-connectors, spark |
| [HBASE-21438](https://issues.apache.org/jira/browse/HBASE-21438) | TestAdmin2#testGetProcedures fails due to FailedProcedure inaccessible |  Major | . |
| [HBASE-21425](https://issues.apache.org/jira/browse/HBASE-21425) | 2.1.1 fails to start over 1.x data; namespace not assigned |  Critical | amv2 |
| [HBASE-21407](https://issues.apache.org/jira/browse/HBASE-21407) | Resolve NPE in backup Master UI |  Minor | UI |
| [HBASE-21422](https://issues.apache.org/jira/browse/HBASE-21422) | NPE in TestMergeTableRegionsProcedure.testMergeWithoutPONR |  Major | proc-v2, test |
| [HBASE-21424](https://issues.apache.org/jira/browse/HBASE-21424) | Change flakies and nightlies so scheduled less often |  Major | build |
| [HBASE-21417](https://issues.apache.org/jira/browse/HBASE-21417) | Pre commit build is broken due to surefire plugin crashes |  Critical | build |
| [HBASE-21371](https://issues.apache.org/jira/browse/HBASE-21371) | Hbase unable to compile against Hadoop trunk (3.3.0-SNAPSHOT) due to license error |  Major | . |
| [HBASE-21391](https://issues.apache.org/jira/browse/HBASE-21391) | RefreshPeerProcedure should also wait master initialized before executing |  Major | Replication |
| [HBASE-21342](https://issues.apache.org/jira/browse/HBASE-21342) | FileSystem in use may get closed by other bulk load call  in secure bulkLoad |  Major | . |
| [HBASE-21349](https://issues.apache.org/jira/browse/HBASE-21349) | Cluster is going down but CatalogJanitor and Normalizer try to run and fail noisely |  Minor | . |
| [HBASE-21356](https://issues.apache.org/jira/browse/HBASE-21356) | bulkLoadHFile API should ensure that rs has the source hfile's write permission |  Major | . |
| [HBASE-21355](https://issues.apache.org/jira/browse/HBASE-21355) | HStore's storeSize is calculated repeatedly which causing the confusing region split |  Blocker | regionserver |
| [HBASE-21334](https://issues.apache.org/jira/browse/HBASE-21334) | TestMergeTableRegionsProcedure is flakey |  Major | amv2, proc-v2, test |
| [HBASE-21178](https://issues.apache.org/jira/browse/HBASE-21178) | [BC break] : Get and Scan operation with a custom converter\_class not working |  Critical | shell |
| [HBASE-21200](https://issues.apache.org/jira/browse/HBASE-21200) | Memstore flush doesn't finish because of seekToPreviousRow() in memstore scanner. |  Critical | Scanners |
| [HBASE-21292](https://issues.apache.org/jira/browse/HBASE-21292) | IdLock.getLockEntry() may hang if interrupted |  Major | . |
| [HBASE-21335](https://issues.apache.org/jira/browse/HBASE-21335) | Change the default wait time of HBCK2 tool |  Critical | . |
| [HBASE-21291](https://issues.apache.org/jira/browse/HBASE-21291) | Add a test for bypassing stuck state-machine procedures |  Major | . |
| [HBASE-21055](https://issues.apache.org/jira/browse/HBASE-21055) | NullPointerException when balanceOverall() but server balance info is null |  Major | Balancer |
| [HBASE-21327](https://issues.apache.org/jira/browse/HBASE-21327) | Fix minor logging issue where we don't report servername if no associated SCP |  Trivial | amv2 |
| [HBASE-21320](https://issues.apache.org/jira/browse/HBASE-21320) | [canary] Cleanup of usage and add commentary |  Major | canary |
| [HBASE-21266](https://issues.apache.org/jira/browse/HBASE-21266) | Not running balancer because processing dead regionservers, but empty dead rs list |  Major | . |
| [HBASE-21260](https://issues.apache.org/jira/browse/HBASE-21260) | The whole balancer plans might be aborted if there are more than one plans to move a same region |  Major | Balancer, master |
| [HBASE-21280](https://issues.apache.org/jira/browse/HBASE-21280) | Add anchors for each heading in UI |  Trivial | UI, Usability |
| [HBASE-20764](https://issues.apache.org/jira/browse/HBASE-20764) | build broken when latest commit is gpg signed |  Critical | build |
| [HBASE-18549](https://issues.apache.org/jira/browse/HBASE-18549) | Unclaimed replication queues can go undetected |  Critical | Replication |
| [HBASE-21248](https://issues.apache.org/jira/browse/HBASE-21248) | Implement exponential backoff when retrying for ModifyPeerProcedure |  Major | proc-v2, Replication |
| [HBASE-21196](https://issues.apache.org/jira/browse/HBASE-21196) | HTableMultiplexer clears the meta cache after every put operation |  Critical | Performance |
| [HBASE-19418](https://issues.apache.org/jira/browse/HBASE-19418) | RANGE\_OF\_DELAY in PeriodicMemstoreFlusher should be configurable. |  Minor | . |
| [HBASE-18451](https://issues.apache.org/jira/browse/HBASE-18451) | PeriodicMemstoreFlusher should inspect the queue before adding a delayed flush request |  Major | regionserver |
| [HBASE-21228](https://issues.apache.org/jira/browse/HBASE-21228) | Memory leak since AbstractFSWAL caches Thread object and never clean later |  Critical | wal |
| [HBASE-20766](https://issues.apache.org/jira/browse/HBASE-20766) | Verify Replication Tool Has Typo "remove cluster" |  Trivial | . |
| [HBASE-21232](https://issues.apache.org/jira/browse/HBASE-21232) | Show table state in Tables view on Master home page |  Major | Operability, UI |
| [HBASE-21212](https://issues.apache.org/jira/browse/HBASE-21212) | Wrong flush time when update flush metric |  Minor | . |
| [HBASE-21208](https://issues.apache.org/jira/browse/HBASE-21208) | Bytes#toShort doesn't work without unsafe |  Critical | . |
| [HBASE-20704](https://issues.apache.org/jira/browse/HBASE-20704) | Sometimes some compacted storefiles are not archived on region close |  Critical | Compaction |
| [HBASE-21203](https://issues.apache.org/jira/browse/HBASE-21203) | TestZKMainServer#testCommandLineWorks won't pass with default 4lw whitelist |  Minor | test, Zookeeper |
| [HBASE-21102](https://issues.apache.org/jira/browse/HBASE-21102) | ServerCrashProcedure should select target server where no other replicas exist for the current region |  Major | Region Assignment |
| [HBASE-21206](https://issues.apache.org/jira/browse/HBASE-21206) | Scan with batch size may return incomplete cells |  Critical | Scanners |
| [HBASE-21182](https://issues.apache.org/jira/browse/HBASE-21182) | Failed to execute start-hbase.sh |  Major | . |
| [HBASE-21179](https://issues.apache.org/jira/browse/HBASE-21179) | Fix the number of actions in responseTooSlow log |  Major | logging, rpc |
| [HBASE-21174](https://issues.apache.org/jira/browse/HBASE-21174) | [REST] Failed to parse empty qualifier in TableResource#getScanResource |  Major | REST |
| [HBASE-21181](https://issues.apache.org/jira/browse/HBASE-21181) | Use the same filesystem for wal archive directory and wal directory |  Major | . |
| [HBASE-21021](https://issues.apache.org/jira/browse/HBASE-21021) | Result returned by Append operation should be ordered |  Major | . |
| [HBASE-21173](https://issues.apache.org/jira/browse/HBASE-21173) | Remove the duplicate HRegion#close in TestHRegion |  Minor | test |
| [HBASE-21144](https://issues.apache.org/jira/browse/HBASE-21144) | AssignmentManager.waitForAssignment is not stable |  Major | amv2, test |
| [HBASE-21143](https://issues.apache.org/jira/browse/HBASE-21143) | Update findbugs-maven-plugin to 3.0.4 |  Major | pom |
| [HBASE-21171](https://issues.apache.org/jira/browse/HBASE-21171) | [amv2] Tool to parse a directory of MasterProcWALs standalone |  Major | amv2, test |
| [HBASE-21052](https://issues.apache.org/jira/browse/HBASE-21052) | After restoring a snapshot, table.jsp page for the table gets stuck |  Major | snapshots |
| [HBASE-21001](https://issues.apache.org/jira/browse/HBASE-21001) | ReplicationObserver fails to load in HBase 2.0.0 |  Major | . |
| [HBASE-20741](https://issues.apache.org/jira/browse/HBASE-20741) | Split of a region with replicas creates all daughter regions and its replica in same server |  Major | read replicas |
| [HBASE-21127](https://issues.apache.org/jira/browse/HBASE-21127) | TableRecordReader need to handle cursor result too |  Major | . |
| [HBASE-20892](https://issues.apache.org/jira/browse/HBASE-20892) | [UI] Start / End keys are empty on table.jsp |  Major | . |
| [HBASE-21136](https://issues.apache.org/jira/browse/HBASE-21136) | NPE in MetricsTableSourceImpl.updateFlushTime |  Major | metrics |
| [HBASE-21132](https://issues.apache.org/jira/browse/HBASE-21132) | return wrong result in rest multiget |  Major | . |
| [HBASE-21128](https://issues.apache.org/jira/browse/HBASE-21128) | TestAsyncRegionAdminApi.testAssignRegionAndUnassignRegion is broken |  Major | test |
| [HBASE-20940](https://issues.apache.org/jira/browse/HBASE-20940) | HStore.cansplit should not allow split to happen if it has references |  Major | . |
| [HBASE-21084](https://issues.apache.org/jira/browse/HBASE-21084) | When cloning a snapshot including a split parent region, the split parent region of the cloned table will be online |  Major | snapshots |
| [HBASE-20968](https://issues.apache.org/jira/browse/HBASE-20968) | list\_procedures\_test fails due to no matching regex |  Major | shell, test |
| [HBASE-21030](https://issues.apache.org/jira/browse/HBASE-21030) | Correct javadoc for append operation |  Minor | documentation |
| [HBASE-21088](https://issues.apache.org/jira/browse/HBASE-21088) | HStoreFile should be closed in HStore#hasReferences |  Major | . |
| [HBASE-20890](https://issues.apache.org/jira/browse/HBASE-20890) | PE filterScan seems to be stuck forever |  Minor | . |
| [HBASE-20772](https://issues.apache.org/jira/browse/HBASE-20772) | Controlled shutdown fills Master log with the disturbing message "No matching procedure found for rit=OPEN, location=ZZZZ, table=YYYYY, region=XXXX transition to CLOSED |  Major | logging |
| [HBASE-20978](https://issues.apache.org/jira/browse/HBASE-20978) | [amv2] Worker terminating UNNATURALLY during MoveRegionProcedure |  Critical | amv2 |
| [HBASE-21078](https://issues.apache.org/jira/browse/HBASE-21078) | [amv2] CODE-BUG NPE in RTP doing Unassign |  Major | amv2 |
| [HBASE-21113](https://issues.apache.org/jira/browse/HBASE-21113) | Apply the branch-2 version of HBASE-21095, The timeout retry logic for several procedures are broken after master restarts |  Major | amv2 |
| [HBASE-21101](https://issues.apache.org/jira/browse/HBASE-21101) | Remove the waitUntilAllRegionsAssigned call after split in TestTruncateTableProcedure |  Major | test |
| [HBASE-19008](https://issues.apache.org/jira/browse/HBASE-19008) | Add missing equals or hashCode method(s) to stock Filter implementations |  Major | . |
| [HBASE-20614](https://issues.apache.org/jira/browse/HBASE-20614) | REST scan API with incorrect filter text file throws HTTP 503 Service Unavailable error |  Minor | REST |
| [HBASE-21041](https://issues.apache.org/jira/browse/HBASE-21041) | Memstore's heap size will be decreased to minus zero after flush |  Major | . |
| [HBASE-21031](https://issues.apache.org/jira/browse/HBASE-21031) | Memory leak if replay edits failed during region opening |  Major | . |
| [HBASE-20666](https://issues.apache.org/jira/browse/HBASE-20666) | Unsuccessful table creation leaves entry in hbase:rsgroup table |  Minor | . |
| [HBASE-21032](https://issues.apache.org/jira/browse/HBASE-21032) | ScanResponses contain only one cell each |  Major | Performance, Scanners |
| [HBASE-20705](https://issues.apache.org/jira/browse/HBASE-20705) | Having RPC Quota on a table prevents Space quota to be recreated/removed |  Major | . |
| [HBASE-21058](https://issues.apache.org/jira/browse/HBASE-21058) | Nightly tests for branches 1 fail to build ref guide |  Major | documentation |
| [HBASE-21074](https://issues.apache.org/jira/browse/HBASE-21074) | JDK7 branches need to pass "-Dhttps.protocols=TLSv1.2" to maven when building |  Major | build, community, test |
| [HBASE-21062](https://issues.apache.org/jira/browse/HBASE-21062) | WALFactory has misleading notion of "default" |  Major | wal |
| [HBASE-21047](https://issues.apache.org/jira/browse/HBASE-21047) | Object creation of StoreFileScanner thru constructor and close may leave refCount to -1 |  Major | . |
| [HBASE-21005](https://issues.apache.org/jira/browse/HBASE-21005) | Maven site configuration causes downstream projects to get a directory named ${project.basedir} |  Minor | build |
| [HBASE-21007](https://issues.apache.org/jira/browse/HBASE-21007) | Memory leak in HBase rest server |  Critical | REST |
| [HBASE-20794](https://issues.apache.org/jira/browse/HBASE-20794) | CreateTable operation does not log its landing at the master nor the initiator at INFO level |  Major | logging |
| [HBASE-20538](https://issues.apache.org/jira/browse/HBASE-20538) | Upgrade our hadoop versions to 2.7.7 and 3.0.3 |  Critical | java, security |
| [HBASE-20927](https://issues.apache.org/jira/browse/HBASE-20927) | RSGroupAdminEndpoint doesn't handle clearing dead servers if they are not processed yet. |  Major | . |
| [HBASE-20932](https://issues.apache.org/jira/browse/HBASE-20932) | Effective MemStoreSize::hashCode() |  Major | . |
| [HBASE-20928](https://issues.apache.org/jira/browse/HBASE-20928) | Rewrite calculation of midpoint in binarySearch functions to prevent overflow |  Minor | io |
| [HBASE-20565](https://issues.apache.org/jira/browse/HBASE-20565) | ColumnRangeFilter combined with ColumnPaginationFilter can produce incorrect result since 1.4 |  Major | Filters |
| [HBASE-20908](https://issues.apache.org/jira/browse/HBASE-20908) | Infinite loop on regionserver if region replica are reduced |  Major | read replicas |
| [HBASE-19893](https://issues.apache.org/jira/browse/HBASE-19893) | restore\_snapshot is broken in master branch when region splits |  Critical | snapshots |
| [HBASE-20870](https://issues.apache.org/jira/browse/HBASE-20870) | Wrong HBase root dir in ITBLL's Search Tool |  Minor | integration tests |
| [HBASE-20901](https://issues.apache.org/jira/browse/HBASE-20901) | Reducing region replica has no effect |  Major | . |
| [HBASE-6028](https://issues.apache.org/jira/browse/HBASE-6028) | Implement a cancel for in-progress compactions |  Minor | regionserver |
| [HBASE-20869](https://issues.apache.org/jira/browse/HBASE-20869) | Endpoint-based Export use incorrect user to write to destination |  Major | Coprocessors |
| [HBASE-20879](https://issues.apache.org/jira/browse/HBASE-20879) | Compacting memstore config should handle lower case |  Major | . |
| [HBASE-20865](https://issues.apache.org/jira/browse/HBASE-20865) | CreateTableProcedure is stuck in retry loop in CREATE\_TABLE\_WRITE\_FS\_LAYOUT state |  Major | amv2 |
| [HBASE-19572](https://issues.apache.org/jira/browse/HBASE-19572) | RegionMover should use the configured default port number and not the one from HConstants |  Major | . |
| [HBASE-20697](https://issues.apache.org/jira/browse/HBASE-20697) | Can't cache All region locations of the specify table by calling table.getRegionLocator().getAllRegionLocations() |  Major | meta |
| [HBASE-20791](https://issues.apache.org/jira/browse/HBASE-20791) | RSGroupBasedLoadBalancer#setClusterMetrics should pass ClusterMetrics to its internalBalancer |  Major | Balancer, rsgroup |
| [HBASE-20770](https://issues.apache.org/jira/browse/HBASE-20770) | WAL cleaner logs way too much; gets clogged when lots of work to do |  Critical | logging |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21963](https://issues.apache.org/jira/browse/HBASE-21963) | Add a script for building and verifying release candidate |  Minor | community, scripts |
| [HBASE-21756](https://issues.apache.org/jira/browse/HBASE-21756) | Backport HBASE-21279 (Split TestAdminShell into several tests) to branch-2 |  Major | . |
| [HBASE-20136](https://issues.apache.org/jira/browse/HBASE-20136) | TestKeyValue misses ClassRule and Category annotations |  Minor | . |
| [HBASE-21261](https://issues.apache.org/jira/browse/HBASE-21261) | Add log4j.properties for hbase-rsgroup tests |  Trivial | . |
| [HBASE-21258](https://issues.apache.org/jira/browse/HBASE-21258) | Add resetting of flags for RS Group pre/post hooks in TestRSGroups |  Major | . |
| [HBASE-21097](https://issues.apache.org/jira/browse/HBASE-21097) | Flush pressure assertion may fail in testFlushThroughputTuning |  Major | regionserver |
| [HBASE-21138](https://issues.apache.org/jira/browse/HBASE-21138) | Close HRegion instance at the end of every test in TestHRegion |  Major | . |
| [HBASE-21161](https://issues.apache.org/jira/browse/HBASE-21161) | Enable the test added in HBASE-20741 that was removed accidentally |  Minor | . |
| [HBASE-21076](https://issues.apache.org/jira/browse/HBASE-21076) | TestTableResource fails with NPE |  Major | REST, test |
| [HBASE-20907](https://issues.apache.org/jira/browse/HBASE-20907) | Fix Intermittent failure on TestProcedurePriority |  Major | . |
| [HBASE-20838](https://issues.apache.org/jira/browse/HBASE-20838) | Include hbase-server in precommit test if CommonFSUtils is changed |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21970](https://issues.apache.org/jira/browse/HBASE-21970) | Document that how to upgrade from 2.0 or 2.1 to 2.2+ |  Major | . |
| [HBASE-22357](https://issues.apache.org/jira/browse/HBASE-22357) | Fix remaining Checkstyle issues in hbase-replication |  Trivial | Replication |
| [HBASE-22554](https://issues.apache.org/jira/browse/HBASE-22554) | Upgrade to surefire 2.22.2 |  Major | test |
| [HBASE-22500](https://issues.apache.org/jira/browse/HBASE-22500) | Modify pom and jenkins jobs for hadoop versions |  Blocker | build, hadoop2, hadoop3 |
| [HBASE-22316](https://issues.apache.org/jira/browse/HBASE-22316) | Record the stack trace for current thread in FutureUtils.get |  Major | asyncclient, Client |
| [HBASE-22326](https://issues.apache.org/jira/browse/HBASE-22326) | Fix Checkstyle errors in hbase-examples |  Minor | . |
| [HBASE-22327](https://issues.apache.org/jira/browse/HBASE-22327) | Fix remaining Checkstyle issues in hbase-hadoop-compat |  Trivial | . |
| [HBASE-22478](https://issues.apache.org/jira/browse/HBASE-22478) | Add jackson dependency for hbase-http module |  Major | build, dependencies |
| [HBASE-22445](https://issues.apache.org/jira/browse/HBASE-22445) | Add file info when throw exceptions in HFileReaderImpl |  Major | . |
| [HBASE-22447](https://issues.apache.org/jira/browse/HBASE-22447) | Check refCount before free block in BucketCache |  Major | BucketCache |
| [HBASE-22400](https://issues.apache.org/jira/browse/HBASE-22400) | Remove the adapter code in async fs implementation for hadoop-2.7.x |  Major | wal |
| [HBASE-22430](https://issues.apache.org/jira/browse/HBASE-22430) | hbase-vote should tee build and test output to console |  Trivial | . |
| [HBASE-22429](https://issues.apache.org/jira/browse/HBASE-22429) | hbase-vote download step requires URL to end with '/' |  Trivial | . |
| [HBASE-22405](https://issues.apache.org/jira/browse/HBASE-22405) | Update Ref Guide for EOL of Hadoop 2.7 |  Major | community, documentation |
| [HBASE-22325](https://issues.apache.org/jira/browse/HBASE-22325) | AsyncRpcRetryingCaller will not schedule retry if we hit a NotServingRegionException but there is no TableName provided |  Major | asyncclient, Client |
| [HBASE-22322](https://issues.apache.org/jira/browse/HBASE-22322) | Use special pause for CallQueueTooBigException |  Major | asyncclient, Client |
| [HBASE-22317](https://issues.apache.org/jira/browse/HBASE-22317) | Support reading from meta replicas |  Major | asyncclient, read replicas |
| [HBASE-22261](https://issues.apache.org/jira/browse/HBASE-22261) | Make use of ClusterStatusListener for async client |  Major | asyncclient |
| [HBASE-22267](https://issues.apache.org/jira/browse/HBASE-22267) | Implement client push back for async client |  Major | asyncclient |
| [HBASE-19763](https://issues.apache.org/jira/browse/HBASE-19763) | Fix Checkstyle errors in hbase-procedure |  Minor | . |
| [HBASE-22244](https://issues.apache.org/jira/browse/HBASE-22244) | Make use of MetricsConnection in async client |  Major | asyncclient, metrics |
| [HBASE-22196](https://issues.apache.org/jira/browse/HBASE-22196) | Split TestRestartCluster |  Major | test |
| [HBASE-22117](https://issues.apache.org/jira/browse/HBASE-22117) | Move hasPermission/checkPermissions from region server to master |  Major | . |
| [HBASE-21886](https://issues.apache.org/jira/browse/HBASE-21886) | Run ITBLL for branch-2.2 |  Major | . |
| [HBASE-22155](https://issues.apache.org/jira/browse/HBASE-22155) | Move 2.2.0 on to hbase-thirdparty-2.2.0 |  Major | thirdparty |
| [HBASE-22153](https://issues.apache.org/jira/browse/HBASE-22153) | Fix the flaky TestRestartCluster |  Major | test |
| [HBASE-22152](https://issues.apache.org/jira/browse/HBASE-22152) | Create a jenkins file for yetus to processing GitHub PR |  Major | build |
| [HBASE-22158](https://issues.apache.org/jira/browse/HBASE-22158) | RawAsyncHBaseAdmin.getTableSplits should filter out none default replicas |  Major | Admin |
| [HBASE-22157](https://issues.apache.org/jira/browse/HBASE-22157) | Include the cause when constructing RestoreSnapshotException in restoreSnapshot |  Major | Admin |
| [HBASE-22141](https://issues.apache.org/jira/browse/HBASE-22141) | Fix TestAsyncDecommissionAdminApi |  Major | test |
| [HBASE-22135](https://issues.apache.org/jira/browse/HBASE-22135) | AsyncAdmin will not refresh master address |  Major | Admin, asyncclient, Client |
| [HBASE-22101](https://issues.apache.org/jira/browse/HBASE-22101) | AsyncAdmin.isTableAvailable should not throw TableNotFoundException |  Major | Admin, asyncclient, Client |
| [HBASE-22094](https://issues.apache.org/jira/browse/HBASE-22094) | Throw TableNotFoundException if table not exists in AsyncAdmin.compact |  Major | Admin |
| [HBASE-21911](https://issues.apache.org/jira/browse/HBASE-21911) | Move getUserPermissions from regionserver to master |  Major | . |
| [HBASE-22015](https://issues.apache.org/jira/browse/HBASE-22015) | UserPermission should be annotated as InterfaceAudience.Public |  Blocker | . |
| [HBASE-22066](https://issues.apache.org/jira/browse/HBASE-22066) | Add markers to CHANGES.md and RELEASENOTES.md |  Major | . |
| [HBASE-22040](https://issues.apache.org/jira/browse/HBASE-22040) | Add mergeRegionsAsync with a List of region names method in AsyncAdmin |  Major | Admin, asyncclient, Client |
| [HBASE-22039](https://issues.apache.org/jira/browse/HBASE-22039) | Should add the synchronous parameter for the XXXSwitch method in AsyncAdmin |  Major | Admin, asyncclient, Client |
| [HBASE-22022](https://issues.apache.org/jira/browse/HBASE-22022) | nightly fails rat check down in the dev-support/hbase\_nightly\_source-artifact.sh check |  Major | . |
| [HBASE-22025](https://issues.apache.org/jira/browse/HBASE-22025) | RAT check fails in nightlies; fails on (old) test data files. |  Major | . |
| [HBASE-21977](https://issues.apache.org/jira/browse/HBASE-21977) | Skip replay WAL and update seqid when open regions restored from snapshot |  Major | Region Assignment, snapshots |
| [HBASE-21999](https://issues.apache.org/jira/browse/HBASE-21999) | [DEBUG] Exit if git returns empty revision! |  Major | build |
| [HBASE-22000](https://issues.apache.org/jira/browse/HBASE-22000) | Deprecated isTableAvailable with splitKeys |  Major | asyncclient, Client |
| [HBASE-21949](https://issues.apache.org/jira/browse/HBASE-21949) | Fix flaky test TestHBaseTestingUtility.testMiniZooKeeperWithMultipleClientPorts |  Major | . |
| [HBASE-21993](https://issues.apache.org/jira/browse/HBASE-21993) | Set version as 2.2.0 in branch-2.2 in prep for first RC |  Major | . |
| [HBASE-21997](https://issues.apache.org/jira/browse/HBASE-21997) | Fix hbase-rest findbugs ST\_WRITE\_TO\_STATIC\_FROM\_INSTANCE\_METHOD complaint |  Major | REST |
| [HBASE-21986](https://issues.apache.org/jira/browse/HBASE-21986) | Generate CHANGES.md and RELEASENOTES.md for 2.2.0 |  Major | . |
| [HBASE-21972](https://issues.apache.org/jira/browse/HBASE-21972) | Copy master doc into branch-2.2 and edit to make it suit 2.2.0 |  Major | . |
| [HBASE-15728](https://issues.apache.org/jira/browse/HBASE-15728) | Add remaining per-table region / store / flush / compaction related metrics |  Major | metrics |
| [HBASE-21934](https://issues.apache.org/jira/browse/HBASE-21934) | RemoteProcedureDispatcher should track the ongoing dispatched calls |  Blocker | proc-v2 |
| [HBASE-21588](https://issues.apache.org/jira/browse/HBASE-21588) | Procedure v2 wal splitting implementation |  Major | . |
| [HBASE-21729](https://issues.apache.org/jira/browse/HBASE-21729) | Extract ProcedureCoordinatorRpcs and ProcedureMemberRpcs from CoordinatedStateManager |  Major | . |
| [HBASE-21094](https://issues.apache.org/jira/browse/HBASE-21094) | Remove the explicit timeout config for TestTruncateTableProcedure |  Major | test |
| [HBASE-21093](https://issues.apache.org/jira/browse/HBASE-21093) | Move TestCreateTableProcedure.testMRegions to a separated file |  Major | test |
| [HBASE-18201](https://issues.apache.org/jira/browse/HBASE-18201) | add UT and docs for DataBlockEncodingTool |  Minor | tooling |
| [HBASE-21978](https://issues.apache.org/jira/browse/HBASE-21978) | Should close AsyncRegistry if we fail to get cluster id when creating AsyncConnection |  Major | asyncclient, Client |
| [HBASE-21974](https://issues.apache.org/jira/browse/HBASE-21974) | Change Admin#grant/revoke parameter from UserPermission to user and Permission |  Major | . |
| [HBASE-21976](https://issues.apache.org/jira/browse/HBASE-21976) | Deal with RetryImmediatelyException for batching request |  Major | asyncclient, Client |
| [HBASE-21820](https://issues.apache.org/jira/browse/HBASE-21820) | Implement CLUSTER quota scope |  Major | . |
| [HBASE-21962](https://issues.apache.org/jira/browse/HBASE-21962) | Filters do not work in ThriftTable |  Major | Thrift |
| [HBASE-21927](https://issues.apache.org/jira/browse/HBASE-21927) | Always fail the locate request when error occur |  Major | asyncclient, Client |
| [HBASE-21944](https://issues.apache.org/jira/browse/HBASE-21944) | Validate put for batch operation |  Major | asyncclient, Client |
| [HBASE-21945](https://issues.apache.org/jira/browse/HBASE-21945) | Maintain the original order when sending batch request |  Major | asyncclient, Client |
| [HBASE-21783](https://issues.apache.org/jira/browse/HBASE-21783) | Support exceed user/table/ns throttle quota if region server has available quota |  Major | . |
| [HBASE-21930](https://issues.apache.org/jira/browse/HBASE-21930) | Deal with ScannerResetException when opening region scanner |  Major | asyncclient, Client |
| [HBASE-21907](https://issues.apache.org/jira/browse/HBASE-21907) | Should set priority for rpc request |  Major | asyncclient, Client |
| [HBASE-21909](https://issues.apache.org/jira/browse/HBASE-21909) | Validate the put instance before executing in AsyncTable.put method |  Major | asyncclient, Client |
| [HBASE-21814](https://issues.apache.org/jira/browse/HBASE-21814) | Remove the TODO in AccessControlLists#addUserPermission |  Major | . |
| [HBASE-19889](https://issues.apache.org/jira/browse/HBASE-19889) | Revert Workaround: Purge User API building from branch-2 so can make a beta-1 |  Major | website |
| [HBASE-21838](https://issues.apache.org/jira/browse/HBASE-21838) | Create a special ReplicationEndpoint just for verifying the WAL entries are fine |  Major | Replication, wal |
| [HBASE-21829](https://issues.apache.org/jira/browse/HBASE-21829) | Use FutureUtils.addListener instead of calling whenComplete directly |  Major | asyncclient, Client |
| [HBASE-21828](https://issues.apache.org/jira/browse/HBASE-21828) | Make sure we do not return CompletionException when locating region |  Major | asyncclient, Client |
| [HBASE-21764](https://issues.apache.org/jira/browse/HBASE-21764) | Size of in-memory compaction thread pool should be configurable |  Major | Compaction, in-memory-compaction |
| [HBASE-21809](https://issues.apache.org/jira/browse/HBASE-21809) | Add retry thrift client for ThriftTable/Admin |  Major | . |
| [HBASE-21739](https://issues.apache.org/jira/browse/HBASE-21739) | Move grant/revoke from regionserver to master |  Major | . |
| [HBASE-21798](https://issues.apache.org/jira/browse/HBASE-21798) | Cut branch-2.2 |  Major | . |
| [HBASE-20542](https://issues.apache.org/jira/browse/HBASE-20542) | Better heap utilization for IMC with MSLABs |  Major | in-memory-compaction |
| [HBASE-21713](https://issues.apache.org/jira/browse/HBASE-21713) | Support set region server throttle quota |  Major | . |
| [HBASE-21761](https://issues.apache.org/jira/browse/HBASE-21761) | Align the methods in RegionLocator and AsyncTableRegionLocator |  Major | asyncclient, Client |
| [HBASE-17370](https://issues.apache.org/jira/browse/HBASE-17370) | Fix or provide shell scripts to drain and decommission region server |  Major | . |
| [HBASE-21750](https://issues.apache.org/jira/browse/HBASE-21750) | Most of KeyValueUtil#length can be replaced by cell#getSerializedSize for better performance because the latter one has been optimized |  Major | . |
| [HBASE-21734](https://issues.apache.org/jira/browse/HBASE-21734) | Some optimization in FilterListWithOR |  Major | . |
| [HBASE-21738](https://issues.apache.org/jira/browse/HBASE-21738) | Remove all the CSLM#size operation in our memstore because it's an quite time consuming. |  Critical | Performance |
| [HBASE-21034](https://issues.apache.org/jira/browse/HBASE-21034) | Add new throttle type: read/write capacity unit |  Major | . |
| [HBASE-21726](https://issues.apache.org/jira/browse/HBASE-21726) | Add getAllRegionLocations method to AsyncTableRegionLocator |  Major | asyncclient, Client |
| [HBASE-19695](https://issues.apache.org/jira/browse/HBASE-19695) | Handle disabled table for async client |  Major | asyncclient, Client |
| [HBASE-21711](https://issues.apache.org/jira/browse/HBASE-21711) | Remove references to git.apache.org/hbase.git |  Critical | . |
| [HBASE-21647](https://issues.apache.org/jira/browse/HBASE-21647) | Add status track for splitting WAL tasks |  Major | Operability |
| [HBASE-21705](https://issues.apache.org/jira/browse/HBASE-21705) | Should treat meta table specially for some methods in AsyncAdmin |  Major | Admin, asyncclient, Client |
| [HBASE-21663](https://issues.apache.org/jira/browse/HBASE-21663) | Add replica scan support |  Major | asyncclient, Client, read replicas |
| [HBASE-21580](https://issues.apache.org/jira/browse/HBASE-21580) | Support getting Hbck instance from AsyncConnection |  Major | asyncclient, Client, hbck2 |
| [HBASE-21652](https://issues.apache.org/jira/browse/HBASE-21652) | Refactor ThriftServer making thrift2 server inherited from thrift1 server |  Major | . |
| [HBASE-21661](https://issues.apache.org/jira/browse/HBASE-21661) | Provide Thrift2 implementation of Table/Admin |  Major | . |
| [HBASE-21682](https://issues.apache.org/jira/browse/HBASE-21682) | Support getting from specific replica |  Major | read replicas |
| [HBASE-21159](https://issues.apache.org/jira/browse/HBASE-21159) | Add shell command to switch throttle on or off |  Major | . |
| [HBASE-21362](https://issues.apache.org/jira/browse/HBASE-21362) | Disable printing of stack-trace in shell when quotas are violated |  Minor | shell |
| [HBASE-21361](https://issues.apache.org/jira/browse/HBASE-21361) | Disable printing of stack-trace in shell when quotas are not enabled |  Minor | shell |
| [HBASE-17356](https://issues.apache.org/jira/browse/HBASE-17356) | Add replica get support |  Major | Client |
| [HBASE-21650](https://issues.apache.org/jira/browse/HBASE-21650) | Add DDL operation and some other miscellaneous to thrift2 |  Major | Thrift |
| [HBASE-21401](https://issues.apache.org/jira/browse/HBASE-21401) | Sanity check when constructing the KeyValue |  Critical | regionserver |
| [HBASE-21578](https://issues.apache.org/jira/browse/HBASE-21578) | Fix wrong throttling exception for capacity unit |  Major | . |
| [HBASE-21570](https://issues.apache.org/jira/browse/HBASE-21570) | Add write buffer periodic flush support for AsyncBufferedMutator |  Major | asyncclient, Client |
| [HBASE-21465](https://issues.apache.org/jira/browse/HBASE-21465) | Retry on reportRegionStateTransition can lead to unexpected errors |  Major | amv2 |
| [HBASE-21508](https://issues.apache.org/jira/browse/HBASE-21508) | Ignore the reportRegionStateTransition call from a dead server |  Major | amv2 |
| [HBASE-21490](https://issues.apache.org/jira/browse/HBASE-21490) | WALProcedure may remove proc wal files still with active procedures |  Major | proc-v2 |
| [HBASE-21377](https://issues.apache.org/jira/browse/HBASE-21377) | Add debug log for procedure stack id related operations |  Major | proc-v2 |
| [HBASE-21472](https://issues.apache.org/jira/browse/HBASE-21472) | Should not persist the dispatched field for RegionRemoteProcedureBase |  Major | amv2 |
| [HBASE-21473](https://issues.apache.org/jira/browse/HBASE-21473) | RowIndexSeekerV1 may return cell with extra two \\x00\\x00 bytes which has no tags |  Major | . |
| [HBASE-21463](https://issues.apache.org/jira/browse/HBASE-21463) | The checkOnlineRegionsReport can accidentally complete a TRSP |  Critical | amv2 |
| [HBASE-21376](https://issues.apache.org/jira/browse/HBASE-21376) | Add some verbose log to MasterProcedureScheduler |  Major | logging, proc-v2 |
| [HBASE-21443](https://issues.apache.org/jira/browse/HBASE-21443) | [hbase-connectors] Purge hbase-\* modules from core now they've been moved to hbase-connectors |  Major | hbase-connectors, spark |
| [HBASE-21421](https://issues.apache.org/jira/browse/HBASE-21421) | Do not kill RS if reportOnlineRegions fails |  Major | . |
| [HBASE-21314](https://issues.apache.org/jira/browse/HBASE-21314) | The implementation of BitSetNode is not efficient |  Major | proc-v2 |
| [HBASE-21351](https://issues.apache.org/jira/browse/HBASE-21351) | The force update thread may have race with PE worker when the procedure is rolling back |  Critical | proc-v2 |
| [HBASE-21191](https://issues.apache.org/jira/browse/HBASE-21191) | Add a holding-pattern if no assign for meta or namespace (Can happen if masterprocwals have been cleared). |  Major | amv2 |
| [HBASE-21322](https://issues.apache.org/jira/browse/HBASE-21322) | Add a scheduleServerCrashProcedure() API to HbckService |  Critical | hbck2 |
| [HBASE-21375](https://issues.apache.org/jira/browse/HBASE-21375) | Revisit the lock and queue implementation in MasterProcedureScheduler |  Major | proc-v2 |
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
| [HBASE-21336](https://issues.apache.org/jira/browse/HBASE-21336) | Simplify the implementation of WALProcedureMap |  Major | proc-v2 |
| [HBASE-21323](https://issues.apache.org/jira/browse/HBASE-21323) | Should not skip force updating for a sub procedure even if it has been finished |  Major | proc-v2 |
| [HBASE-21269](https://issues.apache.org/jira/browse/HBASE-21269) | Forward-port to branch-2 " HBASE-21213 [hbck2] bypass leaves behind     state in RegionStates when assign/unassign" |  Major | amv2 |
| [HBASE-20716](https://issues.apache.org/jira/browse/HBASE-20716) | Unsafe access cleanup |  Critical | Performance |
| [HBASE-21330](https://issues.apache.org/jira/browse/HBASE-21330) | ReopenTableRegionsProcedure will enter an infinite loop if we schedule a TRSP at the same time |  Major | amv2 |
| [HBASE-21310](https://issues.apache.org/jira/browse/HBASE-21310) | Split TestCloneSnapshotFromClient |  Major | test |
| [HBASE-21311](https://issues.apache.org/jira/browse/HBASE-21311) | Split TestRestoreSnapshotFromClient |  Major | test |
| [HBASE-21315](https://issues.apache.org/jira/browse/HBASE-21315) | The getActiveMinProcId and getActiveMaxProcId of BitSetNode are incorrect if there are no active procedure |  Major | . |
| [HBASE-21278](https://issues.apache.org/jira/browse/HBASE-21278) | Do not rollback successful sub procedures when rolling back a procedure |  Critical | proc-v2 |
| [HBASE-21309](https://issues.apache.org/jira/browse/HBASE-21309) | Increase the waiting timeout for TestProcedurePriority |  Major | test |
| [HBASE-21254](https://issues.apache.org/jira/browse/HBASE-21254) | Need to find a way to limit the number of proc wal files |  Critical | proc-v2 |
| [HBASE-21250](https://issues.apache.org/jira/browse/HBASE-21250) | Refactor WALProcedureStore and add more comments for better understanding the implementation |  Major | proc-v2 |
| [HBASE-19275](https://issues.apache.org/jira/browse/HBASE-19275) | TestSnapshotFileCache never worked properly |  Major | . |
| [HBASE-21249](https://issues.apache.org/jira/browse/HBASE-21249) | Add jitter for ProcedureUtil.getBackoffTimeMs |  Major | proc-v2 |
| [HBASE-21244](https://issues.apache.org/jira/browse/HBASE-21244) | Skip persistence when retrying for assignment related procedures |  Major | amv2, Performance, proc-v2 |
| [HBASE-21233](https://issues.apache.org/jira/browse/HBASE-21233) | Allow the procedure implementation to skip persistence of the state after a execution |  Major | Performance, proc-v2 |
| [HBASE-21227](https://issues.apache.org/jira/browse/HBASE-21227) | Implement exponential retrying backoff for Assign/UnassignRegionHandler introduced in HBASE-21217 |  Major | amv2, regionserver |
| [HBASE-21217](https://issues.apache.org/jira/browse/HBASE-21217) | Revisit the executeProcedure method for open/close region |  Critical | amv2, proc-v2 |
| [HBASE-21214](https://issues.apache.org/jira/browse/HBASE-21214) | [hbck2] setTableState just sets hbase:meta state, not in-memory state |  Major | amv2, hbck2 |
| [HBASE-21023](https://issues.apache.org/jira/browse/HBASE-21023) | Add bypassProcedureToCompletion() API to HbckService |  Major | hbck2 |
| [HBASE-21156](https://issues.apache.org/jira/browse/HBASE-21156) | [hbck2] Queue an assign of hbase:meta and bulk assign/unassign |  Critical | hbck2 |
| [HBASE-21169](https://issues.apache.org/jira/browse/HBASE-21169) | Initiate hbck2 tool in hbase-operator-tools repo |  Major | hbck2 |
| [HBASE-21172](https://issues.apache.org/jira/browse/HBASE-21172) | Reimplement the retry backoff logic for ReopenTableRegionsProcedure |  Major | amv2, proc-v2 |
| [HBASE-21189](https://issues.apache.org/jira/browse/HBASE-21189) | flaky job should gather machine stats |  Minor | test |
| [HBASE-21190](https://issues.apache.org/jira/browse/HBASE-21190) | Log files and count of entries in each as we load from the MasterProcWAL store |  Major | amv2 |
| [HBASE-21083](https://issues.apache.org/jira/browse/HBASE-21083) | Introduce a mechanism to bypass the execution of a stuck procedure |  Major | amv2 |
| [HBASE-21017](https://issues.apache.org/jira/browse/HBASE-21017) | Revisit the expected states for open/close |  Major | amv2 |
| [HBASE-20941](https://issues.apache.org/jira/browse/HBASE-20941) | Create and implement HbckService in master |  Major | . |
| [HBASE-21072](https://issues.apache.org/jira/browse/HBASE-21072) | Block out HBCK1 in hbase2 |  Major | hbck |
| [HBASE-21095](https://issues.apache.org/jira/browse/HBASE-21095) | The timeout retry logic for several procedures are broken after master restarts |  Critical | amv2, proc-v2 |
| [HBASE-20975](https://issues.apache.org/jira/browse/HBASE-20975) | Lock may not be taken or released while rolling back procedure |  Major | amv2 |
| [HBASE-21025](https://issues.apache.org/jira/browse/HBASE-21025) | Add cache for TableStateManager |  Major | . |
| [HBASE-21012](https://issues.apache.org/jira/browse/HBASE-21012) | Revert the change of serializing TimeRangeTracker |  Critical | . |
| [HBASE-20813](https://issues.apache.org/jira/browse/HBASE-20813) | Remove RPC quotas when the associated table/Namespace is dropped off |  Minor | . |
| [HBASE-20885](https://issues.apache.org/jira/browse/HBASE-20885) | Remove entry for RPC quota from hbase:quota when RPC quota is removed. |  Minor | . |
| [HBASE-20893](https://issues.apache.org/jira/browse/HBASE-20893) | Data loss if splitting region while ServerCrashProcedure executing |  Major | . |
| [HBASE-20950](https://issues.apache.org/jira/browse/HBASE-20950) | Helper method to configure secure DFS cluster for tests |  Major | test |
| [HBASE-19369](https://issues.apache.org/jira/browse/HBASE-19369) | HBase Should use Builder Pattern to Create Log Files while using WAL on Erasure Coding |  Major | . |
| [HBASE-20939](https://issues.apache.org/jira/browse/HBASE-20939) | There will be race when we call suspendIfNotReady and then throw ProcedureSuspendedException |  Critical | amv2 |
| [HBASE-20921](https://issues.apache.org/jira/browse/HBASE-20921) | Possible NPE in ReopenTableRegionsProcedure |  Major | amv2 |
| [HBASE-20867](https://issues.apache.org/jira/browse/HBASE-20867) | RS may get killed while master restarts |  Major | . |
| [HBASE-20878](https://issues.apache.org/jira/browse/HBASE-20878) | Data loss if merging regions while ServerCrashProcedure executing |  Critical | amv2 |
| [HBASE-20846](https://issues.apache.org/jira/browse/HBASE-20846) | Restore procedure locks when master restarts |  Major | . |
| [HBASE-20914](https://issues.apache.org/jira/browse/HBASE-20914) | Trim Master memory usage |  Major | Balancer, master |
| [HBASE-20853](https://issues.apache.org/jira/browse/HBASE-20853) | Polish "Add defaults to Table Interface so Implementors don't have to" |  Major | API |
| [HBASE-20875](https://issues.apache.org/jira/browse/HBASE-20875) | MemStoreLABImp::copyIntoCell uses 7% CPU when writing |  Major | Performance |
| [HBASE-20860](https://issues.apache.org/jira/browse/HBASE-20860) | Merged region's RIT state may not be cleaned after master restart |  Major | . |
| [HBASE-20847](https://issues.apache.org/jira/browse/HBASE-20847) | The parent procedure of RegionTransitionProcedure may not have the table lock |  Major | proc-v2, Region Assignment |
| [HBASE-20776](https://issues.apache.org/jira/browse/HBASE-20776) | Update branch-2 version to 2.2.0-SNAPSHOT |  Major | build |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21612](https://issues.apache.org/jira/browse/HBASE-21612) | Add developer debug options in  HBase Config for REST server |  Minor | Operability, REST, scripts |
| [HBASE-18735](https://issues.apache.org/jira/browse/HBASE-18735) | Provide a fast mechanism for shutting down mini cluster |  Major | . |
| [HBASE-21489](https://issues.apache.org/jira/browse/HBASE-21489) | TestShell is broken |  Major | shell |
| [HBASE-20152](https://issues.apache.org/jira/browse/HBASE-20152) | [AMv2] DisableTableProcedure versus ServerCrashProcedure |  Major | amv2 |
| [HBASE-20540](https://issues.apache.org/jira/browse/HBASE-20540) | [umbrella] Hadoop 3 compatibility |  Major | . |
| [HBASE-21536](https://issues.apache.org/jira/browse/HBASE-21536) | Fix completebulkload usage instructions |  Trivial | documentation, mapreduce |
| [HBASE-22449](https://issues.apache.org/jira/browse/HBASE-22449) | https everywhere in Maven metadata |  Minor | . |
| [HBASE-22406](https://issues.apache.org/jira/browse/HBASE-22406) | skip generating rdoc when building gems in our docker image for running yetus |  Critical | build, test |
| [HBASE-22375](https://issues.apache.org/jira/browse/HBASE-22375) | Promote AccessChecker to LimitedPrivate(Coprocessor) |  Minor | Coprocessors, security |
| [HBASE-21714](https://issues.apache.org/jira/browse/HBASE-21714) | Deprecated isTableAvailableWithSplit method in thrift module |  Major | Thrift |
| [HBASE-22359](https://issues.apache.org/jira/browse/HBASE-22359) | Backport of HBASE-21371 misses activation-api license information |  Minor | build, community |
| [HBASE-22174](https://issues.apache.org/jira/browse/HBASE-22174) | Remove error prone from our precommit javac check |  Major | build |
| [HBASE-22231](https://issues.apache.org/jira/browse/HBASE-22231) | Remove unused and \* imports |  Minor | . |
| [HBASE-22304](https://issues.apache.org/jira/browse/HBASE-22304) | Fix remaining Checkstyle issues in hbase-endpoint |  Trivial | . |
| [HBASE-22020](https://issues.apache.org/jira/browse/HBASE-22020) | upgrade to yetus 0.9.0 |  Major | build, community |
| [HBASE-22187](https://issues.apache.org/jira/browse/HBASE-22187) | Remove usage of deprecated ClusterConnection.clearRegionCache |  Trivial | Client |
| [HBASE-22203](https://issues.apache.org/jira/browse/HBASE-22203) | Reformat DemoClient.java |  Trivial | . |
| [HBASE-22189](https://issues.apache.org/jira/browse/HBASE-22189) | Remove usage of StoreFile.getModificationTimeStamp |  Trivial | . |
| [HBASE-22108](https://issues.apache.org/jira/browse/HBASE-22108) | Avoid passing null in Admin methods |  Major | Admin |
| [HBASE-22007](https://issues.apache.org/jira/browse/HBASE-22007) | Add restoreSnapshot and cloneSnapshot with acl methods in AsyncAdmin |  Major | Admin, asyncclient, Client |
| [HBASE-22131](https://issues.apache.org/jira/browse/HBASE-22131) | Delete the patches in hbase-protocol-shaded module |  Major | build, Protobufs |
| [HBASE-22099](https://issues.apache.org/jira/browse/HBASE-22099) | Backport HBASE-21895 "Error prone upgrade" to branch-2 |  Major | build |
| [HBASE-22052](https://issues.apache.org/jira/browse/HBASE-22052) | pom cleaning; filter out jersey-core in hadoop2 to match hadoop3 and remove redunant version specifications |  Major | . |
| [HBASE-22065](https://issues.apache.org/jira/browse/HBASE-22065) | Add listTableDescriptors(List\<TableName\>) method in AsyncAdmin |  Major | Admin |
| [HBASE-22042](https://issues.apache.org/jira/browse/HBASE-22042) | Missing @Override annotation for RawAsyncTableImpl.scan |  Major | asyncclient, Client |
| [HBASE-21057](https://issues.apache.org/jira/browse/HBASE-21057) | upgrade to latest spotbugs |  Minor | community, test |
| [HBASE-21888](https://issues.apache.org/jira/browse/HBASE-21888) | Add a isClosed method to AsyncConnection |  Major | asyncclient, Client |
| [HBASE-21884](https://issues.apache.org/jira/browse/HBASE-21884) | Fix box/unbox findbugs warning in secure bulk load |  Minor | . |
| [HBASE-21859](https://issues.apache.org/jira/browse/HBASE-21859) | Add clearRegionLocationCache method for AsyncConnection |  Major | asyncclient, Client |
| [HBASE-21853](https://issues.apache.org/jira/browse/HBASE-21853) | update copyright notices to 2019 |  Major | documentation |
| [HBASE-21791](https://issues.apache.org/jira/browse/HBASE-21791) | Upgrade thrift dependency to 0.12.0 |  Blocker | Thrift |
| [HBASE-21710](https://issues.apache.org/jira/browse/HBASE-21710) | Add quota related methods to the Admin interface |  Major | . |
| [HBASE-21782](https://issues.apache.org/jira/browse/HBASE-21782) | LoadIncrementalHFiles should not be IA.Public |  Major | mapreduce |
| [HBASE-21762](https://issues.apache.org/jira/browse/HBASE-21762) | Move some methods in ClusterConnection to Connection |  Major | Client |
| [HBASE-21715](https://issues.apache.org/jira/browse/HBASE-21715) | Do not throw UnsupportedOperationException in ProcedureFuture.get |  Major | Client |
| [HBASE-21716](https://issues.apache.org/jira/browse/HBASE-21716) | Add toStringCustomizedValues to TableDescriptor |  Major | . |
| [HBASE-21731](https://issues.apache.org/jira/browse/HBASE-21731) | Do not need to use ClusterConnection in IntegrationTestBigLinkedListWithVisibility |  Major | . |
| [HBASE-21685](https://issues.apache.org/jira/browse/HBASE-21685) | Change repository urls to Gitbox |  Critical | . |
| [HBASE-21534](https://issues.apache.org/jira/browse/HBASE-21534) | TestAssignmentManager is flakey |  Major | test |
| [HBASE-21541](https://issues.apache.org/jira/browse/HBASE-21541) | Move MetaTableLocator.verifyRegionLocation to hbase-rsgroup module |  Major | . |
| [HBASE-21265](https://issues.apache.org/jira/browse/HBASE-21265) | Split up TestRSGroups |  Minor | rsgroup, test |
| [HBASE-21517](https://issues.apache.org/jira/browse/HBASE-21517) | Move the getTableRegionForRow method from HMaster to TestMaster |  Major | test |
| [HBASE-21281](https://issues.apache.org/jira/browse/HBASE-21281) | Update bouncycastle dependency. |  Major | dependencies, test |
| [HBASE-21198](https://issues.apache.org/jira/browse/HBASE-21198) | Exclude dependency on net.minidev:json-smart |  Major | . |
| [HBASE-21282](https://issues.apache.org/jira/browse/HBASE-21282) | Upgrade to latest jetty 9.2 and 9.3 versions |  Major | dependencies |
| [HBASE-21287](https://issues.apache.org/jira/browse/HBASE-21287) | JVMClusterUtil Master initialization wait time not configurable |  Major | test |
| [HBASE-21168](https://issues.apache.org/jira/browse/HBASE-21168) | BloomFilterUtil uses hardcoded randomness |  Trivial | . |
| [HBASE-20482](https://issues.apache.org/jira/browse/HBASE-20482) | Print a link to the ref guide chapter for the shell during startup |  Minor | documentation, shell |
| [HBASE-20942](https://issues.apache.org/jira/browse/HBASE-20942) | Improve RpcServer TRACE logging |  Major | Operability |
| [HBASE-20989](https://issues.apache.org/jira/browse/HBASE-20989) | Minor, miscellaneous logging fixes |  Trivial | logging |



## Release 2.1.0 - Unreleased (as of 2018-07-10)

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

## Release 2.0.0 - Unreleased (as of 2018-04-22)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20276](https://issues.apache.org/jira/browse/HBASE-20276) | [shell] Revert shell REPL change and document |  Blocker | documentation, shell |
| [HBASE-16459](https://issues.apache.org/jira/browse/HBASE-16459) | Remove unused hbase shell --format option |  Trivial | shell |
| [HBASE-19128](https://issues.apache.org/jira/browse/HBASE-19128) | Purge Distributed Log Replay from codebase, configurations, text; mark the feature as unsupported, broken. |  Major | documentation |
| [HBASE-19504](https://issues.apache.org/jira/browse/HBASE-19504) | Add TimeRange support into checkAndMutate |  Major | . |
| [HBASE-20119](https://issues.apache.org/jira/browse/HBASE-20119) | Introduce a pojo class to carry coprocessor information in order to make TableDescriptorBuilder accept multiple cp at once |  Minor | . |
| [HBASE-19437](https://issues.apache.org/jira/browse/HBASE-19437) | Batch operation can't handle the null result for Append/Increment |  Critical | Usability |
| [HBASE-19873](https://issues.apache.org/jira/browse/HBASE-19873) | Add a CategoryBasedTimeout ClassRule for all UTs |  Major | . |
| [HBASE-19783](https://issues.apache.org/jira/browse/HBASE-19783) | Change replication peer cluster key/endpoint from a not-null value to null is not allowed |  Minor | Replication |
| [HBASE-19483](https://issues.apache.org/jira/browse/HBASE-19483) | Add proper privilege check for rsgroup commands |  Major | rsgroup, security |
| [HBASE-19492](https://issues.apache.org/jira/browse/HBASE-19492) | Add EXCLUDE\_NAMESPACE and EXCLUDE\_TABLECFS support to replication peer config |  Major | . |
| [HBASE-19357](https://issues.apache.org/jira/browse/HBASE-19357) | Bucket cache no longer L2 for LRU cache |  Major | . |
| [HBASE-19359](https://issues.apache.org/jira/browse/HBASE-19359) | Revisit the default config of hbase client retries number |  Major | . |
| [HBASE-19092](https://issues.apache.org/jira/browse/HBASE-19092) | Make Tag IA.LimitedPrivate and expose for CPs |  Critical | Coprocessors |
| [HBASE-19187](https://issues.apache.org/jira/browse/HBASE-19187) | Remove option to create on heap bucket cache |  Minor | regionserver |
| [HBASE-19033](https://issues.apache.org/jira/browse/HBASE-19033) | Allow CP users to change versions and TTL before opening StoreScanner |  Blocker | Coprocessors |
| [HBASE-19047](https://issues.apache.org/jira/browse/HBASE-19047) | CP exposed Scanner types should not extend Shipper |  Critical | Coprocessors |
| [HBASE-18905](https://issues.apache.org/jira/browse/HBASE-18905) | Allow CPs to request flush on Region and know the completion of the requested flush |  Major | Coprocessors |
| [HBASE-18410](https://issues.apache.org/jira/browse/HBASE-18410) | FilterList  Improvement. |  Major | Filters |
| [HBASE-18893](https://issues.apache.org/jira/browse/HBASE-18893) | Remove Add/Modify/DeleteColumnFamilyProcedure in favor of using ModifyTableProcedure |  Major | Coprocessors, master |
| [HBASE-19067](https://issues.apache.org/jira/browse/HBASE-19067) | Do not expose getHDFSBlockDistribution in StoreFile |  Major | Coprocessors |
| [HBASE-18989](https://issues.apache.org/jira/browse/HBASE-18989) | Polish the compaction related CP hooks |  Major | Compaction, Coprocessors |
| [HBASE-19046](https://issues.apache.org/jira/browse/HBASE-19046) | RegionObserver#postCompactSelection  Avoid passing shaded ImmutableList param |  Major | Coprocessors |
| [HBASE-19001](https://issues.apache.org/jira/browse/HBASE-19001) | Remove the hooks in RegionObserver which are designed to construct a StoreScanner which is marked as IA.Private |  Major | Coprocessors |
| [HBASE-14247](https://issues.apache.org/jira/browse/HBASE-14247) | Separate the old WALs into different regionserver directories |  Critical | wal |
| [HBASE-18183](https://issues.apache.org/jira/browse/HBASE-18183) | Region interface cleanup for CP expose |  Major | Coprocessors |
| [HBASE-18878](https://issues.apache.org/jira/browse/HBASE-18878) | Use Optional\<T\> return types when T can be null |  Major | Coprocessors |
| [HBASE-18649](https://issues.apache.org/jira/browse/HBASE-18649) | Deprecate KV Usage in MR to move to Cells in 3.0 |  Major | API, mapreduce |
| [HBASE-18897](https://issues.apache.org/jira/browse/HBASE-18897) | Substitute MemStore for Memstore |  Major | . |
| [HBASE-18883](https://issues.apache.org/jira/browse/HBASE-18883) | Upgrade to Curator 4.0 |  Major | Client, dependencies |
| [HBASE-18839](https://issues.apache.org/jira/browse/HBASE-18839) | Apply RegionInfo to code base |  Major | Coprocessors |
| [HBASE-18826](https://issues.apache.org/jira/browse/HBASE-18826) | Use HStore instead of Store in our own code base and remove unnecessary methods in Store interface |  Major | Coprocessors |
| [HBASE-17732](https://issues.apache.org/jira/browse/HBASE-17732) | Coprocessor Design Improvements |  Critical | Coprocessors |
| [HBASE-18298](https://issues.apache.org/jira/browse/HBASE-18298) | RegionServerServices Interface cleanup for CP expose |  Critical | Coprocessors |
| [HBASE-16769](https://issues.apache.org/jira/browse/HBASE-16769) | Deprecate/remove PB references from MasterObserver and RegionServerObserver |  Blocker | . |
| [HBASE-18859](https://issues.apache.org/jira/browse/HBASE-18859) | Purge PB from BulkLoadObserver |  Major | Coprocessors |
| [HBASE-18731](https://issues.apache.org/jira/browse/HBASE-18731) | [compat 1-2] Mark protected methods of QuotaSettings that touch Protobuf internals as IA.Private |  Major | API |
| [HBASE-18825](https://issues.apache.org/jira/browse/HBASE-18825) | Use HStoreFile instead of StoreFile in our own code base and remove unnecessary methods in StoreFile interface |  Major | Coprocessors |
| [HBASE-18142](https://issues.apache.org/jira/browse/HBASE-18142) | Deletion of a cell deletes the previous versions too |  Major | API, shell |
| [HBASE-18446](https://issues.apache.org/jira/browse/HBASE-18446) | Mark StoreFileScanner/StoreFileReader as IA.LimitedPrivate(Phoenix) |  Critical | Coprocessors |
| [HBASE-18798](https://issues.apache.org/jira/browse/HBASE-18798) | Remove the unused methods in RegionServerObserver |  Major | Coprocessors |
| [HBASE-18453](https://issues.apache.org/jira/browse/HBASE-18453) | CompactionRequest should not be exposed to user directly |  Major | Coprocessors |
| [HBASE-18794](https://issues.apache.org/jira/browse/HBASE-18794) | Remove deprecated methods in MasterObserver |  Major | Coprocessors |
| [HBASE-17823](https://issues.apache.org/jira/browse/HBASE-17823) | Migrate to Apache Yetus Audience Annotations |  Major | API |
| [HBASE-18793](https://issues.apache.org/jira/browse/HBASE-18793) | Remove deprecated methods in RegionObserver |  Major | Coprocessors |
| [HBASE-18733](https://issues.apache.org/jira/browse/HBASE-18733) | [compat 1-2] Hide WALKey |  Major | API |
| [HBASE-16479](https://issues.apache.org/jira/browse/HBASE-16479) | Move WALEdit from hbase.regionserver.wal package to hbase.wal package |  Major | wal |
| [HBASE-18783](https://issues.apache.org/jira/browse/HBASE-18783) | Declare the builder of ClusterStatus as IA.Private, and remove the Writables from ClusterStatus |  Minor | . |
| [HBASE-18106](https://issues.apache.org/jira/browse/HBASE-18106) | Redo ProcedureInfo and LockInfo |  Critical | proc-v2 |
| [HBASE-18780](https://issues.apache.org/jira/browse/HBASE-18780) | Remove HLogPrettyPrinter and hlog command |  Minor | documentation, wal |
| [HBASE-18704](https://issues.apache.org/jira/browse/HBASE-18704) | Upgrade hbase to commons-collections 4 |  Major | dependencies |
| [HBASE-15607](https://issues.apache.org/jira/browse/HBASE-15607) | Remove PB references from Admin for 2.0 |  Blocker | . |
| [HBASE-18736](https://issues.apache.org/jira/browse/HBASE-18736) | Cleanup the HTD/HCD for Admin |  Major | . |
| [HBASE-18577](https://issues.apache.org/jira/browse/HBASE-18577) | shaded client includes several non-relocated third party dependencies |  Critical | Client |
| [HBASE-3935](https://issues.apache.org/jira/browse/HBASE-3935) | HServerLoad.storefileIndexSizeMB should be changed to storefileIndexSizeKB |  Major | . |
| [HBASE-15982](https://issues.apache.org/jira/browse/HBASE-15982) | Interface ReplicationEndpoint extends Guava's Service |  Blocker | . |
| [HBASE-18546](https://issues.apache.org/jira/browse/HBASE-18546) | Always overwrite the TS for Append/Increment unless no existing cells are found |  Critical | API, Client |
| [HBASE-17442](https://issues.apache.org/jira/browse/HBASE-17442) | Move most of the replication related classes from hbase-client to hbase-replication package |  Critical | build, Replication |
| [HBASE-18511](https://issues.apache.org/jira/browse/HBASE-18511) | Default no regions on master |  Blocker | master |
| [HBASE-18528](https://issues.apache.org/jira/browse/HBASE-18528) | DON'T allow user to modify the passed table/column descriptor |  Critical | Coprocessors, master |
| [HBASE-18469](https://issues.apache.org/jira/browse/HBASE-18469) | Correct  RegionServer metric of  totalRequestCount |  Critical | metrics, regionserver |
| [HBASE-18500](https://issues.apache.org/jira/browse/HBASE-18500) | Performance issue: Don't use BufferedMutator for HTable's put method |  Major | . |
| [HBASE-17125](https://issues.apache.org/jira/browse/HBASE-17125) | Inconsistent result when use filter to read data |  Critical | . |
| [HBASE-18517](https://issues.apache.org/jira/browse/HBASE-18517) | limit max log message width in log4j |  Major | . |
| [HBASE-18502](https://issues.apache.org/jira/browse/HBASE-18502) | Change MasterObserver to use TableDescriptor and ColumnFamilyDescriptor |  Critical | Coprocessors, master |
| [HBASE-18374](https://issues.apache.org/jira/browse/HBASE-18374) | RegionServer Metrics improvements |  Major | . |
| [HBASE-17908](https://issues.apache.org/jira/browse/HBASE-17908) | Upgrade guava |  Critical | dependencies |
| [HBASE-18161](https://issues.apache.org/jira/browse/HBASE-18161) | Incremental Load support for Multiple-Table HFileOutputFormat |  Minor | . |
| [HBASE-18267](https://issues.apache.org/jira/browse/HBASE-18267) | The result from the postAppend is ignored |  Major | Coprocessors |
| [HBASE-18241](https://issues.apache.org/jira/browse/HBASE-18241) | Change client.Table, client.Admin, Region, Store, and HBaseTestingUtility to not use HTableDescriptor or HColumnDescriptor |  Critical | Client |
| [HBASE-18038](https://issues.apache.org/jira/browse/HBASE-18038) | Rename StoreFile to HStoreFile and add a StoreFile interface for CP |  Critical | Coprocessors, regionserver |
| [HBASE-16196](https://issues.apache.org/jira/browse/HBASE-16196) | Update jruby to a newer version. |  Critical | dependencies, shell |
| [HBASE-14614](https://issues.apache.org/jira/browse/HBASE-14614) | Procedure v2: Core Assignment Manager |  Major | proc-v2 |
| [HBASE-3462](https://issues.apache.org/jira/browse/HBASE-3462) | Fix table.jsp in regards to splitting a region/table with an optional splitkey |  Major | master |
| [HBASE-11013](https://issues.apache.org/jira/browse/HBASE-11013) | Clone Snapshots on Secure Cluster Should provide option to apply Retained User Permissions |  Major | snapshots |
| [HBASE-15296](https://issues.apache.org/jira/browse/HBASE-15296) | Break out writer and reader from StoreFile |  Major | regionserver |
| [HBASE-15199](https://issues.apache.org/jira/browse/HBASE-15199) | Move jruby jar so only on hbase-shell module classpath; currently globally available |  Critical | dependencies, jruby, shell |
| [HBASE-18009](https://issues.apache.org/jira/browse/HBASE-18009) | Move RpcServer.Call to a separated file |  Major | IPC/RPC |
| [HBASE-17956](https://issues.apache.org/jira/browse/HBASE-17956) | Raw scan should ignore TTL |  Major | scan |
| [HBASE-17914](https://issues.apache.org/jira/browse/HBASE-17914) | Create a new reader instead of cloning a new StoreFile when compaction |  Major | Compaction, regionserver |
| [HBASE-17595](https://issues.apache.org/jira/browse/HBASE-17595) | Add partial result support for small/limited scan |  Critical | asyncclient, Client, scan |
| [HBASE-17584](https://issues.apache.org/jira/browse/HBASE-17584) | Expose ScanMetrics with ResultScanner rather than Scan |  Major | Client, mapreduce, scan |
| [HBASE-17716](https://issues.apache.org/jira/browse/HBASE-17716) | Formalize Scan Metric names |  Minor | metrics |
| [HBASE-17312](https://issues.apache.org/jira/browse/HBASE-17312) | [JDK8] Use default method for Observer Coprocessors |  Major | Coprocessors |
| [HBASE-17647](https://issues.apache.org/jira/browse/HBASE-17647) | OffheapKeyValue#heapSize() implementation is wrong |  Major | regionserver |
| [HBASE-17472](https://issues.apache.org/jira/browse/HBASE-17472) | Correct the semantic of  permission grant |  Major | Admin |
| [HBASE-17599](https://issues.apache.org/jira/browse/HBASE-17599) | Use mayHaveMoreCellsInRow instead of isPartial |  Major | Client, scan |
| [HBASE-17508](https://issues.apache.org/jira/browse/HBASE-17508) | Unify the implementation of small scan and regular scan for sync client |  Major | Client, scan |
| [HBASE-12894](https://issues.apache.org/jira/browse/HBASE-12894) | Upgrade Jetty to 9.2.6 |  Critical | REST, UI |
| [HBASE-16786](https://issues.apache.org/jira/browse/HBASE-16786) | Procedure V2 - Move ZK-lock's uses to Procedure framework locks (LockProcedure) |  Major | . |
| [HBASE-17470](https://issues.apache.org/jira/browse/HBASE-17470) | Remove merge region code from region server |  Major | regionserver |
| [HBASE-5401](https://issues.apache.org/jira/browse/HBASE-5401) | PerformanceEvaluation generates 10x the number of expected mappers |  Major | test |
| [HBASE-17221](https://issues.apache.org/jira/browse/HBASE-17221) | Abstract out an interface for RpcServer.Call |  Major | . |
| [HBASE-16119](https://issues.apache.org/jira/browse/HBASE-16119) | Procedure v2 - Reimplement merge |  Major | proc-v2, Region Assignment |
| [HBASE-17132](https://issues.apache.org/jira/browse/HBASE-17132) | Cleanup deprecated code for WAL |  Major | wal |
| [HBASE-17017](https://issues.apache.org/jira/browse/HBASE-17017) | Remove the current per-region latency histogram metrics |  Major | metrics |
| [HBASE-15513](https://issues.apache.org/jira/browse/HBASE-15513) | hbase.hregion.memstore.chunkpool.maxsize is 0.0 by default |  Major | . |
| [HBASE-16972](https://issues.apache.org/jira/browse/HBASE-16972) | Log more details for Scan#next request when responseTooSlow |  Major | Operability |
| [HBASE-16765](https://issues.apache.org/jira/browse/HBASE-16765) | New SteppingRegionSplitPolicy, avoid too aggressive spread of regions for small tables. |  Critical | . |
| [HBASE-16747](https://issues.apache.org/jira/browse/HBASE-16747) | Track memstore data size and heap overhead separately |  Major | regionserver |
| [HBASE-14551](https://issues.apache.org/jira/browse/HBASE-14551) | Procedure v2 - Reimplement split |  Minor | proc-v2 |
| [HBASE-16729](https://issues.apache.org/jira/browse/HBASE-16729) | Define the behavior of (default) empty FilterList |  Trivial | . |
| [HBASE-16799](https://issues.apache.org/jira/browse/HBASE-16799) | CP exposed Store should not expose unwanted APIs |  Major | . |
| [HBASE-16117](https://issues.apache.org/jira/browse/HBASE-16117) | Fix Connection leak in mapred.TableOutputFormat |  Major | mapreduce |
| [HBASE-15638](https://issues.apache.org/jira/browse/HBASE-15638) | Shade protobuf |  Critical | Protobufs |
| [HBASE-16257](https://issues.apache.org/jira/browse/HBASE-16257) | Move staging dir to be under hbase root dir |  Blocker | . |
| [HBASE-16650](https://issues.apache.org/jira/browse/HBASE-16650) | Wrong usage of BlockCache eviction stat for heap memory tuning |  Major | . |
| [HBASE-16598](https://issues.apache.org/jira/browse/HBASE-16598) | Enable zookeeper useMulti always and clean up in HBase code |  Major | . |
| [HBASE-15297](https://issues.apache.org/jira/browse/HBASE-15297) | error message is wrong when a wrong namspace is specified in grant in hbase shell |  Minor | shell |
| [HBASE-16340](https://issues.apache.org/jira/browse/HBASE-16340) | ensure no Xerces jars included |  Critical | dependencies |
| [HBASE-16321](https://issues.apache.org/jira/browse/HBASE-16321) | Ensure findbugs jsr305 jar isn't present |  Blocker | dependencies |
| [HBASE-16355](https://issues.apache.org/jira/browse/HBASE-16355) | hbase-client dependency on hbase-common test-jar should be test scope |  Major | Client, dependencies |
| [HBASE-16186](https://issues.apache.org/jira/browse/HBASE-16186) | Fix AssignmentManager MBean name |  Major | master |
| [HBASE-13823](https://issues.apache.org/jira/browse/HBASE-13823) | Procedure V2: unnecessaery operations on AssignmentManager#recoverTableInDisablingState() and recoverTableInEnablingState() |  Major | master, proc-v2 |
| [HBASE-15950](https://issues.apache.org/jira/browse/HBASE-15950) | Fix memstore size estimates to be more tighter |  Major | . |
| [HBASE-15971](https://issues.apache.org/jira/browse/HBASE-15971) | Regression: Random Read/WorkloadC slower in 1.x than 0.98 |  Critical | rpc |
| [HBASE-15875](https://issues.apache.org/jira/browse/HBASE-15875) | Remove HTable references and HTableInterface |  Major | . |
| [HBASE-15610](https://issues.apache.org/jira/browse/HBASE-15610) | Remove deprecated HConnection for 2.0 thus removing all PB references for 2.0 |  Blocker | . |
| [HBASE-15876](https://issues.apache.org/jira/browse/HBASE-15876) | Remove doBulkLoad(Path hfofDir, final HTable table) though it has not been through a full deprecation cycle |  Blocker | . |
| [HBASE-15575](https://issues.apache.org/jira/browse/HBASE-15575) | Rename table DDL \*Handler methods in MasterObserver to more meaningful names |  Minor | Coprocessors |
| [HBASE-15481](https://issues.apache.org/jira/browse/HBASE-15481) | Add pre/post roll to WALObserver |  Trivial | . |
| [HBASE-15568](https://issues.apache.org/jira/browse/HBASE-15568) | Procedure V2 - Remove CreateTableHandler in HBase Apache 2.0 release |  Major | master, proc-v2 |
| [HBASE-15521](https://issues.apache.org/jira/browse/HBASE-15521) | Procedure V2 - RestoreSnapshot and CloneSnapshot |  Major | Client, master, proc-v2 |
| [HBASE-11393](https://issues.apache.org/jira/browse/HBASE-11393) | Replication TableCfs should be a PB object rather than a string |  Major | Replication |
| [HBASE-15265](https://issues.apache.org/jira/browse/HBASE-15265) | Implement an asynchronous FSHLog |  Major | wal |
| [HBASE-15323](https://issues.apache.org/jira/browse/HBASE-15323) | Hbase Rest CheckAndDeleteAPi should be able to delete more cells |  Major | hbase |
| [HBASE-15377](https://issues.apache.org/jira/browse/HBASE-15377) | Per-RS Get metric is time based, per-region metric is size-based |  Major | . |
| [HBASE-13963](https://issues.apache.org/jira/browse/HBASE-13963) | avoid leaking jdk.tools |  Critical | build, documentation |
| [HBASE-15376](https://issues.apache.org/jira/browse/HBASE-15376) | ScanNext metric is size-based while every other per-operation metric is time based |  Major | . |
| [HBASE-15290](https://issues.apache.org/jira/browse/HBASE-15290) | Hbase Rest CheckAndAPI should save other cells along with compared cell |  Major | hbase |
| [HBASE-15100](https://issues.apache.org/jira/browse/HBASE-15100) | Master WALProcs still never clean up |  Blocker | master, proc-v2 |
| [HBASE-15111](https://issues.apache.org/jira/browse/HBASE-15111) | "hbase version" should write to stdout |  Trivial | util |
| [HBASE-14888](https://issues.apache.org/jira/browse/HBASE-14888) | ClusterSchema: Add Namespace Operations |  Major | API |
| [HBASE-15018](https://issues.apache.org/jira/browse/HBASE-15018) | Inconsistent way of handling TimeoutException in the rpc client implementations |  Major | Client, IPC/RPC |
| [HBASE-14205](https://issues.apache.org/jira/browse/HBASE-14205) | RegionCoprocessorHost System.nanoTime() performance bottleneck |  Critical | Coprocessors, Performance, regionserver |
| [HBASE-12751](https://issues.apache.org/jira/browse/HBASE-12751) | Allow RowLock to be reader writer |  Major | regionserver |
| [HBASE-13706](https://issues.apache.org/jira/browse/HBASE-13706) | CoprocessorClassLoader should not exempt Hive classes |  Minor | Coprocessors |
| [HBASE-13954](https://issues.apache.org/jira/browse/HBASE-13954) | Remove HTableInterface#getRowOrBefore related server side code |  Major | API |
| [HBASE-12296](https://issues.apache.org/jira/browse/HBASE-12296) | Filters should work with ByteBufferedCell |  Major | regionserver, Scanners |
| [HBASE-14027](https://issues.apache.org/jira/browse/HBASE-14027) | Clean up netty dependencies |  Major | build |
| [HBASE-7782](https://issues.apache.org/jira/browse/HBASE-7782) | HBaseTestingUtility.truncateTable() not acting like CLI |  Minor | test |
| [HBASE-14047](https://issues.apache.org/jira/browse/HBASE-14047) | Cleanup deprecated APIs from Cell class |  Major | Client |
| [HBASE-13849](https://issues.apache.org/jira/browse/HBASE-13849) | Remove restore and clone snapshot from the WebUI |  Major | snapshots |
| [HBASE-13646](https://issues.apache.org/jira/browse/HBASE-13646) | HRegion#execService should not try to build incomplete messages |  Major | Coprocessors, regionserver |
| [HBASE-13983](https://issues.apache.org/jira/browse/HBASE-13983) | Doc how the oddball HTable methods getStartKey, getEndKey, etc. will be removed in 2.0.0 |  Minor | documentation |
| [HBASE-13214](https://issues.apache.org/jira/browse/HBASE-13214) | Remove deprecated and unused methods from HTable class |  Major | API |
| [HBASE-13843](https://issues.apache.org/jira/browse/HBASE-13843) | Fix internal constant text in ReplicationManager.java |  Trivial | master |
| [HBASE-13375](https://issues.apache.org/jira/browse/HBASE-13375) | Provide HBase superuser higher priority over other users in the RPC handling |  Major | IPC/RPC |
| [HBASE-13636](https://issues.apache.org/jira/browse/HBASE-13636) | Remove deprecation for HBASE-4072 (Reading of zoo.cfg) |  Major | . |
| [HBASE-10800](https://issues.apache.org/jira/browse/HBASE-10800) | Use CellComparator instead of KVComparator |  Major | . |
| [HBASE-13118](https://issues.apache.org/jira/browse/HBASE-13118) | [PE] Add being able to write many columns |  Major | test |
| [HBASE-12990](https://issues.apache.org/jira/browse/HBASE-12990) | MetaScanner should be replaced by MetaTableAccessor |  Major | Client |
| [HBASE-13373](https://issues.apache.org/jira/browse/HBASE-13373) | Squash HFileReaderV3 together with HFileReaderV2 and AbstractHFileReader; ditto for Scanners and BlockReader, etc. |  Major | . |
| [HBASE-10728](https://issues.apache.org/jira/browse/HBASE-10728) | get\_counter value is never used. |  Major | . |
| [HBASE-13298](https://issues.apache.org/jira/browse/HBASE-13298) | Clarify if Table.{set\|get}WriteBufferSize() is deprecated or not |  Critical | API |
| [HBASE-13248](https://issues.apache.org/jira/browse/HBASE-13248) | Make HConnectionImplementation top-level class. |  Major | API |
| [HBASE-13198](https://issues.apache.org/jira/browse/HBASE-13198) | Remove HConnectionManager |  Major | API |
| [HBASE-12586](https://issues.apache.org/jira/browse/HBASE-12586) | Task 6 & 7 from HBASE-9117,  delete all public HTable constructors and delete ConnectionManager#{delete,get}Connection |  Major | . |
| [HBASE-13171](https://issues.apache.org/jira/browse/HBASE-13171) | Change AccessControlClient methods to accept connection object to reduce setup time. |  Minor | . |
| [HBASE-6778](https://issues.apache.org/jira/browse/HBASE-6778) | Deprecate Chore; its a thread per task when we should have one thread to do all tasks |  Major | . |
| [HBASE-12684](https://issues.apache.org/jira/browse/HBASE-12684) | Add new AsyncRpcClient |  Major | Client |
| [HBASE-10378](https://issues.apache.org/jira/browse/HBASE-10378) | Divide HLog interface into User and Implementor specific interfaces |  Major | wal |
| [HBASE-12111](https://issues.apache.org/jira/browse/HBASE-12111) | Remove deprecated APIs from Mutation(s) |  Major | Client |
| [HBASE-12084](https://issues.apache.org/jira/browse/HBASE-12084) | Remove deprecated APIs from Result |  Major | Client |
| [HBASE-12048](https://issues.apache.org/jira/browse/HBASE-12048) | Remove deprecated APIs from Filter |  Major | regionserver |
| [HBASE-11556](https://issues.apache.org/jira/browse/HBASE-11556) | Move HTablePool to hbase-thrift module. |  Major | Thrift |
| [HBASE-4072](https://issues.apache.org/jira/browse/HBASE-4072) | Deprecate/disable and remove support for reading ZooKeeper zoo.cfg files from the classpath |  Major | . |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-15780](https://issues.apache.org/jira/browse/HBASE-15780) | Expose AuthUtil as IA.Public |  Critical | API, security |
| [HBASE-15322](https://issues.apache.org/jira/browse/HBASE-15322) | Operations using Unsafe path broken for platforms not having sun.misc.Unsafe |  Critical | hbase |
| [HBASE-15125](https://issues.apache.org/jira/browse/HBASE-15125) | HBaseFsck's adoptHdfsOrphan function creates region with wrong end key boundary |  Major | hbck |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-19950](https://issues.apache.org/jira/browse/HBASE-19950) | Introduce a ColumnValueFilter |  Minor | Filters |
| [HBASE-19844](https://issues.apache.org/jira/browse/HBASE-19844) | Shell should support flush by regionserver |  Minor | shell |
| [HBASE-19886](https://issues.apache.org/jira/browse/HBASE-19886) | Display maintenance mode in shell, web UI |  Major | . |
| [HBASE-19528](https://issues.apache.org/jira/browse/HBASE-19528) | Major Compaction Tool |  Major | . |
| [HBASE-19897](https://issues.apache.org/jira/browse/HBASE-19897) | RowMutations should follow the fluent pattern |  Minor | . |
| [HBASE-4224](https://issues.apache.org/jira/browse/HBASE-4224) | Need a flush by regionserver rather than by table option |  Major | . |
| [HBASE-15321](https://issues.apache.org/jira/browse/HBASE-15321) | Ability to open a HRegion from hdfs snapshot. |  Major | . |
| [HBASE-19799](https://issues.apache.org/jira/browse/HBASE-19799) | Add web UI to rsgroup |  Major | rsgroup, UI |
| [HBASE-19326](https://issues.apache.org/jira/browse/HBASE-19326) | Remove decommissioned servers from rsgroup |  Major | rsgroup |
| [HBASE-19189](https://issues.apache.org/jira/browse/HBASE-19189) | Ad-hoc test job for running a subset of tests lots of times |  Major | build |
| [HBASE-19103](https://issues.apache.org/jira/browse/HBASE-19103) | Add BigDecimalComparator for filter |  Minor | Client |
| [HBASE-18171](https://issues.apache.org/jira/browse/HBASE-18171) | Scanning cursor for async client |  Major | . |
| [HBASE-18875](https://issues.apache.org/jira/browse/HBASE-18875) | Thrift server supports read-only mode |  Major | Thrift |
| [HBASE-14417](https://issues.apache.org/jira/browse/HBASE-14417) | Incremental backup and bulk loading |  Blocker | . |
| [HBASE-18131](https://issues.apache.org/jira/browse/HBASE-18131) | Add an hbase shell command to clear deadserver list in ServerManager |  Major | Operability |
| [HBASE-15806](https://issues.apache.org/jira/browse/HBASE-15806) | An endpoint-based export tool |  Critical | Coprocessors, tooling |
| [HBASE-14135](https://issues.apache.org/jira/browse/HBASE-14135) | HBase Backup/Restore Phase 3: Merge backup images |  Critical | . |
| [HBASE-15134](https://issues.apache.org/jira/browse/HBASE-15134) | Add visibility into Flush and Compaction queues |  Major | Compaction, metrics, regionserver |
| [HBASE-15968](https://issues.apache.org/jira/browse/HBASE-15968) | New behavior of versions considering mvcc and ts rather than ts only |  Major | . |
| [HBASE-15943](https://issues.apache.org/jira/browse/HBASE-15943) | Add page displaying JVM process metrics |  Major | Operability, UI |
| [HBASE-17928](https://issues.apache.org/jira/browse/HBASE-17928) | Shell tool to clear compaction queues |  Major | Compaction, Operability |
| [HBASE-18226](https://issues.apache.org/jira/browse/HBASE-18226) | Disable reverse DNS lookup at HMaster and use the hostname provided by RegionServer |  Major | . |
| [HBASE-13784](https://issues.apache.org/jira/browse/HBASE-13784) | Add Async Client Table API |  Major | . |
| [HBASE-17849](https://issues.apache.org/jira/browse/HBASE-17849) | PE tool random read is not totally random |  Major | Performance, test |
| [HBASE-15576](https://issues.apache.org/jira/browse/HBASE-15576) | Scanning cursor to prevent blocking long time on ResultScanner.next() |  Major | . |
| [HBASE-16961](https://issues.apache.org/jira/browse/HBASE-16961) | FileSystem Quotas |  Major | . |
| [HBASE-17757](https://issues.apache.org/jira/browse/HBASE-17757) | Unify blocksize after encoding to decrease memory fragment |  Major | . |
| [HBASE-17542](https://issues.apache.org/jira/browse/HBASE-17542) | Move backup system table into separate namespace |  Major | . |
| [HBASE-14141](https://issues.apache.org/jira/browse/HBASE-14141) | HBase Backup/Restore Phase 3: Filter WALs on backup to include only edits from backed up tables |  Blocker | . |
| [HBASE-17758](https://issues.apache.org/jira/browse/HBASE-17758) | [RSGROUP] Add shell command to move servers and tables at the same time |  Major | rsgroup |
| [HBASE-17737](https://issues.apache.org/jira/browse/HBASE-17737) | Thrift2 proxy should support scan timeRange per column family |  Major | Thrift |
| [HBASE-16981](https://issues.apache.org/jira/browse/HBASE-16981) | Expand Mob Compaction Partition policy from daily to weekly, monthly |  Major | mob |
| [HBASE-9774](https://issues.apache.org/jira/browse/HBASE-9774) | HBase native metrics and metric collection for coprocessors |  Major | Coprocessors, metrics |
| [HBASE-17174](https://issues.apache.org/jira/browse/HBASE-17174) | Refactor the AsyncProcess, BufferedMutatorImpl, and HTable |  Minor | . |
| [HBASE-15432](https://issues.apache.org/jira/browse/HBASE-15432) | TableInputFormat - support multi column family scan |  Major | . |
| [HBASE-17181](https://issues.apache.org/jira/browse/HBASE-17181) | Let HBase thrift2 support TThreadedSelectorServer |  Minor | Thrift |
| [HBASE-17151](https://issues.apache.org/jira/browse/HBASE-17151) | New API to create HFile.Reader without instantiating block cache |  Major | . |
| [HBASE-16463](https://issues.apache.org/jira/browse/HBASE-16463) | Improve transparent table/CF encryption with Commons Crypto |  Major | encryption |
| [HBASE-16751](https://issues.apache.org/jira/browse/HBASE-16751) | Add tuning information to HBase Book |  Minor | . |
| [HBASE-16677](https://issues.apache.org/jira/browse/HBASE-16677) | Add table size (total store file size) to table page |  Minor | website |
| [HBASE-16447](https://issues.apache.org/jira/browse/HBASE-16447) | Replication by namespaces config in peer |  Critical | Replication |
| [HBASE-16388](https://issues.apache.org/jira/browse/HBASE-16388) | Prevent client threads being blocked by only one slow region server |  Major | . |
| [HBASE-16213](https://issues.apache.org/jira/browse/HBASE-16213) | A new HFileBlock structure for fast random get |  Major | Performance |
| [HBASE-12721](https://issues.apache.org/jira/browse/HBASE-12721) | Create Docker container cluster infrastructure to enable better testing |  Major | build, community, documentation, test |
| [HBASE-3727](https://issues.apache.org/jira/browse/HBASE-3727) | MultiHFileOutputFormat |  Minor | . |
| [HBASE-15881](https://issues.apache.org/jira/browse/HBASE-15881) | Allow BZIP2 compression |  Major | HFile |
| [HBASE-10358](https://issues.apache.org/jira/browse/HBASE-10358) | Shell changes for setting consistency per request |  Major | shell |
| [HBASE-15892](https://issues.apache.org/jira/browse/HBASE-15892) | submit-patch.py: Single command line to make patch, upload it to jira, and update review board |  Trivial | . |
| [HBASE-15228](https://issues.apache.org/jira/browse/HBASE-15228) | Add the methods to RegionObserver to trigger start/complete restoring WALs |  Major | Coprocessors |
| [HBASE-15847](https://issues.apache.org/jira/browse/HBASE-15847) | VerifyReplication prefix filtering |  Major | Replication |
| [HBASE-15798](https://issues.apache.org/jira/browse/HBASE-15798) | Add Async RpcChannels to all RpcClients |  Major | . |
| [HBASE-15281](https://issues.apache.org/jira/browse/HBASE-15281) | Allow the FileSystem inside HFileSystem to be wrapped |  Major | Filesystem Integration, hbase |
| [HBASE-15592](https://issues.apache.org/jira/browse/HBASE-15592) | Print Procedure WAL content |  Major | . |
| [HBASE-6721](https://issues.apache.org/jira/browse/HBASE-6721) | RegionServer Group based Assignment |  Major | regionserver |
| [HBASE-15136](https://issues.apache.org/jira/browse/HBASE-15136) | Explore different queuing behaviors while busy |  Critical | IPC/RPC |
| [HBASE-15181](https://issues.apache.org/jira/browse/HBASE-15181) | A simple implementation of date based tiered compaction |  Major | Compaction |
| [HBASE-13259](https://issues.apache.org/jira/browse/HBASE-13259) | mmap() based BucketCache IOEngine |  Critical | BlockCache |
| [HBASE-15135](https://issues.apache.org/jira/browse/HBASE-15135) | Add metrics for storefile age |  Major | . |
| [HBASE-14355](https://issues.apache.org/jira/browse/HBASE-14355) | Scan different TimeRange for each column family |  Major | Client, regionserver, Scanners |
| [HBASE-11262](https://issues.apache.org/jira/browse/HBASE-11262) | Avoid empty columns while doing bulk-load |  Major | . |
| [HBASE-15036](https://issues.apache.org/jira/browse/HBASE-15036) | Update HBase Spark documentation to include bulk load with thin records |  Major | . |
| [HBASE-14980](https://issues.apache.org/jira/browse/HBASE-14980) | Project Astro |  Major | documentation |
| [HBASE-13153](https://issues.apache.org/jira/browse/HBASE-13153) | Bulk Loaded HFile Replication |  Major | Replication |
| [HBASE-12911](https://issues.apache.org/jira/browse/HBASE-12911) | Client-side metrics |  Major | Client, Operability, Performance |
| [HBASE-14529](https://issues.apache.org/jira/browse/HBASE-14529) | Respond to SIGHUP to reload config |  Major | Operability |
| [HBASE-14459](https://issues.apache.org/jira/browse/HBASE-14459) | Add request and response sizes metrics |  Major | metrics |
| [HBASE-14456](https://issues.apache.org/jira/browse/HBASE-14456) | Implement a namespace-based region grouping strategy for RegionGroupingProvider |  Major | . |
| [HBASE-14154](https://issues.apache.org/jira/browse/HBASE-14154) | DFS Replication should be configurable at column family level |  Minor | . |
| [HBASE-13702](https://issues.apache.org/jira/browse/HBASE-13702) | ImportTsv: Add dry-run functionality and log bad rows |  Major | . |
| [HBASE-13639](https://issues.apache.org/jira/browse/HBASE-13639) | SyncTable - rsync for HBase tables |  Major | mapreduce, Operability, tooling |
| [HBASE-10070](https://issues.apache.org/jira/browse/HBASE-10070) | HBase read high-availability using timeline-consistent region replicas |  Major | Admin, API, LatencyResilience |
| [HBASE-13356](https://issues.apache.org/jira/browse/HBASE-13356) | HBase should provide an InputFormat supporting multiple scans in mapreduce jobs over snapshots |  Minor | mapreduce |
| [HBASE-5980](https://issues.apache.org/jira/browse/HBASE-5980) | Scanner responses from RS should include metrics on rows/KVs filtered |  Minor | Client, metrics, Operability, regionserver |
| [HBASE-13698](https://issues.apache.org/jira/browse/HBASE-13698) | Add RegionLocator methods to Thrift2 proxy. |  Major | Thrift |
| [HBASE-13071](https://issues.apache.org/jira/browse/HBASE-13071) | Hbase Streaming Scan Feature |  Major | . |
| [HBASE-13090](https://issues.apache.org/jira/browse/HBASE-13090) | Progress heartbeats for long running scanners |  Major | . |
| [HBASE-13412](https://issues.apache.org/jira/browse/HBASE-13412) | Region split decisions should have jitter |  Major | regionserver |
| [HBASE-12972](https://issues.apache.org/jira/browse/HBASE-12972) | Region, a supportable public/evolving subset of HRegion |  Major | . |
| [HBASE-13170](https://issues.apache.org/jira/browse/HBASE-13170) | Allow block cache to be external |  Major | io |
| [HBASE-5238](https://issues.apache.org/jira/browse/HBASE-5238) | Add a log4j category for all edits to META/ROOT |  Minor | regionserver |
| [HBASE-13063](https://issues.apache.org/jira/browse/HBASE-13063) | Allow to turn off memstore replication for region replicas |  Minor | regionserver, Replication |
| [HBASE-13057](https://issues.apache.org/jira/browse/HBASE-13057) | Provide client utility to easily enable and disable table replication |  Major | Replication |
| [HBASE-12869](https://issues.apache.org/jira/browse/HBASE-12869) | Add a REST API implementation of the ClusterManager interface |  Major | integration tests |
| [HBASE-12944](https://issues.apache.org/jira/browse/HBASE-12944) | Support patches to branches in precommit jenkins build |  Major | . |
| [HBASE-12268](https://issues.apache.org/jira/browse/HBASE-12268) | Add support for Scan.setRowPrefixFilter to shell |  Major | shell |
| [HBASE-5162](https://issues.apache.org/jira/browse/HBASE-5162) | Basic client pushback mechanism |  Major | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-19994](https://issues.apache.org/jira/browse/HBASE-19994) | Create a new class for RPC throttling exception, make it retryable. |  Major | . |
| [HBASE-20409](https://issues.apache.org/jira/browse/HBASE-20409) | Set hbase.client.meta.operation.timeout in TestClientOperationTimeout |  Trivial | test |
| [HBASE-17449](https://issues.apache.org/jira/browse/HBASE-17449) | Add explicit document on different timeout settings |  Critical | documentation |
| [HBASE-18784](https://issues.apache.org/jira/browse/HBASE-18784) | Use of filesystem that requires hflush / hsync / append / etc should query outputstream capabilities |  Major | Filesystem Integration |
| [HBASE-15466](https://issues.apache.org/jira/browse/HBASE-15466) | precommit should not run all java goals when given a docs-only patch |  Major | build |
| [HBASE-20135](https://issues.apache.org/jira/browse/HBASE-20135) | NullPointerException during reading bloom filter when upgraded from hbase-1 to hbase-2 |  Minor | . |
| [HBASE-17165](https://issues.apache.org/jira/browse/HBASE-17165) | Add retry to LoadIncrementalHFiles tool |  Critical | hbase, HFile, tooling |
| [HBASE-18467](https://issues.apache.org/jira/browse/HBASE-18467) | nightly job needs to run all stages and then comment on jira |  Critical | community, test |
| [HBASE-17448](https://issues.apache.org/jira/browse/HBASE-17448) | Export metrics from RecoverableZooKeeper |  Major | Zookeeper |
| [HBASE-20055](https://issues.apache.org/jira/browse/HBASE-20055) | Remove declaration of un-thrown exceptions and unused setRegionStateBackToOpen() from MergeTableRegionsProcedure |  Minor | amv2 |
| [HBASE-18020](https://issues.apache.org/jira/browse/HBASE-18020) | Update API Compliance Checker to Incorporate Improvements Done in Hadoop |  Major | API, community |
| [HBASE-20065](https://issues.apache.org/jira/browse/HBASE-20065) | Revisit the timestamp usage in MetaTableAccessor |  Major | . |
| [HBASE-18294](https://issues.apache.org/jira/browse/HBASE-18294) | Reduce global heap pressure: flush based on heap occupancy |  Major | . |
| [HBASE-19680](https://issues.apache.org/jira/browse/HBASE-19680) | BufferedMutatorImpl#mutate should wait the result from AP in order to throw the failed mutations |  Major | . |
| [HBASE-19988](https://issues.apache.org/jira/browse/HBASE-19988) | HRegion#lockRowsAndBuildMiniBatch() is too chatty when interrupted while waiting for a row lock |  Minor | amv2 |
| [HBASE-19915](https://issues.apache.org/jira/browse/HBASE-19915) | From split/ merge procedures daughter/ merged regions get created in OFFLINE state |  Major | . |
| [HBASE-19917](https://issues.apache.org/jira/browse/HBASE-19917) | Improve RSGroupBasedLoadBalancer#filterServers() to be more efficient |  Minor | rsgroup |
| [HBASE-19904](https://issues.apache.org/jira/browse/HBASE-19904) | Break dependency of WAL constructor on Replication |  Major | Replication, wal |
| [HBASE-19912](https://issues.apache.org/jira/browse/HBASE-19912) | The flag "writeToWAL" of Region#checkAndRowMutate is useless |  Minor | . |
| [HBASE-19861](https://issues.apache.org/jira/browse/HBASE-19861) | Avoid using RPCs when querying table infos for master status pages |  Major | UI |
| [HBASE-19770](https://issues.apache.org/jira/browse/HBASE-19770) | Add '--return-values' option to Shell to print return values of commands in interactive mode |  Critical | shell |
| [HBASE-19823](https://issues.apache.org/jira/browse/HBASE-19823) | Make RawCellBuilderFactory LimitedPrivate.UNITTEST |  Minor | . |
| [HBASE-19820](https://issues.apache.org/jira/browse/HBASE-19820) | Restore public constructor of MiniHBaseCluster (API compat) |  Major | . |
| [HBASE-19736](https://issues.apache.org/jira/browse/HBASE-19736) | Remove BaseLogCleanerDelegate deprecated #isLogDeletable(FileStatus) and use #isFileDeletable(FileStatus) instead |  Minor | . |
| [HBASE-19739](https://issues.apache.org/jira/browse/HBASE-19739) | Include thrift IDL files in HBase binary distribution |  Minor | Thrift |
| [HBASE-19789](https://issues.apache.org/jira/browse/HBASE-19789) | Not exclude flaky tests from nightly builds |  Major | . |
| [HBASE-19758](https://issues.apache.org/jira/browse/HBASE-19758) | Split TestHCM to several smaller tests |  Major | test |
| [HBASE-19751](https://issues.apache.org/jira/browse/HBASE-19751) | Use RegionInfo directly instead of an identifier and a namespace when getting WAL |  Major | wal |
| [HBASE-19139](https://issues.apache.org/jira/browse/HBASE-19139) | Create Async Admin methods for Clear Block Cache |  Major | Admin |
| [HBASE-19702](https://issues.apache.org/jira/browse/HBASE-19702) | Improve RSGroupInfo constructors |  Minor | . |
| [HBASE-19684](https://issues.apache.org/jira/browse/HBASE-19684) | BlockCacheKey toString Performance |  Trivial | hbase |
| [HBASE-19358](https://issues.apache.org/jira/browse/HBASE-19358) | Improve the stability of splitting log when do fail over |  Major | MTTR |
| [HBASE-19723](https://issues.apache.org/jira/browse/HBASE-19723) | hbase-thrift declares slf4j-api twice |  Trivial | Thrift |
| [HBASE-19651](https://issues.apache.org/jira/browse/HBASE-19651) | Remove LimitInputStream |  Minor | hbase |
| [HBASE-19473](https://issues.apache.org/jira/browse/HBASE-19473) | Miscellaneous changes to ClientScanner |  Trivial | hbase |
| [HBASE-19613](https://issues.apache.org/jira/browse/HBASE-19613) | Miscellaneous changes to WALSplitter |  Trivial | hbase |
| [HBASE-18806](https://issues.apache.org/jira/browse/HBASE-18806) | VerifyRep by snapshot need not to restore snapshot for each mapper |  Major | Replication |
| [HBASE-18011](https://issues.apache.org/jira/browse/HBASE-18011) | Refactor RpcServer |  Major | IPC/RPC |
| [HBASE-19641](https://issues.apache.org/jira/browse/HBASE-19641) | AsyncHBaseAdmin should use exponential backoff when polling the procedure result |  Major | asyncclient, proc-v2 |
| [HBASE-19675](https://issues.apache.org/jira/browse/HBASE-19675) | Miscellaneous HStore Class Improvements |  Minor | hbase |
| [HBASE-19683](https://issues.apache.org/jira/browse/HBASE-19683) | Remove Superfluous Methods From String Class |  Trivial | hbase |
| [HBASE-19676](https://issues.apache.org/jira/browse/HBASE-19676) | CleanerChore logging improvements |  Trivial | hbase |
| [HBASE-19486](https://issues.apache.org/jira/browse/HBASE-19486) |  Periodically ensure records are not buffered too long by BufferedMutator |  Major | Client |
| [HBASE-19679](https://issues.apache.org/jira/browse/HBASE-19679) | Superusers Logging and Data Structures |  Trivial | hbase |
| [HBASE-19677](https://issues.apache.org/jira/browse/HBASE-19677) | Miscellaneous HFileCleaner Improvements |  Trivial | hbase |
| [HBASE-19649](https://issues.apache.org/jira/browse/HBASE-19649) | Use singleton feature for ImmutableSegment |  Trivial | . |
| [HBASE-8518](https://issues.apache.org/jira/browse/HBASE-8518) | Get rid of hbase.hstore.compaction.complete setting |  Minor | . |
| [HBASE-19659](https://issues.apache.org/jira/browse/HBASE-19659) | Enable -x in make\_rc.sh so logs where it is in execution |  Trivial | build |
| [HBASE-19647](https://issues.apache.org/jira/browse/HBASE-19647) | Logging cleanups; emit regionname when RegionTooBusyException inside RetriesExhausted... make netty connect/disconnect TRACE-level |  Major | . |
| [HBASE-19545](https://issues.apache.org/jira/browse/HBASE-19545) | Replace getBytes(StandardCharsets.UTF\_8) with Bytes.toBytes |  Minor | . |
| [HBASE-19615](https://issues.apache.org/jira/browse/HBASE-19615) | CompositeImmutableSegment ArrayList Instead of LinkedList |  Trivial | hbase |
| [HBASE-19621](https://issues.apache.org/jira/browse/HBASE-19621) | Revisit the methods in ReplicationPeerConfigBuilder |  Minor | . |
| [HBASE-19618](https://issues.apache.org/jira/browse/HBASE-19618) | Remove replicationQueuesClient.class/replicationQueues.class config and remove table based ReplicationQueuesClient/ReplicationQueues implementation |  Major | . |
| [HBASE-19576](https://issues.apache.org/jira/browse/HBASE-19576) | Introduce builder for ReplicationPeerConfig and make it immutable |  Major | . |
| [HBASE-19590](https://issues.apache.org/jira/browse/HBASE-19590) | Remove the duplicate code in deprecated ReplicationAdmin |  Minor | . |
| [HBASE-19570](https://issues.apache.org/jira/browse/HBASE-19570) | Add hadoop3 tests to Nightly master/branch-2 runs |  Critical | . |
| [HBASE-19571](https://issues.apache.org/jira/browse/HBASE-19571) | Minor refactor of Nightly run scripts |  Minor | . |
| [HBASE-19491](https://issues.apache.org/jira/browse/HBASE-19491) | Exclude flaky tests from nightly master run |  Major | . |
| [HBASE-15482](https://issues.apache.org/jira/browse/HBASE-15482) | Provide an option to skip calculating block locations for SnapshotInputFormat |  Minor | mapreduce |
| [HBASE-19531](https://issues.apache.org/jira/browse/HBASE-19531) | Remove needless volatile declaration |  Trivial | . |
| [HBASE-19521](https://issues.apache.org/jira/browse/HBASE-19521) | HBase mob compaction need to check hfile version |  Critical | Compaction, mob |
| [HBASE-14790](https://issues.apache.org/jira/browse/HBASE-14790) | Implement a new DFSOutputStream for logging WAL only |  Major | wal |
| [HBASE-19472](https://issues.apache.org/jira/browse/HBASE-19472) | Remove ArrayUtil Class |  Major | hbase |
| [HBASE-19489](https://issues.apache.org/jira/browse/HBASE-19489) | Check against only the latest maintenance release in pre-commit hadoopcheck. |  Minor | . |
| [HBASE-17425](https://issues.apache.org/jira/browse/HBASE-17425) | Fix calls to deprecated APIs in TestUpdateConfiguration |  Trivial | Client |
| [HBASE-19464](https://issues.apache.org/jira/browse/HBASE-19464) | Replace StringBuffer with StringBuilder for hbase-common |  Trivial | hbase |
| [HBASE-19463](https://issues.apache.org/jira/browse/HBASE-19463) | Make CPEnv#getConnection return a facade that throws Unsupported if CP calls #close |  Major | Coprocessors |
| [HBASE-19180](https://issues.apache.org/jira/browse/HBASE-19180) | Remove unused imports from AlwaysPasses |  Trivial | build |
| [HBASE-18169](https://issues.apache.org/jira/browse/HBASE-18169) | Coprocessor fix and cleanup before 2.0.0 release |  Blocker | Coprocessors |
| [HBASE-19448](https://issues.apache.org/jira/browse/HBASE-19448) | Replace StringBuffer with StringBuilder for hbase-server |  Trivial | hbase |
| [HBASE-19432](https://issues.apache.org/jira/browse/HBASE-19432) | Roll the specified writer in HFileOutputFormat2 |  Major | . |
| [HBASE-19290](https://issues.apache.org/jira/browse/HBASE-19290) | Reduce zk request when doing split log |  Major | . |
| [HBASE-19336](https://issues.apache.org/jira/browse/HBASE-19336) | Improve rsgroup to allow assign all tables within a specified namespace by only writing namespace |  Major | rsgroup |
| [HBASE-19367](https://issues.apache.org/jira/browse/HBASE-19367) | Refactoring in RegionStates, and RSProcedureDispatcher |  Minor | . |
| [HBASE-19382](https://issues.apache.org/jira/browse/HBASE-19382) | Update report-flakies.py script to handle yetus builds |  Major | . |
| [HBASE-19252](https://issues.apache.org/jira/browse/HBASE-19252) | Move the transform logic of FilterList into transformCell() method to avoid extra ref to question cell |  Minor | . |
| [HBASE-19372](https://issues.apache.org/jira/browse/HBASE-19372) | Remove the Span object in SyncFuture as it is useless now |  Major | tracing, wal |
| [HBASE-18090](https://issues.apache.org/jira/browse/HBASE-18090) | Improve TableSnapshotInputFormat to allow more multiple mappers per region |  Major | mapreduce |
| [HBASE-16868](https://issues.apache.org/jira/browse/HBASE-16868) | Add a replicate\_all flag to avoid misuse the namespaces and table-cfs config of replication peer |  Critical | Replication |
| [HBASE-19311](https://issues.apache.org/jira/browse/HBASE-19311) | Promote TestAcidGuarantees to LargeTests and start mini cluster once to make it faster |  Major | test |
| [HBASE-19293](https://issues.apache.org/jira/browse/HBASE-19293) | Support adding a new replication peer in disabled state |  Major | . |
| [HBASE-16574](https://issues.apache.org/jira/browse/HBASE-16574) | Add backup / restore feature to refguide |  Major | . |
| [HBASE-19274](https://issues.apache.org/jira/browse/HBASE-19274) | Log IOException when unable to determine the size of committed file |  Trivial | . |
| [HBASE-19251](https://issues.apache.org/jira/browse/HBASE-19251) | Merge RawAsyncTable and AsyncTable |  Major | asyncclient, Client |
| [HBASE-19262](https://issues.apache.org/jira/browse/HBASE-19262) | Revisit checkstyle rules |  Major | build |
| [HBASE-18601](https://issues.apache.org/jira/browse/HBASE-18601) | Update Htrace to 4.2 |  Major | dependencies, tracing |
| [HBASE-19227](https://issues.apache.org/jira/browse/HBASE-19227) | Nightly jobs should archive JVM dumpstream files |  Critical | build |
| [HBASE-12350](https://issues.apache.org/jira/browse/HBASE-12350) | Backport error-prone build support to branch-1 and branch-2 |  Minor | . |
| [HBASE-19228](https://issues.apache.org/jira/browse/HBASE-19228) | nightly job should gather machine stats. |  Major | build |
| [HBASE-19186](https://issues.apache.org/jira/browse/HBASE-19186) | Unify to use bytes to show size in master/rs ui |  Minor | . |
| [HBASE-19027](https://issues.apache.org/jira/browse/HBASE-19027) | Honor the CellComparator of ScanInfo in scanning over a store |  Major | . |
| [HBASE-13622](https://issues.apache.org/jira/browse/HBASE-13622) | document upgrade rollback |  Major | documentation |
| [HBASE-18925](https://issues.apache.org/jira/browse/HBASE-18925) | Need updated mockito for using java optional |  Major | . |
| [HBASE-19140](https://issues.apache.org/jira/browse/HBASE-19140) | hbase-cleanup.sh uses deprecated call to remove files in hdfs |  Trivial | scripts |
| [HBASE-17065](https://issues.apache.org/jira/browse/HBASE-17065) | Perform more effective sorting for RPC Handler Tasks |  Minor | . |
| [HBASE-18870](https://issues.apache.org/jira/browse/HBASE-18870) | Hbase Backup should set the details to MR job name |  Minor | . |
| [HBASE-18602](https://issues.apache.org/jira/browse/HBASE-18602) | rsgroup cleanup unassign code |  Minor | rsgroup |
| [HBASE-19110](https://issues.apache.org/jira/browse/HBASE-19110) | Add default for Server#isStopping & #getFileSystem |  Minor | . |
| [HBASE-19091](https://issues.apache.org/jira/browse/HBASE-19091) | Code annotation wrote "BinaryComparator" instead of "LongComparator" |  Minor | Client |
| [HBASE-18994](https://issues.apache.org/jira/browse/HBASE-18994) | Decide if META/System tables should use Compacting Memstore or Default Memstore |  Major | . |
| [HBASE-19051](https://issues.apache.org/jira/browse/HBASE-19051) | Add new split algorithm for num string |  Minor | . |
| [HBASE-18824](https://issues.apache.org/jira/browse/HBASE-18824) | Add meaningful comment to HConstants.LATEST\_TIMESTAMP to explain why it is MAX\_VALUE |  Minor | . |
| [HBASE-10367](https://issues.apache.org/jira/browse/HBASE-10367) | RegionServer graceful stop / decommissioning |  Major | . |
| [HBASE-18986](https://issues.apache.org/jira/browse/HBASE-18986) | Remove unnecessary null check after CellUtil.cloneQualifier() |  Minor | . |
| [HBASE-15410](https://issues.apache.org/jira/browse/HBASE-15410) | Utilize the max seek value when all Filters in MUST\_PASS\_ALL FilterList return SEEK\_NEXT\_USING\_HINT |  Major | . |
| [HBASE-18843](https://issues.apache.org/jira/browse/HBASE-18843) | Add DistCp support to incremental backup with bulk loading |  Major | . |
| [HBASE-16010](https://issues.apache.org/jira/browse/HBASE-16010) | Put draining function through Admin API |  Minor | . |
| [HBASE-18899](https://issues.apache.org/jira/browse/HBASE-18899) | Make Fileinfo more readable in HFilePrettyPrinter |  Major | HFile |
| [HBASE-16894](https://issues.apache.org/jira/browse/HBASE-16894) | Create more than 1 split per region, generalize HBASE-12590 |  Major | . |
| [HBASE-18929](https://issues.apache.org/jira/browse/HBASE-18929) | Hbase backup command doesn’t show debug option to enable backup in debug mode |  Minor | . |
| [HBASE-18814](https://issues.apache.org/jira/browse/HBASE-18814) | Make ScanMetrics enabled and add counter \<HBase Counters, ROWS\_SCANNED\> into the MapReduce Job over snapshot |  Minor | mapreduce |
| [HBASE-18559](https://issues.apache.org/jira/browse/HBASE-18559) | Add histogram to MetricsConnection to track concurrent calls per server |  Minor | Client |
| [HBASE-18436](https://issues.apache.org/jira/browse/HBASE-18436) | Add client-side hedged read metrics |  Minor | . |
| [HBASE-13844](https://issues.apache.org/jira/browse/HBASE-13844) | Move static helper methods from KeyValue into CellUtils |  Minor | . |
| [HBASE-18884](https://issues.apache.org/jira/browse/HBASE-18884) | Coprocessor Design Improvements follow up of HBASE-17732 |  Major | Coprocessors |
| [HBASE-18652](https://issues.apache.org/jira/browse/HBASE-18652) | Expose individual cache stats in a CombinedCache through JMX |  Major | regionserver |
| [HBASE-18651](https://issues.apache.org/jira/browse/HBASE-18651) | Let ChaosMonkeyRunner expose the chaos monkey runner it creates |  Major | . |
| [HBASE-11462](https://issues.apache.org/jira/browse/HBASE-11462) | MetaTableAccessor shouldn't use ZooKeeeper |  Major | Client, Zookeeper |
| [HBASE-18478](https://issues.apache.org/jira/browse/HBASE-18478) | Allow users to remove RegionFinder from LoadBalancer calculations if no locality possible |  Major | Balancer |
| [HBASE-18849](https://issues.apache.org/jira/browse/HBASE-18849) | expand "thirdparty" reference to give examples of setting netty location in common testing modules |  Critical | documentation, thirdparty |
| [HBASE-18609](https://issues.apache.org/jira/browse/HBASE-18609) | Apply ClusterStatus#getClusterStatus(EnumSet\<Option\>) in code base |  Major | . |
| [HBASE-18795](https://issues.apache.org/jira/browse/HBASE-18795) | Expose KeyValue.getBuffer() for tests alone |  Major | . |
| [HBASE-18772](https://issues.apache.org/jira/browse/HBASE-18772) | [JDK8]  Replace AtomicLong with LongAdder |  Trivial | . |
| [HBASE-14996](https://issues.apache.org/jira/browse/HBASE-14996) | Some more API cleanup for 2.0 |  Blocker | . |
| [HBASE-18683](https://issues.apache.org/jira/browse/HBASE-18683) | Upgrade hbase to commons-math 3 |  Major | . |
| [HBASE-13271](https://issues.apache.org/jira/browse/HBASE-13271) | Table#puts(List\<Put\>) operation is indeterminate; needs fixing |  Critical | API |
| [HBASE-10240](https://issues.apache.org/jira/browse/HBASE-10240) | Remove 0.94-\>0.96 migration code |  Critical | . |
| [HBASE-18662](https://issues.apache.org/jira/browse/HBASE-18662) | The default values for many configuration items in the code are not consistent with hbase-default.xml |  Minor | . |
| [HBASE-18621](https://issues.apache.org/jira/browse/HBASE-18621) | Refactor ClusterOptions before applying to code base |  Major | . |
| [HBASE-18778](https://issues.apache.org/jira/browse/HBASE-18778) | Use Comparator for StealJobQueue |  Major | Compaction |
| [HBASE-18737](https://issues.apache.org/jira/browse/HBASE-18737) | Display configured max size of memstore and cache on RS UI |  Minor | . |
| [HBASE-18674](https://issues.apache.org/jira/browse/HBASE-18674) | upgrade hbase to commons-lang3 |  Major | . |
| [HBASE-18746](https://issues.apache.org/jira/browse/HBASE-18746) | Throw exception with job.getStatus().getFailureInfo() when ExportSnapshot fails |  Minor | mapreduce, snapshots |
| [HBASE-18740](https://issues.apache.org/jira/browse/HBASE-18740) | Upgrade Zookeeper version to 3.4.10 |  Major | . |
| [HBASE-18699](https://issues.apache.org/jira/browse/HBASE-18699) | Copy LoadIncrementalHFiles to another package and mark the old one as deprecated |  Major | mapreduce |
| [HBASE-18675](https://issues.apache.org/jira/browse/HBASE-18675) | Making {max,min}SessionTimeout configurable for MiniZooKeeperCluster |  Minor | test, Zookeeper |
| [HBASE-17826](https://issues.apache.org/jira/browse/HBASE-17826) | Backup: submit M/R job to a particular Yarn queue |  Major | . |
| [HBASE-18677](https://issues.apache.org/jira/browse/HBASE-18677) | typo in namespace docs |  Trivial | documentation |
| [HBASE-18701](https://issues.apache.org/jira/browse/HBASE-18701) | Optimize reference guide to use cell acl conveniently |  Trivial | . |
| [HBASE-18519](https://issues.apache.org/jira/browse/HBASE-18519) | Use builder pattern to create cell |  Major | . |
| [HBASE-18673](https://issues.apache.org/jira/browse/HBASE-18673) | Some more unwanted reference to unshaded PB classes |  Minor | . |
| [HBASE-18224](https://issues.apache.org/jira/browse/HBASE-18224) | Upgrade jetty |  Critical | dependencies |
| [HBASE-18532](https://issues.apache.org/jira/browse/HBASE-18532) | Improve cache related stats rendered on RS UI |  Major | regionserver, UI |
| [HBASE-18629](https://issues.apache.org/jira/browse/HBASE-18629) | Enhance ChaosMonkeyRunner with interruptibility |  Major | . |
| [HBASE-18631](https://issues.apache.org/jira/browse/HBASE-18631) | Allow configuration of ChaosMonkey properties via hbase-site |  Minor | integration tests |
| [HBASE-18573](https://issues.apache.org/jira/browse/HBASE-18573) | Update Append and Delete to use Mutation#getCellList(family) |  Minor | . |
| [HBASE-18251](https://issues.apache.org/jira/browse/HBASE-18251) | Remove unnecessary traversing to the first and last keys in the CellSet |  Major | . |
| [HBASE-18581](https://issues.apache.org/jira/browse/HBASE-18581) | Remove dead code and some tidy up in BaseLoadBalancer |  Minor | Balancer |
| [HBASE-18504](https://issues.apache.org/jira/browse/HBASE-18504) | Add documentation for WAL compression |  Minor | documentation |
| [HBASE-17064](https://issues.apache.org/jira/browse/HBASE-17064) | Add TaskMonitor#getTasks() variant which accepts type selection |  Minor | . |
| [HBASE-2631](https://issues.apache.org/jira/browse/HBASE-2631) | Decide between "InMB" and "MB" as suffix for field names in ClusterStatus objects |  Minor | . |
| [HBASE-18533](https://issues.apache.org/jira/browse/HBASE-18533) | Expose BucketCache values to be configured |  Major | BucketCache |
| [HBASE-18303](https://issues.apache.org/jira/browse/HBASE-18303) | Clean up some parameterized test declarations |  Minor | test |
| [HBASE-18522](https://issues.apache.org/jira/browse/HBASE-18522) | Add RowMutations support to Batch |  Major | . |
| [HBASE-18566](https://issues.apache.org/jira/browse/HBASE-18566) | [RSGROUP]Log the client IP/port of the rsgroup admin |  Major | rsgroup |
| [HBASE-15511](https://issues.apache.org/jira/browse/HBASE-15511) | ClusterStatus should be able to return responses by scope |  Major | . |
| [HBASE-18555](https://issues.apache.org/jira/browse/HBASE-18555) | Remove redundant familyMap.put() from addxxx() of sub-classes of Mutation and Query |  Minor | Client |
| [HBASE-18387](https://issues.apache.org/jira/browse/HBASE-18387) | [Thrift] Make principal configurable in DemoClient.java |  Minor | . |
| [HBASE-18548](https://issues.apache.org/jira/browse/HBASE-18548) | Move sources of important Jenkins jobs into source control |  Critical | documentation, scripts |
| [HBASE-18248](https://issues.apache.org/jira/browse/HBASE-18248) | Warn if monitored RPC task has been tied up beyond a configurable threshold |  Major | . |
| [HBASE-18485](https://issues.apache.org/jira/browse/HBASE-18485) | Performance issue: ClientAsyncPrefetchScanner is slower than ClientSimpleScanner |  Major | . |
| [HBASE-18426](https://issues.apache.org/jira/browse/HBASE-18426) | nightly job should use independent stages to check supported jdks |  Critical | community, test |
| [HBASE-14220](https://issues.apache.org/jira/browse/HBASE-14220) | nightly tests should verify src tgz generates and builds correctly |  Minor | build |
| [HBASE-18520](https://issues.apache.org/jira/browse/HBASE-18520) | Add jmx value to determine true Master Start time |  Minor | metrics |
| [HBASE-16893](https://issues.apache.org/jira/browse/HBASE-16893) | Use Collection.removeIf instead of Iterator.remove in DependentColumnFilter |  Minor | . |
| [HBASE-16116](https://issues.apache.org/jira/browse/HBASE-16116) | Remove redundant pattern \*.iml |  Trivial | . |
| [HBASE-18261](https://issues.apache.org/jira/browse/HBASE-18261) | [AMv2] Create new RecoverMetaProcedure and use it from ServerCrashProcedure and HMaster.finishActiveMasterInitialization() |  Major | amv2 |
| [HBASE-18402](https://issues.apache.org/jira/browse/HBASE-18402) | Thrift2 should support  DeleteFamilyVersion type |  Major | Thrift |
| [HBASE-18434](https://issues.apache.org/jira/browse/HBASE-18434) | Address some alerts raised by lgtm.com |  Major | . |
| [HBASE-18023](https://issues.apache.org/jira/browse/HBASE-18023) | Log multi-\* requests for more than threshold number of rows |  Minor | regionserver |
| [HBASE-15816](https://issues.apache.org/jira/browse/HBASE-15816) | Provide client with ability to set priority on Operations |  Major | . |
| [HBASE-18389](https://issues.apache.org/jira/browse/HBASE-18389) | Remove byte[] from formal parameter of sizeOf() of ClassSize, ClassSize.MemoryLayout and ClassSize.UnsafeLayout |  Minor | util |
| [HBASE-18412](https://issues.apache.org/jira/browse/HBASE-18412) | [Shell] Support unset of list of configuration for a table |  Minor | . |
| [HBASE-16312](https://issues.apache.org/jira/browse/HBASE-16312) | update jquery version |  Critical | dependencies, UI |
| [HBASE-18332](https://issues.apache.org/jira/browse/HBASE-18332) | Upgrade asciidoctor-maven-plugin |  Minor | website |
| [HBASE-18339](https://issues.apache.org/jira/browse/HBASE-18339) | Update test-patch to use hadoop 3.0.0-alpha4 |  Major | test |
| [HBASE-18004](https://issues.apache.org/jira/browse/HBASE-18004) | getRegionLocations  needs to be called once in ScannerCallableWithReplicas#call() |  Minor | Client |
| [HBASE-18307](https://issues.apache.org/jira/browse/HBASE-18307) | Share the same EventLoopGroup for NettyRpcServer, NettyRpcClient and AsyncFSWALProvider at RS side |  Major | io, rpc, wal |
| [HBASE-18083](https://issues.apache.org/jira/browse/HBASE-18083) | Make large/small file clean thread number configurable in HFileCleaner |  Major | . |
| [HBASE-16730](https://issues.apache.org/jira/browse/HBASE-16730) | Exclude junit as a transitive dependency from hadoop-common |  Trivial | hbase |
| [HBASE-15062](https://issues.apache.org/jira/browse/HBASE-15062) | IntegrationTestMTTR conditionally run some tests |  Minor | integration tests |
| [HBASE-11707](https://issues.apache.org/jira/browse/HBASE-11707) | Using Map instead of list in FailedServers of RpcClient |  Minor | Client |
| [HBASE-18286](https://issues.apache.org/jira/browse/HBASE-18286) | Create static empty byte array to save memory |  Trivial | community |
| [HBASE-17995](https://issues.apache.org/jira/browse/HBASE-17995) | improve log messages during snapshot related tests |  Trivial | integration tests, mapreduce, snapshots, test |
| [HBASE-15391](https://issues.apache.org/jira/browse/HBASE-15391) | Avoid too large "deleted from META" info log |  Minor | . |
| [HBASE-16585](https://issues.apache.org/jira/browse/HBASE-16585) | Rewrite the delegation token tests with Parameterized pattern |  Major | security, test |
| [HBASE-13197](https://issues.apache.org/jira/browse/HBASE-13197) | Connection API cleanup |  Major | API |
| [HBASE-15756](https://issues.apache.org/jira/browse/HBASE-15756) | Pluggable RpcServer |  Critical | Performance, rpc |
| [HBASE-17110](https://issues.apache.org/jira/browse/HBASE-17110) | Improve SimpleLoadBalancer to always take server-level balance into account |  Major | Balancer |
| [HBASE-18022](https://issues.apache.org/jira/browse/HBASE-18022) | Refine the error message issued with TableNotFoundException when expected table is not the same as the one fetched from meta |  Minor | . |
| [HBASE-18281](https://issues.apache.org/jira/browse/HBASE-18281) | Performance update in StoreFileWriter.java for string replacement |  Trivial | community |
| [HBASE-18041](https://issues.apache.org/jira/browse/HBASE-18041) | Add pylintrc file to HBase |  Major | community |
| [HBASE-18275](https://issues.apache.org/jira/browse/HBASE-18275) | Formatting and grammar mistakes in schemadoc chapter |  Trivial | documentation |
| [HBASE-18164](https://issues.apache.org/jira/browse/HBASE-18164) | Much faster locality cost function and candidate generator |  Critical | Balancer |
| [HBASE-16351](https://issues.apache.org/jira/browse/HBASE-16351) | do dependency license check via enforcer plugin |  Major | build, dependencies |
| [HBASE-18252](https://issues.apache.org/jira/browse/HBASE-18252) | Resolve BaseLoadBalancer bad practice warnings |  Minor | . |
| [HBASE-18033](https://issues.apache.org/jira/browse/HBASE-18033) | Update supplemental models for new deps in Hadoop trunk |  Critical | dependencies |
| [HBASE-17959](https://issues.apache.org/jira/browse/HBASE-17959) | Canary timeout should be configurable on a per-table basis |  Minor | canary |
| [HBASE-17777](https://issues.apache.org/jira/browse/HBASE-17777) | TestMemstoreLAB#testLABThreading runs too long for a small test |  Minor | test |
| [HBASE-18101](https://issues.apache.org/jira/browse/HBASE-18101) | Fix type mismatch on container access in QuotaCache#chore |  Trivial | . |
| [HBASE-18001](https://issues.apache.org/jira/browse/HBASE-18001) | Extend the "count" shell command to support specified conditions |  Minor | shell |
| [HBASE-18094](https://issues.apache.org/jira/browse/HBASE-18094) | Display the return value of the command append |  Major | shell |
| [HBASE-18075](https://issues.apache.org/jira/browse/HBASE-18075) | Support namespaces and tables with non-latin alphabetical characters |  Major | Client |
| [HBASE-18067](https://issues.apache.org/jira/browse/HBASE-18067) | Support a default converter for data read shell commands |  Minor | shell |
| [HBASE-18019](https://issues.apache.org/jira/browse/HBASE-18019) | Close redundant memstore scanners |  Major | . |
| [HBASE-17910](https://issues.apache.org/jira/browse/HBASE-17910) | Use separated StoreFileReader for streaming read |  Major | regionserver, scan, Scanners |
| [HBASE-18043](https://issues.apache.org/jira/browse/HBASE-18043) | Institute a hard limit for individual cell size that cannot be overridden by clients |  Major | IPC/RPC, regionserver |
| [HBASE-17343](https://issues.apache.org/jira/browse/HBASE-17343) | Make Compacting Memstore default in 2.0 with BASIC as the default type |  Blocker | regionserver |
| [HBASE-18021](https://issues.apache.org/jira/browse/HBASE-18021) | Add more info in timed out RetriesExhaustedException for read replica client get processing, |  Minor | Client |
| [HBASE-18017](https://issues.apache.org/jira/browse/HBASE-18017) | Reduce frequency of setStoragePolicy failure warnings |  Minor | . |
| [HBASE-14925](https://issues.apache.org/jira/browse/HBASE-14925) | Develop HBase shell command/tool to list table's region info through command line |  Major | shell |
| [HBASE-17924](https://issues.apache.org/jira/browse/HBASE-17924) | Consider sorting the row order when processing multi() ops before taking rowlocks |  Major | . |
| [HBASE-18015](https://issues.apache.org/jira/browse/HBASE-18015) | Storage class aware block placement for procedure v2 WALs |  Minor | . |
| [HBASE-18007](https://issues.apache.org/jira/browse/HBASE-18007) | Clean up rest module code |  Trivial | . |
| [HBASE-17990](https://issues.apache.org/jira/browse/HBASE-17990) | Refactor TestFSUtils to use Before |  Trivial | . |
| [HBASE-16466](https://issues.apache.org/jira/browse/HBASE-16466) | HBase snapshots support in VerifyReplication tool to reduce load on live HBase cluster with large tables |  Major | hbase |
| [HBASE-12870](https://issues.apache.org/jira/browse/HBASE-12870) | "Major compaction triggered" and "Skipping major compaction" messages lack the region information |  Major | Compaction |
| [HBASE-17973](https://issues.apache.org/jira/browse/HBASE-17973) | Create shell command to identify regions with poor locality |  Major | shell |
| [HBASE-17979](https://issues.apache.org/jira/browse/HBASE-17979) | HBase Shell 'list' Command Help Doc Improvements |  Minor | shell |
| [HBASE-17875](https://issues.apache.org/jira/browse/HBASE-17875) | Document why objects over 10MB are not well-suited for hbase |  Major | documentation, mob |
| [HBASE-8486](https://issues.apache.org/jira/browse/HBASE-8486) | IS\_ROOT isnt needed in HTableDescriptor. |  Minor | . |
| [HBASE-17817](https://issues.apache.org/jira/browse/HBASE-17817) | Make Regionservers log which tables it removed coprocessors from when aborting |  Major | Coprocessors, regionserver |
| [HBASE-17835](https://issues.apache.org/jira/browse/HBASE-17835) | Spelling mistakes in the Java source |  Trivial | documentation |
| [HBASE-17962](https://issues.apache.org/jira/browse/HBASE-17962) | Improve documentation on Rest interface |  Trivial | documentation, REST |
| [HBASE-17877](https://issues.apache.org/jira/browse/HBASE-17877) | Improve HBase's byte[] comparator |  Major | util |
| [HBASE-9899](https://issues.apache.org/jira/browse/HBASE-9899) | for idempotent operation dups, return the result instead of throwing conflict exception |  Major | . |
| [HBASE-17514](https://issues.apache.org/jira/browse/HBASE-17514) | Warn when Thrift Server 1 is configured for proxy users but not the HTTP transport |  Minor | Thrift, Usability |
| [HBASE-17944](https://issues.apache.org/jira/browse/HBASE-17944) | Removed unused JDK version parsing from ClassSize. |  Minor | build |
| [HBASE-17912](https://issues.apache.org/jira/browse/HBASE-17912) | Avoid major compactions on region server startup |  Major | Compaction |
| [HBASE-17888](https://issues.apache.org/jira/browse/HBASE-17888) | Add generic methods for updating metrics on start and end of a procedure execution |  Major | proc-v2 |
| [HBASE-16469](https://issues.apache.org/jira/browse/HBASE-16469) | Several log refactoring/improvement suggestions |  Major | Operability |
| [HBASE-17836](https://issues.apache.org/jira/browse/HBASE-17836) | CellUtil#estimatedSerializedSizeOf is slow when input is ByteBufferCell |  Minor | . |
| [HBASE-16969](https://issues.apache.org/jira/browse/HBASE-16969) | RegionCoprocessorServiceExec should override the toString() for debugging |  Minor | . |
| [HBASE-17854](https://issues.apache.org/jira/browse/HBASE-17854) | Use StealJobQueue in HFileCleaner after HBASE-17215 |  Major | . |
| [HBASE-17215](https://issues.apache.org/jira/browse/HBASE-17215) | Separate small/large file delete threads in HFileCleaner to accelerate archived hfile cleanup speed |  Major | . |
| [HBASE-17831](https://issues.apache.org/jira/browse/HBASE-17831) | Support small scan in thrift2 |  Major | Thrift |
| [HBASE-17623](https://issues.apache.org/jira/browse/HBASE-17623) | Reuse the bytes array when building the hfile block |  Major | HFile |
| [HBASE-17815](https://issues.apache.org/jira/browse/HBASE-17815) | Remove the unused field in PrefixTreeSeeker |  Trivial | . |
| [HBASE-17778](https://issues.apache.org/jira/browse/HBASE-17778) | Remove the testing code in the AsyncRequestFutureImpl |  Trivial | . |
| [HBASE-15339](https://issues.apache.org/jira/browse/HBASE-15339) | Improve DateTieredCompactionPolicy |  Major | Compaction |
| [HBASE-17747](https://issues.apache.org/jira/browse/HBASE-17747) | Support both weak and soft object pool |  Major | . |
| [HBASE-15429](https://issues.apache.org/jira/browse/HBASE-15429) | Add a split policy for busy regions |  Major | regionserver |
| [HBASE-17731](https://issues.apache.org/jira/browse/HBASE-17731) | Fractional latency reporting in MultiThreadedAction |  Trivial | . |
| [HBASE-16977](https://issues.apache.org/jira/browse/HBASE-16977) | VerifyReplication should log a printable representation of the row keys |  Minor | Replication |
| [HBASE-15941](https://issues.apache.org/jira/browse/HBASE-15941) | HBCK repair should not unsplit healthy splitted region |  Major | hbck |
| [HBASE-17532](https://issues.apache.org/jira/browse/HBASE-17532) | Replace explicit type with diamond operator |  Trivial | build |
| [HBASE-17734](https://issues.apache.org/jira/browse/HBASE-17734) | Guard against possibly copying the qualifier in the ScanDeleteTracker |  Minor | . |
| [HBASE-17690](https://issues.apache.org/jira/browse/HBASE-17690) | Clean up MOB code |  Major | mob |
| [HBASE-16188](https://issues.apache.org/jira/browse/HBASE-16188) | Add EventCounter information to log4j properties file |  Minor | . |
| [HBASE-17689](https://issues.apache.org/jira/browse/HBASE-17689) | Add support for table.existsAll in thrift2 THBaseservice |  Major | Thrift |
| [HBASE-17634](https://issues.apache.org/jira/browse/HBASE-17634) | Clean up the usage of Result.isPartial |  Major | scan |
| [HBASE-17654](https://issues.apache.org/jira/browse/HBASE-17654) | RSGroup code refactoring |  Major | rsgroup |
| [HBASE-17057](https://issues.apache.org/jira/browse/HBASE-17057) | Minor compactions should also drop page cache behind reads |  Major | Compaction |
| [HBASE-17676](https://issues.apache.org/jira/browse/HBASE-17676) | Get class name once for all in AbstractFSWAL |  Major | Performance |
| [HBASE-13718](https://issues.apache.org/jira/browse/HBASE-13718) | Add a pretty printed table description to the table detail page of HBase's master |  Minor | hbase |
| [HBASE-17172](https://issues.apache.org/jira/browse/HBASE-17172) | Optimize mob compaction with \_del files |  Major | mob |
| [HBASE-17627](https://issues.apache.org/jira/browse/HBASE-17627) | Active workers metric for thrift |  Major | Thrift |
| [HBASE-17637](https://issues.apache.org/jira/browse/HBASE-17637) | Update progress more frequently in IntegrationTestBigLinkedList.Generator.persist |  Minor | . |
| [HBASE-17605](https://issues.apache.org/jira/browse/HBASE-17605) | Refactor procedure framework code |  Major | proc-v2 |
| [HBASE-17280](https://issues.apache.org/jira/browse/HBASE-17280) | Add mechanism to control hbase cleaner behavior |  Minor | Client, hbase, shell |
| [HBASE-17613](https://issues.apache.org/jira/browse/HBASE-17613) | avoid copy of family when initializing the FSWALEntry |  Minor | . |
| [HBASE-17437](https://issues.apache.org/jira/browse/HBASE-17437) | Support specifying a WAL directory outside of the root directory |  Major | Filesystem Integration, wal |
| [HBASE-17592](https://issues.apache.org/jira/browse/HBASE-17592) | Fix typo in IPCUtil and RpcConnection |  Trivial | . |
| [HBASE-17588](https://issues.apache.org/jira/browse/HBASE-17588) | Remove unused imports brought in by HBASE-17437 |  Trivial | . |
| [HBASE-17552](https://issues.apache.org/jira/browse/HBASE-17552) | Update developer section in hbase book |  Major | . |
| [HBASE-17543](https://issues.apache.org/jira/browse/HBASE-17543) | Create additional ReplicationEndpoint WALEntryFilters by configuration |  Major | Replication |
| [HBASE-17569](https://issues.apache.org/jira/browse/HBASE-17569) | HBase-Procedure module need to support mvn clean test -PskipProcedureTests to skip unit test |  Minor | proc-v2 |
| [HBASE-17555](https://issues.apache.org/jira/browse/HBASE-17555) | Change calls to deprecated getHBaseAdmin to getAdmin |  Minor | . |
| [HBASE-17563](https://issues.apache.org/jira/browse/HBASE-17563) | Foreach and switch in RootDocProcessor and StabilityOptions |  Trivial | . |
| [HBASE-17515](https://issues.apache.org/jira/browse/HBASE-17515) | Reduce memory footprint of RegionLoads kept by StochasticLoadBalancer |  Major | . |
| [HBASE-17462](https://issues.apache.org/jira/browse/HBASE-17462) | Use sliding window for read/write request costs in StochasticLoadBalancer |  Major | . |
| [HBASE-16698](https://issues.apache.org/jira/browse/HBASE-16698) | Performance issue: handlers stuck waiting for CountDownLatch inside WALKey#getWriteEntry under high writing workload |  Major | Performance |
| [HBASE-17404](https://issues.apache.org/jira/browse/HBASE-17404) | Replace explicit type with diamond operator in hbase-annotations |  Trivial | . |
| [HBASE-10699](https://issues.apache.org/jira/browse/HBASE-10699) | Optimize some code; set capacity on arraylist when possible; use isEmpty; reduce allocations |  Major | . |
| [HBASE-17488](https://issues.apache.org/jira/browse/HBASE-17488) | WALEdit should be lazily instantiated |  Trivial | . |
| [HBASE-17408](https://issues.apache.org/jira/browse/HBASE-17408) | Introduce per request limit by number of mutations |  Major | . |
| [HBASE-17291](https://issues.apache.org/jira/browse/HBASE-17291) | Remove ImmutableSegment#getKeyValueScanner |  Major | Scanners |
| [HBASE-17314](https://issues.apache.org/jira/browse/HBASE-17314) | Limit total buffered size for all replication sources |  Major | Replication |
| [HBASE-17348](https://issues.apache.org/jira/browse/HBASE-17348) | Remove the unused hbase.replication from javadoc/comment/book completely |  Trivial | . |
| [HBASE-15924](https://issues.apache.org/jira/browse/HBASE-15924) | Enhance hbase services autorestart capability to hbase-daemon.sh |  Major | . |
| [HBASE-17292](https://issues.apache.org/jira/browse/HBASE-17292) | Add observer notification before bulk loaded hfile is moved to region directory |  Major | . |
| [HBASE-17332](https://issues.apache.org/jira/browse/HBASE-17332) | Replace HashMap to Array for DataBlockEncoding.idToEncoding |  Major | . |
| [HBASE-17331](https://issues.apache.org/jira/browse/HBASE-17331) | Avoid busy waiting in ThrottledInputStream |  Minor | . |
| [HBASE-17318](https://issues.apache.org/jira/browse/HBASE-17318) | Increment does not add new column if the increment amount is zero at first time writing |  Major | . |
| [HBASE-17298](https://issues.apache.org/jira/browse/HBASE-17298) | remove unused code in HRegion#doMiniBatchMutation |  Minor | regionserver |
| [HBASE-17296](https://issues.apache.org/jira/browse/HBASE-17296) | Provide per peer throttling for replication |  Major | Replication |
| [HBASE-17276](https://issues.apache.org/jira/browse/HBASE-17276) | Reduce log spam from WrongRegionException in large multi()'s |  Minor | regionserver |
| [HBASE-17241](https://issues.apache.org/jira/browse/HBASE-17241) | Avoid compacting already compacted  mob files with \_del files |  Major | mob |
| [HBASE-17207](https://issues.apache.org/jira/browse/HBASE-17207) | Arrays.asList() with too few arguments |  Trivial | . |
| [HBASE-16700](https://issues.apache.org/jira/browse/HBASE-16700) | Allow for coprocessor whitelisting |  Minor | Coprocessors |
| [HBASE-17239](https://issues.apache.org/jira/browse/HBASE-17239) | Add UnsafeByteOperations#wrap(ByteInput, int offset, int len) API |  Major | Protobufs |
| [HBASE-17245](https://issues.apache.org/jira/browse/HBASE-17245) | Replace HTableDescriptor#htd.getColumnFamilies().length with a low-cost implementation |  Minor | . |
| [HBASE-14882](https://issues.apache.org/jira/browse/HBASE-14882) | Provide a Put API that adds the provided family, qualifier, value without copying |  Major | . |
| [HBASE-17243](https://issues.apache.org/jira/browse/HBASE-17243) | Reuse CompactionPartitionId and avoid creating MobFileName in PartitionedMobCompactor to avoid unnecessary new objects |  Minor | mob |
| [HBASE-17194](https://issues.apache.org/jira/browse/HBASE-17194) | Assign the new region to the idle server after splitting |  Minor | . |
| [HBASE-17232](https://issues.apache.org/jira/browse/HBASE-17232) | Replace HashSet with ArrayList to accumulate delayed scanners in KVHeap and StoreScanner. |  Major | . |
| [HBASE-17161](https://issues.apache.org/jira/browse/HBASE-17161) | MOB : Make ref cell creation more efficient |  Major | . |
| [HBASE-17235](https://issues.apache.org/jira/browse/HBASE-17235) | Improvement in creation of CIS for onheap buffer cases |  Major | rpc |
| [HBASE-17191](https://issues.apache.org/jira/browse/HBASE-17191) | Make use of UnsafeByteOperations#unsafeWrap(ByteBuffer buffer) in PBUtil#toCell(Cell cell) |  Major | . |
| [HBASE-17205](https://issues.apache.org/jira/browse/HBASE-17205) | Add a metric for the duration of region in transition |  Minor | Region Assignment |
| [HBASE-17216](https://issues.apache.org/jira/browse/HBASE-17216) | A Few Fields Can Be Safely Made Static |  Major | . |
| [HBASE-17212](https://issues.apache.org/jira/browse/HBASE-17212) | Should add null checker on table name in HTable constructor and RegionServerCallable |  Major | . |
| [HBASE-17184](https://issues.apache.org/jira/browse/HBASE-17184) | Code cleanup of LruBlockCache |  Trivial | . |
| [HBASE-17211](https://issues.apache.org/jira/browse/HBASE-17211) | Add more details in log when UnknownScannerException thrown in ScannerCallable |  Minor | Operability |
| [HBASE-17178](https://issues.apache.org/jira/browse/HBASE-17178) | Add region balance throttling |  Major | Balancer |
| [HBASE-16302](https://issues.apache.org/jira/browse/HBASE-16302) | age of last shipped op and age of last applied op should be histograms |  Major | Replication |
| [HBASE-16561](https://issues.apache.org/jira/browse/HBASE-16561) | Add metrics about read/write/scan queue length and active read/write/scan handler count |  Minor | IPC/RPC, metrics |
| [HBASE-17086](https://issues.apache.org/jira/browse/HBASE-17086) | Add comments to explain why Cell#getTagsLength() returns an int, rather than a short |  Minor |  Interface |
| [HBASE-17176](https://issues.apache.org/jira/browse/HBASE-17176) | Reuse the builder in RequestConverter |  Minor | . |
| [HBASE-17157](https://issues.apache.org/jira/browse/HBASE-17157) | Increase the default mergeable threshold for mob compaction |  Major | mob |
| [HBASE-17129](https://issues.apache.org/jira/browse/HBASE-17129) | Remove public from methods in DataType interface |  Minor | . |
| [HBASE-17123](https://issues.apache.org/jira/browse/HBASE-17123) | Add postBulkLoadHFile variant that notifies the final paths for the hfiles |  Major | . |
| [HBASE-17126](https://issues.apache.org/jira/browse/HBASE-17126) | Expose KeyValue#checkParameters() and checkForTagsLength() to be used by other Cell implementations |  Minor | Client, regionserver |
| [HBASE-17088](https://issues.apache.org/jira/browse/HBASE-17088) | Refactor RWQueueRpcExecutor/BalancedQueueRpcExecutor/RpcExecutor |  Major | rpc |
| [HBASE-16708](https://issues.apache.org/jira/browse/HBASE-16708) | Expose endpoint Coprocessor name in "responseTooSlow" log messages |  Major | . |
| [HBASE-17037](https://issues.apache.org/jira/browse/HBASE-17037) | Enhance LoadIncrementalHFiles API to convey loaded files |  Major | . |
| [HBASE-17077](https://issues.apache.org/jira/browse/HBASE-17077) | Don't copy the replication queue belonging to the peer which has been deleted |  Minor | . |
| [HBASE-17047](https://issues.apache.org/jira/browse/HBASE-17047) | Add an API to get HBase connection cache statistics |  Minor | spark |
| [HBASE-17063](https://issues.apache.org/jira/browse/HBASE-17063) | Cleanup TestHRegion : remove duplicate variables for method name and two unused params in initRegion |  Minor | . |
| [HBASE-17026](https://issues.apache.org/jira/browse/HBASE-17026) | VerifyReplication log should distinguish whether good row key is result of revalidation |  Minor | . |
| [HBASE-16840](https://issues.apache.org/jira/browse/HBASE-16840) | Reuse cell's timestamp and type in ScanQueryMatcher |  Minor | . |
| [HBASE-17005](https://issues.apache.org/jira/browse/HBASE-17005) | Improve log message in MobFileCache |  Trivial | mob |
| [HBASE-17004](https://issues.apache.org/jira/browse/HBASE-17004) | Refactor IntegrationTestManyRegions to use @ClassRule for timing out |  Minor | integration tests |
| [HBASE-16946](https://issues.apache.org/jira/browse/HBASE-16946) | Provide Raw scan as an option in VerifyReplication |  Minor | hbase |
| [HBASE-17013](https://issues.apache.org/jira/browse/HBASE-17013) | Add constructor to RowMutations for initializing the capacity of internal list |  Minor | . |
| [HBASE-17006](https://issues.apache.org/jira/browse/HBASE-17006) | Add names to threads for better debugability of thread dumps |  Minor | Operability |
| [HBASE-17014](https://issues.apache.org/jira/browse/HBASE-17014) | Add clearly marked starting and shutdown log messages for all services. |  Minor | Operability |
| [HBASE-16950](https://issues.apache.org/jira/browse/HBASE-16950) | Print raw stats in the end of procedure performance tools for parsing results from scripts |  Trivial | . |
| [HBASE-16783](https://issues.apache.org/jira/browse/HBASE-16783) | Use ByteBufferPool for the header and message during Rpc response |  Minor | . |
| [HBASE-16562](https://issues.apache.org/jira/browse/HBASE-16562) | ITBLL should fail to start if misconfigured |  Major | integration tests |
| [HBASE-16414](https://issues.apache.org/jira/browse/HBASE-16414) | Improve performance for RPC encryption with Apache Common Crypto |  Major | IPC/RPC |
| [HBASE-16854](https://issues.apache.org/jira/browse/HBASE-16854) | Refactor the org.apache.hadoop.hbase.client.Action |  Minor | . |
| [HBASE-16774](https://issues.apache.org/jira/browse/HBASE-16774) | [shell] Add coverage to TestShell when ZooKeeper is not reachable |  Major | shell, test |
| [HBASE-16844](https://issues.apache.org/jira/browse/HBASE-16844) |  Procedure V2: DispatchMergingRegionsProcedure to use base class StateMachineProcedure for abort and rollback |  Trivial | master, proc-v2 |
| [HBASE-16818](https://issues.apache.org/jira/browse/HBASE-16818) | Avoid multiple copies of binary data during the conversion from Result to Row |  Major | spark |
| [HBASE-16821](https://issues.apache.org/jira/browse/HBASE-16821) | Enhance LoadIncrementalHFiles API to convey missing hfiles if any |  Major | . |
| [HBASE-15921](https://issues.apache.org/jira/browse/HBASE-15921) | Add first AsyncTable impl and create TableImpl based on it |  Major | Client |
| [HBASE-16792](https://issues.apache.org/jira/browse/HBASE-16792) | Reuse KeyValue.KeyOnlyKeyValue in BufferedDataBlockEncoder.SeekerState |  Minor | . |
| [HBASE-16784](https://issues.apache.org/jira/browse/HBASE-16784) | Make use of ExtendedCell#write(OutputStream os) for the default HFileWriter#append() |  Major | . |
| [HBASE-16809](https://issues.apache.org/jira/browse/HBASE-16809) | Save one cell length calculation in HeapMemStoreLAB#copyCellInto |  Minor | . |
| [HBASE-16661](https://issues.apache.org/jira/browse/HBASE-16661) | Add last major compaction age to per-region metrics |  Minor | . |
| [HBASE-16773](https://issues.apache.org/jira/browse/HBASE-16773) | AccessController should access local region if possible |  Major | . |
| [HBASE-16657](https://issues.apache.org/jira/browse/HBASE-16657) | Expose per-region last major compaction timestamp in RegionServer UI |  Minor | regionserver, UI |
| [HBASE-16772](https://issues.apache.org/jira/browse/HBASE-16772) | Add verbose option to VerifyReplication for logging good rows |  Minor | Replication, Usability |
| [HBASE-16690](https://issues.apache.org/jira/browse/HBASE-16690) | Move znode path configs to a separated class |  Major | Zookeeper |
| [HBASE-16672](https://issues.apache.org/jira/browse/HBASE-16672) | Add option for bulk load to always copy hfile(s) instead of renaming |  Major | . |
| [HBASE-16720](https://issues.apache.org/jira/browse/HBASE-16720) | Sort build ids in flaky dashboard |  Minor | . |
| [HBASE-16691](https://issues.apache.org/jira/browse/HBASE-16691) | Optimize KeyOnlyFilter by utilizing KeyOnlyCell |  Major | . |
| [HBASE-16714](https://issues.apache.org/jira/browse/HBASE-16714) | Procedure V2 - use base class to remove duplicate set up test code in table DDL procedures |  Major | proc-v2, test |
| [HBASE-16694](https://issues.apache.org/jira/browse/HBASE-16694) | Reduce garbage for onDiskChecksum in HFileBlock |  Minor | . |
| [HBASE-16705](https://issues.apache.org/jira/browse/HBASE-16705) | Eliminate long to Long auto boxing in LongComparator |  Minor | Filters |
| [HBASE-16692](https://issues.apache.org/jira/browse/HBASE-16692) | Make ByteBufferUtils#equals safer and correct |  Major | . |
| [HBASE-16667](https://issues.apache.org/jira/browse/HBASE-16667) | Building with JDK 8: ignoring option MaxPermSize=256m |  Minor | build |
| [HBASE-16423](https://issues.apache.org/jira/browse/HBASE-16423) | Add re-compare option to VerifyReplication to avoid occasional inconsistent rows |  Minor | Replication |
| [HBASE-16680](https://issues.apache.org/jira/browse/HBASE-16680) | Reduce garbage in BufferChain |  Minor | . |
| [HBASE-16659](https://issues.apache.org/jira/browse/HBASE-16659) | Use CellUtil.createFirstOnRow instead of KeyValueUtil.createFirstOnRow in some places. |  Minor | . |
| [HBASE-16646](https://issues.apache.org/jira/browse/HBASE-16646) | Enhance LoadIncrementalHFiles API to accept store file paths as input |  Major | . |
| [HBASE-16658](https://issues.apache.org/jira/browse/HBASE-16658) | Optimize UTF8 string/byte conversions |  Minor | . |
| [HBASE-16640](https://issues.apache.org/jira/browse/HBASE-16640) | TimeoutBlockingQueue#remove() should return whether the entry is removed |  Minor | . |
| [HBASE-15949](https://issues.apache.org/jira/browse/HBASE-15949) | Cleanup TestRegionServerMetrics |  Minor | . |
| [HBASE-16381](https://issues.apache.org/jira/browse/HBASE-16381) | Shell deleteall command should support row key prefixes |  Minor | shell |
| [HBASE-16616](https://issues.apache.org/jira/browse/HBASE-16616) | Rpc handlers stuck on ThreadLocalMap.expungeStaleEntry |  Major | Performance |
| [HBASE-16086](https://issues.apache.org/jira/browse/HBASE-16086) | TableCfWALEntryFilter and ScopeWALEntryFilter should not redundantly iterate over cells. |  Major | . |
| [HBASE-16541](https://issues.apache.org/jira/browse/HBASE-16541) | Avoid unnecessary cell copy in Result#compareResults |  Major | . |
| [HBASE-16399](https://issues.apache.org/jira/browse/HBASE-16399) | Provide an API to get list of failed regions and servername in Canary |  Major | canary |
| [HBASE-16502](https://issues.apache.org/jira/browse/HBASE-16502) | Reduce garbage in BufferedDataBlockEncoder |  Major | . |
| [HBASE-16509](https://issues.apache.org/jira/browse/HBASE-16509) | Add option to LoadIncrementalHFiles which allows skipping unmatched column families |  Major | . |
| [HBASE-16224](https://issues.apache.org/jira/browse/HBASE-16224) | Reduce the number of RPCs for the large PUTs |  Minor | Client |
| [HBASE-16508](https://issues.apache.org/jira/browse/HBASE-16508) | Move UnexpectedStateException to common |  Trivial | . |
| [HBASE-16486](https://issues.apache.org/jira/browse/HBASE-16486) | Unify system table creation using the same createSystemTable API. |  Minor | proc-v2 |
| [HBASE-16448](https://issues.apache.org/jira/browse/HBASE-16448) | Custom metrics for custom replication endpoints |  Major | Replication |
| [HBASE-16450](https://issues.apache.org/jira/browse/HBASE-16450) | Shell tool to dump replication queues |  Major | Operability, Replication |
| [HBASE-16455](https://issues.apache.org/jira/browse/HBASE-16455) | Provide API for obtaining all the WAL files |  Major | . |
| [HBASE-16422](https://issues.apache.org/jira/browse/HBASE-16422) | Tighten our guarantees on compatibility across patch versions |  Major | documentation |
| [HBASE-16434](https://issues.apache.org/jira/browse/HBASE-16434) | Improve flaky dashboard |  Minor | . |
| [HBASE-16419](https://issues.apache.org/jira/browse/HBASE-16419) | check REPLICATION\_SCOPE's value more stringently |  Major | . |
| [HBASE-16385](https://issues.apache.org/jira/browse/HBASE-16385) | Have hbase-rest pull hbase.rest.port from Constants.java |  Minor | REST |
| [HBASE-16379](https://issues.apache.org/jira/browse/HBASE-16379) | [replication] Minor improvement to replication/copy\_tables\_desc.rb |  Trivial | Replication, shell |
| [HBASE-14345](https://issues.apache.org/jira/browse/HBASE-14345) | Consolidate printUsage in IntegrationTestLoadAndVerify |  Trivial | integration tests |
| [HBASE-16299](https://issues.apache.org/jira/browse/HBASE-16299) | Update REST API scanner with ability to do reverse scan |  Minor | REST |
| [HBASE-12770](https://issues.apache.org/jira/browse/HBASE-12770) | Don't transfer all the queued hlogs of a dead server to the same alive server |  Minor | Replication |
| [HBASE-15882](https://issues.apache.org/jira/browse/HBASE-15882) | Upgrade to yetus precommit 0.3.0 |  Critical | build |
| [HBASE-8386](https://issues.apache.org/jira/browse/HBASE-8386) | deprecate TableMapReduce.addDependencyJars(Configuration, class\<?\> ...) |  Major | mapreduce |
| [HBASE-16287](https://issues.apache.org/jira/browse/HBASE-16287) | LruBlockCache size should not exceed acceptableSize too many |  Major | BlockCache |
| [HBASE-16225](https://issues.apache.org/jira/browse/HBASE-16225) | Refactor ScanQueryMatcher |  Major | regionserver, Scanners |
| [HBASE-14881](https://issues.apache.org/jira/browse/HBASE-14881) | Provide a Put API that uses the provided row without coping |  Major | . |
| [HBASE-16256](https://issues.apache.org/jira/browse/HBASE-16256) | Purpose of EnvironmentEdge, EnvironmentEdgeManager |  Trivial | documentation, regionserver |
| [HBASE-16275](https://issues.apache.org/jira/browse/HBASE-16275) | Change ServerManager#onlineServers from ConcurrentHashMap to ConcurrentSkipListMap |  Minor | . |
| [HBASE-14743](https://issues.apache.org/jira/browse/HBASE-14743) | Add metrics around HeapMemoryManager |  Minor | . |
| [HBASE-16266](https://issues.apache.org/jira/browse/HBASE-16266) | Do not throw ScannerTimeoutException when catch UnknownScannerException |  Major | Client, Scanners |
| [HBASE-16008](https://issues.apache.org/jira/browse/HBASE-16008) | A robust way deal with early termination of HBCK |  Major | hbck |
| [HBASE-13701](https://issues.apache.org/jira/browse/HBASE-13701) | Consolidate SecureBulkLoadEndpoint into HBase core as default for bulk load |  Major | . |
| [HBASE-16052](https://issues.apache.org/jira/browse/HBASE-16052) | Improve HBaseFsck Scalability |  Major | hbck |
| [HBASE-16241](https://issues.apache.org/jira/browse/HBASE-16241) | Allow specification of annotations to use when running check\_compatibility.sh |  Major | scripts |
| [HBASE-16231](https://issues.apache.org/jira/browse/HBASE-16231) | Integration tests should support client keytab login for secure clusters |  Major | integration tests |
| [HBASE-16220](https://issues.apache.org/jira/browse/HBASE-16220) | Demote log level for "HRegionFileSystem - No StoreFiles for" messages to TRACE |  Minor | . |
| [HBASE-16087](https://issues.apache.org/jira/browse/HBASE-16087) | Replication shouldn't start on a master if if only hosts system tables |  Major | . |
| [HBASE-14548](https://issues.apache.org/jira/browse/HBASE-14548) | Expand how table coprocessor jar and dependency path can be specified |  Major | Coprocessors |
| [HBASE-16108](https://issues.apache.org/jira/browse/HBASE-16108) | RowCounter should support multiple key ranges |  Major | . |
| [HBASE-16140](https://issues.apache.org/jira/browse/HBASE-16140) | bump owasp.esapi from 2.1.0 to 2.1.0.1 |  Major | dependencies |
| [HBASE-16114](https://issues.apache.org/jira/browse/HBASE-16114) | Get regionLocation of required regions only for MR jobs |  Minor | mapreduce |
| [HBASE-16147](https://issues.apache.org/jira/browse/HBASE-16147) | Shell command for getting compaction state |  Major | shell |
| [HBASE-16124](https://issues.apache.org/jira/browse/HBASE-16124) | Make check\_compatibility.sh less verbose when building HBase |  Minor | build, test |
| [HBASE-16149](https://issues.apache.org/jira/browse/HBASE-16149) | Log the underlying RPC exception in RpcRetryingCallerImpl |  Minor | . |
| [HBASE-16130](https://issues.apache.org/jira/browse/HBASE-16130) | Add comments to ProcedureStoreTracker |  Minor | . |
| [HBASE-16139](https://issues.apache.org/jira/browse/HBASE-16139) | Use CellUtil instead of KeyValueUtil in Import |  Minor | . |
| [HBASE-14007](https://issues.apache.org/jira/browse/HBASE-14007) | Writing to table through MR should fail upfront if table does not exist/is disabled |  Minor | mapreduce |
| [HBASE-16089](https://issues.apache.org/jira/browse/HBASE-16089) | Add on FastPath for CoDel |  Major | . |
| [HBASE-15353](https://issues.apache.org/jira/browse/HBASE-15353) | Add metric for number of CallQueueTooBigExceptions |  Minor | IPC/RPC, metrics |
| [HBASE-16085](https://issues.apache.org/jira/browse/HBASE-16085) | Add on metric for failed compactions |  Major | . |
| [HBASE-15870](https://issues.apache.org/jira/browse/HBASE-15870) | Specify columns in REST multi gets |  Minor | REST |
| [HBASE-14397](https://issues.apache.org/jira/browse/HBASE-14397) | PrefixFilter doesn't filter all remaining rows if the prefix is longer than rowkey being compared |  Minor | Filters |
| [HBASE-16042](https://issues.apache.org/jira/browse/HBASE-16042) | Add support in PE tool for InMemory Compaction |  Major | . |
| [HBASE-15600](https://issues.apache.org/jira/browse/HBASE-15600) | Add provision for adding mutations to memstore or able to write to same region in batchMutate coprocessor hooks |  Major | . |
| [HBASE-16018](https://issues.apache.org/jira/browse/HBASE-16018) | Better documentation of ReplicationPeers |  Minor | . |
| [HBASE-16048](https://issues.apache.org/jira/browse/HBASE-16048) | Tag InternalScanner with LimitedPrivate(HBaseInterfaceAudience.COPROC) |  Major | . |
| [HBASE-5291](https://issues.apache.org/jira/browse/HBASE-5291) | Add Kerberos HTTP SPNEGO authentication support to HBase web consoles |  Major | master, regionserver, security |
| [HBASE-16033](https://issues.apache.org/jira/browse/HBASE-16033) | Add more details in logging of responseTooSlow/TooLarge |  Major | Operability |
| [HBASE-16026](https://issues.apache.org/jira/browse/HBASE-16026) | Master UI should display status of additional ZK switches |  Major | . |
| [HBASE-16004](https://issues.apache.org/jira/browse/HBASE-16004) | Update to Netty 4.1.1 |  Major | . |
| [HBASE-15981](https://issues.apache.org/jira/browse/HBASE-15981) | Stripe and Date-tiered compactions inaccurately suggest disabling table in docs |  Minor | documentation |
| [HBASE-15849](https://issues.apache.org/jira/browse/HBASE-15849) | Shell Cleanup: Simplify handling of commands' runtime |  Minor | . |
| [HBASE-15931](https://issues.apache.org/jira/browse/HBASE-15931) | Add log for long-running tasks in AsyncProcess |  Critical | Operability |
| [HBASE-15727](https://issues.apache.org/jira/browse/HBASE-15727) | Canary Tool for Zookeeper |  Major | . |
| [HBASE-15910](https://issues.apache.org/jira/browse/HBASE-15910) | Update hbase ref guide to explain submit-patch.py |  Major | documentation |
| [HBASE-15890](https://issues.apache.org/jira/browse/HBASE-15890) | Allow thrift to set/unset "cacheBlocks" for Scans |  Major | Thrift |
| [HBASE-15854](https://issues.apache.org/jira/browse/HBASE-15854) | Log the cause of SASL connection failures |  Minor | security |
| [HBASE-15837](https://issues.apache.org/jira/browse/HBASE-15837) | Memstore size accounting is wrong if postBatchMutate() throws exception |  Major | regionserver |
| [HBASE-15471](https://issues.apache.org/jira/browse/HBASE-15471) | Add num calls in priority and general queue to RS UI |  Major | UI |
| [HBASE-15802](https://issues.apache.org/jira/browse/HBASE-15802) |  ConnectionUtils should use ThreadLocalRandom instead of Random |  Minor | . |
| [HBASE-15529](https://issues.apache.org/jira/browse/HBASE-15529) | Override needBalance in StochasticLoadBalancer |  Minor | . |
| [HBASE-15864](https://issues.apache.org/jira/browse/HBASE-15864) | Reuse the testing helper to wait regions in transition |  Trivial | test |
| [HBASE-15843](https://issues.apache.org/jira/browse/HBASE-15843) | Replace RegionState.getRegionInTransition() Map with a Set |  Trivial | master, Region Assignment |
| [HBASE-15593](https://issues.apache.org/jira/browse/HBASE-15593) | Time limit of scanning should be offered by client |  Major | . |
| [HBASE-15667](https://issues.apache.org/jira/browse/HBASE-15667) | Add more clarity to Reference Guide related to importing Eclipse Formatter |  Trivial | documentation |
| [HBASE-15842](https://issues.apache.org/jira/browse/HBASE-15842) | SnapshotInfo should display ownership information |  Major | . |
| [HBASE-13532](https://issues.apache.org/jira/browse/HBASE-13532) | Make UnknownScannerException logging less scary |  Trivial | . |
| [HBASE-15808](https://issues.apache.org/jira/browse/HBASE-15808) | Reduce potential bulk load intermediate space usage and waste |  Minor | . |
| [HBASE-15415](https://issues.apache.org/jira/browse/HBASE-15415) | Improve Master WebUI snapshot information |  Minor | master, snapshots |
| [HBASE-15609](https://issues.apache.org/jira/browse/HBASE-15609) | Remove PB references from Result, DoubleColumnInterpreter and any such public facing class for 2.0 |  Blocker | . |
| [HBASE-15791](https://issues.apache.org/jira/browse/HBASE-15791) | Improve javadoc in ScheduledChore |  Minor | master |
| [HBASE-15793](https://issues.apache.org/jira/browse/HBASE-15793) | Port over AsyncCall improvements |  Major | rpc |
| [HBASE-15794](https://issues.apache.org/jira/browse/HBASE-15794) | Fix Findbugs instanceof always true issue in MultiServerCallable |  Minor | . |
| [HBASE-15795](https://issues.apache.org/jira/browse/HBASE-15795) | Cleanup all classes in package org.apache.hadoop.hbase.ipc for code style |  Minor | . |
| [HBASE-15745](https://issues.apache.org/jira/browse/HBASE-15745) | Refactor RPC classes to better accept async changes. |  Major | . |
| [HBASE-15773](https://issues.apache.org/jira/browse/HBASE-15773) | CellCounter improvements |  Major | mapreduce |
| [HBASE-15759](https://issues.apache.org/jira/browse/HBASE-15759) | RegionObserver.preStoreScannerOpen() doesn't have acces to current readpoint |  Minor | Coprocessors |
| [HBASE-15608](https://issues.apache.org/jira/browse/HBASE-15608) | Remove PB references from SnapShot related Exceptions |  Blocker | . |
| [HBASE-15767](https://issues.apache.org/jira/browse/HBASE-15767) | Upgrade httpclient dependency |  Major | build, dependencies |
| [HBASE-15768](https://issues.apache.org/jira/browse/HBASE-15768) | fix capitalization of ZooKeeper usage |  Trivial | documentation |
| [HBASE-15720](https://issues.apache.org/jira/browse/HBASE-15720) | Print row locks at the debug dump page |  Major | . |
| [HBASE-15744](https://issues.apache.org/jira/browse/HBASE-15744) | Port over small format/text improvements from HBASE-13784 |  Trivial | . |
| [HBASE-15551](https://issues.apache.org/jira/browse/HBASE-15551) | Make call queue too big exception use servername |  Minor | . |
| [HBASE-15706](https://issues.apache.org/jira/browse/HBASE-15706) | HFilePrettyPrinter should print out nicely formatted tags |  Minor | HFile |
| [HBASE-15686](https://issues.apache.org/jira/browse/HBASE-15686) | Add override mechanism for the exempt classes when dynamically loading table coprocessor |  Major | Coprocessors |
| [HBASE-15680](https://issues.apache.org/jira/browse/HBASE-15680) | Examples in shell help message for TIMERANGE scanner specifications should use milliseconds instead of seconds |  Trivial | shell |
| [HBASE-15688](https://issues.apache.org/jira/browse/HBASE-15688) | Use MasterServices directly instead of casting to HMaster when possible |  Trivial | master |
| [HBASE-15641](https://issues.apache.org/jira/browse/HBASE-15641) | Shell "alter" should do a single modifyTable operation |  Major | shell |
| [HBASE-15614](https://issues.apache.org/jira/browse/HBASE-15614) | Report metrics from JvmPauseMonitor |  Major | metrics, regionserver |
| [HBASE-13129](https://issues.apache.org/jira/browse/HBASE-13129) | Add troubleshooting hints around WAL retention from replication |  Major | documentation, Replication |
| [HBASE-15632](https://issues.apache.org/jira/browse/HBASE-15632) | Undo the checking of lastStoreFlushTimeMap.isEmpty() introduced in HBASE-13145 |  Minor | regionserver |
| [HBASE-14985](https://issues.apache.org/jira/browse/HBASE-14985) | TimeRange constructors should set allTime when appropriate |  Minor | . |
| [HBASE-15605](https://issues.apache.org/jira/browse/HBASE-15605) | Remove PB references from HCD and HTD for 2.0 |  Blocker | . |
| [HBASE-15507](https://issues.apache.org/jira/browse/HBASE-15507) | Online modification of enabled ReplicationPeerConfig |  Major | Replication |
| [HBASE-15612](https://issues.apache.org/jira/browse/HBASE-15612) | Minor improvements to CellCounter and RowCounter documentation |  Trivial | documentation, mapreduce |
| [HBASE-15586](https://issues.apache.org/jira/browse/HBASE-15586) | Unify human readable numbers in the web UI |  Major | . |
| [HBASE-15606](https://issues.apache.org/jira/browse/HBASE-15606) | Limit creating zk connection in HBaseAdmin#getCompactionState() only to case when 'hbase:meta' is checked. |  Minor | Admin |
| [HBASE-15396](https://issues.apache.org/jira/browse/HBASE-15396) | Enhance mapreduce.TableSplit to add encoded region name |  Minor | mapreduce |
| [HBASE-15571](https://issues.apache.org/jira/browse/HBASE-15571) | Make MasterProcedureManagerHost accessible through MasterServices |  Major | . |
| [HBASE-15569](https://issues.apache.org/jira/browse/HBASE-15569) | Make Bytes.toStringBinary faster |  Minor | Performance |
| [HBASE-14983](https://issues.apache.org/jira/browse/HBASE-14983) | Create metrics for per block type hit/miss ratios |  Major | metrics |
| [HBASE-15191](https://issues.apache.org/jira/browse/HBASE-15191) | CopyTable and VerifyReplication - Option to specify batch size, versions |  Minor | Replication |
| [HBASE-15508](https://issues.apache.org/jira/browse/HBASE-15508) | Add command for exporting snapshot in hbase command script |  Minor | hbase, scripts, snapshots |
| [HBASE-15496](https://issues.apache.org/jira/browse/HBASE-15496) | Throw RowTooBigException only for user scan/get |  Minor | Scanners |
| [HBASE-15526](https://issues.apache.org/jira/browse/HBASE-15526) | Make SnapshotManager accessible through MasterServices |  Major | . |
| [HBASE-15486](https://issues.apache.org/jira/browse/HBASE-15486) | Avoid multiple disable/enable balancer calls while running rolling-restart.sh --graceful |  Minor | scripts |
| [HBASE-15300](https://issues.apache.org/jira/browse/HBASE-15300) | Upgrade to zookeeper 3.4.8 |  Minor | . |
| [HBASE-14703](https://issues.apache.org/jira/browse/HBASE-14703) | HTable.mutateRow does not collect stats |  Major | Client |
| [HBASE-15475](https://issues.apache.org/jira/browse/HBASE-15475) | Allow TimestampsFilter to provide a seek hint |  Major | Client, Filters, regionserver |
| [HBASE-15212](https://issues.apache.org/jira/browse/HBASE-15212) | RPCServer should enforce max request size |  Major | . |
| [HBASE-15447](https://issues.apache.org/jira/browse/HBASE-15447) | Improve javadocs description for Delete methods |  Minor | API, documentation |
| [HBASE-15478](https://issues.apache.org/jira/browse/HBASE-15478) | add comments to FSHLog explaining why syncRunnerIndex won't overflow |  Minor | wal |
| [HBASE-14963](https://issues.apache.org/jira/browse/HBASE-14963) | Remove use of Guava Stopwatch from HBase client code |  Major | Client |
| [HBASE-15451](https://issues.apache.org/jira/browse/HBASE-15451) | Remove unnecessary wait in MVCC |  Major | . |
| [HBASE-15456](https://issues.apache.org/jira/browse/HBASE-15456) | CreateTableProcedure/ModifyTableProcedure needs to fail when there is no family in table descriptor |  Minor | master |
| [HBASE-12940](https://issues.apache.org/jira/browse/HBASE-12940) | Expose listPeerConfigs and getPeerConfig to the HBase shell |  Major | shell |
| [HBASE-15470](https://issues.apache.org/jira/browse/HBASE-15470) | Add a setting for Priority queue length |  Major | IPC/RPC, Region Assignment |
| [HBASE-15413](https://issues.apache.org/jira/browse/HBASE-15413) | Procedure-V2: print out ProcedureInfo during trace |  Trivial | proc-v2 |
| [HBASE-15243](https://issues.apache.org/jira/browse/HBASE-15243) | Utilize the lowest seek value when all Filters in MUST\_PASS\_ONE FilterList return SEEK\_NEXT\_USING\_HINT |  Major | . |
| [HBASE-15338](https://issues.apache.org/jira/browse/HBASE-15338) | Add a option to disable the data block cache for testing the performance of underlying file system |  Minor | integration tests |
| [HBASE-15356](https://issues.apache.org/jira/browse/HBASE-15356) | Remove unused Imports |  Trivial | . |
| [HBASE-14099](https://issues.apache.org/jira/browse/HBASE-14099) | StoreFile.passesKeyRangeFilter need not create Cells from the Scan's start and stop Row |  Major | Performance, Scanners |
| [HBASE-15315](https://issues.apache.org/jira/browse/HBASE-15315) | Remove always set super user call as high priority |  Major | . |
| [HBASE-15222](https://issues.apache.org/jira/browse/HBASE-15222) | Use less contended classes for metrics |  Critical | metrics |
| [HBASE-15312](https://issues.apache.org/jira/browse/HBASE-15312) | Update the dependences of pom for mini cluster in HBase Book |  Minor | documentation |
| [HBASE-15306](https://issues.apache.org/jira/browse/HBASE-15306) | Make RPC call queue length dynamically configurable |  Major | IPC/RPC |
| [HBASE-11927](https://issues.apache.org/jira/browse/HBASE-11927) | Use Native Hadoop Library for HFile checksum (And flip default from CRC32 to CRC32C) |  Major | Performance |
| [HBASE-15301](https://issues.apache.org/jira/browse/HBASE-15301) | Remove the never-thrown NamingException from TableInputFormatBase#reverseDNS method signature |  Minor | . |
| [HBASE-15219](https://issues.apache.org/jira/browse/HBASE-15219) | Canary tool does not return non-zero exit code when one of regions is in stuck state |  Critical | canary |
| [HBASE-15223](https://issues.apache.org/jira/browse/HBASE-15223) | Make convertScanToString public for Spark |  Major | . |
| [HBASE-15229](https://issues.apache.org/jira/browse/HBASE-15229) | Canary Tools should not call System.Exit on error |  Critical | canary |
| [HBASE-11792](https://issues.apache.org/jira/browse/HBASE-11792) | Organize PerformanceEvaluation usage output |  Minor | Performance, test |
| [HBASE-15201](https://issues.apache.org/jira/browse/HBASE-15201) | Add hbase-spark to hbase assembly |  Minor | . |
| [HBASE-15211](https://issues.apache.org/jira/browse/HBASE-15211) | Don't run the CatalogJanitor if there are regions in transition |  Major | master |
| [HBASE-15197](https://issues.apache.org/jira/browse/HBASE-15197) | Expose filtered read requests metric to metrics framework and Web UI |  Minor | . |
| [HBASE-15177](https://issues.apache.org/jira/browse/HBASE-15177) | Reduce garbage created under high load |  Major | . |
| [HBASE-13376](https://issues.apache.org/jira/browse/HBASE-13376) | Improvements to Stochastic load balancer |  Minor | Balancer |
| [HBASE-15129](https://issues.apache.org/jira/browse/HBASE-15129) | Set default value for hbase.fs.tmp.dir rather than fully depend on hbase-default.xml |  Major | mapreduce |
| [HBASE-14969](https://issues.apache.org/jira/browse/HBASE-14969) | Add throughput controller for flush |  Major | regionserver |
| [HBASE-15123](https://issues.apache.org/jira/browse/HBASE-15123) | Remove duplicate code in LocalHBaseCluster and minor formatting |  Minor | . |
| [HBASE-14865](https://issues.apache.org/jira/browse/HBASE-14865) | Support passing multiple QOPs to SaslClient/Server via hbase.rpc.protection |  Major | security |
| [HBASE-15119](https://issues.apache.org/jira/browse/HBASE-15119) | Include git SHA in check\_compatibility reports |  Minor | build |
| [HBASE-13525](https://issues.apache.org/jira/browse/HBASE-13525) | Update test-patch to leverage Apache Yetus |  Major | build |
| [HBASE-15076](https://issues.apache.org/jira/browse/HBASE-15076) | Add getScanner(Scan scan, List\<KeyValueScanner\> additionalScanners) API into Region interface |  Critical | regionserver |
| [HBASE-15068](https://issues.apache.org/jira/browse/HBASE-15068) | Add metrics for region normalization plans |  Major | . |
| [HBASE-14468](https://issues.apache.org/jira/browse/HBASE-14468) | Compaction improvements: FIFO compaction policy |  Major | Compaction, Performance |
| [HBASE-15066](https://issues.apache.org/jira/browse/HBASE-15066) | Small improvements to Canary tool |  Major | . |
| [HBASE-14524](https://issues.apache.org/jira/browse/HBASE-14524) | Short-circuit comparison of rows in CellComparator |  Major | Performance, Scanners |
| [HBASE-15038](https://issues.apache.org/jira/browse/HBASE-15038) | ExportSnapshot should support separate configurations for source and destination clusters |  Major | mapreduce, snapshots |
| [HBASE-15060](https://issues.apache.org/jira/browse/HBASE-15060) | Cull TestHFileWriterV2 and HFileWriterFactory |  Major | HFile |
| [HBASE-15044](https://issues.apache.org/jira/browse/HBASE-15044) | Region normalization should be allowed when underlying namespace has quota |  Major | Balancer |
| [HBASE-14796](https://issues.apache.org/jira/browse/HBASE-14796) | Enhance the Gets in the connector |  Minor | . |
| [HBASE-13322](https://issues.apache.org/jira/browse/HBASE-13322) | Replace explicit HBaseAdmin creation with connection#getAdmin() |  Minor | . |
| [HBASE-14800](https://issues.apache.org/jira/browse/HBASE-14800) | Expose checkAndMutate via Thrift2 |  Major | Thrift |
| [HBASE-14684](https://issues.apache.org/jira/browse/HBASE-14684) | Try to remove all MiniMapReduceCluster in unit tests |  Critical | test |
| [HBASE-13158](https://issues.apache.org/jira/browse/HBASE-13158) | When client supports CellBlock, return the result Cells as controller payload for get(Get) API also |  Major | . |
| [HBASE-14976](https://issues.apache.org/jira/browse/HBASE-14976) | Add RPC call queues to the web ui |  Minor | UI |
| [HBASE-15005](https://issues.apache.org/jira/browse/HBASE-15005) | Use value array in computing block length for 1.2 and 1.3 |  Major | regionserver |
| [HBASE-14978](https://issues.apache.org/jira/browse/HBASE-14978) | Don't allow Multi to retain too many blocks |  Blocker | io, IPC/RPC, regionserver |
| [HBASE-14780](https://issues.apache.org/jira/browse/HBASE-14780) | Integration Tests that run with ChaosMonkey need to specify CFs |  Major | integration tests |
| [HBASE-14951](https://issues.apache.org/jira/browse/HBASE-14951) | Make hbase.regionserver.maxlogs obsolete |  Minor | Performance, wal |
| [HBASE-14984](https://issues.apache.org/jira/browse/HBASE-14984) | Allow memcached block cache to set optimze to false |  Major | BlockCache |
| [HBASE-13425](https://issues.apache.org/jira/browse/HBASE-13425) | Documentation nit in REST Gateway impersonation section |  Minor | documentation |
| [HBASE-14795](https://issues.apache.org/jira/browse/HBASE-14795) | Enhance the spark-hbase scan operations |  Minor | . |
| [HBASE-14946](https://issues.apache.org/jira/browse/HBASE-14946) | Don't allow multi's to over run the max result size. |  Critical | Client, IPC/RPC |
| [HBASE-14906](https://issues.apache.org/jira/browse/HBASE-14906) | Improvements on FlushLargeStoresPolicy |  Major | . |
| [HBASE-14866](https://issues.apache.org/jira/browse/HBASE-14866) | VerifyReplication should use peer configuration in peer connection |  Major | Replication |
| [HBASE-7171](https://issues.apache.org/jira/browse/HBASE-7171) | Initial web UI for region/memstore/storefiles details |  Major | UI |
| [HBASE-14719](https://issues.apache.org/jira/browse/HBASE-14719) | Add metric for number of MasterProcWALs |  Major | . |
| [HBASE-14580](https://issues.apache.org/jira/browse/HBASE-14580) | Make the HBaseMiniCluster compliant with Kerberos |  Major | security, test |
| [HBASE-14749](https://issues.apache.org/jira/browse/HBASE-14749) | Make changes to region\_mover.rb to use RegionMover Java tool |  Major | . |
| [HBASE-14891](https://issues.apache.org/jira/browse/HBASE-14891) | Add log for uncaught exception in RegionServerMetricsWrapperRunnable |  Minor | metrics |
| [HBASE-14859](https://issues.apache.org/jira/browse/HBASE-14859) | Better checkstyle reporting |  Minor | . |
| [HBASE-14826](https://issues.apache.org/jira/browse/HBASE-14826) | Small improvement in KVHeap seek() API |  Minor | . |
| [HBASE-14860](https://issues.apache.org/jira/browse/HBASE-14860) | Improve BoundedByteBufferPool; make lockless |  Minor | . |
| [HBASE-14871](https://issues.apache.org/jira/browse/HBASE-14871) | Allow specifying the base branch for make\_patch |  Major | . |
| [HBASE-14821](https://issues.apache.org/jira/browse/HBASE-14821) | CopyTable should allow overriding more config properties for peer cluster |  Major | mapreduce |
| [HBASE-13347](https://issues.apache.org/jira/browse/HBASE-13347) | Deprecate FirstKeyValueMatchingQualifiersFilter |  Minor | . |
| [HBASE-14862](https://issues.apache.org/jira/browse/HBASE-14862) | Add support for reporting p90 for histogram metrics |  Minor | metrics |
| [HBASE-14172](https://issues.apache.org/jira/browse/HBASE-14172) | Upgrade existing thrift binding using thrift 0.9.3 compiler. |  Minor | . |
| [HBASE-14829](https://issues.apache.org/jira/browse/HBASE-14829) | Add more checkstyles |  Major | . |
| [HBASE-14805](https://issues.apache.org/jira/browse/HBASE-14805) | status should show the master in shell |  Major | shell |
| [HBASE-14708](https://issues.apache.org/jira/browse/HBASE-14708) | Use copy on write Map for region location cache |  Critical | Client |
| [HBASE-14766](https://issues.apache.org/jira/browse/HBASE-14766) | In WALEntryFilter, cell.getFamily() needs to be replaced with the new low-cost implementation |  Major | . |
| [HBASE-14387](https://issues.apache.org/jira/browse/HBASE-14387) | Compaction improvements: Maximum off-peak compaction size |  Major | Compaction, Performance |
| [HBASE-14693](https://issues.apache.org/jira/browse/HBASE-14693) | Add client-side metrics for received pushback signals |  Major | Client, metrics |
| [HBASE-14765](https://issues.apache.org/jira/browse/HBASE-14765) | Remove snappy profile |  Major | build |
| [HBASE-14752](https://issues.apache.org/jira/browse/HBASE-14752) | Add example of using the HBase client in a multi-threaded environment |  Minor | Client |
| [HBASE-14666](https://issues.apache.org/jira/browse/HBASE-14666) | Remove deprecated HBaseTestingUtility#deleteTable methods |  Major | test |
| [HBASE-12986](https://issues.apache.org/jira/browse/HBASE-12986) | Compaction pressure based client pushback |  Major | . |
| [HBASE-12822](https://issues.apache.org/jira/browse/HBASE-12822) | Option for Unloading regions through region\_mover.rb without Acknowledging |  Minor | . |
| [HBASE-13742](https://issues.apache.org/jira/browse/HBASE-13742) | buildbot should run link checker over book |  Major | documentation |
| [HBASE-14700](https://issues.apache.org/jira/browse/HBASE-14700) | Support a "permissive" mode for secure clusters to allow "simple" auth clients |  Major | security |
| [HBASE-14715](https://issues.apache.org/jira/browse/HBASE-14715) | Add javadocs to DelegatingRetryingCallable |  Trivial | Client |
| [HBASE-13014](https://issues.apache.org/jira/browse/HBASE-13014) | Java Tool For Region Moving |  Major | . |
| [HBASE-14731](https://issues.apache.org/jira/browse/HBASE-14731) | Add -DuseMob option to ITBLL |  Major | . |
| [HBASE-14687](https://issues.apache.org/jira/browse/HBASE-14687) | Un-synchronize BufferedMutator |  Critical | Client, Performance |
| [HBASE-14721](https://issues.apache.org/jira/browse/HBASE-14721) | Memstore add cells - Avoid many garbage |  Major | regionserver |
| [HBASE-14714](https://issues.apache.org/jira/browse/HBASE-14714) | some cleanup to snapshot code |  Trivial | snapshots |
| [HBASE-14675](https://issues.apache.org/jira/browse/HBASE-14675) | Exorcise deprecated Put#add(...) and replace with Put#addColumn(...) |  Major | . |
| [HBASE-14672](https://issues.apache.org/jira/browse/HBASE-14672) | Exorcise deprecated Delete#delete\* apis |  Major | . |
| [HBASE-12769](https://issues.apache.org/jira/browse/HBASE-12769) | Replication fails to delete all corresponding zk nodes when peer is removed |  Minor | Replication |
| [HBASE-14696](https://issues.apache.org/jira/browse/HBASE-14696) | Support setting allowPartialResults in mapreduce Mappers |  Major | mapreduce |
| [HBASE-14266](https://issues.apache.org/jira/browse/HBASE-14266) | RegionServers have a lock contention of Configuration.getProps |  Critical | regionserver |
| [HBASE-14683](https://issues.apache.org/jira/browse/HBASE-14683) | Batching in buffered mutator is awful when adding lists of mutations. |  Major | Client |
| [HBASE-14670](https://issues.apache.org/jira/browse/HBASE-14670) | Remove deprecated HBaseTestCase from TestBlocksRead |  Major | test |
| [HBASE-14669](https://issues.apache.org/jira/browse/HBASE-14669) | remove unused import and fix javadoc |  Trivial | . |
| [HBASE-14671](https://issues.apache.org/jira/browse/HBASE-14671) | Remove deprecated HBaseTestCase/Put/Delete apis from TestGetClosestAtOrBefore |  Major | test |
| [HBASE-14665](https://issues.apache.org/jira/browse/HBASE-14665) | Remove deprecated HBaseTestingUtility#createTable methods |  Major | test |
| [HBASE-14668](https://issues.apache.org/jira/browse/HBASE-14668) | Remove deprecated HBaseTestCase dependency from TestHFile |  Major | test |
| [HBASE-14643](https://issues.apache.org/jira/browse/HBASE-14643) | Avoid Splits from once again opening a closed reader for fetching the first and last key |  Major | regionserver |
| [HBASE-14586](https://issues.apache.org/jira/browse/HBASE-14586) | Use a maven profile to run Jacoco analysis |  Minor | pom |
| [HBASE-14268](https://issues.apache.org/jira/browse/HBASE-14268) | Improve KeyLocker |  Minor | util |
| [HBASE-14588](https://issues.apache.org/jira/browse/HBASE-14588) | Stop accessing test resources from within src folder |  Major | build, test |
| [HBASE-14587](https://issues.apache.org/jira/browse/HBASE-14587) | Attach a test-sources.jar for hbase-server |  Major | build |
| [HBASE-14582](https://issues.apache.org/jira/browse/HBASE-14582) | Regionserver status webpage bucketcache list can become huge |  Major | regionserver |
| [HBASE-14517](https://issues.apache.org/jira/browse/HBASE-14517) | Show regionserver's version in master status page |  Minor | monitoring |
| [HBASE-14565](https://issues.apache.org/jira/browse/HBASE-14565) | Make ZK connection timeout configurable in MiniZooKeeperCluster |  Major | . |
| [HBASE-14574](https://issues.apache.org/jira/browse/HBASE-14574) | TableOutputFormat#getRecordWriter javadoc misleads |  Major | . |
| [HBASE-14573](https://issues.apache.org/jira/browse/HBASE-14573) | Edit on the ByteBufferedCell javadoc |  Major | documentation |
| [HBASE-14436](https://issues.apache.org/jira/browse/HBASE-14436) | HTableDescriptor#addCoprocessor will always make RegionCoprocessorHost create new Configuration |  Minor | Coprocessors |
| [HBASE-14520](https://issues.apache.org/jira/browse/HBASE-14520) | Optimize the number of calls for tags creation in bulk load |  Major | . |
| [HBASE-14547](https://issues.apache.org/jira/browse/HBASE-14547) | Add more debug/trace to zk-procedure |  Trivial | snapshots |
| [HBASE-14230](https://issues.apache.org/jira/browse/HBASE-14230) | replace reflection in FSHlog with HdfsDataOutputStream#getCurrentBlockReplication() |  Minor | wal |
| [HBASE-14478](https://issues.apache.org/jira/browse/HBASE-14478) | A ThreadPoolExecutor with a LinkedBlockingQueue cannot execute tasks concurrently |  Major | regionserver |
| [HBASE-14455](https://issues.apache.org/jira/browse/HBASE-14455) | Try to get rid of unused HConnection instance |  Minor | . |
| [HBASE-14193](https://issues.apache.org/jira/browse/HBASE-14193) | Remove support for direct upgrade from pre-0.96 versions |  Minor | . |
| [HBASE-14461](https://issues.apache.org/jira/browse/HBASE-14461) | Cleanup IncreasingToUpperBoundRegionSplitPolicy |  Minor | . |
| [HBASE-14448](https://issues.apache.org/jira/browse/HBASE-14448) | Refine RegionGroupingProvider Phase-2: remove provider nesting and formalize wal group name |  Major | . |
| [HBASE-14082](https://issues.apache.org/jira/browse/HBASE-14082) | Add replica id to JMX metrics names |  Major | metrics |
| [HBASE-14334](https://issues.apache.org/jira/browse/HBASE-14334) | Move Memcached block cache in to it's own optional module. |  Major | BlockCache, build, Operability |
| [HBASE-14306](https://issues.apache.org/jira/browse/HBASE-14306) | Refine RegionGroupingProvider: fix issues and make it more scalable |  Major | wal |
| [HBASE-6617](https://issues.apache.org/jira/browse/HBASE-6617) | ReplicationSourceManager should be able to track multiple WAL paths |  Major | Replication |
| [HBASE-14314](https://issues.apache.org/jira/browse/HBASE-14314) | Metrics for block cache should take region replicas into account |  Major | metrics, regionserver |
| [HBASE-12988](https://issues.apache.org/jira/browse/HBASE-12988) | [Replication]Parallel apply edits across regions |  Major | Replication |
| [HBASE-14261](https://issues.apache.org/jira/browse/HBASE-14261) | Enhance Chaos Monkey framework by adding zookeeper and datanode fault injections. |  Major | integration tests |
| [HBASE-7972](https://issues.apache.org/jira/browse/HBASE-7972) | Add a configuration for the TCP backlog in the Thrift server |  Major | Thrift |
| [HBASE-14332](https://issues.apache.org/jira/browse/HBASE-14332) | Show the table state when we encounter exception while disabling / enabling table |  Minor | . |
| [HBASE-14325](https://issues.apache.org/jira/browse/HBASE-14325) | Add snapshotinfo command to hbase script |  Minor | scripts |
| [HBASE-14309](https://issues.apache.org/jira/browse/HBASE-14309) | Allow load balancer to operate when there is region in transition by adding force flag |  Major | . |
| [HBASE-14078](https://issues.apache.org/jira/browse/HBASE-14078) | improve error message when HMaster can't bind to port |  Major | master |
| [HBASE-13127](https://issues.apache.org/jira/browse/HBASE-13127) | Add timeouts on all tests so less zombie sightings |  Major | test |
| [HBASE-13996](https://issues.apache.org/jira/browse/HBASE-13996) | Add write sniffing in canary |  Major | canary |
| [HBASE-14260](https://issues.apache.org/jira/browse/HBASE-14260) | don't build javadocs for hbase-protocol module |  Major | build, documentation |
| [HBASE-14148](https://issues.apache.org/jira/browse/HBASE-14148) | Web UI Framable Page |  Major | security, UI |
| [HBASE-14165](https://issues.apache.org/jira/browse/HBASE-14165) | The initial size of RWQueueRpcExecutor.queues should be (numWriteQueues + numReadQueues + numScanQueues) |  Minor | IPC/RPC |
| [HBASE-14203](https://issues.apache.org/jira/browse/HBASE-14203) | remove duplicate code getTableDescriptor in HTable |  Trivial | . |
| [HBASE-13914](https://issues.apache.org/jira/browse/HBASE-13914) | Minor improvements to dev-support/publish\_hbase\_website.sh |  Minor | website |
| [HBASE-12812](https://issues.apache.org/jira/browse/HBASE-12812) | Update Netty dependency to latest release |  Major | . |
| [HBASE-13985](https://issues.apache.org/jira/browse/HBASE-13985) | Add configuration to skip validating HFile format when bulk loading |  Minor | . |
| [HBASE-14122](https://issues.apache.org/jira/browse/HBASE-14122) | Client API for determining if server side supports cell level security |  Minor | . |
| [HBASE-14194](https://issues.apache.org/jira/browse/HBASE-14194) | Undeprecate methods in ThriftServerRunner.HBaseHandler |  Trivial | . |
| [HBASE-12256](https://issues.apache.org/jira/browse/HBASE-12256) | Update patch submission guidelines to call out binary file support |  Minor | documentation |
| [HBASE-13965](https://issues.apache.org/jira/browse/HBASE-13965) | Stochastic Load Balancer JMX Metrics |  Major | Balancer, metrics |
| [HBASE-14164](https://issues.apache.org/jira/browse/HBASE-14164) | Display primary region replicas distribution on table.jsp |  Minor | . |
| [HBASE-14097](https://issues.apache.org/jira/browse/HBASE-14097) | Log link to client scan troubleshooting section when scanner exceptions happen. |  Trivial | . |
| [HBASE-14152](https://issues.apache.org/jira/browse/HBASE-14152) | Fix the warnings in Checkstyle and FindBugs brought in by merging hbase-11339 |  Major | mob |
| [HBASE-14151](https://issues.apache.org/jira/browse/HBASE-14151) | Remove the unnecessary file ProtobufUtil.java.rej which is brought in by merging hbase-11339 |  Major | mob |
| [HBASE-14058](https://issues.apache.org/jira/browse/HBASE-14058) | Stabilizing default heap memory tuner |  Major | regionserver |
| [HBASE-14110](https://issues.apache.org/jira/browse/HBASE-14110) | Add CostFunction for balancing primary region replicas |  Major | Balancer |
| [HBASE-8642](https://issues.apache.org/jira/browse/HBASE-8642) | [Snapshot] List and delete snapshot by table |  Major | snapshots |
| [HBASE-14045](https://issues.apache.org/jira/browse/HBASE-14045) | Bumping thrift version to 0.9.2. |  Major | . |
| [HBASE-12596](https://issues.apache.org/jira/browse/HBASE-12596) | bulkload needs to follow locality |  Major | HFile, regionserver |
| [HBASE-13927](https://issues.apache.org/jira/browse/HBASE-13927) | Allow hbase-daemon.sh to conditionally redirect the log or not |  Major | shell |
| [HBASE-14002](https://issues.apache.org/jira/browse/HBASE-14002) | Add --noReplicationSetup option to IntegrationTestReplication |  Major | integration tests |
| [HBASE-13925](https://issues.apache.org/jira/browse/HBASE-13925) | Use zookeeper multi to clear znodes in ZKProcedureUtil |  Major | Zookeeper |
| [HBASE-13500](https://issues.apache.org/jira/browse/HBASE-13500) | Deprecate KVComparator and move to CellComparator |  Major | . |
| [HBASE-14015](https://issues.apache.org/jira/browse/HBASE-14015) | Allow setting a richer state value when toString a pv2 |  Minor | proc-v2 |
| [HBASE-13980](https://issues.apache.org/jira/browse/HBASE-13980) | Distinguish blockedFlushCount vs unblockedFlushCount when tuning heap memory |  Minor | . |
| [HBASE-13670](https://issues.apache.org/jira/browse/HBASE-13670) | [HBase MOB] ExpiredMobFileCleaner tool deletes mob files later for one more day after they are expired |  Major | documentation, mob |
| [HBASE-13943](https://issues.apache.org/jira/browse/HBASE-13943) | Get rid of KeyValue#heapSizeWithoutTags |  Major | . |
| [HBASE-14001](https://issues.apache.org/jira/browse/HBASE-14001) | Optimize write(OutputStream out, boolean withTags) for SizeCachedNoTagsKeyValue |  Minor | . |
| [HBASE-13103](https://issues.apache.org/jira/browse/HBASE-13103) | [ergonomics] add region size balancing as a feature of master |  Major | Balancer, Usability |
| [HBASE-13900](https://issues.apache.org/jira/browse/HBASE-13900) | duplicate methods between ProtobufMagic and ProtobufUtil |  Minor | . |
| [HBASE-13931](https://issues.apache.org/jira/browse/HBASE-13931) | Move Unsafe based operations to UnsafeAccess |  Major | . |
| [HBASE-13917](https://issues.apache.org/jira/browse/HBASE-13917) | Remove string comparison to identify request priority |  Minor | regionserver |
| [HBASE-13876](https://issues.apache.org/jira/browse/HBASE-13876) | Improving performance of HeapMemoryManager |  Minor | hbase, regionserver |
| [HBASE-13247](https://issues.apache.org/jira/browse/HBASE-13247) | Change BufferedMutatorExample to use addColumn() since add() is deprecated |  Trivial | Client |
| [HBASE-13894](https://issues.apache.org/jira/browse/HBASE-13894) | Avoid visitor alloc each call of ByteBufferArray get/putMultiple() |  Minor | regionserver |
| [HBASE-13829](https://issues.apache.org/jira/browse/HBASE-13829) | Add more ThrottleType |  Major | Client |
| [HBASE-13755](https://issues.apache.org/jira/browse/HBASE-13755) | Provide single super user check implementation |  Major | security |
| [HBASE-13848](https://issues.apache.org/jira/browse/HBASE-13848) | Access InfoServer SSL passwords through Credential Provder API |  Major | security |
| [HBASE-13828](https://issues.apache.org/jira/browse/HBASE-13828) | Add group permissions testing coverage to AC. |  Major | test |
| [HBASE-13846](https://issues.apache.org/jira/browse/HBASE-13846) | Run MiniCluster on top of other MiniDfsCluster |  Minor | test |
| [HBASE-13816](https://issues.apache.org/jira/browse/HBASE-13816) | Build shaded modules only in release profile |  Major | build |
| [HBASE-13344](https://issues.apache.org/jira/browse/HBASE-13344) | Add enforcer rule that matches our JDK support statement |  Minor | build |
| [HBASE-13761](https://issues.apache.org/jira/browse/HBASE-13761) | Optimize FuzzyRowFilter |  Minor | Filters |
| [HBASE-13710](https://issues.apache.org/jira/browse/HBASE-13710) | Remove use of Hadoop's ReflectionUtil in favor of our own. |  Minor | . |
| [HBASE-13780](https://issues.apache.org/jira/browse/HBASE-13780) | Default to 700 for HDFS root dir permissions for secure deployments |  Major | Operability, security |
| [HBASE-13671](https://issues.apache.org/jira/browse/HBASE-13671) | More classes to add to the invoking repository of org.apache.hadoop.hbase.mapreduce.driver |  Major | mapreduce |
| [HBASE-13745](https://issues.apache.org/jira/browse/HBASE-13745) | Say why a flush was requested in log message |  Minor | Operability |
| [HBASE-13645](https://issues.apache.org/jira/browse/HBASE-13645) | Rename \*column methods in MasterObserver to \*columnFamily |  Minor | . |
| [HBASE-13725](https://issues.apache.org/jira/browse/HBASE-13725) | [documentation] Pseudo-Distributed Local Install can link to hadoop instructions |  Minor | . |
| [HBASE-13656](https://issues.apache.org/jira/browse/HBASE-13656) | Rename getDeadServers to getDeadServersSize in Admin |  Minor | . |
| [HBASE-13675](https://issues.apache.org/jira/browse/HBASE-13675) | ProcedureExecutor completion report should be at DEBUG log level |  Minor | . |
| [HBASE-13673](https://issues.apache.org/jira/browse/HBASE-13673) | WALProcedureStore procedure is chatty |  Minor | . |
| [HBASE-13677](https://issues.apache.org/jira/browse/HBASE-13677) | RecoverableZookeeper WARNs on expected events |  Minor | . |
| [HBASE-13684](https://issues.apache.org/jira/browse/HBASE-13684) | Allow mlockagent to be used when not starting as root |  Minor | . |
| [HBASE-13655](https://issues.apache.org/jira/browse/HBASE-13655) | Deprecate duplicate getCompression methods in HColumnDescriptor |  Minor | . |
| [HBASE-13251](https://issues.apache.org/jira/browse/HBASE-13251) | Correct 'HBase, MapReduce, and the CLASSPATH' section in HBase Ref Guide |  Major | documentation |
| [HBASE-13598](https://issues.apache.org/jira/browse/HBASE-13598) | Make hbase assembly 'attach' to the project |  Minor | . |
| [HBASE-12415](https://issues.apache.org/jira/browse/HBASE-12415) | Add add(byte[][] arrays) to Bytes. |  Major | API, Client |
| [HBASE-13358](https://issues.apache.org/jira/browse/HBASE-13358) | Upgrade VisibilityClient API to accept Connection object. |  Minor | API, security |
| [HBASE-13420](https://issues.apache.org/jira/browse/HBASE-13420) | RegionEnvironment.offerExecutionLatency Blocks Threads under Heavy Load |  Major | Coprocessors, metrics, Performance |
| [HBASE-13351](https://issues.apache.org/jira/browse/HBASE-13351) | Annotate internal MasterRpcServices methods with admin priority |  Major | master |
| [HBASE-13431](https://issues.apache.org/jira/browse/HBASE-13431) | Allow to skip store file range check based on column family while creating reference files in HRegionFileSystem#splitStoreFile |  Major | . |
| [HBASE-13578](https://issues.apache.org/jira/browse/HBASE-13578) | Remove Arrays.asList().subList() from FSHLog.offer() |  Trivial | wal |
| [HBASE-13518](https://issues.apache.org/jira/browse/HBASE-13518) | Typo in hbase.hconnection.meta.lookup.threads.core parameter |  Major | . |
| [HBASE-13516](https://issues.apache.org/jira/browse/HBASE-13516) | Increase PermSize to 128MB |  Major | . |
| [HBASE-13255](https://issues.apache.org/jira/browse/HBASE-13255) | Bad grammar in RegionServer status page |  Trivial | monitoring |
| [HBASE-13334](https://issues.apache.org/jira/browse/HBASE-13334) | FindBugs should create precise report for new bugs introduced |  Minor | build |
| [HBASE-13550](https://issues.apache.org/jira/browse/HBASE-13550) | [Shell] Support unset of a list of table attributes |  Minor | . |
| [HBASE-13552](https://issues.apache.org/jira/browse/HBASE-13552) | ChoreService shutdown message could be more informative |  Trivial | . |
| [HBASE-13534](https://issues.apache.org/jira/browse/HBASE-13534) | Change HBase master WebUI to explicitly mention if it is a backup master |  Minor | master, UI |
| [HBASE-13456](https://issues.apache.org/jira/browse/HBASE-13456) | Improve HFilePrettyPrinter first hbase:meta region processing |  Minor | util |
| [HBASE-12987](https://issues.apache.org/jira/browse/HBASE-12987) | HBCK should print status while scanning over many regions |  Major | hbck, Usability |
| [HBASE-13350](https://issues.apache.org/jira/browse/HBASE-13350) | Add a debug-warn if we fail HTD checks even if table.sanity.checks is false |  Trivial | master, Operability |
| [HBASE-13453](https://issues.apache.org/jira/browse/HBASE-13453) | Master should not bind to region server ports |  Critical | . |
| [HBASE-13419](https://issues.apache.org/jira/browse/HBASE-13419) | Thrift gateway should propagate text from exception causes. |  Major | Thrift |
| [HBASE-13436](https://issues.apache.org/jira/browse/HBASE-13436) | Include user name in ADE for scans |  Minor | . |
| [HBASE-13381](https://issues.apache.org/jira/browse/HBASE-13381) | Expand TestSizeFailures to include small scans |  Minor | test |
| [HBASE-13270](https://issues.apache.org/jira/browse/HBASE-13270) | Setter for Result#getStats is #addResults; confusing! |  Major | Client |
| [HBASE-13362](https://issues.apache.org/jira/browse/HBASE-13362) | Set max result size from client only (like scanner caching). |  Major | . |
| [HBASE-11864](https://issues.apache.org/jira/browse/HBASE-11864) | Enhance HLogPrettyPrinter to print information from WAL Header |  Minor | . |
| [HBASE-13370](https://issues.apache.org/jira/browse/HBASE-13370) | PE tool could give option for using Explicit Column Tracker which leads to seeks |  Major | . |
| [HBASE-6919](https://issues.apache.org/jira/browse/HBASE-6919) | Remove unnecessary throws IOException from Bytes.readVLong |  Minor | . |
| [HBASE-12891](https://issues.apache.org/jira/browse/HBASE-12891) | Parallel execution for Hbck checkRegionConsistency |  Major | hbck |
| [HBASE-13341](https://issues.apache.org/jira/browse/HBASE-13341) | Add option to disable filtering on interface annotations for the API compatibility report |  Minor | . |
| [HBASE-13340](https://issues.apache.org/jira/browse/HBASE-13340) | Include LimitedPrivate interfaces in the API compatibility report |  Minor | API |
| [HBASE-13366](https://issues.apache.org/jira/browse/HBASE-13366) | Throw DoNotRetryIOException instead of read only IOException |  Minor | . |
| [HBASE-13345](https://issues.apache.org/jira/browse/HBASE-13345) | Fix LocalHBaseCluster so that different region server impl can be used for different slaves |  Minor | . |
| [HBASE-13348](https://issues.apache.org/jira/browse/HBASE-13348) | Separate the thread number configs for meta server and server operations |  Minor | master |
| [HBASE-12975](https://issues.apache.org/jira/browse/HBASE-12975) | Supportable SplitTransaction and RegionMergeTransaction interfaces |  Major | . |
| [HBASE-13369](https://issues.apache.org/jira/browse/HBASE-13369) | Expose scanNext stats to region server level |  Major | . |
| [HBASE-13222](https://issues.apache.org/jira/browse/HBASE-13222) | Provide means of non-destructive balancer inspection |  Minor | Balancer |
| [HBASE-13316](https://issues.apache.org/jira/browse/HBASE-13316) | Reduce the downtime on planned moves of regions |  Minor | Balancer |
| [HBASE-13342](https://issues.apache.org/jira/browse/HBASE-13342) | Fix incorrect interface annotations |  Major | . |
| [HBASE-13199](https://issues.apache.org/jira/browse/HBASE-13199) | Some small improvements on canary tool |  Major | . |
| [HBASE-13286](https://issues.apache.org/jira/browse/HBASE-13286) | Minimum timeout for a rpc call could be 1 ms instead of 2 seconds |  Minor | Client |
| [HBASE-13241](https://issues.apache.org/jira/browse/HBASE-13241) | Add tests for group level grants |  Critical | security, test |
| [HBASE-13235](https://issues.apache.org/jira/browse/HBASE-13235) | Revisit the security auditing semantics. |  Major | . |
| [HBASE-13216](https://issues.apache.org/jira/browse/HBASE-13216) | Add version info in RPC connection header |  Minor | Client, IPC/RPC |
| [HBASE-13109](https://issues.apache.org/jira/browse/HBASE-13109) | Make better SEEK vs SKIP decisions during scanning |  Major | . |
| [HBASE-13256](https://issues.apache.org/jira/browse/HBASE-13256) | HBaseConfiguration#checkDefaultsVersion(Configuration) has spelling error |  Trivial | Client |
| [HBASE-13223](https://issues.apache.org/jira/browse/HBASE-13223) | Add testMoveMeta to IntegrationTestMTTR |  Major | integration tests |
| [HBASE-13240](https://issues.apache.org/jira/browse/HBASE-13240) | add an exemption to test-patch for build-only changes. |  Minor | build |
| [HBASE-13236](https://issues.apache.org/jira/browse/HBASE-13236) | Clean up m2e-related warnings/errors from poms |  Minor | build |
| [HBASE-13162](https://issues.apache.org/jira/browse/HBASE-13162) | Add capability for cleaning hbase acls to hbase cleanup script. |  Minor | . |
| [HBASE-12405](https://issues.apache.org/jira/browse/HBASE-12405) | WAL accounting by Store |  Major | wal |
| [HBASE-13185](https://issues.apache.org/jira/browse/HBASE-13185) | Document hbase.regionserver.thrift.framed.max\_frame\_size\_in\_mb more clearly |  Trivial | documentation |
| [HBASE-13183](https://issues.apache.org/jira/browse/HBASE-13183) | Make ZK tickTime configurable in standalone HBase |  Minor | master |
| [HBASE-12706](https://issues.apache.org/jira/browse/HBASE-12706) | Support multiple port numbers in ZK quorum string |  Critical | . |
| [HBASE-13142](https://issues.apache.org/jira/browse/HBASE-13142) | [PERF] Reuse the IPCUtil#buildCellBlock buffer |  Major | Performance |
| [HBASE-13122](https://issues.apache.org/jira/browse/HBASE-13122) | Improve efficiency for return codes of some filters |  Major | Filters |
| [HBASE-13128](https://issues.apache.org/jira/browse/HBASE-13128) | Make HBCK's lock file retry creation and deletion |  Minor | hbck |
| [HBASE-13100](https://issues.apache.org/jira/browse/HBASE-13100) | Shell command to retrieve table splits |  Minor | shell |
| [HBASE-13132](https://issues.apache.org/jira/browse/HBASE-13132) | Improve RemoveColumn action debug message |  Trivial | integration tests |
| [HBASE-13138](https://issues.apache.org/jira/browse/HBASE-13138) | Clean up TestMasterObserver (debug, trying to figure why fails) |  Major | test |
| [HBASE-13120](https://issues.apache.org/jira/browse/HBASE-13120) | Allow disabling hadoop classpath and native library lookup |  Major | hbase |
| [HBASE-13086](https://issues.apache.org/jira/browse/HBASE-13086) | Show ZK root node on Master WebUI |  Minor | master |
| [HBASE-13080](https://issues.apache.org/jira/browse/HBASE-13080) | Hbase shell message containing extra quote at the end of error message. |  Trivial | . |
| [HBASE-13054](https://issues.apache.org/jira/browse/HBASE-13054) | Provide more tracing information for locking/latching events. |  Major | . |
| [HBASE-13056](https://issues.apache.org/jira/browse/HBASE-13056) | Refactor table.jsp code to remove repeated code and make it easier to add new checks |  Major | . |
| [HBASE-13002](https://issues.apache.org/jira/browse/HBASE-13002) | Make encryption cipher configurable |  Major | . |
| [HBASE-13059](https://issues.apache.org/jira/browse/HBASE-13059) | Set executable bit for scripts in dev-support |  Trivial | scripts |
| [HBASE-13044](https://issues.apache.org/jira/browse/HBASE-13044) | Configuration option for disabling coprocessor loading |  Minor | . |
| [HBASE-13018](https://issues.apache.org/jira/browse/HBASE-13018) | WALSplitter should not try to get table states while splitting META |  Critical | . |
| [HBASE-13016](https://issues.apache.org/jira/browse/HBASE-13016) | Clean up remnants of table states stored in table descriptors |  Major | . |
| [HBASE-12035](https://issues.apache.org/jira/browse/HBASE-12035) | Keep table state in META |  Critical | Client, master |
| [HBASE-12982](https://issues.apache.org/jira/browse/HBASE-12982) | Adding timeouts to TestChoreService |  Major | . |
| [HBASE-12957](https://issues.apache.org/jira/browse/HBASE-12957) | region\_mover#isSuccessfulScan may be extremely slow on region with lots of expired data |  Minor | scripts |
| [HBASE-8329](https://issues.apache.org/jira/browse/HBASE-8329) | Limit compaction speed |  Major | Compaction |
| [HBASE-12808](https://issues.apache.org/jira/browse/HBASE-12808) | Use Java API Compliance Checker for binary/source compatibility |  Major | test |
| [HBASE-12627](https://issues.apache.org/jira/browse/HBASE-12627) | Add back snapshot batching facility from HBASE-11360 dropped by HBASE-11742 |  Major | master, scaling |
| [HBASE-12896](https://issues.apache.org/jira/browse/HBASE-12896) | checkstyle report diff tool |  Minor | build |
| [HBASE-12887](https://issues.apache.org/jira/browse/HBASE-12887) | Cleanup many checkstyle errors in o.a.h.h.client |  Minor | build, Client |
| [HBASE-12840](https://issues.apache.org/jira/browse/HBASE-12840) | Improve unit test coverage of the client pushback mechanism |  Major | . |
| [HBASE-12620](https://issues.apache.org/jira/browse/HBASE-12620) | Add HBASE-11639 related items to Ref Guide |  Major | documentation |
| [HBASE-7541](https://issues.apache.org/jira/browse/HBASE-7541) | Convert all tests that use HBaseTestingUtility.createMultiRegions to HBA.createTable |  Major | . |
| [HBASE-11144](https://issues.apache.org/jira/browse/HBASE-11144) | Filter to support scanning multiple row key ranges |  Major | Filters |
| [HBASE-12839](https://issues.apache.org/jira/browse/HBASE-12839) | Remove synchronization in ServerStatisticsTracker |  Minor | . |
| [HBASE-12796](https://issues.apache.org/jira/browse/HBASE-12796) | Clean up HTable and HBaseAdmin deprecated constructor usage |  Major | . |
| [HBASE-12761](https://issues.apache.org/jira/browse/HBASE-12761) | On region jump ClientScanners should get next row start key instead of a skip. |  Major | . |
| [HBASE-11869](https://issues.apache.org/jira/browse/HBASE-11869) | Support snapshot owner |  Minor | . |
| [HBASE-12429](https://issues.apache.org/jira/browse/HBASE-12429) | Add port to ClusterManager's actions. |  Major | integration tests |
| [HBASE-12590](https://issues.apache.org/jira/browse/HBASE-12590) | A solution for data skew in HBase-Mapreduce Job |  Major | mapreduce |
| [HBASE-12223](https://issues.apache.org/jira/browse/HBASE-12223) | MultiTableInputFormatBase.getSplits is too slow |  Minor | Client |
| [HBASE-10201](https://issues.apache.org/jira/browse/HBASE-10201) | Port 'Make flush decisions per column family' to trunk |  Major | wal |
| [HBASE-12601](https://issues.apache.org/jira/browse/HBASE-12601) | Explain how to grant/revoke permission to a group/namespace in grant/revoke command usage |  Minor | documentation, security, shell |
| [HBASE-12650](https://issues.apache.org/jira/browse/HBASE-12650) | Move ServerName to hbase-common module |  Blocker | . |
| [HBASE-12559](https://issues.apache.org/jira/browse/HBASE-12559) | Provide LoadBalancer with online configuration capability |  Major | . |
| [HBASE-11939](https://issues.apache.org/jira/browse/HBASE-11939) | Document compressed blockcache |  Major | documentation |
| [HBASE-12207](https://issues.apache.org/jira/browse/HBASE-12207) | A script to help keep your Git repo fresh |  Major | documentation, scripts |
| [HBASE-12251](https://issues.apache.org/jira/browse/HBASE-12251) | [book] Hadoop compat matrix 0.94 section needs cleaned up |  Major | documentation |
| [HBASE-12220](https://issues.apache.org/jira/browse/HBASE-12220) | Add hedgedReads and hedgedReadWins metrics |  Major | . |
| [HBASE-12195](https://issues.apache.org/jira/browse/HBASE-12195) | Fix dev-support/findHangingTests |  Minor | test |
| [HBASE-12003](https://issues.apache.org/jira/browse/HBASE-12003) | Fix SecureBulkLoadEndpoint class javadoc formatting |  Trivial | documentation, security |
| [HBASE-6290](https://issues.apache.org/jira/browse/HBASE-6290) | Add a function a mark a server as dead and start the recovery the process |  Minor | monitoring |
| [HBASE-11862](https://issues.apache.org/jira/browse/HBASE-11862) | Get rid of Writables in HTableDescriptor, HColumnDescriptor |  Minor | . |
| [HBASE-11760](https://issues.apache.org/jira/browse/HBASE-11760) | Tighten up region state transition |  Major | Region Assignment |
| [HBASE-11611](https://issues.apache.org/jira/browse/HBASE-11611) | Clean up ZK-based region assignment |  Major | Region Assignment |
| [HBASE-11585](https://issues.apache.org/jira/browse/HBASE-11585) | PE: Allows warm-up |  Trivial | test |
| [HBASE-3135](https://issues.apache.org/jira/browse/HBASE-3135) | Make our MR jobs implement Tool and use ToolRunner so can do -D trickery, etc. |  Major | . |
| [HBASE-11548](https://issues.apache.org/jira/browse/HBASE-11548) | [PE] Add 'cycling' test N times and unit tests for size/zipf/valueSize calculations |  Trivial | test |
| [HBASE-11400](https://issues.apache.org/jira/browse/HBASE-11400) | Edit, consolidate, and update Compression and data encoding docs |  Minor | documentation |
| [HBASE-5696](https://issues.apache.org/jira/browse/HBASE-5696) | Use Hadoop's DataOutputOutputStream instead of have a copy local |  Major | . |
| [HBASE-11344](https://issues.apache.org/jira/browse/HBASE-11344) | Hide row keys and such from the web UIs |  Major | . |
| [HBASE-6580](https://issues.apache.org/jira/browse/HBASE-6580) | Deprecate HTablePool in favor of HConnection.getTable(...) |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-19924](https://issues.apache.org/jira/browse/HBASE-19924) | hbase rpc throttling does not work for multi() with request count rater. |  Major | rpc |
| [HBASE-20293](https://issues.apache.org/jira/browse/HBASE-20293) | get\_splits returns duplicate split points when region replication is on |  Minor | shell |
| [HBASE-20464](https://issues.apache.org/jira/browse/HBASE-20464) | Disable IMC |  Major | in-memory-compaction |
| [HBASE-20442](https://issues.apache.org/jira/browse/HBASE-20442) | clean up incorrect use of commons-collections 3 |  Major | dependencies, thirdparty |
| [HBASE-20440](https://issues.apache.org/jira/browse/HBASE-20440) | Clean up incorrect use of commons-lang 2.y |  Major | dependencies |
| [HBASE-20439](https://issues.apache.org/jira/browse/HBASE-20439) | Clean up incorrect use of commons-logging in hbase-server |  Minor | dependencies, logging |
| [HBASE-20398](https://issues.apache.org/jira/browse/HBASE-20398) | Redirect doesn't work on web UI |  Major | UI |
| [HBASE-20399](https://issues.apache.org/jira/browse/HBASE-20399) | Fix merge layout |  Minor | UI |
| [HBASE-20233](https://issues.apache.org/jira/browse/HBASE-20233) | [metrics] Ill-formatted numRegions metric in "Hadoop:service=HBase,name=RegionServer,sub=Regions" mbean |  Trivial | metrics, Operability |
| [HBASE-20410](https://issues.apache.org/jira/browse/HBASE-20410) | upgrade protoc compiler to 3.5.1-1 |  Critical | build, dependencies, Protobufs |
| [HBASE-20224](https://issues.apache.org/jira/browse/HBASE-20224) | Web UI is broken in standalone mode |  Blocker | UI, Usability |
| [HBASE-20394](https://issues.apache.org/jira/browse/HBASE-20394) | HBase over rides the value of HBASE\_OPTS (if any) set by client |  Minor | Operability |
| [HBASE-20397](https://issues.apache.org/jira/browse/HBASE-20397) | Make it more explicit that monkey.properties is found on CLASSPATH |  Trivial | . |
| [HBASE-20376](https://issues.apache.org/jira/browse/HBASE-20376) | RowCounter and CellCounter documentations are incorrect |  Minor | documentation, mapreduce |
| [HBASE-20330](https://issues.apache.org/jira/browse/HBASE-20330) | ProcedureExecutor.start() gets stuck in recover lease on store. |  Major | proc-v2 |
| [HBASE-20350](https://issues.apache.org/jira/browse/HBASE-20350) | NullPointerException in Scanner during close() |  Blocker | . |
| [HBASE-20280](https://issues.apache.org/jira/browse/HBASE-20280) | Fix possibility of deadlocking in refreshFileConnections when prefetch is enabled |  Major | BucketCache |
| [HBASE-20310](https://issues.apache.org/jira/browse/HBASE-20310) | [hbck] bin/hbase hbck -metaonly shows false inconsistency on HBase 2 |  Major | hbck |
| [HBASE-20219](https://issues.apache.org/jira/browse/HBASE-20219) | An error occurs when scanning with reversed=true and loadColumnFamiliesOnDemand=true |  Critical | phoenix |
| [HBASE-20358](https://issues.apache.org/jira/browse/HBASE-20358) | Fix bin/hbase thrift usage text |  Minor | . |
| [HBASE-20382](https://issues.apache.org/jira/browse/HBASE-20382) | If RSGroups not enabled, rsgroup.jsp prints stack trace |  Major | rsgroup, UI |
| [HBASE-20385](https://issues.apache.org/jira/browse/HBASE-20385) | Purge md5-making from our little make\_rc.sh script |  Minor | . |
| [HBASE-20384](https://issues.apache.org/jira/browse/HBASE-20384) | [AMv2] Logging format improvements; use encoded name rather than full region name marking  transitions |  Minor | . |
| [HBASE-20182](https://issues.apache.org/jira/browse/HBASE-20182) | Can not locate region after split and merge |  Blocker | Region Assignment |
| [HBASE-20363](https://issues.apache.org/jira/browse/HBASE-20363) | TestNamespaceAuditor.testRegionMerge is flaky |  Major | test |
| [HBASE-20362](https://issues.apache.org/jira/browse/HBASE-20362) | TestMasterShutdown.testMasterShutdownBeforeStartingAnyRegionServer is flaky |  Major | test |
| [HBASE-20295](https://issues.apache.org/jira/browse/HBASE-20295) | TableOutputFormat.checkOutputSpecs throw NullPointerException Exception |  Major | mapreduce |
| [HBASE-20343](https://issues.apache.org/jira/browse/HBASE-20343) | [DOC] fix log directory paths |  Critical | documentation |
| [HBASE-16499](https://issues.apache.org/jira/browse/HBASE-16499) | slow replication for small HBase clusters |  Critical | Replication |
| [HBASE-17518](https://issues.apache.org/jira/browse/HBASE-17518) | HBase Reference Guide has a syntax error |  Minor | documentation |
| [HBASE-20231](https://issues.apache.org/jira/browse/HBASE-20231) | Not able to delete column family from a row using RemoteHTable |  Major | REST |
| [HBASE-20259](https://issues.apache.org/jira/browse/HBASE-20259) | Doc configs for in-memory-compaction and add detail to in-memory-compaction logging |  Critical | . |
| [HBASE-17631](https://issues.apache.org/jira/browse/HBASE-17631) | Canary interval too low |  Major | canary |
| [HBASE-20282](https://issues.apache.org/jira/browse/HBASE-20282) | Provide short name invocations for useful tools |  Major | documentation, tooling |
| [HBASE-20314](https://issues.apache.org/jira/browse/HBASE-20314) | Precommit build for master branch fails because of surefire fork fails |  Major | build |
| [HBASE-20261](https://issues.apache.org/jira/browse/HBASE-20261) | Table page (table.jsp) in Master UI does not show replicaIds for hbase meta table |  Minor | UI |
| [HBASE-20229](https://issues.apache.org/jira/browse/HBASE-20229) | ConnectionImplementation.locateRegions() returns duplicated entries when region replication is on |  Major | . |
| [HBASE-20308](https://issues.apache.org/jira/browse/HBASE-20308) | test Dockerfile needs to include git |  Blocker | build, test |
| [HBASE-20130](https://issues.apache.org/jira/browse/HBASE-20130) | Use defaults (16020 & 16030) as base ports when the RS is bound to localhost |  Critical | documentation |
| [HBASE-20111](https://issues.apache.org/jira/browse/HBASE-20111) | Able to split region explicitly even on shouldSplit return false from split policy |  Critical | . |
| [HBASE-20292](https://issues.apache.org/jira/browse/HBASE-20292) | Wrong URLs in the descriptions for update\_all\_config and update\_config commands in shell |  Trivial | shell |
| [HBASE-13300](https://issues.apache.org/jira/browse/HBASE-13300) | Fix casing in getTimeStamp() and setTimestamp() for Mutations |  Critical | API |
| [HBASE-20237](https://issues.apache.org/jira/browse/HBASE-20237) | Put back getClosestRowBefore and throw UnknownProtocolException instead... for asynchbase client |  Critical | compatibility, Operability |
| [HBASE-20090](https://issues.apache.org/jira/browse/HBASE-20090) | Properly handle Preconditions check failure in MemStoreFlusher$FlushHandler.run |  Major | . |
| [HBASE-19639](https://issues.apache.org/jira/browse/HBASE-19639) | ITBLL can't go big because RegionTooBusyException... Above memstore limit |  Blocker | . |
| [HBASE-20213](https://issues.apache.org/jira/browse/HBASE-20213) | [LOGGING] Aligning formatting and logging less (compactions, in-memory compactions) |  Major | logging |
| [HBASE-20141](https://issues.apache.org/jira/browse/HBASE-20141) | Fix TooManyFiles exception when RefreshingChannels in FileIOEngine |  Major | BucketCache |
| [HBASE-18216](https://issues.apache.org/jira/browse/HBASE-18216) | [AMv2] Workaround for HBASE-18152, corrupt procedure WAL |  Major | proc-v2 |
| [HBASE-20200](https://issues.apache.org/jira/browse/HBASE-20200) | list\_procedures fails in shell |  Major | shell |
| [HBASE-20185](https://issues.apache.org/jira/browse/HBASE-20185) | Fix ACL check for MasterRpcServices#execProcedure |  Major | . |
| [HBASE-20146](https://issues.apache.org/jira/browse/HBASE-20146) | Regions are stuck while opening when WAL is disabled |  Critical | wal |
| [HBASE-20187](https://issues.apache.org/jira/browse/HBASE-20187) | Shell startup fails with IncompatibleClassChangeError |  Blocker | shell |
| [HBASE-20189](https://issues.apache.org/jira/browse/HBASE-20189) | Typo in Required Java Version error message while building HBase. |  Trivial | build |
| [HBASE-20078](https://issues.apache.org/jira/browse/HBASE-20078) | MultiByteBuff : bug in reading primitives when individual buffers are too small |  Major | . |
| [HBASE-19075](https://issues.apache.org/jira/browse/HBASE-19075) | Task tabs on master UI cause page scroll |  Major | master |
| [HBASE-20104](https://issues.apache.org/jira/browse/HBASE-20104) | Fix infinite loop of RIT when creating table on a rsgroup that has no online servers |  Major | rsgroup |
| [HBASE-19802](https://issues.apache.org/jira/browse/HBASE-19802) | Wrong usage messages on shell commands (grant/revoke namespace syntax) |  Minor | shell |
| [HBASE-20153](https://issues.apache.org/jira/browse/HBASE-20153) | enable error-prone analysis in precommit |  Major | community |
| [HBASE-20024](https://issues.apache.org/jira/browse/HBASE-20024) | TestMergeTableRegionsProcedure is STILL flakey |  Major | . |
| [HBASE-19598](https://issues.apache.org/jira/browse/HBASE-19598) | Fix TestAssignmentManagerMetrics flaky test |  Major | test |
| [HBASE-20162](https://issues.apache.org/jira/browse/HBASE-20162) | [nightly] depending on pipeline execution we sometimes refer to the wrong workspace |  Critical | test |
| [HBASE-20164](https://issues.apache.org/jira/browse/HBASE-20164) | failed hadoopcheck should add footer link |  Major | community |
| [HBASE-20160](https://issues.apache.org/jira/browse/HBASE-20160) | TestRestartCluster.testRetainAssignmentOnRestart uses the wrong condition to decide whether the assignment is finished |  Major | Region Assignment |
| [HBASE-20114](https://issues.apache.org/jira/browse/HBASE-20114) | Fix IllegalFormatConversionException in rsgroup.jsp |  Major | UI |
| [HBASE-20144](https://issues.apache.org/jira/browse/HBASE-20144) | The shutdown of master will hang if there are no live region server |  Major | Recovery |
| [HBASE-19987](https://issues.apache.org/jira/browse/HBASE-19987) | update error-prone to 2.2.0 |  Major | . |
| [HBASE-20108](https://issues.apache.org/jira/browse/HBASE-20108) | \`hbase zkcli\` falls into a non-interactive prompt after HBASE-15199 |  Critical | Usability |
| [HBASE-19814](https://issues.apache.org/jira/browse/HBASE-19814) | Release hbase-2.0.0-beta-2; "rolling upgrade" release |  Blocker | . |
| [HBASE-20134](https://issues.apache.org/jira/browse/HBASE-20134) | support scripts use hard-coded /tmp |  Minor | website |
| [HBASE-20070](https://issues.apache.org/jira/browse/HBASE-20070) | website generation is failing |  Blocker | website |
| [HBASE-19147](https://issues.apache.org/jira/browse/HBASE-19147) | All branch-2 unit tests pass |  Blocker | test |
| [HBASE-20110](https://issues.apache.org/jira/browse/HBASE-20110) | Findbugs in zk and mr caused nightly #409 branch-2 to fail |  Major | findbugs |
| [HBASE-19656](https://issues.apache.org/jira/browse/HBASE-19656) | Disable TestAssignmentManagerMetrics for beta-1 |  Major | . |
| [HBASE-19863](https://issues.apache.org/jira/browse/HBASE-19863) | java.lang.IllegalStateException: isDelete failed when SingleColumnValueFilter is used |  Major | Filters |
| [HBASE-20106](https://issues.apache.org/jira/browse/HBASE-20106) | API Compliance checker should fall back to specifying origin as remote repo |  Major | API, community |
| [HBASE-20066](https://issues.apache.org/jira/browse/HBASE-20066) | Region sequence id may go backward after split or merge |  Critical | . |
| [HBASE-20086](https://issues.apache.org/jira/browse/HBASE-20086) | PE randomSeekScan fails with ClassNotFoundException |  Major | . |
| [HBASE-20074](https://issues.apache.org/jira/browse/HBASE-20074) | [FindBugs] Same code on both branches in CompactingMemStore#initMemStoreCompactor |  Major | findbugs |
| [HBASE-19974](https://issues.apache.org/jira/browse/HBASE-19974) | Fix decommissioned servers cannot be removed by remove\_servers\_rsgroup methods |  Major | rsgroup |
| [HBASE-20043](https://issues.apache.org/jira/browse/HBASE-20043) | ITBLL fails against hadoop3 |  Major | integration tests |
| [HBASE-19583](https://issues.apache.org/jira/browse/HBASE-19583) | update RM list to remove EOM versions |  Minor | community, documentation |
| [HBASE-19728](https://issues.apache.org/jira/browse/HBASE-19728) | Add lock to filesCompacting in all place. |  Major | . |
| [HBASE-20061](https://issues.apache.org/jira/browse/HBASE-20061) | HStore synchronized member variable filesCompacting should be private |  Major | regionserver |
| [HBASE-20062](https://issues.apache.org/jira/browse/HBASE-20062) | findbugs is not running on precommit checks |  Blocker | community, test |
| [HBASE-20054](https://issues.apache.org/jira/browse/HBASE-20054) | Forward port HBASE-18282 ReplicationLogCleaner can delete WALs not yet replicated in case of KeeperException |  Major | . |
| [HBASE-20049](https://issues.apache.org/jira/browse/HBASE-20049) | Region replicas of SPLIT and MERGED regions are kept in in-memory states until restarting master |  Major | read replicas |
| [HBASE-19767](https://issues.apache.org/jira/browse/HBASE-19767) | Master web UI shows negative values for Remaining KVs |  Major | . |
| [HBASE-19391](https://issues.apache.org/jira/browse/HBASE-19391) | Calling HRegion#initializeRegionInternals from a region replica can still re-create a region directory |  Major | . |
| [HBASE-19166](https://issues.apache.org/jira/browse/HBASE-19166) | AsyncProtobufLogWriter persists ProtobufLogWriter as class name for backward compatibility |  Blocker | wal |
| [HBASE-20027](https://issues.apache.org/jira/browse/HBASE-20027) | Add test TestClusterPortAssignment |  Major | . |
| [HBASE-19953](https://issues.apache.org/jira/browse/HBASE-19953) | Avoid calling post\* hook when procedure fails |  Critical | master, proc-v2 |
| [HBASE-20037](https://issues.apache.org/jira/browse/HBASE-20037) | Race when calling SequenceIdAccounting.resetHighest |  Blocker | wal |
| [HBASE-19991](https://issues.apache.org/jira/browse/HBASE-19991) | lots of hbase-rest test failures against hadoop 3 |  Major | REST, test |
| [HBASE-19920](https://issues.apache.org/jira/browse/HBASE-19920) | TokenUtil.obtainToken unnecessarily creates a local directory |  Major | . |
| [HBASE-20017](https://issues.apache.org/jira/browse/HBASE-20017) | BufferedMutatorImpl submit the same mutation repeatedly |  Blocker | Client |
| [HBASE-20032](https://issues.apache.org/jira/browse/HBASE-20032) | Receving multiple warnings for missing reporting.plugins.plugin.version |  Minor | hbase |
| [HBASE-19954](https://issues.apache.org/jira/browse/HBASE-19954) | Separate TestBlockReorder into individual tests to avoid ShutdownHook suppression error against hadoop3 |  Major | . |
| [HBASE-20023](https://issues.apache.org/jira/browse/HBASE-20023) | CompactionTool command line examples are incorrect |  Trivial | hbase |
| [HBASE-14897](https://issues.apache.org/jira/browse/HBASE-14897) | TestTableLockManager.testReapAllTableLocks is flakey |  Major | . |
| [HBASE-20020](https://issues.apache.org/jira/browse/HBASE-20020) | Make sure we throw DoNotRetryIOException when ConnectionImplementation is closed |  Critical | Client |
| [HBASE-19980](https://issues.apache.org/jira/browse/HBASE-19980) | NullPointerException when restoring a snapshot after splitting a region |  Major | snapshots |
| [HBASE-19998](https://issues.apache.org/jira/browse/HBASE-19998) | Flakey TestVisibilityLabelsWithDefaultVisLabelService |  Major | flakey, test |
| [HBASE-19996](https://issues.apache.org/jira/browse/HBASE-19996) | Some nonce procs might not be cleaned up (follow up HBASE-19756) |  Major | . |
| [HBASE-18282](https://issues.apache.org/jira/browse/HBASE-18282) | ReplicationLogCleaner can delete WALs not yet replicated in case of a KeeperException |  Critical | Replication |
| [HBASE-19979](https://issues.apache.org/jira/browse/HBASE-19979) | ReplicationSyncUp tool may leak Zookeeper connection |  Major | Replication |
| [HBASE-19876](https://issues.apache.org/jira/browse/HBASE-19876) | The exception happening in converting pb mutation to hbase.mutation messes up the CellScanner |  Critical | . |
| [HBASE-19977](https://issues.apache.org/jira/browse/HBASE-19977) | FileMmapEngine allocation of byte buffers should be synchronized |  Major | . |
| [HBASE-19986](https://issues.apache.org/jira/browse/HBASE-19986) | If HBaseTestClassRule timesout a test, thread dump. |  Major | . |
| [HBASE-19968](https://issues.apache.org/jira/browse/HBASE-19968) | MapReduce test fails with NoClassDefFoundError against hadoop3 |  Major | . |
| [HBASE-16060](https://issues.apache.org/jira/browse/HBASE-16060) | 1.x clients cannot access table state talking to 2.0 cluster |  Blocker | . |
| [HBASE-19972](https://issues.apache.org/jira/browse/HBASE-19972) | Should rethrow  the RetriesExhaustedWithDetailsException when failed to apply the batch in ReplicationSink |  Critical | Replication |
| [HBASE-19964](https://issues.apache.org/jira/browse/HBASE-19964) | TestWriteHeavyIncrementObserver fails |  Major | test |
| [HBASE-19966](https://issues.apache.org/jira/browse/HBASE-19966) | The WriteEntry for WALKey maybe null if we failed to call WAL.append |  Major | wal |
| [HBASE-19937](https://issues.apache.org/jira/browse/HBASE-19937) | Ensure createRSGroupTable be called after ProcedureExecutor and LoadBalancer are initialized |  Major | rsgroup |
| [HBASE-19929](https://issues.apache.org/jira/browse/HBASE-19929) | Call RS.stop on a session expired RS may hang |  Major | wal |
| [HBASE-19900](https://issues.apache.org/jira/browse/HBASE-19900) | Region-level exception destroy the result of batch |  Critical | . |
| [HBASE-19941](https://issues.apache.org/jira/browse/HBASE-19941) | Flaky TestCreateTableProcedure times out in nightly, needs to LargeTests |  Major | test |
| [HBASE-19934](https://issues.apache.org/jira/browse/HBASE-19934) | HBaseSnapshotException when read replicas is enabled and online snapshot is taken after region splitting |  Major | snapshots |
| [HBASE-19939](https://issues.apache.org/jira/browse/HBASE-19939) | TestSplitTableRegionProcedure#testSplitWithoutPONR() and testRecoveryAndDoubleExecution() are failing with NPE |  Major | amv2 |
| [HBASE-19907](https://issues.apache.org/jira/browse/HBASE-19907) | TestMetaWithReplicas still flakey |  Major | . |
| [HBASE-19907](https://issues.apache.org/jira/browse/HBASE-19907) | TestMetaWithReplicas still flakey |  Major | . |
| [HBASE-19926](https://issues.apache.org/jira/browse/HBASE-19926) | Use a separated class to implement the WALActionListener for Replication |  Major | Replication, wal |
| [HBASE-19703](https://issues.apache.org/jira/browse/HBASE-19703) | Functionality added as part of HBASE-12583 is not working after moving the split code to master |  Major | . |
| [HBASE-19658](https://issues.apache.org/jira/browse/HBASE-19658) | Fix and reenable TestCompactingToCellFlatMapMemStore#testFlatteningToJumboCellChunkMap |  Major | test |
| [HBASE-19905](https://issues.apache.org/jira/browse/HBASE-19905) | ReplicationSyncUp tool will not exit if a peer replication is disabled |  Major | Replication |
| [HBASE-19726](https://issues.apache.org/jira/browse/HBASE-19726) | Failed to start HMaster due to infinite retrying on meta assign |  Major | . |
| [HBASE-19919](https://issues.apache.org/jira/browse/HBASE-19919) | Tidying up logging |  Major | . |
| [HBASE-19901](https://issues.apache.org/jira/browse/HBASE-19901) | Up yetus proclimit on nightlies |  Major | . |
| [HBASE-19884](https://issues.apache.org/jira/browse/HBASE-19884) | BucketEntryGroup's equals, hashCode and compareTo methods are not consistent |  Major | . |
| [HBASE-19906](https://issues.apache.org/jira/browse/HBASE-19906) | TestZooKeeper Timeout |  Major | . |
| [HBASE-19892](https://issues.apache.org/jira/browse/HBASE-19892) | Checking 'patch attach' and yetus 0.7.0 and move to Yetus 0.7.0 |  Major | . |
| [HBASE-19871](https://issues.apache.org/jira/browse/HBASE-19871) | delete.rb should require user to provide the column |  Major | shell |
| [HBASE-19818](https://issues.apache.org/jira/browse/HBASE-19818) | Scan time limit not work if the filter always filter row key |  Major | . |
| [HBASE-19756](https://issues.apache.org/jira/browse/HBASE-19756) | Master NPE during completed failed proc eviction |  Major | . |
| [HBASE-17079](https://issues.apache.org/jira/browse/HBASE-17079) | HBase build fails on windows, hbase-archetype-builder is reason for failure |  Major | build |
| [HBASE-19853](https://issues.apache.org/jira/browse/HBASE-19853) | duplicate slf4j declaration in mapreduce pom |  Minor | dependencies |
| [HBASE-19838](https://issues.apache.org/jira/browse/HBASE-19838) | Can not shutdown backup master cleanly when it has already tried to become the active master |  Critical | master |
| [HBASE-19780](https://issues.apache.org/jira/browse/HBASE-19780) | Change execution phase of checkstyle plugin back to default 'verify' |  Major | . |
| [HBASE-19774](https://issues.apache.org/jira/browse/HBASE-19774) | incorrect behavior of locateRegionInMeta |  Major | . |
| [HBASE-17513](https://issues.apache.org/jira/browse/HBASE-17513) | Thrift Server 1 uses different QOP settings than RPC and Thrift Server 2 and can easily be misconfigured so there is no encryption when the operator expects it. |  Critical | documentation, security, Thrift, Usability |
| [HBASE-19836](https://issues.apache.org/jira/browse/HBASE-19836) | Fix TestZooKeeper.testLogSplittingAfterMasterRecoveryDueToZKExpiry |  Major | test |
| [HBASE-19794](https://issues.apache.org/jira/browse/HBASE-19794) | TestZooKeeper hangs |  Critical | . |
| [HBASE-19828](https://issues.apache.org/jira/browse/HBASE-19828) | Flakey TestRegionsOnMasterOptions.testRegionsOnAllServers |  Major | . |
| [HBASE-19829](https://issues.apache.org/jira/browse/HBASE-19829) | hadoop-minicluster pulls zookeeper:test-jar:tests 3.4.6 |  Minor | . |
| [HBASE-19163](https://issues.apache.org/jira/browse/HBASE-19163) | "Maximum lock count exceeded" from region server's batch processing |  Major | regionserver |
| [HBASE-19757](https://issues.apache.org/jira/browse/HBASE-19757) | System table gets stuck after enabling region server group feature in secure cluster |  Critical | . |
| [HBASE-19825](https://issues.apache.org/jira/browse/HBASE-19825) | Fix hadoop3 compat test failures |  Major | pom |
| [HBASE-19822](https://issues.apache.org/jira/browse/HBASE-19822) | HFileCleaner threads stick around after shutdown stuck on queue#take |  Major | . |
| [HBASE-19815](https://issues.apache.org/jira/browse/HBASE-19815) | Flakey TestAssignmentManager.testAssignWithRandExec |  Major | flakey, test |
| [HBASE-19821](https://issues.apache.org/jira/browse/HBASE-19821) | TestCleanerChore#testOnConfigurationChange() requires at least 4 processors to get passed |  Minor | test |
| [HBASE-19806](https://issues.apache.org/jira/browse/HBASE-19806) | Lower max versions for table column family of hbase:meta |  Trivial | . |
| [HBASE-19812](https://issues.apache.org/jira/browse/HBASE-19812) | TestFlushSnapshotFromClient fails because of failing region.flush |  Major | . |
| [HBASE-19816](https://issues.apache.org/jira/browse/HBASE-19816) | Replication sink list is not updated on UnknownHostException |  Major | Replication |
| [HBASE-19784](https://issues.apache.org/jira/browse/HBASE-19784) | stop-hbase gives unfriendly message when local hbase isn't running |  Minor | scripts |
| [HBASE-19196](https://issues.apache.org/jira/browse/HBASE-19196) | Release hbase-2.0.0-beta-1; the "Finish-line" release |  Blocker | . |
| [HBASE-19808](https://issues.apache.org/jira/browse/HBASE-19808) | Reenable TestMultiParallel |  Major | test |
| [HBASE-19792](https://issues.apache.org/jira/browse/HBASE-19792) | TestReplicationSmallTests.testDisableEnable fails |  Major | test |
| [HBASE-19797](https://issues.apache.org/jira/browse/HBASE-19797) | Operator priority leads to wrong logic in ReplicationSourceWALReader |  Major | . |
| [HBASE-19732](https://issues.apache.org/jira/browse/HBASE-19732) | Replica regions does not return back the MSLAB chunks to pool |  Critical | . |
| [HBASE-19768](https://issues.apache.org/jira/browse/HBASE-19768) | RegionServer startup failing when DN is dead |  Critical | . |
| [HBASE-19752](https://issues.apache.org/jira/browse/HBASE-19752) | RSGroupBasedLoadBalancer#getMisplacedRegions() should handle the case where rs group cannot be determined |  Major | . |
| [HBASE-11409](https://issues.apache.org/jira/browse/HBASE-11409) | Add more flexibility for input directory structure to LoadIncrementalHFiles |  Major | . |
| [HBASE-19694](https://issues.apache.org/jira/browse/HBASE-19694) | The initialization order for a fresh cluster is incorrect |  Critical | . |
| [HBASE-19769](https://issues.apache.org/jira/browse/HBASE-19769) | IllegalAccessError on package-private Hadoop metrics2 classes in MapReduce jobs |  Critical | mapreduce, metrics |
| [HBASE-19775](https://issues.apache.org/jira/browse/HBASE-19775) | hbase shell doesn't handle the exceptions that are wrapped in java.io.UncheckedIOException |  Major | shell |
| [HBASE-19771](https://issues.apache.org/jira/browse/HBASE-19771) | restore\_snapshot shell command gives wrong namespace if the namespace doesn't exist |  Minor | . |
| [HBASE-19773](https://issues.apache.org/jira/browse/HBASE-19773) | Adding javadoc around getting instance of RawCellBuilder |  Minor | . |
| [HBASE-19685](https://issues.apache.org/jira/browse/HBASE-19685) | Fix TestFSErrorsExposed#testFullSystemBubblesFSErrors |  Major | test |
| [HBASE-19755](https://issues.apache.org/jira/browse/HBASE-19755) | Error message for non-existent namespace is inaccurate |  Minor | . |
| [HBASE-19749](https://issues.apache.org/jira/browse/HBASE-19749) | Revisit  logic of UserScanQueryMatcher#mergeFilterResponse method |  Major | . |
| [HBASE-19740](https://issues.apache.org/jira/browse/HBASE-19740) | Repeated error message for NamespaceExistException |  Minor | . |
| [HBASE-19734](https://issues.apache.org/jira/browse/HBASE-19734) | IntegrationTestReplication broken w/ separate clusters |  Critical | integration tests |
| [HBASE-19729](https://issues.apache.org/jira/browse/HBASE-19729) | UserScanQueryMatcher#mergeFilterResponse should return INCLUDE\_AND\_SEEK\_NEXT\_ROW when filterResponse is INCLUDE\_AND\_SEEK\_NEXT\_ROW |  Major | . |
| [HBASE-19744](https://issues.apache.org/jira/browse/HBASE-19744) | Fix flakey TestZKLeaderManager |  Major | test |
| [HBASE-19717](https://issues.apache.org/jira/browse/HBASE-19717) | IntegrationTestDDLMasterFailover is using outdated values for DataBlockEncoding |  Major | integration tests |
| [HBASE-19424](https://issues.apache.org/jira/browse/HBASE-19424) | Metrics servlet throws NPE |  Minor | . |
| [HBASE-19712](https://issues.apache.org/jira/browse/HBASE-19712) | Fix TestSnapshotQuotaObserverChore#testSnapshotSize |  Major | test |
| [HBASE-19696](https://issues.apache.org/jira/browse/HBASE-19696) | Filter returning INCLUDE\_AND\_NEXT\_COL doesn't skip remaining versions when scan has explicit columns |  Critical | . |
| [HBASE-19721](https://issues.apache.org/jira/browse/HBASE-19721) | Unnecessary stubbings detected in test class: TestReversedScannerCallable |  Major | test |
| [HBASE-19714](https://issues.apache.org/jira/browse/HBASE-19714) | \`status 'detailed'\` invokes nonexistent "getRegionsInTransition" method on ClusterStatus |  Critical | shell |
| [HBASE-19709](https://issues.apache.org/jira/browse/HBASE-19709) | Guard against a ThreadPool size of 0 in CleanerChore |  Critical | . |
| [HBASE-19383](https://issues.apache.org/jira/browse/HBASE-19383) | [1.2] java.lang.AssertionError: expected:\<2\> but was:\<1\> 	at org.apache.hadoop.hbase.TestChoreService.testTriggerNowFailsWhenNotScheduled(TestChoreService.java:707) |  Major | test |
| [HBASE-19688](https://issues.apache.org/jira/browse/HBASE-19688) | TimeToLiveProcedureWALCleaner should extends BaseLogCleanerDelegate |  Minor | . |
| [HBASE-18452](https://issues.apache.org/jira/browse/HBASE-18452) | VerifyReplication by Snapshot should cache HDFS token before submit job for kerberos env. |  Major | . |
| [HBASE-19588](https://issues.apache.org/jira/browse/HBASE-19588) | Additional jar dependencies needed for mapreduce PerformanceEvaluation |  Minor | test |
| [HBASE-19490](https://issues.apache.org/jira/browse/HBASE-19490) | Rare failure in TestRateLimiter |  Major | test |
| [HBASE-19691](https://issues.apache.org/jira/browse/HBASE-19691) | Do not require ADMIN permission for obtaining ClusterStatus |  Critical | . |
| [HBASE-19392](https://issues.apache.org/jira/browse/HBASE-19392) | TestReplicaWithCluster#testReplicaGetWithPrimaryAndMetaDown failure in master |  Minor | regionserver |
| [HBASE-19654](https://issues.apache.org/jira/browse/HBASE-19654) | Remove misleading and chatty debug message in ReplicationLogCleaner |  Major | . |
| [HBASE-19666](https://issues.apache.org/jira/browse/HBASE-19666) | TestDefaultCompactSelection test failed |  Critical | test |
| [HBASE-19671](https://issues.apache.org/jira/browse/HBASE-19671) | Fix TestMultiParallel#testActiveThreadsCount |  Minor | test |
| [HBASE-19672](https://issues.apache.org/jira/browse/HBASE-19672) | Correct comments for default values of major compaction in SortedCompactionPolicy#getNextMajorCompactTime() |  Minor | . |
| [HBASE-19643](https://issues.apache.org/jira/browse/HBASE-19643) | Need to update cache location when get error in AsyncBatchRpcRetryingCaller |  Major | . |
| [HBASE-19619](https://issues.apache.org/jira/browse/HBASE-19619) | Modify replication\_admin.rb to use ReplicationPeerConfigBuilder |  Critical | . |
| [HBASE-19624](https://issues.apache.org/jira/browse/HBASE-19624) | TestIOFencing hangs |  Major | . |
| [HBASE-19496](https://issues.apache.org/jira/browse/HBASE-19496) | Reusing the ByteBuffer in rpc layer corrupt the ServerLoad and RegionLoad |  Blocker | . |
| [HBASE-19457](https://issues.apache.org/jira/browse/HBASE-19457) | Debugging flaky TestTruncateTableProcedure#testRecoveryAndDoubleExecutionPreserveSplits |  Major | . |
| [HBASE-19608](https://issues.apache.org/jira/browse/HBASE-19608) | Race in MasterRpcServices.getProcedureResult |  Major | proc-v2 |
| [HBASE-19593](https://issues.apache.org/jira/browse/HBASE-19593) | Possible NPE if wal is closed during waledit append. |  Major | . |
| [HBASE-19589](https://issues.apache.org/jira/browse/HBASE-19589) | New regions should always be added with state CLOSED (followup of HBASE-19530) |  Major | . |
| [HBASE-17248](https://issues.apache.org/jira/browse/HBASE-17248) | SimpleRegionNormalizer javadoc correction |  Trivial | master |
| [HBASE-19148](https://issues.apache.org/jira/browse/HBASE-19148) | Reevaluate default values of configurations |  Blocker | defaults |
| [HBASE-19578](https://issues.apache.org/jira/browse/HBASE-19578) | MasterProcWALs cleaning is incorrect |  Critical | amv2 |
| [HBASE-19559](https://issues.apache.org/jira/browse/HBASE-19559) | Fix TestLogRolling.testLogRollOnDatanodeDeath |  Major | test, wal |
| [HBASE-19218](https://issues.apache.org/jira/browse/HBASE-19218) | Master stuck thinking hbase:namespace is assigned after restart preventing intialization |  Critical | . |
| [HBASE-19542](https://issues.apache.org/jira/browse/HBASE-19542) | fix TestSafemodeBringsDownMaster |  Major | . |
| [HBASE-19563](https://issues.apache.org/jira/browse/HBASE-19563) | A few hbase-procedure classes missing @InterfaceAudience annotation |  Minor | proc-v2 |
| [HBASE-19561](https://issues.apache.org/jira/browse/HBASE-19561) | maxCacheSize in CacheEvictionStats can't be accumulated repeatedly When dealing with each region |  Major | . |
| [HBASE-19558](https://issues.apache.org/jira/browse/HBASE-19558) | TestRegionsOnMasterOptions hack so it works reliablly |  Major | test |
| [HBASE-19555](https://issues.apache.org/jira/browse/HBASE-19555) | TestSplitTransactionOnCluster is flaky |  Major | test |
| [HBASE-19532](https://issues.apache.org/jira/browse/HBASE-19532) | AssignProcedure#COMPARATOR may produce incorrect sort order |  Critical | . |
| [HBASE-19549](https://issues.apache.org/jira/browse/HBASE-19549) | Change path comparison in CommonFSUtils |  Major | . |
| [HBASE-19546](https://issues.apache.org/jira/browse/HBASE-19546) | TestMasterReplication.testCyclicReplication2 uses wrong assertion |  Major | Replication, test |
| [HBASE-19522](https://issues.apache.org/jira/browse/HBASE-19522) | The complete order may be wrong in AsyncBufferedMutatorImpl |  Major | . |
| [HBASE-19530](https://issues.apache.org/jira/browse/HBASE-19530) | New regions should always be added with state CLOSED |  Major | . |
| [HBASE-19509](https://issues.apache.org/jira/browse/HBASE-19509) | RSGroupAdminEndpoint#preCreateTable triggers TableNotFoundException |  Minor | . |
| [HBASE-18352](https://issues.apache.org/jira/browse/HBASE-18352) | Enable TestMasterOperationsForRegionReplicas#testCreateTableWithMultipleReplicas disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-18838](https://issues.apache.org/jira/browse/HBASE-18838) | shaded artifacts are incorrect when built against hadoop 3 |  Critical | Client |
| [HBASE-18946](https://issues.apache.org/jira/browse/HBASE-18946) | Stochastic load balancer assigns replica regions to the same RS |  Major | . |
| [HBASE-19516](https://issues.apache.org/jira/browse/HBASE-19516) | IntegrationTestBulkLoad and IntegrationTestImportTsv run into 'java.lang.RuntimeException: DistributedHBaseCluster@1bb564e2 not an instance of MiniHBaseCluster' |  Major | . |
| [HBASE-19513](https://issues.apache.org/jira/browse/HBASE-19513) | Fix the wrapped AsyncFSOutput implementation |  Major | wal |
| [HBASE-19511](https://issues.apache.org/jira/browse/HBASE-19511) | Splits causes blocks to be cached again and so such blocks cannot be evicted from bucket cache |  Critical | BucketCache |
| [HBASE-19510](https://issues.apache.org/jira/browse/HBASE-19510) | TestDistributedLogSplitting is flakey for AsyncFSWAL |  Critical | Recovery, test |
| [HBASE-19287](https://issues.apache.org/jira/browse/HBASE-19287) | master hangs forever if RecoverMeta send assign meta region request to target server fail |  Major | proc-v2 |
| [HBASE-19503](https://issues.apache.org/jira/browse/HBASE-19503) | Fix TestWALOpenAfterDNRollingStart for AsyncFSWAL |  Major | Replication, wal |
| [HBASE-19434](https://issues.apache.org/jira/browse/HBASE-19434) | create\_namespace command for existing namespace does not throw useful error message |  Minor | . |
| [HBASE-19484](https://issues.apache.org/jira/browse/HBASE-19484) | The value array written by ExtendedCell#write is out of bounds |  Blocker | . |
| [HBASE-19508](https://issues.apache.org/jira/browse/HBASE-19508) | ReadOnlyConfiguration throws exception if any Configuration in current context calls addDefautlResource |  Major | conf |
| [HBASE-19456](https://issues.apache.org/jira/browse/HBASE-19456) | RegionMover's region server hostname option is no longer case insensitive |  Major | tooling |
| [HBASE-19495](https://issues.apache.org/jira/browse/HBASE-19495) | Fix failed ut TestShell |  Major | . |
| [HBASE-19394](https://issues.apache.org/jira/browse/HBASE-19394) | Support multi-homing env for the publication of RS status with multicast (hbase.status.published) |  Major | Client, master |
| [HBASE-19493](https://issues.apache.org/jira/browse/HBASE-19493) | Make TestWALMonotonicallyIncreasingSeqId also work with AsyncFSWAL |  Major | test |
| [HBASE-19371](https://issues.apache.org/jira/browse/HBASE-19371) | Running WALPerformanceEvaluation against asyncfswal throws exceptions |  Major | . |
| [HBASE-19134](https://issues.apache.org/jira/browse/HBASE-19134) | Make WALKey an Interface; expose Read-Only version to CPs |  Major | Coprocessors, wal |
| [HBASE-19461](https://issues.apache.org/jira/browse/HBASE-19461) | TestRSGroups is broke |  Major | test |
| [HBASE-19433](https://issues.apache.org/jira/browse/HBASE-19433) | ChangeSplitPolicyAction modifies an immutable HTableDescriptor |  Critical | integration tests |
| [HBASE-19454](https://issues.apache.org/jira/browse/HBASE-19454) | Debugging TestDistributedLogSplitting#testThreeRSAbort |  Major | . |
| [HBASE-19349](https://issues.apache.org/jira/browse/HBASE-19349) | Introduce wrong version depencency of servlet-api jar |  Critical | . |
| [HBASE-19435](https://issues.apache.org/jira/browse/HBASE-19435) | Reopen Files for ClosedChannelException in BucketCache |  Major | BucketCache |
| [HBASE-12444](https://issues.apache.org/jira/browse/HBASE-12444) | Total number of requests overflow because it's int |  Minor | hbck, master, regionserver |
| [HBASE-19447](https://issues.apache.org/jira/browse/HBASE-19447) | INFO level logging of GetClusterStatus from HMaster is too chatty |  Major | . |
| [HBASE-19417](https://issues.apache.org/jira/browse/HBASE-19417) | Remove boolean return value from postBulkLoadHFile hook |  Major | . |
| [HBASE-19445](https://issues.apache.org/jira/browse/HBASE-19445) | PerformanceEvaluation NPE processing split policy option |  Trivial | . |
| [HBASE-19023](https://issues.apache.org/jira/browse/HBASE-19023) | Usage for rowcounter in refguide is out of sync with code |  Major | . |
| [HBASE-19422](https://issues.apache.org/jira/browse/HBASE-19422) | using hadoop-profile property leads to confusing failures |  Major | . |
| [HBASE-19384](https://issues.apache.org/jira/browse/HBASE-19384) | Results returned by preAppend hook in a coprocessor are replaced with null from other coprocessor even on bypass |  Critical | Coprocessors |
| [HBASE-19431](https://issues.apache.org/jira/browse/HBASE-19431) | The tag array written by IndividualBytesFieldCell#write is out of bounds |  Critical | . |
| [HBASE-18942](https://issues.apache.org/jira/browse/HBASE-18942) | hbase-hadoop2-compat module ignores hadoop-3 profile |  Major | . |
| [HBASE-19056](https://issues.apache.org/jira/browse/HBASE-19056) |  TestCompactionInDeadRegionServer is top of the flakies charts! |  Major | test |
| [HBASE-19339](https://issues.apache.org/jira/browse/HBASE-19339) | Eager policy results in the negative size of memstore |  Critical | . |
| [HBASE-16239](https://issues.apache.org/jira/browse/HBASE-16239) | Better logging for RPC related exceptions |  Major | . |
| [HBASE-19396](https://issues.apache.org/jira/browse/HBASE-19396) | Fix flaky test TestHTableMultiplexerFlushCache |  Minor | test |
| [HBASE-19406](https://issues.apache.org/jira/browse/HBASE-19406) | Fix CompactionRequest equals and hashCode |  Major | . |
| [HBASE-19285](https://issues.apache.org/jira/browse/HBASE-19285) | Add per-table latency histograms |  Critical | metrics |
| [HBASE-19390](https://issues.apache.org/jira/browse/HBASE-19390) | Revert to older version of Jetty 9.3 |  Major | . |
| [HBASE-19385](https://issues.apache.org/jira/browse/HBASE-19385) | [1.3] TestReplicator failed 1.3 nightly |  Major | test |
| [HBASE-19350](https://issues.apache.org/jira/browse/HBASE-19350) | TestMetaWithReplicas is flaky |  Major | . |
| [HBASE-19388](https://issues.apache.org/jira/browse/HBASE-19388) | Incorrect value is being set for Compaction Pressure in RegionLoadStats object inside HRegion class |  Minor | regionserver |
| [HBASE-19386](https://issues.apache.org/jira/browse/HBASE-19386) | HBase UnsafeAvailChecker returns false on Arm64 |  Minor | . |
| [HBASE-19363](https://issues.apache.org/jira/browse/HBASE-19363) | Tests under TestCheckAndMutate are identical |  Minor | . |
| [HBASE-19035](https://issues.apache.org/jira/browse/HBASE-19035) | Miss metrics when coprocessor use region scanner to read data |  Major | . |
| [HBASE-19351](https://issues.apache.org/jira/browse/HBASE-19351) | Deprecated is missing in Table implementations |  Minor | . |
| [HBASE-19355](https://issues.apache.org/jira/browse/HBASE-19355) | Missing dependency on hbase-zookeeper module causes CopyTable to fail |  Major | . |
| [HBASE-19335](https://issues.apache.org/jira/browse/HBASE-19335) | Fix waitUntilAllRegionsAssigned |  Major | . |
| [HBASE-19318](https://issues.apache.org/jira/browse/HBASE-19318) | MasterRpcServices#getSecurityCapabilities explicitly checks for the HBase AccessController implementation |  Critical | master, security |
| [HBASE-19319](https://issues.apache.org/jira/browse/HBASE-19319) | Fix bug in synchronizing over ProcedureEvent |  Major | . |
| [HBASE-19325](https://issues.apache.org/jira/browse/HBASE-19325) | Pass a list of server name to postClearDeadServers |  Major | . |
| [HBASE-19337](https://issues.apache.org/jira/browse/HBASE-19337) | AsyncMetaTableAccessor may hang when call ScanController.terminate many times |  Major | . |
| [HBASE-19332](https://issues.apache.org/jira/browse/HBASE-19332) | DumpReplicationQueues misreports total WAL size |  Trivial | Replication |
| [HBASE-19310](https://issues.apache.org/jira/browse/HBASE-19310) | Verify IntegrationTests don't rely on Rules outside of JUnit context |  Critical | integration tests |
| [HBASE-19317](https://issues.apache.org/jira/browse/HBASE-19317) | Increase "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage" to avoid host-related failures on MiniMRCluster |  Major | integration tests, test |
| [HBASE-19330](https://issues.apache.org/jira/browse/HBASE-19330) | Remove duplicated dependency from hbase-rest |  Trivial | dependencies |
| [HBASE-19321](https://issues.apache.org/jira/browse/HBASE-19321) | ZKAsyncRegistry ctor would hang when zookeeper cluster is not available |  Major | . |
| [HBASE-19315](https://issues.apache.org/jira/browse/HBASE-19315) | Incorrect snapshot version is used for 2.0.0-beta-1 |  Minor | . |
| [HBASE-19304](https://issues.apache.org/jira/browse/HBASE-19304) | KEEP\_DELETED\_CELLS should ignore case |  Blocker | regionserver |
| [HBASE-19260](https://issues.apache.org/jira/browse/HBASE-19260) | Add lock back to avoid parallel accessing meta to locate region |  Major | . |
| [HBASE-19181](https://issues.apache.org/jira/browse/HBASE-19181) | LogRollBackupSubprocedure will fail if we use AsyncFSWAL instead of FSHLog |  Major | backup&restore |
| [HBASE-18356](https://issues.apache.org/jira/browse/HBASE-18356) | Enable TestFavoredStochasticBalancerPickers#testPickers that was disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-18357](https://issues.apache.org/jira/browse/HBASE-18357) | Enable disabled tests in TestHCM that were disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-19223](https://issues.apache.org/jira/browse/HBASE-19223) | Remove references to Date Tiered compaction from branch-1.2 and branch-1.1 ref guide |  Critical | Compaction, documentation |
| [HBASE-19255](https://issues.apache.org/jira/browse/HBASE-19255) | PerformanceEvaluation class not found when run PE test |  Major | . |
| [HBASE-19245](https://issues.apache.org/jira/browse/HBASE-19245) | MultiTableInputFormatBase#getSplits creates a Connection per Table |  Minor | mapreduce |
| [HBASE-12091](https://issues.apache.org/jira/browse/HBASE-12091) | Optionally ignore edits for dropped tables for replication. |  Major | . |
| [HBASE-19210](https://issues.apache.org/jira/browse/HBASE-19210) | TestNamespacesInstanceResource fails |  Major | . |
| [HBASE-19249](https://issues.apache.org/jira/browse/HBASE-19249) | test for "hbase antipatterns" should check \_count\_ of occurance rather than text of |  Critical | build |
| [HBASE-19240](https://issues.apache.org/jira/browse/HBASE-19240) | Fix error-prone errors, part four? |  Major | . |
| [HBASE-19215](https://issues.apache.org/jira/browse/HBASE-19215) | Incorrect exception handling on the client causes incorrect call timeouts and byte buffer allocations on the server |  Major | rpc |
| [HBASE-19250](https://issues.apache.org/jira/browse/HBASE-19250) | TestClientClusterStatus is flaky |  Trivial | . |
| [HBASE-19246](https://issues.apache.org/jira/browse/HBASE-19246) | Trivial fix in findHangingTests.py |  Trivial | . |
| [HBASE-19244](https://issues.apache.org/jira/browse/HBASE-19244) | Fix simple typos in HBASE-15518 descriptions |  Trivial | . |
| [HBASE-19089](https://issues.apache.org/jira/browse/HBASE-19089) | Fix the list of included moduleSets in src and binary tars |  Major | build |
| [HBASE-19229](https://issues.apache.org/jira/browse/HBASE-19229) | Nightly script to check source artifact should not do a destructive git operation without opt-in |  Critical | build |
| [HBASE-19199](https://issues.apache.org/jira/browse/HBASE-19199) | RatioBasedCompactionPolicy#shouldPerformMajorCompaction() always return true when only one file needs to compact |  Major | . |
| [HBASE-19195](https://issues.apache.org/jira/browse/HBASE-19195) | More error-prone fixes |  Major | . |
| [HBASE-19165](https://issues.apache.org/jira/browse/HBASE-19165) | TODO Handle stuck in transition: rit=OPENING, location=ve0538.... |  Critical | migration |
| [HBASE-19184](https://issues.apache.org/jira/browse/HBASE-19184) | clean up nightly source artifact test to match expectations from switch to git-archive |  Critical | build |
| [HBASE-19211](https://issues.apache.org/jira/browse/HBASE-19211) | B&R: update configuration string in BackupRestoreConstants |  Minor | . |
| [HBASE-19194](https://issues.apache.org/jira/browse/HBASE-19194) | TestRSGroupsBase has some always false checks |  Blocker | rsgroup, test |
| [HBASE-19088](https://issues.apache.org/jira/browse/HBASE-19088) | move\_tables\_rsgroup will throw an exception when the table is disabled |  Major | rsgroup |
| [HBASE-19198](https://issues.apache.org/jira/browse/HBASE-19198) | TestIPv6NIOServerSocketChannel fails; unable to bind |  Minor | test |
| [HBASE-19102](https://issues.apache.org/jira/browse/HBASE-19102) | TestZooKeeperMainServer fails with KeeperException$ConnectionLossException |  Major | . |
| [HBASE-18844](https://issues.apache.org/jira/browse/HBASE-18844) | Release hbase-2.0.0-alpha-4; Theme "Coprocessor API Cleanup" |  Major | . |
| [HBASE-19111](https://issues.apache.org/jira/browse/HBASE-19111) | Add missing CellUtil#isPut(Cell) methods |  Critical | Client |
| [HBASE-19160](https://issues.apache.org/jira/browse/HBASE-19160) | Re-expose CellComparator |  Critical | . |
| [HBASE-19185](https://issues.apache.org/jira/browse/HBASE-19185) | ClassNotFoundException: com.fasterxml.jackson.\* |  Critical | mapreduce |
| [HBASE-18983](https://issues.apache.org/jira/browse/HBASE-18983) | Upgrade to latest error-prone |  Major | build |
| [HBASE-19178](https://issues.apache.org/jira/browse/HBASE-19178) | table.rb use undefined method 'getType' for Cell interface |  Trivial | . |
| [HBASE-19117](https://issues.apache.org/jira/browse/HBASE-19117) | Avoid NPE occurring while active master dies |  Minor | . |
| [HBASE-19173](https://issues.apache.org/jira/browse/HBASE-19173) | Configure IntegrationTestRSGroup automatically for minicluster mode |  Minor | rsgroup, test |
| [HBASE-19144](https://issues.apache.org/jira/browse/HBASE-19144) | [RSgroups] Retry assignments in FAILED\_OPEN state when servers (re)join the cluster |  Major | rsgroup |
| [HBASE-19124](https://issues.apache.org/jira/browse/HBASE-19124) | Move HBase-Nightly source artifact creation test from JenkinsFile to a script in dev-support |  Major | test |
| [HBASE-19156](https://issues.apache.org/jira/browse/HBASE-19156) | Duplicative regions\_per\_server options on LoadTestTool |  Trivial | test |
| [HBASE-19118](https://issues.apache.org/jira/browse/HBASE-19118) | Use SaslUtil to set Sasl.QOP in 'Thrift' |  Major | Thrift |
| [HBASE-19150](https://issues.apache.org/jira/browse/HBASE-19150) | TestSnapshotWithAcl is flaky |  Minor | . |
| [HBASE-19120](https://issues.apache.org/jira/browse/HBASE-19120) | IllegalArgumentException from ZNodeClearer when master shuts down |  Major | . |
| [HBASE-19065](https://issues.apache.org/jira/browse/HBASE-19065) | HRegion#bulkLoadHFiles() should wait for concurrent Region#flush() to finish |  Major | . |
| [HBASE-19100](https://issues.apache.org/jira/browse/HBASE-19100) | Missing break in catch block of InterruptedException in HRegion#waitForFlushesAndCompactions |  Major | . |
| [HBASE-19137](https://issues.apache.org/jira/browse/HBASE-19137) | Nightly test should make junit reports optional rather than attempt archive after reporting. |  Critical | build |
| [HBASE-19135](https://issues.apache.org/jira/browse/HBASE-19135) | TestWeakObjectPool time out |  Major | . |
| [HBASE-19138](https://issues.apache.org/jira/browse/HBASE-19138) | Rare failure in TestLruBlockCache |  Trivial | test |
| [HBASE-19030](https://issues.apache.org/jira/browse/HBASE-19030) | nightly runs should attempt to log test results after archiving |  Critical | test |
| [HBASE-19130](https://issues.apache.org/jira/browse/HBASE-19130) | Typo in HStore.initializeRegionInternals for replaying wal |  Critical | Recovery |
| [HBASE-19087](https://issues.apache.org/jira/browse/HBASE-19087) | Log an Optional's value/null instead of Optional[value]/Optional.empty. |  Major | . |
| [HBASE-19129](https://issues.apache.org/jira/browse/HBASE-19129) | TestChoreService is flaky (branch-1 / branch-1.4) |  Trivial | . |
| [HBASE-19119](https://issues.apache.org/jira/browse/HBASE-19119) | hbase-http shouldn't have a native profile |  Blocker | build |
| [HBASE-18438](https://issues.apache.org/jira/browse/HBASE-18438) | Precommit doesn't warn about unused imports |  Critical | build |
| [HBASE-18922](https://issues.apache.org/jira/browse/HBASE-18922) | Fix all dead links in our HBase book |  Major | documentation |
| [HBASE-19094](https://issues.apache.org/jira/browse/HBASE-19094) | NPE in RSGroupStartupWorker.waitForGroupTableOnline during master startup |  Minor | . |
| [HBASE-13346](https://issues.apache.org/jira/browse/HBASE-13346) | Clean up Filter package for post 1.0 s/KeyValue/Cell/g |  Critical | API, Filters |
| [HBASE-19098](https://issues.apache.org/jira/browse/HBASE-19098) | Python based compatiblity checker fails if git repo does not have a remote named 'origin' |  Critical | tooling |
| [HBASE-19077](https://issues.apache.org/jira/browse/HBASE-19077) | Have Region\*CoprocessorEnvironment provide an ImmutableOnlineRegions |  Critical | Coprocessors |
| [HBASE-19073](https://issues.apache.org/jira/browse/HBASE-19073) | Cleanup CoordinatedStateManager |  Major | . |
| [HBASE-19054](https://issues.apache.org/jira/browse/HBASE-19054) | Switch precommit docker image to one based on maven images |  Major | build, community |
| [HBASE-19018](https://issues.apache.org/jira/browse/HBASE-19018) | Use of hadoop internals that require bouncycastle should declare bouncycastle dependency |  Critical | dependencies, test |
| [HBASE-19021](https://issues.apache.org/jira/browse/HBASE-19021) | Restore a few important missing logics for balancer in 2.0 |  Critical | . |
| [HBASE-19066](https://issues.apache.org/jira/browse/HBASE-19066) | Correct the directory of openjdk-8 for jenkins |  Major | build |
| [HBASE-16290](https://issues.apache.org/jira/browse/HBASE-16290) | Dump summary of callQueue content; can help debugging |  Major | Operability |
| [HBASE-18846](https://issues.apache.org/jira/browse/HBASE-18846) | Accommodate the hbase-indexer/lily/SEP consumer deploy-type |  Major | . |
| [HBASE-19072](https://issues.apache.org/jira/browse/HBASE-19072) | Missing break in catch block of InterruptedException in HRegion#waitForFlushes() |  Major | . |
| [HBASE-19058](https://issues.apache.org/jira/browse/HBASE-19058) | The wget isn't installed in building docker image |  Major | build |
| [HBASE-19039](https://issues.apache.org/jira/browse/HBASE-19039) | refactor shadedjars test to only run on java changes. |  Major | build |
| [HBASE-19060](https://issues.apache.org/jira/browse/HBASE-19060) | "Hadoop check" test is running all the time instead of just when changes to java |  Critical | build |
| [HBASE-19061](https://issues.apache.org/jira/browse/HBASE-19061) | enforcer NPE on hbase-shaded-invariants |  Blocker | build |
| [HBASE-19014](https://issues.apache.org/jira/browse/HBASE-19014) | surefire fails; When writing xml report stdout/stderr ... No such file or directory |  Major | . |
| [HBASE-19042](https://issues.apache.org/jira/browse/HBASE-19042) | Oracle Java 8u144 downloader broken in precommit check |  Blocker | build |
| [HBASE-19020](https://issues.apache.org/jira/browse/HBASE-19020) | TestXmlParsing exception checking relies on a particular xml implementation without declaring it. |  Major | dependencies, REST |
| [HBASE-19038](https://issues.apache.org/jira/browse/HBASE-19038) | precommit mvn install should run from root on patch |  Major | build |
| [HBASE-19032](https://issues.apache.org/jira/browse/HBASE-19032) | Set Content-Type header for patches uploaded by submit-patch.py |  Major | . |
| [HBASE-18350](https://issues.apache.org/jira/browse/HBASE-18350) | RSGroups are broken under AMv2 |  Blocker | rsgroup |
| [HBASE-18990](https://issues.apache.org/jira/browse/HBASE-18990) | ServerLoad doesn't override #equals which leads to #equals in ClusterStatus always false |  Trivial | . |
| [HBASE-19017](https://issues.apache.org/jira/browse/HBASE-19017) | [AMv2] EnableTableProcedure is not retaining the assignments |  Major | Region Assignment |
| [HBASE-19016](https://issues.apache.org/jira/browse/HBASE-19016) | Coordinate storage policy property name for table schema and bulkload |  Minor | . |
| [HBASE-18997](https://issues.apache.org/jira/browse/HBASE-18997) | Remove the redundant methods in RegionInfo |  Major | . |
| [HBASE-18355](https://issues.apache.org/jira/browse/HBASE-18355) | Enable export snapshot tests that were disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-18505](https://issues.apache.org/jira/browse/HBASE-18505) | Our build/yetus personality will run tests on individual modules and then on all (i.e. 'root'). Should do one or other |  Critical | build |
| [HBASE-18998](https://issues.apache.org/jira/browse/HBASE-18998) | processor.getRowsToLock() always assumes there is some row being locked |  Major | . |
| [HBASE-18992](https://issues.apache.org/jira/browse/HBASE-18992) | Comparators passed to the Memstore's flattened segments seems to be wrong |  Major | . |
| [HBASE-17590](https://issues.apache.org/jira/browse/HBASE-17590) | Drop cache hint should work for StoreFile write path |  Major | . |
| [HBASE-18975](https://issues.apache.org/jira/browse/HBASE-18975) | Fix backup / restore hadoop3 incompatibility |  Blocker | . |
| [HBASE-18904](https://issues.apache.org/jira/browse/HBASE-18904) | Missing break in NEXT\_ROW case of FilterList#mergeReturnCodeForOrOperator() |  Minor | . |
| [HBASE-18934](https://issues.apache.org/jira/browse/HBASE-18934) | precommit on branch-1 isn't supposed to run against hadoop 3 |  Critical | test |
| [HBASE-18973](https://issues.apache.org/jira/browse/HBASE-18973) | clean up maven warnings about pom well-formedness for master/branch-2 |  Minor | build |
| [HBASE-18958](https://issues.apache.org/jira/browse/HBASE-18958) | Remove the IS annotation from SpaceLimitingException |  Trivial | . |
| [HBASE-18921](https://issues.apache.org/jira/browse/HBASE-18921) | Result.current() throws ArrayIndexOutOfBoundsException after calling advance() |  Minor | . |
| [HBASE-18940](https://issues.apache.org/jira/browse/HBASE-18940) | branch-2 (and probably others) fail check of generated source artifact |  Critical | build |
| [HBASE-18874](https://issues.apache.org/jira/browse/HBASE-18874) | HMaster abort message will be skipped if Throwable is passed null |  Minor | . |
| [HBASE-18941](https://issues.apache.org/jira/browse/HBASE-18941) | Confusing logging error around rerun of restore on an existing table. |  Minor | . |
| [HBASE-18932](https://issues.apache.org/jira/browse/HBASE-18932) | Backups masking exception in a scenario and though it fails , it shows success message. |  Major | . |
| [HBASE-18606](https://issues.apache.org/jira/browse/HBASE-18606) | Tests in hbase-spark module fail with UnsatisfiedLinkError |  Critical | spark, test |
| [HBASE-18928](https://issues.apache.org/jira/browse/HBASE-18928) | Backup delete command shows wrong number of deletes requested |  Minor | . |
| [HBASE-18490](https://issues.apache.org/jira/browse/HBASE-18490) | Modifying a table descriptor to enable replicas does not create replica regions |  Major | Region Assignment |
| [HBASE-18913](https://issues.apache.org/jira/browse/HBASE-18913) | TestShell fails because NoMethodError: undefined method parseColumn |  Major | shell |
| [HBASE-18894](https://issues.apache.org/jira/browse/HBASE-18894) | null pointer exception in list\_regions in shell command |  Major | . |
| [HBASE-17441](https://issues.apache.org/jira/browse/HBASE-17441) | precommit test "hadoopcheck" not properly testing Hadoop 3 profile |  Blocker | build |
| [HBASE-18845](https://issues.apache.org/jira/browse/HBASE-18845) | TestReplicationSmallTests fails after HBASE-14004 |  Major | Replication |
| [HBASE-18887](https://issues.apache.org/jira/browse/HBASE-18887) | After full backup passed on hdfs root and incremental failed, full backup cannot be cleaned |  Major | . |
| [HBASE-18888](https://issues.apache.org/jira/browse/HBASE-18888) | StealJobQueue should call super() to init the PriorityBlockingQueue |  Major | . |
| [HBASE-18885](https://issues.apache.org/jira/browse/HBASE-18885) | HFileOutputFormat2 hardcodes default FileOutputCommitter |  Major | mapreduce |
| [HBASE-18880](https://issues.apache.org/jira/browse/HBASE-18880) | Failed to start rest server if the value of hbase.rest.threads.max is too small. |  Critical | REST |
| [HBASE-18762](https://issues.apache.org/jira/browse/HBASE-18762) | Canary sink type cast error |  Major | . |
| [HBASE-18830](https://issues.apache.org/jira/browse/HBASE-18830) | TestCanaryTool does not check Canary monitor's error code |  Major | . |
| [HBASE-18876](https://issues.apache.org/jira/browse/HBASE-18876) | Backup create command fails to take queue parameter as option |  Major | . |
| [HBASE-18796](https://issues.apache.org/jira/browse/HBASE-18796) | Admin#isTableAvailable returns incorrect result before daughter regions are opened |  Major | . |
| [HBASE-18866](https://issues.apache.org/jira/browse/HBASE-18866) | clean up warnings about proto syntax |  Minor | Protobufs |
| [HBASE-18787](https://issues.apache.org/jira/browse/HBASE-18787) | Fix the "dependencies connecting to an HBase cluster" |  Minor | documentation |
| [HBASE-18853](https://issues.apache.org/jira/browse/HBASE-18853) | hbase-protocol-shaded includes protobuf (since we moved to hbase-thirdparty) |  Major | thirdparty |
| [HBASE-18852](https://issues.apache.org/jira/browse/HBASE-18852) | Take down the hbasecon asia banner on home page |  Major | website |
| [HBASE-18851](https://issues.apache.org/jira/browse/HBASE-18851) | LICENSE failure after HADOOP-14799 with Hadoop 3 |  Critical | build |
| [HBASE-18832](https://issues.apache.org/jira/browse/HBASE-18832) | LTT fails with casting exception for HColumnDescriptor |  Major | . |
| [HBASE-18803](https://issues.apache.org/jira/browse/HBASE-18803) | Mapreduce job get failed caused by NoClassDefFoundError: org/apache/commons/lang3/ArrayUtils |  Major | . |
| [HBASE-18808](https://issues.apache.org/jira/browse/HBASE-18808) | Ineffective config check in BackupLogCleaner#getDeletableFiles() |  Major | . |
| [HBASE-18836](https://issues.apache.org/jira/browse/HBASE-18836) | Note need for explicit javax.el and exclude from shaded artifacts |  Blocker | Client |
| [HBASE-18834](https://issues.apache.org/jira/browse/HBASE-18834) | fix shellcheck warning on hbase personality |  Minor | test |
| [HBASE-18831](https://issues.apache.org/jira/browse/HBASE-18831) | Add explicit dependency on javax.el |  Major | dependencies |
| [HBASE-18641](https://issues.apache.org/jira/browse/HBASE-18641) | Include block content verification logic used in lruCache in bucketCache |  Minor | regionserver |
| [HBASE-14004](https://issues.apache.org/jira/browse/HBASE-14004) | [Replication] Inconsistency between Memstore and WAL may result in data in remote cluster that is not in the origin |  Critical | regionserver, Replication |
| [HBASE-18801](https://issues.apache.org/jira/browse/HBASE-18801) | Bulk load cleanup may falsely deem file deletion successful |  Major | . |
| [HBASE-18813](https://issues.apache.org/jira/browse/HBASE-18813) | TestCanaryTool fails on branch-1 / branch-1.4 |  Major | . |
| [HBASE-18818](https://issues.apache.org/jira/browse/HBASE-18818) | TestConnectionImplemenation fails |  Major | test |
| [HBASE-16611](https://issues.apache.org/jira/browse/HBASE-16611) | Flakey org.apache.hadoop.hbase.client.TestReplicasClient.testCancelOfMultiGet |  Major | . |
| [HBASE-18723](https://issues.apache.org/jira/browse/HBASE-18723) | [pom cleanup] Do a pass with dependency:analyze; remove unused and explicity list the dependencies we exploit |  Major | pom |
| [HBASE-18789](https://issues.apache.org/jira/browse/HBASE-18789) | Displays the reporting interval of each RS on the Master page |  Major | . |
| [HBASE-18771](https://issues.apache.org/jira/browse/HBASE-18771) | Incorrect StoreFileRefresh leading to split and compaction failures |  Blocker | . |
| [HBASE-18791](https://issues.apache.org/jira/browse/HBASE-18791) | HBASE\_HOME/lib does not contain hbase-mapreduce-${project.version}-tests.jar |  Major | . |
| [HBASE-17853](https://issues.apache.org/jira/browse/HBASE-17853) | Link to "Why does HBase care about /etc/hosts?" does not work |  Trivial | documentation |
| [HBASE-18759](https://issues.apache.org/jira/browse/HBASE-18759) | Fix hbase-shaded-check-invariants failure |  Blocker | Client, mapreduce |
| [HBASE-18765](https://issues.apache.org/jira/browse/HBASE-18765) | The value of balancerRan is true even though no plans are executed |  Minor | rsgroup |
| [HBASE-18543](https://issues.apache.org/jira/browse/HBASE-18543) | Re-enable test master.TestMasterFailover on master |  Major | amv2 |
| [HBASE-17713](https://issues.apache.org/jira/browse/HBASE-17713) | the interface '/version/cluster' with header 'Accept: application/json' return is not JSON but plain text |  Minor | REST |
| [HBASE-18757](https://issues.apache.org/jira/browse/HBASE-18757) | Fix Improper bitwise & in BucketCache offset calculation |  Major | BucketCache |
| [HBASE-18743](https://issues.apache.org/jira/browse/HBASE-18743) | HFiles in use by a table which has the same name and namespace with a default table cloned from snapshot may be deleted when that snapshot and default table are deleted |  Critical | hbase |
| [HBASE-15497](https://issues.apache.org/jira/browse/HBASE-15497) | Incorrect javadoc for atomicity guarantee of Increment and Append |  Minor | documentation |
| [HBASE-16390](https://issues.apache.org/jira/browse/HBASE-16390) | Fix documentation around setAutoFlush |  Minor | documentation |
| [HBASE-18741](https://issues.apache.org/jira/browse/HBASE-18741) | Remove cancel command from backup code |  Major | . |
| [HBASE-18461](https://issues.apache.org/jira/browse/HBASE-18461) | Build broken If the username contains a backslash |  Minor | . |
| [HBASE-18306](https://issues.apache.org/jira/browse/HBASE-18306) | Get rid of TableDescriptor#getConfiguration |  Critical | . |
| [HBASE-15947](https://issues.apache.org/jira/browse/HBASE-15947) | Classes used only for tests included in main code base |  Trivial | build, test |
| [HBASE-18714](https://issues.apache.org/jira/browse/HBASE-18714) | The dropBehind and readahead don't be applied when useHBaseChecksum is enabled |  Minor | . |
| [HBASE-14745](https://issues.apache.org/jira/browse/HBASE-14745) | Shade the last few dependencies in hbase-shaded-client |  Blocker | Client, dependencies |
| [HBASE-18665](https://issues.apache.org/jira/browse/HBASE-18665) | ReversedScannerCallable invokes getRegionLocations incorrectly |  Critical | . |
| [HBASE-18568](https://issues.apache.org/jira/browse/HBASE-18568) | Correct  metric of  numRegions |  Critical | metrics |
| [HBASE-18369](https://issues.apache.org/jira/browse/HBASE-18369) | hbase thrift web-ui not available |  Major | Thrift |
| [HBASE-18640](https://issues.apache.org/jira/browse/HBASE-18640) | Move mapreduce out of hbase-server into separate hbase-mapreduce module |  Major | . |
| [HBASE-18633](https://issues.apache.org/jira/browse/HBASE-18633) | Add more info to understand the source/scenario of large batch requests exceeding threshold |  Major | . |
| [HBASE-18635](https://issues.apache.org/jira/browse/HBASE-18635) | Fix asciidoc warnings |  Major | . |
| [HBASE-18671](https://issues.apache.org/jira/browse/HBASE-18671) | Support Append/Increment in rest api |  Major | REST |
| [HBASE-16722](https://issues.apache.org/jira/browse/HBASE-16722) | Document: Broken link in CatalogJanitor |  Trivial | documentation |
| [HBASE-18679](https://issues.apache.org/jira/browse/HBASE-18679) | YARN may null Counters object and cause an NPE in ITBLL |  Trivial | integration tests |
| [HBASE-18607](https://issues.apache.org/jira/browse/HBASE-18607) | fix submit-patch.py to support utf8 |  Trivial | . |
| [HBASE-18287](https://issues.apache.org/jira/browse/HBASE-18287) | Remove log warning in  PartitionedMobCompactor.java#getFileStatus |  Minor | mob |
| [HBASE-18647](https://issues.apache.org/jira/browse/HBASE-18647) | Parameter cacheBlocks does not take effect in REST API for scan |  Major | REST |
| [HBASE-18628](https://issues.apache.org/jira/browse/HBASE-18628) | ZKPermissionWatcher blocks all ZK notifications |  Critical | regionserver |
| [HBASE-18614](https://issues.apache.org/jira/browse/HBASE-18614) | Setting BUCKET\_CACHE\_COMBINED\_KEY to false disables stats on RS UI |  Major | regionserver |
| [HBASE-18575](https://issues.apache.org/jira/browse/HBASE-18575) | [AMv2] Enable and fix TestRestartCluster#testRetainAssignmentOnRestart on master |  Critical | amv2 |
| [HBASE-18655](https://issues.apache.org/jira/browse/HBASE-18655) | TestAsyncClusterAdminApi2 failing sometimes |  Major | . |
| [HBASE-18615](https://issues.apache.org/jira/browse/HBASE-18615) | hbase-rest tests fail in hbase-2.0.0-alpha2 |  Blocker | test |
| [HBASE-18644](https://issues.apache.org/jira/browse/HBASE-18644) | Duplicate "compactionQueueLength" metric in Region Server metrics |  Minor | metrics |
| [HBASE-16615](https://issues.apache.org/jira/browse/HBASE-16615) | Fix flaky TestScannerHeartbeatMessages |  Major | Client, Scanners |
| [HBASE-18627](https://issues.apache.org/jira/browse/HBASE-18627) | Fix TestRegionServerReadRequestMetrics |  Major | test |
| [HBASE-18634](https://issues.apache.org/jira/browse/HBASE-18634) | Fix client.TestClientClusterStatus |  Major | test |
| [HBASE-18637](https://issues.apache.org/jira/browse/HBASE-18637) | Update the link of "Bending time in HBase" |  Trivial | documentation |
| [HBASE-18471](https://issues.apache.org/jira/browse/HBASE-18471) | The DeleteFamily cell is skipped when StoreScanner seeks to next column |  Critical | Deletes, hbase, scan |
| [HBASE-18572](https://issues.apache.org/jira/browse/HBASE-18572) | Delete can't remove the cells which have no visibility label |  Critical | . |
| [HBASE-18617](https://issues.apache.org/jira/browse/HBASE-18617) | FuzzyRowKeyFilter should not modify the filter pairs |  Minor | . |
| [HBASE-18125](https://issues.apache.org/jira/browse/HBASE-18125) | HBase shell disregards spaces at the end of a split key in a split file |  Major | shell |
| [HBASE-18587](https://issues.apache.org/jira/browse/HBASE-18587) | Fix Flaky TestFileIOEngine |  Major | BucketCache, test |
| [HBASE-18431](https://issues.apache.org/jira/browse/HBASE-18431) | Mitigate compatibility concerns between branch-1.3 and branch-1.4 |  Blocker | . |
| [HBASE-18493](https://issues.apache.org/jira/browse/HBASE-18493) | [AMv2] On region server crash do not process system table regions through AssignmentManager.checkIfShouldMoveSystemRegionAsync() |  Critical | amv2 |
| [HBASE-18598](https://issues.apache.org/jira/browse/HBASE-18598) | AsyncNonMetaRegionLocator use FIFO algorithm to get a candidate locate request |  Minor | asyncclient |
| [HBASE-18437](https://issues.apache.org/jira/browse/HBASE-18437) | Revoke access permissions of a user from a table does not work as expected |  Major | security |
| [HBASE-18526](https://issues.apache.org/jira/browse/HBASE-18526) | FIFOCompactionPolicy pre-check uses wrong scope |  Major | master |
| [HBASE-18599](https://issues.apache.org/jira/browse/HBASE-18599) | Add missing @Deprecated annotations |  Minor | . |
| [HBASE-17803](https://issues.apache.org/jira/browse/HBASE-17803) | PE always re-creates table when we specify the split policy |  Minor | . |
| [HBASE-18592](https://issues.apache.org/jira/browse/HBASE-18592) | [hbase-thirdparty] Doc on new hbase-thirdparty dependency for the refguide |  Major | documentation |
| [HBASE-18557](https://issues.apache.org/jira/browse/HBASE-18557) | change splitable to mergeable in MergeTableRegionsProcedure |  Major | . |
| [HBASE-18025](https://issues.apache.org/jira/browse/HBASE-18025) | CatalogJanitor should collect outdated RegionStates from the AM |  Major | read replicas |
| [HBASE-18551](https://issues.apache.org/jira/browse/HBASE-18551) | [AMv2] UnassignProcedure and crashed regionservers |  Major | amv2 |
| [HBASE-18390](https://issues.apache.org/jira/browse/HBASE-18390) | Sleep too long when finding region location failed |  Major | Client |
| [HBASE-18563](https://issues.apache.org/jira/browse/HBASE-18563) | Fix RAT License complaint about website jenkins scripts |  Trivial | . |
| [HBASE-18024](https://issues.apache.org/jira/browse/HBASE-18024) | HRegion#initializeRegionInternals should not re-create .hregioninfo file when the region directory no longer exists |  Major | Region Assignment, regionserver |
| [HBASE-18560](https://issues.apache.org/jira/browse/HBASE-18560) | Test master.assignment.TestAssignmentManager hangs on master and its in flaky list |  Major | . |
| [HBASE-18262](https://issues.apache.org/jira/browse/HBASE-18262) | name of parameter quote need update in hbase-default.xml |  Minor | . |
| [HBASE-18525](https://issues.apache.org/jira/browse/HBASE-18525) | TestAssignmentManager#testSocketTimeout fails in master branch |  Major | . |
| [HBASE-18492](https://issues.apache.org/jira/browse/HBASE-18492) | [AMv2] Embed code for selecting highest versioned region server for system table regions in AssignmentManager.processAssignQueue() |  Major | amv2 |
| [HBASE-18516](https://issues.apache.org/jira/browse/HBASE-18516) | [AMv2] Remove dead code in ServerManager resulted mostly from AMv2 refactoring |  Major | . |
| [HBASE-18470](https://issues.apache.org/jira/browse/HBASE-18470) | Remove the redundant comma from RetriesExhaustedWithDetailsException#getDesc |  Minor | Client |
| [HBASE-17056](https://issues.apache.org/jira/browse/HBASE-17056) | Remove checked in PB generated files |  Critical | . |
| [HBASE-18480](https://issues.apache.org/jira/browse/HBASE-18480) | The cost of BaseLoadBalancer.cluster is changed even if the rollback is done |  Major | Balancer |
| [HBASE-18491](https://issues.apache.org/jira/browse/HBASE-18491) | [AMv2] Fail UnassignProcedure if source Region Server is not online. |  Critical | amv2 |
| [HBASE-18487](https://issues.apache.org/jira/browse/HBASE-18487) | Minor fixes in row lock implementation |  Major | . |
| [HBASE-18475](https://issues.apache.org/jira/browse/HBASE-18475) | MasterProcedureScheduler incorrectly passes null Procedure to table locking |  Major | proc-v2 |
| [HBASE-18259](https://issues.apache.org/jira/browse/HBASE-18259) | HBase book link to "beginner" issues includes resolved issues |  Major | documentation |
| [HBASE-18481](https://issues.apache.org/jira/browse/HBASE-18481) | The autoFlush flag was not used in PE tool |  Minor | . |
| [HBASE-18406](https://issues.apache.org/jira/browse/HBASE-18406) | In ServerCrashProcedure.java start(MasterProcedureEnv) is a no-op |  Major | . |
| [HBASE-18473](https://issues.apache.org/jira/browse/HBASE-18473) | VC.listLabels() erroneously closes any connection |  Major | Client |
| [HBASE-17131](https://issues.apache.org/jira/browse/HBASE-17131) | Avoid livelock caused by HRegion#processRowsWithLocks |  Major | regionserver |
| [HBASE-18185](https://issues.apache.org/jira/browse/HBASE-18185) | IntegrationTestTimeBoundedRequestsWithRegionReplicas unbalanced tests fails with AssertionError |  Minor | integration tests |
| [HBASE-18362](https://issues.apache.org/jira/browse/HBASE-18362) | hbck should not report split replica parent region from meta as errors |  Minor | hbck |
| [HBASE-17839](https://issues.apache.org/jira/browse/HBASE-17839) | "Data Model" section: Table 1 has only 5 data rows instead 6. |  Trivial | documentation |
| [HBASE-18445](https://issues.apache.org/jira/browse/HBASE-18445) | Upgrading Guava broke hadoop-3.0 profile |  Blocker | build, dependencies, hadoop3 |
| [HBASE-18449](https://issues.apache.org/jira/browse/HBASE-18449) | Fix client.locking.TestEntityLocks#testHeartbeatException |  Minor | test |
| [HBASE-18447](https://issues.apache.org/jira/browse/HBASE-18447) | MetricRegistryInfo#hashCode uses hashCode instead of toHashCode |  Minor | . |
| [HBASE-18441](https://issues.apache.org/jira/browse/HBASE-18441) | ZookeeperWatcher#interruptedException should throw exception |  Major | . |
| [HBASE-18054](https://issues.apache.org/jira/browse/HBASE-18054) | log when we add/remove failed servers in client |  Major | Client, Operability |
| [HBASE-18427](https://issues.apache.org/jira/browse/HBASE-18427) | minor cleanup around AssignmentManager |  Minor | amv2 |
| [HBASE-18323](https://issues.apache.org/jira/browse/HBASE-18323) | Remove multiple ACLs for the same user in kerberos |  Minor | . |
| [HBASE-18354](https://issues.apache.org/jira/browse/HBASE-18354) | Fix TestMasterMetrics that were disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-18433](https://issues.apache.org/jira/browse/HBASE-18433) | Convenience method for creating simple ColumnFamilyDescriptor |  Major | Client |
| [HBASE-18430](https://issues.apache.org/jira/browse/HBASE-18430) | Typo in "contributing to documentation" page |  Major | documentation |
| [HBASE-18404](https://issues.apache.org/jira/browse/HBASE-18404) | Small typo on ACID documentation page |  Trivial | documentation |
| [HBASE-16993](https://issues.apache.org/jira/browse/HBASE-16993) | BucketCache throw java.io.IOException: Invalid HFile block magic when configuring hbase.bucketcache.bucket.sizes |  Major | BucketCache |
| [HBASE-18337](https://issues.apache.org/jira/browse/HBASE-18337) | hbase-shaded-server brings in signed jars |  Major | shading |
| [HBASE-18330](https://issues.apache.org/jira/browse/HBASE-18330) | NPE in ReplicationZKLockCleanerChore |  Major | master, Replication |
| [HBASE-18393](https://issues.apache.org/jira/browse/HBASE-18393) | hbase shell non-interactive broken |  Blocker | scripts, shell |
| [HBASE-17648](https://issues.apache.org/jira/browse/HBASE-17648) | HBase Table-level synchronization fails between two secured(kerberized) clusters |  Critical | mapreduce, Operability, security, tooling |
| [HBASE-16090](https://issues.apache.org/jira/browse/HBASE-16090) | ResultScanner is not closed in SyncTable#finishRemainingHashRanges() |  Major | mapreduce, Operability, tooling |
| [HBASE-15548](https://issues.apache.org/jira/browse/HBASE-15548) | SyncTable: sourceHashDir is supposed to be optional but won't work without |  Critical | mapreduce, Operability, tooling |
| [HBASE-18377](https://issues.apache.org/jira/browse/HBASE-18377) | Error handling for FileNotFoundException should consider RemoteException in openReader() |  Major | . |
| [HBASE-18260](https://issues.apache.org/jira/browse/HBASE-18260) | Address new license dependencies from hadoop3-alpha4 |  Major | dependencies |
| [HBASE-18177](https://issues.apache.org/jira/browse/HBASE-18177) | FanOutOneBlockAsyncDFSOutputHelper fails to compile against Hadoop 3 |  Major | wal |
| [HBASE-18358](https://issues.apache.org/jira/browse/HBASE-18358) | Backport HBASE-18099 'FlushSnapshotSubprocedure should wait for concurrent Region#flush() to finish' to branch-1.3 |  Critical | snapshots |
| [HBASE-18348](https://issues.apache.org/jira/browse/HBASE-18348) | The implementation of AsyncTableRegionLocator does not follow the javadoc |  Critical | asyncclient, Client |
| [HBASE-18292](https://issues.apache.org/jira/browse/HBASE-18292) | Fix flaky test hbase.master.locking.TestLockProcedure#testLocalMasterLockRecovery() |  Major | . |
| [HBASE-17705](https://issues.apache.org/jira/browse/HBASE-17705) | Procedure execution must fail fast if procedure is not registered |  Blocker | . |
| [HBASE-18341](https://issues.apache.org/jira/browse/HBASE-18341) | Update findHangingTests.py script to match changed consoleText of trunk build |  Major | . |
| [HBASE-18335](https://issues.apache.org/jira/browse/HBASE-18335) | clean up configuration guide |  Major | documentation |
| [HBASE-18329](https://issues.apache.org/jira/browse/HBASE-18329) | update links in config guide to point to java 8 references |  Major | documentation |
| [HBASE-18312](https://issues.apache.org/jira/browse/HBASE-18312) | Ineffective handling of FileNotFoundException in FileLink$FileLinkInputStream.tryOpen() |  Major | . |
| [HBASE-17931](https://issues.apache.org/jira/browse/HBASE-17931) | Assign system tables to servers with highest version |  Blocker | Region Assignment, scan |
| [HBASE-18325](https://issues.apache.org/jira/browse/HBASE-18325) | Disable flakey TestMasterProcedureWalLease |  Major | test |
| [HBASE-16120](https://issues.apache.org/jira/browse/HBASE-16120) | Add shell test for truncate\_preserve |  Minor | . |
| [HBASE-18301](https://issues.apache.org/jira/browse/HBASE-18301) | Enable TestSimpleRegionNormalizerOnCluster#testRegionNormalizationMergeOnCluster that was disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-18310](https://issues.apache.org/jira/browse/HBASE-18310) | LoadTestTool unable to write data |  Major | util |
| [HBASE-18320](https://issues.apache.org/jira/browse/HBASE-18320) | Address maven-site-plugin upgrade steps |  Blocker | website |
| [HBASE-18311](https://issues.apache.org/jira/browse/HBASE-18311) | clean up the quickstart guide |  Major | documentation |
| [HBASE-13866](https://issues.apache.org/jira/browse/HBASE-13866) | Add endpoint coprocessor to the section hbase.coprocessor.region.classes in HBase book |  Trivial | documentation |
| [HBASE-18302](https://issues.apache.org/jira/browse/HBASE-18302) | Protobuf section in the docs needs some clean up |  Blocker | documentation |
| [HBASE-16192](https://issues.apache.org/jira/browse/HBASE-16192) | Fix the potential problems in TestAcidGuarantees |  Major | . |
| [HBASE-17982](https://issues.apache.org/jira/browse/HBASE-17982) |  Is the  word "occured" should be "occurred ? |  Trivial | hbase |
| [HBASE-18290](https://issues.apache.org/jira/browse/HBASE-18290) | Fix TestAddColumnFamilyProcedure and TestDeleteTableProcedure |  Major | test |
| [HBASE-18278](https://issues.apache.org/jira/browse/HBASE-18278) | [AMv2] Enable and fix uni test hbase.master.procedure.TestServerCrashProcedure#testRecoveryAndDoubleExecutionOnRsWithMeta |  Major | . |
| [HBASE-18244](https://issues.apache.org/jira/browse/HBASE-18244) | org.apache.hadoop.hbase.client.rsgroup.TestShellRSGroups hangs/fails |  Major | test |
| [HBASE-18230](https://issues.apache.org/jira/browse/HBASE-18230) | Generated LICENSE file includes unsubstituted Velocity variables |  Major | build |
| [HBASE-18274](https://issues.apache.org/jira/browse/HBASE-18274) | hbase autorestart will overwrite the gc log |  Major | hbase |
| [HBASE-18265](https://issues.apache.org/jira/browse/HBASE-18265) | Correct the link unuseful in regionServer's region state UI |  Trivial | UI |
| [HBASE-18263](https://issues.apache.org/jira/browse/HBASE-18263) | Resolve NPE in backup Master UI when access to procedures.jsp |  Trivial | UI |
| [HBASE-18254](https://issues.apache.org/jira/browse/HBASE-18254) | ServerCrashProcedure checks and waits for meta initialized, instead should check and wait for meta loaded |  Major | amv2 |
| [HBASE-18235](https://issues.apache.org/jira/browse/HBASE-18235) | LoadBalancer.BOGUS\_SERVER\_NAME should not have a bogus hostname |  Major | . |
| [HBASE-18212](https://issues.apache.org/jira/browse/HBASE-18212) | In Standalone mode with local filesystem HBase logs Warning message:Failed to invoke 'unbuffer' method in class class org.apache.hadoop.fs.FSDataInputStream |  Minor | Operability |
| [HBASE-17988](https://issues.apache.org/jira/browse/HBASE-17988) | get-active-master.rb and draining\_servers.rb no longer work |  Critical | scripts |
| [HBASE-18180](https://issues.apache.org/jira/browse/HBASE-18180) | Possible connection leak while closing BufferedMutator in TableOutputFormat |  Major | mapreduce |
| [HBASE-18227](https://issues.apache.org/jira/browse/HBASE-18227) | [AMv2] Fix test hbase.coprocessor.TestCoprocessorMetrics#testRegionObserverAfterRegionClosed |  Major | amv2 |
| [HBASE-18225](https://issues.apache.org/jira/browse/HBASE-18225) | Fix findbugs regression calling toString() on an array |  Trivial | . |
| [HBASE-18166](https://issues.apache.org/jira/browse/HBASE-18166) | [AMv2] We are splitting already-split files |  Major | Region Assignment |
| [HBASE-18209](https://issues.apache.org/jira/browse/HBASE-18209) | Include httpclient / httpcore jars in build artifacts |  Major | . |
| [HBASE-18219](https://issues.apache.org/jira/browse/HBASE-18219) | Fix typo in constant HConstants.HBASE\_CLIENT\_MEAT\_REPLICA\_SCAN\_TIMEOUT |  Minor | . |
| [HBASE-18200](https://issues.apache.org/jira/browse/HBASE-18200) | Set hadoop check versions for branch-2 and branch-2.x in pre commit |  Major | build |
| [HBASE-18207](https://issues.apache.org/jira/browse/HBASE-18207) | branch-2 build fails in the checkstyle phase |  Major | . |
| [HBASE-18137](https://issues.apache.org/jira/browse/HBASE-18137) | Replication gets stuck for empty WALs |  Critical | Replication |
| [HBASE-18199](https://issues.apache.org/jira/browse/HBASE-18199) | Race in NettyRpcConnection may cause call stuck in BufferCallBeforeInitHandler forever |  Major | IPC/RPC |
| [HBASE-18192](https://issues.apache.org/jira/browse/HBASE-18192) | Replication drops recovered queues on region server shutdown |  Blocker | Replication |
| [HBASE-18092](https://issues.apache.org/jira/browse/HBASE-18092) | Removing a peer does not properly clean up the ReplicationSourceManager state and metrics |  Major | Replication |
| [HBASE-18193](https://issues.apache.org/jira/browse/HBASE-18193) | Master web UI presents the incorrect number of regions |  Minor | . |
| [HBASE-18195](https://issues.apache.org/jira/browse/HBASE-18195) | Remove redundant single quote from start message for HMaster and HRegionServer |  Minor | master, regionserver |
| [HBASE-18141](https://issues.apache.org/jira/browse/HBASE-18141) | Regionserver fails to shutdown when abort triggered in RegionScannerImpl during RPC call |  Critical | regionserver, security |
| [HBASE-15302](https://issues.apache.org/jira/browse/HBASE-15302) | Reenable the other tests disabled by HBASE-14678 |  Major | test |
| [HBASE-18184](https://issues.apache.org/jira/browse/HBASE-18184) | Add hbase-hadoop2-compat jar as MapReduce job dependency |  Minor | mapreduce |
| [HBASE-18149](https://issues.apache.org/jira/browse/HBASE-18149) | The setting rules for table-scope attributes and family-scope attributes should keep consistent |  Major | shell |
| [HBASE-18158](https://issues.apache.org/jira/browse/HBASE-18158) | Two running in-memory compaction threads may lose data |  Major | . |
| [HBASE-18145](https://issues.apache.org/jira/browse/HBASE-18145) | The flush may cause the corrupt data for reading |  Blocker | . |
| [HBASE-18132](https://issues.apache.org/jira/browse/HBASE-18132) | Low replication should be checked in period in case of datanode rolling upgrade |  Major | . |
| [HBASE-9393](https://issues.apache.org/jira/browse/HBASE-9393) | Hbase does not closing a closed socket resulting in many CLOSE\_WAIT |  Critical | . |
| [HBASE-18005](https://issues.apache.org/jira/browse/HBASE-18005) | read replica: handle the case that region server hosting both primary replica and meta region is down |  Major | . |
| [HBASE-18030](https://issues.apache.org/jira/browse/HBASE-18030) | Per Cell TTL tags may get duplicated with increments/Append causing tags length overflow |  Critical | hbase, regionserver |
| [HBASE-18155](https://issues.apache.org/jira/browse/HBASE-18155) | TestMasterProcedureWalLease is flakey |  Major | amv2 |
| [HBASE-18111](https://issues.apache.org/jira/browse/HBASE-18111) | Replication stuck when cluster connection is closed |  Major | . |
| [HBASE-18143](https://issues.apache.org/jira/browse/HBASE-18143) | [AMv2] Backoff on failed report of region transition quickly goes to astronomical time scale |  Critical | Region Assignment |
| [HBASE-18129](https://issues.apache.org/jira/browse/HBASE-18129) | truncate\_preserve fails when the truncate method doesn't exists on the master |  Major | shell |
| [HBASE-18122](https://issues.apache.org/jira/browse/HBASE-18122) | Scanner id should include ServerName of region server |  Major | . |
| [HBASE-18027](https://issues.apache.org/jira/browse/HBASE-18027) | Replication should respect RPC size limits when batching edits |  Major | Replication |
| [HBASE-18042](https://issues.apache.org/jira/browse/HBASE-18042) | Client Compatibility breaks between versions 1.2 and 1.3 |  Critical | regionserver, scan |
| [HBASE-18118](https://issues.apache.org/jira/browse/HBASE-18118) | Default storage policy if not configured cannot be "NONE" |  Minor | wal |
| [HBASE-16011](https://issues.apache.org/jira/browse/HBASE-16011) | TableSnapshotScanner and TableSnapshotInputFormat can produce duplicate rows |  Major | snapshots |
| [HBASE-18120](https://issues.apache.org/jira/browse/HBASE-18120) | Fix TestAsyncRegionAdminApi |  Major | test |
| [HBASE-18113](https://issues.apache.org/jira/browse/HBASE-18113) | Handle old client without include\_stop\_row flag when startRow equals endRow |  Major | . |
| [HBASE-17997](https://issues.apache.org/jira/browse/HBASE-17997) | In dev environment, add jruby-complete jar to classpath only when jruby is needed |  Major | . |
| [HBASE-18099](https://issues.apache.org/jira/browse/HBASE-18099) | FlushSnapshotSubprocedure should wait for concurrent Region#flush() to finish |  Critical | snapshots |
| [HBASE-18084](https://issues.apache.org/jira/browse/HBASE-18084) | Improve CleanerChore to clean from directory which consumes more disk space |  Major | . |
| [HBASE-18085](https://issues.apache.org/jira/browse/HBASE-18085) | Prevent parallel purge in ObjectPool |  Major | . |
| [HBASE-18093](https://issues.apache.org/jira/browse/HBASE-18093) | Overloading the meaning of 'enabled' in Quota Manager to indicate either quota disabled or quota manager not ready is not good |  Minor | master |
| [HBASE-18077](https://issues.apache.org/jira/browse/HBASE-18077) | Update JUnit license to EPL from CPL |  Blocker | build, community |
| [HBASE-18069](https://issues.apache.org/jira/browse/HBASE-18069) | Fix flaky test TestReplicationAdminWithClusters#testDisableAndEnableReplication |  Trivial | test |
| [HBASE-15616](https://issues.apache.org/jira/browse/HBASE-15616) | Allow null qualifier for all table operations |  Major | Client |
| [HBASE-18081](https://issues.apache.org/jira/browse/HBASE-18081) | The way we process connection preamble in SimpleRpcServer is broken |  Major | IPC/RPC |
| [HBASE-18035](https://issues.apache.org/jira/browse/HBASE-18035) | Meta replica does not give any primaryOperationTimeout to primary meta region |  Critical | . |
| [HBASE-18071](https://issues.apache.org/jira/browse/HBASE-18071) | Fix flaky test TestStochasticLoadBalancer#testBalanceCluster |  Major | Balancer |
| [HBASE-18058](https://issues.apache.org/jira/browse/HBASE-18058) | Zookeeper retry sleep time should have an upper limit |  Major | . |
| [HBASE-17286](https://issues.apache.org/jira/browse/HBASE-17286) | LICENSE.txt in binary tarball contains only ASL text |  Blocker | build, community |
| [HBASE-18049](https://issues.apache.org/jira/browse/HBASE-18049) | It is not necessary to re-open the region when MOB files cannot be found |  Major | Scanners |
| [HBASE-18053](https://issues.apache.org/jira/browse/HBASE-18053) | AsyncTableResultScanner will hang when scan wrong column family |  Major | Client |
| [HBASE-18051](https://issues.apache.org/jira/browse/HBASE-18051) | balance\_rsgroup still run when the Load Balancer is not enabled. |  Major | rsgroup |
| [HBASE-17352](https://issues.apache.org/jira/browse/HBASE-17352) | Fix hbase-assembly build with bash 4 |  Minor | . |
| [HBASE-18055](https://issues.apache.org/jira/browse/HBASE-18055) | Releasing L2 cache HFileBlocks before shipped() when switching from pread to stream causes result corruption |  Major | regionserver, Scanners |
| [HBASE-18000](https://issues.apache.org/jira/browse/HBASE-18000) | Make sure we always return the scanner id with ScanResponse |  Major | regionserver |
| [HBASE-18026](https://issues.apache.org/jira/browse/HBASE-18026) | ProtobufUtil seems to do extra array copying |  Minor | . |
| [HBASE-16356](https://issues.apache.org/jira/browse/HBASE-16356) | REST API scanner: row prefix filter and custom filter parameters are mutually exclusive |  Minor | REST |
| [HBASE-8758](https://issues.apache.org/jira/browse/HBASE-8758) | Error in RegionCoprocessorHost class preScanner method documentation. |  Minor | Coprocessors, documentation |
| [HBASE-17471](https://issues.apache.org/jira/browse/HBASE-17471) | Region Seqid will be out of order in WAL if using mvccPreAssign |  Critical | wal |
| [HBASE-17964](https://issues.apache.org/jira/browse/HBASE-17964) | ensure hbase-metrics-api is included in mapreduce job classpaths |  Blocker | mapreduce |
| [HBASE-17991](https://issues.apache.org/jira/browse/HBASE-17991) | Add more details about compaction queue on /dump |  Minor | . |
| [HBASE-17228](https://issues.apache.org/jira/browse/HBASE-17228) | precommit grep -c ERROR may grab non errors |  Minor | build, community, test |
| [HBASE-17958](https://issues.apache.org/jira/browse/HBASE-17958) | Avoid passing unexpected cell to ScanQueryMatcher when optimize SEEK to SKIP |  Major | . |
| [HBASE-17985](https://issues.apache.org/jira/browse/HBASE-17985) | Inline package manage updates with package installation in Yetus Dockerfile |  Blocker | . |
| [HBASE-17957](https://issues.apache.org/jira/browse/HBASE-17957) |  Custom metrics of replicate endpoints don't prepend "source." to global metrics |  Minor | Replication |
| [HBASE-17862](https://issues.apache.org/jira/browse/HBASE-17862) | Condition that always returns true |  Trivial | Client |
| [HBASE-14286](https://issues.apache.org/jira/browse/HBASE-14286) | Correct typo in argument name for WALSplitter.writeRegionSequenceIdFile |  Trivial | . |
| [HBASE-17879](https://issues.apache.org/jira/browse/HBASE-17879) | Avoid NPE in snapshot.jsp when accessing without any request parameter |  Trivial | UI |
| [HBASE-17975](https://issues.apache.org/jira/browse/HBASE-17975) | TokenUtil.obtainToken squashes remote exceptions |  Blocker | mapreduce, security |
| [HBASE-17970](https://issues.apache.org/jira/browse/HBASE-17970) | Set yarn.app.mapreduce.am.staging-dir when starting MiniMRCluster |  Major | mapreduce, snapshots, test |
| [HBASE-17950](https://issues.apache.org/jira/browse/HBASE-17950) | Write the chunkId also as Int instead of long into the first byte of the chunk |  Major | . |
| [HBASE-17904](https://issues.apache.org/jira/browse/HBASE-17904) | Get runs into NoSuchElementException when using Read Replica, with hbase. ipc.client.specificThreadForWriting to be true and hbase.rpc.client.impl to be org.apache.hadoop.hbase.ipc.RpcClientImpl |  Major | Client, IPC/RPC |
| [HBASE-17947](https://issues.apache.org/jira/browse/HBASE-17947) | Location of Examples.proto is wrong in comment of RowCountEndPoint.java |  Trivial | Coprocessors |
| [HBASE-17302](https://issues.apache.org/jira/browse/HBASE-17302) | The region flush request disappeared from flushQueue |  Major | . |
| [HBASE-17943](https://issues.apache.org/jira/browse/HBASE-17943) | The in-memory flush size is different for each CompactingMemStore located in the same region |  Major | regionserver |
| [HBASE-17946](https://issues.apache.org/jira/browse/HBASE-17946) | Shell command compact\_rs don't work |  Major | shell |
| [HBASE-17941](https://issues.apache.org/jira/browse/HBASE-17941) | CellArrayMap#getCell may throw IndexOutOfBoundsException |  Minor | . |
| [HBASE-13288](https://issues.apache.org/jira/browse/HBASE-13288) | Fix naming of parameter in Delete constructor |  Trivial | API |
| [HBASE-17937](https://issues.apache.org/jira/browse/HBASE-17937) | Memstore size becomes negative in case of expensive postPut/Delete Coprocessor call |  Major | regionserver |
| [HBASE-17940](https://issues.apache.org/jira/browse/HBASE-17940) | HMaster can not start due to Jasper related classes conflict |  Blocker | dependencies, pom |
| [HBASE-17936](https://issues.apache.org/jira/browse/HBASE-17936) | Refine sum endpoint example in ref guide |  Minor | documentation |
| [HBASE-17930](https://issues.apache.org/jira/browse/HBASE-17930) | Avoid using Canary.sniff in HBaseTestingUtility |  Major | canary, test |
| [HBASE-16875](https://issues.apache.org/jira/browse/HBASE-16875) | Changed try-with-resources in the docs to recommended way |  Trivial | documentation |
| [HBASE-17366](https://issues.apache.org/jira/browse/HBASE-17366) | Run TestHFile#testReaderWithoutBlockCache failes |  Trivial | test |
| [HBASE-15535](https://issues.apache.org/jira/browse/HBASE-15535) | Correct link to Trafodion |  Minor | documentation |
| [HBASE-17903](https://issues.apache.org/jira/browse/HBASE-17903) | Corrected the alias for the link of HBASE-6580 |  Trivial | documentation |
| [HBASE-17816](https://issues.apache.org/jira/browse/HBASE-17816) | HRegion#mutateRowWithLocks should update writeRequestCount metric |  Major | metrics |
| [HBASE-17905](https://issues.apache.org/jira/browse/HBASE-17905) | [hbase-spark]  bulkload does not work when table not exist |  Major | . |
| [HBASE-17863](https://issues.apache.org/jira/browse/HBASE-17863) | Procedure V2:  Proc Executor cleanup. Split FINISHED state to two states: SUCCESS and FAILED. |  Major | proc-v2 |
| [HBASE-17886](https://issues.apache.org/jira/browse/HBASE-17886) | Fix compatibility of ServerSideScanMetrics |  Blocker | . |
| [HBASE-17869](https://issues.apache.org/jira/browse/HBASE-17869) | UnsafeAvailChecker wrongly returns false on ppc |  Minor | . |
| [HBASE-17871](https://issues.apache.org/jira/browse/HBASE-17871) | scan#setBatch(int) call leads wrong result of VerifyReplication |  Minor | . |
| [HBASE-15871](https://issues.apache.org/jira/browse/HBASE-15871) | Memstore flush doesn't finish because of backwardseek() in memstore scanner. |  Major | Scanners |
| [HBASE-17785](https://issues.apache.org/jira/browse/HBASE-17785) | RSGroupBasedLoadBalancer fails to assign new table regions when cloning snapshot |  Major | . |
| [HBASE-17698](https://issues.apache.org/jira/browse/HBASE-17698) | ReplicationEndpoint choosing sinks |  Major | Replication |
| [HBASE-16780](https://issues.apache.org/jira/browse/HBASE-16780) | Since move to protobuf3.1, Cells are limited to 64MB where previous they had no limit |  Critical | Protobufs |
| [HBASE-17821](https://issues.apache.org/jira/browse/HBASE-17821) | The CompoundConfiguration#toString is wrong |  Trivial | . |
| [HBASE-17660](https://issues.apache.org/jira/browse/HBASE-17660) | HFileSplitter is not being applied during full table restore |  Major | . |
| [HBASE-17287](https://issues.apache.org/jira/browse/HBASE-17287) | Master becomes a zombie if filesystem object closes |  Blocker | master |
| [HBASE-17807](https://issues.apache.org/jira/browse/HBASE-17807) | correct the value of zookeeper.session.timeout in hbase doc |  Trivial | documentation |
| [HBASE-17798](https://issues.apache.org/jira/browse/HBASE-17798) | RpcServer.Listener.Reader can abort due to CancelledKeyException |  Major | . |
| [HBASE-17812](https://issues.apache.org/jira/browse/HBASE-17812) | Remove RpcConnection from pool in AbstractRpcClient.cancelConnections |  Major | Client, rpc |
| [HBASE-16014](https://issues.apache.org/jira/browse/HBASE-16014) | Get and Put constructor argument lists are divergent |  Major | . |
| [HBASE-17582](https://issues.apache.org/jira/browse/HBASE-17582) | Drop page cache hint is broken |  Critical | Compaction, io |
| [HBASE-16084](https://issues.apache.org/jira/browse/HBASE-16084) | Clean up the stale references in javadoc |  Minor | . |
| [HBASE-17426](https://issues.apache.org/jira/browse/HBASE-17426) | Inconsistent environment variable names for enabling JMX |  Major | . |
| [HBASE-17792](https://issues.apache.org/jira/browse/HBASE-17792) | Use a shared thread pool for AtomicityWriter, AtomicGetReader, AtomicScanReader's connections in TestAcidGuarantees |  Minor | . |
| [HBASE-17723](https://issues.apache.org/jira/browse/HBASE-17723) | ClientAsyncPrefetchScanner may end prematurely when the size of the cache is one |  Major | . |
| [HBASE-17779](https://issues.apache.org/jira/browse/HBASE-17779) | disable\_table\_replication returns misleading message and does not turn off replication |  Major | Replication |
| [HBASE-17780](https://issues.apache.org/jira/browse/HBASE-17780) | BoundedByteBufferPool "At capacity" messages are not actionable |  Minor | . |
| [HBASE-17501](https://issues.apache.org/jira/browse/HBASE-17501) | NullPointerException after Datanodes Decommissioned and Terminated |  Minor | Filesystem Integration, Operability |
| [HBASE-17773](https://issues.apache.org/jira/browse/HBASE-17773) | VerifyReplication tool wrongly emits warning "ERROR: Invalid argument '--recomparesleep=xx'" |  Trivial | . |
| [HBASE-17746](https://issues.apache.org/jira/browse/HBASE-17746) | TestSimpleRpcScheduler.testCoDelScheduling is broken |  Major | integration tests |
| [HBASE-17772](https://issues.apache.org/jira/browse/HBASE-17772) | IntegrationTestRSGroup won't run |  Minor | rsgroup |
| [HBASE-17712](https://issues.apache.org/jira/browse/HBASE-17712) | Remove/Simplify the logic of RegionScannerImpl.handleFileNotFound |  Major | regionserver |
| [HBASE-17763](https://issues.apache.org/jira/browse/HBASE-17763) | IPCUtil.wrapException will wrap DoNotRetryIOException with IOException |  Major | IPC/RPC |
| [HBASE-17736](https://issues.apache.org/jira/browse/HBASE-17736) | Some options can't be configured by the shell |  Minor | . |
| [HBASE-17761](https://issues.apache.org/jira/browse/HBASE-17761) | Test TestRemoveRegionMetrics.testMoveRegion fails intermittently because of race condition |  Major | Region Assignment |
| [HBASE-17760](https://issues.apache.org/jira/browse/HBASE-17760) | HDFS Balancer doc is misleading |  Minor | documentation |
| [HBASE-17718](https://issues.apache.org/jira/browse/HBASE-17718) | Difference between RS's servername and its ephemeral node cause SSH stop working |  Major | . |
| [HBASE-17729](https://issues.apache.org/jira/browse/HBASE-17729) | Missing shortcuts for some useful HCD options |  Trivial | shell |
| [HBASE-17717](https://issues.apache.org/jira/browse/HBASE-17717) | Incorrect ZK ACL set for HBase superuser |  Critical | security, Zookeeper |
| [HBASE-17460](https://issues.apache.org/jira/browse/HBASE-17460) | enable\_table\_replication can not perform cyclic replication of a table |  Critical | Replication |
| [HBASE-16630](https://issues.apache.org/jira/browse/HBASE-16630) | Fragmentation in long running Bucket Cache |  Critical | BucketCache |
| [HBASE-17722](https://issues.apache.org/jira/browse/HBASE-17722) | Metrics subsystem stop/start messages add a lot of useless bulk to operational logging |  Trivial | metrics |
| [HBASE-17710](https://issues.apache.org/jira/browse/HBASE-17710) | HBase in standalone mode creates directories with 777 permission |  Major | regionserver |
| [HBASE-17673](https://issues.apache.org/jira/browse/HBASE-17673) | Monitored RPC Handler not shown in the WebUI |  Minor | . |
| [HBASE-17688](https://issues.apache.org/jira/browse/HBASE-17688) | MultiRowRangeFilter not working correctly if given same start and stop RowKey |  Minor | . |
| [HBASE-17699](https://issues.apache.org/jira/browse/HBASE-17699) | Fix TestLockProcedure |  Blocker | proc-v2 |
| [HBASE-17674](https://issues.apache.org/jira/browse/HBASE-17674) | Major compaction may be cancelled in CompactionChecker |  Major | Compaction |
| [HBASE-17682](https://issues.apache.org/jira/browse/HBASE-17682) | Region stuck in merging\_new state indefinitely |  Major | . |
| [HBASE-17069](https://issues.apache.org/jira/browse/HBASE-17069) | RegionServer writes invalid META entries for split daughters in some circumstances |  Blocker | wal |
| [HBASE-17677](https://issues.apache.org/jira/browse/HBASE-17677) | ServerName parsing from directory name should be more robust to errors from guava's HostAndPort |  Major | wal |
| [HBASE-13882](https://issues.apache.org/jira/browse/HBASE-13882) | Fix RegionSplitPolicy section in HBase book |  Trivial | documentation |
| [HBASE-17675](https://issues.apache.org/jira/browse/HBASE-17675) | ReplicationEndpoint should choose new sinks if a SaslException occurs |  Major | . |
| [HBASE-15328](https://issues.apache.org/jira/browse/HBASE-15328) | Unvalidated Redirect in HMaster |  Minor | security |
| [HBASE-17661](https://issues.apache.org/jira/browse/HBASE-17661) | fix the queue length passed to FastPathBalancedQueueRpcExecutor |  Minor | . |
| [HBASE-17653](https://issues.apache.org/jira/browse/HBASE-17653) | HBASE-17624 rsgroup synchronizations will (distributed) deadlock |  Major | rsgroup |
| [HBASE-17658](https://issues.apache.org/jira/browse/HBASE-17658) | Fix bookkeeping error with max regions for a table |  Major | Balancer |
| [HBASE-17421](https://issues.apache.org/jira/browse/HBASE-17421) | Update refguide w.r.t. MOB Sweeper |  Major | documentation, mob |
| [HBASE-17649](https://issues.apache.org/jira/browse/HBASE-17649) | REST API for scan should return 410 when table is disabled |  Major | . |
| [HBASE-17640](https://issues.apache.org/jira/browse/HBASE-17640) | Unittest error in TestMobCompactor with different timezone |  Minor | mob |
| [HBASE-17624](https://issues.apache.org/jira/browse/HBASE-17624) | Address late review of HBASE-6721, rsgroups feature |  Major | rsgroup |
| [HBASE-17639](https://issues.apache.org/jira/browse/HBASE-17639) | Do not stop server if ReplicationSourceManager's waitUntilCanBePushed throws InterruptedException |  Major | Replication |
| [HBASE-17558](https://issues.apache.org/jira/browse/HBASE-17558) | ZK dumping jsp should escape html |  Minor | security, UI |
| [HBASE-9702](https://issues.apache.org/jira/browse/HBASE-9702) | Change unittests that use "table" or "testtable" to use method names. |  Major | test |
| [HBASE-17611](https://issues.apache.org/jira/browse/HBASE-17611) | Thrift 2 per-call latency metrics are capped at ~ 2 seconds |  Major | metrics, Thrift |
| [HBASE-17603](https://issues.apache.org/jira/browse/HBASE-17603) | REST API for scan should return 404 when table does not exist |  Blocker | REST, scan |
| [HBASE-17638](https://issues.apache.org/jira/browse/HBASE-17638) | Remove duplicated initialization of CacheConfig in HRegionServer |  Minor | . |
| [HBASE-17622](https://issues.apache.org/jira/browse/HBASE-17622) | Add hbase-metrics package to TableMapReduceUtil |  Trivial | mapreduce |
| [HBASE-17616](https://issues.apache.org/jira/browse/HBASE-17616) | Incorrect actions performed by CM |  Major | . |
| [HBASE-17105](https://issues.apache.org/jira/browse/HBASE-17105) | Annotate RegionServerObserver |  Major | . |
| [HBASE-17381](https://issues.apache.org/jira/browse/HBASE-17381) | ReplicationSourceWorkerThread can die due to unhandled exceptions |  Major | Replication |
| [HBASE-17565](https://issues.apache.org/jira/browse/HBASE-17565) | StochasticLoadBalancer may incorrectly skip balancing due to skewed multiplier sum |  Critical | . |
| [HBASE-17606](https://issues.apache.org/jira/browse/HBASE-17606) | Fix failing TestRpcControllerFactory introduced by HBASE-17508 |  Major | Client, scan |
| [HBASE-17187](https://issues.apache.org/jira/browse/HBASE-17187) | DoNotRetryExceptions from coprocessors should bubble up to the application |  Major | . |
| [HBASE-17578](https://issues.apache.org/jira/browse/HBASE-17578) | Thrift per-method metrics should still update in the case of exceptions |  Major | Thrift |
| [HBASE-17593](https://issues.apache.org/jira/browse/HBASE-17593) | Fix build with hadoop 3 profile |  Major | . |
| [HBASE-17601](https://issues.apache.org/jira/browse/HBASE-17601) | close() in TableRecordReaderImpl assumes the split has started |  Minor | hadoop2 |
| [HBASE-17587](https://issues.apache.org/jira/browse/HBASE-17587) | Do not Rethrow DoNotRetryIOException as UnknownScannerException |  Major | Coprocessors, regionserver, rpc |
| [HBASE-17581](https://issues.apache.org/jira/browse/HBASE-17581) | mvn clean test -PskipXXXTests does not work properly for some modules |  Major | . |
| [HBASE-16621](https://issues.apache.org/jira/browse/HBASE-16621) | HBCK should have -fixHFileLinks |  Major | . |
| [HBASE-17522](https://issues.apache.org/jira/browse/HBASE-17522) | RuntimeExceptions from MemoryMXBean should not take down server process |  Major | regionserver |
| [HBASE-17197](https://issues.apache.org/jira/browse/HBASE-17197) | hfile does not work in 2.0 |  Major | HFile |
| [HBASE-17538](https://issues.apache.org/jira/browse/HBASE-17538) | HDFS.setStoragePolicy() logs errors on local fs |  Major | . |
| [HBASE-16785](https://issues.apache.org/jira/browse/HBASE-16785) | We are not running all tests |  Major | build, test |
| [HBASE-17540](https://issues.apache.org/jira/browse/HBASE-17540) | Change SASL server GSSAPI callback log line from DEBUG to TRACE in RegionServer to reduce log volumes in DEBUG mode |  Minor | regionserver |
| [HBASE-17271](https://issues.apache.org/jira/browse/HBASE-17271) | hbase-thrift QA tests only run one test |  Major | . |
| [HBASE-17407](https://issues.apache.org/jira/browse/HBASE-17407) | Correct update of maxFlushedSeqId in HRegion |  Major | wal |
| [HBASE-17489](https://issues.apache.org/jira/browse/HBASE-17489) | ClientScanner may send a next request to a RegionScanner which has been exhausted |  Critical | Client, scan |
| [HBASE-17357](https://issues.apache.org/jira/browse/HBASE-17357) | PerformanceEvaluation parameters parsing triggers NPE. |  Minor | Performance, test |
| [HBASE-17496](https://issues.apache.org/jira/browse/HBASE-17496) | RSGroup shell commands:get\_server\_rsgroup don't work and commands display an incorrect result size |  Major | shell |
| [HBASE-17486](https://issues.apache.org/jira/browse/HBASE-17486) | Tighten the contract for batch client methods |  Trivial | API |
| [HBASE-17482](https://issues.apache.org/jira/browse/HBASE-17482) | mvcc mechanism fails when using mvccPreAssign |  Critical | . |
| [HBASE-17469](https://issues.apache.org/jira/browse/HBASE-17469) | Properly handle empty TableName in TablePermission#readFields and #write |  Major | findbugs |
| [HBASE-17475](https://issues.apache.org/jira/browse/HBASE-17475) | Stack overflow in AsyncProcess if retry too much |  Major | API, Client |
| [HBASE-17464](https://issues.apache.org/jira/browse/HBASE-17464) | Fix HBaseTestingUtility.getNewDataTestDirOnTestFS to always return a unique path |  Minor | test |
| [HBASE-17450](https://issues.apache.org/jira/browse/HBASE-17450) | TablePermission#equals throws NPE after namespace support was added |  Major | findbugs |
| [HBASE-17452](https://issues.apache.org/jira/browse/HBASE-17452) | Failed taking snapshot - region Manifest proto-message too large |  Major | snapshots |
| [HBASE-17434](https://issues.apache.org/jira/browse/HBASE-17434) | New Synchronization Scheme for Compaction Pipeline |  Major | . |
| [HBASE-17445](https://issues.apache.org/jira/browse/HBASE-17445) | Count size of serialized exceptions in checking max result size quota |  Major | . |
| [HBASE-17429](https://issues.apache.org/jira/browse/HBASE-17429) | HBase bulkload cannot support HDFS viewFs |  Major | . |
| [HBASE-17435](https://issues.apache.org/jira/browse/HBASE-17435) | Call to preCommitStoreFile() hook encounters SaslException in secure deployment |  Major | . |
| [HBASE-17430](https://issues.apache.org/jira/browse/HBASE-17430) | dead links in ref guide to class javadocs that moved out of user APIs. |  Major | documentation, website |
| [HBASE-17424](https://issues.apache.org/jira/browse/HBASE-17424) | Protect REST client against malicious XML responses. |  Major | REST |
| [HBASE-17351](https://issues.apache.org/jira/browse/HBASE-17351) | Enforcer plugin fails with NullPointerException |  Critical | build |
| [HBASE-17290](https://issues.apache.org/jira/browse/HBASE-17290) | Potential loss of data for replication of bulk loaded hfiles |  Major | . |
| [HBASE-17390](https://issues.apache.org/jira/browse/HBASE-17390) | Online update of configuration for all servers leaves out masters |  Major | . |
| [HBASE-17403](https://issues.apache.org/jira/browse/HBASE-17403) | ClientAsyncPrefetchScanner doesn’t load any data if the MaxResultSize is too small |  Major | . |
| [HBASE-17387](https://issues.apache.org/jira/browse/HBASE-17387) | Reduce the overhead of exception report in RegionActionResult for multi() |  Minor | . |
| [HBASE-17385](https://issues.apache.org/jira/browse/HBASE-17385) | Change usage documentation from bin/hbase to hbase in various tools |  Major | . |
| [HBASE-17374](https://issues.apache.org/jira/browse/HBASE-17374) | ZKPermissionWatcher crashed when grant after region close |  Critical | regionserver |
| [HBASE-17376](https://issues.apache.org/jira/browse/HBASE-17376) | ClientAsyncPrefetchScanner may fail due to too many rows |  Major | . |
| [HBASE-17330](https://issues.apache.org/jira/browse/HBASE-17330) | SnapshotFileCache will always refresh the file cache |  Minor | snapshots |
| [HBASE-16663](https://issues.apache.org/jira/browse/HBASE-16663) | JMX ConnectorServer stopped when unauthorized user try to stop HM/RS/cluster |  Critical | metrics, security |
| [HBASE-17341](https://issues.apache.org/jira/browse/HBASE-17341) | Add a timeout during replication endpoint termination |  Critical | . |
| [HBASE-17328](https://issues.apache.org/jira/browse/HBASE-17328) | Properly dispose of looped replication peers |  Critical | Replication |
| [HBASE-17347](https://issues.apache.org/jira/browse/HBASE-17347) | ExportSnapshot may write snapshot info file to wrong directory when specifying target name |  Minor | snapshots |
| [HBASE-17344](https://issues.apache.org/jira/browse/HBASE-17344) | The regionserver web UIs miss the coprocessors of RegionServerCoprocessorHost. |  Minor | . |
| [HBASE-17333](https://issues.apache.org/jira/browse/HBASE-17333) | HBASE-17294 always ensures CompactingMemstore is default |  Critical | . |
| [HBASE-17326](https://issues.apache.org/jira/browse/HBASE-17326) | Fix findbugs warning in BufferedMutatorParams |  Major | . |
| [HBASE-17309](https://issues.apache.org/jira/browse/HBASE-17309) | Fix connection leaks in TestAcidGuarantees |  Minor | integration tests |
| [HBASE-17297](https://issues.apache.org/jira/browse/HBASE-17297) | Single Filter in parenthesis cannot be parsed correctly |  Major | Filters |
| [HBASE-17237](https://issues.apache.org/jira/browse/HBASE-17237) | Override the correct compact method in HMobStore |  Major | mob |
| [HBASE-16985](https://issues.apache.org/jira/browse/HBASE-16985) | TestClusterId failed due to wrong hbase rootdir |  Minor | test |
| [HBASE-15437](https://issues.apache.org/jira/browse/HBASE-15437) | Response size calculated in RPCServer for warning tooLarge responses does NOT count CellScanner payload |  Major | IPC/RPC |
| [HBASE-17256](https://issues.apache.org/jira/browse/HBASE-17256) | Rpc handler monitoring will be removed when the task queue is full |  Major | . |
| [HBASE-17170](https://issues.apache.org/jira/browse/HBASE-17170) | HBase is also retrying DoNotRetryIOException because of class loader differences. |  Major | . |
| [HBASE-17252](https://issues.apache.org/jira/browse/HBASE-17252) | Wrong arguments for ValueAndTagRewriteCell in CellUtil |  Major | . |
| [HBASE-16841](https://issues.apache.org/jira/browse/HBASE-16841) | Data loss in MOB files after cloning a snapshot and deleting that snapshot |  Blocker | mob, snapshots |
| [HBASE-17118](https://issues.apache.org/jira/browse/HBASE-17118) | StoreScanner leaked in KeyValueHeap |  Major | . |
| [HBASE-17231](https://issues.apache.org/jira/browse/HBASE-17231) | Region#getCellCompartor sp? |  Trivial | . |
| [HBASE-16886](https://issues.apache.org/jira/browse/HBASE-16886) | hbase-client: scanner with reversed=true and small=true gets no result |  Major | . |
| [HBASE-17112](https://issues.apache.org/jira/browse/HBASE-17112) | Prevent setting timestamp of delta operations the same as previous value's |  Major | . |
| [HBASE-16209](https://issues.apache.org/jira/browse/HBASE-16209) | Provide an ExponentialBackOffPolicy sleep between failed region open requests |  Major | . |
| [HBASE-17206](https://issues.apache.org/jira/browse/HBASE-17206) | FSHLog may roll a new writer successfully with unflushed entries |  Critical | wal |
| [HBASE-17224](https://issues.apache.org/jira/browse/HBASE-17224) | There are lots of spelling errors in the HBase logging and exception messages |  Trivial | Client, io, mapreduce, master, regionserver, security, wal |
| [HBASE-17192](https://issues.apache.org/jira/browse/HBASE-17192) | remove use of scala-tools.org from pom |  Blocker | spark, website |
| [HBASE-17186](https://issues.apache.org/jira/browse/HBASE-17186) | MasterProcedureTestingUtility#testRecoveryAndDoubleExecution displays stale procedure state info |  Minor | proc-v2, test |
| [HBASE-17072](https://issues.apache.org/jira/browse/HBASE-17072) | CPU usage starts to climb up to 90-100% when using G1GC; purge ThreadLocal usage |  Critical | Performance, regionserver |
| [HBASE-17116](https://issues.apache.org/jira/browse/HBASE-17116) | [PerformanceEvaluation] Add option to configure block size |  Trivial | tooling |
| [HBASE-17127](https://issues.apache.org/jira/browse/HBASE-17127) | Locate region should fail fast if underlying Connection already closed |  Major | . |
| [HBASE-17144](https://issues.apache.org/jira/browse/HBASE-17144) | Possible offheap read ByteBuffers leak |  Major | rpc |
| [HBASE-17160](https://issues.apache.org/jira/browse/HBASE-17160) | Undo unnecessary inter-module dependency; spark to hbase-it and hbase-it to shell |  Minor | . |
| [HBASE-17171](https://issues.apache.org/jira/browse/HBASE-17171) | IntegrationTestTimeBoundedRequestsWithRegionReplicas fails with obtuse error when readers have no time to run |  Minor | integration tests |
| [HBASE-17158](https://issues.apache.org/jira/browse/HBASE-17158) | Avoid deadlock caused by HRegion#doDelta |  Major | . |
| [HBASE-17166](https://issues.apache.org/jira/browse/HBASE-17166) | ITBLL fails on master unable to find hbase-protocol-shaded content |  Major | . |
| [HBASE-17095](https://issues.apache.org/jira/browse/HBASE-17095) | The ClientSimpleScanner keeps retrying if the hfile is corrupt or cannot found |  Major | regionserver, scan |
| [HBASE-16989](https://issues.apache.org/jira/browse/HBASE-16989) | RowProcess#postBatchMutate doesn’t be executed before the mvcc transaction completion |  Major | . |
| [HBASE-17058](https://issues.apache.org/jira/browse/HBASE-17058) | Lower epsilon used for jitter verification from HBASE-15324 |  Major | Compaction |
| [HBASE-17082](https://issues.apache.org/jira/browse/HBASE-17082) | ForeignExceptionUtil isn’t packaged when building shaded protocol with -Pcompile-protobuf |  Major | . |
| [HBASE-17091](https://issues.apache.org/jira/browse/HBASE-17091) | IntegrationTestZKAndFSPermissions failed with 'KeeperException$NoNodeException' |  Major | . |
| [HBASE-17092](https://issues.apache.org/jira/browse/HBASE-17092) | Both LoadIncrementalHFiles#doBulkLoad() methods should set return value |  Major | . |
| [HBASE-17044](https://issues.apache.org/jira/browse/HBASE-17044) | Fix merge failed before creating merged region leaves meta inconsistent |  Critical | regionserver |
| [HBASE-17074](https://issues.apache.org/jira/browse/HBASE-17074) | PreCommit job always fails because of OOM |  Critical | build |
| [HBASE-16345](https://issues.apache.org/jira/browse/HBASE-16345) | RpcRetryingCallerWithReadReplicas#call() should catch some RegionServer Exceptions |  Major | Client |
| [HBASE-17062](https://issues.apache.org/jira/browse/HBASE-17062) | RegionSplitter throws ClassCastException |  Minor | util |
| [HBASE-16962](https://issues.apache.org/jira/browse/HBASE-16962) | Add readPoint to preCompactScannerOpen() and preFlushScannerOpen() API |  Major | . |
| [HBASE-17039](https://issues.apache.org/jira/browse/HBASE-17039) | SimpleLoadBalancer schedules large amount of invalid region moves |  Major | Balancer |
| [HBASE-17020](https://issues.apache.org/jira/browse/HBASE-17020) | keylen in midkey() dont computed correctly |  Major | HFile |
| [HBASE-16938](https://issues.apache.org/jira/browse/HBASE-16938) | TableCFsUpdater maybe failed due to no write permission on peerNode |  Major | Replication |
| [HBASE-17052](https://issues.apache.org/jira/browse/HBASE-17052) | compile-protobuf profile does not compile protobufs in some modules anymore |  Major | . |
| [HBASE-17054](https://issues.apache.org/jira/browse/HBASE-17054) | Compactor#preCreateCoprocScanner should be passed user |  Major | . |
| [HBASE-16983](https://issues.apache.org/jira/browse/HBASE-16983) | TestMultiTableSnapshotInputFormat failing with  Unable to create region directory: /tmp/... |  Minor | test |
| [HBASE-17042](https://issues.apache.org/jira/browse/HBASE-17042) | Remove 'public' keyword from MasterObserver interface |  Minor | Coprocessors |
| [HBASE-17033](https://issues.apache.org/jira/browse/HBASE-17033) | LogRoller makes a lot of allocations unnecessarily |  Major | . |
| [HBASE-16992](https://issues.apache.org/jira/browse/HBASE-16992) | The usage of mutation from CP is weird. |  Minor | . |
| [HBASE-17032](https://issues.apache.org/jira/browse/HBASE-17032) | CallQueueTooBigException and CallDroppedException should not be triggering PFFE |  Major | Client |
| [HBASE-16960](https://issues.apache.org/jira/browse/HBASE-16960) | RegionServer hang when aborting |  Critical | . |
| [HBASE-14329](https://issues.apache.org/jira/browse/HBASE-14329) | Report region in transition only ever operates on one region |  Major | Region Assignment |
| [HBASE-16964](https://issues.apache.org/jira/browse/HBASE-16964) | Successfully archived files are not cleared from compacted store file list if archiving of any file fails |  Blocker | regionserver |
| [HBASE-16976](https://issues.apache.org/jira/browse/HBASE-16976) | hbase-protocol-shaded module generates classes to wrong directory |  Minor | build |
| [HBASE-16931](https://issues.apache.org/jira/browse/HBASE-16931) | Setting cell's seqId to zero in compaction flow might cause RS down. |  Critical | regionserver |
| [HBASE-16980](https://issues.apache.org/jira/browse/HBASE-16980) | TestRowProcessorEndpoint failing consistently |  Major | . |
| [HBASE-16966](https://issues.apache.org/jira/browse/HBASE-16966) | Re-enable TestSimpleRpcScheduler#testCoDelScheduling  in master branch |  Major | test |
| [HBASE-16974](https://issues.apache.org/jira/browse/HBASE-16974) | Update os-maven-plugin to 1.4.1.final+ for building shade file on RHEL/CentOS |  Minor | . |
| [HBASE-16971](https://issues.apache.org/jira/browse/HBASE-16971) | The passed durability of Append/Increment isn't used in wal sync |  Major | . |
| [HBASE-16743](https://issues.apache.org/jira/browse/HBASE-16743) | TestSimpleRpcScheduler#testCoDelScheduling is broke |  Major | rpc |
| [HBASE-16948](https://issues.apache.org/jira/browse/HBASE-16948) | Fix inconsistency between HRegion and Region javadoc on getRowLock |  Major | . |
| [HBASE-16949](https://issues.apache.org/jira/browse/HBASE-16949) | Fix RAT License complaint about the hbase-protocol-shaded/src/main/patches content |  Major | . |
| [HBASE-16939](https://issues.apache.org/jira/browse/HBASE-16939) | ExportSnapshot: set owner and permission on right directory |  Minor | . |
| [HBASE-16000](https://issues.apache.org/jira/browse/HBASE-16000) | Table#checkAndPut() docs are too vague |  Minor | documentation |
| [HBASE-16880](https://issues.apache.org/jira/browse/HBASE-16880) | Correct the javadoc/behaviour of the APIs in ByteBufferUtils |  Major | . |
| [HBASE-16930](https://issues.apache.org/jira/browse/HBASE-16930) | AssignmentManager#checkWals() function can recur infinitely |  Major | Region Assignment |
| [HBASE-16887](https://issues.apache.org/jira/browse/HBASE-16887) | Allow setting different hadoopcheck versions in precommit for different branches |  Major | build |
| [HBASE-15684](https://issues.apache.org/jira/browse/HBASE-15684) | Fix the broken log file size accounting |  Major | wal |
| [HBASE-16870](https://issues.apache.org/jira/browse/HBASE-16870) | Add the metrics of replication sources which were transformed from other dead rs to ReplicationLoad |  Minor | Replication |
| [HBASE-16815](https://issues.apache.org/jira/browse/HBASE-16815) | Low scan ratio in RPC queue tuning triggers divide by zero exception |  Major | regionserver, rpc |
| [HBASE-16829](https://issues.apache.org/jira/browse/HBASE-16829) | DemoClient should detect secure mode |  Major | . |
| [HBASE-16754](https://issues.apache.org/jira/browse/HBASE-16754) | Regions failing compaction due to referencing non-existent store file |  Blocker | . |
| [HBASE-15348](https://issues.apache.org/jira/browse/HBASE-15348) | Fix tests broken by recent metrics re-work |  Major | metrics, test |
| [HBASE-16910](https://issues.apache.org/jira/browse/HBASE-16910) | Avoid NPE when starting StochasticLoadBalancer |  Minor | Balancer |
| [HBASE-16701](https://issues.apache.org/jira/browse/HBASE-16701) | TestHRegion and TestHRegionWithInMemoryFlush timing out |  Major | regionserver, test |
| [HBASE-16889](https://issues.apache.org/jira/browse/HBASE-16889) | Proc-V2: verifyTables in the IntegrationTestDDLMasterFailover test after each table DDL is incorrect |  Major | proc-v2 |
| [HBASE-16884](https://issues.apache.org/jira/browse/HBASE-16884) | Add HBase-2.0.x to the hadoop version support matrix in our documentation |  Blocker | documentation |
| [HBASE-16752](https://issues.apache.org/jira/browse/HBASE-16752) | Upgrading from 1.2 to 1.3 can lead to replication failures due to difference in RPC size limit |  Major | Replication, rpc |
| [HBASE-16578](https://issues.apache.org/jira/browse/HBASE-16578) | Mob data loss after mob compaction and normal compaction |  Major | mob |
| [HBASE-16824](https://issues.apache.org/jira/browse/HBASE-16824) | Writer.flush() can be called on already closed streams in WAL roll |  Major | wal |
| [HBASE-16866](https://issues.apache.org/jira/browse/HBASE-16866) | Avoid NPE in AsyncRequestFutureImpl#updateStats |  Minor | . |
| [HBASE-16733](https://issues.apache.org/jira/browse/HBASE-16733) | add hadoop 3.0.0-alpha1 to precommit checks |  Major | build, hadoop3 |
| [HBASE-16283](https://issues.apache.org/jira/browse/HBASE-16283) | Batch Append/Increment will always fail if set ReturnResults to false |  Minor | API |
| [HBASE-16712](https://issues.apache.org/jira/browse/HBASE-16712) | fix hadoop-3.0 profile mvn install |  Major | build, hadoop3 |
| [HBASE-16721](https://issues.apache.org/jira/browse/HBASE-16721) | Concurrency issue in WAL unflushed seqId tracking |  Critical | wal |
| [HBASE-16326](https://issues.apache.org/jira/browse/HBASE-16326) | CellModel / RowModel should override 'equals', 'hashCode' and 'toString' |  Trivial | REST |
| [HBASE-16855](https://issues.apache.org/jira/browse/HBASE-16855) | Avoid NPE in MetricsConnection’s construction |  Minor | . |
| [HBASE-16856](https://issues.apache.org/jira/browse/HBASE-16856) | Exception message in SyncRunner.run() should print currentSequence |  Minor | wal |
| [HBASE-16853](https://issues.apache.org/jira/browse/HBASE-16853) | Regions are assigned to Region Servers in /hbase/draining after HBase Master failover |  Major | Balancer, Region Assignment |
| [HBASE-16810](https://issues.apache.org/jira/browse/HBASE-16810) | HBase Balancer throws ArrayIndexOutOfBoundsException when regionservers are in /hbase/draining znode and unloaded |  Major | Balancer |
| [HBASE-16664](https://issues.apache.org/jira/browse/HBASE-16664) | Timeout logic in AsyncProcess is broken |  Major | . |
| [HBASE-16724](https://issues.apache.org/jira/browse/HBASE-16724) | Snapshot owner can't clone |  Major | snapshots |
| [HBASE-16807](https://issues.apache.org/jira/browse/HBASE-16807) | RegionServer will fail to report new active Hmaster until HMaster/RegionServer failover |  Major | regionserver |
| [HBASE-16716](https://issues.apache.org/jira/browse/HBASE-16716) | OfflineMetaRepair leaves empty directory inside /hbase/WALs which remains forever |  Minor | hbck |
| [HBASE-16746](https://issues.apache.org/jira/browse/HBASE-16746) | The log of “region close” should differ from “region move” |  Minor | . |
| [HBASE-16808](https://issues.apache.org/jira/browse/HBASE-16808) | Included the generated, shaded java files from "HBASE-15638 Shade protobuf" in our src assembly |  Blocker | . |
| [HBASE-16731](https://issues.apache.org/jira/browse/HBASE-16731) | Inconsistent results from the Get/Scan if we use the empty FilterList |  Minor | . |
| [HBASE-16801](https://issues.apache.org/jira/browse/HBASE-16801) | The Append/Increment may return the data from future |  Critical | . |
| [HBASE-15109](https://issues.apache.org/jira/browse/HBASE-15109) | HM/RS failed to start when "fs.hdfs.impl.disable.cache" is set to true |  Critical | regionserver |
| [HBASE-16622](https://issues.apache.org/jira/browse/HBASE-16622) | Fix some issues with the HBase reference guide |  Major | documentation |
| [HBASE-16793](https://issues.apache.org/jira/browse/HBASE-16793) | Exclude shaded protobuf files from rat check |  Major | build |
| [HBASE-16699](https://issues.apache.org/jira/browse/HBASE-16699) | Overflows in AverageIntervalRateLimiter's refill() and getWaitInterval() |  Major | . |
| [HBASE-16771](https://issues.apache.org/jira/browse/HBASE-16771) | VerifyReplication should increase GOODROWS counter if re-comparison passes |  Major | . |
| [HBASE-16768](https://issues.apache.org/jira/browse/HBASE-16768) | Inconsistent results from the Append/Increment |  Minor | . |
| [HBASE-16767](https://issues.apache.org/jira/browse/HBASE-16767) | Mob compaction needs to clean up files in /hbase/mobdir/.tmp and /hbase/mobdir/.tmp/.bulkload when running into IO exceptions |  Major | mob |
| [HBASE-16681](https://issues.apache.org/jira/browse/HBASE-16681) | Fix flaky TestReplicationSourceManagerZkImpl |  Major | . |
| [HBASE-16753](https://issues.apache.org/jira/browse/HBASE-16753) | There is a mismatch between suggested Java version in hbase-env.sh |  Minor | scripts |
| [HBASE-16644](https://issues.apache.org/jira/browse/HBASE-16644) | Errors when reading legit HFile Trailer of old (v2.0) format file |  Critical | HFile |
| [HBASE-16763](https://issues.apache.org/jira/browse/HBASE-16763) | Remove unintentional dependency on net.sf.ehcache.search.Results |  Major | . |
| [HBASE-16764](https://issues.apache.org/jira/browse/HBASE-16764) | hbase-protocol-shaded generate-shaded-classes profile unpacks shaded java files into wrong location |  Major | build |
| [HBASE-16739](https://issues.apache.org/jira/browse/HBASE-16739) | Timed out exception message should include encoded region name |  Minor | . |
| [HBASE-16678](https://issues.apache.org/jira/browse/HBASE-16678) | MapReduce jobs do not update counters from ScanMetrics |  Major | . |
| [HBASE-16732](https://issues.apache.org/jira/browse/HBASE-16732) | Avoid possible NPE in MetaTableLocator |  Minor | . |
| [HBASE-16723](https://issues.apache.org/jira/browse/HBASE-16723) | RMI registry is not destroyed after stopping JMX Connector Server |  Major | metrics |
| [HBASE-16711](https://issues.apache.org/jira/browse/HBASE-16711) | Fix hadoop-3.0 profile compile |  Major | build, hadoop3 |
| [HBASE-16696](https://issues.apache.org/jira/browse/HBASE-16696) | After HBASE-16604 - does not release blocks in case of scanner exception |  Critical | . |
| [HBASE-16682](https://issues.apache.org/jira/browse/HBASE-16682) | Fix Shell tests failure. NoClassDefFoundError for MiniKdc |  Major | . |
| [HBASE-16660](https://issues.apache.org/jira/browse/HBASE-16660) | ArrayIndexOutOfBounds during the majorCompactionCheck in DateTieredCompaction |  Critical | Compaction |
| [HBASE-16643](https://issues.apache.org/jira/browse/HBASE-16643) | Reverse scanner heap creation may not allow MSLAB closure due to improper ref counting of segments |  Critical | . |
| [HBASE-16649](https://issues.apache.org/jira/browse/HBASE-16649) | Truncate table with splits preserved can cause both data loss and truncated data appeared again |  Major | . |
| [HBASE-16704](https://issues.apache.org/jira/browse/HBASE-16704) | Scan will be broken while working with DBE and KeyValueCodecWithTags |  Major | . |
| [HBASE-16645](https://issues.apache.org/jira/browse/HBASE-16645) | Wrong range of Cells is caused by CellFlatMap#tailMap, headMap, and SubMap |  Major | . |
| [HBASE-16665](https://issues.apache.org/jira/browse/HBASE-16665) | Check whether KeyValueUtil.createXXX could be replaced by CellUtil without copy |  Major | . |
| [HBASE-16697](https://issues.apache.org/jira/browse/HBASE-16697) | bump TestRegionServerMetrics to LargeTests |  Trivial | test |
| [HBASE-16679](https://issues.apache.org/jira/browse/HBASE-16679) | Flush throughput controller: Minor perf change and fix flaky TestFlushWithThroughputController |  Major | . |
| [HBASE-16693](https://issues.apache.org/jira/browse/HBASE-16693) | The commit of HBASE-16604 creates a unrelated folder |  Critical | . |
| [HBASE-16688](https://issues.apache.org/jira/browse/HBASE-16688) | Split TestMasterFailoverWithProcedures |  Major | proc-v2, test |
| [HBASE-16604](https://issues.apache.org/jira/browse/HBASE-16604) | Scanner retries on IOException can cause the scans to miss data |  Critical | regionserver, Scanners |
| [HBASE-16662](https://issues.apache.org/jira/browse/HBASE-16662) | Fix open POODLE vulnerabilities |  Major | REST, Thrift |
| [HBASE-16675](https://issues.apache.org/jira/browse/HBASE-16675) | Average region size may be incorrect when there is region whose RegionLoad cannot be retrieved |  Major | . |
| [HBASE-16670](https://issues.apache.org/jira/browse/HBASE-16670) | Make RpcServer#processRequest logic more robust |  Minor | . |
| [HBASE-12088](https://issues.apache.org/jira/browse/HBASE-12088) | Remove un-used profiles in non-root poms |  Major | . |
| [HBASE-16669](https://issues.apache.org/jira/browse/HBASE-16669) | fix flaky TestAdmin#testmergeRegions |  Major | test |
| [HBASE-16294](https://issues.apache.org/jira/browse/HBASE-16294) | hbck reporting "No HDFS region dir found" for replicas |  Minor | hbck |
| [HBASE-12949](https://issues.apache.org/jira/browse/HBASE-12949) | Scanner can be stuck in infinite loop if the HFile is corrupted |  Major | . |
| [HBASE-16654](https://issues.apache.org/jira/browse/HBASE-16654) | Better handle channelInactive and close for netty rpc client |  Major | . |
| [HBASE-16540](https://issues.apache.org/jira/browse/HBASE-16540) | Scan should do additional validation on start and stop row |  Major | Client |
| [HBASE-16647](https://issues.apache.org/jira/browse/HBASE-16647) | hbck should do offline reference repair before online repair |  Major | . |
| [HBASE-16551](https://issues.apache.org/jira/browse/HBASE-16551) | Cleanup SplitLogManager and CatalogJanitor |  Trivial | master |
| [HBASE-16635](https://issues.apache.org/jira/browse/HBASE-16635) | RpcClient under heavy load leaks some netty bytebuf |  Minor | . |
| [HBASE-16165](https://issues.apache.org/jira/browse/HBASE-16165) | Decrease RpcServer.callQueueSize before writeResponse causes OOM |  Minor | IPC/RPC, rpc |
| [HBASE-16624](https://issues.apache.org/jira/browse/HBASE-16624) | MVCC DeSerialization bug in the HFileScannerImpl |  Blocker | HFile |
| [HBASE-16612](https://issues.apache.org/jira/browse/HBASE-16612) | Use array to cache Types for KeyValue.Type.codeToType |  Minor | . |
| [HBASE-16491](https://issues.apache.org/jira/browse/HBASE-16491) | A few org.apache.hadoop.hbase.rsgroup classes missing @InterfaceAudience annotation |  Minor | API, regionserver |
| [HBASE-16609](https://issues.apache.org/jira/browse/HBASE-16609) | Fake cells EmptyByteBufferedCell  created in read path not implementing SettableSequenceId |  Major | . |
| [HBASE-16614](https://issues.apache.org/jira/browse/HBASE-16614) | Use daemon thread for netty event loop |  Major | IPC/RPC, rpc |
| [HBASE-15624](https://issues.apache.org/jira/browse/HBASE-15624) | Move master branch/hbase-2.0.0 to jdk-8 only |  Blocker | build |
| [HBASE-16576](https://issues.apache.org/jira/browse/HBASE-16576) | Shell add\_peer doesn't allow setting cluster\_key for custom endpoints |  Major | shell |
| [HBASE-16309](https://issues.apache.org/jira/browse/HBASE-16309) | TestDefaultCompactSelection.testCompactionRatio is flaky |  Major | Compaction, test |
| [HBASE-16589](https://issues.apache.org/jira/browse/HBASE-16589) | Adjust log level for FATAL messages from HBaseReplicationEndpoint that are not fatal |  Trivial | . |
| [HBASE-16544](https://issues.apache.org/jira/browse/HBASE-16544) | Remove or Clarify  'Using Amazon S3 Storage' section in the reference guide |  Major | documentation, Filesystem Integration |
| [HBASE-16572](https://issues.apache.org/jira/browse/HBASE-16572) | Sync method in RecoverableZooKeeper failed to pass callback function in |  Minor | Zookeeper |
| [HBASE-16460](https://issues.apache.org/jira/browse/HBASE-16460) | Can't rebuild the BucketAllocator's data structures when BucketCache uses FileIOEngine |  Major | BucketCache |
| [HBASE-16556](https://issues.apache.org/jira/browse/HBASE-16556) | The read/write timeout are not used in HTable.delete(List), HTable.get(List), and HTable.existsAll(List) |  Minor | . |
| [HBASE-16538](https://issues.apache.org/jira/browse/HBASE-16538) | Version mismatch in HBaseConfiguration.checkDefaultsVersion |  Major | . |
| [HBASE-16552](https://issues.apache.org/jira/browse/HBASE-16552) | MiniHBaseCluster#getServerWith() does not ignore stopped RSs |  Trivial | test |
| [HBASE-16547](https://issues.apache.org/jira/browse/HBASE-16547) | hbase-archetype-builder shell scripts assume bash is installed in /bin |  Trivial | . |
| [HBASE-16375](https://issues.apache.org/jira/browse/HBASE-16375) | Mapreduce mini cluster using HBaseTestingUtility not setting correct resourcemanager and jobhistory webapp address of MapReduceTestingShim |  Minor | . |
| [HBASE-16527](https://issues.apache.org/jira/browse/HBASE-16527) | IOExceptions from DFS client still can cause CatalogJanitor to delete referenced files |  Major | . |
| [HBASE-15278](https://issues.apache.org/jira/browse/HBASE-15278) | AsyncRPCClient hangs if Connection closes before RPC call response |  Blocker | rpc, test |
| [HBASE-16528](https://issues.apache.org/jira/browse/HBASE-16528) | Procedure-V2: ServerCrashProcedure misses owner information |  Major | hbase, master, proc-v2 |
| [HBASE-16535](https://issues.apache.org/jira/browse/HBASE-16535) | Use regex to exclude generated classes for findbugs |  Major | findbugs |
| [HBASE-16515](https://issues.apache.org/jira/browse/HBASE-16515) | AsyncProcess has incorrent count of tasks if the backoff policy is enabled |  Minor | . |
| [HBASE-16490](https://issues.apache.org/jira/browse/HBASE-16490) | Fix race condition between SnapshotManager and SnapshotCleaner |  Major | . |
| [HBASE-16409](https://issues.apache.org/jira/browse/HBASE-16409) | Row key for bad row should be properly delimited in VerifyReplication |  Minor | . |
| [HBASE-16495](https://issues.apache.org/jira/browse/HBASE-16495) | When accessed via Thrift, all column families have timeToLive equal to -1 |  Minor | Thrift |
| [HBASE-16304](https://issues.apache.org/jira/browse/HBASE-16304) | HRegion#RegionScannerImpl#handleFileNotFoundException may lead to deadlock when trying to obtain write lock on updatesLock |  Critical | . |
| [HBASE-16270](https://issues.apache.org/jira/browse/HBASE-16270) | Handle duplicate clearing of snapshot in region replicas |  Major | Replication |
| [HBASE-16471](https://issues.apache.org/jira/browse/HBASE-16471) | Region Server metrics context will be wrong when machine hostname contain "master" word |  Minor | metrics |
| [HBASE-16456](https://issues.apache.org/jira/browse/HBASE-16456) | Fix findbugs warnings in hbase-rsgroup module |  Minor | . |
| [HBASE-16464](https://issues.apache.org/jira/browse/HBASE-16464) | archive folder grows bigger and bigger due to corrupt snapshot under tmp dir |  Major | snapshots |
| [HBASE-16360](https://issues.apache.org/jira/browse/HBASE-16360) | TableMapReduceUtil addHBaseDependencyJars has the wrong class name for PrefixTreeCodec |  Minor | mapreduce |
| [HBASE-16446](https://issues.apache.org/jira/browse/HBASE-16446) | append\_peer\_tableCFs failed when there already have this table's partial cfs in the peer |  Minor | Replication |
| [HBASE-16454](https://issues.apache.org/jira/browse/HBASE-16454) | Compactor's shipping logic decision should be based on the current store's block size |  Minor | . |
| [HBASE-16444](https://issues.apache.org/jira/browse/HBASE-16444) | CellUtil#estimatedSerializedSizeOfKey() should consider KEY\_INFRASTRUCTURE\_SIZE |  Minor | . |
| [HBASE-7621](https://issues.apache.org/jira/browse/HBASE-7621) | REST client (RemoteHTable) doesn't support binary row keys |  Major | REST |
| [HBASE-16430](https://issues.apache.org/jira/browse/HBASE-16430) | Fix RegionServer Group's bug when moving multiple tables |  Major | master |
| [HBASE-16429](https://issues.apache.org/jira/browse/HBASE-16429) | FSHLog: deadlock if rollWriter called when ring buffer filled with appends |  Critical | . |
| [HBASE-16384](https://issues.apache.org/jira/browse/HBASE-16384) | Update report-flakies.py script to allow specifying a list of build ids to be excluded |  Major | . |
| [HBASE-15635](https://issues.apache.org/jira/browse/HBASE-15635) | Mean age of Blocks in cache (seconds) on webUI should be greater than zero |  Major | . |
| [HBASE-16341](https://issues.apache.org/jira/browse/HBASE-16341) | Missing bit on "Regression: Random Read/WorkloadC slower in 1.x than 0.98" |  Major | rpc |
| [HBASE-16267](https://issues.apache.org/jira/browse/HBASE-16267) | Remove commons-httpclient dependency from hbase-rest module |  Critical | . |
| [HBASE-16389](https://issues.apache.org/jira/browse/HBASE-16389) | Thread leak in CoprocessorHost#getTable(TableName) API |  Major | . |
| [HBASE-16363](https://issues.apache.org/jira/browse/HBASE-16363) | Correct javadoc for qualifier length and value length in Cell interface |  Minor | documentation |
| [HBASE-16368](https://issues.apache.org/jira/browse/HBASE-16368) | test\*WhenRegionMove in TestPartialResultsFromClientSide is flaky |  Major | Scanners |
| [HBASE-16310](https://issues.apache.org/jira/browse/HBASE-16310) | Revisit the logic of filterRowKey for Filters |  Major | . |
| [HBASE-16361](https://issues.apache.org/jira/browse/HBASE-16361) | Revert of HBASE-16317, "revert all ESAPI..." broke TestLogLevel |  Major | dependencies, UI |
| [HBASE-16367](https://issues.apache.org/jira/browse/HBASE-16367) | Race between master and region server initialization may lead to premature server abort |  Major | . |
| [HBASE-16303](https://issues.apache.org/jira/browse/HBASE-16303) | FilterList with MUST\_PASS\_ONE optimization |  Major | Filters |
| [HBASE-15866](https://issues.apache.org/jira/browse/HBASE-15866) | Split hbase.rpc.timeout into \*.read.timeout and \*.write.timeout |  Major | . |
| [HBASE-16362](https://issues.apache.org/jira/browse/HBASE-16362) | Mob compaction does not set cacheBlocks to false when creating StoreScanner |  Major | Compaction, regionserver |
| [HBASE-16359](https://issues.apache.org/jira/browse/HBASE-16359) | NullPointerException in RSRpcServices.openRegion() |  Major | . |
| [HBASE-15461](https://issues.apache.org/jira/browse/HBASE-15461) | ref guide has bad links to blogs originally posted on cloudera website |  Major | website |
| [HBASE-15574](https://issues.apache.org/jira/browse/HBASE-15574) | Minor typo on HRegionServerCommandLine.java |  Trivial | . |
| [HBASE-16350](https://issues.apache.org/jira/browse/HBASE-16350) | Undo server abort from HBASE-14968 |  Major | . |
| [HBASE-16271](https://issues.apache.org/jira/browse/HBASE-16271) | Fix logging and re-run the test in IntegrationTestBulkLoad |  Major | . |
| [HBASE-16347](https://issues.apache.org/jira/browse/HBASE-16347) | Unevaluated expressions in book |  Major | documentation, website |
| [HBASE-16301](https://issues.apache.org/jira/browse/HBASE-16301) | Trigger flush without waiting when compaction is disabled on a table |  Minor | . |
| [HBASE-16319](https://issues.apache.org/jira/browse/HBASE-16319) | Fix TestCacheOnWrite after HBASE-16288 |  Trivial | . |
| [HBASE-16315](https://issues.apache.org/jira/browse/HBASE-16315) | RegionSizeCalculator prints region names as binary without escapes |  Trivial | . |
| [HBASE-16284](https://issues.apache.org/jira/browse/HBASE-16284) | Unauthorized client can shutdown the cluster |  Major | . |
| [HBASE-16296](https://issues.apache.org/jira/browse/HBASE-16296) | Reverse scan performance degrades when using filter lists |  Major | Filters |
| [HBASE-16288](https://issues.apache.org/jira/browse/HBASE-16288) | HFile intermediate block level indexes might recurse forever creating multi TB files |  Critical | HFile |
| [HBASE-16234](https://issues.apache.org/jira/browse/HBASE-16234) | Expect and handle nulls when assigning replicas |  Major | Region Assignment |
| [HBASE-16306](https://issues.apache.org/jira/browse/HBASE-16306) | Add specific imports to avoid namespace clash in defaultSource.scala |  Minor | . |
| [HBASE-16300](https://issues.apache.org/jira/browse/HBASE-16300) | LruBlockCache.CACHE\_FIXED\_OVERHEAD should calculate LruBlockCache size correctly |  Major | . |
| [HBASE-16289](https://issues.apache.org/jira/browse/HBASE-16289) | AsyncProcess stuck messages need to print region/server |  Critical | Operability |
| [HBASE-16096](https://issues.apache.org/jira/browse/HBASE-16096) | Replication keeps accumulating znodes |  Major | Replication |
| [HBASE-16293](https://issues.apache.org/jira/browse/HBASE-16293) | TestSnapshotFromMaster#testSnapshotHFileArchiving flakey |  Major | test |
| [HBASE-16281](https://issues.apache.org/jira/browse/HBASE-16281) | TestMasterReplication is flaky |  Major | . |
| [HBASE-16221](https://issues.apache.org/jira/browse/HBASE-16221) | Thrift server drops connection on long scans |  Major | Thrift |
| [HBASE-16272](https://issues.apache.org/jira/browse/HBASE-16272) | Overflow in ServerName's compareTo method |  Major | hbase |
| [HBASE-16244](https://issues.apache.org/jira/browse/HBASE-16244) | LocalHBaseCluster start timeout should be configurable |  Major | hbase |
| [HBASE-16238](https://issues.apache.org/jira/browse/HBASE-16238) | It's useless to catch SESSIONEXPIRED exception and retry in RecoverableZooKeeper |  Minor | Zookeeper |
| [HBASE-16172](https://issues.apache.org/jira/browse/HBASE-16172) | Unify the retry logic in ScannerCallableWithReplicas and RpcRetryingCallerWithReadReplicas |  Major | . |
| [HBASE-16110](https://issues.apache.org/jira/browse/HBASE-16110) | AsyncFS WAL doesn't work with Hadoop 2.8+ |  Blocker | wal |
| [HBASE-16076](https://issues.apache.org/jira/browse/HBASE-16076) | Cannot configure split policy in HBase shell |  Minor | documentation |
| [HBASE-16144](https://issues.apache.org/jira/browse/HBASE-16144) | Replication queue's lock will live forever if RS acquiring the lock has died prematurely |  Major | . |
| [HBASE-16235](https://issues.apache.org/jira/browse/HBASE-16235) | TestSnapshotFromMaster#testSnapshotHFileArchiving will fail if there are too many hfiles |  Trivial | . |
| [HBASE-16183](https://issues.apache.org/jira/browse/HBASE-16183) | Correct errors in example programs of coprocessor in Ref Guide |  Minor | documentation |
| [HBASE-16095](https://issues.apache.org/jira/browse/HBASE-16095) | Add priority to TableDescriptor and priority region open thread pool |  Major | . |
| [HBASE-16227](https://issues.apache.org/jira/browse/HBASE-16227) | [Shell] Column value formatter not working in scans |  Major | . |
| [HBASE-16211](https://issues.apache.org/jira/browse/HBASE-16211) | JMXCacheBuster restarting the metrics system might cause tests to hang |  Major | . |
| [HBASE-16150](https://issues.apache.org/jira/browse/HBASE-16150) | Remove ConcurrentIndex |  Major | . |
| [HBASE-16207](https://issues.apache.org/jira/browse/HBASE-16207) | can't restore snapshot without "Admin" permission |  Major | master, snapshots |
| [HBASE-16184](https://issues.apache.org/jira/browse/HBASE-16184) | Shell test fails due to rLoadSink being nil |  Major | . |
| [HBASE-16055](https://issues.apache.org/jira/browse/HBASE-16055) | PutSortReducer loses any Visibility/acl attribute set on the Puts |  Critical | security |
| [HBASE-16081](https://issues.apache.org/jira/browse/HBASE-16081) | Replication remove\_peer gets stuck and blocks WAL rolling |  Blocker | regionserver, Replication |
| [HBASE-16044](https://issues.apache.org/jira/browse/HBASE-16044) | Fix 'hbase shell' output parsing in graceful\_stop.sh |  Critical | scripts, shell |
| [HBASE-16160](https://issues.apache.org/jira/browse/HBASE-16160) | Get the UnsupportedOperationException when using delegation token with encryption |  Blocker | . |
| [HBASE-16074](https://issues.apache.org/jira/browse/HBASE-16074) | ITBLL fails, reports lost big or tiny families |  Blocker | integration tests |
| [HBASE-16201](https://issues.apache.org/jira/browse/HBASE-16201) | NPE in RpcServer causing intermittent UT failure of TestMasterReplication#testHFileCyclicReplication |  Major | . |
| [HBASE-16171](https://issues.apache.org/jira/browse/HBASE-16171) | Fix the potential problems in TestHCM.testConnectionCloseAllowsInterrupt |  Major | . |
| [HBASE-15925](https://issues.apache.org/jira/browse/HBASE-15925) | compat-module maven variable not evaluated |  Blocker | build |
| [HBASE-16187](https://issues.apache.org/jira/browse/HBASE-16187) | Fix typo in blog post for metrics2 |  Major | website |
| [HBASE-16190](https://issues.apache.org/jira/browse/HBASE-16190) | IntegrationTestDDLMasterFailover failed with IllegalArgumentException: n must be positive |  Minor | . |
| [HBASE-16182](https://issues.apache.org/jira/browse/HBASE-16182) | Increase IntegrationTestRpcClient timeout |  Major | . |
| [HBASE-16157](https://issues.apache.org/jira/browse/HBASE-16157) | The incorrect block cache count and size are caused by removing duplicate block key in the LruBlockCache |  Trivial | . |
| [HBASE-16177](https://issues.apache.org/jira/browse/HBASE-16177) | In dev mode thrift server can't be run |  Major | . |
| [HBASE-16091](https://issues.apache.org/jira/browse/HBASE-16091) | Canary takes lot more time when there are delete markers in the table |  Major | canary |
| [HBASE-16132](https://issues.apache.org/jira/browse/HBASE-16132) | Scan does not return all the result when regionserver is busy |  Major | Client |
| [HBASE-16135](https://issues.apache.org/jira/browse/HBASE-16135) | PeerClusterZnode under rs of removed peer may never be deleted |  Major | Replication |
| [HBASE-15844](https://issues.apache.org/jira/browse/HBASE-15844) | We should respect hfile.block.index.cacheonwrite when write intermediate index Block |  Major | . |
| [HBASE-16159](https://issues.apache.org/jira/browse/HBASE-16159) | OutOfMemory exception when using AsyncRpcClient with encryption to read rpc response |  Major | . |
| [HBASE-16125](https://issues.apache.org/jira/browse/HBASE-16125) | RegionMover uses hardcoded, Unix-style tmp folder - breaks Windows |  Major | . |
| [HBASE-16133](https://issues.apache.org/jira/browse/HBASE-16133) | RSGroupBasedLoadBalancer.retainAssignment() might miss a region |  Major | . |
| [HBASE-16129](https://issues.apache.org/jira/browse/HBASE-16129) | check\_compatibility.sh is broken when using Java API Compliance Checker v1.7 |  Major | test |
| [HBASE-15976](https://issues.apache.org/jira/browse/HBASE-15976) | RegionServerMetricsWrapperRunnable will be failure  when disable blockcache. |  Major | . |
| [HBASE-16122](https://issues.apache.org/jira/browse/HBASE-16122) | PerformanceEvaluation should provide user friendly hint when client threads argument is missing |  Minor | . |
| [HBASE-16069](https://issues.apache.org/jira/browse/HBASE-16069) | Typo "trapsparently" in item 3 of chapter 87.2 of Reference Guide |  Minor | documentation |
| [HBASE-16109](https://issues.apache.org/jira/browse/HBASE-16109) | Add Interface audience annotation to a few classes |  Minor | . |
| [HBASE-16111](https://issues.apache.org/jira/browse/HBASE-16111) | Truncate preserve shell command is broken |  Major | shell |
| [HBASE-16103](https://issues.apache.org/jira/browse/HBASE-16103) | Procedure v2 - TestCloneSnapshotProcedure relies on execution order |  Minor | proc-v2, test |
| [HBASE-16040](https://issues.apache.org/jira/browse/HBASE-16040) | Remove configuration "hbase.replication" |  Major | . |
| [HBASE-16070](https://issues.apache.org/jira/browse/HBASE-16070) | Mapreduce Serialization classes do not have Interface audience |  Major | . |
| [HBASE-16012](https://issues.apache.org/jira/browse/HBASE-16012) | Major compaction can't work due to obsolete scanner read point in RegionServer |  Major | Compaction, Scanners |
| [HBASE-16062](https://issues.apache.org/jira/browse/HBASE-16062) | Improper error handling in WAL Reader/Writer creation |  Major | . |
| [HBASE-16032](https://issues.apache.org/jira/browse/HBASE-16032) | Possible memory leak in StoreScanner |  Major | . |
| [HBASE-16035](https://issues.apache.org/jira/browse/HBASE-16035) | Nested AutoCloseables might not all get closed |  Major | . |
| [HBASE-15783](https://issues.apache.org/jira/browse/HBASE-15783) | AccessControlConstants#OP\_ATTRIBUTE\_ACL\_STRATEGY\_CELL\_FIRST not used any more. |  Major | . |
| [HBASE-16059](https://issues.apache.org/jira/browse/HBASE-16059) | Region normalizer fails to trigger merge action where one of the regions is empty |  Major | master |
| [HBASE-14485](https://issues.apache.org/jira/browse/HBASE-14485) | ConnectionImplementation leaks on construction failure |  Major | Client |
| [HBASE-16054](https://issues.apache.org/jira/browse/HBASE-16054) | OutOfMemory exception when using AsyncRpcClient with encryption |  Major | . |
| [HBASE-16066](https://issues.apache.org/jira/browse/HBASE-16066) | Resolve RpC\_REPEATED\_CONDITIONAL\_TEST findbugs warnings in HMaster |  Major | . |
| [HBASE-16058](https://issues.apache.org/jira/browse/HBASE-16058) | TestHRegion fails on 1.4 builds |  Major | test |
| [HBASE-15467](https://issues.apache.org/jira/browse/HBASE-15467) | Remove 1.x/2.0 TableDescriptor incompatibility |  Major | master, regionserver |
| [HBASE-16061](https://issues.apache.org/jira/browse/HBASE-16061) | Allow logging to a buffered console |  Major | . |
| [HBASE-16053](https://issues.apache.org/jira/browse/HBASE-16053) | Master code is not setting the table in ENABLING state in create table |  Major | . |
| [HBASE-16045](https://issues.apache.org/jira/browse/HBASE-16045) | endtime argument for VerifyReplication was incorrectly specified in usage |  Major | . |
| [HBASE-15977](https://issues.apache.org/jira/browse/HBASE-15977) | Failed variable substitution on home page |  Major | website |
| [HBASE-16047](https://issues.apache.org/jira/browse/HBASE-16047) | TestFastFail is broken again |  Major | test |
| [HBASE-15908](https://issues.apache.org/jira/browse/HBASE-15908) | Checksum verification is broken due to incorrect passing of ByteBuffers in DataChecksum |  Blocker | HFile |
| [HBASE-16031](https://issues.apache.org/jira/browse/HBASE-16031) | Documents about "hbase.replication" default value seems wrong |  Major | documentation |
| [HBASE-16017](https://issues.apache.org/jira/browse/HBASE-16017) | HBase TableOutputFormat has connection leak in getRecordWriter |  Major | mapreduce |
| [HBASE-15746](https://issues.apache.org/jira/browse/HBASE-15746) | Remove extra RegionCoprocessor preClose() in RSRpcServices#closeRegion |  Minor | Coprocessors, regionserver |
| [HBASE-16021](https://issues.apache.org/jira/browse/HBASE-16021) | graceful\_stop.sh: Wrap variables in double quote to avoid  "[: too many arguments" error |  Minor | scripts |
| [HBASE-16016](https://issues.apache.org/jira/browse/HBASE-16016) | AssignmentManager#waitForAssignment could have unexpected negative deadline |  Major | . |
| [HBASE-16007](https://issues.apache.org/jira/browse/HBASE-16007) | Job's Configuration should be passed to TableMapReduceUtil#addDependencyJars() in WALPlayer |  Major | . |
| [HBASE-14644](https://issues.apache.org/jira/browse/HBASE-14644) | Region in transition metric is broken |  Major | . |
| [HBASE-15946](https://issues.apache.org/jira/browse/HBASE-15946) | Eliminate possible security concerns in RS web UI's store file metrics |  Major | . |
| [HBASE-15990](https://issues.apache.org/jira/browse/HBASE-15990) | The priority value of subsequent coprocessors in the Coprocessor.Priority.SYSTEM list are not incremented by one |  Minor | . |
| [HBASE-15952](https://issues.apache.org/jira/browse/HBASE-15952) | Bulk load data replication is not working when RS user does not have permission on hfile-refs node |  Major | Replication |
| [HBASE-15975](https://issues.apache.org/jira/browse/HBASE-15975) | logic in TestHTableDescriptor#testAddCoprocessorWithSpecStr is wrong |  Trivial | test |
| [HBASE-15959](https://issues.apache.org/jira/browse/HBASE-15959) | Fix flaky test TestRegionServerMetrics.testMobMetrics |  Major | . |
| [HBASE-15957](https://issues.apache.org/jira/browse/HBASE-15957) | RpcClientImpl.close never ends in some circumstances |  Major | Client, rpc |
| [HBASE-15803](https://issues.apache.org/jira/browse/HBASE-15803) | ZooKeeperWatcher's constructor can leak a ZooKeeper instance with throwing ZooKeeperConnectionException when canCreateBaseZNode is true |  Minor | . |
| [HBASE-15698](https://issues.apache.org/jira/browse/HBASE-15698) | Increment TimeRange not serialized to server |  Blocker | . |
| [HBASE-15965](https://issues.apache.org/jira/browse/HBASE-15965) | Shell test changes. Use @shell.command instead directly calling functions in admin.rb and other libraries. |  Major | . |
| [HBASE-15954](https://issues.apache.org/jira/browse/HBASE-15954) | REST server should log requests with TRACE instead of DEBUG |  Major | . |
| [HBASE-15889](https://issues.apache.org/jira/browse/HBASE-15889) | String case conversions are locale-sensitive, used without locale |  Minor | localization |
| [HBASE-15955](https://issues.apache.org/jira/browse/HBASE-15955) | Disable action in CatalogJanitor#setEnabled should wait for active cleanup scan to finish |  Major | master |
| [HBASE-15929](https://issues.apache.org/jira/browse/HBASE-15929) | There are two classes with name TestRegionServerMetrics |  Major | . |
| [HBASE-15845](https://issues.apache.org/jira/browse/HBASE-15845) | Shell Cleanup : move formatter to commands.rb; move one of the two hbase.rb to hbase\_constants.rb |  Minor | shell |
| [HBASE-15933](https://issues.apache.org/jira/browse/HBASE-15933) | NullPointerException may be thrown from SimpleRegionNormalizer#getRegionSize() |  Major | . |
| [HBASE-15938](https://issues.apache.org/jira/browse/HBASE-15938) | submit-patch.py: Don't crash if there are tests with same name. Refactor: Split out html template to separate file. |  Major | . |
| [HBASE-15944](https://issues.apache.org/jira/browse/HBASE-15944) | Spark test flooding mvn output. Redirect test logs to file. |  Major | . |
| [HBASE-15858](https://issues.apache.org/jira/browse/HBASE-15858) | Some region server group shell commands don't work |  Major | . |
| [HBASE-15932](https://issues.apache.org/jira/browse/HBASE-15932) | Shell test fails due to uninitialized constant |  Major | . |
| [HBASE-15907](https://issues.apache.org/jira/browse/HBASE-15907) | Missing documentation of create table split options |  Major | documentation |
| [HBASE-15918](https://issues.apache.org/jira/browse/HBASE-15918) | Cleanup excludes/includes file after use in hbase-personality.sh to avoid asf license error. |  Major | . |
| [HBASE-15897](https://issues.apache.org/jira/browse/HBASE-15897) | Fix a wrong comment about QOS order |  Trivial | . |
| [HBASE-15909](https://issues.apache.org/jira/browse/HBASE-15909) | Use Yetus' patch naming rules in submit-patch.py |  Major | . |
| [HBASE-15912](https://issues.apache.org/jira/browse/HBASE-15912) | REST module has 2 extant results in findbugs |  Major | . |
| [HBASE-15891](https://issues.apache.org/jira/browse/HBASE-15891) | Closeable resources potentially not getting closed if exception is thrown |  Minor | . |
| [HBASE-15693](https://issues.apache.org/jira/browse/HBASE-15693) | Reconsider the ImportOrder rule of checkstyle |  Major | build |
| [HBASE-11625](https://issues.apache.org/jira/browse/HBASE-11625) | Reading datablock throws "Invalid HFile block magic" and can not switch to hdfs checksum |  Major | HFile |
| [HBASE-15830](https://issues.apache.org/jira/browse/HBASE-15830) | Sasl encryption doesn't work with AsyncRpcChannelImpl |  Major | . |
| [HBASE-15884](https://issues.apache.org/jira/browse/HBASE-15884) | NPE in StoreFileScanner#skipKVsNewerThanReadpoint during reverse scan |  Major | Scanners |
| [HBASE-15880](https://issues.apache.org/jira/browse/HBASE-15880) | RpcClientImpl#tracedWriteRequest incorrectly closes HTrace span |  Major | tracing |
| [HBASE-14818](https://issues.apache.org/jira/browse/HBASE-14818) | user\_permission does not list namespace permissions |  Minor | hbase |
| [HBASE-15856](https://issues.apache.org/jira/browse/HBASE-15856) | Cached Connection instances can wind up with addresses never resolved |  Critical | Client |
| [HBASE-15863](https://issues.apache.org/jira/browse/HBASE-15863) | Typo in Put.java class documentation |  Trivial | documentation, java |
| [HBASE-15465](https://issues.apache.org/jira/browse/HBASE-15465) | userPermission returned by getUserPermission() for the selected namespace does not have namespace set |  Minor | Protobufs |
| [HBASE-15617](https://issues.apache.org/jira/browse/HBASE-15617) | Canary in regionserver mode might not enumerate all regionservers |  Minor | . |
| [HBASE-15850](https://issues.apache.org/jira/browse/HBASE-15850) | Localize the configuration change in testCheckTableLocks to reduce flakiness of TestHBaseFsck test suite |  Major | hbck |
| [HBASE-15841](https://issues.apache.org/jira/browse/HBASE-15841) | Performance Evaluation tool total rows may not be set correctly |  Minor | . |
| [HBASE-15824](https://issues.apache.org/jira/browse/HBASE-15824) | LocalHBaseCluster gets bind exception in master info port |  Major | . |
| [HBASE-15784](https://issues.apache.org/jira/browse/HBASE-15784) | Misuse core/maxPoolSize of LinkedBlockingQueue in ThreadPoolExecutor |  Major | Client, Replication, Thrift |
| [HBASE-15834](https://issues.apache.org/jira/browse/HBASE-15834) | Correct Bloom filter documentation in section 96.4 of Reference Guide |  Minor | documentation |
| [HBASE-15769](https://issues.apache.org/jira/browse/HBASE-15769) | Perform validation on cluster key for add\_peer |  Minor | . |
| [HBASE-15848](https://issues.apache.org/jira/browse/HBASE-15848) | Fix possible null point dereference in RSGroupBasedLoadBalancer#getMisplacedRegions() |  Trivial | regionserver |
| [HBASE-15840](https://issues.apache.org/jira/browse/HBASE-15840) | WAL.proto compilation broken for cpp |  Major | . |
| [HBASE-15725](https://issues.apache.org/jira/browse/HBASE-15725) | make\_patch.sh should add the branch name when -b is passed. |  Major | tooling |
| [HBASE-15828](https://issues.apache.org/jira/browse/HBASE-15828) | fix extant findbug |  Major | findbugs |
| [HBASE-15615](https://issues.apache.org/jira/browse/HBASE-15615) | Wrong sleep time when RegionServerCallable need retry |  Major | Client |
| [HBASE-15811](https://issues.apache.org/jira/browse/HBASE-15811) | Batch Get after batch Put does not fetch all Cells |  Blocker | Client |
| [HBASE-15742](https://issues.apache.org/jira/browse/HBASE-15742) | Reduce allocation of objects in metrics |  Major | . |
| [HBASE-15799](https://issues.apache.org/jira/browse/HBASE-15799) | Two Shell 'close\_region' Example Syntaxes Don't Work |  Minor | shell |
| [HBASE-15801](https://issues.apache.org/jira/browse/HBASE-15801) | Upgrade checkstyle for all branches |  Major | build |
| [HBASE-15797](https://issues.apache.org/jira/browse/HBASE-15797) | TestIPCUtil fails after HBASE-15795 |  Major | . |
| [HBASE-15236](https://issues.apache.org/jira/browse/HBASE-15236) | Inconsistent cell reads over multiple bulk-loaded HFiles |  Major | . |
| [HBASE-15807](https://issues.apache.org/jira/browse/HBASE-15807) | Update report-flakies.py to look for "FAILED" status in test report |  Minor | . |
| [HBASE-15738](https://issues.apache.org/jira/browse/HBASE-15738) | Ensure artifacts in project dist area include required md5 file |  Blocker | build, community |
| [HBASE-15796](https://issues.apache.org/jira/browse/HBASE-15796) | TestMetaCache fails after HBASE-15745 |  Major | . |
| [HBASE-15781](https://issues.apache.org/jira/browse/HBASE-15781) | Remove unused TableEventHandler and TotesHRegionInfo |  Trivial | master |
| [HBASE-15782](https://issues.apache.org/jira/browse/HBASE-15782) | TestShell fails due to some moved types |  Major | . |
| [HBASE-15669](https://issues.apache.org/jira/browse/HBASE-15669) | HFile size is not considered correctly in a replication request |  Major | Replication |
| [HBASE-15292](https://issues.apache.org/jira/browse/HBASE-15292) | Refined ZooKeeperWatcher to prevent ZooKeeper's callback while construction |  Minor | Zookeeper |
| [HBASE-15563](https://issues.apache.org/jira/browse/HBASE-15563) | 'counter' may overflow in BoundedGroupingStrategy |  Minor | wal |
| [HBASE-15755](https://issues.apache.org/jira/browse/HBASE-15755) | SnapshotDescriptionUtils and SnapshotTestingUtils do not have any Interface audience marked |  Major | . |
| [HBASE-15741](https://issues.apache.org/jira/browse/HBASE-15741) | Provide backward compatibility for HBase coprocessor service names |  Blocker | Coprocessors |
| [HBASE-15613](https://issues.apache.org/jira/browse/HBASE-15613) | TestNamespaceCommand times out |  Major | . |
| [HBASE-15528](https://issues.apache.org/jira/browse/HBASE-15528) | Clean up outdated entries in hbase-default.xml |  Minor | . |
| [HBASE-15752](https://issues.apache.org/jira/browse/HBASE-15752) | ClassNotFoundException is encountered when custom WAL codec is not found in WALPlayer job |  Major | . |
| [HBASE-15714](https://issues.apache.org/jira/browse/HBASE-15714) | We are calling checkRow() twice in doMiniBatchMutation() |  Major | regionserver |
| [HBASE-15703](https://issues.apache.org/jira/browse/HBASE-15703) | Deadline scheduler needs to return to the client info about skipped calls, not just drop them |  Critical | IPC/RPC |
| [HBASE-15357](https://issues.apache.org/jira/browse/HBASE-15357) | TableInputFormatBase getSplitKey does not handle signed bytes correctly |  Major | mapreduce |
| [HBASE-15711](https://issues.apache.org/jira/browse/HBASE-15711) | Add client side property to allow logging details for batch errors |  Major | . |
| [HBASE-15732](https://issues.apache.org/jira/browse/HBASE-15732) | hbase-rsgroups should be in the assembly |  Major | . |
| [HBASE-15676](https://issues.apache.org/jira/browse/HBASE-15676) | FuzzyRowFilter fails and matches all the rows in the table if the mask consists of all 0s |  Major | Filters |
| [HBASE-15697](https://issues.apache.org/jira/browse/HBASE-15697) | Excessive TestHRegion running time on branch-1 |  Major | . |
| [HBASE-15685](https://issues.apache.org/jira/browse/HBASE-15685) | Typo in REST documentation |  Minor | documentation |
| [HBASE-15708](https://issues.apache.org/jira/browse/HBASE-15708) | Docker for dev-support scripts |  Major | . |
| [HBASE-15707](https://issues.apache.org/jira/browse/HBASE-15707) | ImportTSV bulk output does not support tags with hfile.format.version=3 |  Major | mapreduce |
| [HBASE-15645](https://issues.apache.org/jira/browse/HBASE-15645) | hbase.rpc.timeout is not used in operations of HTable |  Critical | . |
| [HBASE-15634](https://issues.apache.org/jira/browse/HBASE-15634) | TestDateTieredCompactionPolicy#negativeForMajor is flaky |  Major | test |
| [HBASE-15710](https://issues.apache.org/jira/browse/HBASE-15710) | Include issue servers information in RetriesExhaustedWithDetailsException message |  Minor | . |
| [HBASE-14252](https://issues.apache.org/jira/browse/HBASE-14252) | RegionServers fail to start when setting hbase.ipc.server.callqueue.scan.ratio to 0 |  Major | regionserver |
| [HBASE-15699](https://issues.apache.org/jira/browse/HBASE-15699) | Can not sync AsyncFSWAL if no edit is appended |  Major | wal |
| [HBASE-15360](https://issues.apache.org/jira/browse/HBASE-15360) | Fix flaky TestSimpleRpcScheduler |  Critical | test |
| [HBASE-15670](https://issues.apache.org/jira/browse/HBASE-15670) | Add missing Snapshot.proto to the maven profile for compiling protobuf |  Major | . |
| [HBASE-15674](https://issues.apache.org/jira/browse/HBASE-15674) | HRegionLocator#getAllRegionLocations should put the results in cache |  Major | . |
| [HBASE-15672](https://issues.apache.org/jira/browse/HBASE-15672) | hadoop.hbase.security.visibility.TestVisibilityLabelsWithDeletes fails |  Major | test |
| [HBASE-15673](https://issues.apache.org/jira/browse/HBASE-15673) | [PE tool] Fix latency metrics for multiGet |  Major | Performance |
| [HBASE-14898](https://issues.apache.org/jira/browse/HBASE-14898) | Correct Bloom filter documentation in the book |  Minor | . |
| [HBASE-15664](https://issues.apache.org/jira/browse/HBASE-15664) | Use Long.MAX\_VALUE instead of HConstants.FOREVER in CompactionPolicy |  Major | Compaction |
| [HBASE-15668](https://issues.apache.org/jira/browse/HBASE-15668) | HFileReplicator$Copier fails to replicate other hfiles in the request when a hfile in not found in FS anywhere |  Trivial | Replication |
| [HBASE-15287](https://issues.apache.org/jira/browse/HBASE-15287) | mapreduce.RowCounter returns incorrect result with binary row key inputs |  Major | mapreduce, util |
| [HBASE-15187](https://issues.apache.org/jira/browse/HBASE-15187) | Integrate CSRF prevention filter to REST gateway |  Major | . |
| [HBASE-15650](https://issues.apache.org/jira/browse/HBASE-15650) | Remove TimeRangeTracker as point of contention when many threads reading a StoreFile |  Major | Performance |
| [HBASE-15622](https://issues.apache.org/jira/browse/HBASE-15622) | Superusers does not consider the keytab credentials |  Critical | security |
| [HBASE-15405](https://issues.apache.org/jira/browse/HBASE-15405) | Synchronize final results logging single thread in PE, fix wrong defaults in help message |  Minor | Performance |
| [HBASE-15504](https://issues.apache.org/jira/browse/HBASE-15504) | Fix Balancer in 1.3 not moving regions off overloaded regionserver |  Major | . |
| [HBASE-15637](https://issues.apache.org/jira/browse/HBASE-15637) | TSHA Thrift-2 server should allow limiting call queue size |  Major | Thrift |
| [HBASE-15639](https://issues.apache.org/jira/browse/HBASE-15639) | Unguarded access to stackIndexes in Procedure#toStringDetails() |  Minor | . |
| [HBASE-15636](https://issues.apache.org/jira/browse/HBASE-15636) | hard coded wait time out value in HBaseTestingUtility#waitUntilAllRegionsAssigned might cause test failure |  Minor | integration tests, test |
| [HBASE-15621](https://issues.apache.org/jira/browse/HBASE-15621) | Suppress Hbase SnapshotHFile cleaner error  messages when a snaphot is going on |  Minor | snapshots |
| [HBASE-15627](https://issues.apache.org/jira/browse/HBASE-15627) |  Miss space and closing quote in AccessController#checkSystemOrSuperUser |  Minor | security |
| [HBASE-15093](https://issues.apache.org/jira/browse/HBASE-15093) | Replication can report incorrect size of log queue for the global source when multiwal is enabled |  Minor | Replication |
| [HBASE-15591](https://issues.apache.org/jira/browse/HBASE-15591) | ServerCrashProcedure not yielding |  Major | . |
| [HBASE-15623](https://issues.apache.org/jira/browse/HBASE-15623) | Update refguide to change hadoop \<= 2.3.x from NT to X for hbase-1.2.x |  Major | documentation |
| [HBASE-15587](https://issues.apache.org/jira/browse/HBASE-15587) | FSTableDescriptors.getDescriptor() logs stack trace erronously |  Major | . |
| [HBASE-15485](https://issues.apache.org/jira/browse/HBASE-15485) | Filter.reset() should not be called between batches |  Major | . |
| [HBASE-15578](https://issues.apache.org/jira/browse/HBASE-15578) | Handle HBASE-15234 for ReplicationHFileCleaner |  Major | Replication |
| [HBASE-15582](https://issues.apache.org/jira/browse/HBASE-15582) | SnapshotManifestV1 too verbose when there are no regions |  Trivial | master, snapshots |
| [HBASE-15424](https://issues.apache.org/jira/browse/HBASE-15424) | Add bulk load hfile-refs for replication in ZK after the event is appended in the WAL |  Minor | Replication |
| [HBASE-15234](https://issues.apache.org/jira/browse/HBASE-15234) | ReplicationLogCleaner can abort due to transient ZK issues |  Critical | master, Replication |
| [HBASE-15567](https://issues.apache.org/jira/browse/HBASE-15567) | TestReplicationShell broken by recent replication changes |  Minor | Replication, shell |
| [HBASE-15324](https://issues.apache.org/jira/browse/HBASE-15324) | Jitter may cause desiredMaxFileSize overflow in ConstantSizeRegionSplitPolicy and trigger unexpected split |  Major | . |
| [HBASE-15566](https://issues.apache.org/jira/browse/HBASE-15566) | Add timeouts on TestMobFlushSnapshotFromClient and TestRegionMergeTransactionOnCluster |  Major | . |
| [HBASE-15559](https://issues.apache.org/jira/browse/HBASE-15559) | BaseMasterAndRegionObserver doesn't implement all the methods |  Major | . |
| [HBASE-15327](https://issues.apache.org/jira/browse/HBASE-15327) | Canary will always invoke admin.balancer() in each sniffing period when writeSniffing is enabled |  Minor | canary |
| [HBASE-15295](https://issues.apache.org/jira/browse/HBASE-15295) | MutateTableAccess.multiMutate() does not get high priority causing a deadlock |  Major | . |
| [HBASE-15515](https://issues.apache.org/jira/browse/HBASE-15515) | Improve LocalityBasedCandidateGenerator in Balancer |  Minor | . |
| [HBASE-14256](https://issues.apache.org/jira/browse/HBASE-14256) | Flush task message may be confusing when region is recovered |  Major | regionserver |
| [HBASE-15520](https://issues.apache.org/jira/browse/HBASE-15520) | Fix broken TestAsyncIPC |  Major | . |
| [HBASE-15064](https://issues.apache.org/jira/browse/HBASE-15064) | BufferUnderflowException after last Cell fetched from an HFile Block served from L2 offheap cache |  Critical | io |
| [HBASE-15325](https://issues.apache.org/jira/browse/HBASE-15325) | ResultScanner allowing partial result will miss the rest of the row if the region is moved between two rpc requests |  Critical | dataloss, Scanners |
| [HBASE-15433](https://issues.apache.org/jira/browse/HBASE-15433) | SnapshotManager#restoreSnapshot not update table and region count quota correctly when encountering exception |  Major | snapshots |
| [HBASE-15441](https://issues.apache.org/jira/browse/HBASE-15441) | Fix WAL splitting when region has moved multiple times |  Blocker | Recovery, wal |
| [HBASE-15430](https://issues.apache.org/jira/browse/HBASE-15430) | Failed taking snapshot - Manifest proto-message too large |  Critical | snapshots |
| [HBASE-15463](https://issues.apache.org/jira/browse/HBASE-15463) | Region normalizer should check whether split/merge is enabled |  Minor | . |
| [HBASE-15439](https://issues.apache.org/jira/browse/HBASE-15439) | getMaximumAllowedTimeBetweenRuns in ScheduledChore ignores the TimeUnit |  Major | master, mob, regionserver |
| [HBASE-15379](https://issues.apache.org/jira/browse/HBASE-15379) | Fake cells created in read path not implementing SettableSequenceId |  Major | . |
| [HBASE-15425](https://issues.apache.org/jira/browse/HBASE-15425) | Failing to write bulk load event marker in the WAL is ignored |  Major | . |
| [HBASE-15378](https://issues.apache.org/jira/browse/HBASE-15378) | Scanner cannot handle heartbeat message with no results |  Critical | dataloss, Scanners |
| [HBASE-15271](https://issues.apache.org/jira/browse/HBASE-15271) | Spark Bulk Load: Need to write HFiles to tmp location then rename to protect from Spark Executor Failures |  Major | . |
| [HBASE-15364](https://issues.apache.org/jira/browse/HBASE-15364) | Fix unescaped \< characters in Javadoc |  Major | API, documentation |
| [HBASE-15137](https://issues.apache.org/jira/browse/HBASE-15137) | CallTimeoutException and CallQueueTooBigException should trigger PFFE |  Major | . |
| [HBASE-15393](https://issues.apache.org/jira/browse/HBASE-15393) | Enable table replication command will fail when parent znode is not default in peer cluster |  Major | Replication |
| [HBASE-15397](https://issues.apache.org/jira/browse/HBASE-15397) | Create bulk load replication znode(hfile-refs) in ZK replication queue by default |  Major | Replication |
| [HBASE-15329](https://issues.apache.org/jira/browse/HBASE-15329) | Cross-Site Scripting: Reflected in table.jsp |  Minor | security, UI |
| [HBASE-15128](https://issues.apache.org/jira/browse/HBASE-15128) | Disable region splits and merges switch in master |  Major | . |
| [HBASE-15358](https://issues.apache.org/jira/browse/HBASE-15358) | canEnforceTimeLimitFromScope should use timeScope instead of sizeScope |  Major | Scanners |
| [HBASE-15332](https://issues.apache.org/jira/browse/HBASE-15332) | Document how to take advantage of HDFS-6133 in HBase |  Major | documentation |
| [HBASE-15319](https://issues.apache.org/jira/browse/HBASE-15319) | clearJmxCache does not take effect actually |  Major | metrics |
| [HBASE-15247](https://issues.apache.org/jira/browse/HBASE-15247) | InclusiveStopFilter does not respect reverse Filter property |  Major | Filters |
| [HBASE-15285](https://issues.apache.org/jira/browse/HBASE-15285) | Forward-port respect for isReturnResult from HBASE-15095 |  Major | Client |
| [HBASE-15298](https://issues.apache.org/jira/browse/HBASE-15298) | Fix missing or wrong asciidoc anchors in the reference guide |  Minor | documentation |
| [HBASE-15259](https://issues.apache.org/jira/browse/HBASE-15259) | WALEdits under replay will also be replicated |  Minor | . |
| [HBASE-15251](https://issues.apache.org/jira/browse/HBASE-15251) | During a cluster restart, Hmaster thinks it is a failover by mistake |  Major | master |
| [HBASE-15289](https://issues.apache.org/jira/browse/HBASE-15289) | Add details about how to get usage instructions for Import and Export utilities |  Major | documentation |
| [HBASE-13883](https://issues.apache.org/jira/browse/HBASE-13883) | Fix Memstore Flush section in HBase book |  Major | documentation |
| [HBASE-15250](https://issues.apache.org/jira/browse/HBASE-15250) | Fix links that are currently redirected from old URLs |  Major | documentation, website |
| [HBASE-15279](https://issues.apache.org/jira/browse/HBASE-15279) | OrderedBytes.isEncodedValue does not check for int8 and int16 types |  Major | . |
| [HBASE-15276](https://issues.apache.org/jira/browse/HBASE-15276) | TestFlushSnapshotFromClient hung |  Major | . |
| [HBASE-15198](https://issues.apache.org/jira/browse/HBASE-15198) | RPC client not using Codec and CellBlock for puts by default |  Critical | . |
| [HBASE-15122](https://issues.apache.org/jira/browse/HBASE-15122) | Servlets generate XSS\_REQUEST\_PARAMETER\_TO\_SERVLET\_WRITER findbugs warnings |  Critical | UI |
| [HBASE-15079](https://issues.apache.org/jira/browse/HBASE-15079) | TestMultiParallel.validateLoadedData AssertionError: null |  Major | Client, flakey, test |
| [HBASE-13839](https://issues.apache.org/jira/browse/HBASE-13839) | Fix AssgnmentManagerTmpl.jamon issues (coloring, content etc.) |  Major | master, UI |
| [HBASE-15252](https://issues.apache.org/jira/browse/HBASE-15252) | Data loss when replaying wal if HDFS timeout |  Blocker | wal |
| [HBASE-15221](https://issues.apache.org/jira/browse/HBASE-15221) | HTableMultiplexer improvements (stale region locations and resource leaks) |  Critical | Client |
| [HBASE-15253](https://issues.apache.org/jira/browse/HBASE-15253) | Small bug in CellUtil.matchingRow(Cell, byte[]) |  Minor | . |
| [HBASE-14192](https://issues.apache.org/jira/browse/HBASE-14192) | Fix REST Cluster constructor with String List |  Minor | REST |
| [HBASE-15216](https://issues.apache.org/jira/browse/HBASE-15216) | Canary does not accept config params from command line |  Major | canary |
| [HBASE-15231](https://issues.apache.org/jira/browse/HBASE-15231) | Make TableState.State private |  Major | API |
| [HBASE-15214](https://issues.apache.org/jira/browse/HBASE-15214) | Valid mutate Ops fail with RPC Codec in use and region moves across |  Critical | . |
| [HBASE-14770](https://issues.apache.org/jira/browse/HBASE-14770) | RowCounter argument input parse error |  Minor | mapreduce |
| [HBASE-15209](https://issues.apache.org/jira/browse/HBASE-15209) | disable table in HBaseTestingUtility.truncateTable |  Minor | . |
| [HBASE-15218](https://issues.apache.org/jira/browse/HBASE-15218) | On RS crash and replay of WAL, loosing all Tags in Cells |  Blocker | Recovery, regionserver, security |
| [HBASE-15200](https://issues.apache.org/jira/browse/HBASE-15200) | ZooKeeper znode ACL checks should only compare the shortname |  Minor | security |
| [HBASE-15206](https://issues.apache.org/jira/browse/HBASE-15206) | Flakey testSplitDaughtersNotInMeta test |  Minor | flakey |
| [HBASE-15196](https://issues.apache.org/jira/browse/HBASE-15196) | HBASE-15158 Preamble 2 of 2:Add Increment tests |  Major | . |
| [HBASE-15186](https://issues.apache.org/jira/browse/HBASE-15186) | HBASE-15158 Preamble 1 of 2: fix findbugs, add javadoc, change Region#getReadpoint to #getReadPoint, and some util |  Major | . |
| [HBASE-15190](https://issues.apache.org/jira/browse/HBASE-15190) | Monkey dies when running on shared cluster (gives up when can't kill the other fellows processes) |  Major | integration tests |
| [HBASE-15195](https://issues.apache.org/jira/browse/HBASE-15195) | Don't run findbugs on hbase-it; it has nothing in src/main/java |  Major | findbugs |
| [HBASE-14810](https://issues.apache.org/jira/browse/HBASE-14810) | Update Hadoop support description to explain "not tested" vs "not supported" |  Critical | documentation |
| [HBASE-15019](https://issues.apache.org/jira/browse/HBASE-15019) | Replication stuck when HDFS is restarted |  Major | Replication, wal |
| [HBASE-15173](https://issues.apache.org/jira/browse/HBASE-15173) | Execute mergeRegions RPC call as the request user |  Minor | . |
| [HBASE-15146](https://issues.apache.org/jira/browse/HBASE-15146) | Don't block on Reader threads queueing to a scheduler queue |  Blocker | . |
| [HBASE-15145](https://issues.apache.org/jira/browse/HBASE-15145) | HBCK and Replication should authenticate to zookepeer using server principal |  Major | . |
| [HBASE-15164](https://issues.apache.org/jira/browse/HBASE-15164) | Fix broken links found via LinkLint |  Major | documentation |
| [HBASE-15132](https://issues.apache.org/jira/browse/HBASE-15132) | Master region merge RPC should authorize user request |  Major | . |
| [HBASE-13082](https://issues.apache.org/jira/browse/HBASE-13082) | Coarsen StoreScanner locks to RegionScanner |  Major | Performance, Scanners |
| [HBASE-15148](https://issues.apache.org/jira/browse/HBASE-15148) | Resolve IS2\_INCONSISTENT\_SYNC findbugs warning in AuthenticationTokenSecretManager |  Major | . |
| [HBASE-15133](https://issues.apache.org/jira/browse/HBASE-15133) | Data loss after compaction when a row has more than Integer.MAX\_VALUE columns |  Major | Compaction |
| [HBASE-15152](https://issues.apache.org/jira/browse/HBASE-15152) | Automatically include prefix-tree module in MR jobs if present |  Major | mapreduce |
| [HBASE-15147](https://issues.apache.org/jira/browse/HBASE-15147) | Shell should use Admin.listTableNames() instead of Admin.listTables() |  Major | . |
| [HBASE-15126](https://issues.apache.org/jira/browse/HBASE-15126) | HBaseFsck's checkRegionBoundaries function sets incorrect 'storesFirstKey' |  Major | hbck |
| [HBASE-15139](https://issues.apache.org/jira/browse/HBASE-15139) | Connection manager doesn't pass client metrics to RpcClient |  Major | Client, metrics |
| [HBASE-15098](https://issues.apache.org/jira/browse/HBASE-15098) | Normalizer switch in configuration is not used |  Blocker | master |
| [HBASE-15101](https://issues.apache.org/jira/browse/HBASE-15101) | Leaked References to StoreFile.Reader after HBASE-13082 |  Critical | HFile, io |
| [HBASE-15102](https://issues.apache.org/jira/browse/HBASE-15102) | HeapMemoryTuner can "overtune" memstore size and suddenly drop it into blocking zone |  Critical | regionserver |
| [HBASE-14771](https://issues.apache.org/jira/browse/HBASE-14771) | RpcServer#getRemoteAddress always returns null |  Minor | IPC/RPC |
| [HBASE-14512](https://issues.apache.org/jira/browse/HBASE-14512) | Cache UGI groups |  Major | Performance, security |
| [HBASE-15104](https://issues.apache.org/jira/browse/HBASE-15104) | Occasional failures due to NotServingRegionException in IT tests |  Major | integration tests |
| [HBASE-15117](https://issues.apache.org/jira/browse/HBASE-15117) | Resolve ICAST findbugs warnings in current codes |  Minor | . |
| [HBASE-15085](https://issues.apache.org/jira/browse/HBASE-15085) | IllegalStateException was thrown when scanning on bulkloaded HFiles |  Critical | . |
| [HBASE-15083](https://issues.apache.org/jira/browse/HBASE-15083) | Gets from Multiactions are not counted in metrics for gets. |  Major | . |
| [HBASE-15052](https://issues.apache.org/jira/browse/HBASE-15052) | Use EnvironmentEdgeManager in ReplicationSource |  Trivial | Replication |
| [HBASE-15057](https://issues.apache.org/jira/browse/HBASE-15057) | local-master-backup.sh doesn't start HMaster correctly |  Major | shell |
| [HBASE-15065](https://issues.apache.org/jira/browse/HBASE-15065) | SimpleRegionNormalizer should return multiple normalization plans in one run |  Major | . |
| [HBASE-14975](https://issues.apache.org/jira/browse/HBASE-14975) | Don't color the total RIT line yellow if it's zero |  Major | UI |
| [HBASE-15027](https://issues.apache.org/jira/browse/HBASE-15027) | Refactor the way the CompactedHFileDischarger threads are created |  Major | . |
| [HBASE-15070](https://issues.apache.org/jira/browse/HBASE-15070) | DistributedHBaseCluster#restoreRegionServers() starts new RS process on master server |  Minor | integration tests |
| [HBASE-14867](https://issues.apache.org/jira/browse/HBASE-14867) | SimpleRegionNormalizer needs to have better heuristics to trigger merge operation |  Major | master |
| [HBASE-14987](https://issues.apache.org/jira/browse/HBASE-14987) | Compaction marker whose region name doesn't match current region's needs to be handled |  Major | . |
| [HBASE-15063](https://issues.apache.org/jira/browse/HBASE-15063) | Bug in MultiByteBuf#toBytes |  Critical | io, Performance |
| [HBASE-15050](https://issues.apache.org/jira/browse/HBASE-15050) | Block Ref counting does not work in Region Split cases. |  Critical | . |
| [HBASE-15011](https://issues.apache.org/jira/browse/HBASE-15011) | turn off the jdk8 javadoc linter. :( |  Blocker | build, documentation |
| [HBASE-15043](https://issues.apache.org/jira/browse/HBASE-15043) | region\_status.rb broken with TypeError: no public constructors for Java::OrgApacheHadoopHbaseClient::HBaseAdmin |  Major | scripts |
| [HBASE-15026](https://issues.apache.org/jira/browse/HBASE-15026) | The default value of "hbase.regions.slop" in hbase-default.xml is obsolete |  Minor | Balancer |
| [HBASE-15035](https://issues.apache.org/jira/browse/HBASE-15035) | bulkloading hfiles with tags that require splits do not preserve tags |  Blocker | HFile |
| [HBASE-15039](https://issues.apache.org/jira/browse/HBASE-15039) | HMaster and RegionServers should try to refresh token keys from zk when facing InvalidToken |  Major | . |
| [HBASE-14940](https://issues.apache.org/jira/browse/HBASE-14940) | Make our unsafe based ops more safe |  Major | . |
| [HBASE-14717](https://issues.apache.org/jira/browse/HBASE-14717) | enable\_table\_replication command should only create specified table for a peer cluster |  Major | Replication |
| [HBASE-15032](https://issues.apache.org/jira/browse/HBASE-15032) | hbase shell scan filter string assumes UTF-8 encoding |  Major | shell |
| [HBASE-15034](https://issues.apache.org/jira/browse/HBASE-15034) | IntegrationTestDDLMasterFailover does not clean created namespaces |  Minor | integration tests |
| [HBASE-15030](https://issues.apache.org/jira/browse/HBASE-15030) | Deadlock in master TableNamespaceManager while running IntegrationTestDDLMasterFailover |  Major | proc-v2 |
| [HBASE-15021](https://issues.apache.org/jira/browse/HBASE-15021) | hadoopqa doing false positives |  Major | . |
| [HBASE-15028](https://issues.apache.org/jira/browse/HBASE-15028) | Minor fix on RegionGroupingProvider |  Minor | wal |
| [HBASE-14977](https://issues.apache.org/jira/browse/HBASE-14977) | ChoreService.shutdown may result in ConcurrentModificationException |  Minor | util |
| [HBASE-15014](https://issues.apache.org/jira/browse/HBASE-15014) | Fix filterCellByStore in WALsplitter is awful for performance |  Critical | MTTR, Recovery, wal |
| [HBASE-15001](https://issues.apache.org/jira/browse/HBASE-15001) | Thread Safety issues in ReplicationSinkManager and HBaseInterClusterReplicationEndpoint |  Blocker | Replication |
| [HBASE-14654](https://issues.apache.org/jira/browse/HBASE-14654) | Reenable TestMultiParallel#testActiveThreadsCount |  Major | test |
| [HBASE-15022](https://issues.apache.org/jira/browse/HBASE-15022) | undefined method \`getZooKeeperClusterKey' for Java::OrgApacheHadoopHbaseZookeeper::ZKUtil:Class |  Major | shell |
| [HBASE-14991](https://issues.apache.org/jira/browse/HBASE-14991) | Fix the feature warning in scala code |  Minor | . |
| [HBASE-15015](https://issues.apache.org/jira/browse/HBASE-15015) | Checktyle plugin shouldn't check Jamon-generated Java classes |  Minor | build |
| [HBASE-14822](https://issues.apache.org/jira/browse/HBASE-14822) | Renewing leases of scanners doesn't work |  Major | . |
| [HBASE-13907](https://issues.apache.org/jira/browse/HBASE-13907) | Document how to deploy a coprocessor |  Major | documentation |
| [HBASE-14990](https://issues.apache.org/jira/browse/HBASE-14990) | Tests in BaseTestHBaseFsck are run by its subclasses redundantly |  Minor | test |
| [HBASE-14999](https://issues.apache.org/jira/browse/HBASE-14999) | Remove ref to org.mortbay.log.Log |  Minor | dependencies |
| [HBASE-15000](https://issues.apache.org/jira/browse/HBASE-15000) | Fix javadoc warn in LoadIncrementalHFiles |  Trivial | . |
| [HBASE-14974](https://issues.apache.org/jira/browse/HBASE-14974) | Total number of Regions in Transition number on UI incorrect |  Trivial | UI |
| [HBASE-14952](https://issues.apache.org/jira/browse/HBASE-14952) | hbase-assembly source artifact has some incorrect modules |  Blocker | build, dependencies |
| [HBASE-14807](https://issues.apache.org/jira/browse/HBASE-14807) | TestWALLockup is flakey |  Major | flakey, test |
| [HBASE-14843](https://issues.apache.org/jira/browse/HBASE-14843) | TestWALProcedureStore.testLoad is flakey |  Blocker | proc-v2 |
| [HBASE-14838](https://issues.apache.org/jira/browse/HBASE-14838) | Clarify that SimpleRegionNormalizer does not merge empty (\<1MB) regions |  Trivial | documentation, regionserver |
| [HBASE-13976](https://issues.apache.org/jira/browse/HBASE-13976) | release manager list in ref guide is missing 0.94 line |  Major | documentation |
| [HBASE-14968](https://issues.apache.org/jira/browse/HBASE-14968) | ConcurrentModificationException in region close resulting in the region staying in closing state |  Major | Region Assignment, regionserver |
| [HBASE-14929](https://issues.apache.org/jira/browse/HBASE-14929) | There is a space missing from Table "foo" is not currently available. |  Trivial | . |
| [HBASE-14936](https://issues.apache.org/jira/browse/HBASE-14936) | CombinedBlockCache should overwrite CacheStats#rollMetricsPeriod() |  Major | BlockCache |
| [HBASE-14701](https://issues.apache.org/jira/browse/HBASE-14701) | Fix flakey Failed tests:    TestMobFlushSnapshotFromClient\>TestFlushSnapshotFromClient.testSkipFlushTableSnapshot:199 null |  Major | test |
| [HBASE-14953](https://issues.apache.org/jira/browse/HBASE-14953) | HBaseInterClusterReplicationEndpoint: Do not retry the whole batch of edits in case of RejectedExecutionException |  Critical | Replication |
| [HBASE-14960](https://issues.apache.org/jira/browse/HBASE-14960) | Fallback to using default RPCControllerFactory if class cannot be loaded |  Major | . |
| [HBASE-14769](https://issues.apache.org/jira/browse/HBASE-14769) | Remove unused functions and duplicate javadocs from HBaseAdmin |  Major | . |
| [HBASE-14901](https://issues.apache.org/jira/browse/HBASE-14901) | There is duplicated code to create/manage encryption keys |  Minor | encryption |
| [HBASE-14941](https://issues.apache.org/jira/browse/HBASE-14941) | locate\_region shell command |  Trivial | shell |
| [HBASE-14942](https://issues.apache.org/jira/browse/HBASE-14942) | Allow turning off BoundedByteBufferPool |  Major | . |
| [HBASE-14954](https://issues.apache.org/jira/browse/HBASE-14954) | IllegalArgumentException was thrown when doing online configuration change in CompactSplitThread |  Major | Compaction, regionserver |
| [HBASE-14804](https://issues.apache.org/jira/browse/HBASE-14804) | HBase shell's create table command ignores 'NORMALIZATION\_ENABLED' attribute |  Minor | shell |
| [HBASE-14917](https://issues.apache.org/jira/browse/HBASE-14917) | Log in console if individual tests in test-patch.sh fail or pass |  Minor | . |
| [HBASE-14930](https://issues.apache.org/jira/browse/HBASE-14930) | check\_compatibility.sh needs smarter exit codes |  Major | . |
| [HBASE-14923](https://issues.apache.org/jira/browse/HBASE-14923) | VerifyReplication should not mask the exception during result comparison |  Minor | tooling |
| [HBASE-14922](https://issues.apache.org/jira/browse/HBASE-14922) | Delayed flush doesn't work causing flush storms. |  Major | regionserver |
| [HBASE-14926](https://issues.apache.org/jira/browse/HBASE-14926) | Hung ThriftServer; no timeout on read from client; if client crashes, worker thread gets stuck reading |  Major | Thrift |
| [HBASE-14928](https://issues.apache.org/jira/browse/HBASE-14928) | Start row should be set for query through HBase REST gateway involving globbing option |  Major | REST |
| [HBASE-14907](https://issues.apache.org/jira/browse/HBASE-14907) | NPE of MobUtils.hasMobColumns in Build failed in Jenkins: HBase-Trunk\_matrix » latest1.8,Hadoop #513 |  Major | mob |
| [HBASE-14904](https://issues.apache.org/jira/browse/HBASE-14904) | Mark Base[En\|De]coder LimitedPrivate and fix binary compat issue |  Major | . |
| [HBASE-13857](https://issues.apache.org/jira/browse/HBASE-13857) | Slow WAL Append count in ServerMetricsTmpl.jamon is hardcoded to zero |  Major | regionserver, UI |
| [HBASE-14905](https://issues.apache.org/jira/browse/HBASE-14905) | VerifyReplication does not honour versions option |  Major | tooling |
| [HBASE-14541](https://issues.apache.org/jira/browse/HBASE-14541) | TestHFileOutputFormat.testMRIncrementalLoadWithSplit failed due to too many splits and few retries |  Major | HFile, test |
| [HBASE-14462](https://issues.apache.org/jira/browse/HBASE-14462) | rolling\_restart.sh --master-only throws "line 142: test: 0: unary operator expected" |  Major | . |
| [HBASE-14896](https://issues.apache.org/jira/browse/HBASE-14896) | Resolve Javadoc warnings in WALKey and RegionMover |  Minor | . |
| [HBASE-14531](https://issues.apache.org/jira/browse/HBASE-14531) | graceful\_stop.sh "if [ "$local" ]" condition unexpected behaviour |  Major | scripts |
| [HBASE-14890](https://issues.apache.org/jira/browse/HBASE-14890) | Fix javadoc checkstyle errors |  Major | . |
| [HBASE-14894](https://issues.apache.org/jira/browse/HBASE-14894) | Fix misspellings of threshold in log4j.properties files for tests |  Trivial | . |
| [HBASE-14523](https://issues.apache.org/jira/browse/HBASE-14523) | rolling-restart.sh --graceful will start regionserver process on master node |  Major | scripts |
| [HBASE-14893](https://issues.apache.org/jira/browse/HBASE-14893) | Race between mutation on region and region closing operation leads to NotServingRegionException |  Major | . |
| [HBASE-14664](https://issues.apache.org/jira/browse/HBASE-14664) | Master failover issue: Backup master is unable to start if active master is killed and started in short time interval |  Critical | master |
| [HBASE-14777](https://issues.apache.org/jira/browse/HBASE-14777) | Fix Inter Cluster Replication Future ordering issues |  Critical | Replication |
| [HBASE-14885](https://issues.apache.org/jira/browse/HBASE-14885) | NullPointerException in HMaster#normalizeRegions() due to missing TableDescriptor |  Major | master |
| [HBASE-14689](https://issues.apache.org/jira/browse/HBASE-14689) | Addendum and unit test for HBASE-13471 |  Major | . |
| [HBASE-14463](https://issues.apache.org/jira/browse/HBASE-14463) | Severe performance downgrade when parallel reading a single key from BucketCache |  Major | . |
| [HBASE-14861](https://issues.apache.org/jira/browse/HBASE-14861) | HBASE\_ZNODE\_FILE on master server is overwritten by regionserver process in case of master-rs collocation |  Major | Operability |
| [HBASE-14825](https://issues.apache.org/jira/browse/HBASE-14825) | HBase Ref Guide corrections of typos/misspellings |  Minor | documentation |
| [HBASE-14737](https://issues.apache.org/jira/browse/HBASE-14737) | Clear cachedMaxVersions when HColumnDescriptor#setValue(VERSIONS, value) is called |  Major | . |
| [HBASE-14799](https://issues.apache.org/jira/browse/HBASE-14799) | Commons-collections object deserialization remote command execution vulnerability |  Critical | dependencies, security |
| [HBASE-14840](https://issues.apache.org/jira/browse/HBASE-14840) | Sink cluster reports data replication request as success though the data is not replicated |  Major | Replication |
| [HBASE-14712](https://issues.apache.org/jira/browse/HBASE-14712) | MasterProcWALs never clean up |  Blocker | proc-v2 |
| [HBASE-14761](https://issues.apache.org/jira/browse/HBASE-14761) | Deletes with and without visibility expression do not delete the matching mutation |  Major | security |
| [HBASE-14782](https://issues.apache.org/jira/browse/HBASE-14782) | FuzzyRowFilter skips valid rows |  Major | Filters |
| [HBASE-14824](https://issues.apache.org/jira/browse/HBASE-14824) | HBaseAdmin.mergeRegions should recognize both full region names and encoded region names |  Minor | Admin |
| [HBASE-14815](https://issues.apache.org/jira/browse/HBASE-14815) | TestMobExportSnapshot.testExportFailure timeout occasionally |  Major | . |
| [HBASE-14823](https://issues.apache.org/jira/browse/HBASE-14823) | HBase Ref Guide Refactoring |  Major | documentation |
| [HBASE-14812](https://issues.apache.org/jira/browse/HBASE-14812) | Fix ResultBoundedCompletionService deadlock |  Major | Client, Thrift |
| [HBASE-14793](https://issues.apache.org/jira/browse/HBASE-14793) | Allow limiting size of block into L1 block cache. |  Major | BlockCache |
| [HBASE-14806](https://issues.apache.org/jira/browse/HBASE-14806) | Missing sources.jar for several modules when building HBase |  Major | pom |
| [HBASE-14809](https://issues.apache.org/jira/browse/HBASE-14809) | Grant / revoke Namespace admin permission to group |  Major | security |
| [HBASE-14802](https://issues.apache.org/jira/browse/HBASE-14802) | Replaying server crash recovery procedure after a failover causes incorrect handling of deadservers |  Major | master |
| [HBASE-13982](https://issues.apache.org/jira/browse/HBASE-13982) | Add info for visibility labels/cell TTLs to ImportTsv |  Major | mapreduce |
| [HBASE-14797](https://issues.apache.org/jira/browse/HBASE-14797) | Last round of CSS fix-ups |  Major | website |
| [HBASE-14788](https://issues.apache.org/jira/browse/HBASE-14788) | Splitting a region does not support the hbase.rs.evictblocksonclose config when closing source region |  Major | regionserver |
| [HBASE-14778](https://issues.apache.org/jira/browse/HBASE-14778) | Make block cache hit percentages not integer in the metrics system |  Major | BlockCache, metrics |
| [HBASE-14784](https://issues.apache.org/jira/browse/HBASE-14784) | Port conflict is not resolved in HBaseTestingUtility.randomFreePort() |  Minor | test |
| [HBASE-14787](https://issues.apache.org/jira/browse/HBASE-14787) | Remove obsolete ConnectionImplementation.refCount |  Trivial | . |
| [HBASE-14781](https://issues.apache.org/jira/browse/HBASE-14781) | Turn per cf flushing on for ITBLL by default |  Major | integration tests |
| [HBASE-14774](https://issues.apache.org/jira/browse/HBASE-14774) | Raise the font size on high-DPI small-screen devices like iphone 6+ |  Major | website |
| [HBASE-14767](https://issues.apache.org/jira/browse/HBASE-14767) | Remove deprecated functions from HBaseAdmin |  Major | . |
| [HBASE-14759](https://issues.apache.org/jira/browse/HBASE-14759) | Avoid using Math.abs when selecting SyncRunner in FSHLog |  Major | wal |
| [HBASE-14632](https://issues.apache.org/jira/browse/HBASE-14632) | Region server aborts due to unguarded dereference of Reader |  Major | regionserver |
| [HBASE-14706](https://issues.apache.org/jira/browse/HBASE-14706) | RegionLocationFinder should return multiple servernames by top host |  Major | Balancer |
| [HBASE-14755](https://issues.apache.org/jira/browse/HBASE-14755) | Fix some broken links and HTML problems |  Major | documentation |
| [HBASE-14773](https://issues.apache.org/jira/browse/HBASE-14773) | Fix HBase shell tests are skipped when skipping server tests. |  Major | test |
| [HBASE-14768](https://issues.apache.org/jira/browse/HBASE-14768) | bin/graceful\_stop.sh logs nothing as a balancer state to be stored |  Trivial | scripts |
| [HBASE-14723](https://issues.apache.org/jira/browse/HBASE-14723) | Fix IT tests split too many times |  Major | integration tests |
| [HBASE-14733](https://issues.apache.org/jira/browse/HBASE-14733) | Minor typo in alter\_namespace.rb |  Trivial | shell |
| [HBASE-14754](https://issues.apache.org/jira/browse/HBASE-14754) | TestFastFailWithoutTestUtil failing on trunk now in #testPreemptiveFastFailException50Times |  Major | flakey, test |
| [HBASE-14751](https://issues.apache.org/jira/browse/HBASE-14751) | Book seems to be broken |  Major | . |
| [HBASE-14742](https://issues.apache.org/jira/browse/HBASE-14742) | TestHeapMemoryManager is flakey |  Major | test |
| [HBASE-14739](https://issues.apache.org/jira/browse/HBASE-14739) | Fix broken link to Javadoc that is suppressed because it is generated  code |  Trivial | documentation |
| [HBASE-14532](https://issues.apache.org/jira/browse/HBASE-14532) | [book] dfs.client.read.shortcircuit is referenced as hbase-site.xml config and not described in section 7 |  Minor | documentation |
| [HBASE-14711](https://issues.apache.org/jira/browse/HBASE-14711) | Remove or annotated deprecated methods in HRegionInfo |  Major | hbase |
| [HBASE-14557](https://issues.apache.org/jira/browse/HBASE-14557) | MapReduce WALPlayer issue with NoTagsKeyValue |  Blocker | tooling |
| [HBASE-14650](https://issues.apache.org/jira/browse/HBASE-14650) | Reenable TestNamespaceAuditor |  Major | . |
| [HBASE-14660](https://issues.apache.org/jira/browse/HBASE-14660) | AssertionError found when using offheap BucketCache with assertion enabled |  Major | . |
| [HBASE-14695](https://issues.apache.org/jira/browse/HBASE-14695) | Fix some easy HTML warnings |  Minor | documentation |
| [HBASE-14425](https://issues.apache.org/jira/browse/HBASE-14425) | In Secure Zookeeper cluster superuser will not have sufficient permission if multiple values are configured in "hbase.superuser" |  Major | security, Zookeeper |
| [HBASE-14674](https://issues.apache.org/jira/browse/HBASE-14674) | Rpc handler / task monitoring seems to be broken after 0.98 |  Major | . |
| [HBASE-14705](https://issues.apache.org/jira/browse/HBASE-14705) | Javadoc for KeyValue constructor is not correct. |  Minor | . |
| [HBASE-14680](https://issues.apache.org/jira/browse/HBASE-14680) | Two configs for snapshot timeout and better defaults |  Major | snapshots |
| [HBASE-14349](https://issues.apache.org/jira/browse/HBASE-14349) | pre-commit zombie finder is overly broad |  Critical | build |
| [HBASE-14694](https://issues.apache.org/jira/browse/HBASE-14694) | Scan copy constructor doesn't handle allowPartialResults |  Major | Client |
| [HBASE-14257](https://issues.apache.org/jira/browse/HBASE-14257) | Periodic flusher only handles hbase:meta, not other system tables |  Major | regionserver |
| [HBASE-14682](https://issues.apache.org/jira/browse/HBASE-14682) | CM restore functionality for regionservers is broken |  Major | integration tests |
| [HBASE-14283](https://issues.apache.org/jira/browse/HBASE-14283) | Reverse scan doesn’t work with HFile inline index/bloom blocks |  Major | . |
| [HBASE-13318](https://issues.apache.org/jira/browse/HBASE-13318) | RpcServer.getListenerAddress should handle when the accept channel is closed |  Minor | . |
| [HBASE-14661](https://issues.apache.org/jira/browse/HBASE-14661) | RegionServer link is not opening, in HBase Table page. |  Minor | UI |
| [HBASE-14690](https://issues.apache.org/jira/browse/HBASE-14690) | Fix css so there's no left/right scroll bar |  Major | UI |
| [HBASE-14624](https://issues.apache.org/jira/browse/HBASE-14624) | BucketCache.freeBlock is too expensive |  Major | BlockCache |
| [HBASE-14676](https://issues.apache.org/jira/browse/HBASE-14676) | HBaseTestCase clean out: Purge Incommon Interface and Table and Region implementations |  Major | . |
| [HBASE-14343](https://issues.apache.org/jira/browse/HBASE-14343) | Fix debug message in SimpleRegionNormalizer for small regions |  Trivial | regionserver |
| [HBASE-14681](https://issues.apache.org/jira/browse/HBASE-14681) | Upgrade Checkstyle plugin to 2.16 |  Major | build |
| [HBASE-14667](https://issues.apache.org/jira/browse/HBASE-14667) | HBaseFsck constructors have diverged |  Minor | hbck |
| [HBASE-14658](https://issues.apache.org/jira/browse/HBASE-14658) | Allow loading a MonkeyFactory by class name |  Major | integration tests, test |
| [HBASE-14603](https://issues.apache.org/jira/browse/HBASE-14603) | Lots of work on the POM to enhance Javadocs, Xrefs |  Major | documentation |
| [HBASE-14326](https://issues.apache.org/jira/browse/HBASE-14326) | HBase book: fix definition of max min size to compact |  Major | documentation |
| [HBASE-14663](https://issues.apache.org/jira/browse/HBASE-14663) | HStore::close does not honor config hbase.rs.evictblocksonclose |  Minor | BlockCache, regionserver |
| [HBASE-14427](https://issues.apache.org/jira/browse/HBASE-14427) | Fix 'should' assertions in TestFastFail |  Minor | test |
| [HBASE-14633](https://issues.apache.org/jira/browse/HBASE-14633) | Try fluid width UI |  Major | UI |
| [HBASE-14366](https://issues.apache.org/jira/browse/HBASE-14366) | NPE in case visibility expression is not present in labels table during importtsv run |  Minor | . |
| [HBASE-14604](https://issues.apache.org/jira/browse/HBASE-14604) | Improve MoveCostFunction in StochasticLoadBalancer |  Major | Balancer |
| [HBASE-14606](https://issues.apache.org/jira/browse/HBASE-14606) | TestSecureLoadIncrementalHFiles tests timed out in trunk build on apache |  Major | test |
| [HBASE-12558](https://issues.apache.org/jira/browse/HBASE-12558) | Disable TestHCM.testClusterStatus Unexpected exception, expected\<org.apache.hadoop.hbase.regionserver.RegionServerStoppedException\> but was\<junit.framework.AssertionFailedError\> |  Major | test |
| [HBASE-14458](https://issues.apache.org/jira/browse/HBASE-14458) | AsyncRpcClient#createRpcChannel() should check and remove dead channel before creating new one to same server |  Critical | IPC/RPC |
| [HBASE-14634](https://issues.apache.org/jira/browse/HBASE-14634) | Disable flakey TestSnapshotCloneIndependence.testOnlineSnapshotDeleteIndependent |  Major | test |
| [HBASE-14621](https://issues.apache.org/jira/browse/HBASE-14621) | ReplicationLogCleaner gets stuck when a regionserver crashes |  Critical | Replication |
| [HBASE-14625](https://issues.apache.org/jira/browse/HBASE-14625) | Chaos Monkey should shut down faster |  Major | integration tests, test |
| [HBASE-14597](https://issues.apache.org/jira/browse/HBASE-14597) | Fix Groups cache in multi-threaded env |  Major | . |
| [HBASE-14474](https://issues.apache.org/jira/browse/HBASE-14474) | DeadLock in RpcClientImpl.Connection.close() |  Blocker | IPC/RPC |
| [HBASE-14594](https://issues.apache.org/jira/browse/HBASE-14594) | Use new DNS API introduced in HADOOP-12437 |  Major | util |
| [HBASE-14521](https://issues.apache.org/jira/browse/HBASE-14521) | Unify the semantic of hbase.client.retries.number |  Major | . |
| [HBASE-14608](https://issues.apache.org/jira/browse/HBASE-14608) | testWalRollOnLowReplication has some risk to assert failed after HBASE-14600 |  Major | test |
| [HBASE-14598](https://issues.apache.org/jira/browse/HBASE-14598) | ByteBufferOutputStream grows its HeapByteBuffer beyond JVM limitations |  Major | Client, io |
| [HBASE-14591](https://issues.apache.org/jira/browse/HBASE-14591) | Region with reference hfile may split after a forced split in IncreasingToUpperBoundRegionSplitPolicy |  Critical | regionserver |
| [HBASE-14599](https://issues.apache.org/jira/browse/HBASE-14599) | Modify site config to use protocol-relative URLs for CSS/JS |  Blocker | documentation |
| [HBASE-14501](https://issues.apache.org/jira/browse/HBASE-14501) | NPE in replication when HDFS transparent encryption is enabled. |  Critical | Replication, security |
| [HBASE-14211](https://issues.apache.org/jira/browse/HBASE-14211) | Add more rigorous integration tests of splits |  Major | integration tests, test |
| [HBASE-14592](https://issues.apache.org/jira/browse/HBASE-14592) | BatchRestartRsAction always restarts 0 RS when running SlowDeterministicMonkey |  Major | integration tests |
| [HBASE-14577](https://issues.apache.org/jira/browse/HBASE-14577) | HBase shell help for scan and returning a column family has a typo |  Trivial | shell |
| [HBASE-14578](https://issues.apache.org/jira/browse/HBASE-14578) | URISyntaxException during snapshot restore for table with user defined namespace |  Major | snapshots |
| [HBASE-14581](https://issues.apache.org/jira/browse/HBASE-14581) | Znode cleanup throws auth exception in secure mode |  Major | security, Zookeeper |
| [HBASE-13858](https://issues.apache.org/jira/browse/HBASE-13858) | RS/MasterDumpServlet dumps threads before its “Stacks” header |  Trivial | master, regionserver, UI |
| [HBASE-14525](https://issues.apache.org/jira/browse/HBASE-14525) | Append and increment operation throws NullPointerException on non-existing column families. |  Minor | shell |
| [HBASE-14497](https://issues.apache.org/jira/browse/HBASE-14497) | Reverse Scan threw StackOverflow caused by readPt checking |  Major | . |
| [HBASE-14346](https://issues.apache.org/jira/browse/HBASE-14346) | Typo in FamilyFilter |  Trivial | documentation |
| [HBASE-12983](https://issues.apache.org/jira/browse/HBASE-12983) | HBase book mentions hadoop.ssl.enabled when it should be hbase.ssl.enabled |  Major | documentation |
| [HBASE-12615](https://issues.apache.org/jira/browse/HBASE-12615) | Document GC conserving guidelines for contributors |  Major | documentation |
| [HBASE-14555](https://issues.apache.org/jira/browse/HBASE-14555) | Deadlock in MVCC branch-1.2 toString() |  Critical | regionserver |
| [HBASE-14347](https://issues.apache.org/jira/browse/HBASE-14347) | Add a switch to DynamicClassLoader to disable it |  Major | Client, defaults, regionserver |
| [HBASE-14544](https://issues.apache.org/jira/browse/HBASE-14544) | Allow HConnectionImpl to not refresh the dns on errors |  Major | Client |
| [HBASE-14367](https://issues.apache.org/jira/browse/HBASE-14367) | Add normalization support to shell |  Major | Balancer, shell |
| [HBASE-14545](https://issues.apache.org/jira/browse/HBASE-14545) | TestMasterFailover often times out |  Major | test |
| [HBASE-14499](https://issues.apache.org/jira/browse/HBASE-14499) | Master coprocessors shutdown will not happen on master abort |  Major | master |
| [HBASE-13770](https://issues.apache.org/jira/browse/HBASE-13770) | Programmatic JAAS configuration option for secure zookeeper may be broken |  Major | Operability, security |
| [HBASE-14292](https://issues.apache.org/jira/browse/HBASE-14292) | Call Me Maybe HBase links haved moved |  Minor | documentation |
| [HBASE-13744](https://issues.apache.org/jira/browse/HBASE-13744) | TestCorruptedRegionStoreFile is flaky |  Major | test |
| [HBASE-13143](https://issues.apache.org/jira/browse/HBASE-13143) | TestCacheOnWrite is flaky and needs a diet |  Critical | test |
| [HBASE-14475](https://issues.apache.org/jira/browse/HBASE-14475) | Region split requests are always audited with "hbase" user rather than request user |  Major | regionserver, security |
| [HBASE-14494](https://issues.apache.org/jira/browse/HBASE-14494) | Wrong usage messages on shell commands |  Minor | shell |
| [HBASE-14518](https://issues.apache.org/jira/browse/HBASE-14518) | Give TestScanEarlyTermination the same treatment as 'HBASE-14378 Get TestAccessController\* passing again...' -- up priority handlers |  Major | test |
| [HBASE-14394](https://issues.apache.org/jira/browse/HBASE-14394) | Properly close the connection after reading records from table. |  Minor | mapreduce |
| [HBASE-14510](https://issues.apache.org/jira/browse/HBASE-14510) | Can not set coprocessor from Shell after HBASE-14224 |  Major | Coprocessors, shell |
| [HBASE-14362](https://issues.apache.org/jira/browse/HBASE-14362) | org.apache.hadoop.hbase.master.procedure.TestWALProcedureStoreOnHDFS is super duper flaky |  Critical | test |
| [HBASE-14473](https://issues.apache.org/jira/browse/HBASE-14473) | Compute region locality in parallel |  Major | Balancer |
| [HBASE-14437](https://issues.apache.org/jira/browse/HBASE-14437) | ArithmeticException in ReplicationInterClusterEndpoint |  Minor | Replication |
| [HBASE-14500](https://issues.apache.org/jira/browse/HBASE-14500) | Remove load of deprecated MOB ruby scripts after HBASE-14227 |  Major | shell |
| [HBASE-14489](https://issues.apache.org/jira/browse/HBASE-14489) | postScannerFilterRow consumes a lot of CPU |  Major | Coprocessors, Performance |
| [HBASE-14370](https://issues.apache.org/jira/browse/HBASE-14370) | Use separate thread for calling ZKPermissionWatcher#refreshNodes() |  Major | . |
| [HBASE-14492](https://issues.apache.org/jira/browse/HBASE-14492) | Increase REST server header buffer size from 8k to 64k |  Major | REST |
| [HBASE-14471](https://issues.apache.org/jira/browse/HBASE-14471) | Thrift -  HTTP Error 413 full HEAD if using kerberos authentication |  Major | Thrift |
| [HBASE-14469](https://issues.apache.org/jira/browse/HBASE-14469) | Fix some comment, validation and logging around memstore lower limit configuration |  Minor | . |
| [HBASE-14486](https://issues.apache.org/jira/browse/HBASE-14486) | Disable TestRegionPlacement, a flakey test for an unfinished feature |  Major | test |
| [HBASE-13324](https://issues.apache.org/jira/browse/HBASE-13324) | o.a.h.h.Coprocessor should be LimitedPrivate("Coprocessor") |  Minor | API |
| [HBASE-14445](https://issues.apache.org/jira/browse/HBASE-14445) | ExportSnapshot does not honor -chmod option |  Major | snapshots |
| [HBASE-14338](https://issues.apache.org/jira/browse/HBASE-14338) | License notification misspells 'Asciidoctor' |  Minor | . |
| [HBASE-14280](https://issues.apache.org/jira/browse/HBASE-14280) | Bulk Upload from HA cluster to remote HA hbase cluster fails |  Minor | hadoop2, regionserver |
| [HBASE-14431](https://issues.apache.org/jira/browse/HBASE-14431) | AsyncRpcClient#removeConnection() never removes connection from connections pool if server fails |  Critical | IPC/RPC |
| [HBASE-14449](https://issues.apache.org/jira/browse/HBASE-14449) | Rewrite deadlock prevention for concurrent connection close |  Major | master, metrics |
| [HBASE-13250](https://issues.apache.org/jira/browse/HBASE-13250) | chown of ExportSnapshot does not cover all path and files |  Critical | snapshots |
| [HBASE-14411](https://issues.apache.org/jira/browse/HBASE-14411) | Fix unit test failures when using multiwal as default WAL provider |  Major | . |
| [HBASE-14429](https://issues.apache.org/jira/browse/HBASE-14429) | Checkstyle report is broken |  Minor | scripts |
| [HBASE-14400](https://issues.apache.org/jira/browse/HBASE-14400) | Fix HBase RPC protection documentation |  Critical | encryption, IPC/RPC, security |
| [HBASE-14145](https://issues.apache.org/jira/browse/HBASE-14145) | Allow the Canary in regionserver mode to try all regions on the server, not just one |  Major | canary, util |
| [HBASE-14380](https://issues.apache.org/jira/browse/HBASE-14380) | Correct data gets skipped along with bad data in importTsv bulk load thru TsvImporterTextMapper |  Major | mapreduce, tooling |
| [HBASE-14307](https://issues.apache.org/jira/browse/HBASE-14307) | Incorrect use of positional read api in HFileBlock |  Major | io |
| [HBASE-14392](https://issues.apache.org/jira/browse/HBASE-14392) | [tests] TestLogRollingNoCluster fails on master from time to time |  Major | test |
| [HBASE-14382](https://issues.apache.org/jira/browse/HBASE-14382) | TestInterfaceAudienceAnnotations should hadoop-compt module resources |  Minor | test |
| [HBASE-14385](https://issues.apache.org/jira/browse/HBASE-14385) | Close the sockets that is missing in connection closure. |  Minor | Client |
| [HBASE-14393](https://issues.apache.org/jira/browse/HBASE-14393) | Have TestHFileEncryption clean up after itself so it don't go all zombie on us |  Major | test |
| [HBASE-14384](https://issues.apache.org/jira/browse/HBASE-14384) | Trying to run canary locally with -regionserver option causes exception |  Major | canary |
| [HBASE-14317](https://issues.apache.org/jira/browse/HBASE-14317) | Stuck FSHLog: bad disk (HDFS-8960) and can't roll WAL |  Blocker | wal |
| [HBASE-14327](https://issues.apache.org/jira/browse/HBASE-14327) | TestIOFencing#testFencingAroundCompactionAfterWALSync is flaky |  Critical | test |
| [HBASE-14359](https://issues.apache.org/jira/browse/HBASE-14359) | HTable#close will hang forever if unchecked error/exception thrown in AsyncProcess#sendMultiAction |  Major | Client |
| [HBASE-14229](https://issues.apache.org/jira/browse/HBASE-14229) | Flushing canceled by coprocessor still leads to memstoreSize set down |  Major | regionserver |
| [HBASE-14337](https://issues.apache.org/jira/browse/HBASE-14337) | build error on master |  Major | . |
| [HBASE-14354](https://issues.apache.org/jira/browse/HBASE-14354) | Minor improvements for usage of the mlock agent |  Trivial | hbase, regionserver |
| [HBASE-13221](https://issues.apache.org/jira/browse/HBASE-13221) | HDFS Transparent Encryption breaks WAL writing in Hadoop 2.6.0 |  Critical | documentation, wal |
| [HBASE-14258](https://issues.apache.org/jira/browse/HBASE-14258) | Make region\_mover.rb script case insensitive with regard to hostname |  Minor | . |
| [HBASE-14315](https://issues.apache.org/jira/browse/HBASE-14315) | Save one call to KeyValueHeap.peek per row |  Major | . |
| [HBASE-14313](https://issues.apache.org/jira/browse/HBASE-14313) | After a Connection sees ConnectionClosingException it never recovers |  Critical | Client |
| [HBASE-13339](https://issues.apache.org/jira/browse/HBASE-13339) | Update default Hadoop version to latest for master |  Blocker | build |
| [HBASE-14269](https://issues.apache.org/jira/browse/HBASE-14269) | FuzzyRowFilter omits certain rows when multiple fuzzy keys exist |  Major | Filters |
| [HBASE-14302](https://issues.apache.org/jira/browse/HBASE-14302) | TableSnapshotInputFormat should not create back references when restoring snapshot |  Major | . |
| [HBASE-14224](https://issues.apache.org/jira/browse/HBASE-14224) | Fix coprocessor handling of duplicate classes |  Critical | Coprocessors |
| [HBASE-13480](https://issues.apache.org/jira/browse/HBASE-13480) | ShortCircuitConnection doesn't short-circuit all calls as expected |  Critical | Client |
| [HBASE-14273](https://issues.apache.org/jira/browse/HBASE-14273) | Rename MVCC to MVCC: From MultiVersionConsistencyControl to MultiVersionConcurrencyControl |  Major | regionserver |
| [HBASE-14291](https://issues.apache.org/jira/browse/HBASE-14291) | NPE On StochasticLoadBalancer Balance Involving RS With No Regions |  Minor | Balancer |
| [HBASE-14250](https://issues.apache.org/jira/browse/HBASE-14250) | branch-1.1 hbase-server test-jar has incorrect LICENSE |  Blocker | build |
| [HBASE-14251](https://issues.apache.org/jira/browse/HBASE-14251) | javadoc jars use LICENSE/NOTICE from primary artifact |  Blocker | build |
| [HBASE-14249](https://issues.apache.org/jira/browse/HBASE-14249) | shaded jar modules create spurious source and test jars with incorrect LICENSE/NOTICE info |  Major | build |
| [HBASE-14243](https://issues.apache.org/jira/browse/HBASE-14243) | Incorrect NOTICE file in hbase-it test-jar |  Blocker | build |
| [HBASE-14234](https://issues.apache.org/jira/browse/HBASE-14234) | Procedure-V2: Exception encountered in WALProcedureStore#rollWriter() should be properly handled |  Minor | proc-v2 |
| [HBASE-14241](https://issues.apache.org/jira/browse/HBASE-14241) | Fix deadlock during cluster shutdown due to concurrent connection close |  Critical | master, metrics |
| [HBASE-14228](https://issues.apache.org/jira/browse/HBASE-14228) | Close BufferedMutator and connection in MultiTableOutputFormat |  Minor | mapreduce |
| [HBASE-14166](https://issues.apache.org/jira/browse/HBASE-14166) | Per-Region metrics can be stale |  Major | metrics |
| [HBASE-10844](https://issues.apache.org/jira/browse/HBASE-10844) | Coprocessor failure during batchmutation leaves the memstore datastructs in an inconsistent state |  Major | regionserver |
| [HBASE-13966](https://issues.apache.org/jira/browse/HBASE-13966) | Limit column width in table.jsp |  Minor | Operability, UI |
| [HBASE-14219](https://issues.apache.org/jira/browse/HBASE-14219) | src tgz no longer builds after HBASE-14085 |  Blocker | build |
| [HBASE-14214](https://issues.apache.org/jira/browse/HBASE-14214) | list\_labels shouldn't raise ArgumentError if no labels are defined |  Minor | . |
| [HBASE-14054](https://issues.apache.org/jira/browse/HBASE-14054) | Acknowledged writes may get lost if regionserver clock is set backwards |  Major | regionserver |
| [HBASE-14098](https://issues.apache.org/jira/browse/HBASE-14098) | Allow dropping caches behind compactions |  Major | Compaction, hadoop2, HFile |
| [HBASE-14196](https://issues.apache.org/jira/browse/HBASE-14196) | Thrift server idle connection timeout issue |  Major | Thrift |
| [HBASE-14209](https://issues.apache.org/jira/browse/HBASE-14209) | TestShell visibility tests failing |  Major | security, shell |
| [HBASE-14201](https://issues.apache.org/jira/browse/HBASE-14201) | hbck should not take a lock unless fixing errors |  Major | hbck, util |
| [HBASE-13889](https://issues.apache.org/jira/browse/HBASE-13889) | Fix hbase-shaded-client artifact so it works on hbase-downstreamer |  Critical | Client |
| [HBASE-14208](https://issues.apache.org/jira/browse/HBASE-14208) | Remove yarn dependencies on -common and -client |  Major | build, Client |
| [HBASE-14206](https://issues.apache.org/jira/browse/HBASE-14206) | MultiRowRangeFilter returns records whose rowKeys are out of allowed ranges |  Critical | Filters |
| [HBASE-13062](https://issues.apache.org/jira/browse/HBASE-13062) | Add documentation coverage for configuring dns server with thrift and rest gateways |  Minor | documentation |
| [HBASE-5878](https://issues.apache.org/jira/browse/HBASE-5878) | Use getVisibleLength public api from HdfsDataInputStream from Hadoop-2. |  Major | wal |
| [HBASE-14092](https://issues.apache.org/jira/browse/HBASE-14092) | hbck should run without locks by default and only disable the balancer when necessary |  Major | hbck, util |
| [HBASE-13924](https://issues.apache.org/jira/browse/HBASE-13924) | Description for hbase.dynamic.jars.dir is wrong |  Major | . |
| [HBASE-12865](https://issues.apache.org/jira/browse/HBASE-12865) | WALs may be deleted before they are replicated to peers |  Critical | Replication |
| [HBASE-13825](https://issues.apache.org/jira/browse/HBASE-13825) | Use ProtobufUtil#mergeFrom and ProtobufUtil#mergeDelimitedFrom in place of builder methods of same name |  Major | util |
| [HBASE-13865](https://issues.apache.org/jira/browse/HBASE-13865) | Increase the default value for hbase.hregion.memstore.block.multipler from 2 to 4 (part 2) |  Trivial | regionserver |
| [HBASE-14178](https://issues.apache.org/jira/browse/HBASE-14178) | regionserver blocks because of waiting for offsetLock |  Major | regionserver |
| [HBASE-14183](https://issues.apache.org/jira/browse/HBASE-14183) | [Shell] Scanning hbase meta table is failing in master branch |  Major | shell |
| [HBASE-14021](https://issues.apache.org/jira/browse/HBASE-14021) | Quota table has a wrong description on the UI |  Minor | UI |
| [HBASE-14185](https://issues.apache.org/jira/browse/HBASE-14185) | Incorrect region names logged by MemStoreFlusher |  Minor | regionserver |
| [HBASE-13864](https://issues.apache.org/jira/browse/HBASE-13864) | HColumnDescriptor should parse the output from master and from describe for TTL |  Major | shell |
| [HBASE-14162](https://issues.apache.org/jira/browse/HBASE-14162) | Fixing maven target for regenerating thrift classes fails against 0.9.2 |  Blocker | build, Thrift |
| [HBASE-14168](https://issues.apache.org/jira/browse/HBASE-14168) | Avoid useless retry for DoNotRetryIOException in TableRecordReaderImpl |  Minor | mapreduce |
| [HBASE-14173](https://issues.apache.org/jira/browse/HBASE-14173) | includeMVCCReadpoint parameter in DefaultCompactor#createTmpWriter() represents no-op |  Major | . |
| [HBASE-14155](https://issues.apache.org/jira/browse/HBASE-14155) | StackOverflowError in reverse scan |  Critical | regionserver, Scanners |
| [HBASE-14153](https://issues.apache.org/jira/browse/HBASE-14153) | Typo in ProcedureManagerHost.MASTER\_PROCEUDRE\_CONF\_KEY |  Trivial | . |
| [HBASE-14157](https://issues.apache.org/jira/browse/HBASE-14157) | Interfaces implemented by subclasses should be checked when registering CoprocessorService |  Major | Coprocessors |
| [HBASE-14024](https://issues.apache.org/jira/browse/HBASE-14024) | ImportTsv is not loading hbase-default.xml |  Critical | mapreduce |
| [HBASE-14156](https://issues.apache.org/jira/browse/HBASE-14156) | Fix test failure in TestOpenTableInCoprocessor |  Major | . |
| [HBASE-14146](https://issues.apache.org/jira/browse/HBASE-14146) | Once replication sees an error it slows down forever |  Major | Replication |
| [HBASE-14115](https://issues.apache.org/jira/browse/HBASE-14115) | Fix resource leak in HMasterCommandLine |  Major | master, tooling |
| [HBASE-14119](https://issues.apache.org/jira/browse/HBASE-14119) | Show meaningful error messages instead of stack traces in hbase shell commands. Fixing few commands in this jira. |  Minor | . |
| [HBASE-14065](https://issues.apache.org/jira/browse/HBASE-14065) | ref guide section on release candidate generation refers to old doc files |  Major | documentation |
| [HBASE-14000](https://issues.apache.org/jira/browse/HBASE-14000) | Region server failed to report to Master and was stuck in reportForDuty retry loop |  Major | regionserver |
| [HBASE-14106](https://issues.apache.org/jira/browse/HBASE-14106) | TestProcedureRecovery is flaky |  Major | proc-v2, test |
| [HBASE-14076](https://issues.apache.org/jira/browse/HBASE-14076) | ResultSerialization and MutationSerialization can throw InvalidProtocolBufferException when serializing a cell larger than 64MB |  Major | . |
| [HBASE-13971](https://issues.apache.org/jira/browse/HBASE-13971) | Flushes stuck since 6 hours on a regionserver. |  Critical | regionserver |
| [HBASE-14050](https://issues.apache.org/jira/browse/HBASE-14050) | NPE in org.apache.hadoop.hbase.ipc.RpcServer$Connection.readAndProcess |  Minor | . |
| [HBASE-14100](https://issues.apache.org/jira/browse/HBASE-14100) | Fix high priority findbugs warnings |  Major | regionserver |
| [HBASE-14089](https://issues.apache.org/jira/browse/HBASE-14089) | Remove unnecessary draw of system entropy from RecoverableZooKeeper |  Minor | . |
| [HBASE-14094](https://issues.apache.org/jira/browse/HBASE-14094) | Procedure.proto can't be compiled to C++ |  Major | proc-v2, Protobufs |
| [HBASE-14077](https://issues.apache.org/jira/browse/HBASE-14077) | Add package to hbase-protocol protobuf files. |  Major | Protobufs |
| [HBASE-14041](https://issues.apache.org/jira/browse/HBASE-14041) | Client MetaCache is cleared if a ThrottlingException is thrown |  Minor | Client |
| [HBASE-13897](https://issues.apache.org/jira/browse/HBASE-13897) | OOM may occur when Import imports a row with too many KeyValues |  Major | . |
| [HBASE-14073](https://issues.apache.org/jira/browse/HBASE-14073) | TestRemoteTable.testDelete failed in the latest trunk code |  Major | REST |
| [HBASE-13997](https://issues.apache.org/jira/browse/HBASE-13997) | ScannerCallableWithReplicas cause Infinitely blocking |  Minor | Client |
| [HBASE-14029](https://issues.apache.org/jira/browse/HBASE-14029) | getting started for standalone still references hadoop-version-specific binary artifacts |  Major | documentation |
| [HBASE-14042](https://issues.apache.org/jira/browse/HBASE-14042) | Fix FATAL level logging in FSHLog where logged for non fatal exceptions |  Major | Operability, wal |
| [HBASE-13337](https://issues.apache.org/jira/browse/HBASE-13337) | Table regions are not assigning back, after restarting all regionservers at once. |  Blocker | Region Assignment |
| [HBASE-13561](https://issues.apache.org/jira/browse/HBASE-13561) | ITBLL.Verify doesn't actually evaluate counters after job completes |  Major | integration tests |
| [HBASE-13988](https://issues.apache.org/jira/browse/HBASE-13988) | Add exception handler for lease thread |  Minor | . |
| [HBASE-13352](https://issues.apache.org/jira/browse/HBASE-13352) | Add hbase.import.version to Import usage. |  Major | . |
| [HBASE-14012](https://issues.apache.org/jira/browse/HBASE-14012) | Double Assignment and Dataloss when ServerCrashProcedure runs during Master failover |  Blocker | master, Region Assignment |
| [HBASE-13329](https://issues.apache.org/jira/browse/HBASE-13329) | ArrayIndexOutOfBoundsException in CellComparator#getMinimumMidpointArray |  Critical | regionserver |
| [HBASE-14011](https://issues.apache.org/jira/browse/HBASE-14011) | MultiByteBuffer position based reads does not work correctly |  Major | . |
| [HBASE-13861](https://issues.apache.org/jira/browse/HBASE-13861) | BucketCacheTmpl.jamon has wrong bucket free and used labels |  Major | regionserver, UI |
| [HBASE-14010](https://issues.apache.org/jira/browse/HBASE-14010) | TestRegionRebalancing.testRebalanceOnRegionServerNumberChange fails; cluster not balanced |  Major | test |
| [HBASE-14005](https://issues.apache.org/jira/browse/HBASE-14005) | Set permission to .top hfile in LoadIncrementalHFiles |  Trivial | . |
| [HBASE-13970](https://issues.apache.org/jira/browse/HBASE-13970) | NPE during compaction in trunk |  Major | regionserver |
| [HBASE-13895](https://issues.apache.org/jira/browse/HBASE-13895) | DATALOSS: Region assigned before WAL replay when abort |  Critical | Recovery, Region Assignment, wal |
| [HBASE-13978](https://issues.apache.org/jira/browse/HBASE-13978) | Variable never assigned in SimpleTotalOrderPartitioner.getPartition() |  Major | mapreduce |
| [HBASE-13995](https://issues.apache.org/jira/browse/HBASE-13995) | ServerName is not fully case insensitive |  Major | Region Assignment |
| [HBASE-13959](https://issues.apache.org/jira/browse/HBASE-13959) | Region splitting uses a single thread in most common cases |  Critical | regionserver |
| [HBASE-13989](https://issues.apache.org/jira/browse/HBASE-13989) | Threshold for combined MemStore and BlockCache percentages is not checked |  Major | regionserver |
| [HBASE-13930](https://issues.apache.org/jira/browse/HBASE-13930) | Exclude Findbugs packages from shaded jars |  Major | build, Client |
| [HBASE-13863](https://issues.apache.org/jira/browse/HBASE-13863) | Multi-wal feature breaks reported number and size of HLogs |  Major | regionserver, UI |
| [HBASE-13974](https://issues.apache.org/jira/browse/HBASE-13974) | TestRateLimiter#testFixedIntervalResourceAvailability may fail |  Minor | test |
| [HBASE-13969](https://issues.apache.org/jira/browse/HBASE-13969) | AuthenticationTokenSecretManager is never stopped in RPCServer |  Minor | . |
| [HBASE-13923](https://issues.apache.org/jira/browse/HBASE-13923) | Loaded region coprocessors are not reported in shell status command |  Major | regionserver, shell |
| [HBASE-13893](https://issues.apache.org/jira/browse/HBASE-13893) | Replace HTable with Table in client tests |  Major | Client, test |
| [HBASE-13835](https://issues.apache.org/jira/browse/HBASE-13835) | KeyValueHeap.current might be in heap when exception happens in pollRealKV |  Major | Scanners |
| [HBASE-13945](https://issues.apache.org/jira/browse/HBASE-13945) | Prefix\_Tree seekBefore() does not work correctly |  Major | io |
| [HBASE-13958](https://issues.apache.org/jira/browse/HBASE-13958) | RESTApiClusterManager calls kill() instead of suspend() and resume() |  Minor | integration tests |
| [HBASE-13938](https://issues.apache.org/jira/browse/HBASE-13938) | Deletes done during the region merge transaction may get eclipsed |  Major | master, regionserver |
| [HBASE-13933](https://issues.apache.org/jira/browse/HBASE-13933) | DBE's seekBefore with tags corrupts the tag's offset information thus leading to incorrect results |  Critical | io |
| [HBASE-13877](https://issues.apache.org/jira/browse/HBASE-13877) | Interrupt to flush from TableFlushProcedure causes dataloss in ITBLL |  Blocker | integration tests, proc-v2 |
| [HBASE-13918](https://issues.apache.org/jira/browse/HBASE-13918) | Fix hbase:namespace description in webUI |  Trivial | hbase |
| [HBASE-13885](https://issues.apache.org/jira/browse/HBASE-13885) | ZK watches leaks during snapshots |  Critical | snapshots |
| [HBASE-13737](https://issues.apache.org/jira/browse/HBASE-13737) | [HBase MOB] MOBTable cloned from a snapshot leads to data loss, when that actual snapshot and main table is deleted. |  Critical | mob |
| [HBASE-13913](https://issues.apache.org/jira/browse/HBASE-13913) | RAT exclusion list missing asciidoctor support files |  Major | . |
| [HBASE-13279](https://issues.apache.org/jira/browse/HBASE-13279) | Add src/main/asciidoc/asciidoctor.css to RAT exclusion list in POM |  Minor | documentation |
| [HBASE-13821](https://issues.apache.org/jira/browse/HBASE-13821) | WARN if hbase.bucketcache.percentage.in.combinedcache is set |  Minor | regionserver, Usability |
| [HBASE-13888](https://issues.apache.org/jira/browse/HBASE-13888) | Fix refill bug from HBASE-13686 |  Major | regionserver |
| [HBASE-13905](https://issues.apache.org/jira/browse/HBASE-13905) | TestRecoveredEdits.testReplayWorksThoughLotsOfFlushing failing consistently on branch-1.1 |  Critical | regionserver, test |
| [HBASE-13833](https://issues.apache.org/jira/browse/HBASE-13833) | LoadIncrementalHFile.doBulkLoad(Path,HTable) doesn't handle unmanaged connections when using SecureBulkLoad |  Major | tooling |
| [HBASE-13901](https://issues.apache.org/jira/browse/HBASE-13901) | Error while calling watcher on creating and deleting an HBase table |  Minor | . |
| [HBASE-13892](https://issues.apache.org/jira/browse/HBASE-13892) | Scanner with all results filtered out results in NPE |  Critical | Client |
| [HBASE-13560](https://issues.apache.org/jira/browse/HBASE-13560) | Large compaction queue should steal from small compaction queue when idle |  Major | Compaction |
| [HBASE-13878](https://issues.apache.org/jira/browse/HBASE-13878) | Set hbase.fs.tmp.dir config in HBaseTestingUtility.java for Phoenix UT to use |  Minor | test |
| [HBASE-13873](https://issues.apache.org/jira/browse/HBASE-13873) | LoadTestTool addAuthInfoToConf throws UnsupportedOperationException |  Major | integration tests |
| [HBASE-13875](https://issues.apache.org/jira/browse/HBASE-13875) | Clock skew between master and region server may render restored region without server address |  Major | snapshots |
| [HBASE-13845](https://issues.apache.org/jira/browse/HBASE-13845) | Expire of one region server carrying meta can bring down the master |  Major | master |
| [HBASE-13847](https://issues.apache.org/jira/browse/HBASE-13847) | getWriteRequestCount function in HRegionServer uses int variable to return the count. |  Major | hbase, regionserver |
| [HBASE-13811](https://issues.apache.org/jira/browse/HBASE-13811) | Splitting WALs, we are filtering out too many edits -\> DATALOSS |  Critical | wal |
| [HBASE-13853](https://issues.apache.org/jira/browse/HBASE-13853) | ITBLL improvements after HBASE-13811 |  Blocker | integration tests, tooling, wal |
| [HBASE-13686](https://issues.apache.org/jira/browse/HBASE-13686) | Fail to limit rate in RateLimiter |  Major | Client |
| [HBASE-13851](https://issues.apache.org/jira/browse/HBASE-13851) | RpcClientImpl.close() can hang with cancelled replica RPCs |  Major | IPC/RPC |
| [HBASE-13834](https://issues.apache.org/jira/browse/HBASE-13834) | Evict count not properly passed to HeapMemoryTuner. |  Major | hbase, regionserver |
| [HBASE-13729](https://issues.apache.org/jira/browse/HBASE-13729) | Old hbase.regionserver.global.memstore.upperLimit and lowerLimit properties are ignored if present |  Critical | regionserver |
| [HBASE-13789](https://issues.apache.org/jira/browse/HBASE-13789) | ForeignException should not be sent to the client |  Minor | Client, master |
| [HBASE-13779](https://issues.apache.org/jira/browse/HBASE-13779) | Calling table.exists() before table.get() end up with an empty Result |  Major | Client |
| [HBASE-13831](https://issues.apache.org/jira/browse/HBASE-13831) | TestHBaseFsck#testParallelHbck is flaky against hadoop 2.6+ |  Minor | hbck, test |
| [HBASE-13826](https://issues.apache.org/jira/browse/HBASE-13826) | Unable to create table when group acls are appropriately set. |  Major | security |
| [HBASE-13824](https://issues.apache.org/jira/browse/HBASE-13824) | TestGenerateDelegationToken: Master fails to start in Windows environment |  Minor | test |
| [HBASE-13647](https://issues.apache.org/jira/browse/HBASE-13647) | Default value for hbase.client.operation.timeout is too high |  Blocker | Client |
| [HBASE-13820](https://issues.apache.org/jira/browse/HBASE-13820) | Zookeeper is failing to start |  Critical | . |
| [HBASE-13638](https://issues.apache.org/jira/browse/HBASE-13638) | Put copy constructor is shallow |  Major | Client |
| [HBASE-13809](https://issues.apache.org/jira/browse/HBASE-13809) | TestRowTooBig should use HDFS directory for its region directory |  Minor | test |
| [HBASE-13812](https://issues.apache.org/jira/browse/HBASE-13812) | Deleting of last Column Family of a table should not be allowed |  Major | master |
| [HBASE-13813](https://issues.apache.org/jira/browse/HBASE-13813) | Fix Javadoc warnings in Procedure.java |  Minor | documentation |
| [HBASE-13776](https://issues.apache.org/jira/browse/HBASE-13776) | Setting illegal versions for HColumnDescriptor does not throw IllegalArgumentException |  Major | API |
| [HBASE-13802](https://issues.apache.org/jira/browse/HBASE-13802) | Procedure V2: Master fails to come up due to rollback of create namespace table |  Major | master, proc-v2 |
| [HBASE-13796](https://issues.apache.org/jira/browse/HBASE-13796) | ZKUtil doesn't clean quorum setting properly |  Minor | . |
| [HBASE-13800](https://issues.apache.org/jira/browse/HBASE-13800) | TestStore#testDeleteExpiredStoreFiles should create unique data/log directory for each call |  Minor | test |
| [HBASE-13797](https://issues.apache.org/jira/browse/HBASE-13797) | Fix resource leak in HBaseFsck |  Minor | . |
| [HBASE-13801](https://issues.apache.org/jira/browse/HBASE-13801) | Hadoop src checksum is shown instead of HBase src checksum in master / RS UI |  Major | UI |
| [HBASE-13723](https://issues.apache.org/jira/browse/HBASE-13723) | In table.rb scanners are never closed. |  Major | shell |
| [HBASE-13777](https://issues.apache.org/jira/browse/HBASE-13777) | Table fragmentation display triggers NPE on master status page |  Major | UI |
| [HBASE-13778](https://issues.apache.org/jira/browse/HBASE-13778) | BoundedByteBufferPool incorrectly increasing runningAverage buffer length |  Major | io, util |
| [HBASE-13732](https://issues.apache.org/jira/browse/HBASE-13732) | TestHBaseFsck#testParallelWithRetriesHbck fails intermittently |  Minor | hbck, test |
| [HBASE-13768](https://issues.apache.org/jira/browse/HBASE-13768) | ZooKeeper znodes are bootstrapped with insecure ACLs in a secure configuration |  Blocker | security, Zookeeper |
| [HBASE-13767](https://issues.apache.org/jira/browse/HBASE-13767) | Allow ZKAclReset to set and not just clear ZK ACLs |  Trivial | Operability, Zookeeper |
| [HBASE-13746](https://issues.apache.org/jira/browse/HBASE-13746) | list\_replicated\_tables command is not listing table in hbase shell. |  Major | shell |
| [HBASE-13760](https://issues.apache.org/jira/browse/HBASE-13760) | Cleanup Findbugs keySet iterator warnings |  Minor | . |
| [HBASE-13734](https://issues.apache.org/jira/browse/HBASE-13734) | Improper timestamp checking with VisibilityScanDeleteTracker |  Major | security |
| [HBASE-13604](https://issues.apache.org/jira/browse/HBASE-13604) | bin/hbase mapredcp does not include yammer-metrics jar |  Minor | . |
| [HBASE-13703](https://issues.apache.org/jira/browse/HBASE-13703) | ReplicateContext should not be a member of ReplicationSource |  Minor | . |
| [HBASE-13733](https://issues.apache.org/jira/browse/HBASE-13733) | Failed MiniZooKeeperCluster startup did not shutdown ZK servers |  Major | Zookeeper |
| [HBASE-13741](https://issues.apache.org/jira/browse/HBASE-13741) | Disable TestRegionObserverInterface#testRecovery and testLegacyRecovery |  Minor | . |
| [HBASE-13731](https://issues.apache.org/jira/browse/HBASE-13731) | TestReplicationAdmin should clean up MiniZKCluster resource |  Trivial | test |
| [HBASE-13709](https://issues.apache.org/jira/browse/HBASE-13709) | Updates to meta table server columns may be eclipsed |  Major | IPC/RPC, regionserver |
| [HBASE-13721](https://issues.apache.org/jira/browse/HBASE-13721) | Improve shell scan performances when using LIMIT |  Major | shell |
| [HBASE-13700](https://issues.apache.org/jira/browse/HBASE-13700) | Allow Thrift2 HSHA server to have configurable threads |  Major | Thrift |
| [HBASE-13719](https://issues.apache.org/jira/browse/HBASE-13719) | Asynchronous scanner -- cache size-in-bytes bug fix |  Major | . |
| [HBASE-13618](https://issues.apache.org/jira/browse/HBASE-13618) | ReplicationSource is too eager to remove sinks |  Minor | . |
| [HBASE-13711](https://issues.apache.org/jira/browse/HBASE-13711) | Provide an API to set min and max versions in HColumnDescriptor |  Minor | . |
| [HBASE-13722](https://issues.apache.org/jira/browse/HBASE-13722) | Avoid non static method from BloomFilterUtil |  Trivial | . |
| [HBASE-13704](https://issues.apache.org/jira/browse/HBASE-13704) | Hbase throws OutOfOrderScannerNextException when MultiRowRangeFilter is used |  Major | Client |
| [HBASE-13717](https://issues.apache.org/jira/browse/HBASE-13717) | TestBoundedRegionGroupingProvider#setMembershipDedups need to set HDFS diretory for WAL |  Minor | wal |
| [HBASE-13693](https://issues.apache.org/jira/browse/HBASE-13693) | [HBase MOB] Mob files are not encrypting. |  Major | mob |
| [HBASE-13694](https://issues.apache.org/jira/browse/HBASE-13694) | CallQueueSize is incorrectly decremented until the response is sent |  Major | IPC/RPC, master, regionserver |
| [HBASE-13668](https://issues.apache.org/jira/browse/HBASE-13668) | TestFlushRegionEntry is flaky |  Minor | . |
| [HBASE-13699](https://issues.apache.org/jira/browse/HBASE-13699) | Expand information about HBase quotas |  Major | documentation |
| [HBASE-13651](https://issues.apache.org/jira/browse/HBASE-13651) | Handle StoreFileScanner FileNotFoundException |  Minor | . |
| [HBASE-11830](https://issues.apache.org/jira/browse/HBASE-11830) | TestReplicationThrottler.testThrottling failed on virtual boxes |  Minor | test |
| [HBASE-13663](https://issues.apache.org/jira/browse/HBASE-13663) | HMaster fails to restart 'HMaster: Failed to become active master' |  Major | hbase |
| [HBASE-13217](https://issues.apache.org/jira/browse/HBASE-13217) | Procedure fails due to ZK issue |  Major | . |
| [HBASE-13662](https://issues.apache.org/jira/browse/HBASE-13662) | RSRpcService.scan() throws an OutOfOrderScannerNext if the scan has a retriable failure |  Major | IPC/RPC, regionserver |
| [HBASE-13533](https://issues.apache.org/jira/browse/HBASE-13533) | section on configuring ~/.m2/settings.xml has no anchor |  Trivial | documentation |
| [HBASE-13661](https://issues.apache.org/jira/browse/HBASE-13661) | Correct binary compatibility issues discovered in 1.1.0RC0 |  Major | . |
| [HBASE-13606](https://issues.apache.org/jira/browse/HBASE-13606) | AssignmentManager.assign() is not sync in both path |  Major | Region Assignment |
| [HBASE-13635](https://issues.apache.org/jira/browse/HBASE-13635) | Regions stuck in transition because master is incorrectly assumed dead |  Major | master, regionserver |
| [HBASE-13634](https://issues.apache.org/jira/browse/HBASE-13634) | Avoid unsafe reference equality checks to EMPTY byte[] |  Major | Compaction, Scanners |
| [HBASE-1989](https://issues.apache.org/jira/browse/HBASE-1989) | Admin (et al.) not accurate with Column vs. Column-Family usage |  Minor | Client |
| [HBASE-13580](https://issues.apache.org/jira/browse/HBASE-13580) | region\_mover.rb broken with TypeError: no public constructors for Java::OrgApacheHadoopHbaseClient::HTable |  Major | scripts |
| [HBASE-13611](https://issues.apache.org/jira/browse/HBASE-13611) | update clover to work for current versions |  Minor | build |
| [HBASE-13653](https://issues.apache.org/jira/browse/HBASE-13653) | Uninitialized HRegionServer#walFactory may result in NullPointerException at region server startup​ |  Major | hbase |
| [HBASE-13612](https://issues.apache.org/jira/browse/HBASE-13612) | TestRegionFavoredNodes doesn't guard against setup failure |  Minor | test |
| [HBASE-13649](https://issues.apache.org/jira/browse/HBASE-13649) | CellComparator.compareTimestamps javadoc inconsistent and wrong |  Minor | documentation |
| [HBASE-13648](https://issues.apache.org/jira/browse/HBASE-13648) | test-patch.sh should ignore 'protobuf.generated' |  Minor | build |
| [HBASE-13630](https://issues.apache.org/jira/browse/HBASE-13630) | Remove dead code in BufferedDataEncoder |  Minor | . |
| [HBASE-13576](https://issues.apache.org/jira/browse/HBASE-13576) | HBCK enhancement: Failure in checking one region should not fail the entire HBCK operation. |  Major | hbck |
| [HBASE-13625](https://issues.apache.org/jira/browse/HBASE-13625) | Use HDFS for HFileOutputFormat2 partitioner's path |  Major | mapreduce |
| [HBASE-13633](https://issues.apache.org/jira/browse/HBASE-13633) | draining\_servers.rb broken with NoMethodError: undefined method 'getServerInfo' |  Major | scripts |
| [HBASE-13628](https://issues.apache.org/jira/browse/HBASE-13628) | Use AtomicLong as size in BoundedConcurrentLinkedQueue |  Major | util |
| [HBASE-13617](https://issues.apache.org/jira/browse/HBASE-13617) | TestReplicaWithCluster.testChangeTable timeout |  Major | test |
| [HBASE-13599](https://issues.apache.org/jira/browse/HBASE-13599) | The Example Provided in Section 69: Examples of the Documentation Does Not Compile |  Minor | documentation |
| [HBASE-13333](https://issues.apache.org/jira/browse/HBASE-13333) | Renew Scanner Lease without advancing the RegionScanner |  Major | . |
| [HBASE-13607](https://issues.apache.org/jira/browse/HBASE-13607) | TestSplitLogManager.testGetPreviousRecoveryMode consistently failing |  Minor | test |
| [HBASE-12413](https://issues.apache.org/jira/browse/HBASE-12413) | Mismatch in the equals and hashcode methods of KeyValue |  Minor | . |
| [HBASE-13312](https://issues.apache.org/jira/browse/HBASE-13312) | SmallScannerCallable does not increment scan metrics |  Major | Client, Scanners |
| [HBASE-13608](https://issues.apache.org/jira/browse/HBASE-13608) | 413 Error with Stargate through Knox, using AD, SPNEGO, and Pre-Auth |  Major | REST |
| [HBASE-13601](https://issues.apache.org/jira/browse/HBASE-13601) | Connection leak during log splitting |  Major | wal |
| [HBASE-13600](https://issues.apache.org/jira/browse/HBASE-13600) | check\_compatibility.sh should ignore shaded jars |  Minor | build |
| [HBASE-13595](https://issues.apache.org/jira/browse/HBASE-13595) | Fix Javadoc warn induced in Bytes.java |  Trivial | . |
| [HBASE-13564](https://issues.apache.org/jira/browse/HBASE-13564) | Master MBeans are not published |  Major | master, metrics |
| [HBASE-13596](https://issues.apache.org/jira/browse/HBASE-13596) | src assembly does not build |  Major | build |
| [HBASE-13594](https://issues.apache.org/jira/browse/HBASE-13594) | MultiRowRangeFilter shouldn't call HBaseZeroCopyByteString.wrap() directly |  Major | . |
| [HBASE-13577](https://issues.apache.org/jira/browse/HBASE-13577) | Documentation is pointing to wrong port for Master Web UI |  Minor | documentation |
| [HBASE-13585](https://issues.apache.org/jira/browse/HBASE-13585) | HRegionFileSystem#splitStoreFile() finishes without closing the file handle in some situation |  Major | regionserver |
| [HBASE-13589](https://issues.apache.org/jira/browse/HBASE-13589) | [WINDOWS] hbase.cmd script is broken |  Major | . |
| [HBASE-13417](https://issues.apache.org/jira/browse/HBASE-13417) | batchCoprocessorService() does not handle NULL keys |  Minor | Coprocessors |
| [HBASE-13517](https://issues.apache.org/jira/browse/HBASE-13517) | Publish a client artifact with shaded dependencies |  Major | . |
| [HBASE-13575](https://issues.apache.org/jira/browse/HBASE-13575) | TestChoreService has to make sure that the opened ChoreService is closed for each unit test |  Trivial | . |
| [HBASE-13394](https://issues.apache.org/jira/browse/HBASE-13394) | Failed to recreate a table when quota is enabled |  Major | security |
| [HBASE-13490](https://issues.apache.org/jira/browse/HBASE-13490) | foreground daemon start re-executes ulimit output |  Minor | scripts |
| [HBASE-13359](https://issues.apache.org/jira/browse/HBASE-13359) | Update ACL matrix to include table owner. |  Minor | documentation |
| [HBASE-13149](https://issues.apache.org/jira/browse/HBASE-13149) | HBase MR is broken on Hadoop 2.5+ Yarn |  Blocker | . |
| [HBASE-13546](https://issues.apache.org/jira/browse/HBASE-13546) | NPE on region server status page if all masters are down |  Major | regionserver |
| [HBASE-13555](https://issues.apache.org/jira/browse/HBASE-13555) | StackServlet produces 500 error |  Major | . |
| [HBASE-13523](https://issues.apache.org/jira/browse/HBASE-13523) | API Doumentation formatting is broken |  Minor | documentation |
| [HBASE-13528](https://issues.apache.org/jira/browse/HBASE-13528) | A bug on selecting compaction pool |  Minor | Compaction |
| [HBASE-13526](https://issues.apache.org/jira/browse/HBASE-13526) | TestRegionServerReportForDuty can be flaky: hang or timeout |  Minor | test |
| [HBASE-13527](https://issues.apache.org/jira/browse/HBASE-13527) | The default value for hbase.client.scanner.max.result.size is never actually set on Scans |  Major | . |
| [HBASE-13437](https://issues.apache.org/jira/browse/HBASE-13437) | ThriftServer leaks ZooKeeper connections |  Major | Thrift |
| [HBASE-13524](https://issues.apache.org/jira/browse/HBASE-13524) | TestReplicationAdmin fails on JDK 1.8 |  Major | . |
| [HBASE-13499](https://issues.apache.org/jira/browse/HBASE-13499) | AsyncRpcClient test cases failure in powerpc |  Major | IPC/RPC |
| [HBASE-13471](https://issues.apache.org/jira/browse/HBASE-13471) | Fix a possible infinite loop in doMiniBatchMutation |  Major | . |
| [HBASE-13520](https://issues.apache.org/jira/browse/HBASE-13520) | NullPointerException in TagRewriteCell |  Major | . |
| [HBASE-13482](https://issues.apache.org/jira/browse/HBASE-13482) | Phoenix is failing to scan tables on secure environments. |  Major | . |
| [HBASE-13430](https://issues.apache.org/jira/browse/HBASE-13430) | HFiles that are in use by a table cloned from a snapshot may be deleted when that snapshot is deleted |  Critical | hbase |
| [HBASE-13491](https://issues.apache.org/jira/browse/HBASE-13491) | Issue in FuzzyRowFilter#getNextForFuzzyRule |  Major | Filters |
| [HBASE-13477](https://issues.apache.org/jira/browse/HBASE-13477) | Create metrics on failed requests |  Major | . |
| [HBASE-13486](https://issues.apache.org/jira/browse/HBASE-13486) | region\_status.rb broken with NameError: uninitialized constant IOException |  Major | scripts |
| [HBASE-13473](https://issues.apache.org/jira/browse/HBASE-13473) | deleted cells come back alive after the stripe compaction |  Blocker | Compaction |
| [HBASE-12006](https://issues.apache.org/jira/browse/HBASE-12006) | [JDK 8] KeyStoreTestUtil#generateCertificate fails due to "subject class type invalid" |  Minor | . |
| [HBASE-13460](https://issues.apache.org/jira/browse/HBASE-13460) | Revise the MetaLookupPool executor-related defaults (introduced in HBASE-13036) |  Major | . |
| [HBASE-13475](https://issues.apache.org/jira/browse/HBASE-13475) | Small spelling mistake in region\_mover#isSuccessfulScan causes NoMethodError |  Trivial | scripts |
| [HBASE-13301](https://issues.apache.org/jira/browse/HBASE-13301) | Possible memory leak in BucketCache |  Major | BlockCache |
| [HBASE-13457](https://issues.apache.org/jira/browse/HBASE-13457) | SnapshotExistsException doesn't honor the DoNotRetry |  Trivial | Client |
| [HBASE-13423](https://issues.apache.org/jira/browse/HBASE-13423) | Remove duplicate entry for hbase.regionserver.regionSplitLimit in hbase-default.xml |  Minor | hbase |
| [HBASE-13275](https://issues.apache.org/jira/browse/HBASE-13275) | Setting hbase.security.authorization to false does not disable authorization |  Major | security |
| [HBASE-11544](https://issues.apache.org/jira/browse/HBASE-11544) | [Ergonomics] hbase.client.scanner.caching is dogged and will try to return batch even if it means OOME |  Critical | . |
| [HBASE-13414](https://issues.apache.org/jira/browse/HBASE-13414) | TestHCM no longer needs to test for JRE 6. |  Minor | test |
| [HBASE-13377](https://issues.apache.org/jira/browse/HBASE-13377) | Canary may generate false alarm on the first region when there are many delete markers |  Major | monitoring |
| [HBASE-13289](https://issues.apache.org/jira/browse/HBASE-13289) | typo in splitSuccessCount  metric |  Major | metrics |
| [HBASE-13299](https://issues.apache.org/jira/browse/HBASE-13299) | Add setReturnResults() to Increment, like Append has |  Critical | API |
| [HBASE-13410](https://issues.apache.org/jira/browse/HBASE-13410) | Bug in KeyValueUtil.oswrite() for non Keyvalue cases |  Major | . |
| [HBASE-13411](https://issues.apache.org/jira/browse/HBASE-13411) | Misleading error message when request size quota limit exceeds |  Minor | . |
| [HBASE-13409](https://issues.apache.org/jira/browse/HBASE-13409) | Add categories to uncategorized tests |  Trivial | . |
| [HBASE-13374](https://issues.apache.org/jira/browse/HBASE-13374) | Small scanners (with particular configurations) do not return all rows |  Blocker | . |
| [HBASE-13406](https://issues.apache.org/jira/browse/HBASE-13406) | TestAccessController is flaky when create is slow |  Minor | security, test |
| [HBASE-13397](https://issues.apache.org/jira/browse/HBASE-13397) | Purge duplicate rpc request thread local |  Major | IPC/RPC |
| [HBASE-13382](https://issues.apache.org/jira/browse/HBASE-13382) | IntegrationTestBigLinkedList should use SecureRandom |  Major | integration tests |
| [HBASE-13385](https://issues.apache.org/jira/browse/HBASE-13385) | TestGenerateDelegationToken is broken with hadoop 2.8.0 |  Major | test |
| [HBASE-13388](https://issues.apache.org/jira/browse/HBASE-13388) | Handling NullPointer in ZKProcedureMemberRpcs while getting ZNode data |  Minor | . |
| [HBASE-13058](https://issues.apache.org/jira/browse/HBASE-13058) | Hbase shell command 'scan' for non existent table shows unnecessary info for one unrelated existent table. |  Trivial | Client |
| [HBASE-13091](https://issues.apache.org/jira/browse/HBASE-13091) | Split ZK Quorum on Master WebUI |  Minor | . |
| [HBASE-13384](https://issues.apache.org/jira/browse/HBASE-13384) | Fix Javadoc warnings introduced by HBASE-12972 |  Trivial | . |
| [HBASE-13368](https://issues.apache.org/jira/browse/HBASE-13368) | Hash.java is declared as public Interface - but it should be Private |  Trivial | . |
| [HBASE-13383](https://issues.apache.org/jira/browse/HBASE-13383) | TestRegionServerObserver.testCoprocessorHooksInRegionsMerge zombie after HBASE-12975 |  Major | . |
| [HBASE-13296](https://issues.apache.org/jira/browse/HBASE-13296) | Fix the deletion of acl notify nodes for namespace. |  Minor | . |
| [HBASE-12954](https://issues.apache.org/jira/browse/HBASE-12954) | Ability impaired using HBase on multihomed hosts |  Minor | . |
| [HBASE-13317](https://issues.apache.org/jira/browse/HBASE-13317) | Region server reportForDuty stuck looping if there is a master change |  Major | regionserver |
| [HBASE-13371](https://issues.apache.org/jira/browse/HBASE-13371) | Fix typo in TestAsyncIPC |  Major | test |
| [HBASE-13364](https://issues.apache.org/jira/browse/HBASE-13364) | Make using the default javac on by default |  Major | build |
| [HBASE-12993](https://issues.apache.org/jira/browse/HBASE-12993) | Use HBase 1.0 interfaces in hbase-thrift |  Major | . |
| [HBASE-13262](https://issues.apache.org/jira/browse/HBASE-13262) | ResultScanner doesn't return all rows in Scan |  Blocker | Client |
| [HBASE-13357](https://issues.apache.org/jira/browse/HBASE-13357) | If maxTables/maxRegions exceeds quota in a namespace, throw QuotaExceededException |  Minor | . |
| [HBASE-13355](https://issues.apache.org/jira/browse/HBASE-13355) | QA bot reports checking javac twice |  Minor | . |
| [HBASE-13328](https://issues.apache.org/jira/browse/HBASE-13328) | LoadIncrementalHFile.doBulkLoad(Path,HTable) should handle managed connections |  Major | . |
| [HBASE-13295](https://issues.apache.org/jira/browse/HBASE-13295) | TestInfoServers hang |  Major | test |
| [HBASE-8725](https://issues.apache.org/jira/browse/HBASE-8725) | Add total time RPC call metrics |  Major | metrics |
| [HBASE-13265](https://issues.apache.org/jira/browse/HBASE-13265) | Make thrift2 usable from c++ |  Major | . |
| [HBASE-13326](https://issues.apache.org/jira/browse/HBASE-13326) | Disabled table can't be enabled after HBase is restarted |  Blocker | . |
| [HBASE-13325](https://issues.apache.org/jira/browse/HBASE-13325) | Protocol Buffers 2.5 no longer available for download on code.google.com |  Major | . |
| [HBASE-13294](https://issues.apache.org/jira/browse/HBASE-13294) | Fix the critical ancient loopholes in security testing infrastructure. |  Major | . |
| [HBASE-13311](https://issues.apache.org/jira/browse/HBASE-13311) | TestQuotaThrottle flaky on slow machine |  Minor | test |
| [HBASE-13305](https://issues.apache.org/jira/browse/HBASE-13305) | Get(Get get) is not copying the row key |  Major | API |
| [HBASE-13331](https://issues.apache.org/jira/browse/HBASE-13331) | Exceptions from DFS client can cause CatalogJanitor to delete referenced files |  Blocker | master |
| [HBASE-13273](https://issues.apache.org/jira/browse/HBASE-13273) | Make Result.EMPTY\_RESULT read-only; currently it can be modified |  Major | . |
| [HBASE-13321](https://issues.apache.org/jira/browse/HBASE-13321) | Fix flaky TestHBaseFsck |  Minor | test |
| [HBASE-13314](https://issues.apache.org/jira/browse/HBASE-13314) | Fix NPE in HMaster.getClusterStatus() |  Minor | . |
| [HBASE-13281](https://issues.apache.org/jira/browse/HBASE-13281) | 'hbase.bucketcache.size' description in hbase book is not correct |  Major | documentation |
| [HBASE-13315](https://issues.apache.org/jira/browse/HBASE-13315) | BufferedMutator should be @InterfaceAudience.Public |  Major | . |
| [HBASE-13309](https://issues.apache.org/jira/browse/HBASE-13309) | Some tests do not reset EnvironmentEdgeManager |  Minor | test |
| [HBASE-13308](https://issues.apache.org/jira/browse/HBASE-13308) | Fix flaky TestEndToEndSplitTransaction |  Major | flakey, test |
| [HBASE-13282](https://issues.apache.org/jira/browse/HBASE-13282) | Fix the minor issues of running Canary on kerberized environment |  Minor | . |
| [HBASE-12867](https://issues.apache.org/jira/browse/HBASE-12867) | Shell does not support custom replication endpoint specification |  Major | . |
| [HBASE-13274](https://issues.apache.org/jira/browse/HBASE-13274) | Fix misplaced deprecation in Delete#addXYZ |  Major | API |
| [HBASE-13114](https://issues.apache.org/jira/browse/HBASE-13114) | [UNITTEST] TestEnableTableHandler.testDeleteForSureClearsAllTableRowsFromMeta |  Major | test |
| [HBASE-13285](https://issues.apache.org/jira/browse/HBASE-13285) | Fix flaky getRegions() in TestAccessController.setUp() |  Minor | test |
| [HBASE-13188](https://issues.apache.org/jira/browse/HBASE-13188) | java.lang.ArithmeticException issue in BoundedByteBufferPool.putBuffer |  Major | . |
| [HBASE-13200](https://issues.apache.org/jira/browse/HBASE-13200) | Improper configuration can leads to endless lease recovery during failover |  Major | MTTR |
| [HBASE-13253](https://issues.apache.org/jira/browse/HBASE-13253) | LoadIncrementalHFiles unify hfiles discovery |  Major | Client, mapreduce |
| [HBASE-13229](https://issues.apache.org/jira/browse/HBASE-13229) | Specify bash for local-regionservers.sh and local-master-backup.sh |  Minor | scripts |
| [HBASE-13176](https://issues.apache.org/jira/browse/HBASE-13176) | Flakey TestZooKeeper test. |  Major | . |
| [HBASE-13093](https://issues.apache.org/jira/browse/HBASE-13093) | Local mode HBase instance doesn't shut down. |  Major | . |
| [HBASE-12908](https://issues.apache.org/jira/browse/HBASE-12908) | Typos in MemStoreFlusher javadocs |  Trivial | documentation |
| [HBASE-13254](https://issues.apache.org/jira/browse/HBASE-13254) | EnableTableHandler#prepare would not throw TableNotFoundException during recovery |  Minor | . |
| [HBASE-13193](https://issues.apache.org/jira/browse/HBASE-13193) | RegionScannerImpl filters should not be reset if a partial Result is returned |  Major | . |
| [HBASE-13246](https://issues.apache.org/jira/browse/HBASE-13246) | Correct the assertion for namespace permissions in tearDown method of TestAccessController |  Minor | security, test |
| [HBASE-13239](https://issues.apache.org/jira/browse/HBASE-13239) | HBase grant at specific column level does not work for Groups |  Major | hbase |
| [HBASE-13242](https://issues.apache.org/jira/browse/HBASE-13242) | TestPerColumnFamilyFlush.testFlushingWhenLogRolling hung |  Major | test |
| [HBASE-13227](https://issues.apache.org/jira/browse/HBASE-13227) | LoadIncrementalHFile should skip non-files inside a possible family-dir |  Minor | Client, mapreduce |
| [HBASE-13232](https://issues.apache.org/jira/browse/HBASE-13232) | ConnectionManger : Batch pool threads and metaLookup pool threads should use different name pattern |  Trivial | . |
| [HBASE-13097](https://issues.apache.org/jira/browse/HBASE-13097) | Use same EventLoopGroup for different AsyncRpcClients if possible |  Major | IPC/RPC, test |
| [HBASE-13136](https://issues.apache.org/jira/browse/HBASE-13136) | TestSplitLogManager.testGetPreviousRecoveryMode is flakey |  Major | . |
| [HBASE-13224](https://issues.apache.org/jira/browse/HBASE-13224) | Minor formatting issue when logging a namespace scope in AuthResult#toContextString |  Trivial | Coprocessors, security |
| [HBASE-13194](https://issues.apache.org/jira/browse/HBASE-13194) | TableNamespaceManager not ready cause MasterQuotaManager initialization fail |  Major | master |
| [HBASE-13218](https://issues.apache.org/jira/browse/HBASE-13218) | Correct the syntax shown for using ExportSnapshot tool in the book |  Minor | documentation |
| [HBASE-13167](https://issues.apache.org/jira/browse/HBASE-13167) | The check for balancerCutoffTime is buggy |  Trivial | Balancer |
| [HBASE-13192](https://issues.apache.org/jira/browse/HBASE-13192) | IntegrationTestBulkLoad doesn't wait for table modification sometimes leading to spurious test failures |  Major | . |
| [HBASE-13191](https://issues.apache.org/jira/browse/HBASE-13191) | mvn site fails on jenkins due to permgen |  Major | . |
| [HBASE-13208](https://issues.apache.org/jira/browse/HBASE-13208) | Patch build should match the patch filename and not the whole relative URL in findBranchNameFromPatchName |  Trivial | . |
| [HBASE-13206](https://issues.apache.org/jira/browse/HBASE-13206) | Fix TableLock tableName log format |  Trivial | . |
| [HBASE-13165](https://issues.apache.org/jira/browse/HBASE-13165) | Fix docs and scripts for default max heaps size after HBASE-11804 |  Minor | documentation, scripts |
| [HBASE-13174](https://issues.apache.org/jira/browse/HBASE-13174) | Apply HBASE-11804 to Windows scripts |  Major | scripts |
| [HBASE-13196](https://issues.apache.org/jira/browse/HBASE-13196) | Add info about default number of versions when using the export tool |  Major | documentation |
| [HBASE-13181](https://issues.apache.org/jira/browse/HBASE-13181) | TestHRegionReplayEvents.testReplayBulkLoadEvent fails frequently. |  Minor | . |
| [HBASE-13186](https://issues.apache.org/jira/browse/HBASE-13186) | HBase build error due to checkstyle |  Major | build |
| [HBASE-12931](https://issues.apache.org/jira/browse/HBASE-12931) | The existing KeyValues in memstore are not removed completely after inserting cell into memStore |  Minor | . |
| [HBASE-12723](https://issues.apache.org/jira/browse/HBASE-12723) | Update ACL matrix to reflect reality |  Major | . |
| [HBASE-13023](https://issues.apache.org/jira/browse/HBASE-13023) | Document multiWAL |  Major | documentation, wal |
| [HBASE-12468](https://issues.apache.org/jira/browse/HBASE-12468) | AUTHORIZATIONS should be part of Visibility Label Docs |  Major | documentation |
| [HBASE-13135](https://issues.apache.org/jira/browse/HBASE-13135) | Move replication ops mgmt stuff from Javadoc to Ref Guide |  Major | documentation, Replication |
| [HBASE-12969](https://issues.apache.org/jira/browse/HBASE-12969) | Parameter Validation is not there for shell script, local-master-backup.sh and local-regionservers.sh |  Minor | scripts |
| [HBASE-13084](https://issues.apache.org/jira/browse/HBASE-13084) | Add labels to VisibilityLabelsCache asynchronously causes TestShell flakey |  Major | test |
| [HBASE-13150](https://issues.apache.org/jira/browse/HBASE-13150) | TestMasterObserver failing disable table at end of test |  Major | test |
| [HBASE-13163](https://issues.apache.org/jira/browse/HBASE-13163) | Add HBase version to header and footer of HTML and PDF docs |  Major | documentation |
| [HBASE-13076](https://issues.apache.org/jira/browse/HBASE-13076) | Table can be forcibly enabled in AssignmentManager during table disabling. |  Major | master, Region Assignment |
| [HBASE-13155](https://issues.apache.org/jira/browse/HBASE-13155) | Fix TestPrefixTree |  Major | test |
| [HBASE-13156](https://issues.apache.org/jira/browse/HBASE-13156) | Fix minor rat violation recently introduced (asciidoctor.css). |  Major | . |
| [HBASE-13052](https://issues.apache.org/jira/browse/HBASE-13052) | Explain each region split policy |  Major | documentation |
| [HBASE-13146](https://issues.apache.org/jira/browse/HBASE-13146) | Race Condition in ScheduledChore and ChoreService |  Major | regionserver |
| [HBASE-13145](https://issues.apache.org/jira/browse/HBASE-13145) | TestNamespaceAuditor.testRegionMerge is flaky |  Major | test |
| [HBASE-13139](https://issues.apache.org/jira/browse/HBASE-13139) | Clean up missing JAVA\_HOME message in bin/hbase-config.sh |  Trivial | shell |
| [HBASE-13115](https://issues.apache.org/jira/browse/HBASE-13115) | Fix the usage of remote user in thrift doAs implementation. |  Major | . |
| [HBASE-13141](https://issues.apache.org/jira/browse/HBASE-13141) | IntegrationTestAcidGuarantees returns incorrect values for getColumnFamilies |  Major | integration tests |
| [HBASE-13134](https://issues.apache.org/jira/browse/HBASE-13134) | mutateRow and checkAndMutate apis don't throw region level exceptions |  Major | . |
| [HBASE-13123](https://issues.apache.org/jira/browse/HBASE-13123) | Minor bug in ROW bloom filter |  Minor | . |
| [HBASE-13133](https://issues.apache.org/jira/browse/HBASE-13133) | NPE when running TestSplitLogManager |  Major | . |
| [HBASE-13131](https://issues.apache.org/jira/browse/HBASE-13131) | ReplicationAdmin leaks connections if there's an error in the constructor |  Critical | Replication |
| [HBASE-12924](https://issues.apache.org/jira/browse/HBASE-12924) | HRegionServer#MovedRegionsCleaner Chore does not start |  Minor | . |
| [HBASE-13119](https://issues.apache.org/jira/browse/HBASE-13119) | FileLink should implement equals |  Major | . |
| [HBASE-13112](https://issues.apache.org/jira/browse/HBASE-13112) | quota.rb, security.rb and visibility\_labels.rb leak connection |  Major | shell |
| [HBASE-13111](https://issues.apache.org/jira/browse/HBASE-13111) | truncate\_preserve command is failing with undefined method error |  Major | shell |
| [HBASE-13102](https://issues.apache.org/jira/browse/HBASE-13102) | Fix Pseudo-distributed Mode which was broken in 1.0.0 |  Major | . |
| [HBASE-13048](https://issues.apache.org/jira/browse/HBASE-13048) | Use hbase.crypto.wal.algorithm in SecureProtobufLogReader while decrypting the data |  Minor | . |
| [HBASE-13085](https://issues.apache.org/jira/browse/HBASE-13085) | Security issue in the implementation of Rest gataway 'doAs' proxy user support |  Critical | REST, security |
| [HBASE-13077](https://issues.apache.org/jira/browse/HBASE-13077) | BoundedCompletionService doesn't pass trace info to server |  Major | hbase |
| [HBASE-13083](https://issues.apache.org/jira/browse/HBASE-13083) | Master can be dead-locked while assigning META. |  Major | master, Region Assignment |
| [HBASE-12953](https://issues.apache.org/jira/browse/HBASE-12953) | RegionServer is not functionally working with AysncRpcClient in secure mode |  Critical | security |
| [HBASE-13001](https://issues.apache.org/jira/browse/HBASE-13001) | NullPointer in master logs for table.jsp |  Trivial | . |
| [HBASE-13032](https://issues.apache.org/jira/browse/HBASE-13032) | Migration of states should be performed once META is assigned and onlined. |  Major | . |
| [HBASE-13070](https://issues.apache.org/jira/browse/HBASE-13070) | Fix possibly zero length family and qualifier is TestCacheOnWrite |  Major | test |
| [HBASE-13081](https://issues.apache.org/jira/browse/HBASE-13081) | Branch precommit builds are not updating to branch head before patch application |  Major | . |
| [HBASE-13069](https://issues.apache.org/jira/browse/HBASE-13069) | Thrift Http Server returns an error code of 500 instead of 401 when authentication fails |  Minor | . |
| [HBASE-13072](https://issues.apache.org/jira/browse/HBASE-13072) | BucketCache.evictBlock returns true if block does not exist |  Major | BlockCache |
| [HBASE-13075](https://issues.apache.org/jira/browse/HBASE-13075) | TableInputFormatBase spuriously warning about multiple initializeTable calls |  Minor | mapreduce |
| [HBASE-13036](https://issues.apache.org/jira/browse/HBASE-13036) | Meta scanner should use its own threadpool |  Major | . |
| [HBASE-13065](https://issues.apache.org/jira/browse/HBASE-13065) | Increasing -Xmx when running TestDistributedLogSplitting |  Major | test |
| [HBASE-13066](https://issues.apache.org/jira/browse/HBASE-13066) | Fix typo in AsyncRpcChannel |  Major | IPC/RPC |
| [HBASE-12102](https://issues.apache.org/jira/browse/HBASE-12102) | Duplicate keys in HBase.RegionServer metrics JSON |  Minor | . |
| [HBASE-13061](https://issues.apache.org/jira/browse/HBASE-13061) | RegionStates can remove wrong region from server holdings |  Major | Region Assignment |
| [HBASE-13050](https://issues.apache.org/jira/browse/HBASE-13050) | Hbase shell create\_namespace command throws ArrayIndexOutOfBoundException for (invalid) empty text input. |  Trivial | . |
| [HBASE-12948](https://issues.apache.org/jira/browse/HBASE-12948) | Calling Increment#addColumn on the same column multiple times produces wrong result |  Critical | Client, regionserver |
| [HBASE-13040](https://issues.apache.org/jira/browse/HBASE-13040) | Possible failure of TestHMasterRPCException |  Major | test |
| [HBASE-13055](https://issues.apache.org/jira/browse/HBASE-13055) | HRegion FIXED\_OVERHEAD missed one boolean |  Major | . |
| [HBASE-12412](https://issues.apache.org/jira/browse/HBASE-12412) | update the ref guide(currently Example 10.2) to show "update an existing CF" with the new API modifyFamily in master |  Minor | documentation |
| [HBASE-13041](https://issues.apache.org/jira/browse/HBASE-13041) | TestEnableTableHandler should not call AssignmentManager#assign concurrently with master |  Major | integration tests |
| [HBASE-13047](https://issues.apache.org/jira/browse/HBASE-13047) | Add "HBase Configuration" link missing on the table details pages |  Trivial | Operability |
| [HBASE-13011](https://issues.apache.org/jira/browse/HBASE-13011) | TestLoadIncrementalHFiles is flakey when using AsyncRpcClient as client implementation |  Major | . |
| [HBASE-13049](https://issues.apache.org/jira/browse/HBASE-13049) | wal\_roll ruby command doesn't work. |  Major | shell |
| [HBASE-13029](https://issues.apache.org/jira/browse/HBASE-13029) | Table state should be deleted from META as a last operation in DeleteTableHandler |  Major | master |
| [HBASE-13007](https://issues.apache.org/jira/browse/HBASE-13007) | Fix the test timeouts being caused by ChoreService |  Major | . |
| [HBASE-12920](https://issues.apache.org/jira/browse/HBASE-12920) | hadoopqa should compile with different hadoop versions |  Major | . |
| [HBASE-12999](https://issues.apache.org/jira/browse/HBASE-12999) | Make foreground\_start return the correct exit code |  Major | shell |
| [HBASE-13004](https://issues.apache.org/jira/browse/HBASE-13004) | Make possible to explain why HBaseTestingUtility.waitFor fails |  Minor | test |
| [HBASE-12961](https://issues.apache.org/jira/browse/HBASE-12961) | Negative values in read and write region server metrics |  Minor | regionserver |
| [HBASE-12964](https://issues.apache.org/jira/browse/HBASE-12964) | Add the ability for hbase-daemon.sh to start in the foreground |  Major | . |
| [HBASE-7332](https://issues.apache.org/jira/browse/HBASE-7332) | [webui] HMaster webui should display the number of regions a table has. |  Minor | UI |
| [HBASE-12951](https://issues.apache.org/jira/browse/HBASE-12951) | TestHCM.testConnectionClose is flakey when using AsyncRpcClient as client implementation |  Major | IPC/RPC |
| [HBASE-12877](https://issues.apache.org/jira/browse/HBASE-12877) | Hbase documentation- a referenced link is not working |  Minor | documentation |
| [HBASE-12922](https://issues.apache.org/jira/browse/HBASE-12922) | Post-asciidoc conversion fix-ups part 2 |  Major | documentation |
| [HBASE-12902](https://issues.apache.org/jira/browse/HBASE-12902) | Post-asciidoc conversion fix-ups |  Major | documentation |
| [HBASE-12903](https://issues.apache.org/jira/browse/HBASE-12903) | Fix configuration which enables secure bulk load |  Major | Coprocessors, documentation |
| [HBASE-12871](https://issues.apache.org/jira/browse/HBASE-12871) | Document JDK versions supported by each release missing in new documentation |  Minor | documentation |
| [HBASE-12845](https://issues.apache.org/jira/browse/HBASE-12845) | ByteBufferOutputStream should grow as direct buffer if the initial buffer is also direct BB |  Minor | . |
| [HBASE-12858](https://issues.apache.org/jira/browse/HBASE-12858) | Remove unneeded files under src/main/docbkx |  Major | documentation |
| [HBASE-11983](https://issues.apache.org/jira/browse/HBASE-11983) | HRegion constructors should not create HLog |  Major | wal |
| [HBASE-12849](https://issues.apache.org/jira/browse/HBASE-12849) | LoadIncrementalHFiles should use unmanaged connection in branch-1 |  Major | mapreduce |
| [HBASE-12777](https://issues.apache.org/jira/browse/HBASE-12777) | Multi-page book has broken links that work in the single-page version |  Major | documentation |
| [HBASE-12838](https://issues.apache.org/jira/browse/HBASE-12838) | After HBASE-5162 RSRpcServices accidentally applies mutations twice |  Major | . |
| [HBASE-12775](https://issues.apache.org/jira/browse/HBASE-12775) | CompressionTest ate my HFile (sigh!) |  Major | test |
| [HBASE-12772](https://issues.apache.org/jira/browse/HBASE-12772) | TestPerColumnFamilyFlush failing |  Major | test |
| [HBASE-12749](https://issues.apache.org/jira/browse/HBASE-12749) | Tighten HFileLink api to enable non-snapshot uses |  Major | snapshots |
| [HBASE-12734](https://issues.apache.org/jira/browse/HBASE-12734) | TestPerColumnFamilyFlush.testCompareStoreFileCount is flakey |  Minor | . |
| [HBASE-12703](https://issues.apache.org/jira/browse/HBASE-12703) | Cleanup TestClientPushback for repeatability |  Minor | test |
| [HBASE-12688](https://issues.apache.org/jira/browse/HBASE-12688) | Update site with a bootstrap-based UI |  Major | website |
| [HBASE-12682](https://issues.apache.org/jira/browse/HBASE-12682) | compaction thread throttle value is not correct in hbase-default.xml |  Major | regionserver |
| [HBASE-12690](https://issues.apache.org/jira/browse/HBASE-12690) | list\_quotas command is failing with not able to load Java class |  Major | shell |
| [HBASE-12687](https://issues.apache.org/jira/browse/HBASE-12687) | Book is missing styling |  Major | documentation |
| [HBASE-12693](https://issues.apache.org/jira/browse/HBASE-12693) | [docs] nit fix in HBase and MapReduce section |  Major | documentation |
| [HBASE-12677](https://issues.apache.org/jira/browse/HBASE-12677) | Update replication docs to clarify terminology |  Major | documentation |
| [HBASE-9763](https://issues.apache.org/jira/browse/HBASE-9763) | Scan javadoc doesn't fully capture semantics of start and stop row |  Minor | documentation |
| [HBASE-11153](https://issues.apache.org/jira/browse/HBASE-11153) | Document that http webUI's should redirect to https when enabled |  Minor | documentation, master, regionserver, UI |
| [HBASE-12540](https://issues.apache.org/jira/browse/HBASE-12540) | TestRegionServerMetrics#testMobMetrics test failure |  Major | test |
| [HBASE-12628](https://issues.apache.org/jira/browse/HBASE-12628) | Update instructions for running shell tests using maven. |  Minor | documentation, shell |
| [HBASE-12553](https://issues.apache.org/jira/browse/HBASE-12553) | request value is not consistent for restoreSnapshot in audit logs |  Minor | security |
| [HBASE-12548](https://issues.apache.org/jira/browse/HBASE-12548) | Improve debuggability of IntegrationTestTimeBoundedRequestsWithRegionReplicas |  Minor | . |
| [HBASE-12603](https://issues.apache.org/jira/browse/HBASE-12603) | Remove javadoc warnings introduced due to removal of unused imports |  Major | . |
| [HBASE-12474](https://issues.apache.org/jira/browse/HBASE-12474) | Incorrect handling of default namespace in user\_permission command. |  Minor | . |
| [HBASE-12552](https://issues.apache.org/jira/browse/HBASE-12552) | listSnapshots should list only owned snapshots for non-super user |  Major | snapshots |
| [HBASE-12073](https://issues.apache.org/jira/browse/HBASE-12073) | Shell command user\_permission fails on the table created by user if he is not global admin. |  Minor | . |
| [HBASE-12535](https://issues.apache.org/jira/browse/HBASE-12535) | NPE in WALFactory close under contention for getInstance |  Major | Replication, wal |
| [HBASE-12532](https://issues.apache.org/jira/browse/HBASE-12532) | TestFilter failing occasionally with ExitCodeException doing chmod since HBASE-10378 |  Major | test |
| [HBASE-12421](https://issues.apache.org/jira/browse/HBASE-12421) | Clarify ACL concepts and best practices |  Major | documentation, security |
| [HBASE-12488](https://issues.apache.org/jira/browse/HBASE-12488) | Small bug in publish\_hbase\_website.sh script |  Minor | scripts |
| [HBASE-12347](https://issues.apache.org/jira/browse/HBASE-12347) | Fix the edge case where Hadoop QA's parsing of attached patches breaks the JIRA status checker in dev-support/rebase\_all\_git\_branches.sh |  Minor | scripts |
| [HBASE-12409](https://issues.apache.org/jira/browse/HBASE-12409) | Add actual tunable parameters for finding optimal # of regions per RS |  Major | documentation, Performance |
| [HBASE-12397](https://issues.apache.org/jira/browse/HBASE-12397) | CopyTable fails to compile with the Hadoop 1 profile |  Major | . |
| [HBASE-12418](https://issues.apache.org/jira/browse/HBASE-12418) | Multiple branches for MOB feature breaking git for case insensitive filesystems |  Blocker | . |
| [HBASE-12380](https://issues.apache.org/jira/browse/HBASE-12380) | TestRegionServerNoMaster#testMultipleOpen is flaky after HBASE-11760 |  Major | test |
| [HBASE-12326](https://issues.apache.org/jira/browse/HBASE-12326) | Document scanner timeout workarounds in troubleshooting section |  Major | documentation |
| [HBASE-12283](https://issues.apache.org/jira/browse/HBASE-12283) | Clean up some checkstyle errors |  Major | . |
| [HBASE-12307](https://issues.apache.org/jira/browse/HBASE-12307) | Remove unused Imports in hbase-client and hbase-common |  Minor | . |
| [HBASE-12192](https://issues.apache.org/jira/browse/HBASE-12192) | Remove EventHandlerListener |  Major | master |
| [HBASE-12186](https://issues.apache.org/jira/browse/HBASE-12186) | Formatting error in Table 8.2. Examples of Visibility Expressions |  Major | documentation |
| [HBASE-12216](https://issues.apache.org/jira/browse/HBASE-12216) | Lower closed region logging level |  Minor | . |
| [HBASE-12201](https://issues.apache.org/jira/browse/HBASE-12201) | Close the writers in the MOB sweep tool |  Minor | . |
| [HBASE-11998](https://issues.apache.org/jira/browse/HBASE-11998) | Document a workflow for cherry-picking a fix to a different branch |  Major | documentation |
| [HBASE-12193](https://issues.apache.org/jira/browse/HBASE-12193) | Add missing docbook file to git |  Major | documentation |
| [HBASE-12172](https://issues.apache.org/jira/browse/HBASE-12172) | Disable flakey TestRegionReplicaReplicationEndpoint and make fixing it a blocker on 1.0 |  Major | test |
| [HBASE-11957](https://issues.apache.org/jira/browse/HBASE-11957) | Backport to 0.94 HBASE-5974 Scanner retry behavior with RPC timeout on next() seems incorrect |  Critical | . |
| [HBASE-12030](https://issues.apache.org/jira/browse/HBASE-12030) | Wrong compaction report and assert when MOB compaction switches to minor |  Critical | Compaction, regionserver |
| [HBASE-12005](https://issues.apache.org/jira/browse/HBASE-12005) | Split/merge fails if master restarts before PONR |  Major | . |
| [HBASE-12027](https://issues.apache.org/jira/browse/HBASE-12027) | The ZooKeeperWatcher in HMobStore only uses the default conf |  Major | . |
| [HBASE-11987](https://issues.apache.org/jira/browse/HBASE-11987) | Make zk-less table states backward compatible. |  Major | . |
| [HBASE-11968](https://issues.apache.org/jira/browse/HBASE-11968) | If MOB is enabled, it should make sure hfile v3 is being used. |  Major | . |
| [HBASE-11721](https://issues.apache.org/jira/browse/HBASE-11721) | jdiff script no longer works as usage instructions indicate |  Major | scripts |
| [HBASE-8674](https://issues.apache.org/jira/browse/HBASE-8674) | JUnit and Surefire TRUNK-HBASE-2 plugins need a new home |  Major | build |
| [HBASE-11855](https://issues.apache.org/jira/browse/HBASE-11855) | HBase handbook chapter 18.9 out of date |  Minor | documentation |
| [HBASE-11689](https://issues.apache.org/jira/browse/HBASE-11689) | Track meta in transition |  Major | Region Assignment |
| [HBASE-11834](https://issues.apache.org/jira/browse/HBASE-11834) | TestHRegionBusyWait.testParallelAppendWithMemStoreFlush fails sporadically |  Major | test |
| [HBASE-11732](https://issues.apache.org/jira/browse/HBASE-11732) | Should not preemptively offline a region |  Major | Region Assignment |
| [HBASE-11658](https://issues.apache.org/jira/browse/HBASE-11658) | Piped commands to hbase shell should return non-zero if shell command failed. |  Major | shell |
| [HBASE-11333](https://issues.apache.org/jira/browse/HBASE-11333) | Remove deprecated class MetaMigrationConvertingToPB |  Trivial | master |
| [HBASE-11661](https://issues.apache.org/jira/browse/HBASE-11661) | Quickstart chapter claims standalone mode has multiple processes |  Minor | documentation |
| [HBASE-11629](https://issues.apache.org/jira/browse/HBASE-11629) | Operational concerns for Replication should call out ZooKeeper |  Major | documentation, Replication |
| [HBASE-11640](https://issues.apache.org/jira/browse/HBASE-11640) | Add syntax highlighting support to HBase Ref Guide programlistings |  Major | documentation |
| [HBASE-11648](https://issues.apache.org/jira/browse/HBASE-11648) | Typo of config: hbase.hstore.compaction.ratio in book.xml |  Minor | Compaction |
| [HBASE-11539](https://issues.apache.org/jira/browse/HBASE-11539) | Expand info about contributing to and building documentation |  Major | documentation |
| [HBASE-11316](https://issues.apache.org/jira/browse/HBASE-11316) | Expand info about compactions beyond HBASE-11120 |  Major | Compaction, documentation |
| [HBASE-11522](https://issues.apache.org/jira/browse/HBASE-11522) | Move Replication information into the Ref Guide |  Major | documentation |
| [HBASE-11560](https://issues.apache.org/jira/browse/HBASE-11560) | hbase.regionserver.global.memstore.size is documented twice |  Major | . |
| [HBASE-11529](https://issues.apache.org/jira/browse/HBASE-11529) | Images and CSS still don't work properly on both html and html-single book |  Major | documentation |
| [HBASE-11521](https://issues.apache.org/jira/browse/HBASE-11521) | Modify pom.xml to copy the images/ and css/ directories to the right location for the Ref Guide to see them correctly |  Critical | documentation |
| [HBASE-8473](https://issues.apache.org/jira/browse/HBASE-8473) | add note to ref guide about snapshots and ec2 reverse dns requirements. |  Major | documentation, snapshots |
| [HBASE-11499](https://issues.apache.org/jira/browse/HBASE-11499) | AsyncProcess.buildDetailedErrorMessage concatenates strings using + in a loop |  Trivial | Client |
| [HBASE-11500](https://issues.apache.org/jira/browse/HBASE-11500) | Possible null pointer dereference of regionLocation in ReversedScannerCallable |  Minor | Client |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20228](https://issues.apache.org/jira/browse/HBASE-20228) | [Umbrella] Verify 1.2.7 shell works against 2.0.0 server |  Blocker | shell |
| [HBASE-20272](https://issues.apache.org/jira/browse/HBASE-20272) | TestAsyncTable#testCheckAndMutateWithTimeRange fails due to TableExistsException |  Major | . |
| [HBASE-20107](https://issues.apache.org/jira/browse/HBASE-20107) | Add a test case for HBASE-14317 |  Minor | wal |
| [HBASE-20052](https://issues.apache.org/jira/browse/HBASE-20052) | TestRegionOpen#testNonExistentRegionReplica fails due to NPE |  Major | . |
| [HBASE-20031](https://issues.apache.org/jira/browse/HBASE-20031) | Unable to run integration test using mvn due to missing HBaseClassTestRule |  Major | . |
| [HBASE-19949](https://issues.apache.org/jira/browse/HBASE-19949) | TestRSGroupsWithACL fails with ExceptionInInitializerError |  Major | . |
| [HBASE-19869](https://issues.apache.org/jira/browse/HBASE-19869) | Wrong class name used in TestLockManager |  Trivial | . |
| [HBASE-19832](https://issues.apache.org/jira/browse/HBASE-19832) | TestConfServlet#testWriteJson fails against hadoop3 due to spelling change |  Minor | . |
| [HBASE-19514](https://issues.apache.org/jira/browse/HBASE-19514) | Use random port for TestJMXListener |  Minor | . |
| [HBASE-19289](https://issues.apache.org/jira/browse/HBASE-19289) | CommonFSUtils$StreamLacksCapabilityException: hflush when running test against hadoop3 beta1 |  Critical | . |
| [HBASE-19342](https://issues.apache.org/jira/browse/HBASE-19342) | fix TestTableBasedReplicationSourceManagerImpl#testRemovePeerMetricsCleanup |  Major | test |
| [HBASE-19266](https://issues.apache.org/jira/browse/HBASE-19266) | TestAcidGuarantees should cover adaptive in-memory compaction |  Minor | . |
| [HBASE-19288](https://issues.apache.org/jira/browse/HBASE-19288) | Intermittent test failure in TestHStore.testRunDoubleMemStoreCompactors |  Major | test |
| [HBASE-19299](https://issues.apache.org/jira/browse/HBASE-19299) | Assert only one Connection is constructed when calculating splits in a MultiTableInputFormat |  Minor | test |
| [HBASE-19273](https://issues.apache.org/jira/browse/HBASE-19273) | IntegrationTestBulkLoad#installSlowingCoproc() uses read-only HTableDescriptor |  Major | . |
| [HBASE-19248](https://issues.apache.org/jira/browse/HBASE-19248) | TestZooKeeper#testMultipleZK fails due to missing method getKeepAliveZooKeeperWatcher |  Critical | Zookeeper |
| [HBASE-19237](https://issues.apache.org/jira/browse/HBASE-19237) | TestMaster.testMasterOpsWhileSplitting fails |  Major | . |
| [HBASE-16051](https://issues.apache.org/jira/browse/HBASE-16051) | TestScannerHeartbeatMessages fails on some machines |  Major | test |
| [HBASE-19026](https://issues.apache.org/jira/browse/HBASE-19026) | TestLockProcedure#testRemoteNamespaceLockRecovery fails in master |  Major | . |
| [HBASE-18902](https://issues.apache.org/jira/browse/HBASE-18902) | TestCoprocessorServiceBackwardCompatibility fails |  Major | . |
| [HBASE-18632](https://issues.apache.org/jira/browse/HBASE-18632) | TestMultiParallel#testFlushCommitsWithAbort fails in master branch |  Major | test |
| [HBASE-18147](https://issues.apache.org/jira/browse/HBASE-18147) | nightly job to check health of active branches |  Major | community, test |
| [HBASE-17806](https://issues.apache.org/jira/browse/HBASE-17806) | TestRSGroups#testMoveServersAndTables is flaky in master branch |  Major | . |
| [HBASE-17703](https://issues.apache.org/jira/browse/HBASE-17703) | TestThriftServerCmdLine is flaky in master branch |  Major | . |
| [HBASE-17672](https://issues.apache.org/jira/browse/HBASE-17672) | "Grant should set access rights appropriately" test fails |  Major | . |
| [HBASE-17657](https://issues.apache.org/jira/browse/HBASE-17657) | TestZKAsyncRegistry is flaky |  Major | . |
| [HBASE-17628](https://issues.apache.org/jira/browse/HBASE-17628) | Local mode of mini cluster shouldn't use hdfs |  Minor | . |
| [HBASE-17474](https://issues.apache.org/jira/browse/HBASE-17474) | Reduce frequency of NoSuchMethodException when calling setStoragePolicy() |  Minor | . |
| [HBASE-17371](https://issues.apache.org/jira/browse/HBASE-17371) | Enhance 'HBaseContextSuite @ distributedScan to test HBase client' with filter |  Minor | . |
| [HBASE-17246](https://issues.apache.org/jira/browse/HBASE-17246) | TestSerialReplication#testRegionMerge fails in master branch |  Major | . |
| [HBASE-17189](https://issues.apache.org/jira/browse/HBASE-17189) | TestMasterObserver#wasModifyTableActionCalled uses wrong variables |  Minor | test |
| [HBASE-17080](https://issues.apache.org/jira/browse/HBASE-17080) | rest.TestTableResource fails in master branch |  Major | . |
| [HBASE-17120](https://issues.apache.org/jira/browse/HBASE-17120) | TestAssignmentListener#testAssignmentListener fails |  Minor | . |
| [HBASE-16975](https://issues.apache.org/jira/browse/HBASE-16975) | Disable two subtests of TestSerialReplication |  Minor | . |
| [HBASE-16274](https://issues.apache.org/jira/browse/HBASE-16274) | Add more peer tests to replication\_admin\_test |  Minor | . |
| [HBASE-16781](https://issues.apache.org/jira/browse/HBASE-16781) | Fix flaky TestMasterProcedureWalLease |  Minor | proc-v2, test |
| [HBASE-16794](https://issues.apache.org/jira/browse/HBASE-16794) | TestDispatchMergingRegionsProcedure#testMergeRegionsConcurrently is flaky |  Minor | test |
| [HBASE-16791](https://issues.apache.org/jira/browse/HBASE-16791) | Fix TestDispatchMergingRegionsProcedure |  Minor | test |
| [HBASE-16777](https://issues.apache.org/jira/browse/HBASE-16777) | Fix flaky TestMasterProcedureEvents |  Trivial | proc-v2, test |
| [HBASE-16778](https://issues.apache.org/jira/browse/HBASE-16778) | Move testIllegalTableDescriptor out from TestFromClientSide |  Trivial | test |
| [HBASE-16776](https://issues.apache.org/jira/browse/HBASE-16776) | Remove duplicated versions of countRow() in tests |  Trivial | test |
| [HBASE-16725](https://issues.apache.org/jira/browse/HBASE-16725) | Don't let flushThread hang in TestHRegion |  Minor | . |
| [HBASE-16671](https://issues.apache.org/jira/browse/HBASE-16671) | Split TestExportSnapshot |  Minor | snapshots, test |
| [HBASE-16349](https://issues.apache.org/jira/browse/HBASE-16349) | TestClusterId may hang during cluster shutdown |  Minor | . |
| [HBASE-16634](https://issues.apache.org/jira/browse/HBASE-16634) | Speedup TestExportSnapshot |  Minor | snapshots, test |
| [HBASE-16639](https://issues.apache.org/jira/browse/HBASE-16639) | TestProcedureInMemoryChore#testChoreAddAndRemove occasionally fails |  Minor | . |
| [HBASE-16418](https://issues.apache.org/jira/browse/HBASE-16418) | Reduce duration of sleep waiting for region reopen in IntegrationTestBulkLoad#installSlowingCoproc() |  Minor | . |
| [HBASE-16185](https://issues.apache.org/jira/browse/HBASE-16185) | TestReplicationSmallTests fails in master branch |  Major | . |
| [HBASE-16049](https://issues.apache.org/jira/browse/HBASE-16049) | TestRowProcessorEndpoint is failing on Apache Builds |  Major | . |
| [HBASE-15939](https://issues.apache.org/jira/browse/HBASE-15939) | Two shell test failures on master |  Major | . |
| [HBASE-15923](https://issues.apache.org/jira/browse/HBASE-15923) | Shell rows counter test fails |  Major | . |
| [HBASE-15760](https://issues.apache.org/jira/browse/HBASE-15760) | TestBlockEvictionFromClient#testParallelGetsAndScanWithWrappedRegionScanner fails in master branch |  Minor | . |
| [HBASE-15679](https://issues.apache.org/jira/browse/HBASE-15679) | Assertion on wrong variable in TestReplicationThrottler#testThrottling |  Minor | . |
| [HBASE-13372](https://issues.apache.org/jira/browse/HBASE-13372) | Unit tests for SplitTransaction and RegionMergeTransaction listeners |  Major | . |
| [HBASE-15420](https://issues.apache.org/jira/browse/HBASE-15420) | TestCacheConfig failed after HBASE-15338 |  Minor | test |
| [HBASE-15192](https://issues.apache.org/jira/browse/HBASE-15192) | TestRegionMergeTransactionOnCluster#testCleanMergeReference is flaky |  Minor | . |
| [HBASE-14584](https://issues.apache.org/jira/browse/HBASE-14584) | TestNamespacesInstanceModel fails on jdk8 |  Major | REST, test |
| [HBASE-14758](https://issues.apache.org/jira/browse/HBASE-14758) | Add UT case for unchecked error/exception thrown in AsyncProcess#sendMultiAction |  Minor | Client, test |
| [HBASE-14728](https://issues.apache.org/jira/browse/HBASE-14728) | TestRowCounter is broken in master |  Major | . |
| [HBASE-14688](https://issues.apache.org/jira/browse/HBASE-14688) | Cleanup MOB tests |  Trivial | mob |
| [HBASE-14466](https://issues.apache.org/jira/browse/HBASE-14466) | Remove duplicated code from MOB snapshot tests |  Trivial | mob, test |
| [HBASE-14344](https://issues.apache.org/jira/browse/HBASE-14344) | Add timeouts to TestHttpServerLifecycle |  Minor | test |
| [HBASE-14310](https://issues.apache.org/jira/browse/HBASE-14310) | test-patch.sh should handle spurious non-zero exit code from maven |  Minor | . |
| [HBASE-14293](https://issues.apache.org/jira/browse/HBASE-14293) | TestStochasticBalancerJmxMetrics intermittently fails due to port conflict |  Minor | . |
| [HBASE-14277](https://issues.apache.org/jira/browse/HBASE-14277) | TestRegionServerHostname.testRegionServerHostname may fail at host with a case sensitive name |  Minor | test |
| [HBASE-14210](https://issues.apache.org/jira/browse/HBASE-14210) | Create test for cell level ACLs involving user group |  Major | test |
| [HBASE-14200](https://issues.apache.org/jira/browse/HBASE-14200) | Separate RegionReplica subtests of TestStochasticLoadBalancer into TestStochasticLoadBalancer2 |  Minor | . |
| [HBASE-14197](https://issues.apache.org/jira/browse/HBASE-14197) | TestRegionServerHostname#testInvalidRegionServerHostnameAbortsServer fails in Jenkins |  Minor | . |
| [HBASE-13940](https://issues.apache.org/jira/browse/HBASE-13940) | IntegrationTestBulkLoad needs option to specify output folders used by test |  Major | integration tests |
| [HBASE-13609](https://issues.apache.org/jira/browse/HBASE-13609) | TestFastFail is still failing |  Major | test |
| [HBASE-13591](https://issues.apache.org/jira/browse/HBASE-13591) | TestHBaseFsck is flakey |  Major | hbck |
| [HBASE-13413](https://issues.apache.org/jira/browse/HBASE-13413) | Create an integration test for Replication |  Minor | integration tests |
| [HBASE-13280](https://issues.apache.org/jira/browse/HBASE-13280) | TestSecureRPC failed |  Minor | . |
| [HBASE-13182](https://issues.apache.org/jira/browse/HBASE-13182) | Test NamespaceAuditor/AccessController create/delete table is flaky |  Minor | test |
| [HBASE-13179](https://issues.apache.org/jira/browse/HBASE-13179) | TestMasterObserver deleteTable is flaky |  Minor | test |
| [HBASE-13106](https://issues.apache.org/jira/browse/HBASE-13106) | Ensure endpoint-only table coprocessors can be dynamically loaded |  Trivial | . |
| [HBASE-12992](https://issues.apache.org/jira/browse/HBASE-12992) | TestChoreService doesn't close services, that can break test on slow virtual hosts. |  Major | . |
| [HBASE-12764](https://issues.apache.org/jira/browse/HBASE-12764) | TestPerColumnFamilyFlush#testCompareStoreFileCount may fail due to new table not available |  Minor | . |
| [HBASE-11867](https://issues.apache.org/jira/browse/HBASE-11867) | TestSplitLogManager.testUnassignedTimeout is flaky |  Minor | . |
| [HBASE-11866](https://issues.apache.org/jira/browse/HBASE-11866) | TestDistributedLogSplitting is flaky |  Minor | . |
| [HBASE-11673](https://issues.apache.org/jira/browse/HBASE-11673) | TestIOFencing#testFencingAroundCompactionAfterWALSync fails |  Major | . |
| [HBASE-4744](https://issues.apache.org/jira/browse/HBASE-4744) | Remove @Ignore for testLogRollAfterSplitStart |  Critical | . |
| [HBASE-11616](https://issues.apache.org/jira/browse/HBASE-11616) | TestNamespaceUpgrade fails in trunk |  Major | . |
| [HBASE-11039](https://issues.apache.org/jira/browse/HBASE-11039) | [VisibilityController] Integration test for labeled data set mixing and filtered excise |  Critical | . |
| [HBASE-11461](https://issues.apache.org/jira/browse/HBASE-11461) | Compilation errors are not posted back to JIRA during QA run |  Minor | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20059](https://issues.apache.org/jira/browse/HBASE-20059) | Make sure documentation is updated for the offheap Bucket cache usage |  Critical | documentation |
| [HBASE-20454](https://issues.apache.org/jira/browse/HBASE-20454) | [DOC] Add note on perf to upgrade section |  Major | . |
| [HBASE-20347](https://issues.apache.org/jira/browse/HBASE-20347) | [DOC] upgrade section should warn about logging changes |  Critical | documentation, logging |
| [HBASE-20349](https://issues.apache.org/jira/browse/HBASE-20349) | [DOC] upgrade guide should call out removal of prefix-tree data block encoding |  Critical | documentation |
| [HBASE-17554](https://issues.apache.org/jira/browse/HBASE-17554) | Figure 2.0.0 Hadoop Version Support; update refguide |  Blocker | documentation |
| [HBASE-20253](https://issues.apache.org/jira/browse/HBASE-20253) | Error message is missing for restore\_snapshot |  Minor | shell |
| [HBASE-20380](https://issues.apache.org/jira/browse/HBASE-20380) | Put up 2.0.0RC0 |  Major | . |
| [HBASE-20149](https://issues.apache.org/jira/browse/HBASE-20149) | Purge dev javadoc from bin tarball (or make a separate tarball of javadoc) |  Critical | build, community, documentation |
| [HBASE-18828](https://issues.apache.org/jira/browse/HBASE-18828) | [2.0] Generate CHANGES.txt |  Blocker | . |
| [HBASE-20287](https://issues.apache.org/jira/browse/HBASE-20287) | After cluster startup list\_regions command fails on disabled table |  Minor | shell |
| [HBASE-20258](https://issues.apache.org/jira/browse/HBASE-20258) | Shell hangs when scanning a disabled table |  Major | . |
| [HBASE-20354](https://issues.apache.org/jira/browse/HBASE-20354) | [DOC] quickstart guide needs to include note about durability checks for standalone mode |  Blocker | documentation, hadoop3 |
| [HBASE-17730](https://issues.apache.org/jira/browse/HBASE-17730) | [DOC] Migration to 2.0 for coprocessors |  Blocker | documentation, migration |
| [HBASE-20337](https://issues.apache.org/jira/browse/HBASE-20337) | Update the doc on how to setup shortcircuit reads; its stale |  Major | . |
| [HBASE-20298](https://issues.apache.org/jira/browse/HBASE-20298) | Doc change in read/write/total accounting metrics |  Critical | documentation |
| [HBASE-16848](https://issues.apache.org/jira/browse/HBASE-16848) | Usage for show\_peer\_tableCFs command doesn't include peer |  Minor | . |
| [HBASE-20254](https://issues.apache.org/jira/browse/HBASE-20254) | Incorrect help message for merge\_region |  Minor | shell |
| [HBASE-17819](https://issues.apache.org/jira/browse/HBASE-17819) | Reduce the heap overhead for BucketCache |  Critical | BucketCache |
| [HBASE-20245](https://issues.apache.org/jira/browse/HBASE-20245) | HTrace commands do not work |  Major | . |
| [HBASE-20241](https://issues.apache.org/jira/browse/HBASE-20241) | splitormerge\_enabled does not work |  Critical | shell |
| [HBASE-20202](https://issues.apache.org/jira/browse/HBASE-20202) | [AMv2] Don't move region if its a split parent or offlined |  Critical | amv2 |
| [HBASE-20247](https://issues.apache.org/jira/browse/HBASE-20247) | Set version as 2.0.0 in branch-2.0 in prep for first RC |  Major | . |
| [HBASE-20232](https://issues.apache.org/jira/browse/HBASE-20232) | [LOGGING] Formatting around close and flush |  Major | . |
| [HBASE-20190](https://issues.apache.org/jira/browse/HBASE-20190) | Fix default for MIGRATE\_TABLE\_STATE\_FROM\_ZK\_KEY |  Critical | . |
| [HBASE-20180](https://issues.apache.org/jira/browse/HBASE-20180) | Avoid Class::newInstance |  Major | . |
| [HBASE-20178](https://issues.apache.org/jira/browse/HBASE-20178) | [AMv2] Throw exception if hostile environment |  Major | amv2 |
| [HBASE-20173](https://issues.apache.org/jira/browse/HBASE-20173) | [AMv2] DisableTableProcedure concurrent to ServerCrashProcedure can deadlock |  Critical | amv2 |
| [HBASE-19093](https://issues.apache.org/jira/browse/HBASE-19093) | Check Admin/Table to ensure all operations go via AccessControl |  Blocker | . |
| [HBASE-19114](https://issues.apache.org/jira/browse/HBASE-19114) | Split out o.a.h.h.zookeeper from hbase-server and hbase-client |  Major | . |
| [HBASE-18758](https://issues.apache.org/jira/browse/HBASE-18758) | [TEST][compat 1-2] Test delegation tokens continue to work when hbase1 going against hbase2 cluster |  Critical | API |
| [HBASE-20113](https://issues.apache.org/jira/browse/HBASE-20113) | Move branch-2 version from 2.0.0-beta-2-SNAPSHOT to 2.0.0-beta-2 |  Major | . |
| [HBASE-20100](https://issues.apache.org/jira/browse/HBASE-20100) | TestEnableTableProcedure flakey |  Major | amv2, flakey |
| [HBASE-19400](https://issues.apache.org/jira/browse/HBASE-19400) | Add missing security checks in MasterRpcServices |  Major | . |
| [HBASE-20069](https://issues.apache.org/jira/browse/HBASE-20069) | fix existing findbugs errors in hbase-server |  Critical | findbugs |
| [HBASE-20036](https://issues.apache.org/jira/browse/HBASE-20036) | TestAvoidCellReferencesIntoShippedBlocks timed out |  Major | . |
| [HBASE-20083](https://issues.apache.org/jira/browse/HBASE-20083) | Fix findbugs error for ReplicationSyncUp |  Major | findbugs |
| [HBASE-20019](https://issues.apache.org/jira/browse/HBASE-20019) | Document the ColumnValueFilter |  Minor | documentation |
| [HBASE-20048](https://issues.apache.org/jira/browse/HBASE-20048) | Revert serial replication feature |  Blocker | Replication |
| [HBASE-20044](https://issues.apache.org/jira/browse/HBASE-20044) | TestClientClusterStatus is flakey |  Major | flakey |
| [HBASE-19554](https://issues.apache.org/jira/browse/HBASE-19554) | AbstractTestDLS.testThreeRSAbort sometimes fails in pre commit |  Major | Recovery, wal |
| [HBASE-20038](https://issues.apache.org/jira/browse/HBASE-20038) | TestLockProcedure.testTimeout is flakey |  Major | proc-v2, test |
| [HBASE-20041](https://issues.apache.org/jira/browse/HBASE-20041) | cannot start mini mapreduce cluster for ITs |  Major | . |
| [HBASE-20035](https://issues.apache.org/jira/browse/HBASE-20035) | .TestQuotaStatusRPCs.testQuotaStatusFromMaster failed with NPEs and RuntimeExceptions |  Major | . |
| [HBASE-20039](https://issues.apache.org/jira/browse/HBASE-20039) | move testhbasetestingutility mr tests to hbase-mapreduce |  Major | . |
| [HBASE-20021](https://issues.apache.org/jira/browse/HBASE-20021) | TestFromClientSideWithCoprocessor is flakey |  Major | . |
| [HBASE-20029](https://issues.apache.org/jira/browse/HBASE-20029) | @Ignore TestQuotaThrottle and TestReplicasClient#testCancelOfMultiGet |  Major | . |
| [HBASE-20015](https://issues.apache.org/jira/browse/HBASE-20015) | TestMergeTableRegionsProcedure and TestRegionMergeTransactionOnCluster flakey |  Major | flakey |
| [HBASE-20013](https://issues.apache.org/jira/browse/HBASE-20013) | TestZKPermissionWatcher is flakey |  Major | flakey |
| [HBASE-20014](https://issues.apache.org/jira/browse/HBASE-20014) | TestAdmin1 Times out |  Major | . |
| [HBASE-19978](https://issues.apache.org/jira/browse/HBASE-19978) | The keepalive logic is incomplete in ProcedureExecutor |  Major | . |
| [HBASE-20011](https://issues.apache.org/jira/browse/HBASE-20011) | Disable TestRestoreSnapshotFromClientWithRegionReplicas; it is flakey. Needs attention. |  Major | read replicas |
| [HBASE-19903](https://issues.apache.org/jira/browse/HBASE-19903) | Split TestShell so it will not time out |  Major | shell, test |
| [HBASE-20000](https://issues.apache.org/jira/browse/HBASE-20000) | Remove the quantum logic in FairQueue, always put high priority queue in front |  Major | proc-v2 |
| [HBASE-19116](https://issues.apache.org/jira/browse/HBASE-19116) | Currently the tail of hfiles with CellComparator\* classname makes it so hbase1 can't open hbase2 written hfiles; fix |  Critical | HFile, migration |
| [HBASE-19965](https://issues.apache.org/jira/browse/HBASE-19965) | Fix flaky TestAsyncRegionAdminApi |  Critical | . |
| [HBASE-19960](https://issues.apache.org/jira/browse/HBASE-19960) | Doc test timeouts and test categories in hbase2 |  Major | . |
| [HBASE-19942](https://issues.apache.org/jira/browse/HBASE-19942) | Fix flaky TestSimpleRpcScheduler |  Major | . |
| [HBASE-19956](https://issues.apache.org/jira/browse/HBASE-19956) | Remove category as a consideration timing out tests; set all test to timeout at 10minutes regardless |  Major | . |
| [HBASE-19951](https://issues.apache.org/jira/browse/HBASE-19951) | Cleanup the explicit timeout value for test method |  Major | . |
| [HBASE-19791](https://issues.apache.org/jira/browse/HBASE-19791) | TestZKAsyncRegistry hangs |  Critical | . |
| [HBASE-19840](https://issues.apache.org/jira/browse/HBASE-19840) | Flakey TestMetaWithReplicas |  Major | flakey, test |
| [HBASE-19927](https://issues.apache.org/jira/browse/HBASE-19927) | TestFullLogReconstruction flakey |  Major | wal |
| [HBASE-19841](https://issues.apache.org/jira/browse/HBASE-19841) | Tests against hadoop3 fail with StreamLacksCapabilityException |  Major | . |
| [HBASE-19944](https://issues.apache.org/jira/browse/HBASE-19944) | Fix timeout TestVisibilityLabelsWithCustomVisLabService |  Major | . |
| [HBASE-19931](https://issues.apache.org/jira/browse/HBASE-19931) | TestMetaWithReplicas failing 100% of the time in testHBaseFsckWithMetaReplicas |  Major | . |
| [HBASE-19837](https://issues.apache.org/jira/browse/HBASE-19837) | Flakey TestRegionLoad |  Major | flakey, test |
| [HBASE-19803](https://issues.apache.org/jira/browse/HBASE-19803) | False positive for the HBASE-Find-Flaky-Tests job |  Major | . |
| [HBASE-19910](https://issues.apache.org/jira/browse/HBASE-19910) | TestBucketCache TimesOut |  Major | . |
| [HBASE-19916](https://issues.apache.org/jira/browse/HBASE-19916) | TestCacheOnWrite Times Out |  Major | . |
| [HBASE-19914](https://issues.apache.org/jira/browse/HBASE-19914) | Refactor TestVisibilityLabelsOnNewVersionBehaviorTable |  Major | test |
| [HBASE-19868](https://issues.apache.org/jira/browse/HBASE-19868) | TestCoprocessorWhitelistMasterObserver is flakey |  Major | flakey, test |
| [HBASE-19908](https://issues.apache.org/jira/browse/HBASE-19908) | TestCoprocessorShortCircuitRPC Timeout.... |  Major | . |
| [HBASE-19909](https://issues.apache.org/jira/browse/HBASE-19909) | TestRegionLocationFinder Timeout |  Major | . |
| [HBASE-19928](https://issues.apache.org/jira/browse/HBASE-19928) | TestVisibilityLabelsOnNewVersionBehaviorTable fails |  Major | test |
| [HBASE-19918](https://issues.apache.org/jira/browse/HBASE-19918) | Promote TestAsyncClusterAdminApi to LargeTests |  Major | test |
| [HBASE-19896](https://issues.apache.org/jira/browse/HBASE-19896) | Fix ScanInfo.customize() to update only the requested options |  Major | Coprocessors |
| [HBASE-19895](https://issues.apache.org/jira/browse/HBASE-19895) | Add keepDeletedCells option in ScanOptions for customizing scanInfo in pre-hooks |  Major | Coprocessors |
| [HBASE-19913](https://issues.apache.org/jira/browse/HBASE-19913) | Split TestStochasticLoadBalancer2 |  Major | test |
| [HBASE-19839](https://issues.apache.org/jira/browse/HBASE-19839) | Flakey TestMergeTableRegionsProcedure & TestSplitTableRegionProcedure |  Major | flakey, test |
| [HBASE-19911](https://issues.apache.org/jira/browse/HBASE-19911) | Convert some tests from small to medium because they are timing out: TestNettyRpcServer, TestClientClusterStatus, TestCheckTestClasses |  Major | . |
| [HBASE-19887](https://issues.apache.org/jira/browse/HBASE-19887) | Do not overwrite the surefire junit listener property in the pom of sub modules |  Major | build |
| [HBASE-19891](https://issues.apache.org/jira/browse/HBASE-19891) | Up nightly test run timeout from 6 hours to 8 |  Major | . |
| [HBASE-19811](https://issues.apache.org/jira/browse/HBASE-19811) | Fix findbugs and error-prone warnings in hbase-server (branch-2) |  Major | . |
| [HBASE-19885](https://issues.apache.org/jira/browse/HBASE-19885) | Promote TestAssignmentManager to LargeTests |  Major | . |
| [HBASE-19866](https://issues.apache.org/jira/browse/HBASE-19866) | TestRegionServerReportForDuty doesn't timeout |  Major | rpc |
| [HBASE-19882](https://issues.apache.org/jira/browse/HBASE-19882) | Promote TestProcedureManager to MediumTests |  Major | test |
| [HBASE-19877](https://issues.apache.org/jira/browse/HBASE-19877) | hbase-common and hbase-zookeeper don't add the log4j.properties to the resource path for testing |  Critical | test |
| [HBASE-19870](https://issues.apache.org/jira/browse/HBASE-19870) | Fix the NPE in ReadOnlyZKClient#run |  Major | . |
| [HBASE-19881](https://issues.apache.org/jira/browse/HBASE-19881) | Promote TestRegionReplicaReplicationEndpoint to LargeTests |  Major | test |
| [HBASE-19880](https://issues.apache.org/jira/browse/HBASE-19880) | Promote TestFuzzyRowFilterEndToEnd to LargeTests |  Major | test |
| [HBASE-19879](https://issues.apache.org/jira/browse/HBASE-19879) | Promote TestAcidGuaranteesXXX to LargeTests |  Major | test |
| [HBASE-19867](https://issues.apache.org/jira/browse/HBASE-19867) | Split TestStochasticLoadBalancer into several small tests |  Major | test |
| [HBASE-19862](https://issues.apache.org/jira/browse/HBASE-19862) | Fix TestTokenAuthentication - fake RegionCoprocessorEnvironment is not of type HasRegionServerServices |  Major | . |
| [HBASE-19846](https://issues.apache.org/jira/browse/HBASE-19846) | Fix findbugs and error-prone warnings in hbase-rest (branch-2) |  Major | . |
| [HBASE-19845](https://issues.apache.org/jira/browse/HBASE-19845) | Fix findbugs and error-prone warnings in hbase-rsgroup (branch-2) |  Major | . |
| [HBASE-19847](https://issues.apache.org/jira/browse/HBASE-19847) | Fix findbugs and error-prone warnings in hbase-thrift (branch-2) |  Major | . |
| [HBASE-19827](https://issues.apache.org/jira/browse/HBASE-19827) | Addendum for Flakey TestAssignmentManager |  Major | flakey, test |
| [HBASE-18963](https://issues.apache.org/jira/browse/HBASE-18963) | Remove MultiRowMutationProcessor and implement mutateRows... methods using batchMutate() |  Major | regionserver |
| [HBASE-19527](https://issues.apache.org/jira/browse/HBASE-19527) | Make ExecutorService threads daemon=true. |  Major | . |
| [HBASE-19810](https://issues.apache.org/jira/browse/HBASE-19810) | Fix findbugs and error-prone warnings in hbase-metrics (branch-2) |  Major | . |
| [HBASE-19809](https://issues.apache.org/jira/browse/HBASE-19809) | Fix findbugs and error-prone warnings in hbase-procedure (branch-2) |  Major | . |
| [HBASE-19793](https://issues.apache.org/jira/browse/HBASE-19793) | Minor improvements on Master/RS startup |  Major | . |
| [HBASE-19795](https://issues.apache.org/jira/browse/HBASE-19795) | Move the tests which only need zookeeper in TestZooKeeper to hbase-zookeeper module |  Major | . |
| [HBASE-19772](https://issues.apache.org/jira/browse/HBASE-19772) | Do not close connection to zk when there are still pending request in ReadOnlyZKClient |  Major | Zookeeper |
| [HBASE-19787](https://issues.apache.org/jira/browse/HBASE-19787) | Fix or disable tests broken in branch-2 so can cut beta-1 |  Critical | . |
| [HBASE-19746](https://issues.apache.org/jira/browse/HBASE-19746) | Add default impl to Cell#getType |  Critical | . |
| [HBASE-19743](https://issues.apache.org/jira/browse/HBASE-19743) | Disable TestMemstoreLABWithoutPool |  Major | test |
| [HBASE-19731](https://issues.apache.org/jira/browse/HBASE-19731) | TestFromClientSide#testCheckAndDeleteWithCompareOp and testNullQualifier are flakey |  Critical | test |
| [HBASE-19604](https://issues.apache.org/jira/browse/HBASE-19604) | Fix Checkstyle errors in hbase-protocol-shaded |  Minor | . |
| [HBASE-19581](https://issues.apache.org/jira/browse/HBASE-19581) | Fix Checkstyle error in hbase-external-blockcache |  Trivial | . |
| [HBASE-19667](https://issues.apache.org/jira/browse/HBASE-19667) | Get rid of MasterEnvironment#supportGroupCPs |  Major | Coprocessors |
| [HBASE-19670](https://issues.apache.org/jira/browse/HBASE-19670) | Workaround: Purge User API building from branch-2 so can make a beta-1 |  Major | website |
| [HBASE-19428](https://issues.apache.org/jira/browse/HBASE-19428) | Deprecate the compareTo(Row) |  Major | . |
| [HBASE-19282](https://issues.apache.org/jira/browse/HBASE-19282) | CellChunkMap Benchmarking and User Interface |  Major | . |
| [HBASE-19660](https://issues.apache.org/jira/browse/HBASE-19660) | Up default retries from 10 to 15 and blocking store files limit from 10 to 16 |  Major | . |
| [HBASE-19133](https://issues.apache.org/jira/browse/HBASE-19133) | Transfer big cells or upserted/appended cells into MSLAB upon flattening to CellChunkMap |  Major | . |
| [HBASE-19653](https://issues.apache.org/jira/browse/HBASE-19653) | Reduce the default hbase.client.start.log.errors.counter |  Trivial | defaults |
| [HBASE-19626](https://issues.apache.org/jira/browse/HBASE-19626) | Rename Cell.DataType to Cell.Type |  Minor | . |
| [HBASE-19648](https://issues.apache.org/jira/browse/HBASE-19648) | Move branch-2 version from 2.0.0-beta-1-SNAPSHOT to 2.0.0-beta-1 |  Major | . |
| [HBASE-19609](https://issues.apache.org/jira/browse/HBASE-19609) | Fix Checkstyle errors in hbase-metrics |  Minor | metrics |
| [HBASE-19628](https://issues.apache.org/jira/browse/HBASE-19628) | ByteBufferCell should extend ExtendedCell |  Major | . |
| [HBASE-19629](https://issues.apache.org/jira/browse/HBASE-19629) | RawCell#getTags should return the Iterator\<Tag\> in order to avoid iterating through whole tag array at once |  Major | . |
| [HBASE-19605](https://issues.apache.org/jira/browse/HBASE-19605) | Fix Checkstyle errors in hbase-metrics-api |  Minor | metrics |
| [HBASE-19602](https://issues.apache.org/jira/browse/HBASE-19602) | Cleanup the usage of ReplicationAdmin from document |  Minor | . |
| [HBASE-19502](https://issues.apache.org/jira/browse/HBASE-19502) | Make sure we have closed all StoreFileScanners if we fail to open any StoreFileScanners |  Major | regionserver, Scanners |
| [HBASE-19591](https://issues.apache.org/jira/browse/HBASE-19591) | Cleanup the usage of ReplicationAdmin from hbase-shell |  Major | . |
| [HBASE-10092](https://issues.apache.org/jira/browse/HBASE-10092) | Move to slf4j |  Critical | . |
| [HBASE-19575](https://issues.apache.org/jira/browse/HBASE-19575) | add copy constructor to Mutation |  Major | . |
| [HBASE-19566](https://issues.apache.org/jira/browse/HBASE-19566) | Fix Checkstyle errors in hbase-client-project |  Trivial | . |
| [HBASE-19567](https://issues.apache.org/jira/browse/HBASE-19567) | ClassNotFoundException: org.apache.hadoop.hbase.KeyValue$RawBytesComparator starting 2.0.0 over a 0.98.25 data. |  Major | HFile |
| [HBASE-19468](https://issues.apache.org/jira/browse/HBASE-19468) | FNFE during scans and flushes |  Critical | regionserver, Scanners |
| [HBASE-19556](https://issues.apache.org/jira/browse/HBASE-19556) | Remove TestAssignmentManager#testGoodSplit, which no longer make sense |  Minor | . |
| [HBASE-19494](https://issues.apache.org/jira/browse/HBASE-19494) | Create simple WALKey filter that can be plugged in on the Replication Sink |  Major | Replication |
| [HBASE-19480](https://issues.apache.org/jira/browse/HBASE-19480) | Enable Checkstyle in hbase-annotations |  Trivial | . |
| [HBASE-19481](https://issues.apache.org/jira/browse/HBASE-19481) | Enable Checkstyle in hbase-error-prone |  Trivial | . |
| [HBASE-19538](https://issues.apache.org/jira/browse/HBASE-19538) | Remove unnecessary semicolons in hbase-client |  Minor | . |
| [HBASE-19539](https://issues.apache.org/jira/browse/HBASE-19539) | Remove unnecessary semicolons in hbase-common |  Minor | . |
| [HBASE-18440](https://issues.apache.org/jira/browse/HBASE-18440) | ITs and Actions modify immutable TableDescriptors |  Major | integration tests |
| [HBASE-19112](https://issues.apache.org/jira/browse/HBASE-19112) | Suspect methods on Cell to be deprecated |  Blocker | Client |
| [HBASE-19474](https://issues.apache.org/jira/browse/HBASE-19474) | Bring down number of Checkstyle errors in hbase-zookeeper |  Minor | Zookeeper |
| [HBASE-19479](https://issues.apache.org/jira/browse/HBASE-19479) | Fix Checkstyle error in hbase-shell |  Trivial | shell |
| [HBASE-19497](https://issues.apache.org/jira/browse/HBASE-19497) | Fix findbugs and error-prone warnings in hbase-common (branch-2) |  Major | . |
| [HBASE-19498](https://issues.apache.org/jira/browse/HBASE-19498) | Fix findbugs and error-prone warnings in hbase-client (branch-2) |  Major | . |
| [HBASE-19272](https://issues.apache.org/jira/browse/HBASE-19272) | Deal with HBCK tests disabled by HBASE-14614 AMv2 when HBCK works again... |  Major | hbck |
| [HBASE-15536](https://issues.apache.org/jira/browse/HBASE-15536) | Make AsyncFSWAL as our default WAL |  Critical | wal |
| [HBASE-19505](https://issues.apache.org/jira/browse/HBASE-19505) | Disable ByteBufferPool by default at HM |  Major | . |
| [HBASE-19462](https://issues.apache.org/jira/browse/HBASE-19462) | Deprecate all addImmutable methods in Put |  Major | . |
| [HBASE-19000](https://issues.apache.org/jira/browse/HBASE-19000) | Group multiple block cache clear requests per server |  Major | . |
| [HBASE-19213](https://issues.apache.org/jira/browse/HBASE-19213) | Align check and mutate operations in Table and AsyncTable |  Minor | API |
| [HBASE-19427](https://issues.apache.org/jira/browse/HBASE-19427) | Add TimeRange support into Append to optimize for counters |  Major | . |
| [HBASE-16890](https://issues.apache.org/jira/browse/HBASE-16890) | Analyze the performance of AsyncWAL and fix the same |  Blocker | wal |
| [HBASE-19375](https://issues.apache.org/jira/browse/HBASE-19375) | Fix import order in hbase-thrift |  Trivial | Thrift |
| [HBASE-19373](https://issues.apache.org/jira/browse/HBASE-19373) | Fix Checkstyle error in hbase-annotations |  Trivial | . |
| [HBASE-19360](https://issues.apache.org/jira/browse/HBASE-19360) | Remove unused imports from hbase-zookeeper module |  Minor | Zookeeper |
| [HBASE-19301](https://issues.apache.org/jira/browse/HBASE-19301) | Provide way for CPs to create short circuited connection with custom configurations |  Major | Coprocessors |
| [HBASE-19439](https://issues.apache.org/jira/browse/HBASE-19439) | Mark ShortCircuitMasterConnection  with InterfaceAudience Private |  Major | . |
| [HBASE-19430](https://issues.apache.org/jira/browse/HBASE-19430) | Remove the SettableTimestamp and SettableSequenceId |  Major | . |
| [HBASE-19295](https://issues.apache.org/jira/browse/HBASE-19295) | The Configuration returned by CPEnv should be read-only. |  Major | Coprocessors |
| [HBASE-18112](https://issues.apache.org/jira/browse/HBASE-18112) | Write RequestTooBigException back to client for NettyRpcServer |  Major | IPC/RPC |
| [HBASE-19426](https://issues.apache.org/jira/browse/HBASE-19426) | Move has() and setTimestamp() to Mutation |  Major | Client |
| [HBASE-19399](https://issues.apache.org/jira/browse/HBASE-19399) | Purge curator dependency from hbase-client |  Major | Client, Zookeeper |
| [HBASE-19344](https://issues.apache.org/jira/browse/HBASE-19344) | improve asyncWAL by using Independent thread for netty #IO in FanOutOneBlockAsyncDFSOutput |  Major | wal |
| [HBASE-19346](https://issues.apache.org/jira/browse/HBASE-19346) | Use EventLoopGroup to create AsyncFSOutput |  Major | wal |
| [HBASE-19362](https://issues.apache.org/jira/browse/HBASE-19362) | Remove unused imports from hbase-thrift module |  Minor | Thrift |
| [HBASE-19096](https://issues.apache.org/jira/browse/HBASE-19096) | Add RowMutions batch support in AsyncTable |  Major | . |
| [HBASE-17049](https://issues.apache.org/jira/browse/HBASE-17049) | Do not issue sync request when there are still entries in ringbuffer |  Critical | wal |
| [HBASE-19242](https://issues.apache.org/jira/browse/HBASE-19242) | Add MOB compact support for AsyncAdmin |  Blocker | Admin, mob |
| [HBASE-19122](https://issues.apache.org/jira/browse/HBASE-19122) | preCompact and preFlush can bypass by returning null scanner; shut it down |  Critical | Coprocessors, Scanners |
| [HBASE-19313](https://issues.apache.org/jira/browse/HBASE-19313) | Call blockUntilConnected when constructing ZKAsyncRegistry(temporary workaround) |  Major | asyncclient, Client, Zookeeper |
| [HBASE-19269](https://issues.apache.org/jira/browse/HBASE-19269) | Reenable TestShellRSGroups |  Major | test |
| [HBASE-19276](https://issues.apache.org/jira/browse/HBASE-19276) | RegionPlan should correctly implement equals and hashCode |  Major | . |
| [HBASE-18911](https://issues.apache.org/jira/browse/HBASE-18911) | Unify Admin and AsyncAdmin's methods name |  Major | . |
| [HBASE-19268](https://issues.apache.org/jira/browse/HBASE-19268) | Enable Replica tests that were disabled by Proc-V2 AM in HBASE-14614 |  Major | test |
| [HBASE-19009](https://issues.apache.org/jira/browse/HBASE-19009) | implement modifyTable and enable/disableTableReplication for AsyncAdmin |  Major | . |
| [HBASE-18964](https://issues.apache.org/jira/browse/HBASE-18964) | Deprecate RowProcessor and processRowsWithLocks() APIs that take RowProcessor as an argument |  Major | regionserver |
| [HBASE-19270](https://issues.apache.org/jira/browse/HBASE-19270) | Reenable TestRegionMergeTransactionOnCluster#testMergeWithReplicas disable by HBASE-14614 |  Major | Region Assignment |
| [HBASE-19278](https://issues.apache.org/jira/browse/HBASE-19278) | Reenable cleanup in test teardown in TestAccessController3 disabled by HBASE-14614 |  Major | Region Assignment |
| [HBASE-19235](https://issues.apache.org/jira/browse/HBASE-19235) | CoprocessorEnvironment should be exposed to CPs |  Minor | Coprocessors |
| [HBASE-19243](https://issues.apache.org/jira/browse/HBASE-19243) | Start mini cluster once before class for TestFIFOCompactionPolicy |  Major | test |
| [HBASE-18423](https://issues.apache.org/jira/browse/HBASE-18423) | Fix TestMetaWithReplicas |  Major | test |
| [HBASE-18962](https://issues.apache.org/jira/browse/HBASE-18962) | Support atomic BatchOperations through batchMutate() |  Major | regionserver |
| [HBASE-19127](https://issues.apache.org/jira/browse/HBASE-19127) | Set State.SPLITTING, MERGING, MERGING\_NEW, SPLITTING\_NEW properly in RegionStatesNode |  Major | . |
| [HBASE-19220](https://issues.apache.org/jira/browse/HBASE-19220) | Async tests time out talking to zk; 'clusterid came back null' |  Major | test |
| [HBASE-19002](https://issues.apache.org/jira/browse/HBASE-19002) | Introduce more examples to show how to intercept normal region operations |  Minor | Coprocessors |
| [HBASE-18624](https://issues.apache.org/jira/browse/HBASE-18624) | Added support for clearing BlockCache based on table name |  Major | . |
| [HBASE-19203](https://issues.apache.org/jira/browse/HBASE-19203) | Update Hadoop version used for build to 2.7.4 |  Major | . |
| [HBASE-18961](https://issues.apache.org/jira/browse/HBASE-18961) | doMiniBatchMutate() is big, split it into smaller methods |  Major | regionserver |
| [HBASE-19197](https://issues.apache.org/jira/browse/HBASE-19197) | Move version on branch-2 from 2.0.0-alpha4 to 2.0.0-beta-1.SNAPSHOT |  Major | . |
| [HBASE-18950](https://issues.apache.org/jira/browse/HBASE-18950) | Remove Optional parameters in AsyncAdmin interface |  Blocker | Client |
| [HBASE-19131](https://issues.apache.org/jira/browse/HBASE-19131) | Add the ClusterStatus hook and cleanup other hooks which can be replaced by ClusterStatus hook |  Major | Coprocessors |
| [HBASE-19095](https://issues.apache.org/jira/browse/HBASE-19095) | Add CP hooks in RegionObserver for in memory compaction |  Major | Coprocessors |
| [HBASE-19152](https://issues.apache.org/jira/browse/HBASE-19152) | Update refguide 'how to build an RC' and the make\_rc.sh script |  Trivial | build |
| [HBASE-18972](https://issues.apache.org/jira/browse/HBASE-18972) | Use Builder pattern to remove nullable parameters for coprocessor methods in RawAsyncTable interface |  Blocker | Client |
| [HBASE-19141](https://issues.apache.org/jira/browse/HBASE-19141) |  [compat 1-2] getClusterStatus always return empty ClusterStatus |  Critical | API |
| [HBASE-18770](https://issues.apache.org/jira/browse/HBASE-18770) | Remove bypass method in ObserverContext and implement the 'bypass' logic case by case |  Critical | Coprocessors |
| [HBASE-18375](https://issues.apache.org/jira/browse/HBASE-18375) | The pool chunks from ChunkCreator are deallocated while in pool because there is no reference to them |  Critical | . |
| [HBASE-19136](https://issues.apache.org/jira/browse/HBASE-19136) | Set branch-2 version to 2.0.0-alpha4 from 2.0.0-alpha4-SNAPSHOT |  Major | . |
| [HBASE-19031](https://issues.apache.org/jira/browse/HBASE-19031) | Align exist method in Table and AsyncTable interfaces |  Critical | asyncclient, Client |
| [HBASE-18995](https://issues.apache.org/jira/browse/HBASE-18995) | Move methods that are for internal usage from CellUtil to Private util class |  Critical | . |
| [HBASE-18906](https://issues.apache.org/jira/browse/HBASE-18906) | Provide Region#waitForFlushes API |  Critical | Coprocessors |
| [HBASE-19090](https://issues.apache.org/jira/browse/HBASE-19090) | Add config 'hbase.systemtables.compacting.memstore.type' to hbase-default.xml |  Major | . |
| [HBASE-19048](https://issues.apache.org/jira/browse/HBASE-19048) | Cleanup MasterObserver hooks which takes IA private params |  Major | Coprocessors |
| [HBASE-19029](https://issues.apache.org/jira/browse/HBASE-19029) | Align RPC timout methods in Table and AsyncTableBase |  Critical | asyncclient, Client |
| [HBASE-19057](https://issues.apache.org/jira/browse/HBASE-19057) | Fix other code review comments about FilterList Improvement |  Blocker | Filters |
| [HBASE-19074](https://issues.apache.org/jira/browse/HBASE-19074) | Miscellaneous Observer cleanups |  Major | Coprocessors |
| [HBASE-19070](https://issues.apache.org/jira/browse/HBASE-19070) | temporarily make the mvnsite nightly test non-voting. |  Major | build |
| [HBASE-18754](https://issues.apache.org/jira/browse/HBASE-18754) | Get rid of Writable from TimeRangeTracker |  Major | . |
| [HBASE-19053](https://issues.apache.org/jira/browse/HBASE-19053) | Split out o.a.h.h.http from hbase-server into a separate module |  Major | . |
| [HBASE-19069](https://issues.apache.org/jira/browse/HBASE-19069) | Do not wrap the original CompactionLifeCycleTracker when calling CP hooks |  Blocker | Compaction, Coprocessors |
| [HBASE-18873](https://issues.apache.org/jira/browse/HBASE-18873) | Hide protobufs in GlobalQuotaSettings |  Critical | . |
| [HBASE-19010](https://issues.apache.org/jira/browse/HBASE-19010) | Reimplement getMasterInfoPort for Admin |  Major | Client |
| [HBASE-19045](https://issues.apache.org/jira/browse/HBASE-19045) | Deprecate RegionObserver#postInstantiateDeleteTracker |  Major | Coprocessors |
| [HBASE-18977](https://issues.apache.org/jira/browse/HBASE-18977) | Reenable test of filterlist using MUST\_PASS\_ONE and two familyfilters |  Blocker | Filters |
| [HBASE-18945](https://issues.apache.org/jira/browse/HBASE-18945) | Make a IA.LimitedPrivate interface for CellComparator |  Major | . |
| [HBASE-18960](https://issues.apache.org/jira/browse/HBASE-18960) | A few bug fixes and minor improvements around batchMutate() |  Major | regionserver |
| [HBASE-18914](https://issues.apache.org/jira/browse/HBASE-18914) | Remove AsyncAdmin's methods which were already deprecated in Admin interface |  Major | . |
| [HBASE-18954](https://issues.apache.org/jira/browse/HBASE-18954) | Make \*CoprocessorHost classes private |  Major | Coprocessors |
| [HBASE-18966](https://issues.apache.org/jira/browse/HBASE-18966) | Use non-sync TimeRangeTracker as a replacement for TimeRange in ImmutableSegment |  Major | . |
| [HBASE-18747](https://issues.apache.org/jira/browse/HBASE-18747) | Introduce new example and helper classes to tell CP users how to do filtering on scanners |  Critical | Coprocessors |
| [HBASE-18411](https://issues.apache.org/jira/browse/HBASE-18411) | Dividing FiterList  into two separate sub-classes:  FilterListWithOR , FilterListWithAND |  Major | Filters |
| [HBASE-18108](https://issues.apache.org/jira/browse/HBASE-18108) | Procedure WALs are archived but not cleaned; fix |  Blocker | proc-v2 |
| [HBASE-18867](https://issues.apache.org/jira/browse/HBASE-18867) | maven enforcer plugin needs update to work with jdk9 |  Blocker | build |
| [HBASE-18981](https://issues.apache.org/jira/browse/HBASE-18981) | Address issues found by error-prone in hbase-client |  Trivial | . |
| [HBASE-18980](https://issues.apache.org/jira/browse/HBASE-18980) | Address issues found by error-prone in hbase-hadoop2-compat |  Trivial | . |
| [HBASE-18951](https://issues.apache.org/jira/browse/HBASE-18951) | Use Builder pattern to remove nullable parameters for checkAndXXX methods in RawAsyncTable/AsyncTable interface |  Blocker | asyncclient, Client |
| [HBASE-18160](https://issues.apache.org/jira/browse/HBASE-18160) | Fix incorrect  logic in FilterList.filterKeyValue |  Major | Filters |
| [HBASE-17678](https://issues.apache.org/jira/browse/HBASE-17678) | FilterList with MUST\_PASS\_ONE may lead to redundant cells returned |  Major | Filters |
| [HBASE-18957](https://issues.apache.org/jira/browse/HBASE-18957) | add test that confirms 2 FamilyFilters in a FilterList using MUST\_PASS\_ONE operator will return results that match either of the FamilyFilters and revert as needed to make it pass. |  Critical | Filters |
| [HBASE-18949](https://issues.apache.org/jira/browse/HBASE-18949) | Remove the CompactionRequest parameter in preCompactSelection |  Major | Coprocessors |
| [HBASE-18752](https://issues.apache.org/jira/browse/HBASE-18752) | Recalculate the TimeRange in flushing snapshot to store file |  Major | . |
| [HBASE-18909](https://issues.apache.org/jira/browse/HBASE-18909) | Deprecate Admin's methods which used String regex |  Major | Admin |
| [HBASE-18931](https://issues.apache.org/jira/browse/HBASE-18931) | Make ObserverContext an interface and remove private/testing methods |  Major | Coprocessors |
| [HBASE-18933](https://issues.apache.org/jira/browse/HBASE-18933) | set version number to 2.0.0-alpha4-SNAPSHOT following release of alpha3 |  Blocker | build |
| [HBASE-18815](https://issues.apache.org/jira/browse/HBASE-18815) | We need to pass something like CompactionRequest in CP to give user some information about the compaction |  Major | Coprocessors |
| [HBASE-18105](https://issues.apache.org/jira/browse/HBASE-18105) | [AMv2] Split/Merge need cleanup; currently they diverge and do not fully embrace AMv2 world |  Major | Region Assignment |
| [HBASE-18753](https://issues.apache.org/jira/browse/HBASE-18753) | Introduce the unsynchronized TimeRangeTracker |  Major | . |
| [HBASE-18010](https://issues.apache.org/jira/browse/HBASE-18010) | Connect CellChunkMap to be used for flattening in CompactingMemStore |  Major | . |
| [HBASE-18807](https://issues.apache.org/jira/browse/HBASE-18807) | Remove PB references from Observers for Quotas |  Major | . |
| [HBASE-18786](https://issues.apache.org/jira/browse/HBASE-18786) | FileNotFoundException should not be silently handled for primary region replicas |  Major | regionserver, Scanners |
| [HBASE-18823](https://issues.apache.org/jira/browse/HBASE-18823) | Apply RegionInfo to MasterObserver/RegionObserver/WALObserver |  Major | Coprocessors |
| [HBASE-17980](https://issues.apache.org/jira/browse/HBASE-17980) | Any HRegionInfo we give out should be immutable |  Major | . |
| [HBASE-18821](https://issues.apache.org/jira/browse/HBASE-18821) | Enforcer plugin NPEs again when generating site |  Major | website |
| [HBASE-18820](https://issues.apache.org/jira/browse/HBASE-18820) | assembly is missing hbase-replication |  Major | build |
| [HBASE-18819](https://issues.apache.org/jira/browse/HBASE-18819) | Set version number to 2.0.0-alpha3 from 2.0.0-alpha3-SNAPSHOT |  Major | . |
| [HBASE-14998](https://issues.apache.org/jira/browse/HBASE-14998) | Unify synchronous and asynchronous methods in Admin and cleanup |  Blocker | . |
| [HBASE-18718](https://issues.apache.org/jira/browse/HBASE-18718) | Document the coprocessor.Export |  Major | Coprocessors, documentation, tooling |
| [HBASE-18750](https://issues.apache.org/jira/browse/HBASE-18750) | Cleanup the docs saying "HTable use write buffer" |  Minor | documentation |
| [HBASE-18779](https://issues.apache.org/jira/browse/HBASE-18779) | Move CompareOperator to hbase-client module |  Critical | . |
| [HBASE-18769](https://issues.apache.org/jira/browse/HBASE-18769) | Make CompareFilter use generic CompareOperator instead of internal enum |  Major | . |
| [HBASE-14997](https://issues.apache.org/jira/browse/HBASE-14997) | Move compareOp and Comparators out of filter to client package |  Critical | . |
| [HBASE-18697](https://issues.apache.org/jira/browse/HBASE-18697) | Need a shaded hbase-mapreduce module |  Major | mapreduce |
| [HBASE-18749](https://issues.apache.org/jira/browse/HBASE-18749) | Apply the CF specific TimeRange from Scan to filter the segment scanner |  Minor | . |
| [HBASE-18739](https://issues.apache.org/jira/browse/HBASE-18739) | Make all TimeRange Constructors InterfaceAudience Private. |  Major | API |
| [HBASE-18721](https://issues.apache.org/jira/browse/HBASE-18721) | Cleanup unused configs and private declaration |  Minor | . |
| [HBASE-18700](https://issues.apache.org/jira/browse/HBASE-18700) | Document the new changes on mapreduce stuffs |  Major | mapreduce |
| [HBASE-18698](https://issues.apache.org/jira/browse/HBASE-18698) | MapreduceDependencyClasspathTool does not include hbase-server as a dependency |  Major | mapreduce |
| [HBASE-18692](https://issues.apache.org/jira/browse/HBASE-18692) | [compat 1-2] ByteBufferUtils.copyFromBufferToBuffer goes from void to int |  Major | API |
| [HBASE-18691](https://issues.apache.org/jira/browse/HBASE-18691) | [compat 1-2] HCD remove and removeConfiguration change return type |  Major | API |
| [HBASE-18688](https://issues.apache.org/jira/browse/HBASE-18688) | Upgrade commons-codec to 1.10 |  Major | dependencies |
| [HBASE-16324](https://issues.apache.org/jira/browse/HBASE-16324) | Remove LegacyScanQueryMatcher |  Critical | Compaction, regionserver |
| [HBASE-18687](https://issues.apache.org/jira/browse/HBASE-18687) | Add @since 2.0.0 to new classes |  Major | API |
| [HBASE-18448](https://issues.apache.org/jira/browse/HBASE-18448) | EndPoint example  for refreshing HFiles for stores |  Minor | Coprocessors |
| [HBASE-18656](https://issues.apache.org/jira/browse/HBASE-18656) | Address issues found by error-prone in hbase-common |  Major | build |
| [HBASE-18658](https://issues.apache.org/jira/browse/HBASE-18658) | Purge hokey hbase Service implementation; use (internal) Guava Service instead |  Major | . |
| [HBASE-18347](https://issues.apache.org/jira/browse/HBASE-18347) | Implement a BufferedMutator for async client |  Major | asyncclient, Client |
| [HBASE-18503](https://issues.apache.org/jira/browse/HBASE-18503) | Change \*\*\*Util and Master to use TableDescriptor and ColumnFamilyDescriptor |  Major | . |
| [HBASE-18103](https://issues.apache.org/jira/browse/HBASE-18103) | [AMv2] If Master gives OPEN to another, if original eventually succeeds, Master will kill it |  Critical | master, proc-v2 |
| [HBASE-18489](https://issues.apache.org/jira/browse/HBASE-18489) | Expose scan cursor in RawScanResultConsumer |  Major | asyncclient, Client, scan |
| [HBASE-18608](https://issues.apache.org/jira/browse/HBASE-18608) | AsyncConnection should return AsyncAdmin interface instead of the implemenation |  Major | Client |
| [HBASE-18595](https://issues.apache.org/jira/browse/HBASE-18595) | Set version in branch-2 from 2.0.0-alpha2-SNAPSHOT to 2.0.0-alpha2 |  Major | . |
| [HBASE-18553](https://issues.apache.org/jira/browse/HBASE-18553) | Expose scan cursor for asynchronous scanner |  Major | . |
| [HBASE-17994](https://issues.apache.org/jira/browse/HBASE-17994) | Add async client test to Performance Evaluation tool |  Major | . |
| [HBASE-18424](https://issues.apache.org/jira/browse/HBASE-18424) | Fix TestAsyncTableGetMultiThreaded |  Major | test |
| [HBASE-18238](https://issues.apache.org/jira/browse/HBASE-18238) | Address ruby static analysis for bin directory |  Major | . |
| [HBASE-18593](https://issues.apache.org/jira/browse/HBASE-18593) | Tell m2eclipse what to do w/ replacer plugin |  Trivial | build |
| [HBASE-18271](https://issues.apache.org/jira/browse/HBASE-18271) | Shade netty |  Blocker | rpc |
| [HBASE-18398](https://issues.apache.org/jira/browse/HBASE-18398) | Snapshot operation fails with FileNotFoundException |  Major | snapshots |
| [HBASE-18315](https://issues.apache.org/jira/browse/HBASE-18315) | Eliminate the findbugs warnings for hbase-rest |  Major | . |
| [HBASE-18102](https://issues.apache.org/jira/browse/HBASE-18102) | [SHELL] Purge close\_region command that allows by-pass of Master |  Major | Operability, shell |
| [HBASE-18231](https://issues.apache.org/jira/browse/HBASE-18231) | Deprecate and throw unsupported operation when Admin#closeRegion is called. |  Critical | Admin |
| [HBASE-18367](https://issues.apache.org/jira/browse/HBASE-18367) | Reduce ProcedureInfo usage |  Major | master, proc-v2 |
| [HBASE-18107](https://issues.apache.org/jira/browse/HBASE-18107) | [AMv2] Remove DispatchMergingRegionsRequest & DispatchMergingRegions |  Major | Region Assignment |
| [HBASE-18428](https://issues.apache.org/jira/browse/HBASE-18428) | IntegrationTestDDLMasterFailover uses old-style immutable column descriptors |  Major | integration tests |
| [HBASE-18419](https://issues.apache.org/jira/browse/HBASE-18419) | Update IntegrationTestIngestWithMOB and Actions to use ColumnFamily builders for modification |  Critical | integration tests |
| [HBASE-18420](https://issues.apache.org/jira/browse/HBASE-18420) | Some methods of Admin don't use ColumnFamilyDescriptor |  Major | . |
| [HBASE-17738](https://issues.apache.org/jira/browse/HBASE-17738) | BucketCache startup is slow |  Major | BucketCache |
| [HBASE-18308](https://issues.apache.org/jira/browse/HBASE-18308) | Eliminate the findbugs warnings for hbase-server |  Major | . |
| [HBASE-18052](https://issues.apache.org/jira/browse/HBASE-18052) | Add document for async admin |  Major | Client |
| [HBASE-18229](https://issues.apache.org/jira/browse/HBASE-18229) | create new Async Split API to embrace AM v2 |  Critical | proc-v2 |
| [HBASE-18342](https://issues.apache.org/jira/browse/HBASE-18342) | Add coprocessor service support for async admin |  Major | Client |
| [HBASE-17922](https://issues.apache.org/jira/browse/HBASE-17922) | TestRegionServerHostname always fails against hadoop 3.0.0-alpha2 |  Major | hadoop3 |
| [HBASE-18365](https://issues.apache.org/jira/browse/HBASE-18365) | Eliminate the findbugs warnings for hbase-common |  Major | . |
| [HBASE-18343](https://issues.apache.org/jira/browse/HBASE-18343) | Track the remaining unimplemented methods for async admin |  Major | Client |
| [HBASE-18268](https://issues.apache.org/jira/browse/HBASE-18268) | Eliminate the findbugs warnings for hbase-client |  Major | Client |
| [HBASE-18295](https://issues.apache.org/jira/browse/HBASE-18295) |  The result contains the cells across different rows |  Blocker | Scanners |
| [HBASE-18318](https://issues.apache.org/jira/browse/HBASE-18318) | Implement updateConfiguration/stopMaster/stopRegionServer/shutdown methods |  Major | Client |
| [HBASE-18316](https://issues.apache.org/jira/browse/HBASE-18316) | Implement async admin operations for draining region servers |  Major | Client |
| [HBASE-18317](https://issues.apache.org/jira/browse/HBASE-18317) | Implement async admin operations for Normalizer/CleanerChore/CatalogJanitor |  Major | Client |
| [HBASE-18319](https://issues.apache.org/jira/browse/HBASE-18319) | Implement getClusterStatus/getRegionLoad/getCompactionState/getLastMajorCompactionTimestamp methods |  Major | Client |
| [HBASE-18002](https://issues.apache.org/jira/browse/HBASE-18002) | Investigate why bucket cache filling up in file mode in an exisiting file  is slower |  Major | BucketCache |
| [HBASE-17201](https://issues.apache.org/jira/browse/HBASE-17201) | Edit of HFileBlock comments and javadoc |  Major | documentation |
| [HBASE-18240](https://issues.apache.org/jira/browse/HBASE-18240) | Add hbase-thirdparty, a project with hbase utility including an hbase-shaded-thirdparty module with guava, netty, etc. |  Major | dependencies, shading |
| [HBASE-18297](https://issues.apache.org/jira/browse/HBASE-18297) | Provide a AsyncAdminBuilder to create new AsyncAdmin instance |  Major | Client |
| [HBASE-18314](https://issues.apache.org/jira/browse/HBASE-18314) | Eliminate the findbugs warnings for hbase-examples |  Major | . |
| [HBASE-18264](https://issues.apache.org/jira/browse/HBASE-18264) | Update pom plugins |  Major | . |
| [HBASE-18283](https://issues.apache.org/jira/browse/HBASE-18283) | Provide a construct method which accept a thread pool for AsyncAdmin |  Major | Client |
| [HBASE-18293](https://issues.apache.org/jira/browse/HBASE-18293) | Only add the spotbugs dependency when jdk8 is active |  Major | build |
| [HBASE-18239](https://issues.apache.org/jira/browse/HBASE-18239) | Address ruby static analysis for shell module |  Major | . |
| [HBASE-14902](https://issues.apache.org/jira/browse/HBASE-14902) | Revert some of the stringency recently introduced by checkstyle tightening |  Major | . |
| [HBASE-18272](https://issues.apache.org/jira/browse/HBASE-18272) | Fix issue about RSGroupBasedLoadBalancer#roundRobinAssignment where BOGUS\_SERVER\_NAME is involved in two groups |  Major | Balancer |
| [HBASE-18234](https://issues.apache.org/jira/browse/HBASE-18234) | Revisit the async admin api |  Major | Client |
| [HBASE-18221](https://issues.apache.org/jira/browse/HBASE-18221) | Switch from pread to stream should happen under HStore's reentrant lock |  Major | Scanners |
| [HBASE-16242](https://issues.apache.org/jira/browse/HBASE-16242) | Upgrade Avro to 1.7.7 |  Major | dependencies |
| [HBASE-18104](https://issues.apache.org/jira/browse/HBASE-18104) | [AMv2] Enable aggregation of RPCs (assigns/unassigns, etc.) |  Major | Region Assignment |
| [HBASE-18213](https://issues.apache.org/jira/browse/HBASE-18213) | Add documentation about the new async client |  Major | asyncclient, Client, documentation |
| [HBASE-18220](https://issues.apache.org/jira/browse/HBASE-18220) | Compaction scanners need not reopen storefile scanners while trying to switch over from pread to stream |  Major | Compaction |
| [HBASE-17008](https://issues.apache.org/jira/browse/HBASE-17008) | Examples to make AsyncClient go down easy |  Critical | asyncclient, Client |
| [HBASE-18109](https://issues.apache.org/jira/browse/HBASE-18109) | Assign system tables first (priority) |  Critical | Region Assignment |
| [HBASE-18008](https://issues.apache.org/jira/browse/HBASE-18008) | Any HColumnDescriptor we give out should be immutable |  Major | . |
| [HBASE-18190](https://issues.apache.org/jira/browse/HBASE-18190) | Set version in branch-2 to 2.0.0-alpha-1 |  Major | . |
| [HBASE-18191](https://issues.apache.org/jira/browse/HBASE-18191) | Include hbase-metrics-\* in assembly |  Major | . |
| [HBASE-16392](https://issues.apache.org/jira/browse/HBASE-16392) | Backup delete fault tolerance |  Major | . |
| [HBASE-15160](https://issues.apache.org/jira/browse/HBASE-15160) | Put back HFile's HDFS op latency sampling code and add metrics for monitoring |  Critical | . |
| [HBASE-16549](https://issues.apache.org/jira/browse/HBASE-16549) | Procedure v2 - Add new AM metrics |  Major | proc-v2, Region Assignment |
| [HBASE-15995](https://issues.apache.org/jira/browse/HBASE-15995) | Separate replication WAL reading from shipping |  Major | Replication |
| [HBASE-17530](https://issues.apache.org/jira/browse/HBASE-17530) | Readd TestMergeTableRegionsProcedure mistakenly removed by HBASE-16786 |  Critical | test |
| [HBASE-16543](https://issues.apache.org/jira/browse/HBASE-16543) | Separate Create/Modify Table operations from open/reopen regions |  Major | master |
| [HBASE-16261](https://issues.apache.org/jira/browse/HBASE-16261) |  MultiHFileOutputFormat Enhancement |  Major | hbase, mapreduce |
| [HBASE-18115](https://issues.apache.org/jira/browse/HBASE-18115) | Move SaslServer creation to HBaseSaslRpcServer |  Major | IPC/RPC |
| [HBASE-18114](https://issues.apache.org/jira/browse/HBASE-18114) | Update the config of TestAsync\*AdminApi to make test stable |  Major | Client |
| [HBASE-17066](https://issues.apache.org/jira/browse/HBASE-17066) | Procedure v2 - Add handling of merge region transition to the new AM |  Major | proc-v2, Region Assignment |
| [HBASE-18091](https://issues.apache.org/jira/browse/HBASE-18091) | Add API for who currently holds a lock on namespace/ table/ region and log when state is LOCK\_EVENT\_WAIT |  Major | proc-v2 |
| [HBASE-18013](https://issues.apache.org/jira/browse/HBASE-18013) | Write response directly instead of creating a fake call when setup connection |  Major | IPC/RPC |
| [HBASE-17850](https://issues.apache.org/jira/browse/HBASE-17850) | Backup system repair utility |  Major | . |
| [HBASE-18068](https://issues.apache.org/jira/browse/HBASE-18068) | Fix flaky test TestAsyncSnapshotAdminApi |  Major | Client |
| [HBASE-18016](https://issues.apache.org/jira/browse/HBASE-18016) | Implement abort for TruncateTableProcedure |  Major | proc-v2 |
| [HBASE-18018](https://issues.apache.org/jira/browse/HBASE-18018) | Support abort for all procedures by default |  Major | proc-v2 |
| [HBASE-16851](https://issues.apache.org/jira/browse/HBASE-16851) | User-facing documentation for the In-Memory Compaction feature |  Major | . |
| [HBASE-16436](https://issues.apache.org/jira/browse/HBASE-16436) | Add the CellChunkMap variant |  Major | regionserver |
| [HBASE-18012](https://issues.apache.org/jira/browse/HBASE-18012) | Move RpcServer.Connection to a separated file |  Major | IPC/RPC |
| [HBASE-17938](https://issues.apache.org/jira/browse/HBASE-17938) | General fault - tolerance framework for backup/restore operations |  Major | . |
| [HBASE-17786](https://issues.apache.org/jira/browse/HBASE-17786) | Create LoadBalancer perf-tests (test balancer algorithm decoupled from workload) |  Major | Balancer, proc-v2 |
| [HBASE-17887](https://issues.apache.org/jira/browse/HBASE-17887) | Row-level consistency is broken for read |  Blocker | regionserver |
| [HBASE-17917](https://issues.apache.org/jira/browse/HBASE-17917) | Use pread by default for all user scan and switch to streaming read if needed |  Major | scan |
| [HBASE-17977](https://issues.apache.org/jira/browse/HBASE-17977) | Enabled Master observer to delete quotas on table deletion by default |  Major | . |
| [HBASE-17667](https://issues.apache.org/jira/browse/HBASE-17667) | Implement  async  flush/compact region methods |  Major | Admin, asyncclient, Client |
| [HBASE-17978](https://issues.apache.org/jira/browse/HBASE-17978) | Investigate hbase superuser permissions in the face of quota violation |  Major | . |
| [HBASE-17981](https://issues.apache.org/jira/browse/HBASE-17981) | Roll list\_quota\_violations into list\_quota\_snapshots |  Major | . |
| [HBASE-17867](https://issues.apache.org/jira/browse/HBASE-17867) | Implement async procedure RPC API(list/exec/abort/isFinished) |  Major | Client |
| [HBASE-17263](https://issues.apache.org/jira/browse/HBASE-17263) |   Netty based rpc server impl |  Major | Performance, rpc |
| [HBASE-17976](https://issues.apache.org/jira/browse/HBASE-17976) | Remove stability annotation from public audience-marked classes |  Major | . |
| [HBASE-17955](https://issues.apache.org/jira/browse/HBASE-17955) | Commit Reviewboard comments from Vlad |  Major | . |
| [HBASE-17920](https://issues.apache.org/jira/browse/HBASE-17920) | TestFSHDFSUtils always fails against hadoop 3.0.0-alpha2 |  Major | hadoop3 |
| [HBASE-17865](https://issues.apache.org/jira/browse/HBASE-17865) | Implement async listSnapshot/deleteSnapshot methods. |  Major | Client |
| [HBASE-16942](https://issues.apache.org/jira/browse/HBASE-16942) | Add FavoredStochasticLoadBalancer and FN Candidate generators |  Major | FavoredNodes |
| [HBASE-17873](https://issues.apache.org/jira/browse/HBASE-17873) | Change the IA.Public annotation to IA.Private for unstable API |  Blocker | API |
| [HBASE-15583](https://issues.apache.org/jira/browse/HBASE-15583) | Any HTableDescriptor we give out should be immutable |  Minor | . |
| [HBASE-15143](https://issues.apache.org/jira/browse/HBASE-15143) | Procedure v2 - Web UI displaying queues |  Minor | proc-v2, UI |
| [HBASE-17952](https://issues.apache.org/jira/browse/HBASE-17952) | The new options for PE tool do not work |  Major | Performance, test |
| [HBASE-16314](https://issues.apache.org/jira/browse/HBASE-16314) | Retry on table snapshot failure during full backup |  Major | . |
| [HBASE-17864](https://issues.apache.org/jira/browse/HBASE-17864) | Implement async snapshot/cloneSnapshot/restoreSnapshot methods |  Major | Client |
| [HBASE-17915](https://issues.apache.org/jira/browse/HBASE-17915) | Implement async replication admin methods |  Major | Client |
| [HBASE-17929](https://issues.apache.org/jira/browse/HBASE-17929) | Add more options for PE tool |  Major | Performance, test |
| [HBASE-16438](https://issues.apache.org/jira/browse/HBASE-16438) | Create a cell type so that chunk id is embedded in it |  Major | regionserver |
| [HBASE-17925](https://issues.apache.org/jira/browse/HBASE-17925) | mvn assembly:single fails against hadoop3-alpha2 |  Major | hadoop3 |
| [HBASE-17866](https://issues.apache.org/jira/browse/HBASE-17866) | Implement async setQuota/getQuota methods. |  Major | Client |
| [HBASE-17897](https://issues.apache.org/jira/browse/HBASE-17897) | StripeStoreFileManager#nonOpenRowCompare use the wrong comparison function |  Major | . |
| [HBASE-17895](https://issues.apache.org/jira/browse/HBASE-17895) | TestAsyncProcess#testAction fails if unsafe support is false |  Trivial | test |
| [HBASE-17896](https://issues.apache.org/jira/browse/HBASE-17896) | The FIXED\_OVERHEAD of Segment is incorrect |  Minor | . |
| [HBASE-16477](https://issues.apache.org/jira/browse/HBASE-16477) | Remove Writable interface and related code from WALEdit/WALKey |  Major | wal |
| [HBASE-17872](https://issues.apache.org/jira/browse/HBASE-17872) | The MSLABImpl generates the invaild cells when unsafe is not availble |  Critical | . |
| [HBASE-17881](https://issues.apache.org/jira/browse/HBASE-17881) | Remove the ByteBufferCellImpl |  Minor | test |
| [HBASE-17858](https://issues.apache.org/jira/browse/HBASE-17858) | Update refguide about the IS annotation if necessary |  Major | API, documentation |
| [HBASE-16217](https://issues.apache.org/jira/browse/HBASE-16217) | Identify calling user in ObserverContext |  Major | Coprocessors, security |
| [HBASE-17857](https://issues.apache.org/jira/browse/HBASE-17857) | Remove IS annotations from IA.Public classes |  Major | API |
| [HBASE-17859](https://issues.apache.org/jira/browse/HBASE-17859) | ByteBufferUtils#compareTo is wrong |  Critical | . |
| [HBASE-17668](https://issues.apache.org/jira/browse/HBASE-17668) | Implement async assgin/offline/move/unassign methods |  Major | Admin, asyncclient, Client |
| [HBASE-17855](https://issues.apache.org/jira/browse/HBASE-17855) | Fix typo in async client implementation |  Major | asyncclient, Client |
| [HBASE-17844](https://issues.apache.org/jira/browse/HBASE-17844) | Subset of HBASE-14614, Procedure v2: Core Assignment Manager (non-critical related changes) |  Major | Region Assignment |
| [HBASE-17520](https://issues.apache.org/jira/browse/HBASE-17520) | Implement isTableEnabled/Disabled/Available methods |  Major | Client |
| [HBASE-17765](https://issues.apache.org/jira/browse/HBASE-17765) | Reviving the merge possibility in the CompactingMemStore |  Major | . |
| [HBASE-13395](https://issues.apache.org/jira/browse/HBASE-13395) | Remove HTableInterface |  Major | API |
| [HBASE-17669](https://issues.apache.org/jira/browse/HBASE-17669) | Implement async mergeRegion/splitRegion methods. |  Major | Admin, asyncclient, Client |
| [HBASE-17809](https://issues.apache.org/jira/browse/HBASE-17809) | cleanup unused class |  Major | . |
| [HBASE-17805](https://issues.apache.org/jira/browse/HBASE-17805) | We should remove BoundedByteBufferPool because it is replaced by ByteBufferPool |  Minor | . |
| [HBASE-17447](https://issues.apache.org/jira/browse/HBASE-17447) | Automatically delete quota when table is deleted |  Major | . |
| [HBASE-17003](https://issues.apache.org/jira/browse/HBASE-17003) | Update book for filesystem use quotas |  Major | documentation |
| [HBASE-17794](https://issues.apache.org/jira/browse/HBASE-17794) | Remove lingering "violation" in favor of the accurate "snapshot" |  Trivial | . |
| [HBASE-17691](https://issues.apache.org/jira/browse/HBASE-17691) | Add ScanMetrics support for async scan |  Major | Client |
| [HBASE-15314](https://issues.apache.org/jira/browse/HBASE-15314) | Allow more than one backing file in bucketcache |  Major | BucketCache |
| [HBASE-17790](https://issues.apache.org/jira/browse/HBASE-17790) | Mark ReplicationAdmin's peerAdded and listReplicationPeers as Deprecated |  Minor | Replication |
| [HBASE-17740](https://issues.apache.org/jira/browse/HBASE-17740) | Correct the semantic of batch and partial for async client |  Critical | asyncclient, Client, scan |
| [HBASE-17338](https://issues.apache.org/jira/browse/HBASE-17338) | Treat Cell data size under global memstore heap size only when that Cell can not be copied to MSLAB |  Major | regionserver |
| [HBASE-17745](https://issues.apache.org/jira/browse/HBASE-17745) | Support short circuit connection for master services |  Major | . |
| [HBASE-17600](https://issues.apache.org/jira/browse/HBASE-17600) | Implement get/create/modify/delete/list namespace admin operations |  Major | Client |
| [HBASE-17002](https://issues.apache.org/jira/browse/HBASE-17002) | [Master] Report quotas and active violations on Master UI and JMX metrics |  Major | master, UI |
| [HBASE-17568](https://issues.apache.org/jira/browse/HBASE-17568) | Expire region reports in the HMaster |  Major | . |
| [HBASE-15484](https://issues.apache.org/jira/browse/HBASE-15484) | Correct the semantic of batch and partial |  Blocker | Client, scan |
| [HBASE-17602](https://issues.apache.org/jira/browse/HBASE-17602) | Tweak chore delay/period defaults |  Trivial | . |
| [HBASE-17516](https://issues.apache.org/jira/browse/HBASE-17516) | Table quota not taking precedence over namespace quota |  Major | . |
| [HBASE-17646](https://issues.apache.org/jira/browse/HBASE-17646) | Implement Async getRegion method |  Major | asyncclient |
| [HBASE-17662](https://issues.apache.org/jira/browse/HBASE-17662) | Disable in-memory flush when replaying from WAL |  Major | . |
| [HBASE-17428](https://issues.apache.org/jira/browse/HBASE-17428) | Expand on shell commands for detailed insight |  Major | shell |
| [HBASE-16991](https://issues.apache.org/jira/browse/HBASE-16991) | Make the initialization of AsyncConnection asynchronous |  Minor | Client |
| [HBASE-17608](https://issues.apache.org/jira/browse/HBASE-17608) | Add suspend support for RawScanResultConsumer |  Major | asyncclient, Client, scan |
| [HBASE-17561](https://issues.apache.org/jira/browse/HBASE-17561) | table status page should escape values that may contain arbitrary characters. |  Major | master, UI |
| [HBASE-17210](https://issues.apache.org/jira/browse/HBASE-17210) | Set timeout on trying rowlock according to client's RPC timeout |  Major | . |
| [HBASE-17478](https://issues.apache.org/jira/browse/HBASE-17478) | Avoid sending FSUtilization reports to master when quota support is not enabled |  Trivial | . |
| [HBASE-17025](https://issues.apache.org/jira/browse/HBASE-17025) | [Shell] Support space quota get/set via the shell |  Major | shell |
| [HBASE-17644](https://issues.apache.org/jira/browse/HBASE-17644) | Always create ByteBufferCells after copying to MSLAB |  Major | regionserver |
| [HBASE-17619](https://issues.apache.org/jira/browse/HBASE-17619) | Add async admin Impl which connect to RegionServer and implement close region methods. |  Major | asyncclient |
| [HBASE-17656](https://issues.apache.org/jira/browse/HBASE-17656) | Move new Address class from util to net package |  Minor | rsgroup |
| [HBASE-17583](https://issues.apache.org/jira/browse/HBASE-17583) | Add inclusive/exclusive support for startRow and endRow of scan for sync client |  Major | Client, scan |
| [HBASE-17259](https://issues.apache.org/jira/browse/HBASE-17259) | Missing functionality to remove space quota |  Major | . |
| [HBASE-17001](https://issues.apache.org/jira/browse/HBASE-17001) | [RegionServer] Implement enforcement of quota violation policies |  Major | regionserver |
| [HBASE-17575](https://issues.apache.org/jira/browse/HBASE-17575) | Run critical tests with each of the Inmemory Compaction Policies enabled (Towards Making BASIC the Default In-Memory Compaction Policy) |  Major | test |
| [HBASE-17484](https://issues.apache.org/jira/browse/HBASE-17484) | Add non cached version of OffheapKV for write path |  Major | regionserver |
| [HBASE-17389](https://issues.apache.org/jira/browse/HBASE-17389) | Convert all internal usages from ReplicationAdmin to Admin |  Major | . |
| [HBASE-17402](https://issues.apache.org/jira/browse/HBASE-17402) | TestAsyncTableScan sometimes hangs |  Major | Client |
| [HBASE-17350](https://issues.apache.org/jira/browse/HBASE-17350) | Fixup of regionserver group-based assignment |  Critical | regionserver, rsgroup |
| [HBASE-16999](https://issues.apache.org/jira/browse/HBASE-16999) | [Master] Inform RegionServers on size quota violations |  Major | . |
| [HBASE-17596](https://issues.apache.org/jira/browse/HBASE-17596) | Implement add/delete/modify column family methods |  Major | Client |
| [HBASE-17349](https://issues.apache.org/jira/browse/HBASE-17349) | Add doc for regionserver group-based assignment |  Critical | regionserver, rsgroup |
| [HBASE-17511](https://issues.apache.org/jira/browse/HBASE-17511) | Implement enable/disable table methods |  Major | Client |
| [HBASE-17281](https://issues.apache.org/jira/browse/HBASE-17281) | FN should use datanode port from hdfs configuration |  Minor | FavoredNodes |
| [HBASE-17198](https://issues.apache.org/jira/browse/HBASE-17198) | FN updates during region merge (follow up to Procedure v2 merge) |  Major | FavoredNodes |
| [HBASE-17101](https://issues.apache.org/jira/browse/HBASE-17101) | FavoredNodes should not apply to system tables |  Major | FavoredNodes |
| [HBASE-17346](https://issues.apache.org/jira/browse/HBASE-17346) | Add coprocessor service support |  Major | asyncclient, Client, Coprocessors |
| [HBASE-17566](https://issues.apache.org/jira/browse/HBASE-17566) | Jetty upgrade fixes |  Major | REST, UI |
| [HBASE-17562](https://issues.apache.org/jira/browse/HBASE-17562) | Remove documentation for coprocessor execution times after HBASE-14205 |  Major | Coprocessors, Performance, regionserver |
| [HBASE-16998](https://issues.apache.org/jira/browse/HBASE-16998) | [Master] Analyze table use reports and update quota violations |  Major | . |
| [HBASE-17557](https://issues.apache.org/jira/browse/HBASE-17557) | HRegionServer#reportRegionSizesForQuotas() should respond to UnsupportedOperationException |  Major | . |
| [HBASE-17526](https://issues.apache.org/jira/browse/HBASE-17526) | Procedure v2 - cleanup isSuspended from MasterProcedureScheduler#Queue |  Minor | proc-v2 |
| [HBASE-17500](https://issues.apache.org/jira/browse/HBASE-17500) | Implement getTable/creatTable/deleteTable/truncateTable methods |  Major | Client |
| [HBASE-17045](https://issues.apache.org/jira/browse/HBASE-17045) | Unify the implementation of small scan and regular scan |  Major | Client, scan |
| [HBASE-17443](https://issues.apache.org/jira/browse/HBASE-17443) | Move listReplicated/enableTableRep/disableTableRep methods from ReplicationAdmin to Admin |  Major | . |
| [HBASE-17492](https://issues.apache.org/jira/browse/HBASE-17492) | Fix the compacting memstore part in hbase shell ruby script |  Major | . |
| [HBASE-17491](https://issues.apache.org/jira/browse/HBASE-17491) | Remove all setters from HTable interface and introduce a TableBuilder to build Table instance |  Major | . |
| [HBASE-17067](https://issues.apache.org/jira/browse/HBASE-17067) | Procedure v2 - remove tryAcquire\*Lock and use wait/wake to make framework event based |  Major | proc-v2 |
| [HBASE-17367](https://issues.apache.org/jira/browse/HBASE-17367) | Make HTable#getBufferedMutator thread safe |  Major | . |
| [HBASE-17497](https://issues.apache.org/jira/browse/HBASE-17497) | Add first async MetaTableAccessor impl and Implement tableExists method |  Major | Client |
| [HBASE-17498](https://issues.apache.org/jira/browse/HBASE-17498) | Implement listTables and listTableNames methods |  Major | Client |
| [HBASE-16831](https://issues.apache.org/jira/browse/HBASE-16831) | Procedure V2 - Remove org.apache.hadoop.hbase.zookeeper.lock |  Minor | . |
| [HBASE-16867](https://issues.apache.org/jira/browse/HBASE-16867) | Procedure V2 - Check ACLs for remote HBaseLock |  Major | . |
| [HBASE-17480](https://issues.apache.org/jira/browse/HBASE-17480) | Remove split region code from Region Server |  Major | Region Assignment |
| [HBASE-17396](https://issues.apache.org/jira/browse/HBASE-17396) | Add first async admin impl and implement balance methods |  Major | Client |
| [HBASE-17483](https://issues.apache.org/jira/browse/HBASE-17483) | Add equals/hashcode for OffheapKeyValue |  Major | regionserver |
| [HBASE-17081](https://issues.apache.org/jira/browse/HBASE-17081) | Flush the entire CompactingMemStore content to disk |  Major | . |
| [HBASE-17372](https://issues.apache.org/jira/browse/HBASE-17372) | Make AsyncTable thread safe |  Major | asyncclient, Client |
| [HBASE-16744](https://issues.apache.org/jira/browse/HBASE-16744) | Procedure V2 - Lock procedures to allow clients to acquire locks on tables/namespaces/regions |  Major | . |
| [HBASE-17416](https://issues.apache.org/jira/browse/HBASE-17416) | Use isEmpty instead of size() == 0 in hbase-protocol-shaded |  Minor | . |
| [HBASE-14061](https://issues.apache.org/jira/browse/HBASE-14061) | Support CF-level Storage Policy |  Major | HFile, regionserver |
| [HBASE-17337](https://issues.apache.org/jira/browse/HBASE-17337) | list replication peers request should be routed through master |  Major | . |
| [HBASE-12148](https://issues.apache.org/jira/browse/HBASE-12148) | Remove TimeRangeTracker as point of contention when many threads writing a Store |  Major | Performance |
| [HBASE-15172](https://issues.apache.org/jira/browse/HBASE-15172) | Support setting storage policy in bulkload |  Major | . |
| [HBASE-17388](https://issues.apache.org/jira/browse/HBASE-17388) | Move ReplicationPeer and other replication related PB messages to the replication.proto |  Major | Replication |
| [HBASE-17409](https://issues.apache.org/jira/browse/HBASE-17409) | Re-fix XSS request issue in JMXJsonServlet |  Major | security, UI |
| [HBASE-17410](https://issues.apache.org/jira/browse/HBASE-17410) | Use isEmpty instead of size() == 0 in hbase-client |  Minor | Client |
| [HBASE-17373](https://issues.apache.org/jira/browse/HBASE-17373) | Reverse the order of snapshot creation in the CompactingMemStore |  Major | . |
| [HBASE-17336](https://issues.apache.org/jira/browse/HBASE-17336) | get/update replication peer config requests should be routed through master |  Major | . |
| [HBASE-17149](https://issues.apache.org/jira/browse/HBASE-17149) | Procedure V2 - Fix nonce submission to avoid unnecessary calling coprocessor multiple times |  Major | proc-v2 |
| [HBASE-17320](https://issues.apache.org/jira/browse/HBASE-17320) | Add inclusive/exclusive support for startRow and endRow of scan |  Major | Client, scan |
| [HBASE-17090](https://issues.apache.org/jira/browse/HBASE-17090) | Procedure v2 - fast wake if nothing else is running |  Major | proc-v2 |
| [HBASE-17068](https://issues.apache.org/jira/browse/HBASE-17068) | Procedure v2 - inherit region locks |  Major | master, proc-v2 |
| [HBASE-16524](https://issues.apache.org/jira/browse/HBASE-16524) | Procedure v2 - Compute WALs cleanup on wal modification and not on every sync |  Minor | proc-v2 |
| [HBASE-17345](https://issues.apache.org/jira/browse/HBASE-17345) | Implement batch |  Major | asyncclient, Client |
| [HBASE-17335](https://issues.apache.org/jira/browse/HBASE-17335) | enable/disable replication peer requests should be routed through master |  Major | . |
| [HBASE-17334](https://issues.apache.org/jira/browse/HBASE-17334) | Add locate row before/after support for AsyncRegionLocator |  Major | Client |
| [HBASE-17262](https://issues.apache.org/jira/browse/HBASE-17262) | Refactor RpcServer so as to make it extendable and/or pluggable |  Major | rpc |
| [HBASE-11392](https://issues.apache.org/jira/browse/HBASE-11392) | add/remove peer requests should be routed through master |  Critical | . |
| [HBASE-17107](https://issues.apache.org/jira/browse/HBASE-17107) | FN info should be cleaned up on region/table cleanup |  Major | FavoredNodes |
| [HBASE-17142](https://issues.apache.org/jira/browse/HBASE-17142) | Implement multi get |  Major | asyncclient, Client |
| [HBASE-17282](https://issues.apache.org/jira/browse/HBASE-17282) | Reduce the redundant requests to meta table |  Major | Client |
| [HBASE-17148](https://issues.apache.org/jira/browse/HBASE-17148) | Procedure v2 - add bulk proc submit |  Minor | master, proc-v2 |
| [HBASE-16398](https://issues.apache.org/jira/browse/HBASE-16398) | optimize HRegion computeHDFSBlocksDistribution |  Major | regionserver |
| [HBASE-15787](https://issues.apache.org/jira/browse/HBASE-15787) | Change the flush related heuristics to work with offheap size configured |  Major | regionserver |
| [HBASE-17000](https://issues.apache.org/jira/browse/HBASE-17000) | [RegionServer] Compute region filesystem space use and report to Master |  Major | regionserver |
| [HBASE-17316](https://issues.apache.org/jira/browse/HBASE-17316) | Addendum to HBASE-17294, 'External Configuration for Memory Compaction' |  Major | . |
| [HBASE-17313](https://issues.apache.org/jira/browse/HBASE-17313) | Add BufferedMutatorParams#clone method |  Major | Client |
| [HBASE-17277](https://issues.apache.org/jira/browse/HBASE-17277) | Allow alternate BufferedMutator implementation |  Major | . |
| [HBASE-17294](https://issues.apache.org/jira/browse/HBASE-17294) | External Configuration for Memory Compaction |  Major | . |
| [HBASE-16336](https://issues.apache.org/jira/browse/HBASE-16336) | Removing peers seems to be leaving spare queues |  Major | Replication |
| [HBASE-17251](https://issues.apache.org/jira/browse/HBASE-17251) | Add a timeout parameter when locating region |  Major | asyncclient, Client |
| [HBASE-16941](https://issues.apache.org/jira/browse/HBASE-16941) | FavoredNodes - Split/Merge code paths |  Major | FavoredNodes |
| [HBASE-17260](https://issues.apache.org/jira/browse/HBASE-17260) | Procedure v2 - Add setOwner() overload taking a User instance |  Trivial | proc-v2 |
| [HBASE-16996](https://issues.apache.org/jira/browse/HBASE-16996) | Implement storage/retrieval of filesystem-use quotas into quota table |  Major | . |
| [HBASE-17111](https://issues.apache.org/jira/browse/HBASE-17111) | Use Apache CLI in SnapshotInfo tool |  Major | . |
| [HBASE-16648](https://issues.apache.org/jira/browse/HBASE-16648) | [JDK8] Use computeIfAbsent instead of get and putIfAbsent |  Major | Performance |
| [HBASE-17167](https://issues.apache.org/jira/browse/HBASE-17167) | Pass mvcc to client when scan |  Major | Client, scan |
| [HBASE-17012](https://issues.apache.org/jira/browse/HBASE-17012) | Handle Offheap cells in CompressedKvEncoder |  Major | regionserver |
| [HBASE-17048](https://issues.apache.org/jira/browse/HBASE-17048) | Calcuate suitable ByteBuf size when allocating send buffer in FanOutOneBlockAsyncDFSOutput |  Major | wal |
| [HBASE-17183](https://issues.apache.org/jira/browse/HBASE-17183) | Handle ByteBufferCell while making TagRewriteCell |  Major | regionserver |
| [HBASE-17162](https://issues.apache.org/jira/browse/HBASE-17162) | Avoid unconditional call to getXXXArray() in write path |  Major | regionserver |
| [HBASE-17169](https://issues.apache.org/jira/browse/HBASE-17169) | Remove Cell variants with ShareableMemory |  Major | regionserver |
| [HBASE-16984](https://issues.apache.org/jira/browse/HBASE-16984) | Implement getScanner |  Major | Client |
| [HBASE-15786](https://issues.apache.org/jira/browse/HBASE-15786) | Create DBB backed MSLAB pool |  Major | regionserver |
| [HBASE-17141](https://issues.apache.org/jira/browse/HBASE-17141) | Introduce a more user-friendly implementation of AsyncTable |  Major | asyncclient, Client |
| [HBASE-16995](https://issues.apache.org/jira/browse/HBASE-16995) | Build client Java API and client protobuf messages |  Major | Client |
| [HBASE-17139](https://issues.apache.org/jira/browse/HBASE-17139) | Remove sweep tool related configs from hbase-default.xml |  Major | . |
| [HBASE-16956](https://issues.apache.org/jira/browse/HBASE-16956) | Refactor FavoredNodePlan to use regionNames as keys |  Minor | FavoredNodes |
| [HBASE-17085](https://issues.apache.org/jira/browse/HBASE-17085) | AsyncFSWAL may issue unnecessary AsyncDFSOutput.sync |  Major | wal |
| [HBASE-16169](https://issues.apache.org/jira/browse/HBASE-16169) | Make RegionSizeCalculator scalable |  Major | mapreduce, scaling |
| [HBASE-17073](https://issues.apache.org/jira/browse/HBASE-17073) | Increase the max number of buffers in ByteBufferPool |  Major | . |
| [HBASE-17087](https://issues.apache.org/jira/browse/HBASE-17087) | Enable Aliasing for CodedInputStream created by ByteInputByteString#newCodedInput |  Major | regionserver |
| [HBASE-17089](https://issues.apache.org/jira/browse/HBASE-17089) | Add doc on experience running with hedged reads |  Major | documentation |
| [HBASE-15788](https://issues.apache.org/jira/browse/HBASE-15788) | Use Offheap ByteBuffers from BufferPool to read RPC requests. |  Major | regionserver |
| [HBASE-16838](https://issues.apache.org/jira/browse/HBASE-16838) | Implement basic scan |  Major | asyncclient, Client |
| [HBASE-17071](https://issues.apache.org/jira/browse/HBASE-17071) | Do not initialize MemstoreChunkPool when use mslab option is turned off |  Major | . |
| [HBASE-16570](https://issues.apache.org/jira/browse/HBASE-16570) | Compute region locality in parallel at startup |  Major | . |
| [HBASE-17053](https://issues.apache.org/jira/browse/HBASE-17053) | Remove LogRollerExitedChecker |  Major | wal |
| [HBASE-17035](https://issues.apache.org/jira/browse/HBASE-17035) | Check why we roll a wal writer at 10MB when the configured roll size is 120M+ with AsyncFSWAL |  Major | wal |
| [HBASE-17021](https://issues.apache.org/jira/browse/HBASE-17021) | Use RingBuffer to reduce the contention in AsyncFSWAL |  Major | wal |
| [HBASE-16982](https://issues.apache.org/jira/browse/HBASE-16982) | Better integrate Apache CLI in AbstractHBaseTool |  Major | . |
| [HBASE-17050](https://issues.apache.org/jira/browse/HBASE-17050) | Upgrade Apache CLI version from 1.2 to 1.3.1 |  Minor | . |
| [HBASE-17030](https://issues.apache.org/jira/browse/HBASE-17030) | Procedure v2 - A couple of tweaks to the SplitTableRegionProcedure |  Trivial | proc-v2 |
| [HBASE-16937](https://issues.apache.org/jira/browse/HBASE-16937) | Replace SnapshotType protobuf conversion when we can directly use the pojo object |  Trivial | snapshots |
| [HBASE-16892](https://issues.apache.org/jira/browse/HBASE-16892) | Use TableName instead of String in SnapshotDescription |  Trivial | snapshots |
| [HBASE-16865](https://issues.apache.org/jira/browse/HBASE-16865) | Procedure v2 - Inherit lock from root proc |  Major | proc-v2 |
| [HBASE-16970](https://issues.apache.org/jira/browse/HBASE-16970) | Clarify misleading Scan.java comment about caching |  Trivial | . |
| [HBASE-16986](https://issues.apache.org/jira/browse/HBASE-16986) | Add note on how scanner caching has changed since 0.98 to refguid |  Minor | documentation |
| [HBASE-16608](https://issues.apache.org/jira/browse/HBASE-16608) | Introducing the ability to merge ImmutableSegments without copy-compaction or SQM usage |  Major | . |
| [HBASE-16945](https://issues.apache.org/jira/browse/HBASE-16945) | Implement AsyncRegionLocator |  Major | Client |
| [HBASE-16968](https://issues.apache.org/jira/browse/HBASE-16968) | Refactor FanOutOneBlockAsyncDFSOutput |  Major | io, wal |
| [HBASE-16954](https://issues.apache.org/jira/browse/HBASE-16954) | Unify HTable#checkAndDelete with AP |  Minor | . |
| [HBASE-16891](https://issues.apache.org/jira/browse/HBASE-16891) | Try copying to the Netty ByteBuf directly from the WALEdit |  Major | wal |
| [HBASE-16932](https://issues.apache.org/jira/browse/HBASE-16932) | Implement small scan |  Major | Client |
| [HBASE-16835](https://issues.apache.org/jira/browse/HBASE-16835) | Revisit the zookeeper usage at client side |  Major | Client, Zookeeper |
| [HBASE-15709](https://issues.apache.org/jira/browse/HBASE-15709) | Handle large edits for asynchronous WAL |  Critical | io, wal |
| [HBASE-15789](https://issues.apache.org/jira/browse/HBASE-15789) | PB related changes to work with offheap |  Major | regionserver |
| [HBASE-16871](https://issues.apache.org/jira/browse/HBASE-16871) | Procedure v2 - add waiting procs back to the queue after restart |  Major | proc-v2 |
| [HBASE-16872](https://issues.apache.org/jira/browse/HBASE-16872) | Implement mutateRow and checkAndMutate |  Major | Client |
| [HBASE-16874](https://issues.apache.org/jira/browse/HBASE-16874) | Fix TestMasterFailoverWithProcedures and ensure single proc-executor for kill/restart tests |  Minor | proc-v2 |
| [HBASE-16864](https://issues.apache.org/jira/browse/HBASE-16864) | Procedure v2 - Fix StateMachineProcedure support for child procs at last step |  Major | proc-v2 |
| [HBASE-16837](https://issues.apache.org/jira/browse/HBASE-16837) | Implement checkAndPut and checkAndDelete |  Major | Client |
| [HBASE-16836](https://issues.apache.org/jira/browse/HBASE-16836) | Implement increment and append |  Major | Client |
| [HBASE-16846](https://issues.apache.org/jira/browse/HBASE-16846) | Procedure v2 - executor cleanup |  Minor | proc-v2 |
| [HBASE-16834](https://issues.apache.org/jira/browse/HBASE-16834) | Add AsyncConnection support for ConnectionFactory |  Major | Client |
| [HBASE-16839](https://issues.apache.org/jira/browse/HBASE-16839) | Procedure v2 - Move all protobuf handling to ProcedureUtil |  Minor | proc-v2 |
| [HBASE-16642](https://issues.apache.org/jira/browse/HBASE-16642) | Procedure V2: Use DelayQueue instead of TimeoutBlockingQueue |  Minor | proc-v2 |
| [HBASE-16813](https://issues.apache.org/jira/browse/HBASE-16813) | Procedure v2 - Move ProcedureEvent to hbase-procedure module |  Major | proc-v2 |
| [HBASE-16735](https://issues.apache.org/jira/browse/HBASE-16735) | Procedure v2 - Fix yield while holding locks |  Major | proc-v2 |
| [HBASE-16505](https://issues.apache.org/jira/browse/HBASE-16505) | Save deadline in RpcCallContext according to request's timeout |  Major | . |
| [HBASE-15561](https://issues.apache.org/jira/browse/HBASE-15561) | See how G1GC works with MSLAB and chunk pool |  Major | . |
| [HBASE-16802](https://issues.apache.org/jira/browse/HBASE-16802) | Procedure v2 - group procedure cleaning |  Trivial | proc-v2 |
| [HBASE-16146](https://issues.apache.org/jira/browse/HBASE-16146) | Counters are expensive... |  Major | metrics |
| [HBASE-16788](https://issues.apache.org/jira/browse/HBASE-16788) | Race in compacted file deletion between HStore close() and closeAndArchiveCompactedFiles() |  Blocker | regionserver |
| [HBASE-14734](https://issues.apache.org/jira/browse/HBASE-14734) | BindException when setting up MiniKdc |  Major | flakey, test |
| [HBASE-15984](https://issues.apache.org/jira/browse/HBASE-15984) | Given failure to parse a given WAL that was closed cleanly, replay the WAL. |  Critical | Replication |
| [HBASE-15721](https://issues.apache.org/jira/browse/HBASE-15721) | Optimization in cloning cells into MSLAB |  Major | regionserver |
| [HBASE-16372](https://issues.apache.org/jira/browse/HBASE-16372) | References to previous cell in read path should be avoided |  Blocker | Scanners |
| [HBASE-16759](https://issues.apache.org/jira/browse/HBASE-16759) | Avoid ByteString.copyFrom usage wherever possible |  Major | Protobufs |
| [HBASE-16758](https://issues.apache.org/jira/browse/HBASE-16758) | Bring back HBaseZeroCopyByteStringer stuff |  Major | Protobufs |
| [HBASE-16760](https://issues.apache.org/jira/browse/HBASE-16760) | Deprecate ByteString related methods in Bytes.java |  Major | Protobufs |
| [HBASE-16741](https://issues.apache.org/jira/browse/HBASE-16741) | Amend the generate protobufs out-of-band build step to include shade, pulling in protobuf source and a hook for patching protobuf |  Major | Protobufs |
| [HBASE-16264](https://issues.apache.org/jira/browse/HBASE-16264) | Figure how to deal with endpoints and shaded pb |  Critical | Coprocessors, Protobufs |
| [HBASE-16742](https://issues.apache.org/jira/browse/HBASE-16742) | Add chapter for devs on how we do protobufs going forward |  Major | documentation |
| [HBASE-16738](https://issues.apache.org/jira/browse/HBASE-16738) | L1 cache caching shared memory HFile block when blocks promoted from L2 to L1 |  Major | regionserver, Scanners |
| [HBASE-16134](https://issues.apache.org/jira/browse/HBASE-16134) | Introduce Cell extension for server side. |  Major | regionserver |
| [HBASE-16695](https://issues.apache.org/jira/browse/HBASE-16695) | Procedure v2 - Support for parent holding locks |  Major | proc-v2 |
| [HBASE-16587](https://issues.apache.org/jira/browse/HBASE-16587) | Procedure v2 - Cleanup suspended proc execution |  Major | proc-v2 |
| [HBASE-16651](https://issues.apache.org/jira/browse/HBASE-16651) | LRUBlockCache#returnBlock should try return block to Victim Handler L2 cache. |  Major | regionserver, Scanners |
| [HBASE-16554](https://issues.apache.org/jira/browse/HBASE-16554) | Procedure V2 - Recover 'updated' part of WAL tracker if trailer is corrupted. |  Major | . |
| [HBASE-7612](https://issues.apache.org/jira/browse/HBASE-7612) | [JDK8] Replace use of high-scale-lib counters with intrinsic facilities |  Trivial | metrics |
| [HBASE-16534](https://issues.apache.org/jira/browse/HBASE-16534) | Procedure v2 - Perf Tool for Scheduler |  Major | proc-v2, tooling |
| [HBASE-16586](https://issues.apache.org/jira/browse/HBASE-16586) | Procedure v2 - Cleanup sched wait/lock semantic |  Major | proc-v2 |
| [HBASE-16626](https://issues.apache.org/jira/browse/HBASE-16626) | User customized RegionScanner from 1.X is incompatible with 2.0.0's off-heap part |  Major | Offheaping |
| [HBASE-16618](https://issues.apache.org/jira/browse/HBASE-16618) | Procedure v2 - Add base class for table and ns procedures |  Minor | master, proc-v2 |
| [HBASE-16229](https://issues.apache.org/jira/browse/HBASE-16229) | Cleaning up size and heapSize calculation |  Major | . |
| [HBASE-16592](https://issues.apache.org/jira/browse/HBASE-16592) | Unify Delete request with AP |  Major | . |
| [HBASE-16596](https://issues.apache.org/jira/browse/HBASE-16596) | Reduce redundant interfaces in AsyncProcess |  Major | . |
| [HBASE-16530](https://issues.apache.org/jira/browse/HBASE-16530) | Reduce DBE code duplication |  Major | Performance |
| [HBASE-16445](https://issues.apache.org/jira/browse/HBASE-16445) | Refactor and reimplement RpcClient |  Major | IPC/RPC, rpc |
| [HBASE-16516](https://issues.apache.org/jira/browse/HBASE-16516) | Revisit the implementation of PayloadCarryingRpcController |  Major | rpc |
| [HBASE-16101](https://issues.apache.org/jira/browse/HBASE-16101) | Procedure v2 - Perf Tool for WAL |  Major | proc-v2, tooling |
| [HBASE-16311](https://issues.apache.org/jira/browse/HBASE-16311) | Audit log for delete snapshot operation is missing in case of snapshot owner deleting the same |  Minor | snapshots |
| [HBASE-16519](https://issues.apache.org/jira/browse/HBASE-16519) | Procedure v2 - Avoid sync wait on DDLs operation |  Major | master, proc-v2 |
| [HBASE-16507](https://issues.apache.org/jira/browse/HBASE-16507) | Procedure v2 - Force DDL operation to always roll forward |  Minor | master, proc-v2 |
| [HBASE-16526](https://issues.apache.org/jira/browse/HBASE-16526) | Add more ipc tests |  Major | rpc, test |
| [HBASE-16532](https://issues.apache.org/jira/browse/HBASE-16532) | Procedure-V2: Enforce procedure ownership at submission |  Major | proc-v2 |
| [HBASE-16531](https://issues.apache.org/jira/browse/HBASE-16531) | Move cell block related code out of IPCUtil |  Major | rpc |
| [HBASE-16533](https://issues.apache.org/jira/browse/HBASE-16533) | Procedure v2 - Extract chore from the executor |  Minor | proc-v2 |
| [HBASE-16522](https://issues.apache.org/jira/browse/HBASE-16522) | Procedure v2 - Cache system user and avoid IOException |  Major | master, proc-v2 |
| [HBASE-16082](https://issues.apache.org/jira/browse/HBASE-16082) | Procedure v2 - Move out helpers from MasterProcedureScheduler |  Trivial | proc-v2 |
| [HBASE-16510](https://issues.apache.org/jira/browse/HBASE-16510) | Reset RpcController before retry |  Major | rpc |
| [HBASE-16407](https://issues.apache.org/jira/browse/HBASE-16407) | Handle MemstoreChunkPool size when HeapMemoryManager tunes memory |  Major | . |
| [HBASE-16433](https://issues.apache.org/jira/browse/HBASE-16433) | Remove AsyncRpcChannel related stuffs |  Major | rpc |
| [HBASE-16474](https://issues.apache.org/jira/browse/HBASE-16474) | Remove dfs.support.append related code and documentation |  Major | fs, regionserver, wal |
| [HBASE-16451](https://issues.apache.org/jira/browse/HBASE-16451) | Procedure v2 - Test WAL protobuf entry size limit |  Minor | proc-v2 |
| [HBASE-14921](https://issues.apache.org/jira/browse/HBASE-14921) | Inmemory Compaction Optimizations; Segment Structure |  Major | . |
| [HBASE-16485](https://issues.apache.org/jira/browse/HBASE-16485) | Procedure V2 - Add support to addChildProcedure() as last "step" in StateMachineProcedure |  Minor | proc-v2 |
| [HBASE-16440](https://issues.apache.org/jira/browse/HBASE-16440) | MemstoreChunkPool might cross its maxCount of chunks to pool |  Major | . |
| [HBASE-16318](https://issues.apache.org/jira/browse/HBASE-16318) | fail build if license isn't in whitelist |  Major | build, dependencies |
| [HBASE-16452](https://issues.apache.org/jira/browse/HBASE-16452) | Procedure v2 - Make ProcedureWALPrettyPrinter extend Tool |  Minor | proc-v2 |
| [HBASE-16405](https://issues.apache.org/jira/browse/HBASE-16405) | Change read path Bloom check to work with Cells with out any copy |  Major | Compaction |
| [HBASE-16094](https://issues.apache.org/jira/browse/HBASE-16094) | Procedure v2 - Improve cleaning up of proc wals |  Major | proc-v2 |
| [HBASE-16378](https://issues.apache.org/jira/browse/HBASE-16378) | Procedure v2 - Make ProcedureException extend HBaseException |  Trivial | proc-v2 |
| [HBASE-16404](https://issues.apache.org/jira/browse/HBASE-16404) | Make DeleteBloomFilters work with BloomContext |  Minor | Compaction |
| [HBASE-15554](https://issues.apache.org/jira/browse/HBASE-15554) | StoreFile$Writer.appendGeneralBloomFilter generates extra KV |  Major | Performance |
| [HBASE-16308](https://issues.apache.org/jira/browse/HBASE-16308) | Contain protobuf references |  Major | Protobufs |
| [HBASE-16285](https://issues.apache.org/jira/browse/HBASE-16285) | Drop RPC requests if it must be considered as timeout at client |  Major | . |
| [HBASE-16263](https://issues.apache.org/jira/browse/HBASE-16263) | Move all to do w/ protobuf -- \*.proto files and generated classes -- under hbase-protocol |  Major | Protobufs |
| [HBASE-16317](https://issues.apache.org/jira/browse/HBASE-16317) | revert all ESAPI changes |  Blocker | dependencies, security |
| [HBASE-16286](https://issues.apache.org/jira/browse/HBASE-16286) | When TagRewriteCell are not copied to MSLAB, deep clone it while adding to Memstore |  Major | regionserver |
| [HBASE-16280](https://issues.apache.org/jira/browse/HBASE-16280) | Use hash based map in SequenceIdAccounting |  Major | wal |
| [HBASE-16205](https://issues.apache.org/jira/browse/HBASE-16205) | When Cells are not copied to MSLAB, deep clone it while adding to Memstore |  Critical | regionserver |
| [HBASE-14552](https://issues.apache.org/jira/browse/HBASE-14552) | Procedure V2: Reimplement DispatchMergingRegionHandler |  Major | proc-v2 |
| [HBASE-16236](https://issues.apache.org/jira/browse/HBASE-16236) | Typo in javadoc of InstancePending |  Trivial | Zookeeper |
| [HBASE-16233](https://issues.apache.org/jira/browse/HBASE-16233) | Procedure V2: Support acquire/release shared table lock concurrently |  Major | proc-v2 |
| [HBASE-16219](https://issues.apache.org/jira/browse/HBASE-16219) | Move meta bootstrap out of HMaster |  Trivial | master, Region Assignment |
| [HBASE-15305](https://issues.apache.org/jira/browse/HBASE-15305) | Fix a couple of incorrect anchors in HBase ref guide |  Major | documentation |
| [HBASE-16195](https://issues.apache.org/jira/browse/HBASE-16195) | Should not add chunk into chunkQueue if not using chunk pool in HeapMemStoreLAB |  Major | . |
| [HBASE-16208](https://issues.apache.org/jira/browse/HBASE-16208) | Make TableBasedReplicationQueuesImpl check that queue exists before attempting to remove it |  Major | Replication |
| [HBASE-16214](https://issues.apache.org/jira/browse/HBASE-16214) | Add in UI description of Replication Table |  Minor | Replication |
| [HBASE-16092](https://issues.apache.org/jira/browse/HBASE-16092) | Procedure v2 - complete child procedure support |  Minor | proc-v2 |
| [HBASE-16194](https://issues.apache.org/jira/browse/HBASE-16194) | Should count in MSLAB chunk allocation into heap size change when adding duplicate cells |  Major | regionserver |
| [HBASE-16176](https://issues.apache.org/jira/browse/HBASE-16176) | Bug fixes/improvements on HBASE-15650 Remove TimeRangeTracker as point of contention when many threads reading a StoreFile |  Major | Performance |
| [HBASE-15935](https://issues.apache.org/jira/browse/HBASE-15935) | Have a separate Walker task running concurrently with Generator |  Minor | integration tests |
| [HBASE-16162](https://issues.apache.org/jira/browse/HBASE-16162) | Compacting Memstore : unnecessary push of active segments to pipeline |  Critical | . |
| [HBASE-15985](https://issues.apache.org/jira/browse/HBASE-15985) | clarify promises about edits from replication in ref guide |  Major | documentation, Replication |
| [HBASE-16163](https://issues.apache.org/jira/browse/HBASE-16163) | Compacting Memstore : CompactionPipeline#addFirst  can use LinkedList#addFirst |  Minor | . |
| [HBASE-16164](https://issues.apache.org/jira/browse/HBASE-16164) | Missing close of new compacted segments in few occasions which might leak MSLAB chunks from pool |  Critical | . |
| [HBASE-16155](https://issues.apache.org/jira/browse/HBASE-16155) | Compacting Memstore : Few log improvements |  Minor | . |
| [HBASE-16153](https://issues.apache.org/jira/browse/HBASE-16153) | Correct the config name 'hbase.memestore.inmemoryflush.threshold.factor' |  Trivial | . |
| [HBASE-16143](https://issues.apache.org/jira/browse/HBASE-16143) | Change MemstoreScanner constructor to accept List\<KeyValueScanner\> |  Minor | . |
| [HBASE-16121](https://issues.apache.org/jira/browse/HBASE-16121) | Require only MasterServices to the ServerManager constructor |  Trivial | master |
| [HBASE-16068](https://issues.apache.org/jira/browse/HBASE-16068) | Procedure v2 - use consts for conf properties in tests |  Trivial | proc-v2, test |
| [HBASE-16083](https://issues.apache.org/jira/browse/HBASE-16083) | Fix table based replication related configs |  Trivial | Replication |
| [HBASE-16080](https://issues.apache.org/jira/browse/HBASE-16080) | Fix flakey tests |  Critical | Replication |
| [HBASE-16036](https://issues.apache.org/jira/browse/HBASE-16036) | Fix ReplicationTableBase initialization to make it non-blocking |  Major | Replication |
| [HBASE-14878](https://issues.apache.org/jira/browse/HBASE-14878) | maven archetype: client application with shaded jars |  Major | build, Usability |
| [HBASE-14877](https://issues.apache.org/jira/browse/HBASE-14877) | maven archetype: client application |  Major | build, Usability |
| [HBASE-16056](https://issues.apache.org/jira/browse/HBASE-16056) | Procedure v2 - fix master crash for FileNotFound |  Minor | proc-v2 |
| [HBASE-16038](https://issues.apache.org/jira/browse/HBASE-16038) | Ignore JVM crashes or machine shutdown failures in report-flakies |  Major | . |
| [HBASE-15999](https://issues.apache.org/jira/browse/HBASE-15999) | NPE in MemstoreCompactor |  Critical | . |
| [HBASE-16034](https://issues.apache.org/jira/browse/HBASE-16034) | Fix ProcedureTestingUtility#LoadCounter.setMaxProcId() |  Minor | proc-v2, test |
| [HBASE-15974](https://issues.apache.org/jira/browse/HBASE-15974) | Create a ReplicationQueuesClientHBaseImpl |  Major | Replication |
| [HBASE-16023](https://issues.apache.org/jira/browse/HBASE-16023) | Fastpath for the FIFO rpcscheduler |  Major | Performance, rpc |
| [HBASE-15525](https://issues.apache.org/jira/browse/HBASE-15525) | OutOfMemory could occur when using BoundedByteBufferPool during RPC bursts |  Critical | IPC/RPC |
| [HBASE-15991](https://issues.apache.org/jira/browse/HBASE-15991) | CompactingMemstore#InMemoryFlushRunnable should implement Comparable/Comparator |  Critical | . |
| [HBASE-15272](https://issues.apache.org/jira/browse/HBASE-15272) | Add error handling for split and compact actions in table.jsp |  Major | UI |
| [HBASE-15994](https://issues.apache.org/jira/browse/HBASE-15994) | Allow selection of RpcSchedulers |  Major | . |
| [HBASE-15107](https://issues.apache.org/jira/browse/HBASE-15107) | Procedure V2 - Procedure Queue with Regions |  Major | proc-v2 |
| [HBASE-15948](https://issues.apache.org/jira/browse/HBASE-15948) | Port "HADOOP-9956  RPC listener inefficiently assigns connections to readers" |  Major | IPC/RPC |
| [HBASE-15174](https://issues.apache.org/jira/browse/HBASE-15174) | Client Public API should not have PB objects in 2.0 |  Blocker | . |
| [HBASE-15883](https://issues.apache.org/jira/browse/HBASE-15883) | Adding WAL files and tracking offsets in HBase. |  Major | Replication |
| [HBASE-15927](https://issues.apache.org/jira/browse/HBASE-15927) | Remove HMaster.assignRegion() |  Trivial | test |
| [HBASE-15917](https://issues.apache.org/jira/browse/HBASE-15917) | Flaky tests dashboard |  Major | . |
| [HBASE-15919](https://issues.apache.org/jira/browse/HBASE-15919) | Document @Rule vs @ClassRule. Also clarify timeout limits are on TestCase. |  Minor | . |
| [HBASE-15915](https://issues.apache.org/jira/browse/HBASE-15915) | Set timeouts on hanging tests |  Major | . |
| [HBASE-15896](https://issues.apache.org/jira/browse/HBASE-15896) | Add timeout tests to flaky list from report-flakies.py |  Major | test |
| [HBASE-14920](https://issues.apache.org/jira/browse/HBASE-14920) | Compacting Memstore |  Major | . |
| [HBASE-15872](https://issues.apache.org/jira/browse/HBASE-15872) | Split TestWALProcedureStore |  Trivial | proc-v2, test |
| [HBASE-15865](https://issues.apache.org/jira/browse/HBASE-15865) | Move TestTableDeleteFamilyHandler and TestTableDescriptorModification handler tests to procedure |  Trivial | test |
| [HBASE-15651](https://issues.apache.org/jira/browse/HBASE-15651) | Add report-flakies.py to use jenkins api to get failing tests |  Major | . |
| [HBASE-15825](https://issues.apache.org/jira/browse/HBASE-15825) | Fix the null pointer in DynamicLogicExpressionSuite |  Major | . |
| [HBASE-15813](https://issues.apache.org/jira/browse/HBASE-15813) | Rename DefaultWALProvider to a more specific name and clean up unnecessary reference to it |  Major | wal |
| [HBASE-15785](https://issues.apache.org/jira/browse/HBASE-15785) | Unnecessary lock in ByteBufferArray |  Major | regionserver, Scanners |
| [HBASE-15740](https://issues.apache.org/jira/browse/HBASE-15740) | Replication source.shippedKBs metric is undercounting because it is in KB |  Major | . |
| [HBASE-15776](https://issues.apache.org/jira/browse/HBASE-15776) | Replace master.am.getTableStateManager() with the direct master.getTableStateManager() |  Trivial | master |
| [HBASE-15778](https://issues.apache.org/jira/browse/HBASE-15778) | replace master.am and master.sm direct access with getter calls |  Trivial | master |
| [HBASE-15743](https://issues.apache.org/jira/browse/HBASE-15743) | Add Transparent Data Encryption support for FanOutOneBlockAsyncDFSOutput |  Major | wal |
| [HBASE-15763](https://issues.apache.org/jira/browse/HBASE-15763) | Isolate Wal related stuff from MasterFileSystem |  Trivial | master, wal |
| [HBASE-15754](https://issues.apache.org/jira/browse/HBASE-15754) | Add testcase for AES encryption |  Major | wal |
| [HBASE-15337](https://issues.apache.org/jira/browse/HBASE-15337) | Document date tiered compaction in the book |  Major | documentation |
| [HBASE-15675](https://issues.apache.org/jira/browse/HBASE-15675) | Add more details about region on table.jsp |  Major | . |
| [HBASE-15658](https://issues.apache.org/jira/browse/HBASE-15658) | RegionServerCallable / RpcRetryingCaller clear meta cache on retries |  Critical | Client |
| [HBASE-15735](https://issues.apache.org/jira/browse/HBASE-15735) | Tightening of the CP contract |  Major | regionserver, Scanners |
| [HBASE-15477](https://issues.apache.org/jira/browse/HBASE-15477) | Do not save 'next block header' when we cache hfileblocks |  Major | BlockCache, Performance |
| [HBASE-15689](https://issues.apache.org/jira/browse/HBASE-15689) | Changes to hbase-personality.sh to include/exclude flaky tests |  Major | . |
| [HBASE-15628](https://issues.apache.org/jira/browse/HBASE-15628) | Implement an AsyncOutputStream which can work with any FileSystem implementation |  Major | . |
| [HBASE-15579](https://issues.apache.org/jira/browse/HBASE-15579) | Procedure V2 - Remove synchronized around nonce in Procedure submit |  Trivial | proc-v2 |
| [HBASE-15392](https://issues.apache.org/jira/browse/HBASE-15392) | Single Cell Get reads two HFileBlocks |  Major | BucketCache |
| [HBASE-15671](https://issues.apache.org/jira/browse/HBASE-15671) | Add per-table metrics on memstore, storefile and regionsize |  Major | . |
| [HBASE-15366](https://issues.apache.org/jira/browse/HBASE-15366) | Add doc, trace-level logging, and test around hfileblock |  Major | BlockCache |
| [HBASE-15683](https://issues.apache.org/jira/browse/HBASE-15683) | Min latency in latency histograms are emitted as Long.MAX\_VALUE |  Major | . |
| [HBASE-15368](https://issues.apache.org/jira/browse/HBASE-15368) | Add pluggable window support |  Major | Compaction |
| [HBASE-15663](https://issues.apache.org/jira/browse/HBASE-15663) | Hook up JvmPauseMonitor to ThriftServer |  Major | metrics, Thrift |
| [HBASE-15662](https://issues.apache.org/jira/browse/HBASE-15662) | Hook up JvmPauseMonitor to REST server |  Major | metrics, REST |
| [HBASE-15518](https://issues.apache.org/jira/browse/HBASE-15518) | Add Per-Table metrics back |  Major | . |
| [HBASE-15640](https://issues.apache.org/jira/browse/HBASE-15640) | L1 cache doesn't give fair warning that it is showing partial stats only when it hits limit |  Major | BlockCache |
| [HBASE-15386](https://issues.apache.org/jira/browse/HBASE-15386) | PREFETCH\_BLOCKS\_ON\_OPEN in HColumnDescriptor is ignored |  Major | BucketCache |
| [HBASE-15665](https://issues.apache.org/jira/browse/HBASE-15665) | Support using different StoreFileComparators for different CompactionPolicies |  Major | Compaction |
| [HBASE-15527](https://issues.apache.org/jira/browse/HBASE-15527) | Refactor Compactor related classes |  Major | Compaction |
| [HBASE-15407](https://issues.apache.org/jira/browse/HBASE-15407) | Add SASL support for fan out OutputStream |  Major | util, wal |
| [HBASE-15380](https://issues.apache.org/jira/browse/HBASE-15380) | Purge rollback support in Store etc. |  Major | . |
| [HBASE-15537](https://issues.apache.org/jira/browse/HBASE-15537) | Make multi WAL work with WALs other than FSHLog |  Major | . |
| [HBASE-15400](https://issues.apache.org/jira/browse/HBASE-15400) | Use DateTieredCompactor for Date Tiered Compaction |  Major | Compaction |
| [HBASE-15369](https://issues.apache.org/jira/browse/HBASE-15369) | Handle NPE in region.jsp |  Minor | UI |
| [HBASE-15505](https://issues.apache.org/jira/browse/HBASE-15505) | ReplicationPeerConfig should be builder-style |  Major | . |
| [HBASE-15293](https://issues.apache.org/jira/browse/HBASE-15293) | Handle TableNotFound and IllegalArgument exceptions in table.jsp |  Major | UI |
| [HBASE-15538](https://issues.apache.org/jira/browse/HBASE-15538) | Implement secure async protobuf wal writer |  Major | wal |
| [HBASE-15389](https://issues.apache.org/jira/browse/HBASE-15389) | Write out multiple files when compaction |  Major | Compaction |
| [HBASE-15384](https://issues.apache.org/jira/browse/HBASE-15384) | Avoid using '/tmp' directory in TestBulkLoad |  Major | test |
| [HBASE-15524](https://issues.apache.org/jira/browse/HBASE-15524) | Fix NPE in client-side metrics |  Critical | Client, metrics |
| [HBASE-15495](https://issues.apache.org/jira/browse/HBASE-15495) | Connection leak in FanOutOneBlockAsyncDFSOutputHelper |  Major | . |
| [HBASE-15416](https://issues.apache.org/jira/browse/HBASE-15416) | TestHFileBackedByBucketCache is flakey since it went in |  Critical | BucketCache |
| [HBASE-15412](https://issues.apache.org/jira/browse/HBASE-15412) | Add average region size metric |  Major | . |
| [HBASE-15488](https://issues.apache.org/jira/browse/HBASE-15488) | Add ACL for setting split merge switch |  Major | . |
| [HBASE-15464](https://issues.apache.org/jira/browse/HBASE-15464) | Flush / Compaction metrics revisited |  Major | metrics |
| [HBASE-15460](https://issues.apache.org/jira/browse/HBASE-15460) | Fix infer issues in hbase-common |  Major | . |
| [HBASE-15479](https://issues.apache.org/jira/browse/HBASE-15479) | No more garbage or beware of autoboxing |  Major | Client |
| [HBASE-15334](https://issues.apache.org/jira/browse/HBASE-15334) | Add avro support for spark hbase connector |  Major | . |
| [HBASE-15390](https://issues.apache.org/jira/browse/HBASE-15390) | Unnecessary MetaCache evictions cause elevated number of requests to meta |  Blocker | Client |
| [HBASE-15180](https://issues.apache.org/jira/browse/HBASE-15180) | Reduce garbage created while reading Cells from Codec Decoder |  Major | regionserver |
| [HBASE-15435](https://issues.apache.org/jira/browse/HBASE-15435) | Add WAL (in bytes) written metric |  Major | . |
| [HBASE-15113](https://issues.apache.org/jira/browse/HBASE-15113) | Procedure v2 - Speedup eviction of sys operation results |  Minor | proc-v2 |
| [HBASE-15422](https://issues.apache.org/jira/browse/HBASE-15422) | Procedure v2 - Avoid double yield |  Major | proc-v2 |
| [HBASE-15421](https://issues.apache.org/jira/browse/HBASE-15421) | Convert TestStoreScanner to junit4 from junit3 and clean up close of scanners |  Minor | test |
| [HBASE-15354](https://issues.apache.org/jira/browse/HBASE-15354) | Use same criteria for clearing meta cache for all operations |  Major | Client |
| [HBASE-15373](https://issues.apache.org/jira/browse/HBASE-15373) | DEPRECATED\_NAME\_OF\_NO\_LIMIT\_THROUGHPUT\_CONTROLLER\_CLASS value is wrong in CompactionThroughputControllerFactory |  Minor | Compaction |
| [HBASE-15375](https://issues.apache.org/jira/browse/HBASE-15375) | Do not write to '/tmp' in TestRegionMover |  Major | test |
| [HBASE-15359](https://issues.apache.org/jira/browse/HBASE-15359) | Simplifying Segment hierarchy |  Major | . |
| [HBASE-15371](https://issues.apache.org/jira/browse/HBASE-15371) | Procedure V2 - Completed support parent-child procedure |  Major | proc-v2 |
| [HBASE-15365](https://issues.apache.org/jira/browse/HBASE-15365) | Do not write to '/tmp' in TestHBaseConfiguration |  Major | test |
| [HBASE-15346](https://issues.apache.org/jira/browse/HBASE-15346) | add 1.3 RM to docs |  Major | documentation |
| [HBASE-15351](https://issues.apache.org/jira/browse/HBASE-15351) | Fix description of hbase.bucketcache.size in hbase-default.xml |  Major | documentation |
| [HBASE-15205](https://issues.apache.org/jira/browse/HBASE-15205) | Do not find the replication scope for every WAL#append() |  Minor | regionserver |
| [HBASE-15311](https://issues.apache.org/jira/browse/HBASE-15311) | Prevent NPE in BlockCacheViewTmpl |  Major | UI |
| [HBASE-15144](https://issues.apache.org/jira/browse/HBASE-15144) | Procedure v2 - Web UI displaying Store state |  Minor | proc-v2, UI |
| [HBASE-15264](https://issues.apache.org/jira/browse/HBASE-15264) | Implement a fan out HDFS OutputStream |  Major | util, wal |
| [HBASE-15016](https://issues.apache.org/jira/browse/HBASE-15016) | StoreServices facility in Region |  Major | . |
| [HBASE-15232](https://issues.apache.org/jira/browse/HBASE-15232) | Exceptions returned over multi RPC don't automatically trigger region location reloads |  Major | Client |
| [HBASE-14949](https://issues.apache.org/jira/browse/HBASE-14949) | Resolve name conflict when splitting if there are duplicated WAL entries |  Major | wal |
| [HBASE-15270](https://issues.apache.org/jira/browse/HBASE-15270) | Use appropriate encoding for "filter" field in TaskMonitorTmpl.jamon |  Major | UI |
| [HBASE-15263](https://issues.apache.org/jira/browse/HBASE-15263) | TestIPv6NIOServerSocketChannel.testServerSocketFromLocalhostResolution can hang indefinetly |  Major | . |
| [HBASE-15244](https://issues.apache.org/jira/browse/HBASE-15244) | More doc around native lib setup and check and crc |  Major | documentation, Performance |
| [HBASE-15203](https://issues.apache.org/jira/browse/HBASE-15203) | Reduce garbage created by path.toString() during Checksum verification |  Minor | regionserver |
| [HBASE-14919](https://issues.apache.org/jira/browse/HBASE-14919) | Infrastructure refactoring for In-Memory Flush |  Major | . |
| [HBASE-15204](https://issues.apache.org/jira/browse/HBASE-15204) | Try to estimate the cell count for adding into WALEdit |  Major | regionserver |
| [HBASE-15239](https://issues.apache.org/jira/browse/HBASE-15239) | Remove unused LoadBalancer.immediateAssignment() |  Trivial | Balancer |
| [HBASE-15238](https://issues.apache.org/jira/browse/HBASE-15238) | HFileReaderV2 prefetch overreaches; runs off the end of the data |  Major | . |
| [HBASE-15046](https://issues.apache.org/jira/browse/HBASE-15046) | Perf test doing all mutation steps under row lock |  Major | Performance |
| [HBASE-15158](https://issues.apache.org/jira/browse/HBASE-15158) | Change order in which we do write pipeline operations; do all under row locks! |  Major | Performance |
| [HBASE-15163](https://issues.apache.org/jira/browse/HBASE-15163) | Add sampling code and metrics for get/scan/multi/mutate count separately |  Major | . |
| [HBASE-15157](https://issues.apache.org/jira/browse/HBASE-15157) | Add \*PerformanceTest for Append, CheckAnd\* |  Major | Performance, test |
| [HBASE-15094](https://issues.apache.org/jira/browse/HBASE-15094) | Selection of WAL files eligible for incremental backup is broken |  Major | . |
| [HBASE-15210](https://issues.apache.org/jira/browse/HBASE-15210) | Undo aggressive load balancer logging at tens of lines per millisecond |  Major | Balancer |
| [HBASE-15202](https://issues.apache.org/jira/browse/HBASE-15202) | Reduce garbage while setting response |  Minor | regionserver |
| [HBASE-15194](https://issues.apache.org/jira/browse/HBASE-15194) | TestStochasticLoadBalancer.testRegionReplicationOnMidClusterSameHosts flaky on trunk |  Major | flakey, test |
| [HBASE-14841](https://issues.apache.org/jira/browse/HBASE-14841) | Allow Dictionary to work with BytebufferedCells |  Major | regionserver, Scanners |
| [HBASE-15142](https://issues.apache.org/jira/browse/HBASE-15142) | Procedure v2 - Basic WebUI listing the procedures |  Minor | proc-v2, UI |
| [HBASE-15171](https://issues.apache.org/jira/browse/HBASE-15171) | Avoid counting duplicate kv and generating lots of small hfiles in PutSortReducer |  Major | . |
| [HBASE-15106](https://issues.apache.org/jira/browse/HBASE-15106) | Procedure V2 - Procedure Queue pass Procedure for better debuggability |  Major | proc-v2 |
| [HBASE-14221](https://issues.apache.org/jira/browse/HBASE-14221) | Reduce the number of time row comparison is done in a Scan |  Major | Scanners |
| [HBASE-15118](https://issues.apache.org/jira/browse/HBASE-15118) | Fix findbugs complaint in hbase-server |  Major | build |
| [HBASE-15115](https://issues.apache.org/jira/browse/HBASE-15115) | Fix findbugs complaints in hbase-client |  Major | build |
| [HBASE-14962](https://issues.apache.org/jira/browse/HBASE-14962) | TestSplitWalDataLoss fails on all branches |  Blocker | test |
| [HBASE-15114](https://issues.apache.org/jira/browse/HBASE-15114) | NPE when IPC server ByteBuffer reservoir is turned off |  Major | . |
| [HBASE-15105](https://issues.apache.org/jira/browse/HBASE-15105) | Procedure V2 - Procedure Queue with Namespaces |  Major | proc-v2 |
| [HBASE-14837](https://issues.apache.org/jira/browse/HBASE-14837) | Procedure V2 - Procedure Queue Improvement |  Minor | proc-v2 |
| [HBASE-15095](https://issues.apache.org/jira/browse/HBASE-15095) | isReturnResult=false  on fast path in branch-1.1 and branch-1.0 is not respected |  Major | Performance |
| [HBASE-15087](https://issues.apache.org/jira/browse/HBASE-15087) | Fix hbase-common findbugs complaints |  Major | build |
| [HBASE-15077](https://issues.apache.org/jira/browse/HBASE-15077) | Support OffheapKV write in compaction with out copying data on heap |  Major | regionserver, Scanners |
| [HBASE-12593](https://issues.apache.org/jira/browse/HBASE-12593) | Tags to work with ByteBuffer |  Major | regionserver, Scanners |
| [HBASE-14488](https://issues.apache.org/jira/browse/HBASE-14488) | Procedure V2 - shell command to abort a procedure |  Major | proc-v2 |
| [HBASE-14487](https://issues.apache.org/jira/browse/HBASE-14487) | Procedure V2 - shell command to list all procedures |  Major | proc-v2, shell |
| [HBASE-14108](https://issues.apache.org/jira/browse/HBASE-14108) | Procedure V2 - Administrative Task: provide an API to abort a procedure |  Major | proc-v2 |
| [HBASE-14432](https://issues.apache.org/jira/browse/HBASE-14432) | Procedure V2 - enforce ACL on procedure admin tasks |  Major | proc-v2 |
| [HBASE-14107](https://issues.apache.org/jira/browse/HBASE-14107) | Procedure V2 - Administrative Task: Provide an API to List all procedures |  Major | proc-v2 |
| [HBASE-15023](https://issues.apache.org/jira/browse/HBASE-15023) | Reenable TestShell and TestStochasticLoadBalancer |  Major | test |
| [HBASE-14908](https://issues.apache.org/jira/browse/HBASE-14908) | TestRowCounter flakey especially on branch-1 |  Major | flakey, test |
| [HBASE-14863](https://issues.apache.org/jira/browse/HBASE-14863) | Add missing test/resources/log4j files in hbase modules |  Trivial | test |
| [HBASE-14947](https://issues.apache.org/jira/browse/HBASE-14947) | Procedure V2 - WALProcedureStore improvements |  Blocker | proc-v2 |
| [HBASE-14895](https://issues.apache.org/jira/browse/HBASE-14895) | Seek only to the newly flushed file on scanner reset on flush |  Major | . |
| [HBASE-14655](https://issues.apache.org/jira/browse/HBASE-14655) | Narrow the scope of doAs() calls to region observer notifications for compaction |  Blocker | Coprocessors, security |
| [HBASE-14631](https://issues.apache.org/jira/browse/HBASE-14631) | Region merge request should be audited with request user through proper scope of doAs() calls to region observer notifications |  Blocker | Coprocessors, security |
| [HBASE-14605](https://issues.apache.org/jira/browse/HBASE-14605) | Split fails due to 'No valid credentials' error when SecureBulkLoadEndpoint#start tries to access hdfs |  Blocker | Coprocessors, security |
| [HBASE-14772](https://issues.apache.org/jira/browse/HBASE-14772) | Improve zombie detector; be more discerning |  Major | test |
| [HBASE-14884](https://issues.apache.org/jira/browse/HBASE-14884) | TestSplitTransactionOnCluster.testSSHCleanupDaugtherRegionsOfAbortedSplit is flakey |  Major | flakey, test |
| [HBASE-14909](https://issues.apache.org/jira/browse/HBASE-14909) | NPE testing for RIT |  Major | test |
| [HBASE-14575](https://issues.apache.org/jira/browse/HBASE-14575) | Relax region read lock for compactions |  Major | Compaction, regionserver |
| [HBASE-14819](https://issues.apache.org/jira/browse/HBASE-14819) | hbase-it tests failing with OOME; permgen |  Major | test |
| [HBASE-14832](https://issues.apache.org/jira/browse/HBASE-14832) | Ensure write paths work with ByteBufferedCells in case of compaction |  Minor | regionserver, Scanners |
| [HBASE-14798](https://issues.apache.org/jira/browse/HBASE-14798) | NPE reporting server load causes regionserver abort; causes TestAcidGuarantee to fail |  Major | test |
| [HBASE-14794](https://issues.apache.org/jira/browse/HBASE-14794) | Cleanup TestAtomicOperation, TestImportExport, and TestMetaWithReplicas |  Major | flakey, test |
| [HBASE-14785](https://issues.apache.org/jira/browse/HBASE-14785) | Hamburger menu for mobile site |  Major | website |
| [HBASE-14786](https://issues.apache.org/jira/browse/HBASE-14786) | TestProcedureAdmin hangs |  Major | hangingTests, test |
| [HBASE-14589](https://issues.apache.org/jira/browse/HBASE-14589) | Looking for the surefire-killer; builds being killed...ExecutionException: java.lang.RuntimeException: The forked VM terminated without properly saying goodbye. VM crash or System.exit called? |  Major | test |
| [HBASE-14725](https://issues.apache.org/jira/browse/HBASE-14725) | Vet categorization of tests so they for sure go into the right small/medium/large buckets |  Major | test |
| [HBASE-14638](https://issues.apache.org/jira/browse/HBASE-14638) | Move Jython info from the Wiki to the Ref Guide |  Major | documentation |
| [HBASE-14639](https://issues.apache.org/jira/browse/HBASE-14639) | Move Scala info from Wiki to Ref Guide |  Major | documentation |
| [HBASE-13478](https://issues.apache.org/jira/browse/HBASE-13478) | Document the change of default master ports being used . |  Minor | documentation |
| [HBASE-14641](https://issues.apache.org/jira/browse/HBASE-14641) | Move JDO example from Wiki to Ref Guide |  Major | documentation |
| [HBASE-14640](https://issues.apache.org/jira/browse/HBASE-14640) | Move Cascading info from Wiki to Ref Guide |  Major | documentation |
| [HBASE-14720](https://issues.apache.org/jira/browse/HBASE-14720) | Make TestHCM and TestMetaWithReplicas large tests rather than mediums |  Major | hangingTests, test |
| [HBASE-14710](https://issues.apache.org/jira/browse/HBASE-14710) | Add category-based timeouts to MR tests |  Major | hangingTests, mapreduce, test |
| [HBASE-14709](https://issues.apache.org/jira/browse/HBASE-14709) | Parent change breaks graceful\_stop.sh on a cluster |  Major | Operability |
| [HBASE-14702](https://issues.apache.org/jira/browse/HBASE-14702) | TestZKProcedureControllers.testZKCoordinatorControllerWithSingleMemberCohort is a flakey |  Major | flakey, test |
| [HBASE-14698](https://issues.apache.org/jira/browse/HBASE-14698) | Set category timeouts on TestScanner and TestNamespaceAuditor |  Major | test |
| [HBASE-14648](https://issues.apache.org/jira/browse/HBASE-14648) | Reenable TestWALProcedureStoreOnHDFS#testWalRollOnLowReplication |  Critical | test |
| [HBASE-14685](https://issues.apache.org/jira/browse/HBASE-14685) | Procedure Id is not set for  MasterRpcServices#modifyTable |  Major | master, proc-v2 |
| [HBASE-14535](https://issues.apache.org/jira/browse/HBASE-14535) | Integration test for rpc connection concurrency / deadlock testing |  Major | IPC/RPC |
| [HBASE-14657](https://issues.apache.org/jira/browse/HBASE-14657) | Remove unneeded API from EncodedSeeker |  Major | io |
| [HBASE-13538](https://issues.apache.org/jira/browse/HBASE-13538) | Procedure v2 - client add/delete/modify column family sync (incompatible with branch-1.x) |  Major | proc-v2 |
| [HBASE-14662](https://issues.apache.org/jira/browse/HBASE-14662) | Fix NPE in HFileOutputFormat2 |  Major | . |
| [HBASE-14636](https://issues.apache.org/jira/browse/HBASE-14636) | Clear HFileScannerImpl#prevBlocks in between Compaction flow |  Blocker | regionserver, Scanners |
| [HBASE-14595](https://issues.apache.org/jira/browse/HBASE-14595) | For Wiki contents that are also in the Ref Guide, replace content with a URL |  Major | documentation |
| [HBASE-14646](https://issues.apache.org/jira/browse/HBASE-14646) | Move TestCellACLs from medium to large category |  Minor | test |
| [HBASE-14647](https://issues.apache.org/jira/browse/HBASE-14647) | Disable TestWALProcedureStoreOnHDFS#testWalRollOnLowReplication |  Major | test |
| [HBASE-14637](https://issues.apache.org/jira/browse/HBASE-14637) | Loosen TestChoreService assert AND have TestDataBlockEncoders do less work (and add timeouts) |  Major | test |
| [HBASE-14627](https://issues.apache.org/jira/browse/HBASE-14627) | Move Stargate docs to Ref Guide |  Major | documentation |
| [HBASE-14607](https://issues.apache.org/jira/browse/HBASE-14607) | Convert SupportingProjects wiki page to a site page |  Major | documentation |
| [HBASE-14570](https://issues.apache.org/jira/browse/HBASE-14570) | Split TestHBaseFsck in order to help with hanging tests |  Major | test |
| [HBASE-14602](https://issues.apache.org/jira/browse/HBASE-14602) | Convert HBasePoweredBy Wiki page to a hbase.apache.org page |  Major | documentation |
| [HBASE-14596](https://issues.apache.org/jira/browse/HBASE-14596) | TestCellACLs failing... on1.2 builds |  Major | test |
| [HBASE-14600](https://issues.apache.org/jira/browse/HBASE-14600) | Make #testWalRollOnLowReplication looser still |  Major | test |
| [HBASE-13819](https://issues.apache.org/jira/browse/HBASE-13819) | Make RPC layer CellBlock buffer a DirectByteBuffer |  Major | Scanners |
| [HBASE-14558](https://issues.apache.org/jira/browse/HBASE-14558) | Document ChaosMonkey enhancements from HBASE-14261 |  Major | documentation |
| [HBASE-14590](https://issues.apache.org/jira/browse/HBASE-14590) | Shorten ByteBufferedCell#getXXXPositionInByteBuffer method name |  Major | regionserver, Scanners |
| [HBASE-14585](https://issues.apache.org/jira/browse/HBASE-14585) | Clean up TestSnapshotCloneIndependence |  Major | flakey, test |
| [HBASE-14567](https://issues.apache.org/jira/browse/HBASE-14567) | Tuneup hanging test TestMobCompactor and TestMobSweeper |  Major | test |
| [HBASE-14572](https://issues.apache.org/jira/browse/HBASE-14572) | TestImportExport#testImport94Table can't find its src data file |  Major | test |
| [HBASE-14571](https://issues.apache.org/jira/browse/HBASE-14571) | Purge TestProcessBasedCluster; it does nothing and then fails |  Major | test |
| [HBASE-14519](https://issues.apache.org/jira/browse/HBASE-14519) | Purge TestFavoredNodeAssignmentHelper, a test for an abandoned feature that can hang |  Major | test |
| [HBASE-14563](https://issues.apache.org/jira/browse/HBASE-14563) | Disable zombie TestHFileOutputFormat2 |  Major | test |
| [HBASE-14561](https://issues.apache.org/jira/browse/HBASE-14561) | Disable zombie TestReplicationShell |  Major | test |
| [HBASE-14559](https://issues.apache.org/jira/browse/HBASE-14559) | branch-1 test tweeks; disable assert explicit region lands post-restart and up a few handlers |  Major | test |
| [HBASE-14480](https://issues.apache.org/jira/browse/HBASE-14480) | Small optimization in SingleByteBuff |  Major | regionserver, Scanners |
| [HBASE-14539](https://issues.apache.org/jira/browse/HBASE-14539) | Slight improvement of StoreScanner.optimize |  Minor | Performance, Scanners |
| [HBASE-14543](https://issues.apache.org/jira/browse/HBASE-14543) | Have findHangingTests.py dump more info |  Major | tooling |
| [HBASE-14513](https://issues.apache.org/jira/browse/HBASE-14513) | TestBucketCache runs obnoxious 1k threads in a unit test |  Major | test |
| [HBASE-14495](https://issues.apache.org/jira/browse/HBASE-14495) | TestHRegion#testFlushCacheWhileScanning goes zombie |  Major | test |
| [HBASE-14507](https://issues.apache.org/jira/browse/HBASE-14507) | Disable TestDistributedLogSplitting#testWorkerAbort Its flakey with tenuous chance of success |  Major | test |
| [HBASE-14398](https://issues.apache.org/jira/browse/HBASE-14398) | Create the fake keys required in the scan path to avoid copy to byte[] |  Major | . |
| [HBASE-14430](https://issues.apache.org/jira/browse/HBASE-14430) | TestHttpServerLifecycle#testStartedServerIsAlive times out |  Major | test |
| [HBASE-14212](https://issues.apache.org/jira/browse/HBASE-14212) | Add IT test for procedure-v2-based namespace DDL |  Major | proc-v2 |
| [HBASE-14051](https://issues.apache.org/jira/browse/HBASE-14051) | Undo workarounds in IntegrationTestDDLMasterFailover for client double submit |  Major | master |
| [HBASE-12748](https://issues.apache.org/jira/browse/HBASE-12748) | RegionCoprocessorHost.execOperation creates too many iterator objects |  Major | . |
| [HBASE-14378](https://issues.apache.org/jira/browse/HBASE-14378) | Get TestAccessController\* passing again on branch-1 |  Major | . |
| [HBASE-14484](https://issues.apache.org/jira/browse/HBASE-14484) | Follow-on from HBASE-14421, just disable TestFastFail\* until someone digs in and fixes it |  Major | test |
| [HBASE-14464](https://issues.apache.org/jira/browse/HBASE-14464) | Removed unused fs code |  Minor | regionserver |
| [HBASE-14472](https://issues.apache.org/jira/browse/HBASE-14472) | TestHCM and TestRegionServerNoMaster fixes |  Minor | test |
| [HBASE-14147](https://issues.apache.org/jira/browse/HBASE-14147) | REST Support for Namespaces |  Minor | REST |
| [HBASE-14447](https://issues.apache.org/jira/browse/HBASE-14447) | Spark tests failing: bind exception when putting up info server |  Minor | test |
| [HBASE-14278](https://issues.apache.org/jira/browse/HBASE-14278) | Fix NPE that is showing up since HBASE-14274 went in |  Major | test |
| [HBASE-14433](https://issues.apache.org/jira/browse/HBASE-14433) | Set down the client executor core thread count from 256 in tests |  Major | test |
| [HBASE-14401](https://issues.apache.org/jira/browse/HBASE-14401) | Stamp failed appends with sequenceid too.... Cleans up latches |  Major | test, wal |
| [HBASE-14435](https://issues.apache.org/jira/browse/HBASE-14435) | thrift tests don't have test-specific hbase-site.xml so 'BindException: Address already in use' because info port is not turned off |  Major | test |
| [HBASE-14428](https://issues.apache.org/jira/browse/HBASE-14428) | Upgrade our surefire-plugin from 2.18 to 2.18.1 |  Major | test |
| [HBASE-14421](https://issues.apache.org/jira/browse/HBASE-14421) | TestFastFail\* are flakey |  Major | test |
| [HBASE-14423](https://issues.apache.org/jira/browse/HBASE-14423) | TestStochasticBalancerJmxMetrics.testJmxMetrics\_PerTableMode:183 NullPointer |  Major | test |
| [HBASE-14395](https://issues.apache.org/jira/browse/HBASE-14395) | Short circuit last byte check in CellUtil#matchingXXX methods for ByteBufferedCells |  Major | regionserver, Scanners |
| [HBASE-14368](https://issues.apache.org/jira/browse/HBASE-14368) | New TestWALLockup broken by addendum added to parent issue |  Major | test |
| [HBASE-14322](https://issues.apache.org/jira/browse/HBASE-14322) | Master still not using more than it's priority threads |  Major | IPC/RPC, master |
| [HBASE-13212](https://issues.apache.org/jira/browse/HBASE-13212) | Procedure V2 - master Create/Modify/Delete namespace |  Major | master |
| [HBASE-14239](https://issues.apache.org/jira/browse/HBASE-14239) | Branch-1.2 AM can get stuck when meta moves |  Major | Region Assignment |
| [HBASE-14274](https://issues.apache.org/jira/browse/HBASE-14274) | Deadlock in region metrics on shutdown: MetricsRegionSourceImpl vs MetricsRegionAggregateSourceImpl |  Major | test |
| [HBASE-14186](https://issues.apache.org/jira/browse/HBASE-14186) | Read mvcc vlong optimization |  Major | Performance, Scanners |
| [HBASE-14144](https://issues.apache.org/jira/browse/HBASE-14144) | Bloomfilter path to work with Byte buffered cells |  Major | regionserver, Scanners |
| [HBASE-14087](https://issues.apache.org/jira/browse/HBASE-14087) | ensure correct ASF policy compliant headers on source/docs |  Blocker | build |
| [HBASE-14202](https://issues.apache.org/jira/browse/HBASE-14202) | Reduce garbage we create |  Major | regionserver, Scanners |
| [HBASE-14105](https://issues.apache.org/jira/browse/HBASE-14105) | Add shell tests for Snapshot |  Major | test |
| [HBASE-14188](https://issues.apache.org/jira/browse/HBASE-14188) | Read path optimizations after HBASE-11425 profiling |  Major | Scanners |
| [HBASE-14176](https://issues.apache.org/jira/browse/HBASE-14176) | Add missing headers to META-INF files |  Trivial | build |
| [HBASE-14086](https://issues.apache.org/jira/browse/HBASE-14086) | remove unused bundled dependencies |  Blocker | documentation |
| [HBASE-14063](https://issues.apache.org/jira/browse/HBASE-14063) | Use BufferBackedCell in read path after HBASE-12213 and HBASE-12295 |  Major | regionserver, Scanners |
| [HBASE-12295](https://issues.apache.org/jira/browse/HBASE-12295) | Prevent block eviction under us if reads are in progress from the BBs |  Major | regionserver, Scanners |
| [HBASE-14116](https://issues.apache.org/jira/browse/HBASE-14116) | Change ByteBuff.getXXXStrictlyForward to relative position based reads |  Major | . |
| [HBASE-12374](https://issues.apache.org/jira/browse/HBASE-12374) | Change DBEs to work with new BB based cell |  Major | regionserver, Scanners |
| [HBASE-14120](https://issues.apache.org/jira/browse/HBASE-14120) | ByteBufferUtils#compareTo small optimization |  Major | regionserver, Scanners |
| [HBASE-14104](https://issues.apache.org/jira/browse/HBASE-14104) | Add vectorportal.com to NOTICES.txt as src of our logo |  Major | documentation |
| [HBASE-14102](https://issues.apache.org/jira/browse/HBASE-14102) | Add thank you to our thanks page for vectorportal.com |  Major | . |
| [HBASE-12015](https://issues.apache.org/jira/browse/HBASE-12015) | Not cleaning Mob data when Mob CF is removed from table |  Major | . |
| [HBASE-13993](https://issues.apache.org/jira/browse/HBASE-13993) | WALProcedureStore fencing is not effective if new WAL rolls |  Major | master |
| [HBASE-13387](https://issues.apache.org/jira/browse/HBASE-13387) | Add ByteBufferedCell an extension to Cell |  Major | regionserver, Scanners |
| [HBASE-13832](https://issues.apache.org/jira/browse/HBASE-13832) | Procedure V2: master fail to start due to WALProcedureStore sync failures when HDFS data nodes count is low |  Blocker | master, proc-v2 |
| [HBASE-13415](https://issues.apache.org/jira/browse/HBASE-13415) | Procedure V2 - Use nonces for double submits from client |  Blocker | master |
| [HBASE-14017](https://issues.apache.org/jira/browse/HBASE-14017) | Procedure v2 - MasterProcedureQueue fix concurrency issue on table queue deletion |  Blocker | proc-v2 |
| [HBASE-14020](https://issues.apache.org/jira/browse/HBASE-14020) | Unsafe based optimized write in ByteBufferOutputStream |  Major | Scanners |
| [HBASE-13998](https://issues.apache.org/jira/browse/HBASE-13998) | Remove CellComparator#compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength) |  Major | . |
| [HBASE-13977](https://issues.apache.org/jira/browse/HBASE-13977) | Convert getKey and related APIs to Cell |  Major | . |
| [HBASE-14013](https://issues.apache.org/jira/browse/HBASE-14013) | Retry when RegionServerNotYetRunningException rather than go ahead with assign so for sure we don't skip WAL replay |  Major | Region Assignment |
| [HBASE-13975](https://issues.apache.org/jira/browse/HBASE-13975) | add 1.2 RM to docs |  Major | documentation |
| [HBASE-13911](https://issues.apache.org/jira/browse/HBASE-13911) | add 1.2 to prereq tables in ref guide |  Blocker | documentation |
| [HBASE-13990](https://issues.apache.org/jira/browse/HBASE-13990) | clean up remaining errors for maven site goal |  Major | documentation |
| [HBASE-14003](https://issues.apache.org/jira/browse/HBASE-14003) | work around jdk8 spec bug in WALPerfEval |  Critical | test |
| [HBASE-13939](https://issues.apache.org/jira/browse/HBASE-13939) | Make HFileReaderImpl.getFirstKeyInBlock() to return a Cell |  Minor | . |
| [HBASE-13614](https://issues.apache.org/jira/browse/HBASE-13614) | Avoid temp KeyOnlyKeyValue temp objects creations in read hot path |  Critical | . |
| [HBASE-12345](https://issues.apache.org/jira/browse/HBASE-12345) | Unsafe based ByteBuffer Comparator |  Major | regionserver, Scanners |
| [HBASE-13922](https://issues.apache.org/jira/browse/HBASE-13922) | Do not reset mvcc in compactions for mob-enabled column |  Major | mob |
| [HBASE-13968](https://issues.apache.org/jira/browse/HBASE-13968) | Remove deprecated methods from BufferedMutator class |  Major | API |
| [HBASE-13973](https://issues.apache.org/jira/browse/HBASE-13973) | Update documentation for 10070 Phase 2 changes |  Major | . |
| [HBASE-13967](https://issues.apache.org/jira/browse/HBASE-13967) | add jdk profiles for jdk.tools dependency |  Major | . |
| [HBASE-13950](https://issues.apache.org/jira/browse/HBASE-13950) | Add a NoopProcedureStore for testing |  Trivial | proc-v2 |
| [HBASE-13920](https://issues.apache.org/jira/browse/HBASE-13920) | Exclude Java files generated from protobuf from javadoc |  Minor | . |
| [HBASE-13932](https://issues.apache.org/jira/browse/HBASE-13932) | Add mob integrity check in HFilePrettyPrinter |  Major | mob |
| [HBASE-13898](https://issues.apache.org/jira/browse/HBASE-13898) | correct additional javadoc failures under java 8 |  Minor | documentation |
| [HBASE-13728](https://issues.apache.org/jira/browse/HBASE-13728) | Remove use of Hadoop's GenericOptionsParser |  Blocker | . |
| [HBASE-13448](https://issues.apache.org/jira/browse/HBASE-13448) | New Cell implementation with cached component offsets/lengths |  Blocker | Scanners |
| [HBASE-13926](https://issues.apache.org/jira/browse/HBASE-13926) | Close the scanner only after Call#setResponse |  Major | regionserver, Scanners |
| [HBASE-13916](https://issues.apache.org/jira/browse/HBASE-13916) | Create MultiByteBuffer an aggregation of ByteBuffers |  Major | regionserver, Scanners |
| [HBASE-13470](https://issues.apache.org/jira/browse/HBASE-13470) | High level Integration test for master DDL operations |  Major | master |
| [HBASE-13886](https://issues.apache.org/jira/browse/HBASE-13886) | Return empty value when the mob file is corrupt instead of throwing exceptions |  Major | mob |
| [HBASE-13910](https://issues.apache.org/jira/browse/HBASE-13910) | add branch-1.2 to precommit branches |  Major | build |
| [HBASE-13899](https://issues.apache.org/jira/browse/HBASE-13899) | Jacoco instrumentation fails under jdk8 |  Major | build, test |
| [HBASE-13569](https://issues.apache.org/jira/browse/HBASE-13569) | correct errors reported with mvn site |  Minor | documentation |
| [HBASE-13855](https://issues.apache.org/jira/browse/HBASE-13855) | Race in multi threaded PartitionedMobCompactor causes NPE |  Critical | mob |
| [HBASE-13871](https://issues.apache.org/jira/browse/HBASE-13871) | Change RegionScannerImpl to deal with Cell instead of byte[], int, int |  Major | regionserver, Scanners |
| [HBASE-13836](https://issues.apache.org/jira/browse/HBASE-13836) | Do not reset the mvcc for bulk loaded mob reference cells in reading |  Major | mob |
| [HBASE-13451](https://issues.apache.org/jira/browse/HBASE-13451) | Make the HFileBlockIndex blockKeys to Cells so that it could be easy to use in the CellComparators |  Major | . |
| [HBASE-13856](https://issues.apache.org/jira/browse/HBASE-13856) | Wrong mob metrics names in TestRegionServerMetrics |  Major | mob |
| [HBASE-13806](https://issues.apache.org/jira/browse/HBASE-13806) | Check the mob files when there are mob-enabled columns in HFileCorruptionChecker |  Major | mob |
| [HBASE-13827](https://issues.apache.org/jira/browse/HBASE-13827) | Delayed scanner close in KeyValueHeap and StoreScanner |  Major | regionserver, Scanners |
| [HBASE-13817](https://issues.apache.org/jira/browse/HBASE-13817) | ByteBufferOuputStream - add writeInt support |  Major | Scanners |
| [HBASE-13804](https://issues.apache.org/jira/browse/HBASE-13804) | Revert the changes in pom.xml |  Major | mob |
| [HBASE-13803](https://issues.apache.org/jira/browse/HBASE-13803) | Disable the MobCompactionChore when the interval is not larger than 0 |  Major | mob |
| [HBASE-13805](https://issues.apache.org/jira/browse/HBASE-13805) | Use LimitInputStream in hbase-common instead of ProtobufUtil.LimitedInputStream |  Major | mob |
| [HBASE-13759](https://issues.apache.org/jira/browse/HBASE-13759) | Improve procedure yielding |  Trivial | proc-v2 |
| [HBASE-13790](https://issues.apache.org/jira/browse/HBASE-13790) | Remove the DeleteTableHandler |  Major | mob |
| [HBASE-13616](https://issues.apache.org/jira/browse/HBASE-13616) | Move ServerShutdownHandler to Pv2 |  Major | proc-v2 |
| [HBASE-13476](https://issues.apache.org/jira/browse/HBASE-13476) | Procedure V2 - Add Replay Order logic for child procedures |  Major | proc-v2 |
| [HBASE-13763](https://issues.apache.org/jira/browse/HBASE-13763) | Handle the rename, annotation and typo stuff in MOB |  Major | mob |
| [HBASE-13754](https://issues.apache.org/jira/browse/HBASE-13754) | Allow non KeyValue Cell types also to oswrite |  Major | Scanners |
| [HBASE-13393](https://issues.apache.org/jira/browse/HBASE-13393) | Optimize memstore flushing to avoid writing tag information to hfiles when no tags are present. |  Major | HFile, Performance |
| [HBASE-13658](https://issues.apache.org/jira/browse/HBASE-13658) | Improve the test run time for TestAccessController class |  Major | test |
| [HBASE-13762](https://issues.apache.org/jira/browse/HBASE-13762) | Use the same HFileContext with store files in mob files |  Major | mob |
| [HBASE-13720](https://issues.apache.org/jira/browse/HBASE-13720) | Mob files are not encrypting in mob compaction and Sweeper |  Major | regionserver, Scanners |
| [HBASE-13739](https://issues.apache.org/jira/browse/HBASE-13739) | Remove KeyValueUtil.ensureKeyValue(cell) from MOB code. |  Major | mob |
| [HBASE-13641](https://issues.apache.org/jira/browse/HBASE-13641) | Deperecate Filter#filterRowKey(byte[] buffer, int offset, int length) in favor of filterRowKey(Cell firstRowCell) |  Major | Filters, regionserver, Scanners |
| [HBASE-13736](https://issues.apache.org/jira/browse/HBASE-13736) | Add delay for the first execution of ExpiredMobFileCleanerChore and MobFileCompactorChore |  Major | mob |
| [HBASE-13642](https://issues.apache.org/jira/browse/HBASE-13642) | Deprecate RegionObserver#postScannerFilterRow CP hook with byte[],int,int args in favor of taking Cell arg |  Major | Coprocessors, regionserver, Scanners |
| [HBASE-13679](https://issues.apache.org/jira/browse/HBASE-13679) | Change ColumnTracker and SQM to deal with Cell instead of byte[], int, int |  Major | regionserver, Scanners |
| [HBASE-13531](https://issues.apache.org/jira/browse/HBASE-13531) | Flakey failures of TestAcidGuarantees#testMobScanAtomicity |  Major | regionserver, Scanners |
| [HBASE-13510](https://issues.apache.org/jira/browse/HBASE-13510) | Purge ByteBloomFilter |  Major | . |
| [HBASE-13398](https://issues.apache.org/jira/browse/HBASE-13398) | Document HBase Quota |  Major | documentation |
| [HBASE-13201](https://issues.apache.org/jira/browse/HBASE-13201) | Remove HTablePool from thrift-server |  Major | . |
| [HBASE-13571](https://issues.apache.org/jira/browse/HBASE-13571) | Procedure v2 - client modify table sync (incompatible with branch-1.x) |  Minor | proc-v2 |
| [HBASE-13593](https://issues.apache.org/jira/browse/HBASE-13593) | Quota support for namespace should take snapshot restore and clone into account |  Major | snapshots |
| [HBASE-13620](https://issues.apache.org/jira/browse/HBASE-13620) | Bring back the removed VisibilityClient methods and mark them as deprecated. |  Minor | . |
| [HBASE-13464](https://issues.apache.org/jira/browse/HBASE-13464) | Remove deprecations for 2.0.0 - Part 1 |  Major | . |
| [HBASE-13501](https://issues.apache.org/jira/browse/HBASE-13501) | Deprecate/Remove getComparator() in HRegionInfo. |  Major | . |
| [HBASE-13497](https://issues.apache.org/jira/browse/HBASE-13497) | Remove MVCC stamps from HFile when that is safe |  Major | Scanners |
| [HBASE-13184](https://issues.apache.org/jira/browse/HBASE-13184) | Document turning off memstore for region replicas |  Critical | documentation, Replication |
| [HBASE-13537](https://issues.apache.org/jira/browse/HBASE-13537) | Procedure V2 - Change the admin interface for async operations to return Future (incompatible with branch-1.x) |  Major | proc-v2 |
| [HBASE-13572](https://issues.apache.org/jira/browse/HBASE-13572) | Procedure v2 - client truncate table sync (incompatible with branch-1.x) |  Minor | proc-v2 |
| [HBASE-13551](https://issues.apache.org/jira/browse/HBASE-13551) | Procedure V2 - Procedure classes should not be InterfaceAudience.Public |  Blocker | master |
| [HBASE-13563](https://issues.apache.org/jira/browse/HBASE-13563) | Add missing table owner to AC tests. |  Minor | . |
| [HBASE-13466](https://issues.apache.org/jira/browse/HBASE-13466) | Document deprecations in 1.x - Part 1 |  Major | . |
| [HBASE-13536](https://issues.apache.org/jira/browse/HBASE-13536) | Cleanup the handlers that are no longer being used. |  Major | proc-v2 |
| [HBASE-13450](https://issues.apache.org/jira/browse/HBASE-13450) | Purge RawBytescomparator from the writers and readers for HBASE-10800 |  Major | . |
| [HBASE-13529](https://issues.apache.org/jira/browse/HBASE-13529) | Procedure v2 - WAL Improvements |  Blocker | proc-v2 |
| [HBASE-13496](https://issues.apache.org/jira/browse/HBASE-13496) | Make Bytes$LexicographicalComparerHolder$UnsafeComparer::compareTo inlineable |  Major | Scanners |
| [HBASE-13502](https://issues.apache.org/jira/browse/HBASE-13502) | Deprecate/remove getRowComparator() in TableName |  Major | . |
| [HBASE-13515](https://issues.apache.org/jira/browse/HBASE-13515) | Handle FileNotFoundException in region replica replay for flush/compaction events |  Major | . |
| [HBASE-13514](https://issues.apache.org/jira/browse/HBASE-13514) | Fix test failures in TestScannerHeartbeatMessages caused by incorrect setting of hbase.rpc.timeout |  Minor | . |
| [HBASE-13498](https://issues.apache.org/jira/browse/HBASE-13498) | Add more docs and a basic check for storage policy handling |  Minor | wal |
| [HBASE-13481](https://issues.apache.org/jira/browse/HBASE-13481) | Master should respect master (old) DNS/bind related configurations |  Major | . |
| [HBASE-13307](https://issues.apache.org/jira/browse/HBASE-13307) | Making methods under ScannerV2#next inlineable, faster |  Major | Scanners |
| [HBASE-13455](https://issues.apache.org/jira/browse/HBASE-13455) | Procedure V2 - master truncate table |  Minor | master |
| [HBASE-13290](https://issues.apache.org/jira/browse/HBASE-13290) | Procedure v2 - client enable/disable table sync |  Major | Client |
| [HBASE-13211](https://issues.apache.org/jira/browse/HBASE-13211) | Procedure V2 - master Enable/Disable table |  Major | master |
| [HBASE-13210](https://issues.apache.org/jira/browse/HBASE-13210) | Procedure V2 - master Modify table |  Major | master |
| [HBASE-13209](https://issues.apache.org/jira/browse/HBASE-13209) | Procedure V2 - master Add/Modify/Delete Column Family |  Major | master |
| [HBASE-13204](https://issues.apache.org/jira/browse/HBASE-13204) | Procedure v2 - client create/delete table sync |  Minor | master |
| [HBASE-13203](https://issues.apache.org/jira/browse/HBASE-13203) | Procedure v2 - master create/delete table |  Minor | master |
| [HBASE-13202](https://issues.apache.org/jira/browse/HBASE-13202) | Procedure v2 - core framework |  Major | master, proc-v2 |
| [HBASE-13447](https://issues.apache.org/jira/browse/HBASE-13447) | Bypass logic in TimeRange.compare |  Minor | Scanners |
| [HBASE-13313](https://issues.apache.org/jira/browse/HBASE-13313) | Skip the disabled table in mob compaction chore and MasterRpcServices |  Major | regionserver, Scanners |
| [HBASE-13429](https://issues.apache.org/jira/browse/HBASE-13429) | Remove deprecated seek/reseek methods from HFileScanner |  Major | regionserver, Scanners |
| [HBASE-13421](https://issues.apache.org/jira/browse/HBASE-13421) | Reduce the number of object creations introduced by HBASE-11544 in scan RPC hot code paths |  Major | . |
| [HBASE-13302](https://issues.apache.org/jira/browse/HBASE-13302) | fix new  javadoc warns introduced by mob. |  Major | regionserver, Scanners |
| [HBASE-13277](https://issues.apache.org/jira/browse/HBASE-13277) | add mob\_threshold option to load test tool |  Major | regionserver, Scanners |
| [HBASE-13252](https://issues.apache.org/jira/browse/HBASE-13252) | Get rid of managed connections and connection caching |  Major | API |
| [HBASE-13335](https://issues.apache.org/jira/browse/HBASE-13335) | Update ClientSmallScanner and ClientSmallReversedScanner |  Major | Client |
| [HBASE-13213](https://issues.apache.org/jira/browse/HBASE-13213) | Split out locality metrics among primary and secondary region |  Major | . |
| [HBASE-13303](https://issues.apache.org/jira/browse/HBASE-13303) | Fix size calculation of results on the region server |  Major | Client |
| [HBASE-13332](https://issues.apache.org/jira/browse/HBASE-13332) | Fix the usage of doAs/runAs in Visibility Controller tests. |  Major | . |
| [HBASE-13327](https://issues.apache.org/jira/browse/HBASE-13327) | Use Admin in ConnectionCache |  Major | . |
| [HBASE-13258](https://issues.apache.org/jira/browse/HBASE-13258) | Promote TestHRegion to LargeTests |  Major | test |
| [HBASE-13006](https://issues.apache.org/jira/browse/HBASE-13006) | Document visibility label support for groups |  Minor | . |
| [HBASE-13230](https://issues.apache.org/jira/browse/HBASE-13230) | [mob] reads hang when trying to read rows with large mobs (\>10MB) |  Major | mob |
| [HBASE-13244](https://issues.apache.org/jira/browse/HBASE-13244) | Test delegation token generation with kerberos enabled |  Major | security, test |
| [HBASE-13226](https://issues.apache.org/jira/browse/HBASE-13226) | Document enable\_table\_replication and disable\_table\_replication shell commands |  Minor | documentation |
| [HBASE-13169](https://issues.apache.org/jira/browse/HBASE-13169) | ModifyTable increasing the region replica count should also auto-setup RRRE |  Major | . |
| [HBASE-13121](https://issues.apache.org/jira/browse/HBASE-13121) | Async wal replication for region replicas and dist log replay does not work together |  Major | . |
| [HBASE-13095](https://issues.apache.org/jira/browse/HBASE-13095) | Document how to retrieve replication stats from HBase Shell |  Major | documentation, Replication |
| [HBASE-13164](https://issues.apache.org/jira/browse/HBASE-13164) | Update TestUsersOperationsWithSecureHadoop to use MiniKdc |  Major | security, test |
| [HBASE-12562](https://issues.apache.org/jira/browse/HBASE-12562) | Handling memory pressure for secondary region replicas |  Major | . |
| [HBASE-13151](https://issues.apache.org/jira/browse/HBASE-13151) | IllegalArgumentException in compaction when table has a namespace |  Major | regionserver, Scanners |
| [HBASE-13107](https://issues.apache.org/jira/browse/HBASE-13107) | Refactor MOB Snapshot logic to reduce code duplication. |  Major | mob, snapshots |
| [HBASE-13157](https://issues.apache.org/jira/browse/HBASE-13157) | Add mob compaction actions and monkeys to Chaos Monkey |  Major | integration tests, mob |
| [HBASE-12332](https://issues.apache.org/jira/browse/HBASE-12332) | [mob] improve how we resolve mobfiles in reads |  Major | mob |
| [HBASE-13154](https://issues.apache.org/jira/browse/HBASE-13154) | Add support for mob in TestAcidGuarantees and IntegrationTestAcidGuarantees |  Major | integration tests, mob |
| [HBASE-13012](https://issues.apache.org/jira/browse/HBASE-13012) | Add shell commands to trigger the mob file compactor |  Major | regionserver, Scanners |
| [HBASE-11580](https://issues.apache.org/jira/browse/HBASE-11580) | Failover handling for secondary region replicas |  Major | . |
| [HBASE-13152](https://issues.apache.org/jira/browse/HBASE-13152) | NPE in ExpiredMobFileCleanerChore |  Major | regionserver, Scanners |
| [HBASE-11571](https://issues.apache.org/jira/browse/HBASE-11571) | Bulk load handling from secondary region replicas |  Major | . |
| [HBASE-12670](https://issues.apache.org/jira/browse/HBASE-12670) | Add unit tests that exercise the added hfilelink link mob paths |  Major | regionserver, Scanners |
| [HBASE-13130](https://issues.apache.org/jira/browse/HBASE-13130) | Add timeouts on TestMasterObserver, a frequent zombie show |  Major | test |
| [HBASE-13117](https://issues.apache.org/jira/browse/HBASE-13117) | improve mob sweeper javadoc |  Major | mob |
| [HBASE-13067](https://issues.apache.org/jira/browse/HBASE-13067) | Fix caching of stubs to allow IP address changes of restarted remote servers |  Major | . |
| [HBASE-12561](https://issues.apache.org/jira/browse/HBASE-12561) | Replicas of regions can be cached from different instances of the table in MetaCache |  Major | . |
| [HBASE-12714](https://issues.apache.org/jira/browse/HBASE-12714) | RegionReplicaReplicationEndpoint should not set the RPC Codec |  Major | . |
| [HBASE-11842](https://issues.apache.org/jira/browse/HBASE-11842) | Integration test for async wal replication to secondary regions |  Major | . |
| [HBASE-13013](https://issues.apache.org/jira/browse/HBASE-13013) | Add read lock to ExpiredMobFileCleanerChore |  Major | master |
| [HBASE-12425](https://issues.apache.org/jira/browse/HBASE-12425) | Document the phases of the split transaction |  Major | documentation |
| [HBASE-11569](https://issues.apache.org/jira/browse/HBASE-11569) | Flush / Compaction handling from secondary region replicas |  Major | . |
| [HBASE-11910](https://issues.apache.org/jira/browse/HBASE-11910) | Document Premptive Call Me Maybe HBase findings in the online manual |  Major | . |
| [HBASE-11567](https://issues.apache.org/jira/browse/HBASE-11567) | Write bulk load COMMIT events to WAL |  Major | . |
| [HBASE-11568](https://issues.apache.org/jira/browse/HBASE-11568) | Async WAL replication for region replicas |  Major | . |
| [HBASE-11861](https://issues.apache.org/jira/browse/HBASE-11861) | Native MOB Compaction mechanisms. |  Major | regionserver, Scanners |
| [HBASE-10942](https://issues.apache.org/jira/browse/HBASE-10942) | support parallel request cancellation for multi-get |  Major | . |
| [HBASE-12936](https://issues.apache.org/jira/browse/HBASE-12936) | Quota support for namespace should take region merge into account |  Major | . |
| [HBASE-11574](https://issues.apache.org/jira/browse/HBASE-11574) | hbase:meta's regions can be replicated |  Major | . |
| [HBASE-11908](https://issues.apache.org/jira/browse/HBASE-11908) | Region replicas should be added to the meta table at the time of table creation |  Major | . |
| [HBASE-7847](https://issues.apache.org/jira/browse/HBASE-7847) | Use zookeeper multi to clear znodes |  Major | . |
| [HBASE-12669](https://issues.apache.org/jira/browse/HBASE-12669) | Have compaction scanner save info about delete markers |  Major | regionserver, Scanners |
| [HBASE-8410](https://issues.apache.org/jira/browse/HBASE-8410) | Basic quota support for namespaces |  Major | . |
| [HBASE-12820](https://issues.apache.org/jira/browse/HBASE-12820) | Use table lock instead of MobZookeeper |  Major | regionserver, Scanners |
| [HBASE-12848](https://issues.apache.org/jira/browse/HBASE-12848) | Utilize Flash storage for WAL |  Major | . |
| [HBASE-12708](https://issues.apache.org/jira/browse/HBASE-12708) | Document newly introduced params for using Thrift-over-HTTPS. |  Minor | documentation, Thrift |
| [HBASE-11533](https://issues.apache.org/jira/browse/HBASE-11533) | AsciiDoctor POC |  Minor | build, documentation |
| [HBASE-12695](https://issues.apache.org/jira/browse/HBASE-12695) | JDK 1.8 compilation broken |  Critical | build |
| [HBASE-12331](https://issues.apache.org/jira/browse/HBASE-12331) | Shorten the mob snapshot unit tests |  Major | mob |
| [HBASE-12698](https://issues.apache.org/jira/browse/HBASE-12698) | Add mob cell count to the metadata of each mob file |  Major | regionserver |
| [HBASE-12758](https://issues.apache.org/jira/browse/HBASE-12758) | treat mob region as any other region when generating rs manifest. |  Major | mob, snapshots |
| [HBASE-12012](https://issues.apache.org/jira/browse/HBASE-12012) | Improve cancellation for the scan RPCs |  Major | . |
| [HBASE-12738](https://issues.apache.org/jira/browse/HBASE-12738) | Chunk Ref Guide into file-per-chapter |  Major | documentation |
| [HBASE-12528](https://issues.apache.org/jira/browse/HBASE-12528) | Document the newly introduced params for providing principal and keytabs. |  Minor | documentation |
| [HBASE-12691](https://issues.apache.org/jira/browse/HBASE-12691) | sweep job needs to exit non-zero if job fails for any reason. |  Major | mob |
| [HBASE-12648](https://issues.apache.org/jira/browse/HBASE-12648) | Document per cell TTLs |  Minor | documentation |
| [HBASE-12646](https://issues.apache.org/jira/browse/HBASE-12646) | SnapshotInfo tool does not find mob data in snapshots |  Major | mob, snapshots |
| [HBASE-11903](https://issues.apache.org/jira/browse/HBASE-11903) | Directly invoking split & merge of replica regions should be disallowed |  Major | . |
| [HBASE-12523](https://issues.apache.org/jira/browse/HBASE-12523) | Update checkstyle plugin rules to match our use |  Minor | build |
| [HBASE-12591](https://issues.apache.org/jira/browse/HBASE-12591) | Ignore the count of mob compaction metrics when there is issue |  Minor | regionserver |
| [HBASE-12543](https://issues.apache.org/jira/browse/HBASE-12543) | Incorrect log info in the store compaction of mob |  Minor | regionserver |
| [HBASE-12487](https://issues.apache.org/jira/browse/HBASE-12487) | Explicitly flush the file name in sweep job |  Major | regionserver, Scanners |
| [HBASE-12486](https://issues.apache.org/jira/browse/HBASE-12486) | Move the mob table name tag to the 2nd one |  Major | regionserver, Scanners |
| [HBASE-12489](https://issues.apache.org/jira/browse/HBASE-12489) |  Incorrect 'mobFileCacheMissCount' calculated in the mob metrics |  Major | regionserver, Scanners |
| [HBASE-12382](https://issues.apache.org/jira/browse/HBASE-12382) | Restore incremental compilation |  Major | . |
| [HBASE-12343](https://issues.apache.org/jira/browse/HBASE-12343) | Document recommended configuration for 0.98 from HBASE-11964 |  Major | . |
| [HBASE-4625](https://issues.apache.org/jira/browse/HBASE-4625) | Convert @deprecated HBaseTestCase tests  JUnit4 style tests |  Minor | . |
| [HBASE-12392](https://issues.apache.org/jira/browse/HBASE-12392) | Incorrect implementation of CompactionRequest.isRetainDeleteMarkers |  Critical | regionserver, Scanners |
| [HBASE-12391](https://issues.apache.org/jira/browse/HBASE-12391) | Correct a typo in the mob metrics |  Minor | regionserver, Scanners |
| [HBASE-11683](https://issues.apache.org/jira/browse/HBASE-11683) | Metrics for MOB |  Major | regionserver, Scanners |
| [HBASE-11912](https://issues.apache.org/jira/browse/HBASE-11912) | Catch some bad practices at compile time with error-prone |  Major | . |
| [HBASE-11645](https://issues.apache.org/jira/browse/HBASE-11645) | Snapshot for MOB |  Major | snapshots |
| [HBASE-12093](https://issues.apache.org/jira/browse/HBASE-12093) | Support the mob attributes in hbase shell when create/alter table |  Major | shell |
| [HBASE-12085](https://issues.apache.org/jira/browse/HBASE-12085) | mob status should print human readable numbers. |  Major | mob, UI |
| [HBASE-12080](https://issues.apache.org/jira/browse/HBASE-12080) | Shorten the run time of integration test by default when using mvn failsafe:integration-test |  Major | test |
| [HBASE-12066](https://issues.apache.org/jira/browse/HBASE-12066) | Avoid major compaction in TestMobSweeper |  Major | Compaction, mob |
| [HBASE-11644](https://issues.apache.org/jira/browse/HBASE-11644) | External MOB compaction tools |  Major | Compaction, master |
| [HBASE-11472](https://issues.apache.org/jira/browse/HBASE-11472) | Remove ZKTableStateClientSideReader class |  Major | Client, Consensus, Zookeeper |
| [HBASE-11598](https://issues.apache.org/jira/browse/HBASE-11598) | Add simple rpc throttling |  Minor | . |
| [HBASE-8139](https://issues.apache.org/jira/browse/HBASE-8139) | Allow job names to be overridden |  Major | mapreduce, Usability |
| [HBASE-11646](https://issues.apache.org/jira/browse/HBASE-11646) | Handle the MOB in compaction |  Major | Compaction |
| [HBASE-11986](https://issues.apache.org/jira/browse/HBASE-11986) | Document MOB in Ref Guide |  Major | documentation |
| [HBASE-12000](https://issues.apache.org/jira/browse/HBASE-12000) | isMob and mobThreshold do not follow column descriptor property naming conventions |  Major | regionserver, Scanners |
| [HBASE-11975](https://issues.apache.org/jira/browse/HBASE-11975) | Remove the explicit list of maven repositories in pom.xml |  Major | . |
| [HBASE-7767](https://issues.apache.org/jira/browse/HBASE-7767) | Get rid of ZKTable, and table enable/disable state in ZK |  Major | Zookeeper |
| [HBASE-11647](https://issues.apache.org/jira/browse/HBASE-11647) | MOB integration testing |  Major | Performance, test |
| [HBASE-11911](https://issues.apache.org/jira/browse/HBASE-11911) | Break up tests into more fine grained categories |  Major | . |
| [HBASE-11901](https://issues.apache.org/jira/browse/HBASE-11901) | Improve the value size of the reference cell in mob column |  Major | . |
| [HBASE-11786](https://issues.apache.org/jira/browse/HBASE-11786) | Document web UI for tracking time spent in coprocessors |  Minor | Coprocessors, documentation |
| [HBASE-11643](https://issues.apache.org/jira/browse/HBASE-11643) | Read and write MOB in HBase |  Major | regionserver, Scanners |
| [HBASE-11779](https://issues.apache.org/jira/browse/HBASE-11779) | Document the new requirement to set JAVA\_HOME before starting HBase |  Major | documentation |
| [HBASE-11607](https://issues.apache.org/jira/browse/HBASE-11607) | Document HBase metrics |  Major | documentation, metrics |
| [HBASE-10674](https://issues.apache.org/jira/browse/HBASE-10674) | HBCK should be updated to do replica related checks |  Major | . |
| [HBASE-4624](https://issues.apache.org/jira/browse/HBASE-4624) | Remove and convert @deprecated RemoteExceptionHandler.decodeRemoteException calls |  Major | . |
| [HBASE-11261](https://issues.apache.org/jira/browse/HBASE-11261) | Handle splitting/merging of regions that have region\_replication greater than one |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-8770](https://issues.apache.org/jira/browse/HBASE-8770) | deletes and puts with the same ts should be resolved according to mvcc/seqNum |  Blocker | . |
| [HBASE-20067](https://issues.apache.org/jira/browse/HBASE-20067) | Clean up findbugs warnings |  Critical | . |
| [HBASE-10403](https://issues.apache.org/jira/browse/HBASE-10403) | Simplify offheap cache configuration |  Minor | io |
| [HBASE-19976](https://issues.apache.org/jira/browse/HBASE-19976) | Dead lock if the worker threads in procedure executor are exhausted |  Critical | . |
| [HBASE-19948](https://issues.apache.org/jira/browse/HBASE-19948) | Since HBASE-19873, HBaseClassTestRule, Small/Medium/Large has different semantic |  Major | . |
| [HBASE-19296](https://issues.apache.org/jira/browse/HBASE-19296) | Fix findbugs and error-prone warnings (branch-2) |  Major | . |
| [HBASE-19425](https://issues.apache.org/jira/browse/HBASE-19425) | Align the methods in Put/Delete/Increment/Append |  Major | . |
| [HBASE-19627](https://issues.apache.org/jira/browse/HBASE-19627) | Stabilize the method signature for Cell and its impl |  Critical | . |
| [HBASE-18429](https://issues.apache.org/jira/browse/HBASE-18429) | ITs attempt to modify immutable table/column descriptors |  Critical | integration tests |
| [HBASE-18110](https://issues.apache.org/jira/browse/HBASE-18110) | [AMv2] Reenable tests temporarily disabled |  Blocker | Region Assignment |
| [HBASE-18978](https://issues.apache.org/jira/browse/HBASE-18978) | Align the methods in Table and AsyncTable |  Critical | asyncclient, Client |
| [HBASE-18805](https://issues.apache.org/jira/browse/HBASE-18805) | Unify Admin and AsyncAdmin |  Major | . |
| [HBASE-18703](https://issues.apache.org/jira/browse/HBASE-18703) | Inconsistent behavior for preBatchMutate in doMiniBatchMutate and processRowsWithLocks |  Critical | Coprocessors |
| [HBASE-18926](https://issues.apache.org/jira/browse/HBASE-18926) | Cleanup Optional\<T\> from method params |  Major | . |
| [HBASE-17143](https://issues.apache.org/jira/browse/HBASE-17143) | Scan improvement |  Blocker | Client, scan |
| [HBASE-18751](https://issues.apache.org/jira/browse/HBASE-18751) | Revisit the TimeRange and TimeRangeTracker |  Major | . |
| [HBASE-7320](https://issues.apache.org/jira/browse/HBASE-7320) | Remove KeyValue.getBuffer() |  Blocker | . |
| [HBASE-10462](https://issues.apache.org/jira/browse/HBASE-10462) | Recategorize some of the client facing Public / Private interfaces |  Blocker | Client |
| [HBASE-18696](https://issues.apache.org/jira/browse/HBASE-18696) | Fix the problems when introducing the new hbase-mapreduce module |  Blocker | mapreduce |
| [HBASE-18501](https://issues.apache.org/jira/browse/HBASE-18501) | Use TableDescriptor and ColumnFamilyDescriptor as far as possible |  Critical | . |
| [HBASE-18266](https://issues.apache.org/jira/browse/HBASE-18266) | Eliminate the warnings from the spotbugs |  Major | . |
| [HBASE-17359](https://issues.apache.org/jira/browse/HBASE-17359) | Implement async admin |  Major | Client |
| [HBASE-15086](https://issues.apache.org/jira/browse/HBASE-15086) | Fix findbugs complaint so yetus reports more green |  Major | build |
| [HBASE-6581](https://issues.apache.org/jira/browse/HBASE-6581) | Build with hadoop.profile=3.0 |  Major | . |
| [HBASE-16617](https://issues.apache.org/jira/browse/HBASE-16617) | Procedure V2 - Improvements |  Minor | master, proc-v2 |
| [HBASE-16833](https://issues.apache.org/jira/browse/HBASE-16833) | Implement asynchronous hbase client based on HBASE-15921 |  Major | Client |
| [HBASE-18037](https://issues.apache.org/jira/browse/HBASE-18037) | Do not expose implementation classes to CP |  Blocker | Coprocessors |
| [HBASE-15179](https://issues.apache.org/jira/browse/HBASE-15179) | Cell/DBB end-to-end on the write-path |  Major | regionserver |
| [HBASE-16145](https://issues.apache.org/jira/browse/HBASE-16145) | MultiRowRangeFilter constructor shouldn't throw IOException |  Minor | . |
| [HBASE-16567](https://issues.apache.org/jira/browse/HBASE-16567) | Upgrade to protobuf-3.1.x |  Critical | Protobufs |
| [HBASE-11871](https://issues.apache.org/jira/browse/HBASE-11871) | Avoid usage of KeyValueUtil#ensureKeyValue |  Major | regionserver |
| [HBASE-16432](https://issues.apache.org/jira/browse/HBASE-16432) | Revisit the asynchronous ipc implementation |  Major | rpc |
| [HBASE-14460](https://issues.apache.org/jira/browse/HBASE-14460) | [Perf Regression] Merge of MVCC and SequenceId (HBASE-8763) slowed Increments, CheckAndPuts, batch operations |  Critical | Performance |
| [HBASE-11425](https://issues.apache.org/jira/browse/HBASE-11425) | Cell/DBB end-to-end on the read-path |  Major | regionserver, Scanners |
| [HBASE-14457](https://issues.apache.org/jira/browse/HBASE-14457) | Umbrella: Improve Multiple WAL for production usage |  Major | . |
| [HBASE-14420](https://issues.apache.org/jira/browse/HBASE-14420) | Zombie Stomping Session |  Critical | test |
| [HBASE-14869](https://issues.apache.org/jira/browse/HBASE-14869) | Better request latency and size histograms |  Major | . |
| [HBASE-11339](https://issues.apache.org/jira/browse/HBASE-11339) | HBase MOB |  Major | regionserver, Scanners |
| [HBASE-13747](https://issues.apache.org/jira/browse/HBASE-13747) | Promote Java 8 to "yes" in support matrix |  Critical | java |
| [HBASE-7781](https://issues.apache.org/jira/browse/HBASE-7781) | Update security unit tests to use a KDC if available |  Blocker | security, test |
| [HBASE-12859](https://issues.apache.org/jira/browse/HBASE-12859) | New master API to track major compaction completion |  Major | . |
| [HBASE-18792](https://issues.apache.org/jira/browse/HBASE-18792) | hbase-2 needs to defend against hbck operations |  Blocker | hbck |
| [HBASE-19963](https://issues.apache.org/jira/browse/HBASE-19963) | TestFSHDFSUtils assumes wrong default port for Hadoop 3.0.1+ |  Major | test |
| [HBASE-20386](https://issues.apache.org/jira/browse/HBASE-20386) | [DOC] Align WALPlayer help text and refguide |  Minor | documentation |
| [HBASE-20365](https://issues.apache.org/jira/browse/HBASE-20365) | start branch-2.0 specific website section |  Critical | documentation, website |
| [HBASE-14175](https://issues.apache.org/jira/browse/HBASE-14175) | Adopt releasedocmaker for better generated release notes |  Critical | . |
| [HBASE-14348](https://issues.apache.org/jira/browse/HBASE-14348) | Update download mirror link |  Major | documentation, website |
| [HBASE-20299](https://issues.apache.org/jira/browse/HBASE-20299) | Update MOB in hbase refguide |  Minor | mob |
| [HBASE-20132](https://issues.apache.org/jira/browse/HBASE-20132) | Change the "KV" to "Cell" for web UI |  Minor | . |
| [HBASE-20199](https://issues.apache.org/jira/browse/HBASE-20199) | Add test to prevent further permission regression around table flush and snapshot |  Critical | test |
| [HBASE-20223](https://issues.apache.org/jira/browse/HBASE-20223) | Use hbase-thirdparty 2.1.0 |  Blocker | dependencies |
| [HBASE-19158](https://issues.apache.org/jira/browse/HBASE-19158) | Ref guide needs upgrade update |  Blocker | documentation |
| [HBASE-20212](https://issues.apache.org/jira/browse/HBASE-20212) | Make all Public classes have InterfaceAudience category |  Critical | Usability |
| [HBASE-20246](https://issues.apache.org/jira/browse/HBASE-20246) | Remove the spark module |  Blocker | . |
| [HBASE-19552](https://issues.apache.org/jira/browse/HBASE-19552) | update hbase to use new thirdparty libs |  Major | dependencies, thirdparty |
| [HBASE-20171](https://issues.apache.org/jira/browse/HBASE-20171) | Remove o.a.h.h.ProcedureState |  Minor | . |
| [HBASE-15151](https://issues.apache.org/jira/browse/HBASE-15151) | Rely on nightly tests for findbugs compliance on existing branch |  Critical | build, test |
| [HBASE-20075](https://issues.apache.org/jira/browse/HBASE-20075) | remove logic for branch-1.1 nightly testing |  Minor | test |
| [HBASE-20072](https://issues.apache.org/jira/browse/HBASE-20072) | remove 1.1 release line from the prerequisite tables |  Major | community, documentation |
| [HBASE-18882](https://issues.apache.org/jira/browse/HBASE-18882) | [TEST] Run MR branch-1 jobs against hbase2 cluster |  Critical | mapreduce, test |
| [HBASE-20093](https://issues.apache.org/jira/browse/HBASE-20093) | Replace ServerLoad by ServerMetrics for ServerManager |  Major | . |
| [HBASE-20097](https://issues.apache.org/jira/browse/HBASE-20097) | Merge TableDescriptors#getAll and TableDescriptors#getAllDescriptors into one |  Minor | . |
| [HBASE-20084](https://issues.apache.org/jira/browse/HBASE-20084) | Refactor the RSRpcServices#doBatchOp |  Minor | regionserver |
| [HBASE-20092](https://issues.apache.org/jira/browse/HBASE-20092) | Fix TestRegionMetrics#testRegionMetrics |  Minor | test |
| [HBASE-20088](https://issues.apache.org/jira/browse/HBASE-20088) | Update copyright notices to year 2018 |  Minor | . |
| [HBASE-20089](https://issues.apache.org/jira/browse/HBASE-20089) | make\_rc.sh should name SHA-512 checksum files with the extension .sha512 |  Minor | . |
| [HBASE-18596](https://issues.apache.org/jira/browse/HBASE-18596) | [TEST] A hbase1 cluster should be able to replicate to a hbase2 cluster; verify |  Blocker | . |
| [HBASE-19947](https://issues.apache.org/jira/browse/HBASE-19947) | MR jobs using ITU use wrong filesystem |  Critical | integration tests |
| [HBASE-19946](https://issues.apache.org/jira/browse/HBASE-19946) | TestPerColumnFamilyFlush and TestWalAndCompactingMemStoreFlush fail against h3 |  Major | test |
| [HBASE-19922](https://issues.apache.org/jira/browse/HBASE-19922) | ProtobufUtils::PRIMITIVES is unused |  Major | Protobufs |
| [HBASE-19720](https://issues.apache.org/jira/browse/HBASE-19720) | Rename WALKey#getTabnename to WALKey#getTableName |  Major | . |
| [HBASE-15042](https://issues.apache.org/jira/browse/HBASE-15042) | refactor so that site materials are in the Standard Maven Place |  Minor | build, website |
| [HBASE-19596](https://issues.apache.org/jira/browse/HBASE-19596) | RegionMetrics/ServerMetrics/ClusterMetrics should apply to all public classes |  Critical | Client |
| [HBASE-19620](https://issues.apache.org/jira/browse/HBASE-19620) | Add UT to confirm the race in MasterRpcServices.getProcedureResult |  Minor | proc-v2, test |
| [HBASE-19652](https://issues.apache.org/jira/browse/HBASE-19652) | Turn down CleanerChore logging; too chatty |  Major | . |
| [HBASE-19644](https://issues.apache.org/jira/browse/HBASE-19644) | add the checkstyle rule to reject the illegal imports |  Major | . |
| [HBASE-19637](https://issues.apache.org/jira/browse/HBASE-19637) | Add .checkstyle to gitignore |  Major | build |
| [HBASE-19550](https://issues.apache.org/jira/browse/HBASE-19550) | Wrap the cell passed via Mutation#add(Cell) to be of ExtendedCell |  Major | . |
| [HBASE-18970](https://issues.apache.org/jira/browse/HBASE-18970) | The version of jruby we use now can't get interactive input from prompt |  Critical | shell |
| [HBASE-19548](https://issues.apache.org/jira/browse/HBASE-19548) | Backport the missed doc fix from master to branch-2 |  Major | . |
| [HBASE-19526](https://issues.apache.org/jira/browse/HBASE-19526) | Update hadoop version to 3.0 GA |  Major | build, dependencies |
| [HBASE-19477](https://issues.apache.org/jira/browse/HBASE-19477) | Move and align documentation in hbase-annotations |  Trivial | . |
| [HBASE-19267](https://issues.apache.org/jira/browse/HBASE-19267) | Eclipse project import issues on 2.0 |  Major | build |
| [HBASE-19512](https://issues.apache.org/jira/browse/HBASE-19512) | Move EventType and ExecutorType from hbase-client to hbase-server |  Major | regionserver |
| [HBASE-19485](https://issues.apache.org/jira/browse/HBASE-19485) | Minor improvement to TestCompactedHFilesDischarger |  Trivial | . |
| [HBASE-18988](https://issues.apache.org/jira/browse/HBASE-18988) | Add release managers to reference guide |  Trivial | documentation |
| [HBASE-19410](https://issues.apache.org/jira/browse/HBASE-19410) | Move zookeeper related UTs to hbase-zookeeper and mark them as ZKTests |  Major | test, Zookeeper |
| [HBASE-19323](https://issues.apache.org/jira/browse/HBASE-19323) | Make netty engine default in hbase2 |  Major | rpc |
| [HBASE-19416](https://issues.apache.org/jira/browse/HBASE-19416) | Document dynamic configurations currently support |  Minor | documentation |
| [HBASE-19298](https://issues.apache.org/jira/browse/HBASE-19298) | CellScanner and CellScannable should be declared as IA.Public |  Major | . |
| [HBASE-19408](https://issues.apache.org/jira/browse/HBASE-19408) | Remove WALActionsListener.Base |  Trivial | . |
| [HBASE-19407](https://issues.apache.org/jira/browse/HBASE-19407) | [branch-2] Remove backup/restore |  Blocker | . |
| [HBASE-19328](https://issues.apache.org/jira/browse/HBASE-19328) | Remove asked if splittable log messages |  Minor | proc-v2 |
| [HBASE-19200](https://issues.apache.org/jira/browse/HBASE-19200) | make hbase-client only depend on ZKAsyncRegistry and ZNodePaths |  Major | Client, Zookeeper |
| [HBASE-19123](https://issues.apache.org/jira/browse/HBASE-19123) | Purge 'complete' support from Coprocesor Observers |  Major | Coprocessors |
| [HBASE-19241](https://issues.apache.org/jira/browse/HBASE-19241) | Improve javadoc for AsyncAdmin and cleanup warnings for the implementation classes |  Major | documentation |
| [HBASE-14350](https://issues.apache.org/jira/browse/HBASE-14350) | Procedure V2 Phase 2: Assignment Manager |  Blocker | master, proc-v2 |
| [HBASE-19224](https://issues.apache.org/jira/browse/HBASE-19224) | LICENSE failure for Hadoop 3.1 on dnsjava |  Major | build |
| [HBASE-18817](https://issues.apache.org/jira/browse/HBASE-18817) | Pull hbase-spark module out of branch-2 |  Blocker | spark |
| [HBASE-19217](https://issues.apache.org/jira/browse/HBASE-19217) | Update supplemental-models.xml for jetty-sslengine |  Major | . |
| [HBASE-19179](https://issues.apache.org/jira/browse/HBASE-19179) | Remove hbase-prefix-tree |  Critical | . |
| [HBASE-19176](https://issues.apache.org/jira/browse/HBASE-19176) | Remove hbase-native-client from branch-2 |  Major | . |
| [HBASE-19097](https://issues.apache.org/jira/browse/HBASE-19097) | update testing to use Apache Yetus Test Patch version 0.6.0 |  Major | build |
| [HBASE-19049](https://issues.apache.org/jira/browse/HBASE-19049) | Update kerby to 1.0.1 GA release |  Major | dependencies |
| [HBASE-16338](https://issues.apache.org/jira/browse/HBASE-16338) | update jackson to 2.y |  Major | dependencies |
| [HBASE-19007](https://issues.apache.org/jira/browse/HBASE-19007) | Align Services Interfaces in Master and RegionServer |  Blocker | . |
| [HBASE-19043](https://issues.apache.org/jira/browse/HBASE-19043) | Purge TableWrapper and CoprocessorHConnnection |  Major | Coprocessors |
| [HBASE-18991](https://issues.apache.org/jira/browse/HBASE-18991) | Remove RegionMergeRequest |  Trivial | . |
| [HBASE-18927](https://issues.apache.org/jira/browse/HBASE-18927) | Add the DataType which is subset of KeyValue#Type to CellBuilder for building cell |  Major | . |
| [HBASE-18833](https://issues.apache.org/jira/browse/HBASE-18833) | Ensure precommit personality is up to date on all active branches |  Critical | test |
| [HBASE-10504](https://issues.apache.org/jira/browse/HBASE-10504) | Define Replication Interface |  Blocker | Replication |
| [HBASE-18767](https://issues.apache.org/jira/browse/HBASE-18767) | Release hbase-2.0.0-alpha-3; Theme "Scrubbed API" |  Major | . |
| [HBASE-18835](https://issues.apache.org/jira/browse/HBASE-18835) | The return type of ExtendedCell#deepClone should be ExtendedCell rather than Cell |  Major | . |
| [HBASE-18766](https://issues.apache.org/jira/browse/HBASE-18766) | Make TableSnapshotScanner Audience Private |  Major | . |
| [HBASE-18782](https://issues.apache.org/jira/browse/HBASE-18782) | Module untangling work |  Major | . |
| [HBASE-18421](https://issues.apache.org/jira/browse/HBASE-18421) | update hadoop prerequisites docs to call out 2.8.1 |  Major | community, dependencies, documentation |
| [HBASE-13868](https://issues.apache.org/jira/browse/HBASE-13868) | Correct "Disable automatic splitting" section in HBase book |  Trivial | documentation |
| [HBASE-17972](https://issues.apache.org/jira/browse/HBASE-17972) | Remove mergePool from CompactSplitThread |  Minor | regionserver |
| [HBASE-18768](https://issues.apache.org/jira/browse/HBASE-18768) | Move TestTableName to hbase-common from hbase-server |  Major | . |
| [HBASE-17967](https://issues.apache.org/jira/browse/HBASE-17967) | clean up documentation references to -ROOT- table. |  Minor | documentation |
| [HBASE-18710](https://issues.apache.org/jira/browse/HBASE-18710) | Move on to hbase-thirdparty 1.0.1 (it was just released). |  Major | hbase-thirdparty |
| [HBASE-18705](https://issues.apache.org/jira/browse/HBASE-18705) | bin/hbase does not find cached\_classpath.txt |  Major | . |
| [HBASE-18611](https://issues.apache.org/jira/browse/HBASE-18611) | Copy all tests from o.a.h.h.p.TestProtobufUtil to o.a.h.h.s.p.TestProtobufUtil |  Minor | Protobufs, test |
| [HBASE-18670](https://issues.apache.org/jira/browse/HBASE-18670) | Add .DS\_Store to .gitignore |  Minor | community |
| [HBASE-17614](https://issues.apache.org/jira/browse/HBASE-17614) | Move Backup/Restore into separate module |  Blocker | . |
| [HBASE-18660](https://issues.apache.org/jira/browse/HBASE-18660) | Remove duplicate code from the checkAndPut method in HTable |  Trivial | . |
| [HBASE-18653](https://issues.apache.org/jira/browse/HBASE-18653) | Undo hbase2 check against \< hadoop2.6.x; i.e. implement agreed drop of hadoop 2.4 and 2.5 support in hbase2 |  Major | build |
| [HBASE-18630](https://issues.apache.org/jira/browse/HBASE-18630) | Prune dependencies; as is branch-2 has duplicates |  Major | dependencies |
| [HBASE-18594](https://issues.apache.org/jira/browse/HBASE-18594) | Release hbase-2.0.0-alpha2 |  Major | . |
| [HBASE-18623](https://issues.apache.org/jira/browse/HBASE-18623) | Frequent failed to parse at EOF warnings from WALEntryStream |  Minor | . |
| [HBASE-18518](https://issues.apache.org/jira/browse/HBASE-18518) | Remove jersey1\* dependencies from project and jersey1\* jars from lib dir |  Major | dependencies, pom, REST |
| [HBASE-18544](https://issues.apache.org/jira/browse/HBASE-18544) | Move the HRegion#addRegionToMETA to TestDefaultMemStore |  Minor | regionserver |
| [HBASE-18582](https://issues.apache.org/jira/browse/HBASE-18582) | Correct the docs for Mutation#setCellVisibility |  Minor | documentation |
| [HBASE-18588](https://issues.apache.org/jira/browse/HBASE-18588) | Verify we're using netty .so epolling on linux post HBASE-18271 |  Major | test |
| [HBASE-18514](https://issues.apache.org/jira/browse/HBASE-18514) | Backport space quota "phase2" work to branch-2 |  Blocker | . |
| [HBASE-18527](https://issues.apache.org/jira/browse/HBASE-18527) | update nightly builds to compensate for jenkins plugin upgrades |  Blocker | community, test |
| [HBASE-18515](https://issues.apache.org/jira/browse/HBASE-18515) |  Introduce Delete.add as a replacement for Delete#addDeleteMarker |  Major | Client |
| [HBASE-16100](https://issues.apache.org/jira/browse/HBASE-16100) | Procedure V2 - Tools |  Minor | proc-v2, tooling |
| [HBASE-15141](https://issues.apache.org/jira/browse/HBASE-15141) | Procedure V2 - Web UI |  Blocker | proc-v2, UI |
| [HBASE-18465](https://issues.apache.org/jira/browse/HBASE-18465) | [AMv2] remove old split region code that is no longer needed |  Major | Admin, rpc |
| [HBASE-16728](https://issues.apache.org/jira/browse/HBASE-16728) | Add hadoop.profile=3.0 pass to precommit checks. |  Major | build, hadoop3 |
| [HBASE-18384](https://issues.apache.org/jira/browse/HBASE-18384) | Add link to refguide schema section on apache blog on hbase application archetypes |  Minor | documentation |
| [HBASE-18344](https://issues.apache.org/jira/browse/HBASE-18344) | Introduce Append.addColumn as a replacement for Append.add |  Trivial | . |
| [HBASE-18364](https://issues.apache.org/jira/browse/HBASE-18364) | Downgrade surefire |  Major | . |
| [HBASE-18291](https://issues.apache.org/jira/browse/HBASE-18291) | Regenerate thrift2 python examples |  Minor | . |
| [HBASE-18288](https://issues.apache.org/jira/browse/HBASE-18288) | Declared dependency on specific javax.ws.rs |  Major | dependencies, REST |
| [HBASE-18284](https://issues.apache.org/jira/browse/HBASE-18284) | Update hbasecon asia logo on home page of hbase.apache.org |  Major | . |
| [HBASE-12794](https://issues.apache.org/jira/browse/HBASE-12794) | Guidelines for filing JIRA issues |  Major | documentation |
| [HBASE-18258](https://issues.apache.org/jira/browse/HBASE-18258) | Take down hbasecon2017 logo from hbase.apache.org and put up hbaseconasia2017 instead. |  Major | . |
| [HBASE-17954](https://issues.apache.org/jira/browse/HBASE-17954) | Switch findbugs implementation to spotbugs |  Major | build, community, test |
| [HBASE-17898](https://issues.apache.org/jira/browse/HBASE-17898) | Update dependencies |  Critical | . |
| [HBASE-18187](https://issues.apache.org/jira/browse/HBASE-18187) | Release hbase-2.0.0-alpha1 |  Major | . |
| [HBASE-18096](https://issues.apache.org/jira/browse/HBASE-18096) | Limit HFileUtil visibility and add missing annotations |  Trivial | . |
| [HBASE-13074](https://issues.apache.org/jira/browse/HBASE-13074) | Cleaned up usage of hbase.master.lease.thread.wakefrequency |  Trivial | wal |
| [HBASE-17968](https://issues.apache.org/jira/browse/HBASE-17968) | Update copyright year in NOTICE file |  Trivial | build |
| [HBASE-17965](https://issues.apache.org/jira/browse/HBASE-17965) | Canary tool should print the regionserver name on failure |  Minor | . |
| [HBASE-17828](https://issues.apache.org/jira/browse/HBASE-17828) | Revisit the IS annotation and the documentation |  Critical | API |
| [HBASE-16215](https://issues.apache.org/jira/browse/HBASE-16215) | clean up references for EOM release lines |  Major | community, website |
| [HBASE-17847](https://issues.apache.org/jira/browse/HBASE-17847) | update documentation to include positions on recent Hadoop releases |  Critical | community, documentation |
| [HBASE-17834](https://issues.apache.org/jira/browse/HBASE-17834) | close stale github PRs |  Minor | community |
| [HBASE-17802](https://issues.apache.org/jira/browse/HBASE-17802) | Add note that minor versions can add methods to Interfaces |  Major | documentation |
| [HBASE-17782](https://issues.apache.org/jira/browse/HBASE-17782) | Extend IdReadWriteLock to support using both weak and soft reference |  Major | . |
| [HBASE-17618](https://issues.apache.org/jira/browse/HBASE-17618) | Refactor the implementation of modify table and delete column in MOB |  Major | mob |
| [HBASE-17609](https://issues.apache.org/jira/browse/HBASE-17609) | Allow for region merging in the UI |  Major | . |
| [HBASE-16812](https://issues.apache.org/jira/browse/HBASE-16812) | Clean up the locks in MOB |  Minor | mob |
| [HBASE-17502](https://issues.apache.org/jira/browse/HBASE-17502) | Document hadoop pre-2.6.1 and Java 1.8 Kerberos problem in our hadoop support matrix |  Major | . |
| [HBASE-16710](https://issues.apache.org/jira/browse/HBASE-16710) | Add ZStandard Codec to Compression.java |  Minor | . |
| [HBASE-17401](https://issues.apache.org/jira/browse/HBASE-17401) | Remove unnecessary semicolons in hbase-annotations |  Trivial | . |
| [HBASE-17397](https://issues.apache.org/jira/browse/HBASE-17397) | AggregationClient cleanup |  Minor | . |
| [HBASE-16869](https://issues.apache.org/jira/browse/HBASE-16869) | Typo in "Disabling Blockcache" doc |  Trivial | documentation |
| [HBASE-17272](https://issues.apache.org/jira/browse/HBASE-17272) | Doc how to run Standalone HBase over an HDFS instance; all daemons in one JVM but persisting to an HDFS instance |  Major | documentation |
| [HBASE-17121](https://issues.apache.org/jira/browse/HBASE-17121) | Undo the building of xref as part of site build |  Major | . |
| [HBASE-16335](https://issues.apache.org/jira/browse/HBASE-16335) | update to latest apache parent pom |  Major | build, dependencies |
| [HBASE-17046](https://issues.apache.org/jira/browse/HBASE-17046) | Add 1.1 doc to hbase.apache.org |  Major | website |
| [HBASE-16955](https://issues.apache.org/jira/browse/HBASE-16955) | Fixup precommit protoc check to do new distributed protos and pb 3.1.0 build |  Major | build, Protobufs |
| [HBASE-16413](https://issues.apache.org/jira/browse/HBASE-16413) | Add apache-hbase.slack.com #users channel to ref guide |  Minor | documentation |
| [HBASE-16952](https://issues.apache.org/jira/browse/HBASE-16952) | Replace hadoop-maven-plugins with protobuf-maven-plugin for building protos |  Major | build |
| [HBASE-16811](https://issues.apache.org/jira/browse/HBASE-16811) | Remove mob sweep job |  Minor | . |
| [HBASE-16591](https://issues.apache.org/jira/browse/HBASE-16591) | Add a docker file only contains java 8 for running pre commit on master |  Blocker | build |
| [HBASE-16518](https://issues.apache.org/jira/browse/HBASE-16518) | Remove old .arcconfig file |  Trivial | tooling |
| [HBASE-16376](https://issues.apache.org/jira/browse/HBASE-16376) | Document implicit side-effects on partial results when calling Scan#setBatch(int) |  Minor | API, documentation |
| [HBASE-16467](https://issues.apache.org/jira/browse/HBASE-16467) | Move AbstractHBaseTool to hbase-common |  Trivial | . |
| [HBASE-16260](https://issues.apache.org/jira/browse/HBASE-16260) | Audit dependencies for Category-X |  Critical | community, dependencies |
| [HBASE-16426](https://issues.apache.org/jira/browse/HBASE-16426) | Remove company affiliations from committer list |  Major | documentation, website |
| [HBASE-16354](https://issues.apache.org/jira/browse/HBASE-16354) | Clean up defunct GitHub PRs |  Major | community |
| [HBASE-15656](https://issues.apache.org/jira/browse/HBASE-15656) | Fix unused protobuf warning in Admin.proto |  Minor | . |
| [HBASE-16073](https://issues.apache.org/jira/browse/HBASE-16073) | update compatibility\_checker for jacc dropping comma sep args |  Critical | build, documentation |
| [HBASE-15989](https://issues.apache.org/jira/browse/HBASE-15989) | Remove hbase.online.schema.update.enable |  Major | . |
| [HBASE-15888](https://issues.apache.org/jira/browse/HBASE-15888) | Extend HBASE-12769 for bulk load data replication |  Major | Replication |
| [HBASE-15895](https://issues.apache.org/jira/browse/HBASE-15895) | remove unmaintained jenkins build analysis tool. |  Minor | build |
| [HBASE-14635](https://issues.apache.org/jira/browse/HBASE-14635) | Fix flaky test TestSnapshotCloneIndependence |  Major | test |
| [HBASE-4368](https://issues.apache.org/jira/browse/HBASE-4368) | Expose processlist in shell (per regionserver and perhaps by cluster) |  Major | shell |
| [HBASE-15646](https://issues.apache.org/jira/browse/HBASE-15646) | Add some docs about exporting and importing snapshots using S3 |  Major | documentation, snapshots |
| [HBASE-15729](https://issues.apache.org/jira/browse/HBASE-15729) | Remove old JDiff wrapper scripts in dev-support |  Minor | build, community |
| [HBASE-15644](https://issues.apache.org/jira/browse/HBASE-15644) | Add maven-scala-plugin for scaladoc |  Major | build |
| [HBASE-15494](https://issues.apache.org/jira/browse/HBASE-15494) | Close obviated PRs on github |  Major | community |
| [HBASE-14678](https://issues.apache.org/jira/browse/HBASE-14678) | Experiment: Temporarily disable balancer and a few others to see if root of crashed/timedout JVMs |  Critical | test |
| [HBASE-15502](https://issues.apache.org/jira/browse/HBASE-15502) | Skeleton unit test to copy/paste |  Major | documentation |
| [HBASE-15220](https://issues.apache.org/jira/browse/HBASE-15220) | Change two logs in SimpleRegionNormalizer to INFO level |  Minor | . |
| [HBASE-15017](https://issues.apache.org/jira/browse/HBASE-15017) | Close out GitHub pull requests that aren't likely to update into a usable patch |  Trivial | community |
| [HBASE-14526](https://issues.apache.org/jira/browse/HBASE-14526) | Remove delayed rpc |  Major | . |
| [HBASE-15099](https://issues.apache.org/jira/browse/HBASE-15099) | Move RegionStateListener class out of quotas package |  Minor | . |
| [HBASE-15007](https://issues.apache.org/jira/browse/HBASE-15007) | update Hadoop support matrix to list 2.6.1+ as supported |  Major | documentation, Operability |
| [HBASE-15003](https://issues.apache.org/jira/browse/HBASE-15003) | Remove BoundedConcurrentLinkedQueue and associated test |  Minor | util |
| [HBASE-11985](https://issues.apache.org/jira/browse/HBASE-11985) | Document sizing rules of thumb |  Major | documentation |
| [HBASE-14994](https://issues.apache.org/jira/browse/HBASE-14994) | Clean up some broken links and references to old APIs |  Major | documentation |
| [HBASE-14534](https://issues.apache.org/jira/browse/HBASE-14534) | Bump yammer/coda/dropwizard metrics dependency version |  Minor | . |
| [HBASE-14851](https://issues.apache.org/jira/browse/HBASE-14851) | Add test showing how to use TTL from thrift |  Major | test, Thrift |
| [HBASE-14516](https://issues.apache.org/jira/browse/HBASE-14516) | categorize hadoop-compat tests |  Critical | build, hadoop2, test |
| [HBASE-14308](https://issues.apache.org/jira/browse/HBASE-14308) | HTableDescriptor WARN is not actionable |  Minor | Usability |
| [HBASE-14732](https://issues.apache.org/jira/browse/HBASE-14732) | Update HBase website skin and CSS |  Major | documentation |
| [HBASE-14764](https://issues.apache.org/jira/browse/HBASE-14764) | Stop using post-site target |  Major | documentation |
| [HBASE-14713](https://issues.apache.org/jira/browse/HBASE-14713) | Remove simple deprecated-since-1.0 code in hbase-server from hbase 2.0 |  Major | . |
| [HBASE-14762](https://issues.apache.org/jira/browse/HBASE-14762) | Update docs about publishing website to show gitsubpub method instead of svnsubpub |  Critical | documentation |
| [HBASE-14481](https://issues.apache.org/jira/browse/HBASE-14481) | Decommission HBase wiki |  Blocker | documentation |
| [HBASE-11720](https://issues.apache.org/jira/browse/HBASE-11720) | Set up jenkins job to build site documentation |  Minor | build, documentation |
| [HBASE-14026](https://issues.apache.org/jira/browse/HBASE-14026) | Clarify "Web API" in version and compatibility docs |  Critical | documentation |
| [HBASE-13867](https://issues.apache.org/jira/browse/HBASE-13867) | Add endpoint coprocessor guide to HBase book |  Major | Coprocessors, documentation |
| [HBASE-14652](https://issues.apache.org/jira/browse/HBASE-14652) | Improve / update publish-website script in dev-support |  Major | scripts |
| [HBASE-14493](https://issues.apache.org/jira/browse/HBASE-14493) | Upgrade the jamon-runtime dependency |  Minor | . |
| [HBASE-14502](https://issues.apache.org/jira/browse/HBASE-14502) | Purge use of jmock and remove as dependency |  Major | test |
| [HBASE-14271](https://issues.apache.org/jira/browse/HBASE-14271) | Improve Nexus staging instructions |  Minor | build, documentation |
| [HBASE-14424](https://issues.apache.org/jira/browse/HBASE-14424) | Document that DisabledRegionSplitPolicy blocks manual splits |  Minor | documentation |
| [HBASE-14482](https://issues.apache.org/jira/browse/HBASE-14482) | Add hadoop 2.6.1 to the test-patch build list |  Minor | build |
| [HBASE-14227](https://issues.apache.org/jira/browse/HBASE-14227) | Fold special cased MOB APIs into existing APIs |  Blocker | mob |
| [HBASE-14361](https://issues.apache.org/jira/browse/HBASE-14361) | ReplicationSink should create Connection instances lazily |  Major | Replication |
| [HBASE-14253](https://issues.apache.org/jira/browse/HBASE-14253) | update docs + build for maven 3.0.4+ |  Major | build, documentation |
| [HBASE-14318](https://issues.apache.org/jira/browse/HBASE-14318) | make\_rc.sh should purge/re-resolve dependencies from local repository |  Major | build |
| [HBASE-14290](https://issues.apache.org/jira/browse/HBASE-14290) | Spin up less threads in tests |  Major | test |
| [HBASE-14091](https://issues.apache.org/jira/browse/HBASE-14091) | Update site documentation with code of conduct and project policy for transgressions |  Major | documentation, website |
| [HBASE-14288](https://issues.apache.org/jira/browse/HBASE-14288) | Upgrade asciidoctor plugin to v1.5.2.1 |  Minor | build |
| [HBASE-14085](https://issues.apache.org/jira/browse/HBASE-14085) | Correct LICENSE and NOTICE files in artifacts |  Blocker | build |
| [HBASE-13089](https://issues.apache.org/jira/browse/HBASE-13089) | Fix test compilation error on building against htrace-3.2.0-incubating |  Minor | . |
| [HBASE-14081](https://issues.apache.org/jira/browse/HBASE-14081) | (outdated) references to SVN/trunk in documentation |  Minor | documentation, website |
| [HBASE-14071](https://issues.apache.org/jira/browse/HBASE-14071) | Document troubleshooting unexpected filesystem usage by snapshots and WALs |  Major | . |
| [HBASE-13446](https://issues.apache.org/jira/browse/HBASE-13446) | Add docs warning about missing data for downstream on versions prior to HBASE-13262 |  Critical | documentation |
| [HBASE-14057](https://issues.apache.org/jira/browse/HBASE-14057) | HBase shell user\_permission should list super users defined on hbase-site.xml |  Minor | shell |
| [HBASE-11276](https://issues.apache.org/jira/browse/HBASE-11276) | Add back support for running ChaosMonkey as standalone tool |  Minor | . |
| [HBASE-14052](https://issues.apache.org/jira/browse/HBASE-14052) | Mark a few methods in CellUtil audience private since only make sense internally to hbase |  Trivial | API |
| [HBASE-14053](https://issues.apache.org/jira/browse/HBASE-14053) | Disable DLR in branch-1+ |  Major | Recovery |
| [HBASE-13869](https://issues.apache.org/jira/browse/HBASE-13869) | Fix typo in HBase book |  Trivial | documentation |
| [HBASE-13964](https://issues.apache.org/jira/browse/HBASE-13964) | Skip region normalization for tables under namespace quota |  Major | Balancer, Usability |
| [HBASE-13948](https://issues.apache.org/jira/browse/HBASE-13948) | Expand hadoop2 versions built on the pre-commit |  Major | build |
| [HBASE-13956](https://issues.apache.org/jira/browse/HBASE-13956) | Add myself as 1.1 release manager |  Trivial | website |
| [HBASE-13915](https://issues.apache.org/jira/browse/HBASE-13915) | Remove EOL HBase versions from java and hadoop prereq tables |  Major | documentation |
| [HBASE-13929](https://issues.apache.org/jira/browse/HBASE-13929) | make\_rc.sh publishes empty shaded artifacts |  Minor | build |
| [HBASE-13666](https://issues.apache.org/jira/browse/HBASE-13666) | book.pdf is not renamed during site build |  Major | website |
| [HBASE-13660](https://issues.apache.org/jira/browse/HBASE-13660) | Add link to cloud bigtable schema modeling guide into our refguide |  Major | documentation |
| [HBASE-13799](https://issues.apache.org/jira/browse/HBASE-13799) | javadoc how Scan gets polluted when used; if you set attributes or ask for scan metrics |  Minor | documentation |
| [HBASE-13726](https://issues.apache.org/jira/browse/HBASE-13726) | stop using Hadoop's IOUtils |  Major | dependencies |
| [HBASE-13716](https://issues.apache.org/jira/browse/HBASE-13716) | Stop using Hadoop's FSConstants |  Blocker | Filesystem Integration |
| [HBASE-13582](https://issues.apache.org/jira/browse/HBASE-13582) | Update docs for HTrace |  Minor | documentation |
| [HBASE-13713](https://issues.apache.org/jira/browse/HBASE-13713) | See about dropping ClassLoaderBase#getClassLoadingLock |  Minor | . |
| [HBASE-13586](https://issues.apache.org/jira/browse/HBASE-13586) | Update book on Hadoop and Java supported versions for 1.1.x |  Major | documentation |
| [HBASE-11677](https://issues.apache.org/jira/browse/HBASE-11677) | Make Logger instance modifiers consistent |  Minor | util |
| [HBASE-13554](https://issues.apache.org/jira/browse/HBASE-13554) | Update book clarifying API stability guarantees |  Major | documentation |
| [HBASE-13487](https://issues.apache.org/jira/browse/HBASE-13487) | Doc KEEP\_DELETED\_CELLS |  Major | documentation |
| [HBASE-13187](https://issues.apache.org/jira/browse/HBASE-13187) | Add ITBLL that exercises per CF flush |  Critical | integration tests |
| [HBASE-13361](https://issues.apache.org/jira/browse/HBASE-13361) | Remove or undeprecate {get\|set}ScannerCaching in HTable |  Minor | Client |
| [HBASE-13310](https://issues.apache.org/jira/browse/HBASE-13310) | Fix high priority findbugs warnings |  Major | . |
| [HBASE-13257](https://issues.apache.org/jira/browse/HBASE-13257) | Show coverage report on jenkins |  Minor | . |
| [HBASE-13233](https://issues.apache.org/jira/browse/HBASE-13233) | add hbase-11339 branch to the patch testing script |  Minor | . |
| [HBASE-13237](https://issues.apache.org/jira/browse/HBASE-13237) | Improve trademark marks on the hbase.apache.org homepage |  Minor | documentation |
| [HBASE-13234](https://issues.apache.org/jira/browse/HBASE-13234) | Improve the obviousness of the download link on hbase.apache.org |  Minor | documentation |
| [HBASE-7126](https://issues.apache.org/jira/browse/HBASE-7126) | Update website with info on how to report security bugs |  Critical | documentation |
| [HBASE-12466](https://issues.apache.org/jira/browse/HBASE-12466) | Document visibility scan label generator usage and behavior |  Major | documentation, security |
| [HBASE-13177](https://issues.apache.org/jira/browse/HBASE-13177) | Document HBASE-13012 |  Major | documentation, mob |
| [HBASE-11670](https://issues.apache.org/jira/browse/HBASE-11670) | Build PDF of Ref Guide |  Minor | documentation |
| [HBASE-12180](https://issues.apache.org/jira/browse/HBASE-12180) | Fix and reenable TestRegionReplicaReplicationEndpoint |  Major | test |
| [HBASE-12995](https://issues.apache.org/jira/browse/HBASE-12995) | Document that HConnection#getTable methods do not check table existence since 0.98.1 |  Trivial | . |
| [HBASE-13079](https://issues.apache.org/jira/browse/HBASE-13079) | Add an admonition to Scans example that the results scanner should be closed |  Major | documentation |
| [HBASE-12168](https://issues.apache.org/jira/browse/HBASE-12168) | Document Rest gateway SPNEGO-based authentication for client |  Major | documentation, REST, security |
| [HBASE-12701](https://issues.apache.org/jira/browse/HBASE-12701) | Document how to set the split policy on a given table |  Major | documentation |
| [HBASE-12783](https://issues.apache.org/jira/browse/HBASE-12783) | Create efficient RegionLocator implementation |  Major | . |
| [HBASE-12785](https://issues.apache.org/jira/browse/HBASE-12785) | Use FutureTask to timeout the attempt to get the lock for hbck |  Minor | . |
| [HBASE-12623](https://issues.apache.org/jira/browse/HBASE-12623) | Remove pre-0.96 to 0.96 upgrade code |  Major | . |
| [HBASE-12589](https://issues.apache.org/jira/browse/HBASE-12589) | Forward-port fix for TestFromClientSideWithCoprocessor.testMaxKeyValueSize |  Major | test |
| [HBASE-12515](https://issues.apache.org/jira/browse/HBASE-12515) | update test-patch to use git |  Minor | build |
| [HBASE-12438](https://issues.apache.org/jira/browse/HBASE-12438) | Add  -Dsurefire.rerunFailingTestsCount=2 to patch build runs so flakies get rerun |  Major | test |
| [HBASE-12362](https://issues.apache.org/jira/browse/HBASE-12362) | Interim documentation of important master and regionserver metrics |  Major | documentation |
| [HBASE-12239](https://issues.apache.org/jira/browse/HBASE-12239) | Document hedged reads |  Major | documentation |
| [HBASE-11791](https://issues.apache.org/jira/browse/HBASE-11791) | Update docs on visibility tags and ACLs, transparent encryption, secure bulk upload |  Major | documentation |
| [HBASE-11961](https://issues.apache.org/jira/browse/HBASE-11961) | Document region state transitions |  Minor | . |
| [HBASE-11619](https://issues.apache.org/jira/browse/HBASE-11619) | Remove unused test class from TestHLogSplit |  Trivial | wal |
| [HBASE-4593](https://issues.apache.org/jira/browse/HBASE-4593) | Design and document the official procedure for posting patches, commits, commit messages, etc. to smooth process and make integration with tools easier |  Major | documentation |
| [HBASE-11729](https://issues.apache.org/jira/browse/HBASE-11729) | Document HFile v3 |  Trivial | documentation, HFile |
| [HBASE-11606](https://issues.apache.org/jira/browse/HBASE-11606) | Enable ZK-less region assignment by default |  Minor | . |
| [HBASE-10398](https://issues.apache.org/jira/browse/HBASE-10398) | HBase book updates for Replication after HBASE-10322 |  Major | documentation |
| [HBASE-11459](https://issues.apache.org/jira/browse/HBASE-11459) | Add more doc on compression codecs, how to hook up native lib, lz4, etc. |  Minor | documentation |
| [HBASE-8450](https://issues.apache.org/jira/browse/HBASE-8450) | Update hbase-default.xml and general recommendations to better suit current hw, h2, experience, etc. |  Critical | Usability |


> NOTE: Below pre-2.0.0 changes were generated otherwise. Imported as-is, unaltered.

```
Release Notes - HBase - Version 1.0.0 02/20/2015

** Sub-task
    * [HBASE-11852] - Still see "UnsupportedOperationException: CollectionUsage threshold is not supported"
    * [HBASE-12485] - Maintain SeqId monotonically increasing
    * [HBASE-12511] - namespace permissions - add support from table creation privilege in a namespace 'C'
    * [HBASE-12568] - Adopt Semantic Versioning and document it in the book
    * [HBASE-12575] - Sanity check table coprocessor classes are loadable
    * [HBASE-12606] - Sanity check encryption configuration before opening WAL or onlining regions
    * [HBASE-12679] - Add HBaseInterfaceAudience.TOOLS and move some of the Public classes to LimitedPrivate
    * [HBASE-12704] - Add demo client which uses doAs functionality on Thrift-over-HTTPS.
    * [HBASE-12730] - Backport HBASE-5162 (Basic client pushback mechanism) to branch-1
    * [HBASE-12735] - Refactor TAG so it can live as unit test and as an integration test
    * [HBASE-12763] - Make it so there must be WALs for a server to be marked dead
    * [HBASE-12776] - SpliTransaction: Log number of files to be split
    * [HBASE-12779] - SplitTransaction: Add metrics
    * [HBASE-12793] - [hbck] closeRegionSilentlyAndWait() should log cause of IOException and retry until  hbase.hbck.close.timeout expires
    * [HBASE-12802] - Remove unnecessary Table.flushCommits()
    * [HBASE-12926] - Backport HBASE-12688 (Update site with a bootstrap-based UI) for HBASE-12918
    * [HBASE-12980] - Delete of a table may not clean all rows from hbase:meta

** Bug
    * [HBASE-8026] - HBase Shell docs for scan command does not reference VERSIONS
    * [HBASE-9431] - Set  'hbase.bulkload.retries.number' to 10 as HBASE-8450 claims
    * [HBASE-9910] - TestHFilePerformance and HFilePerformanceEvaluation should be merged in a single HFile performance test class.
    * [HBASE-10499] - In write heavy scenario one of the regions does not get flushed causing RegionTooBusyException
    * [HBASE-10528] - DefaultBalancer selects plans to move regions onto draining nodes
    * [HBASE-11979] - Compaction progress reporting is wrong
    * [HBASE-12028] - Abort the RegionServer, when it's handler threads die
    * [HBASE-12070] - Add an option to hbck to fix ZK inconsistencies
    * [HBASE-12108] - HBaseConfiguration: set classloader before loading xml files
    * [HBASE-12267] - Replace HTable constructor in mapreduce.* classes with ConnectionFactory
    * [HBASE-12270] - A bug in the bucket cache, with cache blocks on write enabled
    * [HBASE-12339] - WAL performance evaluation tool doesn't roll logs
    * [HBASE-12348] - preModifyColumn and preDeleteColumn in AC denies user to perform its operation though it has required rights
    * [HBASE-12393] - The regionserver web will throw exception if we disable block cache
    * [HBASE-12422] - Use ConnectionFactory in HTable constructors
    * [HBASE-12431] - Use of getColumnLatestCell(byte[], int, int, byte[], int, int) is Not Thread Safe
    * [HBASE-12454] - Setting didPerformCompaction early in HRegion#compact
    * [HBASE-12467] - Master joins cluster but never completes initialization
    * [HBASE-12480] - Regions in FAILED_OPEN/FAILED_CLOSE should be processed on master failover
    * [HBASE-12564] - consolidate the getTableDescriptors() semantic
    * [HBASE-12565] - Race condition in HRegion.batchMutate()  causes partial data to be written when region closes
    * [HBASE-12574] - Update replication metrics to not do so many map look ups.
    * [HBASE-12585] - Fix refguide so it does hbase 1.0 style API everywhere with callout on how we used to do it in pre-1.0
    * [HBASE-12607] - TestHBaseFsck#testParallelHbck fails running against hadoop 2.6.0
    * [HBASE-12611] - Create autoCommit() method and remove clearBufferOnFail
    * [HBASE-12617] - Running IntegrationTestBigLinkedList against cluster getting not an instance of org.apache.hadoop.hbase.MiniHBaseCluster
    * [HBASE-12618] - Add 'Namespace' to headers while displaying user permissions.
    * [HBASE-12622] - user_permission should require global admin to display global and ns permissions
    * [HBASE-12632] - ThrottledInputStream/ExportSnapshot does not throttle
    * [HBASE-12634] -  Fix the AccessController#requireGlobalPermission(ns) with NS
    * [HBASE-12635] - Delete acl notify znode of table after the table is deleted
    * [HBASE-12637] - Compilation with Hadoop-2.4- is broken
    * [HBASE-12642] - LoadIncrementalHFiles does not throw exception after hitting hbase.bulkload.retries.number setting
    * [HBASE-12644] - Visibility Labels: issue with storing super users in labels table
    * [HBASE-12647] - Truncate table should work with C as well
    * [HBASE-12652] - Allow unmanaged connections in MetaTableAccessor
    * [HBASE-12655] - WALPerformanceEvaluation miscalculates append/sync statistics for multiple regions
    * [HBASE-12661] - rat check fails for several files
    * [HBASE-12662] - region_status.rb is failing with NoMethodError
    * [HBASE-12663] - unify getTableDescriptors() and listTableDescriptorsByNamespace()
    * [HBASE-12664] - TestDefaultLoadBalancer.testBalanceCluster fails in CachedDNSToSwitchMapping
    * [HBASE-12665] - When aborting, dump metrics
    * [HBASE-12666] - TestAssignmentManager hanging; add timeouts
    * [HBASE-12671] - HBASE-12652 broke branch-1 builds (TestAssignmentManager fails)
    * [HBASE-12674] - Add permission check to getNamespaceDescriptor()
    * [HBASE-12675] - Use interface methods in shell scripts
    * [HBASE-12681] - truncate_preserve command fails with undefined method `getTable' error
    * [HBASE-12683] - Compilation with hadoop-2.7.0-SNAPSHOT is broken
    * [HBASE-12686] - Failures in split before PONR not clearing the daughter regions from regions in transition during rollback
    * [HBASE-12692] - NPE from SnapshotManager#stop
    * [HBASE-12694] - testTableExistsIfTheSpecifiedTableRegionIsSplitParent in TestSplitTransactionOnCluster class leaves regions in transition
    * [HBASE-12696] - Possible NPE in HRegionFileSystem#splitStoreFile when skipStoreFileRangeCheck in splitPolicy return true
    * [HBASE-12697] - Don't use RegionLocationFinder if localityCost == 0
    * [HBASE-12699] - undefined method `setAsyncLogFlush' exception thrown when setting DEFERRED_LOG_FLUSH=>true
    * [HBASE-12711] - Fix new findbugs warnings in hbase-thrift module
    * [HBASE-12715] - getLastSequenceId always returns -1
    * [HBASE-12716] - A bug in RegionSplitter.UniformSplit algorithm
    * [HBASE-12718] - Convert TestAcidGuarantees from a unit test to an integration test
    * [HBASE-12728] - buffered writes substantially less useful after removal of HTablePool
    * [HBASE-12732] - Log messages in FileLink$FileLinkInputStream#tryOpen are reversed
    * [HBASE-12739] - Avoid too large identifier of ZooKeeperWatcher
    * [HBASE-12740] - Improve performance of TestHBaseFsck
    * [HBASE-12741] - AccessController contains a javadoc issue
    * [HBASE-12742] - ClusterStatusPublisher crashes with a IPv6 network interface.
    * [HBASE-12744] - hbase-default.xml lists hbase.regionserver.global.memstore.size twice
    * [HBASE-12746] - [1.0.0RC0] Distributed Log Replay is on (HBASE-12577 was insufficient)
    * [HBASE-12747] - IntegrationTestMTTR will OOME if launched with mvn verify
    * [HBASE-12750] - getRequestsCount() in ClusterStatus returns total number of request
    * [HBASE-12767] - Fix a StoreFileScanner NPE in reverse scan flow
    * [HBASE-12771] - TestFailFast#testFastFail failing
    * [HBASE-12774] - Fix the inconsistent permission checks for bulkloading.
    * [HBASE-12781] - thrift2 listen port will bind always to the passed command line address
    * [HBASE-12782] - ITBLL fails for me if generator does anything but 5M per maptask
    * [HBASE-12791] - HBase does not attempt to clean up an aborted split when the regionserver shutting down
    * [HBASE-12798] - Map Reduce jobs should not create Tables in setConf()
    * [HBASE-12801] - Failed to truncate a table while maintaing binary region boundaries
    * [HBASE-12804] - ImportTsv fails to delete partition files created by it
    * [HBASE-12810] - Update to htrace-incubating
    * [HBASE-12811] - [AccessController] NPE while scanning a table with user not having READ permission on the namespace
    * [HBASE-12817] - Data missing while scanning using PREFIX_TREE data block encoding
    * [HBASE-12819] - ExportSnapshot doesn't close FileSystem instances
    * [HBASE-12824] - CompressionTest fails with org.apache.hadoop.hbase.io.hfile.AbstractHFileReader$NotSeekedException: Not seeked to a key/value
    * [HBASE-12831] - Changing the set of vis labels a user has access to doesn't generate an audit log event
    * [HBASE-12832] - Describe table from shell no longer shows Table's attributes, only CF attributes
    * [HBASE-12833] - [shell] table.rb leaks connections
    * [HBASE-12835] - HBASE-12422 changed new HTable(Configuration) to not use managed Connections anymore
    * [HBASE-12837] - ReplicationAdmin leaks zk connections
    * [HBASE-12844] - ServerManager.isServerReacable() should sleep between retries
    * [HBASE-12847] - TestZKLessSplitOnCluster frequently times out in 0.98 builds
    * [HBASE-12862] - Uppercase "wals" in RegionServer webUI
    * [HBASE-12863] - Master info port on RS UI is always 0
    * [HBASE-12864] - IntegrationTestTableSnapshotInputFormat fails
    * [HBASE-12874] - LoadIncrementalHFiles should use unmanaged connection
    * [HBASE-12878] - Incorrect HFile path in TestHFilePerformance print output (fix for easier debugging)
    * [HBASE-12881] - TestFastFail is not compatible with surefire.rerunFailingTestsCount
    * [HBASE-12886] - Correct tag option name in PerformanceEvaluation
    * [HBASE-12892] - Add a class to allow taking a snapshot from the command line
    * [HBASE-12897] - Minimum memstore size is a percentage
    * [HBASE-12898] - Add in used undeclared dependencies
    * [HBASE-12901] - Possible deadlock while onlining a region and get region plan for other region run parallel
    * [HBASE-12904] - Threading issues in region_mover.rb
    * [HBASE-12915] - Disallow small scan with batching
    * [HBASE-12916] - No access control for replicating WAL entries
    * [HBASE-12917] - HFilePerformanceEvaluation Scan tests fail with StackOverflowError due to recursive call in createCell function
    * [HBASE-12918] - Backport asciidoc changes
    * [HBASE-12919] - Compilation with Hadoop-2.4- is broken again
    * [HBASE-12925] - Use acl cache for doing access control checks in prepare and clean phases of Bulkloading.
    * [HBASE-12942] - After disabling the hfile block cache, Regionserver UI is throwing java.lang.NullPointerException
    * [HBASE-12956] - Binding to 0.0.0.0 is broken after HBASE-10569
    * [HBASE-12958] - SSH doing hbase:meta get but hbase:meta not assigned
    * [HBASE-12962] - TestHFileBlockIndex.testBlockIndex() commented out during HBASE-10531
    * [HBASE-12966] - NPE in HMaster while recovering tables in Enabling state
    * [HBASE-12971] - Replication stuck due to large default value for replication.source.maxretriesmultiplier
    * [HBASE-12976] - Set default value for hbase.client.scanner.max.result.size
    * [HBASE-12978] - Region goes permanently offline (WAS: hbase:meta has a row missing hregioninfo and it causes my long-running job to fail)
    * [HBASE-12984] - SSL cannot be used by the InfoPort after removing deprecated code in HBASE-10336
    * [HBASE-12985] - Javadoc warning and findbugs fixes to get us green again
    * [HBASE-12989] - region_mover.rb unloadRegions method uses ArrayList concurrently resulting in errors
    * [HBASE-12991] - Use HBase 1.0 interfaces in hbase-rest
    * [HBASE-12996] - Reversed field on Filter should be transient
    * [HBASE-12998] - Compilation with Hdfs-2.7.0-SNAPSHOT is broken after HDFS-7647
    * [HBASE-13003] - Get tests in TestHFileBlockIndex back
    * [HBASE-13009] - HBase REST UI inaccessible
    * [HBASE-13010] - HFileOutputFormat2 partitioner's path is hard-coded as '/tmp'
    * [HBASE-13026] - Wrong error message in case incorrect snapshot name OR Incorrect table name
    * [HBASE-13027] - mapreduce.TableInputFormatBase should create its own Connection if needed
    * [HBASE-13028] - Cleanup mapreduce API changes
    * [HBASE-13030] - [1.0.0 polish] Make ScanMetrics public again and align Put 'add' with Get, Delete, etc., addColumn
    * [HBASE-13037] - LoadIncrementalHFile should try to verify the content of unmatched families
    * [HBASE-13038] - Fix the java doc warning continuously reported by Hadoop QA
    * [HBASE-13039] - Add patchprocess/* to .gitignore to fix builds of branches

** Improvement
    * [HBASE-5699] - Run with > 1 WAL in HRegionServer
    * [HBASE-11195] - Potentially improve block locality during major compaction for old regions
    * [HBASE-11412] - Minimize a number of hbase-client transitive dependencies
    * [HBASE-11639] - [Visibility controller] Replicate the visibility of Cells as strings
    * [HBASE-12071] - Separate out thread pool for Master <-> RegionServer communication
    * [HBASE-12121] - maven release plugin does not allow for customized goals
    * [HBASE-12204] - Backport HBASE-12016 'Reduce number of versions in Meta table. Make it configurable' to branch-1
    * [HBASE-12373] - Provide a command to list visibility labels
    * [HBASE-12583] - Allow creating reference files even the split row not lies in the storefile range if required
    * [HBASE-12597] - Add RpcClient interface and enable changing of RpcClient implementation
    * [HBASE-12608] - region_mover.rb does not log moving region count correctly when loading regions
    * [HBASE-12630] - Provide capability for dropping malfunctioning ConfigurationObserver automatically
    * [HBASE-12640] - Add Thrift-over-HTTPS and doAs support for Thrift Server
    * [HBASE-12641] - Grant all permissions of hbase zookeeper node to hbase superuser in a secure cluster
    * [HBASE-12651] - Backport HBASE-12559 'Provide LoadBalancer with online configuration capability' to branch-1
    * [HBASE-12653] - Move TestRegionServerOnlineConfigChange & TestConfigurationManager to Junit4 tests
    * [HBASE-12659] - Replace the method calls to grant and revoke in shell scripts with AccessControlClient
    * [HBASE-12668] - Adapt PayloadCarryingRpcController so it can also be used in async way
    * [HBASE-12676] - Fix the misleading ASCII art in IntegrationTestBigLinkedList
    * [HBASE-12678] - HBCK should print command line arguments
    * [HBASE-12680] - Refactor base ClusterManager in HBase to not have the notion of sending a signal.
    * [HBASE-12719] - Add test WAL provider to quantify FSHLog overhead in the absence of HDFS.
    * [HBASE-12720] - Make InternalScan LimitedPrivate
    * [HBASE-12736] - Let MetaScanner recycle a given Connection
    * [HBASE-12745] - Visibility Labels:  support visibility labels for user groups.
    * [HBASE-12762] - Region with no hfiles will have the highest locality cost in LocalityCostFunction
    * [HBASE-12768] - Support enable cache_data_on_write in Shell while creating table
    * [HBASE-12773] - Add warning message when user is trying to bulkload a large HFile.
    * [HBASE-12825] - CallRunner exception messages should include destination host:port
    * [HBASE-12893] - IntegrationTestBigLinkedListWithVisibility should use buffered writes
    * [HBASE-12899] - HBase should prefix htrace configuration keys with "hbase.htrace" rather than just "hbase."
    * [HBASE-12929] - TableMapReduceUtil.initTableMapperJob unnecessarily limits the types of outputKeyClass and outputValueClass
    * [HBASE-12973] - RegionCoprocessorEnvironment should provide HRegionInfo directly
    * [HBASE-12979] - Use setters instead of return values for handing back statistics from HRegion methods
    * [HBASE-12997] - FSHLog should print pipeline on low replication
    * [HBASE-13008] - Better default for hbase.regionserver.regionSplitLimit parameter.

** New Feature
    * [HBASE-9531] - a command line (hbase shell) interface to retreive the replication metrics and show replication lag
    * [HBASE-10560] - Per cell TTLs
    * [HBASE-12709] - [mvn] Add unit test excludes command line flag to the build.
    * [HBASE-12731] - Heap occupancy based client pushback

** Task
    * [HBASE-12493] - User class should provide a way to re-use existing token
    * [HBASE-12567] - Track remaining issues for HBase-1.0
    * [HBASE-12624] - Remove rename_snapshot.rb from code as there is no equivalent renameSnapshot api in Admin class
    * [HBASE-12625] - Deprecate certain methods in classes annotated with InterfaceAudience.Public in branch-1
    * [HBASE-12689] - Move version on from 0.99.2 to HBASE-1.0.0-SNAPSHOT
    * [HBASE-12724] - Upgrade the interface audience of RegionScanner from Private to LimitedPrivate
    * [HBASE-12726] - Backport to branch-1.0 addendum for "Minimize a number of hbase-client transitive dependencies"
    * [HBASE-12788] - Promote Abortable to LimitedPrivate
    * [HBASE-12834] - Promote ScanType to LimitedPrivate

** Test
    * [HBASE-12163] - Move test annotation classes to the same package as in master
    * [HBASE-12645] - HBaseTestingUtility is using ${$HOME} for rootDir
    * [HBASE-12685] - TestSplitLogManager#testLogFilesAreArchived sometimes times out due to race condition
    * [HBASE-12799] - ITAG fails with java.lang.RuntimeException if table does not exist
    * [HBASE-12885] - Unit test for RAW / VERSIONS scanner specifications

Release Notes - HBase - Version 0.99.2 12/07/2014

** Sub-task
    * [HBASE-10671] - Add missing InterfaceAudience annotations for classes in hbase-common and hbase-client modules
    * [HBASE-11164] - Document and test rolling updates from 0.98 -> 1.0
    * [HBASE-11915] - Document and test 0.94 -> 1.0.0 update
    * [HBASE-11964] - Improve spreading replication load from failed regionservers
    * [HBASE-12075] - Preemptive Fast Fail
    * [HBASE-12128] - Cache configuration and RpcController selection for Table in Connection
    * [HBASE-12147] - Porting Online Config Change from 89-fb
    * [HBASE-12202] - Support DirectByteBuffer usage in HFileBlock
    * [HBASE-12214] - Visibility Controller in the peer cluster should be able to extract visibility tags from the replicated cells
    * [HBASE-12288] - Support DirectByteBuffer usage in DataBlock Encoding area
    * [HBASE-12297] - Support DBB usage in Bloom and HFileIndex area
    * [HBASE-12313] - Redo the hfile index length optimization so cell-based rather than serialized KV key
    * [HBASE-12353] - Turn down logging on some spewing unit tests
    * [HBASE-12354] - Update dependencies in time for 1.0 release
    * [HBASE-12355] - Update maven plugins
    * [HBASE-12363] - Improve how KEEP_DELETED_CELLS works with MIN_VERSIONS
    * [HBASE-12379] - Try surefire 2.18-SNAPSHOT
    * [HBASE-12400] - Fix refguide so it does connection#getTable rather than new HTable everywhere: first cut!
    * [HBASE-12404] - Task 5 from parent: Replace internal HTable constructor use with HConnection#getTable (0.98, 0.99)
    * [HBASE-12471] - Task 4. replace internal ConnectionManager#{delete,get}Connection use with #close, #createConnection (0.98, 0.99) under src/main/java
    * [HBASE-12517] - Several HConstant members are assignable
    * [HBASE-12518] - Task 4 polish. Remove CM#{get,delete}Connection
    * [HBASE-12519] - Remove tabs used as whitespace
    * [HBASE-12526] - Remove unused imports
    * [HBASE-12577] - Disable distributed log replay by default



** Bug
    * [HBASE-7211] - Improve hbase ref guide for the testing part.
    * [HBASE-9003] - TableMapReduceUtil should not rely on org.apache.hadoop.util.JarFinder#getJar
    * [HBASE-9117] - Remove HTablePool and all HConnection pooling related APIs
    * [HBASE-9157] - ZKUtil.blockUntilAvailable loops forever with non-recoverable errors
    * [HBASE-9527] - Review all old api that takes a table name as a byte array and ensure none can pass ns + tablename
    * [HBASE-10536] - ImportTsv should fail fast if any of the column family passed to the job is not present in the table
    * [HBASE-10780] - HFilePrettyPrinter#processFile should return immediately if file does not exist
    * [HBASE-11099] - Two situations where we could open a region with smaller sequence number
    * [HBASE-11562] - CopyTable should provide an option to shuffle the mapper tasks
    * [HBASE-11835] - Wrong managenement of non expected calls in the client
    * [HBASE-12017] - Use Connection.createTable() instead of HTable constructors.
    * [HBASE-12029] - Use Table and RegionLocator in HTable.getRegionLocations()
    * [HBASE-12053] - SecurityBulkLoadEndPoint set 777 permission on input data files
    * [HBASE-12072] - Standardize retry handling for master operations
    * [HBASE-12083] - Deprecate new HBaseAdmin() in favor of Connection.getAdmin()
    * [HBASE-12142] - Truncate command does not preserve ACLs table
    * [HBASE-12194] - Make TestEncodedSeekers faster
    * [HBASE-12219] - Cache more efficiently getAll() and get() in FSTableDescriptors
    * [HBASE-12226] - TestAccessController#testPermissionList failing on master
    * [HBASE-12229] - NullPointerException in SnapshotTestingUtils
    * [HBASE-12234] - Make TestMultithreadedTableMapper a little more stable.
    * [HBASE-12237] - HBaseZeroCopyByteString#wrap() should not be called in hbase-client code
    * [HBASE-12238] - A few ugly exceptions on startup
    * [HBASE-12240] - hbase-daemon.sh should remove pid file if process not found running
    * [HBASE-12241] - The crash of regionServer when taking deadserver's replication queue breaks replication
    * [HBASE-12242] - Fix new javadoc warnings in Admin, etc.
    * [HBASE-12246] - Compilation with hadoop-2.3.x and 2.2.x is broken
    * [HBASE-12247] - Replace setHTable() with initializeTable() in TableInputFormat.
    * [HBASE-12248] - broken link in hbase shell help
    * [HBASE-12252] - IntegrationTestBulkLoad fails with illegal partition error
    * [HBASE-12257] - TestAssignmentManager unsynchronized access to regionPlans
    * [HBASE-12258] - Make TestHBaseFsck less flaky
    * [HBASE-12261] - Add checkstyle to HBase build process
    * [HBASE-12263] - RegionServer listens on localhost in distributed cluster when DNS is unavailable
    * [HBASE-12265] - HBase shell 'show_filters' points to internal Facebook URL
    * [HBASE-12274] - Race between RegionScannerImpl#nextInternal() and RegionScannerImpl#close() may produce null pointer exception
    * [HBASE-12277] - Refactor bulkLoad methods in AccessController to its own interface
    * [HBASE-12278] - Race condition in TestSecureLoadIncrementalHFilesSplitRecovery
    * [HBASE-12279] - Generated thrift files were generated with the wrong parameters
    * [HBASE-12281] - ClonedPrefixTreeCell should implement HeapSize
    * [HBASE-12285] - Builds are failing, possibly because of SUREFIRE-1091
    * [HBASE-12294] - Can't build the docs after the hbase-checkstyle module was added
    * [HBASE-12301] - user_permission command does not show global permissions
    * [HBASE-12302] - VisibilityClient getAuths does not propagate remote service exception correctly
    * [HBASE-12304] - CellCounter will throw AIOBE when output directory is not specified
    * [HBASE-12306] - CellCounter output's wrong value for Total Families Across all Rows in output file
    * [HBASE-12308] - Fix typo in hbase-rest profile name
    * [HBASE-12312] - Another couple of createTable race conditions
    * [HBASE-12314] - Add chaos monkey policy to execute two actions concurrently
    * [HBASE-12315] - Fix 0.98 Tests after checkstyle got parented
    * [HBASE-12316] - test-patch.sh (Hadoop-QA) outputs the wrong release audit warnings URL
    * [HBASE-12318] - Add license header to checkstyle xml files
    * [HBASE-12319] - Inconsistencies during region recovery due to close/open of a region during recovery
    * [HBASE-12322] - Add clean up command to ITBLL
    * [HBASE-12327] - MetricsHBaseServerSourceFactory#createContextName has wrong conditions
    * [HBASE-12329] - Table create with duplicate column family names quietly succeeds
    * [HBASE-12334] - Handling of DeserializationException causes needless retry on failure
    * [HBASE-12336] - RegionServer failed to shutdown for NodeFailoverWorker thread
    * [HBASE-12337] - Import tool fails with NullPointerException if clusterIds is not initialized
    * [HBASE-12346] - Scan's default auths behavior under Visibility labels
    * [HBASE-12352] - Add hbase-annotation-tests to runtime classpath so can run hbase it tests.
    * [HBASE-12356] - Rpc with region replica does not propagate tracing spans
    * [HBASE-12359] - MulticastPublisher should specify IPv4/v6 protocol family when creating multicast channel
    * [HBASE-12366] - Add login code to HBase Canary tool.
    * [HBASE-12372] - [WINDOWS] Enable log4j configuration in hbase.cmd
    * [HBASE-12375] - LoadIncrementalHFiles fails to load data in table when CF name starts with '_'
    * [HBASE-12377] - HBaseAdmin#deleteTable fails when META region is moved around the same timeframe
    * [HBASE-12384] - TestTags can hang on fast test hosts
    * [HBASE-12386] - Replication gets stuck following a transient zookeeper error to remote peer cluster
    * [HBASE-12398] - Region isn't assigned in an extreme race condition
    * [HBASE-12399] - Master startup race between metrics and RpcServer
    * [HBASE-12402] - ZKPermissionWatcher race condition in refreshing the cache leaving stale ACLs and causing AccessDenied
    * [HBASE-12407] - HConnectionKey doesn't contain CUSTOM_CONTROLLER_CONF_KEY in CONNECTION_PROPERTIES
    * [HBASE-12414] - Move HFileLink.exists() to base class
    * [HBASE-12417] - Scan copy constructor does not retain small attribute
    * [HBASE-12419] - "Partial cell read caused by EOF" ERRORs on replication source during replication
    * [HBASE-12420] - BucketCache logged startup message is egregiously large
    * [HBASE-12423] - Use a non-managed Table in TableOutputFormat
    * [HBASE-12428] - region_mover.rb script is broken if port is not specified
    * [HBASE-12440] - Region may remain offline on clean startup under certain race condition
    * [HBASE-12445] - hbase is removing all remaining cells immediately after the cell marked with marker = KeyValue.Type.DeleteColumn via PUT
    * [HBASE-12448] - Fix rate reporting in compaction progress DEBUG logging
    * [HBASE-12449] - Use the max timestamp of current or old cell's timestamp in HRegion.append()
    * [HBASE-12450] - Unbalance chaos monkey might kill all region servers without starting them back
    * [HBASE-12459] - Use a non-managed Table in mapred.TableOutputFormat
    * [HBASE-12460] - Moving Chore to hbase-common module.
    * [HBASE-12461] - FSVisitor logging is excessive
    * [HBASE-12464] - meta table region assignment stuck in the FAILED_OPEN state due to region server not fully ready to serve
    * [HBASE-12478] - HBASE-10141 and MIN_VERSIONS are not compatible
    * [HBASE-12479] - Backport HBASE-11689 (Track meta in transition) to 0.98 and branch-1
    * [HBASE-12490] - Replace uses of setAutoFlush(boolean, boolean)
    * [HBASE-12491] - TableMapReduceUtil.findContainingJar() NPE
    * [HBASE-12495] - Use interfaces in the shell scripts
    * [HBASE-12513] - Graceful stop script does not restore the balancer state
    * [HBASE-12514] - Cleanup HFileOutputFormat legacy code
    * [HBASE-12520] - Add protected getters on TableInputFormatBase
    * [HBASE-12533] - staging directories are not deleted after secure bulk load
    * [HBASE-12536] - Reduce the effective scope of GLOBAL CREATE and ADMIN permission
    * [HBASE-12537] - HBase should log the remote host on replication error
    * [HBASE-12539] - HFileLinkCleaner logs are uselessly noisy
    * [HBASE-12541] - Add misc debug logging to hanging tests in TestHCM and TestBaseLoadBalancer
    * [HBASE-12544] - ops_mgt.xml missing in branch-1
    * [HBASE-12550] - Check all storefiles are referenced before splitting
    * [HBASE-12560] - [WINDOWS] Append the classpath from Hadoop to HBase classpath in bin/hbase.cmd
    * [HBASE-12576] - Add metrics for rolling the HLog if there are too few DN's in the write pipeline
    * [HBASE-12580] - Zookeeper instantiated even though we might not need it in the shell
    * [HBASE-12581] - TestCellACLWithMultipleVersions failing since task 5 HBASE-12404 (HBASE-12404 addendum)
    * [HBASE-12584] - Fix branch-1 failing since task 5 HBASE-12404 (HBASE-12404 addendum)
    * [HBASE-12595] - Use Connection.getTable() in table.rb
    * [HBASE-12600] - Remove REPLAY tag dependency in Distributed Replay Mode
    * [HBASE-12610] - Close connection in TableInputFormatBase
    * [HBASE-12611] - Create autoCommit() method and remove clearBufferOnFail
    * [HBASE-12614] - Potentially unclosed StoreFile(s) in DefaultCompactor#compact()
    * [HBASE-12616] - We lost the IntegrationTestBigLinkedList COMMANDS in recent usage refactoring




** Improvement
    * [HBASE-2609] - Harmonize the Get and Delete operations
    * [HBASE-4955] - Use the official versions of surefire & junit
    * [HBASE-8361] - Bulk load and other utilities should not create tables for user
    * [HBASE-8572] - Enhance delete_snapshot.rb to call snapshot deletion API with regex
    * [HBASE-10082] - Describe 'table' output is all on one line, could use better formatting
    * [HBASE-10483] - Provide API for retrieving info port when hbase.master.info.port is set to 0
    * [HBASE-11639] - [Visibility controller] Replicate the visibility of Cells as strings
    * [HBASE-11870] - Optimization : Avoid copy of key and value for tags addition in AC and VC
    * [HBASE-12161] - Add support for grant/revoke on namespaces in AccessControlClient
    * [HBASE-12243] - HBaseFsck should auto set ignorePreCheckPermission to true if no fix option is set.
    * [HBASE-12249] - Script to help you adhere to the patch-naming guidelines
    * [HBASE-12264] - ImportTsv should fail fast if output is not specified and table does not exist
    * [HBASE-12271] - Add counters for files skipped during snapshot export
    * [HBASE-12272] - Generate Thrift code through maven
    * [HBASE-12328] - Need to separate JvmMetrics for Master and RegionServer
    * [HBASE-12389] - Reduce the number of versions configured for the ACL table
    * [HBASE-12390] - Change revision style from svn to git
    * [HBASE-12411] - Optionally enable p-reads and private readers for compactions
    * [HBASE-12416] - RegionServerCallable should report what host it was communicating with
    * [HBASE-12424] - Finer grained logging and metrics for split transactions
    * [HBASE-12432] - RpcRetryingCaller should log after fixed number of retries like AsyncProcess
    * [HBASE-12434] - Add a command to compact all the regions in a regionserver
    * [HBASE-12447] - Add support for setTimeRange for RowCounter and CellCounter
    * [HBASE-12455] - Add 'description' to bean and attribute output when you do /jmx?description=true
    * [HBASE-12529] - Use ThreadLocalRandom for RandomQueueBalancer
    * [HBASE-12569] - Control MaxDirectMemorySize in the same manner as heap size

** New Feature
    * [HBASE-8707] - Add LongComparator for filter
    * [HBASE-12286] - [shell] Add server/cluster online load of configuration changes
    * [HBASE-12361] - Show data locality of region in table page
    * [HBASE-12496] - A blockedRequestsCount metric








** Task
    * [HBASE-10200] - Better error message when HttpServer fails to start due to java.net.BindException
    * [HBASE-10870] - Deprecate and replace HCD methods that have a 'should' prefix with a 'get' instead
    * [HBASE-12250] - Adding an endpoint for updating the regionserver config
    * [HBASE-12344] - Split up TestAdmin
    * [HBASE-12381] - Add maven enforcer rules for build assumptions
    * [HBASE-12388] - Document that WALObservers don't get empty edits.
    * [HBASE-12427] - Change branch-1 version from 0.99.2-SNAPSHOT to 0.99.3-SNAPSHOT
    * [HBASE-12442] - Bring KeyValue#createFirstOnRow() back to branch-1 as deprecated methods
    * [HBASE-12456] - Update surefire from 2.18-SNAPSHOT to 2.18
    * [HBASE-12516] - Clean up master so QA Bot is in known good state
    * [HBASE-12522] - Backport WAL refactoring to branch-1


** Test
    * [HBASE-12317] - Run IntegrationTestRegionReplicaPerf w.o mapred
    * [HBASE-12335] - IntegrationTestRegionReplicaPerf is flaky
    * [HBASE-12367] - Integration tests should not restore the cluster if the CM is not destructive
    * [HBASE-12378] - Add a test to verify that the read-replica is able to read after a compaction
    * [HBASE-12401] - Add some timestamp signposts in IntegrationTestMTTR
    * [HBASE-12403] - IntegrationTestMTTR flaky due to aggressive RS restart timeout
    * [HBASE-12472] - Improve debuggability of IntegrationTestBulkLoad
    * [HBASE-12549] - Fix TestAssignmentManagerOnCluster#testAssignRacingWithSSH() flaky test
    * [HBASE-12554] - TestBaseLoadBalancer may timeout due to lengthy rack lookup

** Umbrella
    * [HBASE-10602] - Cleanup HTable public interface
    * [HBASE-10856] - Prep for 1.0



Release Notes - HBase - Version 0.99.1 10/15/2014

** Sub-task
    * [HBASE-11160] - Undo append waiting on region edit/sequence id update
    * [HBASE-11178] - Remove deprecation annotations from mapred namespace
    * [HBASE-11738] - Document improvements to LoadTestTool and PerformanceEvaluation
    * [HBASE-11872] - Avoid usage of KeyValueUtil#ensureKeyValue from Compactor
    * [HBASE-11874] - Support Cell to be passed to StoreFile.Writer rather than KeyValue
    * [HBASE-11917] - Deprecate / Remove HTableUtil
    * [HBASE-11920] - Add CP hooks for ReplicationEndPoint
    * [HBASE-11930] - Document new permission check to roll WAL writer
    * [HBASE-11980] - Change sync to hsync, remove unused InfoServer, and reference our httpserver instead of hadoops
    * [HBASE-11997] - CopyTable with bulkload
    * [HBASE-12023] - HRegion.applyFamilyMapToMemstore creates too many iterator objects.
    * [HBASE-12046] - HTD/HCD setters should be builder-style
    * [HBASE-12047] - Avoid usage of KeyValueUtil#ensureKeyValue in simple cases
    * [HBASE-12050] - Avoid KeyValueUtil#ensureKeyValue from DefaultMemStore
    * [HBASE-12051] - Avoid KeyValueUtil#ensureKeyValue from DefaultMemStore
    * [HBASE-12059] - Create hbase-annotations module
    * [HBASE-12062] - Fix usage of Collections.toArray
    * [HBASE-12068] - [Branch-1] Avoid need to always do KeyValueUtil#ensureKeyValue for Filter transformCell
    * [HBASE-12069] - Finish making HFile.Writer Cell-centric; undo APIs that expect KV serializations.
    * [HBASE-12076] - Move InterfaceAudience imports to hbase-annotations
    * [HBASE-12077] - FilterLists create many ArrayList$Itr objects per row.
    * [HBASE-12079] - Deprecate KeyValueUtil#ensureKeyValue(s)
    * [HBASE-12082] - Find a way to set timestamp on Cells on the server
    * [HBASE-12086] - Fix bugs in HTableMultiplexer
    * [HBASE-12096] - In ZKSplitLog Coordination and AggregateImplementation replace enhaced for statements with basic for statement to avoid unnecessary object allocation
    * [HBASE-12104] - Some optimization and bugfix for HTableMultiplexer
    * [HBASE-12110] - Fix .arcconfig
    * [HBASE-12112] - Avoid KeyValueUtil#ensureKeyValue some more simple cases
    * [HBASE-12115] - Fix NumberFormat Exception in TableInputFormatBase.
    * [HBASE-12189] - Fix new issues found by coverity static analysis
    * [HBASE-12210] - Avoid KeyValue in Prefix Tree

** Bug
    * [HBASE-6994] - minor doc update about DEFAULT_ACCEPTABLE_FACTOR
    * [HBASE-8808] - Use Jacoco to generate Unit Test coverage reports
    * [HBASE-8936] - Fixing TestSplitLogWorker while running Jacoco tests.
    * [HBASE-9005] - Improve documentation around KEEP_DELETED_CELLS, time range scans, and delete markers
    * [HBASE-9513] - Why is PE#RandomSeekScanTest way slower in 0.96 than in 0.94?
    * [HBASE-10314] - Add Chaos Monkey that doesn't touch the master
    * [HBASE-10748] - hbase-daemon.sh fails to execute with 'sh' command
    * [HBASE-10757] - Change HTable class doc so it sends people to HCM getting instances
    * [HBASE-11145] - UNEXPECTED!!! when HLog sync: Queue full
    * [HBASE-11266] - Remove shaded references to logger
    * [HBASE-11394] - Replication can have data loss if peer id contains hyphen "-"
    * [HBASE-11401] - Late-binding sequenceid presumes a particular KeyValue mvcc format hampering experiment
    * [HBASE-11405] - Multiple invocations of hbck in parallel disables balancer permanently
    * [HBASE-11804] - Raise default heap size if unspecified
    * [HBASE-11815] - Flush and compaction could just close the tmp writer if there is an exception
    * [HBASE-11890] - HBase REST Client is hard coded to http protocol
    * [HBASE-11906] - Meta data loss with distributed log replay
    * [HBASE-11967] - HMaster in standalone won't go down if it gets 'Unhandled exception'
    * [HBASE-11974] - When a disabled table is scanned, NotServingRegionException is thrown instead of TableNotEnabledException
    * [HBASE-11982] - Bootstraping hbase:meta table creates a WAL file in region dir
    * [HBASE-11988] - AC/VC system table create on postStartMaster fails too often in test
    * [HBASE-11991] - Region states may be out of sync
    * [HBASE-11994] - PutCombiner floods the M/R log with repeated log messages.
    * [HBASE-12007] - StochasticBalancer should avoid putting user regions on master
    * [HBASE-12019] - hbase-daemon.sh overwrite HBASE_ROOT_LOGGER and HBASE_SECURITY_LOGGER variables
    * [HBASE-12024] - Fix javadoc warning
    * [HBASE-12025] - TestHttpServerLifecycle.testStartedServerWithRequestLog hangs frequently
    * [HBASE-12034] - If I kill single RS in branch-1, all regions end up on Master!
    * [HBASE-12038] - Replace internal uses of signatures with byte[] and String tableNames to use the TableName equivalents.
    * [HBASE-12041] - AssertionError in HFilePerformanceEvaluation.UniformRandomReadBenchmark
    * [HBASE-12042] - Replace internal uses of HTable(Configuration, String) with HTable(Configuration, TableName)
    * [HBASE-12043] - REST server should respond with FORBIDDEN(403) code on AccessDeniedException
    * [HBASE-12044] - REST delete operation should not retry disableTable for DoNotRetryIOException
    * [HBASE-12045] - REST proxy users configuration in hbase-site.xml is ignored
    * [HBASE-12052] - BulkLoad Failed due to no write permission on input files
    * [HBASE-12054] - bad state after NamespaceUpgrade with reserved table names
    * [HBASE-12056] - RPC logging too much in DEBUG mode
    * [HBASE-12064] - hbase.master.balancer.stochastic.numRegionLoadsToRemember is not used
    * [HBASE-12065] -  Import tool is not restoring multiple DeleteFamily markers of a row
    * [HBASE-12067] - Remove deprecated metrics classes.
    * [HBASE-12078] - Missing Data when scanning using PREFIX_TREE DATA-BLOCK-ENCODING
    * [HBASE-12095] - SecureWALCellCodec should handle the case where encryption is disabled
    * [HBASE-12098] - User granted namespace table create permissions can't create a table
    * [HBASE-12099] - TestScannerModel fails if using jackson 1.9.13
    * [HBASE-12106] - Move test annotations to test artifact
    * [HBASE-12109] - user_permission command for namespace does not return correct result
    * [HBASE-12119] - Master regionserver web UI NOT_FOUND
    * [HBASE-12120] - HBase shell doesn't allow deleting of a cell by user with W-only permissions to it
    * [HBASE-12122] - Try not to assign user regions to master all the time
    * [HBASE-12123] - Failed assertion in BucketCache after 11331
    * [HBASE-12124] - Closed region could stay closed if master stops at bad time
    * [HBASE-12126] - Region server coprocessor endpoint
    * [HBASE-12130] - HBASE-11980 calls hflush and hsync doing near double the syncing work
    * [HBASE-12134] - publish_website.sh script is too optimistic
    * [HBASE-12135] - Website is broken
    * [HBASE-12136] - Race condition between client adding tableCF replication znode and  server triggering TableCFsTracker
    * [HBASE-12137] - Alter table add cf doesn't do compression test
    * [HBASE-12139] - StochasticLoadBalancer doesn't work on large lightly loaded clusters
    * [HBASE-12140] - Add ConnectionFactory.createConnection() to create using default HBaseConfiguration.
    * [HBASE-12145] - Fix javadoc and findbugs so new folks aren't freaked when they see them
    * [HBASE-12146] - RegionServerTracker should escape data in log messages
    * [HBASE-12149] - TestRegionPlacement is failing undeterministically
    * [HBASE-12151] - Make dev scripts executable
    * [HBASE-12153] - Fixing TestReplicaWithCluster
    * [HBASE-12156] - TableName cache isn't used for one of valueOf methods.
    * [HBASE-12158] - TestHttpServerLifecycle.testStartedServerWithRequestLog goes zombie on occasion
    * [HBASE-12160] - Make Surefire's argLine configurable in the command line
    * [HBASE-12164] - Check for presence of user Id in SecureBulkLoadEndpoint#secureBulkLoadHFiles() is inaccurate
    * [HBASE-12165] - TestEndToEndSplitTransaction.testFromClientSideWhileSplitting fails
    * [HBASE-12166] - TestDistributedLogSplitting.testMasterStartsUpWithLogReplayWork
    * [HBASE-12167] - NPE in AssignmentManager
    * [HBASE-12170] - TestReplicaWithCluster.testReplicaAndReplication timeouts
    * [HBASE-12181] - Some tests create a table and try to use it before regions get assigned
    * [HBASE-12183] - FuzzyRowFilter doesn't support reverse scans
    * [HBASE-12184] - ServerShutdownHandler throws NPE
    * [HBASE-12191] - Make TestCacheOnWrite faster.
    * [HBASE-12196] - SSH should retry in case failed to assign regions
    * [HBASE-12197] - Move REST
    * [HBASE-12198] - Fix the bug of not updating location cache
    * [HBASE-12199] - Make TestAtomicOperation and TestEncodedSeekers faster
    * [HBASE-12200] - When an RPC server handler thread dies, throw exception
    * [HBASE-12206] - NPE in RSRpcServices
    * [HBASE-12209] - NPE in HRegionServer#getLastSequenceId
    * [HBASE-12218] - Make HBaseCommonTestingUtil#deleteDir try harder

** Improvement
    * [HBASE-10153] - improve VerifyReplication to compute BADROWS more accurately
    * [HBASE-10411] - [Book] Add a kerberos 'request is a replay (34)' issue at troubleshooting section
    * [HBASE-11796] - Add client support for atomic checkAndMutate
    * [HBASE-11879] - Change TableInputFormatBase to take interface arguments
    * [HBASE-11907] - Use the joni byte[] regex engine in place of j.u.regex in RegexStringComparator
    * [HBASE-11948] - graceful_stop.sh should use hbase-daemon.sh when executed on the decomissioned node
    * [HBASE-12010] - Use TableName.META_TABLE_NAME instead of indirectly from HTableDescriptor
    * [HBASE-12011] - Add namespace column during display of user tables
    * [HBASE-12013] - Make region_mover.rb support multiple regionservers per host
    * [HBASE-12021] - Hbase shell does not respect the HBASE_OPTS set by the user in console
    * [HBASE-12032] - Script to stop regionservers via RPC
    * [HBASE-12049] - Help for alter command is a bit confusing
    * [HBASE-12090] - Bytes: more Unsafe, more Faster
    * [HBASE-12118] - Explain how to grant permission to a namespace in grant command usage
    * [HBASE-12176] - WALCellCodec Encoders support for non-KeyValue Cells
    * [HBASE-12212] - HBaseTestingUtility#waitUntilAllRegionsAssigned should wait for RegionStates
    * [HBASE-12220] - Add hedgedReads and hedgedReadWins metrics

** New Feature
    * [HBASE-11990] - Make setting the start and stop row for a specific prefix easier
    * [HBASE-11995] - Use Connection and ConnectionFactory where possible
    * [HBASE-12127] - Move the core Connection creation functionality into ConnectionFactory
    * [HBASE-12133] - Add FastLongHistogram for metric computation
    * [HBASE-12143] - Minor fix for Table code

** Task
    * [HBASE-9004] - Fix Documentation around Minor compaction and ttl
    * [HBASE-11692] - Document how and why to do a manual region split
    * [HBASE-11730] - Document release managers for non-deprecated branches
    * [HBASE-11761] - Add a FAQ item for updating a maven-managed application from 0.94 -> 0.96+
    * [HBASE-11960] - Provide a sample to show how to use Thrift client authentication
    * [HBASE-11978] - Backport 'HBASE-7767 Get rid of ZKTable, and table enable/disable state in ZK' to 1.0
    * [HBASE-11981] - Document how to find the units of measure for a given HBase metric

** Test
    * [HBASE-11798] - TestBucketWriterThread may hang due to WriterThread stopping prematurely
    * [HBASE-11838] - Enable PREFIX_TREE in integration tests
    * [HBASE-12008] - Remove IntegrationTestImportTsv#testRunFromOutputCommitter
    * [HBASE-12055] - TestBucketWriterThread hangs flakily based on timing


Release Notes - HBase - Version 0.99.0 9/22/2014

** Sub-task
    * [HBASE-2251] - PE defaults to 1k rows - uncommon use case, and easy to hit benchmarks
    * [HBASE-5175] - Add DoubleColumnInterpreter
    * [HBASE-6873] - Clean up Coprocessor loading failure handling
    * [HBASE-8541] - implement flush-into-stripes in stripe compactions
    * [HBASE-9149] - javadoc cleanup of to reflect .META. rename to hbase:meta
    * [HBASE-9261] - Add cp hooks after {start|close}RegionOperation
    * [HBASE-9489] - Add cp hooks in online merge before and after setting PONR
    * [HBASE-9846] - Integration test and LoadTestTool support for cell ACLs
    * [HBASE-9858] - Integration test and LoadTestTool support for cell Visibility
    * [HBASE-9889] - Make sure we clean up scannerReadPoints upon any exceptions
    * [HBASE-9941] - The context ClassLoader isn't set while calling into a coprocessor
    * [HBASE-9966] - Create IntegrationTest for Online Bloom Filter Change
    * [HBASE-9977] - Define C interface of HBase Client Asynchronous APIs
    * [HBASE-10043] - Fix Potential Resouce Leak in MultiTableInputFormatBase
    * [HBASE-10094] - Add batching to HLogPerformanceEvaluation
    * [HBASE-10110] - Fix Potential Resource Leak in StoreFlusher
    * [HBASE-10124] - Make Sub Classes Static When Possible
    * [HBASE-10143] - Clean up dead local stores in FSUtils
    * [HBASE-10150] - Write attachment Id of tested patch into JIRA comment
    * [HBASE-10156] - FSHLog Refactor (WAS -> Fix up the HBASE-8755 slowdown when low contention)
    * [HBASE-10158] - Add sync rate histogram to HLogPE
    * [HBASE-10169] - Batch coprocessor
    * [HBASE-10297] - LoadAndVerify Integration Test for cell visibility
    * [HBASE-10347] - HRegionInfo changes for adding replicaId and MetaEditor/MetaReader changes for region replicas
    * [HBASE-10348] - HTableDescriptor changes for region replicas
    * [HBASE-10350] - Master/AM/RegionStates changes to create and assign region replicas
    * [HBASE-10351] - LoadBalancer changes for supporting region replicas
    * [HBASE-10352] - Region and RegionServer changes for opening region replicas, and refreshing store files
    * [HBASE-10354] - Add an API for defining consistency per request
    * [HBASE-10355] - Failover RPC's from client using region replicas
    * [HBASE-10356] - Failover RPC's for multi-get
    * [HBASE-10357] - Failover RPC's for scans
    * [HBASE-10359] - Master/RS WebUI changes for region replicas
    * [HBASE-10361] - Enable/AlterTable support for region replicas
    * [HBASE-10362] - HBCK changes for supporting region replicas
    * [HBASE-10391] - Deprecate KeyValue#getBuffer
    * [HBASE-10420] - Replace KV.getBuffer with KV.get{Row|Family|Qualifier|Value|Tags}Array
    * [HBASE-10513] - Provide user documentation for region replicas
    * [HBASE-10517] - NPE in MetaCache.clearCache()
    * [HBASE-10519] - Add handling for swallowed InterruptedException thrown by Thread.sleep in rest related files
    * [HBASE-10520] - Add handling for swallowed InterruptedException thrown by Thread.sleep in MiniZooKeeperCluster
    * [HBASE-10521] - Add handling for swallowed InterruptedException thrown by Thread.sleep in RpcServer
    * [HBASE-10522] - Correct wrong handling and add proper handling for swallowed InterruptedException thrown by Thread.sleep in client
    * [HBASE-10523] - Correct wrong handling and add proper handling for swallowed InterruptedException thrown by Thread.sleep in util
    * [HBASE-10524] - Correct wrong handling and add proper handling for swallowed InterruptedException thrown by Thread.sleep in regionserver
    * [HBASE-10526] - Using Cell instead of KeyValue in HFileOutputFormat
    * [HBASE-10529] - Make Cell extend Cloneable
    * [HBASE-10530] - Add util methods in CellUtil
    * [HBASE-10531] - Revisit how the key byte[] is passed to HFileScanner.seekTo and reseekTo
    * [HBASE-10532] - Make KeyValueComparator in KeyValue to accept Cell instead of KeyValue.
    * [HBASE-10550] - Register HBase tokens with ServiceLoader
    * [HBASE-10561] - Forward port: HBASE-10212 New rpc metric: number of active handler
    * [HBASE-10572] - Create an IntegrationTest for region replicas
    * [HBASE-10573] - Use Netty 4
    * [HBASE-10616] - Integration test for multi-get calls
    * [HBASE-10620] - LoadBalancer.needsBalance() should check for co-located region replicas as well
    * [HBASE-10630] - NullPointerException in ConnectionManager$HConnectionImplementation.locateRegionInMeta() due to missing region info
    * [HBASE-10633] - StoreFileRefresherChore throws ConcurrentModificationException sometimes
    * [HBASE-10634] - Multiget doesn't fully work
    * [HBASE-10648] - Pluggable Memstore
    * [HBASE-10650] - Fix incorrect handling of IE that restores current thread's interrupt status within while/for loops in RegionServer
    * [HBASE-10651] - Fix incorrect handling of IE that restores current thread's interrupt status within while/for loops in Replication
    * [HBASE-10652] - Fix incorrect handling of IE that restores current thread's interrupt status within while/for loops in rpc
    * [HBASE-10661] - TestStochasticLoadBalancer.testRegionReplicationOnMidClusterWithRacks() is flaky
    * [HBASE-10672] - Table snapshot should handle tables whose REGION_REPLICATION is greater than one
    * [HBASE-10680] - Check if the block keys, index keys can be used as Cells instead of byte[]
    * [HBASE-10688] - Add a draining_node script to manage nodes in draining mode
    * [HBASE-10691] - test-patch.sh should continue even if compilation against hadoop 1.0 / 1.1 fails
    * [HBASE-10697] - Convert TestSimpleTotalOrderPartitioner to junit4 test
    * [HBASE-10701] - Cache invalidation improvements from client side
    * [HBASE-10704] - BaseLoadBalancer#roundRobinAssignment() may add same region to assignment plan multiple times
    * [HBASE-10717] - TestFSHDFSUtils#testIsSameHdfs fails with IllegalArgumentException running against hadoop 2.3
    * [HBASE-10723] - Convert TestExplicitColumnTracker to junit4 test
    * [HBASE-10729] - Enable table doesn't balance out replicas evenly if the replicas were unassigned earlier
    * [HBASE-10734] - Fix RegionStates.getRegionAssignments to not add duplicate regions
    * [HBASE-10741] - Deprecate HTablePool and HTableFactory
    * [HBASE-10743] - Replica map update is problematic in RegionStates
    * [HBASE-10750] - Pluggable MemStoreLAB
    * [HBASE-10778] - Unique keys accounting in MultiThreadedReader is incorrect
    * [HBASE-10779] - Doc hadoop1 deprecated in 0.98 and NOT supported in hbase 1.0
    * [HBASE-10781] - Remove hadoop-one-compat module and all references to hadoop1
    * [HBASE-10791] - Add integration test to demonstrate performance improvement
    * [HBASE-10794] - multi-get should handle replica location missing from cache
    * [HBASE-10796] - Set default log level as INFO
    * [HBASE-10801] - Ensure DBE interfaces can work with Cell
    * [HBASE-10810] - LoadTestTool should share the connection and connection pool
    * [HBASE-10815] - Master regionserver should be rolling-upgradable
    * [HBASE-10817] - Add some tests on a real cluster for replica: multi master, replication
    * [HBASE-10818] - Add integration test for bulkload with replicas
    * [HBASE-10822] - Thread local addendum to HBASE-10656 Counter
    * [HBASE-10841] - Scan,Get,Put,Delete,etc setters should consistently return this
    * [HBASE-10855] - Enable hfilev3 by default
    * [HBASE-10858] - TestRegionRebalancing is failing
    * [HBASE-10859] - Use HFileLink in opening region files from secondaries
    * [HBASE-10888] - Enable distributed log replay as default
    * [HBASE-10915] - Decouple region closing (HM and HRS) from ZK
    * [HBASE-10918] - [VisibilityController] System table backed ScanLabelGenerator
    * [HBASE-10929] - Change ScanQueryMatcher to use Cells instead of KeyValue.
    * [HBASE-10930] - Change Filters and GetClosestRowBeforeTracker to work with Cells
    * [HBASE-10957] - HBASE-10070: HMaster can abort with NPE in #rebuildUserRegions
    * [HBASE-10962] - Decouple region opening (HM and HRS) from ZK
    * [HBASE-10963] - Refactor cell ACL tests
    * [HBASE-10972] - OOBE in prefix key encoding
    * [HBASE-10985] - Decouple Split Transaction from Zookeeper
    * [HBASE-10993] - Deprioritize long-running scanners
    * [HBASE-11025] - Infrastructure for pluggable consensus service
    * [HBASE-11027] - Remove kv.isDeleteXX() and related methods and use CellUtil apis.
    * [HBASE-11053] - Change DeleteTracker APIs to work with Cell
    * [HBASE-11054] - Create new hook in StoreScanner to help user creating his own delete tracker
    * [HBASE-11059] - ZK-less region assignment
    * [HBASE-11069] - Decouple region merging from ZooKeeper
    * [HBASE-11072] - Abstract WAL splitting from ZK
    * [HBASE-11077] - [AccessController] Restore compatible early-out access denial
    * [HBASE-11088] - Support Visibility Expression Deletes in Shell
    * [HBASE-11092] - Server interface should have method getConsensusProvider()
    * [HBASE-11094] - Distributed log replay is incompatible for rolling restarts
    * [HBASE-11098] - Improve documentation around our blockcache options
    * [HBASE-11101] - Documentation review
    * [HBASE-11102] - Document JDK versions supported by each release
    * [HBASE-11108] - Split ZKTable into interface and implementation
    * [HBASE-11109] - flush region sequence id may not be larger than all edits flushed
    * [HBASE-11135] - Change region sequenceid generation so happens earlier in the append cycle rather than just before added to file
    * [HBASE-11140] - LocalHBaseCluster should create ConsensusProvider per each server
    * [HBASE-11161] - Provide example of POJO encoding with protobuf
    * [HBASE-11171] - More doc improvements on block cache options
    * [HBASE-11214] - Fixes for scans on a replicated table
    * [HBASE-11229] - Change block cache percentage metrics to be doubles rather than ints
    * [HBASE-11280] - Document distributed log replay and distributed log splitting
    * [HBASE-11307] - Deprecate SlabCache
    * [HBASE-11318] - Classes in security subpackages missing @InterfaceAudience annotations.
    * [HBASE-11332] - Fix for metas location cache from HBASE-10785
    * [HBASE-11367] - Pluggable replication endpoint
    * [HBASE-11372] - Remove SlabCache
    * [HBASE-11384] - [Visibility Controller]Check for users covering authorizations for every mutation
    * [HBASE-11395] - Add logging for HBase table operations
    * [HBASE-11471] - Move TableStateManager and ZkTableStateManager and Server to hbase-server
    * [HBASE-11483] - Check the rest of the book for new XML validity errors and fix
    * [HBASE-11508] - Document changes to IPC config parameters from HBASE-11492
    * [HBASE-11511] - Write flush events to WAL
    * [HBASE-11512] - Write region open/close events to WAL
    * [HBASE-11520] - Simplify offheap cache config by removing the confusing "hbase.bucketcache.percentage.in.combinedcache"
    * [HBASE-11559] - Add dumping of DATA block usage to the BlockCache JSON report.
    * [HBASE-11572] - Add support for doing get/scans against a particular replica_id
    * [HBASE-11573] - Report age on eviction
    * [HBASE-11610] - Enhance remote meta updates
    * [HBASE-11651] - Add conf which disables MetaMigrationConvertingToPB check (for experts only)
    * [HBASE-11722] - Document new shortcut commands introduced by HBASE-11649
    * [HBASE-11734] - Document changed behavior of hbase.hstore.time.to.purge.deletes
    * [HBASE-11736] - Document SKIP_FLUSH snapshot option
    * [HBASE-11737] - Document callQueue improvements from HBASE-11355 and HBASE-11724
    * [HBASE-11739] - Document blockCache contents report in the UI
    * [HBASE-11740] - RegionStates.getRegionAssignments() gets stuck on clone
    * [HBASE-11752] - Document blockcache prefetch option
    * [HBASE-11753] - Document HBASE_SHELL_OPTS environment variable
    * [HBASE-11781] - Document new TableMapReduceUtil scanning options
    * [HBASE-11784] - Document global configuration for maxVersion
    * [HBASE-11822] - Convert EnvironmentEdge#getCurrentTimeMillis to getCurrentTime
    * [HBASE-11919] - Remove the deprecated pre/postGet CP hook
    * [HBASE-11923] - Potential race condition in RecoverableZookeeper.checkZk()
    * [HBASE-11934] - Support KeyValueCodec to encode non KeyValue cells.
    * [HBASE-11941] - Rebuild site because of major structural changes to HTML
    * [HBASE-11963] - Synchronize peer cluster replication connection attempts

** Brainstorming
    * [HBASE-9507] - Promote methods of WALActionsListener to WALObserver
    * [HBASE-11209] - Increase the default value for hbase.hregion.memstore.block.multipler from 2 to 4

** Bug
    * [HBASE-3787] - Increment is non-idempotent but client retries RPC
    * [HBASE-4931] - CopyTable instructions could be improved.
    * [HBASE-5356] - region_mover.rb can hang if table region it belongs to is deleted.
    * [HBASE-6506] - Setting CACHE_BLOCKS to false in an hbase shell scan doesn't work
    * [HBASE-6642] - enable_all,disable_all,drop_all can call "list" command with regex directly.
    * [HBASE-6701] - Revisit thrust of paragraph on splitting
    * [HBASE-7226] - HRegion.checkAndMutate uses incorrect comparison result for <, <=, > and >=
    * [HBASE-7963] - HBase VerifyReplication not working when security enabled
    * [HBASE-8112] - Deprecate HTable#batch(final List<? extends Row>)
    * [HBASE-8269] - Fix data locallity documentation.
    * [HBASE-8304] - Bulkload fails to remove files if fs.default.name / fs.defaultFS is configured without default port
    * [HBASE-8529] - checkOpen is missing from multi, mutate, get and multiGet etc.
    * [HBASE-8701] - distributedLogReplay need to apply wal edits in the receiving order of those edits
    * [HBASE-8713] - [hadoop1] Log spam each time the WAL is rolled
    * [HBASE-8803] - region_mover.rb should move multiple regions at a time
    * [HBASE-8817] - Enhance The Apache HBase Reference Guide
    * [HBASE-9151] - HBCK cannot fix when meta server znode deleted, this can happen if all region servers stopped and there are no logs to split.
    * [HBASE-9292] - Syncer fails but we won't go down
    * [HBASE-9294] - NPE in /rs-status during RS shutdown
    * [HBASE-9346] - HBCK should provide an option to check if regions boundaries are the same in META and in stores.
    * [HBASE-9445] - Snapshots should create column family dirs for empty regions
    * [HBASE-9473] - Change UI to list 'system tables' rather than 'catalog tables'.
    * [HBASE-9485] - TableOutputCommitter should implement recovery if we don't want jobs to start from 0 on RM restart
    * [HBASE-9708] - Improve Snapshot Name Error Message
    * [HBASE-9721] - RegionServer should not accept regionOpen RPC intended for another(previous) server
    * [HBASE-9745] - Append HBASE_CLASSPATH to end of Java classpath and use another env var for prefix
    * [HBASE-9746] - RegionServer can't start when replication tries to replicate to an unknown host
    * [HBASE-9754] - Eliminate threadlocal from MVCC code
    * [HBASE-9778] - Add hint to ExplicitColumnTracker to avoid seeking
    * [HBASE-9990] - HTable uses the conf for each "newCaller"
    * [HBASE-10018] - Remove region location prefetching
    * [HBASE-10061] - TableMapReduceUtil.findOrCreateJar calls updateMap(null, ) resulting in thrown NPE
    * [HBASE-10069] - Potential duplicate calls to log#appendNoSync() in HRegion#doMiniBatchMutation()
    * [HBASE-10073] - Revert HBASE-9718 (Add a test scope dependency on org.slf4j:slf4j-api to hbase-client)
    * [HBASE-10079] - Race in TableName cache
    * [HBASE-10080] - Unnecessary call to locateRegion when creating an HTable instance
    * [HBASE-10084] - [WINDOWS] bin\hbase.cmd should allow whitespaces in java.library.path and classpath
    * [HBASE-10087] - Store should be locked during a memstore snapshot
    * [HBASE-10090] - Master could hang in assigning meta
    * [HBASE-10097] - Remove a region name string creation in HRegion#nextInternal
    * [HBASE-10098] - [WINDOWS] pass in native library directory from hadoop for unit tests
    * [HBASE-10099] - javadoc warning introduced by LabelExpander 188: warning - @return tag has no arguments
    * [HBASE-10101] - testOfflineRegionReAssginedAfterMasterRestart times out sometimes.
    * [HBASE-10103] - TestNodeHealthCheckChore#testRSHealthChore: Stoppable must have been stopped
    * [HBASE-10107] - [JDK7] TestHBaseSaslRpcClient.testHBaseSaslRpcClientCreation failing on Jenkins
    * [HBASE-10108] - NullPointerException thrown while use Canary with '-regionserver' option
    * [HBASE-10112] - Hbase rest query params for maxVersions and maxValues are not parsed
    * [HBASE-10114] - _scan_internal() in table.rb should accept argument that specifies reverse scan
    * [HBASE-10117] - Avoid synchronization in HRegionScannerImpl.isFilterDone
    * [HBASE-10118] - Major compact keeps deletes with future timestamps
    * [HBASE-10120] - start-hbase.sh doesn't respect --config in non-distributed mode
    * [HBASE-10123] - Change default ports; move them out of linux ephemeral port range
    * [HBASE-10132] - sun.security.pkcs11.wrapper.PKCS11Exception: CKR_ARGUMENTS_BAD
    * [HBASE-10135] - Remove ? extends from HLogSplitter#getOutputCounts
    * [HBASE-10137] - GeneralBulkAssigner with retain assignment plan can be used in EnableTableHandler to bulk assign the regions
    * [HBASE-10138] - incorrect or confusing test value is used in block caches
    * [HBASE-10142] - TestLogRolling#testLogRollOnDatanodeDeath test failure
    * [HBASE-10146] - Bump HTrace version to 2.04
    * [HBASE-10148] - [VisibilityController] Tolerate regions in recovery
    * [HBASE-10149] - TestZKPermissionsWatcher.testPermissionsWatcher test failure
    * [HBASE-10155] - HRegion isRecovering state is wrongly coming in postOpen hook
    * [HBASE-10161] - [AccessController] Tolerate regions in recovery
    * [HBASE-10163] - Example Thrift DemoClient is flaky
    * [HBASE-10176] - Canary#sniff() should close the HTable instance
    * [HBASE-10178] - Potential null object dereference in TablePermission#equals()
    * [HBASE-10179] - HRegionServer underreports readRequestCounts by 1 under certain conditions
    * [HBASE-10182] - Potential null object deference in AssignmentManager#handleRegion()
    * [HBASE-10186] - region_mover.rb broken because ServerName constructor is changed to private
    * [HBASE-10187] - AccessController#preOpen - Include 'labels' table also into special tables list.
    * [HBASE-10193] - Cleanup HRegion if one of the store fails to open at region initialization
    * [HBASE-10194] - [Usability]: Instructions in CompactionTool no longer accurate because of namespaces
    * [HBASE-10196] - Enhance HBCK to understand the case after online region merge
    * [HBASE-10205] - ConcurrentModificationException in BucketAllocator
    * [HBASE-10207] - ZKVisibilityLabelWatcher : Populate the labels cache on startup
    * [HBASE-10210] - during master startup, RS can be you-are-dead-ed by master in error
    * [HBASE-10215] - TableNotFoundException should be thrown after removing stale znode in ETH
    * [HBASE-10219] - HTTPS support for HBase in RegionServerListTmpl.jamon
    * [HBASE-10220] - Put all test service principals into the superusers list
    * [HBASE-10221] - Region from coprocessor invocations can be null on failure
    * [HBASE-10223] - [VisibilityController] cellVisibility presence check on Delete mutation is wrong
    * [HBASE-10225] - Bug in calls to RegionObsever.postScannerFilterRow
    * [HBASE-10226] - [AccessController] Namespace grants not always checked
    * [HBASE-10231] - Potential NPE in HBaseFsck#checkMetaRegion()
    * [HBASE-10232] - Remove native profile from hbase-shell
    * [HBASE-10233] - VisibilityController is too chatty at DEBUG level
    * [HBASE-10249] - TestReplicationSyncUpTool fails because failover takes too long
    * [HBASE-10251] - Restore API Compat for PerformanceEvaluation.generateValue()
    * [HBASE-10260] - Canary Doesn't pick up Configuration properly.
    * [HBASE-10264] - [MapReduce]: CompactionTool in mapred mode is missing classes in its classpath
    * [HBASE-10267] - TestNamespaceCommands occasionally fails
    * [HBASE-10268] - TestSplitLogWorker occasionally fails
    * [HBASE-10271] - [regression] Cannot use the wildcard address since HBASE-9593
    * [HBASE-10272] - Cluster becomes nonoperational if the node hosting the active Master AND ROOT/META table goes offline
    * [HBASE-10274] - MiniZookeeperCluster should close ZKDatabase when shutdown ZooKeeperServers
    * [HBASE-10284] - Build broken with svn 1.8
    * [HBASE-10292] - TestRegionServerCoprocessorExceptionWithAbort fails occasionally
    * [HBASE-10298] - TestIOFencing occasionally fails
    * [HBASE-10302] - Fix rat check issues in hbase-native-client.
    * [HBASE-10304] - Running an hbase job jar: IllegalAccessError: class com.google.protobuf.ZeroCopyLiteralByteString cannot access its superclass com.google.protobuf.LiteralByteString
    * [HBASE-10307] - IntegrationTestIngestWithEncryption assumes localhost cluster
    * [HBASE-10310] - ZNodeCleaner session expired for /hbase/master
    * [HBASE-10312] - Flooding the cluster with administrative actions leads to collapse
    * [HBASE-10313] - Duplicate servlet-api jars in hbase 0.96.0
    * [HBASE-10315] - Canary shouldn't exit with 3 if there is no master running.
    * [HBASE-10316] - Canary#RegionServerMonitor#monitorRegionServers() should close the scanner returned by table.getScanner()
    * [HBASE-10317] - getClientPort method of MiniZooKeeperCluster does not always return the correct value
    * [HBASE-10318] - generate-hadoopX-poms.sh expects the version to have one extra '-'
    * [HBASE-10320] - Avoid ArrayList.iterator() ExplicitColumnTracker
    * [HBASE-10321] - CellCodec has broken the 96 client to 98 server compatibility
    * [HBASE-10322] - Strip tags from KV while sending back to client on reads
    * [HBASE-10326] - Super user should be able scan all the cells irrespective of the visibility labels
    * [HBASE-10327] - Remove remove(K, V) from type PoolMap<K,V>
    * [HBASE-10329] - Fail the writes rather than proceeding silently to prevent data loss when AsyncSyncer encounters null writer and its writes aren't synced by other Asyncer
    * [HBASE-10330] - TableInputFormat/TableRecordReaderImpl leaks HTable
    * [HBASE-10332] - Missing .regioninfo file during daughter open processing
    * [HBASE-10333] - Assignments are not retained on a cluster start
    * [HBASE-10334] - RegionServer links in table.jsp is broken
    * [HBASE-10335] - AuthFailedException in zookeeper may block replication forever
    * [HBASE-10336] - Remove deprecated usage of Hadoop HttpServer in InfoServer
    * [HBASE-10337] - HTable.get() uninteruptible
    * [HBASE-10338] - Region server fails to start with AccessController coprocessor if installed into RegionServerCoprocessorHost
    * [HBASE-10339] - Mutation::getFamilyMap method was lost in 98
    * [HBASE-10349] - Table became unusable when master balanced its region after table was dropped
    * [HBASE-10365] - HBaseFsck should clean up connection properly when repair is completed
    * [HBASE-10370] - Compaction in out-of-date Store causes region split failure
    * [HBASE-10371] - Compaction creates empty hfile, then selects this file for compaction and creates empty hfile and over again
    * [HBASE-10375] - hbase-default.xml hbase.status.multicast.address.port does not match code
    * [HBASE-10384] - Failed to increment serveral columns in one Increment
    * [HBASE-10392] - Correct references to hbase.regionserver.global.memstore.upperLimit
    * [HBASE-10397] - Fix findbugs introduced from HBASE-9426
    * [HBASE-10400] - [hbck] Continue if region dir missing on region merge attempt
    * [HBASE-10401] - [hbck] perform overlap group merges in parallel
    * [HBASE-10407] - String Format Exception
    * [HBASE-10412] - Distributed log replay : Cell tags getting missed
    * [HBASE-10413] - Tablesplit.getLength returns 0
    * [HBASE-10417] - index is not incremented in PutSortReducer#reduce()
    * [HBASE-10422] - ZeroCopyLiteralByteString.zeroCopyGetBytes has an unusable prototype and conflicts with AsyncHBase
    * [HBASE-10426] - user_permission in security.rb calls non-existent UserPermission#getTable method
    * [HBASE-10428] - Test jars should have scope test
    * [HBASE-10429] - Make Visibility Controller to throw a better msg if it is of type RegionServerCoprocessor
    * [HBASE-10431] - Rename com.google.protobuf.ZeroCopyLiteralByteString
    * [HBASE-10432] - Rpc retries non-recoverable error
    * [HBASE-10433] - SecureProtobufWALReader does not handle unencrypted WALs if configured to encrypt
    * [HBASE-10434] - Store Tag compression information for a WAL in its header
    * [HBASE-10435] - Lower the log level of Canary region server match
    * [HBASE-10436] - restore regionserver lists removed from hbase 0.96+ jmx
    * [HBASE-10438] - NPE from LRUDictionary when size reaches the max init value
    * [HBASE-10441] - [docs] nit default max versions is now 1 instead of 3 after HBASE-8450
    * [HBASE-10442] - prepareDelete() isn't called before doPreMutationHook for a row deletion case
    * [HBASE-10443] - IndexOutOfBoundExceptions when processing compressed tags in HFile
    * [HBASE-10446] - Backup master gives Error 500 for debug dump
    * [HBASE-10447] - Memstore flusher scans storefiles also when the scanner heap gets reset
    * [HBASE-10448] - ZKUtil create and watch methods don't set watch in some cases
    * [HBASE-10449] - Wrong execution pool configuration in HConnectionManager
    * [HBASE-10451] - Enable back Tag compression on HFiles
    * [HBASE-10452] - Fix potential bugs in exception handlers
    * [HBASE-10454] - Tags presence file info can be wrong in HFiles when PrefixTree encoding is used
    * [HBASE-10455] - cleanup InterruptedException management
    * [HBASE-10456] - Balancer should not run if it's just turned off.
    * [HBASE-10458] - Typo in book chapter 9 architecture.html
    * [HBASE-10459] - Broken link F.1. HBase Videos
    * [HBASE-10460] - Return value of Scan#setSmall() should be void
    * [HBASE-10461] - table.close() in TableEventHandler#reOpenAllRegions() should be enclosed in finally block
    * [HBASE-10469] - Hbase book client.html has a broken link
    * [HBASE-10470] - Import generates huge log file while importing large amounts of data
    * [HBASE-10472] - Manage the interruption in ZKUtil#getData
    * [HBASE-10476] - HBase Master log grows very fast after stopped hadoop (due to connection exception)
    * [HBASE-10477] - Regression from HBASE-10337
    * [HBASE-10478] - Hbase book presentations page has broken link
    * [HBASE-10481] - API Compatibility JDiff script does not properly handle arguments in reverse order
    * [HBASE-10482] - ReplicationSyncUp doesn't clean up its ZK, needed for tests
    * [HBASE-10485] - PrefixFilter#filterKeyValue() should perform filtering on row key
    * [HBASE-10486] - ProtobufUtil Append & Increment deserialization lost cell level timestamp
    * [HBASE-10488] - 'mvn site' is broken due to org.apache.jasper.JspC not found
    * [HBASE-10490] - Simplify RpcClient code
    * [HBASE-10493] - InclusiveStopFilter#filterKeyValue() should perform filtering on row key
    * [HBASE-10495] - upgrade script is printing usage two times with help option.
    * [HBASE-10500] - Some tools OOM when BucketCache is enabled
    * [HBASE-10501] - Improve IncreasingToUpperBoundRegionSplitPolicy to avoid too many regions
    * [HBASE-10506] - Fail-fast if client connection is lost before the real call be executed in RPC layer
    * [HBASE-10510] - HTable is not closed in LoadTestTool#loadTable()
    * [HBASE-10514] - Forward port HBASE-10466, possible data loss when failed flushes
    * [HBASE-10516] - Refactor code where Threads.sleep is called within a while/for loop
    * [HBASE-10525] - Allow the client to use a different thread for writing to ease interrupt
    * [HBASE-10533] - commands.rb is giving wrong error messages on exceptions
    * [HBASE-10534] - Rowkey in TsvImporterTextMapper initializing with wrong length
    * [HBASE-10537] - Let the ExportSnapshot mapper fail and retry on error
    * [HBASE-10539] - HRegion.addAndGetGlobalMemstoreSize returns previous size
    * [HBASE-10545] - RS Hangs waiting on region to close on shutdown; has to timeout before can go down
    * [HBASE-10546] - Two scanner objects are open for each hbase map task but only one scanner object is closed
    * [HBASE-10547] - TestFixedLengthWrapper#testReadWrite occasionally fails with the IBM JDK
    * [HBASE-10548] - Correct commons-math dependency version
    * [HBASE-10549] - When there is a hole, LoadIncrementalHFiles will hang in an infinite loop.
    * [HBASE-10552] - HFilePerformanceEvaluation.GaussianRandomReadBenchmark fails sometimes.
    * [HBASE-10556] - Possible data loss due to non-handled DroppedSnapshotException for user-triggered flush from client/shell
    * [HBASE-10563] - Set name for FlushHandler thread
    * [HBASE-10564] - HRegionServer.nextLong should be removed since it's not used anywhere, or should be used somewhere it meant to
    * [HBASE-10565] - FavoredNodesPlan accidentally uses an internal Netty type
    * [HBASE-10566] - cleanup rpcTimeout in the client
    * [HBASE-10567] - Add overwrite manifest option to ExportSnapshot
    * [HBASE-10575] - ReplicationSource thread can't be terminated if it runs into the loop to contact peer's zk ensemble and fails continuously
    * [HBASE-10579] - [Documentation]: ExportSnapshot tool package incorrectly documented
    * [HBASE-10580] - IntegrationTestingUtility#restoreCluster leak resource when running in a mini cluster mode
    * [HBASE-10581] - ACL znode are left without PBed during upgrading hbase0.94* to hbase0.96+
    * [HBASE-10582] - 0.94->0.96 Upgrade: ACL can't be repopulated when ACL table contains row for table '-ROOT' or '.META.'
    * [HBASE-10585] - Avoid early creation of Node objects in LRUDictionary.BidirectionalLRUMap
    * [HBASE-10586] - hadoop2-compat IPC metric registred twice
    * [HBASE-10587] - Master metrics clusterRequests is wrong
    * [HBASE-10593] - FileInputStream in JenkinsHash#main() is never closed
    * [HBASE-10594] - Speed up TestRestoreSnapshotFromClient
    * [HBASE-10598] - Written data can not be read out because MemStore#timeRangeTracker might be updated concurrently
    * [HBASE-10600] - HTable#batch() should perform validation on empty Put
    * [HBASE-10604] - Fix parseArgs javadoc
    * [HBASE-10606] - Bad timeout in RpcRetryingCaller#callWithRetries w/o parameters
    * [HBASE-10608] - Acquire the FS Delegation Token for Secure ExportSnapshot
    * [HBASE-10611] - Description for hbase:acl table is wrong on master-status#catalogTables
    * [HBASE-10614] - Master could not be stopped
    * [HBASE-10618] - User should not be allowed to disable/drop visibility labels table
    * [HBASE-10621] - Unable to grant user permission to namespace
    * [HBASE-10622] - Improve log and Exceptions in Export Snapshot
    * [HBASE-10624] - Fix 2 new findbugs warnings introduced by HBASE-10598
    * [HBASE-10627] - A logic mistake in HRegionServer isHealthy
    * [HBASE-10631] - Avoid extra seek on FileLink open
    * [HBASE-10632] - Region lost in limbo after ArrayIndexOutOfBoundsException during assignment
    * [HBASE-10637] - rpcClient: Setup the iostreams when writing
    * [HBASE-10639] - Unload script displays wrong counts (off by one) when unloading regions
    * [HBASE-10644] - TestSecureExportSnapshot#testExportFileSystemState fails on hadoop-1
    * [HBASE-10656] -  high-scale-lib's Counter depends on Oracle (Sun) JRE, and also has some bug
    * [HBASE-10660] - MR over snapshots can OOM when alternative blockcache is enabled
    * [HBASE-10662] - RegionScanner is never closed if the region has been moved-out or re-opened when performing scan request
    * [HBASE-10665] - TestCompaction and TestCompactionWithCoprocessor run too long
    * [HBASE-10666] - TestMasterCoprocessorExceptionWithAbort hangs at shutdown
    * [HBASE-10668] - TestExportSnapshot runs too long
    * [HBASE-10669] - [hbck tool] Usage is wrong for hbck tool for -sidelineCorruptHfiles option
    * [HBASE-10675] - IntegrationTestIngestWithACL should allow User to be passed as Parameter
    * [HBASE-10677] - boundaries check in hbck throwing IllegalArgumentException
    * [HBASE-10679] - Both clients get wrong scan results if the first scanner expires and the second scanner is created with the same scannerId on the same region
    * [HBASE-10682] - region_mover.rb throws "can't convert nil into String" for regions moved
    * [HBASE-10685] - [WINDOWS] TestKeyStoreKeyProvider fails on windows
    * [HBASE-10686] - [WINDOWS] TestStripeStoreFileManager fails on windows
    * [HBASE-10687] - Fix description about HBaseLocalFileSpanReceiver in reference manual
    * [HBASE-10692] - The Multi TableMap job don't support the security HBase cluster
    * [HBASE-10694] - TableSkewCostFunction#cost() casts integral division result to double
    * [HBASE-10705] - CompactionRequest#toString() may throw NullPointerException
    * [HBASE-10706] - Disable writeToWal in tests where possible
    * [HBASE-10714] - SyncFuture hangs when sequence is 0
    * [HBASE-10715] - TimeRange has a poorly formatted error message
    * [HBASE-10716] - [Configuration]: hbase.regionserver.region.split.policy should be part of hbase-default.xml
    * [HBASE-10718] - TestHLogSplit fails when it sets a KV size to be negative
    * [HBASE-10720] - rpcClient: Wrong log level when closing the connection
    * [HBASE-10726] - Fix java.lang.ArrayIndexOutOfBoundsException in StochasticLoadBalancer$LocalityBasedCandidateGenerator
    * [HBASE-10731] - Fix environment variables typos in scripts
    * [HBASE-10735] - [WINDOWS] Set -XX:MaxPermSize for unit tests
    * [HBASE-10736] - Fix Javadoc warnings introduced in HBASE-10169
    * [HBASE-10737] - HConnectionImplementation should stop RpcClient on close
    * [HBASE-10738] - AssignmentManager should shut down executors on stop
    * [HBASE-10739] - RS web UI NPE if master shuts down sooner
    * [HBASE-10745] - Access ShutdownHook#fsShutdownHooks should be synchronized
    * [HBASE-10749] - CellComparator.compareStatic() compares type wrongly
    * [HBASE-10751] - TestHRegion testWritesWhileScanning occasional fail since HBASE-10514 went in
    * [HBASE-10755] - MetricsRegionSourceImpl creates metrics that start with a lower case
    * [HBASE-10760] - Wrong methods' names in ClusterLoadState class
    * [HBASE-10762] - clone_snapshot doesn't check for missing namespace
    * [HBASE-10766] - SnapshotCleaner allows to delete referenced files
    * [HBASE-10770] - Don't exit from the Canary daemon mode if no regions are present
    * [HBASE-10792] - RingBufferTruck does not release its payload
    * [HBASE-10793] - AuthFailed as a valid zookeeper state
    * [HBASE-10799] - [WINDOWS] TestImportTSVWithVisibilityLabels.testBulkOutputWithTsvImporterTextMapper  fails on windows
    * [HBASE-10802] - CellComparator.compareStaticIgnoreMvccVersion compares type wrongly
    * [HBASE-10804] - Add a validations step to ExportSnapshot
    * [HBASE-10805] - Speed up KeyValueHeap.next() a bit
    * [HBASE-10806] - Two protos missing in hbase-protocol/pom.xml
    * [HBASE-10809] - HBaseAdmin#deleteTable fails when META region happen to move around same time
    * [HBASE-10814] - RpcClient: some calls can get stuck when connection is closing
    * [HBASE-10825] - Add copy-from option to ExportSnapshot
    * [HBASE-10829] - Flush is skipped after log replay if the last recovered edits file is skipped
    * [HBASE-10830] - Integration test MR jobs attempt to load htrace jars from the wrong location
    * [HBASE-10831] - IntegrationTestIngestWithACL is not setting up LoadTestTool correctly
    * [HBASE-10833] - Region assignment may fail during cluster start up
    * [HBASE-10838] - Insufficient AccessController covering permission check
    * [HBASE-10839] - NullPointerException in construction of RegionServer in Security Cluster
    * [HBASE-10840] - Fix findbug warn induced by HBASE-10569
    * [HBASE-10845] - Memstore snapshot size isn't updated in DefaultMemStore#rollback()
    * [HBASE-10846] - Links between active and backup masters are broken
    * [HBASE-10848] - Filter SingleColumnValueFilter combined with NullComparator does not work
    * [HBASE-10849] - Fix increased javadoc warns
    * [HBASE-10850] - essential column family optimization is broken
    * [HBASE-10851] - Wait for regionservers to join the cluster
    * [HBASE-10853] - NPE in RSRpcServices.get on trunk
    * [HBASE-10854] - [VisibilityController] Apply MAX_VERSIONS from schema or request when scanning
    * [HBASE-10860] - Insufficient AccessController covering permission check
    * [HBASE-10862] - Update config field names in hbase-default.xml description for hbase.hregion.memstore.block.multiplier
    * [HBASE-10863] - Scan doesn't return rows for user who has authorization by visibility label in secure deployment
    * [HBASE-10864] - Spelling nit
    * [HBASE-10890] - ExportSnapshot needs to add acquired token to job
    * [HBASE-10895] - unassign a region fails due to the hosting region server is in FailedServerList
    * [HBASE-10897] - On master start, deadlock if refresh UI
    * [HBASE-10899] - [AccessController] Apply MAX_VERSIONS from schema or request when scanning
    * [HBASE-10903] - HBASE-10740 regression; cannot pass commands for zk to run
    * [HBASE-10917] - Fix hbase book "Tests" page
    * [HBASE-10922] - Log splitting status should always be closed
    * [HBASE-10931] - Enhance logs
    * [HBASE-10941] - default for max version isn't updated in doc after change on 0.96
    * [HBASE-10948] - Fix hbase table file 'x' mode
    * [HBASE-10949] - Reversed scan could hang
    * [HBASE-10954] - Fix TestCloseRegionHandler.testFailedFlushAborts
    * [HBASE-10955] - HBCK leaves the region in masters in-memory RegionStates if region hdfs dir is lost
    * [HBASE-10958] - [dataloss] Bulk loading with seqids can prevent some log entries from being replayed
    * [HBASE-10964] - Delete mutation is not consistent with Put wrt timestamp
    * [HBASE-10966] - RowCounter misinterprets column names that have colons in their qualifier
    * [HBASE-10967] - CatalogTracker.waitForMeta should not wait indefinitely silently
    * [HBASE-10968] - Null check in TableSnapshotInputFormat#TableSnapshotRegionRecordReader#initialize() is redundant
    * [HBASE-10970] - [AccessController] Issues with covering cell permission checks
    * [HBASE-10976] - Start CatalogTracker after cluster ID is available
    * [HBASE-10979] - Fix AnnotationReadingPriorityFunction "scan" handling
    * [HBASE-10995] - Fix resource leak related to unclosed HBaseAdmin
    * [HBASE-11005] - Remove dead code in HalfStoreFileReader#getScanner#seekBefore()
    * [HBASE-11009] - We sync every hbase:meta table write twice
    * [HBASE-11011] - Avoid extra getFileStatus() calls on Region startup
    * [HBASE-11012] - InputStream is not properly closed in two methods of JarFinder
    * [HBASE-11018] - ZKUtil.getChildDataAndWatchForNewChildren() will not return null as indicated
    * [HBASE-11028] - FSLog: Avoid an extra sync if the current transaction is already sync'd
    * [HBASE-11030] - HBaseTestingUtility.getMiniHBaseCluster should be able to return null
    * [HBASE-11036] - Online schema change with region merge may cause data loss
    * [HBASE-11038] - Filtered scans can bypass metrics collection
    * [HBASE-11049] - HBase WALPlayer needs to add credentials to job to play to table
    * [HBASE-11052] - Sending random data crashes thrift service
    * [HBASE-11055] - Extends the sampling size
    * [HBASE-11064] - Odd behaviors of TableName for empty namespace
    * [HBASE-11081] - Trunk Master won't start; looking for Constructor that takes conf only
    * [HBASE-11082] - Potential unclosed TraceScope in FSHLog#replaceWriter()
    * [HBASE-11096] - stop method of Master and RegionServer coprocessor  is not invoked
    * [HBASE-11112] - PerformanceEvaluation should document --multiGet option on its printUsage.
    * [HBASE-11117] - [AccessController] checkAndPut/Delete hook should check only Read permission
    * [HBASE-11118] - non environment variable solution for "IllegalAccessError: class com.google.protobuf.ZeroCopyLiteralByteString cannot access its superclass com.google.protobuf.LiteralByteString"
    * [HBASE-11120] - Update documentation about major compaction algorithm
    * [HBASE-11133] - Add an option to skip snapshot verification after Export
    * [HBASE-11139] - BoundedPriorityBlockingQueue#poll() should check the return value from awaitNanos()
    * [HBASE-11143] - Improve replication metrics
    * [HBASE-11149] - Wire encryption is broken
    * [HBASE-11150] - Images in website are broken
    * [HBASE-11155] - Fix Validation Errors in Ref Guide
    * [HBASE-11162] - RegionServer webui uses the default master info port irrespective of the user configuration.
    * [HBASE-11168] - [docs] Remove references to RowLocks in post 0.96 docs.
    * [HBASE-11169] - nit: fix incorrect javadoc in OrderedBytes about BlobVar and BlobCopy
    * [HBASE-11176] - Make /src/main/xslt/configuration_to_docbook_section.xsl produce better Docbook
    * [HBASE-11177] - 'hbase.rest.filter.classes' exists in hbase-default.xml twice
    * [HBASE-11185] - Parallelize Snapshot operations
    * [HBASE-11186] - Improve TestExportSnapshot verifications
    * [HBASE-11189] - Subprocedure should be marked as complete upon failure
    * [HBASE-11190] - Fix easy typos in documentation
    * [HBASE-11194] - [AccessController] issue with covering permission check in case of concurrent op on same row
    * [HBASE-11196] - Update description of -ROOT- in ref guide
    * [HBASE-11202] - Cleanup on HRegion class
    * [HBASE-11212] - Fix increment index in KeyValueSortReducer
    * [HBASE-11215] - Deprecate void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail)
    * [HBASE-11217] - Race between SplitLogManager task creation + TimeoutMonitor
    * [HBASE-11218] - Data loss in HBase standalone mode
    * [HBASE-11226] - Document and increase the default value for hbase.hstore.flusher.count
    * [HBASE-11234] - FastDiffDeltaEncoder#getFirstKeyInBlock returns wrong result
    * [HBASE-11237] - Bulk load initiated by user other than hbase fails
    * [HBASE-11238] - Add info about SlabCache and BucketCache to Ref Guide
    * [HBASE-11239] - Forgot to svn add test that was part of HBASE-11171, TestCacheConfig
    * [HBASE-11248] - KeyOnlyKeyValue#toString() passes wrong offset to keyToString()
    * [HBASE-11251] - LoadTestTool should grant READ permission for the users that are given READ access for specific cells
    * [HBASE-11252] - Fixing new javadoc warnings in master branch
    * [HBASE-11253] - IntegrationTestWithCellVisibilityLoadAndVerify failing since HBASE-10326
    * [HBASE-11255] - Negative request num in region load
    * [HBASE-11260] - hbase-default.xml refers to hbase.regionserver.global.memstore.upperLimit which is deprecated
    * [HBASE-11267] - Dynamic metrics2 metrics may consume large amount of heap memory
    * [HBASE-11268] - HTablePool is now a deprecated class, should update docs to reflect this
    * [HBASE-11273] - Fix jersey and slf4j deps
    * [HBASE-11275] - [AccessController] postCreateTable hook fails when another CP creates table on their startup
    * [HBASE-11277] - RPCServer threads can wedge under high load
    * [HBASE-11279] - Block cache could be disabled by mistake
    * [HBASE-11285] - Expand coprocs info in Ref Guide
    * [HBASE-11297] - Remove some synchros in the rpcServer responder
    * [HBASE-11298] - Simplification in RpcServer code
    * [HBASE-11302] - ReplicationSourceManager#sources is not thread safe
    * [HBASE-11310] - Delete's copy constructor should copy the attributes also
    * [HBASE-11311] - Secure Bulk Load does not execute chmod 777 on the files
    * [HBASE-11312] - Minor refactoring of TestVisibilityLabels class
    * [HBASE-11320] - Reenable bucket cache logging
    * [HBASE-11324] - Update 2.5.2.8. Managed Compactions
    * [HBASE-11327] - ExportSnapshot hit stackoverflow error when target snapshotDir doesn't contain uri
    * [HBASE-11329] - Minor fixup of new blockcache tab number formatting
    * [HBASE-11335] - Fix the TABLE_DIR param in TableSnapshotInputFormat
    * [HBASE-11337] - Document how to create, modify, delete a table using Java
    * [HBASE-11338] - Expand documentation on bloom filters
    * [HBASE-11340] - Remove references to xcievers in documentation
    * [HBASE-11341] - ZKProcedureCoordinatorRpcs should respond only to members
    * [HBASE-11342] - The method isChildReadLock in class ZKInterProcessLockBase is wrong
    * [HBASE-11347] - For some errors, the client can retry infinitely
    * [HBASE-11353] - Wrong Write Request Count
    * [HBASE-11363] - Access checks in preCompact and preCompactSelection are out of sync
    * [HBASE-11371] - Typo in Thrift2 docs
    * [HBASE-11373] - hbase-protocol compile failed for name conflict of RegionTransition
    * [HBASE-11374] - RpcRetryingCaller#callWithoutRetries has a timeout of zero
    * [HBASE-11378] - TableMapReduceUtil overwrites user supplied options for multiple tables/scaners job
    * [HBASE-11380] - HRegion lock object is not being released properly, leading to snapshot failure
    * [HBASE-11382] - Adding unit test for HBASE-10964 (Delete mutation is not consistent with Put wrt timestamp)
    * [HBASE-11387] - metrics: wrong totalRequestCount
    * [HBASE-11391] - Thrift table creation will fail with default TTL with sanity checks
    * [HBASE-11396] - Invalid meta entries can lead to unstartable master
    * [HBASE-11397] - When merging expired stripes, we need to create an empty file to preserve metadata.
    * [HBASE-11399] - Improve Quickstart chapter and move Pseudo-distributed and distributed to it
    * [HBASE-11403] - Fix race conditions around Object#notify
    * [HBASE-11413] - [findbugs] RV: Negating the result of compareTo()/compare()
    * [HBASE-11418] - build target "site" doesn't respect hadoop-two.version property
    * [HBASE-11422] - Specification of scope is missing for certain Hadoop dependencies
    * [HBASE-11423] - Visibility label and per cell ACL feature not working with HTable#mutateRow() and MultiRowMutationEndpoint
    * [HBASE-11424] - Avoid usage of CellUtil#getTagArray(Cell cell) within server
    * [HBASE-11430] - lastFlushSeqId has been updated wrongly during region open
    * [HBASE-11432] - [AccessController] Remove cell first strategy
    * [HBASE-11433] - LruBlockCache does not respect its configurable parameters
    * [HBASE-11435] - Visibility labelled cells fail to getting replicated
    * [HBASE-11439] - StripeCompaction may not obey the OffPeak rule to compaction
    * [HBASE-11442] - ReplicationSourceManager doesn't cleanup the queues for recovered sources
    * [HBASE-11445] - TestZKProcedure#testMultiCohortWithMemberTimeoutDuringPrepare is flaky
    * [HBASE-11448] - Fix javadoc warnings
    * [HBASE-11449] - IntegrationTestIngestWithACL fails to use different users after HBASE-10810
    * [HBASE-11457] - Increment HFile block encoding IVs accounting for ciper's internal use
    * [HBASE-11458] - NPEs if RegionServer cannot initialize
    * [HBASE-11460] - Deadlock in HMaster on masterAndZKLock in HConnectionManager
    * [HBASE-11463] - (findbugs) HE: Class defines equals() and uses Object.hashCode()
    * [HBASE-11465] - [VisibilityController] Reserved tags check not happening for Append/Increment
    * [HBASE-11475] - Distributed log replay should also replay compaction events
    * [HBASE-11476] - Expand 'Conceptual View' section of Data Model chapter
    * [HBASE-11477] - book.xml has Docbook validity issues (again)
    * [HBASE-11481] - TableSnapshotInputFormat javadoc wrongly claims HBase "enforces security"
    * [HBASE-11487] - ScanResponse carries non-zero cellblock for CloseScanRequest (ScanRequest with close_scanner = true)
    * [HBASE-11488] - cancelTasks in SubprocedurePool can hang during task error
    * [HBASE-11489] - ClassNotFoundException while running IT tests in trunk using 'mvn verify'
    * [HBASE-11492] - Hadoop configuration overrides some ipc parameters including tcpNoDelay
    * [HBASE-11493] - Autorestart option is not working because of stale znode  "shutdown"
    * [HBASE-11496] - HBASE-9745 broke cygwin CLASSPATH translation
    * [HBASE-11502] - Track down broken images in Ref Guide
    * [HBASE-11505] - 'snapshot' shell command shadows 'snapshot' shell when 'help' is invoked
    * [HBASE-11506] - IntegrationTestWithCellVisibilityLoadAndVerify allow User to be passed as arg
    * [HBASE-11509] - Forward port HBASE-11039 to trunk and branch-1 after HBASE-11489
    * [HBASE-11510] - Visibility serialization format tag gets duplicated in Append/Increment'ed cells
    * [HBASE-11514] - Fix findbugs warnings in blockcache
    * [HBASE-11517] - TestReplicaWithCluster turns zombie
    * [HBASE-11518] - doc update for how to create non-shared HConnection
    * [HBASE-11523] - JSON serialization of PE Options is broke
    * [HBASE-11525] - Region server holding in region states is out of sync with meta
    * [HBASE-11527] - Cluster free memory limit check should consider L2 block cache size also when L2 cache is onheap.
    * [HBASE-11530] - RegionStates adds regions to wrong servers
    * [HBASE-11531] - RegionStates for regions under region-in-transition znode are not updated on startup
    * [HBASE-11534] - Remove broken JAVA_HOME autodetection in hbase-config.sh
    * [HBASE-11535] - ReplicationPeer map is not thread safe
    * [HBASE-11536] - Puts of region location to Meta may be out of order which causes inconsistent of region location
    * [HBASE-11537] - Avoid synchronization on instances of ConcurrentMap
    * [HBASE-11540] - Document HBASE-11474
    * [HBASE-11541] - Wrong result when scaning meta with startRow
    * [HBASE-11545] - mapred.TableSnapshotInputFormat is missing InterfaceAudience annotation
    * [HBASE-11550] - Custom value for BUCKET_CACHE_BUCKETS_KEY should be sorted
    * [HBASE-11551] - BucketCache$WriterThread.run() doesn't handle exceptions correctly
    * [HBASE-11554] - Remove Reusable poolmap Rpc client type.
    * [HBASE-11555] - TableSnapshotRegionSplit should be public
    * [HBASE-11558] - Caching set on Scan object gets lost when using TableMapReduceUtil in 0.95+
    * [HBASE-11561] - deprecate ImmutableBytesWritable.getSize and replace with getLength
    * [HBASE-11564] - Improve cancellation management in the rpc layer
    * [HBASE-11565] - Stale connection could stay for a while
    * [HBASE-11575] - Pseudo distributed mode does not work as documented
    * [HBASE-11579] - CopyTable should check endtime value only if != 0
    * [HBASE-11582] - Fix Javadoc warning in DataInputInputStream and CacheConfig
    * [HBASE-11586] - HFile's HDFS op latency sampling code is not used
    * [HBASE-11588] - RegionServerMetricsWrapperRunnable misused the 'period' parameter
    * [HBASE-11589] - AccessControlException should be a not retriable exception
    * [HBASE-11591] - Scanner fails to retrieve KV  from bulk loaded file with highest sequence id than the cell's mvcc in a non-bulk loaded file
    * [HBASE-11593] - TestCacheConfig failing consistently in precommit builds
    * [HBASE-11594] - Unhandled NoNodeException in distributed log replay mode
    * [HBASE-11603] - Apply version of HADOOP-8027 to our JMXJsonServlet
    * [HBASE-11609] - LoadIncrementalHFiles fails if the namespace is specified
    * [HBASE-11617] - incorrect AgeOfLastAppliedOp and AgeOfLastShippedOp in replication Metrics when no new replication OP
    * [HBASE-11620] - Record the class name of Writer in WAL header so that only proper Reader can open the WAL file
    * [HBASE-11627] - RegionSplitter's rollingSplit terminated with "/ by zero", and the _balancedSplit file was not deleted properly
    * [HBASE-11632] - Region split needs to clear force split flag at the end of SplitRequest run
    * [HBASE-11659] - Region state RPC call is not idempotent
    * [HBASE-11662] - Launching shell with long-form --debug fails
    * [HBASE-11668] - Re-add HBASE_LIBRARY_PATH to bin/hbase
    * [HBASE-11671] - TestEndToEndSplitTransaction fails on master
    * [HBASE-11678] - BucketCache ramCache fills heap after running a few hours
    * [HBASE-11687] - No need to abort on postOpenDeployTasks exception if region opening is cancelled
    * [HBASE-11703] - Meta region state could be corrupted
    * [HBASE-11705] - callQueueSize should be decremented in a fail-fast scenario
    * [HBASE-11708] - RegionSplitter incorrectly calculates splitcount
    * [HBASE-11709] - TestMasterShutdown can fail sometime
    * [HBASE-11716] - LoadTestDataGeneratorWithVisibilityLabels should handle Delete mutations
    * [HBASE-11717] - Remove unused config 'hbase.offheapcache.percentage' from hbase-default.xml and book
    * [HBASE-11718] - Remove some logs in RpcClient.java
    * [HBASE-11719] - Remove some unused paths in AsyncClient
    * [HBASE-11725] - Backport failover checking change to 1.0
    * [HBASE-11726] - Master should fail-safe if starting with a pre 0.96 layout
    * [HBASE-11727] - Assignment wait time error in case of ServerNotRunningYetException
    * [HBASE-11728] - Data loss while scanning using PREFIX_TREE DATA-BLOCK-ENCODING
    * [HBASE-11733] - Avoid copy-paste in Master/Region CoprocessorHost
    * [HBASE-11744] - RpcServer code should not use a collection from netty internal
    * [HBASE-11745] - FilterAllFilter should return ReturnCode.SKIP
    * [HBASE-11755] - VisibilityController returns the wrong value for preBalanceSwitch()
    * [HBASE-11766] - Backdoor CoprocessorHConnection is no longer being used for local writes
    * [HBASE-11770] - TestBlockCacheReporting.testBucketCache is not stable
    * [HBASE-11772] - Bulk load mvcc and seqId issues with native hfiles
    * [HBASE-11773] - Wrong field used for protobuf construction in RegionStates.
    * [HBASE-11782] - Document that hbase.MetaMigrationConvertingToPB needs to be set to true for migrations pre 0.96
    * [HBASE-11787] - TestRegionLocations is not categorized
    * [HBASE-11788] - hbase is not deleting the cell when a Put with a KeyValue, KeyValue.Type.Delete is submitted
    * [HBASE-11789] - LoadIncrementalHFiles is not picking up the -D option
    * [HBASE-11794] - StripeStoreFlusher causes NullPointerException
    * [HBASE-11797] - Create Table interface to replace HTableInterface
    * [HBASE-11802] - Scan copy constructor doesn't copy reversed member variable
    * [HBASE-11813] - CellScanner#advance may overflow stack
    * [HBASE-11814] - TestAssignmentManager.testCloseFailed() and testOpenCloseRacing() is flaky
    * [HBASE-11816] - Initializing custom Metrics implementation failed in Mapper or Reducer
    * [HBASE-11820] - ReplicationSource : Set replication codec class as RPC codec class on a clonned Configuration
    * [HBASE-11823] - Cleanup javadoc warnings.
    * [HBASE-11832] - maven release plugin overrides command line arguments
    * [HBASE-11836] - IntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas tests simple get by default
    * [HBASE-11839] - TestRegionRebalance is flakey
    * [HBASE-11844] - region_mover.rb load enters an infinite loop if region already present on target server
    * [HBASE-11851] - RpcClient can try to close a connection not ready to close
    * [HBASE-11856] - hbase-common needs a log4j.properties resource for handling unit test logging output
    * [HBASE-11857] - Restore ReaderBase.initAfterCompression() and WALCellCodec.create(Configuration, CompressionContext)
    * [HBASE-11859] - 'hadoop jar' references in documentation should mention hbase-server.jar, not hbase.jar
    * [HBASE-11863] - WAL files are not archived and stays in the WAL directory after splitting
    * [HBASE-11876] - RegionScanner.nextRaw(...) should not update metrics
    * [HBASE-11878] - TestVisibilityLabelsWithDistributedLogReplay#testAddVisibilityLabelsOnRSRestart sometimes fails due to VisibilityController not yet initialized
    * [HBASE-11880] - NPE in MasterStatusServlet
    * [HBASE-11882] - Row level consistency may not be maintained with bulk load and compaction
    * [HBASE-11886] - The creator of the table should have all permissions on the table
    * [HBASE-11887] - Memory retention in branch-1; millions of instances of LiteralByteString for column qualifier and value
    * [HBASE-11892] - configs contain stale entries
    * [HBASE-11893] - RowTooBigException should be in hbase-client module
    * [HBASE-11896] - LoadIncrementalHFiles fails in secure mode if the namespace is specified
    * [HBASE-11898] - CoprocessorHost.Environment should cache class loader instance
    * [HBASE-11905] - Add orca to server UIs and update logo.
    * [HBASE-11921] - Minor fixups that come of testing branch-1
    * [HBASE-11932] - Stop the html-single from building a html-single of every chapter and cluttering the docbkx directory
    * [HBASE-11936] - IsolationLevel must be attribute of a Query not a Scan
    * [HBASE-11946] - Get xref and API docs to build properly again
    * [HBASE-11947] - NoSuchElementException in balancer for master regions
    * [HBASE-11949] - Setting hfile.block.cache.size=0 doesn't actually disable blockcache
    * [HBASE-11959] - TestAssignmentManagerOnCluster is flaky
    * [HBASE-11972] - The "doAs user" used in the update to hbase:acl table RPC is incorrect
    * [HBASE-11976] - Server startcode is not checked for bulk region assignment
    * [HBASE-11984] - TestClassFinder failing on occasion
    * [HBASE-11989] - IntegrationTestLoadAndVerify cannot be configured anymore on distributed mode

** Improvement
    * [HBASE-2217] - VM OPTS for shell only
    * [HBASE-3270] - When we create the .version file, we should create it in a tmp location and then move it into place
    * [HBASE-4163] - Create Split Strategy for YCSB Benchmark
    * [HBASE-4495] - CatalogTracker has an identity crisis; needs to be cut-back in scope
    * [HBASE-5349] - Automagically tweak global memstore and block cache sizes based on workload
    * [HBASE-5923] - Cleanup checkAndXXX logic
    * [HBASE-6626] - Add a chapter on HDFS in the troubleshooting section of the HBase reference guide.
    * [HBASE-6990] - Pretty print TTL
    * [HBASE-7088] - Duplicate code in RowCounter
    * [HBASE-7849] - Provide administrative limits around bulkloads of files into a single region
    * [HBASE-7910] - Dont use reflection for security
    * [HBASE-7987] - Snapshot Manifest file instead of multiple empty files
    * [HBASE-8076] - add better doc for HBaseAdmin#offline API.
    * [HBASE-8298] - In shell, provide alias of 'desc' for 'describe'
    * [HBASE-8315] - Documentation should have more information of LRU Stats
    * [HBASE-8332] - Add truncate as HMaster method
    * [HBASE-8495] - Change ownership of the directory to bulk load
    * [HBASE-8604] - improve reporting of incorrect peer address in replication
    * [HBASE-8755] - A new write thread model for HLog to improve the overall HBase write throughput
    * [HBASE-8763] - Combine MVCC and SeqId
    * [HBASE-8807] - HBase MapReduce Job-Launch Documentation Misplaced
    * [HBASE-8970] - [book] Filter language documentation is hidden
    * [HBASE-9343] - Implement stateless scanner for Stargate
    * [HBASE-9345] - Add support for specifying filters in scan
    * [HBASE-9426] - Make custom distributed barrier procedure pluggable
    * [HBASE-9501] - Provide throttling for replication
    * [HBASE-9524] - Multi row get does not return any results even if any one of the rows specified in the query is missing and improve exception handling
    * [HBASE-9542] - Have Get and MultiGet do cellblocks, currently they are pb all the time
    * [HBASE-9829] - make the compaction logging less confusing
    * [HBASE-9857] - Blockcache prefetch option
    * [HBASE-9866] - Support the mode where REST server authorizes proxy users
    * [HBASE-9892] - Add info port to ServerName to support multi instances in a node
    * [HBASE-9999] - Add support for small reverse scan
    * [HBASE-10010] - eliminate the put latency spike on the new log file beginning
    * [HBASE-10048] - Add hlog number metric in regionserver
    * [HBASE-10074] - consolidate and improve capacity/sizing documentation
    * [HBASE-10086] - [book] document the  HBase canary tool usage in the HBase Book
    * [HBASE-10116] - SlabCache metrics improvements
    * [HBASE-10128] - Improve the copy table doc to include information about versions
    * [HBASE-10141] - instead of putting expired store files thru compaction, just archive them
    * [HBASE-10157] - Provide CP hook post log replay
    * [HBASE-10164] - Allow heapsize of different units to be passed as HBASE_HEAPSIZE
    * [HBASE-10173] - Need HFile version check in security coprocessors
    * [HBASE-10175] - 2-thread ChaosMonkey steps on its own toes
    * [HBASE-10202] - Documentation is lacking information about rolling-restart.sh script.
    * [HBASE-10211] - Improve AccessControl documentation in hbase book
    * [HBASE-10213] - Add read log size per second metrics for replication source
    * [HBASE-10228] - Support setCellVisibility and setAuthorizations in Shell
    * [HBASE-10229] - Support OperationAttributes in Increment and Append in Shell
    * [HBASE-10239] - Improve determinism and debugability of TestAccessController
    * [HBASE-10252] - Don't write back to WAL/memstore when Increment amount is zero (mostly for query rather than update intention)
    * [HBASE-10263] - make LruBlockCache single/multi/in-memory ratio user-configurable and provide preemptive mode for in-memory type block
    * [HBASE-10265] - Upgrade to commons-logging 1.1.3
    * [HBASE-10277] - refactor AsyncProcess
    * [HBASE-10289] - Avoid random port usage by default JMX Server. Create Custome JMX server
    * [HBASE-10323] - Auto detect data block encoding in HFileOutputFormat
    * [HBASE-10324] - refactor deferred-log-flush/Durability related interface/code/naming to align with changed semantic of the new write thread model
    * [HBASE-10331] - Insure security tests use SecureTestUtil methods for grants
    * [HBASE-10344] - Improve write performance by ignoring sync to hdfs when an asyncer's writes have been synced by other asyncer
    * [HBASE-10346] - Add Documentation for stateless scanner
    * [HBASE-10368] - Add Mutation.setWriteToWAL() back to 0.98
    * [HBASE-10373] - Add more details info for ACL group in HBase book
    * [HBASE-10389] - Add namespace help info in table related shell commands
    * [HBASE-10395] - endTime won't be set in VerifyReplication if startTime is not set
    * [HBASE-10419] - Add multiget support to PerformanceEvaluation
    * [HBASE-10423] - Report back the message of split or rollback failure to the master
    * [HBASE-10427] - clean up HRegionLocation/ServerName usage
    * [HBASE-10430] - Support compressTags in shell for enabling tag encoding
    * [HBASE-10471] - Remove HTD.isAsyncLogFlush() from trunk
    * [HBASE-10479] - HConnection interface is public but is used internally, and contains a bunch of methods
    * [HBASE-10487] - Avoid allocating new KeyValue and according bytes-copying for appended kvs which don't have existing values
    * [HBASE-10498] - Add new APIs to load balancer interface
    * [HBASE-10511] - Add latency percentiles on PerformanceEvaluation
    * [HBASE-10518] - DirectMemoryUtils.getDirectMemoryUsage spams when none is configured
    * [HBASE-10569] - Co-locate meta and master
    * [HBASE-10570] - Allow overrides of Surefire secondPartForkMode and testFailureIgnore
    * [HBASE-10589] - Reduce unnecessary TestRowProcessorEndpoint resource usage
    * [HBASE-10590] - Update contents about tracing in the Reference Guide
    * [HBASE-10591] - Sanity check table configuration in createTable
    * [HBASE-10592] - Refactor PerformanceEvaluation tool
    * [HBASE-10597] - IOEngine#read() should return the number of bytes transferred
    * [HBASE-10599] - Replace System.currentMillis() with EnvironmentEdge.currentTimeMillis in memstore flusher and related places
    * [HBASE-10603] - Deprecate RegionSplitter CLI tool
    * [HBASE-10615] - Make LoadIncrementalHFiles skip reference files
    * [HBASE-10638] - Improve error message when there is no region server available for move
    * [HBASE-10641] - Configurable Bucket Sizes in bucketCache
    * [HBASE-10663] - Some code cleanup of class Leases and ScannerListener.leaseExpired
    * [HBASE-10678] - Make verifyrep tool implement toolrunner
    * [HBASE-10690] - Drop Hadoop-1 support
    * [HBASE-10693] - Correct declarations of Atomic* fields from 'volatile' to 'final'
    * [HBASE-10744] - AM#CloseRegion no need to retry on FailedServerException
    * [HBASE-10746] - Bump the version of HTrace to 3.0
    * [HBASE-10752] - Port HBASE-10270 'Remove DataBlockEncoding from BlockCacheKey' to trunk
    * [HBASE-10769] - hbase/bin/hbase-cleanup.sh has wrong usage string
    * [HBASE-10771] - Primitive type put/get APIs in ByteRange
    * [HBASE-10785] - Metas own location should be cached
    * [HBASE-10788] - Add 99th percentile of latency in PE
    * [HBASE-10797] - Add support for -h and --help to rolling_restart.sh and fix the usage string output
    * [HBASE-10813] - Possible over-catch of exceptions
    * [HBASE-10823] - Resolve LATEST_TIMESTAMP to current server time before scanning for ACLs
    * [HBASE-10835] - DBE encode path improvements
    * [HBASE-10842] - Some loggers not declared static final
    * [HBASE-10861] - Extend ByteRange to create Mutable and Immutable ByteRange
    * [HBASE-10871] - Indefinite OPEN/CLOSE wait on busy RegionServers
    * [HBASE-10873] - Control number of regions assigned to backup masters
    * [HBASE-10883] - Restrict the universe of labels and authorizations
    * [HBASE-10884] - [REST] Do not disable block caching when scanning
    * [HBASE-10885] - Support visibility expressions on Deletes
    * [HBASE-10887] - tidy ThriftUtilities format
    * [HBASE-10892] - [Shell] Add support for globs in user_permission
    * [HBASE-10902] - Make Secure Bulk Load work across remote secure clusters
    * [HBASE-10911] - ServerShutdownHandler#toString shows meaningless message
    * [HBASE-10916] - [VisibilityController] Stackable ScanLabelGenerators
    * [HBASE-10923] - Control where to put meta region
    * [HBASE-10925] - Do not OOME, throw RowTooBigException instead
    * [HBASE-10926] - Use global procedure to flush table memstore cache
    * [HBASE-10934] - Provide Admin interface to abstract HBaseAdmin
    * [HBASE-10950] - Add  a configuration point for MaxVersion of Column Family
    * [HBASE-10951] - Use PBKDF2 to generate test encryption keys in the shell
    * [HBASE-10952] - [REST] Let the user turn off block caching if desired
    * [HBASE-10960] - Enhance HBase Thrift 1 to include "append" and "checkAndPut" operations
    * [HBASE-10984] - Add description about setting up htrace-zipkin to documentation
    * [HBASE-11000] - Add autoflush option to PerformanceEvaluation
    * [HBASE-11001] - Shell support for granting cell permissions for testing
    * [HBASE-11002] - Shell support for changing cell visibility for testing
    * [HBASE-11004] - Extend traces through FSHLog#sync
    * [HBASE-11007] - BLOCKCACHE in schema descriptor seems not aptly named
    * [HBASE-11008] - Align bulk load, flush, and compact to require Action.CREATE
    * [HBASE-11026] - Provide option to filter out all rows in PerformanceEvaluation tool
    * [HBASE-11044] - [Shell] Show groups for user in 'whoami' output
    * [HBASE-11047] - Remove TimeoutMontior
    * [HBASE-11048] - Support setting custom priority per client RPC
    * [HBASE-11068] - Update code to use Admin factory method instead of constructor
    * [HBASE-11074] - Have PE emit histogram stats as it runs rather than dump once at end of test
    * [HBASE-11083] - ExportSnapshot should provide capability to limit bandwidth consumption
    * [HBASE-11086] - Add htrace support for PerfEval
    * [HBASE-11119] - Update ExportSnapShot to optionally not use a tmp file on external file system
    * [HBASE-11123] - Upgrade instructions from 0.94 to 0.98
    * [HBASE-11126] - Add RegionObserver pre hooks that operate under row lock
    * [HBASE-11128] - Add -target option to ExportSnapshot to export with a different name
    * [HBASE-11134] - Add a -list-snapshots option to SnapshotInfo
    * [HBASE-11136] - Add permission check to roll WAL writer
    * [HBASE-11137] - Add mapred.TableSnapshotInputFormat
    * [HBASE-11151] - move tracing modules from hbase-server to hbase-common
    * [HBASE-11167] - Avoid usage of java.rmi package Exception in MemStore
    * [HBASE-11201] - Enable global procedure members to return values to procedure master
    * [HBASE-11211] - LoadTestTool option for specifying number of regions per server
    * [HBASE-11219] - HRegionServer#createRegionLoad() should reuse RegionLoad.Builder instance when called in a loop
    * [HBASE-11220] - Add listeners to ServerManager and AssignmentManager
    * [HBASE-11240] - Print hdfs pipeline when hlog's sync is slow
    * [HBASE-11259] - Compression.java different compressions load system classpath differently causing errors
    * [HBASE-11304] - Enable HBaseAdmin.execProcedure to return data from procedure execution
    * [HBASE-11305] - Remove bunch of unused imports in HConnectionManager
    * [HBASE-11315] - Keeping MVCC for configurable longer time
    * [HBASE-11319] - No need to use favored node mapping initialization to find all regions
    * [HBASE-11326] - Use an InputFormat for ExportSnapshot
    * [HBASE-11331] - [blockcache] lazy block decompression
    * [HBASE-11344] - Hide row keys and such from the web UIs
    * [HBASE-11348] - Make frequency and sleep times of  chaos monkeys configurable
    * [HBASE-11349] - [Thrift] support authentication/impersonation
    * [HBASE-11350] - [PE] Allow random value size
    * [HBASE-11355] - a couple of callQueue related improvements
    * [HBASE-11362] - Minor improvements to LoadTestTool and PerformanceEvaluation
    * [HBASE-11370] - SSH doesn't need to scan meta if not using ZK for assignment
    * [HBASE-11376] - Presplit table in IntegrationTestBigLinkedList's Generator tool
    * [HBASE-11390] - PerformanceEvaluation: add an option to use a single connection
    * [HBASE-11398] - Print the stripes' state with file size info
    * [HBASE-11407] - hbase-client should not require Jackson for pure HBase queries be executed
    * [HBASE-11415] - [PE] Dump config before running test
    * [HBASE-11421] - HTableInterface javadoc correction
    * [HBASE-11434] - [AccessController] Disallow inbound cells with reserved tags
    * [HBASE-11436] - Support start Row and stop Row in HBase Export
    * [HBASE-11437] - Modify cell tag handling code to treat the length as unsigned
    * [HBASE-11438] - [Visibility Controller] Support UTF8 character as Visibility Labels
    * [HBASE-11440] - Make KeyValueCodecWithTags as the default codec for replication in trunk
    * [HBASE-11444] - Remove use of reflection for User#getShortName
    * [HBASE-11446] - Reduce the frequency of RNG calls in SecureWALCellCodec#EncryptedKvEncoder
    * [HBASE-11450] - Improve file size info in SnapshotInfo tool
    * [HBASE-11452] - add getUserPermission feature in AccessControlClient as client API
    * [HBASE-11473] - Add BaseWALObserver class
    * [HBASE-11474] - [Thrift2] support authentication/impersonation
    * [HBASE-11491] - Add an option to sleep randomly during the tests with the PE tool
    * [HBASE-11497] - Expose RpcScheduling implementations as LimitedPrivate interfaces
    * [HBASE-11513] - Combine SingleMultiple Queue RpcExecutor into a single class
    * [HBASE-11516] - Track time spent in executing coprocessors in each region.
    * [HBASE-11553] - Abstract visibility label related services into an interface
    * [HBASE-11566] - make ExportSnapshot extendable by removing 'final'
    * [HBASE-11583] - Refactoring out the configuration changes for enabling VisibilityLabels in the unit tests.
    * [HBASE-11623] - mutateRowsWithLocks might require updatesLock.readLock with waitTime=0
    * [HBASE-11630] - Refactor TestAdmin to use Admin interface instead of HBaseAdmin
    * [HBASE-11631] - Wait a little till server is online in assigning meta
    * [HBASE-11649] - Add shortcut commands to bin/hbase for test tools
    * [HBASE-11650] - Write hbase.id to a temporary location and move into place
    * [HBASE-11657] - Put HTable region methods in an interface
    * [HBASE-11664] - Build broken - TestVisibilityWithCheckAuths
    * [HBASE-11667] - Comment ClientScanner logic for NSREs.
    * [HBASE-11674] - LoadIncrementalHFiles should be more verbose after unrecoverable error
    * [HBASE-11679] - Replace "HTable" with "HTableInterface" where backwards-compatible
    * [HBASE-11696] - Make CombinedBlockCache resizable.
    * [HBASE-11697] - Improve the 'Too many blocks' message on UI blockcache status page
    * [HBASE-11701] - Start and end of memstore flush log should be on the same level
    * [HBASE-11702] - Better introspection of long running compactions
    * [HBASE-11706] - Set versions for VerifyReplication
    * [HBASE-11731] - Add option to only run a subset of the shell tests
    * [HBASE-11748] - Cleanup and add pool usage tracing to Compression
    * [HBASE-11749] - Better error logging when coprocessors loading has failed.
    * [HBASE-11754] - [Shell] Record table property SPLITS_FILE in descriptor
    * [HBASE-11757] - Provide a common base abstract class for both RegionObserver and MasterObserver
    * [HBASE-11774] - Avoid allocating unnecessary tag iterators
    * [HBASE-11777] - Find a way to set sequenceId on Cells on the server
    * [HBASE-11790] - Bulk load should use HFileOutputFormat2 in all cases
    * [HBASE-11805] - KeyValue to Cell Convert in WALEdit APIs
    * [HBASE-11810] - Access SSL Passwords through Credential Provider API
    * [HBASE-11821] - [ImportTSV] Abstract labels tags creation into pluggable Interface
    * [HBASE-11825] - Create Connection and ConnectionManager
    * [HBASE-11826] - Split each tableOrRegionName admin methods into two targetted methods
    * [HBASE-11828] - callers of SeverName.valueOf should use equals and not ==
    * [HBASE-11845] - HFile tool should implement Tool, disable blockcache by default
    * [HBASE-11846] - HStore#assertBulkLoadHFileOk should log if a full HFile verification will be performed during a bulkload
    * [HBASE-11847] - HFile tool should be able to print block headers
    * [HBASE-11865] - Result implements CellScannable; rather it should BE a CellScanner
    * [HBASE-11873] - Hbase Version CLI enhancement
    * [HBASE-11877] - Make TableSplit more readable
    * [HBASE-11891] - Introduce HBaseInterfaceAudience level to denote class names that appear in configs.
    * [HBASE-11897] - Add append and remove peer table-cfs cmds for replication

** New Feature
    * [HBASE-4089] - blockCache contents report
    * [HBASE-6104] - Require EXEC permission to call coprocessor endpoints
    * [HBASE-7667] - Support stripe compaction
    * [HBASE-7840] - Enhance the java it framework to start & stop a distributed hbase & hadoop cluster
    * [HBASE-8751] - Enable peer cluster to choose/change the ColumnFamilies/Tables it really want to replicate from a source cluster
    * [HBASE-9047] - Tool to handle finishing replication when the cluster is offline
    * [HBASE-10119] - Allow HBase coprocessors to clean up when they fail
    * [HBASE-10151] - No-op HeapMemoryTuner
    * [HBASE-10416] - Improvements to the import flow
    * [HBASE-10881] - Support reverse scan in thrift2
    * [HBASE-10935] - support snapshot policy where flush memstore can be skipped to prevent production cluster freeze
    * [HBASE-11724] - Add to RWQueueRpcExecutor the ability to split get and scan handlers
    * [HBASE-11885] - Provide a Dockerfile to easily build and run HBase from source
    * [HBASE-11909] - Region count listed by HMaster UI and hbck are different

** Task
    * [HBASE-4456] - [doc] Add a section about RS failover
    * [HBASE-4920] - We need a mascot, a totem
    * [HBASE-5697] - Audit HBase for usage of deprecated hadoop 0.20.x property names.
    * [HBASE-6139] - Add troubleshooting section for CentOS 6.2 page allocation failure issue
    * [HBASE-6192] - Document ACL matrix in the book
    * [HBASE-7394] - Document security config requirements from HBASE-7357
    * [HBASE-8035] - Add site target check to precommit tests
    * [HBASE-8844] - Document the removal of replication state AKA start/stop_replication
    * [HBASE-9580] - Document the meaning of @InterfaceAudience in hbase ref guide
    * [HBASE-9733] - Book should have individual Disqus comment per page
    * [HBASE-9875] - NamespaceJanitor chore is not used
    * [HBASE-10134] - Fix findbug warning in VisibilityController
    * [HBASE-10159] - Replaced deprecated interface Closeable
    * [HBASE-10206] - Explain tags in the hbase book
    * [HBASE-10246] - Wrap long lines in recently added source files
    * [HBASE-10364] - Allow configuration option for parent znode in LoadTestTool
    * [HBASE-10388] - Add export control notice in README
    * [HBASE-10439] - Document how to configure REST server impersonation
    * [HBASE-10473] - Add utility for adorning http Context
    * [HBASE-10601] - Upgrade hadoop dependency to 2.3.0 release
    * [HBASE-10609] - Remove filterKeyValue(Cell ignored) from FilterBase
    * [HBASE-10612] - Remove unnecessary dependency on org.eclipse.jdt:core
    * [HBASE-10670] - HBaseFsck#connect() should use new connection
    * [HBASE-10700] - IntegrationTestWithCellVisibilityLoadAndVerify should allow current user to be the admin
    * [HBASE-10740] - Upgrade zookeeper to 3.4.6 release
    * [HBASE-10786] - If snapshot verification fails with 'Regions moved', the message should contain the name of region causing the failure
    * [HBASE-10787] - TestHCM#testConnection* take too long
    * [HBASE-10821] - Make ColumnInterpreter#getValue() abstract
    * [HBASE-10824] - Enhance detection of protobuf generated code in line length check
    * [HBASE-10889] - test-patch.sh should exclude thrift generated code from long line detection
    * [HBASE-10906] - Change error log for NamingException in TableInputFormatBase to WARN level
    * [HBASE-10912] - setUp / tearDown in TestSCVFWithMiniCluster should be done once per run
    * [HBASE-10956] - Upgrade hadoop-2 dependency to 2.4.0
    * [HBASE-11016] - Remove Filter#filterRow(List)
    * [HBASE-11032] - Replace deprecated methods in FileSystem with their replacements
    * [HBASE-11050] - Replace empty catch block in TestHLog#testFailedToCreateHLogIfParentRenamed with @Test(expected=)
    * [HBASE-11076] - Update refguide on getting 0.94.x to run on hadoop 2.2.0+
    * [HBASE-11090] - Backport HBASE-11083 ExportSnapshot should provide capability to limit bandwidth consumption
    * [HBASE-11107] - Provide utility method equivalent to 0.92's Result.getBytes().getSize()
    * [HBASE-11154] - Document how to use Reverse Scan API
    * [HBASE-11199] - One-time effort to pretty-print the Docbook XML, to make further patch review easier
    * [HBASE-11203] - Clean up javadoc and findbugs warnings in trunk
    * [HBASE-11204] - Document bandwidth consumption limit feature for ExportSnapshot
    * [HBASE-11227] - Mention 8- and 16-bit fixed-with encodings in OrderedBytes docstring
    * [HBASE-11230] - Remove getRowOrBefore from HTableInterface and HTable
    * [HBASE-11317] - Expand unit testing to cover Mockito and MRUnit and give more examples
    * [HBASE-11364] - [BlockCache] Add a flag to cache data blocks in L1 if multi-tier cache
    * [HBASE-11600] - DataInputputStream and DoubleOutputStream are no longer being used
    * [HBASE-11604] - Disable co-locating meta/master by default
    * [HBASE-11621] - Make MiniDFSCluster run faster
    * [HBASE-11666] - Enforce JDK7 javac for builds on branch-1 and master
    * [HBASE-11682] - Explain hotspotting
    * [HBASE-11723] - Document all options of bin/hbase command
    * [HBASE-11735] - Document Configurable Bucket Sizes in bucketCache
    * [HBASE-11762] - Record the class name of Codec in WAL header
    * [HBASE-11800] - Coprocessor service methods in HTableInterface should be annotated public
    * [HBASE-11849] - Clean up orphaned private audience classes
    * [HBASE-11858] - Audit regionserver classes that are missing InterfaceAudience

** Test
    * [HBASE-8889] - TestIOFencing#testFencingAroundCompaction occasionally fails
    * [HBASE-9928] - TestHRegion should clean up test-data directory upon completion
    * [HBASE-9953] - PerformanceEvaluation: Decouple data size from client concurrency
    * [HBASE-10044] - test-patch.sh should accept documents by known file extensions
    * [HBASE-10130] - TestSplitLogManager#testTaskResigned fails sometimes
    * [HBASE-10180] - TestByteBufferIOEngine#testByteBufferIOEngine occasionally fails
    * [HBASE-10189] - Intermittent TestReplicationSyncUpTool failure
    * [HBASE-10301] - TestAssignmentManagerOnCluster#testOpenCloseRacing fails intermittently
    * [HBASE-10377] - Add test for HBASE-10370 Compaction in out-of-date Store causes region split failure
    * [HBASE-10394] - Test for Replication with tags
    * [HBASE-10406] - Column family option is not effective in IntegrationTestSendTraceRequests
    * [HBASE-10408] - Intermittent TestDistributedLogSplitting#testLogReplayForDisablingTable failure
    * [HBASE-10440] - integration tests fail due to nonce collisions
    * [HBASE-10465] - TestZKPermissionsWatcher.testPermissionsWatcher fails sometimes
    * [HBASE-10475] - TestRegionServerCoprocessorExceptionWithAbort may timeout due to concurrent lease removal
    * [HBASE-10480] - TestLogRollPeriod#testWithEdits may fail due to insufficient waiting
    * [HBASE-10543] - Two rare test failures with TestLogsCleaner and TestSplitLogWorker
    * [HBASE-10635] - thrift#TestThriftServer fails due to TTL validity check
    * [HBASE-10649] - TestMasterMetrics fails occasionally
    * [HBASE-10764] - TestLoadIncrementalHFilesSplitRecovery#testBulkLoadPhaseFailure takes too long
    * [HBASE-10767] - Load balancer may interfere with tests in TestHBaseFsck
    * [HBASE-10774] - Restore TestMultiTableInputFormat
    * [HBASE-10782] - Hadoop2 MR tests fail occasionally because of mapreduce.jobhistory.address is no set in job conf
    * [HBASE-10828] - TestRegionObserverInterface#testHBase3583 should wait for all regions to be assigned
    * [HBASE-10852] - TestDistributedLogSplitting#testDisallowWritesInRecovering occasionally fails
    * [HBASE-10867] - TestRegionPlacement#testRegionPlacement occasionally fails
    * [HBASE-10868] - TestAtomicOperation should close HRegion instance after each subtest
    * [HBASE-10988] - Properly wait for server in TestThriftServerCmdLine
    * [HBASE-11010] - TestChangingEncoding is unnecessarily slow
    * [HBASE-11019] - incCount() method should be properly stubbed in HConnectionTestingUtility#getMockedConnectionAndDecorate()
    * [HBASE-11037] - Race condition in TestZKBasedOpenCloseRegion
    * [HBASE-11051] - checkJavacWarnings in test-patch.sh should bail out early if there is compilation error
    * [HBASE-11057] - Improve TestShell coverage of grant and revoke comamnds
    * [HBASE-11104] - IntegrationTestImportTsv#testRunFromOutputCommitter misses credential initialization
    * [HBASE-11152] - test-patch.sh should be able to handle the case where $TERM is not defined
    * [HBASE-11166] - Categorize tests in hbase-prefix-tree module
    * [HBASE-11328] - testMoveRegion could fail
    * [HBASE-11345] - Add an option not to restore cluster after an IT test
    * [HBASE-11375] - Validate compile-protobuf profile in test-patch.sh
    * [HBASE-11404] - TestLogLevel should stop the server at the end
    * [HBASE-11443] - TestIOFencing#testFencingAroundCompactionAfterWALSync times out
    * [HBASE-11615] - TestZKLessAMOnCluster.testForceAssignWhileClosing failed on Jenkins
    * [HBASE-11713] - Adding hbase shell unit test coverage for visibility labels.
    * [HBASE-11918] - TestVisibilityLabelsWithDistributedLogReplay#testAddVisibilityLabelsOnRSRestart sometimes fails due to VisibilityController initialization not being recognized
    * [HBASE-11942] - Fix TestHRegionBusyWait
    * [HBASE-11966] - Minor error in TestHRegion.testCheckAndMutate_WithCorrectValue()

** Umbrella
    * [HBASE-7319] - Extend Cell usage through read path
    * [HBASE-9945] - Coprocessor loading and execution improvements
    * [HBASE-10909] - Abstract out ZooKeeper usage in HBase - phase 1

```
