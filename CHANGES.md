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

## Release 2.2.0 - Unreleased (as of 2019-03-04)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21082](https://issues.apache.org/jira/browse/HBASE-21082) | Reimplement assign/unassign related procedure metrics |  Critical | amv2, metrics |
| [HBASE-20587](https://issues.apache.org/jira/browse/HBASE-20587) | Replace Jackson with shaded thirdparty gson |  Major | dependencies |
| [HBASE-21727](https://issues.apache.org/jira/browse/HBASE-21727) | Simplify documentation around client timeout |  Minor | . |
| [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | Throw DNRIOE when connection or rpc client is closed |  Major | asyncclient, Client |
| [HBASE-21792](https://issues.apache.org/jira/browse/HBASE-21792) | Mark HTableMultiplexer as deprecated and remove it in 3.0.0 |  Major | Client |
| [HBASE-21657](https://issues.apache.org/jira/browse/HBASE-21657) | PrivateCellUtil#estimatedSerializedSizeOf has been the bottleneck in 100% scan case. |  Major | Performance |
| [HBASE-21560](https://issues.apache.org/jira/browse/HBASE-21560) | Return a new TableDescriptor for MasterObserver#preModifyTable to allow coprocessor modify the TableDescriptor |  Major | Coprocessors |
| [HBASE-21452](https://issues.apache.org/jira/browse/HBASE-21452) | Illegal character in hbase counters group name |  Major | spark |
| [HBASE-21158](https://issues.apache.org/jira/browse/HBASE-21158) | Empty qualifier cell should not be returned if it does not match QualifierFilter |  Critical | Filters |
| [HBASE-21223](https://issues.apache.org/jira/browse/HBASE-21223) | [amv2] Remove abort\_procedure from shell |  Critical | amv2, hbck2, shell |
| [HBASE-20881](https://issues.apache.org/jira/browse/HBASE-20881) | Introduce a region transition procedure to handle all the state transition for a region |  Major | amv2, proc-v2 |
| [HBASE-20884](https://issues.apache.org/jira/browse/HBASE-20884) | Replace usage of our Base64 implementation with java.util.Base64 |  Major | . |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-20886](https://issues.apache.org/jira/browse/HBASE-20886) | [Auth] Support keytab login in hbase client |  Critical | asyncclient, Client, security |
| [HBASE-17942](https://issues.apache.org/jira/browse/HBASE-17942) | Disable region splits and merges per table |  Major | . |
| [HBASE-21753](https://issues.apache.org/jira/browse/HBASE-21753) | Support getting the locations for all the replicas of a region |  Major | Client |
| [HBASE-20636](https://issues.apache.org/jira/browse/HBASE-20636) | Introduce two bloom filter type : ROWPREFIX\_FIXED\_LENGTH and ROWPREFIX\_DELIMITED |  Major | HFile, regionserver, scan |
| [HBASE-20649](https://issues.apache.org/jira/browse/HBASE-20649) | Validate HFiles do not have PREFIX\_TREE DataBlockEncoding |  Minor | Operability, tooling |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-21255](https://issues.apache.org/jira/browse/HBASE-21255) | [acl] Refactor TablePermission into three classes (Global, Namespace, Table) |  Major | . |
| [HBASE-21410](https://issues.apache.org/jira/browse/HBASE-21410) | A helper page that help find all problematic regions and procedures |  Major | . |
| [HBASE-20734](https://issues.apache.org/jira/browse/HBASE-20734) | Colocate recovered edits directory with hbase.wal.dir |  Major | MTTR, Recovery, wal |
| [HBASE-20401](https://issues.apache.org/jira/browse/HBASE-20401) | Make \`MAX\_WAIT\` and \`waitIfNotFinished\` in CleanerContext configurable |  Minor | master |
| [HBASE-21481](https://issues.apache.org/jira/browse/HBASE-21481) | [acl] Superuser's permissions should not be granted or revoked by any non-su global admin |  Major | . |
| [HBASE-21967](https://issues.apache.org/jira/browse/HBASE-21967) | Split TestServerCrashProcedure and TestServerCrashProcedureWithReplicas |  Major | . |
| [HBASE-21867](https://issues.apache.org/jira/browse/HBASE-21867) | Support multi-threads in HFileArchiver |  Major | . |
| [HBASE-21667](https://issues.apache.org/jira/browse/HBASE-21667) | Move to latest ASF Parent POM |  Minor | build |
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
| [HBASE-21565](https://issues.apache.org/jira/browse/HBASE-21565) | Delete dead server from dead server list too early leads to concurrent Server Crash Procedures(SCP) for a same server |  Critical | . |
| [HBASE-21740](https://issues.apache.org/jira/browse/HBASE-21740) | NPE happens while shutdown the RS |  Major | . |
| [HBASE-21866](https://issues.apache.org/jira/browse/HBASE-21866) | Do not move the table to null rsgroup when creating an existing table |  Major | proc-v2, rsgroup |
| [HBASE-21983](https://issues.apache.org/jira/browse/HBASE-21983) | Should track the scan metrics in AsyncScanSingleRegionRpcRetryingCaller if scan metrics is enabled |  Major | asyncclient, Client |
| [HBASE-21980](https://issues.apache.org/jira/browse/HBASE-21980) | Fix typo in AbstractTestAsyncTableRegionReplicasRead |  Major | test |
| [HBASE-21487](https://issues.apache.org/jira/browse/HBASE-21487) | Concurrent modify table ops can lead to unexpected results |  Major | . |
| [HBASE-20724](https://issues.apache.org/jira/browse/HBASE-20724) | Sometimes some compacted storefiles are still opened after region failover |  Critical | . |
| [HBASE-21961](https://issues.apache.org/jira/browse/HBASE-21961) | Infinite loop in AsyncNonMetaRegionLocator if there is only one region and we tried to locate before a non empty row |  Critical | asyncclient, Client |
| [HBASE-21960](https://issues.apache.org/jira/browse/HBASE-21960) | RESTServletContainer not configured for REST Jetty server |  Blocker | REST |
| [HBASE-21943](https://issues.apache.org/jira/browse/HBASE-21943) | The usage of RegionLocations.mergeRegionLocations is wrong for async client |  Critical | asyncclient, Client |
| [HBASE-21947](https://issues.apache.org/jira/browse/HBASE-21947) | TestShell is broken after we remove the jackson dependencies |  Major | dependencies, shell |
| [HBASE-21942](https://issues.apache.org/jira/browse/HBASE-21942) | [UI] requests per second is incorrect in rsgroup page(rsgroup.jsp) |  Minor | . |
| [HBASE-21922](https://issues.apache.org/jira/browse/HBASE-21922) | BloomContext#sanityCheck may failed when use ROWPREFIX\_DELIMITED bloom filter |  Major | . |
| [HBASE-21929](https://issues.apache.org/jira/browse/HBASE-21929) | The checks at the end of TestRpcClientLeaks are not executed |  Major | test |
| [HBASE-21938](https://issues.apache.org/jira/browse/HBASE-21938) | Add a new ClusterMetrics.Option SERVERS\_NAME to only return the live region servers's name without metrics |  Major | . |
| [HBASE-21928](https://issues.apache.org/jira/browse/HBASE-21928) | Deprecated HConstants.META\_QOS |  Major | Client, rpc |
| [HBASE-21915](https://issues.apache.org/jira/browse/HBASE-21915) | FileLink$FileLinkInputStream doesn't implement CanUnbuffer |  Major | Filesystem Integration |
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
| [HBASE-21621](https://issues.apache.org/jira/browse/HBASE-21621) | Reversed scan does not return expected  number of rows |  Critical | scan |
| [HBASE-21620](https://issues.apache.org/jira/browse/HBASE-21620) | Problem in scan query when using more than one column prefix filter in some cases. |  Major | scan |
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
| [HBASE-21492](https://issues.apache.org/jira/browse/HBASE-21492) | CellCodec Written To WAL Before It's Verified |  Critical | wal |
| [HBASE-21507](https://issues.apache.org/jira/browse/HBASE-21507) | Compaction failed when execute AbstractMultiFileWriter.beforeShipped() method |  Major | regionserver |
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
| [HBASE-21206](https://issues.apache.org/jira/browse/HBASE-21206) | Scan with batch size may return incomplete cells |  Critical | scan |
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
| [HBASE-20791](https://issues.apache.org/jira/browse/HBASE-20791) | RSGroupBasedLoadBalancer#setClusterMetrics should pass ClusterMetrics to its internalBalancer |  Major | rsgroup |
| [HBASE-20770](https://issues.apache.org/jira/browse/HBASE-20770) | WAL cleaner logs way too much; gets clogged when lots of work to do |  Critical | logging |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
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
| [HBASE-21886](https://issues.apache.org/jira/browse/HBASE-21886) | Run ITBLL for branch-2.2 |  Major | . |
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
| [HBASE-21764](https://issues.apache.org/jira/browse/HBASE-21764) | Size of in-memory compaction thread pool should be configurable |  Major | in-memory-compaction |
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
| [HBASE-20914](https://issues.apache.org/jira/browse/HBASE-20914) | Trim Master memory usage |  Major | master |
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
