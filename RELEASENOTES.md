# RELEASENOTES
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
# HBASE  2.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-15728](https://issues.apache.org/jira/browse/HBASE-15728) | *Major* | **Add remaining per-table region / store / flush / compaction related metrics**

Adds below flush, split, and compaction metrics

 +  // split related metrics
 +  private MutableFastCounter splitRequest;
 +  private MutableFastCounter splitSuccess;
 +  private MetricHistogram splitTimeHisto;
 +
 +  // flush related metrics
 +  private MetricHistogram flushTimeHisto;
 +  private MetricHistogram flushMemstoreSizeHisto;
 +  private MetricHistogram flushOutputSizeHisto;
 +  private MutableFastCounter flushedMemstoreBytes;
 +  private MutableFastCounter flushedOutputBytes;
 +
 +  // compaction related metrics
 +  private MetricHistogram compactionTimeHisto;
 +  private MetricHistogram compactionInputFileCountHisto;
 +  private MetricHistogram compactionInputSizeHisto;
 +  private MetricHistogram compactionOutputFileCountHisto;
 +  private MetricHistogram compactionOutputSizeHisto;
 +  private MutableFastCounter compactedInputBytes;
 +  private MutableFastCounter compactedOutputBytes;
 +
 +  private MetricHistogram majorCompactionTimeHisto;
 +  private MetricHistogram majorCompactionInputFileCountHisto;
 +  private MetricHistogram majorCompactionInputSizeHisto;
 +  private MetricHistogram majorCompactionOutputFileCountHisto;
 +  private MetricHistogram majorCompactionOutputSizeHisto;
 +  private MutableFastCounter majorCompactedInputBytes;
 +  private MutableFastCounter majorCompactedOutputBytes;


---

* [HBASE-20886](https://issues.apache.org/jira/browse/HBASE-20886) | *Critical* | **[Auth] Support keytab login in hbase client**

From 2.2.0, hbase supports client login via keytab. To use this feature, client should specify \`hbase.client.keytab.file\` and \`hbase.client.keytab.principal\` in hbase-site.xml, then the connection will contain the needed credentials which be renewed periodically to communicate with kerberized hbase cluster.


---

* [HBASE-21410](https://issues.apache.org/jira/browse/HBASE-21410) | *Major* | **A helper page that help find all problematic regions and procedures**

After HBASE-21410, we add a helper page to Master UI. This helper page is mainly to help HBase operator quickly found all regions and pids that are get stuck.
There are 2 entries to get in this page.
One is showing in the Regions in Transition section, it made "num region(s) in transition" a link that you can click and check all regions in transition and their related procedure IDs.
The other one is showing in the table details section, it made the number of CLOSING or OPENING regions a link, which you can click and check regions and related procedure IDs of CLOSING or OPENING regions of a certain table.
In this helper page, not only you can see all regions and related procedures, there are 2 buttons at the top which will show these regions or procedure IDs in text format. This is mainly aim to help operator to easily copy and paste all problematic procedure IDs and encoded region names to HBCK2's command line, by which we HBase operator can bypass these procedures or assign these regions.


---

* [HBASE-21588](https://issues.apache.org/jira/browse/HBASE-21588) | *Major* | **Procedure v2 wal splitting implementation**

After HBASE-21588, we introduce a new way to do WAL splitting coordination by procedure framework. This can simplify the process of WAL splitting and no need to connect zookeeper any more.
During ServerCrashProcedure, it will create a SplitWALProcedure for each WAL that need to split. Then each SplitWALProcedure will spawn a SplitWALRemoteProcedure to send the request to regionserver.
At the RegionServer side, whole process is handled by SplitWALCallable. It split the WAL and return the result to master.
According to my test, this patch has a better performance as the number of WALs that need to split increase. And it can relieve the pressure on zookeeper.


---

* [HBASE-20734](https://issues.apache.org/jira/browse/HBASE-20734) | *Major* | **Colocate recovered edits directory with hbase.wal.dir**

Previously the recovered.edits directory was under the root directory. This JIRA moves the recovered.edits directory to be under the hbase.wal.dir if set. It also adds a check for any recovered.edits found under the root directory for backwards compatibility. This gives improvements when a faster media(like SSD) or more local FileSystem is used for the hbase.wal.dir than the root dir.


---

* [HBASE-20401](https://issues.apache.org/jira/browse/HBASE-20401) | *Minor* | **Make \`MAX\_WAIT\` and \`waitIfNotFinished\` in CleanerContext configurable**

When oldwals (and hfile) cleaner cleans stale wals (and hfiles), it will periodically check and wait the clean results from filesystem, the total wait time will be no more than a max time.

The periodically wait and check configurations are hbase.oldwals.cleaner.thread.check.interval.msec (default is 500 ms) and hbase.regionserver.hfilecleaner.thread.check.interval.msec (default is 1000 ms).

Meanwhile, The max time configurations are hbase.oldwals.cleaner.thread.timeout.msec and hbase.regionserver.hfilecleaner.thread.timeout.msec, they are set to 60 seconds by default.

All support dynamic configuration.

e.g. in the oldwals cleaning scenario, one may consider tuning hbase.oldwals.cleaner.thread.timeout.msec and hbase.oldwals.cleaner.thread.check.interval.msec

1. While deleting a oldwal never complete (strange but possible), then delete file task needs to wait for a max of 60 seconds. Here, 60 seconds might be too long, or the opposite way is to increase more than 60 seconds in the use cases of slow file delete.
2. The check and wait of a file delete is set to default in the period of 500 milliseconds, one might want to tune this checking period to a short interval to check more frequently or to a longer interval to avoid checking too often to manage their delete file task checking period (the longer interval may be use to avoid checking too fast while using a high latency storage).


---

* [HBASE-21481](https://issues.apache.org/jira/browse/HBASE-21481) | *Major* | **[acl] Superuser's permissions should not be granted or revoked by any non-su global admin**

HBASE-21481 improves the quality of access control, by strengthening the protection of super users's privileges.


---

* [HBASE-21082](https://issues.apache.org/jira/browse/HBASE-21082) | *Critical* | **Reimplement assign/unassign related procedure metrics**

Now we have four types of RIT procedure metrics, assign, unassign, move, reopen. The meaning of assign/unassign is changed, as we will not increase the unassign metric and then the assign metric when moving a region.
Also introduced two new procedure metrics, open and close, which are used to track the open/close region calls to region server. We may send open/close multiple times to finish a RIT since we may retry multiple times.


---

* [HBASE-20724](https://issues.apache.org/jira/browse/HBASE-20724) | *Critical* | **Sometimes some compacted storefiles are still opened after region failover**

Problem: This is an old problem since HBASE-2231. The compaction event marker was only writed to WAL. But after flush, the WAL may be archived, which means an useful compaction event marker be deleted, too. So the compacted store files cannot be archived when region open and replay WAL.

Solution: After this jira, the compaction event tracker will be writed to HFile. When region open and load store files, read the compaction evnet tracker from HFile and archive the compacted store files which still exist.


---

* [HBASE-21820](https://issues.apache.org/jira/browse/HBASE-21820) | *Major* | **Implement CLUSTER quota scope**

HBase contains two quota scopes: MACHINE and CLUSTER. Before this patch, set quota operations did not expose scope option to client api and use MACHINE as default, CLUSTER scope can not be set and used.
Shell commands are as follows:
set\_quota, TYPE =\> THROTTLE, TABLE =\> 't1', LIMIT =\> '10req/sec'

This issue implements CLUSTER scope in a simple way: For user, namespace, user over namespace quota, use [ClusterLimit / RSNum] as machine limit. For table and user over table quota, use [ClusterLimit / TotalTableRegionNum \* MachineTableRegionNum] as machine limit.
After this patch, user can set CLUSTER scope quota, but MACHINE is still default if user ignore scope.
Shell commands are as follows:
set\_quota, TYPE =\> THROTTLE, TABLE =\> 't1', LIMIT =\> '10req/sec'
set\_quota, TYPE =\> THROTTLE, TABLE =\> 't1', LIMIT =\> '10req/sec', SCOPE =\> MACHINE
set\_quota, TYPE =\> THROTTLE, TABLE =\> 't1', LIMIT =\> '10req/sec', SCOPE =\> CLUSTER


---

* [HBASE-21057](https://issues.apache.org/jira/browse/HBASE-21057) | *Minor* | **upgrade to latest spotbugs**

Change spotbugs version to 3.1.11.


---

* [HBASE-21922](https://issues.apache.org/jira/browse/HBASE-21922) | *Major* | **BloomContext#sanityCheck may failed when use ROWPREFIX\_DELIMITED bloom filter**

Remove bloom filter type ROWPREFIX\_DELIMITED. May add it back when find a better solution.


---

* [HBASE-21783](https://issues.apache.org/jira/browse/HBASE-21783) | *Major* | **Support exceed user/table/ns throttle quota if region server has available quota**

Support enable or disable exceed throttle quota. Exceed throttle quota means, user can over consume user/namespace/table quota if region server has additional available quota because other users don't consume at the same time.
Use the following shell commands to enable/disable exceed throttle quota: enable\_exceed\_throttle\_quota
disable\_exceed\_throttle\_quota
There are two limits when enable exceed throttle quota:
1. Must set at least one read and one write region server throttle quota;
2. All region server throttle quotas must be in seconds time unit. Because once previous requests exceed their quota and consume region server quota, quota in other time units may be refilled in a long time, this may affect later requests.


---

* [HBASE-20587](https://issues.apache.org/jira/browse/HBASE-20587) | *Major* | **Replace Jackson with shaded thirdparty gson**

Remove jackson dependencies from most hbase modules except hbase-rest, use shaded gson instead. The output json will be a bit different since jackson can use getter/setter, but gson will always use the fields.


---

* [HBASE-21928](https://issues.apache.org/jira/browse/HBASE-21928) | *Major* | **Deprecated HConstants.META\_QOS**

Mark HConstants.META\_QOS as deprecated. It is for internal use only, which is the highest priority. You should not try to set a priority greater than or equal to this value, although it is no harm but also useless.


---

* [HBASE-17942](https://issues.apache.org/jira/browse/HBASE-17942) | *Major* | **Disable region splits and merges per table**

This patch adds the ability to disable split and/or merge for a table (By default, split and merge are enabled for a table).


---

* [HBASE-21636](https://issues.apache.org/jira/browse/HBASE-21636) | *Major* | **Enhance the shell scan command to support missing scanner specifications like ReadType, IsolationLevel etc.**

Allows shell to set Scan options previously not exposed. See additions as part of the scan help by typing following hbase shell:

hbase\> help 'scan'


---

* [HBASE-21201](https://issues.apache.org/jira/browse/HBASE-21201) | *Major* | **Support to run VerifyReplication MR tool without peerid**

We can specify peerQuorumAddress instead of peerId in VerifyReplication tool. So it no longer requires peerId to be setup when using this tool.

For example:
hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication zk1,zk2,zk3:2181/hbase testTable


---

* [HBASE-21838](https://issues.apache.org/jira/browse/HBASE-21838) | *Major* | **Create a special ReplicationEndpoint just for verifying the WAL entries are fine**

Introduce a VerifyWALEntriesReplicationEndpoint which replicates nothing but only verifies if all the cells are valid.
It can be used to capture bugs for writing WAL, as most times we will not read the WALs again after writing it if there are no region server crashes.


---

* [HBASE-21727](https://issues.apache.org/jira/browse/HBASE-21727) | *Minor* | **Simplify documentation around client timeout**

Deprecated HBaseConfiguration#getInt(Configuration, String, String, int) method and removed it from 3.0.0 version.


---

* [HBASE-21764](https://issues.apache.org/jira/browse/HBASE-21764) | *Major* | **Size of in-memory compaction thread pool should be configurable**

Introduced an new config key in this issue: hbase.regionserver.inmemory.compaction.pool.size. the default value would be 10.  you can configure this to set the pool size of in-memory compaction pool. Note that all memstores in one region server will share the same pool, so if you have many regions in one region server,  you need to set this larger to compact faster for better read performance.


---

* [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | *Major* | **Throw DNRIOE when connection or rpc client is closed**

Make StoppedRpcClientException extend DoNotRetryIOException.


---

* [HBASE-21739](https://issues.apache.org/jira/browse/HBASE-21739) | *Major* | **Move grant/revoke from regionserver to master**

To implement user permission control in Precedure V2, move grant and revoke method from AccessController to master firstly.
Mark AccessController#grant and AccessController#revoke as deprecated and please use Admin#grant and Admin#revoke instead.


---

* [HBASE-21791](https://issues.apache.org/jira/browse/HBASE-21791) | *Blocker* | **Upgrade thrift dependency to 0.12.0**

IMPORTANT: Due to security issues, all users who use hbase thrift should avoid using releases which do not have this fix.

The effect releases are:
2.1.x: 2.1.2 and below
2.0.x: 2.0.4 and below
1.x: 1.4.x and below

If you are using the effect releases above, please consider upgrading to a newer release ASAP.


---

* [HBASE-21792](https://issues.apache.org/jira/browse/HBASE-21792) | *Major* | **Mark HTableMultiplexer as deprecated and remove it in 3.0.0**

HTableMultiplexer exposes the implementation class, and it is incomplete, so we mark it as deprecated and remove it in 3.0.0 release.

There is no direct replacement for HTableMultiplexer, please use BufferedMutator if you want to batch mutations to a table.


---

* [HBASE-21782](https://issues.apache.org/jira/browse/HBASE-21782) | *Major* | **LoadIncrementalHFiles should not be IA.Public**

Introduce a BulkLoadHFiles interface which is marked as IA.Public, for doing bulk load programmatically.
Introduce a BulkLoadHFilesTool which extends BulkLoadHFiles, and is marked as IA.LimitedPrivate(TOOLS), for using from command line.
The old LoadIncrementalHFiles is deprecated and will be removed in 3.0.0.


---

* [HBASE-21762](https://issues.apache.org/jira/browse/HBASE-21762) | *Major* | **Move some methods in ClusterConnection to Connection**

Move the two getHbck method from ClusterConnection to Connection, and mark the methods as IA.LimitedPrivate(HBCK), as ClusterConnection is IA.Private and should not be depended by HBCK2.

Add a clearRegionLocationCache method in Connection to clear the region location cache for all the tables. As in RegionLocator, most of the methods have a 'reload' parameter, which implicitly tells user that we have a region location cache, so adding a method to clear the cache is fine.


---

* [HBASE-21713](https://issues.apache.org/jira/browse/HBASE-21713) | *Major* | **Support set region server throttle quota**

Support set region server rpc throttle quota which represents the read/write ability of region servers and throttles when region server's total requests exceeding the limit.

Use the following shell command to set RS quota:
set\_quota TYPE =\> THROTTLE, REGIONSERVER =\> 'all', THROTTLE\_TYPE =\> WRITE, LIMIT =\> '20000req/sec'
set\_quota TYPE =\> THROTTLE, REGIONSERVER =\> 'all', LIMIT =\> NONE
"all" represents the throttle quota of all region servers and setting specified region server quota isn't supported currently.


---

* [HBASE-21689](https://issues.apache.org/jira/browse/HBASE-21689) | *Minor* | **Make table/namespace specific current quota info available in shell(describe\_namespace & describe)**

In shell commands "describe\_namespace" and "describe", which are used to see the descriptors of the namespaces and tables respectively, quotas set on that particular namespace/table will also be printed along.


---

* [HBASE-17370](https://issues.apache.org/jira/browse/HBASE-17370) | *Major* | **Fix or provide shell scripts to drain and decommission region server**

Adds shell support for the following:
- List decommissioned/draining region servers
- Decommission a list of region servers, optionally offload corresponding regions
- Recommission a region server, optionally load a list of passed regions


---

* [HBASE-21734](https://issues.apache.org/jira/browse/HBASE-21734) | *Major* | **Some optimization in FilterListWithOR**

After HBASE-21620, the filterListWithOR has been a bit slow because we need to merge each sub-filter's RC , while before HBASE-21620, we will skip many RC merging, but the logic was wrong. So here we choose another way to optimaze the performance: removing the KeyValueUtil#toNewKeyCell.
Anoop Sam John suggested that the KeyValueUtil#toNewKeyCell can save some GC before because if we copy key part of cell into a single byte[], then the block the cell refering won't be refered by the filter list any more, the upper layer can GC the data block quickly. while after HBASE-21620, we will update the prevCellList for every encountered cell now, so the lifecycle of cell in prevCellList for FilterList will be quite shorter. so just use the cell ref for saving cpu.
BTW, we removed all the arrays streams usage in filter list, because it's also quite time-consuming in our test.


---

* [HBASE-21738](https://issues.apache.org/jira/browse/HBASE-21738) | *Critical* | **Remove all the CSLM#size operation in our memstore because it's an quite time consuming.**

We found the memstore snapshotting would cost much time because of calling the time-consuming ConcurrentSkipListMap#Size, it would make the p999 latency spike happen. So in this issue, we remove all ConcurrentSkipListMap#size in memstore by counting the cellsCount in MemstoreSizeing. As the issue described, the p999 latency spike was mitigated.


---

* [HBASE-21034](https://issues.apache.org/jira/browse/HBASE-21034) | *Major* | **Add new throttle type: read/write capacity unit**

Provides a new throttle type: capacity unit. One read/write/request capacity unit represents that read/write/read+write up to 1K data. If data size is more than 1K, then consume additional capacity units.

Use shell command to set capacity unit(CU):
set\_quota TYPE =\> THROTTLE, THROTTLE\_TYPE =\> WRITE, USER =\> 'u1', LIMIT =\> '10CU/sec'

Use the "hbase.quota.read.capacity.unit" property to set the data size of one read capacity unit in bytes, the default value is 1K. Use the "hbase.quota.write.capacity.unit" property to set the data size of one write capacity unit in bytes, the default value is 1K.


---

* [HBASE-21595](https://issues.apache.org/jira/browse/HBASE-21595) | *Minor* | **Print thread's information and stack traces when RS is aborting forcibly**

Does thread dump on stdout on abort.


---

* [HBASE-21732](https://issues.apache.org/jira/browse/HBASE-21732) | *Critical* | **Should call toUpperCase before using Enum.valueOf in some methods for ColumnFamilyDescriptor**

Now all the Enum configs in ColumnFamilyDescriptor can accept lower case config value.


---

* [HBASE-21712](https://issues.apache.org/jira/browse/HBASE-21712) | *Minor* | **Make submit-patch.py python3 compatible**

Python3 support was added to dev-support/submit-patch.py. To install newly required dependencies run \`pip install -r dev-support/python-requirements.txt\` command.


---

* [HBASE-21657](https://issues.apache.org/jira/browse/HBASE-21657) | *Major* | **PrivateCellUtil#estimatedSerializedSizeOf has been the bottleneck in 100% scan case.**

In HBASE-21657,  I simplified the path of estimatedSerialiedSize() & estimatedSerialiedSizeOfCell() by moving the general getSerializedSize()
and heapSize() from ExtendedCell to Cell interface. The patch also included some other improvments:

1. For 99%  of case, our cells has no tags, so let the HFileScannerImpl just return the NoTagsByteBufferKeyValue if no tags, which means we can save
   lots of cpu time when sending no tags cell to rpc because can just return the length instead of getting the serialize size by caculating offset/length
   of each fields(row/cf/cq..)
2. Move the subclass's getSerializedSize implementation from ExtendedCell to their own class, which mean we did not need to call ExtendedCell's
   getSerialiedSize() firstly, then forward to subclass's getSerializedSize(withTags).
3. Give a estimated result arraylist size for avoiding the frequent list extension when in a big scan, now we estimate the array size as min(scan.rows, 512).
   it's also help a lot.

We gain almost ~40% throughput improvement in 100% scan case for branch-2 (cacheHitRatio~100%)[1], it's a good thing. While it's a incompatible change in
some case, such as if the upstream user implemented their own Cells, although it's rare but can happen, then their compile will be error.


---

* [HBASE-21647](https://issues.apache.org/jira/browse/HBASE-21647) | *Major* | **Add status track for splitting WAL tasks**

Adds task monitor that shows ServerCrashProcedure progress in UI.


---

* [HBASE-21652](https://issues.apache.org/jira/browse/HBASE-21652) | *Major* | **Refactor ThriftServer making thrift2 server inherited from thrift1 server**

Before this issue, thrift1 server and thrift2 server are totally different servers. If a new feature is added to thrift1 server, thrfit2 server have to make the same change to support it(e.g. authorization). After this issue, thrift2 server is inherited from thrift1, thrift2 server now have all the features thrift1 server has(e.g http support, which thrift2 server doesn't have before).  The way to start thrift1 or thrift2 server remain the same after this issue.


---

* [HBASE-21661](https://issues.apache.org/jira/browse/HBASE-21661) | *Major* | **Provide Thrift2 implementation of Table/Admin**

ThriftAdmin/ThriftTable are implemented based on Thrift2. With ThriftAdmin/ThriftTable, People can use thrift2 protocol just like HTable/HBaseAdmin.
Example of using ThriftConnection
Configuration conf = HBaseConfiguration.create();
conf.set(ClusterConnection.HBASE\_CLIENT\_CONNECTION\_IMPL,ThriftConnection.class.getName());
Connection conn = ConnectionFactory.createConnection(conf);
Table table = conn.getTable(tablename)
It is just like a normal Connection, similar use experience with the default ConnectionImplementation


---

* [HBASE-21618](https://issues.apache.org/jira/browse/HBASE-21618) | *Critical* | **Scan with the same startRow(inclusive=true) and stopRow(inclusive=false) returns one result**

There was a bug when scan with the same startRow(inclusive=true) and stopRow(inclusive=false). The old incorrect behavior is return one result. After this fix, the new correct behavior is return nothing.


---

* [HBASE-21159](https://issues.apache.org/jira/browse/HBASE-21159) | *Major* | **Add shell command to switch throttle on or off**

Support enable or disable rpc throttle when hbase quota is enabled. If hbase quota is enabled, rpc throttle is enabled by default.  When disable rpc throttle, HBase will not throttle any request. Use the following commands to switch rpc throttle : enable\_rpc\_throttle / disable\_rpc\_throttle.


---

* [HBASE-21659](https://issues.apache.org/jira/browse/HBASE-21659) | *Minor* | **Avoid to load duplicate coprocessors in system config and table descriptor**

Add a new configuration "hbase.skip.load.duplicate.table.coprocessor". The default value is false to keep compatible with the old behavior. Config it true to skip load duplicate table coprocessor.


---

* [HBASE-21650](https://issues.apache.org/jira/browse/HBASE-21650) | *Major* | **Add DDL operation and some other miscellaneous to thrift2**

Added DDL operations and some other structure definition to thrift2. Methods added:
create/modify/addColumnFamily/deleteColumnFamily/modifyColumnFamily/enable/disable/truncate/delete table
create/modify/delete namespace
get(list)TableDescriptor(s)/get(list)NamespaceDescirptor(s)
tableExists/isTableEnabled/isTableDisabled/isTableAvailabe
And some class definitions along with those methods


---

* [HBASE-21643](https://issues.apache.org/jira/browse/HBASE-21643) | *Major* | **Introduce two new region coprocessor method and deprecated postMutationBeforeWAL**

Deprecated region coprocessor postMutationBeforeWAL and introduce two new region coprocessor postIncrementBeforeWAL and postAppendBeforeWAL instead.


---

* [HBASE-21635](https://issues.apache.org/jira/browse/HBASE-21635) | *Major* | **Use maven enforcer to ban imports from illegal packages**

Use de.skuzzle.enforcer.restrict-imports-enforcer-rule extension for maven enforcer plugin to ban illegal imports at compile time. Now if you use illegal imports, for example, import com.google.common.\*, there will be a compile error, instead of a checkstyle warning.


---

* [HBASE-21401](https://issues.apache.org/jira/browse/HBASE-21401) | *Critical* | **Sanity check when constructing the KeyValue**

Add a sanity check when constructing KeyValue from a byte[]. we use the constructor when we're reading kv from socket or HFIle or WAL(replication). the santiy check isn't designed for discovering the bits corruption in network transferring or disk IO. It is designed to detect bugs inside HBase in advance. and HBASE-21459 indicated that there's extremely small performance loss for diff kinds of keyvalue.


---

* [HBASE-21554](https://issues.apache.org/jira/browse/HBASE-21554) | *Minor* | **Show replication endpoint classname for replication peer on master web UI**

The replication UI on master will show the replication endpoint classname.


---

* [HBASE-21549](https://issues.apache.org/jira/browse/HBASE-21549) | *Major* | **Add shell command for serial replication peer**

Add a SERIAL flag for add\_peer command to identifiy whether or not the replication peer is a serial replication peer. The default serial flag is false.


---

* [HBASE-21453](https://issues.apache.org/jira/browse/HBASE-21453) | *Major* | **Convert ReadOnlyZKClient to DEBUG instead of INFO**

Log level of ReadOnlyZKClient moved to debug.


---

* [HBASE-21283](https://issues.apache.org/jira/browse/HBASE-21283) | *Minor* | **Add new shell command 'rit' for listing regions in transition**

<!-- markdown -->

The HBase `shell` now includes a command to list regions currently in transition.

```
HBase Shell
Use "help" to get list of supported commands.
Use "exit" to quit this interactive shell.
Version 1.5.0-SNAPSHOT, r9bb6d2fa8b760f16cd046657240ebd4ad91cb6de, Mon Oct  8 21:05:50 UTC 2018

hbase(main):001:0> help 'rit'
List all regions in transition.
Examples:
  hbase> rit

hbase(main):002:0> create ...
0 row(s) in 2.5150 seconds
=> Hbase::Table - IntegrationTestBigLinkedList

hbase(main):003:0> rit
0 row(s) in 0.0340 seconds

hbase(main):004:0> unassign '56f0c38c81ae453d19906ce156a2d6a1'
0 row(s) in 0.0540 seconds

hbase(main):005:0> rit
IntegrationTestBigLinkedList,L\xCC\xCC\xCC\xCC\xCC\xCC\xCB,1539117183224.56f0c38c81ae453d19906ce156a2d6a1. state=PENDING_CLOSE, ts=Tue Oct 09 20:33:34 UTC 2018 (0s ago), server=null
1 row(s) in 0.0170 seconds
```


---

* [HBASE-21567](https://issues.apache.org/jira/browse/HBASE-21567) | *Major* | **Allow overriding configs starting up the shell**

Allow passing of -Dkey=value option to shell to override hbase-\* configuration: e.g.:

$ ./bin/hbase shell -Dhbase.zookeeper.quorum=ZK0.remote.cluster.example.org,ZK1.remote.cluster.example.org,ZK2.remote.cluster.example.org -Draining=false
...
hbase(main):001:0\> @shell.hbase.configuration.get("hbase.zookeeper.quorum")
=\> "ZK0.remote.cluster.example.org,ZK1.remote.cluster.example.org,ZK2.remote.cluster.example.org"
hbase(main):002:0\> @shell.hbase.configuration.get("raining")
=\> "false"


---

* [HBASE-21560](https://issues.apache.org/jira/browse/HBASE-21560) | *Major* | **Return a new TableDescriptor for MasterObserver#preModifyTable to allow coprocessor modify the TableDescriptor**

Incompatible change. Allow MasterObserver#preModifyTable to return a new TableDescriptor. And master will use this returned TableDescriptor to modify table.


---

* [HBASE-21551](https://issues.apache.org/jira/browse/HBASE-21551) | *Blocker* | **Memory leak when use scan with STREAM at server side**

<!-- markdown -->
### Summary
HBase clusters will experience Region Server failures due to out of memory errors due to a leak given any of the following:

* User initiates Scan operations set to use the STREAM reading type
* User initiates Scan operations set to use the default reading type that read more than 4 * the block size of column families involved in the scan (e.g. by default 4*64KiB)
* Compactions run

### Root cause

When there are long running scans the Region Server process attempts to optimize access by using a different API geared towards sequential access. Due to an error in HBASE-20704 for HBase 2.0+ the Region Server fails to release related resources when those scans finish. That same optimization path is always used for the HBase internal file compaction process.

### Workaround

Impact for this error can be minimized by setting the config value “hbase.storescanner.pread.max.bytes” to MAX_INT to avoid the optimization for default user scans. Clients should also be checked to ensure they do not pass the STREAM read type to the Scan API. This will have a severe impact on performance for long scans.

Compactions always use this sequential optimized reading mechanism so downstream users will need to periodically restart Region Server roles after compactions have happened.


---

* [HBASE-21550](https://issues.apache.org/jira/browse/HBASE-21550) | *Major* | **Add a new method preCreateTableRegionInfos for MasterObserver which allows CPs to modify the TableDescriptor**

Add a new method preCreateTableRegionInfos for MasterObserver, which will be called before creating region infos for the given table,  before the preCreateTable method. It allows you to return a new TableDescritor to override the original one. Returns null or throws exception will stop the creation.


---

* [HBASE-21387](https://issues.apache.org/jira/browse/HBASE-21387) | *Major* | **Race condition surrounding in progress snapshot handling in snapshot cache leads to loss of snapshot files**

To prevent race condition between in progress snapshot (performed by TakeSnapshotHandler) and HFileCleaner which results in data loss, this JIRA introduced mutual exclusion between taking snapshot and running HFileCleaner. That is, at any given moment, either some snapshot can be taken or, HFileCleaner checks hfiles which are not referenced, but not both can be running.


---

* [HBASE-21452](https://issues.apache.org/jira/browse/HBASE-21452) | *Major* | **Illegal character in hbase counters group name**

Changes group name of hbase metrics from "HBase Counters" to "HBaseCounters".


---

* [HBASE-21443](https://issues.apache.org/jira/browse/HBASE-21443) | *Major* | **[hbase-connectors] Purge hbase-\* modules from core now they've been moved to hbase-connectors**

Parent issue moved hbase-spark\* modules to hbase-connectors. This issue removes hbase-spark\* modules from hbase core repo.


---

* [HBASE-21430](https://issues.apache.org/jira/browse/HBASE-21430) | *Major* | **[hbase-connectors] Move hbase-spark\* modules to hbase-connectors repo**

hbase-spark\* modules have been cloned to https://github.com/apache/hbase-connectors All spark connector dev is to happen in that repo from here on out.

Let me file a subtask to remove hbase-spark\* modules from hbase core.


---

* [HBASE-21417](https://issues.apache.org/jira/browse/HBASE-21417) | *Critical* | **Pre commit build is broken due to surefire plugin crashes**

Add -Djdk.net.URLClassPath.disableClassPathURLCheck=true when executing surefire plugin.


---

* [HBASE-21191](https://issues.apache.org/jira/browse/HBASE-21191) | *Major* | **Add a holding-pattern if no assign for meta or namespace (Can happen if masterprocwals have been cleared).**

Puts master startup into holding pattern if meta is not assigned (previous it would exit). To make progress again, operator needs to inject an assign (Caveats and instruction can be found in HBASE-21035).


---

* [HBASE-21322](https://issues.apache.org/jira/browse/HBASE-21322) | *Critical* | **Add a scheduleServerCrashProcedure() API to HbckService**

Adds scheduleServerCrashProcedure to the HbckService.


---

* [HBASE-21325](https://issues.apache.org/jira/browse/HBASE-21325) | *Major* | **Force to terminate regionserver when abort hang in somewhere**

Add two new config hbase.regionserver.abort.timeout and hbase.regionserver.abort.timeout.task. If regionserver abort timeout, it will schedule an abort timeout task to run. The default abort task is SystemExitWhenAbortTimeout, which will force to terminate region server when abort timeout. And you can config a special abort timeout task by hbase.regionserver.abort.timeout.task.


---

* [HBASE-21215](https://issues.apache.org/jira/browse/HBASE-21215) | *Major* | **Figure how to invoke hbck2; make it easy to find**

Adds to bin/hbase means of invoking hbck2. Pass the new '-j' option on the 'hbck' command with a value of the full path to the HBCK2.jar.

E.g:

$ ./bin/hbase hbck -j ~/checkouts/hbase-operator-tools/hbase-hbck2/target/hbase-hbck2-1.0.0-SNAPSHOT.jar  setTableState x ENABLED


---

* [HBASE-21372](https://issues.apache.org/jira/browse/HBASE-21372) | *Major* | **Set hbase.assignment.maximum.attempts to Long.MAX**

Retry assigns 'forever' (or until an intervention such as a ServerCrashProcedure).

Previous retry was a maximum of ten times but on failure, handling was an indeterminate.


---

* [HBASE-21338](https://issues.apache.org/jira/browse/HBASE-21338) | *Major* | **[balancer] If balancer is an ill-fit for cluster size, it gives little indication**

The description claims the balancer not dynamically configurable but this is an error; it is http://hbase.apache.org/book.html#dyn\_config

Also, if balancer is seen to be cutting out too soon, try setting "hbase.master.balancer.stochastic.runMaxSteps" to true.

Adds cleaner logging around balancer start.


---

* [HBASE-21073](https://issues.apache.org/jira/browse/HBASE-21073) | *Major* | **"Maintenance mode" master**

    Instead of being an ephemeral state set by hbck, maintenance mode is now
    an explicit toggle set by either configuration property or environment
    variable. In maintenance mode, master will host system tables and not
    assign any user-space tables to RSs. This gives operators the ability to
    affect repairs to meta table with fewer moving parts.


---

* [HBASE-21335](https://issues.apache.org/jira/browse/HBASE-21335) | *Critical* | **Change the default wait time of HBCK2 tool**

Changed waitTime parameter to lockWait on bypass. Changed default waitTime from 0 -- i.e. wait for ever -- to 1ms so if lock is held, we'll go past it and if override enforce bypass.


---

* [HBASE-21291](https://issues.apache.org/jira/browse/HBASE-21291) | *Major* | **Add a test for bypassing stuck state-machine procedures**

bypass will now throw an Exception if passed a lockWait \<= 0; i.e bypass will prevent an operator getting stuck on an entity lock waiting forever (lockWait == 0)


---

* [HBASE-21320](https://issues.apache.org/jira/browse/HBASE-21320) | *Major* | **[canary] Cleanup of usage and add commentary**

Cleans up usage and docs around Canary.  Does not change command-line args (though we should -- smile).


---

* [HBASE-21278](https://issues.apache.org/jira/browse/HBASE-21278) | *Critical* | **Do not rollback successful sub procedures when rolling back a procedure**

For the sub procedures which are successfully finished, do not do rollback. This is a change in rollback behavior.

State changes which are done by sub procedures should be handled by parent procedures when rolling back. For example, when rolling back a MergeTableProcedure, we will schedule new procedures to bring the offline regions online instead of rolling back the original procedures which off-lined the regions (in fact these procedures can not be rolled back...).


---

* [HBASE-21158](https://issues.apache.org/jira/browse/HBASE-21158) | *Critical* | **Empty qualifier cell should not be returned if it does not match QualifierFilter**

<!-- markdown -->

Scans that make use of `QualifierFilter` previously would erroneously return both columns with an empty qualifier along with those that matched. After this change that behavior has changed to only return those columns that match.


---

* [HBASE-21098](https://issues.apache.org/jira/browse/HBASE-21098) | *Major* | **Improve Snapshot Performance with Temporary Snapshot Directory when rootDir on S3**

It is recommended to place the working directory on-cluster on HDFS as doing so has shown a strong performance increase due to data locality. It is important to note that the working directory should not overlap with any existing directories as the working directory will be cleaned out during the snapshot process. Beyond that, any well-named directory on HDFS should be sufficient.


---

* [HBASE-21185](https://issues.apache.org/jira/browse/HBASE-21185) | *Minor* | **WALPrettyPrinter: Additional useful info to be printed by wal printer tool, for debugability purposes**

This adds two extra features to WALPrettyPrinter tool:

1) Output for each cell combined size of cell descriptors, plus the cell value itself, in a given WAL edit. This is printed on the results as "cell total size sum:" info by default;

2) An optional -g/--goto argument, that allows to seek straight to that specific WAL file position, then sequentially reading the WAL from that point towards its end;


---

* [HBASE-21287](https://issues.apache.org/jira/browse/HBASE-21287) | *Major* | **JVMClusterUtil Master initialization wait time not configurable**

Local HBase cluster (as used by unit tests) wait times on startup and initialization can be configured via \`hbase.master.start.timeout.localHBaseCluster\` and \`hbase.master.init.timeout.localHBaseCluster\`


---

* [HBASE-21280](https://issues.apache.org/jira/browse/HBASE-21280) | *Trivial* | **Add anchors for each heading in UI**

Adds anchors #tables, #tasks, etc.


---

* [HBASE-21232](https://issues.apache.org/jira/browse/HBASE-21232) | *Major* | **Show table state in Tables view on Master home page**

Add table state column to the tables panel


---

* [HBASE-21223](https://issues.apache.org/jira/browse/HBASE-21223) | *Critical* | **[amv2] Remove abort\_procedure from shell**

Removed the abort\_procedure command from shell -- dangerous -- and deprecated abortProcedure in Admin API.


---

* [HBASE-20636](https://issues.apache.org/jira/browse/HBASE-20636) | *Major* | **Introduce two bloom filter type : ROWPREFIX\_FIXED\_LENGTH and ROWPREFIX\_DELIMITED**

Add two bloom filter type : ROWPREFIX\_FIXED\_LENGTH and ROWPREFIX\_DELIMITED
1. ROWPREFIX\_FIXED\_LENGTH: specify the length of the prefix
2. ROWPREFIX\_DELIMITED: specify the delimiter of the prefix
Need to specify parameters for these two types of bloomfilter, otherwise the table will fail to create
Example:
create 't1', {NAME =\> 'f1', BLOOMFILTER =\> 'ROWPREFIX\_FIXED\_LENGTH', CONFIGURATION =\> {'RowPrefixBloomFilter.prefix\_length' =\> '10'}}
create 't1', {NAME =\> 'f1', BLOOMFILTER =\> 'ROWPREFIX\_DELIMITED', CONFIGURATION =\> {'RowPrefixDelimitedBloomFilter.delimiter' =\> '#'}}


---

* [HBASE-21156](https://issues.apache.org/jira/browse/HBASE-21156) | *Critical* | **[hbck2] Queue an assign of hbase:meta and bulk assign/unassign**

Adds 'raw' assigns/unassigns to the Hbck Service. Takes a list of encoded region names and bulk assigns/unassigns. Skirts Master 'state' check and does not invoke Coprocessors. For repair only.

Here is what HBCK2 usage looks like now:

{code}
$ java -cp hbase-hbck2-1.0.0-SNAPSHOT.jar  org.apache.hbase.HBCK2
usage: HBCK2 \<OPTIONS\> COMMAND [\<ARGS\>]

Options:
 -d,--debug                      run with debug output
 -h,--help                       output this help message
    --hbase.zookeeper.peerport   peerport of target hbase ensemble
    --hbase.zookeeper.quorum     ensemble of target hbase
    --zookeeper.znode.parent     parent znode of target hbase

Commands:
 setTableState \<TABLENAME\> \<STATE\>
   Possible table states: ENABLED, DISABLED, DISABLING, ENABLING
   To read current table state, in the hbase shell run:
     hbase\> get 'hbase:meta', '\<TABLENAME\>', 'table:state'
   A value of \\x08\\x00 == ENABLED, \\x08\\x01 == DISABLED, etc.
   An example making table name 'user' ENABLED:
     $ HBCK2 setTableState users ENABLED
   Returns whatever the previous table state was.

 assign \<ENCODED\_REGIONNAME\> ...
   A 'raw' assign that can be used even during Master initialization.
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   e.g. 1588230740 is hard-coded encoding for hbase:meta region and
   de00010733901a05f5a2a3a382e27dd4 is an example of what a random
   user-space encoded Region name looks like. For example:
     $ HBCK2 assign 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid of the created AssignProcedure or -1 if none.

 unassign \<ENCODED\_REGIONNAME\> ...
   A 'raw' unassign that can be used even during Master initialization.
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   de00010733901a05f5a2a3a382e27dd4 is an example of what a random
   user-space encoded Region name looks like. For example:
     $ HBCK2 unassign 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid of the created UnassignProcedure or -1 if none.
{code}


---

* [HBASE-21021](https://issues.apache.org/jira/browse/HBASE-21021) | *Major* | **Result returned by Append operation should be ordered**

This change ensures Append operations are assembled into the expected order.


---

* [HBASE-21171](https://issues.apache.org/jira/browse/HBASE-21171) | *Major* | **[amv2] Tool to parse a directory of MasterProcWALs standalone**

Make it so can run the WAL parse and load system in isolation. Here is an example:

{code}$ HBASE\_OPTS=" -XX:+UnlockDiagnosticVMOptions -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+DebugNonSafepoints" ./bin/hbase org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore ~/big\_set\_of\_masterprocwals/
{code}


---

* [HBASE-21107](https://issues.apache.org/jira/browse/HBASE-21107) | *Minor* | **add a metrics for netty direct memory**

Add a new nettyDirectMemoryUsage under server's ipc metrics to show direct memory usage for netty rpc server.


---

* [HBASE-21153](https://issues.apache.org/jira/browse/HBASE-21153) | *Major* | **Shaded client jars should always build in relevant phase to avoid confusion**

Client facing artifacts are now built whenever Maven is run through the "package" goal. Previously, the client facing artifacts would create placeholder jars that skipped repackaging HBase and third-party dependencies unless the "release" profile was active.

Build times may be noticeably longer depending on your build hardware. For example, the Jenkins worker nodes maintained by ASF Infra take ~14% longer to do a full packaging build. An example portability-focused personal laptop took ~25% longer.


---

* [HBASE-20942](https://issues.apache.org/jira/browse/HBASE-20942) | *Major* | **Improve RpcServer TRACE logging**

Allows configuration of the length of RPC messages printed to the log at TRACE level via "hbase.ipc.trace.param.size" in RpcServer.


---

* [HBASE-20649](https://issues.apache.org/jira/browse/HBASE-20649) | *Minor* | **Validate HFiles do not have PREFIX\_TREE DataBlockEncoding**

<!-- markdown -->
Users who have previously made use of prefix tree encoding can now check that their existing HFiles no longer contain data that uses it with an additional preupgrade check command.

```
hbase pre-upgrade validate-hfile
```

Please see the "HFile Content validation" section of the ref guide's coverage of the pre-upgrade validator tool for usage details.


---

* [HBASE-20941](https://issues.apache.org/jira/browse/HBASE-20941) | *Major* | **Create and implement HbckService in master**

Adds an HBCK Service and a first method to force-change-in-table-state for use by an HBCK client effecting 'repair' to a malfunctioning HBase.


---

* [HBASE-21071](https://issues.apache.org/jira/browse/HBASE-21071) | *Major* | **HBaseTestingUtility::startMiniCluster() to use builder pattern**

Cleanup all the cluster start override combos in HBaseTestingUtility by adding a StartMiniClusterOption and Builder.


---

* [HBASE-21072](https://issues.apache.org/jira/browse/HBASE-21072) | *Major* | **Block out HBCK1 in hbase2**

Fence out hbase-1.x hbck1 instances. Stop them making state changes on an hbase-2.x cluster; they could do damage. We do this by writing the hbck1 lock file into place on hbase-2.x Master start-up.

To disable this new behavior, set hbase.write.hbck1.lock.file to false


---

* [HBASE-20881](https://issues.apache.org/jira/browse/HBASE-20881) | *Major* | **Introduce a region transition procedure to handle all the state transition for a region**

Introduced a new TransitRegionStateProcedure to replace the old AssignProcedure/UnassignProcedure/MoveRegionProcedure. In the old code, MRP will not be attached to RegionStateNode, so it can not be interrupted by ServerCrashProcedure, which introduces lots of tricky code to deal with races, and also causes lots of other difficulties on how to prevent scheduling redundant or even conflict procedures for a region.

And now TRSP is the only one procedure which can bring region online or offline. When you want to schedule one, you need to check whether there is already one attached to the RegionStateNode, under the lock of the RegionStateNode. If not just go ahead, and if there is one, then you should do something, for example, give up and fail directly, or tell the TRSP to give up(This is what SCP does). Since the check and attach are both under the lock of RSN, it will greatly reduce the possible races, and make the code much simpler.


---

* [HBASE-21012](https://issues.apache.org/jira/browse/HBASE-21012) | *Critical* | **Revert the change of serializing TimeRangeTracker**

HFiles generated by 2.0.0, 2.0.1, 2.1.0 are not forward compatible to 1.4.6-, 1.3.2.1-, 1.2.6.1-, and other inactive releases. Why HFile lose compatability is hbase in new versions (2.0.0, 2.0.1, 2.1.0) use protobuf to serialize/deserialize TimeRangeTracker (TRT) while old versions use DataInput/DataOutput. To solve this, We have to put HBASE-21012 to 2.x and put HBASE-21013 in 1.x. For more information, please check HBASE-21008.


---

* [HBASE-20965](https://issues.apache.org/jira/browse/HBASE-20965) | *Major* | **Separate region server report requests to new handlers**

After HBASE-20965, we can use MasterFifoRpcScheduler in master to separate RegionServerReport requests to indenpedent handler. To use this feature, please set "hbase.master.rpc.scheduler.factory.class" to
 "org.apache.hadoop.hbase.ipc.MasterFifoRpcScheduler". Use "hbase.master.server.report.handler.count" to set RegionServerReport handlers count, the default value is half of "hbase.regionserver.handler.count" value, but at least 1, and the other handlers count in master is "hbase.regionserver.handler.count" value minus RegionServerReport handlers count, but at least 1 too.


---

* [HBASE-20813](https://issues.apache.org/jira/browse/HBASE-20813) | *Minor* | **Remove RPC quotas when the associated table/Namespace is dropped off**

In previous releases, when a Space Quota was configured on a table or namespace and that table or namespace was deleted, the Space Quota was also deleted. This change improves the implementation so that the same is also done for RPC Quotas.


---

* [HBASE-20986](https://issues.apache.org/jira/browse/HBASE-20986) | *Major* | **Separate the config of block size when we do log splitting and write Hlog**

After HBASE-20986, we can set different value to block size of WAL and recovered edits. Both of their default value is 2 \* default HDFS blocksize. And hbase.regionserver.recoverededits.blocksize is for block size of recovered edits while hbase.regionserver.hlog.blocksize is for block size of WAL.


---

* [HBASE-20856](https://issues.apache.org/jira/browse/HBASE-20856) | *Minor* | **PITA having to set WAL provider in two places**

With this change if a WAL's meta provider (hbase.wal.meta\_provider) is not explicitly set, it now defaults to whatever hbase.wal.provider is set to. Previous, the two settings operated independently, each with its own default.

This change is operationally incompatible with previous HBase versions because the default WAL meta provider no longer defaults to AsyncFSWALProvider but to hbase.wal.provider.

The thought is that this is more in line with an operator's expectation, that a change in hbase.wal.provider is sufficient to change how WALs are written, especially given hbase.wal.meta\_provider is an obscure configuration and that the very idea that meta regions would have their own wal provider would likely come as a surprise.


---

* [HBASE-20538](https://issues.apache.org/jira/browse/HBASE-20538) | *Critical* | **Upgrade our hadoop versions to 2.7.7 and 3.0.3**

Update hadoop-two.version to 2.7.7 and hadoop-three.version to 3.0.3 due to a JDK issue which is solved by HADOOP-15473.


---

* [HBASE-20846](https://issues.apache.org/jira/browse/HBASE-20846) | *Major* | **Restore procedure locks when master restarts**

1. Make hasLock method final, and add a locked field in Procedure to record whether we have the lock. We will set it to true in doAcquireLock and to false in doReleaseLock. The sub procedures do not need to manage it any more.

2. Also added a locked field in the proto message. When storing, the field will be set according to the return value of hasLock. And when loading, there is a new field in Procedure called lockedWhenLoading. We will set it to true if the locked field in proto message is true.

3. The reason why we can not set the locked field directly to true by calling doAcquireLock is that, during initialization, most procedures need to wait until master is initialized. So the solution here is that, we introduced a new method called waitInitialized in Procedure, and move the wait master initialized related code from acquireLock to this method. And we added a restoreLock method to Procedure, if lockedWhenLoading is true, we will call the acquireLock to get the lock, but do not set locked to true. And later when we call doAcquireLock and pass the waitInitialized check, we will test lockedWhenLoading, if it is true, when we just set the locked field to true and return, without actually calling the acquireLock method since we have already called it once.


---

* [HBASE-20672](https://issues.apache.org/jira/browse/HBASE-20672) | *Minor* | **New metrics ReadRequestRate and WriteRequestRate**

Exposing 2 new metrics in HBase to provide ReadRequestRate and WriteRequestRate at region server level. These metrics give the rate of request handled by the region server and are reset after every monitoring interval.


---

* [HBASE-6028](https://issues.apache.org/jira/browse/HBASE-6028) | *Minor* | **Implement a cancel for in-progress compactions**

Added a new command to the shell to switch on/off compactions called "compaction\_switch". Disabling compactions will interrupt any currently ongoing compactions. This setting will be lost on restart of the server. Added the configuration hbase.regionserver.compaction.enabled so user can enable/disable compactions via hbase-site.xml.


---

* [HBASE-20884](https://issues.apache.org/jira/browse/HBASE-20884) | *Major* | **Replace usage of our Base64 implementation with java.util.Base64**

Class org.apache.hadoop.hbase.util.Base64 has been removed in it's entirety from HBase 2+. In HBase 1, unused methods have been removed from the class and the audience was changed from  Public to Private. This class was originally intended as an internal utility class that could be used externally but thinking since changed; these classes should not have been advertised as public to end-users.

This represents an incompatible change for users who relied on this implementation. An alternative implementation for affected clients is available at java.util.Base64 when using Java 8 or newer; be aware, it may encode/decode differently. For clients seeking to restore this specific implementation, it is available in the public domain for download at http://iharder.sourceforge.net/current/java/base64/


---

* [HBASE-20357](https://issues.apache.org/jira/browse/HBASE-20357) | *Major* | **AccessControlClient API Enhancement**

This enhances the AccessControlClient APIs to retrieve the permissions based on namespace, table name, family and qualifier for specific user. AccessControlClient can also validate a user whether allowed to perform specified operations on a particular table.
Following APIs have been added,
1) getUserPermissions(Connection connection, String tableRegex, byte[] columnFamily, byte[] columnQualifier, String userName)
	 Scope of retrieving permission will be same as existing.
2) hasPermission(onnection connection, String tableName, byte[] columnFamily, byte[] columnQualifier, String userName, Permission.Action... actions)
     Scope of validating user privilege,
           User can perform self check without any special privilege but ADMIN privilege will be required to perform check for other users.
           For example, suppose there are two users "userA" & "userB" then there can be below scenarios,
            a. When userA want to check whether userA have privilege to perform mentioned actions
                 userA don't need ADMIN privilege, as it's a self query.
            b. When userA want to check whether userB have privilege to perform mentioned actions,
                 userA must have ADMIN or superuser privilege, as it's trying to query for other user.
