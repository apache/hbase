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
# HBASE  2.1.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-21764](https://issues.apache.org/jira/browse/HBASE-21764) | *Major* | **Size of in-memory compaction thread pool should be configurable**

Introduced an new config key in this issue: hbase.regionserver.inmemory.compaction.pool.size. the default value would be 10.  you can configure this to set the pool size of in-memory compaction pool. Note that all memstores in one region server will share the same pool, so if you have many regions in one region server,  you need to set this larger to compact faster for better read performance.


---

* [HBASE-21684](https://issues.apache.org/jira/browse/HBASE-21684) | *Major* | **Throw DNRIOE when connection or rpc client is closed**

Make StoppedRpcClientException extend DoNotRetryIOException.


---

* [HBASE-21791](https://issues.apache.org/jira/browse/HBASE-21791) | *Blocker* | **Upgrade thrift dependency to 0.12.0**

IMPORTANT: Due to security issues, all users who use hbase thrift should avoid using releases which do not have this fix.

The effect releases are:
2.1.x: 2.1.2 and below
2.0.x: 2.0.4 and below
1.x: 1.4.x and below

If you are using the effect releases above, please consider upgrading to a newer release ASAP.


---

* [HBASE-21734](https://issues.apache.org/jira/browse/HBASE-21734) | *Major* | **Some optimization in FilterListWithOR**

After HBASE-21620, the filterListWithOR has been a bit slow because we need to merge each sub-filter's RC , while before HBASE-21620, we will skip many RC merging, but the logic was wrong. So here we choose another way to optimaze the performance: removing the KeyValueUtil#toNewKeyCell.
Anoop Sam John suggested that the KeyValueUtil#toNewKeyCell can save some GC before because if we copy key part of cell into a single byte[], then the block the cell refering won't be refered by the filter list any more, the upper layer can GC the data block quickly. while after HBASE-21620, we will update the prevCellList for every encountered cell now, so the lifecycle of cell in prevCellList for FilterList will be quite shorter. so just use the cell ref for saving cpu.
BTW, we removed all the arrays streams usage in filter list, because it's also quite time-consuming in our test.


---

* [HBASE-21738](https://issues.apache.org/jira/browse/HBASE-21738) | *Critical* | **Remove all the CSLM#size operation in our memstore because it's an quite time consuming.**

We found the memstore snapshotting would cost much time because of calling the time-consuming ConcurrentSkipListMap#Size, it would make the p999 latency spike happen. So in this issue, we remove all ConcurrentSkipListMap#size in memstore by counting the cellsCount in MemstoreSizeing. As the issue described, the p999 latency spike was mitigated.


---

* [HBASE-21732](https://issues.apache.org/jira/browse/HBASE-21732) | *Critical* | **Should call toUpperCase before using Enum.valueOf in some methods for ColumnFamilyDescriptor**

Now all the Enum configs in ColumnFamilyDescriptor can accept lower case config value.


---

* [HBASE-21712](https://issues.apache.org/jira/browse/HBASE-21712) | *Minor* | **Make submit-patch.py python3 compatible**

Python3 support was added to dev-support/submit-patch.py. To install newly required dependencies run \`pip install -r dev-support/python-requirements.txt\` command.



# HBASE  2.1.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


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

* [HBASE-21567](https://issues.apache.org/jira/browse/HBASE-21567) | *Major* | **Allow overriding configs starting up the shell**

Allow passing of -Dkey=value option to shell to override hbase-\* configuration: e.g.:

$ ./bin/hbase shell -Dhbase.zookeeper.quorum=ZK0.remote.cluster.example.org,ZK1.remote.cluster.example.org,ZK2.remote.cluster.example.org -Draining=false
...
hbase(main):001:0\> @shell.hbase.configuration.get("hbase.zookeeper.quorum")
=\> "ZK0.remote.cluster.example.org,ZK1.remote.cluster.example.org,ZK2.remote.cluster.example.org"
hbase(main):002:0\> @shell.hbase.configuration.get("raining")
=\> "false"


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

* [HBASE-21387](https://issues.apache.org/jira/browse/HBASE-21387) | *Major* | **Race condition surrounding in progress snapshot handling in snapshot cache leads to loss of snapshot files**

To prevent race condition between in progress snapshot (performed by TakeSnapshotHandler) and HFileCleaner which results in data loss, this JIRA introduced mutual exclusion between taking snapshot and running HFileCleaner. That is, at any given moment, either some snapshot can be taken or, HFileCleaner checks hfiles which are not referenced, but not both can be running.


---

* [HBASE-21423](https://issues.apache.org/jira/browse/HBASE-21423) | *Major* | **Procedures for meta table/region should be able to execute in separate workers**

The procedure for meta table will be executed in a separate worker thread named 'Urgent Worker' to avoid stuck. A new config named 'hbase.master.urgent.procedure.threads' is added, the default value for it is 1. To disable the separate worker, set it to 0.


---

* [HBASE-21417](https://issues.apache.org/jira/browse/HBASE-21417) | *Critical* | **Pre commit build is broken due to surefire plugin crashes**

Add -Djdk.net.URLClassPath.disableClassPathURLCheck=true when executing surefire plugin.


---

* [HBASE-21237](https://issues.apache.org/jira/browse/HBASE-21237) | *Blocker* | **Use CompatRemoteProcedureResolver to dispatch open/close region requests to RS**

Use CompatRemoteProcedureResolver  instead of ExecuteProceduresRemoteCall to dispatch region open/close requests to RS. Since ExecuteProceduresRemoteCall  will group all the open/close operations in one call and execute them sequentially on the target RS. If one operation fails, all the operation will be marked as failure.


---

* [HBASE-21322](https://issues.apache.org/jira/browse/HBASE-21322) | *Major* | **Add a scheduleServerCrashProcedure() API to HbckService**

Adds scheduleServerCrashProcedure to the HbckService.



# HBASE  2.1.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


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

* [HBASE-21075](https://issues.apache.org/jira/browse/HBASE-21075) | *Blocker* | **Confirm that we can (rolling) upgrade from 2.0.x and 2.1.x to 2.2.x after HBASE-20881**

hbase-2.2.x uses a new Procedure form assiging/unassigning/moving Regions; it does not process hbase-2.1.x and earlier Unassign/Assign Procedure types. Upgrade requires that we first drain the Master Procedure Store of old style Procedures before starting the new 2.2.x Master. The patch here facilitates the draining process.

On your running hbase-2.1.1+ (or 2.0.3+ cluster), when upgrading:

1. Shutdown both active and standby masters (Your cluster will continue to server reads and writes without interruption).
2. Set the property hbase.procedure.upgrade-to-2-2 to true in hbase-site.xml for the Master, and start only one Master, still using the 2.1.1+ (or 2.0.3+) binaries.
3. Wait until the Master quits. Confirm that there is a 'READY TO ROLLING UPGRADE' message in the master log as the cause of the shutdown. The Procedure Store is now empty.
4. Start new Masters with the new 2.2.0+ code.


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

* [HBASE-21158](https://issues.apache.org/jira/browse/HBASE-21158) | *Critical* | **Empty qualifier cell should not be returned if it does not match QualifierFilter**

<!-- markdown -->

Scans that make use of `QualifierFilter` previously would erroneously return both columns with an empty qualifier along with those that matched. After this change that behavior has changed to only return those columns that match.


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

* [HBASE-21213](https://issues.apache.org/jira/browse/HBASE-21213) | *Major* | **[hbck2] bypass leaves behind state in RegionStates when assign/unassign**

Adds override to assigns and unassigns. Changes bypass 'force' to align calling the param 'override' instead.

The new override allows 'overriding' previous Procedure owner to override their ownership of the Procedured entity ("Region").

Used by hbck2.


---

* [HBASE-21232](https://issues.apache.org/jira/browse/HBASE-21232) | *Major* | **Show table state in Tables view on Master home page**

Add table state column to the tables panel


---

* [HBASE-21223](https://issues.apache.org/jira/browse/HBASE-21223) | *Critical* | **[amv2] Remove abort\_procedure from shell**

Removed the abort\_procedure command from shell -- dangerous -- and deprecated abortProcedure in Admin API.


---

* [HBASE-21210](https://issues.apache.org/jira/browse/HBASE-21210) | *Major* | **Add bypassProcedure() API to HBCK2**

Added a bypass to hbck2:

{code}
$ HBASE\_CLASSPATH\_PREFIX=../hbase-operator-tools/hbase-hbck2/target/hbase-hbck2-1.0.0-SNAPSHOT.jar ./bin/hbase org.apache.hbase.HBCK2
usage: HBCK2 [OPTIONS] COMMAND \<ARGS\>

Options:
 -d,--debug                                 run with debug output
 -h,--help                                  output this help message
 -p,--hbase.zookeeper.property.clientPort   peerport of target hbase
                                            ensemble
 -q,--hbase.zookeeper.quorum \<arg\>          ensemble of target hbase
 -z,--zookeeper.znode.parent                parent znode of target hbase

Commands:
 setTableState \<TABLENAME\> \<STATE\>
   Possible table states: ENABLED, DISABLED, DISABLING, ENABLING
   To read current table state, in the hbase shell run:
     hbase\> get 'hbase:meta', '\<TABLENAME\>', 'table:state'
   A value of \\x08\\x00 == ENABLED, \\x08\\x01 == DISABLED, etc.
   An example making table name 'user' ENABLED:
     $ HBCK2 setTableState users ENABLED
   Returns whatever the previous table state was.

 assigns \<ENCODED\_REGIONNAME\>...
   A 'raw' assign that can be used even during Master initialization.
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   e.g. 1588230740 is hard-coded encoding for hbase:meta region and
   de00010733901a05f5a2a3a382e27dd4 is an example of what a random
   user-space encoded Region name looks like. For example:
     $ HBCK2 assign 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid of the created AssignProcedure or -1 if none.

 bypass [OPTIONS] \<PID\>...
   Pass one (or more) procedure 'pid's to skip to the procedure finish.
   Parent of this procedures will also skip to its finish. Entities will
   be left in an inconsistent state and will require manual fixup.
   Pass --force to break any outstanding locks.
   Pass --waitTime=\<seconds\> to wait on entity lock before giving up.
   Default: force=false and waitTime=0. Returns true if succeeded.

 unassigns \<ENCODED\_REGIONNAME\>...
   A 'raw' unassign that can be used even during Master initialization.
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   Skirts Coprocessors. Pass one or more encoded RegionNames:
   de00010733901a05f5a2a3a382e27dd4 is an example of what a random
   user-space encoded Region name looks like. For example:
     $ HBCK2 unassign 1588230740 de00010733901a05f5a2a3a382e27dd4
   Returns the pid of the created UnassignProcedure or -1 if none.
{code}


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

* [HBASE-21191](https://issues.apache.org/jira/browse/HBASE-21191) | *Major* | **Add a holding-pattern if no assign for meta or namespace (Can happen if masterprocwals have been cleared).**

Puts master startup into holding pattern if meta is not assigned (previous it would exit). To make progress again, operator needs to inject an assign (Caveats and instruction can be found in HBASE-21035).


---

* [HBASE-21125](https://issues.apache.org/jira/browse/HBASE-21125) | *Major* | **Backport 'HBASE-20942 Improve RpcServer TRACE logging' to branch-2.1**

Allows configuration of the length of RPC messages printed to the log at TRACE level via "hbase.ipc.trace.param.size" in RpcServer.


---

* [HBASE-21171](https://issues.apache.org/jira/browse/HBASE-21171) | *Major* | **[amv2] Tool to parse a directory of MasterProcWALs standalone**

Make it so can run the WAL parse and load system in isolation. Here is an example:

{code}$ HBASE\_OPTS=" -XX:+UnlockDiagnosticVMOptions -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+DebugNonSafepoints" ./bin/hbase org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore ~/big\_set\_of\_masterprocwals/
{code}


---

* [HBASE-21153](https://issues.apache.org/jira/browse/HBASE-21153) | *Major* | **Shaded client jars should always build in relevant phase to avoid confusion**

Client facing artifacts are now built whenever Maven is run through the "package" goal. Previously, the client facing artifacts would create placeholder jars that skipped repackaging HBase and third-party dependencies unless the "release" profile was active.

Build times may be noticeably longer depending on your build hardware. For example, the Jenkins worker nodes maintained by ASF Infra take ~14% longer to do a full packaging build. An example portability-focused personal laptop took ~25% longer.


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

* [HBASE-21072](https://issues.apache.org/jira/browse/HBASE-21072) | *Major* | **Block out HBCK1 in hbase2**

Fence out hbase-1.x hbck1 instances. Stop them making state changes on an hbase-2.x cluster; they could do damage. We do this by writing the hbck1 lock file into place on hbase-2.x Master start-up.

To disable this new behavior, set hbase.write.hbck1.lock.file to false


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

* [HBASE-20884](https://issues.apache.org/jira/browse/HBASE-20884) | *Major* | **Replace usage of our Base64 implementation with java.util.Base64**

Class org.apache.hadoop.hbase.util.Base64 has been removed in it's entirety from HBase 2+. In HBase 1, unused methods have been removed from the class and the audience was changed from  Public to Private. This class was originally intended as an internal utility class that could be used externally but thinking since changed; these classes should not have been advertised as public to end-users.

This represents an incompatible change for users who relied on this implementation. An alternative implementation for affected clients is available at java.util.Base64 when using Java 8 or newer; be aware, it may encode/decode differently. For clients seeking to restore this specific implementation, it is available in the public domain for download at http://iharder.sourceforge.net/current/java/base64/



# HBASE  2.1.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-20691](https://issues.apache.org/jira/browse/HBASE-20691) | *Blocker* | **Storage policy should allow deferring to HDFS**

After HBASE-20691 we have changed the default setting of hbase.wal.storage.policy from "HOT" back to "NONE" which means we defer the policy to HDFS. This fixes the problem of release 2.0.0 that the storage policy of WAL directory will defer to HDFS and may not be "HOT" even if you explicitly set hbase.wal.storage.policy to "HOT"


---

* [HBASE-20839](https://issues.apache.org/jira/browse/HBASE-20839) | *Blocker* | **Fallback to FSHLog if we can not instantiated AsyncFSWAL when user does not specify AsyncFSWAL explicitly**

As we hack into the internal of DFSClient when implementing AsyncFSWAL to get better performance, a patch release of hadoop can make it broken.

So now, if user does not specify a wal provider, then we will first try to use 'asyncfs', i.e, the AsyncFSWALProvider. If we fail due to some compatible issues, we will fallback to 'filesystem', i.e, FSHLog.


---

* [HBASE-20193](https://issues.apache.org/jira/browse/HBASE-20193) | *Critical* | **Basic Replication Web UI - Regionserver**

After HBASE-20193, we add a section to web ui to show the replication status of each wal group. There are 2 parts of this section, they both show the peerId, wal group and current replicating log of each replication source. And one is showing the information of replication log queue, i.e. size of current log, log queue size and replicating offset. The other one is showing the delay of replication, i.e. last shipped age and replication delay.
If the offset shows -1 and replication delay is UNKNOWN, that means replication is not started. This may be caused by this peer is disabled or the replicationEndpoint is sleeping due to some reason.


---

* [HBASE-19997](https://issues.apache.org/jira/browse/HBASE-19997) | *Blocker* | **[rolling upgrade] 1.x =\> 2.x**

Now we have a 'basically work' solution for rolling upgrade from 1.4.x to 2.x. Please see the "Rolling Upgrade from 1.x to 2.x" section in ref guide for more details.


---

* [HBASE-20270](https://issues.apache.org/jira/browse/HBASE-20270) | *Major* | **Turn off command help that follows all errors in shell**

<!-- markdown -->
The command help that followed all errors, before, is now no longer available. Erroneous command inputs would now just show error-texts followed by the shell command to try for seeing the help message. It looks like: For usage try 'help “create”’. Operators can copy-paste the command to get the help message.


---

* [HBASE-20194](https://issues.apache.org/jira/browse/HBASE-20194) | *Critical* | **Basic Replication WebUI - Master**

After HBASE-20194, we added 2 parts to master's web page.
One is Peers that shows all replication peers and some of their configurations, like peer id, cluster key, state, bandwidth, and which namespace or table it will replicate.
The other one is replication status of all regionservers, we added a tab to region servers division, then we can check the replication delay of all region servers for any peer. This table shows AgeOfLastShippedOp, SizeOfLogQueue and ReplicationLag for each regionserver and the table is sort by ReplicationLag in descending order. By this way we can easily find the problematic region server. If the replication delay is UNKNOWN, that means this walGroup doesn't start replicate yet and it may get disabled. ReplicationLag will update once this peer start replicate.


---

* [HBASE-18569](https://issues.apache.org/jira/browse/HBASE-18569) | *Major* | **Add prefetch support for async region locator**

Add prefetch support for async region locator. The default value is 10. Set 'hbase.client.locate.prefetch.limit' in hbase-site.xml if you want to use another value for it.


---

* [HBASE-20642](https://issues.apache.org/jira/browse/HBASE-20642) | *Major* | **IntegrationTestDDLMasterFailover throws 'InvalidFamilyOperationException**

This changes client-side nonce generation to use the same nonce for re-submissions of client RPC DDL operations.


---

* [HBASE-20708](https://issues.apache.org/jira/browse/HBASE-20708) | *Blocker* | **Remove the usage of RecoverMetaProcedure in master startup**

Introduce an InitMetaProcedure to initialize meta table for a new HBase deploy. Marked RecoverMetaProcedure deprecated and remove the usage of it in the current code base. We still need to keep it in place for compatibility. The code in RecoverMetaProcedure has been moved to ServerCrashProcedure, and SCP will always be enabled and we will rely on it to bring meta region online.

For more on the issue addressed by this commit, see the design doc for overview and plan: https://docs.google.com/document/d/1\_872oHzrhJq4ck7f6zmp1J--zMhsIFvXSZyX1Mxg5MA/edit#heading=h.xy1z4alsq7uy


---

* [HBASE-20334](https://issues.apache.org/jira/browse/HBASE-20334) | *Major* | **add a test that expressly uses both our shaded client and the one from hadoop 3**

<!-- markdown -->

HBase now includes a helper script that can be used to run a basic functionality test for a given HBase installation at in `dev_support`. The test can optionally be given an HBase client artifact to rely on and can optionally be given specific Hadoop client artifacts to use.

For usage information see `./dev-support/hbase_nightly_pseudo-distributed-test.sh --help`.

The project nightly tests now make use of this test to check running on top of Hadoop 2, Hadoop 3, and Hadoop 3 with shaded client artifacts.


---

* [HBASE-19735](https://issues.apache.org/jira/browse/HBASE-19735) | *Major* | **Create a minimal "client" tarball installation**

<!-- markdown -->

The HBase convenience binary artifacts now includes a client focused tarball that a) includes more docs and b) does not include scripts or jars only needed for running HBase cluster services.

The new artifact is made as a normal part of the `assembly:single` maven command.


---

* [HBASE-20615](https://issues.apache.org/jira/browse/HBASE-20615) | *Major* | **emphasize use of shaded client jars when they're present in an install**

<!-- markdown -->

HBase's built in scripts now rely on the downstream facing shaded artifacts where possible. In particular interest to downstream users, the `hbase classpath` and `hbase mapredcp` commands now return the relevant shaded client artifact and only those third paty jars needed to make use of them (e.g. slf4j-api, commons-logging, htrace, etc).

Downstream users should note that by default the `hbase classpath` command will treat having `hadoop` on the shell's PATH as an implicit request to include the output of the `hadoop classpath` command in the returned classpath. This long-existing behavior can be opted out of by setting the environment variable `HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP` to the value "true". For example: `HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true" bin/hbase classpath`.


---

* [HBASE-20333](https://issues.apache.org/jira/browse/HBASE-20333) | *Critical* | **break up shaded client into one with no Hadoop and one that's standalone**

<!-- markdown -->

Downstream users who need to use both HBase and Hadoop APIs should switch to relying on the new `hbase-shaded-client-byo-hadoop` artifact rather than the existing `hbase-shaded-client` artifact. The new artifact no longer includes and Hadoop classes.

It should work in combination with either the output of `hadoop classpath` or the Hadoop provided client-facing shaded artifacts in Hadoop 3+.


---

* [HBASE-20332](https://issues.apache.org/jira/browse/HBASE-20332) | *Critical* | **shaded mapreduce module shouldn't include hadoop**

<!-- markdown -->

The `hbase-shaded-mapreduce` artifact no longer include its own copy of Hadoop classes. Users who make use of the artifact via YARN should be able to get these classes from YARN's classpath without having to make any changes.


---

* [HBASE-20681](https://issues.apache.org/jira/browse/HBASE-20681) | *Major* | **IntegrationTestDriver fails after HADOOP-15406 due to missing hamcrest-core**

<!-- markdown -->

Users of our integration tests on Hadoop 3 can now add all needed dependencies by pointing at jars included in our binary convenience artifact.

Prior to this fix, downstream users on Hadoop 3 would need to get a copy of the Hamcrest v1.3 jar from elsewhere.


---

* [HBASE-19852](https://issues.apache.org/jira/browse/HBASE-19852) | *Major* | **HBase Thrift 1 server SPNEGO Improvements**

Adds two new properties for hbase-site.xml for THRIFT SPNEGO when in HTTP mode:
\* hbase.thrift.spnego.keytab.file
\* hbase.thrift.spnego.principal


---

* [HBASE-20590](https://issues.apache.org/jira/browse/HBASE-20590) | *Critical* | **REST Java client is not able to negotiate with the server in the secure mode**

Adds a negotiation logic between a secure java REST client and server. After this jira the Java REST client will start responding to the Negotiate challenge sent by the server. Adds RESTDemoClient which can be used to verify whether the secure Java REST client works against secure REST server or not.


---

* [HBASE-20634](https://issues.apache.org/jira/browse/HBASE-20634) | *Critical* | **Reopen region while server crash can cause the procedure to be stuck**

A second attempt at fixing HBASE-20173. Fixes unfinished keeping of server state inside AM (ONLINE=\>SPLITTING=\>OFFLINE=\>null). Concurrent unassigns look at server state to figure if they should wait on SCP to wake them up or not.


---

* [HBASE-20579](https://issues.apache.org/jira/browse/HBASE-20579) | *Minor* | **Improve snapshot manifest copy in ExportSnapshot**

This patch adds an FSUtil.copyFilesParallel() to help copy files in parallel, and it will return all the paths of directories and files traversed. Thus when we copy manifest in ExportSnapshot, we can copy reference files concurrently and use the paths it returns to help setOwner and setPermission.
The size of thread pool is determined by the configuration snapshot.export.copy.references.threads, and its default value is the number of runtime available processors.


---

* [HBASE-18116](https://issues.apache.org/jira/browse/HBASE-18116) | *Major* | **Replication source in-memory accounting should not include bulk transfer hfiles**

Before this change we would incorrectly include the size of enqueued store files for bulk replication in the calculation for determining whether or not to rate limit the transfer of WAL edits. Because bulk replication uses a separate and asynchronous mechanism for file transfer this could incorrectly limit the batch sizes for WAL replication if bulk replication in progress, with negative impact on latency and throughput.


---

* [HBASE-20592](https://issues.apache.org/jira/browse/HBASE-20592) | *Minor* | **Create a tool to verify tables do not have prefix tree encoding**

PreUpgradeValidator tool with DataBlockEncoding validator was added to verify cluster is upgradable to HBase 2.


---

* [HBASE-20501](https://issues.apache.org/jira/browse/HBASE-20501) | *Blocker* | **Change the Hadoop minimum version to 2.7.1**

<!-- markdown -->
HBase is no longer able to maintain compatibility with Apache Hadoop versions that are no longer receiving updates. This release raises the minimum supported version to Hadoop 2.7.1. Downstream users are strongly advised to upgrade to the latest Hadoop 2.7 maintenance release.

Downstream users of earlier HBase versions are similarly advised to upgrade to Hadoop 2.7.1+. When doing so, it is especially important to follow the guidance from [the HBase Reference Guide's Hadoop section](http://hbase.apache.org/book.html#hadoop) on replacing the Hadoop artifacts bundled with HBase.


---

* [HBASE-20601](https://issues.apache.org/jira/browse/HBASE-20601) | *Minor* | **Add multiPut support and other miscellaneous to PE**

1. Add multiPut support
Set --multiPut=number to enable batchput(meanwhile, --autoflush need be set to false)

2. Add Connection Count support
Added a new parameter connCount to PE. set --connCount=2 means all threads will share 2 connections.
oneCon option and connCount option shouldn't be set at the same time.

3. Add avg RT and avg TPS/QPS statstic for all threads

4. Delete some redundant code
Now RandomWriteTest is inherited from SequentialWrite.


---

* [HBASE-20544](https://issues.apache.org/jira/browse/HBASE-20544) | *Blocker* | **downstream HBaseTestingUtility fails with invalid port**

<!-- markdown -->

HBase now relies on an internal mechanism to determine when it is running a local hbase cluster meant for external interaction vs an encapsulated test. When created via the `HBaseTestingUtility`, ports for Master and RegionServer services and UIs will be set to random ports to allow for multiple parallel uses on a single machine. Normally when running a Standalone HBase Deployment (as described in the HBase Reference Guide) the ports will be picked according to the same defaults used in a full cluster set up. If you wish to instead use the random port assignment set `hbase.localcluster.assign.random.ports` to true.


---

* [HBASE-20004](https://issues.apache.org/jira/browse/HBASE-20004) | *Minor* | **Client is not able to execute REST queries in a secure cluster**

Added 'hbase.rest.http.allow.options.method' configuration property to allow user to decide whether Rest Server HTTP should allow OPTIONS method or not. By default it is enabled in HBase 2.1.0+ versions and in other versions it is disabled.
Similarly 'hbase.thrift.http.allow.options.method' is added HBase 1.5, 2.1.0 and 3.0.0 versions. It is disabled by default.


---

* [HBASE-20327](https://issues.apache.org/jira/browse/HBASE-20327) | *Minor* | **When qualifier is not specified, append and incr operation do not work (shell)**

This change will enable users to perform append and increment operation with null qualifier via hbase-shell.


---

* [HBASE-18842](https://issues.apache.org/jira/browse/HBASE-18842) | *Minor* | **The hbase shell clone\_snaphost command returns bad error message**

<!-- markdown -->

When attempting to clone a snapshot but using a namespace that does not exist, the HBase shell will now correctly report the exception as caused by the passed namespace. Previously, the shell would report that the problem was an unknown namespace but it would claim the user provided table name was not found as a namespace. Both before and after this change the shell properly used the passed namespace to attempt to handle the request.


---

* [HBASE-20406](https://issues.apache.org/jira/browse/HBASE-20406) | *Major* | **HBase Thrift HTTP - Shouldn't handle TRACE/OPTIONS methods**

<!-- markdown -->
When configured to do thrift-over-http, the HBase Thrift API Server no longer accepts the HTTP methods TRACE nor OPTIONS.


---

* [HBASE-20046](https://issues.apache.org/jira/browse/HBASE-20046) | *Major* | **Reconsider the implementation for serial replication**

Now in replication we can make sure the order of pushing logs is same as the order of requests from client. Set the serial flag to true for a replication peer to enable this feature.


---

* [HBASE-20159](https://issues.apache.org/jira/browse/HBASE-20159) | *Major* | **Support using separate ZK quorums for client**

After HBASE-20159 we allow client to use different ZK quorums by introducing three new properties: hbase.client.zookeeper.quorum and hbase.client.zookeeper.property.clientPort to specify client zookeeper properties (note that the combination of these two properties should be different from the server ZK quorums), and hbase.client.zookeeper.observer.mode to indicate whether the client ZK nodes are in observer mode (false by default)

HConstants.DEFAULT\_ZOOKEPER\_CLIENT\_PORT has been removed in HBase 3.0 and replaced by the correctly spelled DEFAULT\_ZOOKEEPER\_CLIENT\_PORT.


---

* [HBASE-20242](https://issues.apache.org/jira/browse/HBASE-20242) | *Major* | **The open sequence number will grow if we fail to open a region after writing the max sequence id file**

Now when opening a region, we will store the current max sequence id of the region to its max sequence id file instead of the 'next sequence id'. This could avoid the sequence id bumping when we fail to open a region, and also align to the behavior when we close a region.


---

* [HBASE-19024](https://issues.apache.org/jira/browse/HBASE-19024) | *Critical* | **Configurable default durability for synchronous WAL**

The default durability setting for the synchronous WAL is Durability.SYNC\_WAL, which triggers HDFS hflush() to flush edits to the datanodes. We also support Durability.FSYNC\_WAL, which instead triggers HDFS hsync() to flush \_and\_ fsync edits. This change introduces the new configuration setting "hbase.wal.hsync", defaulting to FALSE, that if set to TRUE changes the default durability setting for the synchronous WAL to  FSYNC\_WAL.


---

* [HBASE-19389](https://issues.apache.org/jira/browse/HBASE-19389) | *Critical* | **Limit concurrency of put with dense (hundreds) columns to prevent write handler exhausted**

After HBASE-19389 we introduced a RegionServer self-protection mechanism to prevent write handler getting exhausted by high concurrency put with dense columns, mainly through two new properties: hbase.region.store.parallel.put.limit.min.column.count to decide what kind of put (with how many columns within a single column family) to limit (100 by default) and hbase.region.store.parallel.put.limit to limit the concurrency (10 by default). There's another property for advanced user and please check source and javadoc of StoreHotnessProtector for more details.


---

* [HBASE-20148](https://issues.apache.org/jira/browse/HBASE-20148) | *Major* | **Make serial replication as a option for a peer instead of a table**

A new method setSerial has been added to the interface ReplicationPeerConfigBuilder which is marked as IA.Public. This interface is not supposed to be implemented by client code, but if you do, this will be an incompatible change as you need to add this method to your implementation too.


---

* [HBASE-19397](https://issues.apache.org/jira/browse/HBASE-19397) | *Major* | **Design  procedures for ReplicationManager to notify peer change event from master**

Introduce 5 procedures to do peer modifications:
AddPeerProcedure
RemovePeerProcedure
UpdatePeerConfigProcedure
EnablePeerProcedure
DisablePeerProcedure

The procedures are all executed with the following stage:
1. Call pre CP hook, if an exception is thrown then give up
2. Check whether the operation is valid, if not then give up
3. Update peer storage. Notice that if we have entered this stage, then we can not rollback any more.
4. Schedule sub procedures to refresh the peer config on every RS.
5. Do post cleanup if any.
6. Call post CP hook. The exception thrown will be ignored since we have already done the work.

The procedure will hold an exclusive lock on the peer id, so now there is no concurrent modifications on a single peer.

And now it is guaranteed that once the procedure is done, the peer modification has already taken effect on all RSes.

Abstracte a storage layer for replication peer/queue manangement, and refactored the upper layer to remove zk related naming/code/comment.

Add pre/postExecuteProcedures CP hooks to RegionServerObserver, and add permission check for executeProcedures method which requires the caller to be system user or super user.

On rolling upgrade: just do not do any replication peer modifications during the rolling upgrading. There is no pb/layout changes on the peer/queue storage on zk.
