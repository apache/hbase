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
-->
# HBASE  2.5.11 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-29117](https://issues.apache.org/jira/browse/HBASE-29117) | *Major* | **Kerby version conflict with Hadoop 3.4**

HBase now uses Kerby 1.0.1when built with Hadoop 2.x and Kerby 2.3.0 when built with Hadoop 3.x.
This works for the default Hadoop 3.4.x. However, when building HBase with Hadoop 3.2.x or 3.3.x , the kerby.version property must be manually overriden, i.e.
"mvn clean install -Dhadoop-three.version=3.3.6 -Dkerby.version=1.0.1"


---

* [HBASE-29049](https://issues.apache.org/jira/browse/HBASE-29049) | *Major* | **Increase the interval for running flaky tests**

Increased the interval for flaky tests to 12 hours to save build resources.


---

* [HBASE-29067](https://issues.apache.org/jira/browse/HBASE-29067) | *Major* | **Stagger the nightly tests on jenkins**

Run all nightly jobs every 3 days, master and branch-3 on first day, branch-2 on second day, and other branches on third day


---

* [HBASE-28980](https://issues.apache.org/jira/browse/HBASE-28980) | *Major* | **Change the default Hadoop 3 version to 3.4.1 on branch-2.5 and branch-2.6**

Hbase now defaults to using Hadoop 3.4.1 when being built for Hadoop 3.

The -hadoop3 HBase maven artifacts, and the official -hbase3 binaries are now built with Hadoop 3.4.1


---

* [HBASE-29009](https://issues.apache.org/jira/browse/HBASE-29009) | *Major* | **Depend on jaxws-rt instead jaxws-ri**

Phoenix now depends on jaxws-rt instead of jaxwrs-ri.
The phoenix assemblies on branch-2.x now depend on jaxws-rt instead of jaxws-ri.


---

* [HBASE-28328](https://issues.apache.org/jira/browse/HBASE-28328) | *Minor* | **Add an option to count different types of Delete Markers in RowCounter**

The JIRA adds a feature in RowCounter tool to count the various types of delete markers (DELETE\_COLUMN, DELETE\_FAMILY, DELETE\_FAMILY\_VERSION) and the number of rows containing at least one delete marker. The feature can be enabled by passing the flag --countDeleteMarkers as a CLI option. When the feature is enabled, raw scan is performed without FirstKeyOnlyFilter.


---

* [HBASE-27638](https://issues.apache.org/jira/browse/HBASE-27638) | *Major* | **Get slow/large log response that matched the ‘CLIENT\_IP' without client port**

Retrieve latest LargeLog or slowlog Responses based on client IP also.


---

* [HBASE-28070](https://issues.apache.org/jira/browse/HBASE-28070) | *Major* | ** Replace javax.servlet.jsp dependency with tomcat-jasper**

The main driving force behind this change is the need to remove the org.glassfish:javax.el:jar:3.0.1-b08 dependency from our project. Not only has org.glassfish:javax.el reached EOL'ed, but has indirect vulnerabilities (CVE-2020-15250).

Before this change, it was required by the javax.servlet.jsp dependency.

Hence, to eliminate the org.glassfish:javax.el dependency, as part of this change we have replaced the javax.servlet.jsp dependency with tomcat-jasper, which will now be used for JspC Ant task.


---

* [HBASE-28921](https://issues.apache.org/jira/browse/HBASE-28921) | *Major* | **Avoid bundling hbase-webapps folder in default jars**

With this change, we no longer bundle hbase-webapps in default jars.



# HBASE  2.5.10 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-28718](https://issues.apache.org/jira/browse/HBASE-28718) | *Major* | **Should support different license name for 'Apache License, Version 2.0'**

Also accept "Apache-2.0" and "Apache Software License - Version 2.0" when aggregating license in resource bundle module.



# HBASE  2.5.9 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-28699](https://issues.apache.org/jira/browse/HBASE-28699) | *Major* | **Bump jdk and maven versions in pre commit and nighly dockerfile**

maven 3.8.6 -\> 3.9.8
temurin openjdk8 8u352-b08 -\> 8u412-b08
temurin openjdk11 11.0.17\_8 -\> 11.0.23\_9
temurin openjdk17 17.0.10\_7 -\> 17.0.11\_9


---

* [HBASE-28679](https://issues.apache.org/jira/browse/HBASE-28679) | *Major* | **Upgrade yetus to a newer version**

Upgrade yetus to 0.15.0.

Some notable differences:
Whitespace related checks are renamed to blanks.
Use xmllint instead of jrunscript for validating xml files.
For github there is an extra step to write commit status back to github but for HBase it does not work due to insufficient permission.


---

* [HBASE-28616](https://issues.apache.org/jira/browse/HBASE-28616) | *Major* | **Remove/Deprecated the rs.\* related configuration in TableOutputFormat**

Mark these two fileds in TableOutputFormat as deprecated as they do not take effect any more.

REGION\_SERVER\_CLASS
REGION\_SERVER\_IMPL

Mark these two methods in TableMapReduceUtil as deprecated as the serverClass and serverImpl parameters do not take effect any more.

void initTableReducerJob(String table, Class\<? extends TableReducer\> reducer, Job job, Class partitioner, String quorumAddress, String serverClass, String serverImpl) throws IOException
void initTableReducerJob(String table, Class\<? extends TableReducer\> reducer, Job job, Class partitioner, String quorumAddress, String serverClass, String serverImpl, boolean addDependencyJars) throws IOException


---

* [HBASE-25972](https://issues.apache.org/jira/browse/HBASE-25972) | *Major* | **Dual File Compaction**

The default compactor in HBase compacts HFiles into one file. This change introduces a new store file writer which writes the retained cells by compaction into two files, which will be called DualFileWriter. One of these files will include the live cells. This file will be called a live-version file. The other file will include the rest of the cells, that is, historical versions. This file will be called a historical-version file. DualFileWriter will work with the default compactor. The historical files will not be read for the scans scanning latest row versions. This eliminates scanning unnecessary cell versions in compacted files and thus it is expected to improve performance of these scans.


---

* [HBASE-28552](https://issues.apache.org/jira/browse/HBASE-28552) | *Major* | **Bump up bouncycastle dependency from 1.76 to 1.78**

Bump bouncycastle dependency from 1.76 to 1.78 for addressing several CVEs

CVE-2024-29857
CVE-2024-30171
CVE-2024-30172
CVE-2024-301XX(Full CVE Code not available yet)


---

* [HBASE-28517](https://issues.apache.org/jira/browse/HBASE-28517) | *Major* | **Make properties dynamically configured**

Make the following properties dynamically configured:
\* hbase.rs.evictblocksonclose
\* hbase.rs.cacheblocksonwrite
\* hbase.block.data.cacheonread


---

* [HBASE-28457](https://issues.apache.org/jira/browse/HBASE-28457) | *Major* | **Introduce a version field in file based tracker record**

Introduce a 'version' field in file based tracker record, so while downgrading, we will know that we are reading a new version of file tracker file and fail with explicit message instead of failing silently and causing possible data loss.


---

* [HBASE-28444](https://issues.apache.org/jira/browse/HBASE-28444) | *Blocker* | **Bump org.apache.zookeeper:zookeeper from 3.8.3 to 3.8.4**

Upgrade zookeeper to 3.8.4 for addressing CVE-2024-23944.


---

* [HBASE-28260](https://issues.apache.org/jira/browse/HBASE-28260) | *Major* | **Possible data loss in WAL after RegionServer crash**

Adds a new flag hbase.regionserver.wal.avoid-local-writes. When true (default false), we will avoid writing a block replica to the local datanode for WAL writes. This will improve MTTR and redundancy, but may come with a performance impact for WAL writes. It's recommended to enable, but monitor performance in doing so if that is a concern for you.



# HBASE  2.5.8 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-28204](https://issues.apache.org/jira/browse/HBASE-28204) | *Major* | **Region Canary can take lot more time If any region (except the first region) starts with delete markers**

Canary is using Scan for first region of the table and Get for rest of the region. RAW Scan was only enabled for first region of any table. If a region has high number of deleted rows for the first row of the key-space, then It can take really long time for Get to finish execution.

With this change, Region canary will use scan to validate that every region is accessible and also enables RAW Scan if it's enabled by the user.


---

* [HBASE-28319](https://issues.apache.org/jira/browse/HBASE-28319) | *Major* | **Expose DelegatingRpcScheduler as IA.LimitedPrivate**

hbase-server now exposes a DelegatingRpcScheduler. Users who have been using hbase.region.server.rpc.scheduler.factory.class to override the default behavior of the built-in schedulers may find it beneficial to have their implementation extend this new class. This will insulate you from breaking interface changes down the line.


---

* [HBASE-28306](https://issues.apache.org/jira/browse/HBASE-28306) | *Major* | **Add property to customize Version information**

Added a new build property -Dversioninfo.version which can be used to influence the generated Version.java class in custom build scenarios. The version specified will show up in the HMaster UI and also have implications on various version-related checks. This is an advanced usage property and it's recommended not to stray too far from the default format of major.minor.patch-suffix.



# HBASE  2.5.7 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-25549](https://issues.apache.org/jira/browse/HBASE-25549) | *Major* | **Provide a switch that allows avoiding reopening all regions when modifying a table to prevent RIT storms.**

New APIs are added to Admin, AsyncAdmin, and hbase shell to allow modifying a table without reopening all regions. Care should be used in using this API, as regions will be in an inconsistent state until they are all reopened. Whether this matters depends on the change, and some changes are disallowed (such as enabling region replication or adding/removing a column family).


---

* [HBASE-28168](https://issues.apache.org/jira/browse/HBASE-28168) | *Minor* | **Add option in RegionMover.java to isolate one or more regions on the RegionSever**

This adds a new "isolate\_regions" operation to RegionMover, which allows operators to pass a list of region encoded ids to be "isolated" in the passed RegionServer.
Regions currently deployed in the RegionServer that are not in the passed list of regions would be moved to other RegionServers. Regions in the passed list that are currently on other RegionServers would be moved to the passed RegionServer.

Please refer to the command help for further information.



# HBASE  2.5.6 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-28068](https://issues.apache.org/jira/browse/HBASE-28068) | *Minor* | **Add hbase.normalizer.merge.merge\_request\_max\_number\_of\_regions property to limit max number of regions in a merge request for merge normalization**

Added a new property "hbase.normalizer.merge.merge\_request\_max\_number\_of\_regions" to limit the max number of region to be processed for merge request in a single merge normalisation. Defaults to 100


---

* [HBASE-27956](https://issues.apache.org/jira/browse/HBASE-27956) | *Major* | **Support wall clock profiling in ProfilerServlet**

You can now do wall clock profiling with async-profiler by specifying ?event=wall query param on the profiler servlet (/prof)


---

* [HBASE-27888](https://issues.apache.org/jira/browse/HBASE-27888) | *Minor* | **Record readBlock message in log when it takes too long time**

Add a configuration parameter,which control to record read block slow in logs.
\<property\>
  \<name\>hbase.fs.reader.warn.time.ms\</name\>
  \<value\>-1\</value\>
\</property\>
If reading block cost time in milliseconds more than the threshold, a warning will be logged,the default value is -1, it means skipping record the read block slow warning log.



# HBASE  2.5.5 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27838](https://issues.apache.org/jira/browse/HBASE-27838) | *Minor* | **Update zstd-jni from version 1.5.4-2 -\> 1.5.5-2**

Bump zstd-jni from 1.5.4-2 to 1.5.5-2, which fixed a critical issue on s390x.


---

* [HBASE-27799](https://issues.apache.org/jira/browse/HBASE-27799) | *Major* | **RpcThrottlingException wait interval message is misleading between 0-1s**

The RpcThrottleException now includes millis in the message


---

* [HBASE-27762](https://issues.apache.org/jira/browse/HBASE-27762) | *Major* | **Include EventType and ProcedureV2 pid in logging via MDC**

<!-- markdown -->
Log the `o.a.h.hbase.executor.EventType` and ProcedureV2 pid in log messages via MDC. PatternLayouts on master and branch-2 have been updated to make use of the MDC variables. Note that due to LOG4J2-3660, log lines for which the MDC is empty will have extraneous characters. To opt-in on branch-2.5 or branch-2.4, make an appropriate change to `conf/log4j2.properties`.


---

* [HBASE-27808](https://issues.apache.org/jira/browse/HBASE-27808) | *Major* | **Change flatten mode for oss in our pom file**

Changed the flatten mode from default to oss. It will include these extra section in the published pom files:

name, description, url, developers, scm, inceptionYear, organization, mailingLists, issueManagement, distributionManagement.



# HBASE  2.5.4 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27748](https://issues.apache.org/jira/browse/HBASE-27748) | *Major* | **Bump jettison from 1.5.2 to 1.5.4**

Bump jettison from 1.5.2 to 1.5.4 for CVE-2023-1436.


---

* [HBASE-27741](https://issues.apache.org/jira/browse/HBASE-27741) | *Minor* | **Fall back to protoc osx-x86\_64 on Apple Silicon**

<!-- markdown -->
This change introduces and automatically applies a new profile for osx-aarch_64 hosts named `apple-silicon-workaround`. This profile overrides the property `os.detected.classifier` with the value `osx-x86_64`. The intention is that this change will permit the build to proceed with the x86 version of `protoc`, making use of the Rosetta instruction translation service built into the OS. If you'd like to provide and make use of your own aarch_64 `protoc`, you can disable this profile on the command line by adding `-P'!apple-silicon-workaround'`, or through configuration in your `settings.xml`.


---

* [HBASE-27651](https://issues.apache.org/jira/browse/HBASE-27651) | *Minor* | **hbase-daemon.sh foreground\_start should propagate SIGHUP and SIGTERM**

<!-- markdown -->
Introduce separate `trap`s for SIGHUP vs. the rest. Treat `SIGINT`, `SIGKILL`, and `EXIT` identically, as before. Use the signal name without `SIG` prefix for increased portability, as per the POSIX man page for `trap`.

`SIGTERM` handler will now honor `HBASE_STOP_TIMEOUT` as described in the file header.


---

* [HBASE-27250](https://issues.apache.org/jira/browse/HBASE-27250) | *Minor* | **MasterRpcService#setRegionStateInMeta does not support replica region encodedNames or region names**

MasterRpcServices#setRegionStateInMeta can now work with both primary and timeline-consistent replica regions.



# HBASE  2.5.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27506](https://issues.apache.org/jira/browse/HBASE-27506) | *Minor* | **Optionally disable sorting directories by size in CleanerChore**

Added \`hbase.cleaner.directory.sorting\` configuration to enable the CleanerChore to sort the subdirectories by consumed space and start the cleaning with the largest subdirectory. Enabled by default.


---

* [HBASE-27494](https://issues.apache.org/jira/browse/HBASE-27494) | *Minor* | **Client meta cache clear by exception metrics are missing some cases**

Patch available at https://github.com/apache/hbase/pull/4902


---

* [HBASE-27490](https://issues.apache.org/jira/browse/HBASE-27490) | *Major* | **Locating regions for all actions of batch requests can exceed operation timeout**

The first step of submitting a multi request is to resolve all region locations for all actions in the request. If meta is slow, previously it was possible to exceed the configured operation timeout in this phase. Now, the operation timeout will be checked before each region location lookup. Once exceeded, the multi request will be failed but the region locations that had been looked up should remain in the cache (making future requests more likely to succeed).


---

* [HBASE-27513](https://issues.apache.org/jira/browse/HBASE-27513) | *Major* | **Modify README.txt to mention how to contribue**

Remove README.txt and replace it with README.md.
Add a 'How to Contribute' section to tell contributors how to acquire a jira account.


---

* [HBASE-27498](https://issues.apache.org/jira/browse/HBASE-27498) | *Major* | **Observed lot of threads blocked in ConnectionImplementation.getKeepAliveMasterService**

added hbase.client.master.state.cache.timeout.sec for sync connection implementation ConnectionImplementation such that cached the master is running state instead always refresh the master states per RPC call.


---

* [HBASE-27233](https://issues.apache.org/jira/browse/HBASE-27233) | *Major* | **Read blocks into off-heap if caching is disabled for read**

Using Scan.setCacheBlocks(false) with on-heap LRUBlockCache will now result in significantly less heap allocations for those scans if hbase.server.allocator.pool.enabled is enabled. Previously all allocations went to on-heap if LRUBlockCache was used, but now it will go to the off-heap pool if cache blocks is enabled.


---

* [HBASE-27565](https://issues.apache.org/jira/browse/HBASE-27565) | *Major* | **Make the initial corePoolSize configurable for ChoreService**

Add 'hbase.choreservice.initial.pool.size' configuration property to set the initial number of threads for the ChoreService.


---

* [HBASE-27529](https://issues.apache.org/jira/browse/HBASE-27529) | *Major* | **Provide RS coproc ability to attach WAL extended attributes to mutations at replication sink**

New regionserver coproc endpoints that can be used by coproc at the replication sink cluster if WAL has extended attributes.
Using the new endpoints, WAL extended attributes can be transferred to Mutation attributes at the replication sink cluster.


---

* [HBASE-27575](https://issues.apache.org/jira/browse/HBASE-27575) | *Minor* | **Bump future from 0.18.2 to 0.18.3 in /dev-support**

pushed to 2.4, 2.5, branch-2, and master



# HBASE  2.5.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27434](https://issues.apache.org/jira/browse/HBASE-27434) | *Major* | **Use $revision as placeholder for maven version to make it easier to control the version from command line**

Use ${revision} as placeholder for maven version in pom, so later you can use 'mvn install -Drevision=xxx' to specify the version at build time.
After this change, you can not use mvn versions:set to bump the version, instead. you should just modify the parent pom to change the value of the 'revision' property in the properties section.


---

* [HBASE-27472](https://issues.apache.org/jira/browse/HBASE-27472) | *Major* | **The personality script set wrong hadoop2 check version for branch-2**

This only affects branch-2 but for aliging the personality scripts across all active branches, we apply it to all active branches.


---

* [HBASE-27443](https://issues.apache.org/jira/browse/HBASE-27443) | *Major* | **Use java11 in the general check of our jenkins job**

Change to use java 11 in nightly and pre commit jobs.

Bump error prone to 2.16 and force using jdk11 when error prone is enabled.



# HBASE  2.5.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27381](https://issues.apache.org/jira/browse/HBASE-27381) | *Major* | **Still seeing 'Stuck' in static initialization creating RegionInfo instance**

Static constant UNDEFINED has been removed from public interface RegionInfo. This is a breaking change, but resolves a critical deadlock bug. This constant was never meant to be exposed and has been deprecated since version 2.3.2 with no replacement.


---

* [HBASE-27372](https://issues.apache.org/jira/browse/HBASE-27372) | *Major* | **Update java versions in our Dockerfiles**

Upgrade java version to 11.0.16.1 and 8u345b01 in the docker files which are used in our pre commit and nightly jobs.
Remove JDK7 in these docker files as we do not support JDK7 any more.


---

* [HBASE-27371](https://issues.apache.org/jira/browse/HBASE-27371) | *Major* | **Bump spotbugs version**

Bump spotbugs version from 4.2.2 to 4.7.2. Also bump maven spotbugs plugin version from 4.2.0 to 4.7.2.0.


---

* [HBASE-27224](https://issues.apache.org/jira/browse/HBASE-27224) | *Major* | **HFile tool statistic sampling produces misleading results**

Fixes HFilePrettyPrinter's calculation of min and max size for an HFile so that it will truly be the min and max for the whole file. Previously was based on just a sampling, as with the histograms. Additionally adds a new argument to the tool '-d' which prints detailed range counts for each summary. The range counts give you the exact count of rows/cells that fall within the pre-defined ranges, useful for giving more detailed insight into outliers.


---

* [HBASE-27340](https://issues.apache.org/jira/browse/HBASE-27340) | *Minor* | **Artifacts with resolved profiles**

Published poms now contain runtime dependencies only; build and test time dependencies are stripped. Profiles are also now resolved and in-lined at publish time. This removes the need/ability of downstreamers shaping hbase dependencies via enable/disable of hbase profile settings (Implication is that now the hbase project publishes artifacts for hadoop2 and for hadoop3, and so on).


---

* [HBASE-27320](https://issues.apache.org/jira/browse/HBASE-27320) | *Minor* | **hide some sensitive configuration information in the UI**

hide superuser and password related settings in the configuration UI



# HBASE  2.5.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-27305](https://issues.apache.org/jira/browse/HBASE-27305) | *Minor* | **add an option to skip file splitting when bulkload hfiles**

Add a 'hbase.loadincremental.fail.if.need.split.hfile' configuration. If set to true, th bulk load operation will fail immediately if we need to split the hfiles. This can be used to prevent unexpected time consuming bulk load operation.


---

* [HBASE-27104](https://issues.apache.org/jira/browse/HBASE-27104) | *Major* | **Add a tool command list\_unknownservers**

Introduce a shell command 'list\_unknownservers' to list unknown servers.


---

* [HBASE-27129](https://issues.apache.org/jira/browse/HBASE-27129) | *Major* | **Add a config that allows us to configure region-level storage policies**

Add a 'hbase.hregion.block.storage.policy' so you can config storage policy at region level. This is useful when you want to control the storage policy for the directories other than CF directories, such as .splits, .recovered.edits, etc.


---

* [HBASE-27089](https://issues.apache.org/jira/browse/HBASE-27089) | *Minor* | **Add “commons.crypto.stream.buffer.size” configuration**

Add a 'commons.crypto.stream.buffer.size' config for setting the buffer size when doing AES crypto for RPC.


---

* [HBASE-27299](https://issues.apache.org/jira/browse/HBASE-27299) | *Major* | **Bump minimum hadoop 2 version to 2.10.2**

Now the minimum support hadoop 2.x version is 2.10.2 for hbase 2.5+.


---

* [HBASE-27281](https://issues.apache.org/jira/browse/HBASE-27281) | *Critical* | **Add default implementation for Connection$getClusterId**

Adds a default null implementation for Connection$getClusterId. Downstream applications should implement this method.


---

* [HBASE-27229](https://issues.apache.org/jira/browse/HBASE-27229) | *Major* | **BucketCache statistics should not count evictions by hfile**

The eviction metric for the BucketCache has been updated to only count evictions triggered by the eviction process (i.e responding to cache pressure). This brings it in-line with the other cache implementations, with the goal of this metric being to give operators insight into cache pressure. Other evictions by hfile or drain to storage engine failure, etc, no longer count towards the eviction rate.


---

* [HBASE-27204](https://issues.apache.org/jira/browse/HBASE-27204) | *Critical* | **BlockingRpcClient will hang for 20 seconds when SASL is enabled after finishing negotiation**

When Kerberos authentication succeeds, on the server side, after receiving the final SASL token from the client, we simply wait for the client to continue by sending the connection header. After HBASE-24579, on the client side, an additional readStatus() was added, which assumed that after negotiation has completed a status code will be sent. However when authentication has succeeded the server will not send one. As a result the client would hang and only throw an exception when the configured read timeout is reached, which is 20 seconds by default. This was especially noticeable when using BlockingRpcClient as the client implementation. HBASE-24579 was reverted to correct this issue.


---

* [HBASE-27219](https://issues.apache.org/jira/browse/HBASE-27219) | *Minor* | **Change JONI encoding in RegexStringComparator**

In RegexStringComparator an infinite loop can occur if an invalid UTF8 is encountered. We now use joni's NonStrictUTF8Encoding instead of UTF8Encoding to avoid the issue.


---

* [HBASE-20499](https://issues.apache.org/jira/browse/HBASE-20499) | *Minor* | **Replication/Priority executors can use specific max queue length as default value instead of general maxQueueLength**

Added new config 'hbase.ipc.server.replication.max.callqueue.length'


---

* [HBASE-27048](https://issues.apache.org/jira/browse/HBASE-27048) | *Major* | **Server side scanner time limit should account for time in queue**

Server will now account for queue time when determining how long a scanner can run before heartbeat should be returned. This should help avoid timeouts when server is overloaded.


---

* [HBASE-27148](https://issues.apache.org/jira/browse/HBASE-27148) | *Major* | **Move minimum hadoop 3  support version to 3.2.3**

Bump the minimum hadoop 3 dependency to 3.2.3.

Also upgrade apache-avro to 1.11.0 and exclude all jackson 1.x dependencies since all jackson 1.x versions have vulnerabilities.

Notice that for hadoop 2 dependency we will need to include jackson 1.x because hadoop directly depend on it.


---

* [HBASE-27078](https://issues.apache.org/jira/browse/HBASE-27078) | *Major* | **Allow configuring a separate timeout for meta scans**

Similar to hbase.read.rpc.timeout and hbase.client.scanner.timeout.period for normal scans, this issue adds two new configs for meta scans: hbase.client.meta.read.rpc.timeout and hbase.client.meta.scanner.timeout.period.  Each meta scan RPC call will be limited by hbase.client.meta.read.rpc.timeout, while hbase.client.meta.scanner.timeout.period acts as an overall operation timeout.

Additionally, for 2.5.0, normal Table-based scan RPCs will now be limited by hbase.read.rpc.timeout if configured, instead of hbase.rpc.timeout. This behavior already existed for AsyncTable scans.


---

* [HBASE-13126](https://issues.apache.org/jira/browse/HBASE-13126) | *Critical* | **Provide alternate mini cluster classes other than HBTU for downstream users to write unit tests**

Introduce a TestingHBaseCluster for users to implement integration test with mini hbase cluster.
See this section https://hbase.apache.org/book.html#\_integration\_testing\_with\_an\_hbase\_mini\_cluster in the ref guide for more details on how to use it.
TestingHBaseCluster also allowes you to start a mini hbase cluster based on external HDFS cluster and zookeeper cluster, please see the release note of HBASE-26167 for more details.
HBaseTestingUtility is marked as deprecated and will be 'removed' in the future.


---

* [HBASE-27028](https://issues.apache.org/jira/browse/HBASE-27028) | *Minor* | **Add a shell command  for flushing master local region**

Introduced a shell command flush\_master\_store for flushing master local region after HBASE-27028


---

* [HBASE-27125](https://issues.apache.org/jira/browse/HBASE-27125) | *Minor* | **The batch size of cleaning expired mob files should have an upper bound**

Configure "hbase.master.mob.cleaner.batch.size.upper.bound" to set a proper batch size of cleaning expired mob files, its default value is 10000.


---

* [HBASE-26167](https://issues.apache.org/jira/browse/HBASE-26167) | *Major* | **Allow users to not start zookeeper and dfs cluster when using TestingHBaseCluster**

Introduce two new methods when creating a TestingHBaseClusterOption.

public Builder useExternalDfs(String uri)
public Builder useExternalZooKeeper(String connectString)

Users can use these two methods to specify external zookeeper or HDFS cluster to be used by the TestingHBaseCluster.


---

* [HBASE-27108](https://issues.apache.org/jira/browse/HBASE-27108) | *Blocker* | **Revert HBASE-25709**

HBASE-25709 caused a regression for scans that result in a large number of rows and has been reverted in this release.


---

* [HBASE-26923](https://issues.apache.org/jira/browse/HBASE-26923) | *Minor* | **PerformanceEvaluation support encryption option**

Add a new command line argument: --encryption to enable encryptopn in PerformanceEvaluation tool.

Usage:
 encryption          Encryption type to use (AES, ...). Default: 'NONE'"

Examples:
 To run a AES encryption sequentialWrite:
 $ bin/hbase org.apache.hadoop.hbase.PerformanceEvaluation --table=xxx --encryption='AES' sequentialWrite 10


---

* [HBASE-26826](https://issues.apache.org/jira/browse/HBASE-26826) | *Major* | **Backport StoreFileTracker (HBASE-26067, HBASE-26584, and others) to branch-2.5**

Introduces the StoreFileTracker interface to HBase. This is a server-side interface which abstracts how a Store (column family) knows what files should be included in that Store. Previously, HBase relied on a listing the directory a Store used for storage to determine the files which should make up that Store.

\*\*\* StoreFileTracker is EXPERIMENTAL in 2.5. Use at your own risk. \*\*\*

After this feature, there are two implementations of StoreFileTrackers. The first (and default) implementation is listing the Store directory. The second is a new implementation which records files which belong to a Store within each Store. Whenever the list of files that make up a Store change, this metadata file will be updated.

This feature is notable in that it better enables HBase to function on storage systems which do not provide the typical posix filesystem semantics, most importantly, those which do not implement a file rename operation which is atomic. Storage systems which do not implement atomic renames often implement a rename as a copy and delete operation which amplifies the I/O costs by 2x.

At scale, this feature should have a 2x reduction in I/O costs when using storage systems that do not provide atomic renames, most importantly in HBase compactions and memstore flushes. See the corresponding section, "Store File Tracking", in the HBase book for more information on how to use this feature.

The file based StoreFileTracker, FileBasedStoreFileTracker, is currently incompatible with the Medium Objects (MOB) feature. Do not enable them together.


---

* [HBASE-26649](https://issues.apache.org/jira/browse/HBASE-26649) | *Major* | **Support meta replica LoadBalance mode for RegionLocator#getAllRegionLocations()**

When setting 'hbase.locator.meta.replicas.mode' to "LoadBalance" at HBase client, RegionLocator#getAllRegionLocations() now load balances across all Meta Replica Regions. Please note,  results from non-primary meta replica regions may contain stale data.


---

* [HBASE-27055](https://issues.apache.org/jira/browse/HBASE-27055) | *Minor* | **Add additional comments when using HBASE\_TRACE\_OPTS with standalone mode**

hbase-env.sh has been updated with an optional configuration HBASE\_OPTS for standalone mode

# export HBASE\_OPTS="${HBASE\_OPTS} ${HBASE\_TRACE\_OPTS} -Dotel.resource.attributes=service.name=hbase-standalone"


---

* [HBASE-26342](https://issues.apache.org/jira/browse/HBASE-26342) | *Major* | **Support custom paths of independent configuration and pool for hfile cleaner**

Configure the custom hifle paths (under archive directory), e.g. data/default/testTable1,data/default/testTable2 by "hbase.master.hfile.cleaner.custom.paths".
Configure hfile cleaner classes for the custom paths by "hbase.master.hfilecleaner.custom.paths.plugins".
Configure the shared pool size of custom hfile cleaner paths by "hbase.cleaner.custom.hfiles.pool.size".


---

* [HBASE-27047](https://issues.apache.org/jira/browse/HBASE-27047) | *Minor* | **Fix typo for metric drainingRegionServers**

Fix typo for metric drainingRegionServers. Change metric name from draininigRegionServers to drainingRegionServers.


---

* [HBASE-25465](https://issues.apache.org/jira/browse/HBASE-25465) | *Minor* | **Use javac --release option for supporting cross version compilation**

When compiling with java 11 and above, we will use --release 8 to maintain java 8 compatibility.
Also upgrade jackson to 2.13.1 because in hbase-thirdparty 4.1.0 we shade jackson 2.13.1.


---

* [HBASE-27024](https://issues.apache.org/jira/browse/HBASE-27024) | *Major* | **The User API and Developer API links are broken on hbase.apache.org**

Upgrade maven-site-plugin to 3.12.0, maven-javadoc-plugin to 3.4.0.


---

* [HBASE-26986](https://issues.apache.org/jira/browse/HBASE-26986) | *Major* | **Trace a one-shot execution of a Master procedure**

Individual executions of procedures are now wrapped in a tracing span. No effort is made to coordinate multiple executions back to a common PID.


---

* [HBASE-27013](https://issues.apache.org/jira/browse/HBASE-27013) | *Major* | **Introduce read all bytes when using pread for prefetch**

Introduce optional flag hfile.pread.all.bytes.enabled for pread that must read full bytes with the next block header. This feature is specially helpful when users are running HBase with Blob storage like S3 and Azure Blob storage. Especially when using HBase with S3A and set fs.s3a.experimental.input.fadvise=sequential, it can save input stream from seeking backward that spent more time on storage connection reset time.


---

* [HBASE-26899](https://issues.apache.org/jira/browse/HBASE-26899) | *Major* | **Run spotless:apply**

Run spotless:apply to format our code base.
When viewing 'git blame', you may find a file has a large amount lines are modified by the commit of HBASE-26899, so you need to go back to the commit before this commit, and viewing 'git blame' again.


---

* [HBASE-26617](https://issues.apache.org/jira/browse/HBASE-26617) | *Major* | **Use spotless to reduce the pain on fixing checkstyle issues**

Use spotless to format our java file and pom file, using the hbase\_eclipse\_formatter.xml and eclipse.importerorder file under our dev-support directory.
On all branches, the ratchetFrom is set the commit just before the commit which introduces the spotless plugin, so we will only format the files which are touched in later commits.
From now on, you should type mvn spotless:apply before generating a PR, the spotless plugin will fix most of the format issues for you.


---

* [HBASE-22349](https://issues.apache.org/jira/browse/HBASE-22349) | *Major* | **Stochastic Load Balancer skips balancing when node is replaced in cluster**

StochasticLoadBalancer now respects the hbase.regions.slop configuration value as another factor in determining whether to attempt a balancer run. If any regionserver has a region count outside of the target range, the balancer will attempt to balance. Using the default 0.2 value, the target range is 80%-120% of the average (mean) region count per server. Whether the balancer will ultimately move regions will still depend on the weights of StochasticLoadBalancer's cost functions.


---

* [HBASE-26807](https://issues.apache.org/jira/browse/HBASE-26807) | *Major* | **Unify CallQueueTooBigException special pause with CallDroppedException**

Introduces a new config "hbase.client.pause.server.overloaded", deprecating old "hbase.client.pause.cqtbe". The new config specifies a special pause time to use when the client receives an exception from the server indicating that it is overloaded. Currently this applies to CallQueueTooBigException and CallDroppedException.


---

* [HBASE-26891](https://issues.apache.org/jira/browse/HBASE-26891) | *Minor* | **Make MetricsConnection scope configurable**

Adds a new "hbase.client.metrics.scope" config which allows users to define a custom scope for each Connection's metric instance. The default scope has also been changed to include the clusterId of the Connection, which should help differentiate metrics for processes connecting to multiple clusters. The scope is added to the ObjectName for JMX beans, so can be used to query for metrics for a particular connection. Using a custom scope might be useful in cases where you maintain separate Connections for writes vs reads. In that case you can set the scope appropriately and differentiate metrics for each.


---

* [HBASE-26618](https://issues.apache.org/jira/browse/HBASE-26618) | *Minor* | **Involving primary meta region in meta scan with CatalogReplicaLoadBalanceSimpleSelector**

When META replica LoadBalance mode is enabled at client-side, clients will try to read from one META region first. If META location is from any non-primary META regions, in case of errors, it will fall back to the primary META region.


---

* [HBASE-26245](https://issues.apache.org/jira/browse/HBASE-26245) | *Major* | **Store region server list in master local region**

A typical HBase deployment on cloud is to store the data other than WAL on OSS, and store the WAL data on a special HDFS cluster. A common operation is to rebuild the cluster with fresh new zk cluster and HDFS cluster, with only the old rootdir on OSS. But it requires extra manual steps since we rely on the WAL directory to find out previous live region servers, so we can schedule SCP to bring regions online.
After this issue, now it is possible to rebuild the cluster without extra manual  steps as we will also store the previous live region servers in master local region.
But notice that you'd better stop masters first and then region servers when rebuilding, as some tests show that if there are some pending procedures, the new clusters may still hang.


---

* [HBASE-21065](https://issues.apache.org/jira/browse/HBASE-21065) | *Major* | **Try ROW\_INDEX\_V1 encoding on meta table (fix bloomfilters on meta while we are at it)**

Enables ROW\_INDEX\_V1 encoding on hbase:meta by default. Also enables blooms.

Will NOT enable encoding and blooms on upgrade. Operator will need to do this manually by editing hbase:meta schema (Or we provide a migration script to enable these configs -- out-of-scope for this JIRA).


---

* [HBASE-25895](https://issues.apache.org/jira/browse/HBASE-25895) | *Major* | **Implement a Cluster Metrics JSON endpoint**

Introduces a REST+JSON endpoint on the master info server port, which is used to render the "Region Visualizer" section of the Master Status page. When enabled, access to this API is gated by authentication only, just like the Master Status page. This API is considered InterfaceAudience.Private, can change or disappear without notice.


---

* [HBASE-26802](https://issues.apache.org/jira/browse/HBASE-26802) | *Blocker* | **Backport the log4j2 changes to branch-2**

Use log4j2 instead of log4j for logging.
Exclude log4j dependency from hbase and transitive dependencies, use log4j-1.2-api as test dependency for bridging as hadoop still need log4j for some reasons. Copy FileAppender implementation in hbase-logging as the ContainerLogAppender for YARN NodeManager extends it. All log4j.properties files have been replaced by log4j2.properties.


---

* [HBASE-26552](https://issues.apache.org/jira/browse/HBASE-26552) | *Major* | **Introduce retry to logroller to avoid abort**

For retrying to roll log, the wait timeout is limited by "hbase.regionserver.logroll.wait.timeout.ms",
and the max retry time is limited by "hbase.regionserver.logroll.retries".
Do not retry to roll log is the default behavior.


---

* [HBASE-26640](https://issues.apache.org/jira/browse/HBASE-26640) | *Major* | **Reimplement master local region initialization to better work with SFT**

Introduced a 'hbase.master.store.region.file-tracker.impl' config to specify the store file tracker implementation for master local region.

If not present, master local region will use the cluster level store file tracker implementation.


---

* [HBASE-26673](https://issues.apache.org/jira/browse/HBASE-26673) | *Major* | **Implement a shell command for change SFT implementation**

Introduced two shell commands for change table's or family's sft:

change\_sft:
  Change table's or table column family's sft. Examples:
    hbase\> change\_sft 't1','FILE'
    hbase\> change\_sft 't2','cf1','FILE'

change\_sft\_all:
  Change all of the tables's sft matching the given regex:
    hbase\> change\_sft\_all 't.\*','FILE'
    hbase\> change\_sft\_all 'ns:.\*','FILE'
    hbase\> change\_sft\_all 'ns:t.\*','FILE'


---

* [HBASE-26742](https://issues.apache.org/jira/browse/HBASE-26742) | *Major* | **Comparator of NOT\_EQUAL NULL is invalid for checkAndMutate**

The semantics of checkAndPut for null(or empty) value comparator is changed, the old match is always true.
But we should consider that  EQUAL or NOT\_EQUAL for null check is a common usage, so the semantics of checkAndPut for matching null is correct now.
There is rare use of LESS or GREATER null, so keep the semantics for them.


---

* [HBASE-26688](https://issues.apache.org/jira/browse/HBASE-26688) | *Major* | **Threads shared EMPTY\_RESULT may lead to unexpected client job down.**

Result#advance with empty cell list will always return false but not raise NoSuchElementException when called multiple times.
This is a behavior change so it is an 'incompatible change', but since it will not introduce any compile error and the old behavior is 'broken', so we also fix it for current release branches.


---

* [HBASE-26473](https://issues.apache.org/jira/browse/HBASE-26473) | *Major* | **Introduce \`db.hbase.container\_operations\` span attribute**

<!-- markdown --> Introduces an HBase-specific tracing attribute called `db.hbase.container_operations`. This attribute contains a list of table operations contained in the batch/list/envelope operation, allowing an operator to seek, for example, all `PUT` operations, even they occur inside of a `BATCH` or `COMPARE_AND_SET`.


---

* [HBASE-26469](https://issues.apache.org/jira/browse/HBASE-26469) | *Critical* | **correct HBase shell exit behavior to match code passed to exit**

<!-- markdown -->
User input handling has been refactored to make use of IRB sessions directly and the HBase shell attempts to ensure user provided calls to exit are able to convey failure and success.

Those scripting use of the HBase shell should be aware that the exit code may have changed:
    * a 0 code, or no code, passed to a call to exit from stdin in non-interactive mode will now exit cleanly. in prior versions this would have exited with an error and non-zero exit code. (note that in HBase 2.4.x this call will still result in a non-zero exit code)
    * for other combinations of passing in an initialization script or reading from stdin with using the non-interactive flag, the exit code being 0 or non-0 should now line up with releases prior to 2.4, which is a change in behavior compared to versions 2.4.0 - 2.4.9.

Please see the issue details for a table of expected exit codes.


---

* [HBASE-26631](https://issues.apache.org/jira/browse/HBASE-26631) | *Major* | **Upgrade junit to 4.13.2**

Upgrade junit to 4.13.2 for addressing CVE-2020-15250.


---

* [HBASE-26347](https://issues.apache.org/jira/browse/HBASE-26347) | *Major* | **Support detect and exclude slow DNs in fan-out of WAL**

This issue provides the method to detect slow datanodes by checking the packets processing time of each datanode connected by the WAL. When a datanode is considered slow, the datanode will be added to an exclude cache on the regionserver, and every stream created will exclude all the cached slow datanodes in a configured period. The exclude logic cooperate with the log rolling logic, will react more sensitively to the lower slow datanodes, whatever there is hardware failure or hotspots.

hbase.regionserver.async.wal.max.exclude.datanode.count（default 3）and hbase.regionserver.async.wal.exclude.datanode.info.ttl.hour (default 6) means no more than 3 slow datanodes will be excluded on one regionserver, and the exclude cache for the slow datanodes is valid in 6 hours.

There are two conditions used to determine whether a datanode is slow,
1. For small packet, we just have a simple time limit(configured by hbase.regionserver.async.wal.datanode.slow.packet.process.time.millis, default 6s), without considering the size of the packet.

2. For large packet, we will calculate the speed, and check if the speed (configured by hbase.regionserver.async.wal.datanode.slow.packet.speed.min.kbs, default 20KB/s) is too slow.

The large and small split point is configured by hbase.regionserver.async.wal.datanode.slow.check.speed.packet.data.length.min (default 64KB).


---

* [HBASE-26537](https://issues.apache.org/jira/browse/HBASE-26537) | *Major* | **FuzzyRowFilter backwards compatibility**

HBASE-15676 introduced a backwards incompatible change which makes it impossible to upgrade server first, then client, without potentially incorrect scanning results if FuzzyRowFilter is in use. This change corrects that problem by introducing a backwards compatible workaround.


---

* [HBASE-26542](https://issues.apache.org/jira/browse/HBASE-26542) | *Minor* | **Apply a \`package\` to test protobuf files**

The protobuf structures used in test are all now scoped by the package name \`hbase.test.pb\`.


---

* [HBASE-26512](https://issues.apache.org/jira/browse/HBASE-26512) | *Major* | **Make timestamp format configurable in HBase shell scan output**

HBASE-23930 changed the formatting of the timestamp attribute on each Cell as displayed by the HBase shell to be formatted as an ISO-8601 string rather that milliseconds since the epoch. Some users may have logic which expects the timestamp to be displayed as milliseconds since the epoch. This change introduces the configuration property hbase.shell.timestamp.format.epoch which controls whether the shell will print an ISO-8601 formatted timestamp (the default "false") or milliseconds since the epoch ("true").


---

* [HBASE-26363](https://issues.apache.org/jira/browse/HBASE-26363) | *Major* | **OpenTelemetry configuration support for per-process service names**

<!-- markdown -->Each HBase process can have its own `service.name`, a value that can be completely customized by the operator. See the comment and examples in conf/hbase-env.sh.


---

* [HBASE-26362](https://issues.apache.org/jira/browse/HBASE-26362) | *Major* | **Upload mvn site artifacts for nightly build to nightlies**

Now we will upload the site artifacts to nightlies for nightly build as well as pre commit build.


---

* [HBASE-26316](https://issues.apache.org/jira/browse/HBASE-26316) | *Minor* | **Per-table or per-CF compression codec setting overrides**

It is now possible to specify codec configuration options as part of table or column family schema definitions. The configuration options will only apply to the defined scope. For example:

  hbase\> create 'sometable', \\
    { NAME =\> 'somefamily', COMPRESSION =\> 'ZSTD' }, \\
    CONFIGURATION =\> { 'hbase.io.compress.zstd.level' =\> '9' }


---

* [HBASE-26329](https://issues.apache.org/jira/browse/HBASE-26329) | *Major* | **Upgrade commons-io to 2.11.0**

Upgraded commons-io to 2.11.0.


---

* [HBASE-26186](https://issues.apache.org/jira/browse/HBASE-26186) | *Major* | **jenkins script for caching artifacts should verify cached file before relying on it**

Add a '--verify-tar-gz' option to cache-apache-project-artifact.sh for verifying whether the cached file can be parsed as a gzipped tarball.
Use this option in our nightly job to avoid failures on broken cached hadoop tarballs.


---

* [HBASE-26339](https://issues.apache.org/jira/browse/HBASE-26339) | *Major* | **SshPublisher will skip uploading artifacts if the build is failure**

Now we will mark build as unstable instead of failure when the yetus script returns error. This is used to solve the problem that the SshPublisher jenkins plugin will skip uploading artifacts if the build is marked as failure. In fact, the test output will be more important when there are UT failures.


---

* [HBASE-26317](https://issues.apache.org/jira/browse/HBASE-26317) | *Major* | **Publish the test logs for pre commit jenkins job to nightlies**

Now we will upload test\_logs.zip for our pre commit jobs to nightlies to save space on jenkins node. You can see the test\_logs.txt to get the actual url of the test\_logs.zip, or visit https://nightlies.apache.org/hbase directly to find the artifacts.


---

* [HBASE-26313](https://issues.apache.org/jira/browse/HBASE-26313) | *Major* | **Publish the test logs for our nightly jobs to nightlies.apache.org**

Now we will upload test\_logs.zip for our nightly jobs to nightlies to save space on jenkins node. You can see the test\_logs.txt to get the actual url of the test\_logs.zip, or visit https://nightlies.apache.org/hbase directly to find the artifacts.


---

* [HBASE-26318](https://issues.apache.org/jira/browse/HBASE-26318) | *Major* | **Publish test logs for flaky jobs to nightlies**

Now we will upload the surefire output for our flaky test jobs to nightlies to save space on jenkins node. You can see the test\_logs.txt to get the actual url of the surefire output, or visit https://nightlies.apache.org/hbase directly to find the artifacts.


---

* [HBASE-26259](https://issues.apache.org/jira/browse/HBASE-26259) | *Major* | **Fallback support to pure Java compression**

This change introduces provided compression codecs to HBase as
 new Maven modules. Each module provides compression codec support that formerly required Hadoop native codecs, which in turn relies on native code integration, which may or may not be available on a given hardware platform or in an operational environment. We now provide codecs in the HBase distribution for users whom for whatever reason cannot or do not wish to deploy the Hadoop native codecs.


---

* [HBASE-26274](https://issues.apache.org/jira/browse/HBASE-26274) | *Major* | **Create an option to reintroduce BlockCache to mapreduce job**

Introduce \`hfile.onheap.block.cache.fixed.size\` and default to disable. When using ClientSideRegionScanner, it will be enabled with a fixed size for caching INDEX/LEAF\_INDEX block when a client, e.g. snapshot scanner, scans the entire HFile and does not need to seek/reseek to index block multiple times.


---

* [HBASE-26270](https://issues.apache.org/jira/browse/HBASE-26270) | *Minor* | **Provide getConfiguration method for Region and Store interface**

Provide 'getReadOnlyConfiguration' method for Store and Region interface


---

* [HBASE-26273](https://issues.apache.org/jira/browse/HBASE-26273) | *Major* | **TableSnapshotInputFormat/TableSnapshotInputFormatImpl should use ReadType.STREAM for scanning HFiles**

HBase's MapReduce API which can operate over HBase snapshots will now default to using ReadType.STREAM instead of ReadType.DEFAULT (which is PREAD) as a result of this change. HBase developers expect that STREAM will perform significantly better for average Snapshot-based batch jobs. Users can restore the previous functionality (using PREAD) by updating their code to explicitly set a value of \`ReadType.PREAD\` on the \`Scan\` object they provide to TableSnapshotInputFormat, or by setting the configuration property "hbase.TableSnapshotInputFormat.scanner.readtype" to "PREAD" in hbase-site.xml.


---

* [HBASE-26276](https://issues.apache.org/jira/browse/HBASE-26276) | *Major* | **Allow HashTable/SyncTable to perform rawScan when comparing cells**

Added --rawScan option to HashTable job, which allows HashTable/SyncTable to perform raw scans. If this property is omitted, it defaults to false. When used together with --versions set to a high value, SyncTable will fabricate delete markers to all old versions still hanging (not cleaned yet by major compaction), avoiding the inconsistencies reported in HBASE-21596.


---

* [HBASE-26147](https://issues.apache.org/jira/browse/HBASE-26147) | *Major* | **Add dry run mode to hbase balancer**

This change adds new API to the Admin interface for triggering Region balancing on a cluster. A new BalanceRequest object was introduced which allows for configuring a dry run of the balancer (compute a plan without enacting it) and running the balancer in the presence of RITs. Corresponding API was added to the HBase shell as well.


---

* [HBASE-26204](https://issues.apache.org/jira/browse/HBASE-26204) | *Major* | **VerifyReplication should obtain token for peerQuorumAddress too**

VerifyReplication obtains tokens even if the peer quorum parameter is used. VerifyReplication with peer quorum can be used for secure clusters also.


---

* [HBASE-26180](https://issues.apache.org/jira/browse/HBASE-26180) | *Major* | **Introduce a initial refresh interval for RpcConnectionRegistry**

Introduced a 'hbase.client.bootstrap.initial\_refresh\_delay\_secs' config to control the first refresh delay for bootstrap nodes. The default value is 1/10 of periodic refresh interval.


---

* [HBASE-26173](https://issues.apache.org/jira/browse/HBASE-26173) | *Major* | **Return only a sub set of region servers as bootstrap nodes**

Introduced a 'hbase.client.bootstrap.node.limit' config to limit the max number of bootstrap nodes we return to client. The default value is 10.


---

* [HBASE-26182](https://issues.apache.org/jira/browse/HBASE-26182) | *Major* | **Allow disabling refresh of connection registry endpoint**

Set 'hbase.client.bootstrap.refresh\_interval\_secs' to -1 can disable refresh of connection registry endpoint.


---

* [HBASE-26212](https://issues.apache.org/jira/browse/HBASE-26212) | *Minor* | **Allow AuthUtil automatic renewal to be disabled**

This change introduces a configuration property "hbase.client.keytab.automatic.renewal" to control AuthUtil, the class which automatically tries to perform Kerberos ticket renewal in client applications. This configuration property defaults to "true", meaning that AuthUtil will automatically attempt to renew Kerberos tickets per its capabilities. Those who want AuthUtil to not renew client Kerberos tickets can set this property to be "false".


---

* [HBASE-26172](https://issues.apache.org/jira/browse/HBASE-26172) | *Major* | **Deprecate MasterRegistry**

MasterRegistry is deprecated. Please use RpcConnectionRegistry instead.


---

* [HBASE-26193](https://issues.apache.org/jira/browse/HBASE-26193) | *Major* | **Do not store meta region location as permanent state on zookeeper**

Introduce a new 'info' family in master local region for storing the location of meta regions.
We will still mirror the location of meta regions to ZooKeeper, for backwards compatibility. But now you can also clean the meta location znodes(usually prefixed with 'meta-region-server') on ZooKeeper without mess up the cluster state. You can get a clean restart of the cluster, and after restarting, we will mirror the location of meta regions to ZooKeeper again.


---

* [HBASE-24652](https://issues.apache.org/jira/browse/HBASE-24652) | *Minor* | **master-status UI make date type fields sortable**

Makes RegionServer 'Start time' sortable in the Master UI


---

* [HBASE-26200](https://issues.apache.org/jira/browse/HBASE-26200) | *Major* | **Undo 'HBASE-25165 Change 'State time' in UI so sorts (#2508)' in favor of HBASE-24652**

Undid showing RegionServer 'Start time' in ISO-8601 format. Revert.


---

* [HBASE-6908](https://issues.apache.org/jira/browse/HBASE-6908) | *Major* | **Pluggable Call BlockingQueue for HBaseServer**

Can pass in a FQCN to load as the call queue implementation.

Standardized arguments to the constructor are the max queue length, the PriorityFunction, and the Configuration.

PluggableBlockingQueue abstract class provided to help guide the correct constructor signature.

Hard fails with PluggableRpcQueueNotFound if the class fails to load as a BlockingQueue\<CallRunner\>

Upstreaming on behalf of Hubspot, we are interested in defining our own custom RPC queue and don't want to get involved in necessarily upstreaming internal requirements/iterations.


---

* [HBASE-26196](https://issues.apache.org/jira/browse/HBASE-26196) | *Major* | **Support configuration override for remote cluster of HFileOutputFormat locality sensitive**

Allow any configuration for the remote cluster in HFileOutputFormat2 that could be useful the different configuration from the job's configuration is necessary to connect the remote cluster, for instance, non-secure vs secure.


---

* [HBASE-26150](https://issues.apache.org/jira/browse/HBASE-26150) | *Major* | **Let region server also carry ClientMetaService**

Introduced a RpcConnectionRegistry.

Config 'hbase.client.bootstrap.servers' to set up the initial bootstrap nodes. The registry will refresh the bootstrap server periodically or on errors. Notice that, masters and region servers both implement the necessary rpc interface so you are free to config either masters or region servers as the initial bootstrap nodes.


---

* [HBASE-26127](https://issues.apache.org/jira/browse/HBASE-26127) | *Major* | **Backport HBASE-23898 "Add trace support for simple apis in async client" to branch-2**

https://github.com/apache/hbase/commit/4fbc4c29f22e35a72e73dac3aa58359a8d8c7be3


---

* [HBASE-26160](https://issues.apache.org/jira/browse/HBASE-26160) | *Minor* | **Configurable disallowlist for live editing of loglevels**

Adds a new hbase.ui.logLevels.readonly.loggers config which takes a comma-separated list of logger names. Similar to log4j configurations, the logger names can be prefixes or a full logger name. The log level of read only loggers cannot be changed via the logLevel UI or setlevel CLI. This is useful for securing sensitive loggers, such as the SecurityLogger used for audit logs.


---

* [HBASE-26126](https://issues.apache.org/jira/browse/HBASE-26126) | *Major* | **Backport HBASE-25424 "Find a way to config OpenTelemetry tracing without directly depending on opentelemetry-sdk" to branch-2**

https://github.com/apache/hbase/commit/3d29c0c2b4520edb06c0c5d3674cdb6547a57651


---

* [HBASE-26125](https://issues.apache.org/jira/browse/HBASE-26125) | *Major* | **Backport HBASE-25401 "Add trace support for async call in rpc client" to branch-2**

https://github.com/apache/hbase/commit/ca096437d7e096b514ddda53ec2f97b85d90752d


---

* [HBASE-26154](https://issues.apache.org/jira/browse/HBASE-26154) | *Minor* | **Provide exception metric for quota exceeded and throttling**

Adds "exceptions.quotaExceeded" and "exceptions.rpcThrottling" to HBase server and Thrift server metrics.


---

* [HBASE-26098](https://issues.apache.org/jira/browse/HBASE-26098) | *Major* | **Support passing a customized Configuration object when creating TestingHBaseCluster**

Now TestingHBaseClusterOption support passing a Configuration object when building.


---

* [HBASE-26124](https://issues.apache.org/jira/browse/HBASE-26124) | *Major* | **Backport HBASE-25373 "Remove HTrace completely in code base and try to make use of OpenTelemetry" to branch-2**

https://github.com/apache/hbase/commit/f0493016062267fc37e14659d9183673d42a8f1d


---

* [HBASE-26146](https://issues.apache.org/jira/browse/HBASE-26146) | *Minor* | **Allow custom opts for hbck in hbase bin**

Adds HBASE\_HBCK\_OPTS environment variable to bin/hbase for passing extra options to hbck/hbck2. Defaults to HBASE\_SERVER\_JAAS\_OPTS if specified, or HBASE\_REGIONSERVER\_OPTS.


---

* [HBASE-26088](https://issues.apache.org/jira/browse/HBASE-26088) | *Critical* | **conn.getBufferedMutator(tableName) leaks thread executors and other problems**

The API doc for Connection#getBufferedMutator(TableName) and Connection#getBufferedMutator(BufferedMutatorParams) mentioned that when user dont pass a ThreadPool to be used, we use the ThreadPool in the Connection.  But in reality, we were creating new ThreadPool in such cases.

We are keeping the behaviour of code as is but corrected the Javadoc and also a bug of not closing this new pool while Closing the BufferedMutator.


---

* [HBASE-25986](https://issues.apache.org/jira/browse/HBASE-25986) | *Minor* | **Expose the NORMALIZARION\_ENABLED table descriptor through a property in hbase-site**

New config: hbase.table.normalization.enabled

Default value: false

Description: This config is used to set default behaviour of normalizer at table level. To override this at table level one can set NORMALIZATION\_ENABLED at table descriptor level and that property will be honored. Of course, this property at table level can only work if normalizer is enabled at cluster level using "normalizer\_switch true" command.


---

* [HBASE-26080](https://issues.apache.org/jira/browse/HBASE-26080) | *Major* | **Implement a new mini cluster class for end users**

Introduce a new TestingHBaseCluster for end users to start a mini hbase cluster in tests.

After starting the cluster, you can create a Connection with the returned Configuration instance for accessing the cluster.

Besides the mini hbase cluster, TestingHBaseCluster will also start a zookeeper cluster and dfs cluster. But you can not control the zookeeper and dfs cluster separately, as for end users, you do not need to test the behavior of HBase cluster when these systems are broken.

We provide methods for start/stop master and region server, and also the whole hbase cluster. Notice that, we do not provide methods to get the address for masters and regionservers, or locate a region, you could use the Admin interface to do this.


---

* [HBASE-22923](https://issues.apache.org/jira/browse/HBASE-22923) | *Major* | **hbase:meta is assigned to localhost when we downgrade the hbase version**

Introduced new config: hbase.min.version.move.system.tables

When the operator uses this configuration option, any version between
the current cluster version and the value of "hbase.min.version.move.system.tables"
does not trigger any auto-region movement. Auto-region movement here
refers to auto-migration of system table regions to newer server versions.
It is assumed that the configured range of versions does not require special
handling of moving system table regions to higher versioned RegionServer.
This auto-migration is done by AssignmentManager#checkIfShouldMoveSystemRegionAsync().
Example: Let's assume the cluster is on version 1.4.0 and we have
set "hbase.min.version.move.system.tables" as "2.0.0". Now if we upgrade
one RegionServer on 1.4.0 cluster to 1.6.0 (\< 2.0.0), then AssignmentManager will
not move hbase:meta, hbase:namespace and other system table regions
to newly brought up RegionServer 1.6.0 as part of auto-migration.
However, if we upgrade one RegionServer on 1.4.0 cluster to 2.2.0 (\> 2.0.0),
then AssignmentManager will move all system table regions to newly brought
up RegionServer 2.2.0 as part of auto-migration done by
AssignmentManager#checkIfShouldMoveSystemRegionAsync().

Overall, assuming we have system RSGroup where we keep HBase system tables, if we use
config "hbase.min.version.move.system.tables" with value x.y.z then while upgrading cluster to
version greater than or equal to x.y.z, the first RegionServer that we upgrade must
belong to system RSGroup only.


---

* [HBASE-25902](https://issues.apache.org/jira/browse/HBASE-25902) | *Critical* | **Add missing CFs in meta during HBase 1 to 2.3+ Upgrade**

While upgrading cluster from 1.x to 2.3+ versions, after the active master is done setting it's status as 'Initialized', it attempts to add 'table' and 'repl\_barrier' CFs in meta. Once CFs are added successfully, master is aborted with PleaseRestartMasterException because master has missed certain initialization events (e.g ClusterSchemaService is not initialized and tableStateManager fails to migrate table states from ZK to meta due to missing CFs). Subsequent active master initialization is expected to be smooth.
In the presence of multi masters, when one of them becomes active for the first time after upgrading to HBase 2.3+, it is aborted after fixing CFs in meta and one of the other backup masters will take over and become active soon. Hence, overall this is expected to be smooth upgrade if we have backup masters configured. If not, operator is expected to restart same master again manually.


---

* [HBASE-26029](https://issues.apache.org/jira/browse/HBASE-26029) | *Critical* | **It is not reliable to use nodeDeleted event to track region server's death**

Introduce a new step in ServerCrashProcedure to move the replication queues of the dead region server to other live region servers, as this is the only reliable way to get the death event of a region server.
The old ReplicationTracker related code have all been purged as they are not used any more.


---

* [HBASE-25877](https://issues.apache.org/jira/browse/HBASE-25877) | *Major* | **Add access  check for compactionSwitch**

Now calling RSRpcService.compactionSwitch, i.e, Admin.compactionSwitch at client side, requires ADMIN permission.
This is an incompatible change but it is also a bug, as we should not allow any users to disable compaction on a regionserver, so we apply this to all active branches.


---

* [HBASE-25984](https://issues.apache.org/jira/browse/HBASE-25984) | *Critical* | **FSHLog WAL lockup with sync future reuse [RS deadlock]**

Fixes a WAL lockup issue due to premature reuse of the sync futures by the WAL consumers. The lockup causes the WAL system to hang resulting in blocked appends and syncs thus holding up the RPC handlers from progressing. Only workaround without this fix is to force abort the region server.


---

* [HBASE-25993](https://issues.apache.org/jira/browse/HBASE-25993) | *Major* | **Make excluded SSL cipher suites configurable for all Web UIs**

Add "ssl.server.exclude.cipher.list" configuration to excluded cipher suites for the http server started by the InfoServer.


---

* [HBASE-25920](https://issues.apache.org/jira/browse/HBASE-25920) | *Major* | **Support Hadoop 3.3.1**

Fixes to make unit tests pass and to make it so an hbase built from branch-2 against a 3.3.1RC can run on a 3.3.1RC small cluster.


---

* [HBASE-25969](https://issues.apache.org/jira/browse/HBASE-25969) | *Major* | **Cleanup netty-all transitive includes**

We have an (old) netty-all in our produced artifacts. It is transitively included from hadoop. It is needed by MiniMRCluster referenced from a few MR tests in hbase. This commit adds netty-all excludes everywhere else but where tests will fail unless the transitive is allowed through. TODO: move MR and/or MR tests out of hbase core.


---

* [HBASE-25963](https://issues.apache.org/jira/browse/HBASE-25963) | *Major* | **HBaseCluster should be marked as IA.Public**

Change HBaseCluster to IA.Public as its sub class MiniHBaseCluster is IA.Public.


---

* [HBASE-25841](https://issues.apache.org/jira/browse/HBASE-25841) | *Minor* | **Add basic jshell support**

This change adds a new command \`hbase jshell\` command-line interface. It launches an interactive JShell session with HBase on the classpath, as well as a the client package already imported.


---

* [HBASE-25894](https://issues.apache.org/jira/browse/HBASE-25894) | *Major* | **Improve the performance for region load and region count related cost functions**

In CostFromRegionLoadFunction, now we will only recompute the cost for a given region server in regionMoved function, instead of computing all the costs every time.
Introduced a DoubleArrayCost for better abstraction, and also try to only compute the final cost on demand as the computation is also a bit expensive.


---

* [HBASE-25869](https://issues.apache.org/jira/browse/HBASE-25869) | *Major* | **WAL value compression**

WAL storage can be expensive, especially if the cell values represented in the edits are large, consisting of blobs or significant lengths of text. Such WALs might need to be kept around for a fairly long time to satisfy replication constraints on a space limited (or space-contended) filesystem.

Enable WAL compression and, with this feature, WAL value compression, to save space in exchange for slightly higher WAL append latencies. The degree of performance impact will depend on the compression algorithm selection.  SNAPPY or ZSTD are recommended algorithms, if native codec support is available. SNAPPY may even provide an overall improvement in WAL commit latency, so is the best choice. GZ can be a reasonable fallback where native codec support is not available.

To enable WAL compression, value compression, and select the desired algorithm, edit your site configuration like so:

\<!-- to enable compression --\>
\<property\>
    \<name\>hbase.regionserver.wal.enablecompression\</name\>
    \<value\>true\</value\>
\</property\>

\<!-- to enable value compression --\>
\<property\>
    \<name\>hbase.regionserver.wal.value.enablecompression\</name\>
    \<value\>true\</value\>
\</property\>

\<!-- choose the value compression algorithm —\>
\<property\>
    \<name\>hbase.regionserver.wal.value.compression.type\</name\>
    \<value\>snappy\</value\>
\</property\>


---

* [HBASE-25682](https://issues.apache.org/jira/browse/HBASE-25682) | *Major* | **Add a new command to update the configuration of all RSs in a RSGroup**

Added updateConfiguration(String groupName) admin interface & update\_rsgroup\_config to the HBase shell to reload a subset of configuration on all servers in the rsgroup.


---

* [HBASE-25032](https://issues.apache.org/jira/browse/HBASE-25032) | *Major* | **Do not assign regions to region server which has not called regionServerReport yet**

<!-- markdown -->

After this change a region server can only accept regions (as seen by master) after it's first report to master is sent successfully. Prior to this change there could be cases where the region server finishes calling regionServerStartup but is actually stuck during initialization due to issues like misconfiguration and master tries to assign regions and they are stuck because the region server is in a weird state and not ready to serve them.


---

* [HBASE-25826](https://issues.apache.org/jira/browse/HBASE-25826) | *Major* | **Revisit the synchronization of balancer implementation**

Narrow down the public facing API for LoadBalancer by removing balanceTable and setConf methods.
Redesigned the initilization sequence to simplify the initialization code. Now all the setters are just 'setter', all the initialization work are moved to initialize method.
Rename setClusterMetrics to updateClusterMetrics, as it will be called periodically while other setters will only be called once before initialization.
Add javadoc for LoadBalancer class to mention how to do synchronization on implementation classes.


---

* [HBASE-25834](https://issues.apache.org/jira/browse/HBASE-25834) | *Major* | **Remove balanceTable method from LoadBalancer interface**

Remove balanceTable method from LoadBalancer interface as we never call it outside balancer implementation.
Mark balanceTable method as protected in BaseLoadBalancer.
Mark balanceCluster method as final in BaseLoadBalancer, the implementation classes should not override it anymore, just implement the balanceTable method is enough.


---

* [HBASE-25756](https://issues.apache.org/jira/browse/HBASE-25756) | *Minor* | **Support alternate compression for major and minor compactions**

It is now possible to specify alternate compression algorithms for major or minor compactions, via new ColumnFamilyBuilder or HColumnDescriptor methods {get,set}{Major,Minor}CompressionType(), or shell schema attributes COMPRESSION\_COMPACT\_MAJOR or COMPRESSION\_COMPACT\_MINOR. This can be used to select a fast algorithm for frequent minor compactions and a slower algorithm offering better compression ratios for infrequent major compactions.


---

* [HBASE-25766](https://issues.apache.org/jira/browse/HBASE-25766) | *Major* | **Introduce RegionSplitRestriction that restricts the pattern of the split point**

After HBASE-25766, we can specify a split restriction, "KeyPrefix" or "DelimitedKeyPrefix", to a table with the "hbase.regionserver.region.split\_restriction.type" property. The "KeyPrefix" split restriction groups rows by a prefix of the row-key. And the "DelimitedKeyPrefix" split restriction groups rows by a prefix of the row-key with a delimiter.

For example:
\`\`\`
# Create a table with a "KeyPrefix" split restriction, where the prefix length is 2 bytes
hbase\> create 'tbl1', 'fam', {CONFIGURATION =\> {'hbase.regionserver.region.split\_restriction.type' =\> 'KeyPrefix', 'hbase.regionserver.region.split\_restriction.prefix\_length' =\> '2'}}

# Create a table with a "DelimitedKeyPrefix" split restriction, where the delimiter is a comma (,)
hbase\> create 'tbl2', 'fam', {CONFIGURATION =\> {'hbase.regionserver.region.split\_restriction.type' =\> 'DelimitedKeyPrefix', 'hbase.regionserver.region.split\_restriction.delimiter' =\> ','}}
\`\`\`

Instead of specifying a split restriction to a table directly, we can also set the properties in hbase-site.xml. In this case, the specified split restriction is applied for all the tables.

Note that the split restriction is also applied to a user-specified split point so that we don't allow users to break the restriction, which is different behavior from the existing KeyPrefixRegionSplitPolicy and DelimitedKeyPrefixRegionSplitPolicy.


---

* [HBASE-25775](https://issues.apache.org/jira/browse/HBASE-25775) | *Major* | **Use a special balancer to deal with maintenance mode**

Introduced a MaintenanceLoadBalancer to be used only under maintenance mode. Typically you should not use it as your balancer implementation.


---

* [HBASE-25767](https://issues.apache.org/jira/browse/HBASE-25767) | *Major* | **CandidateGenerator.getRandomIterationOrder is too slow on large cluster**

In the actual implementation classes of CandidateGenerator, now we just random select a start point and then iterate sequentially, instead of using the old way, where we will create a big array to hold all the integers in [0, num\_regions\_in\_cluster), shuffle the array, and then iterate on the array.
The new implementation is 'random' enough as every time we just select one candidate. The problem for the old implementation is that, it will create an array every time when we want to get a candidate, if we have tens of thousands regions, we will create an array with tens of thousands length everytime, which causes big GC pressure and slow down the balancer execution.


---

* [HBASE-25744](https://issues.apache.org/jira/browse/HBASE-25744) | *Major* | **Change default of \`hbase.normalizer.merge.min\_region\_size.mb\` to \`0\`**

Before this change, by default, the normalizer would exclude any region with a total \`storefileSizeMB\` \<= 1 from merge consideration. This changes the default so that these small regions will be merged away.


---

* [HBASE-25687](https://issues.apache.org/jira/browse/HBASE-25687) | *Major* | **Backport "HBASE-25681 Add a switch for server/table queryMeter" to branch-2 and branch-1**

Adds flags to disable server and table metrics. They are default on.

"hbase.regionserver.enable.server.query.meter"
"hbase.regionserver.enable.table.query.meter";


---

* [HBASE-25199](https://issues.apache.org/jira/browse/HBASE-25199) | *Minor* | **Remove HStore#getStoreHomedir**

Moved the following methods from HStore to HRegionFileSystem

- #getStoreHomedir(Path, RegionInfo, byte[])
- #getStoreHomedir(Path, String, byte[])


---

* [HBASE-25685](https://issues.apache.org/jira/browse/HBASE-25685) | *Major* | **asyncprofiler2.0 no longer supports svg; wants html**

If asyncprofiler 1.x, all is good. If asyncprofiler 2.x and it is hbase-2.3.x or hbase-2.4.x, add '?output=html' to get flamegraphs from the profiler.

Otherwise, if hbase-2.5+ and asyncprofiler2, all works. If asyncprofiler1 and hbase-2.5+, you may have to add '?output=svg' to the query.


---

* [HBASE-25518](https://issues.apache.org/jira/browse/HBASE-25518) | *Major* | **Support separate child regions to different region servers**

Config key for enable/disable automatically separate child regions to different region servers in the procedure of split regions. One child will be kept to the server where parent region is on, and the other child will be assigned to a random server.

hbase.master.auto.separate.child.regions.after.split.enabled

Default setting is false/off.


---

* [HBASE-25665](https://issues.apache.org/jira/browse/HBASE-25665) | *Major* | **Disable reverse DNS lookup for SASL Kerberos client connection**

New client side configuration \`hbase.unsafe.client.kerberos.hostname.disable.reversedns\` is added.

This configuration is advanced for experts and you shouldn't specify unless you really what is this configuration and doing.
HBase secure client using SASL Kerberos performs DNS reverse lookup to get hostname for server principal using InetAddress.getCanonicalHostName by default (false for this configuration).
If you set true for this configuration, HBase client doen't perform DNS reverse lookup for server principal and use InetAddress.getHostName which is sent by HBase cluster instead.
This helps your client application deploy under unusual network environment which DNS doesn't provide reverse lookup.
Check the description of the JIRA ticket, HBASE-25665 carefully and check that this configuration fits your environment and deployment actually before enable this configuration.


---

* [HBASE-25374](https://issues.apache.org/jira/browse/HBASE-25374) | *Minor* | **Make REST Client connection and socket time out configurable**

Configuration parameter to set rest client connection timeout

"hbase.rest.client.conn.timeout" Default is 2 \* 1000

"hbase.rest.client.socket.timeout" Default of 30 \* 1000


---

* [HBASE-25566](https://issues.apache.org/jira/browse/HBASE-25566) | *Major* | **RoundRobinTableInputFormat**

Adds RoundRobinTableInputFormat, a subclass of TableInputFormat, that takes the TIF#getSplits list and resorts it so as to spread the InputFormats as broadly about the cluster as possible. RRTIF works to frustrate bunching of InputSplits on RegionServers to avoid the scenario where a few RegionServers are working hard fielding many InputSplits while others idle hosting a few or none.


---

* [HBASE-25587](https://issues.apache.org/jira/browse/HBASE-25587) | *Major* | **[hbck2] Schedule SCP for all unknown servers**

Adds scheduleSCPsForUnknownServers to Hbck Service.


---

* [HBASE-25636](https://issues.apache.org/jira/browse/HBASE-25636) | *Minor* | **Expose HBCK report as metrics**

Expose HBCK repost results in metrics, includes: "orphanRegionsOnRS", "orphanRegionsOnFS", "inconsistentRegions", "holes", "overlaps", "unknownServerRegions" and "emptyRegionInfoRegions".


---

* [HBASE-25582](https://issues.apache.org/jira/browse/HBASE-25582) | *Major* | **Support setting scan ReadType to be STREAM at cluster level**

Adding a new meaning for the config 'hbase.storescanner.pread.max.bytes' when configured with a value \<0.
In HBase 2.x we allow the Scan op to specify a ReadType (PREAD / STREAM/ DEFAULT).  When Scan comes with DEFAULT read type, we will start scan with preads and later switch to stream read once we see we are scanning a total data size \> value of hbase.storescanner.pread.max.bytes.  (This is calculated for data per region:cf).  This config defaults to 4 x of HFile block size = 256 KB by default.
This jira added a new meaning for this config when configured with a -ve value.  In such case, for all scans with DEFAULT read type, we will start with STREAM read itself. (Switch at begin of the scan itself)


---

* [HBASE-25492](https://issues.apache.org/jira/browse/HBASE-25492) | *Major* | **Create table with rsgroup info in branch-2**

HBASE-25492 added a new interface in TableDescriptor which allows user to define RSGroup name while creating or modifying a table.


---

* [HBASE-25460](https://issues.apache.org/jira/browse/HBASE-25460) | *Major* | **Expose drainingServers as cluster metric**

Exposed new jmx metrics: "draininigRegionServers" and "numDrainingRegionServers" to provide "comma separated names for regionservers that are put in draining mode" and "num of such regionservers" respectively.


---

* [HBASE-25615](https://issues.apache.org/jira/browse/HBASE-25615) | *Major* | **Upgrade java version in pre commit docker file**

jdk8u232-b09 -\> jdk8u282-b08
jdk-11.0.6\_10 -\> jdk-11.0.10\_9


---

* [HBASE-23887](https://issues.apache.org/jira/browse/HBASE-23887) | *Major* | **New L1 cache : AdaptiveLRU**

Introduced new L1 cache: AdaptiveLRU. This is supposed to provide better performance than default LRU cache.
Set config key "hfile.block.cache.policy" to "AdaptiveLRU" in hbase-site in order to start using this new cache.


---

* [HBASE-25449](https://issues.apache.org/jira/browse/HBASE-25449) | *Major* | **'dfs.client.read.shortcircuit' should not be set in hbase-default.xml**

The presence of HDFS short-circuit read configuration properties in hbase-default.xml inadvertently causes short-circuit reads to not happen inside of RegionServers, despite short-circuit reads being enabled in hdfs-site.xml.


---

* [HBASE-25333](https://issues.apache.org/jira/browse/HBASE-25333) | *Major* | **Add maven enforcer rule to ban VisibleForTesting imports**

Ban the imports of guava VisiableForTesting, which means you should not use this annotation in HBase any more.
For IA.Public and IA.LimitedPrivate classes, typically you should not expose any test related fields/methods there, and if you want to hide something, use IA.Private on the specific fields/methods.
For IA.Private classes, if you want to expose something only for tests, use the RestrictedApi annotation from error prone, where it could cause a compilation error if someone break the rule in the future.


---

* [HBASE-25441](https://issues.apache.org/jira/browse/HBASE-25441) | *Critical* | **add security check for some APIs in RSRpcServices**

RsRpcServices APIs that can be accessed only through Admin rights:
- stopServer
- updateFavoredNodes
- updateConfiguration
- clearRegionBlockCache
- clearSlowLogsResponses


---

* [HBASE-25432](https://issues.apache.org/jira/browse/HBASE-25432) | *Blocker* | **we should add security checks for setTableStateInMeta and fixMeta**

setTableStateInMeta and fixMeta can be accessed only through Admin rights


---

* [HBASE-25318](https://issues.apache.org/jira/browse/HBASE-25318) | *Minor* | **Configure where IntegrationTestImportTsv generates HFiles**

Added IntegrationTestImportTsv.generatedHFileFolder configuration property to override the default location in IntegrationTestImportTsv. Useful for running the integration test when HDFS Transparent Encryption is enabled.


---

* [HBASE-24751](https://issues.apache.org/jira/browse/HBASE-24751) | *Minor* | **Display Task completion time and/or processing duration on Web UI**

Adds completion time to tasks display.


---

* [HBASE-25456](https://issues.apache.org/jira/browse/HBASE-25456) | *Critical* | **setRegionStateInMeta need security check**

setRegionStateInMeta can be accessed only through Admin rights


---

* [HBASE-25451](https://issues.apache.org/jira/browse/HBASE-25451) | *Major* | **Upgrade commons-io to 2.8.0**

Upgrade commons-io to 2.8.0. Remove deprecated IOUtils.closeQuietly call in code base.


---

* [HBASE-22749](https://issues.apache.org/jira/browse/HBASE-22749) | *Major* | **Distributed MOB compactions**

<!-- markdown -->
MOB compaction is now handled in-line with per-region compaction on region
  servers

- regions with mob data store per-hfile metadata about which mob hfiles are
  referenced
- admin requested major compaction will also rewrite MOB files; periodic RS
  initiated major compaction will not
- periodically a chore in the master will initiate a major compaction that
  will rewrite MOB values to ensure it happens. controlled by
  'hbase.mob.compaction.chore.period'. default is weekly
- control how many RS the chore requests major compaction on in parallel
  with 'hbase.mob.major.compaction.region.batch.size'. default is as
  parallel as possible.
- periodic chore in master will scan backing hfiles from regions to get the
  set of referenced mob hfiles and archive those that are no longer
  referenced. control period with 'hbase.master.mob.cleaner.period'
- Optionally, RS that are compacting mob files can limit write
  amplification by not rewriting values from mob hfiles over a certain size
  limit. opt-in by setting 'hbase.mob.compaction.type' to 'optimized'.
  control threshold by 'hbase.mob.compactions.max.file.size'.
  default is 1GiB
- Should smoothly integrate with existing MOB users via rolling upgrade.
  will delay old MOB file cleanup until per-region compaction has managed
  to compact each region at least once so that used mob hfile metadata can
  be gathered.

This improvement obviates the dataloss in HBASE-22075.



# HBASE  2.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-21970](https://issues.apache.org/jira/browse/HBASE-21970) | *Major* | **Document that how to upgrade from 2.0 or 2.1 to 2.2+**

See the document http://hbase.apache.org/book.html#upgrade2.2 about how to upgrade from 2.0 or 2.1 to 2.2+.

HBase 2.2+ uses a new Procedure form assiging/unassigning/moving Regions. It does not process HBase 2.1 and 2.0's Unassign/Assign Procedure types. Upgrade requires that we first drain the Master Procedure Store of old style Procedures before starting the new 2.2 Master. So you need to make sure that before you kill the old version (2.0 or 2.1) Master, there is no region in transition. And once the new version (2.2+) Master is up, you can rolling upgrade RegionServers one by one.

And there is a more safer way if you are running 2.1.1+ or 2.0.3+ cluster. It need four steps to upgrade Master.

1. Shutdown both active and standby Masters (Your cluster will continue to server reads and writes without interruption).
2. Set the property hbase.procedure.upgrade-to-2-2 to true in hbase-site.xml for the Master, and start only one Master, still using the 2.1.1+ (or 2.0.3+) version.
3. Wait until the Master quits. Confirm that there is a 'READY TO ROLLING UPGRADE' message in the Master log as the cause of the shutdown. The Procedure Store is now empty.
4. Start new Masters with the new 2.2+ version.

Then you can rolling upgrade RegionServers one by one. See HBASE-21075 for more details.


---

* [HBASE-21536](https://issues.apache.org/jira/browse/HBASE-21536) | *Trivial* | **Fix completebulkload usage instructions**

Added completebulkload short name for BulkLoadHFilesTool to bin/hbase.


---

* [HBASE-22500](https://issues.apache.org/jira/browse/HBASE-22500) | *Blocker* | **Modify pom and jenkins jobs for hadoop versions**

Change the default hadoop-3 version to 3.1.2. Drop the support for the releases which are effected by CVE-2018-8029, see this email https://lists.apache.org/thread.html/3d6831c3893cd27b6850aea2feff7d536888286d588e703c6ffd2e82@%3Cuser.hadoop.apache.org%3E


---

* [HBASE-22148](https://issues.apache.org/jira/browse/HBASE-22148) | *Blocker* | **Provide an alternative to CellUtil.setTimestamp**

<!-- markdown -->

The `CellUtil.setTimestamp` method changes to be an API with audience `LimitedPrivate(COPROC)` in HBase 3.0. With that designation the API should remain stable within a given minor release line, but may change between minor releases.

Previously, this method was deprecated in HBase 2.0 for removal in HBase 3.0. Deprecation messages in HBase 2.y releases have been updated to indicate the expected API audience change.


---

* [HBASE-21991](https://issues.apache.org/jira/browse/HBASE-21991) | *Major* | **Fix MetaMetrics issues - [Race condition, Faulty remove logic], few improvements**

The class LossyCounting was unintentionally marked Public but was never intended to be part of our public API. This oversight has been corrected and LossyCounting is now marked as Private and going forward may be subject to additional breaking changes or removal without notice. If you have taken a dependency on this class we recommend cloning it locally into your project before upgrading to this release.


---

* [HBASE-22226](https://issues.apache.org/jira/browse/HBASE-22226) | *Trivial* | **Incorrect level for headings in asciidoc**

Warnings for level headings are corrected in the book for the HBase Incompatibilities section.


---

* [HBASE-20970](https://issues.apache.org/jira/browse/HBASE-20970) | *Major* | **Update hadoop check versions for hadoop3 in hbase-personality**

Add hadoop 3.0.3, 3.1.1 3.1.2 in our hadoop check jobs.


---

* [HBASE-21784](https://issues.apache.org/jira/browse/HBASE-21784) | *Major* | **Dump replication queue should show list of wal files ordered chronologically**

The DumpReplicationQueues tool will now list replication queues sorted in chronological order.


---

* [HBASE-22384](https://issues.apache.org/jira/browse/HBASE-22384) | *Minor* | **Formatting issues in administration section of book**

Fixes a formatting issue in the administration section of the book, where listing indentation were a little bit off.


---

* [HBASE-22399](https://issues.apache.org/jira/browse/HBASE-22399) | *Major* | **Change default hadoop-two.version to 2.8.x and remove the 2.7.x hadoop checks**

Now the default hadoop-two.version has been changed to 2.8.5, and all hadoop versions before 2.8.2(exclude) will not be supported any more.


---

* [HBASE-22392](https://issues.apache.org/jira/browse/HBASE-22392) | *Trivial* | **Remove extra/useless +**

Removed extra + in HRegion, HStore and LoadIncrementalHFiles for branch-2 and HRegion and HStore for branch-1.


---

* [HBASE-20494](https://issues.apache.org/jira/browse/HBASE-20494) | *Major* | **Upgrade com.yammer.metrics dependency**

Updated metrics core from 3.2.1 to 3.2.6.


---

* [HBASE-22358](https://issues.apache.org/jira/browse/HBASE-22358) | *Minor* | **Change rubocop configuration for method length**

The rubocop definition for the maximum method length was set to 75.


---

* [HBASE-22379](https://issues.apache.org/jira/browse/HBASE-22379) | *Minor* | **Fix Markdown for "Voting on Release Candidates" in book**

Fixes the formatting of the "Voting on Release Candidates" to actually show the quote and code formatting of the RAT check.


---

* [HBASE-20851](https://issues.apache.org/jira/browse/HBASE-20851) | *Minor* | **Change rubocop config for max line length of 100**

The rubocop configuration in the hbase-shell module now allows a line length with 100 characters, instead of 80 as before. For everything before 2.1.5 this change introduces rubocop itself.


---

* [HBASE-22054](https://issues.apache.org/jira/browse/HBASE-22054) | *Minor* | **Space Quota: Compaction is not working for super user in case of NO\_WRITES\_COMPACTIONS**

This change allows the system and superusers to initiate compactions, even when a space quota violation policy disallows compactions from happening. The original intent behind disallowing of compactions was to prevent end-user compactions from creating undue I/O load, not disallowing \*any\* compaction in the system.


---

* [HBASE-22292](https://issues.apache.org/jira/browse/HBASE-22292) | *Blocker* | **PreemptiveFastFailInterceptor clean repeatedFailuresMap issue**

Adds new configuration hbase.client.failure.map.cleanup.interval which defaults to ten minutes.


---

* [HBASE-22155](https://issues.apache.org/jira/browse/HBASE-22155) | *Major* | **Move 2.2.0 on to hbase-thirdparty-2.2.0**

 Updates libs used internally by hbase via hbase-thirdparty as follows:

 gson 2.8.1 -\\\> 2.8.5
 guava 22.0 -\\\> 27.1-jre
 pb 3.5.1 -\\\> 3.7.0
 netty 4.1.17 -\\\> 4.1.34
 commons-collections4 4.1 -\\\> 4.3


---

* [HBASE-22178](https://issues.apache.org/jira/browse/HBASE-22178) | *Major* | **Introduce a createTableAsync with TableDescriptor method in Admin**

Introduced

Future\<Void\> createTableAsync(TableDescriptor);


---

* [HBASE-22108](https://issues.apache.org/jira/browse/HBASE-22108) | *Major* | **Avoid passing null in Admin methods**

Introduced these methods:
void move(byte[]);
void move(byte[], ServerName);
Future\<Void\> splitRegionAsync(byte[]);

These methods are deprecated:
void move(byte[], byte[])


---

* [HBASE-22152](https://issues.apache.org/jira/browse/HBASE-22152) | *Major* | **Create a jenkins file for yetus to processing GitHub PR**

Add a new jenkins file for running pre commit check for GitHub PR.


---

* [HBASE-22007](https://issues.apache.org/jira/browse/HBASE-22007) | *Major* | **Add restoreSnapshot and cloneSnapshot with acl methods in AsyncAdmin**

Add cloneSnapshot/restoreSnapshot with acl methods in AsyncAdmin.


---

* [HBASE-22123](https://issues.apache.org/jira/browse/HBASE-22123) | *Minor* | **REST gateway reports Insufficient permissions exceptions as 404 Not Found**

When insufficient permissions, you now get:

HTTP/1.1 403 Forbidden

on the HTTP side, and in the message

Forbidden
org.apache.hadoop.hbase.security.AccessDeniedException: org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions for user ‘myuser',action: get, tableName:mytable, family:cf.
at org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor.authorizeAccess(RangerAuthorizationCoprocessor.java:547)
and the rest of the ADE stack


---

* [HBASE-22100](https://issues.apache.org/jira/browse/HBASE-22100) | *Minor* | **False positive for error prone warnings in pre commit job**

Now we will sort the javac WARNING/ERROR before generating diff in pre-commit so we can get a stable output for the error prone. The downside is that we just sort the output lexicographically so the line number will also be sorted lexicographically, which is a bit strange to human.


---

* [HBASE-22057](https://issues.apache.org/jira/browse/HBASE-22057) | *Major* | **Impose upper-bound on size of ZK ops sent in a single multi()**

Exposes a new configuration property "zookeeper.multi.max.size" which dictates the maximum size of deletes that HBase will make to ZooKeeper in a single RPC. This property defaults to 1MB, which should fall beneath the default ZooKeeper limit of 2MB, controlled by "jute.maxbuffer".


---

* [HBASE-22052](https://issues.apache.org/jira/browse/HBASE-22052) | *Major* | **pom cleaning; filter out jersey-core in hadoop2 to match hadoop3 and remove redunant version specifications**

<!-- markdown -->
Fixed awkward dependency issue that prevented site building.

#### note specific to HBase 2.1.4
HBase 2.1.4 shipped with an early version of this fix that incorrectly altered the libraries included in our binary assembly for using Apache Hadoop 2.7 (the current build default Hadoop version for 2.1.z). For folks running out of the box against a Hadoop 2.7 cluster (or folks who skip the installation step of [replacing the bundled Hadoop libraries](http://hbase.apache.org/book.html#hadoop)) this will result in a failure at Region Server startup due to a missing class definition. e.g.:
```
2019-03-27 09:02:05,779 ERROR [main] regionserver.HRegionServer: Failed construction RegionServer
java.lang.NoClassDefFoundError: org/apache/htrace/SamplerBuilder
	at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:644)
	at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:628)
	at org.apache.hadoop.hdfs.DistributedFileSystem.initialize(DistributedFileSystem.java:149)
	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2667)
	at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:93)
	at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2701)
	at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2683)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:372)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:171)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:356)
	at org.apache.hadoop.fs.Path.getFileSystem(Path.java:295)
	at org.apache.hadoop.hbase.util.CommonFSUtils.getRootDir(CommonFSUtils.java:362)
	at org.apache.hadoop.hbase.util.CommonFSUtils.isValidWALRootDir(CommonFSUtils.java:411)
	at org.apache.hadoop.hbase.util.CommonFSUtils.getWALRootDir(CommonFSUtils.java:387)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.initializeFileSystem(HRegionServer.java:704)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.<init>(HRegionServer.java:613)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.constructRegionServer(HRegionServer.java:3029)
	at org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine.start(HRegionServerCommandLine.java:63)
	at org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine.run(HRegionServerCommandLine.java:87)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
	at org.apache.hadoop.hbase.util.ServerCommandLine.doMain(ServerCommandLine.java:149)
	at org.apache.hadoop.hbase.regionserver.HRegionServer.main(HRegionServer.java:3047)
Caused by: java.lang.ClassNotFoundException: org.apache.htrace.SamplerBuilder
	at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:349)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
	... 26 more

```

Workaround via any _one_ of the following:
* If you are running against a Hadoop cluster that is 2.8+, ensure you replace the Hadoop libaries in the default binary assembly with those for your version.
* If you are running against a Hadoop cluster that is 2.8+, build the binary assembly from the source release while specifying your Hadoop version.
* If you are running against a Hadoop cluster that is a supported 2.7 release, ensure the `hadoop` executable is in the `PATH` seen at Region Server startup and that you are not using the `HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP` bypass.
* For any supported Hadoop version, manually make the Apache HTrace artifact `htrace-core-3.1.0-incubating.jar` available to all Region Servers via the HBASE_CLASSPATH environment variable.
* For any supported Hadoop version, manually make the Apache HTrace artifact `htrace-core-3.1.0-incubating.jar` available to all Region Servers by copying it into the directory `${HBASE_HOME}/lib/client-facing-thirdparty/`.


---

* [HBASE-22065](https://issues.apache.org/jira/browse/HBASE-22065) | *Major* | **Add listTableDescriptors(List\<TableName\>) method in AsyncAdmin**

Add a listTableDescriptors(List\<TableName\>) method in the AsyncAdmin interface, to align with the Admin interface.


---

* [HBASE-22040](https://issues.apache.org/jira/browse/HBASE-22040) | *Major* | **Add mergeRegionsAsync with a List of region names method in AsyncAdmin**

Add a mergeRegionsAsync(byte[][], boolean) method in the AsyncAdmin interface.

Instead of using assert, now we will throw IllegalArgumentException when you want to merge less than 2 regions at client side. And also, at master side, instead of using assert, now we will throw DoNotRetryIOException if you want merge more than 2 regions, since we only support merging two regions at once for now.


---

* [HBASE-22039](https://issues.apache.org/jira/browse/HBASE-22039) | *Major* | **Should add the synchronous parameter for the XXXSwitch method in AsyncAdmin**

Add drainXXX parameter for balancerSwitch/splitSwitch/mergeSwitch methods in the AsyncAdmin interface, which has the same meaning with the synchronous parameter for these methods in the Admin interface.


---

* [HBASE-21810](https://issues.apache.org/jira/browse/HBASE-21810) | *Major* | **bulkload  support set hfile compression on client**

bulkload (HFileOutputFormat2)  support config the compression on client ,you can set the job configuration "hbase.mapreduce.hfileoutputformat.compression"  override the auto-detection of the target table's compression


---

* [HBASE-22000](https://issues.apache.org/jira/browse/HBASE-22000) | *Major* | **Deprecated isTableAvailable with splitKeys**

Deprecated AsyncTable.isTableAvailable(TableName, byte[][]).


---

* [HBASE-21871](https://issues.apache.org/jira/browse/HBASE-21871) | *Major* | **Support to specify a peer table name in VerifyReplication tool**

After HBASE-21871, we can specify a peer table name with --peerTableName in VerifyReplication tool like the following:
hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication --peerTableName=peerTable 5 TestTable

In addition, we can compare any 2 tables in any remote clusters with specifying both peerId and --peerTableName.

For example:
hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication --peerTableName=peerTable zk1,zk2,zk3:2181/hbase TestTable


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

* [HBASE-21492](https://issues.apache.org/jira/browse/HBASE-21492) | *Critical* | **CellCodec Written To WAL Before It's Verified**

After HBASE-21492 the return type of WALCellCodec#getWALCellCodecClass has been changed from String to Class


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
# HBASE  2.0.0 Release Notes


These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-20464](https://issues.apache.org/jira/browse/HBASE-20464) | *Major* | **Disable IMC**

Change the default so that on creation of new tables, In-Memory Compaction BASIC is NOT enabled.

This change is in branch-2.0 only, not in branch-2.


---

* [HBASE-20276](https://issues.apache.org/jira/browse/HBASE-20276) | *Blocker* | **[shell] Revert shell REPL change and document**

<!-- markdown -->



The HBase shell now behaves as it did prior to the changes that started in HBASE-15965. Namely, some shell commands return values that may be further manipulated within the shell's IRB session.

The command line option `--return-values` is no longer acted on by the shell since it now always behaves as it did when passed this parameter. Passing the option results in a harmless warning about this change.

Users who wish to maintain the behavior seen in the 1.4.0-1.4.2 releases of the HBase shell should refer to the section _irbrc_ in the reference guide for how to configure their IRB session to avoid echoing expression results to the console.


---

* [HBASE-18792](https://issues.apache.org/jira/browse/HBASE-18792) | *Blocker* | **hbase-2 needs to defend against hbck operations**

As of HBase version 2.0, the hbck tool is significantly changed. In general, all Read-Only options are supported and can be be used safely. Most -fix/ -repair options are NOT supported. Please see usage below for details on which options are not supported:


Usage: fsck [opts] {only tables}
 where [opts] are:
   -help Display help options (this)
   -details Display full report of all regions.
   -timelag \<timeInSeconds\>  Process only regions that  have not experienced any metadata updates in the last  \<timeInSeconds\> seconds.
   -sleepBeforeRerun \<timeInSeconds\> Sleep this many seconds before checking if the fix worked if run with -fix
   -summary Print only summary of the tables and status.
   -metaonly Only check the state of the hbase:meta table.
   -sidelineDir \<hdfs://\> HDFS path to backup existing meta.
   -boundaries Verify that regions boundaries are the same between META and store files.
   -exclusive Abort if another hbck is exclusive or fixing.

  Datafile Repair options: (expert features, use with caution!)
   -checkCorruptHFiles     Check all Hfiles by opening them to make sure they are valid
   -sidelineCorruptHFiles  Quarantine corrupted HFiles.  implies -checkCorruptHFiles

 Replication options
   -fixReplication   Deletes replication queues for removed peers

  Metadata Repair options supported as of version 2.0: (expert features, use with caution!)
   -fixVersionFile   Try to fix missing hbase.version file in hdfs.
   -fixReferenceFiles  Try to offline lingering reference store files
   -fixHFileLinks  Try to offline lingering HFileLinks
   -noHdfsChecking   Don't load/check region info from HDFS. Assumes hbase:meta region info is good. Won't check/fix any HDFS issue, e.g. hole, orphan, or overlap
   -ignorePreCheckPermission  ignore filesystem permission pre-check

NOTE: Following options are NOT supported as of HBase version 2.0+.

  UNSUPPORTED Metadata Repair options: (expert features, use with caution!)
   -fix              Try to fix region assignments.  This is for backwards compatiblity
   -fixAssignments   Try to fix region assignments.  Replaces the old -fix
   -fixMeta          Try to fix meta problems.  This assumes HDFS region info is good.
   -fixHdfsHoles     Try to fix region holes in hdfs.
   -fixHdfsOrphans   Try to fix region dirs with no .regioninfo file in hdfs
   -fixTableOrphans  Try to fix table dirs with no .tableinfo file in hdfs (online mode only)
   -fixHdfsOverlaps  Try to fix region overlaps in hdfs.
   -maxMerge \<n\>     When fixing region overlaps, allow at most \<n\> regions to merge. (n=5 by default)
   -sidelineBigOverlaps  When fixing region overlaps, allow to sideline big overlaps
   -maxOverlapsToSideline \<n\>  When fixing region overlaps, allow at most \<n\> regions to sideline per group. (n=2 by default)
   -fixSplitParents  Try to force offline split parents to be online.
   -removeParents    Try to offline and sideline lingering parents and keep daughter regions.
   -fixEmptyMetaCells  Try to fix hbase:meta entries not referencing any region (empty REGIONINFO\_QUALIFIER rows)

  UNSUPPORTED Metadata Repair shortcuts
   -repair           Shortcut for -fixAssignments -fixMeta -fixHdfsHoles -fixHdfsOrphans -fixHdfsOverlaps -fixVersionFile -sidelineBigOverlaps -fixReferenceFiles-fixHFileLinks
   -repairHoles      Shortcut for -fixAssignments -fixMeta -fixHdfsHoles


---

* [HBASE-19994](https://issues.apache.org/jira/browse/HBASE-19994) | *Major* | **Create a new class for RPC throttling exception, make it retryable.**

A new RpcThrottlingException deprecates ThrottlingException. The new RpcThrottlingException is a retryable Exception that clients will retry when Rpc throttling quota is exceeded. The deprecated ThrottlingException is a nonretryable Exception.


---

* [HBASE-20224](https://issues.apache.org/jira/browse/HBASE-20224) | *Blocker* | **Web UI is broken in standalone mode**

Standalone webui was broken inadvertently by HBASE-20027.


---

* [HBASE-18784](https://issues.apache.org/jira/browse/HBASE-18784) | *Major* | **Use of filesystem that requires hflush / hsync / append / etc should query outputstream capabilities**

<!-- markdown -->



If HBase is run on top of Apache Hadoop libraries that support the needed APIs it will verify that underlying Filesystem implementations provide the needed durability mechanisms to safely operate. The needed APIs *should* be present in Hadoop 3 release and Hadoop 2 releases starting in the Hadoop 2.9 series. If the APIs are not available, HBase behaves as it has in previous releases (that is, it moves forward assuming such a check would pass).

Where this check fails, it is unsafe to rely on HBase in a production setting. In the event of process or node failure, the HBase RegionServer process may fail to have access to all the data it previously wrote to its write ahead log, resulting in data loss. In the event of process or node failure, the HBase master process may lose all or part of the write ahead log that it relies on for cluster management operations, leaving the cluster in an inconsistent state that we aren't sure it could recover from.

Notably, the LocalFileSystem implementation provided by Hadoop reports (accurately) via these new APIs that it can not provide the durability HBase needs to operate. As such, the current instructions for single-node HBase operation have been updated both with a) how to bypass this safety check and b) a strong warning about the dire consequences of doing so outside of a dev/test environment.


---

* [HBASE-20219](https://issues.apache.org/jira/browse/HBASE-20219) | *Critical* | **An error occurs when scanning with reversed=true and loadColumnFamiliesOnDemand=true**

Throws DoNotRetryIOException when you ask for a reverse scan loading adjacent column families on demand. Previous it threw IllegalStateException


---

* [HBASE-20358](https://issues.apache.org/jira/browse/HBASE-20358) | *Minor* | **Fix bin/hbase thrift usage text**

Cleanup usage message and command-line processing (no functional change).


---

* [HBASE-20182](https://issues.apache.org/jira/browse/HBASE-20182) | *Blocker* | **Can not locate region after split and merge**

Now if we hit a split parent when locating a region, we will skip to the next row and try again until the region does not contain our row. So there will be no RegionOfflineException for a split parent any more, instead, if the split children have not been onlined yet, i.e, we finally arrive at a region which does not contain our row, an IOException will be thrown.


---

* [HBASE-20149](https://issues.apache.org/jira/browse/HBASE-20149) | *Critical* | **Purge dev javadoc from bin tarball (or make a separate tarball of javadoc)**

We no longer include dev or dev test javadocs in our binary bundle. We still build them; they are just not included because they were half the size of the resultant tarball.

Here is our story on javadoc as of this commit:

 \* apidocs - user facing main api javadocs. currently for a release line, published on website and linked from menu. included in the bin tarball
 \* devapidocs - hbase internal javadocs. currently for a release line, published on the website but not linked from the menu. no longer included in the bin tarball.
 \* testapidocs - user facing test scope api javadocs. currently for a release line, not published. included in the bin tarball.
 \* testdevapidocs - hbase internal test scope javadocs. currently for a release line, not published. no longer included in the bin tarball


---

* [HBASE-18828](https://issues.apache.org/jira/browse/HBASE-18828) | *Blocker* | **[2.0] Generate CHANGES.txt**

Moves us over to yetus releasedocmaker tooling generating CHANGES. CHANGES is not markdown (CHANGES.md) as opposed to CHANGES.txt. We've also added a new RELEASENOTES.md that lists JIRA release notes (courtesy of releasedocmaker).

CHANGES/RELEASENOTES are current as of now. Will need a 'freshening' when we cut the RC.


---

* [HBASE-14175](https://issues.apache.org/jira/browse/HBASE-14175) | *Critical* | **Adopt releasedocmaker for better generated release notes**

We will use yetus releasedocmaker to make our changes doc from here on out. A CHANGELOG.md will replace our current CHANGES.txt. Adjacent, we'll keep up a RELEASENOTES.md doc courtesy of releasedocmaker.

Over in HBASE-18828 is where we are working through steps for the RM integrating this new tooling.


---

* [HBASE-16499](https://issues.apache.org/jira/browse/HBASE-16499) | *Critical* | **slow replication for small HBase clusters**

Changed the default value for replication.source.ratio from 0.1 to 0.5. Which means now by default 50% of the total RegionServers in peer cluster(s) will participate in replication.


---

* [HBASE-16459](https://issues.apache.org/jira/browse/HBASE-16459) | *Trivial* | **Remove unused hbase shell --format option**

<!-- markdown -->




The HBase `shell` command no longer recognizes the option `--format`. Previously this option only recognized the default value of 'console'. The default value is now always used.


---

* [HBASE-20259](https://issues.apache.org/jira/browse/HBASE-20259) | *Critical* | **Doc configs for in-memory-compaction and add detail to in-memory-compaction logging**

Disables in-memory compaction as default.

Adds logging of in-memory compaction configuration on creation.

Adds a chapter to the refguide on this new feature.


---

* [HBASE-20282](https://issues.apache.org/jira/browse/HBASE-20282) | *Major* | **Provide short name invocations for useful tools**

\`hbase regionsplitter\` is a new short invocation for \`hbase org.apache.hadoop.hbase.util.RegionSplitter\`


---

* [HBASE-20314](https://issues.apache.org/jira/browse/HBASE-20314) | *Major* | **Precommit build for master branch fails because of surefire fork fails**

Upgrade surefire plugin to 2.21.0.


---

* [HBASE-20130](https://issues.apache.org/jira/browse/HBASE-20130) | *Critical* | **Use defaults (16020 & 16030) as base ports when the RS is bound to localhost**

<!-- markdown -->



When region servers bind to localhost (mostly in pseudo distributed mode), default ports (16020 & 16030) are used as base ports. This will support up to 9 instances of region servers by default with `local-regionservers.sh` script. If additional instances are needed, see the reference guide on how to deploy with a different range using the environment variables `HBASE_RS_BASE_PORT` and `HBASE_RS_INFO_BASE_PORT`.


---

* [HBASE-20111](https://issues.apache.org/jira/browse/HBASE-20111) | *Critical* | **Able to split region explicitly even on shouldSplit return false from split policy**

When a split is requested on a Region, the RegionServer hosting that Region will now consult the configured SplitPolicy for that table when determining if a split of that Region is allowed. When a split is disallowed (due to the Region not being OPEN or the SplitPolicy denying the request), the operation will \*not\* be implicitly retried as it has previously done. Users will need to guard against and explicitly retry region split requests which are denied by the system.


---

* [HBASE-20223](https://issues.apache.org/jira/browse/HBASE-20223) | *Blocker* | **Use hbase-thirdparty 2.1.0**

Moves commons-cli and commons-collections4 into the HBase thirdparty shaded jar which means that these are no longer generally available for users on the classpath.


---

* [HBASE-19128](https://issues.apache.org/jira/browse/HBASE-19128) | *Major* | **Purge Distributed Log Replay from codebase, configurations, text; mark the feature as unsupported, broken.**

Removes Distributed Log Replay feature. Disable the feature before upgrading.


---

* [HBASE-19504](https://issues.apache.org/jira/browse/HBASE-19504) | *Major* | **Add TimeRange support into checkAndMutate**

1) checkAndMutate accept a TimeRange to query the specified cell
2) remove writeToWAL flag from Region#checkAndMutate since it is useless (this is a incompatible change)


---

* [HBASE-20237](https://issues.apache.org/jira/browse/HBASE-20237) | *Critical* | **Put back getClosestRowBefore and throw UnknownProtocolException instead... for asynchbase client**

Throw UnknownProtocolException if a client connects and tries to invoke the old getClosestRowOrBefore method. Pre-hbase-1.0.0 or asynchbase do this instead of using its replacement, the reverse Scan.

getClosestRowOrBefore was implemented as a flag on Get. Before this patch though the flag was set, hbase2 were ignoring it. This made it look like a pre-1.0.0 client was 'working' but then it'd fail finding the appropriate Region for a client-specified row doing lookups into hbase:meta.


---

* [HBASE-20247](https://issues.apache.org/jira/browse/HBASE-20247) | *Major* | **Set version as 2.0.0 in branch-2.0 in prep for first RC**

Set version as 2.0.0 on branch-2.0.


---

* [HBASE-20090](https://issues.apache.org/jira/browse/HBASE-20090) | *Major* | **Properly handle Preconditions check failure in MemStoreFlusher$FlushHandler.run**

When there is concurrent region split, MemStoreFlusher may not find flushable region if the only candidate region left hasn't received writes (resulting in 0 data size).
After this JIRA, such scenario wouldn't trigger Precondition assertion (replaced by an if statement to see whether there is any flushable region).
If there is no flushable region, a DEBUG log would appear in region server log, saying "Above memory mark but there is no flushable region".


---

* [HBASE-19552](https://issues.apache.org/jira/browse/HBASE-19552) | *Major* | **update hbase to use new thirdparty libs**

hbase-thirdparty libs have moved to o.a.h.thirdparty offset. Netty shading system property is no longer necessary.


---

* [HBASE-20119](https://issues.apache.org/jira/browse/HBASE-20119) | *Minor* | **Introduce a pojo class to carry coprocessor information in order to make TableDescriptorBuilder accept multiple cp at once**

1) Make all methods in TableDescriptorBuilder be setter pattern.
addCoprocessor -\> setCoprocessor
addColumnFamily -\> setColumnFamily
(addCoprocessor and addColumnFamily are still in branch-2 but they are marked as deprecated)
2) add CoprocessorDescriptor to carry cp information
3) add CoprocessorDescriptorBuilder to build CoprocessorDescriptor
4) TD disallow user to set negative priority to coprocessor since parsing the negative value will cause a exception


---

* [HBASE-17165](https://issues.apache.org/jira/browse/HBASE-17165) | *Critical* | **Add retry to LoadIncrementalHFiles tool**

Adds retry to load of incremental hfiles. Pertinent key is HConstants.HBASE\_CLIENT\_RETRIES\_NUMBER. Default is HConstants.DEFAULT\_HBASE\_CLIENT\_RETRIES\_NUMBER.


---

* [HBASE-20108](https://issues.apache.org/jira/browse/HBASE-20108) | *Critical* | **\`hbase zkcli\` falls into a non-interactive prompt after HBASE-15199**

This issue fixes a runtime dependency issues where JLine is not made available on the classpath which causes the ZooKeeper CLI to appear non-interactive. JLine was being made available unintentionally via the JRuby jar file on the classpath for the HBase shell. While the JRuby jar is not always present, the fix made here was to selectively include the JLine dependency on the zkcli command's classpath.


---

* [HBASE-8770](https://issues.apache.org/jira/browse/HBASE-8770) | *Blocker* | **deletes and puts with the same ts should be resolved according to mvcc/seqNum**

This behavior is available as a new feature. See HBASE-15968 release note.

This issue is just about adding to the refguide documentation on the HBASE\_15968 feature.


---

* [HBASE-19114](https://issues.apache.org/jira/browse/HBASE-19114) | *Major* | **Split out o.a.h.h.zookeeper from hbase-server and hbase-client**

Splits out most of ZooKeeper related code into a separate new module: hbase-zookeeper.
Also, renames some ZooKeeper related classes to follow a common naming pattern - "ZK" prefix - as compared to many different styles earlier.


---

* [HBASE-19437](https://issues.apache.org/jira/browse/HBASE-19437) | *Critical* | **Batch operation can't handle the null result for Append/Increment**

The result from server is changed from null to Result.EMPTY\_RESULT when Append/Increment operation can't retrieve any data from server,


---

* [HBASE-17448](https://issues.apache.org/jira/browse/HBASE-17448) | *Major* | **Export metrics from RecoverableZooKeeper**

Committed to master and branch-1


---

* [HBASE-19400](https://issues.apache.org/jira/browse/HBASE-19400) | *Major* | **Add missing security checks in MasterRpcServices**

Added ACL check to following Admin functions:
enableCatalogJanitor, runCatalogJanitor, cleanerChoreSwitch, runCleanerChore, execProcedure, execProcedureWithReturn, normalize, normalizerSwitch, coprocessorService.
When ACL is enabled, only those with ADMIN rights will be able to invoke these operations successfully.


---

* [HBASE-20048](https://issues.apache.org/jira/browse/HBASE-20048) | *Blocker* | **Revert serial replication feature**

Revert the serial replication feature from all branches. Plan to reimplement it soon and land onto 2.1 release line.


---

* [HBASE-19166](https://issues.apache.org/jira/browse/HBASE-19166) | *Blocker* | **AsyncProtobufLogWriter persists ProtobufLogWriter as class name for backward compatibility**

For backward compatibility, AsyncProtobufLogWriter uses "ProtobufLogWriter" as writer class name and SecureAsyncProtobufLogWriter uses "SecureProtobufLogWriter" as writer class name.


---

* [HBASE-18596](https://issues.apache.org/jira/browse/HBASE-18596) | *Blocker* | **[TEST] A hbase1 cluster should be able to replicate to a hbase2 cluster; verify**

Replication between versions verified as basically working. 0.98.25-SNAPSHOT to beta-2 hbase2 and a 1.2-ish version tried.


---

* [HBASE-20017](https://issues.apache.org/jira/browse/HBASE-20017) | *Blocker* | **BufferedMutatorImpl submit the same mutation repeatedly**

This change fixes multithreading issues in the implementation of BufferedMutator. BufferedMutator should not be used with 1.4 releases prior to 1.4.2.


---

* [HBASE-20032](https://issues.apache.org/jira/browse/HBASE-20032) | *Minor* | **Receving multiple warnings for missing reporting.plugins.plugin.version**

Add (latest) version elements missing from reporting plugins in top-level pom.


---

* [HBASE-19954](https://issues.apache.org/jira/browse/HBASE-19954) | *Major* | **Separate TestBlockReorder into individual tests to avoid ShutdownHook suppression error against hadoop3**

hadoop3 minidfscluster removes all shutdown handlers when the cluster goes down which made this test that does FS-stuff fail (Fix was to break up the test so each test method ran with an unadulterated FS).


---

* [HBASE-20014](https://issues.apache.org/jira/browse/HBASE-20014) | *Major* | **TestAdmin1 Times out**

Ups the overall test timeout from 10 minutes to 13minutes. 15minutes is the surefire timeout.


---

* [HBASE-20020](https://issues.apache.org/jira/browse/HBASE-20020) | *Critical* | **Make sure we throw DoNotRetryIOException when ConnectionImplementation is closed**

Add checkClosed to core Client methods. Avoid unnecessary retry.


---

* [HBASE-19978](https://issues.apache.org/jira/browse/HBASE-19978) | *Major* | **The keepalive logic is incomplete in ProcedureExecutor**

Completes keep-alive logic and then enables it; ProcedureExecutor Workers will spin up more threads when need settling back to the core count after the burst in demand has passed. Default keep-alive is one minute. Default core-count is CPUs/4 or 16, which ever is greater. Maximum is an arbitrary core-count \* 10 (a limit that should never be hit and if it is, there is something else very wrong).


---

* [HBASE-19950](https://issues.apache.org/jira/browse/HBASE-19950) | *Minor* | **Introduce a ColumnValueFilter**

ColumnValueFilter provides a way to fetch matched cells only by providing specified column, value and a comparator, which is different from SingleValueFilter, fetching an entire row as soon as a matched cell found.


---

* [HBASE-18294](https://issues.apache.org/jira/browse/HBASE-18294) | *Major* | **Reduce global heap pressure: flush based on heap occupancy**

A region is flushed if its memory component exceeds the region flush threshold.
A flush policy decides which stores to flush by comparing the size of the store to a column-family-flush threshold.
If the overall size of all memstores in the machine exceeds the bounds defined by the administrator (denoted global pressure) a region is selected and flushed.
HBASE-18294 changes flush decisions to be based on heap-occupancy and not data (key-value) size, consistently across levels. This rolls back some of the changes by HBASE-16747. Specifically,
(1) RSs, Regions and stores track their overall on-heap and off-heap occupancy,
(2) A region is flushed when its on-heap+off-heap size exceeds the region flush threshold specified in hbase.hregion.memstore.flush.size,
(3) The store to be flushed is chosen based on its on-heap+off-heap size
(4) At the RS level, a flush is triggered when the overall on-heap exceeds the on-heap limit, or when the overall off-heap size exceeds the off-heap limit (low/high water marks).

Note that when the region flush size is set to XXmb a region flush may be triggered even before writing keys and values of size XX because the total heap occupancy of the region which includes additional metadata exceeded the threshold.


---

* [HBASE-19116](https://issues.apache.org/jira/browse/HBASE-19116) | *Critical* | **Currently the tail of hfiles with CellComparator\* classname makes it so hbase1 can't open hbase2 written hfiles; fix**

hbase-2.x sets KeyValue Comparators into the tail of hfiles rather than CellComparator, what it uses internally, just so hbase-1.x can continue to read hbase-2.x written hfiles.


---

* [HBASE-19948](https://issues.apache.org/jira/browse/HBASE-19948) | *Major* | **Since HBASE-19873, HBaseClassTestRule, Small/Medium/Large has different semantic**

In subtask, fixed doc and annotations to be more explicit that test timings are for the whole Test Fixture/Test Class/Test Suite NOT the test method only as we'd measuring up to this (tother subtasks untethered Categorization and test timeout such that all categories now have a ten minute timeout -- no test can run longer than ten minutes or it gets killed/timedout).


---

* [HBASE-16060](https://issues.apache.org/jira/browse/HBASE-16060) | *Blocker* | **1.x clients cannot access table state talking to 2.0 cluster**

By default, we mirror table state to zookeeper so hbase-1.x clients will work against an hbase-2 cluster (With this patch, hbase-1.x clients can do most Admin functions including table create; hbase-1.x clients can do all Table/DML against hbase-2 cluster).

Flag to disable mirroring is hbase.mirror.table.state.to.zookeeper; set it to false in Configuration.

Related, Master on startup will look to see if there are table state znodes left over by an hbase-1 instance. If any found, it will migrate the table state to hbase-2 setting the state into the hbase:meta table where table state is now kept. We will do this check on every Master start. Notion is that this will be overall beneficial with low impediment. To disable the migration check, set hbase.migrate.table.state.from.zookeeper to false.


---

* [HBASE-19900](https://issues.apache.org/jira/browse/HBASE-19900) | *Critical* | **Region-level exception destroy the result of batch**

This fix makes the following changes to how client handle the both of action result and region exception.
1) honor the action result rather than region exception. If the action have both of true result and region exception, the action is fine as the exception is caused by other actions which are in the same region.
2) honor the action exception rather than region exception. If the action have both of action exception and region exception, we deal with the action exception only. If we also handle the region exception for the same action, it will introduce the negative count of actions in progress. The AsyncRequestFuture#waitUntilDone will block forever.


---

* [HBASE-19841](https://issues.apache.org/jira/browse/HBASE-19841) | *Major* | **Tests against hadoop3 fail with StreamLacksCapabilityException**

HBaseTestingUtility now assumes that all clusters will use local storage until a MiniDFSCluster is started or assigned.


---

* [HBASE-19528](https://issues.apache.org/jira/browse/HBASE-19528) | *Major* | **Major Compaction Tool**

Tool allows you to compact a cluster with given concurrency of regionservers compacting at a given time.  If tool completes successfully everything requested for compaction will be compacted, regardless of region moves, splits and merges.


---

* [HBASE-19919](https://issues.apache.org/jira/browse/HBASE-19919) | *Major* | **Tidying up logging**

(I thought this change innocuous but I made work for a co-worker when I upped interval between log cleaner runs -- meant a smoke test failed because we were slow doing an expected cleanup).

Edit of log lines removing redundancy. Shorten thread names shown in log.  Made some log TRACE instead of DEBUG.  Capitalizations.

Upped log cleaner interval from every minute to every ten minutes. hbase.master.cleaner.interval

Lowered default count of threads started by Procedure Executor from count of CPUs to 1/4 of count of CPUs.


---

* [HBASE-19901](https://issues.apache.org/jira/browse/HBASE-19901) | *Major* | **Up yetus proclimit on nightlies**

Pass to yetus a dockermemlimit of 20G and a proclimit of 10000. Defaults are 4G and 1G respectively.


---

* [HBASE-19912](https://issues.apache.org/jira/browse/HBASE-19912) | *Minor* | **The flag "writeToWAL" of Region#checkAndRowMutate is useless**

Remove useless 'writeToWAL' flag of Region#checkAndRowMutate & related class


---

* [HBASE-19911](https://issues.apache.org/jira/browse/HBASE-19911) | *Major* | **Convert some tests from small to medium because they are timing out: TestNettyRpcServer, TestClientClusterStatus, TestCheckTestClasses**

Changed a few tests so they are medium sized rather than small size.

Also, upped the time we wait on small tests to 60seconds from 30seconds. Small tests are tests that run in 15seconds or less. What we changed was the timeout watcher. It is now more lax, more tolerant of dodgy infrastructure that might be running tests slowly.


---

* [HBASE-19892](https://issues.apache.org/jira/browse/HBASE-19892) | *Major* | **Checking 'patch attach' and yetus 0.7.0 and move to Yetus 0.7.0**

Moved our internal yetus reference from 0.6.0 to 0.7.0. Concurrently, I changed hadoopqa to run with 0.7.0 (by editing the config in jenkins).


---

* [HBASE-19873](https://issues.apache.org/jira/browse/HBASE-19873) | *Major* | **Add a CategoryBasedTimeout ClassRule for all UTs**

Along with @category -- small, medium, large -- all hbase tests must now carry a ClassRule as follows:

+  @ClassRule
+  public static final HBaseClassTestRule CLASS\_RULE =
+      HBaseClassTestRule.forClass(TestInterfaceAudienceAnnotations.class);

where the class changes by test.

Currently the classrule enforces timeout for the whole test suite -- i.e. if a SmallTest Category then all the tests in the TestSuite must complete inside 60seconds, the timeout we set on SmallTest Category test suite -- but is meant to be a repository for general, runtime, hbase test facility.


---

* [HBASE-19770](https://issues.apache.org/jira/browse/HBASE-19770) | *Critical* | **Add '--return-values' option to Shell to print return values of commands in interactive mode**

Introduces a new option to the HBase shell: -r, --return-values. When the shell is in "interactive" mode (default), the return value of shell commands are not returned to the user as they dirty the console output. For those who desire this functionality, the "--return-values" option restores the old functionality of the commands passing their return value to the user.


---

* [HBASE-15321](https://issues.apache.org/jira/browse/HBASE-15321) | *Major* | **Ability to open a HRegion from hdfs snapshot.**

HRegion.openReadOnlyFileSystemHRegion() provides the ability to open HRegion from a read-only hdfs snapshot.  Because hdfs snapshots are read-only, no cleanup happens when using this API.


---

* [HBASE-17513](https://issues.apache.org/jira/browse/HBASE-17513) | *Critical* | **Thrift Server 1 uses different QOP settings than RPC and Thrift Server 2 and can easily be misconfigured so there is no encryption when the operator expects it.**

This change fixes an issue where users could have unintentionally configured the HBase Thrift1 server to run without wire-encryption, when they believed they had configured the Thrift1 server to do so.


---

* [HBASE-19828](https://issues.apache.org/jira/browse/HBASE-19828) | *Major* | **Flakey TestRegionsOnMasterOptions.testRegionsOnAllServers**

Disables TestRegionsOnMasterOptions because Regions on Master does not work reliably; see HBASE-19831.


---

* [HBASE-18963](https://issues.apache.org/jira/browse/HBASE-18963) | *Major* | **Remove MultiRowMutationProcessor and implement mutateRows... methods using batchMutate()**

Modified HRegion.mutateRow() APIs to use batchMutate() instead of processRowsWithLocks() with MultiRowMutationProcessor. MultiRowMutationProcessor is removed to have single write path that uses batchMutate().


---

* [HBASE-19163](https://issues.apache.org/jira/browse/HBASE-19163) | *Major* | **"Maximum lock count exceeded" from region server's batch processing**

When there are many mutations against the same row in a batch, as each mutation will acquire a shared row lock, it will exceed the maximum shared lock count the java ReadWritelock supports (64k). Along with other optimization, the batch is divided into multiple possible minibatches. A new config is added to limit the maximum number of mutations in the minibatch.

   \<property\>
    \<name\>hbase.regionserver.minibatch.size\</name\>
    \<value\>20000\</value\>
   \</property\>
The default value is 20000.


---

* [HBASE-19739](https://issues.apache.org/jira/browse/HBASE-19739) | *Minor* | **Include thrift IDL files in HBase binary distribution**

Thrift IDLs are now shipped, bundled up in the respective hbase-\*thrift.jars (look for files ending in .thrift).


---

* [HBASE-11409](https://issues.apache.org/jira/browse/HBASE-11409) | *Major* | **Add more flexibility for input directory structure to LoadIncrementalHFiles**

Allows for users to bulk load entire tables from hdfs by specifying the parameter -loadTable.  This allows you to pass in a table level directory and have all regions column families bulk loaded, if you do not specify the -loadTable parameter LoadIncrementalHFiles will work as before. Note: you must have a pre-created table to run with -loadTable it will not create one for you.


---

* [HBASE-19769](https://issues.apache.org/jira/browse/HBASE-19769) | *Critical* | **IllegalAccessError on package-private Hadoop metrics2 classes in MapReduce jobs**

Client-side ZooKeeper metrics which were added to 2.0.0 alpha/beta releases cause issues when launching MapReduce jobs via {{yarn jar}} on the command line. This stems from ClassLoader separation issues that YARN implements. It was chosen that the easiest solution was to remove these ZooKeeper metrics entirely.


---

* [HBASE-19783](https://issues.apache.org/jira/browse/HBASE-19783) | *Minor* | **Change replication peer cluster key/endpoint from a not-null value to null is not allowed**

To reduce the confusing behavior, now when you call updatePeerConfig with empty ClusterKey or ReplicationEndpointImpl, but the value of field of the to-be-updated ReplicationPeerConfig is not null, we will throw exception instead of ignoring them.


---

* [HBASE-19483](https://issues.apache.org/jira/browse/HBASE-19483) | *Major* | **Add proper privilege check for rsgroup commands**

This JIRA aims at refactoring AccessController, using ACL as core library in CPs.
1. Stripping out a public class AccessChecker from AccessController, using ACL as core library in CPs. AccessChecker don't have any dependency on anything CP related. Create it's instance from other CPS.
2. Change the default value of hbase.security.authorization to false.
3. Don't use CP hooks to check access in RSGroup. Use the access checker instance directly in functions of RSGroupAdminServiceImpl.


---

* [HBASE-19358](https://issues.apache.org/jira/browse/HBASE-19358) | *Major* | **Improve the stability of splitting log when do fail over**

After HBASE-19358 we introduced a new property hbase.split.writer.creation.bounded to limit the opening writers for each WALSplitter. If set to true, we won't open any writer for recovered.edits until the entries accumulated in memory reaching hbase.regionserver.hlog.splitlog.buffersize (which defaults at 128M) and will write and close the file in one go instead of keeping the writer open. It's false by default and we recommend to set it to true if your cluster has a high region load (like more than 300 regions per RS), especially when you observed obvious NN/HDFS slow down during hbase (single RS or cluster) failover.


---

* [HBASE-19651](https://issues.apache.org/jira/browse/HBASE-19651) | *Minor* | **Remove LimitInputStream**

HBase had copied from guava the file LmiitedInputStream. This commit removes the copied file in favor of (our internal, shaded) guava's ByteStreams.limit. Guava 14.0's LIS noted: "Use ByteStreams.limit(java.io.InputStream, long) instead. This class is scheduled to be removed in Guava release 15.0."


---

* [HBASE-19691](https://issues.apache.org/jira/browse/HBASE-19691) | *Critical* | **Do not require ADMIN permission for obtaining ClusterStatus**

This change reverts an unintentional requirement for global ADMIN permission to obtain cluster status from the active HMaster.


---

* [HBASE-19486](https://issues.apache.org/jira/browse/HBASE-19486) | *Major* | ** Periodically ensure records are not buffered too long by BufferedMutator**

The BufferedMutator now supports two settings that are used to ensure records do not stay too long in the buffer of a BufferedMutator. For periodically flushing the BufferedMutator there is now a "Timeout": "How old may the oldest record in the buffer be before we force a flush" and a "TimerTick": How often do we check if the timeout has been exceeded. Using these settings you can make the BufferedMutator automatically flush the write buffer if after the specified number of milliseconds no flush has occurred.

This is mainly useful in streaming scenarios (i.e. writing data into HBase using Apache Flink/Beam/Storm) where it is common (especially in a test/development situation) to see small unpredictable bursts of data that need to be written into HBase. When using the BufferedMutator till now the effect was that records would remain in the write buffer until the buffer was full or an explicit flush was triggered. In practice this would mean that the 'last few records' of a burst would remain in the write buffer until the next burst arrives filling the buffer to capacity and thus triggering a flush.


---

* [HBASE-19670](https://issues.apache.org/jira/browse/HBASE-19670) | *Major* | **Workaround: Purge User API building from branch-2 so can make a beta-1**

Disable filtering of User API based off yetus annotation done in doclet. See parent issue for build failure currently being worked on but not done in time for a beta-1.


---

* [HBASE-19282](https://issues.apache.org/jira/browse/HBASE-19282) | *Major* | **CellChunkMap Benchmarking and User Interface**

When MSLAB is in use (that is the default config) , we will always use the CellChunkMap indexing variant for in memory flushed Immutable segments. When MSLAB is turned off, we will use CellAraryMap. These can not be changed with any configs.  The in memory flush threshold been made to be default to 10% of region flush size. This can be turned using 'hbase.memstore.inmemoryflush.threshold.factor'.


---

* [HBASE-19628](https://issues.apache.org/jira/browse/HBASE-19628) | *Major* | **ByteBufferCell should extend ExtendedCell**

ByteBufferCell → ByteBufferExtendedCell
MapReduceCell → MapReduceExtendedCell
ByteBufferChunkCell → ByteBufferChunkKeyValue
NoTagByteBufferChunkCell → NoTagByteBufferChunkKeyValue
KeyOnlyByteBufferCell → KeyOnlyByteBufferExtendedCell
TagRewriteByteBufferCell → TagRewriteByteBufferExtendedCell
ValueAndTagRewriteByteBufferCell → ValueAndTagRewriteByteBufferExtendedCell
EmptyByteBufferCell → EmptyByteBufferExtendedCell
FirstOnRowByteBufferCell → FirstOnRowByteBufferExtendedCell
LastOnRowByteBufferCell → LastOnRowByteBufferExtendedCell
FirstOnRowColByteBufferCell → FirstOnRowColByteBufferExtendedCell
FirstOnRowColTSByteBufferCell → FirstOnRowColTSByteBufferExtendedCell
LastOnRowColByteBufferCell → LastOnRowColByteBufferCell
OffheapDecodedCell → OffheapDecodedExtendedCell


---

* [HBASE-19576](https://issues.apache.org/jira/browse/HBASE-19576) | *Major* | **Introduce builder for ReplicationPeerConfig and make it immutable**

Add a ReplicationPeerConfigBuilder to create ReplicationPeerConfig and make ReplicationPeerConfig immutable. Meanwhile, deprecated set\* methods in ReplicationPeerConfig.


---

* [HBASE-10092](https://issues.apache.org/jira/browse/HBASE-10092) | *Critical* | **Move to slf4j**

We now have slf4j as our front-end. Be careful adding logging from here on out; make sure it slf4j.

From here on out, as us devs go, we need to convert log messages from being 'guarded' -- i.e. surrounded by if (LOG.isDebugEnabled...) -- to instead being parameterized log messages. e.g. the latter rather than the former in the below:

logger.debug("The new entry is "+entry+".");
logger.debug("The new entry is {}.", entry);

See [1] for background on perf benefits.

Note, FATAL log level is not present in slf4j. It is noted as a Marker but won't show in logs as a LEVEL.

1.  https://www.slf4j.org/faq.html#logging\_performance


---

* [HBASE-19148](https://issues.apache.org/jira/browse/HBASE-19148) | *Blocker* | **Reevaluate default values of configurations**

Removed unused hbase.fs.tmp.dir from hbase-default.xml.

Upped hbase.master.fileSplitTimeout from 30s to 10minutes (suggested by production experience)

Added note that handler-count should be ~CPU count.

hbase.regionserver.logroll.multiplier has been changed from 0.95 to 0.5 AND the default block size has been doubled.

A few of the core configs are now dumped to the log on startup.


---

* [HBASE-19492](https://issues.apache.org/jira/browse/HBASE-19492) | *Major* | **Add EXCLUDE\_NAMESPACE and EXCLUDE\_TABLECFS support to replication peer config**

Add two new field:  EXCLUDE\_NAMESPACE and EXCLUDE\_TABLECFS to replication peer config.

If replicate\_all flag is true, it means all user tables will be replicated to peer cluster. Then allow config exclude namespaces or exclude table-cfs which can't be replicated to  peer cluster.

If replicate\_all flag is false, it means all user tables can't be replicated to peer cluster. Then allow to config namespaces or table-cfs which will be replicated to peer cluster.


---

* [HBASE-19494](https://issues.apache.org/jira/browse/HBASE-19494) | *Major* | **Create simple WALKey filter that can be plugged in on the Replication Sink**

Adds means of adding very basic filter on the sink side of replication. We already have a means of installing filter source-side, which is better place to filter edits before they are shipped over the network, but this facility is needed by hbase-indexer.

Set hbase.replication.sink.walentrysinkfilter with a no-param Constructor implementation. See test in patch for example.


---

* [HBASE-19112](https://issues.apache.org/jira/browse/HBASE-19112) | *Blocker* | **Suspect methods on Cell to be deprecated**

Adds method Cell#getType which returns enum describing Cell Type.

Deprecates the following Cell methods:

 getTypeByte
 getSequenceId
 getTagsArray
 getTagsOffset
 getTagsLength

CPs trying to build cells should use RawCellBuilderFactory that supports  building cells with tags.


---

* [HBASE-14790](https://issues.apache.org/jira/browse/HBASE-14790) | *Major* | **Implement a new DFSOutputStream for logging WAL only**

Implement a FanOutOneBlockAsyncDFSOutput for writing WAL only, the WAL provider which uses this class is AsyncFSWALProvider.

It is based on netty, and will write to 3 DNs at the same time concurrently(fan-out) so generally it will lead to a lower latency. And it is also fail-fast, the stream will become unwritable immediately after there are any read/write errors, no pipeline recovery. You need to call recoverLease to force close the output for this case. And it only supports to write a file with a single block. For WAL this is a good behavior as we can always open a new file when the old one is broken. The performance analysis in HBASE-16890 shows that it has a better performance.

Behavior changes:
1. As now we write to 3 DNs concurrently, according to the visibility guarantee of HDFS, the data will be available immediately when arriving at DN since all the DNs will be considered as the last one in pipeline. This means replication may read uncommitted data and replicate it to the remote cluster and cause data inconsistency. HBASE-14004 is used to solve the problem.
2. There will be no sync failure. When the output is broken, we will open a new file and write all the unacked wal entries to the new file. This means that we may have duplicated entries in wal files. HBASE-14949 is used to solve this problem.


---

* [HBASE-15536](https://issues.apache.org/jira/browse/HBASE-15536) | *Critical* | **Make AsyncFSWAL as our default WAL**

Now the default WALProvider is AsyncFSWALProvider, i.e. 'asyncfs'.
If you want to change back to use FSHLog, please add this in hbase-site.xml
{code}
\<property\>
\<name\>hbase.wal.provider\</name\>
\<value\>filesystem\</value\>
\</property\>
{code}
If you want to use FSHLog with multiwal, please add this in hbase-site.xml
{code}
\<property\>
\<name\>hbase.wal.regiongrouping.delegate.provider\</name\>
\<value\>filesystem\</value\>
\</property\>
{code}

This patch also sets hbase.wal.async.use-shared-event-loop to false so WAL has its own netty event group.


---

* [HBASE-19462](https://issues.apache.org/jira/browse/HBASE-19462) | *Major* | **Deprecate all addImmutable methods in Put**

Deprecates Put#addImmutable as of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link #add(Cell)} and {@link org.apache.hadoop.hbase.CellBuilder} instead


---

* [HBASE-19213](https://issues.apache.org/jira/browse/HBASE-19213) | *Minor* | **Align check and mutate operations in Table and AsyncTable**

In Table interface deprecate checkAndPut, checkAndDelete and checkAndMutate methods.
Similarly to AsyncTable a new method was added to replace the deprecated ones: CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) with CheckAndMutateBuilder interface which can be used to construct the checkAnd\*() operations.


---

* [HBASE-19134](https://issues.apache.org/jira/browse/HBASE-19134) | *Major* | **Make WALKey an Interface; expose Read-Only version to CPs**

Made WALKey an Interface and added a WALKeyImpl implementation. WALKey comes through to Coprocessors. WALKey is read-only.


---

* [HBASE-18169](https://issues.apache.org/jira/browse/HBASE-18169) | *Blocker* | **Coprocessor fix and cleanup before 2.0.0 release**

Refactor of Coprocessor API for hbase2. Purged methods that exposed too much of our internals. Other hooks were recast so they no longer took or returned internal classes; instead we pass Interfaces or read-only versions of implementations.

Here is some overview doc on changes in hbase2 for Coprocessors including detail on why the change was made:
https://github.com/apache/hbase/blob/branch-2.0/dev-support/design-docs/Coprocessor\_Design\_Improvements-Use\_composition\_instead\_of\_inheritance-HBASE-17732.adoc


---

* [HBASE-19301](https://issues.apache.org/jira/browse/HBASE-19301) | *Major* | **Provide way for CPs to create short circuited connection with custom configurations**

Provided a way for the CP users to create a short circuitable connection with custom configs.

createConnection(Configuration) is added to MasterCoprocessorEnvironment, RegionServerCoprocessorEnvironment and RegionCoprocessorEnvironment.

The getConnection() method already available in these Env interfaces returns the cluster connection used by the server (which the server also uses) where as this new method will create a new connection on request. The difference from connection created using ConnectionFactory APIs is that this connection can short circuit the calls to same server avoiding the RPC paths. The connection will NOT be cached/maintained by server. That should be done the CPs.

Be careful creating Connections out of a Coprocessor. See the javadoc on these createConnection and getConnection.


---

* [HBASE-19357](https://issues.apache.org/jira/browse/HBASE-19357) | *Major* | **Bucket cache no longer L2 for LRU cache**

Removed cacheDataInL1 option for HCD
BucketCache is no longer the L2 for LRU on heap cache. When BC is used, data blocks will be strictly on BC only where as index/bloom blocks are on LRU L1 cache.
Config 'hbase.bucketcache.combinedcache.enabled' is removed. There is no way set combined mode = false. Means make BC as victim handler for LRU cache.
This will be one more noticeable change when one uses BucketCache in File mode.  Then the system table's data block(Including the META table)  will be cached in Bucket Cache files only. Plain scan from META files alone test reveal that the throughput of file mode BC is almost half only.  But for META entries we have RegionLocation cache at client side connections. So this would not be a big concern in a real cluster usage. Will check more on this and probably fix even when we do tiered BucketCache.


---

* [HBASE-19430](https://issues.apache.org/jira/browse/HBASE-19430) | *Major* | **Remove the SettableTimestamp and SettableSequenceId**

All the cells which are used in server side are of ExtendedCell now.


---

* [HBASE-19295](https://issues.apache.org/jira/browse/HBASE-19295) | *Major* | **The Configuration returned by CPEnv should be read-only.**

CoprocessorEnvironment#getConfiguration returns a READ-ONLY Configuration. Attempts at altering the returned Configuration -- whether setting or adding resources -- will result in an IllegalStateException warning of the Read-only condition of the returned Configuration.


---

* [HBASE-19410](https://issues.apache.org/jira/browse/HBASE-19410) | *Major* | **Move zookeeper related UTs to hbase-zookeeper and mark them as ZKTests**

There is a new HBaseZKTestingUtility which can only start a mini zookeeper cluster. And we will publish sources for test-jar for all modules.


---

* [HBASE-19323](https://issues.apache.org/jira/browse/HBASE-19323) | *Major* | **Make netty engine default in hbase2**

NettyRpcServer is now our default RPC server replacing SimpleRpcServer.


---

* [HBASE-19426](https://issues.apache.org/jira/browse/HBASE-19426) | *Major* | **Move has() and setTimestamp() to Mutation**

Moves #has and #setTimestamp back up to Mutation from the subclass Put so available to other Mutation implementations.


---

* [HBASE-19384](https://issues.apache.org/jira/browse/HBASE-19384) | *Critical* | **Results returned by preAppend hook in a coprocessor are replaced with null from other coprocessor even on bypass**

When a coprocessor sets 'bypass', we will skip calling subsequent Coprocessors that may be stacked-up on the method invocation; e.g. if a prePut has three coprocessors hooked up, if the first coprocessor decides to set 'bypass', we will not call the two subsequent coprocessors (this is similar to the 'complete' functionality that was in hbase1, removed in hbase2).


---

* [HBASE-19408](https://issues.apache.org/jira/browse/HBASE-19408) | *Trivial* | **Remove WALActionsListener.Base**

1) remove the WALActionsListener.Base
2) provide default method implementation to WALActionsListener
The person who want to receive the notification of WAL events should implements the WALActionsListener rather than WALActionsListener.Base.


---

* [HBASE-19339](https://issues.apache.org/jira/browse/HBASE-19339) | *Critical* | **Eager policy results in the negative size of memstore**

Enable TestAcidGuaranteesWithEagerPolicy and TestAcidGuaranteesWithAdaptivePolicy


---

* [HBASE-19336](https://issues.apache.org/jira/browse/HBASE-19336) | *Major* | **Improve rsgroup to allow assign all tables within a specified namespace by only writing namespace**

Add two new shell cmd.
move\_namespaces\_rsgroup is used to reassign tables of specified namespaces from one RegionServer group to another.
move\_servers\_namespaces\_rsgroup is used to reassign regionServers and tables of specified namespaces from one group to another.


---

* [HBASE-19285](https://issues.apache.org/jira/browse/HBASE-19285) | *Critical* | **Add per-table latency histograms**

Per-RegionServer table latency histograms have been returned to HBase (after being removed due to impacting performance). These metrics are exposed via a new JMX bean "TableLatencies" with the typical naming conventions: namespace, table, and histogram component.


---

* [HBASE-19359](https://issues.apache.org/jira/browse/HBASE-19359) | *Major* | **Revisit the default config of hbase client retries number**

The default value of hbase.client.retries.number was 35. It is now 10.
And for server side, the default hbase.client.serverside.retries.multiplier was 10. So the server side retries number was 35 \* 10 = 350. It is now 3.


---

* [HBASE-18090](https://issues.apache.org/jira/browse/HBASE-18090) | *Major* | **Improve TableSnapshotInputFormat to allow more multiple mappers per region**

In this task, we make it possible to run multiple mappers per region in the table snapshot. The following code is primary table snapshot mapper initializatio:

TableMapReduceUtil.initTableSnapshotMapperJob(
          snapshotName,                     // The name of the snapshot (of a table) to read from
          scan,                                      // Scan instance to control CF and attribute selection
          mapper,                                 // mapper
          outputKeyClass,                   // mapper output key
          outputValueClass,                // mapper output value
          job,                                       // The current job to adjust
          true,                                     // upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars)
          restoreDir,                           // a temporary directory to copy the snapshot files into
);

The job only run one map task per region in the table snapshot. With this feature, client can specify the desired num of mappers when init table snapshot mapper job：

TableMapReduceUtil.initTableSnapshotMapperJob(
          snapshotName,                     // The name of the snapshot (of a table) to read from
          scan,                                      // Scan instance to control CF and attribute selection
          mapper,                                 // mapper
          outputKeyClass,                   // mapper output key
          outputValueClass,                // mapper output value
          job,                                       // The current job to adjust
          true,                                     // upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars)
          restoreDir,                           // a temporary directory to copy the snapshot files into
          splitAlgorithm,                     // splitAlgo algorithm to split, current split algorithms  support RegionSplitter.UniformSplit() and RegionSplitter.HexStringSplit()
          n                                         // how many input splits to generate per one region
);


---

* [HBASE-19035](https://issues.apache.org/jira/browse/HBASE-19035) | *Major* | **Miss metrics when coprocessor use region scanner to read data**

1. Move read requests count to region level. Because RegionScanner is exposed to CP.
2. Update write requests count in processRowsWithLocks.
3. Remove requestRowActionCount in RSRpcServices. This metric can be computed by region's readRequestsCount and writeRequestsCount.


---

* [HBASE-19318](https://issues.apache.org/jira/browse/HBASE-19318) | *Critical* | **MasterRpcServices#getSecurityCapabilities explicitly checks for the HBase AccessController implementation**

Fixes an issue with loading customer coprocessor endpoint implementations inside of the HBase Master which breaks Apache Ranger.


---

* [HBASE-19092](https://issues.apache.org/jira/browse/HBASE-19092) | *Critical* | **Make Tag IA.LimitedPrivate and expose for CPs**

This JIRA aims at exposing Tags for Coprocessor usage.
Tag interface is now exposed to Coprocessors and CPs can make use of this interface to create their own Tags.
RawCell is a new interface that is a subtype of Cell and that is exposed to CPs. RawCell has the following APIs

List\<Tag\> getTags()
Optional\<Tag\> getTag(byte type)
byte[] cloneTags()

The above APIs helps to read tags from the Cell.

CellUtil#createCell(Cell cell, List\<Tag\> tags)
CellUtil#createCell(Cell cell, byte[] tags)
CellUtil#createCell(Cell cell, byte[] value, byte[] tags)
are deprecated.
If CPs want to create a cell with Tags they can use the RegionCoprocessorEnvironment#getCellBuilder() that returns an ExtendedCellBuilder.
Using ExtendedCellBuilder the CP can create Cells with Tags. Other helper methods to work on Tags are available as static APIs in Tag interface.


---

* [HBASE-19266](https://issues.apache.org/jira/browse/HBASE-19266) | *Minor* | **TestAcidGuarantees should cover adaptive in-memory compaction**

separate the TestAcidGuarantees by the policy:
1) NONE -\> TestAcidGuaranteesWithNoInMemCompaction
2) BASIC -\> TestAcidGuaranteesWithBasicPolicy
3) EAGER -\> TestAcidGuaranteesWithEagerPolicy
4) ADAPTIVE -\> TestAcidGuaranteesWithAdaptivePolicy

TestAcidGuaranteesWithEagerPolicy and TestAcidGuaranteesWithAdaptivePolicy are disabled by default as the eager policy may cause the negative size of memstore.


---

* [HBASE-16868](https://issues.apache.org/jira/browse/HBASE-16868) | *Critical* | **Add a replicate\_all flag to avoid misuse the namespaces and table-cfs config of replication peer**

Add a replicate\_all flag to replication peer config. The default value is true, which means all user tables (REPLICATION\_SCOPE != 0 ) will be replicated to peer cluster.

How to config a peer from replicate all to only replicate special namespace/tablecfs?
Step1. Add a new peer with no namespace/tablecfs config, the replicate\_all flag will be true automatically.
Step2. User want only replicate some namespaces or tables, so set replicate\_all flag to false first.
Step3. Add special namespaces or table-cfs config to the replication peer.

How to config a peer from replicate special namespace/tablecfs to replicate all?
Step1. Add a new peer with special namespace/tablecfs config, the replicate\_all flag will be false automatically.
Step2. User want replicate all user tables, so remove the special namespace/tablecfs config first.
Step3. Set replicate\_all flag to true.

How to config replicate nothing?
Set replicate\_all flag to false and no namespace/tablecfs config, then all tables cannot be replicated to peer cluster.


---

* [HBASE-19122](https://issues.apache.org/jira/browse/HBASE-19122) | *Critical* | **preCompact and preFlush can bypass by returning null scanner; shut it down**

Remove the ability to 'bypass' preFlush and preCompact by returning a null Scanner. Bypass is disallowed on these methods in hbase2.


---

* [HBASE-19200](https://issues.apache.org/jira/browse/HBASE-19200) | *Major* | **make hbase-client only depend on ZKAsyncRegistry and ZNodePaths**

ConnectionImplementation now uses asynchronous connections to zookeeper via ZKAsyncRegistry to get cluster id, master address, meta region location, etc.
Since ZKAsyncRegistry uses curator framework, this change purges a lot of zookeeper dependencies in hbase-client.
Now hbase-client only depends on only ZKAsyncRegistry, ZNodePaths and the newly introduced ZKMetadata.


---

* [HBASE-19311](https://issues.apache.org/jira/browse/HBASE-19311) | *Major* | **Promote TestAcidGuarantees to LargeTests and start mini cluster once to make it faster**

Introduce a AcidGuaranteesTestTool and expose as tool instead of TestAcidGuarantees. Now TestAcidGuarantees is just a UT.


---

* [HBASE-19293](https://issues.apache.org/jira/browse/HBASE-19293) | *Major* | **Support adding a new replication peer in disabled state**

Add a boolean parameter which means the new replication peer's state is enabled or disabled for Admin/AsyncAdmin's addReplicationPeer method. Meanwhile, you can use shell cmd to add a enabled/disabled replication peer. The STATE parameter is optional and the default state is enabled.

hbase\> add\_peer '1', CLUSTER\_KEY =\> "server1.cie.com:2181:/hbase", STATE =\> "ENABLED"
hbase\> add\_peer '1', CLUSTER\_KEY =\> "server1.cie.com:2181:/hbase", STATE =\> "DISABLED"


---

* [HBASE-19123](https://issues.apache.org/jira/browse/HBASE-19123) | *Major* | **Purge 'complete' support from Coprocesor Observers**

This issue removes the 'complete' facility that was in ObserverContext. It is no longer possible for a Coprocessor to cut the chain-of-invocation and insist its response prevails.


---

* [HBASE-18911](https://issues.apache.org/jira/browse/HBASE-18911) | *Major* | **Unify Admin and AsyncAdmin's methods name**

Deprecated 4 methods for Admin interface.
Deprecated compactRegionServer(ServerName, boolean). Use compactRegionServer(ServerName) and majorCompactcompactRegionServer(ServerName) instead.
Deprecated getRegionLoad(ServerName) method. Use getRegionLoads(ServerName) instead.
Deprecated getRegionLoad(ServerName, TableName) method. Use getRegionLoads(ServerName, TableName) instead.
Deprecated getQuotaRetriever(QuotaFilter) instead. Use  getQuota(QuotaFilter) instead.

Add 7 methods for Admin interface.
ServerName getMaster();
Collection\<ServerName\> getBackupMasters();
Collection\<ServerName\> getRegionServers();
boolean splitSwitch(boolean enabled, boolean synchronous);
boolean mergeSwitch(boolean enabled, boolean synchronous);
boolean isSplitEnabled();
boolean isMergeEnabled();


---

* [HBASE-18703](https://issues.apache.org/jira/browse/HBASE-18703) | *Critical* | **Inconsistent behavior for preBatchMutate in doMiniBatchMutate and processRowsWithLocks**

Two write paths Region.batchMutate() and Region.mutateRows() are unified and inconsistencies are resolved.


---

* [HBASE-18964](https://issues.apache.org/jira/browse/HBASE-18964) | *Major* | **Deprecate RowProcessor and processRowsWithLocks() APIs that take RowProcessor as an argument**

RowProcessor and Region#processRowsWithLocks() methods that take RowProcessor as an argument are deprecated. Use Coprocessors if you want to customize handling.


---

* [HBASE-19251](https://issues.apache.org/jira/browse/HBASE-19251) | *Major* | **Merge RawAsyncTable and AsyncTable**

Merge the RawAsyncTable and AsyncTable interfaces. Use generic to reflection the difference between the observer style scan API. For the implementation which does not have a user specified thread pool, the observer is AdvancedScanResultConsumer. For the implementation which needs a user specified thread pool, the observer is ScanResultConsumer.


---

* [HBASE-19262](https://issues.apache.org/jira/browse/HBASE-19262) | *Major* | **Revisit checkstyle rules**

Change the import order rule that now we should put the shaded import at bottom. Ignore the VisibilityModifier warnings for test code.


---

* [HBASE-19187](https://issues.apache.org/jira/browse/HBASE-19187) | *Minor* | **Remove option to create on heap bucket cache**

Removing the on heap Bucket cache feature.
The config "hbase.bucketcache.ioengine" no longer support the 'heap' value.
Its supported values now are 'offheap',  'file:\<path\>', 'files:\<path\>'  and 'mmap:\<path\>'


---

* [HBASE-12350](https://issues.apache.org/jira/browse/HBASE-12350) | *Minor* | **Backport error-prone build support to branch-1 and branch-2**

This change introduces compile time support for running the error-prone suite of static analyses. Enable with -PerrorProne on the Maven command line. Requires JDK 8 or higher. (Don't enable if building with JDK 7.)


---

* [HBASE-14350](https://issues.apache.org/jira/browse/HBASE-14350) | *Blocker* | **Procedure V2 Phase 2: Assignment Manager**

(Incomplete)

= Incompatbiles

== Coprocessor Incompatibilities

Split/Merge have moved to the Master; it runs them now. Means hooks around Split/Merge are now noops. To intercept Split/Merge phases, CPs need to intercept on MasterObserver.


---

* [HBASE-19189](https://issues.apache.org/jira/browse/HBASE-19189) | *Major* | **Ad-hoc test job for running a subset of tests lots of times**

<!-- markdown -->


Folks can now test out tests on an arbitrary release branch. Head over to [builds.a.o job "HBase-adhoc-run-tests"](https://builds.apache.org/view/H-L/view/HBase/job/HBase-adhoc-run-tests/), then pick "Build with parameters".
Tests are specified as just names e.g. TestLogRollingNoCluster. can also be a glob. e.g. TestHFile*


---

* [HBASE-19220](https://issues.apache.org/jira/browse/HBASE-19220) | *Major* | **Async tests time out talking to zk; 'clusterid came back null'**

Changed retries from 3 to 30 for zk initial connect for registry.


---

* [HBASE-19002](https://issues.apache.org/jira/browse/HBASE-19002) | *Minor* | **Introduce more examples to show how to intercept normal region operations**

With the change in Coprocessor APIs, the hbase-examples module has been updated to provide additional examples that show how to write Coprocessors against the new API.


---

* [HBASE-18961](https://issues.apache.org/jira/browse/HBASE-18961) | *Major* | **doMiniBatchMutate() is big, split it into smaller methods**

HRegion.batchMutate()/ doMiniBatchMutate() is refactored with aim to unify batchMutate() and mutateRows() code paths later. batchMutate() currently handles 2 types of batches: MutationBatchOperations and ReplayBatchOperations. Common base class BatchOperations is augmented with common methods which are overridden in derived classes as needed. doMiniBatchMutate() is implemented using common methods in base class BatchOperations.


---

* [HBASE-19103](https://issues.apache.org/jira/browse/HBASE-19103) | *Minor* | **Add BigDecimalComparator for filter**

If BigDecimal is stored as value, and you need to add a matched comparator to the value filter when scanning, a BigDecimalComparator can be used.


---

* [HBASE-19111](https://issues.apache.org/jira/browse/HBASE-19111) | *Critical* | **Add missing CellUtil#isPut(Cell) methods**

A new public API method was added to CellUtil "isPut(Cell)" for clients to use to determine if the Cell is for a Put operation.

Additionally, other CellUtil API calls which expose Cell-implementation were marked as deprecated and will be removed in a future version.


---

* [HBASE-19160](https://issues.apache.org/jira/browse/HBASE-19160) | *Critical* | **Re-expose CellComparator**

CellComparator is now InterfaceAudience.Public


---

* [HBASE-19131](https://issues.apache.org/jira/browse/HBASE-19131) | *Major* | **Add the ClusterStatus hook and cleanup other hooks which can be replaced by ClusterStatus hook**

1) Add preGetClusterStatus() and postGetClusterStatus() hooks
2) add preGetClusterStatus() to access control check - an admin action


---

* [HBASE-19095](https://issues.apache.org/jira/browse/HBASE-19095) | *Major* | **Add CP hooks in RegionObserver for in memory compaction**

Add 4 methods in RegionObserver:
preMemStoreCompaction
preMemStoreCompactionCompactScannerOpen
preMemStoreCompactionCompact
postMemStoreCompaction
preMemStoreCompaction and postMemStoreCompaction will always be called for all in memory compactions. Under eager mode, preMemStoreCompactionCompactScannerOpen will be called before opening store scanner to allow you changing the max versions and TTL, and preMemStoreCompactionCompact will be called after the creation to let you do wrapping.


---

* [HBASE-19152](https://issues.apache.org/jira/browse/HBASE-19152) | *Trivial* | **Update refguide 'how to build an RC' and the make\_rc.sh script**

The make\_rc.sh script can run an hbase2 build now generating tarballs and pushing up to maven repository. TODO: Sign and checksum, check tarball, push to apache dist.....


---

* [HBASE-19179](https://issues.apache.org/jira/browse/HBASE-19179) | *Critical* | **Remove hbase-prefix-tree**

Purged the hbase-prefix-tree module and all references from the code base.

prefix-tree data block encoding was a super cool experimental feature that saw some usage initially but has since languished. If interested in carrying this sweet facility forward, write the dev list and we'll restore this module.


---

* [HBASE-19176](https://issues.apache.org/jira/browse/HBASE-19176) | *Major* | **Remove hbase-native-client from branch-2**

Removed the hbase-native-client module from branch-2 (it is still in Master). It is not complete. Look for a finished C++ client in the near future. Will restore native client to branch-2 at that point.


---

* [HBASE-19144](https://issues.apache.org/jira/browse/HBASE-19144) | *Major* | **[RSgroups] Retry assignments in FAILED\_OPEN state when servers (re)join the cluster**

When regionserver placement groups (RSGroups) is active, as servers join the cluster the Master will attempt to reassign regions in FAILED\_OPEN state.


---

* [HBASE-18770](https://issues.apache.org/jira/browse/HBASE-18770) | *Critical* | **Remove bypass method in ObserverContext and implement the 'bypass' logic case by case**

Removes blanket bypass mechanism (Observer#bypass). Instead, a curated subset of methods are bypassable.

    Changes Coprocessor ObserverContext 'bypass' semantic. We flip the
    default so bypass is NOT supported on Observer invocations; only a
    couple of preXXX methods in RegionObserver allow it: e.g.  preGet
    and prePut but not preFlush, etc. Everywhere else, we throw
    a Exception if a Coprocessor Observer tries to invoke bypass. Master
    Observers can no longer stop or change move, split, assign, create table, etc.
    preBatchMutate can no longer be bypassed (bypass the finer-grained
    prePut, preDelete, etc. instead)

    Ditto on complete, the mechanism that allowed a Coprocessor
    rule that all subsequent Coprocessors are skipped in an
    invocation chain; now, complete is only available to
    bypassable methods (and Coprocessors will get an exception if
    they try to 'complete' when it is not allowed).

    See javadoc for whether a Coprocessor Observer method supports
    'bypass'. If no mention, 'bypass' is NOT supported.

The below methods have been marked deprecated in hbase2. We would have liked to have removed them because they use IA.Private parameters but they are in use by CoreCoprocessors or are critical to downstreamers and we have no alternatives to provide currently.

@Deprecated public boolean prePrepareTimeStampForDeleteVersion(final Mutation mutation, final Cell kv, final byte[] byteNow, final Get get) throws IOException {

@Deprecated public boolean preWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit) throws IOException {

@Deprecated public void postWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit) throws IOException {

@Deprecated public DeleteTracker postInstantiateDeleteTracker(DeleteTracker result) throws IOException

Metrics are updated now even if the Coprocessor does a bypass; e.g. The put count is updated even if a Coprocessor bypasses the core put operation (We do it this way so no need for Coprocessors to have access to our core metrics system).


---

* [HBASE-19033](https://issues.apache.org/jira/browse/HBASE-19033) | *Blocker* | **Allow CP users to change versions and TTL before opening StoreScanner**

Add back the three methods without a return value:
preFlushScannerOpen
preCompactScannerOpen
preStoreScannerOpen

Introduce a ScanOptions interface to let CP users change the max versions and TTL of a ScanInfo. It will be passed as a parameter in the three methods above.

Inntroduce a new example WriteHeavyIncrementObserver which convert increment to put and do aggregating when get. It uses the above three methods.


---

* [HBASE-19110](https://issues.apache.org/jira/browse/HBASE-19110) | *Minor* | **Add default for Server#isStopping & #getFileSystem**

Made defaults for Server#isStopping and Server#getFileSystem. Should have done this when I added them (lesson learned, was actually mentioned in a review).


---

* [HBASE-19047](https://issues.apache.org/jira/browse/HBASE-19047) | *Critical* | **CP exposed Scanner types should not extend Shipper**

RegionObserver#preScannerOpen signature changed
RegionScanner preScannerOpen( ObserverContext\<RegionCoprocessorEnvironment\> c, Scan scan,  RegionScanner s)   -\>   void preScannerOpen( ObserverContext\<RegionCoprocessorEnvironment\> c, Scan scan)
The pre hook can no longer return a RegionScanner instance.


---

* [HBASE-18995](https://issues.apache.org/jira/browse/HBASE-18995) | *Critical* | **Move methods that are for internal usage from CellUtil to Private util class**

Split CellUtil into public CellUtil and PrivateCellUtil for Internal use only.


---

* [HBASE-18906](https://issues.apache.org/jira/browse/HBASE-18906) | *Critical* | **Provide Region#waitForFlushes API**

Provided an API in Region (Exposed to CPs)
boolean waitForFlushes(long timeout)
This call will make the current thread to be waiting for all flushes in this region to be finished.  (Upto the time out time being specified). The boolean return value specify whether the flushes are really over or the time out being elapsed. Return false when timeout elapsed but flushes are not over or  true when flushes are over


---

* [HBASE-18905](https://issues.apache.org/jira/browse/HBASE-18905) | *Major* | **Allow CPs to request flush on Region and know the completion of the requested flush**

Add a FlushLifeCycleTracker which is similiar to CompactionLifeCycleTracker for tracking flush.
Add a requestFlush method in Region interface to let CP users request flush on a region. The operation is asynchronous, you need to use the FlushLifeCycleTracker to track the flush.
The difference with CompactionLifeCycleTracker is that, flush is per region so we do not use Store as a parameter of the methods. And also, notExecuted means the whole flush has not been executed, and afterExecution means the whole flush has been finished, so we do not have a separated completed method. A flush will be ended either by notExecuted or afterExecution.


---

* [HBASE-19048](https://issues.apache.org/jira/browse/HBASE-19048) | *Major* | **Cleanup MasterObserver hooks which takes IA private params**

Purged InterfaceAudience.Private parameters from methods in MasterObserver.

preAbortProcedure no longer takes a ProcedureExecutor.

postGetProcedures no longer takes a list of Procedures.

postGetLocks no longer takes a list of locks.

preRequestLock and postRequestLock no longer take lock type.

preLockHeartbeat and postLockHeartbeat no longer takes a lock procedure.

The implication is that that the Coprocessors that depended on these params have had to coarsen so for example, the AccessController can not do access per Procedure or Lock but rather, makes a judgement on the general access (You'll need to be ADMIN to see list of procedures and locks).


---

* [HBASE-18994](https://issues.apache.org/jira/browse/HBASE-18994) | *Major* | **Decide if META/System tables should use Compacting Memstore or Default Memstore**

Added a new config 'hbase.systemtables.compacting.memstore.type"  for the system tables. By default all the system tables will have 'NONE' as the type and so it will be using the default memstore by default.
{code}
 \<property\>
    \<name\>hbase.systemtables.compacting.memstore.type\</name\>
    \<value\>NONE\</value\>
  \</property\>
{code}


---

* [HBASE-19029](https://issues.apache.org/jira/browse/HBASE-19029) | *Critical* | **Align RPC timout methods in Table and AsyncTableBase**

Deprecate the following methods in Table:
- int getRpcTimeout()
- int getReadRpcTimeout()
- int getWriteRpcTimeout()
- int getOperationTimeout()

Add the following methods to Table:
- long getRpcTimeout(TimeUnit)
- long getReadRpcTimeout(TimeUnit)
- long getWriteRpcTimeout(TimeUnit)
- long getOperationTimeout(TimeUnit)

Add missing deprecation tag for long getRpcTimeout(TimeUnit unit) in AsyncTableBase


---

* [HBASE-18410](https://issues.apache.org/jira/browse/HBASE-18410) | *Major* | **FilterList  Improvement.**

In this task, we fixed all existing bugs in FilterList, and did the code refactor which ensured interface compatibility .

The primary bug  fixes are :
1. For sub-filter in FilterList with MUST\_PASS\_ONE, if previous filterKeyValue() of sub-filter returns NEXT\_COL, we cannot make sure that the next cell will be the first cell in next column, because FilterList choose the minimal forward step among sub-filters, and it may return a SKIP. so here we add an extra check to ensure that the next cell will match preivous return code for sub-filters.
2. Previous logic about transforming cell of FilterList is incorrect, we should set the previous transform result (rather than the given cell in question) as the initial vaule of transform cell before call filterKeyValue() of FilterList.
3. Handle the ReturnCodes which the previous code did not handle.

About code refactor, we divided the FilterList into two separated sub-classes: FilterListWithOR and FilterListWithAND,  The FilterListWithOR has been optimised to choose the next minimal step to seek cell rather than SKIP cell one by one, and the FilterListWithAND  has been optimised to choose the next maximal key to seek among sub-filters in filter list. All in all, The code in FilterList is clean and easier to follow now.

Note that ReturnCode NEXT\_ROW has been redefined as skipping to next row in current family,   not to next row in all family. it’s more reasonable, because ReturnCode is a concept in store level, not in region level.

Another bug that needs attention is: filterAllRemaining() in FilterList with MUST\_PASS\_ONE  will now return false if the filter list is empty whereas earlier it used to return true for Operator.MUST\_PASS\_ONE.  it's more reasonable now.


---

* [HBASE-19077](https://issues.apache.org/jira/browse/HBASE-19077) | *Critical* | **Have Region\*CoprocessorEnvironment provide an ImmutableOnlineRegions**

Adds getOnlineRegions to the RegionCoprocessorEnvironment (Context) and ditto to RegionServerCoprocessorEnvironment. Allows Coprocessor get list of Regions online on the currently hosting RegionServer.


---

* [HBASE-19021](https://issues.apache.org/jira/browse/HBASE-19021) | *Critical* | **Restore a few important missing logics for balancer in 2.0**

Re-enabled 'hbase.master.loadbalance.bytable', default 'false'.
Draining servers are removed from consideration by blancer.balanceCluster() call.


---

* [HBASE-19049](https://issues.apache.org/jira/browse/HBASE-19049) | *Major* | **Update kerby to 1.0.1 GA release**

HBase now relies on Kerby version 1.0.1 for its test environment. No downstream facing change is expected.


---

* [HBASE-16290](https://issues.apache.org/jira/browse/HBASE-16290) | *Major* | **Dump summary of callQueue content; can help debugging**

Patch to print summary of call queues by size and count. This is displayed on the debug dump page of region server UI


---

* [HBASE-18846](https://issues.apache.org/jira/browse/HBASE-18846) | *Major* | **Accommodate the hbase-indexer/lily/SEP consumer deploy-type**

Makes it so hbase-indexer/lily can move off dependence on internal APIs and instead move to public APIs.

Adds being able to disable near-all HRegionServer services. This along with an existing plugin mechanism which allows configuring the RegionServer to host an alternate Connection implementation, makes it so we can put up a cluster of hollowed-out HRegionServers purposed to pose as a Replication Sink for a source HBase Cluster (Users do not need to figure our RPC, our PB encodings, build a distributed service, etc.). In the alternate supplied Connection implementation, hbase-indexer would install its own code to catch the Replication.

Below and attached are sample hbase-server.xml files and alternate Connection implementations. To start up an HRegionServer as a sink, first make sure there is a ZooKeeper ensemble we can talk to. If none, just start one:
{code}
./bin/hbase-daemon.sh start zookeeper
{code}

To start up a single RegionServer, put in place the below sample hbase-site.xml and a derviative of the below IndexerConnection on the CLASSPATH, and then start the RegionServer:
{code}
./bin/hbase-daemon.sh  start  org.apache.hadoop.hbase.regionserver.HRegionServer
{code}
Stdout and Stderr will go into files under configured logs directory. Browse to localhost:16030 to find webui (unless disabled).

DETAILS

This patch adds configuration to disable RegionServer internal Services, Managers, Caches, etc., starting up.

By default a RegionServer starts up an Admin and Client Service. To disable either or both, use the below booleans:
{code}
hbase.regionserver.admin.service
hbase.regionserver.client.service
{code}

Both default true.

To make a HRegionServer startup and stay up without expecting to communicate with a master, set the below boolean to false:

{code}
hbase.masterless
{code]
Default is false.

h3. Sample hbase-site.xml that disables internal HRegionServer Services
Below is an example hbase-site.xml that turns off most Services and that then installs an alternate Connection implementation, one that is nulled out in all regards except in being able to return a "Table" that can catch a Replication Stream in its {code}batch(List\<? extends Row\> actions, Object[] results){code} method. i.e. what the hbase-indexer wants. I also add the example alternate Connection implementation below (both of these files are also attached to this issue). Expects there to be an up and running zookeeper ensemble.

{code}
\<configuration\>
  \<!-- This file is an example for hbase-indexer. It shuts down
       facility in the regionserver and interjects a special
       Connection implementation which is how hbase-indexer will
       receive the replication stream from source hbase cluster.
       See the class referenced in the config.

       Most of the config in here is booleans set to off and
       setting values to zero so services doon't start. Some of
       the flags are new via this patch.
--\>
  \<!--Need this for the RegionServer to come up standalone--\>
  \<property\>
    \<name\>hbase.cluster.distributed\</name\>
    \<value\>true\</value\>
  \</property\>

  \<!--This is what you implement, a Connection that returns a Table that
       overrides the batch call. It is at this point you do your indexer inserts.
    --\>
  \<property\>
    \<name\>hbase.client.connection.impl\</name\>
    \<value\>org.apache.hadoop.hbase.client.IndexerConnection\</value\>
    \<description\>A customs connection implementation just so we can interject our
      own Table class, one that has an override for the batch call which receives
      the replication stream edits; i.e. it is called by the replication sink
      #replicateEntries method.\</description\>
  \</property\>

  \<!--Set hbase.regionserver.info.port to -1 for no webui--\>

  \<!--Below are configs to shut down unused services in hregionserver--\>
  \<property\>
    \<name\>hbase.regionserver.admin.service\</name\>
    \<value\>false\</value\>
    \<description\>Do NOT stand up an Admin Service Interface on RPC\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.regionserver.client.service\</name\>
    \<value\>false\</value\>
    \<description\>Do NOT stand up a client-facing Service on RPC\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.wal.provider\</name\>
    \<value\>org.apache.hadoop.hbase.wal.DisabledWALProvider\</value\>
    \<description\>Set WAL service to be the null WAL\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.regionserver.workers\</name\>
    \<value\>false\</value\>
    \<description\>Turn off all background workers, log splitters, executors, etc.\</description\>
  \</property\>
  \<property\>
    \<name\>hfile.block.cache.size\</name\>
    \<value\>0.0001\</value\>
    \<description\>Turn off block cache completely\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.mob.file.cache.size\</name\>
    \<value\>0\</value\>
    \<description\>Disable MOB cache.\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.masterless\</name\>
    \<value\>true\</value\>
    \<description\>Do not expect Master in cluster.\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.regionserver.metahandler.count\</name\>
    \<value\>1\</value\>
    \<description\>How many priority handlers to run; we probably need none.
    Default is 20 which is too much on a server like this.\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.regionserver.replication.handler.count\</name\>
    \<value\>1\</value\>
    \<description\>How many replication handlers to run; we probably need none.
    Default is 3 which is too much on a server like this.\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.regionserver.handler.count\</name\>
    \<value\>10\</value\>
    \<description\>How many default handlers to run; tie to # of CPUs.
    Default is 30 which is too much on a server like this.\</description\>
  \</property\>
  \<property\>
    \<name\>hbase.ipc.server.read.threadpool.size\</name\>
    \<value\>3\</value\>
    \<description\>How many Listener request reaaders to run; tie to a portion # of CPUs (1/4?).
    Default is 10 which is too much on a server like this.\</description\>
  \</property\>
\</configuration\>
{code}

h2. Sample Connection Implementation
Has call-out for where an hbase-indexer would insert its capture code.
{code}
package org.apache.hadoop.hbase.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;


/\*\*
 \* Sample class for hbase-indexer.
 \* DO NOT COMMIT TO HBASE CODEBASE!!!
 \* Overrides Connection just so we can return a Table that has the
 \* method that the replication sink calls, i.e. Table#batch.
 \* It is at this point that the hbase-indexer catches the replication
 \* stream so it can insert into the lucene index.
 \*/
public class IndexerConnection implements Connection {
  private final Configuration conf;
  private final User user;
  private final ExecutorService pool;
  private volatile boolean closed = false;

  public IndexerConnection(Configuration conf, ExecutorService pool, User user) throws IOException {
    this.conf = conf;
    this.user = user;
    this.pool = pool;
  }

  @Override
  public void abort(String why, Throwable e) {}

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    return null;
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Admin getAdmin() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (!this.closed) this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public TableBuilder getTableBuilder(final TableName tn, ExecutorService pool) {
    if (isClosed()) {
      throw new RuntimeException("IndexerConnection is closed.");
    }
    final Configuration passedInConfiguration = getConfiguration();
    return new TableBuilder() {
      @Override
      public TableBuilder setOperationTimeout(int timeout) {
        return null;
      }

      @Override
      public TableBuilder setRpcTimeout(int timeout) {
        return null;
      }

      @Override
      public TableBuilder setReadRpcTimeout(int timeout) {
        return null;
      }

      @Override
      public TableBuilder setWriteRpcTimeout(int timeout) {
        return null;
      }

      @Override
      public Table build() {
        return new Table() {
          private final Configuration conf = passedInConfiguration;
          private final TableName tableName = tn;

          @Override
          public TableName getName() {
            return this.tableName;
          }

          @Override
          public Configuration getConfiguration() {
            return this.conf;
          }

          @Override
          public void batch(List\<? extends Row\> actions, Object[] results)
          throws IOException, InterruptedException {
            // Implementation goes here.
          }

          @Override
          public HTableDescriptor getTableDescriptor() throws IOException {
            return null;
          }

          @Override
          public TableDescriptor getDescriptor() throws IOException {
            return null;
          }

          @Override
          public boolean exists(Get get) throws IOException {
            return false;
          }

          @Override
          public boolean[] existsAll(List\<Get\> gets) throws IOException {
            return new boolean[0];
          }

          @Override
          public \<R\> void batchCallback(List\<? extends Row\> actions, Object[] results, Batch.Callback\<R\> callback) throws IOException, InterruptedException {

          }

          @Override
          public Result get(Get get) throws IOException {
            return null;
          }

          @Override
          public Result[] get(List\<Get\> gets) throws IOException {
            return new Result[0];
          }

          @Override
          public ResultScanner getScanner(Scan scan) throws IOException {
            return null;
          }

          @Override
          public ResultScanner getScanner(byte[] family) throws IOException {
            return null;
          }

          @Override
          public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
            return null;
          }

          @Override
          public void put(Put put) throws IOException {

          }

          @Override
          public void put(List\<Put\> puts) throws IOException {

          }

          @Override
          public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
            return false;
          }

          @Override
          public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
            return false;
          }

          @Override
          public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value, Put put) throws IOException {
            return false;
          }

          @Override
          public void delete(Delete delete) throws IOException {

          }

          @Override
          public void delete(List\<Delete\> deletes) throws IOException {

          }

          @Override
          public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
            return false;
          }

          @Override
          public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
            return false;
          }

          @Override
          public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value, Delete delete) throws IOException {
            return false;
          }

          @Override
          public void mutateRow(RowMutations rm) throws IOException {

          }

          @Override
          public Result append(Append append) throws IOException {
            return null;
          }

          @Override
          public Result increment(Increment increment) throws IOException {
            return null;
          }

          @Override
          public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
            return 0;
          }

          @Override
          public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
            return 0;
          }

          @Override
          public void close() throws IOException {

          }

          @Override
          public CoprocessorRpcChannel coprocessorService(byte[] row) {
            return null;
          }

          @Override
          public \<T extends Service, R\> Map\<byte[], R\> coprocessorService(Class\<T\> service, byte[] startKey, byte[] endKey, Batch.Call\<T, R\> callable) throws ServiceException, Throwable {
            return null;
          }

          @Override
          public \<T extends Service, R\> void coprocessorService(Class\<T\> service, byte[] startKey, byte[] endKey, Batch.Call\<T, R\> callable, Batch.Callback\<R\> callback) throws ServiceException, Throwable {

          }

          @Override
          public \<R extends Message\> Map\<byte[], R\> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
            return null;
          }

          @Override
          public \<R extends Message\> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback\<R\> callback) throws ServiceException, Throwable {

          }

          @Override
          public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
            return false;
          }

          @Override
          public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value, RowMutations mutation) throws IOException {
            return false;
          }

          @Override
          public void setOperationTimeout(int operationTimeout) {

          }

          @Override
          public int getOperationTimeout() {
            return 0;
          }

          @Override
          public int getRpcTimeout() {
            return 0;
          }

          @Override
          public void setRpcTimeout(int rpcTimeout) {

          }

          @Override
          public int getReadRpcTimeout() {
            return 0;
          }

          @Override
          public void setReadRpcTimeout(int readRpcTimeout) {

          }

          @Override
          public int getWriteRpcTimeout() {
            return 0;
          }

          @Override
          public void setWriteRpcTimeout(int writeRpcTimeout) {

          }
        };
      }
    };
  }
}
{code}


---

* [HBASE-18873](https://issues.apache.org/jira/browse/HBASE-18873) | *Critical* | **Hide protobufs in GlobalQuotaSettings**

GlobalQuotaSettings was introduced to avoid protocol-specific Java classes from leaking into API which is users may leverage. This class has a number of methods which return plain-Java-objects instead of these protocol-specific classes in an effort to better provide stability in the future.


---

* [HBASE-18893](https://issues.apache.org/jira/browse/HBASE-18893) | *Major* | **Remove Add/Modify/DeleteColumnFamilyProcedure in favor of using ModifyTableProcedure**

The RPC calls for Add/Modify/DeleteColumn have been removed and are now backed by ModifyTable functionality. The corresponding permissions in AccessController have been removed as well.

The shell already bypassed these RPCs and used ModifyTable directly, and thus would not be getting these permission checks, this change brings the rest of the RPC inline with that.

Coprocessor hooks for pre/post Add/Modify/DeleteColumn have likewise been removed. Coprocessors needing to take special actions on schema change should instead process ModifyTable events (which they should have been doing already, but it was easy for developers to miss this nuance).


---

* [HBASE-16338](https://issues.apache.org/jira/browse/HBASE-16338) | *Major* | **update jackson to 2.y**

HBase has upgraded from Jackson 1 to Jackson 2. JSON output should not have changed and this should not be user facing, but server classpaths should be adjusted accordingly.


---

* [HBASE-19051](https://issues.apache.org/jira/browse/HBASE-19051) | *Minor* | **Add new split algorithm for num string**

Add new split algorithm DecimalStringSplit，row are decimal-encoded long values in the range "00000000" =\> "99999999" .
create 't1','f', { NUMREGIONS =\> 10 , SPLITALGO =\> 'DecimalStringSplit' }
The split point will be 10000000,20000000,...,90000000


---

* [HBASE-19067](https://issues.apache.org/jira/browse/HBASE-19067) | *Major* | **Do not expose getHDFSBlockDistribution in StoreFile**

Removed CP exposed StoreFile#getHDFSBlockDistribution


---

* [HBASE-18989](https://issues.apache.org/jira/browse/HBASE-18989) | *Major* | **Polish the compaction related CP hooks**

Add two new methods in CompactionLifeCycleTracker.
The notExecuted method will be called if the selectCompaction failed or space quota limitation reached.
The completed method will be called after all the requested compactions are finished. The compaction scheduling is pre Store so if you request compaction on a region it may lead to multiple compactions.
Remove the User parameter in Region.requestCompaction methods as it is useless for CP users.
Add a boolean parameter to indicate whether you want to do a major compaction. And so that the triggerMajorCompaction method is removed.
Remove the getCompactionProgress method in Store interface.
Add a UT to confirm that CompactionLifeCycleTracker works correctly, and it also shows how to use CompactionLifeCycleTracker to wait for the completion of a compaction.


---

* [HBASE-19046](https://issues.apache.org/jira/browse/HBASE-19046) | *Major* | **RegionObserver#postCompactSelection  Avoid passing shaded ImmutableList param**

RegionObserver#postCompactSelection signature is changed.
Arg type org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList is replaced with java.util.List


---

* [HBASE-19043](https://issues.apache.org/jira/browse/HBASE-19043) | *Major* | **Purge TableWrapper and CoprocessorHConnnection**

Removes getTable from the CoprocessorEnvrionment Interface and from the BaseEnvironment implementation. Also removes TableWrapper and CoprocessorHConnection, two classes that were used by BaseEnvironment to keep a tag on Tables created by Coprocessors that BaseEnvironment might close them out on #shutdown.

Long after these classes and methods were added, in HBase 1.0.0, we moved to a mode where management of Tables was shifted from HBase to the Client; the Client is to manage lifecycle. Table also became a (relatively) lightweight construct so folks are used to getting a Table instance, using it, and then immediately closing it when done.

Coprocessors should do the same in hbase2.0.0.

CoprocessorHConnection short-circuited RPC. This feature has since been integrated into Server Connections; when they create a Connection, they get one that will short-circuit if the request is to a localhost so no need of CoprocessorHConnection any more.

Coprocessors get the Server Connection when they ask for a Connection from their \*CoprocessorEnvironment.


---

* [HBASE-19014](https://issues.apache.org/jira/browse/HBASE-19014) | *Major* | **surefire fails; When writing xml report stdout/stderr ... No such file or directory**

Running tests with a wildcard selector, i.e.{{-Dtest=org.apache.hadoop.hbase.server.\*}} no longer works.


---

* [HBASE-10367](https://issues.apache.org/jira/browse/HBASE-10367) | *Major* | **RegionServer graceful stop / decommissioning**

Added three top level Admin APIs to help decommissioning and graceful stop of region servers.

  /\*\*
   \* Mark region server(s) as decommissioned to prevent additional regions from getting
   \* assigned to them. Optionally unload the regions on the servers. If there are multiple servers
   \* to be decommissioned, decommissioning them at the same time can prevent wasteful region
   \* movements. Region unloading is asynchronous.
   \* @param servers The list of servers to decommission.
   \* @param offload True to offload the regions from the decommissioned servers
   \*/
  void decommissionRegionServers(List\<ServerName\> servers, boolean offload) throws IOException;

  /\*\*
   \* List region servers marked as decommissioned, which can not be assigned regions.
   \* @return List of decommissioned region servers.
   \*/
  List\<ServerName\> listDecommissionedRegionServers() throws IOException;

  /\*\*
   \* Remove decommission marker from a region server to allow regions assignments.
   \* Load regions onto the server if a list of regions is given. Region loading is
   \* asynchronous.
   \* @param server The server to recommission.
   \* @param encodedRegionNames Regions to load onto the server.
   \*/
  void recommissionRegionServer(ServerName server, List\<byte[]\> encodedRegionNames)  throws IOException;


---

* [HBASE-19042](https://issues.apache.org/jira/browse/HBASE-19042) | *Blocker* | **Oracle Java 8u144 downloader broken in precommit check**

Precommit switched from Oracle JDK 8 to OpenJDK-8.


---

* [HBASE-18945](https://issues.apache.org/jira/browse/HBASE-18945) | *Major* | **Make a IA.LimitedPrivate interface for CellComparator**

CellCompartor has been added as an interface with IA.LimitedPrivate. It has the following methods
#int compare(Cell leftCell, Cell rightCell);
#int compareRows(Cell leftCell, Cell rightCell)
#int compareRows(Cell cell, byte[] bytes, int offset, int length)
#int compareWithoutRow(Cell leftCell, Cell rightCell)
#int compareFamilies(Cell leftCell, Cell rightCell
#int compareQualifiers(Cell leftCell, Cell rightCell)
#int compareTimestamps(Cell leftCell, Cell rightCell)
#int compareTimestamps(long leftCellts, long rightCellts)

This is exposed to CPs and CPs can make use of the above methods to do comparisons on the cells.
For internal usage we have CellComparatorImpl and it has static references to COMPARATOR and META\_CELL\_COMPARATOR.
So when a region or store is initialized we should use one of the above comparator. For META table we need the META\_CELL\_COMPARATOR and all other table's  regions/stores will use the COMPARTOR.
While writing the comparator name in FixedFileTrailer of the Hfile we have now ensured that this rename of CellComparator.COMPARATOR/CellComparator.META\_CELL\_COMPARATOR to CellComparatorImpl.COMPARATOR/CellComparatorImpl.META\_CELL\_COMPARATOR is handled.

CellUtils is an util method that provides lot of APIs that helps to do compare, matching functionalities between two cells, or with a cell and a corrpesponding byte[] etc. Some of the APIs are internally used which will be cleaned up in a follow on JIRA HBASE-18995.


---

* [HBASE-19001](https://issues.apache.org/jira/browse/HBASE-19001) | *Major* | **Remove the hooks in RegionObserver which are designed to construct a StoreScanner which is marked as IA.Private**

These methods are removed:
KeyValueScanner preStoreScannerOpen(ObserverContext\<RegionCoprocessorEnvironment\> c,
      Store store, Scan scan, NavigableSet\<byte[]\> targetCols, KeyValueScanner s, long readPt)
      throws IOException;
InternalScanner preFlushScannerOpen(ObserverContext\<RegionCoprocessorEnvironment\> c,
      Store store, List\<KeyValueScanner\> scanners, InternalScanner s, long readPoint)
      throws IOException;
InternalScanner preCompactScannerOpen(ObserverContext\<RegionCoprocessorEnvironment\> c,
      Store store, List\<? extends KeyValueScanner\> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s, CompactionLifeCycleTracker tracker, CompactionRequest request,
      long readPoint) throws IOException;

For flush and compaction, CP users are expected to wrap the InternalScanner in preFlush/preCompact. And for normal region operation, just use preGetOp/preScannerOpen to modify the Get/Scan object.

This method in Region interface is also removed as we do not need to use read point in CP hooks anymore:
long getReadPoint(IsolationLevel isolationLevel);


---

* [HBASE-18350](https://issues.apache.org/jira/browse/HBASE-18350) | *Blocker* | **RSGroups are broken under AMv2**

Moves RSGroup on to AMv2. Reenables disabled RSGroups tests.


---

* [HBASE-18960](https://issues.apache.org/jira/browse/HBASE-18960) | *Major* | **A few bug fixes and minor improvements around batchMutate()**

All operations for which further processing is skipped by preBatchMutate coprocessor hook are treated as SUCCESS instead of FAILED.


---

* [HBASE-14247](https://issues.apache.org/jira/browse/HBASE-14247) | *Critical* | **Separate the old WALs into different regionserver directories**

Add a new config hbase.separate.oldlogdir.by.regionserver. The default value is false. If this config is true, the old wal dir will be separated by regionservers. This will change the oldWALs layout. The oldWALs is used by replication. So if a cluster didn't use replication, it can be rolling upgrade (upgrade this config from false to true) directly. If a cluster use replication, the oldWALs will be not found when layout changed. So the cluster need rolling upgrade twice. Firstly, only rolling cluster to use new version code. Secondly rolling the config from false to true. Because the cluster already rolling to new version code, so it can find the oldWALs in the new dir layout.


---

* [HBASE-18954](https://issues.apache.org/jira/browse/HBASE-18954) | *Major* | **Make \*CoprocessorHost classes private**

- Make CoprocessorHost and its implementations InterfaceAudience.Private
- Configurations from "CoprocessorHost" have been moved to new "CoprocessorConfigurations" class.


---

* [HBASE-15410](https://issues.apache.org/jira/browse/HBASE-15410) | *Major* | **Utilize the max seek value when all Filters in MUST\_PASS\_ALL FilterList return SEEK\_NEXT\_USING\_HINT**

This optimization, targeting SEEK\_NEXT\_USING\_HINT return values, utilizes the max seek value and is transparent to Filters.


---

* [HBASE-18747](https://issues.apache.org/jira/browse/HBASE-18747) | *Critical* | **Introduce new example and helper classes to tell CP users how to do filtering on scanners**

Modify ZooKeeperScanPolicyObserver in hbase-examples to show how to do filtering in the CP hooks of flush and compaction in hbase-2.0.


---

* [HBASE-18108](https://issues.apache.org/jira/browse/HBASE-18108) | *Blocker* | **Procedure WALs are archived but not cleaned; fix**

The archived Procedure WALs are moved to \<hbase\_root\>/oldWALs/masterProcedureWALs
directory. TimeToLiveProcedureWALCleaner class was added which regularly cleans the Procedure WAL files from there.

The TimeToLiveProcedureWALCleaner is added to hbase.master.logcleaner.plugins configuration value.

A new config parameter is added: hbase.master.procedurewalcleaner.ttl, which specifies how long a Procedure WAL should stay in the archive directory.


---

* [HBASE-18183](https://issues.apache.org/jira/browse/HBASE-18183) | *Major* | **Region interface cleanup for CP expose**

Below methods are removed from CP exposed Region interface
getOpenSeqNum
getOldestSeqIdOfStore
isLoadingCfsOnDemandDefault
getReadpoint
updateReadRequestsCount
updateWriteRequestsCount
getRegionServicesForStores
getMetrics
getHDFSBlocksDistribution
releaseRowLocks
batchReplay
get(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
bulkLoadHFiles
execService
registerService
checkFamilies
checkTimestamps
prepareDelete
prepareDeleteTimestamps
updateCellTimestamps
flush
compact
waitForFlushesAndCompactions
waitForFlushes

Change signature of below methods by dropping params 'nonceGroup', 'nonce'
append(Append append, long nonceGroup, long nonce)
batchMutate(Mutation[] mutations, long nonceGroup, long nonce)
increment(Increment increment, long nonceGroup, long nonce)


---

* [HBASE-18949](https://issues.apache.org/jira/browse/HBASE-18949) | *Major* | **Remove the CompactionRequest parameter in preCompactSelection**

Remove the CompactionRequest parameter in preCompactSelection as we do not have a CompactionRequest at that time.


---

* [HBASE-18909](https://issues.apache.org/jira/browse/HBASE-18909) | *Major* | **Deprecate Admin's methods which used String regex**

Pushed to master and branch-2. Thanks all for reviewing.


---

* [HBASE-18931](https://issues.apache.org/jira/browse/HBASE-18931) | *Major* | **Make ObserverContext an interface and remove private/testing methods**

Changes ObserverContext from a class to an interface and hides away constructor, testing functions and other internal-only functions in the implementation class.


---

* [HBASE-18878](https://issues.apache.org/jira/browse/HBASE-18878) | *Major* | **Use Optional\<T\> return types when T can be null**

**WARNING: No release note provided for this change.**


---

* [HBASE-18649](https://issues.apache.org/jira/browse/HBASE-18649) | *Major* | **Deprecate KV Usage in MR to move to Cells in 3.0**

All the mappers and reducers output type will be now of MapReduceCell type. No more KeyValue type. How ever in branch-2 for compatibility we have allowed the older interfaces/classes that work with KeyValue to stay in the code base but they have been marked as deprecated.
The following interfaces/classes have been deprecated in branch-2
Import#KeyValueWritableComparablePartitioner
Import#KeyValueWritableComparator
Import#KeyValueWritableComparable
Import#KeyValueReducer
Import#KeyValueSortImporter
Import#KeyValueImporter
KeyValueSortReducer
KeyValueSerialization
WALPlayer#WALKeyValueMapper

So any existing MR jobs that is using the above public interfaces/classes will continue to work in branch-2 and the expected output value type of those mappers and reducers can continue to be KeyValue type.

In branch-3 the mappers and reducers output will only expect MapReduceCell as the type and will no longer work with KeyValue type.
The new public classes/interfaces added for branch-3 and in branch-2 are
CellSerialization
CellSortReducer
Import#CellWritableComparablePartitioner
Import#CellWritableComparable
Import#CellWritableComparator
Import#CellReducer
Import#CellSortImporter
Import#CellImporter
WALPlayer#WALCellMapper


---

* [HBASE-18897](https://issues.apache.org/jira/browse/HBASE-18897) | *Major* | **Substitute MemStore for Memstore**

The changes of IA.Public/IA.LimitedPrivate classes are shown below:
HTableDescriptor class
\* boolean hasRegionMemstoreReplication()
+ boolean hasRegionMemStoreReplication()
\* HTableDescriptor setRegionMemstoreReplication(boolean)
+ HTableDescriptor setRegionMemStoreReplication(boolean)

RegionLoadStats class
\* int getMemstoreLoad()
+ int getMemStoreLoad()

ServerLoad class
\* int getMemstoreSizeInMB()
+ int getMemStoreSizeMB()

Region class
- long getMemstoreSize()
+ long getMemStoreSize()

Store class
- MemstoreSize getMemStoreSize()
+ MemStoreSize getMemStoreSize()
- MemstoreSize getFlushableSize()
+ MemStoreSize getFlushableSize()
- MemstoreSize getSnapshotSize()
+ MemStoreSize getSnapshotSize()

StoreFile class
- long getMaxMemstoreTS()
+ long getMaxMemStoreTS()


---

* [HBASE-18010](https://issues.apache.org/jira/browse/HBASE-18010) | *Major* | **Connect CellChunkMap to be used for flattening in CompactingMemStore**

The CellChunkMap is very dense index for Memstore ImmutableSegment and the only one that can be taken off-heap. However, CellChunkMap works on-heap as well. The coding of the entire flow of working with CellChunkMap is not yet finished, thus CellChunkMap is disabled for usage so far. The continuation is done under HBASE-18232.


---

* [HBASE-18883](https://issues.apache.org/jira/browse/HBASE-18883) | *Major* | **Upgrade to Curator 4.0**

Curator version has been updated from 2.x to 4.0 (running in ZK 3.4 compatibility mode).

Users who experience classpath issues due to version conflicts are recommended to use either the hbase-shaded-client or hbase-shaded-mapreduce artifacts.


---

* [HBASE-13844](https://issues.apache.org/jira/browse/HBASE-13844) | *Minor* | **Move static helper methods from KeyValue into CellUtils**

Move KeyValue.parseColumn() to CellUtil


---

* [HBASE-18839](https://issues.apache.org/jira/browse/HBASE-18839) | *Major* | **Apply RegionInfo to code base**

The incompatible changes of IA.Public/LimitedPrivate classes are shown below.
+ new method
- removed method
\* deprecated method
-------------------------------------
HRegionLocation class
+ RegionInfo getRegion()
\* HRegionInfo getRegionInfo()

AsyncAdmin class
+ CompletableFuture\<List\<RegionInfo\>\> getOnlineRegions(ServerName serverName);
- CompletableFuture\<List\<HRegionInfo\>\> getOnlineRegions(ServerName serverName);
+ CompletableFuture\<List\<RegionInfo\>\> getTableRegions(TableName tableName);
- CompletableFuture\<List\<HRegionInfo\>\> getTableRegions(TableName tableName);

HBaseTestingUtility class
- Table createTable(HTableDescriptor htd, byte[][] families, Configuration c)
- Table createTable(HTableDescriptor htd, byte[][] families, byte[][] splitKeys, Configuration c)
- Table createTable(HTableDescriptor htd, byte[][] splitRows)
- void modifyTableSync(Admin admin, HTableDescriptor desc)
- HRegion createLocalHRegion(HTableDescriptor desc, byte [] startKey, byte [] endKey)
- HRegion createLocalHRegion(HRegionInfo info, HTableDescriptor desc)
- HRegion createLocalHRegion(HRegionInfo info, TableDescriptor desc)
+ HRegion createLocalHRegion(RegionInfo info, TableDescriptor desc)
- HRegion createLocalHRegion(HRegionInfo info, HTableDescriptor desc, WAL wal)
- HRegion createLocalHRegion(HRegionInfo info, TableDescriptor desc, WAL wal)
+ HRegion createLocalHRegion(RegionInfo info, TableDescriptor desc, WAL wal)
- List\<HRegionInfo\> createMultiRegionsInMeta(final Configuration conf,final TableDescriptor htd, byte [][] startKeys)
+ List\<HRegionInfo\> createMultiRegionsInMeta(final Configuration conf,final TableDescriptor htd, byte [][] startKeys)
- WAL createWal(final Configuration conf, final Path rootDir, final HRegionInfo hri)
+ WAL createWal(final Configuration conf, final Path rootDir, final RegionInfo hri)
- HRegion createRegionAndWAL(final HRegionInfo info, final Path rootDir,final Configuration conf, final HTableDescriptor htd)
- HRegion createRegionAndWAL(final HRegionInfo info, final Path rootDir, final Configuration conf, final TableDescriptor htd)
+ HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir, final Configuration conf, final TableDescriptor htd)
- HRegion createRegionAndWAL(final HRegionInfo info, final Path rootDir, final Configuration conf, final HTableDescriptor htd, boolean initialize)
+ HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir, final Configuration conf, final HTableDescriptor htd, boolean initialize)
- boolean assignRegion(final HRegionInfo regionInfo)
+ boolean assignRegion(final RegionInfo regionInfo)
- void moveRegionAndWait(HRegionInfo destRegion, ServerName destServer)
+ void moveRegionAndWait(RegionInfo destRegion, ServerName destServer)
- int createPreSplitLoadTestTable(Configuration conf, HTableDescriptor desc, HColumnDescriptor hcd)
- int createPreSplitLoadTestTable(Configuration conf, HTableDescriptor desc, HColumnDescriptor hcd, int numRegionsPerServer)
- int createPreSplitLoadTestTable(Configuration conf, HTableDescriptor desc, HColumnDescriptor[] hcds, int numRegionsPerServer)
- HRegion createTestRegion(String tableName, HColumnDescriptor cd)

WALEdit class
- WALEdit createFlushWALEdit(HRegionInfo hri, FlushDescriptor f)
+ WALEdit createFlushWALEdit(RegionInfo hri, FlushDescriptor f)
- WALEdit createRegionEventWALEdit(HRegionInfo hri,RegionEventDescriptor regionEventDesc)
+ WALEdit createRegionEventWALEdit(RegionInfo hri,RegionEventDescriptor regionEventDesc)
- WALEdit createCompaction(final HRegionInfo hri, final CompactionDescriptor c)
+ WALEdit createCompaction(final RegionInfo hri, final CompactionDescriptor c)
- byte[] getRowForRegion(HRegionInfo hri)
+ byte[] getRowForRegion(RegionInfo hri)
- WALEdit createBulkLoadEvent(HRegionInfo hri, WALProtos.BulkLoadDescriptor bulkLoadDescriptor)
+ - WALEdit createBulkLoadEvent(RegionInfo hri, WALProtos.BulkLoadDescriptor bulkLoadDescriptor)

RegionScanner class
- HRegionInfo getRegionInfo();
+ RegionInfo getRegionInfo();

RegionPlan class
- RegionPlan(final HRegionInfo hri, ServerName source, ServerName dest)
+ RegionPlan(final RegionInfo hri, ServerName source, ServerName dest)

Region class
- HRegionInfo getRegionInfo();
+ RegionInfo getRegionInfo();

TableSnapshotInputFormat.TableSnapshotRegionSplit class
\* HRegionInfo getRegionInfo()
+ RegionInfo getRegion()

RawAsyncTable.CoprocessorCallback class
- void onRegionComplete(HRegionInfo region, R resp)
+ void onRegionComplete(RegionInfo region, R resp)
- void onRegionError(RegionInfo region, Throwable error);
+ void onRegionError(HRegionInfo region, Throwable error);


---

* [HBASE-18826](https://issues.apache.org/jira/browse/HBASE-18826) | *Major* | **Use HStore instead of Store in our own code base and remove unnecessary methods in Store interface**

**WARNING: No release note provided for this change.**


---

* [HBASE-17732](https://issues.apache.org/jira/browse/HBASE-17732) | *Critical* | **Coprocessor Design Improvements**

We are moving from Inheritence
- Observer \*is\* Coprocessor
- FooService \*is\* CoprocessorService
To Composition
- Coprocessor \*has\* Observer
- Coprocessor \*has\* Service
------------------------------------------------------
Summary
------------------------------------------------------
- Adds four new interfaces - MasterCoprocessor, RegionCoprocessor, RegionServierCoprocessor,
  WALCoprocessor
- These new \*Coprocessor interfaces have a get\*Observer() function for each observer type
  supported by them.
- Added Coprocessor#getService() to base interface. All extending \*Coprocessor interfaces will
  get it from the base interface.
- Added BulkLoadObserver hooks to RegionCoprocessorHost instad of SecureBulkLoadManager doing its
  own trickery.
- CoprocessorHost#find\*() fuctions: Too many testing hooks digging into CP internals.
  Deleted if can, else marked @VisibleForTesting.
------------------------------------------------------
Backward Compatibility
------------------------------------------------------
- Old coprocessors implementing \*Observer won't get loaded (no backward compatibility guarantees).
- Third party coprocessors only implementing Coprocessor will not get loaded (just like Observers).
- Old coprocessors implementing CoprocessorService (for master/region host)
  /SingletonCoprocessorService (for RegionServer host) will continue to work with 2.0.
- Added test to ensure backward compatibility of CoprocessorService/SingletonCoprocessorService
- Note that if a coprocessor implements both observer and service in same class, its service
  component will continue to work but it's observer component won't work.


---

* [HBASE-18298](https://issues.apache.org/jira/browse/HBASE-18298) | *Critical* | **RegionServerServices Interface cleanup for CP expose**

We used to pass the RegionServerServices (RSS) which gave Coprocesosrs (CP) all sort of access to internal Server machinery. We now only allows the CP a subset of the RSS in the form of the CPRSS Interface. Particulars:

Removed method getRegionServerServices from CP exposed RegionCoprocessorEnvironment and RegionServerCoprocessorEnvironment and replaced with getCoprocessorRegionServerServices. This returns a new interface CoprocessorRegionServerServices which is only a subset of RegionServerServices. With that below methods are no longer exposed for CPs
WAL getWAL(HRegionInfo regionInfo)
List\<WAL\> getWALs()
FlushRequester getFlushRequester()
RegionServerAccounting getRegionServerAccounting()
RegionServerRpcQuotaManager getRegionServerRpcQuotaManager()
SecureBulkLoadManager getSecureBulkLoadManager()
RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager()
void postOpenDeployTasks(final PostOpenDeployContext context)
void postOpenDeployTasks(final Region r)
boolean reportRegionStateTransition(final RegionStateTransitionContext context)
boolean reportRegionStateTransition(TransitionCode code, long openSeqNum, HRegionInfo... hris)
boolean reportRegionStateTransition(TransitionCode code, HRegionInfo... hris)
RpcServerInterface getRpcServer()
ConcurrentMap\<byte[], Boolean\> getRegionsInTransitionInRS()
Leases getLeases()
ExecutorService getExecutorService()
Map\<String, Region\> getRecoveringRegions()
public ServerNonceManager getNonceManager()
boolean registerService(Service service)
HeapMemoryManager getHeapMemoryManager()
double getCompactionPressure()
ThroughputController getFlushThroughputController()
double getFlushPressure()
MetricsRegionServer getMetrics()
EntityLock regionLock(List\<HRegionInfo\> regionInfos, String description, Abortable abort)
void unassign(byte[] regionName)
Configuration getConfiguration()
ZooKeeperWatcher getZooKeeper()
ClusterConnection getClusterConnection()
MetaTableLocator getMetaTableLocator()
CoordinatedStateManager getCoordinatedStateManager()
ChoreService getChoreService()
void stop(String why)
void abort(String why, Throwable e)
boolean isAborted()
void updateRegionFavoredNodesMapping(String encodedRegionName, List\<ServerName\> favoredNodes)
InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName)
void addToOnlineRegions(Region region)
boolean removeFromOnlineRegions(final Region r, ServerName destination)

Also 3 methods name have been changed
List\<Region\> getOnlineRegions(TableName tableName) -\> List\<Region\> getRegions(TableName tableName)
List\<Region\> getOnlineRegions() -\> List\<Region\> getRegions()
Region getFromOnlineRegions(final String encodedRegionName) -\> Region getRegion(final String encodedRegionName)


---

* [HBASE-16769](https://issues.apache.org/jira/browse/HBASE-16769) | *Blocker* | **Deprecate/remove PB references from MasterObserver and RegionServerObserver**

Signature of below methods in MasterObserver changed and instead of org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotDescription param, we will be passing org.apache.hadoop.hbase.client.SnapshotDescription
preListSnapshot
postListSnapshot
preSnapshot
postSnapshot
preCloneSnapshot
postCloneSnapshot
preRestoreSnapshot
postRestoreSnapshot
preDeleteSnapshot
postDeleteSnapshot

Also changed signature of RegionServerObserver#preReplicateLogEntries and preReplicateLogEntries by removing params List\<org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry\>, org.apache.hadoop.hbase.CellScanner


---

* [HBASE-18859](https://issues.apache.org/jira/browse/HBASE-18859) | *Major* | **Purge PB from BulkLoadObserver**

No longer pass the protobuf request to prePrepareBulkLoad and preCleanupBulkLoad in BulkLoadObserver as part of our effort to purge protobuf from our Coprocessor API Interface (if you need to read the Table and RegionInfo, pull it from the passed in RegionCoprocessorEnvironment ObserverContext).


---

* [HBASE-18731](https://issues.apache.org/jira/browse/HBASE-18731) | *Major* | **[compat 1-2] Mark protected methods of QuotaSettings that touch Protobuf internals as IA.Private**

The following methods in QuotaSettings were annotated InterfaceAudience.Private; they are for internal use only in hbase-2.0.0

buildSetQuotaRequestProto(final QuotaSettings settings)
setupSetQuotaRequest(SetQuotaRequest.Builder builder)

Note that there were versions of these methods in HBase 1.y that used classes in the {{org.apache.hadoop.hbase.protobuf.generated}} package. That package no longer exists as a part of our cleanup of protobufs from our public facing API and the related methods have been removed.


---

* [HBASE-18825](https://issues.apache.org/jira/browse/HBASE-18825) | *Major* | **Use HStoreFile instead of StoreFile in our own code base and remove unnecessary methods in StoreFile interface**

Cleanup the StoreFile interface.

The metadata keys are moved to HStoreFile.

These methods are removed:
CacheConfig getCacheConf();
byte[] getMetadataValue(byte[] key);
boolean isCompactedAway();
boolean isReferencedInReads();
void initReader() throws IOException;
StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn);
StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks, boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) throws IOException;
StoreFileReader getReader();
void closeReader(boolean evictOnClose) throws IOException;
void markCompactedAway();
void deleteReader() throws IOException;

Notice that these methods are still available in HStoreFile.

And the return value of getFirstKey and getLastKey are changed from Cell to Optional\<Cell\> to better indicate that they may not be available.


---

* [HBASE-18786](https://issues.apache.org/jira/browse/HBASE-18786) | *Major* | **FileNotFoundException should not be silently handled for primary region replicas**

FileNotFoundException opening a StoreFile in a primary replica now causes a RegionServer to crash out where before it would be ignored (or optionally handled via close/reopen).


---

* [HBASE-10504](https://issues.apache.org/jira/browse/HBASE-10504) | *Blocker* | **Define Replication Interface**

Adds a new plugin point ReplicationEndpoint. ReplicationSource, internal to hbase, tails the WAL and calls registered ReplicationEndpoints. ReplicationEndpoint implementations are responsible for actually shipping the edits to the other (hbase or non-hbase) cluster. ReplicationEndpoint can be defined per peer. Default inter-cluster replication works without any changes (lily etc should still work). ReplicationEndpoints have various facility including means for filtering out WAL edits source-side before they can be shipped to remote peers.


---

* [HBASE-18142](https://issues.apache.org/jira/browse/HBASE-18142) | *Major* | **Deletion of a cell deletes the previous versions too**

Now, delete.rb won't delete all versions of the specified column. It only delete the specified version (if user assigns a timestamp) or the latest version (default behavior)


---

* [HBASE-18446](https://issues.apache.org/jira/browse/HBASE-18446) | *Critical* | **Mark StoreFileScanner/StoreFileReader as IA.LimitedPrivate(Phoenix)**

Mark StoreFileScanner and StoreFileReader as IA.LimitPrivate(Phoenix).
Deprecated the preStoreFileReaderOpen and postStoreFileReaderOpen method in RegionObserver to indicate that these methods are only supposed to be used by Phoenix.


---

* [HBASE-18798](https://issues.apache.org/jira/browse/HBASE-18798) | *Major* | **Remove the unused methods in RegionServerObserver**

Remove the following APIs from RegionServerObserver:
# preRollBackMerge
# postRollBackMerge
# preMergeCommit
# postMergeCommit
# postMerge
# preMerge


---

* [HBASE-18831](https://issues.apache.org/jira/browse/HBASE-18831) | *Major* | **Add explicit dependency on javax.el**

Specify an explicit version for javax.el. Without it we rely on repository cached metadata of which a prevalent version seems to list all versions between b01 and b08 but finishes with a b08-jbossorg which is in the jboss repo, a repo most of us do not list in our poms.


---

* [HBASE-17980](https://issues.apache.org/jira/browse/HBASE-17980) | *Major* | **Any HRegionInfo we give out should be immutable**

Provide alternate user-facing API that takes a RegionInfo Interface instead of a HRegionInfo; the old HRegionInfo methods have been deprecated in 2.0.0 and will be removed in 3.0.0.


---

* [HBASE-14004](https://issues.apache.org/jira/browse/HBASE-14004) | *Critical* | **[Replication] Inconsistency between Memstore and WAL may result in data in remote cluster that is not in the origin**

Now when replicating a wal file which is still opened for write, we will get its committed length from the WAL instance in the same RS to prevent replicating uncommit WALEdit.

This is very important if you use AsyncFSWAL, as we use fan-out in AsyncFSWAL. The data written to DN will be visible immediately as all DNs think it is the end of a pipeline, although the client has not received an ack, and also NN may truncate the file if the client crashes at the same time.


---

* [HBASE-18819](https://issues.apache.org/jira/browse/HBASE-18819) | *Major* | **Set version number to 2.0.0-alpha3 from 2.0.0-alpha3-SNAPSHOT**

Set version on branch-2 to be 2.0.0-alpha3 as part of RC making.


---

* [HBASE-18683](https://issues.apache.org/jira/browse/HBASE-18683) | *Major* | **Upgrade hbase to commons-math 3**

Moved on to commons-math3. Removed commons-math2.


---

* [HBASE-18453](https://issues.apache.org/jira/browse/HBASE-18453) | *Major* | **CompactionRequest should not be exposed to user directly**

Introduce a CompactionLifeCycleTracker to let the CP users know when the compaction starts and ends. CompactionRequest is marked as IA.Private and should be used in CP implementation any more.


---

* [HBASE-18794](https://issues.apache.org/jira/browse/HBASE-18794) | *Major* | **Remove deprecated methods in MasterObserver**

The removed APIs are shown below.
# preCreateTableHandler
# postCreateTableHandler
# preDeleteTableHandler
# postDeleteTableHandler
# preTruncateTableHandler
# postTruncateTableHandler
# preModifyTableHandler
# postModifyTableHandler
# preAddColumn
# postAddColumn
# preAddColumnHandler
# postAddColumnHandler
# preModifyColumn
# postModifyColumn
# preModifyColumnHandler
# postModifyColumnHandler
# preDeleteColumn
# postDeleteColumn
# preDeleteColumnHandler
# postDeleteColumnHandler
# preEnableTableHandler
# postEnableTableHandler
# preDisableTableHandler
# postDisableTableHandler
# preDispatchMerge
# postDispatchMerge


---

* [HBASE-14998](https://issues.apache.org/jira/browse/HBASE-14998) | *Blocker* | **Unify synchronous and asynchronous methods in Admin and cleanup**

 \* Deprecates getAlterStatus. Everywhere else we talk of 'modify' rather
       'alter' and should use Future returned from async instead.
 \* isTableAvailable(TableName, byte [][]) has been deprecated to be
       removed; use the overrie instead. This is a weird method.
 \* Changed listTableDescriptor to getDescriptor.
 \* Renamed other like methods to have same pattern (deprecating the old):
        balancer =\> balance
        setBalancerRunning =\> balancerSwitch
        setNormalizerRunning =\> normalizerSwitch
        enableCatalogJanitor =\> catalogJanitorSwitch
        setCleanerChoreRunning =\> cleanerChoreSwitch
        setSplitOrMergeEnabled =\> splitOrMergeEnabledSwitch

 \* Renamed (with deprecation of old) runCatalogScan =\> runCatalogJanitor.
 \* Reviewed generated javadoc and made some edits; purged reference to
       hbase issues from our API, fixed param names, etc.
 \* Made all the enable services methods have same pattern.
 \* Renamed takeSnapshotAsync as snapshotAsync (with deprecation of old)
 \* Renamed execProcedureWithRet as execProcedureWithReturn (with
       deprecation)


---

* [HBASE-18723](https://issues.apache.org/jira/browse/HBASE-18723) | *Major* | **[pom cleanup] Do a pass with dependency:analyze; remove unused and explicity list the dependencies we exploit**

Purged a bunch of dependencies included but unused. Added reference to dependencies we do use but did not list (transitively included). Purged all but junit from parent pom dependency set and did explicit include in modules instead; not all modules need mockito, etc. Still work to do: grey area around hadoop and its transitive includes need cleanup still to make the  dependency:analyze runs clean. Also figure how to purge junit from parent dependency list.


---

* [HBASE-17823](https://issues.apache.org/jira/browse/HBASE-17823) | *Major* | **Migrate to Apache Yetus Audience Annotations**

HBase now uses stability and audience annotations sourced from Apache Yetus, instead of the custom annotations that were previously in place.


---

* [HBASE-18793](https://issues.apache.org/jira/browse/HBASE-18793) | *Major* | **Remove deprecated methods in RegionObserver**

These deprecated methods are removed from RegionObserver:
InternalScanner preFlushScannerOpen(ObserverContext, Store, List, InternalScanner) throws IOException;
void preCompactSelection(ObserverContext, Store, List) throws IOException;
void postCompactSelection(ObserverContext, Store, ImmutableList);
InternalScanner preCompact(ObserverContext, Store, InternalScanner, ScanType) throws IOException;
InternalScanner preCompactScannerOpen(ObserverContext, Store, List, ScanType, long, InternalScanner, CompactionRequest) throws IOException;
InternalScanner preCompactScannerOpen( ObserverContext, Store store, List, ScanType, long, InternalScanner) throws IOException;
void preSplit(ObserverContext) throws IOException;
void preSplit(ObserverContext, byte[]) throws IOException;
void postSplit(ObserverContext, Region, Region) throws IOException;
void preSplitBeforePONR(ObserverContext, byte[], List) throws IOException;
void preSplitAfterPONR(ObserverContext) throws IOException;
void preRollBackSplit(ObserverContext) throws IOException;
void postRollBackSplit(ObserverContext) throws IOException;
void postCompleteSplit(ObserverContext) throws IOException;
long preIncrementColumnValue(ObserverContext, byte[], byte[], byte[], long, boolean) throws IOException;
long postIncrementColumnValue(ObserverContextc, byte[], byte[], byte[], long, boolean, long) throws IOException;
KeyValueScanner preStoreScannerOpen(ObserverContext, Store, Scan, NavigableSet, KeyValueScanner) throws IOException;
boolean postScannerFilterRow(ObserverContext, InternalScanner, byte[], int, short, boolean) throws IOException;
boolean postBulkLoadHFile(ObserverContext, List, boolean) throws IOException;

And this method is also removed since we never call it in our code base:
InternalScanner preFlushScannerOpen(ObserverContext, Store, KeyValueScanner, InternalScanner, long) throws IOException;

The deprecated annotation is removed for these two methods as they are still being used:
void preFlush(ObserverContext) throws IOException;
void postFlush(ObserverContextc) throws IOException;


---

* [HBASE-18733](https://issues.apache.org/jira/browse/HBASE-18733) | *Major* | **[compat 1-2] Hide WALKey**

WALKey, @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION), changed a bunch for 2.0.0. See below. We figured it ok hiding it since it should be internals anyway -- only we should be making them.


---

* [HBASE-13271](https://issues.apache.org/jira/browse/HBASE-13271) | *Critical* | **Table#puts(List\<Put\>) operation is indeterminate; needs fixing**

Adds more spec on how Get, Delete, and Put work and how they differ to help the user.


---

* [HBASE-16479](https://issues.apache.org/jira/browse/HBASE-16479) | *Major* | **Move WALEdit from hbase.regionserver.wal package to hbase.wal package**

Incompatible move of WALEdit class from regionserver.wal to wal. Effects @InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION,
    HBaseInterfaceAudience.COPROC })

(


---

* [HBASE-10240](https://issues.apache.org/jira/browse/HBASE-10240) | *Critical* | **Remove 0.94-\>0.96 migration code**

Purge 0.94=\>0.96 deprecated, migration code. This means that if you are on 0.94 and wish to go to hbase 2.0, you must first migrate to a version of hbase that is \>= 0.96.


---

* [HBASE-18783](https://issues.apache.org/jira/browse/HBASE-18783) | *Minor* | **Declare the builder of ClusterStatus as IA.Private, and remove the Writables from ClusterStatus**

**WARNING: No release note provided for this change.**


---

* [HBASE-18106](https://issues.apache.org/jira/browse/HBASE-18106) | *Critical* | **Redo ProcedureInfo and LockInfo**

Admin.listProcedures and Admin.listLocks were renamed to getProcedures and getLocks (listProcedures was added to hbase 1.2). This change was done in an incompatible way -- we just yanked listProcedures (Because Admin Interface is not compatible with hbase1).

    Main changes:
    - ProcedureInfo and LockInfo were removed, we use JSON instead of them
    - Procedure and LockedResource are their server side equivalent
    - Procedure protobuf state\_data became obsolate, it is only kept for
      reading previously written WAL
    - Procedure protobuf contains a state\_message field, which stores the internal
      state messages (Any type instead of bytes)
    - Procedure.serializeStateData and deserializeStateData were changed slightly
    - Procedures internal states are available on client side
    - Procedures are displayed on web UI and in shell in the following jruby format:
      { ID =\> '1', PARENT\_ID = '-1', PARAMETERS =\> [ ..extra state information.. ] }


---

* [HBASE-18621](https://issues.apache.org/jira/browse/HBASE-18621) | *Major* | **Refactor ClusterOptions before applying to code base**

Provide a new way to get desired ClusterStatus with a set of ClusterStatus.Option, such that the response back to client can be limited.
Note that, the constructor way to new a ClusterStatus will be no longer support after 2.0.0,  and use ClusterStatus.Builder instead.


---

* [HBASE-18780](https://issues.apache.org/jira/browse/HBASE-18780) | *Minor* | **Remove HLogPrettyPrinter and hlog command**

**WARNING: No release note provided for this change.**


---

* [HBASE-14997](https://issues.apache.org/jira/browse/HBASE-14997) | *Critical* | **Move compareOp and Comparators out of filter to client package**

Deprecate checkAnd\* APIs that take the filter CompareOp. Added new overrides that take a generic CompareOperator instead. CompareOperator will be used by checkAnd\* in Table API and by filters going forward.

Other nice improvements suggested by this issue have been moved out to HBASE-18774.


---

* [HBASE-17972](https://issues.apache.org/jira/browse/HBASE-17972) | *Minor* | **Remove mergePool from CompactSplitThread**

After this jira, mergePool will be permanently removed from CompactSplitThread.


---

* [HBASE-18704](https://issues.apache.org/jira/browse/HBASE-18704) | *Major* | **Upgrade hbase to commons-collections 4**

**WARNING: No release note provided for this change.**


---

* [HBASE-18697](https://issues.apache.org/jira/browse/HBASE-18697) | *Major* | **Need a shaded hbase-mapreduce module**

Replaces hbase-shaded-server-\<version\>.jar with hbase-shaded-mapreduce-\<version\>.jar.


---

* [HBASE-15607](https://issues.apache.org/jira/browse/HBASE-15607) | *Blocker* | **Remove PB references from Admin for 2.0**

All the references to Protos in Admin.java have been removed and replaced with respective POJO classes.
The references to Protos that were removed are
AdminProtos.GetRegionInfoResponse,
HBaseProtos.SnapshotDescription, HBaseProtos.SnapshotDescription.Type,
 MasterProtos.SnapshotResponse.
CompactionType, CompactionState and MasterSwitchType Enums have been moved out of Admin.java to standalone Enums.


---

* [HBASE-18674](https://issues.apache.org/jira/browse/HBASE-18674) | *Major* | **upgrade hbase to commons-lang3**

Move to commons-lang3 from common-lang (check it out!... Nice lib...Some nice utility)


---

* [HBASE-18736](https://issues.apache.org/jira/browse/HBASE-18736) | *Major* | **Cleanup the HTD/HCD for Admin**

Changed the passed arguments from HTD/HCD to TD/CFD for Admin.


---

* [HBASE-18699](https://issues.apache.org/jira/browse/HBASE-18699) | *Major* | **Copy LoadIncrementalHFiles to another package and mark the old one as deprecated**

Introduce a new o.a.h.h.tool.LoadIncrementalHFiles. The old o.a.h.h.mapreduce.LoadIncrementalHFiles is deprecated and will be removed in 3.0.0.


---

* [HBASE-18739](https://issues.apache.org/jira/browse/HBASE-18739) | *Major* | **Make all TimeRange Constructors InterfaceAudience Private.**

All constructors have already been deprecated. This change makes them InterfaceAudience Private.


---

* [HBASE-18675](https://issues.apache.org/jira/browse/HBASE-18675) | *Minor* | **Making {max,min}SessionTimeout configurable for MiniZooKeeperCluster**

<!-- markdown -->


Standalone clusters and minicluster instances can now configure the session timeout for our embedded ZooKeeper quorum using `hbase.zookeeper.property.minSessionTimeout` and `hbase.zookeeper.property.maxSessionTimeout`.


---

* [HBASE-15806](https://issues.apache.org/jira/browse/HBASE-15806) | *Critical* | **An endpoint-based export tool**

org.apache.hadoop.hbase.coprocessor.Export
Instructs HBase to dump the contents of table to HDFS in a sequence file
+ replaces MR by endpoint (see org.apache.hadoop.hbase.mapreduce.Export)
+ no large data to be transfered between hbase server and client
+ same command line as org.apache.hadoop.hbase.mapreduce.Export
- user needs to alter table for deploying ExportEndpoint
- user needs to adjust the endpoint timeout for dumping large data
- user needs to get the EXECUTE permission


---

* [HBASE-18577](https://issues.apache.org/jira/browse/HBASE-18577) | *Critical* | **shaded client includes several non-relocated third party dependencies**

<!-- markdown -->


The HBase shaded artifacts (hbase-shaded-client and hbase-shaded-server) no longer contain several non-relocated third party dependency classes that were mistakenly included. Downstream users who relied on these classes being present will need to add a runtime dependency onto an appropriate third party artifact.

Previously, we erroneously packaged several third party libs without relocating them. In some cases these libraries have now been relocated; in some cases they are no longer included at all.

Includes:

* jaxb
* jetty
* jersey
* codahale metrics (HBase 1.4+ only)
* commons-crypto
* jets3t
* junit
* curator (HBase 1.4+)
* netty 3 (HBase 1.1)
* mokito-junit4 (HBase 1.1)

There is now testing to ensure that the shaded artifacts only contain expected relocated content. It can be run via `mvn -Dtest=noUnitTests -pl hbase-shaded/hbase-shaded-check-invariants -am -Prelease verify`.

For version 2.0+ this patch removes hadoop-mapreduce-client-core from the set of dependencies included for the hbase-client and hbase-shaded-client artifacts.

For 2.0+, the slf4j-log4j12 dependency is now optional for both shaded artifacts.


---

* [HBASE-14745](https://issues.apache.org/jira/browse/HBASE-14745) | *Blocker* | **Shade the last few dependencies in hbase-shaded-client**

Previously some dependencies in hbase-shaded-client were still leaking into the un-shaded namespace. This should now be fixed.

Additionally the rat checking on generated intermediate files from shading should be skipped.


---

* [HBASE-18665](https://issues.apache.org/jira/browse/HBASE-18665) | *Critical* | **ReversedScannerCallable invokes getRegionLocations incorrectly**

Performing reverse scan on tables used the meta cache incorrectly and fetched data from meta table every time. This fix solves this issue and which results in performance improvement for reverse scans.


---

* [HBASE-3935](https://issues.apache.org/jira/browse/HBASE-3935) | *Major* | **HServerLoad.storefileIndexSizeMB should be changed to storefileIndexSizeKB**

This patch removed the storefile\_index\_size\_MB in protobuf. It will cause the value of storefile\_index\_size\_MB is zero if user still use hbase-client 1.x.


---

* [HBASE-18640](https://issues.apache.org/jira/browse/HBASE-18640) | *Major* | **Move mapreduce out of hbase-server into separate hbase-mapreduce module**

- Moves all org.apache.hadoop.hbase.mapreduce.\* (except LoadIncrementalHFiles) and org.apache.hadoop.hbase.mapred.\* classes from hbase-server module to new hbase-mapreduce module.
- Also moves following tools from hbase-server module to hbase-mapreduce module: CompactionTool, ExportSnapshot, PerformanceEvaluation, LoadTestTool
- Very minor breakages in  LoadTestTool(LimitedPrivate HBaseInterfaceAudience.TOOLS)


---

* [HBASE-18519](https://issues.apache.org/jira/browse/HBASE-18519) | *Major* | **Use builder pattern to create cell**

Introduce the CellBuilder helper.
1) Using CellBuilderFactory to get CellBuilder for creating cell with row,
    column, qualifier, type, and value.
2) For internal use, the ExtendedCellBuilder, which is created by ExtendedCellBuilderFactory, is able to build cell with extra fields - sequence id and tags -


---

* [HBASE-18448](https://issues.apache.org/jira/browse/HBASE-18448) | *Minor* | **EndPoint example  for refreshing HFiles for stores**

Adds a new RefreshHFiles Coprocessor Endpoint example. Includes client and serverside-endpoint that iterates region Stores to call #refreshStoreFiles.


---

* [HBASE-18658](https://issues.apache.org/jira/browse/HBASE-18658) | *Major* | **Purge hokey hbase Service implementation; use (internal) Guava Service instead**

Removed hbase Service class. It was not fully-formed. Now Guava is relocated, use its Service instead internally; it has nice implementation facility too in AbstractService.


---

* [HBASE-15982](https://issues.apache.org/jira/browse/HBASE-15982) | *Blocker* | **Interface ReplicationEndpoint extends Guava's Service**

    Breaking change to our ReplicationEndpoint and BaseReplicationEndpoint.

    ReplicationEndpoint implemented Guava 0.12 Service. An abstract
    subclass, BaseReplicationEndpoint, provided default implementations
    and facility, among other things, by extending Guava's
    AbstractService class.

    Both of these HBase classes were marked LimitedPrivate for
    REPLICATION so these classes were semi-public and made it so
    Guava 0.12 was part of our API.

    Having Guava in our API was a mistake. It anchors us and the
    implementation of the Interface to Guava 0.12. This is untenable
    given Guava changes and that the Service Interface in particular
    has had extensive revamp and improvement done. We can't hold to
    the Guava Interface. It changed. We can't stay on Guava 0.12;
    implementors and others on our CLASSPATH won't abide being stuck
    on an old Guava.

    So we make breaking changes. The unhitching of our Interface
    from Guava could only be done in a breaking manner. It undoes the
    LimitedPrivate on BaseReplicationEndpoint while keeping it for the RE
    Interface. It means consumers will have to copy/paste the
    AbstractService-based BRE into their own codebase also supplying their
    own Guava; HBase no longer 'supplies' this (our Guava usage has
    been internalized, relocated).

    This patch then adds into RE the basic methods RE needs of the old
    Guava Service rather than return a Service to start/stop only to go
    back to the RE instance to do actual work. A few method names had to
    be changed so could make implementations with Guava Service internally
    and not have RE method names and types clash). Semantics remained the
    same otherwise. For example startAsync and stopAsync in Guava are start
    and stop in RE.


---

* [HBASE-18347](https://issues.apache.org/jira/browse/HBASE-18347) | *Major* | **Implement a BufferedMutator for async client**

Introduce an AsyncBufferedMutator for batching requests to HBase for a single table.

Use AsyncConnection.getBufferedMutator method to get an AsyncBufferedMutator instance.


---

* [HBASE-18546](https://issues.apache.org/jira/browse/HBASE-18546) | *Critical* | **Always overwrite the TS for Append/Increment unless no existing cells are found**

If there is no existing cell in submitting Append/Increment, the custom ts won't be overridden. By contrast, the cell's ts will always be overridden by server.


---

* [HBASE-18224](https://issues.apache.org/jira/browse/HBASE-18224) | *Critical* | **Upgrade jetty**

Moved from Jetty 9.3.x to 9.4.x.

Jetty returns more correct HTTP code when Header is too long, 431 instead of 413, and it requires more threads to start up (made default 16 instead of 10).


---

* [HBASE-17442](https://issues.apache.org/jira/browse/HBASE-17442) | *Critical* | **Move most of the replication related classes from hbase-client to hbase-replication package**

Move replication implementation's classes from hbase-client to hbase-replication package.


---

* [HBASE-18653](https://issues.apache.org/jira/browse/HBASE-18653) | *Major* | **Undo hbase2 check against \< hadoop2.6.x; i.e. implement agreed drop of hadoop 2.4 and 2.5 support in hbase2**

Change the yetus profile for branch-2 so it no longer runs hadoop 2.4.x and 2.5.x build checks.


---

* [HBASE-18630](https://issues.apache.org/jira/browse/HBASE-18630) | *Major* | **Prune dependencies; as is branch-2 has duplicates**

Removed doubled instances of javax.inject and commons-beanutils where the versions were close.

Other instances of 'double' includes have different groupids so wary pruning especially when transitive includes (hadoop or jetty et al.)


---

* [HBASE-18631](https://issues.apache.org/jira/browse/HBASE-18631) | *Minor* | **Allow configuration of ChaosMonkey properties via hbase-site**

This change invalidates the need for a separate Java properties file to configure the ChaosMonkey included with HBase. These properties can be provided directly in hbase-site.xml. If configuration in provided in both locations, the Java properties file takes precendence.


---

* [HBASE-18489](https://issues.apache.org/jira/browse/HBASE-18489) | *Major* | **Expose scan cursor in RawScanResultConsumer**

Add a 'cursor' method which returns an 'Optional\<Cursor\>' in 'RawScanResultConsumer.ScanController'. You can use this method to obtain the scan cursor if available.


---

* [HBASE-18511](https://issues.apache.org/jira/browse/HBASE-18511) | *Blocker* | **Default no regions on master**

Changes the configuration hbase.balancer.tablesOnMaster from list of table names that the can carry (with 'none' meaning no tables on the master) to instead be a boolean that is set to true if master carries tables/regions and false if it does not. If true, the master acts like any regionserver.

If false, then the master carries no tables. This is the default for hbase-2.0.0.

Another boolean configuration, hbase.balancer.tablesOnMaster.systemTablesOnly, when set to true, enables hbase.balancer.tablesOnMaster and makes it so the master hosts system tables exclusively (the long-time deploy mode of master branch and branch-2 up until this commit).

UPDATE: This is broke. See HBASE-19785.
UPDATE2: Master carrying Regions does not work reliably, see HBASE-19828.

See HBASE-19831, the issue to fix regions on Master

The change of hbase.balancer.tablesOnMaster from String list to boolean and
the addition of a simple boolean to enable system-tables on Master was done
to constrain what operators might ask for via this master configuration.
Stipulating what tables are bound to the Master server verges into
regionserver grouping territory, a more robust means of specifying table
and server combinations. Operators should use this latter if they want
layouts more exotic than those supplied by the provided booleans.


---

* [HBASE-18553](https://issues.apache.org/jira/browse/HBASE-18553) | *Major* | **Expose scan cursor for asynchronous scanner**

The ResultScanner which is gotten from an AsyncTable will also return cursor results if Scan.isNeedCursorResult is true.


---

* [HBASE-18598](https://issues.apache.org/jira/browse/HBASE-18598) | *Minor* | **AsyncNonMetaRegionLocator use FIFO algorithm to get a candidate locate request**

Introduce FIFO algorithm to get a candidate locate request for AsyncNonMetaRegionLocator.


---

* [HBASE-18533](https://issues.apache.org/jira/browse/HBASE-18533) | *Major* | **Expose BucketCache values to be configured**

This patch exposes configuration for Bucketcache. These configs are very similar to those for the LRU cache, but are described below:

"hbase.bucketcache.single.factor"; /\*\* Single access bucket size \*/
"hbase.bucketcache.multi.factor"; /\*\* Multiple access bucket size \*/
"hbase.bucketcache.memory.factor"; /\*\* In-memory bucket size \*/
"hbase.bucketcache.extrafreefactor"; /\*\* Free this floating point factor of extra blocks when evicting. For example free the number of blocks requested \* (1 + extraFreeFactor) \*/
"hbase.bucketcache.acceptfactor"; /\*\* Acceptable size of cache (no evictions if size \< acceptable) \*/
"hbase.bucketcache.minfactor"; /\*\* Minimum threshold of cache (when evicting, evict until size \< min) \*/


---

* [HBASE-18528](https://issues.apache.org/jira/browse/HBASE-18528) | *Critical* | **DON'T allow user to modify the passed table/column descriptor**

**WARNING: No release note provided for this change.**


---

* [HBASE-18271](https://issues.apache.org/jira/browse/HBASE-18271) | *Blocker* | **Shade netty**

Depend on hbase-thirdparty for our netty instead of directly relying on netty-all. netty is relocated in hbase-thirdparty from io.netty to org.apache.hadoop.hbase.shaded.io.netty. One kink is that netty bundles an .so. Its files also are relocated. So netty can find the .so content, need to specify on command-line a system property telling netty about the shading.

The .so trick is from
             https://stackoverflow.com/questions/33825743/rename-files-inside-a-jar-using-some-maven-plugin

In essence we need the below defined whenever we run tests or deploy:

-Dorg.apache.hadoop.hbase.shaded.io.netty.packagePrefix=org.apache.hadoop.hbase.shaded.

(The trailing '.' is required)

See toward the end of this issue for how to pass config: https://github.com/netty/netty/issues/6665

The system property has been added to bin/hbase. If starting hbase with other than bin/hbase, add this system property (at least on linux).

For devs, going forward, do not reference io.netty. Reference org.apache.hadoop.hbase.io.netty instead. Here is sample:

{code}
-import io.netty.channel.Channel;
-import io.netty.channel.EventLoop;
+import org.apache.hadoop.hbase.shaded.io.netty.channel.Channel;
+import org.apache.hadoop.hbase.shaded.io.netty.channel.EventLoop;
{code}


---

* [HBASE-15511](https://issues.apache.org/jira/browse/HBASE-15511) | *Major* | **ClusterStatus should be able to return responses by scope**

Provide a new way to get desired ClusterStatus with a set of ClusterStatus.Option, such that the response back to client can be limited.
Note that, the constructor way to new a ClusterStatus will be no longer support after 2.0.0,  and use ClusterStatus.Builder instead.


---

* [HBASE-18551](https://issues.apache.org/jira/browse/HBASE-18551) | *Major* | **[AMv2] UnassignProcedure and crashed regionservers**

Unassign will not proceed if it is unable to talk to the remote server. Now it will expire the server it is unable to communicate with and then wait until it is signaled by ServerCrashProcedure that the server's logs have been split. Only then will judge the unassign successful.

We do this because a subsequent assign lacking the crashed server context might open a region w/o first splitting logs.


---

* [HBASE-18469](https://issues.apache.org/jira/browse/HBASE-18469) | *Critical* | **Correct  RegionServer metric of  totalRequestCount**

In HBASE-18469 we introduced a new RegionServer metrics in name of "totalRowActionRequestCount" which counts in all row actions and equals to the sum of "readRequestCount" and "writeRequestCount". Meantime, we have changed "totalRequestCount" to count only once for multi request, while previously we will count in action number of the request. As a result, existing monitoring system on totalRequestCount will still work but see a smaller value, and we strongly recommend to change to use the new metrics to monitor server load.


---

* [HBASE-18500](https://issues.apache.org/jira/browse/HBASE-18500) | *Major* | **Performance issue: Don't use BufferedMutator for HTable's put method**

Remove the deprecated method get/setWriteBufferSize from Table and remove writeBufferSize from TableBuilder. Remove the BufferedMutatorImpl from HTable.


---

* [HBASE-18387](https://issues.apache.org/jira/browse/HBASE-18387) | *Minor* | **[Thrift] Make principal configurable in DemoClient.java**

This change allows the demonstration Thrift client to customize the server principal used by the Thrift server for instances secured with Kerberos.


---

* [HBASE-17125](https://issues.apache.org/jira/browse/HBASE-17125) | *Critical* | **Inconsistent result when use filter to read data**

Marked Scan and Get's setMaxVersions() and setMaxVersions(int) as deprecated. They are easy to misunderstand with column family's max versions, so use readAllVersions() and readVersions(int) instead.


---

* [HBASE-18492](https://issues.apache.org/jira/browse/HBASE-18492) | *Major* | **[AMv2] Embed code for selecting highest versioned region server for system table regions in AssignmentManager.processAssignQueue()**

Favors new servers over older versions when assigning system table regions (more to follow in this area; i.e. changes in the AM itself).


---

* [HBASE-18517](https://issues.apache.org/jira/browse/HBASE-18517) | *Major* | **limit max log message width in log4j**

Sets a log length max of 1000 characters.


---

* [HBASE-18502](https://issues.apache.org/jira/browse/HBASE-18502) | *Critical* | **Change MasterObserver to use TableDescriptor and ColumnFamilyDescriptor**

The methods which change to use TableDescriptor/ColumnFamilyDescriptor are shown below.
+ preCreateTable( ObserverContext,TableDescriptor, HRegionInfo[])
+ postCreateTable(ObserverContext ,TableDescriptor, HRegionInfo[])
+ preCreateTableAction(ObserverContext, TableDescriptor,HRegionInfo[])
+ postCompletedCreateTableAction(ObserverContext,TableDescriptor,HRegionInfo[])
+ preModifyTable(ObserverContext,TableName, TableDescriptor)
+ postModifyTable(ObserverContext,TableName, TableDescriptor)
+ preModifyTableAction( ObserverContext,TableName,TableDescriptor)
+ postCompletedModifyTableAction( ObserverContext,TableName,TableDescriptor)
+ preAddColumnFamily(ObserverContext,TableName, ColumnFamilyDescriptor)
+ postAddColumnFamily(ObserverContext,TableName, ColumnFamilyDescriptor)
+ preAddColumnFamilyAction(ObserverContext,TableName,ColumnFamilyDescriptor)
+ postCompletedAddColumnFamilyAction(ObserverContext,TableName, ColumnFamilyDescriptor)
+ preModifyColumnFamily(ObserverContext,TableName, ColumnFamilyDescriptor)
+ preModifyColumnFamilyAction(ObserverContext\<MasterCoprocessorEnvironment,TableName,ColumnFamilyDescriptor)
+ postCompletedModifyColumnFamilyAction(ObserverContext\<MasterCoprocessorEnvironment\>,TableName,ColumnFamilyDescriptor)
+ preCloneSnapshot(ObserverContext\<MasterCoprocessorEnvironment\>,SnapshotDescription,TableDescriptor)
+ postCloneSnapshot(ObserverContext\<MasterCoprocessorEnvironment\>,SnapshotDescription,TableDescripto)
+ preRestoreSnapshot(ObserverContext\<MasterCoprocessorEnvironment,SnapshotDescription,TableDescriptor)
+ postRestoreSnapshot(ObserverContext\<MasterCoprocessorEnvironment,SnapshotDescription,TableDescriptor)
+ preGetTableDescriptors(ObserverContext\<MasterCoprocessorEnvironment\>,List\<TableName\>, List\<TableDescriptor\>,String)
+ postGetTableDescriptors(ObserverContext\<MasterCoprocessorEnvironment\>,List\<TableName\>, List\<TableDescriptor\>,String)
+ preGetTableNames(ObserverContext\<MasterCoprocessorEnvironment\>,List\<TableDescriptor\>, String)
+ postGetTableNames(ObserverContext\<MasterCoprocessorEnvironment\>,List\<TableDescriptor\>, String)


---

* [HBASE-18520](https://issues.apache.org/jira/browse/HBASE-18520) | *Minor* | **Add jmx value to determine true Master Start time**

This JIRA adds a JMX value to track when the Master has finished initializing.
The jmx config is 'masterFinishedInitializationTime' and details the time in millis that the Master is fully usable and ready to serve requests.


---

* [HBASE-17056](https://issues.apache.org/jira/browse/HBASE-17056) | *Critical* | **Remove checked in PB generated files**

Purge all checked in generated protobuf files (30MB). Generate protobuf files inline with the build. Remove checked-in and patched protobuf. Get it from new hbase-thirdparty instead.

Side-effect: Our protobuf went from 3.1.0 to 3.3.1.

Build does not take noticeably longer (still about 2.5 minutes to do a mvn clean install -DskipTests).

IDEs will probably require a mvn build first else they'll complain about missing (generated) files.


---

* [HBASE-18374](https://issues.apache.org/jira/browse/HBASE-18374) | *Major* | **RegionServer Metrics improvements**

This change adds the latency metrics checkAndPut, checkAndDelete, putBatch and deleteBatch . Also the previous regionserver "mutate" latency metrics are renamed to "put" metrics. Batch metrics capture the latency of the entire batch containing put/delete whereas put/delete metrics capture latency per operation. Note this change will break existing monitoring based on regionserver "mutate" latency metric.


---

* [HBASE-18023](https://issues.apache.org/jira/browse/HBASE-18023) | *Minor* | **Log multi-\* requests for more than threshold number of rows**

HBASE-18023 introduces a warning message in the RegionServer log when an RPC is received from a client that has more than 5000 "actions" (where an "action" is a collection of mutations for a specific row) in a single RPC. Misbehaving clients who send large RPCs to RegionServers can be malicious, causing temporary pauses via garbage collection or denial of service via crashes. The threshold of 5000 actions per RPC is defined by the property "hbase.rpc.rows.warning.threshold" in hbase-site.xml.


---

* [HBASE-15968](https://issues.apache.org/jira/browse/HBASE-15968) | *Major* | **New behavior of versions considering mvcc and ts rather than ts only**

This issue resolved two long-term issues in HBase:
Puts may be masked by a delete before them.
Major compactions change query results.

This issue offer a new behavior to fix this issue with a little performance reduction. Set NEW\_VERSION\_BEHAVIOR to true to enable this feature in CF level. See HBASE-15968 for details.
Note if you enable this feature, the order of Mutations matters. But replication will disorder the entries by default. So you have to enable serial replication if you have slave clusters. See HBASE-9465 for details.


---

* [HBASE-18107](https://issues.apache.org/jira/browse/HBASE-18107) | *Major* | **[AMv2] Remove DispatchMergingRegionsRequest & DispatchMergingRegions**

Removes merge region code added into branch-2 but that was not needed after all. Branch-2 replaced dispatchMergingRegions with MergeTableRegionsProcedure.

Removed:

# dispatchMergingRegions from Connection (was superceded long ago in branch-1).
# mergeRegions from RsRpcServices (was not used).


---

* [HBASE-15816](https://issues.apache.org/jira/browse/HBASE-15816) | *Major* | **Provide client with ability to set priority on Operations**

Added setPriority(int priority) API to Put, Delete, Increment, Append, Get and Scan pojos.  So for all these ops, the user can provide a custom priority level.


---

* [HBASE-18430](https://issues.apache.org/jira/browse/HBASE-18430) | *Major* | **Typo in "contributing to documentation" page**

Pushed to {{master}}. Thanks, Coral! Congratulations on your first Apache HBase commit!


---

* [HBASE-17908](https://issues.apache.org/jira/browse/HBASE-17908) | *Critical* | **Upgrade guava**

Use relocated guava 22.0 gotten from the new hbase-thirdparty ancillary project.

Incompatible change. ReplicationEndpoint and subclasses extend guava Service which changed pretty radically between 12.0 and 22.0. Change is kosher because implementations are marked audience private. Still, this will likely cause grief for the likes of the downstream lily indexer.


---

* [HBASE-16993](https://issues.apache.org/jira/browse/HBASE-16993) | *Major* | **BucketCache throw java.io.IOException: Invalid HFile block magic when configuring hbase.bucketcache.bucket.sizes**

Any value for hbase.bucketcache.bucket.sizes  configuration to be multiple of 256.  If that is not the case, instantiation of L2 Bucket cache itself will fail throwing IllegalArgumentException.


---

* [HBASE-16090](https://issues.apache.org/jira/browse/HBASE-16090) | *Major* | **ResultScanner is not closed in SyncTable#finishRemainingHashRanges()**

pushed to 1.3 and 1.2. SyncTable was introduced in 1.2, so skipping 1.1.


---

* [HBASE-18332](https://issues.apache.org/jira/browse/HBASE-18332) | *Minor* | **Upgrade asciidoctor-maven-plugin**

Committed to master and branch-2. Thanks!


---

* [HBASE-18161](https://issues.apache.org/jira/browse/HBASE-18161) | *Minor* | **Incremental Load support for Multiple-Table HFileOutputFormat**

In order to use this feature, a user must  
1. Register their tables when configuring their job
 2. Create a composite key of the tablename and original rowkey to send as the mapper output key.

  To register their tables (and configure their job for incremental load into multiple tables), a user must call the static MultiHFileOutputFormat.configureIncrementalLoad function to register the HBase tables that will be ingested into.   

To create the composite key, a helper function MultiHFileOutputFormat2.createCompositeKey should be called with the destination tablename and rowkey as arguments, and the result should be output as the mapper key.

 Before this JIRA, for HFileOutputFormat2 a configuration for the storage policy was set per Column Family. This was set manually by the user. In this JIRA, this is unchanged when using HFileOutputFormat2. However, when specifically using MultiHFileOutputFormat2, the user now has to manually set the prefix by creating a composite of the table name and the column family. The user can create the new composite value by calling MultiHFileOutputFormat2.createCompositeKey with the tablename and column family as arguments.

Changes added through this JIRA are backwards compatible with existing HFileOutputFormat2 apis and functionality.

The configuration parameter "hbase.mapreduce.hfileoutputformat.table.name" is now a REQUIRED parameter though it is normally set automatically when configureIncrementalLoad method is called within HFileOutputFormat2


---

* [HBASE-18229](https://issues.apache.org/jira/browse/HBASE-18229) | *Critical* | **create new Async Split API to embrace AM v2**

A new splitRegionAsync() API is added in client. The existing splitRegion()  and split() API will call the new API so client does not have to change its code.

Move HBaseAdmin.splitXXX() logic to master, client splitXXX() API now go to master directly instead of going to RegionServer first.

Also added splitSync() API


---

* [HBASE-18339](https://issues.apache.org/jira/browse/HBASE-18339) | *Major* | **Update test-patch to use hadoop 3.0.0-alpha4**

HBase now defaults to Apache Hadoop 3.0.0-alpha4 when the Hadoop 3 profile is active.


---

* [HBASE-18267](https://issues.apache.org/jira/browse/HBASE-18267) | *Major* | **The result from the postAppend is ignored**

**WARNING: No release note provided for this change.**


---

* [HBASE-18307](https://issues.apache.org/jira/browse/HBASE-18307) | *Major* | **Share the same EventLoopGroup for NettyRpcServer, NettyRpcClient and AsyncFSWALProvider at RS side**

There are two configuration name changes as the event loop configs will not only effect rpc server but be shared by different components in the same RS instance.

'hbase.rpc.server.nativetransport' -\> 'hbase.netty.nativetransport'

'hbase.netty.rpc.server.worker.count' -\> 'hbase.netty.worker.count'


---

* [HBASE-18241](https://issues.apache.org/jira/browse/HBASE-18241) | *Critical* | **Change client.Table, client.Admin, Region, Store, and HBaseTestingUtility to not use HTableDescriptor or HColumnDescriptor**

- : removed API
+ : new API
\* : deprecated API
---------------------------
Region class
- HTableDescriptor getTableDesc()
+TableDescriptor getTableDescriptor()

Store class
- HColumnDescriptor getFamily()
+ ColumnFamilyDescriptor getColumnFamilyDescriptor()

Table class
\* HTableDescriptor getTableDescriptor()
+ TableDescriptor getDescriptor()\|

\*Admin class\*
\* HTableDescriptor getTableDescriptor(TableName)
+ List\<TableDescriptor\> listTableDescriptor(TableName)\|
\* HTableDescriptor[] getTableDescriptors(List\<String\>)
\* HTableDescriptor[] getTableDescriptorsByTableName(List\<TableName\>)
+ List\<TableDescriptor\> listTableDescriptors(List\<TableName\>)
\* HTableDescriptor[] listTables()
+ List\<TableDescriptor\> listTableDescriptors()
\* HTableDescriptor[] listTables(Pattern)
+ List\<TableDescriptor\> listTableDescriptors(Pattern)
\* HTableDescriptor[] listTables(String)
+ List\<TableDescriptor\> listTableDescriptors(String)
\* HTableDescriptor[] listTables(Pattern, boolean)
+ List\<TableDescriptor\> listTableDescriptors(Pattern, boolean)
\* HTableDescriptor[] listTables(String, boolean)
+ List\<TableDescriptor\> listTableDescriptors(String, boolean)
\* HTableDescriptor[] deleteTables(String)
\* HTableDescriptor[] deleteTables(Pattern)
\* HTableDescriptor[] enableTables(String)
\* HTableDescriptor[] enableTables(Pattern)
\* HTableDescriptor[] disableTables(String)
\* HTableDescriptor[] disableTables(Pattern)
\* void modifyTable(TableName, HTableDescriptor)
+ void modifyTable(TableDescriptor)
\* void modifyTableAsync(TableName, HTableDescriptor)
+ void modifyTableAsync(TableDescriptor)
\* HTableDescriptor[] listTableDescriptorsByNamespace(String)
+ List\<TableDescriptor\> listTableDescriptorsByNamespace(byte[])
\* void createTable(HTableDescriptor)
+ void createTable(TableDescriptor)
\* void createTable(HTableDescriptor, byte[], byte[], int)
+ void createTable({color:red}TableDescriptor, byte[], byte[], int)
\* void createTable(HTableDescriptor, byte[][])
+ void createTable(TableDescriptor, byte[][])
\* Future\<Void\> createTableAsync(HTableDescriptor, byte[][])
+ Future\<Void\> createTableAsync(TableDescriptor, byte[][])

\*HBaseTestingUtility class\*
\* Table createTable(HTableDescriptor, byte[][], Configuration)
+ Table createTable(TableDescriptor, byte[][], Configuration)
\* Table createTable(HTableDescriptor, byte[][], byte[][], Configuration)
+ Table createTable(TableDescriptor, byte[][], byte[][], Configuration)
\* public Table createTable(HTableDescriptor, byte[][])
+ public Table createTable(TableDescriptor, byte[][])
\* void modifyTableSync(Admin, HTableDescriptor)
+ void modifyTableSync(Admin, TableDescriptor)
\* HRegion createLocalHRegion(HTableDescriptor, byte [], byte [])
+ HRegion createLocalHRegion(TableDescriptor, byte [], byte [])
\* HRegion createLocalHRegion(HRegionInf, HTableDescriptor)
+ HRegion createLocalHRegion(HRegionInf, TableDescriptor)
\* HRegion createLocalHRegion(HRegionInfo, HTableDescriptor, WAL)
+ HRegion createLocalHRegion(HRegionInfo, TableDescriptor, WAL)
\* List createMultiRegionsInMeta(final Configuration, HTableDescriptor, byte [][])
+ List createMultiRegionsInMeta(final Configuration, TableDescriptor, byte [][])
\* HRegion createRegionAndWAL(HRegionInfo, Path, Configuration, HTableDescriptor)
+ HRegion createRegionAndWAL(HRegionInfo, Path, Configuration, TableDescriptor)
\* HRegion createRegionAndWAL(HRegionInfo, Pat, Configuration, HTableDescriptor, boolean)
+ HRegion createRegionAndWAL(HRegionInfo, Pat, Configuration, TableDescriptor, boolean)
\* int createPreSplitLoadTestTable(Configuration,HTableDescriptor, HColumnDescriptor)
+ int createPreSplitLoadTestTable(Configuration,TableDescriptor, ColumnFamilyDescriptor)
\* int createPreSplitLoadTestTable(Configuration, HTableDescriptor, HColumnDescriptor, int)
+ int createPreSplitLoadTestTable(Configuration, TableDescriptor, ColumnFamilyDescriptor, int)
\* int createPreSplitLoadTestTable(Configuration, HTableDescriptor, HColumnDescriptor[], int)
+ int createPreSplitLoadTestTable(Configuration, TableDescriptor, ColumnFamilyDescriptor[], int)
\* int createPreSplitLoadTestTable(Configuration,HTableDescriptor, HColumnDescriptor[],SplitAlgorithm, int)
+ int createPreSplitLoadTestTable(Configuration,TableDescriptor, ColumnFamilyDescriptor[],SplitAlgorithm, int)
\* HRegion createTestRegion(String, HColumnDescriptor)
+ HRegion createTestRegion(String, ColumnFamilyDescriptor)


---

* [HBASE-18083](https://issues.apache.org/jira/browse/HBASE-18083) | *Major* | **Make large/small file clean thread number configurable in HFileCleaner**

After HBASE-18083 we could configure HFileCleaner to use multiple threads for large/small (archived) hfile cleaning with hbase.regionserver.hfilecleaner.large.thread.count and hbase.regionserver.hfilecleaner.small.thread.count, both default to 1. These properties support online configuration change.


---

* [HBASE-17931](https://issues.apache.org/jira/browse/HBASE-17931) | *Blocker* | **Assign system tables to servers with highest version**

We usually keep compatibility between old client and new server so we can do rolling upgrade, HBase cluster first, then HBase client. But we don't guarantee new client can access old server.
In an HBase cluster, we have system tables and region servers will access these tables so for servers they are also an HBase client. So if the system tables are in region servers with lower version we may get trouble because region servers with higher version may can not access them.
After this patch, we will move all system regions to region servers with highest version. So when we do a rolling upgrade across two major or minor versions, we should ALWAYS UPGRADE MASTER FIRST and then upgrade region servers. The new master will handle system tables correctly.


---

* [HBASE-6581](https://issues.apache.org/jira/browse/HBASE-6581) | *Major* | **Build with hadoop.profile=3.0**

Make us build against hadoop trunk (3.0)


---

* [HBASE-16120](https://issues.apache.org/jira/browse/HBASE-16120) | *Minor* | **Add shell test for truncate\_preserve**

Add unit tests for truncate\_preserve


---

* [HBASE-18240](https://issues.apache.org/jira/browse/HBASE-18240) | *Major* | **Add hbase-thirdparty, a project with hbase utility including an hbase-shaded-thirdparty module with guava, netty, etc.**

Adds a new project, hbase-thirdparty, at https://git-wip-us.apache.org/repos/asf/hbase-thirdparty used by core hbase. GroupID org.apache.hbase.thirdparty. Version 1.0.0.

This project packages relocated third-party libraries used by Apache HBase such as protobuf, guava, and netty among others. HBase core depends on it.

It has threre submodules, one to patch and then relocate (shade) protobuf, and one to do messy .so renaming (netty). The remainder module relocates a bundle of other (unpatched) libs used by hbase. This latter set includes protobuf-util, netty-all, gson, and guava.

All shading is done using the same relocation offset of org.apache.hadoop.hbase.shaded; we add this prefix to the relocated thirdparty library class names.

See the pom.xml in hbase-thirdparty for the explicit version of each third-party lib included (of note, we update out internal protobuf from 3.1.0 to 3.3.1).


---

* [HBASE-15943](https://issues.apache.org/jira/browse/HBASE-15943) | *Major* | **Add page displaying JVM process metrics**

Adds new "Process Metrics' tab along the top which leads to new page that dumps mbean -- mostly jvm -- metrics


---

* [HBASE-14902](https://issues.apache.org/jira/browse/HBASE-14902) | *Major* | **Revert some of the stringency recently introduced by checkstyle tightening**

Changes the checkstyle so that on a continuation line for javadoc, instead of default four spaces, instead now it is two spaces. Also one line statements as in if (true) x =1; now pass checkstyle.


---

* [HBASE-17110](https://issues.apache.org/jira/browse/HBASE-17110) | *Major* | **Improve SimpleLoadBalancer to always take server-level balance into account**

After HBASE-17110 the bytable strategy for SimpleLoadBalancer will also take server level balance into account


---

* [HBASE-17928](https://issues.apache.org/jira/browse/HBASE-17928) | *Major* | **Shell tool to clear compaction queues**

Adds clear\_compaction\_queues to the hbase shell.
{code}
  Clear compaction queues on a regionserver.
  The queue\_name contains short and long.
  short is shortCompactions's queue,long is longCompactions's queue.

  Examples:
  hbase\> clear\_compaction\_queues 'host187.example.com,60020'
  hbase\> clear\_compaction\_queues 'host187.example.com,60020','long'
  hbase\> clear\_compaction\_queues 'host187.example.com,60020', ['long','short']
{code}


---

* [HBASE-18164](https://issues.apache.org/jira/browse/HBASE-18164) | *Critical* | **Much faster locality cost function and candidate generator**

New locality cost function and candidate generator that use caching and incremental computation to allow the stochastic load balancer to consider ~20x more cluster configurations for big clusters.


---

* [HBASE-18226](https://issues.apache.org/jira/browse/HBASE-18226) | *Major* | **Disable reverse DNS lookup at HMaster and use the hostname provided by RegionServer**

The following config is added by this JIRA:

hbase.regionserver.hostname.disable.master.reversedns

This config is for experts: don't set its value unless you really know what you are doing.
When set to true, regionserver will use the current node hostname for the servername and HMaster will skip reverse DNS lookup and use the hostname sent by regionserver instead. Note that this config and hbase.regionserver.hostname are mutually exclusive. See https://issues.apache.org/jira/browse/HBASE-18226 for more details.

Caution: please make sure rolling upgrade succeeds before turning on this feature.


---

* [HBASE-16242](https://issues.apache.org/jira/browse/HBASE-16242) | *Major* | **Upgrade Avro to 1.7.7**

Apache HBase now specifies that version 1.7.7 of the Apache Avro library should be pulled in by maven and included in the convenience binary tarball.


---

* [HBASE-18213](https://issues.apache.org/jira/browse/HBASE-18213) | *Major* | **Add documentation about the new async client**

Add documentation for async client in section '66. Client' in ref guide.


---

* [HBASE-17008](https://issues.apache.org/jira/browse/HBASE-17008) | *Critical* | **Examples to make AsyncClient go down easy**

Add two examples for async client. AsyncClientExample is a simple example to show you how to use AsyncTable. HttpProxyExample is an example for advance user to show you how to use RawAsyncTable to write a fully asynchronous HTTP proxy server. There is no extra thread pool, all operations are executed inside netty's event loop.


---

* [HBASE-18200](https://issues.apache.org/jira/browse/HBASE-18200) | *Major* | **Set hadoop check versions for branch-2 and branch-2.x in pre commit**

Allow setting different hadoop check versions for branch-2 and branch-2.x when running pre commit check.


---

* [HBASE-18187](https://issues.apache.org/jira/browse/HBASE-18187) | *Major* | **Release hbase-2.0.0-alpha1**

Pushed the release. For detail: http://apache-hbase.679495.n3.nabble.com/ANNOUNCE-Apache-HBase-2-0-0-alpha-1-is-now-available-for-download-td4088484.html


---

* [HBASE-18137](https://issues.apache.org/jira/browse/HBASE-18137) | *Critical* | **Replication gets stuck for empty WALs**

0-length WAL files can potentially cause the replication queue to get stuck.  A new config "replication.source.eof.autorecovery" has been added: if set to true (default is false), the 0-length WAL file will be skipped after 1) the max number of retries has been hit, and 2) there are more WAL files in the queue.  The risk of enabling this is that there is a chance the 0-length WAL file actually has some data (e.g. block went missing and will come back once a datanode is recovered).


---

* [HBASE-18192](https://issues.apache.org/jira/browse/HBASE-18192) | *Blocker* | **Replication drops recovered queues on region server shutdown**

If a region server that is processing recovered queue for another previously dead region server is gracefully shut down, it can drop the recovered queue under certain conditions. Running without this fix on a 1.2+ release means possibility of continuing data loss in replication, irrespective of which WALProvider is used.
If a single WAL group (or DefaultWALProvider) is used, running without this fix will always cause dataloss in replication whenever a region server processing recovered queues is gracefully shutdown.


---

* [HBASE-18109](https://issues.apache.org/jira/browse/HBASE-18109) | *Critical* | **Assign system tables first (priority)**

Adds a sort of procedures before submission so system tables are queued first (which will help ensure they go out first). This should be good enough along w/ existing scheduling mechanisms to ensure system/meta are assigned first (See reasoning below). Open new issue if insufficient.


---

* [HBASE-18008](https://issues.apache.org/jira/browse/HBASE-18008) | *Major* | **Any HColumnDescriptor we give out should be immutable**

1) The HColumnDescriptor got from Admin, AsyncAdmin, and Table is immutable.
2) HColumnDescriptor have been marked as "Deprecated" and user should substituted
     ColumnFamilyDescriptor for HColumnDescriptor.
3) ColumnFamilyDescriptor is constructed through ColumnFamilyDescriptorBuilder and it contains all of the read-only methods from HColumnDescriptor
4) The value to which the IS\_MOB/MOB\_THRESHOLD is mapped is stored as String rather than Boolean/Long. The MOB is an new feature to 2.0 so this change should be acceptable


---

* [HBASE-18149](https://issues.apache.org/jira/browse/HBASE-18149) | *Major* | **The setting rules for table-scope attributes and family-scope attributes should keep consistent**

If the table-scope attributes value is false, you need not to enclose 'false' in single quotation.Both COMPACTION\_ENABLED =\> false and COMPACTION\_ENABLED =\> 'false' will take effect


---

* [HBASE-17849](https://issues.apache.org/jira/browse/HBASE-17849) | *Major* | **PE tool random read is not totally random**

When randomRead and randomSeekScan is used with PE tool, now we allow using both --size and --rows. The --size specifies the total size of the data (the range) on which the reads should be performed and --rows specifies the number of rows to be read by each client with in that range.


---

* [HBASE-15576](https://issues.apache.org/jira/browse/HBASE-15576) | *Major* | **Scanning cursor to prevent blocking long time on ResultScanner.next()**

If you don't like scanning being blocked too long because of heartbeat and partial result, you can use Scan#setNeedCursorResult(true) to get a special result within scanning timeout setting time which will tell you where row the server is scanning. See its javadoc for more details.


---

* [HBASE-16549](https://issues.apache.org/jira/browse/HBASE-16549) | *Major* | **Procedure v2 - Add new AM metrics**

Following AMv2 procedures are modified to override onSubmit(), onFinish() hooks provided by HBASE-17888 to do
metrics calculations when procedures are submitted and finshed:
\* AssignProcedure
\* UnassignProcedure
\* MergeTableRegionProcedure
\* SplitTableRegionProcedure
\* ServerCrashProcedure

Following metrics is collected for each of the above procedure during lifetime of a process:
\* Total number of requests submitted for a type of procedure
\* Histogram of runtime in milliseconds for successfully completed procedures
\* Total number of failed procedures

As we are moving away from Hadoop's metric2, hbase-metrics-api module is used for newly added metrics.


---

* [HBASE-9393](https://issues.apache.org/jira/browse/HBASE-9393) | *Critical* | **Hbase does not closing a closed socket resulting in many CLOSE\_WAIT**

To handle this issue client need to have Hadoop client 2.6.4 or 2.7.0+ Hadoop version as CanUnBuffer interface which was added as part of HDFS-7694 is available in only those versions.


---

* [HBASE-18038](https://issues.apache.org/jira/browse/HBASE-18038) | *Critical* | **Rename StoreFile to HStoreFile and add a StoreFile interface for CP**

StoreFile is now changed to an interface. This is an incompatible change. The coprocessors which implement RegionObserver may need to modify their code.


---

* [HBASE-16196](https://issues.apache.org/jira/browse/HBASE-16196) | *Critical* | **Update jruby to a newer version.**

The bundled JRuby 1.6.8 has been updated to version 9.1.9.0. The represents a change from Ruby 1.8 to Ruby 2.3.3, which introduces non-compatible language changes for user scripts.

This JRuby version update required an update to joni-2.1.11 and jcodings-1.0.18, used for regular expression matching, as well as several transitive dependency updates that should not be user-visible.


---

* [HBASE-14614](https://issues.apache.org/jira/browse/HBASE-14614) | *Major* | **Procedure v2: Core Assignment Manager**

Replaces the AssignmentManager with a new procedurev2-based AssignmentManager

h1. AMv2
Puts AssignmentManager up on top of the ProcedureV2 state machine with persistence engine. Each assignment atom is now a Procedure implementation; e.g. an AssignProcedure and an UnassignProcedure. Molecules of aggregated Procedures are used to do more involved assignment steps: e.g. the move region procedure is made of an Unassign followed by an Assign subprocedure.

AMv2 is 1500 lines. Old AM was near 4000. Functionality has been moved out to Procedures. In-memory states of regions and servers has been cleaned up stored in new RegionStates implementation. RegionStateStore takes care of publishing final region state out to the hbase:meta table.

New RemoteProcedureDispatcher/RSProcedureDispatcher runs the Procedure-based assignments ‘remotely’. Knows about ‘servers’. Does aggregation of assignments by time on a time/count basis so can send procedures in batches rather than one per RPC. Procedure status comes back on the back of the RegionServer heartbeat reporting online regions. The response is passed to the AMv2 to ‘process’. It will check against the in-memory state. If there is a mismatch, it fences out the RegionServer on the assumption that something went wrong on the RS side.Timeouts trigger retries. The Procedure machine ensures only one operation at a time on any one region/table using locking and smarts about what is serial and what can be run concurrently.

New accounting of RegionServer version will be used running rolling restarts.

‘States’ -- OPENING, CLOSING, etc. -- are now in-memory in-the-master only serialized out to the ProcedureV2 WAL. They are no longer persisted to ZooKeeper.

h2. Assign Detail
The Assign starts by pushing the "assign" operation to the AssignmentManager and then will go into a “waiting" state. The AM will batch the "assign" requests and ask the Balancer where to put the region (the various policies will be respected: retain, round-robin, random). Once the AM and the balancer have found a place for the region, the procedure will be resumed and an "open region" request will be placed in the Remote Dispatcher queue, and the procedure once again will go into a "waiting state".  The Remote Dispatcher will batch the various requests for that server and they will be sent to the RS for execution. The RS will complete the open operation by calling master.reportRegionStateTransition(). The AM will intercept the transition report, and notify the procedure. The procedure will finish the assignment by publishing to new state on hbase:meta or it will retry the assignment.

h3. Unassign Detail
 The Unassign starts by placing a "close region" request in the Remote Dispatcher queue, and the procedure will then go into a "waiting state". The Remote Dispatcher will batch the various requests for that server and they will be sent to the RS for execution. The RS will complete the open operation by calling master.reportRegionStateTransition(). The AM will intercept the transition report, and notify the procedure. The procedure will finish the unassign by publishing its new state on meta or it will retry the unassign.

h1. New Configs
 \* "hbase.procedure.remote.dispatcher.threadpool.size" defaults 128
 \* "hbase.procedure.remote.dispatcher.delay.msec" default 150ms
 \* "hbase.procedure.remote.dispatcher.max.queue.size" with default 32
 \* "hbase.regionserver.rpc.startup.waittime" with default 60 seconds.
h1. TODO
As of this writing.

Put up a model diagram.

 \* Handle region migration
 \* Handle meta assignment first
 \* Handle sys table assignment first (e.g. acl, namespace)
 \* Handle table priorities
 \* Do we report same AM metrics as we used too? We do it all in here now.

INCOMPATIBLE
A known incompatible is that because splits and merges are now run from the master, Coprocessors that used to watch for merge/split from a RegionObserver now no longer work; to watch split/merges, you need to have an observer on the Master instead.


---

* [HBASE-3462](https://issues.apache.org/jira/browse/HBASE-3462) | *Major* | **Fix table.jsp in regards to splitting a region/table with an optional splitkey**

UI pages for splitting/merging now operate by taking a row key prefix from the user rather than a full region name.


---

* [HBASE-18129](https://issues.apache.org/jira/browse/HBASE-18129) | *Major* | **truncate\_preserve fails when the truncate method doesn't exists on the master**

The command truncate\_preserve will be fine when the truncate method doesn't exist on the master


---

* [HBASE-18122](https://issues.apache.org/jira/browse/HBASE-18122) | *Major* | **Scanner id should include ServerName of region server**

The scanner id is not from 1 anymore.
The first 32 bits are MurmurHash32 of ServerName string "host,port,ts". The ServerName contains both host, port, and start timestamp so it can prevent collision. The lowest 32bit is generated by atomic int.


---

* [HBASE-17997](https://issues.apache.org/jira/browse/HBASE-17997) | *Major* | **In dev environment, add jruby-complete jar to classpath only when jruby is needed**

When JRUBY\_HOME is specified, if the command is "hbase shell" or "hbase org.jruby.Main", CLASSPATH and HBASE\_OPTS will be updated according to JRUBY\_HOME specified
\* Jar under JRUBY\_HOME is added to CLASSPATH
\* The following will be added into HBASE\_OPTS

-Djruby.home=$JRUBY\_HOME -Djruby.lib=$JRUBY\_HOME/lib


That is, as long as JRUBY\_HOME is specified, JRUBY\_HOME specified will take precedence.
\* In dev env, the jar recorded in cached\_classpath\_jruby.txt will be ignored
\* In non dev env, jruby-complete jar packaged with HBase will be ignored


---

* [HBASE-15616](https://issues.apache.org/jira/browse/HBASE-15616) | *Major* | **Allow null qualifier for all table operations**

After this issue, all table operations will support null qualifier, such as put/get/scan/increment/append/checkAndMutate/checkAndPut/checkAndDelete.


---

* [HBASE-18035](https://issues.apache.org/jira/browse/HBASE-18035) | *Critical* | **Meta replica does not give any primaryOperationTimeout to primary meta region**

When a client is configured to use meta replica, it sends scan request to all meta replicas almost at the same time. Since meta replica contains stale data, if result from one of replica comes back first, the client may get wrong region locations. To fix this, "hbase.client.meta.replica.scan.timeout" is introduced, a client will always send to primary meta region first, wait the configured timeout for reply. If no result is received, it will send request to replica meta regions. The unit for "hbase.client.meta.replica.scan.timeout"  is microsecond, the default value is 1000000 (1 second).


---

* [HBASE-11013](https://issues.apache.org/jira/browse/HBASE-11013) | *Major* | **Clone Snapshots on Secure Cluster Should provide option to apply Retained User Permissions**

While creating a snapshot, it will save permissions of the original table into .snapshotinfo file(Backward compatibility) , which is in the snapshot root directory.  For clone\_snapshot/restore\_snapshot command, we provide an additional option( RESTORE\_ACL) to decide whether we will grant permissons of the origin table to the newly created table.


---

* [HBASE-18018](https://issues.apache.org/jira/browse/HBASE-18018) | *Major* | **Support abort for all procedures by default**

The default behavior for abort() method of StateMachineProcedure class is changed to support aborting all procedures irrespective of if procedure supports rollback or not.


---

* [HBASE-16851](https://issues.apache.org/jira/browse/HBASE-16851) | *Major* | **User-facing documentation for the In-Memory Compaction feature**

Two blog posts on Apache HBase blog: user manual and programmer manual.
Ref. guide draft published: https://docs.google.com/document/d/1Xi1jh\_30NKnjE3wSR-XF5JQixtyT6H\_CdFTaVi78LKw/edit


---

* [HBASE-17343](https://issues.apache.org/jira/browse/HBASE-17343) | *Blocker* | **Make Compacting Memstore default in 2.0 with BASIC as the default type**

 This JIRA changes the default MemStore to be CompactingMemStore instead of DefaultMemStore. In-memory compaction of CompactingMemStore demonstrated sizable improvement in HBase’s write amplification and read/write performance.

CompactingMemStore achieves these gains through smart use of RAM. The algorithm periodically re-organizes the in-memory data in efficient data structures and reduces redundancies. The  HBase server’s memory footprint therefore periodically expands and contracts. The outcome is longer lifetime of data in memory, less I/O, and overall faster performance. More details about the algorithm and its use appear in the Apache HBase Blog: https://blogs.apache.org/hbase/

How To Use:
The in-memory compaction level can be configured both globally and per column family. The supported levels are none (DefaultMemStore), basic, and eager.

By default, all tables apply basic in-memory compaction. This global configuration can be overridden in hbase-site.xml, as follows:

\<property\>
 \<name\>hbase.hregion.compacting.memstore.type\</name\>
 \<value\>\<none\|basic\|eager\>\</value\>
 \</property\>

The level can also be configured in the HBase shell per column family, as follows:

create ‘\<tablename\>’,
{NAME =\> ‘\<cfname\>’, IN\_MEMORY\_COMPACTION =\> ‘\<NONE\|BASIC\|EAGER\>’}


---

* [HBASE-17786](https://issues.apache.org/jira/browse/HBASE-17786) | *Major* | **Create LoadBalancer perf-tests (test balancer algorithm decoupled from workload)**

$ bin/hbase org.apache.hadoop.hbase.master.balancer.LoadBalancerPerformanceEvaluation -help
usage: hbase org.apache.hadoop.hbase.master.balancer.LoadBalancerPerformanceEvaluation \<options\>
Options:
 -regions \<arg\>         Number of regions to consider by load balancer. Default: 1000000
 -servers \<arg\>         Number of servers to consider by load balancer. Default: 1000
 -load\_balancer \<arg\>   Type of Load Balancer to use. Default:
                        org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer


---

* [HBASE-17887](https://issues.apache.org/jira/browse/HBASE-17887) | *Blocker* | **Row-level consistency is broken for read**

Now we pass on list of memstoreScanners to the StoreScanner along with the new files to ensure that the StoreScanner sees the latest memstore after flush.


---

* [HBASE-15296](https://issues.apache.org/jira/browse/HBASE-15296) | *Major* | **Break out writer and reader from StoreFile**

\<!-- mardown --\>
Refactor that breaks out StoreFile Reader and Writer inner classes as StoreFileReader and StoreFileWriter.

NOTE! Changes RegionObserver Coprocessor Interface so incompatible change (Discussed on dev list in thread "[Note breaking change on RegionObserver in hbase-2.0.0](https://s.apache.org/hbase-dev-note-about-HBASE-15296)"


---

* [HBASE-15199](https://issues.apache.org/jira/browse/HBASE-15199) | *Critical* | **Move jruby jar so only on hbase-shell module classpath; currently globally available**

The JRuby jar is no longer automatically included in classpaths for HBase server processes nor clients. It is still included in the classpath for the HBase shell and for invocations of org.jruby.Main, which should cover HBase provided support scripts.


---

* [HBASE-18009](https://issues.apache.org/jira/browse/HBASE-18009) | *Major* | **Move RpcServer.Call to a separated file**

The return value of CallRunner.getCall is changed so this is an incompatible change as CallRunner is declared as IA.LimitedPrivate. CallRunner is declared as IS.Evolving so we do not break the rule. And we still keep the getCall method to reduce the impact to user code.


---

* [HBASE-14925](https://issues.apache.org/jira/browse/HBASE-14925) | *Major* | **Develop HBase shell command/tool to list table's region info through command line**

Added a shell command 'list\_regions' for displaying the table's region info through command line.

        List all regions for a particular table as an array and also filter them by server name (optional) as prefix
        and maximum locality (optional). By default, it will return all the regions for the table with any locality.
        The command displays server name, region name, start key, end key, size of the region in MB, number of requests
        and the locality. The information can be projected out via an array as third parameter. By default all these information
        is displayed. Possible array values are SERVER\_NAME, REGION\_NAME, START\_KEY, END\_KEY, SIZE, REQ and LOCALITY. Values
        are not case sensitive. If you don't want to filter by server name, pass an empty hash / string as shown below.

        Examples:
        hbase\> list\_regions 'table\_name'
        hbase\> list\_regions 'table\_name', 'server\_name'
        hbase\> list\_regions 'table\_name', {SERVER\_NAME =\> 'server\_name', LOCALITY\_THRESHOLD =\> 0.8}
        hbase\> list\_regions 'table\_name', {SERVER\_NAME =\> 'server\_name', LOCALITY\_THRESHOLD =\> 0.8}, ['SERVER\_NAME']
        hbase\> list\_regions 'table\_name', {}, ['SERVER\_NAME', 'start\_key']
        hbase\> list\_regions 'table\_name', '', ['SERVER\_NAME', 'start\_key']


---

* [HBASE-17471](https://issues.apache.org/jira/browse/HBASE-17471) | *Critical* | **Region Seqid will be out of order in WAL if using mvccPreAssign**

MVCCPreAssign is added by HBASE-16698, but pre-assign mvcc is only used in put/delete path. Other write paths like increment/append still assign mvcc in ringbuffer's consumer thread. If put and increment are used parallel. Then seqid in WAL may not increase monotonically. Disorder in wals will lead to data loss.This patch bring all mvcc/seqid event in wal.append, and synchronize wal append and mvcc acquirement. No disorder in wal will happen. Performance test shows no regression with this patch.


---

* [HBASE-16466](https://issues.apache.org/jira/browse/HBASE-16466) | *Major* | **HBase snapshots support in VerifyReplication tool to reduce load on live HBase cluster with large tables**

Support for snapshots in VerifyReplication tool i.e. verifyrep can compare source table snapshot against peer table snapshot which reduces load on RS by reading data from HDFS directly using Snapshot scanners.
Instead of comparing against live tables whose state changes due to writes and compactions its better to compare HBase  snapshots which are immutable in nature.


---

* [HBASE-17263](https://issues.apache.org/jira/browse/HBASE-17263) | *Major* | **  Netty based rpc server impl**

A new RPC server based on Netty4 which can improve random read (get) performance. By default, it is off. To use this feature, please set “hbase.rpc.server.impl" to “org.apache.hadoop.hbase.ipc.NettyRpcServer”.

In one deploy, doubled the throughput and lowered the latency significantly: see https://www.slideshare.net/HBaseCon/lift-the-ceiling-of-hbase-throughputs?qid=597ee2fa-8125-4faa-bb3b-2bf1ba9ccafb&v=&b=&from\_search=6


---

* [HBASE-17957](https://issues.apache.org/jira/browse/HBASE-17957) | *Minor* | ** Custom metrics of replicate endpoints don't prepend "source." to global metrics**

Global custom metrics names follow the "source.metricsName" format.


---

* [HBASE-17757](https://issues.apache.org/jira/browse/HBASE-17757) | *Major* | **Unify blocksize after encoding to decrease memory fragment**

Blocksize is set in columnfamily's atrributes. It is used to control block sizes when generating blocks. But, it doesn't take encoding into count. If you set encoding to blocks, after encoding, the block size varies. Since blocks will be cached in memory after encoding (default), it will cause memory fragment if using blockcache, or decrease the pool efficiency if using bucketCache. This issue introduced a new config named 'hbase.writer.unified.encoded.blocksize.ratio'. The default value of this config is 1, meaning doing nothing. If this value is set to a smaller value like 0.5, and the blocksize is set to 64KB(default value of blocksize). It will unify the blocksize after encoding to 64KB \* 0.5 = 32KB. Unified blocksize will releaf the memory problems mentioned above.


---

* [HBASE-14286](https://issues.apache.org/jira/browse/HBASE-14286) | *Trivial* | **Correct typo in argument name for WALSplitter.writeRegionSequenceIdFile**

HBASE-14286 Correct typo in argument name for WALSplitter.writeRegionSequenceIdFile


---

* [HBASE-17817](https://issues.apache.org/jira/browse/HBASE-17817) | *Major* | **Make Regionservers log which tables it removed coprocessors from when aborting**

Add table name to exception logging when a coprocessor is removed from a table by the region server


---

* [HBASE-17877](https://issues.apache.org/jira/browse/HBASE-17877) | *Major* | **Improve HBase's byte[] comparator**

updated the lexicographic byte array comparator to use a slightly more optimized version similar to the one available in the guava library that compares only the first index where left[index] != right[index]. The comparator also returns the diff directly instead of mapping it to -1, 0, +1 range as was being done in the earlier version. We have seen significant performance gains, calculated in terms of throughput (ops/ms) with these changes ranging from approx 20% for smaller byte arrays upto 200 bytes and almost 100% for large byte array sizes that are in few KB's. We benchmarked with upto 16KB arrays and the general trend indicates that the performance improvement increases as the size of the byte array increases.


---

* [HBASE-9899](https://issues.apache.org/jira/browse/HBASE-9899) | *Major* | **for idempotent operation dups, return the result instead of throwing conflict exception**

Non-idempotent operations (increment/append/checkAndPut/...) may throw OperationConflictException even though the increment/append succeeded. For example (client rpc retries number set to 3):

1. first increment rpc request success
2. client timeout and send second rpc request, but nonce is same and save in server. The server found that it has already succeed, so return a OperationConflictException to make sure that increment operation only be applied once in server.

This patch will solve this problem by read the previous result when receive a duplicate rpc request.
1. Store the mvcc to OperationContext. When first rpc request succeed, store the mvcc for this operation nonce.
2. When there are duplicate rpc request, convert to read result by the mvcc.


---

* [HBASE-15583](https://issues.apache.org/jira/browse/HBASE-15583) | *Minor* | **Any HTableDescriptor we give out should be immutable**

# The HTD got from Admin, AsyncAdmin, and Table is immutable.
# DEFERRED\_LOG\_FLUSH is removed.
# cleanup the deprecated construction of HTD


---

* [HBASE-17956](https://issues.apache.org/jira/browse/HBASE-17956) | *Major* | **Raw scan should ignore TTL**

Now raw scan can also read expired cells.


---

* [HBASE-15143](https://issues.apache.org/jira/browse/HBASE-15143) | *Minor* | **Procedure v2 - Web UI displaying queues**

Adds a new Admin#listLocks, a panel on the procedures page to list procedure locks, and a list\_locks command to the shell. Use it to see current state of procedure locking in Master process.


---

* [HBASE-17514](https://issues.apache.org/jira/browse/HBASE-17514) | *Minor* | **Warn when Thrift Server 1 is configured for proxy users but not the HTTP transport**

If users of the Thrift 1 Server enable proxy user support without enabling the prerequisite HTTP transport, we now log a WARN message about the mismatch.


---

* [HBASE-17914](https://issues.apache.org/jira/browse/HBASE-17914) | *Major* | **Create a new reader instead of cloning a new StoreFile when compaction**

StoreFile.createReader method is gone. Call initReader and then getReader instead.


---

* [HBASE-16477](https://issues.apache.org/jira/browse/HBASE-16477) | *Major* | **Remove Writable interface and related code from WALEdit/WALKey**

Removes the Writables, and related code from WALEdit class. HBase-2.0 will not be able to read WAL files written with 0.94.x and before.


---

* [HBASE-17858](https://issues.apache.org/jira/browse/HBASE-17858) | *Major* | **Update refguide about the IS annotation if necessary**

Updated refguide to tell users that IS annotation is only valid for IA.LimitedPrivate classes.


---

* [HBASE-17857](https://issues.apache.org/jira/browse/HBASE-17857) | *Major* | **Remove IS annotations from IA.Public classes**

Now we do not have InterfaceStability annotations for IA,Public API. The stability of these classes will follow the rule of 'Semantic Versioning'.


---

* [HBASE-17215](https://issues.apache.org/jira/browse/HBASE-17215) | *Major* | **Separate small/large file delete threads in HFileCleaner to accelerate archived hfile cleanup speed**

After HBASE-17215 we change to use two threads for (archived) hfile cleaning. The size throttling for large/small files could be set through "hbase.regionserver.thread.hfilecleaner.throttle" and default to 67108864 (64M). It supports online configuration change, just find the active master address through zookeeper dump and use it in update\_config command, e.g. update\_config 'hbasem1.et2.tbsite.net,60100,1488038696741'


---

* [HBASE-16780](https://issues.apache.org/jira/browse/HBASE-16780) | *Critical* | **Since move to protobuf3.1, Cells are limited to 64MB where previous they had no limit**

Upgrade internal pb to 3.2 from 3.1. 3.2 has fix for 64MB limit.


---

* [HBASE-17287](https://issues.apache.org/jira/browse/HBASE-17287) | *Blocker* | **Master becomes a zombie if filesystem object closes**

If filesystem is not available during log split, abort master server.


---

* [HBASE-17765](https://issues.apache.org/jira/browse/HBASE-17765) | *Major* | **Reviving the merge possibility in the CompactingMemStore**

Reviving the merge of the compacting pipeline: making the limit on the number of the segments in the pipeline configurable and adding the merge test.

In order to customize the pipeline size limit change the value of the "hbase.hregion.compacting.pipeline.segments.limit" in the hbase-site.xml

Value 1 means to merge the segments on any flush-in-memory. Value higher than 16 means no merge.


---

* [HBASE-13395](https://issues.apache.org/jira/browse/HBASE-13395) | *Major* | **Remove HTableInterface**

HTableInterface was deprecated in 0.21.0 and is removed in 2.0.0. Use org.apache.hadoop.hbase.client.Table instead.


---

* [HBASE-17595](https://issues.apache.org/jira/browse/HBASE-17595) | *Critical* | **Add partial result support for small/limited scan**

Now small scan and limited scan could also return partial results.


---

* [HBASE-16014](https://issues.apache.org/jira/browse/HBASE-16014) | *Major* | **Get and Put constructor argument lists are divergent**

Add 2 constructors fot API Get
1. Get(byte[], int, int)
2. Get(ByteBuffer)


---

* [HBASE-17584](https://issues.apache.org/jira/browse/HBASE-17584) | *Major* | **Expose ScanMetrics with ResultScanner rather than Scan**

Now you can use ResultScanner.getScanMetrics to get the scan metrics at any time during the scan operation. The old Scan.getScanMetrics is deprecated and still work, but if you use ResultScanner.getScanMetrics to get the scan metrics and reset it, then the metrics published to the Scan instaince will be messed up.


---

* [HBASE-17802](https://issues.apache.org/jira/browse/HBASE-17802) | *Major* | **Add note that minor versions can add methods to Interfaces**

Update our semver section to include a note on our allowing ourselves the right to add methods to an Interface over a minor version as agreed to up on the dev list:  "If a Client implements an HBase Interface, a recompile MAY be required upgrading to a newer minor version (See release notes for warning about incompatible changes). All effort will be made to provide a default implementation so this case should not arise."


---

* [HBASE-17426](https://issues.apache.org/jira/browse/HBASE-17426) | *Major* | **Inconsistent environment variable names for enabling JMX**

In bin/hbase-config.sh,
if value for HBASE\_JMX\_BASE is empty, keep current behavior.
if HBASE\_JMX\_OPTS is not empty, keep current behavior.
otherwise use the value of HBASE\_JMX\_BASE


---

* [HBASE-17740](https://issues.apache.org/jira/browse/HBASE-17740) | *Critical* | **Correct the semantic of batch and partial for async client**

Now async client has the same semantic with sync client for batch and partial.
'''
Now setBatch doesn't mean setAllowPartialResult(true)
If user setBatch(5) and rpc returns 3+5+5+5+3 cells, we should return 5+5+5+5+1 to user.
'''

Also a minor API change:
Result#createCompleteResult(List\<Result\>) is changed to Result#createCompleteResult(Iterable\<Result\>).


---

* [HBASE-17746](https://issues.apache.org/jira/browse/HBASE-17746) | *Major* | **TestSimpleRpcScheduler.testCoDelScheduling is broken**

The executor for CoDel is changed to FastPathBalancedQueueRpcExecutor


---

* [HBASE-17712](https://issues.apache.org/jira/browse/HBASE-17712) | *Major* | **Remove/Simplify the logic of RegionScannerImpl.handleFileNotFound**

Add a config named 'hbase.hregion.unassign.for.fnfe'. It is used to control whether to reopen a region when hitting FileNotFoundException. The default value is true.


---

* [HBASE-15941](https://issues.apache.org/jira/browse/HBASE-15941) | *Major* | **HBCK repair should not unsplit healthy splitted region**

A new option -removeParents is now available that will remove an old parent when two valid daughters for that parent exist and -fixHdfsOverlaps is used. If there is an issue trying to remove the parent from META or sidelining the parent from HDFS we will fallback to do a regular merge. For now this option only works when the overlap group consists only of 3 regions (a parent, daughter A and daughter B)


---

* [HBASE-17737](https://issues.apache.org/jira/browse/HBASE-17737) | *Major* | **Thrift2 proxy should support scan timeRange per column family**

Thrift2 proxy supports scan timeRange per column family


---

* [HBASE-17718](https://issues.apache.org/jira/browse/HBASE-17718) | *Major* | **Difference between RS's servername and its ephemeral node cause SSH stop working**

Fix our accidentally registering a RegionServer's ephermal znode BEFORE we checked in with the master.


---

* [HBASE-17717](https://issues.apache.org/jira/browse/HBASE-17717) | *Critical* | **Incorrect ZK ACL set for HBase superuser**

In previous versions of HBase, the system intended to set a ZooKeeper ACL on all "sensitive" ZNodes for the user specified in the hbase.superuser configuration property. Unfortunately, the ACL was malformed which resulted in the hbase.superuser being unable to access the sensitive ZNodes that HBase creates. This JIRA issue fixes this bug. HBase will automatically correct the ACLs on start so users do not need to manually correct the ACLs.


---

* [HBASE-17716](https://issues.apache.org/jira/browse/HBASE-17716) | *Minor* | **Formalize Scan Metric names**

HBASE-17716 breaks compatibility of ServerSideScanMetrics by changing public field names, and the issue is fixed through HBASE-17886


---

* [HBASE-15484](https://issues.apache.org/jira/browse/HBASE-15484) | *Blocker* | **Correct the semantic of batch and partial**

Now setBatch doesn't mean setAllowPartialResult(true)
If user setBatch(5) and rpc returns 3+5+5+5+3 cells, we should return 5+5+5+5+1 to user.
Scan#setBatch is helpful in paging queries, if you just want to prevent OOM at client, use setAllowPartialResults(true) is better.
We deprecated isPartial and use mayHaveMoreCellsInRow. If it returns false, current Result must be the last one of this row.


---

* [HBASE-17312](https://issues.apache.org/jira/browse/HBASE-17312) | *Major* | **[JDK8] Use default method for Observer Coprocessors**

Deletes BaseMasterAndRegionObserver, BaseMasterObserver, BaseRegionObserver, BaseRegionServerObserver and BaseWALObserver.
Their corresponding interface classes now use JDK8's 'default' keyword to provide empty/no-op implementations so that:
1. Derived class don't break when more coprocessor hooks are added in future.
2. Derived classes don't have to redundantly override functions they don't care about with empty implementations.

Earlier, BaseXXXObserver classes provided these exact two benefits, but with 'default' keyword in JDK8, they are not needed anymore.

To fix the breakages because of this change, simply change "Foo extends BaseXXXObserver" to "Foo implements XXXObserver".


---

* [HBASE-17647](https://issues.apache.org/jira/browse/HBASE-17647) | *Major* | **OffheapKeyValue#heapSize() implementation is wrong**

**WARNING: No release note provided for this change.**


---

* [HBASE-13718](https://issues.apache.org/jira/browse/HBASE-13718) | *Minor* | **Add a pretty printed table description to the table detail page of HBase's master**

<!-- markdown -->


The table information page in the Master UI now includes a schema section that describes the column families defined for that table as well as any column family specific properties that are set.


---

* [HBASE-17472](https://issues.apache.org/jira/browse/HBASE-17472) | *Major* | **Correct the semantic of  permission grant**

Before this patch, later granted permissions will override previous granted permissions, and previous granted permissions LOST. this issue re-define grant semantic: for master branch, later granted permissions will merge with previous granted permissions.  for branch-1.4, grant keep override behavior for compatibility purpose, and a grant with mergeExistingPermission flag provided.


---

* [HBASE-17583](https://issues.apache.org/jira/browse/HBASE-17583) | *Major* | **Add inclusive/exclusive support for startRow and endRow of scan for sync client**

Now you can include/exlude the startRow and stopRow for a scan. And the new methods to specify startRow and stopRow are withStartRow and withStopRow. The old methods to specify startRow and Row(include constructors) are marked as deprecated as in the old time if startRow and stopRow are equal then we will consider it as a get scan and include the stopRow implicitly. This is strange after we can set inclusiveness explicitly so we add new methods and depredate the old methods. The deprecated methods will be removed in the future.


---

* [HBASE-9702](https://issues.apache.org/jira/browse/HBASE-9702) | *Major* | **Change unittests that use "table" or "testtable" to use method names.**

Changes all tests to use the TestName JUnit Rule everywhere rather than hardcode table/region/store names.


---

* [HBASE-17280](https://issues.apache.org/jira/browse/HBASE-17280) | *Minor* | **Add mechanism to control hbase cleaner behavior**

The HBase cleaner chore process cleans up old WAL files and archived HFiles. Cleaner operation can affect query performance when running heavy workloads, so disable the cleaner during peak hours. The cleaner has the following HBase shell commands:

- cleaner\_chore\_enabled: Queries whether cleaner chore is enabled/ disabled.
- cleaner\_chore\_run: Manually runs the cleaner to remove files.
- cleaner\_chore\_switch: enables or disables the cleaner and returns the previous state of the cleaner. For example, cleaner-switch true enables the cleaner.

Following APIs are added in Admin:
- setCleanerChoreRunning(boolean on): Enable/Disable the cleaner chore
- runCleanerChore(): Ask for cleaner chore to run
- isCleanerChoreEnabled(): Query whether cleaner chore is enabled/ disabled.


---

* [HBASE-17599](https://issues.apache.org/jira/browse/HBASE-17599) | *Major* | **Use mayHaveMoreCellsInRow instead of isPartial**

The word 'isPartial' is ambiguous so we introduce a new method 'mayHaveMoreCellsInRow' to replace it. And the old meaning of 'isPartial' is not the same with 'mayHaveMoreCellsInRow' as for batched scan, if the number of returned cells equals to the batch, isPartial will be false. After this change the meaning of 'isPartial' will be same with 'mayHaveMoreCellsInRow'. This is an incompatible change but it is not likely to break a lot of things as for batched scan the old 'isPartial' is just a redundant information, i.e, if the number of returned cells reaches the batch limit. You have already know the number of returned cells and the value of batch.


---

* [HBASE-17437](https://issues.apache.org/jira/browse/HBASE-17437) | *Major* | **Support specifying a WAL directory outside of the root directory**

This patch adds support for specifying a WAL directory outside of the HBase root directory.

Multiple configuration variables were added to accomplish this:
hbase.wal.dir: used to configure where the root WAL directory is located. Could be on a different FileSystem than the root directory. WAL directory can not be set to a subdirectory of the root directory. The default value of this is the root directory if unset.

hbase.rootdir.perms: Configures FileSystem permissions to set on the root directory. This is '700' by default.

hbase.wal.dir.perms: Configures FileSystem permissions to set on the WAL directory FileSystem. This is '700' by default.


---

* [HBASE-17350](https://issues.apache.org/jira/browse/HBASE-17350) | *Critical* | **Fixup of regionserver group-based assignment**

A few bug fixes and tweaks to the fsgroup feature.

Renamed shell command move\_rsgroup\_servers as move\_servers\_rsgroup
Renamed shell comand move\_rsgroup\_tables as move\_tables\_rsgroup

Made the 'default' group more 'dynamic'; i.e. dead servers no longer show in the 'default' group.


---

* [HBASE-17578](https://issues.apache.org/jira/browse/HBASE-17578) | *Major* | **Thrift per-method metrics should still update in the case of exceptions**

In prior versions, the HBase Thrift handlers failed to increment per-method metrics when an exception was encountered.  These metrics will now always be incremented, whether an exception is encountered or not.  This change also adds exception-type metrics, similar to those exposed in regionservers, for individual exceptions which are received by the Thrift handlers.


---

* [HBASE-17508](https://issues.apache.org/jira/browse/HBASE-17508) | *Major* | **Unify the implementation of small scan and regular scan for sync client**

Now the scan.setSmall method is deprecated. Consider using scan.setLimit and scan.setReadType in the future. And we will open scanner lazily when you call scanner.next. This is an incompatible change which delays the table existence check and permission check.


---

* [HBASE-16981](https://issues.apache.org/jira/browse/HBASE-16981) | *Major* | **Expand Mob Compaction Partition policy from daily to weekly, monthly**

Mob compaction partition policy can be set by
hbase\> create 't1', {NAME =\> 'f1', IS\_MOB =\> true, MOB\_THRESHOLD =\> 1000000, MOB\_COMPACT\_PARTITION\_POLICY =\> 'weekly'}

or

hbase\> alter 't1', {NAME =\> 'f1', IS\_MOB =\> true, MOB\_THRESHOLD =\> 1000000, MOB\_COMPACT\_PARTITION\_POLICY =\> 'monthly'}

Available MOB\_COMPACT\_PARTITION\_POLICY options are "daily", "weekly" and "monthly", the default is "daily".

When it is "weekly" policy, the mob compaction will try to compact files within one calendar week into one for a specific partition, similar for "daily" and "monthly".

With "weekly" policy, one mob file normally is compacted twice during its lifetime (that is first on daily basis and then all such daily based compacted files belonging to a week at the weekly interval), for one region, there normally are 52 files for one year. With "Monthly" policy, one mob file normally is compacted 3 times during its lifetime (First daily and then weekly followed by monthly at end of every month) and normally there are 12 files for one year.


---

* [HBASE-17197](https://issues.apache.org/jira/browse/HBASE-17197) | *Major* | **hfile does not work in 2.0**

The -f argument is no longer required specifying target file; just pass the file as an argument.


---

* [HBASE-16812](https://issues.apache.org/jira/browse/HBASE-16812) | *Minor* | **Clean up the locks in MOB**

In MOB-enabled column family, the lock in the major compaction is removed. All the delete markers are retained in the major compaction, and a MOB reference tag is appended to each of the retained delete markers.


---

* [HBASE-12894](https://issues.apache.org/jira/browse/HBASE-12894) | *Critical* | **Upgrade Jetty to 9.2.6**

Upgrades Jetty to 9.x from 6.x (Jetty9 is in different namespace from Jetty6). Also updated Jersey to 2.x and Servlet to 3.x.


---

* [HBASE-17566](https://issues.apache.org/jira/browse/HBASE-17566) | *Major* | **Jetty upgrade fixes**

Fix inability at finding static content post push of parent issue moving us to jetty9.


---

* [HBASE-9774](https://issues.apache.org/jira/browse/HBASE-9774) | *Major* | **HBase native metrics and metric collection for coprocessors**

This issue adds two new modules, hbase-metrics and hbase-metrics-api which define and implement the "new" metric system used internally within HBase. These two modules (and some other code in hbase-hadoop2-compat) module are referred as "HBase metrics framework" which is HBase-specific and independent of any other metrics library (including Hadoop metrics2 and dropwizards metrics).

HBase Metrics API (hbase-metrics-api) contains the interface that HBase exposes internally and to third party code (including coprocessors). It is a thin
abstraction over the actual implementation for backwards compatibility guarantees. The metrics API in this hbase-metrics-api module is inspired by the Dropwizard metrics 3.1 API, however, the API is completely independent.

hbase-metrics module contains implementation of the "HBase Metrics API", including MetricRegistry, Counter, Histogram, etc. These are highly concurrent implementations of the Metric interfaces. Metrics in HBase are grouped into different sets (like WAL, RPC, RegionServer, etc). Each group of metrics should be tracked via a MetricRegistry specific to that group.

Historically, HBase has been using Hadoop's Metrics2 framework [3] for collecting and reporting the metrics internally. However, due to the difficultly of dealing with the Metrics2 framework, HBase is moving away from Hadoop's metrics implementation to its custom implementation. The move will happen incrementally, and during the time, both Hadoop Metrics2-based metrics and hbase-metrics module based classes will be in the source code. All new implementations for metrics SHOULD use the new API and framework.

This jira also introduces the metrics API to coprocessor implementations. Coprocessor writes can export custom metrics using the API and have those collected via metrics2 sinks, as well as exported via JMX in regionserver metrics.

More documentation available at: hbase-metrics-api/README.txt


---

* [HBASE-17491](https://issues.apache.org/jira/browse/HBASE-17491) | *Major* | **Remove all setters from HTable interface and introduce a TableBuilder to build Table instance**

After HBASE-17491 all setter methods in HTable are marked as deprecated, moved into TableBuilder, and will be removed later.


---

* [HBASE-17067](https://issues.apache.org/jira/browse/HBASE-17067) | *Major* | **Procedure v2 - remove tryAcquire\*Lock and use wait/wake to make framework event based**

Make the framework more 'lively'; undo 'suspend' notion in Procedure, rely on eventing mechanism instead. Lets us remove no longer needed synchronizations. Framework can now do more ops per second.


---

* [HBASE-16698](https://issues.apache.org/jira/browse/HBASE-16698) | *Major* | **Performance issue: handlers stuck waiting for CountDownLatch inside WALKey#getWriteEntry under high writing workload**

Assign sequenceid to an edit before we go on the ringbuffer; undoes contention on WALKey latch. Adds a new config "hbase.hregion.mvcc.preassign" which defaults to true: i.e. this speedup is enabled.

User could set this per-table level, like:
create 'table',{NAME=\>'f1',CONFIGURATION=\>{'hbase.hregion.mvcc.preassign'=\>'false'}}


---

* [HBASE-17488](https://issues.apache.org/jira/browse/HBASE-17488) | *Trivial* | **WALEdit should be lazily instantiated**

prevent creating unused objects in the WALEdit's construction.
+If the cp#preBatchMutate returns true, the WALEdit is useless. So we should create the WALEdit after step 2.
+The cells came from cp should be counted because they are added into the WALEdit . The use case is the local index of phoenix
+If the mutation contains the SKIP\_WAL property, its cells aren't added into the WALEdit. So these cells shouldn't be counted.


---

* [HBASE-16831](https://issues.apache.org/jira/browse/HBASE-16831) | *Minor* | **Procedure V2 - Remove org.apache.hadoop.hbase.zookeeper.lock**

Purges code that did zk-hosted locks for table ops (we do procedure-based locks now)


---

* [HBASE-16867](https://issues.apache.org/jira/browse/HBASE-16867) | *Major* | **Procedure V2 - Check ACLs for remote HBaseLock**

Add checking ACL when taking locks.


---

* [HBASE-16786](https://issues.apache.org/jira/browse/HBASE-16786) | *Major* | **Procedure V2 - Move ZK-lock's uses to Procedure framework locks (LockProcedure)**

Move locking to be procedure (Pv2) rather than zookeeper based. All locking moved over to new infrastructure including MOBing locking.


---

* [HBASE-17470](https://issues.apache.org/jira/browse/HBASE-17470) | *Major* | **Remove merge region code from region server**

In 1.x branches, Admin.mergeRegions calls MASTER via dispatchMergingRegions RPC; when executing dispatchMergingRegions RPC, MASTER calls RS via MergeRegions to complete the merge in RS-side.

With HBASE-16119, the merge logic moves to master-side.  This JIRA cleans up unused RPCs (dispatchMergingRegions and MergeRegions) , removes dangerous tools such as Merge and HMerge, and deletes unused RegionServer-side merge region logic in 2.0 release.


---

* [HBASE-16744](https://issues.apache.org/jira/browse/HBASE-16744) | *Major* | **Procedure V2 - Lock procedures to allow clients to acquire locks on tables/namespaces/regions**

 Lock for HBase Entity either a Table, a Namespace, or Regions.

These are remote locks which live on master, and need periodic heartbeats to keep them alive. (Once we request the lock, internally an heartbeat thread will be started). If master doesn't receive the heartbeat in time, it'll release the lock and make it available to other users.

Use {@link LockServiceClient} to build instances. Then call {@link #requestLock()}. {@link #requestLock} will contact master to queue the lock and start the heartbeat thread which will check lock's status periodically and once the lock is acquired, it will send the heartbeats to the master.

Use {@link #await} or {@link #await(long, TimeUnit)} to wait for the lock to be acquired. Always call {@link #unlock()} irrespective of whether lock was acquired or not. If the lock was acquired, it'll be released. If it was not acquired, it is possible that master grants the lock in future and the heartbeat thread keeps it alive forever by sending heartbeats. Calling {@link #unlock()} will stop the heartbeat thread and cancel the lock queued on master.

There are 4 ways in which these remote locks may be released/can be lost:
  \* Call {@link #unlock}.
  \* Lock times out on master: Can happen because of network issues, GC pauses, etc. Worker thread will call the given abortable as soon as it detects such a situation. Fail to contact master: If worker thread can not contact mater and thus fails to send heartbeat before the timeout expires, it assumes that lock is lost and calls the
 \*     abortable.
Worker thread is interrupted.

Use example:

 EntityLock lock = lockServiceClient.\*Lock(...., "exampled lock", abortable);
  lock.requestLock();
  ....
   ....can do other initializations here since lock is 'asynchronous'...
 ....
 if (lock.await(timeout)) {
    ....logic requiring mutual exclusion
  }
   lock.unlock();


---

* [HBASE-14061](https://issues.apache.org/jira/browse/HBASE-14061) | *Major* | **Support CF-level Storage Policy**

After HBASE-14061 we support to set storage policy for HFile through "hbase.hstore.block.storage.policy" configuration, and we support CF-level setting to override the settings from configuration file. Currently supported storage policies include ALL\_SSD/ONE\_SSD/HOT/WARM/COLD, refer to http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html for more details

For example, to create a table with two families: "cf1" with "ALL\_SSD" storage policy and "cf2" with "ONE\_SSD", we could use below command in hbase shell:
create 'table',{NAME=\>'f1',STORAGE\_POLICY=\>'ALL\_SSD'},{NAME=\>'f2',STORAGE\_POLICY=\>'ONE\_SSD'}

We could also set the configuration in table attribute like all other configurations:
create 'table',{NAME=\>'f1',CONFIGURATION=\>{'hbase.hstore.block.storage.policy'=\>'ONE\_SSD'}}


---

* [HBASE-17337](https://issues.apache.org/jira/browse/HBASE-17337) | *Major* | **list replication peers request should be routed through master**

List replication peers request will be roughed through master.


---

* [HBASE-15172](https://issues.apache.org/jira/browse/HBASE-15172) | *Major* | **Support setting storage policy in bulkload**

After HBASE-15172/HBASE-19016 we could set storage policy through "hbase.hstore.block.storage.policy" property for bulkload, or "hbase.hstore.block.storage.policy.\<family\_name\>" for a specified family. Supported storage policy includes: ALL\_SSD, ONE\_SSD, HOT, WARM, COLD, etc.


---

* [HBASE-17336](https://issues.apache.org/jira/browse/HBASE-17336) | *Major* | **get/update replication peer config requests should be routed through master**

Get/update replication peer config requests will be routed through master.


---

* [HBASE-17320](https://issues.apache.org/jira/browse/HBASE-17320) | *Major* | **Add inclusive/exclusive support for startRow and endRow of scan**

Now you can specific the inclusive of startRow and stopRow for a scan using the new methods withStartRow(byte[] startRow, boolean inclusive) and withStopRow(byte[] stopRow, boolean inclusive). The old setStartRow and setStopRow methods, and the constructors are marked as deprecated because of an strange behavior that we will include the stopRow implicitly if startRow equals to stopRow. This is used to support get scan in the old time. Use withStartRow and withStopRow instead.

For developers, the ConnectionUtils.createClosestRowBefore is also marked as deprecated as the row returned by this method is only very very close to the current row, not closest. Avoid using this method in the future.


---

* [HBASE-17314](https://issues.apache.org/jira/browse/HBASE-17314) | *Major* | **Limit total buffered size for all replication sources**

Add a conf "replication.total.buffer.quota" to limit total size of buffered entries in all replication peers. It will prevent server getting OOM if there are many peers. Default value is 256MB.


---

* [HBASE-17174](https://issues.apache.org/jira/browse/HBASE-17174) | *Minor* | **Refactor the AsyncProcess, BufferedMutatorImpl, and HTable**

+ cleanup some unused code
+ allow being able to share pool between BufferedMutatorImpl
+ setting "hbase.client.request.controller.impl" to the name of the alternate RequestController (traffic control) implementation class in Configuration
+ The default RequestController implementation is SimpleRequestController
+ setting "hbase.client.log.detail.period.ms" to call logger on a period when waiting for tasks to complete


---

* [HBASE-17335](https://issues.apache.org/jira/browse/HBASE-17335) | *Major* | **enable/disable replication peer requests should be routed through master**

Enable/Disable replication peer requests will be routed through master.


---

* [HBASE-5401](https://issues.apache.org/jira/browse/HBASE-5401) | *Major* | **PerformanceEvaluation generates 10x the number of expected mappers**

Changes how many tasks PE runs when clients are mapreduce. Now tasks == client count. Previous we hardcoded ten tasks per client instance.


---

* [HBASE-11392](https://issues.apache.org/jira/browse/HBASE-11392) | *Critical* | **add/remove peer requests should be routed through master**

Add/Remove replication peer requests will be routed through master. And make ReplicationAdmin as Deprecated.


---

* [HBASE-15924](https://issues.apache.org/jira/browse/HBASE-15924) | *Major* | **Enhance hbase services autorestart capability to hbase-daemon.sh**

Now one can start hbase services with enabled "autostart/autorestart" feature in controlled fashion with the help of "--autostart-window-size" to define the window period and the "--autostart-window-retry-limit" to define the number of times the hbase services have to be restarted upon being killed/terminated abnormally within the provided window perioid.

The following cases are supported with "autostart/autorestart":

a) --autostart-window-size=0 and --autostart-window-retry-limit=0, indicates infinite window size and no retry limit
b) not providing the args, will default to a)
c) --autostart-window-size=0 and --autostart-window-retry-limit=\<positive value\> indicates the autostart process to bail out if the retry limit exceeds irrespective of window period
d) --autostart-window-size=\<x\> and --autostart-window-retry-limit=\<y\> indicates the autostart process to bail out if the retry limit "y" is exceeded for the last window period "x".


---

* [HBASE-17331](https://issues.apache.org/jira/browse/HBASE-17331) | *Minor* | **Avoid busy waiting in ThrottledInputStream**

For each read(), old ThrottledInputStream sleeps/wakes/checks for many times for controlling the throughput. After this patch, ThrottledInputStream sleeps/wakes/checks only once. So we can reduce CPU usage.


---

* [HBASE-17296](https://issues.apache.org/jira/browse/HBASE-17296) | *Major* | **Provide per peer throttling for replication**

Provide per peer throttling for replication. Add the bandwidth upper limit to ReplicationPeerConfig and a new shell cmd set\_peer\_bandwidth to update the bandwidth in need.


---

* [HBASE-17277](https://issues.apache.org/jira/browse/HBASE-17277) | *Major* | **Allow alternate BufferedMutator implementation**

Specify the name of an alternate BufferedMutator implementation by either:

 \* Setting "hbase.client.bufferedmutator.classname" to the name of the alternate implementation class in Configuration
 \* Or, by setting BufferedMutatorParams#implementationClassName and passing the amended BufferedMutatorParams when calling Connection#getBufferedMutator.


---

* [HBASE-17294](https://issues.apache.org/jira/browse/HBASE-17294) | *Major* | **External Configuration for Memory Compaction**

This patch provides a single external knob to control memstore compaction. It also inmemory compaction with BASIC policy as our default (AFTERWORD: inmemory compaction as default was undone in HBASE-17333 because of test failures; will be reenabled in later, dedicated issue)

Possible memstore compaction policies are:
(1) None - no memory compaction, when size threshold is exceeded data is flushed to disk
(2) Basic policy applies optimizations which modify the index to a more compacted representation. This is beneficial in all access patterns. The smaller the cells are the greater the benefit of this policy. This is the default policy.
(3) Eager - in addition to compacting the index representation as the basic policy, eager policy eliminates duplication while the data is still in memory (much like the on-disk compaction does after the data is flushed to disk). This policy is most useful for applications with high data churn or small working sets.

Memory compaction policeman be set at the column family level at table creation time:
{code}
create ‘\<tablename\>’,
   {NAME =\> ‘\<cfname\>’,
    IN\_MEMORY\_COMPACTION =\> ‘\<NONE\|BASIC\|EAGER\>’}
{code}
or as a property at the global configuration level by setting the property in hbase-site.xml, with BASIC being the default value:
{code}
\<property\>
	\<name\>hbase.hregion.compacting.memstore.type\</name\>
	\<value\>\<NONE\|BASIC\|EAGER\>\</value\>
\</property\>
{code}
The values used in this property can change as memstore compaction policies evolve over time.


---

* [HBASE-16336](https://issues.apache.org/jira/browse/HBASE-16336) | *Major* | **Removing peers seems to be leaving spare queues**

Add a ReplicationZKNodeCleaner periodically check and delete the useless replication queue zk node belong to the peer which is not exist.


---

* [HBASE-17272](https://issues.apache.org/jira/browse/HBASE-17272) | *Major* | **Doc how to run Standalone HBase over an HDFS instance; all daemons in one JVM but persisting to an HDFS instance**

Adds section at http://hbase.apache.org/book.html#standalone.over.hdfs on how to make standalone persist to an hdfs instance (where standalone is all daemons in the one jvm).


---

* [HBASE-16700](https://issues.apache.org/jira/browse/HBASE-16700) | *Minor* | **Allow for coprocessor whitelisting**

Provides ability to restrict table coprocessors based on HDFS path whitelist. (Particularly useful for allowing Phoenix coprocessors but not arbitrary user created coprocessors.)


---

* [HBASE-17221](https://issues.apache.org/jira/browse/HBASE-17221) | *Major* | **Abstract out an interface for RpcServer.Call**

Provide an interface RpcCall on the server side.
RpcServer.Call now is marked as @InterfaceAudience.Private, and implements the interface RpcCall,


---

* [HBASE-16119](https://issues.apache.org/jira/browse/HBASE-16119) | *Major* | **Procedure v2 - Reimplement merge**

The merge region logic is controlled by master in 2.0.0 (in 1.x, the core merge region logic is in the region server side).  The coprocessors related to merge region in RS-side would be no-op in 2.0.0 and later release.  Therefore, this is an incompatible change.  Users needs to move the CP logic to new master CP and registers them.

A new mergeRegionsAsync() API is added in client.  The existing mergeRegions() API will call the new API so client does not have to change its code.


---

* [HBASE-17112](https://issues.apache.org/jira/browse/HBASE-17112) | *Major* | **Prevent setting timestamp of delta operations the same as previous value's**

Before this issue, two concurrent Increments/Appends done in same millisecond or RS's clock going back will result in two results have same TS, which is not friendly to versioning and will get wrong result in slave cluster if the replication is disordered.
After this issue, the result of Increment/Append will always have an incremental TS. There is no any inconsistent in replication for these operations. But there is a rare case that if there is a Delete in same millisecond, the later result can not be masked by this Delete. This can be fixed after we have new semantics that previous Delete will never mask later Put even its timestamp is higher.


---

* [HBASE-17181](https://issues.apache.org/jira/browse/HBASE-17181) | *Minor* | **Let HBase thrift2 support TThreadedSelectorServer**

Add TThreadedSelectorServer support for HBase Thrift2


---

* [HBASE-17178](https://issues.apache.org/jira/browse/HBASE-17178) | *Major* | **Add region balance throttling**

Add region balance throttling. Master execute every region balance plan per balance interval, which is equals to divide max balancing time by the size of region balance plan. And Introduce a new config hbase.master.balancer.maxRitPercent to protect availability. If config this to 0.01, then the max percent of regions in transition is 1% when balancing. Then the cluster's availability is at least 99% when balancing.


---

* [HBASE-15786](https://issues.apache.org/jira/browse/HBASE-15786) | *Major* | **Create DBB backed MSLAB pool**

Added a new config hbase.regionserver.offheap.global.memstore.size using which one can specify the global off heap limit that all memstores can use.  When this config is in MSLAB should be turned ON and we will use the entire size for the MSLAB pool. It will make off heap chunks and pool then. It will behave as if we are working with off heap memstores.  When this config is having a valid value and MSLAB is turned OFF, the system will just ignore the offheap size and continue to use global max heap space % for memstores and work with on heap memstores.


---

* [HBASE-17132](https://issues.apache.org/jira/browse/HBASE-17132) | *Major* | **Cleanup deprecated code for WAL**

Remove HLogKey and related classes and methods. Remove SequenceFile based log reader and writer. WALObserver and RegionObserver are changed so this is an incompatible change.


---

* [HBASE-16169](https://issues.apache.org/jira/browse/HBASE-16169) | *Major* | **Make RegionSizeCalculator scalable**

Added couple of API's to Admin.java:

Returns region load map of all regions hosted on a region server
Map\<byte[], RegionLoad\> getRegionLoad(ServerName sn) throws IOException;

Returns region load map of all regions of a table hosted on a region server
Map\<byte[], RegionLoad\> getRegionLoad(ServerName sn, TableName tableName) throws IOException

Added an API to region server:

public GetRegionLoadResponse getRegionLoad(RpcController controller,
    GetRegionLoadRequest request) throws ServiceException;

Primary intention is to use this API for RegionSizeCalculator and not rely on Master for ClusterStatus. On large clusters, ClusterStatus() can take a long time. IfMaster is down/busy, then some of the jobs timeout/fail. Other possible uses:
1. If there is a lighter version of GetClusterStatus API (i.e without the ServerLoad for each RS), then custom maintenance tools can be better. In current world ClusterStatus is heavy. With the new APIs, each API's payload is smaller and distributed. So custom tools can call getRegionLoad() when needed, it will be more accurate. This helps with large clusters. For tools that don't need RegionLoad, the lighter version of API is fine enough.
2. Another use case is a tool like RSTop - since we can see selective metrics at RegionLevel (possibly even deltas between each RPC to the server).


---

* [HBASE-15788](https://issues.apache.org/jira/browse/HBASE-15788) | *Major* | **Use Offheap ByteBuffers from BufferPool to read RPC requests.**

Using the ByteBuffers from ByteBufferPool to read the request bytes at server.  When the size of the request is smaller than 1/6th size of a BB in the pool, we will not use that but read into an on demand created, proper sized on heap ByteBuffer.


---

* [HBASE-17046](https://issues.apache.org/jira/browse/HBASE-17046) | *Major* | **Add 1.1 doc to hbase.apache.org**

Adds a 1.1. item to our 'Documentation and API' tab. Gives access to 1.1 APIs, XRef, etc.


---

* [HBASE-16962](https://issues.apache.org/jira/browse/HBASE-16962) | *Major* | **Add readPoint to preCompactScannerOpen() and preFlushScannerOpen() API**

The following RegionObserver methods are deprecated

InternalScanner preFlushScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
    final Store store, final KeyValueScanner memstoreScanner, final InternalScanner s)
    throws IOException;

InternalScanner preCompactScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
    final Store store, List\<? extends KeyValueScanner\> scanners, final ScanType scanType,
    final long earliestPutTs, final InternalScanner s, CompactionRequest request)

Instead, use the following methods:

InternalScanner preFlushScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
    final Store store, final KeyValueScanner memstoreScanner, final InternalScanner s,
    final long readPoint) throws IOException;

InternalScanner preCompactScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
    final Store store, List\<? extends KeyValueScanner\> scanners, final ScanType scanType,
    final long earliestPutTs, final InternalScanner s, final CompactionRequest request,
    final long readPoint) throws IOException


---

* [HBASE-17017](https://issues.apache.org/jira/browse/HBASE-17017) | *Major* | **Remove the current per-region latency histogram metrics**

Removes per-region level (get size, get time, scan size and scan time histogram) metrics that was exposed before. Per-region histogram metrics with 1000+ regions causes millions of objects to be allocated on heap. The patch introduces getCount and scanCount as counters rather than histograms. Other per-region level metrics are kept as they are.


---

* [HBASE-16955](https://issues.apache.org/jira/browse/HBASE-16955) | *Major* | **Fixup precommit protoc check to do new distributed protos and pb 3.1.0 build**

Test that environment no longer has to have protoc (2.5 and 3.1) available. Needed small adjustment in yetus protoc build but otherwise all works.


---

* [HBASE-17050](https://issues.apache.org/jira/browse/HBASE-17050) | *Minor* | **Upgrade Apache CLI version from 1.2 to 1.3.1**

Upgrade Apache CLI version from 1.2 to 1.3.1.

These are few good/important changes included in this update:
- HelpFormatter now prints command-line options in the same order as they
  have been added. Fixes CLI-212.
- Standard help text now shows mandatory arguments also for the first
  option. Fixes CLI-186.
- A new parser is available: DefaultParser. It combines the features of the
  GnuParser and the PosixParser. It also provides additional features like
  partial matching for the long options, and long options without separator
  (i.e like the JVM memory settings: -Xmx512m). This new parser deprecates
  the previous ones. Fixes CLI-161,CLI-167,CLI-181.

For full list of changes:
  https://commons.apache.org/proper/commons-cli/changes-report.html#a1.3


---

* [HBASE-15513](https://issues.apache.org/jira/browse/HBASE-15513) | *Major* | **hbase.hregion.memstore.chunkpool.maxsize is 0.0 by default**

MSLAB chunk pool is on by default in hbase-2.0.0.


---

* [HBASE-16972](https://issues.apache.org/jira/browse/HBASE-16972) | *Major* | **Log more details for Scan#next request when responseTooSlow**

**WARNING: No release note provided for this change.**


---

* [HBASE-17014](https://issues.apache.org/jira/browse/HBASE-17014) | *Minor* | **Add clearly marked starting and shutdown log messages for all services.**

Delimit START, STOP, and ABORT messages with '\*\*\*\*\*' so denote.


---

* [HBASE-16765](https://issues.apache.org/jira/browse/HBASE-16765) | *Critical* | **New SteppingRegionSplitPolicy, avoid too aggressive spread of regions for small tables.**

Introduces a new split policy: SteppingSplitPolicy
This will use a simple step function to split a region at (by default) 2  xflushSize when no other region of the same table is seen on the region server, or max-file-size when one or more other regions of the same table is seen.

In HBase 2.0 this is going to be the default. In previous versions it can be configured.


---

* [HBASE-16608](https://issues.apache.org/jira/browse/HBASE-16608) | *Major* | **Introducing the ability to merge ImmutableSegments without copy-compaction or SQM usage**

The index-compation and data-compaction variants of CompactingMemStore are introduced. In both types the active (mutable) segment is periodically flushed-in-memory and is added as immutable segment in the compaction pipeline. The CompactingMemStore of index-compaction type is merging all immutable segments of the compacting pipeline into one. The merging of N segments is explained below. The CompactingMemStore of data-compaction type is compacting all immutable segments of the compacting pipeline into one. After the merge/compaction the old segments in the compacting pipeline are replaced with one new.

Before explaining the process of merging N old segments into new one, note that segment structure includes ordered index that allows traversing the cells data efficiently. The merge is copying the ordered indexes of the old segments into one ordered index of new segment. No data is copied, no cells are filtered. Alternatively, in the process of compacting N old segments into new one, both data and index are copied. The old cells are filtered, meaning upon compaction unused versions of the cells are not copied so the new segment has less data then all old ones.

This issue introduces only the merging ability and simplifies the user intervention for switching between types. The previous CompactingMemStore structure was added by HBASE-16420 and HBASE-16421. The future refinements of the policy or merging/compacting will come in HBASE-16417.

In order to create a table with CompactingMemStore as a MemStore one should use:
create ‘\<tablename\>’, {NAME =\> ‘\<cfname\>’, IN\_MEMORY\_COMPACTION =\> true}
IN\_MEMORY\_COMPACTION default is false, so table created as following will have the known DefaultMemStore as a MemStore.
create ‘\<tablename\>’, {NAME =\> ‘\<cfname\>’}

The default type of CompactingMemStore is index-compaction. In order to change it to data-compaction one should add to the hbase-site.xml
\<property\>
    \<name\>hbase.hregion.compacting.memstore.type\</name\>
    \<value\>data-compaction\</value\>
  \</property\>

in addition to creating the table as following
create ‘\<tablename\>’, {NAME =\> ‘\<cfname\>’, IN\_MEMORY\_COMPACTION =\> true}


---

* [HBASE-16747](https://issues.apache.org/jira/browse/HBASE-16747) | *Major* | **Track memstore data size and heap overhead separately**

Marking it as incompatible change as there is a change in behavior for region flush decision. The default flush size of 128 MB per region was tracked against both actual data bytes size + overhead of these cells in memstore memory (Overhead because of Cell java objects and CSLM entry).  As part of this jira we will keep track of cell data size only in region level.  So 128 MB flush size means, 128 MB of cell data bytes (key+ value+..)

Globally we will track cell data size and heap overhead separately and will consider both for forced flushes. We will not allow over consume of heap memory by all memstore. This is as old case. Only tracking way is changed.


---

* [HBASE-16974](https://issues.apache.org/jira/browse/HBASE-16974) | *Minor* | **Update os-maven-plugin to 1.4.1.final+ for building shade file on RHEL/CentOS**

Upgrade os-maven-plugin mvn extension which figures the os we are running on from 1.4 to 1.5.


---

* [HBASE-16952](https://issues.apache.org/jira/browse/HBASE-16952) | *Major* | **Replace hadoop-maven-plugins with protobuf-maven-plugin for building protos**

Simplifies .proto manipulations. One step only now -- no need to keep pom.xml listing up to date with the protobuf protos directory content -- and no need to preinstall protoc; mvn does it all for you now.


---

* [HBASE-14551](https://issues.apache.org/jira/browse/HBASE-14551) | *Minor* | **Procedure v2 - Reimplement split**

Moved the Split Region logic to Master and most of split region coprocessor is in master now.  Need to change dependency such as Phoenix.


---

* [HBASE-15789](https://issues.apache.org/jira/browse/HBASE-15789) | *Major* | **PB related changes to work with offheap**

This issue adds a patch to our checked in internal, shaded protobuf, but it also adds a general means of apply patches to our version of protobuf. Patches found in the new src/main/patches directory are all applied as the last task when you run a build with the -Pcompile-protobuf profile under the hbase-protocol-shaded module. This commit also includes our first patch to protobuf; it adds ByteInput to mimic pb3.1's ByteOutput (src/main/patches/HBASE-15789\_V2.patch attached here).


---

* [HBASE-16930](https://issues.apache.org/jira/browse/HBASE-16930) | *Major* | **AssignmentManager#checkWals() function can recur infinitely**

Fixed potential infinite recursion in AssignmentManager.checkWals().


---

* [HBASE-16463](https://issues.apache.org/jira/browse/HBASE-16463) | *Major* | **Improve transparent table/CF encryption with Commons Crypto**

Improve transparent table/CF encryption with Commons Crypto. The change introduces a new optional CryptoCipherProvider (CommonsCryptoAES) for transparent table/CF encryption. And the encryption performance would be accelerated by hardware in modern CPU (AES-NI). This feature could be enabled by updating the configuration "hbase.crypto.cipherprovider" to "org.apache.hadoop.hbase.io.crypto.CryptoCipherProvider" in hbase-site.xml. For detailed information about transparent table/CF encryption including configuration examples see the Security section of the HBase manual.


---

* [HBASE-16414](https://issues.apache.org/jira/browse/HBASE-16414) | *Major* | **Improve performance for RPC encryption with Apache Common Crypto**

With the security RPC and encryption enabled, introduce Apache Commons Crypto to do the encryption/decryption which supports both supports both JCE Cipher and OpenSSL Cipher. Adds new configs "hbase.rpc.crypto.encryption.aes.enabled" which defaults to false, and "hbase.rpc.crypto.encryption.aes.cipher.class" which defaults to "org.apache.commons.crypto.cipher.JceCipher" to support JCE Cipher, it also can be set as "org.apache.hadoop.crypto.OpensslCipher" to support Openssl Cipher.


---

* [HBASE-16721](https://issues.apache.org/jira/browse/HBASE-16721) | *Critical* | **Concurrency issue in WAL unflushed seqId tracking**

Fixed a bug in sequenceId tracking for the WALs that caused WAL files to accumulate without being deleted due to a rare race condition.


---

* [HBASE-16834](https://issues.apache.org/jira/browse/HBASE-16834) | *Major* | **Add AsyncConnection support for ConnectionFactory**

Add createAsyncConnection method to ConnectionFactory for creating AsyncConnection. The default implementation is org.apache.hadoop.hbase.client.AsyncConnectionImpl. You can use 'hbase.client.async.connection.impl' to plug in your own AsyncConnection implementation.


---

* [HBASE-16729](https://issues.apache.org/jira/browse/HBASE-16729) | *Trivial* | **Define the behavior of (default) empty FilterList**

Empty filter list will behave as when there is no filter added. This change is a behavioral change for those who rely on Empty filter list.


---

* [HBASE-16799](https://issues.apache.org/jira/browse/HBASE-16799) | *Major* | **CP exposed Store should not expose unwanted APIs**

Below APIs from CP exposed Store interface are removed
upsert(Iterable\<Cell\> cells, long readpoint)
add(Cell cell)
add(Iterable\<Cell\> cells)
replayCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,  boolean removeFiles)
assertBulkLoadHFileOk(Path srcPath)
bulkLoadHFile(String srcPathStr, long sequenceId)
bulkLoadHFile(StoreFileInfo fileInfo)


---

* [HBASE-15921](https://issues.apache.org/jira/browse/HBASE-15921) | *Major* | **Add first AsyncTable impl and create TableImpl based on it**

Add AsyncConnection, AsyncTable and AsyncTableRegionLocator. Now the AsyncTable only support get, put and delete. And the implementation of AsyncTableRegionLocator is synchronous actually.


---

* [HBASE-16664](https://issues.apache.org/jira/browse/HBASE-16664) | *Major* | **Timeout logic in AsyncProcess is broken**

This issue fix three bugs:
1.  rpcTimeout configuration not work for one rpc call in AP
2.  operationTimeout configuration not work for multi-request (batch, put) in AP
3.  setRpcTimeout and setOperationTimeout in HTable is not worked for AP and BufferedMutator.


---

* [HBASE-16661](https://issues.apache.org/jira/browse/HBASE-16661) | *Minor* | **Add last major compaction age to per-region metrics**

This adds a new per-region metric named "lastMajorCompactionAge" for tracking time since the last major compaction ran on a given region.  If a major compaction has never run, the age will be equal to the current timestamp.


---

* [HBASE-16117](https://issues.apache.org/jira/browse/HBASE-16117) | *Major* | **Fix Connection leak in mapred.TableOutputFormat**

(This change will be irrelevant after HBASE-16774 lands).
There is a subtle change with error handling when a connection is not able to connect to ZK.  Attempts to create a connection when ZK is not up will now fail immediately instead of silently creating and then failing on a subsequent HBaseAdmin call.


---

* [HBASE-15984](https://issues.apache.org/jira/browse/HBASE-15984) | *Critical* | **Given failure to parse a given WAL that was closed cleanly, replay the WAL.**

In some particular deployments, the Replication code believes it has
reached EOF for a WAL prior to successfully parsing all bytes known to
exist in a cleanly closed file.

If an EOF is detected due to parsing or other errors while there are still unparsed bytes before the end-of-file trailer, we now reset the WAL to the very beginning and attempt a clean read-through. Because we will retry these failures indefinitely, two additional changes are made to help with diagnostics:

\* On each retry attempt, a log message like the below will be emitted at the WARN level:

      Processing end of WAL file '{}'. At position {}, which is too far away
      from reported file length {}. Restarting WAL reading (see HBASE-15983
      for details).

\*  additional metrics measure the use of this recovery mechanism. they are described in the reference guide.


---

* [HBASE-16753](https://issues.apache.org/jira/browse/HBASE-16753) | *Minor* | **There is a mismatch between suggested Java version in hbase-env.sh**

Updates the comments and default values in a few scripts and docs to reflect our Java 1.8+ requirement.


---

* [HBASE-16567](https://issues.apache.org/jira/browse/HBASE-16567) | *Critical* | **Upgrade to protobuf-3.1.x**

Core is now up on protobuf 3.1.0 (Coprocessor Endpoints and REST are still on protobuf 2.5.0).


---

* [HBASE-15638](https://issues.apache.org/jira/browse/HBASE-15638) | *Critical* | **Shade protobuf**

Shade/relocate and include the protobuf we use internally. See protobuf chapter in the refguide for more on how we protobuf in hbase-.2.0.0 and going forward.

See https://docs.google.com/document/d/1H4NgLXQ9Y9KejwobddCqaVMEDCGbyDcXtdF5iAfDIEk/edit# for how we arrived at this approach.

See http://mail-archives.apache.org/mod\_mbox/hbase-dev/201610.mbox/%3C07850EDD-7230-431B-9AB0-C5C91B105EEC%40gmail.com%3E for discussion around merging this change and of how we might revert if an alternative to this awkward patch presents itself; e.g. an hadoop with CLASSPATH isolation (and means of dealing with Sparks use of protobuf 2.5.0, etc.)


---

* [HBASE-16264](https://issues.apache.org/jira/browse/HBASE-16264) | *Critical* | **Figure how to deal with endpoints and shaded pb**

Shade/relocate the protobuf hbase uses internally. All core now refers to new module added in this patch, hbase-protocol-shaded. Coprocessor Endpoints carry-on with references to the original hbase-protocol module. See new chapter in book on protobufs on how-to going forward.


---

* [HBASE-16672](https://issues.apache.org/jira/browse/HBASE-16672) | *Major* | **Add option for bulk load to always copy hfile(s) instead of renaming**

This issue adds a config, always.copy.files, to LoadIncrementalHFiles.
When set to true, source hfiles would be copied. Meaning source hfiles would be kept after bulk load is done.
Default value is false.


---

* [HBASE-16660](https://issues.apache.org/jira/browse/HBASE-16660) | *Critical* | **ArrayIndexOutOfBounds during the majorCompactionCheck in DateTieredCompaction**

"Please do not use DateTieredCompaction with Major Compaction unless you have a version with this. Otherwise your cluster will not compact any store files and you can end up running out of file descriptors." @churro morales


---

* [HBASE-16257](https://issues.apache.org/jira/browse/HBASE-16257) | *Blocker* | **Move staging dir to be under hbase root dir**

The HBase property 'hbase.bulkload.staging.dir' is deprecated and is ignored from HBase 2.0.  It will defaults to hbase.rootdir/staging automatically with the correct permissions.


---

* [HBASE-16650](https://issues.apache.org/jira/browse/HBASE-16650) | *Major* | **Wrong usage of BlockCache eviction stat for heap memory tuning**

Changed tracking of evictedBlocks count NOT to include evictions of blocks for a removed HFile. HFiles gets removed after compaction


---

* [HBASE-16294](https://issues.apache.org/jira/browse/HBASE-16294) | *Minor* | **hbck reporting "No HDFS region dir found" for replicas**

Fixed warning error message displayed for region directory not found for non-default/ non-primary replicas in hbck


---

* [HBASE-16540](https://issues.apache.org/jira/browse/HBASE-16540) | *Major* | **Scan should do additional validation on start and stop row**

Scan#setStartRow() and Scan#setStopRow() now validate the argument passed for each row key.  If the length of the byte[] passed exceeds Short.MAX\_VALUE, an IllegalArgumentException will be thrown.


---

* [HBASE-7612](https://issues.apache.org/jira/browse/HBASE-7612) | *Trivial* | **[JDK8] Replace use of high-scale-lib counters with intrinsic facilities**

org.apache.hadoop.hbase.util.Counter is deprecated now and will be removed in 3.0. Use LongAdder instead.


---

* [HBASE-16447](https://issues.apache.org/jira/browse/HBASE-16447) | *Critical* | **Replication by namespaces config in peer**

Support replication by namespaces config in peer.
1. Set a namespace in peer config means that all tables in this namespace will be replicated.
2. If the namespaces config is null, then the table-cfs config decide which table's edit can be replicated. If the table-cfs config is null, then the namespaces config decide which table's edit can be replicated.
3. If you already have set a namespace in the peer config, then you can't set any table of this namespace to the peer config. If you already have set a table in the peer config, then you can't set this table's namespace to the peer config.


---

* [HBASE-16598](https://issues.apache.org/jira/browse/HBASE-16598) | *Major* | **Enable zookeeper useMulti always and clean up in HBase code**

Deprecate the configuration property 'hbase.zookeeper.useMulti'.
useMulti will always be enabled. ZooKeeper 3.4.x and newer is required.

Internal:

The ZKUtil#multiOrSequential(ZooKeeperWatcher zkw, List\<ZKUtilOp\> ops, boolean runSequentialOnMultiFailure) will not check 'hbase.zookeeper.useMulti' anymore, and will always use multi.
It can still fall back to sequential operations if:

RunSequentialOnMultiFailure is true
On calling multi, we get a ZooKeeper exception that can be handled by a sequential call.


---

* [HBASE-16388](https://issues.apache.org/jira/browse/HBASE-16388) | *Major* | **Prevent client threads being blocked by only one slow region server**

Add a new configuration, hbase.client.perserver.requests.threshold, to limit the max number of concurrent request to one region server. If the user still create new request after reaching the limit, client will throw ServerTooBusyException and do not send the request to the server. This is a client side feature and can prevent client's threads being blocked by one slow region server resulting in the availability of client is much lower than the availability of region servers.

For completeness, here extract on new config from hbase-default.xml:

Property: hbase.client.perserver.requests.threshold
Default: 2147483647
Description: The max number of concurrent pending requests for one server in all client threads (process level). Exceeding requests will be thrown ServerTooBusyException immediately to prevent user's threads being occupied and blocked by only one slow region server. If you use a fix number of threads to access HBase in a synchronous way, set this to a suitable value which is  related to the number of threads will help you. See https://issues.apache.org/jira/browse/HBASE-16388 for details.


---

* [HBASE-15297](https://issues.apache.org/jira/browse/HBASE-15297) | *Minor* | **error message is wrong when a wrong namspace is specified in grant in hbase shell**

The security admin instance available within the HBase shell now returns "false" from the namespace\_exists? method for non-existent namespaces rather than raising a wrapped NamespaceNotFoundException.

As a side effect, when the "grant" and "revoke" commands in the HBase shell are invoked with a non-existent namespace the resulting error message now properly refers to said namespace rather than to the user.


---

* [HBASE-16086](https://issues.apache.org/jira/browse/HBASE-16086) | *Major* | **TableCfWALEntryFilter and ScopeWALEntryFilter should not redundantly iterate over cells.**

push to branch-1.3+


---

* [HBASE-16340](https://issues.apache.org/jira/browse/HBASE-16340) | *Critical* | **ensure no Xerces jars included**

HBase no longer includes Xerces implementation jars that were previously included via transitive dependencies. Downstream users relying on HBase for these artifacts will need to update their dependencies.


---

* [HBASE-16213](https://issues.apache.org/jira/browse/HBASE-16213) | *Major* | **A new HFileBlock structure for fast random get**

HBASE-16213 introduced a new DataBlockEncoding in name of ROW\_INDEX\_V1, which could improve random read (get) performance especially when the average record size (key-value size per row) is small. To use this feature, please set DATA\_BLOCK\_ENCODING to ROW\_INDEX\_V1 for CF of newly created table, or change existing CF with below command:
alter 'table\_name',{NAME =\> 'cf', DATA\_BLOCK\_ENCODING =\> 'ROW\_INDEX\_V1'}.

Please note that if we turn this DBE on, HFile block will be bigger than NONE encoding because it adds some meta infos for binary search:
/\*\*
 \* Store cells following every row's start offset, so we can binary search to a row's cells.
 \*
 \* Format:
 \* flat cells
 \* integer: number of rows
 \* integer: row0's offset
 \* integer: row1's offset
 \* ....
 \* integer: dataSize
 \*
\*/

Seek in row when random reading is one of the main consumers of CPU. This helps. See slide #7 here https://www.slideshare.net/HBaseCon/lift-the-ceiling-of-hbase-throughputs?qid=597ee2fa-8125-4faa-bb3b-2bf1ba9ccafb&v=&b=&from\_search=6


---

* [HBASE-16409](https://issues.apache.org/jira/browse/HBASE-16409) | *Minor* | **Row key for bad row should be properly delimited in VerifyReplication**

--delimiter= option is added to verifyrep.
The delimiter would wrap bad rows in log output.


---

* [HBASE-14921](https://issues.apache.org/jira/browse/HBASE-14921) | *Major* | **Inmemory Compaction Optimizations; Segment Structure**

A long, working issue that discussed Segment formats introducing CellArrayMap (delivered as the patch attached to this issue) and CellChunkMap (to be delivered later in HBASE-16421 but see patch v02 for an embryonic form named CellBlockSerialized); when to copy Segment data (and when not too); and then what to include at flush time (the suffix Segment or all Segments). Designs that evolved as discussion went on are attached. Outstanding issues turned up here, not including a CellChunkMap implementation, are listed below but are to be addressed in follow-ons (See HBASE-16417):

1. The flattening without compaction is causing many small segments in pipeline, and they are not flushed all together.
2. The issue of compaction prediction cost.


---

* [HBASE-16450](https://issues.apache.org/jira/browse/HBASE-16450) | *Major* | **Shell tool to dump replication queues**

New tool to dump existing replication peers, configurations and queues when using HBase Replication. The tool provides two flags:

 --distributed  This flag will poll each RS for information about the replication queues being processed on this RS.
By default this is not enabled and the information about the replication queues and configuration will be obtained from ZooKeeper.
 --hdfs   When --distributed is used, this flag will attempt to calculate the total size of the WAL files used by the replication queues. Since its possible that multiple peers can be configured this value can be overestimated.


---

* [HBASE-16422](https://issues.apache.org/jira/browse/HBASE-16422) | *Major* | **Tighten our guarantees on compatibility across patch versions**

Adds below change to our compat guarantees:

{code}
-\* Example: A user using a newly deprecated api does not need to modify application code with hbase api calls until the next major version.
 10 +\* New APIs introduced in a patch version will only be added in a source compatible way footnote:[See 'Source Compatibility' https://blogs.oracle.com/darcy/entry/kinds\_of\_compatibility]: i.e.     code that implements public APIs will continue to compile.
{code}


---

* [HBASE-7621](https://issues.apache.org/jira/browse/HBASE-7621) | *Major* | **REST client (RemoteHTable) doesn't support binary row keys**

RemoteHTable now supports binary row keys with any character or byte by properly encoding request URLs. This is a both a behavioral change from earlier versions and an important fix for protocol correctness.


---

* [HBASE-12721](https://issues.apache.org/jira/browse/HBASE-12721) | *Major* | **Create Docker container cluster infrastructure to enable better testing**

Downstream users wishing to test HBase in a "distributed" fashion (multiple "nodes" running as separate containers on the same host) can now do so in an automated fashion while leveraging Docker for process isolation via the clusterdock project.

For details see the README.md in the dev-support/apache\_hbase\_topology folder.


---

* [HBASE-16267](https://issues.apache.org/jira/browse/HBASE-16267) | *Critical* | **Remove commons-httpclient dependency from hbase-rest module**

This issue upgrades httpclient to 4.5.2 and httpcore to 4.4.4 which are the versions used by hadoop-2.
This is to handle the following CVE's.

https://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2015-5262 : http/conn/ssl/SSLConnectionSocketFactory.java in Apache HttpComponents HttpClient before 4.3.6 ignores the http.socket.timeout configuration setting during an SSL handshake, which allows remote attackers to cause a denial of service (HTTPS call hang) via unspecified vectors.

https://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2012-6153
https://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2012-5783
Apache Commons HttpClient 3.x, as used in Amazon Flexible Payments Service (FPS) merchant Java SDK and other products, does not verify that the server hostname matches a domain name in the subject's Common Name (CN) or subjectAltName field of the X.509 certificate, which allows man-in-the-middle attackers to spoof SSL servers via an arbitrary valid certificate.

Downstream users who are exposed to commons-httpclient via the HBase classpath will have to similarly update their dependency.


---

* [HBASE-16308](https://issues.apache.org/jira/browse/HBASE-16308) | *Major* | **Contain protobuf references**

Undo protobuf references through the codebase so protobuf references are contained rather than spread about the codebase. For example, moved protobuff-ing up into the various Callables rather than repeat on each method invocation cleaning up boilerplate around rpc calls. Having a few protobuf reference locations only simplifies the parent issue shading project.


---

* [HBASE-16321](https://issues.apache.org/jira/browse/HBASE-16321) | *Blocker* | **Ensure findbugs jsr305 jar isn't present**

HBase now ensures the jsr305 implementation from the findbugs project is not included in its binary artifacts or the compile / runtime dependencies of its user facing modules. Downstream users that rely on this jar will need to update their dependencies.


---

* [HBASE-8386](https://issues.apache.org/jira/browse/HBASE-8386) | *Major* | **deprecate TableMapReduce.addDependencyJars(Configuration, class\<?\> ...)**

The MapReduce helper function \`TableMapReduce.addDependencyJars(Configuration, class\<?\> ...)\` has been deprecated since it is easy to use incorrectly. Most users should rely on addDependencyJars(Job) instead.


---

* [HBASE-16287](https://issues.apache.org/jira/browse/HBASE-16287) | *Major* | **LruBlockCache size should not exceed acceptableSize too many**

In order to avoid blockcache size exceed acceptable size too much, we add one configuration "hbase.lru.blockcache.hard.capacity.limit.factor" to decide whether the block could be put into LruBlockCache or not.  This factor defaults to 1.2
If blockcache size \>= factor\*acceptableSize, we will reject the block into cache.


---

* [HBASE-16355](https://issues.apache.org/jira/browse/HBASE-16355) | *Major* | **hbase-client dependency on hbase-common test-jar should be test scope**

The HBase client artifact previously incorrectly included the hbase-common test jar as a runtime dependency. With this change, that dependency has been moved to test scope. Downstream users are not expected to be impacted, unless they relied on the transitive dependency for these HBase internal test classes.


---

* [HBASE-16317](https://issues.apache.org/jira/browse/HBASE-16317) | *Blocker* | **revert all ESAPI changes**

This issue reverts fixes designed to prevent malicious content from rendering in HBase's UIs. Specifically, these changes shipped in 1.1.4+ and 1.2.0+. They were removed due to licensing issues discovered in the dependencies they introduced. Their implementation and those dependencies have been removed from HBase! Removal of these dependencies is against the strict definition of our version compatibility guidelines. However, inclusion of non-Apache approved licenses cannot be tolerated. Implementation of these fixes using an Apache-appropriate means is tracked in HBASE-16328.


---

* [HBASE-16288](https://issues.apache.org/jira/browse/HBASE-16288) | *Critical* | **HFile intermediate block level indexes might recurse forever creating multi TB files**

A new hfile configuration "hfile.index.block.min.entries" which defaults to 16 determines how many entries the hfile index block can have at least. The configuration which determines how large the index block can be at max (hfile.index.block.max.size) is ignored as long as we have fewer than hfile.index.block.min.entries entries. This ensures that multi-level index does not build up with too many levels.


---

* [HBASE-16186](https://issues.apache.org/jira/browse/HBASE-16186) | *Major* | **Fix AssignmentManager MBean name**

The AssignmentManager MBean was named AssignmentManger (note misspelling). This patch fixed the misspelling.


---

* [HBASE-16289](https://issues.apache.org/jira/browse/HBASE-16289) | *Critical* | **AsyncProcess stuck messages need to print region/server**

Adds logging of region and server. Helpful debugging. Logging now looks like this:
{code}
2016-06-23 17:07:18,759 INFO  [Thread-1] client.AsyncProcess$AsyncRequestFutureImpl(1601): #1, waiting for 1  actions to finish on table: DUMMY\_TABLE
2016-06-23 17:07:18,759 INFO  [Thread-1] client.AsyncProcess(1720): Left over 1 task(s) are processed on server(s): [s1:1,1,1]
2016-06-23 17:07:18,759 INFO  [Thread-1] client.AsyncProcess(1728): Regions against which left over task(s) are processed: [DUMMY\_TABLE,DUMMY\_BYTES\_1,1.3fd12ea80b4df621fb15497ba75f7368.,DUMMY\_TABLE,DUMMY\_BYTES\_2,2.924207e242e313d2e5491c625e0a296e.]
{code}


---

* [HBASE-14743](https://issues.apache.org/jira/browse/HBASE-14743) | *Minor* | **Add metrics around HeapMemoryManager**

A memory metrics reveals situations happened in both MemStores and BlockCache in RegionServer. Through this metrics, users/operators can know
1). Current size of MemStores and BlockCache in bytes.
2). Occurrence for Memstore minor and major flush. (named unblocked flush and blocked flush respectively, shown in histogram)
3). Dynamic changes in size between MemStores and BlockCache. (with Increase/Decrease as prefix, shown in histogram). And a counter for no changes, named DoNothingCounter.
4). Occurrence for memory usage alarm (used more than 95% by default) in RegionServer. (named AboveHeapOccupancyLowWatermarkCounter)


---

* [HBASE-13701](https://issues.apache.org/jira/browse/HBASE-13701) | *Major* | **Consolidate SecureBulkLoadEndpoint into HBase core as default for bulk load**

SecureBulkLoadEndpoint  has been integrated into HBase core as default bulk load mechanism. It is no longer needed to install it as a coprocessor endpoint.
The new server is backward compatible, accommodating non-secure old client and secure old client requesting SecureBulkLoadEndpoint service.
SecureBulkLoadEndpoint is deprecated. The backward compatibility support may be removed in future releases.


---

* [HBASE-16244](https://issues.apache.org/jira/browse/HBASE-16244) | *Major* | **LocalHBaseCluster start timeout should be configurable**

When LocalHBaseCluster is started from the command line the Master would give up after 30 seconds due to a hardcoded timeout meant for unit tests. This change allows the timeout to be configured via hbase-site as well as sets it to 5 minutes when LocalHBaseCluster is started from the command line.


---

* [HBASE-16052](https://issues.apache.org/jira/browse/HBASE-16052) | *Major* | **Improve HBaseFsck Scalability**

HBASE-16052 improves the performance and scalability of HBaseFsck, especially for large clusters with a small number of large tables.

Searching for lingering reference files is now a multi-threaded operation.  Loading HDFS region directory information is now multi-threaded at the region-level instead of the table-level to maximize concurrency.  A performance bug in HBaseFsck that resulted in redundant I/O and RPCs was fixed by introducing a FileStatusFilter that filters FileStatus objects directly.


---

* [HBASE-16144](https://issues.apache.org/jira/browse/HBASE-16144) | *Major* | **Replication queue's lock will live forever if RS acquiring the lock has died prematurely**

If zk based replication queue is used and useMulti is false, we will schedule a chore to clean up the orphan replication queue lock on zk.


---

* [HBASE-3727](https://issues.apache.org/jira/browse/HBASE-3727) | *Minor* | **MultiHFileOutputFormat**

MultiHFileOutputFormat support output of HFiles from multiple tables. It will output directories and hfiles as follow,
     --table1
       --family1
       --family2
         --Hfiles
     --table2
       --family3
         --hfiles
       --family4

family directory and its hfiles match the output of HFileOutputFormat2


---

* [HBASE-16231](https://issues.apache.org/jira/browse/HBASE-16231) | *Major* | **Integration tests should support client keytab login for secure clusters**

Prior to this change, the integration test clients (IntegrationTest\*) relied on the Kerberos credential cache for authentication against secured clusters.  This could lead to the tests failing due to authentication failures when the tickets in the credential cache expired.  With this change, the integration test clients will make use of the configuration properties for "hbase.client.keytab.file" and "hbase.client.kerberos.principal", when available.  This will perform a login from the configured keytab file and automatically refresh the credentials in the background for the process lifetime.


---

* [HBASE-13823](https://issues.apache.org/jira/browse/HBASE-13823) | *Major* | **Procedure V2: unnecessaery operations on AssignmentManager#recoverTableInDisablingState() and recoverTableInEnablingState()**

For cluster upgraded from 1.0.x or older releases, master startup would not continue the in-progress enable/disable table process.  If orphaned znode with ENABLING/DISABLING state exists in the cluster, run hbck or manually fix the issue.

For new cluster or cluster upgraded from 1.1.x and newer release, there is no issue to worry about.


---

* [HBASE-16095](https://issues.apache.org/jira/browse/HBASE-16095) | *Major* | **Add priority to TableDescriptor and priority region open thread pool**

Adds a PRIORITY property to the HTableDescriptor. PRIORITY should be in the same range as the RpcScheduler defines it (HConstants.XXX\_QOS).

Table priorities are only used for region opening for now. There can be other uses later (like RpcScheduling).

Regions of high priority tables (priority \>= than HIGH\_QOS) are opened from a different thread pool than the regular region open thread pool. However, table priorities are not used as a global order for region assigning or opening.


---

* [HBASE-16081](https://issues.apache.org/jira/browse/HBASE-16081) | *Blocker* | **Replication remove\_peer gets stuck and blocks WAL rolling**

When a replication endpoint is sent a shutdown request by the replication source in situations like removing a peer, we now try to gracefully shut it down by draining the items already sent for replication to the peer cluster. If the drain does not complete in the specified time (hbase.rpc.timeout \* replication.source.maxterminationmultiplier), the regionserver is aborted to avoid blocking the WAL roll.


---

* [HBASE-16087](https://issues.apache.org/jira/browse/HBASE-16087) | *Major* | **Replication shouldn't start on a master if if only hosts system tables**

Masters will no longer start any replication threads if they are hosting only system tables.

In order to change this add something to the config for tables on master that doesn't start with "hbase:" ( Replicating system tables is something that's currently unsupported and can open up security holes, so do this at your own peril)


---

* [HBASE-14548](https://issues.apache.org/jira/browse/HBASE-14548) | *Major* | **Expand how table coprocessor jar and dependency path can be specified**

Allow a directory containing the jars or some wildcards to be specified, such as: hdfs://namenode:port/user/hadoop-user/
or
hdfs://namenode:port/user/hadoop-user/\*.jar

Please note that if a directory is specified, all jar files(.jar) directly in the directory are added, but it does not search files in the subtree rooted in the directory.
Do not contain any wildcard if you would like to specify a directory.


---

* [HBASE-15925](https://issues.apache.org/jira/browse/HBASE-15925) | *Blocker* | **compat-module maven variable not evaluated**

Downstream users of HBase dependencies that do not properly activate Maven profiles should now see a correct transitive dependency on the default hadoop-compatibility-module.


---

* [HBASE-16140](https://issues.apache.org/jira/browse/HBASE-16140) | *Major* | **bump owasp.esapi from 2.1.0 to 2.1.0.1**

The dependency owasp.esapi had a compatible change from 2.1.0 to 2.1.0.1. As a result, the transitive dependency commons-fileupload had a change from 1.2 to 1.3.1, which has some minor class changes that impact binary compatibility. Interested users should check the release notes of commons-fileupload to see if any of the incompatible changes impact them.

http://commons.apache.org/proper/commons-fileupload/changes-report.html


---

* [HBASE-16147](https://issues.apache.org/jira/browse/HBASE-16147) | *Major* | **Shell command for getting compaction state**

compaction\_state shell command would return compaction state in String form:
NONE, MINOR, MAJOR, MAJOR\_AND\_MINOR


---

* [HBASE-14878](https://issues.apache.org/jira/browse/HBASE-14878) | *Major* | **maven archetype: client application with shaded jars**

Adds new hbase-shaded-client archetype; also corrects an omission found in hbase-archetypes/README.md in the section headed "How to add a new archetype".


---

* [HBASE-14877](https://issues.apache.org/jira/browse/HBASE-14877) | *Major* | **maven archetype: client application**

This patch introduces a new infrastructure for creation and maintenance of Maven archetypes in the context of the hbase project, and it also introduces the first archetype, which end-users may utilize to generate a simple hbase-client dependent project.

NOTE that this patch should introduce two new WARNINGs ("Using platform encoding ... to copy filtered resources") into the hbase install process. These warnings are hard-wired into the maven-archetype-plugin:create-from-project goal. See hbase/hbase-archetypes/README.md, footnote [6] for details.

After applying the patch, see hbase/hbase-archetypes/README.md for details regarding the new archetype infrastructure introduced by this patch. (The README text is also conveniently positioned at the top of the patch itself.)

Here is the opening paragraph of the README.md file:
=================
The hbase-archetypes subproject of hbase provides an infrastructure for creation and maintenance of Maven archetypes pertinent to HBase. Upon deployment to the archetype catalog of the central Maven repository, these archetypes may be used by end-user developers to autogenerate completely configured Maven projects (including fully-functioning sample code) through invocation of the archetype:generate goal of the maven-archetype-plugin.
========
The README.md file also contains several paragraphs under the heading, "Notes for contributors and committers to the HBase project", which explains the layout of 'hbase-archetypes', and how archetypes are created and installed into the local Maven repository, ready for deployment to the central Maven repository. It also outlines how new archetypes may be developed and added to the collection in the future.


---

* [HBASE-15977](https://issues.apache.org/jira/browse/HBASE-15977) | *Major* | **Failed variable substitution on home page**

Done. Thanks, Dima, Andrew!


---

* [HBASE-5291](https://issues.apache.org/jira/browse/HBASE-5291) | *Major* | **Add Kerberos HTTP SPNEGO authentication support to HBase web consoles**

HBase Web UIs can be secured from general public access using SPNEGO to require a valid Kerberos ticket.

Setting 'hbase.security.authentication.ui' to 'kerberos' in hbase-site.xml is a global switch to have all Web UIs allow only authenticated clients via Kerberos. 'hbase.security.authentication.spnego.kerberos.principal' and 'hbase.security.authentication.spnego.kerberos.keytab' are two other required properties in hbase-site.xml, the Kerberos principal and keytab to use for the server to use to log in. The primary in the Kerberos principal must be 'HTTP' as required by the SPNEGO mechanism, e.g. 'HTTP/host.domain.com@DOMAIN.COM'.


---

* [HBASE-15950](https://issues.apache.org/jira/browse/HBASE-15950) | *Major* | **Fix memstore size estimates to be more tighter**

The estimates of heap usage by the memstore objects (KeyValue, object and array header sizes, etc) have been made more accurate for heap sizes up to 32G (using CompressedOops), resulting in them dropping by 10-50% in practice. This also results in less number of flushes and compactions due to "fatter" flushes. YMMV. As a result, the actual heap usage of the memstore before being flushed may increase by up to 100%. If configured memory limits for the region server had been tuned based on observed usage, this change could result in worse GC behavior or even OutOfMemory errors. Set the environment property (not hbase-site.xml) "hbase.memorylayout.use.unsafe" to false to disable.


---

* [HBASE-16023](https://issues.apache.org/jira/browse/HBASE-16023) | *Major* | **Fastpath for the FIFO rpcscheduler**

Adds a 'fastpath' when using the default FIFO rpc scheduler ('fifo'). Does direct handoff from Reader thread to Handler if there is one ready and willing. Will shine best when high random read workload (YCSB workloadc for instance)


---

* [HBASE-15971](https://issues.apache.org/jira/browse/HBASE-15971) | *Critical* | **Regression: Random Read/WorkloadC slower in 1.x than 0.98**

Change the default rpc scheduler from 'deadline' to 'fifo' instead so it is the same as in branch 0.98. 'deadline' was of questionable benefit but with a high cost scheduling. To re-enable 'deadline', set hbase.ipc.server.callqueue.type to 'deadline' in your hbase-site.xml.


---

* [HBASE-15525](https://issues.apache.org/jira/browse/HBASE-15525) | *Critical* | **OutOfMemory could occur when using BoundedByteBufferPool during RPC bursts**

Added a new ByteBufferPool which pools N ByteBuffers. By default it makes off heap ByteBuffers when getBuffer() is called. The size of each buffer defaults to 64KB. This can be configured using 'hbase.ipc.server.reservoir.initial.buffer.size'.   The max number of buffers which can be pooled defaults to twice the number of handler threads in RS. This can be configured with key 'hbase.ipc.server.reservoir.initial.max'.  While responding to read requests and client support Codec, we will create CellBlocks and directly return it as PB payload. For making this block, we will use N ByteBuffers from pool as per the total size of the response cells. The default size of 64 KB for the buffer is inline with the number of bytes written to RPC layer in one short.(That is also 64KB).  When at point of time, the calle not able to get a free buffer from the pool (it returns null then), it will make on heap Buffer of same size (as that of Buffers in pool) and use that to create cell block.


---

* [HBASE-15994](https://issues.apache.org/jira/browse/HBASE-15994) | *Major* | **Allow selection of RpcSchedulers**

Adds a FifoRpcSchedulerFactory so you can try the FifoRpcScheduler by setting  "hbase.region.server.rpc.scheduler.factory.class"


---

* [HBASE-15989](https://issues.apache.org/jira/browse/HBASE-15989) | *Major* | **Remove hbase.online.schema.update.enable**

Removes the "hbase.online.schema.update.enable" property.
from now, every operation that alter the schema (e.g. modifyTable, addFamily, removeFamily, ...) will use the online schema update. there is no need to disable/enable the table.


---

* [HBASE-15981](https://issues.apache.org/jira/browse/HBASE-15981) | *Minor* | **Stripe and Date-tiered compactions inaccurately suggest disabling table in docs**

Removes reference to disabling table in docs for stripe and date-tiered compactions


---

* [HBASE-15931](https://issues.apache.org/jira/browse/HBASE-15931) | *Critical* | **Add log for long-running tasks in AsyncProcess**

After HBASE-15931, we will log more details for long-running tasks in AsyncProcess#waitForMaximumCurrentTasks every 10 seconds, including:
1. Table name will be included in the tasks status log
2. On which regionserver(s) the tasks are runnning will be logged when less than hbase.client.threshold.log.details tasks left, by default 10.
3. Against which regions the tasks are running will be logged when less than 2 tasks left.


---

* [HBASE-15907](https://issues.apache.org/jira/browse/HBASE-15907) | *Major* | **Missing documentation of create table split options**

documentation changes only - added section to Shell tricks and cross reference from region splitting section


---

* [HBASE-15915](https://issues.apache.org/jira/browse/HBASE-15915) | *Major* | **Set timeouts on hanging tests**

Use @ClassRule to set timeout on test case level (instead of @Rule which sets timeout for the test methods). CategoryBasedTimeout.forClass(..) determines the timeout value based on category annotation (small/medium/large) on the test case.


---

* [HBASE-15875](https://issues.apache.org/jira/browse/HBASE-15875) | *Major* | **Remove HTable references and HTableInterface**

**WARNING: No release note provided for this change.**


---

* [HBASE-15610](https://issues.apache.org/jira/browse/HBASE-15610) | *Blocker* | **Remove deprecated HConnection for 2.0 thus removing all PB references for 2.0**

**WARNING: No release note provided for this change.**


---

* [HBASE-15890](https://issues.apache.org/jira/browse/HBASE-15890) | *Major* | **Allow thrift to set/unset "cacheBlocks" for Scans**

Adds cacheBlocks to Scan


---

* [HBASE-15876](https://issues.apache.org/jira/browse/HBASE-15876) | *Blocker* | **Remove doBulkLoad(Path hfofDir, final HTable table) though it has not been through a full deprecation cycle**

Removes a doBulkLoad method though it has not been through a full deprecation cycle (but it is 'damaged' because it has a parameter that has been properly deprecated). Use the alternative {code}public void doBulkLoad(Path hfofDir, final Admin admin, Table table, RegionLocator regionLocator){code}

See http://mail-archives.apache.org/mod\_mbox/hbase-dev/201605.mbox/%3CCAMUu0w-ZiLoLBLO3D76=n3AjUr=VMtTUeYA28weLHYeq8+e3bQ@mail.gmail.com%3E for NOTICE on this 'premature' removal.


---

* [HBASE-15228](https://issues.apache.org/jira/browse/HBASE-15228) | *Major* | **Add the methods to RegionObserver to trigger start/complete restoring WALs**

Added two hooks around WAL restore.
preReplayWALs(final ObserverContext\<? extends RegionCoprocessorEnvironment\> ctx,  HRegionInfo info, Path edits)
and
postReplayWALs(final ObserverContext\<? extends RegionCoprocessorEnvironment\> ctx,  HRegionInfo info, Path edits)

Will be called at start and end of restore of a WAL file.
The other hook around WAL restore (preWALRestore ) will be called before restore of every entry within the WAL file.


---

* [HBASE-15856](https://issues.apache.org/jira/browse/HBASE-15856) | *Critical* | **Cached Connection instances can wind up with addresses never resolved**

During periods where DNS resolution was not available or not working correctly, we could previously cache unresolved hostnames forever, in some cases preventing further connections to these hosts even when DNS service was restored.  With this change, unresolved hostnames will no longer be cached, and will instead throw an UnknownHostException during connection setup.


---

* [HBASE-15593](https://issues.apache.org/jira/browse/HBASE-15593) | *Major* | **Time limit of scanning should be offered by client**

Add a new configuration: hbase.ipc.min.client.request.timeout
Minimum allowable timeout (in milliseconds) in rpc request's header. This configuration exists to prevent the rpc service regarding this request as timeout immediately.


---

* [HBASE-15784](https://issues.apache.org/jira/browse/HBASE-15784) | *Major* | **Misuse core/maxPoolSize of LinkedBlockingQueue in ThreadPoolExecutor**

The core pool size and max pool size of ThreadPoolExecutor should be the same when LinkedBlockingQueue is used. Thus the configurations hbase.hconnection.threads.max, hbase.hconnection.meta.lookup.threads.max, hbase.region.replica.replication.threads.max and hbase.multihconnection.threads.max are used as the number of the core threads, and the related configurations \*.thread.core are not used any more.


---

* [HBASE-15651](https://issues.apache.org/jira/browse/HBASE-15651) | *Major* | **Add report-flakies.py to use jenkins api to get failing tests**

To find recent set of flakies, run the script added by this patch. Run it to get usage information passing -h:

{code}
$ ./dev-support/report-flakies.py -h
{code}

If you get the below:

{code}
$ python ./dev-support/report-flakies.py
Traceback (most recent call last):
  File "./dev-support/report-flakies.py", line 25, in \<module\>
    import requests
ImportError: No module named requests
{code}

... install the requests module:

{code}
$ sudo pip install requests
{code}


---

* [HBASE-15780](https://issues.apache.org/jira/browse/HBASE-15780) | *Critical* | **Expose AuthUtil as IA.Public**

Downstream users with long lived applications that need to communicate with secure HBase instances can now rely on the AuthUtil class to handle authenticating via keytab.

For more information, see the javadoc for the org.apache.hadoop.hbase.AuthUtil class.


---

* [HBASE-15811](https://issues.apache.org/jira/browse/HBASE-15811) | *Blocker* | **Batch Get after batch Put does not fetch all Cells**

We were not waiting on all executors in a batch to complete which meant a read-your-own-writes could sometimes fail -- especially if client is loaded; i.e. putting to multiple machines in a cluster. The test for no-more-executors was damaged by the 0.99/0.98.4 fix "HBASE-11403 Fix race conditions around Object#notify"


---

* [HBASE-15801](https://issues.apache.org/jira/browse/HBASE-15801) | *Major* | **Upgrade checkstyle for all branches**

All active branches now use maven-checkstyle-plugin 2.17 and checkstyle 6.18.


---

* [HBASE-15236](https://issues.apache.org/jira/browse/HBASE-15236) | *Major* | **Inconsistent cell reads over multiple bulk-loaded HFiles**

This jira fixes that following bug:
During bulkloading, if there are multiple hfiles corresponding to same region, and if they have same timestamps (which may have been set using importtsv.timestamp) and duplicate keys across them, then get and scan may return values coming from different hfiles.


---

* [HBASE-15740](https://issues.apache.org/jira/browse/HBASE-15740) | *Major* | **Replication source.shippedKBs metric is undercounting because it is in KB**

Removed Replication source.shippedKBs metric in favor of source.shippedBytes


---

* [HBASE-15773](https://issues.apache.org/jira/browse/HBASE-15773) | *Major* | **CellCounter improvements**

The CellCounter map reduce job now supports additional configuration options on the Scan instance it creates, using the org.apache.hadoop.hbase.mapreduce.TableInputFormat defined property names.  For a full list of the options, run ./hbase org.apache.hadoop.hbase.mapreduce.CellCounter with no arguments.

CellCounter also no longer creates job counters for per-rowkey and per-rowkey/qualifier cell counts.  For most tables, these counters would cause the job to fail due to mapreduce job counter limits.


---

* [HBASE-15759](https://issues.apache.org/jira/browse/HBASE-15759) | *Minor* | **RegionObserver.preStoreScannerOpen() doesn't have acces to current readpoint**

The following RegionObserver method is deprecated and would no longer be called in hbase 2.0:

  public KeyValueScanner preStoreScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
      final Store store, final Scan scan, final NavigableSet\<byte[]\> targetCols,
      final KeyValueScanner s) throws IOException {

Instead, override this method:

  public KeyValueScanner preStoreScannerOpen(final ObserverContext\<RegionCoprocessorEnvironment\> c,
      final Store store, final Scan scan, final NavigableSet\<byte[]\> targetCols,
      final KeyValueScanner s, final long readPt) throws IOException {


---

* [HBASE-15743](https://issues.apache.org/jira/browse/HBASE-15743) | *Major* | **Add Transparent Data Encryption support for FanOutOneBlockAsyncDFSOutput**

Now the AsyncFSWAL can write data to a encryption zone on HDFS.


---

* [HBASE-15767](https://issues.apache.org/jira/browse/HBASE-15767) | *Major* | **Upgrade httpclient dependency**

HBase now relies on version 4.3.6 of the Apache Commons HTTPClient library. Downstream users who are exposed to it via the HBase classpath will have to similarly update their dependency.


---

* [HBASE-15575](https://issues.apache.org/jira/browse/HBASE-15575) | *Minor* | **Rename table DDL \*Handler methods in MasterObserver to more meaningful names**

**WARNING: No release note provided for this change.**


---

* [HBASE-15720](https://issues.apache.org/jira/browse/HBASE-15720) | *Major* | **Print row locks at the debug dump page**

Adds a section to the debug dump page listing current row locks held.


---

* [HBASE-15703](https://issues.apache.org/jira/browse/HBASE-15703) | *Critical* | **Deadline scheduler needs to return to the client info about skipped calls, not just drop them**

With previous deadline mode of RPC scheduling (the implementation in SimpleRpcScheduler, which is basically a FIFO except that long-running scans are de-prioritized) and FIFO-based RPC scheduler clients are getting CallQueueTooBigException when RPC call queue is full.

With this patch and when hbase.ipc.server.callqueue.type property is set to "codel" mode, clients will also be getting CallDroppedException, which means that the request was discarded by the server as it considers itself to be overloaded and starts to drop requests to avoid going down under the load. The clients will retry upon receiving this exception. It doesn't clear MetaCache with region locations.


---

* [HBASE-15281](https://issues.apache.org/jira/browse/HBASE-15281) | *Major* | **Allow the FileSystem inside HFileSystem to be wrapped**

This patch adds new configuration property - hbase.fs.wrapper. If provided, it should be fully qualified class name of the class used as a pluggable wrapper for HFileSystem. This may be useful for specific debugging/tracing needs.


---

* [HBASE-15551](https://issues.apache.org/jira/browse/HBASE-15551) | *Minor* | **Make call queue too big exception use servername**

Fixes issue when CallQueueTooBig exception returned to the client could print useless address info (like 0.0.0.0) if RPC server is listening on something other than the host name, making troubleshooting inconvenient.


---

* [HBASE-15711](https://issues.apache.org/jira/browse/HBASE-15711) | *Major* | **Add client side property to allow logging details for batch errors**

In HBASE-15711 a new client side property hbase.client.log.batcherrors.details is introduced to allow logging full stacktrace of exceptions for batch error. It's disabled by default and set the property to true will enable it.


---

* [HBASE-15686](https://issues.apache.org/jira/browse/HBASE-15686) | *Major* | **Add override mechanism for the exempt classes when dynamically loading table coprocessor**

New coprocessor table descriptor attribute, hbase.coprocessor.classloader.included.classes, is added.
User can specify class name prefixes (semicolon separated) which should be loaded by CoprocessorClassLoader through this attribute using the following syntax:
{code}
  hbase\> alter 't1',    'coprocessor'=\>'hdfs:///foo.jar\|com.foo.FooRegionObserver\|1001\|arg1=1,arg2=2'
{code}


---

* [HBASE-15645](https://issues.apache.org/jira/browse/HBASE-15645) | *Critical* | **hbase.rpc.timeout is not used in operations of HTable**

Fixes regression where hbase.rpc.timeout configuration was ignored in branch-1.0+

Adds new methods setOperationTimeout, getOperationTimeout, setRpcTimeout, and getRpcTimeout to Table. In branch-1.3+ they are public interfaces and in 1.0-1.2 they are labeled as @InterfaceAudience.Private.

Adds hbase.client.operation.timeout to hbase-default.xml with default of 1200000


---

* [HBASE-15477](https://issues.apache.org/jira/browse/HBASE-15477) | *Major* | **Do not save 'next block header' when we cache hfileblocks**

Fix over-persisting in blockcache; no longer save the block PLUS the header of the next block (33 bytes) when writing the cache.

Also removes support for hfileblock v1; hfile block v1 was used writing hfile v1. hfile v1 was the default in hbase before hbase-0.92. hbase.96 would not start unless all v1 hfiles had been compacted out of the cluster.


---

* [HBASE-15628](https://issues.apache.org/jira/browse/HBASE-15628) | *Major* | **Implement an AsyncOutputStream which can work with any FileSystem implementation**

Introduce an AsyncFSOutput interface which is an abstraction of the original FanOutOneBlockAsyncDFSOutput. Now you can create AsyncFSOutput on any FileSystem using the method AsyncFSOutputHelper.createOutput. The returned AsyncFSOutput will be FanOutOneBlockAsyncDFSOutput if the given FileSystem is a DistributedFileSystem.


---

* [HBASE-15392](https://issues.apache.org/jira/browse/HBASE-15392) | *Major* | **Single Cell Get reads two HFileBlocks**

When an explicit Get with a one or more columns specified, we at a minimum, were overseeking, reading until we tripped over the next row, regardless, and only then returning. If the next row was in-block, we'd just do too much seeking but if the next row was in the next (or in the next block beyond that), we would keep seeking and loading blocks until we found the next row before we'd return.

There remains one case where we will still 'overread'. It is when the row end aligns with the end of the block. In this case we will load the next block just to find that there are no more cells in the current row. See HBASE-15457.


---

* [HBASE-15671](https://issues.apache.org/jira/browse/HBASE-15671) | *Major* | **Add per-table metrics on memstore, storefile and regionsize**

Adds storeFileSize, memstoreSize and tableSize to the per-table metrics.


---

* [HBASE-15366](https://issues.apache.org/jira/browse/HBASE-15366) | *Major* | **Add doc, trace-level logging, and test around hfileblock**

No functional change. Added javadoc, comments, and extra trace-level logging to make clear what is happening around the reading and caching of hfile blocks.


---

* [HBASE-15368](https://issues.apache.org/jira/browse/HBASE-15368) | *Major* | **Add pluggable window support**

Use 'hbase.hstore.compaction.date.tiered.window.factory.class' to specify the window implementation you like for date tiered compaction. Now the only and default implementation is org.apache.hadoop.hbase.regionserver.compactions.ExponentialCompactionWindowFactory.

{code}
\<property\>
\<name\>hbase.hstore.compaction.date.tiered.window.factory.class\</name\>
\<value\>org.apache.hadoop.hbase.regionserver.compactions.ExponentialCompactionWindowFactory\</value\>
\</property\>
\<property\>
{code}


---

* [HBASE-15518](https://issues.apache.org/jira/browse/HBASE-15518) | *Major* | **Add Per-Table metrics back**

Adds per-table metrics aggregated from per-region metrics in region server metrics. New metrics are available under JMX section "Hadoop:service=HBase,name=RegionServer,sub=Tables" and they are available via hadoop metrics2 collectors.


---

* [HBASE-15640](https://issues.apache.org/jira/browse/HBASE-15640) | *Major* | **L1 cache doesn't give fair warning that it is showing partial stats only when it hits limit**

The blockcache UI tab would stop refreshing at 100k blocks (configurable, see "hbase.ui.blockcache.by.file.max"), which isn't very many blocks when doing a big cache, giving a misleading picture of the content of L1 and/or L2 cache. Up the default limit to 1M blocks (UI takes a while but just a few seconds counting over 1M blocks).

Also, when beyond the limit give the user a noticeable WARNING in the UI.


---

* [HBASE-15386](https://issues.apache.org/jira/browse/HBASE-15386) | *Major* | **PREFETCH\_BLOCKS\_ON\_OPEN in HColumnDescriptor is ignored**

This was a non-issue. The PREFETCH\_... flag actually works. While here though made the following additions.

Changes the prefetch TRACE-level loggings to include the word 'Prefetch' in them so you know what they are about.

Changes the cryptic logging of the CacheConfig#toString to have some preamble saying why and what column family is responsible (helps figure what is going on)

Add test that verifies setting flag on HColumnDescriptor actually works.


---

* [HBASE-13372](https://issues.apache.org/jira/browse/HBASE-13372) | *Major* | **Unit tests for SplitTransaction and RegionMergeTransaction listeners**

HBASE-13372 Add unit tests for SplitTransaction and RegionMergeTransaction listeners


---

* [HBASE-15187](https://issues.apache.org/jira/browse/HBASE-15187) | *Major* | **Integrate CSRF prevention filter to REST gateway**

Protection against CSRF attack can be turned on with config parameter, hbase.rest.csrf.enabled - default value is false.

The custom header to be sent can be changed via config parameter, hbase.rest.csrf.custom.header whose default value is "X-XSRF-HEADER".

Config parameter, hbase.rest.csrf.methods.to.ignore , controls which HTTP methods are not associated with customer header check.

Config parameter, hbase.rest-csrf.browser-useragents-regex , is a comma-separated list of regular expressions used to match against an HTTP request's User-Agent header when protection against cross-site request forgery (CSRF) is enabled for REST server by setting hbase.rest.csrf.enabled to true.

The implementation came from hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/http/RestCsrfPreventionFilter.java

We should periodically update the RestCsrfPreventionFilter.java in hbase codebase to include fixes to the hadoop implementation.


---

* [HBASE-15481](https://issues.apache.org/jira/browse/HBASE-15481) | *Trivial* | **Add pre/post roll to WALObserver**

<!-- markdown -->


WALObserver coprocessors now can receive notifications of WAL rolling via the new methods `preWALRoll` and `postWALRoll`.

This change is incompatible due to the addition of these methods to the `WALObserver` interface. Downstream users are encouraged to instead extend the `BaseWALObserver` class, which remains compatible through this change.


---

* [HBASE-15507](https://issues.apache.org/jira/browse/HBASE-15507) | *Major* | **Online modification of enabled ReplicationPeerConfig**

Added update\_peer\_config to the HBase shell and ReplicationAdmin, and provided a callback for custom replication endpoints to be notified of changes to their configuration and peer data


---

* [HBASE-15537](https://issues.apache.org/jira/browse/HBASE-15537) | *Major* | **Make multi WAL work with WALs other than FSHLog**

Add the delegate config for multiwal back. Now you can use 'hbase.wal.regiongrouping.delegate.provider' to specify the wal provider you want to use for multiwal. For example:
{code}
\<property\>
\<name\>hbase.wal.regiongrouping.delegate.provider\</name\>
\<value\>asyncfs\</value\>
\</property\>
{code}
And the default value is filesystem which is the alias of DefaultWALProvider, i.e., the FSHLog.


---

* [HBASE-15400](https://issues.apache.org/jira/browse/HBASE-15400) | *Major* | **Use DateTieredCompactor for Date Tiered Compaction**

With this patch combined with HBASE-15389, when we compact, we can output multiple files along the current window boundaries. There are two use cases:
1. Major compaction: We want to output date tiered store files with data older than max age archived in trunks of the window size on the higher tier. Once a window is old enough, we don't combine the windows to promote to the next tier any further. So files in these windows retain the same timespan as they were minor-compacted last time, which is the window size of the highest tier. Major compaction will touch these files and we want to maintain the same layout. This way, TTL and archiving will be simpler and more efficient.
2. Bulk load files and the old file generated by major compaction before upgrading to DTCP.

This will change the way to enable date tiered compaction.
To turn it on:
hbase.hstore.engine.class: org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine

With tiered compaction all servers in the cluster will promote windows to higher tier at the same time, so using a compaction throttle is recommended:
hbase.regionserver.throughput.controller:org.apache.hadoop.hbase.regionserver.compactions.PressureAwareCompactionThroughputController
hbase.hstore.compaction.throughput.higher.bound and hbase.hstore.compaction.throughput.lower.bound need to be set for desired throughput range as uncompressed rates.

Because there will most likely be more store files around, we need to adjust the configuration so that flush won't be blocked and compaction will be properly throttled:
hbase.hstore.blockingStoreFiles: change to 50 if using all default parameters when turning on date tiered compaction. Use 1.5~2 x projected file count if changing the parameters, Projected file count = windows per tier x tier count + incoming window min + files older than max age

Because major compaction is turned on now, we also need to adjust the configuration for max file to compact according to the larger file count:
hbase.hstore.compaction.max: set to the same number as hbase.hstore.blockingStoreFiles.

For more details, please refer to the design spec at https://docs.google.com/document/d/1\_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG\_uy8/edit#


---

* [HBASE-15592](https://issues.apache.org/jira/browse/HBASE-15592) | *Major* | **Print Procedure WAL content**

Use hbase org.apache.hadoop.hbase.procedure2.store.wal.ProcedureWALPrettyPrinter
to print the content of a Procedure WAL.
e.g.
hbase org.apache.hadoop.hbase.procedure2.store.wal.ProcedureWALPrettyPrinter -f /hbase/MasterProcWALs/state-00000000000000002571.log


---

* [HBASE-15396](https://issues.apache.org/jira/browse/HBASE-15396) | *Minor* | **Enhance mapreduce.TableSplit to add encoded region name**

To aid troubleshooting of MapReduce job that rely on the HBase provided input format, splits now include the encoded region name they cover.


---

* [HBASE-15568](https://issues.apache.org/jira/browse/HBASE-15568) | *Major* | **Procedure V2 - Remove CreateTableHandler in HBase Apache 2.0 release**

**WARNING: No release note provided for this change.**


---

* [HBASE-15521](https://issues.apache.org/jira/browse/HBASE-15521) | *Major* | **Procedure V2 - RestoreSnapshot and CloneSnapshot**

**WARNING: No release note provided for this change.**


---

* [HBASE-15538](https://issues.apache.org/jira/browse/HBASE-15538) | *Major* | **Implement secure async protobuf wal writer**

Add the following config in hbase-site.xml if you want to use secure protobuf wal writer together with AsyncFSWAL
{code}
\<property\>
\<name\>hbase.regionserver.hlog.async.writer.impl\</name\>
\<value\>org.apache.hadoop.hbase.regionserver.wal.SecureAsyncProtobufLogWriter\</value\>
\</property\>
\<property\>
{code}


---

* [HBASE-11393](https://issues.apache.org/jira/browse/HBASE-11393) | *Major* | **Replication TableCfs should be a PB object rather than a string**

**WARNING: No release note provided for this change.**


---

* [HBASE-15265](https://issues.apache.org/jira/browse/HBASE-15265) | *Major* | **Implement an asynchronous FSHLog**

To enable, set the WALProvider as follows:

{code}
\<property\>
\<name\>hbase.wal.provider\</name\>
\<value\>asyncfs\</value\>
\</property\>
\<property\>
{code}

To check which provider is active, look for the log line:

LOG.info("Instantiating WALProvider of type " + clazz);


---

* [HBASE-14256](https://issues.apache.org/jira/browse/HBASE-14256) | *Major* | **Flush task message may be confusing when region is recovered**

HBASE-14256 Correct confusing flush task message


---

* [HBASE-15212](https://issues.apache.org/jira/browse/HBASE-15212) | *Major* | **RPCServer should enforce max request size**

Adds a configuration parameter "hbase.ipc.max.request.size" which defaults to 256MB to protect the server against very large incoming RPC requests. All requests larger than this size will be immediately rejected before allocating any resources (memory allocation, etc).


---

* [HBASE-15412](https://issues.apache.org/jira/browse/HBASE-15412) | *Major* | **Add average region size metric**

Adds a new metric for called "averageRegionSize" that is emitted as a regionserver metric. Metric description:
Average region size over the region server including memstore and storefile sizes


---

* [HBASE-15479](https://issues.apache.org/jira/browse/HBASE-15479) | *Major* | **No more garbage or beware of autoboxing**

This fix decreases client's memory allocation during writes by more than 50%.


---

* [HBASE-15322](https://issues.apache.org/jira/browse/HBASE-15322) | *Critical* | **Operations using Unsafe path broken for platforms not having sun.misc.Unsafe**

**WARNING: No release note provided for this change.**


---

* [HBASE-12940](https://issues.apache.org/jira/browse/HBASE-12940) | *Major* | **Expose listPeerConfigs and getPeerConfig to the HBase shell**

Adds get\_peer\_config and list\_peer\_configs to the hbase shell.


---

* [HBASE-15430](https://issues.apache.org/jira/browse/HBASE-15430) | *Critical* | **Failed taking snapshot - Manifest proto-message too large**

Failed taking snapshot - Manifest proto-message too large. add property ("snapshot.manifest.size.limit")  to change max size of proto-message


---

* [HBASE-15323](https://issues.apache.org/jira/browse/HBASE-15323) | *Major* | **Hbase Rest CheckAndDeleteAPi should be able to delete more cells**

Fixed an issue in REST server checkAndDelete operation where the remaining cells other than the to-be-checked column are also applied in the Delete operation. Also fixed an issue in RemoteHTable where the Delete object was not passed correctly to the REST server side.


---

* [HBASE-15377](https://issues.apache.org/jira/browse/HBASE-15377) | *Major* | **Per-RS Get metric is time based, per-region metric is size-based**

Per-region metrics related to Get histograms are changed from being response size based into being latency based similar to the per-regionserver metrics of the same name.

Added GetSize histogram metrics at the per-regionserver and per-region level for the response sizes.


---

* [HBASE-6721](https://issues.apache.org/jira/browse/HBASE-6721) | *Major* | **RegionServer Group based Assignment**

[ADVANCED USERS ONLY] This patch adds a new experimental module hbase-rsgroup. It is an advanced feature for partitioning regionservers into distinctive groups for strict isolation, and should only be used by users who are sophisticated enough to understand the full implications and have a sufficient background in managing HBase clusters.

RSGroups can be defined and managed with shell commands or corresponding Java APIs. A server can be added to a group with hostname and port pair, and tables can be moved to this group so that only regionservers in the same rsgroup can host the regions of the table. RegionServers and tables can only belong to 1 group at a time. By default, all tables and regionservers belong to the "default" group. System tables can also be put into a group using the regular APIs. A custom balancer implementation tracks assignments per rsgroup and makes sure to move regions to the relevant regionservers in that group. The group information is stored in a regular HBase table, and a zookeeper-based read-only cache is used at the cluster bootstrap time.

To enable, add the following to your hbase-site.xml and restart your Master:


 \<property\>
   \<name\>hbase.coprocessor.master.classes\</name\>
   \<value\>org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint\</value\>
 \</property\>
 \<property\>
   \<name\>hbase.master.loadbalancer.class\</name\>
   \<value\>org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer\</value\>
 \</property\>


Then use the shell 'rsgroup' commands to create and manipulate regionserver groups: e.g. to add a group and then add a server to it, do as follows:

 hbase(main):008:0\> add\_rsgroup 'my\_group'
 Took 0.5610 seconds

This adds a group to the 'hbase:rsgroup' system table. Add a server (hostname + port) to the group using the 'move\_rsgroup\_servers' command as follows:

 hbase(main):010:0\> move\_rsgroup\_servers 'my\_group',['k.att.net:51129']


---

* [HBASE-15435](https://issues.apache.org/jira/browse/HBASE-15435) | *Major* | **Add WAL (in bytes) written metric**

Adds a new metric named "writtenBytes" as a per-regionserver metric. Metric Description:
Size (in bytes) of the data written to the WAL.


---

* [HBASE-13963](https://issues.apache.org/jira/browse/HBASE-13963) | *Critical* | **avoid leaking jdk.tools**

HBase now ensures that the JDK tools jar used during the build process is not exposed to downstream clients as a transitive dependency of hbase-annotations.

If you need to have the JDK tools jar in your classpath, you should add a system dependency on it. See the hbase-annotations pom for an example of the necessary pom additions.


---

* [HBASE-15271](https://issues.apache.org/jira/browse/HBASE-15271) | *Major* | **Spark Bulk Load: Need to write HFiles to tmp location then rename to protect from Spark Executor Failures**

When using the bulk load helper provided by the hbase-spark module, output files will now be written into temporary files and only made available when the executor has successfully completed.

Previously, failed executors would leave their files in place in a way that would be picked up by a bulk load command. This caused retried failures to include spurious copies of some cells.


---

* [HBASE-15364](https://issues.apache.org/jira/browse/HBASE-15364) | *Major* | **Fix unescaped \< characters in Javadoc**

HBASE-15364 Fix unescaped \< and \> characters in Javadoc


---

* [HBASE-15243](https://issues.apache.org/jira/browse/HBASE-15243) | *Major* | **Utilize the lowest seek value when all Filters in MUST\_PASS\_ONE FilterList return SEEK\_NEXT\_USING\_HINT**

When all filters in a MUST\_PASS\_ONE FilterList return a SEEK\_USING\_NEXT\_HINT code, we return SEEK\_NEXT\_USING\_HINT from the FilterList#filterKeyValue() to utilize the lowest seek value.


---

* [HBASE-15354](https://issues.apache.org/jira/browse/HBASE-15354) | *Major* | **Use same criteria for clearing meta cache for all operations**

This patch fixes some issues when MetaCache (region location cache) gets unnecessarily dropped on the client.

On master branch we now in RegionServerCallable and RegionServerAdminCallable pass the actual exception down to Connection#updateCachedLocation, so we could check there if the exception is "meta-clearing" or not.

on branch-1, branch-1.2 and branch 1.3 we now check if the exception is meta-clearing or not in AsyncProcess (this check was there on master, but not on earlier branches)


---

* [HBASE-15376](https://issues.apache.org/jira/browse/HBASE-15376) | *Major* | **ScanNext metric is size-based while every other per-operation metric is time based**

Removed ScanNext histogram metrics as regionserver level and per-region level metrics since the semantics is not compatible with other similar metrics (size histogram vs latency histogram).

Instead, this patch adds ScanTime and ScanSize histogram metrics at the regionserver and per-region level.


---

* [HBASE-15338](https://issues.apache.org/jira/browse/HBASE-15338) | *Minor* | **Add a option to disable the data block cache for testing the performance of underlying file system**

Add a new config: hbase.block.data.cacheonread, which is a global switch for caching data blocks on read. The default value of this switch is true, and data blocks will be cached on read if the block cache is enabled for the family and cacheBlocks flag is set to be true for get and scan operations. If this global switch is set to false, data blocks won't be cached even if the block cache is enabled for the family and the cacheBlocks flag of Gets or Scans are sets as true. Bloom blocks and index blocks are always be cached if the block cache of the regionserver is enabled. One usage of this switch is for the performance tests for the extreme case that  the cache for data blocks all missed and all data blocks are read from underlying file system.


---

* [HBASE-15136](https://issues.apache.org/jira/browse/HBASE-15136) | *Critical* | **Explore different queuing behaviors while busy**

Previously RPC request scheduler in HBase had 2 modes in could operate in:

 - simple FIFO
 - "partial" deadline, where deadline constraints are only imposed on long-running scan requests.

This patch adds new type of scheduler to HBase, based on the research around controlled delay (CoDel) algorithm [1], used in networking to combat bufferbloat, as well as some analysis on generalizing it to generic request queues [2]. The purpose of that work is to prevent long standing call queues caused by discrepancy between request rate and available throughput, caused by kernel/disk IO/networking stalls.

New RPC scheduler could be enabled by setting hbase.ipc.server.callqueue.type=codel in configuration. Several additional params allow to configure algorithm behavior -

hbase.ipc.server.callqueue.codel.target.delay
hbase.ipc.server.callqueue.codel.interval
hbase.ipc.server.callqueue.codel.lifo.threshold

[1] Controlling Queue Delay / A modern AQM is just one piece of the solution to bufferbloat. http://queue.acm.org/detail.cfm?id=2209336
[2] Fail at Scale / Reliability in the face of rapid change. http://queue.acm.org/detail.cfm?id=2839461


---

* [HBASE-15181](https://issues.apache.org/jira/browse/HBASE-15181) | *Major* | **A simple implementation of date based tiered compaction**

Date tiered compaction policy is a date-aware store file layout that is beneficial for time-range scans for time-series data.

When it performs well:

    reads for limited time ranges, especially scans of recent data

When it doesn't perform as well:

    random gets without a time range
    frequent deletes and updates
    out of order data writes, especially writes with timestamps in the future
    bulk loads of historical data

Recommended configuration:
To turn on Date Tiered Compaction (It is not recommended to turn on for the whole cluster because that will put meta table on it too and random get on meta table will be impacted):
hbase.hstore.compaction.compaction.policy: org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionPolicy

Parameters for Date Tiered Compaction:
hbase.hstore.compaction.date.tiered.max.storefile.age.millis: Files with max-timestamp smaller than this will no longer be compacted.Default at Long.MAX\_VALUE.
hbase.hstore.compaction.date.tiered.base.window.millis: base window size in milliseconds. Default at 6 hours.
hbase.hstore.compaction.date.tiered.windows.per.tier: number of windows per tier. Default at 4.
hbase.hstore.compaction.date.tiered.incoming.window.min: minimal number of files to compact in the incoming window. Set it to expected number of files in the window to avoid wasteful compaction. Default at 6.
hbase.hstore.compaction.date.tiered.window.policy.class: the policy to select store files within the same time window. It doesn’t apply to the incoming window. Default at exploring compaction. This is to avoid wasteful compaction.

With tiered compaction all servers in the cluster will promote windows to higher tier at the same time, so using a compaction throttle is recommended:
hbase.regionserver.throughput.controller:org.apache.hadoop.hbase.regionserver.compactions.PressureAwareCompactionThroughputController

Because there will most likely be more store files around, we need to adjust the configuration so that flush won't be blocked and compaction will be properly throttled:
hbase.hstore.blockingStoreFiles: change to 50 if using all default parameters when turning on date tiered compaction. Use 1.5~2 x projected file count if changing the parameters, Projected file count = windows per tier x tier count + incoming window min + files older than max age

For more details, please refer to the design spec at https://docs.google.com/document/d/1\_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG\_uy8/edit#


---

* [HBASE-15290](https://issues.apache.org/jira/browse/HBASE-15290) | *Major* | **Hbase Rest CheckAndAPI should save other cells along with compared cell**

Fixed an issue in REST server checkAndPut operation where the remaining cells other than the to-be-checked column are also applied in the put operation .


---

* [HBASE-15264](https://issues.apache.org/jira/browse/HBASE-15264) | *Major* | **Implement a fan out HDFS OutputStream**

Implement a fan-out asynchronous DFSOutputStream for implementing new WAL writer.


---

* [HBASE-13259](https://issues.apache.org/jira/browse/HBASE-13259) | *Critical* | **mmap() based BucketCache IOEngine**

mmap() based bucket cache can be configured by specifying the property
{code}
\<property\>
  \<name\>hbase.bucketcache.ioengine\</name\>
  \<value\> mmap://filepath \</value\>
\</property\>
{code}
This mode of bucket cache is ideal when your file based bucket cache size is lesser than then available RAM. When the cache is bigger than the available RAM then the kernel page faults will make this cache perform lesser particularly in case of scans.


---

* [HBASE-11927](https://issues.apache.org/jira/browse/HBASE-11927) | *Major* | **Use Native Hadoop Library for HFile checksum (And flip default from CRC32 to CRC32C)**

Checksumming is cpu intensive. HBase computes additional checksums for HFiles (hdfs does checksums too) and stores them inline with file data. During reading, these checksums are verified to ensure data is not corrupted. This patch tries to use Hadoop Native Library for checksum computation, if it’s available, otherwise falls back to standard Java libraries. Instructions to load NHL in HBase can be found here (http://hbase.apache.org/book.html#hadoop.native.lib).

Default checksum algorithm has been changed from CRC32 to CRC32C primarily because of two reasons: 1) CRC32C has better error detection properties, and 2) New Intel processors have a dedicated instruction for crc32c computation (SSE4.2 instruction set)\*. This change is fully backward compatible. Also, users should not see any differences except decrease in cpu usage. To keep old settings, set configuration ‘hbase.hstore.checksum.algorithm’ to ‘CRC32’.

\* On linux, run 'cat /proc/cpuinfo’ and look for sse4\_2 in list of flags to see if your processor supports SSE4.2.


---

* [HBASE-15219](https://issues.apache.org/jira/browse/HBASE-15219) | *Critical* | **Canary tool does not return non-zero exit code when one of regions is in stuck state**

A new flag is added for Canary tool: -treatFailureAsError
When this flag is specified, read / write failure would result in Canary tool exit code of 5.


---

* [HBASE-14949](https://issues.apache.org/jira/browse/HBASE-14949) | *Major* | **Resolve name conflict when splitting if there are duplicated WAL entries**

Now we can write duplicated WAL entries into different WAL files. This feature is required by the replication consistency fix and new implementation of WAL writer.


---

* [HBASE-15100](https://issues.apache.org/jira/browse/HBASE-15100) | *Blocker* | **Master WALProcs still never clean up**

The constructor for o.a.h.hbase.ProcedureInfo was mistakenly labeled IA.Public in previous releases and has now changed to IA.Private. Downstream users are safe to consume ProcedureInfo objects returned from HBase public interfaces, but should not expect to be able to reliably create new instances themselves.

The method ProcedureInfo.setNonceKey has been removed, because it should not have been exposed to clients.


---

* [HBASE-14355](https://issues.apache.org/jira/browse/HBASE-14355) | *Major* | **Scan different TimeRange for each column family**

Adds being able to Scan each column family with a different time range. Adds new methods setColumnFamilyTimeRange and getColumnFamilyTimeRange to Scan.


---

* [HBASE-14460](https://issues.apache.org/jira/browse/HBASE-14460) | *Critical* | **[Perf Regression] Merge of MVCC and SequenceId (HBASE-8763) slowed Increments, CheckAndPuts, batch operations**

This release note tries to tell the general story. Dive into sub-tasks for more specific release noting.

Increments, appends, checkAnd\* have been slow since hbase-.1.0.0. The unification of mvcc and sequence id done by HBASE-8763 was responsible.

A ‘fast-path’ workaround was added by HBASE-15031 “Fix merge of MVCC and SequenceID performance regression in branch-1.0 for Increments”. It became available in 1.0.3 and 1.1.3. To enable the fast path, set "hbase.increment.fast.but.narrow.consistency" and then rolling restart. The workaround was for increments only (appends, checkAndPut, etc., were not addressed. See HBASE-15031 release note for more detail).

Subsequently, the regression was properly identified and fixed in HBASE-15213 and the fix applied to branch-1.0 and branch-1.1. As it happens, hbase-1.2.0 does not suffer from the performance regression (though the thought was that it did -- and so it got the fast-path patch too via HBASE-15092) nor does the master branch. HBASE-15213 identified that HBASE-12751 (as a side effect) had cured the regression.

hbase-1.0.4 (if it is ever released -- 1.0 has been end-of-lifed) and hbase-1.1.4 will have the HBASE-15213 fix.  If you are suffering from the increment regression and you are on 1.0.3 or 1.1.3, you can enable the work around to get back your increment performance but you should upgrade.


---

* [HBASE-15046](https://issues.apache.org/jira/browse/HBASE-15046) | *Major* | **Perf test doing all mutation steps under row lock**

In here we perf tested a realignment of the write pipeline and mvcc handling.  Thought was that this work was a predicate for a general fix of HBASE-14460 (turns out, realignment of write path was not needed to fix the increment perf regression). The perf testing here made it so we were able to simplify writing. HBASE-15158 was just committed. This work is done.


---

* [HBASE-15158](https://issues.apache.org/jira/browse/HBASE-15158) | *Major* | **Change order in which we do write pipeline operations; do all under row locks!**

Changed the write pipeline order; made it more rational, easier-to-reason-about doing all updates to WA, MemStore, and mvcc while read/write rowlock is held where before we'd release after WAL append and then do sync and mvcc.


---

* [HBASE-15157](https://issues.apache.org/jira/browse/HBASE-15157) | *Major* | **Add \*PerformanceTest for Append, CheckAnd\***

Add append, increment, checkAndMutate, checkAndPut, and checkAndDelete tests to PerformanceEvaluation tool. Below are excerpts from new usage from PE:

....
Command:
 append          Append on each row; clients overlap on keyspace so some concurrent operations
 checkAndDelete  CheckAndDelete on each row; clients overlap on keyspace so some concurrent operations
 checkAndMutate  CheckAndMutate on each row; clients overlap on keyspace so some concurrent operations
 checkAndPut     CheckAndPut on each row; clients overlap on keyspace so some concurrent operations
 filterScan      Run scan test using a filter to find a specific row based on it's value (make sure to use --rows=20)
 increment       Increment on each row; clients overlap on keyspace so some concurrent operations
 randomRead      Run random read test
....
Examples:
...
 To run 10 clients doing increments over ten rows:
 $ bin/hbase org.apache.hadoop.hbase.PerformanceEvaluation --rows=10 --nomapred increment 10

Removed IncrementPerformanceTest. It is not as configurable as the additions made here.


---

* [HBASE-15218](https://issues.apache.org/jira/browse/HBASE-15218) | *Blocker* | **On RS crash and replay of WAL, loosing all Tags in Cells**

This issue fixes
- In case of normal WAL (Not encrypted) we were loosing all cell tags on WAL replay after an RS crash
- In case of encrypted WAL we were not even persisting Cell tags in WAL.  Tags from all unflushed (to HFile) Cells will get lost even after WAL replay recovery is done.

As we use tags for Cell level security, this fixes 2 security issues
 - Cell level visibility labels security breach . Making a visibility restricted cell global readable
 - Cell level ACL availability issue.  A user who is cell level authorized to read this cell can not read it. It is a data loss for him.


---

* [HBASE-15129](https://issues.apache.org/jira/browse/HBASE-15129) | *Major* | **Set default value for hbase.fs.tmp.dir rather than fully depend on hbase-default.xml**

Before HBASE-15129, if somehow hbase-default.xml is not on classpath, default values for hbase.fs.tmp.dir and hbase.bulkload.staging.dir are left empty. After HBASE-15129,  default values of both properties are set to "/user/\<user.name\>/hbase-staging".


---

* [HBASE-14969](https://issues.apache.org/jira/browse/HBASE-14969) | *Major* | **Add throughput controller for flush**

Adds means of throttling flush throughput. By default there is no limit; we use NoLimitThroughputController. An alternative controller, PressureAwareFlushThroughputController, allows specifying throughput bounds. A new simple factor, flush pressure, influences throughput. See PressureAwareFlushThroughputController.java class for detail.


---

* [HBASE-11425](https://issues.apache.org/jira/browse/HBASE-11425) | *Major* | **Cell/DBB end-to-end on the read-path**

For E2E off heaped read path, first of all there should be an off heap backed BucketCache(BC). Configure 'hbase.bucketcache.ioengine' to offheap in hbase-site.xml. Also specify the total capacity of the BC using hbase.bucketcache.size config.  Please remember to adjust value of 'HBASE\_OFFHEAPSIZE' in hbase-env.sh as per this capacity. Here-by we specify the max possible off-heap memory allocation for the RS java process. So this should be bigger than the off-heap BC size. Please keep in mind that there is no default for hbase.bucketcache.ioengine which means the BC is turned OFF by default.

Next thing to tune is the ByteBuffer pool in the RPC server side. The buffers from this pool will be used to accumulate the cell bytes and create a result cell block to send back to the client side. 'hbase.ipc.server.reservoir.enabled' can be used to turn this pool ON or OFF. By default this pool is ON and available. HBase will create off heap ByteBuffers and pool them. Please make sure not to turn this OFF if you want E2E off heaping in read path. If this pool is turned off, the server will create temp buffers on heap to accumulate the cell bytes and make a result cell block. This can impact the GC on a highly read loaded server.  The user can tune this pool with respect to how many buffers are in the pool and what should be the size of each ByteBuffer.
Use the config 'hbase.ipc.server.reservoir.initial.buffer.size' to tune each of the buffer sizes. Defaults is 64 KB.

When the read pattern is a random row read and each of the rows are smaller in size compared to this 64 KB, try reducing this. When the result size is larger than one ByteBuffer size, the server will try to grab more than one buffer and make a result cell block out of these.  When the pool is running out of buffers, the server will end up creating temporary on-heap buffers.

The maximum number of ByteBuffers in the pool can be tuned using the config 'hbase.ipc.server.reservoir.initial.max'. Its value defaults to 64 \* region server handlers configured (See the config 'hbase.regionserver.handler.count'). The math is such that by default we consider 2 MB as the result cell block size per read result and each handler will be handling a read. For 2 MB size, we need 32 buffers each of size 64 KB (See default buffer size in pool).  So per handler 32 ByteBuffers(BB). We allocate twice this size as the max BBs count such that one handler can be creating the response and handing it to the RPC Responder thread and then handling a new request creating a new response cell block (using pooled buffers). Even if the responder could not send back the first TCP reply immediately, our count should allow that we should still have enough buffers in our pool without having to make temporary buffers on the heap.  Again for smaller sized random row reads, tune this max count. There are lazily created buffers and the count is the max count to be pooled.

The setting for HBASE\_OFFHEAPSIZE in hbase-env.sh should consider this off heap buffer pool at the RPC side also.  We need to config this max off heap size for RS as a bit higher than the sum of this max pool size and the off heap cache size. The TCP layer will also need to create direct bytebuffers for TCP communication. Also the DFS client will need some off-heap to do its workings especially if short-circuit reads are configured. Allocating an extra of 1 - 2 GB for the max direct memory size has worked in tests.

If you still see GC issues even after making E2E read path off heap, look for issues in the appropriate buffer pool. Check the below RS log with INFO level:

  "Pool already reached its max capacity : XXX and no free buffers now. Consider increasing the value for 'hbase.ipc.server.reservoir.initial.max' ?"

If you are using co processors and refer the Cells in the read results, DO NOT store reference to these Cells out of the scope of the CP hook methods. Some times the CPs need store info about the cell (Like its row key) for considering in the next CP hook call etc. For such cases, pls clone the required fields of the entire Cell as per the use cases.  [ See CellUtil#cloneXXX(Cell) APIs ]


---

* [HBASE-15145](https://issues.apache.org/jira/browse/HBASE-15145) | *Major* | **HBCK and Replication should authenticate to zookepeer using server principal**

Added a new command line argument: --auth-as-server to enable authenticating to ZooKeeper as the HBase Server principal. This is required for secure clusters for doing replication operations like add\_peer, list\_peers, etc until HBASE-11392 is fixed. This advanced option can also be used for manually fixing secure znodes.

Commands can now be invoked like:
hbase --auth-as-server shell
hbase --auth-as-server zkcli

HBCK in secure setup also needs to authenticate to ZK using servers principals.This is turned on by default (no need to pass additional argument).

When authenticating as server, HBASE\_SERVER\_JAAS\_OPTS is concatenated to HBASE\_OPTS if defined in hbase-env.sh. Otherwise, HBASE\_REGIONSERVER\_OPTS is concatenated.


---

* [HBASE-15125](https://issues.apache.org/jira/browse/HBASE-15125) | *Major* | **HBaseFsck's adoptHdfsOrphan function creates region with wrong end key boundary**

**WARNING: No release note provided for this change.**


---

* [HBASE-13082](https://issues.apache.org/jira/browse/HBASE-13082) | *Major* | **Coarsen StoreScanner locks to RegionScanner**

After this JIRA we will not be doing any scanner reset after compaction during a course of a scan. The files that were compacted will still be continued to be used in the scan process. The compacted files will be archived by a background thread that runs every 2 mins by default only when there are no active scanners on those comapcted files. The above duration can be controlled using the knob 'hbase.hfile.compactions.cleaner.interval'.


---

* [HBASE-14865](https://issues.apache.org/jira/browse/HBASE-14865) | *Major* | **Support passing multiple QOPs to SaslClient/Server via hbase.rpc.protection**

With this patch, hbase.rpc.protection can now take multiple comma-separate QOP values. Accepted QOP values remain unchanged and are 'authentication', 'integrity', and 'privacy'. Server or client can use this configuration to specify their preference (in decreasing order) while negotiating QOP.
This feature can be used to upgrade or downgrade QOP in an online cluster without compromising availability (i.e. taking cluster offline). For e.g. to change qop from A to B, typical steps would be:
"A" --\> "B,A" --\> rolling restart --\> "B" --\> rolling restart

Sidenote: Based on experimentation, server's choice is given higher preference than client's choice. i.e. if server's choices are "A,B,C" and client's choices are "B,C,A", both A and B are acceptable, but A is chosen.


---

* [HBASE-15098](https://issues.apache.org/jira/browse/HBASE-15098) | *Blocker* | **Normalizer switch in configuration is not used**

The config parameter, hbase.normalizer.enabled, has been dropped since it is not used in the code base.


---

* [HBASE-15111](https://issues.apache.org/jira/browse/HBASE-15111) | *Trivial* | **"hbase version" should write to stdout**

The \`hbase version\` command now outputs directly to stdout rather than to a logger. This change allows the version information to be output consistently regardless of logger configuration. Naturally, this also means the command output ignores all logger configuration. Furthermore, the move from loggers to direct output changes the output of the command to omit metadata commonly included in logger ouput such as a timestamp, log level, and logger name.


---

* [HBASE-15027](https://issues.apache.org/jira/browse/HBASE-15027) | *Major* | **Refactor the way the CompactedHFileDischarger threads are created**

The property 'hbase.hfile.compactions.discharger.interval' has been renamed to 'hbase.hfile.compaction.discharger.interval' that describes the interval after which the compaction discharger chore service should run.
The property 'hbase.hfile.compaction.discharger.thread.count' describes the thread count that does the compaction discharge work.
The CompactedHFilesDischarger is a chore service now started as part of the RegionServer and this chore service iterates over all the onlineRegions in that RS and uses the RegionServer's executor service to launch a set of threads that does this job of compaction files clean up.


---

* [HBASE-14468](https://issues.apache.org/jira/browse/HBASE-14468) | *Major* | **Compaction improvements: FIFO compaction policy**

FIFO compaction policy selects only files which have all cells expired. The column family MUST have non-default TTL.
Essentially, FIFO compactor does only one job: collects expired store files.

Because we do not do any real compaction, we do not use CPU and IO (disk and network), we do not evict hot data from a block cache. The result: improved throughput and latency both write and read.
See: https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style


---

* [HBASE-14888](https://issues.apache.org/jira/browse/HBASE-14888) | *Major* | **ClusterSchema: Add Namespace Operations**

This patch changes the semantic around namespace create/delete/modify when coprocessor asks that the invocation be by-passed. Previous the by-pass was done silently -- the method would just return with no indication as to whether by-pass route had been taken or not.  This patch adds throwing of a BypassCoprocessorException which is thrown if we have been asked to bypass a call.

The bypass facility has been in place since hbase 1.0.0 when namespace creation/deletion, etc.., was originally added in HBASE-8408 (HBASE-15071 is about addressing bypass handling in a general way)


---

* [HBASE-15018](https://issues.apache.org/jira/browse/HBASE-15018) | *Major* | **Inconsistent way of handling TimeoutException in the rpc client implementations**

When using the new AsyncRpcClient introduced in HBase 1.1.0 (HBASE-12684), time outs now result in an IOException wrapped around a CallTimeoutException instead of a bare CallTimeoutException. This change makes the AsyncRpcClient behave the same as the default HBase 1.y RPC client implementation.


---

* [HBASE-14796](https://issues.apache.org/jira/browse/HBASE-14796) | *Minor* | **Enhance the Gets in the connector**

spark.hbase.bulkGetSize  in HBaseSparkConf is for grouping bulkGet, and default value is 1000.


---

* [HBASE-14976](https://issues.apache.org/jira/browse/HBASE-14976) | *Minor* | **Add RPC call queues to the web ui**

Adds column displaying current aggregated call queues size in region server queues tab UI.


---

* [HBASE-14822](https://issues.apache.org/jira/browse/HBASE-14822) | *Major* | **Renewing leases of scanners doesn't work**

And 1.1, 1.0, and 0.98.


---

* [HBASE-14205](https://issues.apache.org/jira/browse/HBASE-14205) | *Critical* | **RegionCoprocessorHost System.nanoTime() performance bottleneck**

**WARNING: No release note provided for this change.**


---

* [HBASE-14978](https://issues.apache.org/jira/browse/HBASE-14978) | *Blocker* | **Don't allow Multi to retain too many blocks**

Limiting the amount of memory resident for any one request allows the server to handle concurrent requests smoothly. To this end we added the ability to limit the size of responses to a multi request. That worked well however it correctly represent the amount of memory resident. So this issue adds on a an approximation of the number of blocks held for a request.

All clients before 1.2.0 will not get this multi request chunking based upon blocks kept. All clients 1.2.0 and after will.


---

* [HBASE-14951](https://issues.apache.org/jira/browse/HBASE-14951) | *Minor* | **Make hbase.regionserver.maxlogs obsolete**

Rolling WAL events across a cluster can be highly correlated, hence flushing memstores, hence triggering minor compactions, that can be promoted to major ones. These events are highly correlated in time if there is a balanced write-load on the regions in a table. Default value for maximum WAL files (\* hbase.regionserver.maxlogs\*), which controls WAL rolling events - 32 is too small for many modern deployments.
Now we calculate this value dynamically (if not defined by user), using the following formula:

maxLogs = Math.max( 32, HBASE\_HEAP\_SIZE \* memstoreRatio \* 2/ LogRollSize), where

memstoreRatio is \*hbase.regionserver.global.memstore.size\*
LogRollSize is maximum WAL file size (default 0.95 \* HDFS block size)

We need to make sure that we avoid fully or minimize events when RS has to flush memstores prematurely only because it reached artificial limit of hbase.regionserver.maxlogs, this is why we put this 2 x multiplier in equation, this gives us maximum WAL capacity of 2 x RS memstore-size.

Runaway WAL files.

The default log rolling period (1h) allows to accumulate up to 2 X Memstore Size data in a WAL. For heap size - 32G and all other default setting, this gives ~ 26GB of data. Under heavy write load, the number of WAL files can increase dramatically. RegionServer LogRoller will be archiving old WALs periodically. User has three options, either override default hbase.regionserver.maxlogs or override default hbase.regionserver.logroll.period (decrease), or both to control runaway WALs.

For system with bursty write load,  the hbase.regionserver.logroll.period can be decreased to lower value. In this case the maximum number of wal files will be defined by the total size of memstore (unflushed data), not by the hbase.regionserver.maxlogs. But for majority of applications there will be no issues with defaults. Data will be flushed periodically from memstore, the LogRoller will archive old wal files and the system will never reach the new defaults for hbase.regionserver.maxlogs, unless the system is under extreme load for prolonged period of time, but in this case, decreasing hbase.regionserver.logroll.period allows us to control runaway wal files.

The following table gives the new default maximum log files values for several different Region Server heap sizes:

heap	memstore perc	maxLogs
1G	        40%	                        32
2G	        40%	                        32
10G	        40%	                        80
20G	        40%	                        160
32G	        40%	                        256


---

* [HBASE-14984](https://issues.apache.org/jira/browse/HBASE-14984) | *Major* | **Allow memcached block cache to set optimze to false**

Setting hbase.cache.memcached.spy.optimze to true will allow the spy memcached client to try and optimize for the number of requests outstanding. This can increase throughput but can also increase variance for request times.

Setting it to true will help when round trip times are longer.
Setting it to false ( the default ) will help ensure a more even distribution of response times.


---

* [HBASE-14534](https://issues.apache.org/jira/browse/HBASE-14534) | *Minor* | **Bump yammer/coda/dropwizard metrics dependency version**

Updated yammer metrics to version 3.1.2 (now it's been renamed to dropwizard). API has changed quite a bit, consult https://dropwizard.github.io/metrics/3.1.0/manual/core/ for additional information.

Note that among other things, in yammer 2.2.0 histograms were by default created in non-biased mode (uniform sampling), while in 3.1.0 histograms created via MetricsRegistry.histogram(...) are by default exponentially decayed. This shouldn't affect end users, though.


---

* [HBASE-14960](https://issues.apache.org/jira/browse/HBASE-14960) | *Major* | **Fallback to using default RPCControllerFactory if class cannot be loaded**

If the configured RPC controller factory (via hbase.rpc.controllerfactory.class) cannot be found in the classpath or loaded, we fall back to using the default RPC controller factory in HBase.


---

* [HBASE-14946](https://issues.apache.org/jira/browse/HBASE-14946) | *Critical* | **Don't allow multi's to over run the max result size.**

The HBase region server will now send a chunk of get responses to a client if the total response size is too large. This will only be done for clients 1.2.0 and beyond. Older clients by default will have the old behavior.

This patch is for the case where the basic flow is like this:

I want to get a single column from lots of rows. So I create a list of gets. Then I send them to table.get(List\<Get\>). If the regions for that table are spread out then those requests get chunked out to all the region servers. No one regionserver gets too many. However if one region server contains lots of regions for that table then a multi action can contain lots of gets. No single get is too onerous. However the regionserver won't return until every get is complete. So if there are thousands of gets that are sent in one multi then the regionserver can retain lots of data in one thread.


---

* [HBASE-14906](https://issues.apache.org/jira/browse/HBASE-14906) | *Major* | **Improvements on FlushLargeStoresPolicy**

In HBASE-14906 we use "hbase.hregion.memstore.flush.size/column\_family\_number" as the default threshold for memstore flush instead of the fixed value through "hbase.hregion.percolumnfamilyflush.size.lower.bound" property, which makes  the default threshold more flexible to various use case. We also introduce a new property in name of "hbase.hregion.percolumnfamilyflush.size.lower.bound.min" with 16M as the default value to avoid small flush in cases like hundreds of column families.

After this change setting "hbase.hregion.percolumnfamilyflush.size.lower.bound" in hbase-site.xml won't take effect anymore, but expert users could still set this property in table descriptor to override the default value just as before


---

* [HBASE-14769](https://issues.apache.org/jira/browse/HBASE-14769) | *Major* | **Remove unused functions and duplicate javadocs from HBaseAdmin**

- Removes functions from HBaseAdmin which require table name parameter as either byte[] or String. Use their counterparts which take TableName instead.
- Removes redundant javadocs from HBaseAdmin as they will be automatically inherited from Admin interface.
- HBaseAdmin is marked Audience.private so it should have been straight forward okay to remove the functions. But HBaseTestingUtility, which is marked Audience.public had a public function returning its instance, which moved this decision into gray area. Discussing in the community, it was decided that it would be okay to do so in this particular case.


---

* [HBASE-13153](https://issues.apache.org/jira/browse/HBASE-13153) | *Major* | **Bulk Loaded HFile Replication**

This enhances the HBase replication to support replication of bulk loaded data. This is configurable, by default it is set to false which means it will not replicate the bulk loaded data to its peer(s). To enable it set "hbase.replication.bulkload.enabled" to true.

Following are the additional configurations added for this enhancement,
 a. hbase.replication.cluster.id - This is manadatory to configure in cluster where replication for bulk loaded data is enabled. A source cluster is uniquely identified by sink cluster using this id. This should be configured in the source cluster configuration file for all the RS.
 b. hbase.replication.conf.dir - This represents the directory where all the active cluster's file system client configurations are defined in subfolders corresponding to their respective replication cluster id in peer cluster. This should be configured in the peer cluster configuration file for all the RS. Default is HBASE\_CONF\_DIR.
 c. hbase.replication.source.fs.conf.provider - This represents the class which provides the source cluster file system client configuration to peer cluster. This should be configured in the peer cluster configuration file for all the RS. Default is org.apache.hadoop.hbase.replication.regionserver.DefaultSourceFSConfigurationProvider

 For example: If source cluster FS client configurations are copied in peer cluster under directory /home/user/dc1/ then  hbase.replication.cluster.id should be configured as dc1 and hbase.replication.conf.dir as /home/user

Note:
 a. Any modification to source cluster FS client configuration files in peer cluster side replication configuration directory then it needs to restart all its peer(s) cluster RS with default hbase.replication.source.fs.conf.provider.
 b. Only 'xml' type files will be loaded by the default hbase.replication.source.fs.conf.provider.

As part of this we have made following changes to LoadIncrementalHFiles class which is marked as Public and Stable class,
 a. Raised the visibility scope of LoadQueueItem class from package private to public.
 b. Added a new method loadHFileQueue, which loads the queue of LoadQueueItem into the table as per the region keys provided.


---

* [HBASE-7171](https://issues.apache.org/jira/browse/HBASE-7171) | *Major* | **Initial web UI for region/memstore/storefiles details**

HBASE-7171 adds 2 new pages to the region server Web UI to ease debugging and provide greater insight into the physical data layout.

Region names in UI table listing all regions (on the RS status page) are now hyperlinks leading to region detail page which shows some aggregate memstore information (currently just memory used) along with the list of all Store Files (HFiles) in the region. Names of Store Files are also hyperlinks leading to Store File detail page, which currently runs 'hbase hfile' command behind the scene and displays statistics about store file.


---

* [HBASE-14655](https://issues.apache.org/jira/browse/HBASE-14655) | *Blocker* | **Narrow the scope of doAs() calls to region observer notifications for compaction**

Region observer notifications w.r.t. compaction request are now audited with request user through proper scope of doAs() calls.


---

* [HBASE-14631](https://issues.apache.org/jira/browse/HBASE-14631) | *Blocker* | **Region merge request should be audited with request user through proper scope of doAs() calls to region observer notifications**

Region observer notifications w.r.t. merge request are now audited with request user through proper scope of doAs() calls.


---

* [HBASE-14605](https://issues.apache.org/jira/browse/HBASE-14605) | *Blocker* | **Split fails due to 'No valid credentials' error when SecureBulkLoadEndpoint#start tries to access hdfs**

When split is requested by non-super user, split related notifications for Coprocessor are executed using the login of the request user.
Previously the notifications were carried out as super user.


---

* [HBASE-14926](https://issues.apache.org/jira/browse/HBASE-14926) | *Major* | **Hung ThriftServer; no timeout on read from client; if client crashes, worker thread gets stuck reading**

Adds a timeout to server read from clients. Adds new configs hbase.thrift.server.socket.read.timeout for setting read timeout on server socket in milliseconds. Default is 60000;


---

* [HBASE-14825](https://issues.apache.org/jira/browse/HBASE-14825) | *Minor* | **HBase Ref Guide corrections of typos/misspellings**

Corrections to content of "book.html", which is pulled from various \*.adoc files and \*.xml files.
-- corrects typos/misspellings
-- corrects incorrectly formatted links


---

* [HBASE-14821](https://issues.apache.org/jira/browse/HBASE-14821) | *Major* | **CopyTable should allow overriding more config properties for peer cluster**

Configuration properties for org.apache.hadoop.hbase.mapreduce.TableOutputFormat can now be overridden by prefixing the property keys with "hbase.mapred.output.".  When the configuration is applied to TableOutputFormat, these entries will be rewritten with the prefix removed -- ie. "hbase.mapred.output.hbase.security.authentication" becomes "hbase.security.authentication".  This can be useful when directing output to a peer cluster with different security configuration, for example.


---

* [HBASE-14799](https://issues.apache.org/jira/browse/HBASE-14799) | *Critical* | **Commons-collections object deserialization remote command execution vulnerability**

This issue resolves a potential security vulnerability. For all versions we update our commons-collections dependency to the release that fixes the reported vulnerability in that library. In 0.98 we additionally disable by default a feature of code carried from 0.94 for backwards compatibility that is not needed.


---

* [HBASE-12751](https://issues.apache.org/jira/browse/HBASE-12751) | *Major* | **Allow RowLock to be reader writer**

Locks on row are now reader/writer rather than exclusive.

Moves sequenceid out of HRegion and into MVCC class; MVCC is now in charge. A WAL append is still stamped in same way (we pass MVCC context in a few places where we previously we did not).

MVCC methods cleaned up. Make a bit more sense now. Less of them.

Simplifies our update of MemStore/WAL. Now we update memstore AFTER we add to WAL (but before we sync). This fixes possible dataloss when two edits came in with same coordinates; we could order the edits in memstore differently to how they arrived in the WAL.

Marked as an incompatible change because it breaks Distributed Log Replay, a feature we'd determined already was unreliable and to be removed.


---

* [HBASE-14793](https://issues.apache.org/jira/browse/HBASE-14793) | *Major* | **Allow limiting size of block into L1 block cache.**

Very large blocks can fragment the heap and cause bad issues for the garbage collector, especially the G1GC. Now there is a maximum size that a block can be and still stick in the LruBlockCache. That size defaults to 16mb but can be controlled by changing "hbase.lru.max.block.size"


---

* [HBASE-14387](https://issues.apache.org/jira/browse/HBASE-14387) | *Major* | **Compaction improvements: Maximum off-peak compaction size**

New configuration option: hbase.hstore.compaction.max.size.offpeak - maximum selection size eligible for minor compaction during off peak hours.
hbase.hstore.compaction.max.size - this is default maximum if no off-peak hours are defined or if no maximum off-peak maximum size is defined.


---

* [HBASE-12822](https://issues.apache.org/jira/browse/HBASE-12822) | *Minor* | **Option for Unloading regions through region\_mover.rb without Acknowledging**

Incorporated in HBASE-13014.


---

* [HBASE-14700](https://issues.apache.org/jira/browse/HBASE-14700) | *Major* | **Support a "permissive" mode for secure clusters to allow "simple" auth clients**

Secure HBase now supports a permissive mode to allow mixed secure and insecure clients.  This allows clients to be incrementally migrated over to a secure configuration.  To enable clients to continue to connect using SIMPLE authentication when the cluster is configured for security, set "hbase.ipc.server.fallback-to-simple-auth-allowed" equal to "true" in hbase-site.xml.  NOTE: This setting should ONLY be used as a temporary measure while converting clients over to secure authentication.  It MUST BE DISABLED for secure operation.


---

* [HBASE-14257](https://issues.apache.org/jira/browse/HBASE-14257) | *Major* | **Periodic flusher only handles hbase:meta, not other system tables**

Memstore periodic flusher used to flush META table every 5 minutes but not any other system tables. This jira extends it to flush all system tables within this time period.


---

* [HBASE-14658](https://issues.apache.org/jira/browse/HBASE-14658) | *Major* | **Allow loading a MonkeyFactory by class name**

You can specify one of the predefined set of Monkeys when you run Integration Tests by passing the -m\|--monkey arguments on the command line; e.g -m CALM or -m SLOW\_DETERMINISTIC

This patch  makes it so you can pass the name of a class as the monkey to run: e.g. -m org.example.KingKong


---

* [HBASE-14521](https://issues.apache.org/jira/browse/HBASE-14521) | *Major* | **Unify the semantic of hbase.client.retries.number**

After this change, hbase.client.reties.number universally means the number of retry which is one less than total tries number,  for both non-batch operations like get/scan/increment etc. which uses RpcRetryingCallerImpl#callWithRetries to submit the call or batch operations like put through AsyncProcess#submit.

Note that previously this property means total tries number for puts, so please adjust the setting of its value if necessary. Please also be cautious when setting it to zero since retry is necessary for client cache update when region move happens.


---

* [HBASE-13819](https://issues.apache.org/jira/browse/HBASE-13819) | *Major* | **Make RPC layer CellBlock buffer a DirectByteBuffer**

For master branch(2.0 version), the BoundedByteBufferPool always create Direct (off heap) ByteBuffers and return that.
For branch-1(1.3 version), byte default the buffers returned will be off heap. This can be changed to return on heap ByteBuffers by configuring 'hbase.ipc.server.reservoir.direct.buffer' to false.


---

* [HBASE-14517](https://issues.apache.org/jira/browse/HBASE-14517) | *Minor* | **Show regionserver's version in master status page**

Adds server version to the listing of regionservers on the master home page.

if a cluster where the versions deviate, at the bottom of the 'Version' column on the master home page listing of 'Region Servers', you will see a note in red that says something like: 'Total:10		9 nodes with inconsistent version'


---

* [HBASE-12911](https://issues.apache.org/jira/browse/HBASE-12911) | *Major* | **Client-side metrics**

Introduces collection and reporting of various client-perceived metrics. Metrics are exposed via JMX under "org.apache.hadoop.hbase.client.MetricsConnection". Metrics are scoped according to connection instance, so multiple connection objects (ie, to different clusters) will report their metrics separately. Metrics are disabled by default, must be enabled by configuring "hbase.client.metrics.enable=true".


---

* [HBASE-14529](https://issues.apache.org/jira/browse/HBASE-14529) | *Major* | **Respond to SIGHUP to reload config**

HBase daemons can now be signaled to reload their config by sending SIGHUP to the java process. Not all config parameters can be reloaded.

In order for this new feature to work the hbase-daemon.sh script was changed to use disown rather than nohup. Functionally this shouldn't change anything but the processes will have a different parent when being run from a connected login shell.


---

* [HBASE-14502](https://issues.apache.org/jira/browse/HBASE-14502) | *Major* | **Purge use of jmock and remove as dependency**

HBASE-14502 Purge use of jmock and remove as dependency


---

* [HBASE-14544](https://issues.apache.org/jira/browse/HBASE-14544) | *Major* | **Allow HConnectionImpl to not refresh the dns on errors**

By setting hbase.resolve.hostnames.on.failure to false you can reduce the number of dns name resolutions that a client will do. However if machines leave and come back with different ip's the changes will not be noticed by the clients. So only set hbase.resolve.hostnames.on.failure to false if your cluster dns is not changing while clients are connected.


---

* [HBASE-14367](https://issues.apache.org/jira/browse/HBASE-14367) | *Major* | **Add normalization support to shell**

This patch adds shell support for region normalizer (see HBASE-13103).

3 commands have been added to hbase shell 'tools' command group (modeled on how the balancer works):

 - 'normalizer\_enabled' checks whether region normalizer is turned on
 - 'normalizer\_switch' allows user to turn normalizer on and off
 - 'normalize' runs region normalizer if it's turned on.

Also 'alter' command has been extended to allow user to enable/disable region normalization per table (disabled by default). Use it as

alter 'testtable', {NORMALIZATION\_MODE =\> 'true'}

Here is the help for the normalize command:

{code}
hbase(main):008:0\> help 'normalize'
Trigger region normalizer for all tables which have NORMALIZATION\_MODE flag set. Returns true
 if normalizer ran successfully, false otherwise. Note that this command has no effect
 if region normalizer is disabled (make sure it's turned on using 'normalizer\_switch' command).

 Examples:

   hbase\> normalize
{code}


---

* [HBASE-14475](https://issues.apache.org/jira/browse/HBASE-14475) | *Major* | **Region split requests are always audited with "hbase" user rather than request user**

Region observer notifications w.r.t. split request are now audited with request user through proper scope of doAs() calls.


---

* [HBASE-14230](https://issues.apache.org/jira/browse/HBASE-14230) | *Minor* | **replace reflection in FSHlog with HdfsDataOutputStream#getCurrentBlockReplication()**

Remove calling getNumCurrentReplicas on HdfsDataOutputStream via reflection. getNumCurrentReplicas showed up in hadoop 1+ and hadoop 0.2x. In hadoop-2 it was deprecated.


---

* [HBASE-14495](https://issues.apache.org/jira/browse/HBASE-14495) | *Major* | **TestHRegion#testFlushCacheWhileScanning goes zombie**

The WAL append was changed by HBASE-12751. Every append now sets a latch on an edit. The latch needs to be cleared or else the WAL will hang. The original failures in TestHRegion turned up 'holes' where we were failing to throw the latch if we skipped out early because we were interrupted. Other 'holes' were found where we had mocked up a WAL so the latch would just stay in place.  Futher holes were found appending WAL markers... here we were skipping the mvcc completely for a few edits.  A clean up of WALUtils made all markers take the same code paths.


---

* [HBASE-14280](https://issues.apache.org/jira/browse/HBASE-14280) | *Minor* | **Bulk Upload from HA cluster to remote HA hbase cluster fails**

Patch will effectively work with Hadoop version 2.6 or greater with a launch of "internal.nameservices".
There will be no change in versions older than 2.6.


---

* [HBASE-14334](https://issues.apache.org/jira/browse/HBASE-14334) | *Major* | **Move Memcached block cache in to it's own optional module.**

Move external block cache to it's own module. This  will reduce dependencies for people who use hbase-server.
Currently Memcached is the reference implementation for external block cache. External block caches allow HBase to take advantage of other more complex caches that can live longer than the HBase regionserver process and are not necessarily tied to a single computer
    life time. However external block caches add in extra operational overhead.


---

* [HBASE-14433](https://issues.apache.org/jira/browse/HBASE-14433) | *Major* | **Set down the client executor core thread count from 256 in tests**

Tests run with client executors that have core thread count of 4 and a keepalive of 3 seconds. They used to default to 256 core threads and 60 seconds  for keepalive.


---

* [HBASE-14400](https://issues.apache.org/jira/browse/HBASE-14400) | *Critical* | **Fix HBase RPC protection documentation**

To use rpc protection in HBase, set the value of 'hbase.rpc.protection' to:
'authentication' : simple authentication using kerberos
'integrity' : authentication and integrity
'privacy' : authentication and confidentiality

Earlier, HBase reference guide erroneously mentioned in some places to set the value to 'auth-conf'. This patch fixes the guide and adds temporary support for erroneously recommended values.


---

* [HBASE-14306](https://issues.apache.org/jira/browse/HBASE-14306) | *Major* | **Refine RegionGroupingProvider: fix issues and make it more scalable**

In HBASE-14306 we've changed default strategy of RegionGroupingProvider from "identify" to "bounded", so it's required to explicitly set "hbase.wal.regiongrouping.strategy" to "identify" if user still wants to use one WAL per region

Please also notice that in the new framework there will be one WAL per group, and the region-group mapping is decided by RegionGroupingStrategy. Accordingly, we've removed BoundedRegionGroupingProvider and added BoundedRegionGroupingStrategy as a replacement. If you already have a customized class for hbase.wal.regiongrouping.strategy, please check the new logic and make updates if necessary.


---

* [HBASE-6617](https://issues.apache.org/jira/browse/HBASE-6617) | *Major* | **ReplicationSourceManager should be able to track multiple WAL paths**

ReplicationSourceManager now could track multiple wal paths. Notice that although most changes are internal and all metrics names remain the same, signature of below methods in MetricsSource are changed:

1. refreshAgeOfLastShippedOp now requires a String parameter which indicates the wal group id of the reporter
2. setAgeOfLastShippedOp also adds a String parameter for wal group id


---

* [HBASE-14314](https://issues.apache.org/jira/browse/HBASE-14314) | *Major* | **Metrics for block cache should take region replicas into account**

The following metrics for primary region replica are added:

blockCacheHitCountPrimary
blockCacheMissCountPrimary
blockCacheEvictionCountPrimary


---

* [HBASE-14317](https://issues.apache.org/jira/browse/HBASE-14317) | *Blocker* | **Stuck FSHLog: bad disk (HDFS-8960) and can't roll WAL**

Tighten up WAL-use semantic.

1. If an append or a sync throws an exception, all subsequent attempts at using the log will also throw this same exception. The WAL is now a lame-duck until you roll it.
2. If a successful append, and then we fail to sync the append, this is a fatal exception. The container must abort to replay the WAL logs even though we have told the client that the appends failed.

The above rules have been applied laxly up to this; it used to be possible to get a good sync to go in over the top of a failed append. This has been fixed in this patch.

Also fixed a hang in the WAL subsystem if a request to pause the write pipeline took on a failed sync. before the roll requests sync got scheduled.


TODO: Revisit our WAL system. HBASE-12751 helps rationalize our write pipeline. In particular, it manages sequenceid inside mvcc which should make it so we can purge mechanism that writes empty, unflushed appends just to get the next sequenceid... problematic when WAL goes lame-duck. Lets get it in.
TODO: A successful append followed by a failed sync probably only needs us replace the WAL (if we have signalled the client that the appends failed). Bummer is that replicating, these last appends might make it to the sink cluster or get replayed during recovery. HBase should keep its own WAL length? Or sequenceid of last successful sync should be passed when doing recovery and replication?


---

* [HBASE-14261](https://issues.apache.org/jira/browse/HBASE-14261) | *Major* | **Enhance Chaos Monkey framework by adding zookeeper and datanode fault injections.**

This change augments existing chaos monkey framework with actions for restarting underlying zookeeper quorum and hdfs nodes of distributed hbase cluster. One assumption made while creating zk actions are that zookeper ensemble is an independent external service and won't be managed by hbase cluster.  For these actions to work as expected, the following parameters need to be configured appropriately.

{code}
\<property\>
  \<name\>hbase.it.clustermanager.hadoop.home\</name\>
  \<value\>$HADOOP\_HOME\</value\>
\</property\>
\<property\>
  \<name\>hbase.it.clustermanager.zookeeper.home\</name\>
  \<value\>$ZOOKEEPER\_HOME\</value\>
\</property\>
\<property\>
  \<name\>hbase.it.clustermanager.hbase.user\</name\>
  \<value\>hbase\</value\>
\</property\>
\<property\>
  \<name\>hbase.it.clustermanager.hadoop.hdfs.user\</name\>
  \<value\>hdfs\</value\>
\</property\>
\<property\>
  \<name\>hbase.it.clustermanager.zookeeper.user\</name\>
  \<value\>zookeeper\</value\>
\</property\>
{code}

The service user related configurations are newly introduced since in prod/test environments each service is managed by different user. Once the above parameters are configured properly, you can start using them as needed. An example usage for invoking these new actions is:

{{./hbase org.apache.hadoop.hbase.IntegrationTestAcidGuarantees -m serverAndDependenciesKilling}}


---

* [HBASE-14309](https://issues.apache.org/jira/browse/HBASE-14309) | *Major* | **Allow load balancer to operate when there is region in transition by adding force flag**

This issue adds boolean parameter, force, to 'balancer' command so that admin can force region balancing even when there is region (other than hbase:meta) in transition - assuming RIT being transient.
If hbase:meta is in transition, balancer command returns false.

WARNING: For experts only. Forcing a balance may do more damage than repair when assignment is confused
Note: enclose the force parameter in double quotes


---

* [HBASE-14313](https://issues.apache.org/jira/browse/HBASE-14313) | *Critical* | **After a Connection sees ConnectionClosingException it never recovers**

HConnection could get stuck when talking to a host that went down and then returned. This has been fixed by closing the connection in all paths.


---

* [HBASE-13339](https://issues.apache.org/jira/browse/HBASE-13339) | *Blocker* | **Update default Hadoop version to latest for master**

Master/2.0.0 now builds on the latest stable hadoop by default.


---

* [HBASE-14224](https://issues.apache.org/jira/browse/HBASE-14224) | *Critical* | **Fix coprocessor handling of duplicate classes**

Prevent Coprocessors being doubly-loaded; a particular coprocessor can only be loaded once.


---

* [HBASE-13127](https://issues.apache.org/jira/browse/HBASE-13127) | *Major* | **Add timeouts on all tests so less zombie sightings**

Use junit facility to impose timeout on test. Use test category to chose which timeout to apply: small tests timeout after 30 seconds, medium tests after 180 seconds, and large tests after ten minutes.

Updated junit version from 4.11 to 4.12. 4.12 has support for feature used here.

Add this at the head of your junit4 class to add a category-based timeout:

{code}
@Rule public final TestRule timeout =   CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
{code}

For example:


---

* [HBASE-14148](https://issues.apache.org/jira/browse/HBASE-14148) | *Major* | **Web UI Framable Page**

Security fix: Adds protection from clickjacking using X-Frame-Options header.
This will prevent use of HBase UI in frames. To disable this feature, set the configuration 'hbase.http.filter.xframeoptions.mode' to 'ALLOW' (default is 'DENY').


---

* [HBASE-10844](https://issues.apache.org/jira/browse/HBASE-10844) | *Major* | **Coprocessor failure during batchmutation leaves the memstore datastructs in an inconsistent state**

Promotes an -ea assert to logged FATAL and RS abort when memstore is found to be in an inconsistent state.


---

* [HBASE-13966](https://issues.apache.org/jira/browse/HBASE-13966) | *Minor* | **Limit column width in table.jsp**

Wraps region, start key, end key columns if too long.


---

* [HBASE-13706](https://issues.apache.org/jira/browse/HBASE-13706) | *Minor* | **CoprocessorClassLoader should not exempt Hive classes**

Starting from HBase 2.0, CoprocessorClassLoader will not exempt hadoop classes or zookeeper classes.  This means that if the custom coprocessor jar contains hadoop or zookeeper packages and classes, they will be loaded by the CoprocessorClassLoader.  Only hbase packages and classes  are exempted from the CoprocessorClassLoader. They (and their dependencies) are loaded by the parent server class loader.


---

* [HBASE-14054](https://issues.apache.org/jira/browse/HBASE-14054) | *Major* | **Acknowledged writes may get lost if regionserver clock is set backwards**

In {{checkAndPut}} write path use max(max timestamp for the row, System.currentTimeMillis()) in the, instead of blindly taking System.currentTimeMillis() to ensure that checkAndPut() cannot do writes which is already eclipsed. This is similar to what has been done in HBASE-12449 for increment and append.


---

* [HBASE-13985](https://issues.apache.org/jira/browse/HBASE-13985) | *Minor* | **Add configuration to skip validating HFile format when bulk loading**

A new config, hbase.loadincremental.validate.hfile , is introduced - default to true
When set to false, checking hfile format is skipped during bulkloading.


---

* [HBASE-14201](https://issues.apache.org/jira/browse/HBASE-14201) | *Major* | **hbck should not take a lock unless fixing errors**

HBCK no longer takes a lock until there are changes to the cluster being made.

The old behavior can be achieved by passing the -exclusive flag.


---

* [HBASE-14081](https://issues.apache.org/jira/browse/HBASE-14081) | *Minor* | **(outdated) references to SVN/trunk in documentation**

HBASE-14081 Remove (outdated) references to SVN/trunk from documentation


---

* [HBASE-13865](https://issues.apache.org/jira/browse/HBASE-13865) | *Trivial* | **Increase the default value for hbase.hregion.memstore.block.multipler from 2 to 4 (part 2)**

Increase default hbase.hregion.memstore.block.multiplier from 2 to 4 in the code to match the default value in the config files.


---

* [HBASE-12295](https://issues.apache.org/jira/browse/HBASE-12295) | *Major* | **Prevent block eviction under us if reads are in progress from the BBs**

We try to delay the eviction of the block till the cellblocks are formed at the Rpc layer. A simple reference counting mechanism is introduced when ever a block is accessed from the Bucket cache.  Once a scanner completes using a block the reference count is decremented.  The eviction of the block happens only when the reference count of that block is 0.
We also introduce a concept of ShareableMemory based on the type of blocks we create from the Block cache. The blocks from the ByteBufferIOEngine directly refer to the buckets in offheap and such blocks are marked SHARED memory type. The blocks from LRU, HDFS and file mode of Bucket cache are all marked EXCLUSIVE because these blocks have their own exclusive memory.
For the CP case, any cell coming out of SHARED memory block is copied before returning the results, because CPs can use the results as its state so that eviction cannot corrupt the results.


---

* [HBASE-11339](https://issues.apache.org/jira/browse/HBASE-11339) | *Major* | **HBase MOB**

The Moderate Object Storage (MOB) feature (HBASE-11339[1]) is modified I/O and compaction path that allows individual moderately sized values (100KB-10MB) to be stored in a way that write amplification is reduced when compared to the normal I/O path. MOB is defined in the column family and it is almost isolated with other components, the features and performance cannot be effected in normal columns.

For more details on how to use the feature please consult the HBase Reference Guide


---

* [HBASE-13954](https://issues.apache.org/jira/browse/HBASE-13954) | *Major* | **Remove HTableInterface#getRowOrBefore related server side code**

Removed Table#getRowOrBefore, Region#getClosestRowBefore, Store#getRowKeyAtOrBefore, RemoteHTable#getRowOrBefore apis and Thrift support for getRowOrBefore.
Also removed two coprocessor hooks preGetClosestRowBefore and postGetClosestRowBefore.
User using this api can instead use reverse scan something like below,
{code}
 Scan scan = new Scan(row);
  scan.setSmall(true);
  scan.setCaching(1);
  scan.setReversed(true);
  scan.addFamily(family);
{code}
pass this scan object to the scanner and retrieve the first Result from scanner output.


---

* [HBASE-12296](https://issues.apache.org/jira/browse/HBASE-12296) | *Major* | **Filters should work with ByteBufferedCell**

Change to support offheaping.

Incompatible change for filters ColumnPrefixFilter and MultipleColumnPrefixFilter

Changes parameters to filterColumn so takes a Cell rather than a byte [].

hbase-client-1.2.7-SNAPSHOT.jar, ColumnPrefixFilter.class
package org.apache.hadoop.hbase.filter
ColumnPrefixFilter.filterColumn ( byte[ ] buffer, int qualifierOffset, int qualifierLength )  :  Filter.ReturnCode
org/apache/hadoop/hbase/filter/ColumnPrefixFilter.filterColumn:([BII)Lorg/apache/hadoop/hbase/filter/Filter$ReturnCode;

Ditto for filterColumnValue in SingleColumnValueFilter. Takes a Cell instead of byte array.


---

* [HBASE-14045](https://issues.apache.org/jira/browse/HBASE-14045) | *Major* | **Bumping thrift version to 0.9.2.**

This changes upgrades thrift dependency of HBase to 0.9.2. Though this doesn't break any HBase compatibility promises, it might impact any downstream projects that share thrift dependency with HBase.


---

* [HBASE-14027](https://issues.apache.org/jira/browse/HBASE-14027) | *Major* | **Clean up netty dependencies**

HBase's convenience binary artifact no longer contains the netty 3.2.4 jar . This jar was not directly used by HBase, but may have been relied on by downstream applications.


---

* [HBASE-7782](https://issues.apache.org/jira/browse/HBASE-7782) | *Minor* | **HBaseTestingUtility.truncateTable() not acting like CLI**

HBaseTestingUtility now uses the truncate API added in HBASE-8332 so that calls to HBTU.truncateTable will behave like the shell command: effectively dropping the table and recreating a new one with the same split points.

Previously, HBTU.truncateTable instead issued deletes for all the data already in the table. If you wish to maintain the same behavior, you should use the newly added HBTU.deleteTableData method.


---

* [HBASE-14047](https://issues.apache.org/jira/browse/HBASE-14047) | *Major* | **Cleanup deprecated APIs from Cell class**

The following API from Cell (which were deprecated since past few major versions) are removed now.
getRow
getFamily
getQualifier
getValue
getMvccVersion
The above apis can be replaced with their respective CellUtil#cloneXXX (allocates a copy) or Cell#getXXXArray (essentially just returns a pointer) based on the use case.


---

* [HBASE-14029](https://issues.apache.org/jira/browse/HBASE-14029) | *Major* | **getting started for standalone still references hadoop-version-specific binary artifacts**

HBASE-14029 Correct documentation for Hadoop version specific artifacts


---

* [HBASE-13849](https://issues.apache.org/jira/browse/HBASE-13849) | *Major* | **Remove restore and clone snapshot from the WebUI**

The HBase master status web page no longer allows operators to clone snapshots nor restore snapshots.


---

* [HBASE-13646](https://issues.apache.org/jira/browse/HBASE-13646) | *Major* | **HRegion#execService should not try to build incomplete messages**

When RegionServerCoprocessors throw an exception we will no longer attempt to build an incomplete RPC response message. Instead, the response message will be null.


---

* [HBASE-13639](https://issues.apache.org/jira/browse/HBASE-13639) | *Major* | **SyncTable - rsync for HBase tables**

Tool to sync two tables that tries to send the differences only like rsync.

Adds two new MapReduce jobs, SyncTable and HashTable. See usage for these jobs on how to use. See design doc for generally overview: https://docs.google.com/document/d/1-2c9kJEWNrXf5V4q\_wBcoIXfdchN7Pxvxv1IO6PW0-U/edit

From comments below, "It can be challenging to run against a table getting live writes, if those writes are updates/overwrites. In general, you can run it against a time range to ignore new writes, but if those writes update existing cells, then the time range scan may or may not see older versions of those cells depending on whether major compaction has happened, which may be different in remote clusters."


---

* [HBASE-13895](https://issues.apache.org/jira/browse/HBASE-13895) | *Critical* | **DATALOSS: Region assigned before WAL replay when abort**

If the master went to assign a region concurrent with a RegionServer abort, the returned RegionServerAbortedException was being handled as though the region had been cleanly offlined so assign was allowed proceed. If the region was opened in its new location before WAL replay completion, the replayed edits were ignored, worst case, or were later played over the top of edits that had come in since open and so susceptible to overwrite. In either case, DATALOSS.


---

* [HBASE-13983](https://issues.apache.org/jira/browse/HBASE-13983) | *Minor* | **Doc how the oddball HTable methods getStartKey, getEndKey, etc. will be removed in 2.0.0**

Adds extra doc on getStartKeys, getEndKeys, and getStartEndKeys in HTable explaining that they will be removed in 2.0.0 (these methods did not get the proper full major version deprecation cycle).

In this issue, we actually also remove these methods in master/2.0.0 branch.


---

* [HBASE-13747](https://issues.apache.org/jira/browse/HBASE-13747) | *Critical* | **Promote Java 8 to "yes" in support matrix**

Java 8 is considered supported and tested as of HBase 1.2+


---

* [HBASE-13959](https://issues.apache.org/jira/browse/HBASE-13959) | *Critical* | **Region splitting uses a single thread in most common cases**

The performance of region splitting has been improved by using a thread pool to split the store files concurrently. Prior to this change, the store files were always split sequentially in a single thread, so a region with multiple store files ended up taking several seconds. The thread pool is sized dynamically with the aim of getting maximum concurrency, without exceeding the number of cores available for HBase Java process. A lower limit for the thread pool can be explicitly set using the property hbase.regionserver.region.split.threads.max.


---

* [HBASE-13930](https://issues.apache.org/jira/browse/HBASE-13930) | *Major* | **Exclude Findbugs packages from shaded jars**

Exclude Findbugs packages from shaded jars


---

* [HBASE-13214](https://issues.apache.org/jira/browse/HBASE-13214) | *Major* | **Remove deprecated and unused methods from HTable class**

**WARNING: No release note provided for this change.**


---

* [HBASE-13869](https://issues.apache.org/jira/browse/HBASE-13869) | *Trivial* | **Fix typo in HBase book**

Fix typo in HBase book


---

* [HBASE-13938](https://issues.apache.org/jira/browse/HBASE-13938) | *Major* | **Deletes done during the region merge transaction may get eclipsed**

Use the master's timestamp when sending hbase:meta edits on region merge to ensure proper ordering of new region addition and old region deletes.


---

* [HBASE-13898](https://issues.apache.org/jira/browse/HBASE-13898) | *Minor* | **correct additional javadoc failures under java 8**

Correct Javadoc generation errors


---

* [HBASE-13103](https://issues.apache.org/jira/browse/HBASE-13103) | *Major* | **[ergonomics] add region size balancing as a feature of master**

This patch adds optional ability for HMaster to normalize regions in size (disabled by default, change hbase.normalizer.enabled property to true to turn it on). If enabled, HMaster periodically (every 30 minutes by default) monitors tables for which normalization is enabled in table configuration and performs splits/merges as seems appropriate. Users may implement their own normalization strategies by implementing RegionNormalizer interface and configuring it in hbase-site.xml.


---

* [HBASE-13900](https://issues.apache.org/jira/browse/HBASE-13900) | *Minor* | **duplicate methods between ProtobufMagic and ProtobufUtil**

Use ProtobufMagic methods in ProtobufUtil


---

* [HBASE-13843](https://issues.apache.org/jira/browse/HBASE-13843) | *Trivial* | **Fix internal constant text in ReplicationManager.java**

In previous versions of HBase, the ReplicationAdmin utility erroneously used the string key "columnFamlyName" when listing replicated column families. It now uses the corrected spelling of "columnFamilyName" (note the added "i").

Downstream code that parsed the replication entries returned from listReplicated will need to be updated to use the new key. Previously compiled code that relied on the static CFNAME member of ReplicationAdmin will need to be recompiled in order to see the updated value.


---

* [HBASE-13886](https://issues.apache.org/jira/browse/HBASE-13886) | *Major* | **Return empty value when the mob file is corrupt instead of throwing exceptions**

By default the Get/Scan will throw Exception when it is not able to find a mob cell because the mob file is missing/corrupted. This jira adds a facility to continue scan/get and get other cells with mob cell value as empty. Set an attribute MobConstants.EMPTY\_VALUE\_ON\_MOBCELL\_MISS = true in Scan/Get for getting this behaviour


---

* [HBASE-13686](https://issues.apache.org/jira/browse/HBASE-13686) | *Major* | **Fail to limit rate in RateLimiter**

As per this jira contribution. We now support two kinds of RateLimiter.
1) org.apache.hadoop.hbase.quotas.AverageIntervalRateLimiter : This limiter will refill resources at every TimeUnit/resources interval.
Example: For a limiter configured with 10resources/second, then 1resource will be refilled after every 100ms.

2) org.apache.hadoop.hbase.quotas.FixedIntervalRateLimiter: This limiter will refill resources only after a given fixed interval of time.

Client can configure anyone of this rate limiter for the cluster by setting the value for the property "hbase.quota.rate.limiter" in the hbase-site.xml. org.apache.hadoop.hbase.quotas.AverageIntervalRateLimiter is the default value.
Note: Client needs to restart the cluster for the configuration to take into effect.


---

* [HBASE-13816](https://issues.apache.org/jira/browse/HBASE-13816) | *Major* | **Build shaded modules only in release profile**

hbase-shaded-client and hbase-shaded-server modules will not build the actual jars unless -Prelease is supplied in mvn.


---

* [HBASE-13754](https://issues.apache.org/jira/browse/HBASE-13754) | *Major* | **Allow non KeyValue Cell types also to oswrite**

This jira has removed the already deprecated method
KeyValue#oswrite(final KeyValue kv, final OutputStream out)


---

* [HBASE-13375](https://issues.apache.org/jira/browse/HBASE-13375) | *Major* | **Provide HBase superuser higher priority over other users in the RPC handling**

This JIRA modifies the signature of PriorityFunction#getPriority() method to also take request user as a parameter; all RPC requests sent by super users (as determined by cluster configuration) are executed with Admin QoS.


---

* [HBASE-5980](https://issues.apache.org/jira/browse/HBASE-5980) | *Minor* | **Scanner responses from RS should include metrics on rows/KVs filtered**

Adds scan metrics to the result. In the shell, set the ALL\_METRICS attribute to true on your scan to see dump of metrics after results (see the scan help for examples).

If you would prefer to see only a subset of the metrics, the METRICS array can be defined to include the names of only the metrics you care about.


---

* [HBASE-13698](https://issues.apache.org/jira/browse/HBASE-13698) | *Major* | **Add RegionLocator methods to Thrift2 proxy.**

Added getRegionLocation and getAllRegionLocations to the thrift2 interface.


---

* [HBASE-13636](https://issues.apache.org/jira/browse/HBASE-13636) | *Major* | **Remove deprecation for HBASE-4072 (Reading of zoo.cfg)**

Purge support for parsing zookeepers zoo.cfg deprecated since hbase-0.96.0


---

* [HBASE-13071](https://issues.apache.org/jira/browse/HBASE-13071) | *Major* | **Hbase Streaming Scan Feature**

MOTIVATION

A pipelined scan API is introduced for speeding up applications that combine massive data traversal with compute-intensive processing. Traditional HBase scans save network trips through prefetching the data to the client side cache. However, they prefetch synchronously: the fetch request to regionserver is invoked only when the entire cache is consumed. This leads to a stop-and-wait access pattern, in which the client stalls until the next chunk of data is fetched. Applications that do significant processing can benefit from background data prefetching, which eliminates this bottleneck. The pipelined scan implementation overlaps the cache population at the client side with application processing. Namely, it issues a new scan RPC when the iteration retrieves 50% of the cache. If the application processing (that is, the time between invocations of next()) is substantial, the new chunk of data will be available before the previous one is exhausted, and the client will not experience any delay. Ideally, the prefetch and the processing times should be balanced.

API AND CONFIGURATION

Asynchronous scanning can be configured either globally for all tables and scans, or on per-scan basis via a new Scan class API.

Configuration in hbase-site.xml: hbase.client.scanner.async.prefetch, default false:

 \<property\>
   \<name\>hbase.client.scanner.async.prefetch\</name\>
   \<value\>true\</value\>
 \</property\>

API - Scan#setAsyncPrefetch(boolean)

      Scan scan = new Scan();
      scan.setCaching(1000);
      scan.setMaxResultSize(BIG\_SIZE);
      scan.setAsyncPrefetch(true);
        ...
      ResultScanner scanner = table.getScanner(scan);

IMPLEMENTATION NOTES

Pipelined scan is implemented by a new ClientAsyncPrefetchScanner class, which is fully API-compatible with the synchronous ClientSimpleScanner. ClientAsyncPrefetchScanner is not instantiated in case of small (Scan#setSmall) and reversed (Scan#setReversed) scanners. The application is responsible for setting the prefetch size in a way that the prefetch time and the processing times are balanced. Note that due to double buffering, the client side cache can use twice as much memory as the synchronous scanner.

Generally, this feature will put more load on the server (higher fetch rate -- which is the whole point).  Also, YMMV.


---

* [HBASE-13533](https://issues.apache.org/jira/browse/HBASE-13533) | *Trivial* | **section on configuring ~/.m2/settings.xml has no anchor**

Correct setting.xml anchor in book


---

* [HBASE-13625](https://issues.apache.org/jira/browse/HBASE-13625) | *Major* | **Use HDFS for HFileOutputFormat2 partitioner's path**

Introduces a new config hbase.fs.tmp.dir which is a directory in HDFS (or default file system) to use as a staging directory for HFileOutputFormat2. This is also used as the default for hbase.bulkload.staging.dir


---

* [HBASE-10800](https://issues.apache.org/jira/browse/HBASE-10800) | *Major* | **Use CellComparator instead of KVComparator**

From 2.0 branch onwards KVComparator and its subclasses MetaComparator, RawBytesComparator are all deprecated.
All the comparators are moved to CellComparator.  MetaCellComparator, a subclass of CellComparator, will be used to compare hbase:meta cells.
Previously exposed static instances KeyValue.COMPARATOR, KeyValue.META\_COMPARATOR and KeyValue.RAW\_COMPARATOR are deprecated instead use CellComparator.COMPARATOR and CellComparator.META\_COMPARATOR.
Also note that there will be no RawBytesComparator.  Where ever we need to compare raw bytes use Bytes.BYTES\_RAWCOMPARATOR.
CellComparator will always operate on cells and its components, abstracting the fact that a cell can be backed by a single byte[] as opposed to how KVComparators were working.


---

* [HBASE-13333](https://issues.apache.org/jira/browse/HBASE-13333) | *Major* | **Renew Scanner Lease without advancing the RegionScanner**

Adds a renewLease call to ClientScanner


---

* [HBASE-13564](https://issues.apache.org/jira/browse/HBASE-13564) | *Major* | **Master MBeans are not published**

To use the coprocessor-based JMX implementation provided by HBase for Master.
Add below property in hbase-site.xml file:

\<property\>
  \<name\>hbase.coprocessor.master.classes\</name\>
  \<value\>org.apache.hadoop.hbase.JMXListener\</value\>
\</property\>

NOTE: DO NOT set \`com.sun.management.jmxremote.port\` for Java VM at the same time.

By default, the JMX listens on TCP port 10101 for Master, we can further configure the port using below properties:

\<property\>
  \<name\>master.rmi.registry.port\</name\>
  \<value\>61110\</value\>
\</property\>
\<property\>
  \<name\>master.rmi.connector.port\</name\>
  \<value\>61120\</value\>
\</property\>
----

The registry port can be shared with connector port in most cases, so you only need to configure master.rmi.registry.port.
However if you want to use SSL communication, the 2 ports must be configured to different values.


---

* [HBASE-13537](https://issues.apache.org/jira/browse/HBASE-13537) | *Major* | **Procedure V2 - Change the admin interface for async operations to return Future (incompatible with branch-1.x)**

As we made changes to return types in asynchronous methods of Admin API, this change is going to break binary compatibility. The source compatibility is kept intact though. The applications running against this change needs to be recompiled to keep things working.


---

* [HBASE-13517](https://issues.apache.org/jira/browse/HBASE-13517) | *Major* | **Publish a client artifact with shaded dependencies**

HBase now provides added convenience artifacts that shade most dependencies. These jars hbase-shaded-client and hbase-shaded-server are meant to be used when dependency conflicts can not be solved any other way. The normal jars hbase-client and hbase-server should still be preferred when possible.

Do not use hbase-shaded-server or hbase-shaded-client inside of a co-processor as bad things will happen.


---

* [HBASE-13149](https://issues.apache.org/jira/browse/HBASE-13149) | *Blocker* | **HBase MR is broken on Hadoop 2.5+ Yarn**

In HBase 1.1.0 and above we have upgraded the version of Jackson dependencies (jackson-core-asl, jackson-mapper-asl, jackson-jaxrs and jackson-xc) from 1.8.8 to 1.9.13. This is to follow the upgrade to Jackson 1.9.13 in Hadoop 2.5 and above which causes Jackson class incompatibility for HBase as reported in HBASE-13149.  Refer to HADOOP-10104 and YARN-2092 for additional information. Jackson1.9.13 is not completely backward compatible with the prior version 1.8.8 used in HBase. See the Compatibility reports attached in HBASE-13149 and http://svn.codehaus.org/jackson/trunk/release-notes/VERSION for more information.

This upgrade does not have direct impact on HBase users and HBase applications in most cases. In the rare case where your HBase application uses Jackson directly AND your application has compatibility issue with Jackson 1.9.13, you can do the following to mitigate the problem.

1. If you are on Hadoop 2.5 or above, and your HBase application involves running Yarn jobs, we recommend you update your application to use Jackson 1.9.13. You may be able to explore classpath isolation options (e.g. HADOOP-10893) or have your own classpath isolation strategy that works for you, but the general recommendation is that you upgrade to Jackson 1.9.13.
2. You may choose to continue using Jackson 1.8.8 and not to use Jackson 1.9.13 in your classpath.  You can also choose to replace the Jackson 1.9.13 jars in $HBASE\_HOME/lib with 1.8.8 jars.  It can work for you in the following cases:
a) You are on a Hadoop version earlier than Hadoop 2.5,  or
b) You are on Hadoop 2.5 or above, but your HBase application does not involve running Yarn jobs.
3. You may experiment with further isolation using the shaded jars introduced with 1.1.0 via HBASE-13517.

Note that it may not be tested or guaranteed that using Jackson 1.8.8 in $HBASE\_HOME/lib will work in future HBase releases.
It is recommended that your HBase application matches the Jackson version provided in HBase.

In HBase 0.98.x and HBase 1.0.x, we have NOT upgraded the version of Jackson dependencies. If you are on Hadoop 2.5 or above, and your HBase application involves running Yarn jobs, you may encounter Jackson class incomparability issue, as reported in HBASE-13149.

You can do the following to mitigate the problem:
1. Use 'hadoop jar' command to run your HBase jobs.
2. Explore classpath isolation options (e.g. HADOOP-10893) or have your own classpath isolation strategy that works for you.
3. You can also choose to replace the Jackson 1.8.8 jars in $HBASE\_HOME/lib with 1.9.13 jars from your Hadoop lib directory. We have tested HBase 0.98 with Jackson 1.9.13.


---

* [HBASE-13481](https://issues.apache.org/jira/browse/HBASE-13481) | *Major* | **Master should respect master (old) DNS/bind related configurations**

Master now honors configuration options as was before 1.0.0 releases:
hbase.master.ipc.address
hbase.master.dns.interface
hbase.master.dns.nameserver
hbase.master.info.bindAddress
This jira also adds hbase.master.hostname parameter as an extension to HBASE-12954.


---

* [HBASE-13090](https://issues.apache.org/jira/browse/HBASE-13090) | *Major* | **Progress heartbeats for long running scanners**

Previously, there was no way to enforce a time limit on scan RPC requests. The server would receive a scan RPC request and take as much time as it needed to accumulate enough results to reach a limit or exhaust the region. The problem with this approach was that, in the case of a very selective scan, the processing of the scan could take too long and cause timeouts client side.

With this fix, the server will now enforce a time limit on the execution of scan RPC requests. When a scan RPC request arrives to the server, a time limit is calculated to be half of whichever timeout value is more restictive between the configurations ("hbase.client.scanner.timeout.period" and "hbase.rpc.timeout"). When the time limit is reached, the server will return whatever results it has accumulated up to that point. The results may be empty.

To ensure that timeout checks do not occur too often (which would hurt the performance of scans), the configuration "hbase.cells.scanned.per.heartbeat.check" has been introduced. This configuration controls how often System.currentTimeMillis() is called to update the progress towards the time limit. Currently, the default value of this configuration value is 10000. Specifying a smaller value will provide a tighter bound on the time limit, but may hurt scan performance due to the higher frequency of calls to System.currentTimeMillis().

Protobuf models for ScanRequest and ScanResponse have been updated so that heartbeat support can be communicated. Support for heartbeat messages is specified in the request sent to the server via ScanRequest.Builder#setClientHandlesHeartbeats. Only when the server sees that ScanRequest#getClientHandlesHeartbeats() is true will it send heartbeat messages back to the client. A response is marked as a heartbeat message via the boolean flag ScanResponse#getHeartbeatMessage


---

* [HBASE-13307](https://issues.apache.org/jira/browse/HBASE-13307) | *Major* | **Making methods under ScannerV2#next inlineable, faster**

Made methods smaller under Scanner#next so inlinable and compilable (was getting 'too big to compile' from hotspot). Use of unsafe to parse shorts rather than use BB#getShort... faster, etc.


---

* [HBASE-13453](https://issues.apache.org/jira/browse/HBASE-13453) | *Critical* | **Master should not bind to region server ports**

In 1.0.x, master by default binds to the region server ports (both rpc and info). This change brings back the usage of old master rpc and info ports in 1.1+ and master (2.0) branches. The motivation for this change is to ease the life of the user so that he does not need to do anything to bring up a RS on the same host and also to make the migration from 0.98 to 1.1  hassle free.  However, the users going from 1.0 to 1.1 would see the change in the master ports.


---

* [HBASE-13419](https://issues.apache.org/jira/browse/HBASE-13419) | *Major* | **Thrift gateway should propagate text from exception causes.**

Compose thrift exception text from the text of the entire cause chain of the underlying exception.


---

* [HBASE-13275](https://issues.apache.org/jira/browse/HBASE-13275) | *Major* | **Setting hbase.security.authorization to false does not disable authorization**

Prior to this change the configuration setting 'hbase.security.authorization' had no effect if security coprocessor were installed. The act of installing the security coprocessors was assumed to indicate active authorizaton was desired and required. Now it is possible to install the security coprocessors yet have them operate in a passive state with active authorization disabled by setting 'hbase.security.authorization' to false. This can be useful but is probably not what you want. For more information, consult the Security section of the HBase online manual.

'hbase.security.authorization' defaults to true for backwards comptatible behavior.


---

* [HBASE-13118](https://issues.apache.org/jira/browse/HBASE-13118) | *Major* | **[PE] Add being able to write many columns**

Adds a --columns option to PE so you can write more than one column (changes default qualifier from 'data' to '0').


---

* [HBASE-13270](https://issues.apache.org/jira/browse/HBASE-13270) | *Major* | **Setter for Result#getStats is #addResults; confusing!**

Deprecates Result#addResults in favor of Result#setStatistics


---

* [HBASE-13362](https://issues.apache.org/jira/browse/HBASE-13362) | *Major* | **Set max result size from client only (like scanner caching).**

This introduces a new config option: hbase.server.scanner.max.result.size
This setting enforces a maximum result size (in bytes), when reached the server will return the results is has so far.
This is a safety setting and should be kept large. The default is inifinite in 0.98 and 1.0.x and 100mb in 1.1 and later.

Use hbase.client.scanner.max.result.size instead to enforce practical chunk sizes of a few mb (defaults to 2mb)


---

* [HBASE-11544](https://issues.apache.org/jira/browse/HBASE-11544) | *Critical* | **[Ergonomics] hbase.client.scanner.caching is dogged and will try to return batch even if it means OOME**

Results returned from RPC calls may now be returned as partials

When is a Result marked as a partial?
When the server must stop the scan because the max size limit has been reached. Means that the LAST Result returned within the ScanResult's Result array may be marked as a partial if the scan's max size limit caused it to stop in the middle of a row.

Incompatible Change: The return type of InternalScanners#next and RegionScanners#nextRaw has been changed to NextState from boolean
The previous boolean return value can be accessed via NextState#hasMoreValues()
Provides more context as to what happened inside the scanner

Scan caching default has been changed to Integer.Max\_Value
This value works together with the new maxResultSize value from HBASE-12976 (defaults to 2MB)
Results returned from server on basis of size rather than number of rows
Provides better use of network since row size varies amongst tables

Protobuf models have changed for Result, ScanRequest, and ScanResponse to support new partial Results

Partial Results should be invisible to application layer unless Scan#setAllowPartials is set

Scan#setAllowPartials has been added to allow the application to request to see the partial Results returned by the server rather than have the ClientScanner form the complete Result prior to returning it to the application

To disable the use of partial Results on the server, set ScanRequest.Builder#setClientHandlesPartials() to be false in the ScanRequest issued to server

Partial Results should allow the server to return large rows in parts rather than accumulate all the cells for that particular row and run out of memory


---

* [HBASE-11864](https://issues.apache.org/jira/browse/HBASE-11864) | *Minor* | **Enhance HLogPrettyPrinter to print information from WAL Header**

Enhance WALPrettyPrinter to print information (writer classnames and cell codec classname) from WAL Header


---

* [HBASE-13289](https://issues.apache.org/jira/browse/HBASE-13289) | *Major* | **typo in splitSuccessCount  metric**

In hbase 1.0.0, 0.98.10, 0.98.10.1, 0.98.11, and 0.98.12 'splitSuccessCount' was misspelled as 'splitSuccessCounnt'


---

* [HBASE-12990](https://issues.apache.org/jira/browse/HBASE-12990) | *Major* | **MetaScanner should be replaced by MetaTableAccessor**

Removes MetaScanner. Use MetaTableAccessor instead.


---

* [HBASE-13373](https://issues.apache.org/jira/browse/HBASE-13373) | *Major* | **Squash HFileReaderV3 together with HFileReaderV2 and AbstractHFileReader; ditto for Scanners and BlockReader, etc.**

Marking as incompatible change. Requires hfiles be major version \>= 2 and \>= minor version 3.  Version 3 files are enabled by default in 1.0.  0.98 writes version 2 minor version 3.  You cannot go to 1.0 from anything before 0.98.


---

* [HBASE-13252](https://issues.apache.org/jira/browse/HBASE-13252) | *Major* | **Get rid of managed connections and connection caching**

For a long time, HBase supported 2 types of connections - managed, which were cached and closed automatically when not needed, and unmanaged, where user is responsible for closing the connections by calling #close() on them.

The concept of managed connections in HBase (deprecated before) has now been extinguished completely, and now all callers are responsible for managing the lifecycle of connections they acquire.


---

* [HBASE-12954](https://issues.apache.org/jira/browse/HBASE-12954) | *Minor* | **Ability impaired using HBase on multihomed hosts**

The following config is added by this JIRA:

hbase.regionserver.hostname

This config is for experts: don't set its value unless you really know what you are doing.
When set to a non-empty value, this represents the (external facing) hostname for the underlying server.
See https://issues.apache.org/jira/browse/HBASE-12954 for details.

Caution: please make sure rolling upgrade succeeds before turning on this feature.


---

* [HBASE-13187](https://issues.apache.org/jira/browse/HBASE-13187) | *Critical* | **Add ITBLL that exercises per CF flush**

Pass the -D flag generator.multiple.columnfamilies on the command-line if you want the generator to write three column families rather than the default one. When set, we will write the usual 'meta' column family and use it checking linked-list is wholesome but we will also write a 'tiny' column family and a 'big' column family to provoke uneven flushing; good for testing the flush-by-columnfamily feature.


---

* [HBASE-13361](https://issues.apache.org/jira/browse/HBASE-13361) | *Minor* | **Remove or undeprecate {get\|set}ScannerCaching in HTable**

Removed getScannerCaching and setScannerCaching from Table


---

* [HBASE-10728](https://issues.apache.org/jira/browse/HBASE-10728) | *Major* | **get\_counter value is never used.**

for 0.98 and 1.0 changes are compatible (due to mitigation by HBASE-13433):

\* The "get\_counter" command no longer requires a dummy 4th argument. Downstream users are encouraged to migrate code to not pass this argument because it will result in an error for HBase 1.1+.
\* The "incr" command now outputs the current value of the counter to stdout.
ex:
{code}
jruby-1.6.8 :005 \> incr 'counter\_example', 'r1', 'cf1:foo', 10
COUNTER VALUE = 1772
0 row(s) in 0.1180 seconds
{code}

for 1.1+ changes are incompatible:

\* The "get\_counter" command no longer accepts a dummy 4th argument. Downstream users will need to update their code to not pass this argument.
ex:
{code}
jruby-1.6.8 :006 \> get\_counter 'counter\_example', 'r1', 'cf1:foo'
COUNTER VALUE = 1772

{code}
\* The "incr" command now outputs the current value of the counter to stdout.
ex:
{code}
jruby-1.6.8 :005 \> incr 'counter\_example', 'r1', 'cf1:foo', 10
COUNTER VALUE = 1772
0 row(s) in 0.1180 seconds
{code}


---

* [HBASE-13170](https://issues.apache.org/jira/browse/HBASE-13170) | *Major* | **Allow block cache to be external**

HBase can use memcached as an external block cache. To use this change your config to set hbase.blockcache.use.external to true and hbase.cache.memcached.servers to contain the list of memcached servers to use.


---

* [HBASE-13316](https://issues.apache.org/jira/browse/HBASE-13316) | *Minor* | **Reduce the downtime on planned moves of regions**

When issuing an Admin.move command the RegionServer that receive the region will try and open the StoreFiles of that region to prime the block cache with index blocks.


---

* [HBASE-13298](https://issues.apache.org/jira/browse/HBASE-13298) | *Critical* | **Clarify if Table.{set\|get}WriteBufferSize() is deprecated or not**

Deprecate said methods. They were mistakenly included in Table Interface.


---

* [HBASE-13248](https://issues.apache.org/jira/browse/HBASE-13248) | *Major* | **Make HConnectionImplementation top-level class.**

**WARNING: No release note provided for this change.**


---

* [HBASE-13331](https://issues.apache.org/jira/browse/HBASE-13331) | *Blocker* | **Exceptions from DFS client can cause CatalogJanitor to delete referenced files**

Fixes an issue where files from a split region that were still referenced were erroneously deleted leading to data loss.


---

* [HBASE-13273](https://issues.apache.org/jira/browse/HBASE-13273) | *Major* | **Make Result.EMPTY\_RESULT read-only; currently it can be modified**

The Result.EMPTY\_RESULT object is now immutable. In previous releases, the object could be modified by a caller to no longer be empty. Code that relies on this behavior will now receive an UnsupportedOperationException.


---

* [HBASE-12867](https://issues.apache.org/jira/browse/HBASE-12867) | *Major* | **Shell does not support custom replication endpoint specification**

Adds support to add\_peer in hbase shell to add a custom replication endpoint from HBASE-12254.


---

* [HBASE-13198](https://issues.apache.org/jira/browse/HBASE-13198) | *Major* | **Remove HConnectionManager**

**WARNING: No release note provided for this change.**


---

* [HBASE-12586](https://issues.apache.org/jira/browse/HBASE-12586) | *Major* | **Task 6 & 7 from HBASE-9117,  delete all public HTable constructors and delete ConnectionManager#{delete,get}Connection**

HTable class has been marked as private API before, and now it's no longer directly instantiable from client code (all public constructors have been removed). All clients should use Connection#getTable() and Connection#getRegionLocator() when appropriate to obtain Table and RegionLocator implementations to work with.


---

* [HBASE-13171](https://issues.apache.org/jira/browse/HBASE-13171) | *Minor* | **Change AccessControlClient methods to accept connection object to reduce setup time.**

**WARNING: No release note provided for this change.**


---

* [HBASE-12706](https://issues.apache.org/jira/browse/HBASE-12706) | *Critical* | **Support multiple port numbers in ZK quorum string**

hbase.zookeeper.quorum configuration now allows servers together with client ports consistent with the way Zookeeper java client accepts the quorum string. In this case, using hbase.zookeeper.clientPort is not needed. eg.  hbase.zookeeper.quorum=myserver1:2181,myserver2:20000,myserver3:31111


---

* [HBASE-13142](https://issues.apache.org/jira/browse/HBASE-13142) | *Major* | **[PERF] Reuse the IPCUtil#buildCellBlock buffer**

Adds buffer reuse sending Cell results. It is on by default and should not need configuration. Improves GC profile and ups throughput. The benefit gets better the larger the row size returned.

The buffer reservoir is bounded at a maximum count after which we will start logging at WARN level that the reservoir is running at capacity (returned buffers will be discarded and not added back to the reservoir pool). Default maximum is twice the handler count: i.e. 2 \* hbase.regionserver.handler.count. This should be more than enough. Set the maximum with the new configuration: hbase.ipc.server.reservoir.max

The reservoir will not cache buffers in excess of hbase.ipc.server.reservoir.max.buffer.size  The default is 10MB. This means that if a row is very large, then we will allocate a buffer of the average size that is currently in the pool and we will then resize it till we can accommodate the return. These resizes are expensive. The resultant buffer will be used and then discarded.

To check how the reservoir is doing, enable trace level logging for a few seconds on a regionserver. You can do this from the regionserver UI. See 'Log Level'. Set org.apache.hadoop.hbase.io.BoundedByteBufferPool to TRACE. The BoundedByteBufferPool will spew report to the log. Disable the TRACE level and then check the log. You'll see allocation rate, size of pool, size of buffers in pool, etc.


---

* [HBASE-13012](https://issues.apache.org/jira/browse/HBASE-13012) | *Major* | **Add shell commands to trigger the mob file compactor**

This adds two new shell commands -- compact\_mob and major\_compact\_mob to the hbase shell.

Run compaction on a mob enabled column family or all mob enabled column families within a table
          Examples:
          Compact a column family within a table:
          hbase\> compact\_mob 't1', 'c1'
          Compact all mob enabled column families
          hbase\> compact\_mob 't1'

Run major compaction on a mob enabled column family or all mob enabled column families within a table
          Examples:
          Compact a column family within a table:
          hbase\> major\_compact\_mob 't1', 'c1'
          Compact all mob enabled column families within a table
          hbase\> major\_compact\_mob 't1'


---

* [HBASE-12869](https://issues.apache.org/jira/browse/HBASE-12869) | *Major* | **Add a REST API implementation of the ClusterManager interface**

Adds an implementation of ClusterManager to control REST API-managed HBase clusters.


---

* [HBASE-13047](https://issues.apache.org/jira/browse/HBASE-13047) | *Trivial* | **Add "HBase Configuration" link missing on the table details pages**

Add a '/conf' link to UI


---

* [HBASE-13044](https://issues.apache.org/jira/browse/HBASE-13044) | *Minor* | **Configuration option for disabling coprocessor loading**

This change adds two new configuration options:
- "hbase.coprocessor.enabled" controls globally if any coprocessors will be loaded. Set to "false" to disable. Defaults to "true" for compatibility with previous releases.
- "hbase.coprocessor.user.enabled" controls if any user (aka table) coprocessors will be loaded. Set to "false" to disable. Defaults to "true" for compatibility with previous releases.


---

* [HBASE-12961](https://issues.apache.org/jira/browse/HBASE-12961) | *Minor* | **Negative values in read and write region server metrics**

Change read and write request count in ServerLoad from int to long


---

* [HBASE-7332](https://issues.apache.org/jira/browse/HBASE-7332) | *Minor* | **[webui] HMaster webui should display the number of regions a table has.**

Adds counts for various regions states to the table listing on main page. See attached screenshot.


---

* [HBASE-8329](https://issues.apache.org/jira/browse/HBASE-8329) | *Major* | **Limit compaction speed**

Adds compaction throughput limit mechanism(the word "throttle" is already used when choosing compaction thread pool, so use a different word here to avoid ambiguity). Default is org.apache.hadoop.hbase.regionserver.compactions.PressureAwareCompactionThroughputController, will limit throughput as follow:
1. In off peak hours, use a fixed limitation "hbase.hstore.compaction.throughput.offpeak" (default is Long.MAX\_VALUE which means no limitation).
2. In normal hours, the limitation is tuned between "hbase.hstore.compaction.throughput.lower.bound"(default 10MB/sec) and "hbase.hstore.compaction.throughput.higher.bound"(default 20MB/sec), using the formula "lower + (higer - lower) \* param" where param is in range [0.0, 1.0] and calculate based on store files count on this regionserver.
3. If some stores have too many store files(storefilesCount \> blockingFileCount), then there is no limitation no matter peak or off peak.
You can set "hbase.regionserver.throughput.controller" to org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController to disable throughput controlling.
And we have implemented ConfigurationObserver which means you can change all configurations above and do not need to restart cluster.

The throttle is on by default in hbase-2.0.0. There is no limit in hbase-1.x.


---

* [HBASE-6778](https://issues.apache.org/jira/browse/HBASE-6778) | *Major* | **Deprecate Chore; its a thread per task when we should have one thread to do all tasks**

Corresponding usages for new ScheduledChore vs. Deprecated Chore:
Chore.interrupt() -\> ScheduledChore.cancel(mayInterruptWhileRunning = true)
Threads.setDaemonThreadRunning(Chore) -\> ChoreService.scheduleChore(ScheduledChore)
Chore.isAlive -\> ScheduledChore.isScheduled()
Chore.getSleeper().skipSleepCycle() -\> ScheduledChore.triggerNow()


---

* [HBASE-11574](https://issues.apache.org/jira/browse/HBASE-11574) | *Major* | **hbase:meta's regions can be replicated**

On the server side, set hbase.meta.replica.count to the number of replicas of meta that you want to have in the cluster (defaults to 1). hbase.regionserver. meta.storefile.refresh.period should be set to a non-zero number in milliseconds - something like 30000 (defaults to 0).
On the client/user side, set hbase.meta.replicas.use to true.


---

* [HBASE-12808](https://issues.apache.org/jira/browse/HBASE-12808) | *Major* | **Use Java API Compliance Checker for binary/source compatibility**

Adds a dev-support/check\_compatibility.sh script for comparing versions. Run the script to see usage.


---

* [HBASE-12684](https://issues.apache.org/jira/browse/HBASE-12684) | *Major* | **Add new AsyncRpcClient**

Retrofit a new, netty-based rpc transport on the client. This client is slightly slower if little contention given the extra tier or so that netty adds and that we block on a Future waiting on the call to finish.  This client opens the way for HBase having a native Async API.

This client is on by default in master branch (2.0 hbase). It is off in branch-1.0 (hbase-1.1.x).  To enable it, set "hbase.rpc.client.impl" to "org.apache.hadoop.hbase.ipc.AsyncRpcClient"


---

* [HBASE-8410](https://issues.apache.org/jira/browse/HBASE-8410) | *Major* | **Basic quota support for namespaces**

Namespace auditor provides basic quota support for namespaces in terms of number of tables and number of regions. In order to use namespace quotas, quota support must be enabled by setting
"hbase.quota.enabled" property to true in hbase-site.xml file.

The users can add quota information to namespace, while creating new namespaces or by altering existing ones.

Examples:
1. create\_namespace 'ns1', {'hbase.namespace.quota.maxregions'=\>'10'}
2. create\_namespace 'ns2', {'hbase.namespace.quota.maxtables'=\>'2','hbase.namespace.quota.maxregions'=\>'5'}
3. alter\_namespace 'ns3', {METHOD =\> 'set', 'hbase.namespace.quota.maxtables'=\>'5','hbase.namespace.quota.maxregions'=\>'25'}

The quotas can be modified/added to namespace at any point of time. To remove quotas, the following command can be used:

alter\_namespace 'ns3', {METHOD =\> 'unset', NAME =\> 'hbase.namespace.quota.maxtables'}
alter\_namespace 'ns3', {METHOD =\> 'unset', NAME =\> 'hbase.namespace.quota.maxregions'}


---

* [HBASE-12902](https://issues.apache.org/jira/browse/HBASE-12902) | *Major* | **Post-asciidoc conversion fix-ups**

Pushed to master. Shout if there are any issues.


---

* [HBASE-12848](https://issues.apache.org/jira/browse/HBASE-12848) | *Major* | **Utilize Flash storage for WAL**

For users on a version of Hadoop that supports tiered storage policies (i.e. Apache Hadoop 2.6.0+), HBase now allows users to opt-in to having the write ahead log placed on the SSD tier. Users on earlier versions of Hadoop will be unable to take advantage of this feature.

Use of tiered storage is controlled by a new RegionServer config, hbase.wal.storage.policy. It defaults to the value 'NONE', which will rely on HDFS defaults for a policy decision.

User can specify ONE\_SSD or ALL\_SSD as the value:
ONE\_SSD: place only one replica of WAL files in SSD and the remaining in default storage
ALL\_SSD: all replica for WAL files are placed on SSD

See [the HDFS docs on storage policy\|http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html]


---

* [HBASE-11144](https://issues.apache.org/jira/browse/HBASE-11144) | *Major* | **Filter to support scanning multiple row key ranges**

MultiRowRangeFilter is a filter to support scanning multiple row key ranges. If the number of the ranges is small, using multiple scans can also do the same thing and can work well. But when the number of ranges are quite big (e.g. millions), use the MultiRowRangeFilter will be nice. In this filter, the ranges will be sorted and merged, so users do not have to take care of ranges are not continuous. And if users are using something like rest, thrift or pig to access the data the filter might be the practical solution.


---

* [HBASE-12268](https://issues.apache.org/jira/browse/HBASE-12268) | *Major* | **Add support for Scan.setRowPrefixFilter to shell**

Added new option, ROWPREFIXFILTER, to the scan command in the HBase shell to easily scan for a specific row prefix.


---

* [HBASE-12775](https://issues.apache.org/jira/browse/HBASE-12775) | *Major* | **CompressionTest ate my HFile (sigh!)**

CompressionTest will now abort when the target path exists.


---

* [HBASE-12695](https://issues.apache.org/jira/browse/HBASE-12695) | *Critical* | **JDK 1.8 compilation broken**

Use the -Pjavac maven profile in order to compile HBase using the compiler provided by the JDK instead of the default error-prone compiler plugin. This is useful for now if you are building HBase with JDK 1.8 or a JDK that doesn't support error-prone.


---

* [HBASE-10201](https://issues.apache.org/jira/browse/HBASE-10201) | *Major* | **Port 'Make flush decisions per column family' to trunk**

Adds new flushing policy mechanism. Default, org.apache.hadoop.hbase.regionserver.FlushLargeStoresPolicy, will try to avoid flushing out the small column families in a region, those whose memstores are \< hbase.hregion.percolumnfamilyflush.size.lower.bound. To restore the old behavior of flushes writing out all column families, set hbase.regionserver.flush.policy to org.apache.hadoop.hbase.regionserver.FlushAllStoresPolicy either in hbase-default.xml or on a per-table basis by setting the policy to use with HTableDescriptor.getFlushPolicyClassName().


---

* [HBASE-12559](https://issues.apache.org/jira/browse/HBASE-12559) | *Major* | **Provide LoadBalancer with online configuration capability**

updateConfiguration(ServerName server) method of Admin now updates config for HMaster as well.
Specifically, config update would be taken by load balancer.


---

* [HBASE-10378](https://issues.apache.org/jira/browse/HBASE-10378) | *Major* | **Divide HLog interface into User and Implementor specific interfaces**

HBase internals for the write ahead log have been refactored. Advanced users of HBase should be aware of the following changes.

Public Audience
  - The Admin API for asking a region server to roll WAL files has changed from a synchronous command that returns a set of regions the WAL implementation would like flushed into an asynchronous command that returns nothing. Older clients relying on the former behavior will still be able to interact with newer servers, but the response body will always contain an empty list of regions to flush.
  - The shell command "hlog\_roll" has been deprecated. Operators should use the "wal\_roll" command instead. This command is subject to the changes described above for the Admin API to roll WAL files.
  - The command for analyzing write ahead logs has been renamed from 'hlog' to 'wal'. The old usage is deprecated and will be removed in a future version.
  - Some utility methods in the HBaseTesetingUtility related to testing write-ahead-logs were changed in incompatible ways. No functionality has been removed, but method names and arguments have changed. See the HBaseTestingUtility javadoc for details.
  - The WALPlayer utility has deprecated the configuration keys used for advanced customization. Users should switch to the updated configuration keys. See the usage information on the WALPlayer tool for details.
  - The HLogInputFormat utility class for processing logs with MapReduce has been deprecated and will be removed in a future version. Users should switch to the WALInputFormat.
  - The labeling of server metrics on the region server status pages changed. Previously, the number of backing files for the write ahead log was labeled 'Num. HLog Files'. If you wish to see this statistic now, please look for the label 'Num. WAL Files.'  If you rely on JMX for these metrics, their location has not changed.

LimitedPrivate(COPROC) Audience, LimitedPrivate(PHOENIX)
  - The RegionObserver API has been updated. The changes are both binary and source backwards compatible for coprocessors that use the BaseRegionObserver class. For those that implement RegionObserver directly the changes are binary backwards compatible. Depending on the internals of future HBase versions, coprocessors using the deprecated API may not see all WAL related events. Users are strongly encouraged to update their use of the API; see the RegionObserver javadoc for details.
  - Classes related to reading WAL entries (ReaderBase, ProtobufLogReader, SequenceFileLogReader) have changed in a backwards incompatible way. Users who referenced HLog.Reader directly or HLog.Entry will have to update. These changes do not impact compatibility with extant wal files.
  - The WALObserver API has been updated. The changes are both binary and source backwards compatible for coprocessors that use the BaseWALObserver class. For those that implement WALObserver directly the changes are binary backwards compatible. Depending on the internals of future HBase versions, coprocessors using the deprecated API may not see all WAL related events. Users are strongly encouraged to update their use of the API; see the WALObserver javadoc for details.
 - The WALCoprocessorEnvironment  has changed in a backwards incompatible way. WALObserver coprocessors that relied on retrieving an object representing the write ahead log instance will have to be updated.

LimitedPrivate(REPLICATION) Audience
 - The WALEntryFilter API has changed in a backwards incompatible way. Implementers will have to be updated.
 - The ReplicationEndpoint.ReplicateContext API has changed in a backwards incompatible way. Implementers who use this interface will have to be updated. These changes do not impact wire compatibility for replicating between clusters.
 - The HLogKey API is deprecated in favor of the WALKey API. Additionally, the HLogKey API has changed in a backwards incompatible way by changing from implementing WriteableComparable\<HLogKey\> to implementing Writeable and Comparable\<WALKey\>.


---

* [HBASE-11683](https://issues.apache.org/jira/browse/HBASE-11683) | *Major* | **Metrics for MOB**

Adds new mob related metrics:

mobCompactedIntoMobCellsCount
mobCompactedIntoMobCellsSize
mobCompactedFromMobCellsCount
mobCompactedFromMobCellsSize
mobFlushCount
mobFlushedCellsCount
mobFlushedCellsSize
mobScanCellsCount
mobScanCellsSize
mobFileCacheAccessCount
mobFileCacheMissCount
mobFileCacheHitPercent
mobFileCacheEvictedCount
mobFileCacheCount


---

* [HBASE-11912](https://issues.apache.org/jira/browse/HBASE-11912) | *Major* | **Catch some bad practices at compile time with error-prone**

Errors from error-prone will fail the build in the compile phase. Warnings look like Javac warnings and are counted as such by test-patch etc


---

* [HBASE-12220](https://issues.apache.org/jira/browse/HBASE-12220) | *Major* | **Add hedgedReads and hedgedReadWins metrics**

Adds metrics hedgedReads and hedgedReadWins counts.


---

* [HBASE-6290](https://issues.apache.org/jira/browse/HBASE-6290) | *Minor* | **Add a function a mark a server as dead and start the recovery the process**

Adds a script to mark a server as dead.

Usage: considerAsDead.sh --hostname serverName


---

* [HBASE-12111](https://issues.apache.org/jira/browse/HBASE-12111) | *Major* | **Remove deprecated APIs from Mutation(s)**

Removed the below from hbase-2 (were deprecated on release of hbase-1.0.0)

Mutation setWriteToWAL(boolean)
boolean getWriteToWAL()
Mutation setFamilyMap(NavigableMap\<byte [], List\<KeyValue\>\>)
NavigableMap\<byte [], List\<KeyValue\>\> getFamilyMap()


---

* [HBASE-12084](https://issues.apache.org/jira/browse/HBASE-12084) | *Major* | **Remove deprecated APIs from Result**

The below KeyValue based APIs are removed from Result
KeyValue[] raw()
List\<KeyValue\> list()
List\<KeyValue\> getColumn(byte [] family, byte [] qualifier)
KeyValue getColumnLatest(byte [] family, byte [] qualifier)
KeyValue getColumnLatest(byte [] family, int foffset, int flength, byte [] qualifier, int qoffset, int qlength)

They are replaced with
Cell[] rawCells()
List\<Cell\> listCells()
List\<Cell\> getColumnCells(byte [] family, byte [] qualifier)
Cell getColumnLatestCell(byte [] family, byte [] qualifier)
Cell getColumnLatestCell(byte [] family, int foffset, int flength, byte [] qualifier, int qoffset, int qlength)
respectively

Also the constructors which were taking KeyValues also removed
Result(KeyValue [] cells)
Result(List\<KeyValue\> kvs)


---

* [HBASE-12048](https://issues.apache.org/jira/browse/HBASE-12048) | *Major* | **Remove deprecated APIs from Filter**

The following APIs are removed from Filter
KeyValue transform(KeyValue)
KeyValue getNextKeyHint(KeyValue)
and replaced with
Cell transformCell(Cell)
Cell getNextCellHint(Cell)
respectively.
If a custom Filter implementation have overridden any of these methods, we will no longer call them. User has to change the custom Filter to override cell based methods as shown above


---

* [HBASE-7767](https://issues.apache.org/jira/browse/HBASE-7767) | *Major* | **Get rid of ZKTable, and table enable/disable state in ZK**

Keeps table enabled/disabled state in HDFS rather than up in ZooKeeper.  Auto-migrates any existing zk state.


---

* [HBASE-11911](https://issues.apache.org/jira/browse/HBASE-11911) | *Major* | **Break up tests into more fine grained categories**

Adds new test categories besides the class smalltests, mediumtests, and largetests.  Adds:

ClientTests
CoprocessorTests
FilterTests
FlakeyTests
IOTests
MapReduceTests
MasterTests
MiscTests
RegionServerTests
ReplicationTests
RestTests
SecurityTests
VerySlowMapReduceTests
VerySlowRegionServerTests

See description for examples on how to use them.


---

* [HBASE-11658](https://issues.apache.org/jira/browse/HBASE-11658) | *Major* | **Piped commands to hbase shell should return non-zero if shell command failed.**

Adds a noninteractive mode (-n or --noninteractive) to the hbase shell that exits with a non-zero error code on failed or invalid shell command executions, and exits with a zero error code upon successful execution.


---

* [HBASE-11640](https://issues.apache.org/jira/browse/HBASE-11640) | *Major* | **Add syntax highlighting support to HBase Ref Guide programlistings**

This got committed, so I guess it is safe to resolve it?


---

* [HBASE-11606](https://issues.apache.org/jira/browse/HBASE-11606) | *Minor* | **Enable ZK-less region assignment by default**

By default, we don't use ZK for region assignment now. To fall back to the old way, you can set hbase.assignment.usezk to true.


---

* [HBASE-3135](https://issues.apache.org/jira/browse/HBASE-3135) | *Major* | **Make our MR jobs implement Tool and use ToolRunner so can do -D trickery, etc.**

All MR jobs implement Tool Interface, http://hadoop.apache.org/docs/current/api/org/apache/hadoop/util/Tool.html, so now you can pass properties on command line with the -D flag, etc.


---

* [HBASE-11556](https://issues.apache.org/jira/browse/HBASE-11556) | *Major* | **Move HTablePool to hbase-thrift module.**

HTablePool was deprecated in 0.98.1 but was still present and usable by apps built against versions before HBase 2.0.  It has been moved and is not intended to be used by user applications, and is now an internal part of the thrift2 proxy server only.


---

* [HBASE-11548](https://issues.apache.org/jira/browse/HBASE-11548) | *Trivial* | **[PE] Add 'cycling' test N times and unit tests for size/zipf/valueSize calculations**

Adds --cycles=N argument.


---

* [HBASE-11344](https://issues.apache.org/jira/browse/HBASE-11344) | *Major* | **Hide row keys and such from the web UIs**

Configure "hbase.display.keys" to false (default: true) in the master/regionservers if the row-keys should be hidden in the webUIs (like in the webUI for table details).


---

* [HBASE-6580](https://issues.apache.org/jira/browse/HBASE-6580) | *Major* | **Deprecate HTablePool in favor of HConnection.getTable(...)**

This issue introduces a few new APIs:
\* HConnectionManager:
{code}
    public static HConnection createConnection(Configuration conf)
    public static HConnection createConnection(Configuration conf, ExecutorService pool)
{code}
\* HConnection:
{code}
    public HTableInterface getTable(String tableName) throws IOException
    public HTableInterface getTable(byte[] tableName) throws IOException
    public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException
    public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException
{code}

By default HConnectionImplementation will create an ExecutorService when needed. The ExecutorService can optionally passed be passed in.
HTableInterfaces are retrieved from the HConnection. By default the HConnection's ExecutorService is used, but optionally that can be overridden for each HTable.


---

* [HBASE-8450](https://issues.apache.org/jira/browse/HBASE-8450) | *Critical* | **Update hbase-default.xml and general recommendations to better suit current hw, h2, experience, etc.**

Changed defaults:

+ max versions now 1 instead of 3
+ row blooms on by default (except on .META. table)
+ handlers 30 instead of 10
+ upped memstore lower limit from .35 to .38
+ zookeeper timeout default is 90seconds instead of 180
+ client pause is 100ms instead of 1000ms
+ retries are now 20 instead of 10 (so overall we still wait same amount of time)
+ bulkload retries is 10 instead of infinite
+ major compactions are now once a week instead of once every 24 hours; they are staggered so all regionservers do not start compacting at the same time
+ blockingstorefiles is 10 instead of 7
+ block cache is 0.4 instead of 0.25
+ Previous, default for hbase.rootdir was /tmp/hbase-${user.name}.  Now it is ${java.io.tmpdir}/hbase-${user.name} which is usually the same location but may not be (on macos, it points to /var/tmp....).


---

* [HBASE-4072](https://issues.apache.org/jira/browse/HBASE-4072) | *Major* | **Deprecate/disable and remove support for reading ZooKeeper zoo.cfg files from the classpath**

The Apache ZooKeeper config file zoo.cfg will no longer be read when instantiating a HBaseConfiguration object, as it causes various inconsistency issues. Instead, users have to specify all HBase-relevant ZooKeeper properties in the hbase-site.xml using the various "hbase.zookeeper" prefixed properties. For example, specify "hbase.zookeeper.quorum" to provide a ZK quorum server list.

To enable zoo.cfg reading, for which support may be removed in a future release, set the property "hbase.config.read.zookeeper.config" to true in the hbase-site.xml at the client and servers like so:

\<property\>
  \<name\>hbase.config.read.zookeeper.config\</name\>
  \<value\>true\</value\>
  \<description\>
        Set to true to allow HBaseConfiguration to read the
        zoo.cfg file for ZooKeeper properties. Switching this to true
        is not recommended, since the functionality of reading ZK
        properties from a zoo.cfg file has been deprecated.
  \</description\>
\</property\>
