/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.CompactionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * HConstants holds a bunch of HBase-related constants
 */
public final class HConstants {
  /**
   * Status codes used for return values of bulk operations.
   */
  public enum OperationStatusCode {
    NOT_RUN,
    SUCCESS,
    FAILURE;
  }

  /** long constant for zero */
  public static final Long ZERO_L = Long.valueOf(0L);
  public static final String NINES = "99999999999999";
  public static final String ZEROES = "00000000000000";

  // For migration

  /** name of version file */
  public static final String VERSION_FILE_NAME = "hbase.version";

  /**
   * Current version of file system.
   * Version 4 supports only one kind of bloom filter.
   * Version 5 changes versions in catalog table regions.
   * Version 6 enables blockcaching on catalog tables.
   * Version 7 introduces hfile -- hbase 0.19 to 0.20..
   */
  // public static final String FILE_SYSTEM_VERSION = "6";
  public static final String FILE_SYSTEM_VERSION = "7";

  // Configuration parameters

  //TODO: Is having HBase homed on port 60k OK?

  /** Cluster is in distributed mode or not */
  public static final String CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";

  /** Cluster is standalone or pseudo-distributed */
  public static final String CLUSTER_IS_LOCAL = "false";

  /** Cluster is fully-distributed */
  public static final String CLUSTER_IS_DISTRIBUTED = "true";

  /** default host address */
  public static final String DEFAULT_HOST = "0.0.0.0";

  /** Parameter name for port master listens on. */
  public static final String MASTER_PORT = "hbase.master.port";

  /** default port that the master listens on */
  public static final int DEFAULT_MASTER_PORT = 60000;

  /** default port for master web api */
  public static final int DEFAULT_MASTER_INFOPORT = 60010;

  /** Configuration key for master web API port */
  public static final String MASTER_INFO_PORT = "hbase.master.info.port";

  /** Parameter name for the master type being backup (waits for primary to go inactive). */
  public static final String MASTER_TYPE_BACKUP = "hbase.master.backup";

  /** by default every master is a possible primary master unless the conf explicitly overrides it */
  public static final boolean DEFAULT_MASTER_TYPE_BACKUP = false;

  /** Configuration key for enabling table-level locks for schema changes */
  public static final String MASTER_SCHEMA_CHANGES_LOCK_ENABLE =
    "hbase.master.schemaChanges.lock.enable";

  /** by default we should enable table-level locks for schema changes */
  public static final boolean DEFAULT_MASTER_SCHEMA_CHANGES_LOCK_ENABLE = true;

  /** Configuration key for time out for schema modification locks */
  public static final String MASTER_SCHEMA_CHANGES_LOCK_TIMEOUT_MS =
    "hbase.master.schemaChanges.lock.timeout.ms";

  public static final int DEFAULT_MASTER_SCHEMA_CHANGES_LOCK_TIMEOUT_MS =
    60 * 1000;

  /** Configuration key for time out for schema modification try lock */
  public static final String MASTER_SCHEMA_CHANGES_TRY_LOCK_TIMEOUT_MS =
      "hbase.master.schemaChanges.trylock.timeout.ms";

  public static final int DEFAULT_MASTER_SCHEMA_CHANGES_TRY_LOCK_TIMEOUT_MS =
      5 * 1000;

  /** Configuration key for for schema modification wait interval. */
  public static final String MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS =
      "hbase.regionserver.alterTable.waitInterval.ms";

  public static final int DEFAULT_MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS =
      1000;

  /** Configuration key for for schema modification max concurrent regions closed. */
  public static final String MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE =
      "hbase.regionserver.alterTable.maxConcurrentClose";

  public static final int DEFAULT_MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE =
      5;

  /** Name of ZooKeeper quorum configuration parameter. */
  public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

  /** Name of ZooKeeper config file in conf/ directory. */
  public static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";

  /** Common prefix of ZooKeeper configuration properties */
  public static final String ZK_CFG_PROPERTY_PREFIX =
      "hbase.zookeeper.property.";

  public static final int ZK_CFG_PROPERTY_PREFIX_LEN =
    ZK_CFG_PROPERTY_PREFIX.length();

  /** Parameter name for number of times to retry writes to ZooKeeper. */
  public static final String ZOOKEEPER_RETRIES = "zookeeper.retries";

  /** Parameter name for the strategy whether aborting the process
   *  when zookeeper session expired.
   */
  public static final String ZOOKEEPER_SESSION_EXPIRED_ABORT_PROCESS =
    "hbase.zookeeper.sessionExpired.abortProcess";

  /** Parameter name for number of times to retry to connection to ZooKeeper. */
  public static final String ZOOKEEPER_CONNECTION_RETRY_NUM =
    "zookeeper.connection.retry.num";

  /**
   * The ZK client port key in the ZK properties map. The name reflects the
   * fact that this is not an HBase configuration key.
   */
  public static final String CLIENT_PORT_STR = "clientPort";

  /** Parameter name for the client port that the zookeeper listens on */
  public static final String ZOOKEEPER_CLIENT_PORT =
      ZK_CFG_PROPERTY_PREFIX + CLIENT_PORT_STR;

  /** Default number of times to retry writes to ZooKeeper. */
  public static final int DEFAULT_ZOOKEEPER_RETRIES = 5;

  /** Parameter name for ZooKeeper session time out.*/
  public static final String ZOOKEEPER_SESSION_TIMEOUT =
    "zookeeper.session.timeout";

  /** Default value for ZooKeeper session time out. */
  public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 60 * 1000;

  /** Parameter name for ZooKeeper pause between retries. In milliseconds. */
  public static final String ZOOKEEPER_PAUSE = "zookeeper.pause";
  /** Default ZooKeeper pause value. In milliseconds. */
  public static final int DEFAULT_ZOOKEEPER_PAUSE = 2 * 1000;

  /** default client port that the zookeeper listens on */
  public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

  /** Parameter name for the root dir in ZK for this cluster */
  public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase";

  /** Parameter name for port region server listens on. */
  public static final String REGIONSERVER_PORT = "hbase.regionserver.port";

  /** The Parameter for the thrift clients to talk to the thrift server **/
  public static final String REGIONSERVER_SWIFT_PORT = "hbase.regionserver.swift.port";

  /** Default port region server listens on. */
  public static final int DEFAULT_REGIONSERVER_PORT = 60020;

  /** Parameter name for thrift port master listens to. */
  public static final String MASTER_THRIFT_PORT = "hbase.master.thrift.port";

  public static final int MASTER_THRIFT_PORT_DEFAULT = 60022;

  /** The port on which thrit server is listen at **/
  public static final int DEFAULT_REGIONSERVER_SWIFT_PORT = 60021;

  /** Boolean flag to represent whether to use Hadoop RPC or not **/
  public static final String REGIONSERVER_USE_HADOOP_RPC = "hbase.regionserver.rpc.hadoop.enabled";

  /** Default value of REGIONSERVER_USE_HADOOP_RPC boolean flag **/
  public static final boolean DEFAULT_REGIONSERVER_USE_HADOOP_RPC = false;

  /** Boolean flag to represent whether to use THRIFT or not **/
  public static final String REGIONSERVER_USE_THRIFT = "hbase.regionserver.rpc.thrift.enabled";

  /** Value of the REGIONSERVER_USE_THRIFT boolean flag **/
  public static final boolean DEFAULT_REGIONSERVER_USE_THRIFT = true;

  /** Flag to enable/disable client to regionserver communication via Thrift **/
  public static final String CLIENT_TO_RS_USE_THRIFT = "hbase.client.rpc.tors.thrift.enabled";

  /**
   * The default value represent whether to use thrift in
   * client to regionserver communication
   */
  public static final boolean CLIENT_TO_RS_USE_THRIFT_DEFAULT = true;

  /** Flag to enable/disable master to regionserver communication via Thrift **/
  public static final String MASTER_TO_RS_USE_THRIFT = "hbase.master.regionserver.thrift.enabled";

  /** Value of {@link HConstants#MASTER_TO_RS_USE_THRIFT} **/
  public static final boolean MASTER_TO_RS_USE_THRIFT_DEFAULT = true;

  /**
   * The configuration parameter by which we set the client --> master
   * communication to happen via thrift.
   */
  public static final String CLIENT_TO_MASTER_USE_THRIFT =
      "hbase.client.rpc.tomaster.thrift.enabled";

  /**
   * The default value of {@link HConstants#CLIENT_TO_MASTER_USE_THRIFT}
   */
  public static final boolean CLIENT_TO_MASTER_USE_THRIFT_DEFAULT = false;

  /**
   * Knob to override the hadoop port info with thrift port info.
   */
  public static final String REGION_SERVER_WRITE_THRIFT_INFO_TO_META =
      "hbase.regionserver.write.thrift.info.to.meta";

  /**
   * Default value of {@link HConstants#REGION_SERVER_WRITE_THRIFT_INFO_TO_META}
   */
  public static final boolean REGION_SERVER_WRITE_THRIFT_INFO_TO_META_DEFAULT =
      true;

  /** default port for region server web api */
  public static final int DEFAULT_REGIONSERVER_INFOPORT = 60030;

  /** A configuration key for regionserver info port */
  public static final String REGIONSERVER_INFO_PORT =
    "hbase.regionserver.info.port";

  /** A flag that enables automatic selection of regionserver info port */
  public static final String REGIONSERVER_INFO_PORT_AUTO =
      REGIONSERVER_INFO_PORT + ".auto";

  /** Parameter name for what region server interface to use. */
  public static final String REGION_SERVER_CLASS = "hbase.regionserver.class";

  /** Parameter name for what region server implementation to use. */
  public static final String REGION_SERVER_IMPL= "hbase.regionserver.impl";

  /** Default region server interface class name. */
  public static final String DEFAULT_REGION_SERVER_CLASS = HRegionInterface.class.getName();

  /** Parameter name for enabling regionChecker */
  public static final String REGION_CHECKER_ENABLED = "hbase.master.regionchecker.enabled";
  /** Default value for enabling regionChecker */
  public static final Boolean DEFAULT_REGION_CHECKER_ENABLED = false;

  /** Parameter name for what thrift region server implementation class to use */
  public static final String THRIFT_REGION_SERVER_IMPL = "hbase.thriftregionserver.impl";

  /** Parameter name for what compaction manager to use. */
  public static final String COMPACTION_MANAGER_CLASS = "hbase.compactionmanager.class";

  /** Default compaction manager class name. */
  public static final String DEFAULT_COMPACTION_MANAGER_CLASS = CompactionManager.class.getName();

  /** Parameter name for the number of large compaction threads */
  public static final String LARGE_COMPACTION_THREADS =
          "hbase.regionserver.thread.compaction.large";

  /** Default number of large compaction threads */
  public static final int DEFAULT_LARGE_COMPACTION_THREADS = 1;

  /** Parameter name for the number of large compaction threads */
  public static final String SMALL_COMPACTION_THREADS =
          "hbase.regionserver.thread.compaction.small";

  /** Default number of small compaction threads */
  public static final int DEFAULT_SMALL_COMPACTION_THREADS = 1;

  /** Prefix for Compaction related configurations in Store */
  public static final String HSTORE_COMPACTION_PREFIX =
          "hbase.hstore.compaction.";

  /** Parameter name for the number of split threads */
  public static final String SPLIT_THREADS = "hbase.regionserver.thread.split";

  /** Default number of split threads */
  public static final int DEFAULT_SPLIT_THREADS = 1;

  /** Parameter name for what master implementation to use. */
  public static final String MASTER_IMPL = "hbase.master.impl";

  /** Parameter name for how often threads should wake up */
  public static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";

  /** Parameter name for how often a region should should perform a major compaction */
  public static final String MAJOR_COMPACTION_PERIOD = "hbase.hregion.majorcompaction";

  /** Parameter name for HBase instance root directory */
  public static final String HBASE_DIR = "hbase.rootdir";

  /** Parameter name for explicit region placement */
  public static final String LOAD_BALANCER_IMPL = "hbase.loadbalancer.impl";

  /** Used to construct the name of the log directory for a region server
   * Use '.' as a special character to seperate the log files from table data */
  public static final String HREGION_LOGDIR_NAME = ".logs";

  /** Like the previous, but for old logs that are about to be deleted */
  public static final String HREGION_OLDLOGDIR_NAME = ".oldlogs";

  /** Boolean config to determine if we should use a subdir structure
   * in the .oldlogs directory */
  public static final String HREGION_OLDLOGDIR_USE_SUBDIR_STRUCTURE =
    "hbase.regionserver.oldlogs.use.subdir.structure";

  /** Boolean config to determine if we should use a subdir structure in
   * the .oldlogs directory by default */
  public static final boolean HREGION_OLDLOGDIR_USE_SUBDIR_STRUCTURE_DEFAULT =
    true;

  /** Used to construct the name of the compaction directory during compaction */
  public static final String HREGION_COMPACTIONDIR_NAME = "compaction.dir";

  /** Conf key for the max file size after which we split the region */
  public static final String HREGION_MAX_FILESIZE =
      "hbase.hregion.max.filesize";

  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  public static final String HLOG_SPLITTING_EXT = "-splitting";

  /**
   * The max number of threads used for opening and closing stores or store
   * files in parallel
   */
  public static final String HSTORE_OPEN_AND_CLOSE_THREADS_MAX =
    "hbase.hstore.open.and.close.threads.max";

  /**
   * The default number for the max number of threads used for opening and
   * closing stores or store files in parallel
   */
  public static final int DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX = 8;

  /**
   * The max number of threads used for opening and closing regions
   * in parallel
   */
  public static final String HREGION_OPEN_AND_CLOSE_THREADS_MAX =
    "hbase.region.open.and.close.threads.max";

  /**
   * The default number for the max number of threads used for opening and
   * closing regions in parallel
   */
  public static final int DEFAULT_HREGION_OPEN_AND_CLOSE_THREADS_MAX = 8;

  /**
   * The max number of threads used for splitting logs
   * in parallel
   */
  public static final String HREGIONSERVER_SPLITLOG_WORKERS_NUM =
    "hbase.hregionserver.hlog.split.workers.num";

  /**
   * If using quorum reads from HDFS, the maximum size of the thread pool.
   * value <= 0 disables quorum reads.
   */
  public static final String HDFS_QUORUM_READ_THREADS_MAX =
    "hbase.dfsclient.quorum.reads.threads.max";

  /**
   * The default number for the size of thread pool used in quorum reads.
   * value <= 0 disables quorum reads.
   */
  public static final int DEFAULT_HDFS_QUORUM_READ_THREADS_MAX = 50;

  /**
   * If using quorum reads from HDFS, the timeout of using another region server.
   */
  public static final String HDFS_QUORUM_READ_TIMEOUT_MILLIS =
    "hbase.dfsclient.quorum.reads.timeout";
  public static final long DEFAULT_HDFS_QUORUM_READ_TIMEOUT_MILLIS = 0;


  /** Default maximum file size */
  public static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

  /** Default minimum number of files to be compacted */
  public static final int DEFAULT_MIN_FILES_TO_COMPACT = 3;

  /** Default value for files without minFlushTime in metadata */
  public static final long NO_MIN_FLUSH_TIME = -1;

  /** Conf key for the memstore size at which we flush the memstore */
  public static final String HREGION_MEMSTORE_FLUSH_SIZE =
      "hbase.hregion.memstore.flush.size";

  /** Conf key for enabling Per Column Family flushing of memstores */
  public static final String HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH =
      "hbase.hregion.memstore.percolumnfamilyflush.enabled";

  /** Default value for the Per Column Family flush knob */
  public static final Boolean DEFAULT_HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH =
    false;

  /**
   * If Per Column Family flushing is enabled, this is the minimum size
   * at which a column family's memstore is flushed.
   */
  public static final String HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE =
      "hbase.hregion.memstore.percolumnfamilyflush.flush.size";

  public static final String HREGION_MEMSTORE_BLOCK_MULTIPLIER =
      "hbase.hregion.memstore.block.multiplier";
  public static final String HREGION_MEMSTORE_WAIT_ON_BLOCK =
      "hbase.hregion.memstore.block.waitonblock";

  /** Default size of a reservation block   */
  public static final int DEFAULT_SIZE_RESERVATION_BLOCK = 1024 * 1024 * 5;

  /** Maximum value length, enforced on KeyValue construction */
  public static final int MAXIMUM_VALUE_LENGTH = Integer.MAX_VALUE;

  /** Conf key for enabling/disabling server profiling */
  public static final String HREGIONSERVER_ENABLE_SERVERSIDE_PROFILING =
      "hbase.regionserver.enable.serverside.profiling";

  /** Conf key for the preload blocks count if preloading is enabled for some scanner */
  public static final String SCAN_PRELOAD_BLOCK_COUNT =
      "hbase.regionserver.preload.block.count";
  /** Default number of blocks to preload during sequential scan of hfile (if enabled)*/
  public static final int DEFAULT_PRELOAD_BLOCK_COUNT = 64;
  /** Conf key for the core preload threads */
  public static final String CORE_PRELOAD_THREAD_COUNT = "hbase.regionserver.core.preload.thread.count";
  /** Default number of core preload threads per region server */
  public static final int DEFAULT_CORE_PRELOAD_THREAD_COUNT = 1;
  /** Conf key for the max preload threads */
  public static final String MAX_PRELOAD_THREAD_COUNT = "hbase.regionserver.max.preload.thread.count";
  /** Defualt number of core preload threads per region server */
  public static final int DEFAULT_MAX_PRELOAD_THREAD_COUNT = 64;
  /** Conf key for max preload blocks kept in cache per hfilescanner */
  public static final String MAX_PRELOAD_BLOCKS_KEPT_IN_CACHE =
      "hbase.regionserver.preload.blocks.kept.in.cache";
  /** Default maximum number of preload blocks to keep in block cache per hfilescanner */
  public static final int DEFAULT_MAX_PRELOAD_BLOCKS_KEPT_IN_CACHE = 128;

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.

  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?

  // Note that the name of the root table starts with "-" and the name of the
  // meta table starts with "." Why? it's a trick. It turns out that when we
  // store region names in memory, we use a SortedMap. Since "-" sorts before
  // "." (and since no other table name can start with either of these
  // characters, the root region will always be the first entry in such a Map,
  // followed by all the meta regions (which will be ordered by their starting
  // row key as well), followed by all user tables. So when the Master is
  // choosing regions to assign, it will always choose the root region first,
  // followed by the meta regions, followed by user regions. Since the root
  // and meta regions always need to be on-line, this ensures that they will
  // be the first to be reassigned if the server(s) they are being served by
  // should go down.


  //
  // New stuff.  Making a slow transition.
  //

  /** The root table's name.*/
  public static final byte [] ROOT_TABLE_NAME = Bytes.toBytes("-ROOT-");

  /** The META table's name. */
  public static final byte [] META_TABLE_NAME = Bytes.toBytes(".META.");

  /** delimiter used between portions of a region name */
  public static final int META_ROW_DELIMITER = ',';

  /** The catalog family as a string*/
  public static final String CATALOG_FAMILY_STR = "info";

  /** The catalog family */
  public static final byte [] CATALOG_FAMILY = Bytes.toBytes(CATALOG_FAMILY_STR);

  /** The catalog historian family */
  public static final byte [] CATALOG_HISTORIAN_FAMILY = Bytes.toBytes("historian");

  /** The regioninfo column qualifier */
  public static final byte [] REGIONINFO_QUALIFIER = Bytes.toBytes("regioninfo");

  /** The thriftregioninfo column qualifier */
  public static final byte [] THRIFT_REGIONINFO_QUALIFIER = Bytes.toBytes("tregioninfo");

  /** The server column qualifier */
  public static final byte [] SERVER_QUALIFIER = Bytes.toBytes("server");

  /** The startcode column qualifier */
  public static final byte [] STARTCODE_QUALIFIER = Bytes.toBytes("serverstartcode");

  /** The lower-half split region column qualifier */
  public static final byte [] SPLITA_QUALIFIER = Bytes.toBytes("splitA");

  /** The upper-half split region column qualifier */
  public static final byte [] SPLITB_QUALIFIER = Bytes.toBytes("splitB");

  /** The favored nodes column qualifier*/
  public static final byte [] FAVOREDNODES_QUALIFIER = Bytes.toBytes("favorednodes");

  // Other constants

  /**
   * An empty instance.
   */
  public static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);

  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  public static final byte [] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

  public static final ByteBuffer EMPTY_START_ROW_BUF = ByteBuffer.wrap(EMPTY_START_ROW);

  /**
   * Last row in a table.
   */
  public static final byte [] EMPTY_END_ROW = EMPTY_START_ROW;

  public static final ByteBuffer EMPTY_END_ROW_BUF = ByteBuffer.wrap(EMPTY_END_ROW);

  /**
    * Used by scanners and others when they're trying to detect the end of a
    * table
    */
  public static final byte [] LAST_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Max length a row can have because of the limitation in TFile.
   */
  public static final int MAX_ROW_LENGTH = Short.MAX_VALUE;

  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Timestamp to use when we want to refer to the oldest cell.
   */
  public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;

  /**
   * LATEST_TIMESTAMP in bytes form
   */
  public static final byte [] LATEST_TIMESTAMP_BYTES = Bytes.toBytes(LATEST_TIMESTAMP);

  /**
   * Define for 'return-all-versions'.
   */
  public static final int ALL_VERSIONS = Integer.MAX_VALUE;

  /**
   * Unlimited time-to-live.
   */
//  public static final int FOREVER = -1;
  public static final int FOREVER = Integer.MAX_VALUE;

  /**
   * Seconds in a week
   */
  public static final int WEEK_IN_SECONDS = 7 * 24 * 3600;

  //TODO: although the following are referenced widely to format strings for
  //      the shell. They really aren't a part of the public API. It would be
  //      nice if we could put them somewhere where they did not need to be
  //      public. They could have package visibility
  public static final String NAME = "NAME";
  public static final String VERSIONS = "VERSIONS";
  public static final String IN_MEMORY = "IN_MEMORY";
  public static final String CONFIG = "CONFIG";

  /**
   * This is a retry backoff multiplier table similar to the BSD TCP syn
   * backoff table, a bit more aggressive than simple exponential backoff.
   */
  public static int RETRY_BACKOFF[] = { 1, 1, 1, 2, 2, 4, 4, 8, 16, 32 };

  public static final String REGION_IMPL = "hbase.hregion.impl";

  /** modifyTable op for replacing the table descriptor */
  public static enum Modify {
    CLOSE_REGION,
    MOVE_REGION,
    TABLE_COMPACT,
    TABLE_FLUSH,
    TABLE_MAJOR_COMPACT,
    TABLE_SET_HTD,
    TABLE_SPLIT,
    TABLE_EXPLICIT_SPLIT
  }

  /**
   * Scope tag for locally scoped data.
   * This data will not be replicated.
   */
  public static final int REPLICATION_SCOPE_LOCAL = 0;

  /**
   * Scope tag for globally scoped data.
   * This data will be replicated to all peers.
   */
  public static final int REPLICATION_SCOPE_GLOBAL = 1;

  /**
   * Default cluster ID, cannot be used to identify a cluster so a key with
   * this value means it wasn't meant for replication.
   */
  public static final byte DEFAULT_CLUSTER_ID = 0;

  /**
   * Parameter name for maximum number of bytes returned when calling a
   * scanner's next method.
   */
  public static final String HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY = "hbase.client.scanner.max.result.size";

  /**
   * Parameter name for queue length of HTableResultScanner. The total number of
   * prefetched and cached Results are this number times scanner.getCaching().
   *
   */
  public static final String HBASE_CLIENT_SCANNER_QUEUE_LENGTH = "hbase.client.scanner.queue.length";
  public static final int DEFAULT_HBASE_CLIENT_SCANNER_QUEUE_LENGTH = 1;

  /**
   * Parameter name for the number of threads for the ParallelScanner
   */
  public static final String HBASE_CLIENT_PARALLEL_SCANNER_THREAD =
    "hbase.client.parallel.scanner.thread";

  /**
   * The default number of threads for the ParallelScanner
   */
  public static final int HBASE_CLIENT_PARALLEL_SCANNER_THREAD_DEFAULT = 100;

  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Note that when a single row is larger than this limit the row is still
   * returned completely.
   *
   * The default value is unlimited.
   */
  public static final long DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE = Long.MAX_VALUE;


  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Used with partialRow parameter on the client side.  Note that when a
   * single row is larger than this limit, the row is still returned completely
   * if partialRow is true, otherwise, the row will be truncated in order to
   * fit the memory.
   */
  public static final int DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE = Integer.MAX_VALUE;

  /**
   * HRegion server lease period in milliseconds. Clients must report in within this period
   * else they are considered dead. Unit measured in ms (milliseconds).
   */
  public static final String HBASE_REGIONSERVER_LEASE_PERIOD_KEY   = "hbase.regionserver.lease.period";


  /**
   * Default value of {@link #HBASE_REGIONSERVER_LEASE_PERIOD_KEY}.
   */
  public static final long DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD = 60000;

  /**
   * timeout for each RPC
   */
  public static final String HBASE_RPC_TIMEOUT_KEY = "hbase.rpc.timeout";
  public static final String HBASE_RS_REPORT_TIMEOUT_KEY = "hbase.regionserverReport.timeout";

  /**
   * Default value of {@link #HBASE_RPC_TIMEOUT_KEY}
   */
  public static final int DEFAULT_HBASE_RPC_TIMEOUT = 60000;
  public static final int DEFAULT_RS_REPORT_TIMEOUT = 3000;

  /**
   * pause between rpc or connect retries
   */
  public static final String HBASE_CLIENT_PAUSE = "hbase.client.pause";
  public static final int DEFAULT_HBASE_CLIENT_PAUSE = 1000;

  /**
   * compression for each RPC and its default value
   */
  public static final String HBASE_RPC_COMPRESSION_KEY = "hbase.rpc.compression";
  public static final Compression.Algorithm DEFAULT_HBASE_RPC_COMPRESSION =
    Compression.Algorithm.NONE;

  public static final String
      REPLICATION_ENABLE_KEY = "hbase.replication";

  /**
   * Configuration key for the size of the block cache
   */
  public static final String HFILE_BLOCK_CACHE_SIZE_KEY =
      "hfile.block.cache.size";

  public static final float HFILE_BLOCK_CACHE_SIZE_DEFAULT = 0.25f;

  /** The delay when re-trying a socket operation in a loop (HBASE-4712) */
  public static final int SOCKET_RETRY_WAIT_MS = 200;

  /** Host name of the local machine */
  public static final String LOCALHOST = "localhost";

  public static final String LOCALHOST_IP = "127.0.0.1";

  /** Conf key that enables distributed log splitting */
  public static final String DISTRIBUTED_LOG_SPLITTING_KEY =
      "hbase.master.distributed.log.splitting";

  public static final int REGION_SERVER_MSG_INTERVAL = 1 * 1000;

  /** The number of favored nodes for each region */
  public static final int FAVORED_NODES_NUM = 3;

  public static final String UNKNOWN_RACK = "Unknown Rack";

  /** Delay when waiting for a variable (HBASE-4712) */
  public static final int VARIABLE_WAIT_TIME_MS = 40;

  public static final String LOAD_BALANCER_SLOP_KEY = "hbase.regions.slop";

  // Thrift server configuration options

  /** Configuration key prefix for the stand-alone thrift proxy */
  public static final String THRIFT_PROXY_PREFIX = "hbase.thrift.";

  /** Configuration key prefix for thrift server embedded into the region server */
  public static final String RS_THRIFT_PREFIX = "hbase.regionserver.thrift.";

  /** Default port for the stand-alone thrift proxy */
  public static final int DEFAULT_THRIFT_PROXY_PORT = 9090;

  /** Default port for the thrift server embedded into regionserver */
  public static final int DEFAULT_RS_THRIFT_SERVER_PORT = 9091;

  /** Configuration key suffix for thrift server type (e.g. thread pool, nonblocking, etc.) */
  public static final String THRIFT_SERVER_TYPE_SUFFIX = "server.type";

  /** Configuration key suffix for the IP address for thrift server to bind to */
  public static final String THRIFT_BIND_SUFFIX = "ipaddress";

  /** Configuration key suffix for whether to use compact Thrift transport */
  public static final String THRIFT_COMPACT_SUFFIX = "compact";

  /** Configuration key suffix for whether to use framed Thrift transport */
  public static final String THRIFT_FRAMED_SUFFIX = "framed";

  /** Configuration key suffix for Thrift server port */
  public static final String THRIFT_PORT_SUFFIX = "port";

  /** The number of HLogs for each region server */
  public static final String HLOG_CNT_PER_SERVER = "hbase.regionserver.hlog.cnt.perserver";

  public static final String HLOG_FORMAT_BACKWARD_COMPATIBILITY =
      "hbase.regionserver.hlog.format.backward.compatibility";

  /**
   * Number of threads for swift server
   */
  public static final String SWIFT_WORKER_THREADS = "hbase.swift.worker.threads";
  public static final int SWIFT_WORKER_THREADS_DEFAULT = 300;

  /**
   * Number of io threads for swift server
   */
  public static final String SWIFT_IO_THREADS = "hbase.swift.io.threads";
  public static final int SWIFT_IO_THREADS_DEFAULT = 60;

  /**
   * Frame size used for both client and server
   */
  public static final String SWIFT_MAX_FRAME_SIZE_BYTES = "hbase.swift.max.response.size";
  public static final int SWIFT_MAX_FRAME_SIZE_BYTES_DEFAULT = Integer.MAX_VALUE;

  /**
   * Number of connections for swift server
   */
  public static final String SWIFT_CONNECTION_LIMIT = "hbase.swift.connection.limit";
  public static final int SWIFT_CONNECTION_LIMIT_DEFAULT = 50000;

  /**
   * The byte array represents for NO_NEXT_INDEXED_KEY;
   * The actual value is irrelevant because this is always compared by reference.
   */
  public static final byte [] NO_NEXT_INDEXED_KEY = Bytes.toBytes("NO_NEXT_INDEXED_KEY");

  public static final int MULTIPUT_SUCCESS = -1;

  public static final boolean[] BOOLEAN_VALUES = { false, true };

  public static final int IPC_CALL_PARAMETER_LENGTH_MAX = 1000;

  /**
   * Used in Configuration to get/set the KV aggregator
   */
  public static final String KV_AGGREGATOR = "kvaggregator";

  /**
   * Used in Configuration to get/set the compaction hook
   */
  public static final String COMPACTION_HOOK = "compaction_hook";

  /**
   * Absolute path of the external jar which will contain the custom compaction hook
   */
  public static final String COMPACTION_HOOK_JAR = "compaction_hook_jar";

  public static final String GENERAL_BLOOM_FILTER = "general_bloom_filter";

  public static final String DELETE_FAMILY_BLOOM_FILTER = "delete_family_bloom_filter";

  public static final String DELETE_COLUMN_BLOOM_FILTER = "delete_column_bloom_filter";

  public static final String ROWKEY_PREFIX_BLOOM_FILTER = "rowkey_prefix_bloom_filter";

  /**
   * This will enable/disable the usage of delete col bloom filter. Note that
   * this won't enable/disable the delete bloom filter for being written/read.
   * In fact, we could read and write it but we will not use it when we scan
   * data, thus we won't do the optimized reads. In order to disable/enable the
   * filter for write&read both, use
   * BloomFilterFactory.IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED
   */
  public static final boolean USE_DELETE_COLUMN_BLOOM_FILTER = true;
  public static final String USE_DELETE_COLUMN_BLOOM_FILTER_STRING = "use_delete_column_bloom_filter";

  // Delaying the region server load balancing by the following amount for a
  // load balancing where source is a favored region server.
  public static final String HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS
                                = "hbase.master.assignment.load.balancer.waittime.ms";
  public static final int DEFAULT_HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS = 60000;

  /*
   * This defines the number of buckets used for computing the histogram of
   * pread latency.
   */
  public static final String PREAD_LATENCY_HISTOGRAM_NUM_BUCKETS =
      "hbase.histogrambasedmetric.numbuckets.preadlatency";

  /*
   * This defines the number of buckets used for computing the histogram of
   * pread latency during compaction.
   */
  public static final String PREAD_COMPACTION_LATENCY_HISTOGRAM_NUM_BUCKETS =
      "hbase.histogrambasedmetric.numbuckets.preadcompactionlatency";
  public static final String HISTOGRAM_BASED_METRICS_WINDOW =
      "hbase.histogrambasedmetric.window";
  /*
   * This is the folder address for the hard links folder where the
   * hard links are created during creating a read only store.
   */
  public static final String READ_ONLY_HARDLINKS_FOLDER =
      "hbase.store.readonly.hardlinks.folder";
  public static final String READ_ONLY_HARDLINKS_FOLDER_DEFAULT =
      "/tmp/hardlinks/";

  public static final String CLIENT_SOCKED_CLOSED_EXC_MSG = "Interrupting the read request";
  public static final String SERVER_INTERRUPTED_CALLS_KEY = "serverInterruptedCalls";

  /**
   * Controls whether we use one or two throttles to control the insert in the queue
   */
  public static final String USE_MULTIPLE_THROTTLES = "hbase.server.multithrottler";

  /**
   * How much memory do we want for the blocking callqueue, used in HBaseServer
   */
   public static final long MAX_CALL_QUEUE_MEMORY_SIZE = 1024*1024*1024;
   public static final String MAX_CALL_QUEUE_MEMORY_SIZE_STRING = "max.callqueue.memory.size";

   /**
    * Used in HBase Server, when we use the multithrottler for the callQueue
    */
   public static final long MAX_SMALLER_CALL_QUEUE_MEMORY_SIZE = 256*1024*1024;
   public static final String MAX_SMALLER_CALL_QUEUE_MEMORY_SIZE_STRING = "max.smaller.callqueue.memory.size";
   public static final long MAX_LARGER_CALL_QUEUE_MEMORY_SIZE = 768*1024*1024;
   public static final String MAX_LARGER_CALL_QUEUE_MEMORY_SIZE_STRING = "max.larger.callqueue.memory.size";
   public static final int SMALL_QUEUE_REQUEST_LIMIT = 25*1024*1024;
   public static final String SMALL_QUEUE_REQUEST_LIMIT_STRING = "small.queue.request.limit";
  public static final String USE_LOCATEREGION_V2 = "hbase.Client.hconnectionmanager.use.locateregionv2";


  public static final String CLIENT_RETRY_NUM_STRING = "hbase.client.retries.number";
  public static final int DEFAULT_CLIENT_RETRY_NUM = 10;

  public static final String SERVER_REQUESTED_RETRIES_STRING = "hbase.client.server.requested.retries.max";
  public static final int DEFAULT_SERVER_REQUESTED_RETRIES = 0;

  public static final String CLIENT_RPC_RETRY_TIMEOUT_STRING = "hbase.client.rpc.retry.timeout";
  public static final long DEFAULT_CLIENT_RPC_RETRY_TIMEOUT = Long.MAX_VALUE;

  public static final String HTABLE_ASYNC_CALLS = "hbase.htable.async.calls";
  public static final boolean HTABLE_ASYNC_CALLS_DEFAULT = false;

  public static final int DEFAULT_HTABLE_ASYNC_CORE_THREADS = 100;
  public static final int DEFAULT_HTABLE_ASYNC_MAX_THREADS = 1000;
  public static final int DEFAULT_HTABLE_ASYNC_KEEPALIVE_SECONDS = 60;

  // These are the IO priority values for various regionserver operations. Note
  // that these are priorities relative to each other. See the man page for
  // ioprio_set for more details. The default priority for a process with nice
  // value 0 is 4. The priorities range from 0 (highest) to 7 (lowest).
  //
  // The thinking behind the various priorities are as follows :
  // 1. PREAD priority is the highest since client reads are extremely critical.
  // 2. Although HLOG sync is as important as a pread (since the client
  // blocks on it.). But the HLOG sync never hits disk in the critical path
  // and these priorities are when the kernel scheduler writes data to the
  // persistent store. This priority will only be considered when we close the
  // HLOG and help in reducing any stalls while closing the hlog.
  // 3. The priority for flush is more than compaction since if we don't flush
  // quickly enough, the memstore might grow too much and block client updates.
  public static final int PREAD_PRIORITY = 0;
  public static final int HLOG_PRIORITY = 1;
  public static final int FLUSH_PRIORITY = 2;
  public static final int COMPACT_PRIORITY = 3;

  // We use the Best Effort class always since RealTime and Idle are too
  // extreme. Again check man pages for ioprio_set for more details.
  public static final int IOPRIO_CLASSOF_SERVICE = NativeIO.IOPRIO_CLASS_BE;

  public static final String HBASE_ENABLE_QOS_KEY = "hbase.enable.qos";
  public static final String HBASE_ENABLE_SYNCFILERANGE_THROTTLING_KEY = "hbase.enable.syncfilerange.throttling";

  /*
   * MSLAB Constants
   */
  public final static String MSLAB_CHUNK_POOL_MAX_SIZE_KEY = "hbase.hregion.memstore.chunkpool.maxsize";
  public final static String MSLAB_CHUNK_POOL_INITIAL_SIZE_KEY = "hbase.hregion.memstore.chunkpool.initialsize";
  public final static float MSLAB_POOL_MAX_SIZE_DEFAULT = 0.0f;
  public final static float MSLAB_POOL_INITIAL_SIZE_DEFAULT = 0.0f;

  public final static String MSLAB_CHUNK_SIZE_KEY = "hbase.hregion.memstore.mslab.chunksize";
  public final static int MSLAB_CHUNK_SIZE_DEFAULT = 2 * 1024 * 1024;

  public final static String MSLAB_MAX_ALLOC_KEY = "hbase.hregion.memstore.mslab.max.allocation";
  public final static int MSLAB_MAX_ALLOC_DEFAULT = 256  * 1024; // allocs bigger than this don't go through allocator

  public final static String MSLAB_MAX_SIZE_KEY = "hbase.hregion.memstore.mslab.max.size";
  public final static float MSLAB_MAX_SIZE_DEFAULT = 1.25f; // Stop using SLAB if larger than this percentage of memstore size

  public final static float MSLAB_PCT_LOWER_LIMIT = 0.0f;
  public final static float MSLAB_PCT_UPPER_LIMIT = 2.0f;


  /*
   * Memstore Linear search limit
   */
  public final static String MEMSTORE_RESEEK_LINEAR_SEARCH_LIMIT_KEY = "hbase.hregion.memstore.linear.search.limit";
  public final static int MEMSTORE_RESEEK_LINEAR_SEARCH_LIMIT_DEFAULT = 20;

  public static final String USE_MSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  public static final boolean USE_MSLAB_DEFAULT = false;

  /**
   * This wait time is used to periodically probe until
   * we exhaust the timeout in the window
   */
  public static final String WAIT_TIME_FOR_FLUSH_MS =
      "hbase.hregion.flush.waittime";
  public static final long DEFAULT_WAIT_TIME_FOR_FLUSH_MS = 100; //ms

  /**
   * The knob to turn on the ClientLocalScanner to flush and wait for the
   * region flush to finish before it opens the store files.
   * Set the socket timeout for the RPC appropriately for this.
   */
  public static final String CLIENT_LOCAL_SCANNER_FLUSH_AND_WAIT =
      "hbase.clientlocalscanner.flush.and.wait";
  public static final boolean DEFAULT_CLIENT_LOCAL_SCANNER_FLUSH_AND_WAIT =
      false;

  /**
   * The acceptable staleness of a flush. Say if this value is set to 10s,
   * if there was a flush in the last 10s, we would not flush again.
   */
  public static final String CLIENT_LOCAL_SCANNER_FLUSH_ACCEPTABLE_STALENESS_MS =
      "hbase.clientlocalscanner.flush.acceptable.staleness";
  public static final long DEFAULT_CLIENT_LOCAL_SCANNER_FLUSH_ACCEPTABLE_STALENESS_MS =
      30000; // ms

  /**
   * The extra wait time that we wait for the flush to take place.
   */
  public static final String CLIENT_LOCAL_SCANNER_MAX_WAITTIME_FOR_FLUSH_MS =
      "hbase.clientlocal.scanner.flush.maxwaittime";
  public static final int DEFAULT_CLIENT_LOCAL_SCANNER_MAX_WAITTIME_FOR_FLUSH_MS
    = 10000; // ms

  public static final String CLIENT_SIDE_SCAN = "hbase.client.side.scan";
  public static final boolean DEFAULT_CLIENT_SIDE_SCAN = false;
  public static final String USE_HFILEHISTOGRAM = "hbase.client.hfilehistogram.enabled";
  public static final boolean DEFAULT_USE_HFILEHISTOGRAM = true;

  public static final String IN_MEMORY_BLOOM_ENABLED =
      "hbase.hregion.memstore.bloom.filter.enabled";
  public static final boolean DEFAULT_IN_MEMORY_BLOOM_ENABLED = false;


  /**
   * Use header protocol on client and server. This is used for call context
   * and disabling/enabling this would do it for both the client and server.
   */
  public static final String USE_HEADER_PROTOCOL = "hbase.client.headerprotocol.enabled";
  public static final boolean DEFAULT_USE_HEADER_PROTOCOL = true;

  public static final String THRIFT_HEADER_FROM_SERVER = "serverHeader";
  public static final String THRIFT_HEADER_FROM_CLIENT = "clientHeader";

  public static final boolean DISABLE_THRIFT_REGION_INFO_QUALIFIER = false;

  private HConstants() {
    // Can't be instantiated with this constructor.
  }
}
