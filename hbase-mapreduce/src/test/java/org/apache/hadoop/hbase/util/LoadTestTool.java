/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.hbase.util.test.LoadTestDataGeneratorWithACL;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.AlreadySelectedException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.MissingOptionException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

/**
 * A command-line utility that reads, writes, and verifies data. Unlike
 * {@link org.apache.hadoop.hbase.PerformanceEvaluation}, this tool validates the data written,
 * and supports simultaneously writing and reading the same set of keys.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class LoadTestTool extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(LoadTestTool.class);
  private static final String COLON = ":";

  /** Table name for the test */
  private TableName tableName;

  /** Column families for the test */
  private byte[][] families;

  /** Table name to use of not overridden on the command line */
  protected static final String DEFAULT_TABLE_NAME = "cluster_test";

  /** The default data size if not specified */
  protected static final int DEFAULT_DATA_SIZE = 64;

  /** The number of reader/writer threads if not specified */
  protected static final int DEFAULT_NUM_THREADS = 20;

  /** Usage string for the load option */
  protected static final String OPT_USAGE_LOAD =
      "<avg_cols_per_key>:<avg_data_size>" +
      "[:<#threads=" + DEFAULT_NUM_THREADS + ">]";

  /** Usage string for the read option */
  protected static final String OPT_USAGE_READ =
      "<verify_percent>[:<#threads=" + DEFAULT_NUM_THREADS + ">]";

  /** Usage string for the update option */
  protected static final String OPT_USAGE_UPDATE =
      "<update_percent>[:<#threads=" + DEFAULT_NUM_THREADS
      + ">][:<#whether to ignore nonce collisions=0>]";

  protected static final String OPT_USAGE_BLOOM = "Bloom filter type, one of " +
      Arrays.toString(BloomType.values());

  protected static final String OPT_USAGE_COMPRESSION = "Compression type, " +
      "one of " + Arrays.toString(Compression.Algorithm.values());

  protected static final String OPT_VERBOSE = "verbose";

  public static final String OPT_BLOOM = "bloom";
  public static final String OPT_BLOOM_PARAM = "bloom_param";
  public static final String OPT_COMPRESSION = "compression";
  public static final String OPT_DEFERRED_LOG_FLUSH = "deferredlogflush";
  public static final String OPT_DEFERRED_LOG_FLUSH_USAGE = "Enable deferred log flush.";

  public static final String OPT_INMEMORY = "in_memory";
  public static final String OPT_USAGE_IN_MEMORY = "Tries to keep the HFiles of the CF " +
      "inmemory as far as possible.  Not guaranteed that reads are always served from inmemory";

  public static final String OPT_GENERATOR = "generator";
  public static final String OPT_GENERATOR_USAGE = "The class which generates load for the tool."
      + " Any args for this class can be passed as colon separated after class name";

  public static final String OPT_WRITER = "writer";
  public static final String OPT_WRITER_USAGE = "The class for executing the write requests";

  public static final String OPT_UPDATER = "updater";
  public static final String OPT_UPDATER_USAGE = "The class for executing the update requests";

  public static final String OPT_READER = "reader";
  public static final String OPT_READER_USAGE = "The class for executing the read requests";

  protected static final String OPT_KEY_WINDOW = "key_window";
  protected static final String OPT_WRITE = "write";
  protected static final String OPT_MAX_READ_ERRORS = "max_read_errors";
  public static final String OPT_MULTIPUT = "multiput";
  public static final String OPT_MULTIGET = "multiget_batchsize";
  protected static final String OPT_NUM_KEYS = "num_keys";
  protected static final String OPT_READ = "read";
  protected static final String OPT_START_KEY = "start_key";
  public static final String OPT_TABLE_NAME = "tn";
  public static final String OPT_COLUMN_FAMILIES = "families";
  protected static final String OPT_ZK_QUORUM = "zk";
  protected static final String OPT_ZK_PARENT_NODE = "zk_root";
  protected static final String OPT_SKIP_INIT = "skip_init";
  protected static final String OPT_INIT_ONLY = "init_only";
  protected static final String NUM_TABLES = "num_tables";
  protected static final String OPT_BATCHUPDATE = "batchupdate";
  protected static final String OPT_UPDATE = "update";

  public static final String OPT_ENCRYPTION = "encryption";
  protected static final String OPT_ENCRYPTION_USAGE =
    "Enables transparent encryption on the test table, one of " +
    Arrays.toString(Encryption.getSupportedCiphers());

  public static final String OPT_NUM_REGIONS_PER_SERVER = "num_regions_per_server";
  protected static final String OPT_NUM_REGIONS_PER_SERVER_USAGE
    = "Desired number of regions per region server. Defaults to 5.";
  public static int DEFAULT_NUM_REGIONS_PER_SERVER = 5;

  public static final String OPT_REGION_REPLICATION = "region_replication";
  protected static final String OPT_REGION_REPLICATION_USAGE =
      "Desired number of replicas per region";

  public static final String OPT_REGION_REPLICA_ID = "region_replica_id";
  protected static final String OPT_REGION_REPLICA_ID_USAGE =
      "Region replica id to do the reads from";

  public static final String OPT_MOB_THRESHOLD = "mob_threshold";
  protected static final String OPT_MOB_THRESHOLD_USAGE =
      "Desired cell size to exceed in bytes that will use the MOB write path";

  protected static final long DEFAULT_START_KEY = 0;

  /** This will be removed as we factor out the dependency on command line */
  protected CommandLine cmd;

  protected MultiThreadedWriter writerThreads = null;
  protected MultiThreadedReader readerThreads = null;
  protected MultiThreadedUpdater updaterThreads = null;

  protected long startKey, endKey;

  protected boolean isVerbose, isWrite, isRead, isUpdate;
  protected boolean deferredLogFlush;

  // Column family options
  protected DataBlockEncoding dataBlockEncodingAlgo;
  protected Compression.Algorithm compressAlgo;
  protected BloomType bloomType;
  private boolean inMemoryCF;

  private User userOwner;
  // Writer options
  protected int numWriterThreads = DEFAULT_NUM_THREADS;
  protected int minColsPerKey, maxColsPerKey;
  protected int minColDataSize = DEFAULT_DATA_SIZE, maxColDataSize = DEFAULT_DATA_SIZE;
  protected boolean isMultiPut;

  // Updater options
  protected int numUpdaterThreads = DEFAULT_NUM_THREADS;
  protected int updatePercent;
  protected boolean ignoreConflicts = false;
  protected boolean isBatchUpdate;

  // Reader options
  private int numReaderThreads = DEFAULT_NUM_THREADS;
  private int keyWindow = MultiThreadedReader.DEFAULT_KEY_WINDOW;
  private int multiGetBatchSize = MultiThreadedReader.DEFAULT_BATCH_SIZE;
  private int maxReadErrors = MultiThreadedReader.DEFAULT_MAX_ERRORS;
  private int verifyPercent;

  private int numTables = 1;

  private String superUser;

  private String userNames;
  //This file is used to read authentication information in secure clusters.
  private String authnFileName;

  private int numRegionsPerServer = DEFAULT_NUM_REGIONS_PER_SERVER;
  private int regionReplication = -1; // not set
  private int regionReplicaId = -1; // not set

  private int mobThreshold = -1; // not set

  // TODO: refactor LoadTestToolImpl somewhere to make the usage from tests less bad,
  //       console tool itself should only be used from console.
  protected boolean isSkipInit = false;
  protected boolean isInitOnly = false;

  protected Cipher cipher = null;

  protected String[] splitColonSeparated(String option,
      int minNumCols, int maxNumCols) {
    String optVal = cmd.getOptionValue(option);
    String[] cols = optVal.split(COLON);
    if (cols.length < minNumCols || cols.length > maxNumCols) {
      throw new IllegalArgumentException("Expected at least "
          + minNumCols + " columns but no more than " + maxNumCols +
          " in the colon-separated value '" + optVal + "' of the " +
          "-" + option + " option");
    }
    return cols;
  }

  protected int getNumThreads(String numThreadsStr) {
    return parseInt(numThreadsStr, 1, Short.MAX_VALUE);
  }

  public byte[][] getColumnFamilies() {
    return families;
  }

  /**
   * Apply column family options such as Bloom filters, compression, and data
   * block encoding.
   */
  protected void applyColumnFamilyOptions(TableName tableName,
      byte[][] columnFamilies) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()) {
      TableDescriptor tableDesc = admin.getDescriptor(tableName);
      LOG.info("Disabling table " + tableName);
      admin.disableTable(tableName);
      for (byte[] cf : columnFamilies) {
        ColumnFamilyDescriptor columnDesc = tableDesc.getColumnFamily(cf);
        boolean isNewCf = columnDesc == null;
        ColumnFamilyDescriptorBuilder columnDescBuilder = isNewCf ?
            ColumnFamilyDescriptorBuilder.newBuilder(cf) :
            ColumnFamilyDescriptorBuilder.newBuilder(columnDesc);
        if (bloomType != null) {
          columnDescBuilder.setBloomFilterType(bloomType);
        }
        if (compressAlgo != null) {
          columnDescBuilder.setCompressionType(compressAlgo);
        }
        if (dataBlockEncodingAlgo != null) {
          columnDescBuilder.setDataBlockEncoding(dataBlockEncodingAlgo);
        }
        if (inMemoryCF) {
          columnDescBuilder.setInMemory(inMemoryCF);
        }
        if (cipher != null) {
          byte[] keyBytes = new byte[cipher.getKeyLength()];
          Bytes.secureRandom(keyBytes);
          columnDescBuilder.setEncryptionType(cipher.getName());
          columnDescBuilder.setEncryptionKey(
              EncryptionUtil.wrapKey(conf,
                  User.getCurrent().getShortName(),
                  new SecretKeySpec(keyBytes,
                      cipher.getName())));
        }
        if (mobThreshold >= 0) {
          columnDescBuilder.setMobEnabled(true);
          columnDescBuilder.setMobThreshold(mobThreshold);
        }

        if (isNewCf) {
          admin.addColumnFamily(tableName, columnDescBuilder.build());
        } else {
          admin.modifyColumnFamily(tableName, columnDescBuilder.build());
        }
      }
      LOG.info("Enabling table " + tableName);
      admin.enableTable(tableName);
    }
  }

  @Override
  protected void addOptions() {
    addOptNoArg("v", OPT_VERBOSE, "Will display a full readout of logs, including ZooKeeper");
    addOptWithArg(OPT_ZK_QUORUM, "ZK quorum as comma-separated host names " +
        "without port numbers");
    addOptWithArg(OPT_ZK_PARENT_NODE, "name of parent znode in zookeeper");
    addOptWithArg(OPT_TABLE_NAME, "The name of the table to read or write");
    addOptWithArg(OPT_COLUMN_FAMILIES, "The name of the column families to use separated by comma");
    addOptWithArg(OPT_WRITE, OPT_USAGE_LOAD);
    addOptWithArg(OPT_READ, OPT_USAGE_READ);
    addOptWithArg(OPT_UPDATE, OPT_USAGE_UPDATE);
    addOptNoArg(OPT_INIT_ONLY, "Initialize the test table only, don't do any loading");
    addOptWithArg(OPT_BLOOM, OPT_USAGE_BLOOM);
    addOptWithArg(OPT_BLOOM_PARAM, "the parameter of bloom filter type");
    addOptWithArg(OPT_COMPRESSION, OPT_USAGE_COMPRESSION);
    addOptWithArg(HFileTestUtil.OPT_DATA_BLOCK_ENCODING, HFileTestUtil.OPT_DATA_BLOCK_ENCODING_USAGE);
    addOptWithArg(OPT_MAX_READ_ERRORS, "The maximum number of read errors " +
        "to tolerate before terminating all reader threads. The default is " +
        MultiThreadedReader.DEFAULT_MAX_ERRORS + ".");
    addOptWithArg(OPT_MULTIGET, "Whether to use multi-gets as opposed to " +
        "separate gets for every column in a row");
    addOptWithArg(OPT_KEY_WINDOW, "The 'key window' to maintain between " +
        "reads and writes for concurrent write/read workload. The default " +
        "is " + MultiThreadedReader.DEFAULT_KEY_WINDOW + ".");

    addOptNoArg(OPT_MULTIPUT, "Whether to use multi-puts as opposed to " +
        "separate puts for every column in a row");
    addOptNoArg(OPT_BATCHUPDATE, "Whether to use batch as opposed to " +
        "separate updates for every column in a row");
    addOptNoArg(OPT_INMEMORY, OPT_USAGE_IN_MEMORY);
    addOptWithArg(OPT_GENERATOR, OPT_GENERATOR_USAGE);
    addOptWithArg(OPT_WRITER, OPT_WRITER_USAGE);
    addOptWithArg(OPT_UPDATER, OPT_UPDATER_USAGE);
    addOptWithArg(OPT_READER, OPT_READER_USAGE);

    addOptWithArg(OPT_NUM_KEYS, "The number of keys to read/write");
    addOptWithArg(OPT_START_KEY, "The first key to read/write " +
        "(a 0-based index). The default value is " +
        DEFAULT_START_KEY + ".");
    addOptNoArg(OPT_SKIP_INIT, "Skip the initialization; assume test table "
        + "already exists");

    addOptWithArg(NUM_TABLES,
      "A positive integer number. When a number n is specified, load test "
          + "tool  will load n table parallely. -tn parameter value becomes "
          + "table name prefix. Each table name is in format <tn>_1...<tn>_n");

    addOptWithArg(OPT_ENCRYPTION, OPT_ENCRYPTION_USAGE);
    addOptNoArg(OPT_DEFERRED_LOG_FLUSH, OPT_DEFERRED_LOG_FLUSH_USAGE);
    addOptWithArg(OPT_NUM_REGIONS_PER_SERVER, OPT_NUM_REGIONS_PER_SERVER_USAGE);
    addOptWithArg(OPT_REGION_REPLICATION, OPT_REGION_REPLICATION_USAGE);
    addOptWithArg(OPT_REGION_REPLICA_ID, OPT_REGION_REPLICA_ID_USAGE);
    addOptWithArg(OPT_MOB_THRESHOLD, OPT_MOB_THRESHOLD_USAGE);
  }

  @Override
  protected CommandLineParser newParser() {
    // Commons-CLI lacks the capability to handle combinations of options, so we do it ourselves
    // Validate in parse() to get helpful error messages instead of exploding in processOptions()
    return new DefaultParser() {
      @Override
      public CommandLine parse(Options opts, String[] args, Properties props, boolean stop)
          throws ParseException {
        CommandLine cl = super.parse(opts, args, props, stop);

        boolean isReadWriteUpdate = cmd.hasOption(OPT_READ)
            || cmd.hasOption(OPT_WRITE)
            || cmd.hasOption(OPT_UPDATE);
        boolean isInitOnly = cmd.hasOption(OPT_INIT_ONLY);

        if (!isInitOnly && !isReadWriteUpdate) {
          throw new MissingOptionException("Must specify either -" + OPT_INIT_ONLY
              + " or at least one of -" + OPT_READ + ", -" + OPT_WRITE + ", -" + OPT_UPDATE);
        }

        if (isInitOnly && isReadWriteUpdate) {
          throw new AlreadySelectedException(OPT_INIT_ONLY + " cannot be specified with any of -"
              + OPT_READ + ", -" + OPT_WRITE + ", -" + OPT_UPDATE);
        }

        if (isReadWriteUpdate && !cmd.hasOption(OPT_NUM_KEYS)) {
          throw new MissingOptionException(OPT_NUM_KEYS + " must be specified in read/write mode.");
        }

        return cl;
      }
    };
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    this.cmd = cmd;

    tableName = TableName.valueOf(cmd.getOptionValue(OPT_TABLE_NAME,
        DEFAULT_TABLE_NAME));

    if (cmd.hasOption(OPT_COLUMN_FAMILIES)) {
      String[] list = cmd.getOptionValue(OPT_COLUMN_FAMILIES).split(",");
      families = new byte[list.length][];
      for (int i = 0; i < list.length; i++) {
        families[i] = Bytes.toBytes(list[i]);
      }
    } else {
      families = HFileTestUtil.DEFAULT_COLUMN_FAMILIES;
    }

    isVerbose = cmd.hasOption(OPT_VERBOSE);
    isWrite = cmd.hasOption(OPT_WRITE);
    isRead = cmd.hasOption(OPT_READ);
    isUpdate = cmd.hasOption(OPT_UPDATE);
    isInitOnly = cmd.hasOption(OPT_INIT_ONLY);
    deferredLogFlush = cmd.hasOption(OPT_DEFERRED_LOG_FLUSH);

    if (!isInitOnly) {
      startKey = parseLong(cmd.getOptionValue(OPT_START_KEY,
          String.valueOf(DEFAULT_START_KEY)), 0, Long.MAX_VALUE);
      long numKeys = parseLong(cmd.getOptionValue(OPT_NUM_KEYS), 1,
          Long.MAX_VALUE - startKey);
      endKey = startKey + numKeys;
      isSkipInit = cmd.hasOption(OPT_SKIP_INIT);
      System.out.println("Key range: [" + startKey + ".." + (endKey - 1) + "]");
    }

    parseColumnFamilyOptions(cmd);

    if (isWrite) {
      String[] writeOpts = splitColonSeparated(OPT_WRITE, 2, 3);

      int colIndex = 0;
      minColsPerKey = 1;
      maxColsPerKey = 2 * Integer.parseInt(writeOpts[colIndex++]);
      int avgColDataSize =
          parseInt(writeOpts[colIndex++], 1, Integer.MAX_VALUE);
      minColDataSize = avgColDataSize / 2;
      maxColDataSize = avgColDataSize * 3 / 2;

      if (colIndex < writeOpts.length) {
        numWriterThreads = getNumThreads(writeOpts[colIndex++]);
      }

      isMultiPut = cmd.hasOption(OPT_MULTIPUT);

      mobThreshold = -1;
      if (cmd.hasOption(OPT_MOB_THRESHOLD)) {
        mobThreshold = Integer.parseInt(cmd.getOptionValue(OPT_MOB_THRESHOLD));
      }

      System.out.println("Multi-puts: " + isMultiPut);
      System.out.println("Columns per key: " + minColsPerKey + ".."
          + maxColsPerKey);
      System.out.println("Data size per column: " + minColDataSize + ".."
          + maxColDataSize);
    }

    if (isUpdate) {
      String[] mutateOpts = splitColonSeparated(OPT_UPDATE, 1, 3);
      int colIndex = 0;
      updatePercent = parseInt(mutateOpts[colIndex++], 0, 100);
      if (colIndex < mutateOpts.length) {
        numUpdaterThreads = getNumThreads(mutateOpts[colIndex++]);
      }
      if (colIndex < mutateOpts.length) {
        ignoreConflicts = parseInt(mutateOpts[colIndex++], 0, 1) == 1;
      }

      isBatchUpdate = cmd.hasOption(OPT_BATCHUPDATE);

      System.out.println("Batch updates: " + isBatchUpdate);
      System.out.println("Percent of keys to update: " + updatePercent);
      System.out.println("Updater threads: " + numUpdaterThreads);
      System.out.println("Ignore nonce conflicts: " + ignoreConflicts);
    }

    if (isRead) {
      String[] readOpts = splitColonSeparated(OPT_READ, 1, 2);
      int colIndex = 0;
      verifyPercent = parseInt(readOpts[colIndex++], 0, 100);
      if (colIndex < readOpts.length) {
        numReaderThreads = getNumThreads(readOpts[colIndex++]);
      }

      if (cmd.hasOption(OPT_MAX_READ_ERRORS)) {
        maxReadErrors = parseInt(cmd.getOptionValue(OPT_MAX_READ_ERRORS),
            0, Integer.MAX_VALUE);
      }

      if (cmd.hasOption(OPT_KEY_WINDOW)) {
        keyWindow = parseInt(cmd.getOptionValue(OPT_KEY_WINDOW),
            0, Integer.MAX_VALUE);
      }

      if (cmd.hasOption(OPT_MULTIGET)) {
        multiGetBatchSize = parseInt(cmd.getOptionValue(OPT_MULTIGET),
            0, Integer.MAX_VALUE);
      }

      System.out.println("Multi-gets (value of 1 means no multigets): " + multiGetBatchSize);
      System.out.println("Percent of keys to verify: " + verifyPercent);
      System.out.println("Reader threads: " + numReaderThreads);
    }

    numTables = 1;
    if (cmd.hasOption(NUM_TABLES)) {
      numTables = parseInt(cmd.getOptionValue(NUM_TABLES), 1, Short.MAX_VALUE);
    }

    numRegionsPerServer = DEFAULT_NUM_REGIONS_PER_SERVER;
    if (cmd.hasOption(OPT_NUM_REGIONS_PER_SERVER)) {
      numRegionsPerServer = Integer.parseInt(cmd.getOptionValue(OPT_NUM_REGIONS_PER_SERVER));
    }

    regionReplication = 1;
    if (cmd.hasOption(OPT_REGION_REPLICATION)) {
      regionReplication = Integer.parseInt(cmd.getOptionValue(OPT_REGION_REPLICATION));
    }

    regionReplicaId = -1;
    if (cmd.hasOption(OPT_REGION_REPLICA_ID)) {
      regionReplicaId = Integer.parseInt(cmd.getOptionValue(OPT_REGION_REPLICA_ID));
    }
  }

  private void parseColumnFamilyOptions(CommandLine cmd) {
    String dataBlockEncodingStr = cmd.getOptionValue(HFileTestUtil.OPT_DATA_BLOCK_ENCODING);
    dataBlockEncodingAlgo = dataBlockEncodingStr == null ? null :
        DataBlockEncoding.valueOf(dataBlockEncodingStr);

    String compressStr = cmd.getOptionValue(OPT_COMPRESSION);
    compressAlgo = compressStr == null ? Compression.Algorithm.NONE :
        Compression.Algorithm.valueOf(compressStr);

    String bloomStr = cmd.getOptionValue(OPT_BLOOM);
    bloomType = bloomStr == null ? BloomType.ROW :
        BloomType.valueOf(bloomStr);

    if (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH) {
      if (!cmd.hasOption(OPT_BLOOM_PARAM)) {
        LOG.error("the parameter of bloom filter {} is not specified", bloomType.name());
      } else {
        conf.set(BloomFilterUtil.PREFIX_LENGTH_KEY, cmd.getOptionValue(OPT_BLOOM_PARAM));
      }
    }

    inMemoryCF = cmd.hasOption(OPT_INMEMORY);
    if (cmd.hasOption(OPT_ENCRYPTION)) {
      cipher = Encryption.getCipher(conf, cmd.getOptionValue(OPT_ENCRYPTION));
    }

  }

  public void initTestTable() throws IOException {
    Durability durability = Durability.USE_DEFAULT;
    if (deferredLogFlush) {
      durability = Durability.ASYNC_WAL;
    }

    HBaseTestingUtil.createPreSplitLoadTestTable(conf, tableName,
      getColumnFamilies(), compressAlgo, dataBlockEncodingAlgo, numRegionsPerServer,
        regionReplication, durability);
    applyColumnFamilyOptions(tableName, getColumnFamilies());
  }

  @Override
  protected int doWork() throws IOException {
    if (!isVerbose) {
      Log4jUtils.setLogLevel(ZooKeeper.class.getName(), "WARN");
    }
    if (numTables > 1) {
      return parallelLoadTables();
    } else {
      return loadTable();
    }
  }

  protected int loadTable() throws IOException {
    if (cmd.hasOption(OPT_ZK_QUORUM)) {
      conf.set(HConstants.ZOOKEEPER_QUORUM, cmd.getOptionValue(OPT_ZK_QUORUM));
    }
    if (cmd.hasOption(OPT_ZK_PARENT_NODE)) {
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, cmd.getOptionValue(OPT_ZK_PARENT_NODE));
    }

    if (isInitOnly) {
      LOG.info("Initializing only; no reads or writes");
      initTestTable();
      return 0;
    }

    if (!isSkipInit) {
      initTestTable();
    }
    LoadTestDataGenerator dataGen = null;
    if (cmd.hasOption(OPT_GENERATOR)) {
      String[] clazzAndArgs = cmd.getOptionValue(OPT_GENERATOR).split(COLON);
      dataGen = getLoadGeneratorInstance(clazzAndArgs[0]);
      String[] args;
      if (dataGen instanceof LoadTestDataGeneratorWithACL) {
        LOG.info("Using LoadTestDataGeneratorWithACL");
        if (User.isHBaseSecurityEnabled(conf)) {
          LOG.info("Security is enabled");
          authnFileName = clazzAndArgs[1];
          superUser = clazzAndArgs[2];
          userNames = clazzAndArgs[3];
          args = Arrays.copyOfRange(clazzAndArgs, 2, clazzAndArgs.length);
          Properties authConfig = new Properties();
          authConfig.load(this.getClass().getClassLoader().getResourceAsStream(authnFileName));
          try {
            addAuthInfoToConf(authConfig, conf, superUser, userNames);
          } catch (IOException exp) {
            LOG.error(exp.toString(), exp);
            return EXIT_FAILURE;
          }
          userOwner = User.create(HBaseKerberosUtils.loginAndReturnUGI(conf, superUser));
        } else {
          superUser = clazzAndArgs[1];
          userNames = clazzAndArgs[2];
          args = Arrays.copyOfRange(clazzAndArgs, 1, clazzAndArgs.length);
          userOwner = User.createUserForTesting(conf, superUser, new String[0]);
        }
      } else {
        args = clazzAndArgs.length == 1 ? new String[0] : Arrays.copyOfRange(clazzAndArgs, 1,
            clazzAndArgs.length);
      }
      dataGen.initialize(args);
    } else {
      // Default DataGenerator is MultiThreadedAction.DefaultDataGenerator
      dataGen = new MultiThreadedAction.DefaultDataGenerator(minColDataSize, maxColDataSize,
          minColsPerKey, maxColsPerKey, families);
    }

    if (userOwner != null) {
      LOG.info("Granting permissions for user " + userOwner.getShortName());
      Permission.Action[] actions = {
        Permission.Action.ADMIN, Permission.Action.CREATE,
        Permission.Action.READ, Permission.Action.WRITE };
      try {
        AccessControlClient.grant(ConnectionFactory.createConnection(conf),
            tableName, userOwner.getShortName(), null, null, actions);
      } catch (Throwable e) {
        LOG.error(HBaseMarkers.FATAL, "Error in granting permission for the user " +
            userOwner.getShortName(), e);
        return EXIT_FAILURE;
      }
    }

    if (userNames != null) {
      // This will be comma separated list of expressions.
      String users[] = userNames.split(",");
      User user = null;
      for (String userStr : users) {
        if (User.isHBaseSecurityEnabled(conf)) {
          user = User.create(HBaseKerberosUtils.loginAndReturnUGI(conf, userStr));
        } else {
          user = User.createUserForTesting(conf, userStr, new String[0]);
        }
      }
    }

    if (isWrite) {
      if (userOwner != null) {
        writerThreads = new MultiThreadedWriterWithACL(dataGen, conf, tableName, userOwner);
      } else {
        String writerClass = null;
        if (cmd.hasOption(OPT_WRITER)) {
          writerClass = cmd.getOptionValue(OPT_WRITER);
        } else {
          writerClass = MultiThreadedWriter.class.getCanonicalName();
        }

        writerThreads = getMultiThreadedWriterInstance(writerClass, dataGen);
      }
      writerThreads.setMultiPut(isMultiPut);
    }

    if (isUpdate) {
      if (userOwner != null) {
        updaterThreads = new MultiThreadedUpdaterWithACL(dataGen, conf, tableName, updatePercent,
            userOwner, userNames);
      } else {
        String updaterClass = null;
        if (cmd.hasOption(OPT_UPDATER)) {
          updaterClass = cmd.getOptionValue(OPT_UPDATER);
        } else {
          updaterClass = MultiThreadedUpdater.class.getCanonicalName();
        }
        updaterThreads = getMultiThreadedUpdaterInstance(updaterClass, dataGen);
      }
      updaterThreads.setBatchUpdate(isBatchUpdate);
      updaterThreads.setIgnoreNonceConflicts(ignoreConflicts);
    }

    if (isRead) {
      if (userOwner != null) {
        readerThreads = new MultiThreadedReaderWithACL(dataGen, conf, tableName, verifyPercent,
            userNames);
      } else {
        String readerClass = null;
        if (cmd.hasOption(OPT_READER)) {
          readerClass = cmd.getOptionValue(OPT_READER);
        } else {
          readerClass = MultiThreadedReader.class.getCanonicalName();
        }
        readerThreads = getMultiThreadedReaderInstance(readerClass, dataGen);
      }
      readerThreads.setMaxErrors(maxReadErrors);
      readerThreads.setKeyWindow(keyWindow);
      readerThreads.setMultiGetBatchSize(multiGetBatchSize);
      readerThreads.setRegionReplicaId(regionReplicaId);
    }

    if (isUpdate && isWrite) {
      LOG.info("Concurrent write/update workload: making updaters aware of the " +
        "write point");
      updaterThreads.linkToWriter(writerThreads);
    }

    if (isRead && (isUpdate || isWrite)) {
      LOG.info("Concurrent write/read workload: making readers aware of the " +
        "write point");
      readerThreads.linkToWriter(isUpdate ? updaterThreads : writerThreads);
    }

    if (isWrite) {
      System.out.println("Starting to write data...");
      writerThreads.start(startKey, endKey, numWriterThreads);
    }

    if (isUpdate) {
      LOG.info("Starting to mutate data...");
      System.out.println("Starting to mutate data...");
      // TODO : currently append and increment operations not tested with tags
      // Will update this after it is done
      updaterThreads.start(startKey, endKey, numUpdaterThreads);
    }

    if (isRead) {
      System.out.println("Starting to read data...");
      readerThreads.start(startKey, endKey, numReaderThreads);
    }

    if (isWrite) {
      writerThreads.waitForFinish();
    }

    if (isUpdate) {
      updaterThreads.waitForFinish();
    }

    if (isRead) {
      readerThreads.waitForFinish();
    }

    boolean success = true;
    if (isWrite) {
      success = success && writerThreads.getNumWriteFailures() == 0;
    }
    if (isUpdate) {
      success = success && updaterThreads.getNumWriteFailures() == 0;
    }
    if (isRead) {
      success = success && readerThreads.getNumReadErrors() == 0
          && readerThreads.getNumReadFailures() == 0;
    }
    return success ? EXIT_SUCCESS : EXIT_FAILURE;
  }

  private LoadTestDataGenerator getLoadGeneratorInstance(String clazzName) throws IOException {
    try {
      Class<?> clazz = Class.forName(clazzName);
      Constructor<?> constructor = clazz.getConstructor(int.class, int.class, int.class, int.class,
          byte[][].class);
      return (LoadTestDataGenerator) constructor.newInstance(minColDataSize, maxColDataSize,
          minColsPerKey, maxColsPerKey, families);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private MultiThreadedWriter getMultiThreadedWriterInstance(String clazzName
      , LoadTestDataGenerator dataGen) throws IOException {
    try {
      Class<?> clazz = Class.forName(clazzName);
      Constructor<?> constructor = clazz.getConstructor(
        LoadTestDataGenerator.class, Configuration.class, TableName.class);
      return (MultiThreadedWriter) constructor.newInstance(dataGen, conf, tableName);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private MultiThreadedUpdater getMultiThreadedUpdaterInstance(String clazzName
      , LoadTestDataGenerator dataGen) throws IOException {
    try {
      Class<?> clazz = Class.forName(clazzName);
      Constructor<?> constructor = clazz.getConstructor(
        LoadTestDataGenerator.class, Configuration.class, TableName.class, double.class);
      return (MultiThreadedUpdater) constructor.newInstance(
        dataGen, conf, tableName, updatePercent);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private MultiThreadedReader getMultiThreadedReaderInstance(String clazzName
      , LoadTestDataGenerator dataGen) throws IOException {
    try {
      Class<?> clazz = Class.forName(clazzName);
      Constructor<?> constructor = clazz.getConstructor(
        LoadTestDataGenerator.class, Configuration.class, TableName.class, double.class);
      return (MultiThreadedReader) constructor.newInstance(dataGen, conf, tableName, verifyPercent);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void main(String[] args) {
    new LoadTestTool().doStaticMain(args);
  }

  /**
   * When NUM_TABLES is specified, the function starts multiple worker threads
   * which individually start a LoadTestTool instance to load a table. Each
   * table name is in format &lt;tn>_&lt;index>. For example, "-tn test -num_tables 2"
   * , table names will be "test_1", "test_2"
   *
   * @throws IOException if one of the load tasks is unable to complete
   */
  private int parallelLoadTables()
      throws IOException {
    // create new command args
    String tableName = cmd.getOptionValue(OPT_TABLE_NAME, DEFAULT_TABLE_NAME);
    String[] newArgs = null;
    if (!cmd.hasOption(LoadTestTool.OPT_TABLE_NAME)) {
      newArgs = new String[cmdLineArgs.length + 2];
      newArgs[0] = "-" + LoadTestTool.OPT_TABLE_NAME;
      newArgs[1] = LoadTestTool.DEFAULT_TABLE_NAME;
      System.arraycopy(cmdLineArgs, 0, newArgs, 2, cmdLineArgs.length);
    } else {
      newArgs = cmdLineArgs;
    }

    int tableNameValueIndex = -1;
    for (int j = 0; j < newArgs.length; j++) {
      if (newArgs[j].endsWith(OPT_TABLE_NAME)) {
        tableNameValueIndex = j + 1;
      } else if (newArgs[j].endsWith(NUM_TABLES)) {
        // change NUM_TABLES to 1 so that each worker loads one table
        newArgs[j + 1] = "1";
      }
    }

    // starting to load multiple tables
    List<WorkerThread> workers = new ArrayList<>();
    for (int i = 0; i < numTables; i++) {
      String[] workerArgs = newArgs.clone();
      workerArgs[tableNameValueIndex] = tableName + "_" + (i+1);
      WorkerThread worker = new WorkerThread(i, workerArgs);
      workers.add(worker);
      LOG.info(worker + " starting");
      worker.start();
    }

    // wait for all workers finish
    LOG.info("Waiting for worker threads to finish");
    for (WorkerThread t : workers) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        IOException iie = new InterruptedIOException();
        iie.initCause(ie);
        throw iie;
      }
      checkForErrors();
    }

    return EXIT_SUCCESS;
  }

  // If an exception is thrown by one of worker threads, it will be
  // stored here.
  protected AtomicReference<Throwable> thrown = new AtomicReference<>();

  private void workerThreadError(Throwable t) {
    thrown.compareAndSet(null, t);
  }

  /**
   * Check for errors in the writer threads. If any is found, rethrow it.
   */
  private void checkForErrors() throws IOException {
    Throwable thrown = this.thrown.get();
    if (thrown == null) return;
    if (thrown instanceof IOException) {
      throw (IOException) thrown;
    } else {
      throw new RuntimeException(thrown);
    }
  }

  class WorkerThread extends Thread {
    private String[] workerArgs;

    WorkerThread(int i, String[] args) {
      super("WorkerThread-" + i);
      workerArgs = args;
    }

    @Override
    public void run() {
      try {
        int ret = ToolRunner.run(HBaseConfiguration.create(), new LoadTestTool(), workerArgs);
        if (ret != 0) {
          throw new RuntimeException("LoadTestTool exit with non-zero return code.");
        }
      } catch (Exception ex) {
        LOG.error("Error in worker thread", ex);
        workerThreadError(ex);
      }
    }
  }

  private void addAuthInfoToConf(Properties authConfig, Configuration conf, String owner,
      String userList) throws IOException {
    List<String> users = new ArrayList<>(Arrays.asList(userList.split(",")));
    users.add(owner);
    for (String user : users) {
      String keyTabFileConfKey = "hbase." + user + ".keytab.file";
      String principalConfKey = "hbase." + user + ".kerberos.principal";
      if (!authConfig.containsKey(keyTabFileConfKey) || !authConfig.containsKey(principalConfKey)) {
        throw new IOException("Authentication configs missing for user : " + user);
      }
    }
    for (String key : authConfig.stringPropertyNames()) {
      conf.set(key, authConfig.getProperty(key));
    }
    LOG.debug("Added authentication properties to config successfully.");
  }

}
