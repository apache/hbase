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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.hbase.util.test.LoadTestDataGeneratorWithACL;
import org.apache.hadoop.util.ToolRunner;

/**
 * A command-line utility that reads, writes, and verifies data. Unlike
 * {@link PerformanceEvaluation}, this tool validates the data written,
 * and supports simultaneously writing and reading the same set of keys.
 */
public class LoadTestTool extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(LoadTestTool.class);
  private static final String COLON = ":";

  /** Table name for the test */
  private TableName tableName;

  /** Table name to use of not overridden on the command line */
  protected static final String DEFAULT_TABLE_NAME = "cluster_test";

  /** Column family used by the test */
  public static byte[] COLUMN_FAMILY = Bytes.toBytes("test_cf");

  /** Column families used by the test */
  protected static final byte[][] COLUMN_FAMILIES = { COLUMN_FAMILY };

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
      "<update_percent>[:<#threads=" + DEFAULT_NUM_THREADS + ">]";

  protected static final String OPT_USAGE_BLOOM = "Bloom filter type, one of " +
      Arrays.toString(BloomType.values());

  protected static final String OPT_USAGE_COMPRESSION = "Compression type, " +
      "one of " + Arrays.toString(Compression.Algorithm.values());

  public static final String OPT_DATA_BLOCK_ENCODING_USAGE =
    "Encoding algorithm (e.g. prefix "
        + "compression) to use for data blocks in the test column family, "
        + "one of " + Arrays.toString(DataBlockEncoding.values()) + ".";

  private static final String OPT_BLOOM = "bloom";
  private static final String OPT_COMPRESSION = "compression";
  public static final String OPT_DATA_BLOCK_ENCODING =
      HColumnDescriptor.DATA_BLOCK_ENCODING.toLowerCase();

  public static final String OPT_INMEMORY = "in_memory";
  public static final String OPT_USAGE_IN_MEMORY = "Tries to keep the HFiles of the CF " +
  		"inmemory as far as possible.  Not guaranteed that reads are always served from inmemory";

  public static final String OPT_GENERATOR = "generator";
  public static final String OPT_GENERATOR_USAGE = "The class which generates load for the tool."
      + " Any args for this class can be passed as colon separated after class name";

  protected static final String OPT_KEY_WINDOW = "key_window";
  protected static final String OPT_WRITE = "write";
  protected static final String OPT_MAX_READ_ERRORS = "max_read_errors";
  protected static final String OPT_MULTIPUT = "multiput";
  protected static final String OPT_NUM_KEYS = "num_keys";
  protected static final String OPT_READ = "read";
  protected static final String OPT_START_KEY = "start_key";
  protected static final String OPT_TABLE_NAME = "tn";
  protected static final String OPT_ZK_QUORUM = "zk";
  protected static final String OPT_ZK_PARENT_NODE = "zk_root";
  protected static final String OPT_SKIP_INIT = "skip_init";
  protected static final String OPT_INIT_ONLY = "init_only";
  private static final String NUM_TABLES = "num_tables";
  protected static final String OPT_BATCHUPDATE = "batchupdate";
  protected static final String OPT_UPDATE = "update";

  protected static final String OPT_ENCRYPTION = "encryption";
  protected static final String OPT_ENCRYPTION_USAGE =
    "Enables transparent encryption on the test table, one of " +
    Arrays.toString(Encryption.getSupportedCiphers());

  protected static final long DEFAULT_START_KEY = 0;

  /** This will be removed as we factor out the dependency on command line */
  protected CommandLine cmd;

  protected MultiThreadedWriter writerThreads = null;
  protected MultiThreadedReader readerThreads = null;
  protected MultiThreadedUpdater updaterThreads = null;

  protected long startKey, endKey;

  protected boolean isWrite, isRead, isUpdate;

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
  protected boolean isBatchUpdate;

  // Reader options
  private int numReaderThreads = DEFAULT_NUM_THREADS;
  private int keyWindow = MultiThreadedReader.DEFAULT_KEY_WINDOW;
  private int maxReadErrors = MultiThreadedReader.DEFAULT_MAX_ERRORS;
  private int verifyPercent;

  private int numTables = 1;

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

  /**
   * Apply column family options such as Bloom filters, compression, and data
   * block encoding.
   */
  protected void applyColumnFamilyOptions(TableName tableName,
      byte[][] columnFamilies) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
    LOG.info("Disabling table " + tableName);
    admin.disableTable(tableName);
    for (byte[] cf : columnFamilies) {
      HColumnDescriptor columnDesc = tableDesc.getFamily(cf);
      boolean isNewCf = columnDesc == null;
      if (isNewCf) {
        columnDesc = new HColumnDescriptor(cf);
      }
      if (bloomType != null) {
        columnDesc.setBloomFilterType(bloomType);
      }
      if (compressAlgo != null) {
        columnDesc.setCompressionType(compressAlgo);
      }
      if (dataBlockEncodingAlgo != null) {
        columnDesc.setDataBlockEncoding(dataBlockEncodingAlgo);
      }
      if (inMemoryCF) {
        columnDesc.setInMemory(inMemoryCF);
      }
      if (cipher != null) {
        byte[] keyBytes = new byte[cipher.getKeyLength()];
        new SecureRandom().nextBytes(keyBytes);
        columnDesc.setEncryptionType(cipher.getName());
        columnDesc.setEncryptionKey(EncryptionUtil.wrapKey(conf,
          User.getCurrent().getShortName(),
          new SecretKeySpec(keyBytes, cipher.getName())));
      }
      if (isNewCf) {
        admin.addColumn(tableName, columnDesc);
      } else {
        admin.modifyColumn(tableName, columnDesc);
      }
    }
    LOG.info("Enabling table " + tableName);
    admin.enableTable(tableName);
  }

  @Override
  protected void addOptions() {
    addOptWithArg(OPT_ZK_QUORUM, "ZK quorum as comma-separated host names " +
        "without port numbers");
    addOptWithArg(OPT_ZK_PARENT_NODE, "name of parent znode in zookeeper");
    addOptWithArg(OPT_TABLE_NAME, "The name of the table to read or write");
    addOptWithArg(OPT_WRITE, OPT_USAGE_LOAD);
    addOptWithArg(OPT_READ, OPT_USAGE_READ);
    addOptWithArg(OPT_UPDATE, OPT_USAGE_UPDATE);
    addOptNoArg(OPT_INIT_ONLY, "Initialize the test table only, don't do any loading");
    addOptWithArg(OPT_BLOOM, OPT_USAGE_BLOOM);
    addOptWithArg(OPT_COMPRESSION, OPT_USAGE_COMPRESSION);
    addOptWithArg(OPT_DATA_BLOCK_ENCODING, OPT_DATA_BLOCK_ENCODING_USAGE);
    addOptWithArg(OPT_MAX_READ_ERRORS, "The maximum number of read errors " +
        "to tolerate before terminating all reader threads. The default is " +
        MultiThreadedReader.DEFAULT_MAX_ERRORS + ".");
    addOptWithArg(OPT_KEY_WINDOW, "The 'key window' to maintain between " +
        "reads and writes for concurrent write/read workload. The default " +
        "is " + MultiThreadedReader.DEFAULT_KEY_WINDOW + ".");

    addOptNoArg(OPT_MULTIPUT, "Whether to use multi-puts as opposed to " +
        "separate puts for every column in a row");
    addOptNoArg(OPT_BATCHUPDATE, "Whether to use batch as opposed to " +
        "separate updates for every column in a row");
    addOptNoArg(OPT_INMEMORY, OPT_USAGE_IN_MEMORY);
    addOptWithArg(OPT_GENERATOR, OPT_GENERATOR_USAGE);

    addOptWithArg(OPT_NUM_KEYS, "The number of keys to read/write");
    addOptWithArg(OPT_START_KEY, "The first key to read/write " +
        "(a 0-based index). The default value is " +
        DEFAULT_START_KEY + ".");
    addOptNoArg(OPT_SKIP_INIT, "Skip the initialization; assume test table "
        + "already exists");

    addOptWithArg(NUM_TABLES,
      "A positive integer number. When a number n is speicfied, load test "
          + "tool  will load n table parallely. -tn parameter value becomes "
          + "table name prefix. Each table name is in format <tn>_1...<tn>_n");

    addOptWithArg(OPT_ENCRYPTION, OPT_ENCRYPTION_USAGE);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    this.cmd = cmd;

    tableName = TableName.valueOf(cmd.getOptionValue(OPT_TABLE_NAME,
        DEFAULT_TABLE_NAME));

    isWrite = cmd.hasOption(OPT_WRITE);
    isRead = cmd.hasOption(OPT_READ);
    isUpdate = cmd.hasOption(OPT_UPDATE);
    isInitOnly = cmd.hasOption(OPT_INIT_ONLY);

    if (!isWrite && !isRead && !isUpdate && !isInitOnly) {
      throw new IllegalArgumentException("Either -" + OPT_WRITE + " or " +
        "-" + OPT_UPDATE + "-" + OPT_READ + " has to be specified");
    }

    if (isInitOnly && (isRead || isWrite || isUpdate)) {
      throw new IllegalArgumentException(OPT_INIT_ONLY + " cannot be specified with"
          + " either -" + OPT_WRITE + " or -" + OPT_UPDATE + " or -" + OPT_READ);
    }

    if (!isInitOnly) {
      if (!cmd.hasOption(OPT_NUM_KEYS)) {
        throw new IllegalArgumentException(OPT_NUM_KEYS + " must be specified in "
            + "read or write mode");
      }
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

      System.out.println("Multi-puts: " + isMultiPut);
      System.out.println("Columns per key: " + minColsPerKey + ".."
          + maxColsPerKey);
      System.out.println("Data size per column: " + minColDataSize + ".."
          + maxColDataSize);
    }

    if (isUpdate) {
      String[] mutateOpts = splitColonSeparated(OPT_UPDATE, 1, 2);
      int colIndex = 0;
      updatePercent = parseInt(mutateOpts[colIndex++], 0, 100);
      if (colIndex < mutateOpts.length) {
        numUpdaterThreads = getNumThreads(mutateOpts[colIndex++]);
      }

      isBatchUpdate = cmd.hasOption(OPT_BATCHUPDATE);

      System.out.println("Batch updates: " + isBatchUpdate);
      System.out.println("Percent of keys to update: " + updatePercent);
      System.out.println("Updater threads: " + numUpdaterThreads);
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

      System.out.println("Percent of keys to verify: " + verifyPercent);
      System.out.println("Reader threads: " + numReaderThreads);
    }

    numTables = 1;
    if(cmd.hasOption(NUM_TABLES)) {
      numTables = parseInt(cmd.getOptionValue(NUM_TABLES), 1, Short.MAX_VALUE);
    }
  }

  private void parseColumnFamilyOptions(CommandLine cmd) {
    String dataBlockEncodingStr = cmd.getOptionValue(OPT_DATA_BLOCK_ENCODING);
    dataBlockEncodingAlgo = dataBlockEncodingStr == null ? null :
        DataBlockEncoding.valueOf(dataBlockEncodingStr);

    String compressStr = cmd.getOptionValue(OPT_COMPRESSION);
    compressAlgo = compressStr == null ? Compression.Algorithm.NONE :
        Compression.Algorithm.valueOf(compressStr);

    String bloomStr = cmd.getOptionValue(OPT_BLOOM);
    bloomType = bloomStr == null ? null :
        BloomType.valueOf(bloomStr);

    inMemoryCF = cmd.hasOption(OPT_INMEMORY);
    if (cmd.hasOption(OPT_ENCRYPTION)) {
      cipher = Encryption.getCipher(conf, cmd.getOptionValue(OPT_ENCRYPTION));
    }
  }

  public void initTestTable() throws IOException {
    HBaseTestingUtility.createPreSplitLoadTestTable(conf, tableName,
        COLUMN_FAMILY, compressAlgo, dataBlockEncodingAlgo);
    applyColumnFamilyOptions(tableName, COLUMN_FAMILIES);
  }

  @Override
  protected int doWork() throws IOException {
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
      if(dataGen instanceof LoadTestDataGeneratorWithACL) {
        LOG.info("ACL is on");
        userOwner = User.createUserForTesting(conf, "owner", new String[0]);
      }
      String[] args = clazzAndArgs.length == 1 ? new String[0] : Arrays.copyOfRange(clazzAndArgs,
          1, clazzAndArgs.length);
      dataGen.initialize(args);
    } else {
      // Default DataGenerator is MultiThreadedAction.DefaultDataGenerator
      dataGen = new MultiThreadedAction.DefaultDataGenerator(minColDataSize, maxColDataSize,
          minColsPerKey, maxColsPerKey, COLUMN_FAMILY);
    }

    if(userOwner != null) {
      conf.set("hadoop.security.authorization", "false");
      conf.set("hadoop.security.authentication", "simple");
      LOG.info("Granting permission for the user " + userOwner.getShortName());
      HTable table = new HTable(conf, tableName);
      AccessControlProtos.Permission.Action[] actions = {
          AccessControlProtos.Permission.Action.ADMIN,
          AccessControlProtos.Permission.Action.CREATE, AccessControlProtos.Permission.Action.READ,
          AccessControlProtos.Permission.Action.WRITE };

      try {
        AccessControlClient.grant(conf, table.getName(), userOwner.getShortName(), COLUMN_FAMILY,
            null, actions);
      } catch (Throwable e) {
        LOG.fatal("Error in granting permission for the user " + userOwner.getShortName(), e);
        return EXIT_FAILURE;
      }
    }

    if (isWrite) {
      if (userOwner != null) {
        writerThreads = new MultiThreadedWriterWithACL(dataGen, conf, tableName, userOwner);
      } else {
        writerThreads = new MultiThreadedWriter(dataGen, conf, tableName);
      }
      writerThreads.setMultiPut(isMultiPut);
    }

    if (isUpdate) {
      if (userOwner != null) {
        updaterThreads = new MultiThreadedUpdaterWithACL(dataGen, conf, tableName, updatePercent,
            userOwner);
      } else {
        updaterThreads = new MultiThreadedUpdater(dataGen, conf, tableName, updatePercent);
      }
      updaterThreads.setBatchUpdate(isBatchUpdate);
    }

    if (isRead) {
      if (userOwner != null) {
        readerThreads = new MultiThreadedReaderWithACL(dataGen, conf, tableName, verifyPercent);
      } else {
        readerThreads = new MultiThreadedReader(dataGen, conf, tableName, verifyPercent);
      }
      readerThreads.setMaxErrors(maxReadErrors);
      readerThreads.setKeyWindow(keyWindow);
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
      // Will update this aftet it is done
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
          minColsPerKey, maxColsPerKey, COLUMN_FAMILIES);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static byte[] generateData(final Random r, int length) {
    byte [] b = new byte [length];
    int i = 0;

    for(i = 0; i < (length-8); i += 8) {
      b[i] = (byte) (65 + r.nextInt(26));
      b[i+1] = b[i];
      b[i+2] = b[i];
      b[i+3] = b[i];
      b[i+4] = b[i];
      b[i+5] = b[i];
      b[i+6] = b[i];
      b[i+7] = b[i];
    }

    byte a = (byte) (65 + r.nextInt(26));
    for(; i < length; i++) {
      b[i] = a;
    }
    return b;
  }
  public static void main(String[] args) {
    new LoadTestTool().doStaticMain(args);
  }

  /**
   * When NUM_TABLES is specified, the function starts multiple worker threads
   * which individually start a LoadTestTool instance to load a table. Each
   * table name is in format <tn>_<index>. For example, "-tn test -num_tables 2"
   * , table names will be "test_1", "test_2"
   *
   * @throws IOException
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
      for (int i = 0; i < cmdLineArgs.length; i++) {
        newArgs[i + 2] = cmdLineArgs[i];
      }
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
    List<WorkerThread> workers = new ArrayList<WorkerThread>();
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
  protected AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

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
}
