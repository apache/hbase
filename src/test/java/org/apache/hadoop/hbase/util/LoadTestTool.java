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
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * A command-line utility that reads, writes, and verifies data. Unlike
 * {@link PerformanceEvaluation}, this tool validates the data written,
 * and supports simultaneously writing and reading the same set of keys.
 */
public class LoadTestTool extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(LoadTestTool.class);

  /** Table name for the test */
  private byte[] tableName;

  /** Table name to use of not overridden on the command line */
  private static final String DEFAULT_TABLE_NAME = "cluster_test";

  /** Column family used by the test */
  static byte[] COLUMN_FAMILY = Bytes.toBytes("test_cf");

  /** Column families used by the test */
  static final byte[][] COLUMN_FAMILIES = { COLUMN_FAMILY };

  /** The number of reader/writer threads if not specified */
  private static final int DEFAULT_NUM_THREADS = 20;

  /** Usage string for the load option */
  private static final String OPT_USAGE_LOAD =
      "<avg_cols_per_key>:<avg_data_size>" +
      "[:<#threads=" + DEFAULT_NUM_THREADS + ">]";

  /** Usa\ge string for the read option */
  private static final String OPT_USAGE_READ =
      "<verify_percent>[:<#threads=" + DEFAULT_NUM_THREADS + ">]";

  private static final String OPT_USAGE_BLOOM = "Bloom filter type, one of " +
      Arrays.toString(StoreFile.BloomType.values());

  private static final String OPT_USAGE_COMPRESSION = "Compression type, " +
      "one of " + Arrays.toString(Compression.Algorithm.values());

  public static final String OPT_DATA_BLOCK_ENCODING_USAGE =
    "Encoding algorithm (e.g. prefix "
        + "compression) to use for data blocks in the test column family, "
        + "one of " + Arrays.toString(DataBlockEncoding.values()) + ".";

  private static final String OPT_BLOOM = "bloom";
  private static final String OPT_COMPRESSION = "compression";
  public static final String OPT_DATA_BLOCK_ENCODING =
      HColumnDescriptor.DATA_BLOCK_ENCODING.toLowerCase();
  public static final String OPT_ENCODE_IN_CACHE_ONLY =
      "encode_in_cache_only";
  public static final String OPT_ENCODE_IN_CACHE_ONLY_USAGE =
      "If this is specified, data blocks will only be encoded in block " +
      "cache but not on disk";

  private static final String OPT_KEY_WINDOW = "key_window";
  private static final String OPT_WRITE = "write";
  private static final String OPT_MAX_READ_ERRORS = "max_read_errors";
  private static final String OPT_MULTIPUT = "multiput";
  private static final String OPT_NUM_KEYS = "num_keys";
  private static final String OPT_READ = "read";
  private static final String OPT_START_KEY = "start_key";
  private static final String OPT_TABLE_NAME = "tn";
  private static final String OPT_ZK_QUORUM = "zk";
  private static final String OPT_SKIP_INIT = "skip_init";
  private static final String OPT_INIT_ONLY = "init_only";

  private static final long DEFAULT_START_KEY = 0;

  /** This will be removed as we factor out the dependency on command line */
  private CommandLine cmd;

  private MultiThreadedWriter writerThreads = null;
  private MultiThreadedReader readerThreads = null;

  private long startKey, endKey;

  private boolean isWrite, isRead;

  // Column family options
  private DataBlockEncoding dataBlockEncodingAlgo;
  private boolean encodeInCacheOnly;
  private Compression.Algorithm compressAlgo;
  private StoreFile.BloomType bloomType;

  // Writer options
  private int numWriterThreads = DEFAULT_NUM_THREADS;
  private long minColsPerKey, maxColsPerKey;
  private int minColDataSize, maxColDataSize;
  private boolean isMultiPut;

  // Reader options
  private int numReaderThreads = DEFAULT_NUM_THREADS;
  private int keyWindow = MultiThreadedReader.DEFAULT_KEY_WINDOW;
  private int maxReadErrors = MultiThreadedReader.DEFAULT_MAX_ERRORS;
  private int verifyPercent;

  // TODO: refactor LoadTestToolImpl somewhere to make the usage from tests less bad,
  //       console tool itself should only be used from console.
  private boolean isSkipInit = false;
  private boolean isInitOnly = false;

  private String[] splitColonSeparated(String option,
      int minNumCols, int maxNumCols) {
    String optVal = cmd.getOptionValue(option);
    String[] cols = optVal.split(":");
    if (cols.length < minNumCols || cols.length > maxNumCols) {
      throw new IllegalArgumentException("Expected at least "
          + minNumCols + " columns but no more than " + maxNumCols +
          " in the colon-separated value '" + optVal + "' of the " +
          "-" + option + " option");
    }
    return cols;
  }

  private int getNumThreads(String numThreadsStr) {
    return parseInt(numThreadsStr, 1, Short.MAX_VALUE);
  }

  /**
   * Apply column family options such as Bloom filters, compression, and data
   * block encoding.
   */
  private void applyColumnFamilyOptions(byte[] tableName,
      byte[][] columnFamilies) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
    LOG.info("Disabling table " + Bytes.toString(tableName));
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
        columnDesc.setEncodeOnDisk(!encodeInCacheOnly);
      }
      if (isNewCf) {
        admin.addColumn(tableName, columnDesc);
      } else {
        admin.modifyColumn(tableName, columnDesc);
      }
    }
    LOG.info("Enabling table " + Bytes.toString(tableName));
    admin.enableTable(tableName);
  }

  @Override
  protected void addOptions() {
    addOptWithArg(OPT_ZK_QUORUM, "ZK quorum as comma-separated host names " +
        "without port numbers");
    addOptWithArg(OPT_TABLE_NAME, "The name of the table to read or write");
    addOptWithArg(OPT_WRITE, OPT_USAGE_LOAD);
    addOptWithArg(OPT_READ, OPT_USAGE_READ);
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
    addOptNoArg(OPT_ENCODE_IN_CACHE_ONLY, OPT_ENCODE_IN_CACHE_ONLY_USAGE);

    addOptWithArg(OPT_NUM_KEYS, "The number of keys to read/write");
    addOptWithArg(OPT_START_KEY, "The first key to read/write " +
        "(a 0-based index). The default value is " +
        DEFAULT_START_KEY + ".");
    addOptNoArg(OPT_SKIP_INIT, "Skip the initialization; assume test table "
        + "already exists");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    this.cmd = cmd;

    tableName = Bytes.toBytes(cmd.getOptionValue(OPT_TABLE_NAME,
        DEFAULT_TABLE_NAME));

    isWrite = cmd.hasOption(OPT_WRITE);
    isRead = cmd.hasOption(OPT_READ);
    isInitOnly = cmd.hasOption(OPT_INIT_ONLY);

    if (!isWrite && !isRead && !isInitOnly) {
      throw new IllegalArgumentException("Either -" + OPT_WRITE + " or " +
          "-" + OPT_READ + " has to be specified");
    }

    if (isInitOnly && (isRead || isWrite)) {
      throw new IllegalArgumentException(OPT_INIT_ONLY + " cannot be specified with"
          + " either -" + OPT_WRITE + " or -" + OPT_READ);
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

    encodeInCacheOnly = cmd.hasOption(OPT_ENCODE_IN_CACHE_ONLY);
    parseColumnFamilyOptions(cmd);

    if (isWrite) {
      String[] writeOpts = splitColonSeparated(OPT_WRITE, 2, 3);

      int colIndex = 0;
      minColsPerKey = 1;
      maxColsPerKey = 2 * Long.parseLong(writeOpts[colIndex++]);
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
  }

  private void parseColumnFamilyOptions(CommandLine cmd) {
    String dataBlockEncodingStr = cmd.getOptionValue(OPT_DATA_BLOCK_ENCODING);
    dataBlockEncodingAlgo = dataBlockEncodingStr == null ? null :
        DataBlockEncoding.valueOf(dataBlockEncodingStr);
    if (dataBlockEncodingAlgo == DataBlockEncoding.NONE && encodeInCacheOnly) {
      throw new IllegalArgumentException("-" + OPT_ENCODE_IN_CACHE_ONLY + " " +
          "does not make sense when data block encoding is not used");
    }

    String compressStr = cmd.getOptionValue(OPT_COMPRESSION);
    compressAlgo = compressStr == null ? Compression.Algorithm.NONE :
        Compression.Algorithm.valueOf(compressStr);

    String bloomStr = cmd.getOptionValue(OPT_BLOOM);
    bloomType = bloomStr == null ? null :
        StoreFile.BloomType.valueOf(bloomStr);
  }

  public void initTestTable() throws IOException {
    HBaseTestingUtility.createPreSplitLoadTestTable(conf, tableName,
        COLUMN_FAMILY, compressAlgo, dataBlockEncodingAlgo);
    applyColumnFamilyOptions(tableName, COLUMN_FAMILIES);
  }

  @Override
  protected int doWork() throws IOException {
    if (cmd.hasOption(OPT_ZK_QUORUM)) {
      conf.set(HConstants.ZOOKEEPER_QUORUM, cmd.getOptionValue(OPT_ZK_QUORUM));
    }

    if (isInitOnly) {
      LOG.info("Initializing only; no reads or writes");
      initTestTable();
      return 0;
    }

    if (!isSkipInit) {
      initTestTable();
    }

    if (isWrite) {
      writerThreads = new MultiThreadedWriter(conf, tableName, COLUMN_FAMILY);
      writerThreads.setMultiPut(isMultiPut);
      writerThreads.setColumnsPerKey(minColsPerKey, maxColsPerKey);
      writerThreads.setDataSize(minColDataSize, maxColDataSize);
    }

    if (isRead) {
      readerThreads = new MultiThreadedReader(conf, tableName, COLUMN_FAMILY,
          verifyPercent);
      readerThreads.setMaxErrors(maxReadErrors);
      readerThreads.setKeyWindow(keyWindow);
    }

    if (isRead && isWrite) {
      LOG.info("Concurrent read/write workload: making readers aware of the " +
          "write point");
      readerThreads.linkToWriter(writerThreads);
    }

    if (isWrite) {
      System.out.println("Starting to write data...");
      writerThreads.start(startKey, endKey, numWriterThreads);
    }

    if (isRead) {
      System.out.println("Starting to read data...");
      readerThreads.start(startKey, endKey, numReaderThreads);
    }

    if (isWrite) {
      writerThreads.waitForFinish();
    }

    if (isRead) {
      readerThreads.waitForFinish();
    }

    boolean success = true;
    if (isWrite) {
      success = success && writerThreads.getNumWriteFailures() == 0;
    }
    if (isRead) {
      success = success && readerThreads.getNumReadErrors() == 0
          && readerThreads.getNumReadFailures() == 0;
    }
    return success ? 0 : 1;
  }

  public static void main(String[] args) {
    new LoadTestTool().doStaticMain(args);
  }

}
