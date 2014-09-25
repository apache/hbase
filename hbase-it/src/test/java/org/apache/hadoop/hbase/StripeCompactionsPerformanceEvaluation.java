/**
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

import java.io.IOException;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MultiThreadedAction;
import org.apache.hadoop.hbase.util.MultiThreadedReader;
import org.apache.hadoop.hbase.util.MultiThreadedWriter;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;
import org.junit.Assert;


/**
 * A perf test which does large data ingestion using stripe compactions and regular compactions.
 */
@InterfaceAudience.Private
public class StripeCompactionsPerformanceEvaluation extends AbstractHBaseTool {
  private static final Log LOG = LogFactory.getLog(StripeCompactionsPerformanceEvaluation.class);
  private static final String TABLE_NAME =
      StripeCompactionsPerformanceEvaluation.class.getSimpleName();
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("CF");
  private static final int MIN_NUM_SERVERS = 1;

  // Option names.
  private static final String DATAGEN_KEY = "datagen";
  private static final String ITERATIONS_KEY = "iters";
  private static final String PRELOAD_COUNT_KEY = "pwk";
  private static final String WRITE_COUNT_KEY = "wk";
  private static final String WRITE_THREADS_KEY = "wt";
  private static final String READ_THREADS_KEY = "rt";
  private static final String INITIAL_STRIPE_COUNT_KEY = "initstripes";
  private static final String SPLIT_SIZE_KEY = "splitsize";
  private static final String SPLIT_PARTS_KEY = "splitparts";
  private static final String VALUE_SIZE_KEY = "valsize";
  private static final String SEQ_SHARDS_PER_SERVER_KEY = "seqshards";

  // Option values.
  private LoadTestDataGenerator dataGen;
  private int iterationCount;
  private long preloadKeys;
  private long writeKeys;
  private int writeThreads;
  private int readThreads;
  private Long initialStripeCount;
  private Long splitSize;
  private Long splitParts;

  private static final String VALUE_SIZE_DEFAULT = "512:4096";

  protected IntegrationTestingUtility util = new IntegrationTestingUtility();

  @Override
  protected void addOptions() {
    addOptWithArg(DATAGEN_KEY, "Type of data generator to use (default or sequential)");
    addOptWithArg(SEQ_SHARDS_PER_SERVER_KEY, "Sequential generator will shard the data into many"
        + " sequences. The number of such shards per server is specified (default is 1).");
    addOptWithArg(ITERATIONS_KEY, "Number of iterations to run to compare");
    addOptWithArg(PRELOAD_COUNT_KEY, "Number of keys to preload, per server");
    addOptWithArg(WRITE_COUNT_KEY, "Number of keys to write, per server");
    addOptWithArg(WRITE_THREADS_KEY, "Number of threads to use for writing");
    addOptWithArg(READ_THREADS_KEY, "Number of threads to use for reading");
    addOptWithArg(INITIAL_STRIPE_COUNT_KEY, "Number of stripes to split regions into initially");
    addOptWithArg(SPLIT_SIZE_KEY, "Size at which a stripe will split into more stripes");
    addOptWithArg(SPLIT_PARTS_KEY, "Number of stripes to split a stripe into when it splits");
    addOptWithArg(VALUE_SIZE_KEY, "Value size; either a number, or a colon-separated range;"
        + " default " + VALUE_SIZE_DEFAULT);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    int minValueSize = 0, maxValueSize = 0;
    String valueSize = cmd.getOptionValue(VALUE_SIZE_KEY, VALUE_SIZE_DEFAULT);
    if (valueSize.contains(":")) {
      String[] valueSizes = valueSize.split(":");
      if (valueSize.length() != 2) throw new RuntimeException("Invalid value size: " + valueSize);
      minValueSize = Integer.parseInt(valueSizes[0]);
      maxValueSize = Integer.parseInt(valueSizes[1]);
    } else {
      minValueSize = maxValueSize = Integer.parseInt(valueSize);
    }
    String datagen = cmd.getOptionValue(DATAGEN_KEY, "default").toLowerCase();
    if ("default".equals(datagen)) {
      dataGen = new MultiThreadedAction.DefaultDataGenerator(
          minValueSize, maxValueSize, 1, 1, new byte[][] { COLUMN_FAMILY });
    } else if ("sequential".equals(datagen)) {
      int shards = Integer.parseInt(cmd.getOptionValue(SEQ_SHARDS_PER_SERVER_KEY, "1"));
      dataGen = new SeqShardedDataGenerator(minValueSize, maxValueSize, shards);
    } else {
      throw new RuntimeException("Unknown " + DATAGEN_KEY + ": " + datagen);
    }
    iterationCount = Integer.parseInt(cmd.getOptionValue(ITERATIONS_KEY, "1"));
    preloadKeys = Long.parseLong(cmd.getOptionValue(PRELOAD_COUNT_KEY, "1000000"));
    writeKeys = Long.parseLong(cmd.getOptionValue(WRITE_COUNT_KEY, "1000000"));
    writeThreads = Integer.parseInt(cmd.getOptionValue(WRITE_THREADS_KEY, "10"));
    readThreads = Integer.parseInt(cmd.getOptionValue(READ_THREADS_KEY, "20"));
    initialStripeCount = getLongOrNull(cmd, INITIAL_STRIPE_COUNT_KEY);
    splitSize = getLongOrNull(cmd, SPLIT_SIZE_KEY);
    splitParts = getLongOrNull(cmd, SPLIT_PARTS_KEY);
  }

  private Long getLongOrNull(CommandLine cmd, String option) {
    if (!cmd.hasOption(option)) return null;
    return Long.parseLong(cmd.getOptionValue(option));
  }

  @Override
  public Configuration getConf() {
    Configuration c = super.getConf();
    if (c == null && util != null) {
      conf = util.getConfiguration();
      c = conf;
    }
    return c;
  }

  @Override
  protected int doWork() throws Exception {
    setUp();
    try {
      boolean isStripe = true;
      for (int i = 0; i < iterationCount * 2; ++i) {
        createTable(isStripe);
        runOneTest((isStripe ? "Stripe" : "Default") + i, conf);
        isStripe = !isStripe;
      }
      return 0;
    } finally {
      tearDown();
    }
  }


  private void setUp() throws Exception {
    this.util = new IntegrationTestingUtility();
    LOG.debug("Initializing/checking cluster has " + MIN_NUM_SERVERS + " servers");
    util.initializeCluster(MIN_NUM_SERVERS);
    LOG.debug("Done initializing/checking cluster");
  }

  protected void deleteTable() throws Exception {
    if (util.getHBaseAdmin().tableExists(TABLE_NAME)) {
      LOG.info("Deleting table");
      if (!util.getHBaseAdmin().isTableDisabled(TABLE_NAME)) {
        util.getHBaseAdmin().disableTable(TABLE_NAME);
      }
      util.getHBaseAdmin().deleteTable(TABLE_NAME);
      LOG.info("Deleted table");
    }
  }

  private void createTable(boolean isStripe) throws Exception {
    createTable(createHtd(isStripe));
  }

  private void tearDown() throws Exception {
    deleteTable();
    LOG.info("Restoring the cluster");
    util.restoreCluster();
    LOG.info("Done restoring the cluster");
  }

  private void runOneTest(String description, Configuration conf) throws Exception {
    int numServers = util.getHBaseClusterInterface().getClusterStatus().getServersSize();
    long startKey = (long)preloadKeys * numServers;
    long endKey = startKey + (long)writeKeys * numServers;
    status(String.format("%s test starting on %d servers; preloading 0 to %d and writing to %d",
        description, numServers, startKey, endKey));

    TableName tn = TableName.valueOf(TABLE_NAME);
    if (preloadKeys > 0) {
      MultiThreadedWriter preloader = new MultiThreadedWriter(dataGen, conf, tn);
      long time = System.currentTimeMillis();
      preloader.start(0, startKey, writeThreads);
      preloader.waitForFinish();
      if (preloader.getNumWriteFailures() > 0) {
        throw new IOException("Preload failed");
      }
      int waitTime = (int)Math.min(preloadKeys / 100, 30000); // arbitrary
      status(description + " preload took " + (System.currentTimeMillis()-time)/1000
          + "sec; sleeping for " + waitTime/1000 + "sec for store to stabilize");
      Thread.sleep(waitTime);
    }

    MultiThreadedWriter writer = new MultiThreadedWriter(dataGen, conf, tn);
    MultiThreadedReader reader = new MultiThreadedReader(dataGen, conf, tn, 100);
    // reader.getMetrics().enable();
    reader.linkToWriter(writer);

    long testStartTime = System.currentTimeMillis();
    writer.start(startKey, endKey, writeThreads);
    reader.start(startKey, endKey, readThreads);
    writer.waitForFinish();
    reader.waitForFinish();
    // reader.waitForVerification(300000);
    // reader.abortAndWaitForFinish();
    status("Readers and writers stopped for test " + description);

    boolean success = writer.getNumWriteFailures() == 0;
    if (!success) {
      LOG.error("Write failed");
    } else {
      success = reader.getNumReadErrors() == 0 && reader.getNumReadFailures() == 0;
      if (!success) {
        LOG.error("Read failed");
      }
    }

    // Dump perf regardless of the result.
    /*StringBuilder perfDump = new StringBuilder();
    for (Pair<Long, Long> pt : reader.getMetrics().getCombinedCdf()) {
      perfDump.append(String.format(
          "csvread,%s,%d,%d%n", description, pt.getFirst(), pt.getSecond()));
    }
    if (dumpTimePerf) {
      Iterator<Triple<Long, Double, Long>> timePerf = reader.getMetrics().getCombinedTimeSeries();
      while (timePerf.hasNext()) {
        Triple<Long, Double, Long> pt = timePerf.next();
        perfDump.append(String.format("csvtime,%s,%d,%d,%.4f%n",
            description, pt.getFirst(), pt.getThird(), pt.getSecond()));
      }
    }
    LOG.info("Performance data dump for " + description + " test: \n" + perfDump.toString());*/
    status(description + " test took " + (System.currentTimeMillis()-testStartTime)/1000 + "sec");
    Assert.assertTrue(success);
  }

  private static void status(String s) {
    LOG.info("STATUS " + s);
    System.out.println(s);
  }

  private HTableDescriptor createHtd(boolean isStripe) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    String noSplitsPolicy = DisabledRegionSplitPolicy.class.getName();
    htd.setConfiguration(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, noSplitsPolicy);
    if (isStripe) {
      htd.setConfiguration(StoreEngine.STORE_ENGINE_CLASS_KEY, StripeStoreEngine.class.getName());
      if (initialStripeCount != null) {
        htd.setConfiguration(
            StripeStoreConfig.INITIAL_STRIPE_COUNT_KEY, initialStripeCount.toString());
        htd.setConfiguration(
            HStore.BLOCKING_STOREFILES_KEY, Long.toString(10 * initialStripeCount));
      } else {
        htd.setConfiguration(HStore.BLOCKING_STOREFILES_KEY, "500");
      }
      if (splitSize != null) {
        htd.setConfiguration(StripeStoreConfig.SIZE_TO_SPLIT_KEY, splitSize.toString());
      }
      if (splitParts != null) {
        htd.setConfiguration(StripeStoreConfig.SPLIT_PARTS_KEY, splitParts.toString());
      }
    } else {
      htd.setConfiguration(HStore.BLOCKING_STOREFILES_KEY, "10"); // default
    }
    return htd;
  }

  protected void createTable(HTableDescriptor htd) throws Exception {
    deleteTable();
    if (util.getHBaseClusterInterface() instanceof MiniHBaseCluster) {
      LOG.warn("Test does not make a lot of sense for minicluster. Will set flush size low.");
      htd.setConfiguration(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, "1048576");
    }
    byte[][] splits = new RegionSplitter.HexStringSplit().split(
        util.getHBaseClusterInterface().getClusterStatus().getServersSize());
    util.getHBaseAdmin().createTable(htd, splits);
  }

  public static class SeqShardedDataGenerator extends LoadTestDataGenerator {
    private static final byte[][] COLUMN_NAMES = new byte[][] { Bytes.toBytes("col1") };
    private static final int PAD_TO = 10;
    private static final int PREFIX_PAD_TO = 7;

    private final int numPartitions;

    public SeqShardedDataGenerator(int minValueSize, int maxValueSize, int numPartitions) {
      super(minValueSize, maxValueSize);
      this.numPartitions = numPartitions;
    }

    @Override
    public byte[] getDeterministicUniqueKey(long keyBase) {
      String num = StringUtils.leftPad(String.valueOf(keyBase), PAD_TO, "0");
      return Bytes.toBytes(getPrefix(keyBase) + num);
    }

    private String getPrefix(long i) {
      return StringUtils.leftPad(String.valueOf((int)(i % numPartitions)), PREFIX_PAD_TO, "0");
    }

    @Override
    public byte[][] getColumnFamilies() {
      return new byte[][] { COLUMN_FAMILY };
    }

    @Override
    public byte[][] generateColumnsForCf(byte[] rowKey, byte[] cf) {
      return COLUMN_NAMES;
    }

    @Override
    public byte[] generateValue(byte[] rowKey, byte[] cf, byte[] column) {
      return kvGenerator.generateRandomSizeValue(rowKey, cf, column);
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, byte[] column, byte[] value) {
      return LoadTestKVGenerator.verify(value, rowKey, cf, column);
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, Set<byte[]> columnSet) {
      return true;
    }
  };
}
