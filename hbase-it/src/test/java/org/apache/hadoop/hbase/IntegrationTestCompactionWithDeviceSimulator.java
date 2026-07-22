/*
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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.devsim.EBSDevice;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator;
import org.apache.hadoop.hbase.io.hfile.PreviousBlockCompressionRatePredicator;
import org.apache.hadoop.hbase.io.hfile.UncompressedBlockSizePredicator;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * Integration test that demonstrates the EBS device layer simulator's value for diagnosing
 * compaction throughput under different storage device constraints. Runs HBase major compactions
 * under two simulated EBS volume bandwidth limits (constrained and baseline) with realistic HFile
 * configuration (Snappy compression, tunable compression ratio, configurable block parameters) and
 * produces a diagnostic throughput analysis comparing the scenarios.
 * <p>
 * This test always starts a local MiniDFSCluster with the EBS device layer installed — the
 * distributed cluster mode of IntegrationTestBase is not applicable because the device simulator
 * requires the {@code ThrottledFsDatasetFactory} to be injected at DataNode startup.
 * <p>
 * <b>JUnit / maven-failsafe execution:</b>
 *
 * <pre>
 * mvn verify -pl hbase-it -Dtest=IntegrationTestCompactionWithDeviceSimulator
 * </pre>
 *
 * <b>CLI execution via hbase script:</b>
 *
 * <pre>
 * hbase org.apache.hadoop.hbase.IntegrationTestCompactionWithDeviceSimulator \
 *   -totalDataBytes 536870912 -constrainedBwMbps 10 -compression LZ4
 * </pre>
 *
 * <b>Configurable parameters</b> (settable via CLI flags or
 * {@code -Dhbase.IntegrationTestCompactionWithDeviceSimulator.<param>}):
 *
 * <pre>
 * # Data generation
 *   totalDataBytes          (default 1073741824 = 1 GB)
 *   valueSize               (default 102400 = 100 KB)
 *   numFlushCycles          (default 10)
 *   targetCompressionRatio  (default 3.0)
 *
 * # HFile / column family
 *   compression             (default SNAPPY)
 *   dataBlockEncoding       (default NONE)
 *   blockSize               (default 65536 = 64 KB)
 *   bloomType               (default ROW)
 *   blockPredicator         (default PreviousBlockCompressionRatePredicator)
 *
 * # Device simulator
 *   constrainedBwMbps       (default 25)
 *   baselineBwMbps          (default 250)
 *   budgetIops              (default 100000)
 *   deviceLatencyUs         (default 0)
 *   volumesPerDataNode      (default 1)
 * </pre>
 */
@Tag(IntegrationTests.TAG)
public class IntegrationTestCompactionWithDeviceSimulator extends IntegrationTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestCompactionWithDeviceSimulator.class);

  private static final String CLASS_NAME =
    IntegrationTestCompactionWithDeviceSimulator.class.getSimpleName();
  private static final String CONF_PREFIX = "hbase." + CLASS_NAME + ".";

  private static final TableName TABLE_NAME = TableName.valueOf(CLASS_NAME);
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final Map<String,
    Class<? extends BlockCompressedSizePredicator>> PREDICATOR_CLASSES =
      Map.of("PreviousBlockCompressionRatePredicator", PreviousBlockCompressionRatePredicator.class,
        "UncompressedBlockSizePredicator", UncompressedBlockSizePredicator.class);

  // CLI option keys
  private static final String OPT_TOTAL_DATA_BYTES = "totalDataBytes";
  private static final String OPT_VALUE_SIZE = "valueSize";
  private static final String OPT_NUM_FLUSH_CYCLES = "numFlushCycles";
  private static final String OPT_TARGET_COMPRESSION_RATIO = "targetCompressionRatio";
  private static final String OPT_COMPRESSION = "compression";
  private static final String OPT_DATA_BLOCK_ENCODING = "dataBlockEncoding";
  private static final String OPT_BLOCK_SIZE = "blockSize";
  private static final String OPT_BLOOM_TYPE = "bloomType";
  private static final String OPT_BLOCK_PREDICATOR = "blockPredicator";
  private static final String OPT_CONSTRAINED_BW_MBPS = "constrainedBwMbps";
  private static final String OPT_BASELINE_BW_MBPS = "baselineBwMbps";
  private static final String OPT_BUDGET_IOPS = "budgetIops";
  private static final String OPT_DEVICE_LATENCY_US = "deviceLatencyUs";
  private static final String OPT_VOLUMES_PER_DN = "volumesPerDataNode";

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    util = getTestingUtil(getConf());
  }

  @Override
  @AfterEach
  public void cleanUp() throws Exception {
    // Each scenario cleans up its own cluster.
  }

  @Override
  public void setUpCluster() throws Exception {
    // Cluster lifecycle is per-scenario.
  }

  @Override
  public void setUpMonkey() throws Exception {
    // Chaos monkey is not applicable for this test.
  }

  @Override
  public TableName getTablename() {
    return TABLE_NAME;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(FAMILY));
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    addOptWithArg(OPT_TOTAL_DATA_BYTES, "Total uncompressed data bytes (default 1073741824)");
    addOptWithArg(OPT_VALUE_SIZE, "Per-cell value size in bytes (default 102400)");
    addOptWithArg(OPT_NUM_FLUSH_CYCLES, "Number of flush cycles / store files (default 10)");
    addOptWithArg(OPT_TARGET_COMPRESSION_RATIO,
      "Target compression ratio for generated values (default 3.0)");
    addOptWithArg(OPT_COMPRESSION,
      "Compression algorithm: SNAPPY, LZ4, ZSTD, GZ, NONE " + "(default SNAPPY)");
    addOptWithArg(OPT_DATA_BLOCK_ENCODING,
      "Data block encoding: NONE, PREFIX, DIFF, FAST_DIFF, ROW_INDEX_V1 (default NONE)");
    addOptWithArg(OPT_BLOCK_SIZE, "HFile data block size in bytes (default 65536)");
    addOptWithArg(OPT_BLOOM_TYPE, "Bloom filter type: NONE, ROW, ROWCOL (default ROW)");
    addOptWithArg(OPT_BLOCK_PREDICATOR,
      "Block compressed size predicator class short name (default "
        + "PreviousBlockCompressionRatePredicator)");
    addOptWithArg(OPT_CONSTRAINED_BW_MBPS,
      "Per-volume BW limit for constrained scenario in MB/s (default 25)");
    addOptWithArg(OPT_BASELINE_BW_MBPS,
      "Per-volume BW limit for baseline scenario in MB/s (default 250)");
    addOptWithArg(OPT_BUDGET_IOPS, "Per-volume IOPS budget (default 100000)");
    addOptWithArg(OPT_DEVICE_LATENCY_US,
      "Per-IO device latency in microseconds, 0=disabled (default 0)");
    addOptWithArg(OPT_VOLUMES_PER_DN, "Number of storage volumes per DataNode (default 1)");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    processBaseOptions(cmd);
    Configuration c = getConf();
    setOptToConf(cmd, c, OPT_TOTAL_DATA_BYTES);
    setOptToConf(cmd, c, OPT_VALUE_SIZE);
    setOptToConf(cmd, c, OPT_NUM_FLUSH_CYCLES);
    setOptToConf(cmd, c, OPT_TARGET_COMPRESSION_RATIO);
    setOptToConf(cmd, c, OPT_COMPRESSION);
    setOptToConf(cmd, c, OPT_DATA_BLOCK_ENCODING);
    setOptToConf(cmd, c, OPT_BLOCK_SIZE);
    setOptToConf(cmd, c, OPT_BLOOM_TYPE);
    setOptToConf(cmd, c, OPT_BLOCK_PREDICATOR);
    setOptToConf(cmd, c, OPT_CONSTRAINED_BW_MBPS);
    setOptToConf(cmd, c, OPT_BASELINE_BW_MBPS);
    setOptToConf(cmd, c, OPT_BUDGET_IOPS);
    setOptToConf(cmd, c, OPT_DEVICE_LATENCY_US);
    setOptToConf(cmd, c, OPT_VOLUMES_PER_DN);
  }

  private static void setOptToConf(CommandLine cmd, Configuration conf, String opt) {
    if (cmd.hasOption(opt)) {
      conf.set(CONF_PREFIX + opt, cmd.getOptionValue(opt));
    }
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    doTestCompaction();
    return 0;
  }

  @Test
  public void testCompactionThroughputVariesWithDeviceBandwidth() throws Exception {
    doTestCompaction();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestCompactionWithDeviceSimulator(), args);
    System.exit(ret);
  }

  private void doTestCompaction() throws Exception {
    Configuration baseConf = getConf() != null ? getConf() : HBaseConfiguration.create();

    long totalDataBytes =
      baseConf.getLong(CONF_PREFIX + OPT_TOTAL_DATA_BYTES, 1L * 1024 * 1024 * 1024);
    int valueSize = baseConf.getInt(CONF_PREFIX + OPT_VALUE_SIZE, 100 * 1024);
    int numFlushCycles = baseConf.getInt(CONF_PREFIX + OPT_NUM_FLUSH_CYCLES, 10);
    double targetCompressionRatio =
      Double.parseDouble(baseConf.get(CONF_PREFIX + OPT_TARGET_COMPRESSION_RATIO, "3.0"));

    String compressionName = baseConf.get(CONF_PREFIX + OPT_COMPRESSION, "SNAPPY");
    String dataBlockEncodingName = baseConf.get(CONF_PREFIX + OPT_DATA_BLOCK_ENCODING, "NONE");
    int blockSize = baseConf.getInt(CONF_PREFIX + OPT_BLOCK_SIZE, 64 * 1024);
    String bloomTypeName = baseConf.get(CONF_PREFIX + OPT_BLOOM_TYPE, "ROW");
    String blockPredicatorName =
      baseConf.get(CONF_PREFIX + OPT_BLOCK_PREDICATOR, "PreviousBlockCompressionRatePredicator");

    int constrainedBwMbps = baseConf.getInt(CONF_PREFIX + OPT_CONSTRAINED_BW_MBPS, 25);
    int baselineBwMbps = baseConf.getInt(CONF_PREFIX + OPT_BASELINE_BW_MBPS, 250);
    int budgetIops = baseConf.getInt(CONF_PREFIX + OPT_BUDGET_IOPS, 100000);
    int deviceLatencyUs = baseConf.getInt(CONF_PREFIX + OPT_DEVICE_LATENCY_US, 0);
    int volumesPerDataNode = baseConf.getInt(CONF_PREFIX + OPT_VOLUMES_PER_DN, 1);

    Compression.Algorithm compression = Compression.Algorithm.valueOf(compressionName);
    DataBlockEncoding dataBlockEncoding = DataBlockEncoding.valueOf(dataBlockEncodingName);
    BloomType bloomType = BloomType.valueOf(bloomTypeName);

    Class<? extends BlockCompressedSizePredicator> predicatorClass =
      PREDICATOR_CLASSES.get(blockPredicatorName);
    if (predicatorClass == null) {
      throw new IllegalArgumentException("Unknown blockPredicator '" + blockPredicatorName
        + "'. Known values: " + PREDICATOR_CLASSES.keySet());
    }

    LOG.info("========== Test Configuration ==========");
    LOG.info("  totalDataBytes={}, valueSize={}, numFlushCycles={}, targetCompressionRatio={}",
      totalDataBytes, valueSize, numFlushCycles, targetCompressionRatio);
    LOG.info("  compression={}, dataBlockEncoding={}, blockSize={}, bloomType={}", compression,
      dataBlockEncoding, blockSize, bloomType);
    LOG.info("  blockPredicator={}", predicatorClass.getName());
    LOG.info("  constrainedBwMbps={}, baselineBwMbps={}, budgetIops={}, deviceLatencyUs={}",
      constrainedBwMbps, baselineBwMbps, budgetIops, deviceLatencyUs);
    LOG.info("  volumesPerDataNode={}", volumesPerDataNode);
    LOG.info("========================================");

    CompactionPerformance constrained =
      runCompactionScenario("constrained", constrainedBwMbps, budgetIops, deviceLatencyUs,
        volumesPerDataNode, totalDataBytes, valueSize, numFlushCycles, targetCompressionRatio,
        compression, dataBlockEncoding, blockSize, bloomType, predicatorClass);

    CompactionPerformance baseline =
      runCompactionScenario("baseline", baselineBwMbps, budgetIops, deviceLatencyUs,
        volumesPerDataNode, totalDataBytes, valueSize, numFlushCycles, targetCompressionRatio,
        compression, dataBlockEncoding, blockSize, bloomType, predicatorClass);

    constrained.log();
    baseline.log();
    double durationRatio = (double) constrained.durationMs / baseline.durationMs;
    double bwRatio = (double) baselineBwMbps / constrainedBwMbps;
    LOG.info("========== Cross-Scenario Comparison ==========");
    LOG.info("  Duration ratio (constrained/baseline): {}", String.format("%.2f", durationRatio));
    LOG.info("  BW ratio (baseline/constrained): {}", String.format("%.2f", bwRatio));
    LOG.info("  Effective throughput: constrained={} MB/s, baseline={} MB/s",
      String.format("%.2f", constrained.effectiveThroughputMbps()),
      String.format("%.2f", baseline.effectiveThroughputMbps()));
    LOG.info("================================================");

    long constrainedBwSleeps = constrained.bwReadSleepCount + constrained.bwWriteSleepCount;
    assertTrue(constrainedBwSleeps > 0,
      "Expected BW throttle sleeps in constrained scenario, got 0. "
        + "The device simulator did not throttle compaction I/O.");

    assertTrue(constrained.durationMs > baseline.durationMs,
      "Expected constrained scenario (" + constrainedBwMbps + " MB/s, " + constrained.durationMs
        + " ms) to take longer than baseline (" + baselineBwMbps + " MB/s, " + baseline.durationMs
        + " ms).");
  }

  private CompactionPerformance runCompactionScenario(String label, int bwMbps, int budgetIops,
    int deviceLatencyUs, int volumesPerDataNode, long totalDataBytes, int valueSize,
    int numFlushCycles, double targetCompressionRatio, Compression.Algorithm compression,
    DataBlockEncoding dataBlockEncoding, int blockSize, BloomType bloomType,
    Class<? extends BlockCompressedSizePredicator> predicatorClass) throws Exception {

    LOG.info(">>> Starting scenario '{}' with device BW = {} MB/s", label, bwMbps);

    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      "org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy");
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
    conf.set(BlockCompressedSizePredicator.BLOCK_COMPRESSED_SIZE_PREDICATOR,
      predicatorClass.getName());

    EBSDevice.configure(conf, bwMbps, budgetIops, deviceLatencyUs);

    HBaseTestingUtil testUtil = new HBaseTestingUtil(conf);
    try {
      testUtil.startMiniZKCluster();
      MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .storagesPerDatanode(volumesPerDataNode).build();
      dfsCluster.waitClusterUp();
      testUtil.setDFSCluster(dfsCluster);
      testUtil.startMiniHBaseCluster(1, 1);

      ColumnFamilyDescriptorBuilder cfBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setCompressionType(compression)
          .setCompactionCompressionType(compression).setDataBlockEncoding(dataBlockEncoding)
          .setBlocksize(blockSize).setBloomFilterType(bloomType);

      Admin admin = testUtil.getAdmin();
      admin.createTable(
        TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cfBuilder.build()).build());
      testUtil.waitUntilAllRegionsAssigned(TABLE_NAME);

      long uncompressedDataLoaded = loadData(testUtil, totalDataBytes, valueSize, numFlushCycles,
        targetCompressionRatio, admin);

      HStore store = getStore(testUtil);
      int storeFilesBefore = store.getStorefilesCount();
      LOG.info("  Scenario '{}': loaded {} MB uncompressed across {} store files", label,
        String.format("%.2f", uncompressedDataLoaded / (1024.0 * 1024.0)), storeFilesBefore);

      EBSDevice.resetMetrics();

      long startTime = EnvironmentEdgeManager.currentTime();
      admin.majorCompact(TABLE_NAME);
      waitForCompaction(store);
      long durationMs = EnvironmentEdgeManager.currentTime() - startTime;

      CompactionPerformance result = new CompactionPerformance(label, bwMbps, durationMs,
        uncompressedDataLoaded, storeFilesBefore);

      LOG.info("<<< Scenario '{}' complete in {} ms", label, durationMs);
      return result;
    } finally {
      EBSDevice.shutdown();
      testUtil.shutdownMiniCluster();
    }
  }

  private long loadData(HBaseTestingUtil testUtil, long totalDataBytes, int valueSize,
    int numFlushCycles, double targetCompressionRatio, Admin admin) throws IOException {
    long bytesPerFlush = totalDataBytes / numFlushCycles;
    int rowsPerFlush = Math.max(1, (int) (bytesPerFlush / valueSize));
    long totalUncompressed = 0;
    Random rng = new Random(0xCAFEBABE);
    int rowCounter = 0;

    try (Table table = testUtil.getConnection().getTable(TABLE_NAME)) {
      for (int cycle = 0; cycle < numFlushCycles; cycle++) {
        for (int r = 0; r < rowsPerFlush; r++) {
          byte[] rowKey = Bytes.toBytes(String.format("row-%010d", rowCounter++));
          byte[] value = generateCompressibleValue(rng, valueSize, targetCompressionRatio);
          table.put(new Put(rowKey).addColumn(FAMILY, QUALIFIER, value));
          totalUncompressed += valueSize;
        }
        admin.flush(TABLE_NAME);
        waitForFlush(testUtil);
        if ((cycle + 1) % 5 == 0 || cycle == numFlushCycles - 1) {
          LOG.info("  Flush cycle {}/{} complete ({} MB loaded so far)", cycle + 1, numFlushCycles,
            String.format("%.1f", totalUncompressed / (1024.0 * 1024.0)));
        }
      }
    }

    return totalUncompressed;
  }

  /**
   * Generate a byte array that compresses at approximately the requested ratio under Snappy. The
   * value is split into a random (incompressible) segment and a repeated-byte (highly compressible)
   * segment. For a 3:1 target ratio, approximately 1/3 of the bytes are random and 2/3 are a
   * constant.
   */
  static byte[] generateCompressibleValue(Random rng, int size, double targetCompressionRatio) {
    byte[] value = new byte[size];
    int randomBytes = Math.max(1, (int) (size / targetCompressionRatio));
    byte[] randomPart = new byte[randomBytes];
    rng.nextBytes(randomPart);
    System.arraycopy(randomPart, 0, value, 0, randomBytes);
    byte fill = (byte) 0x42;
    for (int i = randomBytes; i < size; i++) {
      value[i] = fill;
    }
    return value;
  }

  private HStore getStore(HBaseTestingUtil testUtil) {
    SingleProcessHBaseCluster cluster = testUtil.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (JVMClusterUtil.RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      for (Region region : hrs.getRegions(TABLE_NAME)) {
        return ((HRegion) region).getStores().iterator().next();
      }
    }
    throw new IllegalStateException("No store found for table " + TABLE_NAME);
  }

  private void waitForCompaction(HStore store) throws InterruptedException {
    long deadline = EnvironmentEdgeManager.currentTime() + 600_000;
    while (store.getStorefilesCount() > 1) {
      if (EnvironmentEdgeManager.currentTime() > deadline) {
        throw new RuntimeException("Compaction did not complete within 10 minutes. Store files: "
          + store.getStorefilesCount());
      }
      Thread.sleep(500);
    }
  }

  private void waitForFlush(HBaseTestingUtil testUtil) throws IOException {
    long deadline = EnvironmentEdgeManager.currentTime() + 120_000;
    while (EnvironmentEdgeManager.currentTime() < deadline) {
      long memstoreSize = 0;
      for (HRegion region : testUtil.getMiniHBaseCluster().getRegionServer(0)
        .getRegions(TABLE_NAME)) {
        memstoreSize += region.getMemStoreDataSize();
      }
      if (memstoreSize == 0) {
        return;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for flush", e);
      }
    }
    throw new IOException("Flush did not complete within timeout");
  }

  private static class CompactionPerformance {
    final String label;
    final int bwMbps;
    final long durationMs;
    final long bytesRead;
    final long bytesWritten;
    final long bwReadSleepCount;
    final long bwWriteSleepCount;
    final long bwReadSleepTimeMs;
    final long bwWriteSleepTimeMs;
    final long deviceReadOps;
    final long deviceWriteOps;
    final long appReadOps;
    final long appWriteOps;
    final long iopsReadSleepCount;
    final long iopsWriteSleepCount;
    final long uncompressedDataBytes;
    final int storeFilesBefore;

    CompactionPerformance(String label, int bwMbps, long durationMs, long uncompressedDataBytes,
      int storeFilesBefore) {
      this.label = label;
      this.bwMbps = bwMbps;
      this.durationMs = durationMs;
      this.bytesRead = EBSDevice.getTotalBytesRead();
      this.bytesWritten = EBSDevice.getTotalBytesWritten();
      this.bwReadSleepCount = EBSDevice.getReadSleepCount();
      this.bwWriteSleepCount = EBSDevice.getWriteSleepCount();
      this.bwReadSleepTimeMs = EBSDevice.getReadSleepTimeMs();
      this.bwWriteSleepTimeMs = EBSDevice.getWriteSleepTimeMs();
      this.deviceReadOps = EBSDevice.getDeviceReadOps();
      this.deviceWriteOps = EBSDevice.getDeviceWriteOps();
      this.appReadOps = EBSDevice.getReadOpCount();
      this.appWriteOps = EBSDevice.getWriteOpCount();
      this.iopsReadSleepCount = EBSDevice.getIopsReadSleepCount();
      this.iopsWriteSleepCount = EBSDevice.getIopsWriteSleepCount();
      this.uncompressedDataBytes = uncompressedDataBytes;
      this.storeFilesBefore = storeFilesBefore;
    }

    double effectiveThroughputMbps() {
      if (durationMs == 0) {
        return Double.MAX_VALUE;
      }
      return (bytesRead + bytesWritten) / (1024.0 * 1024.0) / (durationMs / 1000.0);
    }

    double observedCompressionRatio() {
      if (bytesWritten == 0) {
        return 1.0;
      }
      return (double) uncompressedDataBytes / bytesWritten;
    }

    void log() {
      double throughputMbps = effectiveThroughputMbps();
      LOG.info("=== Scenario: {} (device BW = {} MB/s) ===", label, bwMbps);
      LOG.info("  Store files before compaction: {}", storeFilesBefore);
      LOG.info("  Compaction duration: {} ms", durationMs);
      LOG.info("  Device bytes read:  {} ({} MB)", bytesRead,
        String.format("%.2f", bytesRead / (1024.0 * 1024.0)));
      LOG.info("  Device bytes written: {} ({} MB)", bytesWritten,
        String.format("%.2f", bytesWritten / (1024.0 * 1024.0)));
      LOG.info("  Effective device throughput: {} MB/s", String.format("%.2f", throughputMbps));
      LOG.info("  Observed compression ratio: {}:1 (uncompressed={} MB, on-disk-written={} MB)",
        String.format("%.2f", observedCompressionRatio()),
        String.format("%.2f", uncompressedDataBytes / (1024.0 * 1024.0)),
        String.format("%.2f", bytesWritten / (1024.0 * 1024.0)));
      LOG.info("  BW sleeps: read={}({} ms) write={}({} ms)", bwReadSleepCount, bwReadSleepTimeMs,
        bwWriteSleepCount, bwWriteSleepTimeMs);
      LOG.info("  App ops: read={} write={}  |  Device ops: read={} write={}", appReadOps,
        appWriteOps, deviceReadOps, deviceWriteOps);
      LOG.info("  IOPS sleeps: read={} write={}", iopsReadSleepCount, iopsWriteSleepCount);
    }
  }
}
