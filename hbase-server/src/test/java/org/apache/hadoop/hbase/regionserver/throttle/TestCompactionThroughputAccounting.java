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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies what unit of bytes compaction throughput control is charged with, depending on
 * {@link Compactor#COMPACTION_THROUGHPUT_CONTROL_BY_OUTPUT_KEY}. Unlike
 * {@link TestCompactionWithThroughputController} this does not measure timing: it records the sizes
 * passed to {@link ThroughputController#control(String, long)} and compares them against the
 * on-disk size of the compacted file, which is deterministic.
 */
@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
public class TestCompactionThroughputAccounting {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestCompactionThroughputAccounting.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private static final byte[] FAMILY = Bytes.toBytes("f");

  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  /** Fragment of the throttling operation name identifying compactions of our family. */
  private static final String OP_NAME_FRAGMENT = "#f#compaction#";

  // Long common prefix so that DIFF encoding + GZ compression shrink the data heavily, mimicking
  // a rowkey-dominated meta family whose on-disk size is an order of magnitude smaller than the
  // serialized cell size.
  private static final String ROW_PREFIX =
    "http://very.long.common.row.key.prefix.invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      + "/bbbbbbbbbbbbbbbbbbbb/";

  /**
   * Throughput controller that never throttles but records the total size charged per operation
   * name. Registered via {@link CompactionThroughputControllerFactory}.
   */
  public static final class RecordingThroughputController extends NoLimitThroughputController {

    private static final Map<String, AtomicLong> CHARGED = new ConcurrentHashMap<>();

    @Override
    public long control(String opName, long size) throws InterruptedException {
      CHARGED.computeIfAbsent(opName, k -> new AtomicLong()).addAndGet(size);
      return 0;
    }

    static void reset() {
      CHARGED.clear();
    }

    static long chargedFor(String opNameFragment) {
      long total = 0;
      for (Map.Entry<String, AtomicLong> entry : CHARGED.entrySet()) {
        if (entry.getKey().contains(opNameFragment)) {
          total += entry.getValue().get();
        }
      }
      return total;
    }
  }

  private HStore getStoreWithName(TableName tableName) {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getRegions(tableName)) {
        return ((HRegion) region).getStores().iterator().next();
      }
    }
    return null;
  }

  private static byte[] rowKey(int index) {
    return Bytes.toBytes(ROW_PREFIX + String.format("%010d", index));
  }

  private HStore prepareData() throws IOException {
    TEST_UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
      // Bloom off: bloom bits are incompressible and, being smaller than one bloom chunk for
      // this small file, are written only at close time (uncharged by both accounting modes),
      // which would dominate the on-disk size and blur the charged-vs-disk comparison below.
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
        .setDataBlockEncoding(DataBlockEncoding.DIFF).setCompressionType(Compression.Algorithm.GZ)
        .setBlocksize(32 * 1024).setBloomFilterType(BloomType.NONE).build())
      .build());
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    byte[] value = new byte[16];
    int row = 0;
    for (int flush = 0; flush < 3; flush++) {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < 20000; i++) {
        puts.add(new Put(rowKey(row++)).addColumn(FAMILY, QUALIFIER, value));
      }
      table.put(puts);
      TEST_UTIL.getAdmin().flush(tableName);
    }
    return getStoreWithName(tableName);
  }

  /**
   * Loads three versions for the same rows (incompressible values), then deletes every row and
   * flushes the delete markers as a fourth file. With KEEP_DELETED_CELLS=TRUE the deleted put cells
   * survive the major compaction, and with historical compaction files enabled they are streamed to
   * the historical file writer, which then holds most of the on-disk bytes.
   */
  private HStore prepareDeletedData() throws IOException {
    TEST_UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
          .setDataBlockEncoding(DataBlockEncoding.DIFF).setCompressionType(Compression.Algorithm.GZ)
          .setBlocksize(32 * 1024).setBloomFilterType(BloomType.NONE).setMaxVersions(3)
          .setKeepDeletedCells(KeepDeletedCells.TRUE).build())
        .build());
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    for (int flush = 0; flush < 3; flush++) {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < 20000; i++) {
        byte[] value = new byte[100];
        Bytes.random(value);
        puts.add(new Put(rowKey(i)).addColumn(FAMILY, QUALIFIER, value));
      }
      table.put(puts);
      TEST_UTIL.getAdmin().flush(tableName);
    }
    List<Delete> deletes = new ArrayList<>();
    for (int i = 0; i < 20000; i++) {
      deletes.add(new Delete(rowKey(i)));
    }
    table.delete(deletes);
    TEST_UTIL.getAdmin().flush(tableName);
    return getStoreWithName(tableName);
  }

  /**
   * Starts a mini cluster with the given accounting mode, major compacts the prepared table and
   * returns { charged bytes for our family, on-disk size of the compacted file }.
   */
  private long[] runMajorCompaction(boolean controlByOutput, boolean historicalFiles)
    throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Keep flushes from triggering minor compactions so that only the explicit major compaction
    // below is charged to the controller.
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.setBoolean(Compactor.COMPACTION_THROUGHPUT_CONTROL_BY_OUTPUT_KEY, controlByOutput);
    conf.setBoolean(StoreFileWriter.ENABLE_HISTORICAL_COMPACTION_FILES, historicalFiles);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      RecordingThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      HStore store = historicalFiles ? prepareDeletedData() : prepareData();
      assertEquals(historicalFiles ? 4 : 3, store.getStorefilesCount());
      RecordingThroughputController.reset();
      TEST_UTIL.getAdmin().majorCompact(tableName);
      // With historical compaction files enabled the major compaction commits two files (live +
      // historical), otherwise one. The commit swaps the store file set atomically, so waiting
      // for the count to drop and asserting the exact value fails fast instead of hanging if the
      // output file count is not the expected one.
      int expectedFiles = historicalFiles ? 2 : 1;
      while (store.getStorefilesCount() > expectedFiles) {
        Thread.sleep(20);
      }
      assertEquals(expectedFiles, store.getStorefilesCount());
      long charged = RecordingThroughputController.chargedFor(OP_NAME_FRAGMENT);
      long diskSize = store.getStorefilesSize();
      LOG.info("controlByOutput={}, charged={}, diskSize={}", controlByOutput, charged, diskSize);
      return new long[] { charged, diskSize };
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testAccountingUnit() throws Exception {
    long[] cellSizeMode = runMajorCompaction(false, false);
    long[] outputMode = runMajorCompaction(true, false);

    // Default accounting charges serialized cell sizes, which for this highly compressible data
    // must be far larger than what actually hits the disk.
    assertTrue(cellSizeMode[0] > cellSizeMode[1] * 3,
      "cell size accounting should be charged well beyond the on-disk size, charged="
        + cellSizeMode[0] + ", diskSize=" + cellSizeMode[1]);

    // Output based accounting charges what is actually written: bounded by the final file size
    // (trailing blocks are written at close, after the last charge) and close to it.
    assertTrue(outputMode[0] > 0, "output accounting should have charged something");
    assertTrue(outputMode[0] <= outputMode[1],
      "output accounting must not exceed the on-disk size, charged=" + outputMode[0] + ", diskSize="
        + outputMode[1]);
    assertTrue(outputMode[0] >= outputMode[1] / 2,
      "output accounting should be close to the on-disk size, charged=" + outputMode[0]
        + ", diskSize=" + outputMode[1]);
    assertTrue(outputMode[0] < cellSizeMode[0] / 3,
      "output accounting should charge far less than cell size accounting for compressible data,"
        + " outputCharged=" + outputMode[0] + ", cellSizeCharged=" + cellSizeMode[0]);

    // With historical compaction files the output is split into a live and a historical file;
    // bytes written to both are real disk load and must both be charged. Here the historical
    // file receives all deleted put cells (with incompressible values) and thus holds most of
    // the on-disk bytes, so a live-only accounting would stay well below half of it.
    long[] historicalMode = runMajorCompaction(true, true);
    assertTrue(historicalMode[0] > 0, "output accounting should have charged something");
    assertTrue(historicalMode[0] <= historicalMode[1],
      "output accounting must not exceed the on-disk size, charged=" + historicalMode[0]
        + ", diskSize=" + historicalMode[1]);
    assertTrue(historicalMode[0] >= historicalMode[1] / 2,
      "charged bytes must include the historical file writer, charged=" + historicalMode[0]
        + ", diskSize=" + historicalMode[1]);
  }
}
