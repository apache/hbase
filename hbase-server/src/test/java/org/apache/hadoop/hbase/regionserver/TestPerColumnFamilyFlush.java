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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.hash.Hashing;

/**
 * This test verifies the correctness of the Per Column Family flushing strategy
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestPerColumnFamilyFlush {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPerColumnFamilyFlush.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestPerColumnFamilyFlush.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final Path DIR = TEST_UTIL.getDataTestDir("TestHRegion");

  public static final TableName TABLENAME = TableName.valueOf("TestPerColumnFamilyFlush", "t1");

  public static final byte[][] FAMILIES = { Bytes.toBytes("f1"), Bytes.toBytes("f2"),
      Bytes.toBytes("f3"), Bytes.toBytes("f4"), Bytes.toBytes("f5") };

  public static final byte[] FAMILY1 = FAMILIES[0];

  public static final byte[] FAMILY2 = FAMILIES[1];

  public static final byte[] FAMILY3 = FAMILIES[2];

  private HRegion initHRegion(String callingMethod, Configuration conf) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLENAME);
    for (byte[] family : FAMILIES) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    RegionInfo info = RegionInfoBuilder.newBuilder(TABLENAME).build();
    Path path = new Path(DIR, callingMethod);
    return HBaseTestingUtil.createRegionAndWAL(info, path, conf, builder.build());
  }

  // A helper function to create puts.
  private Put createPut(int familyNum, int putNum) {
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    p.addColumn(FAMILIES[familyNum - 1], qf, val);
    return p;
  }

  // A helper function to create puts.
  private Get createGet(int familyNum, int putNum) {
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    return new Get(row);
  }

  // A helper function to verify edits.
  void verifyEdit(int familyNum, int putNum, Table table) throws IOException {
    Result r = table.get(createGet(familyNum, putNum));
    byte[] family = FAMILIES[familyNum - 1];
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum), r.getFamilyMap(family));
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
      r.getFamilyMap(family).get(qf));
    assertTrue(("Incorrect value for Put#" + putNum + " for CF# " + familyNum),
      Arrays.equals(r.getFamilyMap(family).get(qf), val));
  }

  @Test
  public void testSelectiveFlushWhenEnabled() throws IOException {
    // Set up the configuration, use new one to not conflict with minicluster in other tests
    Configuration conf = new HBaseTestingUtil().getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllLargeStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
        40 * 1024);
    // Intialize the region
    HRegion region = initHRegion("testSelectiveFlushWithDataCompaction", conf);
    // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));

      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }

    long totalMemstoreSize = region.getMemStoreDataSize();

    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1 = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2 = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3 = region.getOldestSeqIdOfStore(FAMILY3);

    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstore = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1, smallestSeqInRegionCurrentMemstore);

    // Some other sanity checks.
    assertTrue(smallestSeqCF1 < smallestSeqCF2);
    assertTrue(smallestSeqCF2 < smallestSeqCF3);
    assertTrue(cf1MemstoreSize.getDataSize() > 0);
    assertTrue(cf2MemstoreSize.getDataSize() > 0);
    assertTrue(cf3MemstoreSize.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSize, cf1MemstoreSize.getDataSize() + cf2MemstoreSize.getDataSize()
        + cf3MemstoreSize.getDataSize());

    // Flush!
    region.flush(false);

    // Will use these to check if anything changed.
    MemStoreSize oldCF2MemstoreSize = cf2MemstoreSize;
    MemStoreSize oldCF3MemstoreSize = cf3MemstoreSize;

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemStoreDataSize();
    smallestSeqInRegionCurrentMemstore = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // We should have cleared out only CF1, since we chose the flush thresholds
    // and number of puts accordingly.
    assertEquals(0, cf1MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf1MemstoreSize.getHeapSize());
    // Nothing should have happened to CF2, ...
    assertEquals(cf2MemstoreSize, oldCF2MemstoreSize);
    // ... or CF3
    assertEquals(cf3MemstoreSize, oldCF3MemstoreSize);
    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF2.
    assertEquals(smallestSeqInRegionCurrentMemstore, smallestSeqCF2);
    // Of course, this should hold too.
    assertEquals(totalMemstoreSize, cf2MemstoreSize.getDataSize() + cf3MemstoreSize.getDataSize());

    // Now add more puts (mostly for CF2), so that we only flush CF2 this time.
    for (int i = 1200; i < 2400; i++) {
      region.put(createPut(2, i));

      // Add only 100 puts for CF3
      if (i - 1200 < 100) {
        region.put(createPut(3, i));
      }
    }

    // How much does the CF3 memstore occupy? Will be used later.
    oldCF3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();

    // Flush again
    region.flush(false);

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemStoreDataSize();
    smallestSeqInRegionCurrentMemstore = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // CF1 and CF2, both should be absent.
    assertEquals(0, cf1MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf1MemstoreSize.getHeapSize());
    assertEquals(0, cf2MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSize.getHeapSize());
    // CF3 shouldn't have been touched.
    assertEquals(cf3MemstoreSize, oldCF3MemstoreSize);
    assertEquals(totalMemstoreSize, cf3MemstoreSize.getDataSize());

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

    // Clearing the existing memstores.
    region.flush(true);

    // The memstore limit is 200*1024 and the column family flush threshold is
    // around 50*1024. We try to just hit the memstore limit with each CF's
    // memstore being below the CF flush threshold.
    for (int i = 1; i <= 300; i++) {
      region.put(createPut(1, i));
      region.put(createPut(2, i));
      region.put(createPut(3, i));
      region.put(createPut(4, i));
      region.put(createPut(5, i));
    }

    region.flush(false);

    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores.
    assertEquals(0, region.getMemStoreDataSize());
    HBaseTestingUtil.closeRegionAndWAL(region);
  }

  @Test
  public void testSelectiveFlushWhenNotEnabled() throws IOException {
    // Set up the configuration, use new one to not conflict with minicluster in other tests
    Configuration conf = new HBaseTestingUtil().getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllStoresPolicy.class.getName());

    // Intialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushWhenNotEnabled", conf);
    // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }

    long totalMemstoreSize = region.getMemStoreDataSize();

    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSize.getDataSize() > 0);
    assertTrue(cf2MemstoreSize.getDataSize() > 0);
    assertTrue(cf3MemstoreSize.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSize, cf1MemstoreSize.getDataSize() + cf2MemstoreSize.getDataSize()
        + cf3MemstoreSize.getDataSize());

    // Flush!
    region.flush(false);

    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemStoreDataSize();
    long smallestSeqInRegionCurrentMemstore =
        region.getWAL().getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // Everything should have been cleared
    assertEquals(0, cf1MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf1MemstoreSize.getHeapSize());
    assertEquals(0, cf2MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSize.getHeapSize());
    assertEquals(0, cf3MemstoreSize.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf3MemstoreSize.getHeapSize());
    assertEquals(0, totalMemstoreSize);
    assertEquals(HConstants.NO_SEQNUM, smallestSeqInRegionCurrentMemstore);
    HBaseTestingUtil.closeRegionAndWAL(region);
  }

  // Find the (first) region which has the specified name.
  private static Pair<HRegion, HRegionServer> getRegionWithName(TableName tableName) {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (HRegion region : hrs.getRegions(tableName)) {
        return Pair.newPair(region, hrs);
      }
    }
    return null;
  }

  private void doTestLogReplay() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 10000);
    // Carefully chosen limits so that the memstore just flushes when we're done
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllLargeStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 2500);
    final int numRegionServers = 4;
    try {
      TEST_UTIL.startMiniCluster(numRegionServers);
      TEST_UTIL.getAdmin().createNamespace(
        NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
      Table table = TEST_UTIL.createTable(TABLENAME, FAMILIES);

      // Add 100 edits for CF1, 20 for CF2, 20 for CF3.
      // These will all be interleaved in the log.
      for (int i = 1; i <= 80; i++) {
        table.put(createPut(1, i));
        if (i <= 10) {
          table.put(createPut(2, i));
          table.put(createPut(3, i));
        }
      }
      Thread.sleep(1000);

      Pair<HRegion, HRegionServer> desiredRegionAndServer = getRegionWithName(TABLENAME);
      HRegion desiredRegion = desiredRegionAndServer.getFirst();
      assertTrue("Could not find a region which hosts the new region.", desiredRegion != null);

      // Flush the region selectively.
      desiredRegion.flush(false);

      long totalMemstoreSize;
      long cf1MemstoreSize, cf2MemstoreSize, cf3MemstoreSize;
      totalMemstoreSize = desiredRegion.getMemStoreDataSize();

      // Find the sizes of the memstores of each CF.
      cf1MemstoreSize = desiredRegion.getStore(FAMILY1).getMemStoreSize().getDataSize();
      cf2MemstoreSize = desiredRegion.getStore(FAMILY2).getMemStoreSize().getDataSize();
      cf3MemstoreSize = desiredRegion.getStore(FAMILY3).getMemStoreSize().getDataSize();

      // CF1 Should have been flushed
      assertEquals(0, cf1MemstoreSize);
      // CF2 and CF3 shouldn't have been flushed.
      // TODO: This test doesn't allow for this case:
      // " Since none of the CFs were above the size, flushing all."
      // i.e. a flush happens before we get to here and its a flush-all.
      assertTrue(cf2MemstoreSize >= 0);
      assertTrue(cf3MemstoreSize >= 0);
      assertEquals(totalMemstoreSize, cf2MemstoreSize + cf3MemstoreSize);

      // Wait for the RS report to go across to the master, so that the master
      // is aware of which sequence ids have been flushed, before we kill the RS.
      // If in production, the RS dies before the report goes across, we will
      // safely replay all the edits.
      Thread.sleep(2000);

      // Abort the region server where we have the region hosted.
      HRegionServer rs = desiredRegionAndServer.getSecond();
      rs.abort("testing");

      // The aborted region server's regions will be eventually assigned to some
      // other region server, and the get RPC call (inside verifyEdit()) will
      // retry for some time till the regions come back up.

      // Verify that all the edits are safe.
      for (int i = 1; i <= 80; i++) {
        verifyEdit(1, i, table);
        if (i <= 10) {
          verifyEdit(2, i, table);
          verifyEdit(3, i, table);
        }
      }
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  // Test Log Replay with Distributed log split on.
  @Test
  public void testLogReplayWithDistributedLogSplit() throws Exception {
    doTestLogReplay();
  }

  private WAL getWAL(Region region) {
    return ((HRegion)region).getWAL();
  }

  private int getNumRolledLogFiles(Region region) {
    return AbstractFSWALProvider.getNumRolledLogFiles(getWAL(region));
  }

  /**
   * When a log roll is about to happen, we do a flush of the regions who will be affected by the
   * log roll. These flushes cannot be a selective flushes, otherwise we cannot roll the logs. This
   * test ensures that we do a full-flush in that scenario.
   */
  @Test
  public void testFlushingWhenLogRolling() throws Exception {
    TableName tableName = TableName.valueOf("testFlushingWhenLogRolling");
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllLargeStoresPolicy.class.getName());
    long cfFlushSizeLowerBound = 2048;
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
      cfFlushSizeLowerBound);

    // One hour, prevent periodic rolling
    conf.setLong("hbase.regionserver.logroll.period", 60L * 60 * 1000);
    // prevent rolling by size
    conf.setLong("hbase.regionserver.hlog.blocksize", 128L * 1024 * 1024);
    // Make it 10 as max logs before a flush comes on.
    final int maxLogs = 10;
    conf.setInt("hbase.regionserver.maxlogs", maxLogs);

    final int numRegionServers = 1;
    TEST_UTIL.startMiniCluster(numRegionServers);
    try {
      Table table = TEST_UTIL.createTable(tableName, FAMILIES);
      Pair<HRegion, HRegionServer> desiredRegionAndServer = getRegionWithName(tableName);
      final HRegion desiredRegion = desiredRegionAndServer.getFirst();
      assertTrue("Could not find a region which hosts the new region.", desiredRegion != null);
      LOG.info("Writing to region=" + desiredRegion);

      // Add one row for both CFs.
      for (int i = 1; i <= 3; i++) {
        table.put(createPut(i, 0));
      }
      // Now only add row to CF1, make sure when we force a flush, CF1 is larger than the lower
      // bound and CF2 and CF3 are smaller than the lower bound.
      for (int i = 0; i < maxLogs; i++) {
        for (int j = 0; j < 100; j++) {
          table.put(createPut(1, i * 100 + j));
        }
        // Roll the WAL. The log file count is less than maxLogs so no flush is triggered.
        int currentNumRolledLogFiles = getNumRolledLogFiles(desiredRegion);
        assertNull(getWAL(desiredRegion).rollWriter());
        while (getNumRolledLogFiles(desiredRegion) <= currentNumRolledLogFiles) {
          Thread.sleep(100);
        }
      }
      assertEquals(maxLogs, getNumRolledLogFiles(desiredRegion));
      assertTrue(desiredRegion.getStore(FAMILY1).getMemStoreSize().getHeapSize() > cfFlushSizeLowerBound);
      assertTrue(desiredRegion.getStore(FAMILY2).getMemStoreSize().getHeapSize() < cfFlushSizeLowerBound);
      assertTrue(desiredRegion.getStore(FAMILY3).getMemStoreSize().getHeapSize() < cfFlushSizeLowerBound);
      table.put(createPut(1, 12345678));
      // Make numRolledLogFiles greater than maxLogs
      desiredRegionAndServer.getSecond().getWalRoller().requestRollAll();
      // Wait for some time till the flush caused by log rolling happens.
      TEST_UTIL.waitFor(30000, new Waiter.ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          return desiredRegion.getMemStoreDataSize() == 0;
        }

        @Override
        public String explainFailure() throws Exception {
          long memstoreSize = desiredRegion.getMemStoreDataSize();
          if (memstoreSize > 0) {
            return "Still have unflushed entries in memstore, memstore size is " + memstoreSize;
          }
          return "Unknown";
        }
      });
      LOG.info("Finished waiting on flush after too many WALs...");
      // Individual families should have been flushed.
      assertEquals(MutableSegment.DEEP_OVERHEAD,
        desiredRegion.getStore(FAMILY1).getMemStoreSize().getHeapSize());
      assertEquals(MutableSegment.DEEP_OVERHEAD,
        desiredRegion.getStore(FAMILY2).getMemStoreSize().getHeapSize());
      assertEquals(MutableSegment.DEEP_OVERHEAD,
        desiredRegion.getStore(FAMILY3).getMemStoreSize().getHeapSize());
      // let WAL cleanOldLogs
      assertNull(getWAL(desiredRegion).rollWriter(true));
      assertTrue(getNumRolledLogFiles(desiredRegion) < maxLogs);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private void doPut(Table table, long memstoreFlushSize) throws IOException, InterruptedException {
    Region region = getRegionWithName(table.getName()).getFirst();
    // cf1 4B per row, cf2 40B per row and cf3 400B per row
    byte[] qf = Bytes.toBytes("qf");
    for (int i = 0; i < 10000; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      byte[] value1 = new byte[100];
      Bytes.random(value1);
      put.addColumn(FAMILY1, qf, value1);
      byte[] value2 = new byte[200];
      Bytes.random(value2);
      put.addColumn(FAMILY2, qf, value2);
      byte[] value3 = new byte[400];
      Bytes.random(value3);
      put.addColumn(FAMILY3, qf, value3);
      table.put(put);
      // slow down to let regionserver flush region.
      while (region.getMemStoreHeapSize() > memstoreFlushSize) {
        Thread.sleep(100);
      }
    }
  }

  // Under the same write load, small stores should have less store files when
  // percolumnfamilyflush enabled.
  @Test
  public void testCompareStoreFileCount() throws Exception {
    long memstoreFlushSize = 1024L * 1024;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, memstoreFlushSize);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllStoresPolicy.class.getName());
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
      .setCompactionEnabled(false).setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY1))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY2))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY3)).build();

    LOG.info("==============Test with selective flush disabled===============");
    int cf1StoreFileCount = -1;
    int cf2StoreFileCount = -1;
    int cf3StoreFileCount = -1;
    int cf1StoreFileCount1 = -1;
    int cf2StoreFileCount1 = -1;
    int cf3StoreFileCount1 = -1;
    try {
      TEST_UTIL.startMiniCluster(1);
      TEST_UTIL.getAdmin().createNamespace(
        NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
      TEST_UTIL.getAdmin().createTable(tableDescriptor);
      TEST_UTIL.waitTableAvailable(TABLENAME);
      Connection conn = ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(TABLENAME);
      doPut(table, memstoreFlushSize);
      table.close();
      conn.close();

      Region region = getRegionWithName(TABLENAME).getFirst();
      cf1StoreFileCount = region.getStore(FAMILY1).getStorefilesCount();
      cf2StoreFileCount = region.getStore(FAMILY2).getStorefilesCount();
      cf3StoreFileCount = region.getStore(FAMILY3).getStorefilesCount();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }

    LOG.info("==============Test with selective flush enabled===============");
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllLargeStoresPolicy.class.getName());
    // default value of per-cf flush lower bound is too big, set to a small enough value
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 0);
    try {
      TEST_UTIL.startMiniCluster(1);
      TEST_UTIL.getAdmin().createNamespace(
        NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
      TEST_UTIL.getAdmin().createTable(tableDescriptor);
      Connection conn = ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(TABLENAME);
      doPut(table, memstoreFlushSize);
      table.close();
      conn.close();

      Region region = getRegionWithName(TABLENAME).getFirst();
      cf1StoreFileCount1 = region.getStore(FAMILY1).getStorefilesCount();
      cf2StoreFileCount1 = region.getStore(FAMILY2).getStorefilesCount();
      cf3StoreFileCount1 = region.getStore(FAMILY3).getStorefilesCount();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }

    LOG.info("disable selective flush: " + Bytes.toString(FAMILY1) + "=>" + cf1StoreFileCount
        + ", " + Bytes.toString(FAMILY2) + "=>" + cf2StoreFileCount + ", "
        + Bytes.toString(FAMILY3) + "=>" + cf3StoreFileCount);
    LOG.info("enable selective flush: " + Bytes.toString(FAMILY1) + "=>" + cf1StoreFileCount1
        + ", " + Bytes.toString(FAMILY2) + "=>" + cf2StoreFileCount1 + ", "
        + Bytes.toString(FAMILY3) + "=>" + cf3StoreFileCount1);
    // small CF will have less store files.
    assertTrue(cf1StoreFileCount1 < cf1StoreFileCount);
    assertTrue(cf2StoreFileCount1 < cf2StoreFileCount);
  }

  public static void main(String[] args) throws Exception {
    int numRegions = Integer.parseInt(args[0]);
    long numRows = Long.parseLong(args[1]);

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
      .setMaxFileSize(10L * 1024 * 1024 * 1024)
      .setValue(TableDescriptorBuilder.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY1))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY2))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY3)).build();

    Configuration conf = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = conn.getAdmin();
    if (admin.tableExists(TABLENAME)) {
      admin.disableTable(TABLENAME);
      admin.deleteTable(TABLENAME);
    }
    if (numRegions >= 3) {
      byte[] startKey = new byte[16];
      byte[] endKey = new byte[16];
      Arrays.fill(endKey, (byte) 0xFF);
      admin.createTable(tableDescriptor, startKey, endKey, numRegions);
    } else {
      admin.createTable(tableDescriptor);
    }
    admin.close();

    Table table = conn.getTable(TABLENAME);
    byte[] qf = Bytes.toBytes("qf");
    byte[] value1 = new byte[16];
    byte[] value2 = new byte[256];
    byte[] value3 = new byte[4096];
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Hashing.md5().hashLong(i).asBytes());
      Bytes.random(value1);
      Bytes.random(value2);
      Bytes.random(value3);
      put.addColumn(FAMILY1, qf, value1);
      put.addColumn(FAMILY2, qf, value2);
      put.addColumn(FAMILY3, qf, value3);
      table.put(put);
      if (i % 10000 == 0) {
        LOG.info(i + " rows put");
      }
    }
    table.close();
    conn.close();
  }
}
