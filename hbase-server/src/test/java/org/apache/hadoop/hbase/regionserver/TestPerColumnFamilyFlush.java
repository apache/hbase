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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.FlushAllStoresPolicy;
import org.apache.hadoop.hbase.regionserver.FlushLargeStoresPolicy;
import org.apache.hadoop.hbase.regionserver.FlushPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.hash.Hashing;

/**
 * This test verifies the correctness of the Per Column Family flushing strategy
 */
@Category(LargeTests.class)
public class TestPerColumnFamilyFlush {
  private static final Log LOG = LogFactory.getLog(TestPerColumnFamilyFlush.class);

  HRegion region = null;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final Path DIR = TEST_UTIL.getDataTestDir("TestHRegion");

  public static final TableName TABLENAME = TableName.valueOf("TestPerColumnFamilyFlush", "t1");

  public static final byte[][] families = { Bytes.toBytes("f1"), Bytes.toBytes("f2"),
      Bytes.toBytes("f3"), Bytes.toBytes("f4"), Bytes.toBytes("f5") };

  public static final byte[] FAMILY1 = families[0];

  public static final byte[] FAMILY2 = families[1];

  public static final byte[] FAMILY3 = families[2];

  private void initHRegion(String callingMethod, Configuration conf) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(TABLENAME, null, null, false);
    Path path = new Path(DIR, callingMethod);
    region = HRegion.createHRegion(info, path, conf, htd);
  }

  // A helper function to create puts.
  private Put createPut(int familyNum, int putNum) {
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    p.add(families[familyNum - 1], qf, val);
    return p;
  }

  // A helper function to create puts.
  private Get createGet(int familyNum, int putNum) {
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    return new Get(row);
  }

  // A helper function to verify edits.
  void verifyEdit(int familyNum, int putNum, HTable table) throws IOException {
    Result r = table.get(createGet(familyNum, putNum));
    byte[] family = families[familyNum - 1];
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
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
      FlushLargeStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND,
      100 * 1024);
    // Intialize the HRegion
    initHRegion("testSelectiveFlushWhenEnabled", conf);
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

    long totalMemstoreSize = region.getMemstoreSize().get();

    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1 = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2 = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3 = region.getOldestSeqIdOfStore(FAMILY3);

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstore =
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1, smallestSeqInRegionCurrentMemstore);

    // Some other sanity checks.
    assertTrue(smallestSeqCF1 < smallestSeqCF2);
    assertTrue(smallestSeqCF2 < smallestSeqCF3);
    assertTrue(cf1MemstoreSize > 0);
    assertTrue(cf2MemstoreSize > 0);
    assertTrue(cf3MemstoreSize > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSize + 3 * DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize
        + cf2MemstoreSize + cf3MemstoreSize);

    // Flush!
    region.flushcache(false);

    // Will use these to check if anything changed.
    long oldCF2MemstoreSize = cf2MemstoreSize;
    long oldCF3MemstoreSize = cf3MemstoreSize;

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    smallestSeqInRegionCurrentMemstore =
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // We should have cleared out only CF1, since we chose the flush thresholds
    // and number of puts accordingly.
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize);
    // Nothing should have happened to CF2, ...
    assertEquals(cf2MemstoreSize, oldCF2MemstoreSize);
    // ... or CF3
    assertEquals(cf3MemstoreSize, oldCF3MemstoreSize);
    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF2.
    assertEquals(smallestSeqInRegionCurrentMemstore, smallestSeqCF2);
    // Of course, this should hold too.
    assertEquals(totalMemstoreSize + 2 * DefaultMemStore.DEEP_OVERHEAD, cf2MemstoreSize
        + cf3MemstoreSize);

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
    region.flushcache(false);

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    smallestSeqInRegionCurrentMemstore =
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // CF1 and CF2, both should be absent.
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf2MemstoreSize);
    // CF3 shouldn't have been touched.
    assertEquals(cf3MemstoreSize, oldCF3MemstoreSize);
    assertEquals(totalMemstoreSize + DefaultMemStore.DEEP_OVERHEAD, cf3MemstoreSize);
    assertEquals(smallestSeqInRegionCurrentMemstore, smallestSeqCF3);

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

    // Clearing the existing memstores.
    region.flushcache(true);

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

    region.flushcache(false);
    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores.
    assertEquals(0, region.getMemstoreSize().get());
  }

  @Test
  public void testSelectiveFlushWhenNotEnabled() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllStoresPolicy.class.getName());

    // Intialize the HRegion
    initHRegion("testSelectiveFlushWhenNotEnabled", conf);
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

    long totalMemstoreSize = region.getMemstoreSize().get();

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSize > 0);
    assertTrue(cf2MemstoreSize > 0);
    assertTrue(cf3MemstoreSize > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSize + 3 * DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize
        + cf2MemstoreSize + cf3MemstoreSize);

    // Flush!
    region.flushcache(false);

    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    long smallestSeqInRegionCurrentMemstore =
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    // Everything should have been cleared
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf2MemstoreSize);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf3MemstoreSize);
    assertEquals(0, totalMemstoreSize);
    assertEquals(HConstants.NO_SEQNUM, smallestSeqInRegionCurrentMemstore);
  }

  // Find the (first) region which has the specified name.
  private static Pair<HRegion, HRegionServer> getRegionWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (HRegion region : hrs.getOnlineRegions(tableName)) {
        return Pair.newPair(region, hrs);
      }
    }
    return null;
  }

  @Test
  public void testLogReplay() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 20000);
    // Carefully chosen limits so that the memstore just flushes when we're done
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
      FlushLargeStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND, 10000);
    final int numRegionServers = 4;
    TEST_UTIL.startMiniCluster(numRegionServers);
    TEST_UTIL.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
    HTable table = TEST_UTIL.createTable(TABLENAME, families);
    HTableDescriptor htd = table.getTableDescriptor();

    for (byte[] family : families) {
      if (!htd.hasFamily(family)) {
        htd.addFamily(new HColumnDescriptor(family));
      }
    }

    // Add 100 edits for CF1, 20 for CF2, 20 for CF3.
    // These will all be interleaved in the log.
    for (int i = 1; i <= 80; i++) {
      table.put(createPut(1, i));
      if (i <= 10) {
        table.put(createPut(2, i));
        table.put(createPut(3, i));
      }
    }
    table.flushCommits();
    Thread.sleep(1000);

    Pair<HRegion, HRegionServer> desiredRegionAndServer = getRegionWithName(TABLENAME);
    HRegion desiredRegion = desiredRegionAndServer.getFirst();
    assertTrue("Could not find a region which hosts the new region.", desiredRegion != null);

    // Flush the region selectively.
    desiredRegion.flushcache(false);

    long totalMemstoreSize;
    long cf1MemstoreSize, cf2MemstoreSize, cf3MemstoreSize;
    totalMemstoreSize = desiredRegion.getMemstoreSize().get();

    // Find the sizes of the memstores of each CF.
    cf1MemstoreSize = desiredRegion.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = desiredRegion.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = desiredRegion.getStore(FAMILY3).getMemStoreSize();

    // CF1 Should have been flushed
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, cf1MemstoreSize);
    // CF2 and CF3 shouldn't have been flushed.
    assertTrue(cf2MemstoreSize > 0);
    assertTrue(cf3MemstoreSize > 0);
    assertEquals(totalMemstoreSize + 2 * DefaultMemStore.DEEP_OVERHEAD, cf2MemstoreSize
        + cf3MemstoreSize);

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

    TEST_UTIL.shutdownMiniCluster();
  }

  // Test Log Replay with Distributed Replay on.
  // In distributed log replay, the log splitters ask the master for the
  // last flushed sequence id for a region. This test would ensure that we
  // are doing the book-keeping correctly.
  @Test
  public void testLogReplayWithDistributedReplay() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    testLogReplay();
  }

  /**
   * When a log roll is about to happen, we do a flush of the regions who will be affected by the
   * log roll. These flushes cannot be a selective flushes, otherwise we cannot roll the logs. This
   * test ensures that we do a full-flush in that scenario.
   * @throws IOException
   */
  @Test
  public void testFlushingWhenLogRolling() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300000);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
      FlushLargeStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND, 100000);

    // Also, let us try real hard to get a log roll to happen.
    // Keeping the log roll period to 2s.
    conf.setLong("hbase.regionserver.logroll.period", 2000);
    // Keep the block size small so that we fill up the log files very fast.
    conf.setLong("hbase.regionserver.hlog.blocksize", 6144);
    int maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);

    final int numRegionServers = 4;
    TEST_UTIL.startMiniCluster(numRegionServers);
    TEST_UTIL.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
    HTable table = TEST_UTIL.createTable(TABLENAME, families);
    HTableDescriptor htd = table.getTableDescriptor();

    for (byte[] family : families) {
      if (!htd.hasFamily(family)) {
        htd.addFamily(new HColumnDescriptor(family));
      }
    }

    HRegion desiredRegion = getRegionWithName(TABLENAME).getFirst();
    assertTrue("Could not find a region which hosts the new region.", desiredRegion != null);

    // Add some edits. Most will be for CF1, some for CF2 and CF3.
    for (int i = 1; i <= 10000; i++) {
      table.put(createPut(1, i));
      if (i <= 200) {
        table.put(createPut(2, i));
        table.put(createPut(3, i));
      }
      table.flushCommits();
      // Keep adding until we exceed the number of log files, so that we are
      // able to trigger the cleaning of old log files.
      int currentNumLogFiles = ((FSHLog) (desiredRegion.getWAL())).getNumLogFiles();
      if (currentNumLogFiles > maxLogs) {
        LOG.info("The number of log files is now: " + currentNumLogFiles
            + ". Expect a log roll and memstore flush.");
        break;
      }
    }
    table.close();
    // Wait for some time till the flush caused by log rolling happens.
    Thread.sleep(4000);

    // We have artificially created the conditions for a log roll. When a
    // log roll happens, we should flush all the column families. Testing that
    // case here.

    // Individual families should have been flushed.
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, desiredRegion.getStore(FAMILY1).getMemStoreSize());
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, desiredRegion.getStore(FAMILY2).getMemStoreSize());
    assertEquals(DefaultMemStore.DEEP_OVERHEAD, desiredRegion.getStore(FAMILY3).getMemStoreSize());

    // And of course, the total memstore should also be clean.
    assertEquals(0, desiredRegion.getMemstoreSize().get());

    TEST_UTIL.shutdownMiniCluster();
  }

  private void doPut(HTableInterface table) throws IOException {
    // cf1 4B per row, cf2 40B per row and cf3 400B per row
    byte[] qf = Bytes.toBytes("qf");
    Random rand = new Random();
    byte[] value1 = new byte[100];
    byte[] value2 = new byte[200];
    byte[] value3 = new byte[400];
    for (int i = 0; i < 10000; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      rand.setSeed(i);
      rand.nextBytes(value1);
      rand.nextBytes(value2);
      rand.nextBytes(value3);
      put.add(FAMILY1, qf, value1);
      put.add(FAMILY2, qf, value2);
      put.add(FAMILY3, qf, value3);
      table.put(put);
    }
  }

  // Under the same write load, small stores should have less store files when
  // percolumnfamilyflush enabled.
  @Test
  public void testCompareStoreFileCount() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushAllStoresPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND,
      400 * 1024);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());

    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    htd.setCompactionEnabled(false);
    htd.addFamily(new HColumnDescriptor(FAMILY1));
    htd.addFamily(new HColumnDescriptor(FAMILY2));
    htd.addFamily(new HColumnDescriptor(FAMILY3));

    LOG.info("==============Test with selective flush disabled===============");
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    getRegionWithName(TABLENAME).getFirst();
    HConnection conn = HConnectionManager.createConnection(conf);
    HTableInterface table = conn.getTable(TABLENAME);
    doPut(table);
    table.close();
    conn.close();

    HRegion region = getRegionWithName(TABLENAME).getFirst();
    int cf1StoreFileCount = region.getStore(FAMILY1).getStorefilesCount();
    int cf2StoreFileCount = region.getStore(FAMILY2).getStorefilesCount();
    int cf3StoreFileCount = region.getStore(FAMILY3).getStorefilesCount();
    TEST_UTIL.shutdownMiniCluster();

    LOG.info("==============Test with selective flush enabled===============");
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
      FlushLargeStoresPolicy.class.getName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(TABLENAME.getNamespaceAsString()).build());
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    conn = HConnectionManager.createConnection(conf);
    table = conn.getTable(TABLENAME);
    doPut(table);
    table.close();
    conn.close();

    region = getRegionWithName(TABLENAME).getFirst();
    int cf1StoreFileCount1 = region.getStore(FAMILY1).getStorefilesCount();
    int cf2StoreFileCount1 = region.getStore(FAMILY2).getStorefilesCount();
    int cf3StoreFileCount1 = region.getStore(FAMILY3).getStorefilesCount();
    TEST_UTIL.shutdownMiniCluster();

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

    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    htd.setMaxFileSize(10L * 1024 * 1024 * 1024);
    htd.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
    htd.addFamily(new HColumnDescriptor(FAMILY1));
    htd.addFamily(new HColumnDescriptor(FAMILY2));
    htd.addFamily(new HColumnDescriptor(FAMILY3));

    Configuration conf = HBaseConfiguration.create();
    HConnection conn = HConnectionManager.createConnection(conf);
    HBaseAdmin admin = new HBaseAdmin(conn);
    if (admin.tableExists(TABLENAME)) {
      admin.disableTable(TABLENAME);
      admin.deleteTable(TABLENAME);
    }
    if (numRegions >= 3) {
      byte[] startKey = new byte[16];
      byte[] endKey = new byte[16];
      Arrays.fill(endKey, (byte) 0xFF);
      admin.createTable(htd, startKey, endKey, numRegions);
    } else {
      admin.createTable(htd);
    }
    admin.close();

    HTableInterface table = conn.getTable(TABLENAME);
    byte[] qf = Bytes.toBytes("qf");
    Random rand = new Random();
    byte[] value1 = new byte[16];
    byte[] value2 = new byte[256];
    byte[] value3 = new byte[4096];
    for (long i = 0; i < numRows; i++) {
      Put put = new Put(Hashing.md5().hashLong(i).asBytes());
      rand.setSeed(i);
      rand.nextBytes(value1);
      rand.nextBytes(value2);
      rand.nextBytes(value3);
      put.add(FAMILY1, qf, value1);
      put.add(FAMILY2, qf, value2);
      put.add(FAMILY3, qf, value3);
      table.put(put);
      if (i % 10000 == 0) {
        LOG.info(i + " rows put");
      }
    }
    table.close();
    conn.close();
  }
}
