/*
 * Copyright 2013 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;

/**
 * This test verifies the correctness of the Per Column Family flushing strategy
 */
public class TestPerColumnFamilyFlush extends TestCase {
  private static final Log LOG =
          LogFactory.getLog(TestPerColumnFamilyFlush.class);
  HRegion region = null;
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getTestDir() +
          "/TestHRegion/";

  public static final String TABLENAME_STR = "t1";
  public static final byte[] TABLENAME = Bytes.toBytes("t1");
  public static final byte[][] families = { Bytes.toBytes("f1"),
          Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4"),
          Bytes.toBytes("f5") };
  public static final byte[] FAMILY1 = families[0];
  public static final byte[] FAMILY2 = families[1];
  public static final byte[] FAMILY3 = families[2];

  private void initHRegion (String callingMethod, Configuration conf)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf);
  }

  // A helper function to create puts.
  Put createPut(int familyNum, int putNum) {
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    p.add(families[familyNum - 1], qf, val);
    return p;
  }

  // A helper function to create puts.
  Get createGet(int familyNum, int putNum) {
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    return new Get(row);
  }

  // A helper function to verify edits.
  void verifyEdit(int familyNum, int putNum, HTable table) throws IOException {
    Result r = table.get(createGet(familyNum, putNum));
    byte[] family = families[familyNum - 1];
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
            r.getFamilyMap(family));
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
            r.getFamilyMap(family).get(qf));
    assertTrue(("Incorrect value for Put#" + putNum + " for CF# " + familyNum),
            Arrays.equals(r.getFamilyMap(family).get(qf), val));
  }

  @Test
  public void testSelectiveFlushWhenEnabled() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200*1024);
    conf.setBoolean(HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH, true);
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, 100*1024);

    // Initialize the HRegion
    initHRegion(getName(), conf);
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
    long smallestSeqCF1 =
            region.getStore(FAMILY1).getSmallestSeqNumberInMemstore();
    long smallestSeqCF2 =
            region.getStore(FAMILY2).getSmallestSeqNumberInMemstore();
    long smallestSeqCF3 =
            region.getStore(FAMILY3).getSmallestSeqNumberInMemstore();

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSize =
            region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSize =
            region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSize =
            region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstore =
            region.getLog().
                    getFirstSeqWrittenInCurrentMemstoreForRegion(region);

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
    assertTrue(totalMemstoreSize ==
                (cf1MemstoreSize + cf2MemstoreSize + cf3MemstoreSize));

    // Flush!
    region.flushcache(true);

    // Will use these to check if anything changed.
    long oldCF2MemstoreSize = cf2MemstoreSize;
    long oldCF3MemstoreSize = cf3MemstoreSize;

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    smallestSeqInRegionCurrentMemstore = region.getLog()
            .getFirstSeqWrittenInCurrentMemstoreForRegion(region);

    // We should have cleared out only CF1, since we chose the flush thresholds
    // and number of puts accordingly.
    assertEquals(0, cf1MemstoreSize);
    // Nothing should have happened to CF2, ...
    assertTrue(cf2MemstoreSize == oldCF2MemstoreSize);
    // ... or CF3
    assertTrue(cf3MemstoreSize == oldCF3MemstoreSize);
    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF2.
    assertTrue(smallestSeqInRegionCurrentMemstore == smallestSeqCF2);
    // Of course, this should hold too.
    assertTrue(totalMemstoreSize == (cf2MemstoreSize + cf3MemstoreSize));

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
    region.flushcache(true);

    // Recalculate everything
    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    smallestSeqInRegionCurrentMemstore = region.getLog()
            .getFirstSeqWrittenInCurrentMemstoreForRegion(region);

    // CF1 and CF2, both should be absent.
    assertEquals(0, cf1MemstoreSize);
    assertEquals(0, cf2MemstoreSize);
    // CF3 shouldn't have been touched.
    assertTrue(cf3MemstoreSize == oldCF3MemstoreSize);
    assertTrue(totalMemstoreSize == cf3MemstoreSize);
    assertTrue(smallestSeqInRegionCurrentMemstore == smallestSeqCF3);

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

    // Clearing the existing memstores.
    region.flushcache(false);

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

    region.flushcache(true);
    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores.
    Assert.assertEquals(0, region.getMemstoreSize().get());
  }

  public void testSelectiveFlushWithThreshold(long t) throws IOException {
    /*         t->|
     * Phase 1    |     |     |
     *            +-----+-----+
     *           FM1   FM2   FM3
     */
    // flush all
    region.flushcache(false);
    region.put(createPut(2, 1));
    region.put(createPut(3, 1));


    int cnt = 0;
    while (region.getStore(FAMILY1).getMemStoreSize() <= t) {
      region.put(createPut(1, cnt));
      cnt ++;
    }

    final long sizeFamily1_1 = region.getStore(FAMILY1).getMemStoreSize();
    final long sizeFamily2_1 = region.getStore(FAMILY2).getMemStoreSize();
    final long sizeFamily3_1 = region.getStore(FAMILY3).getMemStoreSize();

    Assert.assertTrue(sizeFamily1_1 > t);
    Assert.assertTrue(sizeFamily2_1 > 0);
    Assert.assertTrue(sizeFamily3_1 > 0);

    /*         t->
     * Phase 2          |     |
     *            +-----+-----+
     *           FM1   FM2   FM3
     */
    // flush, should only flush family 1
    region.flushcache(true);

    final long sizeFamily1_2 = region.getStore(FAMILY1).getMemStoreSize();
    final long sizeFamily2_2 = region.getStore(FAMILY2).getMemStoreSize();
    final long sizeFamily3_2 = region.getStore(FAMILY3).getMemStoreSize();

    // should clear FAMILY1 only
    Assert.assertEquals("sizeFamily1", 0, sizeFamily1_2);
    Assert.assertEquals("sizeFamily2", sizeFamily2_1, sizeFamily2_2);
    Assert.assertEquals("sizeFamily3", sizeFamily3_1, sizeFamily3_2);
    /*         t->,
     * Phase 3    |     |     |
     *            +-----+-----+
     *           FM1   FM2   FM3
     */
    // flush all
    region.flushcache(false);

    region.put(createPut(2, 1));
    region.put(createPut(3, 1));
    // reduce cnt to 1 to make the size less than t
    cnt--;
    for (int i = 0; i < cnt; i ++) {
      region.put(createPut(1, i));
    }

    final long sizeFamily1_3 = region.getStore(FAMILY1).getMemStoreSize();
    final long sizeFamily2_3 = region.getStore(FAMILY2).getMemStoreSize();
    final long sizeFamily3_3 = region.getStore(FAMILY3).getMemStoreSize();

    Assert.assertTrue("sizeFamily1 < t", sizeFamily1_3 < t);
    Assert.assertEquals("sizeFamily2", sizeFamily2_1, sizeFamily2_3);
    Assert.assertEquals("sizeFamily3", sizeFamily3_1, sizeFamily3_3);
    /*         t->
     * Phase 3
     *            +-----+-----+
     *           FM1   FM2   FM3
     */
    // flush, should flush all
    region.flushcache(true);

    final long sizeFamily1_4 = region.getStore(FAMILY1).getMemStoreSize();
    final long sizeFamily2_4 = region.getStore(FAMILY2).getMemStoreSize();
    final long sizeFamily3_4 = region.getStore(FAMILY3).getMemStoreSize();
    Assert.assertEquals("sizeFamily1", 0, sizeFamily1_4);
    Assert.assertEquals("sizeFamily2", 0, sizeFamily2_4);
    Assert.assertEquals("sizeFamily3", 0, sizeFamily3_4);
  }

  @Test
  public void testConfigueChange() throws IOException {
    final int T_init = 100*1024;
    final int T_1 = 200*1024;
    final int T_2 = 50*1024;

    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200*1024);
    conf.setBoolean(HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH, true);
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, T_init);

    // Initialize the HRegion
    initHRegion(getName(), conf);

    // test for threshold of T_init
    this.testSelectiveFlushWithThreshold(T_init);
    // test for threshold of T_1
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, T_1);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    this.testSelectiveFlushWithThreshold(T_1);
    // test for threshold of T_2
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, T_2);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    this.testSelectiveFlushWithThreshold(T_2);
  }

  @Test
  public void testSelectiveFlushWhenNotEnabled() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 200 * 1024);
    conf.setBoolean(HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH, false);
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, 100 * 1024);

    // Initialize the HRegion
    initHRegion(getName(), conf);
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
    long smallestSeqCF1 =
            region.getStore(FAMILY1).getSmallestSeqNumberInMemstore();
    long smallestSeqCF2 =
            region.getStore(FAMILY2).getSmallestSeqNumberInMemstore();
    long smallestSeqCF3 =
            region.getStore(FAMILY3).getSmallestSeqNumberInMemstore();

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSize =
            region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSize =
            region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSize =
            region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstore =
            region.getLog().
                    getFirstSeqWrittenInCurrentMemstoreForRegion(region);

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
    assertTrue(totalMemstoreSize ==
            (cf1MemstoreSize + cf2MemstoreSize + cf3MemstoreSize));

    // Flush!
    region.flushcache(true);

    cf1MemstoreSize = region.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = region.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = region.getStore(FAMILY3).getMemStoreSize();
    totalMemstoreSize = region.getMemstoreSize().get();
    smallestSeqInRegionCurrentMemstore = region.getLog()
            .getFirstSeqWrittenInCurrentMemstoreForRegion(region);

    // Everything should have been cleared
    assertEquals(0, cf1MemstoreSize);
    assertEquals(0, cf2MemstoreSize);
    assertEquals(0, cf3MemstoreSize);
    assertEquals(0, totalMemstoreSize);
    assertEquals(Long.MAX_VALUE, smallestSeqInRegionCurrentMemstore);
  }

  // Find the (first) region which has a name starting with a particular prefix.
  private HRegion getRegionWithNameStartingWith(String regionPrefix) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts =
      cluster.getRegionServerThreads();
    int rsIndexContainingOurRegion = -1;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (HRegion region : hrs.getOnlineRegions()) {
        if (region.getRegionNameAsString().startsWith(regionPrefix)) {
          if (rsIndexContainingOurRegion == -1) {
            return region;
          }
        }
      }
    }
    return null;
  }

  @Test
  public void testLogReplay() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH, true);
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 20000);
    // Carefully chosen limits so that the memstore just flushes when we're done
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, 10000);
    final int numRegionServers = 4;
    try {
      TEST_UTIL.startMiniCluster(numRegionServers);
    } catch (Exception e) {
      LOG.error("Could not start the mini cluster. Terminating.");
      e.printStackTrace();
      throw e;
    }

    TEST_UTIL.createTable(TABLENAME, families);
    HTable table = new HTable(conf, TABLENAME);
    HTableDescriptor htd = table.getTableDescriptor();

    for (byte [] family : families) {
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
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw e;
    }

    HRegion desiredRegion = getRegionWithNameStartingWith(TABLENAME_STR);
    assertTrue("Could not find a region which hosts the new region.",
      desiredRegion != null);

    // Flush the region selectively.
    desiredRegion.flushcache(true);

    long totalMemstoreSize;
    long cf1MemstoreSize, cf2MemstoreSize, cf3MemstoreSize;
    totalMemstoreSize = desiredRegion.getMemstoreSize().get();

    // Find the sizes of the memstores of each CF.
    cf1MemstoreSize = desiredRegion.getStore(FAMILY1).getMemStoreSize();
    cf2MemstoreSize = desiredRegion.getStore(FAMILY2).getMemStoreSize();
    cf3MemstoreSize = desiredRegion.getStore(FAMILY3).getMemStoreSize();

    // CF1 Should have been flushed
    assertEquals(0, cf1MemstoreSize);
    // CF2 and CF3 shouldn't have been flushed.
    assertTrue(cf2MemstoreSize > 0);
    assertTrue(cf3MemstoreSize > 0);
    assertEquals(totalMemstoreSize, cf2MemstoreSize + cf3MemstoreSize);

    // Wait for the RS report to go across to the master, so that the master
    // is aware of which sequence ids have been flushed, before we kill the RS.
    // If in production, the RS dies before the report goes across, we will
    // safely replay all the edits.
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw e;
    }

    // Abort the region server where we have the region hosted.
    HRegionServer rs = desiredRegion.getRegionServer();
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

    try {
      TEST_UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.error("Could not shutdown the mini cluster. Terminating.");
      e.printStackTrace();
      throw e;
    }
  }

  // Test Log Replay with Distributed Splitting on.
  // In distributed log splitting, the log splitters ask the master for the
  // last flushed sequence id for a region. This test would ensure that we
  // are doing the book-keeping correctly.
  @Test
  public void testLogReplayWithDistributedSplitting() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
            HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    testLogReplay();
  }

  /**
   * When a log roll is about to happen, we do a flush of the regions who will
   * be affected by the log roll. These flushes cannot be a selective flushes,
   * otherwise we cannot roll the logs. This test ensures that we do a
   * full-flush in that scenario.
   * @throws IOException
   */
  @Test
  public void testFlushingWhenLogRolling() throws Exception {

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH, true);
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300000);
    conf.setLong(HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE, 100000);

    // Also, let us try real hard to get a log roll to happen.
    // Keeping the log roll period to 2s.
    conf.setLong("hbase.regionserver.logroll.period", 2000);
    // Keep the block size small so that we fill up the log files very fast.
    conf.setLong("hbase.regionserver.hlog.blocksize", 6144);
    int maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);

    final int numRegionServers = 4;
    try {
      TEST_UTIL.startMiniCluster(numRegionServers);
    } catch (Exception e) {
      LOG.error("Could not start the mini cluster. Terminating.");
      e.printStackTrace();
      throw e;
    }

    TEST_UTIL.createTable(TABLENAME, families);
    HTable table = new HTable(conf, TABLENAME);
    HTableDescriptor htd = table.getTableDescriptor();

    for (byte [] family : families) {
      if (!htd.hasFamily(family)) {
        htd.addFamily(new HColumnDescriptor(family));
      }
    }

    HRegion desiredRegion = getRegionWithNameStartingWith(TABLENAME_STR);
    assertTrue("Could not find a region which hosts the new region.",
      desiredRegion != null);

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
      int currentNumLogFiles = desiredRegion.getLog().getNumLogFiles();
      if (currentNumLogFiles > maxLogs) {
        LOG.info("The number of log files is now: " + currentNumLogFiles +
                 ". Expect a log roll and memstore flush.");
        break;
      }
    }

    // Wait for some time till the flush caused by log rolling happens.
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      throw e;
    }

    // We have artificially created the conditions for a log roll. When a
    // log roll happens, we should flush all the column families. Testing that
    // case here.

    // Individual families should have been flushed.
    assertEquals(0, desiredRegion.getStore(FAMILY1).getMemStoreSize());
    assertEquals(0, desiredRegion.getStore(FAMILY2).getMemStoreSize());
    assertEquals(0, desiredRegion.getStore(FAMILY3).getMemStoreSize());

    // And of course, the total memstore should also be clean.
    assertEquals(0, desiredRegion.getMemstoreSize().get());
  }
}
