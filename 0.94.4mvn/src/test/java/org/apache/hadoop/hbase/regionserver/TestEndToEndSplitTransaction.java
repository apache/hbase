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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestEndToEndSplitTransaction {
  private static final Log LOG = LogFactory.getLog(TestEndToEndSplitTransaction.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterOpsWhileSplitting() throws Exception {
    byte[] tableName = Bytes.toBytes("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    HTable ht = TEST_UTIL.createTable(tableName, familyName);
    TEST_UTIL.loadTable(ht, familyName);
    ht.close();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte []firstRow = Bytes.toBytes("aaa");
    byte []splitRow = Bytes.toBytes("lll");
    byte []lastRow = Bytes.toBytes("zzz");
    HConnection con = HConnectionManager
        .getConnection(TEST_UTIL.getConfiguration());
    // this will also cache the region
    byte[] regionName = con.locateRegion(tableName, splitRow).getRegionInfo()
        .getRegionName();
    HRegion region = server.getRegion(regionName);
    SplitTransaction split = new SplitTransaction(region, splitRow);
    split.prepare();

    // 1. phase I
    PairOfSameType<HRegion> regions = split.createDaughters(server, server);
    assertFalse(test(con, tableName, firstRow, server));
    assertFalse(test(con, tableName, lastRow, server));

    // passing null as services prevents final step
    // 2, most of phase II
    split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
    assertFalse(test(con, tableName, firstRow, server));
    assertFalse(test(con, tableName, lastRow, server));

    // 3. finish phase II
    // note that this replicates some code from SplitTransaction
    // 2nd daughter first
    server.postOpenDeployTasks(regions.getSecond(), server.getCatalogTracker(), true);
    // Add to online regions
    server.addToOnlineRegions(regions.getSecond());
    // THIS is the crucial point:
    // the 2nd daughter was added, so querying before the split key should fail.
    assertFalse(test(con, tableName, firstRow, server));
    // past splitkey is ok.
    assertTrue(test(con, tableName, lastRow, server));

    // first daughter second
    server.postOpenDeployTasks(regions.getFirst(), server.getCatalogTracker(), true);
    // Add to online regions
    server.addToOnlineRegions(regions.getFirst());
    assertTrue(test(con, tableName, firstRow, server));
    assertTrue(test(con, tableName, lastRow, server));

    // 4. phase III
    split.transitionZKNode(server, server, regions.getFirst(),
        regions.getSecond());
    assertTrue(test(con, tableName, firstRow, server));
    assertTrue(test(con, tableName, lastRow, server));
  }

  /**
   * attempt to locate the region and perform a get and scan
   * @return True if successful, False otherwise.
   */
  private boolean test(HConnection con, byte[] tableName, byte[] row,
      HRegionServer server) {
    // not using HTable to avoid timeouts and retries
    try {
      byte[] regionName = con.relocateRegion(tableName, row).getRegionInfo()
          .getRegionName();
      // get and scan should now succeed without exception
      server.get(regionName, new Get(row));
      server.openScanner(regionName, new Scan(row));
    } catch (IOException x) {
      return false;
    }
    return true;
  }

  /**
   * Tests that the client sees meta table changes as atomic during splits
   */
  @Test
  public void testFromClientSideWhileSplitting() throws Throwable {
    LOG.info("Starting testFromClientSideWhileSplitting");
    final byte[] TABLENAME = Bytes.toBytes("testFromClientSideWhileSplitting");
    final byte[] FAMILY = Bytes.toBytes("family");

    //SplitTransaction will update the meta table by offlining the parent region, and adding info
    //for daughters.
    HTable table = TEST_UTIL.createTable(TABLENAME, FAMILY);

    Stoppable stopper = new StoppableImplementation();
    RegionSplitter regionSplitter = new RegionSplitter(table);
    RegionChecker regionChecker = new RegionChecker(conf, stopper, TABLENAME);

    regionChecker.start();
    regionSplitter.start();

    //wait until the splitter is finished
    regionSplitter.join();
    stopper.stop(null);

    if (regionChecker.ex != null) {
      throw regionChecker.ex;
    }

    if (regionSplitter.ex != null) {
      throw regionSplitter.ex;
    }

    //one final check
    regionChecker.verify();
  }

  static class RegionSplitter extends Thread {
    Throwable ex;
    HTable table;
    byte[] tableName, family;
    HBaseAdmin admin;
    HTable metaTable;
    HRegionServer rs;

    RegionSplitter(HTable table) throws IOException {
      this.table = table;
      this.tableName = table.getTableName();
      this.family = table.getTableDescriptor().getFamiliesKeys().iterator().next();
      admin = TEST_UTIL.getHBaseAdmin();
      rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
      metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
    }

    public void run() {
      try {
        Random random = new Random();
        for (int i=0; i< 5; i++) {
          NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(conf, tableName, false);
          if (regions.size() == 0) {
            continue;
          }
          int regionIndex = random.nextInt(regions.size());

          //pick a random region and split it into two
          HRegionInfo region = Iterators.get(regions.keySet().iterator(), regionIndex);

          //pick the mid split point
          int start = 0, end = Integer.MAX_VALUE;
          if (region.getStartKey().length > 0) {
            start = Bytes.toInt(region.getStartKey());
          }
          if (region.getEndKey().length > 0) {
            end = Bytes.toInt(region.getEndKey());
          }
          int mid = start + ((end - start) / 2);
          byte[] splitPoint = Bytes.toBytes(mid);

          //put some rows to the regions
          addData(start);
          addData(mid);

          flushAndBlockUntilDone(region.getRegionName());
          compactAndBlockUntilDone(region.getRegionName());

          log("Initiating region split for:" + region.getRegionNameAsString());
          try {
            admin.split(region.getRegionName(), splitPoint);
            //wait until the split is complete
            blockUntilRegionSplit(50000, region.getRegionName(), true);

          } catch (NotServingRegionException ex) {
            //ignore
          }
        }
      } catch (Throwable ex) {
        this.ex = ex;
      } finally {
        if (metaTable != null) {
          IOUtils.closeQuietly(metaTable);
        }
      }
    }

    void addData(int start) throws IOException {
      for (int i=start; i< start + 100; i++) {
        Put put = new Put(Bytes.toBytes(i));

        put.add(family, family, Bytes.toBytes(i));
        table.put(put);
      }
      table.flushCommits();
    }

    void flushAndBlockUntilDone(byte[] regionName) throws IOException, InterruptedException {
      log("flushing region: " + Bytes.toStringBinary(regionName));
      admin.flush(regionName);
      log("blocking until flush is complete: " + Bytes.toStringBinary(regionName));
      Threads.sleepWithoutInterrupt(500);
      while (rs.cacheFlusher.getFlushQueueSize() > 0) {
        Threads.sleep(50);
      }
    }

    void compactAndBlockUntilDone(byte[] regionName) throws IOException,
      InterruptedException {
      log("Compacting region: " + Bytes.toStringBinary(regionName));
      admin.majorCompact(regionName);
      log("blocking until compaction is complete: " + Bytes.toStringBinary(regionName));
      Threads.sleepWithoutInterrupt(500);
      while (rs.compactSplitThread.getCompactionQueueSize() > 0) {
        Threads.sleep(50);
      }
    }

    /** bloks until the region split is complete in META and region server opens the daughters */
    void blockUntilRegionSplit(long timeout, final byte[] regionName, boolean waitForDaughters)
        throws IOException, InterruptedException {
      long start = System.currentTimeMillis();
      log("blocking until region is split:" +  Bytes.toStringBinary(regionName));
      HRegionInfo daughterA = null, daughterB = null;

      while (System.currentTimeMillis() - start < timeout) {
        Result result = getRegionRow(regionName);
        if (result == null) {
          break;
        }

        HRegionInfo region = MetaEditor.getHRegionInfo(result);
        if(region.isSplitParent()) {
          log("found parent region: " + region.toString());
          PairOfSameType<HRegionInfo> pair = MetaEditor.getDaughterRegions(result);
          daughterA = pair.getFirst();
          daughterB = pair.getSecond();
          break;
        }
        sleep(100);
      }

      //if we are here, this means the region split is complete or timed out
      if (waitForDaughters) {
        long rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(rem, daughterA.getRegionName());

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(rem, daughterB.getRegionName());

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpenedByRS(rem, daughterA.getRegionName());

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpenedByRS(rem, daughterB.getRegionName());
      }
    }

    Result getRegionRow(byte[] regionName) throws IOException {
      Get get = new Get(regionName);
      return metaTable.get(get);
    }

    void blockUntilRegionIsInMeta(long timeout, byte[] regionName)
        throws IOException, InterruptedException {
      log("blocking until region is in META: " + Bytes.toStringBinary(regionName));
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
        Result result = getRegionRow(regionName);
        if (result != null) {
          HRegionInfo info = MetaEditor.getHRegionInfo(result);
          if (info != null && !info.isOffline()) {
            log("found region in META: " + Bytes.toStringBinary(regionName));
            break;
          }
        }
        sleep(10);
      }
    }

    void blockUntilRegionIsOpenedByRS(long timeout, byte[] regionName)
      throws IOException, InterruptedException {
      log("blocking until region is opened by region server: " + Bytes.toStringBinary(regionName));
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
        List<HRegion> regions = rs.getOnlineRegions(tableName);
        for (HRegion region : regions) {
          if (Bytes.compareTo(region.getRegionName(), regionName) == 0) {
            log("found region open in RS: " + Bytes.toStringBinary(regionName));
            return;
          }
        }
        sleep(10);
      }
    }

  }

  /**
   * Checks regions using MetaScanner, MetaReader and HTable methods
   */
  static class RegionChecker extends Chore {
    Configuration conf;
    byte[] tableName;
    Throwable ex;

    RegionChecker(Configuration conf, Stoppable stopper, byte[] tableName) {
      super("RegionChecker", 10, stopper);
      this.conf = conf;
      this.tableName = tableName;
      this.setDaemon(true);
    }

    /** verify region boundaries obtained from MetaScanner */
    void verifyRegionsUsingMetaScanner() throws Exception {

      //MetaScanner.allTableRegions()
      NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(conf, tableName,
          false);
      verifyTableRegions(regions.keySet());

      //MetaScanner.listAllRegions()
      List<HRegionInfo> regionList = MetaScanner.listAllRegions(conf, false);
      verifyTableRegions(Sets.newTreeSet(regionList));
    }

    /** verify region boundaries obtained from HTable.getStartEndKeys() */
    void verifyRegionsUsingHTable() throws IOException {
      HTable table = null;
      try {
        //HTable.getStartEndKeys()
        table = new HTable(conf, tableName);
        Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
        verifyStartEndKeys(keys);

        //HTable.getRegionsInfo()
        Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
        verifyTableRegions(regions.keySet());
      } finally {
        IOUtils.closeQuietly(table);
      }
    }

    void verify() throws Exception {
      verifyRegionsUsingMetaScanner();
      verifyRegionsUsingHTable();
    }

    void verifyTableRegions(Set<HRegionInfo> regions) {
      log("Verifying " + regions.size() + " regions");

      byte[][] startKeys = new byte[regions.size()][];
      byte[][] endKeys = new byte[regions.size()][];

      int i=0;
      for (HRegionInfo region : regions) {
        startKeys[i] = region.getStartKey();
        endKeys[i] = region.getEndKey();
        i++;
      }

      Pair<byte[][], byte[][]> keys = new Pair<byte[][], byte[][]>(startKeys, endKeys);
      verifyStartEndKeys(keys);
    }

    void verifyStartEndKeys(Pair<byte[][], byte[][]> keys) {
      byte[][] startKeys = keys.getFirst();
      byte[][] endKeys = keys.getSecond();
      assertEquals(startKeys.length, endKeys.length);
      assertTrue("Found 0 regions for the table", startKeys.length > 0);

      assertArrayEquals("Start key for the first region is not byte[0]",
          HConstants.EMPTY_START_ROW, startKeys[0]);
      byte[] prevEndKey = HConstants.EMPTY_START_ROW;

      // ensure that we do not have any gaps
      for (int i=0; i<startKeys.length; i++) {
        assertArrayEquals(
            "Hole in .META. is detected. prevEndKey=" + Bytes.toStringBinary(prevEndKey)
                + " ,regionStartKey=" + Bytes.toStringBinary(startKeys[i]), prevEndKey,
            startKeys[i]);
        prevEndKey = endKeys[i];
      }
      assertArrayEquals("End key for the last region is not byte[0]", HConstants.EMPTY_END_ROW,
          endKeys[endKeys.length - 1]);
    }

    @Override
    protected void chore() {
      try {
        verify();
      } catch (Throwable ex) {
        this.ex = ex;
        stopper.stop("caught exception");
      }
    }
  }

  public static void log(String msg) {
    LOG.info(msg);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

