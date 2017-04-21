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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.junit.rules.TestName;

@Category(LargeTests.class)
public class TestEndToEndSplitTransaction {
  private static final Log LOG = LogFactory.getLog(TestEndToEndSplitTransaction.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = TEST_UTIL.getConfiguration();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that the client sees meta table changes as atomic during splits
   */
  @Test
  public void testFromClientSideWhileSplitting() throws Throwable {
    LOG.info("Starting testFromClientSideWhileSplitting");
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] FAMILY = Bytes.toBytes("family");

    //SplitTransaction will update the meta table by offlining the parent region, and adding info
    //for daughters.
    Table table = TEST_UTIL.createTable(tableName, FAMILY);

    Stoppable stopper = new StoppableImplementation();
    RegionSplitter regionSplitter = new RegionSplitter(table);
    RegionChecker regionChecker = new RegionChecker(CONF, stopper, tableName);
    final ChoreService choreService = new ChoreService("TEST_SERVER");

    choreService.scheduleChore(regionChecker);
    regionSplitter.start();

    //wait until the splitter is finished
    regionSplitter.join();
    stopper.stop(null);

    if (regionChecker.ex != null) {
      throw new AssertionError("regionChecker", regionChecker.ex);
    }

    if (regionSplitter.ex != null) {
      throw new AssertionError("regionSplitter", regionSplitter.ex);
    }

    //one final check
    regionChecker.verify();
  }

  static class RegionSplitter extends Thread {
    final Connection connection;
    Throwable ex;
    Table table;
    TableName tableName;
    byte[] family;
    Admin admin;
    HRegionServer rs;

    RegionSplitter(Table table) throws IOException {
      this.table = table;
      this.tableName = table.getName();
      this.family = table.getTableDescriptor().getFamiliesKeys().iterator().next();
      admin = TEST_UTIL.getAdmin();
      rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
      connection = TEST_UTIL.getConnection();
    }

    @Override
    public void run() {
      try {
        Random random = new Random();
        for (int i= 0; i< 5; i++) {
          List<HRegionInfo> regions =
              MetaTableAccessor.getTableRegions(connection, tableName, true);
          if (regions.isEmpty()) {
            continue;
          }
          int regionIndex = random.nextInt(regions.size());

          //pick a random region and split it into two
          HRegionInfo region = Iterators.get(regions.iterator(), regionIndex);

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

          flushAndBlockUntilDone(admin, rs, region.getRegionName());
          compactAndBlockUntilDone(admin, rs, region.getRegionName());

          log("Initiating region split for:" + region.getRegionNameAsString());
          try {
            admin.splitRegion(region.getRegionName(), splitPoint);
            //wait until the split is complete
            blockUntilRegionSplit(CONF, 50000, region.getRegionName(), true);

          } catch (NotServingRegionException ex) {
            //ignore
          }
        }
      } catch (Throwable ex) {
        this.ex = ex;
      }
    }

    void addData(int start) throws IOException {
      List<Put> puts = new ArrayList<>();
      for (int i=start; i< start + 100; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(family, family, Bytes.toBytes(i));
        puts.add(put);
      }
      table.put(puts);
    }
  }

  /**
   * Checks regions using MetaTableAccessor and HTable methods
   */
  static class RegionChecker extends ScheduledChore {
    Connection connection;
    Configuration conf;
    TableName tableName;
    Throwable ex;

    RegionChecker(Configuration conf, Stoppable stopper, TableName tableName) throws IOException {
      super("RegionChecker", stopper, 10);
      this.conf = conf;
      this.tableName = tableName;

      this.connection = ConnectionFactory.createConnection(conf);
    }

    /** verify region boundaries obtained from MetaScanner */
    void verifyRegionsUsingMetaTableAccessor() throws Exception {
      List<HRegionInfo> regionList = MetaTableAccessor.getTableRegions(connection, tableName, true);
      verifyTableRegions(Sets.newTreeSet(regionList));
      regionList = MetaTableAccessor.getAllRegions(connection, true);
      verifyTableRegions(Sets.newTreeSet(regionList));
    }

    /** verify region boundaries obtained from HTable.getStartEndKeys() */
    void verifyRegionsUsingHTable() throws IOException {
      Table table = null;
      try {
        //HTable.getStartEndKeys()
        table = connection.getTable(tableName);

        try(RegionLocator rl = connection.getRegionLocator(tableName)) {
          Pair<byte[][], byte[][]> keys = rl.getStartEndKeys();
          verifyStartEndKeys(keys);

          //HTable.getRegionsInfo()
          Set<HRegionInfo> regions = new TreeSet<>();
          for (HRegionLocation loc : rl.getAllRegionLocations()) {
            regions.add(loc.getRegionInfo());
          }
          verifyTableRegions(regions);
        }

      } finally {
        IOUtils.closeQuietly(table);
      }
    }

    void verify() throws Exception {
      verifyRegionsUsingMetaTableAccessor();
      verifyRegionsUsingHTable();
    }

    void verifyTableRegions(Set<HRegionInfo> regions) {
      log("Verifying " + regions.size() + " regions: " + regions);

      byte[][] startKeys = new byte[regions.size()][];
      byte[][] endKeys = new byte[regions.size()][];

      int i=0;
      for (HRegionInfo region : regions) {
        startKeys[i] = region.getStartKey();
        endKeys[i] = region.getEndKey();
        i++;
      }

      Pair<byte[][], byte[][]> keys = new Pair<>(startKeys, endKeys);
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
            "Hole in hbase:meta is detected. prevEndKey=" + Bytes.toStringBinary(prevEndKey)
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
        getStopper().stop("caught exception");
      }
    }
  }

  public static void log(String msg) {
    LOG.info(msg);
  }

  /* some utility methods for split tests */

  public static void flushAndBlockUntilDone(Admin admin, HRegionServer rs, byte[] regionName)
      throws IOException, InterruptedException {
    log("flushing region: " + Bytes.toStringBinary(regionName));
    admin.flushRegion(regionName);
    log("blocking until flush is complete: " + Bytes.toStringBinary(regionName));
    Threads.sleepWithoutInterrupt(500);
    while (rs.getOnlineRegion(regionName).getMemstoreSize() > 0) {
      Threads.sleep(50);
    }
  }

  public static void compactAndBlockUntilDone(Admin admin, HRegionServer rs, byte[] regionName)
      throws IOException, InterruptedException {
    log("Compacting region: " + Bytes.toStringBinary(regionName));
    admin.majorCompactRegion(regionName);
    log("blocking until compaction is complete: " + Bytes.toStringBinary(regionName));
    Threads.sleepWithoutInterrupt(500);
    outer: for (;;) {
      for (Store store : rs.getOnlineRegion(regionName).getStores()) {
        if (store.getStorefilesCount() > 1) {
          Threads.sleep(50);
          continue outer;
        }
      }
      break;
    }
  }

  /** Blocks until the region split is complete in hbase:meta and region server opens the daughters */
  public static void blockUntilRegionSplit(Configuration conf, long timeout,
      final byte[] regionName, boolean waitForDaughters)
      throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    log("blocking until region is split:" +  Bytes.toStringBinary(regionName));
    HRegionInfo daughterA = null, daughterB = null;
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Table metaTable = conn.getTable(TableName.META_TABLE_NAME)) {
      Result result = null;
      HRegionInfo region = null;
      while ((System.currentTimeMillis() - start) < timeout) {
        result = metaTable.get(new Get(regionName));
        if (result == null) {
          break;
        }

        region = MetaTableAccessor.getHRegionInfo(result);
        if (region.isSplitParent()) {
          log("found parent region: " + region.toString());
          PairOfSameType<HRegionInfo> pair = MetaTableAccessor.getDaughterRegions(result);
          daughterA = pair.getFirst();
          daughterB = pair.getSecond();
          break;
        }
        Threads.sleep(100);
      }
      if (daughterA == null || daughterB == null) {
        throw new IOException("Failed to get daughters, daughterA=" + daughterA + ", daughterB=" +
          daughterB + ", timeout=" + timeout + ", result=" + result + ", regionName=" + regionName +
          ", region=" + region);
      }

      //if we are here, this means the region split is complete or timed out
      if (waitForDaughters) {
        long rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(conn, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(conn, rem, daughterB);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterB);
      }
    }
  }

  public static void blockUntilRegionIsInMeta(Connection conn, long timeout, HRegionInfo hri)
      throws IOException, InterruptedException {
    log("blocking until region is in META: " + hri.getRegionNameAsString());
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      HRegionLocation loc = MetaTableAccessor.getRegionLocation(conn, hri);
      if (loc != null && !loc.getRegionInfo().isOffline()) {
        log("found region in META: " + hri.getRegionNameAsString());
        break;
      }
      Threads.sleep(10);
    }
  }

  public static void blockUntilRegionIsOpened(Configuration conf, long timeout, HRegionInfo hri)
      throws IOException, InterruptedException {
    log("blocking until region is opened for reading:" + hri.getRegionNameAsString());
    long start = System.currentTimeMillis();
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(hri.getTable())) {
      byte[] row = hri.getStartKey();
      // Check for null/empty row. If we find one, use a key that is likely to be in first region.
      if (row == null || row.length <= 0) row = new byte[] { '0' };
      Get get = new Get(row);
      while (System.currentTimeMillis() - start < timeout) {
        try {
          table.get(get);
          break;
        } catch (IOException ex) {
          // wait some more
        }
        Threads.sleep(10);
      }
    }
  }
}
