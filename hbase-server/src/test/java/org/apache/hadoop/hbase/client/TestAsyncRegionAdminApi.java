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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.NoSuchProcedureException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class to test asynchronous region admin operations.
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi extends TestAsyncAdminBase {

  public static Random RANDOM = new Random(System.currentTimeMillis());

  private void createTableWithDefaultConf(TableName TABLENAME) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null).get();
  }

  @Test
  public void testCloseRegion() throws Exception {
    TableName TABLENAME = TableName.valueOf("TestHBACloseRegion");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.getTable().isSystemTable()) {
        info = regionInfo;
        boolean closed = admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
          rs.getServerName().getServerName()).get();
        assertTrue(closed);
      }
    }
    boolean isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.", isInList);
  }

  @Test
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    final String name = "TestHBACloseRegion1";
    byte[] TABLENAME = Bytes.toBytes(name);
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains(name)) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName("sample", rs.getServerName().getServerName())
                .get();
          } catch (Exception e) {
            catchNotServingException = true;
            // expected, ignore it
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionWhenServerNameIsNull() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion3");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion3")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), null).get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is null.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegionWhenServerNameIsEmpty");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegionWhenServerNameIsEmpty")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), " ").get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenEncodedRegionNameIsNotGiven() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion4");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion4")) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName(regionInfo.getRegionNameAsString(),
              rs.getServerName().getServerName()).get();
          } catch (Exception e) {
            // expected, ignore it.
            catchNotServingException = true;
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testGetRegion() throws Exception {
    AsyncHBaseAdmin rawAdmin = (AsyncHBaseAdmin) admin;

    final TableName tableName = TableName.valueOf("testGetRegion");
    LOG.info("Started " + tableName);
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      HRegionLocation regionLocation = locator.getRegionLocation(Bytes.toBytes("mmm"));
      HRegionInfo region = regionLocation.getRegionInfo();
      byte[] regionName = region.getRegionName();
      Pair<HRegionInfo, ServerName> pair = rawAdmin.getRegion(regionName).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
      pair = rawAdmin.getRegion(region.getEncodedNameAsBytes()).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeRegions");
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    byte[][] splitRows = new byte[][] { Bytes.toBytes("3"), Bytes.toBytes("6") };
    Admin syncAdmin = TEST_UTIL.getAdmin();
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<HRegionInfo> tableRegions;
      HRegionInfo regionA;
      HRegionInfo regionB;

      // merge with full name
      tableRegions = syncAdmin.getTableRegions(tableName);
      assertEquals(3, syncAdmin.getTableRegions(tableName).size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(2, syncAdmin.getTableRegions(tableName).size());

      // merge with encoded name
      tableRegions = syncAdmin.getTableRegions(tableName);
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(1, syncAdmin.getTableRegions(tableName).size());
    } finally {
      syncAdmin.disableTable(tableName);
      syncAdmin.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitTable() throws Exception {
    splitTests(TableName.valueOf("testSplitTable"), 3000, false, null);
    splitTests(TableName.valueOf("testSplitTableWithSplitPoint"), 3000, false, Bytes.toBytes("3"));
    splitTests(TableName.valueOf("testSplitRegion"), 3000, true, null);
    splitTests(TableName.valueOf("testSplitRegionWithSplitPoint"), 3000, true, Bytes.toBytes("3"));
  }

  private void splitTests(TableName tableName, int rowCount, boolean isSplitRegion,
      byte[] splitPoint) throws Exception {
    int count = 0;
    // create table
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    Table table = TEST_UTIL.createTable(td, null);
    TEST_UTIL.waitTableAvailable(tableName);

    List<HRegionInfo> regions = TEST_UTIL.getAdmin().getTableRegions(tableName);
    assertEquals(regions.size(), 1);

    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(Bytes.toBytes("d"), null, Bytes.toBytes("value" + i));
      puts.add(put);
    }
    table.put(puts);

    if (isSplitRegion) {
      admin.splitRegion(regions.get(0).getRegionName(), splitPoint).get();
    } else {
      if (splitPoint == null) {
        admin.split(tableName).get();
      } else {
        admin.split(tableName, splitPoint).get();
      }
    }

    for (int i = 0; i < 45; i++) {
      try {
        List<HRegionInfo> hRegionInfos = TEST_UTIL.getAdmin().getTableRegions(tableName);
        count = hRegionInfos.size();
        if (count >= 2) {
          break;
        }
        Thread.sleep(1000L);
      } catch (Exception e) {
        LOG.error(e);
      }
    }

    assertEquals(count, 2);
  }

  @Test
  public void testAssignRegionAndUnassignRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignRegionAndUnassignRegion");
    try {
      // create test table
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc).get();

      // assign region.
      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      AssignmentManager am = master.getAssignmentManager();
      HRegionInfo hri = am.getRegionStates().getRegionsOfTable(tableName).get(0);

      // assert region on server
      RegionStates regionStates = am.getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
      assertTrue(regionStates.getRegionState(hri).isOpened());

      // Region is assigned now. Let's assign it again.
      // Master should not abort, and region should stay assigned.
      admin.assign(hri.getRegionName()).get();
      try {
        am.waitForAssignment(hri);
        fail("Expected NoSuchProcedureException");
      } catch (NoSuchProcedureException e) {
        // Expected
      }
      assertTrue(regionStates.getRegionState(hri).isOpened());

      // unassign region
      admin.unassign(hri.getRegionName(), true).get();
      try {
        am.waitForAssignment(hri);
        fail("Expected NoSuchProcedureException");
      } catch (NoSuchProcedureException e) {
        // Expected
      }
      assertTrue(regionStates.getRegionState(hri).isClosed());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  HRegionInfo createTableAndGetOneRegion(final TableName tableName)
      throws IOException, InterruptedException, ExecutionException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5).get();

    // wait till the table is assigned
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 3000;
    while (true) {
      List<HRegionInfo> regions =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName);
      if (regions.size() > 3) {
        return regions.get(2);
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Could not find an online region");
      }
      Thread.sleep(10);
    }
  }

  @Ignore @Test
  // Turning off this tests in AMv2. Doesn't make sense.Offlining means something
  // different now.
  // You can't 'offline' a region unless you know what you are doing
  // Will cause the Master to tell the regionserver to shut itself down because
  // regionserver is reporting the state as OPEN.
  public void testOfflineRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testOfflineRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);

      RegionStates regionStates =
          TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
      admin.offline(hri.getRegionName()).get();

      long timeoutTime = System.currentTimeMillis() + 3000;
      while (true) {
        if (regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OFFLINE)
            .contains(hri))
          break;
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to offline the region in time");
          break;
        }
        Thread.sleep(10);
      }
      RegionState regionState = regionStates.getRegionState(hri);
      assertTrue(regionState.isOffline());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testGetRegionByStateOfTable() throws Exception {
    final TableName tableName = TableName.valueOf("testGetRegionByStateOfTable");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);

      RegionStates regionStates =
          TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
      assertTrue(regionStates.getRegionByStateOfTable(tableName)
              .get(RegionState.State.OPEN)
              .contains(hri));
      assertFalse(regionStates.getRegionByStateOfTable(TableName.valueOf("I_am_the_phantom"))
              .get(RegionState.State.OPEN)
              .contains(hri));
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMoveRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testMoveRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      ServerManager serverManager = master.getServerManager();
      ServerName destServerName = null;
      List<JVMClusterUtil.RegionServerThread> regionServers =
          TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();
      for (JVMClusterUtil.RegionServerThread regionServer : regionServers) {
        HRegionServer destServer = regionServer.getRegionServer();
        destServerName = destServer.getServerName();
        if (!destServerName.equals(serverName) && serverManager.isServerOnline(destServerName)) {
          break;
        }
      }
      assertTrue(destServerName != null && !destServerName.equals(serverName));
      admin.move(hri.getEncodedNameAsBytes(), Bytes.toBytes(destServerName.getServerName())).get();

      long timeoutTime = System.currentTimeMillis() + 30000;
      while (true) {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertRegionOnServer(hri, sn, 200);
          break;
        }
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to move the region in time: " + regionStates.getRegionState(hri));
        }
        regionStates.wait(50);
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testGetOnlineRegions() throws Exception {
    final TableName tableName = TableName.valueOf("testGetOnlineRegions");
    try {
      createTableAndGetOneRegion(tableName);
      AtomicInteger regionServerCount = new AtomicInteger(0);
      TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
          .map(rsThread -> rsThread.getRegionServer().getServerName()).forEach(serverName -> {
            try {
              Assert.assertEquals(admin.getOnlineRegions(serverName).get().size(),
                TEST_UTIL.getAdmin().getOnlineRegions(serverName).size());
            } catch (Exception e) {
              fail("admin.getOnlineRegions() method throws a exception: " + e.getMessage());
            }
            regionServerCount.incrementAndGet();
          });
      Assert.assertEquals(regionServerCount.get(), 2);
    } catch (Exception e) {
      LOG.info("Exception", e);
      throw e;
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testFlushTableAndRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testFlushRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);
      ServerName serverName = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .getRegionStates().getRegionServerOfRegion(hri);
      HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
          .map(rsThread -> rsThread.getRegionServer())
          .filter(rs -> rs.getServerName().equals(serverName)).findFirst().get();
      // write a put into the specific region
      try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
        table.put(new Put(hri.getStartKey()).addColumn(FAMILY, FAMILY_0, Bytes.toBytes("value-1")));
      }
      Assert.assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize() > 0);
      // flush region and wait flush operation finished.
      LOG.info("flushing region: " + Bytes.toStringBinary(hri.getRegionName()));
      admin.flushRegion(hri.getRegionName()).get();
      LOG.info("blocking until flush is complete: " + Bytes.toStringBinary(hri.getRegionName()));
      Threads.sleepWithoutInterrupt(500);
      while (regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize() > 0) {
        Threads.sleep(50);
      }
      // check the memstore.
      Assert.assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize(), 0);

      // write another put into the specific region
      try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
        table.put(new Put(hri.getStartKey()).addColumn(FAMILY, FAMILY_0, Bytes.toBytes("value-2")));
      }
      Assert.assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize() > 0);
      admin.flush(tableName).get();
      Threads.sleepWithoutInterrupt(500);
      while (regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize() > 0) {
        Threads.sleep(50);
      }
      // check the memstore.
      Assert.assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemstoreSize(), 0);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test(timeout = 600000)
  public void testCompactRpcAPI() throws Exception {
    String tableName = "testCompactRpcAPI";
    compactionTest(tableName, 8, CompactionState.MAJOR, false);
    compactionTest(tableName, 15, CompactionState.MINOR, false);
    compactionTest(tableName, 8, CompactionState.MAJOR, true);
    compactionTest(tableName, 15, CompactionState.MINOR, true);
  }

  @Test(timeout = 600000)
  public void testCompactRegionServer() throws Exception {
    TableName table = TableName.valueOf("testCompactRegionServer");
    byte[][] families = { Bytes.toBytes("f1"), Bytes.toBytes("f2"), Bytes.toBytes("f3") };
    Table ht = null;
    try {
      ht = TEST_UTIL.createTable(table, families);
      loadData(ht, families, 3000, 8);
      List<HRegionServer> rsList = TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
          .map(rsThread -> rsThread.getRegionServer()).collect(Collectors.toList());
      List<Region> regions = new ArrayList<>();
      rsList.forEach(rs -> regions.addAll(rs.getOnlineRegions(table)));
      Assert.assertEquals(regions.size(), 1);
      int countBefore = countStoreFilesInFamilies(regions, families);
      Assert.assertTrue(countBefore > 0);
      // Minor compaction for all region servers.
      for (HRegionServer rs : rsList)
        admin.compactRegionServer(rs.getServerName()).get();
      Thread.sleep(5000);
      int countAfterMinorCompaction = countStoreFilesInFamilies(regions, families);
      Assert.assertTrue(countAfterMinorCompaction < countBefore);
      // Major compaction for all region servers.
      for (HRegionServer rs : rsList)
        admin.majorCompactRegionServer(rs.getServerName()).get();
      Thread.sleep(5000);
      int countAfterMajorCompaction = countStoreFilesInFamilies(regions, families);
      Assert.assertEquals(countAfterMajorCompaction, 3);
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(table);
      }
    }
  }

  private void compactionTest(final String tableName, final int flushes,
      final CompactionState expectedState, boolean singleFamily) throws Exception {
    // Create a table with regions
    final TableName table = TableName.valueOf(tableName);
    byte[] family = Bytes.toBytes("family");
    byte[][] families =
        { family, Bytes.add(family, Bytes.toBytes("2")), Bytes.add(family, Bytes.toBytes("3")) };
    Table ht = null;
    try {
      ht = TEST_UTIL.createTable(table, families);
      loadData(ht, families, 3000, flushes);
      List<Region> regions = new ArrayList<>();
      TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads()
          .forEach(rsThread -> regions.addAll(rsThread.getRegionServer().getOnlineRegions(table)));
      Assert.assertEquals(regions.size(), 1);
      int countBefore = countStoreFilesInFamilies(regions, families);
      int countBeforeSingleFamily = countStoreFilesInFamily(regions, family);
      assertTrue(countBefore > 0); // there should be some data files
      if (expectedState == CompactionState.MINOR) {
        if (singleFamily) {
          admin.compact(table, family).get();
        } else {
          admin.compact(table).get();
        }
      } else {
        if (singleFamily) {
          admin.majorCompact(table, family).get();
        } else {
          admin.majorCompact(table).get();
        }
      }
      long curt = System.currentTimeMillis();
      long waitTime = 5000;
      long endt = curt + waitTime;
      CompactionState state = TEST_UTIL.getAdmin().getCompactionState(table);
      while (state == CompactionState.NONE && curt < endt) {
        Thread.sleep(10);
        state = TEST_UTIL.getAdmin().getCompactionState(table);
        curt = System.currentTimeMillis();
      }
      // Now, should have the right compaction state,
      // otherwise, the compaction should have already been done
      if (expectedState != state) {
        for (Region region : regions) {
          state = CompactionState.valueOf(region.getCompactionState().toString());
          assertEquals(CompactionState.NONE, state);
        }
      } else {
        // Wait until the compaction is done
        state = TEST_UTIL.getAdmin().getCompactionState(table);
        while (state != CompactionState.NONE && curt < endt) {
          Thread.sleep(10);
          state = TEST_UTIL.getAdmin().getCompactionState(table);
        }
        // Now, compaction should be done.
        assertEquals(CompactionState.NONE, state);
      }
      int countAfter = countStoreFilesInFamilies(regions, families);
      int countAfterSingleFamily = countStoreFilesInFamily(regions, family);
      assertTrue(countAfter < countBefore);
      if (!singleFamily) {
        if (expectedState == CompactionState.MAJOR) assertTrue(families.length == countAfter);
        else assertTrue(families.length < countAfter);
      } else {
        int singleFamDiff = countBeforeSingleFamily - countAfterSingleFamily;
        // assert only change was to single column family
        assertTrue(singleFamDiff == (countBefore - countAfter));
        if (expectedState == CompactionState.MAJOR) {
          assertTrue(1 == countAfterSingleFamily);
        } else {
          assertTrue(1 < countAfterSingleFamily);
        }
      }
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(table);
      }
    }
  }

  private static int countStoreFilesInFamily(List<Region> regions, final byte[] family) {
    return countStoreFilesInFamilies(regions, new byte[][] { family });
  }

  private static int countStoreFilesInFamilies(List<Region> regions, final byte[][] families) {
    int count = 0;
    for (Region region : regions) {
      count += region.getStoreFileList(families).size();
    }
    return count;
  }

  private static void loadData(final Table ht, final byte[][] families, final int rows,
      final int flushes) throws IOException {
    List<Put> puts = new ArrayList<>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.toBytes(RANDOM.nextLong());
        Put p = new Put(row);
        for (int j = 0; j < families.length; ++j) {
          p.addColumn(families[j], qualifier, row);
        }
        puts.add(p);
      }
      ht.put(puts);
      TEST_UTIL.flush();
      puts.clear();
    }
  }
}
