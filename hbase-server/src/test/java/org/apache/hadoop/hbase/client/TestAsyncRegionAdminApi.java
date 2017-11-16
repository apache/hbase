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

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
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
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous region admin operations.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi extends TestAsyncAdminBase {

  @Test
  public void testGetRegionLocation() throws Exception {
    RawAsyncHBaseAdmin rawAdmin = (RawAsyncHBaseAdmin) ASYNC_CONN.getAdmin();
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);
    AsyncTableRegionLocator locator = ASYNC_CONN.getRegionLocator(tableName);
    HRegionLocation regionLocation = locator.getRegionLocation(Bytes.toBytes("mmm")).get();
    RegionInfo region = regionLocation.getRegion();
    byte[] regionName = regionLocation.getRegion().getRegionName();
    HRegionLocation location = rawAdmin.getRegionLocation(regionName).get();
    assertTrue(Bytes.equals(regionName, location.getRegion().getRegionName()));
    location = rawAdmin.getRegionLocation(region.getEncodedNameAsBytes()).get();
    assertTrue(Bytes.equals(regionName, location.getRegion().getRegionName()));
  }

  @Test
  public void testAssignRegionAndUnassignRegion() throws Exception {
    createTableWithDefaultConf(tableName);

    // assign region.
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    RegionInfo hri = am.getRegionStates().getRegionsOfTable(tableName).get(0);

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
  }

  RegionInfo createTableAndGetOneRegion(final TableName tableName)
      throws IOException, InterruptedException, ExecutionException {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName)
            .addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5).get();

    // wait till the table is assigned
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 3000;
    while (true) {
      List<RegionInfo> regions =
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
    RegionInfo hri = createTableAndGetOneRegion(tableName);

    RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    admin.offline(hri.getRegionName()).get();

    long timeoutTime = System.currentTimeMillis() + 3000;
    while (true) {
      if (regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OFFLINE)
          .stream().anyMatch(r -> RegionInfo.COMPARATOR.compare(r, hri) == 0)) break;
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Failed to offline the region in time");
        break;
      }
      Thread.sleep(10);
    }
    RegionState regionState = regionStates.getRegionState(hri);
    assertTrue(regionState.isOffline());
  }

  @Test
  public void testGetRegionByStateOfTable() throws Exception {
    RegionInfo hri = createTableAndGetOneRegion(tableName);

    RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    assertTrue(regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN)
        .stream().anyMatch(r -> RegionInfo.COMPARATOR.compare(r, hri) == 0));
    assertFalse(regionStates.getRegionByStateOfTable(TableName.valueOf("I_am_the_phantom"))
        .get(RegionState.State.OPEN).stream().anyMatch(r -> RegionInfo.COMPARATOR.compare(r, hri) == 0));
  }

  @Test
  public void testMoveRegion() throws Exception {
    admin.setBalancerOn(false).join();

    RegionInfo hri = createTableAndGetOneRegion(tableName);
    RawAsyncHBaseAdmin rawAdmin = (RawAsyncHBaseAdmin) ASYNC_CONN.getAdmin();
    ServerName serverName = rawAdmin.getRegionLocation(hri.getRegionName()).get().getServerName();

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
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
    admin.move(hri.getRegionName(), destServerName).get();

    long timeoutTime = System.currentTimeMillis() + 30000;
    while (true) {
      ServerName sn = rawAdmin.getRegionLocation(hri.getRegionName()).get().getServerName();
      if (sn != null && sn.equals(destServerName)) {
        break;
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Failed to move the region in time: " + hri);
      }
      Thread.sleep(100);
    }
    admin.setBalancerOn(true).join();
  }

  @Test
  public void testGetOnlineRegions() throws Exception {
    createTableAndGetOneRegion(tableName);
    AtomicInteger regionServerCount = new AtomicInteger(0);
    TEST_UTIL
        .getHBaseCluster()
        .getLiveRegionServerThreads()
        .stream()
        .map(rsThread -> rsThread.getRegionServer())
        .forEach(
          rs -> {
            ServerName serverName = rs.getServerName();
            try {
              Assert.assertEquals(admin.getOnlineRegions(serverName).get().size(), rs
                  .getRegions().size());
            } catch (Exception e) {
              fail("admin.getOnlineRegions() method throws a exception: " + e.getMessage());
            }
            regionServerCount.incrementAndGet();
          });
    Assert.assertEquals(regionServerCount.get(), 2);
  }

  @Test
  public void testFlushTableAndRegion() throws Exception {
    RegionInfo hri = createTableAndGetOneRegion(tableName);
    ServerName serverName =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
            .getRegionServerOfRegion(hri);
    HRegionServer regionServer =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
            .map(rsThread -> rsThread.getRegionServer())
            .filter(rs -> rs.getServerName().equals(serverName)).findFirst().get();

    // write a put into the specific region
    ASYNC_CONN.getTable(tableName)
        .put(new Put(hri.getStartKey()).addColumn(FAMILY, FAMILY_0, Bytes.toBytes("value-1")))
        .join();
    Assert.assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize() > 0);
    // flush region and wait flush operation finished.
    LOG.info("flushing region: " + Bytes.toStringBinary(hri.getRegionName()));
    admin.flushRegion(hri.getRegionName()).get();
    LOG.info("blocking until flush is complete: " + Bytes.toStringBinary(hri.getRegionName()));
    Threads.sleepWithoutInterrupt(500);
    while (regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize() > 0) {
      Threads.sleep(50);
    }
    // check the memstore.
    Assert.assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize(), 0);

    // write another put into the specific region
    ASYNC_CONN.getTable(tableName)
        .put(new Put(hri.getStartKey()).addColumn(FAMILY, FAMILY_0, Bytes.toBytes("value-2")))
        .join();
    Assert.assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize() > 0);
    admin.flush(tableName).get();
    Threads.sleepWithoutInterrupt(500);
    while (regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize() > 0) {
      Threads.sleep(50);
    }
    // check the memstore.
    Assert.assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreSize(), 0);
  }

  @Test
  public void testSplitSwitch() throws Exception {
    createTableWithDefaultConf(tableName);
    byte[][] families = { FAMILY };
    final int rows = 10000;
    loadData(tableName, families, rows);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    int originalCount = regionLocations.size();

    initSplitMergeSwitch();
    assertTrue(admin.setSplitOn(false).get());
    try {
      admin.split(tableName, Bytes.toBytes(rows / 2)).join();
    } catch (Exception e){
      //Expected
    }
    int count = admin.getTableRegions(tableName).get().size();
    assertTrue(originalCount == count);

    assertFalse(admin.setSplitOn(true).get());
    admin.split(tableName).join();
    while ((count = admin.getTableRegions(tableName).get().size()) == originalCount) {
      Threads.sleep(100);
    }
    assertTrue(originalCount < count);
  }

  @Test
  @Ignore
  // It was ignored in TestSplitOrMergeStatus, too
  public void testMergeSwitch() throws Exception {
    createTableWithDefaultConf(tableName);
    byte[][] families = { FAMILY };
    loadData(tableName, families, 1000);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    int originalCount = regionLocations.size();

    initSplitMergeSwitch();
    admin.split(tableName).join();
    int postSplitCount = originalCount;
    while ((postSplitCount = admin.getTableRegions(tableName).get().size()) == originalCount) {
      Threads.sleep(100);
    }
    assertTrue("originalCount=" + originalCount + ", postSplitCount=" + postSplitCount,
      originalCount != postSplitCount);

    // Merge switch is off so merge should NOT succeed.
    assertTrue(admin.setMergeOn(false).get());
    List<RegionInfo> regions = admin.getTableRegions(tableName).get();
    assertTrue(regions.size() > 1);
    admin.mergeRegions(regions.get(0).getRegionName(), regions.get(1).getRegionName(), true).join();
    int count = admin.getTableRegions(tableName).get().size();
    assertTrue("postSplitCount=" + postSplitCount + ", count=" + count, postSplitCount == count);

    // Merge switch is on so merge should succeed.
    assertFalse(admin.setMergeOn(true).get());
    admin.mergeRegions(regions.get(0).getRegionName(), regions.get(1).getRegionName(), true).join();
    count = admin.getTableRegions(tableName).get().size();
    assertTrue((postSplitCount / 2) == count);
  }

  private void initSplitMergeSwitch() throws Exception {
    if (!admin.isSplitOn().get()) {
      admin.setSplitOn(true).get();
    }
    if (!admin.isMergeOn().get()) {
      admin.setMergeOn(true).get();
    }
    assertTrue(admin.isSplitOn().get());
    assertTrue(admin.isMergeOn().get());
  }

  @Test
  public void testMergeRegions() throws Exception {
    byte[][] splitRows = new byte[][] { Bytes.toBytes("3"), Bytes.toBytes("6") };
    createTableWithDefaultConf(tableName, splitRows);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    RegionInfo regionA;
    RegionInfo regionB;

    // merge with full name
    assertEquals(3, regionLocations.size());
    regionA = regionLocations.get(0).getRegion();
    regionB = regionLocations.get(1).getRegion();
    admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

    regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    assertEquals(2, regionLocations.size());
    // merge with encoded name
    regionA = regionLocations.get(0).getRegion();
    regionB = regionLocations.get(1).getRegion();
    admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

    regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    assertEquals(1, regionLocations.size());
  }

  @Test
  public void testSplitTable() throws Exception {
    initSplitMergeSwitch();
    splitTest(TableName.valueOf("testSplitTable"), 3000, false, null);
    splitTest(TableName.valueOf("testSplitTableWithSplitPoint"), 3000, false, Bytes.toBytes("3"));
    splitTest(TableName.valueOf("testSplitTableRegion"), 3000, true, null);
    splitTest(TableName.valueOf("testSplitTableRegionWithSplitPoint2"), 3000, true, Bytes.toBytes("3"));
  }

  private void
      splitTest(TableName tableName, int rowCount, boolean isSplitRegion, byte[] splitPoint)
          throws Exception {
    // create table
    createTableWithDefaultConf(tableName);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regionLocations =
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    assertEquals(1, regionLocations.size());

    AsyncTable<?> table = ASYNC_CONN.getTable(tableName);
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(FAMILY, null, Bytes.toBytes("value" + i));
      puts.add(put);
    }
    table.putAll(puts).join();

    if (isSplitRegion) {
      if (splitPoint == null) {
        admin.splitRegion(regionLocations.get(0).getRegion().getRegionName()).get();
      } else {
        admin.splitRegion(regionLocations.get(0).getRegion().getRegionName(), splitPoint).get();
      }
    } else {
      if (splitPoint == null) {
        admin.split(tableName).get();
      } else {
        admin.split(tableName, splitPoint).get();
      }
    }

    int count = 0;
    for (int i = 0; i < 45; i++) {
      try {
        regionLocations =
            AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
                .get();
        count = regionLocations.size();
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
  public void testCompactRegionServer() throws Exception {
    byte[][] families = { Bytes.toBytes("f1"), Bytes.toBytes("f2"), Bytes.toBytes("f3") };
    createTableWithDefaultConf(tableName, null, families);
    loadData(tableName, families, 3000, 8);

    List<HRegionServer> rsList =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
            .map(rsThread -> rsThread.getRegionServer()).collect(Collectors.toList());
    List<Region> regions = new ArrayList<>();
    rsList.forEach(rs -> regions.addAll(rs.getRegions(tableName)));
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
  }

  @Test
  public void testCompact() throws Exception {
    compactionTest(TableName.valueOf("testCompact1"), 8, CompactionState.MAJOR, false);
    compactionTest(TableName.valueOf("testCompact2"), 15, CompactionState.MINOR, false);
    compactionTest(TableName.valueOf("testCompact3"), 8, CompactionState.MAJOR, true);
    compactionTest(TableName.valueOf("testCompact4"), 15, CompactionState.MINOR, true);
  }

  private void compactionTest(final TableName tableName, final int flushes,
      final CompactionState expectedState, boolean singleFamily) throws Exception {
    // Create a table with regions
    byte[] family = Bytes.toBytes("family");
    byte[][] families =
        { family, Bytes.add(family, Bytes.toBytes("2")), Bytes.add(family, Bytes.toBytes("3")) };
    createTableWithDefaultConf(tableName, null, families);
    loadData(tableName, families, 3000, flushes);

    List<Region> regions = new ArrayList<>();
    TEST_UTIL
        .getHBaseCluster()
        .getLiveRegionServerThreads()
        .forEach(rsThread -> regions.addAll(rsThread.getRegionServer().getRegions(tableName)));
    Assert.assertEquals(regions.size(), 1);

    int countBefore = countStoreFilesInFamilies(regions, families);
    int countBeforeSingleFamily = countStoreFilesInFamily(regions, family);
    assertTrue(countBefore > 0); // there should be some data files
    if (expectedState == CompactionState.MINOR) {
      if (singleFamily) {
        admin.compact(tableName, family).get();
      } else {
        admin.compact(tableName).get();
      }
    } else {
      if (singleFamily) {
        admin.majorCompact(tableName, family).get();
      } else {
        admin.majorCompact(tableName).get();
      }
    }

    long curt = System.currentTimeMillis();
    long waitTime = 5000;
    long endt = curt + waitTime;
    CompactionState state = admin.getCompactionState(tableName).get();
    while (state == CompactionState.NONE && curt < endt) {
      Thread.sleep(10);
      state = admin.getCompactionState(tableName).get();
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
      state = admin.getCompactionState(tableName).get();
      while (state != CompactionState.NONE && curt < endt) {
        Thread.sleep(10);
        state = admin.getCompactionState(tableName).get();
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

  private static void loadData(final TableName tableName, final byte[][] families, final int rows)
      throws IOException {
    loadData(tableName, families, rows, 1);
  }

  private static void loadData(final TableName tableName, final byte[][] families, final int rows,
      final int flushes) throws IOException {
    AsyncTable<?> table = ASYNC_CONN.getTable(tableName);
    List<Put> puts = new ArrayList<>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.add(Bytes.toBytes(k), Bytes.toBytes(i));
        Put p = new Put(row);
        for (int j = 0; j < families.length; ++j) {
          p.addColumn(families[j], qualifier, row);
        }
        puts.add(p);
      }
      table.putAll(puts).join();
      TEST_UTIL.flush();
      puts.clear();
    }
  }
}
