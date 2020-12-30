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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous region admin operations.
 * @see TestAsyncRegionAdminApi2 This test and it used to be joined it was taking longer than our
 * ten minute timeout so they were split.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi extends TestAsyncAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncRegionAdminApi.class);

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
    try {
      admin.assign(hri.getRegionName()).get();
      fail("Should fail when assigning an already onlined region");
    } catch (ExecutionException e) {
      // Expected
      assertThat(e.getCause(), instanceOf(DoNotRetryRegionException.class));
    }
    assertFalse(am.getRegionStates().getRegionStateNode(hri).isInTransition());
    assertTrue(regionStates.getRegionState(hri).isOpened());

    // unassign region
    admin.unassign(hri.getRegionName(), true).get();
    assertFalse(am.getRegionStates().getRegionStateNode(hri).isInTransition());
    assertTrue(regionStates.getRegionState(hri).isClosed());
  }

  RegionInfo createTableAndGetOneRegion(final TableName tableName)
      throws IOException, InterruptedException, ExecutionException {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
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
    admin.balancerSwitch(false).join();

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
    admin.balancerSwitch(true).join();
  }

  @Test
  public void testGetOnlineRegions() throws Exception {
    createTableAndGetOneRegion(tableName);
    AtomicInteger regionServerCount = new AtomicInteger(0);
    TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().stream()
      .map(rsThread -> rsThread.getRegionServer()).forEach(rs -> {
        ServerName serverName = rs.getServerName();
        try {
          assertEquals(admin.getRegions(serverName).get().size(), rs.getRegions().size());
        } catch (Exception e) {
          fail("admin.getOnlineRegions() method throws a exception: " + e.getMessage());
        }
        regionServerCount.incrementAndGet();
      });
    assertEquals(TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size(),
      regionServerCount.get());
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
    assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize() > 0);
    // flush region and wait flush operation finished.
    LOG.info("flushing region: " + Bytes.toStringBinary(hri.getRegionName()));
    admin.flushRegion(hri.getRegionName()).get();
    LOG.info("blocking until flush is complete: " + Bytes.toStringBinary(hri.getRegionName()));
    Threads.sleepWithoutInterrupt(500);
    while (regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize() > 0) {
      Threads.sleep(50);
    }
    // check the memstore.
    assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize(), 0);

    // write another put into the specific region
    ASYNC_CONN.getTable(tableName)
        .put(new Put(hri.getStartKey()).addColumn(FAMILY, FAMILY_0, Bytes.toBytes("value-2")))
        .join();
    assertTrue(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize() > 0);
    admin.flush(tableName).get();
    Threads.sleepWithoutInterrupt(500);
    while (regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize() > 0) {
      Threads.sleep(50);
    }
    // check the memstore.
    assertEquals(regionServer.getOnlineRegion(hri.getRegionName()).getMemStoreDataSize(), 0);
  }

  private void waitUntilMobCompactionFinished(TableName tableName)
      throws ExecutionException, InterruptedException {
    long finished = EnvironmentEdgeManager.currentTime() + 60000;
    CompactionState state = admin.getCompactionState(tableName, CompactType.MOB).get();
    while (EnvironmentEdgeManager.currentTime() < finished) {
      if (state == CompactionState.NONE) {
        break;
      }
      Thread.sleep(10);
      state = admin.getCompactionState(tableName, CompactType.MOB).get();
    }
    assertEquals(CompactionState.NONE, state);
  }

  @Test
  public void testCompactMob() throws Exception {
    ColumnFamilyDescriptor columnDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"))
            .setMobEnabled(true).setMobThreshold(0).build();

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(columnDescriptor).build();

    admin.createTable(tableDescriptor).get();

    byte[][] families = { Bytes.toBytes("mob") };
    loadData(tableName, families, 3000, 8);

    admin.majorCompact(tableName, CompactType.MOB).get();

    CompactionState state = admin.getCompactionState(tableName, CompactType.MOB).get();
    assertNotEquals(CompactionState.NONE, state);

    waitUntilMobCompactionFinished(tableName);
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
    assertEquals(1, regions.size());
    int countBefore = countStoreFilesInFamilies(regions, families);
    assertTrue(countBefore > 0);

    // Minor compaction for all region servers.
    for (HRegionServer rs : rsList)
      admin.compactRegionServer(rs.getServerName()).get();
    Thread.sleep(5000);
    int countAfterMinorCompaction = countStoreFilesInFamilies(regions, families);
    assertTrue(countAfterMinorCompaction < countBefore);

    // Major compaction for all region servers.
    for (HRegionServer rs : rsList)
      admin.majorCompactRegionServer(rs.getServerName()).get();
    Thread.sleep(5000);
    int countAfterMajorCompaction = countStoreFilesInFamilies(regions, families);
    assertEquals(3, countAfterMajorCompaction);
  }

  @Test
  public void testCompactionSwitchStates() throws Exception {
    // Create a table with regions
    byte[] family = Bytes.toBytes("family");
    byte[][] families = {family, Bytes.add(family, Bytes.toBytes("2")),
        Bytes.add(family, Bytes.toBytes("3"))};
    createTableWithDefaultConf(tableName, null, families);
    loadData(tableName, families, 3000, 8);
    List<Region> regions = new ArrayList<>();
    TEST_UTIL
        .getHBaseCluster()
        .getLiveRegionServerThreads()
        .forEach(rsThread -> regions.addAll(rsThread.getRegionServer().getRegions(tableName)));
    CompletableFuture<Map<ServerName, Boolean>> listCompletableFuture =
        admin.compactionSwitch(true, new ArrayList<>());
    Map<ServerName, Boolean> pairs = listCompletableFuture.get();
    for (Map.Entry<ServerName, Boolean> p : pairs.entrySet()) {
      assertEquals("Default compaction state, expected=enabled actual=disabled",
          true, p.getValue());
    }
    CompletableFuture<Map<ServerName, Boolean>> listCompletableFuture1 =
        admin.compactionSwitch(false, new ArrayList<>());
    Map<ServerName, Boolean> pairs1 = listCompletableFuture1.get();
    for (Map.Entry<ServerName, Boolean> p : pairs1.entrySet()) {
      assertEquals("Last compaction state, expected=enabled actual=disabled",
          true, p.getValue());
    }
    CompletableFuture<Map<ServerName, Boolean>> listCompletableFuture2 =
        admin.compactionSwitch(true, new ArrayList<>());
    Map<ServerName, Boolean> pairs2 = listCompletableFuture2.get();
    for (Map.Entry<ServerName, Boolean> p : pairs2.entrySet()) {
      assertEquals("Last compaction state, expected=disabled actual=enabled",
          false, p.getValue());
    }
    ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0)
        .getServerName();
    List<String> serverNameList = new ArrayList<String>();
    serverNameList.add(serverName.getServerName());
    CompletableFuture<Map<ServerName, Boolean>> listCompletableFuture3 =
        admin.compactionSwitch(false, serverNameList);
    Map<ServerName, Boolean> pairs3 = listCompletableFuture3.get();
    assertEquals(pairs3.entrySet().size(), 1);
    for (Map.Entry<ServerName, Boolean> p : pairs3.entrySet()) {
      assertEquals("Last compaction state, expected=enabled actual=disabled",
          true, p.getValue());
    }
    CompletableFuture<Map<ServerName, Boolean>> listCompletableFuture4 =
        admin.compactionSwitch(true, serverNameList);
    Map<ServerName, Boolean> pairs4 = listCompletableFuture4.get();
    assertEquals(pairs4.entrySet().size(), 1);
    for (Map.Entry<ServerName, Boolean> p : pairs4.entrySet()) {
      assertEquals("Last compaction state, expected=disabled actual=enabled",
          false, p.getValue());
    }
  }

  @Test
  public void testCompact() throws Exception {
    compactionTest(TableName.valueOf("testCompact1"), 15, CompactionState.MINOR, false);
    compactionTest(TableName.valueOf("testCompact2"), 15, CompactionState.MINOR, true);

    // For major compaction, set up a higher hbase.hstore.compaction.min to avoid
    // minor compactions. It is a hack to avoid random delays introduced by Admins's
    // updateConfiguration() method.
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration conf = thread.getRegionServer().getConfiguration();
      conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 25);
    });

    compactionTest(TableName.valueOf("testCompact3"), 8, CompactionState.MAJOR, false);
    compactionTest(TableName.valueOf("testCompact4"), 8, CompactionState.MAJOR, true);

    // Restore to default
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration conf = thread.getRegionServer().getConfiguration();
      conf.unset(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY);
    });
  }

  private void compactionTest(final TableName tableName, final int flushes,
      final CompactionState expectedState, boolean singleFamily) throws Exception {
    // Create a table with regions
    byte[] family = Bytes.toBytes("family");
    byte[][] families =
        { family, Bytes.add(family, Bytes.toBytes("2")), Bytes.add(family, Bytes.toBytes("3")) };
    createTableWithDefaultConf(tableName, null, families);

    byte[][] singleFamilyArray = { family };

    // When singleFamily is true, only load data for the family being tested. This is to avoid
    // the case that while major compaction is going on for the family, minor compaction could
    // happen for other families at the same time (Two compaction threads long/short), thus
    // pollute the compaction and store file numbers for the region.
    if (singleFamily) {
      loadData(tableName, singleFamilyArray, 3000, flushes);
    } else {
      loadData(tableName, families, 3000, flushes);
    }

    List<Region> regions = new ArrayList<>();
    TEST_UTIL
        .getHBaseCluster()
        .getLiveRegionServerThreads()
        .forEach(rsThread -> regions.addAll(rsThread.getRegionServer().getRegions(tableName)));
    assertEquals(1, regions.size());

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
    long waitTime = 10000;
    long endt = curt + waitTime;
    CompactionState state = admin.getCompactionState(tableName).get();
    while (state == CompactionState.NONE && curt < endt) {
      Thread.sleep(1);
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
      if (expectedState == CompactionState.MAJOR) {
        assertEquals(families.length, countAfter);
      } else {
        assertTrue(families.length <= countAfter);
      }
    } else {
      int singleFamDiff = countBeforeSingleFamily - countAfterSingleFamily;
      // assert only change was to single column family
      assertEquals(singleFamDiff, (countBefore - countAfter));
      if (expectedState == CompactionState.MAJOR) {
        assertEquals(1, countAfterSingleFamily);
      } else {
        assertTrue("" + countAfterSingleFamily, 1 <= countAfterSingleFamily);
      }
    }
  }

  @Test
  public void testNonExistentTableCompaction() {
    testNonExistentTableCompaction(CompactionState.MINOR);
    testNonExistentTableCompaction(CompactionState.MAJOR);
  }

  private void testNonExistentTableCompaction(CompactionState compactionState) {
    try {
      if (compactionState == CompactionState.MINOR) {
        admin.compact(TableName.valueOf("NonExistentTable")).get();
      } else {
        admin.majorCompact(TableName.valueOf("NonExistentTable")).get();
      }
      fail("Expected TableNotFoundException when table doesn't exist");
    } catch (Exception e) {
      // expected.
      assertTrue(e.getCause() instanceof TableNotFoundException);
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

  static void loadData(final TableName tableName, final byte[][] families, final int rows)
      throws IOException {
    loadData(tableName, families, rows, 1);
  }

  static void loadData(final TableName tableName, final byte[][] families, final int rows,
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
