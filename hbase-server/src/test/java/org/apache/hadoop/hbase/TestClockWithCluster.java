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

package org.apache.hadoop.hbase;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class})
public class TestClockWithCluster {
  private static final Log LOG = LogFactory.getLog(TestClockWithCluster.class);
  private static final Clock SYSTEM_CLOCK = new SystemClock();
  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtility HBTU = new HBaseTestingUtility();
  private static Connection connection;

  private Admin admin;
  private TableName tableName;
  private Table table;

  // Test names
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  @BeforeClass
  public static void setupClass() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    HBTU.startMiniCluster(NUM_MASTERS, NUM_RS);
    connection = ConnectionFactory.createConnection(HBTU.getConfiguration());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    connection.close();
    HBTU.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    admin = connection.getAdmin();
    tableName = TableName.valueOf(name.getMethodName());
    admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(new HColumnDescriptor(TEST_FAMILY))
        .build());
    table = connection.getTable(tableName);
  }

  @After
  public void teardown() throws Exception {
    try {
      if (table != null) {
        table.close();
      }
    } finally {
      try {
        HBTU.deleteTable(tableName);
      } catch (IOException ioe) {
        LOG.error("Failed deleting table '" + tableName + "' during teardown. Exception:" + ioe);
      }
    }
  }

  private void verifyTimestamps(Table table, final byte[] f, int startRow, int endRow,
      TimestampType timestamp) throws IOException {
    for (int i = startRow; i < endRow; i++) {
      String failMsg = "Failed verification of row :" + i;
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Get get = new Get(data);
      Result result = table.get(get);
      Cell cell = result.getColumnLatestCell(f, null);
      assertTrue(failMsg, timestamp.isLikelyOfType(cell.getTimestamp()));
    }
  }

  @Test
  public void testNewTablesAreCreatedWithSystemClock() throws IOException {
    ClockType clockType = admin.getTableDescriptor(tableName).getClockType();
    assertEquals(ClockType.SYSTEM, clockType);
    // write
    HBTU.loadNumericRows(table, TEST_FAMILY, 0, 1000);
    // read , check if the it is same.
    HBTU.verifyNumericRows(table, TEST_FAMILY, 0, 1000, 0);

    // This check will be useful if Clock type were to be system monotonic or hybrid logical.
    verifyTimestamps(table, TEST_FAMILY, 0, 1000, TimestampType.PHYSICAL);
  }

  @Test
  public void testMetaTableClockTypeIsHLC() throws IOException {
    ClockType clockType = admin
      .getTableDescriptor(TableName.META_TABLE_NAME).getClockType();
    assertEquals(ClockType.HYBRID_LOGICAL, clockType);
  }

  @Test
  public void testMetaTableTimestampsAreHLC() throws IOException {
    // Checks timestamps of whatever is present in meta table currently.
    // ToDo: Include complete meta table sample with all column families to check all paths of
    // meta table modification.
    Table table = connection.getTable(TableName.META_TABLE_NAME);
    Result result = table.getScanner(new Scan()).next();
    for (Cell cell : result.rawCells()) {
      assertTrue(TimestampType.HYBRID.isLikelyOfType(cell.getTimestamp()));
    }
  }

  private long getColumnLatestCellTimestamp(HRegionInfo hri) throws IOException {
    Result result = MetaTableAccessor.getRegionResult(connection, hri.getRegionName());
    Cell cell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
    return cell.getTimestamp();
  }

  private void assertHLCTime(HybridLogicalClock clock,
      long expectedPhysicalTime, long expectedLogicalTime) {
    assertEquals(expectedPhysicalTime, clock.getPhysicalTime());
    assertEquals(expectedLogicalTime, clock.getLogicalTime());
  }

  @Test
  public void testRegionStateTransitionTimestampsIncreaseMonotonically() throws Exception {
    HRegionServer rs = HBTU.getRSForFirstRegionInTable(tableName);
    List<Region> regions = rs.getOnlineRegions();

    assert(!regions.isEmpty());

    MiniHBaseCluster cluster = HBTU.getHBaseCluster();

    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = cluster.getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    RegionStates regionStates = master.getAssignmentManager().getRegionStates();

    assertEquals(3, cluster.countServedRegions());
    HRegionInfo hriOnline;
    try (RegionLocator locator =
        HBTU.getConnection().getRegionLocator(tableName)) {
      hriOnline = locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    }

    HRegion regionMeta = null;
    for (Region r : master.getOnlineRegions()) {
      if (r.getRegionInfo().isMetaRegion()) {
        regionMeta = ((HRegion) r);
      }
    }

    assertNotNull(regionMeta);

    // Inject physical clock that always returns same physical time into hybrid logical clock
    long systemTime = SYSTEM_CLOCK.now();
    Clock mockSystemClock = mock(Clock.class);
    when(mockSystemClock.now()).thenReturn(systemTime);
    when(mockSystemClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    when(mockSystemClock.isMonotonic()).thenReturn(true);
    HybridLogicalClock masterHLC =
        new HybridLogicalClock(new SystemMonotonicClock(mockSystemClock));

    // The region clock is used for setting timestamps for table mutations and the region server
    // clock is used for updating the clock on region assign/unassign events.

    // Set meta region clock so that region state transitions are timestamped with mocked clock
    regionMeta.setClock(masterHLC);
    master.setClock(masterHLC);

    HRegion userRegion = null;
    for (Region region : regions) {
      if (region.getRegionInfo().getTable().equals(tableName)) {
        userRegion = (HRegion) region;
      }
    }
    assertNotNull(userRegion);

    // Only mock the region server clock because the region clock does not get used during
    // unassignment and assignment
    rs.setClock(masterHLC);

    // Repeatedly unassign and assign region while tracking the timestamps of the region state
    // transitions from the meta table
    List<Long> timestamps = new ArrayList<>();
    // Set expected logical time to 0 as initial clock.now() sets clock's logical time to 0
    long expectedLogicalTime = TimestampType.HYBRID.getLogicalTime(masterHLC.now());
    for (int i = 0; i < 10; i++) {
      admin.unassign(hriOnline.getRegionName(), false);
      assertEquals(RegionState.State.CLOSED, regionStates.getRegionState(hriOnline).getState());
      // clock.now() is called 8 times and clock.update() is called 2 times, each call increments
      // the logical time by one.
      // 0   [now]    Get region info from hbase:meta in HBaseAdmin#unassign
      // 1   [now]    Get region info from hbase:meta in MasterRpcServices#unassignRegion
      // 2,3 [now]    Update hbase:meta
      // 4   [now]    Send unassign region request to region server
      // 5   [update] Upon region region server clock upon receiving unassign region request
      // 6   [now]    Send region server response back to master
      // 7   [update] Update master clock upon close region response form region server
      // 8,9 [now]    Update hbase:meta
      expectedLogicalTime += 10;

      assertEquals(expectedLogicalTime, masterHLC.getLogicalTime());
      timestamps.add(masterHLC.getLogicalTime());

      admin.assign(hriOnline.getRegionName());
      // clock.now() is called 7 times and clock.update() is called 2 times, each call increments
      // the logical time by one.
      // 0   [now]    Get region info from hbase:meta in HBaseAdmin#unassign
      // 1,2 [now]    Update hbase:meta
      // 3   [now]    Send unassign region request to region server
      // 4   [update] Update region region server clock upon receiving unassign region request
      // 5   [now]    Send region server response back to master
      // 6   [update] Update master clock upon close region response form region server
      // 7,8 [now]    Update hbase:meta
      // Assignment has one less call to clock.now() because MasterRpcServices#assignRegion instead
      // gets the region info from assignment manager rather than meta table accessor
      expectedLogicalTime += 9;
      assertEquals(RegionState.State.OPEN, regionStates.getRegionState(hriOnline).getState());
      assertEquals(expectedLogicalTime, masterHLC.getLogicalTime());
      timestamps.add(masterHLC.getLogicalTime());
    }

    // Ensure that the hybrid timestamps are strictly increasing
    for (int i = 0; i < timestamps.size() - 1; i++) {
      if (timestamps.get(i) >= timestamps.get(i + 1)) {
        Assert.fail("Current ts is " + timestamps.get(i)
            + ", but the next ts is equal or smaller " + timestamps.get(i + 1));
      }
    }
  }

  @Test
  public void testRegionOpenAndCloseClockUpdates() throws Exception {
    HRegionServer rs = HBTU.getRSForFirstRegionInTable(tableName);
    List<Region> regions = rs.getOnlineRegions();

    assert(!regions.isEmpty());

    MiniHBaseCluster cluster = HBTU.getHBaseCluster();

    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = cluster.getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    RegionStates regionStates = master.getAssignmentManager().getRegionStates();

    HRegionInfo hriOnline;
    try (RegionLocator locator =
        HBTU.getConnection().getRegionLocator(tableName)) {
      hriOnline = locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    }

    HRegion regionMeta = null;
    for (Region r : master.getOnlineRegions()) {
      if (r.getRegionInfo().isMetaRegion()) {
        regionMeta = ((HRegion) r);
      }
    }

    assertNotNull(regionMeta);

    // Instantiate two hybrid logical clocks with mocked physical clocks
    long expectedPhysicalTime = SYSTEM_CLOCK.now();
    Clock masterMockSystemClock = mock(Clock.class);
    when(masterMockSystemClock.now()).thenReturn(expectedPhysicalTime);
    when(masterMockSystemClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    HybridLogicalClock masterHLC =
        new HybridLogicalClock(new SystemMonotonicClock(masterMockSystemClock));
    master.setClock(masterHLC);
    regionMeta.setClock(masterHLC);

    Clock rsMockSystemClock = mock(Clock.class);
    when(rsMockSystemClock.now()).thenReturn(expectedPhysicalTime);
    when(rsMockSystemClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    when(rsMockSystemClock.isMonotonic()).thenReturn(true);
    HybridLogicalClock rsHLC =
        new HybridLogicalClock(new SystemMonotonicClock(rsMockSystemClock));
    // We only mock the region server clock here because the region clock does not get used
    // during unassignment and assignment
    rs.setClock(rsHLC);

    // Increment master physical clock time
    expectedPhysicalTime += 1000;
    when(masterMockSystemClock.now()).thenReturn(expectedPhysicalTime);

    // Unassign region, region server should advance its clock upon receiving close region request
    admin.unassign(hriOnline.getRegionName(), false);
    assertEquals(RegionState.State.CLOSED, regionStates.getRegionState(hriOnline).getState());
    // Verify that region server clock time increased
    // Previous test has explanation for each event that increases logical time
    assertHLCTime(masterHLC, expectedPhysicalTime, 9);
    assertHLCTime(rsHLC, expectedPhysicalTime, 6);

    // Increase region server physical clock time
    expectedPhysicalTime += 1000;
    when(rsMockSystemClock.now()).thenReturn(expectedPhysicalTime);
    // Assign region, master server should advance its clock upon receiving close region response
    admin.assign(hriOnline.getRegionName());
    assertEquals(RegionState.State.OPEN, regionStates.getRegionState(hriOnline).getState());
    // Verify that master clock time increased
    assertHLCTime(masterHLC, expectedPhysicalTime, 4);
    assertHLCTime(rsHLC, expectedPhysicalTime, 1);

    // Increment region server physical clock time
    expectedPhysicalTime += 1000;
    when(rsMockSystemClock.now()).thenReturn(expectedPhysicalTime);
    // Unassign region, region server should advance its clock upon receiving close region request
    admin.unassign(hriOnline.getRegionName(), false);
    assertEquals(RegionState.State.CLOSED, regionStates.getRegionState(hriOnline).getState());
    // Verify that master server clock time increased
    assertHLCTime(masterHLC, expectedPhysicalTime, 4);
    assertHLCTime(rsHLC, expectedPhysicalTime, 1);

    // Increase master server physical clock time
    expectedPhysicalTime += 1000;
    when(masterMockSystemClock.now()).thenReturn(expectedPhysicalTime);
    // Assign region, master server should advance its clock upon receiving close region response
    admin.assign(hriOnline.getRegionName());
    assertEquals(RegionState.State.OPEN, regionStates.getRegionState(hriOnline).getState());
    // Verify that region server clock time increased
    assertHLCTime(masterHLC, expectedPhysicalTime, 8);
    assertHLCTime(rsHLC, expectedPhysicalTime, 5);
  }
}