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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous region admin operations.
 * @see TestAsyncRegionAdminApi This test and it used to be joined it was taking longer than our ten
 *      minute timeout so they were split.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi2 extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncRegionAdminApi2.class);

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
  public void testSplitSwitch() throws Exception {
    createTableWithDefaultConf(tableName);
    byte[][] families = { FAMILY };
    final int rows = 10000;
    TestAsyncRegionAdminApi.loadData(tableName, families, rows);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    int originalCount = regionLocations.size();

    initSplitMergeSwitch();
    assertTrue(admin.splitSwitch(false).get());
    try {
      admin.split(tableName, Bytes.toBytes(rows / 2)).join();
    } catch (Exception e) {
      // Expected
    }
    int count = admin.getRegions(tableName).get().size();
    assertTrue(originalCount == count);

    assertFalse(admin.splitSwitch(true).get());
    admin.split(tableName).join();
    while ((count = admin.getRegions(tableName).get().size()) == originalCount) {
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
    TestAsyncRegionAdminApi.loadData(tableName, families, 1000);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    int originalCount = regionLocations.size();

    initSplitMergeSwitch();
    admin.split(tableName).join();
    int postSplitCount = originalCount;
    while ((postSplitCount = admin.getRegions(tableName).get().size()) == originalCount) {
      Threads.sleep(100);
    }
    assertTrue("originalCount=" + originalCount + ", postSplitCount=" + postSplitCount,
      originalCount != postSplitCount);

    // Merge switch is off so merge should NOT succeed.
    assertTrue(admin.mergeSwitch(false).get());
    List<RegionInfo> regions = admin.getRegions(tableName).get();
    assertTrue(regions.size() > 1);
    admin.mergeRegions(regions.get(0).getRegionName(), regions.get(1).getRegionName(), true).join();
    int count = admin.getRegions(tableName).get().size();
    assertTrue("postSplitCount=" + postSplitCount + ", count=" + count, postSplitCount == count);

    // Merge switch is on so merge should succeed.
    assertFalse(admin.mergeSwitch(true).get());
    admin.mergeRegions(regions.get(0).getRegionName(), regions.get(1).getRegionName(), true).join();
    count = admin.getRegions(tableName).get().size();
    assertTrue((postSplitCount / 2) == count);
  }

  private void initSplitMergeSwitch() throws Exception {
    if (!admin.isSplitEnabled().get()) {
      admin.splitSwitch(true).get();
    }
    if (!admin.isMergeEnabled().get()) {
      admin.mergeSwitch(true).get();
    }
    assertTrue(admin.isSplitEnabled().get());
    assertTrue(admin.isMergeEnabled().get());
  }

  @Test
  public void testMergeRegions() throws Exception {
    byte[][] splitRows = new byte[][] { Bytes.toBytes("3"), Bytes.toBytes("6") };
    createTableWithDefaultConf(tableName, splitRows);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    RegionInfo regionA;
    RegionInfo regionB;
    RegionInfo regionC;
    RegionInfo mergedChildRegion = null;

    // merge with full name
    assertEquals(3, regionLocations.size());
    regionA = regionLocations.get(0).getRegion();
    regionB = regionLocations.get(1).getRegion();
    regionC = regionLocations.get(2).getRegion();
    admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

    regionLocations = ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();

    assertEquals(2, regionLocations.size());
    for (HRegionLocation rl : regionLocations) {
      if (regionC.compareTo(rl.getRegion()) != 0) {
        mergedChildRegion = rl.getRegion();
        break;
      }
    }

    assertNotNull(mergedChildRegion);
    // Need to wait GC for merged child region is done.
    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    CatalogJanitor cj = services.getCatalogJanitor();
    assertTrue(cj.scan() > 0);
    // Wait until all procedures settled down
    while (!services.getMasterProcedureExecutor().getActiveProcIds().isEmpty()) {
      Thread.sleep(200);
    }
    // merge with encoded name
    admin.mergeRegions(regionC.getRegionName(), mergedChildRegion.getRegionName(), false).get();

    regionLocations = ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    assertEquals(1, regionLocations.size());
  }

  @Test
  public void testMergeRegionsInvalidRegionCount() throws Exception {
    byte[][] splitRows = new byte[][] { Bytes.toBytes("3"), Bytes.toBytes("6") };
    createTableWithDefaultConf(tableName, splitRows);
    List<RegionInfo> regions = admin.getRegions(tableName).join();
    // 0
    try {
      admin.mergeRegions(Collections.emptyList(), false).get();
      fail();
    } catch (ExecutionException e) {
      // expected
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
    // 1
    try {
      admin.mergeRegions(regions.stream().limit(1).map(RegionInfo::getEncodedNameAsBytes)
        .collect(Collectors.toList()), false).get();
      fail();
    } catch (ExecutionException e) {
      // expected
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
  }

  @Test
  public void testSplitTable() throws Exception {
    initSplitMergeSwitch();
    splitTest(TableName.valueOf("testSplitTable"), 3000, false, null);
    splitTest(TableName.valueOf("testSplitTableWithSplitPoint"), 3000, false, Bytes.toBytes("3"));
    splitTest(TableName.valueOf("testSplitTableRegion"), 3000, true, null);
    splitTest(TableName.valueOf("testSplitTableRegionWithSplitPoint2"), 3000, true,
      Bytes.toBytes("3"));
  }

  private void splitTest(TableName tableName, int rowCount, boolean isSplitRegion,
    byte[] splitPoint) throws Exception {
    // create table
    createTableWithDefaultConf(tableName);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
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
          ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
        count = regionLocations.size();
        if (count >= 2) {
          break;
        }
        Thread.sleep(1000L);
      } catch (Exception e) {
        LOG.error(e.toString(), e);
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testTruncateRegion() throws Exception {
    // Arrange - Create table, insert data, identify region to truncate.
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("30"), Bytes.toBytes("60"), Bytes.toBytes("90") };
    String family1 = "f1";
    String family2 = "f2";

    final String[] sFamilies = new String[] { family1, family2 };
    final byte[][] bFamilies = new byte[][] { Bytes.toBytes(family1), Bytes.toBytes(family2) };
    createTableWithDefaultConf(tableName, splitKeys, bFamilies);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    RegionInfo regionToBeTruncated = regionLocations.get(0).getRegion();

    assertEquals(4, regionLocations.size());

    AssignmentTestingUtil.insertData(TEST_UTIL, tableName, 2, 21, sFamilies);
    AssignmentTestingUtil.insertData(TEST_UTIL, tableName, 2, 31, sFamilies);
    AssignmentTestingUtil.insertData(TEST_UTIL, tableName, 2, 61, sFamilies);
    AssignmentTestingUtil.insertData(TEST_UTIL, tableName, 2, 91, sFamilies);
    int rowCountBeforeTruncate = TEST_UTIL.countRows(tableName);

    // Act - Truncate the first region
    admin.truncateRegion(regionToBeTruncated.getRegionName()).get();

    // Assert
    int rowCountAfterTruncate = TEST_UTIL.countRows(tableName);
    assertNotEquals(rowCountBeforeTruncate, rowCountAfterTruncate);
    int expectedRowCount = rowCountBeforeTruncate - 2;// Since region with 2 rows was truncated.
    assertEquals(expectedRowCount, rowCountAfterTruncate);
  }

  @Test
  public void testTruncateReplicaRegionNotAllowed() throws Exception {
    // Arrange - Create table, insert data, identify region to truncate.
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("30"), Bytes.toBytes("60"), Bytes.toBytes("90") };
    String family1 = "f1";
    String family2 = "f2";

    final byte[][] bFamilies = new byte[][] { Bytes.toBytes(family1), Bytes.toBytes(family2) };
    createTableWithDefaultConf(tableName, 2, splitKeys, bFamilies);

    AsyncTable<AdvancedScanResultConsumer> metaTable =
      ASYNC_CONN.getTable(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations =
      ClientMetaTableAccessor.getTableHRegionLocations(metaTable, tableName).get();
    RegionInfo primaryRegion = regionLocations.get(0).getRegion();

    RegionInfo firstReplica = RegionReplicaUtil.getRegionInfoForReplica(primaryRegion, 1);

    // Act - Truncate the first region
    try {
      admin.truncateRegion(firstReplica.getRegionName()).get();
    } catch (Exception e) {
      // Assert
      assertEquals("Expected message is different",
        "Can't truncate replicas directly.Replicas are auto-truncated "
          + "when their primary is truncated.",
        e.getCause().getMessage());
    }
  }

  @Test
  public void testTruncateRegionsMetaTableRegionsNotAllowed() throws Exception {
    AsyncTableRegionLocator locator =
      ASYNC_CONN.getRegionLocator(RegionInfoBuilder.FIRST_META_REGIONINFO.getTable());
    List<HRegionLocation> regionLocations = locator.getAllRegionLocations().get();
    HRegionLocation regionToBeTruncated = regionLocations.get(0);
    // 1
    try {
      admin.truncateRegion(regionToBeTruncated.getRegion().getRegionName()).get();
      fail();
    } catch (ExecutionException e) {
      // expected
      assertThat(e.getCause(), instanceOf(IOException.class));
      assertEquals("Can't truncate region in catalog tables", e.getCause().getMessage());
    }
  }
}
