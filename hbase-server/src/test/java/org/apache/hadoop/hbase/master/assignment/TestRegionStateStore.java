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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.TestMetaTableAccessor.assertEmptyMetaLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionStateStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionStateStore.class);

  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testVisitMetaForRegionExistingRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testVisitMetaForRegion");
    UTIL.createTable(tableName, "cf");
    final List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    final String encodedName = regions.get(0).getRegionInfo().getEncodedName();
    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
    final AtomicBoolean visitorCalled = new AtomicBoolean(false);
    regionStateStore.visitMetaForRegion(encodedName, new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, RegionInfo regionInfo, RegionState.State state,
        ServerName regionLocation, ServerName lastHost, long openSeqNum) {
        assertEquals(encodedName, regionInfo.getEncodedName());
        visitorCalled.set(true);
      }
    });
    assertTrue("Visitor has not been called.", visitorCalled.get());
  }

  @Test
  public void testVisitMetaForBadRegionState() throws Exception {
    final TableName tableName = TableName.valueOf("testVisitMetaForBadRegionState");
    UTIL.createTable(tableName, "cf");
    final List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    final String encodedName = regions.get(0).getRegionInfo().getEncodedName();
    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();

    // add the BAD_STATE which does not exist in enum RegionState.State
    Put put =
      new Put(regions.get(0).getRegionInfo().getRegionName(), EnvironmentEdgeManager.currentTime());
    put.addColumn(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER,
      Bytes.toBytes("BAD_STATE"));

    try (Table table = UTIL.getConnection().getTable(MetaTableName.getInstance())) {
      table.put(put);
    }

    final AtomicBoolean visitorCalled = new AtomicBoolean(false);
    regionStateStore.visitMetaForRegion(encodedName, new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, RegionInfo regionInfo, RegionState.State state,
        ServerName regionLocation, ServerName lastHost, long openSeqNum) {
        assertEquals(encodedName, regionInfo.getEncodedName());
        assertNull(state);
        visitorCalled.set(true);
      }
    });
    assertTrue("Visitor has not been called.", visitorCalled.get());
  }

  @Test
  public void testVisitMetaForRegionNonExistingRegion() throws Exception {
    final String encodedName = "fakeencodedregionname";
    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
    final AtomicBoolean visitorCalled = new AtomicBoolean(false);
    regionStateStore.visitMetaForRegion(encodedName, new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, RegionInfo regionInfo, RegionState.State state,
        ServerName regionLocation, ServerName lastHost, long openSeqNum) {
        visitorCalled.set(true);
      }
    });
    assertFalse("Visitor has been called, but it shouldn't.", visitorCalled.get());
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionSplit() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 =
      ServerName.valueOf("foo", 60010, ThreadLocalRandom.current().nextLong());
    TableName tableName = name.getTableName();
    RegionInfo parent = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.EMPTY_END_ROW).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();

    RegionInfo splitA = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(Bytes.toBytes("a")).setSplit(false)
      .setRegionId(regionId + 1).setReplicaId(0).build();
    RegionInfo splitB = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(HConstants.EMPTY_END_ROW).setSplit(false).setRegionId(regionId + 1).setReplicaId(0)
      .build();
    List<RegionInfo> regionInfos = Lists.newArrayList(parent);
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), regionInfos, 3);
    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
    regionStateStore.splitRegion(parent, splitA, splitB, serverName0,
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(3).build());
    try (Table meta = MetaTableAccessor.getMetaHTable(UTIL.getConnection())) {
      assertEmptyMetaLocation(meta, splitA.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitA.getRegionName(), 2);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 2);
    }
  }

  @Test
  public void testEmptyMetaDaughterLocationDuringSplit() throws IOException {
    TableName tableName = name.getTableName();
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 =
      ServerName.valueOf("foo", 60010, ThreadLocalRandom.current().nextLong());
    RegionInfo parent = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.EMPTY_END_ROW).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();
    RegionInfo splitA = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(Bytes.toBytes("a")).setSplit(false)
      .setRegionId(regionId + 1).setReplicaId(0).build();
    RegionInfo splitB = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(HConstants.EMPTY_END_ROW).setSplit(false).setRegionId(regionId + 1).setReplicaId(0)
      .build();
    List<RegionInfo> regionInfos = Lists.newArrayList(parent);
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), regionInfos, 3);
    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
    regionStateStore.splitRegion(parent, splitA, splitB, serverName0,
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(3).build());
    try (Table meta = MetaTableAccessor.getMetaHTable(UTIL.getConnection())) {
      Get get1 = new Get(splitA.getRegionName());
      Result resultA = meta.get(get1);
      Cell serverCellA = resultA.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getServerColumn(splitA.getReplicaId()));
      Cell startCodeCellA = resultA.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getStartCodeColumn(splitA.getReplicaId()));
      assertNull(serverCellA);
      assertNull(startCodeCellA);

      Get get2 = new Get(splitB.getRegionName());
      Result resultB = meta.get(get2);
      Cell serverCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getServerColumn(splitB.getReplicaId()));
      Cell startCodeCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getStartCodeColumn(splitB.getReplicaId()));
      assertNull(serverCellB);
      assertNull(startCodeCellB);
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionMerge() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 =
      ServerName.valueOf("foo", 60010, ThreadLocalRandom.current().nextLong());

    TableName tableName = name.getTableName();
    RegionInfo parentA = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("a"))
      .setEndKey(HConstants.EMPTY_END_ROW).setSplit(false).setRegionId(regionId).setReplicaId(0)
      .build();

    RegionInfo parentB = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(Bytes.toBytes("a")).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();
    RegionInfo merged = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.EMPTY_END_ROW).setSplit(false)
      .setRegionId(regionId + 1).setReplicaId(0).build();

    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();

    try (Table meta = MetaTableAccessor.getMetaHTable(UTIL.getConnection())) {
      List<RegionInfo> regionInfos = Lists.newArrayList(parentA, parentB);
      MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), regionInfos, 3);
      regionStateStore.mergeRegions(merged, new RegionInfo[] { parentA, parentB }, serverName0,
        TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(3).build());
      assertEmptyMetaLocation(meta, merged.getRegionName(), 1);
      assertEmptyMetaLocation(meta, merged.getRegionName(), 2);
    }
  }

  @Test
  public void testMastersSystemTimeIsUsedInMergeRegions() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    TableName tableName = name.getTableName();

    RegionInfo regionInfoA = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(new byte[] { 'a' }).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();

    RegionInfo regionInfoB = RegionInfoBuilder.newBuilder(tableName).setStartKey(new byte[] { 'a' })
      .setEndKey(HConstants.EMPTY_END_ROW).setSplit(false).setRegionId(regionId).setReplicaId(0)
      .build();
    RegionInfo mergedRegionInfo = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.EMPTY_END_ROW).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();

    ServerName sn = ServerName.valueOf("bar", 0, 0);
    try (Table meta = MetaTableAccessor.getMetaHTable(UTIL.getConnection())) {
      List<RegionInfo> regionInfos = Lists.newArrayList(regionInfoA, regionInfoB);
      MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), regionInfos, 1);

      // write the serverName column with a big current time, but set the masters time as even
      // bigger. When region merge deletes the rows for regionA and regionB, the serverName columns
      // should not be seen by the following get
      long serverNameTime = EnvironmentEdgeManager.currentTime() + 100000000;
      long masterSystemTime = EnvironmentEdgeManager.currentTime() + 123456789;

      // write the serverName columns
      MetaTableAccessor.updateRegionLocation(UTIL.getConnection(), regionInfoA, sn, 1,
        serverNameTime);

      // assert that we have the serverName column with expected ts
      Get get = new Get(mergedRegionInfo.getRegionName());
      Result result = meta.get(get);
      Cell serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getServerColumn(0));
      assertNotNull(serverCell);
      assertEquals(serverNameTime, serverCell.getTimestamp());

      final RegionStateStore regionStateStore =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();

      ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
      edge.setValue(masterSystemTime);
      EnvironmentEdgeManager.injectEdge(edge);
      try {
        // now merge the regions, effectively deleting the rows for region a and b.
        regionStateStore.mergeRegions(mergedRegionInfo,
          new RegionInfo[] { regionInfoA, regionInfoB }, sn,
          TableDescriptorBuilder.newBuilder(tableName).build());
      } finally {
        EnvironmentEdgeManager.reset();
      }

      result = meta.get(get);
      serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getServerColumn(0));
      Cell startCodeCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getStartCodeColumn(0));
      Cell seqNumCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getSeqNumColumn(0));
      assertNull(serverCell);
      assertNull(startCodeCell);
      assertNull(seqNumCell);
    }
  }

  /**
   * Test for HBASE-23044.
   */
  @Test
  public void testGetMergeRegions() throws Exception {
    TableName tn = name.getTableName();
    UTIL.createMultiRegionTable(tn, Bytes.toBytes("CF"), 4);
    UTIL.waitTableAvailable(tn);
    Admin admin = UTIL.getAdmin();
    List<RegionInfo> regions = admin.getRegions(tn);
    assertEquals(4, regions.size());
    admin
      .mergeRegionsAsync(
        new byte[][] { regions.get(0).getRegionName(), regions.get(1).getRegionName() }, false)
      .get(60, TimeUnit.SECONDS);
    admin
      .mergeRegionsAsync(
        new byte[][] { regions.get(2).getRegionName(), regions.get(3).getRegionName() }, false)
      .get(60, TimeUnit.SECONDS);

    List<RegionInfo> mergedRegions = admin.getRegions(tn);
    assertEquals(2, mergedRegions.size());
    RegionInfo mergedRegion0 = mergedRegions.get(0);
    RegionInfo mergedRegion1 = mergedRegions.get(1);

    final RegionStateStore regionStateStore =
      UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();

    List<RegionInfo> mergeParents = regionStateStore.getMergeRegions(mergedRegion0);
    assertTrue(mergeParents.contains(regions.get(0)));
    assertTrue(mergeParents.contains(regions.get(1)));
    mergeParents = regionStateStore.getMergeRegions(mergedRegion1);
    assertTrue(mergeParents.contains(regions.get(2)));
    assertTrue(mergeParents.contains(regions.get(3)));

    // Delete merge qualifiers for mergedRegion0, then cannot getMergeRegions again
    regionStateStore.deleteMergeQualifiers(mergedRegion0);
    mergeParents = regionStateStore.getMergeRegions(mergedRegion0);
    assertNull(mergeParents);

    mergeParents = regionStateStore.getMergeRegions(mergedRegion1);
    assertTrue(mergeParents.contains(regions.get(2)));
    assertTrue(mergeParents.contains(regions.get(3)));
  }

  @Test
  public void testAddMergeRegions() throws IOException {
    TableName tn = name.getTableName();
    Put put = new Put(Bytes.toBytes(name.getTableName().getNameAsString()));
    List<RegionInfo> ris = new ArrayList<>();
    int limit = 10;
    byte[] previous = HConstants.EMPTY_START_ROW;
    for (int i = 0; i < limit; i++) {
      RegionInfo ri =
        RegionInfoBuilder.newBuilder(tn).setStartKey(previous).setEndKey(Bytes.toBytes(i)).build();
      ris.add(ri);
    }
    put = RegionStateStore.addMergeRegions(put, ris);
    List<Cell> cells = put.getFamilyCellMap().get(HConstants.CATALOG_FAMILY);
    String previousQualifier = null;
    assertEquals(limit, cells.size());
    for (Cell cell : cells) {
      String qualifier = Bytes.toString(cell.getQualifierArray());
      assertTrue(qualifier.startsWith(HConstants.MERGE_QUALIFIER_PREFIX_STR));
      assertNotEquals(qualifier, previousQualifier);
      previousQualifier = qualifier;
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsRemovedAtTableDeletion() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    TableName tableName = name.getTableName();
    RegionInfo primary = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.EMPTY_END_ROW).setSplit(false)
      .setRegionId(regionId).setReplicaId(0).build();

    try (Table meta = MetaTableAccessor.getMetaHTable(UTIL.getConnection())) {
      List<RegionInfo> regionInfos = Lists.newArrayList(primary);
      MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), regionInfos, 3);
      final RegionStateStore regionStateStore =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
      regionStateStore.removeRegionReplicas(tableName, 3, 1);
      Get get = new Get(primary.getRegionName());
      Result result = meta.get(get);
      for (int replicaId = 0; replicaId < 3; replicaId++) {
        Cell serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          CatalogFamilyFormat.getServerColumn(replicaId));
        Cell startCodeCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          CatalogFamilyFormat.getStartCodeColumn(replicaId));
        Cell stateCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          CatalogFamilyFormat.getRegionStateColumn(replicaId));
        Cell snCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          CatalogFamilyFormat.getServerNameColumn(replicaId));
        if (replicaId == 0) {
          assertNotNull(stateCell);
        } else {
          assertNull(serverCell);
          assertNull(startCodeCell);
          assertNull(stateCell);
          assertNull(snCell);
        }
      }
    }
  }
}
