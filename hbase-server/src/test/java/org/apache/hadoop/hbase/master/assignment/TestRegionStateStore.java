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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
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

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

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
    final TableName tableName = name.getTableName();
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
    final TableName tableName = name.getTableName();
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

    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
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
    long regionId = System.currentTimeMillis();
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
    long regionId = System.currentTimeMillis();
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

      Get get2 = new Get(splitA.getRegionName());
      Result resultB = meta.get(get2);
      Cell serverCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getServerColumn(splitB.getReplicaId()));
      Cell startCodeCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        CatalogFamilyFormat.getStartCodeColumn(splitB.getReplicaId()));
      assertNull(serverCellB);
      assertNull(startCodeCellB);
    }
  }
}
