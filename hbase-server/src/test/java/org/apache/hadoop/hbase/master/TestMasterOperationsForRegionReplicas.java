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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterOperationsForRegionReplicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterOperationsForRegionReplicas.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Connection CONNECTION = null;
  private static Admin ADMIN;
  private static int numSlaves = 2;
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(numSlaves);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    ADMIN = CONNECTION.getAdmin();
    while (ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics()
      .size() < numSlaves) {
      Thread.sleep(100);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ADMIN, true);
    Closeables.close(CONNECTION, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTableWithSingleReplica() throws Exception {
    final int numRegions = 3;
    final int numReplica = 1;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(numReplica)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();

      validateNumberOfRowsInMeta(tableName, numRegions, ADMIN.getConnection());
      List<RegionInfo> hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), tableName);
      assertEquals(numRegions * numReplica, hris.size());
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  @Test
  public void testCreateTableWithMultipleReplicas() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final int numRegions = 3;
    final int numReplica = 2;
    try {
      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(numReplica)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      validateNumberOfRowsInMeta(tableName, numRegions, ADMIN.getConnection());

      List<RegionInfo> hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), tableName);
      assertEquals(numRegions * numReplica, hris.size());
      // check that the master created expected number of RegionState objects
      for (int i = 0; i < numRegions; i++) {
        for (int j = 0; j < numReplica; j++) {
          RegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), j);
          RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionState(replica);
          assertNotNull(state);
        }
      }

      List<Result> metaRows = MetaTableAccessor.fullScanRegions(ADMIN.getConnection());
      int numRows = 0;
      for (Result result : metaRows) {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        RegionInfo hri = locations.getRegionLocation().getRegion();
        if (!hri.getTable().equals(tableName)) continue;
        numRows += 1;
        HRegionLocation[] servers = locations.getRegionLocations();
        // have two locations for the replicas of a region, and the locations should be different
        assertEquals(2, servers.length);
        assertNotEquals(servers[1], servers[0]);
      }
      assertEquals(numRegions, numRows);

      // The same verification of the meta as above but with the SnapshotOfRegionAssignmentFromMeta
      // class
      validateFromSnapshotFromMeta(TEST_UTIL, tableName, numRegions, numReplica,
        ADMIN.getConnection());

      // Now kill the master, restart it and see if the assignments are kept
      ServerName master = TEST_UTIL.getHBaseClusterInterface().getClusterMetrics().getMasterName();
      TEST_UTIL.getHBaseClusterInterface().stopMaster(master);
      TEST_UTIL.getHBaseClusterInterface().waitForMasterToStop(master, 30000);
      TEST_UTIL.getHBaseClusterInterface().startMaster(master.getHostname(), master.getPort());
      TEST_UTIL.getHBaseClusterInterface().waitForActiveAndReadyMaster();
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      for (int i = 0; i < numRegions; i++) {
        for (int j = 0; j < numReplica; j++) {
          RegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), j);
          RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionState(replica);
          assertNotNull(state);
        }
      }
      validateFromSnapshotFromMeta(TEST_UTIL, tableName, numRegions, numReplica,
        ADMIN.getConnection());

      // Now shut the whole cluster down, and verify regions are assigned even if there is only
      // one server running
      TEST_UTIL.shutdownMiniHBaseCluster();
      TEST_UTIL.startMiniHBaseCluster(1, 1);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      validateSingleRegionServerAssignment(ADMIN.getConnection(), numRegions, numReplica);
      for (int i = 1; i < numSlaves; i++) { // restore the cluster
        TEST_UTIL.getMiniHBaseCluster().startRegionServer();
      }

      // Check on alter table
      ADMIN.disableTable(tableName);
      assertTrue(ADMIN.isTableDisabled(tableName));
      // increase the replica
      ADMIN.modifyTable(
        TableDescriptorBuilder.newBuilder(desc).setRegionReplication(numReplica + 1).build());
      ADMIN.enableTable(tableName);
      LOG.info(ADMIN.getDescriptor(tableName).toString());
      assertTrue(ADMIN.isTableEnabled(tableName));
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      List<RegionInfo> regions = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionsOfTable(tableName);
      assertTrue("regions.size=" + regions.size() + ", numRegions=" + numRegions + ", numReplica=" +
        numReplica, regions.size() == numRegions * (numReplica + 1));

      // decrease the replica(earlier, table was modified to have a replica count of numReplica + 1)
      ADMIN.disableTable(tableName);
      ADMIN.modifyTable(
        TableDescriptorBuilder.newBuilder(desc).setRegionReplication(numReplica).build());
      ADMIN.enableTable(tableName);
      assertTrue(ADMIN.isTableEnabled(tableName));
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      regions = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
        .getRegionsOfTable(tableName);
      assertEquals(numRegions * numReplica, regions.size());
      // also make sure the meta table has the replica locations removed
      hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), tableName);
      assertEquals(numRegions * numReplica, hris.size());
      // just check that the number of default replica regions in the meta table are the same
      // as the number of regions the table was created with, and the count of the
      // replicas is numReplica for each region
      Map<RegionInfo, Integer> defaultReplicas = new HashMap<>();
      for (RegionInfo hri : hris) {
        RegionInfo regionReplica0 = RegionReplicaUtil.getRegionInfoForDefaultReplica(hri);
        Integer i = defaultReplicas.get(regionReplica0);
        defaultReplicas.put(regionReplica0, i == null ? 1 : i + 1);
      }
      assertEquals(numRegions, defaultReplicas.size());
      Collection<Integer> counts = new HashSet<>(defaultReplicas.values());
      assertEquals(1, counts.size());
      assertTrue(counts.contains(numReplica));
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  @Test
  @Ignore("Enable when we have support for alter_table- HBASE-10361")
  public void testIncompleteMetaTableReplicaInformation() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final int numRegions = 3;
    final int numReplica = 2;
    try {
      // Create a table and let the meta table be updated with the location of the
      // region locations.
      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(numReplica)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      Set<byte[]> tableRows = new HashSet<>();
      List<RegionInfo> hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), tableName);
      for (RegionInfo hri : hris) {
        tableRows.add(hri.getRegionName());
      }
      ADMIN.disableTable(tableName);
      // now delete one replica info from all the rows
      // this is to make the meta appear to be only partially updated
      Table metaTable = ADMIN.getConnection().getTable(TableName.META_TABLE_NAME);
      for (byte[] row : tableRows) {
        Delete deleteOneReplicaLocation = new Delete(row);
        deleteOneReplicaLocation.addColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getServerColumn(1));
        deleteOneReplicaLocation.addColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getSeqNumColumn(1));
        deleteOneReplicaLocation.addColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getStartCodeColumn(1));
        metaTable.delete(deleteOneReplicaLocation);
      }
      metaTable.close();
      // even if the meta table is partly updated, when we re-enable the table, we should
      // get back the desired number of replicas for the regions
      ADMIN.enableTable(tableName);
      assertTrue(ADMIN.isTableEnabled(tableName));
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      List<RegionInfo> regions = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionsOfTable(tableName);
      assertEquals(numRegions * numReplica, regions.size());
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }

  private void validateNumberOfRowsInMeta(final TableName table, int numRegions,
      Connection connection) throws IOException {
    assert (ADMIN.tableExists(table));
    final AtomicInteger count = new AtomicInteger();
    Visitor visitor = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (MetaTableAccessor.getRegionInfo(r).getTable().equals(table)) count.incrementAndGet();
        return true;
      }
    };
    MetaTableAccessor.fullScanRegions(connection, visitor);
    assertEquals(numRegions, count.get());
  }

  private void validateFromSnapshotFromMeta(HBaseTestingUtility util, TableName table,
      int numRegions, int numReplica, Connection connection) throws IOException {
    SnapshotOfRegionAssignmentFromMeta snapshot =
      new SnapshotOfRegionAssignmentFromMeta(connection);
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToServerMap = snapshot.getRegionToRegionServerMap();
    assert (regionToServerMap.size() == numRegions * numReplica + 1); // '1' for the namespace
    Map<ServerName, List<RegionInfo>> serverToRegionMap = snapshot.getRegionServerToRegionMap();
    for (Map.Entry<ServerName, List<RegionInfo>> entry : serverToRegionMap.entrySet()) {
      if (entry.getKey().equals(util.getHBaseCluster().getMaster().getServerName())) {
        continue;
      }
      List<RegionInfo> regions = entry.getValue();
      Set<byte[]> setOfStartKeys = new HashSet<>();
      for (RegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (region.getTable().equals(table)) {
          setOfStartKeys.add(startKey); // ignore other tables
          LOG.info("--STARTKEY {}--", new String(startKey, StandardCharsets.UTF_8));
        }
      }
      // the number of startkeys will be equal to the number of regions hosted in each server
      // (each server will be hosting one replica of a region)
      assertEquals(numRegions, setOfStartKeys.size());
    }
  }

  private void validateSingleRegionServerAssignment(Connection connection, int numRegions,
      int numReplica) throws IOException {
    SnapshotOfRegionAssignmentFromMeta snapshot =
      new SnapshotOfRegionAssignmentFromMeta(connection);
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToServerMap = snapshot.getRegionToRegionServerMap();
    assertEquals(regionToServerMap.size(), numRegions * numReplica + 1);
    Map<ServerName, List<RegionInfo>> serverToRegionMap = snapshot.getRegionServerToRegionMap();
    assertEquals("One Region Only", 1, serverToRegionMap.keySet().size());
    for (Map.Entry<ServerName, List<RegionInfo>> entry : serverToRegionMap.entrySet()) {
      if (entry.getKey().equals(TEST_UTIL.getHBaseCluster().getMaster().getServerName())) {
        continue;
      }
      assertEquals(entry.getValue().size(), numRegions * numReplica + 1);
    }
  }
}
