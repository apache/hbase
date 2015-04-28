/**
 *
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterOperationsForRegionReplicas {
  private static final Log LOG = LogFactory.getLog(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Connection CONNECTION = null;
  private static Admin ADMIN;
  private static int numSlaves = 2;
  private static Configuration conf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(numSlaves);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    ADMIN = CONNECTION.getAdmin();
    while(ADMIN.getClusterStatus().getServers().size() < numSlaves) {
      Thread.sleep(100);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) ADMIN.close();
    if (CONNECTION != null && !CONNECTION.isClosed()) CONNECTION.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTableWithSingleReplica() throws Exception {
    final int numRegions = 3;
    final int numReplica = 1;
    final TableName table = TableName.valueOf("singleReplicaTable");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.setRegionReplication(numReplica);
      desc.addFamily(new HColumnDescriptor("family"));
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);

      validateNumberOfRowsInMeta(table, numRegions, ADMIN.getConnection());
      List<HRegionInfo> hris = MetaTableAccessor.getTableRegions(
        ADMIN.getConnection(), table);
      assert(hris.size() == numRegions * numReplica);
    } finally {
      ADMIN.disableTable(table);
      ADMIN.deleteTable(table);
    }
  }

  @Test
  public void testCreateTableWithMultipleReplicas() throws Exception {
    final TableName table = TableName.valueOf("fooTable");
    final int numRegions = 3;
    final int numReplica = 2;
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.setRegionReplication(numReplica);
      desc.addFamily(new HColumnDescriptor("family"));
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
      TEST_UTIL.waitTableEnabled(table);
      validateNumberOfRowsInMeta(table, numRegions, ADMIN.getConnection());

      List<HRegionInfo> hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), table);
      assert(hris.size() == numRegions * numReplica);
      // check that the master created expected number of RegionState objects
      for (int i = 0; i < numRegions; i++) {
        for (int j = 0; j < numReplica; j++) {
          HRegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), j);
          RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionState(replica);
          assert (state != null);
        }
      }

      List<Result> metaRows = MetaTableAccessor.fullScanRegions(ADMIN.getConnection());
      int numRows = 0;
      for (Result result : metaRows) {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        HRegionInfo hri = locations.getRegionLocation().getRegionInfo();
        if (!hri.getTable().equals(table)) continue;
        numRows += 1;
        HRegionLocation[] servers = locations.getRegionLocations();
        // have two locations for the replicas of a region, and the locations should be different
        assert(servers.length == 2);
        assert(!servers[0].equals(servers[1]));
      }
      assert(numRows == numRegions);

      // The same verification of the meta as above but with the SnapshotOfRegionAssignmentFromMeta
      // class
      validateFromSnapshotFromMeta(TEST_UTIL, table, numRegions, numReplica,
        ADMIN.getConnection());

      // Now kill the master, restart it and see if the assignments are kept
      ServerName master = TEST_UTIL.getHBaseClusterInterface().getClusterStatus().getMaster();
      TEST_UTIL.getHBaseClusterInterface().stopMaster(master);
      TEST_UTIL.getHBaseClusterInterface().waitForMasterToStop(master, 30000);
      TEST_UTIL.getHBaseClusterInterface().startMaster(master.getHostname(), master.getPort());
      TEST_UTIL.getHBaseClusterInterface().waitForActiveAndReadyMaster();
      for (int i = 0; i < numRegions; i++) {
        for (int j = 0; j < numReplica; j++) {
          HRegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), j);
          RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionState(replica);
          assert (state != null);
        }
      }
      validateFromSnapshotFromMeta(TEST_UTIL, table, numRegions, numReplica,
        ADMIN.getConnection());

      // Now shut the whole cluster down, and verify the assignments are kept so that the
      // availability constraints are met.
      TEST_UTIL.getConfiguration().setBoolean("hbase.master.startup.retainassign", true);
      TEST_UTIL.shutdownMiniHBaseCluster();
      TEST_UTIL.startMiniHBaseCluster(1, numSlaves);
      TEST_UTIL.waitTableEnabled(table);
      validateFromSnapshotFromMeta(TEST_UTIL, table, numRegions, numReplica,
        ADMIN.getConnection());

      // Now shut the whole cluster down, and verify regions are assigned even if there is only
      // one server running
      TEST_UTIL.shutdownMiniHBaseCluster();
      TEST_UTIL.startMiniHBaseCluster(1, 1);
      TEST_UTIL.waitTableEnabled(table);
      validateSingleRegionServerAssignment(ADMIN.getConnection(), numRegions, numReplica);
      for (int i = 1; i < numSlaves; i++) { //restore the cluster
        TEST_UTIL.getMiniHBaseCluster().startRegionServer();
      }

      //check on alter table
      ADMIN.disableTable(table);
      assert(ADMIN.isTableDisabled(table));
      //increase the replica
      desc.setRegionReplication(numReplica + 1);
      ADMIN.modifyTable(table, desc);
      ADMIN.enableTable(table);
      assert(ADMIN.isTableEnabled(table));
      List<HRegionInfo> regions = TEST_UTIL.getMiniHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates().getRegionsOfTable(table);
      assert(regions.size() == numRegions * (numReplica + 1));

      //decrease the replica(earlier, table was modified to have a replica count of numReplica + 1)
      ADMIN.disableTable(table);
      desc.setRegionReplication(numReplica);
      ADMIN.modifyTable(table, desc);
      ADMIN.enableTable(table);
      assert(ADMIN.isTableEnabled(table));
      regions = TEST_UTIL.getMiniHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates().getRegionsOfTable(table);
      assert(regions.size() == numRegions * numReplica);
      //also make sure the meta table has the replica locations removed
      hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), table);
      assert(hris.size() == numRegions * numReplica);
      //just check that the number of default replica regions in the meta table are the same
      //as the number of regions the table was created with, and the count of the
      //replicas is numReplica for each region
      Map<HRegionInfo, Integer> defaultReplicas = new HashMap<HRegionInfo, Integer>();
      for (HRegionInfo hri : hris) {
        Integer i;
        HRegionInfo regionReplica0 = RegionReplicaUtil.getRegionInfoForDefaultReplica(hri);
        defaultReplicas.put(regionReplica0,
            (i = defaultReplicas.get(regionReplica0)) == null ? 1 : i + 1);
      }
      assert(defaultReplicas.size() == numRegions);
      Collection<Integer> counts = new HashSet<Integer>(defaultReplicas.values());
      assert(counts.size() == 1 && counts.contains(new Integer(numReplica)));
    } finally {
      ADMIN.disableTable(table);
      ADMIN.deleteTable(table);
    }
  }

  //@Test (TODO: enable when we have support for alter_table- HBASE-10361).
  public void testIncompleteMetaTableReplicaInformation() throws Exception {
    final TableName table = TableName.valueOf("fooTableTest1");
    final int numRegions = 3;
    final int numReplica = 2;
    try {
      // Create a table and let the meta table be updated with the location of the
      // region locations.
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.setRegionReplication(numReplica);
      desc.addFamily(new HColumnDescriptor("family"));
      ADMIN.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
      TEST_UTIL.waitTableEnabled(table);
      Set<byte[]> tableRows = new HashSet<byte[]>();
      List<HRegionInfo> hris = MetaTableAccessor.getTableRegions(ADMIN.getConnection(), table);
      for (HRegionInfo hri : hris) {
        tableRows.add(hri.getRegionName());
      }
      ADMIN.disableTable(table);
      // now delete one replica info from all the rows
      // this is to make the meta appear to be only partially updated
      Table metaTable = ADMIN.getConnection().getTable(TableName.META_TABLE_NAME);
      for (byte[] row : tableRows) {
        Delete deleteOneReplicaLocation = new Delete(row);
        deleteOneReplicaLocation.deleteColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getServerColumn(1));
        deleteOneReplicaLocation.deleteColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getSeqNumColumn(1));
        deleteOneReplicaLocation.deleteColumns(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getStartCodeColumn(1));
        metaTable.delete(deleteOneReplicaLocation);
      }
      metaTable.close();
      // even if the meta table is partly updated, when we re-enable the table, we should
      // get back the desired number of replicas for the regions
      ADMIN.enableTable(table);
      assert(ADMIN.isTableEnabled(table));
      List<HRegionInfo> regions = TEST_UTIL.getMiniHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates().getRegionsOfTable(table);
      assert(regions.size() == numRegions * numReplica);
    } finally {
      ADMIN.disableTable(table);
      ADMIN.deleteTable(table);
    }
  }

  private String printRegions(List<HRegionInfo> regions) {
    StringBuffer strBuf = new StringBuffer();
    for (HRegionInfo r : regions) {
      strBuf.append(" ____ " + r.toString());
    }
    return strBuf.toString();
  }

  private void validateNumberOfRowsInMeta(final TableName table, int numRegions,
      Connection connection) throws IOException {
    assert(ADMIN.tableExists(table));
    final AtomicInteger count = new AtomicInteger();
    Visitor visitor = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (HRegionInfo.getHRegionInfo(r).getTable().equals(table)) count.incrementAndGet();
        return true;
      }
    };
    MetaTableAccessor.fullScanRegions(connection, visitor);
    assert(count.get() == numRegions);
  }

  private void validateFromSnapshotFromMeta(HBaseTestingUtility util, TableName table,
      int numRegions, int numReplica, Connection connection) throws IOException {
    SnapshotOfRegionAssignmentFromMeta snapshot = new SnapshotOfRegionAssignmentFromMeta(
      connection);
    snapshot.initialize();
    Map<HRegionInfo, ServerName> regionToServerMap = snapshot.getRegionToRegionServerMap();
    assert(regionToServerMap.size() == numRegions * numReplica + 1); //'1' for the namespace
    Map<ServerName, List<HRegionInfo>> serverToRegionMap = snapshot.getRegionServerToRegionMap();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverToRegionMap.entrySet()) {
      if (entry.getKey().equals(util.getHBaseCluster().getMaster().getServerName())) {
        continue;
      }
      List<HRegionInfo> regions = entry.getValue();
      Set<byte[]> setOfStartKeys = new HashSet<byte[]>();
      for (HRegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (region.getTable().equals(table)) {
          setOfStartKeys.add(startKey); //ignore other tables
          LOG.info("--STARTKEY " + new String(startKey)+"--");
        }
      }
      // the number of startkeys will be equal to the number of regions hosted in each server
      // (each server will be hosting one replica of a region)
      assertEquals(numRegions, setOfStartKeys.size());
    }
  }

  private void validateSingleRegionServerAssignment(Connection connection, int numRegions,
      int numReplica) throws IOException {
    SnapshotOfRegionAssignmentFromMeta snapshot = new SnapshotOfRegionAssignmentFromMeta(
      connection);
    snapshot.initialize();
    Map<HRegionInfo, ServerName>  regionToServerMap = snapshot.getRegionToRegionServerMap();
    assertEquals(regionToServerMap.size(), numRegions * numReplica + 1); //'1' for the namespace
    Map<ServerName, List<HRegionInfo>> serverToRegionMap = snapshot.getRegionServerToRegionMap();
    assertEquals(serverToRegionMap.keySet().size(), 2); // 1 rs + 1 master
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverToRegionMap.entrySet()) {
      if (entry.getKey().equals(TEST_UTIL.getHBaseCluster().getMaster().getServerName())) {
        continue;
      }
      assertEquals(entry.getValue().size(), numRegions * numReplica);
    }
  }
}
