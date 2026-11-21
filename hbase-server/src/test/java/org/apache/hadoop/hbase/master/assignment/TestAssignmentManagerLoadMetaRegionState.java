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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestAssignmentManagerLoadMetaRegionState {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final byte[] CF = Bytes.toBytes("cf");

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRestart() throws InterruptedException, IOException {
    ServerName sn = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    Set<RegionInfo> regions = new HashSet<>(am.getRegionsOnServer(sn));

    UTIL.getMiniHBaseCluster().stopMaster(0).join();
    HMaster newMaster = UTIL.getMiniHBaseCluster().startMaster().getMaster();
    UTIL.waitFor(30000, () -> newMaster.isInitialized());
    UTIL.invalidateConnection();

    am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    List<RegionInfo> newRegions = am.getRegionsOnServer(sn);
    assertEquals(regions.size(), newRegions.size());
    for (RegionInfo region : newRegions) {
      assertTrue(regions.contains(region));
    }
  }

  @Test
  public void testRestartWithClosedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testRestartWithClosedRegion");
    UTIL.createTable(tableName, CF);
    try (Admin admin = UTIL.getConnection().getAdmin()) {
      List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
      assertEquals(1, regions.size());
      RegionInfo region = regions.get(0).getRegionInfo();
      ServerName hostBeforeClose = UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(region);
      assertNotNull(hostBeforeClose, "region must be assigned before disable");

      admin.disableTable(tableName);

      AssignmentManager masterBeforeRestart =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager();
      RegionStateNode nodeBeforeRestart =
        masterBeforeRestart.getRegionStates().getRegionStateNode(region);
      assertEquals(State.CLOSED, nodeBeforeRestart.getState(),
        "region should be CLOSED after disable");
      assertNull(nodeBeforeRestart.getRegionLocation(),
        "regionLocation should be null for a CLOSED region before master failover");

      restartActiveMaster();

      AssignmentManager masterAfterRestart =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
      RegionStateNode nodeAfterRestart =
        masterAfterRestart.getRegionStates().getRegionStateNode(region);
      assertNotNull(nodeAfterRestart, "region state node must be restored from meta");
      assertEquals(State.CLOSED, nodeAfterRestart.getState(),
        "state should still be CLOSED after failover");
      assertNull(nodeAfterRestart.getRegionLocation(),
        "regionLocation must be null for a CLOSED region after master failover "
          + "(meta keeps stale info:sn, but in-memory state must be normalized)");
      assertEquals(hostBeforeClose, nodeAfterRestart.getLastHost(),
        "lastHost must be preserved across failover for locality / recovery");
      assertEquals(hostBeforeClose,
        masterAfterRestart.getRegionStates().getRegionServerOfRegion(region),
        "closed region should still resolve to lastHost for locality-sensitive paths");
      assertNull(masterAfterRestart.getRegionStates().getRegionAssignments().get(region),
        "getRegionAssignments must not export a stale current owner for CLOSED region");
      Map<ServerName, List<RegionInfo>> assignmentSnapshot =
        masterAfterRestart.getSnapShotOfAssignment(Collections.singleton(region));
      assertFalse(assignmentSnapshot.containsKey(hostBeforeClose),
        "getSnapShotOfAssignment must not place CLOSED region under a live server");
      List<RegionInfo> regionsOnHost = masterAfterRestart.getRegionsOnServer(hostBeforeClose);
      assertFalse(regionsOnHost.contains(region),
        "CLOSED region must not appear in getRegionsOnServer after failover "
          + "(ServerStateNode membership must stay consistent with regionLocation == null)");

      try (Admin postRestartAdmin = UTIL.getAdmin()) {
        postRestartAdmin.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testRestartWithOpenRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testRestartWithOpenRegion");
    UTIL.createTable(tableName, CF);
    try (Admin admin = UTIL.getConnection().getAdmin()) {
      List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
      assertEquals(1, regions.size());
      RegionInfo region = regions.get(0).getRegionInfo();
      ServerName host = UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(region);
      assertNotNull(host, "region must be assigned before failover");

      restartActiveMaster();

      AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
      RegionStateNode node = am.getRegionStates().getRegionStateNode(region);
      assertNotNull(node, "region state node must be restored from meta");
      assertEquals(State.OPEN, node.getState(), "state should remain OPEN after failover");
      assertEquals(host, node.getRegionLocation(),
        "regionLocation must match the hosting RS after failover");
      assertEquals(host, am.getRegionStates().getRegionAssignments().get(region),
        "getRegionAssignments must keep exporting current owner for OPEN region");
      Map<ServerName, List<RegionInfo>> assignmentSnapshot =
        am.getSnapShotOfAssignment(Collections.singleton(region));
      assertTrue(assignmentSnapshot.get(host).contains(region),
        "getSnapShotOfAssignment must keep OPEN region under its hosting RS");
      List<RegionInfo> regionsOnHost = am.getRegionsOnServer(host);
      assertTrue(regionsOnHost.contains(region),
        "OPEN region must appear in getRegionsOnServer after failover");

      try (Admin postRestartAdmin = UTIL.getAdmin()) {
        postRestartAdmin.disableTable(tableName);
        postRestartAdmin.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testRestartWithImplicitOfflineRegionCanBeAssigned() throws Exception {
    TableName tableName = TableName.valueOf("testRestartWithImplicitOfflineRegionCanBeAssigned");
    UTIL.createTable(tableName, CF);
    try (Admin admin = UTIL.getConnection().getAdmin()) {
      List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
      assertEquals(1, regions.size());
      RegionInfo region = regions.get(0).getRegionInfo();
      ServerName hostBeforeDisable = UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(region);
      assertNotNull(hostBeforeDisable, "region must be assigned before disable");

      admin.disableTable(tableName);
      setMetaLocation(region, hostBeforeDisable);

      restartActiveMaster();

      AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
      RegionStateNode nodeAfterRestart = am.getRegionStates().getRegionStateNode(region);
      assertNotNull(nodeAfterRestart, "region state node must be restored from meta");
      assertEquals(State.OFFLINE, nodeAfterRestart.getState(),
        "missing state in meta should be restored as OFFLINE");
      assertNull(nodeAfterRestart.getRegionLocation(),
        "OFFLINE region must not restore stale regionLocation after failover");
      assertEquals(hostBeforeDisable, nodeAfterRestart.getLastHost(),
        "lastHost must be preserved across failover");
      assertTrue(am.getRegionStates().isRegionOffline(region),
        "OFFLINE region must still be treated as offline after failover");

      try (Admin postRestartAdmin = UTIL.getAdmin()) {
        postRestartAdmin.enableTable(tableName);
        UTIL.waitFor(30000, () -> {
          RegionStateNode node = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionStateNode(region);
          return node != null && node.isInState(State.OPEN) && node.getRegionLocation() != null;
        });

        RegionStateNode nodeAfterEnable = UTIL.getMiniHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates().getRegionStateNode(region);
        assertEquals(State.OPEN, nodeAfterEnable.getState(),
          "OFFLINE region should be assignable after failover");
        assertNotNull(nodeAfterEnable.getRegionLocation(),
          "assigned region must have a live location");

        postRestartAdmin.disableTable(tableName);
        postRestartAdmin.deleteTable(tableName);
      }
    }
  }

  private void setMetaLocation(RegionInfo region, ServerName staleLocation)
    throws IOException {
    Delete delete = new Delete(region.getRegionName());
    delete.addColumns(HConstants.CATALOG_FAMILY,
      CatalogFamilyFormat.getRegionStateColumn(region.getReplicaId()));
    Put put = new Put(region.getRegionName());
    put.addColumn(HConstants.CATALOG_FAMILY,
      CatalogFamilyFormat.getServerNameColumn(region.getReplicaId()),
      Bytes.toBytes(staleLocation.getServerName()));
    try (Table meta = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      meta.delete(delete);
      meta.put(put);
    }
  }

  private void restartActiveMaster() throws Exception {
    ServerName oldMasterSn = UTIL.getMiniHBaseCluster().getMaster().getServerName();
    UTIL.getMiniHBaseCluster().stopMaster(oldMasterSn);
    UTIL.getMiniHBaseCluster().waitForMasterToStop(oldMasterSn, 60000);
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000);
    UTIL.waitFor(30000, () -> {
      HMaster m = UTIL.getMiniHBaseCluster().getMaster();
      return m != null && m.isInitialized();
    });
    UTIL.invalidateConnection();
  }
}
