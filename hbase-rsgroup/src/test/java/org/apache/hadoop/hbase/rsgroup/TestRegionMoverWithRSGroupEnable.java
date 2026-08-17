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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.RegionMover;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that RegionMover.unloadRegions() respects RSGroup membership in branch-2: regions
 * decommissioned from a server in a non-default RSGroup must land only on other servers in the same
 * group, not on servers in unrelated groups.
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionMoverWithRSGroupEnable {

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMoverWithRSGroupEnable.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TEST_RSGROUP = "test";
  private static final TableName TABLE_NAME = TableName.valueOf("testRegionMoverWithRSGroupEnable");

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName());
    TEST_UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 5);
    TEST_UTIL.startMiniCluster(5);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // Addresses of the two servers placed in TEST_RSGROUP each test.
  private final List<Address> rsservers = new ArrayList<>(2);
  // Addresses of the servers that remain in the default group (excludes meta RS).
  private final List<ServerName> defaultGroupServers = new ArrayList<>();
  private RSGroupAdminClient rsGroupAdmin;
  private ServerName rsContainMeta;

  @BeforeEach
  public void setUp() throws Exception {
    rsGroupAdmin = new RSGroupAdminClient(TEST_UTIL.getConnection());
    if (rsGroupAdmin.getRSGroupInfo(TEST_RSGROUP) == null) {
      rsGroupAdmin.addRSGroup(TEST_RSGROUP);
    }
    Collection<ServerName> allServers = TEST_UTIL.getAdmin().getRegionServers();

    // Exclude the RS that hosts hbase:meta to keep the test stable.
    rsContainMeta = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> !rs.getRegions(TableName.META_TABLE_NAME).isEmpty()).findFirst().get()
      .getServerName();
    LOG.info("{} contains hbase:meta, keeping in default group", rsContainMeta);

    // Move any leftover servers back to default before setting up fresh assignments.
    RSGroupInfo existingGroup = rsGroupAdmin.getRSGroupInfo(TEST_RSGROUP);
    if (existingGroup != null && !existingGroup.getServers().isEmpty()) {
      rsGroupAdmin.moveServers(new HashSet<>(existingGroup.getServers()),
        RSGroupInfo.DEFAULT_GROUP);
    }

    List<ServerName> modifiable = new ArrayList<>(allServers);
    modifiable.remove(rsContainMeta);
    int i = 0;
    for (ServerName server : modifiable) {
      if (i == 2) {
        break;
      }
      rsservers.add(Address.fromParts(server.getHostname(), server.getPort()));
      i++;
    }
    rsGroupAdmin.moveServers(new HashSet<>(rsservers), TEST_RSGROUP);
    LOG.info("Servers moved to {} group: {}", TEST_RSGROUP, rsservers);

    assertEquals(3, rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    assertEquals(2, rsGroupAdmin.getRSGroupInfo(TEST_RSGROUP).getServers().size());

    // Record the three default-group servers (used for isolation assertions).
    for (ServerName sn : allServers) {
      Address addr = sn.getAddress();
      if (!rsservers.contains(addr)) {
        defaultGroupServers.add(sn);
      }
    }

    if (TEST_UTIL.getAdmin().tableExists(TABLE_NAME)) {
      TEST_UTIL.deleteTable(TABLE_NAME);
    }
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    TEST_UTIL.getAdmin().createTable(tableDesc, Bytes.toBytes("a"), Bytes.toBytes("z"), 9);
    rsGroupAdmin.moveTables(new HashSet<>(Arrays.asList(TABLE_NAME)), TEST_RSGROUP);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (TEST_UTIL.getAdmin().tableExists(TABLE_NAME)) {
      TEST_UTIL.deleteTable(TABLE_NAME);
    }
    if (!rsservers.isEmpty()) {
      rsGroupAdmin.moveServers(new HashSet<>(rsservers), RSGroupInfo.DEFAULT_GROUP);
    }
    if (rsGroupAdmin.getRSGroupInfo(TEST_RSGROUP) != null) {
      rsGroupAdmin.removeRSGroup(TEST_RSGROUP);
    }
    rsservers.clear();
    defaultGroupServers.clear();
    rsContainMeta = null;
  }

  /**
   * Unloading a server in a non-default RSGroup must move all regions to the remaining server in
   * that group — and must not move any region to a server in the default group.
   */
  @Test
  public void testUnloadRegionsRespectsRSGroup() throws Exception {
    // Regions are placed via randomAssignment across the test group's servers, so pick the
    // decommission target as whichever rsservers member actually hosts a TABLE_NAME region —
    // otherwise the test could pass without ever exercising the move/filter path.
    HRegionServer hostingRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> rsservers.contains(rs.getServerName().getAddress()))
      .filter(rs -> !rs.getRegions(TABLE_NAME).isEmpty()).findFirst().get();
    Address decommission = hostingRS.getServerName().getAddress();
    Address online =
      rsservers.stream().filter(addr -> !addr.equals(decommission)).findFirst().get();
    String filename = new Path(TEST_UTIL.getDataTestDir(), "testRSGroupUnload").toString();

    RegionMoverBuilder builder =
      new RegionMoverBuilder(decommission.toString(), TEST_UTIL.getConfiguration());
    try (RegionMover rm = builder.filename(filename).ack(true).build()) {
      LOG.info("Unloading {}", decommission.getHostname());
      rm.unload();
    }

    HRegionServer onlineRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> rs.getServerName().getAddress().equals(online)).findFirst().get();

    // Positive assertion: all 9 regions landed on the one remaining test-group server.
    assertEquals(9, onlineRS.getNumberOfOnlineRegions(),
      "All 9 regions must be on the single remaining server in the test RSGroup");

    // Isolation assertion: no default-group server received any of the table's regions.
    for (ServerName defaultSN : defaultGroupServers) {
      HRegionServer defaultRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(rs -> rs.getServerName().equals(defaultSN)).findFirst().orElse(null);
      if (defaultRS == null) {
        continue;
      }
      List<HRegion> tableRegions = defaultRS.getRegions(TABLE_NAME);
      assertTrue(tableRegions.isEmpty(), "Default-group server " + defaultSN
        + " must not hold any regions of " + TABLE_NAME + " but had: " + tableRegions);
    }
  }

  /**
   * Unloading a server that is in the default RSGroup must still succeed end-to-end when RSGroups
   * are enabled. Destinations must be filtered to the default group: regions may spread across the
   * other default-group servers, but must not land on any test-group server.
   */
  @Test
  public void testUnloadDefaultGroupServerWithRSGroupEnabled() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    // Avoid unloading the meta-carrying server here too, for the same stability reason setUp()
    // avoids it when picking rsservers.
    ServerName defaultSN =
      defaultGroupServers.stream().filter(sn -> !sn.equals(rsContainMeta)).findFirst().get();
    Address decommission = defaultSN.getAddress();
    String filename = new Path(TEST_UTIL.getDataTestDir(), "testDefaultGroupUnload").toString();

    // Create a table in the default group; the balancer will distribute its regions naturally
    // across the default-group servers, so defaultSN will hold at least some.
    TableName defaultTable = TableName.valueOf("testDefaultGroupTable");
    if (admin.tableExists(defaultTable)) {
      TEST_UTIL.deleteTable(defaultTable);
    }
    try {
      TableDescriptor td = TableDescriptorBuilder.newBuilder(defaultTable)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
      admin.createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 6);
      TEST_UTIL.waitTableAvailable(defaultTable);

      HRegionServer decommRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(rs -> rs.getServerName().equals(defaultSN)).findFirst().get();
      assertFalse(decommRS.getRegions(defaultTable).isEmpty(),
        "Precondition: decommissioned server must actually host some regions of the default "
          + "table, otherwise the post-unload check below is vacuous");

      RegionMoverBuilder builder =
        new RegionMoverBuilder(decommission.toString(), TEST_UTIL.getConfiguration());
      try (RegionMover rm = builder.filename(filename).ack(true).build()) {
        LOG.info("Unloading default-group server {}", decommission.getHostname());
        rm.unload();
      }

      // After unload, the decommissioned server must hold no regions of the default table.
      assertEquals(0, decommRS.getRegions(defaultTable).size(),
        "Decommissioned default-group server must hold no regions after unload");

      // Isolation assertion: no test-group server must hold any region of the default table.
      // RegionMover must have restricted move targets to the default group only.
      for (JVMClusterUtil.RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster()
        .getRegionServerThreads()) {
        HRegionServer rs = rst.getRegionServer();
        Address addr = rs.getServerName().getAddress();
        if (rsservers.contains(addr)) {
          List<HRegion> found = rs.getRegions(defaultTable);
          assertTrue(found.isEmpty(), "Test-group server " + addr + " must not hold any region of "
            + defaultTable + " but had: " + found);
        }
      }
    } finally {
      if (admin.tableExists(defaultTable)) {
        TEST_UTIL.deleteTable(defaultTable);
      }
    }
  }
}
