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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
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
 * Test for rsgroup enable, unloaded regions from decommissoned host of a rsgroup should be assigned
 * to those regionservers belonging to the same rsgroup.
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionMoverWithRSGroupEnable {

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMoverWithRSGroupEnable.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String TEST_RSGROUP = "test";

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    RSGroupUtil.enableRSGroup(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(5);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static final TableName TABLE_NAME = TableName.valueOf("testRegionMoverWithRSGroupEnable");

  private final List<Address> rsservers = new ArrayList<>(2);
  private final List<ServerName> defaultGroupServers = new ArrayList<>();
  private ServerName rsContainMeta;

  @BeforeEach
  public void setUp() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();

    // Add a new rsgroup and assign two servers to it.
    admin.addRSGroup(TEST_RSGROUP);
    Collection<ServerName> allServers = admin.getRegionServers();
    // Remove rs contains hbase:meta, otherwise test looks unstable and buggy in test env.
    rsContainMeta = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer())
      .filter(rs -> rs.getRegions(TableName.META_TABLE_NAME).size() > 0).findFirst().get()
      .getServerName();
    LOG.info("{} contains hbase:meta", rsContainMeta);
    List<ServerName> modifiable = new ArrayList<>(allServers);
    modifiable.remove(rsContainMeta);
    int i = 0;
    for (ServerName server : modifiable) {
      if (i == 2) break;
      rsservers.add(Address.fromParts(server.getHostname(), server.getPort()));
      i++;
    }
    admin.moveServersToRSGroup(new HashSet<>(rsservers), TEST_RSGROUP);
    LOG.info("Servers in {} are {}", TEST_RSGROUP, rsservers);
    assertEquals(3, admin.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    assertEquals(2, admin.getRSGroup(TEST_RSGROUP).getServers().size());

    // Track the servers left in the default group, used for isolation assertions.
    for (ServerName server : allServers) {
      if (!rsservers.contains(Address.fromParts(server.getHostname(), server.getPort()))) {
        defaultGroupServers.add(server);
      }
    }

    // Create a pre-split table in test rsgroup
    if (admin.tableExists(TABLE_NAME)) {
      TEST_UTIL.deleteTable(TABLE_NAME);
    }
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).setRegionServerGroup(TEST_RSGROUP)
      .build();
    String startKey = "a";
    String endKey = "z";
    admin.createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
  }

  @AfterEach
  public void tearDown() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      TEST_UTIL.deleteTable(TABLE_NAME);
    }
    if (!rsservers.isEmpty()) {
      admin.moveServersToRSGroup(new HashSet<>(rsservers), RSGroupInfo.DEFAULT_GROUP);
    }
    if (admin.getRSGroup(TEST_RSGROUP) != null) {
      admin.removeRSGroup(TEST_RSGROUP);
    }
    rsservers.clear();
    defaultGroupServers.clear();
    rsContainMeta = null;
  }

  @Test
  public void testUnloadRegions() throws Exception {
    Address decommission = rsservers.get(0);
    Address online = rsservers.get(1);
    String filename = new Path(TEST_UTIL.getDataTestDir(), "testRSGroupUnload").toString();
    RegionMoverBuilder builder =
      new RegionMoverBuilder(decommission.toString(), TEST_UTIL.getConfiguration());
    try (RegionMover rm = builder.filename(filename).ack(true).build()) {
      LOG.info("Unloading " + decommission.getHostname());
      rm.unload();
    }
    HRegionServer onlineRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> rs.getServerName().getAddress().equals(online)).findFirst().get();
    assertEquals(9, onlineRS.getNumberOfOnlineRegions());

    // Isolation assertion: no default-group server must hold any region of the test table.
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
