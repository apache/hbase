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
package org.apache.hadoop.hbase.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MiscTests.class, LargeTests.class })
public class TestTestingHBaseCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTestingHBaseCluster.class);

  private static TestingHBaseCluster CLUSTER;

  private Connection conn;

  private Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    CLUSTER = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().numMasters(2)
      .numRegionServers(3).numDataNodes(3).conf(conf).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (CLUSTER.isClusterRunning()) {
      CLUSTER.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    if (!CLUSTER.isClusterRunning()) {
      CLUSTER.start();
    }
    if (!CLUSTER.isHBaseClusterRunning()) {
      CLUSTER.startHBaseCluster();
    }
    conn = ConnectionFactory.createConnection(CLUSTER.getConf());
    admin = conn.getAdmin();
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(admin, true);
    Closeables.close(conn, true);
    if (CLUSTER.isHBaseClusterRunning()) {
      CLUSTER.stopHBaseCluster();
    }
  }

  @Test
  public void testStartStop() throws Exception {
    assertTrue(CLUSTER.isClusterRunning());
    assertTrue(CLUSTER.isHBaseClusterRunning());
    assertThrows(IllegalStateException.class, () -> CLUSTER.start());
    CLUSTER.stop();
    assertFalse(CLUSTER.isClusterRunning());
    assertFalse(CLUSTER.isHBaseClusterRunning());
    assertThrows(IllegalStateException.class, () -> CLUSTER.stop());
  }

  @Test
  public void testStartStopHBaseCluster() throws Exception {
    assertTrue(CLUSTER.isHBaseClusterRunning());
    assertThrows(IllegalStateException.class, () -> CLUSTER.startHBaseCluster());
    CLUSTER.stopHBaseCluster();
    assertTrue(CLUSTER.isClusterRunning());
    assertFalse(CLUSTER.isHBaseClusterRunning());
    assertThrows(IllegalStateException.class, () -> CLUSTER.stopHBaseCluster());
  }

  @Test
  public void testStartStopMaster() throws Exception {
    ServerName master = admin.getMaster();
    CLUSTER.stopMaster(master).join();
    // wait until the backup master becomes active master.
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> {
      try {
        return admin.getMaster() != null;
      } catch (Exception e) {
        // ignore
        return false;
      }
    });
    // should have no backup master
    assertTrue(admin.getBackupMasters().isEmpty());
    CLUSTER.startMaster();
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> !admin.getBackupMasters().isEmpty());
    CLUSTER.startMaster(DNS.getHostname(CLUSTER.getConf(), ServerType.MASTER), 0);
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> admin.getBackupMasters().size() == 2);
  }

  @Test
  public void testStartStopRegionServer() throws Exception {
    Collection<ServerName> regionServers = admin.getRegionServers();
    assertEquals(3, regionServers.size());
    CLUSTER.stopRegionServer(Iterables.get(regionServers, 0)).join();
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> admin.getRegionServers().size() == 2);
    CLUSTER.startRegionServer();
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> admin.getRegionServers().size() == 3);
    CLUSTER.startRegionServer(DNS.getHostname(CLUSTER.getConf(), ServerType.REGIONSERVER), 0);
    Waiter.waitFor(CLUSTER.getConf(), 30000, () -> admin.getRegionServers().size() == 4);
  }

  @Test
  public void testGetAddresses() throws Exception {
    assertTrue(CLUSTER.getActiveMasterAddress().isPresent());
    assertEquals(1, CLUSTER.getBackupMasterAddresses().size());
    assertEquals(3, CLUSTER.getRegionServerAddresses().size());

    // [HOSTNAME1:PORT1, HOSTNAME2:PORT2]
    final List<String> masterAddrs = CLUSTER.getMasterAddresses();
    assertEquals(2, masterAddrs.size());
    assertTrue(masterAddrs.stream().allMatch(addr -> addr.matches(".*:[0-9]+$")));
  }

  @Test
  public void testGetPorts() throws Exception {
    final String addressPattern = "^https?://.*:[0-9]+$";
    assertTrue(CLUSTER.getActiveMasterInfoAddress().map(a -> Pattern.matches(addressPattern, a))
      .orElse(false));
    assertTrue(CLUSTER.getActiveNameNodeInfoAddress().map(a -> Pattern.matches(addressPattern, a))
      .orElse(false));
    assertTrue(CLUSTER.getZooKeeperQuorum().isPresent());
  }
}
