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
package org.apache.hadoop.hbase.rsgroup;

import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ MediumTests.class })
public class TestFallbackGroup extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFallbackGroup.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestFallbackGroup.class);

  public static void setUpTestBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setFloat("hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration()
        .set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.getConfiguration()
        .setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 100000);

    // enable fallback to default group
    TEST_UTIL.getConfiguration().setBoolean(RSGroupBasedLoadBalancer.ENABLE_FALLBACK_GROUP, true);
    // set correct region interval to 3s
    TEST_UTIL.getConfiguration().setInt(RSGroupBasedLoadBalancer.CORRECT_REGIONS_INTERVAL, 3000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_PERIOD, 2000);

    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    initialize();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    setUpTestBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  @Test
  public void testFallbackGroup() throws Exception {
    // create a rsgroup and move two regionservers to it
    String groupName = "test_group";
    int groupRSCount = 1;
    addGroup(groupName, groupRSCount);

    // create a table, and move it to test_group
    Table t = TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 5);
    TEST_UTIL.loadTable(t, Bytes.toBytes("f"));
    Set<TableName> toAddTables = new HashSet<>();
    toAddTables.add(tableName);
    rsGroupAdmin.moveTables(toAddTables, groupName);
    assertTrue(rsGroupAdmin.getRSGroupInfo(groupName).getTables().contains(tableName));
    TEST_UTIL.waitTableAvailable(tableName, 30000);

    // check test_group servers and table regions
    Set<Address> servers = rsGroupAdmin.getRSGroupInfo(groupName).getServers();
    assertEquals(groupRSCount, servers.size());
    LOG.debug("test_group servers {}", servers);
    for (RegionInfo tr : master.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(tableName)) {
      assertTrue(servers.contains(
          master.getAssignmentManager().getRegionStates().getRegionAssignments().get(tr)
              .getAddress()));
    }

    // stop all the regionservers in test_group
    for (Address addr : rsGroupAdmin.getRSGroupInfo(groupName).getServers()) {
      TEST_UTIL.getMiniHBaseCluster().stopRegionServer(getServerName(addr));
    }
    assertTrue(TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() > 0);
    // better wait for a while for region reassign
    sleep(10000);
    TEST_UTIL.waitTableAvailable(tableName, 30000);
    // check all table regions on 'default' group servers
    Set<Address> defaultGroupServers =
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers();
    for (RegionInfo tr : master.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(tableName)) {
      assertTrue(defaultGroupServers.contains(
          master.getAssignmentManager().getRegionStates().getRegionAssignments().get(tr)
              .getAddress()));
    }

    // move another regionserver(from the 'default' group) to test_group,
    // and then check if all table regions are online
    ServerName newServer = master.getServerManager().getOnlineServersList().get(0);
    rsGroupAdmin.moveServers(Sets.newHashSet(newServer.getAddress()), groupName);
    // better wait for a while for correct region thread working and region reassign
    sleep(10000);
    // wait and check if table regions are online
    TEST_UTIL.waitTableAvailable(tableName, 30000);
    // check all table regions on 'test_group' server: newServer
    for (RegionInfo tr : master.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(tableName)) {
      assertEquals(newServer,
          master.getAssignmentManager().getRegionStates().getRegionAssignments().get(tr));
    }
  }
}

