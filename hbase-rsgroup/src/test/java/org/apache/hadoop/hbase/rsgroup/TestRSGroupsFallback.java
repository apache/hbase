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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class })
public class TestRSGroupsFallback extends TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsFallback.class);

  private static final String FALLBACK_GROUP = "fallback";

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setBoolean(RSGroupBasedLoadBalancer.FALLBACK_GROUP_ENABLE_KEY,
      true);
    TEST_UTIL.getConfiguration().setFloat(
      "hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().set(
      HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(
      HConstants.ZOOKEEPER_USEMULTI,
      true);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
      ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
      NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
    master.balanceSwitch(true);
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
  public void testFallback() throws Exception {
    // add fallback group
    addGroup(rsGroupAdmin, FALLBACK_GROUP, 1);
    // add test group
    String groupName = "appInfo";
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, groupName, 1);
    final TableName tableName = TableName.valueOf(tablePrefix + "_ns", "_testFallback");
    admin.createNamespace(
      NamespaceDescriptor.create(tableName.getNamespaceAsString())
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // server of test group crash, regions move to default group
    crashRsInGroup(groupName);
    assertRegionsInGroup(tableName, RSGroupInfo.DEFAULT_GROUP);

    // server of default group crash, regions move to any other group
    crashRsInGroup(RSGroupInfo.DEFAULT_GROUP);
    assertRegionsInGroup(tableName, FALLBACK_GROUP);

    // add a new server to default group, regions move to default group
    JVMClusterUtil.RegionServerThread t =
      TEST_UTIL.getMiniHBaseCluster().startRegionServerAndWait(60000);
    assertTrue(master.balance());
    assertRegionsInGroup(tableName, RSGroupInfo.DEFAULT_GROUP);

    // add a new server to test group, regions move back
    JVMClusterUtil.RegionServerThread t1 =
      TEST_UTIL.getMiniHBaseCluster().startRegionServerAndWait(60000);
    rsGroupAdmin.moveServers(Collections.singleton(t.getRegionServer().getServerName()
      .getAddress()), groupName);
    assertTrue(master.balance());
    assertRegionsInGroup(tableName, groupName);

    TEST_UTIL.getMiniHBaseCluster().killRegionServer(t.getRegionServer().getServerName());
    TEST_UTIL.getMiniHBaseCluster().killRegionServer(t1.getRegionServer().getServerName());

    TEST_UTIL.deleteTable(tableName);
  }

  private void assertRegionsInGroup(TableName table, String group) throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExecutor = TEST_UTIL.getMiniHBaseCluster()
      .getMaster().getMasterProcedureExecutor();
    for (ProcedureInfo procInfo: procExecutor.listProcedures()) {
      LOG.debug("Waiting for " + procInfo.getProcName() + " " + procInfo.toString());
      waitProcedure(procExecutor, procInfo, 10000);
    }
    RSGroupInfo rsGroup = rsGroupAdmin.getRSGroupInfo(group);
    for (HRegionInfo region: master.getAssignmentManager().getRegionStates()
      .getRegionsOfTable(table)) {
      Address regionOnServer = master.getAssignmentManager().getRegionStates()
        .getRegionAssignments().get(region).getAddress();
      assertTrue(rsGroup.getServers().contains(regionOnServer));
    }
  }

  private void crashRsInGroup(String groupName) throws Exception {
    for (Address server : rsGroupAdmin.getRSGroupInfo(groupName).getServers()) {
      final ServerName sn = getServerName(server);
      TEST_UTIL.getMiniHBaseCluster().killRegionServer(sn);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() {
          return master.getServerManager().isServerDead(sn);
        }
      });
    }
    Threads.sleep(1000);
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
  }

  private void waitProcedure(ProcedureExecutor<MasterProcedureEnv> procExecutor,
                             ProcedureInfo procInfo, long timeout) {
    long start = EnvironmentEdgeManager.currentTime();
    while ((EnvironmentEdgeManager.currentTime() - start) < timeout) {
      if (procInfo.getProcState() == ProcedureProtos.ProcedureState.INITIALIZING ||
        (procExecutor.isRunning() && !procExecutor.isFinished(procInfo.getProcId()))) {
        Threads.sleep(1000);
      } else {
        break;
      }
    }
  }
}
