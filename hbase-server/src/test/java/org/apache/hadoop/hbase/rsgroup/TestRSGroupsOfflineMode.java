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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseClusterInterface;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This tests that GroupBasedBalancer will use data in zk to do balancing during master startup.
 * This does not test retain assignment.
 * <p/>
 * The tests brings up 3 RS, creates a new RS group 'my_group', moves 1 RS to 'my_group', assigns
 * 'hbase:rsgroup' to 'my_group', and kill the only server in that group so that 'hbase:rsgroup'
 * table isn't available. It then kills the active master and waits for backup master to come
 * online. In new master, RSGroupInfoManagerImpl gets the data from zk and waits for the expected
 * assignment with a timeout.
 */
@Category({ RSGroupTests.class, MediumTests.class })
public class TestRSGroupsOfflineMode extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsOfflineMode.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsOfflineMode.class);
  private static HMaster master;
  private static Admin hbaseAdmin;
  private static HBaseTestingUtil TEST_UTIL;
  private static HBaseClusterInterface cluster;
  private final static long WAIT_TIMEOUT = 60000 * 5;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    RSGroupUtil.enableRSGroup(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().set(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, "1");
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().numMasters(2).numRegionServers(3).numDataNodes(3).build();
    TEST_UTIL.startMiniCluster(option);
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((SingleProcessHBaseCluster) cluster).getMaster();
    master.balanceSwitch(false);
    hbaseAdmin = TEST_UTIL.getAdmin();
    // wait till the balancer is in online mode
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized()
          && ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline()
          && master.getServerManager().getOnlineServersList().size() >= 3;
      }
    });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOffline() throws Exception, InterruptedException {
    // Table should be after group table name so it gets assigned later.
    final TableName failoverTable = TableName.valueOf(getNameWithoutIndex(name.getMethodName()));
    TEST_UTIL.createTable(failoverTable, Bytes.toBytes("f"));
    final HRegionServer killRS = ((SingleProcessHBaseCluster) cluster).getRegionServer(0);
    final HRegionServer groupRS = ((SingleProcessHBaseCluster) cluster).getRegionServer(1);
    final HRegionServer failoverRS = ((SingleProcessHBaseCluster) cluster).getRegionServer(2);
    String newGroup = "my_group";
    Admin admin = TEST_UTIL.getAdmin();
    admin.addRSGroup(newGroup);
    if (
      master.getAssignmentManager().getRegionStates().getRegionAssignments()
        .containsValue(failoverRS.getServerName())
    ) {
      for (RegionInfo regionInfo : hbaseAdmin.getRegions(failoverRS.getServerName())) {
        hbaseAdmin.move(regionInfo.getEncodedNameAsBytes(), failoverRS.getServerName());
      }
      LOG.info("Waiting for region unassignments on failover RS...");
      TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !master.getServerManager().getLoad(failoverRS.getServerName()).getRegionMetrics()
            .isEmpty();
        }
      });
    }

    // Move server to group and make sure all tables are assigned.
    admin.moveServersToRSGroup(Sets.newHashSet(groupRS.getServerName().getAddress()), newGroup);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return groupRS.getNumberOfOnlineRegions() < 1
          && master.getAssignmentManager().getRegionsInTransitionCount() < 1;
      }
    });
    // Move table to group and wait.
    admin.setRSGroup(Sets.newHashSet(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME), newGroup);
    LOG.info("Waiting for move table...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return groupRS.getNumberOfOnlineRegions() == 1;
      }
    });

    groupRS.stop("die");
    // Race condition here.
    TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
    LOG.info("Waiting for offline mode...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getHBaseCluster().getMaster() != null
          && TEST_UTIL.getHBaseCluster().getMaster().isActiveMaster()
          && TEST_UTIL.getHBaseCluster().getMaster().isInitialized()
          && TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size()
              <= 3;
      }
    });

    // Get groupInfoManager from the new active master.
    RSGroupInfoManager groupMgr =
      ((SingleProcessHBaseCluster) cluster).getMaster().getRSGroupInfoManager();
    // Make sure balancer is in offline mode, since this is what we're testing.
    assertFalse(groupMgr.isOnline());
    // Kill final regionserver to see the failover happens for all tables except GROUP table since
    // it's group does not have any online RS.
    killRS.stop("die");
    master = TEST_UTIL.getHBaseCluster().getMaster();
    LOG.info("Waiting for new table assignment...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return failoverRS.getRegions(failoverTable).size() >= 1;
      }
    });
    assertEquals(0, failoverRS.getRegions(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME).size());

    // Need this for minicluster to shutdown cleanly.
    master.stopMaster();
  }
}
