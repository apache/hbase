/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.rsgroup;

import com.google.common.collect.Sets;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


//This tests that GroupBasedBalancer will use data in zk
//to do balancing during master startup
//This does not test retain assignment
@Category(MediumTests.class)
public class TestRSGroupsOfflineMode {
  private static final org.apache.commons.logging.Log LOG =
      LogFactory.getLog(TestRSGroupsOfflineMode.class);
  private static HMaster master;
  private static HBaseAdmin hbaseAdmin;
  private static HBaseTestingUtility TEST_UTIL;
  private static HBaseCluster cluster;
  private static RSGroupAdminEndpoint RSGroupAdminEndpoint;
  public final static long WAIT_TIMEOUT = 60000*5;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName());
    TEST_UTIL.getConfiguration().set(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        "1");
    TEST_UTIL.startMiniCluster(2, 3);
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();
    master.balanceSwitch(false);
    hbaseAdmin = TEST_UTIL.getHBaseAdmin();
    //wait till the balancer is in online mode
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline() &&
            master.getServerManager().getOnlineServersList().size() >= 3;
      }
    });
    RSGroupAdminEndpoint =
        master.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class).get(0);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOffline() throws Exception, InterruptedException {
    //table should be after group table name
    //so it gets assigned later
    final TableName failoverTable = TableName.valueOf("testOffline");
    TEST_UTIL.createTable(failoverTable, Bytes.toBytes("f"));

    RSGroupAdmin groupAdmin = RSGroupAdmin.newClient(TEST_UTIL.getConnection());

    final HRegionServer killRS = ((MiniHBaseCluster)cluster).getRegionServer(0);
    final HRegionServer groupRS = ((MiniHBaseCluster)cluster).getRegionServer(1);
    final HRegionServer failoverRS = ((MiniHBaseCluster)cluster).getRegionServer(2);

    String newGroup =  "my_group";
    groupAdmin.addRSGroup(newGroup);
    if(master.getAssignmentManager().getRegionStates().getRegionAssignments()
        .containsValue(failoverRS.getServerName())) {
      for(HRegionInfo regionInfo: hbaseAdmin.getOnlineRegions(failoverRS.getServerName())) {
        hbaseAdmin.move(regionInfo.getEncodedNameAsBytes(),
            Bytes.toBytes(failoverRS.getServerName().getServerName()));
      }
      LOG.info("Waiting for region unassignments on failover RS...");
      TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return master.getServerManager().getLoad(failoverRS.getServerName())
              .getRegionsLoad().size() > 0;
        }
      });
    }

    //move server to group and make sure all tables are assigned
    groupAdmin.moveServers(Sets.newHashSet(groupRS.getServerName().getHostPort()), newGroup);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return groupRS.getNumberOfOnlineRegions() < 1 &&
            master.getAssignmentManager().getRegionStates().getRegionsInTransition().size() < 1;
      }
    });
    //move table to group and wait
    groupAdmin.moveTables(Sets.newHashSet(RSGroupInfoManager.RSGROUP_TABLE_NAME), newGroup);
    LOG.info("Waiting for move table...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return groupRS.getNumberOfOnlineRegions() == 1;
      }
    });

    groupRS.stop("die");
    //race condition here
    TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
    LOG.info("Waiting for offline mode...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getHBaseCluster().getMaster() != null &&
            TEST_UTIL.getHBaseCluster().getMaster().isActiveMaster() &&
            TEST_UTIL.getHBaseCluster().getMaster().isInitialized() &&
            TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size()
                <= 3;
      }
    });


    RSGroupInfoManager groupMgr = RSGroupAdminEndpoint.getGroupInfoManager();
    //make sure balancer is in offline mode, since this is what we're testing
    assertFalse(groupMgr.isOnline());
    //verify the group affiliation that's loaded from ZK instead of tables
    assertEquals(newGroup,
        groupMgr.getRSGroupOfTable(RSGroupInfoManager.RSGROUP_TABLE_NAME));
    assertEquals(RSGroupInfo.DEFAULT_GROUP, groupMgr.getRSGroupOfTable(failoverTable));

    //kill final regionserver to see the failover happens for all tables
    //except GROUP table since it's group does not have any online RS
    killRS.stop("die");
    master = TEST_UTIL.getHBaseCluster().getMaster();
    LOG.info("Waiting for new table assignment...");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return failoverRS.getOnlineRegions(failoverTable).size() >= 1;
      }
    });
    Assert.assertEquals(0, failoverRS.getOnlineRegions(RSGroupInfoManager.RSGROUP_TABLE_NAME).size());

    //need this for minicluster to shutdown cleanly
    master.stopMaster();
  }
}
