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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestAssignmentOnRSGroupCrash {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentOnRSGroupCrash.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentOnRSGroupCrash.class);

  private static final TableName TEST_TABLE = TableName.valueOf("testb");
  private static final String FAMILY_STR = "f";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STR);
  private static final int NUM_RS = 3;

  private HBaseTestingUtility UTIL;

  private static RSGroupAdmin rsGroupAdmin;

  private static void setupConf(Configuration conf) {
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
  }

  @Before
  public void setup() throws Exception {
    UTIL = new HBaseTestingUtility();

    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);

    UTIL.createTable(TEST_TABLE, new byte[][] { FAMILY },
        new byte[][] { Bytes.toBytes("B"), Bytes.toBytes("D"), Bytes.toBytes("F"),
            Bytes.toBytes("L") });
    rsGroupAdmin = new VerifyingRSGroupAdminClient(new RSGroupAdminClient(UTIL.getConnection()),
        UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testKillAllRSInGroupAndThenStart() throws Exception {
    // create a rsgroup and move one regionserver to it
    String groupName = "my_group";
    int groupRSCount = 1;
    RSGroupTestingUtil.addRSGroup(rsGroupAdmin, groupName, groupRSCount);
    Set<TableName> toAddTables = new HashSet<>();
    toAddTables.add(TEST_TABLE);
    rsGroupAdmin.moveTables(toAddTables, groupName);
    RSGroupInfo rsGroupInfo = rsGroupAdmin.getRSGroupInfo(groupName);
    LOG.debug("my_group: " + rsGroupInfo.toString());
    Set<Address> servers = rsGroupInfo.getServers();
    ServerName myGroupRS = null;
    for (int i = 0; i < NUM_RS; ++i) {
      ServerName sn = UTIL.getMiniHBaseCluster().getRegionServer(i).getServerName();
      if (servers.contains(sn.getAddress())) {
        myGroupRS = sn;
        break;
      }
    }
    assertNotNull(myGroupRS);
    checkRegionsOnline(TEST_TABLE, true);

    // stop regionserver in the rsgroup, and table regions will be offline
    UTIL.getMiniHBaseCluster().stopRegionServer(myGroupRS);
    // better wait for a while for region reassign
    sleep(10000);
    assertEquals(UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size(),
        NUM_RS - servers.size());
    checkRegionsOnline(TEST_TABLE, false);

    // move another regionserver to the rsgroup
    // in this case, moving another region server can be replaced by restarting the regionserver
    // mentioned before
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    Set<Address> set = new HashSet<>();
    for (Address server : defaultInfo.getServers()) {
      if (set.size() == groupRSCount) {
        break;
      }
      set.add(server);
    }
    rsGroupAdmin.moveServers(set, groupName);

    // wait and check if table regions are online
    sleep(10000);
    checkRegionsOnline(TEST_TABLE, true);
  }

  private void checkRegionsOnline(TableName tableName, boolean isOnline) throws IOException {
    for (RegionInfo hri : UTIL.getHBaseAdmin().getTableRegions(tableName)) {
      assertTrue(UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
          .isRegionOnline(hri) == isOnline);
    }
  }
}
