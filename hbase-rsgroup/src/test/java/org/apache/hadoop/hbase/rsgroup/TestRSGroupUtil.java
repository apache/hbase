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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class})
public class TestRSGroupUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupUtil.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupUtil.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static HMaster master;

  private static RSGroupAdminClient rsGroupAdminClient;

  private static final String GROUP_NAME = "rsg";

  private static RSGroupInfoManager rsGroupInfoManager;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().set(
      HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName());
    UTIL.startMiniCluster(5);
    master = UTIL.getMiniHBaseCluster().getMaster();

    UTIL.waitFor(60000, (Predicate<Exception>) () ->
        master.isInitialized() && ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline());

    rsGroupAdminClient = new RSGroupAdminClient(UTIL.getConnection());

    List<RSGroupAdminEndpoint> cps =
        master.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class);
    assertTrue(cps.size() > 0);
    rsGroupInfoManager = cps.get(0).getGroupInfoManager();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void rsGroupHasOnlineServer() throws IOException {
    rsGroupInfoManager.refresh();
    RSGroupInfo defaultGroup = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(RSGroupUtil.rsGroupHasOnlineServer(master, defaultGroup));

    HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
    rsGroupAdminClient.addRSGroup(GROUP_NAME);
    rsGroupAdminClient.moveServers(Collections.singleton(rs.getServerName().getAddress()), GROUP_NAME);

    rsGroupInfoManager.refresh();
    RSGroupInfo rsGroup = rsGroupInfoManager.getRSGroup(GROUP_NAME);
    assertTrue(RSGroupUtil.rsGroupHasOnlineServer(master, rsGroup));

    rsGroupAdminClient.addRSGroup("empty");
    rsGroupInfoManager.refresh();
    RSGroupInfo emptyGroup = rsGroupInfoManager.getRSGroup("empty");
    assertFalse(RSGroupUtil.rsGroupHasOnlineServer(master, emptyGroup));
  }
}
