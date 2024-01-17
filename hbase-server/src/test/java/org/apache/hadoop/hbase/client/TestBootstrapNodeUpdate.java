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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.BootstrapNodeManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Make sure that we can update the bootstrap server from master to region server, and region server
 * could also contact each other to update the bootstrap nodes.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestBootstrapNodeUpdate {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBootstrapNodeUpdate.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static RpcConnectionRegistry REGISTRY;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(BootstrapNodeManager.REQUEST_MASTER_INTERVAL_SECS, 5);
    conf.setLong(BootstrapNodeManager.REQUEST_MASTER_MIN_INTERVAL_SECS, 1);
    conf.setLong(BootstrapNodeManager.REQUEST_REGIONSERVER_INTERVAL_SECS, 1);
    conf.setInt(HBaseRpcServicesBase.CLIENT_BOOTSTRAP_NODE_LIMIT, 2);
    conf.setLong(RpcConnectionRegistry.INITIAL_REFRESH_DELAY_SECS, 5);
    conf.setLong(RpcConnectionRegistry.PERIODIC_REFRESH_INTERVAL_SECS, 1);
    conf.setLong(RpcConnectionRegistry.MIN_SECS_BETWEEN_REFRESHES, 1);
    UTIL.startMiniCluster(3);
    REGISTRY = new RpcConnectionRegistry(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(REGISTRY, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testUpdate() throws Exception {
    ServerName activeMasterServerName = REGISTRY.getActiveMaster().get();
    ServerName masterInConf = ServerName.valueOf(activeMasterServerName.getHostname(),
      activeMasterServerName.getPort(), -1);
    // we should have master in the beginning
    assertThat(REGISTRY.getParsedServers(), hasItem(masterInConf));
    // and after refreshing, we will switch to use region servers
    UTIL.waitFor(15000, () -> !REGISTRY.getParsedServers().contains(masterInConf)
      && !REGISTRY.getParsedServers().contains(activeMasterServerName));
    Set<ServerName> parsedServers = REGISTRY.getParsedServers();
    assertEquals(2, parsedServers.size());
    // now kill one region server
    ServerName serverToKill = parsedServers.iterator().next();
    UTIL.getMiniHBaseCluster().killRegionServer(serverToKill);
    // wait until the region server disappears
    // since the min node limit is 2, this means region server will still contact each other for
    // getting bootstrap nodes, instead of requesting master directly, so this assert can make sure
    // that the getAllBootstrapNodes works fine, and also the client can communicate with region
    // server to update bootstrap nodes
    UTIL.waitFor(30000, () -> !REGISTRY.getParsedServers().contains(serverToKill));
    // should still have 2 servers, the remaining 2 live region servers
    assertEquals(2, parsedServers.size());
    // make sure the registry still works fine
    assertNotNull(REGISTRY.getClusterId().get());
  }
}
