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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.BootstrapNodeManager;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestRpcConnectionRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcConnectionRegistry.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private RpcConnectionRegistry registry;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // allow refresh immediately so we will switch to use region servers soon.
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.INITIAL_REFRESH_DELAY_SECS, 1);
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.PERIODIC_REFRESH_INTERVAL_SECS, 1);
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.MIN_SECS_BETWEEN_REFRESHES, 0);
    UTIL.getConfiguration().setLong(BootstrapNodeManager.REQUEST_MASTER_MIN_INTERVAL_SECS, 1);
    UTIL.startMiniCluster(3);
    HBaseTestingUtil.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    registry = new RpcConnectionRegistry(UTIL.getConfiguration(), User.getCurrent());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(registry, true);
  }

  private void setMaxNodeCount(int count) {
    UTIL.getMiniHBaseCluster().getMasterThreads().stream()
      .map(t -> t.getMaster().getConfiguration())
      .forEach(conf -> conf.setInt(RSRpcServices.CLIENT_BOOTSTRAP_NODE_LIMIT, count));
    UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getConfiguration())
      .forEach(conf -> conf.setInt(RSRpcServices.CLIENT_BOOTSTRAP_NODE_LIMIT, count));
  }

  @Test
  public void testRegistryRPCs() throws Exception {
    HMaster activeMaster = UTIL.getHBaseCluster().getMaster();
    // should only contains the active master
    Set<ServerName> initialParsedServers = registry.getParsedServers();
    assertThat(initialParsedServers, hasSize(1));
    // no start code in configuration
    assertThat(initialParsedServers,
      hasItem(ServerName.valueOf(activeMaster.getServerName().getHostname(),
        activeMaster.getServerName().getPort(), -1)));
    // Since our initial delay is 1 second, finally we should have refreshed the endpoints
    UTIL.waitFor(5000, () -> registry.getParsedServers()
      .contains(activeMaster.getServerManager().getOnlineServersList().get(0)));
    Set<ServerName> parsedServers = registry.getParsedServers();
    assertThat(parsedServers,
      hasSize(activeMaster.getServerManager().getOnlineServersList().size()));
    assertThat(parsedServers,
      hasItems(activeMaster.getServerManager().getOnlineServersList().toArray(new ServerName[0])));

    // Add wait on all replicas being assigned before proceeding w/ test. Failed on occasion
    // because not all replicas had made it up before test started.
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(UTIL, registry);

    assertEquals(registry.getClusterId().get(), activeMaster.getClusterId());
    assertEquals(registry.getActiveMaster().get(), activeMaster.getServerName());
    List<HRegionLocation> metaLocations =
      Arrays.asList(registry.getMetaRegionLocations().get().getRegionLocations());
    List<HRegionLocation> actualMetaLocations = activeMaster.getMetaLocations();
    Collections.sort(metaLocations);
    Collections.sort(actualMetaLocations);
    assertEquals(actualMetaLocations, metaLocations);

    // test that the node count config works
    setMaxNodeCount(1);
    UTIL.waitFor(10000, () -> registry.getParsedServers().size() == 1);
  }

  /**
   * Make sure that we can create the RpcClient when there are broken servers in the bootstrap nodes
   */
  @Test
  public void testBrokenBootstrapNodes() throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    String currentMasterAddrs = Preconditions.checkNotNull(conf.get(HConstants.MASTER_ADDRS_KEY));
    HMaster activeMaster = UTIL.getHBaseCluster().getMaster();
    String clusterId = activeMaster.getClusterId();
    // Add a non-working master
    ServerName badServer = ServerName.valueOf("localhost", 1234, -1);
    conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES, badServer.toShortString());
    // only a bad server, the request should fail
    try (RpcConnectionRegistry reg = new RpcConnectionRegistry(conf, User.getCurrent())) {
      assertThrows(IOException.class, () -> reg.getParsedServers());
    }

    conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES,
      badServer.toShortString() + ", " + currentMasterAddrs);
    // we will choose bootstrap node randomly so here we need to test it multiple times to make sure
    // that we can skip the broken node
    for (int i = 0; i < 10; i++) {
      try (RpcConnectionRegistry reg = new RpcConnectionRegistry(conf, User.getCurrent())) {
        assertEquals(clusterId, reg.getClusterId().get());
      }
    }
  }
}
