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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestRpcConnectionRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcConnectionRegistry.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private RpcConnectionRegistry registry;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // allow refresh immediately so we will switch to use region servers soon.
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.INITIAL_REFRESH_DELAY_SECS, 1);
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.PERIODIC_REFRESH_INTERVAL_SECS, 1);
    UTIL.getConfiguration().setLong(RpcConnectionRegistry.MIN_SECS_BETWEEN_REFRESHES, 0);
    UTIL.startMiniCluster(3);
    HBaseTestingUtility.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    registry = new RpcConnectionRegistry(UTIL.getConfiguration());
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
    // sleep 3 seconds, since our initial delay is 1 second, we should have refreshed the endpoints
    Thread.sleep(3000);
    assertThat(registry.getParsedServers(),
      hasItems(activeMaster.getServerManager().getOnlineServersList().toArray(new ServerName[0])));

    // Add wait on all replicas being assigned before proceeding w/ test. Failed on occasion
    // because not all replicas had made it up before test started.
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(UTIL, registry);

    assertEquals(registry.getClusterId().get(), activeMaster.getClusterId());
    assertEquals(registry.getActiveMaster().get(), activeMaster.getServerName());
    List<HRegionLocation> metaLocations =
      Arrays.asList(registry.getMetaRegionLocations().get().getRegionLocations());
    List<HRegionLocation> actualMetaLocations =
      activeMaster.getMetaRegionLocationCache().getMetaRegionLocations().get();
    Collections.sort(metaLocations);
    Collections.sort(actualMetaLocations);
    assertEquals(actualMetaLocations, metaLocations);

    // test that the node count config works
    setMaxNodeCount(1);
    UTIL.waitFor(10000, () -> registry.getParsedServers().size() == 1);
  }
}
