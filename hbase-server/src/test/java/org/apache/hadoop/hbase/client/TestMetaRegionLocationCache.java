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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class, MasterTests.class })
public class TestMetaRegionLocationCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncMetaRegionLocator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static AsyncRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(BaseLoadBalancer.TABLES_ON_MASTER, "none");
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    REGISTRY = AsyncRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    RegionReplicaTestHelper.waitUntilAllMetaReplicasHavingRegionLocation(
        TEST_UTIL.getConfiguration(), REGISTRY, 3);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  private List<HRegionLocation> getCurrentMetaLocations(ZKWatcher zk) throws Exception {
    List<HRegionLocation> result = new ArrayList<>();
    for (String znode: zk.getMetaReplicaNodes()) {
      String path = ZNodePaths.joinZNode(zk.getZNodePaths().baseZNode, znode);
      int replicaId = zk.getZNodePaths().getMetaReplicaIdFromPath(path);
      RegionState state = MetaTableLocator.getMetaRegionState(zk, replicaId);
      result.add(new HRegionLocation(state.getRegion(), state.getServerName()));
    }
    return result;
  }

  // Verifies that the cached meta locations in the given master are in sync with what is in ZK.
  private void verifyCachedMetaLocations(HMaster master) throws Exception {
    List<HRegionLocation> metaHRLs =
        master.getMetaRegionLocationCache().getCachedMetaRegionLocations().get();
    assertTrue(metaHRLs != null);
    assertFalse(metaHRLs.isEmpty());
    ZKWatcher zk = master.getZooKeeper();
    List<String> metaZnodes = zk.getMetaReplicaNodes();
    assertEquals(metaZnodes.size(), metaHRLs.size());
    List<HRegionLocation> actualHRLs = getCurrentMetaLocations(zk);
    Collections.sort(metaHRLs);
    Collections.sort(actualHRLs);
    assertEquals(actualHRLs, metaHRLs);
  }

  @Test public void testInitialMetaLocations() throws Exception {
    verifyCachedMetaLocations(TEST_UTIL.getMiniHBaseCluster().getMaster());
  }

  @Test public void testStandByMetaLocations() throws Exception {
    HMaster standBy = TEST_UTIL.getMiniHBaseCluster().startMaster().getMaster();
    verifyCachedMetaLocations(standBy);
  }

  /*
   * Shuffles the meta region replicas around the cluster and makes sure the cache is not stale.
   */
  @Test public void testMetaLocationsChange() throws Exception {
    List<HRegionLocation> currentMetaLocs =
        getCurrentMetaLocations(TEST_UTIL.getMiniHBaseCluster().getMaster().getZooKeeper());
    // Move these replicas to random servers.
    for (HRegionLocation location: currentMetaLocs) {
      RegionReplicaTestHelper.moveRegion(TEST_UTIL, location);
    }
    RegionReplicaTestHelper.waitUntilAllMetaReplicasHavingRegionLocation(
        TEST_UTIL.getConfiguration(), REGISTRY, 3);
    for (JVMClusterUtil.MasterThread masterThread:
        TEST_UTIL.getMiniHBaseCluster().getMasterThreads()) {
      verifyCachedMetaLocations(masterThread.getMaster());
    }
  }
}