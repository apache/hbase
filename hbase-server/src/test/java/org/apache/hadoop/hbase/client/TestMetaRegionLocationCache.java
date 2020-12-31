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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetaRegionLocationCache;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({SmallTests.class, MasterTests.class })
public class TestMetaRegionLocationCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaRegionLocationCache.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ConnectionRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    HBaseTestingUtility.setReplicas(TEST_UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    REGISTRY = ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, REGISTRY);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    Closeables.close(REGISTRY, true);
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
    // Wait until initial meta locations are loaded.
    int retries = 0;
    while (!master.getMetaRegionLocationCache().getMetaRegionLocations().isPresent()) {
      Thread.sleep(1000);
      if (++retries == 10) {
        break;
      }
    }
    List<HRegionLocation> metaHRLs =
        master.getMetaRegionLocationCache().getMetaRegionLocations().get();
    assertFalse(metaHRLs.isEmpty());
    ZKWatcher zk = master.getZooKeeper();
    List<String> metaZnodes = zk.getMetaReplicaNodes();
    // Wait till all replicas available.
    retries = 0;
    while (master.getMetaRegionLocationCache().getMetaRegionLocations().get().size() !=
        metaZnodes.size()) {
      Thread.sleep(1000);
      if (++retries == 10) {
        break;
      }
    }
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
    standBy.isInitialized();
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
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, REGISTRY);
    for (JVMClusterUtil.MasterThread masterThread:
        TEST_UTIL.getMiniHBaseCluster().getMasterThreads()) {
      verifyCachedMetaLocations(masterThread.getMaster());
    }
  }

  /**
   * Tests MetaRegionLocationCache's init procedure to make sure that it correctly watches the base
   * znode for notifications.
   */
  @Test public void testMetaRegionLocationCache() throws Exception {
    final String parentZnodeName = "/randomznodename";
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parentZnodeName);
    ServerName sn = ServerName.valueOf("localhost", 1234, 5678);
    try (ZKWatcher zkWatcher = new ZKWatcher(conf, null, null, true)) {
      // A thread that repeatedly creates and drops an unrelated child znode. This is to simulate
      // some ZK activity in the background.
      MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(conf);
      ctx.addThread(new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        @Override public void doAnAction() throws Exception {
          final String testZnode = parentZnodeName + "/child";
          ZKUtil.createNodeIfNotExistsAndWatch(zkWatcher, testZnode, testZnode.getBytes());
          ZKUtil.deleteNode(zkWatcher, testZnode);
        }
      });
      ctx.startThreads();
      try {
        MetaRegionLocationCache metaCache = new MetaRegionLocationCache(zkWatcher);
        // meta znodes do not exist at this point, cache should be empty.
        assertFalse(metaCache.getMetaRegionLocations().isPresent());
        // Set the meta locations for a random meta replicas, simulating an active hmaster meta
        // assignment.
        for (int i = 0; i < 3; i++) {
          // Updates the meta znodes.
          MetaTableLocator.setMetaLocation(zkWatcher, sn, i, RegionState.State.OPEN);
        }
        // Wait until the meta cache is populated.
        int iters = 0;
        while (iters++ < 10) {
          if (metaCache.getMetaRegionLocations().isPresent()
            && metaCache.getMetaRegionLocations().get().size() == 3) {
            break;
          }
          Thread.sleep(1000);
        }
        List<HRegionLocation> metaLocations = metaCache.getMetaRegionLocations().get();
        assertEquals(3, metaLocations.size());
        for (HRegionLocation location : metaLocations) {
          assertEquals(sn, location.getServerName());
        }
      } finally {
        // clean up.
        ctx.stop();
        ZKUtil.deleteChildrenRecursively(zkWatcher, parentZnodeName);
      }
    }
  }
}
