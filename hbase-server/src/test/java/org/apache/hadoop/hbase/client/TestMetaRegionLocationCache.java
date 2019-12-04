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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetaRegionLocationCache;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class, MasterTests.class })
public class TestMetaRegionLocationCache {

  private static final Log LOG = LogFactory.getLog(TestMetaRegionLocationCache.class.getName());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Registry REGISTRY;

  // waits for all replicas to have region location
  static void waitUntilAllMetaReplicasHavingRegionLocation(Configuration conf,
       final Registry registry, final int regionReplication) throws IOException {
    Waiter.waitFor(conf, conf.getLong(
        "hbase.client.sync.wait.timeout.msec", 60000), 200, true,
        new Waiter.ExplainingPredicate<IOException>() {
          @Override
          public String explainFailure() throws IOException {
            return "Not all meta replicas get assigned";
          }

          @Override
          public boolean evaluate() throws IOException {
            try {
              RegionLocations locs = registry.getMetaRegionLocation();
              if (locs == null || locs.size() < regionReplication) {
                return false;
              }
              for (int i = 0; i < regionReplication; i++) {
                if (locs.getRegionLocation(i) == null) {
                  return false;
                }
              }
              return true;
            } catch (Exception e) {
              LOG.warn("Failed to get meta region locations", e);
              return false;
            }
          }
        });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    REGISTRY = RegistryFactory.getRegistry(TEST_UTIL.getConnection());
    waitUntilAllMetaReplicasHavingRegionLocation(
        TEST_UTIL.getConfiguration(), REGISTRY, 3);
    TEST_UTIL.getConnection().getAdmin().setBalancerRunning(false, true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private List<HRegionLocation> getCurrentMetaLocations(ZooKeeperWatcher zk) throws Exception {
    List<HRegionLocation> result = new ArrayList<>();
    for (String znode: zk.getMetaReplicaNodes()) {
      String path = ZKUtil.joinZNode(zk.baseZNode, znode);
      int replicaId = zk.getMetaReplicaIdFromPath(path);
      RegionState state = MetaTableLocator.getMetaRegionState(zk, replicaId);
      result.add(new HRegionLocation(state.getRegion(), state.getServerName()));
    }
    return result;
  }

  // Verifies that the cached meta locations in the given master are in sync with what is in ZK.
  private void verifyCachedMetaLocations(final HMaster master) throws Exception {
    // Wait until initial meta locations are loaded.
    ZooKeeperWatcher zk = master.getZooKeeper();
    final List<String> metaZnodes = zk.getMetaReplicaNodes();
    assertEquals(3, metaZnodes.size());
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.getMetaRegionLocationCache().getMetaRegionLocations().size()
            == metaZnodes.size();
      }
    });
    List<HRegionLocation> metaHRLs = master.getMetaRegionLocationCache().getMetaRegionLocations();
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

  private static ServerName getOtherRS(List<ServerName> allServers, ServerName except) {
    Preconditions.checkArgument(allServers.size() > 0);
    allServers.remove(except);
    ServerName ret;
    try {
      Collections.shuffle(allServers);
      ret = allServers.get(0);
    } finally {
      allServers.add(except);
    }
    return ret;
  }

  /*
   * Shuffles the meta region replicas around the cluster and makes sure the cache is not stale.
   */
  @Test public void testMetaLocationsChange() throws Exception {
    List<HRegionLocation> currentMetaLocs =
        getCurrentMetaLocations(TEST_UTIL.getMiniHBaseCluster().getMaster().getZooKeeper());
    List<ServerName> allServers = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rs:
        TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      allServers.add(rs.getRegionServer().getServerName());
    }
    // Move these replicas to random servers.
    for (HRegionLocation location: currentMetaLocs) {
      TEST_UTIL.moveRegionAndWait(
          location.getRegionInfo(), getOtherRS(allServers, location.getServerName()));
    }
    waitUntilAllMetaReplicasHavingRegionLocation(
        TEST_UTIL.getConfiguration(), REGISTRY, 3);
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
    try (ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher(conf, null, null, true)) {
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
        assertTrue(metaCache.getMetaRegionLocations().isEmpty());
        // Set the meta locations for a random meta replicas, simulating an active hmaster meta
        // assignment.
        for (int i = 0; i < 3; i++) {
          // Updates the meta znodes.
          MetaTableLocator.setMetaLocation(zkWatcher, sn, i, RegionState.State.OPEN);
        }
        // Wait until the meta cache is populated.
        int iters = 0;
        while (iters++ < 10) {
          if (metaCache.getMetaRegionLocations().size() == 3) {
            break;
          }
          Thread.sleep(1000);
        }
        List<HRegionLocation> metaLocations = metaCache.getMetaRegionLocations();
        assertNotNull(metaLocations);
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
