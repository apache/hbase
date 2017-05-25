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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests handling of meta-carrying region server failover.
 */
@Category(MediumTests.class)
public class TestMetaShutdownHandler {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1, 3, null, null, MyRegionServer.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test will test the expire handling of a meta-carrying
   * region server.
   * After HBaseMiniCluster is up, we will delete the ephemeral
   * node of the meta-carrying region server, which will trigger
   * the expire of this region server on the master.
   * On the other hand, we will slow down the abort process on
   * the region server so that it is still up during the master SSH.
   * We will check that the master SSH is still successfully done.
   */
  @Test (timeout=180000)
  public void testExpireMetaRegionServer() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    ServerName metaServerName = regionStates.getRegionServerOfRegion(
      HRegionInfo.FIRST_META_REGIONINFO);
    if (master.getServerName().equals(metaServerName) || metaServerName == null
        || !metaServerName.equals(cluster.getServerHoldingMeta())) {
      // Move meta off master
      metaServerName = cluster.getLiveRegionServerThreads()
          .get(0).getRegionServer().getServerName();
      master.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        Bytes.toBytes(metaServerName.getServerName()));
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    }
    RegionState metaState =
        MetaTableLocator.getMetaRegionState(master.getZooKeeper());
    assertEquals("Meta should be not in transition",
      metaState.getState(), RegionState.State.OPEN);
    assertNotEquals("Meta should be moved off master",
      metaServerName, master.getServerName());

    // Delete the ephemeral node of the meta-carrying region server.
    // This is trigger the expire of this region server on the master.
    String rsEphemeralNodePath =
        ZKUtil.joinZNode(master.getZooKeeper().znodePaths.rsZNode, metaServerName.toString());
    ZKUtil.deleteNode(master.getZooKeeper(), rsEphemeralNodePath);
    // Wait for SSH to finish
    final ServerManager serverManager = master.getServerManager();
    final ServerName priorMetaServerName = metaServerName;
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !serverManager.isServerOnline(priorMetaServerName)
            && !serverManager.areDeadServersInProgress();
      }
    });

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    // Now, make sure meta is assigned
    assertTrue("Meta should be assigned",
      regionStates.isRegionOnline(HRegionInfo.FIRST_META_REGIONINFO));
    // Now, make sure meta is registered in zk
    metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper());
    assertEquals("Meta should be not in transition",
      metaState.getState(), RegionState.State.OPEN);
    assertEquals("Meta should be assigned", metaState.getServerName(),
      regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO));
    assertNotEquals("Meta should be assigned on a different server",
      metaState.getServerName(), metaServerName);
  }

  public static class MyRegionServer extends MiniHBaseClusterRegionServer {

    public MyRegionServer(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    public void abort(String reason, Throwable cause) {
      // sleep to slow down the region server abort
      try {
        Thread.sleep(30*1000);
      } catch (InterruptedException e) {
        return;
      }
      super.abort(reason, cause);
    }
  }
}
