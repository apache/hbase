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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests handling of meta-carrying region server failover.
 */
@Tag(MediumTests.TAG)
public class TestMetaShutdownHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetaShutdownHandler.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(3)
      .rsClass(MyRegionServer.class).numDataNodes(3).build();
    TEST_UTIL.startMiniCluster(option);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test will test the expire handling of a meta-carrying region server. After
   * HBaseMiniCluster is up, we will delete the ephemeral node of the meta-carrying region server,
   * which will trigger the expire of this region server on the master. On the other hand, we will
   * slow down the abort process on the region server so that it is still up during the master SSH.
   * We will check that the master SSH is still successfully done.
   */
  @Test
  public void testExpireMetaRegionServer() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    ServerName metaServerName =
      regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO);
    if (
      master.getServerName().equals(metaServerName) || metaServerName == null
        || !metaServerName.equals(cluster.getServerHoldingMeta())
    ) {
      // Move meta off master
      metaServerName =
        cluster.getLiveRegionServerThreads().get(0).getRegionServer().getServerName();
      master.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        Bytes.toBytes(metaServerName.getServerName()));
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      metaServerName = regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO);
    }
    RegionState metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper());
    assertEquals(metaState.getState(), RegionState.State.OPEN, "Wrong state for meta!");
    assertNotEquals(master.getServerName(), metaServerName, "Meta is on master!");
    HRegionServer metaRegionServer = cluster.getRegionServer(metaServerName);

    // Delete the ephemeral node of the meta-carrying region server.
    // This is trigger the expire of this region server on the master.
    String rsEphemeralNodePath = ZNodePaths.joinZNode(master.getZooKeeper().getZNodePaths().rsZNode,
      metaServerName.toString());
    ZKUtil.deleteNode(master.getZooKeeper(), rsEphemeralNodePath);
    LOG.info("Deleted the znode for the RegionServer hosting hbase:meta; waiting on SSH");
    // Wait for SSH to finish
    final ServerManager serverManager = master.getServerManager();
    final ServerName priorMetaServerName = metaServerName;
    TEST_UTIL.waitFor(60000, 100, () -> metaRegionServer.isStopped());
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !serverManager.isServerOnline(priorMetaServerName)
          && !serverManager.areDeadServersInProgress();
      }
    });
    LOG.info("Past wait on RIT");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    // Now, make sure meta is assigned
    assertTrue(regionStates.isRegionOnline(HRegionInfo.FIRST_META_REGIONINFO),
      "Meta should be assigned");
    // Now, make sure meta is registered in zk
    metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper());
    assertEquals(RegionState.State.OPEN, metaState.getState(), "Meta should not be in transition");
    assertEquals(metaState.getServerName(),
      regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO),
      "Meta should be assigned");
    assertNotEquals(metaState.getServerName(), metaServerName,
      "Meta should be assigned on a different server");
  }

  public static class MyRegionServer extends MiniHBaseClusterRegionServer {

    public MyRegionServer(Configuration conf)
      throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    public void abort(String reason, Throwable cause) {
      // sleep to slow down the region server abort
      try {
        Thread.sleep(30 * 1000);
      } catch (InterruptedException e) {
        return;
      }
      super.abort(reason, cause);
    }
  }
}
