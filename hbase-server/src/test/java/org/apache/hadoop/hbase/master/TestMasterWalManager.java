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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the master wal manager in a local cluster
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterWalManager {
  private static final Log LOG = LogFactory.getLog(TestMasterWalManager.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRemoveStaleRecoveringRegionsDuringMasterInitialization() throws Exception {
    // this test is for when distributed log replay is enabled
    if (!UTIL.getConfiguration().getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false)) return;

    LOG.info("Starting testRemoveStaleRecoveringRegionsDuringMasterInitialization");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterWalManager mwm = master.getMasterWalManager();

    String failedRegion = "failedRegoin1";
    String staleRegion = "staleRegion";
    ServerName inRecoveryServerName = ServerName.valueOf("mgr,1,1");
    ServerName previouselyFaildServerName = ServerName.valueOf("previous,1,1");
    String walPath = "/hbase/data/.logs/" + inRecoveryServerName.getServerName()
        + "-splitting/test";
    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, walPath),
      new SplitLogTask.Owned(inRecoveryServerName, mwm.getLogRecoveryMode()).toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    String staleRegionPath = ZKUtil.joinZNode(zkw.znodePaths.recoveringRegionsZNode, staleRegion);
    ZKUtil.createWithParents(zkw, staleRegionPath);
    String inRecoveringRegionPath = ZKUtil.joinZNode(zkw.znodePaths.recoveringRegionsZNode,
      failedRegion);
    inRecoveringRegionPath = ZKUtil.joinZNode(inRecoveringRegionPath,
      inRecoveryServerName.getServerName());
    ZKUtil.createWithParents(zkw, inRecoveringRegionPath);
    Set<ServerName> servers = new HashSet<>();
    servers.add(previouselyFaildServerName);
    mwm.removeStaleRecoveringRegionsFromZK(servers);

    // verification
    assertFalse(ZKUtil.checkExists(zkw, staleRegionPath) != -1);
    assertTrue(ZKUtil.checkExists(zkw, inRecoveringRegionPath) != -1);

    ZKUtil.deleteChildrenRecursively(zkw, zkw.znodePaths.recoveringRegionsZNode);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.znodePaths.splitLogZNode);
    zkw.close();
  }
}
