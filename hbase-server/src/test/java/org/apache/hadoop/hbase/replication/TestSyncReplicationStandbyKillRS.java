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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationStandbyKillRS extends SyncReplicationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestSyncReplicationStandbyKillRS.class);

  private final long SLEEP_TIME = 1000;

  private final int COUNT = 1000;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationStandbyKillRS.class);

  @Test
  public void testStandbyKillRegionServer() throws Exception {
    MasterFileSystem mfs = UTIL2.getHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = getRemoteWALDir(mfs, PEER_ID);
    assertFalse(mfs.getWALFileSystem().exists(remoteWALDir));
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    assertTrue(mfs.getWALFileSystem().exists(remoteWALDir));
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    // Disable async replication and write data, then shutdown
    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    write(UTIL1, 0, COUNT);
    UTIL1.shutdownMiniCluster();

    JVMClusterUtil.MasterThread activeMaster = UTIL2.getMiniHBaseCluster().getMasterThread();
    String threadName = "RegionServer-Restarter";
    Thread t = new Thread(() -> {
      try {
        List<JVMClusterUtil.RegionServerThread> regionServers =
          new ArrayList<>(UTIL2.getMiniHBaseCluster().getLiveRegionServerThreads());
        LOG.debug("Going to stop {} RSes: [{}]", regionServers.size(),
          regionServers.stream().map(rst -> rst.getRegionServer().getServerName().getServerName())
            .collect(Collectors.joining(", ")));
        for (JVMClusterUtil.RegionServerThread rst : regionServers) {
          ServerName serverName = rst.getRegionServer().getServerName();
          LOG.debug("Going to RS stop [{}]", serverName);
          rst.getRegionServer().stop("Stop RS for test");
          waitForRSShutdownToStartAndFinish(activeMaster, serverName);
          LOG.debug("Going to start a new RS");
          JVMClusterUtil.RegionServerThread restarted =
            UTIL2.getMiniHBaseCluster().startRegionServer();
          LOG.debug("Waiting RS [{}] to online", restarted.getRegionServer().getServerName());
          restarted.waitForServerOnline();
          LOG.debug("Waiting the old RS {} thread to quit", rst.getName());
          rst.join();
          LOG.debug("Done stop RS [{}] and restart [{}]", serverName,
            restarted.getRegionServer().getServerName());
        }
        LOG.debug("All RSes restarted");
      } catch (Exception e) {
        LOG.error("Failed to kill RS", e);
      }
    }, threadName);
    t.start();

    LOG.debug("Going to transit peer {} to {} state", PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    // Transit standby to DA to replay logs
    try {
      UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
        SyncReplicationState.DOWNGRADE_ACTIVE);
    } catch (Exception e) {
      LOG.error("Failed to transit standby cluster to " + SyncReplicationState.DOWNGRADE_ACTIVE, e);
    }

    LOG.debug("Waiting for the restarter thread {} to quit", threadName);
    t.join();

    while (
      UTIL2.getAdmin().getReplicationPeerSyncReplicationState(PEER_ID)
          != SyncReplicationState.DOWNGRADE_ACTIVE
    ) {
      LOG.debug("Waiting for peer {} to be in {} state", PEER_ID,
        SyncReplicationState.DOWNGRADE_ACTIVE);
      Thread.sleep(SLEEP_TIME);
    }
    LOG.debug("Going to verify the result, {} records expected", COUNT);
    verify(UTIL2, 0, COUNT);
    LOG.debug("Verification successfully done");
  }

  private void waitForRSShutdownToStartAndFinish(JVMClusterUtil.MasterThread activeMaster,
    ServerName serverName) throws InterruptedException, IOException {
    ServerManager sm = activeMaster.getMaster().getServerManager();
    // First wait for it to be in dead list
    while (!sm.getDeadServers().isDeadServer(serverName)) {
      LOG.debug("Waiting for {} to be listed as dead in master", serverName);
      Thread.sleep(SLEEP_TIME);
    }
    LOG.debug("Server {} marked as dead, waiting for it to finish dead processing", serverName);
    while (sm.areDeadServersInProgress()) {
      LOG.debug("Server {} still being processed, waiting", serverName);
      Thread.sleep(SLEEP_TIME);
    }
    LOG.debug("Server {} done with server shutdown processing", serverName);
  }
}
