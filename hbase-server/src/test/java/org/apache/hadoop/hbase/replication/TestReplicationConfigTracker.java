/*
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationConfigTracker extends TestReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestReplicationKillRS.class);

  @Test
  public void testReplicationConfigTracker() throws Exception {
    // killing the RS with hbase:meta can result into failed puts until we solve
    // IO fencing
    int rsToKill1 = utility1.getHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;
    int otherRs = rsToKill1 == 0 ? 1 : 0;
    final HRegionServer regionServer = utility1.getHBaseCluster().getRegionServer(otherRs);
    final Thread listenerTracker = trackListener(utility1, otherRs);
    LOG.info("Start loading table");
    utility1.loadTable(htable1, famName, true);
    LOG.info("Done loading table");
    utility1.getHBaseCluster().getRegionServer(rsToKill1).abort("Stopping as part of the test");
    utility1.getHBaseCluster().waitOnRegionServer(rsToKill1);
    while (utility1.getHBaseCluster().getMaster().getServerManager().areDeadServersInProgress()) {
      LOG.info("Waiting on processing of crashed server before proceeding...");
      Threads.sleep(1000);
    }
    Waiter.waitFor(utility1.getConfiguration(), 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return !listenerTracker.isAlive();
      }
    });
    final ReplicationPeerZKImpl.PeerConfigTracker tracker = getPeerConfigTracker(regionServer);
    Waiter.waitFor(utility1.getConfiguration(), 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return tracker.getListeners().size() == 1;
      }
    });
  }

  private static Thread trackListener(final HBaseTestingUtility utility, final int rs) {
    Thread trackListener = new Thread() {
      public void run() {
        Replication replication = (Replication) utility.getHBaseCluster().getRegionServer(rs)
            .getReplicationSourceService();
        ReplicationSourceManager manager = replication.getReplicationManager();
        ReplicationPeerZKImpl replicationPeerZK =
            (ReplicationPeerZKImpl) manager.getReplicationPeers().getPeer(PEER_ID);
        ReplicationPeerZKImpl.PeerConfigTracker peerConfigTracker =
            replicationPeerZK.getPeerConfigTracker();
        while (peerConfigTracker.getListeners().size() != 2) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            LOG.error("track config failed", e);
          }
        }
      }
    };
    trackListener.setDaemon(true);
    trackListener.start();
    return trackListener;
  }

  private ReplicationPeerZKImpl.PeerConfigTracker getPeerConfigTracker(HRegionServer rs) {
    Replication replication = (Replication) rs.getReplicationSourceService();
    ReplicationSourceManager manager = replication.getReplicationManager();
    ReplicationPeerZKImpl replicationPeerZK =
        (ReplicationPeerZKImpl) manager.getReplicationPeers().getPeer(PEER_ID);
    ReplicationPeerZKImpl.PeerConfigTracker peerConfigTracker =
        replicationPeerZK.getPeerConfigTracker();
    return peerConfigTracker;
  }
}