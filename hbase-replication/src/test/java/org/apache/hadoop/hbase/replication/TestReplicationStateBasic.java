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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * White box testing for replication state interfaces. Implementations should extend this class, and
 * initialize the interfaces properly.
 */
public abstract class TestReplicationStateBasic {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationStateBasic.class);

  protected ServerName server1 = ServerName.valueOf("hostname1.example.org", 1234, 12345);
  protected ServerName server2 = ServerName.valueOf("hostname2.example.org", 1234, 12345);
  protected ServerName server3 = ServerName.valueOf("hostname3.example.org", 1234, 12345);
  protected ReplicationPeers rp;
  protected static final String ID_ONE = "1";
  protected static final String ID_TWO = "2";
  protected static String KEY_ONE;
  protected static String KEY_TWO;

  // For testing when we try to replicate to ourself
  protected String OUR_KEY;

  protected static int zkTimeoutCount;
  protected static final int ZK_MAX_COUNT = 300;
  protected static final int ZK_SLEEP_INTERVAL = 100; // millis

  @Test
  public void testReplicationPeers() throws Exception {
    rp.init();

    try {
      rp.getPeerStorage().setPeerState("bogus", true);
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (ReplicationException e) {
    }
    try {
      rp.getPeerStorage().setPeerState("bogus", false);
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (ReplicationException e) {
    }

    try {
      assertFalse(rp.addPeer("bogus"));
      fail("Should have thrown an ReplicationException when passed a bogus peerId");
    } catch (ReplicationException e) {
    }

    assertNumberOfPeers(0);

    // Add some peers
    rp.getPeerStorage().addPeer(ID_ONE,
      ReplicationPeerConfig.newBuilder().setClusterKey(KEY_ONE).build(), true,
      SyncReplicationState.NONE);
    assertNumberOfPeers(1);
    rp.getPeerStorage().addPeer(ID_TWO,
      ReplicationPeerConfig.newBuilder().setClusterKey(KEY_TWO).build(), true,
      SyncReplicationState.NONE);
    assertNumberOfPeers(2);

    assertEquals(KEY_ONE, ZKConfig.getZooKeeperClusterKey(ReplicationPeerConfigUtil
      .getPeerClusterConfiguration(rp.getConf(), rp.getPeerStorage().getPeerConfig(ID_ONE))));
    rp.getPeerStorage().removePeer(ID_ONE);
    rp.removePeer(ID_ONE);
    assertNumberOfPeers(1);

    // Add one peer
    rp.getPeerStorage().addPeer(ID_ONE,
      ReplicationPeerConfig.newBuilder().setClusterKey(KEY_ONE).build(), true,
      SyncReplicationState.NONE);
    rp.addPeer(ID_ONE);
    assertNumberOfPeers(2);
    assertTrue(rp.getPeer(ID_ONE).isPeerEnabled());
    rp.getPeerStorage().setPeerState(ID_ONE, false);
    // now we do not rely on zk watcher to trigger the state change so we need to trigger it
    // manually...
    ReplicationPeerImpl peer = rp.getPeer(ID_ONE);
    rp.refreshPeerState(peer.getId());
    assertEquals(PeerState.DISABLED, peer.getPeerState());
    assertConnectedPeerStatus(false, ID_ONE);
    rp.getPeerStorage().setPeerState(ID_ONE, true);
    // now we do not rely on zk watcher to trigger the state change so we need to trigger it
    // manually...
    rp.refreshPeerState(peer.getId());
    assertEquals(PeerState.ENABLED, peer.getPeerState());
    assertConnectedPeerStatus(true, ID_ONE);

    // Disconnect peer
    rp.removePeer(ID_ONE);
    assertNumberOfPeers(2);
  }

  private void assertConnectedPeerStatus(boolean status, String peerId) throws Exception {
    // we can first check if the value was changed in the store, if it wasn't then fail right away
    if (status != rp.getPeerStorage().isPeerEnabled(peerId)) {
      fail("ConnectedPeerStatus was " + !status + " but expected " + status + " in ZK");
    }
    while (true) {
      if (status == rp.getPeer(peerId).isPeerEnabled()) {
        return;
      }
      if (zkTimeoutCount < ZK_MAX_COUNT) {
        LOG.debug("ConnectedPeerStatus was " + !status + " but expected " + status
          + ", sleeping and trying again.");
        Thread.sleep(ZK_SLEEP_INTERVAL);
      } else {
        fail("Timed out waiting for ConnectedPeerStatus to be " + status);
      }
    }
  }

  private void assertNumberOfPeers(int total) throws ReplicationException {
    assertEquals(total, rp.getPeerStorage().listPeerIds().size());
  }
}
