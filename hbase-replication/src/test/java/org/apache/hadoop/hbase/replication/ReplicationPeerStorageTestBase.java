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

import static org.apache.hadoop.hbase.replication.ReplicationPeerConfigTestUtil.assertConfigEquals;
import static org.apache.hadoop.hbase.replication.ReplicationPeerConfigTestUtil.getConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.util.List;
import org.junit.Test;

public abstract class ReplicationPeerStorageTestBase {

  protected static ReplicationPeerStorage STORAGE;

  @Test
  public void test() throws ReplicationException {
    int peerCount = 10;
    for (int i = 0; i < peerCount; i++) {
      STORAGE.addPeer(Integer.toString(i), getConfig(i), i % 2 == 0,
        SyncReplicationState.valueOf(i % 4));
    }
    List<String> peerIds = STORAGE.listPeerIds();
    assertEquals(peerCount, peerIds.size());
    for (String peerId : peerIds) {
      int seed = Integer.parseInt(peerId);
      assertConfigEquals(getConfig(seed), STORAGE.getPeerConfig(peerId));
    }
    for (int i = 0; i < peerCount; i++) {
      STORAGE.updatePeerConfig(Integer.toString(i), getConfig(i + 1));
    }
    for (String peerId : peerIds) {
      int seed = Integer.parseInt(peerId);
      assertConfigEquals(getConfig(seed + 1), STORAGE.getPeerConfig(peerId));
    }
    for (int i = 0; i < peerCount; i++) {
      assertEquals(i % 2 == 0, STORAGE.isPeerEnabled(Integer.toString(i)));
    }
    for (int i = 0; i < peerCount; i++) {
      STORAGE.setPeerState(Integer.toString(i), i % 2 != 0);
    }
    for (int i = 0; i < peerCount; i++) {
      assertEquals(i % 2 != 0, STORAGE.isPeerEnabled(Integer.toString(i)));
    }
    for (int i = 0; i < peerCount; i++) {
      assertEquals(SyncReplicationState.valueOf(i % 4),
        STORAGE.getPeerSyncReplicationState(Integer.toString(i)));
    }
    String toRemove = Integer.toString(peerCount / 2);
    STORAGE.removePeer(toRemove);
    peerIds = STORAGE.listPeerIds();
    assertEquals(peerCount - 1, peerIds.size());
    assertFalse(peerIds.contains(toRemove));

    try {
      STORAGE.getPeerConfig(toRemove);
      fail("Should throw a ReplicationException when getting peer config of a removed peer");
    } catch (ReplicationException e) {
    }
  }

  protected abstract void removePeerSyncRelicationState(String peerId) throws Exception;

  protected abstract void assertPeerSyncReplicationStateCreate(String peerId) throws Exception;

  @Test
  public void testNoSyncReplicationState() throws Exception {
    // This could happen for a peer created before we introduce sync replication.
    String peerId = "testNoSyncReplicationState";
    assertThrows("Should throw a ReplicationException when getting state of inexist peer",
      ReplicationException.class, () -> STORAGE.getPeerSyncReplicationState(peerId));
    assertThrows("Should throw a ReplicationException when getting state of inexist peer",
      ReplicationException.class, () -> STORAGE.getPeerNewSyncReplicationState(peerId));

    STORAGE.addPeer(peerId, getConfig(0), true, SyncReplicationState.NONE);
    // delete the sync replication state node to simulate
    removePeerSyncRelicationState(peerId);
    // should not throw exception as the peer exists
    assertEquals(SyncReplicationState.NONE, STORAGE.getPeerSyncReplicationState(peerId));
    assertEquals(SyncReplicationState.NONE, STORAGE.getPeerNewSyncReplicationState(peerId));
    // make sure we create the node for the old format peer
    assertPeerSyncReplicationStateCreate(peerId);
  }

  protected abstract void assertPeerNameControlException(ReplicationException e);

  @Test
  public void testPeerNameControl() throws Exception {
    String clusterKey = "key";
    STORAGE.addPeer("6", ReplicationPeerConfig.newBuilder().setClusterKey(clusterKey).build(), true,
      SyncReplicationState.NONE);

    try {
      ReplicationException e = assertThrows(ReplicationException.class,
        () -> STORAGE.addPeer("6",
          ReplicationPeerConfig.newBuilder().setClusterKey(clusterKey).build(), true,
          SyncReplicationState.NONE));
      assertPeerNameControlException(e);
    } finally {
      // clean up
      STORAGE.removePeer("6");
    }
  }
}
