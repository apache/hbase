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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PeerProcedureHandlerImpl implements PeerProcedureHandler {

  private final ReplicationSourceManager replicationSourceManager;
  private final KeyLocker<String> peersLock = new KeyLocker<>();

  public PeerProcedureHandlerImpl(ReplicationSourceManager replicationSourceManager) {
    this.replicationSourceManager = replicationSourceManager;
  }

  @Override
  public void addPeer(String peerId) throws IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    try {
      replicationSourceManager.addPeer(peerId);
    } finally {
      peerLock.unlock();
    }
  }

  @Override
  public void removePeer(String peerId) throws IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    try {
      if (replicationSourceManager.getReplicationPeers().getPeer(peerId) != null) {
        replicationSourceManager.removePeer(peerId);
      }
    } finally {
      peerLock.unlock();
    }
  }

  private void refreshPeerState(String peerId) throws ReplicationException, IOException {
    PeerState newState;
    Lock peerLock = peersLock.acquireLock(peerId);
    ReplicationPeerImpl peer = null;
    PeerState oldState = null;
    boolean success = false;
    try {
      peer = replicationSourceManager.getReplicationPeers().getPeer(peerId);
      if (peer == null) {
        throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
      }
      oldState = peer.getPeerState();
      newState = replicationSourceManager.getReplicationPeers().refreshPeerState(peerId);
      // RS need to start work with the new replication state change
      if (oldState.equals(PeerState.ENABLED) && newState.equals(PeerState.DISABLED)) {
        replicationSourceManager.refreshSources(peerId);
      }
      success = true;
    } finally {
      if (!success && peer != null) {
        // Reset peer state if refresh source failed
        peer.setPeerState(oldState.equals(PeerState.ENABLED));
      }
      peerLock.unlock();
    }
  }

  @Override
  public void enablePeer(String peerId) throws ReplicationException, IOException {
    refreshPeerState(peerId);
  }

  @Override
  public void disablePeer(String peerId) throws ReplicationException, IOException {
    refreshPeerState(peerId);
  }

  @Override
  public void updatePeerConfig(String peerId) throws ReplicationException, IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    ReplicationPeers peers = replicationSourceManager.getReplicationPeers();
    ReplicationPeerImpl peer = null;
    ReplicationPeerConfig oldConfig = null;
    PeerState oldState = null;
    boolean success = false;
    try {
      peer = peers.getPeer(peerId);
      if (peer == null) {
        throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
      }
      oldConfig = peer.getPeerConfig();
      oldState = peer.getPeerState();
      ReplicationPeerConfig newConfig = peers.refreshPeerConfig(peerId);
      // also need to refresh peer state here. When updating a serial replication peer we may
      // disable it first and then enable it.
      PeerState newState = peers.refreshPeerState(peerId);
      // RS need to start work with the new replication config change
      if (!ReplicationUtils.isNamespacesAndTableCFsEqual(oldConfig, newConfig) ||
        oldConfig.isSerial() != newConfig.isSerial() ||
        (oldState.equals(PeerState.ENABLED) && newState.equals(PeerState.DISABLED))) {
        replicationSourceManager.refreshSources(peerId);
      }
      success = true;
    } finally {
      if (!success && peer != null) {
        // Reset peer config if refresh source failed
        peer.setPeerConfig(oldConfig);
        peer.setPeerState(oldState.equals(PeerState.ENABLED));
      }
      peerLock.unlock();
    }
  }
}
