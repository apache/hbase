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
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PeerProcedureHandlerImpl implements PeerProcedureHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PeerProcedureHandlerImpl.class);

  private final ReplicationSourceManager replicationSourceManager;
  private final PeerActionListener peerActionListener;
  private final KeyLocker<String> peersLock = new KeyLocker<>();

  public PeerProcedureHandlerImpl(ReplicationSourceManager replicationSourceManager,
      PeerActionListener peerActionListener) {
    this.replicationSourceManager = replicationSourceManager;
    this.peerActionListener = peerActionListener;
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
      PeerState newState = replicationSourceManager.getReplicationPeers().refreshPeerState(peerId);
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

  @Override
  public void transitSyncReplicationPeerState(String peerId, int stage, HRegionServer rs)
      throws ReplicationException, IOException {
    ReplicationPeers replicationPeers = replicationSourceManager.getReplicationPeers();
    Lock peerLock = peersLock.acquireLock(peerId);
    try {
      ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
      if (peer == null) {
        throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
      }
      if (!peer.getPeerConfig().isSyncReplication()) {
        throw new ReplicationException("Peer with id=" + peerId + " is not synchronous.");
      }
      SyncReplicationState newState = peer.getNewSyncReplicationState();
      if (stage == 0) {
        if (newState != SyncReplicationState.NONE) {
          LOG.warn("The new sync replication state for peer {} has already been set to {}, " +
            "this should be a retry, give up", peerId, newState);
          return;
        }
        newState = replicationPeers.refreshPeerNewSyncReplicationState(peerId);
        SyncReplicationState oldState = peer.getSyncReplicationState();
        peerActionListener.peerSyncReplicationStateChange(peerId, oldState, newState, stage);
      } else {
        if (newState == SyncReplicationState.NONE) {
          LOG.warn("The new sync replication state for peer {} has already been clear, and the " +
            "current state is {}, this should be a retry, give up", peerId, newState);
          return;
        }
        SyncReplicationState oldState = peer.getSyncReplicationState();
        peerActionListener.peerSyncReplicationStateChange(peerId, oldState, newState, stage);
        peer.transitSyncReplicationState();
      }
    } finally {
      peerLock.unlock();
    }
  }
}
