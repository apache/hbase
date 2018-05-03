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
import java.io.InterruptedIOException;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.LogRoller;
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
      SyncReplicationState newSyncReplicationState = peer.getNewSyncReplicationState();
      if (stage == 0) {
        if (newSyncReplicationState != SyncReplicationState.NONE) {
          LOG.warn("The new sync replication state for peer {} has already been set to {}, " +
            "this should be a retry, give up", peerId, newSyncReplicationState);
          return;
        }
        // refresh the peer state first, as when we transit to STANDBY, we may need to disable the
        // peer before processing the sync replication state.
        PeerState oldState = peer.getPeerState();
        boolean success = false;
        try {
          PeerState newState = replicationPeers.refreshPeerState(peerId);
          if (oldState.equals(PeerState.ENABLED) && newState.equals(PeerState.DISABLED)) {
            replicationSourceManager.refreshSources(peerId);
          }
          success = true;
        } finally {
          if (!success) {
            peer.setPeerState(oldState.equals(PeerState.ENABLED));
          }
        }
        newSyncReplicationState = replicationPeers.refreshPeerNewSyncReplicationState(peerId);
        SyncReplicationState oldSyncReplicationState = peer.getSyncReplicationState();
        peerActionListener.peerSyncReplicationStateChange(peerId, oldSyncReplicationState,
          newSyncReplicationState, stage);
      } else {
        if (newSyncReplicationState == SyncReplicationState.NONE) {
          LOG.warn(
            "The new sync replication state for peer {} has already been clear, and the " +
              "current state is {}, this should be a retry, give up",
            peerId, newSyncReplicationState);
          return;
        }
        if (newSyncReplicationState == SyncReplicationState.STANDBY) {
          replicationSourceManager.drainSources(peerId);
          // Need to roll the wals and make the ReplicationSource for this peer track the new file.
          // If we do not do this, there will be two problems that can not be addressed at the same
          // time. First, if we just throw away the current wal file, and later when we transit the
          // peer to DA, and the wal has not been rolled yet, then the new data written to the wal
          // file will not be replicated and cause data inconsistency. But if we just track the
          // current wal file without rolling, it may contains some data before we transit the peer
          // to S, later if we transit the peer to DA, the data will also be replicated and cause
          // data inconsistency. So here we need to roll the wal, and let the ReplicationSource
          // track the new wal file, and throw the old wal files away.
          LogRoller roller = rs.getWalRoller();
          roller.requestRollAll();
          try {
            roller.waitUntilWalRollFinished();
          } catch (InterruptedException e) {
            // reset the interrupted flag
            Thread.currentThread().interrupt();
            throw (IOException) new InterruptedIOException(
              "Interrupted while waiting for wal roll finish").initCause(e);
          }
        }
        SyncReplicationState oldState = peer.getSyncReplicationState();
        peerActionListener.peerSyncReplicationStateChange(peerId, oldState, newSyncReplicationState,
          stage);
        peer.transitSyncReplicationState();
      }
    } finally {
      peerLock.unlock();
    }
  }
}
