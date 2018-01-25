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
package org.apache.hadoop.hbase.replication;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This provides an class for maintaining a set of peer clusters. These peers are remote slave
 * clusters that data is replicated to.
 */
@InterfaceAudience.Private
public class ReplicationPeers {

  private final Configuration conf;

  // Map of peer clusters keyed by their id
  private final ConcurrentMap<String, ReplicationPeerImpl> peerCache;
  private final ReplicationPeerStorage peerStorage;

  ReplicationPeers(ZKWatcher zookeeper, Configuration conf) {
    this.conf = conf;
    this.peerCache = new ConcurrentHashMap<>();
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(zookeeper, conf);
  }

  public Configuration getConf() {
    return conf;
  }

  public void init() throws ReplicationException {
    // Loading all existing peerIds into peer cache.
    for (String peerId : this.peerStorage.listPeerIds()) {
      addPeer(peerId);
    }
  }

  @VisibleForTesting
  public ReplicationPeerStorage getPeerStorage() {
    return this.peerStorage;
  }

  /**
   * Method called after a peer has been connected. It will create a ReplicationPeer to track the
   * newly connected cluster.
   * @param peerId a short that identifies the cluster
   * @return whether a ReplicationPeer was successfully created
   * @throws ReplicationException if connecting to the peer fails
   */
  public boolean addPeer(String peerId) throws ReplicationException {
    if (this.peerCache.containsKey(peerId)) {
      return false;
    }

    peerCache.put(peerId, createPeer(peerId));
    return true;
  }

  public void removePeer(String peerId) {
    peerCache.remove(peerId);
  }

  /**
   * Returns the ReplicationPeerImpl for the specified cached peer. This ReplicationPeer will
   * continue to track changes to the Peer's state and config. This method returns null if no peer
   * has been cached with the given peerId.
   * @param peerId id for the peer
   * @return ReplicationPeer object
   */
  public ReplicationPeerImpl getPeer(String peerId) {
    return peerCache.get(peerId);
  }

  /**
   * Returns the set of peerIds of the clusters that have been connected and have an underlying
   * ReplicationPeer.
   * @return a Set of Strings for peerIds
   */
  public Set<String> getAllPeerIds() {
    return Collections.unmodifiableSet(peerCache.keySet());
  }

  public Map<String, ReplicationPeerImpl> getPeerCache() {
    return Collections.unmodifiableMap(peerCache);
  }

  public PeerState refreshPeerState(String peerId) throws ReplicationException {
    ReplicationPeerImpl peer = peerCache.get(peerId);
    if (peer == null) {
      throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
    }
    peer.setPeerState(peerStorage.isPeerEnabled(peerId));
    return peer.getPeerState();
  }

  public ReplicationPeerConfig refreshPeerConfig(String peerId) throws ReplicationException {
    ReplicationPeerImpl peer = peerCache.get(peerId);
    if (peer == null) {
      throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
    }
    peer.setPeerConfig(peerStorage.getPeerConfig(peerId));
    return peer.getPeerConfig();
  }

  /**
   * Helper method to connect to a peer
   * @param peerId peer's identifier
   * @return object representing the peer
   */
  private ReplicationPeerImpl createPeer(String peerId) throws ReplicationException {
    ReplicationPeerConfig peerConfig = peerStorage.getPeerConfig(peerId);
    boolean enabled = peerStorage.isPeerEnabled(peerId);
    SyncReplicationState syncReplicationState = peerStorage.getPeerSyncReplicationState(peerId);
    return new ReplicationPeerImpl(ReplicationUtils.getPeerClusterConfiguration(peerConfig, conf),
        peerId, peerConfig, enabled, syncReplicationState);
  }
}
