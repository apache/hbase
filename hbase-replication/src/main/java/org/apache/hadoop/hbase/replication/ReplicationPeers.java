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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provides an class for maintaining a set of peer clusters. These peers are remote slave
 * clusters that data is replicated to.
 * <p>
 * We implement {@link ConfigurationObserver} mainly for recreating the
 * {@link ReplicationPeerStorage}, so we can change the {@link ReplicationPeerStorage} without
 * restarting the region server.
 */
@InterfaceAudience.Private
public class ReplicationPeers implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPeers.class);

  private volatile Configuration conf;

  // Map of peer clusters keyed by their id
  private final ConcurrentMap<String, ReplicationPeerImpl> peerCache;
  private final FileSystem fs;
  private final ZKWatcher zookeeper;
  private volatile ReplicationPeerStorage peerStorage;

  ReplicationPeers(FileSystem fs, ZKWatcher zookeeper, Configuration conf) {
    this.conf = conf;
    this.fs = fs;
    this.zookeeper = zookeeper;
    this.peerCache = new ConcurrentHashMap<>();
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(fs, zookeeper, conf);
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

  public ReplicationPeerImpl removePeer(String peerId) {
    return peerCache.remove(peerId);
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
    peer.setPeerState(peerStorage.isPeerEnabled(peerId));
    return peer.getPeerState();
  }

  public ReplicationPeerConfig refreshPeerConfig(String peerId) throws ReplicationException {
    ReplicationPeerImpl peer = peerCache.get(peerId);
    peer.setPeerConfig(peerStorage.getPeerConfig(peerId));
    return peer.getPeerConfig();
  }

  public SyncReplicationState refreshPeerNewSyncReplicationState(String peerId)
    throws ReplicationException {
    ReplicationPeerImpl peer = peerCache.get(peerId);
    SyncReplicationState newState = peerStorage.getPeerNewSyncReplicationState(peerId);
    peer.setNewSyncReplicationState(newState);
    return newState;
  }

  public void transitPeerSyncReplicationState(String peerId) {
    ReplicationPeerImpl peer = peerCache.get(peerId);
    peer.transitSyncReplicationState();
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
    SyncReplicationState newSyncReplicationState =
      peerStorage.getPeerNewSyncReplicationState(peerId);
    Configuration peerClusterConf;
    try {
      peerClusterConf = ReplicationPeerConfigUtil.getPeerClusterConfiguration(conf, peerConfig);
    } catch (IOException e) {
      throw new ReplicationException(
        "failed to apply cluster key to configuration for peer config " + peerConfig, e);
    }
    return new ReplicationPeerImpl(peerClusterConf, peerId, peerConfig, enabled,
      syncReplicationState, newSyncReplicationState);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = conf;
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(fs, zookeeper, conf);
    for (ReplicationPeerImpl peer : peerCache.values()) {
      try {
        peer.onConfigurationChange(
          ReplicationPeerConfigUtil.getPeerClusterConfiguration(conf, peer.getPeerConfig()));
      } catch (IOException e) {
        LOG.warn("failed to reload configuration for peer {}", peer.getId(), e);
      }
    }
  }
}
