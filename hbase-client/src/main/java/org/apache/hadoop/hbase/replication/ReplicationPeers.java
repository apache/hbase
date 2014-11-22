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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This provides an interface for maintaining a set of peer clusters. These peers are remote slave
 * clusters that data is replicated to. A peer cluster can be in three different states:
 *
 * 1. Not-Registered - There is no notion of the peer cluster.
 * 2. Registered - The peer has an id and is being tracked but there is no connection.
 * 3. Connected - There is an active connection to the remote peer.
 *
 * In the registered or connected state, a peer cluster can either be enabled or disabled.
 */
@InterfaceAudience.Private
public interface ReplicationPeers {

  /**
   * Initialize the ReplicationPeers interface.
   */
  void init() throws ReplicationException;

  /**
   * Add a new remote slave cluster for replication.
   * @param peerId a short that identifies the cluster
   * @param peerConfig configuration for the replication slave cluster
   * @param tableCFs the table and column-family list which will be replicated for this peer or null
   * for all table and column families
   */
  void addPeer(String peerId, ReplicationPeerConfig peerConfig, String tableCFs)
      throws ReplicationException;

  /**
   * Removes a remote slave cluster and stops the replication to it.
   * @param peerId a short that identifies the cluster
   */
  void removePeer(String peerId) throws ReplicationException;

  boolean peerAdded(String peerId) throws ReplicationException;

  void peerRemoved(String peerId);

  /**
   * Restart the replication to the specified remote slave cluster.
   * @param peerId a short that identifies the cluster
   */
  void enablePeer(String peerId) throws ReplicationException;

  /**
   * Stop the replication to the specified remote slave cluster.
   * @param peerId a short that identifies the cluster
   */
  void disablePeer(String peerId) throws ReplicationException;

  /**
   * Get the table and column-family list string of the peer from ZK.
   * @param peerId a short that identifies the cluster
   */
  public String getPeerTableCFsConfig(String peerId) throws ReplicationException;

  /**
   * Set the table and column-family list string of the peer to ZK.
   * @param peerId a short that identifies the cluster
   * @param tableCFs the table and column-family list which will be replicated for this peer
   */
  public void setPeerTableCFsConfig(String peerId, String tableCFs) throws ReplicationException;

  /**
   * Get the table and column-family-list map of the peer.
   * @param peerId a short that identifies the cluster
   * @return the table and column-family list which will be replicated for this peer
   */
  public Map<String, List<String>> getTableCFs(String peerId);

  /**
   * Returns the ReplicationPeer
   * @param peerId id for the peer
   * @return ReplicationPeer object
   */
  ReplicationPeer getPeer(String peerId);

  /**
   * Returns the set of peerIds defined
   * @return a Set of Strings for peerIds
   */
  public Set<String> getPeerIds();

  /**
   * Get the replication status for the specified connected remote slave cluster.
   * The value might be read from cache, so it is recommended to
   * use {@link #getStatusOfPeerFromBackingStore(String)}
   * if reading the state after enabling or disabling it.
   * @param peerId a short that identifies the cluster
   * @return true if replication is enabled, false otherwise.
   */
  boolean getStatusOfPeer(String peerId);

  /**
   * Get the replication status for the specified remote slave cluster, which doesn't
   * have to be connected. The state is read directly from the backing store.
   * @param peerId a short that identifies the cluster
   * @return true if replication is enabled, false otherwise.
   * @throws IOException Throws if there's an error contacting the store
   */
  boolean getStatusOfPeerFromBackingStore(String peerId) throws ReplicationException;

  /**
   * List the cluster replication configs of all remote slave clusters (whether they are
   * enabled/disabled or connected/disconnected).
   * @return A map of peer ids to peer cluster keys
   */
  Map<String, ReplicationPeerConfig> getAllPeerConfigs();

  /**
   * List the peer ids of all remote slave clusters (whether they are enabled/disabled or
   * connected/disconnected).
   * @return A list of peer ids
   */
  List<String> getAllPeerIds();

  /**
   * Returns the configured ReplicationPeerConfig for this peerId
   * @param peerId a short name that identifies the cluster
   * @return ReplicationPeerConfig for the peer
   */
  ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws ReplicationException;

  /**
   * Returns the configuration needed to talk to the remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return the configuration for the peer cluster, null if it was unable to get the configuration
   */
  Pair<ReplicationPeerConfig, Configuration> getPeerConf(String peerId) throws ReplicationException;
}
