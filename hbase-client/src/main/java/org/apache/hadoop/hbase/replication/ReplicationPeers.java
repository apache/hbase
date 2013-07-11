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
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.zookeeper.KeeperException;

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
   * @throws KeeperException
   */
  void init() throws IOException, KeeperException;

  /**
   * Add a new remote slave cluster for replication.
   * @param peerId a short that identifies the cluster
   * @param clusterKey the concatenation of the slave cluster's:
   *          hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
   */
  void addPeer(String peerId, String clusterKey) throws IOException;

  /**
   * Removes a remote slave cluster and stops the replication to it.
   * @param peerId a short that identifies the cluster
   */
  void removePeer(String peerId) throws IOException;

  /**
   * Restart the replication to the specified remote slave cluster.
   * @param peerId a short that identifies the cluster
   */
  void enablePeer(String peerId) throws IOException;

  /**
   * Stop the replication to the specified remote slave cluster.
   * @param peerId a short that identifies the cluster
   */
  void disablePeer(String peerId) throws IOException;

  /**
   * Get the replication status for the specified connected remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return true if replication is enabled, false otherwise.
   */
  boolean getStatusOfConnectedPeer(String peerId);

  /**
   * Get a set of all connected remote slave clusters.
   * @return set of peer ids
   */
  Set<String> getConnectedPeers();

  /**
   * List the cluster keys of all remote slave clusters (whether they are enabled/disabled or
   * connected/disconnected).
   * @return A map of peer ids to peer cluster keys
   */
  Map<String, String> getAllPeerClusterKeys();

  /**
   * List the peer ids of all remote slave clusters (whether they are enabled/disabled or
   * connected/disconnected).
   * @return A list of peer ids
   */
  List<String> getAllPeerIds();

  /**
   * Attempt to connect to a new remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return true if a new connection was made, false if no new connection was made.
   */
  boolean connectToPeer(String peerId) throws IOException, KeeperException;

  /**
   * Disconnect from a remote slave cluster.
   * @param peerId a short that identifies the cluster
   */
  void disconnectFromPeer(String peerId);

  /**
   * Returns all region servers from given connected remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return addresses of all region servers in the peer cluster. Returns an empty list if the peer
   *         cluster is unavailable or there are no region servers in the cluster.
   */
  List<ServerName> getRegionServersOfConnectedPeer(String peerId);

  /**
   * Returns the UUID of the provided peer id.
   * @param peerId the peer's ID that will be converted into a UUID
   * @return a UUID or null if the peer cluster does not exist or is not connected.
   */
  UUID getPeerUUID(String peerId);

  /**
   * Returns the configuration needed to talk to the remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return the configuration for the peer cluster, null if it was unable to get the configuration
   */
  Configuration getPeerConf(String peerId) throws KeeperException;
}
