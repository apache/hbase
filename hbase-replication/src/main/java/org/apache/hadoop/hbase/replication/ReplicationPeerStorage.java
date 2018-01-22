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

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Perform read/write to the replication peer storage.
 */
@InterfaceAudience.Private
public interface ReplicationPeerStorage {

  /**
   * Add a replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled,
      SyncReplicationState syncReplicationState) throws ReplicationException;

  /**
   * Remove a replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  void removePeer(String peerId) throws ReplicationException;

  /**
   * Set the state of peer, {@code true} to {@code ENABLED}, otherwise to {@code DISABLED}.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  void setPeerState(String peerId, boolean enabled) throws ReplicationException;

  /**
   * Update the config a replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException;

  /**
   * Return the peer ids of all replication peers.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  List<String> listPeerIds() throws ReplicationException;

  /**
   * Test whether a replication peer is enabled.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  boolean isPeerEnabled(String peerId) throws ReplicationException;

  /**
   * Get the peer config of a replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException;

  /**
   * Set the state of current cluster in a synchronous replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  void setPeerSyncReplicationState(String peerId, SyncReplicationState state)
      throws ReplicationException;

  /**
   * Get the state of current cluster in a synchronous replication peer.
   * @throws ReplicationException if there are errors accessing the storage service.
   */
  SyncReplicationState getPeerSyncReplicationState(String peerId)
      throws ReplicationException;
}
