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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Perform read/write to the replication queue storage.
 */
@InterfaceAudience.Private
public interface ReplicationQueueStorage {

  /**
   * Set the current offset for a specific WAL group in a given queue.
   * @param queueId    the id of the queue
   * @param walGroup   the group of the WAL, can be empty if multi wal is not enabled
   * @param offset     the current offset of replication progress
   * @param lastSeqIds map with {encodedRegionName, sequenceId} pairs for serial replication.
   */
  void setOffset(ReplicationQueueId queueId, String walGroup, ReplicationGroupOffset offset,
    Map<String, Long> lastSeqIds) throws ReplicationException;

  /**
   * Get the current offset of all the WAL groups for a queue
   * @param queueId the id of the queue
   * @return a map of all offsets of the WAL groups. The key the is WAL group and the value is the
   *         position.
   */
  Map<String, ReplicationGroupOffset> getOffsets(ReplicationQueueId queueId)
    throws ReplicationException;

  /**
   * Get a list of all queues for the specific peer.
   * @param peerId the id of the peer
   * @return a list of queueIds
   */
  List<ReplicationQueueId> listAllQueueIds(String peerId) throws ReplicationException;

  /**
   * Get a list of all queues for the specific region server.
   * @param serverName the server name of the region server that owns the set of queues
   * @return a list of queueIds
   */
  List<ReplicationQueueId> listAllQueueIds(ServerName serverName) throws ReplicationException;

  /**
   * Get a list of all queues for the specific region server and the specific peer
   * @param peerId     the id of the peer
   * @param serverName the server name of the region server that owns the set of queues
   * @return a list of queueIds
   */
  List<ReplicationQueueId> listAllQueueIds(String peerId, ServerName serverName)
    throws ReplicationException;

  /**
   * Get a list of all queues and the offsets.
   */
  List<ReplicationQueueData> listAllQueues() throws ReplicationException;

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   */
  List<ServerName> listAllReplicators() throws ReplicationException;

  /**
   * Change ownership for the queue identified by queueId and belongs to a dead region server.
   * @param queueId          the id of the queue
   * @param targetServerName the name of the target region server
   * @return the new PeerId and A SortedSet of WALs in its queue
   */
  Map<String, ReplicationGroupOffset> claimQueue(ReplicationQueueId queueId,
    ServerName targetServerName) throws ReplicationException;

  /**
   * Remove a replication queue
   * @param queueId the id of the queue to remove
   */
  void removeQueue(ReplicationQueueId queueId) throws ReplicationException;

  /**
   * Remove all the replication queues for the given peer. Usually used when removing a peer.
   * @param peerId the id of the peer
   */
  void removeAllQueues(String peerId) throws ReplicationException;

  /**
   * Read the max sequence id of the specific region for a given peer. For serial replication, we
   * need the max sequenced id to decide whether we can push the next entries.
   * @param encodedRegionName the encoded region name
   * @param peerId            peer id
   * @return the max sequence id of the specific region for a given peer.
   */
  long getLastSequenceId(String encodedRegionName, String peerId) throws ReplicationException;

  /**
   * Set the max sequence id of a bunch of regions for a given peer. Will be called when setting up
   * a serial replication peer.
   * @param peerId     peer id
   * @param lastSeqIds map with {encodedRegionName, sequenceId} pairs for serial replication.
   */
  void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds) throws ReplicationException;

  /**
   * Remove all the max sequence id record for the given peer.
   * @param peerId peer id
   */
  void removeLastSequenceIds(String peerId) throws ReplicationException;

  /**
   * Remove the max sequence id record for the given peer and regions.
   * @param peerId             peer id
   * @param encodedRegionNames the encoded region names
   */
  void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
    throws ReplicationException;

  /**
   * Remove a peer from hfile reference queue.
   * @param peerId peer cluster id to be removed
   */
  void removePeerFromHFileRefs(String peerId) throws ReplicationException;

  /**
   * Add new hfile references to the queue.
   * @param peerId peer cluster id to which the hfiles need to be replicated
   * @param pairs  list of pairs of { HFile location in staging dir, HFile path in region dir which
   *               will be added in the queue }
   * @throws ReplicationException if fails to add a hfile reference
   */
  void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs) throws ReplicationException;

  /**
   * Remove hfile references from the queue.
   * @param peerId peer cluster id from which this hfile references needs to be removed
   * @param files  list of hfile references to be removed
   */
  void removeHFileRefs(String peerId, List<String> files) throws ReplicationException;

  /**
   * Get list of all peers from hfile reference queue.
   * @return a list of peer ids
   */
  List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException;

  /**
   * Get a list of all hfile references in the given peer.
   * @param peerId a String that identifies the peer
   * @return a list of hfile references
   */
  List<String> getReplicableHFiles(String peerId) throws ReplicationException;

  /**
   * Load all hfile references in all replication queues. This method guarantees to return a
   * snapshot which contains all hfile references at the start of this call. However, some newly
   * created hfile references during the call may not be included.
   */
  Set<String> getAllHFileRefs() throws ReplicationException;

  /**
   * Whether the replication queue table exists.
   * @return Whether the replication queue table exists
   */
  boolean hasData() throws ReplicationException;
}
