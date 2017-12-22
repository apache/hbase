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
import java.util.Set;
import java.util.SortedSet;

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
   * Remove a replication queue for a given regionserver.
   * @param serverName the name of the regionserver
   * @param queueId a String that identifies the queue.
   */
  void removeQueue(ServerName serverName, String queueId) throws ReplicationException;

  /**
   * Add a new WAL file to the given queue for a given regionserver. If the queue does not exist it
   * is created.
   * @param serverName the name of the regionserver
   * @param queueId a String that identifies the queue.
   * @param fileName name of the WAL
   */
  void addWAL(ServerName serverName, String queueId, String fileName) throws ReplicationException;

  /**
   * Remove an WAL file from the given queue for a given regionserver.
   * @param serverName the name of the regionserver
   * @param queueId a String that identifies the queue.
   * @param fileName name of the WAL
   */
  void removeWAL(ServerName serverName, String queueId, String fileName)
      throws ReplicationException;

  /**
   * Set the current position for a specific WAL in a given queue for a given regionserver.
   * @param serverName the name of the regionserver
   * @param queueId a String that identifies the queue
   * @param fileName name of the WAL
   * @param position the current position in the file
   */
  void setWALPosition(ServerName serverName, String queueId, String fileName, long position)
      throws ReplicationException;

  /**
   * Get the current position for a specific WAL in a given queue for a given regionserver.
   * @param serverName the name of the regionserver
   * @param queueId a String that identifies the queue
   * @param fileName name of the WAL
   * @return the current position in the file
   */
  long getWALPosition(ServerName serverName, String queueId, String fileName)
      throws ReplicationException;

  /**
   * Get a list of all queues for the specified region server.
   * @param serverName the server name of the region server that owns the set of queues
   * @return a list of queueIds
   */
  List<String> getAllQueues(ServerName serverName) throws ReplicationException;

  /**
   * Change ownership for the queue identified by queueId and belongs to a dead region server.
   * @param sourceServerName the name of the dead region server
   * @param destServerName the name of the target region server
   * @param queueId the id of the queue
   * @return the new PeerId and A SortedSet of WALs in its queue
   */
  Pair<String, SortedSet<String>> claimQueue(ServerName sourceServerName, String queueId,
      ServerName destServerName) throws ReplicationException;

  /**
   * Remove the record of region server if the queue is empty.
   */
  void removeReplicatorIfQueueIsEmpty(ServerName serverName) throws ReplicationException;

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   */
  List<ServerName> getListOfReplicators() throws ReplicationException;

  /**
   * Load all wals in all replication queues. This method guarantees to return a snapshot which
   * contains all WALs in the zookeeper at the start of this call even there is concurrent queue
   * failover. However, some newly created WALs during the call may not be included.
   */
  Set<String> getAllWALs() throws ReplicationException;

  /**
   * Add a peer to hfile reference queue if peer does not exist.
   * @param peerId peer cluster id to be added
   * @throws ReplicationException if fails to add a peer id to hfile reference queue
   */
  void addPeerToHFileRefs(String peerId) throws ReplicationException;

  /**
   * Remove a peer from hfile reference queue.
   * @param peerId peer cluster id to be removed
   */
  void removePeerFromHFileRefs(String peerId) throws ReplicationException;

  /**
   * Add new hfile references to the queue.
   * @param peerId peer cluster id to which the hfiles need to be replicated
   * @param pairs list of pairs of { HFile location in staging dir, HFile path in region dir which
   *          will be added in the queue }
   * @throws ReplicationException if fails to add a hfile reference
   */
  void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs) throws ReplicationException;

  /**
   * Remove hfile references from the queue.
   * @param peerId peer cluster id from which this hfile references needs to be removed
   * @param files list of hfile references to be removed
   */
  void removeHFileRefs(String peerId, List<String> files) throws ReplicationException;

  /**
   * Get the change version number of replication hfile references node. This can be used as
   * optimistic locking to get a consistent snapshot of the replication queues of hfile references.
   * @return change version number of hfile references node
   */
  int getHFileRefsNodeChangeVersion() throws ReplicationException;

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
}
