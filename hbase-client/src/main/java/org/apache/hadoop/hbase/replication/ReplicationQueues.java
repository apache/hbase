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

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This provides an interface for maintaining a region server's replication queues. These queues
 * keep track of the WALs and HFile references (if hbase.replication.bulkload.enabled is enabled)
 * that still need to be replicated to remote clusters.
 */
@InterfaceAudience.Private
public interface ReplicationQueues {

  /**
   * Initialize the region server replication queue interface.
   * @param serverName The server name of the region server that owns the replication queues this
   *          interface manages.
   */
  void init(String serverName) throws ReplicationException;

  /**
   * Remove a replication queue.
   * @param queueId a String that identifies the queue.
   */
  void removeQueue(String queueId);

  /**
   * Add a new WAL file to the given queue. If the queue does not exist it is created.
   * @param queueId a String that identifies the queue.
   * @param filename name of the WAL
   */
  void addLog(String queueId, String filename) throws ReplicationException;

  /**
   * Remove an WAL file from the given queue.
   * @param queueId a String that identifies the queue.
   * @param filename name of the WAL
   */
  void removeLog(String queueId, String filename);

  /**
   * Set the current position for a specific WAL in a given queue.
   * @param queueId a String that identifies the queue
   * @param filename name of the WAL
   * @param position the current position in the file
   */
  void setLogPosition(String queueId, String filename, long position);

  /**
   * Get the current position for a specific WAL in a given queue.
   * @param queueId a String that identifies the queue
   * @param filename name of the WAL
   * @return the current position in the file
   */
  long getLogPosition(String queueId, String filename) throws ReplicationException;

  /**
   * Remove all replication queues for this region server.
   */
  void removeAllQueues();

  /**
   * Get a list of all WALs in the given queue.
   * @param queueId a String that identifies the queue
   * @return a list of WALs, null if no such queue exists for this server
   */
  List<String> getLogsInQueue(String queueId);

  /**
   * Get a list of all queues for this region server.
   * @return a list of queueIds, an empty list if this region server is dead and has no outstanding queues
   */
  List<String> getAllQueues();

  /**
   * Take ownership for the set of queues belonging to a dead region server.
   * @param regionserver the id of the dead region server
   * @return A SortedMap of the queues that have been claimed, including a SortedSet of WALs in
   *         each queue. Returns an empty map if no queues were failed-over.
   */
  SortedMap<String, SortedSet<String>> claimQueues(String regionserver);

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   */
  List<String> getListOfReplicators();

  /**
   * Checks if the provided znode is the same as this region server's
   * @param regionserver the id of the region server
   * @return if this is this rs's znode
   */
  boolean isThisOurRegionServer(String regionserver);

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
  void removePeerFromHFileRefs(String peerId);

  /**
   * Add new hfile references to the queue.
   * @param peerId peer cluster id to which the hfiles need to be replicated
   * @param files list of hfile references to be added
   * @throws ReplicationException if fails to add a hfile reference
   */
  void addHFileRefs(String peerId, List<String> files) throws ReplicationException;

  /**
   * Remove hfile references from the queue.
   * @param peerId peer cluster id from which this hfile references needs to be removed
   * @param files list of hfile references to be removed
   */
  void removeHFileRefs(String peerId, List<String> files);
}
