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
import java.util.Set;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * This provides an interface for clients of replication to view replication queues. These queues
 * keep track of the sources(WALs/HFile references) that still need to be replicated to remote
 * clusters.
 */
@InterfaceAudience.Private
public interface ReplicationQueuesClient {

  /**
   * Initialize the replication queue client interface.
   */
  public void init() throws ReplicationException;

  /**
   * Get a list of all region servers that have outstanding replication queues. These servers could
   * be alive, dead or from a previous run of the cluster.
   * @return a list of server names
   * @throws KeeperException zookeeper exception
   */
  List<String> getListOfReplicators() throws KeeperException;

  /**
   * Get a list of all WALs in the given queue on the given region server.
   * @param serverName the server name of the region server that owns the queue
   * @param queueId a String that identifies the queue
   * @return a list of WALs, null if this region server is dead and has no outstanding queues
   * @throws KeeperException zookeeper exception
   */
  List<String> getLogsInQueue(String serverName, String queueId) throws KeeperException;

  /**
   * Get a list of all queues for the specified region server.
   * @param serverName the server name of the region server that owns the set of queues
   * @return a list of queueIds, null if this region server is not a replicator.
   */
  List<String> getAllQueues(String serverName) throws KeeperException;

  /**
   * Load all wals in all replication queues from ZK. This method guarantees to return a
   * snapshot which contains all WALs in the zookeeper at the start of this call even there
   * is concurrent queue failover. However, some newly created WALs during the call may
   * not be included.
   */
   Set<String> getAllWALs() throws KeeperException;

  /**
   * Get the change version number of replication hfile references node. This can be used as
   * optimistic locking to get a consistent snapshot of the replication queues of hfile references.
   * @return change version number of hfile references node
   */
  int getHFileRefsNodeChangeVersion() throws KeeperException;

  /**
   * Get list of all peers from hfile reference queue.
   * @return a list of peer ids
   * @throws KeeperException zookeeper exception
   */
  List<String> getAllPeersFromHFileRefsQueue() throws KeeperException;

  /**
   * Get a list of all hfile references in the given peer.
   * @param peerId a String that identifies the peer
   * @return a list of hfile references, null if not found any
   * @throws KeeperException zookeeper exception
   */
  List<String> getReplicableHFiles(String peerId) throws KeeperException;
}
