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

import org.apache.hadoop.hbase.classification.InterfaceAudience;


/**
 * This provides an interface for clients of replication to view replication queues. These queues
 * keep track of the WALs that still need to be replicated to remote clusters.
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
   */
  List<String> getListOfReplicators();

  /**
   * Get a list of all WALs in the given queue on the given region server.
   * @param serverName the server name of the region server that owns the queue
   * @param queueId a String that identifies the queue
   * @return a list of WALs, null if this region server is dead and has no outstanding queues
   */
  List<String> getLogsInQueue(String serverName, String queueId);

  /**
   * Get a list of all queues for the specified region server.
   * @param serverName the server name of the region server that owns the set of queues
   * @return a list of queueIds, null if this region server is not a replicator.
   */
  List<String> getAllQueues(String serverName);
}
