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
 * This is the interface for a Replication Tracker. A replication tracker provides the facility to
 * subscribe and track events that reflect a change in replication state. These events are used by
 * the ReplicationSourceManager to coordinate replication tasks such as addition/deletion of queues
 * and queue failover. These events are defined in the ReplicationListener interface. If a class
 * would like to listen to replication events it must implement the ReplicationListener interface
 * and register itself with a Replication Tracker.
 */
@InterfaceAudience.Private
public interface ReplicationTracker {

  /**
   * Register a replication listener to receive replication events.
   * @param listener
   */
  public void registerListener(ReplicationListener listener);

  public void removeListener(ReplicationListener listener);

  /**
   * Returns a list of other live region servers in the cluster.
   * @return List of region servers.
   */
  public List<String> getListOfRegionServers();
}
