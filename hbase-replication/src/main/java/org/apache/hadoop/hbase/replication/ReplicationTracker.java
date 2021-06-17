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
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the interface for a Replication Tracker.
 * <p/>
 * A replication tracker provides the facility to subscribe and track events that reflect a change
 * in replication state. These events are used by the ReplicationSourceManager to coordinate
 * replication tasks such as addition/deletion of queues and queue failover. These events are
 * defined in the ReplicationListener interface. If a class would like to listen to replication
 * events it must implement the ReplicationListener interface and register itself with a Replication
 * Tracker.
 */
@InterfaceAudience.Private
public interface ReplicationTracker {

  /**
   * Register a replication listener to receive replication events.
   * @param listener
   */
  void registerListener(ReplicationListener listener);

  /**
   * Remove a replication listener
   * @param listener the listener to remove
   */
  void removeListener(ReplicationListener listener);

  /**
   * In this method, you need to load the newest list of region server list and return it, and all
   * later changes to the region server list must be passed to the listeners.
   * <p/>
   * This is very important for us to not miss a region server crash.
   * <p/>
   * Notice that this method can only be called once.
   * @return Set of region servers.
   */
  Set<ServerName> loadLiveRegionServersAndInitializeListeners() throws IOException;
}
