/**
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Defines the set of shared functions implemented by HBase servers (Masters
 * and RegionServers).
 */
@InterfaceAudience.Private
public interface Server extends Abortable, Stoppable {
  /**
   * Gets the configuration object for this server.
   */
  Configuration getConfiguration();

  /**
   * Gets the ZooKeeper instance for this server.
   */
  ZooKeeperWatcher getZooKeeper();

  /**
   * Returns a reference to the servers' cluster connection.
   *
   * Important note: this method returns a reference to Connection which is managed
   * by Server itself, so callers must NOT attempt to close connection obtained.
   */
  ClusterConnection getConnection();

  /**
   * Returns instance of {@link org.apache.hadoop.hbase.zookeeper.MetaTableLocator}
   * running inside this server. This MetaServerLocator is started and stopped by server, clients
   * shouldn't manage it's lifecycle.
   * @return instance of {@link MetaTableLocator} associated with this server.
   */
  MetaTableLocator getMetaTableLocator();

  /**
   * @return The unique server name for this server.
   */
  ServerName getServerName();

  /**
   * Get CoordinatedStateManager instance for this server.
   */
  CoordinatedStateManager getCoordinatedStateManager();

  /**
   * @return The {@link ChoreService} instance for this server
   */
  ChoreService getChoreService();
}