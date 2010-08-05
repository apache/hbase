/**
 * Copyright 2010 The Apache Software Foundation
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
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Defines the set of shared functions implemented by all HBase servers (Masters
 * and RegionServers).
 */
public interface ServerController extends Abortable {
  /**
   * Returns the address of the current server.
   */
  public HServerAddress getHServerAddress();

  /**
   * Gets the configuration object for this server.
   */
  public Configuration getConfiguration();

  /**
   * Gets the ZooKeeper instance for this server.
   */
  public ZooKeeperWatcher getZooKeeper();

  /**
   * Gets the unique server name for this server.
   * @return unique server name
   */
  public String getServerName();

  /**
   * Return the server RPC connection object
   */
  public ServerConnection getServerConnection();

  /**
   * Returns the timeout to use for operations such as waiting on root and meta
   * availability.
   * @return default timeout to wait for root and meta
   */
  public long getTimeout();
}
