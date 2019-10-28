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
package org.apache.hadoop.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Defines a curated set of shared functions implemented by HBase servers (Masters
 * and RegionServers). For use internally only. Be judicious adding API. Changes cause ripples
 * through the code base.
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
  ZKWatcher getZooKeeper();

  /**
   * Returns a reference to the servers' connection.
   *
   * Important note: this method returns a reference to Connection which is managed
   * by Server itself, so callers must NOT attempt to close connection obtained.
   */
  Connection getConnection();

  Connection createConnection(Configuration conf) throws IOException;

  /**
   * Returns a reference to the servers' cluster connection. Prefer {@link #getConnection()}.
   *
   * Important note: this method returns a reference to Connection which is managed
   * by Server itself, so callers must NOT attempt to close connection obtained.
   */
  ClusterConnection getClusterConnection();

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

  /**
   * @return Return the FileSystem object used (can return null!).
   */
  // TODO: Distinguish between "dataFs" and "walFs".
  default FileSystem getFileSystem() {
    // This default is pretty dodgy!
    Configuration c = getConfiguration();
    FileSystem fs = null;
    try {
      if (c != null) {
        fs = FileSystem.get(c);
      }
    } catch (IOException e) {
      // If an exception, just return null
    }
    return fs;
  };

  /**
   * @return True is the server is Stopping
   */
  // Note: This method is not part of the Stoppable Interface.
  default boolean isStopping() {
    return false;
  }
}
