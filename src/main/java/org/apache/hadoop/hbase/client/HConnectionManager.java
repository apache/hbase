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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

/**
 * A non-instantiable class that manages connections to multiple tables in
 * multiple HBase instances.
 *
 * Used by {@link HTable} and {@link HBaseAdmin}
 */
@SuppressWarnings("serial")
public class HConnectionManager {
  // Register a shutdown hook, one that cleans up RPC and closes zk sessions.
  public static final Thread shutdownHook = new Thread("HCM.shutdownHook") {
    @Override
    public void run() {
      HConnectionManager.deleteAllConnections();
    }
  };

  static {
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /*
   * Not instantiable.
   */
  protected HConnectionManager() {
    super();
  }

  private static final int MAX_CACHED_HBASE_INSTANCES = 31;
  // A LRU Map of master HBaseConfiguration -> connection information for that
  // instance. The objects it contains are mutable and hence require
  // synchronized access to them. We set instances to 31. The zk default max
  // connections is 30 so should run into zk issues before hit this value of 31.
  private static final Map<Integer, TableServers> HBASE_INSTANCES = new LinkedHashMap<Integer, TableServers>(
      (int) (MAX_CACHED_HBASE_INSTANCES / 0.75F) + 1, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer, TableServers> eldest) {
      return size() > MAX_CACHED_HBASE_INSTANCES;
    }
  };

  private static final Map<String, ClientZKConnection> ZK_WRAPPERS = new HashMap<String, ClientZKConnection>();

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   *
   * @param conf
   *          configuration
   * @return HConnection object for the instance specified by the configuration
   */
  public static HConnection getConnection(Configuration conf) {
    TableServers connection;
    Integer key = HBaseConfiguration.hashCode(conf);
    synchronized (HBASE_INSTANCES) {
      connection = HBASE_INSTANCES.get(key);
      if (connection == null) {
        connection = new TableServers(conf);
        HBASE_INSTANCES.put(key, connection);
      }
    }
    return connection;
  }

  /**
   * Delete connection information for the instance specified by configuration
   *
   * @param conf
   *          configuration
   * @param stopProxy
   *          stop the proxy as well
   */
  public static void deleteConnectionInfo(Configuration conf, boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      Integer key = HBaseConfiguration.hashCode(conf);
      TableServers t = HBASE_INSTANCES.remove(key);
      if (t != null) {
        t.close();
      }
    }
  }

  /**
   * Delete information for all connections.
   */
  public static void deleteAllConnections() {
    synchronized (HBASE_INSTANCES) {
      for (TableServers t : HBASE_INSTANCES.values()) {
        if (t != null) {
          t.close();
        }
      }
      HBaseRPC.stopClients();
    }
    deleteAllZookeeperConnections();
    try {
      HBaseThriftRPC.clearAll();
    } catch (Exception e) {
      // Exiting
    }
  }

  /**
   * Delete information for all zookeeper connections.
   */
  public static void deleteAllZookeeperConnections() {
    synchronized (ZK_WRAPPERS) {
      for (ClientZKConnection connection : ZK_WRAPPERS.values()) {
        connection.closeZooKeeperConnection();
      }
      ZK_WRAPPERS.clear();
    }
  }

  /**
   * Get a watcher of a zookeeper connection for a given quorum address.
   * If the connection isn't established, a new one is created.
   * This acts like a multiton.
   * @param conf configuration
   * @return zkConnection ClientZKConnection
   * @throws IOException if a remote or network exception occurs
   */
  public static synchronized ClientZKConnection getClientZKConnection(
      Configuration conf) throws IOException {
    if (!ZK_WRAPPERS.containsKey(
        ZooKeeperWrapper.getZookeeperClusterKey(conf))) {
      ZK_WRAPPERS.put(ZooKeeperWrapper.getZookeeperClusterKey(conf),
          new ClientZKConnection(conf));
    }
    return ZK_WRAPPERS.get(ZooKeeperWrapper.getZookeeperClusterKey(conf));
  }

  /**
   * It is provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   *
   * @return Number of cached regions for the table.
   */
  static int getCachedRegionCount(Configuration conf, byte[] tableName) {
    TableServers connection = (TableServers) getConnection(conf);
    return connection.metaCache.getNumber(tableName);
  }

  /**
   * It's provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   *
   * @return true if the region where the table and row reside is cached.
   */
  static boolean isRegionCached(Configuration conf, byte[] tableName,
      byte[] row) {
    TableServers connection = (TableServers) getConnection(conf);
    return connection.metaCache.getForRow(tableName, row) != null;
  }
}
