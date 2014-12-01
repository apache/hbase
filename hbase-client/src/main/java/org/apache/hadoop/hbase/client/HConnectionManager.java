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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

/**
 * A non-instantiable class that manages creation of {@link HConnection}s.
 * <p>The simplest way to use this class is by using {@link #createConnection(Configuration)}.
 * This creates a new {@link HConnection} to the cluster that is managed by the caller.
 * From this {@link HConnection} {@link HTableInterface} implementations are retrieved
 * with {@link HConnection#getTable(byte[])}. Example:
 * <pre>
 * HConnection connection = HConnectionManager.createConnection(config);
 * HTableInterface table = connection.getTable(TableName.valueOf("table1"));
 * try {
 *   // Use the table as needed, for a single operation and a single thread
 * } finally {
 *   table.close();
 *   connection.close();
 * }
 * </pre>
 * <p>This class has a static Map of {@link HConnection} instances keyed by
 * {@link HConnectionKey}; A {@link HConnectionKey} is identified by a set of
 * {@link Configuration} properties. Invocations of {@link #getConnection(Configuration)}
 * that pass the same {@link Configuration} instance will return the same
 * {@link  HConnection} instance ONLY WHEN the set of properties are the same
 * (i.e. if you change properties in your {@link Configuration} instance, such as RPC timeout,
 * the codec used, HBase will create a new {@link HConnection} instance. For more details on
 * how this is done see {@link HConnectionKey}).
 * <p>Sharing {@link HConnection} instances is usually what you want; all clients
 * of the {@link HConnection} instances share the HConnections' cache of Region
 * locations rather than each having to discover for itself the location of meta, etc.
 * But sharing connections makes clean up of {@link HConnection} instances a little awkward.
 * Currently, clients cleanup by calling {@link #deleteConnection(Configuration)}. This will
 * shutdown the zookeeper connection the HConnection was using and clean up all
 * HConnection resources as well as stopping proxies to servers out on the
 * cluster. Not running the cleanup will not end the world; it'll
 * just stall the closeup some and spew some zookeeper connection failed
 * messages into the log.  Running the cleanup on a {@link HConnection} that is
 * subsequently used by another will cause breakage so be careful running
 * cleanup.
 * <p>To create a {@link HConnection} that is not shared by others, you can
 * set property "hbase.client.instance.id" to a unique value for your {@link Configuration}
 * instance, like the following:
 * <pre>
 * {@code
 * conf.set("hbase.client.instance.id", "12345");
 * HConnection connection = HConnectionManager.getConnection(conf);
 * // Use the connection to your hearts' delight and then when done...
 * conf.set("hbase.client.instance.id", "12345");
 * HConnectionManager.deleteConnection(conf, true);
 * }
 * </pre>
 * <p>Cleanup used to be done inside in a shutdown hook.  On startup we'd
 * register a shutdown hook that called {@link #deleteAllConnections()}
 * on its way out but the order in which shutdown hooks run is not defined so
 * were problematic for clients of HConnection that wanted to register their
 * own shutdown hooks so we removed ours though this shifts the onus for
 * cleanup to the client.
 * @deprecated Please use ConnectionFactory instead
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Deprecated
public class HConnectionManager extends ConnectionFactory {

  @Deprecated
  public static final String RETRIES_BY_SERVER_KEY =
      ConnectionManager.RETRIES_BY_SERVER_KEY;

  @Deprecated
  public static final int MAX_CACHED_CONNECTION_INSTANCES =
      ConnectionManager.MAX_CACHED_CONNECTION_INSTANCES;

  /*
   * Non-instantiable.
   */
  private HConnectionManager() {
    super();
  }

  /**
   * Get the connection that goes with the passed <code>conf</code> configuration instance.
   * If no current connection exists, method creates a new connection and keys it using
   * connection-specific properties from the passed {@link Configuration}; see
   * {@link HConnectionKey}.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection getConnection(final Configuration conf) throws IOException {
    return ConnectionManager.getConnectionInternal(conf);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   *
   * This is the recommended way to create HConnections.
   * <pre>
   * HConnection connection = HConnectionManager.createConnection(conf);
   * HTableInterface table = connection.getTable("mytable");
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection createConnection(Configuration conf) throws IOException {
    return ConnectionManager.createConnectionInternal(conf);
  }


  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * <pre>
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * </pre>
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection createConnection(Configuration conf, ExecutorService pool)
      throws IOException {
    return ConnectionManager.createConnection(conf, pool);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * <pre>
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * </pre>
   * @param conf configuration
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection createConnection(Configuration conf, User user)
  throws IOException {
    return ConnectionManager.createConnection(conf, user);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * <pre>
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * </pre>
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection createConnection(Configuration conf, ExecutorService pool, User user)
  throws IOException {
    return ConnectionManager.createConnection(conf, pool, user);
  }

  @Deprecated
  static HConnection createConnection(final Configuration conf, final boolean managed)
      throws IOException {
    return ConnectionManager.createConnection(conf, managed);
  }

  @Deprecated
  static ClusterConnection createConnection(final Configuration conf, final boolean managed,
      final ExecutorService pool, final User user) throws IOException {
    return ConnectionManager.createConnection(conf, managed, pool, user);
  }

  /**
   * Delete connection information for the instance specified by passed configuration.
   * If there are no more references to the designated connection connection, this method will
   * then close connection to the zookeeper ensemble and let go of all associated resources.
   *
   * @param conf configuration whose identity is used to find {@link HConnection} instance.
   * @deprecated
   */
  @Deprecated
  public static void deleteConnection(Configuration conf) {
    ConnectionManager.deleteConnection(conf);
  }

  /**
   * Cleanup a known stale connection.
   * This will then close connection to the zookeeper ensemble and let go of all resources.
   *
   * @param connection
   * @deprecated
   */
  @Deprecated
  public static void deleteStaleConnection(HConnection connection) {
    ConnectionManager.deleteStaleConnection(connection);
  }

  /**
   * Delete information for all connections. Close or not the connection, depending on the
   *  staleConnection boolean and the ref count. By default, you should use it with
   *  staleConnection to true.
   * @deprecated
   */
  @Deprecated
  public static void deleteAllConnections(boolean staleConnection) {
    ConnectionManager.deleteAllConnections(staleConnection);
  }

  /**
   * Delete information for all connections..
   * @deprecated kept for backward compatibility, but the behavior is broken. HBASE-8983
   */
  @Deprecated
  public static void deleteAllConnections() {
    ConnectionManager.deleteAllConnections();
  }

  /**
   * This convenience method invokes the given {@link HConnectable#connect}
   * implementation using a {@link HConnection} instance that lasts just for the
   * duration of the invocation.
   *
   * @param <T> the return type of the connect method
   * @param connectable the {@link HConnectable} instance
   * @return the value returned by the connect method
   * @throws IOException
   * @deprecated Internal method, do not use thru HConnectionManager.
   */
  @InterfaceAudience.Private
  @Deprecated
  public static <T> T execute(HConnectable<T> connectable) throws IOException {
    return ConnectionManager.execute(connectable);
  }

  /**
   * Set the number of retries to use serverside when trying to communicate
   * with another server over {@link HConnection}.  Used updating catalog
   * tables, etc.  Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   * @deprecated Internal method, do not use.
   */
  @InterfaceAudience.Private
  @Deprecated
  public static void setServerSideHConnectionRetries(
      final Configuration c, final String sn, final Log log) {
    ConnectionUtils.setServerSideHConnectionRetriesConfig(c, sn, log);
  }
}
