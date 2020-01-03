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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A non-instantiable class that manages creation of {@link Connection}s. Managing the lifecycle of
 * the {@link Connection}s to the cluster is the responsibility of the caller. From a
 * {@link Connection}, {@link Table} implementations are retrieved with
 * {@link Connection#getTable(org.apache.hadoop.hbase.TableName)}. Example:
 *
 * <pre>
 * Connection connection = ConnectionFactory.createConnection(config);
 * Table table = connection.getTable(TableName.valueOf("table1"));
 * try {
 *   // Use the table as needed, for a single operation and a single thread
 * } finally {
 *   table.close();
 *   connection.close();
 * }
 * </pre>
 *
 * Since 2.2.0, Connection created by ConnectionFactory can contain user-specified kerberos
 * credentials if caller has following two configurations set:
 * <ul>
 *   <li>hbase.client.keytab.file, points to a valid keytab on the local filesystem
 *   <li>hbase.client.kerberos.principal, gives the Kerberos principal to use
 * </ul>
 * By this way, caller can directly connect to kerberized cluster without caring login and
 * credentials renewal logic in application.
 * <pre>
 * </pre>
 * Similarly, {@link Connection} also returns {@link Admin} and {@link RegionLocator}
 * implementations.
 * @see Connection
 * @since 0.99.0
 */
@InterfaceAudience.Public
public class ConnectionFactory {

  public static final String HBASE_CLIENT_ASYNC_CONNECTION_IMPL = "hbase.client.async.connection.impl";

  /** No public c.tors */
  protected ConnectionFactory() {
  }

  /**
   * Create a new Connection instance using default HBaseConfiguration. Connection encapsulates all
   * housekeeping for a connection to the cluster. All tables and interfaces created from returned
   * connection share zookeeper connection, meta cache, and connections to region servers and
   * masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection();
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    return createConnection(conf, null, AuthUtil.loginClient(conf));
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections to
   * region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
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
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf) throws IOException {
    return createConnection(conf, null, AuthUtil.loginClient(conf));
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections to
   * region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
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
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool)
      throws IOException {
    return createConnection(conf, pool, AuthUtil.loginClient(conf));
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections to
   * region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
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
   * @param user the user the connection is for
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, User user) throws IOException {
    return createConnection(conf, null, user);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections to
   * region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
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
   * @param user the user the connection is for
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool,
    final User user) throws IOException {
    String className = conf.get(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
      ConnectionImplementation.class.getName());
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    try {
      // Default HCM#HCI is not accessible; make it so before invoking.
      Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class,
        ExecutorService.class, User.class);
      constructor.setAccessible(true);
      return user.runAs(
        (PrivilegedExceptionAction<Connection>)() ->
          (Connection) constructor.newInstance(conf, pool, user));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Call {@link #createAsyncConnection(Configuration)} using default HBaseConfiguration.
   * @see #createAsyncConnection(Configuration)
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection() {
    return createAsyncConnection(HBaseConfiguration.create());
  }

  /**
   * Call {@link #createAsyncConnection(Configuration, User)} using the given {@code conf} and a
   * User object created by {@link UserProvider}. The given {@code conf} will also be used to
   * initialize the {@link UserProvider}.
   * @param conf configuration
   * @return AsyncConnection object wrapped by CompletableFuture
   * @see #createAsyncConnection(Configuration, User)
   * @see UserProvider
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(Configuration conf) {
    User user;
    try {
      user = AuthUtil.loginClient(conf);
    } catch (IOException e) {
      CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
    return createAsyncConnection(conf, user);
  }

  /**
   * Create a new AsyncConnection instance using the passed {@code conf} and {@code user}.
   * AsyncConnection encapsulates all housekeeping for a connection to the cluster. All tables and
   * interfaces created from returned connection share zookeeper connection, meta cache, and
   * connections to region servers and masters.
   * <p>
   * The caller is responsible for calling {@link AsyncConnection#close()} on the returned
   * connection instance.
   * <p>
   * Usually you should only create one AsyncConnection instance in your code and use it everywhere
   * as it is thread safe.
   * @param conf configuration
   * @param user the user the asynchronous connection is for
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(Configuration conf,
      final User user) {
    CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
    ConnectionRegistry registry = ConnectionRegistryFactory.getRegistry(conf);
    addListener(registry.getClusterId(), (clusterId, error) -> {
      if (error != null) {
        registry.close();
        future.completeExceptionally(error);
        return;
      }
      if (clusterId == null) {
        registry.close();
        future.completeExceptionally(new IOException("clusterid came back null"));
        return;
      }
      Class<? extends AsyncConnection> clazz = conf.getClass(HBASE_CLIENT_ASYNC_CONNECTION_IMPL,
        AsyncConnectionImpl.class, AsyncConnection.class);
      try {
        future.complete(
          user.runAs((PrivilegedExceptionAction<? extends AsyncConnection>) () -> ReflectionUtils
            .newInstance(clazz, conf, registry, clusterId, user)));
      } catch (Exception e) {
        registry.close();
        future.completeExceptionally(e);
      }
    });
    return future;
  }
}
