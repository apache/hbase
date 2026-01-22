/*
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
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

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
 * <li>hbase.client.keytab.file, points to a valid keytab on the local filesystem
 * <li>hbase.client.kerberos.principal, gives the Kerberos principal to use
 * </ul>
 * By this way, caller can directly connect to kerberized cluster without caring login and
 * credentials renewal logic in application.
 *
 * <pre>
 * </pre>
 *
 * Similarly, {@link Connection} also returns {@link Admin} and {@link RegionLocator}
 * implementations.
 * @see Connection
 * @since 0.99.0
 */
@InterfaceAudience.Public
public class ConnectionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);

  public static final String HBASE_CLIENT_ASYNC_CONNECTION_IMPL =
    "hbase.client.async.connection.impl";

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
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection() throws IOException {
    return createConnection(HBaseConfiguration.create());
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
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri the connection uri for the hbase cluster
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri) throws IOException {
    return createConnection(connectionUri, HBaseConfiguration.create());
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
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
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri, Configuration conf)
    throws IOException {
    return createConnection(connectionUri, conf, null, AuthUtil.loginClient(conf));
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
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
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @param pool          the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri, Configuration conf,
    ExecutorService pool) throws IOException {
    return createConnection(connectionUri, conf, pool, AuthUtil.loginClient(conf));
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
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
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @param user          the user the connection is for
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri, Configuration conf, User user)
    throws IOException {
    return createConnection(connectionUri, conf, null, user);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
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
    return createConnection(conf, pool, user, Collections.emptyMap());
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @param user          the user the connection is for
   * @param pool          the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri, Configuration conf,
    ExecutorService pool, User user) throws IOException {
    return createConnection(connectionUri, conf, pool, user, Collections.emptyMap());
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param conf                 configuration
   * @param user                 the user the connection is for
   * @param pool                 the thread pool to use for batch operations
   * @param connectionAttributes attributes to be sent along to server during connection establish
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool,
    final User user, Map<String, byte[]> connectionAttributes) throws IOException {
    return createConnection(null, conf, pool, user, connectionAttributes);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection(if used), meta cache, and
   * connections to region servers and masters. <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned connection
   * instance. Typical usage:
   *
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
   * try (Connection connection = ConnectionFactory.createConnection(conf);
   *   Table table = connection.getTable(TableName.valueOf("table1"))) {
   *   table.get(...);
   *   ...
   * }
   * </pre>
   *
   * @param connectionUri        the connection uri for the hbase cluster
   * @param conf                 configuration
   * @param user                 the user the connection is for
   * @param pool                 the thread pool to use for batch operations
   * @param connectionAttributes attributes to be sent along to server during connection establish
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(URI connectionUri, Configuration conf,
    ExecutorService pool, final User user, Map<String, byte[]> connectionAttributes)
    throws IOException {
    Class<?> clazz = conf.getClass(ConnectionUtils.HBASE_CLIENT_CONNECTION_IMPL,
      ConnectionOverAsyncConnection.class, Connection.class);
    if (clazz != ConnectionOverAsyncConnection.class) {
      return TraceUtil.trace(() -> {
        try {
          // Default HCM#HCI is not accessible; make it so before invoking.
          Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class,
            ExecutorService.class, User.class, ConnectionRegistry.class, Map.class);
          constructor.setAccessible(true);
          ConnectionRegistry registry = connectionUri != null
            ? ConnectionRegistryFactory.create(connectionUri, conf, user)
            : ConnectionRegistryFactory.create(conf, user);
          return user.runAs((PrivilegedExceptionAction<Connection>) () -> (Connection) constructor
            .newInstance(conf, pool, user, registry, connectionAttributes));
        } catch (NoSuchMethodException e) {
          LOG.debug("Constructor with connection registry not found for class {},"
            + " fallback to use old constructor", clazz.getName(), e);
        } catch (Exception e) {
          Throwables.throwIfInstanceOf(e, IOException.class);
          Throwables.throwIfUnchecked(e);
          throw new IOException(e);
        }

        try {
          // Default HCM#HCI is not accessible; make it so before invoking.
          Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class,
            ExecutorService.class, User.class, Map.class);
          constructor.setAccessible(true);
          return user.runAs((PrivilegedExceptionAction<Connection>) () -> (Connection) constructor
            .newInstance(conf, pool, user, connectionAttributes));
        } catch (Exception e) {
          Throwables.throwIfInstanceOf(e, IOException.class);
          Throwables.throwIfUnchecked(e);
          throw new IOException(e);
        }
      }, () -> TraceUtil.createSpan(ConnectionFactory.class.getSimpleName() + ".createConnection"));
    } else {
      return FutureUtils.get(createAsyncConnection(connectionUri, conf, user, connectionAttributes))
        .toConnection();
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
   * Call {@link #createAsyncConnection(URI, Configuration)} using default HBaseConfiguration.
   * @param connectionUri the connection uri for the hbase cluster
   * @see #createAsyncConnection(URI, Configuration)
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(URI connectionUri) {
    return createAsyncConnection(connectionUri, HBaseConfiguration.create());
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
    return createAsyncConnection(null, conf);
  }

  /**
   * Call {@link #createAsyncConnection(Configuration, User)} using the given {@code connectionUri},
   * {@code conf} and a User object created by {@link UserProvider}. The given {@code conf} will
   * also be used to initialize the {@link UserProvider}.
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @return AsyncConnection object wrapped by CompletableFuture
   * @see #createAsyncConnection(Configuration, User)
   * @see UserProvider
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(URI connectionUri,
    Configuration conf) {
    User user;
    try {
      user = AuthUtil.loginClient(conf);
    } catch (IOException e) {
      CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
    return createAsyncConnection(connectionUri, conf, user);
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
    return createAsyncConnection(null, conf, user);
  }

  /**
   * Create a new AsyncConnection instance using the passed {@code connectionUri}, {@code conf} and
   * {@code user}. AsyncConnection encapsulates all housekeeping for a connection to the cluster.
   * All tables and interfaces created from returned connection share zookeeper connection(if used),
   * meta cache, and connections to region servers and masters.
   * <p>
   * The caller is responsible for calling {@link AsyncConnection#close()} on the returned
   * connection instance.
   * <p>
   * Usually you should only create one AsyncConnection instance in your code and use it everywhere
   * as it is thread safe.
   * @param connectionUri the connection uri for the hbase cluster
   * @param conf          configuration
   * @param user          the user the asynchronous connection is for
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(URI connectionUri,
    Configuration conf, final User user) {
    return createAsyncConnection(connectionUri, conf, user, null);
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
   * @param conf                 configuration
   * @param user                 the user the asynchronous connection is for
   * @param connectionAttributes attributes to be sent along to server during connection establish
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(Configuration conf,
    final User user, Map<String, byte[]> connectionAttributes) {
    return createAsyncConnection(null, conf, user, connectionAttributes);
  }

  /**
   * Create a new AsyncConnection instance using the passed {@code connectionUri}, {@code conf} and
   * {@code user}. AsyncConnection encapsulates all housekeeping for a connection to the cluster.
   * All tables and interfaces created from returned connection share zookeeper connection(if used),
   * meta cache, and connections to region servers and masters.
   * <p>
   * The caller is responsible for calling {@link AsyncConnection#close()} on the returned
   * connection instance.
   * <p>
   * Usually you should only create one AsyncConnection instance in your code and use it everywhere
   * as it is thread safe.
   * @param connectionUri        the connection uri for the hbase cluster
   * @param conf                 configuration
   * @param user                 the user the asynchronous connection is for
   * @param connectionAttributes attributes to be sent along to server during connection establish
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(URI connectionUri,
    Configuration conf, final User user, Map<String, byte[]> connectionAttributes) {
    return TraceUtil.tracedFuture(() -> {
      ConnectionRegistry registry;
      Configuration appliedConf;
      try {
        if (connectionUri != null) {
          appliedConf = new Configuration(conf);
          Strings.applyURIQueriesToConf(connectionUri, appliedConf);
          registry = ConnectionRegistryFactory.create(connectionUri, appliedConf, user);
        } else {
          appliedConf = conf;
          registry = ConnectionRegistryFactory.create(appliedConf, user);
        }
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
      CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
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
        // Fetch meta table name from registry
        addListener(registry.getMetaTableName(), (metaTableName, metaError) -> {
          if (metaError != null) {
            registry.close();
            future.completeExceptionally(metaError);
            return;
          }
          if (metaTableName == null) {
            registry.close();
            future.completeExceptionally(new IOException("meta table name came back null"));
            return;
          }
          Class<? extends AsyncConnection> clazz = appliedConf.getClass(
            HBASE_CLIENT_ASYNC_CONNECTION_IMPL, AsyncConnectionImpl.class, AsyncConnection.class);
          try {
            future.complete(user.runAs((PrivilegedExceptionAction<
              ? extends AsyncConnection>) () -> ReflectionUtils.newInstance(clazz, appliedConf,
                registry, clusterId, metaTableName, null, user, connectionAttributes)));
          } catch (Exception e) {
            registry.close();
            future.completeExceptionally(e);
          }
        });
      });
      return future;
    }, "ConnectionFactory.createAsyncConnection");
  }
}
