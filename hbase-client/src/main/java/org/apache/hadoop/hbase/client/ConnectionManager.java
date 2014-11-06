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

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncRequestFuture;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * An internal, A non-instantiable class that manages creation of {@link HConnection}s.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Private
// NOTE: DO NOT make this class public. It was made package-private on purpose.
class ConnectionManager {
  static final Log LOG = LogFactory.getLog(ConnectionManager.class);

  public static final String RETRIES_BY_SERVER_KEY = "hbase.client.retries.by.server";
  private static final String CLIENT_NONCES_ENABLED_KEY = "hbase.client.nonces.enabled";

  // An LRU Map of HConnectionKey -> HConnection (TableServer).  All
  // access must be synchronized.  This map is not private because tests
  // need to be able to tinker with it.
  static final Map<HConnectionKey, HConnectionImplementation> CONNECTION_INSTANCES;

  public static final int MAX_CACHED_CONNECTION_INSTANCES;

  /**
   * Global nonceGenerator shared per client.Currently there's no reason to limit its scope.
   * Once it's set under nonceGeneratorCreateLock, it is never unset or changed.
   */
  private static volatile NonceGenerator nonceGenerator = null;
  /** The nonce generator lock. Only taken when creating HConnection, which gets a private copy. */
  private static Object nonceGeneratorCreateLock = new Object();

  static {
    // We set instances to one more than the value specified for {@link
    // HConstants#ZOOKEEPER_MAX_CLIENT_CNXNS}. By default, the zk default max
    // connections to the ensemble from the one client is 30, so in that case we
    // should run into zk issues before the LRU hit this value of 31.
    MAX_CACHED_CONNECTION_INSTANCES = HBaseConfiguration.create().getInt(
      HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS, HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS) + 1;
    CONNECTION_INSTANCES = new LinkedHashMap<HConnectionKey, HConnectionImplementation>(
        (int) (MAX_CACHED_CONNECTION_INSTANCES / 0.75F) + 1, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(
          Map.Entry<HConnectionKey, HConnectionImplementation> eldest) {
         return size() > MAX_CACHED_CONNECTION_INSTANCES;
       }
    };
  }

  /** Dummy nonce generator for disabled nonces. */
  static class NoNonceGenerator implements NonceGenerator {
    @Override
    public long getNonceGroup() {
      return HConstants.NO_NONCE;
    }
    @Override
    public long newNonce() {
      return HConstants.NO_NONCE;
    }
  }

  /*
   * Non-instantiable.
   */
  private ConnectionManager() {
    super();
  }

  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  @VisibleForTesting
  static NonceGenerator injectNonceGeneratorForTesting(
      HConnection conn, NonceGenerator cnm) {
    HConnectionImplementation connImpl = (HConnectionImplementation)conn;
    NonceGenerator ng = connImpl.getNonceGenerator();
    LOG.warn("Nonce generator is being replaced by test code for " + cnm.getClass().getName());
    connImpl.nonceGenerator = cnm;
    return ng;
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
    return getConnectionInternal(conf);
  }


  static ClusterConnection getConnectionInternal(final Configuration conf)
    throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (CONNECTION_INSTANCES) {
      HConnectionImplementation connection = CONNECTION_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = (HConnectionImplementation)createConnection(conf, true);
        CONNECTION_INSTANCES.put(connectionKey, connection);
      } else if (connection.isClosed()) {
        ConnectionManager.deleteConnection(connectionKey, true);
        connection = (HConnectionImplementation)createConnection(conf, true);
        CONNECTION_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   *
   * This is the recommended way to create HConnections.
   * {@code
   * HConnection connection = ConnectionManagerInternal.createConnection(conf);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   *
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf) throws IOException {
    return createConnectionInternal(conf);
  }

  static ClusterConnection createConnectionInternal(Configuration conf) throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, false, null, provider.getCurrent());
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, ExecutorService pool)
  throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, false, pool, provider.getCurrent());
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, User user)
  throws IOException {
    return createConnection(conf, false, null, user);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for
   * calling {@link HConnection#close()} on the returned connection instance.
   * This is the recommended way to create HConnections.
   * {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, ExecutorService pool, User user)
  throws IOException {
    return createConnection(conf, false, pool, user);
  }

  @Deprecated
  static HConnection createConnection(final Configuration conf, final boolean managed)
      throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, managed, null, provider.getCurrent());
  }

  @Deprecated
  static ClusterConnection createConnection(final Configuration conf, final boolean managed,
      final ExecutorService pool, final User user)
  throws IOException {
    return (ClusterConnection) ConnectionFactory.createConnection(conf, managed, pool, user);
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
    deleteConnection(new HConnectionKey(conf), false);
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
    deleteConnection(connection, true);
  }

  /**
   * Delete information for all connections. Close or not the connection, depending on the
   *  staleConnection boolean and the ref count. By default, you should use it with
   *  staleConnection to true.
   * @deprecated
   */
  @Deprecated
  public static void deleteAllConnections(boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(CONNECTION_INSTANCES.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, staleConnection);
      }
      CONNECTION_INSTANCES.clear();
    }
  }

  /**
   * Delete information for all connections..
   * @deprecated kept for backward compatibility, but the behavior is broken. HBASE-8983
   */
  @Deprecated
  public static void deleteAllConnections() {
    deleteAllConnections(false);
  }


  @Deprecated
  private static void deleteConnection(HConnection connection, boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      for (Entry<HConnectionKey, HConnectionImplementation> e: CONNECTION_INSTANCES.entrySet()) {
        if (e.getValue() == connection) {
          deleteConnection(e.getKey(), staleConnection);
          break;
        }
      }
    }
  }

  @Deprecated
  private static void deleteConnection(HConnectionKey connectionKey, boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      HConnectionImplementation connection = CONNECTION_INSTANCES.get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          CONNECTION_INSTANCES.remove(connectionKey);
          connection.internalClose();
        }
      } else {
        LOG.error("Connection not found in the list, can't delete it "+
          "(connection key=" + connectionKey + "). May be the key was modified?", new Exception());
      }
    }
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
   */
  @InterfaceAudience.Private
  public static <T> T execute(HConnectable<T> connectable) throws IOException {
    if (connectable == null || connectable.conf == null) {
      return null;
    }
    Configuration conf = connectable.conf;
    HConnection connection = getConnection(conf);
    boolean connectSucceeded = false;
    try {
      T returnValue = connectable.connect(connection);
      connectSucceeded = true;
      return returnValue;
    } finally {
      try {
        connection.close();
      } catch (Exception e) {
        ExceptionUtil.rethrowIfInterrupt(e);
        if (connectSucceeded) {
          throw new IOException("The connection to " + connection
              + " could not be deleted.", e);
        }
      }
    }
  }

  /** Encapsulates connection to zookeeper and regionservers.*/
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
      justification="Access to the conncurrent hash map is under a lock so should be fine.")
  static class HConnectionImplementation implements ClusterConnection, Closeable {
    static final Log LOG = LogFactory.getLog(HConnectionImplementation.class);
    private final long pause;
    private final int numTries;
    final int rpcTimeout;
    private NonceGenerator nonceGenerator = null;
    private final AsyncProcess asyncProcess;

    private volatile boolean closed;
    private volatile boolean aborted;

    // package protected for the tests
    ClusterStatusListener clusterStatusListener;


    private final Object metaRegionLock = new Object();

    // We have a single lock for master & zk to prevent deadlocks. Having
    //  one lock for ZK and one lock for master is not possible:
    //  When creating a connection to master, we need a connection to ZK to get
    //  its address. But another thread could have taken the ZK lock, and could
    //  be waiting for the master lock => deadlock.
    private final Object masterAndZKLock = new Object();

    private long keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
    private final DelayedClosing delayedClosing =
      DelayedClosing.createAndStart(this);

    // thread executor shared by all HTableInterface instances created
    // by this connection
    private volatile ExecutorService batchPool = null;
    private volatile boolean cleanupPool = false;

    private final Configuration conf;

    // Client rpc instance.
    private RpcClient rpcClient;

    private MetaCache metaCache = new MetaCache();

    private int refCount;

    // indicates whether this connection's life cycle is managed (by us)
    private boolean managed;

    private User user;

    private RpcRetryingCallerFactory rpcCallerFactory;

    private RpcControllerFactory rpcControllerFactory;

    private final RetryingCallerInterceptor interceptor;

    /**
     * Cluster registry of basic info such as clusterid and meta region location.
     */
     Registry registry;

     HConnectionImplementation(Configuration conf, boolean managed) throws IOException {
       this(conf, managed, null, null);
     }

    /**
     * constructor
     * @param conf Configuration object
     * @param managed If true, does not do full shutdown on close; i.e. cleanup of connection
     * to zk and shutdown of all services; we just close down the resources this connection was
     * responsible for and decrement usage counters.  It is up to the caller to do the full
     * cleanup.  It is set when we want have connection sharing going on -- reuse of zk connection,
     * and cached region locations, established regionserver connections, etc.  When connections
     * are shared, we have reference counting going on and will only do full cleanup when no more
     * users of an HConnectionImplementation instance.
     */
    HConnectionImplementation(Configuration conf, boolean managed,
        ExecutorService pool, User user) throws IOException {
      this(conf);
      this.user = user;
      this.batchPool = pool;
      this.managed = managed;
      this.registry = setupRegistry();
      retrieveClusterId();

      this.rpcClient = new RpcClient(this.conf, this.clusterId);
      this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);

      // Do we publish the status?
      boolean shouldListen = conf.getBoolean(HConstants.STATUS_PUBLISHED,
          HConstants.STATUS_PUBLISHED_DEFAULT);
      Class<? extends ClusterStatusListener.Listener> listenerClass =
          conf.getClass(ClusterStatusListener.STATUS_LISTENER_CLASS,
              ClusterStatusListener.DEFAULT_STATUS_LISTENER_CLASS,
              ClusterStatusListener.Listener.class);
      if (shouldListen) {
        if (listenerClass == null) {
          LOG.warn(HConstants.STATUS_PUBLISHED + " is true, but " +
              ClusterStatusListener.STATUS_LISTENER_CLASS + " is not set - not listening status");
        } else {
          clusterStatusListener = new ClusterStatusListener(
              new ClusterStatusListener.DeadServerHandler() {
                @Override
                public void newDead(ServerName sn) {
                  clearCaches(sn);
                  rpcClient.cancelConnections(sn.getHostname(), sn.getPort());
                }
              }, conf, listenerClass);
        }
      }
    }

    /**
     * For tests.
     */
    protected HConnectionImplementation(Configuration conf) {
      this.conf = conf;
      this.closed = false;
      this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
          HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
      this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      this.rpcTimeout = conf.getInt(
          HConstants.HBASE_RPC_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
      if (conf.getBoolean(CLIENT_NONCES_ENABLED_KEY, true)) {
        synchronized (nonceGeneratorCreateLock) {
          if (ConnectionManager.nonceGenerator == null) {
            ConnectionManager.nonceGenerator = new PerClientRandomNonceGenerator();
          }
          this.nonceGenerator = ConnectionManager.nonceGenerator;
        }
      } else {
        this.nonceGenerator = new NoNonceGenerator();
      }
      this.asyncProcess = createAsyncProcess(this.conf);
      this.interceptor = (new RetryingCallerInterceptorFactory(conf)).build();
      this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, interceptor);
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
      return getTable(TableName.valueOf(tableName));
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException {
      return getTable(TableName.valueOf(tableName));
    }

    @Override
    public HTableInterface getTable(TableName tableName) throws IOException {
      return getTable(tableName, getBatchPool());
    }

    @Override
    public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException {
      return getTable(TableName.valueOf(tableName), pool);
    }

    @Override
    public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException {
      return getTable(TableName.valueOf(tableName), pool);
    }

    @Override
    public HTableInterface getTable(TableName tableName, ExecutorService pool) throws IOException {
      if (managed) {
        throw new IOException("The connection has to be unmanaged.");
      }
      return new HTable(tableName, this, pool);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
      if (managed) {
        throw new IOException("The connection has to be unmanaged.");
      }
      return new HTable(tableName, this, getBatchPool());
    }

    @Override
    public Admin getAdmin() throws IOException {
      if (managed) {
        throw new IOException("The connection has to be unmanaged.");
      }
      return new HBaseAdmin(this);
    }

    private ExecutorService getBatchPool() {
      if (batchPool == null) {
        // shared HTable thread executor not yet initialized
        synchronized (this) {
          if (batchPool == null) {
            int maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
            int coreThreads = conf.getInt("hbase.hconnection.threads.core", 256);
            if (maxThreads == 0) {
              maxThreads = Runtime.getRuntime().availableProcessors() * 8;
            }
            if (coreThreads == 0) {
              coreThreads = Runtime.getRuntime().availableProcessors() * 8;
            }
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            LinkedBlockingQueue<Runnable> workQueue =
              new LinkedBlockingQueue<Runnable>(maxThreads *
                conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                  HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                keepAliveTime,
                TimeUnit.SECONDS,
                workQueue,
                Threads.newDaemonThreadFactory(toString() + "-shared-"));
            tpe.allowCoreThreadTimeOut(true);
            this.batchPool = tpe;
          }
          this.cleanupPool = true;
        }
      }
      return this.batchPool;
    }

    protected ExecutorService getCurrentBatchPool() {
      return batchPool;
    }

    private void shutdownBatchPool() {
      if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
        this.batchPool.shutdown();
        try {
          if (!this.batchPool.awaitTermination(10, TimeUnit.SECONDS)) {
            this.batchPool.shutdownNow();
          }
        } catch (InterruptedException e) {
          this.batchPool.shutdownNow();
        }
      }
    }

    /**
     * @return The cluster registry implementation to use.
     * @throws IOException
     */
    private Registry setupRegistry() throws IOException {
      String registryClass = this.conf.get("hbase.client.registry.impl",
        ZooKeeperRegistry.class.getName());
      Registry registry = null;
      try {
        registry = (Registry)Class.forName(registryClass).newInstance();
      } catch (Throwable t) {
        throw new IOException(t);
      }
      registry.init(this);
      return registry;
    }

    /**
     * For tests only.
     * @param rpcClient Client we should use instead.
     * @return Previous rpcClient
     */
    @VisibleForTesting
    RpcClient setRpcClient(final RpcClient rpcClient) {
      RpcClient oldRpcClient = this.rpcClient;
      this.rpcClient = rpcClient;
      return oldRpcClient;
    }

    /**
     * For tests only.
     */
    @VisibleForTesting
    RpcClient getRpcClient() {
      return rpcClient;
    }

    /**
     * An identifier that will remain the same for a given connection.
     */
    @Override
    public String toString(){
      return "hconnection-0x" + Integer.toHexString(hashCode());
    }

    protected String clusterId = null;

    void retrieveClusterId() {
      if (clusterId != null) return;
      this.clusterId = this.registry.getClusterId();
      if (clusterId == null) {
        clusterId = HConstants.CLUSTER_ID_DEFAULT;
        LOG.debug("clusterid came back null, using default " + clusterId);
      }
    }

    @Override
    public Configuration getConfiguration() {
      return this.conf;
    }

    private void checkIfBaseNodeAvailable(ZooKeeperWatcher zkw)
      throws MasterNotRunningException {
      String errorMsg;
      try {
        if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
          errorMsg = "The node " + zkw.baseZNode+" is not in ZooKeeper. "
            + "It should have been written by the master. "
            + "Check the value configured in 'zookeeper.znode.parent'. "
            + "There could be a mismatch with the one configured in the master.";
          LOG.error(errorMsg);
          throw new MasterNotRunningException(errorMsg);
        }
      } catch (KeeperException e) {
        errorMsg = "Can't get connection to ZooKeeper: " + e.getMessage();
        LOG.error(errorMsg);
        throw new MasterNotRunningException(errorMsg, e);
      }
    }

    /**
     * @return true if the master is running, throws an exception otherwise
     * @throws MasterNotRunningException - if the master is not running
     * @throws ZooKeeperConnectionException
     */
    @Override
    public boolean isMasterRunning()
    throws MasterNotRunningException, ZooKeeperConnectionException {
      // When getting the master connection, we check it's running,
      // so if there is no exception, it means we've been able to get a
      // connection on a running master
      MasterKeepAliveConnection m = getKeepAliveMasterService();
      m.close();
      return true;
    }

    @Override
    public HRegionLocation getRegionLocation(final TableName tableName,
        final byte [] row, boolean reload)
    throws IOException {
      return reload? relocateRegion(tableName, row): locateRegion(tableName, row);
    }

    @Override
    public HRegionLocation getRegionLocation(final byte[] tableName,
        final byte [] row, boolean reload)
    throws IOException {
      return getRegionLocation(TableName.valueOf(tableName), row, reload);
    }

    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException {
      return getTableState(tableName).inStates(TableState.State.ENABLED);
    }

    @Override
    public boolean isTableEnabled(byte[] tableName) throws IOException {
      return isTableEnabled(TableName.valueOf(tableName));
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
      return getTableState(tableName).inStates(TableState.State.DISABLED);
    }

    @Override
    public boolean isTableDisabled(byte[] tableName) throws IOException {
      return isTableDisabled(TableName.valueOf(tableName));
    }

    @Override
    public boolean isTableAvailable(final TableName tableName) throws IOException {
      final AtomicBoolean available = new AtomicBoolean(true);
      final AtomicInteger regionCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result row) throws IOException {
          HRegionInfo info = MetaScanner.getHRegionInfo(row);
          if (info != null && !info.isSplitParent()) {
            if (tableName.equals(info.getTable())) {
              ServerName server = HRegionInfo.getServerName(row);
              if (server == null) {
                available.set(false);
                return false;
              }
              regionCount.incrementAndGet();
            } else if (tableName.compareTo(info.getTable()) < 0) {
              // Return if we are done with the current table
              return false;
            }
          }
          return true;
        }
      };
      MetaScanner.metaScan(conf, this, visitor, tableName);
      return available.get() && (regionCount.get() > 0);
    }

    @Override
    public boolean isTableAvailable(final byte[] tableName) throws IOException {
      return isTableAvailable(TableName.valueOf(tableName));
    }

    @Override
    public boolean isTableAvailable(final TableName tableName, final byte[][] splitKeys)
        throws IOException {
      final AtomicBoolean available = new AtomicBoolean(true);
      final AtomicInteger regionCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result row) throws IOException {
          HRegionInfo info = MetaScanner.getHRegionInfo(row);
          if (info != null && !info.isSplitParent()) {
            if (tableName.equals(info.getTable())) {
              ServerName server = HRegionInfo.getServerName(row);
              if (server == null) {
                available.set(false);
                return false;
              }
              if (!Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
                for (byte[] splitKey : splitKeys) {
                  // Just check if the splitkey is available
                  if (Bytes.equals(info.getStartKey(), splitKey)) {
                    regionCount.incrementAndGet();
                    break;
                  }
                }
              } else {
                // Always empty start row should be counted
                regionCount.incrementAndGet();
              }
            } else if (tableName.compareTo(info.getTable()) < 0) {
              // Return if we are done with the current table
              return false;
            }
          }
          return true;
        }
      };
      MetaScanner.metaScan(conf, this, visitor, tableName);
      // +1 needs to be added so that the empty start row is also taken into account
      return available.get() && (regionCount.get() == splitKeys.length + 1);
    }

    @Override
    public boolean isTableAvailable(final byte[] tableName, final byte[][] splitKeys)
        throws IOException {
      return isTableAvailable(TableName.valueOf(tableName), splitKeys);
    }

    @Override
    public HRegionLocation locateRegion(final byte[] regionName) throws IOException {
      RegionLocations locations = locateRegion(HRegionInfo.getTable(regionName),
        HRegionInfo.getStartKey(regionName), false, true);
      return locations == null ? null : locations.getRegionLocation();
    }

    @Override
    public boolean isDeadServer(ServerName sn) {
      if (clusterStatusListener == null) {
        return false;
      } else {
        return clusterStatusListener.isDeadServer(sn);
      }
    }

    @Override
    public List<HRegionLocation> locateRegions(final TableName tableName)
    throws IOException {
      return locateRegions (tableName, false, true);
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte[] tableName)
    throws IOException {
      return locateRegions(TableName.valueOf(tableName));
    }

    @Override
    public List<HRegionLocation> locateRegions(final TableName tableName,
        final boolean useCache, final boolean offlined) throws IOException {
      NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(conf, this,
          tableName);
      final List<HRegionLocation> locations = new ArrayList<HRegionLocation>();
      for (HRegionInfo regionInfo : regions.keySet()) {
        RegionLocations list = locateRegion(tableName, regionInfo.getStartKey(), useCache, true);
        if (list != null) {
          for (HRegionLocation loc : list.getRegionLocations()) {
            if (loc != null) {
              locations.add(loc);
            }
          }
        }
      }
      return locations;
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte[] tableName,
       final boolean useCache, final boolean offlined) throws IOException {
      return locateRegions(TableName.valueOf(tableName), useCache, offlined);
    }

    @Override
    public HRegionLocation locateRegion(
        final TableName tableName, final byte[] row) throws IOException{
      RegionLocations locations = locateRegion(tableName, row, true, true);
      return locations == null ? null : locations.getRegionLocation();
    }

    @Override
    public HRegionLocation locateRegion(final byte[] tableName,
        final byte [] row)
    throws IOException{
      return locateRegion(TableName.valueOf(tableName), row);
    }

    @Override
    public HRegionLocation relocateRegion(final TableName tableName,
        final byte [] row) throws IOException{
      RegionLocations locations =  relocateRegion(tableName, row,
        RegionReplicaUtil.DEFAULT_REPLICA_ID);
      return locations == null ? null :
        locations.getRegionLocation(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    }

    @Override
    public RegionLocations relocateRegion(final TableName tableName,
        final byte [] row, int replicaId) throws IOException{
      // Since this is an explicit request not to use any caching, finding
      // disabled tables should not be desirable.  This will ensure that an exception is thrown when
      // the first time a disabled table is interacted with.
      if (!tableName.equals(TableName.META_TABLE_NAME) && isTableDisabled(tableName)) {
        throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
      }

      return locateRegion(tableName, row, false, true, replicaId);
    }

    @Override
    public HRegionLocation relocateRegion(final byte[] tableName,
        final byte [] row) throws IOException {
      return relocateRegion(TableName.valueOf(tableName), row);
    }

    @Override
    public RegionLocations locateRegion(final TableName tableName,
      final byte [] row, boolean useCache, boolean retry)
    throws IOException {
      return locateRegion(tableName, row, useCache, retry, RegionReplicaUtil.DEFAULT_REPLICA_ID);
    }

    @Override
    public RegionLocations locateRegion(final TableName tableName,
      final byte [] row, boolean useCache, boolean retry, int replicaId)
    throws IOException {
      if (this.closed) throw new IOException(toString() + " closed");
      if (tableName== null || tableName.getName().length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
      if (tableName.equals(TableName.META_TABLE_NAME)) {
        return locateMeta(tableName, useCache, replicaId);
      } else {
        // Region not in the cache - have to go to the meta RS
        return locateRegionInMeta(tableName, row, useCache, retry, replicaId);
      }
    }

    private RegionLocations locateMeta(final TableName tableName,
        boolean useCache, int replicaId) throws IOException {
      // HBASE-10785: We cache the location of the META itself, so that we are not overloading
      // zookeeper with one request for every region lookup. We cache the META with empty row
      // key in MetaCache.
      byte[] metaCacheKey = HConstants.EMPTY_START_ROW; // use byte[0] as the row for meta
      RegionLocations locations = null;
      if (useCache) {
        locations = getCachedLocation(tableName, metaCacheKey);
        if (locations != null) {
          return locations;
        }
      }

      // only one thread should do the lookup.
      synchronized (metaRegionLock) {
        // Check the cache again for a hit in case some other thread made the
        // same query while we were waiting on the lock.
        if (useCache) {
          locations = getCachedLocation(tableName, metaCacheKey);
          if (locations != null) {
            return locations;
          }
        }

        // Look up from zookeeper
        locations = this.registry.getMetaRegionLocation();
        if (locations != null) {
          cacheLocation(tableName, locations);
        }
      }
      return locations;
    }

    /*
      * Search the hbase:meta table for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    private RegionLocations locateRegionInMeta(TableName tableName, byte[] row,
                   boolean useCache, boolean retry, int replicaId) throws IOException {

      // If we are supposed to be using the cache, look in the cache to see if
      // we already have the region.
      if (useCache) {
        RegionLocations locations = getCachedLocation(tableName, row);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
      }

      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      byte[] metaKey = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);

      Scan s = new Scan();
      s.setReversed(true);
      s.setStartRow(metaKey);
      s.setSmall(true);
      s.setCaching(1);

      int localNumRetries = (retry ? numTries : 1);

      for (int tries = 0; true; tries++) {
        if (tries >= localNumRetries) {
          throw new NoServerForRegionException("Unable to find region for "
              + Bytes.toStringBinary(row) + " in " + tableName +
              " after " + localNumRetries + " tries.");
        }
        if (useCache) {
          RegionLocations locations = getCachedLocation(tableName, row);
          if (locations != null && locations.getRegionLocation(replicaId) != null) {
            return locations;
          }
        } else {
          // If we are not supposed to be using the cache, delete any existing cached location
          // so it won't interfere.
          metaCache.clearCache(tableName, row);
        }

        // Query the meta region
        try {
          Result regionInfoRow = null;
          ReversedClientScanner rcs = null;
          try {
            rcs = new ClientSmallReversedScanner(conf, s, TableName.META_TABLE_NAME, this,
              rpcCallerFactory, rpcControllerFactory, getBatchPool(), 0);
            regionInfoRow = rcs.next();
          } finally {
            if (rcs != null) {
              rcs.close();
            }
          }

          if (regionInfoRow == null) {
            throw new TableNotFoundException(tableName);
          }

          // convert the row result into the HRegionLocation we need!
          RegionLocations locations = MetaTableAccessor.getRegionLocations(regionInfoRow);
          if (locations == null || locations.getRegionLocation(replicaId) == null) {
            throw new IOException("HRegionInfo was null in " +
              tableName + ", row=" + regionInfoRow);
          }
          HRegionInfo regionInfo = locations.getRegionLocation(replicaId).getRegionInfo();
          if (regionInfo == null) {
            throw new IOException("HRegionInfo was null or empty in " +
              TableName.META_TABLE_NAME + ", row=" + regionInfoRow);
          }

          // possible we got a region of a different table...
          if (!regionInfo.getTable().equals(tableName)) {
            throw new TableNotFoundException(
                  "Table '" + tableName + "' was not found, got: " +
                  regionInfo.getTable() + ".");
          }
          if (regionInfo.isSplit()) {
            throw new RegionOfflineException("the only available region for" +
              " the required row is a split parent," +
              " the daughters should be online soon: " +
              regionInfo.getRegionNameAsString());
          }
          if (regionInfo.isOffline()) {
            throw new RegionOfflineException("the region is offline, could" +
              " be caused by a disable table call: " +
              regionInfo.getRegionNameAsString());
          }

          ServerName serverName = locations.getRegionLocation(replicaId).getServerName();
          if (serverName == null) {
            throw new NoServerForRegionException("No server address listed " +
              "in " + TableName.META_TABLE_NAME + " for region " +
              regionInfo.getRegionNameAsString() + " containing row " +
              Bytes.toStringBinary(row));
          }

          if (isDeadServer(serverName)){
            throw new RegionServerStoppedException("hbase:meta says the region "+
                regionInfo.getRegionNameAsString()+" is managed by the server " + serverName +
                ", but it is dead.");
          }
          // Instantiate the location
          cacheLocation(tableName, locations);
          return locations;
        } catch (TableNotFoundException e) {
          // if we got this error, probably means the table just plain doesn't
          // exist. rethrow the error immediately. this should always be coming
          // from the HTable constructor.
          throw e;
        } catch (IOException e) {
          ExceptionUtil.rethrowIfInterrupt(e);

          if (e instanceof RemoteException) {
            e = ((RemoteException)e).unwrapRemoteException();
          }
          if (tries < localNumRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("locateRegionInMeta parentTable=" +
                  TableName.META_TABLE_NAME + ", metaLocation=" +
                ", attempt=" + tries + " of " +
                localNumRetries + " failed; retrying after sleep of " +
                ConnectionUtils.getPauseTime(this.pause, tries) + " because: " + e.getMessage());
            }
          } else {
            throw e;
          }
          // Only relocate the parent region if necessary
          if(!(e instanceof RegionOfflineException ||
              e instanceof NoServerForRegionException)) {
            relocateRegion(TableName.META_TABLE_NAME, metaKey, replicaId);
          }
        }
        try{
          Thread.sleep(ConnectionUtils.getPauseTime(this.pause, tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Giving up trying to location region in " +
            "meta: thread is interrupted.");
        }
      }
    }

    /**
     * Put a newly discovered HRegionLocation into the cache.
     * @param tableName The table name.
     * @param location the new location
     */
    private void cacheLocation(final TableName tableName, final RegionLocations location) {
      metaCache.cacheLocation(tableName, location);
    }

    /**
     * Search the cache for a location that fits our table and row key.
     * Return null if no suitable region is located.
     *
     * @param tableName
     * @param row
     * @return Null or region location found in cache.
     */
    RegionLocations getCachedLocation(final TableName tableName,
        final byte [] row) {
      return metaCache.getCachedLocation(tableName, row);
    }

    public void clearRegionCache(final TableName tableName, byte[] row) {
      metaCache.clearCache(tableName, row);
    }

    /*
     * Delete all cached entries of a table that maps to a specific location.
     */
    @Override
    public void clearCaches(final ServerName serverName) {
      metaCache.clearCache(serverName);
    }

    @Override
    public void clearRegionCache() {
      metaCache.clearCache();
    }

    @Override
    public void clearRegionCache(final TableName tableName) {
      metaCache.clearCache(tableName);
    }

    @Override
    public void clearRegionCache(final byte[] tableName) {
      clearRegionCache(TableName.valueOf(tableName));
    }

    /**
     * Put a newly discovered HRegionLocation into the cache.
     * @param tableName The table name.
     * @param source the source of the new location, if it's not coming from meta
     * @param location the new location
     */
    private void cacheLocation(final TableName tableName, final ServerName source,
        final HRegionLocation location) {
      metaCache.cacheLocation(tableName, source, location);
    }

    // Map keyed by service name + regionserver to service stub implementation
    private final ConcurrentHashMap<String, Object> stubs =
      new ConcurrentHashMap<String, Object>();
    // Map of locks used creating service stubs per regionserver.
    private final ConcurrentHashMap<String, String> connectionLock =
      new ConcurrentHashMap<String, String>();

    /**
     * State of the MasterService connection/setup.
     */
    static class MasterServiceState {
      HConnection connection;
      MasterService.BlockingInterface stub;
      int userCount;
      long keepAliveUntil = Long.MAX_VALUE;

      MasterServiceState (final HConnection connection) {
        super();
        this.connection = connection;
      }

      @Override
      public String toString() {
        return "MasterService";
      }

      Object getStub() {
        return this.stub;
      }

      void clearStub() {
        this.stub = null;
      }

      boolean isMasterRunning() throws ServiceException {
        IsMasterRunningResponse response =
          this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
        return response != null? response.getIsMasterRunning(): false;
      }
    }

    /**
     * Makes a client-side stub for master services. Sub-class to specialize.
     * Depends on hosting class so not static.  Exists so we avoid duplicating a bunch of code
     * when setting up the MasterMonitorService and MasterAdminService.
     */
    abstract class StubMaker {
      /**
       * Returns the name of the service stub being created.
       */
      protected abstract String getServiceName();

      /**
       * Make stub and cache it internal so can be used later doing the isMasterRunning call.
       * @param channel
       */
      protected abstract Object makeStub(final BlockingRpcChannel channel);

      /**
       * Once setup, check it works by doing isMasterRunning check.
       * @throws ServiceException
       */
      protected abstract void isMasterRunning() throws ServiceException;

      /**
       * Create a stub. Try once only.  It is not typed because there is no common type to
       * protobuf services nor their interfaces.  Let the caller do appropriate casting.
       * @return A stub for master services.
       * @throws IOException
       * @throws KeeperException
       * @throws ServiceException
       */
      private Object makeStubNoRetries() throws IOException, KeeperException, ServiceException {
        ZooKeeperKeepAliveConnection zkw;
        try {
          zkw = getKeepAliveZooKeeperWatcher();
        } catch (IOException e) {
          ExceptionUtil.rethrowIfInterrupt(e);
          throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
        }
        try {
          checkIfBaseNodeAvailable(zkw);
          ServerName sn = MasterAddressTracker.getMasterAddress(zkw);
          if (sn == null) {
            String msg = "ZooKeeper available but no active master location found";
            LOG.info(msg);
            throw new MasterNotRunningException(msg);
          }
          if (isDeadServer(sn)) {
            throw new MasterNotRunningException(sn + " is dead.");
          }
          // Use the security info interface name as our stub key
          String key = getStubKey(getServiceName(), sn.getHostAndPort());
          connectionLock.putIfAbsent(key, key);
          Object stub = null;
          synchronized (connectionLock.get(key)) {
            stub = stubs.get(key);
            if (stub == null) {
              BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
              stub = makeStub(channel);
              isMasterRunning();
              stubs.put(key, stub);
            }
          }
          return stub;
        } finally {
          zkw.close();
        }
      }

      /**
       * Create a stub against the master.  Retry if necessary.
       * @return A stub to do <code>intf</code> against the master
       * @throws MasterNotRunningException
       */
      @edu.umd.cs.findbugs.annotations.SuppressWarnings (value="SWL_SLEEP_WITH_LOCK_HELD")
      Object makeStub() throws MasterNotRunningException {
        // The lock must be at the beginning to prevent multiple master creations
        //  (and leaks) in a multithread context
        synchronized (masterAndZKLock) {
          Exception exceptionCaught = null;
          Object stub = null;
          int tries = 0;
          while (!closed && stub == null) {
            tries++;
            try {
              stub = makeStubNoRetries();
            } catch (IOException e) {
              exceptionCaught = e;
            } catch (KeeperException e) {
              exceptionCaught = e;
            } catch (ServiceException e) {
              exceptionCaught = e;
            }

            if (exceptionCaught != null)
              // It failed. If it's not the last try, we're going to wait a little
              if (tries < numTries && !ExceptionUtil.isInterrupt(exceptionCaught)) {
                // tries at this point is 1 or more; decrement to start from 0.
                long pauseTime = ConnectionUtils.getPauseTime(pause, tries - 1);
                LOG.info("getMaster attempt " + tries + " of " + numTries +
                    " failed; retrying after sleep of " + pauseTime + ", exception=" +
                  exceptionCaught);

                try {
                  Thread.sleep(pauseTime);
                } catch (InterruptedException e) {
                  throw new MasterNotRunningException(
                      "Thread was interrupted while trying to connect to master.", e);
                }
              } else {
                // Enough tries, we stop now
                LOG.info("getMaster attempt " + tries + " of " + numTries +
                    " failed; no more retrying.", exceptionCaught);
                throw new MasterNotRunningException(exceptionCaught);
              }
          }

          if (stub == null) {
            // implies this.closed true
            throw new MasterNotRunningException("Connection was closed while trying to get master");
          }
          return stub;
        }
      }
    }

    /**
     * Class to make a MasterServiceStubMaker stub.
     */
    class MasterServiceStubMaker extends StubMaker {
      private MasterService.BlockingInterface stub;
      @Override
      protected String getServiceName() {
        return MasterService.getDescriptor().getName();
      }

      @Override
      @edu.umd.cs.findbugs.annotations.SuppressWarnings("SWL_SLEEP_WITH_LOCK_HELD")
      MasterService.BlockingInterface makeStub() throws MasterNotRunningException {
        return (MasterService.BlockingInterface)super.makeStub();
      }

      @Override
      protected Object makeStub(BlockingRpcChannel channel) {
        this.stub = MasterService.newBlockingStub(channel);
        return this.stub;
      }

      @Override
      protected void isMasterRunning() throws ServiceException {
        this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      }
    }

    @Override
    public AdminService.BlockingInterface getAdmin(final ServerName serverName)
        throws IOException {
      return getAdmin(serverName, false);
    }

    @Override
    // Nothing is done w/ the 'master' parameter.  It is ignored.
    public AdminService.BlockingInterface getAdmin(final ServerName serverName,
      final boolean master)
    throws IOException {
      if (isDeadServer(serverName)) {
        throw new RegionServerStoppedException(serverName + " is dead.");
      }
      String key = getStubKey(AdminService.BlockingInterface.class.getName(),
        serverName.getHostAndPort());
      this.connectionLock.putIfAbsent(key, key);
      AdminService.BlockingInterface stub = null;
      synchronized (this.connectionLock.get(key)) {
        stub = (AdminService.BlockingInterface)this.stubs.get(key);
        if (stub == null) {
          BlockingRpcChannel channel =
              this.rpcClient.createBlockingRpcChannel(serverName, user, rpcTimeout);
          stub = AdminService.newBlockingStub(channel);
          this.stubs.put(key, stub);
        }
      }
      return stub;
    }

    @Override
    public ClientService.BlockingInterface getClient(final ServerName sn)
    throws IOException {
      if (isDeadServer(sn)) {
        throw new RegionServerStoppedException(sn + " is dead.");
      }
      String key = getStubKey(ClientService.BlockingInterface.class.getName(), sn.getHostAndPort());
      this.connectionLock.putIfAbsent(key, key);
      ClientService.BlockingInterface stub = null;
      synchronized (this.connectionLock.get(key)) {
        stub = (ClientService.BlockingInterface)this.stubs.get(key);
        if (stub == null) {
          BlockingRpcChannel channel =
              this.rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
          stub = ClientService.newBlockingStub(channel);
          // In old days, after getting stub/proxy, we'd make a call.  We are not doing that here.
          // Just fail on first actual call rather than in here on setup.
          this.stubs.put(key, stub);
        }
      }
      return stub;
    }

    static String getStubKey(final String serviceName, final String rsHostnamePort) {
      return serviceName + "@" + rsHostnamePort;
    }

    private ZooKeeperKeepAliveConnection keepAliveZookeeper;
    private AtomicInteger keepAliveZookeeperUserCount = new AtomicInteger(0);
    private boolean canCloseZKW = true;

    // keepAlive time, in ms. No reason to make it configurable.
    private static final long keepAlive = 5 * 60 * 1000;

    /**
     * Retrieve a shared ZooKeeperWatcher. You must close it it once you've have finished with it.
     * @return The shared instance. Never returns null.
     */
    ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher()
      throws IOException {
      synchronized (masterAndZKLock) {
        if (keepAliveZookeeper == null) {
          if (this.closed) {
            throw new IOException(toString() + " closed");
          }
          // We don't check that our link to ZooKeeper is still valid
          // But there is a retry mechanism in the ZooKeeperWatcher itself
          keepAliveZookeeper = new ZooKeeperKeepAliveConnection(conf, this.toString(), this);
        }
        keepAliveZookeeperUserCount.addAndGet(1);
        keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
        return keepAliveZookeeper;
      }
    }

    void releaseZooKeeperWatcher(final ZooKeeperWatcher zkw) {
      if (zkw == null){
        return;
      }
      if (keepAliveZookeeperUserCount.addAndGet(-1) <= 0 ){
        keepZooKeeperWatcherAliveUntil = System.currentTimeMillis() + keepAlive;
      }
    }

    /**
     * Creates a Chore thread to check the connections to master & zookeeper
     *  and close them when they reach their closing time (
     *  {@link MasterServiceState#keepAliveUntil} and
     *  {@link #keepZooKeeperWatcherAliveUntil}). Keep alive time is
     *  managed by the release functions and the variable {@link #keepAlive}
     */
    private static class DelayedClosing extends Chore implements Stoppable {
      private HConnectionImplementation hci;
      Stoppable stoppable;

      private DelayedClosing(
        HConnectionImplementation hci, Stoppable stoppable){
        super(
          "ZooKeeperWatcher and Master delayed closing for connection "+hci,
          60*1000, // We check every minutes
          stoppable);
        this.hci = hci;
        this.stoppable = stoppable;
      }

      static DelayedClosing createAndStart(HConnectionImplementation hci){
        Stoppable stoppable = new Stoppable() {
              private volatile boolean isStopped = false;
              @Override public void stop(String why) { isStopped = true;}
              @Override public boolean isStopped() {return isStopped;}
            };

        return new DelayedClosing(hci, stoppable);
      }

      protected void closeMasterProtocol(MasterServiceState protocolState) {
        if (System.currentTimeMillis() > protocolState.keepAliveUntil) {
          hci.closeMasterService(protocolState);
          protocolState.keepAliveUntil = Long.MAX_VALUE;
        }
      }

      @Override
      protected void chore() {
        synchronized (hci.masterAndZKLock) {
          if (hci.canCloseZKW) {
            if (System.currentTimeMillis() >
              hci.keepZooKeeperWatcherAliveUntil) {

              hci.closeZooKeeperWatcher();
              hci.keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
            }
          }
          closeMasterProtocol(hci.masterServiceState);
          closeMasterProtocol(hci.masterServiceState);
        }
      }

      @Override
      public void stop(String why) {
        stoppable.stop(why);
      }

      @Override
      public boolean isStopped() {
        return stoppable.isStopped();
      }
    }

    private void closeZooKeeperWatcher() {
      synchronized (masterAndZKLock) {
        if (keepAliveZookeeper != null) {
          LOG.info("Closing zookeeper sessionid=0x" +
            Long.toHexString(
              keepAliveZookeeper.getRecoverableZooKeeper().getSessionId()));
          keepAliveZookeeper.internalClose();
          keepAliveZookeeper = null;
        }
        keepAliveZookeeperUserCount.set(0);
      }
    }

    final MasterServiceState masterServiceState = new MasterServiceState(this);

    @Override
    public MasterService.BlockingInterface getMaster() throws MasterNotRunningException {
      return getKeepAliveMasterService();
    }

    private void resetMasterServiceState(final MasterServiceState mss) {
      mss.userCount++;
      mss.keepAliveUntil = Long.MAX_VALUE;
    }

    @Override
    public MasterKeepAliveConnection getKeepAliveMasterService()
    throws MasterNotRunningException {
      synchronized (masterAndZKLock) {
        if (!isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
          MasterServiceStubMaker stubMaker = new MasterServiceStubMaker();
          this.masterServiceState.stub = stubMaker.makeStub();
        }
        resetMasterServiceState(this.masterServiceState);
      }
      // Ugly delegation just so we can add in a Close method.
      final MasterService.BlockingInterface stub = this.masterServiceState.stub;
      return new MasterKeepAliveConnection() {
        MasterServiceState mss = masterServiceState;
        @Override
        public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request)
        throws ServiceException {
          return stub.addColumn(controller, request);
        }

        @Override
        public DeleteColumnResponse deleteColumn(RpcController controller,
            DeleteColumnRequest request)
        throws ServiceException {
          return stub.deleteColumn(controller, request);
        }

        @Override
        public ModifyColumnResponse modifyColumn(RpcController controller,
            ModifyColumnRequest request)
        throws ServiceException {
          return stub.modifyColumn(controller, request);
        }

        @Override
        public MoveRegionResponse moveRegion(RpcController controller,
            MoveRegionRequest request) throws ServiceException {
          return stub.moveRegion(controller, request);
        }

        @Override
        public DispatchMergingRegionsResponse dispatchMergingRegions(
            RpcController controller, DispatchMergingRegionsRequest request)
            throws ServiceException {
          return stub.dispatchMergingRegions(controller, request);
        }

        @Override
        public AssignRegionResponse assignRegion(RpcController controller,
            AssignRegionRequest request) throws ServiceException {
          return stub.assignRegion(controller, request);
        }

        @Override
        public UnassignRegionResponse unassignRegion(RpcController controller,
            UnassignRegionRequest request) throws ServiceException {
          return stub.unassignRegion(controller, request);
        }

        @Override
        public OfflineRegionResponse offlineRegion(RpcController controller,
            OfflineRegionRequest request) throws ServiceException {
          return stub.offlineRegion(controller, request);
        }

        @Override
        public DeleteTableResponse deleteTable(RpcController controller,
            DeleteTableRequest request) throws ServiceException {
          return stub.deleteTable(controller, request);
        }

        @Override
        public TruncateTableResponse truncateTable(RpcController controller,
            TruncateTableRequest request) throws ServiceException {
          return stub.truncateTable(controller, request);
        }

        @Override
        public EnableTableResponse enableTable(RpcController controller,
            EnableTableRequest request) throws ServiceException {
          return stub.enableTable(controller, request);
        }

        @Override
        public DisableTableResponse disableTable(RpcController controller,
            DisableTableRequest request) throws ServiceException {
          return stub.disableTable(controller, request);
        }

        @Override
        public ModifyTableResponse modifyTable(RpcController controller,
            ModifyTableRequest request) throws ServiceException {
          return stub.modifyTable(controller, request);
        }

        @Override
        public CreateTableResponse createTable(RpcController controller,
            CreateTableRequest request) throws ServiceException {
          return stub.createTable(controller, request);
        }

        @Override
        public ShutdownResponse shutdown(RpcController controller,
            ShutdownRequest request) throws ServiceException {
          return stub.shutdown(controller, request);
        }

        @Override
        public StopMasterResponse stopMaster(RpcController controller,
            StopMasterRequest request) throws ServiceException {
          return stub.stopMaster(controller, request);
        }

        @Override
        public BalanceResponse balance(RpcController controller,
            BalanceRequest request) throws ServiceException {
          return stub.balance(controller, request);
        }

        @Override
        public SetBalancerRunningResponse setBalancerRunning(
            RpcController controller, SetBalancerRunningRequest request)
            throws ServiceException {
          return stub.setBalancerRunning(controller, request);
        }

        @Override
        public RunCatalogScanResponse runCatalogScan(RpcController controller,
            RunCatalogScanRequest request) throws ServiceException {
          return stub.runCatalogScan(controller, request);
        }

        @Override
        public EnableCatalogJanitorResponse enableCatalogJanitor(
            RpcController controller, EnableCatalogJanitorRequest request)
            throws ServiceException {
          return stub.enableCatalogJanitor(controller, request);
        }

        @Override
        public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(
            RpcController controller, IsCatalogJanitorEnabledRequest request)
            throws ServiceException {
          return stub.isCatalogJanitorEnabled(controller, request);
        }

        @Override
        public CoprocessorServiceResponse execMasterService(
            RpcController controller, CoprocessorServiceRequest request)
            throws ServiceException {
          return stub.execMasterService(controller, request);
        }

        @Override
        public SnapshotResponse snapshot(RpcController controller,
            SnapshotRequest request) throws ServiceException {
          return stub.snapshot(controller, request);
        }

        @Override
        public GetCompletedSnapshotsResponse getCompletedSnapshots(
            RpcController controller, GetCompletedSnapshotsRequest request)
            throws ServiceException {
          return stub.getCompletedSnapshots(controller, request);
        }

        @Override
        public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
            DeleteSnapshotRequest request) throws ServiceException {
          return stub.deleteSnapshot(controller, request);
        }

        @Override
        public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
            IsSnapshotDoneRequest request) throws ServiceException {
          return stub.isSnapshotDone(controller, request);
        }

        @Override
        public RestoreSnapshotResponse restoreSnapshot(
            RpcController controller, RestoreSnapshotRequest request)
            throws ServiceException {
          return stub.restoreSnapshot(controller, request);
        }

        @Override
        public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(
            RpcController controller, IsRestoreSnapshotDoneRequest request)
            throws ServiceException {
          return stub.isRestoreSnapshotDone(controller, request);
        }

        @Override
        public ExecProcedureResponse execProcedure(
            RpcController controller, ExecProcedureRequest request)
            throws ServiceException {
          return stub.execProcedure(controller, request);
        }

        @Override
        public ExecProcedureResponse execProcedureWithRet(
            RpcController controller, ExecProcedureRequest request)
            throws ServiceException {
          return stub.execProcedureWithRet(controller, request);
        }

        @Override
        public IsProcedureDoneResponse isProcedureDone(RpcController controller,
            IsProcedureDoneRequest request) throws ServiceException {
          return stub.isProcedureDone(controller, request);
        }

        @Override
        public IsMasterRunningResponse isMasterRunning(
            RpcController controller, IsMasterRunningRequest request)
            throws ServiceException {
          return stub.isMasterRunning(controller, request);
        }

        @Override
        public ModifyNamespaceResponse modifyNamespace(RpcController controller,
            ModifyNamespaceRequest request)
        throws ServiceException {
          return stub.modifyNamespace(controller, request);
        }

        @Override
        public CreateNamespaceResponse createNamespace(
            RpcController controller, CreateNamespaceRequest request) throws ServiceException {
          return stub.createNamespace(controller, request);
        }

        @Override
        public DeleteNamespaceResponse deleteNamespace(
            RpcController controller, DeleteNamespaceRequest request) throws ServiceException {
          return stub.deleteNamespace(controller, request);
        }

        @Override
        public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller,
            GetNamespaceDescriptorRequest request) throws ServiceException {
          return stub.getNamespaceDescriptor(controller, request);
        }

        @Override
        public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
            ListNamespaceDescriptorsRequest request) throws ServiceException {
          return stub.listNamespaceDescriptors(controller, request);
        }

        @Override
        public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
            RpcController controller, ListTableDescriptorsByNamespaceRequest request)
                throws ServiceException {
          return stub.listTableDescriptorsByNamespace(controller, request);
        }

        @Override
        public ListTableNamesByNamespaceResponse listTableNamesByNamespace(
            RpcController controller, ListTableNamesByNamespaceRequest request)
                throws ServiceException {
          return stub.listTableNamesByNamespace(controller, request);
        }

        @Override
        public GetTableStateResponse getTableState(
                RpcController controller, GetTableStateRequest request)
                throws ServiceException {
          return stub.getTableState(controller, request);
        }

        @Override
        public void close() {
          release(this.mss);
        }

        @Override
        public GetSchemaAlterStatusResponse getSchemaAlterStatus(
            RpcController controller, GetSchemaAlterStatusRequest request)
            throws ServiceException {
          return stub.getSchemaAlterStatus(controller, request);
        }

        @Override
        public GetTableDescriptorsResponse getTableDescriptors(
            RpcController controller, GetTableDescriptorsRequest request)
            throws ServiceException {
          return stub.getTableDescriptors(controller, request);
        }

        @Override
        public GetTableNamesResponse getTableNames(
            RpcController controller, GetTableNamesRequest request)
            throws ServiceException {
          return stub.getTableNames(controller, request);
        }

        @Override
        public GetClusterStatusResponse getClusterStatus(
            RpcController controller, GetClusterStatusRequest request)
            throws ServiceException {
          return stub.getClusterStatus(controller, request);
        }

        @Override
        public SetQuotaResponse setQuota(
            RpcController controller, SetQuotaRequest request)
            throws ServiceException {
          return stub.setQuota(controller, request);
        }
      };
    }


    private static void release(MasterServiceState mss) {
      if (mss != null && mss.connection != null) {
        ((HConnectionImplementation)mss.connection).releaseMaster(mss);
      }
    }

    private boolean isKeepAliveMasterConnectedAndRunning(MasterServiceState mss) {
      if (mss.getStub() == null){
        return false;
      }
      try {
        return mss.isMasterRunning();
      } catch (UndeclaredThrowableException e) {
        // It's somehow messy, but we can receive exceptions such as
        //  java.net.ConnectException but they're not declared. So we catch it...
        LOG.info("Master connection is not running anymore", e.getUndeclaredThrowable());
        return false;
      } catch (ServiceException se) {
        LOG.warn("Checking master connection", se);
        return false;
      }
    }

    void releaseMaster(MasterServiceState mss) {
      if (mss.getStub() == null) return;
      synchronized (masterAndZKLock) {
        --mss.userCount;
        if (mss.userCount <= 0) {
          mss.keepAliveUntil = System.currentTimeMillis() + keepAlive;
        }
      }
    }

    private void closeMasterService(MasterServiceState mss) {
      if (mss.getStub() != null) {
        LOG.info("Closing master protocol: " + mss);
        mss.clearStub();
      }
      mss.userCount = 0;
    }

    /**
     * Immediate close of the shared master. Can be by the delayed close or when closing the
     * connection itself.
     */
    private void closeMaster() {
      synchronized (masterAndZKLock) {
        closeMasterService(masterServiceState);
      }
    }

    void updateCachedLocation(HRegionInfo hri, ServerName source,
                              ServerName serverName, long seqNum) {
      HRegionLocation newHrl = new HRegionLocation(hri, serverName, seqNum);
      cacheLocation(hri.getTable(), source, newHrl);
    }

    @Override
    public void deleteCachedRegionLocation(final HRegionLocation location) {
      metaCache.clearCache(location);
    }

    @Override
    public void updateCachedLocations(final TableName tableName, byte[] rowkey,
        final Object exception, final HRegionLocation source) {
      assert source != null;
      updateCachedLocations(tableName, source.getRegionInfo().getRegionName()
        , rowkey, exception, source.getServerName());
    }

    /**
     * Update the location with the new value (if the exception is a RegionMovedException)
     * or delete it from the cache. Does nothing if we can be sure from the exception that
     * the location is still accurate, or if the cache has already been updated.
     * @param exception an object (to simplify user code) on which we will try to find a nested
     *                  or wrapped or both RegionMovedException
     * @param source server that is the source of the location update.
     */
    @Override
    public void updateCachedLocations(final TableName tableName, byte[] regionName, byte[] rowkey,
      final Object exception, final ServerName source) {
      if (rowkey == null || tableName == null) {
        LOG.warn("Coding error, see method javadoc. row=" + (rowkey == null ? "null" : rowkey) +
            ", tableName=" + (tableName == null ? "null" : tableName));
        return;
      }

      if (source == null) {
        // This should not happen, but let's secure ourselves.
        return;
      }

      if (regionName == null) {
        // we do not know which region, so just remove the cache entry for the row and server
        metaCache.clearCache(tableName, rowkey, source);
        return;
      }

      // Is it something we have already updated?
      final RegionLocations oldLocations = getCachedLocation(tableName, rowkey);
      HRegionLocation oldLocation = null;
      if (oldLocations != null) {
        oldLocation = oldLocations.getRegionLocationByRegionName(regionName);
      }
      if (oldLocation == null || !source.equals(oldLocation.getServerName())) {
        // There is no such location in the cache (it's been removed already) or
        // the cache has already been refreshed with a different location.  => nothing to do
        return;
      }

      HRegionInfo regionInfo = oldLocation.getRegionInfo();
      Throwable cause = findException(exception);
      if (cause != null) {
        if (cause instanceof RegionTooBusyException || cause instanceof RegionOpeningException) {
          // We know that the region is still on this region server
          return;
        }

        if (cause instanceof RegionMovedException) {
          RegionMovedException rme = (RegionMovedException) cause;
          if (LOG.isTraceEnabled()) {
            LOG.trace("Region " + regionInfo.getRegionNameAsString() + " moved to " +
                rme.getHostname() + ":" + rme.getPort() +
                " according to " + source.getHostAndPort());
          }
          // We know that the region is not anymore on this region server, but we know
          //  the new location.
          updateCachedLocation(
              regionInfo, source, rme.getServerName(), rme.getLocationSeqNum());
          return;
        }
      }

      // If we're here, it means that can cannot be sure about the location, so we remove it from
      // the cache. Do not send the source because source can be a new server in the same host:port
      metaCache.clearCache(regionInfo);
    }

    @Override
    public void updateCachedLocations(final byte[] tableName, byte[] rowkey,
      final Object exception, final HRegionLocation source) {
      updateCachedLocations(TableName.valueOf(tableName), rowkey, exception, source);
    }

    @Override
    @Deprecated
    public void processBatch(List<? extends Row> list,
        final TableName tableName,
        ExecutorService pool,
        Object[] results) throws IOException, InterruptedException {
      // This belongs in HTable!!! Not in here.  St.Ack

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException(
          "argument results must be the same size as argument list");
      }
      processBatchCallback(list, tableName, pool, results, null);
    }

    @Override
    @Deprecated
    public void processBatch(List<? extends Row> list,
        final byte[] tableName,
        ExecutorService pool,
        Object[] results) throws IOException, InterruptedException {
      processBatch(list, TableName.valueOf(tableName), pool, results);
    }

    /**
     * Send the queries in parallel on the different region servers. Retries on failures.
     * If the method returns it means that there is no error, and the 'results' array will
     * contain no exception. On error, an exception is thrown, and the 'results' array will
     * contain results and exceptions.
     * @deprecated since 0.96 - Use {@link HTable#processBatchCallback} instead
     */
    @Override
    @Deprecated
    public <R> void processBatchCallback(
      List<? extends Row> list,
      TableName tableName,
      ExecutorService pool,
      Object[] results,
      Batch.Callback<R> callback)
      throws IOException, InterruptedException {

      AsyncRequestFuture ars = this.asyncProcess.submitAll(
          pool, tableName, list, callback, results);
      ars.waitUntilDone();
      if (ars.hasError()) {
        throw ars.getErrors();
      }
    }

    @Override
    @Deprecated
    public <R> void processBatchCallback(
      List<? extends Row> list,
      byte[] tableName,
      ExecutorService pool,
      Object[] results,
      Batch.Callback<R> callback)
      throws IOException, InterruptedException {
      processBatchCallback(list, TableName.valueOf(tableName), pool, results, callback);
    }

    // For tests to override.
    protected AsyncProcess createAsyncProcess(Configuration conf) {
      // No default pool available.
      return new AsyncProcess(this, conf, this.batchPool,
          RpcRetryingCallerFactory.instantiate(conf), false, RpcControllerFactory.instantiate(conf));
    }

    @Override
    public AsyncProcess getAsyncProcess() {
      return asyncProcess;
    }

    /*
     * Return the number of cached region for a table. It will only be called
     * from a unit test.
     */
    @VisibleForTesting
    int getNumberOfCachedRegionLocations(final TableName tableName) {
      return metaCache.getNumberOfCachedRegionLocations(tableName);
    }

    @Override
    @Deprecated
    public void setRegionCachePrefetch(final TableName tableName, final boolean enable) {
    }

    @Override
    @Deprecated
    public void setRegionCachePrefetch(final byte[] tableName,
        final boolean enable) {
    }

    @Override
    @Deprecated
    public boolean getRegionCachePrefetch(TableName tableName) {
      return false;
    }

    @Override
    @Deprecated
    public boolean getRegionCachePrefetch(byte[] tableName) {
      return false;
    }

    @Override
    public void abort(final String msg, Throwable t) {
      if (t instanceof KeeperException.SessionExpiredException
        && keepAliveZookeeper != null) {
        synchronized (masterAndZKLock) {
          if (keepAliveZookeeper != null) {
            LOG.warn("This client just lost it's session with ZooKeeper," +
              " closing it." +
              " It will be recreated next time someone needs it", t);
            closeZooKeeperWatcher();
          }
        }
      } else {
        if (t != null) {
          LOG.fatal(msg, t);
        } else {
          LOG.fatal(msg);
        }
        this.aborted = true;
        close();
        this.closed = true;
      }
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }

    @Override
    public boolean isAborted(){
      return this.aborted;
    }

    @Override
    public int getCurrentNrHRS() throws IOException {
      return this.registry.getCurrentNrHRS();
    }

    /**
     * Increment this client's reference count.
     */
    void incCount() {
      ++refCount;
    }

    /**
     * Decrement this client's reference count.
     */
    void decCount() {
      if (refCount > 0) {
        --refCount;
      }
    }

    /**
     * Return if this client has no reference
     *
     * @return true if this client has no reference; false otherwise
     */
    boolean isZeroReference() {
      return refCount == 0;
    }

    void internalClose() {
      if (this.closed) {
        return;
      }
      delayedClosing.stop("Closing connection");
      closeMaster();
      shutdownBatchPool();
      this.closed = true;
      closeZooKeeperWatcher();
      this.stubs.clear();
      if (clusterStatusListener != null) {
        clusterStatusListener.close();
      }
      if (rpcClient != null) {
        rpcClient.stop();
      }
    }

    @Override
    public void close() {
      if (managed) {
        if (aborted) {
          ConnectionManager.deleteStaleConnection(this);
        } else {
          ConnectionManager.deleteConnection(this, false);
        }
      } else {
        internalClose();
      }
    }

    /**
     * Close the connection for good, regardless of what the current value of
     * {@link #refCount} is. Ideally, {@link #refCount} should be zero at this
     * point, which would be the case if all of its consumers close the
     * connection. However, on the off chance that someone is unable to close
     * the connection, perhaps because it bailed out prematurely, the method
     * below will ensure that this {@link HConnection} instance is cleaned up.
     * Caveat: The JVM may take an unknown amount of time to call finalize on an
     * unreachable object, so our hope is that every consumer cleans up after
     * itself, like any good citizen.
     */
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      // Pretend as if we are about to release the last remaining reference
      refCount = 1;
      close();
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
      MasterKeepAliveConnection master = getKeepAliveMasterService();
      try {
        GetTableDescriptorsRequest req =
          RequestConverter.buildGetTableDescriptorsRequest((List<TableName>)null);
        return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public String[] getTableNames() throws IOException {
      TableName[] tableNames = listTableNames();
      String result[] = new String[tableNames.length];
      for (int i = 0; i < tableNames.length; i++) {
        result[i] = tableNames[i].getNameAsString();
      }
      return result;
    }

    @Override
    public TableName[] listTableNames() throws IOException {
      MasterKeepAliveConnection master = getKeepAliveMasterService();
      try {
        return ProtobufUtil.getTableNameArray(master.getTableNames(null,
            GetTableNamesRequest.newBuilder().build())
          .getTableNamesList());
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public HTableDescriptor[] getHTableDescriptorsByTableName(
        List<TableName> tableNames) throws IOException {
      if (tableNames == null || tableNames.isEmpty()) return new HTableDescriptor[0];
      MasterKeepAliveConnection master = getKeepAliveMasterService();
      try {
        GetTableDescriptorsRequest req =
          RequestConverter.buildGetTableDescriptorsRequest(tableNames);
        return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public HTableDescriptor[] getHTableDescriptors(
        List<String> names) throws IOException {
      List<TableName> tableNames = new ArrayList<TableName>(names.size());
      for(String name : names) {
        tableNames.add(TableName.valueOf(name));
      }

      return getHTableDescriptorsByTableName(tableNames);
    }

    @Override
    public NonceGenerator getNonceGenerator() {
      return this.nonceGenerator;
    }

    /**
     * Connects to the master to get the table descriptor.
     * @param tableName table name
     * @throws IOException if the connection to master fails or if the table
     *  is not found.
     */
    @Override
    public HTableDescriptor getHTableDescriptor(final TableName tableName)
    throws IOException {
      if (tableName == null) return null;
      MasterKeepAliveConnection master = getKeepAliveMasterService();
      GetTableDescriptorsResponse htds;
      try {
        GetTableDescriptorsRequest req =
          RequestConverter.buildGetTableDescriptorsRequest(tableName);
        htds = master.getTableDescriptors(null, req);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
      if (!htds.getTableSchemaList().isEmpty()) {
        return HTableDescriptor.convert(htds.getTableSchemaList().get(0));
      }
      throw new TableNotFoundException(tableName.getNameAsString());
    }

    @Override
    public HTableDescriptor getHTableDescriptor(final byte[] tableName)
    throws IOException {
      return getHTableDescriptor(TableName.valueOf(tableName));
    }

    @Override
    public TableState getTableState(TableName tableName) throws IOException {
      MasterKeepAliveConnection master = getKeepAliveMasterService();
      try {
        GetTableStateResponse resp = master.getTableState(null,
                RequestConverter.buildGetTableStateRequest(tableName));
        return TableState.convert(resp.getTableState());
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf) {
      return RpcRetryingCallerFactory.instantiate(conf, this.interceptor);
    }
  }

  /**
   * The record of errors for servers.
   */
  static class ServerErrorTracker {
    // We need a concurrent map here, as we could have multiple threads updating it in parallel.
    private final ConcurrentMap<ServerName, ServerErrors> errorsByServer =
        new ConcurrentHashMap<ServerName, ServerErrors>();
    private final long canRetryUntil;
    private final int maxRetries;
    private final long startTrackingTime;

    public ServerErrorTracker(long timeout, int maxRetries) {
      this.maxRetries = maxRetries;
      this.canRetryUntil = EnvironmentEdgeManager.currentTime() + timeout;
      this.startTrackingTime = new Date().getTime();
    }

    /**
     * We stop to retry when we have exhausted BOTH the number of retries and the time allocated.
     */
    boolean canRetryMore(int numRetry) {
      // If there is a single try we must not take into account the time.
      return numRetry < maxRetries || (maxRetries > 1 &&
          EnvironmentEdgeManager.currentTime() < this.canRetryUntil);
    }

    /**
     * Calculates the back-off time for a retrying request to a particular server.
     *
     * @param server    The server in question.
     * @param basePause The default hci pause.
     * @return The time to wait before sending next request.
     */
    long calculateBackoffTime(ServerName server, long basePause) {
      long result;
      ServerErrors errorStats = errorsByServer.get(server);
      if (errorStats != null) {
        result = ConnectionUtils.getPauseTime(basePause, errorStats.retries.get());
      } else {
        result = 0; // yes, if the server is not in our list we don't wait before retrying.
      }
      return result;
    }

    /**
     * Reports that there was an error on the server to do whatever bean-counting necessary.
     *
     * @param server The server in question.
     */
    void reportServerError(ServerName server) {
      ServerErrors errors = errorsByServer.get(server);
      if (errors != null) {
        errors.addError();
      } else {
        errors = errorsByServer.putIfAbsent(server, new ServerErrors());
        if (errors != null){
          errors.addError();
        }
      }
    }

    long getStartTrackingTime() {
      return startTrackingTime;
    }

    /**
     * The record of errors for a server.
     */
    private static class ServerErrors {
      public final AtomicInteger retries = new AtomicInteger(0);

      public void addError() {
        retries.incrementAndGet();
      }
    }
  }

  /**
   * Look for an exception we know in the remote exception:
   * - hadoop.ipc wrapped exceptions
   * - nested exceptions
   *
   * Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (cur instanceof RegionMovedException || cur instanceof RegionOpeningException
          || cur instanceof RegionTooBusyException) {
        return cur;
      }
      if (cur instanceof RemoteException) {
        RemoteException re = (RemoteException) cur;
        cur = re.unwrapRemoteException(
            RegionOpeningException.class, RegionMovedException.class,
            RegionTooBusyException.class);
        if (cur == null) {
          cur = re.unwrapRemoteException();
        }
        // unwrapRemoteException can return the exception given as a parameter when it cannot
        //  unwrap it. In this case, there is no need to look further
        // noinspection ObjectEquality
        if (cur == re) {
          return null;
        }
      } else {
        cur = cur.getCause();
      }
    }

    return null;
  }
}
