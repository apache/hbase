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

import static org.apache.hadoop.hbase.client.ConnectionUtils.NO_NONCE_GENERATOR;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getStubKey;
import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;
import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsentEx;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicyFactory;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.BlockingInterface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;

/**
 * Main implementation of {@link Connection} and {@link ClusterConnection} interfaces.
 * Encapsulates connection to zookeeper and regionservers.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
    justification="Access to the conncurrent hash map is under a lock so should be fine.")
@InterfaceAudience.Private
class ConnectionImplementation implements ClusterConnection, Closeable {
  public static final String RETRIES_BY_SERVER_KEY = "hbase.client.retries.by.server";
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionImplementation.class);

  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final boolean hostnamesCanChange;
  private final long pause;
  private final long pauseForCQTBE;// pause for CallQueueTooBigException, if specified
  private boolean useMetaReplicas;
  private final int metaReplicaCallTimeoutScanInMicroSecond;
  private final int numTries;
  final int rpcTimeout;

  /**
   * Global nonceGenerator shared per client.Currently there's no reason to limit its scope.
   * Once it's set under nonceGeneratorCreateLock, it is never unset or changed.
   */
  private static volatile NonceGenerator nonceGenerator = null;
  /** The nonce generator lock. Only taken when creating Connection, which gets a private copy. */
  private static final Object nonceGeneratorCreateLock = new Object();

  private final AsyncProcess asyncProcess;
  // single tracker per connection
  private final ServerStatisticTracker stats;

  private volatile boolean closed;
  private volatile boolean aborted;

  // package protected for the tests
  ClusterStatusListener clusterStatusListener;

  private final Object metaRegionLock = new Object();

  private final Object masterLock = new Object();

  // thread executor shared by all Table instances created
  // by this connection
  private volatile ThreadPoolExecutor batchPool = null;
  // meta thread executor shared by all Table instances created
  // by this connection
  private volatile ThreadPoolExecutor metaLookupPool = null;
  private volatile boolean cleanupPool = false;

  private final Configuration conf;

  // cache the configuration value for tables so that we can avoid calling
  // the expensive Configuration to fetch the value multiple times.
  private final ConnectionConfiguration connectionConfig;

  // Client rpc instance.
  private final RpcClient rpcClient;

  private final MetaCache metaCache;
  private final MetricsConnection metrics;

  protected User user;

  private final RpcRetryingCallerFactory rpcCallerFactory;

  private final RpcControllerFactory rpcControllerFactory;

  private final RetryingCallerInterceptor interceptor;

  /**
   * Cluster registry of basic info such as clusterid and meta region location.
   */
  private final ConnectionRegistry registry;

  private final ClientBackoffPolicy backoffPolicy;

  /**
   * Allow setting an alternate BufferedMutator implementation via
   * config. If null, use default.
   */
  private final String alternateBufferedMutatorClassName;

  /** lock guards against multiple threads trying to query the meta region at the same time */
  private final ReentrantLock userRegionLock = new ReentrantLock();

  private ChoreService authService;

  /**
   * constructor
   * @param conf Configuration object
   */
  ConnectionImplementation(Configuration conf, ExecutorService pool, User user) throws IOException {
    this.conf = conf;
    this.user = user;
    if (user != null && user.isLoginFromKeytab()) {
      spawnRenewalChore(user.getUGI());
    }
    this.batchPool = (ThreadPoolExecutor) pool;
    this.connectionConfig = new ConnectionConfiguration(conf);
    this.closed = false;
    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    long configuredPauseForCQTBE = conf.getLong(HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE, pause);
    if (configuredPauseForCQTBE < pause) {
      LOG.warn("The " + HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE + " setting: "
          + configuredPauseForCQTBE + " is smaller than " + HConstants.HBASE_CLIENT_PAUSE
          + ", will use " + pause + " instead.");
      this.pauseForCQTBE = pause;
    } else {
      this.pauseForCQTBE = configuredPauseForCQTBE;
    }
    this.useMetaReplicas = conf.getBoolean(HConstants.USE_META_REPLICAS,
      HConstants.DEFAULT_USE_META_REPLICAS);
    this.metaReplicaCallTimeoutScanInMicroSecond =
        connectionConfig.getMetaReplicaCallTimeoutMicroSecondScan();

    // how many times to try, one more than max *retry* time
    this.numTries = retries2Attempts(connectionConfig.getRetriesNumber());
    this.rpcTimeout = conf.getInt(
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    if (conf.getBoolean(NonceGenerator.CLIENT_NONCES_ENABLED_KEY, true)) {
      synchronized (nonceGeneratorCreateLock) {
        if (nonceGenerator == null) {
          nonceGenerator = PerClientRandomNonceGenerator.get();
        }
      }
    } else {
      nonceGenerator = NO_NONCE_GENERATOR;
    }

    this.stats = ServerStatisticTracker.create(conf);
    this.interceptor = (new RetryingCallerInterceptorFactory(conf)).build();
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, interceptor, this.stats);
    this.backoffPolicy = ClientBackoffPolicyFactory.create(conf);
    this.asyncProcess = new AsyncProcess(this, conf, rpcCallerFactory, rpcControllerFactory);
    if (conf.getBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, false)) {
      this.metrics =
        new MetricsConnection(this.toString(), this::getBatchPool, this::getMetaLookupPool);
    } else {
      this.metrics = null;
    }
    this.metaCache = new MetaCache(this.metrics);

    boolean shouldListen = conf.getBoolean(HConstants.STATUS_PUBLISHED,
        HConstants.STATUS_PUBLISHED_DEFAULT);
    this.hostnamesCanChange = conf.getBoolean(RESOLVE_HOSTNAME_ON_FAIL_KEY, true);
    Class<? extends ClusterStatusListener.Listener> listenerClass =
        conf.getClass(ClusterStatusListener.STATUS_LISTENER_CLASS,
            ClusterStatusListener.DEFAULT_STATUS_LISTENER_CLASS,
            ClusterStatusListener.Listener.class);

    // Is there an alternate BufferedMutator to use?
    this.alternateBufferedMutatorClassName =
        this.conf.get(BufferedMutator.CLASSNAME_KEY);

    try {
      this.registry = ConnectionRegistryFactory.getRegistry(conf);
      retrieveClusterId();

      this.rpcClient = RpcClientFactory.createClient(this.conf, this.clusterId, this.metrics);

      // Do we publish the status?
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
                  rpcClient.cancelConnections(sn);
                }
              }, conf, listenerClass);
        }
      }
    } catch (Throwable e) {
      // avoid leaks: registry, rpcClient, ...
      LOG.debug("connection construction failed", e);
      close();
      throw e;
    }
  }

  private void spawnRenewalChore(final UserGroupInformation user) {
    authService = new ChoreService("Relogin service");
    authService.scheduleChore(AuthUtil.getAuthRenewalChore(user));
  }

  /**
   * @param useMetaReplicas
   */
  @VisibleForTesting
  void setUseMetaReplicas(final boolean useMetaReplicas) {
    this.useMetaReplicas = useMetaReplicas;
  }

  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  @VisibleForTesting
  static NonceGenerator injectNonceGeneratorForTesting(
      ClusterConnection conn, NonceGenerator cnm) {
    ConnectionImplementation connImpl = (ConnectionImplementation)conn;
    NonceGenerator ng = connImpl.getNonceGenerator();
    LOG.warn("Nonce generator is being replaced by test code for "
      + cnm.getClass().getName());
    nonceGenerator = cnm;
    return ng;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return getTable(tableName, getBatchPool());
  }

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
    return new TableBuilderBase(tableName, connectionConfig) {

      @Override
      public Table build() {
        return new HTable(ConnectionImplementation.this, this, rpcCallerFactory,
            rpcControllerFactory, pool);
      }
    };
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) {
    if (params.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    }
    if (params.getPool() == null) {
      params.pool(HTable.getDefaultExecutor(getConfiguration()));
    }
    if (params.getWriteBufferSize() == BufferedMutatorParams.UNSET) {
      params.writeBufferSize(connectionConfig.getWriteBufferSize());
    }
    if (params.getWriteBufferPeriodicFlushTimeoutMs() == BufferedMutatorParams.UNSET) {
      params.setWriteBufferPeriodicFlushTimeoutMs(
              connectionConfig.getWriteBufferPeriodicFlushTimeoutMs());
    }
    if (params.getWriteBufferPeriodicFlushTimerTickMs() == BufferedMutatorParams.UNSET) {
      params.setWriteBufferPeriodicFlushTimerTickMs(
              connectionConfig.getWriteBufferPeriodicFlushTimerTickMs());
    }
    if (params.getMaxKeyValueSize() == BufferedMutatorParams.UNSET) {
      params.maxKeyValueSize(connectionConfig.getMaxKeyValueSize());
    }
    // Look to see if an alternate BufferedMutation implementation is wanted.
    // Look in params and in config. If null, use default.
    String implementationClassName = params.getImplementationClassName();
    if (implementationClassName == null) {
      implementationClassName = this.alternateBufferedMutatorClassName;
    }
    if (implementationClassName == null) {
      return new BufferedMutatorImpl(this, rpcCallerFactory, rpcControllerFactory, params);
    }
    try {
      return (BufferedMutator)ReflectionUtils.newInstance(Class.forName(implementationClassName),
          this, rpcCallerFactory, rpcControllerFactory, params);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return new HRegionLocator(tableName, this);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return new HBaseAdmin(this);
  }

  @Override
  public Hbck getHbck() throws IOException {
    return getHbck(get(registry.getActiveMaster()));
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    checkClosed();
    if (isDeadServer(masterServer)) {
      throw new RegionServerStoppedException(masterServer + " is dead.");
    }
    String key = getStubKey(MasterProtos.HbckService.BlockingInterface.class.getName(),
      masterServer, this.hostnamesCanChange);

    return new HBaseHbck(
      (MasterProtos.HbckService.BlockingInterface) computeIfAbsentEx(stubs, key, () -> {
        BlockingRpcChannel channel =
          this.rpcClient.createBlockingRpcChannel(masterServer, user, rpcTimeout);
        return MasterProtos.HbckService.newBlockingStub(channel);
      }), rpcControllerFactory);
  }

  @Override
  public MetricsConnection getConnectionMetrics() {
    return this.metrics;
  }

  private ThreadPoolExecutor getBatchPool() {
    if (batchPool == null) {
      synchronized (this) {
        if (batchPool == null) {
          int threads = conf.getInt("hbase.hconnection.threads.max", 256);
          this.batchPool = getThreadPool(threads, threads, "-shared", null);
          this.cleanupPool = true;
        }
      }
    }
    return this.batchPool;
  }

  private ThreadPoolExecutor getThreadPool(int maxThreads, int coreThreads, String nameHint,
      BlockingQueue<Runnable> passedWorkQueue) {
    // shared HTable thread executor not yet initialized
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
    BlockingQueue<Runnable> workQueue = passedWorkQueue;
    if (workQueue == null) {
      workQueue =
        new LinkedBlockingQueue<>(maxThreads *
            conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
      coreThreads = maxThreads;
    }
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(
        coreThreads,
        maxThreads,
        keepAliveTime,
        TimeUnit.SECONDS,
        workQueue,
        Threads.newDaemonThreadFactory(toString() + nameHint));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  private ThreadPoolExecutor getMetaLookupPool() {
    if (this.metaLookupPool == null) {
      synchronized (this) {
        if (this.metaLookupPool == null) {
          //Some of the threads would be used for meta replicas
          //To start with, threads.max.core threads can hit the meta (including replicas).
          //After that, requests will get queued up in the passed queue, and only after
          //the queue is full, a new thread will be started
          int threads = conf.getInt("hbase.hconnection.meta.lookup.threads.max", 128);
          this.metaLookupPool = getThreadPool(
             threads,
             threads,
             "-metaLookup-shared-", new LinkedBlockingQueue<>());
        }
      }
    }
    return this.metaLookupPool;
  }

  protected ExecutorService getCurrentMetaLookupPool() {
    return metaLookupPool;
  }

  protected ExecutorService getCurrentBatchPool() {
    return batchPool;
  }

  private void shutdownPools() {
    if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
      shutdownBatchPool(this.batchPool);
    }
    if (this.metaLookupPool != null && !this.metaLookupPool.isShutdown()) {
      shutdownBatchPool(this.metaLookupPool);
    }
  }

  private void shutdownBatchPool(ExecutorService pool) {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      pool.shutdownNow();
    }
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

  protected void retrieveClusterId() {
    if (clusterId != null) {
      return;
    }
    try {
      this.clusterId = this.registry.getClusterId().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn("Retrieve cluster id failed", e);
    }
    if (clusterId == null) {
      clusterId = HConstants.CLUSTER_ID_DEFAULT;
      LOG.debug("clusterid came back null, using default " + clusterId);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  private void checkClosed() throws LocalConnectionClosedException {
    if (this.closed) {
      throw new LocalConnectionClosedException(toString() + " closed");
    }
  }

  /**
   * Like {@link ConnectionClosedException} but thrown from the checkClosed call which looks
   * at the local this.closed flag. We use this rather than {@link ConnectionClosedException}
   * because the latter does not inherit from DoNotRetryIOE (it should. TODO).
   */
  private static class LocalConnectionClosedException extends DoNotRetryIOException {
    LocalConnectionClosedException(String message) {
      super(message);
    }
  }

  /**
   * @return true if the master is running, throws an exception otherwise
   * @throws org.apache.hadoop.hbase.MasterNotRunningException - if the master is not running
   * @deprecated this has been deprecated without a replacement
   */
  @Deprecated
  @Override
  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    // When getting the master connection, we check it's running,
    // so if there is no exception, it means we've been able to get a
    // connection on a running master
    MasterKeepAliveConnection m;
    try {
      m = getKeepAliveMasterService();
    } catch (IOException e) {
      throw new MasterNotRunningException(e);
    }
    m.close();
    return true;
  }

  @Override
  public HRegionLocation getRegionLocation(final TableName tableName, final byte[] row,
      boolean reload) throws IOException {
    return reload ? relocateRegion(tableName, row) : locateRegion(tableName, row);
  }


  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return getTableState(tableName).inStates(TableState.State.ENABLED);
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return getTableState(tableName).inStates(TableState.State.DISABLED);
  }

  @Override
  public boolean isTableAvailable(final TableName tableName, @Nullable final byte[][] splitKeys)
      throws IOException {
    checkClosed();
    try {
      if (!isTableEnabled(tableName)) {
        LOG.debug("Table {} not enabled", tableName);
        return false;
      }
      if (TableName.isMetaTableName(tableName)) {
        // meta table is always available
        return true;
      }
      List<Pair<RegionInfo, ServerName>> locations =
        MetaTableAccessor.getTableRegionsAndLocations(this, tableName, true);

      int notDeployed = 0;
      int regionCount = 0;
      for (Pair<RegionInfo, ServerName> pair : locations) {
        RegionInfo info = pair.getFirst();
        if (pair.getSecond() == null) {
          LOG.debug("Table {} has not deployed region {}", tableName,
              pair.getFirst().getEncodedName());
          notDeployed++;
        } else if (splitKeys != null
            && !Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
          for (byte[] splitKey : splitKeys) {
            // Just check if the splitkey is available
            if (Bytes.equals(info.getStartKey(), splitKey)) {
              regionCount++;
              break;
            }
          }
        } else {
          // Always empty start row should be counted
          regionCount++;
        }
      }
      if (notDeployed > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table {} has {} regions not deployed", tableName, notDeployed);
        }
        return false;
      } else if (splitKeys != null && regionCount != splitKeys.length + 1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table {} expected to have {} regions, but only {} available", tableName,
              splitKeys.length + 1, regionCount);
        }
        return false;
      } else {
        LOG.trace("Table {} should be available", tableName);
        return true;
      }
    } catch (TableNotFoundException tnfe) {
      LOG.warn("Table {} does not exist", tableName);
      return false;
    }
  }

  @Override
  public HRegionLocation locateRegion(final byte[] regionName) throws IOException {
    RegionLocations locations = locateRegion(RegionInfo.getTable(regionName),
      RegionInfo.getStartKey(regionName), false, true);
    return locations == null ? null : locations.getRegionLocation();
  }

  private boolean isDeadServer(ServerName sn) {
    if (clusterStatusListener == null) {
      return false;
    } else {
      return clusterStatusListener.isDeadServer(sn);
    }
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName) throws IOException {
    return locateRegions(tableName, false, true);
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName, boolean useCache,
      boolean offlined) throws IOException {
    List<RegionInfo> regions;
    if (TableName.isMetaTableName(tableName)) {
      regions = Collections.singletonList(RegionInfoBuilder.FIRST_META_REGIONINFO);
    } else {
      regions = MetaTableAccessor.getTableRegions(this, tableName, !offlined);
    }
    List<HRegionLocation> locations = new ArrayList<>();
    for (RegionInfo regionInfo : regions) {
      if (!RegionReplicaUtil.isDefaultReplica(regionInfo)) {
        continue;
      }
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
  public HRegionLocation locateRegion(final TableName tableName, final byte[] row)
      throws IOException {
    RegionLocations locations = locateRegion(tableName, row, true, true);
    return locations == null ? null : locations.getRegionLocation();
  }

  @Override
  public HRegionLocation relocateRegion(final TableName tableName, final byte[] row)
      throws IOException {
    RegionLocations locations =
      relocateRegion(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID);
    return locations == null ? null
      : locations.getRegionLocation(RegionReplicaUtil.DEFAULT_REPLICA_ID);
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
  public RegionLocations locateRegion(final TableName tableName, final byte[] row, boolean useCache,
      boolean retry) throws IOException {
    return locateRegion(tableName, row, useCache, retry, RegionReplicaUtil.DEFAULT_REPLICA_ID);
  }

  @Override
  public RegionLocations locateRegion(final TableName tableName, final byte[] row, boolean useCache,
      boolean retry, int replicaId) throws IOException {
    checkClosed();
    if (tableName == null || tableName.getName().length == 0) {
      throw new IllegalArgumentException("table name cannot be null or zero length");
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
      if (locations != null && locations.getRegionLocation(replicaId) != null) {
        return locations;
      }
    }

    // only one thread should do the lookup.
    synchronized (metaRegionLock) {
      // Check the cache again for a hit in case some other thread made the
      // same query while we were waiting on the lock.
      if (useCache) {
        locations = getCachedLocation(tableName, metaCacheKey);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
      }

      // Look up from zookeeper
      locations = get(this.registry.getMetaRegionLocations());
      if (locations != null) {
        cacheLocation(tableName, locations);
      }
    }
    return locations;
  }

  /**
   * Search the hbase:meta table for the HRegionLocation info that contains the table and row we're
   * seeking.
   */
  private RegionLocations locateRegionInMeta(TableName tableName, byte[] row, boolean useCache,
      boolean retry, int replicaId) throws IOException {
    // If we are supposed to be using the cache, look in the cache to see if we already have the
    // region.
    if (useCache) {
      RegionLocations locations = getCachedLocation(tableName, row);
      if (locations != null && locations.getRegionLocation(replicaId) != null) {
        return locations;
      }
    }
    // build the key of the meta region we should be looking for.
    // the extra 9's on the end are necessary to allow "exact" matches
    // without knowing the precise region names.
    byte[] metaStartKey = RegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    byte[] metaStopKey =
      RegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW, "", false);
    Scan s = new Scan().withStartRow(metaStartKey).withStopRow(metaStopKey, true)
      .addFamily(HConstants.CATALOG_FAMILY).setReversed(true).setCaching(5)
      .setReadType(ReadType.PREAD);
    if (this.useMetaReplicas) {
      s.setConsistency(Consistency.TIMELINE);
    }
    int maxAttempts = (retry ? numTries : 1);
    boolean relocateMeta = false;
    for (int tries = 0; ; tries++) {
      if (tries >= maxAttempts) {
        throw new NoServerForRegionException("Unable to find region for "
            + Bytes.toStringBinary(row) + " in " + tableName + " after " + tries + " tries.");
      }
      if (useCache) {
        RegionLocations locations = getCachedLocation(tableName, row);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
      } else {
        // If we are not supposed to be using the cache, delete any existing cached location
        // so it won't interfere.
        // We are only supposed to clean the cache for the specific replicaId
        metaCache.clearCache(tableName, row, replicaId);
      }
      // Query the meta region
      long pauseBase = this.pause;
      takeUserRegionLock();
      try {
        // We don't need to check if useCache is enabled or not. Even if useCache is false
        // we already cleared the cache for this row before acquiring userRegion lock so if this
        // row is present in cache that means some other thread has populated it while we were
        // waiting to acquire user region lock.
        RegionLocations locations = getCachedLocation(tableName, row);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
        if (relocateMeta) {
          relocateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
            RegionInfo.DEFAULT_REPLICA_ID);
        }
        s.resetMvccReadPoint();
        try (ReversedClientScanner rcs =
          new ReversedClientScanner(conf, s, TableName.META_TABLE_NAME, this, rpcCallerFactory,
            rpcControllerFactory, getMetaLookupPool(), metaReplicaCallTimeoutScanInMicroSecond)) {
          boolean tableNotFound = true;
          for (;;) {
            Result regionInfoRow = rcs.next();
            if (regionInfoRow == null) {
              if (tableNotFound) {
                throw new TableNotFoundException(tableName);
              } else {
                throw new IOException(
                  "Unable to find region for " + Bytes.toStringBinary(row) + " in " + tableName);
              }
            }
            tableNotFound = false;
            // convert the row result into the HRegionLocation we need!
            locations = MetaTableAccessor.getRegionLocations(regionInfoRow);
            if (locations == null || locations.getRegionLocation(replicaId) == null) {
              throw new IOException("RegionInfo null in " + tableName + ", row=" + regionInfoRow);
            }
            RegionInfo regionInfo = locations.getRegionLocation(replicaId).getRegion();
            if (regionInfo == null) {
              throw new IOException("RegionInfo null or empty in " + TableName.META_TABLE_NAME +
                ", row=" + regionInfoRow);
            }
            // See HBASE-20182. It is possible that we locate to a split parent even after the
            // children are online, so here we need to skip this region and go to the next one.
            if (regionInfo.isSplitParent()) {
              continue;
            }
            if (regionInfo.isOffline()) {
              throw new RegionOfflineException("Region offline; disable table call? " +
                  regionInfo.getRegionNameAsString());
            }
            // It is possible that the split children have not been online yet and we have skipped
            // the parent in the above condition, so we may have already reached a region which does
            // not contains us.
            if (!regionInfo.containsRow(row)) {
              throw new IOException(
                "Unable to find region for " + Bytes.toStringBinary(row) + " in " + tableName);
            }
            ServerName serverName = locations.getRegionLocation(replicaId).getServerName();
            if (serverName == null) {
              throw new NoServerForRegionException("No server address listed in " +
                TableName.META_TABLE_NAME + " for region " + regionInfo.getRegionNameAsString() +
                " containing row " + Bytes.toStringBinary(row));
            }
            if (isDeadServer(serverName)) {
              throw new RegionServerStoppedException(
                "hbase:meta says the region " + regionInfo.getRegionNameAsString() +
                  " is managed by the server " + serverName + ", but it is dead.");
            }
            // Instantiate the location
            cacheLocation(tableName, locations);
            return locations;
          }
        }
      } catch (TableNotFoundException e) {
        // if we got this error, probably means the table just plain doesn't
        // exist. rethrow the error immediately. this should always be coming
        // from the HTable constructor.
        throw e;
      } catch (LocalConnectionClosedException cce) {
        // LocalConnectionClosedException is specialized instance of DoNotRetryIOE.
        // Thrown when we check if this connection is closed. If it is, don't retry.
        throw cce;
      } catch (IOException e) {
        ExceptionUtil.rethrowIfInterrupt(e);
        if (e instanceof RemoteException) {
          e = ((RemoteException)e).unwrapRemoteException();
        }
        if (e instanceof CallQueueTooBigException) {
          // Give a special check on CallQueueTooBigException, see #HBASE-17114
          pauseBase = this.pauseForCQTBE;
        }
        if (tries < maxAttempts - 1) {
          LOG.debug("locateRegionInMeta parentTable='{}', attempt={} of {} failed; retrying " +
            "after sleep of {}", TableName.META_TABLE_NAME, tries, maxAttempts, maxAttempts, e);
        } else {
          throw e;
        }
        // Only relocate the parent region if necessary
        relocateMeta =
          !(e instanceof RegionOfflineException || e instanceof NoServerForRegionException);
      } finally {
        userRegionLock.unlock();
      }
      try{
        Thread.sleep(ConnectionUtils.getPauseTime(pauseBase, tries));
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Giving up trying to location region in " +
          "meta: thread is interrupted.");
      }
    }
  }

  void takeUserRegionLock() throws IOException {
    try {
      long waitTime = connectionConfig.getMetaOperationTimeout();
      if (!userRegionLock.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
        throw new LockTimeoutException("Failed to get user region lock in"
            + waitTime + " ms. " + " for accessing meta region server.");
      }
    } catch (InterruptedException ie) {
      LOG.error("Interrupted while waiting for a lock", ie);
      throw ExceptionUtil.asInterrupt(ie);
    }
  }

  /**
   * Put a newly discovered HRegionLocation into the cache.
   * @param tableName The table name.
   * @param location the new location
   */
  @Override
  public void cacheLocation(final TableName tableName, final RegionLocations location) {
    metaCache.cacheLocation(tableName, location);
  }

  /**
   * Search the cache for a location that fits our table and row key.
   * Return null if no suitable region is located.
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
  public void clearRegionLocationCache() {
    metaCache.clearCache();
  }

  @Override
  public void clearRegionCache(final TableName tableName) {
    metaCache.clearCache(tableName);
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
  private final ConcurrentMap<String, Object> stubs = new ConcurrentHashMap<>();

  /**
   * State of the MasterService connection/setup.
   */
  static class MasterServiceState {
    Connection connection;

    MasterProtos.MasterService.BlockingInterface stub;
    int userCount;

    MasterServiceState(final Connection connection) {
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

    boolean isMasterRunning() throws IOException {
      MasterProtos.IsMasterRunningResponse response = null;
      try {
        response = this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      } catch (Exception e) {
        throw ProtobufUtil.handleRemoteException(e);
      }
      return response != null? response.getIsMasterRunning(): false;
    }
  }

  /**
   * The record of errors for servers.
   */
  static class ServerErrorTracker {
    // We need a concurrent map here, as we could have multiple threads updating it in parallel.
    private final ConcurrentMap<ServerName, ServerErrors> errorsByServer = new ConcurrentHashMap<>();
    private final long canRetryUntil;
    private final int maxTries;// max number to try
    private final long startTrackingTime;

    /**
     * Constructor
     * @param timeout how long to wait before timeout, in unit of millisecond
     * @param maxTries how many times to try
     */
    public ServerErrorTracker(long timeout, int maxTries) {
      this.maxTries = maxTries;
      this.canRetryUntil = EnvironmentEdgeManager.currentTime() + timeout;
      this.startTrackingTime = new Date().getTime();
    }

    /**
     * We stop to retry when we have exhausted BOTH the number of tries and the time allocated.
     * @param numAttempt how many times we have tried by now
     */
    boolean canTryMore(int numAttempt) {
      // If there is a single try we must not take into account the time.
      return numAttempt < maxTries || (maxTries > 1 &&
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
        result = ConnectionUtils.getPauseTime(basePause, Math.max(0, errorStats.getCount() - 1));
      } else {
        result = 0; // yes, if the server is not in our list we don't wait before retrying.
      }
      return result;
    }

    /**
     * Reports that there was an error on the server to do whatever bean-counting necessary.
     * @param server The server in question.
     */
    void reportServerError(ServerName server) {
      computeIfAbsent(errorsByServer, server, ServerErrors::new).addError();
    }

    long getStartTrackingTime() {
      return startTrackingTime;
    }

    /**
     * The record of errors for a server.
     */
    private static class ServerErrors {
      private final AtomicInteger retries = new AtomicInteger(0);

      public int getCount() {
        return retries.get();
      }

      public void addError() {
        retries.incrementAndGet();
      }
    }
  }

  /**
   * Class to make a MasterServiceStubMaker stub.
   */
  private final class MasterServiceStubMaker {

    private void isMasterRunning(MasterProtos.MasterService.BlockingInterface stub)
        throws IOException {
      try {
        stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      } catch (ServiceException e) {
        throw ProtobufUtil.handleRemoteException(e);
      }
    }

    /**
     * Create a stub. Try once only. It is not typed because there is no common type to protobuf
     * services nor their interfaces. Let the caller do appropriate casting.
     * @return A stub for master services.
     */
    private MasterProtos.MasterService.BlockingInterface makeStubNoRetries()
        throws IOException, KeeperException {
      ServerName sn = get(registry.getActiveMaster());
      if (sn == null) {
        String msg = "ZooKeeper available but no active master location found";
        LOG.info(msg);
        throw new MasterNotRunningException(msg);
      }
      if (isDeadServer(sn)) {
        throw new MasterNotRunningException(sn + " is dead.");
      }
      // Use the security info interface name as our stub key
      String key =
          getStubKey(MasterProtos.MasterService.getDescriptor().getName(), sn, hostnamesCanChange);
      MasterProtos.MasterService.BlockingInterface stub =
          (MasterProtos.MasterService.BlockingInterface) computeIfAbsentEx(stubs, key, () -> {
            BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
            return MasterProtos.MasterService.newBlockingStub(channel);
          });
      isMasterRunning(stub);
      return stub;
    }

    /**
     * Create a stub against the master. Retry if necessary.
     * @return A stub to do <code>intf</code> against the master
     * @throws org.apache.hadoop.hbase.MasterNotRunningException if master is not running
     */
    MasterProtos.MasterService.BlockingInterface makeStub() throws IOException {
      // The lock must be at the beginning to prevent multiple master creations
      // (and leaks) in a multithread context
      synchronized (masterLock) {
        Exception exceptionCaught = null;
        if (!closed) {
          try {
            return makeStubNoRetries();
          } catch (IOException e) {
            exceptionCaught = e;
          } catch (KeeperException e) {
            exceptionCaught = e;
          }
          throw new MasterNotRunningException(exceptionCaught);
        } else {
          throw new DoNotRetryIOException("Connection was closed while trying to get master");
        }
      }
    }
  }

  @Override
  public AdminProtos.AdminService.BlockingInterface getAdminForMaster() throws IOException {
    return getAdmin(get(registry.getActiveMaster()));
  }

  @Override
  public AdminProtos.AdminService.BlockingInterface getAdmin(ServerName serverName)
      throws IOException {
    checkClosed();
    if (isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    }
    String key = getStubKey(AdminProtos.AdminService.BlockingInterface.class.getName(), serverName,
      this.hostnamesCanChange);
    return (AdminProtos.AdminService.BlockingInterface) computeIfAbsentEx(stubs, key, () -> {
      BlockingRpcChannel channel =
          this.rpcClient.createBlockingRpcChannel(serverName, user, rpcTimeout);
      return AdminProtos.AdminService.newBlockingStub(channel);
    });
  }

  @Override
  public BlockingInterface getClient(ServerName serverName) throws IOException {
    checkClosed();
    if (isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    }
    String key = getStubKey(ClientProtos.ClientService.BlockingInterface.class.getName(),
      serverName, this.hostnamesCanChange);
    return (ClientProtos.ClientService.BlockingInterface) computeIfAbsentEx(stubs, key, () -> {
      BlockingRpcChannel channel =
          this.rpcClient.createBlockingRpcChannel(serverName, user, rpcTimeout);
      return ClientProtos.ClientService.newBlockingStub(channel);
    });
  }

  final MasterServiceState masterServiceState = new MasterServiceState(this);

  @Override
  public MasterKeepAliveConnection getMaster() throws IOException {
    return getKeepAliveMasterService();
  }

  private void resetMasterServiceState(final MasterServiceState mss) {
    mss.userCount++;
  }

  private MasterKeepAliveConnection getKeepAliveMasterService() throws IOException {
    synchronized (masterLock) {
      if (!isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
        MasterServiceStubMaker stubMaker = new MasterServiceStubMaker();
        this.masterServiceState.stub = stubMaker.makeStub();
      }
      resetMasterServiceState(this.masterServiceState);
    }
    // Ugly delegation just so we can add in a Close method.
    final MasterProtos.MasterService.BlockingInterface stub = this.masterServiceState.stub;
    return new MasterKeepAliveConnection() {
      MasterServiceState mss = masterServiceState;

      @Override
      public MasterProtos.AbortProcedureResponse abortProcedure(
          RpcController controller,
          MasterProtos.AbortProcedureRequest request) throws ServiceException {
        return stub.abortProcedure(controller, request);
      }

      @Override
      public MasterProtos.GetProceduresResponse getProcedures(
          RpcController controller,
          MasterProtos.GetProceduresRequest request) throws ServiceException {
        return stub.getProcedures(controller, request);
      }

      @Override
      public MasterProtos.GetLocksResponse getLocks(
          RpcController controller,
          MasterProtos.GetLocksRequest request) throws ServiceException {
        return stub.getLocks(controller, request);
      }

      @Override
      public MasterProtos.AddColumnResponse addColumn(
          RpcController controller,
          MasterProtos.AddColumnRequest request) throws ServiceException {
        return stub.addColumn(controller, request);
      }

      @Override
      public MasterProtos.DeleteColumnResponse deleteColumn(RpcController controller,
          MasterProtos.DeleteColumnRequest request)
      throws ServiceException {
        return stub.deleteColumn(controller, request);
      }

      @Override
      public MasterProtos.ModifyColumnResponse modifyColumn(RpcController controller,
          MasterProtos.ModifyColumnRequest request)
      throws ServiceException {
        return stub.modifyColumn(controller, request);
      }

      @Override
      public MasterProtos.MoveRegionResponse moveRegion(RpcController controller,
          MasterProtos.MoveRegionRequest request) throws ServiceException {
        return stub.moveRegion(controller, request);
      }

      @Override
      public MasterProtos.MergeTableRegionsResponse mergeTableRegions(
          RpcController controller, MasterProtos.MergeTableRegionsRequest request)
          throws ServiceException {
        return stub.mergeTableRegions(controller, request);
      }

      @Override
      public MasterProtos.AssignRegionResponse assignRegion(RpcController controller,
          MasterProtos.AssignRegionRequest request) throws ServiceException {
        return stub.assignRegion(controller, request);
      }

      @Override
      public MasterProtos.UnassignRegionResponse unassignRegion(RpcController controller,
          MasterProtos.UnassignRegionRequest request) throws ServiceException {
        return stub.unassignRegion(controller, request);
      }

      @Override
      public MasterProtos.OfflineRegionResponse offlineRegion(RpcController controller,
          MasterProtos.OfflineRegionRequest request) throws ServiceException {
        return stub.offlineRegion(controller, request);
      }

      @Override
      public MasterProtos.SplitTableRegionResponse splitRegion(RpcController controller,
          MasterProtos.SplitTableRegionRequest request) throws ServiceException {
        return stub.splitRegion(controller, request);
      }

      @Override
      public MasterProtos.DeleteTableResponse deleteTable(RpcController controller,
          MasterProtos.DeleteTableRequest request) throws ServiceException {
        return stub.deleteTable(controller, request);
      }

      @Override
      public MasterProtos.TruncateTableResponse truncateTable(RpcController controller,
          MasterProtos.TruncateTableRequest request) throws ServiceException {
        return stub.truncateTable(controller, request);
      }

      @Override
      public MasterProtos.EnableTableResponse enableTable(RpcController controller,
          MasterProtos.EnableTableRequest request) throws ServiceException {
        return stub.enableTable(controller, request);
      }

      @Override
      public MasterProtos.DisableTableResponse disableTable(RpcController controller,
          MasterProtos.DisableTableRequest request) throws ServiceException {
        return stub.disableTable(controller, request);
      }

      @Override
      public MasterProtos.ModifyTableResponse modifyTable(RpcController controller,
          MasterProtos.ModifyTableRequest request) throws ServiceException {
        return stub.modifyTable(controller, request);
      }

      @Override
      public MasterProtos.CreateTableResponse createTable(RpcController controller,
          MasterProtos.CreateTableRequest request) throws ServiceException {
        return stub.createTable(controller, request);
      }

      @Override
      public MasterProtos.ShutdownResponse shutdown(RpcController controller,
          MasterProtos.ShutdownRequest request) throws ServiceException {
        return stub.shutdown(controller, request);
      }

      @Override
      public MasterProtos.StopMasterResponse stopMaster(RpcController controller,
          MasterProtos.StopMasterRequest request) throws ServiceException {
        return stub.stopMaster(controller, request);
      }

      @Override
      public MasterProtos.IsInMaintenanceModeResponse isMasterInMaintenanceMode(
          final RpcController controller,
          final MasterProtos.IsInMaintenanceModeRequest request) throws ServiceException {
        return stub.isMasterInMaintenanceMode(controller, request);
      }

      @Override
      public MasterProtos.BalanceResponse balance(RpcController controller,
          MasterProtos.BalanceRequest request) throws ServiceException {
        return stub.balance(controller, request);
      }

      @Override
      public MasterProtos.SetBalancerRunningResponse setBalancerRunning(
          RpcController controller, MasterProtos.SetBalancerRunningRequest request)
          throws ServiceException {
        return stub.setBalancerRunning(controller, request);
      }

      @Override
      public NormalizeResponse normalize(RpcController controller,
          NormalizeRequest request) throws ServiceException {
        return stub.normalize(controller, request);
      }

      @Override
      public SetNormalizerRunningResponse setNormalizerRunning(
          RpcController controller, SetNormalizerRunningRequest request)
          throws ServiceException {
        return stub.setNormalizerRunning(controller, request);
      }

      @Override
      public MasterProtos.RunCatalogScanResponse runCatalogScan(RpcController controller,
          MasterProtos.RunCatalogScanRequest request) throws ServiceException {
        return stub.runCatalogScan(controller, request);
      }

      @Override
      public MasterProtos.EnableCatalogJanitorResponse enableCatalogJanitor(
          RpcController controller, MasterProtos.EnableCatalogJanitorRequest request)
          throws ServiceException {
        return stub.enableCatalogJanitor(controller, request);
      }

      @Override
      public MasterProtos.IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(
          RpcController controller, MasterProtos.IsCatalogJanitorEnabledRequest request)
          throws ServiceException {
        return stub.isCatalogJanitorEnabled(controller, request);
      }

      @Override
      public MasterProtos.RunCleanerChoreResponse runCleanerChore(RpcController controller,
          MasterProtos.RunCleanerChoreRequest request)
          throws ServiceException {
        return stub.runCleanerChore(controller, request);
      }

      @Override
      public MasterProtos.SetCleanerChoreRunningResponse setCleanerChoreRunning(
          RpcController controller, MasterProtos.SetCleanerChoreRunningRequest request)
          throws ServiceException {
        return stub.setCleanerChoreRunning(controller, request);
      }

      @Override
      public MasterProtos.IsCleanerChoreEnabledResponse isCleanerChoreEnabled(
          RpcController controller, MasterProtos.IsCleanerChoreEnabledRequest request)
          throws ServiceException {
        return stub.isCleanerChoreEnabled(controller, request);
      }

      @Override
      public ClientProtos.CoprocessorServiceResponse execMasterService(
          RpcController controller, ClientProtos.CoprocessorServiceRequest request)
          throws ServiceException {
        return stub.execMasterService(controller, request);
      }

      @Override
      public MasterProtos.SnapshotResponse snapshot(RpcController controller,
          MasterProtos.SnapshotRequest request) throws ServiceException {
        return stub.snapshot(controller, request);
      }

      @Override
      public MasterProtos.GetCompletedSnapshotsResponse getCompletedSnapshots(
          RpcController controller, MasterProtos.GetCompletedSnapshotsRequest request)
          throws ServiceException {
        return stub.getCompletedSnapshots(controller, request);
      }

      @Override
      public MasterProtos.DeleteSnapshotResponse deleteSnapshot(RpcController controller,
          MasterProtos.DeleteSnapshotRequest request) throws ServiceException {
        return stub.deleteSnapshot(controller, request);
      }

      @Override
      public MasterProtos.IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
          MasterProtos.IsSnapshotDoneRequest request) throws ServiceException {
        return stub.isSnapshotDone(controller, request);
      }

      @Override
      public MasterProtos.RestoreSnapshotResponse restoreSnapshot(
          RpcController controller, MasterProtos.RestoreSnapshotRequest request)
          throws ServiceException {
        return stub.restoreSnapshot(controller, request);
      }

      @Override
      public MasterProtos.SetSnapshotCleanupResponse switchSnapshotCleanup(
          RpcController controller, MasterProtos.SetSnapshotCleanupRequest request)
          throws ServiceException {
        return stub.switchSnapshotCleanup(controller, request);
      }

      @Override
      public MasterProtos.IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(
          RpcController controller, MasterProtos.IsSnapshotCleanupEnabledRequest request)
          throws ServiceException {
        return stub.isSnapshotCleanupEnabled(controller, request);
      }

      @Override
      public MasterProtos.ExecProcedureResponse execProcedure(
          RpcController controller, MasterProtos.ExecProcedureRequest request)
          throws ServiceException {
        return stub.execProcedure(controller, request);
      }

      @Override
      public MasterProtos.ExecProcedureResponse execProcedureWithRet(
          RpcController controller, MasterProtos.ExecProcedureRequest request)
          throws ServiceException {
        return stub.execProcedureWithRet(controller, request);
      }

      @Override
      public MasterProtos.IsProcedureDoneResponse isProcedureDone(RpcController controller,
          MasterProtos.IsProcedureDoneRequest request) throws ServiceException {
        return stub.isProcedureDone(controller, request);
      }

      @Override
      public MasterProtos.GetProcedureResultResponse getProcedureResult(RpcController controller,
          MasterProtos.GetProcedureResultRequest request) throws ServiceException {
        return stub.getProcedureResult(controller, request);
      }

      @Override
      public MasterProtos.IsMasterRunningResponse isMasterRunning(
          RpcController controller, MasterProtos.IsMasterRunningRequest request)
          throws ServiceException {
        return stub.isMasterRunning(controller, request);
      }

      @Override
      public MasterProtos.ModifyNamespaceResponse modifyNamespace(RpcController controller,
          MasterProtos.ModifyNamespaceRequest request)
      throws ServiceException {
        return stub.modifyNamespace(controller, request);
      }

      @Override
      public MasterProtos.CreateNamespaceResponse createNamespace(
          RpcController controller,
          MasterProtos.CreateNamespaceRequest request) throws ServiceException {
        return stub.createNamespace(controller, request);
      }

      @Override
      public MasterProtos.DeleteNamespaceResponse deleteNamespace(
          RpcController controller,
          MasterProtos.DeleteNamespaceRequest request) throws ServiceException {
        return stub.deleteNamespace(controller, request);
      }

      @Override
      public MasterProtos.ListNamespacesResponse listNamespaces(
          RpcController controller,
          MasterProtos.ListNamespacesRequest request) throws ServiceException {
        return stub.listNamespaces(controller, request);
      }

      @Override
      public MasterProtos.GetNamespaceDescriptorResponse getNamespaceDescriptor(
          RpcController controller,
          MasterProtos.GetNamespaceDescriptorRequest request) throws ServiceException {
        return stub.getNamespaceDescriptor(controller, request);
      }

      @Override
      public MasterProtos.ListNamespaceDescriptorsResponse listNamespaceDescriptors(
          RpcController controller,
          MasterProtos.ListNamespaceDescriptorsRequest request) throws ServiceException {
        return stub.listNamespaceDescriptors(controller, request);
      }

      @Override
      public MasterProtos.ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
          RpcController controller, MasterProtos.ListTableDescriptorsByNamespaceRequest request)
              throws ServiceException {
        return stub.listTableDescriptorsByNamespace(controller, request);
      }

      @Override
      public MasterProtos.ListTableNamesByNamespaceResponse listTableNamesByNamespace(
          RpcController controller, MasterProtos.ListTableNamesByNamespaceRequest request)
              throws ServiceException {
        return stub.listTableNamesByNamespace(controller, request);
      }

      @Override
      public MasterProtos.GetTableStateResponse getTableState(
              RpcController controller, MasterProtos.GetTableStateRequest request)
              throws ServiceException {
        return stub.getTableState(controller, request);
      }

      @Override
      public void close() {
        release(this.mss);
      }

      @Override
      public MasterProtos.GetSchemaAlterStatusResponse getSchemaAlterStatus(
          RpcController controller, MasterProtos.GetSchemaAlterStatusRequest request)
          throws ServiceException {
        return stub.getSchemaAlterStatus(controller, request);
      }

      @Override
      public MasterProtos.GetTableDescriptorsResponse getTableDescriptors(
          RpcController controller, MasterProtos.GetTableDescriptorsRequest request)
          throws ServiceException {
        return stub.getTableDescriptors(controller, request);
      }

      @Override
      public MasterProtos.GetTableNamesResponse getTableNames(
          RpcController controller, MasterProtos.GetTableNamesRequest request)
          throws ServiceException {
        return stub.getTableNames(controller, request);
      }

      @Override
      public MasterProtos.GetClusterStatusResponse getClusterStatus(
          RpcController controller, MasterProtos.GetClusterStatusRequest request)
          throws ServiceException {
        return stub.getClusterStatus(controller, request);
      }

      @Override
      public MasterProtos.SetQuotaResponse setQuota(
          RpcController controller, MasterProtos.SetQuotaRequest request)
          throws ServiceException {
        return stub.setQuota(controller, request);
      }

      @Override
      public MasterProtos.MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(
          RpcController controller, MasterProtos.MajorCompactionTimestampRequest request)
          throws ServiceException {
        return stub.getLastMajorCompactionTimestamp(controller, request);
      }

      @Override
      public MasterProtos.MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(
          RpcController controller, MasterProtos.MajorCompactionTimestampForRegionRequest request)
          throws ServiceException {
        return stub.getLastMajorCompactionTimestampForRegion(controller, request);
      }

      @Override
      public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
          IsBalancerEnabledRequest request) throws ServiceException {
        return stub.isBalancerEnabled(controller, request);
      }

      @Override
      public MasterProtos.SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(
        RpcController controller, MasterProtos.SetSplitOrMergeEnabledRequest request)
        throws ServiceException {
        return stub.setSplitOrMergeEnabled(controller, request);
      }

      @Override
      public MasterProtos.IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(
        RpcController controller, MasterProtos.IsSplitOrMergeEnabledRequest request)
              throws ServiceException {
        return stub.isSplitOrMergeEnabled(controller, request);
      }

      @Override
      public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
          IsNormalizerEnabledRequest request) throws ServiceException {
        return stub.isNormalizerEnabled(controller, request);
      }

      @Override
      public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
          SecurityCapabilitiesRequest request) throws ServiceException {
        return stub.getSecurityCapabilities(controller, request);
      }

      @Override
      public AddReplicationPeerResponse addReplicationPeer(RpcController controller,
          AddReplicationPeerRequest request) throws ServiceException {
        return stub.addReplicationPeer(controller, request);
      }

      @Override
      public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
          RemoveReplicationPeerRequest request) throws ServiceException {
        return stub.removeReplicationPeer(controller, request);
      }

      @Override
      public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
          EnableReplicationPeerRequest request) throws ServiceException {
        return stub.enableReplicationPeer(controller, request);
      }

      @Override
      public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
          DisableReplicationPeerRequest request) throws ServiceException {
        return stub.disableReplicationPeer(controller, request);
      }

      @Override
      public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(RpcController controller,
          ListDecommissionedRegionServersRequest request) throws ServiceException {
        return stub.listDecommissionedRegionServers(controller, request);
      }

      @Override
      public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller,
          DecommissionRegionServersRequest request) throws ServiceException {
        return stub.decommissionRegionServers(controller, request);
      }

      @Override
      public RecommissionRegionServerResponse recommissionRegionServer(
          RpcController controller, RecommissionRegionServerRequest request)
          throws ServiceException {
        return stub.recommissionRegionServer(controller, request);
      }

      @Override
      public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
          GetReplicationPeerConfigRequest request) throws ServiceException {
        return stub.getReplicationPeerConfig(controller, request);
      }

      @Override
      public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(
          RpcController controller, UpdateReplicationPeerConfigRequest request)
          throws ServiceException {
        return stub.updateReplicationPeerConfig(controller, request);
      }

      @Override
      public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
          ListReplicationPeersRequest request) throws ServiceException {
        return stub.listReplicationPeers(controller, request);
      }

      @Override
      public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(
          RpcController controller, GetSpaceQuotaRegionSizesRequest request)
          throws ServiceException {
        return stub.getSpaceQuotaRegionSizes(controller, request);
      }

      @Override
      public GetQuotaStatesResponse getQuotaStates(
          RpcController controller, GetQuotaStatesRequest request) throws ServiceException {
        return stub.getQuotaStates(controller, request);
      }

      @Override
      public MasterProtos.ClearDeadServersResponse clearDeadServers(RpcController controller,
          MasterProtos.ClearDeadServersRequest request) throws ServiceException {
        return stub.clearDeadServers(controller, request);
      }

      @Override
      public SwitchRpcThrottleResponse switchRpcThrottle(RpcController controller,
          SwitchRpcThrottleRequest request) throws ServiceException {
        return stub.switchRpcThrottle(controller, request);
      }

      @Override
      public IsRpcThrottleEnabledResponse isRpcThrottleEnabled(RpcController controller,
          IsRpcThrottleEnabledRequest request) throws ServiceException {
        return stub.isRpcThrottleEnabled(controller, request);
      }

      @Override
      public SwitchExceedThrottleQuotaResponse switchExceedThrottleQuota(RpcController controller,
          SwitchExceedThrottleQuotaRequest request) throws ServiceException {
        return stub.switchExceedThrottleQuota(controller, request);
      }

      @Override
      public AccessControlProtos.GrantResponse grant(RpcController controller,
          AccessControlProtos.GrantRequest request) throws ServiceException {
        return stub.grant(controller, request);
      }

      @Override
      public AccessControlProtos.RevokeResponse revoke(RpcController controller,
          AccessControlProtos.RevokeRequest request) throws ServiceException {
        return stub.revoke(controller, request);
      }

      @Override
      public GetUserPermissionsResponse getUserPermissions(RpcController controller,
          GetUserPermissionsRequest request) throws ServiceException {
        return stub.getUserPermissions(controller, request);
      }

      @Override
      public HasUserPermissionsResponse hasUserPermissions(RpcController controller,
          HasUserPermissionsRequest request) throws ServiceException {
        return stub.hasUserPermissions(controller, request);
      }
    };
  }

  private static void release(MasterServiceState mss) {
    if (mss != null && mss.connection != null) {
      ((ConnectionImplementation)mss.connection).releaseMaster(mss);
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
    } catch (IOException se) {
      LOG.warn("Checking master connection", se);
      return false;
    }
  }

  void releaseMaster(MasterServiceState mss) {
    if (mss.getStub() == null) {
      return;
    }
    synchronized (masterLock) {
      --mss.userCount;
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
    synchronized (masterLock) {
      closeMasterService(masterServiceState);
    }
  }

  void updateCachedLocation(RegionInfo hri, ServerName source, ServerName serverName, long seqNum) {
    HRegionLocation newHrl = new HRegionLocation(hri, serverName, seqNum);
    cacheLocation(hri.getTable(), source, newHrl);
  }

  @Override
  public void deleteCachedRegionLocation(final HRegionLocation location) {
    metaCache.clearCache(location);
  }

  /**
   * Update the location with the new value (if the exception is a RegionMovedException)
   * or delete it from the cache. Does nothing if we can be sure from the exception that
   * the location is still accurate, or if the cache has already been updated.
   * @param exception an object (to simplify user code) on which we will try to find a nested
   *   or wrapped or both RegionMovedException
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
      if (metrics != null) {
        metrics.incrCacheDroppingExceptions(exception);
      }
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

    RegionInfo regionInfo = oldLocation.getRegion();
    Throwable cause = ClientExceptionsUtil.findException(exception);
    if (cause != null) {
      if (!ClientExceptionsUtil.isMetaClearingException(cause)) {
        // We know that the region is still on this region server
        return;
      }

      if (cause instanceof RegionMovedException) {
        RegionMovedException rme = (RegionMovedException) cause;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Region " + regionInfo.getRegionNameAsString() + " moved to " +
              rme.getHostname() + ":" + rme.getPort() +
              " according to " + source.getAddress());
        }
        // We know that the region is not anymore on this region server, but we know
        //  the new location.
        updateCachedLocation(
            regionInfo, source, rme.getServerName(), rme.getLocationSeqNum());
        return;
      }
    }

    if (metrics != null) {
      metrics.incrCacheDroppingExceptions(exception);
    }

    // If we're here, it means that can cannot be sure about the location, so we remove it from
    // the cache. Do not send the source because source can be a new server in the same host:port
    metaCache.clearCache(regionInfo);
  }

  @Override
  public AsyncProcess getAsyncProcess() {
    return asyncProcess;
  }

  @Override
  public ServerStatisticTracker getStatisticsTracker() {
    return this.stats;
  }

  @Override
  public ClientBackoffPolicy getBackoffPolicy() {
    return this.backoffPolicy;
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
  public void abort(final String msg, Throwable t) {
    if (t != null) {
      LOG.error(HBaseMarkers.FATAL, msg, t);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    this.aborted = true;
    close();
    this.closed = true;
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
  public void close() {
    if (this.closed) {
      return;
    }
    closeMaster();
    shutdownPools();
    if (this.metrics != null) {
      this.metrics.shutdown();
    }
    this.closed = true;
    registry.close();
    this.stubs.clear();
    if (clusterStatusListener != null) {
      clusterStatusListener.close();
    }
    if (rpcClient != null) {
      rpcClient.close();
    }
    if (authService != null) {
      authService.shutdown();
    }
  }

  /**
   * Close the connection for good. On the off chance that someone is unable to close
   * the connection, perhaps because it bailed out prematurely, the method
   * below will ensure that this instance is cleaned up.
   * Caveat: The JVM may take an unknown amount of time to call finalize on an
   * unreachable object, so our hope is that every consumer cleans up after
   * itself, like any good citizen.
   */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return nonceGenerator;
  }

  @Override
  public TableState getTableState(TableName tableName) throws IOException {
    checkClosed();
    TableState tableState = MetaTableAccessor.getTableState(this, tableName);
    if (tableState == null) {
      throw new TableNotFoundException(tableName);
    }
    return tableState;
  }

  @Override
  public RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf) {
    return RpcRetryingCallerFactory
        .instantiate(conf, this.interceptor, this.getStatisticsTracker());
  }

  @Override
  public boolean hasCellBlockSupport() {
    return this.rpcClient.hasCellBlockSupport();
  }

  @Override
  public ConnectionConfiguration getConnectionConfiguration() {
    return this.connectionConfig;
  }

  @Override
  public RpcRetryingCallerFactory getRpcRetryingCallerFactory() {
    return this.rpcCallerFactory;
  }

  @Override
  public RpcControllerFactory getRpcControllerFactory() {
    return this.rpcControllerFactory;
  }

  private static <T> T get(CompletableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    }
  }
}
