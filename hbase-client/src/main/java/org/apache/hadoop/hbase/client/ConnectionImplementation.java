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

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicyFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final Log LOG = LogFactory.getLog(ConnectionImplementation.class);
  private static final String CLIENT_NONCES_ENABLED_KEY = "hbase.client.nonces.enabled";
  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final boolean hostnamesCanChange;
  private final long pause;
  private final boolean useMetaReplicas;
  private final int numTries;
  final int rpcTimeout;

  /**
   * Global nonceGenerator shared per client.Currently there's no reason to limit its scope.
   * Once it's set under nonceGeneratorCreateLock, it is never unset or changed.
   */
  private static volatile NonceGenerator nonceGenerator = null;
  /** The nonce generator lock. Only taken when creating HConnection, which gets a private copy. */
  private static Object nonceGeneratorCreateLock = new Object();

  private final AsyncProcess asyncProcess;
  // single tracker per connection
  private final ServerStatisticTracker stats;

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

  // thread executor shared by all HTableInterface instances created
  // by this connection
  private volatile ExecutorService batchPool = null;
  // meta thread executor shared by all HTableInterface instances created
  // by this connection
  private volatile ExecutorService metaLookupPool = null;
  private volatile boolean cleanupPool = false;

  private final Configuration conf;

  // cache the configuration value for tables so that we can avoid calling
  // the expensive Configuration to fetch the value multiple times.
  private final TableConfiguration tableConfig;

  // Client rpc instance.
  private RpcClient rpcClient;

  private final MetaCache metaCache;
  private final MetricsConnection metrics;

  private int refCount;

  protected User user;

  private RpcRetryingCallerFactory rpcCallerFactory;

  private RpcControllerFactory rpcControllerFactory;

  private final RetryingCallerInterceptor interceptor;

  /**
   * Cluster registry of basic info such as clusterid and meta region location.
   */
   Registry registry;

  private final ClientBackoffPolicy backoffPolicy;

  /**
   * constructor
   * @param conf Configuration object
   */
  ConnectionImplementation(Configuration conf,
                           ExecutorService pool, User user) throws IOException {
    this.conf = conf;
    this.user = user;
    this.batchPool = pool;
    this.tableConfig = new TableConfiguration(conf);
    this.closed = false;
    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.useMetaReplicas = conf.getBoolean(HConstants.USE_META_REPLICAS,
        HConstants.DEFAULT_USE_META_REPLICAS);
    this.numTries = tableConfig.getRetriesNumber();
    this.rpcTimeout = conf.getInt(
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    if (conf.getBoolean(CLIENT_NONCES_ENABLED_KEY, true)) {
      synchronized (nonceGeneratorCreateLock) {
        if (nonceGenerator == null) {
          nonceGenerator = new PerClientRandomNonceGenerator();
        }
      }
    } else {
      nonceGenerator = new NoNonceGenerator();
    }

    this.stats = ServerStatisticTracker.create(conf);
    this.interceptor = (new RetryingCallerInterceptorFactory(conf)).build();
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, interceptor, this.stats);
    this.backoffPolicy = ClientBackoffPolicyFactory.create(conf);
    this.asyncProcess = createAsyncProcess(this.conf);
    if (conf.getBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true)) {
      this.metrics = new MetricsConnection(this);
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

    try {
      this.registry = setupRegistry();
      retrieveClusterId();

      this.rpcClient = RpcClientFactory.createClient(this.conf, this.clusterId);

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

  /**
   * Look for an exception we know in the remote exception:
   * - hadoop.ipc wrapped exceptions
   * - nested exceptions
   *
   * Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException /
   *            ThrottlingException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (cur instanceof RegionMovedException || cur instanceof RegionOpeningException
          || cur instanceof RegionTooBusyException || cur instanceof ThrottlingException) {
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
    return new HTable(tableName, this, tableConfig, rpcCallerFactory, rpcControllerFactory, pool);
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
      params.writeBufferSize(tableConfig.getWriteBufferSize());
    }
    if (params.getMaxKeyValueSize() == BufferedMutatorParams.UNSET) {
      params.maxKeyValueSize(tableConfig.getMaxKeyValueSize());
    }
    return new BufferedMutatorImpl(this, rpcCallerFactory, rpcControllerFactory, params);
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
  public MetricsConnection getConnectionMetrics() {
    return this.metrics;
  }

  private ExecutorService getBatchPool() {
    if (batchPool == null) {
      synchronized (this) {
        if (batchPool == null) {
          this.batchPool = getThreadPool(conf.getInt("hbase.hconnection.threads.max", 256),
              conf.getInt("hbase.hconnection.threads.core", 256), "-shared-", null);
          this.cleanupPool = true;
        }
      }
    }
    return this.batchPool;
  }

  private ExecutorService getThreadPool(int maxThreads, int coreThreads, String nameHint,
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
        new LinkedBlockingQueue<Runnable>(maxThreads *
            conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
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

  private ExecutorService getMetaLookupPool() {
    if (this.metaLookupPool == null) {
      synchronized (this) {
        if (this.metaLookupPool == null) {
          //Some of the threads would be used for meta replicas
          //To start with, threads.max.core threads can hit the meta (including replicas).
          //After that, requests will get queued up in the passed queue, and only after
          //the queue is full, a new thread will be started
          this.metaLookupPool = getThreadPool(
             conf.getInt("hbase.hconnection.meta.lookup.threads.max", 128),
             conf.getInt("hbase.hconnection.meta.lookup.threads.core", 10),
             "-metaLookup-shared-", new LinkedBlockingQueue<Runnable>());
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
   * @return The cluster registry implementation to use.
   * @throws java.io.IOException
   */
  private Registry setupRegistry() throws IOException {
    return RegistryFactory.getRegistry(this);
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
   * @throws org.apache.hadoop.hbase.MasterNotRunningException - if the master is not running
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @deprecated this has been deprecated without a replacement
   */
  @Deprecated
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
    return isTableAvailable(tableName, null);
  }

  @Override
  public boolean isTableAvailable(final byte[] tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(final TableName tableName, @Nullable final byte[][] splitKeys)
      throws IOException {
    if (this.closed) throw new IOException(toString() + " closed");
    try {
      if (!isTableEnabled(tableName)) {
        LOG.debug("Table " + tableName + " not enabled");
        return false;
      }
      List<Pair<HRegionInfo, ServerName>> locations =
        MetaTableAccessor.getTableRegionsAndLocations(this, tableName, true);

      int notDeployed = 0;
      int regionCount = 0;
      for (Pair<HRegionInfo, ServerName> pair : locations) {
        HRegionInfo info = pair.getFirst();
        if (pair.getSecond() == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Table " + tableName + " has not deployed region " + pair.getFirst()
                .getEncodedName());
          }
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
          LOG.debug("Table " + tableName + " has " + notDeployed + " regions");
        }
        return false;
      } else if (splitKeys != null && regionCount != splitKeys.length + 1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table " + tableName + " expected to have " + (splitKeys.length + 1)
              + " regions, but only " + regionCount + " available");
        }
        return false;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table " + tableName + " should be available");
        }
        return true;
      }
    } catch (TableNotFoundException tnfe) {
      LOG.warn("Table " + tableName + " not enabled, it is not exists");
      return false;
    }
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
    return locateRegions(tableName, false, true);
  }

  @Override
  public List<HRegionLocation> locateRegions(final byte[] tableName)
  throws IOException {
    return locateRegions(TableName.valueOf(tableName));
  }

  @Override
  public List<HRegionLocation> locateRegions(final TableName tableName,
      final boolean useCache, final boolean offlined) throws IOException {
    List<HRegionInfo> regions = MetaTableAccessor
        .getTableRegions(this, tableName, !offlined);
    final List<HRegionLocation> locations = new ArrayList<HRegionLocation>();
    for (HRegionInfo regionInfo : regions) {
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
    if (this.useMetaReplicas) {
      s.setConsistency(Consistency.TIMELINE);
    }

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
            rpcCallerFactory, rpcControllerFactory, getMetaLookupPool(), 0);
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
    MasterProtos.MasterService.BlockingInterface stub;
    int userCount;

    MasterServiceState(final HConnection connection) {
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
      MasterProtos.IsMasterRunningResponse response =
        this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      return response != null? response.getIsMasterRunning(): false;
    }
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
        result = ConnectionUtils.getPauseTime(basePause, errorStats.getCount());
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
     * @throws com.google.protobuf.ServiceException
     */
    protected abstract void isMasterRunning() throws ServiceException;

    /**
     * Create a stub. Try once only.  It is not typed because there is no common type to
     * protobuf services nor their interfaces.  Let the caller do appropriate casting.
     * @return A stub for master services.
     * @throws java.io.IOException
     * @throws org.apache.zookeeper.KeeperException
     * @throws com.google.protobuf.ServiceException
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
        String key = getStubKey(getServiceName(),
            sn.getHostname(), sn.getPort(), hostnamesCanChange);
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
     * @throws org.apache.hadoop.hbase.MasterNotRunningException
     */
    Object makeStub() throws IOException {
      // The lock must be at the beginning to prevent multiple master creations
      //  (and leaks) in a multithread context
      synchronized (masterAndZKLock) {
        Exception exceptionCaught = null;
        if (!closed) {
          try {
            return makeStubNoRetries();
          } catch (IOException e) {
            exceptionCaught = e;
          } catch (KeeperException e) {
            exceptionCaught = e;
          } catch (ServiceException e) {
            exceptionCaught = e;
          }

          throw new MasterNotRunningException(exceptionCaught);
        } else {
          throw new DoNotRetryIOException("Connection was closed while trying to get master");
        }
      }
    }
  }

  /**
   * Class to make a MasterServiceStubMaker stub.
   */
  class MasterServiceStubMaker extends StubMaker {
    private MasterProtos.MasterService.BlockingInterface stub;
    @Override
    protected String getServiceName() {
      return MasterProtos.MasterService.getDescriptor().getName();
    }

    @Override
    MasterProtos.MasterService.BlockingInterface makeStub() throws IOException {
      return (MasterProtos.MasterService.BlockingInterface)super.makeStub();
    }

    @Override
    protected Object makeStub(BlockingRpcChannel channel) {
      this.stub = MasterProtos.MasterService.newBlockingStub(channel);
      return this.stub;
    }

    @Override
    protected void isMasterRunning() throws ServiceException {
      this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
    }
  }

  @Override
  public AdminProtos.AdminService.BlockingInterface getAdmin(final ServerName serverName)
      throws IOException {
    return getAdmin(serverName, false);
  }

  @Override
  // Nothing is done w/ the 'master' parameter.  It is ignored.
  public AdminProtos.AdminService.BlockingInterface getAdmin(final ServerName serverName,
    final boolean master)
  throws IOException {
    if (isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    }
    String key = getStubKey(AdminProtos.AdminService.BlockingInterface.class.getName(),
        serverName.getHostname(), serverName.getPort(), this.hostnamesCanChange);
    this.connectionLock.putIfAbsent(key, key);
    AdminProtos.AdminService.BlockingInterface stub = null;
    synchronized (this.connectionLock.get(key)) {
      stub = (AdminProtos.AdminService.BlockingInterface)this.stubs.get(key);
      if (stub == null) {
        BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(serverName, user, rpcTimeout);
        stub = AdminProtos.AdminService.newBlockingStub(channel);
        this.stubs.put(key, stub);
      }
    }
    return stub;
  }

  @Override
  public ClientProtos.ClientService.BlockingInterface getClient(final ServerName sn)
  throws IOException {
    if (isDeadServer(sn)) {
      throw new RegionServerStoppedException(sn + " is dead.");
    }
    String key = getStubKey(
      ClientProtos.ClientService.BlockingInterface.class.getName(), sn.getHostname(),
      sn.getPort(), this.hostnamesCanChange);
    this.connectionLock.putIfAbsent(key, key);
    ClientProtos.ClientService.BlockingInterface stub = null;
    synchronized (this.connectionLock.get(key)) {
      stub = (ClientProtos.ClientService.BlockingInterface)this.stubs.get(key);
      if (stub == null) {
        BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
        stub = ClientProtos.ClientService.newBlockingStub(channel);
        // In old days, after getting stub/proxy, we'd make a call.  We are not doing that here.
        // Just fail on first actual call rather than in here on setup.
        this.stubs.put(key, stub);
      }
    }
    return stub;
  }

  static String getStubKey(final String serviceName,
                           final String rsHostname,
                           int port,
                           boolean resolveHostnames) {
    // Sometimes, servers go down and they come back up with the same hostname but a different
    // IP address. Force a resolution of the rsHostname by trying to instantiate an
    // InetSocketAddress, and this way we will rightfully get a new stubKey.
    // Also, include the hostname in the key so as to take care of those cases where the
    // DNS name is different but IP address remains the same.
    String address = rsHostname;
    if (resolveHostnames) {
      InetAddress i = new InetSocketAddress(rsHostname, port).getAddress();
      if (i != null) {
        address = i.getHostAddress() + "-" + rsHostname;
      }
    }
    return serviceName + "@" + address + ":" + port;
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
    if (keepAliveZookeeperUserCount.addAndGet(-1) <= 0) {
      keepZooKeeperWatcherAliveUntil = System.currentTimeMillis() + keepAlive;
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
  public MasterProtos.MasterService.BlockingInterface getMaster() throws MasterNotRunningException {
    return getKeepAliveMasterService();
  }

  private void resetMasterServiceState(final MasterServiceState mss) {
    mss.userCount++;
  }

  @Override
  public MasterKeepAliveConnection getKeepAliveMasterService()
  throws MasterNotRunningException {
    synchronized (masterAndZKLock) {
      if (!isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
        MasterServiceStubMaker stubMaker = new MasterServiceStubMaker();
        try {
          this.masterServiceState.stub = stubMaker.makeStub();
        } catch (MasterNotRunningException ex) {
          throw ex;
        } catch (IOException e) {
          // rethrow as MasterNotRunningException so that we can keep the method sig
          throw new MasterNotRunningException(e);
        }
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
      public MasterProtos.ListProceduresResponse listProcedures(
          RpcController controller,
          MasterProtos.ListProceduresRequest request) throws ServiceException {
        return stub.listProcedures(controller, request);
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
      public MasterProtos.DispatchMergingRegionsResponse dispatchMergingRegions(
          RpcController controller, MasterProtos.DispatchMergingRegionsRequest request)
          throws ServiceException {
        return stub.dispatchMergingRegions(controller, request);
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
      public MasterProtos.IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(
          RpcController controller, MasterProtos.IsRestoreSnapshotDoneRequest request)
          throws ServiceException {
        return stub.isRestoreSnapshotDone(controller, request);
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
      public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
          IsNormalizerEnabledRequest request) throws ServiceException {
        return stub.isNormalizerEnabled(controller, request);
      }

      @Override
      public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
          SecurityCapabilitiesRequest request) throws ServiceException {
        return stub.getSecurityCapabilities(controller, request);
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
    } catch (ServiceException se) {
      LOG.warn("Checking master connection", se);
      return false;
    }
  }

  void releaseMaster(MasterServiceState mss) {
    if (mss.getStub() == null) return;
    synchronized (masterAndZKLock) {
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
      if (cause instanceof RegionTooBusyException || cause instanceof RegionOpeningException
          || cause instanceof ThrottlingException) {
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

  /**
   * @deprecated since 0.96 - Use {@link org.apache.hadoop.hbase.client.HTableInterface#batch} instead
   */
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

  /**
   * @deprecated Unsupported API
   */
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
   * @deprecated since 0.96 -
   *   Use {@link org.apache.hadoop.hbase.client.HTable#processBatchCallback} instead
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

    AsyncProcess.AsyncRequestFuture ars = this.asyncProcess.submitAll(
        pool, tableName, list, callback, results);
    ars.waitUntilDone();
    if (ars.hasError()) {
      throw ars.getErrors();
    }
  }

  /**
   * @deprecated Unsupported API
   */
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
    return new AsyncProcess(this, conf, batchPool, rpcCallerFactory, false, rpcControllerFactory);
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

  /**
   * @deprecated always return false since 0.99
   */
  @Override
  @Deprecated
  public void setRegionCachePrefetch(final TableName tableName, final boolean enable) {
  }

  /**
   * @deprecated always return false since 0.99
   */
  @Override
  @Deprecated
  public void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable) {
  }

  /**
   * @deprecated always return false since 0.99
   */
  @Override
  @Deprecated
  public boolean getRegionCachePrefetch(TableName tableName) {
    return false;
  }

  /**
   * @deprecated always return false since 0.99
   */
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
    closeZooKeeperWatcher();
    this.stubs.clear();
    if (clusterStatusListener != null) {
      clusterStatusListener.close();
    }
    if (rpcClient != null) {
      rpcClient.close();
    }
  }

  /**
   * Close the connection for good, regardless of what the current value of
   * {@link #refCount} is. Ideally, {@link #refCount} should be zero at this
   * point, which would be the case if all of its consumers close the
   * connection. However, on the off chance that someone is unable to close
   * the connection, perhaps because it bailed out prematurely, the method
   * below will ensure that this {@link org.apache.hadoop.hbase.client.HConnection} instance
   * is cleaned up.
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

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#listTables()} instead
   */
  @Deprecated
  @Override
  public HTableDescriptor[] listTables() throws IOException {
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      MasterProtos.GetTableDescriptorsRequest req =
        RequestConverter.buildGetTableDescriptorsRequest((List<TableName>)null);
      return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#listTableNames()} instead
   */
  @Deprecated
  @Override
  public String[] getTableNames() throws IOException {
    TableName[] tableNames = listTableNames();
    String[] result = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      result[i] = tableNames[i].getNameAsString();
    }
    return result;
  }

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#listTableNames()} instead
   */
  @Deprecated
  @Override
  public TableName[] listTableNames() throws IOException {
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      return ProtobufUtil.getTableNameArray(master.getTableNames(null,
        MasterProtos.GetTableNamesRequest.newBuilder().build())
        .getTableNamesList());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#getTableDescriptorsByTableName(java.util.List)} instead
   */
  @Deprecated
  @Override
  public HTableDescriptor[] getHTableDescriptorsByTableName(
      List<TableName> tableNames) throws IOException {
    if (tableNames == null || tableNames.isEmpty()) return new HTableDescriptor[0];
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      MasterProtos.GetTableDescriptorsRequest req =
        RequestConverter.buildGetTableDescriptorsRequest(tableNames);
      return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#getTableDescriptorsByTableName(java.util.List)} instead
   */
  @Deprecated
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
    return nonceGenerator;
  }

  /**
   * Connects to the master to get the table descriptor.
   * @param tableName table name
   * @throws java.io.IOException if the connection to master fails or if the table
   *  is not found.
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#getTableDescriptor(org.apache.hadoop.hbase.TableName)} instead
   */
  @Deprecated
  @Override
  public HTableDescriptor getHTableDescriptor(final TableName tableName)
  throws IOException {
    if (tableName == null) return null;
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    MasterProtos.GetTableDescriptorsResponse htds;
    try {
      MasterProtos.GetTableDescriptorsRequest req =
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

  /**
   * @deprecated Use {@link org.apache.hadoop.hbase.client.Admin#getTableDescriptor(org.apache.hadoop.hbase.TableName)} instead
   */
  @Deprecated
  @Override
  public HTableDescriptor getHTableDescriptor(final byte[] tableName)
  throws IOException {
    return getHTableDescriptor(TableName.valueOf(tableName));
  }

  @Override
  public TableState getTableState(TableName tableName) throws IOException {
    if (this.closed) throw new IOException(toString() + " closed");

    TableState tableState = MetaTableAccessor.getTableState(this, tableName);
    if (tableState == null)
      throw new TableNotFoundException(tableName);
    return tableState;
  }

  @Override
  public RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf) {
    return RpcRetryingCallerFactory
        .instantiate(conf, this.interceptor, this.getStatisticsTracker());
  }
}
