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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.STATUS_PUBLISHED;
import static org.apache.hadoop.hbase.HConstants.STATUS_PUBLISHED_DEFAULT;
import static org.apache.hadoop.hbase.client.ClusterStatusListener.DEFAULT_STATUS_LISTENER_CLASS;
import static org.apache.hadoop.hbase.client.ClusterStatusListener.STATUS_LISTENER_CLASS;
import static org.apache.hadoop.hbase.client.ConnectionUtils.NO_NONCE_GENERATOR;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getStubKey;
import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;
import static org.apache.hadoop.hbase.client.NonceGenerator.CLIENT_NONCES_ENABLED_KEY;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicyFactory;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * The implementation of AsyncConnection.
 */
@InterfaceAudience.Private
class AsyncConnectionImpl implements AsyncConnection {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncConnectionImpl.class);

  static final HashedWheelTimer RETRY_TIMER = new HashedWheelTimer(
    new ThreadFactoryBuilder().setNameFormat("Async-Client-Retry-Timer-pool-%d").setDaemon(true)
      .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
    10, TimeUnit.MILLISECONDS);

  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final Configuration conf;

  final AsyncConnectionConfiguration connConf;

  private final User user;

  final ConnectionRegistry registry;

  private final int rpcTimeout;

  private final RpcClient rpcClient;

  final RpcControllerFactory rpcControllerFactory;

  private final boolean hostnameCanChange;

  private final AsyncRegionLocator locator;

  final AsyncRpcRetryingCallerFactory callerFactory;

  private final NonceGenerator nonceGenerator;

  private final ConcurrentMap<String, ClientService.Interface> rsStubs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AdminService.Interface> adminSubs = new ConcurrentHashMap<>();

  private final AtomicReference<MasterService.Interface> masterStub = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<MasterService.Interface>> masterStubMakeFuture =
    new AtomicReference<>();

  private final Optional<ServerStatisticTracker> stats;
  private final ClientBackoffPolicy backoffPolicy;

  private ChoreService choreService;

  private volatile boolean closed = false;

  private final Optional<MetricsConnection> metrics;

  private final ClusterStatusListener clusterStatusListener;

  public AsyncConnectionImpl(Configuration conf, ConnectionRegistry registry, String clusterId,
      User user) {
    this.conf = conf;
    this.user = user;

    if (user.isLoginFromKeytab()) {
      spawnRenewalChore(user.getUGI());
    }
    this.connConf = new AsyncConnectionConfiguration(conf);
    this.registry = registry;
    if (conf.getBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, false)) {
      this.metrics = Optional.of(new MetricsConnection(this.toString(), () -> null, () -> null));
    } else {
      this.metrics = Optional.empty();
    }
    this.rpcClient = RpcClientFactory.createClient(conf, clusterId, metrics.orElse(null));
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.hostnameCanChange = conf.getBoolean(RESOLVE_HOSTNAME_ON_FAIL_KEY, true);
    this.rpcTimeout =
      (int) Math.min(Integer.MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(connConf.getRpcTimeoutNs()));
    this.locator = new AsyncRegionLocator(this, RETRY_TIMER);
    this.callerFactory = new AsyncRpcRetryingCallerFactory(this, RETRY_TIMER);
    if (conf.getBoolean(CLIENT_NONCES_ENABLED_KEY, true)) {
      nonceGenerator = PerClientRandomNonceGenerator.get();
    } else {
      nonceGenerator = NO_NONCE_GENERATOR;
    }
    this.stats = Optional.ofNullable(ServerStatisticTracker.create(conf));
    this.backoffPolicy = ClientBackoffPolicyFactory.create(conf);
    ClusterStatusListener listener = null;
    if (conf.getBoolean(STATUS_PUBLISHED, STATUS_PUBLISHED_DEFAULT)) {
      // TODO: this maybe a blocking operation, better to create it outside the constructor and pass
      // it in, just like clusterId. Not a big problem for now as the default value is false.
      Class<? extends ClusterStatusListener.Listener> listenerClass = conf.getClass(
        STATUS_LISTENER_CLASS, DEFAULT_STATUS_LISTENER_CLASS, ClusterStatusListener.Listener.class);
      if (listenerClass == null) {
        LOG.warn("{} is true, but {} is not set", STATUS_PUBLISHED, STATUS_LISTENER_CLASS);
      } else {
        try {
          listener = new ClusterStatusListener(
            new ClusterStatusListener.DeadServerHandler() {
              @Override
              public void newDead(ServerName sn) {
                locator.clearCache(sn);
                rpcClient.cancelConnections(sn);
              }
            }, conf, listenerClass);
        } catch (IOException e) {
          LOG.warn("Failed create of ClusterStatusListener, not a critical, ignoring...", e);
        }
      }
    }
    this.clusterStatusListener = listener;
  }

  private void spawnRenewalChore(final UserGroupInformation user) {
    ChoreService service = getChoreService();
    service.scheduleChore(AuthUtil.getAuthRenewalChore(user));
  }

  /**
   * If choreService has not been created yet, create the ChoreService.
   * @return ChoreService
   */
  synchronized ChoreService getChoreService() {
    if (isClosed()) {
      throw new IllegalStateException("connection is already closed");
    }
    if (choreService == null) {
      choreService = new ChoreService("AsyncConn Chore Service");
    }
    return choreService;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void close() {
    // As the code below is safe to be executed in parallel, here we do not use CAS or lock, just a
    // simple volatile flag.
    if (closed) {
      return;
    }
    LOG.info("Connection has been closed by {}.", Thread.currentThread().getName());
    if(LOG.isDebugEnabled()){
      logCallStack(Thread.currentThread().getStackTrace());
    }
    IOUtils.closeQuietly(clusterStatusListener,
      e -> LOG.warn("failed to close clusterStatusListener", e));
    IOUtils.closeQuietly(rpcClient, e -> LOG.warn("failed to close rpcClient", e));
    IOUtils.closeQuietly(registry, e -> LOG.warn("failed to close registry", e));
    synchronized (this) {
      if (choreService != null) {
        choreService.shutdown();
        choreService = null;
      }
    }
    metrics.ifPresent(MetricsConnection::shutdown);
    closed = true;
  }

  private void logCallStack(StackTraceElement[] stackTraceElements) {
    StringBuilder stackBuilder = new StringBuilder("Call stack:");
    for (StackTraceElement element : stackTraceElements) {
      stackBuilder.append("\n    at ");
      stackBuilder.append(element);
    }
    stackBuilder.append("\n");
    LOG.debug(stackBuilder.toString());
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new AsyncTableRegionLocatorImpl(tableName, this);
  }

  // we will override this method for testing retry caller, so do not remove this method.
  AsyncRegionLocator getLocator() {
    return locator;
  }

  // ditto
  public NonceGenerator getNonceGenerator() {
    return nonceGenerator;
  }

  private ClientService.Interface createRegionServerStub(ServerName serverName) throws IOException {
    return ClientService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
  }

  ClientService.Interface getRegionServerStub(ServerName serverName) throws IOException {
    return ConcurrentMapUtils.computeIfAbsentEx(rsStubs,
      getStubKey(ClientService.getDescriptor().getName(), serverName, hostnameCanChange),
      () -> createRegionServerStub(serverName));
  }

  private MasterService.Interface createMasterStub(ServerName serverName) throws IOException {
    return MasterService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
  }

  private AdminService.Interface createAdminServerStub(ServerName serverName) throws IOException {
    return AdminService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
  }

  AdminService.Interface getAdminStub(ServerName serverName) throws IOException {
    return ConcurrentMapUtils.computeIfAbsentEx(adminSubs,
      getStubKey(AdminService.getDescriptor().getName(), serverName, hostnameCanChange),
      () -> createAdminServerStub(serverName));
  }

  CompletableFuture<MasterService.Interface> getMasterStub() {
    return ConnectionUtils.getOrFetch(masterStub, masterStubMakeFuture, false, () -> {
      CompletableFuture<MasterService.Interface> future = new CompletableFuture<>();
      addListener(registry.getActiveMaster(), (addr, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else if (addr == null) {
          future.completeExceptionally(new MasterNotRunningException(
            "ZooKeeper available but no active master location found"));
        } else {
          LOG.debug("The fetched master address is {}", addr);
          try {
            future.complete(createMasterStub(addr));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }

      });
      return future;
    }, stub -> true, "master stub");
  }

  void clearMasterStubCache(MasterService.Interface stub) {
    masterStub.compareAndSet(stub, null);
  }

  Optional<ServerStatisticTracker> getStatisticsTracker() {
    return stats;
  }

  ClientBackoffPolicy getBackoffPolicy() {
    return backoffPolicy;
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return new AsyncTableBuilderBase<AdvancedScanResultConsumer>(tableName, connConf) {

      @Override
      public AsyncTable<AdvancedScanResultConsumer> build() {
        return new RawAsyncTableImpl(AsyncConnectionImpl.this, RETRY_TIMER, this);
      }
    };
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(TableName tableName,
      ExecutorService pool) {
    return new AsyncTableBuilderBase<ScanResultConsumer>(tableName, connConf) {

      @Override
      public AsyncTable<ScanResultConsumer> build() {
        RawAsyncTableImpl rawTable =
          new RawAsyncTableImpl(AsyncConnectionImpl.this, RETRY_TIMER, this);
        return new AsyncTableImpl(AsyncConnectionImpl.this, rawTable, pool);
      }
    };
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    return new AsyncAdminBuilderBase(connConf) {
      @Override
      public AsyncAdmin build() {
        return new RawAsyncHBaseAdmin(AsyncConnectionImpl.this, RETRY_TIMER, this);
      }
    };
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService pool) {
    return new AsyncAdminBuilderBase(connConf) {
      @Override
      public AsyncAdmin build() {
        RawAsyncHBaseAdmin rawAdmin =
          new RawAsyncHBaseAdmin(AsyncConnectionImpl.this, RETRY_TIMER, this);
        return new AsyncHBaseAdmin(rawAdmin, pool);
      }
    };
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    return new AsyncBufferedMutatorBuilderImpl(connConf, getTableBuilder(tableName), RETRY_TIMER);
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName,
      ExecutorService pool) {
    return new AsyncBufferedMutatorBuilderImpl(connConf, getTableBuilder(tableName, pool),
      RETRY_TIMER);
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    CompletableFuture<Hbck> future = new CompletableFuture<>();
    addListener(registry.getActiveMaster(), (sn, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        try {
          future.complete(getHbck(sn));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
      }
    });
    return future;
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    // we will not create a new connection when creating a new protobuf stub, and for hbck there
    // will be no performance consideration, so for simplification we will create a new stub every
    // time instead of caching the stub here.
    return new HBaseHbck(MasterProtos.HbckService.newBlockingStub(
      rpcClient.createBlockingRpcChannel(masterServer, user, rpcTimeout)), rpcControllerFactory);
  }

  @Override
  public void clearRegionLocationCache() {
    locator.clearCache();
  }

  Optional<MetricsConnection> getConnectionMetrics() {
    return metrics;
  }
}
