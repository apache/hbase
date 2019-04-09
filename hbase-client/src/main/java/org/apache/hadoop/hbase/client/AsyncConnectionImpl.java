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

import static org.apache.hadoop.hbase.client.ConnectionUtils.NO_NONCE_GENERATOR;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getStubKey;
import static org.apache.hadoop.hbase.client.NonceGenerator.CLIENT_NONCES_ENABLED_KEY;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
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

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
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

  @VisibleForTesting
  static final HashedWheelTimer RETRY_TIMER = new HashedWheelTimer(
    Threads.newDaemonThreadFactory("Async-Client-Retry-Timer"), 10, TimeUnit.MILLISECONDS);

  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final Configuration conf;

  final AsyncConnectionConfiguration connConf;

  private final User user;

  final AsyncRegistry registry;

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

  private ChoreService authService;

  private volatile boolean closed = false;

  public AsyncConnectionImpl(Configuration conf, AsyncRegistry registry, String clusterId,
      User user) {
    this.conf = conf;
    this.user = user;
    if (user.isLoginFromKeytab()) {
      spawnRenewalChore(user.getUGI());
    }
    this.connConf = new AsyncConnectionConfiguration(conf);
    this.registry = registry;
    this.rpcClient = RpcClientFactory.createClient(conf, clusterId);
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
  }

  private void spawnRenewalChore(final UserGroupInformation user) {
    authService = new ChoreService("Relogin service");
    authService.scheduleChore(AuthUtil.getAuthRenewalChore(user));
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
    IOUtils.closeQuietly(rpcClient);
    IOUtils.closeQuietly(registry);
    if (authService != null) {
      authService.shutdown();
    }
    closed = true;
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
  @VisibleForTesting
  public NonceGenerator getNonceGenerator() {
    return nonceGenerator;
  }

  private ClientService.Interface createRegionServerStub(ServerName serverName) throws IOException {
    return ClientService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
  }

  ClientService.Interface getRegionServerStub(ServerName serverName) throws IOException {
    return ConcurrentMapUtils.computeIfAbsentEx(rsStubs,
      getStubKey(ClientService.Interface.class.getSimpleName(), serverName, hostnameCanChange),
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
      getStubKey(AdminService.Interface.class.getSimpleName(), serverName, hostnameCanChange),
      () -> createAdminServerStub(serverName));
  }

  CompletableFuture<MasterService.Interface> getMasterStub() {
    return ConnectionUtils.getOrFetch(masterStub, masterStubMakeFuture, false, () -> {
      CompletableFuture<MasterService.Interface> future = new CompletableFuture<>();
      addListener(registry.getMasterAddress(), (addr, error) -> {
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
    addListener(registry.getMasterAddress(), (sn, error) -> {
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
}
