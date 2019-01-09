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
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningResponse;
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

  public AsyncConnectionImpl(Configuration conf, AsyncRegistry registry, String clusterId,
      User user) {
    this.conf = conf;
    this.user = user;
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

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(rpcClient);
    IOUtils.closeQuietly(registry);
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new AsyncTableRegionLocatorImpl(tableName, locator);
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
    return CollectionUtils.computeIfAbsentEx(rsStubs,
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
    return CollectionUtils.computeIfAbsentEx(adminSubs,
      getStubKey(AdminService.Interface.class.getSimpleName(), serverName, hostnameCanChange),
      () -> createAdminServerStub(serverName));
  }

  private void makeMasterStub(CompletableFuture<MasterService.Interface> future) {
    registry.getMasterAddress().whenComplete((sn, error) -> {
      if (sn == null) {
        String msg = "ZooKeeper available but no active master location found";
        LOG.info(msg);
        this.masterStubMakeFuture.getAndSet(null)
          .completeExceptionally(new MasterNotRunningException(msg));
        return;
      }
      try {
        MasterService.Interface stub = createMasterStub(sn);
        HBaseRpcController controller = getRpcController();
        stub.isMasterRunning(controller, RequestConverter.buildIsMasterRunningRequest(),
          new RpcCallback<IsMasterRunningResponse>() {
            @Override
            public void run(IsMasterRunningResponse resp) {
              if (controller.failed() || resp == null ||
                (resp != null && !resp.getIsMasterRunning())) {
                masterStubMakeFuture.getAndSet(null).completeExceptionally(
                  new MasterNotRunningException("Master connection is not running anymore"));
              } else {
                masterStub.set(stub);
                masterStubMakeFuture.set(null);
                future.complete(stub);
              }
            }
          });
      } catch (IOException e) {
        this.masterStubMakeFuture.getAndSet(null)
          .completeExceptionally(new IOException("Failed to create async master stub", e));
      }
    });
  }

  CompletableFuture<MasterService.Interface> getMasterStub() {
    MasterService.Interface masterStub = this.masterStub.get();

    if (masterStub == null) {
      for (;;) {
        if (this.masterStubMakeFuture.compareAndSet(null, new CompletableFuture<>())) {
          CompletableFuture<MasterService.Interface> future = this.masterStubMakeFuture.get();
          makeMasterStub(future);
        } else {
          CompletableFuture<MasterService.Interface> future = this.masterStubMakeFuture.get();
          if (future != null) {
            return future;
          }
        }
      }
    }

    for (;;) {
      if (masterStubMakeFuture.compareAndSet(null, new CompletableFuture<>())) {
        CompletableFuture<MasterService.Interface> future = masterStubMakeFuture.get();
        HBaseRpcController controller = getRpcController();
        masterStub.isMasterRunning(controller, RequestConverter.buildIsMasterRunningRequest(),
          new RpcCallback<IsMasterRunningResponse>() {
            @Override
            public void run(IsMasterRunningResponse resp) {
              if (controller.failed() || resp == null ||
                (resp != null && !resp.getIsMasterRunning())) {
                makeMasterStub(future);
              } else {
                future.complete(masterStub);
              }
            }
          });
      } else {
        CompletableFuture<MasterService.Interface> future = masterStubMakeFuture.get();
        if (future != null) {
          return future;
        }
      }
    }
  }

  private HBaseRpcController getRpcController() {
    HBaseRpcController controller = this.rpcControllerFactory.newController();
    controller.setCallTimeout((int) TimeUnit.NANOSECONDS.toMillis(connConf.getRpcTimeoutNs()));
    return controller;
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return new AsyncTableBuilderBase<AdvancedScanResultConsumer>(tableName, connConf) {

      @Override
      public AsyncTable<AdvancedScanResultConsumer> build() {
        return new RawAsyncTableImpl(AsyncConnectionImpl.this, this);
      }
    };
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(TableName tableName,
      ExecutorService pool) {
    return new AsyncTableBuilderBase<ScanResultConsumer>(tableName, connConf) {

      @Override
      public AsyncTable<ScanResultConsumer> build() {
        RawAsyncTableImpl rawTable = new RawAsyncTableImpl(AsyncConnectionImpl.this, this);
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
}
