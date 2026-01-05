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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.IOExceptionSupplier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;

/**
 * A class for creating {@link RpcClient} and related stubs used by
 * {@link AbstractRpcBasedConnectionRegistry}. We need to connect to bootstrap nodes to get the
 * cluster id first, before creating the final {@link RpcClient} and related stubs.
 * <p>
 * See HBASE-25051 for more details.
 */
@InterfaceAudience.Private
class ConnectionRegistryRpcStubHolder implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionRegistryRpcStubHolder.class);

  private final Configuration conf;

  // used for getting cluster id
  private final Configuration noAuthConf;

  private final User user;

  private final RpcControllerFactory rpcControllerFactory;

  private final Set<ServerName> bootstrapNodes;

  private final int rpcTimeoutMs;

  private volatile ImmutableMap<ServerName, ClientMetaService.Interface> addr2Stub;

  private volatile RpcClient rpcClient;

  private CompletableFuture<ImmutableMap<ServerName, ClientMetaService.Interface>> addr2StubFuture;

  ConnectionRegistryRpcStubHolder(Configuration conf, User user,
    RpcControllerFactory rpcControllerFactory, Set<ServerName> bootstrapNodes) {
    this.conf = conf;
    if (User.isHBaseSecurityEnabled(conf)) {
      this.noAuthConf = new Configuration(conf);
      this.noAuthConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    } else {
      this.noAuthConf = conf;
    }
    this.user = user;
    this.rpcControllerFactory = rpcControllerFactory;
    this.bootstrapNodes = Collections.unmodifiableSet(bootstrapNodes);
    this.rpcTimeoutMs = (int) Math.min(Integer.MAX_VALUE,
      conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
  }

  private ImmutableMap<ServerName, ClientMetaService.Interface> createStubs(RpcClient rpcClient,
    Collection<ServerName> addrs) {
    LOG.debug("Going to use new servers to create stubs: {}", addrs);
    Preconditions.checkNotNull(addrs);
    ImmutableMap.Builder<ServerName, ClientMetaService.Interface> builder =
      ImmutableMap.builderWithExpectedSize(addrs.size());
    for (ServerName masterAddr : addrs) {
      builder.put(masterAddr,
        ClientMetaService.newStub(rpcClient.createRpcChannel(masterAddr, user, rpcTimeoutMs)));
    }
    return builder.build();
  }

  private CompletableFuture<ImmutableMap<ServerName, ClientMetaService.Interface>>
    fetchClusterIdAndCreateStubs() {
    CompletableFuture<ImmutableMap<ServerName, ClientMetaService.Interface>> future =
      new CompletableFuture<>();
    addr2StubFuture = future;
    FutureUtils.addListener(
      new ClusterIdFetcher(noAuthConf, user, rpcControllerFactory, bootstrapNodes).fetchClusterId(),
      (clusterId, error) -> {
        synchronized (ConnectionRegistryRpcStubHolder.this) {
          if (error != null) {
            addr2StubFuture.completeExceptionally(error);
          } else {
            RpcClient c = RpcClientFactory.createClient(conf, clusterId);
            ImmutableMap<ServerName, ClientMetaService.Interface> m =
              createStubs(c, bootstrapNodes);
            rpcClient = c;
            addr2Stub = m;
            addr2StubFuture.complete(m);
          }
          addr2StubFuture = null;
        }
      });
    // here we must use the local variable future instead of addr2StubFuture, as the above listener
    // could be executed directly in the same thread(if the future completes quick enough), since
    // the synchronized lock is reentrant, it could set addr2StubFuture to null in the end, so when
    // arriving here the addr2StubFuture could be null.
    return future;
  }

  CompletableFuture<ImmutableMap<ServerName, ClientMetaService.Interface>> getStubs() {
    ImmutableMap<ServerName, ClientMetaService.Interface> s = this.addr2Stub;
    if (s != null) {
      return CompletableFuture.completedFuture(s);
    }
    synchronized (this) {
      s = this.addr2Stub;
      if (s != null) {
        return CompletableFuture.completedFuture(s);
      }
      if (addr2StubFuture != null) {
        return addr2StubFuture;
      }
      return fetchClusterIdAndCreateStubs();
    }
  }

  void refreshStubs(IOExceptionSupplier<Collection<ServerName>> fetchEndpoints) throws IOException {
    // There is no actual call yet so we have not initialize the rpc client and related stubs yet,
    // give up refreshing
    if (addr2Stub == null) {
      LOG.debug("Skip refreshing stubs as we have not initialized rpc client yet");
      return;
    }
    LOG.debug("Going to refresh stubs");
    assert rpcClient != null;
    addr2Stub = createStubs(rpcClient, fetchEndpoints.get());
  }

  @Override
  public void close() {
    if (rpcClient != null) {
      rpcClient.close();
    }
  }
}
