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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ConnectionRegistryService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryResponse;

/**
 * Fetch cluster id through special preamble header.
 * <p>
 * An instance of this class should only be used once, like:
 *
 * <pre>
 * new ClusterIdFetcher().fetchClusterId()
 * </pre>
 *
 * Calling the fetchClusterId multiple times will lead unexpected behavior.
 * <p>
 * See HBASE-25051 for more details.
 */
@InterfaceAudience.Private
class ClusterIdFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterIdFetcher.class);

  private final List<ServerName> bootstrapServers;

  private final User user;

  private final RpcClient rpcClient;

  private final RpcControllerFactory rpcControllerFactory;

  private final CompletableFuture<String> future;

  ClusterIdFetcher(Configuration conf, User user, RpcControllerFactory rpcControllerFactory,
    Set<ServerName> bootstrapServers) {
    this.user = user;
    // use null cluster id here as we do not know the cluster id yet, we will fetch it through this
    // rpc client
    this.rpcClient = RpcClientFactory.createClient(conf, null);
    this.rpcControllerFactory = rpcControllerFactory;
    this.bootstrapServers = new ArrayList<ServerName>(bootstrapServers);
    // shuffle the bootstrap servers so we will not always fetch from the same one
    Collections.shuffle(this.bootstrapServers);
    future = new CompletableFuture<String>();
  }

  /**
   * Try get cluster id from the server with the given {@code index} in {@link #bootstrapServers}.
   */
  private void getClusterId(int index) {
    ServerName server = bootstrapServers.get(index);
    LOG.debug("Going to request {} for getting cluster id", server);
    // user and rpcTimeout are both not important here, as we will not actually send any rpc calls
    // out, only a preamble connection header, but if we pass null as user, there will be NPE in
    // some code paths...
    RpcChannel channel = rpcClient.createRpcChannel(server, user, 0);
    ConnectionRegistryService.Interface stub = ConnectionRegistryService.newStub(channel);
    HBaseRpcController controller = rpcControllerFactory.newController();
    stub.getConnectionRegistry(controller, GetConnectionRegistryRequest.getDefaultInstance(),
      new RpcCallback<GetConnectionRegistryResponse>() {

        @Override
        public void run(GetConnectionRegistryResponse resp) {
          if (!controller.failed()) {
            LOG.debug("Got connection registry info: {}", resp);
            future.complete(resp.getClusterId());
            return;
          }
          if (ConnectionUtils.isUnexpectedPreambleHeaderException(controller.getFailed())) {
            // this means we have connected to an old server where it does not support passing
            // cluster id through preamble connnection header, so we fallback to use null
            // cluster id, which is the old behavior
            LOG.debug("Failed to get connection registry info, should be an old server,"
              + " fallback to use null cluster id", controller.getFailed());
            future.complete(null);
          } else {
            LOG.debug("Failed to get connection registry info", controller.getFailed());
            if (index == bootstrapServers.size() - 1) {
              future.completeExceptionally(controller.getFailed());
            } else {
              // try next bootstrap server
              getClusterId(index + 1);
            }
          }
        }
      });

  }

  CompletableFuture<String> fetchClusterId() {
    getClusterId(0);
    // close the rpc client after we finish the request
    FutureUtils.addListener(future, (r, e) -> rpcClient.close());
    return future;
  }
}
