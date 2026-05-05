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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ConnectionRegistryService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryResponse;

/**
 * Test that ConnectionRegistryRpcStubHolder properly completes its future when RPC client creation
 * fails. Before the fix, an exception thrown by RpcClientFactory.createClient() inside the
 * FutureUtils.addListener callback would be swallowed, leaving the CompletableFuture permanently
 * incomplete and hanging all callers.
 */
@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestConnectionRegistryRpcClientFailure {

  private static final String HEDGED_REQS_FANOUT_CONFIG_NAME = "hbase.test.hedged.reqs.fanout";
  private static final String INITIAL_DELAY_SECS_CONFIG_NAME =
    "hbase.test.refresh.initial.delay.secs";
  private static final String REFRESH_INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.refresh.interval.secs";
  private static final String MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.min.refresh.interval.secs";

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static Set<ServerName> BOOTSTRAP_NODES;

  /**
   * RPC client that succeeds for the preamble cluster ID fetch (clusterId is null) but throws on
   * the real client creation (clusterId is non-null). This simulates the production failure where
   * TLS certificate provisioning fails during RPC client construction.
   */
  public static final class FailingRpcClientImpl implements RpcClient {

    public FailingRpcClientImpl(Configuration configuration, String clusterId,
      SocketAddress localAddress, MetricsConnection metrics, Map<String, byte[]> attributes) {
      if (clusterId != null) {
        throw new RuntimeException("Simulated RPC client creation failure");
      }
    }

    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn, User user, int rpcTimeout) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RpcChannel createRpcChannel(ServerName sn, User user, int rpcTimeout) {
      return new PreambleOnlyRpcChannel();
    }

    @Override
    public void cancelConnections(ServerName sn) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasCellBlockSupport() {
      return false;
    }
  }

  /**
   * RPC channel that only handles the preamble GetConnectionRegistry call, returning a valid
   * cluster ID. All other RPCs are ignored.
   */
  public static final class PreambleOnlyRpcChannel implements RpcChannel {

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller, Message request,
      Message responsePrototype, RpcCallback<Message> done) {
      if (method.getService().equals(ConnectionRegistryService.getDescriptor())) {
        done.run(
          GetConnectionRegistryResponse.newBuilder().setClusterId("test-cluster-id").build());
      } else {
        controller.setFailed("unexpected call");
        done.run(null);
      }
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, FailingRpcClientImpl.class,
      RpcClient.class);
    conf.setLong(INITIAL_DELAY_SECS_CONFIG_NAME, Integer.MAX_VALUE);
    conf.setLong(REFRESH_INTERVAL_SECS_CONFIG_NAME, Integer.MAX_VALUE);
    conf.setLong(MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME, Integer.MAX_VALUE - 1);
    BOOTSTRAP_NODES = IntStream.range(0, 3)
      .mapToObj(i -> ServerName.valueOf("localhost", (10000 + 100 * i), ServerName.NON_STARTCODE))
      .collect(Collectors.toSet());
  }

  private AbstractRpcBasedConnectionRegistry createRegistry() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(HEDGED_REQS_FANOUT_CONFIG_NAME, 1);
    return new AbstractRpcBasedConnectionRegistry(conf, User.getCurrent(),
      HEDGED_REQS_FANOUT_CONFIG_NAME, INITIAL_DELAY_SECS_CONFIG_NAME,
      REFRESH_INTERVAL_SECS_CONFIG_NAME, MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME) {

      @Override
      protected Set<ServerName> getBootstrapNodes(Configuration conf) throws IOException {
        return BOOTSTRAP_NODES;
      }

      @Override
      protected CompletableFuture<Set<ServerName>> fetchEndpoints() {
        return CompletableFuture.completedFuture(BOOTSTRAP_NODES);
      }

      @Override
      public String getConnectionString() {
        return "unimplemented";
      }
    };
  }

  /**
   * Verify that when RPC client creation throws during stub initialization, the registry's future
   * completes exceptionally rather than hanging indefinitely.
   */
  @Test
  public void testRpcClientCreationFailureCompletesExceptionally() throws Exception {
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry()) {
      CompletableFuture<String> future = registry.getClusterId();
      // The future must complete within a few seconds. Before the fix, this would hang forever.
      IOException e = assertThrows(IOException.class,
        () -> FutureUtils.get(future, 5, TimeUnit.SECONDS));
      assertTrue(e.getCause().getMessage().contains("Simulated RPC client creation failure"),
        "Expected simulated failure in cause chain, got: " + e);
    }
  }

  /**
   * Verify that after a failed stub initialization, subsequent getClusterId() calls also fail
   * promptly rather than returning a zombie future.
   */
  @Test
  public void testSubsequentCallsAfterFailure() throws Exception {
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry()) {
      // First call triggers the failure
      CompletableFuture<String> first = registry.getClusterId();
      assertThrows(IOException.class, () -> FutureUtils.get(first, 5, TimeUnit.SECONDS));

      // Second call should also fail promptly, not return a stale zombie future
      CompletableFuture<String> second = registry.getClusterId();
      assertThrows(IOException.class, () -> FutureUtils.get(second, 5, TimeUnit.SECONDS));
    }
  }
}
