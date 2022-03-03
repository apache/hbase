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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdResponse;

@Category({ ClientTests.class, SmallTests.class })
public class TestRpcBasedRegistryHedgedReads {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcBasedRegistryHedgedReads.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRpcBasedRegistryHedgedReads.class);

  private static final String HEDGED_REQS_FANOUT_CONFIG_NAME = "hbase.test.hedged.reqs.fanout";
  private static final String INITIAL_DELAY_SECS_CONFIG_NAME =
    "hbase.test.refresh.initial.delay.secs";
  private static final String REFRESH_INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.refresh.interval.secs";
  private static final String MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.min.refresh.interval.secs";

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static final ExecutorService EXECUTOR =
    Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());

  private static Set<ServerName> BOOTSTRAP_NODES;

  private static AtomicInteger CALLED = new AtomicInteger(0);

  private static volatile int BAD_RESP_INDEX;

  private static volatile Set<Integer> GOOD_RESP_INDEXS;

  private static GetClusterIdResponse RESP =
    GetClusterIdResponse.newBuilder().setClusterId("id").build();

  public static final class RpcClientImpl implements RpcClient {

    public RpcClientImpl(Configuration configuration, String clusterId, SocketAddress localAddress,
      MetricsConnection metrics) {
    }

    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn, User user, int rpcTimeout) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RpcChannel createRpcChannel(ServerName sn, User user, int rpcTimeout) {
      return new RpcChannelImpl();
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
   * A dummy RpcChannel implementation that intercepts the GetClusterId() RPC calls and injects
   * errors. All other RPCs are ignored.
   */
  public static final class RpcChannelImpl implements RpcChannel {

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller, Message request,
      Message responsePrototype, RpcCallback<Message> done) {
      if (!method.getName().equals("GetClusterId")) {
        // On RPC failures, MasterRegistry internally runs getMasters() RPC to keep the master list
        // fresh. We do not want to intercept those RPCs here and double count.
        return;
      }
      // simulate the asynchronous behavior otherwise all logic will perform in the same thread...
      EXECUTOR.execute(() -> {
        int index = CALLED.getAndIncrement();
        if (index == BAD_RESP_INDEX) {
          done.run(GetClusterIdResponse.getDefaultInstance());
        } else if (GOOD_RESP_INDEXS.contains(index)) {
          done.run(RESP);
        } else {
          controller.setFailed("inject error");
          done.run(null);
        }
      });
    }
  }

  private AbstractRpcBasedConnectionRegistry createRegistry(int hedged) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(HEDGED_REQS_FANOUT_CONFIG_NAME, hedged);
    return new AbstractRpcBasedConnectionRegistry(conf, HEDGED_REQS_FANOUT_CONFIG_NAME,
      INITIAL_DELAY_SECS_CONFIG_NAME, REFRESH_INTERVAL_SECS_CONFIG_NAME,
      MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME) {

      @Override
      protected Set<ServerName> getBootstrapNodes(Configuration conf) throws IOException {
        return BOOTSTRAP_NODES;
      }

      @Override
      protected CompletableFuture<Set<ServerName>> fetchEndpoints() {
        return CompletableFuture.completedFuture(BOOTSTRAP_NODES);
      }

      @Override public String getConnectionString() {
        return "unimplemented";
      }
    };
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, RpcClientImpl.class,
      RpcClient.class);
    // disable refresh, we do not need to refresh in this test
    conf.setLong(INITIAL_DELAY_SECS_CONFIG_NAME, Integer.MAX_VALUE);
    conf.setLong(REFRESH_INTERVAL_SECS_CONFIG_NAME, Integer.MAX_VALUE);
    conf.setLong(MIN_REFRESH_INTERVAL_SECS_CONFIG_NAME, Integer.MAX_VALUE - 1);
    BOOTSTRAP_NODES = IntStream.range(0, 10)
      .mapToObj(i -> ServerName.valueOf("localhost", (10000 + 100 * i), ServerName.NON_STARTCODE))
      .collect(Collectors.toSet());
  }

  @AfterClass
  public static void tearDownAfterClass() {
    EXECUTOR.shutdownNow();
  }

  @Before
  public void setUp() {
    CALLED.set(0);
    BAD_RESP_INDEX = -1;
    GOOD_RESP_INDEXS = Collections.emptySet();
  }

  private <T> T logIfError(CompletableFuture<T> future) throws IOException {
    try {
      return FutureUtils.get(future);
    } catch (Throwable t) {
      LOG.warn("", t);
      throw t;
    }
  }

  @Test
  public void testAllFailNoHedged() throws IOException {
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry(1)) {
      assertThrows(IOException.class, () -> logIfError(registry.getClusterId()));
      assertEquals(10, CALLED.get());
    }
  }

  @Test
  public void testAllFailHedged3() throws IOException {
    BAD_RESP_INDEX = 5;
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry(3)) {
      assertThrows(IOException.class, () -> logIfError(registry.getClusterId()));
      assertEquals(10, CALLED.get());
    }
  }

  @Test
  public void testFirstSucceededNoHedge() throws IOException {
    GOOD_RESP_INDEXS =
      IntStream.range(0, 10).mapToObj(Integer::valueOf).collect(Collectors.toSet());
    // will be set to 1
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry(0)) {
      String clusterId = logIfError(registry.getClusterId());
      assertEquals(RESP.getClusterId(), clusterId);
      assertEquals(1, CALLED.get());
    }
  }

  @Test
  public void testSecondRoundSucceededHedge4() throws IOException {
    GOOD_RESP_INDEXS = Collections.singleton(6);
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry(4)) {
      String clusterId = logIfError(registry.getClusterId());
      assertEquals(RESP.getClusterId(), clusterId);
      UTIL.waitFor(5000, () -> CALLED.get() == 8);
    }
  }

  @Test
  public void testSucceededWithLargestHedged() throws IOException, InterruptedException {
    GOOD_RESP_INDEXS = Collections.singleton(5);
    try (AbstractRpcBasedConnectionRegistry registry = createRegistry(Integer.MAX_VALUE)) {
      String clusterId = logIfError(registry.getClusterId());
      assertEquals(RESP.getClusterId(), clusterId);
      UTIL.waitFor(5000, () -> CALLED.get() == 10);
      Thread.sleep(1000);
      // make sure we do not send more
      assertEquals(10, CALLED.get());
    }
  }
}
