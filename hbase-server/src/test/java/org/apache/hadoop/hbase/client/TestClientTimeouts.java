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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

@Category({ MediumTests.class, ClientTests.class })
public class TestClientTimeouts {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientTimeouts.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static int SLAVES = 1;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Connection createConnection() {
    // Ensure the HBaseAdmin uses a new connection by changing Configuration.
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    // Set the custom RPC client with random timeouts as the client
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      RandomTimeoutRpcClient.class.getName());
    conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    for (;;) {
      try {
        return ConnectionFactory.createConnection(conf);
      } catch (IOException e) {
        // since we randomly throw SocketTimeoutException, it is possible that we fail when creating
        // the Connection, but this is not what we want to test here, so just ignore it and try
        // again
      }
    }
  }

  /**
   * Test that a client that fails an RPC to the master retries properly and doesn't throw any
   * unexpected exceptions.
   */
  @Test
  public void testAdminTimeout() throws Exception {
    try (Connection conn = createConnection(); Admin admin = conn.getAdmin()) {
      int initialInvocations = invokations.get();
      boolean balanceEnabled = admin.isBalancerEnabled();
      for (int i = 0; i < 5; i++) {
        assertEquals(balanceEnabled, admin.balancerSwitch(!balanceEnabled, false));
        balanceEnabled = !balanceEnabled;
      }
      // Ensure the RandomTimeoutRpcEngine is actually being used.
      assertTrue(invokations.get() > initialInvocations);
    }
  }

  /**
   * Rpc Channel implementation with RandomTimeoutBlockingRpcChannel
   */
  public static class RandomTimeoutRpcClient extends BlockingRpcClient {
    public RandomTimeoutRpcClient(Configuration conf, String clusterId, SocketAddress localAddr,
      MetricsConnection metrics) {
      super(conf, clusterId, localAddr, metrics);
    }

    // Return my own instance, one that does random timeouts
    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn, User ticket, int rpcTimeout) {
      return new RandomTimeoutBlockingRpcChannel(this, sn, ticket, rpcTimeout);
    }

    @Override
    public RpcChannel createRpcChannel(ServerName sn, User ticket, int rpcTimeout) {
      return new RandomTimeoutRpcChannel(this, sn, ticket, rpcTimeout);
    }
  }

  private static final double CHANCE_OF_TIMEOUT = 0.3;
  private static AtomicInteger invokations = new AtomicInteger();

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  static class RandomTimeoutBlockingRpcChannel
    extends AbstractRpcClient.BlockingRpcChannelImplementation {

    RandomTimeoutBlockingRpcChannel(final BlockingRpcClient rpcClient, final ServerName sn,
      final User ticket, final int rpcTimeout) {
      super(rpcClient, Address.fromParts(sn.getHostname(), sn.getPort()), ticket, rpcTimeout);
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller, Message param,
      Message returnType) throws ServiceException {
      invokations.getAndIncrement();
      if (ThreadLocalRandom.current().nextFloat() < CHANCE_OF_TIMEOUT) {
        // throw a ServiceException, becuase that is the only exception type that
        // {@link ProtobufRpcEngine} throws. If this RpcEngine is used with a different
        // "actual" type, this may not properly mimic the underlying RpcEngine.
        throw new ServiceException(new SocketTimeoutException("fake timeout"));
      }
      return super.callBlockingMethod(md, controller, param, returnType);
    }
  }

  private static class RandomTimeoutRpcChannel extends AbstractRpcClient.RpcChannelImplementation {

    RandomTimeoutRpcChannel(AbstractRpcClient<?> rpcClient, ServerName sn, User ticket,
      int rpcTimeout) {
      super(rpcClient, Address.fromParts(sn.getHostname(), sn.getPort()), ticket, rpcTimeout);
    }

    @Override
    public void callMethod(MethodDescriptor md, RpcController controller, Message param,
      Message returnType, RpcCallback<Message> done) {
      invokations.getAndIncrement();
      if (ThreadLocalRandom.current().nextFloat() < CHANCE_OF_TIMEOUT) {
        // throw a ServiceException, because that is the only exception type that
        // {@link ProtobufRpcEngine} throws. If this RpcEngine is used with a different
        // "actual" type, this may not properly mimic the underlying RpcEngine.
        ((HBaseRpcController) controller).setFailed(new SocketTimeoutException("fake timeout"));
        done.run(null);
        return;
      }
      super.callMethod(md, controller, param, returnType, done);
    }
  }
}
