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
package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

/**
 * Test for testing protocol buffer based RPC mechanism. This test depends on test.proto definition
 * of types in <code>src/test/protobuf/test.proto</code> and protobuf service definition from
 * <code>src/test/protobuf/test_rpc_service.proto</code>
 */
@RunWith(Parameterized.class)
@Category({ RPCTests.class, SmallTests.class })
public class TestProtoBufRpc {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProtoBufRpc.class);

  public final static String ADDRESS = "localhost";
  private static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;

  @Parameters(name = "{index}: rpcServerImpl={0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[] { SimpleRpcServer.class.getName() },
      new Object[] { NettyRpcServer.class.getName() });
  }

  @Parameter(0)
  public String rpcServerImpl;

  @Before
  public void setUp() throws IOException { // Setup server for both protocols
    this.conf = HBaseConfiguration.create();
    this.conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.ipc.HBaseServer")
      .setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.ipc.HBaseServer.trace")
      .setLevel(org.apache.log4j.Level.TRACE);
    // Create server side implementation
    // Get RPC server for server side implementation
    this.server = RpcServerFactory.createRpcServer(null, "testrpc",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress(ADDRESS, PORT), conf, new FifoRpcScheduler(conf, 10));
    InetSocketAddress address = server.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    this.isa = address;
    this.server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test(expected = org.apache.hbase.thirdparty.com.google.protobuf.ServiceException.class
  /* Thrown when we call stub.error */)
  public void testProtoBufRpc() throws Exception {
    RpcClient rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    try {
      BlockingInterface stub = newBlockingStub(rpcClient, this.isa);
      // Test ping method
      TestProtos.EmptyRequestProto emptyRequest = TestProtos.EmptyRequestProto.newBuilder().build();
      stub.ping(null, emptyRequest);

      // Test echo method
      EchoRequestProto echoRequest = EchoRequestProto.newBuilder().setMessage("hello").build();
      EchoResponseProto echoResponse = stub.echo(null, echoRequest);
      assertEquals("hello", echoResponse.getMessage());

      stub.error(null, emptyRequest);
      fail("Expected exception is not thrown");
    } finally {
      rpcClient.close();
    }
  }
}
