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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

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
@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: rpcServerImpl={0}")
public class TestProtoBufRpc {

  public final static String ADDRESS = "localhost";
  private static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;
  private final String rpcServerImpl;

  public TestProtoBufRpc(String rpcServerImpl) {
    this.rpcServerImpl = rpcServerImpl;
  }

  public static Stream<Arguments> parameters() {
    return Arrays.stream(new Object[] { SimpleRpcServer.class.getName(),
      NettyRpcServer.class.getName() }).map(Arguments::of);
  }

  @BeforeEach
  public void setUp() throws IOException { // Setup server for both protocols
    this.conf = HBaseConfiguration.create();
    this.conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
    Log4jUtils.setLogLevel("org.apache.hadoop.ipc.HBaseServer", "ERROR");
    Log4jUtils.setLogLevel("org.apache.hadoop.ipc.HBaseServer.trace", "TRACE");
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

  @AfterEach
  public void tearDown() throws Exception {
    server.stop();
  }

  @TestTemplate
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

      assertThrows(org.apache.hbase.thirdparty.com.google.protobuf.ServiceException.class,
        () -> stub.error(null, emptyRequest));
    } finally {
      rpcClient.close();
    }
  }
}
