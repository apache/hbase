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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in <code>src/test/protobuf/test.proto</code>
 * and protobuf service definition from <code>src/test/protobuf/test_rpc_service.proto</code>
 */
@Category(MediumTests.class)
public class TestProtoBufRpc {
  public final static String ADDRESS = "localhost";
  public static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;

  /**
   * Implementation of the test service defined out in TestRpcServiceProtos
   */
  static class PBServerImpl
  implements TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface {
    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public EmptyResponseProto error(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new IOException("error"));
    }
  }

  @Before
  public  void setUp() throws IOException { // Setup server for both protocols
    this.conf = HBaseConfiguration.create();
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer");
    log.setLevel(Level.DEBUG);
    log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer.trace");
    log.setLevel(Level.TRACE);
    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service =
      TestRpcServiceProtos.TestProtobufRpcProto.newReflectiveBlockingService(serverImpl);
    // Get RPC server for server side implementation
    this.server = new RpcServer(null, "testrpc",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
        new InetSocketAddress(ADDRESS, PORT), conf,
        new FifoRpcScheduler(conf, 10));
    this.isa = server.getListenerAddress();
    this.server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testProtoBufRpc() throws Exception {
    RpcClient rpcClient = new RpcClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(this.isa.getHostName(), this.isa.getPort(), System.currentTimeMillis()),
        User.getCurrent(), 0);
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
        TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(channel);
      // Test ping method
      TestProtos.EmptyRequestProto emptyRequest =
        TestProtos.EmptyRequestProto.newBuilder().build();
      stub.ping(null, emptyRequest);

      // Test echo method
      EchoRequestProto echoRequest = EchoRequestProto.newBuilder().setMessage("hello").build();
      EchoResponseProto echoResponse = stub.echo(null, echoRequest);
      Assert.assertEquals(echoResponse.getMessage(), "hello");

      // Test error method - error should be thrown as RemoteException
      try {
        stub.error(null, emptyRequest);
        Assert.fail("Expected exception is not thrown");
      } catch (ServiceException e) {
      }
    } finally {
      rpcClient.stop();
    }
  }
}
