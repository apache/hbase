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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in 
 * hbase-server/src/test/protobuf/test.proto
 * and protobuf service definition from 
 * hbase-server/src/test/protobuf/test_rpc_service.proto
 */
@Category(MediumTests.class)
public class TestProtoBufRpc {
  public final static String ADDRESS = "0.0.0.0";
  public final static int PORT = 0;
  private static InetSocketAddress addr;
  private static Configuration conf;
  private static RpcServer server;

  public interface TestRpcService
      extends TestProtobufRpcProto.BlockingInterface, VersionedProtocol {
    public long VERSION = 1;
  }

  public static class PBServerImpl implements TestRpcService {

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

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }
  }

  @Before
  public  void setUp() throws IOException { // Setup server for both protocols
    conf = new Configuration();
    // Set RPC engine to protobuf RPC engine
    HBaseClientRPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcClientEngine.class);
    HBaseServerRPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcServerEngine.class);

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    // Get RPC server for server side implementation
    server = HBaseServerRPC.getServer(TestRpcService.class,serverImpl,
        new Class[]{TestRpcService.class}, 
        ADDRESS, PORT, 10, 10, true, conf, 0);
    addr = server.getListenerAddress();
    server.start();
  }
  
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  private static TestRpcService getClient() throws IOException {
    // Set RPC engine to protobuf RPC engine
    HBaseClientRPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcClientEngine.class);
    HBaseServerRPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcServerEngine.class);

    return (TestRpcService) HBaseClientRPC.getProxy(TestRpcService.class, 0,
        addr, conf, 10000);
  }

  @Test
  public void testProtoBufRpc() throws Exception {
    TestRpcService client = getClient();
    testProtoBufRpc(client);
  }
  
  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {  
    // Test ping method
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    client.ping(null, emptyRequest);
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");
    
    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, emptyRequest);
      Assert.fail("Expected exception is not thrown");
    } catch (ServiceException e) {
    }
  }
}
