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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for testing protocol buffer based RPC mechanism. This test depends on test.proto definition
 * of types in <code>src/test/protobuf/test.proto</code> and protobuf service definition from
 * <code>src/test/protobuf/test_rpc_service.proto</code>
 */
@Category({ RPCTests.class, MediumTests.class })
public class TestProtoBufRpc {
  public final static String ADDRESS = "localhost";
  public static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;

  @Before
  public void setUp() throws IOException { // Setup server for both protocols
    this.conf = HBaseConfiguration.create();
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer");
    log.setLevel(Level.DEBUG);
    log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer.trace");
    log.setLevel(Level.TRACE);
    // Create server side implementation
    // Get RPC server for server side implementation
    this.server = new RpcServer(null, "testrpc",
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

  @Test
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
      assertEquals(echoResponse.getMessage(), "hello");

      // Test error method - error should be thrown as RemoteException
      try {
        stub.error(null, emptyRequest);
        fail("Expected exception is not thrown");
      } catch (ServiceException e) {
      }
    } finally {
      rpcClient.close();
    }
  }
}
