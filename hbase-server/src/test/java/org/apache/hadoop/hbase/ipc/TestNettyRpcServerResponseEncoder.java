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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TestCellUtil;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

/**
 * Test ByteBuffAllocator can recycle ByteBuff resources correctly
 * when Netty rpc server enabled and client disconnect
 */
@Category(SmallTests.class)
public class TestNettyRpcServerResponseEncoder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNettyRpcServerResponseEncoder.class);

  public final static String ADDRESS = "localhost";
  public static int PORT = 0;
  private InetSocketAddress isa;
  private Configuration conf;
  private RpcServerInterface server;

  @Before
  public void setUp() throws IOException {
    this.conf = HBaseConfiguration.create();
    this.conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
        TestFailingRpcServer.class.getName());
    this.conf.set(ByteBuffAllocator.BUFFER_SIZE_KEY, "5");
    this.conf.set(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, "1");
    this.conf.set(ByteBuffAllocator.BUFFER_SIZE_KEY, "1024");
    this.server = createRpcServer("testrpc",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
        new InetSocketAddress(ADDRESS, PORT), conf, new FifoRpcScheduler(conf, 10));
    InetSocketAddress address = server.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    this.isa = address;
    this.server.start();
  }

  private RpcServerInterface createRpcServer(String name,
      ArrayList<BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, FifoRpcScheduler scheduler) throws IOException {
    return new TestFailingRpcServer(null, name, services, bindAddress, conf, scheduler);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  /**
   * Test we can recycle BB correctly when Client disconnect
   */
  @Test
  public void testRecycleBBCorrectly() throws Exception {
    RpcClient rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    assertEquals(0, server.getByteBuffAllocator().getFreeBufferCount());
    try {
      BlockingInterface stub = newBlockingStub(rpcClient, this.isa);
      // Test echo method
      EchoRequestProto echoRequest = EchoRequestProto.newBuilder().setMessage("hello").build();
      RpcController controller = new HBaseRpcControllerImpl(new TestCellUtil.TestCellScanner(3));
      EchoResponseProto echoResponse = null;
      try {
        echoResponse = stub.echo(controller, echoRequest);
      } catch (Exception e) {
        // expected, since we close the connection at server side
      }
      if (echoResponse != null) {
        assertEquals("hello", echoResponse.getMessage());
      }
      assertEquals(1, server.getByteBuffAllocator().getFreeBufferCount());
    } finally {
      rpcClient.close();
    }
  }

  private static class TestFailingRpcServer extends NettyRpcServer {
    TestFailingRpcServer(Server server, String name,
        List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
        Configuration conf, RpcScheduler scheduler) throws IOException {
      super(server, name, services, bindAddress, conf, scheduler, true);
    }

    @Override
    protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
      return new NettyRpcServerPreambleHandler(TestFailingRpcServer.this) {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
          super.channelRead0(ctx, msg);
          ChannelPipeline p = ctx.pipeline();
          // mock the client has disconnect
          p.addBefore("encoder", "close", new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws Exception {
              NettyRpcServerRequestDecoder decoder =
                  (NettyRpcServerRequestDecoder) ctx.pipeline().get("decoder");
              decoder.getConnection().close();
            }
          });
        }
      };
    }
  }
}
