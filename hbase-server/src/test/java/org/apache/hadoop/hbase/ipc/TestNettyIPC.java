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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.JVM;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestNettyIPC extends AbstractTestIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNettyIPC.class);

  @Parameters(name = "{index}: EventLoop={0}")
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] { "nio" });
    params.add(new Object[] { "perClientNio" });
    if (JVM.isLinux() && JVM.isAmd64()) {
      params.add(new Object[] { "epoll" });
    }
    return params;
  }

  @Parameter
  public String eventLoopType;

  private static NioEventLoopGroup NIO;

  private static EpollEventLoopGroup EPOLL;

  @BeforeClass
  public static void setUpBeforeClass() {
    NIO = new NioEventLoopGroup();
    if (JVM.isLinux() && JVM.isAmd64()) {
      EPOLL = new EpollEventLoopGroup();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (NIO != null) {
      NIO.shutdownGracefully();
    }
    if (EPOLL != null) {
      EPOLL.shutdownGracefully();
    }
  }

  private void setConf(Configuration conf) {
    switch (eventLoopType) {
      case "nio":
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, NIO, NioSocketChannel.class);
        break;
      case "epoll":
        NettyRpcClientConfigHelper.setEventLoopConfig(conf, EPOLL, EpollSocketChannel.class);
        break;
      case "perClientNio":
        NettyRpcClientConfigHelper.createEventLoopPerClient(conf);
        break;
      default:
        break;
    }
  }

  @Override
  protected RpcServer createRpcServer(Server server, String name,
      List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws IOException {
    return new NettyRpcServer(server, name, services, bindAddress, conf, scheduler, true);
  }

  @Override
  protected NettyRpcClient createRpcClientNoCodec(Configuration conf) {
    setConf(conf);
    return new NettyRpcClient(conf) {

      @Override
      Codec getCodec() {
        return null;
      }

    };
  }

  @Override
  protected NettyRpcClient createRpcClient(Configuration conf) {
    setConf(conf);
    return new NettyRpcClient(conf);
  }

  @Override
  protected NettyRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf) {
    setConf(conf);
    return new NettyRpcClient(conf) {

      @Override
      boolean isTcpNoDelay() {
        throw new RuntimeException("Injected fault");
      }
    };
  }

  private static class TestFailingRpcServer extends NettyRpcServer {

    TestFailingRpcServer(Server server, String name,
        List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
        Configuration conf, RpcScheduler scheduler) throws IOException {
      super(server, name, services, bindAddress, conf, scheduler, true);
    }

    static final class FailingConnection extends NettyServerRpcConnection {
      private FailingConnection(TestFailingRpcServer rpcServer, Channel channel) {
        super(rpcServer, channel);
      }

      @Override
      public void processRequest(ByteBuff buf) throws IOException, InterruptedException {
        // this will throw exception after the connection header is read, and an RPC is sent
        // from client
        throw new DoNotRetryIOException("Failing for test");
      }
    }

    @Override
    protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
      return new NettyRpcServerPreambleHandler(TestFailingRpcServer.this) {
        @Override
        protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
          return new FailingConnection(TestFailingRpcServer.this, channel);
        }
      };
    }
  }

  @Override
  protected RpcServer createTestFailingRpcServer(Server server, String name,
      List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws IOException {
    return new TestFailingRpcServer(server, name, services, bindAddress, conf, scheduler);
  }
}
