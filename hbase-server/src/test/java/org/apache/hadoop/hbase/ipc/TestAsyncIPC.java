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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, SmallTests.class })
public class TestAsyncIPC extends AbstractTestIPC {

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<>();
    paramList.add(new Object[] { false, false });
    paramList.add(new Object[] { false, true });
    paramList.add(new Object[] { true, false });
    paramList.add(new Object[] { true, true });
    return paramList;
  }

  private final boolean useNativeTransport;

  private final boolean useGlobalEventLoopGroup;

  public TestAsyncIPC(boolean useNativeTransport, boolean useGlobalEventLoopGroup) {
    this.useNativeTransport = useNativeTransport;
    this.useGlobalEventLoopGroup = useGlobalEventLoopGroup;
  }

  private void setConf(Configuration conf) {
    conf.setBoolean(AsyncRpcClient.USE_NATIVE_TRANSPORT, useNativeTransport);
    conf.setBoolean(AsyncRpcClient.USE_GLOBAL_EVENT_LOOP_GROUP, useGlobalEventLoopGroup);
    if (useGlobalEventLoopGroup && AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP != null) {
      if (useNativeTransport
          && !(AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP.getFirst() instanceof EpollEventLoopGroup)
          || (!useNativeTransport && !(AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP
              .getFirst() instanceof NioEventLoopGroup))) {
        AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP.getFirst().shutdownGracefully();
        AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP = null;
      }
    }
  }

  @Override
  protected AsyncRpcClient createRpcClientNoCodec(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf) {

      @Override
      Codec getCodec() {
        return null;
      }

    };
  }

  @Override
  protected AsyncRpcClient createRpcClient(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf);
  }

  @Override
  protected AsyncRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf) {
    setConf(conf);
    return new AsyncRpcClient(conf, new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
          @Override
          public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
              throws Exception {
            promise.setFailure(new RuntimeException("Injected fault"));
          }
        });
      }
    });
  }
}
