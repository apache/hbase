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

import org.apache.hadoop.hbase.shaded.io.netty.bootstrap.ServerBootstrap;
import org.apache.hadoop.hbase.shaded.io.netty.channel.Channel;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ChannelInitializer;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ChannelOption;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ChannelPipeline;
import org.apache.hadoop.hbase.shaded.io.netty.channel.EventLoopGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ServerChannel;
import org.apache.hadoop.hbase.shaded.io.netty.channel.group.ChannelGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hadoop.hbase.shaded.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hadoop.hbase.shaded.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Server;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * An RPC server with Netty4 implementation.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class NettyRpcServer extends RpcServer {

  public static final Log LOG = LogFactory.getLog(NettyRpcServer.class);

  private final InetSocketAddress bindAddress;

  private final CountDownLatch closed = new CountDownLatch(1);
  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  public NettyRpcServer(Server server, String name, List<BlockingServiceAndInterface> services,
      InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler)
      throws IOException {
    super(server, name, services, bindAddress, conf, scheduler);
    this.bindAddress = bindAddress;
    EventLoopGroup eventLoopGroup;
    Class<? extends ServerChannel> channelClass;
    if (server instanceof HRegionServer) {
      NettyEventLoopGroupConfig config = ((HRegionServer) server).getEventLoopGroupConfig();
      eventLoopGroup = config.group();
      channelClass = config.serverChannelClass();
    } else {
      eventLoopGroup = new NioEventLoopGroup(0,
          new DefaultThreadFactory("NettyRpcServer", true, Thread.MAX_PRIORITY));
      channelClass = NioServerSocketChannel.class;
    }
    ServerBootstrap bootstrap = new ServerBootstrap().group(eventLoopGroup).channel(channelClass)
        .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
        .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
        .childHandler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
            preambleDecoder.setSingleDecode(true);
            pipeline.addLast("preambleDecoder", preambleDecoder);
            pipeline.addLast("preambleHandler",
              new NettyRpcServerPreambleHandler(NettyRpcServer.this));
            pipeline.addLast("frameDecoder",
              new LengthFieldBasedFrameDecoder(maxRequestSize, 0, 4, 0, 4, true));
            pipeline.addLast("decoder", new NettyRpcServerRequestDecoder(allChannels, metrics));
            pipeline.addLast("encoder", new NettyRpcServerResponseEncoder(metrics));
          }
        });
    try {
      serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
      LOG.info("NettyRpcServer bind to address=" + serverChannel.localAddress());
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    initReconfigurable(conf);
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  @Override
  public synchronized void start() {
    if (started) {
      return;
    }
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      setSecretManager(authTokenSecretMgr);
      authTokenSecretMgr.start();
    }
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    scheduler.start();
    started = true;
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    LOG.info("Stopping server on " + this.bindAddress.getPort());
    if (authTokenSecretMgr != null) {
      authTokenSecretMgr.stop();
      authTokenSecretMgr = null;
    }
    allChannels.close().awaitUninterruptibly();
    serverChannel.close();
    scheduler.stop();
    closed.countDown();
    running = false;
  }

  @Override
  public synchronized void join() throws InterruptedException {
    closed.await();
  }

  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    return ((InetSocketAddress) serverChannel.localAddress());
  }

  @Override
  public void setSocketSendBufSize(int size) {
  }

  @Override
  public int getNumOpenConnections() {
    // allChannels also contains the server channel, so exclude that from the count.
    return allChannels.size() - 1;
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service,
      MethodDescriptor md, Message param, CellScanner cellScanner,
      long receiveTime, MonitoredRPCHandler status) throws IOException {
    return call(service, md, param, cellScanner, receiveTime, status,
        System.currentTimeMillis(), 0);
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status,
      long startTime, int timeout) throws IOException {
    NettyServerCall fakeCall = new NettyServerCall(-1, service, md, null, param, cellScanner, null,
        -1, null, null, receiveTime, timeout, reservoir, cellBlockBuilder, null);
    return call(fakeCall, status);
  }
}
