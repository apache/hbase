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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;

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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.util.JVM;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * An RPC server with Netty4 implementation.
 */
@InterfaceAudience.Private
public class NettyRpcServer extends RpcServer {

  public static final Log LOG = LogFactory.getLog(NettyRpcServer.class);

  protected final InetSocketAddress bindAddress;

  private final CountDownLatch closed = new CountDownLatch(1);
  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);;

  public NettyRpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler) throws IOException {
    super(server, name, services, bindAddress, conf, scheduler);
    this.bindAddress = bindAddress;
    boolean useEpoll = useEpoll(conf);
    int workerCount = conf.getInt("hbase.netty.rpc.server.worker.count",
        Runtime.getRuntime().availableProcessors() / 4);
    EventLoopGroup bossGroup = null;
    EventLoopGroup workerGroup = null;
    if (useEpoll) {
      bossGroup = new EpollEventLoopGroup(1);
      workerGroup = new EpollEventLoopGroup(workerCount);
    } else {
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup(workerCount);
    }
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup);
    if (useEpoll) {
      bootstrap.channel(EpollServerSocketChannel.class);
    } else {
      bootstrap.channel(NioServerSocketChannel.class);
    }
    bootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);
    bootstrap.childOption(ChannelOption.ALLOCATOR,
        PooledByteBufAllocator.DEFAULT);
    bootstrap.childHandler(new Initializer(maxRequestSize));

    try {
      serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
      LOG.info("NettyRpcServer bind to address=" + serverChannel.localAddress()
          + ", hbase.netty.rpc.server.worker.count=" + workerCount
          + ", useEpoll=" + useEpoll);
      allChannels.add(serverChannel);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    initReconfigurable(conf);
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  private static boolean useEpoll(Configuration conf) {
    // Config to enable native transport.
    boolean epollEnabled = conf.getBoolean("hbase.rpc.server.nativetransport",
        true);
    // Use the faster native epoll transport mechanism on linux if enabled
    return epollEnabled && JVM.isLinux() && JVM.isAmd64();
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

  private class Initializer extends ChannelInitializer<SocketChannel> {

    final int maxRequestSize;

    Initializer(int maxRequestSize) {
      this.maxRequestSize = maxRequestSize;
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast("header", new ConnectionHeaderHandler());
      pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
          maxRequestSize, 0, 4, 0, 4, true));
      pipeline.addLast("decoder", new MessageDecoder());
      pipeline.addLast("encoder", new MessageEncoder());
    }

  }

  private class ConnectionHeaderHandler extends ByteToMessageDecoder {
    private NettyServerRpcConnection connection;

    ConnectionHeaderHandler() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf,
        List<Object> out) throws Exception {
      if (byteBuf.readableBytes() < 6) {
        return;
      }
      connection = new NettyServerRpcConnection(NettyRpcServer.this, ctx.channel());
      connection.readPreamble(byteBuf);
      ((MessageDecoder) ctx.pipeline().get("decoder"))
          .setConnection(connection);
      ctx.pipeline().remove(this);
    }

  }

  private class MessageDecoder extends ChannelInboundHandlerAdapter {

    private NettyServerRpcConnection connection;

    void setConnection(NettyServerRpcConnection connection) {
      this.connection = connection;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connection from " + ctx.channel().remoteAddress()
            + "; # active connections: " + getNumOpenConnections());
      }
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      ByteBuf input = (ByteBuf) msg;
      // 4 bytes length field
      metrics.receivedBytes(input.readableBytes() + 4);
      connection.process(input);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      allChannels.remove(ctx.channel());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disconnecting client: " + ctx.channel().remoteAddress()
            + ". Number of active connections: " + getNumOpenConnections());
      }
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      allChannels.remove(ctx.channel());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connection from " + ctx.channel().remoteAddress()
            + " catch unexpected exception from downstream.", e.getCause());
      }
      ctx.channel().close();
    }

  }

  private class MessageEncoder extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      final NettyServerCall call = (NettyServerCall) msg;
      ByteBuf response = Unpooled.wrappedBuffer(call.response.getBuffers());
      ctx.write(response, promise).addListener(new CallWriteListener(call));
    }

  }

  private class CallWriteListener implements ChannelFutureListener {

    private NettyServerCall call;

    CallWriteListener(NettyServerCall call) {
      this.call = call;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      call.done();
      if (future.isSuccess()) {
        metrics.sentBytes(call.response.size());
      }
    }

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
