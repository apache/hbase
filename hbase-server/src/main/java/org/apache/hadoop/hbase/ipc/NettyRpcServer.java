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

import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_SERVER_NETTY_TLS_ENABLED;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.exceptions.X509Exception;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.ServerChannel;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hbase.thirdparty.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.OptionalSslHandler;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GlobalEventExecutor;

/**
 * An RPC server with Netty4 implementation.
 * @since 2.0.0
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.CONFIG })
public class NettyRpcServer extends RpcServer {
  public static final Logger LOG = LoggerFactory.getLogger(NettyRpcServer.class);

  /**
   * Name of property to change the byte buf allocator for the netty channels. Default is no value,
   * which causes us to use PooledByteBufAllocator. Valid settings here are "pooled", "unpooled",
   * and "heap", or, the name of a class implementing ByteBufAllocator.
   * <p>
   * "pooled" and "unpooled" may prefer direct memory depending on netty configuration, which is
   * controlled by platform specific code and documented system properties.
   * <p>
   * "heap" will prefer heap arena allocations.
   */
  public static final String HBASE_NETTY_ALLOCATOR_KEY = "hbase.netty.rpcserver.allocator";
  static final String POOLED_ALLOCATOR_TYPE = "pooled";
  static final String UNPOOLED_ALLOCATOR_TYPE = "unpooled";
  static final String HEAP_ALLOCATOR_TYPE = "heap";

  private final InetSocketAddress bindAddress;

  private final CountDownLatch closed = new CountDownLatch(1);
  private final Channel serverChannel;
  final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
  private final ByteBufAllocator channelAllocator;

  public NettyRpcServer(Server server, String name, List<BlockingServiceAndInterface> services,
    InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler,
    boolean reservoirEnabled) throws IOException {
    super(server, name, services, bindAddress, conf, scheduler, reservoirEnabled);
    this.bindAddress = bindAddress;
    this.channelAllocator = getChannelAllocator(conf);
    // Get the event loop group configuration from the server class if available.
    NettyEventLoopGroupConfig config = null;
    if (server instanceof HRegionServer) {
      config = ((HRegionServer) server).getEventLoopGroupConfig();
    }
    if (config == null) {
      config = new NettyEventLoopGroupConfig(conf, "NettyRpcServer");
    }
    EventLoopGroup eventLoopGroup = config.group();
    Class<? extends ServerChannel> channelClass = config.serverChannelClass();
    ServerBootstrap bootstrap = new ServerBootstrap().group(eventLoopGroup).channel(channelClass)
      .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
      .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
      .childOption(ChannelOption.SO_REUSEADDR, true)
      .childHandler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          ch.config().setAllocator(channelAllocator);
          ChannelPipeline pipeline = ch.pipeline();
          FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
          preambleDecoder.setSingleDecode(true);
          if (conf.getBoolean(HBASE_SERVER_NETTY_TLS_ENABLED, false)) {
            initSSL(pipeline, conf.getBoolean(HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, true));
          }
          pipeline.addLast(NettyRpcServerPreambleHandler.DECODER_NAME, preambleDecoder)
            .addLast(createNettyRpcServerPreambleHandler());
        }
      });
    try {
      serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
      LOG.info("Bind to {}", serverChannel.localAddress());
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    initReconfigurable(conf);
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  private ByteBufAllocator getChannelAllocator(Configuration conf) throws IOException {
    final String value = conf.get(HBASE_NETTY_ALLOCATOR_KEY);
    if (value != null) {
      if (POOLED_ALLOCATOR_TYPE.equalsIgnoreCase(value)) {
        LOG.info("Using {} for buffer allocation", PooledByteBufAllocator.class.getName());
        return PooledByteBufAllocator.DEFAULT;
      } else if (UNPOOLED_ALLOCATOR_TYPE.equalsIgnoreCase(value)) {
        LOG.info("Using {} for buffer allocation", UnpooledByteBufAllocator.class.getName());
        return UnpooledByteBufAllocator.DEFAULT;
      } else if (HEAP_ALLOCATOR_TYPE.equalsIgnoreCase(value)) {
        LOG.info("Using {} for buffer allocation", HeapByteBufAllocator.class.getName());
        return HeapByteBufAllocator.DEFAULT;
      } else {
        // If the value is none of the recognized labels, treat it as a class name. This allows the
        // user to supply a custom implementation, perhaps for debugging.
        try {
          // ReflectionUtils throws UnsupportedOperationException if there are any problems.
          ByteBufAllocator alloc = (ByteBufAllocator) ReflectionUtils.newInstance(value);
          LOG.info("Using {} for buffer allocation", value);
          return alloc;
        } catch (ClassCastException | UnsupportedOperationException e) {
          throw new IOException(e);
        }
      }
    } else {
      LOG.info("Using {} for buffer allocation", PooledByteBufAllocator.class.getName());
      return PooledByteBufAllocator.DEFAULT;
    }
  }

  // will be overriden in tests
  @InterfaceAudience.Private
  protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
    return new NettyRpcServerPreambleHandler(NettyRpcServer.this);
  }

  @Override
  public synchronized void start() {
    if (started) {
      return;
    }
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      // Start AuthenticationTokenSecretManager in synchronized way to avoid race conditions in
      // LeaderElector start. See HBASE-25875
      synchronized (authTokenSecretMgr) {
        setSecretManager(authTokenSecretMgr);
        authTokenSecretMgr.start();
      }
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
    LOG.info("Stopping server on " + this.serverChannel.localAddress());
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
    return allChannels.size();
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
    Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
    throws IOException {
    return call(service, md, param, cellScanner, receiveTime, status,
      EnvironmentEdgeManager.currentTime(), 0);
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
    Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status,
    long startTime, int timeout) throws IOException {
    NettyServerCall fakeCall = new NettyServerCall(-1, service, md, null, param, cellScanner, null,
      -1, null, receiveTime, timeout, bbAllocator, cellBlockBuilder, null);
    return call(fakeCall, status);
  }

  private void initSSL(ChannelPipeline p, boolean supportPlaintext)
    throws X509Exception, IOException {
    SslContext nettySslContext = X509Util.createSslContextForServer(conf);

    if (supportPlaintext) {
      p.addLast("ssl", new OptionalSslHandler(nettySslContext));
      LOG.debug("Dual mode SSL handler added for channel: {}", p.channel());
    } else {
      p.addLast("ssl", nettySslContext.newHandler(p.channel().alloc()));
      LOG.debug("SSL handler added for channel: {}", p.channel());
    }
  }
}
