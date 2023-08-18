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
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.TLS_CONFIG_REVERSE_DNS_LOOKUP_ENABLED;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.FileChangeWatcher;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.exceptions.X509Exception;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.NettyUnsafeUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.ServerChannel;
import org.apache.hbase.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hbase.thirdparty.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.OptionalSslHandler;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GlobalEventExecutor;

/**
 * An RPC server with Netty4 implementation.
 * @since 2.0.0
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.CONFIG })
public class NettyRpcServer extends RpcServer {
  public static final Logger LOG = LoggerFactory.getLogger(NettyRpcServer.class);

  /**
   * Name of property to change netty rpc server eventloop thread count. Default is 0. Tests may set
   * this down from unlimited.
   */
  public static final String HBASE_NETTY_EVENTLOOP_RPCSERVER_THREADCOUNT_KEY =
    "hbase.netty.eventloop.rpcserver.thread.count";
  private static final int EVENTLOOP_THREADCOUNT_DEFAULT = 0;

  /**
   * Low watermark for pending outbound bytes of a single netty channel. If the high watermark was
   * exceeded, channel will have setAutoRead to true again. The server will start reading incoming
   * bytes (requests) from the client channel.
   */
  public static final String CHANNEL_WRITABLE_LOW_WATERMARK_KEY =
    "hbase.server.netty.writable.watermark.low";
  private static final int CHANNEL_WRITABLE_LOW_WATERMARK_DEFAULT = 0;

  /**
   * High watermark for pending outbound bytes of a single netty channel. If the number of pending
   * outbound bytes exceeds this threshold, setAutoRead will be false for the channel. The server
   * will stop reading incoming requests from the client channel.
   * <p>
   * Note: any requests already in the call queue will still be processed.
   */
  public static final String CHANNEL_WRITABLE_HIGH_WATERMARK_KEY =
    "hbase.server.netty.writable.watermark.high";
  private static final int CHANNEL_WRITABLE_HIGH_WATERMARK_DEFAULT = 0;

  /**
   * Fatal watermark for pending outbound bytes of a single netty channel. If the number of pending
   * outbound bytes exceeds this threshold, the connection will be forcibly closed so that memory
   * can be reclaimed. The client will have to re-establish a new connection and retry any in-flight
   * requests.
   * <p>
   * Note: must be higher than the high watermark, otherwise it's ignored.
   */
  public static final String CHANNEL_WRITABLE_FATAL_WATERMARK_KEY =
    "hbase.server.netty.writable.watermark.fatal";
  private static final int CHANNEL_WRITABLE_FATAL_WATERMARK_DEFAULT = 0;

  private final InetSocketAddress bindAddress;

  private final CountDownLatch closed = new CountDownLatch(1);
  private final Channel serverChannel;
  final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
  private final AtomicReference<SslContext> sslContextForServer = new AtomicReference<>();
  private final AtomicReference<FileChangeWatcher> keyStoreWatcher = new AtomicReference<>();
  private final AtomicReference<FileChangeWatcher> trustStoreWatcher = new AtomicReference<>();

  private volatile int writeBufferFatalThreshold;
  private volatile WriteBufferWaterMark writeBufferWaterMark;

  public NettyRpcServer(Server server, String name, List<BlockingServiceAndInterface> services,
    InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler,
    boolean reservoirEnabled) throws IOException {
    super(server, name, services, bindAddress, conf, scheduler, reservoirEnabled);
    this.bindAddress = bindAddress;
    EventLoopGroup eventLoopGroup;
    Class<? extends ServerChannel> channelClass;
    if (server instanceof HRegionServer) {
      NettyEventLoopGroupConfig config = ((HRegionServer) server).getEventLoopGroupConfig();
      eventLoopGroup = config.group();
      channelClass = config.serverChannelClass();
    } else {
      int threadCount = server == null
        ? EVENTLOOP_THREADCOUNT_DEFAULT
        : server.getConfiguration().getInt(HBASE_NETTY_EVENTLOOP_RPCSERVER_THREADCOUNT_KEY,
          EVENTLOOP_THREADCOUNT_DEFAULT);
      eventLoopGroup = new NioEventLoopGroup(threadCount,
        new DefaultThreadFactory("NettyRpcServer", true, Thread.MAX_PRIORITY));
      channelClass = NioServerSocketChannel.class;
    }

    // call before creating bootstrap below so that the necessary configs can be set
    configureNettyWatermarks(conf);

    ServerBootstrap bootstrap = new ServerBootstrap().group(eventLoopGroup).channel(channelClass)
      .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
      .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
      .childOption(ChannelOption.SO_REUSEADDR, true)
      .childHandler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          ch.config().setWriteBufferWaterMark(writeBufferWaterMark);
          ChannelPipeline pipeline = ch.pipeline();
          FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
          preambleDecoder.setSingleDecode(true);
          if (conf.getBoolean(HBASE_SERVER_NETTY_TLS_ENABLED, false)) {
            initSSL(pipeline, conf.getBoolean(HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, true));
          }
          NettyServerRpcConnection conn = createNettyServerRpcConnection(ch);
          pipeline.addLast(NettyRpcServerPreambleHandler.DECODER_NAME, preambleDecoder)
            .addLast(new NettyRpcServerPreambleHandler(NettyRpcServer.this, conn))
            // We need NettyRpcServerResponseEncoder here because NettyRpcServerPreambleHandler may
            // send RpcResponse to client.
            .addLast(NettyRpcServerResponseEncoder.NAME, new NettyRpcServerResponseEncoder(metrics))
            // Add writability handler after the response encoder, so we can abort writes before
            // they get encoded, if the fatal threshold is exceeded. We pass in suppliers here so
            // that the handler configs can be live updated via update_config.
            .addLast(NettyRpcServerChannelWritabilityHandler.NAME,
              new NettyRpcServerChannelWritabilityHandler(metrics, () -> writeBufferFatalThreshold,
                () -> isWritabilityBackpressureEnabled()));
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

  @Override
  public void onConfigurationChange(Configuration newConf) {
    super.onConfigurationChange(newConf);
    configureNettyWatermarks(newConf);
  }

  private void configureNettyWatermarks(Configuration conf) {
    int watermarkLow =
      conf.getInt(CHANNEL_WRITABLE_LOW_WATERMARK_KEY, CHANNEL_WRITABLE_LOW_WATERMARK_DEFAULT);
    int watermarkHigh =
      conf.getInt(CHANNEL_WRITABLE_HIGH_WATERMARK_KEY, CHANNEL_WRITABLE_HIGH_WATERMARK_DEFAULT);
    int fatalThreshold =
      conf.getInt(CHANNEL_WRITABLE_FATAL_WATERMARK_KEY, CHANNEL_WRITABLE_FATAL_WATERMARK_DEFAULT);

    WriteBufferWaterMark oldWaterMark = writeBufferWaterMark;
    int oldFatalThreshold = writeBufferFatalThreshold;

    boolean disabled = false;
    if (watermarkHigh == 0 && watermarkLow == 0) {
      // if both are 0, use the netty default, which we will treat as "disabled".
      // when disabled, we won't manage autoRead in response to writability changes.
      writeBufferWaterMark = WriteBufferWaterMark.DEFAULT;
      disabled = true;
    } else {
      // netty checks pendingOutboundBytes < watermarkLow. It can never be less than 0, so set to
      // 1 to avoid confusing behavior.
      if (watermarkLow == 0) {
        LOG.warn(
          "Detected a {} value of 0, which is impossible to achieve "
            + "due to how netty evaluates these thresholds, setting to 1",
          CHANNEL_WRITABLE_LOW_WATERMARK_KEY);
        watermarkLow = 1;
      }

      // netty validates the watermarks and throws an exception if high < low, fail more gracefully
      // by disabling the watermarks and warning.
      if (watermarkHigh <= watermarkLow) {
        LOG.warn(
          "Detected {} value {}, lower than {} value {}. This will fail netty validation, "
            + "so disabling",
          CHANNEL_WRITABLE_HIGH_WATERMARK_KEY, watermarkHigh, CHANNEL_WRITABLE_LOW_WATERMARK_KEY,
          watermarkLow);
        writeBufferWaterMark = WriteBufferWaterMark.DEFAULT;
      } else {
        writeBufferWaterMark = new WriteBufferWaterMark(watermarkLow, watermarkHigh);
      }

      // only apply this check when watermark is enabled. this way we give the operator some
      // flexibility if they want to try enabling fatal threshold without backpressure.
      if (fatalThreshold > 0 && fatalThreshold <= watermarkHigh) {
        LOG.warn("Detected a {} value of {}, which is lower than the {} value of {}, ignoring.",
          CHANNEL_WRITABLE_FATAL_WATERMARK_KEY, fatalThreshold, CHANNEL_WRITABLE_HIGH_WATERMARK_KEY,
          watermarkHigh);
        fatalThreshold = 0;
      }
    }

    writeBufferFatalThreshold = fatalThreshold;

    if (
      oldWaterMark != null && (oldWaterMark.low() != writeBufferWaterMark.low()
        || oldWaterMark.high() != writeBufferWaterMark.high()
        || oldFatalThreshold != writeBufferFatalThreshold)
    ) {
      LOG.info("Updated netty outbound write buffer watermarks: low={}, high={}, fatal={}",
        disabled ? "disabled" : writeBufferWaterMark.low(),
        disabled ? "disabled" : writeBufferWaterMark.high(),
        writeBufferFatalThreshold <= 0 ? "disabled" : writeBufferFatalThreshold);
    }

    // update any existing channels
    for (Channel channel : allChannels) {
      channel.config().setWriteBufferWaterMark(writeBufferWaterMark);
      // if disabling watermark, set auto read to true in case channel had been exceeding
      // previous watermark
      if (disabled) {
        channel.config().setAutoRead(true);
      }
    }
  }

  public boolean isWritabilityBackpressureEnabled() {
    return writeBufferWaterMark != WriteBufferWaterMark.DEFAULT;
  }

  // will be overridden in tests
  @InterfaceAudience.Private
  protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
    return new NettyServerRpcConnection(NettyRpcServer.this, channel);
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
    FileChangeWatcher ks = keyStoreWatcher.getAndSet(null);
    if (ks != null) {
      ks.stop();
    }
    FileChangeWatcher ts = trustStoreWatcher.getAndSet(null);
    if (ts != null) {
      ts.stop();
    }
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
    SslContext nettySslContext = getSslContext();

    if (supportPlaintext) {
      p.addLast("ssl", new OptionalSslHandler(nettySslContext));
      LOG.debug("Dual mode SSL handler added for channel: {}", p.channel());
    } else {
      SocketAddress remoteAddress = p.channel().remoteAddress();
      SslHandler sslHandler;

      if (remoteAddress instanceof InetSocketAddress) {
        InetSocketAddress remoteInetAddress = (InetSocketAddress) remoteAddress;
        String host;

        if (conf.getBoolean(TLS_CONFIG_REVERSE_DNS_LOOKUP_ENABLED, true)) {
          host = remoteInetAddress.getHostName();
        } else {
          host = remoteInetAddress.getHostString();
        }

        int port = remoteInetAddress.getPort();

        /*
         * our HostnameVerifier gets the host name from SSLEngine, so we have to construct the
         * engine properly by passing the remote address
         */
        sslHandler = nettySslContext.newHandler(p.channel().alloc(), host, port);
      } else {
        sslHandler = nettySslContext.newHandler(p.channel().alloc());
      }

      p.addLast("ssl", sslHandler);
      LOG.debug("SSL handler added for channel: {}", p.channel());
    }
  }

  SslContext getSslContext() throws X509Exception, IOException {
    SslContext result = sslContextForServer.get();
    if (result == null) {
      result = X509Util.createSslContextForServer(conf);
      if (!sslContextForServer.compareAndSet(null, result)) {
        // lost the race, another thread already set the value
        result = sslContextForServer.get();
      } else if (
        keyStoreWatcher.get() == null && trustStoreWatcher.get() == null
          && conf.getBoolean(X509Util.TLS_CERT_RELOAD, false)
      ) {
        X509Util.enableCertFileReloading(conf, keyStoreWatcher, trustStoreWatcher,
          () -> sslContextForServer.set(null));
      }
    }
    return result;
  }

  public int getWriteBufferFatalThreshold() {
    return writeBufferFatalThreshold;
  }

  public Pair<Long, Long> getTotalAndMaxNettyOutboundBytes() {
    long total = 0;
    long max = 0;
    for (Channel channel : allChannels) {
      long outbound = NettyUnsafeUtils.getTotalPendingOutboundBytes(channel);
      total += outbound;
      max = Math.max(max, outbound);
    }
    return Pair.newPair(total, max);
  }
}
