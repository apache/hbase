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

import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_CLIENT_NETTY_TLS_HANDSHAKETIMEOUT;
import static org.apache.hadoop.hbase.ipc.CallEvent.Type.CANCELLED;
import static org.apache.hadoop.hbase.ipc.CallEvent.Type.TIMEOUT;
import static org.apache.hadoop.hbase.ipc.IPCUtil.setCancelled;
import static org.apache.hadoop.hbase.ipc.IPCUtil.toIOE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.X509Exception;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.security.NettyHBaseRpcConnectionHeaderHandler;
import org.apache.hadoop.hbase.security.NettyHBaseSaslRpcClientHandler;
import org.apache.hadoop.hbase.security.SaslChallengeDecoder;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateHandler;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.FutureListener;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Promise;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;

/**
 * RPC connection implementation based on netty.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcConnection extends RpcConnection {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcConnection.class);

  private static final ScheduledExecutorService RELOGIN_EXECUTOR = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Relogin-pool-%d")
      .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

  private final NettyRpcClient rpcClient;

  private ByteBuf connectionHeaderPreamble;

  private ByteBuf connectionHeaderWithLength;

  private final Object connectLock = new Object();
  private volatile ChannelFuture channelFuture;
  private volatile Channel channel;

  NettyRpcConnection(NettyRpcClient rpcClient, ConnectionId remoteId) throws IOException {
    super(rpcClient.conf, AbstractRpcClient.WHEEL_TIMER, remoteId, rpcClient.clusterId,
      rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.codec, rpcClient.compressor,
      rpcClient.metrics);
    this.rpcClient = rpcClient;
    byte[] connectionHeaderPreamble = getConnectionHeaderPreamble();
    this.connectionHeaderPreamble =
      Unpooled.directBuffer(connectionHeaderPreamble.length).writeBytes(connectionHeaderPreamble);
    ConnectionHeader header = getConnectionHeader();
    this.connectionHeaderWithLength = Unpooled.directBuffer(4 + header.getSerializedSize());
    this.connectionHeaderWithLength.writeInt(header.getSerializedSize());
    header.writeTo(new ByteBufOutputStream(this.connectionHeaderWithLength));
  }

  private Channel getChannel() {
    return channel;
  }

  @Override
  protected void callTimeout(Call call) {
    Channel channel = getChannel();

    if (channel != null) {
      channel.pipeline().fireUserEventTriggered(new CallEvent(TIMEOUT, call));
    }
  }

  @Override
  public boolean isActive() {
    return getChannel() != null;
  }

  @Override
  public void shutdown() {
    ChannelFuture currentChannelFuture;
    Channel currentChannel;

    synchronized (connectLock) {
      currentChannelFuture = channelFuture;
      currentChannel = channel;
      channelFuture = null;
      channel = null;
    }

    if (currentChannelFuture == null) {
      return;
    }

    if (!currentChannelFuture.isDone()) {
      currentChannelFuture.cancel(true);
    }

    if (currentChannel != null) {
      currentChannel.close();
    }
  }

  @Override
  public void cleanupConnection() {
    if (connectionHeaderPreamble != null) {
      ReferenceCountUtil.safeRelease(connectionHeaderPreamble);
      connectionHeaderPreamble = null;
    }
    if (connectionHeaderWithLength != null) {
      ReferenceCountUtil.safeRelease(connectionHeaderWithLength);
      connectionHeaderWithLength = null;
    }
  }

  private void established(Channel ch) {
    ChannelPipeline p = ch.pipeline();
    String addBeforeHandler = p.context(BufferCallBeforeInitHandler.class).name();
    p.addBefore(addBeforeHandler, null,
      new IdleStateHandler(0, rpcClient.minIdleTimeBeforeClose, 0, TimeUnit.MILLISECONDS));
    p.addBefore(addBeforeHandler, null, new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4));
    p.addBefore(addBeforeHandler, null,
      new NettyRpcDuplexHandler(this, rpcClient.cellBlockBuilder, codec, compressor));
    p.fireUserEventTriggered(BufferCallEvent.success());
  }

  private boolean reloginInProgress;

  private void scheduleRelogin(Throwable error) {
    if (error instanceof FallbackDisallowedException) {
      return;
    }
    if (!provider.canRetry()) {
      LOG.trace("SASL Provider does not support retries");
      return;
    }
    if (reloginInProgress) {
      return;
    }
    reloginInProgress = true;
    RELOGIN_EXECUTOR.schedule(() -> {
      try {
        provider.relogin();
      } catch (IOException e) {
        LOG.warn("Relogin failed", e);
      }
      reloginInProgress = false;
    }, ThreadLocalRandom.current().nextInt(reloginMaxBackoff), TimeUnit.MILLISECONDS);
  }

  private void failInit(Channel ch, IOException e) {
    // fail all pending calls
    ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
    shutdown();
  }

  private void saslNegotiate(final Channel ch) {
    UserGroupInformation ticket = provider.getRealUser(remoteId.getTicket());
    if (ticket == null) {
      failInit(ch, new FatalConnectionException("ticket/user is null"));
      return;
    }
    Promise<Boolean> saslPromise = ch.eventLoop().newPromise();
    final NettyHBaseSaslRpcClientHandler saslHandler;
    try {
      saslHandler = new NettyHBaseSaslRpcClientHandler(saslPromise, ticket, provider, token,
        ((InetSocketAddress) ch.remoteAddress()).getAddress(), securityInfo,
        rpcClient.fallbackAllowed, this.rpcClient.conf);
    } catch (IOException e) {
      failInit(ch, e);
      return;
    }
    if (conf.getBoolean(HBASE_CLIENT_NETTY_TLS_ENABLED, false)) {
      ch.pipeline().addAfter("ssl", "saslchdecoder", new SaslChallengeDecoder());
      ch.pipeline().addAfter("saslchdecoder", "saslhandler", saslHandler);
    } else {
      ch.pipeline().addFirst(new SaslChallengeDecoder(), saslHandler);
    }
    saslPromise.addListener(new FutureListener<Boolean>() {

      @Override
      public void operationComplete(Future<Boolean> future) {
        if (future.isSuccess()) {
          ChannelPipeline p = ch.pipeline();
          // check if negotiate with server for connection header is necessary
          if (saslHandler.isNeedProcessConnectionHeader()) {
            Promise<Boolean> connectionHeaderPromise = ch.eventLoop().newPromise();
            // create the handler to handle the connection header
            ChannelHandler chHandler = new NettyHBaseRpcConnectionHeaderHandler(
              connectionHeaderPromise, conf, connectionHeaderWithLength);

            // add ReadTimeoutHandler to deal with server doesn't response connection header
            // because of the different configuration in client side and server side
            p.addFirst(
              new ReadTimeoutHandler(RpcClient.DEFAULT_SOCKET_TIMEOUT_READ, TimeUnit.MILLISECONDS));
            p.addLast(chHandler);
            connectionHeaderPromise.addListener(new FutureListener<Boolean>() {
              @Override
              public void operationComplete(Future<Boolean> future) throws Exception {
                if (future.isSuccess()) {
                  ChannelPipeline p = ch.pipeline();
                  p.remove(ReadTimeoutHandler.class);
                  p.remove(NettyHBaseRpcConnectionHeaderHandler.class);
                  // don't send connection header, NettyHbaseRpcConnectionHeaderHandler
                  // sent it already
                  established(ch);
                } else {
                  final Throwable error = future.cause();
                  scheduleRelogin(error);
                  failInit(ch, toIOE(error));
                }
              }
            });
          } else {
            // send the connection header to server
            ch.write(connectionHeaderWithLength.retainedDuplicate());
            established(ch);
          }
        } else {
          final Throwable error = future.cause();
          scheduleRelogin(error);
          failInit(ch, toIOE(error));
        }
      }
    });
  }

  private ChannelFuture connect() throws UnknownHostException {
    LOG.trace("Connecting to {}", remoteId.getAddress());
    InetSocketAddress remoteAddr = getRemoteInetAddress(rpcClient.metrics);
    Bootstrap bootstrap = new Bootstrap().group(rpcClient.group.next())
      .channel(rpcClient.channelClass).option(ChannelOption.TCP_NODELAY, rpcClient.isTcpNoDelay())
      .option(ChannelOption.SO_KEEPALIVE, rpcClient.tcpKeepAlive)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, rpcClient.connectTO).handler(
        new HBaseClientPipelineFactory(remoteAddr.getHostString(), remoteAddr.getPort(), conf));

    bootstrap.validate();

    return bootstrap.localAddress(rpcClient.localAddr).remoteAddress(remoteAddr).connect()
      .addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
          Channel ch = future.channel();
          if (!future.isSuccess()) {
            failInit(ch, toIOE(future.cause()));
            rpcClient.failedServers.addToFailedServers(remoteId.getAddress(), future.cause());
            return;
          }
          ch.writeAndFlush(connectionHeaderPreamble.retainedDuplicate());
          if (useSasl) {
            saslNegotiate(ch);
          } else {
            // send the connection header to server
            ch.write(connectionHeaderWithLength.retainedDuplicate());
            established(ch);
          }
        }
      });
  }

  private void sendRequest0(Call call, HBaseRpcController hrc) throws IOException {
    if (reloginInProgress) {
      throw new IOException("Can not send request because relogin is in progress.");
    }
    hrc.notifyOnCancel(new RpcCallback<Object>() {

      @Override
      public void run(Object parameter) {
        setCancelled(call);
        Channel channel = getChannel();
        if (channel != null) {
          channel.pipeline().fireUserEventTriggered(new CallEvent(CANCELLED, call));
        }
      }
    }, new CancellationCallback() {

      @Override
      public void run(boolean cancelled) throws IOException {
        if (cancelled) {
          setCancelled(call);
          return;
        }

        Channel ch = getChannel();

        if (ch != null) {
          writeAndFlushToChannel(call, ch);
          return;
        }

        synchronized (connectLock) {
          if (channelFuture == null) {
            channelFuture = connect();
          }

          channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) {
              if (channelFuture.isSuccess()) {
                synchronized (connectLock) {
                  if (channel == null) {
                    channel = channelFuture.channel();
                  }
                }
                writeAndFlushToChannel(call, channel);
              } else {
                call.setException(toIOE(channelFuture.cause()));
              }
            }
          });
        }
      }
    });
  }

  private void writeAndFlushToChannel(Call call, Channel ch) {
    if (ch == null) {
      return;
    }

    scheduleTimeoutTask(call);
    ch.writeAndFlush(call).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        // Fail the call if we failed to write it out. This usually because the channel is
        // closed. This is needed because we may shutdown the channel inside event loop and
        // there may still be some pending calls in the event loop queue after us.
        if (!future.isSuccess()) {
          call.setException(toIOE(future.cause()));
        }
      }
    });
  }

  @Override
  public void sendRequest(final Call call, HBaseRpcController hrc) {
    try {
      sendRequest0(call, hrc);
    } catch (Exception e) {
      call.setException(toIOE(e));
    }
  }

  /**
   * HBaseClientPipelineFactory is the netty pipeline factory for this netty connection
   * implementation.
   */
  private static class HBaseClientPipelineFactory extends ChannelInitializer<SocketChannel> {

    private SSLContext sslContext = null;
    private SSLEngine sslEngine = null;
    private final String host;
    private final int port;
    private final Configuration conf;

    public HBaseClientPipelineFactory(String host, int port, Configuration conf) {
      this.host = host;
      this.port = port;
      this.conf = conf;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws X509Exception.SSLContextException {
      ChannelPipeline pipeline = ch.pipeline();
      if (conf.getBoolean(HBASE_CLIENT_NETTY_TLS_ENABLED, false)) {
        initSSL(pipeline);
      }
      pipeline.addLast("handler", new BufferCallBeforeInitHandler());
    }

    // The synchronized is to prevent the race on shared variable "sslEngine".
    // Basically we only need to create it once.
    private synchronized void initSSL(ChannelPipeline pipeline)
      throws X509Exception.SSLContextException {
      if (sslContext == null || sslEngine == null) {
        X509Util x509Util = new X509Util(conf);
        sslContext = x509Util.createSSLContextAndOptions().getSSLContext();
        sslEngine = sslContext.createSSLEngine(host, port);
        sslEngine.setUseClientMode(true);
        LOG.debug("SSL engine initialized");
      }

      SslHandler sslHandler = new SslHandler(sslEngine);
      sslHandler.setHandshakeTimeoutMillis(conf.getInt(HBASE_CLIENT_NETTY_TLS_HANDSHAKETIMEOUT,
        DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS));
      pipeline.addLast("ssl", sslHandler);
      LOG.info("SSL handler with handshake timeout {} ms added for channel: {}",
        sslHandler.getHandshakeTimeoutMillis(), pipeline.channel());
    }
  }
}
