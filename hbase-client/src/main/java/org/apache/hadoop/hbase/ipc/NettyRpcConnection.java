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

import static org.apache.hadoop.hbase.ipc.CallEvent.Type.CANCELLED;
import static org.apache.hadoop.hbase.ipc.CallEvent.Type.TIMEOUT;
import static org.apache.hadoop.hbase.ipc.IPCUtil.execute;
import static org.apache.hadoop.hbase.ipc.IPCUtil.setCancelled;
import static org.apache.hadoop.hbase.ipc.IPCUtil.toIOE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.security.NettyHBaseRpcConnectionHeaderHandler;
import org.apache.hadoop.hbase.security.NettyHBaseSaslRpcClientHandler;
import org.apache.hadoop.hbase.security.SaslChallengeDecoder;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
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
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
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
 * <p/>
 * Most operations are executed in handlers. Netty handler is always executed in the same
 * thread(EventLoop) so no lock is needed.
 * <p/>
 * <strong>Implementation assumptions:</strong> All the private methods should be called in the
 * {@link #eventLoop} thread, otherwise there will be races.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcConnection extends RpcConnection {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcConnection.class);

  private static final ScheduledExecutorService RELOGIN_EXECUTOR = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Relogin-pool-%d")
      .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

  private final NettyRpcClient rpcClient;

  // the event loop used to set up the connection, we will also execute other operations for this
  // connection in this event loop, to avoid locking everywhere.
  private final EventLoop eventLoop;

  private ByteBuf connectionHeaderPreamble;

  private ByteBuf connectionHeaderWithLength;

  // make it volatile so in the isActive method below we do not need to switch to the event loop
  // thread to access this field.
  private volatile Channel channel;

  NettyRpcConnection(NettyRpcClient rpcClient, ConnectionId remoteId) throws IOException {
    super(rpcClient.conf, AbstractRpcClient.WHEEL_TIMER, remoteId, rpcClient.clusterId,
      rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.codec, rpcClient.compressor,
      rpcClient.cellBlockBuilder, rpcClient.metrics, rpcClient.authenticationProviders,
      rpcClient.connectionAttributes);
    this.rpcClient = rpcClient;
    this.eventLoop = rpcClient.group.next();
    byte[] connectionHeaderPreamble = getConnectionHeaderPreamble();
    this.connectionHeaderPreamble =
      Unpooled.directBuffer(connectionHeaderPreamble.length).writeBytes(connectionHeaderPreamble);
    ConnectionHeader header = getConnectionHeader();
    this.connectionHeaderWithLength = Unpooled.directBuffer(4 + header.getSerializedSize());
    this.connectionHeaderWithLength.writeInt(header.getSerializedSize());
    header.writeTo(new ByteBufOutputStream(this.connectionHeaderWithLength));
  }

  @Override
  protected void callTimeout(Call call) {
    execute(eventLoop, () -> {
      if (channel != null) {
        channel.pipeline().fireUserEventTriggered(new CallEvent(TIMEOUT, call));
      }
    });
  }

  @Override
  public boolean isActive() {
    return channel != null;
  }

  private void shutdown0() {
    assert eventLoop.inEventLoop();
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }

  @Override
  public void shutdown() {
    execute(eventLoop, this::shutdown0);
  }

  @Override
  public void cleanupConnection() {
    execute(eventLoop, () -> {
      if (connectionHeaderPreamble != null) {
        ReferenceCountUtil.safeRelease(connectionHeaderPreamble);
        connectionHeaderPreamble = null;
      }
      if (connectionHeaderWithLength != null) {
        ReferenceCountUtil.safeRelease(connectionHeaderWithLength);
        connectionHeaderWithLength = null;
      }
    });
  }

  private void established(Channel ch) {
    assert eventLoop.inEventLoop();
    ch.pipeline()
      .addBefore(BufferCallBeforeInitHandler.NAME, null,
        new IdleStateHandler(0, rpcClient.minIdleTimeBeforeClose, 0, TimeUnit.MILLISECONDS))
      .addBefore(BufferCallBeforeInitHandler.NAME, null,
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4))
      .addBefore(BufferCallBeforeInitHandler.NAME, null,
        new NettyRpcDuplexHandler(this, rpcClient.cellBlockBuilder, codec, compressor))
      .fireUserEventTriggered(BufferCallEvent.success());
  }

  private void saslEstablished(Channel ch, String serverPrincipal) {
    saslNegotiationDone(serverPrincipal, true);
    established(ch);
  }

  private boolean reloginInProgress;

  private void scheduleRelogin(Throwable error) {
    assert eventLoop.inEventLoop();
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
      eventLoop.execute(() -> {
        reloginInProgress = false;
      });
    }, ThreadLocalRandom.current().nextInt(reloginMaxBackoff), TimeUnit.MILLISECONDS);
  }

  private void failInit(Channel ch, IOException e) {
    assert eventLoop.inEventLoop();
    // fail all pending calls
    ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
    shutdown0();
    rpcClient.failedServers.addToFailedServers(remoteId.getAddress(), e);
  }

  private void saslFailInit(Channel ch, String serverPrincipal, IOException error) {
    assert eventLoop.inEventLoop();
    saslNegotiationDone(serverPrincipal, false);
    failInit(ch, error);
  }

  private void saslNegotiate(Channel ch, String serverPrincipal) {
    assert eventLoop.inEventLoop();
    NettyFutureUtils.safeWriteAndFlush(ch, connectionHeaderPreamble.retainedDuplicate());
    UserGroupInformation ticket = provider.getRealUser(remoteId.getTicket());
    if (ticket == null) {
      saslFailInit(ch, serverPrincipal, new FatalConnectionException("ticket/user is null"));
      return;
    }
    Promise<Boolean> saslPromise = ch.eventLoop().newPromise();
    final NettyHBaseSaslRpcClientHandler saslHandler;
    try {
      saslHandler = new NettyHBaseSaslRpcClientHandler(saslPromise, ticket, provider, token,
        ((InetSocketAddress) ch.remoteAddress()).getAddress(), serverPrincipal,
        rpcClient.fallbackAllowed, this.rpcClient.conf);
    } catch (IOException e) {
      saslFailInit(ch, serverPrincipal, e);
      return;
    }
    ch.pipeline().addBefore(BufferCallBeforeInitHandler.NAME, null, new SaslChallengeDecoder())
      .addBefore(BufferCallBeforeInitHandler.NAME, NettyHBaseSaslRpcClientHandler.HANDLER_NAME,
        saslHandler);
    NettyFutureUtils.addListener(saslPromise, new FutureListener<Boolean>() {

      @Override
      public void operationComplete(Future<Boolean> future) throws Exception {
        if (future.isSuccess()) {
          ChannelPipeline p = ch.pipeline();
          // check if negotiate with server for connection header is necessary
          if (saslHandler.isNeedProcessConnectionHeader()) {
            Promise<Boolean> connectionHeaderPromise = ch.eventLoop().newPromise();
            // create the handler to handle the connection header
            NettyHBaseRpcConnectionHeaderHandler chHandler =
              new NettyHBaseRpcConnectionHeaderHandler(connectionHeaderPromise, conf,
                connectionHeaderWithLength);

            // add ReadTimeoutHandler to deal with server doesn't response connection header
            // because of the different configuration in client side and server side
            final String readTimeoutHandlerName = "ReadTimeout";
            p.addBefore(BufferCallBeforeInitHandler.NAME, readTimeoutHandlerName,
              new ReadTimeoutHandler(rpcClient.readTO, TimeUnit.MILLISECONDS))
              .addBefore(BufferCallBeforeInitHandler.NAME, null, chHandler);
            NettyFutureUtils.addListener(connectionHeaderPromise, new FutureListener<Boolean>() {
              @Override
              public void operationComplete(Future<Boolean> future) throws Exception {
                if (future.isSuccess()) {
                  ChannelPipeline p = ch.pipeline();
                  p.remove(readTimeoutHandlerName);
                  p.remove(NettyHBaseRpcConnectionHeaderHandler.class);
                  // don't send connection header, NettyHBaseRpcConnectionHeaderHandler
                  // sent it already
                  saslEstablished(ch, serverPrincipal);
                } else {
                  final Throwable error = future.cause();
                  scheduleRelogin(error);
                  saslFailInit(ch, serverPrincipal, toIOE(error));
                }
              }
            });
          } else {
            // send the connection header to server
            ch.write(connectionHeaderWithLength.retainedDuplicate());
            saslEstablished(ch, serverPrincipal);
          }
        } else {
          final Throwable error = future.cause();
          scheduleRelogin(error);
          saslFailInit(ch, serverPrincipal, toIOE(error));
        }
      }
    });
  }

  private void getConnectionRegistry(Channel ch, Call connectionRegistryCall) {
    assert eventLoop.inEventLoop();
    PreambleCallHandler.setup(ch.pipeline(), rpcClient.readTO, this,
      RpcClient.REGISTRY_PREAMBLE_HEADER, connectionRegistryCall);
  }

  private void onSecurityPreambleError(Channel ch, Set<String> serverPrincipals,
    IOException error) {
    assert eventLoop.inEventLoop();
    LOG.debug("Error when trying to do a security preamble call to {}", remoteId.address, error);
    if (ConnectionUtils.isUnexpectedPreambleHeaderException(error)) {
      // this means we are connecting to an old server which does not support the security
      // preamble call, so we should fallback to randomly select a principal to use
      // TODO: find a way to reconnect without failing all the pending calls, for now, when we
      // reach here, shutdown should have already been scheduled
      return;
    }
    if (IPCUtil.isSecurityNotEnabledException(error)) {
      // server tells us security is not enabled, then we should check whether fallback to
      // simple is allowed, if so we just go without security, otherwise we should fail the
      // negotiation immediately
      if (rpcClient.fallbackAllowed) {
        // TODO: just change the preamble and skip the fallback to simple logic, for now, just
        // select the first principal can finish the connection setup, but waste one client
        // message
        saslNegotiate(ch, serverPrincipals.iterator().next());
      } else {
        failInit(ch, new FallbackDisallowedException());
      }
      return;
    }
    // usually we should not reach here, but for robust, just randomly select a principal to
    // connect
    saslNegotiate(ch, randomSelect(serverPrincipals));
  }

  private void onSecurityPreambleFinish(Channel ch, Set<String> serverPrincipals,
    Call securityPreambleCall) {
    assert eventLoop.inEventLoop();
    String serverPrincipal;
    try {
      serverPrincipal = chooseServerPrincipal(serverPrincipals, securityPreambleCall);
    } catch (SaslException e) {
      failInit(ch, e);
      return;
    }
    saslNegotiate(ch, serverPrincipal);
  }

  private void saslNegotiate(Channel ch) throws IOException {
    assert eventLoop.inEventLoop();
    Set<String> serverPrincipals = getServerPrincipals();
    if (serverPrincipals.size() == 1) {
      saslNegotiate(ch, serverPrincipals.iterator().next());
      return;
    }
    // this means we use kerberos authentication and there are multiple server principal candidates,
    // in this way we need to send a special preamble header to get server principal from server
    Call securityPreambleCall = createSecurityPreambleCall(call -> {
      assert eventLoop.inEventLoop();
      if (call.error != null) {
        onSecurityPreambleError(ch, serverPrincipals, call.error);
      } else {
        onSecurityPreambleFinish(ch, serverPrincipals, call);
      }
    });
    PreambleCallHandler.setup(ch.pipeline(), rpcClient.readTO, this,
      RpcClient.SECURITY_PREAMBLE_HEADER, securityPreambleCall);
  }

  private void connect(Call connectionRegistryCall) throws UnknownHostException {
    assert eventLoop.inEventLoop();
    LOG.trace("Connecting to {}", remoteId.getAddress());
    InetSocketAddress remoteAddr = getRemoteInetAddress(rpcClient.metrics);
    this.channel = new Bootstrap().group(eventLoop).channel(rpcClient.channelClass)
      .option(ChannelOption.TCP_NODELAY, rpcClient.isTcpNoDelay())
      .option(ChannelOption.SO_KEEPALIVE, rpcClient.tcpKeepAlive)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, rpcClient.connectTO)
      .handler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          if (conf.getBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, false)) {
            SslContext sslContext = rpcClient.getSslContext();
            SslHandler sslHandler = sslContext.newHandler(ch.alloc(),
              remoteId.address.getHostName(), remoteId.address.getPort());
            sslHandler.setHandshakeTimeoutMillis(
              conf.getInt(X509Util.HBASE_CLIENT_NETTY_TLS_HANDSHAKETIMEOUT,
                X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS));
            ch.pipeline().addFirst(sslHandler);
            LOG.debug("SSL handler added with handshake timeout {} ms",
              sslHandler.getHandshakeTimeoutMillis());
          }
          ch.pipeline().addLast(BufferCallBeforeInitHandler.NAME,
            new BufferCallBeforeInitHandler());
        }
      }).localAddress(rpcClient.localAddr).remoteAddress(remoteAddr).connect()
      .addListener(new ChannelFutureListener() {

        private void succeed(Channel ch) throws IOException {
          if (connectionRegistryCall != null) {
            getConnectionRegistry(ch, connectionRegistryCall);
            return;
          }
          if (!useSasl) {
            // BufferCallBeforeInitHandler will call ctx.flush when receiving the
            // BufferCallEvent.success() event, so here we just use write for the below two messages
            NettyFutureUtils.safeWrite(ch, connectionHeaderPreamble.retainedDuplicate());
            NettyFutureUtils.safeWrite(ch, connectionHeaderWithLength.retainedDuplicate());
            established(ch);
          } else {
            saslNegotiate(ch);
          }
        }

        private void fail(Channel ch, Throwable error) {
          IOException ex = toIOE(error);
          LOG.warn("Exception encountered while connecting to the server " + remoteId.getAddress(),
            ex);
          if (connectionRegistryCall != null) {
            connectionRegistryCall.setException(ex);
          }
          failInit(ch, ex);
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          Channel ch = future.channel();
          if (!future.isSuccess()) {
            fail(ch, future.cause());
            return;
          }
          SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
          if (sslHandler != null) {
            NettyFutureUtils.addListener(sslHandler.handshakeFuture(), f -> {
              if (f.isSuccess()) {
                succeed(ch);
              } else {
                fail(ch, f.cause());
              }
            });
          } else {
            succeed(ch);
          }
        }
      }).channel();
  }

  private void sendRequest0(Call call, HBaseRpcController hrc) throws IOException {
    assert eventLoop.inEventLoop();
    if (call.isConnectionRegistryCall()) {
      // For get connection registry call, we will send a special preamble header to get the
      // response, instead of sending a real rpc call. See HBASE-25051
      connect(call);
      return;
    }
    if (reloginInProgress) {
      throw new IOException(RpcConnectionConstants.RELOGIN_IS_IN_PROGRESS);
    }
    hrc.notifyOnCancel(new RpcCallback<Object>() {

      @Override
      public void run(Object parameter) {
        setCancelled(call);
        if (channel != null) {
          channel.pipeline().fireUserEventTriggered(new CallEvent(CANCELLED, call));
        }
      }
    }, new CancellationCallback() {

      @Override
      public void run(boolean cancelled) throws IOException {
        if (cancelled) {
          setCancelled(call);
        } else {
          if (channel == null) {
            connect(null);
          }
          scheduleTimeoutTask(call);
          channel.writeAndFlush(call).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              // Fail the call if we failed to write it out. This usually because the channel is
              // closed. This is needed because we may shutdown the channel inside event loop and
              // there may still be some pending calls in the event loop queue after us.
              if (!future.isSuccess()) {
                call.setException(toIOE(future.cause()));
              }
            }
          });
        }
      }
    });
  }

  @Override
  public void sendRequest(final Call call, HBaseRpcController hrc) {
    execute(eventLoop, () -> {
      try {
        sendRequest0(call, hrc);
      } catch (Exception e) {
        call.setException(toIOE(e));
      }
    });
  }
}
