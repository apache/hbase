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

import static org.apache.hadoop.hbase.ipc.CallEvent.Type.CANCELLED;
import static org.apache.hadoop.hbase.ipc.CallEvent.Type.TIMEOUT;
import static org.apache.hadoop.hbase.ipc.IPCUtil.setCancelled;
import static org.apache.hadoop.hbase.ipc.IPCUtil.toIOE;

import io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.hadoop.hbase.security.NettyHBaseRpcConnectionHeaderHandler;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.security.NettyHBaseSaslRpcClientHandler;
import org.apache.hadoop.hbase.security.SaslChallengeDecoder;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * RPC connection implementation based on netty.
 * <p>
 * Most operations are executed in handlers. Netty handler is always executed in the same
 * thread(EventLoop) so no lock is needed.
 */
@InterfaceAudience.Private
class NettyRpcConnection extends RpcConnection {

  private static final Log LOG = LogFactory.getLog(NettyRpcConnection.class);

  private static final ScheduledExecutorService RELOGIN_EXECUTOR = Executors
      .newSingleThreadScheduledExecutor(Threads.newDaemonThreadFactory("Relogin"));

  private final NettyRpcClient rpcClient;

  private ByteBuf connectionHeaderPreamble;

  private ByteBuf connectionHeaderWithLength;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "connect is also under lock as notifyOnCancel will call our action directly")
  private Channel channel;

  NettyRpcConnection(NettyRpcClient rpcClient, ConnectionId remoteId) throws IOException {
    super(rpcClient.conf, AbstractRpcClient.WHEEL_TIMER, remoteId, rpcClient.clusterId,
        rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.codec, rpcClient.compressor);
    this.rpcClient = rpcClient;
    byte[] connectionHeaderPreamble = getConnectionHeaderPreamble();
    this.connectionHeaderPreamble = Unpooled.directBuffer(connectionHeaderPreamble.length)
        .writeBytes(connectionHeaderPreamble);
    ConnectionHeader header = getConnectionHeader();
    this.connectionHeaderWithLength = Unpooled.directBuffer(4 + header.getSerializedSize());
    this.connectionHeaderWithLength.writeInt(header.getSerializedSize());
    header.writeTo(new ByteBufOutputStream(this.connectionHeaderWithLength));
  }

  @Override
  protected synchronized void callTimeout(Call call) {
    if (channel != null) {
      channel.pipeline().fireUserEventTriggered(new CallEvent(TIMEOUT, call));
    }
  }

  @Override
  public synchronized boolean isActive() {
    return channel != null;
  }

  private void shutdown0() {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }

  @Override
  public synchronized void shutdown() {
    shutdown0();
  }

  @Override
  public synchronized void cleanupConnection() {
    if (connectionHeaderPreamble != null) {
      ReferenceCountUtil.safeRelease(connectionHeaderPreamble);
    }
    if (connectionHeaderWithLength != null) {
      ReferenceCountUtil.safeRelease(connectionHeaderWithLength);
    }
  }

  private void established(Channel ch) throws IOException {
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
    synchronized (this) {
      if (reloginInProgress) {
        return;
      }
      reloginInProgress = true;
      RELOGIN_EXECUTOR.schedule(new Runnable() {

        @Override
        public void run() {
          try {
            if (shouldAuthenticateOverKrb()) {
              relogin();
            }
          } catch (IOException e) {
            LOG.warn("relogin failed", e);
          }
          synchronized (this) {
            reloginInProgress = false;
          }
        }
      }, ThreadLocalRandom.current().nextInt(reloginMaxBackoff), TimeUnit.MILLISECONDS);
    }
  }

  private void failInit(Channel ch, IOException e) {
    synchronized (this) {
      // fail all pending calls
      ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
      shutdown0();
      return;
    }
  }

  private void saslNegotiate(final Channel ch) {
    UserGroupInformation ticket = getUGI();
    if (ticket == null) {
      failInit(ch, new FatalConnectionException("ticket/user is null"));
      return;
    }
    Promise<Boolean> saslPromise = ch.eventLoop().newPromise();
    final NettyHBaseSaslRpcClientHandler saslHandler;
    try {
      saslHandler = new NettyHBaseSaslRpcClientHandler(saslPromise, ticket, authMethod, token,
          serverPrincipal, rpcClient.fallbackAllowed, this.rpcClient.conf);
    } catch (IOException e) {
      failInit(ch, e);
      return;
    }
    ch.pipeline().addFirst(new SaslChallengeDecoder(), saslHandler);
    saslPromise.addListener(new FutureListener<Boolean>() {

      @Override
      public void operationComplete(Future<Boolean> future) throws Exception {
        if (future.isSuccess()) {
          ChannelPipeline p = ch.pipeline();
          p.remove(SaslChallengeDecoder.class);
          p.remove(NettyHBaseSaslRpcClientHandler.class);

          // check if negotiate with server for connection header is necessary
          if (saslHandler.isNeedProcessConnectionHeader()) {
            Promise<Boolean> connectionHeaderPromise = ch.eventLoop().newPromise();
            // create the handler to handle the connection header
            ChannelHandler chHandler = new NettyHBaseRpcConnectionHeaderHandler(
                connectionHeaderPromise, conf, connectionHeaderWithLength);

            // add ReadTimeoutHandler to deal with server doesn't response connection header
            // because of the different configuration in client side and server side
            p.addFirst(new ReadTimeoutHandler(
                RpcClient.DEFAULT_SOCKET_TIMEOUT_READ, TimeUnit.MILLISECONDS));
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

  private void connect() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to " + remoteId.address);
    }

    this.channel = new Bootstrap().group(rpcClient.group).channel(rpcClient.channelClass)
        .option(ChannelOption.TCP_NODELAY, rpcClient.isTcpNoDelay())
        .option(ChannelOption.SO_KEEPALIVE, rpcClient.tcpKeepAlive)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, rpcClient.connectTO)
        .handler(new BufferCallBeforeInitHandler()).localAddress(rpcClient.localAddr)
        .remoteAddress(remoteId.address).connect().addListener(new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channel ch = future.channel();
            if (!future.isSuccess()) {
              failInit(ch, toIOE(future.cause()));
              rpcClient.failedServers.addToFailedServers(remoteId.address);
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
        }).channel();
  }

  @Override
  public synchronized void sendRequest(final Call call, HBaseRpcController hrc)
      throws IOException {
    if (reloginInProgress) {
      throw new IOException("Can not send request because relogin is in progress.");
    }
    hrc.notifyOnCancel(new RpcCallback<Object>() {

      @Override
      public void run(Object parameter) {
        setCancelled(call);
        synchronized (this) {
          if (channel != null) {
            channel.pipeline().fireUserEventTriggered(new CallEvent(CANCELLED, call));
          }
        }
      }
    }, new CancellationCallback() {

      @Override
      public void run(boolean cancelled) throws IOException {
        if (cancelled) {
          setCancelled(call);
        } else {
          if (channel == null) {
            connect();
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
}
