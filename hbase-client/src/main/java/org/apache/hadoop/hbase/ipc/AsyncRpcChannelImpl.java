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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Future;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.protobuf.generated.TracingProtos;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslClientHandler;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;

/**
 * Netty RPC channel
 */
@InterfaceAudience.Private
public class AsyncRpcChannelImpl implements AsyncRpcChannel {
  private static final Log LOG = LogFactory.getLog(AsyncRpcChannelImpl.class.getName());

  private static final int MAX_SASL_RETRIES = 5;

  protected final static Map<Kind, TokenSelector<? extends TokenIdentifier>> TOKEN_HANDDLERS
    = new HashMap<>();

  static {
    TOKEN_HANDDLERS.put(AuthenticationProtos.TokenIdentifier.Kind.HBASE_AUTH_TOKEN,
      new AuthenticationTokenSelector());
  }

  final AsyncRpcClient client;

  // Contains the channel to work with.
  // Only exists when connected
  private Channel channel;

  String name;
  final User ticket;
  final String serviceName;
  final InetSocketAddress address;

  private int failureCounter = 0;

  boolean useSasl;
  AuthMethod authMethod;
  private int reloginMaxBackoff;
  private Token<? extends TokenIdentifier> token;
  private String serverPrincipal;

  // NOTE: closed and connected flags below are only changed when a lock on pendingCalls
  private final Map<Integer, AsyncCall> pendingCalls = new HashMap<Integer, AsyncCall>();
  private boolean connected = false;
  private boolean closed = false;

  private Timeout cleanupTimer;

  private final TimerTask timeoutTask = new TimerTask() {
    @Override
    public void run(Timeout timeout) throws Exception {
      cleanupCalls();
    }
  };

  /**
   * Constructor for netty RPC channel
   * @param bootstrap to construct channel on
   * @param client to connect with
   * @param ticket of user which uses connection
   * @param serviceName name of service to connect to
   * @param address to connect to
   */
  public AsyncRpcChannelImpl(Bootstrap bootstrap, final AsyncRpcClient client, User ticket,
      String serviceName, InetSocketAddress address) {
    this.client = client;

    this.ticket = ticket;
    this.serviceName = serviceName;
    this.address = address;

    this.channel = connect(bootstrap).channel();

    name = ("IPC Client (" + channel.hashCode() + ") to " + address.toString()
        + ((ticket == null) ? " from unknown user" : (" from " + ticket.getName())));
  }

  /**
   * Connect to channel
   * @param bootstrap to connect to
   * @return future of connection
   */
  private ChannelFuture connect(final Bootstrap bootstrap) {
    return bootstrap.remoteAddress(address).connect()
        .addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(final ChannelFuture f) throws Exception {
            if (!f.isSuccess()) {
              retryOrClose(bootstrap, failureCounter++, client.failureSleep, f.cause());
              return;
            }
            channel = f.channel();

            setupAuthorization();

            ByteBuf b = channel.alloc().directBuffer(6);
            createPreamble(b, authMethod);
            channel.writeAndFlush(b).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            if (useSasl) {
              UserGroupInformation ticket = AsyncRpcChannelImpl.this.ticket.getUGI();
              if (authMethod == AuthMethod.KERBEROS) {
                if (ticket != null && ticket.getRealUser() != null) {
                  ticket = ticket.getRealUser();
                }
              }
              SaslClientHandler saslHandler;
              if (ticket == null) {
                throw new FatalConnectionException("ticket/user is null");
              }
              final UserGroupInformation realTicket = ticket;
              saslHandler = ticket.doAs(new PrivilegedExceptionAction<SaslClientHandler>() {
                @Override
                public SaslClientHandler run() throws IOException {
                  return getSaslHandler(realTicket, bootstrap);
                }
              });
              if (saslHandler != null) {
                // Sasl connect is successful. Let's set up Sasl channel handler
                channel.pipeline().addFirst(saslHandler);
              } else {
                // fall back to simple auth because server told us so.
                authMethod = AuthMethod.SIMPLE;
                useSasl = false;
              }
            } else {
              startHBaseConnection(f.channel());
            }
          }
        });
  }

  /**
   * Start HBase connection
   * @param ch channel to start connection on
   */
  private void startHBaseConnection(Channel ch) {
    ch.pipeline().addLast("frameDecoder",
      new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    ch.pipeline().addLast(new AsyncServerResponseHandler(this));
    try {
      writeChannelHeader(ch).addListener(new GenericFutureListener<ChannelFuture>() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            close(future.cause());
            return;
          }
          List<AsyncCall> callsToWrite;
          synchronized (pendingCalls) {
            connected = true;
            callsToWrite = new ArrayList<AsyncCall>(pendingCalls.values());
          }
          for (AsyncCall call : callsToWrite) {
            writeRequest(call);
          }
        }
      });
    } catch (IOException e) {
      close(e);
    }
  }

  private void startConnectionWithEncryption(Channel ch) {
    // for rpc encryption, the order of ChannelInboundHandler should be:
    // LengthFieldBasedFrameDecoder->SaslClientHandler->LengthFieldBasedFrameDecoder
    // Don't skip the first 4 bytes for length in beforeUnwrapDecoder,
    // SaslClientHandler will handler this
    ch.pipeline().addFirst("beforeUnwrapDecoder",
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 0));
    ch.pipeline().addLast("afterUnwrapDecoder",
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    ch.pipeline().addLast(new AsyncServerResponseHandler(this));
    List<AsyncCall> callsToWrite;
    synchronized (pendingCalls) {
      connected = true;
      callsToWrite = new ArrayList<AsyncCall>(pendingCalls.values());
    }
    for (AsyncCall call : callsToWrite) {
      writeRequest(call);
    }
  }

  /**
   * Get SASL handler
   * @param bootstrap to reconnect to
   * @return new SASL handler
   * @throws java.io.IOException if handler failed to create
   */
  private SaslClientHandler getSaslHandler(final UserGroupInformation realTicket,
      final Bootstrap bootstrap) throws IOException {
    return new SaslClientHandler(realTicket, authMethod, token, serverPrincipal,
        client.fallbackAllowed,
        client.conf.get("hbase.rpc.protection",
          SaslUtil.QualityOfProtection.AUTHENTICATION.name().toLowerCase()),
        getChannelHeaderBytes(authMethod),
        new SaslClientHandler.SaslExceptionHandler() {
          @Override
          public void handle(int retryCount, Random random, Throwable cause) {
            try {
              // Handle Sasl failure. Try to potentially get new credentials
              handleSaslConnectionFailure(retryCount, cause, realTicket);

              retryOrClose(bootstrap, failureCounter++, random.nextInt(reloginMaxBackoff) + 1,
                cause);
            } catch (IOException | InterruptedException e) {
              close(e);
            }
          }
        }, new SaslClientHandler.SaslSuccessfulConnectHandler() {
          @Override
          public void onSuccess(Channel channel) {
            startHBaseConnection(channel);
          }

          @Override
          public void onSaslProtectionSucess(Channel channel) {
            startConnectionWithEncryption(channel);
          }
        });
  }

  /**
   * Retry to connect or close
   * @param bootstrap to connect with
   * @param failureCount failure count
   * @param e exception of fail
   */
  private void retryOrClose(final Bootstrap bootstrap, int failureCount, long timeout,
      Throwable e) {
    if (failureCount < client.maxRetries) {
      client.newTimeout(new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
          connect(bootstrap);
        }
      }, timeout, TimeUnit.MILLISECONDS);
    } else {
      client.failedServers.addToFailedServers(address);
      close(e);
    }
  }

  /**
   * Calls method on channel
   * @param method to call
   * @param request to send
   * @param cellScanner with cells to send
   * @param responsePrototype to construct response with
   * @param rpcTimeout timeout for request
   * @param priority for request
   * @return Promise for the response Message
   */
  @Override
  public <R extends Message, O> Future<O> callMethod(
      final Descriptors.MethodDescriptor method,
      final Message request,final CellScanner cellScanner,
      R responsePrototype, MessageConverter<R, O> messageConverter, IOExceptionConverter
      exceptionConverter, long rpcTimeout, int priority) {
    final AsyncCall<R, O> call = new AsyncCall<>(this, client.callIdCnt.getAndIncrement(),
        method, request, cellScanner, responsePrototype, messageConverter, exceptionConverter,
        rpcTimeout, priority, client.metrics);

    synchronized (pendingCalls) {
      if (closed) {
        call.setFailure(new ConnectException());
        return call;
      }
      pendingCalls.put(call.id, call);
      // Add timeout for cleanup if none is present
      if (cleanupTimer == null && call.getRpcTimeout() > 0) {
        cleanupTimer = client.newTimeout(timeoutTask, call.getRpcTimeout(), TimeUnit.MILLISECONDS);
      }
      if (!connected) {
        return call;
      }
    }
    writeRequest(call);
    return call;
  }

  @Override
  public EventLoop getEventExecutor() {
    return this.channel.eventLoop();
  }

  AsyncCall removePendingCall(int id) {
    synchronized (pendingCalls) {
      return pendingCalls.remove(id);
    }
  }

  /**
   * Write the channel header
   * @param channel to write to
   * @return future of write
   * @throws java.io.IOException on failure to write
   */
  private ChannelFuture writeChannelHeader(Channel channel) throws IOException {
    RPCProtos.ConnectionHeader header = getChannelHeader(authMethod);
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header);
    ByteBuf b = channel.alloc().directBuffer(totalSize);

    b.writeInt(header.getSerializedSize());
    b.writeBytes(header.toByteArray());

    return channel.writeAndFlush(b);
  }

  private byte[] getChannelHeaderBytes(AuthMethod authMethod) {
    RPCProtos.ConnectionHeader header = getChannelHeader(authMethod);
    ByteBuffer b = ByteBuffer.allocate(header.getSerializedSize() + 4);
    b.putInt(header.getSerializedSize());
    b.put(header.toByteArray());
    return b.array();
  }

  private RPCProtos.ConnectionHeader getChannelHeader(AuthMethod authMethod) {
    RPCProtos.ConnectionHeader.Builder headerBuilder = RPCProtos.ConnectionHeader.newBuilder()
        .setServiceName(serviceName);

    RPCProtos.UserInformation userInfoPB = buildUserInfo(ticket.getUGI(), authMethod);
    if (userInfoPB != null) {
      headerBuilder.setUserInfo(userInfoPB);
    }

    if (client.codec != null) {
      headerBuilder.setCellBlockCodecClass(client.codec.getClass().getCanonicalName());
    }
    if (client.compressor != null) {
      headerBuilder.setCellBlockCompressorClass(client.compressor.getClass().getCanonicalName());
    }

    headerBuilder.setVersionInfo(ProtobufUtil.getVersionInfo());
    return headerBuilder.build();
  }

  /**
   * Write request to channel
   * @param call to write
   */
  private void writeRequest(final AsyncCall call) {
    try {
      final RPCProtos.RequestHeader.Builder requestHeaderBuilder = RPCProtos.RequestHeader
          .newBuilder();
      requestHeaderBuilder.setCallId(call.id).setMethodName(call.method.getName())
          .setRequestParam(call.param != null);

      if (Trace.isTracing()) {
        Span s = Trace.currentSpan();
        requestHeaderBuilder.setTraceInfo(TracingProtos.RPCTInfo.newBuilder()
            .setParentId(s.getSpanId()).setTraceId(s.getTraceId()));
      }

      ByteBuffer cellBlock = client.buildCellBlock(call.cellScanner());
      if (cellBlock != null) {
        final RPCProtos.CellBlockMeta.Builder cellBlockBuilder = RPCProtos.CellBlockMeta
            .newBuilder();
        cellBlockBuilder.setLength(cellBlock.limit());
        requestHeaderBuilder.setCellBlockMeta(cellBlockBuilder.build());
      }
      // Only pass priority if there one. Let zero be same as no priority.
      if (call.getPriority() != PayloadCarryingRpcController.PRIORITY_UNSET) {
        requestHeaderBuilder.setPriority(call.getPriority());
      }
      requestHeaderBuilder.setTimeout(call.rpcTimeout > Integer.MAX_VALUE ?
          Integer.MAX_VALUE : (int)call.rpcTimeout);

      RPCProtos.RequestHeader rh = requestHeaderBuilder.build();

      int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(rh, call.param);
      if (cellBlock != null) {
        totalSize += cellBlock.remaining();
      }

      ByteBuf b = channel.alloc().directBuffer(4 + totalSize);
      try (ByteBufOutputStream out = new ByteBufOutputStream(b)) {
        call.callStats.setRequestSizeBytes(IPCUtil.write(out, rh, call.param, cellBlock));
      }

      channel.writeAndFlush(b).addListener(new CallWriteListener(this, call.id));
    } catch (IOException e) {
      close(e);
    }
  }

  /**
   * Set up server authorization
   * @throws java.io.IOException if auth setup failed
   */
  private void setupAuthorization() throws IOException {
    SecurityInfo securityInfo = SecurityInfo.getInfo(serviceName);
    this.useSasl = client.userProvider.isHBaseSecurityEnabled();

    this.token = null;
    if (useSasl && securityInfo != null) {
      AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
      if (tokenKind != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector = TOKEN_HANDDLERS.get(tokenKind);
        if (tokenSelector != null) {
          token = tokenSelector.selectToken(new Text(client.clusterId),
            ticket.getUGI().getTokens());
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("No token selector found for type " + tokenKind);
        }
      }
      String serverKey = securityInfo.getServerPrincipal();
      if (serverKey == null) {
        throw new IOException("Can't obtain server Kerberos config key from SecurityInfo");
      }
      this.serverPrincipal = SecurityUtil.getServerPrincipal(client.conf.get(serverKey),
        address.getAddress().getCanonicalHostName().toLowerCase());
      if (LOG.isDebugEnabled()) {
        LOG.debug("RPC Server Kerberos principal name for service=" + serviceName + " is "
            + serverPrincipal);
      }
    }

    if (!useSasl) {
      authMethod = AuthMethod.SIMPLE;
    } else if (token != null) {
      authMethod = AuthMethod.DIGEST;
    } else {
      authMethod = AuthMethod.KERBEROS;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Use " + authMethod + " authentication for service " + serviceName + ", sasl=" + useSasl);
    }
    reloginMaxBackoff = client.conf.getInt("hbase.security.relogin.maxbackoff", 5000);
  }

  /**
   * Build the user information
   * @param ugi User Group Information
   * @param authMethod Authorization method
   * @return UserInformation protobuf
   */
  private RPCProtos.UserInformation buildUserInfo(UserGroupInformation ugi, AuthMethod authMethod) {
    if (ugi == null || authMethod == AuthMethod.DIGEST) {
      // Don't send user for token auth
      return null;
    }
    RPCProtos.UserInformation.Builder userInfoPB = RPCProtos.UserInformation.newBuilder();
    if (authMethod == AuthMethod.KERBEROS) {
      // Send effective user for Kerberos auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
    } else if (authMethod == AuthMethod.SIMPLE) {
      // Send both effective user and real user for simple auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
      if (ugi.getRealUser() != null) {
        userInfoPB.setRealUser(ugi.getRealUser().getUserName());
      }
    }
    return userInfoPB.build();
  }

  /**
   * Create connection preamble
   * @param byteBuf to write to
   * @param authMethod to write
   */
  private void createPreamble(ByteBuf byteBuf, AuthMethod authMethod) {
    byteBuf.writeBytes(HConstants.RPC_HEADER);
    byteBuf.writeByte(HConstants.RPC_CURRENT_VERSION);
    byteBuf.writeByte(authMethod.code);
  }

  private void close0(Throwable e) {
    List<AsyncCall> toCleanup;
    synchronized (pendingCalls) {
      if (closed) {
        return;
      }
      closed = true;
      toCleanup = new ArrayList<AsyncCall>(pendingCalls.values());
      pendingCalls.clear();
    }
    IOException closeException = null;
    if (e != null) {
      if (e instanceof IOException) {
        closeException = (IOException) e;
      } else {
        closeException = new IOException(e);
      }
    }
    // log the info
    if (LOG.isDebugEnabled() && closeException != null) {
      LOG.debug(name + ": closing ipc connection to " + address, closeException);
    }
    if (cleanupTimer != null) {
      cleanupTimer.cancel();
      cleanupTimer = null;
    }
    for (AsyncCall call : toCleanup) {
      call.setFailed(closeException != null ? closeException
          : new ConnectionClosingException(
              "Call id=" + call.id + " on server " + address + " aborted: connection is closing"));
    }
    channel.disconnect().addListener(ChannelFutureListener.CLOSE);
    if (LOG.isDebugEnabled()) {
      LOG.debug(name + ": closed");
    }
  }

  /**
   * Close connection
   * @param e exception on close
   */
  public void close(final Throwable e) {
    client.removeConnection(this);

    // Move closing from the requesting thread to the channel thread
    if (channel.eventLoop().inEventLoop()) {
      close0(e);
    } else {
      channel.eventLoop().execute(new Runnable() {
        @Override
        public void run() {
          close0(e);
        }
      });
    }
  }

  /**
   * Clean up calls.
   */
  private void cleanupCalls() {
    List<AsyncCall> toCleanup = new ArrayList<AsyncCall>();
    long currentTime = EnvironmentEdgeManager.currentTime();
    long nextCleanupTaskDelay = -1L;
    synchronized (pendingCalls) {
      for (Iterator<AsyncCall> iter = pendingCalls.values().iterator(); iter.hasNext();) {
        AsyncCall call = iter.next();
        long timeout = call.getRpcTimeout();
        if (timeout > 0) {
          if (currentTime - call.getStartTime() >= timeout) {
            iter.remove();
            toCleanup.add(call);
          } else {
            if (nextCleanupTaskDelay < 0 || timeout < nextCleanupTaskDelay) {
              nextCleanupTaskDelay = timeout;
            }
          }
        }
      }
      if (nextCleanupTaskDelay > 0) {
        cleanupTimer = client.newTimeout(timeoutTask, nextCleanupTaskDelay, TimeUnit.MILLISECONDS);
      } else {
        cleanupTimer = null;
      }
    }
    for (AsyncCall call : toCleanup) {
      call.setFailed(new CallTimeoutException("Call id=" + call.id + ", waitTime="
          + (currentTime - call.getStartTime()) + ", rpcTimeout=" + call.getRpcTimeout()));
    }
  }

  /**
   * Check if the connection is alive
   * @return true if alive
   */
  public boolean isAlive() {
    return channel.isOpen();
  }

  @Override
  public InetSocketAddress getAddress() {
    return this.address;
  }

  /**
   * Check if user should authenticate over Kerberos
   * @return true if should be authenticated over Kerberos
   * @throws java.io.IOException on failure of check
   */
  private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    return authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
      loginUser.hasKerberosCredentials() &&
      // relogin only in case it is the login user (e.g. JT)
      // or superuser (like oozie).
      (loginUser.equals(currentUser) || loginUser.equals(realUser));
  }

  /**
   * If multiple clients with the same principal try to connect to the same server at the same time,
   * the server assumes a replay attack is in progress. This is a feature of kerberos. In order to
   * work around this, what is done is that the client backs off randomly and tries to initiate the
   * connection again. The other problem is to do with ticket expiry. To handle that, a relogin is
   * attempted.
   * <p>
   * The retry logic is governed by the {@link #shouldAuthenticateOverKrb} method. In case when the
   * user doesn't have valid credentials, we don't need to retry (from cache or ticket). In such
   * cases, it is prudent to throw a runtime exception when we receive a SaslException from the
   * underlying authentication implementation, so there is no retry from other high level (for eg,
   * HCM or HBaseAdmin).
   * </p>
   * @param currRetries retry count
   * @param ex exception describing fail
   * @param user which is trying to connect
   * @throws java.io.IOException if IO fail
   * @throws InterruptedException if thread is interrupted
   */
  private void handleSaslConnectionFailure(final int currRetries, final Throwable ex,
      final UserGroupInformation user) throws IOException, InterruptedException {
    user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException, InterruptedException {
        if (shouldAuthenticateOverKrb()) {
          if (currRetries < MAX_SASL_RETRIES) {
            LOG.debug("Exception encountered while connecting to the server : " + ex);
            // try re-login
            if (UserGroupInformation.isLoginKeytabBased()) {
              UserGroupInformation.getLoginUser().reloginFromKeytab();
            } else {
              UserGroupInformation.getLoginUser().reloginFromTicketCache();
            }

            // Should reconnect
            return null;
          } else {
            String msg = "Couldn't setup connection for "
                + UserGroupInformation.getLoginUser().getUserName() + " to " + serverPrincipal;
            LOG.warn(msg, ex);
            throw (IOException) new IOException(msg).initCause(ex);
          }
        } else {
          LOG.warn("Exception encountered while connecting to " + "the server : " + ex);
        }
        if (ex instanceof RemoteException) {
          throw (RemoteException) ex;
        }
        if (ex instanceof SaslException) {
          String msg = "SASL authentication failed."
              + " The most likely cause is missing or invalid credentials." + " Consider 'kinit'.";
          LOG.fatal(msg, ex);
          throw new RuntimeException(msg, ex);
        }
        throw new IOException(ex);
      }
    });
  }

  public int getConnectionHashCode() {
    return ConnectionId.hashCode(ticket, serviceName, address);
  }

  @Override
  public int hashCode() {
    return getConnectionHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AsyncRpcChannelImpl) {
      AsyncRpcChannelImpl channel = (AsyncRpcChannelImpl) obj;
      return channel.hashCode() == obj.hashCode();
    }
    return false;
  }

  @Override
  public String toString() {
    return this.address.toString() + "/" + this.serviceName + "/" + this.ticket;
  }

  /**
   * Listens to call writes and fails if write failed
   */
  private static final class CallWriteListener implements ChannelFutureListener {
    private final AsyncRpcChannelImpl rpcChannel;
    private final int id;

    public CallWriteListener(AsyncRpcChannelImpl asyncRpcChannelImpl, int id) {
      this.rpcChannel = asyncRpcChannelImpl;
      this.id = id;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess()) {
        AsyncCall call = rpcChannel.removePendingCall(id);
        if (call != null) {
          if (future.cause() instanceof IOException) {
            call.setFailed((IOException) future.cause());
          } else {
            call.setFailed(new IOException(future.cause()));
          }
        }
      }
    }
  }
}
