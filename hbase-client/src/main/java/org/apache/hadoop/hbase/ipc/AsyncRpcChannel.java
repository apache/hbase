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
import com.google.protobuf.RpcCallback;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
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
import org.htrace.Span;
import org.htrace.Trace;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * Netty RPC channel
 */
@InterfaceAudience.Private
public class AsyncRpcChannel {
  public static final Log LOG = LogFactory.getLog(AsyncRpcChannel.class.getName());

  private static final int MAX_SASL_RETRIES = 5;

  protected final static Map<AuthenticationProtos.TokenIdentifier.Kind, TokenSelector<? extends
      TokenIdentifier>> tokenHandlers = new HashMap<>();

  static {
    tokenHandlers.put(AuthenticationProtos.TokenIdentifier.Kind.HBASE_AUTH_TOKEN,
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

  ConcurrentSkipListMap<Integer, AsyncCall> calls = new ConcurrentSkipListMap<>();

  private int ioFailureCounter = 0;
  private int connectFailureCounter = 0;

  boolean useSasl;
  AuthMethod authMethod;
  private int reloginMaxBackoff;
  private Token<? extends TokenIdentifier> token;
  private String serverPrincipal;

  boolean shouldCloseConnection = false;
  private IOException closeException;

  private Timeout cleanupTimer;

  private final TimerTask timeoutTask = new TimerTask() {
    @Override public void run(Timeout timeout) throws Exception {
      cleanupTimer = null;
      cleanupCalls(false);
    }
  };

  /**
   * Constructor for netty RPC channel
   *
   * @param bootstrap to construct channel on
   * @param client    to connect with
   * @param ticket of user which uses connection
   *               @param serviceName name of service to connect to
   * @param address to connect to
   */
  public AsyncRpcChannel(Bootstrap bootstrap, final AsyncRpcClient client, User ticket, String
      serviceName, InetSocketAddress address) {
    this.client = client;

    this.ticket = ticket;
    this.serviceName = serviceName;
    this.address = address;

    this.channel = connect(bootstrap).channel();

    name = ("IPC Client (" + channel.hashCode() + ") connection to " +
        address.toString() +
        ((ticket == null) ?
            " from an unknown user" :
            (" from " + ticket.getName())));
  }

  /**
   * Connect to channel
   *
   * @param bootstrap to connect to
   * @return future of connection
   */
  private ChannelFuture connect(final Bootstrap bootstrap) {
    return bootstrap.remoteAddress(address).connect()
        .addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(final ChannelFuture f) throws Exception {
            if (!f.isSuccess()) {
              if (f.cause() instanceof SocketException) {
                retryOrClose(bootstrap, connectFailureCounter++, f.cause());
              } else {
                retryOrClose(bootstrap, ioFailureCounter++, f.cause());
              }
              return;
            }
            channel = f.channel();

            setupAuthorization();

            ByteBuf b = channel.alloc().directBuffer(6);
            createPreamble(b, authMethod);
            channel.writeAndFlush(b).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            if (useSasl) {
              UserGroupInformation ticket = AsyncRpcChannel.this.ticket.getUGI();
              if (authMethod == AuthMethod.KERBEROS) {
                if (ticket != null && ticket.getRealUser() != null) {
                  ticket = ticket.getRealUser();
                }
              }
              SaslClientHandler saslHandler;
              if (ticket == null) {
                throw new FatalConnectionException("ticket/user is null");
              }
              saslHandler = ticket.doAs(new PrivilegedExceptionAction<SaslClientHandler>() {
                @Override
                public SaslClientHandler run() throws IOException {
                  return getSaslHandler(bootstrap);
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
   *
   * @param ch channel to start connection on
   */
  private void startHBaseConnection(Channel ch) {
    ch.pipeline()
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    ch.pipeline().addLast(new AsyncServerResponseHandler(this));

    try {
      writeChannelHeader(ch).addListener(new GenericFutureListener<ChannelFuture>() {
        @Override public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            close(future.cause());
            return;
          }
          for (AsyncCall call : calls.values()) {
            writeRequest(call);
          }
        }
      });
    } catch (IOException e) {
      close(e);
    }
  }

  /**
   * Get SASL handler
   *
   * @param bootstrap to reconnect to
   * @return new SASL handler
   * @throws java.io.IOException if handler failed to create
   */
  private SaslClientHandler getSaslHandler(final Bootstrap bootstrap) throws IOException {
    return new SaslClientHandler(authMethod, token, serverPrincipal, client.fallbackAllowed,
        client.conf.get("hbase.rpc.protection",
            SaslUtil.QualityOfProtection.AUTHENTICATION.name().toLowerCase()),
        new SaslClientHandler.SaslExceptionHandler() {
          @Override public void handle(int retryCount, Random random, Throwable cause) {
            try {
              // Handle Sasl failure. Try to potentially get new credentials
              handleSaslConnectionFailure(retryCount, cause, ticket.getUGI());

              // Try to reconnect
              AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
                @Override public void run(Timeout timeout) throws Exception {
                  connect(bootstrap);
                }
              }, random.nextInt(reloginMaxBackoff) + 1, TimeUnit.MILLISECONDS);
            } catch (IOException | InterruptedException e) {
              close(e);
            }
          }
        }, new SaslClientHandler.SaslSuccessfulConnectHandler() {
      @Override public void onSuccess(Channel channel) {
        startHBaseConnection(channel);
      }
    });
  }

  /**
   * Retry to connect or close
   *
   * @param bootstrap      to connect with
   * @param connectCounter amount of tries
   * @param e              exception of fail
   */
  private void retryOrClose(final Bootstrap bootstrap, int connectCounter, Throwable e) {
    if (connectCounter < client.maxRetries) {
      AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
        @Override public void run(Timeout timeout) throws Exception {
          connect(bootstrap);
        }
      }, client.failureSleep, TimeUnit.MILLISECONDS);
    } else {
      client.failedServers.addToFailedServers(address);
      close(e);
    }
  }

  /**
   * Calls method on channel
   * @param method to call
   * @param controller to run call with
   * @param request to send
   * @param responsePrototype to construct response with
   */
  public Promise<Message> callMethod(final Descriptors.MethodDescriptor method,
      final PayloadCarryingRpcController controller, final Message request,
      final Message responsePrototype) {
    if (shouldCloseConnection) {
      Promise<Message> promise = channel.eventLoop().newPromise();
      promise.setFailure(new ConnectException());
      return promise;
    }

    final AsyncCall call = new AsyncCall(channel.eventLoop(), client.callIdCnt.getAndIncrement(),
        method, request, controller, responsePrototype);

    controller.notifyOnCancel(new RpcCallback<Object>() {
      @Override
      public void run(Object parameter) {
        failCall(call, new IOException("Canceled connection"));
      }
    });

    calls.put(call.id, call);

    // Add timeout for cleanup if none is present
    if (cleanupTimer == null) {
      cleanupTimer = AsyncRpcClient.WHEEL_TIMER.newTimeout(timeoutTask, call.getRpcTimeout(),
          TimeUnit.MILLISECONDS);
    }

    if(channel.isActive()) {
      writeRequest(call);
    }

    return call;
  }

  /**
   * Calls method and returns a promise
   * @param method to call
   * @param controller to run call with
   * @param request to send
   * @param responsePrototype for response message
   * @return Promise to listen to result
   * @throws java.net.ConnectException on connection failures
   */
  public Promise<Message> callMethodWithPromise(
      final Descriptors.MethodDescriptor method, final PayloadCarryingRpcController controller,
      final Message request, final Message responsePrototype) throws ConnectException {
    if (shouldCloseConnection || !channel.isOpen()) {
      throw new ConnectException();
    }

    return this.callMethod(method, controller, request, responsePrototype);
  }

  /**
   * Write the channel header
   *
   * @param channel to write to
   * @return future of write
   * @throws java.io.IOException on failure to write
   */
  private ChannelFuture writeChannelHeader(Channel channel) throws IOException {
    RPCProtos.ConnectionHeader.Builder headerBuilder =
        RPCProtos.ConnectionHeader.newBuilder().setServiceName(serviceName);

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

    RPCProtos.ConnectionHeader header = headerBuilder.build();


    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header);

    ByteBuf b = channel.alloc().directBuffer(totalSize);

    b.writeInt(header.getSerializedSize());
    b.writeBytes(header.toByteArray());

    return channel.writeAndFlush(b);
  }

  /**
   * Write request to channel
   *
   * @param call    to write
   */
  private void writeRequest(final AsyncCall call) {
    try {
      if (shouldCloseConnection) {
        return;
      }

      final RPCProtos.RequestHeader.Builder requestHeaderBuilder = RPCProtos.RequestHeader
          .newBuilder();
      requestHeaderBuilder.setCallId(call.id)
              .setMethodName(call.method.getName()).setRequestParam(call.param != null);

      if (Trace.isTracing()) {
        Span s = Trace.currentSpan();
        requestHeaderBuilder.setTraceInfo(TracingProtos.RPCTInfo.newBuilder().
            setParentId(s.getSpanId()).setTraceId(s.getTraceId()));
      }

      ByteBuffer cellBlock = client.buildCellBlock(call.controller.cellScanner());
      if (cellBlock != null) {
        final RPCProtos.CellBlockMeta.Builder cellBlockBuilder = RPCProtos.CellBlockMeta
            .newBuilder();
        cellBlockBuilder.setLength(cellBlock.limit());
        requestHeaderBuilder.setCellBlockMeta(cellBlockBuilder.build());
      }
      // Only pass priority if there one.  Let zero be same as no priority.
      if (call.controller.getPriority() != 0) {
        requestHeaderBuilder.setPriority(call.controller.getPriority());
      }

      RPCProtos.RequestHeader rh = requestHeaderBuilder.build();

      int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(rh, call.param);
      if (cellBlock != null) {
        totalSize += cellBlock.remaining();
      }

      ByteBuf b = channel.alloc().directBuffer(4 + totalSize);
      try(ByteBufOutputStream out = new ByteBufOutputStream(b)) {
        IPCUtil.write(out, rh, call.param, cellBlock);
      }

      channel.writeAndFlush(b).addListener(new CallWriteListener(this,call));
    } catch (IOException e) {
      if (!shouldCloseConnection) {
        close(e);
      }
    }
  }

  /**
   * Fail a call
   *
   * @param call  to fail
   * @param cause of fail
   */
  void failCall(AsyncCall call, IOException cause) {
    calls.remove(call.id);
    call.setFailed(cause);
  }

  /**
   * Set up server authorization
   *
   * @throws java.io.IOException if auth setup failed
   */
  private void setupAuthorization() throws IOException {
    SecurityInfo securityInfo = SecurityInfo.getInfo(serviceName);
    this.useSasl = client.userProvider.isHBaseSecurityEnabled();

    this.token = null;
    if (useSasl && securityInfo != null) {
      AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
      if (tokenKind != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector = tokenHandlers.get(tokenKind);
        if (tokenSelector != null) {
          token = tokenSelector
              .selectToken(new Text(client.clusterId), ticket.getUGI().getTokens());
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
      LOG.debug("Use " + authMethod + " authentication for service " + serviceName +
          ", sasl=" + useSasl);
    }
    reloginMaxBackoff = client.conf.getInt("hbase.security.relogin.maxbackoff", 5000);
  }

  /**
   * Build the user information
   *
   * @param ugi        User Group Information
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
      //Send both effective user and real user for simple auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
      if (ugi.getRealUser() != null) {
        userInfoPB.setRealUser(ugi.getRealUser().getUserName());
      }
    }
    return userInfoPB.build();
  }

  /**
   * Create connection preamble
   *
   * @param byteBuf    to write to
   * @param authMethod to write
   */
  private void createPreamble(ByteBuf byteBuf, AuthMethod authMethod) {
    byteBuf.writeBytes(HConstants.RPC_HEADER);
    byteBuf.writeByte(HConstants.RPC_CURRENT_VERSION);
    byteBuf.writeByte(authMethod.code);
  }

  /**
   * Close connection
   *
   * @param e exception on close
   */
  public void close(final Throwable e) {
    client.removeConnection(ConnectionId.hashCode(ticket,serviceName,address));

    // Move closing from the requesting thread to the channel thread
    channel.eventLoop().execute(new Runnable() {
      @Override
      public void run() {
        if (shouldCloseConnection) {
          return;
        }

        shouldCloseConnection = true;

        if (e != null) {
          if (e instanceof IOException) {
            closeException = (IOException) e;
          } else {
            closeException = new IOException(e);
          }
        }

        // log the info
        if (LOG.isDebugEnabled() && closeException != null) {
          LOG.debug(name + ": closing ipc connection to " + address + ": " +
              closeException.getMessage());
        }

        cleanupCalls(true);
        channel.disconnect().addListener(ChannelFutureListener.CLOSE);

        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": closed");
        }
      }
    });
  }

  /**
   * Clean up calls.
   *
   * @param cleanAll true if all calls should be cleaned, false for only the timed out calls
   */
  public void cleanupCalls(boolean cleanAll) {
    // Cancel outstanding timers
    if (cleanupTimer != null) {
      cleanupTimer.cancel();
      cleanupTimer = null;
    }

    if (cleanAll) {
      for (AsyncCall call : calls.values()) {
        synchronized (call) {
          // Calls can be done on another thread so check before failing them
          if(!call.isDone()) {
            if (closeException == null) {
              failCall(call, new ConnectionClosingException("Call id=" + call.id +
                  " on server " + address + " aborted: connection is closing"));
            } else {
              failCall(call, closeException);
            }
          }
        }
      }
    } else {
      for (AsyncCall call : calls.values()) {
        long waitTime = EnvironmentEdgeManager.currentTime() - call.getStartTime();
        long timeout = call.getRpcTimeout();
        if (timeout > 0 && waitTime >= timeout) {
          synchronized (call) {
            // Calls can be done on another thread so check before failing them
            if (!call.isDone()) {
              closeException = new CallTimeoutException("Call id=" + call.id +
                  ", waitTime=" + waitTime + ", rpcTimeout=" + timeout);
              failCall(call, closeException);
            }
          }
        } else {
          // We expect the call to be ordered by timeout. It may not be the case, but stopping
          //  at the first valid call allows to be sure that we still have something to do without
          //  spending too much time by reading the full list.
          break;
        }
      }

      if (!calls.isEmpty()) {
        AsyncCall firstCall = calls.firstEntry().getValue();

        final long newTimeout;
        long maxWaitTime = EnvironmentEdgeManager.currentTime() - firstCall.getStartTime();
        if (maxWaitTime < firstCall.getRpcTimeout()) {
          newTimeout = firstCall.getRpcTimeout() - maxWaitTime;
        } else {
          newTimeout = 0;
        }

        closeException = null;
        cleanupTimer = AsyncRpcClient.WHEEL_TIMER.newTimeout(timeoutTask,
            newTimeout, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Check if the connection is alive
   *
   * @return true if alive
   */
  public boolean isAlive() {
    return channel.isOpen();
  }

  /**
   * Check if user should authenticate over Kerberos
   *
   * @return true if should be authenticated over Kerberos
   * @throws java.io.IOException on failure of check
   */
  private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    return authMethod == AuthMethod.KERBEROS &&
        loginUser != null &&
        //Make sure user logged in using Kerberos either keytab or TGT
        loginUser.hasKerberosCredentials() &&
        // relogin only in case it is the login user (e.g. JT)
        // or superuser (like oozie).
        (loginUser.equals(currentUser) || loginUser.equals(realUser));
  }

  /**
   * If multiple clients with the same principal try to connect
   * to the same server at the same time, the server assumes a
   * replay attack is in progress. This is a feature of kerberos.
   * In order to work around this, what is done is that the client
   * backs off randomly and tries to initiate the connection
   * again.
   * The other problem is to do with ticket expiry. To handle that,
   * a relogin is attempted.
   * <p>
   * The retry logic is governed by the {@link #shouldAuthenticateOverKrb}
   * method. In case when the user doesn't have valid credentials, we don't
   * need to retry (from cache or ticket). In such cases, it is prudent to
   * throw a runtime exception when we receive a SaslException from the
   * underlying authentication implementation, so there is no retry from
   * other high level (for eg, HCM or HBaseAdmin).
   * </p>
   *
   * @param currRetries retry count
   * @param ex          exception describing fail
   * @param user        which is trying to connect
   * @throws java.io.IOException  if IO fail
   * @throws InterruptedException if thread is interrupted
   */
  private void handleSaslConnectionFailure(final int currRetries, final Throwable ex,
      final UserGroupInformation user) throws IOException, InterruptedException {
    user.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws IOException, InterruptedException {
        if (shouldAuthenticateOverKrb()) {
          if (currRetries < MAX_SASL_RETRIES) {
            LOG.debug("Exception encountered while connecting to the server : " + ex);
            //try re-login
            if (UserGroupInformation.isLoginKeytabBased()) {
              UserGroupInformation.getLoginUser().reloginFromKeytab();
            } else {
              UserGroupInformation.getLoginUser().reloginFromTicketCache();
            }

            // Should reconnect
            return null;
          } else {
            String msg = "Couldn't setup connection for " +
                UserGroupInformation.getLoginUser().getUserName() +
                " to " + serverPrincipal;
            LOG.warn(msg);
            throw (IOException) new IOException(msg).initCause(ex);
          }
        } else {
          LOG.warn("Exception encountered while connecting to " +
              "the server : " + ex);
        }
        if (ex instanceof RemoteException) {
          throw (RemoteException) ex;
        }
        if (ex instanceof SaslException) {
          String msg = "SASL authentication failed." +
              " The most likely cause is missing or invalid credentials." +
              " Consider 'kinit'.";
          LOG.fatal(msg, ex);
          throw new RuntimeException(msg, ex);
        }
        throw new IOException(ex);
      }
    });
  }

  @Override
  public String toString() {
    return this.address.toString() + "/" + this.serviceName + "/" + this.ticket;
  }

  /**
   * Listens to call writes and fails if write failed
   */
  private static final class CallWriteListener implements ChannelFutureListener {
    private final AsyncRpcChannel rpcChannel;
    private final AsyncCall call;

    public CallWriteListener(AsyncRpcChannel asyncRpcChannel, AsyncCall call) {
      this.rpcChannel = asyncRpcChannel;
      this.call = call;
    }

    @Override public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess()) {
        if(!this.call.isDone()) {
          if (future.cause() instanceof IOException) {
            rpcChannel.failCall(call, (IOException) future.cause());
          } else {
            rpcChannel.failCall(call, new IOException(future.cause()));
          }
        }
      }
    }
  }
}
