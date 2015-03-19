/**
 *
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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.RpcCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.protobuf.generated.TracingProtos.RPCTInfo;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import javax.net.SocketFactory;
import javax.security.sasl.SaslException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Does RPC against a cluster.  Manages connections per regionserver in the cluster.
 * <p>See HBaseServer
 */
@InterfaceAudience.Private
public class RpcClientImpl extends AbstractRpcClient {
  protected final AtomicInteger callIdCnt = new AtomicInteger();

  protected final PoolMap<ConnectionId, Connection> connections;

  protected final AtomicBoolean running = new AtomicBoolean(true); // if client runs

  protected final FailedServers failedServers;

  protected final SocketFactory socketFactory;           // how to create sockets

  protected final static Map<AuthenticationProtos.TokenIdentifier.Kind,
      TokenSelector<? extends TokenIdentifier>> tokenHandlers =
      new HashMap<AuthenticationProtos.TokenIdentifier.Kind,
        TokenSelector<? extends TokenIdentifier>>();
  static {
    tokenHandlers.put(AuthenticationProtos.TokenIdentifier.Kind.HBASE_AUTH_TOKEN,
        new AuthenticationTokenSelector());
  }

  /**
   * Creates a connection. Can be overridden by a subclass for testing.
   * @param remoteId - the ConnectionId to use for the connection creation.
   */
  protected Connection createConnection(ConnectionId remoteId, final Codec codec,
      final CompressionCodec compressor)
  throws IOException {
    return new Connection(remoteId, codec, compressor);
  }

  /**
   * see {@link RpcClientImpl.Connection.CallSender}
   */
  private static class CallFuture {
    final Call call;
    final int priority;
    final Span span;

    // We will use this to stop the writer
    final static CallFuture DEATH_PILL = new CallFuture(null, -1, null);

    CallFuture(Call call, int priority, Span span) {
      this.call = call;
      this.priority = priority;
      this.span = span;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  protected class Connection extends Thread {
    private ConnectionHeader header;              // connection header
    protected ConnectionId remoteId;
    protected Socket socket = null;                 // connected socket
    protected DataInputStream in;
    protected DataOutputStream out;
    private Object outLock = new Object();
    private InetSocketAddress server;             // server ip:port
    private String serverPrincipal;  // server's krb5 principal name
    private AuthMethod authMethod; // authentication method
    private boolean useSasl;
    private Token<? extends TokenIdentifier> token;
    private HBaseSaslRpcClient saslRpcClient;
    private int reloginMaxBackoff; // max pause before relogin on sasl failure
    private final Codec codec;
    private final CompressionCodec compressor;

    // currently active calls
    protected final ConcurrentSkipListMap<Integer, Call> calls =
      new ConcurrentSkipListMap<Integer, Call>();

    protected final AtomicBoolean shouldCloseConnection = new AtomicBoolean();
    protected final CallSender callSender;


    /**
     * If the client wants to interrupt its calls easily (i.e. call Thread#interrupt),
     *  it gets into a java issue: an interruption during a write closes the socket/channel.
     * A way to avoid this is to use a different thread for writing. This way, on interruptions,
     *  we either cancel the writes or ignore the answer if the write is already done, but we
     *  don't stop the write in the middle.
     * This adds a thread per region server in the client, so it's kept as an option.
     * <p>
     * The implementation is simple: the client threads adds their call to the queue, and then
     *  wait for an answer. The CallSender blocks on the queue, and writes the calls one
     *  after the other. On interruption, the client cancels its call. The CallSender checks that
     *  the call has not been canceled before writing it.
     * </p>
     * When the connection closes, all the calls not yet sent are dismissed. The client thread
     *  is notified with an appropriate exception, as if the call was already sent but the answer
     *  not yet received.
     * </p>
     */
    private class CallSender extends Thread implements Closeable {
      protected final BlockingQueue<CallFuture> callsToWrite;


      public CallFuture sendCall(Call call, int priority, Span span)
          throws InterruptedException, IOException {
        CallFuture cts = new CallFuture(call, priority, span);
        if (!callsToWrite.offer(cts)) {
          throw new IOException("Can't add the call " + call.id +
              " to the write queue. callsToWrite.size()=" + callsToWrite.size());
        }
        checkIsOpen(); // We check after the put, to be sure that the call we added won't stay
                       //  in the list while the cleanup was already done.
        return cts;
      }

      @Override
      public void close(){
        assert shouldCloseConnection.get();
        callsToWrite.offer(CallFuture.DEATH_PILL);
        // We don't care if we can't add the death pill to the queue: the writer
        //  won't be blocked in the 'take', as its queue is full.
      }

      CallSender(String name, Configuration conf) {
        int queueSize = conf.getInt("hbase.ipc.client.write.queueSize", 1000);
        callsToWrite = new ArrayBlockingQueue<CallFuture>(queueSize);
        setDaemon(true);
        setName(name + " - writer");
      }

      public void remove(CallFuture cts){
        callsToWrite.remove(cts);

        // By removing the call from the expected call list, we make the list smaller, but
        //  it means as well that we don't know how many calls we cancelled.
        calls.remove(cts.call.id);
        cts.call.callComplete();
      }

      /**
       * Reads the call from the queue, write them on the socket.
       */
      @Override
      public void run() {
        while (!shouldCloseConnection.get()) {
          CallFuture cts = null;
          try {
            cts = callsToWrite.take();
          } catch (InterruptedException e) {
            markClosed(new InterruptedIOException());
          }

          if (cts == null || cts == CallFuture.DEATH_PILL) {
            assert shouldCloseConnection.get();
            break;
          }

          if (cts.call.done) {
            continue;
          }

          if (cts.call.checkAndSetTimeout()) {
            continue;
          }

          try {
            Connection.this.tracedWriteRequest(cts.call, cts.priority, cts.span);
          } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("call write error for call #" + cts.call.id
                + ", message =" + e.getMessage());
            }
            cts.call.setException(e);
            markClosed(e);
          }
        }

        cleanup();
      }

      /**
       * Cleans the call not yet sent when we finish.
       */
      private void cleanup() {
        assert shouldCloseConnection.get();

        IOException ie = new ConnectionClosingException("Connection to " + server + " is closing.");
        while (true) {
          CallFuture cts = callsToWrite.poll();
          if (cts == null) {
            break;
          }
          if (cts.call != null && !cts.call.done) {
            cts.call.setException(ie);
          }
        }
      }
    }

    Connection(ConnectionId remoteId, final Codec codec, final CompressionCodec compressor)
    throws IOException {
      if (remoteId.getAddress().isUnresolved()) {
        throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
      }
      this.server = remoteId.getAddress();
      this.codec = codec;
      this.compressor = compressor;

      UserGroupInformation ticket = remoteId.getTicket().getUGI();
      SecurityInfo securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
      this.useSasl = userProvider.isHBaseSecurityEnabled();
      if (useSasl && securityInfo != null) {
        AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
        if (tokenKind != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector =
              tokenHandlers.get(tokenKind);
          if (tokenSelector != null) {
            token = tokenSelector.selectToken(new Text(clusterId),
                ticket.getTokens());
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("No token selector found for type "+tokenKind);
          }
        }
        String serverKey = securityInfo.getServerPrincipal();
        if (serverKey == null) {
          throw new IOException(
              "Can't obtain server Kerberos config key from SecurityInfo");
        }
        serverPrincipal = SecurityUtil.getServerPrincipal(
            conf.get(serverKey), server.getAddress().getCanonicalHostName().toLowerCase());
        if (LOG.isDebugEnabled()) {
          LOG.debug("RPC Server Kerberos principal name for service="
              + remoteId.getServiceName() + " is " + serverPrincipal);
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
        LOG.debug("Use " + authMethod + " authentication for service " + remoteId.serviceName +
          ", sasl=" + useSasl);
      }
      reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
      this.remoteId = remoteId;

      ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
      builder.setServiceName(remoteId.getServiceName());
      UserInformation userInfoPB = getUserInfo(ticket);
      if (userInfoPB != null) {
        builder.setUserInfo(userInfoPB);
      }
      if (this.codec != null) {
        builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
      }
      if (this.compressor != null) {
        builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
      }
      builder.setVersionInfo(ProtobufUtil.getVersionInfo());
      this.header = builder.build();

      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
        remoteId.getAddress().toString() +
        ((ticket==null)?" from an unknown user": (" from "
        + ticket.getUserName())));
      this.setDaemon(true);

      if (conf.getBoolean(SPECIFIC_WRITE_THREAD, false)) {
        callSender = new CallSender(getName(), conf);
        callSender.start();
      } else {
        callSender = null;
      }
    }

    private UserInformation getUserInfo(UserGroupInformation ugi) {
      if (ugi == null || authMethod == AuthMethod.DIGEST) {
        // Don't send user for token auth
        return null;
      }
      UserInformation.Builder userInfoPB = UserInformation.newBuilder();
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

    protected synchronized void setupConnection() throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.setKeepAlive(tcpKeepAlive);
          if (localAddr != null) {
            this.socket.bind(localAddr);
          }
          NetUtils.connect(this.socket, remoteId.getAddress(), connectTO);
          this.socket.setSoTimeout(readTO);
          return;
        } catch (SocketTimeoutException toe) {
          /* The max number of retries is 45,
           * which amounts to 20s*45 = 15 minutes retries.
           */
          handleConnectionFailure(timeoutFailures++, maxRetries, toe);
        } catch (IOException ie) {
          handleConnectionFailure(ioFailures++, maxRetries, ie);
        }
      }
    }

    protected synchronized void closeConnection() {
      if (socket == null) {
        return;
      }

      // close the current connection
      try {
        if (socket.getOutputStream() != null) {
          socket.getOutputStream().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
        if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
      }
      try {
        if (socket.getInputStream() != null) {
          socket.getInputStream().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
        if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
      }
      try {
        if (socket.getChannel() != null) {
          socket.getChannel().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
        if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
      }
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }

      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /**
     *  Handle connection failures
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff N seconds and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe)
    throws IOException {
      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries || ExceptionUtil.isInterrupt(ioe)) {
        throw ioe;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(failureSleep);
      } catch (InterruptedException ie) {
        ExceptionUtil.rethrowIfInterrupt(ie);
      }

      LOG.info("Retrying connect to server: " + remoteId.getAddress() +
        " after sleeping " + failureSleep + "ms. Already tried " + curRetries +
        " time(s).");
    }

    /**
     * @throws IOException if the connection is not open.
     */
    private void checkIsOpen() throws IOException {
      if (shouldCloseConnection.get()) {
        throw new ConnectionClosingException(getName() + " is closing");
      }
    }

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed,
     * or the client is marked as not running.
     *
     * @return true if it is time to read a response; false otherwise.
     */
    protected synchronized boolean waitForWork() throws InterruptedException {
      // beware of the concurrent access to the calls list: we can add calls, but as well
      //  remove them.
      long waitUntil = EnvironmentEdgeManager.currentTime() + minIdleTimeBeforeClose;

      while (true) {
        if (shouldCloseConnection.get()) {
          return false;
        }

        if (!running.get()) {
          markClosed(new IOException("stopped with " + calls.size() + " pending request(s)"));
          return false;
        }

        if (!calls.isEmpty()) {
          // shouldCloseConnection can be set to true by a parallel thread here. The caller
          //  will need to check anyway.
          return true;
        }

        if (EnvironmentEdgeManager.currentTime() >= waitUntil) {
          // Connection is idle.
          // We expect the number of calls to be zero here, but actually someone can
          //  adds a call at the any moment, as there is no synchronization between this task
          //  and adding new calls. It's not a big issue, but it will get an exception.
          markClosed(new IOException(
              "idle connection closed with " + calls.size() + " pending request(s)"));
          return false;
        }

        wait(Math.min(minIdleTimeBeforeClose, 1000));
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return remoteId.getAddress();
    }

    @Override
    public void run() {
      if (LOG.isTraceEnabled()) {
        LOG.trace(getName() + ": starting, connections " + connections.size());
      }

      try {
        while (waitForWork()) { // Wait here for work - read or close connection
          readResponse();
        }
      } catch (InterruptedException t) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(getName() + ": interrupted while waiting for call responses");
        }
        markClosed(ExceptionUtil.asInterrupt(t));
      } catch (Throwable t) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": unexpected throwable while waiting for call responses", t);
        }
        markClosed(new IOException("Unexpected throwable while waiting call responses", t));
      }

      close();

      if (LOG.isTraceEnabled()) {
        LOG.trace(getName() + ": stopped, connections " + connections.size());
      }
    }

    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ioe) {
          LOG.error("Error disposing of SASL client", ioe);
        }
      }
    }

    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      return authMethod == AuthMethod.KERBEROS &&
          loginUser != null &&
          //Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser));
    }

    private synchronized boolean setupSaslConnection(final InputStream in2,
        final OutputStream out2) throws IOException {
      saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal, fallbackAllowed,
          conf.get("hbase.rpc.protection",
              QualityOfProtection.AUTHENTICATION.name().toLowerCase()));
      return saslRpcClient.saslConnect(in2, out2);
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
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries,
        final int maxRetries, final Exception ex, final Random rand,
        final UserGroupInformation user)
    throws IOException, InterruptedException{
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          closeConnection();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to " +
                    "the server : " + ex);
              }
              //try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              disposeSasl();
              //have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(reloginMaxBackoff) + 1));
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
            throw (RemoteException)ex;
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

    protected synchronized void setupIOstreams() throws IOException {
      if (socket != null) {
        // The connection is already available. Perfect.
        return;
      }

      if (shouldCloseConnection.get()){
        throw new ConnectionClosingException("This connection is closing");
      }

      if (failedServers.isFailedServer(remoteId.getAddress())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not trying to connect to " + server +
              " this server is in the failed servers list");
        }
        IOException e = new FailedServerException(
            "This server is in the failed servers list: " + server);
        markClosed(e);
        close();
        throw e;
      }

      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to " + server);
        }
        short numRetries = 0;
        final short MAX_RETRIES = 5;
        Random rand = null;
        while (true) {
          setupConnection();
          InputStream inStream = NetUtils.getInputStream(socket);
          // This creates a socket with a write timeout. This timeout cannot be changed.
          OutputStream outStream = NetUtils.getOutputStream(socket, writeTO);
          // Write out the preamble -- MAGIC, version, and auth to use.
          writeConnectionHeaderPreamble(outStream);
          if (useSasl) {
            final InputStream in2 = inStream;
            final OutputStream out2 = outStream;
            UserGroupInformation ticket = remoteId.getTicket().getUGI();
            if (authMethod == AuthMethod.KERBEROS) {
              if (ticket != null && ticket.getRealUser() != null) {
                ticket = ticket.getRealUser();
              }
            }
            boolean continueSasl;
            if (ticket == null) throw new FatalConnectionException("ticket/user is null");
            try {
              continueSasl = ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException {
                  return setupSaslConnection(in2, out2);
                }
              });
            } catch (Exception ex) {
              ExceptionUtil.rethrowIfInterrupt(ex);
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand, ticket);
              continue;
            }
            if (continueSasl) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
            } else {
              // fall back to simple auth because server told us so.
              authMethod = AuthMethod.SIMPLE;
              useSasl = false;
            }
          }
          this.in = new DataInputStream(new BufferedInputStream(inStream));
          synchronized (this.outLock) {
            this.out = new DataOutputStream(new BufferedOutputStream(outStream));
          }
          // Now write out the connection header
          writeConnectionHeader();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (Throwable t) {
        IOException e = ExceptionUtil.asInterrupt(t);
        if (e == null) {
          failedServers.addToFailedServers(remoteId.address);
          if (t instanceof LinkageError) {
            // probably the hbase hadoop version does not match the running hadoop version
            e = new DoNotRetryIOException(t);
          } else if (t instanceof IOException) {
            e = (IOException) t;
          } else {
            e = new IOException("Could not set up IO Streams to " + server, t);
          }
        }
        markClosed(e);
        close();
        throw e;
      }
    }

    /**
     * Write the RPC header: <MAGIC WORD -- 'HBas'> <ONEBYTE_VERSION> <ONEBYTE_AUTH_TYPE>
     */
    private void writeConnectionHeaderPreamble(OutputStream outStream) throws IOException {
      // Assemble the preamble up in a buffer first and then send it.  Writing individual elements,
      // they are getting sent across piecemeal according to wireshark and then server is messing
      // up the reading on occasion (the passed in stream is not buffered yet).

      // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
      int rpcHeaderLen = HConstants.RPC_HEADER.length;
      byte [] preamble = new byte [rpcHeaderLen + 2];
      System.arraycopy(HConstants.RPC_HEADER, 0, preamble, 0, rpcHeaderLen);
      preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
      preamble[rpcHeaderLen + 1] = authMethod.code;
      outStream.write(preamble);
      outStream.flush();
    }

    /**
     * Write the connection header.
     */
    private synchronized void writeConnectionHeader() throws IOException {
      synchronized (this.outLock) {
        this.out.writeInt(this.header.getSerializedSize());
        this.header.writeTo(this.out);
        this.out.flush();
      }
    }

    /** Close the connection. */
    protected synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error(getName() + ": the connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        connections.removeValue(remoteId, this);
      }

      // close the streams and therefore the socket
      synchronized(this.outLock) {
        if (this.out != null) {
          IOUtils.closeStream(out);
          this.out = null;
        }
      }
      IOUtils.closeStream(in);
      this.in = null;
      disposeSasl();

      // log the info
      if (LOG.isTraceEnabled()) {
        LOG.trace(getName() + ": closing ipc connection to " + server);
      }

      cleanupCalls(true);

      if (LOG.isTraceEnabled()) {
        LOG.trace(getName() + ": ipc connection to " + server + " closed");
      }
    }

    protected void tracedWriteRequest(Call call, int priority, Span span) throws IOException {
      TraceScope ts = Trace.continueSpan(span);
      try {
        writeRequest(call, priority, span);
      } finally {
        ts.close();
      }
    }

    /**
     * Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @see #readResponse()
     */
    private void writeRequest(Call call, final int priority, Span span) throws IOException {
      RequestHeader.Builder builder = RequestHeader.newBuilder();
      builder.setCallId(call.id);
      if (span != null) {
        builder.setTraceInfo(
            RPCTInfo.newBuilder().setParentId(span.getSpanId()).setTraceId(span.getTraceId()));
      }
      builder.setMethodName(call.md.getName());
      builder.setRequestParam(call.param != null);
      ByteBuffer cellBlock = ipcUtil.buildCellBlock(this.codec, this.compressor, call.cells);
      if (cellBlock != null) {
        CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
        cellBlockBuilder.setLength(cellBlock.limit());
        builder.setCellBlockMeta(cellBlockBuilder.build());
      }
      // Only pass priority if there one.  Let zero be same as no priority.
      if (priority != 0) builder.setPriority(priority);
      RequestHeader header = builder.build();

      setupIOstreams();

      // Now we're going to write the call. We take the lock, then check that the connection
      //  is still valid, and, if so we do the write to the socket. If the write fails, we don't
      //  know where we stand, we have to close the connection.
      checkIsOpen();
      IOException writeException = null;
      synchronized (this.outLock) {
        if (Thread.interrupted()) throw new InterruptedIOException();

        calls.put(call.id, call); // We put first as we don't want the connection to become idle.
        checkIsOpen(); // Now we're checking that it didn't became idle in between.

        try {
          IPCUtil.write(this.out, header, call.param, cellBlock);
        } catch (IOException e) {
          // We set the value inside the synchronized block, this way the next in line
          //  won't even try to write
          shouldCloseConnection.set(true);
          writeException = e;
          interrupt();
        }
      }

      // We added a call, and may be started the connection close. In both cases, we
      //  need to notify the reader.
      synchronized (this) {
        notifyAll();
      }

      // Now that we notified, we can rethrow the exception if any. Otherwise we're good.
      if (writeException != null) throw writeException;
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    protected void readResponse() {
      if (shouldCloseConnection.get()) return;
      Call call = null;
      boolean expectedCall = false;
      try {
        // See HBaseServer.Call.setResponse for where we write out the response.
        // Total size of the response.  Unused.  But have to read it in anyways.
        int totalSize = in.readInt();

        // Read the header
        ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
        int id = responseHeader.getCallId();
        call = calls.remove(id); // call.done have to be set before leaving this method
        expectedCall = (call != null && !call.done);
        if (!expectedCall) {
          // So we got a response for which we have no corresponding 'call' here on the client-side.
          // We probably timed out waiting, cleaned up all references, and now the server decides
          // to return a response.  There is nothing we can do w/ the response at this stage. Clean
          // out the wire of the response so its out of the way and we can get other responses on
          // this connection.
          int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
          int whatIsLeftToRead = totalSize - readSoFar;
          IOUtils.skipFully(in, whatIsLeftToRead);
          return;
        }
        if (responseHeader.hasException()) {
          ExceptionResponse exceptionResponse = responseHeader.getException();
          RemoteException re = createRemoteException(exceptionResponse);
          call.setException(re);
          if (isFatalConnectionException(exceptionResponse)) {
            markClosed(re);
          }
        } else {
          Message value = null;
          if (call.responseDefaultType != null) {
            Builder builder = call.responseDefaultType.newBuilderForType();
            builder.mergeDelimitedFrom(in);
            value = builder.build();
          }
          CellScanner cellBlockScanner = null;
          if (responseHeader.hasCellBlockMeta()) {
            int size = responseHeader.getCellBlockMeta().getLength();
            byte [] cellBlock = new byte[size];
            IOUtils.readFully(this.in, cellBlock, 0, cellBlock.length);
            cellBlockScanner = ipcUtil.createCellScanner(this.codec, this.compressor, cellBlock);
          }
          call.setResponse(value, cellBlockScanner);
        }
      } catch (IOException e) {
        if (expectedCall) call.setException(e);
        if (e instanceof SocketTimeoutException) {
          // Clean up open calls but don't treat this as a fatal condition,
          // since we expect certain responses to not make it by the specified
          // {@link ConnectionId#rpcTimeout}.
          if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
        } else {
          // Treat this as a fatal condition and close this connection
          markClosed(e);
        }
      } finally {
        cleanupCalls(false);
      }
    }

    /**
     * @return True if the exception is a fatal connection exception.
     */
    private boolean isFatalConnectionException(final ExceptionResponse e) {
      return e.getExceptionClassName().
        equals(FatalConnectionException.class.getName());
    }

    /**
     * @param e exception to be wrapped
     * @return RemoteException made from passed <code>e</code>
     */
    private RemoteException createRemoteException(final ExceptionResponse e) {
      String innerExceptionClassName = e.getExceptionClassName();
      boolean doNotRetry = e.getDoNotRetry();
      return e.hasHostname()?
        // If a hostname then add it to the RemoteWithExtrasException
        new RemoteWithExtrasException(innerExceptionClassName,
          e.getStackTrace(), e.getHostname(), e.getPort(), doNotRetry):
        new RemoteWithExtrasException(innerExceptionClassName,
          e.getStackTrace(), doNotRetry);
    }

    protected synchronized void markClosed(IOException e) {
      if (e == null) throw new NullPointerException();

      if (shouldCloseConnection.compareAndSet(false, true)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(getName() + ": marking at should close, reason: " + e.getMessage());
        }
        if (callSender != null) {
          callSender.close();
        }
        notifyAll();
      }
    }


    /**
     * Cleanup the calls older than a given timeout, in milli seconds.
     * @param allCalls true for all calls, false for only the calls in timeout
     */
    protected synchronized void cleanupCalls(boolean allCalls) {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
      while (itor.hasNext()) {
        Call c = itor.next().getValue();
        if (c.done) {
          // To catch the calls without timeout that were cancelled.
          itor.remove();
        } else if (allCalls) {
          long waitTime = EnvironmentEdgeManager.currentTime() - c.getStartTime();
          IOException ie = new ConnectionClosingException("Connection to " + getRemoteAddress()
              + " is closing. Call id=" + c.id + ", waitTime=" + waitTime);
          c.setException(ie);
          itor.remove();
        } else if (c.checkAndSetTimeout()) {
          itor.remove();
        } else {
          // We expect the call to be ordered by timeout. It may not be the case, but stopping
          //  at the first valid call allows to be sure that we still have something to do without
          //  spending too much time by reading the full list.
          break;
        }
      }
    }
  }

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   * @param conf configuration
   * @param clusterId the cluster id
   * @param factory socket factory
   */
  RpcClientImpl(Configuration conf, String clusterId, SocketFactory factory) {
    this(conf, clusterId, factory, null);
  }

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   * @param conf configuration
   * @param clusterId the cluster id
   * @param factory socket factory
   * @param localAddr client socket bind address
   */
  RpcClientImpl(Configuration conf, String clusterId, SocketFactory factory,
      SocketAddress localAddr) {
    super(conf, clusterId, localAddr);

    this.socketFactory = factory;
    this.connections = new PoolMap<ConnectionId, Connection>(getPoolType(conf), getPoolSize(conf));
    this.failedServers = new FailedServers(conf);
  }

  /**
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   * @param conf configuration
   * @param clusterId the cluster id
   */
  public RpcClientImpl(Configuration conf, String clusterId) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), null);
  }

  /**
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   *
   * This method is called with reflection by the RpcClientFactory to create an instance
   *
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   */
  public RpcClientImpl(Configuration conf, String clusterId, SocketAddress localAddr) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), localAddr);
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  @Override
  public void close() {
    if (LOG.isDebugEnabled()) LOG.debug("Stopping rpc client");
    if (!running.compareAndSet(true, false)) return;

    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }

    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.info("Interrupted while stopping the client. We still have " + connections.size() +
            " connections.");
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws IOException
   */
  @Override
  protected Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc, MethodDescriptor md,
      Message param, Message returnType, User ticket, InetSocketAddress addr)
      throws IOException, InterruptedException {
    if (pcrc == null) {
      pcrc = new PayloadCarryingRpcController();
    }
    CellScanner cells = pcrc.cellScanner();

    final Call call = new Call(this.callIdCnt.getAndIncrement(), md, param, cells, returnType,
        pcrc.getCallTimeout());

    final Connection connection = getConnection(ticket, call, addr);

    final CallFuture cts;
    if (connection.callSender != null) {
      cts = connection.callSender.sendCall(call, pcrc.getPriority(), Trace.currentSpan());
        pcrc.notifyOnCancel(new RpcCallback<Object>() {
          @Override
          public void run(Object parameter) {
            connection.callSender.remove(cts);
          }
        });
        if (pcrc.isCanceled()) {
          // To finish if the call was cancelled before we set the notification (race condition)
          call.callComplete();
          return new Pair<Message, CellScanner>(call.response, call.cells);
        }
    } else {
      cts = null;
      connection.tracedWriteRequest(call, pcrc.getPriority(), Trace.currentSpan());
    }

    while (!call.done) {
      if (call.checkAndSetTimeout()) {
        if (cts != null) connection.callSender.remove(cts);
        break;
      }
      if (connection.shouldCloseConnection.get()) {
        throw new ConnectionClosingException("Call id=" + call.id +
            " on server " + addr + " aborted: connection is closing");
      }
      try {
        synchronized (call) {
          if (call.done) break;
          call.wait(Math.min(call.remainingTime(), 1000) + 1);
        }
      } catch (InterruptedException e) {
        call.setException(new InterruptedIOException());
        if (cts != null) connection.callSender.remove(cts);
        throw e;
      }
    }

    if (call.error != null) {
      if (call.error instanceof RemoteException) {
        call.error.fillInStackTrace();
        throw call.error;
      }
      // local exception
      throw wrapException(addr, call.error);
    }

    return new Pair<Message, CellScanner>(call.response, call.cells);
  }


  /**
   * Take an IOException and the address we were trying to connect to
   * and return an IOException with the input exception as the cause.
   * The new exception provides the stack trace of the place where
   * the exception is thrown and some extra diagnostics information.
   * If the exception is ConnectException or SocketTimeoutException,
   * return a new one of the same type; Otherwise return an IOException.
   *
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  protected IOException wrapException(InetSocketAddress addr,
                                         IOException exception) {
    if (exception instanceof ConnectException) {
      //connection refused; include the host:port in the error
      return (ConnectException)new ConnectException(
         "Call to " + addr + " failed on connection exception: " + exception).initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException)new SocketTimeoutException("Call to " + addr +
        " failed because " + exception).initCause(exception);
    } else if (exception instanceof ConnectionClosingException){
      return (ConnectionClosingException) new ConnectionClosingException(
          "Call to " + addr + " failed on local exception: " + exception).initCause(exception);
    } else {
      return (IOException)new IOException("Call to " + addr + " failed on local exception: " +
        exception).initCause(exception);
    }
  }

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server
   *  is known as actually dead. This will not prevent current operation to be retried, and,
   *  depending on their own behavior, they may retry on the same server. This can be a feature,
   *  for example at startup. In any case, they're likely to get connection refused (if the
   *  process died) or no route to host: i.e. their next retries should be faster and with a
   *  safe exception.
   */
  @Override
  public void cancelConnections(ServerName sn) {
    synchronized (connections) {
      for (Connection connection : connections.values()) {
        if (connection.isAlive() &&
            connection.getRemoteAddress().getPort() == sn.getPort() &&
            connection.getRemoteAddress().getHostName().equals(sn.getHostname())) {
          LOG.info("The server on " + sn.toString() +
              " is dead - stopping the connection " + connection.remoteId);
          connection.interrupt(); // We're interrupting a Reader. It means we want it to finish.
                                  // This will close the connection as well.
        }
      }
    }
  }

  /**
   *  Get a connection from the pool, or create a new one and add it to the
   * pool. Connections to a given host/port are reused.
   */
  protected Connection getConnection(User ticket, Call call, InetSocketAddress addr)
  throws IOException {
    if (!running.get()) throw new StoppedRpcClientException();
    Connection connection;
    ConnectionId remoteId =
      new ConnectionId(ticket, call.md.getService().getName(), addr);
    synchronized (connections) {
      connection = connections.get(remoteId);
      if (connection == null) {
        connection = createConnection(remoteId, this.codec, this.compressor);
        connections.put(remoteId, connection);
      }
    }

    return connection;
  }
}
