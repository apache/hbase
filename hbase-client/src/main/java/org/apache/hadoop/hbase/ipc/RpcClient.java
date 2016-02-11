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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;
import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
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
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
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
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;


/**
 * Does RPC against a cluster.  Manages connections per regionserver in the cluster.
 * <p>See HBaseServer
 */
@InterfaceAudience.Private
public class RpcClient {
  // The LOG key is intentionally not from this package to avoid ipc logging at DEBUG (all under
  // o.a.h.hbase is set to DEBUG as default).
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.ipc.RpcClient");
  protected final PoolMap<ConnectionId, Connection> connections;

  protected int counter;                            // counter for call ids
  protected final AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final protected Configuration conf;
  final protected int maxIdleTime; // connections will be culled if it was idle for
                           // maxIdleTime microsecs
  final protected int maxRetries; //the max. no. of retries for socket connections
  final protected long failureSleep; // Time to sleep before retry on failure.
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected int pingInterval; // how often sends ping to the server in msecs
  protected FailedServers failedServers;
  private final Codec codec;
  private final CompressionCodec compressor;
  private final IPCUtil ipcUtil;

  protected final SocketFactory socketFactory;           // how to create sockets
  protected String clusterId;
  protected final SocketAddress localAddr;
  protected final MetricsConnection metrics;

  private final boolean fallbackAllowed;
  private UserProvider userProvider;

  final public static String PING_INTERVAL_NAME = "hbase.ipc.ping.interval";
  final public static String SOCKET_TIMEOUT = "hbase.ipc.socket.timeout";
  final static int DEFAULT_PING_INTERVAL = 60000;  // 1 min
  final static int DEFAULT_SOCKET_TIMEOUT = 20000; // 20 seconds
  final static int PING_CALL_ID = -1;

  public final static String FAILED_SERVER_EXPIRY_KEY = "hbase.ipc.client.failed.servers.expiry";
  public final static int FAILED_SERVER_EXPIRY_DEFAULT = 2000;

  public static final String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY =
      "hbase.ipc.client.fallback-to-simple-auth-allowed";
  public static final boolean IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;

  // thread-specific RPC timeout, which may override that of what was passed in.
  // This is used to change dynamically the timeout (for read only) when retrying: if
  //  the time allowed for the operation is less than the usual socket timeout, then
  //  we lower the timeout. This is subject to race conditions, and should be used with
  //  extreme caution.
  private static ThreadLocal<Integer> rpcTimeout = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    }
  };

  /**
   * A class to manage a list of servers that failed recently.
   */
  static class FailedServers {
    private final LinkedList<Pair<Long, String>> failedServers = new
        LinkedList<Pair<Long, java.lang.String>>();
    private final int recheckServersTimeout;

    FailedServers(Configuration conf) {
      this.recheckServersTimeout = conf.getInt(
          FAILED_SERVER_EXPIRY_KEY, FAILED_SERVER_EXPIRY_DEFAULT);
    }

    /**
     * Add an address to the list of the failed servers list.
     */
    public synchronized void addToFailedServers(InetSocketAddress address) {
      final long expiry = EnvironmentEdgeManager.currentTimeMillis() + recheckServersTimeout;
      failedServers.addFirst(new Pair<Long, String>(expiry, address.toString()));
    }

    /**
     * Check if the server should be considered as bad. Clean the old entries of the list.
     *
     * @return true if the server is in the failed servers list
     */
    public synchronized boolean isFailedServer(final InetSocketAddress address) {
      if (failedServers.isEmpty()) {
        return false;
      }

      final String lookup = address.toString();
      final long now = EnvironmentEdgeManager.currentTimeMillis();

      // iterate, looking for the search entry and cleaning expired entries
      Iterator<Pair<Long, String>> it = failedServers.iterator();
      while (it.hasNext()) {
        Pair<Long, String> cur = it.next();
        if (cur.getFirst() < now) {
          it.remove();
        } else {
          if (lookup.equals(cur.getSecond())) {
            return true;
          }
        }
      }

      return false;
    }
  }

  @SuppressWarnings("serial")
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  // Shouldn't this be a DoNotRetryException? St.Ack 10/2/2013
  public static class FailedServerException extends HBaseIOException {
    public FailedServerException(String s) {
      super(s);
    }
  }

  /**
   * set the ping interval value in configuration
   *
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  // Any reason we couldn't just do tcp keepalive instead of this pingery?
  // St.Ack 20130121
  public static void setPingInterval(Configuration conf, int pingInterval) {
    conf.setInt(PING_INTERVAL_NAME, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   *
   * @param conf Configuration
   * @return the ping interval
   */
  static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, conf.getInt("ipc.ping.interval", DEFAULT_PING_INTERVAL));
  }

  /**
   * Set the socket timeout
   * @param conf Configuration
   * @param socketTimeout the socket timeout
   */
  public static void setSocketTimeout(Configuration conf, int socketTimeout) {
    conf.setInt(SOCKET_TIMEOUT, socketTimeout);
  }

  /**
   * @return the socket timeout
   */
  static int getSocketTimeout(Configuration conf) {
    return conf.getInt(SOCKET_TIMEOUT, conf.getInt("ipc.socket.timeout", DEFAULT_SOCKET_TIMEOUT));
  }

  /** A call waiting for a value. */
  protected class Call {
    final int id;                                 // call id
    final Message param;                          // rpc request method param object
    /**
     * Optionally has cells when making call.  Optionally has cells set on response.  Used
     * passing cells to the rpc and receiving the response.
     */
    CellScanner cells;
    Message response;                             // value, null if error
    // The return type.  Used to create shell into which we deserialize the response if any.
    Message responseDefaultType;
    IOException error;                            // exception, null if value
    boolean done;                                 // true when call is done
    final MethodDescriptor md;
    final MetricsConnection.CallStats callStats;

    protected Call(final MethodDescriptor md, Message param, final CellScanner cells,
        final Message responseDefaultType, MetricsConnection.CallStats callStats) {
      this.param = param;
      this.md = md;
      this.cells = cells;
      this.callStats = callStats;
      this.callStats.setStartTime(System.currentTimeMillis());
      this.responseDefaultType = responseDefaultType;
      synchronized (RpcClient.this) {
        this.id = counter++;
      }
    }

    @Override
    public String toString() {
      return "callId: " + this.id + " methodName: " + this.md.getName() + " param {" +
        (this.param != null? ProtobufUtil.getShortTextFormat(this.param): "") + "}";
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     *
     * @param error exception thrown by the call; either local or remote
     */
    public void setException(IOException error) {
      this.error = error;
      callComplete();
    }

    /**
     * Set the return value when there is no error.
     * Notify the caller the call is done.
     *
     * @param response return value of the call.
     * @param cells Can be null
     */
    public void setResponse(Message response, final CellScanner cells) {
      this.response = response;
      this.cells = cells;
      callComplete();
    }

    public long getStartTime() {
      return this.callStats.getStartTime();
    }
  }

  protected final static Map<AuthenticationProtos.TokenIdentifier.Kind,
      TokenSelector<? extends TokenIdentifier>> tokenHandlers =
      new HashMap<AuthenticationProtos.TokenIdentifier.Kind, TokenSelector<? extends TokenIdentifier>>();
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

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  protected class Connection extends Thread {
    private ConnectionHeader header;              // connection header
    protected ConnectionId remoteId;
    protected Socket socket = null;                 // connected socket
    protected DataInputStream in;
    protected DataOutputStream out;
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
    protected final AtomicLong lastActivity =
      new AtomicLong(); // last I/O activity time
    protected final AtomicBoolean shouldCloseConnection =
      new AtomicBoolean();  // indicate if the connection is closed
    protected IOException closeException; // close reason

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
      UserInformation userInfoPB;
      if ((userInfoPB = getUserInfo(ticket)) != null) {
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

    /** Update lastActivity with the current time. */
    protected void touch() {
      lastActivity.set(System.currentTimeMillis());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized. If the connection is dead, the call is not added, and the
     * caller is notified.
     * This function can return a connection that is already marked as 'shouldCloseConnection'
     *  It is up to the user code to check this status.
     * @param call to add
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Notify because new call available for processing")
    protected synchronized void addCall(Call call) {
      // If the connection is about to close, we manage this as if the call was already added
      //  to the connection calls list. If not, the connection creations are serialized, as
      //  mentioned in HBASE-6364
      if (this.shouldCloseConnection.get()) {
        if (this.closeException == null) {
          call.setException(new IOException(
              "Call " + call.id + " not added as the connection " + remoteId + " is closing"));
        } else {
          call.setException(this.closeException);
        }
        synchronized (call) {
          call.notifyAll();
        }
      } else {
        calls.put(call.id, call);
        synchronized (call) {
          notify();
        }
      }
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    protected class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed, send a ping.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() || remoteId.rpcTimeout > 0) {
          throw e;
        }
        sendPing();
      }

      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       *
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
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
          // connection time out is 20s
          NetUtils.connect(this.socket, remoteId.getAddress(),
              getSocketTimeout(conf));
          if (remoteId.rpcTimeout > 0) {
            pingInterval = remoteId.rpcTimeout; // overwrite pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
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

    protected void closeConnection() {
      if (socket == null) {
        return;
      }

      // close the current connection
      try {
        if (socket.getOutputStream() != null) {
          socket.getOutputStream().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
      }
      try {
        if (socket.getInputStream() != null) {
          socket.getInputStream().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
      }
      try {
        if (socket.getChannel() != null) {
          socket.getChannel().close();
        }
      } catch (IOException ignored) {  // Can happen if the socket is already closed
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

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed,
     * or the client is marked as not running.
     *
     * Return true if it is time to read a response; false otherwise.
     */
    protected synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime - (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }

      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return remoteId.getAddress();
    }

    /* Send a ping to the server if the time elapsed
     * since last I/O activity is equal to or greater than the ping interval
     */
    protected synchronized void sendPing() throws IOException {
      // Can we do tcp keepalive instead of this pinging?
      long curTime = System.currentTimeMillis();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        //noinspection SynchronizeOnNonFinalField
        synchronized (this.out) {
          out.writeInt(PING_CALL_ID);
          out.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + ": starting, connections " + connections.size());
      }

      try {
        while (waitForWork()) { // Wait here for work - read or close connection
          readResponse();
        }
      } catch (Throwable t) {
        LOG.warn(getName() + ": unexpected exception receiving call responses", t);
        markClosed(new IOException("Unexpected exception receiving call responses", t));
      }

      close();

      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, connections " + connections.size());
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
        public Object run() throws IOException, InterruptedException {
          closeConnection();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              LOG.debug("Exception encountered while connecting to " +
                  "the server : " + ex);
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

    protected synchronized void setupIOstreams()
    throws IOException, InterruptedException {
      if (socket != null || shouldCloseConnection.get()) {
        return;
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
          // This creates a socket with a write timeout. This timeout cannot be changed,
          //  RpcClient allows to change the timeout dynamically, but we can only
          //  change the read timeout today.
          OutputStream outStream = NetUtils.getOutputStream(socket, pingInterval);
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
            boolean continueSasl = false;
            if (ticket == null) throw new FatalConnectionException("ticket/user is null");
            try {
              continueSasl = ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException {
                  return setupSaslConnection(in2, out2);
                }
              });
            } catch (Exception ex) {
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
          this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(inStream)));
          this.out = new DataOutputStream(new BufferedOutputStream(outStream));
          // Now write out the connection header
          writeConnectionHeader();

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (Throwable t) {
        failedServers.addToFailedServers(remoteId.address);
        IOException e = null;
        if (t instanceof LinkageError) {
          // probably the hbase hadoop version does not match the running hadoop version
          e = new DoNotRetryIOException(t);
          markClosed(e);
        } else if (t instanceof IOException) {
          e = (IOException)t;
          markClosed(e);
        } else {
          e = new IOException("Could not set up IO Streams", t);
          markClosed(e);
        }
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
      int rpcHeaderLen = HConstants.RPC_HEADER.array().length;
      byte [] preamble = new byte [rpcHeaderLen + 2];
      System.arraycopy(HConstants.RPC_HEADER.array(), 0, preamble, 0, rpcHeaderLen);
      preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
      preamble[rpcHeaderLen + 1] = authMethod.code;
      outStream.write(preamble);
      outStream.flush();
    }

    /**
     * Write the connection header.
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionHeader() throws IOException {
      synchronized (this.out) {
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
      if (this.out != null) {
        synchronized(this.out) {
          IOUtils.closeStream(out);
          this.out = null;
        }
      }
      IOUtils.closeStream(in);
      this.in = null;
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(getName() + ": connection is closed for no cause and calls are not empty. " +
              "#Calls: " + calls.size());

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": closing ipc connection to " + server + ": " +
              closeException.getMessage(), closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }

    /**
     * Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call
     * @param priority
     * @see #readResponse()
     */
    protected void writeRequest(Call call, final int priority) {
      if (shouldCloseConnection.get()) return;
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        builder.setCallId(call.id);
        if (Trace.isTracing()) {
          Span s = Trace.currentSpan();
          builder.setTraceInfo(RPCTInfo.newBuilder().
            setParentId(s.getSpanId()).setTraceId(s.getTraceId()));
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
        if (priority != PayloadCarryingRpcController.PRIORITY_UNSET) {
          builder.setPriority(priority);
        }
        //noinspection SynchronizeOnNonFinalField
        RequestHeader header = builder.build();
        synchronized (this.out) { // FindBugs IS2_INCONSISTENT_SYNC
          call.callStats.setRequestSizeBytes(
              IPCUtil.write(this.out, header, call.param, cellBlock));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": wrote request header " + TextFormat.shortDebugString(header));
        }
      } catch(IOException e) {
        synchronized (this) {
          if (!shouldCloseConnection.get()) {
            markClosed(e);
            interrupt();
          }
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    protected void readResponse() {
      if (shouldCloseConnection.get()) return;
      touch();
      int totalSize = -1;
      try {
        // See HBaseServer.Call.setResponse for where we write out the response.
        // Total size of the response.  Unused.  But have to read it in anyways.
        totalSize = in.readInt();

        // Read the header
        ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
        int id = responseHeader.getCallId();
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": got response header " +
            TextFormat.shortDebugString(responseHeader) + ", totalSize: " + totalSize + " bytes");
        }
        Call call = calls.get(id);
        if (call == null) {
          // So we got a response for which we have no corresponding 'call' here on the client-side.
          // We probably timed out waiting, cleaned up all references, and now the server decides
          // to return a response.  There is nothing we can do w/ the response at this stage. Clean
          // out the wire of the response so its out of the way and we can get other responses on
          // this connection.
          int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
          int whatIsLeftToRead = totalSize - readSoFar;
          LOG.debug("Unknown callId: " + id + ", skipping over this response of " +
            whatIsLeftToRead + " bytes");
          IOUtils.skipFully(in, whatIsLeftToRead);
        }
        if (responseHeader.hasException()) {
          ExceptionResponse exceptionResponse = responseHeader.getException();
          RemoteException re = createRemoteException(exceptionResponse);
          if (isFatalConnectionException(exceptionResponse)) {
            markClosed(re);
          } else {
            if (call != null) {
              call.setException(re);
              call.callStats.setResponseSizeBytes(totalSize);
              call.callStats.setCallTimeMs(
                  System.currentTimeMillis() - call.callStats.getStartTime());
            }
          }
        } else {
          Message value = null;
          // Call may be null because it may have timedout and been cleaned up on this side already
          if (call != null && call.responseDefaultType != null) {
            Builder builder = call.responseDefaultType.newBuilderForType();
            ProtobufUtil.mergeDelimitedFrom(builder, in);
            value = builder.build();
          }
          CellScanner cellBlockScanner = null;
          if (responseHeader.hasCellBlockMeta()) {
            int size = responseHeader.getCellBlockMeta().getLength();
            byte [] cellBlock = new byte[size];
            IOUtils.readFully(this.in, cellBlock, 0, cellBlock.length);
            cellBlockScanner = ipcUtil.createCellScanner(this.codec, this.compressor, cellBlock);
          }
          // it's possible that this call may have been cleaned up due to a RPC
          // timeout, so check if it still exists before setting the value.
          if (call != null) {
            call.setResponse(value, cellBlockScanner);
            call.callStats.setResponseSizeBytes(totalSize);
            call.callStats.setCallTimeMs(
                System.currentTimeMillis() - call.callStats.getStartTime());
          }
        }
        if (call != null) calls.remove(id);
      } catch (IOException e) {
        if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
          // Clean up open calls but don't treat this as a fatal condition,
          // since we expect certain responses to not make it by the specified
          // {@link ConnectionId#rpcTimeout}.
          closeException = e;
        } else {
          // Treat this as a fatal condition and close this connection
          markClosed(e);
        }
      } finally {
        if (remoteId.rpcTimeout > 0) {
          cleanupCalls(remoteId.rpcTimeout);
        }
      }
    }

    /**
     * @param e
     * @return True if the exception is a fatal connection exception.
     */
    private boolean isFatalConnectionException(final ExceptionResponse e) {
      return e.getExceptionClassName().
        equals(FatalConnectionException.class.getName());
    }

    /**
     * @param e
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
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }

    /* Cleanup all calls and mark them as done */
    protected void cleanupCalls() {
      cleanupCalls(0);
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Notify because timedout")
    protected void cleanupCalls(long rpcTimeout) {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
      while (itor.hasNext()) {
        Call c = itor.next().getValue();
        long waitTime = System.currentTimeMillis() - c.getStartTime();
        if (waitTime >= rpcTimeout) {
          if (this.closeException == null) {
            // There may be no exception in the case that there are many calls
            // being multiplexed over this connection and these are succeeding
            // fine while this Call object is taking a long time to finish
            // over on the server; e.g. I just asked the regionserver to bulk
            // open 3k regions or its a big fat multiput into a heavily-loaded
            // server (Perhaps this only happens at the extremes?)
            this.closeException = new CallTimeoutException("Call id=" + c.id +
              ", waitTime=" + waitTime + ", rpcTimetout=" + rpcTimeout);
          }
          c.setException(this.closeException);
          synchronized (c) {
            c.notifyAll();
          }
          itor.remove();
        } else {
          break;
        }
      }
      try {
        if (!calls.isEmpty()) {
          Call firstCall = calls.get(calls.firstKey());
          long maxWaitTime = System.currentTimeMillis() - firstCall.getStartTime();
          if (maxWaitTime < rpcTimeout) {
            rpcTimeout -= maxWaitTime;
          }
        }
        if (!shouldCloseConnection.get()) {
          closeException = null;
          setSocketTimeout(socket, (int) rpcTimeout);
        }
      } catch (SocketException e) {
        LOG.debug("Couldn't lower timeout, which may result in longer than expected calls");
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
    justification="Presume sync not needed setting socket timeout")
  private static void setSocketTimeout(final Socket socket, final int rpcTimeout)
  throws java.net.SocketException {
    if (socket == null) return;
    socket.setSoTimeout(rpcTimeout);
  }

  /**
   * Client-side call timeout
   */
  @SuppressWarnings("serial")
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class CallTimeoutException extends IOException {
    public CallTimeoutException(final String msg) {
      super(msg);
    }
  }

  /**
   * Used in test only. Construct an IPC cluster client whose values are of the
   * {@link Message} class.
   * @param conf configuration
   * @param clusterId
   * @param factory socket factory
   */
  @VisibleForTesting
  RpcClient(Configuration conf, String clusterId, SocketFactory factory) {
    this(conf, clusterId, factory, null, null);
  }

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   * @param conf configuration
   * @param clusterId
   * @param factory socket factory
   * @param localAddr client socket bind address
   * @param metrics the connection metrics
   */
  RpcClient(Configuration conf, String clusterId, SocketFactory factory, SocketAddress localAddr,
      MetricsConnection metrics) {
    this.maxIdleTime = conf.getInt("hbase.ipc.client.connection.maxidletime", 10000); //10s
    this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.failureSleep = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.pingInterval = getPingInterval(conf);
    this.ipcUtil = new IPCUtil(conf);
    this.conf = conf;
    this.codec = getCodec();
    this.compressor = getCompressor(conf);
    this.socketFactory = factory;
    this.clusterId = clusterId != null ? clusterId : HConstants.CLUSTER_ID_DEFAULT;
    this.connections = new PoolMap<ConnectionId, Connection>(getPoolType(conf), getPoolSize(conf));
    this.failedServers = new FailedServers(conf);
    this.fallbackAllowed = conf.getBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.localAddr = localAddr;
    this.userProvider = UserProvider.instantiate(conf);
    this.metrics = metrics;

    // login the server principal (if using secure Hadoop)
    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor +
        ", tcpKeepAlive=" + this.tcpKeepAlive +
        ", tcpNoDelay=" + this.tcpNoDelay +
        ", maxIdleTime=" + this.maxIdleTime +
        ", maxRetries=" + this.maxRetries +
        ", fallbackAllowed=" + this.fallbackAllowed +
        ", ping interval=" + this.pingInterval + "ms" +
        ", bind address=" + (this.localAddr != null ? this.localAddr : "null"));
    }
  }

  /**
   * Helper method for tests only. Creates an {@code RpcClient} without metrics.
   */
  @VisibleForTesting
  public RpcClient(Configuration conf, String clusterId) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), null, null);
  }

  /**
   * Construct an IPC client for the cluster {@code clusterId} with the default SocketFactory
   * @param conf configuration
   * @param clusterId
   */
  public RpcClient(Configuration conf, String clusterId, MetricsConnection metrics) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), null, metrics);
  }

  /**
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   * @param conf configuration
   * @param clusterId
   * @param localAddr client socket bind address.
   */
  public RpcClient(Configuration conf, String clusterId, SocketAddress localAddr,
      MetricsConnection metrics) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), localAddr, metrics);
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @return Codec to use on this client.
   */
  Codec getCodec() {
    // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
    // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
    String className = conf.get(HConstants.RPC_CODEC_CONF_KEY, getDefaultCodec(this.conf));
    if (className == null || className.length() == 0) return null;
    try {
      return (Codec)Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting codec " + className, e);
    }
  }

  @VisibleForTesting
  public static String getDefaultCodec(final Configuration c) {
    // If "hbase.client.default.rpc.codec" is empty string -- you can't set it to null because
    // Configuration will complain -- then no default codec (and we'll pb everything).  Else
    // default is KeyValueCodec
    return c.get("hbase.client.default.rpc.codec", KeyValueCodec.class.getCanonicalName());
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @param conf
   * @return The compressor to use on this client.
   */
  private static CompressionCodec getCompressor(final Configuration conf) {
    String className = conf.get("hbase.client.rpc.compressor", null);
    if (className == null || className.isEmpty()) return null;
    try {
        return (CompressionCodec)Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting compressor " + className, e);
    }
  }

  /**
   * Return the pool type specified in the configuration, which must be set to
   * either {@link PoolType#RoundRobin} or {@link PoolType#ThreadLocal},
   * otherwise default to the former.
   *
   * For applications with many user threads, use a small round-robin pool. For
   * applications with few user threads, you may want to try using a
   * thread-local pool. In any case, the number of {@link RpcClient} instances
   * should not exceed the operating system's hard limit on the number of
   * connections.
   *
   * @param config configuration
   * @return either a {@link PoolType#RoundRobin} or
   *         {@link PoolType#ThreadLocal}
   */
  protected static PoolType getPoolType(Configuration config) {
    return PoolType.valueOf(config.get(HConstants.HBASE_CLIENT_IPC_POOL_TYPE),
        PoolType.RoundRobin, PoolType.ThreadLocal);
  }

  /**
   * Return the pool size specified in the configuration, which is applicable only if
   * the pool type is {@link PoolType#RoundRobin}.
   *
   * @param config
   * @return the maximum pool size
   */
  protected static int getPoolSize(Configuration config) {
    return config.getInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, 1);
  }

  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
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
      } catch (InterruptedException ignored) {
      }
    }
  }

  @VisibleForTesting
  Pair<Message, CellScanner> call(MethodDescriptor md, Message param, CellScanner cells,
      Message returnType, User ticket, InetSocketAddress addr, int rpcTimeout)
  throws InterruptedException, IOException {
    return call(md, param, cells, returnType, ticket, addr, rpcTimeout, HConstants.NORMAL_QOS,
        MetricsConnection.newCallStats());
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @param md
   * @param param
   * @param cells
   * @param addr
   * @param returnType
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @param rpcTimeout
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws IOException
   */
  Pair<Message, CellScanner> call(MethodDescriptor md, Message param, CellScanner cells,
      Message returnType, User ticket, InetSocketAddress addr,
      int rpcTimeout, int priority, MetricsConnection.CallStats callStats)
  throws InterruptedException, IOException {
    Call call = new Call(md, param, cells, returnType, callStats);
    Connection connection =
      getConnection(ticket, call, addr, rpcTimeout, this.codec, this.compressor);
    connection.writeRequest(call, priority);                 // send the parameter

    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (call) {
      while (!call.done) {
        if (connection.shouldCloseConnection.get()) {
          throw new IOException("Unexpected closed connection");
        }
        call.wait(1000);                       // wait for the result
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
   *  process died) or no route to host: i.e. there next retries should be faster and with a
   *  safe exception.
   */
  public void cancelConnections(String hostname, int port, IOException ioe) {
    synchronized (connections) {
      for (Connection connection : connections.values()) {
        if (connection.isAlive() &&
            connection.getRemoteAddress().getPort() == port &&
            connection.getRemoteAddress().getHostName().equals(hostname)) {
          LOG.info("The server on " + hostname + ":" + port +
              " is dead - stopping the connection " + connection.remoteId);
          connection.closeConnection();
          // We could do a connection.interrupt(), but it's safer not to do it, as the
          //  interrupted exception behavior is not defined nor enforced enough.
        }
      }
    }
  }

  /* Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  protected Connection getConnection(User ticket, Call call, InetSocketAddress addr,
      int rpcTimeout, final Codec codec, final CompressionCodec compressor)
  throws IOException, InterruptedException {
    if (!running.get()) throw new StoppedRpcClientException();
    Connection connection;
    ConnectionId remoteId =
      new ConnectionId(ticket, call.md.getService().getName(), addr, rpcTimeout);
    synchronized (connections) {
      connection = connections.get(remoteId);
      if (connection == null) {
        connection = createConnection(remoteId, this.codec, this.compressor);
        connections.put(remoteId, connection);
      }
    }
    connection.addCall(call);

    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    //Moreover, if the connection is currently created, there will be many threads
    // waiting here; as setupIOstreams is synchronized. If the connection fails with a
    // timeout, they will all fail simultaneously. This is checked in setupIOstreams.
    connection.setupIOstreams();
    return connection;
  }

  /**
   * This class holds the address and the user ticket, etc. The client connections
   * to servers are uniquely identified by <remoteAddress, ticket, serviceName, rpcTimeout>
   */
  protected static class ConnectionId {
    final InetSocketAddress address;
    final User ticket;
    final int rpcTimeout;
    private static final int PRIME = 16777619;
    final String serviceName;

    ConnectionId(User ticket,
        String serviceName,
        InetSocketAddress address,
        int rpcTimeout) {
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.serviceName = serviceName;
    }

    String getServiceName() {
      return this.serviceName;
    }

    InetSocketAddress getAddress() {
      return address;
    }

    User getTicket() {
      return ticket;
    }

    @Override
    public String toString() {
      return this.address.toString() + "/" + this.serviceName + "/" + this.ticket + "/" +
        this.rpcTimeout;
    }

    @Override
    public boolean equals(Object obj) {
     if (obj instanceof ConnectionId) {
       ConnectionId id = (ConnectionId) obj;
       return address.equals(id.address) &&
              ((ticket != null && ticket.equals(id.ticket)) ||
               (ticket == id.ticket)) && rpcTimeout == id.rpcTimeout &&
               this.serviceName == id.serviceName;
     }
     return false;
    }

    @Override  // simply use the default Object#hashcode() ?
    public int hashCode() {
      int hashcode = (address.hashCode() +
        PRIME * (PRIME * this.serviceName.hashCode() ^
        (ticket == null ? 0 : ticket.hashCode()) )) ^
        rpcTimeout;
      return hashcode;
    }
  }

  public static void setRpcTimeout(int t) {
    rpcTimeout.set(t);
  }

  public static int getRpcTimeout() {
    return rpcTimeout.get();
  }

  /**
   * Returns the lower of the thread-local RPC time from {@link #setRpcTimeout(int)} and the given
   * default timeout.
   */
  public static int getRpcTimeout(int defaultTimeout) {
    return Math.min(defaultTimeout, rpcTimeout.get());
  }

  public static void resetRpcTimeout() {
    rpcTimeout.remove();
  }

  /**
   * Make a blocking call. Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @param md
   * @param controller
   * @param param
   * @param returnType
   * @param isa
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @param rpcTimeout
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws IOException
   */
  Message callBlockingMethod(MethodDescriptor md, RpcController controller,
      Message param, Message returnType, final User ticket, final InetSocketAddress isa,
      final int rpcTimeout)
  throws ServiceException {
    PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController)controller;
    CellScanner cells = null;
    if (pcrc != null) {
      cells = pcrc.cellScanner();
      // Clear it here so we don't by mistake try and these cells processing results.
      pcrc.setCellScanner(null);
    }
    Pair<Message, CellScanner> val = null;
    try {
      final MetricsConnection.CallStats cs = MetricsConnection.newCallStats();
      cs.setStartTime(System.currentTimeMillis());
      val = call(md, param, cells, returnType, ticket, isa, rpcTimeout,
        pcrc != null? pcrc.getPriority(): HConstants.NORMAL_QOS, cs);
      if (pcrc != null) {
        // Shove the results into controller so can be carried across the proxy/pb service void.
        if (val.getSecond() != null) pcrc.setCellScanner(val.getSecond());
      } else if (val.getSecond() != null) {
        throw new ServiceException("Client dropping data on the floor!");
      }

      cs.setCallTimeMs(System.currentTimeMillis() - cs.getStartTime());
      if (metrics != null) {
        metrics.updateRpc(md, param, cs);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Call: " + md.getName() + ", callTime: " + cs.getCallTimeMs() + "ms");
      }
      return val.getFirst();
    } catch (Throwable e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Creates a "channel" that can be used by a blocking protobuf service.  Useful setting up
   * protobuf blocking stubs.
   * @param sn
   * @param ticket
   * @param rpcTimeout
   * @return A blocking rpc channel that goes via this rpc client instance.
   */
  public BlockingRpcChannel createBlockingRpcChannel(final ServerName sn,
      final User ticket, final int rpcTimeout) {
    return new BlockingRpcChannelImplementation(this, sn, ticket, rpcTimeout);
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  // Public so can be subclassed for tests.
  public static class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private final InetSocketAddress isa;
    private volatile RpcClient rpcClient;
    private final int rpcTimeout;
    private final User ticket;

    protected BlockingRpcChannelImplementation(final RpcClient rpcClient, final ServerName sn,
        final User ticket, final int rpcTimeout) {
      this.isa = new InetSocketAddress(sn.getHostname(), sn.getPort());
      this.rpcClient = rpcClient;
      // Set the rpc timeout to be the minimum of configured timeout and whatever the current
      // thread local setting is.
      this.rpcTimeout = getRpcTimeout(rpcTimeout);
      this.ticket = ticket;
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
        Message param, Message returnType)
    throws ServiceException {
      return this.rpcClient.callBlockingMethod(md, controller, param, returnType, this.ticket,
        this.isa, this.rpcTimeout);
    }
  }
}
