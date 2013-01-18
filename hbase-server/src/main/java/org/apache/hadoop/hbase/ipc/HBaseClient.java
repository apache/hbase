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
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;
import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcException;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcResponseBody;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcResponseHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcResponseHeader.Status;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.protobuf.generated.Tracing.RPCTInfo;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.AuthMethod;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;


/** A client for an IPC service.  IPC calls take a single Protobuf message as a
 * parameter, and return a single Protobuf message as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * <p>This is the org.apache.hadoop.ipc.Client renamed as HBaseClient and
 * moved into this package so can access package-private methods.
 *
 * @see HBaseServer
 */
@InterfaceAudience.Private
public class HBaseClient {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseClient");
  protected final PoolMap<ConnectionId, Connection> connections;
  private static final Map<String, Method> methodInstances =
      new ConcurrentHashMap<String, Method>();

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
  protected int socketTimeout; // socket timeout
  protected FailedServers failedServers;

  protected final SocketFactory socketFactory;           // how to create sockets
  protected String clusterId;

  final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
  final private static String SOCKET_TIMEOUT = "ipc.socket.timeout";
  final static int DEFAULT_PING_INTERVAL = 60000;  // 1 min
  final static int DEFAULT_SOCKET_TIMEOUT = 20000; // 20 seconds
  final static int PING_CALL_ID = -1;

  public final static String FAILED_SERVER_EXPIRY_KEY = "hbase.ipc.client.failed.servers.expiry";
  public final static int FAILED_SERVER_EXPIRY_DEFAULT = 2000;

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

  public static class FailedServerException extends IOException {
    private static final long serialVersionUID = -4744376109431464127L;

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
    return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
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
    return conf.getInt(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
  }

  /** A call waiting for a value. */
  protected class Call {
    final int id;                                 // call id
    final RpcRequestBody param;                   // rpc request object
    Message value;                                // value, null if error
    IOException error;                            // exception, null if value
    boolean done;                                 // true when call is done
    long startTime;

    protected Call(RpcRequestBody param) {
      this.param = param;
      this.startTime = System.currentTimeMillis();
      synchronized (HBaseClient.this) {
        this.id = counter++;
      }
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
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }

    /** Set the return value when there is no error.
     * Notify the caller the call is done.
     *
     * @param value return value of the call.
     */
    public synchronized void setValue(Message value) {
      this.value = value;
      callComplete();
    }

    public long getStartTime() {
      return this.startTime;
    }
  }

  protected static Map<String,TokenSelector<? extends TokenIdentifier>> tokenHandlers =
      new HashMap<String,TokenSelector<? extends TokenIdentifier>>();
  static {
    tokenHandlers.put(AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE.toString(),
        new AuthenticationTokenSelector());
  }

  /**
   * Creates a connection. Can be overridden by a subclass for testing.
   * @param remoteId - the ConnectionId to use for the connection creation.
   */
  protected Connection createConnection(ConnectionId remoteId) throws IOException {
    return new Connection(remoteId);
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

    // currently active calls
    protected final ConcurrentSkipListMap<Integer, Call> calls =
      new ConcurrentSkipListMap<Integer, Call>();
    protected final AtomicLong lastActivity =
      new AtomicLong(); // last I/O activity time
    protected final AtomicBoolean shouldCloseConnection =
      new AtomicBoolean();  // indicate if the connection is closed
    protected IOException closeException; // close reason

    Connection(ConnectionId remoteId) throws IOException {
      if (remoteId.getAddress().isUnresolved()) {
        throw new UnknownHostException("unknown host: " +
                                       remoteId.getAddress().getHostName());
      }
      this.server = remoteId.getAddress();

      UserGroupInformation ticket = remoteId.getTicket().getUGI();
      Class<?> protocol = remoteId.getProtocol();
      this.useSasl = User.isHBaseSecurityEnabled(conf);
      if (useSasl && protocol != null) {
        TokenInfo tokenInfo = protocol.getAnnotation(TokenInfo.class);
        if (tokenInfo != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector =
              tokenHandlers.get(tokenInfo.value());
          if (tokenSelector != null) {
            token = tokenSelector.selectToken(new Text(clusterId),
                ticket.getTokens());
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("No token selector found for type "+tokenInfo.value());
          }
        }
        KerberosInfo krbInfo = protocol.getAnnotation(KerberosInfo.class);
        if (krbInfo != null) {
          String serverKey = krbInfo.serverPrincipal();
          if (serverKey == null) {
            throw new IOException(
                "Can't obtain server Kerberos config key from KerberosInfo");
          }
          serverPrincipal = SecurityUtil.getServerPrincipal(
              conf.get(serverKey), server.getAddress().getCanonicalHostName().toLowerCase());
          if (LOG.isDebugEnabled()) {
            LOG.debug("RPC Server Kerberos principal name for protocol="
                + protocol.getCanonicalName() + " is " + serverPrincipal);
          }
        }
      }

      if (!useSasl) {
        authMethod = AuthMethod.SIMPLE;
      } else if (token != null) {
        authMethod = AuthMethod.DIGEST;
      } else {
        authMethod = AuthMethod.KERBEROS;
      }

      if (LOG.isDebugEnabled())
        LOG.debug("Use " + authMethod + " authentication for protocol "
            + protocol.getSimpleName());

      reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
      this.remoteId = remoteId;

      ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
      builder.setProtocol(protocol == null ? "" : protocol.getName());
      UserInformation userInfoPB;
      if ((userInfoPB = getUserInfoPB(ticket)) != null) {
        builder.setUserInfo(userInfoPB);
      }
      this.header = builder.build();

      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
        remoteId.getAddress().toString() +
        ((ticket==null)?" from an unknown user": (" from " 
        + ticket.getUserName())));
      this.setDaemon(true);
    }

    private UserInformation getUserInfoPB(UserGroupInformation ugi) {
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
        notify();
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
        if (shouldCloseConnection.get() || !running.get() ||
            remoteId.rpcTimeout > 0) {
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
      // close the current connection
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Not able to close a socket", e);
        }
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
    private void handleConnectionFailure(
        int curRetries, int maxRetries, IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(failureSleep);
      } catch (InterruptedException ignored) {}

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
        long timeout = maxIdleTime-
              (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException ignored) {}
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
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections "
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveResponse();
        }
      } catch (Throwable t) {
        LOG.warn("Unexpected exception receiving call responses", t);
        markClosed(new IOException("Unexpected exception receiving call responses", t));
      }

      close();

      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
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
      saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal);
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
          LOG.debug("Connecting to "+server);
        }
        short numRetries = 0;
        final short MAX_RETRIES = 5;
        Random rand = null;
        while (true) {
          setupConnection();
          InputStream inStream = NetUtils.getInputStream(socket);
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeRpcHeader(outStream);
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
            try {
              continueSasl =
                ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
                  @Override
                  public Boolean run() throws IOException {
                    return setupSaslConnection(in2, out2);
                  }
                });
            } catch (Exception ex) {
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                   ticket);
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
          this.in = new DataInputStream(new BufferedInputStream
              (new PingInputStream(inStream)));
          this.out = new DataOutputStream
          (new BufferedOutputStream(outStream));
          writeHeader();

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (Throwable t) {
        failedServers.addToFailedServers(remoteId.address);
        IOException e = null;
        if (t instanceof IOException) {
          e = (IOException)t;
          markClosed(e);
        } else {
          e = new IOException("Coundn't set up IO Streams", t);
          markClosed(e);
        }
        close();
        throw e;
      }
    }

    /* Write the RPC header */
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(HBaseServer.HEADER.array());
      out.write(HBaseServer.CURRENT_VERSION);
      authMethod.write(out);
      out.flush();
    }

    /**
     * Write the protocol header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader() throws IOException {
      // Write out the ConnectionHeader
      out.writeInt(header.getSerializedSize());
      header.writeTo(out);
    }

    /** Close the connection. */
    protected synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty. " +
              "#Calls: " + calls.size());

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }

    /* Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    protected void sendParam(Call call) {
      if (shouldCloseConnection.get()) {
        return;
      }
      try {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " sending #" + call.id);

        RpcRequestHeader.Builder headerBuilder = RPCProtos.RpcRequestHeader.newBuilder();
        headerBuilder.setCallId(call.id);

        if (Trace.isTracing()) {
          Span s = Trace.currentTrace();
          headerBuilder.setTinfo(RPCTInfo.newBuilder()
              .setParentId(s.getSpanId())
              .setTraceId(s.getTraceId()));
        }

        //noinspection SynchronizeOnNonFinalField
        synchronized (this.out) { // FindBugs IS2_INCONSISTENT_SYNC
          RpcRequestHeader header = headerBuilder.build();
          int serializedHeaderSize = header.getSerializedSize();
          int requestSerializedSize = call.param.getSerializedSize();
          this.out.writeInt(serializedHeaderSize +
              CodedOutputStream.computeRawVarint32Size(serializedHeaderSize) +
              requestSerializedSize +
              CodedOutputStream.computeRawVarint32Size(requestSerializedSize));
          header.writeDelimitedTo(this.out);
          call.param.writeDelimitedTo(this.out);
          this.out.flush();
        }
      } catch(IOException e) {
        markClosed(e);
      }
    }


    private Method getMethod(Class<? extends IpcProtocol> protocol,
        String methodName) {
      Method method = methodInstances.get(methodName);
      if (method != null) {
        return method;
      }
      Method[] methods = protocol.getMethods();
      for (Method m : methods) {
        if (m.getName().equals(methodName)) {
          m.setAccessible(true);
          methodInstances.put(methodName, m);
          return m;
        }
      }
      return null;
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    protected void receiveResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();

      try {
        // See HBaseServer.Call.setResponse for where we write out the response.
        // It writes the call.id (int), a boolean signifying any error (and if 
        // so the exception name/trace), and the response bytes

        // Read the call id.
        RpcResponseHeader response = RpcResponseHeader.parseDelimitedFrom(in);
        if (response == null) {
          // When the stream is closed, protobuf doesn't raise an EOFException,
          // instead, it returns a null message object. 
          throw new EOFException();
        }
        int id = response.getCallId();

        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + id);
        Call call = calls.get(id);

        Status status = response.getStatus();
        if (status == Status.SUCCESS) {
          Message rpcResponseType;
          try {
            rpcResponseType = ProtobufRpcClientEngine.Invoker.getReturnProtoType(
                getMethod(remoteId.getProtocol(),
                          call.param.getMethodName()));
          } catch (Exception e) {
            throw new RuntimeException(e); //local exception
          }
          Builder builder = rpcResponseType.newBuilderForType();
          builder.mergeDelimitedFrom(in);
          Message value = builder.build();
          // it's possible that this call may have been cleaned up due to a RPC
          // timeout, so check if it still exists before setting the value.
          if (call != null) {
            call.setValue(value);
          }
          calls.remove(id);
        } else if (status == Status.ERROR) {
          RpcException exceptionResponse = RpcException.parseDelimitedFrom(in);
          if (call != null) {
            //noinspection ThrowableInstanceNeverThrown
            call.setException(new RemoteException(
                exceptionResponse.getExceptionName(),
                exceptionResponse.getStackTrace()));
            calls.remove(id);
          }
        } else if (status == Status.FATAL) {
          RpcException exceptionResponse = RpcException.parseDelimitedFrom(in);
          // Close the connection
          markClosed(new RemoteException(
              exceptionResponse.getExceptionName(),
              exceptionResponse.getStackTrace()));
        }
      } catch (IOException e) {
        if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
          // Clean up open calls but don't treat this as a fatal condition,
          // since we expect certain responses to not make it by the specified
          // {@link ConnectionId#rpcTimeout}.
          closeException = e;
        } else {
          // Since the server did not respond within the default ping interval
          // time, treat this as a fatal condition and close this connection
          markClosed(e);
        }
      } finally {
        if (remoteId.rpcTimeout > 0) {
          cleanupCalls(remoteId.rpcTimeout);
        }
      }
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
          if (socket != null) {
            socket.setSoTimeout((int) rpcTimeout);
          }
        }
      } catch (SocketException e) {
        LOG.debug("Couldn't lower timeout, which may result in longer than expected calls");
      }
    }
  }

  /**
   * Client-side call timeout
   */
  public static class CallTimeoutException extends IOException {
    public CallTimeoutException(final String msg) {
      super(msg);
    }
  }

  /** Call implementation used for parallel calls. */
  protected class ParallelCall extends Call {
    private final ParallelResults results;
    protected final int index;

    public ParallelCall(RpcRequestBody param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    @Override
    protected void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  protected static class ParallelResults {
    protected final Message[] values;
    protected int size;
    protected int count;

    public ParallelResults(int size) {
      this.values = new RpcResponseBody[size];
      this.size = size;
    }

    /*
     * Collect a result.
     */
    synchronized void callComplete(ParallelCall call) {
      // FindBugs IS2_INCONSISTENT_SYNC
      values[call.index] = call.value;            // store the value
      count++;                                    // count it
      if (count == size)                          // if all values are in
        notify();                                 // then notify waiting caller
    }
  }

  /**
   * Construct an IPC client whose values are of the {@link Message}
   * class.
   * @param conf configuration
   * @param factory socket factory
   */
  public HBaseClient(Configuration conf, SocketFactory factory) {
    this.maxIdleTime =
      conf.getInt("hbase.ipc.client.connection.maxidletime", 10000); //10s
    this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.failureSleep = conf.getInt("hbase.client.pause", 1000);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.pingInterval = getPingInterval(conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The ping interval is" + this.pingInterval + "ms.");
    }
    this.conf = conf;
    this.socketFactory = factory;
    this.clusterId = conf.get(HConstants.CLUSTER_ID, "default");
    this.connections = new PoolMap<ConnectionId, Connection>(
        getPoolType(conf), getPoolSize(conf));
    this.failedServers = new FailedServers(conf);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param conf configuration
   */
  public HBaseClient(Configuration conf) {
    this(conf, NetUtils.getDefaultSocketFactory(conf));
  }

  /**
   * Return the pool type specified in the configuration, which must be set to
   * either {@link PoolType#RoundRobin} or {@link PoolType#ThreadLocal},
   * otherwise default to the former.
   *
   * For applications with many user threads, use a small round-robin pool. For
   * applications with few user threads, you may want to try using a
   * thread-local pool. In any case, the number of {@link HBaseClient} instances
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }

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

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @param param RpcRequestBody parameter
   * @param address network address
   * @return Message
   * @throws IOException e
   */
  public Message call(RpcRequestBody param, InetSocketAddress address)
  throws IOException, InterruptedException {
      return call(param, address, null, 0);
  }

  public Message call(RpcRequestBody param, InetSocketAddress addr,
                       User ticket, int rpcTimeout)
                       throws IOException, InterruptedException {
    return call(param, addr, null, ticket, rpcTimeout);
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception. */
  public Message call(RpcRequestBody param, InetSocketAddress addr,
                       Class<? extends IpcProtocol> protocol,
                       User ticket, int rpcTimeout)
      throws InterruptedException, IOException {
    Call call = new Call(param);
    Connection connection = getConnection(addr, protocol, ticket, rpcTimeout, call);
    connection.sendParam(call);                 // send the parameter
    boolean interrupted = false;
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ignored) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        }
        // local exception
        throw wrapException(addr, call.error);
      }
      return call.value;
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
           "Call to " + addr + " failed on connection exception: " + exception)
                    .initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException)new SocketTimeoutException(
           "Call to " + addr + " failed on socket timeout exception: "
                      + exception).initCause(exception);
    } else {
      return (IOException)new IOException(
           "Call to " + addr + " failed on local exception: " + exception)
                                 .initCause(exception);

    }
  }

  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.  */
  public Message[] call(RpcRequestBody[] params, InetSocketAddress[] addresses,
                         Class<? extends IpcProtocol> protocol,
                         User ticket)
      throws IOException, InterruptedException {
    if (addresses.length == 0) return new RpcResponseBody[0];

    ParallelResults results = new ParallelResults(params.length);
    // TODO this synchronization block doesnt make any sense, we should possibly fix it
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        try {
          Connection connection =
              getConnection(addresses[i], protocol, ticket, 0, call);
          connection.sendParam(call);             // send each parameter
        } catch (IOException e) {
          // log errors
          LOG.info("Calling "+addresses[i]+" caught: " +
                   e.getMessage(),e);
          results.size--;                         //  wait for one fewer result
        }
      }
      while (results.count != results.size) {
        try {
          results.wait();                    // wait for all results
        } catch (InterruptedException ignored) {}
      }

      return results.values;
    }
  }

  /* Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  protected Connection getConnection(InetSocketAddress addr,
                                   Class<? extends IpcProtocol> protocol,
                                   User ticket,
                                   int rpcTimeout,
                                   Call call)
                                   throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, protocol, ticket, rpcTimeout);
    synchronized (connections) {
      connection = connections.get(remoteId);
      if (connection == null) {
        connection = createConnection(remoteId);
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
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, ticket>
   */
  protected static class ConnectionId {
    final InetSocketAddress address;
    final User ticket;
    final int rpcTimeout;
    Class<? extends IpcProtocol> protocol;
    private static final int PRIME = 16777619;

    ConnectionId(InetSocketAddress address, Class<? extends IpcProtocol> protocol,
        User ticket,
        int rpcTimeout) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
    }

    InetSocketAddress getAddress() {
      return address;
    }

    Class<? extends IpcProtocol> getProtocol() {
      return protocol;
    }

    User getTicket() {
      return ticket;
    }

    @Override
    public boolean equals(Object obj) {
     if (obj instanceof ConnectionId) {
       ConnectionId id = (ConnectionId) obj;
       return address.equals(id.address) && protocol == id.protocol &&
              ((ticket != null && ticket.equals(id.ticket)) ||
               (ticket == id.ticket)) && rpcTimeout == id.rpcTimeout;
     }
     return false;
    }

    @Override  // simply use the default Object#hashcode() ?
    public int hashCode() {
      return (address.hashCode() + PRIME * (
                  PRIME * System.identityHashCode(protocol) ^
             (ticket == null ? 0 : ticket.hashCode()) )) ^ rpcTimeout;
    }
  }
}