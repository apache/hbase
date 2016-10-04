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

import static org.apache.hadoop.hbase.ipc.IPCUtil.toIOE;
import static org.apache.hadoop.hbase.ipc.IPCUtil.wrapException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * Provides the basics for a RpcClient implementation like configuration and Logging.
 * <p>
 * Locking schema of the current IPC implementation
 * <ul>
 * <li>There is a lock in {@link AbstractRpcClient} to protect the fetching or creating
 * connection.</li>
 * <li>There is a lock in {@link Call} to make sure that we can only finish the call once.</li>
 * <li>The same for {@link HBaseRpcController} as {@link Call}. And see the comment of
 * {@link HBaseRpcController#notifyOnCancel(RpcCallback, HBaseRpcController.CancellationCallback)}
 * of how to deal with cancel.</li>
 * <li>For connection implementation, the construction of a connection should be as fast as possible
 * because the creation is protected under a lock. Connect to remote side when needed. There is no
 * forced locking schema for a connection implementation.</li>
 * <li>For the locking order, the {@link Call} and {@link HBaseRpcController}'s lock should be held
 * at last. So the callbacks in {@link Call} and {@link HBaseRpcController} should be execute
 * outside the lock in {@link Call} and {@link HBaseRpcController} which means the implementations
 * of the callbacks are free to hold any lock.</li>
 * </ul>
 */
@InterfaceAudience.Private
public abstract class AbstractRpcClient<T extends RpcConnection> implements RpcClient {
  // Log level is being changed in tests
  public static final Log LOG = LogFactory.getLog(AbstractRpcClient.class);

  protected static final HashedWheelTimer WHEEL_TIMER = new HashedWheelTimer(
      Threads.newDaemonThreadFactory("RpcClient-timer"), 10, TimeUnit.MILLISECONDS);

  private static final ScheduledExecutorService IDLE_CONN_SWEEPER = Executors
      .newScheduledThreadPool(1, Threads.newDaemonThreadFactory("Idle-Rpc-Conn-Sweeper"));

  protected final static Map<Kind, TokenSelector<? extends TokenIdentifier>> TOKEN_HANDLERS = new HashMap<>();

  static {
    TOKEN_HANDLERS.put(Kind.HBASE_AUTH_TOKEN, new AuthenticationTokenSelector());
  }

  protected boolean running = true; // if client runs

  protected final Configuration conf;
  protected final String clusterId;
  protected final SocketAddress localAddr;
  protected final MetricsConnection metrics;

  protected final UserProvider userProvider;
  protected final CellBlockBuilder cellBlockBuilder;

  protected final int minIdleTimeBeforeClose; // if the connection is idle for more than this
  // time (in ms), it will be closed at any moment.
  protected final int maxRetries; // the max. no. of retries for socket connections
  protected final long failureSleep; // Time to sleep before retry on failure.
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final Codec codec;
  protected final CompressionCodec compressor;
  protected final boolean fallbackAllowed;

  protected final FailedServers failedServers;

  protected final int connectTO;
  protected final int readTO;
  protected final int writeTO;

  protected final PoolMap<ConnectionId, T> connections;

  private final AtomicInteger callIdCnt = new AtomicInteger(0);

  private final ScheduledFuture<?> cleanupIdleConnectionTask;

  private int maxConcurrentCallsPerServer;

  private static final LoadingCache<InetSocketAddress, AtomicInteger> concurrentCounterCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).
          build(new CacheLoader<InetSocketAddress, AtomicInteger>() {
            @Override public AtomicInteger load(InetSocketAddress key) throws Exception {
              return new AtomicInteger(0);
            }
          });

  /**
   * Construct an IPC client for the cluster <code>clusterId</code>
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   * @param metrics the connection metrics
   */
  public AbstractRpcClient(Configuration conf, String clusterId, SocketAddress localAddr,
      MetricsConnection metrics) {
    this.userProvider = UserProvider.instantiate(conf);
    this.localAddr = localAddr;
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.clusterId = clusterId != null ? clusterId : HConstants.CLUSTER_ID_DEFAULT;
    this.failureSleep = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", true);
    this.cellBlockBuilder = new CellBlockBuilder(conf);

    this.minIdleTimeBeforeClose = conf.getInt(IDLE_TIME, 120000); // 2 minutes
    this.conf = conf;
    this.codec = getCodec();
    this.compressor = getCompressor(conf);
    this.fallbackAllowed = conf.getBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
      IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.failedServers = new FailedServers(conf);
    this.connectTO = conf.getInt(SOCKET_TIMEOUT_CONNECT, DEFAULT_SOCKET_TIMEOUT_CONNECT);
    this.readTO = conf.getInt(SOCKET_TIMEOUT_READ, DEFAULT_SOCKET_TIMEOUT_READ);
    this.writeTO = conf.getInt(SOCKET_TIMEOUT_WRITE, DEFAULT_SOCKET_TIMEOUT_WRITE);
    this.metrics = metrics;
    this.maxConcurrentCallsPerServer = conf.getInt(
        HConstants.HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD,
        HConstants.DEFAULT_HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD);

    this.connections = new PoolMap<>(getPoolType(conf), getPoolSize(conf));

    this.cleanupIdleConnectionTask = IDLE_CONN_SWEEPER.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        cleanupIdleConnections();
      }
    }, minIdleTimeBeforeClose, minIdleTimeBeforeClose, TimeUnit.MILLISECONDS);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor + ", tcpKeepAlive="
          + this.tcpKeepAlive + ", tcpNoDelay=" + this.tcpNoDelay + ", connectTO=" + this.connectTO
          + ", readTO=" + this.readTO + ", writeTO=" + this.writeTO + ", minIdleTimeBeforeClose="
          + this.minIdleTimeBeforeClose + ", maxRetries=" + this.maxRetries + ", fallbackAllowed="
          + this.fallbackAllowed + ", bind address="
          + (this.localAddr != null ? this.localAddr : "null"));
    }
  }

  private void cleanupIdleConnections() {
    long closeBeforeTime = EnvironmentEdgeManager.currentTime() - minIdleTimeBeforeClose;
    synchronized (connections) {
      for (T conn : connections.values()) {
        // remove connection if it has not been chosen by anyone for more than maxIdleTime, and the
        // connection itself has already shutdown. The latter check is because that we may still
        // have some pending calls on connection so we should not shutdown the connection outside.
        // The connection itself will disconnect if there is no pending call for maxIdleTime.
        if (conn.getLastTouched() < closeBeforeTime && !conn.isActive()) {
          LOG.info("Cleanup idle connection to " + conn.remoteId().address);
          connections.removeValue(conn.remoteId(), conn);
          conn.cleanupConnection();
        }
      }
    }
  }

  @VisibleForTesting
  public static String getDefaultCodec(final Configuration c) {
    // If "hbase.client.default.rpc.codec" is empty string -- you can't set it to null because
    // Configuration will complain -- then no default codec (and we'll pb everything). Else
    // default is KeyValueCodec
    return c.get(DEFAULT_CODEC_CLASS, KeyValueCodec.class.getCanonicalName());
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @return Codec to use on this client.
   */
  Codec getCodec() {
    // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
    // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
    String className = conf.get(HConstants.RPC_CODEC_CONF_KEY, getDefaultCodec(this.conf));
    if (className == null || className.length() == 0) {
      return null;
    }
    try {
      return (Codec) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting codec " + className, e);
    }
  }

  @Override
  public boolean hasCellBlockSupport() {
    return this.codec != null;
  }

  // for writing tests that want to throw exception when connecting.
  @VisibleForTesting
  boolean isTcpNoDelay() {
    return tcpNoDelay;
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @param conf configuration
   * @return The compressor to use on this client.
   */
  private static CompressionCodec getCompressor(final Configuration conf) {
    String className = conf.get("hbase.client.rpc.compressor", null);
    if (className == null || className.isEmpty()) {
      return null;
    }
    try {
      return (CompressionCodec) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting compressor " + className, e);
    }
  }

  /**
   * Return the pool type specified in the configuration, which must be set to either
   * {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin} or
   * {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#ThreadLocal}, otherwise default to the
   * former. For applications with many user threads, use a small round-robin pool. For applications
   * with few user threads, you may want to try using a thread-local pool. In any case, the number
   * of {@link org.apache.hadoop.hbase.ipc.RpcClient} instances should not exceed the operating
   * system's hard limit on the number of connections.
   * @param config configuration
   * @return either a {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin} or
   *         {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#ThreadLocal}
   */
  private static PoolMap.PoolType getPoolType(Configuration config) {
    return PoolMap.PoolType.valueOf(config.get(HConstants.HBASE_CLIENT_IPC_POOL_TYPE),
      PoolMap.PoolType.RoundRobin, PoolMap.PoolType.ThreadLocal);
  }

  /**
   * Return the pool size specified in the configuration, which is applicable only if the pool type
   * is {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin}.
   * @param config configuration
   * @return the maximum pool size
   */
  private static int getPoolSize(Configuration config) {
    return config.getInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, 1);
  }

  private int nextCallId() {
    int id, next;
    do {
      id = callIdCnt.get();
      next = id < Integer.MAX_VALUE ? id + 1 : 0;
    } while (!callIdCnt.compareAndSet(id, next));
    return id;
  }

  /**
   * Make a blocking call. Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   */
  private Message callBlockingMethod(Descriptors.MethodDescriptor md, HBaseRpcController hrc,
      Message param, Message returnType, final User ticket, final InetSocketAddress isa)
      throws ServiceException {
    BlockingRpcCallback<Message> done = new BlockingRpcCallback<>();
    callMethod(md, hrc, param, returnType, ticket, isa, done);
    Message val;
    try {
      val = done.get();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    if (hrc.failed()) {
      throw new ServiceException(hrc.getFailed());
    } else {
      return val;
    }
  }

  /**
   * Get a connection from the pool, or create a new one and add it to the pool. Connections to a
   * given host/port are reused.
   */
  private T getConnection(ConnectionId remoteId) throws IOException {
    if (failedServers.isFailedServer(remoteId.getAddress())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + remoteId.address
            + " this server is in the failed servers list");
      }
      throw new FailedServerException(
          "This server is in the failed servers list: " + remoteId.address);
    }
    T conn;
    synchronized (connections) {
      if (!running) {
        throw new StoppedRpcClientException();
      }
      conn = connections.get(remoteId);
      if (conn == null) {
        conn = createConnection(remoteId);
        connections.put(remoteId, conn);
      }
      conn.setLastTouched(EnvironmentEdgeManager.currentTime());
    }
    return conn;
  }

  /**
   * Not connected.
   */
  protected abstract T createConnection(ConnectionId remoteId) throws IOException;

  private void onCallFinished(Call call, HBaseRpcController hrc, InetSocketAddress addr,
      RpcCallback<Message> callback) {
    call.callStats.setCallTimeMs(EnvironmentEdgeManager.currentTime() - call.getStartTime());
    if (metrics != null) {
      metrics.updateRpc(call.md, call.param, call.callStats);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Call: " + call.md.getName() + ", callTime: " + call.callStats.getCallTimeMs() + "ms");
    }
    if (call.error != null) {
      if (call.error instanceof RemoteException) {
        call.error.fillInStackTrace();
        hrc.setFailed(call.error);
      } else {
        hrc.setFailed(wrapException(addr, call.error));
      }
      callback.run(null);
    } else {
      hrc.setDone(call.cells);
      callback.run(call.response);
    }
  }

  private void callMethod(final Descriptors.MethodDescriptor md, final HBaseRpcController hrc,
      final Message param, Message returnType, final User ticket, final InetSocketAddress addr,
      final RpcCallback<Message> callback) {
    final MetricsConnection.CallStats cs = MetricsConnection.newCallStats();
    cs.setStartTime(EnvironmentEdgeManager.currentTime());
    final AtomicInteger counter = concurrentCounterCache.getUnchecked(addr);
    Call call = new Call(nextCallId(), md, param, hrc.cellScanner(), returnType,
        hrc.getCallTimeout(), hrc.getPriority(), new RpcCallback<Call>() {
          @Override
          public void run(Call call) {
            counter.decrementAndGet();
            onCallFinished(call, hrc, addr, callback);
          }
        }, cs);
    ConnectionId remoteId = new ConnectionId(ticket, md.getService().getName(), addr);
    int count = counter.incrementAndGet();
    try {
      if (count > maxConcurrentCallsPerServer) {
        throw new ServerTooBusyException(addr, count);
      }
      T connection = getConnection(remoteId);
      connection.sendRequest(call, hrc);
    } catch (Exception e) {
      call.setException(toIOE(e));
    }
  }

  private InetSocketAddress createAddr(ServerName sn) throws UnknownHostException {
    InetSocketAddress addr = new InetSocketAddress(sn.getHostname(), sn.getPort());
    if (addr.isUnresolved()) {
      throw new UnknownHostException("can not resolve " + sn.getServerName());
    }
    return addr;
  }

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server is
   * known as actually dead. This will not prevent current operation to be retried, and, depending
   * on their own behavior, they may retry on the same server. This can be a feature, for example at
   * startup. In any case, they're likely to get connection refused (if the process died) or no
   * route to host: i.e. their next retries should be faster and with a safe exception.
   */
  @Override
  public void cancelConnections(ServerName sn) {
    synchronized (connections) {
      for (T connection : connections.values()) {
        ConnectionId remoteId = connection.remoteId();
        if (remoteId.address.getPort() == sn.getPort()
            && remoteId.address.getHostName().equals(sn.getHostname())) {
          LOG.info("The server on " + sn.toString() + " is dead - stopping the connection "
              + connection.remoteId);
          connection.shutdown();
        }
      }
    }
  }
  /**
   * Configure an hbase rpccontroller
   * @param controller to configure
   * @param channelOperationTimeout timeout for operation
   * @return configured controller
   */
  static HBaseRpcController configureHBaseRpcController(
      RpcController controller, int channelOperationTimeout) {
    HBaseRpcController hrc;
    if (controller != null && controller instanceof HBaseRpcController) {
      hrc = (HBaseRpcController) controller;
      if (!hrc.hasCallTimeout()) {
        hrc.setCallTimeout(channelOperationTimeout);
      }
    } else {
      hrc = new HBaseRpcControllerImpl();
      hrc.setCallTimeout(channelOperationTimeout);
    }
    return hrc;
  }

  protected abstract void closeInternal();

  @Override
  public void close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping rpc client");
    }
    Collection<T> connToClose;
    synchronized (connections) {
      if (!running) {
        return;
      }
      running = false;
      connToClose = connections.values();
      connections.clear();
    }
    cleanupIdleConnectionTask.cancel(true);
    for (T conn : connToClose) {
      conn.shutdown();
    }
    closeInternal();
    for (T conn : connToClose) {
      conn.cleanupConnection();
    }
  }

  @Override
  public BlockingRpcChannel createBlockingRpcChannel(final ServerName sn, final User ticket,
      int rpcTimeout) throws UnknownHostException {
    return new BlockingRpcChannelImplementation(this, createAddr(sn), ticket, rpcTimeout);
  }

  @Override
  public RpcChannel createRpcChannel(ServerName sn, User user, int rpcTimeout)
      throws UnknownHostException {
    return new RpcChannelImplementation(this, createAddr(sn), user, rpcTimeout);
  }

  private static class AbstractRpcChannel {

    protected final InetSocketAddress addr;

    protected final AbstractRpcClient<?> rpcClient;

    protected final User ticket;

    protected final int rpcTimeout;

    protected AbstractRpcChannel(AbstractRpcClient<?> rpcClient, InetSocketAddress addr,
        User ticket, int rpcTimeout) {
      this.addr = addr;
      this.rpcClient = rpcClient;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
    }

    /**
     * Configure an rpc controller
     * @param controller to configure
     * @return configured rpc controller
     */
    protected HBaseRpcController configureRpcController(RpcController controller) {
      HBaseRpcController hrc;
      // TODO: Ideally we should not use an RpcController other than HBaseRpcController at client
      // side. And now we may use ServerRpcController.
      if (controller != null && controller instanceof HBaseRpcController) {
        hrc = (HBaseRpcController) controller;
        if (!hrc.hasCallTimeout()) {
          hrc.setCallTimeout(rpcTimeout);
        }
      } else {
        hrc = new HBaseRpcControllerImpl();
        hrc.setCallTimeout(rpcTimeout);
      }
      return hrc;
    }
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  @VisibleForTesting
  public static class BlockingRpcChannelImplementation extends AbstractRpcChannel
      implements BlockingRpcChannel {

    protected BlockingRpcChannelImplementation(AbstractRpcClient<?> rpcClient,
        InetSocketAddress addr, User ticket, int rpcTimeout) {
      super(rpcClient, addr, ticket, rpcTimeout);
    }

    @Override
    public Message callBlockingMethod(Descriptors.MethodDescriptor md, RpcController controller,
        Message param, Message returnType) throws ServiceException {
      return rpcClient.callBlockingMethod(md, configureRpcController(controller),
        param, returnType, ticket, addr);
    }
  }

  /**
   * Async rpc channel that goes via hbase rpc.
   */
  public static class RpcChannelImplementation extends AbstractRpcChannel implements
      RpcChannel {

    protected RpcChannelImplementation(AbstractRpcClient<?> rpcClient, InetSocketAddress addr,
        User ticket, int rpcTimeout) throws UnknownHostException {
      super(rpcClient, addr, ticket, rpcTimeout);
    }

    @Override
    public void callMethod(Descriptors.MethodDescriptor md, RpcController controller,
        Message param, Message returnType, RpcCallback<Message> done) {
      // This method does not throw any exceptions, so the caller must provide a
      // HBaseRpcController which is used to pass the exceptions.
      this.rpcClient.callMethod(md,
        configureRpcController(Preconditions.checkNotNull(controller,
          "RpcController can not be null for async rpc call")),
        param, returnType, ticket, addr, done);
    }
  }
}