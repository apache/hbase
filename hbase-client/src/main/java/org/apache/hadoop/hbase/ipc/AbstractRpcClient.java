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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * Provides the basics for a RpcClient implementation like configuration and Logging.
 */
@InterfaceAudience.Private
public abstract class AbstractRpcClient implements RpcClient {
  public static final Log LOG = LogFactory.getLog(AbstractRpcClient.class);

  protected final Configuration conf;
  protected String clusterId;
  protected final SocketAddress localAddr;

  protected UserProvider userProvider;
  protected final IPCUtil ipcUtil;

  protected final int minIdleTimeBeforeClose; // if the connection is idle for more than this
  // time (in ms), it will be closed at any moment.
  protected final int maxRetries; //the max. no. of retries for socket connections
  protected final long failureSleep; // Time to sleep before retry on failure.
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final Codec codec;
  protected final CompressionCodec compressor;
  protected final boolean fallbackAllowed;

  protected final int connectTO;
  protected final int readTO;
  protected final int writeTO;

  /**
   * Construct an IPC client for the cluster <code>clusterId</code>
   *
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   */
  public AbstractRpcClient(Configuration conf, String clusterId, SocketAddress localAddr) {
    this.userProvider = UserProvider.instantiate(conf);
    this.localAddr = localAddr;
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.clusterId = clusterId != null ? clusterId : HConstants.CLUSTER_ID_DEFAULT;
    this.failureSleep = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", true);
    this.ipcUtil = new IPCUtil(conf);

    this.minIdleTimeBeforeClose = conf.getInt(IDLE_TIME, 120000); // 2 minutes
    this.conf = conf;
    this.codec = getCodec();
    this.compressor = getCompressor(conf);
    this.fallbackAllowed = conf.getBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.connectTO = conf.getInt(SOCKET_TIMEOUT_CONNECT, DEFAULT_SOCKET_TIMEOUT_CONNECT);
    this.readTO = conf.getInt(SOCKET_TIMEOUT_READ, DEFAULT_SOCKET_TIMEOUT_READ);
    this.writeTO = conf.getInt(SOCKET_TIMEOUT_WRITE, DEFAULT_SOCKET_TIMEOUT_WRITE);

    // login the server principal (if using secure Hadoop)
    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor +
          ", tcpKeepAlive=" + this.tcpKeepAlive +
          ", tcpNoDelay=" + this.tcpNoDelay +
          ", connectTO=" + this.connectTO +
          ", readTO=" + this.readTO +
          ", writeTO=" + this.writeTO +
          ", minIdleTimeBeforeClose=" + this.minIdleTimeBeforeClose +
          ", maxRetries=" + this.maxRetries +
          ", fallbackAllowed=" + this.fallbackAllowed +
          ", bind address=" + (this.localAddr != null ? this.localAddr : "null"));
    }
  }

  @VisibleForTesting
  public static String getDefaultCodec(final Configuration c) {
    // If "hbase.client.default.rpc.codec" is empty string -- you can't set it to null because
    // Configuration will complain -- then no default codec (and we'll pb everything).  Else
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
    if (className == null || className.length() == 0) return null;
    try {
      return (Codec)Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting codec " + className, e);
    }
  }

  @Override
  public boolean hasCellBlockSupport() {
    return this.codec != null;
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @param conf configuration
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
   * either {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin} or
   * {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#ThreadLocal},
   * otherwise default to the former.
   *
   * For applications with many user threads, use a small round-robin pool. For
   * applications with few user threads, you may want to try using a
   * thread-local pool. In any case, the number of {@link org.apache.hadoop.hbase.ipc.RpcClient}
   * instances should not exceed the operating system's hard limit on the number of
   * connections.
   *
   * @param config configuration
   * @return either a {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin} or
   *         {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#ThreadLocal}
   */
  protected static PoolMap.PoolType getPoolType(Configuration config) {
    return PoolMap.PoolType
        .valueOf(config.get(HConstants.HBASE_CLIENT_IPC_POOL_TYPE), PoolMap.PoolType.RoundRobin,
            PoolMap.PoolType.ThreadLocal);
  }

  /**
   * Return the pool size specified in the configuration, which is applicable only if
   * the pool type is {@link org.apache.hadoop.hbase.util.PoolMap.PoolType#RoundRobin}.
   *
   * @param config configuration
   * @return the maximum pool size
   */
  protected static int getPoolSize(Configuration config) {
    return config.getInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, 1);
  }

  /**
   * Make a blocking call. Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   *
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *               {@link UserProvider#getCurrent()} makes a new instance of User each time so
   *               will be a
   *               new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   */
  Message callBlockingMethod(Descriptors.MethodDescriptor md, PayloadCarryingRpcController pcrc,
      Message param, Message returnType, final User ticket, final InetSocketAddress isa)
      throws ServiceException {
    if (pcrc == null) {
      pcrc = new PayloadCarryingRpcController();
    }

    long startTime = 0;
    if (LOG.isTraceEnabled()) {
      startTime = EnvironmentEdgeManager.currentTime();
    }
    Pair<Message, CellScanner> val;
    try {
      val = call(pcrc, md, param, returnType, ticket, isa);
      // Shove the results into controller so can be carried across the proxy/pb service void.
      pcrc.setCellScanner(val.getSecond());

      if (LOG.isTraceEnabled()) {
        long callTime = EnvironmentEdgeManager.currentTime() - startTime;
        LOG.trace("Call: " + md.getName() + ", callTime: " + callTime + "ms");
      }
      return val.getFirst();
    } catch (Throwable e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   *
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *               {@link UserProvider#getCurrent()} makes a new instance of User each time so
   *               will be a
   *               new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws java.io.IOException
   */
  protected abstract Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc,
      Descriptors.MethodDescriptor md, Message param, Message returnType, User ticket,
      InetSocketAddress isa) throws IOException, InterruptedException;

  @Override
  public BlockingRpcChannel createBlockingRpcChannel(final ServerName sn, final User ticket,
      int defaultOperationTimeout) throws UnknownHostException {
    return new BlockingRpcChannelImplementation(this, sn, ticket, defaultOperationTimeout);
  }

  /**
   * Takes an Exception and the address we were trying to connect to and return an IOException with
   * the input exception as the cause. The new exception provides the stack trace of the place where
   * the exception is thrown and some extra diagnostics information. If the exception is
   * ConnectException or SocketTimeoutException, return a new one of the same type; Otherwise return
   * an IOException.
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  protected IOException wrapException(InetSocketAddress addr, Exception exception) {
    if (exception instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (ConnectException) new ConnectException("Call to " + addr
          + " failed on connection exception: " + exception).initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException) new SocketTimeoutException("Call to " + addr
          + " failed because " + exception).initCause(exception);
    } else if (exception instanceof ConnectionClosingException) {
      return (ConnectionClosingException) new ConnectionClosingException("Call to " + addr
          + " failed on local exception: " + exception).initCause(exception);
    } else {
      return (IOException) new IOException("Call to " + addr + " failed on local exception: "
          + exception).initCause(exception);
    }
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  @VisibleForTesting
  public static class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private final InetSocketAddress isa;
    private final AbstractRpcClient rpcClient;
    private final User ticket;
    private final int channelOperationTimeout;

    /**
     * @param channelOperationTimeout - the default timeout when no timeout is given
     */
    protected BlockingRpcChannelImplementation(final AbstractRpcClient rpcClient,
        final ServerName sn, final User ticket, int channelOperationTimeout)
        throws UnknownHostException {
      this.isa = new InetSocketAddress(sn.getHostname(), sn.getPort());
      if (this.isa.isUnresolved()) {
        throw new UnknownHostException(sn.getHostname());
      }
      this.rpcClient = rpcClient;
      this.ticket = ticket;
      this.channelOperationTimeout = channelOperationTimeout;
    }

    @Override
    public Message callBlockingMethod(Descriptors.MethodDescriptor md, RpcController controller,
        Message param, Message returnType) throws ServiceException {
      PayloadCarryingRpcController pcrc;
      if (controller != null && controller instanceof PayloadCarryingRpcController) {
        pcrc = (PayloadCarryingRpcController) controller;
        if (!pcrc.hasCallTimeout()) {
          pcrc.setCallTimeout(channelOperationTimeout);
        }
      } else {
        pcrc = new PayloadCarryingRpcController();
        pcrc.setCallTimeout(channelOperationTimeout);
      }

      return this.rpcClient.callBlockingMethod(md, pcrc, param, returnType, this.ticket, this.isa);
    }
  }
}
