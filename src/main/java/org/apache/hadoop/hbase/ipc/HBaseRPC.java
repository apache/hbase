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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/** A simple RPC mechanism.
 *
 * This is a local hbase copy of the hadoop RPC so we can do things like
 * address HADOOP-414 for hbase-only and try other hbase-specific
 * optimizations like using our own version of ObjectWritable.  Class has been
 * renamed to avoid confusing it w/ hadoop versions.
 * <p>
 *
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class HBaseRPC {
  // Leave this out in the hadoop ipc package but keep class name.  Do this
  // so that we dont' get the logging of this class's invocations by doing our
  // blanket enabling DEBUG on the o.a.h.h. package.
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseRPC");

  private HBaseRPC() {
    super();
  }                                  // no public ctor

  /**
   * Configuration key for the {@link RpcEngine} implementation to load to
   * handle connection protocols.  Handlers for individual protocols can be
   * configured using {@code "hbase.rpc.engine." + protocol.class.name}.
   */
  public static final String RPC_ENGINE_PROP = "hbase.rpc.engine";

  // thread-specific RPC timeout, which may override that of RpcEngine
  private static ThreadLocal<Integer> rpcTimeout = new ThreadLocal<Integer>() {
    @Override
      protected Integer initialValue() {
        return HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
      }
    };

  /**
   * Returns a new instance of the configured {@link RpcEngine} implementation.
   */
  public static synchronized RpcEngine getProtocolEngine(Configuration conf) {
    // check for a configured default engine
    Class<?> impl =
        conf.getClass(RPC_ENGINE_PROP, WritableRpcEngine.class);

    LOG.debug("Using RpcEngine: "+impl.getName());
    RpcEngine engine = (RpcEngine) ReflectionUtils.newInstance(impl, conf);
    return engine;
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  @SuppressWarnings("serial")
  public static class VersionMismatch extends IOException {
    private static final long serialVersionUID = 0;
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;

    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }

    /**
     * Get the interface name
     * @return the java class name
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }

    /**
     * @return the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }

    /**
     * @return the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  /**
   * An error requesting an RPC protocol that the server is not serving.
   */
  public static class UnknownProtocolException extends DoNotRetryIOException {
    private Class<?> protocol;

    public UnknownProtocolException(String mesg) {
      // required for unwrapping from a RemoteException
      super(mesg);
    }

    public UnknownProtocolException(Class<?> protocol) {
      this(protocol, "Server is not handling protocol "+protocol.getName());
    }

    public UnknownProtocolException(Class<?> protocol, String mesg) {
      super(mesg);
      this.protocol = protocol;
    }

    public Class getProtocol() {
      return protocol;
    }
  }

  /**
   * @param protocol protocol interface
   * @param clientVersion which client version we expect
   * @param addr address of remote service
   * @param conf configuration
   * @param maxAttempts max attempts
   * @param rpcTimeout timeout for each RPC
   * @param timeout timeout in milliseconds
   * @return proxy
   * @throws IOException e
   */
  @SuppressWarnings("unchecked")
  public static <T extends VersionedProtocol> T waitForProxy(RpcEngine rpcClient,
                                               Class<T> protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               int maxAttempts,
                                               int rpcTimeout,
                                               long timeout
  ) throws IOException {
    // HBase does limited number of reconnects which is different from hadoop.
    long startTime = System.currentTimeMillis();
    IOException ioe;
    int reconnectAttempts = 0;
    while (true) {
      try {
        return rpcClient.getProxy(protocol, clientVersion, addr, conf, rpcTimeout);
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch (IOException ioex) {
        // We only handle the ConnectException.
        ConnectException ce = null;
        if (ioex instanceof ConnectException) {
          ce = (ConnectException) ioex;
          ioe = ce;
        } else if (ioex.getCause() != null
            && ioex.getCause() instanceof ConnectException) {
          ce = (ConnectException) ioex.getCause();
          ioe = ce;
        } else if (ioex.getMessage().toLowerCase()
            .contains("connection refused")) {
          ce = new ConnectException(ioex.getMessage());
          ioe = ce;
        } else {
          // This is the exception we can't handle.
          ioe = ioex;
        }
        if (ce != null) {
          handleConnectionException(++reconnectAttempts, maxAttempts, protocol,
              addr, ce);
        }
      }
      // check if timed out
      if (System.currentTimeMillis() - timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /**
   * @param retries current retried times.
   * @param maxAttmpts max attempts
   * @param protocol protocol interface
   * @param addr address of remote service
   * @param ce ConnectException
   * @throws RetriesExhaustedException
   */
  private static void handleConnectionException(int retries, int maxAttmpts,
      Class<?> protocol, InetSocketAddress addr, ConnectException ce)
      throws RetriesExhaustedException {
    if (maxAttmpts >= 0 && retries >= maxAttmpts) {
      LOG.info("Server at " + addr + " could not be reached after "
          + maxAttmpts + " tries, giving up.");
      throw new RetriesExhaustedException("Failed setting up proxy " + protocol
          + " to " + addr.toString() + " after attempts=" + maxAttmpts, ce);
    }
  }
  
  /**
   * Expert: Make multiple, parallel calls to a set of servers.
   *
   * @param method method to invoke
   * @param params array of parameters
   * @param addrs array of addresses
   * @param conf configuration
   * @return values
   * @throws IOException e
   * @deprecated Instead of calling statically, use
   *     {@link HBaseRPC#getProtocolEngine(org.apache.hadoop.conf.Configuration)}
   *     to obtain an {@link RpcEngine} instance and then use
   *     {@link RpcEngine#call(java.lang.reflect.Method, Object[][], java.net.InetSocketAddress[], Class, org.apache.hadoop.hbase.security.User, org.apache.hadoop.conf.Configuration)}
   */
  @Deprecated
  public static Object[] call(Method method, Object[][] params,
      InetSocketAddress[] addrs,
      Class<? extends VersionedProtocol> protocol,
      User ticket,
      Configuration conf)
    throws IOException, InterruptedException {
    Object[] result = null;
    RpcEngine engine = null;
    try {
      engine = getProtocolEngine(conf);
      result = engine.call(method, params, addrs, protocol, ticket, conf);
    } finally {
      engine.close();
    }
    return result;
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance instance
   * @param bindAddress bind address
   * @param port port to bind to
   * @param numHandlers number of handlers to start
   * @param verbose verbose flag
   * @param conf configuration
   * @return Server
   * @throws IOException e
   */
  public static RpcServer getServer(final Object instance,
                                 final Class<?>[] ifaces,
                                 final String bindAddress, final int port,
                                 final int numHandlers,
                                 int metaHandlerCount, final boolean verbose, Configuration conf, int highPriorityLevel)
    throws IOException {
    return getServer(instance.getClass(), instance, ifaces, bindAddress, port, numHandlers, metaHandlerCount, verbose, conf, highPriorityLevel);
  }

  /** Construct a server for a protocol implementation instance. */
  public static RpcServer getServer(Class protocol,
                                 final Object instance,
                                 final Class<?>[] ifaces, String bindAddress,
                                 int port,
                                 final int numHandlers,
                                 int metaHandlerCount, final boolean verbose, Configuration conf, int highPriorityLevel)
    throws IOException {
    return getProtocolEngine(conf)
        .getServer(protocol, instance, ifaces, bindAddress, port, numHandlers, metaHandlerCount, verbose, conf, highPriorityLevel);
  }

  public static void setRpcTimeout(int rpcTimeout) {
    HBaseRPC.rpcTimeout.set(rpcTimeout);
  }

  public static int getRpcTimeout() {
    return HBaseRPC.rpcTimeout.get();
  }

  public static void resetRpcTimeout() {
    HBaseRPC.rpcTimeout.remove();
  }

  /**
   * Returns the lower of the thread-local RPC time from {@link #setRpcTimeout(int)} and the given
   * default timeout.
   */
  public static int getRpcTimeout(int defaultTimeout) {
    return Math.min(defaultTimeout, HBaseRPC.rpcTimeout.get());
  }
}
