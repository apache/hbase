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

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An RPC implementation. This class provides the client side.
 */
@InterfaceAudience.Private
public class HBaseClientRPC {
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseClientRPC");

  /**
   * Configuration key for the {@link org.apache.hadoop.hbase.ipc.RpcClientEngine}
   * implementation to load to handle connection protocols.  Handlers for individual
   * protocols can be configured using {@code "hbase.rpc.client.engine." +
   * protocol.class.name}.
   */
  public static final String RPC_ENGINE_PROP = "hbase.rpc.client.engine";

  // cache of RpcEngines by protocol
  private static final Map<Class<? extends IpcProtocol>, RpcClientEngine> PROTOCOL_ENGINES  =
    new HashMap<Class<? extends IpcProtocol>, RpcClientEngine>();

  // Track what RpcEngine is used by a proxy class, for stopProxy()
  private static final Map<Class<?>, RpcClientEngine> PROXY_ENGINES =
    new HashMap<Class<?>, RpcClientEngine>();

  // thread-specific RPC timeout, which may override that of RpcEngine
  private static ThreadLocal<Integer> rpcTimeout = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    }
  };

  // set a protocol to use a non-default RpcEngine
  static void setProtocolEngine(Configuration conf,
      Class<? extends IpcProtocol> protocol, Class<? extends RpcClientEngine> engine) {
    conf.setClass(RPC_ENGINE_PROP + "." + protocol.getName(), engine,
      RpcClientEngine.class);
  }

  // return the RpcEngine configured to handle a protocol
  static synchronized RpcClientEngine getProtocolEngine(
      Class<? extends IpcProtocol> protocol, Configuration conf) {
    RpcClientEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      // check for a configured default engine
      Class<?> defaultEngine =
        conf.getClass(RPC_ENGINE_PROP, ProtobufRpcClientEngine.class);

      // check for a per interface override
      Class<?> impl = conf.getClass(RPC_ENGINE_PROP + "." + protocol.getName(),
        defaultEngine);
      LOG.debug("Using " + impl.getName() + " for " + protocol.getName());
      engine = (RpcClientEngine) ReflectionUtils.newInstance(impl, conf);
      if (protocol.isInterface()) {
        PROXY_ENGINES.put(Proxy.getProxyClass(protocol.getClassLoader(), protocol),
          engine);
      }
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  // return the RpcEngine that handles a proxy object
  private static synchronized RpcClientEngine getProxyEngine(Object proxy) {
    return PROXY_ENGINES.get(proxy.getClass());
  }

  /**
   * @param protocol      protocol interface
   * @param addr          address of remote service
   * @param conf          configuration
   * @param maxAttempts   max attempts
   * @param rpcTimeout    timeout for each RPC
   * @param timeout       timeout in milliseconds
   * @return proxy
   * @throws java.io.IOException e
   */
  public static IpcProtocol waitForProxy(Class<? extends IpcProtocol> protocol,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               int maxAttempts,
                                               int rpcTimeout,
                                               long timeout)
  throws IOException {
    // HBase does limited number of reconnects which is different from hadoop.
    long startTime = System.currentTimeMillis();
    IOException ioe;
    int reconnectAttempts = 0;
    while (true) {
      try {
        return getProxy(protocol, addr, conf, rpcTimeout);
      } catch (SocketTimeoutException te) {
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
   * @param retries    current retried times.
   * @param maxAttmpts max attempts
   * @param protocol   protocol interface
   * @param addr       address of remote service
   * @param ce         ConnectException
   * @throws org.apache.hadoop.hbase.client.RetriesExhaustedException
   *
   */
  private static void handleConnectionException(int retries,
                                                int maxAttmpts,
                                                Class<?> protocol,
                                                InetSocketAddress addr,
                                                ConnectException ce)
      throws RetriesExhaustedException {
    if (maxAttmpts >= 0 && retries >= maxAttmpts) {
      LOG.info("Server at " + addr + " could not be reached after "
          + maxAttmpts + " tries, giving up.");
      throw new RetriesExhaustedException("Failed setting up proxy " + protocol
          + " to " + addr.toString() + " after attempts=" + maxAttmpts, ce);
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol      interface
   * @param addr          remote address
   * @param conf          configuration
   * @param factory       socket factory
   * @param rpcTimeout    timeout for each RPC
   * @return proxy
   * @throws java.io.IOException e
   */
  public static IpcProtocol getProxy(Class<? extends IpcProtocol> protocol,
                                           InetSocketAddress addr,
                                           Configuration conf,
                                           SocketFactory factory,
                                           int rpcTimeout) throws IOException {
    return getProxy(protocol, addr, User.getCurrent(), conf, factory, rpcTimeout);
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol      interface
   * @param addr          remote address
   * @param ticket        ticket
   * @param conf          configuration
   * @param factory       socket factory
   * @param rpcTimeout    timeout for each RPC
   * @return proxy
   * @throws java.io.IOException e
   */
  public static IpcProtocol getProxy(Class<? extends IpcProtocol> protocol,
      InetSocketAddress addr, User ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout)
      throws IOException {
    RpcClientEngine engine = getProtocolEngine(protocol, conf);
    IpcProtocol proxy = engine.getProxy(protocol, addr, ticket, conf, factory,
      Math.min(rpcTimeout, getRpcTimeout()));
    return proxy;
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   *
   * @param protocol      interface
   * @param addr          remote address
   * @param conf          configuration
   * @param rpcTimeout    timeout for each RPC
   * @return a proxy instance
   * @throws java.io.IOException e
   */
  public static IpcProtocol getProxy(Class<? extends IpcProtocol> protocol,
      InetSocketAddress addr, Configuration conf, int rpcTimeout)
  throws IOException {
    return getProxy(protocol, addr, conf,
      NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
  }

  /**
   * Stop this proxy and release its invoker's resource
   *
   * @param proxy the proxy to be stopped
   */
  public static void stopProxy(IpcProtocol proxy) {
    if (proxy != null) {
      getProxyEngine(proxy).stopProxy(proxy);
    }
  }

  public static void setRpcTimeout(int t) {
    rpcTimeout.set(t);
  }

  public static int getRpcTimeout() {
    return rpcTimeout.get();
  }

  public static void resetRpcTimeout() {
    rpcTimeout.remove();
  }
}