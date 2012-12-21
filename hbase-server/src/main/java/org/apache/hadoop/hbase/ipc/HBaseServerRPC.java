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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple RPC mechanism.
 *
 * This is a local hbase copy of the hadoop RPC so we can do things like
 * address HADOOP-414 for hbase-only and try other hbase-specific
 * optimizations.  Class has been renamed to avoid confusing it w/ hadoop
 * versions.
 * <p>
 *
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be Protobuf objects.
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 *
 * This class provides the server side implementation.
 */
@InterfaceAudience.Private
public class HBaseServerRPC {
  // Leave this out in the hadoop ipc package but keep class name.  Do this
  // so that we dont' get the logging of this class's invocations by doing our
  // blanket enabling DEBUG on the o.a.h.h. package.
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseServerRPC");

  // cache of RpcEngines by protocol
  private static final Map<Class, RpcServerEngine> PROTOCOL_ENGINES
      = new HashMap<Class, RpcServerEngine>();

  /**
   * Configuration key for the {@link org.apache.hadoop.hbase.ipc.RpcServerEngine} implementation to
   * load to handle connection protocols.  Handlers for individual protocols can be
   * configured using {@code "hbase.rpc.server.engine." + protocol.class.name}.
   */
  public static final String RPC_ENGINE_PROP = "hbase.rpc.server.engine";
  // track what RpcEngine is used by a proxy class, for stopProxy()
  private static final Map<Class, RpcServerEngine> PROXY_ENGINES
      = new HashMap<Class, RpcServerEngine>();

  private HBaseServerRPC() {
    super();
  }                                  // no public ctor


  // set a protocol to use a non-default RpcEngine
  static void setProtocolEngine(Configuration conf,
                                Class protocol, Class engine) {
    conf.setClass(RPC_ENGINE_PROP + "." + protocol.getName(), engine, RpcServerEngine.class);
  }

  // return the RpcEngine configured to handle a protocol
  static synchronized RpcServerEngine getProtocolEngine(Class protocol,
                                                        Configuration conf) {
    RpcServerEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      // check for a configured default engine
      Class<?> defaultEngine =
          conf.getClass(RPC_ENGINE_PROP, ProtobufRpcServerEngine.class);

      // check for a per interface override
      Class<?> impl = conf.getClass(RPC_ENGINE_PROP + "." + protocol.getName(),
          defaultEngine);
      LOG.debug("Using " + impl.getName() + " for " + protocol.getName());
      engine = (RpcServerEngine) ReflectionUtils.newInstance(impl, conf);
      if (protocol.isInterface())
        PROXY_ENGINES.put(Proxy.getProxyClass(protocol.getClassLoader(),
            protocol),
            engine);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  // return the RpcEngine that handles a proxy object
  private static synchronized RpcServerEngine getProxyEngine(Object proxy) {
    return PROXY_ENGINES.get(proxy.getClass());
  }


  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance    instance
   * @param bindAddress bind address
   * @param port        port to bind to
   * @param numHandlers number of handlers to start
   * @param verbose     verbose flag
   * @param conf        configuration
   * @return Server
   * @throws IOException e
   */
  public static RpcServer getServer(final Object instance,
                                    final Class<?>[] ifaces,
                                    final String bindAddress, final int port,
                                    final int numHandlers,
                                    int metaHandlerCount,
                                    final boolean verbose,
                                    Configuration conf,
                                    int highPriorityLevel)
      throws IOException {
    return getServer(instance.getClass(),
        instance,
        ifaces,
        bindAddress,
        port,
        numHandlers,
        metaHandlerCount,
        verbose,
        conf,
        highPriorityLevel);
  }

  /**
   * Construct a server for a protocol implementation instance.
   */
  public static RpcServer getServer(Class protocol,
                                    final Object instance,
                                    final Class<?>[] ifaces,
                                    String bindAddress,
                                    int port,
                                    final int numHandlers,
                                    int metaHandlerCount,
                                    final boolean verbose,
                                    Configuration conf,
                                    int highPriorityLevel)
      throws IOException {
    return getProtocolEngine(protocol, conf)
        .getServer(protocol,
            instance,
            ifaces,
            bindAddress,
            port,
            numHandlers,
            metaHandlerCount,
            verbose,
            conf,
            highPriorityLevel);
  }

}
