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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AbstractRpcServices;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

@InterfaceAudience.Private
public class CSRpcServices extends AbstractRpcServices {
  protected static final Logger LOG = LoggerFactory.getLogger(CSRpcServices.class);

  protected final HCompactionServer compactionServer;
  /** RPC scheduler to use for the compaction server. */
  public static final String COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
      "hbase.compaction.server.rpc.scheduler.factory.class";

  /**
   * @return immutable list of blocking services and the security info classes that this server
   *         supports
   */
  protected List<RpcServer.BlockingServiceAndInterface> getServices() {
    // now return empty, compaction server do not receive rpc request
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>();
    return new ImmutableList.Builder<RpcServer.BlockingServiceAndInterface>().addAll(bssi).build();
  }

  protected RpcServerInterface createRpcServer(final Server server,
      final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
      final String name) throws IOException {
    final Configuration conf = server.getConfiguration();
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      // use final bindAddress for this server.
      return RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, conf,
        rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '"
          + HConstants.COMPACTION_SERVER_PORT + "' configuration property.",
          be.getCause() != null ? be.getCause() : be);
    }
  }

  void start() {
    rpcServer.start();
  }

  protected Class<?> getRpcSchedulerFactoryClass() {
    final Configuration conf = compactionServer.getConfiguration();
    return conf.getClass(COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  public CSRpcServices(final HCompactionServer cs) throws IOException {
    final Configuration conf = cs.getConfiguration();
    compactionServer = cs;
    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass().asSubclass(RpcSchedulerFactory.class)
          .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
        | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    // Server to handle client requests.
    final InetSocketAddress initialIsa;
    final InetSocketAddress bindAddress;

    String hostname = DNS.getHostname(conf, DNS.ServerType.COMPACTIONSERVER);
    int port =
        conf.getInt(HConstants.COMPACTION_SERVER_PORT, HConstants.DEFAULT_COMPACTION_SERVER_PORT);
    // Creation of a HSA will force a resolve.
    initialIsa = new InetSocketAddress(hostname, port);
    bindAddress =
        new InetSocketAddress(conf.get("hbase.compaction.server.ipc.address", hostname), port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = cs.getProcessName() + "/"
        + Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    rpcServer = createRpcServer(cs, rpcSchedulerFactory, bindAddress, name);
    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);
  }

}
