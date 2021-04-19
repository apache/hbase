/*
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@InterfaceAudience.Private
public abstract class AbstractRpcServices
    implements HBaseRPCErrorHandler, PriorityFunction, ConfigurationObserver {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractRpcServices.class);
  protected RpcServerInterface rpcServer;
  protected InetSocketAddress isa;
  @Override
  public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
    return 0;
  }

  @Override
  public long getDeadline(RPCProtos.RequestHeader header, Message param) {
    return 0;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getIsa() {
    return isa;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    if (rpcServer instanceof ConfigurationObserver) {
      ((ConfigurationObserver)rpcServer).onConfigurationChange(newConf);
    }
  }

  /*
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   *
   * @param e
   *
   * @return True if we OOME'd and are aborting.
   */
  @Override
  public boolean checkOOME(final Throwable e) {
    return exitIfOOME(e);
  }

  public static boolean exitIfOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains("java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.error(HBaseMarkers.FATAL, "Run out of memory; "
            + AbstractRpcServices.class.getSimpleName() + " will abort itself immediately",
          e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }
  protected void stop(){
    rpcServer.stop();
  }

  protected abstract Class<?> getRpcSchedulerFactoryClass(final Configuration conf);

  protected abstract List<RpcServer.BlockingServiceAndInterface>
      getServices(final Configuration conf);

  protected RpcServerInterface createRpcServer(final Server server,
      final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
      final String name) throws IOException {
    final Configuration conf = server.getConfiguration();
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      // use final bindAddress for this server.
      return RpcServerFactory.createRpcServer(server, name, getServices(conf), bindAddress, conf,
        rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(
          be.getMessage() + ". To switch ports use the hbase.server.port configuration property.",
          be.getCause() != null ? be.getCause() : be);
    }
  }

  protected AbstractRpcServices(AbstractServer abstractServer) throws IOException {
    final Configuration conf = abstractServer.getConfiguration();
    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass(conf).asSubclass(RpcSchedulerFactory.class)
          .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
        | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    // Server to handle client requests.
    final InetSocketAddress initialIsa;
    final InetSocketAddress bindAddress;

    if (this instanceof MasterRpcServices) {
      String hostname = DNS.getHostname(conf, DNS.ServerType.MASTER);
      int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
      // Creation of a HSA will force a resolve.
      initialIsa = new InetSocketAddress(hostname, port);
      bindAddress = new InetSocketAddress(conf.get("hbase.master.ipc.address", hostname), port);
    } else if (this instanceof RSRpcServices) {
      String hostname = DNS.getHostname(conf, DNS.ServerType.REGIONSERVER);
      int port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);
      // Creation of a HSA will force a resolve.
      initialIsa = new InetSocketAddress(hostname, port);
      bindAddress =
          new InetSocketAddress(conf.get("hbase.regionserver.ipc.address", hostname), port);
    } else {
      String hostname = DNS.getHostname(conf, DNS.ServerType.COMPACTIONSERVER);
      int port =
          conf.getInt(HConstants.COMPACTION_SERVER_PORT, HConstants.DEFAULT_COMPACTION_SERVER_PORT);
      // Creation of a HSA will force a resolve.
      initialIsa = new InetSocketAddress(hostname, port);
      bindAddress =
          new InetSocketAddress(conf.get("hbase.compaction.server.ipc.address", hostname), port);
    }
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = abstractServer.getProcessName() + "/"
        + Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    rpcServer = createRpcServer(abstractServer, rpcSchedulerFactory, bindAddress, name);
    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);
    abstractServer.setName(name);
  }
}
