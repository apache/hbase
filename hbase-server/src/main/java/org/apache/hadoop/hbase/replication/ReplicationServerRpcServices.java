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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.NoopAccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerProtos.ReplicationServerService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerProtos.StartReplicationSourceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerProtos.StartReplicationSourceResponse;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

/**
 * Implements the regionserver RPC services for {@link HReplicationServer}.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class ReplicationServerRpcServices implements HBaseRPCErrorHandler,
    ReplicationServerService.BlockingInterface, PriorityFunction {

  protected static final Logger LOG = LoggerFactory.getLogger(ReplicationServerRpcServices.class);

  /** Parameter name for port replication server listens on. */
  public static final String REPLICATION_SERVER_PORT = "hbase.replicationserver.port";

  /** Default port replication server listens on. */
  public static final int DEFAULT_REPLICATION_SERVER_PORT = 16040;

  /** default port for replication server web api */
  public static final int DEFAULT_REPLICATION_SERVER_INFOPORT = 16050;

  // Request counter.
  final LongAdder requestCount = new LongAdder();

  // Server to handle client requests.
  final RpcServerInterface rpcServer;
  final InetSocketAddress isa;

  protected final HReplicationServer replicationServer;

  // The reference to the priority extraction function
  private final PriorityFunction priority;

  private AccessChecker accessChecker;
  private ZKPermissionWatcher zkPermissionWatcher;

  public ReplicationServerRpcServices(final HReplicationServer rs) throws IOException {
    final Configuration conf = rs.getConfiguration();
    replicationServer = rs;

    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass().asSubclass(RpcSchedulerFactory.class)
          .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException |
        InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    // Server to handle client requests.
    final InetSocketAddress initialIsa;
    final InetSocketAddress bindAddress;

    String hostname = DNS.getHostname(conf, ServerType.REPLICATIONSERVER);
    int port = conf.getInt(REPLICATION_SERVER_PORT, DEFAULT_REPLICATION_SERVER_PORT);
    // Creation of a HSA will force a resolve.
    initialIsa = new InetSocketAddress(hostname, port);
    bindAddress = new InetSocketAddress(
        conf.get("hbase.replicationserver.ipc.address", hostname), port);

    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    priority = createPriority();
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = rs.getProcessName() + "/" +
        Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    rpcServer = createRpcServer(rs, rpcSchedulerFactory, bindAddress, name);

    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);
    rs.setName(name);
  }

  protected RpcServerInterface createRpcServer(
      final Server server,
      final RpcSchedulerFactory rpcSchedulerFactory,
      final InetSocketAddress bindAddress,
      final String name
  ) throws IOException {
    final Configuration conf = server.getConfiguration();
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      return RpcServerFactory.createRpcServer(server, name, getServices(),
          bindAddress, // use final bindAddress for this server.
          conf, rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '"
          + REPLICATION_SERVER_PORT + "' configuration property.",
          be.getCause() != null ? be.getCause() : be);
    }
  }

  protected Class<?> getRpcSchedulerFactoryClass() {
    final Configuration conf = replicationServer.getConfiguration();
    return conf.getClass(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  public PriorityFunction getPriority() {
    return priority;
  }

  public Configuration getConfiguration() {
    return replicationServer.getConfiguration();
  }

  void start(ZKWatcher zkWatcher) {
    if (AccessChecker.isAuthorizationSupported(getConfiguration())) {
      accessChecker = new AccessChecker(getConfiguration());
    } else {
      accessChecker = new NoopAccessChecker(getConfiguration());
    }
    if (!getConfiguration().getBoolean("hbase.testing.nocluster", false) && zkWatcher != null) {
      zkPermissionWatcher =
          new ZKPermissionWatcher(zkWatcher, accessChecker.getAuthManager(), getConfiguration());
      try {
        zkPermissionWatcher.start();
      } catch (KeeperException e) {
        LOG.error("ZooKeeper permission watcher initialization failed", e);
      }
    }
    rpcServer.start();
  }

  void stop() {
    if (zkPermissionWatcher != null) {
      zkPermissionWatcher.close();
    }
    rpcServer.stop();
  }

  /**
   * By default, put up an Admin Service.
   * @return immutable list of blocking services and the security info classes that this server
   *   supports
   */
  protected List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<>();
    bssi.add(new BlockingServiceAndInterface(
      ReplicationServerService.newReflectiveBlockingService(this),
        ReplicationServerService.BlockingInterface.class));
    return new ImmutableList.Builder<BlockingServiceAndInterface>().addAll(bssi).build();
  }

  public InetSocketAddress getSocketAddress() {
    return isa;
  }

  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    return priority.getPriority(header, param, user);
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    return priority.getDeadline(header, param);
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
          || (e.getMessage() != null && e.getMessage().contains(
              "java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.error(HBaseMarkers.FATAL, "Run out of memory; "
          + ReplicationServerRpcServices.class.getSimpleName() + " will abort itself immediately",
          e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }

  /**
   * Called to verify that this server is up and running.
   */
  protected void checkOpen() throws IOException {
    if (replicationServer.isAborted()) {
      throw new RegionServerAbortedException("Server " + replicationServer.getServerName()
          + " aborting");
    }
    if (replicationServer.isStopped()) {
      throw new RegionServerStoppedException("Server " + replicationServer.getServerName()
          + " stopping");
    }
    if (!replicationServer.isOnline()) {
      throw new ServerNotRunningYetException("Server " + replicationServer.getServerName()
          + " is not running yet");
    }
  }

  protected AccessChecker getAccessChecker() {
    return accessChecker;
  }

  protected PriorityFunction createPriority() {
    return new PriorityFunction() {
      @Override
      public int getPriority(RequestHeader header, Message param, User user) {
        return 0;
      }

      @Override
      public long getDeadline(RequestHeader header, Message param) {
        return 0;
      }
    };
  }

  @Override
  public ReplicateWALEntryResponse replicateWALEntry(RpcController controller,
      ReplicateWALEntryRequest request) throws ServiceException {
    try {
      checkOpen();
      if (replicationServer.getReplicationSinkService() != null) {
        requestCount.increment();
        List<WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();
        // TODO: CP pre
        replicationServer.getReplicationSinkService().replicateLogEntries(entries, cellScanner,
            request.getReplicationClusterId(), request.getSourceBaseNamespaceDirPath(),
            request.getSourceHFileArchiveDirPath());
        // TODO: CP post
        return ReplicateWALEntryResponse.newBuilder().build();
      } else {
        throw new ServiceException("Replication services are not initialized yet");
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public StartReplicationSourceResponse startReplicationSource(RpcController controller,
    StartReplicationSourceRequest request) throws ServiceException {
    try {
      replicationServer.startReplicationSource(ProtobufUtil.toServerName(request.getServerName()),
        request.getQueueId());
      return StartReplicationSourceResponse.newBuilder().build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }
}
