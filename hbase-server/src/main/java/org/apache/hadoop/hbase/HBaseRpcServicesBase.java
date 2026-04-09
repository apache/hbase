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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.ClientMetaCoprocessorHost;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.RpcLogDetails;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.NoopAccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.OOMEChecker;
import org.apache.hadoop.hbase.util.ReservoirSample;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponseEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaTableNameRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaTableNameResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload;

/**
 * Base class for Master and RegionServer RpcServices.
 */
@InterfaceAudience.Private
public abstract class HBaseRpcServicesBase<S extends HBaseServerBase<?>>
  implements ClientMetaService.BlockingInterface, AdminService.BlockingInterface,
  HBaseRPCErrorHandler, PriorityFunction, ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseRpcServicesBase.class);

  public static final String CLIENT_BOOTSTRAP_NODE_LIMIT = "hbase.client.bootstrap.node.limit";

  public static final int DEFAULT_CLIENT_BOOTSTRAP_NODE_LIMIT = 10;

  protected final S server;

  // Server to handle client requests.
  protected final RpcServer rpcServer;

  private final InetSocketAddress isa;

  protected final PriorityFunction priority;

  private ClientMetaCoprocessorHost clientMetaCoprocessorHost;

  private AccessChecker accessChecker;

  private ZKPermissionWatcher zkPermissionWatcher;

  protected HBaseRpcServicesBase(S server, String processName) throws IOException {
    this.server = server;
    Configuration conf = server.getConfiguration();
    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass(conf).asSubclass(RpcSchedulerFactory.class)
        .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
      | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    String hostname = DNS.getHostname(conf, getDNSServerType());
    int port = conf.getInt(getPortConfigName(), getDefaultPort());
    // Creation of a HSA will force a resolve.
    final InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    final InetSocketAddress bindAddress = new InetSocketAddress(getHostname(conf, hostname), port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    priority = createPriority();
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = processName + "/"
      + Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    server.setName(name);
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    boolean reservoirEnabled =
      conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, defaultReservoirEnabled());
    try {
      // use final bindAddress for this server.
      rpcServer = RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, conf,
        rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '" + getPortConfigName()
        + "' configuration property.", be.getCause() != null ? be.getCause() : be);
    }
    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);

    clientMetaCoprocessorHost = new ClientMetaCoprocessorHost(conf);
  }

  protected abstract boolean defaultReservoirEnabled();

  protected abstract DNS.ServerType getDNSServerType();

  protected abstract String getHostname(Configuration conf, String defaultHostname);

  protected abstract String getPortConfigName();

  protected abstract int getDefaultPort();

  protected abstract PriorityFunction createPriority();

  protected abstract Class<?> getRpcSchedulerFactoryClass(Configuration conf);

  protected abstract List<BlockingServiceAndInterface> getServices();

  protected final void internalStart(ZKWatcher zkWatcher) {
    if (AccessChecker.isAuthorizationSupported(getConfiguration())) {
      accessChecker = new AccessChecker(getConfiguration());
    } else {
      accessChecker = new NoopAccessChecker(getConfiguration());
    }
    zkPermissionWatcher =
      new ZKPermissionWatcher(zkWatcher, accessChecker.getAuthManager(), getConfiguration());
    try {
      zkPermissionWatcher.start();
    } catch (KeeperException e) {
      LOG.error("ZooKeeper permission watcher initialization failed", e);
    }
    rpcServer.start();
  }

  protected final void requirePermission(String request, Permission.Action perm)
    throws IOException {
    if (accessChecker != null) {
      accessChecker.requirePermission(RpcServer.getRequestUser().orElse(null), request, null, perm);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public ClientMetaCoprocessorHost getClientMetaCoprocessorHost() {
    return clientMetaCoprocessorHost;
  }

  public AccessChecker getAccessChecker() {
    return accessChecker;
  }

  public ZKPermissionWatcher getZkPermissionWatcher() {
    return zkPermissionWatcher;
  }

  protected final void internalStop() {
    if (zkPermissionWatcher != null) {
      zkPermissionWatcher.close();
    }
    rpcServer.stop();
  }

  public Configuration getConfiguration() {
    return server.getConfiguration();
  }

  public S getServer() {
    return server;
  }

  public InetSocketAddress getSocketAddress() {
    return isa;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  public RpcScheduler getRpcScheduler() {
    return rpcServer.getScheduler();
  }

  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    return priority.getPriority(header, param, user);
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    return priority.getDeadline(header, param);
  }

  /**
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   * @return True if we OOME'd and are aborting.
   */
  @Override
  public boolean checkOOME(Throwable e) {
    return OOMEChecker.exitIfOOME(e, getClass().getSimpleName());
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    rpcServer.onConfigurationChange(conf);
  }

  @Override
  public GetClusterIdResponse getClusterId(RpcController controller, GetClusterIdRequest request)
    throws ServiceException {
    try {
      clientMetaCoprocessorHost.preGetClusterId();

      String clusterId = server.getClusterId();
      String clusterIdReply = clientMetaCoprocessorHost.postGetClusterId(clusterId);

      return GetClusterIdResponse.newBuilder().setClusterId(clusterIdReply).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetActiveMasterResponse getActiveMaster(RpcController controller,
    GetActiveMasterRequest request) throws ServiceException {
    GetActiveMasterResponse.Builder builder = GetActiveMasterResponse.newBuilder();

    try {
      clientMetaCoprocessorHost.preGetActiveMaster();

      ServerName serverName = server.getActiveMaster().orElse(null);
      ServerName serverNameReply = clientMetaCoprocessorHost.postGetActiveMaster(serverName);

      if (serverNameReply != null) {
        builder.setServerName(ProtobufUtil.toServerName(serverNameReply));
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }

  @Override
  public GetMastersResponse getMasters(RpcController controller, GetMastersRequest request)
    throws ServiceException {
    GetMastersResponse.Builder builder = GetMastersResponse.newBuilder();

    try {
      clientMetaCoprocessorHost.preGetMasters();

      Map<ServerName, Boolean> serverNames = new LinkedHashMap<>();

      server.getActiveMaster().ifPresent(serverName -> serverNames.put(serverName, Boolean.TRUE));
      server.getBackupMasters().forEach(serverName -> serverNames.put(serverName, Boolean.FALSE));

      Map<ServerName, Boolean> serverNamesReply =
        clientMetaCoprocessorHost.postGetMasters(serverNames);

      serverNamesReply
        .forEach((serverName, active) -> builder.addMasterServers(GetMastersResponseEntry
          .newBuilder().setServerName(ProtobufUtil.toServerName(serverName)).setIsActive(active)));
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }

  @Override
  public GetMetaRegionLocationsResponse getMetaRegionLocations(RpcController controller,
    GetMetaRegionLocationsRequest request) throws ServiceException {
    GetMetaRegionLocationsResponse.Builder builder = GetMetaRegionLocationsResponse.newBuilder();

    try {
      clientMetaCoprocessorHost.preGetMetaLocations();

      List<HRegionLocation> metaLocations = server.getMetaLocations();
      List<HRegionLocation> metaLocationsReply =
        clientMetaCoprocessorHost.postGetMetaLocations(metaLocations);

      metaLocationsReply
        .forEach(location -> builder.addMetaLocations(ProtobufUtil.toRegionLocation(location)));
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }

  @Override
  public final GetBootstrapNodesResponse getBootstrapNodes(RpcController controller,
    GetBootstrapNodesRequest request) throws ServiceException {
    GetBootstrapNodesResponse.Builder builder = GetBootstrapNodesResponse.newBuilder();

    try {
      clientMetaCoprocessorHost.preGetBootstrapNodes();

      int maxNodeCount = server.getConfiguration().getInt(CLIENT_BOOTSTRAP_NODE_LIMIT,
        DEFAULT_CLIENT_BOOTSTRAP_NODE_LIMIT);
      ReservoirSample<ServerName> sample = new ReservoirSample<>(maxNodeCount);
      sample.add(server.getBootstrapNodes());

      List<ServerName> bootstrapNodes = sample.getSamplingResult();
      List<ServerName> bootstrapNodesReply =
        clientMetaCoprocessorHost.postGetBootstrapNodes(bootstrapNodes);

      bootstrapNodesReply
        .forEach(serverName -> builder.addServerName(ProtobufUtil.toServerName(serverName)));
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }

  @Override
  public final GetMetaTableNameResponse getMetaTableName(RpcController controller,
    GetMetaTableNameRequest request) throws ServiceException {
    GetMetaTableNameResponse.Builder builder = GetMetaTableNameResponse.newBuilder();

    try {
      TableName metaTableName = server.getMetaTableName();
      if (metaTableName != null) {
        builder.setTableName(metaTableName.getNameAsString());
      }
    } catch (Exception e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public UpdateConfigurationResponse updateConfiguration(RpcController controller,
    UpdateConfigurationRequest request) throws ServiceException {
    try {
      requirePermission("updateConfiguration", Permission.Action.ADMIN);
      this.server.updateConfiguration();

      clientMetaCoprocessorHost = new ClientMetaCoprocessorHost(getConfiguration());
    } catch (Exception e) {
      throw new ServiceException(e);
    }
    return UpdateConfigurationResponse.getDefaultInstance();
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public ClearSlowLogResponses clearSlowLogsResponses(final RpcController controller,
    final ClearSlowLogResponseRequest request) throws ServiceException {
    try {
      requirePermission("clearSlowLogsResponses", Permission.Action.ADMIN);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    final NamedQueueRecorder namedQueueRecorder = this.server.getNamedQueueRecorder();
    boolean slowLogsCleaned = Optional.ofNullable(namedQueueRecorder)
      .map(
        queueRecorder -> queueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.SLOW_LOG))
      .orElse(false);
    ClearSlowLogResponses clearSlowLogResponses =
      ClearSlowLogResponses.newBuilder().setIsCleaned(slowLogsCleaned).build();
    return clearSlowLogResponses;
  }

  private List<SlowLogPayload> getSlowLogPayloads(SlowLogResponseRequest request,
    NamedQueueRecorder namedQueueRecorder) {
    if (namedQueueRecorder == null) {
      return Collections.emptyList();
    }
    List<SlowLogPayload> slowLogPayloads;
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    namedQueueGetRequest.setSlowLogResponseRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    slowLogPayloads = namedQueueGetResponse != null
      ? namedQueueGetResponse.getSlowLogPayloads()
      : Collections.emptyList();
    return slowLogPayloads;
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public HBaseProtos.LogEntry getLogEntries(RpcController controller,
    HBaseProtos.LogRequest request) throws ServiceException {
    try {
      final String logClassName = request.getLogClassName();
      Class<?> logClass = Class.forName(logClassName).asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("SlowLogResponseRequest")) {
        SlowLogResponseRequest slowLogResponseRequest =
          (SlowLogResponseRequest) method.invoke(null, request.getLogMessage());
        final NamedQueueRecorder namedQueueRecorder = this.server.getNamedQueueRecorder();
        final List<SlowLogPayload> slowLogPayloads =
          getSlowLogPayloads(slowLogResponseRequest, namedQueueRecorder);
        SlowLogResponses slowLogResponses =
          SlowLogResponses.newBuilder().addAllSlowLogPayloads(slowLogPayloads).build();
        return HBaseProtos.LogEntry.newBuilder()
          .setLogClassName(slowLogResponses.getClass().getName())
          .setLogMessage(slowLogResponses.toByteString()).build();
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
      | InvocationTargetException e) {
      LOG.error("Error while retrieving log entries.", e);
      throw new ServiceException(e);
    }
    throw new ServiceException("Invalid request params");
  }
}
