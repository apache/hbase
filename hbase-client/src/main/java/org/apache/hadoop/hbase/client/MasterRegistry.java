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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.MASTER_ADDRS_KEY;
import static org.apache.hadoop.hbase.util.DNS.getHostname;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.MasterRegistryFetchException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMastersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMastersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMastersResponseEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaRegionLocationsResponse;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Master based registry implementation. Makes RPCs to the configured master addresses from config
 * {@value org.apache.hadoop.hbase.HConstants#MASTER_ADDRS_KEY}.
 * <p/>
 * It supports hedged reads, set the fan out of the requests batch by
 * {@link #MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY} to a value greater than {@code 1} will enable
 * it(the default value is {@link #MASTER_REGISTRY_HEDGED_REQS_FANOUT_DEFAULT}).
 * <p/>
 * TODO: Handle changes to the configuration dynamically without having to restart the client.
 */
@InterfaceAudience.Private
public class MasterRegistry implements ConnectionRegistry {

  /** Configuration key that controls the fan out of requests **/
  public static final String MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY =
    "hbase.client.master_registry.hedged.fanout";

  /** Default value for the fan out of hedged requests. **/
  public static final int MASTER_REGISTRY_HEDGED_REQS_FANOUT_DEFAULT = 2;

  private static final String MASTER_ADDRS_CONF_SEPARATOR = ",";

  private final int hedgedReadFanOut;

  // Configured list of masters to probe the meta information from.
  private volatile ImmutableMap<ServerName, ClientMetaService.Interface> masterAddr2Stub;

  // RPC client used to talk to the masters.
  private final RpcClient rpcClient;
  private final RpcControllerFactory rpcControllerFactory;
  private final int rpcTimeoutMs;

  protected final MasterAddressRefresher masterAddressRefresher;

  /**
   * Parses the list of master addresses from the provided configuration. Supported format is comma
   * separated host[:port] values. If no port number if specified, default master port is assumed.
   * @param conf Configuration to parse from.
   */
  private static Set<ServerName> parseMasterAddrs(Configuration conf) throws UnknownHostException {
    Set<ServerName> masterAddrs = new HashSet<>();
    String configuredMasters = getMasterAddr(conf);
    for (String masterAddr : configuredMasters.split(MASTER_ADDRS_CONF_SEPARATOR)) {
      HostAndPort masterHostPort =
        HostAndPort.fromString(masterAddr.trim()).withDefaultPort(HConstants.DEFAULT_MASTER_PORT);
      masterAddrs.add(ServerName.valueOf(masterHostPort.toString(), ServerName.NON_STARTCODE));
    }
    Preconditions.checkArgument(!masterAddrs.isEmpty(), "At least one master address is needed");
    return masterAddrs;
  }

  MasterRegistry(Configuration conf) throws IOException {
    this.hedgedReadFanOut = Math.max(1, conf.getInt(MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY,
      MASTER_REGISTRY_HEDGED_REQS_FANOUT_DEFAULT));
    rpcTimeoutMs = (int) Math.min(Integer.MAX_VALUE,
      conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    // XXX: we pass cluster id as null here since we do not have a cluster id yet, we have to fetch
    // this through the master registry...
    // This is a problem as we will use the cluster id to determine the authentication method
    rpcClient = RpcClientFactory.createClient(conf, null);
    rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    // Generate the seed list of master stubs. Subsequent RPCs try to keep a live list of masters
    // by fetching the end points from this list.
    populateMasterStubs(parseMasterAddrs(conf));
    masterAddressRefresher = new MasterAddressRefresher(conf, this);
  }

  void populateMasterStubs(Set<ServerName> masters) throws IOException {
    Preconditions.checkNotNull(masters);
    ImmutableMap.Builder<ServerName, ClientMetaService.Interface> builder =
        ImmutableMap.builderWithExpectedSize(masters.size());
    User user = User.getCurrent();
    for (ServerName masterAddr : masters) {
      builder.put(masterAddr,
          ClientMetaService.newStub(rpcClient.createRpcChannel(masterAddr, user, rpcTimeoutMs)));
    }
    masterAddr2Stub = builder.build();
  }

  /**
   * Builds the default master address end point if it is not specified in the configuration.
   * <p/>
   * Will be called in {@code HBaseTestingUtility}.
   */
  public static String getMasterAddr(Configuration conf) throws UnknownHostException {
    String masterAddrFromConf = conf.get(MASTER_ADDRS_KEY);
    if (!Strings.isNullOrEmpty(masterAddrFromConf)) {
      return masterAddrFromConf;
    }
    String hostname = getHostname(conf, ServerType.MASTER);
    int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
    return String.format("%s:%d", hostname, port);
  }

  /**
   * For describing the actual asynchronous rpc call.
   * <p/>
   * Typically, you can use lambda expression to implement this interface as
   *
   * <pre>
   * (c, s, d) -> s.xxx(c, your request here, d)
   * </pre>
   */
  @FunctionalInterface
  private interface Callable<T> {
    void call(HBaseRpcController controller, ClientMetaService.Interface stub, RpcCallback<T> done);
  }

  private <T extends Message> CompletableFuture<T> call(ClientMetaService.Interface stub,
    Callable<T> callable) {
    HBaseRpcController controller = rpcControllerFactory.newController();
    CompletableFuture<T> future = new CompletableFuture<>();
    callable.call(controller, stub, resp -> {
      if (controller.failed()) {
        IOException failureReason = controller.getFailed();
        future.completeExceptionally(failureReason);
        if (ClientExceptionsUtil.isConnectionException(failureReason)) {
          // RPC has failed, trigger a refresh of master end points. We can have some spurious
          // refreshes, but that is okay since the RPC is not expensive and not in a hot path.
          masterAddressRefresher.refreshNow();
        }
      } else {
        future.complete(resp);
      }
    });
    return future;
  }

  private IOException badResponse(String debug) {
    return new IOException(String.format("Invalid result for request %s. Will be retried", debug));
  }

  /**
   * send requests concurrently to hedgedReadsFanout masters. If any of the request is succeeded, we
   * will complete the future and quit. If all the requests in one round are failed, we will start
   * another round to send requests concurrently tohedgedReadsFanout masters. If all masters have
   * been tried and all of them are failed, we will fail the future.
   */
  private <T extends Message> void groupCall(CompletableFuture<T> future,
      Set<ServerName> masterServers, List<ClientMetaService.Interface> masterStubs,
      int startIndexInclusive, Callable<T> callable, Predicate<T> isValidResp, String debug,
      ConcurrentLinkedQueue<Throwable> errors) {
    int endIndexExclusive = Math.min(startIndexInclusive + hedgedReadFanOut, masterStubs.size());
    AtomicInteger remaining = new AtomicInteger(endIndexExclusive - startIndexInclusive);
    for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
      addListener(call(masterStubs.get(i), callable), (r, e) -> {
        // a simple check to skip all the later operations earlier
        if (future.isDone()) {
          return;
        }
        if (e == null && !isValidResp.test(r)) {
          e = badResponse(debug);
        }
        if (e != null) {
          // make sure when remaining reaches 0 we have all exceptions in the errors queue
          errors.add(e);
          if (remaining.decrementAndGet() == 0) {
            if (endIndexExclusive == masterStubs.size()) {
              // we are done, complete the future with exception
              RetriesExhaustedException ex = new RetriesExhaustedException("masters",
                masterStubs.size(), new ArrayList<>(errors));
              future.completeExceptionally(
                new MasterRegistryFetchException(masterServers, ex));
            } else {
              groupCall(future, masterServers, masterStubs, endIndexExclusive, callable,
                  isValidResp, debug, errors);
            }
          }
        } else {
          // do not need to decrement the counter any more as we have already finished the future.
          future.complete(r);
        }
      });
    }
  }

  private <T extends Message> CompletableFuture<T> call(Callable<T> callable,
    Predicate<T> isValidResp, String debug) {
    ImmutableMap<ServerName, ClientMetaService.Interface> masterAddr2StubRef = masterAddr2Stub;
    Set<ServerName> masterServers = masterAddr2StubRef.keySet();
    List<ClientMetaService.Interface> masterStubs = new ArrayList<>(masterAddr2StubRef.values());
    Collections.shuffle(masterStubs, ThreadLocalRandom.current());
    CompletableFuture<T> future = new CompletableFuture<>();
    groupCall(future, masterServers, masterStubs, 0, callable, isValidResp, debug,
        new ConcurrentLinkedQueue<>());
    return future;
  }

  /**
   * Simple helper to transform the result of getMetaRegionLocations() rpc.
   */
  private static RegionLocations transformMetaRegionLocations(GetMetaRegionLocationsResponse resp) {
    List<HRegionLocation> regionLocations = new ArrayList<>();
    resp.getMetaLocationsList()
      .forEach(location -> regionLocations.add(ProtobufUtil.toRegionLocation(location)));
    return new RegionLocations(regionLocations);
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    return this.<GetMetaRegionLocationsResponse> call((c, s, d) -> s.getMetaRegionLocations(c,
      GetMetaRegionLocationsRequest.getDefaultInstance(), d), r -> r.getMetaLocationsCount() != 0,
      "getMetaLocationsCount").thenApply(MasterRegistry::transformMetaRegionLocations);
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return this
      .<GetClusterIdResponse> call(
        (c, s, d) -> s.getClusterId(c, GetClusterIdRequest.getDefaultInstance(), d),
        GetClusterIdResponse::hasClusterId, "getClusterId()")
      .thenApply(GetClusterIdResponse::getClusterId);
  }

  private static boolean hasActiveMaster(GetMastersResponse resp) {
    List<GetMastersResponseEntry> activeMasters =
        resp.getMasterServersList().stream().filter(GetMastersResponseEntry::getIsActive).collect(
        Collectors.toList());
    return activeMasters.size() == 1;
  }

  private static ServerName filterActiveMaster(GetMastersResponse resp) throws IOException {
    List<GetMastersResponseEntry> activeMasters =
        resp.getMasterServersList().stream().filter(GetMastersResponseEntry::getIsActive).collect(
            Collectors.toList());
    if (activeMasters.size() != 1) {
      throw new IOException(String.format("Incorrect number of active masters encountered." +
          " Expected: 1 found: %d. Content: %s", activeMasters.size(), activeMasters));
    }
    return ProtobufUtil.toServerName(activeMasters.get(0).getServerName());
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    CompletableFuture<ServerName> future = new CompletableFuture<>();
    addListener(call((c, s, d) -> s.getMasters(c, GetMastersRequest.getDefaultInstance(), d),
      MasterRegistry::hasActiveMaster, "getMasters()"), (resp, ex) -> {
        if (ex != null) {
          future.completeExceptionally(ex);
        }
        ServerName result = null;
        try {
          result = filterActiveMaster((GetMastersResponse)resp);
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        future.complete(result);
      });
    return future;
  }

  private static List<ServerName> transformServerNames(GetMastersResponse resp) {
    return resp.getMasterServersList().stream().map(s -> ProtobufUtil.toServerName(
        s.getServerName())).collect(Collectors.toList());
  }

  CompletableFuture<List<ServerName>> getMasters() {
    return this
        .<GetMastersResponse> call((c, s, d) -> s.getMasters(
            c, GetMastersRequest.getDefaultInstance(), d), r -> r.getMasterServersCount() != 0,
            "getMasters()").thenApply(MasterRegistry::transformServerNames);
  }

  Set<ServerName> getParsedMasterServers() {
    return masterAddr2Stub.keySet();
  }

  @Override
  public void close() {
    if (masterAddressRefresher != null) {
      masterAddressRefresher.close();
    }
    if (rpcClient != null) {
      rpcClient.close();
    }
  }
}
