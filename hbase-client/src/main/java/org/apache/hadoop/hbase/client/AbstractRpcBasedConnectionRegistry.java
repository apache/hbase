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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.trace.TraceUtil.trace;
import static org.apache.hadoop.hbase.trace.TraceUtil.tracedFuture;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
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
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsResponse;

/**
 * Base class for rpc based connection registry implementation.
 * <p/>
 * The implementation needs a bootstrap node list in configuration, and then it will use the methods
 * in {@link ClientMetaService} to refresh the connection registry end points.
 * <p/>
 * It also supports hedged reads, the default fan out value is 2.
 * <p/>
 * For the actual configuration names, see javadoc of sub classes.
 */
@InterfaceAudience.Private
abstract class AbstractRpcBasedConnectionRegistry implements ConnectionRegistry {

  /** Default value for the fan out of hedged requests. **/
  public static final int HEDGED_REQS_FANOUT_DEFAULT = 2;

  private final int hedgedReadFanOut;

  // Configured list of end points to probe the meta information from.
  private volatile ImmutableMap<ServerName, ClientMetaService.Interface> addr2Stub;

  // RPC client used to talk to the masters.
  private final RpcClient rpcClient;
  private final RpcControllerFactory rpcControllerFactory;
  private final int rpcTimeoutMs;

  private final RegistryEndpointsRefresher registryEndpointRefresher;

  protected AbstractRpcBasedConnectionRegistry(Configuration conf,
    String hedgedReqsFanoutConfigName, String initialRefreshDelaySecsConfigName,
    String refreshIntervalSecsConfigName, String minRefreshIntervalSecsConfigName)
    throws IOException {
    this.hedgedReadFanOut =
      Math.max(1, conf.getInt(hedgedReqsFanoutConfigName, HEDGED_REQS_FANOUT_DEFAULT));
    rpcTimeoutMs = (int) Math.min(Integer.MAX_VALUE,
      conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    // XXX: we pass cluster id as null here since we do not have a cluster id yet, we have to fetch
    // this through the master registry...
    // This is a problem as we will use the cluster id to determine the authentication method
    rpcClient = RpcClientFactory.createClient(conf, null);
    rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    populateStubs(getBootstrapNodes(conf));
    // could return null here is refresh interval is less than zero
    registryEndpointRefresher =
      RegistryEndpointsRefresher.create(conf, initialRefreshDelaySecsConfigName,
        refreshIntervalSecsConfigName, minRefreshIntervalSecsConfigName, this::refreshStubs);
  }

  protected abstract Set<ServerName> getBootstrapNodes(Configuration conf) throws IOException;

  protected abstract CompletableFuture<Set<ServerName>> fetchEndpoints();

  private void refreshStubs() throws IOException {
    populateStubs(FutureUtils.get(fetchEndpoints()));
  }

  private void populateStubs(Set<ServerName> addrs) throws IOException {
    Preconditions.checkNotNull(addrs);
    ImmutableMap.Builder<ServerName, ClientMetaService.Interface> builder =
      ImmutableMap.builderWithExpectedSize(addrs.size());
    User user = User.getCurrent();
    for (ServerName masterAddr : addrs) {
      builder.put(masterAddr,
        ClientMetaService.newStub(rpcClient.createRpcChannel(masterAddr, user, rpcTimeoutMs)));
    }
    addr2Stub = builder.build();
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
  protected interface Callable<T> {
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
          // RPC has failed, trigger a refresh of end points. We can have some spurious
          // refreshes, but that is okay since the RPC is not expensive and not in a hot path.
          registryEndpointRefresher.refreshNow();
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
   * send requests concurrently to hedgedReadsFanout end points. If any of the request is succeeded,
   * we will complete the future and quit. If all the requests in one round are failed, we will
   * start another round to send requests concurrently tohedgedReadsFanout end points. If all end
   * points have been tried and all of them are failed, we will fail the future.
   */
  private <T extends Message> void groupCall(CompletableFuture<T> future, Set<ServerName> servers,
    List<ClientMetaService.Interface> stubs, int startIndexInclusive, Callable<T> callable,
    Predicate<T> isValidResp, String debug, ConcurrentLinkedQueue<Throwable> errors) {
    int endIndexExclusive = Math.min(startIndexInclusive + hedgedReadFanOut, stubs.size());
    AtomicInteger remaining = new AtomicInteger(endIndexExclusive - startIndexInclusive);
    for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
      addListener(call(stubs.get(i), callable), (r, e) -> {
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
            if (endIndexExclusive == stubs.size()) {
              // we are done, complete the future with exception
              RetriesExhaustedException ex =
                new RetriesExhaustedException("masters", stubs.size(), new ArrayList<>(errors));
              future.completeExceptionally(new MasterRegistryFetchException(servers, ex));
            } else {
              groupCall(future, servers, stubs, endIndexExclusive, callable, isValidResp, debug,
                errors);
            }
          }
        } else {
          // do not need to decrement the counter any more as we have already finished the future.
          future.complete(r);
        }
      });
    }
  }

  protected final <T extends Message> CompletableFuture<T> call(Callable<T> callable,
    Predicate<T> isValidResp, String debug) {
    ImmutableMap<ServerName, ClientMetaService.Interface> addr2StubRef = addr2Stub;
    Set<ServerName> servers = addr2StubRef.keySet();
    List<ClientMetaService.Interface> stubs = new ArrayList<>(addr2StubRef.values());
    Collections.shuffle(stubs, ThreadLocalRandom.current());
    CompletableFuture<T> future = new CompletableFuture<>();
    groupCall(future, servers, stubs, 0, callable, isValidResp, debug,
      new ConcurrentLinkedQueue<>());
    return future;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  Set<ServerName> getParsedServers() {
    return addr2Stub.keySet();
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
    return tracedFuture(
      () -> this
        .<GetMetaRegionLocationsResponse> call(
          (c, s, d) -> s.getMetaRegionLocations(c,
            GetMetaRegionLocationsRequest.getDefaultInstance(), d),
          r -> r.getMetaLocationsCount() != 0, "getMetaLocationsCount")
        .thenApply(AbstractRpcBasedConnectionRegistry::transformMetaRegionLocations),
      getClass().getSimpleName() + ".getMetaRegionLocations");
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return tracedFuture(
      () -> this
        .<GetClusterIdResponse> call(
          (c, s, d) -> s.getClusterId(c, GetClusterIdRequest.getDefaultInstance(), d),
          GetClusterIdResponse::hasClusterId, "getClusterId()")
        .thenApply(GetClusterIdResponse::getClusterId),
      getClass().getSimpleName() + ".getClusterId");
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    return tracedFuture(
      () -> this
        .<GetActiveMasterResponse> call(
          (c, s, d) -> s.getActiveMaster(c, GetActiveMasterRequest.getDefaultInstance(), d),
          GetActiveMasterResponse::hasServerName, "getActiveMaster()")
        .thenApply(resp -> ProtobufUtil.toServerName(resp.getServerName())),
      getClass().getSimpleName() + ".getClusterId");
  }

  @Override
  public void close() {
    trace(() -> {
      if (registryEndpointRefresher != null) {
        registryEndpointRefresher.stop();
      }
      if (rpcClient != null) {
        rpcClient.close();
      }
    }, getClass().getSimpleName() + ".close");
  }
}
