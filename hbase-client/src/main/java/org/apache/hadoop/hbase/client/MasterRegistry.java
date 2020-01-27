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

import static org.apache.hadoop.hbase.HConstants.MASTER_ADDRS_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.MASTER_ADDRS_KEY;
import static org.apache.hadoop.hbase.HConstants.MASTER_REGISTRY_ENABLE_HEDGED_READS_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.MASTER_REGISTRY_ENABLE_HEDGED_READS_KEY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.MasterRegistryFetchException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetActiveMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetActiveMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaRegionLocationsResponse;

/**
 * Master based registry implementation. Makes RPCs to the configured master addresses from config
 * {@value org.apache.hadoop.hbase.HConstants#MASTER_ADDRS_KEY}.
 *
 * It supports hedged reads, which can be enabled by setting
 * {@value org.apache.hadoop.hbase.HConstants#MASTER_REGISTRY_ENABLE_HEDGED_READS_KEY} to True. Fan
 * out the requests batch is controlled by
 * {@value org.apache.hadoop.hbase.HConstants#HBASE_RPCS_HEDGED_REQS_FANOUT_KEY}.
 *
 * TODO: Handle changes to the configuration dynamically without having to restart the client.
 */
@InterfaceAudience.Private
public class MasterRegistry implements ConnectionRegistry {
  private static final String MASTER_ADDRS_CONF_SEPARATOR = ",";

  // Configured list of masters to probe the meta information from.
  private final Set<ServerName> masterServers;

  // RPC client used to talk to the masters.
  private final RpcClient rpcClient;
  private final RpcControllerFactory rpcControllerFactory;
  private final int rpcTimeoutMs;

  MasterRegistry(Configuration conf) {
    boolean hedgedReadsEnabled = conf.getBoolean(MASTER_REGISTRY_ENABLE_HEDGED_READS_KEY,
        MASTER_REGISTRY_ENABLE_HEDGED_READS_DEFAULT);
    Configuration finalConf;
    if (!hedgedReadsEnabled) {
      // If hedged reads are disabled, it is equivalent to setting a fan out of 1. We make a copy of
      // the configuration so that other places reusing this reference is not affected.
      finalConf = new Configuration(conf);
      finalConf.setInt(HConstants.HBASE_RPCS_HEDGED_REQS_FANOUT_KEY, 1);
    } else {
      finalConf = conf;
    }
    finalConf.set(MASTER_ADDRS_KEY, conf.get(MASTER_ADDRS_KEY, MASTER_ADDRS_DEFAULT));
    rpcTimeoutMs = (int) Math.min(Integer.MAX_VALUE, conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    masterServers = new HashSet<>();
    parseMasterAddrs(finalConf);
    rpcClient = RpcClientFactory.createClient(finalConf, HConstants.CLUSTER_ID_DEFAULT);
    rpcControllerFactory = RpcControllerFactory.instantiate(finalConf);
  }

  /**
   * @return Stub needed to make RPC using a hedged channel to the master end points.
   */
  private ClientMetaService.Interface getMasterStub() throws IOException {
    return ClientMetaService.newStub(
        rpcClient.createHedgedRpcChannel(masterServers, User.getCurrent(), rpcTimeoutMs));
  }

  /**
   * Parses the list of master addresses from the provided configuration. Supported format is
   * comma separated host[:port] values. If no port number if specified, default master port is
   * assumed.
   * @param conf Configuration to parse from.
   */
  private void parseMasterAddrs(Configuration conf) {
    String configuredMasters = conf.get(MASTER_ADDRS_KEY, MASTER_ADDRS_DEFAULT);
    for (String masterAddr: configuredMasters.split(MASTER_ADDRS_CONF_SEPARATOR)) {
      HostAndPort masterHostPort =
          HostAndPort.fromString(masterAddr.trim()).withDefaultPort(HConstants.DEFAULT_MASTER_PORT);
      masterServers.add(ServerName.valueOf(masterHostPort.toString(), ServerName.NON_STARTCODE));
    }
    Preconditions.checkArgument(!masterServers.isEmpty(), "At least one master address is needed");
  }

  @VisibleForTesting
  public Set<ServerName> getParsedMasterServers() {
    return Collections.unmodifiableSet(masterServers);
  }

  /**
   * Returns a call back that can be passed along to the non-blocking rpc call. It is invoked once
   * the rpc finishes and the response is propagated to the passed future.
   * @param future Result future to which the rpc response is propagated.
   * @param isValidResp Checks if the rpc response has a valid result.
   * @param transformResult Transforms the result to a different form as expected by callers.
   * @param hrc RpcController instance for this rpc.
   * @param debug Debug message passed along to the caller in case of exceptions.
   * @param <T> RPC result type.
   * @param <R> Transformed type of the result.
   * @return A call back that can be embedded in the non-blocking rpc call.
   */
  private <T, R> RpcCallback<T> getRpcCallBack(CompletableFuture<R> future,
      Predicate<T> isValidResp, Function<T, R> transformResult, HBaseRpcController hrc,
      final String debug) {
    return rpcResult -> {
      if (rpcResult == null) {
        future.completeExceptionally(
            new MasterRegistryFetchException(masterServers, hrc.getFailed()));
        return;
      }
      if (!isValidResp.test(rpcResult)) {
        // Rpc returned ok, but result was malformed.
        future.completeExceptionally(new IOException(
            String.format("Invalid result for request %s. Will be retried", debug)));
        return;
      }
      future.complete(transformResult.apply(rpcResult));
    };
  }

  /**
   * Simple helper to transform the result of getMetaRegionLocations() rpc.
   */
  private RegionLocations transformMetaRegionLocations(GetMetaRegionLocationsResponse resp) {
    List<HRegionLocation> regionLocations = new ArrayList<>();
    resp.getMetaLocationsList().forEach(
      location -> regionLocations.add(ProtobufUtil.toRegionLocation(location)));
    return new RegionLocations(regionLocations);
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    CompletableFuture<RegionLocations> result = new CompletableFuture<>();
    HBaseRpcController hrc = rpcControllerFactory.newController();
    RpcCallback<GetMetaRegionLocationsResponse> callback = getRpcCallBack(result,
      (rpcResp) -> rpcResp.getMetaLocationsCount() != 0, this::transformMetaRegionLocations, hrc,
        "getMetaRegionLocations()");
    try {
      getMasterStub().getMetaRegionLocations(
          hrc, GetMetaRegionLocationsRequest.getDefaultInstance(), callback);
    } catch (IOException e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    CompletableFuture<String> result = new CompletableFuture<>();
    HBaseRpcController hrc = rpcControllerFactory.newController();
    RpcCallback<GetClusterIdResponse> callback = getRpcCallBack(result,
        GetClusterIdResponse::hasClusterId,  GetClusterIdResponse::getClusterId, hrc,
        "getClusterId()");
    try {
      getMasterStub().getClusterId(hrc, GetClusterIdRequest.getDefaultInstance(), callback);
    } catch (IOException e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  private ServerName transformServerName(GetActiveMasterResponse resp) {
    return ProtobufUtil.toServerName(resp.getServerName());
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    CompletableFuture<ServerName> result = new CompletableFuture<>();
    HBaseRpcController hrc = rpcControllerFactory.newController();
    RpcCallback<GetActiveMasterResponse> callback = getRpcCallBack(result,
        GetActiveMasterResponse::hasServerName, this::transformServerName, hrc,
        "getActiveMaster()");
    try {
      getMasterStub().getActiveMaster(hrc, GetActiveMasterRequest.getDefaultInstance(), callback);
    } catch (IOException e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  @Override
  public void close() {
    if (rpcClient != null) {
      rpcClient.close();
    }
  }
}