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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaRegionsNotAvailableException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaLocationsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsActiveRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsActiveResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the meta information directly from HMaster by making relevant RPCs. HMaster RPC end
 * points are looked up via client configuration 'hbase.client.asyncregistry.masteraddrs' as
 * comma separated list of host:port values. Should be set in conjunction with
 * 'hbase.client.registry.impl' set to this class impl.
 *
 * The class does not cache anything. It is the responsibility of the callers to cache and
 * avoid repeated requests.
 */
@InterfaceAudience.Private
public class HMasterAsyncRegistry implements AsyncRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(HMasterAsyncRegistry.class);
  public static final String CONF_KEY = "hbase.client.asyncregistry.masteraddrs";
  private static final String DEFAULT_HOST_PORT = "localhost:" + HConstants.DEFAULT_MASTER_PORT;

  // Parsed list of host and ports for masters from hbase-site.xml
  private final List<ServerName> masterServers;
  final Configuration conf;
  // RPC client used to talk to the master servers. This uses a stand alone RpcClient instance
  // because AsyncRegistry is created prior to creating a cluster Connection. The client is torn
  // down in close().
  final RpcClient rpcClient;
  final int rpcTimeout;

  public HMasterAsyncRegistry(Configuration config) {
    masterServers = new ArrayList<>();
    conf = config;
    parseHortPorts();
    // Passing the default cluster ID means that the token based authentication does not work for
    // certain client implementations.
    // TODO(bharathv): Figure out a way to fetch the CLUSTER ID using a non authenticated way.
    rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    rpcTimeout = (int) Math.min(Integer.MAX_VALUE,
        TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_TIMEOUT_KEY,
            DEFAULT_HBASE_RPC_TIMEOUT)));
  }

  private void parseHortPorts() {
    String hostPorts = conf.get(CONF_KEY, DEFAULT_HOST_PORT);
    for (String hostPort: hostPorts.split(",")) {
      masterServers.add(ServerName.valueOf(hostPort, ServerName.NON_STARTCODE));
    }
    Preconditions.checkArgument(!masterServers.isEmpty(), String.format("%s is empty", CONF_KEY));
    // Randomize so that not every client sends requests in the same order.
    Collections.shuffle(masterServers);
  }

  /**
   * Util that generates a master stub for a given ServerName.
   */
  private MasterService.BlockingInterface getMasterStub(ServerName server) throws IOException  {
    return MasterService.newBlockingStub(
        rpcClient.createBlockingRpcChannel(server, User.getCurrent(), rpcTimeout));
  }

  /**
   * Blocking RPC to fetch the meta region locations using one of the masters from the parsed list.
   */
  private RegionLocations getMetaRegionLocationsHelper() throws MetaRegionsNotAvailableException {
    List<HBaseProtos.RegionLocation> result = null;
    for (ServerName sname: masterServers) {
      try {
        MasterService.BlockingInterface stub = getMasterStub(sname);
        HBaseRpcController rpcController = RpcControllerFactory.instantiate(conf).newController();
        GetMetaLocationsResponse resp = stub.getMetaLocations(rpcController,
            GetMetaLocationsRequest.getDefaultInstance());
        result = resp.getLocationsList();
      } catch (Exception e) {
        LOG.warn("Error fetch meta locations from master {}. Trying others.", sname, e);
      }
    }
    if (result == null || result.isEmpty()) {
      throw new MetaRegionsNotAvailableException(String.format(
          "Meta locations not found. Probed masters: %s", conf.get(CONF_KEY, DEFAULT_HOST_PORT)));
    }
    List<HRegionLocation> deserializedResult = new ArrayList<>();
    result.stream().forEach(
        location -> deserializedResult.add(ProtobufUtil.toRegionLocation(location)));
    return new RegionLocations(deserializedResult);
  }

  /**
   * Picks the first master entry from 'masterHortPorts' to fetch the meta region locations.
   */
  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    CompletableFuture<RegionLocations> result = new CompletableFuture<>();
    CompletableFuture.runAsync(() -> {
      try {
        result.complete(this.getMetaRegionLocationsHelper());
      } catch (Exception e) {
        result.completeExceptionally(e);
      }
    });
    return result;
  }

  /**
   * Blocking RPC to get the cluster ID from the parsed master list. Returns null if no active
   * master found.
   */
  private String getClusterIdHelper() throws  MasterNotRunningException {
    // Loop through all the masters serially. We could be hitting some standby masters which cannot
    // process this RPC, so we just skip them.
    for (ServerName sname: masterServers) {
      try {
        MasterService.BlockingInterface stub = getMasterStub(sname);
        HBaseRpcController rpcController = RpcControllerFactory.instantiate(conf).newController();
        GetClusterIdResponse resp =
            stub.getClusterId(rpcController, GetClusterIdRequest.getDefaultInstance());
        return resp.getClusterId();
      } catch (IOException e) {
        LOG.warn("Error fetching cluster ID from master: {}", sname, e);
      } catch (ServiceException e) {
        // This is probably a standby master, can be ignored.
        LOG.debug("Error fetching cluster ID from server: {}" , sname, e);
      }
    }
    // If it comes to this point, no active master could be found.
    throw new MasterNotRunningException(String.format(
        "No active master found. Probed masters: %s", conf.get(CONF_KEY, DEFAULT_HOST_PORT)));
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    CompletableFuture<String> result = new CompletableFuture<>();
    CompletableFuture.runAsync(() -> {
      try {
        result.complete(this.getClusterIdHelper());
      } catch (Exception e) {
        result.completeExceptionally(e);
      }
    });
    return result;
  }

  /**
   * Blocking RPC to get the active master address from the parsed list of master servers.
   */
  private ServerName getMasterAddressHelper() throws MasterNotRunningException  {
    for (ServerName sname: masterServers) {
      try {
        MasterService.BlockingInterface stub = getMasterStub(sname);
        HBaseRpcController rpcController = RpcControllerFactory.instantiate(conf).newController();
        IsActiveResponse resp = stub.isActive(rpcController, IsActiveRequest.getDefaultInstance());
        if (resp.getIsMasterActive()) {
          return ServerName.valueOf(sname.getHostname(), sname.getPort(), resp.getStartCode());
        }
      } catch (Exception e) {

      }
    }
    throw new MasterNotRunningException(String.format("No active master found. Probed masters: %s",
        conf.get(CONF_KEY, DEFAULT_HOST_PORT)));
  }

  /**
   * @return the active master among the configured master addresses in 'masterHortPorts'.
   */
  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    CompletableFuture<ServerName> result = new CompletableFuture<>();
    CompletableFuture.runAsync(() -> {
      try {
        result.complete(this.getMasterAddressHelper());
      } catch (Exception e) {
        result.completeExceptionally(e);
      }
    });
    return result;
  }

  @Override
  public void close() {
    if (rpcClient != null) {
      rpcClient.close();
    }
  }
}
