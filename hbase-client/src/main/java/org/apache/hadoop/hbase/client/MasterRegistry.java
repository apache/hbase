/*
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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.DNS.getMasterHostname;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.MasterRegistryFetchException;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.security.User;

import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ClientMetaService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetMastersRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetMastersResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetMastersResponseEntry;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetMetaRegionLocationsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNumLiveRSRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNumLiveRSResponse;

/**
 * Master based registry implementation. Makes RPCs to the configured master addresses from config
 * {@value org.apache.hadoop.hbase.HConstants#MASTER_ADDRS_KEY}. All the registry methods are
 * blocking unlike implementations in other branches.
 */
@InterfaceAudience.Private
public class MasterRegistry implements ConnectionRegistry {
  private static final String MASTER_ADDRS_CONF_SEPARATOR = ",";

  private volatile ImmutableMap<String, ClientMetaService.Interface> masterAddr2Stub;

  // RPC client used to talk to the masters.
  private RpcClient rpcClient;
  private RpcControllerFactory rpcControllerFactory;
  private int rpcTimeoutMs;

  protected MasterAddressRefresher masterAddressRefresher;

  @Override
  public void init(Connection connection) throws IOException {
    Configuration conf = connection.getConfiguration();
    rpcTimeoutMs = (int) Math.min(Integer.MAX_VALUE,
        conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    // HBASE-25051: we pass cluster id as null here since we do not have a cluster id yet, we have
    // to fetch this through the master registry...
    // This is a problem as we will use the cluster id to determine the authentication method
    rpcClient = RpcClientFactory.createClient(conf, null);
    rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    populateMasterStubs(parseMasterAddrs(conf));
    masterAddressRefresher = new MasterAddressRefresher(conf, this);
  }

  protected interface Callable <T extends Message> {
    T call(ClientMetaService.Interface stub, RpcController controller) throws IOException;
  }

   protected <T extends Message> T doCall(Callable<T> callable)
       throws MasterRegistryFetchException {
    Exception lastException = null;
    Set<String> masters = masterAddr2Stub.keySet();
    List<ClientMetaService.Interface> stubs = new ArrayList<>(masterAddr2Stub.values());
    Collections.shuffle(stubs, ThreadLocalRandom.current());
    for (ClientMetaService.Interface stub: stubs) {
      HBaseRpcController controller = rpcControllerFactory.newController();
      try {
        T resp = callable.call(stub, controller);
        if (!controller.failed()) {
          return resp;
        }
        lastException = controller.getFailed();
      } catch (Exception e) {
        lastException = e;
      }
      if (ClientExceptionsUtil.isConnectionException(lastException)) {
        masterAddressRefresher.refreshNow();
      }
    }
    // rpcs to all masters failed.
    throw new MasterRegistryFetchException(masters, lastException);
  }

  @Override
  public ServerName getActiveMaster() throws IOException {
    GetMastersResponseEntry activeMaster = null;
    for (GetMastersResponseEntry entry: getMastersInternal().getMasterServersList()) {
      if (entry.getIsActive()) {
        activeMaster = entry;
        break;
      }
    }
    if (activeMaster == null) {
      throw new HBaseIOException("No active master found");
    }
    return ProtobufUtil.toServerName(activeMaster.getServerName());
  }

  List<ServerName> getMasters() throws IOException {
    List<ServerName> result = new ArrayList<>();
    for (GetMastersResponseEntry entry: getMastersInternal().getMasterServersList()) {
      result.add(ProtobufUtil.toServerName(entry.getServerName()));
    }
    return result;
  }

  private GetMastersResponse getMastersInternal() throws IOException {
    return doCall(new Callable<GetMastersResponse>() {
      @Override
      public GetMastersResponse call(
          ClientMetaService.Interface stub, RpcController controller) throws IOException {
        BlockingRpcCallback<GetMastersResponse> cb = new BlockingRpcCallback<>();
        stub.getMasters(controller, GetMastersRequest.getDefaultInstance(), cb);
        return cb.get();
      }
    });
  }

  @Override
  public RegionLocations getMetaRegionLocations() throws IOException {
    GetMetaRegionLocationsResponse resp = doCall(new Callable<GetMetaRegionLocationsResponse>() {
      @Override
      public GetMetaRegionLocationsResponse call(
          ClientMetaService.Interface stub, RpcController controller) throws IOException {
        BlockingRpcCallback<GetMetaRegionLocationsResponse> cb = new BlockingRpcCallback<>();
        stub.getMetaRegionLocations(controller, GetMetaRegionLocationsRequest.getDefaultInstance(),
            cb);
        return cb.get();
      }
    });
    List<HRegionLocation> result = new ArrayList<>();
    for (HBaseProtos.RegionLocation loc: resp.getMetaLocationsList()) {
      result.add(ProtobufUtil.toRegionLocation(loc));
    }
    return new RegionLocations(result);
  }

  @Override
  public String getClusterId() throws IOException {
    GetClusterIdResponse resp = doCall(new Callable<GetClusterIdResponse>() {
      @Override
      public GetClusterIdResponse call(ClientMetaService.Interface stub, RpcController controller)
          throws IOException {
        BlockingRpcCallback<GetClusterIdResponse> cb = new BlockingRpcCallback<>();
        stub.getClusterId(controller, GetClusterIdRequest.getDefaultInstance(), cb);
        return cb.get();
      }
    });
    return resp.getClusterId();
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    GetNumLiveRSResponse resp = doCall(new Callable<GetNumLiveRSResponse>() {
      @Override
      public GetNumLiveRSResponse call(ClientMetaService.Interface stub, RpcController controller)
          throws IOException {
        BlockingRpcCallback<GetNumLiveRSResponse> cb = new BlockingRpcCallback<>();
        stub.getNumLiveRS(controller, GetNumLiveRSRequest.getDefaultInstance(), cb);
        return cb.get();
      }
    });
    return resp.getNumRegionServers();
  }

  @Override
  public void close() {
    if (rpcClient != null) {
      rpcClient.close();
    }
  }

  /**
   * Parses the list of master addresses from the provided configuration. Supported format is comma
   * separated host[:port] values. If no port number if specified, default master port is assumed.
   * @param conf Configuration to parse from.
   */
  @InterfaceAudience.Private
   public static Set<ServerName> parseMasterAddrs(Configuration conf) throws UnknownHostException {
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

  /**
   * Builds the default master address end point if it is not specified in the configuration.
   * <p/>
   * Will be called in {@code HBaseTestingUtility}.
   */
  @InterfaceAudience.Private
  public static String getMasterAddr(Configuration conf) throws UnknownHostException {
    String masterAddrFromConf = conf.get(HConstants.MASTER_ADDRS_KEY);
    if (!Strings.isNullOrEmpty(masterAddrFromConf)) {
      return masterAddrFromConf;
    }
    String hostname = getMasterHostname(conf);
    int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
    return String.format("%s:%d", hostname, port);
  }

  void populateMasterStubs(Set<ServerName> masters) throws IOException {
    Preconditions.checkNotNull(masters);
    ImmutableMap.Builder<String, ClientMetaService.Interface> builder = ImmutableMap.builder();
    User user = User.getCurrent();
    for (ServerName masterAddr : masters) {
      builder.put(masterAddr.toString(), ClientMetaService.newStub(
          rpcClient.createRpcChannel(masterAddr, user, rpcTimeoutMs)));
    }
    masterAddr2Stub = builder.build();
  }

  @InterfaceAudience.Private
  ImmutableSet<String> getParsedMasterServers() {
    return masterAddr2Stub.keySet();
  }

}
