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

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.BootstrapNodeService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.GetAllBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLiveRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;

/**
 * The implementation of AsyncClusterConnection.
 */
@InterfaceAudience.Private
class AsyncClusterConnectionImpl extends AsyncConnectionImpl implements AsyncClusterConnection {

  public AsyncClusterConnectionImpl(Configuration conf, ConnectionRegistry registry,
      String clusterId, SocketAddress localAddress, User user) {
    super(conf, registry, clusterId, localAddress, user);
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return super.getNonceGenerator();
  }

  @Override
  public RpcClient getRpcClient() {
    return rpcClient;
  }

  @Override
  public AsyncRegionServerAdmin getRegionServerAdmin(ServerName serverName) {
    return new AsyncRegionServerAdmin(serverName, this);
  }

  @Override
  public CompletableFuture<FlushRegionResponse> flush(byte[] regionName,
      boolean writeFlushWALMarker) {
    RawAsyncHBaseAdmin admin = (RawAsyncHBaseAdmin) getAdmin();
    return admin.flushRegionInternal(regionName, null, writeFlushWALMarker);
  }

  @Override
  public CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      boolean reload) {
    return getLocator().getRegionLocations(tableName, row, RegionLocateType.CURRENT, reload, -1L);
  }

  @Override
  public CompletableFuture<String> prepareBulkLoad(TableName tableName) {
    return callerFactory.<String> single().table(tableName).row(HConstants.EMPTY_START_ROW)
      .action((controller, loc, stub) -> ConnectionUtils
        .<TableName, PrepareBulkLoadRequest, PrepareBulkLoadResponse, String> call(controller, loc,
          stub, tableName, (rn, tn) -> {
            RegionSpecifier region =
              RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, rn);
            return PrepareBulkLoadRequest.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName(tn)).setRegion(region).build();
          }, (s, c, req, done) -> s.prepareBulkLoad(c, req, done),
          (c, resp) -> resp.getBulkToken()))
      .call();
  }

  @Override
  public CompletableFuture<Boolean> bulkLoad(TableName tableName,
    List<Pair<byte[], String>> familyPaths, byte[] row, boolean assignSeqNum, Token<?> userToken,
    String bulkToken, boolean copyFiles, List<String> clusterIds, boolean replicate) {
    return callerFactory.<Boolean> single().table(tableName).row(row)
      .action((controller, loc, stub) -> ConnectionUtils
        .<Void, BulkLoadHFileRequest, BulkLoadHFileResponse, Boolean> call(controller, loc, stub,
          null,
          (rn, nil) -> RequestConverter.buildBulkLoadHFileRequest(familyPaths, rn, assignSeqNum,
            userToken, bulkToken, copyFiles, clusterIds, replicate),
          (s, c, req, done) -> s.bulkLoadHFile(c, req, done), (c, resp) -> resp.getLoaded()))
      .call();
  }

  @Override
  public CompletableFuture<Void> cleanupBulkLoad(TableName tableName, String bulkToken) {
    return callerFactory.<Void> single().table(tableName).row(HConstants.EMPTY_START_ROW)
      .action((controller, loc, stub) -> ConnectionUtils
        .<String, CleanupBulkLoadRequest, CleanupBulkLoadResponse, Void> call(controller, loc, stub,
          bulkToken, (rn, bt) -> {
            RegionSpecifier region =
              RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, rn);
            return CleanupBulkLoadRequest.newBuilder().setRegion(region).setBulkToken(bt).build();
          }, (s, c, req, done) -> s.cleanupBulkLoad(c, req, done), (c, resp) -> null))
      .call();
  }

  @Override
  public CompletableFuture<List<ServerName>>
    getLiveRegionServers(MasterAddressTracker masterAddrTracker, int count) {
    CompletableFuture<List<ServerName>> future = new CompletableFuture<>();
    RegionServerStatusService.Interface stub = RegionServerStatusService
      .newStub(rpcClient.createRpcChannel(masterAddrTracker.getMasterAddress(), user, rpcTimeout));
    HBaseRpcController controller = rpcControllerFactory.newController();
    stub.getLiveRegionServers(controller,
      GetLiveRegionServersRequest.newBuilder().setCount(count).build(), resp -> {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          future.complete(resp.getServerList().stream().map(ProtobufUtil::toServerName)
            .collect(Collectors.toList()));
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<List<ServerName>> getAllBootstrapNodes(ServerName regionServer) {
    CompletableFuture<List<ServerName>> future = new CompletableFuture<>();
    BootstrapNodeService.Interface stub =
      BootstrapNodeService.newStub(rpcClient.createRpcChannel(regionServer, user, rpcTimeout));
    HBaseRpcController controller = rpcControllerFactory.newController();
    stub.getAllBootstrapNodes(controller, GetAllBootstrapNodesRequest.getDefaultInstance(),
      resp -> {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          future.complete(resp.getNodeList().stream().map(ProtobufUtil::toServerName)
            .collect(Collectors.toList()));
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> replicate(RegionInfo replica,
    List<Entry> entries, int retries, long rpcTimeoutNs,
    long operationTimeoutNs) {
    return new AsyncRegionReplicationRetryingCaller(RETRY_TIMER, this,
      ConnectionUtils.retries2Attempts(retries), rpcTimeoutNs, operationTimeoutNs, replica, entries)
        .call();
  }
}
