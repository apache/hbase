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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;

/**
 * A simple wrapper of the {@link AdminService} for a region server, which returns a
 * {@link CompletableFuture}. This is easier to use, as if you use the raw protobuf interface, you
 * need to get the result from the {@link RpcCallback}, and if there is an exception, you need to
 * get it from the {@link RpcController} passed in.
 * <p/>
 * Notice that there is no retry, and this is intentional. We have different retry for different
 * usage for now, if later we want to unify them, we can move the retry logic into this class.
 */
@InterfaceAudience.Private
public class AsyncRegionServerAdmin {

  private final ServerName server;

  private final AsyncConnectionImpl conn;

  AsyncRegionServerAdmin(ServerName server, AsyncConnectionImpl conn) {
    this.server = server;
    this.conn = conn;
  }

  @FunctionalInterface
  private interface RpcCall<RESP> {
    void call(AdminService.Interface stub, HBaseRpcController controller, RpcCallback<RESP> done);
  }

  private <RESP> CompletableFuture<RESP> call(RpcCall<RESP> rpcCall, CellScanner cellScanner) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    HBaseRpcController controller = conn.rpcControllerFactory.newController(cellScanner);
    try {
      rpcCall.call(conn.getAdminStub(server), controller, new RpcCallback<RESP>() {

        @Override
        public void run(RESP resp) {
          if (controller.failed()) {
            future.completeExceptionally(controller.getFailed());
          } else {
            future.complete(resp);
          }
        }
      });
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private <RESP> CompletableFuture<RESP> call(RpcCall<RESP> rpcCall) {
    return call(rpcCall, null);
  }

  public CompletableFuture<GetRegionInfoResponse> getRegionInfo(GetRegionInfoRequest request) {
    return call((stub, controller, done) -> stub.getRegionInfo(controller, request, done));
  }

  public CompletableFuture<GetStoreFileResponse> getStoreFile(GetStoreFileRequest request) {
    return call((stub, controller, done) -> stub.getStoreFile(controller, request, done));
  }

  public CompletableFuture<GetOnlineRegionResponse> getOnlineRegion(
      GetOnlineRegionRequest request) {
    return call((stub, controller, done) -> stub.getOnlineRegion(controller, request, done));
  }

  public CompletableFuture<OpenRegionResponse> openRegion(OpenRegionRequest request) {
    return call((stub, controller, done) -> stub.openRegion(controller, request, done));
  }

  public CompletableFuture<WarmupRegionResponse> warmupRegion(WarmupRegionRequest request) {
    return call((stub, controller, done) -> stub.warmupRegion(controller, request, done));
  }

  public CompletableFuture<CloseRegionResponse> closeRegion(CloseRegionRequest request) {
    return call((stub, controller, done) -> stub.closeRegion(controller, request, done));
  }

  public CompletableFuture<FlushRegionResponse> flushRegion(FlushRegionRequest request) {
    return call((stub, controller, done) -> stub.flushRegion(controller, request, done));
  }

  public CompletableFuture<CompactionSwitchResponse> compactionSwitch(
      CompactionSwitchRequest request) {
    return call((stub, controller, done) -> stub.compactionSwitch(controller, request, done));
  }

  public CompletableFuture<CompactRegionResponse> compactRegion(CompactRegionRequest request) {
    return call((stub, controller, done) -> stub.compactRegion(controller, request, done));
  }

  public CompletableFuture<ReplicateWALEntryResponse> replicateWALEntry(
      ReplicateWALEntryRequest request, CellScanner cellScanner) {
    return call((stub, controller, done) -> stub.replicateWALEntry(controller, request, done),
      cellScanner);
  }

  public CompletableFuture<ReplicateWALEntryResponse> replay(ReplicateWALEntryRequest request,
      CellScanner cellScanner) {
    return call((stub, controller, done) -> stub.replay(controller, request, done), cellScanner);
  }

  public CompletableFuture<RollWALWriterResponse> rollWALWriter(RollWALWriterRequest request) {
    return call((stub, controller, done) -> stub.rollWALWriter(controller, request, done));
  }

  public CompletableFuture<GetServerInfoResponse> getServerInfo(GetServerInfoRequest request) {
    return call((stub, controller, done) -> stub.getServerInfo(controller, request, done));
  }

  public CompletableFuture<StopServerResponse> stopServer(StopServerRequest request) {
    return call((stub, controller, done) -> stub.stopServer(controller, request, done));
  }

  public CompletableFuture<UpdateFavoredNodesResponse> updateFavoredNodes(
      UpdateFavoredNodesRequest request) {
    return call((stub, controller, done) -> stub.updateFavoredNodes(controller, request, done));
  }

  public CompletableFuture<UpdateConfigurationResponse> updateConfiguration(
      UpdateConfigurationRequest request) {
    return call((stub, controller, done) -> stub.updateConfiguration(controller, request, done));
  }

  public CompletableFuture<GetRegionLoadResponse> getRegionLoad(GetRegionLoadRequest request) {
    return call((stub, controller, done) -> stub.getRegionLoad(controller, request, done));
  }

  public CompletableFuture<ClearCompactionQueuesResponse> clearCompactionQueues(
      ClearCompactionQueuesRequest request) {
    return call((stub, controller, done) -> stub.clearCompactionQueues(controller, request, done));
  }

  public CompletableFuture<ClearRegionBlockCacheResponse> clearRegionBlockCache(
      ClearRegionBlockCacheRequest request) {
    return call((stub, controller, done) -> stub.clearRegionBlockCache(controller, request, done));
  }

  public CompletableFuture<GetSpaceQuotaSnapshotsResponse> getSpaceQuotaSnapshots(
      GetSpaceQuotaSnapshotsRequest request) {
    return call((stub, controller, done) -> stub.getSpaceQuotaSnapshots(controller, request, done));
  }

  public CompletableFuture<ExecuteProceduresResponse> executeProcedures(
      ExecuteProceduresRequest request) {
    return call((stub, controller, done) -> stub.executeProcedures(controller, request, done));
  }
}
