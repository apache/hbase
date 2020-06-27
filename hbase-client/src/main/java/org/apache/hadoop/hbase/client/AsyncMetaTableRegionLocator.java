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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetAllMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.LocateMetaRegionRequest;

/**
 * The class for locating region for meta table.
 */
@InterfaceAudience.Private
class AsyncMetaTableRegionLocator extends AbstractAsyncTableRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncMetaTableRegionLocator.class);

  private final AtomicReference<Interface> stub = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<Interface>> stubMakeFuture =
    new AtomicReference<>();

  AsyncMetaTableRegionLocator(AsyncConnectionImpl conn, TableName tableName, int maxConcurrent) {
    // for meta region we should use MetaCellComparator to compare the row keys
    super(conn, tableName, maxConcurrent, (r1, r2) -> CellComparatorImpl.MetaCellComparator
      .compareRows(r1, 0, r1.length, r2, 0, r2.length));
  }

  private Interface createStub(ServerName serverName) throws IOException {
    return ClientMetaService.newStub(conn.rpcClient.createRpcChannel(serverName, conn.user,
      (int) TimeUnit.NANOSECONDS.toMillis(conn.connConf.getReadRpcTimeoutNs())));
  }

  CompletableFuture<Interface> getStub() {
    return ConnectionUtils.getOrFetch(stub, stubMakeFuture, false, () -> {
      CompletableFuture<Interface> future = new CompletableFuture<>();
      addListener(conn.registry.getActiveMaster(), (addr, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else if (addr == null) {
          future.completeExceptionally(new MasterNotRunningException(
            "ZooKeeper available but no active master location found"));
        } else {
          LOG.debug("The fetched master address is {}", addr);
          try {
            future.complete(createStub(addr));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }

      });
      return future;
    }, stub -> true, "ClientLocateMetaStub");
  }

  private void tryClearMasterStubCache(IOException error, Interface currentStub) {
    if (ClientExceptionsUtil.isConnectionException(error) ||
      error instanceof ServerNotRunningYetException) {
      stub.compareAndSet(currentStub, null);
    }
  }

  @Override
  protected void locate(LocateRequest req) {
    addListener(getStub(), (stub, error) -> {
      if (error != null) {
        onLocateComplete(req, null, error);
        return;
      }
      HBaseRpcController controller = conn.rpcControllerFactory.newController();
      stub.locateMetaRegion(controller,
        LocateMetaRegionRequest.newBuilder().setRow(ByteString.copyFrom(req.row))
          .setLocateType(ProtobufUtil.toProtoRegionLocateType(req.locateType)).build(),
        resp -> {
          if (controller.failed()) {
            IOException ex = controller.getFailed();
            tryClearMasterStubCache(ex, stub);
            onLocateComplete(req, null, ex);
            return;
          }
          RegionLocations locs = new RegionLocations(resp.getMetaLocationsList().stream()
            .map(ProtobufUtil::toRegionLocation).collect(Collectors.toList()));
          if (validateRegionLocations(locs, req)) {
            onLocateComplete(req, locs, null);
          }
        });
    });
  }

  @Override
  CompletableFuture<List<HRegionLocation>>
    getAllRegionLocations(boolean excludeOfflinedSplitParents) {
    CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
    addListener(getStub(), (stub, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      HBaseRpcController controller = conn.rpcControllerFactory.newController();
      stub.getAllMetaRegionLocations(controller, GetAllMetaRegionLocationsRequest.newBuilder()
        .setExcludeOfflinedSplitParents(excludeOfflinedSplitParents).build(), resp -> {
          if (controller.failed()) {
            IOException ex = controller.getFailed();
            tryClearMasterStubCache(ex, stub);
            future.completeExceptionally(ex);
            return;
          }
          List<HRegionLocation> locs = resp.getMetaLocationsList().stream()
            .map(ProtobufUtil::toRegionLocation).collect(Collectors.toList());
          future.complete(locs);
        });
    });
    return future;
  }
}
