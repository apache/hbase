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

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerProtos.ReplicationServerService;

/**
 * A simple wrapper of the {@link ReplicationServerService} for a replication server.
 * <p/>
 * Notice that there is no retry, and this is intentional.
 */
@InterfaceAudience.Private
public class AsyncReplicationServerAdmin {

  private final ServerName server;

  private final AsyncConnectionImpl conn;

  AsyncReplicationServerAdmin(ServerName server, AsyncConnectionImpl conn) {
    this.server = server;
    this.conn = conn;
  }

  @FunctionalInterface
  private interface RpcCall<RESP> {
    void call(ReplicationServerService.Interface stub, HBaseRpcController controller,
        RpcCallback<RESP> done);
  }

  private <RESP> CompletableFuture<RESP> call(RpcCall<RESP> rpcCall, CellScanner cellScanner) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    HBaseRpcController controller = conn.rpcControllerFactory.newController(cellScanner);
    try {
      rpcCall.call(conn.getReplicationServerStub(server), controller, resp -> {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          future.complete(resp);
        }
      });
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  public CompletableFuture<AdminProtos.ReplicateWALEntryResponse> replicateWALEntry(
      AdminProtos.ReplicateWALEntryRequest request, CellScanner cellScanner, int timeout) {
    return call((stub, controller, done) -> {
      controller.setCallTimeout(timeout);
      stub.replicateWALEntry(controller, request, done);
    }, cellScanner);
  }
}
