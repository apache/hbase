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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactionService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactResponse;



/**
 * A simple wrapper of the {@link CompactionService} for a compaction server, which returns a
 * {@link CompletableFuture}. This is easier to use, as if you use the raw protobuf interface, you
 * need to get the result from the {@link RpcCallback}, and if there is an exception, you need to
 * get it from the {@link RpcController} passed in.
 * <p/>
 * Notice that there is no retry, and this is intentional. We have different retry for different
 * usage for now, if later we want to unify them, we can move the retry logic into this class.
 */
@InterfaceAudience.Private
public class AsyncCompactionServerCaller {

  private final ServerName server;

  private final AsyncConnectionImpl conn;

  AsyncCompactionServerCaller(ServerName server, AsyncConnectionImpl conn) {
    this.server = server;
    this.conn = conn;
  }

  @FunctionalInterface
  private interface RpcCall<RESP> {
    void call(CompactionService.Interface stub, HBaseRpcController controller, RpcCallback<RESP> done);
  }

  private <RESP> CompletableFuture<RESP> call(RpcCall<RESP> rpcCall) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    HBaseRpcController controller = conn.rpcControllerFactory.newController();
    try {
      rpcCall.call(conn.getCompactionStub(server), controller, new RpcCallback<RESP>() {
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

  public CompletableFuture<CompactResponse> requestCompaction(CompactRequest request) {
    return call((stub, controller, done) -> stub.requestCompaction(controller, request, done));
  }

}
