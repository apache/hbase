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

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import java.util.concurrent.CompletableFuture;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * Retry caller for a request call to master.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class AsyncMasterRequestRpcRetryingCaller<T> extends AsyncRpcRetryingCaller<T> {

  @FunctionalInterface
  public interface Callable<T> {
    CompletableFuture<T> call(HBaseRpcController controller, MasterService.Interface stub);
  }

  private final Callable<T> callable;

  public AsyncMasterRequestRpcRetryingCaller(HashedWheelTimer retryTimer, AsyncConnectionImpl conn,
      Callable<T> callable, long pauseNs, int maxRetries, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    super(retryTimer, conn, pauseNs, maxRetries, operationTimeoutNs, rpcTimeoutNs,
        startLogErrorsCnt);
    this.callable = callable;
  }

  @Override
  protected void doCall() {
    conn.getMasterStub().whenComplete((stub, error) -> {
      if (error != null) {
        onError(error, () -> "Get async master stub failed", err -> {
        });
        return;
      }
      resetCallTimeout();
      callable.call(controller, stub).whenComplete((result, error2) -> {
        if (error2 != null) {
          onError(error2, () -> "Call to master failed", err -> {
          });
          return;
        }
        future.complete(result);
      });
    });
  }

  @Override
  public CompletableFuture<T> call() {
    doCall();
    return future;
  }
}
