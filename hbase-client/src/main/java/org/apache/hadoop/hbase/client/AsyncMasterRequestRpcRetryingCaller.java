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

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

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

  public AsyncMasterRequestRpcRetryingCaller(Timer retryTimer, AsyncConnectionImpl conn,
      Callable<T> callable, int priority, long pauseNs, long pauseNsForServerOverloaded,
      int maxRetries, long operationTimeoutNs, long rpcTimeoutNs, int startLogErrorsCnt) {
    super(retryTimer, conn, priority, pauseNs, pauseNsForServerOverloaded, maxRetries,
      operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
    this.callable = callable;
  }

  private void clearMasterStubCacheOnError(MasterService.Interface stub, Throwable error) {
    // ServerNotRunningYetException may because it is the backup master.
    if (ClientExceptionsUtil.isConnectionException(error) ||
      error instanceof ServerNotRunningYetException) {
      conn.clearMasterStubCache(stub);
    }
  }

  @Override
  protected void doCall() {
    addListener(conn.getMasterStub(), (stub, error) -> {
      if (error != null) {
        onError(error, () -> "Get async master stub failed", err -> {
        });
        return;
      }
      resetCallTimeout();
      addListener(callable.call(controller, stub), (result, error2) -> {
        if (error2 != null) {
          onError(error2, () -> "Call to master failed",
            err -> clearMasterStubCacheOnError(stub, error2));
          return;
        }
        future.complete(result);
      });
    });
  }
}
