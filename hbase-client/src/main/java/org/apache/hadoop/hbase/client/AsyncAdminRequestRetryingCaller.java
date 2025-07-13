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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;

@InterfaceAudience.Private
public class AsyncAdminRequestRetryingCaller<T> extends AsyncRpcRetryingCaller<T> {

  @FunctionalInterface
  public interface Callable<T> {
    CompletableFuture<T> call(HBaseRpcController controller, AdminService.Interface stub);
  }

  private final Callable<T> callable;
  private ServerName serverName;

  public AsyncAdminRequestRetryingCaller(Timer retryTimer, AsyncConnectionImpl conn, int priority,
    long pauseNs, long pauseNsForServerOverloaded, int maxAttempts, long operationTimeoutNs,
    long rpcTimeoutNs, int startLogErrorsCnt, ServerName serverName, Callable<T> callable) {
    super(retryTimer, conn, priority, pauseNs, pauseNsForServerOverloaded, maxAttempts,
      operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt, Collections.emptyMap());
    this.serverName = serverName;
    this.callable = callable;
  }

  @Override
  protected Throwable preProcessError(Throwable error) {
    // This retrying caller is mainly used for admin operations, thus we do not implement
    // complicated relocating logic. If here we get a NotServingRegionException, there is no way to
    // recover since we just pass in the server name, which means we can not send request to another
    // region server. So here we just wrap it with a DoNotRetryIOException to fail the request
    // immediately
    if (error instanceof NotServingRegionException) {
      return new DoNotRetryIOException("region is not on " + serverName + ", give up retrying",
        error);
    }
    return error;
  }

  @Override
  protected void doCall() {
    AdminService.Interface adminStub;
    try {
      adminStub = this.conn.getAdminStub(serverName);
    } catch (IOException e) {
      onError(e, () -> "Get async admin stub to " + serverName + " failed", err -> {
      });
      return;
    }
    resetCallTimeout();
    addListener(callable.call(controller, adminStub), (result, error) -> {
      if (error != null) {
        onError(error, () -> "Call to admin stub failed", err -> {
        });
        return;
      }
      future.complete(result);
    });
  }
}
