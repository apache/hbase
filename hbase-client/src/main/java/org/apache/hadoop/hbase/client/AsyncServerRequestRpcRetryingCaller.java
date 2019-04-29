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
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

/**
 * Retry caller for a request call to region server.
 * Now only used for coprocessor call to region server.
 */
@InterfaceAudience.Private
public class AsyncServerRequestRpcRetryingCaller<T> extends AsyncRpcRetryingCaller<T> {

  @FunctionalInterface
  public interface Callable<T> {
    CompletableFuture<T> call(HBaseRpcController controller, ClientService.Interface stub);
  }

  private final Callable<T> callable;
  private ServerName serverName;

  public AsyncServerRequestRpcRetryingCaller(Timer retryTimer, AsyncConnectionImpl conn,
      long pauseNs, long pauseForCQTBENs, int maxAttempts, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt, ServerName serverName, Callable<T> callable) {
    super(retryTimer, conn, HConstants.NORMAL_QOS, pauseNs, pauseForCQTBENs, maxAttempts,
      operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
    this.serverName = serverName;
    this.callable = callable;
  }

  @Override
  protected void doCall() {
    ClientService.Interface stub;
    try {
      stub = this.conn.getRegionServerStub(serverName);
    } catch (IOException e) {
      onError(e, () -> "Get async admin stub to " + serverName + " failed", err -> {
      });
      return;
    }
    resetCallTimeout();
    addListener(callable.call(controller, stub), (result, error) -> {
      if (error != null) {
        onError(error, () -> "Call to admin stub failed", err -> {
        });
        return;
      }
      future.complete(result);
    });
  }
}
