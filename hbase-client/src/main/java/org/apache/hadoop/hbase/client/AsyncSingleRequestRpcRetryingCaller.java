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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Retry caller for a single request, such as get, put, delete, etc.
 */
@InterfaceAudience.Private
class AsyncSingleRequestRpcRetryingCaller<T> extends AsyncRpcRetryingCaller<T> {

  @FunctionalInterface
  public interface Callable<T> {
    CompletableFuture<T> call(HBaseRpcController controller, HRegionLocation loc,
        ClientService.Interface stub);
  }

  private final TableName tableName;

  private final byte[] row;

  private final RegionLocateType locateType;

  private final Callable<T> callable;

  public AsyncSingleRequestRpcRetryingCaller(HashedWheelTimer retryTimer, AsyncConnectionImpl conn,
      TableName tableName, byte[] row, RegionLocateType locateType, Callable<T> callable,
      long pauseNs, int maxAttempts, long operationTimeoutNs, long rpcTimeoutNs,
      int startLogErrorsCnt) {
    super(retryTimer, conn, pauseNs, maxAttempts, operationTimeoutNs, rpcTimeoutNs,
        startLogErrorsCnt);
    this.tableName = tableName;
    this.row = row;
    this.locateType = locateType;
    this.callable = callable;
  }

  private void call(HRegionLocation loc) {
    ClientService.Interface stub;
    try {
      stub = conn.getRegionServerStub(loc.getServerName());
    } catch (IOException e) {
      onError(e,
        () -> "Get async stub to " + loc.getServerName() + " for '" + Bytes.toStringBinary(row)
            + "' in " + loc.getRegion().getEncodedName() + " of " + tableName + " failed",
        err -> conn.getLocator().updateCachedLocation(loc, err));
      return;
    }
    resetCallTimeout();
    callable.call(controller, loc, stub).whenComplete(
      (result, error) -> {
        if (error != null) {
          onError(error,
            () -> "Call to " + loc.getServerName() + " for '" + Bytes.toStringBinary(row) + "' in "
                + loc.getRegion().getEncodedName() + " of " + tableName + " failed",
            err -> conn.getLocator().updateCachedLocation(loc, err));
          return;
        }
        future.complete(result);
      });
  }

  @Override
  protected void doCall() {
    long locateTimeoutNs;
    if (operationTimeoutNs > 0) {
      locateTimeoutNs = remainingTimeNs();
      if (locateTimeoutNs <= 0) {
        completeExceptionally();
        return;
      }
    } else {
      locateTimeoutNs = -1L;
    }
    conn.getLocator()
        .getRegionLocation(tableName, row, locateType, locateTimeoutNs)
        .whenComplete(
          (loc, error) -> {
            if (error != null) {
              onError(error, () -> "Locate '" + Bytes.toStringBinary(row) + "' in " + tableName
                  + " failed", err -> {
              });
              return;
            }
            call(loc);
          });
  }

  @Override
  public CompletableFuture<T> call() {
    doCall();
    return future;
  }
}
