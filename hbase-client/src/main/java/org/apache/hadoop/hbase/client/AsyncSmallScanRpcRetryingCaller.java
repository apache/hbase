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

import static org.apache.hadoop.hbase.client.ConnectionUtils.getLocateType;
import static org.apache.hadoop.hbase.client.ConnectionUtils.noMoreResultsForReverseScan;
import static org.apache.hadoop.hbase.client.ConnectionUtils.noMoreResultsForScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;

/**
 * Retry caller for smaller scan.
 */
@InterfaceAudience.Private
class AsyncSmallScanRpcRetryingCaller {

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final Scan scan;

  private final int limit;

  private final long scanTimeoutNs;

  private final long rpcTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrosCnt;

  private final Function<HRegionInfo, Boolean> nextScan;

  private final List<Result> resultList;

  private final CompletableFuture<List<Result>> future;

  public AsyncSmallScanRpcRetryingCaller(AsyncConnectionImpl conn, TableName tableName, Scan scan,
      int limit, long pauseNs, int maxAttempts, long scanTimeoutNs, long rpcTimeoutNs,
      int startLogErrosCnt) {
    this.conn = conn;
    this.tableName = tableName;
    this.scan = scan;
    this.limit = limit;
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.pauseNs = pauseNs;
    this.maxAttempts = maxAttempts;
    this.startLogErrosCnt = startLogErrosCnt;
    if (scan.isReversed()) {
      this.nextScan = this::reversedNextScan;
    } else {
      this.nextScan = this::nextScan;
    }
    this.resultList = new ArrayList<>();
    this.future = new CompletableFuture<>();
  }

  private static final class SmallScanResponse {

    public final Result[] results;

    public final HRegionInfo currentRegion;

    public final boolean hasMoreResultsInRegion;

    public SmallScanResponse(Result[] results, HRegionInfo currentRegion,
        boolean hasMoreResultsInRegion) {
      this.results = results;
      this.currentRegion = currentRegion;
      this.hasMoreResultsInRegion = hasMoreResultsInRegion;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD",
      justification = "Findbugs seems to be confused by lambda expression.")
  private CompletableFuture<SmallScanResponse> scan(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub) {
    CompletableFuture<SmallScanResponse> future = new CompletableFuture<>();
    ScanRequest req;
    try {
      req = RequestConverter.buildScanRequest(loc.getRegionInfo().getRegionName(), scan,
        limit - resultList.size(), true);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    stub.scan(controller, req, resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getFailed());
      } else {
        try {
          Result[] results = ResponseConverter.getResults(controller.cellScanner(), resp);
          future.complete(
            new SmallScanResponse(results, loc.getRegionInfo(), resp.getMoreResultsInRegion()));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
      }
    });
    return future;
  }

  private void onComplete(SmallScanResponse resp) {
    resultList.addAll(Arrays.asList(resp.results));
    if (resultList.size() == limit) {
      future.complete(resultList);
      return;
    }
    if (resp.hasMoreResultsInRegion) {
      if (resp.results.length > 0) {
        scan.withStartRow(resp.results[resp.results.length - 1].getRow(), false);
      }
      scan();
      return;
    }
    if (!nextScan.apply(resp.currentRegion)) {
      future.complete(resultList);
    }
  }

  private void scan() {
    conn.callerFactory.<SmallScanResponse> single().table(tableName).row(scan.getStartRow())
        .locateType(getLocateType(scan)).rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
        .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrosCnt).action(this::scan).call()
        .whenComplete((resp, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          } else {
            onComplete(resp);
          }
        });
  }

  public CompletableFuture<List<Result>> call() {
    scan();
    return future;
  }

  private boolean nextScan(HRegionInfo info) {
    if (noMoreResultsForScan(scan, info)) {
      return false;
    } else {
      scan.withStartRow(info.getEndKey());
      scan();
      return true;
    }
  }

  private boolean reversedNextScan(HRegionInfo info) {
    if (noMoreResultsForReverseScan(scan, info)) {
      return false;
    } else {
      scan.withStartRow(info.getStartKey(), false);
      scan();
      return true;
    }
  }
}
