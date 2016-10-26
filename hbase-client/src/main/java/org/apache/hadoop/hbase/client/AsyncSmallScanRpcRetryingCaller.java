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

import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStartRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;

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
import org.apache.hadoop.hbase.util.Bytes;

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

  private final Function<byte[], byte[]> createClosestNextRow;

  private final Runnable firstScan;

  private final Function<HRegionInfo, Boolean> nextScan;

  private final List<Result> resultList;

  private final CompletableFuture<List<Result>> future;

  public AsyncSmallScanRpcRetryingCaller(AsyncConnectionImpl conn, TableName tableName, Scan scan,
      int limit, long scanTimeoutNs, long rpcTimeoutNs) {
    this.conn = conn;
    this.tableName = tableName;
    this.scan = scan;
    this.limit = limit;
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    if (scan.isReversed()) {
      this.createClosestNextRow = ConnectionUtils::createClosestRowBefore;
      this.firstScan = this::reversedFirstScan;
      this.nextScan = this::reversedNextScan;
    } else {
      this.createClosestNextRow = ConnectionUtils::createClosestRowAfter;
      this.firstScan = this::firstScan;
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
        scan.setStartRow(
          createClosestNextRow.apply(resp.results[resp.results.length - 1].getRow()));
      }
      scan(false);
      return;
    }
    if (!nextScan.apply(resp.currentRegion)) {
      future.complete(resultList);
    }
  }

  private void scan(boolean locateToPreviousRegion) {
    conn.callerFactory.<SmallScanResponse> single().table(tableName).row(scan.getStartRow())
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS)
        .locateToPreviousRegion(locateToPreviousRegion).action(this::scan).call()
        .whenComplete((resp, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          } else {
            onComplete(resp);
          }
        });
  }

  public CompletableFuture<List<Result>> call() {
    firstScan.run();
    return future;
  }

  private void firstScan() {
    scan(false);
  }

  private void reversedFirstScan() {
    scan(isEmptyStartRow(scan.getStartRow()));
  }

  private boolean nextScan(HRegionInfo region) {
    if (isEmptyStopRow(scan.getStopRow())) {
      if (isEmptyStopRow(region.getEndKey())) {
        return false;
      }
    } else {
      if (Bytes.compareTo(region.getEndKey(), scan.getStopRow()) >= 0) {
        return false;
      }
    }
    scan.setStartRow(region.getEndKey());
    scan(false);
    return true;
  }

  private boolean reversedNextScan(HRegionInfo region) {
    if (isEmptyStopRow(scan.getStopRow())) {
      if (isEmptyStartRow(region.getStartKey())) {
        return false;
      }
    } else {
      if (Bytes.compareTo(region.getStartKey(), scan.getStopRow()) <= 0) {
        return false;
      }
    }
    scan.setStartRow(region.getStartKey());
    scan(true);
    return true;
  }
}
