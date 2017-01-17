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

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getLocateType;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;

/**
 * The asynchronous client scanner implementation.
 * <p>
 * Here we will call OpenScanner first and use the returned scannerId to create a
 * {@link AsyncScanSingleRegionRpcRetryingCaller} to do the real scan operation. The return value of
 * {@link AsyncScanSingleRegionRpcRetryingCaller} will tell us whether open a new scanner or finish
 * scan.
 */
@InterfaceAudience.Private
class AsyncClientScanner {

  // We will use this scan object during the whole scan operation. The
  // AsyncScanSingleRegionRpcRetryingCaller will modify this scan object directly.
  private final Scan scan;

  private final RawScanResultConsumer consumer;

  private final TableName tableName;

  private final AsyncConnectionImpl conn;

  private final long pauseNs;

  private final int maxAttempts;

  private final long scanTimeoutNs;

  private final long rpcTimeoutNs;

  private final int startLogErrorsCnt;

  private final ScanResultCache resultCache;

  public AsyncClientScanner(Scan scan, RawScanResultConsumer consumer, TableName tableName,
      AsyncConnectionImpl conn, long pauseNs, int maxAttempts, long scanTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    if (scan.getStartRow() == null) {
      scan.withStartRow(EMPTY_START_ROW, scan.includeStartRow());
    }
    if (scan.getStopRow() == null) {
      scan.withStopRow(EMPTY_END_ROW, scan.includeStopRow());
    }
    this.scan = scan;
    this.consumer = consumer;
    this.tableName = tableName;
    this.conn = conn;
    this.pauseNs = pauseNs;
    this.maxAttempts = maxAttempts;
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.resultCache = scan.getAllowPartialResults() || scan.getBatch() > 0
        ? new AllowPartialScanResultCache() : new CompleteScanResultCache();
  }

  private static final class OpenScannerResponse {

    public final HRegionLocation loc;

    public final ClientService.Interface stub;

    public final long scannerId;

    public OpenScannerResponse(HRegionLocation loc, Interface stub, long scannerId) {
      this.loc = loc;
      this.stub = stub;
      this.scannerId = scannerId;
    }
  }

  private CompletableFuture<OpenScannerResponse> callOpenScanner(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub) {
    CompletableFuture<OpenScannerResponse> future = new CompletableFuture<>();
    try {
      ScanRequest request =
          RequestConverter.buildScanRequest(loc.getRegionInfo().getRegionName(), scan, 0, false);
      stub.scan(controller, request, resp -> {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
          return;
        }
        future.complete(new OpenScannerResponse(loc, stub, resp.getScannerId()));
      });
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private void startScan(OpenScannerResponse resp) {
    conn.callerFactory.scanSingleRegion().id(resp.scannerId).location(resp.loc).stub(resp.stub)
        .setScan(scan).consumer(consumer).resultCache(resultCache)
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .scanTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
        .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt).start()
        .whenComplete((hasMore, error) -> {
          if (error != null) {
            consumer.onError(error);
            return;
          }
          if (hasMore) {
            openScanner();
          } else {
            consumer.onComplete();
          }
        });
  }

  private void openScanner() {
    conn.callerFactory.<OpenScannerResponse> single().table(tableName).row(scan.getStartRow())
        .locateType(getLocateType(scan)).rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
        .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt).action(this::callOpenScanner)
        .call().whenComplete((resp, error) -> {
          if (error != null) {
            consumer.onError(error);
            return;
          }
          startScan(resp);
        });
  }

  public void start() {
    openScanner();
  }
}
