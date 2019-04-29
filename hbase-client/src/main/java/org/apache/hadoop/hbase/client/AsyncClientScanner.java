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
import static org.apache.hadoop.hbase.client.ConnectionUtils.createScanResultCache;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getLocateType;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRPCCallsMetrics;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRPCRetriesMetrics;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRegionCountMetrics;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isRemote;
import static org.apache.hadoop.hbase.client.ConnectionUtils.timelineConsistentRead;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

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

  private final ScanMetrics scanMetrics;

  private final AdvancedScanResultConsumer consumer;

  private final TableName tableName;

  private final AsyncConnectionImpl conn;

  private final Timer retryTimer;

  private final long pauseNs;

  private final long pauseForCQTBENs;

  private final int maxAttempts;

  private final long scanTimeoutNs;

  private final long rpcTimeoutNs;

  private final int startLogErrorsCnt;

  private final ScanResultCache resultCache;

  public AsyncClientScanner(Scan scan, AdvancedScanResultConsumer consumer, TableName tableName,
      AsyncConnectionImpl conn, Timer retryTimer, long pauseNs, long pauseForCQTBENs,
      int maxAttempts, long scanTimeoutNs, long rpcTimeoutNs, int startLogErrorsCnt) {
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
    this.retryTimer = retryTimer;
    this.pauseNs = pauseNs;
    this.pauseForCQTBENs = pauseForCQTBENs;
    this.maxAttempts = maxAttempts;
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.resultCache = createScanResultCache(scan);
    if (scan.isScanMetricsEnabled()) {
      this.scanMetrics = new ScanMetrics();
      consumer.onScanMetricsCreated(scanMetrics);
    } else {
      this.scanMetrics = null;
    }
  }

  private static final class OpenScannerResponse {

    public final HRegionLocation loc;

    public final boolean isRegionServerRemote;

    public final ClientService.Interface stub;

    public final HBaseRpcController controller;

    public final ScanResponse resp;

    public OpenScannerResponse(HRegionLocation loc, boolean isRegionServerRemote, Interface stub,
        HBaseRpcController controller, ScanResponse resp) {
      this.loc = loc;
      this.isRegionServerRemote = isRegionServerRemote;
      this.stub = stub;
      this.controller = controller;
      this.resp = resp;
    }
  }

  private final AtomicInteger openScannerTries = new AtomicInteger();

  private CompletableFuture<OpenScannerResponse> callOpenScanner(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub) {
    boolean isRegionServerRemote = isRemote(loc.getHostname());
    incRPCCallsMetrics(scanMetrics, isRegionServerRemote);
    if (openScannerTries.getAndIncrement() > 1) {
      incRPCRetriesMetrics(scanMetrics, isRegionServerRemote);
    }
    CompletableFuture<OpenScannerResponse> future = new CompletableFuture<>();
    try {
      ScanRequest request = RequestConverter.buildScanRequest(loc.getRegion().getRegionName(), scan,
        scan.getCaching(), false);
      stub.scan(controller, request, resp -> {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
          return;
        }
        future.complete(new OpenScannerResponse(loc, isRegionServerRemote, stub, controller, resp));
      });
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private void startScan(OpenScannerResponse resp) {
    addListener(
      conn.callerFactory.scanSingleRegion().id(resp.resp.getScannerId()).location(resp.loc)
        .remote(resp.isRegionServerRemote)
        .scannerLeaseTimeoutPeriod(resp.resp.getTtl(), TimeUnit.MILLISECONDS).stub(resp.stub)
        .setScan(scan).metrics(scanMetrics).consumer(consumer).resultCache(resultCache)
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .scanTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
        .pauseForCQTBE(pauseForCQTBENs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt).start(resp.controller, resp.resp),
      (hasMore, error) -> {
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

  private CompletableFuture<OpenScannerResponse> openScanner(int replicaId) {
    return conn.callerFactory.<OpenScannerResponse> single().table(tableName)
      .row(scan.getStartRow()).replicaId(replicaId).locateType(getLocateType(scan))
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
      .operationTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForCQTBE(pauseForCQTBENs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
      .startLogErrorsCnt(startLogErrorsCnt).action(this::callOpenScanner).call();
  }

  private long getPrimaryTimeoutNs() {
    return TableName.isMetaTableName(tableName) ? conn.connConf.getPrimaryMetaScanTimeoutNs()
      : conn.connConf.getPrimaryScanTimeoutNs();
  }

  private void openScanner() {
    incRegionCountMetrics(scanMetrics);
    openScannerTries.set(1);
    addListener(timelineConsistentRead(conn.getLocator(), tableName, scan, scan.getStartRow(),
      getLocateType(scan), this::openScanner, rpcTimeoutNs, getPrimaryTimeoutNs(), retryTimer,
      conn.getConnectionMetrics()), (resp, error) -> {
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
