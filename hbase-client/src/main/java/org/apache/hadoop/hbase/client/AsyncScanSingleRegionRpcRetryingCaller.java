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

import static org.apache.hadoop.hbase.client.ConnectionUtils.getPauseTime;
import static org.apache.hadoop.hbase.client.ConnectionUtils.noMoreResultsForReverseScan;
import static org.apache.hadoop.hbase.client.ConnectionUtils.noMoreResultsForScan;
import static org.apache.hadoop.hbase.client.ConnectionUtils.resetController;
import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;
import static org.apache.hadoop.hbase.client.ConnectionUtils.translateException;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Retry caller for scanning a region.
 * <p>
 * We will modify the {@link Scan} object passed in directly. The upper layer should store the
 * reference of this object and use it to open new single region scanners.
 */
@InterfaceAudience.Private
class AsyncScanSingleRegionRpcRetryingCaller {

  private static final Log LOG = LogFactory.getLog(AsyncScanSingleRegionRpcRetryingCaller.class);

  private final HashedWheelTimer retryTimer;

  private final Scan scan;

  private final long scannerId;

  private final ScanResultCache resultCache;

  private final RawScanResultConsumer consumer;

  private final ClientService.Interface stub;

  private final HRegionLocation loc;

  private final long pauseNs;

  private final int maxAttempts;

  private final long scanTimeoutNs;

  private final long rpcTimeoutNs;

  private final int startLogErrorsCnt;

  private final Runnable completeWhenNoMoreResultsInRegion;

  private final CompletableFuture<Boolean> future;

  private final HBaseRpcController controller;

  private byte[] nextStartRowWhenError;

  private boolean includeNextStartRowWhenError;

  private long nextCallStartNs;

  private int tries = 1;

  private final List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions;

  private long nextCallSeq = -1L;

  public AsyncScanSingleRegionRpcRetryingCaller(HashedWheelTimer retryTimer,
      AsyncConnectionImpl conn, Scan scan, long scannerId, ScanResultCache resultCache,
      RawScanResultConsumer consumer, Interface stub, HRegionLocation loc, long pauseNs,
      int maxRetries, long scanTimeoutNs, long rpcTimeoutNs, int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.scan = scan;
    this.scannerId = scannerId;
    this.resultCache = resultCache;
    this.consumer = consumer;
    this.stub = stub;
    this.loc = loc;
    this.pauseNs = pauseNs;
    this.maxAttempts = retries2Attempts(maxRetries);
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    if (scan.isReversed()) {
      completeWhenNoMoreResultsInRegion = this::completeReversedWhenNoMoreResultsInRegion;
    } else {
      completeWhenNoMoreResultsInRegion = this::completeWhenNoMoreResultsInRegion;
    }
    this.future = new CompletableFuture<>();
    this.controller = conn.rpcControllerFactory.newController();
    this.exceptions = new ArrayList<>();
  }

  private long elapsedMs() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nextCallStartNs);
  }

  private void closeScanner() {
    resetController(controller, rpcTimeoutNs);
    ScanRequest req = RequestConverter.buildScanRequest(this.scannerId, 0, true, false);
    stub.scan(controller, req, resp -> {
      if (controller.failed()) {
        LOG.warn("Call to " + loc.getServerName() + " for closing scanner id = " + scannerId
            + " for " + loc.getRegionInfo().getEncodedName() + " of "
            + loc.getRegionInfo().getTable() + " failed, ignore, probably already closed",
          controller.getFailed());
      }
    });
  }

  private void completeExceptionally(boolean closeScanner) {
    resultCache.clear();
    if (closeScanner) {
      closeScanner();
    }
    future.completeExceptionally(new RetriesExhaustedException(tries - 1, exceptions));
  }

  private void completeNoMoreResults() {
    future.complete(false);
  }

  private void completeWithNextStartRow(byte[] row, boolean inclusive) {
    scan.withStartRow(row, inclusive);
    future.complete(true);
  }

  private void completeWhenError(boolean closeScanner) {
    resultCache.clear();
    if (closeScanner) {
      closeScanner();
    }
    if (nextStartRowWhenError != null) {
      scan.withStartRow(nextStartRowWhenError, includeNextStartRowWhenError);
    }
    future.complete(true);
  }

  private void onError(Throwable error) {
    error = translateException(error);
    if (tries > startLogErrorsCnt) {
      LOG.warn("Call to " + loc.getServerName() + " for scanner id = " + scannerId + " for "
          + loc.getRegionInfo().getEncodedName() + " of " + loc.getRegionInfo().getTable()
          + " failed, , tries = " + tries + ", maxAttempts = " + maxAttempts + ", timeout = "
          + TimeUnit.NANOSECONDS.toMillis(scanTimeoutNs) + " ms, time elapsed = " + elapsedMs()
          + " ms",
        error);
    }
    boolean scannerClosed =
        error instanceof UnknownScannerException || error instanceof NotServingRegionException
            || error instanceof RegionServerStoppedException;
    RetriesExhaustedException.ThrowableWithExtraContext qt =
        new RetriesExhaustedException.ThrowableWithExtraContext(error,
            EnvironmentEdgeManager.currentTime(), "");
    exceptions.add(qt);
    if (tries >= maxAttempts) {
      completeExceptionally(!scannerClosed);
      return;
    }
    long delayNs;
    if (scanTimeoutNs > 0) {
      long maxDelayNs = scanTimeoutNs - (System.nanoTime() - nextCallStartNs);
      if (maxDelayNs <= 0) {
        completeExceptionally(!scannerClosed);
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNs, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNs, tries - 1);
    }
    if (scannerClosed) {
      completeWhenError(false);
      return;
    }
    if (error instanceof OutOfOrderScannerNextException || error instanceof ScannerResetException) {
      completeWhenError(true);
      return;
    }
    if (error instanceof DoNotRetryIOException) {
      completeExceptionally(true);
      return;
    }
    tries++;
    retryTimer.newTimeout(t -> call(), delayNs, TimeUnit.NANOSECONDS);
  }

  private void updateNextStartRowWhenError(Result result) {
    nextStartRowWhenError = result.getRow();
    includeNextStartRowWhenError = scan.getBatch() > 0 || result.isPartial();
  }

  private void completeWhenNoMoreResultsInRegion() {
    if (noMoreResultsForScan(scan, loc.getRegionInfo())) {
      completeNoMoreResults();
    } else {
      completeWithNextStartRow(loc.getRegionInfo().getEndKey(), true);
    }
  }

  private void completeReversedWhenNoMoreResultsInRegion() {
    if (noMoreResultsForReverseScan(scan, loc.getRegionInfo())) {
      completeNoMoreResults();
    } else {
      completeWithNextStartRow(loc.getRegionInfo().getStartKey(), false);
    }
  }

  private void onComplete(ScanResponse resp) {
    if (controller.failed()) {
      onError(controller.getFailed());
      return;
    }
    boolean isHeartbeatMessage = resp.hasHeartbeatMessage() && resp.getHeartbeatMessage();
    Result[] results;
    try {
      results = resultCache.addAndGet(
        Optional.ofNullable(ResponseConverter.getResults(controller.cellScanner(), resp))
            .orElse(ScanResultCache.EMPTY_RESULT_ARRAY),
        isHeartbeatMessage);
    } catch (IOException e) {
      // We can not retry here. The server has responded normally and the call sequence has been
      // increased so a new scan with the same call sequence will cause an
      // OutOfOrderScannerNextException. Let the upper layer open a new scanner.
      LOG.warn("decode scan response failed", e);
      completeWhenError(true);
      return;
    }

    boolean stopByUser;
    if (results.length == 0) {
      // if we have nothing to return then this must be a heartbeat message.
      stopByUser = !consumer.onHeartbeat();
    } else {
      updateNextStartRowWhenError(results[results.length - 1]);
      stopByUser = !consumer.onNext(results);
    }
    if (resp.hasMoreResults() && !resp.getMoreResults()) {
      // RS tells us there is no more data for the whole scan
      completeNoMoreResults();
      return;
    }
    if (stopByUser) {
      if (resp.getMoreResultsInRegion()) {
        // we have more results in region but user request to stop the scan, so we need to close the
        // scanner explicitly.
        closeScanner();
      }
      completeNoMoreResults();
      return;
    }
    // as in 2.0 this value will always be set
    if (!resp.getMoreResultsInRegion()) {
      completeWhenNoMoreResultsInRegion.run();
      return;
    }
    next();
  }

  private void call() {
    resetController(controller, rpcTimeoutNs);
    ScanRequest req = RequestConverter.buildScanRequest(scannerId, scan.getCaching(), false,
      nextCallSeq, false, false);
    stub.scan(controller, req, this::onComplete);
  }

  private void next() {
    nextCallSeq++;
    tries = 0;
    exceptions.clear();
    nextCallStartNs = System.nanoTime();
    call();
  }

  /**
   * @return {@code true} if we should continue, otherwise {@code false}.
   */
  public CompletableFuture<Boolean> start() {
    next();
    return future;
  }
}
