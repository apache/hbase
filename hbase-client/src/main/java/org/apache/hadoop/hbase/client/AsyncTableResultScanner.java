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

import static org.apache.hadoop.hbase.client.ConnectionUtils.calcEstimatedSize;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ResultScanner} implementation for {@link AsyncTable}. It will fetch data automatically
 * in background and cache it in memory. Typically the {@link #maxCacheSize} will be
 * {@code 2 * scan.getMaxResultSize()}.
 */
@InterfaceAudience.Private
class AsyncTableResultScanner implements ResultScanner, AdvancedScanResultConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncTableResultScanner.class);

  private final AsyncTable<AdvancedScanResultConsumer> rawTable;

  private final long maxCacheSize;

  private final Scan scan;

  private final Queue<Result> queue = new ArrayDeque<>();

  private ScanMetrics scanMetrics;

  private long cacheSize;

  private boolean closed = false;

  private Throwable error;

  private ScanResumer resumer;

  public AsyncTableResultScanner(AsyncTable<AdvancedScanResultConsumer> table, Scan scan,
      long maxCacheSize) {
    this.rawTable = table;
    this.maxCacheSize = maxCacheSize;
    this.scan = scan;
    table.scan(scan, this);
  }

  private void addToCache(Result result) {
    queue.add(result);
    cacheSize += calcEstimatedSize(result);
  }

  private void stopPrefetch(ScanController controller) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("0x%x", System.identityHashCode(this)) +
        " stop prefetching when scanning " + rawTable.getName() + " as the cache size " +
        cacheSize + " is greater than the maxCacheSize " + maxCacheSize);
    }
    resumer = controller.suspend();
  }

  @Override
  public synchronized void onNext(Result[] results, ScanController controller) {
    assert results.length > 0;
    if (closed) {
      controller.terminate();
      return;
    }
    for (Result result : results) {
      addToCache(result);
    }
    notifyAll();
    if (cacheSize >= maxCacheSize) {
      stopPrefetch(controller);
    }
  }

  @Override
  public synchronized void onHeartbeat(ScanController controller) {
    if (closed) {
      controller.terminate();
      return;
    }
    if (scan.isNeedCursorResult()) {
      controller.cursor().ifPresent(c -> queue.add(Result.createCursorResult(c)));
    }
  }

  @Override
  public synchronized void onError(Throwable error) {
    this.error = error;
    notifyAll();
  }

  @Override
  public synchronized void onComplete() {
    closed = true;
    notifyAll();
  }

  @Override
  public void onScanMetricsCreated(ScanMetrics scanMetrics) {
    this.scanMetrics = scanMetrics;
  }

  private void resumePrefetch() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("0x%x", System.identityHashCode(this)) + " resume prefetching");
    }
    resumer.resume();
    resumer = null;
  }

  @Override
  public synchronized Result next() throws IOException {
    while (queue.isEmpty()) {
      if (closed) {
        return null;
      }
      if (error != null) {
        FutureUtils.rethrow(error);
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }
    Result result = queue.poll();
    if (!result.isCursor()) {
      cacheSize -= calcEstimatedSize(result);
      if (resumer != null && cacheSize <= maxCacheSize / 2) {
        resumePrefetch();
      }
    }
    return result;
  }

  @Override
  public synchronized void close() {
    closed = true;
    queue.clear();
    cacheSize = 0;
    if (resumer != null) {
      resumePrefetch();
    }
    notifyAll();
  }

  @Override
  public boolean renewLease() {
    // we will do prefetching in the background and if there is no space we will just suspend the
    // scanner. The renew lease operation will be handled in the background.
    return false;
  }

  // used in tests to test whether the scanner has been suspended
  synchronized boolean isSuspended() {
    return resumer != null;
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }
}
