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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

/**
 * A ResultScanner which will only send request to RS when there are no cached results when calling
 * next, just like the ResultScanner in the old time. Mainly used for writing UTs, that we can
 * control when to send request to RS. The default ResultScanner implementation will fetch in
 * background.
 */
@InterfaceAudience.Private
public class ScanPerNextResultScanner implements ResultScanner, AdvancedScanResultConsumer {

  private final AsyncTable<AdvancedScanResultConsumer> table;

  private final Scan scan;

  private final Queue<Result> queue = new ArrayDeque<>();

  private ScanMetrics scanMetrics;

  private boolean closed = false;

  private Throwable error;

  private ScanResumer resumer;

  public ScanPerNextResultScanner(AsyncTable<AdvancedScanResultConsumer> table, Scan scan) {
    this.table = table;
    this.scan = scan;
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

  @Override
  public synchronized void onNext(Result[] results, ScanController controller) {
    assert results.length > 0;
    if (closed) {
      controller.terminate();
      return;
    }
    for (Result result : results) {
      queue.add(result);
    }
    notifyAll();
    resumer = controller.suspend();
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
  public synchronized Result next() throws IOException {
    if (queue.isEmpty()) {
      if (resumer != null) {
        resumer.resume();
        resumer = null;
      } else {
        table.scan(scan, this);
      }
    }
    while (queue.isEmpty()) {
      if (closed) {
        return null;
      }
      if (error != null) {
        Throwables.propagateIfPossible(error, IOException.class);
        throw new IOException(error);
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }
    return queue.poll();
  }

  @Override
  public synchronized void close() {
    closed = true;
    queue.clear();
    if (resumer != null) {
      resumer.resume();
      resumer = null;
    }
    notifyAll();
  }

  @Override
  public boolean renewLease() {
    // The renew lease operation will be handled in background
    return false;
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }
}
