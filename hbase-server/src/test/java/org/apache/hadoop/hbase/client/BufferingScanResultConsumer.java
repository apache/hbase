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

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * A scan result consumer which buffers all the data in memory and you can call the {@link #take()}
 * method below to get the result one by one. Should only be used by tests, do not write production
 * code like this as the buffer is unlimited and may cause OOM.
 */
class BufferingScanResultConsumer implements AdvancedScanResultConsumer {

  private ScanMetrics scanMetrics;

  private final Queue<Result> queue = new ArrayDeque<>();

  private boolean finished;

  private Throwable error;

  @Override
  public void onScanMetricsCreated(ScanMetrics scanMetrics) {
    this.scanMetrics = scanMetrics;
  }

  @Override
  public synchronized void onNext(Result[] results, ScanController controller) {
    for (Result result : results) {
      queue.offer(result);
    }
    notifyAll();
  }

  @Override
  public synchronized void onError(Throwable error) {
    finished = true;
    this.error = error;
    notifyAll();
  }

  @Override
  public synchronized void onComplete() {
    finished = true;
    notifyAll();
  }

  public synchronized Result take() throws IOException, InterruptedException {
    for (;;) {
      if (!queue.isEmpty()) {
        return queue.poll();
      }
      if (finished) {
        if (error != null) {
          Throwables.propagateIfPossible(error, IOException.class);
          throw new IOException(error);
        } else {
          return null;
        }
      }
      wait();
    }
  }

  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }
}
