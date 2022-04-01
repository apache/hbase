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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.trace.TraceUtil;

/**
 * A drop-in replacement for {@link BufferingScanResultConsumer} that adds tracing spans to its
 * implementation of the {@link AdvancedScanResultConsumer} API.
 */
public class TracedAdvancedScanResultConsumer implements AdvancedScanResultConsumer {

  private final BufferingScanResultConsumer delegate = new BufferingScanResultConsumer();

  @Override
  public void onScanMetricsCreated(ScanMetrics scanMetrics) {
    TraceUtil.trace(
      () -> delegate.onScanMetricsCreated(scanMetrics),
      "TracedAdvancedScanResultConsumer#onScanMetricsCreated");
  }

  @Override
  public void onNext(Result[] results, ScanController controller) {
    TraceUtil.trace(
      () -> delegate.onNext(results, controller),
      "TracedAdvancedScanResultConsumer#onNext");
  }

  @Override
  public void onError(Throwable error) {
    TraceUtil.trace(
      () -> delegate.onError(error),
      "TracedAdvancedScanResultConsumer#onError");
  }

  @Override
  public void onComplete() {
    TraceUtil.trace(delegate::onComplete, "TracedAdvancedScanResultConsumer#onComplete");
  }

  public Result take() throws IOException, InterruptedException {
    return delegate.take();
  }

  public ScanMetrics getScanMetrics() {
    return delegate.getScanMetrics();
  }
}
