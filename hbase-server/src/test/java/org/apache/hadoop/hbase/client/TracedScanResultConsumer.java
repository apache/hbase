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

import java.util.List;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.trace.TraceUtil;

/**
 * A wrapper over {@link SimpleScanResultConsumer} that adds tracing of spans to its
 * implementation.
 */
class TracedScanResultConsumer implements SimpleScanResultConsumer {

  private final SimpleScanResultConsumer delegate;

  public TracedScanResultConsumer(final SimpleScanResultConsumer delegate) {
    this.delegate = delegate;
  }

  @Override
  public void onScanMetricsCreated(ScanMetrics scanMetrics) {
    TraceUtil.trace(
      () -> delegate.onScanMetricsCreated(scanMetrics),
      "TracedScanResultConsumer#onScanMetricsCreated");
  }

  @Override
  public boolean onNext(Result result) {
    return TraceUtil.trace(() -> delegate.onNext(result),
      "TracedScanResultConsumer#onNext");
  }

  @Override
  public void onError(Throwable error) {
    TraceUtil.trace(() -> delegate.onError(error), "TracedScanResultConsumer#onError");
  }

  @Override
  public void onComplete() {
    TraceUtil.trace(delegate::onComplete, "TracedScanResultConsumer#onComplete");
  }

  @Override
  public List<Result> getAll() throws Exception {
    return delegate.getAll();
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return delegate.getScanMetrics();
  }
}
