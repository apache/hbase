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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

final class SimpleScanResultConsumer implements ScanResultConsumer {

  private ScanMetrics scanMetrics;

  private final List<Result> results = new ArrayList<>();

  private Throwable error;

  private boolean finished = false;

  @Override
  public void onScanMetricsCreated(ScanMetrics scanMetrics) {
    this.scanMetrics = scanMetrics;
  }

  @Override
  public synchronized boolean onNext(Result result) {
    results.add(result);
    return true;
  }

  @Override
  public synchronized void onError(Throwable error) {
    this.error = error;
    finished = true;
    notifyAll();
  }

  @Override
  public synchronized void onComplete() {
    finished = true;
    notifyAll();
  }

  public synchronized List<Result> getAll() throws Exception {
    while (!finished) {
      wait();
    }
    if (error != null) {
      Throwables.propagateIfPossible(error, Exception.class);
      throw new Exception(error);
    }
    return results;
  }

  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }
}
