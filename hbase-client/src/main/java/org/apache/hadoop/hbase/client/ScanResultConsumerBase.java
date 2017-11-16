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

import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The base interface for scan result consumer.
 */
@InterfaceAudience.Public
public interface ScanResultConsumerBase {
  /**
   * Indicate that we hit an unrecoverable error and the scan operation is terminated.
   * <p>
   * We will not call {@link #onComplete()} after calling {@link #onError(Throwable)}.
   */
  void onError(Throwable error);

  /**
   * Indicate that the scan operation is completed normally.
   */
  void onComplete();

  /**
   * If {@code scan.isScanMetricsEnabled()} returns true, then this method will be called prior to
   * all other methods in this interface to give you the {@link ScanMetrics} instance for this scan
   * operation. The {@link ScanMetrics} instance will be updated on-the-fly during the scan, you can
   * store it somewhere to get the metrics at any time if you want.
   */
  default void onScanMetricsCreated(ScanMetrics scanMetrics) {
  }
}
