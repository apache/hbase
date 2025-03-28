/*
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
package org.apache.hadoop.hbase.client.metrics;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Provides server side metrics related to scan operations.
 */
@InterfaceAudience.Public
@SuppressWarnings("checkstyle:VisibilityModifier") // See HBASE-27757
public class ServerSideScanMetrics extends ServerSideMetricsCounter {

  /**
   * number of rows filtered during scan RPC
   */
  public final AtomicLong countOfRowsFiltered =
    createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);

  /**
   * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
   * since rows may be filtered.
   */
  public final AtomicLong countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);

  public final AtomicLong countOfBlockBytesScanned =
    createCounter(BLOCK_BYTES_SCANNED_KEY_METRIC_NAME);

  public final AtomicLong fsReadTime = createCounter(FS_READ_TIME_METRIC_NAME);
}
