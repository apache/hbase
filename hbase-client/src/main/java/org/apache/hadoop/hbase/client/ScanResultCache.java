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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Used to separate the row constructing logic.
 * <p>
 * After we add heartbeat support for scan, RS may return partial result even if allowPartial is
 * false and batch is 0. With this interface, the implementation now looks like:
 * <ol>
 * <li>Get results from ScanResponse proto.</li>
 * <li>Pass them to ScanResultCache and get something back.</li>
 * <li>If we actually get something back, then pass it to ScanObserver.</li>
 * </ol>
 */
@InterfaceAudience.Private
interface ScanResultCache {

  static final Result[] EMPTY_RESULT_ARRAY = new Result[0];

  /**
   * Add the given results to cache and get valid results back.
   * @param results the results of a scan next. Must not be null.
   * @param isHeartbeatMessage indicate whether the results is gotten from a heartbeat response.
   * @return valid results, never null.
   */
  Result[] addAndGet(Result[] results, boolean isHeartbeatMessage) throws IOException;

  /**
   * Clear the cached result if any. Called when scan error and we will start from a start of a row
   * again.
   */
  void clear();
}
