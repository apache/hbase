/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;

/**
 * <p>
 * Used by the {@link StoreScanner} to aggregate KeyValue instances to form a
 * single row/column. Implementations of this interface can expect the following
 * call sequence:
 * </p>
 * <ul>
 *   <li>reset(): When the scanner starts the row/col</li>
 *   <li>process(): This is called for each included KeyValue as determined by
 *     {@link ScanQueryMatcher}. The aggregator can decide to emit an intermediate
 *     KeyValue.
 *   </li>
 *   <li>nextAction(): The scanner will decide the next action by calling this if
 *     a call to process() returns a non-null value.
 *   </li>
 *   <li>finalizeKeyValues(): The aggregator can manipulate the final row/column
 *     contents that will be returned by the scanner. This lets it use the
 *     intermediate KeyValue instances returned till then and any accumulated state
 *     at the end to create the final row.
 *   </li>
 * </ul>
 */
public interface KeyValueAggregator {
  /**
   * Called when the {@link StoreScanner} starts its attempt to get a row/col.
   */
  public void reset();

  /**
   * Make the aggregator process a single KeyValue.
   * @param kv
   * @return
   */
  public KeyValue process(KeyValue kv);

  /**
   * Called if the previous call to
   * {@link #process(org.apache.hadoop.hbase.KeyValue)} had a non-null
   * return value.
   * @param origCode
   * @return
   */
  public ScanQueryMatcher.MatchCode nextAction(
    ScanQueryMatcher.MatchCode origCode);

  /**
   * Called at the end of the scan to flush out any remaining KeyValue.
   */
  public KeyValue finalizeKeyValues();
}
