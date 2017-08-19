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
package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A metric which measures the distribution of values.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Histogram extends Metric {

  /**
   * Adds a new value to the distribution.
   *
   * @param value The value to add
   */
  void update(int value);

  /**
   * Adds a new value to the distribution.
   *
   * @param value The value to add
   */
  void update(long value);

  /**
   * Return the total number of values added to the histogram.
   * @return the total number of values.
   */
  long getCount();

  /**
   * Snapshot the current values in the Histogram
   * @return a Snapshot of the distribution.
   */
  @InterfaceAudience.Private
  Snapshot snapshot();

}
