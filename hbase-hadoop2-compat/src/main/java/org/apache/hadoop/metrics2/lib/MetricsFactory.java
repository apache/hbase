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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * A superclass of DynamicMetricsRegistry that creates metrics.
 * Needed for tags-based metrics to be able to create some metrics without registering them for
 * snapshots; and without having tons of null checks and copy-paste everywhere.
 * For this to be better, table, region and server metrics that are the same should be all
 * managed in one place, and in the same manner (see TO-DOs elsewhere).
 */
@InterfaceAudience.Private
public class MetricsFactory {

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableFastCounter newCounter(String name, String desc, long iVal) {
    return new MutableFastCounter(new MetricsInfoImpl(name, desc), iVal);
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableFastCounter newCounter(MetricsInfo info, long iVal) {
    return new MutableFastCounter(info, iVal);
  }

  /**
   * Create a mutable long integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeLong newGauge(MetricsInfo info, long iVal) {
    return new MutableGaugeLong(info, iVal);
  }

  /**
   * Create a new histogram.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableHistogram
   */
  public MutableHistogram newHistogram(String name, String desc) {
    return new MutableHistogram(name, desc);
  }

  /**
   * Create a new histogram with time range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableTimeHistogram
   */
  public MutableTimeHistogram newTimeHistogram(String name, String desc) {
    return new MutableTimeHistogram(name, desc);
  }

  /**
   * Create a new histogram with size range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableSizeHistogram
   */
  public MutableSizeHistogram newSizeHistogram(String name, String desc) {
    return new MutableSizeHistogram(name, desc);
  }
}
