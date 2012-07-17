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

/**
 * Factory providing static methods to create MutableMetrics classes.
 * HBase uses this class rather than MetricsRegistry because MetricsRegistry does not
 * allow metrics to be removed.
 */
public class HBaseMetricsFactory {

  /**
   * Create a new gauge
   * @param name Name of the gauge
   * @param desc Description of the gauge
   * @param startingValue The starting value
   * @return a new MutableGaugeLong that has a starting value.
   */
  public static MutableGaugeLong newGauge(String name, String desc, long startingValue) {
    return new MutableGaugeLong(Interns.info(name, desc), startingValue);
  }

  /**
   * Create a new counter.
   * @param name Name of the counter.
   * @param desc Description of the counter.
   * @param startingValue The starting value.
   * @return a new MutableCounterLong that has a starting value.
   */
  public static MutableCounterLong newCounter(String name, String desc, long startingValue) {
    return new MutableCounterLong(Interns.info(name, desc), startingValue);
  }

}
