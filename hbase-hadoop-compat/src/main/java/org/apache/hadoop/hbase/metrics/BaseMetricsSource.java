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

/**
 *   BaseMetricsSource for dynamic metrics to announce to Metrics2
 */
public interface BaseMetricsSource {

  /**
   * Set a gauge to a specific value.
   *
   * @param gaugeName the name of the gauge
   * @param value     the value
   */
  public void setGauge(String gaugeName, long value);

  /**
   * Add some amount to a gauge.
   *
   * @param gaugeName the name of the gauge
   * @param delta     the amount to change the gauge by.
   */
  public void incGauge(String gaugeName, long delta);

  /**
   * Subtract some amount from a gauge.
   *
   * @param gaugeName the name of the gauge
   * @param delta     the amount to change the gauge by.
   */
  public void decGauge(String gaugeName, long delta);

  /**
   * Remove a gauge and no longer announce it.
   *
   * @param key Name of the gauge to remove.
   */
  public void removeGauge(String key);

  /**
   * Add some amount to a counter.
   *
   * @param counterName the name of the counter
   * @param delta       the amount to change the counter by.
   */
  public void incCounters(String counterName, long delta);

  /**
   * Remove a counter and stop announcing it to metrics2.
   *
   * @param key
   */
  public void removeCounter(String key);

}
