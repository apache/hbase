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

package org.apache.hadoop.hbase.test;

import org.apache.hadoop.hbase.metrics.BaseMetricsSource;

/**
 *
 */
public interface MetricsAssertHelper {

  public void assertTag(String name, String expected, BaseMetricsSource source);

  public void assertGauge(String name, long expected, BaseMetricsSource source);

  public void assertGaugeGt(String name, long expected, BaseMetricsSource source);

  public void assertGaugeLt(String name, long expected, BaseMetricsSource source);

  public void assertGauge(String name, double expected, BaseMetricsSource source);

  public void assertGaugeGt(String name, double expected, BaseMetricsSource source);

  public void assertGaugeLt(String name, double expected, BaseMetricsSource source);

  public void assertCounter(String name, long expected, BaseMetricsSource source);

  public void assertCounterGt(String name, long expected, BaseMetricsSource source);

  public void assertCounterLt(String name, long expected, BaseMetricsSource source);

}
