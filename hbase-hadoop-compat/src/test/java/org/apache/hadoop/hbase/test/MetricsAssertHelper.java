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

import org.apache.hadoop.hbase.metrics.BaseSource;

/** Interface of a class to make assertions about metrics values. */
public interface MetricsAssertHelper {

  /**
   * Init helper.  This method will make sure that the metrics system is set
   * up for tests.
   */
  void init();

  /**
   * Assert that a tag exists and has a given value.
   *
   * @param name     The name of the tag.
   * @param expected The expected value
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertTag(String name, String expected, BaseSource source);

  /**
   * Assert that a gauge exists and that it's value is equal to the expected value.
   *
   * @param name     The name of the gauge
   * @param expected The expected value of the gauge.
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGauge(String name, long expected, BaseSource source);

  /**
   * Assert that a gauge exists and it's value is greater than a given value
   *
   * @param name     The name of the gauge
   * @param expected Value that the gauge is expected to be greater than
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGaugeGt(String name, long expected, BaseSource source);

  /**
   * Assert that a gauge exists and it's value is less than a given value
   *
   * @param name     The name of the gauge
   * @param expected Value that the gauge is expected to be less than
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGaugeLt(String name, long expected, BaseSource source);

  /**
   * Assert that a gauge exists and that it's value is equal to the expected value.
   *
   * @param name     The name of the gauge
   * @param expected The expected value of the gauge.
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGauge(String name, double expected, BaseSource source);

  /**
   * Assert that a gauge exists and it's value is greater than a given value
   *
   * @param name     The name of the gauge
   * @param expected Value that the gauge is expected to be greater than
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGaugeGt(String name, double expected, BaseSource source);

  /**
   * Assert that a gauge exists and it's value is less than a given value
   *
   * @param name     The name of the gauge
   * @param expected Value that the gauge is expected to be less than
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertGaugeLt(String name, double expected, BaseSource source);

  /**
   * Assert that a counter exists and that it's value is equal to the expected value.
   *
   * @param name     The name of the counter.
   * @param expected The expected value
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertCounter(String name, long expected, BaseSource source);

  /**
   * Assert that a counter exists and that it's value is greater than the given value.
   *
   * @param name     The name of the counter.
   * @param expected The value the counter is expected to be greater than.
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertCounterGt(String name, long expected, BaseSource source);

  /**
   * Assert that a counter exists and that it's value is less than the given value.
   *
   * @param name     The name of the counter.
   * @param expected The value the counter is expected to be less than.
   * @param source   The BaseSource{@link BaseSource} that will provide the tags,
   *                 gauges, and counters.
   */
  void assertCounterLt(String name, long expected, BaseSource source);

  /**
   * Get the value of a counter.
   *
   * @param name   name of the counter.
   * @param source The BaseSource{@link BaseSource} that will provide the tags,
   *               gauges, and counters.
   * @return long value of the counter.
   */
  long getCounter(String name, BaseSource source);

  /**
   * Check if a dynamic counter exists.
   *
   * @param name   name of the counter.
   * @param source The BaseSource{@link BaseSource} that will provide the tags,
   *               gauges, and counters.
   * @return boolean true id counter metric exists.
   */
  boolean checkCounterExists(String name, BaseSource source);

  /**
   * Get the value of a gauge as a double.
   *
   * @param name   name of the gauge.
   * @param source The BaseSource{@link BaseSource} that will provide the tags,
   *               gauges, and counters.
   * @return double value of the gauge.
   */
  double getGaugeDouble(String name, BaseSource source);

  /**
   * Get the value of a gauge as a long.
   *
   * @param name   name of the gauge.
   * @param source The BaseSource{@link BaseSource} that will provide the tags,
   *               gauges, and counters.
   * @return long value of the gauge.
   */
  long getGaugeLong(String name, BaseSource source);
}
