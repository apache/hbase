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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A metric which encompasses a {@link Histogram} and {@link Meter}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Timer extends Metric {
  /**
   * Update the timer with the given duration in given time unit.
   * @param duration the duration of the event
   * @param unit the time unit for the duration
   */
  void update(long duration, TimeUnit unit);

  /**
   * Update the timer with the given duration in milliseconds
   * @param durationMillis the duration of the event in ms
   */
  default void updateMillis(long durationMillis) {
    update(durationMillis, TimeUnit.NANOSECONDS);
  }

  /**
   * Update the timer with the given duration in microseconds
   * @param durationMicros the duration of the event in microseconds
   */
  default void updateMicros(long durationMicros) {
    update(durationMicros, TimeUnit.MICROSECONDS);
  }

  /**
   * Update the timer with the given duration in nanoseconds
   * @param durationNanos the duration of the event in ns
   */
  default void updateNanos(long durationNanos) {
    update(durationNanos, TimeUnit.NANOSECONDS);
  }

  @InterfaceAudience.Private
  Histogram getHistogram();

  @InterfaceAudience.Private
  Meter getMeter();
}
