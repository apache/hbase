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
package org.apache.hadoop.hbase.metrics.impl;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Custom implementation of {@link Timer}.
 */
@InterfaceAudience.Private
public class TimerImpl implements Timer {
  private final HistogramImpl histogram;
  private final DropwizardMeter meter;

  // track time events in micros
  private static final TimeUnit DEFAULT_UNIT = TimeUnit.MICROSECONDS;

  public TimerImpl() {
    this.histogram = new HistogramImpl();
    this.meter = new DropwizardMeter();
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    if (duration >= 0) {
      histogram.update(DEFAULT_UNIT.convert(duration, unit));
      meter.mark();
    }
  }

  @Override
  public HistogramImpl getHistogram() {
    return histogram;
  }

  @Override
  public DropwizardMeter getMeter() {
    return meter;
  }
}
