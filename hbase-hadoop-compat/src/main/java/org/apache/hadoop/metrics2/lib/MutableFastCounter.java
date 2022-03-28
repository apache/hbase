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

import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MutableFastCounter extends MutableCounter {

  private final LongAdder counter;

  protected MutableFastCounter(MetricsInfo info, long iVal) {
    super(info);
    counter = new LongAdder();
    counter.add(iVal);
  }

  @Override
  public void incr() {
    counter.increment();
    setChanged();
  }

  /**
   * Increment the value by a delta
   * @param delta of the increment
   */
  public void incr(long delta) {
    counter.add(delta);
    setChanged();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addCounter(info(), value());
      clearChanged();
    }
  }

  public long value() {
    return counter.sum();
  }
}
