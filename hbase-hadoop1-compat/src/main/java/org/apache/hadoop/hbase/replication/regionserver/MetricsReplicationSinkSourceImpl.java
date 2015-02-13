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

package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;

public class MetricsReplicationSinkSourceImpl implements MetricsReplicationSinkSource {

  private final MetricMutableGaugeLong ageGauge;
  private long ageOfLastApplied; // Hadoop 1 metrics don't let you read from gauges
  private final MetricMutableCounterLong batchesCounter;
  private final MetricMutableCounterLong opsCounter;

  public MetricsReplicationSinkSourceImpl(MetricsReplicationSourceImpl rms) {
    ageGauge = rms.getMetricsRegistry().getLongGauge(SINK_AGE_OF_LAST_APPLIED_OP, 0L);
    batchesCounter = rms.getMetricsRegistry().getLongCounter(SINK_APPLIED_BATCHES, 0L);
    opsCounter = rms.getMetricsRegistry().getLongCounter(SINK_APPLIED_OPS, 0L);
  }

  @Override public void setLastAppliedOpAge(long age) {
    ageGauge.set(age);
    ageOfLastApplied = age;
  }

  @Override public void incrAppliedBatches(long batches) {
    batchesCounter.incr(batches);
  }

  @Override public void incrAppliedOps(long batchsize) {
    opsCounter.incr(batchsize);
  }

  @Override
  public long getLastAppliedOpAge() {
    return ageOfLastApplied;
  }
}
