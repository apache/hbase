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

import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

public class MetricsReplicationSinkSourceImpl implements MetricsReplicationSinkSource {

  private final MutableGaugeLong ageGauge;
  private final MutableCounterLong batchesCounter;
  private final MutableCounterLong opsCounter;
  private final MutableCounterLong hfilesCounter;

  public MetricsReplicationSinkSourceImpl(MetricsReplicationSourceImpl rms) {
    ageGauge = rms.getMetricsRegistry().getLongGauge(SINK_AGE_OF_LAST_APPLIED_OP, 0L);
    batchesCounter = rms.getMetricsRegistry().getLongCounter(SINK_APPLIED_BATCHES, 0L);
    opsCounter = rms.getMetricsRegistry().getLongCounter(SINK_APPLIED_OPS, 0L);
    hfilesCounter = rms.getMetricsRegistry().getLongCounter(SINK_APPLIED_HFILES, 0L);
  }

  @Override public void setLastAppliedOpAge(long age) {
    ageGauge.set(age);
  }

  @Override public void incrAppliedBatches(long batches) {
    batchesCounter.incr(batches);
  }

  @Override public void incrAppliedOps(long batchsize) {
    opsCounter.incr(batchsize);
  }

  @Override
  public long getLastAppliedOpAge() {
    return ageGauge.value();
  }

  @Override
  public void incrAppliedHFiles(long hfiles) {
    hfilesCounter.incr(hfiles);
  }
}
