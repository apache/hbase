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

public class MetricsReplicationSourceSourceImpl implements MetricsReplicationSourceSource {

  private final MetricsReplicationSourceImpl rms;
  private final String id;
  private final String sizeOfLogQueueKey;
  private final String ageOfLastShippedOpKey;
  private final String logReadInEditsKey;
  private final String logEditsFilteredKey;
  private final String shippedBatchesKey;
  private final String shippedOpsKey;
  @Deprecated
  private final String shippedKBsKey;
  private final String shippedBytesKey;
  private final String logReadInBytesKey;

  private final MetricMutableGaugeLong ageOfLastShippedOpGauge;
  private long ageOfLastShipped; // Hadoop 1 metrics don't let you read from gauges
  private final MetricMutableGaugeLong sizeOfLogQueueGauge;
  private final MetricMutableCounterLong logReadInEditsCounter;
  private final MetricMutableCounterLong logEditsFilteredCounter;
  private final MetricMutableCounterLong shippedBatchesCounter;
  private final MetricMutableCounterLong shippedOpsCounter;
  private final MetricMutableCounterLong shippedKBsCounter;
  private final MetricMutableCounterLong shippedBytesCounter;
  private final MetricMutableCounterLong logReadInBytesCounter;

  public MetricsReplicationSourceSourceImpl(MetricsReplicationSourceImpl rms, String id) {
    this.rms = rms;
    this.id = id;

    ageOfLastShippedOpKey = "source." + id + ".ageOfLastShippedOp";
    ageOfLastShippedOpGauge = rms.getMetricsRegistry().getLongGauge(ageOfLastShippedOpKey, 0L);

    sizeOfLogQueueKey = "source." + id + ".sizeOfLogQueue";
    sizeOfLogQueueGauge = rms.getMetricsRegistry().getLongGauge(sizeOfLogQueueKey, 0L);

    shippedBatchesKey = "source." + this.id + ".shippedBatches";
    shippedBatchesCounter = rms.getMetricsRegistry().getLongCounter(shippedBatchesKey, 0L);

    shippedOpsKey = "source." + this.id + ".shippedOps";
    shippedOpsCounter = rms.getMetricsRegistry().getLongCounter(shippedOpsKey, 0L);

    shippedKBsKey = "source." + this.id + ".shippedKBs";
    shippedKBsCounter = rms.getMetricsRegistry().getLongCounter(shippedKBsKey, 0L);

    shippedBytesKey = "source." + this.id + ".shippedBytes";
    shippedBytesCounter = rms.getMetricsRegistry().getLongCounter(shippedBytesKey, 0L);

    logReadInBytesKey = "source." + this.id + ".logReadInBytes";
    logReadInBytesCounter = rms.getMetricsRegistry().getLongCounter(logReadInBytesKey, 0L);

    logReadInEditsKey = "source." + id + ".logEditsRead";
    logReadInEditsCounter = rms.getMetricsRegistry().getLongCounter(logReadInEditsKey, 0L);

    logEditsFilteredKey = "source." + id + ".logEditsFiltered";
    logEditsFilteredCounter = rms.getMetricsRegistry().getLongCounter(logEditsFilteredKey, 0L);
  }

  @Override public void setLastShippedAge(long age) {
    ageOfLastShippedOpGauge.set(age);
    ageOfLastShipped = age;
  }

  @Override public void setSizeOfLogQueue(int size) {
    sizeOfLogQueueGauge.set(size);
  }

  @Override public void incrSizeOfLogQueue(int size) {
    sizeOfLogQueueGauge.incr(size);
  }

  @Override public void decrSizeOfLogQueue(int size) {
    sizeOfLogQueueGauge.decr(size);
  }

  @Override public void incrLogReadInEdits(long size) {
    logReadInEditsCounter.incr(size);
  }

  @Override public void incrLogEditsFiltered(long size) {
    logEditsFilteredCounter.incr(size);
  }

  @Override public void incrBatchesShipped(int batches) {
    shippedBatchesCounter.incr(batches);
  }

  @Override public void incrOpsShipped(long ops) {
    shippedOpsCounter.incr(ops);
  }

  @Override public void incrShippedBytes(long size) {
    MetricsReplicationGlobalSourceSource
      .incrementKBsCounter(size, shippedBytesCounter, shippedKBsCounter);
  }

  @Override public void incrLogReadInBytes(long size) {
    logReadInBytesCounter.incr(size);
  }

  @Override public void clear() {
    rms.removeMetric(ageOfLastShippedOpKey);

    rms.removeMetric(sizeOfLogQueueKey);

    rms.removeMetric(shippedBatchesKey);
    rms.removeMetric(shippedOpsKey);
    rms.removeMetric(shippedKBsKey);

    rms.removeMetric(logReadInBytesKey);
    rms.removeMetric(logReadInEditsKey);

    rms.removeMetric(logEditsFilteredKey);
  }

  @Override
  public long getLastShippedAge() {
    return ageOfLastShipped;
  }
}
