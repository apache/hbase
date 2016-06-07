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

public class MetricsReplicationSourceSourceImpl implements MetricsReplicationSourceSource {

  private final MetricsReplicationSourceImpl rms;
  private final String id;
  private final String sizeOfLogQueueKey;
  private final String ageOfLastShippedOpKey;
  private final String logReadInEditsKey;
  private final String logEditsFilteredKey;
  private final String shippedBatchesKey;
  private final String shippedOpsKey;
  private String keyPrefix;

  @Deprecated
  private final String shippedKBsKey;
  private final String shippedBytesKey;
  private final String logReadInBytesKey;

  private final MutableGaugeLong ageOfLastShippedOpGauge;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableCounterLong logReadInEditsCounter;
  private final MutableCounterLong logEditsFilteredCounter;
  private final MutableCounterLong shippedBatchesCounter;
  private final MutableCounterLong shippedOpsCounter;
  private final MutableCounterLong shippedKBsCounter;
  private final MutableCounterLong shippedBytesCounter;
  private final MutableCounterLong logReadInBytesCounter;

  private final String unknownFileLengthKey;
  private final String uncleanlyClosedKey;
  private final String uncleanlySkippedBytesKey;
  private final String restartedKey;
  private final String repeatedBytesKey;
  private final String completedLogsKey;
  private final String completedRecoveryKey;
  private final MutableCounterLong unknownFileLengthForClosedWAL;
  private final MutableCounterLong uncleanlyClosedWAL;
  private final MutableCounterLong uncleanlyClosedSkippedBytes;
  private final MutableCounterLong restartWALReading;
  private final MutableCounterLong repeatedFileBytes;
  private final MutableCounterLong completedWAL;
  private final MutableCounterLong completedRecoveryQueue;

  public MetricsReplicationSourceSourceImpl(MetricsReplicationSourceImpl rms, String id) {
    this.rms = rms;
    this.id = id;
    this.keyPrefix = "source." + this.id + ".";

    ageOfLastShippedOpKey = this.keyPrefix + "ageOfLastShippedOp";
    ageOfLastShippedOpGauge = rms.getMetricsRegistry().getLongGauge(ageOfLastShippedOpKey, 0L);

    sizeOfLogQueueKey = this.keyPrefix + "sizeOfLogQueue";
    sizeOfLogQueueGauge = rms.getMetricsRegistry().getLongGauge(sizeOfLogQueueKey, 0L);

    shippedBatchesKey = this.keyPrefix + "shippedBatches";
    shippedBatchesCounter = rms.getMetricsRegistry().getLongCounter(shippedBatchesKey, 0L);

    shippedOpsKey = this.keyPrefix + "shippedOps";
    shippedOpsCounter = rms.getMetricsRegistry().getLongCounter(shippedOpsKey, 0L);

    shippedKBsKey = this.keyPrefix + "shippedKBs";
    shippedKBsCounter = rms.getMetricsRegistry().getLongCounter(shippedKBsKey, 0L);

    shippedBytesKey = this.keyPrefix + "shippedBytes";
    shippedBytesCounter = rms.getMetricsRegistry().getLongCounter(shippedBytesKey, 0L);

    logReadInBytesKey = this.keyPrefix + "logReadInBytes";
    logReadInBytesCounter = rms.getMetricsRegistry().getLongCounter(logReadInBytesKey, 0L);

    logReadInEditsKey = this.keyPrefix + "logEditsRead";
    logReadInEditsCounter = rms.getMetricsRegistry().getLongCounter(logReadInEditsKey, 0L);

    logEditsFilteredKey = this.keyPrefix + "logEditsFiltered";
    logEditsFilteredCounter = rms.getMetricsRegistry().getLongCounter(logEditsFilteredKey, 0L);

    unknownFileLengthKey = this.keyPrefix + "closedLogsWithUnknownFileLength";
    unknownFileLengthForClosedWAL = rms.getMetricsRegistry().getLongCounter(unknownFileLengthKey, 0L);

    uncleanlyClosedKey = this.keyPrefix + "uncleanlyClosedLogs";
    uncleanlyClosedWAL = rms.getMetricsRegistry().getLongCounter(uncleanlyClosedKey, 0L);

    uncleanlySkippedBytesKey = this.keyPrefix + "ignoredUncleanlyClosedLogContentsInBytes";
    uncleanlyClosedSkippedBytes = rms.getMetricsRegistry().getLongCounter(uncleanlySkippedBytesKey, 0L);

    restartedKey = this.keyPrefix + "restartedLogReading";
    restartWALReading = rms.getMetricsRegistry().getLongCounter(restartedKey, 0L);

    repeatedBytesKey = this.keyPrefix + "repeatedLogFileBytes";
    repeatedFileBytes = rms.getMetricsRegistry().getLongCounter(repeatedBytesKey, 0L);

    completedLogsKey = this.keyPrefix + "completedLogs";
    completedWAL = rms.getMetricsRegistry().getLongCounter(completedLogsKey, 0L);

    completedRecoveryKey = this.keyPrefix + "completedRecoverQueues";
    completedRecoveryQueue = rms.getMetricsRegistry().getLongCounter(completedRecoveryKey, 0L);
  }

  @Override public void setLastShippedAge(long age) {
    ageOfLastShippedOpGauge.set(age);
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
    rms.removeMetric(shippedBytesKey);

    rms.removeMetric(logReadInBytesKey);
    rms.removeMetric(logReadInEditsKey);

    rms.removeMetric(logEditsFilteredKey);

    rms.removeMetric(unknownFileLengthKey);
    rms.removeMetric(uncleanlyClosedKey);
    rms.removeMetric(uncleanlySkippedBytesKey);
    rms.removeMetric(restartedKey);
    rms.removeMetric(repeatedBytesKey);
    rms.removeMetric(completedLogsKey);
    rms.removeMetric(completedRecoveryKey);
  }

  @Override
  public long getLastShippedAge() {
    return ageOfLastShippedOpGauge.value();
  }

  @Override
  public void incrUnknownFileLengthForClosedWAL() {
    unknownFileLengthForClosedWAL.incr(1L);
  }

  @Override
  public void incrUncleanlyClosedWALs() {
    uncleanlyClosedWAL.incr(1L);
  }

  @Override
  public void incrBytesSkippedInUncleanlyClosedWALs(final long bytes) {
    uncleanlyClosedSkippedBytes.incr(bytes);
  }

  @Override
  public void incrRestartedWALReading() {
    restartWALReading.incr(1L);
  }

  @Override
  public void incrRepeatedFileBytes(final long bytes) {
    repeatedFileBytes.incr(bytes);
  }

  @Override
  public void incrCompletedWAL() {
    completedWAL.incr(1L);
  }

  @Override
  public void incrCompletedRecoveryQueue() {
    completedRecoveryQueue.incr(1L);
  }

  @Override
  public void init() {
    rms.init();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    rms.setGauge(this.keyPrefix + gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    rms.incGauge(this.keyPrefix + gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    rms.decGauge(this.keyPrefix + gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    rms.removeMetric(this.keyPrefix + key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    rms.incCounters(this.keyPrefix + counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    rms.updateHistogram(this.keyPrefix + name, value);
  }

  @Override
  public void updateQuantile(String name, long value) {
    rms.updateQuantile(this.keyPrefix + name, value);
  }

  @Override
  public String getMetricsContext() {
    return rms.getMetricsContext();
  }

  @Override
  public String getMetricsDescription() {
    return rms.getMetricsDescription();
  }

  @Override
  public String getMetricsJmxContext() {
    return rms.getMetricsJmxContext();
  }

  @Override
  public String getMetricsName() {
    return rms.getMetricsName();
  }
}
