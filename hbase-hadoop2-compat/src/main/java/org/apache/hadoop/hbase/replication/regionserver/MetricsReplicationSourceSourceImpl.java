/*
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
  private final String keyPrefix;
  private final String sizeOfLogQueueKey;
  private final String ageOfLastShippedOpKey;
  private final String logReadInEditsKey;
  private final String logEditsFilteredKey;
  private final String shippedBatchesKey;
  private final String shippedOpsKey;
  private final String shippedKBsKey;
  private final String logReadInBytesKey;

  private final MutableGaugeLong ageOfLastShippedOpGauge;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableCounterLong logReadInEditsCounter;
  private final MutableCounterLong logEditsFilteredCounter;
  private final MutableCounterLong shippedBatchesCounter;
  private final MutableCounterLong shippedOpsCounter;
  private final MutableCounterLong shippedKBsCounter;
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

    logReadInBytesKey = "source." + this.id + ".logReadInBytes";
    logReadInBytesCounter = rms.getMetricsRegistry().getLongCounter(logReadInBytesKey, 0L);

    logReadInEditsKey = "source." + id + ".logEditsRead";
    logReadInEditsCounter = rms.getMetricsRegistry().getLongCounter(logReadInEditsKey, 0L);

    logEditsFilteredKey = "source." + id + ".logEditsFiltered";
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

  @Override public void incrShippedKBs(long size) {
    shippedKBsCounter.incr(size);
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
}
