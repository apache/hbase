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

import org.apache.hadoop.metrics2.lib.MutableFastCounter;
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
  @Deprecated
  private final String shippedKBsKey;
  private final String shippedBytesKey;
  private final String logReadInBytesKey;
  private final String shippedHFilesKey;
  private final String sizeOfHFileRefsQueueKey;

  private final MutableGaugeLong ageOfLastShippedOpGauge;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableFastCounter logReadInEditsCounter;
  private final MutableFastCounter logEditsFilteredCounter;
  private final MutableFastCounter shippedBatchesCounter;
  private final MutableFastCounter shippedOpsCounter;
  private final MutableFastCounter shippedKBsCounter;
  private final MutableFastCounter shippedBytesCounter;
  private final MutableFastCounter logReadInBytesCounter;
  private final MutableFastCounter shippedHFilesCounter;
  private final MutableGaugeLong sizeOfHFileRefsQueueGauge;

  private final String unknownFileLengthKey;
  private final String uncleanlyClosedKey;
  private final String uncleanlySkippedBytesKey;
  private final String restartedKey;
  private final String repeatedBytesKey;
  private final String completedLogsKey;
  private final String completedRecoveryKey;
  private final MutableFastCounter unknownFileLengthForClosedWAL;
  private final MutableFastCounter uncleanlyClosedWAL;
  private final MutableFastCounter uncleanlyClosedSkippedBytes;
  private final MutableFastCounter restartWALReading;
  private final MutableFastCounter repeatedFileBytes;
  private final MutableFastCounter completedWAL;
  private final MutableFastCounter completedRecoveryQueue;

  public MetricsReplicationSourceSourceImpl(MetricsReplicationSourceImpl rms, String id) {
    this.rms = rms;
    this.id = id;
    this.keyPrefix = "source." + this.id + ".";

    ageOfLastShippedOpKey = "source." + id + ".ageOfLastShippedOp";
    ageOfLastShippedOpGauge = rms.getMetricsRegistry().getGauge(ageOfLastShippedOpKey, 0L);

    sizeOfLogQueueKey = "source." + id + ".sizeOfLogQueue";
    sizeOfLogQueueGauge = rms.getMetricsRegistry().getGauge(sizeOfLogQueueKey, 0L);

    shippedBatchesKey = "source." + this.id + ".shippedBatches";
    shippedBatchesCounter = rms.getMetricsRegistry().getCounter(shippedBatchesKey, 0L);

    shippedOpsKey = "source." + this.id + ".shippedOps";
    shippedOpsCounter = rms.getMetricsRegistry().getCounter(shippedOpsKey, 0L);

    shippedKBsKey = "source." + this.id + ".shippedKBs";
    shippedKBsCounter = rms.getMetricsRegistry().getCounter(shippedKBsKey, 0L);

    shippedBytesKey = "source." + this.id + ".shippedBytes";
    shippedBytesCounter = rms.getMetricsRegistry().getCounter(shippedBytesKey, 0L);

    logReadInBytesKey = "source." + this.id + ".logReadInBytes";
    logReadInBytesCounter = rms.getMetricsRegistry().getCounter(logReadInBytesKey, 0L);

    logReadInEditsKey = "source." + id + ".logEditsRead";
    logReadInEditsCounter = rms.getMetricsRegistry().getCounter(logReadInEditsKey, 0L);

    logEditsFilteredKey = "source." + id + ".logEditsFiltered";
    logEditsFilteredCounter = rms.getMetricsRegistry().getCounter(logEditsFilteredKey, 0L);

    shippedHFilesKey = "source." + this.id + ".shippedHFiles";
    shippedHFilesCounter = rms.getMetricsRegistry().getCounter(shippedHFilesKey, 0L);

    sizeOfHFileRefsQueueKey = "source." + id + ".sizeOfHFileRefsQueue";
    sizeOfHFileRefsQueueGauge = rms.getMetricsRegistry().getGauge(sizeOfHFileRefsQueueKey, 0L);

    unknownFileLengthKey = this.keyPrefix + "closedLogsWithUnknownFileLength";
    unknownFileLengthForClosedWAL = rms.getMetricsRegistry().getCounter(unknownFileLengthKey, 0L);

    uncleanlyClosedKey = this.keyPrefix + "uncleanlyClosedLogs";
    uncleanlyClosedWAL = rms.getMetricsRegistry().getCounter(uncleanlyClosedKey, 0L);

    uncleanlySkippedBytesKey = this.keyPrefix + "ignoredUncleanlyClosedLogContentsInBytes";
    uncleanlyClosedSkippedBytes = rms.getMetricsRegistry().getCounter(uncleanlySkippedBytesKey, 0L);

    restartedKey = this.keyPrefix + "restartedLogReading";
    restartWALReading = rms.getMetricsRegistry().getCounter(restartedKey, 0L);

    repeatedBytesKey = this.keyPrefix + "repeatedLogFileBytes";
    repeatedFileBytes = rms.getMetricsRegistry().getCounter(repeatedBytesKey, 0L);

    completedLogsKey = this.keyPrefix + "completedLogs";
    completedWAL = rms.getMetricsRegistry().getCounter(completedLogsKey, 0L);

    completedRecoveryKey = this.keyPrefix + "completedRecoverQueues";
    completedRecoveryQueue = rms.getMetricsRegistry().getCounter(completedRecoveryKey, 0L);
  }

  @Override public void setLastShippedAge(long age) {
    ageOfLastShippedOpGauge.set(age);
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
    shippedBytesCounter.incr(size);
    MetricsReplicationGlobalSourceSource
      .incrementKBsCounter(shippedBytesCounter, shippedKBsCounter);
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

    rms.removeMetric(shippedHFilesKey);
    rms.removeMetric(sizeOfHFileRefsQueueKey);

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
  public void incrHFilesShipped(long hfiles) {
    shippedHFilesCounter.incr(hfiles);
  }

  @Override
  public void incrSizeOfHFileRefsQueue(long size) {
    sizeOfHFileRefsQueueGauge.incr(size);
  }

  @Override
  public void decrSizeOfHFileRefsQueue(long size) {
    sizeOfHFileRefsQueueGauge.decr(size);
  }

  @Override
  public int getSizeOfLogQueue() {
    return (int)sizeOfLogQueueGauge.value();
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
