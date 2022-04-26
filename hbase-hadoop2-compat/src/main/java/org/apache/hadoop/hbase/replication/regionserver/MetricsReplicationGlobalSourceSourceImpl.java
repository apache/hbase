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

import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsReplicationGlobalSourceSourceImpl
  implements MetricsReplicationGlobalSourceSource {
  private static final String KEY_PREFIX = "source.";

  private final MetricsReplicationSourceImpl rms;

  private final MutableHistogram ageOfLastShippedOpHist;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableFastCounter logReadInEditsCounter;
  private final MutableFastCounter walEditsFilteredCounter;
  private final MutableFastCounter shippedBatchesCounter;
  private final MutableFastCounter failedBatchesCounter;
  private final MutableFastCounter shippedOpsCounter;
  private final MutableFastCounter shippedBytesCounter;

  /**
   * @deprecated since 1.3.0. Use {@link #shippedBytesCounter} instead.
   */
  @Deprecated
  private final MutableFastCounter shippedKBsCounter;
  private final MutableFastCounter logReadInBytesCounter;
  private final MutableFastCounter shippedHFilesCounter;
  private final MutableGaugeLong sizeOfHFileRefsQueueGauge;
  private final MutableFastCounter unknownFileLengthForClosedWAL;
  private final MutableFastCounter uncleanlyClosedWAL;
  private final MutableFastCounter uncleanlyClosedSkippedBytes;
  private final MutableFastCounter restartWALReading;
  private final MutableFastCounter repeatedFileBytes;
  private final MutableFastCounter completedWAL;
  private final MutableFastCounter completedRecoveryQueue;
  private final MutableFastCounter failedRecoveryQueue;
  private final MutableGaugeLong walReaderBufferUsageBytes;
  private final MutableGaugeInt sourceInitializing;

  public MetricsReplicationGlobalSourceSourceImpl(MetricsReplicationSourceImpl rms) {
    this.rms = rms;

    ageOfLastShippedOpHist =
      rms.getMetricsRegistry().newTimeHistogram(SOURCE_AGE_OF_LAST_SHIPPED_OP);

    sizeOfLogQueueGauge = rms.getMetricsRegistry().getGauge(SOURCE_SIZE_OF_LOG_QUEUE, 0L);

    shippedBatchesCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_BATCHES, 0L);

    failedBatchesCounter = rms.getMetricsRegistry().getCounter(SOURCE_FAILED_BATCHES, 0L);

    shippedOpsCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_OPS, 0L);

    shippedKBsCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_KBS, 0L);

    shippedBytesCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_BYTES, 0L);

    logReadInBytesCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_READ_IN_BYTES, 0L);

    logReadInEditsCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_READ_IN_EDITS, 0L);

    walEditsFilteredCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_EDITS_FILTERED, 0L);

    shippedHFilesCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_HFILES, 0L);

    sizeOfHFileRefsQueueGauge =
        rms.getMetricsRegistry().getGauge(SOURCE_SIZE_OF_HFILE_REFS_QUEUE, 0L);

    unknownFileLengthForClosedWAL = rms.getMetricsRegistry()
            .getCounter(SOURCE_CLOSED_LOGS_WITH_UNKNOWN_LENGTH, 0L);
    uncleanlyClosedWAL = rms.getMetricsRegistry().getCounter(SOURCE_UNCLEANLY_CLOSED_LOGS, 0L);
    uncleanlyClosedSkippedBytes = rms.getMetricsRegistry()
            .getCounter(SOURCE_UNCLEANLY_CLOSED_IGNORED_IN_BYTES, 0L);
    restartWALReading = rms.getMetricsRegistry().getCounter(SOURCE_RESTARTED_LOG_READING, 0L);
    repeatedFileBytes = rms.getMetricsRegistry().getCounter(SOURCE_REPEATED_LOG_FILE_BYTES, 0L);
    completedWAL = rms.getMetricsRegistry().getCounter(SOURCE_COMPLETED_LOGS, 0L);
    completedRecoveryQueue = rms.getMetricsRegistry()
            .getCounter(SOURCE_COMPLETED_RECOVERY_QUEUES, 0L);
    failedRecoveryQueue = rms.getMetricsRegistry()
            .getCounter(SOURCE_FAILED_RECOVERY_QUEUES, 0L);

    walReaderBufferUsageBytes = rms.getMetricsRegistry()
        .getGauge(SOURCE_WAL_READER_EDITS_BUFFER, 0L);
    sourceInitializing = rms.getMetricsRegistry().getGaugeInt(SOURCE_INITIALIZING, 0);
  }

  @Override public void setLastShippedAge(long age) {
    ageOfLastShippedOpHist.add(age);
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
    walEditsFilteredCounter.incr(size);
  }

  @Override public void incrBatchesShipped(int batches) {
    shippedBatchesCounter.incr(batches);
  }

  @Override public void incrFailedBatches() {
    failedBatchesCounter.incr();
  }

  @Override public void incrOpsShipped(long ops) {
    shippedOpsCounter.incr(ops);
  }

  @Override public void incrShippedBytes(long size) {
    shippedBytesCounter.incr(size);
    // obtained value maybe smaller than 1024. We should make sure that KB count
    // eventually picks up even from multiple smaller updates.
    incrementKBsCounter(shippedBytesCounter, shippedKBsCounter);
  }

  static void incrementKBsCounter(MutableFastCounter bytesCounter, MutableFastCounter kbsCounter) {
    // Following code should be thread-safe.
    long delta = 0;
    while(true) {
      long bytes = bytesCounter.value();
      delta = (bytes / 1024) - kbsCounter.value();
      if (delta > 0) {
        kbsCounter.incr(delta);
      } else {
        break;
      }
    }
  }
  @Override public void incrLogReadInBytes(long size) {
    logReadInBytesCounter.incr(size);
  }

  @Override public void clear() {
  }

  @Override
  public long getLastShippedAge() {
    return ageOfLastShippedOpHist.getMax();
  }

  @Override public void incrHFilesShipped(long hfiles) {
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
  public long getUncleanlyClosedWALs() {
    return uncleanlyClosedWAL.value();
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
  public void incrFailedRecoveryQueue() {
    failedRecoveryQueue.incr(1L);
  }

  @Override
  public void setOldestWalAge(long age) {
    // Not implemented
  }

  @Override
  public long getOldestWalAge() {
    // Not implemented
    return 0;
  }

  @Override
  public void incrSourceInitializing() {
    sourceInitializing.incr(1);
  }

  @Override
  public void decrSourceInitializing() {
    sourceInitializing.decr(1);
  }

  @Override
  public int getSourceInitializing() {
    return sourceInitializing.value();
  }

  @Override
  public void init() {
    rms.init();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    rms.setGauge(KEY_PREFIX + gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    rms.incGauge(KEY_PREFIX + gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    rms.decGauge(KEY_PREFIX + gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    rms.removeMetric(KEY_PREFIX + key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    rms.incCounters(KEY_PREFIX + counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    rms.updateHistogram(KEY_PREFIX + name, value);
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

  @Override
  public long getWALEditsRead() {
    return this.logReadInEditsCounter.value();
  }

  @Override
  public long getShippedOps() {
    return this.shippedOpsCounter.value();
  }

  @Override
  public long getEditsFiltered() {
    return this.walEditsFilteredCounter.value();
  }

  @Override
  public void setWALReaderEditsBufferBytes(long usage) {
    this.walReaderBufferUsageBytes.set(usage);
  }

  @Override
  public long getWALReaderEditsBufferBytes() {
    return this.walReaderBufferUsageBytes.value();
  }
}
