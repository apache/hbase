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

public class MetricsReplicationGlobalSourceSource implements MetricsReplicationSourceSource {
  private final MetricsReplicationSource rms;
  private final MutableGaugeLong ageOfLastShippedOpGauge;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableCounterLong logReadInEditsCounter;
  private final MutableCounterLong logEditsFilteredCounter;
  private final MutableCounterLong shippedBatchesCounter;
  private final MutableCounterLong shippedOpsCounter;
  private final MutableCounterLong shippedBytesCounter;
  @Deprecated
  private final MutableCounterLong shippedKBsCounter;
  private final MutableCounterLong logReadInBytesCounter;
  private final MutableCounterLong unknownFileLengthForClosedWAL;
  private final MutableCounterLong uncleanlyClosedWAL;
  private final MutableCounterLong uncleanlyClosedSkippedBytes;
  private final MutableCounterLong restartWALReading;
  private final MutableCounterLong repeatedFileBytes;
  private final MutableCounterLong completedWAL;
  private final MutableCounterLong completedRecoveryQueue;

  public MetricsReplicationGlobalSourceSource(MetricsReplicationSourceImpl rms) {
    this.rms = rms;
    ageOfLastShippedOpGauge = rms.getMetricsRegistry().getLongGauge(SOURCE_AGE_OF_LAST_SHIPPED_OP, 0L);

    sizeOfLogQueueGauge = rms.getMetricsRegistry().getLongGauge(SOURCE_SIZE_OF_LOG_QUEUE, 0L);

    shippedBatchesCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_SHIPPED_BATCHES, 0L);

    shippedOpsCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_SHIPPED_OPS, 0L);

    shippedKBsCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_SHIPPED_KBS, 0L);

    shippedBytesCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_SHIPPED_BYTES, 0L);

    logReadInBytesCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_LOG_READ_IN_BYTES, 0L);

    logReadInEditsCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_LOG_READ_IN_EDITS, 0L);

    logEditsFilteredCounter = rms.getMetricsRegistry().getLongCounter(SOURCE_LOG_EDITS_FILTERED, 0L);

    unknownFileLengthForClosedWAL = rms.getMetricsRegistry().getLongCounter(SOURCE_CLOSED_LOGS_WITH_UNKNOWN_LENGTH, 0L);
    uncleanlyClosedWAL = rms.getMetricsRegistry().getLongCounter(SOURCE_UNCLEANLY_CLOSED_LOGS, 0L);
    uncleanlyClosedSkippedBytes = rms.getMetricsRegistry().getLongCounter(SOURCE_UNCLEANLY_CLOSED_IGNORED_IN_BYTES, 0L);
    restartWALReading = rms.getMetricsRegistry().getLongCounter(SOURCE_RESTARTED_LOG_READING, 0L);
    repeatedFileBytes = rms.getMetricsRegistry().getLongCounter(SOURCE_REPEATED_LOG_FILE_BYTES, 0L);
    completedWAL = rms.getMetricsRegistry().getLongCounter(SOURCE_COMPLETED_LOGS, 0L);
    completedRecoveryQueue = rms.getMetricsRegistry().getLongCounter(SOURCE_COMPLETED_RECOVERY_QUEUES, 0L);
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
    incrementKBsCounter(size, shippedBytesCounter, shippedKBsCounter);
  }

  static void incrementKBsCounter(long size, MutableCounterLong bytesCounter,
      MutableCounterLong kbsCounter) {
    bytesCounter.incr(size);
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
    rms.setGauge(gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    rms.incGauge(gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    rms.decGauge(gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    rms.removeMetric(key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    rms.incCounters(counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    rms.updateHistogram(name, value);
  }

  @Override
  public void updateQuantile(String name, long value) {
    rms.updateQuantile(name, value);
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
