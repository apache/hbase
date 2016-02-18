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
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

public class MetricsReplicationGlobalSourceSource implements MetricsReplicationSourceSource{
  private final MetricsReplicationSourceImpl rms;

  private final MutableGaugeLong ageOfLastShippedOpGauge;
  private final MutableGaugeLong sizeOfLogQueueGauge;
  private final MutableFastCounter logReadInEditsCounter;
  private final MutableFastCounter logEditsFilteredCounter;
  private final MutableFastCounter shippedBatchesCounter;
  private final MutableFastCounter shippedOpsCounter;
  private final MutableFastCounter shippedKBsCounter;
  private final MutableFastCounter logReadInBytesCounter;
  private final MutableFastCounter shippedHFilesCounter;
  private final MutableGaugeLong sizeOfHFileRefsQueueGauge;

  public MetricsReplicationGlobalSourceSource(MetricsReplicationSourceImpl rms) {
    this.rms = rms;

    ageOfLastShippedOpGauge = rms.getMetricsRegistry().getGauge(SOURCE_AGE_OF_LAST_SHIPPED_OP, 0L);

    sizeOfLogQueueGauge = rms.getMetricsRegistry().getGauge(SOURCE_SIZE_OF_LOG_QUEUE, 0L);

    shippedBatchesCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_BATCHES, 0L);

    shippedOpsCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_OPS, 0L);

    shippedKBsCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_KBS, 0L);

    logReadInBytesCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_READ_IN_BYTES, 0L);

    logReadInEditsCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_READ_IN_EDITS, 0L);

    logEditsFilteredCounter = rms.getMetricsRegistry().getCounter(SOURCE_LOG_EDITS_FILTERED, 0L);

    shippedHFilesCounter = rms.getMetricsRegistry().getCounter(SOURCE_SHIPPED_HFILES, 0L);

    sizeOfHFileRefsQueueGauge =
        rms.getMetricsRegistry().getGauge(SOURCE_SIZE_OF_HFILE_REFS_QUEUE, 0L);
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
  }

  @Override
  public long getLastShippedAge() {
    return ageOfLastShippedOpGauge.value();
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
}
