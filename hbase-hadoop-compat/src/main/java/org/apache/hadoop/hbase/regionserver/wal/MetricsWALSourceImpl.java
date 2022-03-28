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

package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that transitions metrics from MetricsWAL into the metrics subsystem.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern.
 * @see org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource
 */
@InterfaceAudience.Private
public class MetricsWALSourceImpl extends BaseSourceImpl implements MetricsWALSource {

  private final MetricHistogram appendSizeHisto;
  private final MetricHistogram appendTimeHisto;
  private final MetricHistogram syncTimeHisto;
  private final MutableFastCounter appendCount;
  private final MutableFastCounter slowAppendCount;
  private final MutableFastCounter logRollRequested;
  private final MutableFastCounter errorRollRequested;
  private final MutableFastCounter lowReplicationRollRequested;
  private final MutableFastCounter slowSyncRollRequested;
  private final MutableFastCounter sizeRollRequested;
  private final MutableFastCounter writtenBytes;
  private final MutableFastCounter successfulLogRolls;
  // Per table metrics.
  private final ConcurrentMap<TableName, MutableFastCounter> perTableAppendCount;
  private final ConcurrentMap<TableName, MutableFastCounter> perTableAppendSize;

  public MetricsWALSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsWALSourceImpl(String metricsName,
                              String metricsDescription,
                              String metricsContext,
                              String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    //Create and store the metrics that will be used.
    appendTimeHisto = this.getMetricsRegistry().newTimeHistogram(APPEND_TIME, APPEND_TIME_DESC);
    appendSizeHisto = this.getMetricsRegistry().newSizeHistogram(APPEND_SIZE, APPEND_SIZE_DESC);
    appendCount = this.getMetricsRegistry().newCounter(APPEND_COUNT, APPEND_COUNT_DESC, 0L);
    slowAppendCount =
        this.getMetricsRegistry().newCounter(SLOW_APPEND_COUNT, SLOW_APPEND_COUNT_DESC, 0L);
    syncTimeHisto = this.getMetricsRegistry().newTimeHistogram(SYNC_TIME, SYNC_TIME_DESC);
    logRollRequested =
        this.getMetricsRegistry().newCounter(ROLL_REQUESTED, ROLL_REQUESTED_DESC, 0L);
    errorRollRequested = this.getMetricsRegistry()
        .newCounter(ERROR_ROLL_REQUESTED, ERROR_ROLL_REQUESTED_DESC, 0L);
    lowReplicationRollRequested = this.getMetricsRegistry()
        .newCounter(LOW_REPLICA_ROLL_REQUESTED, LOW_REPLICA_ROLL_REQUESTED_DESC, 0L);
    slowSyncRollRequested = this.getMetricsRegistry()
        .newCounter(SLOW_SYNC_ROLL_REQUESTED, SLOW_SYNC_ROLL_REQUESTED_DESC, 0L);
    sizeRollRequested = this.getMetricsRegistry()
        .newCounter(SIZE_ROLL_REQUESTED, SIZE_ROLL_REQUESTED_DESC, 0L);
    writtenBytes = this.getMetricsRegistry().newCounter(WRITTEN_BYTES, WRITTEN_BYTES_DESC, 0L);
    successfulLogRolls = this.getMetricsRegistry()
      .newCounter(SUCCESSFUL_LOG_ROLLS, SUCCESSFUL_LOG_ROLLS_DESC, 0L);
    perTableAppendCount = new ConcurrentHashMap<>();
    perTableAppendSize = new ConcurrentHashMap<>();
  }

  @Override
  public void incrementAppendSize(TableName tableName, long size) {
    appendSizeHisto.add(size);
    MutableFastCounter tableAppendSizeCounter = perTableAppendSize.get(tableName);
    if (tableAppendSizeCounter == null) {
      // Ideally putIfAbsent is atomic and we don't need a branch check but we still do it to avoid
      // expensive string construction for every append.
      String metricsKey = String.format("%s.%s", tableName, APPEND_SIZE);
      perTableAppendSize.putIfAbsent(
          tableName, getMetricsRegistry().newCounter(metricsKey, APPEND_SIZE_DESC, 0L));
      tableAppendSizeCounter = perTableAppendSize.get(tableName);
    }
    tableAppendSizeCounter.incr(size);
  }

  @Override
  public void incrementAppendTime(long time) {
    appendTimeHisto.add(time);
  }

  @Override
  public void incrementAppendCount(TableName tableName) {
    appendCount.incr();
    MutableFastCounter tableAppendCounter = perTableAppendCount.get(tableName);
    if (tableAppendCounter == null) {
      String metricsKey = String.format("%s.%s", tableName, APPEND_COUNT);
      perTableAppendCount.putIfAbsent(
          tableName, getMetricsRegistry().newCounter(metricsKey, APPEND_COUNT_DESC, 0L));
      tableAppendCounter = perTableAppendCount.get(tableName);
    }
    tableAppendCounter.incr();
  }

  @Override
  public void incrementSlowAppendCount() {
    slowAppendCount.incr();
  }

  @Override
  public void incrementSyncTime(long time) {
    syncTimeHisto.add(time);
  }

  @Override
  public void incrementLogRollRequested() {
    logRollRequested.incr();
  }

  @Override
  public void incrementErrorLogRoll() {
    errorRollRequested.incr();
  }

  @Override
  public void incrementLowReplicationLogRoll() {
    lowReplicationRollRequested.incr();
  }

  @Override
  public void incrementSlowSyncLogRoll() {
    slowSyncRollRequested.incr();
  }

  @Override
  public void incrementSizeLogRoll() {
    sizeRollRequested.incr();
  }

  @Override
  public long getSlowAppendCount() {
    return slowAppendCount.value();
  }

  @Override
  public void incrementWrittenBytes(long val) {
    writtenBytes.incr(val);
  }

  @Override
  public void incrementSuccessfulLogRolls() {
    successfulLogRolls.incr();
  }

  @Override
  public long getSuccessfulLogRolls() {
    return successfulLogRolls.value();
  }
}
