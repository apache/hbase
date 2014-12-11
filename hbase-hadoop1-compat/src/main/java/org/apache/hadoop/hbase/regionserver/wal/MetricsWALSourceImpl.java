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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

/**
 * Class that transitions metrics from HLog's MetricsWAL into the metrics subsystem.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern.
 */
public class MetricsWALSourceImpl extends BaseSourceImpl implements MetricsWALSource {

  private final MetricHistogram appendSizeHisto;
  private final MetricHistogram appendTimeHisto;
  private final MetricMutableCounterLong appendCount;
  private final MetricMutableCounterLong slowAppendCount;
  private final MetricHistogram syncTimeHisto;
  private final MetricMutableCounterLong logRollRequested;
  private final MetricMutableCounterLong lowReplicationLogRollRequested;

  public MetricsWALSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsWALSourceImpl(String metricsName,
                              String metricsDescription,
                              String metricsContext,
                              String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    //Create and store the metrics that will be used.
    appendTimeHisto = this.getMetricsRegistry().newHistogram(APPEND_TIME, APPEND_TIME_DESC);
    appendSizeHisto = this.getMetricsRegistry().newHistogram(APPEND_SIZE, APPEND_SIZE_DESC);
    appendCount = this.getMetricsRegistry().newCounter(APPEND_COUNT, APPEND_COUNT_DESC, 0l);
    slowAppendCount =
        this.getMetricsRegistry().newCounter(SLOW_APPEND_COUNT, SLOW_APPEND_COUNT_DESC, 0l);
    syncTimeHisto = this.getMetricsRegistry().newHistogram(SYNC_TIME, SYNC_TIME_DESC);
    logRollRequested =
        this.getMetricsRegistry().newCounter(ROLL_REQUESTED, ROLL_REQUESTED_DESC, 0L);
    lowReplicationLogRollRequested = this.getMetricsRegistry()
        .newCounter(LOW_REPLICA_ROLL_REQUESTED, LOW_REPLICA_ROLL_REQUESTED_DESC, 0L);
  }

  @Override
  public void incrementAppendSize(long size) {
    appendSizeHisto.add(size);
  }

  @Override
  public void incrementAppendTime(long time) {
    appendTimeHisto.add(time);
  }

  @Override
  public void incrementAppendCount() {
    appendCount.incr();
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
  public void incrementLowReplicationLogRoll() {
    lowReplicationLogRollRequested.incr();
  }
}
