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

package org.apache.hadoop.hbase.thrift;

import org.apache.hadoop.hbase.metrics.ExceptionTrackingSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop 2 version of {@link org.apache.hadoop.hbase.thrift.MetricsThriftServerSource}
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsThriftServerSourceImpl extends ExceptionTrackingSourceImpl implements
    MetricsThriftServerSource {

  private MetricHistogram batchGetStat;
  private MetricHistogram batchMutateStat;
  private MetricHistogram queueTimeStat;

  private MetricHistogram thriftCallStat;
  private MetricHistogram thriftSlowCallStat;

  private MutableGaugeLong callQueueLenGauge;

  private MutableGaugeLong activeWorkerCountGauge;

  // pause monitor metrics
  private final MutableFastCounter infoPauseThresholdExceeded;
  private final MutableFastCounter warnPauseThresholdExceeded;
  private final MetricHistogram pausesWithGc;
  private final MetricHistogram pausesWithoutGc;

  public MetricsThriftServerSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    // pause monitor metrics
    infoPauseThresholdExceeded = getMetricsRegistry().newCounter(INFO_THRESHOLD_COUNT_KEY,
      INFO_THRESHOLD_COUNT_DESC, 0L);
    warnPauseThresholdExceeded = getMetricsRegistry().newCounter(WARN_THRESHOLD_COUNT_KEY,
      WARN_THRESHOLD_COUNT_DESC, 0L);
    pausesWithGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITH_GC_KEY);
    pausesWithoutGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITHOUT_GC_KEY);
  }

  @Override
  public void init() {
    super.init();
    batchGetStat = getMetricsRegistry().newTimeHistogram(BATCH_GET_KEY);
    batchMutateStat = getMetricsRegistry().newTimeHistogram(BATCH_MUTATE_KEY);
    queueTimeStat = getMetricsRegistry().newTimeHistogram(TIME_IN_QUEUE_KEY);
    thriftCallStat = getMetricsRegistry().newTimeHistogram(THRIFT_CALL_KEY);
    thriftSlowCallStat = getMetricsRegistry().newTimeHistogram(SLOW_THRIFT_CALL_KEY);
    callQueueLenGauge = getMetricsRegistry().getGauge(CALL_QUEUE_LEN_KEY, 0);
    activeWorkerCountGauge = getMetricsRegistry().getGauge(ACTIVE_WORKER_COUNT_KEY, 0);
  }

  @Override
  public void incTimeInQueue(long time) {
    queueTimeStat.add(time);
  }

  @Override
  public void setCallQueueLen(int len) {
    callQueueLenGauge.set(len);
  }

  @Override
  public void incNumRowKeysInBatchGet(int diff) {
    batchGetStat.add(diff);
  }

  @Override
  public void incNumRowKeysInBatchMutate(int diff) {
    batchMutateStat.add(diff);
  }

  @Override
  public void incMethodTime(String name, long time) {
    MutableHistogram s = getMetricsRegistry().getHistogram(name);
    s.add(time);
  }

  @Override
  public void incCall(long time) {
    thriftCallStat.add(time);
  }

  @Override
  public void incSlowCall(long time) {
    thriftSlowCallStat.add(time);
  }

  @Override
  public void incActiveWorkerCount() {
    activeWorkerCountGauge.incr();
  }

  @Override
  public void decActiveWorkerCount() {
    activeWorkerCountGauge.decr();
  }

  @Override
  public void incInfoThresholdExceeded(int count) {
    infoPauseThresholdExceeded.incr(count);
  }

  @Override
  public void incWarnThresholdExceeded(int count) {
    warnPauseThresholdExceeded.incr(count);
  }

  @Override
  public void updatePauseTimeWithGc(long t) {
    pausesWithGc.add(t);
  }

  @Override
  public void updatePauseTimeWithoutGc(long t) {
    pausesWithoutGc.add(t);
  }
}
