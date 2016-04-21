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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MetricMutableHistogram;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;

/**
 * Hadoop 1 version of MetricsThriftServerSource{@link MetricsThriftServerSource}
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
public class MetricsThriftServerSourceImpl extends BaseSourceImpl implements
    MetricsThriftServerSource {


  private MetricMutableHistogram batchGetStat;
  private MetricMutableHistogram batchMutateStat;
  private MetricMutableHistogram queueTimeStat;

  private MetricMutableHistogram thriftCallStat;
  private MetricMutableHistogram thriftSlowCallStat;

  private MetricMutableGaugeLong callQueueLenGauge;

  // pause monitor metrics
  private final MetricMutableCounterLong infoPauseThresholdExceeded;
  private final MetricMutableCounterLong warnPauseThresholdExceeded;
  private final MetricMutableHistogram pausesWithGc;
  private final MetricMutableHistogram pausesWithoutGc;

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
    callQueueLenGauge = getMetricsRegistry().getLongGauge(CALL_QUEUE_LEN_KEY, 0);
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
    MetricMutableHistogram s = getMetricsRegistry().getHistogram(name);
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
