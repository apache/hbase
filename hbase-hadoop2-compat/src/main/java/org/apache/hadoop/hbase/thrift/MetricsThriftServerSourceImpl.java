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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

/**
 * Hadoop 2 version of MetricsThriftServerSource{@link org.apache.hadoop.hbase.thrift.MetricsThriftServerSource}
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsThriftServerSourceImpl extends BaseSourceImpl implements
    MetricsThriftServerSource {

  private MutableHistogram batchGetStat;
  private MutableHistogram batchMutateStat;
  private MutableHistogram queueTimeStat;

  private MutableHistogram thriftCallStat;
  private MutableHistogram thriftSlowCallStat;

  private MutableGaugeLong callQueueLenGauge;

  public MetricsThriftServerSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
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

}
