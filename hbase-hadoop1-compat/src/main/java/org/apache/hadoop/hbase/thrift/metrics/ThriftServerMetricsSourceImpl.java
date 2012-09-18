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

package org.apache.hadoop.hbase.thrift.metrics;

import org.apache.hadoop.hbase.metrics.BaseMetricsSourceImpl;
import org.apache.hadoop.hbase.thrift.metrics.ThriftServerMetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;

/**
 * Hadoop 1 version of ThriftServerMetricsSource{@link ThriftServerMetricsSource}
 */
public class ThriftServerMetricsSourceImpl extends BaseMetricsSourceImpl implements
    ThriftServerMetricsSource {


  private MetricMutableStat batchGetStat;
  private MetricMutableStat batchMutateStat;
  private MetricMutableStat queueTimeStat;

  private MetricMutableStat thriftCallStat;
  private MetricMutableStat thriftSlowCallStat;

  private MetricMutableGaugeLong callQueueLenGauge;

  public ThriftServerMetricsSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }


  @Override
  public void init() {
    super.init();
    batchGetStat = getMetricsRegistry().newStat(BATCH_GET_KEY, "", "Keys", "Ops");
    batchMutateStat = getMetricsRegistry().newStat(BATCH_MUTATE_KEY, "", "Keys", "Ops");
    queueTimeStat = getMetricsRegistry().newStat(TIME_IN_QUEUE_KEY);
    thriftCallStat = getMetricsRegistry().newStat(THRIFT_CALL_KEY);
    thriftSlowCallStat = getMetricsRegistry().newStat(SLOW_THRIFT_CALL_KEY);
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
    MetricMutableStat s = getMetricsRegistry().newStat(name);
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
