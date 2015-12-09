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

package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableHistogram;

public class MetricsBalancerSourceImpl extends BaseSourceImpl implements MetricsBalancerSource{

  private MetricMutableHistogram blanceClusterHisto;
  private MetricMutableCounterLong miscCount;

  public MetricsBalancerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsBalancerSourceImpl(String metricsName,
                                   String metricsDescription,
                                   String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }


  @Override
  public void init() {
    blanceClusterHisto = metricsRegistry.newTimeHistogram(BALANCE_CLUSTER);
    miscCount = metricsRegistry.newCounter(MISC_INVOATION_COUNT, "", 0L);

  }

  @Override
  public void updateBalanceCluster(long time) {
    blanceClusterHisto.add(time);
  }

  @Override
  public void incrMiscInvocations() {
     miscCount.incr();
  }
}
