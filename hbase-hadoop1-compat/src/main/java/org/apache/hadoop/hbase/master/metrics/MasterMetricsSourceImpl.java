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

package org.apache.hadoop.hbase.master.metrics;

import org.apache.hadoop.hbase.metrics.BaseMetricsSourceImpl;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;

/**
 * Hadoop1 implementation of MasterMetricsSource.
 */
public class MasterMetricsSourceImpl
        extends BaseMetricsSourceImpl implements MasterMetricsSource {

  MetricMutableCounterLong clusterRequestsCounter;
  MetricMutableGaugeLong ritGauge;
  MetricMutableGaugeLong ritCountOverThresholdGauge;
  MetricMutableGaugeLong ritOldestAgeGauge;


  public MasterMetricsSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT);
  }

  public MasterMetricsSourceImpl(String metricsName,
                                 String metricsDescription,
                                 String metricsContext) {
    super(metricsName, metricsDescription, metricsContext);

    clusterRequestsCounter = getLongCounter("cluster_requests", 0);
    ritGauge = getLongGauge("ritCount", 0);
    ritCountOverThresholdGauge = getLongGauge("ritCountOverThreshold", 0);
    ritOldestAgeGauge = getLongGauge("ritOldestAge", 0);
  }

  public void incRequests(final int inc) {
    this.clusterRequestsCounter.incr(inc);
  }

  public void setRIT(int ritCount) {
    ritGauge.set(ritCount);
  }

  public void setRITCountOverThreshold(int ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }

  public void setRITOldestAge(long ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }
}
