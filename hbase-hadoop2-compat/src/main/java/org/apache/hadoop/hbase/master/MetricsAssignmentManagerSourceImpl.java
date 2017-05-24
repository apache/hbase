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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

@InterfaceAudience.Private
public class MetricsAssignmentManagerSourceImpl
    extends BaseSourceImpl
    implements MetricsAssignmentManagerSource {

  private MutableGaugeLong ritGauge;
  private MutableGaugeLong ritCountOverThresholdGauge;
  private MutableGaugeLong ritOldestAgeGauge;
  private MetricHistogram ritDurationHisto;

  private MutableFastCounter operationCounter;
  private MetricHistogram assignTimeHisto;
  private MetricHistogram unassignTimeHisto;

  public MetricsAssignmentManagerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsAssignmentManagerSourceImpl(String metricsName,
                                            String metricsDescription,
                                            String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  public void init() {
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, RIT_COUNT_DESC, 0l);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(RIT_COUNT_OVER_THRESHOLD_NAME,
        RIT_COUNT_OVER_THRESHOLD_DESC,0l);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, RIT_OLDEST_AGE_DESC, 0l);
    assignTimeHisto = metricsRegistry.newTimeHistogram(ASSIGN_TIME_NAME);
    unassignTimeHisto = metricsRegistry.newTimeHistogram(UNASSIGN_TIME_NAME);
    ritDurationHisto = metricsRegistry.newTimeHistogram(RIT_DURATION_NAME, RIT_DURATION_DESC);
    operationCounter = metricsRegistry.getCounter(OPERATION_COUNT_NAME, 0l);
  }

  @Override
  public void setRIT(final int ritCount) {
    ritGauge.set(ritCount);
  }

  @Override
  public void setRITCountOverThreshold(final int ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }

  @Override
  public void setRITOldestAge(final long ritCount) {
    ritOldestAgeGauge.set(ritCount);
  }

  @Override
  public void incrementOperationCounter() {
    operationCounter.incr();
  }

  @Override
  public void updateAssignTime(final long time) {
    assignTimeHisto.add(time);
  }

  @Override
  public void updateUnassignTime(final long time) {
    unassignTimeHisto.add(time);
  }

  @Override
  public void updateRitDuration(long duration) {
    ritDurationHisto.add(duration);
  }
}
