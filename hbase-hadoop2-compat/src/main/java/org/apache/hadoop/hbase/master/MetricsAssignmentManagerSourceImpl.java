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
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

@InterfaceAudience.Private
public class MetricsAssignmentManagerSourceImpl extends BaseSourceImpl implements MetricsAssignmentManagerSource {

  private MutableGaugeLong ritGauge;
  private MutableGaugeLong ritCountOverThresholdGauge;
  private MutableGaugeLong ritOldestAgeGauge;
  private MutableHistogram assignTimeHisto;
  private MutableHistogram bulkAssignTimeHisto;

  public MetricsAssignmentManagerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsAssignmentManagerSourceImpl(String metricsName,
                                            String metricsDescription,
                                            String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  public void init() {
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, "", 0l);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(RIT_COUNT_OVER_THRESHOLD_NAME, "", 0l);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, "", 0l);
    assignTimeHisto = metricsRegistry.newTimeHistogram(ASSIGN_TIME_NAME);
    bulkAssignTimeHisto = metricsRegistry.newTimeHistogram(BULK_ASSIGN_TIME_NAME);
  }

  @Override
  public void updateAssignmentTime(long time) {
    assignTimeHisto.add(time);
  }

  @Override
  public void updateBulkAssignTime(long time) {
    bulkAssignTimeHisto.add(time);
  }

  public void setRIT(int ritCount) {
    ritGauge.set(ritCount);
  }

  public void setRITCountOverThreshold(int ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }

  public void setRITOldestAge(long ritCount) {
    ritOldestAgeGauge.set(ritCount);
  }
}
