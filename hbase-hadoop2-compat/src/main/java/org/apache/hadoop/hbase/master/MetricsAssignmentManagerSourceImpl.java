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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.OperationMetrics;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsAssignmentManagerSourceImpl
    extends BaseSourceImpl
    implements MetricsAssignmentManagerSource {

  private MutableGaugeLong ritGauge;
  private MutableGaugeLong ritCountOverThresholdGauge;
  private MutableGaugeLong ritOldestAgeGauge;
  private MetricHistogram ritDurationHisto;

  private MutableFastCounter operationCounter;

  private OperationMetrics assignMetrics;
  private OperationMetrics unassignMetrics;
  private OperationMetrics moveMetrics;
  private OperationMetrics reopenMetrics;
  private OperationMetrics openMetrics;
  private OperationMetrics closeMetrics;
  private OperationMetrics splitMetrics;
  private OperationMetrics mergeMetrics;

  public MetricsAssignmentManagerSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsAssignmentManagerSourceImpl(String metricsName,
                                            String metricsDescription,
                                            String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  public void init() {
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, RIT_COUNT_DESC, 0L);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(RIT_COUNT_OVER_THRESHOLD_NAME,
        RIT_COUNT_OVER_THRESHOLD_DESC,0L);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, RIT_OLDEST_AGE_DESC, 0L);
    ritDurationHisto = metricsRegistry.newTimeHistogram(RIT_DURATION_NAME, RIT_DURATION_DESC);
    operationCounter = metricsRegistry.getCounter(OPERATION_COUNT_NAME, 0L);

    /**
     * NOTE: Please refer to HBASE-9774 and HBASE-14282. Based on these two issues, HBase is
     * moving away from using Hadoop's metric2 to having independent HBase specific Metrics. Use
     * {@link BaseSourceImpl#registry} to register the new metrics.
     */
    assignMetrics = new OperationMetrics(registry, ASSIGN_METRIC_PREFIX);
    unassignMetrics = new OperationMetrics(registry, UNASSIGN_METRIC_PREFIX);
    moveMetrics = new OperationMetrics(registry, MOVE_METRIC_PREFIX);
    reopenMetrics = new OperationMetrics(registry, REOPEN_METRIC_PREFIX);
    openMetrics = new OperationMetrics(registry, OPEN_METRIC_PREFIX);
    closeMetrics = new OperationMetrics(registry, CLOSE_METRIC_PREFIX);
    splitMetrics = new OperationMetrics(registry, SPLIT_METRIC_PREFIX);
    mergeMetrics = new OperationMetrics(registry, MERGE_METRIC_PREFIX);
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
  public void setRITOldestAge(final long ritOldestAge) {
    ritOldestAgeGauge.set(ritOldestAge);
  }

  @Override
  public void incrementOperationCounter() {
    operationCounter.incr();
  }

  @Override
  public void updateRitDuration(long duration) {
    ritDurationHisto.add(duration);
  }

  @Override
  public OperationMetrics getAssignMetrics() {
    return assignMetrics;
  }

  @Override
  public OperationMetrics getUnassignMetrics() {
    return unassignMetrics;
  }

  @Override
  public OperationMetrics getSplitMetrics() {
    return splitMetrics;
  }

  @Override
  public OperationMetrics getMergeMetrics() {
    return mergeMetrics;
  }

  @Override
  public OperationMetrics getMoveMetrics() {
    return moveMetrics;
  }

  @Override
  public OperationMetrics getReopenMetrics() {
    return reopenMetrics;
  }

  @Override
  public OperationMetrics getOpenMetrics() {
    return openMetrics;
  }

  @Override
  public OperationMetrics getCloseMetrics() {
    return closeMetrics;
  }
}
