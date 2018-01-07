/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link MetricsMasterQuotaSource} which writes the values passed in via the
 * interface to the metrics backend.
 */
@InterfaceAudience.Private
public class MetricsMasterQuotaSourceImpl extends BaseSourceImpl
        implements MetricsMasterQuotaSource {
  private final MetricsMasterWrapper wrapper;
  private final MutableGaugeLong spaceQuotasGauge;
  private final MutableGaugeLong tablesViolatingQuotasGauge;
  private final MutableGaugeLong namespacesViolatingQuotasGauge;
  private final MutableGaugeLong regionSpaceReportsGauge;
  private final MetricHistogram quotaObserverTimeHisto;
  private final MetricHistogram snapshotObserverTimeHisto;
  private final MetricHistogram snapshotObserverSizeComputationTimeHisto;
  private final MetricHistogram snapshotObserverSnapshotFetchTimeHisto;

  public MetricsMasterQuotaSourceImpl(MetricsMasterWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, wrapper);
  }

  public MetricsMasterQuotaSourceImpl(
      String metricsName, String metricsDescription, String metricsContext,
      String metricsJmxContext, MetricsMasterWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    spaceQuotasGauge = getMetricsRegistry().newGauge(
        NUM_SPACE_QUOTAS_NAME, NUM_SPACE_QUOTAS_DESC, 0L);
    tablesViolatingQuotasGauge = getMetricsRegistry().newGauge(
        NUM_TABLES_QUOTA_VIOLATIONS_NAME, NUM_TABLES_QUOTA_VIOLATIONS_DESC, 0L);
    namespacesViolatingQuotasGauge = getMetricsRegistry().newGauge(
        NUM_NS_QUOTA_VIOLATIONS_NAME, NUM_NS_QUOTA_VIOLATIONS_DESC, 0L);
    regionSpaceReportsGauge = getMetricsRegistry().newGauge(
        NUM_REGION_SIZE_REPORTS_NAME, NUM_REGION_SIZE_REPORTS_DESC, 0L);

    quotaObserverTimeHisto = getMetricsRegistry().newTimeHistogram(
        QUOTA_OBSERVER_CHORE_TIME_NAME, QUOTA_OBSERVER_CHORE_TIME_DESC);
    snapshotObserverTimeHisto = getMetricsRegistry().newTimeHistogram(
        SNAPSHOT_OBSERVER_CHORE_TIME_NAME, SNAPSHOT_OBSERVER_CHORE_TIME_DESC);

    snapshotObserverSizeComputationTimeHisto = getMetricsRegistry().newTimeHistogram(
        SNAPSHOT_OBSERVER_SIZE_COMPUTATION_TIME_NAME, SNAPSHOT_OBSERVER_SIZE_COMPUTATION_TIME_DESC);
    snapshotObserverSnapshotFetchTimeHisto = getMetricsRegistry().newTimeHistogram(
        SNAPSHOT_OBSERVER_FETCH_TIME_NAME, SNAPSHOT_OBSERVER_FETCH_TIME_DESC);
  }

  @Override
  public void updateNumSpaceQuotas(long numSpaceQuotas) {
    spaceQuotasGauge.set(numSpaceQuotas);
  }

  @Override
  public void updateNumTablesInSpaceQuotaViolation(long numTablesInViolation) {
    tablesViolatingQuotasGauge.set(numTablesInViolation);
  }

  @Override
  public void updateNumNamespacesInSpaceQuotaViolation(long numNamespacesInViolation) {
    namespacesViolatingQuotasGauge.set(numNamespacesInViolation);
  }

  @Override
  public void updateNumCurrentSpaceQuotaRegionSizeReports(long numCurrentRegionSizeReports) {
    regionSpaceReportsGauge.set(numCurrentRegionSizeReports);
  }

  @Override
  public void incrementSpaceQuotaObserverChoreTime(long time) {
    quotaObserverTimeHisto.add(time);
  }

  @Override
  public void incrementSnapshotObserverChoreTime(long time) {
    snapshotObserverTimeHisto.add(time);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder record = metricsCollector.addRecord(metricsRegistry.info());
    if (wrapper != null) {
      // Summarize the tables
      Map<String,Entry<Long,Long>> tableUsages = wrapper.getTableSpaceUtilization();
      String tableSummary = "[]";
      if (tableUsages != null && !tableUsages.isEmpty()) {
        tableSummary = generateJsonQuotaSummary(tableUsages.entrySet(), "table");
      }
      record.tag(Interns.info(TABLE_QUOTA_USAGE_NAME, TABLE_QUOTA_USAGE_DESC), tableSummary);

      // Summarize the namespaces
      String nsSummary = "[]";
      Map<String,Entry<Long,Long>> namespaceUsages = wrapper.getNamespaceSpaceUtilization();
      if (namespaceUsages != null && !namespaceUsages.isEmpty()) {
        nsSummary = generateJsonQuotaSummary(namespaceUsages.entrySet(), "namespace");
      }
      record.tag(Interns.info(NS_QUOTA_USAGE_NAME, NS_QUOTA_USAGE_DESC), nsSummary);
    }
    metricsRegistry.snapshot(record, all);
  }

  /**
   * Summarizes the usage and limit for many targets (table or namespace) into JSON.
   */
  private String generateJsonQuotaSummary(
      Iterable<Entry<String,Entry<Long,Long>>> data, String target) {
    StringBuilder sb = new StringBuilder();
    for (Entry<String,Entry<Long,Long>> tableUsage : data) {
      String tableName = tableUsage.getKey();
      long usage = tableUsage.getValue().getKey();
      long limit = tableUsage.getValue().getValue();
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("{").append(target).append("=").append(tableName).append(", usage=").append(usage)
          .append(", limit=").append(limit).append("}");
    }
    sb.insert(0, "[").append("]");
    return sb.toString();
  }

  @Override
  public void incrementSnapshotObserverSnapshotComputationTime(long time) {
    snapshotObserverSizeComputationTimeHisto.add(time);
  }

  @Override
  public void incrementSnapshotObserverSnapshotFetchTime(long time) {
    snapshotObserverSnapshotFetchTimeHisto.add(time);
  }
}
