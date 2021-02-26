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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A collection of exposed metrics for space quotas from the HBase Master.
 */
@InterfaceAudience.Private
public interface MetricsMasterQuotaSource extends BaseSource {

  String METRICS_NAME = "Quotas";
  String METRICS_CONTEXT = "master";
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;
  String METRICS_DESCRIPTION = "Metrics about HBase Quotas by the Master";

  String NUM_SPACE_QUOTAS_NAME = "numSpaceQuotas";
  String NUM_SPACE_QUOTAS_DESC = "Number of space quotas defined";
  String NUM_TABLES_QUOTA_VIOLATIONS_NAME = "numTablesInQuotaViolation";
  String NUM_TABLES_QUOTA_VIOLATIONS_DESC = "Number of tables violating space quotas";
  String NUM_NS_QUOTA_VIOLATIONS_NAME = "numNamespaceInQuotaViolation";
  String NUM_NS_QUOTA_VIOLATIONS_DESC = "Number of namespaces violating space quotas";
  String NUM_REGION_SIZE_REPORTS_NAME = "numRegionSizeReports";
  String NUM_REGION_SIZE_REPORTS_DESC = "Number of Region sizes reported";
  String QUOTA_OBSERVER_CHORE_TIME_NAME = "quotaObserverChoreTime";
  String QUOTA_OBSERVER_CHORE_TIME_DESC =
      "Histogram for the time in millis for the QuotaObserverChore";
  String SNAPSHOT_OBSERVER_CHORE_TIME_NAME = "snapshotQuotaObserverChoreTime";
  String SNAPSHOT_OBSERVER_CHORE_TIME_DESC =
      "Histogram for the time in millis for the SnapshotQuotaObserverChore";
  String SNAPSHOT_OBSERVER_SIZE_COMPUTATION_TIME_NAME = "snapshotObserverSizeComputationTime";
  String SNAPSHOT_OBSERVER_SIZE_COMPUTATION_TIME_DESC =
      "Histogram for the time in millis to compute the size of each snapshot";
  String SNAPSHOT_OBSERVER_FETCH_TIME_NAME = "snapshotObserverSnapshotFetchTime";
  String SNAPSHOT_OBSERVER_FETCH_TIME_DESC =
      "Histogram for the time in millis to fetch all snapshots from HBase";
  String TABLE_QUOTA_USAGE_NAME = "tableSpaceQuotaOverview";
  String TABLE_QUOTA_USAGE_DESC = "A JSON summary of the usage of all tables with space quotas";
  String NS_QUOTA_USAGE_NAME = "namespaceSpaceQuotaOverview";
  String NS_QUOTA_USAGE_DESC = "A JSON summary of the usage of all namespaces with space quotas";

  /**
   * Updates the metric tracking the number of space quotas defined in the system.
   *
   * @param numSpaceQuotas The number of space quotas defined
   */
  void updateNumSpaceQuotas(long numSpaceQuotas);

  /**
   * Updates the metric tracking the number of tables the master has computed to be in
   * violation of their space quota.
   *
   * @param numTablesInViolation The number of tables violating a space quota
   */
  void updateNumTablesInSpaceQuotaViolation(long numTablesInViolation);

  /**
   * Updates the metric tracking the number of namespaces the master has computed to be in
   * violation of their space quota.
   *
   * @param numNamespacesInViolation The number of namespaces violating a space quota
   */
  void updateNumNamespacesInSpaceQuotaViolation(long numNamespacesInViolation);

  /**
   * Updates the metric tracking the number of region size reports the master is currently
   * retaining in memory.
   *
   * @param numCurrentRegionSizeReports The number of region size reports the master is holding in
   *    memory
   */
  void updateNumCurrentSpaceQuotaRegionSizeReports(long numCurrentRegionSizeReports);

  /**
   * Updates the metric tracking the amount of time taken by the {@code QuotaObserverChore}
   * which runs periodically.
   *
   * @param time The execution time of the chore in milliseconds
   */
  void incrementSpaceQuotaObserverChoreTime(long time);

  /**
   * Updates the metric tracking the amount of time taken by the {@code SnapshotQuotaObserverChore}
   * which runs periodically.
   */
  void incrementSnapshotObserverChoreTime(long time);

  /**
   * Updates the metric tracking the amount of time taken by the {@code SnapshotQuotaObserverChore}
   * to compute the size of one snapshot, relative to the files referenced by the originating table.
   */
  void incrementSnapshotObserverSnapshotComputationTime(long time);

  /**
   * Updates the metric tracking the amount of time taken by the {@code SnapshotQuotaObserverChore}
   * to fetch all snapshots.
   */
  void incrementSnapshotObserverSnapshotFetchTime(long time);
}
