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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A collection of exposed metrics for space quotas from an HBase RegionServer.
 */
@InterfaceAudience.Private
public interface MetricsRegionServerQuotaSource extends BaseSource {

  String METRICS_NAME = "Quotas";
  String METRICS_CONTEXT = "regionserver";
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer Quotas";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String NUM_TABLES_IN_VIOLATION_NAME = "numTablesInViolation";
  String NUM_SPACE_SNAPSHOTS_RECEIVED_NAME = "numSpaceSnapshotsReceived";
  String FILE_SYSTEM_UTILIZATION_CHORE_TIME = "fileSystemUtilizationChoreTime";
  String SPACE_QUOTA_REFRESHER_CHORE_TIME = "spaceQuotaRefresherChoreTime";

  String NUM_REGION_SIZE_REPORT_NAME = "numRegionSizeReports";
  String REGION_SIZE_REPORTING_CHORE_TIME_NAME = "regionSizeReportingChoreTime";

  /**
   * Updates the metric tracking how many tables this RegionServer has marked as in violation
   * of their space quota.
   */
  void updateNumTablesInSpaceQuotaViolation(long tablesInViolation);

  /**
   * Updates the metric tracking how many tables this RegionServer has received
   * {@code SpaceQuotaSnapshot}s for.
   *
   * @param numSnapshots The number of {@code SpaceQuotaSnapshot}s received from the Master.
   */
  void updateNumTableSpaceQuotaSnapshots(long numSnapshots);

  /**
   * Updates the metric tracking how much time was spent scanning the filesystem to compute
   * the size of each region hosted by this RegionServer.
   *
   * @param time The execution time of the chore in milliseconds.
   */
  void incrementSpaceQuotaFileSystemScannerChoreTime(long time);

  /**
   * Updates the metric tracking how much time was spent updating the RegionServer with the
   * latest information on space quotas from the {@code hbase:quota} table.
   *
   * @param time The execution time of the chore in milliseconds.
   */
  void incrementSpaceQuotaRefresherChoreTime(long time);

  /**
   * Updates the metric tracking how many region size reports were sent from this RegionServer to
   * the Master. These reports contain information on the size of each Region hosted locally.
   *
   * @param numReportsSent The number of region size reports sent
   */
  void incrementNumRegionSizeReportsSent(long numReportsSent);

  /**
   * Updates the metric tracking how much time was spent sending region size reports to the Master
   * by the RegionSizeReportingChore.
   *
   * @param time The execution time in milliseconds.
   */
  void incrementRegionSizeReportingChoreTime(long time);
}
