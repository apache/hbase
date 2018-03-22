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
}
