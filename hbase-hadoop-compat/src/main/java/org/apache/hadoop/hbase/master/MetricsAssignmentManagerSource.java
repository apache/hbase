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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.metrics.OperationMetrics;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsAssignmentManagerSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "AssignmentManager";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master assignment manager.";

  // RIT metrics
  String RIT_COUNT_NAME = "ritCount";
  String RIT_COUNT_OVER_THRESHOLD_NAME = "ritCountOverThreshold";
  String RIT_OLDEST_AGE_NAME = "ritOldestAge";
  String RIT_DURATION_NAME = "ritDuration";
  String DEAD_SERVER_OPEN_REGIONS = "deadServerOpenRegions";
  String UNKNOWN_SERVER_OPEN_REGIONS = "unknownServerOpenRegions";

  String RIT_COUNT_DESC = "Current number of Regions In Transition (Gauge).";
  String RIT_COUNT_OVER_THRESHOLD_DESC =
      "Current number of Regions In Transition over threshold time (Gauge).";
  String RIT_OLDEST_AGE_DESC = "Timestamp in milliseconds of the oldest Region In Transition (Gauge).";
  String RIT_DURATION_DESC =
      "Total durations in milliseconds for all Regions in Transition (Histogram).";

  // HBCK report metrics
  String ORPHAN_REGIONS_ON_RS = "orphanRegionsOnRS";
  String ORPHAN_REGIONS_ON_FS = "orphanRegionsOnFS";
  String INCONSISTENT_REGIONS = "inconsistentRegions";

  String ORPHAN_REGIONS_ON_RS_DESC = "Current number of Orphan Regions on RS (Gauge).";
  String ORPHAN_REGIONS_ON_FS_DESC = "Current number of Orphan Regions on FS (Gauge).";
  String INCONSISTENT_REGIONS_DESC = "Current number of Inconsistent Regions (Gauge).";

  // CatalogJanitor Consistency report metrics
  String HOLES = "holes";
  String OVERLAPS = "overlaps";
  String UNKNOWN_SERVER_REGIONS = "unknownServerRegions";
  String EMPTY_REGION_INFO_REGIONS = "emptyRegionInfoRegions";

  String HOLES_DESC = "Current number of Holes (Gauge).";
  String OVERLAPS_DESC = "Current number of Overlaps (Gauge).";
  String UNKNOWN_SERVER_REGIONS_DESC = "Current number of Unknown Server Regions (Gauge).";
  String EMPTY_REGION_INFO_REGIONS_DESC =
      "Current number of Regions with Empty Region Info (Gauge).";

  String ASSIGN_METRIC_PREFIX = "assign";
  String UNASSIGN_METRIC_PREFIX = "unassign";
  String MOVE_METRIC_PREFIX = "move";
  String REOPEN_METRIC_PREFIX = "reopen";
  String OPEN_METRIC_PREFIX = "open";
  String CLOSE_METRIC_PREFIX = "close";
  String SPLIT_METRIC_PREFIX = "split";
  String MERGE_METRIC_PREFIX = "merge";

  String OPERATION_COUNT_NAME = "operationCount";

  /**
   * Set the number of regions in transition.
   *
   * @param ritCount count of the regions in transition.
   */
  void setRIT(int ritCount);

  /**
   * Set the count of the number of regions that have been in transition over the threshold time.
   *
   * @param ritCountOverThreshold number of regions in transition for longer than threshold.
   */
  void setRITCountOverThreshold(int ritCountOverThreshold);

  /**
   * Set the oldest region in transition.
   *
   * @param age age of the oldest RIT.
   */
  void setRITOldestAge(long age);

  void updateRitDuration(long duration);

  void updateDeadServerOpenRegions(int deadRegions);

  void updateUnknownServerOpenRegions(int unknownRegions);

  /**
   * Set the number of orphan regions on RS.
   *
   * @param orphanRegionsOnRs count of the orphan regions on RS in HBCK chore report.
   */
  void setOrphanRegionsOnRs(int orphanRegionsOnRs);

  /**
   * Set the number of orphan regions on FS.
   *
   * @param orphanRegionsOnFs count of the orphan regions on FS in HBCK chore report.
   */
  void setOrphanRegionsOnFs(int orphanRegionsOnFs);

  /**
   * Set the number of inconsistent regions.
   *
   * @param inconsistentRegions count of the inconsistent regions in HBCK chore report.
   */
  void setInconsistentRegions(int inconsistentRegions);

  /**
   * Set the number of holes.
   *
   * @param holes count of the holes in CatalogJanitor Consistency report.
   */
  void setHoles(int holes);

  /**
   * Set the number of overlaps.
   *
   * @param overlaps count of the overlaps in CatalogJanitor Consistency report.
   */
  void setOverlaps(int overlaps);

  /**
   * Set the number of unknown server regions.
   * @param unknownServerRegions count of the unknown server regions in CatalogJanitor Consistency
   *          report.
   */
  void setUnknownServerRegions(int unknownServerRegions);

  /**
   * Set the number of regions with empty region info.
   * @param emptyRegionInfoRegions count of the regions with empty region info in CatalogJanitor
   *          Consistency report.
   */
  void setEmptyRegionInfoRegions(int emptyRegionInfoRegions);

  /**
   * TODO: Remove. This may not be needed now as assign and unassign counts are tracked separately
   * Increment the count of operations (assign/unassign).
   */
  void incrementOperationCounter();

  /**
   * @return {@link OperationMetrics} containing common metrics for assign region operation
   */
  OperationMetrics getAssignMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for unassign region operation
   */
  OperationMetrics getUnassignMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for move region operation
   */
  OperationMetrics getMoveMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for reopen region operation
   */
  OperationMetrics getReopenMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for open region request
   */
  OperationMetrics getOpenMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for close region request
   */
  OperationMetrics getCloseMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for split operation
   */
  OperationMetrics getSplitMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for merge operation
   */
  OperationMetrics getMergeMetrics();
}
