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

  String RIT_COUNT_DESC = "Current number of Regions In Transition (Gauge).";
  String RIT_COUNT_OVER_THRESHOLD_DESC =
      "Current number of Regions In Transition over threshold time (Gauge).";
  String RIT_OLDEST_AGE_DESC =
      "Timestamp in milliseconds of the oldest Region In Transition (Gauge).";
  String RIT_DURATION_DESC =
      "Total durations in milliseconds for all Regions in Transition (Histogram).";

  String ASSIGN_METRIC_PREFIX = "assign";
  String UNASSIGN_METRIC_PREFIX = "unassign";
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

  /**
   * TODO: Remove. This may not be needed now as assign and unassign counts are tracked separately
   * Increment the count of operations (assign/unassign).
   */
  void incrementOperationCounter();

  /**
   * @return {@link OperationMetrics} containing common metrics for assign operation
   */
  OperationMetrics getAssignMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for unassign operation
   */
  OperationMetrics getUnassignMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for split operation
   */
  OperationMetrics getSplitMetrics();

  /**
   * @return {@link OperationMetrics} containing common metrics for merge operation
   */
  OperationMetrics getMergeMetrics();
}
