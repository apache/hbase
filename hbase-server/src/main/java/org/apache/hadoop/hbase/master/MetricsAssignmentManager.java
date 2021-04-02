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

import static org.apache.hadoop.hbase.master.MetricsMaster.convertToProcedureMetrics;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsAssignmentManager {
  private final MetricsAssignmentManagerSource assignmentManagerSource;

  private final ProcedureMetrics assignProcMetrics;
  private final ProcedureMetrics unassignProcMetrics;
  private final ProcedureMetrics moveProcMetrics;
  private final ProcedureMetrics reopenProcMetrics;
  private final ProcedureMetrics openProcMetrics;
  private final ProcedureMetrics closeProcMetrics;
  private final ProcedureMetrics splitProcMetrics;
  private final ProcedureMetrics mergeProcMetrics;

  public MetricsAssignmentManager() {
    assignmentManagerSource = CompatibilitySingletonFactory.getInstance(
        MetricsAssignmentManagerSource.class);

    assignProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getAssignMetrics());
    unassignProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getUnassignMetrics());
    moveProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getMoveMetrics());
    reopenProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getReopenMetrics());
    openProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getOpenMetrics());
    closeProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getCloseMetrics());
    splitProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getSplitMetrics());
    mergeProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getMergeMetrics());
  }

  public MetricsAssignmentManagerSource getMetricsProcSource() {
    return assignmentManagerSource;
  }

  /**
   * set new value for number of regions in transition.
   * @param ritCount
   */
  public void updateRITCount(final int ritCount) {
    assignmentManagerSource.setRIT(ritCount);
  }

  /**
   * update RIT count that are in this state for more than the threshold
   * as defined by the property rit.metrics.threshold.time.
   * @param ritCountOverThreshold
   */
  public void updateRITCountOverThreshold(final int ritCountOverThreshold) {
    assignmentManagerSource.setRITCountOverThreshold(ritCountOverThreshold);
  }

  /**
   * update the timestamp for oldest region in transition metrics.
   * @param timestamp
   */
  public void updateRITOldestAge(final long timestamp) {
    assignmentManagerSource.setRITOldestAge(timestamp);
  }

  /**
   * update the duration metrics of region is transition
   * @param duration
   */
  public void updateRitDuration(long duration) {
    assignmentManagerSource.updateRitDuration(duration);
  }

  /*
   * TODO: Remove. This may not be required as assign and unassign operations are tracked separately
   * Increment the count of assignment operation (assign/unassign).
   */
  public void incrementOperationCounter() {
    assignmentManagerSource.incrementOperationCounter();
  }

  public void updateDeadServerOpenRegions(int deadRegions) {
    assignmentManagerSource.updateDeadServerOpenRegions(deadRegions);
  }

  public void updateUnknownServerOpenRegions(int unknownRegions) {
    assignmentManagerSource.updateUnknownServerOpenRegions(unknownRegions);
  }

  public void updateOrphanRegionsOnRs(int orphanRegionsOnRs) {
    assignmentManagerSource.setOrphanRegionsOnRs(orphanRegionsOnRs);
  }

  public void updateOrphanRegionsOnFs(int orphanRegionsOnFs) {
    assignmentManagerSource.setOrphanRegionsOnFs(orphanRegionsOnFs);
  }

  public void updateInconsistentRegions(int inconsistentRegions) {
    assignmentManagerSource.setInconsistentRegions(inconsistentRegions);
  }

  public void updateHoles(int holes) {
    assignmentManagerSource.setHoles(holes);
  }

  public void updateOverlaps(int overlaps) {
    assignmentManagerSource.setOverlaps(overlaps);
  }

  public void updateUnknownServerRegions(int unknownServerRegions) {
    assignmentManagerSource.setUnknownServerRegions(unknownServerRegions);
  }

  public void updateEmptyRegionInfoRegions(int emptyRegionInfoRegions) {
    assignmentManagerSource.setEmptyRegionInfoRegions(emptyRegionInfoRegions);
  }

  /**
   * @return Set of common metrics for assign procedure
   */
  public ProcedureMetrics getAssignProcMetrics() {
    return assignProcMetrics;
  }

  /**
   * @return Set of common metrics for unassign procedure
   */
  public ProcedureMetrics getUnassignProcMetrics() {
    return unassignProcMetrics;
  }

  /**
   * @return Set of common metrics for move procedure
   */
  public ProcedureMetrics getMoveProcMetrics() {
    return moveProcMetrics;
  }

  /**
   * @return Set of common metrics for reopen procedure
   */
  public ProcedureMetrics getReopenProcMetrics() {
    return reopenProcMetrics;
  }

  /**
   * @return Set of common metrics for OpenRegionProcedure
   */
  public ProcedureMetrics getOpenProcMetrics() {
    return openProcMetrics;
  }

  /**
   * @return Set of common metrics for CloseRegionProcedure
   */
  public ProcedureMetrics getCloseProcMetrics() {
    return closeProcMetrics;
  }

  /**
   * @return Set of common metrics for split procedure
   */
  public ProcedureMetrics getSplitProcMetrics() {
    return splitProcMetrics;
  }

  /**
   * @return Set of common metrics for merge procedure
   */
  public ProcedureMetrics getMergeProcMetrics() {
    return mergeProcMetrics;
  }
}
