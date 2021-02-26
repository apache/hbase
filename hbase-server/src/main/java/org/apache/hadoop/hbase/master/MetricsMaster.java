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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.OperationMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;

/**
 * This class is for maintaining the various master statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsMaster {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsMaster.class);
  private MetricsMasterSource masterSource;
  private MetricsMasterProcSource masterProcSource;
  private MetricsMasterQuotaSource masterQuotaSource;

  private ProcedureMetrics serverCrashProcMetrics;

  public MetricsMaster(MetricsMasterWrapper masterWrapper) {
    masterSource = CompatibilitySingletonFactory.getInstance(MetricsMasterSourceFactory.class).create(masterWrapper);
    masterProcSource =
            CompatibilitySingletonFactory.getInstance(MetricsMasterProcSourceFactory.class).create(masterWrapper);
    masterQuotaSource =
            CompatibilitySingletonFactory.getInstance(MetricsMasterQuotaSourceFactory.class).create(masterWrapper);

    serverCrashProcMetrics = convertToProcedureMetrics(masterSource.getServerCrashMetrics());
  }

  // for unit-test usage
  public MetricsMasterSource getMetricsSource() {
    return masterSource;
  }

  public MetricsMasterProcSource getMetricsProcSource() {
    return masterProcSource;
  }

  public MetricsMasterQuotaSource getMetricsQuotaSource() {
    return masterQuotaSource;
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final long inc) {
    masterSource.incRequests(inc);
  }

  /**
   * Sets the number of space quotas defined.
   *
   * @see MetricsMasterQuotaSource#updateNumSpaceQuotas(long)
   */
  public void setNumSpaceQuotas(final long numSpaceQuotas) {
    masterQuotaSource.updateNumSpaceQuotas(numSpaceQuotas);
  }

  /**
   * Sets the number of table in violation of a space quota.
   *
   * @see MetricsMasterQuotaSource#updateNumTablesInSpaceQuotaViolation(long)
   */
  public void setNumTableInSpaceQuotaViolation(final long numTablesInViolation) {
    masterQuotaSource.updateNumTablesInSpaceQuotaViolation(numTablesInViolation);
  }

  /**
   * Sets the number of namespaces in violation of a space quota.
   *
   * @see MetricsMasterQuotaSource#updateNumNamespacesInSpaceQuotaViolation(long)
   */
  public void setNumNamespacesInSpaceQuotaViolation(final long numNamespacesInViolation) {
    masterQuotaSource.updateNumNamespacesInSpaceQuotaViolation(numNamespacesInViolation);
  }

  /**
   * Sets the number of region size reports the master currently has in memory.
   *
   * @see MetricsMasterQuotaSource#updateNumCurrentSpaceQuotaRegionSizeReports(long)
   */
  public void setNumRegionSizeReports(final long numRegionReports) {
    masterQuotaSource.updateNumCurrentSpaceQuotaRegionSizeReports(numRegionReports);
  }

  /**
   * Sets the execution time of a period of the QuotaObserverChore.
   *
   * @param executionTime The execution time in milliseconds.
   * @see MetricsMasterQuotaSource#incrementSpaceQuotaObserverChoreTime(long)
   */
  public void incrementQuotaObserverTime(final long executionTime) {
    masterQuotaSource.incrementSpaceQuotaObserverChoreTime(executionTime);
  }

  /**
   * @return Set of metrics for assign procedure
   */
  public ProcedureMetrics getServerCrashProcMetrics() {
    return serverCrashProcMetrics;
  }

  /**
   * This is utility function that converts {@link OperationMetrics} to {@link ProcedureMetrics}.
   *
   * NOTE: Procedure framework in hbase-procedure module accesses metrics common to most procedures
   * through {@link ProcedureMetrics} interface. Metrics source classes in hbase-hadoop-compat
   * module provides similar interface {@link OperationMetrics} that contains metrics common to
   * most operations. As both hbase-procedure and hbase-hadoop-compat are lower level modules used
   * by hbase-server (this) module and there is no dependency between them, this method does the
   * required conversion.
   */
  public static ProcedureMetrics convertToProcedureMetrics(final OperationMetrics metrics) {
    return new ProcedureMetrics() {
      @Override
      public Counter getSubmittedCounter() {
        return metrics.getSubmittedCounter();
      }

      @Override
      public Histogram getTimeHisto() {
        return metrics.getTimeHisto();
      }

      @Override
      public Counter getFailedCounter() {
        return metrics.getFailedCounter();
      }
    };
  }

  /**
   * Sets the execution time of a period of the {@code SnapshotQuotaObserverChore}.
   */
  public void incrementSnapshotObserverTime(final long executionTime) {
    masterQuotaSource.incrementSnapshotObserverChoreTime(executionTime);
  }

  /**
   * Sets the execution time to compute the size of a single snapshot.
   */
  public void incrementSnapshotSizeComputationTime(final long executionTime) {
    masterQuotaSource.incrementSnapshotObserverSnapshotComputationTime(executionTime);
  }

  /**
   * Sets the execution time to fetch the mapping of snapshots to originating table.
   */
  public void incrementSnapshotFetchTime(long executionTime) {
    masterQuotaSource.incrementSnapshotObserverSnapshotFetchTime(executionTime);
  }
}
