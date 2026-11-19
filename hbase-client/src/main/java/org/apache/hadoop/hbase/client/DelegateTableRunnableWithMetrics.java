/*
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.client.metrics.TableMetrics;
import org.apache.hadoop.hbase.client.metrics.TableThreadMetricsHolder;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Wraps a runnable to record metrics about execution of the runnable. The metrics are
 * recorded within {@link TableThreadMetricsHolder}.
 */
@InterfaceAudience.Private
public class DelegateTableRunnableWithMetrics implements Runnable {

  private long submissionTime;
  private Runnable delegate;

  private TableThreadMetricsHolder tableThreadMetricsHolder;

  public DelegateTableRunnableWithMetrics(Runnable delegate) {
    this.delegate = delegate;
    this.tableThreadMetricsHolder = new TableThreadMetricsHolder();
    this.submissionTime = EnvironmentEdgeManager.currentTime();
  }

  @Override public void run() {
    long executionStartTime = EnvironmentEdgeManager.currentTime();
    try {
      delegate.run();
    } catch (Exception e) {
      tableThreadMetricsHolder.setHasTaskFailed(true);
      throw e;
    }
    finally {
      long executionEndTime = EnvironmentEdgeManager.currentTime();
      tableThreadMetricsHolder.setTaskExecutionTime(executionEndTime - executionStartTime);
      tableThreadMetricsHolder.setQueueWaitTime(executionStartTime - submissionTime);
    }
  }

  public TableThreadMetricsHolder getTableThreadMetricsHolder() {
    return tableThreadMetricsHolder;
  }

  /**
   * Wraps the runnable within {@link DelegateTableRunnableWithMetrics} and registers
   * {@link TableThreadMetricsHolder}, created as part of wrapping runnable, in
   * {@link TableMetrics}.
   * @param runnable Runnable to wrap.
   * @param tableMetrics Table level metrics.
   * @return Wrapped runnable if table level metrics are enabled, otherwise the original runnable.
   */
  public static Runnable wrapInDelegateTableRunnableWithMetrics(Runnable runnable, TableMetrics tableMetrics) {
    if (tableMetrics.equals(TableMetrics.EMPTY_TABLE_METRICS)) {
      // Table level metrics are not enabled.
      return runnable;
    }
    DelegateTableRunnableWithMetrics runnableWrapper = new DelegateTableRunnableWithMetrics(runnable);
    tableMetrics.addThreadMetricsHolder(runnableWrapper.getTableThreadMetricsHolder());
    return runnableWrapper;
  }
}
