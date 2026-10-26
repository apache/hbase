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
package org.apache.hadoop.hbase.client.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Container for metrics related to a table. This class is thread-safe.
 *
 * One instance of {@link TableMetrics} is created for each instance of
 * {@link org.apache.hadoop.hbase.client.Table}. If instance of
 * {@link org.apache.hadoop.hbase.client.Table} is shared across multiple application threads
 * then {@link TableMetrics} will be collection of metrics recorded for all the application threads.
 */
@InterfaceAudience.Public
public class TableMetrics {

  private long queueWaitTime = 0;
  private long taskExecutionTime = 0;
  private int taskCount = 0;
  private int failedTaskCount = 0;


  public static final TableMetrics EMPTY_TABLE_METRICS = new TableMetrics() {
    @Override
    public List<TableThreadMetricsHolder> getThreadMetricsHolders() {
      return Collections.emptyList();
    }

    @Override
    public void addThreadMetricsHolder(TableThreadMetricsHolder threadMetricsHolder) {}
  };

  private final List<TableThreadMetricsHolder> threadMetricsHolders;

  public TableMetrics() {
    threadMetricsHolders = Collections.synchronizedList(new ArrayList<>());
  }

  public TableMetrics(int capacity) {
    threadMetricsHolders = Collections.synchronizedList(new ArrayList<>(capacity));
  }

  public void addThreadMetricsHolder(TableThreadMetricsHolder threadMetricsHolder) {
    threadMetricsHolders.add(threadMetricsHolder);
  }

  /**
   * Returns list of metric holders. The list is synchronized but make sure to
   * synchronize on returned list object while iterating the list.
   * <pre>
   *   synchronized (list) {
   *       Iterator i = list.iterator(); // Must be in synchronized block
   *       while (i.hasNext())
   *           foo(i.next());
   *   }
   * </pre>
   * Failure to follow this advice may result in non-deterministic behavior.
   * @return list of metric holders
   */
  public List<TableThreadMetricsHolder> getThreadMetricsHolders() {
    return threadMetricsHolders;
  }

  public void resetMetrics() {
    synchronized (threadMetricsHolders) {
      threadMetricsHolders.clear();
      queueWaitTime = 0;
      taskExecutionTime = 0;
      taskCount = 0;
      failedTaskCount = 0;
    }
  }

  public void combineMetrics() {
    long queueWaitTime = 0;
    long taskExecutionTime = 0;
    int taskCount = 0;
    int failedTaskCount = 0;
    synchronized (threadMetricsHolders) {
      for (TableThreadMetricsHolder threadMetricsHolder : threadMetricsHolders) {
        queueWaitTime += threadMetricsHolder.getQueueWaitTime();
        taskExecutionTime += threadMetricsHolder.getTaskExecutionTime();
        taskCount++;
        if (threadMetricsHolder.hasTaskFailed()) {
          failedTaskCount++;
        }
      }
      this.queueWaitTime = queueWaitTime;
      this.taskExecutionTime = taskExecutionTime;
      this.taskCount = taskCount;
      this.failedTaskCount = failedTaskCount;
    }
  }

  public long getQueueWaitTime() {
    return queueWaitTime;
  }

  public long getTaskExecutionTime() {
    return taskExecutionTime;
  }

  public int getTaskCount() {
    return taskCount;
  }

  public int getFailedTaskCount() {
    return failedTaskCount;
  }

}
