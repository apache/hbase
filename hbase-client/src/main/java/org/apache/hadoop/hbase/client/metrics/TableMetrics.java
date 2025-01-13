package org.apache.hadoop.hbase.client.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

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
