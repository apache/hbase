package org.apache.hadoop.hbase.client.metrics;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class TableThreadMetricsHolder {
  private long queueWaitTime;
  private long taskExecutionTime;
  private boolean hasTaskFailed = false;

  public long getQueueWaitTime() {
    return queueWaitTime;
  }

  public long getTaskExecutionTime() {
    return taskExecutionTime;
  }

  public void setQueueWaitTime(long queueWaitTime) {
    this.queueWaitTime = queueWaitTime;
  }

  public void setTaskExecutionTime(long taskExecutionTime) {
    this.taskExecutionTime = taskExecutionTime;
  }

  public boolean hasTaskFailed() {
    return hasTaskFailed;
  }

  public void setHasTaskFailed(boolean hasTaskFailed) {
    this.hasTaskFailed = hasTaskFailed;
  }

}
