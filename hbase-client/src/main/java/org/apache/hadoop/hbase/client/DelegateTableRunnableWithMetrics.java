package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.metrics.TableMetrics;
import org.apache.hadoop.hbase.client.metrics.TableThreadMetricsHolder;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.apache.yetus.audience.InterfaceAudience;

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

  public static Runnable wrapInDelegateTableRunnableWithMetrics(Runnable runnable, TableMetrics tableMetrics) {
    if (tableMetrics.equals(TableMetrics.EMPTY_TABLE_METRICS)) {
      return runnable;
    }
    DelegateTableRunnableWithMetrics runnableWrapper = new DelegateTableRunnableWithMetrics(runnable);
    tableMetrics.addThreadMetricsHolder(runnableWrapper.getTableThreadMetricsHolder());
    return runnableWrapper;
  }
}
