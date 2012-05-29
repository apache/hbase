package org.apache.hadoop.hbase.thrift;

public interface IncrementCoalescerMBean {
  public int getQueueSize();

  public int getMaxQueueSize();

  public void setMaxQueueSize(int newSize);

  public long getPoolCompletedTaskCount();

  public long getPoolTaskCount();

  public int getPoolLargestPoolSize();

  public int getCorePoolSize();

  public void setCorePoolSize(int newCoreSize);

  public int getMaxPoolSize();

  public void setMaxPoolSize(int newMaxSize);

  public long getFailedIncrements();

  public long getSuccessfulCoalescings();

  public long getTotalIncrements();

  public long getCountersMapSize();
}
