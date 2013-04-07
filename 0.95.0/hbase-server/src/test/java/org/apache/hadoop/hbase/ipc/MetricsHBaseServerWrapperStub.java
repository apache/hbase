package org.apache.hadoop.hbase.ipc;

public class MetricsHBaseServerWrapperStub implements MetricsHBaseServerWrapper{
  @Override
  public long getTotalQueueSize() {
    return 101;
  }

  @Override
  public int getGeneralQueueLength() {
    return 102;
  }

  @Override
  public int getReplicationQueueLength() {
    return 103;
  }

  @Override
  public int getPriorityQueueLength() {
    return 104;
  }

  @Override
  public int getNumOpenConnections() {
    return 105;
  }
}
