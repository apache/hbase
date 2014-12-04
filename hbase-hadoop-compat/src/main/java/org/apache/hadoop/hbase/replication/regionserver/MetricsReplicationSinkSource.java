package org.apache.hadoop.hbase.replication.regionserver;

public interface MetricsReplicationSinkSource {
  public static final String SINK_AGE_OF_LAST_APPLIED_OP = "sink.ageOfLastAppliedOp";
  public static final String SINK_APPLIED_BATCHES = "sink.appliedBatches";
  public static final String SINK_APPLIED_OPS = "sink.appliedOps";

  void setLastAppliedOpAge(long age);
  void incrAppliedBatches(long batches);
  void incrAppliedOps(long batchsize);
}
