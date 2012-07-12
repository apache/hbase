package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * A workload that has separate threadpools for insert operations and get
 * operations on each client.
 */
public class SeparateWorkloadGenerator extends Workload.Generator {

  private static final byte[] columnFamily = "actions".getBytes();

  private int insertThreads = 20;
  private int insertOpsPerSecond = insertThreads * 1000000;
  private int getThreads = 20;
  private int getOpsPerSecond = getThreads * 1000000;
  private double getVerificationFraction = 0.05;
  private double getProfilingFraction = 0.05;

  public List<List<Workload>> generateWorkloads(int numWorkloads, String args) {

    if (args != null) {
      String[] splits = args.split(":");
      if (splits.length != 6) {
        throw new IllegalArgumentException("Wrong number of argument splits");
      }
      insertOpsPerSecond = Integer.parseInt(splits[0]);
      insertThreads = Integer.parseInt(splits[1]);
      getOpsPerSecond = Integer.parseInt(splits[2]);
      getThreads = Integer.parseInt(splits[3]);
      getVerificationFraction = Double.parseDouble(splits[4]);
      getProfilingFraction = Double.parseDouble(splits[5]);
    }

    List<List<Workload>> workloads =
        new ArrayList<List<Workload>>(numWorkloads);
    for (int i = 0; i < numWorkloads; i++) {
      List<Workload> clientWorkloads = new ArrayList<Workload>();
      long startKey = Long.MAX_VALUE / numWorkloads * i;
      KeyCounter keysWritten = new KeyCounter(startKey);
      clientWorkloads.add(new GetWorkload(getOpsPerSecond, getThreads,
          getVerificationFraction, getProfilingFraction, keysWritten));
      clientWorkloads.add(new InsertWorkload(startKey, insertOpsPerSecond,
          insertThreads, getProfilingFraction, keysWritten));
      workloads.add(clientWorkloads);
    }
    return workloads;
  }

  public HTableDescriptor getTableDescriptor() {
    HTableDescriptor desc = new HTableDescriptor();
    desc.addFamily(new HColumnDescriptor(columnFamily));
    return desc;
  }

  public static class GetWorkload implements Workload {

    private static final long serialVersionUID = 4077118754127556529L;
    private int opsPerSecond;
    private int numThreads;
    private double getVerificationFraction;
    private double getProfilingFraction = LoadTest.DEFAULT_PROFILING_FRACTION;
    private KeyCounter keysWritten;

    public GetWorkload(int opsPerSecond, int numThreads,
        double getVerificationFraction, double getProfilingFraction,
        KeyCounter keysWritten) {
      this.opsPerSecond = opsPerSecond;
      this.numThreads = numThreads;
      this.getVerificationFraction = getVerificationFraction;
      this.getProfilingFraction = getProfilingFraction;
      this.keysWritten = keysWritten;
    }

    public OperationGenerator constructGenerator() {
      return new GetGenerator(columnFamily, keysWritten,
          getVerificationFraction, getProfilingFraction);
    }

    public int getNumThreads() {
      return numThreads;
    }

    public int getOpsPerSecond() {
      return opsPerSecond;
    }

    public EnumSet<Operation.Type> getOperationTypes() {
      return EnumSet.of(Operation.Type.GET);
    }
  }

  public static class InsertWorkload implements Workload {

    private static final long serialVersionUID = -6717959795026317422L;
    private long startKey;
    private int opsPerSecond;
    private int numThreads;
    private double getProfilingFraction;
    private KeyCounter keysWritten;

    public InsertWorkload(long startKey, int opsPerSecond, int numThreads,
        double getProfilingFraction, KeyCounter keysWritten) {
      this.startKey = startKey;
      this.opsPerSecond = opsPerSecond;
      this.numThreads = numThreads;
      this.getProfilingFraction = getProfilingFraction;
      this.keysWritten = keysWritten;
    }

    public OperationGenerator constructGenerator() {
      return new PutGenerator(columnFamily, keysWritten, startKey, true,
          getProfilingFraction);
    }

    public int getNumThreads() {
      return numThreads;
    }

    public int getOpsPerSecond() {
      return opsPerSecond;
    }

    public EnumSet<Operation.Type> getOperationTypes() {
      return EnumSet.of(Operation.Type.BULK_PUT);
    }
  }
}
