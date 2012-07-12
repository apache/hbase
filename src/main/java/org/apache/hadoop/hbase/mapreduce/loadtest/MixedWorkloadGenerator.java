package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * A generator which mixes insert and get operations in a single threadpool,
 * such that the ratio of operation types is strictly enforced.
 */
public class MixedWorkloadGenerator extends Workload.Generator {

  private static final byte[] columnFamily = "actions".getBytes();

  private int opsPerSecond = Integer.MAX_VALUE;
  private int numThreads = 20;
  private int insertWeight = 1;
  private int getWeight = 1;
  private double getVerificationFraction = 0.05;
  private double getProfilingFraction = LoadTest.DEFAULT_PROFILING_FRACTION;

  public List<List<Workload>> generateWorkloads(int numWorkloads, String args) {
    if (args != null) {
      String[] splits = args.split(":");
      if (splits.length != 6) {
        throw new IllegalArgumentException("Wrong number of argument splits");
      }
      opsPerSecond = Integer.parseInt(splits[0]);
      numThreads = Integer.parseInt(splits[1]);
      insertWeight = Integer.parseInt(splits[2]);
      getWeight = Integer.parseInt(splits[3]);
      getVerificationFraction = Double.parseDouble(splits[4]);
      getProfilingFraction = Double.parseDouble(splits[5]);
    }

    List<List<Workload>> workloads = new ArrayList<List<Workload>>(numWorkloads);
    for (int i = 0; i < numWorkloads; i++) {
      List<Workload> clientWorkloads = new ArrayList<Workload>();
      long startKey = Long.MAX_VALUE / numWorkloads * i;
      clientWorkloads.add(new MixedWorkload(startKey, opsPerSecond, numThreads,
          insertWeight, getWeight, getVerificationFraction,
          getProfilingFraction));
      workloads.add(clientWorkloads);
    }
    return workloads;
  }

  public HTableDescriptor getTableDescriptor() {
    HTableDescriptor desc = new HTableDescriptor();
    desc.addFamily(new HColumnDescriptor(columnFamily));
    return desc;
  }

  public static class MixedWorkload implements Workload {

    private static final long serialVersionUID = 576338016524909210L;
    private long startKey;
    private int opsPerSecond;
    private int numThreads;
    private int insertWeight;
    private int getWeight;
    private double getVerificationFraction;
    private double getProfilingFraction;

    public MixedWorkload(long startKey, int opsPerSecond, int numThreads,
        int insertWeight, int getWeight, double getVerificationFraction,
        double getProfilingFraction) {
      this.startKey = startKey;
      this.opsPerSecond = opsPerSecond;
      this.numThreads = numThreads;
      this.insertWeight = insertWeight;
      this.getWeight = getWeight;
      this.getVerificationFraction = getVerificationFraction;
      this.getProfilingFraction = getProfilingFraction;
    }

    public OperationGenerator constructGenerator() {
      KeyCounter keysWritten = new KeyCounter(startKey);
      PutGenerator insertGenerator =
          new PutGenerator(columnFamily, keysWritten, startKey, true,
              getProfilingFraction);
      GetGenerator getGenerator =
          new GetGenerator(columnFamily, keysWritten, getVerificationFraction,
              getProfilingFraction);

      CompositeOperationGenerator compositeGenerator =
          new CompositeOperationGenerator();
      compositeGenerator.addGenerator(insertGenerator, insertWeight);
      compositeGenerator.addGenerator(getGenerator, getWeight);
      return compositeGenerator;
    }

    public int getNumThreads() {
      return numThreads;
    }

    public int getOpsPerSecond() {
      return opsPerSecond;
    }

    public EnumSet<Operation.Type> getOperationTypes() {
      return EnumSet.of(Operation.Type.BULK_PUT, Operation.Type.GET);
    }
  }
}
