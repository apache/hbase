package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * A workload that inserts key-values with new timestamps into existing rows of
 * a table which keeps all versions, using a single threadpool per client for
 * all operations.
 */
public class VersionWorkloadGenerator extends Workload.Generator {

  private static final byte[] columnFamily = "actions".getBytes();

  private int opsPerSecond = Integer.MAX_VALUE;
  private int numThreads = 20;
  private int insertWeight = 1;
  private int reinsertWeight = 1;
  private int getWeight = 1;
  private double getVerificationFraction = 0.05;
  private double getProfilingFraction = LoadTest.DEFAULT_PROFILING_FRACTION;

  public List<List<Workload>> generateWorkloads(int numWorkloads, String args) {

    if (args != null) {
      String[] splits = args.split(":");
      if (splits.length != 7) {
        throw new IllegalArgumentException("Wrong number of argument splits");
      }
      opsPerSecond = Integer.parseInt(splits[0]);
      numThreads = Integer.parseInt(splits[1]);
      insertWeight = Integer.parseInt(splits[2]);
      reinsertWeight = Integer.parseInt(splits[3]);
      getWeight = Integer.parseInt(splits[4]);
      getVerificationFraction = Double.parseDouble(splits[5]);
      getProfilingFraction = Double.parseDouble(splits[6]);
    }

    List<List<Workload>> workloads =
        new ArrayList<List<Workload>>(numWorkloads);
    for (int i = 0; i < numWorkloads; i++) {
      List<Workload> clientWorkloads = new ArrayList<Workload>();
      long startKey = Long.MAX_VALUE / numWorkloads * i;
      clientWorkloads.add(new MessagesWorkload(startKey, opsPerSecond,
          numThreads, insertWeight, reinsertWeight, getWeight,
          getVerificationFraction, getProfilingFraction));
      workloads.add(clientWorkloads);
    }
    return workloads;
  }

  public HTableDescriptor getTableDescriptor() {
    HTableDescriptor desc = new HTableDescriptor();

    // The column family should keep as many versions as possible.
    HColumnDescriptor colDesc = new HColumnDescriptor(columnFamily);
    colDesc.setMaxVersions(Integer.MAX_VALUE);
    desc.addFamily(colDesc);
    return desc;
  }

  public static class MessagesWorkload implements Workload {

    private static final long serialVersionUID = 576338016524909210L;
    private long startKey;
    private int opsPerSecond;
    private int numThreads;
    private int insertWeight;
    private int reinsertWeight;
    private int getWeight;
    private double getVerificationFraction;
    private double getProfilingFraction;

    public MessagesWorkload(long startKey, int opsPerSecond, int numThreads,
        int insertWeight, int getWeight, int reinsertWeight,
        double getVerificationFraction, double getProfilingFraction) {
      this.startKey = startKey;
      this.opsPerSecond = opsPerSecond;
      this.numThreads = numThreads;
      this.insertWeight = insertWeight;
      this.reinsertWeight = reinsertWeight;
      this.getWeight = getWeight;
      this.getVerificationFraction = getVerificationFraction;
      this.getProfilingFraction = getProfilingFraction;
    }

    public OperationGenerator constructGenerator() {
      KeyCounter keysWritten = new KeyCounter(startKey);
      PutGenerator insertGenerator =
          new PutGenerator(columnFamily, keysWritten, startKey, true, 
              getProfilingFraction);
      PutReGenerator insertReGenerator =
          new PutReGenerator(columnFamily, keysWritten, true,
              getProfilingFraction);
      GetGenerator getGenerator =
          new GetGenerator(columnFamily, keysWritten, getVerificationFraction, 
              getProfilingFraction, Integer.MAX_VALUE, 3600000);

      CompositeOperationGenerator compositeGenerator =
          new CompositeOperationGenerator();
      compositeGenerator.addGenerator(insertGenerator, insertWeight);
      compositeGenerator.addGenerator(insertReGenerator, reinsertWeight);
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
