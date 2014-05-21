package org.apache.hadoop.hbase.benchmarks;

public class HDFSBenchmarks extends Benchmark {

  @Override
  public void initBenchmarkResults() {
    // TODO Auto-generated method stub
  }

  @Override
  public void runBenchmark() throws Throwable {
    // TODO Auto-generated method stub

  }

  public static void main(String[] args) throws Throwable {
    String className =
      Thread.currentThread().getStackTrace()[1].getClassName();
    System.out.println("Running benchmark " + className);
    @SuppressWarnings("unchecked")
    Class<? extends Benchmark> benchmarkClass =
      (Class<? extends Benchmark>)Class.forName(className);
    Benchmark.benchmarkRunner(benchmarkClass, args);
  }
}
