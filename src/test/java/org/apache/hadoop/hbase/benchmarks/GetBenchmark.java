package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.util.Bytes;

public class GetBenchmark extends Benchmark {
  public static final Log LOG = LogFactory.getLog(GetBenchmark.class);
  static byte[] tableName = Bytes.toBytes("bench.GetFromMemory");
  static String cfName = "cf1";
  private static Integer[] CLIENT_THREADS = { 40, 50, 60, 70, 80, 90, 100 };
  private static Integer[] NUM_CONNECTIONS = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
  public static Configuration[] connCountConfs = new Configuration[100];
  public static final int numRegionsPerRS = 10;
  public static final int kvSize = 50;
  public static int numKVs = 1000000;
  
  /**
   * Initialize the benchmark results tracking and output.
   */
  public void initBenchmarkResults() {
    List<String> header = new ArrayList<String>();
    header.add("Threads");
    for (int i = 0; i < NUM_CONNECTIONS.length; i++) {
      header.add("   conn=" +  NUM_CONNECTIONS[i]);
    }
    benchmarkResults = new BenchmarkResults<Integer, Integer, Double>(
        CLIENT_THREADS, NUM_CONNECTIONS, "  %5d", "   %5.2f", header);
  }
  
  public void runBenchmark() throws Throwable {
    // populate the table, bulk load it
    createTableAndLoadData(tableName, cfName, kvSize, numKVs, numRegionsPerRS, 
        true);
    // warm block cache, force jit compilation
    System.out.println("Warming blockcache and forcing JIT compilation...");
    runExperiment("warmup-", false, 1, 100*1000, 1);
    // iterate on the number of connections
    for (int numConnections : NUM_CONNECTIONS) {
      LOG.info("Num connection = " + numConnections);
      // stop all connections so that we respect num-connections param
      HBaseRPC.stopClients();
      // vary for a number of client threads
      for (int numThreads : CLIENT_THREADS) {
        if (numConnections > numThreads) continue;
        try {
          // read enough KVs to benchmark within a reasonable time
          long numKVsToRead = 40*1000;
          if (numThreads >= 5) numKVsToRead = 20*1000;
          if (numThreads >= 40) numKVsToRead = 10*1000;
          // run the experiment
          runExperiment("t" + numThreads + "-", true, 
              numThreads, numKVsToRead, numConnections);
        } catch (IOException e) { 
          e.printStackTrace();
        }
      }
    }
  }

  public void runExperiment(String prefix, boolean printStats, 
      int numThreads, long numReadsPerThread, int numConnections) 
  throws IOException {
    // Prepare the read threads
    GetBenchMarkThread threads[] = new GetBenchMarkThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      if (connCountConfs[numConnections] == null) {
        connCountConfs[numConnections] = getNewConfObject();
        // set the number of connections per thread
        connCountConfs[numConnections].setInt(
            HBaseClient.NUM_CONNECTIONS_PER_SERVER, numConnections);
      }
      Configuration conf = connCountConfs[numConnections];
      threads[i] = new GetBenchMarkThread(prefix+i, tableName, 
          Bytes.toBytes(cfName), 0, numKVs, numReadsPerThread, conf, true);
    }
    // start the read threads, each one times itself
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }
    // wait for all the threads and compute the total ops/sec
    double totalOpsPerSec = 0;
    int successThreads = 0;
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i].join();
        totalOpsPerSec += threads[i].getOpsPerSecond();
        successThreads++;
      } catch (InterruptedException e) {
        LOG.error("Exception in thread " + i, e);
      }
    }
    System.out.println("Num threads =  " + successThreads + ", " + 
        "performance = " + String.format("%5.2f", totalOpsPerSec) + " ops/sec");
    // add to the benchmark results
    benchmarkResults.addResult(numThreads, numConnections, totalOpsPerSec);
  }
  
  /**
   * Thread that performs a given number of read operations and computes the 
   * number of read operations per second it was able to get.
   */
  public static class GetBenchMarkThread extends Thread {
    public static final long PRINT_INTERVAL = 20000;
    String name;
    byte[] table;
    byte[] cf;
    int startKey;
    int endKey;
    long numGetsToPerform;
    Configuration conf;
    long timeTakenMillis = 0;
    boolean debug = false;
    Random random = new Random();
    
    public GetBenchMarkThread(String name, byte[] table, byte[] cf, 
        int startKey, int endKey, long numGetsToPerform, Configuration conf, 
        boolean debug) {
      this.name = name;
      this.table = table;
      this.cf = cf;
      this.startKey = startKey;
      this.endKey = endKey;
      this.numGetsToPerform = numGetsToPerform;
      this.conf = conf;
      this.debug = debug;
    }
    
    /**
     * Returns the number of ops/second at the current moment.
     */
    public double getOpsPerSecond() {
      return (numGetsToPerform * 1.0 * 1000 / timeTakenMillis);
    }
    
    public void run() {
      try {
        // create a new HTable instance
        HTable htable = new HTable(conf, tableName);
        long rowKey = 0;
        // number of reads we have performed
        long numSuccessfulGets = 0;
        while (numSuccessfulGets < numGetsToPerform) {
          // create a random key in the range passed in
          rowKey = startKey + random.nextInt(endKey - startKey);
          // keys are assumed to be 20 chars
          Get get = 
            new Get(Bytes.toBytes(String.format("%20d", rowKey)));
          get.addFamily(cf);
          // time the actual get
          long t1 = System.currentTimeMillis();
          htable.get(get);
          timeTakenMillis += System.currentTimeMillis() - t1;
          numSuccessfulGets++;
          // print progress if needed
          if (debug && numSuccessfulGets % PRINT_INTERVAL == 0) {
            double opsPerSec = 
              (numSuccessfulGets * 1.0 * 1000 / timeTakenMillis);
            LOG.debug("[Thread-" + name + "] " + "" +
            		"Num gets = " + numSuccessfulGets + "/" + numGetsToPerform + 
            		", current rate = " + String.format("%.2f", opsPerSec) + 
            		" ops/sec");
          }
        }
      } catch (IOException e) {
        LOG.error("IOException while running read thread, will exit", e);
      }
    }
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
