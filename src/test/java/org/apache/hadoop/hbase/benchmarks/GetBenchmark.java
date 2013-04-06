package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.util.Bytes;

public class GetBenchmark extends Benchmark {
  public static final Log LOG = LogFactory.getLog(GetBenchmark.class);
  static String cfName;
  private static Integer[] CLIENT_THREADS;
  private static Integer[] NUM_CONNECTIONS;
  public static Map<Integer, Configuration> connCountConfs = new HashMap<Integer, Configuration>();
  public static int numRegionsPerRS;
  public static int kvSize;
  public static int numKVs;
  static List<byte[]> keysWritten = new ArrayList<byte[]>();
  static byte[] tableName;
  
  /**
   * Initialize the benchmark results tracking and output.
   */
  public void initBenchmarkResults() {

    Configuration conf = new Configuration();

    kvSize = conf.getInt("benchmark.kvSize", 100);
    tableName = Bytes.toBytes("bench.GetFromMemory." + String.format("%d", kvSize));
    //This is produce 1,000,000 kvs for 100 byte keys. For larger size kvs we want fewer
    //of them to keep the load time reasonable.
    numKVs = 1000000 / (kvSize/100);
    numRegionsPerRS = conf.getInt("benchmark.numRegionsPerRegionServer", 1);
    cfName = conf.get("benchmark.cfName", "cf1");

    String[] clientThreadsStr = conf.getStrings("benchmark.clientThreads", new String[]{"16", "32", "64", "128"});
    CLIENT_THREADS = new Integer[clientThreadsStr.length];
    int index = 0;
    for (String str : clientThreadsStr) {
      CLIENT_THREADS[index++] = new Integer(str);
    }

    String[] numConnectionsStr = conf.getStrings("benchmark.numConnections", new String[]{ "2", "4", "8", "16" });
    NUM_CONNECTIONS = new Integer[numConnectionsStr.length];
    index = 0;
    for (String str : numConnectionsStr) {
      NUM_CONNECTIONS[index++] = new Integer(str);
    }

    List<String> header = new ArrayList<String>();
    header.add("Threads");
    for (int i = 0; i < NUM_CONNECTIONS.length; i++) {
      header.add("   conn=" +  NUM_CONNECTIONS[i]);
    }
    benchmarkResults = new BenchmarkResults<Integer, Integer, Double>(
        CLIENT_THREADS, NUM_CONNECTIONS, "  %5d", "   %,10.0f", header);
  }
  
  public void runBenchmark() throws Throwable {
    // populate the table, bulk load it
    createTableAndLoadData(tableName, cfName, kvSize, numKVs, numRegionsPerRS, 
        true, keysWritten);
    System.out.println("Total kvs = " + keysWritten.size());
    // warm block cache, force jit compilation
    System.out.println("Warming blockcache and forcing JIT compilation...");
    runExperiment("warmup-", false, 1, 100000 * (kvSize/100), 1);
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
          long numKVsToRead = (1024*1024*1024)/(kvSize/4);
          if (numThreads >= 5) numKVsToRead /= 2;
          if (numThreads >= 40) numKVsToRead /= 2;
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

    //Used in the run loop for each worker thread
    AtomicBoolean running = new AtomicBoolean(true);
    //Used to sync all worker threads on startup
    CountDownLatch readySignal = new CountDownLatch(numThreads);
    //Used to signal to the worker threads to start generating load
    CountDownLatch startSignal = new CountDownLatch(1);
    //Used by the worker threads to signal when each one has started
    // generating load
    CountDownLatch providingLoad = new CountDownLatch(numThreads);
    //Used to tell the worker threads it is safe to start collecting
    //statistics as all the other threads are currently generating load
    AtomicBoolean startCollectingStats = new AtomicBoolean(false);
    //Used to tell the worker threads to stop collecting load
    AtomicBoolean stopCollectingStats = new AtomicBoolean(false);
    //Used by the worker threads to signal they are done collecting
    //statistics
    CountDownLatch doneSignal = new CountDownLatch(numThreads);

    // Prepare the worker threads
    GetBenchMarkThread threads[] = new GetBenchMarkThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      if (connCountConfs.get(numConnections) == null) {
        connCountConfs.put(numConnections, getNewConfObject());
        // set the number of connections per thread
        connCountConfs.get(numConnections).setInt(
            HBaseClient.NUM_CONNECTIONS_PER_SERVER, numConnections);
      }
      Configuration conf = connCountConfs.get(numConnections);
      threads[i] = new GetBenchMarkThread(prefix+i, tableName, 
          Bytes.toBytes(cfName), 0, numKVs, conf, true,
          running, readySignal, startSignal, providingLoad,
          startCollectingStats, stopCollectingStats, doneSignal);
    }
    // start the read threads, each one times itself
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }

    try {

      //System.out.println("Waiting on threads to start up.");
      //Signal Threads to start running test when all are ready
      readySignal.await();

      //System.out.println("Signaling threads to start generating load.");
      //Start generating load
      startSignal.countDown();

      //System.out.println("Waiting for all threads to start generating load.");
      //Wait for all thread to start Providing load
      providingLoad.await();

      //System.out.println("Signaling threads to start collecting stats.");
      //Signal threads to collect stats
      startCollectingStats.set(true);

      //System.out.println("Waiting while threads collect stats.");
      //Wait some time
      Thread.sleep(60 * 1000);

      //System.out.println("Signaling threads to stop collecting stats.");
      //Signal threads to stop collecting stats
      stopCollectingStats.set(true);

      //System.out.println("Waiting for all threads to stop collecting stats.");
      //Wait for all threads to be finished collecting stats
      doneSignal.await();

      //System.out.println("Signaling all threads to shutdown.");
      //Stop all client threads
      running.set(false);

    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    // wait for all the threads and compute the total ops/sec
    double totalOpsPerSec = 0;
    double totalBytesPerSec = 0;

    int successThreads = 0;
    for (int i = 0; i < numThreads; i++) {
      //threads[i].join();
      totalOpsPerSec += threads[i].getOpsPerSecond();
      totalBytesPerSec += threads[i].getBytesPerSecond();
      successThreads++;
    }
    System.out.println("Num threads =  " + successThreads + ", " + 
        "performance = " + String.format("%6.2f", totalOpsPerSec) + " ops/sec " +
        String.format("%,10.0f", totalBytesPerSec) + " bytes/sec");
    // add to the benchmark results
    benchmarkResults.addResult(numThreads, numConnections, totalBytesPerSec);
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
    // number of reads we have performed
    long numSuccessfulGets = 0;
    Configuration conf;
    long timeTakenMillis = 0;
    boolean debug = false;
    Random random = new Random();

    private boolean collectedStats = false;
    private boolean communicatedLoad = false;

    private AtomicBoolean running;
    private AtomicBoolean startCollectingStats;
    private AtomicBoolean stopCollectingStats;

    private CountDownLatch readySignal;
    private CountDownLatch startSignal;
    private CountDownLatch providingLoadSignal;
    private CountDownLatch doneSignal;
    
    public GetBenchMarkThread(String name, byte[] table, byte[] cf, 
        int startKey, int endKey, Configuration conf, boolean debug,
        AtomicBoolean running, CountDownLatch readySignal, CountDownLatch startSignal,
        CountDownLatch providingLoadSignal, AtomicBoolean startCollectingStats,
        AtomicBoolean stopCollectingStats, CountDownLatch doneSignal) {
      this.name = name;
      this.table = table;
      this.cf = cf;
      this.startKey = startKey;
      this.endKey = endKey;
      this.conf = conf;
      this.debug = debug;
      this.running = running;
      this.readySignal = readySignal;
      this.startSignal = startSignal;
      this.providingLoadSignal = providingLoadSignal;
      this.startCollectingStats = startCollectingStats;
      this.stopCollectingStats = stopCollectingStats;
      this.doneSignal = doneSignal;
    }

    /**
     * Returns the number of bytes/second at the current moment.
     */
    public double getBytesPerSecond() {
      return kvSize*getOpsPerSecond();
    }

    /**
     * Returns the number of ops/second at the current moment.
     */
    public double getOpsPerSecond() {
      return (numSuccessfulGets / (timeTakenMillis/1000.0));
    }
    
    public void run() {
      try {
        //Signal this thread is ready
        readySignal.countDown();
        //Wait for start signal
        startSignal.await();
        // create a new HTable instance
        HTable htable = new HTable(conf, tableName);
        byte[] rowKey = null;
        Result result;
        //Run until stopped
        while (running.get()) {

          //If this worker has collected stats in the past
          //and is now being told not to do so signal
          //that this thread is done.
          if (collectedStats &&
              stopCollectingStats.get()) {
            //Signal This thread is done collecting stats
            doneSignal.countDown();
          }

          // create a random key in the range passed in
          rowKey = keysWritten.get(random.nextInt(keysWritten.size()));

          Get get = new Get(rowKey);
          get.addFamily(cf);

          // time the actual get
          long t1 = System.currentTimeMillis();
          result = htable.get(get);
          long delta = System.currentTimeMillis() - t1;

          //Let the coordinating thread know we are providing load.
          //If we have not been counted as generating load before
          //count us now.
          if (!communicatedLoad) {
            providingLoadSignal.countDown();
            communicatedLoad = true;
          }

          //Test to make sure the result we got back is what we asked for
          if (result == null) {
            LOG.warn("Null result returned from HBase!!!");
          } else {
            byte [] resultRowKey = result.getRow();

            if(result.isEmpty()) {
              LOG.info("NO RESULTS RETURNED!!!");
            }

            if (Bytes.equals(rowKey, resultRowKey)) {
              if (!stopCollectingStats.get() &&
                  startCollectingStats.get()) {
                collectedStats = true;
                timeTakenMillis += delta;
                numSuccessfulGets++;
              }

            } else {
              LOG.warn("Row Keys didn't match!!! Get RowKey: " + Bytes.toStringBinary(get.getRow()) +
                  " result: " + Bytes.toStringBinary(result.getRow()) +
                  " rowKey: " + Bytes.toStringBinary(rowKey));
            }
          }
        }

      } catch (IOException e) {
        LOG.error("IOException while running read thread, will exit", e);
      } catch (InterruptedException e) {
        e.printStackTrace();
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
