package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionGetBenchmark extends Benchmark {
  public static final Log LOG = LogFactory.getLog(RegionGetBenchmark.class);
  static String cfName = "cf1";
  private static Integer[] NUM_THREADS = { 50, 60, 70, 80, 90, 100, 110, 120 };
  private static Integer[] NUM_REGIONS = { 10, 15, 20 };
  private Configuration conf;
  private FileSystem fs;
  private Path hbaseRootDir = null;
  private Path oldLogDir;
  private Path logDir;
  private static final String tableName = "dummyTable";
  public static final int kvSize = 50;
  public static int numKVs = 1000000;
  static List<byte[]> keysWritten = new ArrayList<byte[]>();
  
  public RegionGetBenchmark() throws IOException {
    conf = HBaseConfiguration.create();
    fs = FileSystem.get(conf);
    this.hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
    this.oldLogDir = 
      new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
    
    reinitialize();
  }
  
  public void reinitialize() throws IOException {
    if (fs.exists(this.hbaseRootDir)) {
      fs.delete(this.hbaseRootDir, true);
    }
    Path rootdir = fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
    fs.mkdirs(rootdir);    
  }

  /**
   * Initialize the benchmark results tracking and output.
   */
  public void initBenchmarkResults() {
    List<String> header = new ArrayList<String>();
    header.add("Threads");
    for (int i = 0; i < NUM_REGIONS.length; i++) {
      header.add("   " +  NUM_REGIONS[i] + " regions");
    }
    benchmarkResults = new BenchmarkResults<Integer, Integer, Double>(
        NUM_THREADS, NUM_REGIONS, "     %5d", "   %5.2f", header);
  }
  
  public void runBenchmark() throws Throwable {
    // cleanup old data
    Path basedir = new Path(this.hbaseRootDir, tableName);
    if (this.fs.exists(basedir)) {
      if (!this.fs.delete(basedir, true)) {
        throw new IOException("Failed remove of " + basedir);
      }
    }
    
    // create some data to read
    HLog wal = new HLog(FileSystem.get(conf), logDir, oldLogDir, conf, null);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cfName));
    hcd.setBlocksize(4 * 1024);
    htd.addFamily(hcd);
    HRegionInfo hRegionInfo = 
      new HRegionInfo(htd, getRowKeyFromLong(0), null, false);
    bulkLoadDataForRegion(fs, basedir, hRegionInfo, null, cfName, kvSize, 
        numKVs, keysWritten);

    // setup all the regions
    int maxRegions = NUM_REGIONS[0];
    for (int numRegion : NUM_REGIONS) {
      if (numRegion > maxRegions) maxRegions = numRegion;
    }    
    HRegion[] hRegions = new HRegion[maxRegions];
    for (int i = 0; i < maxRegions; i++) {
      hRegions[i] = HRegion.openHRegion(hRegionInfo, basedir, wal, this.conf);
    }
    System.out.println("Total kvs = " + keysWritten.size() + 
        ", num regions = " + NUM_REGIONS);
    // warm block cache, force jit compilation
    System.out.println("Warming blockcache and forcing JIT compilation...");
    runExperiment("warmup-", false, 1, 100*1000, hRegions, 1);

    for (int numRegions : NUM_REGIONS) {
      for (int numThreads : NUM_THREADS) {
        try {
          // read enough KVs to benchmark within a reasonable time
          long numKVsToRead = 100*1000;
          if (numThreads >= 5) numKVsToRead = 20*1000;
          if (numThreads >= 40) numKVsToRead = 10*1000;
          if (NUM_THREADS.length == 1) numKVsToRead = 10*1000*1000;
          // run the experiment
          runExperiment("t" + numThreads + "-", true, numThreads, numKVsToRead, 
              hRegions, numRegions);
        } catch (IOException e) { 
          e.printStackTrace();
        }
      }
    }
  }

  public void runExperiment(String prefix, boolean printStats, int numThreads, 
      long numReadsPerThread, HRegion[] hRegions, int numRegions) 
  throws IOException {
    // Prepare the read threads
    RegionGetBenchMarkThread threads[] = 
      new RegionGetBenchMarkThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new RegionGetBenchMarkThread(prefix+i, 
          Bytes.toBytes(tableName), Bytes.toBytes(cfName), 0, numKVs, 
          numReadsPerThread, hRegions[i%numRegions], true);
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
    benchmarkResults.addResult(numThreads, numRegions, totalOpsPerSec);
  }
  
  /**
   * Thread that performs a given number of read operations and computes the 
   * number of read operations per second it was able to get.
   */
  public static class RegionGetBenchMarkThread extends Thread {
    public static final long PRINT_INTERVAL = 20000;
    String name;
    byte[] table;
    byte[] cf;
    int startKey;
    int endKey;
    long numGetsToPerform;
    HRegion hRegion;
    long timeTakenMillis = 0;
    boolean debug = false;
    Random random = new Random();
    
    public RegionGetBenchMarkThread(String name, byte[] table, byte[] cf, 
        int startKey, int endKey, long numGetsToPerform, HRegion hRegion, 
        boolean debug) {
      this.name = name;
      this.table = table;
      this.cf = cf;
      this.startKey = startKey;
      this.endKey = endKey;
      this.numGetsToPerform = numGetsToPerform;
      this.hRegion = hRegion;
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
        byte[] rowKey = null;
        // number of reads we have performed
        long numSuccessfulGets = 0;
        while (numSuccessfulGets < numGetsToPerform) {
          // create a random key in the range passed in
          rowKey = keysWritten.get(random.nextInt(keysWritten.size()));
          // keys are assumed to be 20 chars
          Get get = new Get(rowKey);
          get.addFamily(cf);
          // time the actual get
          long t1 = System.currentTimeMillis();
          hRegion.get(get, null);
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
