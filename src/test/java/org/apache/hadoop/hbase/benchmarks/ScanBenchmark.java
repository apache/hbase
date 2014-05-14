package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This test compares the performance of scan when all the data is in memory 
 * (in the block cache). The setCaching parameter is varied and the read 
 * throughput is printed for various values of setCaching.
 */
public class ScanBenchmark extends Benchmark {
  public static final Log LOG = LogFactory.getLog(ScanBenchmark.class);
  private static final long PRINT_INTERVAL_KVS = 1000000;
  private byte[] tableName = Bytes.toBytes("bench.ScanFromMemoryPerf");
  private static Integer[] SET_CACHING_VALUES = { 
    5000,  6000,  7000,  8000, 
    9000,  10000, 11000, 12000, 
    13000, 14000, 15000, 16000,
    17000, 18000, 19000, 20000
  };
  private static Integer[] SET_PREFETCH_VALUES = { 0, 1 };
  
  public void initBenchmarkResults() {
    List<String> header = new ArrayList<String>();
    header.add("caching");
    for (int i = 0; i < SET_PREFETCH_VALUES.length; i++) {
      header.add("prefetch=" +  SET_PREFETCH_VALUES[i]);
    }
    benchmarkResults = new BenchmarkResults<Integer, Integer, Double>(
        SET_CACHING_VALUES, SET_PREFETCH_VALUES, "  %5d", "   %3.2f", header);
  }
  
  public void runBenchmark() throws Throwable {
    // populate the table
    createTableAndLoadData(tableName, 50, 1000000, true);
    // warm block cache, force jit compilation
    System.out.println("Warming blockcache and forcing JIT compilation...");
    for (int i = 0; i < 20; i++) {
      runExperiment(false, 10000, 0);
    }
    for (int caching : SET_CACHING_VALUES) {  
      for (int prefetch : SET_PREFETCH_VALUES) {
        try { 
          runExperiment(true, caching, prefetch); 
        } catch (IOException e) { 
          e.printStackTrace();  
        }
      }
    }
  }

  public void runExperiment(boolean printStats, int caching, int prefetch) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HTable htable = HBaseUtils.getHTable(conf, tableName);

    // create the scan object with the right params
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    // set caching
    scan.setCaching(caching);
    // set prefetch if needed
    if (prefetch > 0) {
      scan.setServerPrefetching(true);
    }
    
    long numKVs = 0;
    long numBytes = 0;
    Result kv;
    long printAfterNumKVs = PRINT_INTERVAL_KVS;
    long startTime = System.currentTimeMillis();
    
    // read all the KV's
    ResultScanner scanner = htable.getScanner(scan);
    while ( (kv = scanner.next()) != null) {
      numKVs += kv.size();
      if (kv.raw() != null) {
        for (KeyValue k : kv.raw())
          numBytes += k.getLength();
      }

      if (numKVs > printAfterNumKVs) {
        printAfterNumKVs += PRINT_INTERVAL_KVS;
        if (printStats) {
          printStats(numKVs, numBytes, startTime, caching, prefetch);
        }
      }
    }

    if (printStats) {
      printStats(numKVs, numBytes, startTime, caching, prefetch);
    }
    scanner.close();
  }

  private void printStats(long numKVs, long numBytes, long startTime, 
      int caching, int prefetch) {
    long t2 = System.currentTimeMillis();
    double numBytesInMB = numBytes * 1.0 / (1024 * 1024);
    double rate = numBytesInMB * (1000 * 1.0 / (t2 - startTime));
    System.out.println("Scan: caching = " + caching +
                     ", prefetch = " + prefetch +
                     ", kvs = " + numKVs +
                     ", bytes = " + String.format("%1$,.2f", numBytesInMB) + " MB" +
                     ", time = " + (t2 - startTime) + " ms" +
                     ", rate = " + String.format("%1$,.2f", rate) + "MB/s"
                     );
    benchmarkResults.addResult(caching, prefetch, rate);
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
