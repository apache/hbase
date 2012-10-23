package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.loadtest.ColumnFamilyProperties;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public abstract class Benchmark {
  public static final Log LOG = LogFactory.getLog(Benchmark.class);
  public static final String ARG_ZOOKEEPER = "--zookeeper";
  // use local zookeeper by default
  public String zkNodeName = null;
  // cached config object
  public Configuration conf;
  // benchmark results abstraction
  public BenchmarkResults benchmarkResults;

  /**
   * Initialize the benchmark results structure "benchmarkResults"
   */
  public abstract void initBenchmarkResults();
  
  /**
   * Run the actual benchmark
   * @throws Throwable
   */
  public abstract void runBenchmark() throws Throwable;
  
  /**
   * Parse the args to the benchmark
   * @param args
   */
  public void parseArgs(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(ARG_ZOOKEEPER)) {
        zkNodeName = args[i+1];
        i++;
      }
    }
  }
  
  public void initialize() {
    conf = HBaseConfiguration.create();
    if (zkNodeName != null) {
      conf.set("hbase.zookeeper.quorum", zkNodeName);
      conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    }
  }
  
  public void printBenchmarkResults() {
    System.out.println("Benchmark results");
    benchmarkResults.prettyPrint();
  }

  /**
   * Helper function to create a table and load the requested number of KVs 
   * into the table - if the table does not exist. If the table exists, then 
   * nothing is done.
   * @throws IOException 
   */
  public HTable createTableAndLoadData(byte[] tableName, int kvSize, 
      long numKVs, boolean flushData) throws IOException {
    HTable htable = null;
    try {
      htable = new HTable(conf, tableName);
    } catch (IOException e) {
      LOG.info("Table " + new String(tableName) + " already exists.");
    }
    
    if (htable != null) return htable;

    ColumnFamilyProperties familyProperty = new ColumnFamilyProperties();
    familyProperty.familyName = "cf1";
    familyProperty.minColsPerKey = 1;
    familyProperty.maxColsPerKey = 1;    
    familyProperty.minColDataSize = kvSize;
    familyProperty.maxColDataSize = kvSize;
    familyProperty.maxVersions = 1;
    familyProperty.compressionType = "none";
    
    ColumnFamilyProperties[] familyProperties = new ColumnFamilyProperties[1];
    familyProperties[0] = familyProperty;
    
    // create the table
    HBaseUtils.createTableIfNotExists(conf, tableName, familyProperties, 1);
    // write data if the table was created
    LOG.info("Loading data for the table");
    String[] loadTestToolArgs = {
      "-zk", "localhost", 
      "-tn", new String(tableName),
      "-cf", familyProperty.familyName,
      "-write", "1:" + kvSize, 
      "-num_keys", "" + numKVs, 
      "-multiput",
      "-compression", "NONE",
    };
    LoadTestTool.doMain(loadTestToolArgs);
    LOG.info("Done loading data");
    
    if (flushData) {
      LOG.info("Flush of data requested");
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.flush(tableName);
      try {
        Thread.sleep(2*1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Done flushing data");
    }
    
    htable = new HTable(conf, tableName);
    return htable;
  }
  
  public static void benchmarkRunner(
      Class<? extends Benchmark> benchmarkClass, String[] args) 
  throws Throwable {
    // suppress unnecessary logging
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(Level.ERROR);
    
    Benchmark benchmark = benchmarkClass.newInstance();    
    benchmark.parseArgs(args);
    benchmark.initialize();
    benchmark.initBenchmarkResults();
    benchmark.runBenchmark();
    benchmark.printBenchmarkResults();    
  }
}
