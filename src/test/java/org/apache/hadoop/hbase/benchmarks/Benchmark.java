package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.NavigableMap;
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
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.loadtest.ColumnFamilyProperties;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.loadtest.RegionSplitter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Abstract class to run a benchmark. Extend this class to implement a 
 * benchmark. See GetBenchmark as an example.
 */
public abstract class Benchmark {
  public static final Log LOG = LogFactory.getLog(Benchmark.class);
  // the zk to run against, defaults to localhost
  public static final String ARG_ZOOKEEPER = "--zookeeper";
  // print help
  public static final String ARG_HELP = "--help";
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
      else if (args[i].equals(ARG_HELP)) {
        System.out.println("Usage: \n" +
            "bin/hbase org.apache.hadoop.hbase.benchmarks.<CLASSNAME>" 
            + " [--zookeeper zknode]\n");
        System.exit(0);
      }
    }
  }
  
  /**
   * Returns the zk nodename we are to run against
   */
  public String getZookeeperHostname() {
    return zkNodeName;
  }
  
  /**
   * Creates a new instance of a conf object
   */
  public Configuration getNewConfObject() {
    Configuration conf = HBaseConfiguration.create();
    if (zkNodeName != null) {
      conf.set("hbase.zookeeper.quorum", zkNodeName);
      conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    }
    return conf;
  }
  
  /**
   * Initialize this object.
   */
  public void initialize() {
    conf = getNewConfObject();
  }
  
  /**
   * Print the results of the benchmark
   */
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
      long numKVs, boolean bulkLoad) throws IOException {
    return createTableAndLoadData(tableName, "cf1", kvSize, numKVs, bulkLoad);
  }
  
  /**
   * Helper to create a table and load data into it.
   * 
   * @param tableName table name to create and load
   * @param cfNameStr cf to create
   * @param kvSize size of kv's to write
   * @param numKVs number of kv's to write into the table and cf
   * @param bulkLoad if true, create HFile and load. Else do puts.
   * @return HTable instance to the table just created
   * @throws IOException
   */
  public HTable createTableAndLoadData(byte[] tableName, String cfNameStr, 
      int kvSize, long numKVs, boolean bulkLoad) throws IOException {
    HTable htable = null;
    try {
      htable = new HTable(conf, tableName);
    } catch (IOException e) {
      LOG.info("Table " + new String(tableName) + " already exists.");
    }
    
    if (htable != null) return htable;

    // setup the column family properties
    ColumnFamilyProperties familyProperty = new ColumnFamilyProperties();
    familyProperty.familyName = cfNameStr;
    familyProperty.minColsPerKey = 1;
    familyProperty.maxColsPerKey = 1;    
    familyProperty.minColDataSize = kvSize;
    familyProperty.maxColDataSize = kvSize;
    familyProperty.maxVersions = 1;
    familyProperty.compressionType = "none";
    
    ColumnFamilyProperties[] familyProperties = new ColumnFamilyProperties[1];
    familyProperties[0] = familyProperty;
    
    // create the table
    LOG.info("Creating table " + new String(tableName));
    HBaseUtils.createTableIfNotExists(conf, tableName, familyProperties, 1);

    if (bulkLoad) {
      LOG.info("Bulk load of " + numKVs + " KVs of " + kvSize + 
          " bytes requested.");
      // get the regions to RS map for this table
      htable = new HTable(conf, tableName);
      NavigableMap<HRegionInfo, HServerAddress> regionsToRS = 
        htable.getRegionsInfo();
      // get the region and rs objects
      HRegionInfo hRegionInfo = regionsToRS.firstKey();
      HServerAddress hServerAddress = regionsToRS.get(hRegionInfo);
      // get the regionserver
      HConnection connection = HConnectionManager.getConnection(conf);
      HRegionInterface regionServer = 
        connection.getHRegionConnection(hServerAddress);

      // create an hfile
      LOG.info("Creating data files...");
      String tableNameStr = new String(tableName);
      FileSystem fs = FileSystem.get(conf);
      Path hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      Path basedir = new Path(hbaseRootDir, tableNameStr);
      Path hFile =  new Path(basedir, "hfile." + System.currentTimeMillis());
      HFile.Writer writer =
        HFile.getWriterFactoryNoCache(conf).withPath(fs, hFile).create();
      byte [] family = 
        hRegionInfo.getTableDesc().getFamilies().iterator().next().getName();
      byte [] value = new byte[kvSize];
      (new Random()).nextBytes(value);
      for (long rowID = 0; rowID < numKVs; rowID++) {
        // write a 20 byte fixed length key (Long.MAX_VALUE has 19 digits)
        byte[] row = Bytes.toBytes(String.format("%20d", rowID));
        writer.append(new KeyValue(row, family, Bytes.toBytes(rowID), 
            System.currentTimeMillis(), value));
      }
      writer.close();
      LOG.info("Done creating data file [" + hFile.getName() + 
          "], will bulk load data...");

      // bulk load the file
      regionServer.bulkLoadHFile(hFile.toString(), hRegionInfo.getRegionName(), 
          Bytes.toBytes(cfNameStr), true);
    } 
    else {
      // write data as puts
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
    }
    
    // make sure we can open the table just created and loaded
    htable = new HTable(conf, tableName);
    return htable;
  }
  
  /**
   * Sets up the environment for the benchmark. All extending class should call 
   * this method from their main() method.
   */
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
