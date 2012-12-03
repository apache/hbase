package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.List;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
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
    return createTableAndLoadData(tableName, "cf1", kvSize, 
        numKVs, 1, bulkLoad, null);
  }
  
  /**
   * Helper to create a table and load data into it.
   * 
   * @param tableName table name to create and load
   * @param cfNameStr cf to create
   * @param kvSize size of kv's to write
   * @param numKVs number of kv's to write into the table and cf
   * @param numRegionsPerRS numer of regions per RS, 0 for one region
   * @param bulkLoad if true, create HFile and load. Else do puts.
   * @param keys if not null, fills in the keys populated for bulk load case.
   * @return HTable instance to the table just created
   * @throws IOException
   */
  public HTable createTableAndLoadData(byte[] tableName, String cfNameStr, 
      int kvSize, long numKVs, int numRegionsPerRS, boolean bulkLoad, 
      List<byte[]> keysWritten) throws IOException {
    HTable htable = null;
    try {
      htable = new HTable(conf, tableName);
      LOG.info("Table " + new String(tableName) + " exists, skipping create.");
    } catch (IOException e) {
      LOG.info("Table " + new String(tableName) + " does not exist.");
    }
    
    int numRegions = numRegionsPerRS;
    if (htable == null) {
      // create the table - 1 version, no compression or bloomfilters
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes(cfNameStr))
        .setMaxVersions(1)
        .setCompressionType("NONE")
        .setBloomFilterType("NONE"));
      try {
        HBaseAdmin admin = new HBaseAdmin(conf);
        int numberOfServers = admin.getClusterStatus().getServers();
        numRegions = numberOfServers * numRegionsPerRS;
        if (numRegions <= 0) numRegions = 1;
        // create the correct number of regions
        if (numRegions == 1) {
          LOG.info("Creating table " + new String(tableName) + 
          " with 1 region");
          admin.createTable(desc);
        }
        else {
          // if we are asked for n regions, this will create (n+1), but the 
          // the first region is always skipped.
          byte[][] regionSplits = new byte[numRegions][];
          long delta = Math.round(numKVs * 1.0 / numRegions);
          for (int i = 0; i < numRegions; i++) {
            regionSplits[i] = getRowKeyFromLong(delta * (i+1));
          }
          LOG.info("Creating table " + new String(tableName) + " with " + 
              numRegions + " regions");
          admin.createTable(desc, regionSplits);
        }
      } catch (Exception e) {
        LOG.error("Failed to create table", e);
        System.exit(0);
      }
    }
    
    // get the table again
    if (htable == null) {
      try {
        htable = new HTable(conf, tableName);
        LOG.info("Table " + new String(tableName) + " exists, skipping create.");
      } catch (IOException e) {
        LOG.info("Table " + new String(tableName) + " does not exist.");
      }
    }
    
    // check if the table has any data
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(cfNameStr));
    ResultScanner scanner = htable.getScanner(scan);
    Result result = scanner.next();
    
    // if it has data we are done. Regenerate the keys we would have written if 
    // needed
    if (result != null && !result.isEmpty()) {
      LOG.info("Table " + new String(tableName) + " has data, skipping load");
      if (keysWritten != null) {
        NavigableMap<HRegionInfo, HServerAddress> regionsToRS = 
          htable.getRegionsInfo();
        // bulk load some data into the tables
        long numKVsInRegion = Math.round(numKVs * 1.0 / numRegions);
        for (HRegionInfo hRegionInfo : regionsToRS.keySet()) {
          // skip the first region which has an empty start key in case of 
          // multiple regions
          if (numRegions > 1 && 
              "".equals(new String(hRegionInfo.getStartKey()))) {
            continue;
          }
          long startKey = 0;
          try {
            startKey = getLongFromRowKey(hRegionInfo.getStartKey());
          } catch (NumberFormatException e) { }
          long rowID = startKey;
          for (; rowID < startKey + numKVsInRegion; rowID++) {
            byte[] row = getRowKeyFromLong(rowID);
            keysWritten.add(row);
          }
        }
      }
      return htable;
    }
    LOG.info("Table " + new String(tableName) + " has no data, loading");

    // load the data
    if (bulkLoad) {
      LOG.info("Bulk load of " + numKVs + " KVs of " + kvSize + 
          " bytes each requested.");
      // get the regions to RS map for this table
      htable = new HTable(conf, tableName);
      NavigableMap<HRegionInfo, HServerAddress> regionsToRS = 
        htable.getRegionsInfo();
      // bulk load some data into the tables
      long numKVsInRegion = Math.round(numKVs * 1.0 / numRegions);
      for (HRegionInfo hRegionInfo : regionsToRS.keySet()) {
        // skip the first region which has an empty start key
        if (numRegions > 1 && 
            "".equals(new String(hRegionInfo.getStartKey()))) {
          continue;
        }
        // get the region server
        HServerAddress hServerAddress = regionsToRS.get(hRegionInfo);
        HConnection connection = HConnectionManager.getConnection(conf);
        HRegionInterface regionServer = 
          connection.getHRegionConnection(hServerAddress);
        FileSystem fs = FileSystem.get(conf);
        Path hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
        Path basedir = new Path(hbaseRootDir, new String(tableName));
        bulkLoadDataForRegion(fs, basedir, hRegionInfo, regionServer, 
            cfNameStr, kvSize, numKVsInRegion, keysWritten);
      }
    } 
    else {
      // write data as puts
      LOG.info("Loading data for the table");
      String[] loadTestToolArgs = {
        "-zk", "localhost", 
        "-tn", new String(tableName),
        "-cf", cfNameStr,
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
   * Create a HFile and bulk load it for a given region
   * @throws IOException 
   */
  public void bulkLoadDataForRegion(FileSystem fs, Path basedir, 
      HRegionInfo hRegionInfo, HRegionInterface regionServer, String cfNameStr, 
      int kvSize, long numKVsInRegion, List<byte[]> keysWritten) 
  throws IOException {
    // create an hfile
    Path hFile =  new Path(basedir, "hfile." + hRegionInfo.getEncodedName() + 
        "." + System.currentTimeMillis());
    HFile.Writer writer =
      HFile.getWriterFactoryNoCache(conf).withPath(fs, hFile).create();
    byte [] family = 
      hRegionInfo.getTableDesc().getFamilies().iterator().next().getName();
    byte [] value = new byte[kvSize];
    (new Random()).nextBytes(value);

    long startKey = 0;
    try {
      startKey = getLongFromRowKey(hRegionInfo.getStartKey());
    } catch (NumberFormatException e) { }
    long rowID = startKey;
    for (; rowID < startKey + numKVsInRegion; rowID++) {
      byte[] row = getRowKeyFromLong(rowID);
      writer.append(new KeyValue(row, family, Bytes.toBytes(rowID), 
          System.currentTimeMillis(), value));
      if (keysWritten != null) keysWritten.add(row);
    }
    writer.close();
    LOG.info("Done creating data file: " + hFile.getName() + 
        ", hfile key-range: (" + startKey + ", " + rowID + 
        ") for region: " + hRegionInfo.getEncodedName() + 
        ", region key-range: (" + 
        (new String(hRegionInfo.getStartKey())).trim() + ", " + 
        (new String(hRegionInfo.getEndKey())).trim() + ")");

    // bulk load the file
    if (regionServer != null) {
      regionServer.bulkLoadHFile(hFile.toString(), hRegionInfo.getRegionName(), 
          Bytes.toBytes(cfNameStr), true);
      LOG.info("Done bulk-loading data file [" + hFile.getName() + 
          "] for region [" + hRegionInfo.getEncodedName() + "]");
    }
  }
  
  public static byte[] getRowKeyFromLong(long l) {
    return Bytes.toBytes(String.format("%20d", l));
  }
  
  public static long getLongFromRowKey(byte[] rowKey) {
    return Long.parseLong((new String(rowKey)).trim());
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
    Logger.getLogger("org.apache.hadoop.hbase.benchmark").setLevel(Level.DEBUG);    
    
    Benchmark benchmark = benchmarkClass.newInstance();    
    benchmark.parseArgs(args);
    benchmark.initialize();
    benchmark.initBenchmarkResults();
    benchmark.runBenchmark();
    benchmark.printBenchmarkResults();    
  }
}
