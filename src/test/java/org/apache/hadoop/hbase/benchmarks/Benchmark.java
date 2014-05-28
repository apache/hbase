package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Abstract class to run a benchmark. Extend this class to implement a 
 * benchmark. See GetBenchmark as an example.
 */
public abstract class Benchmark extends AbstractHBaseTool {
  public static final Log LOG = LogFactory.getLog(Benchmark.class);
  // the zk to run against, defaults to localhost
  public static final String ARG_ZOOKEEPER = "--zookeeper";
  // print help
  public static final String ARG_HELP = "--help";
  // use local zookeeper by default
  public String zkNodeName = null;
  // cached config object
  public Configuration conf = HBaseConfiguration.create();
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

  public Options getOptions(Options opt) {

    OptionBuilder.withArgName("property=value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Override HBase Configuration Settings");
    OptionBuilder.withValueSeparator('=');
    opt.addOption(OptionBuilder.create("D"));

    OptionBuilder.withArgName("zookeeper");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Zookeeper property");
    opt.addOption(OptionBuilder.create("zk"));


    OptionBuilder.withArgName("help");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Print help");
    opt.addOption(OptionBuilder.create("h"));
    return opt;
  }

  /**
   * Parse the args to the benchmark
   * @param args
   * @throws ParseException
   */
  public CommandLine parseArgs(String[] args) throws ParseException {
    Options opt = new Options();
    opt = getOptions(opt);
    CommandLine cmd = null;
    try {
      cmd = new GnuParser().parse(opt, args);
      if (cmd.hasOption("D")) {
        for (String confOpt : cmd.getOptionValues("D")) {
          String[] kv = confOpt.split("=", 2);
          if (kv.length == 2) {
            conf.set(kv[0], kv[1]);
            LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
          } else {
            throw new ParseException("-D option format invalid: " + confOpt);
          }
        }
      }
    } catch (ParseException e) {
      new HelpFormatter().printHelp("Benchmark", opt);
      System.exit(1);
    }
    // any unknown args or -h
    if (!cmd.getArgList().isEmpty() || cmd.hasOption("h")) {
      new HelpFormatter().printHelp("Benchmark", opt);
      System.exit(1);
    }
    return cmd;
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
    if (benchmarkResults != null) {
      benchmarkResults.prettyPrint();
    }
  }


  /**
   * Helper function to create a table and load the requested number of KVs 
   * into the table - if the table does not exist. If the table exists, then 
   * nothing is done.
   * @throws IOException 
   */
  public HTable createTableAndLoadData(byte[] tableName, int kvSize, 
      long numKVs, boolean bulkLoad) throws Exception {
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
   * @param keysWritten if not null, fills in the keys populated for bulk load case.
   * @return HTable instance to the table just created
   * @throws IOException
   */
  public HTable createTableAndLoadData(byte[] tableName, String cfNameStr, 
      int kvSize, long numKVs, int numRegionsPerRS, boolean bulkLoad, 
      List<byte[]> keysWritten) throws Exception {
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
          byte[] startKeyBytes = hRegionInfo.getStartKey();
          if (startKeyBytes.length != 0) {
            startKey = Bytes.toLong(startKeyBytes);
          }

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
            cfNameStr, kvSize, numKVsInRegion, tableName, keysWritten, conf);
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
      int ret;
      ret = ToolRunner.run(conf, new LoadTestTool(), loadTestToolArgs);
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
                                    int kvSize, long numKVsInRegion, byte[] tableName, List<byte[]> keysWritten,
                                    Configuration conf)
  throws IOException {

    HTable hTable = new HTable(conf, tableName);

    HFileOutputFormat hfof = new HFileOutputFormat();
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;

    String outputDirStr = conf.get("mapred.output.dir");

    if (outputDirStr == null || outputDirStr.equals("")) {
      conf.setStrings("mapred.output.dir", basedir.toString() + "/mapredOutput");
      outputDirStr = basedir.toString() + "/mapredOutput";
    }

    Path outputDir = new Path(outputDirStr);

    TaskAttemptContext cntx = new TaskAttemptContext(conf, new TaskAttemptID());

    try {
      writer = hfof.getRecordWriter(cntx);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    byte [] family = 
      hRegionInfo.getTableDesc().getFamilies().iterator().next().getName();
    byte [] value = new byte[kvSize];
    (new Random()).nextBytes(value);

    long startKey = 0;

    LOG.info(Bytes.toStringBinary(hRegionInfo.getRegionName()) +
        " Start Key: " + Bytes.toStringBinary(hRegionInfo.getStartKey()) +
        " End Key: " + Bytes.toStringBinary(hRegionInfo.getEndKey()));

    byte[] startKeyBytes = hRegionInfo.getStartKey();
    if (startKeyBytes.length != 0) {
      startKey = getLongFromRowKey(startKeyBytes);
    }

    long rowID = startKey;
    for (; rowID < startKey + numKVsInRegion; rowID++) {
      byte[] row = getRowKeyFromLong(rowID);
      KeyValue keyValue = new KeyValue(row, family, row,
          System.currentTimeMillis(), value);

      try {
        writer.write(new ImmutableBytesWritable(), keyValue);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      if (keysWritten != null) keysWritten.add(keyValue.getRow());

    }
    try {
      writer.close(cntx);
      hfof.getOutputCommitter(cntx).commitTask(cntx);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // bulk load the file
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, hTable);

    LOG.info("Done bulk loading file.");

  }

  public static byte[] getRowKeyFromLong(long l) {
    return Bytes.toBytes(l);
  }

  public static long getLongFromRowKey(byte[] rowKey) {
    return Bytes.toLong(rowKey);
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

  @Override
  protected void addOptions() {

  }

  @Override
  protected void processOptions(CommandLine cmd) {

  }

  @Override
  protected int doWork() throws Exception {
    return 0;
  }
}
