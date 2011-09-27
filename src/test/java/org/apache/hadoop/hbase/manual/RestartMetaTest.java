package org.apache.hadoop.hbase.manual;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.manual.utils.HBaseUtils;
import org.apache.hadoop.hbase.manual.utils.KillProcessesAndVerify;
import org.apache.hadoop.hbase.manual.utils.MultiThreadedReader;
import org.apache.hadoop.hbase.manual.utils.MultiThreadedWriter;
import org.apache.hadoop.hbase.manual.utils.ProcessBasedLocalHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RestartMetaTest {
  static {
    // make the root logger display only errors
    Logger.getRootLogger().setLevel(Level.ERROR);
    // enable debugging for our package
    Logger.getLogger("org.apache.hadoop.hbase.manual").setLevel(Level.DEBUG);
    Logger.getLogger("org.apache.hadoop.hbase.manual").setLevel(Level.DEBUG);
  }

  // startup options
  public static Options options_ = new Options();

  private static HBaseConfiguration conf_;

  // command line options object
  public static CommandLine cmd_;

  // table name for the test
  public static byte[] tableName_ = Bytes.toBytes("test1");

  // column families used by the test
  public static byte[][] columnFamilies_ = { Bytes.toBytes("actions") };

  private static final Log LOG = LogFactory.getLog(RestartMetaTest.class);

  static Random random_ = new Random();


  public static void loadData() {
    // parse command line data
    long startKey = 0;
    long endKey = 100000;
    long minColsPerKey = 5;
    long maxColsPerKey = 15;
    int minColDataSize = 256;
    int maxColDataSize = 256 * 3;
    int numThreads = 10;

    // print out the args
    System.out.printf("Key range %d .. %d\n", startKey, endKey);
    System.out.printf("Number of Columns/Key: %d..%d\n", minColsPerKey, maxColsPerKey);
    System.out.printf("Data Size/Column: %d..%d bytes\n", minColDataSize, maxColDataSize);
    System.out.printf("Client Threads: %d\n", numThreads);

    // start the writers
    MultiThreadedWriter writer = new MultiThreadedWriter(conf_, tableName_, columnFamilies_[0]);
    writer.setBulkLoad(true);
    writer.setColumnsPerKey(minColsPerKey, maxColsPerKey);
    writer.setDataSize(minColDataSize, maxColDataSize);
    writer.start(startKey, endKey, numThreads);
    System.out.printf("Started loading data...");
    writer.waitForFinish();
    System.out.printf("Finished loading data...");
  }

  public static void main(String[] args) {
    try {
      // parse the command line args
      initAndParseArgs(args);

      String hbaseHome = cmd_.getOptionValue(OPT_HBASE_HOME);
      int numRegionServers = Integer.parseInt(cmd_.getOptionValue(OPT_NUM_RS));

      ProcessBasedLocalHBaseCluster hbaseCluster =
          new ProcessBasedLocalHBaseCluster(hbaseHome, numRegionServers);

      // start the process based HBase cluster
      conf_ = hbaseCluster.start();

      // create tables if needed
      HBaseUtils.createTableIfNotExists(conf_, tableName_, columnFamilies_);

      LOG.debug("Loading data....\n\n");
      loadData();

      LOG.debug("Sleeping 30 seconds....\n\n");
      HBaseUtils.sleep(30000);

      HServerAddress address = HBaseUtils.getMetaRS(conf_);
      int metaRSPort = address.getPort();

      LOG.debug("Killing META region server running on port " + metaRSPort);
      hbaseCluster.killRegionServer(metaRSPort);
      HBaseUtils.sleep(2000);

      LOG.debug("Restarting region server running on port metaRSPort");
      hbaseCluster.startRegionServer(metaRSPort);
      HBaseUtils.sleep(2000);

    } catch(Exception e) {
      e.printStackTrace();
      printUsage();
    }
  }

  private static String USAGE;
  private static final String HEADER = "HBase Tests";
  private static final String FOOTER = "";
  private static final String OPT_HBASE_HOME = "hbase_home";
  private static final String OPT_NUM_RS     = "num_rs";

  static void initAndParseArgs(String[] args) throws ParseException {
    // set the usage object
    USAGE =  "bin/hbase org.apache.hadoop.hbase.manual.RestartMetaTest "
            + "  -" + OPT_HBASE_HOME   + " <HBase home directory> "
            + "  -" + OPT_NUM_RS       + " <number of region servers> ";

    // add options
    options_.addOption(OPT_HBASE_HOME, true, "HBase home directory");
    options_.addOption(OPT_NUM_RS, true, "Number of Region Servers");

    // parse the passed in options
    CommandLineParser parser = new BasicParser();
    cmd_ = parser.parse(options_, args);
  }

  private static void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter( );
    helpFormatter.setWidth( 80 );
    helpFormatter.printHelp( USAGE, HEADER, options_, FOOTER );
  }
}
