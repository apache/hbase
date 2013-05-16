package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class ClientLocalScanBenchmark {
  public final Log LOG = LogFactory.getLog(ClientLocalScanBenchmark.class);
  private static final long PRINT_INTERVAL_KVS = 100000;
  private static Integer[] CACHING_VALUES = {
    10000, 5000, 20000
  };
  private static boolean [] PREFETCH_VALUES = {
    true, false
  };
  private static byte[] tableName;
  private static byte[] startRow;
  private static byte[] endRow;
  private static byte[] zkQuorum;
  private static int version = 1;

  private static void runBenchmark() throws IOException {
    for (int caching : CACHING_VALUES) {
      for (boolean prefetch : PREFETCH_VALUES) {
        Scan s = new Scan();
        s.setStartRow(startRow);
        s.setStopRow(endRow);
        s.setCaching(caching);
        s.setServerPrefetching(prefetch);
        performScan(s);
      }
    }
  }

  private static void performScan(Scan s) throws IOException {
    Configuration c = HBaseConfiguration.create();
    c.set("hbase.zookeeper.quorum", Bytes.toString(zkQuorum));
    HTable table = new HTable(c, tableName);
    ResultScanner scanner = null;
    if (version == 1) {
      scanner = table.getScanner(s);
    } else if (version == 2) {
      scanner = table.getLocalScanner(s);
    }
    if (scanner == null) scanner = table.getScanner(s);
    long bytes = 0;
    long resultCnt = 0;
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    for (Result r : scanner) {
      resultCnt++;
      for (KeyValue kv : r.raw()) bytes += kv.getLength();
      if (resultCnt % PRINT_INTERVAL_KVS == 0) {
        printStats(resultCnt, bytes, startTime, s.getCaching(),
            s.getServerPrefetching());
      }
    }
    printStats(resultCnt, bytes, startTime, s.getCaching(),
        s.getServerPrefetching());
  }

  private static void printStats(long numKVs, long numBytes, long startTime,
      int caching, boolean prefetch) {
    long t2 = EnvironmentEdgeManager.currentTimeMillis();
    double numBytesInMB = numBytes * 1.0 / (1024 * 1024);
    double rate = numBytesInMB * (1000 * 1.0 / (t2 - startTime));
    System.out.println("Scan: caching = " + caching +
                     ", prefetch = " + prefetch +
                     ", kvs = " + numKVs +
                     ", bytes = " + String.format("%1$,.2f", numBytesInMB) + " MB" +
                     ", time = " + (t2 - startTime) + " ms" +
                     ", rate = " + String.format("%1$,.2f", rate) + "MB/s"
                     );
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws ParseException, IOException {
    // TODO Auto-generated method stub
    Options opt = new Options();
    opt.addOption(OptionBuilder.withArgName("startRow").hasArg().
        withDescription("Start row to scan from").create("st"));
    opt.addOption(OptionBuilder.withArgName("endRow").hasArg().
        withDescription("End row to scan till").create("en"));
    opt.addOption(OptionBuilder.withArgName("table").hasArg().
        withDescription("Table to scan over").create("t"));
    opt.addOption(OptionBuilder.withArgName("zkquorum").hasArg().
        withDescription("Zookeeper Quorum").create("zk"));
    opt.addOption(OptionBuilder.withArgName("version").hasArg().
        withDescription("Scanner Version(1 - Remote Scanner, 2 - Local Scanner").create("v"));
    opt.addOption("g", false, "use guava bytes comparator");
    opt.addOption("h", false, "Display this help");
    opt.addOption("detail", false, "Display full report");
    CommandLine cmd = new GnuParser().parse(opt, args);

    if (!cmd.getArgList().isEmpty() || cmd.hasOption("h")) {
      new HelpFormatter().printHelp("clientlocalscannerbenchmark", opt);
      return;
    }
    if (cmd.hasOption("t") && cmd.hasOption("zk") ) {
      if (cmd.hasOption("st")) {
        startRow = Bytes.toBytes(cmd.getOptionValue("st"));
      } else {
        startRow = Bytes.toBytes("");
      }
      if (cmd.hasOption("en")) {
        endRow = Bytes.toBytes(cmd.getOptionValue("en"));
      } else {
        endRow = Bytes.toBytes("");
      }
      if (cmd.hasOption("v")) {
        version = Integer.parseInt(cmd.getOptionValue("v"));
      }
      tableName = Bytes.toBytes(cmd.getOptionValue("t"));
      zkQuorum = Bytes.toBytes(cmd.getOptionValue("zk"));
    } else {
      new HelpFormatter().printHelp("clientlocalscannerbenchmark", opt);
      return;
    }
    runBenchmark();
  }
}
