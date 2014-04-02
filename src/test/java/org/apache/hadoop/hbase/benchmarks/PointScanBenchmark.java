package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Histogram;

/**
 * Performs random point scans using 4 byte startRow and (startRow + 1)
 * @author manukranthk
 */
public class PointScanBenchmark {

  private final Log LOG = LogFactory.getLog(PointScanBenchmark.class);
  private byte[] tableName;
  private String zkQuorum;
  private byte[] family;
  private int times;
  private long threshold;
  private boolean profiling;

  public PointScanBenchmark(
      byte[] tableName,
      String zookeeperQuorum,
      byte[] family,
      int times,
      long threshold,
      boolean profiling) throws IOException {
    this.tableName = tableName;
    this.zkQuorum = zookeeperQuorum;
    this.family = family;
    this.times = times;
    this.threshold = threshold;
    this.profiling = profiling;
  }

  public void runBenchmarks() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    HTable table = new HTable(conf, tableName);
    long totalTime = 0;
    Histogram hist = new Histogram(10, 0, 100);
    table.setProfiling(profiling);
    List<Long> thresholdList = new ArrayList<Long>();
    List<Byte> thresholdBytes = new ArrayList<Byte>();
    int thresholdCount = 10000;
    Random rand = new Random();
    byte[] startRow = new byte[10];
    for (int i = 0; i < times; i++) {
      for (byte b = 0; b < 127; b++) {
        long startTime = System.nanoTime();
        rand.nextBytes(startRow);
        Scan s = getScan(startRow);
        ResultScanner scanner = table.getScanner(s);
        int cnt = 0;
        for (Result r : scanner) {
          cnt++;
        }
        long endTime = System.nanoTime();
        long curTime = endTime - startTime;
        totalTime += curTime;
        if (curTime > threshold) {
          LOG.debug("Adding to threshold list : " + curTime);
          if (profiling) {
            LOG.debug(table.getProfilingData().toPrettyString());
          }
          if (thresholdList.size() < thresholdCount) {
            thresholdList.add(curTime);
            thresholdBytes.add(b);
          }
        }
        table.getProfilingData();
        hist.addValue(curTime);
        LOG.debug(String.format("Printing the stats: Row Cnt : %d, Time Taken : %d ns, Byte : %d", cnt, curTime, b));
      }
    }
    LOG.debug(String.format("Avg time : %d ns", totalTime/times));
    LOG.debug("Histogram stats : P99 : " +hist.getPercentileEstimate(PercentileMetric.P95) + ", P95 : " + hist.getPercentileEstimate(PercentileMetric.P99));
    for (int i = 0; i< thresholdList.size(); i++) {
      LOG.debug("(" + thresholdBytes.get(i) + ", " + thresholdList.get(i) + ")");
    }
  }

  public Scan getScan(byte[] startRow) {
    Scan s = new Scan();
    s.setStartRow(startRow);
    return s;
  }

  /**
   * @param args
   * @throws ParseException
   * @throws IOException
   */
  public static void main(String[] args) throws ParseException, IOException {
    Options opt = new Options();
    opt.addOption(OptionBuilder.withArgName("tableName").hasArg()
        .withDescription("Table Name").create("t"));
    opt.addOption(OptionBuilder.withArgName("zookeeper").hasArg()
        .withDescription("Zookeeper Quorum").create("zk"));
    opt.addOption(OptionBuilder.withArgName("times").hasArg()
        .withDescription("Number of times to perform the scan").create("times"));
    opt.addOption(OptionBuilder.withArgName("family").hasArg()
        .withDescription("Column Family").create("cf"));
    opt.addOption(OptionBuilder.withArgName("threshold").hasArg()
        .withDescription("Threshold").create("th"));
    opt.addOption(OptionBuilder.withArgName("profiling").hasArg()
        .withDescription("Enable per request profiling").create("prof"));
    CommandLine cmd = new GnuParser().parse(opt, args);
    byte[] tableName = Bytes.toBytes(cmd.getOptionValue("t"));
    String zkQuorum = "";
    byte[] family = null;
    int times = 1000;
    long threshold = 100000000;
    boolean profiling = false;
    if (cmd.hasOption("zk")) {
      zkQuorum = cmd.getOptionValue("zk");
    }
    if (cmd.hasOption("cf")) {
      family = Bytes.toBytes(cmd.getOptionValue("cf"));
    }
    if (cmd.hasOption("times")) {
      times = Integer.parseInt(cmd.getOptionValue("times"));
    }
    if (cmd.hasOption("th")) {
      threshold = Long.parseLong(cmd.getOptionValue("th"));
    }
    if (cmd.hasOption("prof")) {
      profiling = Boolean.parseBoolean(cmd.getOptionValue("prof"));
    }
    PointScanBenchmark bench =
        new PointScanBenchmark(tableName, zkQuorum, family, times, threshold, profiling);
    bench.runBenchmarks();
  }
}
