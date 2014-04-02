package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This test compares the performance of scan when all the data is in memory (in the block cache).
 * The setCaching parameter is varied and the read throughput is printed for various values of
 * setCaching.
 */
public class ScanSearch extends Benchmark {
  public static final Log LOG = LogFactory.getLog(ScanSearch.class);
  private static long PRINT_INTERVAL_KVS = 1000000;
  public static byte[] tableName = null;
  public static int cachingSize = 10000;
  public static boolean prefetch = true;
  public static boolean oneRegion = false;
  public static boolean localScan = true;
  public static int blockingPreloadingCount = 0;
  public static int nonBlockingPreloadingCount = 0;
  public static boolean clientSideScan = false;
  public static int max_regions = Integer.MAX_VALUE;
  public static boolean doProfiling = false;

  public void runBenchmark() throws Throwable {
    ArrayList<HRegionInfo> regions = this.getRegions();
    System.out.println("Size :" + regions.size());
    ArrayList<ScanRegion> scans = new ArrayList<ScanRegion>();
    int count = 0;
    for (HRegionInfo region : regions) {
      boolean mode = false;
      if (blockingPreloadingCount > 0) {
        blockingPreloadingCount--;
        mode = true;
      }
      ScanRegion s =
          new ScanRegion(true, mode, cachingSize, prefetch, clientSideScan,
              region);
      scans.add(s);
      s.start();
      if (oneRegion) {
        s.join();
      }
      count++;
      if (count == max_regions) break;
    }
    if (!oneRegion) {
      for (ScanRegion s : scans) {
        s.join();
      }
    }
  }

  public ArrayList<HRegionInfo> getRegions() throws IOException {
    Map<HRegionInfo, HServerAddress> regions = null;
    Configuration conf = HBaseConfiguration.create();
    ArrayList<HRegionInfo> scanRegions = new ArrayList<HRegionInfo>();
    conf.setInt("hbase.client.max.connections.per.server", 10);
    HTable htable = HBaseUtils.getHTable(conf, tableName);

    regions = htable.getRegionsInfo();

    String localHostName = InetAddress.getLocalHost().getHostName();
    String hostName = localHostName;

    if (!localScan) {
      Random generator = new Random();
      do {
        HServerAddress random =
            (HServerAddress) regions.values().toArray()[generator
                .nextInt(regions.size())];
        hostName = random.getInetSocketAddress().getHostName();
      } while (hostName.equals(localHostName));
    }
    System.out.println("Scanning form RS " + hostName);
    for (HRegionInfo region : regions.keySet()) {
      if (regions.get(region).getInetSocketAddress().getHostName()
          .equals(hostName)) {
        scanRegions.add(region);
      }
    }

    return scanRegions;
  }

  class ScanRegion extends Thread {

    int caching;
    boolean prefetch;
    boolean printStats;
    boolean preloadBlocks;
    boolean clientSideScan;

    HRegionInfo region;

    public ScanRegion(boolean printStats, boolean preloadBlocks, int caching,
        boolean prefetch, boolean clientSide, HRegionInfo region) {
      this.printStats = printStats;
      this.prefetch = prefetch;
      this.caching = caching;
      this.region = region;
      this.clientSideScan = clientSide;
      this.preloadBlocks = preloadBlocks;
    }

    public void run() {
      Configuration conf = HBaseConfiguration.create();
      HTable htable = HBaseUtils.getHTable(conf, tableName);
      System.out.println("Scanning Region : " + region.getRegionNameAsString()
          + region.getStartKey() + " " + region.getEndKey());
      // create the scan object with the right params
      Scan scan = new Scan();
      scan.setPreloadBlocks(preloadBlocks);
      scan.setMaxVersions(1);
      // set caching
      scan.setCaching(caching);
      scan.setStartRow(region.getStartKey());
      scan.setStopRow(region.getEndKey());
      scan.setCacheBlocks(false);

      scan.setServerPrefetching(prefetch);

      long numKVs = 0;
      long numBytes = 0;
      long printAfterNumKVs = PRINT_INTERVAL_KVS;
      long startTime = System.currentTimeMillis();

      // read all the KV's
      ResultScanner scanner = null;
      if (doProfiling) {
        htable.setProfiling(true);
      }
      try {
        if (!clientSideScan) {
          scanner = htable.getScanner(scan);
        } else {
          // Need to set the necessary conf so this can work
          scanner = htable.getLocalScanner(scan);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return;
      }
      try {
        for (Result kv : scanner) {
          numKVs += kv.size();
          if (kv.raw() != null) {
            for (KeyValue k : kv.raw())
              numBytes += k.getLength();
          }

          if (numKVs > printAfterNumKVs) {
            if (doProfiling) {
              System.out.println(htable.getProfilingData().toPrettyString());
            }
            printAfterNumKVs += PRINT_INTERVAL_KVS;
            if (printStats) {
              printStats(region.getRegionNameAsString(), numKVs, numBytes,
                startTime, caching, prefetch, preloadBlocks);
            }
          }
        }
      } catch (Exception e) {
        LOG.debug("Caught exception", e);
      } finally {
        scanner.close();
      }

      if (printStats) {
        printStats(region.getRegionNameAsString(), numKVs, numBytes, startTime,
          caching, prefetch, preloadBlocks);
      }
    }
  }

  private void printStats(String name, long numKVs, long numBytes,
      long startTime, int caching, boolean prefetch2, boolean preload) {
    long t2 = System.currentTimeMillis();
    double numBytesInMB = numBytes * 1.0 / (1024 * 1024);
    double rate = numBytesInMB * (1000 * 1.0 / (t2 - startTime));
    System.out.println("Scan: Name : " + name + " caching = " + caching
        + ", prefetch = " + prefetch2 + ", preload = " + preload + ", kvs = "
        + numKVs + ", bytes = " + String.format("%1$,.2f", numBytesInMB)
        + " MB" + ", time = " + (t2 - startTime) + " ms" + ", rate = "
        + String.format("%1$,.2f", rate) + "MB/s");
  }

  public static void main(String[] args) throws Throwable {
    Options opt = new Options();
    opt.addOption(OptionBuilder.withArgName("tableName").hasArg()
        .withDescription("Table Name").create("t"));
    opt.addOption(OptionBuilder.withArgName("zookeeper").hasArg()
        .withDescription("zookeeper node to contact").create("zk"));
    opt.addOption(OptionBuilder.withArgName("prefetch").hasArg()
        .withDescription("prefetch enabled").create("p"));
    opt.addOption(OptionBuilder.withArgName("caching").hasArg()
        .withDescription("Caching Size").create("c"));
    opt.addOption(OptionBuilder.withArgName("sequential").hasArg()
        .withDescription("One Region at a time").create("s"));
    opt.addOption(OptionBuilder.withArgName("Local").hasArg()
        .withDescription("Local scan only, true by default").create("l"));
    opt.addOption(OptionBuilder.withArgName("preloadcnt").hasArg()
        .withDescription("Number of scanners preloading").create("x"));
    opt.addOption(OptionBuilder.withArgName("maxregions").hasArg()
        .withDescription("Max number of regions to scan").create("n"));
    opt.addOption(OptionBuilder.withArgName("print-interval").hasArg()
        .withDescription("Number of key values after which we " +
            "can print the stats.").create("pi"));
    opt.addOption(OptionBuilder.withArgName("useProfiling").hasArg()
        .withDescription("Set per request profiling data and get it").create("prof"));

    CommandLine cmd = new GnuParser().parse(opt, args);
    ScanSearch.tableName = Bytes.toBytes(cmd.getOptionValue("t"));
    if (cmd.hasOption("c")) {
      ScanSearch.cachingSize = Integer.parseInt(cmd.getOptionValue("c"));
    }
    if (cmd.hasOption("p")) {
      ScanSearch.prefetch = Boolean.parseBoolean(cmd.getOptionValue("p"));
    }
    if (cmd.hasOption("s")) {
      ScanSearch.oneRegion = Boolean.parseBoolean(cmd.getOptionValue("s"));
    }
    if (cmd.hasOption("l")) {
      ScanSearch.localScan = Boolean.parseBoolean(cmd.getOptionValue("l"));
    }
    if (cmd.hasOption("x")) {
      ScanSearch.blockingPreloadingCount =
          Integer.parseInt(cmd.getOptionValue("x"));
    }
    if (cmd.hasOption("n")) {
      ScanSearch.max_regions = Integer.parseInt(cmd.getOptionValue("n"));
    }
    if (cmd.hasOption("pi")) {
      ScanSearch.PRINT_INTERVAL_KVS = Integer.parseInt(cmd.getOptionValue("pi"));
    }
    if (cmd.hasOption("prof")) {
      ScanSearch.doProfiling = Boolean.parseBoolean(cmd.getOptionValue("prof"));
    }
    String className = Thread.currentThread().getStackTrace()[1].getClassName();
    System.out.println("Running benchmark " + className);
    @SuppressWarnings("unchecked")
    Class<? extends Benchmark> benchmarkClass =
        (Class<? extends Benchmark>) Class.forName(className);
    Benchmark.benchmarkRunner(benchmarkClass, args);
  }

  @Override
  public void initBenchmarkResults() {
    // TODO Auto-generated method stub
  }
}
