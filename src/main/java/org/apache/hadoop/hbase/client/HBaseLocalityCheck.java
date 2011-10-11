package org.apache.hadoop.hbase.client;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HBaseLocalityCheck {
  private static final Log LOG = LogFactory.getLog(HBaseLocalityCheck.class
      .getName());

  private Map<Writable, Writable> preferredRegionToRegionServerMapping = null;
  private Configuration conf;
  /**
   * The table we want to get locality for, or null in case we wanted a check
   * over all
   */
  private final String tableName;

  /**
   * Default constructor
   *
   * @param conf
   *          the configuration object to use
   * @param tableName
   *          the tableName we wish to get locality check over, or null if all
   * @throws IOException
   *           in case of file system issues
   */
  public HBaseLocalityCheck(Configuration conf, final String tableName) throws IOException {
    this.conf = conf;
    this.tableName = tableName;
  }

  /**
   * Show the locality information for each table. It will show how many regions
   * in each table and how many of them are running the best locality region
   * server
   *
   * @throws MasterNotRunningException
   * @throws IOException
   * @throws InterruptedException
   */
  public void showTableLocality()
      throws MasterNotRunningException, IOException, InterruptedException {
    // create a fsck object
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.initAndScanRootMeta();
    fsck.scanRegionServers();
    TreeMap<String, HbckInfo> regionInfo = fsck.getRegionInfo();

    boolean localityMatch = false;
    LOG.info("Locality information by region");

    // Get the locality info for each region by scanning the file system
    preferredRegionToRegionServerMapping = HMaster.reevaluateRegionLocality(conf,
        tableName,
        conf.getInt("hbase.client.localityCheck.threadPoolSize", 2));

    Map<String, AtomicInteger> tableToRegionCountMap =
      new HashMap<String, AtomicInteger>();
    Map<String, AtomicInteger> tableToRegionsWithLocalityMap =
      new HashMap<String, AtomicInteger>();

    for (Map.Entry<Writable, Writable> entry :
        preferredRegionToRegionServerMapping.entrySet()) {
      // get region name and table
      String name = ((Text)entry.getKey()).toString();
      int spliterIndex =name.lastIndexOf(":");
      String regionName = name.substring(spliterIndex+1);
      String tableName = name.substring(0, spliterIndex);

      //get region server hostname
      String bestHostName = ((Text)entry.getValue()).toString();
      localityMatch = false;
      HbckInfo region = regionInfo.get(regionName);
      if (region != null && region.deployedOn != null &&
            region.deployedOn.size() != 0) {
        String realHostName = null;
        List<HServerAddress> serverList = region.deployedOn;
        if (!tableToRegionCountMap.containsKey(tableName)){
          tableToRegionCountMap.put(tableName, new AtomicInteger(1));
          tableToRegionsWithLocalityMap.put(tableName, new AtomicInteger(0));
        } else {
          tableToRegionCountMap.get(tableName).incrementAndGet();
        }

        realHostName = serverList.get(0).getHostname();
        if (realHostName.equalsIgnoreCase(bestHostName)) {
          localityMatch = true;
          tableToRegionsWithLocalityMap.get(tableName).incrementAndGet();
        }

        LOG.info("<table:region> : <" + name + "> is running on host: "
            + realHostName + " \n and the prefered host is " + bestHostName +
            " [" + (localityMatch ? "Matched]" : "NOT matched]"));

      } else {
        LOG.info("<table:region> : <" + name + "> no info obtained for this" +
			" region from any of the region servers.");
        continue;
      }
    }

    LOG.info("======== Locality Summary ===============");
    for(String tableName : tableToRegionCountMap.keySet()) {
      int totalRegions = tableToRegionCountMap.get(tableName).get();
      int totalRegionsWithLocality =
        tableToRegionsWithLocalityMap.get(tableName).get();

      float rate = (totalRegionsWithLocality / (float) totalRegions) * 100;
      LOG.info("For Table: "+tableName+" ; #Total Regions: " + totalRegions +
          " ;" + " # Local Regions " + totalRegionsWithLocality + " rate = "
          + rate + " %");
    }

  }


  public static void main(String[] args) throws IOException,
      InterruptedException {
    long startTime = System.currentTimeMillis();
    Configuration conf = HBaseConfiguration.create();
    String tableName = null;
    Options opt = new Options();
    opt.addOption("D", true, "Override HBase Configuration Settings");
    opt.addOption("table", true,
        "Specify one precise table to scan for locality");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

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

      if (cmd.hasOption("table")) {
        tableName = cmd.getOptionValue("table");
      }
    } catch (ParseException e) {
      LOG.error("Could not parse", e);
      printUsageAndExit();
    }

    HBaseLocalityCheck localck = new HBaseLocalityCheck(conf, tableName);
    localck.showTableLocality();
    LOG.info("Locality Summary takes " +
        (System.currentTimeMillis() - startTime) + " ms to run" );
    Runtime.getRuntime().exit(0);
  }

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }

  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.err
        .println("Usage: hbase localityck [-D <conf.param=value>]* [-table <tableName>]");
    System.exit(0);
  }
}
