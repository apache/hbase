package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.FSUtils;

public class HBaseLocalityCheck {
  private static final Log LOG = LogFactory.getLog(HBaseLocalityCheck.class
      .getName());

  private Map<String, Map<String, Float>> localityMap = null;
  private Configuration conf;
  /**
   * The table we want to get locality for, or null in case we wanted a check
   * over all
   */
  private String tableName = null;

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
  public HBaseLocalityCheck(Configuration conf, final String tableName)
  throws IOException {
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
  public void showTableLocality() throws MasterNotRunningException,
      IOException, InterruptedException {
    // create a fsck object
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.initAndScanRootMeta();
    fsck.scanRegionServers();
    TreeMap<String, HbckInfo> regionInfo = fsck.getRegionInfo();

    // Get the locality info for each region by scanning the file system
    localityMap = FSUtils.getRegionDegreeLocalityMappingFromFS(conf, tableName);

    Map<String, Integer> tableToRegionCntMap =  new HashMap<String, Integer>();
    Map<String, Float> tableToLocalityMap = new HashMap<String, Float>();
    int numUnknownRegion = 0;

    for (Map.Entry<String, HbckInfo> entry : regionInfo.entrySet()) {
      String regionEncodedName = entry.getKey();
      Map<String, Float> localityInfo = localityMap.get(regionEncodedName);
      HbckInfo hbckInfo = entry.getValue();
      if (hbckInfo == null || hbckInfo.metaEntry == null
          || localityInfo == null || hbckInfo.deployedOn == null
          || hbckInfo.deployedOn.isEmpty()) {
        LOG.warn("<" + regionEncodedName + "> no info" +
        		" obtained for this region from any of the region servers.");
        numUnknownRegion++;
        continue;
      }

      String tableName = hbckInfo.metaEntry.getTableDesc().getNameAsString();
      String realHostName = hbckInfo.deployedOn.get(0).getHostname();
      Float localityPercentage = localityInfo.get(realHostName);
      if (localityPercentage == null)
        localityPercentage = new Float(0);

      if (!tableToRegionCntMap.containsKey(tableName)) {
        tableToRegionCntMap.put(tableName, new Integer(1));
        tableToLocalityMap.put(tableName, localityPercentage);
      } else {
        tableToRegionCntMap.put(tableName,
           (tableToRegionCntMap.get(tableName) + 1));
        tableToLocalityMap.put(tableName,
           (tableToLocalityMap.get(tableName) + localityPercentage));
      }
      LOG.info("<" + tableName + " : " + regionEncodedName +
          "> is running on host: " + realHostName + " \n " +
          		"and the locality is " + localityPercentage);
    }

    LOG.info("======== Locality Summary ===============");
    for (String tableName : tableToRegionCntMap.keySet()) {
      int totalRegions = tableToRegionCntMap.get(tableName).intValue();
      float totalRegionsLocality = tableToLocalityMap.get(tableName)
          .floatValue();

      float averageLocality = (totalRegionsLocality / (float) totalRegions);
      LOG.info("For Table: " + tableName + " ; #Total Regions: " + totalRegions
          + " ; The average locality is " + averageLocality * 100 + " %");
    }
    if (numUnknownRegion != 0) {
      LOG.info("The number of unknow regions is " + numUnknownRegion);
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
