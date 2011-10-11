package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.FSUtils;

public class HBaseLocalityCheck {
  private final FileSystem fs;
  private final Path rootdir;
  private Map<String, String> preferredRegionToRegionServerMapping = null;
  private Configuration conf;
  private static final Log LOG =
    LogFactory.getLog(HBaseLocalityCheck.class.getName());

  public HBaseLocalityCheck(Configuration conf) throws IOException {
    this.conf = conf;
    this.rootdir = FSUtils.getRootDir(conf);
    this.fs = FileSystem.get(conf);
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
    preferredRegionToRegionServerMapping = FSUtils
        .getRegionLocalityMappingFromFS(fs, rootdir,
            conf.getInt("hbase.client.localityCheck.threadPoolSize", 2),
            conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 60 * 1000));

    Map<String, AtomicInteger> tableToRegionCountMap =
      new HashMap<String, AtomicInteger>();
    Map<String, AtomicInteger> tableToRegionsWithLocalityMap =
      new HashMap<String, AtomicInteger>();

    for (Map.Entry<String, String> entry :
        preferredRegionToRegionServerMapping.entrySet()) {
      // get region name and table
      String name = entry.getKey();
      int spliterIndex =name.lastIndexOf(":");
      String regionName = name.substring(spliterIndex+1);
      String tableName = name.substring(0, spliterIndex);

      //get region server hostname
      String bestHostName = entry.getValue();
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
    HBaseLocalityCheck localck = new HBaseLocalityCheck(conf);
    localck.showTableLocality();
    LOG.info("Locality Summary takes " +
        (System.currentTimeMillis() - startTime) + " ms to run" );
    Runtime.getRuntime().exit(0);
  }
}
