package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Takes comma-separated list of (full/partial) region-names and output the
 * required information about that region
 */
public class GetHydraBaseRegionInfoUtil {
  private static Logger LOG = LoggerFactory.getLogger(
    GetHydraBaseRegionInfoUtil.class);

  public static void main(String[] args) throws IOException, RMapException {

    // Silent the noisy o/p
    org.apache.log4j.Logger.getLogger(
      "org.apache.zookeeper").setLevel(Level.ERROR);
    org.apache.log4j.Logger.getLogger(
      "org.apache.hadoop.conf.ClientConfigurationUtil").setLevel(Level.ERROR);
    org.apache.log4j.Logger.getLogger(
      "org.apache.hadoop.fs").setLevel(Level.ERROR);
    org.apache.log4j.Logger.getLogger(
      "org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.ERROR);

    String[] regions = args[0].split(",");
    Configuration conf = HBaseConfiguration.create();
    RMapConfiguration rMapConfiguration = new RMapConfiguration(conf);

    Map<String, HRegionInfo> regionInfoMap = new HashMap<>();
    List<HRegionInfo> regionInfoList;

    URI uri = rMapConfiguration.getRMapSubscription(conf);
    if (uri != null) {
      rMapConfiguration.readRMap(uri);
      regionInfoList = rMapConfiguration.getRegions(uri);
      for (HRegionInfo r : regionInfoList) {
        regionInfoMap.put(r.getEncodedName(), r);
      }
    }

    HRegionInfo region;
    for (String regionName : regions) {
      if ((region = regionInfoMap.get(regionName)) != null) {
        LOG.info(String.format("%s:[table: %s, start_key: %s, " +
          "end_key: %s, peers: %s]", regionName,
          region.getTableDesc().getNameAsString(),
          Bytes.toStringBinary(region.getStartKey()),
          Bytes.toStringBinary(region.getEndKey()),
          region.getQuorumInfo().getPeersAsString()));
      } else {
        LOG.error("No region found with encoded name " + regionName);
      }
    }
  }
}
