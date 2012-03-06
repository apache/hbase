package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;

public interface RegionPlacementPolicy {

  public Map<HRegionInfo, List<HServerInfo>> getFaroredNodesForNewRegions(
      final HRegionInfo[] newRegions, Collection<HServerInfo> serverInfo)
      throws IOException;

  public Map<HRegionInfo, List<HServerInfo>> getFaroredNodesForAllRegions()
      throws MasterNotRunningException, IOException, InterruptedException;
}
