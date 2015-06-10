package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

public class GroupLoadBalancerGroupedClusterFactory {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancerGroupedClusterFactory.class);

  private GroupLoadBalancerConfiguration configuration;
  private Map<ServerName, List<HRegionInfo>> clusterMap;
  private Map<String, Map<ServerName, List<HRegionInfo>>> groupedClusterMap;


  public GroupLoadBalancerGroupedClusterFactory(GroupLoadBalancerConfiguration configuration,
      Map<ServerName, List<HRegionInfo>> clusterMap) {
    this.configuration = configuration;
    this.clusterMap = clusterMap;
  }

  public Map<String, Map<ServerName, List<HRegionInfo>>> getGroupedClusters() {
    this.groupedClusterMap = new HashMap<>();

    for (Map.Entry<ServerName, List<HRegionInfo>> regionServer : this.clusterMap.entrySet()) {
      String serverNameString =
          GroupLoadBalancerUtils.getServerNameWithoutStartCode(
              regionServer.getKey().getServerName());
      String groupServerBelongsTo =
          configuration.getServer(serverNameString).getGroupServerBelongsTo();

      Map<ServerName, List<HRegionInfo>> newClusterEntry = new HashMap<>();
      newClusterEntry.put(regionServer.getKey(), null);

      groupedClusterMap.put(groupServerBelongsTo, newClusterEntry);
    }

    for (Map.Entry<ServerName, List<HRegionInfo>> regionServer : this.clusterMap.entrySet()) {
      for (HRegionInfo hri : regionServer.getValue()) {
        String regionName = hri.getRegionNameAsString();
        String tableName = GroupLoadBalancerUtils.getTableNameFromRegionName(regionName);

        // Get the group the table belongs to, if none is assigned, it belongs to the default group
        String groupTableBelongsTo = configuration.getTables().containsKey(tableName)?
            configuration.getTable(tableName).getGroupTableBelongsTo():
            configuration.getDefaultGroupName();

        // Get the server the region is currently assigned to. If the server that the region is
        // assigned to is in a different group, then assign it to the random server in its group
        // the group that its assigned to doesn't matter since it'll will be balanced later
        ServerName defaultServer =
            groupedClusterMap.get(groupTableBelongsTo).entrySet().iterator().next().getKey();
        ServerName serverTableBelongsTo =
            groupedClusterMap.get(groupTableBelongsTo).containsKey(regionServer.getKey())?
                regionServer.getKey():defaultServer;

        // Need to update listing in GroupLoadBalancerServer also to be consistent with cluster
        this.configuration.getTable(tableName).setGroupTableBelongsTo(groupTableBelongsTo);

        if (groupedClusterMap.get(groupTableBelongsTo).get(serverTableBelongsTo) == null) {
          List<HRegionInfo> hriList = new ArrayList<>();
          hriList.add(hri);
          groupedClusterMap.get(groupTableBelongsTo).put(serverTableBelongsTo, hriList);
        } else {
          groupedClusterMap.get(groupTableBelongsTo).get(serverTableBelongsTo).add(hri);
        }
      }
    }

    return this.groupedClusterMap;
  }


  public String toString() {
    return this.groupedClusterMap.toString();
  }
}
