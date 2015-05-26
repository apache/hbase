package org.apache.hadoop.hbase.master.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.balancer.GroupLoadBalancerRegion;
import org.apache.hadoop.hbase.master.balancer.GroupLoadBalancerRegionServer;

import java.util.*;

public class GroupLoadBalancerConfiguration {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  private static final String DEFAULT_GROUP =
      "hbase.master.balancer.grouploadbalancer.defaultgroup";
  private static final String SERVER_GROUPS =
      "hbase.master.balancer.grouploadbalancer.servergroups";
  private static final String REGION_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.regiongroups.";

  private static final String SERVER_GROUP_DELIMITER = ";";
  private static final String SERVER_NAME_DELIMITER = "-";
  private static final String GROUP_NAME_DELIMITER = ";";

  private Map<String, GroupLoadBalancerRegionServer> serverGroups;
  private String defaultServerName;

  public GroupLoadBalancerConfiguration(Configuration configuration) {

    LOG.info("**************** STARTING GROUP LOAD BALANCER CONFIGURATION *******************");

    String defaultGroupString = configuration.get(DEFAULT_GROUP);
    String serverGroupsString = configuration.get(SERVER_GROUPS);

    this.serverGroups = getServerGroups(serverGroupsString);

    // Go through config and create server groups, and add their respective regions
    for (Map.Entry<String, GroupLoadBalancerRegionServer> regionServerEntry :
        serverGroups.entrySet()) {
      String serverName = regionServerEntry.getKey();
      GroupLoadBalancerRegionServer regionServer = regionServerEntry.getValue();
      String regionServerConfigString = configuration.get(REGION_GROUPS_PREFIX + serverName);
      LOG.info("**************** regionServerConfig " + regionServerConfigString);
      addRegionsToServerGroup(regionServer, serverName, regionServerConfigString);
    }

    defaultServerName = configuration.get(DEFAULT_GROUP);
    if (defaultServerName.length() == 0) {
      throw new IllegalArgumentException("hbase.master.balancer.grouploadbalancer.defaultgroup "
          + "must be defined in hbase-site.xml");
    }
    if(!serverGroups.containsKey(defaultServerName)) {
      throw new IllegalArgumentException("hbase.master.balancer.grouploadbalancer.defaultgroup "
          + "must be pre-existing group defined in "
          + "hbase.master.balancer.grouploadbalancer.servergroups");
    }

    LOG.info("**************** defaultServerName " + defaultServerName);

    LOG.info("**************** serverGroupNameList " + serverGroups);

  }

  // Given config from XML as String, create RegionServer objects from them and put them into a map
  public Map<String, GroupLoadBalancerRegionServer> getServerGroups(String serverGroupsString) {
    Map<String, GroupLoadBalancerRegionServer> serverGroupMap = new HashMap<>();
    String[] serverGroupsArray = serverGroupsString.split(SERVER_GROUP_DELIMITER);

    for (String serverGroup : serverGroupsArray) {
      String[] serverNameArray = serverGroup.split(SERVER_NAME_DELIMITER);
      String serverName = serverNameArray[0];
      String serverIP = serverNameArray[1];
      GroupLoadBalancerRegionServer regionServer =
          new GroupLoadBalancerRegionServer(serverName, serverIP);
      if (serverName.length() > 0) {
        serverGroupMap.put(serverName, regionServer);
      }
    }

    return serverGroupMap;
  }

  public void addRegionsToServerGroup(GroupLoadBalancerRegionServer regionServer,
      String serverName, String regionServerConfigString) {
    String[] regionNames = regionServerConfigString.split(GROUP_NAME_DELIMITER);
    for (String regionName : regionNames) {
      if (regionName.length() > 0) {
        GroupLoadBalancerRegion region = new GroupLoadBalancerRegion(regionName, serverName);
        regionServer.addRegion(region);
      }
    }
  }

}
