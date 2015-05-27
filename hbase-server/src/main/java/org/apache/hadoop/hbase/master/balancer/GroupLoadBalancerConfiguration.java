package org.apache.hadoop.hbase.master.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.StringBuilder;
import java.util.*;

public class GroupLoadBalancerConfiguration {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  private static final String GROUPS = "hbase.master.balancer.grouploadbalancer.groups";
  private static final String DEFAULT_GROUP =
      "hbase.master.balancer.grouploadbalancer.defaultgroup";
  private static final String SERVER_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.servergroups.";
  private static final String REGION_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.regiongroups.";

  private static final String GROUP_DELIMITER = ";";

  private Map<String, GroupLoadBalancerGroup> groups;

  public GroupLoadBalancerConfiguration(Configuration configuration) {

    LOG.info("**************** STARTING GROUP LOAD BALANCER CONFIGURATION *******************");

    this.groups = new HashMap<>();

    String groupNamesString = configuration.get(GROUPS);
    String[] groupNamesArray = groupNamesString.split(GROUP_DELIMITER);

    // Build group configurations
    for (String groupName : groupNamesArray) {

      if (groupName.length() < 1) {
        throw new IllegalArgumentException("Group name cannot be null.");
      }

      GroupLoadBalancerGroup group = new GroupLoadBalancerGroup(groupName);

      addRegionsToGroup(group, configuration.get(REGION_GROUPS_PREFIX + groupName));
      addServersToGroup(group, configuration.get(SERVER_GROUPS_PREFIX + groupName));

      if (this.groups.containsKey(groupName)) {
        throw new IllegalArgumentException("Group name cannot be duplicated");
      }

      this.groups.put(groupName, group);
    }

    String defaultGroupName = configuration.get(DEFAULT_GROUP);
    if (defaultGroupName.length() < 1) {
      throw new IllegalArgumentException("Default group name cannot be null");
    }
    if (!this.groups.containsKey(defaultGroupName)) {
      throw new IllegalArgumentException("Default group name must be a pre-existing group name");
    }

    LOG.info("**************** groups " + toString());

  }

  public void addRegionsToGroup(GroupLoadBalancerGroup group, String regionsString) {
    String groupRegionsBelongTo = group.getName();
    String[] regionsArray = regionsString.split(GROUP_DELIMITER);
    for (String regionName : regionsArray) {
      GroupLoadBalancerRegion region =
          new GroupLoadBalancerRegion(regionName, groupRegionsBelongTo);
      group.addRegion(region);
    }
  }

  public void addServersToGroup(GroupLoadBalancerGroup group, String serversString) {
    String groupServersBelongTo = group.getName();
    String[] serversArray = serversString.split(GROUP_DELIMITER);
    for (String serverName : serversArray) {
      GroupLoadBalancerServer server =
          new GroupLoadBalancerServer(serverName, groupServersBelongTo);
      group.addServer(server);
    }
  }

  public String toString() {
    StringBuilder description = new StringBuilder();
    description.append("Groups List: \n");
    for (GroupLoadBalancerGroup group : this.groups.values()) {
      description.append(group.toString());
    }
    return description.toString();
  }

}
