package org.apache.hadoop.hbase.master.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.StringBuilder;
import java.security.acl.Group;
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
  private Map<String, GroupLoadBalancerRegion> regions;
  private Map<String, GroupLoadBalancerServer> servers;
  private String defaultGroupName;

  public GroupLoadBalancerConfiguration(Configuration configuration) {

    this.groups = new HashMap<>();
    this.regions = new HashMap<>();
    this.servers = new HashMap<>();

    String groupNamesString = configuration.get(GROUPS);
    String[] groupNamesArray = groupNamesString.split(GROUP_DELIMITER);

    // Build group configurations
    for (String groupName : groupNamesArray) {

      if (groupName.length() < 1) {
        throw new IllegalArgumentException("Group name cannot be null.");
      }

      GroupLoadBalancerGroup group = new GroupLoadBalancerGroup(groupName);

      String regionConfig = configuration.get(REGION_GROUPS_PREFIX + groupName);
      String serverConfig = configuration.get(SERVER_GROUPS_PREFIX + groupName);

      String[] regionsArray = regionConfig.split(GROUP_DELIMITER);
      String[] serversArray = serverConfig.split(GROUP_DELIMITER);

      for (String regionName : regionsArray) {
        GroupLoadBalancerRegion region = new GroupLoadBalancerRegion(regionName, groupName);
        group.addRegion(region);
        this.regions.put(regionName, region);
      }

      for (String serverName : serversArray) {
        GroupLoadBalancerServer server = new GroupLoadBalancerServer(serverName, groupName);
        group.addServer(server);
        this.servers.put(serverName, server);
      }

      if (this.groups.containsKey(groupName)) {
        throw new IllegalArgumentException("Group name cannot be duplicated");
      }
      this.groups.put(groupName, group);

    }

    this.defaultGroupName = configuration.get(DEFAULT_GROUP);
    if (this.defaultGroupName.length() < 1) {
      throw new IllegalArgumentException("Default group name cannot be null");
    }
    if (!this.groups.containsKey(this.defaultGroupName)) {
      throw new IllegalArgumentException("Default group name must be a pre-existing group name");
    }

    LOG.info("**************** groups " + toString());

  }

  public Map<String, GroupLoadBalancerGroup> getGroups() {
    return this.groups;
  }

  public Map<String, GroupLoadBalancerRegion> getRegions() {
    return this.regions;
  }

  public Map<String, GroupLoadBalancerServer> getServers() {
    return this.servers;
  }

  public String getDefaultGroupName() {
    return this.defaultGroupName;
  }

  public String toString() {
    StringBuilder description = new StringBuilder();
    description.append("Groups List: \n");
    for (GroupLoadBalancerGroup group : this.groups.values()) {
      description.append(group.toString());
    }
    description.append("\n");
    description.append("Regions Map: \n");
    for (GroupLoadBalancerRegion region : this.regions.values()) {
      description.append(region + "\n");
    }
    description.append("Servers Map: \n");
    for (GroupLoadBalancerServer server : this.servers.values()) {
      description.append(server + "\n");
    }
    return description.toString();
  }

}
