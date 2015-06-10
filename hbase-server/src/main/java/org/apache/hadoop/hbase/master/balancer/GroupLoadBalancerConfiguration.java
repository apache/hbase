package org.apache.hadoop.hbase.master.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.lang.StringBuilder;
import java.util.*;

public class GroupLoadBalancerConfiguration {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  private static final String GROUPS = "hbase.master.balancer.grouploadbalancer.groups";
  private static final String DEFAULT_GROUP =
      "hbase.master.balancer.grouploadbalancer.defaultgroup";
  private static final String SERVER_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.servergroups.";
  private static final String TABLE_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.tablegroups.";


  private static final String GROUP_DELIMITER = ";";

  private Map<String, GroupLoadBalancerGroup> groups;
  private Map<String, GroupLoadBalancerServer> servers;
  private Map<String, GroupLoadBalancerTable> tables;
  private String defaultGroupName;

  public GroupLoadBalancerConfiguration(Configuration configuration,
      Map<ServerName, List<HRegionInfo>> clusterMap){

    this.groups = new HashMap<>();
    this.servers = new HashMap<>();
    this.tables = new HashMap<>();

    String groupNamesString = configuration.get(GROUPS);
    String[] groupNamesArray = groupNamesString.split(GROUP_DELIMITER);

    // Build group configurations
    for (String groupName : groupNamesArray) {

      if (groupName.length() < 1) {
        throw new IllegalArgumentException("Group name cannot be null.");
      }

      GroupLoadBalancerGroup group = new GroupLoadBalancerGroup(groupName);

      String serverConfig = configuration.get(SERVER_GROUPS_PREFIX + groupName);
      String tableConfig = configuration.get(TABLE_GROUPS_PREFIX + groupName);

      if (serverConfig == null) {
        throw new IllegalArgumentException("No servers defined for the group: " + groupName);
      }
      if (tableConfig == null) {
        throw new IllegalArgumentException("No tables defined for the group: " + groupName);
      }

      String[] serversArray = serverConfig.split(GROUP_DELIMITER);
      String[] tablesArray = tableConfig.split(GROUP_DELIMITER);

      for (String serverNameString : serversArray) {
        GroupLoadBalancerServer server = new GroupLoadBalancerServer(serverNameString, groupName);
        group.addServer(server);
        this.servers.put(serverNameString, server);
      }

      for (String tableNameString : tablesArray) {
        GroupLoadBalancerTable table = new GroupLoadBalancerTable(tableNameString, groupName);
        group.addTable(table);
        this.tables.put(tableNameString, table);
      }

      if (this.groups.containsKey(groupName)) {
        throw new IllegalArgumentException("Group name cannot be duplicated");
      }
      this.groups.put(groupName, group);

    }
    this.defaultGroupName = configuration.get(DEFAULT_GROUP);
    if (this.defaultGroupName == null) {
      throw new IllegalArgumentException("Default group name cannot be null");
    }
    if (!this.groups.containsKey(this.defaultGroupName)) {
      throw new IllegalArgumentException("Default group name must be a pre-existing group name");
    }

    // Go through clusterMap and if we encounter a new server, put it in the default group and
    // store the ServerName object in GroupLoadBalancerServer
    for (ServerName serverName : clusterMap.keySet()) {
      String serverNameString =
          GroupLoadBalancerUtils.getServerNameWithoutStartCode(serverName.getServerName());
      if (!this.servers.containsKey(serverNameString)) {
        GroupLoadBalancerServer groupLoadBalancerServer =
            new GroupLoadBalancerServer(serverNameString, defaultGroupName);
        this.servers.put(serverNameString, groupLoadBalancerServer);
        this.groups.get(defaultGroupName).addServer(groupLoadBalancerServer);
      }
      this.servers.get(serverNameString).setServerName(serverName);
    }

    // Go through clusterMap and if we encounter a new table, put it in the default group
    for (List<HRegionInfo> hriList : clusterMap.values()) {
      for (HRegionInfo hri : hriList) {
        String tableNameString =
            GroupLoadBalancerUtils.getTableNameFromRegionName(hri.getRegionNameAsString());
        if (!this.tables.containsKey(tableNameString)) {
          GroupLoadBalancerTable groupLoadBalancerTable =
              new GroupLoadBalancerTable(tableNameString, defaultGroupName);
          this.tables.put(tableNameString, groupLoadBalancerTable);
          this.groups.get(defaultGroupName).addTable(groupLoadBalancerTable);
        }
      }
    }
  }

  public Map<String, GroupLoadBalancerGroup> getGroups() {
    return this.groups;
  }

  public Map<String, GroupLoadBalancerServer> getServers() {
    return this.servers;
  }

  public Map<String, GroupLoadBalancerTable> getTables() {
    return this.tables;
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
    return description.toString();
  }

}
