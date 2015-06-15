/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

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

  /**
   * Create an object that holds all configuration info for grouping
   *
   * @param configuration the configuration object, which includes hbase-site.xml
   * @param clusterMap this maps a server to the list of regions which is on it
   */
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

  /**
   * @return a map which maps the name of a group as a string to a GroupLoadBalancerGroup object
   */
  public Map<String, GroupLoadBalancerGroup> getGroups() {
    return this.groups;
  }

  /**
   * @param serverNameString the name of the server as a string
   * @return a GroupLoadBalancerServer object with the name specified
   */
  public GroupLoadBalancerServer getServer(String serverNameString) {
    return this.servers.get(serverNameString);
  }

  /**
   * @return a map of all the servers. The key of the map is the name of the server as a string,
   * and the value of the map is a GroupLoadBalancerServer object.
   */
  public Map<String, GroupLoadBalancerServer> getServers() {
    return this.servers;
  }

  /**
   * @param tableNameString the name of the table as a string
   * @return a GroupLoadBalancerTable object with the name specified
   */
  public GroupLoadBalancerTable getTable(String tableNameString) {
    return this.tables.get(tableNameString);
  }

  /**
   * @return a map of all the tables. The key of the map is the name of the table as a string,
   * and the value of the map is a GroupLoadBalancerTable object.
   */
  public Map<String, GroupLoadBalancerTable> getTables() {
    return this.tables;
  }

  /**
   * @return a the name of the default group name as a string
   */
  public String getDefaultGroupName() {
    return this.defaultGroupName;
  }

  /**
   * @return a formatted string of the configuration
   */
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
