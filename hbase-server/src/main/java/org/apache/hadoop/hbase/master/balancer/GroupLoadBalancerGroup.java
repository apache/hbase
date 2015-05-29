package org.apache.hadoop.hbase.master.balancer;

import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.Map;

public class GroupLoadBalancerGroup {

  private String name;
  private Map<String, GroupLoadBalancerServer> servers;
  private Map<String, GroupLoadBalancerTable> tables;

  public GroupLoadBalancerGroup(String name) {
    this.name = name;
    this.servers = new HashMap<>();
    this.tables = new HashMap<>();
  }

  public void addServer(GroupLoadBalancerServer server) {
    this.servers.put(server.getServerName(), server);
  }

  public void addTable(GroupLoadBalancerTable table) {
    this.tables.put(table.getTableName(), table);
  }

  public String getName() {
    return this.name;
  }

  public String toString() {
    StringBuilder description = new StringBuilder();
    description.append("Servers Map: \n");
    for (GroupLoadBalancerServer server : servers.values()) {
      description.append("\t" + server + "\n");
    }
    description.append("Tables Map: \n");
    for (GroupLoadBalancerTable table : tables.values()) {
      description.append("\t" + table + "\n");
    }
    return description.toString();
  }
}
