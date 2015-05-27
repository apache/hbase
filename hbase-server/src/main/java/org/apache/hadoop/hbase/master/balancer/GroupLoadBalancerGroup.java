package org.apache.hadoop.hbase.master.balancer;

import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.Map;

public class GroupLoadBalancerGroup {

  private String name;
  private Map<String, GroupLoadBalancerRegion> regions;
  private Map<String, GroupLoadBalancerServer> servers;

  public GroupLoadBalancerGroup(String name) {
    this.name = name;
    this.regions = new HashMap<>();
    this.servers = new HashMap<>();
  }

  public void addRegion(GroupLoadBalancerRegion region) {
    this.regions.put(region.getRegionName(), region);
  }

  public void addServer(GroupLoadBalancerServer server) {
    this.servers.put(server.getServerName(), server);
  }

  public String getName() {
    return this.name;
  }

  public String toString() {
    StringBuilder description = new StringBuilder();
    description.append("Regions List: \n");
    for (GroupLoadBalancerRegion region : regions.values()) {
      description.append("\t" + region + "\n");
    }
    description.append("Servers List: \n");
    for (GroupLoadBalancerServer server : servers.values()) {
      description.append("\t" + server + "\n");
    }
    return description.toString();
  }
}
