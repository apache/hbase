package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.master.balancer.GroupLoadBalancerRegion;

import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;

public class GroupLoadBalancerRegionServer {

  private String serverName;
  private String serverIP;
  private List<GroupLoadBalancerRegion> regionsList;

  public GroupLoadBalancerRegionServer(String serverName, String serverIP) {
    this.serverName = serverName;
    this.serverIP = serverIP;
    this.regionsList = new ArrayList<>();
  }

  public void addRegion(GroupLoadBalancerRegion region) {
    this.regionsList.add(region);
  }

  public String getServerName(){
    return serverName;
  }

  public String getServerIP(){
    return serverIP;
  }

  public String toString() {
    StringBuilder description = new StringBuilder();
    description.append("Server Name: " + serverName + "\n");
    description.append("Server IP: " + serverIP + "\n");
    description.append("Regions List: \n");
    for (GroupLoadBalancerRegion region : regionsList) {
      description.append("\t" + region + "\n");
    }
    return description.toString();
  }
}
