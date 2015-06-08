package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.ServerName;

public class GroupLoadBalancerServer {

  private ServerName serverName;
  private String serverNameString;
  private String groupServerBelongsTo;

  public GroupLoadBalancerServer(String serverNameString, String groupServerBelongsTo) {
    this.serverNameString = serverNameString;
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  public String getGroupServerBelongsTo() {
    return this.groupServerBelongsTo;
  }

  public ServerName getServerName() {
    return this.serverName;
  }

  public String getServerNameString() {
    return this.serverNameString;
  }

  public void setGroupServerBelongsTo(String groupServerBelongsTo) {
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  public void setServerName(ServerName serverName) {
    this.serverName = serverName;
  }

  public String toString() {
    return "{serverName: " + serverName + ", serverNameString: " + this.serverNameString +
        ", groupServerBelongsTo: " + this.groupServerBelongsTo + "}";
  }
}
