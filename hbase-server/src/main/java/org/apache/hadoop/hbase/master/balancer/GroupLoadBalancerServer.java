package org.apache.hadoop.hbase.master.balancer;

public class GroupLoadBalancerServer {

  private String serverName;
  private String groupServerBelongsTo;

  public GroupLoadBalancerServer(String serverName, String groupServerBelongsTo) {
    this.serverName = serverName;
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  public String getServerName() {
    return this.serverName;
  }

  public String getGroupServerBelongsTo() {
    return this.groupServerBelongsTo;
  }

  public void setGroupServerBelongsTo(String groupServerBelongsTo) {
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  public String toString() {
    return "{serverName: " + this.serverName + ", groupServerBelongsTo: " +
        this.groupServerBelongsTo + "}";
  }
}
