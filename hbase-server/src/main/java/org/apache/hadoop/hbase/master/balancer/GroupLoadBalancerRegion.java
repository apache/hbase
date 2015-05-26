package org.apache.hadoop.hbase.master.balancer;

public class GroupLoadBalancerRegion {

  private String regionName;
  private String regionServerBelongsTo;

  public GroupLoadBalancerRegion(String regionName, String regionServerBelongsTo) {
    this.regionName = regionName;
    this.regionServerBelongsTo = regionServerBelongsTo;
  }

  public void setRegionServerBelongsTo(String regionServerBelongsTo) {
    this.regionServerBelongsTo = regionServerBelongsTo;
  }

  public String toString() {
    return "{regionName: " + this.regionName + ", regionServerBelongsTo: " +
        this.regionServerBelongsTo + "}";
  }
}
