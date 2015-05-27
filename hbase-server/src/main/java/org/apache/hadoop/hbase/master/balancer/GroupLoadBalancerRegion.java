package org.apache.hadoop.hbase.master.balancer;

public class GroupLoadBalancerRegion {

  private String regionName;
  private String groupRegionBelongsTo;

  public GroupLoadBalancerRegion(String regionName, String groupRegionBelongsTo) {
    this.regionName = regionName;
    this.groupRegionBelongsTo = groupRegionBelongsTo;
  }

  public String getRegionName() {
    return this.regionName;
  }

  public void setGroupRegionBelongsTo(String groupRegionBelongsTo) {
    this.groupRegionBelongsTo = groupRegionBelongsTo;
  }

  public String toString() {
    return "{regionName: " + this.regionName + ", groupRegionBelongsTo: " +
        this.groupRegionBelongsTo + "}";
  }
}
