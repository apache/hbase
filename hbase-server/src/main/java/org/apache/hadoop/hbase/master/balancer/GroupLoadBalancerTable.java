package org.apache.hadoop.hbase.master.balancer;

public class GroupLoadBalancerTable {

  private String tableName;
  private String groupTableBelongsTo;

  public GroupLoadBalancerTable(String tableName, String groupTableBelongsTo) {
    this.tableName = tableName;
    this.groupTableBelongsTo = groupTableBelongsTo;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setGroupTableBelongsTo(String groupTableBelongsTo) {
    this.groupTableBelongsTo = groupTableBelongsTo;
  }

  public String toString() {
    return "{tableName: " + this.tableName + ", groupTableBelongsTo: " +
        this.groupTableBelongsTo + "}";
  }

}
