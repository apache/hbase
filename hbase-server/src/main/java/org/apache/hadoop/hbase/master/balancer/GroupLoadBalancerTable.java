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

  public String getGroupTableBelongsTo() {
    return this.groupTableBelongsTo;
  }

  public void setGroupTableBelongsTo(String groupTableBelongsTo) {
    this.groupTableBelongsTo = groupTableBelongsTo;
  }

  public String toString() {
    return "{tableName: " + this.tableName + ", groupTableBelongsTo: " +
        this.groupTableBelongsTo + "}";
  }

}
