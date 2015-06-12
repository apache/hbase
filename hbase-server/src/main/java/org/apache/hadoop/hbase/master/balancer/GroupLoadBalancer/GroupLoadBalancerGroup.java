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
    if (this.servers.containsKey(server.getServerNameString())) {
      throw new IllegalArgumentException("Server name already exists.");
    }
    this.servers.put(server.getServerNameString(), server);
  }

  public void addTable(GroupLoadBalancerTable table) {
    if (this.tables.containsKey(table.getTableName())) {
      throw new IllegalArgumentException("Table name already exists");
    }
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
