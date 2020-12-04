/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rsgroup;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.net.Address;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Stores the group information of region server groups.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RSGroupInfo {

  public static final String DEFAULT_GROUP = "default";
  public static final String NAMESPACE_DESC_PROP_GROUP = "hbase.rsgroup.name";

  private String name;
  private Set<Address> servers;
  private NavigableSet<TableName> tables;

  public RSGroupInfo(String name) {
    this(name, Sets.<Address>newHashSet(), Sets.<TableName>newTreeSet());
  }

  RSGroupInfo(String name,
              Set<Address> servers,
              NavigableSet<TableName> tables) {
    this.name = name;
    this.servers = servers;
    this.tables = tables;
  }

  public RSGroupInfo(RSGroupInfo src) {
    name = src.getName();
    servers = Sets.newHashSet(src.getServers());
    tables = Sets.newTreeSet(src.getTables());
  }

  /**
   * Get group name.
   *
   * @return group name
   */
  public String getName() {
    return name;
  }

  /**
   * Adds the server to the group.
   *
   * @param server the server
   */
  public void addServer(Address server){
    servers.add(server);
  }

  /**
   * Adds a group of servers.
   *
   * @param servers the servers
   */
  public void addAllServers(Collection<Address> addresses){
    this.servers.addAll(addresses);
  }

  /**
   * @param address Address of the server
   * @return true, if a server with address is found
   */
  public boolean containsServer(Address address) {
    return servers.contains(address);
  }

  /**
   * Get list of servers.
   *
   * @return set of servers
   */
  public Set<Address> getServers() {
    return servers;
  }

  /**
   * Remove a server from this group.
   *
   * @param address Address of the server to remove
   */
  public boolean removeServer(Address address) {
    return servers.remove(address);
  }

  /**
   * Set of tables that are members of this group
   * @return set of tables
   */
  public NavigableSet<TableName> getTables() {
    return tables;
  }

  public void addTable(TableName table) {
    tables.add(table);
  }

  public void addAllTables(Collection<TableName> arg) {
    tables.addAll(arg);
  }

  public boolean containsTable(TableName table) {
    return tables.contains(table);
  }

  public boolean removeTable(TableName table) {
    return tables.remove(table);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Name:");
    sb.append(this.name);
    sb.append(", ");
    sb.append(" Servers:");
    sb.append(this.servers);
    sb.append(", ");
    sb.append(" Tables:");
    sb.append(this.tables);
    return sb.toString();

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RSGroupInfo RSGroupInfo = (RSGroupInfo) o;

    if (!name.equals(RSGroupInfo.name)) {
      return false;
    }
    if (!servers.equals(RSGroupInfo.servers)) {
      return false;
    }
    if (!tables.equals(RSGroupInfo.tables)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = servers.hashCode();
    result = 31 * result + tables.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }

}
