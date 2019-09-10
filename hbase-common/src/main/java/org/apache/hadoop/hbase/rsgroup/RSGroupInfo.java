/**
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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Stores the group information of region server groups.
 */
@InterfaceAudience.Public
public class RSGroupInfo {
  public static final String DEFAULT_GROUP = "default";
  public static final String NAMESPACE_DESC_PROP_GROUP = "hbase.rsgroup.name";
  public static final String TABLE_DESC_PROP_GROUP = "hbase.rsgroup.name";

  private final String name;
  // Keep servers in a sorted set so has an expected ordering when displayed.
  private final SortedSet<Address> servers;
  // Keep tables sorted too.
  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  private final SortedSet<TableName> tables;

  public RSGroupInfo(String name) {
    this(name, new TreeSet<Address>(), new TreeSet<TableName>());
  }

  RSGroupInfo(String name, SortedSet<Address> servers) {
    this.name = name;
    this.servers = servers == null ? new TreeSet<>() : new TreeSet<>(servers);
    this.tables = new TreeSet<>();
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information for a table will be
   *             stored in the configuration of a table so this will be removed.
   */
  @Deprecated
  RSGroupInfo(String name, SortedSet<Address> servers, SortedSet<TableName> tables) {
    this.name = name;
    this.servers = (servers == null) ? new TreeSet<>() : new TreeSet<>(servers);
    this.tables = (tables == null) ? new TreeSet<>() : new TreeSet<>(tables);
  }

  public RSGroupInfo(RSGroupInfo src) {
    this(src.name, src.servers, src.tables);
  }

  /**
   * Get group name.
   */
  public String getName() {
    return name;
  }

  /**
   * Adds the given server to the group.
   */
  public void addServer(Address hostPort){
    servers.add(hostPort);
  }

  /**
   * Adds the given servers to the group.
   */
  public void addAllServers(Collection<Address> hostPort){
    servers.addAll(hostPort);
  }

  /**
   * @param hostPort hostPort of the server
   * @return true, if a server with hostPort is found
   */
  public boolean containsServer(Address hostPort) {
    return servers.contains(hostPort);
  }

  /**
   * Get list of servers.
   */
  public Set<Address> getServers() {
    return servers;
  }

  /**
   * Remove given server from the group.
   */
  public boolean removeServer(Address hostPort) {
    return servers.remove(hostPort);
  }

  /**
   * Get set of tables that are members of the group.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  public SortedSet<TableName> getTables() {
    return tables;
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  public void addTable(TableName table) {
    tables.add(table);
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  public void addAllTables(Collection<TableName> arg) {
    tables.addAll(arg);
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  public boolean containsTable(TableName table) {
    return tables.contains(table);
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. The rsgroup information will be stored in
   *             the configuration of a table so this will be removed.
   */
  @Deprecated
  public boolean removeTable(TableName table) {
    return tables.remove(table);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
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
