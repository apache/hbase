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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
  private final SortedSet<TableName> tables;

  private final Map<String, String> configuration;

  public RSGroupInfo(String name) {
    this(name, new TreeSet<>(), new TreeSet<>());
  }

  RSGroupInfo(String name, SortedSet<Address> servers, SortedSet<TableName> tables) {
    this.name = name;
    this.servers = (servers == null) ? new TreeSet<>() : new TreeSet<>(servers);
    this.tables  = (tables  == null) ? new TreeSet<>() : new TreeSet<>(tables);
    configuration = new HashMap<>();
  }

  public RSGroupInfo(RSGroupInfo src) {
    this(src.name, src.servers, src.tables);
    src.configuration.forEach(this::setConfiguration);
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
   */
  public SortedSet<TableName> getTables() {
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

  /**
   * Getter for fetching an unmodifiable {@link #configuration} map.
   */
  public Map<String, String> getConfiguration() {
    // shallow pointer copy
    return Collections.unmodifiableMap(configuration);
  }

  /**
   * Setter for storing a configuration setting in {@link #configuration} map.
   * @param key Config key.
   * @param value String value.
   */
  public void setConfiguration(String key, String value) {
    configuration.put(key, Objects.requireNonNull(value));
  }

  /**
   * Remove a config setting represented by the key from the {@link #configuration} map
   */
  public void removeConfiguration(final String key) {
    configuration.remove(key);
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
    sb.append(", ");
    sb.append(" Configurations:");
    sb.append(this.configuration);
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

    RSGroupInfo rsGroupInfo = (RSGroupInfo) o;

    if (!name.equals(rsGroupInfo.name)) {
      return false;
    }
    if (!servers.equals(rsGroupInfo.servers)) {
      return false;
    }
    if (!tables.equals(rsGroupInfo.tables)) {
      return false;
    }
    if (!configuration.equals(rsGroupInfo.configuration)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = servers.hashCode();
    result = 31 * result + tables.hashCode();
    result = 31 * result + name.hashCode();
    result = 31 * result + configuration.hashCode();
    return result;
  }
}
