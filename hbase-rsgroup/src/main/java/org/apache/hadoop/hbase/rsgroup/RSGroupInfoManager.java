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

import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface used to manage RSGroupInfo storage. An implementation
 * has the option to support offline mode.
 * See {@link RSGroupBasedLoadBalancer}
 */
public interface RSGroupInfoManager {
  //Assigned before user tables
  public static final TableName RSGROUP_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");
  public static final byte[] RSGROUP_TABLE_NAME_BYTES = RSGROUP_TABLE_NAME.toBytes();
  public static final String rsGroupZNode = "rsgroup";
  public static final byte[] META_FAMILY_BYTES = Bytes.toBytes("m");
  public static final byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");
  public static final byte[] ROW_KEY = {0};


  /**
   * Adds the group.
   *
   * @param rsGroupInfo the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void addRSGroup(RSGroupInfo rsGroupInfo) throws IOException;

  /**
   * Remove a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void removeRSGroup(String groupName) throws IOException;

  /**
   * move servers to a new group.
   * @param hostPorts list of servers, must be part of the same group
   * @param srcGroup groupName being moved from
   * @param dstGroup groupName being moved to
   * @return true if move was successful
   * @throws java.io.IOException on move failure
   */
  boolean moveServers(Set<HostAndPort> hostPorts,
                      String srcGroup, String dstGroup) throws IOException;

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of RSGroupInfo
   */
  RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of RSGroupInfo
   */
  RSGroupInfo getRSGroup(String groupName) throws IOException;

  /**
   * Get the group membership of a table
   * @param tableName name of table to get group membership
   * @return Group name of table
   * @throws java.io.IOException on failure to retrive information
   */
  String getRSGroupOfTable(TableName tableName) throws IOException;

  /**
   * Set the group membership of a set of tables
   *
   * @param tableNames set of tables to move
   * @param groupName name of group of tables to move to
   * @throws java.io.IOException on failure to move
   */
  void moveTables(Set<TableName> tableNames, String groupName) throws IOException;

  /**
   * List the groups
   *
   * @return list of RSGroupInfo
   * @throws java.io.IOException on failure
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Refresh/reload the group information from
   * the persistent store
   *
   * @throws java.io.IOException on failure to refresh
   */
  void refresh() throws IOException;

  /**
   * Whether the manager is able to fully
   * return group metadata
   *
   * @return whether the manager is in online mode
   */
  boolean isOnline();
}
