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


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface used to manage RSGroupInfo storage. An implementation
 * has the option to support offline mode.
 * See {@link RSGroupBasedLoadBalancer}
 */
@InterfaceAudience.Private
public interface RSGroupInfoManager {

  String REASSIGN_WAIT_INTERVAL_KEY = "hbase.rsgroup.reassign.wait";
  long DEFAULT_REASSIGN_WAIT_INTERVAL = 30 * 1000L;

  //Assigned before user tables
  TableName RSGROUP_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");
  String rsGroupZNode = "rsgroup";
  byte[] META_FAMILY_BYTES = Bytes.toBytes("m");
  byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");
  byte[] ROW_KEY = {0};

  void start();

  /**
   * Add given RSGroupInfo to existing list of group infos.
   */
  void addRSGroup(RSGroupInfo rsGroupInfo) throws IOException;

  /**
   * Remove a region server group.
   */
  void removeRSGroup(String groupName) throws IOException;

  /**
   * Move servers to a new group.
   * @param servers list of servers, must be part of the same group
   * @param srcGroup groupName being moved from
   * @param dstGroup groupName being moved to
   * @return Set of servers moved (May be a subset of {@code servers}).
   */
  Set<Address> moveServers(Set<Address> servers, String srcGroup, String dstGroup)
      throws IOException;

  /**
   * Gets the group info of server.
   */
  RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException;

  /**
   * Gets {@code RSGroupInfo} for the given group name.
   */
  RSGroupInfo getRSGroup(String groupName) throws IOException;

  /**
   * Get the group membership of a table
   */
  String getRSGroupOfTable(TableName tableName) throws IOException;

  /**
   * Set the group membership of a set of tables
   *
   * @param tableNames set of tables to move
   * @param groupName name of group of tables to move to
   */
  void moveTables(Set<TableName> tableNames, String groupName) throws IOException;

  /**
   * List the existing {@code RSGroupInfo}s.
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Refresh/reload the group information from the persistent store
   */
  void refresh() throws IOException;

  /**
   * Whether the manager is able to fully return group metadata
   *
   * @return whether the manager is in online mode
   */
  boolean isOnline();

  /**
   * Move servers and tables to a new group.
   * @param servers list of servers, must be part of the same group
   * @param tables set of tables to move
   * @param srcGroup groupName being moved from
   * @param dstGroup groupName being moved to
   */
  void moveServersAndTables(Set<Address> servers, Set<TableName> tables,
      String srcGroup, String dstGroup) throws IOException;

  /**
   * Remove decommissioned servers from rsgroup
   * @param servers set of servers to remove
   */
  void removeServers(Set<Address> servers) throws IOException;

  /**
   * Rename RSGroup
   * @param oldName old rsgroup name
   * @param newName new rsgroup name
   */
  void renameRSGroup(String oldName, String newName) throws IOException;

  /**
   * Determine {@code RSGroupInfo} for the given table.
   * @param tableName table name
   * @return {@link RSGroupInfo} which table should belong to
   */
  RSGroupInfo determineRSGroupInfoForTable(TableName tableName) throws IOException;

  /**
   * Update RSGroup configuration
   * @param groupName the group name
   * @param configuration new configuration of the group name to be set
   * @throws IOException if a remote or network exception occurs
   */
  void updateRSGroupConfig(String groupName, Map<String, String> configuration) throws IOException;
}
