/*
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface used to manage RSGroupInfo storage. An implementation has the option to support offline
 * mode. See {@code RSGroupBasedLoadBalancer}.
 */
@InterfaceAudience.Private
public interface RSGroupInfoManager {

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
   */
  void moveServers(Set<Address> servers, String targetGroupName) throws IOException;

  /**
   * Move all servers from src to a new group.
   */
  void moveAllServers(String sourceGroupName, String targetGroupName) throws IOException;

  /**
   * Gets the group info of server.
   */
  RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException;

  /**
   * Gets {@code RSGroupInfo} for the given group name.
   */
  RSGroupInfo getRSGroup(String groupName) throws IOException;

  /**
   * List the existing {@code RSGroupInfo}s.
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Whether the manager is able to fully return group metadata
   * @return whether the manager is in online mode
   */
  boolean isOnline();

  /**
   * Remove decommissioned servers from rsgroup
   * @param servers set of servers to remove
   */
  void removeServers(Set<Address> servers) throws IOException;

  /**
   * Get {@code RSGroupInfo} for the given table.
   */
  RSGroupInfo getRSGroupForTable(TableName tableName) throws IOException;

  static RSGroupInfoManager create(MasterServices master) throws IOException {
    if (RSGroupUtil.isRSGroupEnabled(master.getConfiguration())) {
      return RSGroupInfoManagerImpl.getInstance(master);
    } else {
      return new DisabledRSGroupInfoManager(master.getServerManager());
    }
  }

  /**
   * Balance a region server group.
   */
  BalanceResponse balanceRSGroup(String groupName, BalanceRequest request) throws IOException;

  /**
   * Set group for tables.
   */
  void setRSGroup(Set<TableName> tables, String groupName) throws IOException;

  /**
   * Determine {@code RSGroupInfo} for the given table.
   * @param tableName table name
   * @return rsgroup name
   */
  String determineRSGroupInfoForTable(TableName tableName);

  /**
   * Rename rsgroup
   * @param oldName old rsgroup name
   * @param newName new rsgroup name
   */
  void renameRSGroup(String oldName, String newName) throws IOException;

  /**
   * Update RSGroup configuration
   * @param groupName     the group name
   * @param configuration new configuration of the group name to be set
   * @throws IOException if a remote or network exception occurs
   */
  void updateRSGroupConfig(String groupName, Map<String, String> configuration) throws IOException;

}
