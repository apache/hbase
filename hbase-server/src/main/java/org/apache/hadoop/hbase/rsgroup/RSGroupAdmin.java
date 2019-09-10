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
import java.util.Set;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Group user API interface used between client and server.
 */
@InterfaceAudience.Private
public interface RSGroupAdmin {
  /**
   * Gets {@code RSGroupInfo} for given group name.
   */
  RSGroupInfo getRSGroupInfo(String groupName) throws IOException;

  /**
   * Move given set of servers to the specified target RegionServer group.
   */
  void moveServers(Set<Address> servers, String targetGroup) throws IOException;

  /**
   * Creates a new RegionServer group with the given name.
   */
  void addRSGroup(String groupName) throws IOException;

  /**
   * Removes RegionServer group associated with the given name.
   */
  void removeRSGroup(String groupName) throws IOException;

  /**
   * Balance regions in the given RegionServer group.
   *
   * @return boolean Whether balance ran or not
   */
  boolean balanceRSGroup(String groupName) throws IOException;

  /**
   * Lists current set of RegionServer groups.
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Retrieve the RSGroupInfo a server is affiliated to
   * @param hostPort HostPort to get RSGroupInfo for
   */
  RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException;

  /**
   * Remove decommissioned servers from rsgroup.
   * 1. Sometimes we may find the server aborted due to some hardware failure and we must offline
   * the server for repairing. Or we need to move some servers to join other clusters.
   * So we need to remove these servers from the rsgroup.
   * 2. Dead/recovering/live servers will be disallowed.
   * @param servers set of servers to remove
   */
  void removeServers(Set<Address> servers) throws IOException;
}
