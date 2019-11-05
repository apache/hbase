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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A dummy RSGroupInfoManager which only contains a default rs group.
 */
@InterfaceAudience.Private
class DisabledRSGroupInfoManager implements RSGroupInfoManager {

  private final ServerManager serverManager;

  public DisabledRSGroupInfoManager(ServerManager serverManager) {
    this.serverManager = serverManager;
  }

  @Override
  public void start() {
  }

  @Override
  public void addRSGroup(RSGroupInfo rsGroupInfo) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }

  @Override
  public void removeRSGroup(String groupName) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }

  @Override
  public void moveServers(Set<Address> servers, String targetGroupName) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }

  private SortedSet<Address> getOnlineServers() {
    SortedSet<Address> onlineServers = new TreeSet<Address>();
    serverManager.getOnlineServers().keySet().stream().map(ServerName::getAddress)
      .forEach(onlineServers::add);
    return onlineServers;
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException {
    SortedSet<Address> onlineServers = getOnlineServers();
    if (onlineServers.contains(serverHostPort)) {
      return new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, onlineServers);
    } else {
      return null;
    }
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) throws IOException {
    if (RSGroupInfo.DEFAULT_GROUP.equals(groupName)) {
      return new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, getOnlineServers());
    } else {
      return null;
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return Arrays.asList(new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, getOnlineServers()));
  }

  @Override
  public boolean isOnline() {
    return true;
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }

  @Override
  public RSGroupInfo getRSGroupForTable(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }

  @Override
  public void setRSGroup(Set<TableName> tables, String groupName) throws IOException {
    throw new DoNotRetryIOException("RSGroup is disabled");
  }
}
