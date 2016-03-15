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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Group user API interface used between client and server.
 */
@InterfaceAudience.Private
public abstract class RSGroupAdmin implements Closeable {

  /**
   * Create a new RSGroupAdmin client
   * @param conn connection RSGroupAdmin instance will use
   * @return a new RSGroupAdmin client
   * @throws IOException on failure to create new client
   */
  public static RSGroupAdmin newClient(Connection conn) throws IOException {
    return new RSGroupAdminClient(conn);
  }

  /**
   * Gets the regionserver group information.
   *
   * @param groupName the group name
   * @return An instance of RSGroupInfo
   */
  public abstract RSGroupInfo getRSGroupInfo(String groupName) throws IOException;

  /**
   * Gets the regionserver group info of table.
   *
   * @param tableName the table name
   * @return An instance of RSGroupInfo.
   */
  public abstract RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException;

  /**
   * Move a set of serves to another group
   *
   *
   * @param servers set of servers, must be in the form HOST:PORT
   * @param targetGroup the target group
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  public abstract void moveServers(Set<HostAndPort> servers, String targetGroup) throws IOException;

  /**
   * Move tables to a new group.
   * This will unassign all of a table's region so it can be reassigned to the correct group.
   * @param tables list of tables to move
   * @param targetGroup target group
   * @throws java.io.IOException on failure to move tables
   */
  public abstract void moveTables(Set<TableName> tables, String targetGroup) throws IOException;

  /**
   * Add a new group
   * @param name name of the group
   * @throws java.io.IOException on failure to add group
   */
  public abstract void addRSGroup(String name) throws IOException;

  /**
   * Remove a regionserver group
   * @param name name of the group
   * @throws java.io.IOException on failure to remove group
   */
  public abstract void removeRSGroup(String name) throws IOException;

  /**
   * Balance the regions in a group
   *
   * @param name the name of the group to balance
   * @return boolean whether balance ran or not
   * @throws java.io.IOException on unexpected failure to balance group
   */
  public abstract boolean balanceRSGroup(String name) throws IOException;

  /**
   * Lists the existing groups.
   *
   * @return Collection of RSGroupInfo.
   */
  public abstract List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Retrieve the RSGroupInfo a server is affiliated to
   * @param hostPort HostPort to get RSGroupInfo for
   * @return RSGroupInfo associated with the server
   * @throws java.io.IOException on unexpected failure to retrieve GroupInfo
   */
  public abstract RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException;
}
