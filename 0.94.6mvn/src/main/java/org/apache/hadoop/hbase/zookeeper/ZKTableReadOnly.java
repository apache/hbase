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
package org.apache.hadoop.hbase.zookeeper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKTable.TableState;
import org.apache.zookeeper.KeeperException;

/**
 * Non-instantiable class that provides helper functions for
 * clients other than {@link AssignmentManager} for reading the
 * state of a table in ZK.
 *
 * <p>Does not cache state like {@link ZKTable}, actually reads from ZK each call.
 */
public class ZKTableReadOnly {

  private ZKTableReadOnly() {}

  /**
   * Go to zookeeper and see if state of table is {@link TableState#DISABLED}.
   * This method does not use cache as {@link #isDisabledTable(String)} does.
   * This method is for clients other than {@link AssignmentManager}
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisabledTable(final ZooKeeperWatcher zkw,
      final String tableName)
  throws KeeperException {
    TableState state = getTableState(zkw, tableName);
    return isTableState(TableState.DISABLED, state);
  }

  /**
   * Go to zookeeper and see if state of table is {@link TableState#ENABLED}.
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isEnabledTable(final ZooKeeperWatcher zkw,
      final String tableName) throws KeeperException {
    TableState state = getTableState(zkw, tableName);
    return state == null || state == TableState.ENABLED;
  }

  /**
   * Go to zookeeper and see if state of table is {@link TableState#DISABLING}
   * of {@link TableState#DISABLED}.
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisablingOrDisabledTable(final ZooKeeperWatcher zkw,
      final String tableName) throws KeeperException {
    TableState state = getTableState(zkw, tableName);
    return isTableState(TableState.DISABLING, state) ||
      isTableState(TableState.DISABLED, state);
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getDisabledTables(ZooKeeperWatcher zkw)
  throws KeeperException {
    Set<String> disabledTables = new HashSet<String>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.clientTableZNode);
    for (String child: children) {
      TableState state = getTableState(zkw, child);
      if (state == TableState.DISABLED) disabledTables.add(child);
    }
    return disabledTables;
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getDisabledOrDisablingTables(ZooKeeperWatcher zkw)
  throws KeeperException {
    Set<String> disabledTables = new HashSet<String>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.clientTableZNode);
    for (String child: children) {
      TableState state = getTableState(zkw, child);
      if (state == TableState.DISABLED || state == TableState.DISABLING)
        disabledTables.add(child);
    }
    return disabledTables;
  }

  static boolean isTableState(final TableState expectedState,
    final TableState currentState) {
    return currentState != null && currentState.equals(expectedState);
  }

  /**
   * Read the TableState from ZooKeeper
   * @throws KeeperException
   */
  static TableState getTableState(final ZooKeeperWatcher zkw,
    final String child) throws KeeperException {
    return getTableState(zkw, zkw.clientTableZNode, child);
  }

  /**
   * @deprecated Only for 0.92/0.94 compatibility.  Use getTableState(zkw, child) instead.
   */
  static TableState getTableState(final ZooKeeperWatcher zkw,
    final String parent, final String child) throws KeeperException {
    String znode = ZKUtil.joinZNode(parent, child);
    byte [] data = ZKUtil.getData(zkw, znode);
    if (data == null || data.length <= 0) {
      return null;
    }
    String str = Bytes.toString(data);
    try {
      return TableState.valueOf(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str);
    }
  }
}
