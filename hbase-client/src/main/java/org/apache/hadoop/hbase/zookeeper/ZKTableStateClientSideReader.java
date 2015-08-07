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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Non-instantiable class that provides helper functions to learn
 * about HBase table state for code running on client side (hence, not having
 * access to consensus context).
 *
 * Doesn't cache any table state, just goes directly to ZooKeeper.
 * TODO: decouple this class from ZooKeeper.
 */
@InterfaceAudience.Private
public class ZKTableStateClientSideReader {

  private ZKTableStateClientSideReader() {}
  
  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#DISABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
      throws KeeperException, InterruptedException {
    ZooKeeperProtos.Table.State state = getTableState(zkw, tableName);
    return isTableState(ZooKeeperProtos.Table.State.DISABLED, state);
  }

  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#ENABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isEnabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
      throws KeeperException, InterruptedException {
    return getTableState(zkw, tableName) == ZooKeeperProtos.Table.State.ENABLED;
  }

  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#DISABLING}
   * of {@code ZooKeeperProtos.Table.State#DISABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager.
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisablingOrDisabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
      throws KeeperException, InterruptedException {
    ZooKeeperProtos.Table.State state = getTableState(zkw, tableName);
    return isTableState(ZooKeeperProtos.Table.State.DISABLING, state) ||
      isTableState(ZooKeeperProtos.Table.State.DISABLED, state);
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getDisabledTables(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    Set<TableName> disabledTables = new HashSet<TableName>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    for (String child: children) {
      TableName tableName =
          TableName.valueOf(child);
      ZooKeeperProtos.Table.State state = getTableState(zkw, tableName);
      if (state == ZooKeeperProtos.Table.State.DISABLED) disabledTables.add(tableName);
    }
    return disabledTables;
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getDisabledOrDisablingTables(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    return
        getTablesInStates(
          zkw,
          ZooKeeperProtos.Table.State.DISABLED,
          ZooKeeperProtos.Table.State.DISABLING);
  }

  /**
   * Gets a list of all the tables set as enabling in zookeeper.
   * @param zkw ZooKeeperWatcher instance to use
   * @return Set of enabling tables, empty Set if none
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static Set<TableName> getEnablingTables(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    return getTablesInStates(zkw, ZooKeeperProtos.Table.State.ENABLING);
  }

  /**
   * Gets a list of tables that are set as one of the passing in states in zookeeper.
   * @param zkw ZooKeeperWatcher instance to use
   * @param states the list of states that a table could be in
   * @return Set of tables in one of the states, empty Set if none
   * @throws KeeperException
   * @throws InterruptedException
   */
  private static Set<TableName> getTablesInStates(
    ZooKeeperWatcher zkw,
    ZooKeeperProtos.Table.State... states)
      throws KeeperException, InterruptedException {
    Set<TableName> tableNameSet = new HashSet<TableName>();
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    TableName tableName;
    ZooKeeperProtos.Table.State tableState;
    for (String child: children) {
      tableName = TableName.valueOf(child);
      tableState = getTableState(zkw, tableName);
      for (ZooKeeperProtos.Table.State state : states) {
         if (tableState == state) {
           tableNameSet.add(tableName);
           break;
         }
      }
    }
    return tableNameSet;
  }

  static boolean isTableState(final ZooKeeperProtos.Table.State expectedState,
      final ZooKeeperProtos.Table.State currentState) {
    return currentState != null && currentState.equals(expectedState);
  }

  /**
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return Null or {@link ZooKeeperProtos.Table.State} found in znode.
   * @throws KeeperException
   */
  static ZooKeeperProtos.Table.State getTableState(final ZooKeeperWatcher zkw,
      final TableName tableName)
      throws KeeperException, InterruptedException {
    String znode = ZKUtil.joinZNode(zkw.tableZNode, tableName.getNameAsString());
    byte [] data = ZKUtil.getData(zkw, znode);
    if (data == null || data.length <= 0) return null;
    try {
      ProtobufUtil.expectPBMagicPrefix(data);
      ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
      return builder.getState();
    } catch (IOException e) {
      KeeperException ke = new KeeperException.DataInconsistencyException();
      ke.initCause(e);
      throw ke;
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }
}
