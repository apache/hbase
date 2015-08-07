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
 * Non-instantiable class that provides helper functions for
 * clients other than AssignmentManager for reading the
 * state of a table in ZK.
 *
 * <p>Does not cache state like {@link ZKTable}, actually reads from ZK each call.
 */
@InterfaceAudience.Private
public class ZKTableReadOnly {

  private ZKTableReadOnly() {}
  
  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#DISABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
  throws KeeperException {
    ZooKeeperProtos.Table.State state = getTableState(zkw, tableName);
    return isTableState(ZooKeeperProtos.Table.State.DISABLED, state);
  }

  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#ENABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isEnabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
  throws KeeperException {
    return getTableState(zkw, tableName) == ZooKeeperProtos.Table.State.ENABLED;
  }

  /**
   * Go to zookeeper and see if state of table is {@code ZooKeeperProtos.Table.State#DISABLING}
   * of {@code ZooKeeperProtos.Table.State#DISABLED}.
   * This method does not use cache.
   * This method is for clients other than AssignmentManager.
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisablingOrDisabledTable(final ZooKeeperWatcher zkw,
      final TableName tableName)
  throws KeeperException {
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
  throws KeeperException {
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
  throws KeeperException {
    Set<TableName> disabledTables = new HashSet<TableName>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    for (String child: children) {
      TableName tableName =
          TableName.valueOf(child);
      ZooKeeperProtos.Table.State state = getTableState(zkw, tableName);
      if (state == ZooKeeperProtos.Table.State.DISABLED ||
          state == ZooKeeperProtos.Table.State.DISABLING)
        disabledTables.add(tableName);
    }
    return disabledTables;
  }

  static boolean isTableState(final ZooKeeperProtos.Table.State expectedState,
      final ZooKeeperProtos.Table.State currentState) {
    return currentState != null && currentState.equals(expectedState);
  }

  /**
   * @param zkw
   * @param tableName
   * @return Null or {@link ZooKeeperProtos.Table.State} found in znode.
   * @throws KeeperException
   */
  static ZooKeeperProtos.Table.State getTableState(final ZooKeeperWatcher zkw,
      final TableName tableName)
  throws KeeperException {
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
