/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for table state tracking for use by AssignmentManager.
 * Reads, caches and sets state up in zookeeper.  If multiple read/write
 * clients, will make for confusion.  Read-only clients other than
 * AssignmentManager interested in learning table state can use the
 * read-only utility methods in {@link ZKTableReadOnly}.
 *
 * <p>To save on trips to the zookeeper ensemble, internally we cache table
 * state.
 */
@InterfaceAudience.Private
public class ZKTable {
  // A znode will exist under the table directory if it is in any of the
  // following states: {@link TableState#ENABLING} , {@link TableState#DISABLING},
  // or {@link TableState#DISABLED}.  If {@link TableState#ENABLED}, there will
  // be no entry for a table in zk.  Thats how it currently works.

  private static final Log LOG = LogFactory.getLog(ZKTable.class);
  private final ZooKeeperWatcher watcher;

  /**
   * Cache of what we found in zookeeper so we don't have to go to zk ensemble
   * for every query.  Synchronize access rather than use concurrent Map because
   * synchronization needs to span query of zk.
   */
  private final Map<TableName, ZooKeeperProtos.Table.State> cache =
    new HashMap<TableName, ZooKeeperProtos.Table.State>();

  // TODO: Make it so always a table znode. Put table schema here as well as table state.
  // Have watcher on table znode so all are notified of state or schema change.

  public ZKTable(final ZooKeeperWatcher zkw) throws KeeperException {
    super();
    this.watcher = zkw;
    populateTableStates();
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @throws KeeperException
   */
  private void populateTableStates()
  throws KeeperException {
    synchronized (this.cache) {
      List<String> children = ZKUtil.listChildrenNoWatch(this.watcher, this.watcher.tableZNode);
      if (children == null) return;
      for (String child: children) {
        TableName tableName = TableName.valueOf(child);
        ZooKeeperProtos.Table.State state = ZKTableReadOnly.getTableState(this.watcher, tableName);
        if (state != null) this.cache.put(tableName, state);
      }
    }
  }

  /**
   * Sets the specified table as DISABLED in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisabledTable(TableName tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisablingOrDisabledTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabled but was " +
          "not first in disabling state: " + this.cache.get(tableName));
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLED);
    }
  }

  /**
   * Sets the specified table as DISABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisablingTable(final TableName tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isEnabledOrDisablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabling but was " +
          "not first in enabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setEnablingTable(final TableName tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledOrEnablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to enabling but was " +
          "not first in disabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.ENABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table is already in ENABLING state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkAndSetEnablingTable(final TableName tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingOrEnabledTable(tableName)) {
        // If the table is in the one of the states from the states list, the cache
        // might be out-of-date, try to find it out from the master source (zookeeper server).
        //
        // Note: this adds extra zookeeper server calls and might have performance impact.
        // However, this is not the happy path so we should not reach here often. Therefore,
        // the performance impact should be minimal to none.
        ZooKeeperProtos.Table.State currentState =
            ZKTableReadOnly.getTableState(this.watcher, tableName);

        if (currentState == null ||
           currentState == ZooKeeperProtos.Table.State.ENABLING ||
           currentState == ZooKeeperProtos.Table.State.ENABLED) {
          return false;
        }
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.ENABLING);
      return true;
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table isn't in DISABLED state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkDisabledAndSetEnablingTable(final TableName tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.ENABLING);
      return true;
    }
  }

  /**
   * Sets the specified table as DISABLING in zookeeper atomically
   * If the table isn't in ENABLED state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkEnabledAndSetDisablingTable(final TableName tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.get(tableName) != null && !isEnabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
      return true;
    }
  }

  private void setTableState(final TableName tableName, final ZooKeeperProtos.Table.State state)
  throws KeeperException {
    String znode = ZKUtil.joinZNode(this.watcher.tableZNode, tableName.getNameAsString());
    if (ZKUtil.checkExists(this.watcher, znode) == -1) {
      ZKUtil.createAndFailSilent(this.watcher, znode);
    }
    synchronized (this.cache) {
      ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
      builder.setState(state);
      byte [] data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
      ZKUtil.setData(this.watcher, znode, data);
      this.cache.put(tableName, state);
    }
  }

  public boolean isDisabledTable(final TableName tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.DISABLED);
  }

  public boolean isDisablingTable(final TableName tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
  }

  public boolean isEnablingTable(final TableName tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.ENABLING);
  }

  public boolean isEnabledTable(TableName tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.ENABLED);
  }

  public boolean isDisablingOrDisabledTable(final TableName tableName) {
    synchronized (this.cache) {
      return isDisablingTable(tableName) || isDisabledTable(tableName);
    }
  }

  public boolean isEnablingOrEnabledTable(final TableName tableName) {
    synchronized (this.cache) {
      return isEnablingTable(tableName) || isEnabledTable(tableName);
    }
  }

  public boolean isEnabledOrDisablingTable(final TableName tableName) {
    synchronized (this.cache) {
      return isEnabledTable(tableName) || isDisablingTable(tableName);
    }
  }

  public boolean isDisabledOrEnablingTable(final TableName tableName) {
    synchronized (this.cache) {
      return isDisabledTable(tableName) || isEnablingTable(tableName);
    }
  }

  private boolean isTableState(final TableName tableName, final ZooKeeperProtos.Table.State state) {
    synchronized (this.cache) {
      ZooKeeperProtos.Table.State currentState = this.cache.get(tableName);
      return ZKTableReadOnly.isTableState(currentState, state);
    }
  }

  /**
   * Deletes the table in zookeeper.  Fails silently if the
   * table is not currently disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDeletedTable(final TableName tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.remove(tableName) == null) {
        LOG.warn("Moving table " + tableName + " state to deleted but was " +
          "already deleted");
      }
      ZKUtil.deleteNodeFailSilent(this.watcher,
        ZKUtil.joinZNode(this.watcher.tableZNode, tableName.getNameAsString()));
    }
  }
  
  /**
   * Sets the ENABLED state in the cache and creates or force updates a node to
   * ENABLED state for the specified table
   * 
   * @param tableName
   * @throws KeeperException
   */
  public void setEnabledTable(final TableName tableName) throws KeeperException {
    setTableState(tableName, ZooKeeperProtos.Table.State.ENABLED);
  }

  /**
   * check if table is present .
   * 
   * @param tableName
   * @return true if the table is present
   */
  public boolean isTablePresent(final TableName tableName) {
    synchronized (this.cache) {
      ZooKeeperProtos.Table.State state = this.cache.get(tableName);
      return !(state == null);
    }
  }
  
  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   */
  public Set<TableName> getDisabledTables() {
    Set<TableName> disabledTables = new HashSet<TableName>();
    synchronized (this.cache) {
      Set<TableName> tables = this.cache.keySet();
      for (TableName table: tables) {
        if (isDisabledTable(table)) disabledTables.add(table);
      }
    }
    return disabledTables;
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getDisabledTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLED);
  }

  /**
   * Gets a list of all the tables set as disabling in zookeeper.
   * @return Set of disabling tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getDisablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLING);
  }

  /**
   * Gets a list of all the tables set as enabling in zookeeper.
   * @return Set of enabling tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getEnablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.ENABLING);
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<TableName> getDisabledOrDisablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLED,
      ZooKeeperProtos.Table.State.DISABLING);
  }
  
  /**
   * If the table is found in ENABLING state the inmemory state is removed. This
   * helps in cases where CreateTable is to be retried by the client incase of
   * failures.  If deleteZNode is true - the znode is also deleted
   * 
   * @param tableName
   * @param deleteZNode
   * @throws KeeperException
   */
  public void removeEnablingTable(final TableName tableName, boolean deleteZNode)
      throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingTable(tableName)) {
        this.cache.remove(tableName);
        if (deleteZNode) {
          ZKUtil.deleteNodeFailSilent(this.watcher,
              ZKUtil.joinZNode(this.watcher.tableZNode, tableName.getNameAsString()));
        }
      }
    }
  }


  /**
   * Gets a list of all the tables of specified states in zookeeper.
   * @return Set of tables of specified states, empty Set if none
   * @throws KeeperException
   */
  static Set<TableName> getAllTables(final ZooKeeperWatcher zkw,
      final ZooKeeperProtos.Table.State... states) throws KeeperException {
    Set<TableName> allTables = new HashSet<TableName>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    if(children == null) return allTables;
    for (String child: children) {
      TableName tableName = TableName.valueOf(child);
      ZooKeeperProtos.Table.State state = ZKTableReadOnly.getTableState(zkw, tableName);
      for (ZooKeeperProtos.Table.State expectedState: states) {
        if (state == expectedState) {
          allTables.add(tableName);
          break;
        }
      }
    }
    return allTables;
  }
}
