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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.zookeeper.KeeperException;

/**
 * Helper class for table state tracking for use by {@link AssignmentManager}.
 * Reads, caches and sets state up in zookeeper.  If multiple read/write
 * clients, will make for confusion.  Read-only clients other than
 * AssignmentManager interested in learning table state can use the
 * read-only utility methods in {@link ZKTableReadOnly}.
 *
 * <p>To save on trips to the zookeeper ensemble, internally we cache table
 * state.
 */
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
  private final Map<String, TableState> cache =
    new HashMap<String, TableState>();

  // TODO: Make it so always a table znode. Put table schema here as well as table state.
  // Have watcher on table znode so all are notified of state or schema change.
  /**
   * States a Table can be in.
   * Compatibility note: ENABLED does not exist in 0.92 releases.  In 0.92, the absence of
   * the znode indicates the table is enabled.
   */
  public static enum TableState {
    ENABLED,
    DISABLED,
    DISABLING,
    ENABLING
  };

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
      List<String> children =
        ZKUtil.listChildrenNoWatch(this.watcher, this.watcher.masterTableZNode);
      if (children == null) return;
      for (String child: children) {
        TableState state = getTableState(this.watcher, child);
        if (state != null) this.cache.put(child, state);
      }
    }
  }

  /**
   * @param zkw
   * @param child
   * @return Null or {@link TableState} found in znode.
   * @throws KeeperException
   */
  private static TableState getTableState(final ZooKeeperWatcher zkw,
      final String child)
  throws KeeperException {
    return ZKTableReadOnly.getTableState(zkw, zkw.masterTableZNode, child);
  }

  /**
   * Sets the specified table as DISABLED in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisabledTable(String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisablingOrDisabledTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabled but was " +
          "not first in disabling state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.DISABLED);
    }
  }

  /**
   * Sets the specified table as DISABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisablingTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isEnabledOrDisablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabling but was " +
          "not first in enabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.DISABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setEnablingTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledOrEnablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to enabling but was " +
          "not first in disabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.ENABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table is already in ENABLING state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkAndSetEnablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingOrEnabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.ENABLING);
      return true;
    }
  }
  
  /**
   * If the table is found in ENABLING state the inmemory state is removed.
   * This helps in cases where CreateTable is to be retried by the client incase of failures.
   * If deleteZNode is true - the znode is also deleted
   * @param tableName
   * @param deleteZNode
   * @throws KeeperException
   */
  public void removeEnablingTable(final String tableName, boolean deleteZNode)
      throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingTable(tableName)) {
        this.cache.remove(tableName);
        if (deleteZNode) {
          ZKUtil.deleteNodeFailSilent(this.watcher,
              ZKUtil.joinZNode(this.watcher.masterTableZNode, tableName));
        }
      }

    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table isn't in DISABLED state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkDisabledAndSetEnablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.ENABLING);
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
  public boolean checkEnabledAndSetDisablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.get(tableName) != null && !isEnabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.DISABLING);
      return true;
    }
  }

  private void setTableState(final String tableName, final TableState state)
  throws KeeperException {
    String znode = ZKUtil.joinZNode(this.watcher.masterTableZNode, tableName);
    if (ZKUtil.checkExists(this.watcher, znode) == -1) {
      ZKUtil.createAndFailSilent(this.watcher, znode);
    }
    String znode92 = ZKUtil.joinZNode(this.watcher.masterTableZNode92, tableName);
    boolean settingToEnabled = (state == TableState.ENABLED);
    // 0.92 format znode differs in that it is deleted to represent ENABLED,
    // so only create if we are not setting to enabled.
    if (!settingToEnabled) {
      if (ZKUtil.checkExists(this.watcher, znode92) == -1) {
        ZKUtil.createAndFailSilent(this.watcher, znode92);
      }
    }
    synchronized (this.cache) {
      List<ZKUtilOp> ops = new LinkedList<ZKUtilOp>();
      if (settingToEnabled) {
        ops.add(ZKUtilOp.deleteNodeFailSilent(znode92));
      }
      else {
        ops.add(ZKUtilOp.setData(znode92, Bytes.toBytes(state.toString())));
      }
      // If not running multi-update either because of configuration or failure,
      // set the current format znode after the 0.92 format znode.
      // This is so in the case of failure, the AssignmentManager is guaranteed to
      // see the state was not applied, since it uses the current format znode internally.
      ops.add(ZKUtilOp.setData(znode, Bytes.toBytes(state.toString())));
      ZKUtil.multiOrSequential(this.watcher, ops, true);
      this.cache.put(tableName, state);
    }
  }

  public boolean isDisabledTable(final String tableName) {
    return isTableState(tableName, TableState.DISABLED);
  }

  public boolean isDisablingTable(final String tableName) {
    return isTableState(tableName, TableState.DISABLING);
  }

  public boolean isEnablingTable(final String tableName) {
    return isTableState(tableName, TableState.ENABLING);
  }

  public boolean isEnabledTable(String tableName) {
    return isTableState(tableName, TableState.ENABLED);
  }

  public boolean isDisablingOrDisabledTable(final String tableName) {
    synchronized (this.cache) {
      return isDisablingTable(tableName) || isDisabledTable(tableName);
    }
  }

  public boolean isEnablingOrEnabledTable(final String tableName) {
    synchronized (this.cache) {
      return isEnablingTable(tableName) || isEnabledTable(tableName);
    }
  }

  public boolean isEnabledOrDisablingTable(final String tableName) {
    synchronized (this.cache) {
      return isEnabledTable(tableName) || isDisablingTable(tableName);
    }
  }

  public boolean isDisabledOrEnablingTable(final String tableName) {
    synchronized (this.cache) {
      return isDisabledTable(tableName) || isEnablingTable(tableName);
    }
  }

  private boolean isTableState(final String tableName, final TableState state) {
    synchronized (this.cache) {
      TableState currentState = this.cache.get(tableName);
      return ZKTableReadOnly.isTableState(currentState, state);
    }
  }

  /**
   * Deletes the table in zookeeper.  Fails silently if the
   * table is not currently disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDeletedTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      List<ZKUtilOp> ops = new LinkedList<ZKUtilOp>();
      ops.add(ZKUtilOp.deleteNodeFailSilent(
        ZKUtil.joinZNode(this.watcher.masterTableZNode92, tableName)));
      // If not running multi-update either because of configuration or failure,
      // delete the current format znode after the 0.92 format znode.  This is so in the case of
      // failure, the AssignmentManager is guaranteed to see the table was not deleted, since it
      // uses the current format znode internally.
      ops.add(ZKUtilOp.deleteNodeFailSilent(
        ZKUtil.joinZNode(this.watcher.masterTableZNode, tableName)));
      ZKUtil.multiOrSequential(this.watcher, ops, true);
      if (this.cache.remove(tableName) == null) {
        LOG.warn("Moving table " + tableName + " state to deleted but was " +
          "already deleted");
      }
    }
  }
  
  /**
   * Sets the ENABLED state in the cache and creates or force updates a node to
   * ENABLED state for the specified table
   * 
   * @param tableName
   * @throws KeeperException
   */
  public void setEnabledTable(final String tableName) throws KeeperException {
    setTableState(tableName, TableState.ENABLED);
  }

  /**
   * check if table is present .
   * 
   * @param tableName
   * @return true if the table is present
   */
  public boolean isTablePresent(final String tableName) {
    synchronized (this.cache) {
      TableState state = this.cache.get(tableName);
      return !(state == null);
    }
  }
  
  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   */
  public Set<String> getDisabledTables() {
    Set<String> disabledTables = new HashSet<String>();
    synchronized (this.cache) {
      Set<String> tables = this.cache.keySet();
      for (String table: tables) {
        if (isDisabledTable(table)) disabledTables.add(table);
      }
    }
    return disabledTables;
  }

}
