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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
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
  private final Map<String, ZooKeeperProtos.Table.State> cache =
    new HashMap<String, ZooKeeperProtos.Table.State>();

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
        ZooKeeperProtos.Table.State state = ZKTableReadOnly.getTableState(this.watcher, child);
        if (state != null) this.cache.put(child, state);
      }
    }
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
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLED);
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
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
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
  public boolean checkAndSetEnablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingTable(tableName)) {
        return false;
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
  public boolean checkDisabledAndSetEnablingTable(final String tableName)
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
  public boolean checkEnabledAndSetDisablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.get(tableName) != null && !isEnabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
      return true;
    }
  }

  private void setTableState(final String tableName, final ZooKeeperProtos.Table.State state)
  throws KeeperException {
    String znode = ZKUtil.joinZNode(this.watcher.tableZNode, tableName);
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

  public boolean isDisabledTable(final String tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.DISABLED);
  }

  public boolean isDisablingTable(final String tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.DISABLING);
  }

  public boolean isEnablingTable(final String tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.ENABLING);
  }

  public boolean isEnabledTable(String tableName) {
    return isTableState(tableName, ZooKeeperProtos.Table.State.ENABLED);
  }

  public boolean isDisablingOrDisabledTable(final String tableName) {
    synchronized (this.cache) {
      return isDisablingTable(tableName) || isDisabledTable(tableName);
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

  private boolean isTableState(final String tableName, final ZooKeeperProtos.Table.State state) {
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
  public void setDeletedTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.remove(tableName) == null) {
        LOG.warn("Moving table " + tableName + " state to deleted but was " +
          "already deleted");
      }
      ZKUtil.deleteNodeFailSilent(this.watcher,
        ZKUtil.joinZNode(this.watcher.tableZNode, tableName));
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
    setTableState(tableName, ZooKeeperProtos.Table.State.ENABLED);
  }

  /**
   * check if table is present .
   * 
   * @param tableName
   * @return true if the table is present
   */
  public boolean isTablePresent(final String tableName) {
    synchronized (this.cache) {
      ZooKeeperProtos.Table.State state = this.cache.get(tableName);
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

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getDisabledTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLED);
  }

  /**
   * Gets a list of all the tables set as disabling in zookeeper.
   * @return Set of disabling tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getDisablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLING);
  }

  /**
   * Gets a list of all the tables set as enabling in zookeeper.
   * @return Set of enabling tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getEnablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.ENABLING);
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException
   */
  public static Set<String> getDisabledOrDisablingTables(ZooKeeperWatcher zkw)
      throws KeeperException {
    return getAllTables(zkw, ZooKeeperProtos.Table.State.DISABLED,
      ZooKeeperProtos.Table.State.DISABLING);
  }

  /**
   * Gets a list of all the tables of specified states in zookeeper.
   * @return Set of tables of specified states, empty Set if none
   * @throws KeeperException
   */
  static Set<String> getAllTables(final ZooKeeperWatcher zkw,
      final ZooKeeperProtos.Table.State... states) throws KeeperException {
    Set<String> allTables = new HashSet<String>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    for (String child: children) {
      ZooKeeperProtos.Table.State state = ZKTableReadOnly.getTableState(zkw, child);
      for (ZooKeeperProtos.Table.State expectedState: states) {
        if (state == expectedState) {
          allTables.add(child);
          break;
        }
      }
    }
    return allTables;
  }
}
