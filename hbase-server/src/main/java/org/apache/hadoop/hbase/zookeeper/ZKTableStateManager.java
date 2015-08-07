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
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of TableStateManager which reads, caches and sets state
 * up in ZooKeeper.  If multiple read/write clients, will make for confusion.
 * Code running on client side without consensus context should use
 * {@link ZKTableStateClientSideReader} instead.
 *
 * <p>To save on trips to the zookeeper ensemble, internally we cache table
 * state.
 */
@InterfaceAudience.Private
public class ZKTableStateManager implements TableStateManager {
  // A znode will exist under the table directory if it is in any of the
  // following states: {@link TableState#ENABLING} , {@link TableState#DISABLING},
  // or {@link TableState#DISABLED}.  If {@link TableState#ENABLED}, there will
  // be no entry for a table in zk.  Thats how it currently works.

  private static final Log LOG = LogFactory.getLog(ZKTableStateManager.class);
  private final ZooKeeperWatcher watcher;

  /**
   * Cache of what we found in zookeeper so we don't have to go to zk ensemble
   * for every query.  Synchronize access rather than use concurrent Map because
   * synchronization needs to span query of zk.
   */
  private final Map<TableName, ZooKeeperProtos.Table.State> cache =
    new HashMap<TableName, ZooKeeperProtos.Table.State>();

  public ZKTableStateManager(final ZooKeeperWatcher zkw) throws KeeperException,
      InterruptedException {
    super();
    this.watcher = zkw;
    populateTableStates();
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @throws KeeperException, InterruptedException
   */
  private void populateTableStates() throws KeeperException, InterruptedException {
    synchronized (this.cache) {
      List<String> children = ZKUtil.listChildrenNoWatch(this.watcher, this.watcher.tableZNode);
      if (children == null) return;
      for (String child: children) {
        TableName tableName = TableName.valueOf(child);
        ZooKeeperProtos.Table.State state = getTableState(this.watcher, tableName);
        if (state != null) this.cache.put(tableName, state);
      }
    }
  }

  /**
   * Sets table state in ZK. Sets no watches.
   *
   * {@inheritDoc}
   */
  @Override
  public void setTableState(TableName tableName, ZooKeeperProtos.Table.State state)
  throws CoordinatedStateException {
    synchronized (this.cache) {
      LOG.warn("Moving table " + tableName + " state from " + this.cache.get(tableName)
        + " to " + state);
      try {
        setTableStateInZK(tableName, state);
      } catch (KeeperException e) {
        throw new CoordinatedStateException(e);
      }
    }
  }

  /**
   * Checks and sets table state in ZK. Sets no watches.
   * {@inheritDoc}
   */
  @Override
  public boolean setTableStateIfInStates(TableName tableName,
                                         ZooKeeperProtos.Table.State newState,
                                         ZooKeeperProtos.Table.State... states)
      throws CoordinatedStateException {
    synchronized (this.cache) {
      // Transition ENABLED->DISABLING has to be performed with a hack, because
      // we treat empty state as enabled in this case because 0.92- clusters.
      if (
          (newState == ZooKeeperProtos.Table.State.DISABLING) &&
               this.cache.get(tableName) != null && !isTableState(tableName, states) ||
          (newState != ZooKeeperProtos.Table.State.DISABLING &&
               !isTableState(tableName, states) )) {
        return false;
      }
      try {
        setTableStateInZK(tableName, newState);
      } catch (KeeperException e) {
        throw new CoordinatedStateException(e);
      }
      return true;
    }
  }

  /**
   * Checks and sets table state in ZK. Sets no watches.
   * {@inheritDoc}
   */
  @Override
  public boolean setTableStateIfNotInStates(TableName tableName,
                                            ZooKeeperProtos.Table.State newState,
                                            ZooKeeperProtos.Table.State... states)
    throws CoordinatedStateException {
    synchronized (this.cache) {
      if (isTableState(tableName, states)) {
        // If the table is in the one of the states from the states list, the cache
        // might be out-of-date, try to find it out from the master source (zookeeper server).
        //
        // Note: this adds extra zookeeper server calls and might have performance impact.
        // However, this is not the happy path so we should not reach here often. Therefore,
        // the performance impact should be minimal to none.
        try {
          ZooKeeperProtos.Table.State curstate = getTableState(watcher, tableName);

          if (isTableInState(Arrays.asList(states), curstate)) {
            return false;
          }
        } catch (KeeperException e) {
          throw new CoordinatedStateException(e);
        } catch (InterruptedException e) {
          throw new CoordinatedStateException(e);
        }
      }
      try {
        setTableStateInZK(tableName, newState);
      } catch (KeeperException e) {
        throw new CoordinatedStateException(e);
      }
      return true;
    }
  }

  private void setTableStateInZK(final TableName tableName,
                                 final ZooKeeperProtos.Table.State state)
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

  /**
   * Checks if table is marked in specified state in ZK.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean isTableState(final TableName tableName,
      final ZooKeeperProtos.Table.State... states) {
    synchronized (this.cache) {
      ZooKeeperProtos.Table.State currentState = this.cache.get(tableName);
      return isTableInState(Arrays.asList(states), currentState);
    }
  }

  /**
   * Deletes the table in zookeeper.  Fails silently if the
   * table is not currently disabled in zookeeper.  Sets no watches.
   *
   * {@inheritDoc}
   */
  @Override
  public void setDeletedTable(final TableName tableName)
  throws CoordinatedStateException {
    synchronized (this.cache) {
      if (this.cache.remove(tableName) == null) {
        LOG.warn("Moving table " + tableName + " state to deleted but was " +
          "already deleted");
      }
      try {
        ZKUtil.deleteNodeFailSilent(this.watcher,
          ZKUtil.joinZNode(this.watcher.tableZNode, tableName.getNameAsString()));
      } catch (KeeperException e) {
        throw new CoordinatedStateException(e);
      }
    }
  }

  /**
   * check if table is present.
   *
   * @param tableName table we're working on
   * @return true if the table is present
   */
  @Override
  public boolean isTablePresent(final TableName tableName) {
    synchronized (this.cache) {
      ZooKeeperProtos.Table.State state = this.cache.get(tableName);
      return !(state == null);
    }
  }

  /**
   * Gets a list of all the tables set as disabling in zookeeper.
   * @return Set of disabling tables, empty Set if none
   * @throws CoordinatedStateException if error happened in underlying coordination engine
   */
  @Override
  public Set<TableName> getTablesInStates(ZooKeeperProtos.Table.State... states)
    throws InterruptedIOException, CoordinatedStateException {
    try {
      return getAllTables(states);
    } catch (KeeperException e) {
      throw new CoordinatedStateException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkAndRemoveTableState(TableName tableName, ZooKeeperProtos.Table.State states,
                                       boolean deletePermanentState)
      throws CoordinatedStateException {
    synchronized (this.cache) {
      if (isTableState(tableName, states)) {
        this.cache.remove(tableName);
        if (deletePermanentState) {
          try {
            ZKUtil.deleteNodeFailSilent(this.watcher,
                ZKUtil.joinZNode(this.watcher.tableZNode, tableName.getNameAsString()));
          } catch (KeeperException e) {
            throw new CoordinatedStateException(e);
          }
        }
      }
    }
  }

  /**
   * Gets a list of all the tables of specified states in zookeeper.
   * @return Set of tables of specified states, empty Set if none
   * @throws KeeperException
   */
  Set<TableName> getAllTables(final ZooKeeperProtos.Table.State... states)
      throws KeeperException, InterruptedIOException {

    Set<TableName> allTables = new HashSet<TableName>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(watcher, watcher.tableZNode);
    if(children == null) return allTables;
    for (String child: children) {
      TableName tableName = TableName.valueOf(child);
      ZooKeeperProtos.Table.State state;
      try {
        state = getTableState(watcher, tableName);
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
      for (ZooKeeperProtos.Table.State expectedState: states) {
        if (state == expectedState) {
          allTables.add(tableName);
          break;
        }
      }
    }
    return allTables;
  }

  /**
   * Gets table state from ZK.
   * @param zkw ZooKeeperWatcher instance to use
   * @param tableName table we're checking
   * @return Null or {@link ZooKeeperProtos.Table.State} found in znode.
   * @throws KeeperException
   */
  private ZooKeeperProtos.Table.State getTableState(final ZooKeeperWatcher zkw,
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

  /**
   * @return true if current state isn't null and is contained
   * in the list of expected states.
   */
  private boolean isTableInState(final List<ZooKeeperProtos.Table.State> expectedStates,
                       final ZooKeeperProtos.Table.State currentState) {
    return currentState != null && expectedStates.contains(currentState);
  }
}
