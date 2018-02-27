/*
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.ZKDataMigrator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;

/**
 * This is a helper class used to manage table states.
 * This class uses hbase:meta as its store for table state so hbase:meta must be online before
 * {@link #start()} is called.
 * TODO: Cache state. Cut down on meta looksups.
 */
// TODO: Make this a guava Service
@InterfaceAudience.Private
public class TableStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(TableStateManager.class);
  /**
   * Set this key to false in Configuration to disable migrating table state from zookeeper
   * so hbase:meta table.
   */
  static final String MIGRATE_TABLE_STATE_FROM_ZK_KEY = "hbase.migrate.table.state.from.zookeeper";

  final ReadWriteLock lock = new ReentrantReadWriteLock();
  final MasterServices master;

  public TableStateManager(MasterServices master) {
    this.master = master;
  }

  /**
   * Set table state to provided.
   * Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState new state
   * @throws IOException
   */
  public void setTableState(TableName tableName, TableState.State newState) throws IOException {
    lock.writeLock().lock();
    try {
      updateMetaState(tableName, newState);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Set table state to provided but only if table in specified states
   * Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState new state
   * @param states states to check against
   * @return null if succeed or table state if failed
   * @throws IOException
   */
  public TableState.State setTableStateIfInStates(TableName tableName,
                                         TableState.State newState,
                                         TableState.State... states)
          throws IOException {
    lock.writeLock().lock();
    try {
      TableState currentState = readMetaState(tableName);
      if (currentState == null) {
        throw new TableNotFoundException(tableName);
      }
      if (currentState.inStates(states)) {
        updateMetaState(tableName, newState);
        return null;
      } else {
        return currentState.getState();
      }
    } finally {
      lock.writeLock().unlock();
    }

  }

  /**
   * Set table state to provided but only if table not in specified states
   * Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState new state
   * @param states states to check against
   * @throws IOException
   */
  public boolean setTableStateIfNotInStates(TableName tableName,
                                            TableState.State newState,
                                            TableState.State... states)
          throws IOException {
    TableState currentState = readMetaState(tableName);
    if (currentState == null) {
      throw new TableNotFoundException(tableName);
    }
    if (!currentState.inStates(states)) {
      updateMetaState(tableName, newState);
      return true;
    } else {
      return false;
    }
  }

  public boolean isTableState(TableName tableName, TableState.State... states) {
    try {
      TableState.State tableState = getTableState(tableName);
      return TableState.isInStates(tableState, states);
    } catch (IOException e) {
      LOG.error("Unable to get table " + tableName + " state", e);
      return false;
    }
  }

  public void setDeletedTable(TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return;
    }
    MetaTableAccessor.deleteTableState(master.getConnection(), tableName);
  }

  public boolean isTablePresent(TableName tableName) throws IOException {
    return readMetaState(tableName) != null;
  }

  /**
   * Return all tables in given states.
   *
   * @param states filter by states
   * @return tables in given states
   * @throws IOException
   */
  public Set<TableName> getTablesInStates(final TableState.State... states) throws IOException {
    final Set<TableName> rv = Sets.newHashSet();
    MetaTableAccessor.fullScanTables(master.getConnection(), new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState tableState = MetaTableAccessor.getTableState(r);
        if (tableState != null && tableState.inStates(states))
          rv.add(tableState.getTableName());
        return true;
      }
    });
    return rv;
  }

  public static class TableStateNotFoundException extends TableNotFoundException {
    TableStateNotFoundException(TableName tableName) {
      super(tableName.getNameAsString());
    }
  }

  @NonNull
  public TableState.State getTableState(TableName tableName) throws IOException {
    TableState currentState = readMetaState(tableName);
    if (currentState == null) {
      throw new TableStateNotFoundException(tableName);
    }
    return currentState.getState();
  }

  protected void updateMetaState(TableName tableName, TableState.State newState)
      throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      if (TableState.State.DISABLING.equals(newState) ||
          TableState.State.DISABLED.equals(newState)) {
        throw new IllegalArgumentIOException("Cannot disable the meta table; " + newState);
      }
      // Otherwise, just return; no need to set ENABLED on meta -- it is always ENABLED.
      return;
    }
    MetaTableAccessor.updateTableState(master.getConnection(), tableName, newState);
  }

  @Nullable
  protected TableState readMetaState(TableName tableName) throws IOException {
    return MetaTableAccessor.getTableState(master.getConnection(), tableName);
  }

  public void start() throws IOException {
    TableDescriptors tableDescriptors = master.getTableDescriptors();
    migrateZooKeeper();
    Connection connection = master.getConnection();
    fixTableStates(tableDescriptors, connection);
  }

  private void fixTableStates(TableDescriptors tableDescriptors, Connection connection)
      throws IOException {
    final Map<String, TableDescriptor> allDescriptors = tableDescriptors.getAll();
    final Map<String, TableState> states = new HashMap<>();
    // NOTE: Ful hbase:meta table scan!
    MetaTableAccessor.fullScanTables(connection, new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState state = MetaTableAccessor.getTableState(r);
        states.put(state.getTableName().getNameAsString(), state);
        return true;
      }
    });
    for (Map.Entry<String, TableDescriptor> entry: allDescriptors.entrySet()) {
      String table = entry.getKey();
      if (table.equals(TableName.META_TABLE_NAME.getNameAsString())) {
        // This table is always enabled. No fixup needed. No entry in hbase:meta needed.
        // Call through to fixTableState though in case a super class wants to do something.
        fixTableState(new TableState(TableName.valueOf(table), TableState.State.ENABLED));
        continue;
      }
      TableState tableState = states.get(table);
      if (tableState == null || tableState.getState() == null) {
        LOG.warn(table + " has no table state in hbase:meta, assuming ENABLED");
        MetaTableAccessor.updateTableState(connection, TableName.valueOf(table),
            TableState.State.ENABLED);
        fixTableState(new TableState(TableName.valueOf(table), TableState.State.ENABLED));
      } else {
        fixTableState(tableState);
      }
    }
  }

  /**
   * For subclasses in case they want to do fixup post hbase:meta.
   */
  protected void fixTableState(TableState tableState) throws IOException {}

  /**
   * This code is for case where a hbase2 Master is starting for the first time. ZooKeeper is
   * where we used to keep table state. On first startup, read zookeeper and update hbase:meta
   * with the table states found in zookeeper. This is tricky as we'll do this check every time we
   * startup until mirroring is disabled. See the {@link #MIGRATE_TABLE_STATE_FROM_ZK_KEY} flag.
   * Original form of this migration came in with HBASE-13032. It deleted all znodes when done.
   * We can't do that if we want to support hbase-1.x clients who need to be able to read table
   * state out of zk. See {@link MirroringTableStateManager}.
   * @deprecated Since 2.0.0. Remove in hbase-3.0.0.
   */
  @Deprecated
  private void migrateZooKeeper() throws IOException {
    if (this.master.getConfiguration().getBoolean(MIGRATE_TABLE_STATE_FROM_ZK_KEY, false)) {
      return;
    }
    try {
      for (Map.Entry<TableName, TableState.State> entry:
          ZKDataMigrator.queryForTableStates(this.master.getZooKeeper()).entrySet()) {
        if (this.master.getTableDescriptors().get(entry.getKey()) == null) {
          deleteZooKeeper(entry.getKey());
          LOG.info("Purged table state entry from zookeepr for table not in hbase:meta: " +
              entry.getKey());
          continue;
        }
        TableState.State state = null;
        try {
          state = getTableState(entry.getKey());
        } catch (TableStateNotFoundException e) {
          // This can happen; table exists but no TableState.
        }
        if (state == null) {
          TableState.State zkstate = entry.getValue();
          // Only migrate if it is an enable or disabled table. If in-between -- ENABLING or
          // DISABLING then we have a problem; we are starting up an hbase-2 on a cluster with
          // RIT. It is going to be rough!
          if (zkstate.equals(TableState.State.ENABLED) ||
              zkstate.equals(TableState.State.DISABLED)) {
            LOG.info("Migrating table state from zookeeper to hbase:meta; tableName=" +
                entry.getKey() + ", state=" + entry.getValue());
            updateMetaState(entry.getKey(), entry.getValue());
          } else {
            LOG.warn("Table={} has no state and zookeeper state is in-between={} (neither " +
                "ENABLED or DISABLED); NOT MIGRATING table state", entry.getKey(), zkstate);
          }
        }
        // What if the table states disagree? Defer to the hbase:meta setting rather than have the
        // hbase-1.x support prevail.
      }
    } catch (KeeperException |InterruptedException e) {
      LOG.warn("Failed reading table state from zookeeper", e);
    }
  }

  /**
   * Utility method that knows how to delete the old hbase-1.x table state znode.
   * Used also by the Mirroring subclass.
   * @deprecated Since 2.0.0. To be removed in hbase-3.0.0.
   */
  @Deprecated
  protected void deleteZooKeeper(TableName tableName) {
    try {
      // Delete from ZooKeeper
      String znode = ZNodePaths.joinZNode(this.master.getZooKeeper().getZNodePaths().tableZNode,
          tableName.getNameAsString());
      ZKUtil.deleteNodeFailSilent(this.master.getZooKeeper(), znode);
    } catch (KeeperException e) {
      LOG.warn("Failed deleting table state from zookeeper", e);
    }
  }
}
