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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.hbase.util.ZKDataMigrator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This is a helper class used to manage table states. This class uses hbase:meta as its store for
 * table state so hbase:meta must be online before {@link #start()} is called.
 */
// TODO: Make this a guava Service
@InterfaceAudience.Private
public class TableStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(TableStateManager.class);
  /**
   * Set this key to false in Configuration to disable migrating table state from zookeeper so
   * hbase:meta table.
   */
  private static final String MIGRATE_TABLE_STATE_FROM_ZK_KEY =
    "hbase.migrate.table.state.from.zookeeper";

  private final IdReadWriteLock<TableName> tnLock = new IdReadWriteLock<>();
  protected final MasterServices master;

  private final ConcurrentMap<TableName, TableState.State> tableName2State =
    new ConcurrentHashMap<>();

  TableStateManager(MasterServices master) {
    this.master = master;
  }

  /**
   * Set table state to provided. Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState new state
   */
  public void setTableState(TableName tableName, TableState.State newState) throws IOException {
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.writeLock().lock();
    try {
      updateMetaState(tableName, newState);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isTableState(TableName tableName, TableState.State... states) {
    try {
      TableState tableState = getTableState(tableName);
      return tableState.isInStates(states);
    } catch (IOException e) {
      LOG.error("Unable to get table " + tableName + " state", e);
      // XXX: is it safe to just return false here?
      return false;
    }
  }

  public void setDeletedTable(TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // Can't delete the hbase:meta table.
      return;
    }
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.writeLock().lock();
    try {
      MetaTableAccessor.deleteTableState(master.getConnection(), tableName);
      metaStateDeleted(tableName);
    } finally {
      tableName2State.remove(tableName);
      lock.writeLock().unlock();
    }
  }

  public boolean isTablePresent(TableName tableName) throws IOException {
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.readLock().lock();
    try {
      return readMetaState(tableName) != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all tables in given states.
   * @param states filter by states
   * @return tables in given states
   */
  Set<TableName> getTablesInStates(TableState.State... states) throws IOException {
    // Only be called in region normalizer, will not use cache.
    final Set<TableName> rv = Sets.newHashSet();
    MetaTableAccessor.fullScanTables(master.getConnection(), new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState tableState = MetaTableAccessor.getTableState(r);
        if (tableState != null && tableState.inStates(states)) {
          rv.add(tableState.getTableName());
        }
        return true;
      }
    });
    return rv;
  }

  @NonNull
  public TableState getTableState(TableName tableName) throws IOException {
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.readLock().lock();
    try {
      TableState currentState = readMetaState(tableName);
      if (currentState == null) {
        throw new TableNotFoundException("No state found for " + tableName);
      }
      return currentState;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void updateMetaState(TableName tableName, TableState.State newState) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      if (TableState.State.DISABLING.equals(newState) ||
          TableState.State.DISABLED.equals(newState)) {
        throw new IllegalArgumentIOException("Cannot disable meta table; " + newState);
      }
      // Otherwise, just return; no need to set ENABLED on meta -- it is always ENABLED.
      return;
    }
    boolean succ = false;
    try {
      MetaTableAccessor.updateTableState(master.getConnection(), tableName, newState);
      tableName2State.put(tableName, newState);
      succ = true;
    } finally {
      if (!succ) {
        this.tableName2State.remove(tableName);
      }
    }
    metaStateUpdated(tableName, newState);
  }

  protected void metaStateUpdated(TableName tableName, TableState.State newState)
      throws IOException {
  }

  protected void metaStateDeleted(TableName tableName) throws IOException {
  }

  @Nullable
  private TableState readMetaState(TableName tableName) throws IOException {
    TableState.State state = tableName2State.get(tableName);
    if (state != null) {
      return new TableState(tableName, state);
    }
    TableState tableState = MetaTableAccessor.getTableState(master.getConnection(), tableName);
    if (tableState != null) {
      tableName2State.putIfAbsent(tableName, tableState.getState());
    }
    return tableState;
  }

  public void start() throws IOException {
    migrateZooKeeper();
    fixTableStates(master.getTableDescriptors(), master.getConnection());
  }

  private void fixTableStates(TableDescriptors tableDescriptors, Connection connection)
      throws IOException {
    Map<String, TableState> states = new HashMap<>();
    // NOTE: Full hbase:meta table scan!
    MetaTableAccessor.fullScanTables(connection, new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState state = MetaTableAccessor.getTableState(r);
        states.put(state.getTableName().getNameAsString(), state);
        return true;
      }
    });
    for (TableDescriptor tableDesc : tableDescriptors.getAll().values()) {
      TableName tableName = tableDesc.getTableName();
      if (TableName.isMetaTableName(tableName)) {
        // This table is always enabled. No fixup needed. No entry in hbase:meta needed.
        // Call through to fixTableState though in case a super class wants to do something.
        fixTableState(new TableState(tableName, TableState.State.ENABLED));
        continue;
      }
      TableState tableState = states.get(tableName.getNameAsString());
      if (tableState == null) {
        LOG.warn(tableName + " has no table state in hbase:meta, assuming ENABLED");
        MetaTableAccessor.updateTableState(connection, tableName, TableState.State.ENABLED);
        fixTableState(new TableState(tableName, TableState.State.ENABLED));
        tableName2State.put(tableName, TableState.State.ENABLED);
      } else {
        fixTableState(tableState);
        tableName2State.put(tableName, tableState.getState());
      }
    }
  }

  /**
   * For subclasses in case they want to do fixup post hbase:meta.
   */
  protected void fixTableState(TableState tableState) throws IOException {
  }

  /**
   * This code is for case where a hbase2 Master is starting for the first time. ZooKeeper is where
   * we used to keep table state. On first startup, read zookeeper and update hbase:meta with the
   * table states found in zookeeper. This is tricky as we'll do this check every time we startup
   * until mirroring is disabled. See the {@link #MIGRATE_TABLE_STATE_FROM_ZK_KEY} flag. Original
   * form of this migration came in with HBASE-13032. It deleted all znodes when done. We can't do
   * that if we want to support hbase-1.x clients who need to be able to read table state out of zk.
   * See {@link MirroringTableStateManager}.
   * @deprecated Since 2.0.0. Remove in hbase-3.0.0.
   */
  @Deprecated
  private void migrateZooKeeper() throws IOException {
    if (!this.master.getConfiguration().getBoolean(MIGRATE_TABLE_STATE_FROM_ZK_KEY, true)) {
      return;
    }
    try {
      for (Map.Entry<TableName, TableState.State> entry : ZKDataMigrator
        .queryForTableStates(this.master.getZooKeeper()).entrySet()) {
        if (this.master.getTableDescriptors().get(entry.getKey()) == null) {
          deleteZooKeeper(entry.getKey());
          LOG.info("Purged table state entry from zookeepr for table not in hbase:meta: " +
            entry.getKey());
          continue;
        }
        TableState ts = null;
        try {
          ts = getTableState(entry.getKey());
        } catch (TableNotFoundException e) {
          // This can happen; table exists but no TableState.
        }
        if (ts == null) {
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
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Failed reading table state from zookeeper", e);
    }
  }

  /**
   * Utility method that knows how to delete the old hbase-1.x table state znode. Used also by the
   * Mirroring subclass.
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
