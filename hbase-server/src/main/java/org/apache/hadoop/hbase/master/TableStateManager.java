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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.hbase.util.IdReadWriteLockWithObjectPool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This is a helper class used to manage table states. This class uses hbase:meta as its store for
 * table state so hbase:meta must be online before accessing its methods.
 */
@InterfaceAudience.Private
public class TableStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(TableStateManager.class);

  private final IdReadWriteLock<TableName> tnLock = new IdReadWriteLockWithObjectPool<>();
  private final MasterServices master;

  private final ConcurrentMap<TableName, TableState.State> tableName2State =
    new ConcurrentHashMap<>();

  TableStateManager(MasterServices master) {
    this.master = master;
  }

  /**
   * Set table state to provided. Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState  new state
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
    if (tableName.equals(master.getConnection().getMetaTableName())) {
      // Can't delete the hbase:meta table.
      return;
    }
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.writeLock().lock();
    try {
      MetaTableAccessor.deleteTableState(master.getConnection(), tableName);
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
    MetaTableAccessor.fullScanTables(master.getConnection(), new ClientMetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState tableState = CatalogFamilyFormat.getTableState(r);
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
    if (tableName.equals(master.getConnection().getMetaTableName())) {
      if (
        TableState.State.DISABLING.equals(newState) || TableState.State.DISABLED.equals(newState)
      ) {
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
}
