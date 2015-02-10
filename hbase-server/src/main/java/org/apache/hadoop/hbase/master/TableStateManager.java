/**
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;

/**
 * This is a helper class used to manage table states.
 * States persisted in tableinfo and cached internally.
 */
@InterfaceAudience.Private
public class TableStateManager {
  private static final Log LOG = LogFactory.getLog(TableStateManager.class);

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final MasterServices master;

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
      udpateMetaState(tableName, newState);
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
   * @throws IOException
   */
  public boolean setTableStateIfInStates(TableName tableName,
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
        udpateMetaState(tableName, newState);
        return true;
      } else {
        return false;
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
      udpateMetaState(tableName, newState);
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
      LOG.error("Unable to get table " + tableName + " state, probably table not exists");
      return false;
    }
  }

  public void setDeletedTable(TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME))
      return;
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

  @Nonnull
  public TableState.State getTableState(TableName tableName) throws IOException {
    TableState currentState = readMetaState(tableName);
    if (currentState == null) {
      throw new TableNotFoundException(tableName);
    }
    return currentState.getState();
  }

  protected void udpateMetaState(TableName tableName, TableState.State newState)
      throws IOException {
    MetaTableAccessor.updateTableState(master.getConnection(), tableName, newState);
  }

  @Nullable
  protected TableState readMetaState(TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME))
      return new TableState(tableName, TableState.State.ENABLED);
    return MetaTableAccessor.getTableState(master.getConnection(), tableName);
  }

  @SuppressWarnings("deprecation")
  public void start() throws IOException {
    TableDescriptors tableDescriptors = master.getTableDescriptors();
    Connection connection = master.getConnection();
    fixTableStates(tableDescriptors, connection);
  }

  public static void fixTableStates(TableDescriptors tableDescriptors, Connection connection)
      throws IOException {
    final Map<String, TableDescriptor> allDescriptors =
        tableDescriptors.getAllDescriptors();
    final Map<String, TableState> states = new HashMap<>();
    MetaTableAccessor.fullScanTables(connection, new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState state = MetaTableAccessor.getTableState(r);
        if (state != null)
          states.put(state.getTableName().getNameAsString(), state);
        return true;
      }
    });
    for (Map.Entry<String, TableDescriptor> entry : allDescriptors.entrySet()) {
      String table = entry.getKey();
      if (table.equals(TableName.META_TABLE_NAME.getNameAsString()))
        continue;
      if (!states.containsKey(table)) {
        LOG.warn("Found table without state " + table);
        TableDescriptor td = entry.getValue();
        TableState.State tds = td.getTableState();
        if (tds != null) {
          LOG.warn("Found table with state in descriptor, using that state");
          MetaTableAccessor.updateTableState(connection, TableName.valueOf(table), tds);
          LOG.warn("Updating table descriptor");
          td.setTableState(null);
          tableDescriptors.add(td);
        } else {
          LOG.warn("Found table with no state in descriptor, assuming ENABLED");
          MetaTableAccessor.updateTableState(connection, TableName.valueOf(table),
              TableState.State.ENABLED);
        }
      }
    }
  }
}
