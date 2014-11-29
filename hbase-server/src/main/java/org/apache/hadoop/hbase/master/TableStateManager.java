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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.TableState;

/**
 * This is a helper class used to manage table states.
 * States persisted in tableinfo and cached internally.
 */
@InterfaceAudience.Private
public class TableStateManager {
  private static final Log LOG = LogFactory.getLog(TableStateManager.class);
  private final TableDescriptors descriptors;

  private final Map<TableName, TableState.State> tableStates = Maps.newConcurrentMap();

  public TableStateManager(MasterServices master) {
    this.descriptors = master.getTableDescriptors();
  }

  public void start() throws IOException {
    Map<String, TableDescriptor> all = descriptors.getAllDescriptors();
    for (TableDescriptor table : all.values()) {
      TableName tableName = table.getHTableDescriptor().getTableName();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding table state: " + tableName
            + ": " + table.getTableState());
      }
      tableStates.put(tableName, table.getTableState());
    }
  }

  /**
   * Set table state to provided.
   * Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState new state
   * @throws IOException
   */
  public void setTableState(TableName tableName, TableState.State newState) throws IOException {
    synchronized (tableStates) {
      TableDescriptor descriptor = readDescriptor(tableName);
      if (descriptor == null) {
        throw new TableNotFoundException(tableName);
      }
      if (descriptor.getTableState() != newState) {
        writeDescriptor(
            new TableDescriptor(descriptor.getHTableDescriptor(), newState));
      }
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
    synchronized (tableStates) {
      TableDescriptor descriptor = readDescriptor(tableName);
      if (descriptor == null) {
        throw new TableNotFoundException(tableName);
      }
      if (TableState.isInStates(descriptor.getTableState(), states)) {
        writeDescriptor(
            new TableDescriptor(descriptor.getHTableDescriptor(), newState));
        return true;
      } else {
        return false;
      }
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
    synchronized (tableStates) {
      TableDescriptor descriptor = readDescriptor(tableName);
      if (descriptor == null) {
        throw new TableNotFoundException(tableName);
      }
      if (!TableState.isInStates(descriptor.getTableState(), states)) {
        writeDescriptor(
            new TableDescriptor(descriptor.getHTableDescriptor(), newState));
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean isTableState(TableName tableName, TableState.State... states) {
    TableState.State tableState = null;
    try {
      tableState = getTableState(tableName);
    } catch (IOException e) {
      LOG.error("Unable to get table state, probably table not exists");
      return false;
    }
    return tableState != null && TableState.isInStates(tableState, states);
  }

  public void setDeletedTable(TableName tableName) throws IOException {
    TableState.State remove = tableStates.remove(tableName);
    if (remove == null) {
      LOG.warn("Moving table " + tableName + " state to deleted but was " +
              "already deleted");
    }
  }

  public boolean isTablePresent(TableName tableName) throws IOException {
    return getTableState(tableName) != null;
  }

  /**
   * Return all tables in given states.
   *
   * @param states filter by states
   * @return tables in given states
   * @throws IOException
   */
  public Set<TableName> getTablesInStates(TableState.State... states) throws IOException {
    Set<TableName> rv = Sets.newHashSet();
    for (Map.Entry<TableName, TableState.State> entry : tableStates.entrySet()) {
      if (TableState.isInStates(entry.getValue(), states))
        rv.add(entry.getKey());
    }
    return rv;
  }

  public TableState.State getTableState(TableName tableName) throws IOException {
    TableState.State tableState = tableStates.get(tableName);
    if (tableState == null) {
      TableDescriptor descriptor = readDescriptor(tableName);
      if (descriptor != null)
        tableState = descriptor.getTableState();
    }
    return tableState;
  }

  /**
   * Write descriptor in place, update cache of states.
   * Write lock should be hold by caller.
   *
   * @param descriptor what to write
   */
  private void writeDescriptor(TableDescriptor descriptor) throws IOException {
    TableName tableName = descriptor.getHTableDescriptor().getTableName();
    TableState.State state = descriptor.getTableState();
    descriptors.add(descriptor);
    LOG.debug("Table " + tableName + " written descriptor for state " + state);
    tableStates.put(tableName, state);
    LOG.debug("Table " + tableName + " updated state to " + state);
  }

  /**
   * Read current descriptor for table, update cache of states.
   *
   * @param table descriptor to read
   * @return descriptor
   * @throws IOException
   */
  private TableDescriptor readDescriptor(TableName tableName) throws IOException {
    TableDescriptor descriptor = descriptors.getDescriptor(tableName);
    if (descriptor == null)
      tableStates.remove(tableName);
    else
      tableStates.put(tableName, descriptor.getTableState());
    return descriptor;
  }
}
