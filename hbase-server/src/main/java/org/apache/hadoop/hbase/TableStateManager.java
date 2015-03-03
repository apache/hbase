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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;

import java.io.InterruptedIOException;
import java.util.Set;

/**
 * Helper class for table state management for operations running inside
 * RegionServer or HMaster.
 * Depending on implementation, fetches information from HBase system table,
 * local data store, ZooKeeper ensemble or somewhere else.
 * Code running on client side (with no coordinated state context) shall instead use
 * {@link org.apache.hadoop.hbase.zookeeper.ZKTableStateClientSideReader}
 */
@InterfaceAudience.Private
public interface TableStateManager {

  /**
   * Sets the table into desired state. Fails silently if the table is already in this state.
   * @param tableName table to process
   * @param state new state of this table
   * @throws CoordinatedStateException if error happened when trying to set table state
   */
  void setTableState(TableName tableName, ZooKeeperProtos.Table.State state)
    throws CoordinatedStateException;

  /**
   * Sets the specified table into the newState, but only if the table is already in
   * one of the possibleCurrentStates (otherwise no operation is performed).
   * @param tableName table to process
   * @param newState new state for the table
   * @param states table should be in one of these states for the operation
   *                              to be performed
   * @throws CoordinatedStateException if error happened while performing operation
   * @return true if operation succeeded, false otherwise
   */
  boolean setTableStateIfInStates(TableName tableName, ZooKeeperProtos.Table.State newState,
                                  ZooKeeperProtos.Table.State... states)
    throws CoordinatedStateException;

  /**
   * Sets the specified table into the newState, but only if the table is NOT in
   * one of the possibleCurrentStates (otherwise no operation is performed).
   * @param tableName table to process
   * @param newState new state for the table
   * @param states table should NOT be in one of these states for the operation
   *                              to be performed
   * @throws CoordinatedStateException if error happened while performing operation
   * @return true if operation succeeded, false otherwise
   */
  boolean setTableStateIfNotInStates(TableName tableName, ZooKeeperProtos.Table.State newState,
                                     ZooKeeperProtos.Table.State... states)
    throws CoordinatedStateException;

  /**
   * @return true if the table is in any one of the listed states, false otherwise.
   */
  boolean isTableState(TableName tableName, ZooKeeperProtos.Table.State... states);

  /**
   * Mark table as deleted.  Fails silently if the table is not currently marked as disabled.
   * @param tableName table to be deleted
   * @throws CoordinatedStateException if error happened while performing operation
   */
  void setDeletedTable(TableName tableName) throws CoordinatedStateException;

  /**
   * Checks if table is present.
   *
   * @param tableName table we're checking
   * @return true if the table is present, false otherwise
   */
  boolean isTablePresent(TableName tableName);

  /**
   * @return set of tables which are in any one of the listed states, empty Set if none
   */
  Set<TableName> getTablesInStates(ZooKeeperProtos.Table.State... states)
    throws InterruptedIOException, CoordinatedStateException;

  /**
   * If the table is found in the given state the in-memory state is removed. This
   * helps in cases where CreateTable is to be retried by the client in case of
   * failures.  If deletePermanentState is true - the flag kept permanently is
   * also reset.
   *
   * @param tableName table we're working on
   * @param states if table isn't in any one of these states, operation aborts
   * @param deletePermanentState if true, reset the permanent flag
   * @throws CoordinatedStateException if error happened in underlying coordination engine
   */
  void checkAndRemoveTableState(TableName tableName, ZooKeeperProtos.Table.State states,
                            boolean deletePermanentState)
    throws CoordinatedStateException;
}
