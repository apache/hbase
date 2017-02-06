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
package org.apache.hadoop.hbase.client;

import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Pair;

/**
 *  The asynchronous administrative API for HBase.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncAdmin {

  /**
   * @return Async Connection used by this object.
   */
  AsyncConnectionImpl getConnection();

  /**
   * @param tableName Table to check.
   * @return True if table exists already. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> tableExists(final TableName tableName);

  /**
   * List all the userspace tables.
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   * @see #listTables(Pattern, boolean)
   */
  CompletableFuture<HTableDescriptor[]> listTables();

  /**
   * List all the tables matching the given pattern.
   * @param regex The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   * @see #listTables(Pattern, boolean)
   */
  CompletableFuture<HTableDescriptor[]> listTables(String regex, boolean includeSysTables);

  /**
   * List all the tables matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> listTables(Pattern pattern, boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Pattern, boolean)
   */
  CompletableFuture<TableName[]> listTableNames();

  /**
   * List all of the names of userspace tables.
   * @param regex The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Pattern, boolean)
   */
  CompletableFuture<TableName[]> listTableNames(final String regex, final boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<TableName[]> listTableNames(final Pattern pattern,
      final boolean includeSysTables);

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a {@link TableName}
   * @return the tableDescriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor> getTableDescriptor(final TableName tableName);

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   */
  CompletableFuture<Void> createTable(HTableDescriptor desc);

  /**
   * Creates a new table with the specified number of regions. The start key specified will become
   * the end key of the first region of the table, and the end key specified will become the start
   * key of the last region of the table (the first region has a null start key and the last region
   * has a null end key). BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total regions.
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   */
  CompletableFuture<Void> createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions);

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one.
   * Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   */
  CompletableFuture<Void> createTable(final HTableDescriptor desc, byte[][] splitKeys);

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   */
  CompletableFuture<Void> deleteTable(final TableName tableName);

  /**
   * Deletes tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(String, boolean)} and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> deleteTables(String regex);

  /**
   * Delete tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(Pattern, boolean) } and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> deleteTables(Pattern pattern);

  /**
   * Truncate a table.
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   */
  CompletableFuture<Void> truncateTable(final TableName tableName, final boolean preserveSplits);

  /**
   * Enable a table. The table has to be in disabled state for it to be enabled.
   * @param tableName name of the table
   */
  CompletableFuture<Void> enableTable(final TableName tableName);

  /**
   * Enable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #enableTable(TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be enabled. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> enableTables(String regex);

  /**
   * Enable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #enableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be enabled. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> enableTables(Pattern pattern);

  /**
   * Disable a table. The table has to be in enabled state for it to be disabled.
   * @param tableName
   */
  CompletableFuture<Void> disableTable(final TableName tableName);

  /**
   * Disable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #disableTable(TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> disableTables(String regex);

  /**
   * Disable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #disableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> disableTables(Pattern pattern);

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   * @param tableName TableName instance
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   *         yet to be updated Pair.getSecond() is the total number of regions of the table. The
   *         return value will be wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Pair<Integer, Integer>> getAlterStatus(final TableName tableName);

  /**
   * Add a column family to an existing table.
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   */
  CompletableFuture<Void> addColumnFamily(final TableName tableName,
      final HColumnDescriptor columnFamily);

  /**
   * Delete a column family from a table.
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   */
  CompletableFuture<Void> deleteColumnFamily(final TableName tableName, final byte[] columnFamily);

  /**
   * Modify an existing column family on a table.
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   */
  CompletableFuture<Void> modifyColumnFamily(final TableName tableName,
      final HColumnDescriptor columnFamily);

  /**
   * Turn the load balancer on or off.
   * @param on
   * @return Previous balancer value wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> setBalancerRunning(final boolean on);

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. Can NOT run for various reasons. Check logs.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balancer();

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. If there is region in transition, force parameter of true would still run
   * balancer. Can *not* run for other reasons. Check logs.
   * @param force whether we should force balance even if there is region in transition.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balancer(boolean force);

  /**
   * Query the current state of the balancer.
   * @return true if the balancer is enabled, false otherwise.
   *         The return value will be wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isBalancerEnabled();
}
