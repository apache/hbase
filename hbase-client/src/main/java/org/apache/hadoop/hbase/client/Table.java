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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to communicate with a single HBase table.
 * Obtain an instance from a {@link Connection} and call {@link #close()} afterwards.
 *
 * <p><code>Table</code> can be used to get, put, delete or scan data from a table.
 * @see ConnectionFactory
 * @see Connection
 * @see Admin
 * @see RegionLocator
 * @since 0.99.0
 */
@InterfaceAudience.Public
public interface Table extends Closeable {
  /**
   * Gets the fully qualified table name instance of this table.
   */
  TableName getName();

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will
   * affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Gets the {@link org.apache.hadoop.hbase.HTableDescriptor table descriptor} for this table.
   * @throws java.io.IOException if a remote or network exception occurs.
   * @deprecated since 2.0 version and will be removed in 3.0 version.
   *             use {@link #getDescriptor()}
   */
  @Deprecated
  default HTableDescriptor getTableDescriptor() throws IOException {
    TableDescriptor descriptor = getDescriptor();

    if (descriptor instanceof HTableDescriptor) {
      return (HTableDescriptor)descriptor;
    } else {
      return new HTableDescriptor(descriptor);
    }
  }

  /**
   * Gets the {@link org.apache.hadoop.hbase.client.TableDescriptor table descriptor} for this table.
   * @throws java.io.IOException if a remote or network exception occurs.
   */
  TableDescriptor getDescriptor() throws IOException;

  /**
   * Gets the {@link RegionLocator} for this table.
   */
  RegionLocator getRegionLocator() throws IOException;

  /**
   * Test for the existence of columns in the table, as specified by the Get.
   * <p>
   *
   * This will return true if the Get matches one or more keys, false if not.
   * <p>
   *
   * This is a server-side call so it prevents any data from being transfered to
   * the client.
   *
   * @param get the Get
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException e
   */
  default boolean exists(Get get) throws IOException {
    return exists(Collections.singletonList(get))[0];
  }

  /**
   * Test for the existence of columns in the table, as specified by the Gets.
   * <p>
   *
   * This will return an array of booleans. Each value will be true if the related Get matches
   * one or more keys, false if not.
   * <p>
   *
   * This is a server-side call so it prevents any data from being transferred to
   * the client.
   *
   * @param gets the Gets
   * @return Array of boolean.  True if the specified Get matches one or more keys, false if not.
   * @throws IOException e
   */
  default boolean[] exists(List<Get> gets) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Test for the existence of columns in the table, as specified by the Gets.
   * This will return an array of booleans. Each value will be true if the related Get matches
   * one or more keys, false if not.
   * This is a server-side call so it prevents any data from being transferred to
   * the client.
   *
   * @param gets the Gets
   * @return Array of boolean.  True if the specified Get matches one or more keys, false if not.
   * @throws IOException e
   * @deprecated since 2.0 version and will be removed in 3.0 version.
   *             use {@link #exists(List)}
   */
  @Deprecated
  default boolean[] existsAll(List<Get> gets) throws IOException {
    return exists(gets);
  }

  /**
   * Method that does a batch call on Deletes, Gets, Puts, Increments, Appends, RowMutations.
   * The ordering of execution of the actions is not defined. Meaning if you do a Put and a
   * Get in the same {@link #batch} call, you will not necessarily be
   * guaranteed that the Get returns what the Put had put.
   *
   * @param actions list of Get, Put, Delete, Increment, Append, RowMutations.
   * @param results Empty Object[], same size as actions. Provides access to partial
   *                results, in case an exception is thrown. A null in the result array means that
   *                the call for that action failed, even after retries. The order of the objects
   *                in the results array corresponds to the order of actions in the request list.
   * @throws IOException
   * @since 0.90.0
   */
  default void batch(final List<? extends Row> actions, final Object[] results) throws IOException,
    InterruptedException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Same as {@link #batch(List, Object[])}, but with a callback.
   * @since 0.96.0
   */
  default <R> void batchCallback(
    final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Extracts certain cells from a given row.
   * @param get The object that specifies what data to fetch and from which row.
   * @return The data coming from the specified row, if it exists.  If the row
   *   specified doesn't exist, the {@link Result} instance returned won't
   *   contain any {@link org.apache.hadoop.hbase.KeyValue}, as indicated by
   *   {@link Result#isEmpty()}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default Result get(Get get) throws IOException {
    return get(Collections.singletonList(get))[0];
  }

  /**
   * Extracts specified cells from the given rows, as a batch.
   *
   * @param gets The objects that specify what data to fetch and from which rows.
   * @return The data coming from the specified rows, if it exists.  If the row specified doesn't
   *   exist, the {@link Result} instance returned won't contain any
   *   {@link org.apache.hadoop.hbase.Cell}s, as indicated by {@link Result#isEmpty()}. If there
   *   are any failures even after retries, there will be a <code>null</code> in the results' array
   *   for  those Gets, AND an exception will be thrown. The ordering of the Result array
   *   corresponds to  the order of the list of passed in Gets.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.90.0
   * @apiNote {@link #put(List)} runs pre-flight validations on the input list on client.
   *          Currently {@link #get(List)} doesn't run any validations on the client-side,
   *          currently there is no need, but this may change in the future. An
   *          {@link IllegalArgumentException} will be thrown in this case.
   */
  default Result[] get(List<Get> gets) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Returns a scanner on the current table as specified by the {@link Scan}
   * object.
   * Note that the passed {@link Scan}'s start row and caching properties
   * maybe changed.
   *
   * @param scan A configured {@link Scan} object.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default ResultScanner getScanner(Scan scan) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Gets a scanner on the current table for the given family.
   *
   * @param family The column family to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default ResultScanner getScanner(byte[] family) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   *
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Puts some data in the table.
   *
   * @param put The data to put.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default void put(Put put) throws IOException {
    put(Collections.singletonList(put));
  }

  /**
   * Batch puts the specified data into the table.
   * <p>
   * This can be used for group commit, or for submitting user defined batches. Before sending
   * a batch of mutations to the server, the client runs a few validations on the input list. If an
   * error is found, for example, a mutation was supplied but was missing it's column an
   * {@link IllegalArgumentException} will be thrown and no mutations will be applied. If there
   * are any failures even after retries, a {@link RetriesExhaustedWithDetailsException} will be
   * thrown. RetriesExhaustedWithDetailsException contains lists of failed mutations and
   * corresponding remote exceptions. The ordering of mutations and exceptions in the
   * encapsulating exception corresponds to the order of the input list of Put requests.
   *
   * @param puts The list of mutations to apply.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default void put(List<Put> puts) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    return checkAndPut(row, family, qualifier, CompareOperator.EQUAL, value, put);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> add the put.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
    RowMutations mutations = new RowMutations(put.getRow(), 1);
    mutations.add(put);

    return checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> add the put.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op comparison operator to use
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      byte[] value, Put put) throws IOException {
    RowMutations mutations = new RowMutations(put.getRow(), 1);
    mutations.add(put);

    return checkAndMutate(row, family, qualifier, op, value, mutations);
  }

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  default void delete(Delete delete) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Batch Deletes the specified cells/rows from the table.
   * <p>
   * If a specified row does not exist, {@link Delete} will report as though sucessful
   * delete; no exception will be thrown. If there are any failures even after retries,
   * a {@link RetriesExhaustedWithDetailsException} will be thrown.
   * RetriesExhaustedWithDetailsException contains lists of failed {@link Delete}s and
   * corresponding remote exceptions.
   *
   * @param deletes List of things to delete. The input list gets modified by this
   * method. All successfully applied {@link Delete}s in the list are removed (in particular it
   * gets re-ordered, so the order in which the elements are inserted in the list gives no
   * guarantee as to the order in which the {@link Delete}s are executed).
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code deletes} argument will contain the {@link Delete} instances
   * that have not be successfully applied.
   * @since 0.20.1
   * @apiNote In 3.0.0 version, the input list {@code deletes} will no longer be modified. Also,
   *          {@link #put(List)} runs pre-flight validations on the input list on client. Currently
   *          {@link #delete(List)} doesn't run validations on the client, there is no need
   *          currently, but this may change in the future. An * {@link IllegalArgumentException}
   *          will be thrown in this case.
   */
  default void delete(List<Delete> deletes) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
    byte[] value, Delete delete) throws IOException {
    return checkAndDelete(row, family, qualifier, CompareOperator.EQUAL, value, delete);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> add the delete.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
    CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
    RowMutations mutations = new RowMutations(delete.getRow(), 1);
    mutations.add(delete);

    return checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> add the delete.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op comparison operator to use
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                         CompareOperator op, byte[] value, Delete delete) throws IOException {
    RowMutations mutations = new RowMutations(delete.getRow(), 1);
    mutations.add(delete);

    return checkAndMutate(row, family, qualifier, op, value, mutations);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it
   * adds the Put/Delete/RowMutations.
   * <p>
   * Use the returned {@link CheckAndMutateBuilder} to construct your request and then execute it.
   * This is a fluent style API, the code is like:
   *
   * <pre>
   * <code>
   * table.checkAndMutate(row, family).qualifier(qualifier).ifNotExists().thenPut(put);
   * </code>
   * </pre>
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  default CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * A helper class for sending checkAndMutate request.
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  interface CheckAndMutateBuilder {

    /**
     * @param qualifier column qualifier to check.
     */
    CheckAndMutateBuilder qualifier(byte[] qualifier);

    /**
     * @param timeRange timeRange to check
     */
    CheckAndMutateBuilder timeRange(TimeRange timeRange);

    /**
     * Check for lack of column.
     */
    CheckAndMutateBuilder ifNotExists();

    /**
     * Check for equality.
     * @param value the expected value
     */
    default CheckAndMutateBuilder ifEquals(byte[] value) {
      return ifMatches(CompareOperator.EQUAL, value);
    }

    /**
     * @param compareOp comparison operator to use
     * @param value the expected value
     */
    CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value);

    /**
     * @param put data to put if check succeeds
     * @return {@code true} if the new put was executed, {@code false} otherwise.
     */
    boolean thenPut(Put put) throws IOException;

    /**
     * @param delete data to delete if check succeeds
     * @return {@code true} if the new delete was executed, {@code false} otherwise.
     */
    boolean thenDelete(Delete delete) throws IOException;

    /**
     * @param mutation mutations to perform if check succeeds
     * @return true if the new mutation was executed, false otherwise.
     */
    boolean thenMutate(RowMutations mutation) throws IOException;
  }

  /**
   * Atomically checks if a row matches the specified filter. If it does, it adds the
   * Put/Delete/RowMutations.
   * <p>
   * Use the returned {@link CheckAndMutateWithFilterBuilder} to construct your request and then
   * execute it. This is a fluent style API, the code is like:
   *
   * <pre>
   * <code>
   * table.checkAndMutate(row, filter).thenPut(put);
   * </code>
   * </pre>
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  default CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * A helper class for sending checkAndMutate request with a filter.
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  interface CheckAndMutateWithFilterBuilder {

    /**
     * @param timeRange timeRange to check
     */
    CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange);

    /**
     * @param put data to put if check succeeds
     * @return {@code true} if the new put was executed, {@code false} otherwise.
     */
    boolean thenPut(Put put) throws IOException;

    /**
     * @param delete data to delete if check succeeds
     * @return {@code true} if the new delete was executed, {@code false} otherwise.
     */
    boolean thenDelete(Delete delete) throws IOException;

    /**
     * @param mutation mutations to perform if check succeeds
     * @return true if the new mutation was executed, false otherwise.
     */
    boolean thenMutate(RowMutations mutation) throws IOException;
  }

  /**
   * checkAndMutate that atomically checks if a row matches the specified condition. If it does,
   * it performs the specified action.
   *
   * @param checkAndMutate The CheckAndMutate object.
   * @return A CheckAndMutateResult object that represents the result for the CheckAndMutate.
   * @throws IOException if a remote or network exception occurs.
   */
  default CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    return checkAndMutate(Collections.singletonList(checkAndMutate)).get(0);
  }

  /**
   * Batch version of checkAndMutate. The specified CheckAndMutates are batched only in the sense
   * that they are sent to a RS in one RPC, but each CheckAndMutate operation is still executed
   * atomically (and thus, each may fail independently of others).
   *
   * @param checkAndMutates The list of CheckAndMutate.
   * @return A list of CheckAndMutateResult objects that represents the result for each
   *   CheckAndMutate.
   * @throws IOException if a remote or network exception occurs.
   */
  default List<CheckAndMutateResult> checkAndMutate(List<CheckAndMutate> checkAndMutates)
    throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param rm object that specifies the set of mutations to perform atomically
   * @return results of Increment/Append operations
   * @throws IOException if a remote or network exception occurs.
   */
  default Result mutateRow(final RowMutations rm) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Appends values to one or more columns within a single row.
   * <p>
   * This operation guaranteed atomicity to readers. Appends are done
   * under a single row lock, so write operations to a row are synchronized, and
   * readers are guaranteed to see this operation fully completed.
   *
   * @param append object that specifies the columns and values to be appended
   * @throws IOException e
   * @return values of columns after the append operation (maybe null)
   */
  default Result append(final Append append) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Increments one or more columns within a single row.
   * <p>
   * This operation ensures atomicity to readers. Increments are done
   * under a single row lock, so write operations to a row are synchronized, and
   * readers are guaranteed to see this operation fully completed.
   *
   * @param increment object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the increment
   */
  default Result increment(final Increment increment) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * See {@link #incrementColumnValue(byte[], byte[], byte[], long, Durability)}
   * <p>
   * The {@link Durability} is defaulted to {@link Durability#SYNC_WAL}.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  default long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Increment increment = new Increment(row).addColumn(family, qualifier, amount);
    Cell cell = increment(increment).getColumnLatestCell(family, qualifier);
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception. If the column
   * value does not yet exist it is initialized to <code>amount</code> and
   * written to the specified column.
   *
   * <p>Setting durability to {@link Durability#SKIP_WAL} means that in a fail
   * scenario you will lose any increments that have not been flushed.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @param durability The persistence guarantee for this increment.
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  default long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
    long amount, Durability durability) throws IOException {
    Increment increment = new Increment(row)
        .addColumn(family, qualifier, amount)
        .setDurability(durability);
    Cell cell = increment(increment).getColumnLatestCell(family, qualifier);
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Releases any resources held or pending changes in internal buffers.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  @Override
  default void close() throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance connected to the
   * table region containing the specified row.  The row given does not actually have
   * to exist.  Whichever region would contain the row based on start and end keys will
   * be used.  Note that the {@code row} parameter is also not passed to the
   * coprocessor handler registered for this protocol, unless the {@code row}
   * is separately passed as an argument in the service request.  The parameter
   * here is only used to locate the region used to handle the call.
   *
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   *
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myTable.coprocessorService(rowkey);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @param row The row key used to identify the remote region location
   * @return A CoprocessorRpcChannel instance
   */
  default CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), and
   * invokes the passed {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * with each {@link com.google.protobuf.Service} instance.
   *
   * @param service the protocol buffer {@code Service} implementation to call
   * @param startKey start region selection with region containing this row.  If {@code null}, the
   *   selection will start with the first table region.
   * @param endKey select regions up to and including the region containing this row. If
   *   {@code null}, selection will continue through the last table region.
   * @param callable this instance's
   *   {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call}
   *   method will be invoked once per table region, using the {@link com.google.protobuf.Service}
   *   instance connected to that region.
   * @param <T> the {@link com.google.protobuf.Service} subclass to connect to
   * @param <R> Return type for the {@code callable} parameter's {@link
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * @return a map of result values keyed by region name
   */
  default <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
    byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
    throws ServiceException, Throwable {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), and
   * invokes the passed {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * with each {@link Service} instance.
   *
   * <p> The given
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[],byte[],Object)}
   * method will be called with the return value from each region's
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} invocation. </p>
   *
   * @param service the protocol buffer {@code Service} implementation to call
   * @param startKey start region selection with region containing this row.  If {@code null}, the
   *   selection will start with the first table region.
   * @param endKey select regions up to and including the region containing this row. If
   *   {@code null}, selection will continue through the last table region.
   * @param callable this instance's
   *   {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call}
   *   method will be invoked once per table region, using the {@link Service} instance connected to
   *   that region.
   * @param <T> the {@link Service} subclass to connect to
   * @param <R> Return type for the {@code callable} parameter's {@link
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   */
  default <T extends Service, R> void coprocessorService(final Class<T> service,
    byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
    final Batch.Callback<R> callback) throws ServiceException, Throwable {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), all
   * the invocations to the same region server will be batched into one call. The coprocessor
   * service is invoked according to the service instance, method name and parameters.
   *
   * @param methodDescriptor
   *          the descriptor for the protobuf service method to call.
   * @param request
   *          the method call parameters
   * @param startKey
   *          start region selection with region containing this row. If {@code null}, the
   *          selection will start with the first table region.
   * @param endKey
   *          select regions up to and including the region containing this row. If {@code null},
   *          selection will continue through the last table region.
   * @param responsePrototype
   *          the proto type of the response of the method in Service.
   * @param <R>
   *          the response type for the coprocessor Service method
   * @return a map of result values keyed by region name
   */
  default <R extends Message> Map<byte[], R> batchCoprocessorService(
    Descriptors.MethodDescriptor methodDescriptor, Message request,
    byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), all
   * the invocations to the same region server will be batched into one call. The coprocessor
   * service is invoked according to the service instance, method name and parameters.
   *
   * <p>
   * The given
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[],byte[],Object)}
   * method will be called with the return value from each region's invocation.
   * </p>
   *
   * @param methodDescriptor the descriptor for the protobuf service method to call.
   * @param request the method call parameters
   * @param startKey start region selection with region containing this row.
   *   If {@code null}, the selection will start with the first table region.
   * @param endKey select regions up to and including the region containing this row.
   *   If {@code null}, selection will continue through the last table region.
   * @param responsePrototype the proto type of the response of the method in Service.
   * @param callback callback to invoke with the response for each region
   * @param <R>
   *          the response type for the coprocessor Service method
   */
  default <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey,
      byte[] endKey, R responsePrototype, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value.
   * If it does, it performs the row mutations.  If the passed value is null, the check
   * is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> perform row mutations.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param value the expected value
   * @param mutation  mutations to perform if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value.
   * If it does, it performs the row mutations.  If the passed value is null, the check
   * is for the lack of column (ie: non-existence)
   *
   * The expected value argument of this call is on the left and the current
   * value of the cell is on the right side of the comparison operator.
   *
   * Ie. eg. GREATER operator means expected value > existing <=> perform row mutations.
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op the comparison operator
   * @param value the expected value
   * @param mutation  mutations to perform if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use {@link #checkAndMutate(byte[], byte[])}
   */
  @Deprecated
  default boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
                         byte[] value, RowMutations mutation) throws IOException {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout of each rpc request in this Table instance. It will be overridden by a more
   * specific rpc timeout config such as readRpcTimeout or writeRpcTimeout.
   * @see #getReadRpcTimeout(TimeUnit)
   * @see #getWriteRpcTimeout(TimeUnit)
   * @param unit the unit of time the timeout to be represented in
   * @return rpc timeout in the specified time unit
   */
  default long getRpcTimeout(TimeUnit unit) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout (millisecond) of each rpc request in this Table instance.
   *
   * @return Currently configured read timeout
   * @deprecated use {@link #getReadRpcTimeout(TimeUnit)} or
   *             {@link #getWriteRpcTimeout(TimeUnit)} instead
   */
  @Deprecated
  default int getRpcTimeout() {
    return (int)getRpcTimeout(TimeUnit.MILLISECONDS);
  }

  /**
   * Set timeout (millisecond) of each rpc request in operations of this Table instance, will
   * override the value of hbase.rpc.timeout in configuration.
   * If a rpc request waiting too long, it will stop waiting and send a new request to retry until
   * retries exhausted or operation timeout reached.
   * <p>
   * NOTE: This will set both the read and write timeout settings to the provided value.
   *
   * @param rpcTimeout the timeout of each rpc request in millisecond.
   *
   * @deprecated Use setReadRpcTimeout or setWriteRpcTimeout instead
   */
  @Deprecated
  default void setRpcTimeout(int rpcTimeout) {
    setReadRpcTimeout(rpcTimeout);
    setWriteRpcTimeout(rpcTimeout);
  }

  /**
   * Get timeout of each rpc read request in this Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return read rpc timeout in the specified time unit
   */
  default long getReadRpcTimeout(TimeUnit unit) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout (millisecond) of each rpc read request in this Table instance.
   * @deprecated since 2.0 and will be removed in 3.0 version
   *             use {@link #getReadRpcTimeout(TimeUnit)} instead
   */
  @Deprecated
  default int getReadRpcTimeout() {
    return (int)getReadRpcTimeout(TimeUnit.MILLISECONDS);
  }

  /**
   * Set timeout (millisecond) of each rpc read request in operations of this Table instance, will
   * override the value of hbase.rpc.read.timeout in configuration.
   * If a rpc read request waiting too long, it will stop waiting and send a new request to retry
   * until retries exhausted or operation timeout reached.
   *
   * @param readRpcTimeout the timeout for read rpc request in milliseconds
   * @deprecated since 2.0.0, use {@link TableBuilder#setReadRpcTimeout} instead
   */
  @Deprecated
  default void setReadRpcTimeout(int readRpcTimeout) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout of each rpc write request in this Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return write rpc timeout in the specified time unit
   */
  default long getWriteRpcTimeout(TimeUnit unit) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout (millisecond) of each rpc write request in this Table instance.
   * @deprecated since 2.0 and will be removed in 3.0 version
   *             use {@link #getWriteRpcTimeout(TimeUnit)} instead
   */
  @Deprecated
  default int getWriteRpcTimeout() {
    return (int)getWriteRpcTimeout(TimeUnit.MILLISECONDS);
  }

  /**
   * Set timeout (millisecond) of each rpc write request in operations of this Table instance, will
   * override the value of hbase.rpc.write.timeout in configuration.
   * If a rpc write request waiting too long, it will stop waiting and send a new request to retry
   * until retries exhausted or operation timeout reached.
   *
   * @param writeRpcTimeout the timeout for write rpc request in milliseconds
   * @deprecated since 2.0.0, use {@link TableBuilder#setWriteRpcTimeout} instead
   */
  @Deprecated
  default void setWriteRpcTimeout(int writeRpcTimeout) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout of each operation in Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return operation rpc timeout in the specified time unit
   */
  default long getOperationTimeout(TimeUnit unit) {
    throw new NotImplementedException("Add an implementation!");
  }

  /**
   * Get timeout (millisecond) of each operation for in Table instance.
   * @deprecated since 2.0 and will be removed in 3.0 version
   *             use {@link #getOperationTimeout(TimeUnit)} instead
   */
  @Deprecated
  default int getOperationTimeout() {
    return (int)getOperationTimeout(TimeUnit.MILLISECONDS);
  }

  /**
   * Set timeout (millisecond) of each operation in this Table instance, will override the value
   * of hbase.client.operation.timeout in configuration.
   * Operation timeout is a top-level restriction that makes sure a blocking method will not be
   * blocked more than this. In each operation, if rpc request fails because of timeout or
   * other reason, it will retry until success or throw a RetriesExhaustedException. But if the
   * total time being blocking reach the operation timeout before retries exhausted, it will break
   * early and throw SocketTimeoutException.
   * @param operationTimeout the total timeout of each operation in millisecond.
   * @deprecated since 2.0.0, use {@link TableBuilder#setOperationTimeout} instead
   */
  @Deprecated
  default void setOperationTimeout(int operationTimeout) {
    throw new NotImplementedException("Add an implementation!");
  }
}
