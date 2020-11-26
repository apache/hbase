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

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.ConnectionUtils.allOf;
import static org.apache.hadoop.hbase.client.ConnectionUtils.toCheckExistenceOnly;

import com.google.protobuf.RpcChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The interface for asynchronous version of Table. Obtain an instance from a
 * {@link AsyncConnection}.
 * <p>
 * The implementation is required to be thread safe.
 * <p>
 * Usually the implementation will not throw any exception directly. You need to get the exception
 * from the returned {@link CompletableFuture}.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncTable<C extends ScanResultConsumerBase> {

  /**
   * Gets the fully qualified table name instance of this table.
   */
  TableName getName();

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Gets the {@link TableDescriptor} for this table.
   */
  CompletableFuture<TableDescriptor> getDescriptor();

  /**
   * Gets the {@link AsyncTableRegionLocator} for this table.
   */
  AsyncTableRegionLocator getRegionLocator();
  /**
   * Get timeout of each rpc request in this Table instance. It will be overridden by a more
   * specific rpc timeout config such as readRpcTimeout or writeRpcTimeout.
   * @see #getReadRpcTimeout(TimeUnit)
   * @see #getWriteRpcTimeout(TimeUnit)
   * @param unit the unit of time the timeout to be represented in
   * @return rpc timeout in the specified time unit
   */
  long getRpcTimeout(TimeUnit unit);

  /**
   * Get timeout of each rpc read request in this Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return read rpc timeout in the specified time unit
   */
  long getReadRpcTimeout(TimeUnit unit);

  /**
   * Get timeout of each rpc write request in this Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return write rpc timeout in the specified time unit
   */
  long getWriteRpcTimeout(TimeUnit unit);

  /**
   * Get timeout of each operation in Table instance.
   * @param unit the unit of time the timeout to be represented in
   * @return operation rpc timeout in the specified time unit
   */
  long getOperationTimeout(TimeUnit unit);

  /**
   * Get the timeout of a single operation in a scan. It works like operation timeout for other
   * operations.
   * @param unit the unit of time the timeout to be represented in
   * @return scan rpc timeout in the specified time unit
   */
  long getScanTimeout(TimeUnit unit);

  /**
   * Test for the existence of columns in the table, as specified by the Get.
   * <p>
   * This will return true if the Get matches one or more keys, false if not.
   * <p>
   * This is a server-side call so it prevents any data from being transfered to the client.
   * @return true if the specified Get matches one or more keys, false if not. The return value will
   *         be wrapped by a {@link CompletableFuture}.
   */
  default CompletableFuture<Boolean> exists(Get get) {
    return get(toCheckExistenceOnly(get)).thenApply(r -> r.getExists());
  }

  /**
   * Extracts certain cells from a given row.
   * @param get The object that specifies what data to fetch and from which row.
   * @return The data coming from the specified row, if it exists. If the row specified doesn't
   *         exist, the {@link Result} instance returned won't contain any
   *         {@link org.apache.hadoop.hbase.KeyValue}, as indicated by {@link Result#isEmpty()}. The
   *         return value will be wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Result> get(Get get);

  /**
   * Puts some data to the table.
   * @param put The data to put.
   * @return A {@link CompletableFuture} that always returns null when complete normally.
   */
  CompletableFuture<Void> put(Put put);

  /**
   * Deletes the specified cells/row.
   * @param delete The object that specifies what to delete.
   * @return A {@link CompletableFuture} that always returns null when complete normally.
   */
  CompletableFuture<Void> delete(Delete delete);

  /**
   * Appends values to one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers. Appends are done under a single row lock, so
   * write operations to a row are synchronized, but readers do not take row locks so get and scan
   * operations can see this operation partially completed.
   * @param append object that specifies the columns and amounts to be used for the increment
   *          operations
   * @return values of columns after the append operation (maybe null). The return value will be
   *         wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Result> append(Append append);

  /**
   * Increments one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers. Increments are done under a single row lock,
   * so write operations to a row are synchronized, but readers do not take row locks so get and
   * scan operations can see this operation partially completed.
   * @param increment object that specifies the columns and amounts to be used for the increment
   *          operations
   * @return values of columns after the increment. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Result> increment(Increment increment);

  /**
   * See {@link #incrementColumnValue(byte[], byte[], byte[], long, Durability)}
   * <p>
   * The {@link Durability} is defaulted to {@link Durability#SYNC_WAL}.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the amount is negative).
   * @return The new value, post increment. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  default CompletableFuture<Long> incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) {
    return incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
  }

  /**
   * Atomically increments a column value. If the column value already exists and is not a
   * big-endian long, this could throw an exception. If the column value does not yet exist it is
   * initialized to <code>amount</code> and written to the specified column.
   * <p>
   * Setting durability to {@link Durability#SKIP_WAL} means that in a fail scenario you will lose
   * any increments that have not been flushed.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the amount is negative).
   * @param durability The persistence guarantee for this increment.
   * @return The new value, post increment. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  default CompletableFuture<Long> incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, Durability durability) {
    Preconditions.checkNotNull(row, "row is null");
    Preconditions.checkNotNull(family, "family is null");
    return increment(
      new Increment(row).addColumn(family, qualifier, amount).setDurability(durability))
          .thenApply(r -> Bytes.toLong(r.getValue(family, qualifier)));
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
   * table.checkAndMutate(row, family).qualifier(qualifier).ifNotExists().thenPut(put)
   *     .thenAccept(succ -> {
   *       if (succ) {
   *         System.out.println("Check and put succeeded");
   *       } else {
   *         System.out.println("Check and put failed");
   *       }
   *     });
   * </code>
   * </pre>
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family);

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
     * @param timeRange time range to check.
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
     * @return {@code true} if the new put was executed, {@code false} otherwise. The return value
     *         will be wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenPut(Put put);

    /**
     * @param delete data to delete if check succeeds
     * @return {@code true} if the new delete was executed, {@code false} otherwise. The return
     *         value will be wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenDelete(Delete delete);

    /**
     * @param mutation mutations to perform if check succeeds
     * @return true if the new mutation was executed, false otherwise. The return value will be
     *         wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenMutate(RowMutations mutation);
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
   * table.checkAndMutate(row, filter).thenPut(put)
   *     .thenAccept(succ -> {
   *       if (succ) {
   *         System.out.println("Check and put succeeded");
   *       } else {
   *         System.out.println("Check and put failed");
   *       }
   *     });
   * </code>
   * </pre>
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter);

  /**
   * A helper class for sending checkAndMutate request with a filter.
   *
   * @deprecated Since 2.4.0, will be removed in 4.0.0. For internal test use only, do not use it
   *   any more.
   */
  @Deprecated
  interface CheckAndMutateWithFilterBuilder {

    /**
     * @param timeRange time range to check.
     */
    CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange);

    /**
     * @param put data to put if check succeeds
     * @return {@code true} if the new put was executed, {@code false} otherwise. The return value
     *         will be wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenPut(Put put);

    /**
     * @param delete data to delete if check succeeds
     * @return {@code true} if the new delete was executed, {@code false} otherwise. The return
     *         value will be wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenDelete(Delete delete);

    /**
     * @param mutation mutations to perform if check succeeds
     * @return true if the new mutation was executed, false otherwise. The return value will be
     *         wrapped by a {@link CompletableFuture}.
     */
    CompletableFuture<Boolean> thenMutate(RowMutations mutation);
  }

  /**
   * checkAndMutate that atomically checks if a row matches the specified condition. If it does,
   * it performs the specified action.
   *
   * @param checkAndMutate The CheckAndMutate object.
   * @return A {@link CompletableFuture}s that represent the result for the CheckAndMutate.
   */
  CompletableFuture<CheckAndMutateResult> checkAndMutate(CheckAndMutate checkAndMutate);

  /**
   * Batch version of checkAndMutate. The specified CheckAndMutates are batched only in the sense
   * that they are sent to a RS in one RPC, but each CheckAndMutate operation is still executed
   * atomically (and thus, each may fail independently of others).
   *
   * @param checkAndMutates The list of CheckAndMutate.
   * @return A list of {@link CompletableFuture}s that represent the result for each
   *   CheckAndMutate.
   */
  List<CompletableFuture<CheckAndMutateResult>> checkAndMutate(
    List<CheckAndMutate> checkAndMutates);

  /**
   * A simple version of batch checkAndMutate. It will fail if there are any failures.
   *
   * @param checkAndMutates The list of rows to apply.
   * @return A {@link CompletableFuture} that wrapper the result list.
   */
  default CompletableFuture<List<CheckAndMutateResult>> checkAndMutateAll(
    List<CheckAndMutate> checkAndMutates) {
    return allOf(checkAndMutate(checkAndMutates));
  }

  /**
   * Performs multiple mutations atomically on a single row. Currently {@link Put} and
   * {@link Delete} are supported.
   * @param mutation object that specifies the set of mutations to perform atomically
   * @return A {@link CompletableFuture} that returns results of Increment/Append operations
   */
  CompletableFuture<Result> mutateRow(RowMutations mutation);

  /**
   * The scan API uses the observer pattern.
   * @param scan A configured {@link Scan} object.
   * @param consumer the consumer used to receive results.
   * @see ScanResultConsumer
   * @see AdvancedScanResultConsumer
   */
  void scan(Scan scan, C consumer);

  /**
   * Gets a scanner on the current table for the given family.
   * @param family The column family to scan.
   * @return A scanner.
   */
  default ResultScanner getScanner(byte[] family) {
    return getScanner(new Scan().addFamily(family));
  }

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   */
  default ResultScanner getScanner(byte[] family, byte[] qualifier) {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  /**
   * Returns a scanner on the current table as specified by the {@link Scan} object.
   * @param scan A configured {@link Scan} object.
   * @return A scanner.
   */
  ResultScanner getScanner(Scan scan);

  /**
   * Return all the results that match the given scan object.
   * <p>
   * Notice that usually you should use this method with a {@link Scan} object that has limit set.
   * For example, if you want to get the closest row after a given row, you could do this:
   * <p>
   *
   * <pre>
   * <code>
   * table.scanAll(new Scan().withStartRow(row, false).setLimit(1)).thenAccept(results -> {
   *   if (results.isEmpty()) {
   *      System.out.println("No row after " + Bytes.toStringBinary(row));
   *   } else {
   *     System.out.println("The closest row after " + Bytes.toStringBinary(row) + " is "
   *         + Bytes.toStringBinary(results.stream().findFirst().get().getRow()));
   *   }
   * });
   * </code>
   * </pre>
   * <p>
   * If your result set is very large, you should use other scan method to get a scanner or use
   * callback to process the results. They will do chunking to prevent OOM. The scanAll method will
   * fetch all the results and store them in a List and then return the list to you.
   * <p>
   * The scan metrics will be collected background if you enable it but you have no way to get it.
   * Usually you can get scan metrics from {@code ResultScanner}, or through
   * {@code ScanResultConsumer.onScanMetricsCreated} but this method only returns a list of results.
   * So if you really care about scan metrics then you'd better use other scan methods which return
   * a {@code ResultScanner} or let you pass in a {@code ScanResultConsumer}. There is no
   * performance difference between these scan methods so do not worry.
   * @param scan A configured {@link Scan} object. So if you use this method to fetch a really large
   *          result set, it is likely to cause OOM.
   * @return The results of this small scan operation. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<Result>> scanAll(Scan scan);

  /**
   * Test for the existence of columns in the table, as specified by the Gets.
   * <p>
   * This will return a list of booleans. Each value will be true if the related Get matches one or
   * more keys, false if not.
   * <p>
   * This is a server-side call so it prevents any data from being transferred to the client.
   * @param gets the Gets
   * @return A list of {@link CompletableFuture}s that represent the existence for each get.
   */
  default List<CompletableFuture<Boolean>> exists(List<Get> gets) {
    return get(toCheckExistenceOnly(gets)).stream()
        .<CompletableFuture<Boolean>> map(f -> f.thenApply(r -> r.getExists())).collect(toList());
  }

  /**
   * A simple version for batch exists. It will fail if there are any failures and you will get the
   * whole result boolean list at once if the operation is succeeded.
   * @param gets the Gets
   * @return A {@link CompletableFuture} that wrapper the result boolean list.
   */
  default CompletableFuture<List<Boolean>> existsAll(List<Get> gets) {
    return allOf(exists(gets));
  }

  /**
   * Extracts certain cells from the given rows, in batch.
   * <p>
   * Notice that you may not get all the results with this function, which means some of the
   * returned {@link CompletableFuture}s may succeed while some of the other returned
   * {@link CompletableFuture}s may fail.
   * @param gets The objects that specify what data to fetch and from which rows.
   * @return A list of {@link CompletableFuture}s that represent the result for each get.
   */
  List<CompletableFuture<Result>> get(List<Get> gets);

  /**
   * A simple version for batch get. It will fail if there are any failures and you will get the
   * whole result list at once if the operation is succeeded.
   * @param gets The objects that specify what data to fetch and from which rows.
   * @return A {@link CompletableFuture} that wrapper the result list.
   */
  default CompletableFuture<List<Result>> getAll(List<Get> gets) {
    return allOf(get(gets));
  }

  /**
   * Puts some data in the table, in batch.
   * @param puts The list of mutations to apply.
   * @return A list of {@link CompletableFuture}s that represent the result for each put.
   */
  List<CompletableFuture<Void>> put(List<Put> puts);

  /**
   * A simple version of batch put. It will fail if there are any failures.
   * @param puts The list of mutations to apply.
   * @return A {@link CompletableFuture} that always returns null when complete normally.
   */
  default CompletableFuture<Void> putAll(List<Put> puts) {
    return allOf(put(puts)).thenApply(r -> null);
  }

  /**
   * Deletes the specified cells/rows in bulk.
   * @param deletes list of things to delete.
   * @return A list of {@link CompletableFuture}s that represent the result for each delete.
   */
  List<CompletableFuture<Void>> delete(List<Delete> deletes);

  /**
   * A simple version of batch delete. It will fail if there are any failures.
   * @param deletes list of things to delete.
   * @return A {@link CompletableFuture} that always returns null when complete normally.
   */
  default CompletableFuture<Void> deleteAll(List<Delete> deletes) {
    return allOf(delete(deletes)).thenApply(r -> null);
  }

  /**
   * Method that does a batch call on Deletes, Gets, Puts, Increments, Appends and RowMutations. The
   * ordering of execution of the actions is not defined. Meaning if you do a Put and a Get in the
   * same {@link #batch} call, you will not necessarily be guaranteed that the Get returns what the
   * Put had put.
   * @param actions list of Get, Put, Delete, Increment, Append, and RowMutations objects
   * @return A list of {@link CompletableFuture}s that represent the result for each action.
   */
  <T> List<CompletableFuture<T>> batch(List<? extends Row> actions);

  /**
   * A simple version of batch. It will fail if there are any failures and you will get the whole
   * result list at once if the operation is succeeded.
   * @param actions list of Get, Put, Delete, Increment, Append and RowMutations objects
   * @return A list of the result for the actions. Wrapped by a {@link CompletableFuture}.
   */
  default <T> CompletableFuture<List<T>> batchAll(List<? extends Row> actions) {
    return allOf(batch(actions));
  }

  /**
   * Execute the given coprocessor call on the region which contains the given {@code row}.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code newStub} call. Usually it is only a
   * one line lambda expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   *
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link ServiceCaller} for more details.
   * @param row The row key used to identify the remote region location
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see ServiceCaller
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, byte[] row);

  /**
   * The callback when we want to execute a coprocessor call on a range of regions.
   * <p>
   * As the locating itself also takes some time, the implementation may want to send rpc calls on
   * the fly, which means we do not know how many regions we have when we get the return value of
   * the rpc calls, so we need an {@link #onComplete()} which is used to tell you that we have
   * passed all the return values to you(through the {@link #onRegionComplete(RegionInfo, Object)}
   * or {@link #onRegionError(RegionInfo, Throwable)} calls), i.e, there will be no
   * {@link #onRegionComplete(RegionInfo, Object)} or {@link #onRegionError(RegionInfo, Throwable)}
   * calls in the future.
   * <p>
   * Here is a pseudo code to describe a typical implementation of a range coprocessor service
   * method to help you better understand how the {@link CoprocessorCallback} will be called. The
   * {@code callback} in the pseudo code is our {@link CoprocessorCallback}. And notice that the
   * {@code whenComplete} is {@code CompletableFuture.whenComplete}.
   *
   * <pre>
   * locateThenCall(byte[] row) {
   *   locate(row).whenComplete((location, locateError) -> {
   *     if (locateError != null) {
   *       callback.onError(locateError);
   *       return;
   *     }
   *     incPendingCall();
   *     region = location.getRegion();
   *     if (region.getEndKey() > endKey) {
   *       locateEnd = true;
   *     } else {
   *       locateThenCall(region.getEndKey());
   *     }
   *     sendCall().whenComplete((resp, error) -> {
   *       if (error != null) {
   *         callback.onRegionError(region, error);
   *       } else {
   *         callback.onRegionComplete(region, resp);
   *       }
   *       if (locateEnd && decPendingCallAndGet() == 0) {
   *         callback.onComplete();
   *       }
   *     });
   *   });
   * }
   * </pre>
   */
  @InterfaceAudience.Public
  interface CoprocessorCallback<R> {

    /**
     * @param region the region that the response belongs to
     * @param resp the response of the coprocessor call
     */
    void onRegionComplete(RegionInfo region, R resp);

    /**
     * @param region the region that the error belongs to
     * @param error the response error of the coprocessor call
     */
    void onRegionError(RegionInfo region, Throwable error);

    /**
     * Indicate that all responses of the regions have been notified by calling
     * {@link #onRegionComplete(RegionInfo, Object)} or
     * {@link #onRegionError(RegionInfo, Throwable)}.
     */
    void onComplete();

    /**
     * Indicate that we got an error which does not belong to any regions. Usually a locating error.
     */
    void onError(Throwable error);
  }

  /**
   * Helper class for sending coprocessorService request that executes a coprocessor call on regions
   * which are covered by a range.
   * <p>
   * If {@code fromRow} is not specified the selection will start with the first table region. If
   * {@code toRow} is not specified the selection will continue through the last table region.
   * @param <S> the type of the protobuf Service you want to call.
   * @param <R> the type of the return value.
   */
  interface CoprocessorServiceBuilder<S, R> {

    /**
     * @param startKey start region selection with region containing this row, inclusive.
     */
    default CoprocessorServiceBuilder<S, R> fromRow(byte[] startKey) {
      return fromRow(startKey, true);
    }

    /**
     * @param startKey start region selection with region containing this row
     * @param inclusive whether to include the startKey
     */
    CoprocessorServiceBuilder<S, R> fromRow(byte[] startKey, boolean inclusive);

    /**
     * @param endKey select regions up to and including the region containing this row, exclusive.
     */
    default CoprocessorServiceBuilder<S, R> toRow(byte[] endKey) {
      return toRow(endKey, false);
    }

    /**
     * @param endKey select regions up to and including the region containing this row
     * @param inclusive whether to include the endKey
     */
    CoprocessorServiceBuilder<S, R> toRow(byte[] endKey, boolean inclusive);

    /**
     * Execute the coprocessorService request. You can get the response through the
     * {@link CoprocessorCallback}.
     */
    void execute();
  }

  /**
   * Execute a coprocessor call on the regions which are covered by a range.
   * <p>
   * Use the returned {@link CoprocessorServiceBuilder} construct your request and then execute it.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code xxxService.newStub} call. Usually it
   * is only a one line lambda expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   *
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link ServiceCaller} for more details.
   * @param callback callback to get the response. See the comment of {@link CoprocessorCallback}
   *          for more details.
   */
  <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, CoprocessorCallback<R> callback);
}
