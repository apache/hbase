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

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * The base interface for asynchronous version of Table. Obtain an instance from a
 * {@link AsyncConnection}.
 * <p>
 * The implementation is NOT required to be thread safe. Do NOT access it from multiple threads
 * concurrently.
 * <p>
 * Usually the implementations will not throw any exception directly, you need to get the exception
 * from the returned {@link CompletableFuture}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncTableBase {

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
   * Set timeout of each rpc read request in operations of this Table instance, will override the
   * value of {@code hbase.rpc.read.timeout} in configuration. If a rpc read request waiting too
   * long, it will stop waiting and send a new request to retry until retries exhausted or operation
   * timeout reached.
   */
  void setReadRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Get timeout of each rpc read request in this Table instance.
   */
  long getReadRpcTimeout(TimeUnit unit);

  /**
   * Set timeout of each rpc write request in operations of this Table instance, will override the
   * value of {@code hbase.rpc.write.timeout} in configuration. If a rpc write request waiting too
   * long, it will stop waiting and send a new request to retry until retries exhausted or operation
   * timeout reached.
   */
  void setWriteRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Get timeout of each rpc write request in this Table instance.
   */
  long getWriteRpcTimeout(TimeUnit unit);

  /**
   * Set timeout of each operation in this Table instance, will override the value of
   * {@code hbase.client.operation.timeout} in configuration.
   * <p>
   * Operation timeout is a top-level restriction that makes sure an operation will not be blocked
   * more than this. In each operation, if rpc request fails because of timeout or other reason, it
   * will retry until success or throw a RetriesExhaustedException. But if the total time elapsed
   * reach the operation timeout before retries exhausted, it will break early and throw
   * SocketTimeoutException.
   */
  void setOperationTimeout(long timeout, TimeUnit unit);

  /**
   * Get timeout of each operation in Table instance.
   */
  long getOperationTimeout(TimeUnit unit);

  /**
   * Set timeout of a single operation in a scan, such as openScanner and next. Will override the
   * value {@code hbase.client.scanner.timeout.period} in configuration.
   * <p>
   * Generally a scan will never timeout after we add heartbeat support unless the region is
   * crashed. The {@code scanTimeout} works like the {@code operationTimeout} for each single
   * operation in a scan.
   */
  void setScanTimeout(long timeout, TimeUnit unit);

  /**
   * Get the timeout of a single operation in a scan.
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
    if (!get.isCheckExistenceOnly()) {
      get = ReflectionUtils.newInstance(get.getClass(), get);
      get.setCheckExistenceOnly(true);
    }
    return get(get).thenApply(r -> r.getExists());
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
    Preconditions.checkNotNull(qualifier, "qualifier is null");
    return increment(
      new Increment(row).addColumn(family, qualifier, amount).setDurability(durability))
          .thenApply(r -> Bytes.toLong(r.getValue(family, qualifier)));
  }

  /**
   * Atomically checks if a row/family/qualifier value equals to the expected value. If it does, it
   * adds the put. If the passed value is null, the check is for the lack of column (ie:
   * non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param put data to put if check succeeds
   * @return true if the new put was executed, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}.
   */
  default CompletableFuture<Boolean> checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) {
    return checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it
   * adds the put. If the passed value is null, the check is for the lack of column (ie:
   * non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param put data to put if check succeeds
   * @return true if the new put was executed, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put);

  /**
   * Atomically checks if a row/family/qualifier value equals to the expected value. If it does, it
   * adds the delete. If the passed value is null, the check is for the lack of column (ie:
   * non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @return true if the new delete was executed, false otherwise. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  default CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) {
    return checkAndDelete(row, family, qualifier, CompareOp.EQUAL, value, delete);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it
   * adds the delete. If the passed value is null, the check is for the lack of column (ie:
   * non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @return true if the new delete was executed, false otherwise. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete);

  /**
   * Performs multiple mutations atomically on a single row. Currently {@link Put} and
   * {@link Delete} are supported.
   * @param mutation object that specifies the set of mutations to perform atomically
   * @return A {@link CompletableFuture} that always returns null when complete normally.
   */
  CompletableFuture<Void> mutateRow(RowMutations mutation);

  /**
   * Atomically checks if a row/family/qualifier value equals to the expected value. If it does, it
   * performs the row mutations. If the passed value is null, the check is for the lack of column
   * (ie: non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param mutation mutations to perform if check succeeds
   * @return true if the new put was executed, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}.
   */
  default CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, RowMutations mutation) {
    return checkAndMutate(row, family, qualifier, CompareOp.EQUAL, value, mutation);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it
   * performs the row mutations. If the passed value is null, the check is for the lack of column
   * (ie: non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param value the expected value
   * @param mutation mutations to perform if check succeeds
   * @return true if the new put was executed, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations mutation);

  /**
   * Just call {@link #smallScan(Scan, int)} with {@link Integer#MAX_VALUE}.
   * @see #smallScan(Scan, int)
   */
  default CompletableFuture<List<Result>> smallScan(Scan scan) {
    return smallScan(scan, Integer.MAX_VALUE);
  }

  /**
   * Return all the results that match the given scan object. The number of the returned results
   * will not be greater than {@code limit}.
   * <p>
   * Notice that the scan must be small, and should not use batch or allowPartialResults. The
   * {@code caching} property of the scan object is also ignored as we will use {@code limit}
   * instead.
   * @param scan A configured {@link Scan} object.
   * @param limit the limit of results count
   * @return The results of this small scan operation. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<Result>> smallScan(Scan scan, int limit);
}
