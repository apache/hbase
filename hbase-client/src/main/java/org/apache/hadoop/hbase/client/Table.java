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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * Used to communicate with a single HBase table.
 * Obtain an instance from a {@link Connection} and call {@link #close()} afterwards.
 *
 * <p>Table can be used to get, put, delete or scan data from a table.
 * @see ConnectionFactory
 * @see Connection
 * @see Admin
 * @see RegionLocator
 * @since 0.99.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
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
   */
  HTableDescriptor getTableDescriptor() throws IOException;

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
  boolean exists(Get get) throws IOException;

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
  boolean[] existsAll(List<Get> gets) throws IOException;

  /**
   * Method that does a batch call on Deletes, Gets, Puts, Increments and Appends.
   * The ordering of execution of the actions is not defined. Meaning if you do a Put and a
   * Get in the same {@link #batch} call, you will not necessarily be
   * guaranteed that the Get returns what the Put had put.
   *
   * @param actions list of Get, Put, Delete, Increment, Append objects
   * @param results Empty Object[], same size as actions. Provides access to partial
   *                results, in case an exception is thrown. A null in the result array means that
   *                the call for that action failed, even after retries
   * @throws IOException
   * @since 0.90.0
   */
  void batch(final List<? extends Row> actions, final Object[] results) throws IOException,
    InterruptedException;

  /**
   * Same as {@link #batch(List, Object[])}, but with a callback.
   * @since 0.96.0
   */
  <R> void batchCallback(
    final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback
  )
    throws IOException, InterruptedException;

  /**
   * Extracts certain cells from a given row.
   * @param get The object that specifies what data to fetch and from which row.
   * @return The data coming from the specified row, if it exists.  If the row
   * specified doesn't exist, the {@link Result} instance returned won't
   * contain any {@link org.apache.hadoop.hbase.KeyValue}, as indicated by {@link Result#isEmpty()}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  Result get(Get get) throws IOException;

  /**
   * Extracts certain cells from the given rows, in batch.
   *
   * @param gets The objects that specify what data to fetch and from which rows.
   * @return The data coming from the specified rows, if it exists.  If the row specified doesn't
   * exist, the {@link Result} instance returned won't contain any {@link
   * org.apache.hadoop.hbase.KeyValue}, as indicated by {@link Result#isEmpty()}. If there are any
   * failures even after retries, there will be a null in the results array for those Gets, AND an
   * exception will be thrown.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.90.0
   */
  Result[] get(List<Get> gets) throws IOException;

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
  ResultScanner getScanner(Scan scan) throws IOException;

  /**
   * Gets a scanner on the current table for the given family.
   *
   * @param family The column family to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family) throws IOException;

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   *
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;


  /**
   * Puts some data in the table.
   *
   * @param put The data to put.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void put(Put put) throws IOException;

  /**
   * Puts some data in the table, in batch.
   * <p>
   * This can be used for group commit, or for submitting user defined
   * batches.  The writeBuffer will be periodically inspected while the List
   * is processed, so depending on the List size the writeBuffer may flush
   * not at all, or more than once.
   * @param puts The list of mutations to apply. The batch put is done by
   * aggregating the iteration of the Puts over the write buffer
   * at the client-side for a single RPC call.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void put(List<Put> puts) throws IOException;

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
   */
  boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
    byte[] value, Put put) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   */
  boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
    CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException;

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void delete(Delete delete) throws IOException;

  /**
   * Deletes the specified cells/rows in bulk.
   * @param deletes List of things to delete.  List gets modified by this
   * method (in particular it gets re-ordered, so the order in which the elements
   * are inserted in the list gives no guarantee as to the order in which the
   * {@link Delete}s are executed).
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code deletes} argument will contain the {@link Delete} instances
   * that have not be successfully applied.
   * @since 0.20.1
   */
  void delete(List<Delete> deletes) throws IOException;

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
   */
  boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
    byte[] value, Delete delete) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp comparison operator to use
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   */
  boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
    CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException;

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param rm object that specifies the set of mutations to perform atomically
   * @throws IOException
   */
  void mutateRow(final RowMutations rm) throws IOException;

  /**
   * Appends values to one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers.  Appends are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param append object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the append operation (maybe null)
   */
  Result append(final Append append) throws IOException;

  /**
   * Increments one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers.  Increments are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param increment object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the increment
   */
  Result increment(final Increment increment) throws IOException;

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
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
    long amount) throws IOException;

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
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
    long amount, Durability durability) throws IOException;

  /**
   * Releases any resources held or pending changes in internal buffers.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  @Override
  void close() throws IOException;

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
  CoprocessorRpcChannel coprocessorService(byte[] row);

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), and
   * invokes the passed {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * with each {@link com.google.protobuf.Service} instance.
   *
   * @param service the protocol buffer {@code Service} implementation to call
   * @param startKey start region selection with region containing this row.  If {@code null}, the
   * selection will start with the first table region.
   * @param endKey select regions up to and including the region containing this row. If {@code
   * null}, selection will continue through the last table region.
   * @param callable this instance's {@link org.apache.hadoop.hbase.client.coprocessor.Batch
   * .Call#call}
   * method will be invoked once per table region, using the {@link com.google.protobuf.Service}
   * instance connected to that region.
   * @param <T> the {@link com.google.protobuf.Service} subclass to connect to
   * @param <R> Return type for the {@code callable} parameter's {@link
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * @return a map of result values keyed by region name
   */
  <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
    byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
    throws ServiceException, Throwable;

  /**
   * Creates an instance of the given {@link com.google.protobuf.Service} subclass for each table
   * region spanning the range from the {@code startKey} row to {@code endKey} row (inclusive), and
   * invokes the passed {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   * with each {@link Service} instance.
   *
   * <p> The given {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[],
   * byte[], Object)} method will be called with the return value from each region's {@link
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} invocation. </p>
   *
   * @param service the protocol buffer {@code Service} implementation to call
   * @param startKey start region selection with region containing this row.  If {@code null}, the
   * selection will start with the first table region.
   * @param endKey select regions up to and including the region containing this row. If {@code
   * null}, selection will continue through the last table region.
   * @param callable this instance's {@link org.apache.hadoop.hbase.client.coprocessor.Batch
   * .Call#call}
   * method will be invoked once per table region, using the {@link Service} instance connected to
   * that region.
   * @param callback
   * @param <T> the {@link Service} subclass to connect to
   * @param <R> Return type for the {@code callable} parameter's {@link
   * org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call} method
   */
  <T extends Service, R> void coprocessorService(final Class<T> service,
    byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
    final Batch.Callback<R> callback) throws ServiceException, Throwable;

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
    * @deprecated as of 1.0.1 (should not have been in 1.0.0). Replaced by {@link BufferedMutator#getWriteBufferSize()}
   */
  @Deprecated
  long getWriteBufferSize();

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   * @deprecated as of 1.0.1 (should not have been in 1.0.0). Replaced by {@link BufferedMutator} and
   * {@link BufferedMutatorParams#writeBufferSize(long)}
   */
  @Deprecated
  void setWriteBufferSize(long writeBufferSize) throws IOException;

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
   * @throws ServiceException
   * @throws Throwable
   * @return a map of result values keyed by region name
   */
  <R extends Message> Map<byte[], R> batchCoprocessorService(
    Descriptors.MethodDescriptor methodDescriptor, Message request,
    byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable;

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
   * @param callback
   *          callback to invoke with the response for each region
   * @param <R>
   *          the response type for the coprocessor Service method
   * @throws ServiceException
   * @throws Throwable
   */
  <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
    Message request, byte[] startKey, byte[] endKey, R responsePrototype,
    Batch.Callback<R> callback) throws ServiceException, Throwable;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value.
   * If it does, it performs the row mutations.  If the passed value is null, the check
   * is for the lack of column (ie: non-existence)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param value the expected value
   * @param mutation  mutations to perform if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   */
  boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException;
}
