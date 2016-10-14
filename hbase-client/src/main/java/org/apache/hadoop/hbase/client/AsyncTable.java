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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The asynchronous version of Table. Obtain an instance from a {@link AsyncConnection}.
 * <p>
 * The implementation is NOT required to be thread safe. Do NOT access it from multiple threads
 * concurrently.
 * <p>
 * Usually the implementations will not throw any exception directly, you need to get the exception
 * from the returned {@link CompletableFuture}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AsyncTable {

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
   * Test for the existence of columns in the table, as specified by the Get.
   * <p>
   * This will return true if the Get matches one or more keys, false if not.
   * <p>
   * This is a server-side call so it prevents any data from being transfered to the client.
   */
  CompletableFuture<Boolean> exists(Get get);

  /**
   * Extracts certain cells from a given row.
   * <p>
   * Return the data coming from the specified row, if it exists. If the row specified doesn't
   * exist, the {@link Result} instance returned won't contain any
   * {@link org.apache.hadoop.hbase.KeyValue}, as indicated by {@link Result#isEmpty()}.
   * @param get The object that specifies what data to fetch and from which row.
   */
  CompletableFuture<Result> get(Get get);

  /**
   * Puts some data to the table.
   * @param put The data to put.
   */
  CompletableFuture<Void> put(Put put);

  /**
   * Deletes the specified cells/row.
   * @param delete The object that specifies what to delete.
   */
  CompletableFuture<Void> delete(Delete delete);
}
