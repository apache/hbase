/**
 * Copyright 2013 The Apache Software Foundation
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

import java.io.IOException;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Used to communicate with a single HBase table.
 * Provide additional asynchronous APIs as complement of HTableInterface.
 * When APIs are called, it returns a ListenableFuture object immediately,
 * and executes the request in the background. Call blocking method get()
 * on the ListenableFuture object to get the result.
 *
 * We don't provide asynchronous API that does not directly contact region
 * server, for example, put().
 */
public interface HTableAsyncInterface extends HTableInterface {

  /**
   * Extracts certain cells from a given row.
   *
   * @param get The object that specifies what data to fetch and from which row.
   * @return Listenable future of data coming from the specified row, if it exists.
   * If the row specified doesn't exist, the {@link Result} instance returned won't
   * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   */
  ListenableFuture<Result> getAsync(Get get);

  /**
   * Extracts certain cells from the given rows, in batch.
   *
   * @param list The objects that specify what data to fetch and from which rows.
   * @return Listenable future of data coming from the specified rows, if it exists.
   * If the row specified doesn't exist, the {@link Result} instance returned won't
   * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   * If there are any failures even after retries, there will be a null in
   * the results array for those Gets, AND an exception will be thrown.
   */
  ListenableFuture<Result[]> batchGetAsync(final List<Get> list);

  /**
   * Return the row that matches <i>row</i> exactly, or the one that immediately precedes it.
   *
   * @param row A row key.
   * @param family Column family to include in the {@link Result}.
   * @return Listenable future of the row that matches <i>row</i> exactly, or
   * the one that immediately precedes it.
   */
  ListenableFuture<Result> getRowOrBeforeAsync(byte[] row, byte[] family);

  /**
   * Puts some data in the table.
   * <p>
   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
   * until the internal buffer is full.
   *
   * @param put The data to put.
   * @return Listenable future.
   */
  ListenableFuture<Void> putAsync(Put put);

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @return Listenable future.
   */
  ListenableFuture<Void> deleteAsync(Delete delete);

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param arm object that specifies the set of mutations to perform
   * @return Listenable future.
   */
  ListenableFuture<Void> mutateRowAsync(RowMutations arm);

  /**
   * Process batch of mutations on a row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param mutations objects that specify the set of mutations to perform.
   * @return Listenable future.
   */
  ListenableFuture<Void> batchMutateAsync(List<Mutation> mutations);

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch
   * of {@link Put}s (when <code>put(List<Put>)</code> is used) when
   * {@link #isAutoFlush} is {@code true}.
   *
   * @return Listenable future.
   */
  ListenableFuture<Void> flushCommitsAsync();

  /**
   * Obtains a lock on a row.
   *
   * @param row The row to lock.
   * @return Listenable future of a {@link RowLock} containing the row and lock id.
   */
  ListenableFuture<RowLock> lockRowAsync(byte[] row);

  /**
   * Releases a row lock.
   *
   * @param rl The row lock to release.
   * @return Listenable future.
   */
  ListenableFuture<Void> unlockRowAsync(RowLock rl);
}
