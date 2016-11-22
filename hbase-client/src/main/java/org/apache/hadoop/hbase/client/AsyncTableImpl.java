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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

/**
 * The implementation of AsyncTable. Based on {@link RawAsyncTable}.
 */
@InterfaceAudience.Private
class AsyncTableImpl implements AsyncTable {

  private final RawAsyncTable rawTable;

  private final ExecutorService pool;

  public AsyncTableImpl(AsyncConnectionImpl conn, TableName tableName, ExecutorService pool) {
    this.rawTable = conn.getRawTable(tableName);
    this.pool = pool;
  }

  @Override
  public TableName getName() {
    return rawTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return rawTable.getConfiguration();
  }

  @Override
  public void setReadRpcTimeout(long timeout, TimeUnit unit) {
    rawTable.setReadRpcTimeout(timeout, unit);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return rawTable.getReadRpcTimeout(unit);
  }

  @Override
  public void setWriteRpcTimeout(long timeout, TimeUnit unit) {
    rawTable.setWriteRpcTimeout(timeout, unit);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return rawTable.getWriteRpcTimeout(unit);
  }

  @Override
  public void setOperationTimeout(long timeout, TimeUnit unit) {
    rawTable.setOperationTimeout(timeout, unit);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return rawTable.getOperationTimeout(unit);
  }

  @Override
  public void setScanTimeout(long timeout, TimeUnit unit) {
    rawTable.setScanTimeout(timeout, unit);
  }

  @Override
  public long getScanTimeout(TimeUnit unit) {
    return rawTable.getScanTimeout(unit);
  }

  private <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    CompletableFuture<T> asyncFuture = new CompletableFuture<>();
    future.whenCompleteAsync((r, e) -> {
      if (e != null) {
        asyncFuture.completeExceptionally(e);
      } else {
        asyncFuture.complete(r);
      }
    }, pool);
    return asyncFuture;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    return wrap(rawTable.get(get));
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    return wrap(rawTable.put(put));
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    return wrap(rawTable.delete(delete));
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    return wrap(rawTable.append(append));
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    return wrap(rawTable.increment(increment));
  }

  @Override
  public CompletableFuture<Boolean> checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put) {
    return wrap(rawTable.checkAndPut(row, family, qualifier, compareOp, value, put));
  }

  @Override
  public CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete) {
    return wrap(rawTable.checkAndDelete(row, family, qualifier, compareOp, value, delete));
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations mutation) {
    return wrap(rawTable.mutateRow(mutation));
  }

  @Override
  public CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations mutation) {
    return wrap(rawTable.checkAndMutate(row, family, qualifier, compareOp, value, mutation));
  }

  @Override
  public CompletableFuture<List<Result>> smallScan(Scan scan, int limit) {
    return wrap(rawTable.smallScan(scan, limit));
  }
}
