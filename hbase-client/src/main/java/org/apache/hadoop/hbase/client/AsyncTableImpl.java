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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.stream.Collectors.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * The implementation of AsyncTable. Based on {@link RawAsyncTable}.
 */
@InterfaceAudience.Private
class AsyncTableImpl implements AsyncTable {

  private final RawAsyncTable rawTable;

  private final ExecutorService pool;

  private final long defaultScannerMaxResultSize;

  AsyncTableImpl(AsyncConnectionImpl conn, RawAsyncTable rawTable, ExecutorService pool) {
    this.rawTable = rawTable;
    this.pool = pool;
    this.defaultScannerMaxResultSize = conn.connConf.getScannerMaxResultSize();
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
  public long getRpcTimeout(TimeUnit unit) {
    return rawTable.getRpcTimeout(unit);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return rawTable.getReadRpcTimeout(unit);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return rawTable.getWriteRpcTimeout(unit);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return rawTable.getOperationTimeout(unit);
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
                                                CompareOperator op, byte[] value, Put put) {
    return wrap(rawTable.checkAndPut(row, family, qualifier, op, value, put));
  }

  @Override
  public CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                                   CompareOperator op, byte[] value, Delete delete) {
    return wrap(rawTable.checkAndDelete(row, family, qualifier, op, value, delete));
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations mutation) {
    return wrap(rawTable.mutateRow(mutation));
  }

  @Override
  public CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
                                                   CompareOperator op, byte[] value, RowMutations mutation) {
    return wrap(rawTable.checkAndMutate(row, family, qualifier, op, value, mutation));
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    return wrap(rawTable.scanAll(scan));
  }

  private long resultSize2CacheSize(long maxResultSize) {
    // * 2 if possible
    return maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    return new AsyncTableResultScanner(rawTable, ReflectionUtils.newInstance(scan.getClass(), scan),
        resultSize2CacheSize(
          scan.getMaxResultSize() > 0 ? scan.getMaxResultSize() : defaultScannerMaxResultSize));
  }

  private void scan0(Scan scan, ScanResultConsumer consumer) {
    try (ResultScanner scanner = getScanner(scan)) {
      consumer.onScanMetricsCreated(scanner.getScanMetrics());
      for (Result result; (result = scanner.next()) != null;) {
        if (!consumer.onNext(result)) {
          break;
        }
      }
      consumer.onComplete();
    } catch (IOException e) {
      consumer.onError(e);
    }
  }

  @Override
  public void scan(Scan scan, ScanResultConsumer consumer) {
    pool.execute(() -> scan0(scan, consumer));
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    return rawTable.get(gets).stream().map(this::wrap).collect(toList());
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return rawTable.put(puts).stream().map(this::wrap).collect(toList());
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return rawTable.delete(deletes).stream().map(this::wrap).collect(toList());
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    return rawTable.<T> batch(actions).stream().map(this::wrap).collect(toList());
  }

}
