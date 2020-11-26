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

import com.google.protobuf.RpcChannel;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Just a wrapper of {@link RawAsyncTableImpl}. The difference is that users need to provide a
 * thread pool when constructing this class, and the callback methods registered to the returned
 * {@link CompletableFuture} will be executed in this thread pool. So usually it is safe for users
 * to do anything they want in the callbacks without breaking the rpc framework.
 */
@InterfaceAudience.Private
class AsyncTableImpl implements AsyncTable<ScanResultConsumer> {

  private final AsyncTable<AdvancedScanResultConsumer> rawTable;

  private final ExecutorService pool;

  AsyncTableImpl(AsyncConnectionImpl conn, AsyncTable<AdvancedScanResultConsumer> rawTable,
      ExecutorService pool) {
    this.rawTable = rawTable;
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
  public CompletableFuture<TableDescriptor> getDescriptor() {
    return wrap(rawTable.getDescriptor());
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator() {
    return rawTable.getRegionLocator();
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
    return FutureUtils.wrapFuture(future, pool);
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
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilder() {

      private final CheckAndMutateBuilder builder = rawTable.checkAndMutate(row, family);

      @Override
      public CompletableFuture<Boolean> thenPut(Put put) {
        return wrap(builder.thenPut(put));
      }

      @Override
      public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
        return wrap(builder.thenMutate(mutation));
      }

      @Override
      public CompletableFuture<Boolean> thenDelete(Delete delete) {
        return wrap(builder.thenDelete(delete));
      }

      @Override
      public CheckAndMutateBuilder qualifier(byte[] qualifier) {
        builder.qualifier(qualifier);
        return this;
      }

      @Override
      public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
        builder.timeRange(timeRange);
        return this;
      }

      @Override
      public CheckAndMutateBuilder ifNotExists() {
        builder.ifNotExists();
        return this;
      }

      @Override
      public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
        builder.ifMatches(compareOp, value);
        return this;
      }
    };
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    return new CheckAndMutateWithFilterBuilder() {

      private final CheckAndMutateWithFilterBuilder builder =
        rawTable.checkAndMutate(row, filter);

      @Override
      public CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange) {
        builder.timeRange(timeRange);
        return this;
      }

      @Override
      public CompletableFuture<Boolean> thenPut(Put put) {
        return wrap(builder.thenPut(put));
      }

      @Override
      public CompletableFuture<Boolean> thenDelete(Delete delete) {
        return wrap(builder.thenDelete(delete));
      }

      @Override
      public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
        return wrap(builder.thenMutate(mutation));
      }
    };
  }

  @Override
  public CompletableFuture<CheckAndMutateResult> checkAndMutate(CheckAndMutate checkAndMutate) {
    return wrap(rawTable.checkAndMutate(checkAndMutate));
  }

  @Override
  public List<CompletableFuture<CheckAndMutateResult>> checkAndMutate(
    List<CheckAndMutate> checkAndMutates) {
    return rawTable.checkAndMutate(checkAndMutates).stream()
      .map(this::wrap).collect(toList());
  }

  @Override
  public CompletableFuture<Result> mutateRow(RowMutations mutation) {
    return wrap(rawTable.mutateRow(mutation));
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    return wrap(rawTable.scanAll(scan));
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    return rawTable.getScanner(scan);
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

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, byte[] row) {
    return wrap(rawTable.coprocessorService(stubMaker, callable, row));
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
      Function<RpcChannel, S> stubMaker, ServiceCaller<S, R> callable,
      CoprocessorCallback<R> callback) {
    CoprocessorCallback<R> wrappedCallback = new CoprocessorCallback<R>() {

      @Override
      public void onRegionComplete(RegionInfo region, R resp) {
        pool.execute(() -> callback.onRegionComplete(region, resp));
      }

      @Override
      public void onRegionError(RegionInfo region, Throwable error) {
        pool.execute(() -> callback.onRegionError(region, error));
      }

      @Override
      public void onComplete() {
        pool.execute(() -> callback.onComplete());
      }

      @Override
      public void onError(Throwable error) {
        pool.execute(() -> callback.onError(error));
      }
    };
    CoprocessorServiceBuilder<S, R> builder =
      rawTable.coprocessorService(stubMaker, callable, wrappedCallback);
    return new CoprocessorServiceBuilder<S, R>() {

      @Override
      public CoprocessorServiceBuilder<S, R> fromRow(byte[] startKey, boolean inclusive) {
        builder.fromRow(startKey, inclusive);
        return this;
      }

      @Override
      public CoprocessorServiceBuilder<S, R> toRow(byte[] endKey, boolean inclusive) {
        builder.toRow(endKey, inclusive);
        return this;
      }

      @Override
      public void execute() {
        builder.execute();
      }
    };
  }
}
