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

import com.google.protobuf.RpcChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

/**
 * Can be overridden in UT if you only want to implement part of the methods in {@link AsyncTable}.
 */
public class DummyAsyncTable<C extends ScanResultConsumerBase> implements AsyncTable<C> {

  @Override
  public TableName getName() {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return 0;
  }

  @Override
  public long getScanTimeout(TimeUnit unit) {
    return 0;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    return null;
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    return null;
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    return null;
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    return null;
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    return null;
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return null;
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations mutation) {
    return null;
  }

  @Override
  public void scan(Scan scan, C consumer) {
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    return null;
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    return null;
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    return null;
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return null;
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return null;
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    return null;
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, byte[] row) {
    return null;
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
      Function<RpcChannel, S> stubMaker, ServiceCaller<S, R> callable,
      CoprocessorCallback<R> callback) {
    return null;
  }

}
