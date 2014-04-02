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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.HConnectionParams;
import org.apache.hadoop.hbase.ipc.thrift.HBaseToThriftAdapter;
import org.apache.hadoop.hbase.thrift.SelfRetryingListenableFuture;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Used to communicate with a single HBase table.
 * Provide additional asynchronous APIs as complement of HTableInterface.
 */
public class HTableAsync extends HTable implements HTableAsyncInterface {

  private static final ListeningScheduledExecutorService executorService;
  static {
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(
        HConstants.DEFAULT_HTABLE_ASYNC_CORE_THREADS, new DaemonThreadFactory("htable-async-thread-"));
    ((ThreadPoolExecutor)executor).setMaximumPoolSize(HConstants.DEFAULT_HTABLE_ASYNC_MAX_THREADS);
    ((ThreadPoolExecutor)executor).setKeepAliveTime(
        HConstants.DEFAULT_HTABLE_ASYNC_KEEPALIVE_SECONDS, TimeUnit.SECONDS);
    executorService = MoreExecutors.listeningDecorator(executor);
  }

  private HConnectionParams hConnectionParams;

  /**
   * Creates an object to access a HBase table through asynchronous APIs.
   *
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public HTableAsync(Configuration conf, String tableName)
      throws IOException {
    this(conf, Bytes.toBytes(tableName));

    this.hConnectionParams = HConnectionParams.getInstance(conf);
  }

  /**
   * Creates an object to access a HBase table through asynchronous APIs.
   *
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTableAsync(Configuration conf, byte[] tableName)
      throws IOException {
    super(conf, tableName);

    this.hConnectionParams = HConnectionParams.getInstance(conf);
  }

  public HTableAsync(HTable t) {
    super(t);

    this.hConnectionParams = HConnectionParams.getInstance(getConfiguration());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Result> getAsync(final Get get) {
    ServerCallable<ListenableFuture<Result>> callable =
        new ServerCallable<ListenableFuture<Result>>(getConnection(),
            tableName, get.getRow(), getOptions()) {
      @Override
      public ListenableFuture<Result> call() throws Exception {
        return ((HBaseToThriftAdapter)server).getAsync(location.getRegionInfo().getRegionName(), get);
      }
    };

    SelfRetryingListenableFuture<Result> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Result[]> batchGetAsync(final List<Get> list) {
    return executorService.submit(new Callable<Result[]>() {
      @Override
      public Result[] call() throws IOException {
        return batchGet(list);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> putAsync(final Put put) {
    // Since put has a buffer on client side, use mutateRowAsync instead
    try {
      RowMutations arm = new RowMutations.Builder(put.getRow()).add(put).create();
      return mutateRowAsync(arm);
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Result> getRowOrBeforeAsync(final byte[] row, final byte[] family) {
    ServerCallable<ListenableFuture<Result>> callable = new ServerCallable<ListenableFuture<Result>>(
        getConnection(), tableName, row, getOptions()) {
      @Override
      public ListenableFuture<Result> call() throws Exception {
        return ((HBaseToThriftAdapter)server).getClosestRowBeforeAsync(
            location.getRegionInfo().getRegionName(), row, family);
      }
    };

    SelfRetryingListenableFuture<Result> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> deleteAsync(final Delete delete) {
    ServerCallable<ListenableFuture<Void>> callable = new ServerCallable<ListenableFuture<Void>>(
        getConnection(), tableName, delete.getRow(), getOptions()) {
      @Override
      public ListenableFuture<Void> call() throws Exception {
        return ((HBaseToThriftAdapter)server).deleteAsync(location.getRegionInfo().getRegionName(), delete);
      }
    };

    SelfRetryingListenableFuture<Void> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> mutateRowAsync(final RowMutations arm) {
    ServerCallable<ListenableFuture<Void>> callable = new ServerCallable<ListenableFuture<Void>>(
        getConnection(), tableName, arm.getRow(), getOptions()) {
      @Override
      public ListenableFuture<Void> call() throws Exception {
        return ((HBaseToThriftAdapter)server).mutateRowAsync(location.getRegionInfo().getRegionName(), arm);
      }
    };

    SelfRetryingListenableFuture<Void> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> batchMutateAsync(final List<Mutation> mutations) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        batchMutate(mutations);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> flushCommitsAsync() {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        flushCommits();
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<RowLock> lockRowAsync(final byte[] row) {
    ServerCallable<ListenableFuture<RowLock>> callable =
        new ServerCallable<ListenableFuture<RowLock>>(getConnection(),
            tableName, row, getOptions()) {
      @Override
      public ListenableFuture<RowLock> call() throws Exception {
        return ((HBaseToThriftAdapter)server).lockRowAsync(location.getRegionInfo().getRegionName(), row);
      }
    };

    SelfRetryingListenableFuture<RowLock> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Void> unlockRowAsync(final RowLock rl) {
    ServerCallable<ListenableFuture<Void>> callable =
        new ServerCallable<ListenableFuture<Void>>(getConnection(), tableName,
            rl.getRow(), getOptions()) {
      @Override
      public ListenableFuture<Void> call() throws Exception {
        return ((HBaseToThriftAdapter)server).unlockRowAsync(
            location.getRegionInfo().getRegionName(), rl.getLockId());
      }
    };

    SelfRetryingListenableFuture<Void> future = new SelfRetryingListenableFuture<>(this, callable,
        hConnectionParams, executorService);

    return future.startFuture();
  }
}
