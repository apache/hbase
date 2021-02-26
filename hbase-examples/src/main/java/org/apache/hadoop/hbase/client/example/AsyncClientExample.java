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
package org.apache.hadoop.hbase.client.example;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple example shows how to use asynchronous client.
 */
@InterfaceAudience.Private
public class AsyncClientExample extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncClientExample.class);

  /**
   * The size for thread pool.
   */
  private static final int THREAD_POOL_SIZE = 16;

  /**
   * The default number of operations.
   */
  private static final int DEFAULT_NUM_OPS = 100;

  /**
   * The name of the column family. d for default.
   */
  private static final byte[] FAMILY = Bytes.toBytes("d");

  /**
   * For the example we're just using one qualifier.
   */
  private static final byte[] QUAL = Bytes.toBytes("test");

  private final AtomicReference<CompletableFuture<AsyncConnection>> future =
      new AtomicReference<>();

  private CompletableFuture<AsyncConnection> getConn() {
    CompletableFuture<AsyncConnection> f = future.get();
    if (f != null) {
      return f;
    }
    for (;;) {
      if (future.compareAndSet(null, new CompletableFuture<>())) {
        CompletableFuture<AsyncConnection> toComplete = future.get();
        addListener(ConnectionFactory.createAsyncConnection(getConf()),(conn, error) -> {
          if (error != null) {
            toComplete.completeExceptionally(error);
            // we need to reset the future holder so we will get a chance to recreate an async
            // connection at next try.
            future.set(null);
            return;
          }
          toComplete.complete(conn);
        });
        return toComplete;
      } else {
        f = future.get();
        if (f != null) {
          return f;
        }
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NONNULL_PARAM_VIOLATION",
      justification = "it is valid to pass NULL to CompletableFuture#completedFuture")
  private CompletableFuture<Void> closeConn() {
    CompletableFuture<AsyncConnection> f = future.get();
    if (f == null) {
      return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    addListener(f, (conn, error) -> {
      if (error == null) {
        IOUtils.closeQuietly(conn, e -> LOG.warn("failed to close conn", e));
      }
      closeFuture.complete(null);
    });
    return closeFuture;
  }

  private byte[] getKey(int i) {
    return Bytes.toBytes(String.format("%08x", i));
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
      return -1;
    }
    TableName tableName = TableName.valueOf(args[0]);
    int numOps = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_NUM_OPS;
    ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE,
      new ThreadFactoryBuilder().setNameFormat("AsyncClientExample-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    // We use AsyncTable here so we need to provide a separated thread pool. RawAsyncTable does not
    // need a thread pool and may have a better performance if you use it correctly as it can save
    // some context switches. But if you use RawAsyncTable incorrectly, you may have a very bad
    // impact on performance so use it with caution.
    CountDownLatch latch = new CountDownLatch(numOps);
    IntStream.range(0, numOps).forEach(i -> {
      CompletableFuture<AsyncConnection> future = getConn();
      addListener(future, (conn, error) -> {
        if (error != null) {
          LOG.warn("failed to get async connection for " + i, error);
          latch.countDown();
          return;
        }
        AsyncTable<?> table = conn.getTable(tableName, threadPool);
        addListener(table.put(new Put(getKey(i)).addColumn(FAMILY, QUAL, Bytes.toBytes(i))),
          (putResp, putErr) -> {
            if (putErr != null) {
              LOG.warn("put failed for " + i, putErr);
              latch.countDown();
              return;
            }
            LOG.info("put for " + i + " succeeded, try getting");
            addListener(table.get(new Get(getKey(i))), (result, getErr) -> {
              if (getErr != null) {
                LOG.warn("get failed for " + i);
                latch.countDown();
                return;
              }
              if (result.isEmpty()) {
                LOG.warn("get failed for " + i + ", server returns empty result");
              } else if (!result.containsColumn(FAMILY, QUAL)) {
                LOG.warn("get failed for " + i + ", the result does not contain " +
                  Bytes.toString(FAMILY) + ":" + Bytes.toString(QUAL));
              } else {
                int v = Bytes.toInt(result.getValue(FAMILY, QUAL));
                if (v != i) {
                  LOG.warn("get failed for " + i + ", the value of " + Bytes.toString(FAMILY) +
                    ":" + Bytes.toString(QUAL) + " is " + v + ", exected " + i);
                } else {
                  LOG.info("get for " + i + " succeeded");
                }
              }
              latch.countDown();
            });
          });
      });
    });
    latch.await();
    closeConn().get();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new AsyncClientExample(), args);
  }
}
