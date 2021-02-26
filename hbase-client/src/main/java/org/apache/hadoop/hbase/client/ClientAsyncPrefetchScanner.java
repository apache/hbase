/*
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

import static org.apache.hadoop.hbase.client.ConnectionUtils.calcEstimatedSize;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ClientAsyncPrefetchScanner implements async scanner behaviour.
 * Specifically, the cache used by this scanner is a concurrent queue which allows both
 * the producer (hbase client) and consumer (application) to access the queue in parallel.
 * The number of rows returned in a prefetch is defined by the caching factor and the result size
 * factor.
 * This class allocates a buffer cache, whose size is a function of both factors.
 * The prefetch is invoked when the cache is half­filled, instead of waiting for it to be empty.
 * This is defined in the method {@link ClientAsyncPrefetchScanner#prefetchCondition()}.
 */
@InterfaceAudience.Private
public class ClientAsyncPrefetchScanner extends ClientSimpleScanner {

  private long maxCacheSize;
  private AtomicLong cacheSizeInBytes;
  // exception queue (from prefetch to main scan execution)
  private final Queue<Exception> exceptionsQueue;
  // used for testing
  private Consumer<Boolean> prefetchListener;

  private final Lock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final Condition notFull = lock.newCondition();

  public ClientAsyncPrefetchScanner(Configuration configuration, Scan scan, TableName name,
      ClusterConnection connection, RpcRetryingCallerFactory rpcCallerFactory,
      RpcControllerFactory rpcControllerFactory, ExecutorService pool,
      int replicaCallTimeoutMicroSecondScan) throws IOException {
    super(configuration, scan, name, connection, rpcCallerFactory, rpcControllerFactory, pool,
        replicaCallTimeoutMicroSecondScan);
    exceptionsQueue = new ConcurrentLinkedQueue<>();
    Threads.setDaemonThreadRunning(new Thread(new PrefetchRunnable()), name + ".asyncPrefetcher");
  }

  void setPrefetchListener(Consumer<Boolean> prefetchListener) {
    this.prefetchListener = prefetchListener;
  }

  @Override
  protected void initCache() {
    // Override to put a different cache in place of the super's -- a concurrent one.
    cache = new LinkedBlockingQueue<>();
    maxCacheSize = resultSize2CacheSize(maxScannerResultSize);
    cacheSizeInBytes = new AtomicLong(0);
  }

  private long resultSize2CacheSize(long maxResultSize) {
    // * 2 if possible
    return maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
  }

  @Override
  public Result next() throws IOException {
    try {
      lock.lock();
      while (cache.isEmpty()) {
        handleException();
        if (this.closed) {
          return null;
        }
        try {
          notEmpty.await();
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when wait to load cache");
        }
      }

      Result result = pollCache();
      if (prefetchCondition()) {
        notFull.signalAll();
      }
      return result;
    } finally {
      lock.unlock();
      handleException();
    }
  }

  @Override
  public void close() {
    try {
      lock.lock();
      super.close();
      closed = true;
      notFull.signalAll();
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void addEstimatedSize(long estimatedSize) {
    cacheSizeInBytes.addAndGet(estimatedSize);
  }

  private void handleException() throws IOException {
    //The prefetch task running in the background puts any exception it
    //catches into this exception queue.
    // Rethrow the exception so the application can handle it.
    while (!exceptionsQueue.isEmpty()) {
      Exception first = exceptionsQueue.peek();
      first.printStackTrace();
      if (first instanceof IOException) {
        throw (IOException) first;
      }
      throw (RuntimeException) first;
    }
  }

  private boolean prefetchCondition() {
    return cacheSizeInBytes.get() < maxCacheSize / 2;
  }

  private Result pollCache() {
    Result res = cache.poll();
    long estimatedSize = calcEstimatedSize(res);
    addEstimatedSize(-estimatedSize);
    return res;
  }

  private class PrefetchRunnable implements Runnable {

    @Override
    public void run() {
      while (!closed) {
        boolean succeed = false;
        try {
          lock.lock();
          while (!prefetchCondition()) {
            notFull.await();
          }
          loadCache();
          succeed = true;
        } catch (Exception e) {
          exceptionsQueue.add(e);
        } finally {
          notEmpty.signalAll();
          lock.unlock();
          if (prefetchListener != null) {
            prefetchListener.accept(succeed);
          }
        }
      }
    }

  }

}
