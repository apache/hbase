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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
public class ClientAsyncPrefetchScanner extends ClientScanner {

  private static final int ESTIMATED_SINGLE_RESULT_SIZE = 1024;
  private static final int DEFAULT_QUEUE_CAPACITY = 1024;

  private int cacheCapacity;
  private AtomicLong cacheSizeInBytes;
  // exception queue (from prefetch to main scan execution)
  private Queue<Exception> exceptionsQueue;
  // prefetch runnable object to be executed asynchronously
  private PrefetchRunnable prefetchRunnable;
  // Boolean flag to ensure only a single prefetch is running (per scan)
  // We use atomic boolean to allow multiple concurrent threads to
  // consume records from the same cache, but still have a single prefetcher thread.
  // For a single consumer thread this can be replace with a native boolean.
  private AtomicBoolean prefetchRunning;
  // an attribute for synchronizing close between scanner and prefetch threads
  private AtomicLong closingThreadId;
  private static final int NO_THREAD = -1;

  public ClientAsyncPrefetchScanner(Configuration configuration, Scan scan, TableName name,
      ClusterConnection connection, RpcRetryingCallerFactory rpcCallerFactory,
      RpcControllerFactory rpcControllerFactory, ExecutorService pool,
      int replicaCallTimeoutMicroSecondScan) throws IOException {
    super(configuration, scan, name, connection, rpcCallerFactory, rpcControllerFactory, pool,
        replicaCallTimeoutMicroSecondScan);
  }

  @Override
  protected void initCache() {
    // concurrent cache
    cacheCapacity = calcCacheCapacity();
    cache = new LinkedBlockingQueue<Result>(cacheCapacity);
    cacheSizeInBytes = new AtomicLong(0);
    exceptionsQueue = new ConcurrentLinkedQueue<Exception>();
    prefetchRunnable = new PrefetchRunnable();
    prefetchRunning = new AtomicBoolean(false);
    closingThreadId = new AtomicLong(NO_THREAD);
  }

  @Override
  public Result next() throws IOException {

    try {
      handleException();

      // If the scanner is closed and there's nothing left in the cache, next is a no-op.
      if (getCacheCount() == 0 && this.closed) {
        return null;
      }
      if (prefetchCondition()) {
        // run prefetch in the background only if no prefetch is already running
        if (!isPrefetchRunning()) {
          if (prefetchRunning.compareAndSet(false, true)) {
            getPool().execute(prefetchRunnable);
          }
        }
      }

      while (isPrefetchRunning()) {
        // prefetch running or still pending
        if (getCacheCount() > 0) {
          return pollCache();
        } else {
          // (busy) wait for a record - sleep
          Threads.sleep(1);
        }
      }

      if (getCacheCount() > 0) {
        return pollCache();
      }

      // if we exhausted this scanner before calling close, write out the scan metrics
      writeScanMetrics();
      return null;
    } finally {
      handleException();
    }
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    closed = true;
    if (!isPrefetchRunning()) {
      if(closingThreadId.compareAndSet(NO_THREAD, Thread.currentThread().getId())) {
        super.close();
      }
    } // else do nothing since the async prefetch still needs this resources
  }

  @Override
  public int getCacheCount() {
    if(cache != null) {
      int size = cache.size();
      if(size > cacheCapacity) {
        cacheCapacity = size;
      }
      return size;
    } else {
      return 0;
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

  private boolean isPrefetchRunning() {
    return prefetchRunning.get();
  }

  // double buffer - double cache size
  private int calcCacheCapacity() {
    int capacity = Integer.MAX_VALUE;
    if(caching > 0 && caching < (Integer.MAX_VALUE /2)) {
      capacity = caching * 2 + 1;
    }
    if(capacity == Integer.MAX_VALUE){
      if(maxScannerResultSize != Integer.MAX_VALUE) {
        capacity = (int) (maxScannerResultSize / ESTIMATED_SINGLE_RESULT_SIZE);
      }
      else {
        capacity = DEFAULT_QUEUE_CAPACITY;
      }
    }
    return capacity;
  }

  private boolean prefetchCondition() {
    return
        (getCacheCount() < getCountThreshold()) &&
        (maxScannerResultSize == Long.MAX_VALUE ||
         getCacheSizeInBytes() < getSizeThreshold()) ;
  }

  private int getCountThreshold() {
    return cacheCapacity / 2 ;
  }

  private long getSizeThreshold() {
    return maxScannerResultSize / 2 ;
  }

  private long getCacheSizeInBytes() {
    return cacheSizeInBytes.get();
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
      try {
        loadCache();
      } catch (Exception e) {
        exceptionsQueue.add(e);
      } finally {
        prefetchRunning.set(false);
        if(closed) {
          if (closingThreadId.compareAndSet(NO_THREAD, Thread.currentThread().getId())) {
            // close was waiting for the prefetch to end
            close();
          }
        }
      }
    }

  }

}
