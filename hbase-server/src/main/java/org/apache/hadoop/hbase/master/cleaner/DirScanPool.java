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
package org.apache.hadoop.hbase.master.cleaner;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The thread pool used for scan directories
 */
@InterfaceAudience.Private
public class DirScanPool implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(DirScanPool.class);
  private volatile int size;
  private final ThreadPoolExecutor pool;
  private int cleanerLatch;
  private boolean reconfigNotification;

  public DirScanPool(Configuration conf) {
    String poolSize = conf.get(CleanerChore.CHORE_POOL_SIZE, CleanerChore.DEFAULT_CHORE_POOL_SIZE);
    size = CleanerChore.calculatePoolSize(poolSize);
    // poolSize may be 0 or 0.0 from a careless configuration,
    // double check to make sure.
    size = size == 0 ? CleanerChore.calculatePoolSize(CleanerChore.DEFAULT_CHORE_POOL_SIZE) : size;
    pool = initializePool(size);
    LOG.info("Cleaner pool size is {}", size);
    cleanerLatch = 0;
  }

  private static ThreadPoolExecutor initializePool(int size) {
    return Threads.getBoundedCachedThreadPool(size, 1, TimeUnit.MINUTES,
      new ThreadFactoryBuilder().setNameFormat("dir-scan-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  /**
   * Checks if pool can be updated. If so, mark for update later.
   * @param conf configuration
   */
  @Override
  public synchronized void onConfigurationChange(Configuration conf) {
    int newSize = CleanerChore.calculatePoolSize(
      conf.get(CleanerChore.CHORE_POOL_SIZE, CleanerChore.DEFAULT_CHORE_POOL_SIZE));
    if (newSize == size) {
      LOG.trace("Size from configuration is same as previous={}, no need to update.", newSize);
      return;
    }
    size = newSize;
    // Chore is working, update it later.
    reconfigNotification = true;
  }

  synchronized void latchCountUp() {
    cleanerLatch++;
  }

  synchronized void latchCountDown() {
    cleanerLatch--;
    notifyAll();
  }

  synchronized void execute(Runnable runnable) {
    pool.execute(runnable);
  }

  public synchronized void shutdownNow() {
    if (pool == null || pool.isShutdown()) {
      return;
    }
    pool.shutdownNow();
  }

  synchronized void tryUpdatePoolSize(long timeout) {
    if (!reconfigNotification) {
      return;
    }
    reconfigNotification = false;
    long stopTime = System.currentTimeMillis() + timeout;
    while (cleanerLatch != 0 && timeout > 0) {
      try {
        wait(timeout);
        timeout = stopTime - System.currentTimeMillis();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    LOG.info("Update chore's pool size from {} to {}", pool.getPoolSize(), size);
    pool.setCorePoolSize(size);
  }

  public int getSize() {
    return size;
  }
}
