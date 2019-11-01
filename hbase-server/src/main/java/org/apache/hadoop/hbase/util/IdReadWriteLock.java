/*
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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
@InterfaceAudience.Private
public abstract class IdReadWriteLock<T> {
  public abstract ReentrantReadWriteLock getLock(T id);

  @VisibleForTesting
  public void waitForWaiters(T id, int numWaiters) throws InterruptedException {
    for (ReentrantReadWriteLock readWriteLock;;) {
      readWriteLock = getLock(id);
      if (readWriteLock != null) {
        synchronized (readWriteLock) {
          if (readWriteLock.getQueueLength() >= numWaiters) {
            return;
          }
        }
      }
      Thread.sleep(50);
    }
  }
}
