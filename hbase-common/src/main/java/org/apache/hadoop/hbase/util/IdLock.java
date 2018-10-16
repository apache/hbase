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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Allows multiple concurrent clients to lock on a numeric id with a minimal
 * memory overhead. The intended usage is as follows:
 *
 * <pre>
 * IdLock.Entry lockEntry = idLock.getLockEntry(id);
 * try {
 *   // User code.
 * } finally {
 *   idLock.releaseLockEntry(lockEntry);
 * }</pre>
 */
@InterfaceAudience.Private
public class IdLock {

  /** An entry returned to the client as a lock object */
  public static final class Entry {
    private final long id;
    private int numWaiters;
    private boolean locked = true;

    private Entry(long id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return "id=" + id + ", numWaiter=" + numWaiters + ", isLocked="
          + locked;
    }
  }

  private ConcurrentMap<Long, Entry> map = new ConcurrentHashMap<>();

  /**
   * Blocks until the lock corresponding to the given id is acquired.
   *
   * @param id an arbitrary number to lock on
   * @return an "entry" to pass to {@link #releaseLockEntry(Entry)} to release
   *         the lock
   * @throws IOException if interrupted
   */
  public Entry getLockEntry(long id) throws IOException {
    Entry entry = new Entry(id);
    Entry existing;
    while ((existing = map.putIfAbsent(entry.id, entry)) != null) {
      synchronized (existing) {
        if (existing.locked) {
          ++existing.numWaiters;  // Add ourselves to waiters.
          while (existing.locked) {
            try {
              existing.wait();
            } catch (InterruptedException e) {
              --existing.numWaiters;  // Remove ourselves from waiters.
              throw new InterruptedIOException(
                  "Interrupted waiting to acquire sparse lock");
            }
          }

          --existing.numWaiters;  // Remove ourselves from waiters.
          existing.locked = true;
          return existing;
        }
        // If the entry is not locked, it might already be deleted from the
        // map, so we cannot return it. We need to get our entry into the map
        // or get someone else's locked entry.
      }
    }
    return entry;
  }

  /**
   * Blocks until the lock corresponding to the given id is acquired.
   *
   * @param id an arbitrary number to lock on
   * @param time time to wait in ms
   * @return an "entry" to pass to {@link #releaseLockEntry(Entry)} to release
   *         the lock
   * @throws IOException if interrupted
   */
  public Entry tryLockEntry(long id, long time) throws IOException {
    Preconditions.checkArgument(time >= 0);
    Entry entry = new Entry(id);
    Entry existing;
    long waitUtilTS = System.currentTimeMillis() + time;
    long remaining = time;
    while ((existing = map.putIfAbsent(entry.id, entry)) != null) {
      synchronized (existing) {
        if (existing.locked) {
          ++existing.numWaiters;  // Add ourselves to waiters.
          try {
            while (existing.locked) {
              existing.wait(remaining);
              if (existing.locked) {
                long currentTS = System.currentTimeMillis();
                if (currentTS >= waitUtilTS) {
                  // time is up
                  return null;
                } else {
                  // our wait is waken, but the lock is still taken, this can happen
                  // due to JDK Object's wait/notify mechanism.
                  // Calculate the new remaining time to wait
                  remaining = waitUtilTS - currentTS;
                }
              }

            }
          } catch (InterruptedException e) {
            throw new InterruptedIOException(
                "Interrupted waiting to acquire sparse lock");
          } finally {
            --existing.numWaiters;  // Remove ourselves from waiters.
          }
          existing.locked = true;
          return existing;
        }
        // If the entry is not locked, it might already be deleted from the
        // map, so we cannot return it. We need to get our entry into the map
        // or get someone else's locked entry.
      }
    }
    return entry;
  }

  /**
   * Must be called in a finally block to decrease the internal counter and
   * remove the monitor object for the given id if the caller is the last
   * client.
   *
   * @param entry the return value of {@link #getLockEntry(long)}
   */
  public void releaseLockEntry(Entry entry) {
    synchronized (entry) {
      entry.locked = false;
      if (entry.numWaiters > 0) {
        entry.notify();
      } else {
        map.remove(entry.id);
      }
    }
  }

  /** For testing */
  void assertMapEmpty() {
    assert map.isEmpty();
  }

  @VisibleForTesting
  public void waitForWaiters(long id, int numWaiters) throws InterruptedException {
    for (Entry entry;;) {
      entry = map.get(id);
      if (entry != null) {
        synchronized (entry) {
          if (entry.numWaiters >= numWaiters) {
            return;
          }
        }
      }
      Thread.sleep(100);
    }
  }
}
