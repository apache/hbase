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
package org.apache.hadoop.hbase.master.assignment;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A lock implementation which supports unlock by another thread.
 * <p>
 * This is because we need to hold region state node lock while updating region state to meta(for
 * keeping consistency), so it is better to yield the procedure to release the procedure worker. But
 * after waking up the procedure, we may use another procedure worker to execute the procedure,
 * which means we need to unlock by another thread. See HBASE-28196 for more details.
 */
@InterfaceAudience.Private
class RegionStateNodeLock {

  // for better logging message
  private final RegionInfo regionInfo;

  private final Lock lock = new ReentrantLock();

  private final Condition cond = lock.newCondition();

  private Object owner;

  private int count;

  RegionStateNodeLock(RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  private void lock0(Object lockBy) {
    lock.lock();
    try {
      for (;;) {
        if (owner == null) {
          owner = lockBy;
          count = 1;
          return;
        }
        if (owner == lockBy) {
          count++;
          return;
        }
        cond.awaitUninterruptibly();
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean tryLock0(Object lockBy) {
    if (!lock.tryLock()) {
      return false;
    }
    try {
      if (owner == null) {
        owner = lockBy;
        count = 1;
        return true;
      }
      if (owner == lockBy) {
        count++;
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  private void unlock0(Object unlockBy) {
    lock.lock();
    try {
      if (owner == null) {
        throw new IllegalMonitorStateException("RegionStateNode " + regionInfo + " is not locked");
      }
      if (owner != unlockBy) {
        throw new IllegalMonitorStateException("RegionStateNode " + regionInfo + " is locked by "
          + owner + ", can not be unlocked by " + unlockBy);
      }
      count--;
      if (count == 0) {
        owner = null;
        cond.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Normal lock, will set the current thread as owner. Typically you should use try...finally to
   * call unlock in the finally block.
   */
  void lock() {
    lock0(Thread.currentThread());
  }

  /**
   * Normal tryLock, will set the current thread as owner. Typically you should use try...finally to
   * call unlock in the finally block.
   */
  boolean tryLock() {
    return tryLock0(Thread.currentThread());
  }

  /**
   * Normal unLock, will use the current thread as owner. Typically you should use try...finally to
   * call unlock in the finally block.
   */
  void unlock() {
    unlock0(Thread.currentThread());
  }

  /**
   * Lock by a procedure. You can release the lock in another thread.
   */
  void lock(Procedure<?> proc) {
    lock0(proc);
  }

  /**
   * TryLock by a procedure. You can release the lock in another thread.
   */
  boolean tryLock(Procedure<?> proc) {
    return tryLock0(proc);
  }

  /**
   * Unlock by a procedure. You do not need to call this method in the same thread with lock.
   */
  void unlock(Procedure<?> proc) {
    unlock0(proc);
  }

  boolean isLocked() {
    lock.lock();
    try {
      return owner != null;
    } finally {
      lock.unlock();
    }
  }
}
