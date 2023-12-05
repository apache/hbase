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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureFutureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A lock implementation which supports unlock by another thread.
 * <p>
 * This is because we need to hold region state node lock while updating region state to meta(for
 * keeping consistency), so it is better to yield the procedure to release the procedure worker. But
 * after waking up the procedure, we may use another procedure worker to execute the procedure,
 * which means we need to unlock by another thread.
 * <p>
 * For locking by procedure, we will also suspend the procedure if the lock is not ready, and
 * schedule it again when lock is ready. This is very important to not block the PEWorker as we may
 * hold the lock when updating meta, which could take a lot of time.
 * <p>
 * Please see HBASE-28196 for more details.
 */
@InterfaceAudience.Private
class RegionStateNodeLock {

  // for better logging message
  private final RegionInfo regionInfo;

  private final Lock lock = new ReentrantLock();

  private final Queue<QueueEntry> waitingQueue = new ArrayDeque<>();

  private Object owner;

  private int count;

  /**
   * This is for abstraction the common lock/unlock logic for both Thread and Procedure.
   */
  private interface QueueEntry {

    /**
     * A thread, or a procedure.
     */
    Object getOwner();

    /**
     * Called when we can not hold the lock and should wait. For thread, you should wait on a
     * condition, and for procedure, a ProcedureSuspendedException should be thrown.
     */
    void await() throws ProcedureSuspendedException;

    /**
     * Called when it is your turn to get the lock. For thread, just delegate the call to
     * condition's signal method, and for procedure, you should call the {@code wakeUp} action, to
     * add the procedure back to procedure scheduler.
     */
    void signal();
  }

  RegionStateNodeLock(RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  private void lock0(QueueEntry entry) throws ProcedureSuspendedException {
    lock.lock();
    try {
      for (;;) {
        if (owner == null) {
          owner = entry.getOwner();
          count = 1;
          return;
        }
        if (owner == entry.getOwner()) {
          count++;
          return;
        }
        waitingQueue.add(entry);
        entry.await();
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
        QueueEntry entry = waitingQueue.poll();
        if (entry != null) {
          entry.signal();
        }
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
    Thread currentThread = Thread.currentThread();
    try {
      lock0(new QueueEntry() {

        private Condition cond;

        @Override
        public void signal() {
          cond.signal();
        }

        @Override
        public Object getOwner() {
          return currentThread;
        }

        @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_AWAIT_NOT_IN_LOOP",
            justification = "Loop is in the caller method")
        @Override
        public void await() {
          if (cond == null) {
            cond = lock.newCondition();
          }
          cond.awaitUninterruptibly();
        }
      });
    } catch (ProcedureSuspendedException e) {
      // should not happen
      throw new AssertionError(e);
    }
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
   * <p>
   * When the procedure can not get the lock immediately, a ProcedureSuspendedException will be
   * thrown to suspend the procedure. And when we want to wake up a procedure, we will call the
   * {@code wakeUp} action. Usually in the {@code wakeUp} action you should add the procedure back
   * to procedure scheduler.
   */
  void lock(Procedure<?> proc, Runnable wakeUp) throws ProcedureSuspendedException {
    lock0(new QueueEntry() {

      @Override
      public Object getOwner() {
        return proc;
      }

      @Override
      public void await() throws ProcedureSuspendedException {
        ProcedureFutureUtil.suspend(proc);
      }

      @Override
      public void signal() {
        // Here we need to set the owner to the procedure directly.
        // For thread, we just block inside the lock0 method, so after signal we will continue and
        // get the lock
        // For procedure, the waking up here is actually a reschedule of the procedure to
        // ProcedureExecutor, so here we need to set the owner first, and it is the procedure's duty
        // to make sure that it has already hold the lock so do not need to call lock again, usually
        // this should be done by calling isLockedBy method below
        assert owner == null;
        assert count == 0;
        owner = proc;
        count = 1;
        wakeUp.run();
      }
    });
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

  /**
   * Check whether the lock is locked by someone.
   */
  boolean isLocked() {
    lock.lock();
    try {
      return owner != null;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Check whether the lock is locked by the given {@code lockBy}.
   */
  boolean isLockedBy(Object lockBy) {
    lock.lock();
    try {
      return owner == lockBy;
    } finally {
      lock.unlock();
    }
  }
}
