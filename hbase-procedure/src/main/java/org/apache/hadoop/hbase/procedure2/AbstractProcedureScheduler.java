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
package org.apache.hadoop.hbase.procedure2;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class AbstractProcedureScheduler implements ProcedureScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractProcedureScheduler.class);
  private final ReentrantLock schedulerLock = new ReentrantLock();
  private final Condition schedWaitCond = schedulerLock.newCondition();
  private boolean running = false;

  // TODO: metrics
  private long pollCalls = 0;
  private long nullPollCalls = 0;

  @Override
  public void start() {
    schedLock();
    try {
      running = true;
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void stop() {
    schedLock();
    try {
      running = false;
      schedWaitCond.signalAll();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void signalAll() {
    schedLock();
    try {
      schedWaitCond.signalAll();
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Add related
  // ==========================================================================
  /**
   * Add the procedure to the queue.
   * NOTE: this method is called with the sched lock held.
   * @param procedure the Procedure to add
   * @param addFront true if the item should be added to the front of the queue
   */
  protected abstract void enqueue(Procedure procedure, boolean addFront);

  @Override
  public void addFront(final Procedure procedure) {
    push(procedure, true, true);
  }

  @Override
  public void addFront(final Procedure procedure, boolean notify) {
    push(procedure, true, notify);
  }

  @Override
  public void addFront(Iterator<Procedure> procedureIterator) {
    schedLock();
    try {
      int count = 0;
      while (procedureIterator.hasNext()) {
        Procedure procedure = procedureIterator.next();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Wake " + procedure);
        }
        push(procedure, /* addFront= */ true, /* notify= */false);
        count++;
      }
      wakePollIfNeeded(count);
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void addBack(final Procedure procedure) {
    push(procedure, false, true);
  }

  @Override
  public void addBack(final Procedure procedure, boolean notify) {
    push(procedure, false, notify);
  }

  protected void push(final Procedure procedure, final boolean addFront, final boolean notify) {
    schedLock();
    try {
      enqueue(procedure, addFront);
      if (notify) {
        schedWaitCond.signal();
      }
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Poll related
  // ==========================================================================
  /**
   * Fetch one Procedure from the queue
   * NOTE: this method is called with the sched lock held.
   * @return the Procedure to execute, or null if nothing is available.
   */
  protected abstract Procedure dequeue();

  @Override
  public Procedure poll() {
    return poll(-1);
  }

  @Override
  public Procedure poll(long timeout, TimeUnit unit) {
    return poll(unit.toNanos(timeout));
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  public Procedure poll(final long nanos) {
    schedLock();
    try {
      if (!running) {
        LOG.debug("the scheduler is not running");
        return null;
      }

      if (!queueHasRunnables()) {
        // WA_AWAIT_NOT_IN_LOOP: we are not in a loop because we want the caller
        // to take decisions after a wake/interruption.
        if (nanos < 0) {
          schedWaitCond.await();
        } else {
          schedWaitCond.awaitNanos(nanos);
        }
        if (!queueHasRunnables()) {
          nullPollCalls++;
          return null;
        }
      }
      final Procedure pollResult = dequeue();

      pollCalls++;
      nullPollCalls += (pollResult == null) ? 1 : 0;
      return pollResult;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      nullPollCalls++;
      return null;
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Utils
  // ==========================================================================
  /**
   * Returns the number of elements in this queue.
   * NOTE: this method is called with the sched lock held.
   * @return the number of elements in this queue.
   */
  protected abstract int queueSize();

  /**
   * Returns true if there are procedures available to process.
   * NOTE: this method is called with the sched lock held.
   * @return true if there are procedures available to process, otherwise false.
   */
  protected abstract boolean queueHasRunnables();

  @Override
  public int size() {
    schedLock();
    try {
      return queueSize();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public boolean hasRunnables() {
    schedLock();
    try {
      return queueHasRunnables();
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  TODO: Metrics
  // ============================================================================
  public long getPollCalls() {
    return pollCalls;
  }

  public long getNullPollCalls() {
    return nullPollCalls;
  }

  // ==========================================================================
  //  Procedure Events
  // ==========================================================================

  /**
   * Wake up all of the given events.
   * Note that we first take scheduler lock and then wakeInternal() synchronizes on the event.
   * Access should remain package-private. Use ProcedureEvent class to wake/suspend events.
   * @param events the list of events to wake
   */
  void wakeEvents(ProcedureEvent[] events) {
    schedLock();
    try {
      for (ProcedureEvent event : events) {
        if (event == null) {
          continue;
        }
        event.wakeInternal(this);
      }
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wakes up given waiting procedures by pushing them back into scheduler queues.
   * @return size of given {@code waitQueue}.
   */
  protected int wakeWaitingProcedures(LockAndQueue lockAndQueue) {
    return lockAndQueue.wakeWaitingProcedures(this);
  }

  protected void waitProcedure(LockAndQueue lockAndQueue, final Procedure proc) {
    lockAndQueue.addLast(proc);
  }

  protected void wakeProcedure(final Procedure procedure) {
    LOG.trace("Wake {}", procedure);
    push(procedure, /* addFront= */ true, /* notify= */false);
  }

  // ==========================================================================
  //  Internal helpers
  // ==========================================================================
  protected void schedLock() {
    schedulerLock.lock();
  }

  protected void schedUnlock() {
    schedulerLock.unlock();
  }

  protected void wakePollIfNeeded(final int waitingCount) {
    if (waitingCount <= 0) {
      return;
    }
    if (waitingCount == 1) {
      schedWaitCond.signal();
    } else {
      schedWaitCond.signalAll();
    }
  }
}
