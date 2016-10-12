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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractProcedureScheduler implements ProcedureScheduler {
  private static final Log LOG = LogFactory.getLog(AbstractProcedureScheduler.class);

  private final ReentrantLock schedLock = new ReentrantLock();
  private final Condition schedWaitCond = schedLock.newCondition();
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

  public void addFront(final Procedure procedure) {
    push(procedure, true, true);
  }

  public void addBack(final Procedure procedure) {
    push(procedure, false, true);
  }

  protected void push(final Procedure procedure, final boolean addFront, final boolean notify) {
    schedLock.lock();
    try {
      enqueue(procedure, addFront);
      if (notify) {
        schedWaitCond.signal();
      }
    } finally {
      schedLock.unlock();
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

  public Procedure poll(long nanos) {
    final boolean waitForever = (nanos < 0);
    schedLock();
    try {
      while (!queueHasRunnables()) {
        if (!running) return null;
        if (waitForever) {
          schedWaitCond.await();
        } else {
          if (nanos <= 0) return null;
          nanos = schedWaitCond.awaitNanos(nanos);
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
   * Removes all of the elements from the queue
   * NOTE: this method is called with the sched lock held.
   */
  protected abstract void clearQueue();

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
  public void clear() {
    // NOTE: USED ONLY FOR TESTING
    schedLock();
    try {
      clearQueue();
    } finally {
      schedUnlock();
    }
  }

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
  @Override
  public boolean waitEvent(final ProcedureEvent event, final Procedure procedure) {
    synchronized (event) {
      if (event.isReady()) {
        return false;
      }
      suspendProcedure(event, procedure);
      return true;
    }
  }

  @Override
  public void suspendEvent(final ProcedureEvent event) {
    final boolean isTraceEnabled = LOG.isTraceEnabled();
    synchronized (event) {
      event.setReady(false);
      if (isTraceEnabled) {
        LOG.trace("Suspend event " + event);
      }
    }
  }

  @Override
  public void wakeEvent(final ProcedureEvent event) {
    wakeEvents(1, event);
  }

  @Override
  public void wakeEvents(final int count, final ProcedureEvent... events) {
    final boolean isTraceEnabled = LOG.isTraceEnabled();
    schedLock();
    try {
      int waitingCount = 0;
      for (int i = 0; i < count; ++i) {
        final ProcedureEvent event = events[i];
        synchronized (event) {
          event.setReady(true);
          if (isTraceEnabled) {
            LOG.trace("Wake event " + event);
          }
          waitingCount += popEventWaitingObjects(event);
        }
      }
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  protected int popEventWaitingObjects(final ProcedureEvent event) {
    return popEventWaitingProcedures(event);
  }

  protected int popEventWaitingProcedures(final ProcedureEventQueue event) {
    int count = 0;
    while (event.hasWaitingProcedures()) {
      wakeProcedure(event.popWaitingProcedure(false));
      count++;
    }
    return count;
  }

  protected void suspendProcedure(final ProcedureEventQueue event, final Procedure procedure) {
    procedure.suspend();
    event.suspendProcedure(procedure);
  }

  protected void wakeProcedure(final Procedure procedure) {
    procedure.resume();
    push(procedure, /* addFront= */ true, /* notify= */false);
  }

  // ==========================================================================
  //  Internal helpers
  // ==========================================================================
  protected void schedLock() {
    schedLock.lock();
  }

  protected void schedUnlock() {
    schedLock.unlock();
  }

  protected void wakePollIfNeeded(final int waitingCount) {
    if (waitingCount > 1) {
      schedWaitCond.signalAll();
    } else if (waitingCount > 0) {
      schedWaitCond.signal();
    }
  }
}
