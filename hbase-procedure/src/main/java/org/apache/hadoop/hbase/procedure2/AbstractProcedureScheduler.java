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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractProcedureScheduler implements ProcedureScheduler {
  private static final Log LOG = LogFactory.getLog(AbstractProcedureScheduler.class);
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

  public void addFront(final Procedure procedure) {
    push(procedure, true, true);
  }

  public void addBack(final Procedure procedure) {
    push(procedure, false, true);
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
  @Override
  public boolean waitEvent(final ProcedureEvent event, final Procedure procedure) {
    synchronized (event) {
      if (event.isReady()) {
        return false;
      }
      waitProcedure(event.getSuspendedProcedures(), procedure);
      return true;
    }
  }

  @Override
  public void suspendEvent(final ProcedureEvent event) {
    final boolean traceEnabled = LOG.isTraceEnabled();
    synchronized (event) {
      event.setReady(false);
      if (traceEnabled) {
        LOG.trace("Suspend " + event);
      }
    }
  }

  @Override
  public void wakeEvent(final ProcedureEvent event) {
    wakeEvents(1, event);
  }

  @Override
  public void wakeEvents(final int count, final ProcedureEvent... events) {
    final boolean traceEnabled = LOG.isTraceEnabled();
    schedLock();
    try {
      int waitingCount = 0;
      for (int i = 0; i < count; ++i) {
        final ProcedureEvent event = events[i];
        synchronized (event) {
          if (!event.isReady()) {
            // Only set ready if we were not ready; i.e. suspended. Otherwise, we double-wake
            // on this event and down in wakeWaitingProcedures, we double decrement this
            // finish which messes up child procedure accounting.
            event.setReady(true);
            if (traceEnabled) {
              LOG.trace("Unsuspend " + event);
            }
            waitingCount += wakeWaitingProcedures(event.getSuspendedProcedures());
          } else {
            ProcedureDeque q = event.getSuspendedProcedures();
            if (q != null && !q.isEmpty()) {
              LOG.warn("Q is not empty! size=" + q.size() + "; PROCESSING...");
              waitingCount += wakeWaitingProcedures(event.getSuspendedProcedures());
            }
          }
        }
      }
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wakes up given waiting procedures by pushing them back into scheduler queues.
   * @return size of given {@code waitQueue}.
   */
  protected int wakeWaitingProcedures(final ProcedureDeque waitQueue) {
    int count = waitQueue.size();
    // wakeProcedure adds to the front of queue, so we start from last in the
    // waitQueue' queue, so that the procedure which was added first goes in the front for
    // the scheduler queue.
    while (!waitQueue.isEmpty()) {
      wakeProcedure(waitQueue.removeLast());
    }
    return count;
  }

  protected void waitProcedure(final ProcedureDeque waitQueue, final Procedure proc) {
    waitQueue.addLast(proc);
  }

  protected void wakeProcedure(final Procedure procedure) {
    if (LOG.isTraceEnabled()) LOG.trace("Wake " + procedure);
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
    if (waitingCount <= 0) return;
    if (waitingCount == 1) {
      schedWaitCond.signal();
    } else {
      schedWaitCond.signalAll();
    }
  }
}
