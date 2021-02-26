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

package org.apache.hadoop.hbase.procedure2;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic ProcedureEvent that contains an "object", which can be a description or a reference to the
 * resource to wait on, and a queue for suspended procedures.
 */
@InterfaceAudience.Private
public class ProcedureEvent<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureEvent.class);

  private final T object;
  private boolean ready = false;
  private ProcedureDeque suspendedProcedures = new ProcedureDeque();

  public ProcedureEvent(final T object) {
    this.object = object;
  }

  public synchronized boolean isReady() {
    return ready;
  }

  /**
   * @return true if event is not ready and adds procedure to suspended queue, else returns false.
   */
  public synchronized boolean suspendIfNotReady(Procedure proc) {
    if (!ready) {
      suspendedProcedures.addLast(proc);
    }
    return !ready;
  }

  /** Mark the event as not ready. */
  public synchronized void suspend() {
    ready = false;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Suspend " + toString());
    }
  }

  /**
   * Wakes up the suspended procedures by pushing them back into scheduler queues and sets the
   * event as ready.
   * See {@link #wakeInternal(AbstractProcedureScheduler)} for why this is not synchronized.
   */
  public void wake(AbstractProcedureScheduler procedureScheduler) {
    procedureScheduler.wakeEvents(new ProcedureEvent[]{this});
  }

  /**
   * Wakes up the suspended procedures only if the given {@code proc} is waiting on this event.
   * <p/>
   * Mainly used by region assignment to reject stale OpenRegionProcedure/CloseRegionProcedure. Use
   * with caution as it will cause performance issue if there are lots of procedures waiting on the
   * event.
   */
  public synchronized boolean wakeIfSuspended(AbstractProcedureScheduler procedureScheduler,
      Procedure<?> proc) {
    if (suspendedProcedures.stream().anyMatch(p -> p.getProcId() == proc.getProcId())) {
      wake(procedureScheduler);
      return true;
    }
    return false;
  }

  /**
   * Wakes up all the given events and puts the procedures waiting on them back into
   * ProcedureScheduler queues.
   */
  public static void wakeEvents(AbstractProcedureScheduler scheduler, ProcedureEvent ... events) {
    scheduler.wakeEvents(events);
  }

  /**
   * Only to be used by ProcedureScheduler implementations.
   * Reason: To wake up multiple events, locking sequence is
   * schedLock --> synchronized (event)
   * To wake up an event, both schedLock() and synchronized(event) are required.
   * The order is schedLock() --> synchronized(event) because when waking up multiple events
   * simultaneously, we keep the scheduler locked until all procedures suspended on these events
   * have been added back to the queue (Maybe it's not required? Evaluate!)
   * To avoid deadlocks, we want to keep the locking order same even when waking up single event.
   * That's why, {@link #wake(AbstractProcedureScheduler)} above uses the same code path as used
   * when waking up multiple events.
   * Access should remain package-private.
   */
  synchronized void wakeInternal(AbstractProcedureScheduler procedureScheduler) {
    if (ready && !suspendedProcedures.isEmpty()) {
      LOG.warn("Found procedures suspended in a ready event! Size=" + suspendedProcedures.size());
    }
    ready = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Unsuspend " + toString());
    }
    // wakeProcedure adds to the front of queue, so we start from last in the
    // waitQueue' queue, so that the procedure which was added first goes in the front for
    // the scheduler queue.
    procedureScheduler.addFront(suspendedProcedures.descendingIterator());
    suspendedProcedures.clear();
  }

  /**
   * Access to suspendedProcedures is 'synchronized' on this object, but it's fine to return it
   * here for tests.
   */
  public ProcedureDeque getSuspendedProcedures() {
    return suspendedProcedures;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " for " + object + ", ready=" + isReady() +
        ", " + suspendedProcedures;
  }
}
