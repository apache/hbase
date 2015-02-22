/**
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
package org.apache.hadoop.hbase;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * ScheduledChore is a task performed on a period in hbase. ScheduledChores become active once
 * scheduled with a {@link ChoreService} via {@link ChoreService#scheduleChore(ScheduledChore)}. The
 * chore is run in a {@link ScheduledThreadPoolExecutor} and competes with other ScheduledChores for
 * access to the threads in the core thread pool. If an unhandled exception occurs, the chore
 * cancellation is logged. Implementers should consider whether or not the Chore will be able to
 * execute within the defined period. It is bad practice to define a ScheduledChore whose execution
 * time exceeds its period since it will try to hog one of the threads in the {@link ChoreService}'s
 * thread pool.
 * <p>
 * Don't subclass ScheduledChore if the task relies on being woken up for something to do, such as
 * an entry being added to a queue, etc.
 */
@InterfaceAudience.Private
public abstract class ScheduledChore implements Runnable {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final String name;

  /**
   * Default values for scheduling parameters should they be excluded during construction
   */
  private final static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  private final static long DEFAULT_INITIAL_DELAY = 0;

  /**
   * Scheduling parameters. Used by ChoreService when scheduling the chore to run periodically
   */
  private final int period;
  private final TimeUnit timeUnit;
  private final long initialDelay;

  /**
   * Interface to the ChoreService that this ScheduledChore is scheduled with. null if the chore is
   * not scheduled.
   */
  private ChoreServicer choreServicer;

  /**
   * Variables that encapsulate the meaningful state information
   */
  private long timeOfLastRun = -1;
  private long timeOfThisRun = -1;
  private boolean initialChoreComplete = false;

  /**
   * A means by which a ScheduledChore can be stopped. Once a chore recognizes that it has been
   * stopped, it will cancel itself. This is particularly useful in the case where a single stopper
   * instance is given to multiple chores. In such a case, a single {@link Stoppable#stop(String)}
   * command can cause many chores to stop together.
   */
  private final Stoppable stopper;

  interface ChoreServicer {
    /**
     * Cancel any ongoing schedules that this chore has with the implementer of this interface.
     */
    public void cancelChore(ScheduledChore chore);
    public void cancelChore(ScheduledChore chore, boolean mayInterruptIfRunning);

    /**
     * @return true when the chore is scheduled with the implementer of this interface
     */
    public boolean isChoreScheduled(ScheduledChore chore);

    /**
     * This method tries to execute the chore immediately. If the chore is executing at the time of
     * this call, the chore will begin another execution as soon as the current execution finishes
     * <p>
     * If the chore is not scheduled with a ChoreService, this call will fail.
     * @return false when the chore could not be triggered immediately
     */
    public boolean triggerNow(ScheduledChore chore);

    /**
     * A callback that tells the implementer of this interface that one of the scheduled chores is
     * missing its start time. The implication of a chore missing its start time is that the
     * service's current means of scheduling may not be sufficient to handle the number of ongoing
     * chores (the other explanation is that the chore's execution time is greater than its
     * scheduled period). The service should try to increase its concurrency when this callback is
     * received.
     * @param chore The chore that missed its start time
     */
    public void onChoreMissedStartTime(ScheduledChore chore);
  }

  /**
   * This constructor is for test only. It allows us to create an object and to call chore() on it.
   */
  protected ScheduledChore() {
    this.name = null;
    this.stopper = null;
    this.period = 0;
    this.initialDelay = DEFAULT_INITIAL_DELAY;
    this.timeUnit = DEFAULT_TIME_UNIT;
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period with which this Chore repeats execution when scheduled.
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period) {
    this(name, stopper, period, DEFAULT_INITIAL_DELAY);
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period with which this Chore repeats execution when scheduled.
   * @param initialDelay Delay before this Chore begins to execute once it has been scheduled. A
   *          value of 0 means the chore will begin to execute immediately. Negative delays are
   *          invalid and will be corrected to a value of 0.
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period,
      final long initialDelay) {
    this(name, stopper, period, initialDelay, DEFAULT_TIME_UNIT);
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period with which this Chore repeats execution when scheduled.
   * @param initialDelay Delay before this Chore begins to execute once it has been scheduled. A
   *          value of 0 means the chore will begin to execute immediately. Negative delays are
   *          invalid and will be corrected to a value of 0.
   * @param unit The unit that is used to measure period and initialDelay
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period,
      final long initialDelay, final TimeUnit unit) {
    this.name = name;
    this.stopper = stopper;
    this.period = period;
    this.initialDelay = initialDelay < 0 ? 0 : initialDelay;
    this.timeUnit = unit;
  }

  synchronized void resetState() {
    timeOfLastRun = -1;
    timeOfThisRun = -1;
    initialChoreComplete = false;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public synchronized void run() {
    timeOfLastRun = timeOfThisRun;
    timeOfThisRun = System.currentTimeMillis();
    if (missedStartTime() && isScheduled()) {
      choreServicer.onChoreMissedStartTime(this);
      if (LOG.isInfoEnabled()) LOG.info("Chore: " + getName() + " missed its start time");
    } else if (stopper.isStopped() || choreServicer == null || !isScheduled()) {
      cancel();
      cleanup();
      LOG.info("Chore: " + getName() + " was stopped");
    } else {
      try {
        if (!initialChoreComplete) {
          initialChoreComplete = initialChore();
        } else {
          chore();
        }
      } catch (Throwable t) {
        LOG.error("Caught error", t);
        if (this.stopper.isStopped()) {
          cancel();
          cleanup();
        }
      }
    }
  }

  /**
   * @return How long has it been since this chore last run. Useful for checking if the chore has
   *         missed its scheduled start time by too large of a margin
   */
  synchronized long getTimeBetweenRuns() {
    return timeOfThisRun - timeOfLastRun;
  }

  /**
   * @return true when the time between runs exceeds the acceptable threshold
   */
  private synchronized boolean missedStartTime() {
    return isValidTime(timeOfLastRun) && isValidTime(timeOfThisRun)
        && getTimeBetweenRuns() > getMaximumAllowedTimeBetweenRuns();
  }

  private synchronized double getMaximumAllowedTimeBetweenRuns() {
    // Threshold used to determine if the Chore's current run started too late
    return 1.5 * period;
  }

  private synchronized boolean isValidTime(final long time) {
    return time > 0 && time <= System.currentTimeMillis();
  }

  /**
   * @return false when the Chore is not currently scheduled with a ChoreService
   */
  public synchronized boolean triggerNow() {
    if (choreServicer != null) {
      return choreServicer.triggerNow(this);
    } else {
      return false;
    }
  }

  synchronized void setChoreServicer(ChoreServicer service) {
    // Chores should only ever be scheduled with a single ChoreService. If the choreServicer
    // is changing, cancel any existing schedules of this chore.
    if (choreServicer != null && choreServicer != service) {
      choreServicer.cancelChore(this, false);
    }
    choreServicer = service;
    timeOfThisRun = System.currentTimeMillis();
  }

  public synchronized void cancel() {
    cancel(false);
  }

  public synchronized void cancel(boolean mayInterruptIfRunning) {
    if (isScheduled()) choreServicer.cancelChore(this, mayInterruptIfRunning);

    choreServicer = null;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized Stoppable getStopper() {
    return stopper;
  }

  public synchronized int getPeriod() {
    return period;
  }

  public synchronized long getInitialDelay() {
    return initialDelay;
  }

  public final synchronized TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public synchronized boolean isInitialChoreComplete() {
    return initialChoreComplete;
  }

  @VisibleForTesting
  synchronized ChoreServicer getChoreServicer() {
    return choreServicer;
  }

  @VisibleForTesting
  synchronized long getTimeOfLastRun() {
    return timeOfLastRun;
  }

  @VisibleForTesting
  synchronized long getTimeOfThisRun() {
    return timeOfThisRun;
  }

  /**
   * @return true when this Chore is scheduled with a ChoreService
   */
  public synchronized boolean isScheduled() {
    return choreServicer != null && choreServicer.isChoreScheduled(this);
  }

  @VisibleForTesting
  public synchronized void choreForTesting() {
    chore();
  }

  /**
   * The task to execute on each scheduled execution of the Chore
   */
  protected abstract void chore();

  /**
   * Override to run a task before we start looping.
   * @return true if initial chore was successful
   */
  protected synchronized boolean initialChore() {
    // Default does nothing
    return true;
  }

  /**
   * Override to run cleanup tasks when the Chore encounters an error and must stop running
   */
  protected synchronized void cleanup() {
  }
}
