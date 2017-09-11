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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * Base Procedure class responsible for Procedure Metadata;
 * e.g. state, submittedTime, lastUpdate, stack-indexes, etc.
 *
 * <p>Procedures are run by a {@link ProcedureExecutor} instance. They are submitted and then
 * the ProcedureExecutor keeps calling {@link #execute(Object)} until the Procedure is done.
 * Execute may be called multiple times in the case of failure or a restart, so code must be
 * idempotent. The return from an execute call is either: null to indicate we are done;
 * ourself if there is more to do; or, a set of sub-procedures that need to
 * be run to completion before the framework resumes our execution.
 *
 * <p>The ProcedureExecutor keeps its
 * notion of Procedure State in the Procedure itself; e.g. it stamps the Procedure as INITIALIZING,
 * RUNNABLE, SUCCESS, etc. Here are some of the States defined in the ProcedureState enum from
 * protos:
 *<ul>
 * <li>{@link #isFailed()} A procedure has executed at least once and has failed. The procedure
 * may or may not have rolled back yet. Any procedure in FAILED state will be eventually moved
 * to ROLLEDBACK state.</li>
 *
 * <li>{@link #isSuccess()} A procedure is completed successfully without exception.</li>
 *
 * <li>{@link #isFinished()} As a procedure in FAILED state will be tried forever for rollback, only
 * condition when scheduler/ executor will drop procedure from further processing is when procedure
 * state is ROLLEDBACK or isSuccess() returns true. This is a terminal state of the procedure.</li>
 *
 * <li>{@link #isWaiting()} - Procedure is in one of the two waiting states
 * ({@link ProcedureState#WAITING}, {@link ProcedureState#WAITING_TIMEOUT}).</li>
 *</ul>
 * NOTE: These states are of the ProcedureExecutor. Procedure implementations in turn can keep
 * their own state. This can lead to confusion. Try to keep the two distinct.
 *
 * <p>rollback() is called when the procedure or one of the sub-procedures
 * has failed. The rollback step is supposed to cleanup the resources created
 * during the execute() step. In case of failure and restart, rollback() may be
 * called multiple times, so again the code must be idempotent.
 *
 * <p>Procedure can be made respect a locking regime. It has acqure/release methods as
 * well as an {@link #hasLock(Object)}. The lock implementation is up to the implementor.
 * If an entity needs to be locked for the life of a procedure -- not just the calls to
 * execute -- then implementations should say so with the {@link #holdLock(Object)}
 * method.
 *
 * <p>There are hooks for collecting metrics on submit of the procedure and on finish.
 * See {@link #updateMetricsOnSubmit(Object)} and
 * {@link #updateMetricsOnFinish(Object, long, boolean)}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Procedure<TEnvironment> implements Comparable<Procedure<TEnvironment>> {
  private static final Log LOG = LogFactory.getLog(Procedure.class);
  public static final long NO_PROC_ID = -1;
  protected static final int NO_TIMEOUT = -1;

  public enum LockState {
    LOCK_ACQUIRED,       // Lock acquired and ready to execute
    LOCK_YIELD_WAIT,     // Lock not acquired, framework needs to yield
    LOCK_EVENT_WAIT,     // Lock not acquired, an event will yield the procedure
  }

  // Unchanged after initialization
  private NonceKey nonceKey = null;
  private String owner = null;
  private long parentProcId = NO_PROC_ID;
  private long rootProcId = NO_PROC_ID;
  private long procId = NO_PROC_ID;
  private long submittedTime;

  // Runtime state, updated every operation
  private ProcedureState state = ProcedureState.INITIALIZING;
  private RemoteProcedureException exception = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;

  private volatile int timeout = NO_TIMEOUT;
  private volatile long lastUpdate;

  private volatile byte[] result = null;

  /**
   * The main code of the procedure. It must be idempotent since execute()
   * may be called multiple times in case of machine failure in the middle
   * of the execution.
   * @param env the environment passed to the ProcedureExecutor
   * @return a set of sub-procedures to run or ourselves if there is more work to do or null if the
   * procedure is done.
   * @throws ProcedureYieldException the procedure will be added back to the queue and retried later.
   * @throws InterruptedException the procedure will be added back to the queue and retried later.
   * @throws ProcedureSuspendedException Signal to the executor that Procedure has suspended itself and
   * has set itself up waiting for an external event to wake it back up again.
   */
  protected abstract Procedure<TEnvironment>[] execute(TEnvironment env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException;

  /**
   * The code to undo what was done by the execute() code.
   * It is called when the procedure or one of the sub-procedures failed or an
   * abort was requested. It should cleanup all the resources created by
   * the execute() call. The implementation must be idempotent since rollback()
   * may be called multiple time in case of machine failure in the middle
   * of the execution.
   * @param env the environment passed to the ProcedureExecutor
   * @throws IOException temporary failure, the rollback will retry later
   * @throws InterruptedException the procedure will be added back to the queue and retried later
   */
  protected abstract void rollback(TEnvironment env)
    throws IOException, InterruptedException;

  /**
   * The abort() call is asynchronous and each procedure must decide how to deal
   * with it, if they want to be abortable. The simplest implementation
   * is to have an AtomicBoolean set in the abort() method and then the execute()
   * will check if the abort flag is set or not.
   * abort() may be called multiple times from the client, so the implementation
   * must be idempotent.
   *
   * <p>NOTE: abort() is not like Thread.interrupt(). It is just a notification
   * that allows the procedure implementor abort.
   */
  protected abstract boolean abort(TEnvironment env);

  /**
   * The user-level code of the procedure may have some state to
   * persist (e.g. input arguments or current position in the processing state) to
   * be able to resume on failure.
   * @param serializer stores the serializable state
   */
  protected abstract void serializeStateData(final ProcedureStateSerializer serializer)
    throws IOException;

  /**
   * Called on store load to allow the user to decode the previously serialized
   * state.
   * @param serializer contains the serialized state
   */
  protected abstract void deserializeStateData(final ProcedureStateSerializer serializer)
    throws IOException;

  /**
   * The user should override this method if they need a lock on an Entity.
   * A lock can be anything, and it is up to the implementor. The Procedure
   * Framework will call this method just before it invokes {@link #execute(Object)}.
   * It calls {@link #releaseLock(Object)} after the call to execute.
   *
   * <p>If you need to hold the lock for the life of the Procedure -- i.e. you do not
   * want any other Procedure interfering while this Procedure is running, see
   * {@link #holdLock(Object)}.
   *
   * <p>Example: in our Master we can execute request in parallel for different tables.
   * We can create t1 and create t2 and these creates can be executed at the same time.
   * Anything else on t1/t2 is queued waiting that specific table create to happen.
   *
   * <p>There are 3 LockState:
   * <ul><li>LOCK_ACQUIRED should be returned when the proc has the lock and the proc is
   * ready to execute.</li>
   * <li>LOCK_YIELD_WAIT should be returned when the proc has not the lock and the framework
   * should take care of readding the procedure back to the runnable set for retry</li>
   * <li>LOCK_EVENT_WAIT should be returned when the proc has not the lock and someone will
   * take care of readding the procedure back to the runnable set when the lock is available.
   * </li></ul>
   * @return the lock state as described above.
   */
  protected LockState acquireLock(final TEnvironment env) {
    return LockState.LOCK_ACQUIRED;
  }

  /**
   * The user should override this method, and release lock if necessary.
   */
  protected void releaseLock(final TEnvironment env) {
    // no-op
  }

  /**
   * Used to keep the procedure lock even when the procedure is yielding or suspended.
   * Must implement {@link #hasLock(Object)} if you want to hold the lock for life
   * of the Procedure.
   * @see #hasLock(Object)
   * @return true if the procedure should hold on the lock until completionCleanup()
   */
  protected boolean holdLock(final TEnvironment env) {
    return false;
  }

  /**
   * This is used in conjunction with {@link #holdLock(Object)}. If {@link #holdLock(Object)}
   * returns true, the procedure executor will call acquireLock() once and thereafter
   * not call {@link #releaseLock(Object)} until the Procedure is done (Normally, it calls
   * release/acquire around each invocation of {@link #execute(Object)}.
   * @see #holdLock(Object)
   * @return true if the procedure has the lock, false otherwise.
   */
  protected boolean hasLock(final TEnvironment env) {
    return false;
  }

  /**
   * Called when the procedure is loaded for replay.
   * The procedure implementor may use this method to perform some quick
   * operation before replay.
   * e.g. failing the procedure if the state on replay may be unknown.
   */
  protected void beforeReplay(final TEnvironment env) {
    // no-op
  }

  /**
   * Called when the procedure is ready to be added to the queue after
   * the loading/replay operation.
   */
  protected void afterReplay(final TEnvironment env) {
    // no-op
  }

  /**
   * Called when the procedure is marked as completed (success or rollback).
   * The procedure implementor may use this method to cleanup in-memory states.
   * This operation will not be retried on failure. If a procedure took a lock,
   * it will have been released when this method runs.
   */
  protected void completionCleanup(final TEnvironment env) {
    // no-op
  }

  /**
   * By default, the procedure framework/executor will try to run procedures start to finish.
   * Return true to make the executor yield between each execution step to
   * give other procedures a chance to run.
   * @param env the environment passed to the ProcedureExecutor
   * @return Return true if the executor should yield on completion of an execution step.
   *         Defaults to return false.
   */
  protected boolean isYieldAfterExecutionStep(final TEnvironment env) {
    return false;
  }

  /**
   * By default, the executor will keep the procedure result around util
   * the eviction TTL is expired. The client can cut down the waiting time
   * by requesting that the result is removed from the executor.
   * In case of system started procedure, we can force the executor to auto-ack.
   * @param env the environment passed to the ProcedureExecutor
   * @return true if the executor should wait the client ack for the result.
   *         Defaults to return true.
   */
  protected boolean shouldWaitClientAck(final TEnvironment env) {
    return true;
  }

  /**
   * Override this method to provide procedure specific counters for submitted count, failed
   * count and time histogram.
   * @param env The environment passed to the procedure executor
   * @return Container object for procedure related metric
   */
  protected ProcedureMetrics getProcedureMetrics(final TEnvironment env) {
    return null;
  }

  /**
   * This function will be called just when procedure is submitted for execution. Override this
   * method to update the metrics at the beginning of the procedure. The default implementation
   * updates submitted counter if {@link #getProcedureMetrics(Object)} returns non-null
   * {@link ProcedureMetrics}.
   */
  protected void updateMetricsOnSubmit(final TEnvironment env) {
    ProcedureMetrics metrics = getProcedureMetrics(env);
    if (metrics == null) {
      return;
    }

    Counter submittedCounter = metrics.getSubmittedCounter();
    if (submittedCounter != null) {
      submittedCounter.increment();
    }
  }

  /**
   * This function will be called just after procedure execution is finished. Override this method
   * to update metrics at the end of the procedure. If {@link #getProcedureMetrics(Object)}
   * returns non-null {@link ProcedureMetrics}, the default implementation adds runtime of a
   * procedure to a time histogram for successfully completed procedures. Increments failed
   * counter for failed procedures.
   *
   * TODO: As any of the sub-procedures on failure rolls back all procedures in the stack,
   * including successfully finished siblings, this function may get called twice in certain
   * cases for certain procedures. Explore further if this can be called once.
   *
   * @param env The environment passed to the procedure executor
   * @param runtime Runtime of the procedure in milliseconds
   * @param success true if procedure is completed successfully
   */
  protected void updateMetricsOnFinish(final TEnvironment env, final long runtime,
                                       boolean success) {
    ProcedureMetrics metrics = getProcedureMetrics(env);
    if (metrics == null) {
      return;
    }

    if (success) {
      Histogram timeHisto = metrics.getTimeHisto();
      if (timeHisto != null) {
        timeHisto.update(runtime);
      }
    } else {
      Counter failedCounter = metrics.getFailedCounter();
      if (failedCounter != null) {
        failedCounter.increment();
      }
    }
  }

  @Override
  public String toString() {
    // Return the simple String presentation of the procedure.
    return toStringSimpleSB().toString();
  }

  /**
   * Build the StringBuilder for the simple form of
   * procedure string.
   * @return the StringBuilder
   */
  protected StringBuilder toStringSimpleSB() {
    final StringBuilder sb = new StringBuilder();

    sb.append("pid=");
    sb.append(getProcId());

    if (hasParent()) {
      sb.append(", ppid=");
      sb.append(getParentProcId());
    }

    /**
     * TODO
     * Enable later when this is being used.
     * Currently owner not used.
    if (hasOwner()) {
      sb.append(", owner=");
      sb.append(getOwner());
    }*/

    sb.append(", state="); // pState for Procedure State as opposed to any other kind.
    toStringState(sb);

    if (hasException()) {
      sb.append(", exception=" + getException());
    }

    sb.append("; ");
    toStringClassDetails(sb);

    return sb;
  }

  /**
   * Extend the toString() information with more procedure
   * details
   */
  public String toStringDetails() {
    final StringBuilder sb = toStringSimpleSB();

    sb.append(" submittedTime=");
    sb.append(getSubmittedTime());

    sb.append(", lastUpdate=");
    sb.append(getLastUpdate());

    final int[] stackIndices = getStackIndexes();
    if (stackIndices != null) {
      sb.append("\n");
      sb.append("stackIndexes=");
      sb.append(Arrays.toString(stackIndices));
    }

    return sb.toString();
  }

  protected String toStringClass() {
    StringBuilder sb = new StringBuilder();
    toStringClassDetails(sb);
    return sb.toString();
  }

  /**
   * Called from {@link #toString()} when interpolating {@link Procedure} State.
   * Allows decorating generic Procedure State with Procedure particulars.
   * @param builder Append current {@link ProcedureState}
   */
  protected void toStringState(StringBuilder builder) {
    builder.append(getState());
  }

  /**
   * Extend the toString() information with the procedure details
   * e.g. className and parameters
   * @param builder the string builder to use to append the proc specific information
   */
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName());
  }

  // ==========================================================================
  //  Those fields are unchanged after initialization.
  //
  //  Each procedure will get created from the user or during
  //  ProcedureExecutor.start() during the load() phase and then submitted
  //  to the executor. these fields will never be changed after initialization
  // ==========================================================================
  public long getProcId() {
    return procId;
  }

  public boolean hasParent() {
    return parentProcId != NO_PROC_ID;
  }

  public long getParentProcId() {
    return parentProcId;
  }

  public long getRootProcId() {
    return rootProcId;
  }

  public String getProcName() {
    return toStringClass();
  }

  public NonceKey getNonceKey() {
    return nonceKey;
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  public String getOwner() {
    return owner;
  }

  public boolean hasOwner() {
    return owner != null;
  }

  /**
   * Called by the ProcedureExecutor to assign the ID to the newly created procedure.
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setProcId(final long procId) {
    this.procId = procId;
    this.submittedTime = EnvironmentEdgeManager.currentTime();
    setState(ProcedureState.RUNNABLE);
  }

  /**
   * Called by the ProcedureExecutor to assign the parent to the newly created procedure.
   */
  @InterfaceAudience.Private
  protected void setParentProcId(final long parentProcId) {
    this.parentProcId = parentProcId;
  }

  @InterfaceAudience.Private
  protected void setRootProcId(final long rootProcId) {
    this.rootProcId = rootProcId;
  }

  /**
   * Called by the ProcedureExecutor to set the value to the newly created procedure.
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setNonceKey(final NonceKey nonceKey) {
    this.nonceKey = nonceKey;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public void setOwner(final String owner) {
    this.owner = StringUtils.isEmpty(owner) ? null : owner;
  }

  public void setOwner(final User owner) {
    assert owner != null : "expected owner to be not null";
    setOwner(owner.getShortName());
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  protected void setSubmittedTime(final long submittedTime) {
    this.submittedTime = submittedTime;
  }

  // ==========================================================================
  //  runtime state - timeout related
  // ==========================================================================
  /**
   * @param timeout timeout interval in msec
   */
  protected void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  public boolean hasTimeout() {
    return timeout != NO_TIMEOUT;
  }

  /**
   * @return the timeout in msec
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  protected void setLastUpdate(final long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /**
   * Called by ProcedureExecutor after each time a procedure step is executed.
   */
  @InterfaceAudience.Private
  protected void updateTimestamp() {
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  /**
   * Timeout of the next timeout.
   * Called by the ProcedureExecutor if the procedure has timeout set and
   * the procedure is in the waiting queue.
   * @return the timestamp of the next timeout.
   */
  @InterfaceAudience.Private
  protected long getTimeoutTimestamp() {
    return getLastUpdate() + getTimeout();
  }

  // ==========================================================================
  //  runtime state
  // ==========================================================================
  /**
   * @return the time elapsed between the last update and the start time of the procedure.
   */
  public long elapsedTime() {
    return getLastUpdate() - getSubmittedTime();
  }

  /**
   * @return the serialized result if any, otherwise null
   */
  public byte[] getResult() {
    return result;
  }

  /**
   * The procedure may leave a "result" on completion.
   * @param result the serialized result that will be passed to the client
   */
  protected void setResult(final byte[] result) {
    this.result = result;
  }

  // ==============================================================================================
  //  Runtime state, updated every operation by the ProcedureExecutor
  //
  //  There is always 1 thread at the time operating on the state of the procedure.
  //  The ProcedureExecutor may check and set states, or some Procecedure may
  //  update its own state. but no concurrent updates. we use synchronized here
  //  just because the procedure can get scheduled on different executor threads on each step.
  // ==============================================================================================

  /**
   * @return true if the procedure is in a RUNNABLE state.
   */
  public synchronized boolean isRunnable() {
    return state == ProcedureState.RUNNABLE;
  }

  public synchronized boolean isInitializing() {
    return state == ProcedureState.INITIALIZING;
  }

  /**
   * @return true if the procedure has failed. It may or may not have rolled back.
   */
  public synchronized boolean isFailed() {
    return state == ProcedureState.FAILED || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is finished successfully.
   */
  public synchronized boolean isSuccess() {
    return state == ProcedureState.SUCCESS && !hasException();
  }

  /**
   * @return true if the procedure is finished. The Procedure may be completed successfully or
   * rolledback.
   */
  public synchronized boolean isFinished() {
    return isSuccess() || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is waiting for a child to finish or for an external event.
   */
  public synchronized boolean isWaiting() {
    switch (state) {
      case WAITING:
      case WAITING_TIMEOUT:
        return true;
      default:
        break;
    }
    return false;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  protected synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  @InterfaceAudience.Private
  public synchronized ProcedureState getState() {
    return state;
  }

  protected void setFailure(final String source, final Throwable cause) {
    setFailure(new RemoteProcedureException(source, cause));
  }

  protected synchronized void setFailure(final RemoteProcedureException exception) {
    this.exception = exception;
    if (!isFinished()) {
      setState(ProcedureState.FAILED);
    }
  }

  protected void setAbortFailure(final String source, final String msg) {
    setFailure(source, new ProcedureAbortedException(msg));
  }

  /**
   * Called by the ProcedureExecutor when the timeout set by setTimeout() is expired.
   * @return true to let the framework handle the timeout as abort,
   *         false in case the procedure handled the timeout itself.
   */
  protected synchronized boolean setTimeoutFailure(final TEnvironment env) {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = EnvironmentEdgeManager.currentTime() - lastUpdate;
      setFailure("ProcedureExecutor", new TimeoutIOException(
        "Operation timed out after " + StringUtils.humanTimeDiff(timeDiff)));
      return true;
    }
    return false;
  }

  public synchronized boolean hasException() {
    return exception != null;
  }

  public synchronized RemoteProcedureException getException() {
    return exception;
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  protected synchronized void setChildrenLatch(final int numChildren) {
    this.childrenLatch = numChildren;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT SET " +
          this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  protected synchronized void incChildrenLatch() {
    // TODO: can this be inferred from the stack? I think so...
    this.childrenLatch++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT " + this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /**
   * Called by the ProcedureExecutor to notify that one of the sub-procedures has completed.
   */
  @InterfaceAudience.Private
  private synchronized boolean childrenCountDown() {
    assert childrenLatch > 0: this;
    boolean b = --childrenLatch == 0;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH DECREMENT " + childrenLatch, new Throwable(this.toString()));
    }
    return b;
  }

  /**
   * Try to set this procedure into RUNNABLE state.
   * Succeeds if all subprocedures/children are done.
   * @return True if we were able to move procedure to RUNNABLE state.
   */
  synchronized boolean tryRunnable() {
    // Don't use isWaiting in the below; it returns true for WAITING and WAITING_TIMEOUT
    boolean b = getState() == ProcedureState.WAITING && childrenCountDown();
    if (b) setState(ProcedureState.RUNNABLE);
    return b;
  }

  @InterfaceAudience.Private
  protected synchronized boolean hasChildren() {
    return childrenLatch > 0;
  }

  @InterfaceAudience.Private
  protected synchronized int getChildrenLatch() {
    return childrenLatch;
  }

  /**
   * Called by the RootProcedureState on procedure execution.
   * Each procedure store its stack-index positions.
   */
  @InterfaceAudience.Private
  protected synchronized void addStackIndex(final int index) {
    if (stackIndexes == null) {
      stackIndexes = new int[] { index };
    } else {
      int count = stackIndexes.length;
      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
      stackIndexes[count] = index;
    }
  }

  @InterfaceAudience.Private
  protected synchronized boolean removeStackIndex() {
    if (stackIndexes != null && stackIndexes.length > 1) {
      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);
      return false;
    } else {
      stackIndexes = null;
      return true;
    }
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  protected synchronized void setStackIndexes(final List<Integer> stackIndexes) {
    this.stackIndexes = new int[stackIndexes.size()];
    for (int i = 0; i < this.stackIndexes.length; ++i) {
      this.stackIndexes[i] = stackIndexes.get(i);
    }
  }

  @InterfaceAudience.Private
  protected synchronized boolean wasExecuted() {
    return stackIndexes != null;
  }

  @InterfaceAudience.Private
  protected synchronized int[] getStackIndexes() {
    return stackIndexes;
  }

  // ==========================================================================
  //  Internal methods - called by the ProcedureExecutor
  // ==========================================================================

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code execute().
   * @throws ProcedureSuspendedException This is used when procedure wants to halt processing and
   * skip out without changing states or releasing any locks held.
   */
  @InterfaceAudience.Private
  protected Procedure<TEnvironment>[] doExecute(final TEnvironment env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    try {
      updateTimestamp();
      return execute(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code rollback().
   */
  @InterfaceAudience.Private
  protected void doRollback(final TEnvironment env)
      throws IOException, InterruptedException {
    try {
      updateTimestamp();
      rollback(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code acquireLock().
   */
  @InterfaceAudience.Private
  protected LockState doAcquireLock(final TEnvironment env) {
    return acquireLock(env);
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code releaseLock().
   */
  @InterfaceAudience.Private
  protected void doReleaseLock(final TEnvironment env) {
    releaseLock(env);
  }

  @Override
  public int compareTo(final Procedure<TEnvironment> other) {
    return Long.compare(getProcId(), other.getProcId());
  }

  // ==========================================================================
  //  misc utils
  // ==========================================================================

  /**
   * Get an hashcode for the specified Procedure ID
   * @return the hashcode for the specified procId
   */
  public static long getProcIdHashCode(final long procId) {
    long h = procId;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
  }

  /*
   * Helper to lookup the root Procedure ID given a specified procedure.
   */
  @InterfaceAudience.Private
  protected static Long getRootProcedureId(final Map<Long, Procedure> procedures,
      Procedure<?> proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) return null;
    }
    return proc.getProcId();
  }

  /**
   * @param a the first procedure to be compared.
   * @param b the second procedure to be compared.
   * @return true if the two procedures have the same parent
   */
  public static boolean haveSameParent(final Procedure<?> a, final Procedure<?> b) {
    return a.hasParent() && b.hasParent() && (a.getParentProcId() == b.getParentProcId());
  }
}
