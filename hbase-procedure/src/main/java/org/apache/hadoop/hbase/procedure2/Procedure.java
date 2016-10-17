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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;

import com.google.common.annotations.VisibleForTesting;

/**
 * Base Procedure class responsible to handle the Procedure Metadata
 * e.g. state, startTime, lastUpdate, stack-indexes, ...
 *
 * execute() is called each time the procedure is executed.
 * it may be called multiple times in case of failure and restart, so the
 * code must be idempotent.
 * the return is a set of sub-procedures or null in case the procedure doesn't
 * have sub-procedures. Once the sub-procedures are successfully completed
 * the execute() method is called again, you should think at it as a stack:
 *  -&gt; step 1
 *  ---&gt; step 2
 *  -&gt; step 1
 *
 * rollback() is called when the procedure or one of the sub-procedures is failed.
 * the rollback step is supposed to cleanup the resources created during the
 * execute() step. in case of failure and restart rollback() may be called
 * multiple times, so the code must be idempotent.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Procedure<TEnvironment> implements Comparable<Procedure> {
  protected static final long NO_PROC_ID = -1;
  protected static final int NO_TIMEOUT = -1;

  // unchanged after initialization
  private NonceKey nonceKey = null;
  private String owner = null;
  private long parentProcId = NO_PROC_ID;
  private long rootProcId = NO_PROC_ID;
  private long procId = NO_PROC_ID;
  private long startTime;

  // runtime state, updated every operation
  private ProcedureState state = ProcedureState.INITIALIZING;
  private RemoteProcedureException exception = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;

  private volatile int timeout = NO_TIMEOUT;
  private volatile long lastUpdate;

  private volatile byte[] result = null;

  // TODO: it will be nice having pointers to allow the scheduler doing suspend/resume tricks
  private boolean suspended = false;

  /**
   * The main code of the procedure. It must be idempotent since execute()
   * may be called multiple time in case of machine failure in the middle
   * of the execution.
   * @param env the environment passed to the ProcedureExecutor
   * @return a set of sub-procedures or null if there is nothing else to execute.
   * @throws ProcedureYieldException the procedure will be added back to the queue and retried later
   * @throws InterruptedException the procedure will be added back to the queue and retried later
   */
  protected abstract Procedure[] execute(TEnvironment env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException;

  /**
   * The code to undo what done by the execute() code.
   * It is called when the procedure or one of the sub-procedure failed or an
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
   * with that, if they want to be abortable. The simplest implementation
   * is to have an AtomicBoolean set in the abort() method and then the execute()
   * will check if the abort flag is set or not.
   * abort() may be called multiple times from the client, so the implementation
   * must be idempotent.
   *
   * NOTE: abort() is not like Thread.interrupt() it is just a notification
   * that allows the procedure implementor where to abort to avoid leak and
   * have a better control on what was executed and what not.
   */
  protected abstract boolean abort(TEnvironment env);

  /**
   * The user-level code of the procedure may have some state to
   * persist (e.g. input arguments) to be able to resume on failure.
   * @param stream the stream that will contain the user serialized data
   */
  protected abstract void serializeStateData(final OutputStream stream)
    throws IOException;

  /**
   * Called on store load to allow the user to decode the previously serialized
   * state.
   * @param stream the stream that contains the user serialized data
   */
  protected abstract void deserializeStateData(final InputStream stream)
    throws IOException;

  /**
   * The user should override this method, and try to take a lock if necessary.
   * A lock can be anything, and it is up to the implementor.
   * Example: in our Master we can execute request in parallel for different tables
   *          create t1 and create t2 can be executed at the same time.
   *          anything else on t1/t2 is queued waiting that specific table create to happen.
   *
   * @return true if the lock was acquired and false otherwise
   */
  protected boolean acquireLock(final TEnvironment env) {
    return true;
  }

  /**
   * The user should override this method, and release lock if necessary.
   */
  protected void releaseLock(final TEnvironment env) {
    // no-op
  }

  /**
   * Used to keep the procedure lock even when the procedure is yielding or suspended.
   * @return true if the procedure should hold on the lock until completionCleanup()
   */
  protected boolean holdLock(final TEnvironment env) {
    return false;
  }

  /**
   * This is used in conjuction with holdLock(). If holdLock() is true
   * the procedure executor will not call acquireLock() if hasLock() is true.
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
   * This operation will not be retried on failure.
   */
  protected void completionCleanup(final TEnvironment env) {
    // no-op
  }

  /**
   * By default, the executor will try ro run procedures start to finish.
   * Return true to make the executor yield between each execution step to
   * give other procedures time to run their steps.
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
    toStringClassDetails(sb);

    sb.append(" id=");
    sb.append(getProcId());

    if (hasParent()) {
      sb.append(" parent=");
      sb.append(getParentProcId());
    }

    if (hasOwner()) {
      sb.append(" owner=");
      sb.append(getOwner());
    }

    sb.append(" state=");
    toStringState(sb);

    if (hasException()) {
      sb.append(" failed=" + getException());
    }

    return sb;
  }

  /**
   * Extend the toString() information with more procedure
   * details
   */
  public String toStringDetails() {
    final StringBuilder sb = toStringSimpleSB();

    sb.append(" startTime=");
    sb.append(getStartTime());

    sb.append(" lastUpdate=");
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
   * Called from {@link #toString()} when interpolating {@link Procedure} state
   * @param builder Append current {@link ProcedureState}
   */
  protected void toStringState(StringBuilder builder) {
    builder.append(getState());
    if (isSuspended()) {
      builder.append("|SUSPENDED");
    }
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

  public NonceKey getNonceKey() {
    return nonceKey;
  }

  public long getStartTime() {
    return startTime;
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
    this.startTime = EnvironmentEdgeManager.currentTime();
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

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  protected void setStartTime(final long startTime) {
    this.startTime = startTime;
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
    return getLastUpdate() - getStartTime();
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
   * @return true if the procedure is in a suspended state,
   *         waiting for the resources required to execute the procedure will become available.
   */
  public synchronized boolean isSuspended() {
    return suspended;
  }

  public synchronized void suspend() {
    suspended = true;
  }

  public synchronized void resume() {
    assert isSuspended() : this + " expected suspended state, got " + state;
    suspended = false;
  }

  /**
   * @return true if the procedure is in a RUNNABLE state.
   */
  protected synchronized boolean isRunnable() {
    return state == ProcedureState.RUNNABLE;
  }

  public synchronized boolean isInitializing() {
    return state == ProcedureState.INITIALIZING;
  }

  /**
   * @return true if the procedure has failed.
   *         true may mean failed but not yet rolledback or failed and rolledback.
   */
  public synchronized boolean isFailed() {
    return exception != null || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is finished successfully.
   */
  public synchronized boolean isSuccess() {
    return state == ProcedureState.FINISHED && exception == null;
  }

  /**
   * @return true if the procedure is finished. The Procedure may be completed
   *         successfuly or failed and rolledback.
   */
  public synchronized boolean isFinished() {
    switch (state) {
      case ROLLEDBACK:
        return true;
      case FINISHED:
        return exception == null;
      default:
        break;
    }
    return false;
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
  protected synchronized ProcedureState getState() {
    return state;
  }

  protected void setFailure(final String source, final Throwable cause) {
    setFailure(new RemoteProcedureException(source, cause));
  }

  protected synchronized void setFailure(final RemoteProcedureException exception) {
    this.exception = exception;
    if (!isFinished()) {
      setState(ProcedureState.FINISHED);
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
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  protected synchronized void incChildrenLatch() {
    // TODO: can this be inferred from the stack? I think so...
    this.childrenLatch++;
  }

  /**
   * Called by the ProcedureExecutor to notify that one of the sub-procedures has completed.
   */
  @InterfaceAudience.Private
  protected synchronized boolean childrenCountDown() {
    assert childrenLatch > 0;
    return --childrenLatch == 0;
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
   */
  @InterfaceAudience.Private
  protected Procedure[] doExecute(final TEnvironment env)
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
  protected boolean doAcquireLock(final TEnvironment env) {
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
  public int compareTo(final Procedure other) {
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
  protected static Long getRootProcedureId(final Map<Long, Procedure> procedures, Procedure proc) {
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
  public static boolean haveSameParent(final Procedure a, final Procedure b) {
    if (a.hasParent() && b.hasParent()) {
      return a.getParentProcId() == b.getParentProcId();
    }
    return false;
  }
}
