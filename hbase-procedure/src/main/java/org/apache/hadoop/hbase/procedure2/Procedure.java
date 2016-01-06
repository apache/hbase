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
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

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
 *  -> step 1
 *  ---> step 2
 *  -> step 1
 *
 * rollback() is called when the procedure or one of the sub-procedures is failed.
 * the rollback step is supposed to cleanup the resources created during the
 * execute() step. in case of failure and restart rollback() may be called
 * multiple times, so the code must be idempotent.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Procedure<TEnvironment> implements Comparable<Procedure> {
  // unchanged after initialization
  private String owner = null;
  private Long parentProcId = null;
  private Long procId = null;
  private long startTime;

  // runtime state, updated every operation
  private ProcedureState state = ProcedureState.INITIALIZING;
  private Integer timeout = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;
  private long lastUpdate;

  private RemoteProcedureException exception = null;
  private byte[] result = null;

  private NonceKey nonceKey = null;

  /**
   * The main code of the procedure. It must be idempotent since execute()
   * may be called multiple time in case of machine failure in the middle
   * of the execution.
   * @return a set of sub-procedures or null if there is nothing else to execute.
   */
  protected abstract Procedure[] execute(TEnvironment env)
    throws ProcedureYieldException;

  /**
   * The code to undo what done by the execute() code.
   * It is called when the procedure or one of the sub-procedure failed or an
   * abort was requested. It should cleanup all the resources created by
   * the execute() call. The implementation must be idempotent since rollback()
   * may be called multiple time in case of machine failure in the middle
   * of the execution.
   * @throws IOException temporary failure, the rollback will retry later
   */
  protected abstract void rollback(TEnvironment env)
    throws IOException;

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
   * Called when the procedure is loaded for replay.
   * The procedure implementor may use this method to perform some quick
   * operation before replay.
   * e.g. failing the procedure if the state on replay may be unknown.
   */
  protected void beforeReplay(final TEnvironment env) {
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toStringClassDetails(sb);

    if (procId != null) {
      sb.append(" id=");
      sb.append(getProcId());
    }

    if (hasParent()) {
      sb.append(" parent=");
      sb.append(getParentProcId());
    }

    if (hasOwner()) {
      sb.append(" owner=");
      sb.append(getOwner());
    }

    sb.append(" state=");
    sb.append(getState());
    return sb.toString();
  }

  /**
   * Extend the toString() information with the procedure details
   * e.g. className and parameters
   * @param builder the string builder to use to append the proc specific information
   */
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName());
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

  public long getProcId() {
    return procId;
  }

  public boolean hasParent() {
    return parentProcId != null;
  }

  public boolean hasException() {
    return exception != null;
  }

  public boolean hasTimeout() {
    return timeout != null;
  }

  public long getParentProcId() {
    return parentProcId;
  }

  public NonceKey getNonceKey() {
    return nonceKey;
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

  public synchronized RemoteProcedureException getException() {
    return exception;
  }

  public long getStartTime() {
    return startTime;
  }

  public synchronized long getLastUpdate() {
    return lastUpdate;
  }

  public synchronized long elapsedTime() {
    return lastUpdate - startTime;
  }

  /**
   * @param timeout timeout in msec
   */
  protected void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  /**
   * @return the timeout in msec
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * @return the remaining time before the timeout
   */
  public long getTimeRemaining() {
    return Math.max(0, timeout - (EnvironmentEdgeManager.currentTime() - startTime));
  }

  protected void setOwner(final String owner) {
    this.owner = StringUtils.isEmpty(owner) ? null : owner;
  }

  public String getOwner() {
    return owner;
  }

  public boolean hasOwner() {
    return owner != null;
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

  @InterfaceAudience.Private
  protected synchronized boolean setTimeoutFailure() {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = EnvironmentEdgeManager.currentTime() - lastUpdate;
      setFailure("ProcedureExecutor", new TimeoutException(
        "Operation timed out after " + StringUtils.humanTimeDiff(timeDiff)));
      return true;
    }
    return false;
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

  /**
   * Called by the ProcedureExecutor to set the value to the newly created procedure.
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setNonceKey(final NonceKey nonceKey) {
    this.nonceKey = nonceKey;
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code execute().
   */
  @InterfaceAudience.Private
  protected Procedure[] doExecute(final TEnvironment env)
      throws ProcedureYieldException {
    try {
      updateTimestamp();
      return execute(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code rollback().
   */
  @InterfaceAudience.Private
  protected void doRollback(final TEnvironment env) throws IOException {
    try {
      updateTimestamp();
      rollback(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  protected void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  private synchronized void setLastUpdate(final long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  protected synchronized void updateTimestamp() {
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
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
   * Called by the ProcedureExecutor to notify that one of the sub-procedures
   * has completed.
   */
  @InterfaceAudience.Private
  protected synchronized boolean childrenCountDown() {
    assert childrenLatch > 0;
    return --childrenLatch == 0;
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
    if (stackIndexes.length > 1) {
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

  @Override
  public int compareTo(final Procedure other) {
    long diff = getProcId() - other.getProcId();
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
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

  protected static Procedure newInstance(final String className) throws IOException {
    try {
      Class<?> clazz = Class.forName(className);
      if (!Modifier.isPublic(clazz.getModifiers())) {
        throw new Exception("the " + clazz + " class is not public");
      }

      Constructor<?> ctor = clazz.getConstructor();
      assert ctor != null : "no constructor found";
      if (!Modifier.isPublic(ctor.getModifiers())) {
        throw new Exception("the " + clazz + " constructor is not public");
      }
      return (Procedure)ctor.newInstance();
    } catch (Exception e) {
      throw new IOException("The procedure class " + className +
          " must be accessible and have an empty constructor", e);
    }
  }

  protected static void validateClass(final Procedure proc) throws IOException {
    try {
      Class<?> clazz = proc.getClass();
      if (!Modifier.isPublic(clazz.getModifiers())) {
        throw new Exception("the " + clazz + " class is not public");
      }

      Constructor<?> ctor = clazz.getConstructor();
      assert ctor != null;
      if (!Modifier.isPublic(ctor.getModifiers())) {
        throw new Exception("the " + clazz + " constructor is not public");
      }
    } catch (Exception e) {
      throw new IOException("The procedure class " + proc.getClass().getName() +
          " must be accessible and have an empty constructor", e);
    }
  }

  /**
   * Helper to convert the procedure to protobuf.
   * Used by ProcedureStore implementations.
   */
  @InterfaceAudience.Private
  public static ProcedureProtos.Procedure convert(final Procedure proc)
      throws IOException {
    Preconditions.checkArgument(proc != null);
    validateClass(proc);

    ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder()
      .setClassName(proc.getClass().getName())
      .setProcId(proc.getProcId())
      .setState(proc.getState())
      .setStartTime(proc.getStartTime())
      .setLastUpdate(proc.getLastUpdate());

    if (proc.hasParent()) {
      builder.setParentId(proc.getParentProcId());
    }

    if (proc.hasTimeout()) {
      builder.setTimeout(proc.getTimeout());
    }

    if (proc.hasOwner()) {
      builder.setOwner(proc.getOwner());
    }

    int[] stackIds = proc.getStackIndexes();
    if (stackIds != null) {
      for (int i = 0; i < stackIds.length; ++i) {
        builder.addStackId(stackIds[i]);
      }
    }

    if (proc.hasException()) {
      RemoteProcedureException exception = proc.getException();
      builder.setException(
        RemoteProcedureException.toProto(exception.getSource(), exception.getCause()));
    }

    byte[] result = proc.getResult();
    if (result != null) {
      builder.setResult(ByteStringer.wrap(result));
    }

    ByteString.Output stateStream = ByteString.newOutput();
    proc.serializeStateData(stateStream);
    if (stateStream.size() > 0) {
      builder.setStateData(stateStream.toByteString());
    }

    if (proc.getNonceKey() != null) {
      builder.setNonceGroup(proc.getNonceKey().getNonceGroup());
      builder.setNonce(proc.getNonceKey().getNonce());
    }

    return builder.build();
  }

  /**
   * Helper to convert the protobuf procedure.
   * Used by ProcedureStore implementations.
   *
   * TODO: OPTIMIZATION: some of the field never change during the execution
   *                     (e.g. className, procId, parentId, ...).
   *                     We can split in 'data' and 'state', and the store
   *                     may take advantage of it by storing the data only on insert().
   */
  @InterfaceAudience.Private
  public static Procedure convert(final ProcedureProtos.Procedure proto)
      throws IOException {
    // Procedure from class name
    Procedure proc = Procedure.newInstance(proto.getClassName());

    // set fields
    proc.setProcId(proto.getProcId());
    proc.setState(proto.getState());
    proc.setStartTime(proto.getStartTime());
    proc.setLastUpdate(proto.getLastUpdate());

    if (proto.hasParentId()) {
      proc.setParentProcId(proto.getParentId());
    }

    if (proto.hasOwner()) {
      proc.setOwner(proto.getOwner());
    }

    if (proto.hasTimeout()) {
      proc.setTimeout(proto.getTimeout());
    }

    if (proto.getStackIdCount() > 0) {
      proc.setStackIndexes(proto.getStackIdList());
    }

    if (proto.hasException()) {
      assert proc.getState() == ProcedureState.FINISHED ||
             proc.getState() == ProcedureState.ROLLEDBACK :
             "The procedure must be failed (waiting to rollback) or rolledback";
      proc.setFailure(RemoteProcedureException.fromProto(proto.getException()));
    }

    if (proto.hasResult()) {
      proc.setResult(proto.getResult().toByteArray());
    }

    if (proto.getNonce() != HConstants.NO_NONCE) {
      NonceKey nonceKey = new NonceKey(proto.getNonceGroup(), proto.getNonce());
      proc.setNonceKey(nonceKey);
    }

    // we want to call deserialize even when the stream is empty, mainly for testing.
    proc.deserializeStateData(proto.getStateData().newInput());

    return proc;
  }
}
