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

package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the Assign and Unassign Procedure.
 *
 * Locking:
 * Takes exclusive lock on the region being assigned/unassigned. Thus, there can only be one
 * RegionTransitionProcedure per region running at a time (see MasterProcedureScheduler).
 *
 * <p>This procedure is asynchronous and responds to external events.
 * The AssignmentManager will notify this procedure when the RS completes
 * the operation and reports the transitioned state
 * (see the Assign and Unassign class for more detail).</p>
 *
 * <p>Procedures move from the REGION_TRANSITION_QUEUE state when they are
 * first submitted, to the REGION_TRANSITION_DISPATCH state when the request
 * to remote server is sent and the Procedure is suspended waiting on external
 * event to be woken again. Once the external event is triggered, Procedure
 * moves to the REGION_TRANSITION_FINISH state.</p>
 *
 * <p>NOTE: {@link AssignProcedure} and {@link UnassignProcedure} should not be thought of
 * as being asymmetric, at least currently.
 * <ul>
 * <li>{@link AssignProcedure} moves through all the above described states and implements methods
 * associated with each while {@link UnassignProcedure} starts at state
 * REGION_TRANSITION_DISPATCH and state REGION_TRANSITION_QUEUE is not supported.</li>
 *
 * <li>When any step in {@link AssignProcedure} fails, failure handler
 * AssignProcedure#handleFailure(MasterProcedureEnv, RegionStateNode) re-attempts the
 * assignment by setting the procedure state to REGION_TRANSITION_QUEUE and forces
 * assignment to a different target server by setting {@link AssignProcedure#forceNewPlan}. When
 * the number of attempts reaches threshold configuration 'hbase.assignment.maximum.attempts',
 * the procedure is aborted. For {@link UnassignProcedure}, similar re-attempts are
 * intentionally not implemented. It is a 'one shot' procedure. See its class doc for how it
 * handles failure.
 * </li>
 * </ul>
 * </p>
 *
 * <p>TODO: Considering it is a priority doing all we can to get make a region available as soon as possible,
 * re-attempting with any target makes sense if specified target fails in case of
 * {@link AssignProcedure}. For {@link UnassignProcedure}, our concern is preventing data loss
 * on failed unassign. See class doc for explanation.
 */
@InterfaceAudience.Private
public abstract class RegionTransitionProcedure
    extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface,
      RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Logger LOG = LoggerFactory.getLogger(RegionTransitionProcedure.class);

  protected final AtomicBoolean aborted = new AtomicBoolean(false);

  private RegionTransitionState transitionState = RegionTransitionState.REGION_TRANSITION_QUEUE;
  private RegionInfo regionInfo;
  private volatile boolean lock = false;

  // Required by the Procedure framework to create the procedure on replay
  public RegionTransitionProcedure() {}

  public RegionTransitionProcedure(final RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  @VisibleForTesting
  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  protected void setRegionInfo(final RegionInfo regionInfo) {
    // Setter is for deserialization.
    this.regionInfo = regionInfo;
  }

  @Override
  public TableName getTableName() {
    RegionInfo hri = getRegionInfo();
    return hri != null? hri.getTable(): null;
  }

  public boolean isMeta() {
    return TableName.isMetaTableName(getTableName());
  }

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", region=");
    sb.append(getRegionInfo() == null? null: getRegionInfo().getEncodedName());
  }

  public RegionStateNode getRegionState(final MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegionInfo());
  }

  void setTransitionState(final RegionTransitionState state) {
    this.transitionState = state;
  }

  RegionTransitionState getTransitionState() {
    return transitionState;
  }

  protected abstract boolean startTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;

  /**
   * Called when the Procedure is in the REGION_TRANSITION_DISPATCH state.
   * In here we do the RPC call to OPEN/CLOSE the region. The suspending of
   * the thread so it sleeps until it gets update that the OPEN/CLOSE has
   * succeeded is complicated. Read the implementations to learn more.
   */
  protected abstract boolean updateTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;

  protected abstract void finishTransition(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException, ProcedureSuspendedException;

  protected abstract void reportTransition(MasterProcedureEnv env,
      RegionStateNode regionNode, TransitionCode code, long seqId) throws UnexpectedStateException;

  @Override
  public abstract RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName serverName);

  /**
   * @return True if processing of fail is complete; the procedure will be woken from its suspend
   * and we'll go back to running through procedure steps:
   * otherwise if false we leave the procedure in suspended state.
   */
  protected abstract boolean remoteCallFailed(MasterProcedureEnv env,
      RegionStateNode regionNode, IOException exception);

  @Override
  public void remoteCallFailed(final MasterProcedureEnv env,
      final ServerName serverName, final IOException exception) {
    final RegionStateNode regionNode = getRegionState(env);
    String msg = exception.getMessage() == null? exception.getClass().getSimpleName():
      exception.getMessage();
    LOG.warn("Remote call failed " + this + "; " + regionNode.toShortString() +
      "; exception=" + msg);
    if (remoteCallFailed(env, regionNode, exception)) {
      // NOTE: This call to wakeEvent puts this Procedure back on the scheduler.
      // Thereafter, another Worker can be in here so DO NOT MESS WITH STATE beyond
      // this method. Just get out of this current processing quickly.
      regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
    }
    // else leave the procedure in suspended state; it is waiting on another call to this callback
  }

  /**
   * Be careful! At the end of this method, the procedure has either succeeded
   * and this procedure has been set into a suspended state OR, we failed and
   * this procedure has been put back on the scheduler ready for another worker
   * to pick it up. In both cases, we need to exit the current Worker processing
   * immediately!
   * @return True if we successfully dispatched the call and false if we failed;
   * if failed, we need to roll back any setup done for the dispatch.
   */
  protected boolean addToRemoteDispatcher(final MasterProcedureEnv env,
      final ServerName targetServer) {
    assert targetServer == null || targetServer.equals(getRegionState(env).getRegionLocation()):
      "targetServer=" + targetServer + " getRegionLocation=" +
        getRegionState(env).getRegionLocation(); // TODO

    LOG.info("Dispatch " + this + "; " + getRegionState(env).toShortString());

    // Put this procedure into suspended mode to wait on report of state change
    // from remote regionserver. Means Procedure associated ProcedureEvent is marked not 'ready'.
    getRegionState(env).getProcedureEvent().suspend();

    // Tricky because the below call to addOperationToNode can fail. If it fails, we need to
    // backtrack on stuff like the 'suspend' done above -- tricky as the 'wake' requests us -- and
    // ditto up in the caller; it needs to undo state changes. Inside in remoteCallFailed, it does
    // wake to undo the above suspend.
    if (!env.getRemoteDispatcher().addOperationToNode(targetServer, this)) {
      remoteCallFailed(env, targetServer,
          new FailedRemoteDispatchException(this + " to " + targetServer));
      return false;
    }
    return true;
  }

  protected void reportTransition(final MasterProcedureEnv env, final ServerName serverName,
      final TransitionCode code, final long seqId) throws UnexpectedStateException {
    final RegionStateNode regionNode = getRegionState(env);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received report " + code + " seqId=" + seqId + ", " +
            this + "; " + regionNode.toShortString());
    }
    if (!serverName.equals(regionNode.getRegionLocation())) {
      if (isMeta() && regionNode.getRegionLocation() == null) {
        regionNode.setRegionLocation(serverName);
      } else {
        throw new UnexpectedStateException(String.format(
          "Unexpected state=%s from server=%s; expected server=%s; %s; %s",
          code, serverName, regionNode.getRegionLocation(),
          this, regionNode.toShortString()));
      }
    }

    reportTransition(env, regionNode, code, seqId);

    // NOTE: This call adds this procedure back on the scheduler.
    // This makes it so this procedure can run again. Another worker will take
    // processing to the next stage. At an extreme, the other worker may run in
    // parallel so DO  NOT CHANGE any state hereafter! This should be last thing
    // done in this processing step.
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  protected boolean isServerOnline(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    return isServerOnline(env, regionNode.getRegionLocation());
  }

  protected boolean isServerOnline(final MasterProcedureEnv env, final ServerName serverName) {
    return env.getMasterServices().getServerManager().isServerOnline(serverName);
  }

  @Override
  protected void toStringState(StringBuilder builder) {
    super.toStringState(builder);
    RegionTransitionState ts = this.transitionState;
    if (!isFinished() && ts != null) {
      builder.append(":").append(ts);
    }
  }

  @Override
  protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
    final AssignmentManager am = env.getAssignmentManager();
    final RegionStateNode regionNode = getRegionState(env);
    if (!am.addRegionInTransition(regionNode, this)) {
      String msg = String.format(
        "There is already another procedure running on this region this=%s owner=%s",
        this, regionNode.getProcedure());
      LOG.warn(msg + " " + this + "; " + regionNode.toShortString());
      setAbortFailure(getClass().getSimpleName(), msg);
      return null;
    }
    try {
      boolean retry;
      do {
        retry = false;
        switch (transitionState) {
          case REGION_TRANSITION_QUEUE:
            // 1. push into the AM queue for balancer policy
            if (!startTransition(env, regionNode)) {
              // The operation figured it is done or it aborted; check getException()
              am.removeRegionInTransition(getRegionState(env), this);
              return null;
            }
            transitionState = RegionTransitionState.REGION_TRANSITION_DISPATCH;
            if (regionNode.getProcedureEvent().suspendIfNotReady(this)) {
              // Why this suspend? Because we want to ensure Store happens before proceed?
              throw new ProcedureSuspendedException();
            }
            break;

          case REGION_TRANSITION_DISPATCH:
            // 2. send the request to the target server
            if (!updateTransition(env, regionNode)) {
              // The operation figured it is done or it aborted; check getException()
              am.removeRegionInTransition(regionNode, this);
              return null;
            }
            if (transitionState != RegionTransitionState.REGION_TRANSITION_DISPATCH) {
              retry = true;
              break;
            }
            if (regionNode.getProcedureEvent().suspendIfNotReady(this)) {
              throw new ProcedureSuspendedException();
            }
            break;

          case REGION_TRANSITION_FINISH:
            // 3. wait assignment response. completion/failure
            finishTransition(env, regionNode);
            am.removeRegionInTransition(regionNode, this);
            return null;
        }
      } while (retry);
    } catch (IOException e) {
      LOG.warn("Retryable error trying to transition: " +
          this + "; " + regionNode.toShortString(), e);
    }

    return new Procedure[] {this};
  }

  @Override
  protected void rollback(final MasterProcedureEnv env) {
    if (isRollbackSupported(transitionState)) {
      // Nothing done up to this point. abort safely.
      // This should happen when something like disableTable() is triggered.
      env.getAssignmentManager().removeRegionInTransition(getRegionState(env), this);
      return;
    }

    // There is no rollback for assignment unless we cancel the operation by
    // dropping/disabling the table.
    throw new UnsupportedOperationException("Unhandled state " + transitionState +
        "; there is no rollback for assignment unless we cancel the operation by " +
        "dropping/disabling the table");
  }

  protected abstract boolean isRollbackSupported(final RegionTransitionState state);

  @Override
  protected boolean abort(final MasterProcedureEnv env) {
    if (isRollbackSupported(transitionState)) {
      aborted.set(true);
      return true;
    }
    return false;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    // Unless we are assigning meta, wait for meta to be available and loaded.
    if (!isMeta() && (env.waitFailoverCleanup(this) ||
        env.getAssignmentManager().waitMetaInitialized(this, getRegionInfo()))) {
      return LockState.LOCK_EVENT_WAIT;
    }

    // TODO: Revisit this and move it to the executor
    if (env.getProcedureScheduler().waitRegion(this, getRegionInfo())) {
      try {
        LOG.debug(LockState.LOCK_EVENT_WAIT + " pid=" + getProcId() + " " +
          env.getProcedureScheduler().dumpLocks());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return LockState.LOCK_EVENT_WAIT;
    }
    this.lock = true;
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegion(this, getRegionInfo());
    lock = false;
  }

  @Override
  protected boolean holdLock(final MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected boolean hasLock(final MasterProcedureEnv env) {
    return lock;
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // The operation is triggered internally on the server
    // the client does not know about this procedure.
    return false;
  }

  /**
   * Used by ServerCrashProcedure to see if this Assign/Unassign needs processing.
   * @return ServerName the Assign or Unassign is going against.
   */
  public abstract ServerName getServer(final MasterProcedureEnv env);

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    // should not be called for region operation until we modified the open/close region procedure
    throw new UnsupportedOperationException();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, String error) {
    // should not be called for region operation until we modified the open/close region procedure
    throw new UnsupportedOperationException();
  }
}
