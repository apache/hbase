/*
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
import java.util.Comparator;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionOpenOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Procedure that describe the assignment of a single region.
 * There can only be one RegionTransitionProcedure per region running at a time
 * since each procedure takes a lock on the region.
 *
 * <p>The Assign starts by pushing the "assign" operation to the AssignmentManager
 * and then will go in a "waiting" state.
 * The AM will batch the "assign" requests and ask the Balancer where to put
 * the region (the various policies will be respected: retain, round-robin, random).
 * Once the AM and the balancer have found a place for the region the procedure
 * will be resumed and an "open region" request will be placed in the Remote Dispatcher
 * queue, and the procedure once again will go in a "waiting state".
 * The Remote Dispatcher will batch the various requests for that server and
 * they will be sent to the RS for execution.
 * The RS will complete the open operation by calling master.reportRegionStateTransition().
 * The AM will intercept the transition report, and notify the procedure.
 * The procedure will finish the assignment by publishing to new state on meta
 * or it will retry the assignment.
 *
 * <p>This procedure does not rollback when beyond the first
 * REGION_TRANSITION_QUEUE step; it will press on trying to assign in the face of
 * failure. Should we ignore rollback calls to Assign/Unassign then? Or just
 * remove rollback here?
 */
// TODO: Add being able to assign a region to open read-only.
@InterfaceAudience.Private
public class AssignProcedure extends RegionTransitionProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(AssignProcedure.class);

  /**
   * Set to true when we need recalibrate -- choose a new target -- because original assign failed.
   */
  private boolean forceNewPlan = false;

  /**
   * Gets set as desired target on move, merge, etc., when we want to go to a particular server.
   * We may not be able to respect this request but will try. When it is NOT set, then we ask
   * the balancer to assign. This value is used below in startTransition to set regionLocation if
   * non-null. Setting regionLocation in regionServerNode is how we override balancer setting
   * destination.
   */
  protected volatile ServerName targetServer;

  /**
   * Comparator that will sort AssignProcedures so meta assigns come first, then system table
   * assigns and finally user space assigns.
   */
  public static final CompareAssignProcedure COMPARATOR = new CompareAssignProcedure();

  public AssignProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public AssignProcedure(final RegionInfo regionInfo) {
    super(regionInfo);
    this.targetServer = null;
  }

  public AssignProcedure(final RegionInfo regionInfo, final ServerName destinationServer) {
    super(regionInfo);
    this.targetServer = destinationServer;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_ASSIGN;
  }

  @Override
  protected boolean isRollbackSupported(final RegionTransitionState state) {
    switch (state) {
      case REGION_TRANSITION_QUEUE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    final AssignRegionStateData.Builder state = AssignRegionStateData.newBuilder()
        .setTransitionState(getTransitionState())
        .setRegionInfo(ProtobufUtil.toRegionInfo(getRegionInfo()));
    if (forceNewPlan) {
      state.setForceNewPlan(true);
    }
    if (this.targetServer != null) {
      state.setTargetServer(ProtobufUtil.toServerName(this.targetServer));
    }
    if (getAttempt() > 0) {
      state.setAttempt(getAttempt());
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    final AssignRegionStateData state = serializer.deserialize(AssignRegionStateData.class);
    setTransitionState(state.getTransitionState());
    setRegionInfo(ProtobufUtil.toRegionInfo(state.getRegionInfo()));
    forceNewPlan = state.getForceNewPlan();
    if (state.hasTargetServer()) {
      this.targetServer = ProtobufUtil.toServerName(state.getTargetServer());
    }
    if (state.hasAttempt()) {
      setAttempt(state.getAttempt());
    }
  }

  @Override
  protected boolean startTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    // If the region is already open we can't do much...
    if (regionNode.isInState(State.OPEN) && isServerOnline(env, regionNode)) {
      LOG.info("Assigned, not reassigning; " + this + "; " + regionNode.toShortString());
      return false;
    }
    // Don't assign if table is in disabling or disabled state.
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    TableName tn = regionNode.getRegionInfo().getTable();
    if (tsm.getTableState(tn).isDisabledOrDisabling()) {
      LOG.info("Table " + tn + " state=" + tsm.getTableState(tn) + ", skipping " + this);
      return false;
    }
    // If the region is SPLIT, we can't assign it. But state might be CLOSED, rather than
    // SPLIT which is what a region gets set to when unassigned as part of SPLIT. FIX.
    if (regionNode.isInState(State.SPLIT) ||
        (regionNode.getRegionInfo().isOffline() && regionNode.getRegionInfo().isSplit())) {
      LOG.info("SPLIT, cannot be assigned; " + this + "; " + regionNode +
        "; hri=" + regionNode.getRegionInfo());
      return false;
    }

    // If we haven't started the operation yet, we can abort
    if (aborted.get() && regionNode.isInState(State.CLOSED, State.OFFLINE)) {
      if (incrementAndCheckMaxAttempts(env, regionNode)) {
        regionNode.setState(State.FAILED_OPEN);
        setFailure(getClass().getSimpleName(),
          new RetriesExhaustedException("Max attempts exceeded"));
      } else {
        setAbortFailure(getClass().getSimpleName(), "Abort requested");
      }
      return false;
    }

    // Send assign (add into assign-pool). We call regionNode.offline below to set state to
    // OFFLINE and to clear the region location. Setting a new regionLocation here is how we retain
    // old assignment or specify target server if a move or merge. See
    // AssignmentManager#processAssignQueue. Otherwise, balancer gives us location.
    // TODO: Region will be set into OFFLINE state below regardless of what its previous state was
    // This is dangerous? Wrong? What if region was in an unexpected state?
    ServerName lastRegionLocation = regionNode.offline();
    boolean retain = false;
    if (!forceNewPlan) {
      if (this.targetServer != null) {
        retain = targetServer.equals(lastRegionLocation);
        regionNode.setRegionLocation(targetServer);
      } else {
        if (lastRegionLocation != null) {
          // Try and keep the location we had before we offlined.
          retain = true;
          regionNode.setRegionLocation(lastRegionLocation);
        } else if (regionNode.getLastHost() != null) {
          retain = true;
          LOG.info("Setting lastHost as the region location " + regionNode.getLastHost());
          regionNode.setRegionLocation(regionNode.getLastHost());
        }
      }
    }
    LOG.info("Starting " + this + "; " + regionNode.toShortString() +
        "; forceNewPlan=" + this.forceNewPlan +
        ", retain=" + retain);
    env.getAssignmentManager().queueAssign(regionNode);
    return true;
  }

  @Override
  protected boolean updateTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
  throws IOException, ProcedureSuspendedException {
    // TODO: crash if destinationServer is specified and not online
    // which is also the case when the balancer provided us with a different location.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Update " + this + "; " + regionNode.toShortString());
    }
    if (regionNode.getRegionLocation() == null) {
      setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
      return true;
    }

    if (!isServerOnline(env, regionNode)) {
      // TODO: is this correct? should we wait the chore/ssh?
      LOG.info("Server not online, re-queuing " + this + "; " + regionNode.toShortString());
      setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
      return true;
    }

    if (env.getAssignmentManager().waitServerReportEvent(regionNode.getRegionLocation(), this)) {
      LOG.info("Early suspend! " + this + "; " + regionNode.toShortString());
      throw new ProcedureSuspendedException();
    }

    if (regionNode.isInState(State.OPEN)) {
      LOG.info("Already assigned: " + this + "; " + regionNode.toShortString());
      return false;
    }

    // Transition regionNode State. Set it to OPENING. Update hbase:meta, and add
    // region to list of regions on the target regionserver. Need to UNDO if failure!
    env.getAssignmentManager().markRegionAsOpening(regionNode);

    // TODO: Requires a migration to be open by the RS?
    // regionNode.getFormatVersion()

    if (!addToRemoteDispatcher(env, regionNode.getRegionLocation())) {
      // Failed the dispatch BUT addToRemoteDispatcher internally does
      // cleanup on failure -- even the undoing of markRegionAsOpening above --
      // so nothing more to do here; in fact we need to get out of here
      // fast since we've been put back on the scheduler.
    }

    // We always return true, even if we fail dispatch because addToRemoteDispatcher
    // failure processing sets state back to REGION_TRANSITION_QUEUE so we try again;
    // i.e. return true to keep the Procedure running; it has been reset to startover.
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    env.getAssignmentManager().markRegionAsOpened(regionNode);
    // This success may have been after we failed open a few times. Be sure to cleanup any
    // failed open references. See #incrementAndCheckMaxAttempts and where it is called.
    env.getAssignmentManager().getRegionStates().removeFromFailedOpen(regionNode.getRegionInfo());
  }

  @Override
  protected void reportTransition(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final TransitionCode code, final long openSeqNum) throws UnexpectedStateException {
    switch (code) {
      case OPENED:
        if (openSeqNum < 0) {
          throw new UnexpectedStateException("Received report unexpected " + code +
              " transition openSeqNum=" + openSeqNum + ", " + regionNode);
        }
        if (openSeqNum < regionNode.getOpenSeqNum()) {
          // Don't bother logging if openSeqNum == 0
          if (openSeqNum != 0) {
            LOG.warn("Skipping update of open seqnum with " + openSeqNum +
                " because current seqnum=" + regionNode.getOpenSeqNum());
          }
        } else {
          regionNode.setOpenSeqNum(openSeqNum);
        }
        // Leave the state here as OPENING for now. We set it to OPEN in
        // REGION_TRANSITION_FINISH section where we do a bunch of checks.
        // regionNode.setState(RegionState.State.OPEN, RegionState.State.OPENING);
        setTransitionState(RegionTransitionState.REGION_TRANSITION_FINISH);
        break;
      case FAILED_OPEN:
        handleFailure(env, regionNode);
        break;
      default:
        throw new UnexpectedStateException("Received report unexpected " + code +
            " transition openSeqNum=" + openSeqNum + ", " + regionNode.toShortString() +
            ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  /**
   * Called when dispatch or subsequent OPEN request fail. Can be run by the
   * inline dispatch call or later by the ServerCrashProcedure. Our state is
   * generally OPENING. Cleanup and reset to OFFLINE and put our Procedure
   * State back to REGION_TRANSITION_QUEUE so the Assign starts over.
   */
  private void handleFailure(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    if (incrementAndCheckMaxAttempts(env, regionNode)) {
      aborted.set(true);
    }
    this.forceNewPlan = true;
    this.targetServer = null;
    regionNode.offline();
    // We were moved to OPENING state before dispatch. Undo. It is safe to call
    // this method because it checks for OPENING first.
    env.getAssignmentManager().undoRegionAsOpening(regionNode);
    setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
  }

  private boolean incrementAndCheckMaxAttempts(final MasterProcedureEnv env,
      final RegionStateNode regionNode) {
    final int retries = env.getAssignmentManager().getRegionStates().
        addToFailedOpen(regionNode).incrementAndGetRetries();
    int max = env.getAssignmentManager().getAssignMaxAttempts();
    LOG.info("Retry=" + retries + " of max=" + max + "; " +
        this + "; " + regionNode.toShortString());
    return retries >= max;
  }

  @Override
  public RemoteOperation remoteCallBuild(final MasterProcedureEnv env, final ServerName serverName) {
    assert serverName.equals(getRegionState(env).getRegionLocation());
    return new RegionOpenOperation(this, getRegionInfo(),
        env.getAssignmentManager().getFavoredNodes(getRegionInfo()), false);
  }

  @Override
  protected boolean remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    handleFailure(env, regionNode);
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    if (this.targetServer != null) sb.append(", target=").append(this.targetServer);
  }

  @Override
  public ServerName getServer(final MasterProcedureEnv env) {
    RegionStateNode node =
        env.getAssignmentManager().getRegionStates().getRegionStateNode(this.getRegionInfo());
    if (node == null) return null;
    return node.getRegionLocation();
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getAssignProcMetrics();
  }

  /**
   * Sort AssignProcedures such that meta and system assigns come first before user-space assigns.
   * Have to do it this way w/ distinct Comparator because Procedure is already Comparable on
   * 'Env'(?).
   */
  public static class CompareAssignProcedure implements Comparator<AssignProcedure> {
    @Override
    public int compare(AssignProcedure left, AssignProcedure right) {
      if (left.getRegionInfo().isMetaRegion()) {
        if (right.getRegionInfo().isMetaRegion()) {
          return RegionInfo.COMPARATOR.compare(left.getRegionInfo(), right.getRegionInfo());
        }
        return -1;
      } else if (right.getRegionInfo().isMetaRegion()) {
        return +1;
      }
      if (left.getRegionInfo().getTable().isSystemTable()) {
        if (right.getRegionInfo().getTable().isSystemTable()) {
          return RegionInfo.COMPARATOR.compare(left.getRegionInfo(), right.getRegionInfo());
        }
        return -1;
      } else if (right.getRegionInfo().getTable().isSystemTable()) {
        return +1;
      }
      return RegionInfo.COMPARATOR.compare(left.getRegionInfo(), right.getRegionInfo());
    }
  }
}
