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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The procedure to deal with the state transition of a region. A region with a TRSP in place is
 * called RIT, i.e, RegionInTransition.
 * <p/>
 * It can be used to assign/unassign/reopen/move a region, and for
 * {@link #unassign(MasterProcedureEnv, RegionInfo)} and
 * {@link #reopen(MasterProcedureEnv, RegionInfo)}, you do not need to specify a target server, and
 * for {@link #assign(MasterProcedureEnv, RegionInfo, ServerName)} and
 * {@link #move(MasterProcedureEnv, RegionInfo, ServerName)}, if you want to you can provide a
 * target server. And for {@link #move(MasterProcedureEnv, RegionInfo, ServerName)}, if you do not
 * specify a targetServer, we will select one randomly.
 * <p/>
 * <p/>
 * The typical state transition for assigning a region is:
 *
 * <pre>
 * GET_ASSIGN_CANDIDATE ------> OPEN -----> CONFIRM_OPENED
 * </pre>
 *
 * Notice that, if there are failures we may go back to the {@code GET_ASSIGN_CANDIDATE} state to
 * try again.
 * <p/>
 * The typical state transition for unassigning a region is:
 *
 * <pre>
 * CLOSE -----> CONFIRM_CLOSED
 * </pre>
 *
 * Here things go a bit different, if there are failures, especially that if there is a server
 * crash, we will go to the {@code GET_ASSIGN_CANDIDATE} state to bring the region online first, and
 * then go through the normal way to unassign it.
 * <p/>
 * The typical state transition for reopening/moving a region is:
 *
 * <pre>
 * CLOSE -----> CONFIRM_CLOSED -----> GET_ASSIGN_CANDIDATE ------> OPEN -----> CONFIRM_OPENED
 * </pre>
 *
 * The retry logic is the same with the above assign/unassign.
 * <p/>
 * Notice that, although we allow specify a target server, it just acts as a candidate, we do not
 * guarantee that the region will finally be on the target server. If this is important for you, you
 * should check whether the region is on the target server after the procedure is finished.
 * <p/>
 * When you want to schedule a TRSP, please check whether there is still one for this region, and
 * the check should be under the RegionStateNode lock. We will remove the TRSP from a
 * RegionStateNode when we are done, see the code in {@code reportTransition} method below. There
 * could be at most one TRSP for a give region.
 */
@InterfaceAudience.Private
public class TransitRegionStateProcedure
    extends AbstractStateMachineRegionProcedure<RegionStateTransitionState> {

  private static final Logger LOG = LoggerFactory.getLogger(TransitRegionStateProcedure.class);

  private RegionStateTransitionState initialState;

  private RegionStateTransitionState lastState;

  // the candidate where we want to assign the region to.
  private ServerName assignCandidate;

  private boolean forceNewPlan;

  private int attempt;

  public TransitRegionStateProcedure() {
  }

  @VisibleForTesting
  protected TransitRegionStateProcedure(MasterProcedureEnv env, RegionInfo hri,
      ServerName assignCandidate, boolean forceNewPlan, RegionStateTransitionState initialState,
      RegionStateTransitionState lastState) {
    super(env, hri);
    this.assignCandidate = assignCandidate;
    this.forceNewPlan = forceNewPlan;
    this.initialState = initialState;
    this.lastState = lastState;
  }

  @Override
  public TableOperationType getTableOperationType() {
    // TODO: maybe we should make another type here, REGION_TRANSITION?
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (TableName.isMetaTableName(getTableName())) {
      return false;
    }
    // First we need meta to be loaded, and second, if meta is not online then we will likely to
    // fail when updating meta so we wait until it is assigned.
    AssignmentManager am = env.getAssignmentManager();
    return am.waitMetaLoaded(this) || am.waitMetaAssigned(this, getRegion());
  }

  private void queueAssign(MasterProcedureEnv env, RegionStateNode regionNode)
      throws ProcedureSuspendedException {
    // Here the assumption is that, the region must be in CLOSED state, so the region location
    // will be null. And if we fail to open the region and retry here, the forceNewPlan will be
    // true, and also we will set the region location to null.
    boolean retain = false;
    if (!forceNewPlan) {
      if (assignCandidate != null) {
        retain = assignCandidate.equals(regionNode.getLastHost());
        regionNode.setRegionLocation(assignCandidate);
      } else if (regionNode.getLastHost() != null) {
        retain = true;
        LOG.info("Setting lastHost as the region location {}", regionNode.getLastHost());
        regionNode.setRegionLocation(regionNode.getLastHost());
      }
    }
    LOG.info("Starting {}; {}; forceNewPlan={}, retain={}", this, regionNode.toShortString(),
      forceNewPlan, retain);
    env.getAssignmentManager().queueAssign(regionNode);
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_OPEN);
    if (regionNode.getProcedureEvent().suspendIfNotReady(this)) {
      throw new ProcedureSuspendedException();
    }
  }

  private void openRegion(MasterProcedureEnv env, RegionStateNode regionNode) throws IOException {
    ServerName loc = regionNode.getRegionLocation();
    if (loc == null) {
      LOG.warn("No location specified for {}, jump back to state {} to get one", getRegion(),
        RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      return;
    }
    env.getAssignmentManager().regionOpening(regionNode);
    addChildProcedure(new OpenRegionProcedure(getRegion(), loc));
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED);
  }

  private Flow confirmOpened(MasterProcedureEnv env, RegionStateNode regionNode)
      throws IOException {
    // notice that, for normal case, if we successfully opened a region, we will not arrive here, as
    // in reportTransition we will call unsetProcedure, and in executeFromState we will return
    // directly. But if the master is crashed before we finish the procedure, then next time we will
    // arrive here. So we still need to add code for normal cases.
    if (regionNode.isInState(State.OPEN)) {
      attempt = 0;
      if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED) {
        // we are the last state, finish
        regionNode.unsetProcedure(this);
        return Flow.NO_MORE_STATE;
      }
      // It is possible that we arrive here but confirm opened is not the last state, for example,
      // when merging or splitting a region, we unassign the region from a RS and the RS is crashed,
      // then there will be recovered edits for this region, we'd better make the region online
      // again and then unassign it, otherwise we have to fail the merge/split procedure as we may
      // loss data.
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE);
      return Flow.HAS_MORE_STATE;
    }

    if (incrementAndCheckMaxAttempts(env, regionNode)) {
      env.getAssignmentManager().regionFailedOpen(regionNode, true);
      setFailure(getClass().getSimpleName(), new RetriesExhaustedException(
        "Max attempts " + env.getAssignmentManager().getAssignMaxAttempts() + " exceeded"));
      regionNode.unsetProcedure(this);
      return Flow.NO_MORE_STATE;
    }
    env.getAssignmentManager().regionFailedOpen(regionNode, false);
    // we failed to assign the region, force a new plan
    forceNewPlan = true;
    regionNode.setRegionLocation(null);
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
    // Here we do not throw exception because we want to the region to be online ASAP
    return Flow.HAS_MORE_STATE;
  }

  private void closeRegion(MasterProcedureEnv env, RegionStateNode regionNode) throws IOException {
    if (regionNode.isInState(State.OPEN, State.CLOSING, State.MERGING, State.SPLITTING)) {
      // this is the normal case
      env.getAssignmentManager().regionClosing(regionNode);
      addChildProcedure(
        new CloseRegionProcedure(getRegion(), regionNode.getRegionLocation(), assignCandidate));
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED);
    } else {
      forceNewPlan = true;
      regionNode.setRegionLocation(null);
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
    }
  }

  private Flow confirmClosed(MasterProcedureEnv env, RegionStateNode regionNode)
      throws IOException {
    // notice that, for normal case, if we successfully opened a region, we will not arrive here, as
    // in reportTransition we will call unsetProcedure, and in executeFromState we will return
    // directly. But if the master is crashed before we finish the procedure, then next time we will
    // arrive here. So we still need to add code for normal cases.
    if (regionNode.isInState(State.CLOSED)) {
      attempt = 0;
      if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
        // we are the last state, finish
        regionNode.unsetProcedure(this);
        return Flow.NO_MORE_STATE;
      }
      // This means we need to open the region again, should be a move or reopen
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      return Flow.HAS_MORE_STATE;
    }
    if (regionNode.isInState(State.CLOSING)) {
      // This is possible, think the target RS crashes and restarts immediately, the close region
      // operation will return a NotServingRegionException soon, we can only recover after SCP takes
      // care of this RS. So here we throw an IOException to let upper layer to retry with backoff.
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE);
      throw new HBaseIOException("Failed to close region");
    }
    // abnormally closed, need to reopen it, no matter what is the last state, see the comment in
    // confirmOpened for more details that why we need to reopen the region first even if we just
    // want to close it.
    // The only exception is for non-default replica, where we do not need to deal with recovered
    // edits. Notice that the region will remain in ABNORMALLY_CLOSED state, the upper layer need to
    // deal with this state. For non-default replica, this is usually the same with CLOSED.
    assert regionNode.isInState(State.ABNORMALLY_CLOSED);
    if (!RegionReplicaUtil.isDefaultReplica(getRegion()) &&
      lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
      regionNode.unsetProcedure(this);
      return Flow.NO_MORE_STATE;
    }
    attempt = 0;
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
    return Flow.HAS_MORE_STATE;
  }

  // Override to lock RegionStateNode
  @SuppressWarnings("rawtypes")
  @Override
  protected Procedure[] execute(MasterProcedureEnv env)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegion());
    regionNode.lock();
    try {
      return super.execute(env);
    } finally {
      regionNode.unlock();
    }
  }

  private RegionStateNode getRegionStateNode(MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegion());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RegionStateTransitionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    RegionStateNode regionNode = getRegionStateNode(env);
    if (regionNode.getProcedure() != this) {
      // This is possible, and is the normal case, as we will call unsetProcedure in
      // reportTransition, this means we have already done
      // This is because that, when we mark the region as OPENED or CLOSED, then all the works
      // should have already been done, and logically we could have another TRSP scheduled for this
      // region immediately(think of a RS crash at the point...).
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE:
          queueAssign(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_OPEN:
          openRegion(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_CONFIRM_OPENED:
          return confirmOpened(env, regionNode);
        case REGION_STATE_TRANSITION_CLOSE:
          closeRegion(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_CONFIRM_CLOSED:
          return confirmClosed(env, regionNode);
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      long backoff = ProcedureUtil.getBackoffTimeMs(this.attempt++);
      LOG.warn(
        "Failed transition, suspend {}secs {}; {}; waiting on rectified condition fixed " +
          "by other Procedure or operator intervention",
        backoff / 1000, this, regionNode.toShortString(), e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  /**
   * At end of timeout, wake ourselves up so we run again.
   */
  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }

  private boolean isOpening(RegionStateNode regionNode, ServerName serverName,
      TransitionCode code) {
    if (!regionNode.isInState(State.OPENING)) {
      LOG.warn("Received report {} transition from {} for {}, pid={}, but the region is not in" +
        " OPENING state, should be a retry, ignore", code, serverName, regionNode, getProcId());
      return false;
    }
    if (getCurrentState() != REGION_STATE_TRANSITION_CONFIRM_OPENED) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={}," +
          " but the TRSP is not in {} state, should be a retry, ignore",
        code, serverName, regionNode, getProcId(), REGION_STATE_TRANSITION_CONFIRM_OPENED);
      return false;
    }
    return true;
  }

  private void reportTransitionOpen(MasterProcedureEnv env, RegionStateNode regionNode,
      ServerName serverName, long openSeqNum) throws IOException {
    if (!isOpening(regionNode, serverName, TransitionCode.OPENED)) {
      return;
    }
    if (openSeqNum < 0) {
      throw new UnexpectedStateException("Received report unexpected " + TransitionCode.OPENED +
        " transition openSeqNum=" + openSeqNum + ", " + regionNode + ", proc=" + this);
    }
    if (openSeqNum < regionNode.getOpenSeqNum()) {
      // use the openSeqNum as a fence, if this is not a retry, then the openSeqNum should be
      // greater than or equal to the existing one.
      LOG.warn(
        "Received report {} transition from {} for {}, pid={} but the new openSeqNum {}" +
          " is less than the current one {}, should be a retry, ignore",
        TransitionCode.OPENED, serverName, regionNode, getProcId(), openSeqNum,
        regionNode.getOpenSeqNum());
      return;
    }
    // notice that it is possible for a region to still have the same openSeqNum if it crashes and
    // we haven't written anything into it. That's why we can not just change the above condition
    // from '<' to '<='. So here we still need to check whether the serverName
    // matches, to determine whether this is a retry when the openSeqNum is not changed.
    if (!regionNode.getRegionLocation().equals(serverName)) {
      LOG.warn("Received report {} transition from {} for {}, pid={} but the region is not on it," +
        " should be a retry, ignore", TransitionCode.OPENED, serverName, regionNode, getProcId());
      return;
    }
    regionNode.setOpenSeqNum(openSeqNum);
    env.getAssignmentManager().regionOpened(regionNode);
    if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED) {
      // we are done
      regionNode.unsetProcedure(this);
    }
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  private void reportTransitionFailedOpen(MasterProcedureEnv env, RegionStateNode regionNode,
      ServerName serverName) {
    if (!isOpening(regionNode, serverName, TransitionCode.FAILED_OPEN)) {
      return;
    }
    // there is no openSeqNum for FAILED_OPEN, so we will check the target server instead
    if (!regionNode.getRegionLocation().equals(serverName)) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={}," +
          " but the region is not on it, should be a retry, ignore",
        TransitionCode.FAILED_OPEN, regionNode, serverName, getProcId());
      return;
    }
    // just wake up the procedure and see if we can retry
    // Notice that, even if we arrive here, this call could still be a retry, as we may retry
    // opening on the same server again. And the assumption here is that, once the region state is
    // OPENING, and the TRSP state is REGION_STATE_TRANSITION_CONFIRM_OPENED, the TRSP must have
    // been suspended on the procedure event, so after the waking operation here, the TRSP will be
    // executed and try to schedule new OpenRegionProcedure again. Once there is a successful open
    // then we are done, so the TRSP will not be stuck.
    // TODO: maybe we could send the procedure id of the OpenRegionProcedure to the region server
    // and let the region server send it back when done, so it will be easy to detect whether this
    // is a retry.
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  // we do not need seqId for closing a region
  private void reportTransitionClosed(MasterProcedureEnv env, RegionStateNode regionNode,
      ServerName serverName) throws IOException {
    if (!regionNode.isInState(State.CLOSING)) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={}" +
          ", but the region is not in CLOSING state, should be a retry, ignore",
        TransitionCode.CLOSED, serverName, regionNode, getProcId());
      return;
    }
    if (getCurrentState() != REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={} but the proc is not in {}" +
          " state, should be a retry, ignore",
        TransitionCode.CLOSED, serverName, regionNode, getProcId(),
        REGION_STATE_TRANSITION_CONFIRM_CLOSED);
      return;
    }
    if (!regionNode.getRegionLocation().equals(serverName)) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={}," +
          " but the region is not on it, should be a retry, ignore",
        TransitionCode.CLOSED, serverName, regionNode, getProcId());
      return;
    }
    env.getAssignmentManager().regionClosed(regionNode, true);
    if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
      // we are done
      regionNode.unsetProcedure(this);
    }
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  // Should be called with RegionStateNode locked
  public void reportTransition(MasterProcedureEnv env, RegionStateNode regionNode,
      ServerName serverName, TransitionCode code, long seqId) throws IOException {
    // It is possible that the previous reportRegionStateTransition call was succeeded at master
    // side, but before returning the result to region server, the rpc connection was broken, or the
    // master restarted. The region server will try calling reportRegionStateTransition again under
    // this scenario, so here we need to check whether this is a retry.
    switch (code) {
      case OPENED:
        reportTransitionOpen(env, regionNode, serverName, seqId);
        break;
      case FAILED_OPEN:
        reportTransitionFailedOpen(env, regionNode, serverName);
        break;
      case CLOSED:
        reportTransitionClosed(env, regionNode, serverName);
        break;
      default:
        throw new UnexpectedStateException("Received report unexpected " + code + " transition, " +
          regionNode.toShortString() + ", " + this + ", expected OPENED or FAILED_OPEN or CLOSED.");
    }
  }

  // Should be called with RegionStateNode locked
  public void serverCrashed(MasterProcedureEnv env, RegionStateNode regionNode,
      ServerName serverName) throws IOException {
    // Notice that, in this method, we do not change the procedure state, instead, we update the
    // region state in hbase:meta. This is because that, the procedure state change will not be
    // persisted until the region is woken up and finish one step, if we crash before that then the
    // information will be lost. So here we will update the region state in hbase:meta, and when the
    // procedure is woken up, it will process the error and jump to the correct procedure state.
    RegionStateTransitionState currentState = getCurrentState();
    switch (currentState) {
      case REGION_STATE_TRANSITION_CLOSE:
      case REGION_STATE_TRANSITION_CONFIRM_CLOSED:
      case REGION_STATE_TRANSITION_CONFIRM_OPENED:
        // for these 3 states, the region may still be online on the crashed server
        env.getAssignmentManager().regionClosed(regionNode, false);
        if (currentState != RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE) {
          regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
        }
        break;
      default:
        // If the procedure is in other 2 states, then actually we should not arrive here, as we
        // know that the region is not online on any server, so we need to do nothing... But anyway
        // let's add a log here
        LOG.warn("{} received unexpected server crash call for region {} from {}", this, regionNode,
          serverName);

    }
  }

  private boolean incrementAndCheckMaxAttempts(MasterProcedureEnv env, RegionStateNode regionNode) {
    int retries = env.getAssignmentManager().getRegionStates().addToFailedOpen(regionNode)
      .incrementAndGetRetries();
    int max = env.getAssignmentManager().getAssignMaxAttempts();
    LOG.info("Retry={} of max={}; {}; {}", retries, max, this, regionNode.toShortString());
    return retries >= max;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RegionStateTransitionState state)
      throws IOException, InterruptedException {
    // no rollback
    throw new UnsupportedOperationException();
  }

  @Override
  protected RegionStateTransitionState getState(int stateId) {
    return RegionStateTransitionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RegionStateTransitionState state) {
    return state.getNumber();
  }

  @Override
  protected RegionStateTransitionState getInitialState() {
    return initialState;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RegionStateTransitionStateData.Builder builder = RegionStateTransitionStateData.newBuilder()
      .setInitialState(initialState).setLastState(lastState).setForceNewPlan(forceNewPlan);
    if (assignCandidate != null) {
      builder.setAssignCandidate(ProtobufUtil.toServerName(assignCandidate));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RegionStateTransitionStateData data =
      serializer.deserialize(RegionStateTransitionStateData.class);
    initialState = data.getInitialState();
    lastState = data.getLastState();
    forceNewPlan = data.getForceNewPlan();
    if (data.hasAssignCandidate()) {
      assignCandidate = ProtobufUtil.toServerName(data.getAssignCandidate());
    }
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    // TODO: need to reimplement the metrics system for assign/unassign
    if (initialState == RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE) {
      return env.getAssignmentManager().getAssignmentManagerMetrics().getAssignProcMetrics();
    } else {
      return env.getAssignmentManager().getAssignmentManagerMetrics().getUnassignProcMetrics();
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    if (initialState == RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE) {
      sb.append(", ASSIGN");
    } else if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
      sb.append(", UNASSIGN");
    } else {
      sb.append(", REOPEN/MOVE");
    }
  }

  private static TransitRegionStateProcedure setOwner(MasterProcedureEnv env,
      TransitRegionStateProcedure proc) {
    proc.setOwner(env.getRequestUser().getShortName());
    return proc;
  }

  // Be careful that, when you call these 4 methods below, you need to manually attach the returned
  // procedure with the RegionStateNode, otherwise the procedure will quit immediately without doing
  // anything. See the comment in executeFromState to find out why we need this assumption.
  public static TransitRegionStateProcedure assign(MasterProcedureEnv env, RegionInfo region,
      @Nullable ServerName targetServer) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, targetServer, false,
        RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED));
  }

  public static TransitRegionStateProcedure unassign(MasterProcedureEnv env, RegionInfo region) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, null, false,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED));
  }

  public static TransitRegionStateProcedure reopen(MasterProcedureEnv env, RegionInfo region) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, null, false,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED));
  }

  public static TransitRegionStateProcedure move(MasterProcedureEnv env, RegionInfo region,
      @Nullable ServerName targetServer) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, targetServer, targetServer == null,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE,
        RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED));
  }
}
