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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionCloseOperation;
import org.apache.hadoop.hbase.master.procedure.ServerCrashException;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UnassignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Procedure that describes the unassignment of a single region.
 * There can only be one RegionTransitionProcedure -- i.e. an assign or an unassign -- per region
 * running at a time, since each procedure takes a lock on the region.
 *
 * <p>The Unassign starts by placing a "close region" request in the Remote Dispatcher
 * queue, and the procedure will then go into a "waiting state" (suspend).
 * The Remote Dispatcher will batch the various requests for that server and
 * they will be sent to the RS for execution.
 * The RS will complete the open operation by calling master.reportRegionStateTransition().
 * The AM will intercept the transition report, and notify this procedure.
 * The procedure will wakeup and finish the unassign by publishing its new state on meta.
 * <p>If we are unable to contact the remote regionserver whether because of ConnectException
 * or socket timeout, we will call expire on the server we were trying to contact. We will remain
 * in suspended state waiting for a wake up from the ServerCrashProcedure that is processing the
 * failed server. The basic idea is that if we notice a crashed server, then we have a
 * responsibility; i.e. we should not let go of the region until we are sure the server that was
 * hosting has had its crash processed. If we let go of the region before then, an assign might
 * run before the logs have been split which would make for data loss.
 *
 * <p>TODO: Rather than this tricky coordination between SCP and this Procedure, instead, work on
 * returning a SCP as our subprocedure; probably needs work on the framework to do this,
 * especially if the SCP already created.
 */
@InterfaceAudience.Private
public class UnassignProcedure extends RegionTransitionProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(UnassignProcedure.class);

  /**
   * Where to send the unassign RPC.
   */
  protected volatile ServerName hostingServer;
  /**
   * The Server we will subsequently assign the region too (can be null).
   */
  protected volatile ServerName destinationServer;

  // TODO: should this be in a reassign procedure?
  //       ...and keep unassign for 'disable' case?
  private boolean force;

  /**
   * Whether deleting the region from in-memory states after unassigning the region.
   */
  private boolean removeAfterUnassigning;

  public UnassignProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public UnassignProcedure(final RegionInfo regionInfo, final ServerName hostingServer,
      final boolean force, final boolean removeAfterUnassigning) {
    this(regionInfo, hostingServer, null, force, removeAfterUnassigning);
  }

  public UnassignProcedure(final RegionInfo regionInfo,
      final ServerName hostingServer, final ServerName destinationServer, final boolean force) {
    this(regionInfo, hostingServer, destinationServer, force, false);
  }

  public UnassignProcedure(final RegionInfo regionInfo, final ServerName hostingServer,
      final ServerName destinationServer, final boolean force,
      final boolean removeAfterUnassigning) {
    super(regionInfo);
    this.hostingServer = hostingServer;
    this.destinationServer = destinationServer;
    this.force = force;
    this.removeAfterUnassigning = removeAfterUnassigning;

    // we don't need REGION_TRANSITION_QUEUE, we jump directly to sending the request
    setTransitionState(RegionTransitionState.REGION_TRANSITION_DISPATCH);
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_UNASSIGN;
  }

  @Override
  protected boolean isRollbackSupported(final RegionTransitionState state) {
    switch (state) {
      case REGION_TRANSITION_QUEUE:
      case REGION_TRANSITION_DISPATCH:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    UnassignRegionStateData.Builder state = UnassignRegionStateData.newBuilder()
        .setTransitionState(getTransitionState())
        .setHostingServer(ProtobufUtil.toServerName(this.hostingServer))
        .setRegionInfo(ProtobufUtil.toRegionInfo(getRegionInfo()));
    if (this.destinationServer != null) {
      state.setDestinationServer(ProtobufUtil.toServerName(destinationServer));
    }
    if (force) {
      state.setForce(true);
    }
    if (removeAfterUnassigning) {
      state.setRemoveAfterUnassigning(true);
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    final UnassignRegionStateData state =
        serializer.deserialize(UnassignRegionStateData.class);
    setTransitionState(state.getTransitionState());
    setRegionInfo(ProtobufUtil.toRegionInfo(state.getRegionInfo()));
    this.hostingServer = ProtobufUtil.toServerName(state.getHostingServer());
    force = state.getForce();
    if (state.hasDestinationServer()) {
      this.destinationServer = ProtobufUtil.toServerName(state.getDestinationServer());
    }
    removeAfterUnassigning = state.getRemoveAfterUnassigning();
  }

  @Override
  protected boolean startTransition(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    // nothing to do here. we skip the step in the constructor
    // by jumping to REGION_TRANSITION_DISPATCH
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean updateTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
        throws IOException {
    // if the region is already closed or offline we can't do much...
    if (regionNode.isInState(State.CLOSED, State.OFFLINE)) {
      LOG.info("Not unassigned " + this + "; " + regionNode.toShortString());
      return false;
    }

    // if we haven't started the operation yet, we can abort
    if (aborted.get() && regionNode.isInState(State.OPEN)) {
      setAbortFailure(getClass().getSimpleName(), "abort requested");
      return false;
    }

    // Mark the region as CLOSING.
    env.getAssignmentManager().markRegionAsClosing(regionNode);

    // Add the close region operation the the server dispatch queue.
    if (!addToRemoteDispatcher(env, regionNode.getRegionLocation())) {
      // If addToRemoteDispatcher fails, it calls the callback #remoteCallFailed.
    }

    // Return true to keep the procedure running.
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    AssignmentManager am = env.getAssignmentManager();
    RegionInfo regionInfo = getRegionInfo();

    if (!removeAfterUnassigning) {
      am.markRegionAsClosed(regionNode);
    } else {
      // Remove from in-memory states
      am.getRegionStates().deleteRegion(regionInfo);
      env.getMasterServices().getServerManager().removeRegion(regionInfo);
      FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();
      if (fnm != null) {
        fnm.deleteFavoredNodesForRegions(Lists.newArrayList(regionInfo));
      }
    }
  }

  @Override
  public RemoteOperation remoteCallBuild(final MasterProcedureEnv env, final ServerName serverName) {
    assert serverName.equals(getRegionState(env).getRegionLocation());
    return new RegionCloseOperation(this, getRegionInfo(), this.destinationServer);
  }

  @Override
  protected void reportTransition(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final TransitionCode code, final long seqId) throws UnexpectedStateException {
    switch (code) {
      case CLOSED:
        setTransitionState(RegionTransitionState.REGION_TRANSITION_FINISH);
        break;
      default:
        throw new UnexpectedStateException(String.format(
          "Received report unexpected transition state=%s for region=%s server=%s, expected CLOSED.",
          code, regionNode.getRegionInfo(), regionNode.getRegionLocation()));
    }
  }

  @Override
  protected boolean remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    // TODO: Is there on-going rpc to cleanup?
    if (exception instanceof ServerCrashException) {
      // This exception comes from ServerCrashProcedure after log splitting.
      // SCP found this region as a RIT. Its call into here says it is ok to let this procedure go
      // on to a complete close now. This will release lock on this region so subsequent action on
      // region can succeed; e.g. the assign that follows this unassign when a move (w/o wait on SCP
      // the assign could run w/o logs being split so data loss).
      try {
        reportTransition(env, regionNode, TransitionCode.CLOSED, HConstants.NO_SEQNUM);
      } catch (UnexpectedStateException e) {
        // Should never happen.
        throw new RuntimeException(e);
      }
    } else if (exception instanceof RegionServerAbortedException ||
        exception instanceof RegionServerStoppedException ||
        exception instanceof ServerNotRunningYetException) {
      // TODO
      // RS is aborting, we cannot offline the region since the region may need to do WAL
      // recovery. Until we see the RS expiration, we should retry.
      // TODO: This should be suspend like the below where we call expire on server?
      LOG.info("Ignoring; waiting on ServerCrashProcedure", exception);
    } else if (exception instanceof NotServingRegionException) {
      LOG.info("IS THIS OK? ANY LOGS TO REPLAY; ACTING AS THOUGH ALL GOOD " + regionNode,
        exception);
      setTransitionState(RegionTransitionState.REGION_TRANSITION_FINISH);
    } else {
      LOG.warn("Expiring server " + this + "; " + regionNode.toShortString() +
        ", exception=" + exception);
      env.getMasterServices().getServerManager().expireServer(regionNode.getRegionLocation());
      // Return false so this procedure stays in suspended state. It will be woken up by a
      // ServerCrashProcedure when it notices this RIT.
      // TODO: Add a SCP as a new subprocedure that we now come to depend on.
      return false;
    }
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    sb.append(", server=").append(this.hostingServer);
  }

  @Override
  public ServerName getServer(final MasterProcedureEnv env) {
    return this.hostingServer;
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getUnassignProcMetrics();
  }
}
