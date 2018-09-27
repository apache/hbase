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
      final ServerName destinationServer, final boolean override,
      final boolean removeAfterUnassigning) {
    super(regionInfo, override);
    this.hostingServer = hostingServer;
    this.destinationServer = destinationServer;
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
    if (isOverride()) {
      state.setForce(true);
    }
    if (removeAfterUnassigning) {
      state.setRemoveAfterUnassigning(true);
    }
    if (getAttempt() > 0) {
      state.setAttempt(getAttempt());
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
    // The 'force' flag is the override flag in unassign.
    setOverride(state.getForce());
    if (state.hasDestinationServer()) {
      this.destinationServer = ProtobufUtil.toServerName(state.getDestinationServer());
    }
    removeAfterUnassigning = state.getRemoveAfterUnassigning();
    if (state.hasAttempt()) {
      setAttempt(state.getAttempt());
    }
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

    // Add the close region operation to the server dispatch queue.
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

  /**
   * Our remote call failed but there are a few states where it is safe to proceed with the
   * unassign; e.g. if a server crash and it has had all of its WALs processed, then we can allow
   * this unassign to go to completion.
   * @return True if it is safe to proceed with the unassign.
   */
  private boolean isSafeToProceed(final MasterProcedureEnv env, final RegionStateNode regionNode,
    final IOException exception) {
    if (exception instanceof ServerCrashException) {
      // This exception comes from ServerCrashProcedure AFTER log splitting. Its a signaling
      // exception. SCP found this region as a RIT during its processing of the crash.  Its call
      // into here says it is ok to let this procedure go complete.
      return true;
    }
    if (exception instanceof NotServingRegionException) {
      LOG.warn("IS OK? ANY LOGS TO REPLAY; ACTING AS THOUGH ALL GOOD {}", regionNode, exception);
      return true;
    }
    return false;
  }

  /**
   * Set it up so when procedure is unsuspended, we'll move to the procedure finish.
   */
  protected void proceed(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    try {
      reportTransition(env, regionNode, TransitionCode.CLOSED, HConstants.NO_SEQNUM);
    } catch (UnexpectedStateException e) {
      // Should never happen.
      throw new RuntimeException(e);
    }
  }

  /**
   * @return If true, we will re-wake up this procedure; if false, the procedure stays suspended.
   */
  @Override
  protected boolean remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    // Be careful reading the below; we do returns in middle of the method a few times.
    if (isSafeToProceed(env, regionNode, exception)) {
      proceed(env, regionNode);
    } else if (exception instanceof RegionServerAbortedException ||
        exception instanceof RegionServerStoppedException) {
      // RS is aborting/stopping, we cannot offline the region since the region may need to do WAL
      // recovery. Until we see the RS expiration, stay suspended; return false.
      LOG.info("Ignoring; waiting on ServerCrashProcedure", exception);
      return false;
    } else if (exception instanceof ServerNotRunningYetException) {
      // This should not happen. If it does, procedure will be woken-up and we'll retry.
      // TODO: Needs a pause and backoff?
      LOG.info("Retry", exception);
    } else {
      // We failed to RPC this server. Set it as expired.
      ServerName serverName = regionNode.getRegionLocation();
      LOG.warn("Expiring {}, {} {}; exception={}", serverName, this, regionNode.toShortString(),
          exception.getClass().getSimpleName());
      if (!env.getMasterServices().getServerManager().expireServer(serverName)) {
        // Failed to queue an expire. Lots of possible reasons including it may be already expired.
        // In ServerCrashProcedure, there is a handleRIT stage where we
        // will iterator over all the RIT procedures for the related regions of a crashed RS and
        // fail them with ServerCrashException. You can see the isSafeToProceed method above for
        // more details.
        // This can work for most cases, but since we do not hold the region lock in handleRIT,
        // there could be race that we arrive here after the handleRIT stage of the SCP. So here we
        // need to check whether it is safe to quit.
        // Notice that, the first assumption is that we can only quit after the log splitting is
        // done, as MRP can schedule an AssignProcedure right after us, and if the log splitting has
        // not been done then there will be data loss. And in SCP, we will change the state from
        // SPLITTING to OFFLINE(or SPLITTING_META_DONE for meta log processing) after finishing the
        // log splitting, and then calling handleRIT, so checking the state here can be a safe
        // fence. If the state is not OFFLINE(or SPLITTING_META_DONE), then we can just leave this
        // procedure in suspended state as we can make sure that the handleRIT has not been executed
        // yet and it will wake us up later. And if the state is OFFLINE(or SPLITTING_META_DONE), we
        // can safely quit since there will be no data loss. There could be duplicated
        // AssignProcedures for the same region but it is OK as we will do a check at the beginning
        // of AssignProcedure to prevent double assign. And there we have region lock so there will
        // be no race.
        if (env.getAssignmentManager().isLogSplittingDone(serverName, isMeta())) {
          // Its ok to proceed with this unassign.
          LOG.info("{} is dead and processed; moving procedure to finished state; {}", serverName,
            this);
          proceed(env, regionNode);
          // Return true; wake up the procedure so we can act on proceed.
          return true;
        }
      }
      // Return false so this procedure stays in suspended state. It will be woken up by the
      // ServerCrashProcedure that was scheduled when we called #expireServer above. SCP calls
      // #handleRIT which will call this method only the exception will be a ServerCrashException
      // this time around (See above).
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
