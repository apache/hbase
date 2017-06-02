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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashException;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionCloseOperation;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UnassignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;


/**
 * Procedure that describe the unassignment of a single region.
 * There can only be one RegionTransitionProcedure per region running at the time,
 * since each procedure takes a lock on the region.
 *
 * <p>The Unassign starts by placing a "close region" request in the Remote Dispatcher
 * queue, and the procedure will then go into a "waiting state".
 * The Remote Dispatcher will batch the various requests for that server and
 * they will be sent to the RS for execution.
 * The RS will complete the open operation by calling master.reportRegionStateTransition().
 * The AM will intercept the transition report, and notify the procedure.
 * The procedure will finish the unassign by publishing its new state on meta
 * or it will retry the unassign.
 */
@InterfaceAudience.Private
public class UnassignProcedure extends RegionTransitionProcedure {
  private static final Log LOG = LogFactory.getLog(UnassignProcedure.class);

  /**
   * Where to send the unassign RPC.
   */
  protected volatile ServerName hostingServer;
  /**
   * The Server we will subsequently assign the region too (can be null).
   */
  protected volatile ServerName destinationServer;

  protected final AtomicBoolean serverCrashed = new AtomicBoolean(false);

  // TODO: should this be in a reassign procedure?
  //       ...and keep unassign for 'disable' case?
  private boolean force;

  public UnassignProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public UnassignProcedure(final HRegionInfo regionInfo,  final ServerName hostingServer,
                           final boolean force) {
    this(regionInfo, hostingServer, null, force);
  }

  public UnassignProcedure(final HRegionInfo regionInfo,
      final ServerName hostingServer, final ServerName destinationServer, final boolean force) {
    super(regionInfo);
    this.hostingServer = hostingServer;
    this.destinationServer = destinationServer;
    this.force = force;

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
  public void serializeStateData(final OutputStream stream) throws IOException {
    UnassignRegionStateData.Builder state = UnassignRegionStateData.newBuilder()
        .setTransitionState(getTransitionState())
        .setHostingServer(ProtobufUtil.toServerName(this.hostingServer))
        .setRegionInfo(HRegionInfo.convert(getRegionInfo()));
    if (this.destinationServer != null) {
      state.setDestinationServer(ProtobufUtil.toServerName(destinationServer));
    }
    if (force) {
      state.setForce(true);
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    final UnassignRegionStateData state = UnassignRegionStateData.parseDelimitedFrom(stream);
    setTransitionState(state.getTransitionState());
    setRegionInfo(HRegionInfo.convert(state.getRegionInfo()));
    this.hostingServer = ProtobufUtil.toServerName(state.getHostingServer());
    force = state.getForce();
    if (state.hasDestinationServer()) {
      this.destinationServer = ProtobufUtil.toServerName(state.getDestinationServer());
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

    // if the server is down, mark the operation as complete
    if (serverCrashed.get() || !isServerOnline(env, regionNode)) {
      LOG.info("Server already down: " + this + "; " + regionNode.toShortString());
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
      // If addToRemoteDispatcher fails, it calls #remoteCallFailed which
      // does all cleanup.
    }

    // We always return true, even if we fail dispatch because addToRemoteDispatcher
    // failure processing sets state back to REGION_TRANSITION_QUEUE so we try again;
    // i.e. return true to keep the Procedure running; it has been reset to startover.
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    env.getAssignmentManager().markRegionAsClosed(regionNode);
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
  protected void remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    // TODO: Is there on-going rpc to cleanup?
    if (exception instanceof ServerCrashException) {
      // This exception comes from ServerCrashProcedure after log splitting.
      // It is ok to let this procedure go on to complete close now.
      // This will release lock on this region so the subsequent assign can succeed.
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
      LOG.info("Ignoring; waiting on ServerCrashProcedure", exception);
      // serverCrashed.set(true);
    } else if (exception instanceof NotServingRegionException) {
      LOG.info("IS THIS OK? ANY LOGS TO REPLAY; ACTING AS THOUGH ALL GOOD " + regionNode, exception);
      setTransitionState(RegionTransitionState.REGION_TRANSITION_FINISH);
    } else {
      // TODO: kill the server in case we get an exception we are not able to handle
      LOG.warn("Killing server; unexpected exception; " +
          this + "; " + regionNode.toShortString() +
        " exception=" + exception);
      env.getMasterServices().getServerManager().expireServer(regionNode.getRegionLocation());
      serverCrashed.set(true);
    }
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