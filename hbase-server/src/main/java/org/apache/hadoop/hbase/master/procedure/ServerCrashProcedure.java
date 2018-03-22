/*
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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterWalManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionTransitionProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ServerCrashState;

/**
 * Handle crashed server. This is a port to ProcedureV2 of what used to be euphemistically called
 * ServerShutdownHandler.
 *
 * <p>The procedure flow varies dependent on whether meta is assigned and if we are to split logs.
 *
 * <p>We come in here after ServerManager has noticed a server has expired. Procedures
 * queued on the rpc should have been notified about fail and should be concurrently
 * getting themselves ready to assign elsewhere.
 */
@InterfaceAudience.Private
public class ServerCrashProcedure
extends StateMachineProcedure<MasterProcedureEnv, ServerCrashState>
implements ServerProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(ServerCrashProcedure.class);

  /**
   * Name of the crashed server to process.
   */
  private ServerName serverName;

  /**
   * Whether DeadServer knows that we are processing it.
   */
  private boolean notifiedDeadServer = false;

  /**
   * Regions that were on the crashed server.
   */
  private List<RegionInfo> regionsOnCrashedServer;

  private boolean carryingMeta = false;
  private boolean shouldSplitWal;

  /**
   * Call this constructor queuing up a Procedure.
   * @param serverName Name of the crashed server.
   * @param shouldSplitWal True if we should split WALs as part of crashed server processing.
   * @param carryingMeta True if carrying hbase:meta table region.
   */
  public ServerCrashProcedure(
      final MasterProcedureEnv env,
      final ServerName serverName,
      final boolean shouldSplitWal,
      final boolean carryingMeta) {
    this.serverName = serverName;
    this.shouldSplitWal = shouldSplitWal;
    this.carryingMeta = carryingMeta;
    this.setOwner(env.getRequestUser());
  }

  /**
   * Used when deserializing from a procedure store; we'll construct one of these then call
   * #deserializeStateData(InputStream). Do not use directly.
   */
  public ServerCrashProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ServerCrashState state)
      throws ProcedureSuspendedException, ProcedureYieldException {
    final MasterServices services = env.getMasterServices();
    // HBASE-14802
    // If we have not yet notified that we are processing a dead server, we should do now.
    if (!notifiedDeadServer) {
      services.getServerManager().getDeadServers().notifyServer(serverName);
      notifiedDeadServer = true;
    }

    try {
      switch (state) {
        case SERVER_CRASH_START:
          LOG.info("Start " + this);
          // If carrying meta, process it first. Else, get list of regions on crashed server.
          if (this.carryingMeta) {
            setNextState(ServerCrashState.SERVER_CRASH_PROCESS_META);
          } else {
            setNextState(ServerCrashState.SERVER_CRASH_GET_REGIONS);
          }
          break;

        case SERVER_CRASH_GET_REGIONS:
          // If hbase:meta is not assigned, yield.
          if (env.getAssignmentManager().waitMetaLoaded(this)) {
            throw new ProcedureSuspendedException();
          }

          this.regionsOnCrashedServer = services.getAssignmentManager().getRegionStates()
            .getServerRegionInfoSet(serverName);
          // Where to go next? Depends on whether we should split logs at all or
          // if we should do distributed log splitting.
          if (!this.shouldSplitWal) {
            setNextState(ServerCrashState.SERVER_CRASH_ASSIGN);
          } else {
            setNextState(ServerCrashState.SERVER_CRASH_SPLIT_LOGS);
          }
          break;

        case SERVER_CRASH_PROCESS_META:
          processMeta(env);
          setNextState(ServerCrashState.SERVER_CRASH_GET_REGIONS);
          break;

        case SERVER_CRASH_SPLIT_LOGS:
          splitLogs(env);
          setNextState(ServerCrashState.SERVER_CRASH_ASSIGN);
          break;

        case SERVER_CRASH_ASSIGN:
          // If no regions to assign, skip assign and skip to the finish.
          // Filter out meta regions. Those are handled elsewhere in this procedure.
          // Filter changes this.regionsOnCrashedServer.
          if (filterDefaultMetaRegions(regionsOnCrashedServer)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Assigning regions " +
                RegionInfo.getShortNameToLog(regionsOnCrashedServer) + ", " + this +
                "; cycles=" + getCycles());
            }
            // Handle RIT against crashed server. Will cancel any ongoing assigns/unassigns.
            // Returns list of regions we need to reassign.
            List<RegionInfo> toAssign = handleRIT(env, regionsOnCrashedServer);
            AssignmentManager am = env.getAssignmentManager();
            // CreateAssignProcedure will try to use the old location for the region deploy.
            addChildProcedure(am.createAssignProcedures(toAssign));
            setNextState(ServerCrashState.SERVER_CRASH_HANDLE_RIT2);
          } else {
            setNextState(ServerCrashState.SERVER_CRASH_FINISH);
          }
          break;

        case SERVER_CRASH_HANDLE_RIT2:
          // Run the handleRIT again for case where another procedure managed to grab the lock on
          // a region ahead of this crash handling procedure. Can happen in rare case. See
          handleRIT(env, regionsOnCrashedServer);
          setNextState(ServerCrashState.SERVER_CRASH_FINISH);
          break;

        case SERVER_CRASH_FINISH:
          services.getAssignmentManager().getRegionStates().removeServer(serverName);
          services.getServerManager().getDeadServers().finish(serverName);
          return Flow.NO_MORE_STATE;

        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Failed state=" + state + ", retry " + this + "; cycles=" + getCycles(), e);
    }
    return Flow.HAS_MORE_STATE;
  }


  /**
   * @param env
   * @throws IOException
   */
  private void processMeta(final MasterProcedureEnv env) throws IOException {
    LOG.debug("{}; processing hbase:meta", this);

    // Assign meta if still carrying it. Check again: region may be assigned because of RIT timeout
    final AssignmentManager am = env.getMasterServices().getAssignmentManager();
    for (RegionInfo hri: am.getRegionStates().getServerRegionInfoSet(serverName)) {
      if (!isDefaultMetaRegion(hri)) {
        continue;
      }
      addChildProcedure(new RecoverMetaProcedure(serverName, this.shouldSplitWal));
    }
  }

  private boolean filterDefaultMetaRegions(final List<RegionInfo> regions) {
    if (regions == null) return false;
    regions.removeIf(this::isDefaultMetaRegion);
    return !regions.isEmpty();
  }

  private boolean isDefaultMetaRegion(final RegionInfo hri) {
    return hri.getTable().equals(TableName.META_TABLE_NAME) &&
      RegionReplicaUtil.isDefaultReplica(hri);
  }

  private void splitLogs(final MasterProcedureEnv env) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Splitting WALs " + this);
    }
    MasterWalManager mwm = env.getMasterServices().getMasterWalManager();
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    // TODO: For Matteo. Below BLOCKs!!!! Redo so can relinquish executor while it is running.
    // PROBLEM!!! WE BLOCK HERE.
    mwm.splitLog(this.serverName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Done splitting WALs " + this);
    }
    am.getRegionStates().logSplit(this.serverName);
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ServerCrashState state)
  throws IOException {
    // Can't rollback.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected ServerCrashState getState(int stateId) {
    return ServerCrashState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ServerCrashState state) {
    return state.getNumber();
  }

  @Override
  protected ServerCrashState getInitialState() {
    return ServerCrashState.SERVER_CRASH_START;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO
    return false;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    // TODO: Put this BACK AFTER AMv2 goes in!!!!
    // if (env.waitFailoverCleanup(this)) return LockState.LOCK_EVENT_WAIT;
    if (env.waitServerCrashProcessingEnabled(this)) return LockState.LOCK_EVENT_WAIT;
    if (env.getProcedureScheduler().waitServerExclusiveLock(this, getServerName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeServerExclusiveLock(this, getServerName());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=");
    sb.append(serverName);
    sb.append(", splitWal=");
    sb.append(shouldSplitWal);
    sb.append(", meta=");
    sb.append(carryingMeta);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.ServerCrashStateData.Builder state =
      MasterProcedureProtos.ServerCrashStateData.newBuilder().
      setServerName(ProtobufUtil.toServerName(this.serverName)).
      setCarryingMeta(this.carryingMeta).
      setShouldSplitWal(this.shouldSplitWal);
    if (this.regionsOnCrashedServer != null && !this.regionsOnCrashedServer.isEmpty()) {
      for (RegionInfo hri: this.regionsOnCrashedServer) {
        state.addRegionsOnCrashedServer(ProtobufUtil.toRegionInfo(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.ServerCrashStateData state =
        serializer.deserialize(MasterProcedureProtos.ServerCrashStateData.class);
    this.serverName = ProtobufUtil.toServerName(state.getServerName());
    this.carryingMeta = state.hasCarryingMeta()? state.getCarryingMeta(): false;
    // shouldSplitWAL has a default over in pb so this invocation will always work.
    this.shouldSplitWal = state.getShouldSplitWal();
    int size = state.getRegionsOnCrashedServerCount();
    if (size > 0) {
      this.regionsOnCrashedServer = new ArrayList<>(size);
      for (org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo ri: state.getRegionsOnCrashedServerList()) {
        this.regionsOnCrashedServer.add(ProtobufUtil.toRegionInfo(ri));
      }
    }
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return this.carryingMeta;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.CRASH_HANDLER;
  }

  /**
   * For this procedure, yield at end of each successful flow step so that all crashed servers
   * can make progress rather than do the default which has each procedure running to completion
   * before we move to the next. For crashed servers, especially if running with distributed log
   * replay, we will want all servers to come along; we do not want the scenario where a server is
   * stuck waiting for regions to online so it can replay edits.
   */
  @Override
  protected boolean isYieldBeforeExecuteFromState(MasterProcedureEnv env, ServerCrashState state) {
    return true;
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // The operation is triggered internally on the server
    // the client does not know about this procedure.
    return false;
  }

  /**
   * Handle any outstanding RIT that are up against this.serverName, the crashed server.
   * Notify them of crash. Remove assign entries from the passed in <code>regions</code>
   * otherwise we have two assigns going on and they will fight over who has lock.
   * Notify Unassigns. If unable to unassign because server went away, unassigns block waiting
   * on the below callback from a ServerCrashProcedure before proceeding.
   * @param regions Regions on the Crashed Server.
   * @return List of regions we should assign to new homes (not same as regions on crashed server).
   */
  private List<RegionInfo> handleRIT(final MasterProcedureEnv env, List<RegionInfo> regions) {
    if (regions == null || regions.isEmpty()) {
      return Collections.emptyList();
    }
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    List<RegionInfo> toAssign = new ArrayList<RegionInfo>(regions);
    // Get an iterator so can remove items.
    final Iterator<RegionInfo> it = toAssign.iterator();
    ServerCrashException sce = null;
    while (it.hasNext()) {
      final RegionInfo hri = it.next();
      RegionTransitionProcedure rtp = am.getRegionStates().getRegionTransitionProcedure(hri);
      if (rtp == null) {
        continue;
      }
      // Make sure the RIT is against this crashed server. In the case where there are many
      // processings of a crashed server -- backed up for whatever reason (slow WAL split) --
      // then a previous SCP may have already failed an assign, etc., and it may have a new
      // location target; DO NOT fail these else we make for assign flux.
      ServerName rtpServerName = rtp.getServer(env);
      if (rtpServerName == null) {
        LOG.warn("RIT with ServerName null! " + rtp);
        continue;
      }
      if (!rtpServerName.equals(this.serverName)) continue;
      LOG.info("pid=" + getProcId() + " found RIT " + rtp + "; " +
        rtp.getRegionState(env).toShortString());
      // Notify RIT on server crash.
      if (sce == null) {
        sce = new ServerCrashException(getProcId(), getServerName());
      }
      rtp.remoteCallFailed(env, this.serverName, sce);
      // If an assign, remove from passed-in list of regions so we subsequently do not create
      // a new assign; the exisitng assign after the call to remoteCallFailed will recalibrate
      // and assign to a server other than the crashed one; no need to create new assign.
      // If an unassign, do not return this region; the above cancel will wake up the unassign and
      // it will complete. Done.
      it.remove();
    }
    return toAssign;
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getMasterServices().getMasterMetrics().getServerCrashProcMetrics();
  }
}
