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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ServerCrashState;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Handle crashed server. This is a port to ProcedureV2 of what used to be euphemistically called
 * ServerShutdownHandler.
 *
 * <p>The procedure flow varies dependent on whether meta is assigned, if we are
 * doing distributed log replay versus distributed log splitting, and if we are to split logs at
 * all.
 *
 * <p>This procedure asks that all crashed servers get processed equally; we yield after the
 * completion of each successful flow step. We do this so that we do not 'deadlock' waiting on
 * a region assignment so we can replay edits which could happen if a region moved there are edits
 * on two servers for replay.
 *
 * <p>TODO: ASSIGN and WAIT_ON_ASSIGN (at least) are not idempotent. Revisit when assign is pv2.
 * TODO: We do not have special handling for system tables.
 */
public class ServerCrashProcedure
extends StateMachineProcedure<MasterProcedureEnv, ServerCrashState>
implements ServerProcedureInterface {
  private static final Log LOG = LogFactory.getLog(ServerCrashProcedure.class);

  /**
   * Configuration key to set how long to wait in ms doing a quick check on meta state.
   */
  public static final String KEY_SHORT_WAIT_ON_META =
      "hbase.master.servercrash.short.wait.on.meta.ms";

  public static final int DEFAULT_SHORT_WAIT_ON_META = 1000;

  /**
   * Configuration key to set how many retries to cycle before we give up on meta.
   * Each attempt will wait at least {@link #KEY_SHORT_WAIT_ON_META} milliseconds.
   */
  public static final String KEY_RETRIES_ON_META =
      "hbase.master.servercrash.meta.retries";

  public static final int DEFAULT_RETRIES_ON_META = 10;

  /**
   * Configuration key to set how long to wait in ms on regions in transition.
   */
  public static final String KEY_WAIT_ON_RIT =
      "hbase.master.servercrash.wait.on.rit.ms";

  public static final int DEFAULT_WAIT_ON_RIT = 30000;

  private static final Set<HRegionInfo> META_REGION_SET = new HashSet<HRegionInfo>();
  static {
    META_REGION_SET.add(HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * Name of the crashed server to process.
   */
  private ServerName serverName;

  /**
   * Regions that were on the crashed server.
   */
  private Set<HRegionInfo> regionsOnCrashedServer;

  /**
   * Regions to assign. Usually some subset of {@link #regionsOnCrashedServer}
   */
  private List<HRegionInfo> regionsToAssign;

  private boolean distributedLogReplay = false;
  private boolean carryingMeta = false;
  private boolean shouldSplitWal;

  /**
   * Cycles on same state. Good for figuring if we are stuck.
   */
  private int cycles = 0;

  /**
   * Ordinal of the previous state. So we can tell if we are progressing or not. TODO: if useful,
   * move this back up into StateMachineProcedure
   */
  private int previousState;

  /**
   * Call this constructor queuing up a Procedure.
   * @param serverName Name of the crashed server.
   * @param shouldSplitWal True if we should split WALs as part of crashed server processing.
   * @param carryingMeta True if carrying hbase:meta table region.
   */
  public ServerCrashProcedure(final ServerName serverName,
      final boolean shouldSplitWal, final boolean carryingMeta) {
    this.serverName = serverName;
    this.shouldSplitWal = shouldSplitWal;
    this.carryingMeta = carryingMeta;
    // Currently not used.
  }

  /**
   * Used when deserializing from a procedure store; we'll construct one of these then call
   * {@link #deserializeStateData(InputStream)}. Do not use directly.
   */
  public ServerCrashProcedure() {
    super();
  }

  private void throwProcedureYieldException(final String msg) throws ProcedureYieldException {
    String logMsg = msg + "; cycle=" + this.cycles + ", running for " +
        StringUtils.formatTimeDiff(System.currentTimeMillis(), getStartTime());
    // The procedure executor logs ProcedureYieldException at trace level. For now, log these
    // yields for server crash processing at DEBUG. Revisit when stable.
    if (LOG.isDebugEnabled()) LOG.debug(logMsg);
    throw new ProcedureYieldException(logMsg);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ServerCrashState state)
  throws ProcedureYieldException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(state);
    }
    // Keep running count of cycles
    if (state.ordinal() != this.previousState) {
      this.previousState = state.ordinal();
      this.cycles = 0;
    } else {
      this.cycles++;
    }
    MasterServices services = env.getMasterServices();
    try {
      switch (state) {
      case SERVER_CRASH_START:
        // Is master fully online? If not, yield. No processing of servers unless master is up
        if (!services.getAssignmentManager().isFailoverCleanupDone()) {
          throwProcedureYieldException("Waiting on master failover to complete");
        }
        LOG.info("Start processing crashed " + this.serverName);
        start(env);
        // If carrying meta, process it first. Else, get list of regions on crashed server.
        if (this.carryingMeta) setNextState(ServerCrashState.SERVER_CRASH_PROCESS_META);
        else setNextState(ServerCrashState.SERVER_CRASH_GET_REGIONS);
        break;

      case SERVER_CRASH_GET_REGIONS:
        // If hbase:meta is not assigned, yield.
        if (!isMetaAssignedQuickTest(env)) {
          throwProcedureYieldException("Waiting on hbase:meta assignment");
        }
        this.regionsOnCrashedServer =
          services.getAssignmentManager().getRegionStates().getServerRegions(this.serverName);
        // Where to go next? Depends on whether we should split logs at all or if we should do
        // distributed log splitting (DLS) vs distributed log replay (DLR).
        if (!this.shouldSplitWal) {
          setNextState(ServerCrashState.SERVER_CRASH_CALC_REGIONS_TO_ASSIGN);
        } else if (this.distributedLogReplay) {
          setNextState(ServerCrashState.SERVER_CRASH_PREPARE_LOG_REPLAY);
        } else {
          setNextState(ServerCrashState.SERVER_CRASH_SPLIT_LOGS);
        }
        break;

      case SERVER_CRASH_PROCESS_META:
        // If we fail processing hbase:meta, yield.
        if (!processMeta(env)) {
          throwProcedureYieldException("Waiting on regions-in-transition to clear");
        }
        setNextState(ServerCrashState.SERVER_CRASH_GET_REGIONS);
        break;

      case SERVER_CRASH_PREPARE_LOG_REPLAY:
        prepareLogReplay(env, this.regionsOnCrashedServer);
        setNextState(ServerCrashState.SERVER_CRASH_CALC_REGIONS_TO_ASSIGN);
        break;

      case SERVER_CRASH_SPLIT_LOGS:
        splitLogs(env);
        // If DLR, go to FINISH. Otherwise, if DLS, go to SERVER_CRASH_CALC_REGIONS_TO_ASSIGN
        if (this.distributedLogReplay) setNextState(ServerCrashState.SERVER_CRASH_FINISH);
        else setNextState(ServerCrashState.SERVER_CRASH_CALC_REGIONS_TO_ASSIGN);
        break;

      case SERVER_CRASH_CALC_REGIONS_TO_ASSIGN:
        this.regionsToAssign = calcRegionsToAssign(env);
        setNextState(ServerCrashState.SERVER_CRASH_ASSIGN);
        break;

      case SERVER_CRASH_ASSIGN:
        // Assign may not be idempotent. SSH used to requeue the SSH if we got an IOE assigning
        // which is what we are mimicing here but it looks prone to double assignment if assign
        // fails midway. TODO: Test.

        // If no regions to assign, skip assign and skip to the finish.
        boolean regions = this.regionsToAssign != null && !this.regionsToAssign.isEmpty();
        if (regions) {
          if (!assign(env, this.regionsToAssign)) {
            throwProcedureYieldException("Failed assign; will retry");
          }
        }
        if (regions && this.shouldSplitWal && distributedLogReplay) {
          setNextState(ServerCrashState.SERVER_CRASH_WAIT_ON_ASSIGN);
        } else {
          setNextState(ServerCrashState.SERVER_CRASH_FINISH);
        }
        break;

      case SERVER_CRASH_WAIT_ON_ASSIGN:
        // TODO: The list of regionsToAssign may be more than we actually assigned. See down in
        // AM #1629 around 'if (regionStates.wasRegionOnDeadServer(encodedName)) {' where where we
        // will skip assigning a region because it is/was on a dead server. Should never happen!
        // It was on this server. Worst comes to worst, we'll still wait here till other server is
        // processed.

        // If the wait on assign failed, yield -- if we have regions to assign.
        if (this.regionsToAssign != null && !this.regionsToAssign.isEmpty()) {
          if (!waitOnAssign(env, this.regionsToAssign)) {
            throwProcedureYieldException("Waiting on region assign");
          }
        }
        setNextState(ServerCrashState.SERVER_CRASH_SPLIT_LOGS);
        break;

      case SERVER_CRASH_FINISH:
        LOG.info("Finished processing of crashed " + serverName);
        services.getServerManager().getDeadServers().finish(serverName);
        return Flow.NO_MORE_STATE;

      default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Failed serverName=" + this.serverName + ", state=" + state + "; retry", e);
    } catch (InterruptedException e) {
      // TODO: Make executor allow IEs coming up out of execute.
      LOG.warn("Interrupted serverName=" + this.serverName + ", state=" + state + "; retry", e);
      Thread.currentThread().interrupt();
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Start processing of crashed server. In here we'll just set configs. and return.
   * @param env
   * @throws IOException
   */
  private void start(final MasterProcedureEnv env) throws IOException {
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    // Set recovery mode late. This is what the old ServerShutdownHandler used do.
    mfs.setLogRecoveryMode();
    this.distributedLogReplay = mfs.getLogRecoveryMode() == RecoveryMode.LOG_REPLAY;
  }

  /**
   * @param env
   * @return False if we fail to assign and split logs on meta ('process').
   * @throws IOException
   * @throws InterruptedException
   */
  private boolean processMeta(final MasterProcedureEnv env)
  throws IOException {
    if (LOG.isDebugEnabled()) LOG.debug("Processing hbase:meta that was on " + this.serverName);
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    HRegionInfo metaHRI = HRegionInfo.FIRST_META_REGIONINFO;
    if (this.shouldSplitWal) {
      if (this.distributedLogReplay) {
        prepareLogReplay(env, META_REGION_SET);
      } else {
        // TODO: Matteo. We BLOCK here but most important thing to be doing at this moment.
        mfs.splitMetaLog(serverName);
        am.getRegionStates().logSplit(metaHRI);
      }
    }

    // Assign meta if still carrying it. Check again: region may be assigned because of RIT timeout
    boolean processed = true;
    if (am.isCarryingMeta(serverName)) {
      // TODO: May block here if hard time figuring state of meta.
      am.regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
      verifyAndAssignMetaWithRetries(env);
      if (this.shouldSplitWal && distributedLogReplay) {
        int timeout = env.getMasterConfiguration().getInt(KEY_WAIT_ON_RIT, DEFAULT_WAIT_ON_RIT);
        if (!waitOnRegionToClearRegionsInTransition(am, metaHRI, timeout)) {
          processed = false;
        } else {
          // TODO: Matteo. We BLOCK here but most important thing to be doing at this moment.
          mfs.splitMetaLog(serverName);
        }
      }
    }
    return processed;
  }

  /**
   * @return True if region cleared RIT, else false if we timed out waiting.
   * @throws InterruptedIOException
   */
  private boolean waitOnRegionToClearRegionsInTransition(AssignmentManager am,
      final HRegionInfo hri, final int timeout)
  throws InterruptedIOException {
    try {
      if (!am.waitOnRegionToClearRegionsInTransition(hri, timeout)) {
        // Wait here is to avoid log replay hits current dead server and incur a RPC timeout
        // when replay happens before region assignment completes.
        LOG.warn("Region " + hri.getEncodedName() + " didn't complete assignment in time");
        return false;
      }
    } catch (InterruptedException ie) {
      throw new InterruptedIOException("Caught " + ie +
        " during waitOnRegionToClearRegionsInTransition for " + hri);
    }
    return true;
  }

  private void prepareLogReplay(final MasterProcedureEnv env, final Set<HRegionInfo> regions)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Mark " + size(this.regionsOnCrashedServer) +
        " regions-in-recovery from " + this.serverName);
    }
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    mfs.prepareLogReplay(this.serverName, regions);
    am.getRegionStates().logSplit(this.serverName);
  }

  private void splitLogs(final MasterProcedureEnv env) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Splitting logs from " + serverName + "; region count=" +
        size(this.regionsOnCrashedServer));
    }
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    // TODO: For Matteo. Below BLOCKs!!!! Redo so can relinquish executor while it is running.
    mfs.splitLog(this.serverName);
    am.getRegionStates().logSplit(this.serverName);
  }

  static int size(final Collection<HRegionInfo> hris) {
    return hris == null? 0: hris.size();
  }

  /**
   * Figure out what we need to assign. Should be idempotent.
   * @param env
   * @return List of calculated regions to assign; may be empty or null.
   * @throws IOException
   */
  private List<HRegionInfo> calcRegionsToAssign(final MasterProcedureEnv env)
  throws IOException {
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    List<HRegionInfo> regionsToAssignAggregator = new ArrayList<HRegionInfo>();
    int replicaCount = env.getMasterConfiguration().getInt(HConstants.META_REPLICAS_NUM,
      HConstants.DEFAULT_META_REPLICA_NUM);
    for (int i = 1; i < replicaCount; i++) {
      HRegionInfo metaHri =
          RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, i);
      if (am.isCarryingMetaReplica(this.serverName, metaHri)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reassigning meta replica" + metaHri + " that was on " + this.serverName);
        }
        regionsToAssignAggregator.add(metaHri);
      }
    }
    // Clean out anything in regions in transition.
    List<HRegionInfo> regionsInTransition = am.cleanOutCrashedServerReferences(serverName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reassigning " + size(this.regionsOnCrashedServer) +
        " region(s) that " + (serverName == null? "null": serverName)  +
        " was carrying (and " + regionsInTransition.size() +
        " regions(s) that were opening on this server)");
    }
    regionsToAssignAggregator.addAll(regionsInTransition);

    // Iterate regions that were on this server and figure which of these we need to reassign
    if (this.regionsOnCrashedServer != null && !this.regionsOnCrashedServer.isEmpty()) {
      RegionStates regionStates = am.getRegionStates();
      for (HRegionInfo hri: this.regionsOnCrashedServer) {
        if (regionsInTransition.contains(hri)) continue;
        String encodedName = hri.getEncodedName();
        Lock lock = am.acquireRegionLock(encodedName);
        try {
          RegionState rit = regionStates.getRegionTransitionState(hri);
          if (processDeadRegion(hri, am)) {
            ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
            if (addressFromAM != null && !addressFromAM.equals(this.serverName)) {
              // If this region is in transition on the dead server, it must be
              // opening or pending_open, which should have been covered by
              // AM#cleanOutCrashedServerReferences
              LOG.info("Skip assigning region " + hri.getRegionNameAsString()
                + " because it has been opened in " + addressFromAM.getServerName());
              continue;
            }
            if (rit != null) {
              if (rit.getServerName() != null && !rit.isOnServer(this.serverName)) {
                // Skip regions that are in transition on other server
                LOG.info("Skip assigning region in transition on other server" + rit);
                continue;
              }
              LOG.info("Reassigning region " + rit + " and clearing zknode if exists");
              try {
                // This clears out any RIT that might be sticking around.
                ZKAssign.deleteNodeFailSilent(env.getMasterServices().getZooKeeper(), hri);
              } catch (KeeperException e) {
                // TODO: FIX!!!! ABORTING SERVER BECAUSE COULDN"T PURGE ZNODE. This is what we
                // used to do but that doesn't make it right!!!
                env.getMasterServices().abort("Unexpected error deleting RIT " + hri, e);
                throw new IOException(e);
              }
              regionStates.updateRegionState(hri, RegionState.State.OFFLINE);
            } else if (regionStates.isRegionInState(
                hri, RegionState.State.SPLITTING_NEW, RegionState.State.MERGING_NEW)) {
              regionStates.updateRegionState(hri, RegionState.State.OFFLINE);
            }
            regionsToAssignAggregator.add(hri);
          // TODO: The below else if is different in branch-1 from master branch.
          } else if (rit != null) {
            if ((rit.isPendingCloseOrClosing() || rit.isOffline())
                && am.getTableStateManager().isTableState(hri.getTable(),
                ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING) ||
                am.getReplicasToClose().contains(hri)) {
              // If the table was partially disabled and the RS went down, we should clear the
              // RIT and remove the node for the region.
              // The rit that we use may be stale in case the table was in DISABLING state
              // but though we did assign we will not be clearing the znode in CLOSING state.
              // Doing this will have no harm. See HBASE-5927
              regionStates.updateRegionState(hri, RegionState.State.OFFLINE);
              am.deleteClosingOrClosedNode(hri, rit.getServerName());
              am.offlineDisabledRegion(hri);
            } else {
              LOG.warn("THIS SHOULD NOT HAPPEN: unexpected region in transition "
                + rit + " not to be assigned by SSH of server " + serverName);
            }
          }
        } finally {
          lock.unlock();
        }
      }
    }
    return regionsToAssignAggregator;
  }

  private boolean assign(final MasterProcedureEnv env, final List<HRegionInfo> hris)
  throws InterruptedIOException {
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    try {
      am.assign(hris);
    } catch (InterruptedException ie) {
      LOG.error("Caught " + ie + " during round-robin assignment");
      throw (InterruptedIOException)new InterruptedIOException().initCause(ie);
    } catch (IOException ioe) {
      LOG.info("Caught " + ioe + " during region assignment, will retry");
      return false;
    }
    return true;
  }

  private boolean waitOnAssign(final MasterProcedureEnv env, final List<HRegionInfo> hris)
  throws InterruptedIOException {
    int timeout = env.getMasterConfiguration().getInt(KEY_WAIT_ON_RIT, DEFAULT_WAIT_ON_RIT);
    for (HRegionInfo hri: hris) {
      // TODO: Blocks here.
      if (!waitOnRegionToClearRegionsInTransition(env.getMasterServices().getAssignmentManager(),
          hri, timeout)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ServerCrashState state)
  throws IOException {
    // Can't rollback.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected ServerCrashState getState(int stateId) {
    return ServerCrashState.valueOf(stateId);
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
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (!env.getMasterServices().isServerCrashProcessingEnabled()) return false;
    return env.getProcedureQueue().tryAcquireServerExclusiveLock(this);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseServerExclusiveLock(this);
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" serverName=");
    sb.append(this.serverName);
    sb.append(", shouldSplitWal=");
    sb.append(shouldSplitWal);
    sb.append(", carryingMeta=");
    sb.append(carryingMeta);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.ServerCrashStateData.Builder state =
      MasterProcedureProtos.ServerCrashStateData.newBuilder().
      setServerName(ProtobufUtil.toServerName(this.serverName)).
      setDistributedLogReplay(this.distributedLogReplay).
      setCarryingMeta(this.carryingMeta).
      setShouldSplitWal(this.shouldSplitWal);
    if (this.regionsOnCrashedServer != null && !this.regionsOnCrashedServer.isEmpty()) {
      for (HRegionInfo hri: this.regionsOnCrashedServer) {
        state.addRegionsOnCrashedServer(HRegionInfo.convert(hri));
      }
    }
    if (this.regionsToAssign != null && !this.regionsToAssign.isEmpty()) {
      for (HRegionInfo hri: this.regionsToAssign) {
        state.addRegionsToAssign(HRegionInfo.convert(hri));
      }
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.ServerCrashStateData state =
      MasterProcedureProtos.ServerCrashStateData.parseDelimitedFrom(stream);
    this.serverName = ProtobufUtil.toServerName(state.getServerName());
    this.distributedLogReplay = state.hasDistributedLogReplay()?
      state.getDistributedLogReplay(): false;
    this.carryingMeta = state.hasCarryingMeta()? state.getCarryingMeta(): false;
    // shouldSplitWAL has a default over in pb so this invocation will always work.
    this.shouldSplitWal = state.getShouldSplitWal();
    int size = state.getRegionsOnCrashedServerCount();
    if (size > 0) {
      this.regionsOnCrashedServer = new HashSet<HRegionInfo>(size);
      for (RegionInfo ri: state.getRegionsOnCrashedServerList()) {
        this.regionsOnCrashedServer.add(HRegionInfo.convert(ri));
      }
    }
    size = state.getRegionsToAssignCount();
    if (size > 0) {
      this.regionsToAssign = new ArrayList<HRegionInfo>(size);
      for (RegionInfo ri: state.getRegionsOnCrashedServerList()) {
        this.regionsToAssign.add(HRegionInfo.convert(ri));
      }
    }
  }

  /**
   * Process a dead region from a dead RS. Checks if the region is disabled or
   * disabling or if the region has a partially completed split.
   * @param hri
   * @param assignmentManager
   * @return Returns true if specified region should be assigned, false if not.
   * @throws IOException
   */
  private static boolean processDeadRegion(HRegionInfo hri, AssignmentManager assignmentManager)
  throws IOException {
    boolean tablePresent = assignmentManager.getTableStateManager().isTablePresent(hri.getTable());
    if (!tablePresent) {
      LOG.info("The table " + hri.getTable() + " was deleted.  Hence not proceeding.");
      return false;
    }
    // If table is not disabled but the region is offlined,
    boolean disabled = assignmentManager.getTableStateManager().isTableState(hri.getTable(),
      ZooKeeperProtos.Table.State.DISABLED);
    if (disabled){
      LOG.info("The table " + hri.getTable() + " was disabled.  Hence not proceeding.");
      return false;
    }
    if (hri.isOffline() && hri.isSplit()) {
      // HBASE-7721: Split parent and daughters are inserted into hbase:meta as an atomic operation.
      // If the meta scanner saw the parent split, then it should see the daughters as assigned
      // to the dead server. We don't have to do anything.
      return false;
    }
    boolean disabling = assignmentManager.getTableStateManager().isTableState(hri.getTable(),
      ZooKeeperProtos.Table.State.DISABLING);
    if (disabling) {
      LOG.info("The table " + hri.getTable() + " is disabled.  Hence not assigning region" +
        hri.getEncodedName());
      return false;
    }
    return true;
  }

  /**
   * If hbase:meta is not assigned already, assign.
   * @throws IOException
   */
  private void verifyAndAssignMetaWithRetries(final MasterProcedureEnv env) throws IOException {
    MasterServices services = env.getMasterServices();
    int iTimes = services.getConfiguration().getInt(KEY_RETRIES_ON_META, DEFAULT_RETRIES_ON_META);
    // Just reuse same time as we have for short wait on meta. Adding another config is overkill.
    long waitTime =
      services.getConfiguration().getLong(KEY_SHORT_WAIT_ON_META, DEFAULT_SHORT_WAIT_ON_META);
    int iFlag = 0;
    while (true) {
      try {
        verifyAndAssignMeta(env);
        break;
      } catch (KeeperException e) {
        services.abort("In server shutdown processing, assigning meta", e);
        throw new IOException("Aborting", e);
      } catch (Exception e) {
        if (iFlag >= iTimes) {
          services.abort("verifyAndAssignMeta failed after" + iTimes + " retries, aborting", e);
          throw new IOException("Aborting", e);
        }
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted when is the thread sleep", e1);
          Thread.currentThread().interrupt();
          throw (InterruptedIOException)new InterruptedIOException().initCause(e1);
        }
        iFlag++;
      }
    }
  }

  /**
   * If hbase:meta is not assigned already, assign.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void verifyAndAssignMeta(final MasterProcedureEnv env)
      throws InterruptedException, IOException, KeeperException {
    MasterServices services = env.getMasterServices();
    if (!isMetaAssignedQuickTest(env)) {
      services.getAssignmentManager().assignMeta(HRegionInfo.FIRST_META_REGIONINFO);
    } else if (serverName.equals(services.getMetaTableLocator().
        getMetaRegionLocation(services.getZooKeeper()))) {
      // hbase:meta seems to be still alive on the server whom master is expiring
      // and thinks is dying. Let's re-assign the hbase:meta anyway.
      services.getAssignmentManager().assignMeta(HRegionInfo.FIRST_META_REGIONINFO);
    } else {
      LOG.info("Skip assigning hbase:meta because it is online at "
          + services.getMetaTableLocator().getMetaRegionLocation(services.getZooKeeper()));
    }
  }

  /**
   * A quick test that hbase:meta is assigned; blocks for short time only.
   * @return True if hbase:meta location is available and verified as good.
   * @throws InterruptedException
   * @throws IOException
   */
  private boolean isMetaAssignedQuickTest(final MasterProcedureEnv env)
  throws InterruptedException, IOException {
    ZooKeeperWatcher zkw = env.getMasterServices().getZooKeeper();
    MetaTableLocator mtl = env.getMasterServices().getMetaTableLocator();
    boolean metaAssigned = false;
    // Is hbase:meta location available yet?
    if (mtl.isLocationAvailable(zkw)) {
      ClusterConnection connection = env.getMasterServices().getConnection();
      // Is hbase:meta location good yet?
      long timeout =
        env.getMasterConfiguration().getLong(KEY_SHORT_WAIT_ON_META, DEFAULT_SHORT_WAIT_ON_META);
      if (mtl.verifyMetaRegionLocation(connection, zkw, timeout)) {
        metaAssigned = true;
      }
    }
    return metaAssigned;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return this.carryingMeta;
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
}
