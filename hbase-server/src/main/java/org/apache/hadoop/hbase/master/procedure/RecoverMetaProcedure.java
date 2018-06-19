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
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionTransitionProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverMetaState;


/**
 * This procedure recovers meta from prior shutdown/ crash of a server, and brings meta online by
 * assigning meta region/s. Any place where meta is accessed and requires meta to be online, need to
 * submit this procedure instead of duplicating steps to recover meta in the code.
 * <p/>
 * @deprecated Do not use any more, leave it here only for compatible. The recovery work will be
 *             done in {@link ServerCrashProcedure} directly, and the initial work for meta table
 *             will be done by {@link InitMetaProcedure}.
 * @see ServerCrashProcedure
 * @see InitMetaProcedure
 */
@Deprecated
@InterfaceAudience.Private
public class RecoverMetaProcedure
    extends StateMachineProcedure<MasterProcedureEnv, MasterProcedureProtos.RecoverMetaState>
    implements MetaProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(RecoverMetaProcedure.class);

  private ServerName failedMetaServer;
  private boolean shouldSplitWal;
  private int replicaId;

  private final ProcedurePrepareLatch syncLatch;
  private MasterServices master;

  /**
   * Call this constructor to queue up a {@link RecoverMetaProcedure} in response to meta
   * carrying server crash
   * @param failedMetaServer failed/ crashed region server that was carrying meta
   * @param shouldSplitLog split log file of meta region
   */
  public RecoverMetaProcedure(final ServerName failedMetaServer, final boolean shouldSplitLog) {
    this(failedMetaServer, shouldSplitLog, null);
  }

  /**
   * Constructor with latch, for blocking/ sync usage
   */
  public RecoverMetaProcedure(final ServerName failedMetaServer, final boolean shouldSplitLog,
                              final ProcedurePrepareLatch latch) {
    this.failedMetaServer = failedMetaServer;
    this.shouldSplitWal = shouldSplitLog;
    this.replicaId = RegionInfo.DEFAULT_REPLICA_ID;
    this.syncLatch = latch;
  }

  /**
   * This constructor is also used when deserializing from a procedure store; we'll construct one
   * of these then call #deserializeStateData(InputStream). Do not use directly.
   */
  public RecoverMetaProcedure() {
    this(null, false);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
      MasterProcedureProtos.RecoverMetaState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    prepare(env);

    if (!isRunRequired()) {
      LOG.info(this + "; Meta already initialized. Skipping run");
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case RECOVER_META_PREPARE:
          // If Master is going down or cluster is up, skip this assign by returning NO_MORE_STATE
          if (!master.isClusterUp()) {
            String msg = "Cluster not up! Skipping hbase:meta assign.";
            LOG.warn(msg);
            return Flow.NO_MORE_STATE;
          }
          if (master.isStopping() || master.isStopped()) {
            String msg = "Master stopping=" + master.isStopping() + ", stopped=" +
                master.isStopped() + "; skipping hbase:meta assign.";
            LOG.warn(msg);
            return Flow.NO_MORE_STATE;
          }
          setNextState(RecoverMetaState.RECOVER_META_SPLIT_LOGS);
          break;
        case RECOVER_META_SPLIT_LOGS:
          LOG.info("Start " + this);
          if (shouldSplitWal) {
            // TODO: Matteo. We BLOCK here but most important thing to be doing at this moment.
            AssignmentManager am = env.getMasterServices().getAssignmentManager();
            if (failedMetaServer != null) {
              am.getRegionStates().metaLogSplitting(failedMetaServer);
              master.getMasterWalManager().splitMetaLog(failedMetaServer);
              am.getRegionStates().metaLogSplit(failedMetaServer);
            } else {
              ServerName serverName =
                  master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper());
              Set<ServerName> previouslyFailedServers =
                  master.getMasterWalManager().getFailedServersFromLogFolders();
              if (serverName != null && previouslyFailedServers.contains(serverName)) {
                am.getRegionStates().metaLogSplitting(serverName);
                master.getMasterWalManager().splitMetaLog(serverName);
                am.getRegionStates().metaLogSplit(serverName);
              }
            }
          }
          setNextState(RecoverMetaState.RECOVER_META_ASSIGN_REGIONS);
          break;
        case RECOVER_META_ASSIGN_REGIONS:
          RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(
              RegionInfoBuilder.FIRST_META_REGIONINFO, this.replicaId);

          AssignProcedure metaAssignProcedure;
          AssignmentManager am = master.getAssignmentManager();
          if (failedMetaServer != null) {
            handleRIT(env, hri, this.failedMetaServer);
            LOG.info(this + "; Assigning meta with new plan; previous server=" + failedMetaServer);
            metaAssignProcedure = am.createAssignProcedure(hri);
          } else {
            // get server carrying meta from zk
            ServerName metaServer =
                MetaTableLocator.getMetaRegionState(master.getZooKeeper()).getServerName();
            LOG.info(this + "; Retaining meta assignment to server=" + metaServer);
            metaAssignProcedure = am.createAssignProcedure(hri, metaServer);
          }

          addChildProcedure(metaAssignProcedure);
          return Flow.NO_MORE_STATE;

        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException|KeeperException e) {
      LOG.warn(this + "; Failed state=" + state + ", retry " + this + "; cycles=" +
          getCycles(), e);
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Is the region stuck assigning to this failedMetaServer? If so, cancel the call
   * just as we do over in ServerCrashProcedure#handleRIT except less to do here; less context
   * to carry.
   */
  // NOTE: Make sure any fix or improvement done here is also done in SCP#handleRIT; the methods
  // have overlap.
  private void handleRIT(MasterProcedureEnv env, RegionInfo ri, ServerName crashedServerName) {
    AssignmentManager am = env.getAssignmentManager();
    RegionTransitionProcedure rtp = am.getRegionStates().getRegionTransitionProcedure(ri);
    if (rtp == null) {
      return; // Nothing to do. Not in RIT.
    }
    // Make sure the RIT is against this crashed server. In the case where there are many
    // processings of a crashed server -- backed up for whatever reason (slow WAL split)
    // -- then a previous SCP may have already failed an assign, etc., and it may have a
    // new location target; DO NOT fail these else we make for assign flux.
    ServerName rtpServerName = rtp.getServer(env);
    if (rtpServerName == null) {
      LOG.warn("RIT with ServerName null! " + rtp);
    } else if (rtpServerName.equals(crashedServerName)) {
      LOG.info("pid=" + getProcId() + " found RIT " + rtp + "; " +
          rtp.getRegionState(env).toShortString());
      rtp.remoteCallFailed(env, crashedServerName,
          new ServerCrashException(getProcId(), crashedServerName));
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
      MasterProcedureProtos.RecoverMetaState recoverMetaState)
      throws IOException, InterruptedException {
    // Can't rollback
    throw new UnsupportedOperationException("unhandled state=" + recoverMetaState);
  }

  @Override
  protected MasterProcedureProtos.RecoverMetaState getState(int stateId) {
    return RecoverMetaState.forNumber(stateId);
  }

  @Override
  protected int getStateId(MasterProcedureProtos.RecoverMetaState recoverMetaState) {
    return recoverMetaState.getNumber();
  }

  @Override
  protected MasterProcedureProtos.RecoverMetaState getInitialState() {
    return RecoverMetaState.RECOVER_META_PREPARE;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" failedMetaServer=");
    sb.append(failedMetaServer);
    sb.append(", splitWal=");
    sb.append(shouldSplitWal);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);
    MasterProcedureProtos.RecoverMetaStateData.Builder state =
        MasterProcedureProtos.RecoverMetaStateData.newBuilder().setShouldSplitWal(shouldSplitWal);
    if (failedMetaServer != null) {
      state.setFailedMetaServer(ProtobufUtil.toServerName(failedMetaServer));
    }
    state.setReplicaId(replicaId);
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    MasterProcedureProtos.RecoverMetaStateData state =
        serializer.deserialize(MasterProcedureProtos.RecoverMetaStateData.class);
    this.shouldSplitWal = state.hasShouldSplitWal() && state.getShouldSplitWal();
    this.failedMetaServer = state.hasFailedMetaServer() ?
        ProtobufUtil.toServerName(state.getFailedMetaServer()) : null;
    this.replicaId = state.hasReplicaId() ? state.getReplicaId() : RegionInfo.DEFAULT_REPLICA_ID;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitMetaExclusiveLock(this)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeMetaExclusiveLock(this);
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);
  }

  /**
   * @return true if failedMetaServer is not null (meta carrying server crashed) or meta is
   * already initialized
   */
  private boolean isRunRequired() {
    return failedMetaServer != null || !master.getAssignmentManager().isMetaAssigned();
  }

  /**
   * Prepare for execution
   */
  private void prepare(MasterProcedureEnv env) {
    if (master == null) {
      master = env.getMasterServices();
      Preconditions.checkArgument(master != null);
    }
  }
}
