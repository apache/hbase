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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverMetaState;

import com.google.common.base.Preconditions;

/**
 * This procedure recovers meta from prior shutdown/ crash of a server, and brings meta online by
 * assigning meta region/s. Any place where meta is accessed and requires meta to be online, need to
 * submit this procedure instead of duplicating steps to recover meta in the code.
 */
public class RecoverMetaProcedure
    extends StateMachineProcedure<MasterProcedureEnv, MasterProcedureProtos.RecoverMetaState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(RecoverMetaProcedure.class);

  private ServerName failedMetaServer;
  private boolean shouldSplitWal;
  private int replicaId;

  private final ProcedurePrepareLatch syncLatch;
  private HMaster master;

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
        case RECOVER_META_SPLIT_LOGS:
          LOG.info("Start " + this);
          if (shouldSplitWal) {
            // TODO: Matteo. We BLOCK here but most important thing to be doing at this moment.
            if (failedMetaServer != null) {
              master.getMasterWalManager().splitMetaLog(failedMetaServer);
            } else {
              ServerName serverName =
                  master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper());
              Set<ServerName> previouslyFailedServers =
                  master.getMasterWalManager().getFailedServersFromLogFolders();
              if (serverName != null && previouslyFailedServers.contains(serverName)) {
                master.getMasterWalManager().splitMetaLog(serverName);
              }
            }
          }
          setNextState(RecoverMetaState.RECOVER_META_ASSIGN_REGIONS);
          break;

        case RECOVER_META_ASSIGN_REGIONS:
          RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(
              RegionInfoBuilder.FIRST_META_REGIONINFO, this.replicaId);

          AssignProcedure metaAssignProcedure;
          if (failedMetaServer != null) {
            LOG.info(this + "; Assigning meta with new plan. previous meta server=" +
                failedMetaServer);
            metaAssignProcedure = master.getAssignmentManager().createAssignProcedure(hri);
          } else {
            // get server carrying meta from zk
            ServerName metaServer =
                MetaTableLocator.getMetaRegionState(master.getZooKeeper()).getServerName();
            LOG.info(this + "; Retaining meta assignment to server=" + metaServer);
            metaAssignProcedure =
                master.getAssignmentManager().createAssignProcedure(hri, metaServer);
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
    return RecoverMetaState.RECOVER_META_SPLIT_LOGS;
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
    if (env.getProcedureScheduler().waitTableExclusiveLock(this, TableName.META_TABLE_NAME)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeTableExclusiveLock(this, TableName.META_TABLE_NAME);
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);
  }

  @Override
  public TableName getTableName() {
    return TableName.META_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.ENABLE;
  }

  /**
   * @return true if failedMetaServer is not null (meta carrying server crashed) or meta is
   * already initialized
   */
  private boolean isRunRequired() {
    return failedMetaServer != null || !master.getAssignmentManager().isMetaInitialized();
  }

  /**
   * Prepare for execution
   */
  private void prepare(MasterProcedureEnv env) {
    if (master == null) {
      master = (HMaster) env.getMasterServices();
      Preconditions.checkArgument(master != null);
    }
  }
}
