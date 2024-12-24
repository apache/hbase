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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateRegionState;

@InterfaceAudience.Private
public class TruncateRegionProcedure
  extends AbstractSnapshottingStateMachineRegionProcedure<TruncateRegionState> {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateRegionProcedure.class);

  @SuppressWarnings("unused")
  public TruncateRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public TruncateRegionProcedure(final MasterProcedureEnv env, final RegionInfo hri)
    throws HBaseIOException {
    super(env, hri, null);
    checkOnline(env, getRegion());
  }

  public TruncateRegionProcedure(final MasterProcedureEnv env, final RegionInfo region,
    ProcedurePrepareLatch latch) throws HBaseIOException {
    super(env, region, latch);
    preflightChecks(env, true);
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, TruncateRegionState state)
    throws InterruptedException, ProcedureSuspendedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case TRUNCATE_REGION_PRE_OPERATION:
          if (!prepareTruncate()) {
            assert isFailed() : "the truncate should have an exception here";
            return Flow.NO_MORE_STATE;
          }
          checkOnline(env, getRegion());
          assert getRegion().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID || isFailed()
            : "Can't truncate replicas directly. "
              + "Replicas are auto-truncated when their primary is truncated.";
          preTruncate(env);
          setNextState(TruncateRegionState.TRUNCATE_REGION_SNAPSHOT);
          break;
        case TRUNCATE_REGION_SNAPSHOT:
          if (isSnapshotEnabled()) {
            takeSnapshot();
          } else {
            LOG.debug("Recovery snapshots are not enabled");
          }
          setNextState(TruncateRegionState.TRUNCATE_REGION_MAKE_OFFLINE);
          break;
        case TRUNCATE_REGION_MAKE_OFFLINE:
          addChildProcedure(createUnAssignProcedures(env));
          setNextState(TruncateRegionState.TRUNCATE_REGION_REMOVE);
          break;
        case TRUNCATE_REGION_REMOVE:
          deleteRegionFromFileSystem(env);
          setNextState(TruncateRegionState.TRUNCATE_REGION_MAKE_ONLINE);
          break;
        case TRUNCATE_REGION_MAKE_ONLINE:
          addChildProcedure(createAssignProcedures(env));
          setNextState(TruncateRegionState.TRUNCATE_REGION_POST_OPERATION);
          break;
        case TRUNCATE_REGION_POST_OPERATION:
          postTruncate(env);
          LOG.debug("truncate '" + getTableName() + "' completed");
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-truncate-region", e);
      } else {
        LOG.warn("Retriable error trying to truncate region=" + getRegion().getRegionNameAsString()
          + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void deleteRegionFromFileSystem(final MasterProcedureEnv env) throws IOException {
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(getRegion());
    regionNode.lock();
    try {
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
      final Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), getTableName());
      HRegionFileSystem.deleteRegionFromFileSystem(env.getMasterConfiguration(),
        mfs.getFileSystem(), tableDir, getRegion());
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final TruncateRegionState state)
    throws IOException {
    switch (state) {
      case TRUNCATE_REGION_PRE_OPERATION:
        // Nothing to rollback, pre-truncate is just table-state checks.
        break;
      case TRUNCATE_REGION_SNAPSHOT:
        if (isSnapshotEnabled()) {
          // If a snapshot exists, we should delete it.
          deleteSnapshot();
        }
        break;
      case TRUNCATE_REGION_MAKE_OFFLINE:
        RegionStateNode regionNode =
          env.getAssignmentManager().getRegionStates().getRegionStateNode(getRegion());
        if (regionNode == null) {
          // Region was unassigned by state TRUNCATE_REGION_MAKE_OFFLINE.
          // So Assign it back
          addChildProcedure(createAssignProcedures(env));
        }
        break;
      default:
        // The truncate doesn't have a rollback. The execution will succeed, at some point.
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void completionCleanup(final MasterProcedureEnv env) {
    releaseSyncLatch();
  }

  @Override
  protected boolean isRollbackSupported(final TruncateRegionState state) {
    switch (state) {
      case TRUNCATE_REGION_PRE_OPERATION:
        return true;
      case TRUNCATE_REGION_SNAPSHOT:
        return true;
      case TRUNCATE_REGION_MAKE_OFFLINE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected TruncateRegionState getState(final int stateId) {
    return TruncateRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final TruncateRegionState state) {
    return state.getNumber();
  }

  @Override
  protected TruncateRegionState getInitialState() {
    return TruncateRegionState.TRUNCATE_REGION_PRE_OPERATION;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (region=");
    sb.append(getRegion().getRegionNameAsString());
    sb.append(")");
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MasterProcedureProtos.TruncateRegionStateData.Builder state =
      MasterProcedureProtos.TruncateRegionStateData.newBuilder();
    if (getSnapshotName() != null) {
      state.setSnapshotName(getSnapshotName());
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    try {
      MasterProcedureProtos.TruncateRegionStateData state =
        serializer.deserialize(MasterProcedureProtos.TruncateRegionStateData.class);
      if (state.hasSnapshotName()) {
        setSnapshotName(state.getSnapshotName());
      }
    } catch (IOException e) {
      // Earlier versions of HBase did not serialize state for this procedure
      LOG.warn("No state to deserialize?", e);
    }
  }

  private boolean prepareTruncate() throws IOException {
    if (getTableName().equals(TableName.META_TABLE_NAME)) {
      throw new IOException("Can't truncate region in catalog tables");
    }
    return true;
  }

  private void preTruncate(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTruncateRegionAction(getRegion(), getUser());
    }
  }

  private void postTruncate(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postTruncateRegionAction(getRegion(), getUser());
    }
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_TRUNCATE;
  }

  private TransitRegionStateProcedure createUnAssignProcedures(MasterProcedureEnv env)
    throws IOException {
    return env.getAssignmentManager().createOneUnassignProcedure(getRegion(), true, true);
  }

  private TransitRegionStateProcedure createAssignProcedures(MasterProcedureEnv env) {
    return env.getAssignmentManager().createOneAssignProcedure(getRegion(), true, true);
  }
}
