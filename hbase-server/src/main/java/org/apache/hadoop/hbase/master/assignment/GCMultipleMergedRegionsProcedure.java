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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.GCMergedRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.GCMultipleMergedRegionsStateData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GC regions that have been Merged. Caller determines if it is GC time. This Procedure does not
 * check. This is a Table Procedure. We take a read lock on the Table. We do NOT keep a lock for
 * the life of this procedure. The sub-procedures take locks on the Regions they are purging.
 * Replaces a Procedure that did two regions only at a time instead doing multiple merges in the
 * one go; only difference from the old {@link GCMergedRegionsState} is the serialization; this
 * class has a different serialization profile writing out more than just two regions.
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class GCMultipleMergedRegionsProcedure extends
    AbstractStateMachineTableProcedure<GCMergedRegionsState> {
  private static final Logger LOG = LoggerFactory.getLogger(GCMultipleMergedRegionsProcedure.class);
  private List<RegionInfo> parents;
  private RegionInfo mergedChild;

  public GCMultipleMergedRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo mergedChild, final List<RegionInfo> parents) {
    super(env);
    this.parents = parents;
    this.mergedChild = mergedChild;
  }

  public GCMultipleMergedRegionsProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  @Override protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    // It now takes an exclusive lock on the merged child region to make sure
    // that no two parallel running of two GCMultipleMergedRegionsProcedures on the
    // region.
    if (env.getProcedureScheduler().waitRegion(this, mergedChild)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegion(this, mergedChild);
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.MERGED_REGIONS_GC;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, GCMergedRegionsState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case GC_MERGED_REGIONS_PREPARE:
          // If GCMultipleMergedRegionsProcedure processing is slower than the CatalogJanitor's scan
          // interval, it will end resubmitting GCMultipleMergedRegionsProcedure for the same
          // region. We can skip duplicate GCMultipleMergedRegionsProcedure while previous finished
          List<RegionInfo> parents = MetaTableAccessor.getMergeRegions(
            env.getMasterServices().getConnection(), mergedChild.getRegionName());
          if (parents == null || parents.isEmpty()) {
            LOG.info("{} mergeXXX qualifiers have ALL been deleted", mergedChild.getShortNameToLog());
            return Flow.NO_MORE_STATE;
          }
          setNextState(GCMergedRegionsState.GC_MERGED_REGIONS_PURGE);
          break;
        case GC_MERGED_REGIONS_PURGE:
          addChildProcedure(createGCRegionProcedures(env));
          setNextState(GCMergedRegionsState.GC_REGION_EDIT_METADATA);
          break;
        case GC_REGION_EDIT_METADATA:
          MetaTableAccessor.deleteMergeQualifiers(env.getMasterServices().getConnection(),
              mergedChild);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException ioe) {
      // TODO: This is going to spew log?
      LOG.warn("Error trying to GC merged regions {}; retrying...",
          this.parents.stream().map(r -> RegionInfo.getShortNameToLog(r)).
              collect(Collectors.joining(", ")),
          ioe);
    }
    return Flow.HAS_MORE_STATE;
  }

  private GCRegionProcedure[] createGCRegionProcedures(final MasterProcedureEnv env) {
    GCRegionProcedure [] procs = new GCRegionProcedure[this.parents.size()];
    int index = 0;
    for (RegionInfo ri: this.parents) {
      GCRegionProcedure proc = new GCRegionProcedure(env, ri);
      proc.setOwner(env.getRequestUser().getShortName());
      procs[index++] = proc;
    }
    return procs;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, GCMergedRegionsState state)
      throws IOException, InterruptedException {
    // no-op
  }

  @Override
  protected GCMergedRegionsState getState(int stateId) {
    return GCMergedRegionsState.forNumber(stateId);
  }

  @Override
  protected int getStateId(GCMergedRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected GCMergedRegionsState getInitialState() {
    return GCMergedRegionsState.GC_MERGED_REGIONS_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);
    final GCMultipleMergedRegionsStateData.Builder msg =
        GCMultipleMergedRegionsStateData.newBuilder().
            addAllParents(this.parents.stream().map(ProtobufUtil::toRegionInfo).
                collect(Collectors.toList())).
            setMergedChild(ProtobufUtil.toRegionInfo(this.mergedChild));
    serializer.serialize(msg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    final GCMultipleMergedRegionsStateData msg =
        serializer.deserialize(GCMultipleMergedRegionsStateData.class);
    this.parents = msg.getParentsList().stream().map(ProtobufUtil::toRegionInfo).
        collect(Collectors.toList());
    this.mergedChild = ProtobufUtil.toRegionInfo(msg.getMergedChild());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" child=");
    sb.append(this.mergedChild.getShortNameToLog());
    sb.append(", parents:");
    sb.append(this.parents.stream().map(r -> RegionInfo.getShortNameToLog(r)).
        collect(Collectors.joining(", ")));
  }

  @Override
  public TableName getTableName() {
    return this.mergedChild.getTable();
  }
}
