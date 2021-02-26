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

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.GCMergedRegionsState;

/**
 * GC regions that have been Merged.
 * Caller determines if it is GC time. This Procedure does not check.
 * <p>This is a Table Procedure. We take a read lock on the Table.
 * We do NOT keep a lock for the life of this procedure. The subprocedures
 * take locks on the Regions they are purging.
 * @deprecated 2.3.0 Use {@link GCMultipleMergedRegionsProcedure}.
 */
@InterfaceAudience.Private
@Deprecated
public class GCMergedRegionsProcedure
extends AbstractStateMachineTableProcedure<GCMergedRegionsState> {
  private static final Logger LOG = LoggerFactory.getLogger(GCMergedRegionsProcedure.class);
  private RegionInfo father;
  private RegionInfo mother;
  private RegionInfo mergedChild;

  public GCMergedRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo mergedChild,
      final RegionInfo father,
      final RegionInfo mother) {
    super(env);
    this.father = father;
    this.mother = mother;
    this.mergedChild = mergedChild;
  }

  public GCMergedRegionsProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
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
        // Nothing to do to prepare.
        setNextState(GCMergedRegionsState.GC_MERGED_REGIONS_PURGE);
        break;
      case GC_MERGED_REGIONS_PURGE:
        addChildProcedure(createGCRegionProcedures(env));
        setNextState(GCMergedRegionsState.GC_REGION_EDIT_METADATA);
        break;
      case GC_REGION_EDIT_METADATA:
        MetaTableAccessor.deleteMergeQualifiers(env.getMasterServices().getConnection(), mergedChild);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException ioe) {
      // TODO: This is going to spew log?
      LOG.warn("Error trying to GC merged regions " + this.father.getShortNameToLog() +
          " & " + this.mother.getShortNameToLog() + "; retrying...", ioe);
    }
    return Flow.HAS_MORE_STATE;
  }

  private GCRegionProcedure[] createGCRegionProcedures(final MasterProcedureEnv env) {
    GCRegionProcedure [] procs = new GCRegionProcedure[2];
    int index = 0;
    for (RegionInfo hri: new RegionInfo [] {this.father, this.mother}) {
      GCRegionProcedure proc = new GCRegionProcedure(env, hri);
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
    final MasterProcedureProtos.GCMergedRegionsStateData.Builder msg =
        MasterProcedureProtos.GCMergedRegionsStateData.newBuilder().
        setParentA(ProtobufUtil.toRegionInfo(this.father)).
        setParentB(ProtobufUtil.toRegionInfo(this.mother)).
        setMergedChild(ProtobufUtil.toRegionInfo(this.mergedChild));
    serializer.serialize(msg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    final MasterProcedureProtos.GCMergedRegionsStateData msg =
        serializer.deserialize(MasterProcedureProtos.GCMergedRegionsStateData.class);
    this.father = ProtobufUtil.toRegionInfo(msg.getParentA());
    this.mother = ProtobufUtil.toRegionInfo(msg.getParentB());
    this.mergedChild = ProtobufUtil.toRegionInfo(msg.getMergedChild());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" child=");
    sb.append(this.mergedChild.getShortNameToLog());
    sb.append(", father=");
    sb.append(this.father.getShortNameToLog());
    sb.append(", mother=");
    sb.append(this.mother.getShortNameToLog());
  }

  @Override
  public TableName getTableName() {
    return this.mergedChild.getTable();
  }
}
