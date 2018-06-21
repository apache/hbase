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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.assignment.MoveRegionProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsStateData;

/**
 * Used for reopening the regions for a table.
 * <p/>
 * Currently we use {@link MoveRegionProcedure} to reopen regions.
 */
@InterfaceAudience.Private
public class ReopenTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<ReopenTableRegionsState> {

  private static final Logger LOG = LoggerFactory.getLogger(ReopenTableRegionsProcedure.class);

  private TableName tableName;

  private List<HRegionLocation> regions = Collections.emptyList();

  public ReopenTableRegionsProcedure() {
  }

  public ReopenTableRegionsProcedure(TableName tableName) {
    this.tableName = tableName;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  private MoveRegionProcedure createReopenProcedure(MasterProcedureEnv env, HRegionLocation loc) {
    try {
      return new MoveRegionProcedure(env,
        new RegionPlan(loc.getRegion(), loc.getServerName(), loc.getServerName()), false);
    } catch (HBaseIOException e) {
      // we skip the checks so this should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ReopenTableRegionsState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REOPEN_TABLE_REGIONS_GET_REGIONS:
        if (!env.getAssignmentManager().isTableEnabled(tableName)) {
          LOG.info("Table {} is disabled, give up reopening its regions");
          return Flow.NO_MORE_STATE;
        }
        regions =
          env.getAssignmentManager().getRegionStates().getRegionsOfTableForReopen(tableName);
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_REOPEN_REGIONS:
        addChildProcedure(regions.stream().filter(l -> l.getSeqNum() >= 0)
          .map(l -> createReopenProcedure(env, l)).toArray(MoveRegionProcedure[]::new));
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_CONFIRM_REOPENED);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_CONFIRM_REOPENED:
        regions = regions.stream().map(env.getAssignmentManager().getRegionStates()::checkReopened)
          .filter(l -> l != null).collect(Collectors.toList());
        if (regions.isEmpty()) {
          return Flow.NO_MORE_STATE;
        }
        if (regions.stream().anyMatch(l -> l.getSeqNum() >= 0)) {
          setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
          return Flow.HAS_MORE_STATE;
        }
        LOG.info("There are still {} region(s) which need to be reopened for table {} are in " +
          "OPENING state, try again later", regions.size(), tableName);
        // All the regions need to reopen are in OPENING state which means we can not schedule any
        // MRPs. Then sleep for one second, and yield the procedure to let other procedures run
        // first and hope next time we can get some regions in other state to make progress.
        // TODO: add a delay for ProcedureYieldException so that we do not need to sleep here which
        // blocks a procedure worker.
        Thread.sleep(1000);
        throw new ProcedureYieldException();
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ReopenTableRegionsState state)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ReopenTableRegionsState getState(int stateId) {
    return ReopenTableRegionsState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ReopenTableRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected ReopenTableRegionsState getInitialState() {
    return ReopenTableRegionsState.REOPEN_TABLE_REGIONS_GET_REGIONS;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    ReopenTableRegionsStateData.Builder builder = ReopenTableRegionsStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName));
    regions.stream().map(ProtobufUtil::toRegionLocation).forEachOrdered(builder::addRegion);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ReopenTableRegionsStateData data = serializer.deserialize(ReopenTableRegionsStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    regions = data.getRegionList().stream().map(ProtobufUtil::toRegionLocation)
      .collect(Collectors.toList());
  }
}
