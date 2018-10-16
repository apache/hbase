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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Used for reopening the regions for a table.
 */
@InterfaceAudience.Private
public class ReopenTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<ReopenTableRegionsState> {

  private static final Logger LOG = LoggerFactory.getLogger(ReopenTableRegionsProcedure.class);

  private TableName tableName;

  private List<HRegionLocation> regions = Collections.emptyList();

  private int attempt;

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
        for (HRegionLocation loc : regions) {
          RegionStateNode regionNode = env.getAssignmentManager().getRegionStates()
            .getOrCreateRegionStateNode(loc.getRegion());
          TransitRegionStateProcedure proc;
          regionNode.lock();
          try {
            if (regionNode.getProcedure() != null) {
              continue;
            }
            proc = TransitRegionStateProcedure.reopen(env, regionNode.getRegionInfo());
            regionNode.setProcedure(proc);
          } finally {
            regionNode.unlock();
          }
          addChildProcedure(proc);
        }
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_CONFIRM_REOPENED);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_CONFIRM_REOPENED:
        regions = regions.stream().map(env.getAssignmentManager().getRegionStates()::checkReopened)
          .filter(l -> l != null).collect(Collectors.toList());
        if (regions.isEmpty()) {
          return Flow.NO_MORE_STATE;
        }
        if (regions.stream().anyMatch(l -> l.getSeqNum() >= 0)) {
          attempt = 0;
          setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
          return Flow.HAS_MORE_STATE;
        }
        // All the regions need to reopen are in OPENING state which means we can not schedule any
        // MRPs.
        long backoff = ProcedureUtil.getBackoffTimeMs(this.attempt++);
        LOG.info(
          "There are still {} region(s) which need to be reopened for table {} are in " +
            "OPENING state, suspend {}secs and try again later",
          regions.size(), tableName, backoff / 1000);
        setTimeout(Math.toIntExact(backoff));
        setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
        skipPersistence();
        throw new ProcedureSuspendedException();
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  /**
   * At end of timeout, wake ourselves up so we run again.
   */
  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }
  @Override
  protected void rollbackState(MasterProcedureEnv env, ReopenTableRegionsState state)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("unhandled state=" + state);
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
