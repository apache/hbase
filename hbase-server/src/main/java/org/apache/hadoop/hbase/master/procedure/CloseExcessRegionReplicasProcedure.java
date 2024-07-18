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
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseExcessRegionReplicasProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseExcessRegionReplicasProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Procedure for close excess region replicas.
 */
@InterfaceAudience.Private
public class CloseExcessRegionReplicasProcedure
  extends AbstractStateMachineTableProcedure<CloseExcessRegionReplicasProcedureState> {

  private static final Logger LOG =
    LoggerFactory.getLogger(CloseExcessRegionReplicasProcedure.class);

  private TableName tableName;
  private int newReplicaCount;

  private RetryCounter retryCounter;

  public CloseExcessRegionReplicasProcedure() {
  }

  public CloseExcessRegionReplicasProcedure(TableName tableName, int newReplicaCount) {
    this.tableName = tableName;
    this.newReplicaCount = newReplicaCount;
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
  protected Flow executeFromState(MasterProcedureEnv env,
    CloseExcessRegionReplicasProcedureState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.trace("{} execute state={}", this, state);
    switch (state) {
      case CLOSE_EXCESS_REGION_REPLICAS_SCHEDULE:
        MutableBoolean submitted = new MutableBoolean(false);
        int inTransitionCount = env.getAssignmentManager()
          .submitUnassignProcedureForClosingExcessRegionReplicas(tableName, newReplicaCount, p -> {
            submitted.setTrue();
            addChildProcedure(p);
          });
        if (inTransitionCount > 0 && submitted.isFalse()) {
          // we haven't scheduled any unassign procedures and there are still regions in
          // transition, sleep for a while and try again
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoffMillis = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.info(
            "There are still {} region(s) in transition for table {} when closing excess"
              + " region replicas, suspend {}secs and try again later",
            inTransitionCount, tableName, backoffMillis / 1000);
          suspend((int) backoffMillis, true);
        }
        setNextState(CloseExcessRegionReplicasProcedureState.CLOSE_EXCESS_REGION_REPLICAS_CONFIRM);
        return Flow.HAS_MORE_STATE;
      case CLOSE_EXCESS_REGION_REPLICAS_CONFIRM:
        int unclosedCount = env.getAssignmentManager()
          .numberOfUnclosedExcessRegionReplicas(tableName, newReplicaCount);
        if (unclosedCount > 0) {
          LOG.info("There are still {} unclosed region(s) for table {} when closing excess"
            + " region replicas, continue...");
          setNextState(
            CloseExcessRegionReplicasProcedureState.CLOSE_EXCESS_REGION_REPLICAS_SCHEDULE);
          return Flow.HAS_MORE_STATE;
        } else {
          return Flow.NO_MORE_STATE;
        }
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
    CloseExcessRegionReplicasProcedureState state) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CloseExcessRegionReplicasProcedureState getState(int stateId) {
    return CloseExcessRegionReplicasProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(CloseExcessRegionReplicasProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected CloseExcessRegionReplicasProcedureState getInitialState() {
    return CloseExcessRegionReplicasProcedureState.CLOSE_EXCESS_REGION_REPLICAS_SCHEDULE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    CloseExcessRegionReplicasProcedureStateData data = CloseExcessRegionReplicasProcedureStateData
      .newBuilder().setTableName(ProtobufUtil.toProtoTableName(tableName))
      .setNewReplicaCount(newReplicaCount).build();
    serializer.serialize(data);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseExcessRegionReplicasProcedureStateData data =
      serializer.deserialize(CloseExcessRegionReplicasProcedureStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    newReplicaCount = data.getNewReplicaCount();
  }

}
