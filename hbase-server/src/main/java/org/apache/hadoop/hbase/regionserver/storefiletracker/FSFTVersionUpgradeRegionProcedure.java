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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FSFTVersionUpgradeRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FSFTVersionUpgradeRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class FSFTVersionUpgradeRegionProcedure
  extends StateMachineProcedure<MasterProcedureEnv, FSFTVersionUpgradeRegionState> {

  private static final Logger LOG = LoggerFactory.getLogger(FSFTVersionUpgradeRegionProcedure.class);

  private RegionInfo regionInfo;

  public FSFTVersionUpgradeRegionProcedure() {
  }

  public FSFTVersionUpgradeRegionProcedure(RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, FSFTVersionUpgradeRegionState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case FSFT_VERSION_UPGRADE_REGION_PREPARE:
        if (regionInfo == null) {
          throw new IllegalStateException("regionInfo is null");
        }
        setNextState(FSFTVersionUpgradeRegionState.FSFT_VERSION_UPGRADE_REGION_UPGRADE);
        return Flow.HAS_MORE_STATE;
      case FSFT_VERSION_UPGRADE_REGION_UPGRADE:
        // Intentionally left as a no-op placeholder. The actual upgrade implementation will be
        // added later.
        LOG.info("FSFTVersionUpgradeRegionProcedure placeholder for region={}, nothing to do yet",
          regionInfo);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("Unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, FSFTVersionUpgradeRegionState state)
    throws IOException, InterruptedException {
  }

  @Override
  protected FSFTVersionUpgradeRegionState getState(int stateId) {
    return FSFTVersionUpgradeRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(FSFTVersionUpgradeRegionState state) {
    return state.getNumber();
  }

  @Override
  protected FSFTVersionUpgradeRegionState getInitialState() {
    return FSFTVersionUpgradeRegionState.FSFT_VERSION_UPGRADE_REGION_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(FSFTVersionUpgradeRegionStateData.newBuilder()
      .setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    FSFTVersionUpgradeRegionStateData data =
      serializer.deserialize(FSFTVersionUpgradeRegionStateData.class);
    this.regionInfo = ProtobufUtil.toRegionInfo(data.getRegionInfo());
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }
}

