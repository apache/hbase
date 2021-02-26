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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionOpenOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.OpenRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The remote procedure used to open a region.
 */
@InterfaceAudience.Private
public class OpenRegionProcedure extends RegionRemoteProcedureBase {

  private static final Logger LOG = LoggerFactory.getLogger(OpenRegionProcedure.class);

  public OpenRegionProcedure() {
    super();
  }

  public OpenRegionProcedure(TransitRegionStateProcedure parent, RegionInfo region,
      ServerName targetServer) {
    super(parent, region, targetServer);
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_ASSIGN;
  }

  @Override
  public RemoteOperation newRemoteOperation() {
    return new RegionOpenOperation(this, region, getProcId());
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(OpenRegionProcedureStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(OpenRegionProcedureStateData.class);
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getOpenProcMetrics();
  }

  private void regionOpenedWithoutPersistingToMeta(AssignmentManager am, RegionStateNode regionNode,
      TransitionCode transitionCode, long openSeqNum) throws IOException {
    if (openSeqNum < regionNode.getOpenSeqNum()) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={} but the new openSeqNum {}" +
          " is less than the current one {}, ignoring...",
        transitionCode, targetServer, regionNode, getProcId(), openSeqNum,
        regionNode.getOpenSeqNum());
    } else {
      regionNode.setOpenSeqNum(openSeqNum);
    }
    am.regionOpenedWithoutPersistingToMeta(regionNode);
  }

  @Override
  protected void checkTransition(RegionStateNode regionNode, TransitionCode transitionCode,
      long openSeqNum) throws UnexpectedStateException {
    switch (transitionCode) {
      case OPENED:
        if (openSeqNum < 0) {
          throw new UnexpectedStateException("Received report unexpected " + TransitionCode.OPENED +
            " transition openSeqNum=" + openSeqNum + ", " + regionNode + ", proc=" + this);
        }
        break;
      case FAILED_OPEN:
        break;
      default:
        throw new UnexpectedStateException(
          "Received report unexpected " + transitionCode + " transition, " +
            regionNode.toShortString() + ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  @Override
  protected void updateTransitionWithoutPersistingToMeta(MasterProcedureEnv env,
      RegionStateNode regionNode, TransitionCode transitionCode, long openSeqNum)
      throws IOException {
    if (transitionCode == TransitionCode.OPENED) {
      regionOpenedWithoutPersistingToMeta(env.getAssignmentManager(), regionNode, transitionCode,
        openSeqNum);
    } else {
      assert transitionCode == TransitionCode.FAILED_OPEN;
      // will not persist to meta if giveUp is false
      env.getAssignmentManager().regionFailedOpen(regionNode, false);
    }
  }

  @Override
  protected void restoreSucceedState(AssignmentManager am, RegionStateNode regionNode,
      long openSeqNum) throws IOException {
    if (regionNode.getState() == State.OPEN) {
      // should have already been persisted, ignore
      return;
    }
    regionOpenedWithoutPersistingToMeta(am, regionNode, TransitionCode.OPENED, openSeqNum);
  }
}
