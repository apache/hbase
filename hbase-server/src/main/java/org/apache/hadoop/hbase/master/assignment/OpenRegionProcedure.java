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
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
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

  @Override
  protected void reportTransition(RegionStateNode regionNode, TransitionCode transitionCode,
      long seqId) throws IOException {
    switch (transitionCode) {
      case OPENED:
        // this is the openSeqNum
        if (seqId < 0) {
          throw new UnexpectedStateException("Received report unexpected " + TransitionCode.OPENED +
            " transition openSeqNum=" + seqId + ", " + regionNode + ", proc=" + this);
        }
        break;
      case FAILED_OPEN:
        // nothing to check
        break;
      default:
        throw new UnexpectedStateException(
          "Received report unexpected " + transitionCode + " transition, " +
            regionNode.toShortString() + ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  @Override
  protected void updateTransition(MasterProcedureEnv env, RegionStateNode regionNode,
      TransitionCode transitionCode, long openSeqNum) throws IOException {
    switch (transitionCode) {
      case OPENED:
        if (openSeqNum < regionNode.getOpenSeqNum()) {
          LOG.warn(
            "Received report {} transition from {} for {}, pid={} but the new openSeqNum {}" +
              " is less than the current one {}, ignoring...",
            transitionCode, targetServer, regionNode, getProcId(), openSeqNum,
            regionNode.getOpenSeqNum());
        } else {
          regionNode.setOpenSeqNum(openSeqNum);
        }
        env.getAssignmentManager().regionOpened(regionNode);
        break;
      case FAILED_OPEN:
        env.getAssignmentManager().regionFailedOpen(regionNode, false);
        break;
      default:
        throw new UnexpectedStateException("Unexpected transition code: " + transitionCode);
    }

  }
}
