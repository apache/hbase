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
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionCloseOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseRegionProcedureStateData;

/**
 * The remote procedure used to close a region.
 */
@InterfaceAudience.Private
public class CloseRegionProcedure extends RegionRemoteProcedureBase {

  // For a region move operation, we will assign the region after we unassign it, this is the target
  // server for the subsequent assign. We will send this value to RS, and RS will record the region
  // in a Map to tell client that where the region has been moved to. Can be null. And also, can be
  // wrong(but do not make it wrong intentionally). The client can handle this error.
  private ServerName assignCandidate;

  public CloseRegionProcedure() {
    super();
  }

  public CloseRegionProcedure(RegionInfo region, ServerName targetServer,
      ServerName assignCandidate) {
    super(region, targetServer);
    this.assignCandidate = assignCandidate;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_UNASSIGN;
  }

  @Override
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    return new RegionCloseOperation(this, region, assignCandidate);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    CloseRegionProcedureStateData.Builder builder = CloseRegionProcedureStateData.newBuilder();
    if (assignCandidate != null) {
      builder.setAssignCandidate(ProtobufUtil.toServerName(assignCandidate));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseRegionProcedureStateData data =
      serializer.deserialize(CloseRegionProcedureStateData.class);
    if (data.hasAssignCandidate()) {
      assignCandidate = ProtobufUtil.toServerName(data.getAssignCandidate());
    }
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getCloseProcMetrics();
  }

  @Override
  protected boolean shouldDispatch(RegionStateNode regionNode) {
    return regionNode.isInState(RegionState.State.CLOSING);
  }
}
