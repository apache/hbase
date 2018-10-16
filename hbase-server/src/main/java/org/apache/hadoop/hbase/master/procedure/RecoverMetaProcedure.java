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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverMetaState;

/**
 * Leave here only for checking if we can successfully start the master.
 * @deprecated Do not use any more, leave it here only for compatible. The recovery work will be
 *             done in {@link ServerCrashProcedure} directly, and the initial work for meta table
 *             will be done by {@link InitMetaProcedure}.
 * @see ServerCrashProcedure
 * @see InitMetaProcedure
 */
@Deprecated
@InterfaceAudience.Private
public class RecoverMetaProcedure
    extends StateMachineProcedure<MasterProcedureEnv, MasterProcedureProtos.RecoverMetaState>
    implements MetaProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(RecoverMetaProcedure.class);

  private ServerName failedMetaServer;
  private boolean shouldSplitWal;
  private int replicaId;

  private MasterServices master;

  public RecoverMetaProcedure() {

  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
      MasterProcedureProtos.RecoverMetaState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
      MasterProcedureProtos.RecoverMetaState recoverMetaState)
      throws IOException, InterruptedException {
    // Can't rollback
    throw new UnsupportedOperationException("unhandled state=" + recoverMetaState);
  }

  @Override
  protected MasterProcedureProtos.RecoverMetaState getState(int stateId) {
    return RecoverMetaState.forNumber(stateId);
  }

  @Override
  protected int getStateId(MasterProcedureProtos.RecoverMetaState recoverMetaState) {
    return recoverMetaState.getNumber();
  }

  @Override
  protected MasterProcedureProtos.RecoverMetaState getInitialState() {
    return RecoverMetaState.RECOVER_META_PREPARE;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" failedMetaServer=");
    sb.append(failedMetaServer);
    sb.append(", splitWal=");
    sb.append(shouldSplitWal);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MasterProcedureProtos.RecoverMetaStateData.Builder state =
      MasterProcedureProtos.RecoverMetaStateData.newBuilder().setShouldSplitWal(shouldSplitWal);
    if (failedMetaServer != null) {
      state.setFailedMetaServer(ProtobufUtil.toServerName(failedMetaServer));
    }
    state.setReplicaId(replicaId);
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    MasterProcedureProtos.RecoverMetaStateData state =
      serializer.deserialize(MasterProcedureProtos.RecoverMetaStateData.class);
    this.shouldSplitWal = state.hasShouldSplitWal() && state.getShouldSplitWal();
    this.failedMetaServer =
      state.hasFailedMetaServer() ? ProtobufUtil.toServerName(state.getFailedMetaServer()) : null;
    this.replicaId = state.hasReplicaId() ? state.getReplicaId() : RegionInfo.DEFAULT_REPLICA_ID;
  }
}
