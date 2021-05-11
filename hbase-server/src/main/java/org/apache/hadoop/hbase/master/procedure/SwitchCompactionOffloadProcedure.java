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
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.regionserver.CompactionOffloadSwitchStorage;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchCompactionOffloadState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchCompactionOffloadStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * The procedure to switch compaction offload
 */
@InterfaceAudience.Private
public class SwitchCompactionOffloadProcedure
    extends StateMachineProcedure<MasterProcedureEnv, SwitchCompactionOffloadState>
    implements ServerProcedureInterface {

  private static Logger LOG = LoggerFactory.getLogger(SwitchCompactionOffloadProcedure.class);

  private CompactionOffloadSwitchStorage compactionOffloadSwitchStorage;
  private boolean CompactionOffloadEnabled;
  private ProcedurePrepareLatch syncLatch;
  private ServerName serverName;
  private RetryCounter retryCounter;

  public SwitchCompactionOffloadProcedure() {
  }

  public SwitchCompactionOffloadProcedure(
      CompactionOffloadSwitchStorage compactionOffloadSwitchStorage,
      boolean CompactionOffloadEnabled, ServerName serverName,
      final ProcedurePrepareLatch syncLatch) {
    this.compactionOffloadSwitchStorage = compactionOffloadSwitchStorage;
    this.syncLatch = syncLatch;
    this.CompactionOffloadEnabled = CompactionOffloadEnabled;
    this.serverName = serverName;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SwitchCompactionOffloadState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case UPDATE_SWITCH_COMPACTION_OFFLOAD_STORAGE:
        try {
          switchThrottleState(env, CompactionOffloadEnabled);
        } catch (IOException e) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed to store compaction offload value {}, sleep {} secs and retry",
            CompactionOffloadEnabled, backoff / 1000, e);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
        setNextState(SwitchCompactionOffloadState.SWITCH_COMPACTION_OFFLOAD_ON_RS);
        return Flow.HAS_MORE_STATE;
      case SWITCH_COMPACTION_OFFLOAD_ON_RS:
        SwitchCompactionOffloadRemoteProcedure[] subProcedures =
            env.getMasterServices().getServerManager().getOnlineServersList().stream()
                .map(sn -> new SwitchCompactionOffloadRemoteProcedure(sn, CompactionOffloadEnabled))
                .toArray(SwitchCompactionOffloadRemoteProcedure[]::new);
        addChildProcedure(subProcedures);
        setNextState(SwitchCompactionOffloadState.POST_SWITCH_COMPACTION_OFFLOAD);
        return Flow.HAS_MORE_STATE;
      case POST_SWITCH_COMPACTION_OFFLOAD:
        ProcedurePrepareLatch.releaseLatch(syncLatch, this);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, SwitchCompactionOffloadState state)
      throws IOException, InterruptedException {
  }

  @Override
  protected SwitchCompactionOffloadState getState(int stateId) {
    return SwitchCompactionOffloadState.forNumber(stateId);
  }

  @Override
  protected int getStateId(SwitchCompactionOffloadState state) {
    return state.getNumber();
  }

  @Override
  protected SwitchCompactionOffloadState getInitialState() {
    return SwitchCompactionOffloadState.UPDATE_SWITCH_COMPACTION_OFFLOAD_STORAGE;
  }

  @Override
  protected SwitchCompactionOffloadState getCurrentState() {
    return super.getCurrentState();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(SwitchCompactionOffloadStateData.newBuilder()
        .setCompactionOffloadEnabled(CompactionOffloadEnabled).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    SwitchCompactionOffloadStateData data =
        serializer.deserialize(SwitchCompactionOffloadStateData.class);
    CompactionOffloadEnabled = data.getCompactionOffloadEnabled();
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.SWITCH_COMPACTION_OFFLOAD;
  }

  private void switchThrottleState(MasterProcedureEnv env, boolean CompactionOffloadEnabled)
      throws IOException {
    compactionOffloadSwitchStorage.switchCompactionOffload(CompactionOffloadEnabled);
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=");
    sb.append(serverName);
    sb.append(", CompactionOffloadEnabled=");
    sb.append(CompactionOffloadEnabled);
  }
}
