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
import org.apache.hadoop.hbase.quotas.RpcThrottleStorage;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchRpcThrottleState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchRpcThrottleStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * The procedure to switch rpc throttle
 */
@InterfaceAudience.Private
public class SwitchRpcThrottleProcedure
    extends StateMachineProcedure<MasterProcedureEnv, SwitchRpcThrottleState>
    implements ServerProcedureInterface {

  private static Logger LOG = LoggerFactory.getLogger(SwitchRpcThrottleProcedure.class);

  private RpcThrottleStorage rpcThrottleStorage;
  private boolean rpcThrottleEnabled;
  private ProcedurePrepareLatch syncLatch;
  private ServerName serverName;
  private RetryCounter retryCounter;

  public SwitchRpcThrottleProcedure() {
  }

  public SwitchRpcThrottleProcedure(RpcThrottleStorage rpcThrottleStorage,
      boolean rpcThrottleEnabled, ServerName serverName, final ProcedurePrepareLatch syncLatch) {
    this.rpcThrottleStorage = rpcThrottleStorage;
    this.syncLatch = syncLatch;
    this.rpcThrottleEnabled = rpcThrottleEnabled;
    this.serverName = serverName;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SwitchRpcThrottleState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case UPDATE_SWITCH_RPC_THROTTLE_STORAGE:
        try {
          switchThrottleState(env, rpcThrottleEnabled);
        } catch (IOException e) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed to store rpc throttle value {}, sleep {} secs and retry",
            rpcThrottleEnabled, backoff / 1000, e);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
        setNextState(SwitchRpcThrottleState.SWITCH_RPC_THROTTLE_ON_RS);
        return Flow.HAS_MORE_STATE;
      case SWITCH_RPC_THROTTLE_ON_RS:
        SwitchRpcThrottleRemoteProcedure[] subProcedures =
            env.getMasterServices().getServerManager().getOnlineServersList().stream()
                .map(sn -> new SwitchRpcThrottleRemoteProcedure(sn, rpcThrottleEnabled))
                .toArray(SwitchRpcThrottleRemoteProcedure[]::new);
        addChildProcedure(subProcedures);
        setNextState(SwitchRpcThrottleState.POST_SWITCH_RPC_THROTTLE);
        return Flow.HAS_MORE_STATE;
      case POST_SWITCH_RPC_THROTTLE:
        ProcedurePrepareLatch.releaseLatch(syncLatch, this);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, SwitchRpcThrottleState state)
      throws IOException, InterruptedException {
  }

  @Override
  protected SwitchRpcThrottleState getState(int stateId) {
    return SwitchRpcThrottleState.forNumber(stateId);
  }

  @Override
  protected int getStateId(SwitchRpcThrottleState throttleState) {
    return throttleState.getNumber();
  }

  @Override
  protected SwitchRpcThrottleState getInitialState() {
    return SwitchRpcThrottleState.UPDATE_SWITCH_RPC_THROTTLE_STORAGE;
  }

  @Override
  protected SwitchRpcThrottleState getCurrentState() {
    return super.getCurrentState();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(
      SwitchRpcThrottleStateData.newBuilder().setRpcThrottleEnabled(rpcThrottleEnabled).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    SwitchRpcThrottleStateData data = serializer.deserialize(SwitchRpcThrottleStateData.class);
    rpcThrottleEnabled = data.getRpcThrottleEnabled();
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
    return ServerOperationType.SWITCH_RPC_THROTTLE;
  }

  public void switchThrottleState(MasterProcedureEnv env, boolean rpcThrottleEnabled)
      throws IOException {
    rpcThrottleStorage.switchRpcThrottle(rpcThrottleEnabled);
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=");
    sb.append(serverName);
    sb.append(", rpcThrottleEnabled=");
    sb.append(rpcThrottleEnabled);
  }
}
