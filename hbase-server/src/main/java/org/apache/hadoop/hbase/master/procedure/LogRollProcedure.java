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
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.LogRollState;

/**
 * The procedure to perform WAL rolling on all of RegionServers.
 */
@InterfaceAudience.Private
public class LogRollProcedure extends StateMachineProcedure<MasterProcedureEnv, LogRollState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(LogRollProcedure.class);

  public LogRollProcedure() {
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, LogRollState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    try {
      switch (state) {
        case LOG_ROLL_ROLL_LOG_ON_RS:
          final ServerManager serverManager = env.getMasterServices().getServerManager();
          // avoid potential new region server missing
          serverManager.registerListener(new NewServerWALRoller(env));

          final List<LogRollRemoteProcedure> subProcedures =
            serverManager.getOnlineServersList().stream().map(LogRollRemoteProcedure::new).toList();
          addChildProcedure(subProcedures.toArray(new LogRollRemoteProcedure[0]));
          setNextState(LogRollState.LOG_ROLL_UNREGISTER_SERVER_LISTENER);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_UNREGISTER_SERVER_LISTENER:
          env.getMasterServices().getServerManager()
            .unregisterListenerIf(l -> l instanceof NewServerWALRoller);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      setFailure("log-roll", e);
    }
    return Flow.NO_MORE_STATE;
  }

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  private record NewServerWALRoller(MasterProcedureEnv env) implements ServerListener {

    @Override
    public void serverAdded(ServerName server) {
      env.getMasterServices().getMasterProcedureExecutor()
        .submitProcedure(new LogRollRemoteProcedure(server));
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, LogRollState state) {
    // nothing to rollback
  }

  @Override
  protected LogRollState getState(int stateId) {
    return LogRollState.forNumber(stateId);
  }

  @Override
  protected int getStateId(LogRollState state) {
    return state.getNumber();
  }

  @Override
  protected LogRollState getInitialState() {
    return LogRollState.LOG_ROLL_ROLL_LOG_ON_RS;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // nothing to persist
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // nothing to persist
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
  }
}
