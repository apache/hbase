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
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LastHighestWalFilenum;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.LogRollProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.LogRollRemoteProcedureResult;

/**
 * The procedure to perform WAL rolling on all of RegionServers.
 */
@InterfaceAudience.Private
public class LogRollProcedure
  extends StateMachineProcedure<MasterProcedureEnv, LogRollProcedureState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(LogRollProcedure.class);

  public LogRollProcedure() {
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, LogRollProcedureState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    final ServerManager serverManager = env.getMasterServices().getServerManager();

    try {
      switch (state) {
        case LOG_ROLL_ROLL_LOG_ON_RS:
          // avoid potential new region server missing
          serverManager.registerListener(new NewServerWALRoller(env));

          final List<LogRollRemoteProcedure> subProcedures =
            serverManager.getOnlineServersList().stream().map(LogRollRemoteProcedure::new).toList();
          addChildProcedure(subProcedures.toArray(new LogRollRemoteProcedure[0]));
          setNextState(LogRollProcedureState.LOG_ROLL_COLLECT_RS_HIGHEST_WAL_FILENUM);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_COLLECT_RS_HIGHEST_WAL_FILENUM:
          // get children procedure
          List<LogRollRemoteProcedure> children =
            env.getMasterServices().getMasterProcedureExecutor().getProcedures().stream()
              .filter(p -> p instanceof LogRollRemoteProcedure)
              .filter(p -> p.getParentProcId() == getProcId()).map(p -> (LogRollRemoteProcedure) p)
              .toList();
          LastHighestWalFilenum.Builder builder = LastHighestWalFilenum.newBuilder();
          for (Procedure<MasterProcedureEnv> child : children) {
            LogRollRemoteProcedureResult result =
              LogRollRemoteProcedureResult.parseFrom(child.getResult());
            builder.putFileNum(ProtobufUtil.toServerName(result.getServerName()).toString(),
              result.getLastHighestWalFilenum());
          }
          setResult(builder.build().toByteArray());
          setNextState(LogRollProcedureState.LOG_ROLL_UNREGISTER_SERVER_LISTENER);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_UNREGISTER_SERVER_LISTENER:
          serverManager.unregisterListenerIf(l -> l instanceof NewServerWALRoller);
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

  private static final class NewServerWALRoller implements ServerListener {

    private final MasterProcedureEnv env;

    public NewServerWALRoller(MasterProcedureEnv env) {
      this.env = env;
    }

    @Override
    public void serverAdded(ServerName server) {
      env.getMasterServices().getMasterProcedureExecutor()
        .submitProcedure(new LogRollRemoteProcedure(server));
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, LogRollProcedureState state) {
    // nothing to rollback
  }

  @Override
  protected LogRollProcedureState getState(int stateId) {
    return LogRollProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(LogRollProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected LogRollProcedureState getInitialState() {
    return LogRollProcedureState.LOG_ROLL_ROLL_LOG_ON_RS;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    if (getResult() != null && getResult().length > 0) {
      serializer.serialize(LastHighestWalFilenum.parseFrom(getResult()));
    } else {
      serializer.serialize(LastHighestWalFilenum.getDefaultInstance());
    }
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    if (getResult() == null) {
      LastHighestWalFilenum lastHighestWalFilenum =
        serializer.deserialize(LastHighestWalFilenum.class);
      if (lastHighestWalFilenum != null) {
        if (
          lastHighestWalFilenum.getFileNumMap().isEmpty()
            && getCurrentState() == LogRollProcedureState.LOG_ROLL_UNREGISTER_SERVER_LISTENER
        ) {
          LOG.warn("pid = {}, current state is the last state, but rsHighestWalFilenumMap is "
            + "empty, this should not happen. Are all region servers down ?", getProcId());
        } else {
          setResult(lastHighestWalFilenum.toByteArray());
        }
      }
    }
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
  }
}
