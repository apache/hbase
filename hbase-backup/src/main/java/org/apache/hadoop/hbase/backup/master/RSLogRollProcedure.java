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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.RSLogRollData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.RSLogRollState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * The procedure to perform WAL rolling on the specific RegionServer. It will keep retrying until
 * succeed.
 */
@InterfaceAudience.Private
public class RSLogRollProcedure extends StateMachineProcedure<MasterProcedureEnv, RSLogRollState>
  implements ServerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RSLogRollProcedure.class);

  private ServerName targetServer;

  private String backupRoot;

  private long lastLogRollResult;

  private RetryCounter retryCounter;

  public RSLogRollProcedure() {
  }

  public RSLogRollProcedure(ServerName targetServer, String backupRoot, long lastLogRollResult) {
    this.targetServer = targetServer;
    this.backupRoot = backupRoot;
    this.lastLogRollResult = lastLogRollResult;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RSLogRollState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    switch (state) {
      case RS_LOG_ROLL_ROLL_LOG_REMOTE:
        addChildProcedure(new RSLogRollRemoteProcedure(targetServer, backupRoot));
        setNextState(RSLogRollState.RS_LOG_ROLL_CONFIRM_RESULT);
        return Flow.HAS_MORE_STATE;
      case RS_LOG_ROLL_CONFIRM_RESULT:
        // if server is crashed. skip the rest steps.
        if (!env.getMasterServices().getServerManager().isServerOnline(targetServer)) {
          return Flow.NO_MORE_STATE;
        }
        try {
          BackupSystemTable table = new BackupSystemTable(env.getMasterServices().getConnection());
          long newestLogRollResult = table
            .getRegionServerLastLogRollResult(targetServer.getAddress().toString(), backupRoot);
          LOG.debug("newestLogRollResult={}, lastLogRollResult={}", newestLogRollResult,
            lastLogRollResult);
          if (newestLogRollResult > lastLogRollResult) {
            return Flow.NO_MORE_STATE;
          } else {
            setNextState(RSLogRollState.RS_LOG_ROLL_ROLL_LOG_REMOTE);
            return Flow.HAS_MORE_STATE;
          }
        } catch (IOException e) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed get newest log roll result, retry later", e);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RSLogRollState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected RSLogRollState getState(int stateId) {
    return RSLogRollState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RSLogRollState state) {
    return state.getNumber();
  }

  @Override
  protected RSLogRollState getInitialState() {
    return RSLogRollState.RS_LOG_ROLL_ROLL_LOG_REMOTE;
  }

  @Override
  public ServerName getServerName() {
    return targetServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.LOG_ROLL;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RSLogRollData.Builder builder = RSLogRollData.newBuilder();
    builder.setTargetServer(ProtobufUtil.toServerName(targetServer)).setBackupRoot(backupRoot)
      .setLastLogRollResult(lastLogRollResult);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RSLogRollData data = serializer.deserialize(RSLogRollData.class);
    this.targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    this.backupRoot = data.getBackupRoot();
    this.lastLogRollResult = data.getLastLogRollResult();
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName()).append(" targetServer=").append(targetServer)
      .append(", backupRoot=").append(backupRoot).append(", lastLogRollResult=")
      .append(lastLogRollResult);
  }
}
