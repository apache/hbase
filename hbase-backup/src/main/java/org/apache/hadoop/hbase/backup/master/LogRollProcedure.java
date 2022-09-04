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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_ENABLE_DEFAULT;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_ENABLE_KEY;
import static org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager.LOGROLL_PROCEDURE_ENABLED;
import static org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager.LOGROLL_PROCEDURE_ENABLED_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.LogRollData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.LogRollState;

@InterfaceAudience.Private
public class LogRollProcedure extends StateMachineProcedure<MasterProcedureEnv, LogRollState>
  implements TableProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RSLogRollProcedure.class);

  private Configuration conf;

  private String backupRoot;

  public LogRollProcedure() {
  }

  public LogRollProcedure(final String backupRoot, final Configuration conf) {
    this.backupRoot = backupRoot;
    this.conf = conf;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, LogRollState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    try {
      switch (state) {
        case LOG_ROLL_ROLL_LOG_ON_EACH_RS:
          final List<ServerName> onlineServers =
            env.getMasterServices().getServerManager().getOnlineServersList();
          final List<RSLogRollProcedure> subProcedures = new ArrayList<>(onlineServers.size());
          final BackupSystemTable table =
            new BackupSystemTable(env.getMasterServices().getConnection());
          final HashMap<String, Long> lastLogRollResults =
            table.readRegionServerLastLogRollResult(backupRoot);
          final long now = EnvironmentEdgeManager.currentTime();
          for (ServerName server : onlineServers) {
            long lastLogRollResult =
              lastLogRollResults.getOrDefault(server.getAddress().toString(), Long.MIN_VALUE);
            if (lastLogRollResult > now) {
              LOG.warn("Bad lastLogRollResult={}. reset lastLogRollResult.", lastLogRollResult);
              lastLogRollResult = Long.MIN_VALUE;
            }
            subProcedures.add(new RSLogRollProcedure(server, backupRoot, lastLogRollResult));
          }
          addChildProcedure(subProcedures.toArray(new RSLogRollProcedure[0]));
          setNextState(LogRollState.LOG_ROLL_ROLL_LOG_FINISH);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_ROLL_LOG_FINISH:
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      setFailure("log-roll", e);
    }
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, LogRollState state) {
    throw new UnsupportedOperationException();
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
    return LogRollState.LOG_ROLL_ROLL_LOG_ON_EACH_RS;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(LogRollData.newBuilder().setBackupRoot(backupRoot).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    this.backupRoot = serializer.deserialize(LogRollData.class).getBackupRoot();
  }

  @Override
  public TableName getTableName() {
    return BackupSystemTable.getTableName(conf);
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    Configuration conf = env.getMasterConfiguration();
    if (!conf.getBoolean(BACKUP_ENABLE_KEY, BACKUP_ENABLE_DEFAULT)) {
      setFailure("log-roll", new IOException("backup is DISABLED"));
      LOG.info("backup is DISABLED. Mark unfinished procedure {} as FAILED", this);
      return;
    }
    if (!conf.getBoolean(LOGROLL_PROCEDURE_ENABLED, LOGROLL_PROCEDURE_ENABLED_DEFAULT)) {
      setFailure("log-roll", new IOException("LogRollProcedure is DISABLED"));
      LOG.info("LogRollProcedure is DISABLED. Mark unfinished procedure {} as FAILED", this);
      return;
    }
    this.conf = conf;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.READ;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName()).append(", backupRoot=").append(backupRoot);
  }
}
