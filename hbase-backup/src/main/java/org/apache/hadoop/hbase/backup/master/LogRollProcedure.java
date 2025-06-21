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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.LogRollData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.LogRollState;

/**
 * The procedure to perform WAL rolling on all of RegionServers.
 */
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
        case LOG_ROLL_PRE_CHECK_NAMESPACE:
          // create backup namespace if not exists
          if (
            !env.getMasterServices().getClusterSchema().getTableNamespaceManager()
              .doesNamespaceExist(getTableName().getNamespaceAsString())
          ) {
            NamespaceDescriptor desc =
              NamespaceDescriptor.create(getTableName().getNamespaceAsString()).build();
            addChildProcedure(new CreateNamespaceProcedure(env, desc));
          }
          setNextState(LogRollState.LOG_ROLL_PRE_CHECK_TABLES);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_PRE_CHECK_TABLES:
          // create backup system table and backup system bulkload table if not exists
          List<CreateTableProcedure> toCreate = new ArrayList<>();
          TableStateManager tsm = env.getMasterServices().getTableStateManager();
          createIfNeeded(env, tsm, BackupSystemTable.getTableName(conf),
            BackupSystemTable.getSystemTableDescriptor(conf)).ifPresent(toCreate::add);
          createIfNeeded(env, tsm, BackupSystemTable.getTableNameForBulkLoadedData(conf),
            BackupSystemTable.getSystemTableForBulkLoadedDataDescriptor(conf))
              .ifPresent(toCreate::add);
          if (!toCreate.isEmpty()) {
            addChildProcedure(toCreate.toArray(new CreateTableProcedure[0]));
          }
          setNextState(LogRollState.LOG_ROLL_ROLL_LOG_ON_EACH_RS);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_ROLL_LOG_ON_EACH_RS:
          final ServerManager serverManager = env.getMasterServices().getServerManager();
          // avoid potential new region server missing
          serverManager.registerListener(new NewServerWALRoller(env, backupRoot));
          final List<ServerName> onlineServers = serverManager.getOnlineServersList();
          final List<RSLogRollProcedure> subProcedures = new ArrayList<>(onlineServers.size());
          final BackupSystemTable table =
            new BackupSystemTable(env.getMasterServices().getConnection());
          final HashMap<String, Long> lastLogRollResults =
            table.readRegionServerLastLogRollResult(backupRoot);
          final long now = EnvironmentEdgeManager.currentTime();
          for (ServerName server : onlineServers) {
            long lastLogRollTime =
              lastLogRollResults.getOrDefault(server.getAddress().toString(), Long.MIN_VALUE);
            if (lastLogRollTime > now) {
              LOG.warn("Bad lastLogRollTime={}. reset lastLogRollResult.", lastLogRollTime);
              lastLogRollTime = Long.MIN_VALUE;
            }
            subProcedures.add(new RSLogRollProcedure(server, backupRoot, lastLogRollTime));
          }
          addChildProcedure(subProcedures.toArray(new RSLogRollProcedure[0]));
          setNextState(LogRollState.LOG_ROLL_AFTER_RS_ROLL);
          return Flow.HAS_MORE_STATE;
        case LOG_ROLL_AFTER_RS_ROLL:
          env.getMasterServices().getServerManager()
            .unregisterListenerIf(l -> l instanceof NewServerWALRoller);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      setFailure("log-roll", e);
    }
    return Flow.NO_MORE_STATE;
  }

  private static final class NewServerWALRoller implements ServerListener {

    private final MasterProcedureEnv env;

    private final String backupRoot;

    public NewServerWALRoller(MasterProcedureEnv env, String backupRoot) {
      this.env = env;
      this.backupRoot = backupRoot;
    }

    @Override
    public void serverAdded(ServerName server) {
      env.getMasterServices().getMasterProcedureExecutor()
        .submitProcedure(new RSLogRollProcedure(server, backupRoot, Long.MIN_VALUE));
    }
  }

  private Optional<CreateTableProcedure> createIfNeeded(MasterProcedureEnv env,
    TableStateManager tsm, TableName tableName, TableDescriptor desc) throws IOException {
    if (tsm.isTablePresent(tableName)) {
      TableState state = tsm.getTableState(tableName);
      if (state.isEnabled()) {
        return Optional.empty();
      } else {
        throw new TableNotEnabledException("table=" + tableName + ", state=" + state);
      }
    } else {
      RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(desc, new byte[][] {});
      return Optional.of(new CreateTableProcedure(env, desc, regions));
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
    return LogRollState.LOG_ROLL_PRE_CHECK_NAMESPACE;
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
    env.getMasterServices().getServerManager()
      .registerListener(new NewServerWALRoller(env, backupRoot));
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
