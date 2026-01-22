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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateNamespaceTableProcedureState;

/**
 * Migrate the namespace data to meta table's namespace family while upgrading
 */
@InterfaceAudience.Private
public class MigrateNamespaceTableProcedure
  extends StateMachineProcedure<MasterProcedureEnv, MigrateNamespaceTableProcedureState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateNamespaceTableProcedure.class);

  private RetryCounter retryCounter;

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  private void migrate(MasterProcedureEnv env) throws IOException {
    Connection conn = env.getMasterServices().getConnection();
    try (Table nsTable = conn.getTable(TableName.NAMESPACE_TABLE_NAME);
      ResultScanner scanner = nsTable.getScanner(
        new Scan().addFamily(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES).readAllVersions());
      BufferedMutator mutator =
        conn.getBufferedMutator(env.getMasterServices().getConnection().getMetaTableName())) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        Put put = new Put(result.getRow());
        result
          .getColumnCells(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES,
            TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES)
          .forEach(c -> put.addColumn(HConstants.NAMESPACE_FAMILY,
            HConstants.NAMESPACE_COL_DESC_QUALIFIER, c.getTimestamp(), CellUtil.cloneValue(c)));
        mutator.mutate(put);
      }
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, MigrateNamespaceTableProcedureState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case MIGRATE_NAMESPACE_TABLE_ADD_FAMILY:
          TableDescriptor metaTableDesc = env.getMasterServices().getTableDescriptors()
            .get(env.getMasterServices().getConnection().getMetaTableName());
          if (!metaTableDesc.hasColumnFamily(HConstants.NAMESPACE_FAMILY)) {
            TableDescriptor newMetaTableDesc = TableDescriptorBuilder.newBuilder(metaTableDesc)
              .setColumnFamily(
                FSTableDescriptors.getNamespaceFamilyDescForMeta(env.getMasterConfiguration()))
              .build();
            addChildProcedure(new ModifyTableProcedure(env, newMetaTableDesc));
          }
          setNextState(MigrateNamespaceTableProcedureState.MIGRATE_NAMESPACE_TABLE_MIGRATE_DATA);
          return Flow.HAS_MORE_STATE;
        case MIGRATE_NAMESPACE_TABLE_MIGRATE_DATA:
          migrate(env);
          setNextState(MigrateNamespaceTableProcedureState.MIGRATE_NAMESPACE_TABLE_DISABLE_TABLE);
          return Flow.HAS_MORE_STATE;
        case MIGRATE_NAMESPACE_TABLE_DISABLE_TABLE:
          addChildProcedure(new DisableTableProcedure(env, TableName.NAMESPACE_TABLE_NAME, false));
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("Unhandled state=" + state);
      }
    } catch (IOException e) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed migrating namespace data, suspend {}secs {}", backoff / 1000, this, e);
      throw suspend(Math.toIntExact(backoff), true);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, MigrateNamespaceTableProcedureState state)
    throws IOException, InterruptedException {
  }

  @Override
  protected MigrateNamespaceTableProcedureState getState(int stateId) {
    return MigrateNamespaceTableProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(MigrateNamespaceTableProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected MigrateNamespaceTableProcedureState getInitialState() {
    return MigrateNamespaceTableProcedureState.MIGRATE_NAMESPACE_TABLE_ADD_FAMILY;
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    env.getMasterServices().getClusterSchema().getTableNamespaceManager().setMigrationDone();
  }
}
