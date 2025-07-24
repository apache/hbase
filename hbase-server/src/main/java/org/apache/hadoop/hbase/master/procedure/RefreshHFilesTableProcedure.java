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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshHFilesTableProcedureState;

@InterfaceAudience.Private
public class RefreshHFilesTableProcedure extends AbstractStateMachineTableProcedure<RefreshHFilesTableProcedureState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesTableProcedure.class);

  private TableName tableName;
  private String namespace;

  public RefreshHFilesTableProcedure() {
    super();
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env) {
    super(env);
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env, TableName tableName) {
    super(env);
    this.tableName = tableName;
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env, String namespace) {
    super(env);
    this.namespace = namespace;
  }
  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REFRESH_HFILES;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  protected RefreshHFilesTableProcedureState getInitialState() {
    return RefreshHFilesTableProcedureState.REFRESH_HFILES_PREPARE;
  }

  @Override
  protected int getStateId(RefreshHFilesTableProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected RefreshHFilesTableProcedureState getState(int stateId) {
    return RefreshHFilesTableProcedureState.forNumber(stateId);
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RefreshHFilesTableProcedureState RefreshHFilesTableProcedureState)
    throws IOException, InterruptedException {
    // Refresh HFiles is idempotent operation hence rollback is not needed
    LOG.trace("Rollback not implemented for RefreshHFilesTableProcedure state: {}", RefreshHFilesTableProcedureState);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshHFilesTableProcedureState RefreshHFilesTableProcedureState) {
    LOG.info("Executing RefreshHFilesTableProcedureState state: {}", RefreshHFilesTableProcedureState);

    try {
      return switch (RefreshHFilesTableProcedureState) {
        case REFRESH_HFILES_PREPARE -> prepare(env);
        case REFRESH_HFILES_REFRESH_REGION -> refreshRegionHFiles();
        case REFRESH_HFILES_FINISH -> finish();
        default -> throw new UnsupportedOperationException("Unhandled state: " + RefreshHFilesTableProcedureState);
      };
    } catch (Exception ex) {
      LOG.error("Error in RefreshHFilesTableProcedure state {}", RefreshHFilesTableProcedureState, ex);
      setFailure("RefreshHFilesTableProcedure", ex);
      return Flow.NO_MORE_STATE;
    }
  }

  private Flow prepare(final MasterProcedureEnv env){
    System.out.println("Anuj: In Prepare State");
    // TODO Check if table exists otherwise send exception.
    // Get list of regions for the table
    AssignmentManager am = env.getAssignmentManager();
    List<RegionInfo> regions = am.getRegionStates().getRegionsOfTable(tableName);

    // For each region get the server where it is hosted and then call refreshHfile on that server with given region as parameter
//    for(RegionInfo region : regions){
//      ServerName server  = regionStates.getRegionServerOfRegion(region);
//      // Refresh Region on the region server
//
//    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow refreshRegionHFiles(){
    System.out.println("Anuj: In Refresh Region State");
    return Flow.HAS_MORE_STATE;
  }

  private Flow finish(){
    System.out.println("Anuj: In Finish State");
    return Flow.NO_MORE_STATE;
  }
}

