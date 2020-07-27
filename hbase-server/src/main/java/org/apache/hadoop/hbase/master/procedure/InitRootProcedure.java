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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.InitRootState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.InitRootStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * This procedure is used to initialize meta table for a new hbase deploy. It will just schedule an
 * {@link TransitRegionStateProcedure} to assign meta.
 */
@InterfaceAudience.Private
public class InitRootProcedure extends AbstractStateMachineTableProcedure<InitRootState> {

  private static final Logger LOG = LoggerFactory.getLogger(InitRootProcedure.class);

  private CountDownLatch latch = new CountDownLatch(1);

  private RetryCounter retryCounter;

  @Override
  public TableName getTableName() {
    return TableName.ROOT_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, InitRootState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.debug("Execute {}", this);
    switch (state) {
      case INIT_ROOT_ASSIGN_ROOT:
        LOG.info("Going to assign root");
        addChildProcedure(env.getAssignmentManager()
          .createAssignProcedures(Arrays.asList(RegionInfoBuilder.ROOT_REGIONINFO)));
        setNextState(MasterProcedureProtos.InitRootState.INIT_ROOT_LOAD_ROOT);
        return Flow.HAS_MORE_STATE;
      case INIT_ROOT_LOAD_ROOT:
        try {
          addMetaRegionToRoot(env);
          env.getAssignmentManager().loadRoot();
        } catch (IOException e) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed to init default and system namespaces, suspend {}secs", backoff, e);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
      case INIT_ROOT_INIT_META:
        addChildProcedure(new InitMetaProcedure());
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    // we do not need to wait for master initialized, we are part of the initialization.
    return false;
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitTableExclusiveLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, InitRootState state)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected InitRootState getState(int stateId) {
    return InitRootState.forNumber(stateId);
  }

  @Override
  protected int getStateId(InitRootState state) {
    return state.getNumber();
  }

  @Override
  protected InitRootState getInitialState() {
    return InitRootState.INIT_ROOT_ASSIGN_ROOT;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(InitRootStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(InitRootStateData.class);
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    latch.countDown();
  }

  public void await() throws InterruptedException {
    latch.await();
  }


  public void addMetaRegionToRoot(MasterProcedureEnv env) throws IOException {
    Table rooTable = env.getMasterServices().getConnection().getTable(TableName.ROOT_TABLE_NAME);
    // The row key is the region name
    byte[] row = HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
    Put put = new Put(row);
    final long now = EnvironmentEdgeManager.currentTime();
    put.add(new KeyValue(row, HConstants.CATALOG_FAMILY,
      HConstants.REGIONINFO_QUALIFIER,
      now,
      RegionInfo.toByteArray(HRegionInfo.FIRST_META_REGIONINFO)));
    // Set into the root table the version of the meta table.
    put.add(new KeyValue(row,
      HConstants.CATALOG_FAMILY,
      HConstants.META_VERSION_QUALIFIER,
      now,
      Bytes.toBytes(HConstants.META_VERSION)));
    put.add(new KeyValue(row,
      HConstants.CATALOG_FAMILY,
      HConstants.STATE_QUALIFIER,
      now,
      Bytes.toBytes(RegionState.State.OFFLINE.name())));
    rooTable.put(put);
  }
}
