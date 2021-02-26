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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;

public class DummyRegionProcedure
    extends AbstractStateMachineRegionProcedure<DummyRegionProcedureState> {

  private final CountDownLatch arrive = new CountDownLatch(1);

  private final CountDownLatch resume = new CountDownLatch(1);

  public DummyRegionProcedure() {
  }

  public DummyRegionProcedure(MasterProcedureEnv env, RegionInfo hri) {
    super(env, hri);
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, DummyRegionProcedureState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    arrive.countDown();
    resume.await();
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, DummyRegionProcedureState state)
      throws IOException, InterruptedException {
  }

  @Override
  protected DummyRegionProcedureState getState(int stateId) {
    return DummyRegionProcedureState.STATE;
  }

  @Override
  protected int getStateId(DummyRegionProcedureState state) {
    return 0;
  }

  @Override
  protected DummyRegionProcedureState getInitialState() {
    return DummyRegionProcedureState.STATE;
  }

  public void waitUntilArrive() throws InterruptedException {
    arrive.await();
  }

  public void resume() {
    resume.countDown();
  }
}
