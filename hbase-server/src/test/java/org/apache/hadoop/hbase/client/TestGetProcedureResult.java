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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;

/**
 * Testcase for HBASE-19608.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestGetProcedureResult {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGetProcedureResult.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  public static final class DummyProcedure extends Procedure<MasterProcedureEnv>
      implements TableProcedureInterface {

    private final CountDownLatch failureSet = new CountDownLatch(1);

    private final CountDownLatch canRollback = new CountDownLatch(1);

    @Override
    public TableName getTableName() {
      return TableName.valueOf("dummy");
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.READ;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      setFailure("dummy", new IOException("inject error"));
      failureSet.countDown();
      return null;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      canRollback.await();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private GetProcedureResultResponse.State getState(long procId)
      throws MasterNotRunningException, IOException, ServiceException {
    MasterProtos.MasterService.BlockingInterface master =
      ((ConnectionImplementation) UTIL.getConnection()).getMaster();
    GetProcedureResultResponse resp = master.getProcedureResult(null,
      GetProcedureResultRequest.newBuilder().setProcId(procId).build());
    return resp.getState();
  }

  @Test
  public void testRace() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> executor =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    DummyProcedure p = new DummyProcedure();
    long procId = executor.submitProcedure(p);
    p.failureSet.await();
    assertEquals(GetProcedureResultResponse.State.RUNNING, getState(procId));
    p.canRollback.countDown();
    UTIL.waitFor(30000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return getState(procId) == GetProcedureResultResponse.State.FINISHED;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Procedure pid=" + procId + " is still in " + getState(procId) +
          " state, expected " + GetProcedureResultResponse.State.FINISHED;
      }
    });
  }
}
