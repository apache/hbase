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


import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.MockMasterServices;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestRecoverMetaProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestRecoverMetaProcedure.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRecoverMetaProcedure.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  /**
   * Test the new prepare step.
   * Here we test that our Mock is faking out the precedure well-enough for it to progress past the
   * first prepare stage.
   */
  @Test
  public void testPrepare() throws ProcedureSuspendedException, ProcedureYieldException,
      InterruptedException, IOException {
    RecoverMetaProcedure rmp = new RecoverMetaProcedure();
    MasterProcedureEnv env = Mockito.mock(MasterProcedureEnv.class);
    MasterServices masterServices =
        new MockMasterServices(UTIL.getConfiguration(), null);
    Mockito.when(env.getMasterServices()).thenReturn(masterServices);
    assertEquals(StateMachineProcedure.Flow.HAS_MORE_STATE,
        rmp.executeFromState(env, rmp.getInitialState()));
    int stateId = rmp.getCurrentStateId();
    assertEquals(MasterProcedureProtos.RecoverMetaState.RECOVER_META_SPLIT_LOGS_VALUE,
        rmp.getCurrentStateId());
  }

  /**
   * Test the new prepare step.
   * If Master is stopping, procedure should skip the assign by returning NO_MORE_STATE
   */
  @Test
  public void testPrepareWithMasterStopping() throws ProcedureSuspendedException,
      ProcedureYieldException, InterruptedException, IOException {
    RecoverMetaProcedure rmp = new RecoverMetaProcedure();
    MasterProcedureEnv env = Mockito.mock(MasterProcedureEnv.class);
    MasterServices masterServices = new MockMasterServices(UTIL.getConfiguration(), null) {
      @Override
      public boolean isStopping() {
        return true;
      }
    };
    Mockito.when(env.getMasterServices()).thenReturn(masterServices);
    assertEquals(StateMachineProcedure.Flow.NO_MORE_STATE,
        rmp.executeFromState(env, rmp.getInitialState()));
  }

  /**
   * Test the new prepare step.
   * If cluster is down, procedure should skip the assign by returning NO_MORE_STATE
   */
  @Test
  public void testPrepareWithNoCluster() throws ProcedureSuspendedException,
      ProcedureYieldException, InterruptedException, IOException {
    RecoverMetaProcedure rmp = new RecoverMetaProcedure();
    MasterProcedureEnv env = Mockito.mock(MasterProcedureEnv.class);
    MasterServices masterServices = new MockMasterServices(UTIL.getConfiguration(), null) {
      @Override
      public boolean isClusterUp() {
        return false;
      }
    };
    Mockito.when(env.getMasterServices()).thenReturn(masterServices);
    assertEquals(StateMachineProcedure.Flow.NO_MORE_STATE,
        rmp.executeFromState(env, rmp.getInitialState()));
  }
}
