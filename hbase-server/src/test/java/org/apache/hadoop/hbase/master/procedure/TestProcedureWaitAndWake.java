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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.master.replication.AbstractPeerNoLockProcedure;
import org.apache.hadoop.hbase.master.replication.AbstractPeerProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-29380
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestProcedureWaitAndWake {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureWaitAndWake.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  public static final class MyPeerProcedure extends AbstractPeerProcedure<Integer> {

    private final CyclicBarrier barrier;

    private boolean passedBarrier;

    public MyPeerProcedure() {
      this(null);
    }

    public MyPeerProcedure(CyclicBarrier barrier) {
      super("1");
      this.barrier = barrier;
    }

    @Override
    public PeerOperationType getPeerOperationType() {
      return PeerOperationType.REMOVE;
    }

    @Override
    protected LockState acquireLock(MasterProcedureEnv env) {
      // make sure we have two procedure arrive here at the same time, so one of them will enter the
      // lock wait state
      if (!passedBarrier) {
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new RuntimeException(e);
        }
        passedBarrier = true;
      }
      return super.acquireLock(env);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, Integer state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      if (state.intValue() == 0) {
        setNextState(1);
        addChildProcedure(new MySubPeerProcedure());
        return Flow.HAS_MORE_STATE;
      } else {
        Thread.sleep(200);
        return Flow.NO_MORE_STATE;
      }
    }

    @Override
    protected Integer getState(int stateId) {
      return Integer.valueOf(stateId);
    }

    @Override
    protected int getStateId(Integer state) {
      return state.intValue();
    }

    @Override
    protected Integer getInitialState() {
      return 0;
    }

    public static final class MySubPeerProcedure extends AbstractPeerNoLockProcedure<Integer> {

      public MySubPeerProcedure() {
        super("1");
      }

      @Override
      public PeerOperationType getPeerOperationType() {
        return PeerOperationType.REFRESH;
      }

      @Override
      protected Flow executeFromState(MasterProcedureEnv env, Integer state)
        throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
        return Flow.NO_MORE_STATE;
      }

      @Override
      protected Integer getState(int stateId) {
        return Integer.valueOf(stateId);
      }

      @Override
      protected int getStateId(Integer state) {
        return state.intValue();
      }

      @Override
      protected Integer getInitialState() {
        return 0;
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 8);
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPeerProcedure() {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    CyclicBarrier barrier = new CyclicBarrier(2);
    MyPeerProcedure p1 = new MyPeerProcedure(barrier);
    MyPeerProcedure p2 = new MyPeerProcedure(barrier);
    long id1 = procExec.submitProcedure(p1);
    long id2 = procExec.submitProcedure(p2);
    UTIL.waitFor(10000, () -> procExec.isFinished(id1));
    UTIL.waitFor(10000, () -> procExec.isFinished(id2));
  }
}
