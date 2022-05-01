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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MasterFailoverWithProceduresTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(MasterFailoverWithProceduresTestBase.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numMasters(2).build();
    UTIL.startMiniCluster(option);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExec, false);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================
  protected static ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  protected static Path getRootDir() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
  }

  protected static void testRecoveryAndDoubleExecution(final HBaseTestingUtility testUtil,
    final long procId, final int lastStepBeforeFailover) throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      testUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    final Procedure<?> proc = procExec.getProcedure(procId);
    for (int i = 0; i < lastStepBeforeFailover; ++i) {
      LOG.info("Restart " + i + " exec state: " + proc);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }
    ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);

    LOG.info("Trigger master failover");
    MasterProcedureTestingUtility.masterFailover(testUtil);

    procExec = testUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }
}
