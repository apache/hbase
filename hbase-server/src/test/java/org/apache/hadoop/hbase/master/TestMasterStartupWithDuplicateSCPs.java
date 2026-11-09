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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The master must initialize when its procedure store holds more than one unfinished
 * ServerCrashProcedure for the same server.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestMasterStartupWithDuplicateSCPs {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    // the restarted master gets a new port, so the default rpc connection registry can not work
    UTIL.getConfiguration().set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRestartWithDuplicateSCPs() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    ServerName deadServer =
      ServerName.valueOf("dead.example.org", 16020, EnvironmentEdgeManager.currentTime());

    // Hold the server lock so both SCPs stay queued, and therefore unfinished, in the store.
    NoopProcedure<MasterProcedureEnv> lockHolder = new NoopProcedure<>();
    assertFalse(env.getProcedureScheduler().waitServerExclusiveLock(lockHolder, deadServer));
    long pid1 = procExec.submitProcedure(new ServerCrashProcedure(env, deadServer, false, false));
    long pid2 = procExec.submitProcedure(new ServerCrashProcedure(env, deadServer, false, false));
    assertEquals(2,
      procExec
        .getActiveProceduresNoCopy().stream().filter(p -> p instanceof ServerCrashProcedure
          && !p.isFinished() && ((ServerCrashProcedure) p).getServerName().equals(deadServer))
        .count());

    UTIL.getMiniHBaseCluster().stopMaster(0).join();
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.waitFor(60000, () -> {
      HMaster m = UTIL.getMiniHBaseCluster().getMaster();
      return m != null && m.isInitialized();
    });

    HMaster newMaster = UTIL.getMiniHBaseCluster().getMaster();
    assertTrue(newMaster.getServerManager().getDeadServers().isDeadServer(deadServer));
    ProcedureExecutor<MasterProcedureEnv> newProcExec = newMaster.getMasterProcedureExecutor();
    UTIL.waitFor(60000, () -> newProcExec.isFinished(pid1) && newProcExec.isFinished(pid2));
  }
}
