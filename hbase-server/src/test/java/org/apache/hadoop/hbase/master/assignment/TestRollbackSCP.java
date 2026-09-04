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
package org.apache.hadoop.hbase.master.assignment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SCP does not support rollback actually, here we just want to simulate that when there is a code
 * bug, SCP and its sub procedures will not hang there forever, and it will not mess up the
 * procedure store.
 */
@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestRollbackSCP {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] FAMILY = Bytes.toBytes("family");

  private static final AtomicBoolean INJECTED = new AtomicBoolean(false);

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    CompletableFuture<Void> persistToMeta(RegionStateNode regionNode) {
      TransitRegionStateProcedure proc = regionNode.getProcedure();
      if (!regionNode.getRegionInfo().isMetaRegion() && proc.hasParent()) {
        Procedure<?> p =
          getMaster().getMasterProcedureExecutor().getProcedure(proc.getRootProcId());
        // fail the procedure if it is a sub procedure for SCP
        if (p instanceof ServerCrashProcedure) {
          if (INJECTED.compareAndSet(false, true)) {
            ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdateInRollback(
              getMaster().getMasterProcedureExecutor(), true);
          }
          return FutureUtils.failedFuture(new RuntimeException("inject code bug"));
        }
      }
      return super.persistToMeta(regionNode);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    // We kill the region server that carries meta during the test and then restart the master to
    // simulate a real master failover. Allow a restarting master to finish initializing with the
    // remaining region servers instead of waiting for the original count (one is intentionally
    // down), otherwise the failover would block in finishActiveMasterInitialization.
    UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numDataNodes(3).numRegionServers(3)
      .masterClass(HMasterForTest.class).build());
    UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
    UTIL.getAdmin().balance(BalanceRequest.newBuilder().setIgnoreRegionsInTransition(true).build());
    UTIL.waitUntilNoRegionsInTransition();
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDownAfterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp() throws IOException {
    UTIL.ensureSomeNonStoppedRegionServersAvailable(2);
  }

  private ServerCrashProcedure getSCPForServer(ServerName serverName) throws IOException {
    return UTIL.getMiniHBaseCluster().getMaster().getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).map(p -> (ServerCrashProcedure) p)
      .filter(p -> p.getServerName().equals(serverName)).findFirst().orElse(null);
  }

  private Matcher<Procedure<MasterProcedureEnv>> subProcOf(Procedure<MasterProcedureEnv> proc) {
    return new BaseMatcher<Procedure<MasterProcedureEnv>>() {

      @Override
      public boolean matches(Object item) {
        if (!(item instanceof Procedure)) {
          return false;
        }
        Procedure<?> p = (Procedure<?>) item;
        return p.hasParent() && p.getRootProcId() == proc.getProcId();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("sub procedure of(").appendValue(proc).appendText(")");
      }
    };
  }

  /**
   * Restart the active master, exactly like a real master failover: kill the current master and
   * bring up a fresh one, which reloads the procedures from the store and resumes them. This is a
   * faithful reproduction of what happens in production when a master crashes and recovers -- a new
   * process comes up with a brand new procedure executor and scheduler, and rejects region state
   * reports (via HMaster.checkServiceStarted) until it has finished initializing (i.e. after the
   * executor has loaded). We use this instead of reloading the executor in place, because the
   * in-place reload runs under a live master where other threads (an async meta-update wake-up, or
   * an incoming reportRegionStateTransition from a live region server) can push a procedure back
   * into the shared scheduler in the small window between clearing it and ProcedureExecutor.load()
   * asserting it is empty, which does not happen in a real failover.
   */
  private void restartMaster() throws IOException {
    HMaster oldMaster = UTIL.getMiniHBaseCluster().getMaster();
    UTIL.getMiniHBaseCluster().killMaster(oldMaster.getServerName());
    UTIL.getMiniHBaseCluster().waitForMasterToStop(oldMaster.getServerName(), 60000);
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(120000);
  }

  @Test
  public void testFailAndRollback() throws Exception {
    HRegionServer rsWithMeta = UTIL.getRSForFirstRegionInTable(TableName.META_TABLE_NAME);
    UTIL.getMiniHBaseCluster().killRegionServer(rsWithMeta.getServerName());
    UTIL.waitFor(15000, () -> getSCPForServer(rsWithMeta.getServerName()) != null);
    ServerCrashProcedure scp = getSCPForServer(rsWithMeta.getServerName());
    long scpProcId = scp.getProcId();
    ProcedureExecutor<MasterProcedureEnv> initialProcExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // wait for the procedure to stop, as we inject a code bug and also set kill before store update
    UTIL.waitFor(30000, () -> !initialProcExec.isRunning());
    // Restart the master to simulate a real master failover instead of reloading the procedure
    // executor in place. A fresh master comes up with a brand new procedure executor and scheduler,
    // reloads the procedure from the store and resumes it, exactly like production. Since SCP does
    // not support rollback, the failed procedure ends up rolled back.
    restartMaster();
    // Wait until the fresh master has finished the procedure. getResult only returns non-null once
    // the procedure has completed (it is then retained for a while before being cleaned up).
    UTIL.waitFor(120000, () -> UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
      .getResult(scpProcId) != null);
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // SCP does not support rollback, so it ends up in the ROLLEDBACK state, which also counts as
    // failed (see Procedure.isFailed).
    Procedure<?> result = procExec.getResult(scpProcId);
    assertTrue(result.isFailed());
    assertThat(result.getException().getMessage(), containsString("inject code bug"));
    // make sure all sub procedures are cleaned up
    assertThat(procExec.getProcedures(), everyItem(not(subProcOf(scp))));
  }
}
