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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * SCP does not support rollback actually, here we just want to simulate that when there is a code
 * bug, SCP and its sub procedures will not hang there forever, and it will not mess up the
 * procedure store.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestRollbackSCP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRollbackSCP.class);

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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numDataNodes(3).numRegionServers(3)
      .masterClass(HMasterForTest.class).build());
    UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
    UTIL.getAdmin().balance(BalanceRequest.newBuilder().setIgnoreRegionsInTransition(true).build());
    UTIL.waitUntilNoRegionsInTransition();
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Before
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

  @Test
  public void testFailAndRollback() throws Exception {
    HRegionServer rsWithMeta = UTIL.getRSForFirstRegionInTable(TEST_UTIL.getConnection().getMetaTableName());
    UTIL.getMiniHBaseCluster().killRegionServer(rsWithMeta.getServerName());
    UTIL.waitFor(15000, () -> getSCPForServer(rsWithMeta.getServerName()) != null);
    ServerCrashProcedure scp = getSCPForServer(rsWithMeta.getServerName());
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // wait for the procedure to stop, as we inject a code bug and also set kill before store update
    UTIL.waitFor(30000, () -> !procExec.isRunning());
    // make sure that finally we could successfully rollback the procedure
    while (scp.getState() != ProcedureState.FAILED || !procExec.isRunning()) {
      MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, scp);
    }
    assertEquals(scp.getState(), ProcedureState.FAILED);
    assertThat(scp.getException().getMessage(), containsString("inject code bug"));
    // make sure all sub procedures are cleaned up
    assertThat(procExec.getProcedures(), everyItem(not(subProcOf(scp))));
  }
}
