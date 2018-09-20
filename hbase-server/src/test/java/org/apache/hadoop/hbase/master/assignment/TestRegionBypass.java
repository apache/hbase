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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.testclassification.LargeTests;

/**
 * Tests bypass on a region assign/unassign
 */
@Category({LargeTests.class})
public class TestRegionBypass {
  private final static Logger LOG = LoggerFactory.getLogger(TestRegionBypass.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionBypass.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private TableName tableName;

  @BeforeClass
  public static void startCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    this.tableName = TableName.valueOf(this.name.getMethodName());
    // Create a table. Has one region at least.
    TEST_UTIL.createTable(this.tableName, Bytes.toBytes("cf"));

  }

  @Test
  public void testBypass() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    List<RegionInfo> regions = admin.getRegions(this.tableName);
    for (RegionInfo ri: regions) {
      admin.unassign(ri.getRegionName(), false);
    }
    List<Long> pids = new ArrayList<>(regions.size());
    for (RegionInfo ri: regions) {
      Procedure<MasterProcedureEnv> p = new StallingAssignProcedure(ri);
      pids.add(TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().
          submitProcedure(p));
    }
    for (Long pid: pids) {
      while (!TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().isStarted(pid)) {
        Thread.currentThread().yield();
      }
    }
    // Call bypass on all. We should be stuck in the dispatch at this stage.
    List<Procedure<MasterProcedureEnv>> ps =
        TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().getProcedures();
    for (Procedure<MasterProcedureEnv> p: ps) {
      if (p instanceof StallingAssignProcedure) {
        List<Boolean> bs = TEST_UTIL.getHbck().
            bypassProcedure(Arrays.<Long>asList(p.getProcId()), 0, false, false);
        for (Boolean b: bs) {
          LOG.info("BYPASSED {} {}", p.getProcId(), b);
        }
      }
    }
    // Countdown the latch so its not hanging out.
    for (Procedure<MasterProcedureEnv> p: ps) {
      if (p instanceof StallingAssignProcedure) {
        ((StallingAssignProcedure)p).latch.countDown();
      }
    }
    // Try and assign WITHOUT override flag. Should fail!.
    for (RegionInfo ri: regions) {
      try {
        admin.assign(ri.getRegionName());
      } catch (Throwable dnrioe) {
        // Expected
        LOG.info("Expected {}", dnrioe);
      }
    }
    while (!TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().
        getActiveProcIds().isEmpty()) {
      Thread.currentThread().yield();
    }
    // Now assign with the override flag.
    for (RegionInfo ri: regions) {
      TEST_UTIL.getHbck().assigns(Arrays.<String>asList(ri.getEncodedName()), true);
    }
    while (!TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().
        getActiveProcIds().isEmpty()) {
      Thread.currentThread().yield();
    }
    for (RegionInfo ri: regions) {
      assertTrue(ri.toString(), TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().
          getRegionStates().isRegionOnline(ri));
    }
  }

  /**
   * An AssignProcedure that Stalls just before the finish.
   */
  public static class StallingAssignProcedure extends AssignProcedure {
    public final CountDownLatch latch = new CountDownLatch(2);

    public StallingAssignProcedure() {
      super();
    }

    public StallingAssignProcedure(RegionInfo regionInfo) {
      super(regionInfo);
    }

    @Override
    void setTransitionState(MasterProcedureProtos.RegionTransitionState state) {
      if (state == MasterProcedureProtos.RegionTransitionState.REGION_TRANSITION_DISPATCH) {
        try {
          LOG.info("LATCH2 {}", this.latch.getCount());
          this.latch.await();
          LOG.info("LATCH3 {}", this.latch.getCount());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else if (state == MasterProcedureProtos.RegionTransitionState.REGION_TRANSITION_QUEUE) {
        // Set latch.
        LOG.info("LATCH1 {}", this.latch.getCount());
        this.latch.countDown();
      }
      super.setTransitionState(state);
    }
  }
}
