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

import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestExceptionInUnassignedRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExceptionInUnassignedRegion.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      ThrowInCloseCP.class.getName());
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExceptionInUnassignRegion() {
    ProcedureExecutor procedureExecutor =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();

    JVMClusterUtil.RegionServerThread rsThread = null;
    for (JVMClusterUtil.RegionServerThread t : UTIL.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      if (!t.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        rsThread = t;
        break;
      }
    }
    // find the rs and hri of the table
    HRegionServer rs = rsThread.getRegionServer();
    RegionInfo hri = rs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    TransitRegionStateProcedure moveRegionProcedure = TransitRegionStateProcedure.reopen(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(), hri);
    RegionStateNode regionNode = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getOrCreateRegionStateNode(hri);
    regionNode.setProcedure(moveRegionProcedure);
    long prodId = procedureExecutor.submitProcedure(moveRegionProcedure);
    ProcedureTestingUtility.waitProcedure(procedureExecutor, prodId);

    Assert.assertEquals("Should be two RS since other is aborted", 2,
      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    Assert.assertNull("RIT Map doesn't have correct value",
      getRegionServer(0).getRegionsInTransitionInRS().get(hri.getEncodedNameAsBytes()));
    Assert.assertNull("RIT Map doesn't have correct value",
      getRegionServer(1).getRegionsInTransitionInRS().get(hri.getEncodedNameAsBytes()));
    Assert.assertNull("RIT Map doesn't have correct value",
      getRegionServer(2).getRegionsInTransitionInRS().get(hri.getEncodedNameAsBytes()));
  }

  private HRegionServer getRegionServer(int index) {
    return UTIL.getMiniHBaseCluster().getRegionServer(index);
  }

  public static class ThrowInCloseCP implements RegionCoprocessor, RegionObserver {

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
      if (!c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
        throw new RuntimeException();
      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }
}
