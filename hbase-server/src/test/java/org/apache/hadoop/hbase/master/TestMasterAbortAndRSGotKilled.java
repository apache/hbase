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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterAbortAndRSGotKilled {
  private static Logger LOG =
    LoggerFactory.getLogger(TestMasterAbortAndRSGotKilled.class.getName());

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterAbortAndRSGotKilled.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static CountDownLatch countDownLatch = new CountDownLatch(1);

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      DelayCloseCP.class.getName());
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
  public void test() throws Exception {
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
    UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
      .submitProcedure(moveRegionProcedure);
    countDownLatch.await();
    UTIL.getMiniHBaseCluster().stopMaster(0);
    UTIL.getMiniHBaseCluster().startMaster();
    // wait until master initialized
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster() != null &&
      UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    Assert.assertTrue("Should be 3 RS after master restart",
      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 3);

  }

  public static class DelayCloseCP implements RegionCoprocessor, RegionObserver {

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
        throws IOException {
      if (!c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
        LOG.info("begin to sleep");
        countDownLatch.countDown();
        // Sleep here so we can stuck the RPC call
        Threads.sleep(10000);
        LOG.info("finish sleep");
      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }
}
