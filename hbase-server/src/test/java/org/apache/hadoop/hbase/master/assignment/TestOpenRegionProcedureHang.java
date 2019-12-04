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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * See HBASE-22060 and HBASE-22074 for more details.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestOpenRegionProcedureHang {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOpenRegionProcedureHang.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestOpenRegionProcedureHang.class);

  private static CountDownLatch ARRIVE;
  private static CountDownLatch RESUME;

  private static CountDownLatch FINISH;

  private static CountDownLatch ABORT;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
        ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      RegionStateTransition transition = req.getTransition(0);
      if (transition.getTransitionCode() == TransitionCode.OPENED &&
        ProtobufUtil.toTableName(transition.getRegionInfo(0).getTableName()).equals(NAME) &&
        ARRIVE != null) {
        ARRIVE.countDown();
        try {
          RESUME.await();
          RESUME = null;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        try {
          return super.reportRegionStateTransition(req);
        } finally {
          FINISH.countDown();
        }
      } else {
        return super.reportRegionStateTransition(req);
      }
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master) {
      return new AssignmentManagerForTest(master);
    }

    @Override
    public void abort(String reason, Throwable cause) {
      // hang here so we can finish the reportRegionStateTransition call, which is the most
      // important part to reproduce the bug
      if (ABORT != null) {
        try {
          ABORT.await();
          ABORT = null;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      super.abort(reason, cause);
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Open");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);

    // make sure we do not timeout when caling reportRegionStateTransition
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 10 * 60 * 1000);
    conf.setInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY, 10 * 60 * 1000);
    UTIL
      .startMiniCluster(StartMiniClusterOption.builder().numMasters(2).numRegionServers(3).build());
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, KeeperException, IOException {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();

    HRegionServer rs1 = UTIL.getRSForFirstRegionInTable(NAME);
    HRegionServer rs2 = UTIL.getOtherRegionServer(rs1);

    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    FINISH = new CountDownLatch(1);
    ABORT = new CountDownLatch(1);
    am.moveAsync(new RegionPlan(region, rs1.getServerName(), rs2.getServerName()));

    ARRIVE.await();
    ARRIVE = null;
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    master.getZooKeeper().close();
    UTIL.waitFor(30000, () -> {
      for (MasterThread mt : UTIL.getMiniHBaseCluster().getMasterThreads()) {
        if (mt.getMaster() != master && mt.getMaster().isActiveMaster()) {
          return mt.getMaster().isInitialized();
        }
      }
      return false;
    });
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    UTIL.waitFor(30000,
      () -> procExec.getProcedures().stream().filter(p -> p instanceof OpenRegionProcedure)
        .map(p -> (OpenRegionProcedure) p).anyMatch(p -> p.region.getTable().equals(NAME)));
    OpenRegionProcedure proc = procExec.getProcedures().stream()
      .filter(p -> p instanceof OpenRegionProcedure).map(p -> (OpenRegionProcedure) p)
      .filter(p -> p.region.getTable().equals(NAME)).findFirst().get();
    // wait a bit to let the OpenRegionProcedure send out the request
    Thread.sleep(2000);
    RESUME.countDown();
    if (!FINISH.await(15, TimeUnit.SECONDS)) {
      LOG.info("Wait reportRegionStateTransition to finish timed out, this is possible if" +
        " we update the procedure store, as the WALProcedureStore" +
        " will retry forever to roll the writer if it is not closed");
    }
    FINISH = null;
    // if the reportRegionTransition is finished, wait a bit to let it return the data to RS
    Thread.sleep(2000);
    ABORT.countDown();

    UTIL.waitFor(30000, () -> procExec.isFinished(proc.getProcId()));
    UTIL.waitFor(30000, () -> procExec.isFinished(proc.getParentProcId()));
  }
}
