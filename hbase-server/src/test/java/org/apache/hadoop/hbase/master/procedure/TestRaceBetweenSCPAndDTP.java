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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Testcase for HBASE-28522.
 * <p>
 * We used to have test with the same name but in different package for HBASE-23636, where DTP will
 * hold the exclusive lock all the time, and it will reset TRSPs which has been attached to
 * RegionStateNodes, so we need special logic in SCP to deal with it.
 * <p>
 * After HBASE-28522, DTP will not reset TRSPs any more, so SCP does not need to take care of this
 * special case, thues we removed the special logic in SCP and also the UT for HBASE-22636 is not
 * valid any more, so we just removed the old one and introduce a new one with the same name here.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestRaceBetweenSCPAndDTP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRaceBetweenSCPAndDTP.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRaceBetweenSCPAndDTP.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("Race");

  private static byte[] CF = Bytes.toBytes("cf");

  private static CountDownLatch ARRIVE_GET_REPLICATION_PEER_MANAGER;

  private static CountDownLatch RESUME_GET_REPLICATION_PEER_MANAGER;

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException, KeeperException {
      super(conf);
    }

    @Override
    public ReplicationPeerManager getReplicationPeerManager() {
      if (ARRIVE_GET_REPLICATION_PEER_MANAGER != null) {
        ARRIVE_GET_REPLICATION_PEER_MANAGER.countDown();
        ARRIVE_GET_REPLICATION_PEER_MANAGER = null;
        try {
          RESUME_GET_REPLICATION_PEER_MANAGER.await();
        } catch (InterruptedException e) {
        }
      }
      return super.getReplicationPeerManager();
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(2);
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private boolean wasExecuted(Procedure<?> proc) {
    // RUNNABLE is not enough to make sure that the DTP has acquired the table lock, as we will set
    // procedure to RUNNABLE first and then acquire the execution lock
    return proc.wasExecuted() || proc.getState() == ProcedureState.WAITING_TIMEOUT
      || proc.getState() == ProcedureState.WAITING;
  }

  @Test
  public void testRace() throws Exception {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ServerName sn = am.getRegionStates().getRegionState(region).getServerName();
    LOG.info("ServerName={}, region={}", sn, region);

    ARRIVE_GET_REPLICATION_PEER_MANAGER = new CountDownLatch(1);
    RESUME_GET_REPLICATION_PEER_MANAGER = new CountDownLatch(1);
    // Assign to local variable because this static gets set to null in above running thread and
    // so NPE.
    CountDownLatch cdl = ARRIVE_GET_REPLICATION_PEER_MANAGER;
    UTIL.getMiniHBaseCluster().stopRegionServer(sn);
    cdl.await();

    Future<?> future = UTIL.getAdmin().disableTableAsync(NAME);
    ProcedureExecutor<?> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // make sure the DTP has been executed
    UTIL.waitFor(60000,
      () -> procExec.getProcedures().stream().filter(p -> p instanceof DisableTableProcedure)
        .map(p -> (DisableTableProcedure) p).filter(p -> p.getTableName().equals(NAME))
        .anyMatch(this::wasExecuted));
    RESUME_GET_REPLICATION_PEER_MANAGER.countDown();

    // make sure the DTP can finish
    future.get();

    // also make sure all SCPs are finished
    UTIL.waitFor(60000, () -> procExec.getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).allMatch(Procedure::isFinished));
  }
}
