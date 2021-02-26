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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testcase for HBASE-23636.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestRaceBetweenSCPAndDTP {
  private static final Logger LOG = LoggerFactory.getLogger(TestRaceBetweenSCPAndDTP.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRaceBetweenSCPAndDTP.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Race");

  private static byte[] CF = Bytes.toBytes("cf");

  private static CountDownLatch ARRIVE_GET_REGIONS_ON_TABLE;

  private static CountDownLatch RESUME_GET_REGIONS_ON_SERVER;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    public TransitRegionStateProcedure[] createUnassignProceduresForDisabling(TableName tableName) {
      if (ARRIVE_GET_REGIONS_ON_TABLE != null) {
        ARRIVE_GET_REGIONS_ON_TABLE.countDown();
        ARRIVE_GET_REGIONS_ON_TABLE = null;
        try {
          RESUME_GET_REGIONS_ON_SERVER.await();
        } catch (InterruptedException e) {
        }
      }
      TransitRegionStateProcedure[] procs = super.createUnassignProceduresForDisabling(tableName);
      return procs;
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException, KeeperException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master) {
      return new AssignmentManagerForTest(master);
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

  @Test
  public void test() throws Exception {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ServerName sn = am.getRegionStates().getRegionState(region).getServerName();
    LOG.info("ServerName={}, region={}", sn, region);

    ARRIVE_GET_REGIONS_ON_TABLE = new CountDownLatch(1);
    RESUME_GET_REGIONS_ON_SERVER = new CountDownLatch(1);
    // Assign to local variable because this static gets set to null in above running thread and
    // so NPE.
    CountDownLatch cdl = ARRIVE_GET_REGIONS_ON_TABLE;
    UTIL.getAdmin().disableTableAsync(NAME);
    cdl.await();

    ProcedureExecutor<?> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    UTIL.getMiniHBaseCluster().stopRegionServer(sn);
    long pid = Procedure.NO_PROC_ID;
    do {
      Threads.sleep(1);
      pid = getSCPPID(procExec);
    } while (pid != Procedure.NO_PROC_ID);
    final long scppid = pid;
    UTIL.waitFor(60000, () -> procExec.isFinished(scppid));
    RESUME_GET_REGIONS_ON_SERVER.countDown();

    long dtpProcId =
        procExec.getProcedures().stream().filter(p -> p instanceof DisableTableProcedure)
            .map(p -> (DisableTableProcedure) p).findAny().get().getProcId();
    UTIL.waitFor(60000, () -> procExec.isFinished(dtpProcId));
  }

  /**
   * @return Returns {@link Procedure#NO_PROC_ID} if no SCP found else actual pid.
   */
  private long getSCPPID(ProcedureExecutor<?> e) {
    Optional<ServerCrashProcedure> optional = e.getProcedures().stream().
      filter(p -> p instanceof ServerCrashProcedure).map(p -> (ServerCrashProcedure) p).findAny();
    return optional.isPresent()? optional.get().getProcId(): Procedure.NO_PROC_ID;
  }
}
