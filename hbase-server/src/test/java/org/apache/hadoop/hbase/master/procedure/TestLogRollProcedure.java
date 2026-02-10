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

import static org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY;
import static org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestLogRollProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogRollProcedure.class);

  @Rule
  public TestName name = new TestName();

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set(DISPATCH_DELAY_CONF_KEY, "2000");
    conf.set(DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, "128");
    TEST_UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimpleLogRoll() throws IOException {
    HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    long fileNumBefore = ((AbstractFSWAL<?>) rs.getWAL(null)).getFilenum();

    TEST_UTIL.getAdmin().rollAllWALWriters();

    long fileNumAfter = ((AbstractFSWAL<?>) rs.getWAL(null)).getFilenum();
    assertTrue(fileNumAfter > fileNumBefore);
  }

  @Test
  public void testMasterRestarts() throws IOException {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer rs = cluster.getRegionServer(0);
    long fileNumBefore = ((AbstractFSWAL<?>) rs.getWAL(null)).getFilenum();

    LogRollProcedure procedure = new LogRollProcedure();
    long procId = cluster.getMaster().getMasterProcedureExecutor().submitProcedure(procedure);

    TEST_UTIL.waitFor(60000, () -> cluster.getMaster().getMasterProcedureExecutor().getProcedures()
      .stream().anyMatch(p -> p instanceof LogRollRemoteProcedure));
    ServerName serverName = cluster.getMaster().getServerName();
    cluster.killMaster(serverName);
    cluster.waitForMasterToStop(serverName, 30000);
    cluster.startMaster();
    cluster.waitForActiveAndReadyMaster();

    ProcedureExecutor<MasterProcedureEnv> exec = cluster.getMaster().getMasterProcedureExecutor();
    TEST_UTIL.waitFor(30000, () -> exec.isRunning() && exec.isFinished(procId));

    long fileNumAfter = ((AbstractFSWAL<?>) rs.getWAL(null)).getFilenum();

    assertTrue(fileNumAfter > fileNumBefore);
  }
}
