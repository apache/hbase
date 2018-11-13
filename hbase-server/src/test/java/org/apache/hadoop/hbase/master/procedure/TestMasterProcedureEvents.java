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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterProcedureEvents {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureEvents.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCreateTableProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  private static void setupConf(Configuration conf) throws IOException {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniDFSCluster(3);
    CommonFSUtils.setWALRootDir(conf, new Path(conf.get("fs.defaultFS"), "/tmp/wal"));
    UTIL.startMiniCluster(2);
    UTIL.waitUntilNoRegionsInTransition();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd: UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testMasterInitializedEvent() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();

    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();

    while (!master.isInitialized()) {
      Thread.sleep(250);
    }
    master.setInitialized(false); // fake it, set back later

    // check event wait/wake
    testProcedureEventWaitWake(master, master.getInitializedEvent(),
      new CreateTableProcedure(procExec.getEnvironment(), htd, new RegionInfo[] { hri }));
  }

  private void testProcedureEventWaitWake(final HMaster master, final ProcedureEvent<?> event,
      final Procedure<MasterProcedureEnv> proc) throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    final MasterProcedureScheduler procSched = procExec.getEnvironment().getProcedureScheduler();

    final long startPollCalls = procSched.getPollCalls();
    final long startNullPollCalls = procSched.getNullPollCalls();

    // check that nothing is in the event queue
    LOG.debug("checking " + event);
    assertEquals(false, event.isReady());
    assertEquals(0, event.getSuspendedProcedures().size());

    // submit the procedure
    LOG.debug("submit " + proc);
    long procId = procExec.submitProcedure(proc);

    // wait until the event is in the queue (proc executed and got into suspended state)
    LOG.debug("wait procedure suspended on " + event);
    while (event.getSuspendedProcedures().size() < 1) Thread.sleep(25);

    // check that the proc is in the event queue
    LOG.debug("checking " + event + " size=" + event.getSuspendedProcedures().size());
    assertEquals(false, event.isReady());
    assertEquals(1, event.getSuspendedProcedures().size());

    // wake the event
    LOG.debug("wake " + event);
    event.wake(procSched);
    assertEquals(true, event.isReady());

    // wait until proc completes
    LOG.debug("waiting " + proc);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // check that nothing is in the event queue and the event is not suspended
    assertEquals(true, event.isReady());
    assertEquals(0, event.getSuspendedProcedures().size());
    LOG.debug("completed execution of " + proc +
      " pollCalls=" + (procSched.getPollCalls() - startPollCalls) +
      " nullPollCalls=" + (procSched.getNullPollCalls() - startNullPollCalls));
  }
}
