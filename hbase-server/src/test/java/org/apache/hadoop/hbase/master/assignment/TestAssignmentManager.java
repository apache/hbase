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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestAssignmentManager extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAssignmentManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentManager.class);

  @Test(expected = NullPointerException.class)
  public void testWaitServerReportEventWithNullServer() throws UnexpectedStateException {
    // Test what happens if we pass in null server. I'd expect it throws NPE.
    if (this.am.waitServerReportEvent(null, null)) {
      throw new UnexpectedStateException();
    }
  }

  @Test
  public void testAssignWithGoodExec() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testAssign(new GoodRsExecutor());

    assertEquals(assignSubmittedCount + NREGIONS,
      assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testAssignAndCrashBeforeResponse() throws Exception {
    TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangThenRSCrashExecutor());
    TransitRegionStateProcedure proc = createAssignProcedure(hri);
    waitOnFuture(submitProcedure(proc));
  }

  @Test
  public void testUnassignAndCrashBeforeResponse() throws Exception {
    TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangOnCloseThenRSCrashExecutor());
    for (int i = 0; i < HangOnCloseThenRSCrashExecutor.TYPES_OF_FAILURE; i++) {
      TransitRegionStateProcedure assign = createAssignProcedure(hri);
      waitOnFuture(submitProcedure(assign));
      TransitRegionStateProcedure unassign = createUnassignProcedure(hri);
      waitOnFuture(submitProcedure(unassign));
    }
  }

  // Disabled for now. Since HBASE-18551, this mock is insufficient.
  @Ignore
  @Test
  public void testSocketTimeout() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 3));
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 1));
    // exception.expect(ServerCrashException.class);
    waitOnFuture(submitProcedure(createUnassignProcedure(hri)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount + 1, unassignProcMetrics.getFailedCounter().getCount());
  }

  private void testAssign(final MockRSExecutor executor) throws Exception {
    testAssign(executor, NREGIONS);
  }

  private void testAssign(MockRSExecutor executor, int nRegions) throws Exception {
    rsDispatcher.setMockRsExecutor(executor);

    TransitRegionStateProcedure[] assignments = new TransitRegionStateProcedure[nRegions];

    long st = System.currentTimeMillis();
    bulkSubmit(assignments);

    for (int i = 0; i < assignments.length; ++i) {
      ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), assignments[i]);
      assertTrue(assignments[i].toString(), assignments[i].isSuccess());
    }
    long et = System.currentTimeMillis();
    float sec = ((et - st) / 1000.0f);
    LOG.info(String.format("[T] Assigning %dprocs in %s (%.2fproc/sec)", assignments.length,
      StringUtils.humanTimeDiff(et - st), assignments.length / sec));
  }

  @Test
  public void testAssignAnAssignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAnAssignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    Future<byte[]> futureA = submitProcedure(createAssignProcedure(hri));

    // wait first assign
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // Second should be a noop. We should recognize region is already OPEN internally
    // and skip out doing nothing.
    // wait second assign
    Future<byte[]> futureB = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(futureB);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // TODO: What else can we do to ensure just a noop.

    // TODO: Though second assign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testUnassignAnUnassignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testUnassignAnUnassignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    // assign the region first
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    final Future<byte[]> futureA = submitProcedure(createUnassignProcedure(hri));

    // Wait first unassign.
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // Second should be a noop. We should recognize region is already CLOSED internally
    // and skip out doing nothing.
    final Future<byte[]> futureB = submitProcedure(createUnassignProcedure(hri));
    waitOnFuture(futureB);
    // Ensure we are still CLOSED.
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // TODO: What else can we do to ensure just a noop.

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    // TODO: Though second unassign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(unassignSubmittedCount + 2, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
  }

  /**
   * It is possible that when AM send assign meta request to a RS successfully, but RS can not send
   * back any response, which cause master startup hangs forever
   */
  @Test
  public void testAssignMetaAndCrashBeforeResponse() throws Exception {
    tearDown();
    // See setUp(), start HBase until set up meta
    util = new HBaseTestingUtility();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    setupConfiguration(util.getConfiguration());
    master = new MockMasterServices(util.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    am = master.getAssignmentManager();

    // Assign meta
    rsDispatcher.setMockRsExecutor(new HangThenRSRestartExecutor());
    am.assign(RegionInfoBuilder.FIRST_META_REGIONINFO);
    assertEquals(true, am.isMetaAssigned());

    // set it back as default, see setUpMeta()
    am.wakeMetaLoadedEvent();
  }
}
