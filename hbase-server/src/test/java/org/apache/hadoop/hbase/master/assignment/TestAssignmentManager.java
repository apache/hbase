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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
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

  @Test
  public void testAssignSocketTimeout() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20));
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    // we crashed a rs, so it is possible that there are other regions on the rs which will also be
    // reassigned, so here we just assert greater than, not the exact number.
    assertTrue(assignProcMetrics.getSubmittedCounter().getCount() > assignSubmittedCount);
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testAssignQueueFullOnce() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new CallQueueTooBigOnceRsExecutor());
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testTimeoutThenQueueFull() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new TimeoutThenCallQueueTooBigRsExecutor(10));
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));
    rsDispatcher.setMockRsExecutor(new TimeoutThenCallQueueTooBigRsExecutor(15));
    waitOnFuture(submitProcedure(createUnassignProcedure(hri)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
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
    setUpMeta(new HangThenRSRestartExecutor());
    // set it back as default, see setUpMeta()
    am.wakeMetaLoadedEvent();
  }

  private void assertCloseThenOpen() {
    assertEquals(closeSubmittedCount + 1, closeProcMetrics.getSubmittedCounter().getCount());
    assertEquals(closeFailedCount, closeProcMetrics.getFailedCounter().getCount());
    assertEquals(openSubmittedCount + 1, openProcMetrics.getSubmittedCounter().getCount());
    assertEquals(openFailedCount, openProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testMove() throws Exception {
    TableName tableName = TableName.valueOf("testMove");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(hri);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    am.move(hri);

    assertEquals(moveSubmittedCount + 1, moveProcMetrics.getSubmittedCounter().getCount());
    assertEquals(moveFailedCount, moveProcMetrics.getFailedCounter().getCount());
    assertCloseThenOpen();
  }

  @Test
  public void testReopen() throws Exception {
    TableName tableName = TableName.valueOf("testReopen");
    RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(hri);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    TransitRegionStateProcedure proc =
      TransitRegionStateProcedure.reopen(master.getMasterProcedureExecutor().getEnvironment(), hri);
    am.getRegionStates().getRegionStateNode(hri).setProcedure(proc);
    waitOnFuture(submitProcedure(proc));

    assertEquals(reopenSubmittedCount + 1, reopenProcMetrics.getSubmittedCounter().getCount());
    assertEquals(reopenFailedCount, reopenProcMetrics.getFailedCounter().getCount());
    assertCloseThenOpen();
  }

  @Test
  public void testLoadRegionFromMetaAfterRegionManuallyAdded() throws Exception {
    try {
      this.util.startMiniCluster();
      final AssignmentManager am = this.util.getHBaseCluster().getMaster().getAssignmentManager();
      final TableName tableName = TableName.
        valueOf("testLoadRegionFromMetaAfterRegionManuallyAdded");
      this.util.createTable(tableName, "f");
      RegionInfo hri = createRegionInfo(tableName, 1);
      assertNull("RegionInfo was just instantiated by the test, but "
        + "shouldn't be in AM regionStates yet.", am.getRegionStates().getRegionState(hri));
      MetaTableAccessor.addRegionToMeta(this.util.getConnection(), hri);
      assertNull("RegionInfo was manually added in META, but "
        + "shouldn't be in AM regionStates yet.", am.getRegionStates().getRegionState(hri));
      hri = am.loadRegionFromMeta(hri.getEncodedName());
      assertEquals(hri.getEncodedName(),
        am.getRegionStates().getRegionState(hri).getRegion().getEncodedName());
    }finally {
      this.util.killMiniHBaseCluster();
    }
  }

  @Test
  public void testLoadRegionFromMetaRegionNotInMeta() throws Exception {
    try {
      this.util.startMiniCluster();
      final AssignmentManager am = this.util.getHBaseCluster().getMaster().getAssignmentManager();
      final TableName tableName = TableName.valueOf("testLoadRegionFromMetaRegionNotInMeta");
      this.util.createTable(tableName, "f");
      final RegionInfo hri = createRegionInfo(tableName, 1);
      assertNull("RegionInfo was just instantiated by the test, but "
        + "shouldn't be in AM regionStates yet.", am.getRegionStates().getRegionState(hri));
      assertNull("RegionInfo was never added in META, should had returned null.",
        am.loadRegionFromMeta(hri.getEncodedName()));
    }finally {
      this.util.killMiniHBaseCluster();
    }
  }
}
