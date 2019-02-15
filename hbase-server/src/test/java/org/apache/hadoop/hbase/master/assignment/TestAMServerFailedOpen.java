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
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestAMServerFailedOpen extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAMServerFailedOpen.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAMServerFailedOpen.class);

  @Override
  protected int getAssignMaxAttempts() {
    // do not need to retry so many times as we will finally fail...
    return 10;
  }

  @Test
  public void testServerNotYetRunning() throws Exception {
    testRetriesExhaustedFailure(TableName.valueOf(this.name.getMethodName()),
      new ServerNotYetRunningRsExecutor());
  }

  private void testRetriesExhaustedFailure(final TableName tableName, final MockRSExecutor executor)
      throws Exception {
    RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(createAssignProcedure(hri)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
    }

    // Assign the region (without problems)
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    waitOnFuture(submitProcedure(createAssignProcedure(hri)));
  }

  @Test
  public void testDoNotRetryExceptionOnAssignment() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testFailedOpen(TableName.valueOf("testDoNotRetryExceptionOnAssignment"),
      new FaultyRsExecutor(new DoNotRetryIOException("test do not retry fault")));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount + 1, assignProcMetrics.getFailedCounter().getCount());
  }

  private void testFailedOpen(final TableName tableName, final MockRSExecutor executor)
      throws Exception {
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(createAssignProcedure(hri)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("REGION STATE " + am.getRegionStates().getRegionStateNode(hri));
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
      assertEquals(true, am.getRegionStates().getRegionState(hri).isFailedOpen());
    }
  }

  @Test
  public void testCallQueueTooBigExceptionOnAssignment() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testFailedOpen(TableName.valueOf("testCallQueueTooBigExceptionOnAssignment"),
      new FaultyRsExecutor(new CallQueueTooBigException("test do not retry fault")));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount + 1, assignProcMetrics.getFailedCounter().getCount());
  }
}
