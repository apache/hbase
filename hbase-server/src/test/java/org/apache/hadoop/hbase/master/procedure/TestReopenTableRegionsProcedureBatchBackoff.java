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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Confirm that we will rate limit reopen batches when reopening all table regions. This can avoid
 * the pain associated with reopening too many regions at once.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureBatchBackoff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureBatchBackoff.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("BatchBackoff");
  private static final int BACKOFF_MILLIS_PER_RS = 3_000;
  private static final int REOPEN_BATCH_SIZE_MAX = 8;
  private static final int NUM_REGIONS = 10;
  private static final int NUM_BATCHES = 4; // 1 + 2 + 4 + 8 > 10

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(1);
    UTIL.createMultiRegionTable(TABLE_NAME, CF, NUM_REGIONS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegionBatchBackoff() throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(NUM_REGIONS <= regions.size());
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, BACKOFF_MILLIS_PER_RS, REOPEN_BATCH_SIZE_MAX);
    procExec.submitProcedure(proc);
    Instant startedAt = Instant.now();
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);
    Instant stoppedAt = Instant.now();
    assertTrue(Duration.between(startedAt, stoppedAt).toMillis()
        > (NUM_BATCHES - 1) * BACKOFF_MILLIS_PER_RS);
    assertEquals(NUM_BATCHES, proc.getBatchesProcessed());
  }

  @Test
  public void testRegionBatchNoBackoff() throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(NUM_REGIONS <= regions.size());
    int noBackoffMillis = 0;
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, noBackoffMillis, REOPEN_BATCH_SIZE_MAX);
    procExec.submitProcedure(proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc,
      (long) regions.size() * BACKOFF_MILLIS_PER_RS);
    assertEquals(NUM_BATCHES, proc.getBatchesProcessed());
  }
}
