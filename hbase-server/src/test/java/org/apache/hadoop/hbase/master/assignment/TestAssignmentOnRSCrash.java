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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, LargeTests.class})
public class TestAssignmentOnRSCrash {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentOnRSCrash.class);

  private static final TableName TEST_TABLE = TableName.valueOf("testb");
  private static final String FAMILY_STR = "f";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STR);
  private static final int NUM_RS = 3;

  private HBaseTestingUtility UTIL;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.set("hbase.balancer.tablesOnMaster", "none");
  }

  @Before
  public void setup() throws Exception {
    UTIL = new HBaseTestingUtility();

    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);

    UTIL.createTable(TEST_TABLE, new byte[][] { FAMILY }, new byte[][] {
      Bytes.toBytes("B"), Bytes.toBytes("D"), Bytes.toBytes("F"), Bytes.toBytes("L")
    });
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testKillRsWithUserRegionWithData() throws Exception {
    testCrashRsWithUserRegion(true, true);
  }

  @Test
  public void testKillRsWithUserRegionWithoutData() throws Exception {
    testCrashRsWithUserRegion(true, false);
  }

  @Test
  public void testStopRsWithUserRegionWithData() throws Exception {
    testCrashRsWithUserRegion(false, true);
  }

  @Test
  public void testStopRsWithUserRegionWithoutData() throws Exception {
    testCrashRsWithUserRegion(false, false);
  }

  private void testCrashRsWithUserRegion(final boolean kill, final boolean withData)
      throws Exception {
    final int NROWS = 100;
    int nkilled = 0;
    for (RegionInfo hri: UTIL.getAdmin().getRegions(TEST_TABLE)) {
      ServerName serverName = AssignmentTestingUtil.getServerHoldingRegion(UTIL, hri);
      if (AssignmentTestingUtil.isServerHoldingMeta(UTIL, serverName)) continue;

      if (withData) {
        testInsert(hri, NROWS);
      }

      // wait for regions to enter in transition and then to get out of transition
      AssignmentTestingUtil.crashRs(UTIL, serverName, kill);
      AssignmentTestingUtil.waitForRegionToBeInTransition(UTIL, hri);
      UTIL.waitUntilNoRegionsInTransition();

      if (withData) {
        assertEquals(NROWS, testGet(hri, NROWS));
      }

      // region should be moved to another RS
      assertNotEquals(serverName, AssignmentTestingUtil.getServerHoldingRegion(UTIL, hri));

      if (++nkilled == (NUM_RS - 1)) {
        break;
      }
    }
    assertTrue("expected RSs to be killed", nkilled > 0);
  }

  @Test
  public void testKillRsWithMetaRegion() throws Exception {
    testCrashRsWithMetaRegion(true);
  }

  @Test
  public void testStopRsWithMetaRegion() throws Exception {
    testCrashRsWithMetaRegion(false);
  }

  private void testCrashRsWithMetaRegion(final boolean kill) throws Exception {
    int nkilled = 0;
    for (RegionInfo hri: AssignmentTestingUtil.getMetaRegions(UTIL)) {
      ServerName serverName = AssignmentTestingUtil.crashRsWithRegion(UTIL, hri, kill);

      // wait for region to enter in transition and then to get out of transition
      AssignmentTestingUtil.waitForRegionToBeInTransition(UTIL, hri);
      UTIL.waitUntilNoRegionsInTransition();
      testGet(hri, 10);

      // region should be moved to another RS
      assertNotEquals(serverName, AssignmentTestingUtil.getServerHoldingRegion(UTIL, hri));

      if (++nkilled == (NUM_RS - 1)) {
        break;
      }
    }
    assertTrue("expected RSs to be killed", nkilled > 0);
  }

  private void testInsert(final RegionInfo hri, final int nrows) throws IOException {
    final Table table = UTIL.getConnection().getTable(hri.getTable());
    for (int i = 0; i < nrows; ++i) {
      final byte[] row = Bytes.add(hri.getStartKey(), Bytes.toBytes(i));
      final Put put = new Put(row);
      put.addColumn(FAMILY, null, row);
      table.put(put);
    }
  }

  public int testGet(final RegionInfo hri, final int nrows) throws IOException {
    int nresults = 0;
    final Table table = UTIL.getConnection().getTable(hri.getTable());
    for (int i = 0; i < nrows; ++i) {
      final byte[] row = Bytes.add(hri.getStartKey(), Bytes.toBytes(i));
      final Result result = table.get(new Get(row));
      if (result != null && !result.isEmpty() &&
          Bytes.equals(row, result.getValue(FAMILY, null))) {
        nresults++;
      }
    }
    return nresults;
  }
}
