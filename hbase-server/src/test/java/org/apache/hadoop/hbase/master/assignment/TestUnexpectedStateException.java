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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;

/**
 * Tests for HBASE-18408 "AM consumes CPU and fills up the logs really fast when there is no RS to
 * assign". If an {@link org.apache.hadoop.hbase.exceptions.UnexpectedStateException}, we'd spin on
 * the ProcedureExecutor consuming CPU and filling logs. Test new back-off facility.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestUnexpectedStateException {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUnexpectedStateException.class);
  @Rule public final TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestUnexpectedStateException.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte [] FAMILY = Bytes.toBytes("family");
  private TableName tableName;
  private static final int REGIONS = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.localcluster.assign.random.ports", false);
    TEST_UTIL.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, 50655);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    this.tableName = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createMultiRegionTable(this.tableName, FAMILY, REGIONS);
  }

  private RegionInfo pickArbitraryRegion(Admin admin) throws IOException {
    List<RegionInfo> regions = admin.getRegions(this.tableName);
    return regions.get(3);
  }

  /**
   * Manufacture a state that will throw UnexpectedStateException.
   * Change an assigned region's 'state' to be OPENING. That'll mess up a subsequent unassign
   * causing it to throw UnexpectedStateException. We can easily manufacture this infinite retry
   * state in UnassignProcedure because it has no startTransition. AssignProcedure does where it
   * squashes whatever the current region state is making it OFFLINE. That makes it harder to mess
   * it up. Make do with UnassignProcedure for now.
   */
  @Test
  public void testUnableToAssign() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      // Pick a random region from this tests' table to play with. Get its RegionStateNode.
      // Clone it because the original will be changed by the system. We need clone to fake out
      // a state.
      final RegionInfo region = pickArbitraryRegion(admin);
      AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
      RegionStates.RegionStateNode rsn =  am.getRegionStates().getRegionStateNode(region);
      // Now force region to be in OPENING state.
      am.markRegionAsOpening(rsn);
      // Now the 'region' is in an artificially bad state, try an unassign again.
      // Run unassign in a thread because it is blocking.
      Runnable unassign = () -> {
        try {
          admin.unassign(region.getRegionName(), true);
        } catch (IOException ioe) {
          fail("Failed assign");
        }
      };
      Thread t = new Thread(unassign, "unassign");
      t.start();
      while(!t.isAlive()) {
        Threads.sleep(100);
      }
      Threads.sleep(1000);
      // Unassign should be running and failing. Look for incrementing timeout as evidence that
      // Unassign is stuck and doing backoff.
      // Now fix the condition we were waiting on so the unassign can complete.
      JsonParser parser = new JsonParser();
      long oldTimeout = 0;
      int timeoutIncrements = 0;
      while (true) {
        long timeout = getUnassignTimeout(parser, admin.getProcedures());
        if (timeout > oldTimeout) {
          LOG.info("Timeout incremented, was {}, now is {}, increments={}",
              timeout, oldTimeout, timeoutIncrements);
          oldTimeout = timeout;
          timeoutIncrements++;
          if (timeoutIncrements > 3) {
            // If we incremented at least twice, break; the backoff is working.
            break;
          }
        }
        Thread.sleep(1000);
      }
      TEST_UTIL.getMiniHBaseCluster().stopMaster(0).join();
      HMaster master = TEST_UTIL.getMiniHBaseCluster().startMaster().getMaster();
      TEST_UTIL.waitFor(30000, () -> master.isInitialized());
      am = master.getAssignmentManager();
      rsn = am.getRegionStates().getRegionStateNode(region);
      am.markRegionAsOpened(rsn);
      t.join();
    }
  }

  /**
   * @param proceduresAsJSON This is String returned by admin.getProcedures call... an array of
   *                         Procedures as JSON.
   * @return The Procedure timeout value parsed from the Unassign Procedure.
   * @Exception Thrown if we do not find UnassignProcedure or fail to parse timeout.
   */
  private long getUnassignTimeout(JsonParser parser, String proceduresAsJSON) throws Exception {
    JsonArray array = parser.parse(proceduresAsJSON).getAsJsonArray();
    Iterator<JsonElement> iterator = array.iterator();
    while (iterator.hasNext()) {
      JsonElement element = iterator.next();
      JsonObject obj = element.getAsJsonObject();
      String className = obj.get("className").getAsString();
      String actualClassName = UnassignProcedure.class.getName();
      if (className.equals(actualClassName)) {
        return obj.get("timeout").getAsLong();
      }
    }
    throw new Exception("Failed to find UnassignProcedure or timeout in " + proceduresAsJSON);
  }
}
