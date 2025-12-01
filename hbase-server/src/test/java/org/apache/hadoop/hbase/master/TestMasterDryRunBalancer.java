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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterDryRunBalancer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterDryRunBalancer.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @After
  public void shutdown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDryRunBalancer() throws Exception {
    TEST_UTIL.startMiniCluster(2);

    int numRegions = 100;
    int regionsPerRs = numRegions / 2;
    TableName tableName = createTable("testDryRunBalancer", numRegions);
    HMaster master = Mockito.spy(TEST_UTIL.getHBaseCluster().getMaster());

    // dry run should be possible with balancer disabled
    // disabling it will ensure the chore does not mess with our forced unbalance below
    master.balanceSwitch(false);
    assertFalse(master.isBalancerOn());

    HRegionServer biasedServer = unbalance(master, tableName);

    BalanceResponse response = master.balance(BalanceRequest.newBuilder().setDryRun(true).build());
    assertTrue(response.isBalancerRan());
    // we don't know for sure that it will be exactly half the regions
    assertTrue(response.getMovesCalculated() >= (regionsPerRs - 1)
      && response.getMovesCalculated() <= (regionsPerRs + 1));
    // but we expect no moves executed due to dry run
    assertEquals(0, response.getMovesExecuted());

    // sanity check that we truly don't try to execute any plans
    Mockito.verify(master, Mockito.never()).executeRegionPlansWithThrottling(Mockito.anyList());

    // should still be unbalanced post dry run
    assertServerContainsAllRegions(biasedServer.getServerName(), tableName);

    TEST_UTIL.deleteTable(tableName);
  }

  private TableName createTable(String table, int numRegions) throws IOException {
    TableName tableName = TableName.valueOf(table);
    TEST_UTIL.createMultiRegionTable(tableName, FAMILYNAME, numRegions);
    return tableName;
  }

  private HRegionServer unbalance(HMaster master, TableName tableName) throws Exception {
    waitForRegionsToSettle(master);

    HRegionServer biasedServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

    for (RegionInfo regionInfo : TEST_UTIL.getAdmin().getRegions(tableName)) {
      master.move(regionInfo.getEncodedNameAsBytes(),
        Bytes.toBytes(biasedServer.getServerName().getServerName()));
    }

    waitForRegionsToSettle(master);

    assertServerContainsAllRegions(biasedServer.getServerName(), tableName);

    return biasedServer;
  }

  private void assertServerContainsAllRegions(ServerName serverName, TableName tableName)
    throws IOException {
    int numRegions = TEST_UTIL.getAdmin().getRegions(tableName).size();
    assertEquals(numRegions,
      TEST_UTIL.getMiniHBaseCluster().getRegionServer(serverName).getRegions(tableName).size());
  }

  private void waitForRegionsToSettle(HMaster master) {
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 60_000,
      () -> master.getAssignmentManager().getRegionsInTransitionCount() <= 0);
  }
}
