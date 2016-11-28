/**
 *
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterBalanceThrottling {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Before
  public void setupConfiguration() {
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        "org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer");
  }

  @After
  public void shutdown() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_MAX_BALANCING,
      HConstants.DEFAULT_HBASE_BALANCER_PERIOD);
    TEST_UTIL.getConfiguration().setDouble(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT,
      HConstants.DEFAULT_HBASE_MASTER_BALANCER_MAX_RIT_PERCENT);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 60000)
  public void testThrottlingByBalanceInterval() throws Exception {
    // Use default config and start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);

    TableName tableName = createTable("testNoThrottling");
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    // Default max balancing time is 300000 ms and there are 50 regions to balance
    // The balance interval is 6000 ms, much longger than the normal region in transition duration
    // So the master can balance the region one by one
    unbalance(master, tableName);
    AtomicInteger maxCount = new AtomicInteger(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread checker = startBalancerChecker(master, maxCount, stop);
    master.balance();
    stop.set(true);
    checker.interrupt();
    checker.join();
    assertTrue("max regions in transition: " + maxCount.get(), maxCount.get() == 1);

    TEST_UTIL.deleteTable(tableName);
  }

  @Test(timeout = 60000)
  public void testThrottlingByMaxRitPercent() throws Exception {
    // Set max balancing time to 500 ms and max percent of regions in transition to 0.05
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_MAX_BALANCING, 500);
    TEST_UTIL.getConfiguration().setDouble(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT, 0.05);
    TEST_UTIL.startMiniCluster(2);

    TableName tableName = createTable("testThrottlingByMaxRitPercent");
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    unbalance(master, tableName);
    AtomicInteger maxCount = new AtomicInteger(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread checker = startBalancerChecker(master, maxCount, stop);
    master.balance();
    stop.set(true);
    checker.interrupt();
    checker.join();
    // The max number of regions in transition is 100 * 0.05 = 5
    assertTrue("max regions in transition: " + maxCount.get(), maxCount.get() == 5);

    TEST_UTIL.deleteTable(tableName);
  }

  private TableName createTable(String table) throws IOException {
    TableName tableName = TableName.valueOf(table);
    byte[] startKey = new byte[] { 0x00 };
    byte[] stopKey = new byte[] { 0x7f };
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILYNAME }, 1, startKey, stopKey,
      100);
    return tableName;
  }

  private Thread startBalancerChecker(final HMaster master, final AtomicInteger maxCount,
      final AtomicBoolean stop) {
    Runnable checker = new Runnable() {
      @Override
      public void run() {
        while (!stop.get()) {
          maxCount.set(Math.max(maxCount.get(), master.getAssignmentManager().getRegionStates()
              .getRegionsInTransitionCount()));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
    Thread thread = new Thread(checker);
    thread.start();
    return thread;
  }

  private void unbalance(HMaster master, TableName tableName) throws Exception {
    while (master.getAssignmentManager().getRegionStates().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
    HRegionServer biasedServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    for (HRegionInfo regionInfo : TEST_UTIL.getAdmin().getTableRegions(tableName)) {
      master.move(regionInfo.getEncodedNameAsBytes(),
        Bytes.toBytes(biasedServer.getServerName().getServerName()));
    }
    while (master.getAssignmentManager().getRegionStates().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
  }
}
