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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
public class TestFullLogReconstruction {
  private static final Logger LOG = LoggerFactory.getLogger(TestFullLogReconstruction.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFullLogReconstruction.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static TableName TABLE_NAME = TableName.valueOf("tabletest");
  private final static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    // quicker heartbeat interval for faster DN death notification
    c.setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    c.setInt("dfs.heartbeat.interval", 1);
    c.setInt("dfs.client.socket-timeout", 5000);
    // faster failover with cluster.shutdown();fs.close() idiom
    c.setInt("hbase.ipc.client.connect.max.retries", 1);
    c.setInt("dfs.client.block.recovery.retries", 1);
    c.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test the whole reconstruction loop. Build a table with regions aaa to zzz and load every one of
   * them multiple times with the same date and do a flush at some point. Kill one of the region
   * servers and scan the table. We should see all the rows.
   */
  @Test
  public void testReconstruction() throws Exception {
    Table table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);

    // Load up the table with simple rows and count them
    int initialCount = TEST_UTIL.loadTable(table, FAMILY);
    int count = TEST_UTIL.countRows(table);

    assertEquals(initialCount, count);

    for (int i = 0; i < 4; i++) {
      TEST_UTIL.loadTable(table, FAMILY);
    }
    RegionServerThread rsThread = TEST_UTIL.getHBaseCluster().getRegionServerThreads().get(0);
    int index = 0;
    LOG.info("Expiring {}", TEST_UTIL.getMiniHBaseCluster().getRegionServer(index));
    TEST_UTIL.expireRegionServerSession(index);
    // make sure that the RS is fully down before reading, so that we will read the data from other
    // RSes.
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !rsThread.isAlive();
      }

      @Override
      public String explainFailure() throws Exception {
        return rsThread.getRegionServer() + " is still alive";
      }
    });
    LOG.info("Starting count");

    int newCount = TEST_UTIL.countRows(table);
    assertEquals(count, newCount);
    table.close();
  }
}
