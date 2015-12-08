/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * This test sets the multi size WAAAAAY low and then checks to make sure that gets will still make
 * progress.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestMultiRespectsLimits {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final MetricsAssertHelper METRICS_ASSERT =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);
  private final static byte[] FAMILY = Bytes.toBytes("D");
  public static final int MAX_SIZE = 500;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setLong(
        HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
        MAX_SIZE);

    // Only start on regionserver so that all regions are on the same server.
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiLimits() throws Exception {
    final TableName name = TableName.valueOf("testMultiLimits");
    Table t = TEST_UTIL.createTable(name, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    // Split the table to make sure that the chunking happens accross regions.
    try (final Admin admin = TEST_UTIL.getHBaseAdmin()) {
      admin.split(name);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return admin.getTableRegions(name).size() > 1;
        }
      });
    }
    List<Get> gets = new ArrayList<>(MAX_SIZE);

    for (int i = 0; i < MAX_SIZE; i++) {
      gets.add(new Get(HBaseTestingUtility.ROWS[i]));
    }
    Result[] results = t.get(gets);
    assertEquals(MAX_SIZE, results.length);
    RpcServerInterface rpcServer = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer();
    BaseSource s = rpcServer.getMetrics().getMetricsSource();

    // Cells from TEST_UTIL.loadTable have a length of 27.
    // Multiplying by less than that gives an easy lower bound on size.
    // However in reality each kv is being reported as much higher than that.
    METRICS_ASSERT.assertCounterGt("exceptions", (MAX_SIZE * 25) / MAX_SIZE, s);
    METRICS_ASSERT.assertCounterGt("exceptions.multiResponseTooLarge",
        (MAX_SIZE * 25) / MAX_SIZE, s);
  }
}
