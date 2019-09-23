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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test multiget's return rows should not more than the threshold number
 */
@Category({MediumTests.class, ClientTests.class})
public class TestMultiGetRowLimit {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiGetRowLimit.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final MetricsAssertHelper METRICS_ASSERT =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);
  private final static byte[] FAMILY = Bytes.toBytes("D");
  public static final int ROWS = 100;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // disable the debug log to avoid flooding the output
    LogManager.getLogger(AsyncRegionLocatorHelper.class).setLevel(Level.INFO);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HBASE_RS_PARALLEL_GET_ENABLED, true);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_RS_PARALLEL_GET_THREADS, 3);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_RS_MULTIGET_BATCHSIZE, ROWS / 2);
    // Only start on regionserver so that all regions are on the same server.
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiGetRowLimits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);
    List<Get> gets = new ArrayList<>(ROWS);

    for (int i = 0; i < ROWS; i++) {
      gets.add(new Get(HBaseTestingUtility.ROWS[i]));
    }

    RpcServerInterface rpcServer = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer();
    BaseSource s = rpcServer.getMetrics().getMetricsSource();
    long startingExceptions = METRICS_ASSERT.getCounter("exceptions", s);
    long startingMultiExceptions = METRICS_ASSERT
        .getCounter("exceptions.multiResponseTooLarge", s);

    Result[] results = t.get(gets);
    assertEquals(ROWS, results.length);

    assertEquals(METRICS_ASSERT.getCounter("exceptions", s), startingExceptions + 1);
    assertEquals(METRICS_ASSERT.getCounter("exceptions.multiResponseTooLarge", s),
        startingMultiExceptions + 1);
  }
}
