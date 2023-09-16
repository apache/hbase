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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

@Category(MediumTests.class)
public class TestClientTableMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientTableMetrics.class);

  private static HBaseTestingUtility UTIL;
  private static Connection CONN;
  private static MetricsConnection METRICS;
  private static final String tableName = "table_1";
  private static final TableName TABLE_1 = TableName.valueOf(tableName);
  private static final byte[] FAMILY = Bytes.toBytes("f");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_TABLE_METRICS_ENABLED_KEY, true);
    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster(2);
    UTIL.createTable(TABLE_1, FAMILY);
    UTIL.waitTableAvailable(TABLE_1);
    // Only test the sync connection mode.
    CONN = UTIL.getConnection();
    METRICS = ((ConnectionImplementation) CONN).getConnectionMetrics();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.deleteTableIfAny(TABLE_1);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetTableMetrics() throws IOException {
    Table table = CONN.getTable(TABLE_1);
    table.get(new Get(Bytes.toBytes("row1")));
    table.get(new Get(Bytes.toBytes("row2")));
    table.get(new Get(Bytes.toBytes("row3")));
    table.close();

    String metricKey =
      "rpcCallDurationMs_" + ClientService.getDescriptor().getName() + "_Get_" + tableName;
    verifyTableMetrics(metricKey, 3);
  }

  @Test
  public void testMutateTableMetrics() throws IOException {
    Table table = CONN.getTable(TABLE_1);
    // PUT
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(FAMILY, Bytes.toBytes("name"), Bytes.toBytes("tom"));
    table.put(put);
    put = new Put(Bytes.toBytes("row2"));
    put.addColumn(FAMILY, Bytes.toBytes("name"), Bytes.toBytes("jerry"));
    table.put(put);
    // DELETE
    table.delete(new Delete(Bytes.toBytes("row1")));
    table.close();

    String metricKey =
      "rpcCallDurationMs_" + ClientService.getDescriptor().getName() + "_Mutate(Put)_" + tableName;
    verifyTableMetrics(metricKey, 2);

    metricKey = "rpcCallDurationMs_" + ClientService.getDescriptor().getName() + "_Mutate(Delete)_"
      + tableName;
    verifyTableMetrics(metricKey, 1);
  }

  @Test
  public void testScanTableMetrics() throws IOException {
    Table table = CONN.getTable(TABLE_1);
    table.getScanner(new Scan());
    table.close();

    String metricKey =
      "rpcCallDurationMs_" + ClientService.getDescriptor().getName() + "_Scan_" + tableName;
    verifyTableMetrics(metricKey, 1);
  }

  @Test
  public void testMultiTableMetrics() throws IOException {
    Table table = CONN.getTable(TABLE_1);
    table.put(Arrays.asList(
      new Put(Bytes.toBytes("row1")).addColumn(FAMILY, Bytes.toBytes("name"), Bytes.toBytes("tom")),
      new Put(Bytes.toBytes("row2")).addColumn(FAMILY, Bytes.toBytes("name"),
        Bytes.toBytes("jerry"))));
    table.get(Arrays.asList(new Get(Bytes.toBytes("row1")), new Get(Bytes.toBytes("row2"))));
    table.close();

    String metricKey =
      "rpcCallDurationMs_" + ClientService.getDescriptor().getName() + "_Multi_" + tableName;
    verifyTableMetrics(metricKey, 2);
  }

  private static void verifyTableMetrics(String metricKey, int expectedVal) {
    String numOpsSuffix = "_num_ops";
    String p95Suffix = "_95th_percentile";
    String p99Suffix = "_99th_percentile";
    Timer timer = METRICS.getRpcTimers().get(metricKey);
    long numOps = timer.getCount();
    double p95 = timer.getSnapshot().get95thPercentile();
    double p99 = timer.getSnapshot().get99thPercentile();
    assertEquals("metric: " + metricKey + numOpsSuffix + " val: " + numOps, expectedVal, numOps);
    assertTrue("metric: " + metricKey + p95Suffix + " val: " + p95, p95 >= 0);
    assertTrue("metric: " + metricKey + p99Suffix + " val: " + p99, p99 >= 0);
  }
}
