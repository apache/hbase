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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MediumTests.class)
public class TestAssignmentManagerMetrics {

  private static final Log LOG = LogFactory.getLog(TestAssignmentManagerMetrics.class);
  private static final MetricsAssertHelper metricsHelper = CompatibilityFactory
      .getInstance(MetricsAssertHelper.class);

  private static MiniHBaseCluster cluster;
  private static HMaster master;
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf;
  private static final int msgInterval = 1000;

  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();

    // Disable sanity check for coprocessor
    conf.setBoolean("hbase.table.sanity.checks", false);

    // set RIT stuck warning threshold to a small value
    conf.setInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 20);

    // set msgInterval to 1 second
    conf.setInt("hbase.regionserver.msginterval", msgInterval);

    // set tablesOnMaster to none
    conf.set("hbase.balancer.tablesOnMaster", "none");

    TEST_UTIL.startMiniCluster(1);
    cluster = TEST_UTIL.getHBaseCluster();
    master = cluster.getMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testRITAssignmentManagerMetrics() throws Exception {

    final TableName TABLENAME = TableName.valueOf("testRITMetrics");
    final byte[] FAMILY = Bytes.toBytes("family");

    Table table = null;
    try {
      table = TEST_UTIL.createTable(TABLENAME, FAMILY);

      final byte[] row = Bytes.toBytes("row");
      final byte[] qualifier = Bytes.toBytes("qualifier");
      final byte[] value = Bytes.toBytes("value");

      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, value);
      table.put(put);

      // Sleep 3 seconds, wait for doMetrics chore catching up
      Thread.sleep(msgInterval * 3);

      // check the RIT is 0
      MetricsAssignmentManagerSource amSource =
          master.getAssignmentManager().getAssignmentManagerMetrics().getMetricsProcSource();

      metricsHelper.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_NAME, 0, amSource);
      metricsHelper.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_OVER_THRESHOLD_NAME, 0,
          amSource);

      // alter table with a non-existing coprocessor
      HTableDescriptor htd = new HTableDescriptor(TABLENAME);
      HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

      htd.addFamily(hcd);

      String spec = "hdfs:///foo.jar|com.foo.FooRegionObserver|1001|arg1=1,arg2=2";
      htd.addCoprocessorWithSpec(spec);

      TEST_UTIL.getHBaseAdmin().modifyTable(TABLENAME, htd);

      // Sleep 3 seconds, wait for doMetrics chore catching up
      Thread.sleep(msgInterval * 3);
      metricsHelper.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_NAME, 1, amSource);
      metricsHelper.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_OVER_THRESHOLD_NAME, 1,
          amSource);

    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
}
