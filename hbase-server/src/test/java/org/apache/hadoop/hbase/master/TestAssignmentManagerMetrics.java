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

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestAssignmentManagerMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentManagerMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentManagerMetrics.class);
  private static final MetricsAssertHelper METRICS_HELPER = CompatibilityFactory
      .getInstance(MetricsAssertHelper.class);

  private static MiniHBaseCluster CLUSTER;
  private static HMaster MASTER;
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int MSG_INTERVAL = 1000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    Configuration conf = TEST_UTIL.getConfiguration();

    // Disable sanity check for coprocessor
    conf.setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);

    // set RIT stuck warning threshold to a small value
    conf.setInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 20);

    // set msgInterval to 1 second
    conf.setInt("hbase.regionserver.msginterval", MSG_INTERVAL);

    // set tablesOnMaster to none
    conf.set("hbase.balancer.tablesOnMaster", "none");

    // set client sync wait timeout to 5sec
    conf.setInt("hbase.client.sync.wait.timeout.msec", 5000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 2500);
    // set a small interval for updating rit metrics
    conf.setInt(AssignmentManager.RIT_CHORE_INTERVAL_MSEC_CONF_KEY, MSG_INTERVAL);
    // set a small assign attempts for avoiding assert when retrying. (HBASE-20533)
    conf.setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);

    // keep rs online so it can report the failed opens.
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.startMiniCluster(1);
    CLUSTER = TEST_UTIL.getHBaseCluster();
    MASTER = CLUSTER.getMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    LOG.info("AFTER {} <= IS THIS NULL?", TEST_UTIL);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRITAssignmentManagerMetrics() throws Exception {
    final TableName TABLENAME = TableName.valueOf(name.getMethodName());
    final byte[] FAMILY = Bytes.toBytes("family");
    try (Table table = TEST_UTIL.createTable(TABLENAME, FAMILY)){
      final byte[] row = Bytes.toBytes("row");
      final byte[] qualifier = Bytes.toBytes("qualifier");
      final byte[] value = Bytes.toBytes("value");

      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, value);
      table.put(put);

      // Sleep 3 seconds, wait for doMetrics chore catching up
      Thread.sleep(MSG_INTERVAL * 3);

      // check the RIT is 0
      MetricsAssignmentManagerSource amSource =
          MASTER.getAssignmentManager().getAssignmentManagerMetrics().getMetricsProcSource();

      METRICS_HELPER.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_NAME, 0, amSource);
      METRICS_HELPER.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_OVER_THRESHOLD_NAME, 0,
          amSource);

      // alter table with a non-existing coprocessor

      TableDescriptor htd = TableDescriptorBuilder.newBuilder(TABLENAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setCoprocessor(CoprocessorDescriptorBuilder.newBuilder("com.foo.FooRegionObserver")
          .setJarPath("hdfs:///foo.jar")
          .setPriority(1001)
          .setProperty("arg1", "1")
          .setProperty("arg2", "2")
          .build())
        .build();
      try {
        TEST_UTIL.getAdmin().modifyTable(htd);
        fail("Expected region failed to open");
      } catch (IOException e) {
        // expected, the RS will crash and the assignment will spin forever waiting for a RS
        // to assign the region. the region will not go to FAILED_OPEN because in this case
        // we have just one RS and it will do one retry.
        LOG.info("Expected error", e);
      }

      // Sleep 5 seconds, wait for doMetrics chore catching up
      // the rit count consists of rit and failed opens. see RegionInTransitionStat#update
      // Waiting for the completion of rit makes the assert stable.
      TEST_UTIL.waitUntilNoRegionsInTransition();
      Thread.sleep(MSG_INTERVAL * 5);
      METRICS_HELPER.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_NAME, 1, amSource);
      METRICS_HELPER.assertGauge(MetricsAssignmentManagerSource.RIT_COUNT_OVER_THRESHOLD_NAME, 1,
        amSource);
      METRICS_HELPER.assertCounter(MetricsAssignmentManagerSource.ASSIGN_METRIC_PREFIX
        + "SubmittedCount", 3, amSource);
    }
  }
}
