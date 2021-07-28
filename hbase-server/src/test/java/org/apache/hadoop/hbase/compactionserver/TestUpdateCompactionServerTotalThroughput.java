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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.throttle.PressureAwareCompactionThroughputController;
import org.apache.hadoop.hbase.testclassification.CompactionServerTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({ MediumTests.class, CompactionServerTests.class })
public class TestUpdateCompactionServerTotalThroughput {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUpdateCompactionServerTotalThroughput.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf = HBaseConfiguration.create();
  private static Connection connection;
  private static Admin admin;
  private static int COMPACTION_SERVER_NUM = 2;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(
      StartMiniClusterOption.builder().numCompactionServers(COMPACTION_SERVER_NUM).build());
    admin = TEST_UTIL.getAdmin();
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  private void checkThroughPut(long upperBound, long lowBound, long offPeak) {
    TEST_UTIL.getHBaseCluster().getCompactionServerThreads().forEach(cs -> {
      PressureAwareCompactionThroughputController throughputController =
          (PressureAwareCompactionThroughputController) cs
              .getCompactionServer().compactionThreadManager.getCompactionThroughputController();
      TEST_UTIL.waitFor(60000,
        () -> throughputController.getMaxThroughputUpperBound() == upperBound
            && throughputController.getMaxThroughputLowerBound() == lowBound
            && throughputController.getMaxThroughputOffPeak() == offPeak);
    });
  }

  private void checkUpdateThroughPutResult(Map<String, Long> result, long upperBound, long lowBound,
      long offPeak) {
    Assert.assertEquals(upperBound, result.get("UpperBound").longValue());
    Assert.assertEquals(lowBound, result.get("LowerBound").longValue());
    Assert.assertEquals(offPeak, result.get("OffPeak").longValue());
  }

  @Test
  public void testUpdateCompactionServerTotalThroughput() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    Map<String, Long> result = admin.updateCompactionServerTotalThroughput(200L, 100L, 400L);
    checkThroughPut(200 / COMPACTION_SERVER_NUM, 100 / COMPACTION_SERVER_NUM,
      400 / COMPACTION_SERVER_NUM);
    checkUpdateThroughPutResult(result, 200, 100, 400);

    result = admin.updateCompactionServerTotalThroughput(0L, 0L, 0L);
    // set TotalThroughput to 0 will not take effect
    checkUpdateThroughPutResult(result, 200, 100, 400);

    result = admin.updateCompactionServerTotalThroughput(-100L, -100L, -100L);
    // set TotalThroughput to negative will not take effect
    checkUpdateThroughPutResult(result, 200, 100, 400);

    result = admin.updateCompactionServerTotalThroughput(2000L, 500L, 10000L);
    checkThroughPut(2000 / COMPACTION_SERVER_NUM, 500 / COMPACTION_SERVER_NUM,
      10000 / COMPACTION_SERVER_NUM);
    checkUpdateThroughPutResult(result, 2000, 500, 10000);
  }
}
