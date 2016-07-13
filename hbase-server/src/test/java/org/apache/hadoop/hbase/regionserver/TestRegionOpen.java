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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, RegionServerTests.class})
public class TestRegionOpen {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(TestRegionOpen.class);
  private static final int NB_SERVERS = 1;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  final TableName tableName = TableName.valueOf(TestRegionOpen.class.getSimpleName());

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  private static HRegionServer getRS() {
    return HTU.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
  }

  @Test(timeout = 60000)
  public void testPriorityRegionIsOpenedWithSeparateThreadPool() throws Exception {
    ThreadPoolExecutor exec = getRS().getExecutorService()
        .getExecutorThreadPool(ExecutorType.RS_OPEN_PRIORITY_REGION);

    assertEquals(1, exec.getCompletedTaskCount()); // namespace region

    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setPriority(HConstants.HIGH_QOS);
    htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Admin admin = connection.getAdmin()) {
      admin.createTable(htd);
    }

    assertEquals(2, exec.getCompletedTaskCount());
  }
}
