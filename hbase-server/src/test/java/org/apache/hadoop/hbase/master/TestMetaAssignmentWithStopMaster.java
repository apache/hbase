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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class })
public class TestMetaAssignmentWithStopMaster {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMetaAssignmentWithStopMaster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaAssignmentWithStopMaster.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final long WAIT_TIMEOUT = 120000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(2).numRegionServers(3).numDataNodes(3).build();
    UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStopActiveMaster() throws Exception {
    ClusterConnection conn =
        (ClusterConnection) ConnectionFactory.createConnection(UTIL.getConfiguration());
    ServerName oldMetaServer = conn.locateRegions(TableName.META_TABLE_NAME).get(0).getServerName();
    ServerName oldMaster = UTIL.getMiniHBaseCluster().getMaster().getServerName();

    UTIL.getMiniHBaseCluster().getMaster().stop("Stop master for test");
    long startTime = System.currentTimeMillis();
    while (UTIL.getMiniHBaseCluster().getMaster() == null || UTIL.getMiniHBaseCluster().getMaster()
        .getServerName().equals(oldMaster)) {
      LOG.info("Wait the standby master become active");
      Thread.sleep(3000);
      if (System.currentTimeMillis() - startTime > WAIT_TIMEOUT) {
        fail("Wait too long for standby master become active");
      }
    }
    startTime = System.currentTimeMillis();
    while (!UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
      LOG.info("Wait the new active master to be initialized");
      Thread.sleep(3000);
      if (System.currentTimeMillis() - startTime > WAIT_TIMEOUT) {
        fail("Wait too long for the new active master to be initialized");
      }
    }

    ServerName newMetaServer = conn.locateRegions(TableName.META_TABLE_NAME).get(0).getServerName();
    assertTrue("The new meta server " + newMetaServer + " should be same with" +
        " the old meta server " + oldMetaServer, newMetaServer.equals(oldMetaServer));
  }
}
