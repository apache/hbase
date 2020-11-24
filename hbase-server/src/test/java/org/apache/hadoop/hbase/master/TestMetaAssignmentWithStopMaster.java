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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class })
public class TestMetaAssignmentWithStopMaster {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMetaAssignmentWithStopMaster.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final long WAIT_TIMEOUT = 120000;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(2,3);
  }

  @Test
  public void testStopActiveMaster() throws Exception {
    ClusterConnection conn =
      (ClusterConnection) ConnectionFactory.createConnection(UTIL.getConfiguration());
    ServerName oldMetaServer = conn.locateRegion(HRegionInfo.FIRST_META_REGIONINFO
      .getRegionName()).getServerName();
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

    ServerName newMetaServer = conn.locateRegion(HRegionInfo.FIRST_META_REGIONINFO
      .getRegionName()).getServerName();
    assertTrue("The new meta server " + newMetaServer + " should be same with" +
      " the old meta server " + oldMetaServer, newMetaServer.equals(oldMetaServer));
  }
}
