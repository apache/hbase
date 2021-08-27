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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.master.ReplicationServerManager.REPLICATION_SERVER_REFRESH_PERIOD;
import static org.apache.hadoop.hbase.replication.ReplicationServerRpcServices.REPLICATION_SERVER_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.HReplicationServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationServerManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationServerManager.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration CONF = TEST_UTIL.getConfiguration();

  private static HMaster MASTER;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONF.setInt(REPLICATION_SERVER_PORT, 0);
    CONF.setLong(HBASE_CLIENT_OPERATION_TIMEOUT, 1000);
    CONF.setLong(REPLICATION_SERVER_REFRESH_PERIOD, 10000);
    CONF.setBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY, true);
    TEST_UTIL.startMiniCluster(1);
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReplicationServerReport() throws Exception {
    ReplicationServerManager replicationServerManager = MASTER.getReplicationServerManager();
    assertNotNull(replicationServerManager);

    HReplicationServer replicationServer = new HReplicationServer(CONF);
    replicationServer.start();
    ServerName replicationServerName = replicationServer.getServerName();

    TEST_UTIL.waitFor(60000, () -> !replicationServerManager.getOnlineServersList().isEmpty()
      && null != replicationServerManager.getServerMetrics(replicationServerName));

    for (int i = 0; i < 10; i++) {
      replicationServer.getReplicationServerRpcServices().requestCount.add(i);
      TEST_UTIL.waitFor(60000, () ->
        replicationServer.getReplicationServerRpcServices().requestCount.sum()
          == replicationServerManager.getServerMetrics(replicationServerName).getRequestCount());
    }

    replicationServer.stop("test");
  }

  @Test
  public void testReplicationServerExpire() throws Exception {
    int initialNum = TEST_UTIL.getMiniHBaseCluster().getNumLiveReplicationServers();
    HReplicationServer replicationServer = new HReplicationServer(CONF);
    replicationServer.start();
    ServerName replicationServerName = replicationServer.getServerName();

    ReplicationServerManager replicationServerManager = MASTER.getReplicationServerManager();
    TEST_UTIL.waitFor(60000, () ->
      initialNum + 1 == replicationServerManager.getOnlineServersList().size()
        && null != replicationServerManager.getServerMetrics(replicationServerName));

    replicationServer.stop("test");

    TEST_UTIL.waitFor(180000, 1000, () ->
      initialNum == replicationServerManager.getOnlineServersList().size());
    assertNull(replicationServerManager.getServerMetrics(replicationServerName));
  }
}
