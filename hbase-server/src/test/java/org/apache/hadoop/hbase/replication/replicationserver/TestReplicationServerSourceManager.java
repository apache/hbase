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
package org.apache.hadoop.hbase.replication.replicationserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.HReplicationServer;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, MediumTests.class})
public class TestReplicationServerSourceManager extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationServerSourceManager.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReplicationServerSourceManager.class);

  @Rule
  public TestName name = new TestName();

  private HReplicationServer replicationServer;

  private ReplicationServerSourceManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF1.setBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY, true);
    TestReplicationBase.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
    replicationServer = new HReplicationServer(UTIL1.getConfiguration());
    replicationServer.start();
    replicationServer.waitForServerOnline();
    manager = replicationServer.getReplicationServerSourceManager();
    super.setUpBase();
  }

  @After
  public void tearDown() throws Exception {
    replicationServer.stop("test");
    super.tearDownBase();
  }

  @Test
  public void testAddSource() throws Exception {
    ServerName rs =
      UTIL1.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer().getServerName();
    manager.startReplicationSource(rs, PEER_ID2);
    loadData("aaa", row);
    UTIL2.waitFor(30000, () -> htable2.getScanner(new Scan()).next() != null);
  }

  @Test
  public void testRemovePeerMetricsCleanup() throws Exception {
    HRegionServer rs =
      UTIL1.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
    ReplicationSourceManager rsSourceManager =
      rs.getReplicationSourceService().getReplicationManager();
    int rsSourceSizeInitial = rsSourceManager.getSources().size();
    String peerId = name.getMethodName();
    ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(rs.getServerName(), peerId);

    addPeer(peerId, tableName);
    UTIL1.waitFor(30000,
      () -> rsSourceSizeInitial + 1 == rsSourceManager.getSources().size());

    MetricsReplicationGlobalSourceSource globalMetrics = manager.getGlobalMetrics();
    int globalLogQueueSizeInitial = globalMetrics.getSizeOfLogQueue();
    LOG.debug("globalLogQueueSize: {} before starting source on replication server",
      globalLogQueueSizeInitial);
    // Start source on replication server
    manager.startReplicationSource(rs.getServerName(), peerId);
    ReplicationSourceInterface src = manager.getSource(queueInfo);
    assertNotNull(src);
    assertEquals(globalLogQueueSizeInitial + src.getSourceMetrics().getSizeOfLogQueue(),
      globalMetrics.getSizeOfLogQueue());

    // Stopping the peer should reset the global metrics
    manager.stopReplicationSource(rs.getServerName(), peerId);
    UTIL1.waitFor(30000, () -> manager.getSources().size() == 0);
    assertEquals(globalLogQueueSizeInitial, globalMetrics.getSizeOfLogQueue());

    // Adding the same source back again should reset the single source metrics
    manager.startReplicationSource(rs.getServerName(), peerId);
    src = manager.getSource(queueInfo);
    assertNotNull(src);
    assertEquals(globalLogQueueSizeInitial + src.getSourceMetrics().getSizeOfLogQueue(),
      globalMetrics.getSizeOfLogQueue());

    removePeer(peerId);
  }
}