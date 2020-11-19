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
package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.master.ReplicationServerManager.ONLINE_SERVER_REFRESH_INTERVAL;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncReplicationServerAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ReplicationServerManager;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint.ReplicationServerSinkPeer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationServerSink {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationServerSink.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationServerSink.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration CONF = TEST_UTIL.getConfiguration();

  private static HMaster MASTER;

  private static HReplicationServer REPLICATION_SERVER;
  private static ServerName REPLICATION_SERVER_NAME;

  private static Path baseNamespaceDir;
  private static Path hfileArchiveDir;
  private static String replicationClusterId;

  private static int BATCH_SIZE = 10;

  private static TableName TABLENAME = TableName.valueOf("t");
  private static String FAMILY = "C";

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONF.setLong(HBASE_CLIENT_OPERATION_TIMEOUT, 1000);
    CONF.setLong(ONLINE_SERVER_REFRESH_INTERVAL, 10000);
    CONF.setBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY, true);
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numReplicationServers(1).build());
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    REPLICATION_SERVER = TEST_UTIL.getMiniHBaseCluster().getReplicationServerThreads().get(0)
        .getReplicationServer();
    REPLICATION_SERVER_NAME = REPLICATION_SERVER.getServerName();

    Path rootDir = CommonFSUtils.getRootDir(CONF);
    baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
    hfileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY));
    replicationClusterId = "12345";
  }

  @AfterClass
  public static void afterClass() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME);
  }

  @After
  public void after() throws IOException {
    TEST_UTIL.deleteTableIfAny(TABLENAME);
  }

  /**
   * Requests replication server using {@link AsyncReplicationServerAdmin}
   */
  @Test
  public void testReplicateWAL() throws Exception {
    AsyncClusterConnection conn =
        TEST_UTIL.getHBaseCluster().getMaster().getAsyncClusterConnection();
    AsyncReplicationServerAdmin replAdmin =
        conn.getReplicationServerAdmin(REPLICATION_SERVER_NAME);

    ReplicationServerSinkPeer sinkPeer =
        new ReplicationServerSinkPeer(REPLICATION_SERVER_NAME, replAdmin);
    replicateWALEntryAndVerify(sinkPeer);
  }

  private void replicateWALEntryAndVerify(ReplicationServerSinkPeer sinkPeer) throws Exception {
    Entry[] entries = new Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] = generateEdit(i, TABLENAME, Bytes.toBytes(i));
    }

    sinkPeer.replicateWALEntry(entries, replicationClusterId, baseNamespaceDir, hfileArchiveDir,
        1000);

    Table table = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = 0; i < BATCH_SIZE; i++) {
      Result result = table.get(new Get(Bytes.toBytes(i)));
      Cell cell = result.getColumnLatestCell(Bytes.toBytes(FAMILY), Bytes.toBytes(FAMILY));
      assertNotNull(cell);
      assertTrue(Bytes.equals(CellUtil.cloneValue(cell), Bytes.toBytes(i)));
    }
  }

  private static WAL.Entry generateEdit(int i, TableName tableName, byte[] row) {
    Threads.sleep(1);
    long timestamp = System.currentTimeMillis();
    WALKeyImpl key = new WALKeyImpl(new byte[32], tableName, i, timestamp,
        HConstants.DEFAULT_CLUSTER_ID, null);
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(row, Bytes.toBytes(FAMILY), Bytes.toBytes(FAMILY), timestamp, row));
    return new WAL.Entry(key, edit);
  }

  @Test
  public void testReplicationServerReport() throws Exception {
    ReplicationServerManager replicationServerManager = MASTER.getReplicationServerManager();
    assertNotNull(replicationServerManager);
    TEST_UTIL.waitFor(60000, () -> !replicationServerManager.getOnlineServers().isEmpty()
        && null != replicationServerManager.getServerMetrics(REPLICATION_SERVER_NAME));
    // put data via replication server
    testReplicateWAL();
    TEST_UTIL.waitFor(60000, () -> REPLICATION_SERVER.rpcServices.requestCount.sum() > 0
        && REPLICATION_SERVER.rpcServices.requestCount.sum() == replicationServerManager
        .getServerMetrics(REPLICATION_SERVER_NAME).getRequestCount());
  }

  @Test
  public void testReplicationServerExpire() throws Exception {
    int initialNum = TEST_UTIL.getMiniHBaseCluster().getNumLiveReplicationServers();
    HReplicationServer replicationServer = new HReplicationServer(CONF);
    replicationServer.start();
    ServerName replicationServerName = replicationServer.getServerName();

    ReplicationServerManager replicationServerManager = MASTER.getReplicationServerManager();
    TEST_UTIL.waitFor(60000, () ->
        initialNum + 1 == replicationServerManager.getOnlineServers().size()
        && null != replicationServerManager.getServerMetrics(replicationServerName));

    replicationServer.stop("test");

    TEST_UTIL.waitFor(180000, 1000, () ->
        initialNum == replicationServerManager.getOnlineServers().size());
    assertNull(replicationServerManager.getServerMetrics(replicationServerName));
  }
}
