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

import static org.junit.Assert.assertNotNull;
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncReplicationServerAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.ReplicationServerSinkPeer;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.SinkPeer;
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
public class TestReplicationServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationServer.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration CONF = TEST_UTIL.getConfiguration();

  private static HMaster MASTER;

  private static HReplicationServer replicationServer;

  private static Path baseNamespaceDir;
  private static Path hfileArchiveDir;
  private static String replicationClusterId;

  private static int BATCH_SIZE = 10;

  private static TableName TABLENAME = TableName.valueOf("t");
  private static String FAMILY = "C";

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();

    replicationServer = new HReplicationServer(CONF);
    replicationServer.start();

    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitFor(60000, () -> replicationServer.isOnline());

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
        conn.getReplicationServerAdmin(replicationServer.getServerName());

    ReplicationServerSinkPeer sinkPeer =
        new ReplicationServerSinkPeer(replicationServer.getServerName(), replAdmin);
    replicateWALEntryAndVerify(sinkPeer);
  }

  /**
   * Requests region server using {@link AsyncReplicationServerAdmin}
   */
  @Test
  public void testReplicateWAL2() throws Exception {
    AsyncClusterConnection conn =
        TEST_UTIL.getHBaseCluster().getMaster().getAsyncClusterConnection();
    ServerName rs = TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().get(0)
        .getRegionServer().getServerName();
    AsyncReplicationServerAdmin replAdmin = conn.getReplicationServerAdmin(rs);

    ReplicationServerSinkPeer sinkPeer = new ReplicationServerSinkPeer(rs, replAdmin);
    replicateWALEntryAndVerify(sinkPeer);
  }

  private void replicateWALEntryAndVerify(SinkPeer sinkPeer) throws Exception {
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
}
