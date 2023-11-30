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
package org.apache.hadoop.hbase.master.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager.ReplicationQueueStorageInitializer;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.TableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.TestZKReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Category({ MasterTests.class, MediumTests.class })
public class TestReplicationPeerManagerMigrateQueuesFromZk {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationPeerManagerMigrateQueuesFromZk.class);

  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static ExecutorService EXECUTOR;

  ConcurrentMap<String, ReplicationPeerDescription> peers;

  private ReplicationPeerStorage peerStorage;

  private ReplicationQueueStorage queueStorage;

  private ReplicationQueueStorageInitializer queueStorageInitializer;

  private ReplicationPeerManager manager;

  private int nServers = 10;

  private int nPeers = 10;

  private int nRegions = 100;

  private ServerName deadServerName;

  @Rule
  public final TableNameTestRule tableNameRule = new TableNameTestRule();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    EXECUTOR = Executors.newFixedThreadPool(3,
      new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(TestReplicationPeerManagerMigrateQueuesFromZk.class.getSimpleName() + "-%d")
        .build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EXECUTOR.shutdownNow();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    peerStorage = mock(ReplicationPeerStorage.class);
    TableName tableName = tableNameRule.getTableName();
    UTIL.getAdmin()
      .createTable(ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName));
    queueStorage = new TableReplicationQueueStorage(UTIL.getConnection(), tableName);
    queueStorageInitializer = mock(ReplicationQueueStorageInitializer.class);
    peers = new ConcurrentHashMap<>();
    deadServerName =
      ServerName.valueOf("test-hbase-dead", 12345, EnvironmentEdgeManager.currentTime());
    manager = new ReplicationPeerManager(UTIL.getTestFileSystem(), UTIL.getZooKeeperWatcher(),
      peerStorage, queueStorage, peers, conf, "cluster", queueStorageInitializer);
  }

  private Map<String, Set<String>> prepareData() throws Exception {
    ZKReplicationQueueStorageForMigration storage = new ZKReplicationQueueStorageForMigration(
      UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    TestZKReplicationQueueStorage.mockQueuesData(storage, 10, "peer_0", deadServerName);
    Map<String, Set<String>> encodedName2PeerIds = TestZKReplicationQueueStorage
      .mockLastPushedSeqIds(storage, "peer_1", "peer_2", nRegions, 10, 10);
    TestZKReplicationQueueStorage.mockHFileRefs(storage, 10);
    return encodedName2PeerIds;
  }

  @Test
  public void testNoPeers() throws Exception {
    prepareData();
    manager.migrateQueuesFromZk(UTIL.getZooKeeperWatcher(), EXECUTOR).get(1, TimeUnit.MINUTES);
    // should have called initializer
    verify(queueStorageInitializer).initialize();
    // should have not migrated any data since there is no peer
    try (Table table = UTIL.getConnection().getTable(tableNameRule.getTableName())) {
      assertEquals(0, HBaseTestingUtil.countRows(table));
    }
  }

  @Test
  public void testMigrate() throws Exception {
    Map<String, Set<String>> encodedName2PeerIds = prepareData();
    // add all peers so we will migrate them all
    for (int i = 0; i < nPeers; i++) {
      // value is not used in this test, so just add a mock
      peers.put("peer_" + i, mock(ReplicationPeerDescription.class));
    }
    manager.migrateQueuesFromZk(UTIL.getZooKeeperWatcher(), EXECUTOR).get(1, TimeUnit.MINUTES);
    // should have called initializer
    verify(queueStorageInitializer).initialize();
    List<ReplicationQueueData> queueDatas = queueStorage.listAllQueues();
    // there should be two empty queues so minus 2
    assertEquals(2 * nServers - 2, queueDatas.size());
    for (ReplicationQueueData queueData : queueDatas) {
      assertEquals("peer_0", queueData.getId().getPeerId());
      assertEquals(1, queueData.getOffsets().size());
      String walGroup = queueData.getId().getServerWALsBelongTo().toString();
      ReplicationGroupOffset offset = queueData.getOffsets().get(walGroup);
      assertEquals(0, offset.getOffset());
      assertEquals(queueData.getId().getServerWALsBelongTo().toString() + ".0", offset.getWal());
    }
    // there is no method in ReplicationQueueStorage can list all the last pushed sequence ids
    try (Table table = UTIL.getConnection().getTable(tableNameRule.getTableName());
      ResultScanner scanner =
        table.getScanner(TableReplicationQueueStorage.LAST_SEQUENCE_ID_FAMILY)) {
      for (int i = 0; i < 2; i++) {
        Result result = scanner.next();
        String peerId = Bytes.toString(result.getRow());
        assertEquals(nRegions, result.size());
        for (Cell cell : result.rawCells()) {
          String encodedRegionName = Bytes.toString(cell.getQualifierArray(),
            cell.getQualifierOffset(), cell.getQualifierLength());
          encodedName2PeerIds.get(encodedRegionName).remove(peerId);
          long seqId =
            Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          assertEquals(i + 1, seqId);
        }
      }
      encodedName2PeerIds.forEach((encodedRegionName, peerIds) -> {
        assertThat(encodedRegionName + " still has unmigrated peers", peerIds, empty());
      });
      assertNull(scanner.next());
    }
    for (int i = 0; i < nPeers; i++) {
      List<String> refs = queueStorage.getReplicableHFiles("peer_" + i);
      assertEquals(i, refs.size());
      Set<String> refsSet = new HashSet<>(refs);
      for (int j = 0; j < i; j++) {
        assertTrue(refsSet.remove("hfile-" + j));
      }
      assertThat(refsSet, empty());
    }
  }
}
