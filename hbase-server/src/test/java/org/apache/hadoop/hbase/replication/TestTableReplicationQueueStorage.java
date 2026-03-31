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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
public class TestTableReplicationQueueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(TestTableReplicationQueueStorage.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private TableReplicationQueueStorage storage;

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUpBeforeTest(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    TableDescriptor td = ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName);
    UTIL.getAdmin().createTable(td);
    UTIL.waitTableAvailable(tableName);
    storage = new TableReplicationQueueStorage(UTIL.getConnection(), tableName);
  }

  private ServerName getServerName(int i) {
    return ServerName.valueOf("127.0.0.1", 8000 + i, 10000 + i);
  }

  private String getFileName(String base, int i) {
    return String.format(base + "-%04d", i);
  }

  @Test
  public void testReplicator() throws ReplicationException {
    assertTrue(storage.listAllReplicators().isEmpty());
    String peerId = "1";
    for (int i = 0; i < 10; i++) {
      ReplicationQueueId queueId = new ReplicationQueueId(getServerName(i), peerId);
      storage.setOffset(queueId, "group-" + i, new ReplicationGroupOffset("file-" + i, i * 100),
        Collections.emptyMap());
    }
    List<ServerName> replicators = storage.listAllReplicators();
    assertEquals(10, replicators.size());
    for (int i = 0; i < 10; i++) {
      assertThat(replicators, hasItem(getServerName(i)));
    }
    for (int i = 0; i < 5; i++) {
      ReplicationQueueId queueId = new ReplicationQueueId(getServerName(i), peerId);
      storage.removeQueue(queueId);
    }
    replicators = storage.listAllReplicators();
    assertEquals(5, replicators.size());
    for (int i = 0; i < 5; i++) {
      assertThat(replicators, not(hasItem(getServerName(i))));
    }
    for (int i = 5; i < 10; i++) {
      assertThat(replicators, hasItem(getServerName(i)));
    }
  }

  private void assertQueueId(String peerId, ServerName serverName, ReplicationQueueId queueId) {
    assertEquals(peerId, queueId.getPeerId());
    assertEquals(serverName, queueId.getServerName());
    assertFalse(queueId.getSourceServerName().isPresent());
  }

  @Test
  public void testPersistLogPositionAndSeqIdAtomically() throws Exception {
    ServerName serverName1 = ServerName.valueOf("127.0.0.1", 8000, 10000);
    assertTrue(storage.listAllQueueIds(serverName1).isEmpty());
    String peerId1 = "1";
    String region0 = "6b2c8f8555335cc9af74455b94516cbe";
    String region1 = "6ecd2e9e010499f8ddef97ee8f70834f";

    for (int i = 0; i < 10; i++) {
      ReplicationQueueId queueId = new ReplicationQueueId(serverName1, peerId1);
      assertTrue(storage.getOffsets(queueId).isEmpty());
    }
    assertEquals(HConstants.NO_SEQNUM, storage.getLastSequenceId(region0, peerId1));
    assertEquals(HConstants.NO_SEQNUM, storage.getLastSequenceId(region1, peerId1));

    for (int i = 0; i < 10; i++) {
      ReplicationQueueId queueId = new ReplicationQueueId(serverName1, peerId1);
      storage.setOffset(queueId, "group1-" + i,
        new ReplicationGroupOffset(getFileName("file1", i), (i + 1) * 100),
        ImmutableMap.of(region0, i * 100L, region1, (i + 1) * 100L));
    }

    List<ReplicationQueueId> queueIds = storage.listAllQueueIds(serverName1);
    assertEquals(1, queueIds.size());
    assertQueueId(peerId1, serverName1, queueIds.get(0));

    Map<String, ReplicationGroupOffset> offsets =
      storage.getOffsets(new ReplicationQueueId(serverName1, peerId1));
    for (int i = 0; i < 10; i++) {
      ReplicationGroupOffset offset = offsets.get("group1-" + i);
      assertEquals(getFileName("file1", i), offset.getWal());
      assertEquals((i + 1) * 100, offset.getOffset());
    }
    assertEquals(900L, storage.getLastSequenceId(region0, peerId1));
    assertEquals(1000L, storage.getLastSequenceId(region1, peerId1));

    // Try to decrease the last pushed id by setWALPosition method.
    storage.setOffset(new ReplicationQueueId(serverName1, peerId1), "group1-0",
      new ReplicationGroupOffset(getFileName("file1", 0), 11 * 100),
      ImmutableMap.of(region0, 899L, region1, 1001L));
    assertEquals(900L, storage.getLastSequenceId(region0, peerId1));
    assertEquals(1001L, storage.getLastSequenceId(region1, peerId1));
  }

  private void assertGroupOffset(String wal, long offset, ReplicationGroupOffset groupOffset) {
    assertEquals(wal, groupOffset.getWal());
    assertEquals(offset, groupOffset.getOffset());
  }

  @Test
  public void testClaimQueue() throws Exception {
    String peerId = "1";
    ServerName serverName1 = getServerName(1);
    ReplicationQueueId queueId = new ReplicationQueueId(serverName1, peerId);
    for (int i = 0; i < 10; i++) {
      storage.setOffset(queueId, "group-" + i, new ReplicationGroupOffset("wal-" + i, i),
        Collections.emptyMap());
    }

    ServerName serverName2 = getServerName(2);
    Map<String, ReplicationGroupOffset> offsets2 = storage.claimQueue(queueId, serverName2);
    assertEquals(10, offsets2.size());
    for (int i = 0; i < 10; i++) {
      assertGroupOffset("wal-" + i, i, offsets2.get("group-" + i));
    }
    ReplicationQueueId claimedQueueId2 = new ReplicationQueueId(serverName2, peerId, serverName1);
    assertThat(storage.listAllQueueIds(peerId, serverName1), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId, serverName2),
      Matchers.<List<ReplicationQueueId>> both(hasItem(claimedQueueId2)).and(hasSize(1)));
    offsets2 = storage.getOffsets(claimedQueueId2);
    assertEquals(10, offsets2.size());
    for (int i = 0; i < 10; i++) {
      assertGroupOffset("wal-" + i, i, offsets2.get("group-" + i));
    }

    ServerName serverName3 = getServerName(3);
    Map<String, ReplicationGroupOffset> offsets3 = storage.claimQueue(claimedQueueId2, serverName3);
    assertEquals(10, offsets3.size());
    for (int i = 0; i < 10; i++) {
      assertGroupOffset("wal-" + i, i, offsets3.get("group-" + i));
    }
    ReplicationQueueId claimedQueueId3 = new ReplicationQueueId(serverName3, peerId, serverName1);
    assertThat(storage.listAllQueueIds(peerId, serverName1), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId, serverName2), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId, serverName3),
      Matchers.<List<ReplicationQueueId>> both(hasItem(claimedQueueId3)).and(hasSize(1)));
    offsets3 = storage.getOffsets(claimedQueueId3);
    assertEquals(10, offsets3.size());
    for (int i = 0; i < 10; i++) {
      assertGroupOffset("wal-" + i, i, offsets3.get("group-" + i));
    }
    storage.removeQueue(claimedQueueId3);
    assertThat(storage.listAllQueueIds(peerId), IsEmptyCollection.empty());
  }

  @Test
  public void testClaimQueueMultiThread() throws Exception {
    String peerId = "3";
    String walGroup = "group";
    ReplicationGroupOffset groupOffset = new ReplicationGroupOffset("wal", 123);
    ServerName sourceServerName = getServerName(100);
    ReplicationQueueId queueId = new ReplicationQueueId(sourceServerName, peerId);
    storage.setOffset(queueId, walGroup, groupOffset, Collections.emptyMap());
    List<ServerName> serverNames =
      IntStream.range(0, 10).mapToObj(this::getServerName).collect(Collectors.toList());
    for (int i = 0; i < 10; i++) {
      final ReplicationQueueId toClaim = queueId;
      List<Thread> threads = new ArrayList<>();
      Map<ServerName, Map<String, ReplicationGroupOffset>> claimed = new ConcurrentHashMap<>();
      Set<ServerName> failed = ConcurrentHashMap.newKeySet();
      for (ServerName serverName : serverNames) {
        if (serverName.equals(queueId.getServerName())) {
          continue;
        }
        threads.add(new Thread("Claim-" + i + "-" + serverName) {

          @Override
          public void run() {
            try {
              Map<String, ReplicationGroupOffset> offsets = storage.claimQueue(toClaim, serverName);
              if (!offsets.isEmpty()) {
                claimed.put(serverName, offsets);
              }
            } catch (ReplicationException e) {
              LOG.error("failed to claim queue", e);
              failed.add(serverName);
            }
          }
        });
      }
      LOG.info("Claim round {}, there are {} threads to claim {}", i, threads.size(), toClaim);
      for (Thread thread : threads) {
        thread.start();
      }
      for (Thread thread : threads) {
        thread.join(30000);
        assertFalse(thread.isAlive());
      }
      LOG.info("Finish claim round {}, claimed={}, failed={}", i, claimed, failed);
      assertThat(failed, IsEmptyCollection.empty());
      assertEquals(1, claimed.size());
      Map<String, ReplicationGroupOffset> offsets = Iterables.getOnlyElement(claimed.values());
      assertEquals(1, offsets.size());
      assertGroupOffset("wal", 123, offsets.get("group"));
      queueId = new ReplicationQueueId(Iterables.getOnlyElement(claimed.keySet()), peerId,
        sourceServerName);
      assertThat(storage.listAllQueueIds(peerId),
        Matchers.<List<ReplicationQueueId>> both(hasItem(queueId)).and(hasSize(1)));
    }
  }

  @Test
  public void testListRemovePeerAllQueues() throws Exception {
    String peerId1 = "1";
    String peerId2 = "2";
    for (int i = 0; i < 100; i++) {
      ServerName serverName = getServerName(i);
      String group = "group";
      ReplicationGroupOffset offset = new ReplicationGroupOffset("wal", i);
      ReplicationQueueId queueId1 = new ReplicationQueueId(serverName, peerId1);
      ReplicationQueueId queueId2 = new ReplicationQueueId(serverName, peerId2);
      storage.setOffset(queueId1, group, offset, Collections.emptyMap());
      storage.setOffset(queueId2, group, offset, Collections.emptyMap());
    }
    List<ReplicationQueueData> queueDatas = storage.listAllQueues();
    assertThat(queueDatas, hasSize(200));
    for (int i = 0; i < 100; i++) {
      ReplicationQueueData peerId1Data = queueDatas.get(i);
      ReplicationQueueData peerId2Data = queueDatas.get(i + 100);
      ServerName serverName = getServerName(i);
      assertEquals(new ReplicationQueueId(serverName, peerId1), peerId1Data.getId());
      assertEquals(new ReplicationQueueId(serverName, peerId2), peerId2Data.getId());
      assertEquals(1, peerId1Data.getOffsets().size());
      assertEquals(1, peerId2Data.getOffsets().size());
      assertGroupOffset("wal", i, peerId1Data.getOffsets().get("group"));
      assertGroupOffset("wal", i, peerId2Data.getOffsets().get("group"));
    }
    List<ReplicationQueueId> queueIds1 = storage.listAllQueueIds(peerId1);
    assertThat(queueIds1, hasSize(100));
    for (int i = 0; i < 100; i++) {
      ServerName serverName = getServerName(i);
      assertEquals(new ReplicationQueueId(serverName, peerId1), queueIds1.get(i));
    }
    List<ReplicationQueueId> queueIds2 = storage.listAllQueueIds(peerId2);
    assertThat(queueIds2, hasSize(100));
    for (int i = 0; i < 100; i++) {
      ServerName serverName = getServerName(i);
      assertEquals(new ReplicationQueueId(serverName, peerId2), queueIds2.get(i));
    }

    storage.removeAllQueues(peerId1);
    assertThat(storage.listAllQueues(), hasSize(100));
    assertThat(storage.listAllQueueIds(peerId1), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId2), hasSize(100));

    storage.removeAllQueues(peerId2);
    assertThat(storage.listAllQueues(), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId1), IsEmptyCollection.empty());
    assertThat(storage.listAllQueueIds(peerId2), IsEmptyCollection.empty());
  }

  @Test
  public void testRemoveAllLastPushedSeqIdsForPeer() throws Exception {
    String peerId = "1";
    String peerIdToDelete = "2";
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      storage.setLastSequenceIds(peerId, ImmutableMap.of(encodedRegionName, (long) i));
      storage.setLastSequenceIds(peerIdToDelete, ImmutableMap.of(encodedRegionName, (long) i));
    }
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, storage.getLastSequenceId(encodedRegionName, peerId));
      assertEquals(i, storage.getLastSequenceId(encodedRegionName, peerIdToDelete));
    }
    storage.removeLastSequenceIds(peerIdToDelete);
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, storage.getLastSequenceId(encodedRegionName, peerId));
      assertEquals(HConstants.NO_SEQNUM,
        storage.getLastSequenceId(encodedRegionName, peerIdToDelete));
    }
  }

  @Test
  public void testHfileRefsReplicationQueues() throws ReplicationException, KeeperException {
    String peerId1 = "1";

    List<Pair<Path, Path>> files1 = new ArrayList<>(3);
    files1.add(new Pair<>(null, new Path("file_1")));
    files1.add(new Pair<>(null, new Path("file_2")));
    files1.add(new Pair<>(null, new Path("file_3")));
    assertTrue(storage.getReplicableHFiles(peerId1).isEmpty());
    assertEquals(0, storage.getAllPeersFromHFileRefsQueue().size());

    storage.addHFileRefs(peerId1, files1);
    assertEquals(1, storage.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, storage.getReplicableHFiles(peerId1).size());
    List<String> hfiles2 = new ArrayList<>(files1.size());
    for (Pair<Path, Path> p : files1) {
      hfiles2.add(p.getSecond().getName());
    }
    String removedString = hfiles2.remove(0);
    storage.removeHFileRefs(peerId1, hfiles2);
    assertEquals(1, storage.getReplicableHFiles(peerId1).size());
    hfiles2 = new ArrayList<>(1);
    hfiles2.add(removedString);
    storage.removeHFileRefs(peerId1, hfiles2);
    assertEquals(0, storage.getReplicableHFiles(peerId1).size());
  }

  @Test
  public void testRemovePeerForHFileRefs() throws ReplicationException, KeeperException {
    String peerId1 = "1";
    String peerId2 = "2";

    List<Pair<Path, Path>> files1 = new ArrayList<>(3);
    files1.add(new Pair<>(null, new Path("file_1")));
    files1.add(new Pair<>(null, new Path("file_2")));
    files1.add(new Pair<>(null, new Path("file_3")));
    storage.addHFileRefs(peerId1, files1);
    storage.addHFileRefs(peerId2, files1);
    assertEquals(2, storage.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, storage.getReplicableHFiles(peerId1).size());
    assertEquals(3, storage.getReplicableHFiles(peerId2).size());

    storage.removePeerFromHFileRefs(peerId1);
    assertEquals(1, storage.getAllPeersFromHFileRefsQueue().size());
    assertTrue(storage.getReplicableHFiles(peerId1).isEmpty());
    assertEquals(3, storage.getReplicableHFiles(peerId2).size());

    storage.removePeerFromHFileRefs(peerId2);
    assertEquals(0, storage.getAllPeersFromHFileRefsQueue().size());
    assertTrue(storage.getReplicableHFiles(peerId2).isEmpty());
  }

  private void addLastSequenceIdsAndHFileRefs(String peerId1, String peerId2)
    throws ReplicationException {
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      storage.setLastSequenceIds(peerId1, ImmutableMap.of(encodedRegionName, (long) i));
    }

    List<Pair<Path, Path>> files1 = new ArrayList<>(3);
    files1.add(new Pair<>(null, new Path("file_1")));
    files1.add(new Pair<>(null, new Path("file_2")));
    files1.add(new Pair<>(null, new Path("file_3")));
    storage.addHFileRefs(peerId2, files1);
  }

  @Test
  public void testRemoveLastSequenceIdsAndHFileRefsBefore()
    throws ReplicationException, InterruptedException {
    String peerId1 = "1";
    String peerId2 = "2";
    addLastSequenceIdsAndHFileRefs(peerId1, peerId2);
    // make sure we have write these out
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, storage.getLastSequenceId(encodedRegionName, peerId1));
    }
    assertEquals(1, storage.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, storage.getReplicableHFiles(peerId2).size());

    // should have nothing after removal
    long ts = EnvironmentEdgeManager.currentTime();
    storage.removeLastSequenceIdsAndHFileRefsBefore(ts);
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(HConstants.NO_SEQNUM, storage.getLastSequenceId(encodedRegionName, peerId1));
    }
    assertEquals(0, storage.getAllPeersFromHFileRefsQueue().size());

    Thread.sleep(100);
    // add again and remove with the old timestamp
    addLastSequenceIdsAndHFileRefs(peerId1, peerId2);
    storage.removeLastSequenceIdsAndHFileRefsBefore(ts);
    // make sure we do not delete the data which are written after the give timestamp
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, storage.getLastSequenceId(encodedRegionName, peerId1));
    }
    assertEquals(1, storage.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, storage.getReplicableHFiles(peerId2).size());
  }

  @Test
  public void testListAllPeerIds() throws ReplicationException {
    assertThat(storage.listAllPeerIds(), empty());

    for (int i = 0; i < 20; i++) {
      int numQueues = ThreadLocalRandom.current().nextInt(10, 100);
      for (int j = 0; j < numQueues; j++) {
        ReplicationQueueId queueId = new ReplicationQueueId(getServerName(j), "Peer_" + i);
        storage.setOffset(queueId, "group-" + j, new ReplicationGroupOffset("file-" + j, j * 100),
          Collections.emptyMap());
      }
    }
    List<String> peerIds = storage.listAllPeerIds();
    assertThat(peerIds, hasSize(20));
    for (int i = 0; i < 20; i++) {
      assertThat(peerIds, hasItem("Peer_" + i));
    }
  }
}
