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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestZKReplicationQueueStorage {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKReplicationQueueStorage.class);

  private static final HBaseZKTestingUtility UTIL = new HBaseZKTestingUtility();

  private static ZKReplicationQueueStorage STORAGE;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniZKCluster();
    STORAGE = new ZKReplicationQueueStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @After
  public void tearDownAfterTest() throws ReplicationException, KeeperException, IOException {
    for (ServerName serverName : STORAGE.getListOfReplicators()) {
      for (String queue : STORAGE.getAllQueues(serverName)) {
        STORAGE.removeQueue(serverName, queue);
      }
      STORAGE.removeReplicatorIfQueueIsEmpty(serverName);
    }
    for (String peerId : STORAGE.getAllPeersFromHFileRefsQueue()) {
      STORAGE.removePeerFromHFileRefs(peerId);
    }
  }

  private ServerName getServerName(int i) {
    return ServerName.valueOf("127.0.0.1", 8000 + i, 10000 + i);
  }

  @Test
  public void testReplicator() throws ReplicationException {
    assertTrue(STORAGE.getListOfReplicators().isEmpty());
    String queueId = "1";
    for (int i = 0; i < 10; i++) {
      STORAGE.addWAL(getServerName(i), queueId, "file" + i);
    }
    List<ServerName> replicators = STORAGE.getListOfReplicators();
    assertEquals(10, replicators.size());
    for (int i = 0; i < 10; i++) {
      assertThat(replicators, hasItems(getServerName(i)));
    }
    for (int i = 0; i < 5; i++) {
      STORAGE.removeQueue(getServerName(i), queueId);
    }
    for (int i = 0; i < 10; i++) {
      STORAGE.removeReplicatorIfQueueIsEmpty(getServerName(i));
    }
    replicators = STORAGE.getListOfReplicators();
    assertEquals(5, replicators.size());
    for (int i = 5; i < 10; i++) {
      assertThat(replicators, hasItems(getServerName(i)));
    }
  }

  private String getFileName(String base, int i) {
    return String.format(base + "-%04d", i);
  }

  @Test
  public void testAddRemoveLog() throws ReplicationException {
    ServerName serverName1 = ServerName.valueOf("127.0.0.1", 8000, 10000);
    assertTrue(STORAGE.getAllQueues(serverName1).isEmpty());
    String queue1 = "1";
    String queue2 = "2";
    for (int i = 0; i < 10; i++) {
      STORAGE.addWAL(serverName1, queue1, getFileName("file1", i));
      STORAGE.addWAL(serverName1, queue2, getFileName("file2", i));
    }
    List<String> queueIds = STORAGE.getAllQueues(serverName1);
    assertEquals(2, queueIds.size());
    assertThat(queueIds, hasItems("1", "2"));

    List<String> wals1 = STORAGE.getWALsInQueue(serverName1, queue1);
    List<String> wals2 = STORAGE.getWALsInQueue(serverName1, queue2);
    assertEquals(10, wals1.size());
    assertEquals(10, wals2.size());
    for (int i = 0; i < 10; i++) {
      assertThat(wals1, hasItems(getFileName("file1", i)));
      assertThat(wals2, hasItems(getFileName("file2", i)));
    }

    for (int i = 0; i < 10; i++) {
      assertEquals(0, STORAGE.getWALPosition(serverName1, queue1, getFileName("file1", i)));
      assertEquals(0, STORAGE.getWALPosition(serverName1, queue2, getFileName("file2", i)));
      STORAGE.setWALPosition(serverName1, queue1, getFileName("file1", i), (i + 1) * 100,
        Collections.emptyMap());
      STORAGE.setWALPosition(serverName1, queue2, getFileName("file2", i), (i + 1) * 100 + 10,
        Collections.emptyMap());
    }

    for (int i = 0; i < 10; i++) {
      assertEquals((i + 1) * 100,
        STORAGE.getWALPosition(serverName1, queue1, getFileName("file1", i)));
      assertEquals((i + 1) * 100 + 10,
        STORAGE.getWALPosition(serverName1, queue2, getFileName("file2", i)));
    }

    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        STORAGE.removeWAL(serverName1, queue1, getFileName("file1", i));
      } else {
        STORAGE.removeWAL(serverName1, queue2, getFileName("file2", i));
      }
    }

    queueIds = STORAGE.getAllQueues(serverName1);
    assertEquals(2, queueIds.size());
    assertThat(queueIds, hasItems("1", "2"));

    ServerName serverName2 = ServerName.valueOf("127.0.0.1", 8001, 10001);
    Pair<String, SortedSet<String>> peer1 = STORAGE.claimQueue(serverName1, "1", serverName2);

    assertEquals("1-" + serverName1.getServerName(), peer1.getFirst());
    assertEquals(5, peer1.getSecond().size());
    int i = 1;
    for (String wal : peer1.getSecond()) {
      assertEquals(getFileName("file1", i), wal);
      assertEquals((i + 1) * 100,
        STORAGE.getWALPosition(serverName2, peer1.getFirst(), getFileName("file1", i)));
      i += 2;
    }

    queueIds = STORAGE.getAllQueues(serverName1);
    assertEquals(1, queueIds.size());
    assertThat(queueIds, hasItems("2"));
    wals2 = STORAGE.getWALsInQueue(serverName1, queue2);
    assertEquals(5, wals2.size());
    for (i = 0; i < 10; i += 2) {
      assertThat(wals2, hasItems(getFileName("file2", i)));
    }

    queueIds = STORAGE.getAllQueues(serverName2);
    assertEquals(1, queueIds.size());
    assertThat(queueIds, hasItems(peer1.getFirst()));
    wals1 = STORAGE.getWALsInQueue(serverName2, peer1.getFirst());
    assertEquals(5, wals1.size());
    for (i = 1; i < 10; i += 2) {
      assertThat(wals1, hasItems(getFileName("file1", i)));
    }

    Set<String> allWals = STORAGE.getAllWALs();
    assertEquals(10, allWals.size());
    for (i = 0; i < 10; i++) {
      assertThat(allWals, hasItems(i % 2 == 0 ? getFileName("file2", i) : getFileName("file1", i)));
    }
  }

  // For HBASE-12865, HBASE-26482
  @Test
  public void testClaimQueueChangeCversion() throws ReplicationException, KeeperException {
    ServerName serverName1 = ServerName.valueOf("127.0.0.1", 8000, 10000);
    STORAGE.addWAL(serverName1, "1", "file");
    STORAGE.addWAL(serverName1, "2", "file");

    ServerName serverName2 = ServerName.valueOf("127.0.0.1", 8001, 10001);
    // Avoid claimQueue update cversion for prepare server2 rsNode.
    STORAGE.addWAL(serverName2, "1", "file");
    STORAGE.addWAL(serverName2, "2", "file");

    int v0 = STORAGE.getQueuesZNodeCversion();

    STORAGE.claimQueue(serverName1, "1", serverName2);
    int v1 = STORAGE.getQueuesZNodeCversion();
    // cversion should be increased by claimQueue method.
    assertTrue(v1 > v0);

    STORAGE.claimQueue(serverName1, "2", serverName2);
    int v2 = STORAGE.getQueuesZNodeCversion();
    // cversion should be increased by claimQueue method.
    assertTrue(v2 > v1);
  }

  private ZKReplicationQueueStorage createWithUnstableVersion() throws IOException {
    return new ZKReplicationQueueStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration()) {

      private int called = 0;
      private int getLastSeqIdOpIndex = 0;

      @Override
      protected int getQueuesZNodeCversion() throws KeeperException {
        if (called < 4) {
          called++;
        }
        return called;
      }

      @Override
      protected Pair<Long, Integer> getLastSequenceIdWithVersion(String encodedRegionName,
          String peerId) throws KeeperException {
        Pair<Long, Integer> oldPair = super.getLastSequenceIdWithVersion(encodedRegionName, peerId);
        if (getLastSeqIdOpIndex < 100) {
          // Let the ZNode version increase.
          String path = getSerialReplicationRegionPeerNode(encodedRegionName, peerId);
          ZKUtil.createWithParents(zookeeper, path);
          ZKUtil.setData(zookeeper, path, ZKUtil.positionToByteArray(100L));
        }
        getLastSeqIdOpIndex++;
        return oldPair;
      }
    };
  }

  @Test
  public void testGetAllWALsCversionChange() throws IOException, ReplicationException {
    ZKReplicationQueueStorage storage = createWithUnstableVersion();
    storage.addWAL(getServerName(0), "1", "file");
    // This should return eventually when cversion stabilizes
    Set<String> allWals = storage.getAllWALs();
    assertEquals(1, allWals.size());
    assertThat(allWals, hasItems("file"));
  }

  // For HBASE-14621
  @Test
  public void testGetAllHFileRefsCversionChange() throws IOException, ReplicationException {
    ZKReplicationQueueStorage storage = createWithUnstableVersion();
    storage.addPeerToHFileRefs("1");
    Path p = new Path("/test");
    storage.addHFileRefs("1", Arrays.asList(Pair.newPair(p, p)));
    // This should return eventually when cversion stabilizes
    Set<String> allHFileRefs = storage.getAllHFileRefs();
    assertEquals(1, allHFileRefs.size());
    assertThat(allHFileRefs, hasItems("test"));
  }

  // For HBASE-20138
  @Test
  public void testSetWALPositionBadVersion() throws IOException, ReplicationException {
    ZKReplicationQueueStorage storage = createWithUnstableVersion();
    ServerName serverName1 = ServerName.valueOf("128.0.0.1", 8000, 10000);
    assertTrue(storage.getAllQueues(serverName1).isEmpty());
    String queue1 = "1";
    String fileName = getFileName("file1", 0);
    String encodedRegionName = "31d9792f4435b99d9fb1016f6fbc8dc6";
    storage.addWAL(serverName1, queue1, fileName);

    List<String> wals1 = storage.getWALsInQueue(serverName1, queue1);
    assertEquals(1, wals1.size());

    assertEquals(0, storage.getWALPosition(serverName1, queue1, fileName));
    // This should return eventually when data version stabilizes
    storage.setWALPosition(serverName1, queue1, fileName, 100,
      ImmutableMap.of(encodedRegionName, 120L));

    assertEquals(100, storage.getWALPosition(serverName1, queue1, fileName));
    assertEquals(120L, storage.getLastSequenceId(encodedRegionName, queue1));
  }

  @Test
  public void testRegionsZNodeLayout() throws Exception {
    String peerId = "1";
    String encodedRegionName = "31d9792f4435b99d9fb1016f6fbc8dc7";
    String expectedPath = "/hbase/replication/regions/31/d9/792f4435b99d9fb1016f6fbc8dc7-" + peerId;
    String path = STORAGE.getSerialReplicationRegionPeerNode(encodedRegionName, peerId);
    assertEquals(expectedPath, path);
  }

  @Test
  public void testRemoveAllLastPushedSeqIdsForPeer() throws Exception {
    String peerId = "1";
    String peerIdToDelete = "2";
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      STORAGE.setLastSequenceIds(peerId, ImmutableMap.of(encodedRegionName, (long) i));
      STORAGE.setLastSequenceIds(peerIdToDelete, ImmutableMap.of(encodedRegionName, (long) i));
    }
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, STORAGE.getLastSequenceId(encodedRegionName, peerId));
      assertEquals(i, STORAGE.getLastSequenceId(encodedRegionName, peerIdToDelete));
    }
    STORAGE.removeLastSequenceIds(peerIdToDelete);
    for (int i = 0; i < 100; i++) {
      String encodedRegionName = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      assertEquals(i, STORAGE.getLastSequenceId(encodedRegionName, peerId));
      assertEquals(HConstants.NO_SEQNUM,
        STORAGE.getLastSequenceId(encodedRegionName, peerIdToDelete));
    }
  }
}
