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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
  public void tearDownAfterTest() throws ReplicationException {
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
    assertEquals(10, wals1.size());
    for (int i = 0; i < 10; i++) {
      assertThat(wals1, hasItems(getFileName("file1", i)));
      assertThat(wals2, hasItems(getFileName("file2", i)));
    }

    for (int i = 0; i < 10; i++) {
      assertEquals(0, STORAGE.getWALPosition(serverName1, queue1, getFileName("file1", i)));
      assertEquals(0, STORAGE.getWALPosition(serverName1, queue2, getFileName("file2", i)));
      STORAGE.setWALPosition(serverName1, queue1, getFileName("file1", i), (i + 1) * 100);
      STORAGE.setWALPosition(serverName1, queue2, getFileName("file2", i), (i + 1) * 100 + 10);
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

  // For HBASE-12865
  @Test
  public void testClaimQueueChangeCversion() throws ReplicationException, KeeperException {
    ServerName serverName1 = ServerName.valueOf("127.0.0.1", 8000, 10000);
    STORAGE.addWAL(serverName1, "1", "file");

    int v0 = STORAGE.getQueuesZNodeCversion();
    ServerName serverName2 = ServerName.valueOf("127.0.0.1", 8001, 10001);
    STORAGE.claimQueue(serverName1, "1", serverName2);
    int v1 = STORAGE.getQueuesZNodeCversion();
    // cversion should increase by 1 since a child node is deleted
    assertEquals(1, v1 - v0);
  }

  private ZKReplicationQueueStorage createWithUnstableCversion() throws IOException {
    return new ZKReplicationQueueStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration()) {

      private int called = 0;

      @Override
      protected int getQueuesZNodeCversion() throws KeeperException {
        if (called < 4) {
          called++;
        }
        return called;
      }
    };
  }

  @Test
  public void testGetAllWALsCversionChange() throws IOException, ReplicationException {
    ZKReplicationQueueStorage storage = createWithUnstableCversion();
    storage.addWAL(getServerName(0), "1", "file");
    // This should return eventually when cversion stabilizes
    Set<String> allWals = storage.getAllWALs();
    assertEquals(1, allWals.size());
    assertThat(allWals, hasItems("file"));
  }

  // For HBASE-14621
  @Test
  public void testGetAllHFileRefsCversionChange() throws IOException, ReplicationException {
    ZKReplicationQueueStorage storage = createWithUnstableCversion();
    storage.addPeerToHFileRefs("1");
    Path p = new Path("/test");
    storage.addHFileRefs("1", Arrays.asList(Pair.newPair(p, p)));
    // This should return eventually when cversion stabilizes
    Set<String> allHFileRefs = storage.getAllHFileRefs();
    assertEquals(1, allHFileRefs.size());
    assertThat(allHFileRefs, hasItems("test"));
  }
}
