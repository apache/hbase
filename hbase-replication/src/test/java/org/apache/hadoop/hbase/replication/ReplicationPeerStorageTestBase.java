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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

public abstract class ReplicationPeerStorageTestBase {

  // Seed may be set with Random#setSeed
  private static final Random RNG = new Random();

  protected static ReplicationPeerStorage STORAGE;

  private Set<String> randNamespaces(Random rand) {
    return Stream.generate(() -> Long.toHexString(rand.nextLong())).limit(rand.nextInt(5))
      .collect(toSet());
  }

  private Map<TableName, List<String>> randTableCFs(Random rand) {
    int size = rand.nextInt(5);
    Map<TableName, List<String>> map = new HashMap<>();
    for (int i = 0; i < size; i++) {
      TableName tn = TableName.valueOf(Long.toHexString(rand.nextLong()));
      List<String> cfs = Stream.generate(() -> Long.toHexString(rand.nextLong()))
        .limit(rand.nextInt(5)).collect(toList());
      map.put(tn, cfs);
    }
    return map;
  }

  private ReplicationPeerConfig getConfig(int seed) {
    RNG.setSeed(seed);
    return ReplicationPeerConfig.newBuilder().setClusterKey(Long.toHexString(RNG.nextLong()))
      .setReplicationEndpointImpl(Long.toHexString(RNG.nextLong()))
      .setNamespaces(randNamespaces(RNG)).setExcludeNamespaces(randNamespaces(RNG))
      .setTableCFsMap(randTableCFs(RNG)).setExcludeTableCFsMap(randTableCFs(RNG))
      .setReplicateAllUserTables(RNG.nextBoolean()).setBandwidth(RNG.nextInt(1000)).build();
  }

  private void assertSetEquals(Set<String> expected, Set<String> actual) {
    if (expected == null || expected.size() == 0) {
      assertTrue(actual == null || actual.size() == 0);
      return;
    }
    assertEquals(expected.size(), actual.size());
    expected.forEach(s -> assertTrue(actual.contains(s)));
  }

  private void assertMapEquals(Map<TableName, List<String>> expected,
    Map<TableName, List<String>> actual) {
    if (expected == null || expected.size() == 0) {
      assertTrue(actual == null || actual.size() == 0);
      return;
    }
    assertEquals(expected.size(), actual.size());
    expected.forEach((expectedTn, expectedCFs) -> {
      List<String> actualCFs = actual.get(expectedTn);
      if (expectedCFs == null || expectedCFs.size() == 0) {
        assertTrue(actual.containsKey(expectedTn));
        assertTrue(actualCFs == null || actualCFs.size() == 0);
      } else {
        assertNotNull(actualCFs);
        assertEquals(expectedCFs.size(), actualCFs.size());
        for (Iterator<String> expectedIt = expectedCFs.iterator(),
            actualIt = actualCFs.iterator(); expectedIt.hasNext();) {
          assertEquals(expectedIt.next(), actualIt.next());
        }
      }
    });
  }

  private void assertConfigEquals(ReplicationPeerConfig expected, ReplicationPeerConfig actual) {
    assertEquals(expected.getClusterKey(), actual.getClusterKey());
    assertEquals(expected.getReplicationEndpointImpl(), actual.getReplicationEndpointImpl());
    assertSetEquals(expected.getNamespaces(), actual.getNamespaces());
    assertSetEquals(expected.getExcludeNamespaces(), actual.getExcludeNamespaces());
    assertMapEquals(expected.getTableCFsMap(), actual.getTableCFsMap());
    assertMapEquals(expected.getExcludeTableCFsMap(), actual.getExcludeTableCFsMap());
    assertEquals(expected.replicateAllUserTables(), actual.replicateAllUserTables());
    assertEquals(expected.getBandwidth(), actual.getBandwidth());
  }

  @Test
  public void test() throws ReplicationException {
    int peerCount = 10;
    for (int i = 0; i < peerCount; i++) {
      STORAGE.addPeer(Integer.toString(i), getConfig(i), i % 2 == 0);
    }
    List<String> peerIds = STORAGE.listPeerIds();
    assertEquals(peerCount, peerIds.size());
    for (String peerId : peerIds) {
      int seed = Integer.parseInt(peerId);
      assertConfigEquals(getConfig(seed), STORAGE.getPeerConfig(peerId));
    }
    for (int i = 0; i < peerCount; i++) {
      STORAGE.updatePeerConfig(Integer.toString(i), getConfig(i + 1));
    }
    for (String peerId : peerIds) {
      int seed = Integer.parseInt(peerId);
      assertConfigEquals(getConfig(seed + 1), STORAGE.getPeerConfig(peerId));
    }
    for (int i = 0; i < peerCount; i++) {
      assertEquals(i % 2 == 0, STORAGE.isPeerEnabled(Integer.toString(i)));
    }
    for (int i = 0; i < peerCount; i++) {
      STORAGE.setPeerState(Integer.toString(i), i % 2 != 0);
    }
    for (int i = 0; i < peerCount; i++) {
      assertEquals(i % 2 != 0, STORAGE.isPeerEnabled(Integer.toString(i)));
    }
    String toRemove = Integer.toString(peerCount / 2);
    STORAGE.removePeer(toRemove);
    peerIds = STORAGE.listPeerIds();
    assertEquals(peerCount - 1, peerIds.size());
    assertFalse(peerIds.contains(toRemove));

    try {
      STORAGE.getPeerConfig(toRemove);
      fail("Should throw a ReplicationException when getting peer config of a removed peer");
    } catch (ReplicationException e) {
    }
  }

  protected abstract void assertPeerNameControlException(ReplicationException e);

  @Test
  public void testPeerNameControl() throws Exception {
    String clusterKey = "key";
    STORAGE.addPeer("6", ReplicationPeerConfig.newBuilder().setClusterKey(clusterKey).build(),
      true);

    try {
      ReplicationException e = assertThrows(ReplicationException.class, () -> STORAGE.addPeer("6",
        ReplicationPeerConfig.newBuilder().setClusterKey(clusterKey).build(), true));
      assertPeerNameControlException(e);
    } finally {
      // clean up
      STORAGE.removePeer("6");
    }
  }
}
