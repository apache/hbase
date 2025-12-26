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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableName;

/**
 * A helper tool for generating random {@link ReplicationPeerConfig} and do assertion.
 */
public final class ReplicationPeerConfigTestUtil {

  // Seed may be set with Random#setSeed
  private static final Random RNG = new Random();

  private ReplicationPeerConfigTestUtil() {
  }

  private static Set<String> randNamespaces(Random rand) {
    return Stream.generate(() -> Long.toHexString(rand.nextLong())).limit(rand.nextInt(5))
      .collect(toSet());
  }

  private static Map<TableName, List<String>> randTableCFs(Random rand) {
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

  public static ReplicationPeerConfig getConfig(int seed) {
    RNG.setSeed(seed);
    return ReplicationPeerConfig.newBuilder().setClusterKey(Long.toHexString(RNG.nextLong()))
      .setReplicationEndpointImpl(Long.toHexString(RNG.nextLong()))
      .setRemoteWALDir(Long.toHexString(RNG.nextLong())).setNamespaces(randNamespaces(RNG))
      .setExcludeNamespaces(randNamespaces(RNG)).setTableCFsMap(randTableCFs(RNG))
      .setExcludeTableCFsMap(randTableCFs(RNG)).setReplicateAllUserTables(RNG.nextBoolean())
      .setBandwidth(RNG.nextInt(1000)).setSleepForRetries(RNG.nextInt(5000)).build();
  }

  private static void assertSetEquals(Set<String> expected, Set<String> actual) {
    if (expected == null || expected.size() == 0) {
      assertTrue(actual == null || actual.size() == 0);
      return;
    }
    assertEquals(expected.size(), actual.size());
    expected.forEach(s -> assertTrue(actual.contains(s)));
  }

  private static void assertMapEquals(Map<TableName, List<String>> expected,
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

  public static void assertConfigEquals(ReplicationPeerConfig expected,
    ReplicationPeerConfig actual) {
    assertEquals(expected.getClusterKey(), actual.getClusterKey());
    assertEquals(expected.getReplicationEndpointImpl(), actual.getReplicationEndpointImpl());
    assertSetEquals(expected.getNamespaces(), actual.getNamespaces());
    assertSetEquals(expected.getExcludeNamespaces(), actual.getExcludeNamespaces());
    assertMapEquals(expected.getTableCFsMap(), actual.getTableCFsMap());
    assertMapEquals(expected.getExcludeTableCFsMap(), actual.getExcludeTableCFsMap());
    assertEquals(expected.replicateAllUserTables(), actual.replicateAllUserTables());
    assertEquals(expected.getBandwidth(), actual.getBandwidth());
    assertEquals(expected.getSleepForRetries(), actual.getSleepForRetries());
  }
}
