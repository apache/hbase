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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractTestRegionLocator {

  protected static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  protected static TableName TABLE_NAME = TableName.valueOf("Locator");

  /**
   * Single-replica companion table used only by tests that need to assert per-replica cache
   * population without tripping over the multi-replica
   * {@link AsyncNonMetaRegionLocator#addLocationToCache(HRegionLocation)} merge limitation.
   */
  protected static TableName TABLE_NAME_NO_REPLICA = TableName.valueOf("LocatorNoReplica");

  protected static byte[] FAMILY = Bytes.toBytes("family");

  protected static int REGION_REPLICATION = 3;

  protected static byte[][] SPLIT_KEYS;

  protected static void startClusterAndCreateTable() throws Exception {
    UTIL.startMiniCluster(3);
    HBaseTestingUtil.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, REGION_REPLICATION);
    TableDescriptor td =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setRegionReplication(REGION_REPLICATION)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    SPLIT_KEYS = new byte[9][];
    for (int i = 0; i < 9; i++) {
      SPLIT_KEYS[i] = Bytes.toBytes(Integer.toString(i + 1));
    }
    UTIL.getAdmin().createTable(td, SPLIT_KEYS);
    UTIL.waitTableAvailable(TABLE_NAME);
    TableDescriptor tdNoReplica = TableDescriptorBuilder.newBuilder(TABLE_NAME_NO_REPLICA)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    UTIL.getAdmin().createTable(tdNoReplica, SPLIT_KEYS);
    UTIL.waitTableAvailable(TABLE_NAME_NO_REPLICA);
    try (ConnectionRegistry registry =
      ConnectionRegistryFactory.create(UTIL.getConfiguration(), User.getCurrent())) {
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(UTIL, registry);
    }
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterEach
  public void tearDownAfterTest() throws IOException {
    clearCache(TABLE_NAME);
    clearCache(TABLE_NAME_NO_REPLICA);
    clearCache(TableName.META_TABLE_NAME);
  }

  private byte[] getStartKey(int index) {
    return index == 0 ? HConstants.EMPTY_START_ROW : SPLIT_KEYS[index - 1];
  }

  private byte[] getEndKey(int index) {
    return index == SPLIT_KEYS.length ? HConstants.EMPTY_END_ROW : SPLIT_KEYS[index];
  }

  private void assertStartKeys(byte[][] startKeys) {
    assertEquals(SPLIT_KEYS.length + 1, startKeys.length);
    for (int i = 0; i < startKeys.length; i++) {
      assertArrayEquals(getStartKey(i), startKeys[i]);
    }
  }

  private void assertEndKeys(byte[][] endKeys) {
    assertEquals(SPLIT_KEYS.length + 1, endKeys.length);
    for (int i = 0; i < endKeys.length; i++) {
      assertArrayEquals(getEndKey(i), endKeys[i]);
    }
  }

  @Test
  public void testStartEndKeys() throws IOException {
    assertStartKeys(getStartKeys(TABLE_NAME));
    assertEndKeys(getEndKeys(TABLE_NAME));
    Pair<byte[][], byte[][]> startEndKeys = getStartEndKeys(TABLE_NAME);
    assertStartKeys(startEndKeys.getFirst());
    assertEndKeys(startEndKeys.getSecond());
  }

  private void assertRegionLocation(HRegionLocation loc, int index, int replicaId) {
    RegionInfo region = loc.getRegion();
    byte[] startKey = getStartKey(index);
    assertArrayEquals(startKey, region.getStartKey());
    assertArrayEquals(getEndKey(index), region.getEndKey());
    assertEquals(replicaId, region.getReplicaId());
    ServerName expected = findRegionLocation(TABLE_NAME, region.getStartKey(), replicaId);
    assertEquals(expected, loc.getServerName());
  }

  private ServerName findRegionLocation(TableName tableName, byte[] startKey, int replicaId) {
    return UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer())
      .filter(rs -> rs.getRegions(tableName).stream().map(Region::getRegionInfo)
        .anyMatch(r -> r.containsRow(startKey) && r.getReplicaId() == replicaId))
      .findFirst().get().getServerName();
  }

  @Test
  public void testGetRegionLocation() throws IOException {
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(getRegionLocation(TABLE_NAME, getStartKey(i), replicaId), i,
          replicaId);
      }
    }
  }

  @Test
  public void testGetRegionLocations() throws IOException {
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      List<HRegionLocation> locs = getRegionLocations(TABLE_NAME, getStartKey(i));
      assertEquals(REGION_REPLICATION, locs.size());
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(locs.get(replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetAllRegionLocations() throws IOException {
    List<HRegionLocation> locs = getAllRegionLocations(TABLE_NAME);
    assertEquals(REGION_REPLICATION * (SPLIT_KEYS.length + 1), locs.size());
    Collections.sort(locs, (l1, l2) -> {
      int c = Bytes.compareTo(l1.getRegion().getStartKey(), l2.getRegion().getStartKey());
      if (c != 0) {
        return c;
      }
      return Integer.compare(l1.getRegion().getReplicaId(), l2.getRegion().getReplicaId());
    });
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(locs.get(i * REGION_REPLICATION + replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetRegionLocationsFirstPage() throws IOException {
    List<HRegionLocation> page = getRegionLocationsPage(TABLE_NAME, null, 2);
    assertEquals(2 * REGION_REPLICATION, page.size());
    for (int i = 0; i < 2; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(page.get(i * REGION_REPLICATION + replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetRegionLocationsPagination() throws IOException {
    int pageSize = 3;
    int totalRegions = SPLIT_KEYS.length + 1;
    List<HRegionLocation> all = new ArrayList<>();
    byte[] cursor = null;
    while (true) {
      List<HRegionLocation> page = getRegionLocationsPage(TABLE_NAME, cursor, pageSize);
      if (page.isEmpty()) {
        break;
      }
      all.addAll(page);
      HRegionLocation last = page.get(page.size() - 1);
      byte[] endKey = last.getRegion().getEndKey();
      if (endKey.length == 0) {
        break;
      }
      cursor = endKey;
    }
    assertEquals(totalRegions * REGION_REPLICATION, all.size());
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(all.get(i * REGION_REPLICATION + replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetRegionLocationsEmptyAfterEnd() throws IOException {
    // Use a startKey lexicographically after all split keys: SPLIT_KEYS go "1".."9", so "z".
    List<HRegionLocation> page = getRegionLocationsPage(TABLE_NAME, Bytes.toBytes("z"), 5);
    assertTrue(page.isEmpty(),
      "expected empty page past the last region; got " + page.size() + " entries");
  }

  @Test
  public void testGetRegionLocationsLimitFallsBackToConfig() throws IOException {
    // mini-cluster default for hbase.meta.scanner.caching is well above SPLIT_KEYS.length+1, so
    // limit<=0 must return every region.
    List<HRegionLocation> page = getRegionLocationsPage(TABLE_NAME, null, 0);
    assertEquals((SPLIT_KEYS.length + 1) * REGION_REPLICATION, page.size());
    page = getRegionLocationsPage(TABLE_NAME, null, -1);
    assertEquals((SPLIT_KEYS.length + 1) * REGION_REPLICATION, page.size());
  }

  @Test
  public void testGetRegionLocationsRejectsMeta() {
    assertThrows(IOException.class,
      () -> getRegionLocationsPage(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, 1));
  }

  @Test
  public void testGetRegionLocationsPopulatesCache() throws IOException {
    // Use the single-replica companion table so the multi-replica
    // addLocationToCache merge limitation does not interfere with this test.
    clearCache(TABLE_NAME_NO_REPLICA);
    List<HRegionLocation> page = getRegionLocationsPage(TABLE_NAME_NO_REPLICA, null, 3);
    assertEquals(3, page.size());
    for (HRegionLocation loc : page) {
      byte[] startKey = loc.getRegion().getStartKey();
      RegionLocations cached = getCachedLocation(TABLE_NAME_NO_REPLICA, startKey);
      assertNotNull(cached, "metaCache miss for region starting at " + Bytes.toStringBinary(startKey)
        + " — bulk API did not populate the cache");
      HRegionLocation cachedLoc = cached.getRegionLocation(RegionInfo.DEFAULT_REPLICA_ID);
      assertNotNull(cachedLoc, "metaCache had region but missing default replica entry");
      assertEquals(loc.getServerName(), cachedLoc.getServerName(),
        "cached server differs from server returned by bulk API");
    }
  }

  private void assertMetaStartOrEndKeys(byte[][] keys) {
    assertEquals(1, keys.length);
    assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, keys[0]);
  }

  private void assertMetaRegionLocation(HRegionLocation loc, int replicaId) {
    RegionInfo region = loc.getRegion();
    assertArrayEquals(HConstants.EMPTY_START_ROW, region.getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW, region.getEndKey());
    assertEquals(replicaId, region.getReplicaId());
    ServerName expected =
      findRegionLocation(TableName.META_TABLE_NAME, region.getStartKey(), replicaId);
    assertEquals(expected, loc.getServerName());
  }

  private void assertMetaRegionLocations(List<HRegionLocation> locs) {
    assertEquals(REGION_REPLICATION, locs.size());
    for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
      assertMetaRegionLocation(locs.get(replicaId), replicaId);
    }
  }

  @Test
  public void testMeta() throws IOException {
    assertMetaStartOrEndKeys(getStartKeys(TableName.META_TABLE_NAME));
    assertMetaStartOrEndKeys(getEndKeys(TableName.META_TABLE_NAME));
    Pair<byte[][], byte[][]> startEndKeys = getStartEndKeys(TableName.META_TABLE_NAME);
    assertMetaStartOrEndKeys(startEndKeys.getFirst());
    assertMetaStartOrEndKeys(startEndKeys.getSecond());
    for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
      assertMetaRegionLocation(
        getRegionLocation(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, replicaId),
        replicaId);
    }
    assertMetaRegionLocations(
      getRegionLocations(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW));
    assertMetaRegionLocations(getAllRegionLocations(TableName.META_TABLE_NAME));
  }

  protected abstract byte[][] getStartKeys(TableName tableName) throws IOException;

  protected abstract byte[][] getEndKeys(TableName tableName) throws IOException;

  protected abstract Pair<byte[][], byte[][]> getStartEndKeys(TableName tableName)
    throws IOException;

  protected abstract HRegionLocation getRegionLocation(TableName tableName, byte[] row,
    int replicaId) throws IOException;

  protected abstract List<HRegionLocation> getRegionLocations(TableName tableName, byte[] row)
    throws IOException;

  protected abstract List<HRegionLocation> getAllRegionLocations(TableName tableName)
    throws IOException;

  protected abstract List<HRegionLocation> getRegionLocationsPage(TableName tableName,
    byte[] startKey, int limit) throws IOException;

  protected abstract RegionLocations getCachedLocation(TableName tableName, byte[] startKey)
    throws IOException;

  protected abstract void clearCache(TableName tableName) throws IOException;
}
