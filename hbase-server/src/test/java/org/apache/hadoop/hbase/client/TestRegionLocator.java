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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestRegionLocator extends AbstractTestRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocator.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startClusterAndCreateTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Override
  protected byte[][] getStartKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getStartKeys();
    }
  }

  @Override
  protected byte[][] getEndKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getEndKeys();
    }
  }

  @Override
  protected Pair<byte[][], byte[][]> getStartEndKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getStartEndKeys();
    }
  }

  @Override
  protected HRegionLocation getRegionLocation(TableName tableName, byte[] row, int replicaId)
    throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getRegionLocation(row, replicaId);
    }
  }

  @Override
  protected List<HRegionLocation> getRegionLocations(TableName tableName, byte[] row)
    throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getRegionLocations(row);
    }
  }

  @Override
  protected List<HRegionLocation> getAllRegionLocations(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getAllRegionLocations();
    }
  }

  @Override
  protected void clearCache(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      locator.clearRegionLocationCache();
    }
  }

  @Test
  public void testGetRegionLocationsFirstPage() throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(TABLE_NAME)) {
      List<HRegionLocation> page = locator.getRegionLocations(HConstants.EMPTY_START_ROW, 3);
      assertEquals(3 * REGION_REPLICATION, page.size());
      // Contract: regions in ascending start-key order, replicas in ascending replicaId order
      // within each region.
      byte[][] expectedStartKeys =
        new byte[][] { HConstants.EMPTY_START_ROW, SPLIT_KEYS[0], SPLIT_KEYS[1] };
      for (int i = 0; i < 3; i++) {
        for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
          HRegionLocation loc = page.get(i * REGION_REPLICATION + replicaId);
          assertArrayEquals("region " + i + " replica " + replicaId + " start key",
            expectedStartKeys[i], loc.getRegion().getStartKey());
          assertEquals(
            "region " + i + " replica id at index " + (i * REGION_REPLICATION + replicaId),
            replicaId, loc.getRegion().getReplicaId());
        }
      }
    }
  }

  @Test
  public void testGetRegionLocationsPagination() throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(TABLE_NAME)) {
      List<HRegionLocation> all = locator.getAllRegionLocations();
      Set<String> expectedRegionNames = new HashSet<>();
      for (HRegionLocation l : all) {
        expectedRegionNames.add(l.getRegion().getRegionNameAsString());
      }

      Set<String> seen = new HashSet<>();
      byte[] cursor = null;
      int pages = 0;
      while (true) {
        List<HRegionLocation> page = locator.getRegionLocations(cursor, 4);
        if (page.isEmpty()) {
          break;
        }
        pages++;
        for (HRegionLocation l : page) {
          seen.add(l.getRegion().getRegionNameAsString());
        }
        byte[] lastEnd = page.get(page.size() - 1).getRegion().getEndKey();
        if (lastEnd.length == 0) {
          break;
        }
        cursor = lastEnd;
      }
      assertEquals(expectedRegionNames, seen);
      // 10 regions, page size 4 → exactly 3 pages: [reg0..reg3], [reg4..reg7], [reg8..reg9].
      assertEquals(3, pages);
    }
  }

  @Test
  public void testGetRegionLocationsEmptyAfterEnd() throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(TABLE_NAME)) {
      // Use a startKey lexicographically after all split keys: SPLIT_KEYS go "1".."9", so "z".
      List<HRegionLocation> page = locator.getRegionLocations(Bytes.toBytes("z"), 5);
      assertTrue("expected empty page past the last region; got " + page.size() + " entries",
        page.isEmpty());
    }
  }

  @Test
  public void testGetRegionLocationsCursorMatchesAllReplicas() throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(TABLE_NAME)) {
      List<HRegionLocation> page = locator.getRegionLocations(HConstants.EMPTY_START_ROW, 2);
      assertEquals(2 * REGION_REPLICATION, page.size());
      // Last REGION_REPLICATION entries are all replicas of the last region — same RegionInfo,
      // so same end key regardless of which one the caller picks as the cursor.
      byte[] expectedCursor = page.get(page.size() - 1).getRegion().getEndKey();
      for (int i = 1; i <= REGION_REPLICATION; i++) {
        byte[] cursor = page.get(page.size() - i).getRegion().getEndKey();
        assertArrayEquals("replica " + i + " end key disagrees", expectedCursor, cursor);
      }
    }
  }

  @Test
  public void testGetRegionLocationsLimitFallsBackToConfig() throws IOException {
    // Default HBASE_META_SCANNER_CACHING is 100, table has 10 regions; limit=0 must fall back
    // to the config and return everything in one shot.
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(TABLE_NAME)) {
      List<HRegionLocation> page = locator.getRegionLocations(HConstants.EMPTY_START_ROW, 0);
      assertEquals(REGION_REPLICATION * (SPLIT_KEYS.length + 1), page.size());
    }
  }

  @Test
  public void testGetRegionLocationsHoldsUserRegionLock() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    try (
      ConnectionImplementation conn =
        (ConnectionImplementation) ConnectionFactory.createConnection(conf);
      RegionLocator locator = conn.getRegionLocator(TABLE_NAME)) {
      MetricsConnection metrics = conn.getConnectionMetrics();
      long before = metrics.getUserRegionLockHeldTimer().getCount();
      locator.getRegionLocations(HConstants.EMPTY_START_ROW, 3);
      long after = metrics.getUserRegionLockHeldTimer().getCount();
      assertEquals(
        "userRegionLock held-timer should have incremented exactly once for the bulk" + " lookup",
        before + 1, after);
    }
  }

  /**
   * Directly verify that the new API writes into the same {@code metaCache} that
   * {@code ConnectionImplementation.locateRegionInMeta} reads from: after the bulk call, looking up
   * each returned region's start key via the package-private cache accessor must return non-null.
   */
  @Test
  public void testGetRegionLocationsPopulatesMetaCacheDirect() throws IOException {
    ConnectionImplementation conn = (ConnectionImplementation) UTIL.getConnection();
    conn.clearRegionCache(TABLE_NAME);
    try (RegionLocator locator = conn.getRegionLocator(TABLE_NAME)) {
      List<HRegionLocation> page = locator.getRegionLocations(HConstants.EMPTY_START_ROW, 4);
      assertEquals(4 * REGION_REPLICATION, page.size());
      for (HRegionLocation loc : page) {
        byte[] startKey = loc.getRegion().getStartKey();
        RegionLocations cached = conn.getCachedLocation(TABLE_NAME, startKey);
        assertNotNull("metaCache miss for region starting at " + Bytes.toStringBinary(startKey)
          + " — bulk API did not populate the same cache locateRegionInMeta uses", cached);
        HRegionLocation cachedLoc = cached.getRegionLocation(loc.getRegion().getReplicaId());
        assertNotNull("metaCache had region but missing replica " + loc.getRegion().getReplicaId(),
          cachedLoc);
        assertEquals("cached server differs from server returned by bulk API", loc.getServerName(),
          cachedLoc.getServerName());
      }
    }
  }

  /**
   * Indirect verification of the same property: after the bulk call,
   * {@code RegionLocator.getRegionLocation(row, useCache=true)} for a row inside any returned
   * region must be served from cache — i.e. it must NOT acquire the user-region lock (which is only
   * taken when {@code locateRegionInMeta} actually issues a meta RPC). This is the end-to-end proof
   * that the bulk API and the single-region API share the cache.
   */
  @Test
  public void testGetRegionLocationsAvoidsMetaRpcForCachedRows() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    try (
      ConnectionImplementation conn =
        (ConnectionImplementation) ConnectionFactory.createConnection(conf);
      RegionLocator locator = conn.getRegionLocator(TABLE_NAME)) {
      conn.clearRegionCache(TABLE_NAME);
      MetricsConnection metrics = conn.getConnectionMetrics();

      List<HRegionLocation> page = locator.getRegionLocations(HConstants.EMPTY_START_ROW, 4);
      long afterBulk = metrics.getUserRegionLockHeldTimer().getCount();

      // For each returned region, look up a row inside it via the single-region API. Each lookup
      // must be a cache hit (no user-region lock acquired) because the bulk call already
      // populated the shared metaCache.
      for (HRegionLocation loc : page) {
        // Skip non-default replicas — getRegionLocation(row) without a replicaId resolves only
        // the default replica, and the cache check in locateRegionInMeta is per replicaId.
        if (loc.getRegion().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
          continue;
        }
        byte[] startKey = loc.getRegion().getStartKey();
        // EMPTY_START_ROW belongs to the first region; any byte works.
        byte[] probe = startKey.length == 0 ? new byte[] { 0x00 } : startKey;
        HRegionLocation viaCache = locator.getRegionLocation(probe, false);
        assertEquals("single-region lookup returned a different server than the bulk API for "
          + Bytes.toStringBinary(startKey), loc.getServerName(), viaCache.getServerName());
      }

      long afterPointLookups = metrics.getUserRegionLockHeldTimer().getCount();
      assertEquals(
        "user-region lock held-timer should not have advanced — every point lookup should have"
          + " been a metaCache hit, but " + (afterPointLookups - afterBulk)
          + " meta RPCs were issued",
        afterBulk, afterPointLookups);
    }
  }
}
