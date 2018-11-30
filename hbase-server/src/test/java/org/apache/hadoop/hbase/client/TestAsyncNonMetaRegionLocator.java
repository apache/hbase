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
package org.apache.hadoop.hbase.client;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;
import static org.apache.hadoop.hbase.client.RegionReplicaTestHelper.testLocator;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper.Locator;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncNonMetaRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncNonMetaRegionLocator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static AsyncConnectionImpl CONN;

  private static AsyncNonMetaRegionLocator LOCATOR;

  private static byte[][] SPLIT_KEYS;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    AsyncRegistry registry = AsyncRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    CONN = new AsyncConnectionImpl(TEST_UTIL.getConfiguration(), registry,
      registry.getClusterId().get(), null, User.getCurrent());
    LOCATOR = new AsyncNonMetaRegionLocator(CONN);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(CONN);
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDownAfterTest() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      if (admin.isTableEnabled(TABLE_NAME)) {
        TEST_UTIL.getAdmin().disableTable(TABLE_NAME);
      }
      TEST_UTIL.getAdmin().deleteTable(TABLE_NAME);
    }
    LOCATOR.clearCache(TABLE_NAME);
  }

  private void createSingleRegionTable() throws IOException, InterruptedException {
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  private CompletableFuture<HRegionLocation> getDefaultRegionLocation(TableName tableName,
      byte[] row, RegionLocateType locateType, boolean reload) {
    return LOCATOR
      .getRegionLocations(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID, locateType, reload)
      .thenApply(RegionLocations::getDefaultRegionLocation);
  }

  @Test
  public void testNoTable() throws InterruptedException {
    for (RegionLocateType locateType : RegionLocateType.values()) {
      try {
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get();
      } catch (ExecutionException e) {
        assertThat(e.getCause(), instanceOf(TableNotFoundException.class));
      }
    }
  }

  @Test
  public void testDisableTable() throws IOException, InterruptedException {
    createSingleRegionTable();
    TEST_UTIL.getAdmin().disableTable(TABLE_NAME);
    for (RegionLocateType locateType : RegionLocateType.values()) {
      try {
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get();
      } catch (ExecutionException e) {
        assertThat(e.getCause(), instanceOf(TableNotFoundException.class));
      }
    }
  }

  private void assertLocEquals(byte[] startKey, byte[] endKey, ServerName serverName,
      HRegionLocation loc) {
    RegionInfo info = loc.getRegion();
    assertEquals(TABLE_NAME, info.getTable());
    assertArrayEquals(startKey, info.getStartKey());
    assertArrayEquals(endKey, info.getEndKey());
    assertEquals(serverName, loc.getServerName());
  }

  @Test
  public void testSingleRegionTable() throws IOException, InterruptedException, ExecutionException {
    createSingleRegionTable();
    ServerName serverName = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName();
    for (RegionLocateType locateType : RegionLocateType.values()) {
      assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, serverName,
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get());
    }
    byte[] randKey = new byte[ThreadLocalRandom.current().nextInt(128)];
    ThreadLocalRandom.current().nextBytes(randKey);
    for (RegionLocateType locateType : RegionLocateType.values()) {
      assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, serverName,
        getDefaultRegionLocation(TABLE_NAME, randKey, locateType, false).get());
    }
  }

  private void createMultiRegionTable() throws IOException, InterruptedException {
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, SPLIT_KEYS);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  private static byte[][] getStartKeys() {
    byte[][] startKeys = new byte[SPLIT_KEYS.length + 1][];
    startKeys[0] = EMPTY_START_ROW;
    System.arraycopy(SPLIT_KEYS, 0, startKeys, 1, SPLIT_KEYS.length);
    return startKeys;
  }

  private static byte[][] getEndKeys() {
    byte[][] endKeys = Arrays.copyOf(SPLIT_KEYS, SPLIT_KEYS.length + 1);
    endKeys[endKeys.length - 1] = EMPTY_START_ROW;
    return endKeys;
  }

  private ServerName[] getLocations(byte[][] startKeys) {
    ServerName[] serverNames = new ServerName[startKeys.length];
    TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream().map(t -> t.getRegionServer())
      .forEach(rs -> {
        rs.getRegions(TABLE_NAME).forEach(r -> {
          serverNames[Arrays.binarySearch(startKeys, r.getRegionInfo().getStartKey(),
            Bytes::compareTo)] = rs.getServerName();
        });
      });
    return serverNames;
  }

  @Test
  public void testMultiRegionTable() throws IOException, InterruptedException {
    createMultiRegionTable();
    byte[][] startKeys = getStartKeys();
    ServerName[] serverNames = getLocations(startKeys);
    IntStream.range(0, 2).forEach(n -> IntStream.range(0, startKeys.length).forEach(i -> {
      try {
        assertLocEquals(startKeys[i], i == startKeys.length - 1 ? EMPTY_END_ROW : startKeys[i + 1],
          serverNames[i],
          getDefaultRegionLocation(TABLE_NAME, startKeys[i], RegionLocateType.CURRENT, false)
            .get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }));

    LOCATOR.clearCache(TABLE_NAME);
    IntStream.range(0, 2).forEach(n -> IntStream.range(0, startKeys.length).forEach(i -> {
      try {
        assertLocEquals(startKeys[i], i == startKeys.length - 1 ? EMPTY_END_ROW : startKeys[i + 1],
          serverNames[i],
          getDefaultRegionLocation(TABLE_NAME, startKeys[i], RegionLocateType.AFTER, false).get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }));

    LOCATOR.clearCache(TABLE_NAME);
    byte[][] endKeys = getEndKeys();
    IntStream.range(0, 2).forEach(
      n -> IntStream.range(0, endKeys.length).map(i -> endKeys.length - 1 - i).forEach(i -> {
        try {
          assertLocEquals(i == 0 ? EMPTY_START_ROW : endKeys[i - 1], endKeys[i], serverNames[i],
            getDefaultRegionLocation(TABLE_NAME, endKeys[i], RegionLocateType.BEFORE, false).get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }));
  }

  @Test
  public void testRegionMove() throws IOException, InterruptedException, ExecutionException {
    createSingleRegionTable();
    ServerName serverName = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName();
    HRegionLocation loc =
      getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, RegionLocateType.CURRENT, false).get();
    assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, serverName, loc);
    ServerName newServerName = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> !sn.equals(serverName)).findAny()
      .get();

    TEST_UTIL.getAdmin().move(Bytes.toBytes(loc.getRegion().getEncodedName()), newServerName);
    while (!TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName()
      .equals(newServerName)) {
      Thread.sleep(100);
    }
    // Should be same as it is in cache
    assertSame(loc,
      getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, RegionLocateType.CURRENT, false).get());
    LOCATOR.updateCachedLocationOnError(loc, null);
    // null error will not trigger a cache cleanup
    assertSame(loc,
      getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, RegionLocateType.CURRENT, false).get());
    LOCATOR.updateCachedLocationOnError(loc, new NotServingRegionException());
    assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, newServerName,
      getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, RegionLocateType.CURRENT, false).get());
  }

  // usually locate after will return the same result, so we add a test to make it return different
  // result.
  @Test
  public void testLocateAfter() throws IOException, InterruptedException, ExecutionException {
    byte[] row = Bytes.toBytes("1");
    byte[] splitKey = Arrays.copyOf(row, 2);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, new byte[][] { splitKey });
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    HRegionLocation currentLoc =
      getDefaultRegionLocation(TABLE_NAME, row, RegionLocateType.CURRENT, false).get();
    ServerName currentServerName = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName();
    assertLocEquals(EMPTY_START_ROW, splitKey, currentServerName, currentLoc);

    HRegionLocation afterLoc =
      getDefaultRegionLocation(TABLE_NAME, row, RegionLocateType.AFTER, false).get();
    ServerName afterServerName =
      TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream().map(t -> t.getRegionServer())
        .filter(rs -> rs.getRegions(TABLE_NAME).stream()
          .anyMatch(r -> Bytes.equals(splitKey, r.getRegionInfo().getStartKey())))
        .findAny().get().getServerName();
    assertLocEquals(splitKey, EMPTY_END_ROW, afterServerName, afterLoc);

    assertSame(afterLoc,
      getDefaultRegionLocation(TABLE_NAME, row, RegionLocateType.AFTER, false).get());
  }

  // For HBASE-17402
  @Test
  public void testConcurrentLocate() throws IOException, InterruptedException, ExecutionException {
    createMultiRegionTable();
    byte[][] startKeys = getStartKeys();
    byte[][] endKeys = getEndKeys();
    ServerName[] serverNames = getLocations(startKeys);
    for (int i = 0; i < 100; i++) {
      LOCATOR.clearCache(TABLE_NAME);
      List<CompletableFuture<HRegionLocation>> futures =
        IntStream.range(0, 1000).mapToObj(n -> String.format("%03d", n)).map(s -> Bytes.toBytes(s))
          .map(r -> getDefaultRegionLocation(TABLE_NAME, r, RegionLocateType.CURRENT, false))
          .collect(toList());
      for (int j = 0; j < 1000; j++) {
        int index = Math.min(8, j / 111);
        assertLocEquals(startKeys[index], endKeys[index], serverNames[index], futures.get(j).get());
      }
    }
  }

  @Test
  public void testReload() throws Exception {
    createSingleRegionTable();
    ServerName serverName = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName();
    for (RegionLocateType locateType : RegionLocateType.values()) {
      assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, serverName,
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get());
    }
    ServerName newServerName = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> !sn.equals(serverName)).findAny()
      .get();
    Admin admin = TEST_UTIL.getAdmin();
    RegionInfo region = admin.getRegions(TABLE_NAME).stream().findAny().get();
    admin.move(region.getEncodedNameAsBytes(), newServerName);
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        ServerName newServerName = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME).getServerName();
        return newServerName != null && !newServerName.equals(serverName);
      }

      @Override
      public String explainFailure() throws Exception {
        return region.getRegionNameAsString() + " is still on " + serverName;
      }

    });
    // The cached location will not change
    for (RegionLocateType locateType : RegionLocateType.values()) {
      assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, serverName,
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get());
    }
    // should get the new location when reload = true
    assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, newServerName,
      getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, RegionLocateType.CURRENT, true).get());
    // the cached location should be replaced
    for (RegionLocateType locateType : RegionLocateType.values()) {
      assertLocEquals(EMPTY_START_ROW, EMPTY_END_ROW, newServerName,
        getDefaultRegionLocation(TABLE_NAME, EMPTY_START_ROW, locateType, false).get());
    }
  }

  // Testcase for HBASE-20822
  @Test
  public void testLocateBeforeLastRegion()
      throws IOException, InterruptedException, ExecutionException {
    createMultiRegionTable();
    getDefaultRegionLocation(TABLE_NAME, SPLIT_KEYS[0], RegionLocateType.CURRENT, false).join();
    HRegionLocation loc =
      getDefaultRegionLocation(TABLE_NAME, EMPTY_END_ROW, RegionLocateType.BEFORE, false).get();
    // should locate to the last region
    assertArrayEquals(loc.getRegion().getEndKey(), EMPTY_END_ROW);
  }

  @Test
  public void testRegionReplicas() throws Exception {
    TEST_UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(3).build());
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
    testLocator(TEST_UTIL, TABLE_NAME, new Locator() {

      @Override
      public void updateCachedLocationOnError(HRegionLocation loc, Throwable error)
          throws Exception {
        LOCATOR.updateCachedLocationOnError(loc, error);
      }

      @Override
      public RegionLocations getRegionLocations(TableName tableName, int replicaId, boolean reload)
          throws Exception {
        return LOCATOR.getRegionLocations(tableName, EMPTY_START_ROW, replicaId,
          RegionLocateType.CURRENT, reload).get();
      }
    });
  }

  // Testcase for HBASE-21961
  @Test
  public void testLocateBeforeInOnlyRegion() throws IOException, InterruptedException {
    createSingleRegionTable();
    HRegionLocation loc =
      getDefaultRegionLocation(TABLE_NAME, Bytes.toBytes(1), RegionLocateType.BEFORE, false).join();
    // should locate to the only region
    assertArrayEquals(loc.getRegion().getStartKey(), EMPTY_START_ROW);
    assertArrayEquals(loc.getRegion().getEndKey(), EMPTY_END_ROW);
  }
}
