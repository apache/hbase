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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestMetaLocationCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaLocationCache.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private static ChoreService CHORE_SERVICE;

  private static byte[] SPLIT = Bytes.toBytes("a");

  private MasterServices master;

  private MetaLocationCache cache;

  @BeforeClass
  public static void setUpBeforeClass() {
    CONF.setInt(MetaLocationCache.SYNC_INTERVAL_SECONDS, 1);
    CHORE_SERVICE = new ChoreService("TestMetaLocationCache");
  }

  @AfterClass
  public static void tearDownAfterClass() {
    CHORE_SERVICE.shutdown();
  }

  @Before
  public void setUp() {
    master = mock(MasterServices.class);
    when(master.getConfiguration()).thenReturn(CONF);
    when(master.getChoreService()).thenReturn(CHORE_SERVICE);
    cache = new MetaLocationCache(master);
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.stop("test end");
    }
  }

  @Test
  public void testError() throws InterruptedException {
    AsyncClusterConnection conn = mock(AsyncClusterConnection.class);
    when(conn.getAllMetaRegionLocations(anyInt()))
      .thenReturn(FutureUtils.failedFuture(new RuntimeException("inject error")));
    when(master.getAsyncClusterConnection()).thenReturn(conn);
    Thread.sleep(2000);
    assertNull(cache.locateMeta(HConstants.EMPTY_BYTE_ARRAY, RegionLocateType.CURRENT));
    assertTrue(cache.getAllMetaRegionLocations(true).isEmpty());

    HRegionLocation loc =
      new HRegionLocation(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build(),
        ServerName.valueOf("localhost", 12345, System.currentTimeMillis()));
    when(conn.getAllMetaRegionLocations(anyInt()))
      .thenReturn(CompletableFuture.completedFuture(Arrays.asList(loc)));
    Thread.sleep(2000);
    List<HRegionLocation> list = cache.getAllMetaRegionLocations(false);
    assertEquals(1, list.size());
    assertEquals(loc, list.get(0));
  }

  private void prepareData() throws InterruptedException {
    AsyncClusterConnection conn = mock(AsyncClusterConnection.class);
    RegionInfo parent = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setSplit(true)
      .setOffline(true).build();
    RegionInfo daughter1 =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(SPLIT).build();
    RegionInfo daughter2 =
      RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(SPLIT).build();
    HRegionLocation parentLoc = new HRegionLocation(parent,
      ServerName.valueOf("127.0.0.1", 12345, System.currentTimeMillis()));
    HRegionLocation daughter1Loc = new HRegionLocation(daughter1,
      ServerName.valueOf("127.0.0.2", 12345, System.currentTimeMillis()));
    HRegionLocation daughter2Loc = new HRegionLocation(daughter2,
      ServerName.valueOf("127.0.0.3", 12345, System.currentTimeMillis()));
    when(conn.getAllMetaRegionLocations(anyInt())).thenReturn(
      CompletableFuture.completedFuture(Arrays.asList(parentLoc, daughter1Loc, daughter2Loc)));
    when(master.getAsyncClusterConnection()).thenReturn(conn);
    Thread.sleep(2000);
  }

  @Test
  public void testLocateMeta() throws InterruptedException {
    prepareData();
    RegionLocations locs = cache.locateMeta(SPLIT, RegionLocateType.BEFORE);
    assertEquals(1, locs.size());
    HRegionLocation loc = locs.getDefaultRegionLocation();
    assertArrayEquals(SPLIT, loc.getRegion().getEndKey());

    locs = cache.locateMeta(SPLIT, RegionLocateType.CURRENT);
    assertEquals(1, locs.size());
    loc = locs.getDefaultRegionLocation();
    assertArrayEquals(SPLIT, loc.getRegion().getStartKey());

    locs = cache.locateMeta(SPLIT, RegionLocateType.AFTER);
    assertEquals(1, locs.size());
    loc = locs.getDefaultRegionLocation();
    assertArrayEquals(SPLIT, loc.getRegion().getStartKey());
  }

  @Test
  public void testGetAllMetaRegionLocations() throws InterruptedException {
    prepareData();
    List<HRegionLocation> locs = cache.getAllMetaRegionLocations(false);
    assertEquals(3, locs.size());
    HRegionLocation loc = locs.get(0);
    assertTrue(loc.getRegion().isSplitParent());
    loc = locs.get(1);
    assertArrayEquals(SPLIT, loc.getRegion().getEndKey());
    loc = locs.get(2);
    assertArrayEquals(SPLIT, loc.getRegion().getStartKey());

    locs = cache.getAllMetaRegionLocations(true);
    assertEquals(2, locs.size());
    loc = locs.get(0);
    assertArrayEquals(SPLIT, loc.getRegion().getEndKey());
    loc = locs.get(1);
    assertArrayEquals(SPLIT, loc.getRegion().getStartKey());
  }
}
