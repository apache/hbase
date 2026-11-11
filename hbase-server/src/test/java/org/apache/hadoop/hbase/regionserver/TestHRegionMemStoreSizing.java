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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opentest4j.MultipleFailuresError;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestHRegionMemStoreSizing {

  private HRegion region;
  private FlushRequester flushRequester;

  @BeforeEach
  public void setUp() {
    Configuration conf = new Configuration(false);
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 55);
    conf.setLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 2);
    TableName tableName = TableName.valueOf("testMemStoreSizing");
    HRegionFileSystem fs = mock(HRegionFileSystem.class);
    when(fs.getRegionInfo()).thenReturn(RegionInfoBuilder.newBuilder(tableName).build());
    region =
      new HRegion(fs, null, conf, TableDescriptorBuilder.newBuilder(tableName).build(), null);
    RegionServerServices services = mock(RegionServerServices.class);
    flushRequester = mock(FlushRequester.class);
    when(services.getFlushRequester()).thenReturn(flushRequester);
    region.rsServices = services;
  }

  @Test
  public void testResourceCheckWithMixedSnapshot() throws Exception {
    // Real totals are 90, 15 and 100, all below the blocking limit of 110.
    mixSnapshotAcrossFlushAndWrite(10, 75);
    assertDoesNotThrow(() -> invokeRegionMethod("checkResources"));
    verifyNoInteractions(flushRequester);
  }

  @Test
  public void testFlushCheckWithMixedSnapshot() throws Exception {
    // The mixed snapshot is 95, while the current total is 30 (below the flush size of 55).
    mixSnapshotAcrossFlushAndWrite(5, 10);
    invokeRegionMethod("requestFlushIfNeeded");
    verifyNoInteractions(flushRequester);
  }

  private void mixSnapshotAcrossFlushAndWrite(long heapDelta, long offHeapDelta) throws Exception {
    region.incMemStoreSize(0, 80, 10, 0);
    long oldHeapSize = region.getMemStoreHeapSize();
    region.decrMemStoreSize(new MemStoreSize(0, 70, 5, 0));
    region.incMemStoreSize(0, heapDelta, offHeapDelta, 0);

    // Model a reader that read heap before the flush and off-heap after the subsequent write.
    Field field = HRegion.class.getDeclaredField("memStoreSizing");
    field.setAccessible(true);
    MemStoreSizing sizing = spy((MemStoreSizing) field.get(region));
    doReturn(new MemStoreSize(0, oldHeapSize, region.getMemStoreOffHeapSize(), 0)).when(sizing)
      .getMemStoreSize();
    field.set(region, sizing);
  }

  @Test
  public void testSizeUpdatesAndThresholdBoundaries() throws Exception {
    region.incMemStoreSize(0, 40, 15, 0);
    invokeRegionMethod("requestFlushIfNeeded");
    verifyNoInteractions(flushRequester);

    // Moving bytes between heap and off-heap must not change the total.
    region.getRegionServicesForStores().addMemStoreSize(0, -20, 20, 0);
    invokeRegionMethod("requestFlushIfNeeded");
    verifyNoInteractions(flushRequester);

    region.incMemStoreSize(0, 0, 55, 0);
    assertDoesNotThrow(() -> invokeRegionMethod("checkResources"));
    invokeRegionMethod("requestFlushIfNeeded");
    verify(flushRequester).requestFlush(region, FlushLifeCycleTracker.DUMMY);

    region.incMemStoreSize(0, 1, 0, 0);
    InvocationTargetException error =
      assertThrows(InvocationTargetException.class, () -> invokeRegionMethod("checkResources"));
    assertInstanceOf(RegionTooBusyException.class, error.getCause());

    region.decrMemStoreSize(new MemStoreSize(0, 21, 90, 0));
    assertEquals(0, region.getMemStoreHeapSize());
    assertEquals(0, region.getMemStoreOffHeapSize());
    assertEquals(0, getMemStoreHeapAndOffHeapSize(region));
    assertMemStoreSizeConsistent(region);
    assertDoesNotThrow(() -> invokeRegionMethod("checkResources"));
  }

  @Test
  public void testNonZeroMixedSignDeltas() throws Exception {
    region.incMemStoreSize(0, 60, 40, 0);
    assertMemStoreSizeConsistent(region);

    region.getRegionServicesForStores().addMemStoreSize(0, -30, 10, 0);
    assertEquals(80, getMemStoreHeapAndOffHeapSize(region));
    assertMemStoreSizeConsistent(region);

    region.getRegionServicesForStores().addMemStoreSize(0, 20, -5, 0);
    assertEquals(95, getMemStoreHeapAndOffHeapSize(region));
    assertMemStoreSizeConsistent(region);

    region.decrMemStoreSize(new MemStoreSize(0, 50, 45, 0));
    assertEquals(0, getMemStoreHeapAndOffHeapSize(region));
    assertMemStoreSizeConsistent(region);
  }

  @ParameterizedTest(name = "offHeap={0}, policy={1}")
  @CsvSource({ "false, BASIC", "true, BASIC", "false, EAGER", "true, EAGER" })
  public void testAccountingAfterCompactionAndFlush(boolean offHeap, MemoryCompactionPolicy policy)
    throws Exception {
    HBaseTestingUtil util = new HBaseTestingUtil();
    ChunkCreator oldCreator = ChunkCreator.instance;
    boolean oldPoolDisabled = ChunkCreator.chunkPoolDisabled;
    ChunkCreator creator = null;
    HRegion localRegion = null;
    WAL wal = null;
    try {
      ChunkCreator.chunkPoolDisabled = false;
      creator = new ChunkCreator(MemStoreLAB.CHUNK_SIZE_DEFAULT, offHeap, 4 * 1024 * 1024, 1.0f, 0,
        null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      ChunkCreator.instance = creator;
      util.getConfiguration().setLong("hbase.regionserver.offheap.global.memstore.size",
        offHeap ? 4 : 0);
      byte[] family = Bytes.toBytes("f");
      TableName tableName = TableName.valueOf("testMemStoreAccounting");
      RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
      TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(family).setInMemoryCompaction(policy).build())
        .build();
      // Keep ownership of the WAL even if region initialization fails.
      wal = HBaseTestingUtil.createWal(util.getConfiguration(), util.getDataTestDir(), info);
      localRegion = util.createLocalHRegion(info, util.getConfiguration(), descriptor, wal);
      assertMemStoreSizeConsistent(localRegion);

      for (int i = 0; i < 100; i++) {
        localRegion
          .put(new Put(Bytes.toBytes(i % 25)).addColumn(family, family, i, new byte[1000 + i]));
      }
      assertMemStoreSizeConsistent(localRegion);
      if (offHeap) {
        assertTrue(localRegion.getMemStoreOffHeapSize() > 0);
      }

      CompactingMemStore memstore = (CompactingMemStore) localRegion.getStore(family).memstore;
      memstore.flushInMemory();
      util.waitFor(10000, () -> !memstore.isMemStoreFlushingInMemory());
      assertInstanceOf(CellChunkImmutableSegment.class,
        memstore.getImmutableSegments().getStoreSegments().get(0));
      assertEquals(policy == MemoryCompactionPolicy.EAGER ? 25 : 100,
        memstore.getImmutableSegments().getNumOfCells());
      assertMemStoreSizeConsistent(localRegion);

      localRegion.flush(true);
      assertEquals(0, localRegion.getMemStoreDataSize());
      assertEquals(MutableSegment.DEEP_OVERHEAD, getMemStoreHeapAndOffHeapSize(localRegion));
      assertMemStoreSizeConsistent(localRegion);
    } finally {
      cleanupTestResources(localRegion, wal, creator, util, oldCreator, oldPoolDisabled);
    }
    assertSame(oldCreator, ChunkCreator.instance);
    assertEquals(oldPoolDisabled, ChunkCreator.chunkPoolDisabled);
    assertTrue(getStatisticsExecutor(creator.getDataChunksPool()).isTerminated());
    assertTrue(getStatisticsExecutor(creator.getIndexChunksPool()).isTerminated());
  }

  @ParameterizedTest
  @CsvSource({ "false, false", "true, true" })
  public void testCleanupAfterRegionCloseFailure(boolean walCloseFails, boolean poolDisabled)
    throws Exception {
    ChunkCreator oldCreator = ChunkCreator.instance;
    boolean oldPoolDisabled = ChunkCreator.chunkPoolDisabled;
    ChunkCreator previousCreator = mock(ChunkCreator.class);
    ChunkCreator creator = null;
    HBaseTestingUtil util = mock(HBaseTestingUtil.class);
    when(util.cleanupTestDir()).thenReturn(true);
    try {
      ChunkCreator.chunkPoolDisabled = false;
      creator = new ChunkCreator(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 4 * 1024 * 1024, 1.0f, 0,
        null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      ChunkCreator.instance = creator;
      HRegion localRegion = mock(HRegion.class);
      WAL wal = mock(WAL.class);
      IOException regionFailure = new IOException("region close failed");
      IOException walFailure = new IOException("WAL close failed");
      doThrow(regionFailure).when(localRegion).close();
      if (walCloseFails) {
        doThrow(walFailure).when(wal).close();
      }
      ChunkCreator ownedCreator = creator;
      MultipleFailuresError error =
        assertThrows(MultipleFailuresError.class, () -> cleanupTestResources(localRegion, wal,
          ownedCreator, util, previousCreator, poolDisabled));
      assertEquals(walCloseFails ? List.of(regionFailure, walFailure) : List.of(regionFailure),
        error.getFailures());
      verify(wal).close();
      verify(util).cleanupTestDir();
      assertSame(previousCreator, ChunkCreator.instance);
      assertEquals(poolDisabled, ChunkCreator.chunkPoolDisabled);
      assertTrue(getStatisticsExecutor(creator.getDataChunksPool()).isTerminated());
      assertTrue(getStatisticsExecutor(creator.getIndexChunksPool()).isTerminated());
    } finally {
      cleanupTestResources(null, null, creator, util, oldCreator, oldPoolDisabled);
    }
  }

  private static void cleanupTestResources(HRegion localRegion, WAL wal, ChunkCreator creator,
    HBaseTestingUtil util, ChunkCreator oldCreator, boolean oldPoolDisabled) {
    try {
      // Attempt every cleanup action, retaining failures without skipping later resources.
      assertAll("test resource cleanup", () -> {
        if (localRegion != null) {
          localRegion.close();
        }
      }, () -> {
        if (wal != null) {
          wal.close();
        }
      }, () -> {
        if (creator != null) {
          shutdownStatisticsExecutor(creator.getDataChunksPool());
        }
      }, () -> {
        if (creator != null) {
          shutdownStatisticsExecutor(creator.getIndexChunksPool());
        }
      }, () -> {
        if (creator != null) {
          creator.clearChunksInPool();
          creator.clearChunkIds();
        }
      }, () -> assertTrue(util.cleanupTestDir(), "Failed to delete the test directory"));
    } finally {
      ChunkCreator.instance = oldCreator;
      ChunkCreator.chunkPoolDisabled = oldPoolDisabled;
    }
  }

  private static ExecutorService getStatisticsExecutor(Object chunkPool) throws Exception {
    Field field = chunkPool.getClass().getDeclaredField("scheduleThreadPool");
    field.setAccessible(true);
    return (ExecutorService) field.get(chunkPool);
  }

  private static void shutdownStatisticsExecutor(Object chunkPool) throws Exception {
    if (chunkPool != null) {
      // Clearing a ChunkCreator's chunks does not stop its statistics threads.
      ExecutorService executor = getStatisticsExecutor(chunkPool);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
        "Chunk pool statistics executor did not terminate");
    }
  }

  @Test
  public void testConcurrentSizeUpdates() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> updates = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        updates.add(executor.submit(() -> {
          for (int j = 0; j < 1000; j++) {
            region.incMemStoreSize(0, 1, 2, 0);
            region.decrMemStoreSize(new MemStoreSize(0, 1, 2, 0));
          }
        }));
      }
      for (Future<?> update : updates) {
        update.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
        "MemStore sizing executor did not terminate");
    }
    assertEquals(0, region.getMemStoreHeapSize());
    assertEquals(0, region.getMemStoreOffHeapSize());
    assertEquals(0, getMemStoreHeapAndOffHeapSize(region));
    assertMemStoreSizeConsistent(region);
    // Check both sides of the blocking boundary after all increments/decrements have balanced.
    region.incMemStoreSize(0, 55, 55, 0);
    assertDoesNotThrow(() -> invokeRegionMethod("checkResources"));
    region.incMemStoreSize(0, 0, 1, 0);
    InvocationTargetException error =
      assertThrows(InvocationTargetException.class, () -> invokeRegionMethod("checkResources"));
    assertInstanceOf(RegionTooBusyException.class, error.getCause());
  }

  private static long getMemStoreHeapAndOffHeapSize(HRegion region) throws Exception {
    Field field = HRegion.class.getDeclaredField("memStoreHeapAndOffHeapSize");
    field.setAccessible(true);
    return ((AtomicLong) field.get(region)).get();
  }

  private static void assertMemStoreSizeConsistent(HRegion region) throws Exception {
    assertEquals(region.getMemStoreHeapSize() + region.getMemStoreOffHeapSize(),
      getMemStoreHeapAndOffHeapSize(region));
  }

  private Object invokeRegionMethod(String name) throws Exception {
    Method method = HRegion.class.getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(region);
  }
}
