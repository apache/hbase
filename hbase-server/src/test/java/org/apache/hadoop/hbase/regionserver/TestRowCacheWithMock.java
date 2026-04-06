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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowCacheWithMock {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheWithMock.class);

  @Test
  public void testBarrier() throws IOException {
    // Mocking dependencies to create rowCache instance
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo.getEncodedName()).thenReturn("region1");
    TableName tableName = TableName.valueOf("table1");
    Mockito.when(regionInfo.getTable()).thenReturn(tableName);

    List<HStore> stores = new ArrayList<>();
    HStore hStore = Mockito.mock(HStore.class);
    Mockito.when(hStore.getStorefilesCount()).thenReturn(2);
    stores.add(hStore);

    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder("CF1".getBytes()).build();
    TableDescriptor td = Mockito.mock(TableDescriptor.class);
    Mockito.when(td.getColumnFamilies()).thenReturn(new ColumnFamilyDescriptor[] { cfd });

    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Scan scan = new Scan(get);
    List<Cell> results = new ArrayList<>();

    RegionScannerImpl regionScanner = Mockito.mock(RegionScannerImpl.class);

    RpcCallContext context = Mockito.mock(RpcCallContext.class);
    Mockito.when(context.getBlockBytesScanned()).thenReturn(1L);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HConstants.ROW_CACHE_SIZE_KEY, 0.01f);

    RowCache rowCache = new RowCache(conf);

    HRegion region = Mockito.mock(HRegion.class);
    Mockito.doCallRealMethod().when(region).setRowCache(Mockito.any());
    region.setRowCache(rowCache);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Mockito.when(region.getStores()).thenReturn(stores);
    Mockito.when(region.getScanner(scan)).thenReturn(regionScanner);
    Mockito.when(region.getReadOnlyConfiguration()).thenReturn(conf);
    Mockito.when(region.isRowCacheEnabled()).thenReturn(true);
    Mockito.when(region.getScannerWithResults(get, scan, results, context)).thenCallRealMethod();

    RowCacheKey key = new RowCacheKey(region, rowKey);
    results.add(KeyValueTestUtil.create("row", "CF", "q1", 1, "v1"));

    // Verify that row cache populated before creating a row level barrier
    region.getScannerWithResults(get, scan, results, context);
    assertNotNull(rowCache.getRow(key));
    assertNull(rowCache.getRowLevelBarrier(key));

    // Evict the row cache
    rowCache.evictRow(key);
    assertNull(rowCache.getRow(key));

    // Create a row level barrier for the row key
    rowCache.createRowLevelBarrier(key);
    assertEquals(1, rowCache.getRowLevelBarrier(key).get());

    // Verify that no row cache populated after creating a row level barrier
    region.getScannerWithResults(get, scan, results, context);
    assertNull(rowCache.getRow(key));

    // Remove the row level barrier
    rowCache.removeRowLevelBarrier(key);
    assertNull(rowCache.getRowLevelBarrier(key));

    // Verify that row cache populated before creating a table level barrier
    region.getScannerWithResults(get, scan, results, context);
    assertNotNull(rowCache.getRow(key));
    assertNull(rowCache.getRegionLevelBarrier(region));

    // Evict the row cache
    rowCache.evictRow(key);
    assertNull(rowCache.getRow(key));

    // Create a table level barrier for the row key
    rowCache.createRegionLevelBarrier(region);
    assertEquals(1, rowCache.getRegionLevelBarrier(region).get());

    // Verify that no row cache populated after creating a table level barrier
    region.getScannerWithResults(get, scan, results, context);
    assertNull(rowCache.getRow(key));

    // Remove the table level barrier
    rowCache.removeRegionLevelBarrier(region);
    assertNull(rowCache.getRegionLevelBarrier(region));
  }

  @Test
  public void testMutate() throws IOException, ServiceException {
    // Mocking RowCache and its dependencies
    TableDescriptor tableDescriptor = Mockito.mock(TableDescriptor.class);

    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo.getEncodedName()).thenReturn("region1");

    RowCache rowCache = Mockito.mock(RowCache.class);

    RegionServerServices rss = Mockito.mock(RegionServerServices.class);
    Mockito.when(rss.getRowCache()).thenReturn(rowCache);

    HRegion region = Mockito.mock(HRegion.class);
    Mockito.doCallRealMethod().when(region).setRowCache(Mockito.any());
    region.setRowCache(rowCache);
    Mockito.when(region.getTableDescriptor()).thenReturn(tableDescriptor);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getBlockCache()).thenReturn(Mockito.mock(BlockCache.class));
    Mockito.when(region.isRowCacheEnabled()).thenReturn(true);
    Mockito.when(region.getRegionServerServices()).thenReturn(rss);

    RSRpcServices rsRpcServices = Mockito.mock(RSRpcServices.class);
    Mockito.when(rsRpcServices.getRegion(Mockito.any())).thenReturn(region);

    RpcController rpcController = Mockito.mock(RpcController.class);

    CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder("row".getBytes())
      .ifEquals("CF".getBytes(), "q1".getBytes(), "v1".getBytes()).build(new Put("row".getBytes()));

    Put put1 = new Put("row1".getBytes());
    put1.addColumn("CF".getBytes(), "q1".getBytes(), "v1".getBytes());
    Put put2 = new Put("row1".getBytes());
    put2.addColumn("CF".getBytes(), "q1".getBytes(), "v1".getBytes());
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(put1);
    mutations.add(put2);

    Delete del = new Delete("row1".getBytes());
    Append append = new Append("row1".getBytes());
    append.addColumn("CF".getBytes(), "q1".getBytes(), "v1".getBytes());
    Increment increment = new Increment("row1".getBytes());
    increment.addColumn("CF".getBytes(), "q1".getBytes(), 1L);

    Mutation[] mutationArray = new Mutation[mutations.size()];
    mutations.toArray(mutationArray);

    // rowCache.mutateWithRowCacheBarrier must run real code so internal calls are recorded
    Mockito.doCallRealMethod().when(rowCache).mutateWithRowCacheBarrier(Mockito.any(HRegion.class),
      Mockito.any(byte[].class), Mockito.any());
    Mockito.doCallRealMethod().when(rowCache).mutateWithRowCacheBarrier(Mockito.any(HRegion.class),
      Mockito.anyList(), Mockito.any());

    InOrder inOrder;

    // Put
    Mockito.doAnswer(invocation -> {
      Put arg = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, arg.getRow(), () -> null);
      return null;
    }).when(region).put(put1);
    Mockito.clearInvocations(rowCache);
    inOrder = Mockito.inOrder(rowCache);
    region.put(put1);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Delete
    Mockito.doAnswer(invocation -> {
      Delete arg = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, arg.getRow(), () -> null);
      return null;
    }).when(region).delete(del);
    inOrder = Mockito.inOrder(rowCache);
    Mockito.clearInvocations(rowCache);
    region.delete(del);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Append
    Mockito.doAnswer(invocation -> {
      Append arg = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, arg.getRow(), () -> null);
      return null;
    }).when(region).append(append);
    inOrder = Mockito.inOrder(rowCache);
    Mockito.clearInvocations(rowCache);
    region.append(append);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Increment
    Mockito.doAnswer(invocation -> {
      Increment arg = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, arg.getRow(), () -> null);
      return null;
    }).when(region).increment(increment);
    inOrder = Mockito.inOrder(rowCache);
    Mockito.clearInvocations(rowCache);
    region.increment(increment);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // CheckAndMutate
    Mockito.doAnswer(invocation -> {
      CheckAndMutate c = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, c.getRow(), () -> null);
      return null;
    }).when(region).checkAndMutate(Mockito.any(CheckAndMutate.class), Mockito.anyLong(),
      Mockito.anyLong());
    Mockito.clearInvocations(rowCache);
    inOrder = Mockito.inOrder(rowCache);
    region.checkAndMutate(checkAndMutate, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // RowMutations
    Mockito.doAnswer(invocation -> {
      List<Mutation> muts = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, muts, () -> null);
      return null;
    }).when(region).checkAndMutate(Mockito.anyList(), Mockito.any(CheckAndMutate.class),
      Mockito.anyLong(), Mockito.anyLong());
    Mockito.clearInvocations(rowCache);
    inOrder = Mockito.inOrder(rowCache);
    region.checkAndMutate(mutations, checkAndMutate, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Batch
    Mockito.doAnswer(invocation -> {
      Mutation[] muts = invocation.getArgument(0);
      rowCache.mutateWithRowCacheBarrier(region, Arrays.asList(muts), () -> null);
      return null;
    }).when(region).batchMutate(Mockito.any(Mutation[].class), Mockito.anyBoolean(),
      Mockito.anyLong(), Mockito.anyLong());
    Mockito.clearInvocations(rowCache);
    inOrder = Mockito.inOrder(rowCache);
    region.batchMutate(mutationArray, true, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Bulkload
    HBaseProtos.RegionSpecifier regionSpecifier = HBaseProtos.RegionSpecifier.newBuilder()
      .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
      .setValue(ByteString.copyFrom("region".getBytes())).build();
    ClientProtos.BulkLoadHFileRequest bulkLoadRequest =
      ClientProtos.BulkLoadHFileRequest.newBuilder().setRegion(regionSpecifier).build();
    Mockito.doCallRealMethod().when(rsRpcServices).bulkLoadHFile(rpcController, bulkLoadRequest);
    Mockito.clearInvocations(rowCache);
    inOrder = Mockito.inOrder(rowCache);
    rsRpcServices.bulkLoadHFile(rpcController, bulkLoadRequest);
    // Verify the sequence of method calls
    inOrder.verify(rowCache, Mockito.times(1)).createRegionLevelBarrier(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).increaseRowCacheSeqNum(Mockito.any());
    inOrder.verify(rowCache, Mockito.times(1)).removeRegionLevelBarrier(Mockito.any());
  }

  @Test
  public void testCaching() throws IOException {
    // Mocking dependencies to create RowCache instance
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo.getEncodedName()).thenReturn("region1");
    TableName tableName = TableName.valueOf("table1");
    Mockito.when(regionInfo.getTable()).thenReturn(tableName);

    List<HStore> stores = new ArrayList<>();
    HStore hStore = Mockito.mock(HStore.class);
    Mockito.when(hStore.getStorefilesCount()).thenReturn(2);
    stores.add(hStore);

    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder("CF1".getBytes()).build();
    TableDescriptor td = Mockito.mock(TableDescriptor.class);
    Mockito.when(td.getColumnFamilies()).thenReturn(new ColumnFamilyDescriptor[] { cfd });

    RpcCallContext context = Mockito.mock(RpcCallContext.class);
    Mockito.when(context.getBlockBytesScanned()).thenReturn(1L);

    byte[] rowKey = "row".getBytes();
    RegionScannerImpl regionScanner = Mockito.mock(RegionScannerImpl.class);

    Get get = new Get(rowKey);
    Scan scan = new Scan(get);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HConstants.ROW_CACHE_SIZE_KEY, 0.01f);
    RowCache rowCache = new RowCache(conf);

    HRegion region = Mockito.mock(HRegion.class);
    Mockito.doCallRealMethod().when(region).setRowCache(Mockito.any());
    region.setRowCache(rowCache);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Mockito.when(region.getStores()).thenReturn(stores);
    Mockito.when(region.getScanner(scan)).thenReturn(regionScanner);
    Mockito.when(region.getReadOnlyConfiguration()).thenReturn(conf);
    Mockito.when(region.isRowCacheEnabled()).thenReturn(true);
    Mockito.when(region.getScannerWithResults(Mockito.any(Get.class), Mockito.any(Scan.class),
      Mockito.anyList(), Mockito.any())).thenCallRealMethod();

    RowCacheKey key = new RowCacheKey(region, rowKey);
    List<Cell> results = new ArrayList<>();
    results.add(KeyValueTestUtil.create("row", "CF", "q1", 1, "v1"));

    // Verify that row cache populated with caching=false
    // This should be called first not to populate the row cache
    get.setCacheBlocks(false);
    region.getScannerWithResults(get, scan, results, context);
    assertNull(rowCache.getRow(key));
    assertNull(rowCache.getRow(key));

    // Verify that row cache populated with caching=true
    get.setCacheBlocks(true);
    region.getScannerWithResults(get, scan, results, context);
    assertNotNull(rowCache.getRow(key, true));
    assertNull(rowCache.getRow(key, false));
  }
}
