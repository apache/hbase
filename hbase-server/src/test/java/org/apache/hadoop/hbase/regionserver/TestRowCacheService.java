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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowCacheService {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheService.class);

  @Test
  public void testBarrier() throws IOException {
    // Mocking dependencies to create RowCacheService instance
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
    Mockito.when(td.isRowCacheEnabled()).thenReturn(true);
    Mockito.when(td.getColumnFamilies()).thenReturn(new ColumnFamilyDescriptor[] { cfd });

    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Scan scan = new Scan(get);

    RegionScannerImpl regionScanner = Mockito.mock(RegionScannerImpl.class);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HConstants.ROW_CACHE_SIZE_KEY, 0.01f);
    HRegion region = Mockito.mock(HRegion.class);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Mockito.when(region.getStores()).thenReturn(stores);
    Mockito.when(region.getScanner(scan)).thenReturn(regionScanner);
    Mockito.when(region.getReadOnlyConfiguration()).thenReturn(conf);

    RpcCallContext context = Mockito.mock(RpcCallContext.class);
    Mockito.when(context.getBlockBytesScanned()).thenReturn(1L);

    RowCacheKey key = new RowCacheKey(region, rowKey);
    List<Cell> results = new ArrayList<>();
    results.add(KeyValueTestUtil.create("row", "CF", "q1", 1, "v1"));

    // Initialize RowCacheService
    RowCacheService rowCacheService = new RowCacheService(conf);
    RowCache rowCache = rowCacheService.getRowCache();

    // Verify that row cache populated before creating a row level barrier
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNotNull(rowCache.getRow(key, true));
    assertNull(rowCacheService.getRowLevelBarrier(key));

    // Evict the row cache
    rowCache.evictRow(key);
    assertNull(rowCache.getRow(key, true));

    // Create a row level barrier for the row key
    rowCacheService.createRowLevelBarrier(key);
    assertEquals(1, rowCacheService.getRowLevelBarrier(key).get());

    // Verify that no row cache populated after creating a row level barrier
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNull(rowCache.getRow(key, true));

    // Remove the row level barrier
    rowCacheService.removeRowLevelBarrier(key);
    assertNull(rowCacheService.getRowLevelBarrier(key));

    // Verify that row cache populated before creating a table level barrier
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNotNull(rowCache.getRow(key, true));
    assertNull(rowCacheService.getRegionLevelBarrier(region));

    // Evict the row cache
    rowCache.evictRow(key);
    assertNull(rowCache.getRow(key, true));

    // Create a table level barrier for the row key
    rowCacheService.createRegionLevelBarrier(region);
    assertEquals(1, rowCacheService.getRegionLevelBarrier(region).get());

    // Verify that no row cache populated after creating a table level barrier
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNull(rowCache.getRow(key, true));

    // Remove the table level barrier
    rowCacheService.removeTableLevelBarrier(region);
    assertNull(rowCacheService.getRegionLevelBarrier(region));
  }

  @Test
  public void testMutate() throws IOException, ServiceException {
    // Mocking RowCacheService and its dependencies
    TableDescriptor tableDescriptor = Mockito.mock(TableDescriptor.class);
    Mockito.when(tableDescriptor.isRowCacheEnabled()).thenReturn(true);

    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo.getEncodedName()).thenReturn("region1");

    HRegion region = Mockito.mock(HRegion.class);
    Mockito.when(region.getTableDescriptor()).thenReturn(tableDescriptor);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getBlockCache()).thenReturn(Mockito.mock(BlockCache.class));

    RSRpcServices rsRpcServices = Mockito.mock(RSRpcServices.class);
    Mockito.when(rsRpcServices.getRegion(Mockito.any())).thenReturn(region);

    MutationProto mutationProto = MutationProto.newBuilder().build();

    OperationQuota operationQuota = Mockito.mock(OperationQuota.class);

    CellScanner cellScanner = Mockito.mock(CellScanner.class);

    ActivePolicyEnforcement activePolicyEnforcement = Mockito.mock(ActivePolicyEnforcement.class);

    RpcCallContext rpcCallContext = Mockito.mock(RpcCallContext.class);

    CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder("row".getBytes())
      .ifEquals("CF".getBytes(), "q1".getBytes(), "v1".getBytes()).build(new Put("row".getBytes()));

    Put put1 = new Put("row1".getBytes());
    put1.addColumn("CF".getBytes(), "q1".getBytes(), "v1".getBytes());
    Put put2 = new Put("row1".getBytes());
    put2.addColumn("CF".getBytes(), "q1".getBytes(), "v1".getBytes());
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(put1);
    mutations.add(put2);

    Mutation[] mutationArray = new Mutation[mutations.size()];
    mutations.toArray(mutationArray);

    RowCacheService rowCacheService;
    InOrder inOrder;

    // Mutate
    rowCacheService = Mockito.mock(RowCacheService.class);
    Mockito.when(rowCacheService.mutate(rsRpcServices, region, mutationProto, operationQuota,
      cellScanner, 0, activePolicyEnforcement, rpcCallContext)).thenCallRealMethod();
    inOrder = Mockito.inOrder(rowCacheService);
    rowCacheService.mutate(rsRpcServices, region, mutationProto, operationQuota, cellScanner, 0,
      activePolicyEnforcement, rpcCallContext);
    // Verify the sequence of method calls
    inOrder.verify(rowCacheService, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // CheckAndMutate
    rowCacheService = Mockito.mock(RowCacheService.class);
    Mockito.when(rowCacheService.checkAndMutate(region, checkAndMutate, 0, 0)).thenCallRealMethod();
    inOrder = Mockito.inOrder(rowCacheService);
    rowCacheService.checkAndMutate(region, checkAndMutate, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCacheService, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // RowMutations
    rowCacheService = Mockito.mock(RowCacheService.class);
    Mockito.when(rowCacheService.checkAndMutate(region, mutations, checkAndMutate, 0, 0))
      .thenCallRealMethod();
    inOrder = Mockito.inOrder(rowCacheService);
    rowCacheService.checkAndMutate(region, mutations, checkAndMutate, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCacheService, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Batch
    rowCacheService = Mockito.mock(RowCacheService.class);
    Mockito.when(rowCacheService.batchMutate(region, mutationArray, true, 0, 0))
      .thenCallRealMethod();
    inOrder = Mockito.inOrder(rowCacheService);
    rowCacheService.batchMutate(region, mutationArray, true, 0, 0);
    // Verify the sequence of method calls
    inOrder.verify(rowCacheService, Mockito.times(1)).createRowLevelBarrier(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).evictRow(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).execute(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).removeRowLevelBarrier(Mockito.any());

    // Bulkload
    HBaseProtos.RegionSpecifier regionSpecifier = HBaseProtos.RegionSpecifier.newBuilder()
      .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
      .setValue(ByteString.copyFrom("region".getBytes())).build();
    ClientProtos.BulkLoadHFileRequest bulkLoadRequest =
      ClientProtos.BulkLoadHFileRequest.newBuilder().setRegion(regionSpecifier).build();
    rowCacheService = Mockito.mock(RowCacheService.class);
    Mockito.when(rowCacheService.bulkLoadHFile(rsRpcServices, bulkLoadRequest))
      .thenCallRealMethod();
    inOrder = Mockito.inOrder(rowCacheService);
    rowCacheService.bulkLoadHFile(rsRpcServices, bulkLoadRequest);
    // Verify the sequence of method calls
    inOrder.verify(rowCacheService, Mockito.times(1)).createRegionLevelBarrier(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).increaseRowCacheSeqNum(Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).bulkLoad(Mockito.any(), Mockito.any());
    inOrder.verify(rowCacheService, Mockito.times(1)).removeTableLevelBarrier(Mockito.any());
  }

  @Test
  public void testCaching() throws IOException {
    // Mocking dependencies to create RowCacheService instance
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
    Mockito.when(td.isRowCacheEnabled()).thenReturn(true);
    Mockito.when(td.getColumnFamilies()).thenReturn(new ColumnFamilyDescriptor[] { cfd });

    RpcCallContext context = Mockito.mock(RpcCallContext.class);
    Mockito.when(context.getBlockBytesScanned()).thenReturn(1L);

    byte[] rowKey = "row".getBytes();
    RegionScannerImpl regionScanner = Mockito.mock(RegionScannerImpl.class);

    Get get = new Get(rowKey);
    Scan scan = new Scan(get);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HConstants.ROW_CACHE_SIZE_KEY, 0.01f);
    HRegion region = Mockito.mock(HRegion.class);
    Mockito.when(region.getRegionInfo()).thenReturn(regionInfo);
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Mockito.when(region.getStores()).thenReturn(stores);
    Mockito.when(region.getScanner(scan)).thenReturn(regionScanner);
    Mockito.when(region.getReadOnlyConfiguration()).thenReturn(conf);

    RowCacheKey key = new RowCacheKey(region, rowKey);
    List<Cell> results = new ArrayList<>();
    results.add(KeyValueTestUtil.create("row", "CF", "q1", 1, "v1"));

    // Initialize RowCacheService
    RowCacheService rowCacheService = new RowCacheService(conf);
    RowCache rowCache = rowCacheService.getRowCache();

    // Verify that row cache populated with caching=false
    // This should be called first not to populate the row cache
    get.setCacheBlocks(false);
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNull(rowCache.getRow(key, true));
    assertNull(rowCache.getRow(key, false));

    // Verify that row cache populated with caching=true
    get.setCacheBlocks(true);
    rowCacheService.getScanner(region, get, scan, results, context);
    assertNotNull(rowCache.getRow(key, true));
    assertNull(rowCache.getRow(key, false));
  }
}
