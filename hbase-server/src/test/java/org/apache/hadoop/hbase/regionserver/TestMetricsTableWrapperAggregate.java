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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Tag(SmallTests.TAG)
@Tag(RegionServerTests.TAG)
public class TestMetricsTableWrapperAggregate {

  private static final TableName TABLE = TableName.valueOf("table1");

  @Test
  public void testAvgStoreFileAgeAccumulatesAcrossStores() throws IOException {
    // Two regions of the same table, three stores in total; per store: (count, avg, max, min).
    HStore storeA = getMockedStore("cf1", 2, 10.0, 15, 5);
    HStore storeB = getMockedStore("cf2", 3, 20.0, 30, 1);
    HStore storeC = getMockedStore("cf3", 5, 4.0, 8, 2);

    HRegion region1 = getMockedRegion(Lists.newArrayList(storeA, storeB));
    HRegion region2 = getMockedRegion(Lists.newArrayList(storeC));
    List<HRegion> regions = Lists.newArrayList(region1, region2);

    Configuration conf = HBaseConfiguration.create();
    // Long period so the scheduled task never fires; we drive the aggregation manually.
    conf.setLong(HConstants.REGIONSERVER_METRICS_PERIOD, 600 * 1000);

    HRegionServer regionServer = mock(HRegionServer.class);
    when(regionServer.getConfiguration()).thenReturn(conf);
    when(regionServer.getOnlineRegionsLocalContext()).thenReturn(regions);

    MetricsTableWrapperAggregateImpl wrapper = new MetricsTableWrapperAggregateImpl(regionServer);
    try {
      MetricsTableWrapperAggregateImpl.TableMetricsWrapperRunnable runnable =
        wrapper.new TableMetricsWrapperRunnable();
      runnable.run();

      String table = TABLE.getNameAsString();
      // avg = (10*2 + 20*3 + 4*5) / (2 + 3 + 5) = 100 / 10 = 10.
      assertEquals(10, wrapper.getAvgStoreFileAge(table));
      assertEquals(10, wrapper.getNumStoreFiles(table));
      assertEquals(30, wrapper.getMaxStoreFileAge(table));
      assertEquals(1, wrapper.getMinStoreFileAge(table));
      assertEquals(3, wrapper.getNumStores(table));
      assertEquals(2, wrapper.getNumRegions(table));
    } finally {
      wrapper.close();
    }
  }

  private HRegion getMockedRegion(List<HStore> stores) {
    TableDescriptor descriptor = mock(TableDescriptor.class);
    when(descriptor.getTableName()).thenReturn(TABLE);
    HRegion region = mock(HRegion.class);
    when(region.getTableDescriptor()).thenReturn(descriptor);
    when(region.getStores()).thenReturn(stores);
    return region;
  }

  private HStore getMockedStore(String family, int storeFileCount, double avgStoreFileAge,
    long maxStoreFileAge, long minStoreFileAge) {
    HStore store = mock(HStore.class);
    when(store.getColumnFamilyName()).thenReturn(family);
    when(store.getStorefilesCount()).thenReturn(storeFileCount);
    when(store.getAvgStoreFileAge()).thenReturn(OptionalDouble.of(avgStoreFileAge));
    when(store.getMaxStoreFileAge()).thenReturn(OptionalLong.of(maxStoreFileAge));
    when(store.getMinStoreFileAge()).thenReturn(OptionalLong.of(minStoreFileAge));
    when(store.getMemStoreSize()).thenReturn(mock(MemStoreSize.class));
    return store;
  }
}
