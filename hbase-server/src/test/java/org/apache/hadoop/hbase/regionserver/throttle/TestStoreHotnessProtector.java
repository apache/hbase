/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.apache.hadoop.hbase.regionserver.throttle.StoreHotnessProtector.PARALLEL_PREPARE_PUT_STORE_MULTIPLIER;
import static org.apache.hadoop.hbase.regionserver.throttle.StoreHotnessProtector.PARALLEL_PUT_STORE_THREADS_LIMIT;
import static org.apache.hadoop.hbase.regionserver.throttle.StoreHotnessProtector.PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_COUNT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestStoreHotnessProtector {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStoreHotnessProtector.class);

  @Test
  public void testPreparePutCounter() throws Exception {

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    Configuration conf = new Configuration();
    conf.setInt(PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_COUNT, 0);
    conf.setInt(PARALLEL_PUT_STORE_THREADS_LIMIT, 10);
    conf.setInt(PARALLEL_PREPARE_PUT_STORE_MULTIPLIER, 3);
    Region mockRegion = mock(Region.class);
    StoreHotnessProtector storeHotnessProtector = new StoreHotnessProtector(mockRegion, conf);

    Store mockStore1 = mock(Store.class);
    RegionInfo mockRegionInfo = mock(RegionInfo.class);
    byte[] family = "testF1".getBytes();

    when(mockRegion.getStore(family)).thenReturn(mockStore1);
    when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
    when(mockRegionInfo.getRegionNameAsString()).thenReturn("test_region_1");

    when(mockStore1.getCurrentParallelPutCount()).thenReturn(1);
    when(mockStore1.getColumnFamilyName()).thenReturn("test_Family_1");

    final Map<byte[], List<Cell>> familyMaps = new HashMap<>();
    familyMaps.put(family, Lists.newArrayList(mock(Cell.class), mock(Cell.class)));

    final AtomicReference<Exception> exception = new AtomicReference<>();

    // PreparePutCounter not access limit

    int threadCount = conf.getInt(PARALLEL_PUT_STORE_THREADS_LIMIT, 10) * conf
        .getInt(PARALLEL_PREPARE_PUT_STORE_MULTIPLIER, 3);
    CountDownLatch countDownLatch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      executorService.execute(() -> {
        try {
          storeHotnessProtector.start(familyMaps);
        } catch (RegionTooBusyException e) {
          e.printStackTrace();
          exception.set(e);
        } finally {
          countDownLatch.countDown();
        }
      });
    }

    countDownLatch.await(60, TimeUnit.SECONDS);
    //no exception
    Assert.assertEquals(exception.get(), null);
    Assert.assertEquals(storeHotnessProtector.getPreparePutToStoreMap().size(), 1);
    Assert.assertEquals(storeHotnessProtector.getPreparePutToStoreMap().get(family).get(),
        threadCount);

    // access limit

    try {
      storeHotnessProtector.start(familyMaps);
    } catch (RegionTooBusyException e) {
      e.printStackTrace();
      exception.set(e);
    }

    Assert.assertEquals(exception.get().getClass(), RegionTooBusyException.class);

    Assert.assertEquals(storeHotnessProtector.getPreparePutToStoreMap().size(), 1);
    // when access limit, counter will not changed.
    Assert.assertEquals(storeHotnessProtector.getPreparePutToStoreMap().get(family).get(),
        threadCount + 1);

    storeHotnessProtector.finish(familyMaps);
    Assert.assertEquals(storeHotnessProtector.getPreparePutToStoreMap().get(family).get(),
        threadCount);
  }

}