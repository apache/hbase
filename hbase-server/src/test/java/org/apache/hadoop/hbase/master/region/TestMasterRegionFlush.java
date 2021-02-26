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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionFlush {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionFlush.class);

  private Configuration conf;

  private HRegion region;

  private MasterRegionFlusherAndCompactor flusher;

  private AtomicInteger flushCalled;

  private AtomicLong memstoreHeapSize;

  private AtomicLong memstoreOffHeapSize;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    region = mock(HRegion.class);
    HStore store = mock(HStore.class);
    when(store.getStorefilesCount()).thenReturn(1);
    when(region.getStores()).thenReturn(Collections.singletonList(store));
    when(region.getRegionInfo())
      .thenReturn(RegionInfoBuilder.newBuilder(TableName.valueOf("hbase:local")).build());
    flushCalled = new AtomicInteger(0);
    memstoreHeapSize = new AtomicLong(0);
    memstoreOffHeapSize = new AtomicLong(0);
    when(region.getMemStoreHeapSize()).thenAnswer(invocation -> memstoreHeapSize.get());
    when(region.getMemStoreOffHeapSize()).thenAnswer(invocation -> memstoreOffHeapSize.get());
    when(region.flush(anyBoolean())).thenAnswer(invocation -> {
      assertTrue(invocation.getArgument(0));
      memstoreHeapSize.set(0);
      memstoreOffHeapSize.set(0);
      flushCalled.incrementAndGet();
      return null;
    });
  }

  @After
  public void tearDown() {
    if (flusher != null) {
      flusher.close();
      flusher = null;
    }
  }

  private void initFlusher(long flushSize, long flushPerChanges, long flushIntervalMs) {
    flusher = new MasterRegionFlusherAndCompactor(conf, new Abortable() {

      @Override
      public boolean isAborted() {
        return false;
      }

      @Override
      public void abort(String why, Throwable e) {
      }
    }, region, flushSize, flushPerChanges, flushIntervalMs, 4, new Path("/tmp"), "");
  }

  @Test
  public void testTriggerFlushBySize() throws IOException, InterruptedException {
    initFlusher(1024 * 1024, 1_000_000, TimeUnit.MINUTES.toMillis(15));
    memstoreHeapSize.set(1000 * 1024);
    flusher.onUpdate();
    Thread.sleep(1000);
    assertEquals(0, flushCalled.get());
    memstoreOffHeapSize.set(1000 * 1024);
    flusher.onUpdate();
    Waiter.waitFor(conf, 2000, () -> flushCalled.get() == 1);
  }

  private void assertTriggerFlushByChanges(int changes) throws InterruptedException {
    int currentFlushCalled = flushCalled.get();
    for (int i = 0; i < changes; i++) {
      flusher.onUpdate();
    }
    Thread.sleep(1000);
    assertEquals(currentFlushCalled, flushCalled.get());
    flusher.onUpdate();
    Waiter.waitFor(conf, 5000, () -> flushCalled.get() == currentFlushCalled + 1);
  }

  @Test
  public void testTriggerFlushByChanges() throws InterruptedException {
    initFlusher(128 * 1024 * 1024, 10, TimeUnit.MINUTES.toMillis(15));
    assertTriggerFlushByChanges(10);
    assertTriggerFlushByChanges(10);
  }

  @Test
  public void testPeriodicalFlush() throws InterruptedException {
    initFlusher(128 * 1024 * 1024, 1_000_000, TimeUnit.SECONDS.toMillis(1));
    assertEquals(0, flushCalled.get());
    Thread.sleep(1500);
    assertEquals(1, flushCalled.get());
    Thread.sleep(1000);
    assertEquals(2, flushCalled.get());

  }
}
