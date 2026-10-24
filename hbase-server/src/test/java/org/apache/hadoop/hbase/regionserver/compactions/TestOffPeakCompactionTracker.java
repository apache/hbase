/**
 *
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

package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestOffPeakCompactionTracker {

  @Test
  public void testOffPeakCompactionTracker() throws Exception {
    OffPeakCompactionTracker offPeakCompactionTracker = OffPeakCompactionTracker.getInstance();
    Configuration conf = new Configuration();
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY, 5);
    offPeakCompactionTracker.updateConcurrency(conf);
    OffPeakHours offPeakHours = Mockito.mock(OffPeakHours.class);
    Mockito.when(offPeakHours.isOffPeakHour()).thenReturn(true);

    AtomicInteger count = new AtomicInteger(0);
    int familyNum = 10;
    List<HStore> stores = new ArrayList<>();
    for (int i = 0; i < familyNum; i++) {
      HStore store = Mockito.mock(HStore.class);
      Mockito.doAnswer(invocation -> {
        boolean mayUseOffPeak = offPeakHours.isOffPeakHour() &&
          offPeakCompactionTracker.tryAcquire();
        if (mayUseOffPeak) {
          count.incrementAndGet();
        }
        return null;
      }).when(store).requestCompaction();
      stores.add(store);
    }

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(familyNum);

    for (int i = 0; i < familyNum; i++) {
      HStore store = stores.get(i);
      CompletableFuture.supplyAsync(() -> {
        try {
          startLatch.await();
          store.requestCompaction();
          doneLatch.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      });
    }

    assertEquals(5, offPeakCompactionTracker.availablePermits());

    startLatch.countDown();
    doneLatch.await();
    assertEquals(5, count.get());
    assertEquals(0, offPeakCompactionTracker.availablePermits());
    offPeakCompactionTracker.release(count.get());
  }

  @Test
  public void testOffPeakCompactionConcurrencyOnChange() throws Exception {
    OffPeakCompactionTracker offPeakCompactionTracker = OffPeakCompactionTracker.getInstance();
    OffPeakCompactionTracker spyTracker = Mockito.spy(offPeakCompactionTracker);
    ConfigurationManager manager = new ConfigurationManager();
    Configuration conf = new Configuration();
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY, 2);

    int familyNum = 10;
    List<HStore> stores = new ArrayList<>();
    for (int i = 0; i < familyNum; i++) {
      HStore store = Mockito.mock(HStore.class);
      Mockito.doAnswer(invocation -> {
        spyTracker.registerIfNeeded(manager, conf);
        return null;
      }).when(store).registerChildren(manager);
      stores.add(store);
    }
    for (HStore store : stores) {
      store.registerChildren(manager);
    }

    Mockito.verify(spyTracker, Mockito.times(1))
           .updateConcurrency(Mockito.any(Configuration.class));
    assertEquals(2, spyTracker.availablePermits());

    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY, 100);
    manager.notifyAllObservers(conf);
    Mockito.verify(spyTracker, Mockito.times(2))
           .updateConcurrency(Mockito.any(Configuration.class));
    assertEquals(100, spyTracker.availablePermits());

    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY, 10);
    manager.notifyAllObservers(conf);
    Mockito.verify(spyTracker, Mockito.times(3))
           .updateConcurrency(Mockito.any(Configuration.class));
    assertEquals(10, spyTracker.availablePermits());
  }
}
