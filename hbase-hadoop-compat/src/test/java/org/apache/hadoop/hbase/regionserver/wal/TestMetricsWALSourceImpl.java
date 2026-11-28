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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource.TableMetrics;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MetricsTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetricsWALSourceImpl {

  @Test
  public void testGetInstance() throws Exception {
    MetricsWALSource walSource = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    assertTrue(walSource instanceof MetricsWALSourceImpl);
    assertSame(walSource, CompatibilitySingletonFactory.getInstance(MetricsWALSource.class));
  }

  @Test
  public void testRetainAndReclaimTableCounters() {
    String name = "TestRetainAndReclaimTableCounters";
    MetricsWALSourceImpl source = new MetricsWALSourceImpl(name, name, name, name);
    DynamicMetricsRegistry registry = source.getMetricsRegistry();
    TableName activeTable = TableName.valueOf("active");
    String activeCount = activeTable + "." + MetricsWALSource.APPEND_COUNT;
    String activeSize = activeTable + "." + MetricsWALSource.APPEND_SIZE;
    try (TableMetrics first = source.retainTableMetrics(activeTable);
      TableMetrics second = source.retainTableMetrics(activeTable)) {
      first.recordAppend(10);
      first.close();
      first.close();
      MutableFastCounter count = (MutableFastCounter) registry.get(activeCount);
      MutableFastCounter size = (MutableFastCounter) registry.get(activeSize);
      assertEquals(1, count.value());
      assertEquals(10, size.value());
      for (int i = 0; i < 100; i++) {
        TableName table = TableName.valueOf("date_" + i);
        String tableCount = table + "." + MetricsWALSource.APPEND_COUNT;
        String tableSize = table + "." + MetricsWALSource.APPEND_SIZE;
        TableMetrics removed = source.retainTableMetrics(table);
        removed.recordAppend(100);
        assertEquals(2, source.getTableMetricsCount());
        assertEquals(1, ((MutableFastCounter) registry.get(tableCount)).value());
        removed.close();
        assertEquals(1, source.getTableMetricsCount());
        assertNull(registry.get(tableCount));
        assertNull(registry.get(tableSize));

        try (TableMetrics recreated = source.retainTableMetrics(table)) {
          removed.recordAppend(100);
          removed.close();
          assertNull(registry.get(tableCount));
          recreated.recordAppend(1);
          assertEquals(1, ((MutableFastCounter) registry.get(tableCount)).value());
          assertEquals(1, ((MutableFastCounter) registry.get(tableSize)).value());
        }
        second.recordAppend(1);
      }
      assertSame(count, registry.get(activeCount));
      assertSame(size, registry.get(activeSize));
      assertEquals(101, count.value());
      assertEquals(110, size.value());
    }
    assertEquals(0, source.getTableMetricsCount());
    assertNull(registry.get(activeCount));
    assertNull(registry.get(activeSize));
  }

  @Test
  public void testClosingLastRegionWhileOpeningAnother() throws Exception {
    String name = "TestClosingLastRegionWhileOpeningAnother";
    MetricsWALSourceImpl source = new MetricsWALSourceImpl(name, name, name, name);
    DynamicMetricsRegistry registry = source.getMetricsRegistry();
    TableName table = TableName.valueOf("reopening");
    String countKey = table + "." + MetricsWALSource.APPEND_COUNT;
    String sizeKey = table + "." + MetricsWALSource.APPEND_SIZE;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (int i = 0; i < 20; i++) {
        TableMetrics closing = source.retainTableMetrics(table);
        closing.recordAppend(10);
        CyclicBarrier start = new CyclicBarrier(2);
        Future<?> close = executor.submit(() -> {
          start.await(10, TimeUnit.SECONDS);
          closing.close();
          return null;
        });
        start.await(10, TimeUnit.SECONDS);
        try (TableMetrics opening = source.retainTableMetrics(table)) {
          opening.recordAppend(1);
          close.get(10, TimeUnit.SECONDS);
          long count = ((MutableFastCounter) registry.get(countKey)).value();
          long size = ((MutableFastCounter) registry.get(sizeKey)).value();
          assertTrue((count == 1 && size == 1) || (count == 2 && size == 11));
        }
        assertNull(registry.get(countKey));
        assertNull(registry.get(sizeKey));
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
