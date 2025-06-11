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
package org.apache.hadoop.hbase.io.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.lang.management.MemoryUsage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;

@Category({ MiscTests.class, SmallTests.class })
public class TestMemorySizeUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMemorySizeUtil.class);

  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
  }

  @Test
  public void testValidateRegionServerHeapMemoryAllocation() {
    // when memstore size + block cache size + default free heap min size == 1.0
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.4f);
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.4f);
    assertEquals(HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD, 0.2f, 0.0f);
    MemorySizeUtil.validateRegionServerHeapMemoryAllocation(conf);

    // when memstore size + block cache size + default free heap min size > 1.0
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.5f);
    assertThrows(RuntimeException.class,
      () -> MemorySizeUtil.validateRegionServerHeapMemoryAllocation(conf));

    // set the min free heap size to 1.6g, which is 10% of the total heap size
    conf.set(HConstants.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "1.6g");
    try (MockedStatic<MemorySizeUtil> mockedMemorySizeUtil =
      mockStatic(MemorySizeUtil.class, CALLS_REAL_METHODS)) {
      mockedMemorySizeUtil.when(MemorySizeUtil::safeGetHeapMemoryUsage)
        .thenReturn(createMemoryUsage(16L * 1024 * 1024 * 1024));

      MemorySizeUtil.validateRegionServerHeapMemoryAllocation(conf);
    }

    // set the min free heap size to 4g, which is 25% of the total heap size
    conf.set(HConstants.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "4g");
    try (MockedStatic<MemorySizeUtil> mockedMemorySizeUtil =
      mockStatic(MemorySizeUtil.class, CALLS_REAL_METHODS)) {
      mockedMemorySizeUtil.when(MemorySizeUtil::safeGetHeapMemoryUsage)
        .thenReturn(createMemoryUsage(16L * 1024 * 1024 * 1024));

      // this should throw an exception as 0.5 + 0.4 + 0.25 = 1.15 > 1.0
      assertThrows(RuntimeException.class,
        () -> MemorySizeUtil.validateRegionServerHeapMemoryAllocation(conf));
    }
  }

  @Test
  public void testGetRegionServerMinFreeHeapFraction() {
    // when setting is not set, it should return the default value
    conf.set(HConstants.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "");
    float minFreeHeapFraction = MemorySizeUtil.getRegionServerMinFreeHeapFraction(conf);
    assertEquals(HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD, minFreeHeapFraction, 0.0f);

    // when setting to 0, it should return 0.0f
    conf.set(HConstants.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "0");
    minFreeHeapFraction = MemorySizeUtil.getRegionServerMinFreeHeapFraction(conf);
    assertEquals(0.0f, minFreeHeapFraction, 0.0f);

    // when setting with human-readable format
    conf.set(HConstants.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "4g");
    try (MockedStatic<MemorySizeUtil> mockedMemorySizeUtil =
      mockStatic(MemorySizeUtil.class, CALLS_REAL_METHODS)) {
      mockedMemorySizeUtil.when(MemorySizeUtil::safeGetHeapMemoryUsage)
        .thenReturn(createMemoryUsage(16L * 1024 * 1024 * 1024));

      minFreeHeapFraction = MemorySizeUtil.getRegionServerMinFreeHeapFraction(conf);
      assertEquals(0.25f, minFreeHeapFraction, 0.001f);
    }
  }

  private MemoryUsage createMemoryUsage(long max) {
    return new MemoryUsage(0L, 0L, 0L, max);
  }
}
