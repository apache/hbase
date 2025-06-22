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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
  }

  @Test
  public void testGetRegionServerMinFreeHeapFraction() {
    // when setting is not set, it should return the default value
    conf.set(MemorySizeUtil.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "");
    float minFreeHeapFraction = MemorySizeUtil.getRegionServerMinFreeHeapFraction(conf);
    assertEquals(HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD, minFreeHeapFraction, 0.0f);

    // when setting to 0, it should return 0.0f
    conf.set(MemorySizeUtil.HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY, "0");
    minFreeHeapFraction = MemorySizeUtil.getRegionServerMinFreeHeapFraction(conf);
    assertEquals(0.0f, minFreeHeapFraction, 0.0f);
  }
}
