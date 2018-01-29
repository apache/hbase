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
package org.apache.hadoop.hbase.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test class for {@link MetricRegistriesLoader}.
 */
@Category(SmallTests.class)
public class TestMetricRegistriesLoader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricRegistriesLoader.class);

  @Test
  public void testLoadSinleInstance() {
    MetricRegistries loader = mock(MetricRegistries.class);
    MetricRegistries instance = MetricRegistriesLoader.load(Lists.newArrayList(loader));
    assertEquals(loader, instance);
  }

  @Test
  public void testLoadMultipleInstances() {
    MetricRegistries loader1 = mock(MetricRegistries.class);
    MetricRegistries loader2 = mock(MetricRegistries.class);
    MetricRegistries loader3 = mock(MetricRegistries.class);
    MetricRegistries instance = MetricRegistriesLoader.load(Lists.newArrayList(loader1, loader2,
        loader3));

    // the load() returns the first instance
    assertEquals(loader1, instance);
    assertNotEquals(loader2, instance);
    assertNotEquals(loader3, instance);
  }
}
