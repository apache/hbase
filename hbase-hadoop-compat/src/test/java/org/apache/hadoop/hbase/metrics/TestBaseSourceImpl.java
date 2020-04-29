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
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Test of default BaseSource for hadoop 2
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestBaseSourceImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBaseSourceImpl.class);

  private static BaseSourceImpl bmsi;

  @BeforeClass
  public static void setUp() throws Exception {
    bmsi = new BaseSourceImpl("TestName", "test description", "testcontext", "TestContext");
  }

  @Test
  public void testSetGauge() throws Exception {
    bmsi.setGauge("testset", 100);
    assertEquals(100, ((MutableGaugeLong) bmsi.metricsRegistry.get("testset")).value());
    bmsi.setGauge("testset", 300);
    assertEquals(300, ((MutableGaugeLong) bmsi.metricsRegistry.get("testset")).value());

  }

  @Test
  public void testIncGauge() throws Exception {
    bmsi.incGauge("testincgauge", 100);
    assertEquals(100, ((MutableGaugeLong) bmsi.metricsRegistry.get("testincgauge")).value());
    bmsi.incGauge("testincgauge", 100);
    assertEquals(200, ((MutableGaugeLong) bmsi.metricsRegistry.get("testincgauge")).value());

  }

  @Test
  public void testDecGauge() throws Exception {
    bmsi.decGauge("testdec", 100);
    assertEquals(-100, ((MutableGaugeLong) bmsi.metricsRegistry.get("testdec")).value());
    bmsi.decGauge("testdec", 100);
    assertEquals(-200, ((MutableGaugeLong) bmsi.metricsRegistry.get("testdec")).value());

  }

  @Test
  public void testIncCounters() throws Exception {
    bmsi.incCounters("testinccounter", 100);
    assertEquals(100, ((MutableFastCounter) bmsi.metricsRegistry.get("testinccounter")).value());
    bmsi.incCounters("testinccounter", 100);
    assertEquals(200, ((MutableFastCounter) bmsi.metricsRegistry.get("testinccounter")).value());

  }

  @Test
  public void testRemoveMetric() throws Exception {
    bmsi.setGauge("testrmgauge", 100);
    bmsi.removeMetric("testrmgauge");
    assertNull(bmsi.metricsRegistry.get("testrmgauge"));
  }

}
