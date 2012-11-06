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

import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Test of the default BaseSource implementation for hadoop 1
 */
public class TestBaseSourceImpl {

  private static BaseSourceImpl bmsi;

  @BeforeClass
  public static void setUp() throws Exception {
    bmsi = new BaseSourceImpl("TestName", "test description", "testcontext", "TestContext");
  }

  @Test
  public void testSetGauge() throws Exception {
    String key = "testset";
    bmsi.setGauge(key, 100);
    MetricMutableGaugeLong g = (MetricMutableGaugeLong) bmsi.metricsRegistry.get(key);
    assertEquals(key, g.name);
    bmsi.setGauge(key, 110);
    assertSame(g, bmsi.metricsRegistry.get(key));

  }

  @Test
  public void testIncGauge() throws Exception {
    String key = "testincgauge";
    bmsi.incGauge(key, 100);
    MetricMutableGaugeLong g = (MetricMutableGaugeLong) bmsi.metricsRegistry.get(key);
    assertEquals(key, g.name);
    bmsi.incGauge(key, 10);
    assertSame(g, bmsi.metricsRegistry.get(key));
  }

  @Test
  public void testDecGauge() throws Exception {
    String key = "testdec";
    bmsi.decGauge(key, 100);
    MetricMutableGaugeLong g = (MetricMutableGaugeLong) bmsi.metricsRegistry.get(key);
    assertEquals(key, g.name);
    bmsi.decGauge(key, 100);
    assertSame(g, bmsi.metricsRegistry.get(key));
  }

  @Test
  public void testIncCounters() throws Exception {
    String key = "testinccounter";
    bmsi.incCounters(key, 100);
    MetricMutableCounterLong c = (MetricMutableCounterLong) bmsi.metricsRegistry.get(key);
    assertEquals(key, c.name);
    bmsi.incCounters(key, 100);
    assertSame(c, bmsi.metricsRegistry.get(key));
  }

  @Test
  public void testRemoveMetric() throws Exception {
    bmsi.setGauge("testrm", 100);
    bmsi.removeMetric("testrm");
    assertNull(bmsi.metricsRegistry.get("testrm"));

  }

}
