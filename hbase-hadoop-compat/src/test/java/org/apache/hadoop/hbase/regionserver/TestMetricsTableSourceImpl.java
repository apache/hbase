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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Test for MetricsTableSourceImpl
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsTableSourceImpl {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsTableSourceImpl.class);

  @SuppressWarnings("SelfComparison")
  @Test
  public void testCompareToHashCode() throws Exception {
    MetricsRegionServerSourceFactory metricsFact =
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class);

    MetricsTableSource one = metricsFact.createTable(
        "ONETABLE", new MetricsTableWrapperStub("ONETABLE"));
    MetricsTableSource oneClone = metricsFact.createTable(
        "ONETABLE",
            new MetricsTableWrapperStub("ONETABLE"));
    MetricsTableSource two = metricsFact.createTable(
        "TWOTABLE", new MetricsTableWrapperStub("TWOTABLE"));

    assertEquals(0, one.compareTo(oneClone));
    assertEquals(one.hashCode(), oneClone.hashCode());
    assertNotEquals(one, two);

    assertTrue(one.compareTo(two) != 0);
    assertTrue(two.compareTo(one) != 0);
    assertTrue(two.compareTo(one) != one.compareTo(two));
    assertTrue(two.compareTo(two) == 0);
  }

  @Test(expected = RuntimeException.class)
  public void testNoGetTableMetricsSourceImpl() {
    // This should throw an exception because MetricsTableSourceImpl should only
    // be created by a factory.
    CompatibilitySingletonFactory.getInstance(MetricsTableSourceImpl.class);
  }

  @Test
  public void testGetTableMetrics() {
    MetricsTableSource oneTbl =
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .createTable("ONETABLE", new MetricsTableWrapperStub("ONETABLE"));
    assertEquals("ONETABLE", oneTbl.getTableName());
  }

}
