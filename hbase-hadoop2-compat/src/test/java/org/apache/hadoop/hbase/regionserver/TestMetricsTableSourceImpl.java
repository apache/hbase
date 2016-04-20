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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Test for MetricsTableSourceImpl
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsTableSourceImpl {

  @Test
  public void testCompareToHashCode() throws Exception {
    MetricsRegionServerSourceFactory metricsFact =
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class);

    MetricsTableSource one = metricsFact.createTable("ONETABLE", new TableWrapperStub("ONETABLE"));
    MetricsTableSource oneClone = metricsFact.createTable("ONETABLE", new TableWrapperStub("ONETABLE"));
    MetricsTableSource two = metricsFact.createTable("TWOTABLE", new TableWrapperStub("TWOTABLE"));

    assertEquals(0, one.compareTo(oneClone));
    assertEquals(one.hashCode(), oneClone.hashCode());
    assertNotEquals(one, two);

    assertTrue(one.compareTo(two) != 0);
    assertTrue(two.compareTo(one) != 0);
    assertTrue(two.compareTo(one) != one.compareTo(two));
    assertTrue(two.compareTo(two) == 0);
  }

  @Test(expected = RuntimeException.class)
  public void testNoGetTableMetricsSourceImpl() throws Exception {
    // This should throw an exception because MetricsTableSourceImpl should only
    // be created by a factory.
    CompatibilitySingletonFactory.getInstance(MetricsTableSourceImpl.class);
  }

  @Test
  public void testGetTableMetrics() throws Exception{
    MetricsTableSource oneTbl =
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .createTable("ONETABLE", new TableWrapperStub("ONETABLE"));
    assertEquals("ONETABLE", oneTbl.getTableName());
  }

  static class TableWrapperStub implements MetricsTableWrapperAggregate {

    private String tableName;

    public TableWrapperStub(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public long getReadRequestsCount(String table) {
      return 10;
    }

    @Override
    public long getWriteRequestsCount(String table) {
      return 20;
    }

    @Override
    public long getTotalRequestsCount(String table) {
      return 30;
    }

    public String getTableName() {
      return tableName;
    }
  }
}
