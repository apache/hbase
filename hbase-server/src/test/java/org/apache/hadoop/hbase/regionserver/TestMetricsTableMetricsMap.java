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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestMetricsTableMetricsMap {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsTableMetricsMap.class);

  private String tableName = "testTableMetricsMap";

  private MetricsTableWrapperStub tableWrapper;
  private MetricsTable mt;
  private MetricsRegionServerWrapper rsWrapper;
  private MetricsRegionServer rsm;
  private MetricsTableAggregateSourceImpl agg;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();

    tableWrapper = new MetricsTableWrapperStub(tableName);
    mt = new MetricsTable(tableWrapper);
    rsWrapper = new MetricsRegionServerWrapperStub();

    rsm = new MetricsRegionServer(rsWrapper, conf, mt);
    MetricsTableAggregateSource tableSourceAgg = mt.getTableSourceAgg();
    if (tableSourceAgg instanceof MetricsTableAggregateSourceImpl) {
      agg = (MetricsTableAggregateSourceImpl) tableSourceAgg;
    } else {
      throw new RuntimeException(
        "tableSourceAgg should be the instance of MetricsTableAggregateSourceImpl");
    }
  }

  @Test
  public void testMetricsMap() throws InterruptedException {
    // do major compaction
    rsm.updateCompaction(tableName, true, 100, 200, 300, 400, 500);

    int metricsMapSize = agg.getMetricsRegistry().getMetricsMap().size();
    assertTrue("table metrics added then metricsMapSize should larger than 0", metricsMapSize > 0);

    // just for metrics update
    Thread.sleep(1000);
    // delete table all metrics
    agg.deleteTableSource(tableName);

    metricsMapSize = agg.getMetricsRegistry().getMetricsMap().size();
    assertEquals("table metrics all deleted then metricsSize should be 0", 0, metricsMapSize);
  }
}
