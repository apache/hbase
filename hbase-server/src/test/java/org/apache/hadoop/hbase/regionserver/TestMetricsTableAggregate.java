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

import java.io.IOException;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestMetricsTableAggregate {

  public static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testTableWrapperAggregateMetrics() throws IOException {
    String tableName = "testRequestCount";
    MetricsTableWrapperStub tableWrapper = new MetricsTableWrapperStub(tableName);
    CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
    .createTable(tableName, tableWrapper);
    MetricsTableAggregateSource agg = CompatibilitySingletonFactory
        .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();

    HELPER.assertCounter("Namespace_default_table_testRequestCount_metric_readRequestCount", 10, agg);
    HELPER.assertCounter("Namespace_default_table_testRequestCount_metric_writeRequestCount", 20, agg);
    HELPER.assertCounter("Namespace_default_table_testRequestCount_metric_totalRequestCount", 30, agg);
  }
}