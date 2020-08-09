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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Test for MetricsMasterProcSourceImpl
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsMasterProcSourceImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsMasterProcSourceImpl.class);

  public static MetricsAssertHelper HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private MetricsMasterWrapperStub wrapper;
  private MetricsMasterProcSourceImpl masterProcSource;

  @BeforeClass
  public static void classSetup() {
    HELPER.init();
  }

  @Before
  public void setUp() {
    wrapper = new MetricsMasterWrapperStub();
    masterProcSource = new MetricsMasterProcSourceImpl(wrapper);
  }

  @Test
  public void testGetInstance() throws Exception {
    MetricsMasterProcSourceFactory metricsMasterProcSourceFactory = CompatibilitySingletonFactory
        .getInstance(MetricsMasterProcSourceFactory.class);
    MetricsMasterProcSource masterProcSource = metricsMasterProcSourceFactory.create(null);
    assertTrue(masterProcSource instanceof MetricsMasterProcSourceImpl);
    assertSame(metricsMasterProcSourceFactory,
            CompatibilitySingletonFactory.getInstance(MetricsMasterProcSourceFactory.class));
  }

  @Test
  public void testSplitProcedureMetrics() {
    HELPER
      .assertGauge(MetricsMasterProcSource.NUM_SPLIT_PROCEDURE_REQUEST_NAME, 32, masterProcSource);
    HELPER
      .assertGauge(MetricsMasterProcSource.NUM_SPLIT_PROCEDURE_SUCCESS_NAME, 24, masterProcSource);
    HELPER.assertGauge("SplitProcedureTime_max", 3011, masterProcSource);
    HELPER.assertGauge("SplitProcedureTime_min", 2082, masterProcSource);
  }
}
