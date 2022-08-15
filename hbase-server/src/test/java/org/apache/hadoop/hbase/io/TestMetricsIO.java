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
package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetricsIO {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsIO.class);

  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testMetrics() {
    MetricsIO metrics = new MetricsIO(new MetricsIOWrapper() {
      @Override
      public long getChecksumFailures() {
        return 40;
      }
    });

    metrics.updateFsReadTime(100);
    metrics.updateFsReadTime(200);

    metrics.updateFsPreadTime(300);

    metrics.updateFsWriteTime(400);
    metrics.updateFsWriteTime(500);
    metrics.updateFsWriteTime(600);

    HELPER.assertCounter("fsChecksumFailureCount", 40, metrics.getMetricsSource());

    HELPER.assertCounter("fsReadTime_numOps", 2, metrics.getMetricsSource());
    HELPER.assertCounter("fsPReadTime_numOps", 1, metrics.getMetricsSource());
    HELPER.assertCounter("fsWriteTime_numOps", 3, metrics.getMetricsSource());
  }
}
