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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestScanAttributes {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScanAttributes.class);

  @Test
  public void testCoEnableAndCoDisableScanMetricsAndScanMetricsByRegion() {
    Scan scan = new Scan();
    Assert.assertFalse(scan.isScanMetricsEnabled());
    Assert.assertFalse(scan.isScanMetricsByRegionEnabled());

    // Assert enabling scan metrics by region enables scan metrics also
    scan.setEnableScanMetricsByRegion(true);
    Assert.assertTrue(scan.isScanMetricsEnabled());
    Assert.assertTrue(scan.isScanMetricsByRegionEnabled());

    // Assert disabling scan metrics disables scan metrics by region
    scan.setScanMetricsEnabled(false);
    Assert.assertFalse(scan.isScanMetricsEnabled());
    Assert.assertFalse(scan.isScanMetricsByRegionEnabled());
  }
}
