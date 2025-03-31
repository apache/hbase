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
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category({ ClientTests.class, MediumTests.class })
public class TestScanMetricsByRegion extends FromClientSideBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScanMetricsByRegion.class);

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Parameters(name = "{index}: scanner={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] {"ForwardScanner", new Scan()},
      new Object[] {"ReverseScanner", new Scan().setReversed(true)});
  }

  @Parameter(0)
  public String scannerName;

  @Parameter(1)
  public Scan originalScan;

  @BeforeClass
  public static void setUp() throws Exception {
    // Start the minicluster
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Scan getScan(byte[][] ROWS, boolean isSingleRegionScan) throws IOException {
    Scan scan = new Scan(originalScan);
    if (isSingleRegionScan) {
      scan.withStartRow(ROWS[0], true);
      scan.withStopRow(ROWS[0], true);
    }
    else if (scan.isReversed()) {
      scan.withStartRow(ROWS[1], true);
      scan.withStopRow(ROWS[0], true);
    }
    else {
      scan.withStartRow(ROWS[0], true);
      scan.withStopRow(ROWS[1], true);
    }
    return scan;
  }

  @Test
  public void testScanMetricsByRegion() throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] ROWS = makeN(ROW, 2);
    byte[][] FAMILIES = makeNAscii(FAMILY, 2);
    byte[][] QUALIFIERS = makeN(QUALIFIER, 1);
    byte[][] VALUES = makeN(VALUE, 2);
    byte[][] splitKeys = new byte[][] {ROWS[1]};
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES, splitKeys)) {
      // Add two rows in two separate regions
      Put put1 = new Put(ROWS[0]);
      put1.addColumn(FAMILIES[0], QUALIFIERS[0], VALUES[0]);
      Put put2 = new Put(ROWS[1]);
      put2.addColumn(FAMILIES[0], QUALIFIERS[0], VALUES[1]);
      List<Put> puts = Arrays.asList(put1, put2);
      ht.batch(puts, null);
      // Verify we have two regions
      Admin admin = TEST_UTIL.getAdmin();
      Assert.assertEquals(2, admin.getRegions(tableName).size());

      // Test scan metrics disabled
      Scan scan = getScan(ROWS, false);
      Pair<ScanMetrics, List<ScanMetrics>> pair = getScanMetrics(ht, scan);
      ScanMetrics scanMetrics = pair.getFirst();
      List<ScanMetrics> scanMetricsByRegion = pair.getSecond();
      Assert.assertNull(scanMetrics);
      Assert.assertNull(scanMetricsByRegion);

      // Test scan metrics are enabled but scan metrics by region are disabled
      scan = getScan(ROWS, false);
      scan.setScanMetricsEnabled(true);
      pair = getScanMetrics(ht, scan);
      scanMetrics = pair.getFirst();
      scanMetricsByRegion = pair.getSecond();
      Assert.assertNull(scanMetrics.getServerName());
      Assert.assertNull(scanMetrics.getRegionName());
      Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
      Assert.assertEquals(2, scanMetrics.countOfRegions.get());
      Assert.assertEquals(2, scanMetrics.countOfRowsScanned.get());
      Assert.assertNull(scanMetricsByRegion);

      // Test scan metrics by region are enabled and multi-region scan
      scan = getScan(ROWS, false);
      scan.setScanMetricsEnabled(true);
      scan.setEnableScanMetricsByRegion(true);
      pair = getScanMetrics(ht, scan);
      scanMetrics = pair.getFirst();
      scanMetricsByRegion = pair.getSecond();
      Assert.assertNull(scanMetrics.getServerName());
      Assert.assertNull(scanMetrics.getRegionName());
      Assert.assertEquals(2, scanMetrics.countOfRegions.get());
      Assert.assertEquals(2, scanMetrics.countOfRowsScanned.get());
      long bytesInResults = 0;
      for (ScanMetrics perRegionScanMetrics : scanMetricsByRegion) {
        Assert.assertNotNull(perRegionScanMetrics.getServerName());
        Assert.assertNotNull(perRegionScanMetrics.getRegionName());
        Assert.assertEquals(1, perRegionScanMetrics.countOfRowsScanned.get());
        Assert.assertEquals(1, perRegionScanMetrics.countOfRegions.get());
        bytesInResults += perRegionScanMetrics.countOfBytesInResults.get();
      }
      Assert.assertEquals(scanMetrics.countOfBytesInResults.get(), bytesInResults);
      Assert.assertEquals(2, scanMetricsByRegion.size());

      // Test scan metrics by region are enabled and single-region scan
      scan = getScan(ROWS, true);
      scan.setScanMetricsEnabled(true);
      scan.setEnableScanMetricsByRegion(true);
      pair = getScanMetrics(ht, scan);
      scanMetrics = pair.getFirst();
      scanMetricsByRegion = pair.getSecond();
      Assert.assertNotNull(scanMetrics.getServerName());
      Assert.assertNotNull(scanMetrics.getRegionName());
      Assert.assertEquals(1, scanMetrics.countOfRowsScanned.get());
      Assert.assertEquals(1, scanMetrics.countOfRegions.get());
      Assert.assertEquals(scanMetrics, scanMetricsByRegion.get(0));
      Assert.assertEquals(1, scanMetricsByRegion.size());
    }
    finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
}
