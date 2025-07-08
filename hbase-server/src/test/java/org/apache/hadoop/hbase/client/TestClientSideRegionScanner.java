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

import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.IndexOnlyLruBlockCache;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestClientSideRegionScanner {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientSideRegionScanner.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final byte[] FAM_NAME = Bytes.toBytes("f");
  private static final byte[] FAM_NAME_2 = Bytes.toBytes("f2");

  private Configuration conf;
  private Path rootDir;
  private FileSystem fs;
  private TableDescriptor htd;
  private RegionInfo hri;
  private Scan scan;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = TEST_UTIL.getTestFileSystem();
    htd = TEST_UTIL.getAdmin().getDescriptor(TableName.META_TABLE_NAME);
    hri = TEST_UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME).get(0);
    scan = new Scan();
  }

  @Test
  public void testDefaultBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNotNull(blockCache);
    assertTrue(blockCache instanceof IndexOnlyLruBlockCache);
    assertTrue(HConstants.HBASE_CLIENT_SCANNER_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT
        == blockCache.getMaxSize());
  }

  @Test
  public void testConfiguredBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    // tiny 1MB fixed cache size
    long blockCacheFixedSize = 1024 * 1024L;
    copyConf.setLong(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY, blockCacheFixedSize);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNotNull(blockCache);
    assertTrue(blockCache instanceof IndexOnlyLruBlockCache);
    assertTrue(blockCacheFixedSize == blockCache.getMaxSize());
  }

  @Test
  public void testNoBlockCache() throws IOException {
    Configuration copyConf = new Configuration(conf);
    copyConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

    BlockCache blockCache = clientSideRegionScanner.getRegion().getBlockCache();
    assertNull(blockCache);
  }

  @Test
  public void testContinuesToScanIfHasMore() throws IOException {
    // In order to hit this bug, we need RegionScanner to exit out early despite having retrieved
    // no values. By default, RegionScanner will continue until it gets values or a limit is
    // exceeded. In the context of this test, the easiest way to do that is to trigger the
    // retryImmediately behavior of converting from PREAD to STREAM. To do that, we set PREAD max
    // bytes to a small number. Then we need to actually check our limits, which would trigger
    // retryImmediately. To do that, we use joinedContinuationRow (essential family) through
    // a SingleColumnValueFilter with setFilterIfMissing(true). This causes FAM_NAME_2 to be
    // non-essential, and only brought in when the filter passes. This causes us to go through the
    // limit check in joinedContinuationRow and exit out early with hasMore=true but empty values.
    // Prior to HBASE-27950, the first call to ClientSideRegionScanner.next() below would
    // return null because of the empty values list, and there would further be nulls in
    // between row 5 and 8. With the fix, the scanner appropriately continues iterating, only
    // returning if there are values or finally when hasMore=false at the end. Thus, the first
    // next returns row 5, then row 8, then null because the scanner is exhausted. The intermediate
    // nulls from rows in between are correctly skipped.

    Configuration copyConf = new Configuration(conf);
    copyConf.setLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 1);
    int[] filteredRows = new int[] { 5, 8 };
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    for (int filteredRow : filteredRows) {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(FAM_NAME,
        Bytes.toBytes(filteredRow), CompareOperator.EQUAL, Bytes.toBytes(filteredRow));
      filter.setFilterIfMissing(true);
      filterList.addFilter(filter);
    }

    Scan scan = new Scan();
    scan.setFilter(filterList);
    scan.setLimit(1);

    try (Table table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAM_NAME, FAM_NAME_2 })) {
      TableDescriptor htd = TEST_UTIL.getAdmin().getDescriptor(TABLE_NAME);
      RegionInfo hri = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).get(0);

      for (int i = 0; i < 10; ++i) {
        table.put(createPut(i));
      }

      // Flush contents to disk so we can scan the fs
      TEST_UTIL.getAdmin().flush(TABLE_NAME);

      ClientSideRegionScanner clientSideRegionScanner =
        new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);

      Result result;

      for (int filteredRow : filteredRows) {
        result = clientSideRegionScanner.next();
        assertNotNull(result);
        assertEquals(Bytes.toInt(result.getRow()), filteredRow);
        assertTrue(clientSideRegionScanner.hasMore);
      }

      result = clientSideRegionScanner.next();
      assertNull(result);
      assertFalse(clientSideRegionScanner.hasMore);
    }
  }

  @Test
  public void testScanMetricsDisabled() throws IOException {
    Configuration copyConf = new Configuration(conf);
    Scan scan = new Scan();
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null)) {
      clientSideRegionScanner.next();
      assertNull(clientSideRegionScanner.getScanMetrics());
    }
  }

  private void testScanMetricsWithScanMetricsByRegionDisabled(ScanMetrics scanMetrics)
    throws IOException {
    Configuration copyConf = new Configuration(conf);
    Scan scan = new Scan();
    scan.setScanMetricsEnabled(true);
    TEST_UTIL.getAdmin().flush(TableName.META_TABLE_NAME);
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, scanMetrics)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetricsFromScanner = clientSideRegionScanner.getScanMetrics();
      assertNotNull(scanMetricsFromScanner);
      if (scanMetrics != null) {
        Assert.assertSame(scanMetrics, scanMetricsFromScanner);
      }
      Map<String, Long> metricsMap = scanMetricsFromScanner.getMetricsMap(false);
      Assert.assertTrue(metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME) > 0);
      Assert.assertTrue(scanMetricsFromScanner.collectMetricsByRegion(false).isEmpty());
    }
  }

  @Test
  public void testScanMetricsNotAsInputWithScanMetricsByRegionDisabled() throws IOException {
    testScanMetricsWithScanMetricsByRegionDisabled(null);
  }

  @Test
  public void testScanMetricsAsInputWithScanMetricsByRegionDisabled() throws IOException {
    testScanMetricsWithScanMetricsByRegionDisabled(new ScanMetrics());
  }

  private void testScanMetricByRegion(ScanMetrics scanMetrics) throws IOException {
    Configuration copyConf = new Configuration(conf);
    Scan scan = new Scan();
    scan.setEnableScanMetricsByRegion(true);
    TEST_UTIL.getAdmin().flush(TableName.META_TABLE_NAME);
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, scanMetrics)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetricsFromScanner = clientSideRegionScanner.getScanMetrics();
      assertNotNull(scanMetricsFromScanner);
      if (scanMetrics != null) {
        Assert.assertSame(scanMetrics, scanMetricsFromScanner);
      }
      Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
        scanMetricsFromScanner.collectMetricsByRegion();
      Assert.assertEquals(1, scanMetricsByRegion.size());
      for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
        .entrySet()) {
        ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
        Map<String, Long> metricsMap = entry.getValue();
        Assert.assertEquals(hri.getEncodedName(), scanMetricsRegionInfo.getEncodedRegionName());
        Assert.assertNull(scanMetricsRegionInfo.getServerName());
        Assert.assertTrue(metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME) > 0);
        Assert.assertEquals((long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME),
          scanMetricsFromScanner.countOfRowsScanned.get());
      }
    }
  }

  @Test
  public void testScanMetricsByRegionWithoutScanMetricsAsInput() throws IOException {
    testScanMetricByRegion(null);
  }

  @Test
  public void testScanMetricsByRegionWithScanMetricsAsInput() throws IOException {
    testScanMetricByRegion(new ScanMetrics());
  }

  private static Put createPut(int rowAsInt) {
    byte[] row = Bytes.toBytes(rowAsInt);
    Put put = new Put(row);
    put.addColumn(FAM_NAME, row, row);
    put.addColumn(FAM_NAME_2, row, row);
    return put;
  }

}
