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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.IndexOnlyLruBlockCache;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
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

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final byte[] FAM_NAME = Bytes.toBytes("f");

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
    // Conditions for this test to set up RegionScannerImpl to bail on the scan
    // after a single iteration
    // 1. Configure preadMaxBytes to something small to trigger scannerContext#returnImmediately
    // 2. Configure a filter to filter out some rows, in this case rows with values < 5
    // 3. Configure the filter's hasFilterRow to return true so RegionScannerImpl sets
    // the limitScope to something with a depth of 0, so we bail on the scan after the first
    // iteration

    Configuration copyConf = new Configuration(conf);
    copyConf.setLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 1);
    Scan scan = new Scan();
    scan.setFilter(new FiltersRowsLessThan5());
    scan.setLimit(1);

    try (Table table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAME)) {
      TableDescriptor htd = TEST_UTIL.getAdmin().getDescriptor(TABLE_NAME);
      RegionInfo hri = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).get(0);

      for (int i = 0; i < 10; ++i) {
        table.put(createPut(i));
      }

      // Flush contents to disk so we can scan the fs
      TEST_UTIL.getAdmin().flush(TABLE_NAME);

      ClientSideRegionScanner clientSideRegionScanner =
        new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null);
      RegionScanner scannerSpy = spy(clientSideRegionScanner.scanner);
      clientSideRegionScanner.scanner = scannerSpy;
      Result result = clientSideRegionScanner.next();

      verify(scannerSpy, times(6)).nextRaw(anyList());
      assertNotNull(result);
      assertEquals(Bytes.toInt(result.getRow()), 5);
      assertTrue(clientSideRegionScanner.hasMore);

      for (int i = 6; i < 10; ++i) {
        result = clientSideRegionScanner.next();
        verify(scannerSpy, times(i + 1)).nextRaw(anyList());
        assertNotNull(result);
        assertEquals(Bytes.toInt(result.getRow()), i);
      }

      result = clientSideRegionScanner.next();
      assertNull(result);
      assertFalse(clientSideRegionScanner.hasMore);
    }
  }

  @Test
  public void testScanMetricsByRegion() throws IOException {
    Scan scan = new Scan();
    scan.setScanMetricsEnabled(true);
    scan.setEnableScanMetricsByRegion(true);

    Configuration copyConf = new Configuration(conf);
    // Test without providing scan metric at prior and scan metrics by region enabled
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetrics = clientSideRegionScanner.getScanMetrics();
      assertNotNull(scanMetrics);
      List<ScanMetrics> scanMetricsByRegion = clientSideRegionScanner.getScanMetricsByRegion();
      assertEquals(1, scanMetricsByRegion.size());
      assertEquals(scanMetrics, scanMetricsByRegion.get(0));
      assertNotNull(scanMetrics.getRegionName());
      assertNull(scanMetrics.getServerName());
    }

    // Test without providing scan metric at prior and scan metrics by region disabled
    scan.setEnableScanMetricsByRegion(false);
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetrics = clientSideRegionScanner.getScanMetrics();
      assertNotNull(scanMetrics);
      assertNull(clientSideRegionScanner.getScanMetricsByRegion());
    }

    // Test by providing scan metric at prior with scan metrics by region enabled
    scan.setEnableScanMetricsByRegion(true);
    ScanMetrics scanMetrics = new ScanMetrics();
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, scanMetrics)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetricsFromScanner = clientSideRegionScanner.getScanMetrics();
      assertEquals(scanMetrics, scanMetricsFromScanner);
      List<ScanMetrics> scanMetricsByRegion = clientSideRegionScanner.getScanMetricsByRegion();
      assertEquals(1, scanMetricsByRegion.size());
      assertEquals(scanMetrics, scanMetricsByRegion.get(0));
      assertNotNull(scanMetrics.getRegionName());
      assertNull(scanMetrics.getServerName());
    }

    // Test by providing scan metric at prior with scan metrics by region disabled
    scan.setEnableScanMetricsByRegion(false);
    scanMetrics = new ScanMetrics();
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, scanMetrics)) {
      clientSideRegionScanner.next();
      ScanMetrics scanMetricsFromScanner = clientSideRegionScanner.getScanMetrics();
      assertEquals(scanMetrics, scanMetricsFromScanner);
      assertNull(clientSideRegionScanner.getScanMetricsByRegion());
    }

    // Test with scan metrics disabled
    scan.setScanMetricsEnabled(false);
    try (ClientSideRegionScanner clientSideRegionScanner =
      new ClientSideRegionScanner(copyConf, fs, rootDir, htd, hri, scan, null)) {
      clientSideRegionScanner.next();
      assertNull(clientSideRegionScanner.getScanMetrics());
      assertNull(clientSideRegionScanner.getScanMetricsByRegion());
    }
  }

  private static Put createPut(int rowAsInt) {
    byte[] row = Bytes.toBytes(rowAsInt);
    Put put = new Put(row);
    put.addColumn(FAM_NAME, row, row);
    return put;
  }

  private static class FiltersRowsLessThan5 extends FilterBase {

    @Override
    public boolean filterRowKey(Cell cell) {
      byte[] rowKey = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength() + cell.getRowOffset());
      int intValue = Bytes.toInt(rowKey);
      return intValue < 5;
    }

    @Override
    public boolean hasFilterRow() {
      return true;
    }
  }
}
