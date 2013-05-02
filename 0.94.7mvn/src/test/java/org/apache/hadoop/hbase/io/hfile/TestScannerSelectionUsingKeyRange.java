/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.BlockMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test the optimization that does not scan files where all key ranges are excluded.
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestScannerSelectionUsingKeyRange {
  private static final Log LOG = LogFactory.getLog(TestScannerSelectionUsingKeyRange.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String TABLE = "myTable";
  private static String FAMILY = "myCF";
  private static byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);
  private static final int NUM_ROWS = 8;
  private static final int NUM_COLS_PER_ROW = 5;
  private static final int NUM_FILES = 2;
  private static final Map<Object, Integer> TYPE_COUNT = new HashMap<Object, Integer>(3);
  static {
    TYPE_COUNT.put(BloomType.ROWCOL, 2);
    TYPE_COUNT.put(BloomType.ROW, 2);
    TYPE_COUNT.put(BloomType.NONE, 2);
  }

  private BloomType bloomType;
  private int expectedCount;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<Object[]>();
    for (Object type : TYPE_COUNT.keySet()) {
        params.add(new Object[] { type, TYPE_COUNT.get(type) });
    }
    return params;
  }

  public TestScannerSelectionUsingKeyRange(Object expectedType, Object expectedCount) {
    bloomType = (BloomType)expectedType;
    expectedCount = expectedCount;
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testScannerSelection() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY_BYTES).setBlockCacheEnabled(true)
        .setBloomFilterType(bloomType);
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(Bytes.toBytes(TABLE));
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.getClusterTestDir(), conf, htd);

    for (int iFile = 0; iFile < NUM_FILES; ++iFile) {
      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        Put put = new Put(Bytes.toBytes("row" + iRow));
        for (int iCol = 0; iCol < NUM_COLS_PER_ROW; ++iCol) {
          put.add(FAMILY_BYTES, Bytes.toBytes("col" + iCol),
              Bytes.toBytes("value" + iFile + "_" + iRow + "_" + iCol));
        }
        region.put(put);
      }
      region.flushcache();
    }

    Scan scan = new Scan(Bytes.toBytes("aaa"), Bytes.toBytes("aaz"));
    CacheConfig cacheConf = new CacheConfig(conf);
    LruBlockCache cache = (LruBlockCache) cacheConf.getBlockCache();
    cache.clearCache();
    Map<String, Long> metricsBefore = SchemaMetrics.getMetricsSnapshot();
    SchemaMetrics.validateMetricChanges(metricsBefore);
    InternalScanner scanner = region.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (scanner.next(results)) {
    }
    scanner.close();
    assertEquals(0, results.size());
    Set<String> accessedFiles = cache.getCachedFileNamesForTest();
    assertEquals(accessedFiles.size(), 0);
    //assertEquals(cache.getBlockCount(), 0);
    Map<String, Long> diffMetrics = SchemaMetrics.diffMetrics(metricsBefore,
      SchemaMetrics.getMetricsSnapshot());
    SchemaMetrics schemaMetrics = SchemaMetrics.getInstance(TABLE, FAMILY);
    long dataBlockRead = SchemaMetrics.getLong(diffMetrics,
      schemaMetrics.getBlockMetricName(BlockCategory.DATA, false, BlockMetricType.READ_COUNT));
    assertEquals(dataBlockRead, 0);
    region.close();
  }
}
