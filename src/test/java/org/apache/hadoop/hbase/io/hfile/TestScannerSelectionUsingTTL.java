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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test the optimization that does not scan files where all timestamps are
 * expired.
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestScannerSelectionUsingTTL {

  private static final Log LOG =
      LogFactory.getLog(TestScannerSelectionUsingTTL.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static String TABLE = "myTable";
  private static String FAMILY = "myCF";
  private static byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  private static final int TTL_SECONDS = 2;
  private static final int TTL_MS = TTL_SECONDS * 1000;

  private static final int NUM_EXPIRED_FILES = 2;
  private static final int NUM_ROWS = 8;
  private static final int NUM_COLS_PER_ROW = 5;

  public final int numFreshFiles;

  @Parameters
  public static Collection<Object[]> parametersNumFreshFiles() {
    return Arrays.asList(new Object[][]{
        new Object[] { new Integer(1) },
        new Object[] { new Integer(2) },
        new Object[] { new Integer(3) }
    });
  }

  public TestScannerSelectionUsingTTL(int numFreshFiles) {
    this.numFreshFiles = numFreshFiles;
  }

  @Test
  public void testScannerSelection() throws IOException {
    HColumnDescriptor hcd =
      new HColumnDescriptor(FAMILY_BYTES, Integer.MAX_VALUE,
          Compression.Algorithm.NONE.getName(),
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          TTL_SECONDS,
          BloomType.NONE.toString());
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(Bytes.toBytes(TABLE));
    HRegion region =
        HRegion.createHRegion(info, TEST_UTIL.getClusterTestDir(),
            TEST_UTIL.getConfiguration(), htd);

    for (int iFile = 0; iFile < NUM_EXPIRED_FILES + numFreshFiles; ++iFile) {
      if (iFile == NUM_EXPIRED_FILES) {
        Threads.sleepWithoutInterrupt(TTL_MS);
      }

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

    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE);
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    LruBlockCache cache = (LruBlockCache) cacheConf.getBlockCache();
    cache.clearCache();
    InternalScanner scanner = region.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    final int expectedKVsPerRow = numFreshFiles * NUM_COLS_PER_ROW;
    int numReturnedRows = 0;
    LOG.info("Scanning the entire table");
    while (scanner.next(results) || results.size() > 0) {
      assertEquals(expectedKVsPerRow, results.size());
      ++numReturnedRows;
      results.clear();
    }
    assertEquals(NUM_ROWS, numReturnedRows);
    Set<String> accessedFiles = cache.getCachedFileNamesForTest();
    LOG.debug("Files accessed during scan: " + accessedFiles);
    assertEquals("If " + (NUM_EXPIRED_FILES + numFreshFiles) + " files are "
        + "accessed instead of " + numFreshFiles + ", we are "
        + "not filtering expired files out.", numFreshFiles,
        accessedFiles.size());

    region.close();
  }

}
