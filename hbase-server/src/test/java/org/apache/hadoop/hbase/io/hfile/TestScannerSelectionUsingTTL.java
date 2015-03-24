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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
@Category(MediumTests.class)
public class TestScannerSelectionUsingTTL {

  private static final Log LOG =
      LogFactory.getLog(TestScannerSelectionUsingTTL.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility().createLocalHTU();
  private static TableName TABLE = TableName.valueOf("myTable");
  private static String FAMILY = "myCF";
  private static byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  private static final int TTL_SECONDS = 10;
  private static final int TTL_MS = TTL_SECONDS * 1000;

  private static final int NUM_EXPIRED_FILES = 2;
  private static final int NUM_ROWS = 8;
  private static final int NUM_COLS_PER_ROW = 5;

  public final int numFreshFiles, totalNumFiles;

  /** Whether we are specifying the exact files to compact */
  private final boolean explicitCompaction;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<Object[]>();
    for (int numFreshFiles = 1; numFreshFiles <= 3; ++numFreshFiles) {
      for (boolean explicitCompaction : new boolean[] { false, true }) {
        params.add(new Object[] { numFreshFiles, explicitCompaction });
      }
    }
    return params;
  }

  public TestScannerSelectionUsingTTL(int numFreshFiles,
      boolean explicitCompaction) {
    this.numFreshFiles = numFreshFiles;
    this.totalNumFiles = numFreshFiles + NUM_EXPIRED_FILES;
    this.explicitCompaction = explicitCompaction;
  }

  @Test
  public void testScannerSelection() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.store.delete.expired.storefile", false);
    HColumnDescriptor hcd =
      new HColumnDescriptor(FAMILY_BYTES)
          .setMaxVersions(Integer.MAX_VALUE)
          .setTimeToLive(TTL_SECONDS);
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(TABLE);
    HRegion region =
        HRegion.createHRegion(info, TEST_UTIL.getDataTestDir(info.getEncodedName()),
            conf, htd);

    long ts = EnvironmentEdgeManager.currentTime();
    long version = 0; //make sure each new set of Put's have a new ts
    for (int iFile = 0; iFile < totalNumFiles; ++iFile) {
      if (iFile == NUM_EXPIRED_FILES) {
        Threads.sleepWithoutInterrupt(TTL_MS);
        version += TTL_MS;
      }

      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        Put put = new Put(Bytes.toBytes("row" + iRow));
        for (int iCol = 0; iCol < NUM_COLS_PER_ROW; ++iCol) {
          put.add(FAMILY_BYTES, Bytes.toBytes("col" + iCol),
              ts + version, Bytes.toBytes("value" + iFile + "_" + iRow + "_" + iCol));
        }
        region.put(put);
      }
      region.flush(true);
      version++;
    }

    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE);
    CacheConfig cacheConf = new CacheConfig(conf);
    LruBlockCache cache = (LruBlockCache) cacheConf.getBlockCache();
    cache.clearCache();
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
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

    // Exercise both compaction codepaths.
    if (explicitCompaction) {
      HStore store = (HStore)region.getStore(FAMILY_BYTES);
      store.compactRecentForTestingAssumingDefaultPolicy(totalNumFiles);
    } else {
      region.compact(false);
    }

    region.close();
  }
}
