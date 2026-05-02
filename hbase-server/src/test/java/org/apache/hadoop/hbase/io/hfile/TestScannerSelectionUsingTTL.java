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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the optimization that does not scan files where all timestamps are expired.
 */
@HBaseParameterizedTestTemplate(name = "numFreshFiles={0}")
@Tag(IOTests.TAG)
@Tag(LargeTests.TAG)
public class TestScannerSelectionUsingTTL {

  private static final Logger LOG = LoggerFactory.getLogger(TestScannerSelectionUsingTTL.class);

  private static final HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();
  private static TableName TABLE = TableName.valueOf("myTable");
  private static String FAMILY = "myCF";
  private static byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  private static final int TTL_SECONDS = 10;
  private static final int TTL_MS = TTL_SECONDS * 1000;

  private static final int NUM_EXPIRED_FILES = 2;
  private static final int NUM_ROWS = 8;
  private static final int NUM_COLS_PER_ROW = 5;

  public final int numFreshFiles, totalNumFiles;

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    for (int numFreshFiles = 1; numFreshFiles <= 3; ++numFreshFiles) {
      params.add(Arguments.of(numFreshFiles));
    }
    return params.stream();
  }

  public TestScannerSelectionUsingTTL(int numFreshFiles) {
    this.numFreshFiles = numFreshFiles;
    this.totalNumFiles = numFreshFiles + NUM_EXPIRED_FILES;
  }

  @TestTemplate
  public void testScannerSelection() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.store.delete.expired.storefile", false);
    LruBlockCache cache = (LruBlockCache) BlockCacheFactory.createBlockCache(conf);

    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_BYTES)
        .setMaxVersions(Integer.MAX_VALUE).setTimeToLive(TTL_SECONDS).build())
      .build();
    RegionInfo info = RegionInfoBuilder.newBuilder(TABLE).build();
    HRegion region = HBaseTestingUtility.createRegionAndWAL(info,
      TEST_UTIL.getDataTestDir(info.getEncodedName()), conf, td, cache);

    long ts = EnvironmentEdgeManager.currentTime();
    long version = 0; // make sure each new set of Put's have a new ts
    for (int iFile = 0; iFile < totalNumFiles; ++iFile) {
      if (iFile == NUM_EXPIRED_FILES) {
        Threads.sleepWithoutInterrupt(TTL_MS);
        version += TTL_MS;
      }

      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        Put put = new Put(Bytes.toBytes("row" + iRow));
        for (int iCol = 0; iCol < NUM_COLS_PER_ROW; ++iCol) {
          put.addColumn(FAMILY_BYTES, Bytes.toBytes("col" + iCol), ts + version,
            Bytes.toBytes("value" + iFile + "_" + iRow + "_" + iCol));
        }
        region.put(put);
      }
      region.flush(true);
      version++;
    }

    Scan scan = new Scan().readVersions(Integer.MAX_VALUE);
    cache.clearCache();
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> results = new ArrayList<>();
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

    region.compact(false);

    HBaseTestingUtility.closeRegionAndWAL(region);
  }
}
