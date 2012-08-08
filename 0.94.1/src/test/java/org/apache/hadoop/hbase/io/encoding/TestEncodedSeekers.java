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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.apache.hadoop.hbase.util.MultiThreadedWriter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests encoded seekers by loading and reading values.
 */
@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestEncodedSeekers {

  private static final String TABLE_NAME = "encodedSeekersTable";
  private static final String CF_NAME = "encodedSeekersCF";
  private static final byte[] CF_BYTES = Bytes.toBytes(CF_NAME);
  private static final int MAX_VERSIONS = 5;

  private static final int MIN_VALUE_SIZE = 30;
  private static final int MAX_VALUE_SIZE = 60;
  private static final int NUM_ROWS = 1000;
  private static final int NUM_COLS_PER_ROW = 20;
  private static final int NUM_HFILES = 4;
  private static final int NUM_ROWS_PER_FLUSH = NUM_ROWS / NUM_HFILES;

  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private final DataBlockEncoding encoding;
  private final boolean encodeOnDisk;

  /** Enable when debugging */
  private static final boolean VERBOSE = false;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      for (boolean encodeOnDisk : new boolean[]{false, true}) {
        paramList.add(new Object[] { encoding, encodeOnDisk });
      }
    }
    return paramList;
  }

  public TestEncodedSeekers(DataBlockEncoding encoding, boolean encodeOnDisk) {
    this.encoding = encoding;
    this.encodeOnDisk = encodeOnDisk;
  }

  @Test
  public void testEncodedSeeker() throws IOException {
    System.err.println("Testing encoded seekers for encoding " + encoding);
    LruBlockCache cache = (LruBlockCache)
    new CacheConfig(testUtil.getConfiguration()).getBlockCache();
    cache.clearCache();

    HRegion region = testUtil.createTestRegion(
        TABLE_NAME, new HColumnDescriptor(CF_NAME)
            .setMaxVersions(MAX_VERSIONS)
            .setDataBlockEncoding(encoding)
            .setEncodeOnDisk(encodeOnDisk)
    );
    LoadTestKVGenerator dataGenerator = new LoadTestKVGenerator(
        MIN_VALUE_SIZE, MAX_VALUE_SIZE);

    // Write
    for (int i = 0; i < NUM_ROWS; ++i) {
      byte[] key = MultiThreadedWriter.longToByteArrayKey(i);
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        Put put = new Put(key);
        String colAsStr = String.valueOf(j);
        byte[] value = dataGenerator.generateRandomSizeValue(i, colAsStr);
        put.add(CF_BYTES, Bytes.toBytes(colAsStr), value);
        region.put(put);
      }
      if (i % NUM_ROWS_PER_FLUSH == 0) {
        region.flushcache();
      }
    }

    for (int doneCompaction = 0; doneCompaction <= 1; ++doneCompaction) {
      // Read
      for (int i = 0; i < NUM_ROWS; ++i) {
        final byte[] rowKey = MultiThreadedWriter.longToByteArrayKey(i);
        for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
          if (VERBOSE) {
            System.err.println("Reading row " + i + ", column " +  j);
          }
          final String qualStr = String.valueOf(j);
          final byte[] qualBytes = Bytes.toBytes(qualStr);
          Get get = new Get(rowKey);
          get.addColumn(CF_BYTES, qualBytes);
          Result result = region.get(get, null);
          assertEquals(1, result.size());
          assertTrue(LoadTestKVGenerator.verify(Bytes.toString(rowKey), qualStr,
              result.getValue(CF_BYTES, qualBytes)));
        }
      }

      if (doneCompaction == 0) {
        // Compact, then read again at the next loop iteration.
        region.compactStores();
      }
    }

    Map<DataBlockEncoding, Integer> encodingCounts =
        cache.getEncodingCountsForTest();

    // Ensure that compactions don't pollute the cache with unencoded blocks
    // in case of in-cache-only encoding.
    System.err.println("encodingCounts=" + encodingCounts);
    assertEquals(1, encodingCounts.size());
    DataBlockEncoding encodingInCache =
        encodingCounts.keySet().iterator().next();
    assertEquals(encoding, encodingInCache);
    assertTrue(encodingCounts.get(encodingInCache) > 0);
  }

}
