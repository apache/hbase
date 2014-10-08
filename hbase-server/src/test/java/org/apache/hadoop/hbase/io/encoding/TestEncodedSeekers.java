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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests encoded seekers by loading and reading values.
 */
@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestEncodedSeekers {

  private static final String TABLE_NAME = "encodedSeekersTable";
  private static final String CF_NAME = "encodedSeekersCF";
  private static final byte[] CF_BYTES = Bytes.toBytes(CF_NAME);
  private static final int MAX_VERSIONS = 5;

  private static final int BLOCK_SIZE = 64 * 1024;
  private static final int MIN_VALUE_SIZE = 30;
  private static final int MAX_VALUE_SIZE = 60;
  private static final int NUM_ROWS = 1003;
  private static final int NUM_COLS_PER_ROW = 20;
  private static final int NUM_HFILES = 4;
  private static final int NUM_ROWS_PER_FLUSH = NUM_ROWS / NUM_HFILES;

  private final HBaseTestingUtility testUtil = HBaseTestingUtility.createLocalHTU();
  private final DataBlockEncoding encoding;
  private final boolean includeTags;
  private final boolean compressTags;

  /** Enable when debugging */
  private static final boolean VERBOSE = false;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      for (boolean includeTags : new boolean[] { false, true }) {
        for (boolean compressTags : new boolean[] { false, true }) {
          paramList.add(new Object[] { encoding, includeTags, compressTags });
        }
      }
    }
    return paramList;
  }

  public TestEncodedSeekers(DataBlockEncoding encoding, boolean includeTags, boolean compressTags) {
    this.encoding = encoding;
    this.includeTags = includeTags;
    this.compressTags = compressTags;
  }

  @Test
  public void testEncodedSeeker() throws IOException {
    System.err.println("Testing encoded seekers for encoding : " + encoding + ", includeTags : "
        + includeTags + ", compressTags : " + compressTags);
    if(includeTags) {
      testUtil.getConfiguration().setInt(HFile.FORMAT_VERSION_KEY, 3);
    }
    LruBlockCache cache =
      (LruBlockCache)new CacheConfig(testUtil.getConfiguration()).getBlockCache();
    cache.clearCache();
    // Need to disable default row bloom filter for this test to pass.
    HColumnDescriptor hcd = (new HColumnDescriptor(CF_NAME)).setMaxVersions(MAX_VERSIONS).
        setDataBlockEncoding(encoding).
        setBlocksize(BLOCK_SIZE).
        setBloomFilterType(BloomType.NONE).
        setCompressTags(compressTags);
    HRegion region = testUtil.createTestRegion(TABLE_NAME, hcd);

    //write the data, but leave some in the memstore
    doPuts(region);

    //verify correctness when memstore contains data
    doGets(region);

    //verify correctness again after compacting
    region.compactStores();
    doGets(region);


    Map<DataBlockEncoding, Integer> encodingCounts = cache.getEncodingCountsForTest();

    // Ensure that compactions don't pollute the cache with unencoded blocks
    // in case of in-cache-only encoding.
    System.err.println("encodingCounts=" + encodingCounts);
    assertEquals(1, encodingCounts.size());
    DataBlockEncoding encodingInCache = encodingCounts.keySet().iterator().next();
    assertEquals(encoding, encodingInCache);
    assertTrue(encodingCounts.get(encodingInCache) > 0);
  }


  private void doPuts(HRegion region) throws IOException{
    LoadTestKVGenerator dataGenerator = new LoadTestKVGenerator(MIN_VALUE_SIZE, MAX_VALUE_SIZE);
     for (int i = 0; i < NUM_ROWS; ++i) {
      byte[] key = LoadTestKVGenerator.md5PrefixedKey(i).getBytes();
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        Put put = new Put(key);
        put.setDurability(Durability.ASYNC_WAL);
        byte[] col = Bytes.toBytes(String.valueOf(j));
        byte[] value = dataGenerator.generateRandomSizeValue(key, col);
        if (includeTags) {
          Tag[] tag = new Tag[1];
          tag[0] = new Tag((byte) 1, "Visibility");
          KeyValue kv = new KeyValue(key, CF_BYTES, col, HConstants.LATEST_TIMESTAMP, value, tag);
          put.add(kv);
        } else {
          put.add(CF_BYTES, col, value);
        }
        if(VERBOSE){
          KeyValue kvPut = new KeyValue(key, CF_BYTES, col, value);
          System.err.println(Strings.padFront(i+"", ' ', 4)+" "+kvPut);
        }
        region.put(put);
      }
      if (i % NUM_ROWS_PER_FLUSH == 0) {
        region.flushcache();
      }
    }
  }


  private void doGets(HRegion region) throws IOException{
    for (int i = 0; i < NUM_ROWS; ++i) {
      final byte[] rowKey = LoadTestKVGenerator.md5PrefixedKey(i).getBytes();
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        final String qualStr = String.valueOf(j);
        if (VERBOSE) {
          System.err.println("Reading row " + i + ", column " + j + " " + Bytes.toString(rowKey)+"/"
              +qualStr);
        }
        final byte[] qualBytes = Bytes.toBytes(qualStr);
        Get get = new Get(rowKey);
        get.addColumn(CF_BYTES, qualBytes);
        Result result = region.get(get);
        assertEquals(1, result.size());
        byte[] value = result.getValue(CF_BYTES, qualBytes);
        assertTrue(LoadTestKVGenerator.verify(value, rowKey, qualBytes));
      }
    }
  }

}
