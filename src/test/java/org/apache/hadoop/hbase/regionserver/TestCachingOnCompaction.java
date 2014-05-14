/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test caching on Compaction
 */
@Category(MediumTests.class)
public class TestCachingOnCompaction {
  static final Log LOG = LogFactory.getLog(TestCachingOnCompaction.class.getName());
  private int compactionThreshold;
  byte [] TABLE = null;
  HTable ht = null;
  HRegion region = null;
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf = null;
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static int SLAVES = 2;
  protected static final int MAXVERSIONS = 3;

  /** constructor */
  public TestCachingOnCompaction() throws Exception {
    super();
    conf = TEST_UTIL.getConfiguration();
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 2);
    conf.set("hfile.block.cacheoncompaction", "true");
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  /**
   * Disables the cacheOnCompaction feature and tests to make sure that
   * the blocks are not cached after compaction, even if the
   * cacheOnCompactionThreshold is met.
   * @throws Exception
   */
  public void testAutoCachingOnCompactionDisabled() throws Exception {
    conf.setBoolean("hfile.block.cacheoncompaction", false);
    conf.setFloat("hfile.block.cacheoncompaction.threshold", 0.7F);
    setupAndCompact("testAutoCachingOnCompactionDisabled", DataBlockEncoding.NONE);
    int cacheHitPercentage = scanRegion();
    ht.close();
    TEST_UTIL.deleteTable(TABLE);
    assert (cacheHitPercentage == 0);
  }

  @Test
  /**
   * Verifies that if the cacheOnCompaction threshold is not met, then the
   * blocks are not cached after compaction.
   * @throws Exception
   */
  public void testNoCachingOnCompaction() throws Exception {
    conf.setBoolean("hfile.block.cacheoncompaction", true);
    conf.setFloat("hfile.block.cacheoncompaction.threshold", 0.8F);
    setupAndCompact("testNoCachingOnCompaction", DataBlockEncoding.NONE);
    int cacheHitPercentage = scanRegion();
    ht.close();
    TEST_UTIL.deleteTable(TABLE);
    assert(cacheHitPercentage == 0);
  }

  @Test
  /**
   * Verifies that if the cacheOnCompaction threshold is met, then the blocks
   * are cached after compaction.
   * @throws Exception
   */
  public void testFullCachingOnCompaction() throws Exception {
    conf.setBoolean("hfile.block.cacheoncompaction", true);
    conf.setFloat("hfile.block.cacheoncompaction.threshold", 0.7F);
    setupAndCompact("testFullCachingOnCompaction", DataBlockEncoding.NONE);
    int cacheHitPercentage = scanRegion();
    ht.close();
    TEST_UTIL.deleteTable(TABLE);
    assert(cacheHitPercentage == 100);
  }

  @Test
  /**
   * Verifies that only blocks that meet the criteria are cached after
   * compaction.
   * @throws Exception
   */
  public void testHalfCachingOnCompaction() throws Exception {
    conf.setBoolean("hfile.block.cacheoncompaction", true);
    conf.setFloat("hfile.block.cacheoncompaction.threshold", 0.6F);
    setupAndCompact("testHalfCachingOnCompaction", DataBlockEncoding.NONE);

    // Create more entries to have a mix of cached and non cached blocks
    // for compaction and then scan the region to get the cachitHit %.
    createStoreFile(1000, 1000);
    createStoreFile(1000, 2000);
    region.compactStores();
    int cacheHitPercentage = scanRegion();
    ht.close();
    TEST_UTIL.deleteTable(TABLE);

    assert(cacheHitPercentage > 33 && cacheHitPercentage < 75);
  }

  @Test
  /**
   * Verifies that only blocks that meet the criteria are cached after
   * compaction with PREFIX encoding enabled
   * @throws Exception
   */
  public void testHalfCachingOnCompactionWithEncoding() throws Exception {
    conf.setBoolean("hfile.block.cacheoncompaction", true);
    conf.setFloat("hfile.block.cacheoncompaction.threshold", 0.59F);
    setupAndCompact("testHalfCachingOnCompactionWithEncoding", DataBlockEncoding.PREFIX);

    // Create more entries to have a mix of cached and non cached blocks
    // for compaction and then scan the region to get the cachitHit %.

    createStoreFile(1000, 1000);
    createStoreFile(1000, 2000);

    region.compactStores();
    int cacheHitPercentage = scanRegion();
    ht.close();
    TEST_UTIL.deleteTable(TABLE);

    assert(cacheHitPercentage > 33 && cacheHitPercentage < 75);
  }


  /**
   * The method creates a table and then creates 3 store-files with the same
   * information.It then scans all the rows, so that the blocks for these store
   * files are cached. In then creates one more store file with the same set
   * of KVs. It then issues a compaction, so now the % of cachedKeys/block
   * should be 75%. Depending upon the cacheOnCompactionThreshold set by the
   * caller, the blocks will be cached accordingly.
   *
   * @param name of the table to setup
   * @throws IOException
   */
  public void setupAndCompact(String tableName, DataBlockEncoding type) throws Exception {
    TABLE = Bytes.toBytes(tableName);
    int NUM_REGIONS = 3;

    ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaa"), Bytes.toBytes("zzzz"), NUM_REGIONS);

    HColumnDescriptor col = new HColumnDescriptor(FAMILY);
    col.setMaxVersions(MAXVERSIONS);
    col.setBlockCacheEnabled(true);
    col.setDataBlockEncoding(type);

    ht.getConnectionAndResetOperationContext()
        .getHTableDescriptor(new StringBytes(TABLE)).addFamily(col);

    region = TEST_UTIL.createTestRegion(tableName, col);

    // Create Store files
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(1000, 0);
    }

    // Set caching to true and enable cacheOnRead for each Store File in the Region
    Scan s1 = new Scan();
    s1.setCacheBlocks(true);

    InternalScanner s = region.getScanner(s1);

    // Scan the keys to get make sure that the Block Cache caches these blocks
    do {
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean result = s.next(results);
      if (!result) break;
    } while(true);

    // Create one more store file to reduce the over all caching factor
    createStoreFile(1000, 0);

    // Major Compaction, with blocks of one store file not present in Cache
    region.compactStores();
  }

  /**
   * Creates a store file.
   * @param numRows number of rows in the store file
   * @param startKey the start row key, from which it then monotonically
   *        increases for numRows times.
   * @throws IOException
   */
  public void createStoreFile(int numRows, int startKey) throws IOException {
    for (long i = startKey; i < (startKey + numRows); i++) {
      byte [] rowKey = Bytes.toBytes(i);
      Put put = new Put(rowKey);
      byte[] value = rowKey; // value is the same as the row key
      put.add(FAMILY, QUALIFIER, value);
      try {
        region.put(put);
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }
    }
    region.flushcache();
  }

  /**
   * Scans the region, and keeps a track of number of KVs obtained from cache
   * and returns the %.
   *
   * @return int cache hit percentage
   * @throws IOException
   */
  private int scanRegion() throws IOException {
    int cacheHitPercentage = 0;

    int numKeysObtainedFromCache = 0;
    int numKeysScanned = 0;
    KeyValueContext kvContext = new KeyValueContext();
    Scan s2 = new Scan();
    s2.setCacheBlocks(false);

    InternalScanner afterScanner = region.getScanner(s2);
    do {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      boolean result = false;
      try {
        result = afterScanner.next(kvs, 1, kvContext);
        if (!result) {
          break;
        }

        numKeysScanned++;
        if (kvContext.getObtainedFromCache()) {
          numKeysObtainedFromCache++;
        }
      } catch (IOException e) {
        throw e;
      }

    } while(true);
    LOG.info("KEYS, from Cache: " + numKeysObtainedFromCache + " , total: " + numKeysScanned);

    cacheHitPercentage = (numKeysObtainedFromCache * 100)/numKeysScanned;
    return cacheHitPercentage;
  }
}
