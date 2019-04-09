/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterWriter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests writing Bloom filter blocks in the same part of the file as data
 * blocks.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestCompoundBloomFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompoundBloomFilter.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final Logger LOG = LoggerFactory.getLogger(
      TestCompoundBloomFilter.class);

  private static final int NUM_TESTS = 9;
  private static final BloomType BLOOM_TYPES[] = { BloomType.ROW,
      BloomType.ROW, BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROW,
      BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROW };

  private static final int NUM_KV[];
  static {
    final int N = 10000; // Only used in initialization.
    NUM_KV = new int[] { 21870, N, N, N, N, 1000, N, 7500, 7500};
    assert NUM_KV.length == NUM_TESTS;
  }

  private static final int BLOCK_SIZES[];
  static {
    final int blkSize = 65536;
    BLOCK_SIZES = new int[] { 512, 1000, blkSize, blkSize, blkSize, 128, 300,
        blkSize, blkSize };
    assert BLOCK_SIZES.length == NUM_TESTS;
  }

  /**
   * Be careful not to specify too high a Bloom filter block size, otherwise
   * there will only be one oversized chunk and the observed false positive
   * rate will be too low.
   */
  private static final int BLOOM_BLOCK_SIZES[] = { 1000, 4096, 4096, 4096,
      8192, 128, 1024, 600, 600 };
  static { assert BLOOM_BLOCK_SIZES.length == NUM_TESTS; }

  private static final double TARGET_ERROR_RATES[] = { 0.025, 0.01, 0.015,
      0.01, 0.03, 0.01, 0.01, 0.07, 0.07 };
  static { assert TARGET_ERROR_RATES.length == NUM_TESTS; }

  /** A false positive rate that is obviously too high. */
  private static final double TOO_HIGH_ERROR_RATE;
  static {
    double m = 0;
    for (double errorRate : TARGET_ERROR_RATES)
      m = Math.max(m, errorRate);
    TOO_HIGH_ERROR_RATE = m + 0.03;
  }

  private static Configuration conf;
  private static CacheConfig cacheConf;
  private FileSystem fs;

  /** A message of the form "in test#&lt;number>:" to include in logging. */
  private String testIdMsg;

  private static final int GENERATION_SEED = 2319;
  private static final int EVALUATION_SEED = 135;

  private BlockCache blockCache;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();

    // This test requires the most recent HFile format (i.e. v2).
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);

    fs = FileSystem.get(conf);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
  }

  private List<KeyValue> createSortedKeyValues(Random rand, int n) {
    List<KeyValue> kvList = new ArrayList<>(n);
    for (int i = 0; i < n; ++i)
      kvList.add(RandomKeyValueUtil.randomKeyValue(rand));
    Collections.sort(kvList, CellComparatorImpl.COMPARATOR);
    return kvList;
  }

  @Test
  public void testCompoundBloomFilter() throws IOException {
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    for (int t = 0; t < NUM_TESTS; ++t) {
      conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE,
          (float) TARGET_ERROR_RATES[t]);

      testIdMsg = "in test #" + t + ":";
      Random generationRand = new Random(GENERATION_SEED);
      List<KeyValue> kvs = createSortedKeyValues(generationRand, NUM_KV[t]);
      BloomType bt = BLOOM_TYPES[t];
      Path sfPath = writeStoreFile(t, bt, kvs);
      readStoreFile(t, bt, kvs, sfPath);
    }
  }

  /**
   * Validates the false positive ratio by computing its z-value and comparing
   * it to the provided threshold.
   *
   * @param falsePosRate experimental positive rate
   * @param nTrials the number of Bloom filter checks
   * @param zValueBoundary z-value boundary, positive for an upper bound and
   *          negative for a lower bound
   * @param cbf the compound Bloom filter we are using
   * @param additionalMsg additional message to include in log output and
   *          assertion failures
   */
  private void validateFalsePosRate(double falsePosRate, int nTrials,
      double zValueBoundary, CompoundBloomFilter cbf, String additionalMsg) {
    double p = BloomFilterFactory.getErrorRate(conf);
    double zValue = (falsePosRate - p) / Math.sqrt(p * (1 - p) / nTrials);

    String assortedStatsStr = " (targetErrorRate=" + p + ", falsePosRate="
        + falsePosRate + ", nTrials=" + nTrials + ")";
    LOG.info("z-value is " + zValue + assortedStatsStr);

    boolean isUpperBound = zValueBoundary > 0;

    if (isUpperBound && zValue > zValueBoundary ||
        !isUpperBound && zValue < zValueBoundary) {
      String errorMsg = "False positive rate z-value " + zValue + " is "
          + (isUpperBound ? "higher" : "lower") + " than " + zValueBoundary
          + assortedStatsStr + ". Per-chunk stats:\n"
          + cbf.formatTestingStats();
      fail(errorMsg + additionalMsg);
    }
  }

  private void readStoreFile(int t, BloomType bt, List<KeyValue> kvs,
      Path sfPath) throws IOException {
    HStoreFile sf = new HStoreFile(fs, sfPath, conf, cacheConf, bt, true);
    sf.initReader();
    StoreFileReader r = sf.getReader();
    final boolean pread = true; // does not really matter
    StoreFileScanner scanner = r.getStoreFileScanner(true, pread, false, 0, 0, false);

    {
      // Test for false negatives (not allowed).
      int numChecked = 0;
      for (KeyValue kv : kvs) {
        byte[] row = CellUtil.cloneRow(kv);
        boolean present = isInBloom(scanner, row, CellUtil.cloneQualifier(kv));
        assertTrue(testIdMsg + " Bloom filter false negative on row "
            + Bytes.toStringBinary(row) + " after " + numChecked
            + " successful checks", present);
        ++numChecked;
      }
    }

    // Test for false positives (some percentage allowed). We test in two modes:
    // "fake lookup" which ignores the key distribution, and production mode.
    for (boolean fakeLookupEnabled : new boolean[] { true, false }) {
      if (fakeLookupEnabled) {
        BloomFilterUtil.setRandomGeneratorForTest(new Random(283742987L));
      }
      try {
        String fakeLookupModeStr = ", fake lookup is " + (fakeLookupEnabled ?
            "enabled" : "disabled");
        CompoundBloomFilter cbf = (CompoundBloomFilter) r.getGeneralBloomFilter();
        cbf.enableTestingStats();
        int numFalsePos = 0;
        Random rand = new Random(EVALUATION_SEED);
        int nTrials = NUM_KV[t] * 10;
        for (int i = 0; i < nTrials; ++i) {
          byte[] query = RandomKeyValueUtil.randomRowOrQualifier(rand);
          if (isInBloom(scanner, query, bt, rand)) {
            numFalsePos += 1;
          }
        }
        double falsePosRate = numFalsePos * 1.0 / nTrials;
        LOG.debug(String.format(testIdMsg
            + " False positives: %d out of %d (%f)",
            numFalsePos, nTrials, falsePosRate) + fakeLookupModeStr);

        // Check for obvious Bloom filter crashes.
        assertTrue("False positive is too high: " + falsePosRate + " (greater "
            + "than " + TOO_HIGH_ERROR_RATE + ")" + fakeLookupModeStr,
            falsePosRate < TOO_HIGH_ERROR_RATE);

        // Now a more precise check to see if the false positive rate is not
        // too high. The reason we use a relaxed restriction for the real-world
        // case as opposed to the "fake lookup" case is that our hash functions
        // are not completely independent.

        double maxZValue = fakeLookupEnabled ? 1.96 : 2.5;
        validateFalsePosRate(falsePosRate, nTrials, maxZValue, cbf,
            fakeLookupModeStr);

        // For checking the lower bound we need to eliminate the last chunk,
        // because it is frequently smaller and the false positive rate in it
        // is too low. This does not help if there is only one under-sized
        // chunk, though.
        int nChunks = cbf.getNumChunks();
        if (nChunks > 1) {
          numFalsePos -= cbf.getNumPositivesForTesting(nChunks - 1);
          nTrials -= cbf.getNumQueriesForTesting(nChunks - 1);
          falsePosRate = numFalsePos * 1.0 / nTrials;
          LOG.info(testIdMsg + " False positive rate without last chunk is " +
              falsePosRate + fakeLookupModeStr);
        }

        validateFalsePosRate(falsePosRate, nTrials, -2.58, cbf,
            fakeLookupModeStr);
      } finally {
        BloomFilterUtil.setRandomGeneratorForTest(null);
      }
    }

    r.close(true); // end of test so evictOnClose
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row, BloomType bt,
      Random rand) {
    return isInBloom(scanner, row, RandomKeyValueUtil.randomRowOrQualifier(rand));
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row,
      byte[] qualifier) {
    Scan scan = new Scan().withStartRow(row).withStopRow(row, true);
    scan.addColumn(Bytes.toBytes(RandomKeyValueUtil.COLUMN_FAMILY_NAME), qualifier);
    HStore store = mock(HStore.class);
    when(store.getColumnFamilyDescriptor())
        .thenReturn(ColumnFamilyDescriptorBuilder.of(RandomKeyValueUtil.COLUMN_FAMILY_NAME));
    return scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
  }

  private Path writeStoreFile(int t, BloomType bt, List<KeyValue> kvs)
      throws IOException {
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZES[t]);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    cacheConf = new CacheConfig(conf, blockCache);
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCK_SIZES[t]).build();
    StoreFileWriter w = new StoreFileWriter.Builder(conf, cacheConf, fs)
            .withOutputDir(TEST_UTIL.getDataTestDir())
            .withBloomType(bt)
            .withFileContext(meta)
            .build();

    assertTrue(w.hasGeneralBloom());
    assertTrue(w.getGeneralBloomWriter() instanceof CompoundBloomFilterWriter);
    CompoundBloomFilterWriter cbbf =
        (CompoundBloomFilterWriter) w.getGeneralBloomWriter();

    int keyCount = 0;
    KeyValue prev = null;
    LOG.debug("Total keys/values to insert: " + kvs.size());
    for (KeyValue kv : kvs) {
      w.append(kv);

      // Validate the key count in the Bloom filter.
      boolean newKey = true;
      if (prev != null) {
        newKey = !(bt == BloomType.ROW ? CellUtil.matchingRows(kv,
            prev) : CellUtil.matchingRowColumn(kv, prev));
      }
      if (newKey)
        ++keyCount;
      assertEquals(keyCount, cbbf.getKeyCount());

      prev = kv;
    }
    w.close();

    return w.getPath();
  }

  @Test
  public void testCompoundBloomSizing() {
    int bloomBlockByteSize = 4096;
    int bloomBlockBitSize = bloomBlockByteSize * 8;
    double targetErrorRate = 0.01;
    long maxKeysPerChunk = BloomFilterUtil.idealMaxKeys(bloomBlockBitSize,
        targetErrorRate);

    long bloomSize1 = bloomBlockByteSize * 8;
    long bloomSize2 = BloomFilterUtil.computeBitSize(maxKeysPerChunk,
        targetErrorRate);

    double bloomSizeRatio = (bloomSize2 * 1.0 / bloomSize1);
    assertTrue(Math.abs(bloomSizeRatio - 0.9999) < 0.0001);
  }

  @Test
  public void testCreateKey() {
    byte[] row = Bytes.toBytes("myRow");
    byte[] qualifier = Bytes.toBytes("myQualifier");
    // Mimic what Storefile.createBloomKeyValue() does
    byte[] rowKey = KeyValueUtil.createFirstOnRow(row, 0, row.length, new byte[0], 0, 0, row, 0, 0).getKey();
    byte[] rowColKey = KeyValueUtil.createFirstOnRow(row, 0, row.length,
        new byte[0], 0, 0, qualifier, 0, qualifier.length).getKey();
    KeyValue rowKV = KeyValueUtil.createKeyValueFromKey(rowKey);
    KeyValue rowColKV = KeyValueUtil.createKeyValueFromKey(rowColKey);
    assertEquals(rowKV.getTimestamp(), rowColKV.getTimestamp());
    assertEquals(Bytes.toStringBinary(rowKV.getRowArray(), rowKV.getRowOffset(),
      rowKV.getRowLength()), Bytes.toStringBinary(rowColKV.getRowArray(), rowColKV.getRowOffset(),
      rowColKV.getRowLength()));
    assertEquals(0, rowKV.getQualifierLength());
  }
}

