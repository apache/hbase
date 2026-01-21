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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CompoundRibbonFilter;
import org.apache.hadoop.hbase.io.hfile.CompoundRibbonFilterWriter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests writing and reading Compound Ribbon filters in HFiles.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestCompoundRibbonFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompoundRibbonFilter.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final Logger LOG = LoggerFactory.getLogger(TestCompoundRibbonFilter.class);

  // Test configurations
  private static final int NUM_TESTS = 4;
  private static final BloomType[] RIBBON_TYPES = { BloomType.RIBBON_ROW, BloomType.RIBBON_ROW,
    BloomType.RIBBON_ROWCOL, BloomType.RIBBON_ROWCOL };

  private static final int[] NUM_KV;
  static {
    final int N = 5000;
    NUM_KV = new int[] { N, N * 2, N, N * 2 };
  }

  private static final int[] BLOCK_SIZES;
  static {
    final int blkSize = 65536;
    BLOCK_SIZES = new int[] { blkSize, blkSize, blkSize, blkSize };
  }

  // Ribbon filter block sizes for testing (controls chunk size)
  // These are chosen to create appropriate number of keys per chunk
  // ICML uses ~7 bits/key for 1% FPR, plus overhead for slots
  private static final int[] RIBBON_BLOCK_SIZES = { 1024, 2048, 1024, 2048 };

  private static Configuration conf;
  private static CacheConfig cacheConf;
  private FileSystem fs;

  private String testIdMsg;

  private static final int GENERATION_SEED = 2319;
  private static final int EVALUATION_SEED = 135;

  private BlockCache blockCache;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    fs = FileSystem.get(conf);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
  }

  private List<KeyValue> createSortedKeyValues(Random rand, int n) {
    List<KeyValue> kvList = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      kvList.add(RandomKeyValueUtil.randomKeyValue(rand));
    }
    kvList.sort(CellComparatorImpl.COMPARATOR);
    return kvList;
  }

  @Test
  public void testCompoundRibbonFilter() throws IOException {
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    for (int t = 0; t < NUM_TESTS; ++t) {
      // Configure Ribbon filter parameters (block size controls chunk size)
      conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, RIBBON_BLOCK_SIZES[t]);

      testIdMsg = "in test #" + t + ":";
      Random generationRand = new Random(GENERATION_SEED);
      List<KeyValue> kvs = createSortedKeyValues(generationRand, NUM_KV[t]);
      BloomType bt = RIBBON_TYPES[t];
      Path sfPath = writeStoreFile(t, bt, kvs);
      readStoreFile(t, bt, kvs, sfPath);
    }
  }

  private void readStoreFile(int t, BloomType bt, List<KeyValue> kvs, Path sfPath)
    throws IOException {
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, sfPath, true);
    HStoreFile sf = new HStoreFile(storeFileInfo, bt, cacheConf);
    sf.initReader();
    StoreFileReader r = sf.getReader();
    StoreFileScanner scanner = r.getStoreFileScanner(true, true, false, 0, 0, false);

    // Test for false negatives (not allowed)
    int numChecked = 0;
    for (KeyValue kv : kvs) {
      byte[] row = CellUtil.cloneRow(kv);
      boolean present = isInBloom(scanner, row, CellUtil.cloneQualifier(kv));
      assertTrue(testIdMsg + " Ribbon filter false negative on row " + Bytes.toStringBinary(row)
        + " after " + numChecked + " successful checks", present);
      ++numChecked;
    }

    // Test for false positives
    CompoundRibbonFilter crf = (CompoundRibbonFilter) r.getGeneralBloomFilter();
    int numFalsePos = 0;
    Random rand = new Random(EVALUATION_SEED);
    int nTrials = NUM_KV[t] * 5;
    for (int i = 0; i < nTrials; ++i) {
      byte[] query = RandomKeyValueUtil.randomRowOrQualifier(rand);
      if (isInBloom(scanner, query, rand)) {
        numFalsePos += 1;
      }
    }
    double falsePosRate = numFalsePos * 1.0 / nTrials;
    LOG.info("{} False positives: {} out of {} ({}) - overhead={}, chunks={}", testIdMsg,
      numFalsePos, nTrials, falsePosRate, crf.getOverheadRatio(), crf.getNumChunks());

    // FPR should be within 2x of expected error rate
    float expectedFpr = conf.getFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, 0.01f);
    double maxAllowedFpr = expectedFpr * 2;
    assertTrue(
      "False positive rate too high: " + falsePosRate + " (expected < " + maxAllowedFpr + ")",
      falsePosRate < maxAllowedFpr);

    r.close(true);
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row, Random rand) {
    return isInBloom(scanner, row, RandomKeyValueUtil.randomRowOrQualifier(rand));
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row, byte[] qualifier) {
    Scan scan = new Scan().withStartRow(row).withStopRow(row, true);
    scan.addColumn(Bytes.toBytes(RandomKeyValueUtil.COLUMN_FAMILY_NAME), qualifier);
    HStore store = mock(HStore.class);
    when(store.getColumnFamilyDescriptor())
      .thenReturn(ColumnFamilyDescriptorBuilder.of(RandomKeyValueUtil.COLUMN_FAMILY_NAME));
    return scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
  }

  private Path writeStoreFile(int t, BloomType bt, List<KeyValue> kvs) throws IOException {
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    cacheConf = new CacheConfig(conf, blockCache);
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCK_SIZES[t]).build();
    StoreFileWriter w = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withOutputDir(TEST_UTIL.getDataTestDir()).withBloomType(bt).withFileContext(meta).build();

    assertTrue(w.hasGeneralBloom());
    assertTrue(
      "Expected CompoundRibbonFilterWriter but got " + w.getGeneralBloomWriter().getClass(),
      w.getGeneralBloomWriter() instanceof CompoundRibbonFilterWriter);
    CompoundRibbonFilterWriter crfw = (CompoundRibbonFilterWriter) w.getGeneralBloomWriter();

    int keyCount = 0;
    KeyValue prev = null;
    LOG.debug("Total keys/values to insert: {}", kvs.size());
    for (KeyValue kv : kvs) {
      w.append(kv);

      // Validate the key count
      boolean newKey = true;
      if (prev != null) {
        newKey = !(bt.isRowOnly()
          ? CellUtil.matchingRows(kv, prev)
          : CellUtil.matchingRowColumn(kv, prev));
      }
      if (newKey) {
        ++keyCount;
      }
      assertEquals(keyCount, crfw.getKeyCount());

      prev = kv;
    }
    w.close();

    return w.getPath();
  }

  @Test
  public void testPerChunkNumSlotsMetadata() throws IOException {
    // Test that per-chunk numSlots are correctly stored in metadata
    // by creating multiple chunks with different sizes
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    // Small block size to create multiple chunks (~500 keys per chunk)
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, 550);

    testIdMsg = "in per-chunk numSlots test:";
    Random generationRand = new Random(GENERATION_SEED);
    // 2500 keys with ~500 max per chunk = ~5 chunks
    List<KeyValue> kvs = createSortedKeyValues(generationRand, 2500);

    Path sfPath = writeStoreFile(0, BloomType.RIBBON_ROW, kvs);

    // Read and verify per-chunk numSlots metadata
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, sfPath, true);
    HStoreFile sf = new HStoreFile(storeFileInfo, BloomType.RIBBON_ROW, cacheConf);
    sf.initReader();
    StoreFileReader r = sf.getReader();
    CompoundRibbonFilter crf = (CompoundRibbonFilter) r.getGeneralBloomFilter();

    int numChunks = crf.getNumChunks();
    LOG.info("{} Created {} chunks", testIdMsg, numChunks);
    assertTrue("Should have multiple chunks", numChunks > 1);

    // Verify all keys are found (this confirms metadata is read correctly)
    StoreFileScanner scanner = r.getStoreFileScanner(true, true, false, 0, 0, false);
    int numChecked = 0;
    for (KeyValue kv : kvs) {
      byte[] row = CellUtil.cloneRow(kv);
      boolean present = isInBloom(scanner, row, CellUtil.cloneQualifier(kv));
      assertTrue(testIdMsg + " Ribbon filter false negative on row " + Bytes.toStringBinary(row),
        present);
      ++numChecked;
    }
    LOG.info("{} Verified {} keys across {} chunks with per-chunk numSlots metadata", testIdMsg,
      numChecked, numChunks);

    r.close(true);
  }

  @Test
  public void testDeleteFamilyRibbonFilter() throws IOException {
    // Enable Delete Family Bloom filter (Ribbon type will be used because general bloom is Ribbon)
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED, true);

    testIdMsg = "in Delete Family Ribbon test:";

    // Write the file
    Path testDir = TEST_UTIL.getDataTestDir("delete_family_ribbon_test");
    fs.mkdirs(testDir);
    Path filePath = StoreFileWriter.getUniqueFile(fs, testDir);

    HFileContext meta = new HFileContextBuilder().withBlockSize(65536).build();
    // Use RIBBON_ROW so delete family filter also uses Ribbon
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(filePath)
      .withBloomType(BloomType.RIBBON_ROW).withMaxKeyCount(2000).withFileContext(meta).build();

    // Add delete family markers for even rows
    long now = System.currentTimeMillis();
    for (int i = 0; i < 2000; i += 2) {
      String row = String.format("%08d", i);
      KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes.toBytes("col"),
        now, KeyValue.Type.DeleteFamily, Bytes.toBytes("value"));
      writer.append(kv);
    }
    writer.close();

    // Read and verify
    StoreFileInfo storeFileInfo =
      StoreFileInfo.createStoreFileInfoForHFile(conf, fs, filePath, true);
    HStoreFile sf = new HStoreFile(storeFileInfo, BloomType.RIBBON_ROW, cacheConf);
    sf.initReader();
    StoreFileReader reader = sf.getReader();

    // Check that delete family ribbon filter works
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      String row = String.format("%08d", i);
      byte[] rowKey = Bytes.toBytes(row);
      boolean exists = reader.passesDeleteFamilyBloomFilter(rowKey, 0, rowKey.length);
      if (i % 2 == 0) {
        // Even rows have delete family markers - should be found
        if (!exists) {
          falseNeg++;
        }
      } else {
        // Odd rows don't have delete family markers - false positives counted
        if (exists) {
          falsePos++;
        }
      }
    }

    // There should be 1000 delete family markers
    assertEquals(testIdMsg + " Delete family count should be 1000", 1000,
      reader.getDeleteFamilyCnt());

    // No false negatives allowed
    assertEquals(testIdMsg + " Should have no false negatives", 0, falseNeg);

    // FPR should be within 2x of expected error rate
    double fpr = (double) falsePos / 1000;
    LOG.info("{} Delete Family Ribbon - False positives: {} out of 1000 (FPR={})", testIdMsg,
      falsePos, fpr);
    float expectedFpr = conf.getFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, 0.01f);
    double maxAllowedFpr = expectedFpr * 2;
    assertTrue("False positive rate too high: " + fpr + " (expected < " + maxAllowedFpr + ")",
      fpr < maxAllowedFpr);

    reader.close(true);

    LOG.info("{} Successfully verified Delete Family Ribbon filter", testIdMsg);
  }
}
