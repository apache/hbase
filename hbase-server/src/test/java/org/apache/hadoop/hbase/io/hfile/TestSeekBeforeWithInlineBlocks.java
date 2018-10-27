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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IOTests.class, MediumTests.class})
public class TestSeekBeforeWithInlineBlocks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSeekBeforeWithInlineBlocks.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSeekBeforeWithInlineBlocks.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final int NUM_KV = 10000;

  private static final int DATA_BLOCK_SIZE = 4096;
  private static final int BLOOM_BLOCK_SIZE = 1024;
  private static final int[] INDEX_CHUNK_SIZES = { 65536, 4096, 1024 };
  private static final int[] EXPECTED_NUM_LEVELS = { 1, 2, 3 };

  private static final Random RAND = new Random(192537);
  private static final byte[] FAM = Bytes.toBytes("family");

  private FileSystem fs;
  private Configuration conf;

  /**
   * Scanner.seekBefore() could fail because when seeking to a previous HFile data block, it needs
   * to know the size of that data block, which it calculates using current data block offset and
   * the previous data block offset.  This fails to work when there are leaf-level index blocks in
   * the scannable section of the HFile, i.e. starting in HFileV2.  This test will try seekBefore()
   * on a flat (single-level) and multi-level (2,3) HFile and confirm this bug is now fixed.  This
   * bug also happens for inline Bloom blocks for the same reasons.
   */
  @Test
  public void testMultiIndexLevelRandomHFileWithBlooms() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.getConfiguration().setInt(BloomFilterUtil.PREFIX_LENGTH_KEY, 10);
    TEST_UTIL.getConfiguration().set(BloomFilterUtil.DELIMITER_KEY, "#");

    // Try out different HFile versions to ensure reverse scan works on each version
    for (int hfileVersion = HFile.MIN_FORMAT_VERSION_WITH_TAGS;
            hfileVersion <= HFile.MAX_FORMAT_VERSION; hfileVersion++) {

      conf.setInt(HFile.FORMAT_VERSION_KEY, hfileVersion);
      fs = HFileSystem.get(conf);

      // Try out different bloom types because inline Bloom blocks break seekBefore()
      for (BloomType bloomType : BloomType.values()) {

        // Test out HFile block indices of various sizes/levels
        for (int testI = 0; testI < INDEX_CHUNK_SIZES.length; testI++) {
          int indexBlockSize = INDEX_CHUNK_SIZES[testI];
          int expectedNumLevels = EXPECTED_NUM_LEVELS[testI];

          LOG.info(String.format("Testing HFileVersion: %s, BloomType: %s, Index Levels: %s",
            hfileVersion, bloomType, expectedNumLevels));

          conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, indexBlockSize);
          conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, BLOOM_BLOCK_SIZE);
          conf.setInt(BloomFilterUtil.PREFIX_LENGTH_KEY, 10);
          conf.set(BloomFilterUtil.DELIMITER_KEY, "#");

          Cell[] cells = new Cell[NUM_KV];

          Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
            String.format("testMultiIndexLevelRandomHFileWithBlooms-%s-%s-%s",
              hfileVersion, bloomType, testI));

          // Disable caching to prevent it from hiding any bugs in block seeks/reads
          conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
          CacheConfig cacheConf = new CacheConfig(conf);

          // Write the HFile
          {
            HFileContext meta = new HFileContextBuilder()
                                .withBlockSize(DATA_BLOCK_SIZE)
                                .build();

            StoreFileWriter storeFileWriter =
                new StoreFileWriter.Builder(conf, cacheConf, fs)
              .withFilePath(hfilePath)
              .withFileContext(meta)
              .withBloomType(bloomType)
              .build();

            for (int i = 0; i < NUM_KV; i++) {
              byte[] row = RandomKeyValueUtil.randomOrderedKey(RAND, i);
              byte[] qual = RandomKeyValueUtil.randomRowOrQualifier(RAND);
              byte[] value = RandomKeyValueUtil.randomValue(RAND);
              KeyValue kv = new KeyValue(row, FAM, qual, value);

              storeFileWriter.append(kv);
              cells[i] = kv;
            }

            storeFileWriter.close();
          }

          // Read the HFile
          HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, conf);

          // Sanity check the HFile index level
          assertEquals(expectedNumLevels, reader.getTrailer().getNumDataIndexLevels());

          // Check that we can seekBefore in either direction and with both pread
          // enabled and disabled
          for (boolean pread : new boolean[] { false, true }) {
            HFileScanner scanner = reader.getScanner(true, pread);
            checkNoSeekBefore(cells, scanner, 0);
            for (int i = 1; i < NUM_KV; i++) {
              checkSeekBefore(cells, scanner, i);
              checkCell(cells[i-1], scanner.getCell());
            }
            assertTrue(scanner.seekTo());
            for (int i = NUM_KV - 1; i >= 1; i--) {
              checkSeekBefore(cells, scanner, i);
              checkCell(cells[i-1], scanner.getCell());
            }
            checkNoSeekBefore(cells, scanner, 0);
            scanner.close();
          }

          reader.close();
        }
      }
    }
  }

  private void checkSeekBefore(Cell[] cells, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Failed to seek to the key before #" + i + " ("
        + CellUtil.getCellKeyAsString(cells[i]) + ")", true,
        scanner.seekBefore(cells[i]));
  }

  private void checkNoSeekBefore(Cell[] cells, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Incorrectly succeeded in seeking to before first key ("
        + CellUtil.getCellKeyAsString(cells[i]) + ")", false,
        scanner.seekBefore(cells[i]));
  }

  /** Check a key/value pair after it was read by the reader */
  private void checkCell(Cell expected, Cell actual) {
    assertTrue(String.format("Expected key %s, but was %s",
      CellUtil.getCellKeyAsString(expected), CellUtil.getCellKeyAsString(actual)),
      CellUtil.equals(expected, actual));
  }
}

