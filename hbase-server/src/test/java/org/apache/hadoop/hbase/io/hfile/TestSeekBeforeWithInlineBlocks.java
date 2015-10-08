/*
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

package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSeekBeforeWithInlineBlocks {

  private static final Log LOG = LogFactory.getLog(TestSeekBeforeWithInlineBlocks.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final int NUM_KV = 10000;

  private static final int DATA_BLOCK_SIZE = 4096;
  private static final int BLOOM_BLOCK_SIZE = 1024;
  private static final int[] INDEX_CHUNK_SIZES = { HFileBlockIndex.DEFAULT_MAX_CHUNK_SIZE, 65536, 4096, 1024 };
  private static final int[] EXPECTED_NUM_LEVELS = { 1, 1, 2, 3 };

  private static final Random RAND = new Random(192537);
  private static final byte[] FAM = Bytes.toBytes("family");

  private FileSystem fs;
  private Configuration conf;

  /**
   * Scanner.seekBefore() can fail because when seeking to a previous HFile data block, it needs to
   * know the size of that data block, which it calculates using current data block offset and the
   * previous data block offset.  This fails to work when there are leaf-level index blocks in the
   * scannable section of the HFile, i.e. starting in HFileV2.  This test will try seekBefore() on
   * a flat (single-level) and multi-level (2,3) HFile and confirm this bug is now fixed.  This
   * bug also happens for inline Bloom blocks.
   */
  @Test
  public void testMultiIndexLevelRandomHFileWithBlooms() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    
    // Try out different HFile versions to ensure reverse scan works on each version
    for (int hfileVersion = HFile.MIN_FORMAT_VERSION; 
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
          
          byte[][] keys = new byte[NUM_KV][];
          byte[][] values = new byte[NUM_KV][];
  
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
            
            StoreFile.Writer storeFileWriter = new StoreFile.WriterBuilder(conf, cacheConf, fs)
              .withFilePath(hfilePath)
              .withFileContext(meta)
              .withBloomType(bloomType)
              .build();
            
            for (int i = 0; i < NUM_KV; i++) {
              byte[] row = TestHFileWriterV2.randomOrderedKey(RAND, i);
              byte[] qual = TestHFileWriterV2.randomRowOrQualifier(RAND);
              byte[] value = TestHFileWriterV2.randomValue(RAND);
              KeyValue kv = new KeyValue(row, FAM, qual, value);
  
              storeFileWriter.append(kv);
              keys[i] = kv.getKey();
              values[i] = value;
            }
  
            storeFileWriter.close();
          }
  
          // Read the HFile
          HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, conf);
          
          // Sanity check the HFile index level
          assertEquals(expectedNumLevels, reader.getTrailer().getNumDataIndexLevels());
          
          // Check that we can seekBefore in either direction and with both pread
          // enabled and disabled
          for (boolean pread : new boolean[] { false, true }) {
            HFileScanner scanner = reader.getScanner(true, pread);
            checkNoSeekBefore(keys, scanner, 0);
            for (int i = 1; i < NUM_KV; i++) {
              checkSeekBefore(keys, scanner, i);
              checkKeyValue("i=" + i, keys[i-1], values[i-1], 
                scanner.getKey(),
                scanner.getValue());
            }
            assertTrue(scanner.seekTo());
            for (int i = NUM_KV - 1; i >= 1; i--) {
              checkSeekBefore(keys, scanner, i);
              checkKeyValue("i=" + i, keys[i-1], values[i-1], 
                scanner.getKey(),
                scanner.getValue());
            }
            checkNoSeekBefore(keys, scanner, 0);
          }
  
          reader.close();
        }    
      }
    }
  }
  
  private void checkSeekBefore(byte[][] keys, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Failed to seek to the key before #" + i + " ("
        + Bytes.toStringBinary(keys[i]) + ")", true, 
        scanner.seekBefore(keys[i]));
  }

  private void checkNoSeekBefore(byte[][] keys, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Incorrectly succeeded in seeking to before first key ("
        + Bytes.toStringBinary(keys[i]) + ")", false, 
        scanner.seekBefore(keys[i]));
  }

  private void assertArrayEqualsBuffer(String msgPrefix, byte[] arr,
      ByteBuffer buf) {
    assertEquals(msgPrefix + ": expected " + Bytes.toStringBinary(arr)
        + ", actual " + Bytes.toStringBinary(buf), 0, Bytes.compareTo(arr, 0,
        arr.length, buf.array(), buf.arrayOffset(), buf.limit()));
  }

  /** Check a key/value pair after it was read by the reader */
  private void checkKeyValue(String msgPrefix, byte[] expectedKey,
      byte[] expectedValue, ByteBuffer keyRead, ByteBuffer valueRead) {
    if (!msgPrefix.isEmpty())
      msgPrefix += ". ";

    assertArrayEqualsBuffer(msgPrefix + "Invalid key", expectedKey, keyRead);
    assertArrayEqualsBuffer(msgPrefix + "Invalid value", expectedValue,
        valueRead);
  }
}
