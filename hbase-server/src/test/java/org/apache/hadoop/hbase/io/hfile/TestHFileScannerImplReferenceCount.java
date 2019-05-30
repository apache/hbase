/**
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

import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MAX_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MIN_INDEX_NUM_ENTRIES_KEY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileReaderImpl.HFileScannerImpl;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileScannerImplReferenceCount {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileScannerImplReferenceCount.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHFileScannerImplReferenceCount.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] SUFFIX = randLongBytes();

  private static byte[] randLongBytes() {
    Random rand = new Random();
    byte[] keys = new byte[300];
    rand.nextBytes(keys);
    return keys;
  }

  private Cell firstCell = null;
  private Cell secondCell = null;

  @BeforeClass
  public static void setUp() {
    Configuration conf = UTIL.getConfiguration();
    // Set the max chunk size and min entries key to be very small for index block, so that we can
    // create an index block tree with level >= 2.
    conf.setInt(MAX_CHUNK_SIZE_KEY, 10);
    conf.setInt(MIN_INDEX_NUM_ENTRIES_KEY, 2);
  }

  private void writeHFile(Configuration conf, FileSystem fs, Path hfilePath, Algorithm compression,
      DataBlockEncoding encoding, int cellCount) throws IOException {
    HFileContext context =
        new HFileContextBuilder().withBlockSize(1).withDataBlockEncoding(DataBlockEncoding.NONE)
            .withCompression(compression).withDataBlockEncoding(encoding).build();
    try (HFile.Writer writer =
        new HFile.WriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfilePath)
            .withFileContext(context).withComparator(CellComparatorImpl.COMPARATOR).create()) {
      Random rand = new Random(9713312); // Just a fixed seed.
      for (int i = 0; i < cellCount; ++i) {
        byte[] keyBytes = Bytes.add(Bytes.toBytes(i), SUFFIX);

        // A random-length random value.
        byte[] valueBytes = RandomKeyValueUtil.randomValue(rand);
        KeyValue keyValue =
            new KeyValue(keyBytes, FAMILY, QUALIFIER, HConstants.LATEST_TIMESTAMP, valueBytes);
        if (firstCell == null) {
          firstCell = keyValue;
        } else if (secondCell == null) {
          secondCell = keyValue;
        }
        writer.append(keyValue);
      }
    }
  }

  private void testReleaseBlock(Algorithm compression, DataBlockEncoding encoding)
      throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    Path dir = UTIL.getDataTestDir("testReleasingBlock");
    FileSystem fs = dir.getFileSystem(conf);
    try {
      String hfileName = "testReleaseBlock_hfile_0_" + System.currentTimeMillis();
      Path hfilePath = new Path(dir, hfileName);
      int cellCount = 1000;
      LOG.info("Start to write {} cells into hfile: {}", cellCount, hfilePath);
      writeHFile(conf, fs, hfilePath, compression, encoding, cellCount);

      BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
      Assert.assertNotNull(defaultBC);
      HFile.Reader reader =
          HFile.createReader(fs, hfilePath, new CacheConfig(conf, defaultBC), true, conf);
      Assert.assertTrue(reader instanceof HFileReaderImpl);
      // We've build a HFile tree with index = 16.
      Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

      HFileScanner scanner = reader.getScanner(true, true, false);
      BlockWithScanInfo scanInfo = reader.getDataBlockIndexReader()
          .loadDataBlockWithScanInfo(firstCell, null, true, true, false, DataBlockEncoding.NONE);
      BlockWithScanInfo scanInfo2 = reader.getDataBlockIndexReader()
          .loadDataBlockWithScanInfo(secondCell, null, true, true, false, DataBlockEncoding.NONE);
      HFileBlock block = scanInfo.getHFileBlock();
      HFileBlock block2 = scanInfo2.getHFileBlock();
      // One refCnt for blockCache and the other refCnt for RPC path.
      Assert.assertEquals(block.refCnt(), 2);
      Assert.assertEquals(block2.refCnt(), 2);
      Assert.assertFalse(block == block2);

      scanner.seekTo(firstCell);
      Assert.assertEquals(block.refCnt(), 3);

      // Seek to the block again, the curBlock won't change and won't read from BlockCache. so
      // refCnt should be unchanged.
      scanner.seekTo(firstCell);
      Assert.assertEquals(block.refCnt(), 3);

      scanner.seekTo(secondCell);
      Assert.assertEquals(block.refCnt(), 3);
      Assert.assertEquals(block2.refCnt(), 3);

      // After shipped, the block will be release, but block2 is still referenced by the curBlock.
      scanner.shipped();
      Assert.assertEquals(block.refCnt(), 2);
      Assert.assertEquals(block2.refCnt(), 3);

      // Try to ship again, though with nothing to client.
      scanner.shipped();
      Assert.assertEquals(block.refCnt(), 2);
      Assert.assertEquals(block2.refCnt(), 3);

      // The curBlock(block2) will also be released.
      scanner.close();
      Assert.assertEquals(block2.refCnt(), 2);

      // Finish the block & block2 RPC path
      block.release();
      block2.release();
      Assert.assertEquals(block.refCnt(), 1);
      Assert.assertEquals(block2.refCnt(), 1);

      // Evict the LRUBlockCache
      Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfileName) >= 2);
      Assert.assertEquals(block.refCnt(), 0);
      Assert.assertEquals(block2.refCnt(), 0);

      int count = 0;
      Assert.assertTrue(scanner.seekTo());
      ++count;
      while (scanner.next()) {
        count++;
      }
      assertEquals(cellCount, count);
    } finally {
      fs.delete(dir, true);
    }
  }

  /**
   * See HBASE-22480
   */
  @Test
  public void testSeekBefore() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    Path dir = UTIL.getDataTestDir("testSeekBefore");
    FileSystem fs = dir.getFileSystem(conf);
    try {
      String hfileName = "testSeekBefore_hfile_0_" + System.currentTimeMillis();
      Path hfilePath = new Path(dir, hfileName);
      int cellCount = 1000;
      LOG.info("Start to write {} cells into hfile: {}", cellCount, hfilePath);
      writeHFile(conf, fs, hfilePath, Algorithm.NONE, DataBlockEncoding.NONE, cellCount);

      BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
      Assert.assertNotNull(defaultBC);
      HFile.Reader reader =
          HFile.createReader(fs, hfilePath, new CacheConfig(conf, defaultBC), true, conf);
      Assert.assertTrue(reader instanceof HFileReaderImpl);
      // We've build a HFile tree with index = 16.
      Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

      HFileScanner scanner = reader.getScanner(true, true, false);
      HFileBlock block1 = reader.getDataBlockIndexReader()
          .loadDataBlockWithScanInfo(firstCell, null, true, true, false, DataBlockEncoding.NONE)
          .getHFileBlock();
      HFileBlock block2 = reader.getDataBlockIndexReader()
          .loadDataBlockWithScanInfo(secondCell, null, true, true, false, DataBlockEncoding.NONE)
          .getHFileBlock();
      Assert.assertEquals(block1.refCnt(), 2);
      Assert.assertEquals(block2.refCnt(), 2);

      // Let the curBlock refer to block2.
      scanner.seekTo(secondCell);
      Assert.assertTrue(((HFileScannerImpl) scanner).curBlock == block2);
      Assert.assertEquals(3, block2.refCnt());

      // Release the block1, only one reference: blockCache.
      Assert.assertFalse(block1.release());
      Assert.assertEquals(1, block1.refCnt());
      // Release the block2, so the remain references are: 1. scanner; 2. blockCache.
      Assert.assertFalse(block2.release());
      Assert.assertEquals(2, block2.refCnt());

      // Do the seekBefore: the newBlock will be the previous block of curBlock.
      Assert.assertTrue(scanner.seekBefore(secondCell));
      Assert.assertTrue(((HFileScannerImpl) scanner).curBlock == block1);
      // Two reference for block1: 1. scanner; 2. blockCache.
      Assert.assertEquals(2, block1.refCnt());
      // Reference count of block2 must be unchanged because we haven't shipped.
      Assert.assertEquals(2, block2.refCnt());

      // Do the shipped
      scanner.shipped();
      Assert.assertEquals(2, block1.refCnt());
      Assert.assertEquals(1, block2.refCnt());

      // Do the close
      scanner.close();
      Assert.assertEquals(1, block1.refCnt());
      Assert.assertEquals(1, block2.refCnt());

      Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfileName) >= 2);
      Assert.assertEquals(0, block1.refCnt());
      Assert.assertEquals(0, block2.refCnt());

      // Reload the block1 again.
      block1 = reader.getDataBlockIndexReader()
          .loadDataBlockWithScanInfo(firstCell, null, true, true, false, DataBlockEncoding.NONE)
          .getHFileBlock();
      Assert.assertFalse(block1.release());
      Assert.assertEquals(1, block1.refCnt());
      // Re-seek to the begin.
      Assert.assertTrue(scanner.seekTo());
      Assert.assertTrue(((HFileScannerImpl) scanner).curBlock == block1);
      Assert.assertEquals(2, block1.refCnt());
      // Return false because firstCell <= c[0]
      Assert.assertFalse(scanner.seekBefore(firstCell));
      // The block1 shouldn't be released because we still don't do the shipped or close.
      Assert.assertEquals(2, block1.refCnt());

      scanner.close();
      Assert.assertEquals(1, block1.refCnt());
      Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfileName) >= 1);
      Assert.assertEquals(0, block1.refCnt());
    } finally {
      fs.delete(dir, true);
    }
  }

  @Test
  public void testDefault() throws Exception {
    testReleaseBlock(Algorithm.NONE, DataBlockEncoding.NONE);
  }

  @Test
  public void testCompression() throws Exception {
    testReleaseBlock(Algorithm.GZ, DataBlockEncoding.NONE);
  }

  @Test
  public void testDataBlockEncoding() throws Exception {
    testReleaseBlock(Algorithm.NONE, DataBlockEncoding.ROW_INDEX_V1);
  }

  @Test
  public void testDataBlockEncodingAndCompression() throws Exception {
    testReleaseBlock(Algorithm.GZ, DataBlockEncoding.ROW_INDEX_V1);
  }
}
