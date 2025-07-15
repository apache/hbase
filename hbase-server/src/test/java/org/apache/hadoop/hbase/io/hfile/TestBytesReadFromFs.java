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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.monitoring.ThreadLocalServerSideScanMetrics;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestBytesReadFromFs {
  private static final int NUM_KEYS = 100000;
  private static final int BLOOM_BLOCK_SIZE = 512;
  private static final int INDEX_CHUNK_SIZE = 512;
  private static final int DATA_BLOCK_SIZE = 4096;
  private static final int ROW_PREFIX_LENGTH_IN_BLOOM_FILTER = 42;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBytesReadFromFs.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestBytesReadFromFs.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Random RNG = new Random(9713312); // Just a fixed seed.

  private Configuration conf;
  private FileSystem fs;
  private List<KeyValue> keyValues = new ArrayList<>();
  private List<byte[]> keyList = new ArrayList<>();
  private Path path;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(BloomFilterUtil.PREFIX_LENGTH_KEY, ROW_PREFIX_LENGTH_IN_BLOOM_FILTER);
    fs = FileSystem.get(conf);
    String hfileName = UUID.randomUUID().toString().replaceAll("-", "");
    path = new Path(TEST_UTIL.getDataTestDir(), hfileName);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, INDEX_CHUNK_SIZE);
  }

  @Test
  public void testBytesReadFromFsWithScanMetricsDisabled() throws IOException {
    ThreadLocalServerSideScanMetrics.setScanMetricsEnabled(false);
    writeData(path);
    KeyValue keyValue = keyValues.get(0);
    readDataAndIndexBlocks(path, keyValue, false);
  }

  @Test
  public void testBytesReadFromFsToReadDataUsingIndexBlocks() throws IOException {
    ThreadLocalServerSideScanMetrics.setScanMetricsEnabled(true);
    writeData(path);
    KeyValue keyValue = keyValues.get(0);
    readDataAndIndexBlocks(path, keyValue, true);
  }

  @Test
  public void testBytesReadFromFsToReadLoadOnOpenDataSection() throws IOException {
    ThreadLocalServerSideScanMetrics.setScanMetricsEnabled(true);
    writeData(path);
    readLoadOnOpenDataSection(path, false);
  }

  @Test
  public void testBytesReadFromFsToReadBloomFilterIndexesAndBloomBlocks() throws IOException {
    ThreadLocalServerSideScanMetrics.setScanMetricsEnabled(true);
    BloomType[] bloomTypes = { BloomType.ROW, BloomType.ROWCOL, BloomType.ROWPREFIX_FIXED_LENGTH };
    for (BloomType bloomType : bloomTypes) {
      LOG.info("Testing bloom type: {}", bloomType);
      ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
      ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();
      keyList.clear();
      keyValues.clear();
      writeBloomFilters(path, bloomType, BLOOM_BLOCK_SIZE);
      if (bloomType == BloomType.ROWCOL) {
        KeyValue keyValue = keyValues.get(0);
        readBloomFilters(path, bloomType, null, keyValue);
      } else {
        Assert.assertEquals(ROW_PREFIX_LENGTH_IN_BLOOM_FILTER, keyList.get(0).length);
        byte[] key = keyList.get(0);
        readBloomFilters(path, bloomType, key, null);
      }
    }
  }

  private void writeData(Path path) throws IOException {
    HFileContext context = new HFileContextBuilder().withBlockSize(DATA_BLOCK_SIZE)
      .withIncludesTags(false).withDataBlockEncoding(DataBlockEncoding.NONE)
      .withCompression(Compression.Algorithm.NONE).build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Writer writer = new HFile.WriterFactory(conf, cacheConfig).withPath(fs, path)
      .withFileContext(context).create();

    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");

    for (int i = 0; i < NUM_KEYS; i++) {
      byte[] keyBytes = RandomKeyValueUtil.randomOrderedFixedLengthKey(RNG, i, 10);
      // A random-length random value.
      byte[] valueBytes = RandomKeyValueUtil.randomFixedLengthValue(RNG, 10);
      KeyValue keyValue =
        new KeyValue(keyBytes, cf, cq, EnvironmentEdgeManager.currentTime(), valueBytes);
      writer.append(keyValue);
      keyValues.add(keyValue);
    }

    writer.close();
  }

  private void readDataAndIndexBlocks(Path path, KeyValue keyValue, boolean isScanMetricsEnabled)
    throws IOException {
    long fileSize = fs.getFileStatus(path).getLen();

    ReaderContext readerContext =
      new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(fs, path))
        .withFilePath(path).withFileSystem(fs).withFileSize(fileSize).build();

    // Read HFile trailer and create HFileContext
    HFileInfo hfile = new HFileInfo(readerContext, conf);
    FixedFileTrailer trailer = hfile.getTrailer();

    // Read HFile info and load-on-open data section (we will read root again explicitly later)
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Reader reader = new HFilePreadReader(readerContext, hfile, cacheConfig, conf);
    hfile.initMetaAndIndex(reader);
    HFileContext meta = hfile.getHFileContext();

    // Get access to the block reader
    HFileBlock.FSReader blockReader = reader.getUncachedBlockReader();

    // Create iterator for reading load-on-open data section
    HFileBlock.BlockIterator blockIter = blockReader.blockRange(trailer.getLoadOnOpenDataOffset(),
      fileSize - trailer.getTrailerSize());

    // Indexes use NoOpEncodedSeeker
    MyNoOpEncodedSeeker seeker = new MyNoOpEncodedSeeker();
    ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
    ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();

    int bytesRead = 0;
    int blockLevelsRead = 0;

    // Read the root index block
    HFileBlock block = blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX);
    bytesRead += block.getOnDiskSizeWithHeader();
    if (block.getNextBlockOnDiskSize() > 0) {
      bytesRead += HFileBlock.headerSize(meta.isUseHBaseChecksum());
    }
    blockLevelsRead++;

    // Comparator class name is stored in the trailer in version 3.
    CellComparator comparator = trailer.createComparator();
    // Initialize the seeker
    seeker.initRootIndex(block, trailer.getDataIndexCount(), comparator,
      trailer.getNumDataIndexLevels());

    int rootLevIndex = seeker.rootBlockContainingKey(keyValue);
    long currentOffset = seeker.getBlockOffset(rootLevIndex);
    int currentDataSize = seeker.getBlockDataSize(rootLevIndex);

    HFileBlock prevBlock = null;
    do {
      prevBlock = block;
      block = blockReader.readBlockData(currentOffset, currentDataSize, true, true, true);
      HFileBlock unpacked = block.unpack(meta, blockReader);
      if (unpacked != block) {
        block.release();
        block = unpacked;
      }
      bytesRead += block.getOnDiskSizeWithHeader();
      if (block.getNextBlockOnDiskSize() > 0) {
        bytesRead += HFileBlock.headerSize(meta.isUseHBaseChecksum());
      }
      if (!block.getBlockType().isData()) {
        ByteBuff buffer = block.getBufferWithoutHeader();
        // Place the buffer at the correct position
        HFileBlockIndex.BlockIndexReader.locateNonRootIndexEntry(buffer, keyValue, comparator);
        currentOffset = buffer.getLong();
        currentDataSize = buffer.getInt();
      }
      prevBlock.release();
      blockLevelsRead++;
    } while (!block.getBlockType().isData());
    block.release();

    reader.close();

    Assert.assertEquals(isScanMetricsEnabled,
      ThreadLocalServerSideScanMetrics.isScanMetricsEnabled());
    bytesRead = isScanMetricsEnabled ? bytesRead : 0;
    Assert.assertEquals(bytesRead, ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset());
    Assert.assertEquals(blockLevelsRead, trailer.getNumDataIndexLevels() + 1);
    Assert.assertEquals(0, ThreadLocalServerSideScanMetrics.getBytesReadFromBlockCacheAndReset());
    // At every index level we read one index block and finally read data block
    long blockReadOpsCount = isScanMetricsEnabled ? blockLevelsRead : 0;
    Assert.assertEquals(blockReadOpsCount,
      ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset());
  }

  private void readLoadOnOpenDataSection(Path path, boolean hasBloomFilters) throws IOException {
    long fileSize = fs.getFileStatus(path).getLen();

    ReaderContext readerContext =
      new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(fs, path))
        .withFilePath(path).withFileSystem(fs).withFileSize(fileSize).build();

    ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
    ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();
    // Read HFile trailer
    HFileInfo hfile = new HFileInfo(readerContext, conf);
    FixedFileTrailer trailer = hfile.getTrailer();
    Assert.assertEquals(trailer.getTrailerSize(),
      ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset());
    Assert.assertEquals(1, ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset());

    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Reader reader = new HFilePreadReader(readerContext, hfile, cacheConfig, conf);
    // Since HBASE-28466, we call fileInfo.initMetaAndIndex inside HFilePreadReader,
    // which reads some blocks and increment the counters, so we need to reset it here.
    ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
    ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();
    HFileBlock.FSReader blockReader = reader.getUncachedBlockReader();

    // Create iterator for reading root index block
    HFileBlock.BlockIterator blockIter = blockReader.blockRange(trailer.getLoadOnOpenDataOffset(),
      fileSize - trailer.getTrailerSize());
    boolean readNextHeader = false;

    // Read the root index block
    readNextHeader = readEachBlockInLoadOnOpenDataSection(
      blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX), readNextHeader);

    // Read meta index block
    readNextHeader = readEachBlockInLoadOnOpenDataSection(
      blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX), readNextHeader);

    // Read File info block
    readNextHeader = readEachBlockInLoadOnOpenDataSection(
      blockIter.nextBlockWithBlockType(BlockType.FILE_INFO), readNextHeader);

    // Read bloom filter indexes
    boolean bloomFilterIndexesRead = false;
    HFileBlock block;
    while ((block = blockIter.nextBlock()) != null) {
      bloomFilterIndexesRead = true;
      readNextHeader = readEachBlockInLoadOnOpenDataSection(block, readNextHeader);
    }

    reader.close();

    Assert.assertEquals(hasBloomFilters, bloomFilterIndexesRead);
    Assert.assertEquals(0, ThreadLocalServerSideScanMetrics.getBytesReadFromBlockCacheAndReset());
  }

  private boolean readEachBlockInLoadOnOpenDataSection(HFileBlock block, boolean readNextHeader)
    throws IOException {
    long bytesRead = block.getOnDiskSizeWithHeader();
    if (readNextHeader) {
      bytesRead -= HFileBlock.headerSize(true);
      readNextHeader = false;
    }
    if (block.getNextBlockOnDiskSize() > 0) {
      bytesRead += HFileBlock.headerSize(true);
      readNextHeader = true;
    }
    block.release();
    Assert.assertEquals(bytesRead, ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset());
    Assert.assertEquals(1, ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset());
    return readNextHeader;
  }

  private void readBloomFilters(Path path, BloomType bt, byte[] key, KeyValue keyValue)
    throws IOException {
    Assert.assertTrue(keyValue == null || key == null);

    // Assert that the bloom filter index was read and it's size is accounted in bytes read from
    // fs
    readLoadOnOpenDataSection(path, true);

    CacheConfig cacheConf = new CacheConfig(conf);
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, path, true);
    HStoreFile sf = new HStoreFile(storeFileInfo, bt, cacheConf);

    // Read HFile trailer and load-on-open data section
    sf.initReader();

    // Reset bytes read from fs to 0
    ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
    // Reset read ops count to 0
    ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();

    StoreFileReader reader = sf.getReader();
    BloomFilter bloomFilter = reader.getGeneralBloomFilter();
    Assert.assertTrue(bloomFilter instanceof CompoundBloomFilter);
    CompoundBloomFilter cbf = (CompoundBloomFilter) bloomFilter;

    // Get the bloom filter index reader
    HFileBlockIndex.BlockIndexReader index = cbf.getBloomIndex();
    int block;

    // Search for the key in the bloom filter index
    if (keyValue != null) {
      block = index.rootBlockContainingKey(keyValue);
    } else {
      byte[] row = key;
      block = index.rootBlockContainingKey(row, 0, row.length);
    }

    // Read the bloom block from FS
    HFileBlock bloomBlock = cbf.getBloomBlock(block);
    long bytesRead = bloomBlock.getOnDiskSizeWithHeader();
    if (bloomBlock.getNextBlockOnDiskSize() > 0) {
      bytesRead += HFileBlock.headerSize(true);
    }
    // Asser that the block read is a bloom block
    Assert.assertEquals(bloomBlock.getBlockType(), BlockType.BLOOM_CHUNK);
    bloomBlock.release();

    // Close the reader
    reader.close(true);

    Assert.assertEquals(bytesRead, ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset());
    Assert.assertEquals(1, ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset());
  }

  private void writeBloomFilters(Path path, BloomType bt, int bloomBlockByteSize)
    throws IOException {
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, bloomBlockByteSize);
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(DATA_BLOCK_SIZE)
      .withIncludesTags(false).withDataBlockEncoding(DataBlockEncoding.NONE)
      .withCompression(Compression.Algorithm.NONE).build();
    StoreFileWriter w = new StoreFileWriter.Builder(conf, cacheConf, fs).withFileContext(meta)
      .withBloomType(bt).withFilePath(path).build();
    Assert.assertTrue(w.hasGeneralBloom());
    Assert.assertTrue(w.getGeneralBloomWriter() instanceof CompoundBloomFilterWriter);
    CompoundBloomFilterWriter cbbf = (CompoundBloomFilterWriter) w.getGeneralBloomWriter();
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");
    for (int i = 0; i < NUM_KEYS; i++) {
      byte[] keyBytes = RandomKeyValueUtil.randomOrderedFixedLengthKey(RNG, i, 10);
      // A random-length random value.
      byte[] valueBytes = RandomKeyValueUtil.randomFixedLengthValue(RNG, 10);
      KeyValue keyValue =
        new KeyValue(keyBytes, cf, cq, EnvironmentEdgeManager.currentTime(), valueBytes);
      w.append(keyValue);
      keyList.add(keyBytes);
      keyValues.add(keyValue);
    }
    Assert.assertEquals(keyList.size(), cbbf.getKeyCount());
    w.close();
  }

  private static class MyNoOpEncodedSeeker extends NoOpIndexBlockEncoder.NoOpEncodedSeeker {
    public long getBlockOffset(int i) {
      return blockOffsets[i];
    }

    public int getBlockDataSize(int i) {
      return blockDataSizes[i];
    }
  }
}
