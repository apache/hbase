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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.metrics.ThreadLocalScanMetrics;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileIndexBlockEncoder.EncodedSeeker;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.t;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ IOTests.class, MediumTests.class })
public class TestBytesReadFromFs {
    private static final int NUM_KEYS = 100000;

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestBytesReadFromFs.class);

    @Rule
    public TestName name = new TestName();
    

    private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
    private static final Random RNG = new Random(9713312); // Just a fixed seed.

    private Configuration conf;
    private FileSystem fs;
    private List<KeyValue> keyValues = new ArrayList<>();
    private Path path;

    @Before
    public void setUp() throws IOException {
        conf = TEST_UTIL.getConfiguration();
        fs = FileSystem.get(conf);
        path = new Path(TEST_UTIL.getDataTestDir(), name.getMethodName());
        conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, 512);
    }

    @Test
    public void testBytesReadFromFsToReadDataUsingIndexBlocks() throws IOException {
        writeData(path);
        KeyValue keyValue = keyValues.get(0);
        readDataAndIndexBlocks(path, keyValue);
    }

    @Test
    public void testBytesReadFromFsToReadLoadOnOpenDataSection() throws IOException {
        writeData(path);
        readLoadOnOpenDataSection(path);
    }

    @Test
    public void testBytesReadFromFsToReadBloomFilterIndexesAndBloomBlocks() throws IOException {
        writeData(path);
    }

    private void writeData(Path path) throws IOException {
        HFileContext context = new HFileContextBuilder().withBlockSize(4096).withIncludesTags(false)
            .withDataBlockEncoding(DataBlockEncoding.NONE)
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

    private void readDataAndIndexBlocks(Path path, KeyValue keyValue) throws IOException {
        ThreadLocalScanMetrics.setScanMetricsEnabled(true);
        long fileSize = fs.getFileStatus(path).getLen();
        
        ReaderContext readerContext = new ReaderContextBuilder()
            .withInputStreamWrapper(new FSDataInputStreamWrapper(fs, path)).withFilePath(path)
            .withFileSystem(fs).withFileSize(fileSize).build();
        
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
        HFileBlock.BlockIterator blockIter = blockReader
            .blockRange(trailer.getLoadOnOpenDataOffset(), fileSize - trailer.getTrailerSize());
        
        // Indexes use NoOpEncodedSeeker
        MyNoOpEncodedSeeker seeker = new MyNoOpEncodedSeeker();
        ThreadLocalScanMetrics.getBytesReadFromFsAndReset();

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
                HFileBlockIndex.BlockIndexReader.locateNonRootIndexEntry(buffer, keyValue,
                    comparator);
                currentOffset = buffer.getLong();
                currentDataSize = buffer.getInt();
            }
            prevBlock.release();
            blockLevelsRead++;
        } while (!block.getBlockType().isData());
        block.release();

        reader.close();

        Assert.assertEquals(bytesRead, ThreadLocalScanMetrics.getBytesReadFromFsAndReset());
        Assert.assertEquals(blockLevelsRead, trailer.getNumDataIndexLevels() + 1);
        Assert.assertEquals(0, ThreadLocalScanMetrics.getBytesReadFromBlockCacheAndReset());
    }

    private void readLoadOnOpenDataSection(Path path) throws IOException {
        ThreadLocalScanMetrics.setScanMetricsEnabled(true);
        long fileSize = fs.getFileStatus(path).getLen();
        
        ReaderContext readerContext = new ReaderContextBuilder()
            .withInputStreamWrapper(new FSDataInputStreamWrapper(fs, path)).withFilePath(path)
            .withFileSystem(fs).withFileSize(fileSize).build();
        
        // Read HFile trailer
        HFileInfo hfile = new HFileInfo(readerContext, conf);
        FixedFileTrailer trailer = hfile.getTrailer();
        Assert.assertEquals(trailer.getTrailerSize(),
            ThreadLocalScanMetrics.getBytesReadFromFsAndReset());

        CacheConfig cacheConfig = new CacheConfig(conf);
        HFile.Reader reader = new HFilePreadReader(readerContext, hfile, cacheConfig, conf);
        HFileBlock.FSReader blockReader = reader.getUncachedBlockReader();

        // Create iterator for reading root index block
        HFileBlock.BlockIterator blockIter = blockReader
            .blockRange(trailer.getLoadOnOpenDataOffset(), fileSize - trailer.getTrailerSize());
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
        while (blockIter.nextBlock() != null) {
            bloomFilterIndexesRead = true;
            readNextHeader =
                readEachBlockInLoadOnOpenDataSection(blockIter.nextBlock(), readNextHeader);
        }
        
        reader.close();

        Assert.assertFalse(bloomFilterIndexesRead);
        Assert.assertEquals(0, ThreadLocalScanMetrics.getBytesReadFromBlockCacheAndReset());
    }

    private boolean readEachBlockInLoadOnOpenDataSection(HFileBlock block, boolean readNextHeader)
        throws IOException {
        int bytesRead = block.getOnDiskSizeWithHeader();
        if (readNextHeader) {
            bytesRead -= HFileBlock.headerSize(true);
            readNextHeader = false;
        }
        if (block.getNextBlockOnDiskSize() > 0) {
            bytesRead += HFileBlock.headerSize(true);
            readNextHeader = true;
        }
        block.release();
        Assert.assertEquals(bytesRead, ThreadLocalScanMetrics.getBytesReadFromFsAndReset());
        return readNextHeader;
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
