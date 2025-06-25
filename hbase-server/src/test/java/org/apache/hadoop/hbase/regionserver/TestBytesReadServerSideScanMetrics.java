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

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.NoOpIndexBlockEncoder;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
public class TestBytesReadServerSideScanMetrics {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestBytesReadServerSideScanMetrics.class);

    @Rule
    public TestName name = new TestName();

    private static final Logger LOG =
        LoggerFactory.getLogger(TestBytesReadServerSideScanMetrics.class);

    private HBaseTestingUtil UTIL;

    private static final byte[] CF = Bytes.toBytes("cf");

    private static final byte[] CQ = Bytes.toBytes("cq");

    private static final byte[] VALUE = Bytes.toBytes("value");

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        UTIL = new HBaseTestingUtil();
        conf = UTIL.getConfiguration();
    }

    @Test
    public void testScanMetricsDisabled() throws Exception {
        conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
        UTIL.startMiniCluster();
        try {
            TableName tableName = TableName.valueOf(name.getMethodName());
            createTable(tableName, false, BloomType.NONE);
            writeThenReadDataAndAssertMetrics(tableName, false);
        } finally {
            UTIL.shutdownMiniCluster();
        }
    }

    @Test
    public void testBytesReadFromFsForSerialSeeks() throws Exception {
        conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
        UTIL.startMiniCluster();
        try {
            TableName tableName = TableName.valueOf(name.getMethodName());
            createTable(tableName, false, BloomType.ROW);
            writeThenReadDataAndAssertMetrics(tableName, true);
        } finally {
            UTIL.shutdownMiniCluster();
        }
    }

    @Test
    public void testBytesReadFromFsForParallelSeeks() throws Exception {
        conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
        conf.setBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, true);
        UTIL.startMiniCluster();
        try {
            TableName tableName = TableName.valueOf(name.getMethodName());
            createTable(tableName, false, BloomType.NONE);
            writeThenReadDataAndAssertMetrics(tableName, true);
        } finally {
            UTIL.shutdownMiniCluster();
        }
    }

    @Test
    public void testBytesReadFromBlockCache() throws Exception {
        UTIL.startMiniCluster();
        try {
            TableName tableName = TableName.valueOf(name.getMethodName());
        } finally {
            UTIL.shutdownMiniCluster();
        }
    }

    private void writeThenReadDataAndAssertMetrics(TableName tableName,
        boolean isScanMetricsEnabled) throws Exception {
        Put row2Put = new Put(Bytes.toBytes("row2")).addColumn(CF, CQ, VALUE);
        try (Table table = UTIL.getConnection().getTable(tableName)) {
            // Create a HFile
            table.put(row2Put);
            table.put(new Put(Bytes.toBytes("row4")).addColumn(CF, CQ, VALUE));
            UTIL.flush(tableName);

            // Create a HFile
            table.put(new Put(Bytes.toBytes("row1")).addColumn(CF, CQ, VALUE));
            table.put(new Put(Bytes.toBytes("row5")).addColumn(CF, CQ, VALUE));
            UTIL.flush(tableName);

            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes("row2"), true);
            scan.withStopRow(Bytes.toBytes("row2"), true);
            scan.setScanMetricsEnabled(isScanMetricsEnabled);
            ScanMetrics scanMetrics;
            try (ResultScanner scanner = table.getScanner(scan)) {
                int rowCount = 0;
                StoreFileScanner.instrument();
                for (Result r : scanner) {
                    rowCount++;
                }
                Assert.assertEquals(1, rowCount);
                scanMetrics = scanner.getScanMetrics();
            }
            if (isScanMetricsEnabled) {
                System.out.println("Bytes read from fs: " + scanMetrics.bytesReadFromFs.get());
                System.out.println(
                    "Count of bytes scanned: " + scanMetrics.countOfBlockBytesScanned.get());
                System.out
                    .println("StoreFileScanners seek count: " + StoreFileScanner.getSeekCount());
                
                // Use oldest timestamp to make sure the fake key is not less than the first key in
                // the file containing key: row2
                KeyValue keyValue = new KeyValue(row2Put.getRow(), CF, CQ,
                    PrivateConstants.OLDEST_TIMESTAMP, VALUE);
                assertBytesReadFromFs(tableName, scanMetrics.bytesReadFromFs.get(), keyValue);
                Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
                Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
            }
            else {
                Assert.assertNull(scanMetrics);
            }
        }
    }

    private void createTable(TableName tableName, boolean blockCacheEnabled, BloomType bloomType)
        throws Exception {
        Admin admin = UTIL.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder =
            TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(CF);
        columnFamilyDescriptorBuilder.setBloomFilterType(bloomType);
        columnFamilyDescriptorBuilder.setBlockCacheEnabled(blockCacheEnabled);
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        admin.createTable(tableDescriptorBuilder.build());
        UTIL.waitUntilAllRegionsAssigned(tableName);
    }

    private void assertBytesReadFromFs(TableName tableName, long actualBytesReadFromFs,
        KeyValue keyValue) throws Exception {
        List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
        Assert.assertEquals(1, regions.size());
        int totalExpectedBytesReadFromFs = 0;
        for (HRegion region : regions) {
            Assert.assertNull(region.getBlockCache());
            HStore store = region.getStore(CF);
            Collection<HStoreFile> storeFiles = store.getStorefiles();
            Assert.assertEquals(2, storeFiles.size());
            for (HStoreFile storeFile : storeFiles) {
                StoreFileReader reader = storeFile.getReader();
                HFile.Reader hfileReader = reader.getHFileReader();
                BloomFilter bloomFilter = reader.getGeneralBloomFilter();
                Assert
                    .assertTrue(bloomFilter == null || bloomFilter instanceof CompoundBloomFilter);
                CompoundBloomFilter cbf =
                    bloomFilter == null ? null : (CompoundBloomFilter) bloomFilter;
                int bytesReadFromFs = getBytesReadFromFs(hfileReader, cbf, keyValue);
                totalExpectedBytesReadFromFs += bytesReadFromFs;
            }
        }
        Assert.assertEquals(totalExpectedBytesReadFromFs, actualBytesReadFromFs);
    }

    private int getBytesReadFromFs(HFile.Reader hfileReader, CompoundBloomFilter cbf,
        KeyValue keyValue) throws Exception {
        HFileBlock.FSReader blockReader = hfileReader.getUncachedBlockReader();
        FixedFileTrailer trailer = hfileReader.getTrailer();
        HFileContext meta = hfileReader.getFileContext();
        long fileSize = hfileReader.length();
        int bytesRead = 0;

        // Read the bloom block from FS
        if (cbf != null) {
            // Read a block in load-on-open section to make sure prefetched header is not bloom
            // block's header
            blockReader.readBlockData(trailer.getLoadOnOpenDataOffset(), -1, true, true, true)
                .release();

            HFileBlockIndex.BlockIndexReader index = cbf.getBloomIndex();
            byte[] row = Bytes.toBytes("row2");
            int blockIndex = index.rootBlockContainingKey(row, 0, row.length);
            HFileBlock bloomBlock = cbf.getBloomBlock(blockIndex);
            boolean fileContainsKey = BloomFilterUtil.contains(row, 0, row.length,
                bloomBlock.getBufferReadOnly(), bloomBlock.headerSize(),
                bloomBlock.getUncompressedSizeWithoutHeader(), cbf.getHash(), cbf.getHashCount());
            bytesRead += bloomBlock.getOnDiskSizeWithHeader();
            if (bloomBlock.getNextBlockOnDiskSize() > 0) {
                bytesRead += bloomBlock.headerSize();
            }
            // Asser that the block read is a bloom block
            Assert.assertEquals(bloomBlock.getBlockType(), BlockType.BLOOM_CHUNK);
            bloomBlock.release();
            if (!fileContainsKey) {
                // Key is not in the file, so we don't need to read the data block
                return bytesRead;
            }
        }

        // Indexes use NoOpEncodedSeeker
        MyNoOpEncodedSeeker seeker = new MyNoOpEncodedSeeker();
        HFileBlock.BlockIterator blockIter = blockReader
            .blockRange(trailer.getLoadOnOpenDataOffset(), fileSize - trailer.getTrailerSize());
        HFileBlock block = blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX);

        // Comparator class name is stored in the trailer in version 3.
        CellComparator comparator = trailer.createComparator();
        // Initialize the seeker
        seeker.initRootIndex(block, trailer.getDataIndexCount(), comparator,
            trailer.getNumDataIndexLevels());

        int blockLevelsRead = 1; // Root index is the first level

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
                bytesRead += block.headerSize();
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
        blockIter.freeBlocks();

        Assert.assertEquals(blockLevelsRead, trailer.getNumDataIndexLevels() + 1);

        return bytesRead;
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
