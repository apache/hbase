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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexChunk;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({IOTests.class, MediumTests.class})
public class TestHFileBlockIndex {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileBlockIndex.class);

  @Parameters
  public static Collection<Object[]> compressionAlgorithms() {
    return HBaseCommonTestingUtil.COMPRESSION_ALGORITHMS_PARAMETERIZED;
  }

  public TestHFileBlockIndex(Compression.Algorithm compr) {
    this.compr = compr;
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileBlockIndex.class);
  private static final Random RNG = new Random(); // This test depends on Random#setSeed
  private static final int NUM_DATA_BLOCKS = 1000;
  private static final HBaseTestingUtil TEST_UTIL =
      new HBaseTestingUtil();

  private static final int SMALL_BLOCK_SIZE = 4096;
  private static final int NUM_KV = 10000;

  private static FileSystem fs;
  private Path path;
  private long rootIndexOffset;
  private int numRootEntries;
  private int numLevels;
  private static final List<byte[]> keys = new ArrayList<>();
  private final Compression.Algorithm compr;
  private byte[] firstKeyInFile;
  private Configuration conf;

  private static final int[] INDEX_CHUNK_SIZES = { 4096, 512, 384 };
  private static final int[] EXPECTED_NUM_LEVELS = { 2, 3, 4 };
  private static final int[] UNCOMPRESSED_INDEX_SIZES =
      { 19187, 21813, 23086 };

  private static final boolean includesMemstoreTS = true;

  static {
    assert INDEX_CHUNK_SIZES.length == EXPECTED_NUM_LEVELS.length;
    assert INDEX_CHUNK_SIZES.length == UNCOMPRESSED_INDEX_SIZES.length;
  }

  @Before
  public void setUp() throws IOException {
    keys.clear();
    firstKeyInFile = null;
    conf = TEST_UTIL.getConfiguration();
    RNG.setSeed(2389757);

    // This test requires at least HFile format version 2.
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);

    fs = HFileSystem.get(conf);
  }

  @Test
  public void testBlockIndex() throws IOException {
    testBlockIndexInternals(false);
    clear();
    testBlockIndexInternals(true);
  }

  private void clear() throws IOException {
    keys.clear();
    firstKeyInFile = null;
    conf = TEST_UTIL.getConfiguration();
    RNG.setSeed(2389757);

    // This test requires at least HFile format version 2.
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);

    fs = HFileSystem.get(conf);
  }

  private void testBlockIndexInternals(boolean useTags) throws IOException {
    path = new Path(TEST_UTIL.getDataTestDir(), "block_index_" + compr + useTags);
    writeWholeIndex(useTags);
    readIndex(useTags);
  }

  /**
   * A wrapper around a block reader which only caches the results of the last
   * operation. Not thread-safe.
   */
  private static class BlockReaderWrapper implements HFile.CachingBlockReader {

    private HFileBlock.FSReader realReader;
    private long prevOffset;
    private long prevOnDiskSize;
    private boolean prevPread;
    private HFileBlock prevBlock;

    public int hitCount = 0;
    public int missCount = 0;

    public BlockReaderWrapper(HFileBlock.FSReader realReader) {
      this.realReader = realReader;
    }

    @Override
    public HFileBlock readBlock(long offset, long onDiskSize,
        boolean cacheBlock, boolean pread, boolean isCompaction,
        boolean updateCacheMetrics, BlockType expectedBlockType,
        DataBlockEncoding expectedDataBlockEncoding)
        throws IOException {
      if (offset == prevOffset && onDiskSize == prevOnDiskSize &&
          pread == prevPread) {
        hitCount += 1;
        return prevBlock;
      }

      missCount += 1;
      prevBlock = realReader.readBlockData(offset, onDiskSize, pread, false, true);
      prevOffset = offset;
      prevOnDiskSize = onDiskSize;
      prevPread = pread;

      return prevBlock;
    }
  }

  private void readIndex(boolean useTags) throws IOException {
    long fileSize = fs.getFileStatus(path).getLen();
    LOG.info("Size of {}: {} compression={}", path, fileSize, compr.toString());

    FSDataInputStream istream = fs.open(path);
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(true)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(useTags)
                        .withCompression(compr)
                        .build();
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, path).build();
    HFileBlock.FSReader blockReader = new HFileBlock.FSReaderImpl(context, meta,
        ByteBuffAllocator.HEAP, conf);

    BlockReaderWrapper brw = new BlockReaderWrapper(blockReader);
    HFileBlockIndex.BlockIndexReader indexReader =
        new HFileBlockIndex.CellBasedKeyBlockIndexReader(
            CellComparatorImpl.COMPARATOR, numLevels);

    indexReader.readRootIndex(blockReader.blockRange(rootIndexOffset,
        fileSize).nextBlockWithBlockType(BlockType.ROOT_INDEX), numRootEntries);

    long prevOffset = -1;
    int i = 0;
    int expectedHitCount = 0;
    int expectedMissCount = 0;
    LOG.info("Total number of keys: " + keys.size());
    for (byte[] key : keys) {
      assertTrue(key != null);
      assertTrue(indexReader != null);
      KeyValue.KeyOnlyKeyValue keyOnlyKey = new KeyValue.KeyOnlyKeyValue(key, 0, key.length);
      HFileBlock b =
          indexReader.seekToDataBlock(keyOnlyKey, null, true,
            true, false, null, brw);
      if (PrivateCellUtil.compare(CellComparatorImpl.COMPARATOR, keyOnlyKey, firstKeyInFile, 0,
        firstKeyInFile.length) < 0) {
        assertTrue(b == null);
        ++i;
        continue;
      }

      String keyStr = "key #" + i + ", " + Bytes.toStringBinary(key);

      assertTrue("seekToDataBlock failed for " + keyStr, b != null);

      if (prevOffset == b.getOffset()) {
        assertEquals(++expectedHitCount, brw.hitCount);
      } else {
        LOG.info("First key in a new block: " + keyStr + ", block offset: "
            + b.getOffset() + ")");
        assertTrue(b.getOffset() > prevOffset);
        assertEquals(++expectedMissCount, brw.missCount);
        prevOffset = b.getOffset();
      }
      ++i;
    }

    istream.close();
  }

  private void writeWholeIndex(boolean useTags) throws IOException {
    assertEquals(0, keys.size());
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(true)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(useTags)
                        .withCompression(compr)
                        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                        .build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(TEST_UTIL.getConfiguration(), null,
        meta);
    FSDataOutputStream outputStream = fs.create(path);
    HFileBlockIndex.BlockIndexWriter biw =
        new HFileBlockIndex.BlockIndexWriter(hbw, null, null);
    for (int i = 0; i < NUM_DATA_BLOCKS; ++i) {
      hbw.startWriting(BlockType.DATA).write(Bytes.toBytes(String.valueOf(RNG.nextInt(1000))));
      long blockOffset = outputStream.getPos();
      hbw.writeHeaderAndData(outputStream);

      byte[] firstKey = null;
      byte[] family = Bytes.toBytes("f");
      byte[] qualifier = Bytes.toBytes("q");
      for (int j = 0; j < 16; ++j) {
        byte[] k =
            new KeyValue(RandomKeyValueUtil.randomOrderedKey(RNG, i * 16 + j), family, qualifier,
                EnvironmentEdgeManager.currentTime(), KeyValue.Type.Put).getKey();
        keys.add(k);
        if (j == 8) {
          firstKey = k;
        }
      }
      assertTrue(firstKey != null);
      if (firstKeyInFile == null) {
        firstKeyInFile = firstKey;
      }
      biw.addEntry(firstKey, blockOffset, hbw.getOnDiskSizeWithHeader());

      writeInlineBlocks(hbw, outputStream, biw, false);
    }
    writeInlineBlocks(hbw, outputStream, biw, true);
    rootIndexOffset = biw.writeIndexBlocks(outputStream);
    outputStream.close();

    numLevels = biw.getNumLevels();
    numRootEntries = biw.getNumRootEntries();

    LOG.info("Index written: numLevels=" + numLevels + ", numRootEntries=" +
        numRootEntries + ", rootIndexOffset=" + rootIndexOffset);
  }

  private void writeInlineBlocks(HFileBlock.Writer hbw,
      FSDataOutputStream outputStream, HFileBlockIndex.BlockIndexWriter biw,
      boolean isClosing) throws IOException {
    while (biw.shouldWriteBlock(isClosing)) {
      long offset = outputStream.getPos();
      biw.writeInlineBlock(hbw.startWriting(biw.getInlineBlockType()));
      hbw.writeHeaderAndData(outputStream);
      biw.blockWritten(offset, hbw.getOnDiskSizeWithHeader(),
          hbw.getUncompressedSizeWithoutHeader());
      LOG.info("Wrote an inline index block at " + offset + ", size " +
          hbw.getOnDiskSizeWithHeader());
    }
  }

  private static final long getDummyFileOffset(int i) {
    return i * 185 + 379;
  }

  private static final int getDummyOnDiskSize(int i) {
    return i * i * 37 + i * 19 + 13;
  }

  @Test
  public void testSecondaryIndexBinarySearch() throws IOException {
    int numTotalKeys = 99;
    assertTrue(numTotalKeys % 2 == 1); // Ensure no one made this even.

    // We only add odd-index keys into the array that we will binary-search.
    int numSearchedKeys = (numTotalKeys - 1) / 2;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    dos.writeInt(numSearchedKeys);
    int curAllEntriesSize = 0;
    int numEntriesAdded = 0;

    // Only odd-index elements of this array are used to keep the secondary
    // index entries of the corresponding keys.
    int secondaryIndexEntries[] = new int[numTotalKeys];

    for (int i = 0; i < numTotalKeys; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(RNG, i * 2);
      KeyValue cell = new KeyValue(k, Bytes.toBytes("f"), Bytes.toBytes("q"),
          Bytes.toBytes("val"));
      //KeyValue cell = new KeyValue.KeyOnlyKeyValue(k, 0, k.length);
      keys.add(cell.getKey());
      String msgPrefix = "Key #" + i + " (" + Bytes.toStringBinary(k) + "): ";
      StringBuilder padding = new StringBuilder();
      while (msgPrefix.length() + padding.length() < 70)
        padding.append(' ');
      msgPrefix += padding;
      if (i % 2 == 1) {
        dos.writeInt(curAllEntriesSize);
        secondaryIndexEntries[i] = curAllEntriesSize;
        LOG.info(msgPrefix + "secondary index entry #" + ((i - 1) / 2) +
            ", offset " + curAllEntriesSize);
        curAllEntriesSize += cell.getKey().length
            + HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
        ++numEntriesAdded;
      } else {
        secondaryIndexEntries[i] = -1;
        LOG.info(msgPrefix + "not in the searched array");
      }
    }

    // Make sure the keys are increasing.
    for (int i = 0; i < keys.size() - 1; ++i)
      assertTrue(CellComparatorImpl.COMPARATOR.compare(
          new KeyValue.KeyOnlyKeyValue(keys.get(i), 0, keys.get(i).length),
          new KeyValue.KeyOnlyKeyValue(keys.get(i + 1), 0, keys.get(i + 1).length)) < 0);

    dos.writeInt(curAllEntriesSize);
    assertEquals(numSearchedKeys, numEntriesAdded);
    int secondaryIndexOffset = dos.size();
    assertEquals(Bytes.SIZEOF_INT * (numSearchedKeys + 2),
        secondaryIndexOffset);

    for (int i = 1; i <= numTotalKeys - 1; i += 2) {
      assertEquals(dos.size(),
          secondaryIndexOffset + secondaryIndexEntries[i]);
      long dummyFileOffset = getDummyFileOffset(i);
      int dummyOnDiskSize = getDummyOnDiskSize(i);
      LOG.debug("Storing file offset=" + dummyFileOffset + " and onDiskSize=" +
          dummyOnDiskSize + " at offset " + dos.size());
      dos.writeLong(dummyFileOffset);
      dos.writeInt(dummyOnDiskSize);
      LOG.debug("Stored key " + ((i - 1) / 2) +" at offset " + dos.size());
      dos.write(keys.get(i));
    }

    dos.writeInt(curAllEntriesSize);

    ByteBuffer nonRootIndex = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < numTotalKeys; ++i) {
      byte[] searchKey = keys.get(i);
      byte[] arrayHoldingKey = new byte[searchKey.length +
                                        searchKey.length / 2];

      // To make things a bit more interesting, store the key we are looking
      // for at a non-zero offset in a new array.
      System.arraycopy(searchKey, 0, arrayHoldingKey, searchKey.length / 2,
            searchKey.length);

      KeyValue.KeyOnlyKeyValue cell = new KeyValue.KeyOnlyKeyValue(
          arrayHoldingKey, searchKey.length / 2, searchKey.length);
      int searchResult = BlockIndexReader.binarySearchNonRootIndex(cell,
          new MultiByteBuff(nonRootIndex), CellComparatorImpl.COMPARATOR);
      String lookupFailureMsg = "Failed to look up key #" + i + " ("
          + Bytes.toStringBinary(searchKey) + ")";

      int expectedResult;
      int referenceItem;

      if (i % 2 == 1) {
        // This key is in the array we search as the element (i - 1) / 2. Make
        // sure we find it.
        expectedResult = (i - 1) / 2;
        referenceItem = i;
      } else {
        // This key is not in the array but between two elements on the array,
        // in the beginning, or in the end. The result should be the previous
        // key in the searched array, or -1 for i = 0.
        expectedResult = i / 2 - 1;
        referenceItem = i - 1;
      }

      assertEquals(lookupFailureMsg, expectedResult, searchResult);

      // Now test we can get the offset and the on-disk-size using a
      // higher-level API function.s
      boolean locateBlockResult =
          (BlockIndexReader.locateNonRootIndexEntry(new MultiByteBuff(nonRootIndex), cell,
          CellComparatorImpl.COMPARATOR) != -1);

      if (i == 0) {
        assertFalse(locateBlockResult);
      } else {
        assertTrue(locateBlockResult);
        String errorMsg = "i=" + i + ", position=" + nonRootIndex.position();
        assertEquals(errorMsg, getDummyFileOffset(referenceItem),
            nonRootIndex.getLong());
        assertEquals(errorMsg, getDummyOnDiskSize(referenceItem),
            nonRootIndex.getInt());
      }
    }

  }

  @Test
  public void testBlockIndexChunk() throws IOException {
    BlockIndexChunk c = new BlockIndexChunk();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int N = 1000;
    int[] numSubEntriesAt = new int[N];
    int numSubEntries = 0;
    for (int i = 0; i < N; ++i) {
      baos.reset();
      DataOutputStream dos = new DataOutputStream(baos);
      c.writeNonRoot(dos);
      assertEquals(c.getNonRootSize(), dos.size());

      baos.reset();
      dos = new DataOutputStream(baos);
      c.writeRoot(dos);
      assertEquals(c.getRootSize(), dos.size());

      byte[] k = RandomKeyValueUtil.randomOrderedKey(RNG, i);
      numSubEntries += RNG.nextInt(5) + 1;
      keys.add(k);
      c.add(k, getDummyFileOffset(i), getDummyOnDiskSize(i), numSubEntries);
    }

    // Test the ability to look up the entry that contains a particular
    // deeper-level index block's entry ("sub-entry"), assuming a global
    // 0-based ordering of sub-entries. This is needed for mid-key calculation.
    for (int i = 0; i < N; ++i) {
      for (int j = i == 0 ? 0 : numSubEntriesAt[i - 1];
           j < numSubEntriesAt[i];
           ++j) {
        assertEquals(i, c.getEntryBySubEntry(j));
      }
    }
  }

  /** Checks if the HeapSize calculator is within reason */
  @Test
  public void testHeapSizeForBlockIndex() throws IOException {
    Class<HFileBlockIndex.BlockIndexReader> cl =
        HFileBlockIndex.BlockIndexReader.class;
    long expected = ClassSize.estimateBase(cl, false);

    HFileBlockIndex.BlockIndexReader bi =
        new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
    long actual = bi.heapSize();

    // Since the arrays in BlockIndex(byte [][] blockKeys, long [] blockOffsets,
    // int [] blockDataSizes) are all null they are not going to show up in the
    // HeapSize calculation, so need to remove those array costs from expected.
    // Already the block keys are not there in this case
    expected -= ClassSize.align(2 * ClassSize.ARRAY);

    if (expected != actual) {
      expected = ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
  }

  /**
  * to check if looks good when midKey on a leaf index block boundary
  * @throws IOException
  */
  @Test
  public void testMidKeyOnLeafIndexBlockBoundary() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "hfile_for_midkey");
    int maxChunkSize = 512;
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, maxChunkSize);
    // should open hfile.block.index.cacheonwrite
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);
    CacheConfig cacheConf = new CacheConfig(conf, BlockCacheFactory.createBlockCache(conf));
    BlockCache blockCache = cacheConf.getBlockCache().get();
    // Evict all blocks that were cached-on-write by the previous invocation.
    blockCache.evictBlocksByHfileName(hfilePath.getName());
    // Write the HFile
    HFileContext meta =
        new HFileContextBuilder().withBlockSize(SMALL_BLOCK_SIZE).withCompression(Algorithm.NONE)
            .withDataBlockEncoding(DataBlockEncoding.NONE).build();
    HFile.Writer writer =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, hfilePath).withFileContext(meta)
            .create();
    Random rand = new Random(19231737);
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    int kvNumberToBeWritten = 16;
    // the new generated hfile will contain 2 leaf-index blocks and 16 data blocks,
    // midkey is just on the boundary of the first leaf-index block
    for (int i = 0; i < kvNumberToBeWritten; ++i) {
      byte[] row = RandomKeyValueUtil.randomOrderedFixedLengthKey(rand, i, 30);

      // Key will be interpreted by KeyValue.KEY_COMPARATOR
      KeyValue kv = new KeyValue(row, family, qualifier, EnvironmentEdgeManager.currentTime(),
          RandomKeyValueUtil.randomFixedLengthValue(rand, SMALL_BLOCK_SIZE));
      writer.append(kv);
    }
    writer.close();

    // close hfile.block.index.cacheonwrite
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, false);

    // Read the HFile
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, conf);

    boolean hasArrayIndexOutOfBoundsException = false;
    try {
      // get the mid-key.
      reader.midKey();
    } catch (ArrayIndexOutOfBoundsException e) {
      hasArrayIndexOutOfBoundsException = true;
    } finally {
      reader.close();
    }

    // to check if ArrayIndexOutOfBoundsException occurred
    assertFalse(hasArrayIndexOutOfBoundsException);
  }

  /**
   * Testing block index through the HFile writer/reader APIs. Allows to test
   * setting index block size through configuration, intermediate-level index
   * blocks, and caching index blocks on write.
   *
   * @throws IOException
   */
  @Test
  public void testHFileWriterAndReader() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
        "hfile_for_block_index");
    CacheConfig cacheConf = new CacheConfig(conf, BlockCacheFactory.createBlockCache(conf));
    BlockCache blockCache = cacheConf.getBlockCache().get();

    for (int testI = 0; testI < INDEX_CHUNK_SIZES.length; ++testI) {
      int indexBlockSize = INDEX_CHUNK_SIZES[testI];
      int expectedNumLevels = EXPECTED_NUM_LEVELS[testI];
      LOG.info("Index block size: " + indexBlockSize + ", compression: "
          + compr);
      // Evict all blocks that were cached-on-write by the previous invocation.
      blockCache.evictBlocksByHfileName(hfilePath.getName());

      conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, indexBlockSize);
      Set<String> keyStrSet = new HashSet<>();
      byte[][] keys = new byte[NUM_KV][];
      byte[][] values = new byte[NUM_KV][];

      // Write the HFile
      {
        HFileContext meta = new HFileContextBuilder()
                            .withBlockSize(SMALL_BLOCK_SIZE)
                            .withCompression(compr)
                            .build();
        HFile.Writer writer =
            HFile.getWriterFactory(conf, cacheConf)
                .withPath(fs, hfilePath)
                .withFileContext(meta)
                .create();
        Random rand = new Random(19231737);
        byte[] family = Bytes.toBytes("f");
        byte[] qualifier = Bytes.toBytes("q");
        for (int i = 0; i < NUM_KV; ++i) {
          byte[] row = RandomKeyValueUtil.randomOrderedKey(rand, i);

          // Key will be interpreted by KeyValue.KEY_COMPARATOR
          KeyValue kv =
              new KeyValue(row, family, qualifier, EnvironmentEdgeManager.currentTime(),
                  RandomKeyValueUtil.randomValue(rand));
          byte[] k = kv.getKey();
          writer.append(kv);
          keys[i] = k;
          values[i] = CellUtil.cloneValue(kv);
          keyStrSet.add(Bytes.toStringBinary(k));
          if (i > 0) {
            assertTrue((PrivateCellUtil.compare(CellComparatorImpl.COMPARATOR, kv, keys[i - 1],
                0, keys[i - 1].length)) > 0);
          }
        }

        writer.close();
      }

      // Read the HFile
      HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, conf);
      assertEquals(expectedNumLevels,
          reader.getTrailer().getNumDataIndexLevels());

      assertTrue(Bytes.equals(keys[0], ((KeyValue)reader.getFirstKey().get()).getKey()));
      assertTrue(Bytes.equals(keys[NUM_KV - 1], ((KeyValue)reader.getLastKey().get()).getKey()));
      LOG.info("Last key: " + Bytes.toStringBinary(keys[NUM_KV - 1]));

      for (boolean pread : new boolean[] { false, true }) {
        HFileScanner scanner = reader.getScanner(conf, true, pread);
        for (int i = 0; i < NUM_KV; ++i) {
          checkSeekTo(keys, scanner, i);
          checkKeyValue("i=" + i, keys[i], values[i],
              ByteBuffer.wrap(((KeyValue) scanner.getKey()).getKey()), scanner.getValue());
        }
        assertTrue(scanner.seekTo());
        for (int i = NUM_KV - 1; i >= 0; --i) {
          checkSeekTo(keys, scanner, i);
          checkKeyValue("i=" + i, keys[i], values[i],
              ByteBuffer.wrap(((KeyValue) scanner.getKey()).getKey()), scanner.getValue());
        }
      }

      // Manually compute the mid-key and validate it.
      HFile.Reader reader2 = reader;
      HFileBlock.FSReader fsReader = reader2.getUncachedBlockReader();

      HFileBlock.BlockIterator iter = fsReader.blockRange(0,
          reader.getTrailer().getLoadOnOpenDataOffset());
      HFileBlock block;
      List<byte[]> blockKeys = new ArrayList<>();
      while ((block = iter.nextBlock()) != null) {
        if (block.getBlockType() != BlockType.LEAF_INDEX)
          return;
        ByteBuff b = block.getBufferReadOnly();
        int n = b.getIntAfterPosition(0);
        // One int for the number of items, and n + 1 for the secondary index.
        int entriesOffset = Bytes.SIZEOF_INT * (n + 2);

        // Get all the keys from the leaf index block. S
        for (int i = 0; i < n; ++i) {
          int keyRelOffset = b.getIntAfterPosition(Bytes.SIZEOF_INT * (i + 1));
          int nextKeyRelOffset = b.getIntAfterPosition(Bytes.SIZEOF_INT * (i + 2));
          int keyLen = nextKeyRelOffset - keyRelOffset;
          int keyOffset = b.arrayOffset() + entriesOffset + keyRelOffset +
              HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
          byte[] blockKey = Arrays.copyOfRange(b.array(), keyOffset, keyOffset
              + keyLen);
          String blockKeyStr = Bytes.toString(blockKey);
          blockKeys.add(blockKey);

          // If the first key of the block is not among the keys written, we
          // are not parsing the non-root index block format correctly.
          assertTrue("Invalid block key from leaf-level block: " + blockKeyStr,
              keyStrSet.contains(blockKeyStr));
        }
      }

      // Validate the mid-key.
      assertEquals(
          Bytes.toStringBinary(blockKeys.get((blockKeys.size() - 1) / 2)),
          reader.midKey());

      assertEquals(UNCOMPRESSED_INDEX_SIZES[testI],
          reader.getTrailer().getUncompressedDataIndexSize());

      reader.close();
      reader2.close();
    }
  }

  private void checkSeekTo(byte[][] keys, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Failed to seek to key #" + i + " (" + Bytes.toStringBinary(keys[i]) + ")", 0,
        scanner.seekTo(KeyValueUtil.createKeyValueFromKey(keys[i])));
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

  @Test
  public void testIntermediateLevelIndicesWithLargeKeys() throws IOException {
    testIntermediateLevelIndicesWithLargeKeys(16);
  }

  @Test
  public void testIntermediateLevelIndicesWithLargeKeysWithMinNumEntries() throws IOException {
    // because of the large rowKeys, we will end up with a 50-level block index without sanity check
    testIntermediateLevelIndicesWithLargeKeys(2);
  }

  public void testIntermediateLevelIndicesWithLargeKeys(int minNumEntries) throws IOException {
    Path hfPath = new Path(TEST_UTIL.getDataTestDir(),
      "testIntermediateLevelIndicesWithLargeKeys.hfile");
    int maxChunkSize = 1024;
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, maxChunkSize);
    conf.setInt(HFileBlockIndex.MIN_INDEX_NUM_ENTRIES_KEY, minNumEntries);
    HFileContext context = new HFileContextBuilder().withBlockSize(16).build();
    HFile.Writer hfw = new HFile.WriterFactory(conf, cacheConf)
            .withFileContext(context)
            .withPath(fs, hfPath).create();
    List<byte[]> keys = new ArrayList<>();

    // This should result in leaf-level indices and a root level index
    for (int i=0; i < 100; i++) {
      byte[] rowkey = new byte[maxChunkSize + 1];
      byte[] b = Bytes.toBytes(i);
      System.arraycopy(b, 0, rowkey, rowkey.length - b.length, b.length);
      keys.add(rowkey);
      hfw.append(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(rowkey).setFamily(HConstants.EMPTY_BYTE_ARRAY)
        .setQualifier(HConstants.EMPTY_BYTE_ARRAY)
        .setTimestamp(HConstants.LATEST_TIMESTAMP)
        .setType(KeyValue.Type.Maximum.getCode())
        .setValue(HConstants.EMPTY_BYTE_ARRAY)
        .build());
    }
    hfw.close();

    HFile.Reader reader = HFile.createReader(fs, hfPath, cacheConf, true, conf);
    // Scanner doesn't do Cells yet.  Fix.
    HFileScanner scanner = reader.getScanner(conf, true, true);
    for (int i = 0; i < keys.size(); ++i) {
      scanner.seekTo(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(keys.get(i)).setFamily(HConstants.EMPTY_BYTE_ARRAY)
        .setQualifier(HConstants.EMPTY_BYTE_ARRAY)
        .setTimestamp(HConstants.LATEST_TIMESTAMP)
        .setType(KeyValue.Type.Maximum.getCode())
        .setValue(HConstants.EMPTY_BYTE_ARRAY)
        .build());
    }
    reader.close();
  }
}

