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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * {@link HFile} reader for version 2.
 */
public class HFileReaderV2 extends AbstractHFileReader {

  private static final Log LOG = LogFactory.getLog(HFileReaderV2.class);

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  private static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  private boolean includesMemstoreTS = false;
  private boolean decodeMemstoreTS = false;

  private boolean shouldIncludeMemstoreTS() {
    return includesMemstoreTS;
  }

  /**
   * A "sparse lock" implementation allowing to lock on a particular block
   * identified by offset. The purpose of this is to avoid two clients loading
   * the same block, and have all but one client wait to get the block from the
   * cache.
   */
  private IdLock offsetLock = new IdLock();

  /**
   * Blocks read from the load-on-open section, excluding data root index, meta
   * index, and file info.
   */
  private List<HFileBlock> loadOnOpenBlocks = new ArrayList<HFileBlock>();

  /** Minimum minor version supported by this HFile format */
  static final int MIN_MINOR_VERSION = 0;

  /** Maximum minor version supported by this HFile format */
  static final int MAX_MINOR_VERSION = 1;

  /**
   * Opens a HFile. You must load the index before you can use it by calling
   * {@link #loadFileInfo()}.
   *
   * @param path Path to HFile.
   * @param trailer File trailer.
   * @param fsdis input stream. Caller is responsible for closing the passed
   *          stream.
   * @param size Length of the stream.
   * @param closeIStream Whether to close the stream.
   * @param cacheConf Cache configuration.
   * @param preferredEncodingInCache the encoding to use in cache in case we
   *          have a choice. If the file is already encoded on disk, we will
   *          still use its on-disk encoding in cache.
   */
  public HFileReaderV2(Path path, FixedFileTrailer trailer,
      final FSDataInputStream fsdis, final FSDataInputStream fsdisNoFsChecksum,
      final long size,
      final boolean closeIStream, final CacheConfig cacheConf,
      DataBlockEncoding preferredEncodingInCache, final HFileSystem hfs)
      throws IOException {
    super(path, trailer, fsdis, fsdisNoFsChecksum, size, 
          closeIStream, cacheConf, hfs);
    trailer.expectMajorVersion(2);
    validateMinorVersion(path, trailer.getMinorVersion());
    HFileBlock.FSReaderV2 fsBlockReaderV2 = new HFileBlock.FSReaderV2(fsdis,
        fsdisNoFsChecksum,
        compressAlgo, fileSize, trailer.getMinorVersion(), hfs, path);
    this.fsBlockReader = fsBlockReaderV2; // upcast

    // Comparator class name is stored in the trailer in version 2.
    comparator = trailer.createComparator();
    dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(comparator,
        trailer.getNumDataIndexLevels(), this);
    metaBlockIndexReader = new HFileBlockIndex.BlockIndexReader(
        Bytes.BYTES_RAWCOMPARATOR, 1);

    // Parse load-on-open data.

    HFileBlock.BlockIterator blockIter = fsBlockReaderV2.blockRange(
        trailer.getLoadOnOpenDataOffset(),
        fileSize - trailer.getTrailerSize());

    // Data index. We also read statistics about the block index written after
    // the root level.
    dataBlockIndexReader.readMultiLevelIndexRoot(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),
        trailer.getDataIndexCount());

    // Meta index.
    metaBlockIndexReader.readRootIndex(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),
        trailer.getMetaIndexCount());

    // File info
    fileInfo = new FileInfo();
    fileInfo.readFields(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    lastKey = fileInfo.get(FileInfo.LASTKEY);
    avgKeyLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_VALUE_LEN));
    byte [] keyValueFormatVersion =
        fileInfo.get(HFileWriterV2.KEY_VALUE_VERSION);
    includesMemstoreTS = keyValueFormatVersion != null &&
        Bytes.toInt(keyValueFormatVersion) ==
            HFileWriterV2.KEY_VALUE_VER_WITH_MEMSTORE;
    fsBlockReaderV2.setIncludesMemstoreTS(includesMemstoreTS);
    if (includesMemstoreTS) {
      decodeMemstoreTS = Bytes.toLong(fileInfo.get(HFileWriterV2.MAX_MEMSTORE_TS_KEY)) > 0;
    }

    // Read data block encoding algorithm name from file info.
    dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo,
        preferredEncodingInCache);
    fsBlockReaderV2.setDataBlockEncoder(dataBlockEncoder);

    // Store all other load-on-open blocks for further consumption.
    HFileBlock b;
    while ((b = blockIter.nextBlock()) != null) {
      loadOnOpenBlocks.add(b);
    }
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @param isCompaction is scanner being used for a compaction?
   * @return Scanner on this file.
   */
   @Override
   public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
      final boolean isCompaction) {
    // check if we want to use data block encoding in memory
    if (dataBlockEncoder.useEncodedScanner(isCompaction)) {
      return new EncodedScannerV2(this, cacheBlocks, pread, isCompaction,
          includesMemstoreTS);
    }

    return new ScannerV2(this, cacheBlocks, pread, isCompaction);
  }

  /**
   * @param metaBlockName
   * @param cacheBlock Add block to cache, if found
   * @return block wrapped in a ByteBuffer, with header skipped
   * @throws IOException
   */
  @Override
  public ByteBuffer getMetaBlock(String metaBlockName, boolean cacheBlock)
      throws IOException {
    if (trailer.getMetaIndexCount() == 0) {
      return null; // there are no meta blocks
    }
    if (metaBlockIndexReader == null) {
      throw new IOException("Meta index not loaded");
    }

    byte[] mbname = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname, 0,
        mbname.length);
    if (block == -1)
      return null;
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);
    long startTimeNs = System.nanoTime();

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      BlockCacheKey cacheKey = new BlockCacheKey(name, metaBlockOffset,
          DataBlockEncoding.NONE, BlockType.META);

      cacheBlock &= cacheConf.shouldCacheDataOnRead();
      if (cacheConf.isBlockCacheEnabled()) {
        HFileBlock cachedBlock =
          (HFileBlock) cacheConf.getBlockCache().getBlock(cacheKey, cacheBlock, false);
        if (cachedBlock != null) {
          // Return a distinct 'shallow copy' of the block,
          // so pos does not get messed by the scanner
          getSchemaMetrics().updateOnCacheHit(BlockCategory.META, false);
          return cachedBlock.getBufferWithoutHeader();
        }
        // Cache Miss, please load.
      }

      HFileBlock metaBlock = fsBlockReader.readBlockData(metaBlockOffset,
          blockSize, -1, true);
      passSchemaMetricsTo(metaBlock);

      final long delta = System.nanoTime() - startTimeNs;
      HFile.offerReadLatency(delta, true);
      getSchemaMetrics().updateOnCacheMiss(BlockCategory.META, false, delta);

      // Cache the block
      if (cacheBlock) {
        cacheConf.getBlockCache().cacheBlock(cacheKey, metaBlock,
            cacheConf.isInMemory());
      }

      return metaBlock.getBufferWithoutHeader();
    }
  }

  /**
   * Read in a file block.
   * @param dataBlockOffset offset to read.
   * @param onDiskBlockSize size of the block
   * @param cacheBlock
   * @param pread Use positional read instead of seek+read (positional is
   *          better doing random reads whereas seek+read is better scanning).
   * @param isCompaction is this block being read as part of a compaction
   * @param expectedBlockType the block type we are expecting to read with this
   *          read operation, or null to read whatever block type is available
   *          and avoid checking (that might reduce caching efficiency of
   *          encoded data blocks)
   * @return Block wrapped in a ByteBuffer.
   * @throws IOException
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize,
      final boolean cacheBlock, boolean pread, final boolean isCompaction,
      BlockType expectedBlockType)
      throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException("Block index not loaded");
    }
    if (dataBlockOffset < 0
        || dataBlockOffset >= trailer.getLoadOnOpenDataOffset()) {
      throw new IOException("Requested block is out of range: "
          + dataBlockOffset + ", lastDataBlockOffset: "
          + trailer.getLastDataBlockOffset());
    }
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    BlockCacheKey cacheKey =
        new BlockCacheKey(name, dataBlockOffset,
            dataBlockEncoder.getEffectiveEncodingInCache(isCompaction),
            expectedBlockType);

    boolean useLock = false;
    IdLock.Entry lockEntry = null;

    try {
      while (true) {

        if (useLock) {
          lockEntry = offsetLock.getLockEntry(dataBlockOffset);
        }

        // Check cache for block. If found return.
        if (cacheConf.isBlockCacheEnabled()) {
          // Try and get the block from the block cache.  If the useLock variable is true then this
          // is the second time through the loop and it should not be counted as a block cache miss.
          HFileBlock cachedBlock = (HFileBlock)
              cacheConf.getBlockCache().getBlock(cacheKey, cacheBlock, useLock);
          if (cachedBlock != null) {
            BlockCategory blockCategory =
                cachedBlock.getBlockType().getCategory();

            getSchemaMetrics().updateOnCacheHit(blockCategory, isCompaction);

            if (cachedBlock.getBlockType() == BlockType.DATA) {
              HFile.dataBlockReadCnt.incrementAndGet();
            }

            validateBlockType(cachedBlock, expectedBlockType);

            // Validate encoding type for encoded blocks. We include encoding
            // type in the cache key, and we expect it to match on a cache hit.
            if (cachedBlock.getBlockType() == BlockType.ENCODED_DATA &&
                cachedBlock.getDataBlockEncoding() !=
                    dataBlockEncoder.getEncodingInCache()) {
              throw new IOException("Cached block under key " + cacheKey + " " +
                  "has wrong encoding: " + cachedBlock.getDataBlockEncoding() +
                  " (expected: " + dataBlockEncoder.getEncodingInCache() + ")");
            }
            return cachedBlock;
          }
          // Carry on, please load.
        }
        if (!useLock) {
          // check cache again with lock
          useLock = true;
          continue;
        }

        // Load block from filesystem.
        long startTimeNs = System.nanoTime();
        HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset,
            onDiskBlockSize, -1, pread);
        hfileBlock = dataBlockEncoder.diskToCacheFormat(hfileBlock,
            isCompaction);
        validateBlockType(hfileBlock, expectedBlockType);
        passSchemaMetricsTo(hfileBlock);
        BlockCategory blockCategory = hfileBlock.getBlockType().getCategory();

        final long delta = System.nanoTime() - startTimeNs;
        HFile.offerReadLatency(delta, pread);
        getSchemaMetrics().updateOnCacheMiss(blockCategory, isCompaction, delta);

        // Cache the block if necessary
        if (cacheBlock && cacheConf.shouldCacheBlockOnRead(
            hfileBlock.getBlockType().getCategory())) {
          cacheConf.getBlockCache().cacheBlock(cacheKey, hfileBlock,
              cacheConf.isInMemory());
        }

        if (hfileBlock.getBlockType() == BlockType.DATA) {
          HFile.dataBlockReadCnt.incrementAndGet();
        }

        return hfileBlock;
      }
    } finally {
      if (lockEntry != null) {
        offsetLock.releaseLockEntry(lockEntry);
      }
    }
  }

  @Override
  public boolean hasMVCCInfo() {
    return includesMemstoreTS && decodeMemstoreTS;
  }

  /**
   * Compares the actual type of a block retrieved from cache or disk with its
   * expected type and throws an exception in case of a mismatch. Expected
   * block type of {@link BlockType#DATA} is considered to match the actual
   * block type [@link {@link BlockType#ENCODED_DATA} as well.
   * @param block a block retrieved from cache or disk
   * @param expectedBlockType the expected block type, or null to skip the
   *          check
   */
  private void validateBlockType(HFileBlock block,
      BlockType expectedBlockType) throws IOException {
    if (expectedBlockType == null) {
      return;
    }
    BlockType actualBlockType = block.getBlockType();
    if (actualBlockType == BlockType.ENCODED_DATA &&
        expectedBlockType == BlockType.DATA) {
      // We consider DATA to match ENCODED_DATA for the purpose of this
      // verification.
      return;
    }
    if (actualBlockType != expectedBlockType) {
      throw new IOException("Expected block type " + expectedBlockType + ", " +
          "but got " + actualBlockType + ": " + block);
    }
  }

  /**
   * @return Last key in the file. May be null if file has no entries. Note that
   *         this is not the last row key, but rather the byte form of the last
   *         KeyValue.
   */
  @Override
  public byte[] getLastKey() {
    return dataBlockIndexReader.isEmpty() ? null : lastKey;
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   * @throws IOException
   */
  @Override
  public byte[] midkey() throws IOException {
    return dataBlockIndexReader.midkey();
  }

  @Override
  public void close() throws IOException {
    close(cacheConf.shouldEvictOnClose());
  }

  public void close(boolean evictOnClose) throws IOException {
    if (evictOnClose && cacheConf.isBlockCacheEnabled()) {
      int numEvicted = cacheConf.getBlockCache().evictBlocksByHfileName(name);
      if (LOG.isTraceEnabled()) {
        LOG.trace("On close, file=" + name + " evicted=" + numEvicted
          + " block(s)");
      }
    }
    if (closeIStream) {
      if (istream != istreamNoFsChecksum && istreamNoFsChecksum != null) {
        istreamNoFsChecksum.close();
        istreamNoFsChecksum = null;
      }
      if (istream != null) {
        istream.close();
        istream = null;
      }
    }

    getSchemaMetrics().flushMetrics();
  }

  protected abstract static class AbstractScannerV2
      extends AbstractHFileReader.Scanner {
    protected HFileBlock block;

    /**
     * The next indexed key is to keep track of the indexed key of the next data block.
     * If the nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the
     * current data block is the last data block.
     *
     * If the nextIndexedKey is null, it means the nextIndexedKey has not been loaded yet.
     */
    protected byte[] nextIndexedKey;

    public AbstractScannerV2(HFileReaderV2 r, boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      super(r, cacheBlocks, pread, isCompaction);
    }

    /**
     * An internal API function. Seek to the given key, optionally rewinding to
     * the first key of the block before doing the seek.
     *
     * @param key key byte array
     * @param offset key offset in the key byte array
     * @param length key length
     * @param rewind whether to rewind to the first key of the block before
     *        doing the seek. If this is false, we are assuming we never go
     *        back, otherwise the result is undefined.
     * @return -1 if the key is earlier than the first key of the file,
     *         0 if we are at the given key, and 1 if we are past the given key
     * @throws IOException
     */
    protected int seekTo(byte[] key, int offset, int length, boolean rewind)
        throws IOException {
      HFileBlockIndex.BlockIndexReader indexReader =
          reader.getDataBlockIndexReader();
      BlockWithScanInfo blockWithScanInfo =
        indexReader.loadDataBlockWithScanInfo(key, offset, length, block,
            cacheBlocks, pread, isCompaction);
      if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
        // This happens if the key e.g. falls before the beginning of the file.
        return -1;
      }
      return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
          blockWithScanInfo.getNextIndexedKey(), rewind, key, offset, length, false);
    }

    protected abstract ByteBuffer getFirstKeyInBlock(HFileBlock curBlock);

    protected abstract int loadBlockAndSeekToKey(HFileBlock seekToBlock, byte[] nextIndexedKey,
        boolean rewind, byte[] key, int offset, int length, boolean seekBefore)
        throws IOException;

    @Override
    public int seekTo(byte[] key, int offset, int length) throws IOException {
      // Always rewind to the first key of the block, because the given key
      // might be before or after the current key.
      return seekTo(key, offset, length, true);
    }

    @Override
    public int reseekTo(byte[] key, int offset, int length) throws IOException {
      int compared;
      if (isSeeked()) {
        compared = compareKey(reader.getComparator(), key, offset, length);
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        } else {
          if (this.nextIndexedKey != null &&
              (this.nextIndexedKey == HConstants.NO_NEXT_INDEXED_KEY ||
               reader.getComparator().compare(key, offset, length,
                   nextIndexedKey, 0, nextIndexedKey.length) < 0)) {
            // The reader shall continue to scan the current data block instead of querying the
            // block index as long as it knows the target key is strictly smaller than
            // the next indexed key or the current data block is the last data block.
            return loadBlockAndSeekToKey(this.block, this.nextIndexedKey,
                false, key, offset, length, false);
          }
        }
      }
      // Don't rewind on a reseek operation, because reseek implies that we are
      // always going forward in the file.
      return seekTo(key, offset, length, false);
    }

    @Override
    public boolean seekBefore(byte[] key, int offset, int length)
        throws IOException {
      HFileBlock seekToBlock =
          reader.getDataBlockIndexReader().seekToDataBlock(key, offset, length,
              block, cacheBlocks, pread, isCompaction);
      if (seekToBlock == null) {
        return false;
      }
      ByteBuffer firstKey = getFirstKeyInBlock(seekToBlock);

      if (reader.getComparator().compare(firstKey.array(),
          firstKey.arrayOffset(), firstKey.limit(), key, offset, length) == 0)
      {
        long previousBlockOffset = seekToBlock.getPrevBlockOffset();
        // The key we are interested in
        if (previousBlockOffset == -1) {
          // we have a 'problem', the key we want is the first of the file.
          return false;
        }

        // It is important that we compute and pass onDiskSize to the block
        // reader so that it does not have to read the header separately to
        // figure out the size.
        seekToBlock = reader.readBlock(previousBlockOffset,
            seekToBlock.getOffset() - previousBlockOffset, cacheBlocks,
            pread, isCompaction, BlockType.DATA);
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      byte[] firstKeyInCurrentBlock = Bytes.getBytes(firstKey);
      loadBlockAndSeekToKey(seekToBlock, firstKeyInCurrentBlock, true, key, offset, length, true);
      return true;
    }


    /**
     * Scans blocks in the "scanned" section of the {@link HFile} until the next
     * data block is found.
     *
     * @return the next block, or null if there are no more data blocks
     * @throws IOException
     */
    protected HFileBlock readNextDataBlock() throws IOException {
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (block == null)
        return null;

      HFileBlock curBlock = block;

      do {
        if (curBlock.getOffset() >= lastDataBlockOffset)
          return null;

        if (curBlock.getOffset() < 0) {
          throw new IOException("Invalid block file offset: " + block);
        }

        // We are reading the next block without block type validation, because
        // it might turn out to be a non-data block.
        curBlock = reader.readBlock(curBlock.getOffset()
            + curBlock.getOnDiskSizeWithHeader(),
            curBlock.getNextBlockOnDiskSizeWithHeader(), cacheBlocks, pread,
            isCompaction, null);
      } while (!(curBlock.getBlockType().equals(BlockType.DATA) ||
          curBlock.getBlockType().equals(BlockType.ENCODED_DATA)));

      return curBlock;
    }
    /**
     * Compare the given key against the current key
     * @param comparator
     * @param key
     * @param offset
     * @param length
     * @return -1 is the passed key is smaller than the current key, 0 if equal and 1 if greater
     */
    public abstract int compareKey(RawComparator<byte[]> comparator, byte[] key, int offset,
        int length);
  }

  /**
   * Implementation of {@link HFileScanner} interface.
   */
  protected static class ScannerV2 extends AbstractScannerV2 {
    private HFileReaderV2 reader;

    public ScannerV2(HFileReaderV2 r, boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      super(r, cacheBlocks, pread, isCompaction);
      this.reader = r;
    }

    @Override
    public KeyValue getKeyValue() {
      if (!isSeeked())
        return null;

      KeyValue ret = new KeyValue(blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position(),
          KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen);
      if (this.reader.shouldIncludeMemstoreTS()) {
        ret.setMemstoreTS(currMemstoreTS);
      }
      return ret;
    }

    @Override
    public ByteBuffer getKey() {
      assertSeeked();
      return ByteBuffer.wrap(
          blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE, currKeyLen).slice();
    }

    @Override
    public int compareKey(RawComparator<byte []> comparator, byte[] key, int offset, int length) {
      return comparator.compare(key, offset, length, blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen);
      }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      return ByteBuffer.wrap(
          blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE + currKeyLen, currValueLen).slice();
    }

    private void setNonSeekedState() {
      block = null;
      blockBuffer = null;
      currKeyLen = 0;
      currValueLen = 0;
      currMemstoreTS = 0;
      currMemstoreTSLen = 0;
    }

    /**
     * Go to the next key/value in the block section. Loads the next block if
     * necessary. If successful, {@link #getKey()} and {@link #getValue()} can
     * be called.
     *
     * @return true if successfully navigated to the next key/value
     */
    @Override
    public boolean next() throws IOException {
      assertSeeked();

      try {
        blockBuffer.position(blockBuffer.position() + KEY_VALUE_LEN_SIZE
            + currKeyLen + currValueLen + currMemstoreTSLen);
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position()
            + "; currKeyLen = " + currKeyLen + "; currValLen = "
            + currValueLen + "; block limit = " + blockBuffer.limit()
            + "; HFile name = " + reader.getName()
            + "; currBlock currBlockOffset = " + block.getOffset());
        throw e;
      }

      if (blockBuffer.remaining() <= 0) {
        long lastDataBlockOffset =
            reader.getTrailer().getLastDataBlockOffset();

        if (block.getOffset() >= lastDataBlockOffset) {
          setNonSeekedState();
          return false;
        }

        // read the next block
        HFileBlock nextBlock = readNextDataBlock();
        if (nextBlock == null) {
          setNonSeekedState();
          return false;
        }

        updateCurrBlock(nextBlock);
        return true;
      }

      // We are still in the same block.
      readKeyValueLen();
      return true;
    }

    /**
     * Positions this scanner at the start of the file.
     *
     * @return false if empty file; i.e. a call to next would return false and
     *         the current key and value are undefined.
     * @throws IOException
     */
    @Override
    public boolean seekTo() throws IOException {
      if (reader == null) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset =
          reader.getTrailer().getFirstDataBlockOffset();
      if (block != null && block.getOffset() == firstDataBlockOffset) {
        blockBuffer.rewind();
        readKeyValueLen();
        return true;
      }

      block = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
          isCompaction, BlockType.DATA);
      if (block.getOffset() < 0) {
        throw new IOException("Invalid block offset: " + block.getOffset());
      }
      updateCurrBlock(block);
      return true;
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, byte[] nextIndexedKey,
        boolean rewind, byte[] key, int offset, int length, boolean seekBefore)
        throws IOException {
      if (block == null || block.getOffset() != seekToBlock.getOffset()) {
        updateCurrBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }

      // Update the nextIndexedKey
      this.nextIndexedKey = nextIndexedKey;
      return blockSeek(key, offset, length, seekBefore);
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     */
    private void updateCurrBlock(HFileBlock newBlock) {
      block = newBlock;

      // sanity check
      if (block.getBlockType() != BlockType.DATA) {
        throw new IllegalStateException("ScannerV2 works only on data " +
            "blocks, got " + block.getBlockType() + "; " +
            "fileName=" + reader.name + ", " +
            "dataBlockEncoder=" + reader.dataBlockEncoder + ", " +
            "isCompaction=" + isCompaction);
      }

      blockBuffer = block.getBufferWithoutHeader();
      readKeyValueLen();
      blockFetches++;

      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    private final void readKeyValueLen() {
      blockBuffer.mark();
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      blockBuffer.reset();
      if (this.reader.shouldIncludeMemstoreTS()) {
        if (this.reader.decodeMemstoreTS) {
          try {
            int memstoreTSOffset = blockBuffer.arrayOffset()
                + blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen
                + currValueLen;
            currMemstoreTS = Bytes.readVLong(blockBuffer.array(),
                memstoreTSOffset);
            currMemstoreTSLen = WritableUtils.getVIntSize(currMemstoreTS);
          } catch (Exception e) {
            throw new RuntimeException("Error reading memstore timestamp", e);
          }
        } else {
          currMemstoreTS = 0;
          currMemstoreTSLen = 1;
        }
      }

      if (currKeyLen < 0 || currValueLen < 0
          || currKeyLen > blockBuffer.limit()
          || currValueLen > blockBuffer.limit()) {
        throw new IllegalStateException("Invalid currKeyLen " + currKeyLen
            + " or currValueLen " + currValueLen + ". Block offset: "
            + block.getOffset() + ", block length: " + blockBuffer.limit()
            + ", position: " + blockBuffer.position() + " (without header).");
      }
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller
     * than (or equal to?) the key we are interested in.
     *
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
     *
     * @param key the key to find
     * @param seekBefore find the key before the given key in case of exact
     *          match.
     * @return 0 in case of an exact key match, 1 in case of an inexact match
     */
    private int blockSeek(byte[] key, int offset, int length,
        boolean seekBefore) {
      int klen, vlen;
      long memstoreTS = 0;
      int memstoreTSLen = 0;
      int lastKeyValueSize = -1;
      do {
        blockBuffer.mark();
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        blockBuffer.reset();
        if (this.reader.shouldIncludeMemstoreTS()) {
          if (this.reader.decodeMemstoreTS) {
            try {
              int memstoreTSOffset = blockBuffer.arrayOffset()
                  + blockBuffer.position() + KEY_VALUE_LEN_SIZE + klen + vlen;
              memstoreTS = Bytes.readVLong(blockBuffer.array(),
                  memstoreTSOffset);
              memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
            } catch (Exception e) {
              throw new RuntimeException("Error reading memstore timestamp", e);
            }
          } else {
            memstoreTS = 0;
            memstoreTSLen = 1;
          }
        }

        int keyOffset = blockBuffer.arrayOffset() + blockBuffer.position()
            + KEY_VALUE_LEN_SIZE;
        int comp = reader.getComparator().compare(key, offset, length,
            blockBuffer.array(), keyOffset, klen);

        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key="
                  + Bytes.toStringBinary(key) + ", blockOffset="
                  + block.getOffset() + ", onDiskSize="
                  + block.getOnDiskSizeWithHeader());
            }
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          if (this.reader.shouldIncludeMemstoreTS()) {
            currMemstoreTS = memstoreTS;
            currMemstoreTSLen = memstoreTSLen;
          }
          return 0; // indicate exact match
        }

        if (comp < 0) {
          if (lastKeyValueSize > 0)
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
          readKeyValueLen();
          return 1;
        }

        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + memstoreTSLen + KEY_VALUE_LEN_SIZE;
        blockBuffer.position(blockBuffer.position() + lastKeyValueSize);
      } while (blockBuffer.remaining() > 0);

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

    @Override
    protected ByteBuffer getFirstKeyInBlock(HFileBlock curBlock) {
      ByteBuffer buffer = curBlock.getBufferWithoutHeader();
      // It is safe to manipulate this buffer because we own the buffer object.
      buffer.rewind();
      int klen = buffer.getInt();
      buffer.getInt();
      ByteBuffer keyBuff = buffer.slice();
      keyBuff.limit(klen);
      keyBuff.rewind();
      return keyBuff;
    }

    @Override
    public String getKeyString() {
      return Bytes.toStringBinary(blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE, currKeyLen);
    }

    @Override
    public String getValueString() {
      return Bytes.toString(blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
          currValueLen);
    }
  }

  /**
   * ScannerV2 that operates on encoded data blocks.
   */
  protected static class EncodedScannerV2 extends AbstractScannerV2 {
    private DataBlockEncoder.EncodedSeeker seeker = null;
    private DataBlockEncoder dataBlockEncoder = null;
    private final boolean includesMemstoreTS;

    public EncodedScannerV2(HFileReaderV2 reader, boolean cacheBlocks,
        boolean pread, boolean isCompaction, boolean includesMemstoreTS) {
      super(reader, cacheBlocks, pread, isCompaction);
      this.includesMemstoreTS = includesMemstoreTS;
    }

    private void setDataBlockEncoder(DataBlockEncoder dataBlockEncoder) {
      this.dataBlockEncoder = dataBlockEncoder;
      seeker = dataBlockEncoder.createSeeker(reader.getComparator(),
          includesMemstoreTS);
    }

    @Override
    public boolean isSeeked(){
      return this.block != null;
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     */
    private void updateCurrentBlock(HFileBlock newBlock) {
      block = newBlock;

      // sanity checks
      if (block.getBlockType() != BlockType.ENCODED_DATA) {
        throw new IllegalStateException(
            "EncodedScannerV2 works only on encoded data blocks");
      }

      short dataBlockEncoderId = block.getDataBlockEncodingId();
      if (dataBlockEncoder == null ||
          !DataBlockEncoding.isCorrectEncoder(dataBlockEncoder,
              dataBlockEncoderId)) {
        DataBlockEncoder encoder =
            DataBlockEncoding.getDataBlockEncoderById(dataBlockEncoderId);
        setDataBlockEncoder(encoder);
      }

      seeker.setCurrentBuffer(getEncodedBuffer(newBlock));
      blockFetches++;

      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    private ByteBuffer getEncodedBuffer(HFileBlock newBlock) {
      ByteBuffer origBlock = newBlock.getBufferReadOnly();
      ByteBuffer encodedBlock = ByteBuffer.wrap(origBlock.array(),
          origBlock.arrayOffset() + newBlock.headerSize() +
          DataBlockEncoding.ID_SIZE,
          newBlock.getUncompressedSizeWithoutHeader() -
          DataBlockEncoding.ID_SIZE).slice();
      return encodedBlock;
    }

    @Override
    public boolean seekTo() throws IOException {
      if (reader == null) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset =
          reader.getTrailer().getFirstDataBlockOffset();
      if (block != null && block.getOffset() == firstDataBlockOffset) {
        seeker.rewind();
        return true;
      }

      block = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
          isCompaction, BlockType.DATA);
      if (block.getOffset() < 0) {
        throw new IOException("Invalid block offset: " + block.getOffset());
      }
      updateCurrentBlock(block);
      return true;
    }

    @Override
    public boolean next() throws IOException {
      boolean isValid = seeker.next();
      if (!isValid) {
        block = readNextDataBlock();
        isValid = block != null;
        if (isValid) {
          updateCurrentBlock(block);
        }
      }
      return isValid;
    }

    @Override
    public ByteBuffer getKey() {
      assertValidSeek();
      return seeker.getKeyDeepCopy();
    }

    @Override
    public int compareKey(RawComparator<byte []> comparator, byte[] key, int offset, int length) {
      return seeker.compareKey(comparator, key, offset, length);
    }

    @Override
    public ByteBuffer getValue() {
      assertValidSeek();
      return seeker.getValueShallowCopy();
    }

    @Override
    public KeyValue getKeyValue() {
      if (block == null) {
        return null;
      }
      return seeker.getKeyValue();
    }

    @Override
    public String getKeyString() {
      ByteBuffer keyBuffer = getKey();
      return Bytes.toStringBinary(keyBuffer.array(),
          keyBuffer.arrayOffset(), keyBuffer.limit());
    }

    @Override
    public String getValueString() {
      ByteBuffer valueBuffer = getValue();
      return Bytes.toStringBinary(valueBuffer.array(),
          valueBuffer.arrayOffset(), valueBuffer.limit());
    }

    private void assertValidSeek() {
      if (block == null) {
        throw new NotSeekedException();
      }
    }

    @Override
    protected ByteBuffer getFirstKeyInBlock(HFileBlock curBlock) {
      return dataBlockEncoder.getFirstKeyInBlock(getEncodedBuffer(curBlock));
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, byte[] nextIndexedKey,
        boolean rewind, byte[] key, int offset, int length, boolean seekBefore)
        throws IOException  {
      if (block == null || block.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        seeker.rewind();
      }
      this.nextIndexedKey = nextIndexedKey;
      return seeker.seekToKeyInBlock(key, offset, length, seekBefore);
    }
  }

  /**
   * Returns a buffer with the Bloom filter metadata. The caller takes
   * ownership of the buffer.
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private DataInput getBloomFilterMetadata(BlockType blockType)
  throws IOException {
    if (blockType != BlockType.GENERAL_BLOOM_META &&
        blockType != BlockType.DELETE_FAMILY_BLOOM_META) {
      throw new RuntimeException("Block Type: " + blockType.toString() +
          " is not supported") ;
    }

    for (HFileBlock b : loadOnOpenBlocks)
      if (b.getBlockType() == blockType)
        return b.getByteStream();
    return null;
  }

  @Override
  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
  }

  /**
   * Validates that the minor version is within acceptable limits.
   * Otherwise throws an Runtime exception
   */
  private void validateMinorVersion(Path path, int minorVersion) {
    if (minorVersion < MIN_MINOR_VERSION ||
        minorVersion > MAX_MINOR_VERSION) {
      String msg = "Minor version for path " + path + 
                   " is expected to be between " +
                   MIN_MINOR_VERSION + " and " + MAX_MINOR_VERSION +
                   " but is found to be " + minorVersion;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }
}
