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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link HFile} reader for version 2.
 */
@InterfaceAudience.Private
public class HFileReaderV2 extends AbstractHFileReader {

  private static final Log LOG = LogFactory.getLog(HFileReaderV2.class);

  /** Minor versions in HFile V2 starting with this number have hbase checksums */
  public static final int MINOR_VERSION_WITH_CHECKSUM = 1;
  /** In HFile V2 minor version that does not support checksums */
  public static final int MINOR_VERSION_NO_CHECKSUM = 0;

  /** HFile minor version that introduced pbuf filetrailer */
  public static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  protected boolean includesMemstoreTS = false;
  protected boolean decodeMemstoreTS = false;
  protected boolean shouldIncludeMemstoreTS() {
    return includesMemstoreTS;
  }

  /** Filesystem-level block reader. */
  protected HFileBlock.FSReader fsBlockReader;

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
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Minor versions starting with this number have faked index key */
  static final int MINOR_VERSION_WITH_FAKED_KEY = 3;

  protected HFileContext hfileContext;

  /**
   * Opens a HFile. You must load the index before you can use it by calling
   * {@link #loadFileInfo()}.
   *
   * @param path Path to HFile.
   * @param trailer File trailer.
   * @param fsdis input stream.
   * @param size Length of the stream.
   * @param cacheConf Cache configuration.
   * @param hfs
   * @param conf
   */
  public HFileReaderV2(final Path path, final FixedFileTrailer trailer,
      final FSDataInputStreamWrapper fsdis, final long size, final CacheConfig cacheConf,
      final HFileSystem hfs, final Configuration conf) throws IOException {
    super(path, trailer, size, cacheConf, hfs, conf);
    this.conf = conf;
    trailer.expectMajorVersion(getMajorVersion());
    validateMinorVersion(path, trailer.getMinorVersion());
    this.hfileContext = createHFileContext(fsdis, fileSize, hfs, path, trailer);
    HFileBlock.FSReaderImpl fsBlockReaderV2 =
      new HFileBlock.FSReaderImpl(fsdis, fileSize, hfs, path, hfileContext);
    this.fsBlockReader = fsBlockReaderV2; // upcast

    // Comparator class name is stored in the trailer in version 2.
    comparator = trailer.createComparator();
    dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(comparator,
        trailer.getNumDataIndexLevels(), this);
    metaBlockIndexReader = new HFileBlockIndex.BlockIndexReader(
        KeyValue.RAW_COMPARATOR, 1);

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
    fileInfo.read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
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
    dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo);
    fsBlockReaderV2.setDataBlockEncoder(dataBlockEncoder);

    // Store all other load-on-open blocks for further consumption.
    HFileBlock b;
    while ((b = blockIter.nextBlock()) != null) {
      loadOnOpenBlocks.add(b);
    }

    // Prefetch file blocks upon open if requested
    if (cacheConf.shouldPrefetchOnOpen()) {
      PrefetchExecutor.request(path, new Runnable() {
        public void run() {
          try {
            long offset = 0;
            long end = fileSize - getTrailer().getTrailerSize();
            HFileBlock prevBlock = null;
            while (offset < end) {
              if (Thread.interrupted()) {
                break;
              }
              long onDiskSize = -1;
              if (prevBlock != null) {
                onDiskSize = prevBlock.getNextBlockOnDiskSizeWithHeader();
              }
              HFileBlock block = readBlock(offset, onDiskSize, true, false, false, false,
                null, null);
              prevBlock = block;
              offset += block.getOnDiskSizeWithHeader();
            }
          } catch (IOException e) {
            // IOExceptions are probably due to region closes (relocation, etc.)
            if (LOG.isTraceEnabled()) {
              LOG.trace("Exception encountered while prefetching " + path + ":", e);
            }
          } catch (Exception e) {
            // Other exceptions are interesting
            LOG.warn("Exception encountered while prefetching " + path + ":", e);
          } finally {
            PrefetchExecutor.complete(path);
          }
        }
      });
    }
  }

  protected HFileContext createHFileContext(FSDataInputStreamWrapper fsdis, long fileSize,
      HFileSystem hfs, Path path, FixedFileTrailer trailer) throws IOException {
    return new HFileContextBuilder()
      .withIncludesMvcc(this.includesMemstoreTS)
      .withCompression(this.compressAlgo)
      .withHBaseCheckSum(trailer.getMinorVersion() >= MINOR_VERSION_WITH_CHECKSUM)
      .build();
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
    if (dataBlockEncoder.useEncodedScanner()) {
      return new EncodedScannerV2(this, cacheBlocks, pread, isCompaction,
          hfileContext);
    }

    return new ScannerV2(this, cacheBlocks, pread, isCompaction);
  }

  /**
   * Retrieve block from cache. Validates the retrieved block's type vs {@code expectedBlockType}
   * and its encoding vs. {@code expectedDataBlockEncoding}. Unpacks the block as necessary.
   */
   private HFileBlock getCachedBlock(BlockCacheKey cacheKey, boolean cacheBlock, boolean useLock,
       boolean isCompaction, boolean updateCacheMetrics, BlockType expectedBlockType,
       DataBlockEncoding expectedDataBlockEncoding) throws IOException {
     // Check cache for block. If found return.
     if (cacheConf.isBlockCacheEnabled()) {
       BlockCache cache = cacheConf.getBlockCache();
       HFileBlock cachedBlock = (HFileBlock) cache.getBlock(cacheKey, cacheBlock, useLock,
         updateCacheMetrics);
       if (cachedBlock != null) {
         if (cacheConf.shouldCacheCompressed(cachedBlock.getBlockType().getCategory())) {
           cachedBlock = cachedBlock.unpack(hfileContext, fsBlockReader);
         }
         validateBlockType(cachedBlock, expectedBlockType);

         if (expectedDataBlockEncoding == null) {
           return cachedBlock;
         }
         DataBlockEncoding actualDataBlockEncoding =
                 cachedBlock.getDataBlockEncoding();
         // Block types other than data blocks always have
         // DataBlockEncoding.NONE. To avoid false negative cache misses, only
         // perform this check if cached block is a data block.
         if (cachedBlock.getBlockType().isData() &&
                 !actualDataBlockEncoding.equals(expectedDataBlockEncoding)) {
           // This mismatch may happen if a ScannerV2, which is used for say a
           // compaction, tries to read an encoded block from the block cache.
           // The reverse might happen when an EncodedScannerV2 tries to read
           // un-encoded blocks which were cached earlier.
           //
           // Because returning a data block with an implicit BlockType mismatch
           // will cause the requesting scanner to throw a disk read should be
           // forced here. This will potentially cause a significant number of
           // cache misses, so update so we should keep track of this as it might
           // justify the work on a CompoundScannerV2.
           if (!expectedDataBlockEncoding.equals(DataBlockEncoding.NONE) &&
                   !actualDataBlockEncoding.equals(DataBlockEncoding.NONE)) {
             // If the block is encoded but the encoding does not match the
             // expected encoding it is likely the encoding was changed but the
             // block was not yet evicted. Evictions on file close happen async
             // so blocks with the old encoding still linger in cache for some
             // period of time. This event should be rare as it only happens on
             // schema definition change.
             LOG.info("Evicting cached block with key " + cacheKey +
                     " because of a data block encoding mismatch" +
                     "; expected: " + expectedDataBlockEncoding +
                     ", actual: " + actualDataBlockEncoding);
             cache.evictBlock(cacheKey);
           }
           return null;
         }
         return cachedBlock;
       }
     }
     return null;
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
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname,
        0, mbname.length);
    if (block == -1)
      return null;
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      BlockCacheKey cacheKey = new BlockCacheKey(name, metaBlockOffset);

      cacheBlock &= cacheConf.shouldCacheDataOnRead();
      if (cacheConf.isBlockCacheEnabled()) {
        HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, false, true, true,
          BlockType.META, null);
        if (cachedBlock != null) {
          assert cachedBlock.isUnpacked() : "Packed block leak.";
          // Return a distinct 'shallow copy' of the block,
          // so pos does not get messed by the scanner
          return cachedBlock.getBufferWithoutHeader();
        }
        // Cache Miss, please load.
      }

      HFileBlock metaBlock = fsBlockReader.readBlockData(metaBlockOffset,
          blockSize, -1, true).unpack(hfileContext, fsBlockReader);

      // Cache the block
      if (cacheBlock) {
        cacheConf.getBlockCache().cacheBlock(cacheKey, metaBlock,
            cacheConf.isInMemory(), this.cacheConf.isCacheDataInL1());
      }

      return metaBlock.getBufferWithoutHeader();
    }
  }

  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize,
      final boolean cacheBlock, boolean pread, final boolean isCompaction,
      boolean updateCacheMetrics, BlockType expectedBlockType,
      DataBlockEncoding expectedDataBlockEncoding)
      throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException("Block index not loaded");
    }
    if (dataBlockOffset < 0 || dataBlockOffset >= trailer.getLoadOnOpenDataOffset()) {
      throw new IOException("Requested block is out of range: " + dataBlockOffset +
        ", lastDataBlockOffset: " + trailer.getLastDataBlockOffset());
    }

    // For any given block from any given file, synchronize reads for said block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).
    BlockCacheKey cacheKey = new BlockCacheKey(name, dataBlockOffset);
    boolean useLock = false;
    IdLock.Entry lockEntry = null;
    TraceScope traceScope = Trace.startSpan("HFileReaderV2.readBlock");
    try {
      while (true) {
        if (useLock) {
          lockEntry = offsetLock.getLockEntry(dataBlockOffset);
        }

        // Check cache for block. If found return.
        if (cacheConf.isBlockCacheEnabled()) {
          // Try and get the block from the block cache. If the useLock variable is true then this
          // is the second time through the loop and it should not be counted as a block cache miss.
          HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, useLock, isCompaction,
            updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding);
          if (cachedBlock != null) {
            if (Trace.isTracing()) {
              traceScope.getSpan().addTimelineAnnotation("blockCacheHit");
            }
            assert cachedBlock.isUnpacked() : "Packed block leak.";
            if (cachedBlock.getBlockType().isData()) {
              if (updateCacheMetrics) {
                HFile.dataBlockReadCnt.incrementAndGet();
              }
              // Validate encoding type for data blocks. We include encoding
              // type in the cache key, and we expect it to match on a cache hit.
              if (cachedBlock.getDataBlockEncoding() != dataBlockEncoder.getDataBlockEncoding()) {
                throw new IOException("Cached block under key " + cacheKey + " "
                  + "has wrong encoding: " + cachedBlock.getDataBlockEncoding() + " (expected: "
                  + dataBlockEncoder.getDataBlockEncoding() + ")");
              }
            }
            // Cache-hit. Return!
            return cachedBlock;
          }
          // Carry on, please load.
        }
        if (!useLock) {
          // check cache again with lock
          useLock = true;
          continue;
        }
        if (Trace.isTracing()) {
          traceScope.getSpan().addTimelineAnnotation("blockCacheMiss");
        }
        // Load block from filesystem.
        HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset, onDiskBlockSize, -1,
            pread);
        validateBlockType(hfileBlock, expectedBlockType);
        HFileBlock unpacked = hfileBlock.unpack(hfileContext, fsBlockReader);
        BlockType.BlockCategory category = hfileBlock.getBlockType().getCategory();

        // Cache the block if necessary
        if (cacheBlock && cacheConf.shouldCacheBlockOnRead(category)) {
          cacheConf.getBlockCache().cacheBlock(cacheKey,
            cacheConf.shouldCacheCompressed(category) ? hfileBlock : unpacked,
            cacheConf.isInMemory(), this.cacheConf.isCacheDataInL1());
        }

        if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
          HFile.dataBlockReadCnt.incrementAndGet();
        }

        return unpacked;
      }
    } finally {
      traceScope.close();
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
    if (expectedBlockType.isData() && actualBlockType.isData()) {
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
    PrefetchExecutor.cancel(path);
    if (evictOnClose && cacheConf.isBlockCacheEnabled()) {
      int numEvicted = cacheConf.getBlockCache().evictBlocksByHfileName(name);
      if (LOG.isTraceEnabled()) {
        LOG.trace("On close, file=" + name + " evicted=" + numEvicted
          + " block(s)");
      }
    }
    fsBlockReader.closeStreams();
  }

  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return dataBlockEncoder.getEffectiveEncodingInCache(isCompaction);
  }

  /** For testing */
  @Override
  HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }


  protected abstract static class AbstractScannerV2
      extends AbstractHFileReader.Scanner {
    protected HFileBlock block;

    @Override
    public Cell getNextIndexedKey() {
      return nextIndexedKey;
    }
    /**
     * The next indexed key is to keep track of the indexed key of the next data block.
     * If the nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the
     * current data block is the last data block.
     *
     * If the nextIndexedKey is null, it means the nextIndexedKey has not been loaded yet.
     */
    protected Cell nextIndexedKey;

    public AbstractScannerV2(HFileReaderV2 r, boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      super(r, cacheBlocks, pread, isCompaction);
    }

    protected abstract ByteBuffer getFirstKeyInBlock(HFileBlock curBlock);

    protected abstract int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
        boolean rewind, Cell key, boolean seekBefore) throws IOException;

    @Override
    public int seekTo(byte[] key, int offset, int length) throws IOException {
      // Always rewind to the first key of the block, because the given key
      // might be before or after the current key.
      return seekTo(new KeyValue.KeyOnlyKeyValue(key, offset, length));
    }

    @Override
    public int reseekTo(byte[] key, int offset, int length) throws IOException {
      return reseekTo(new KeyValue.KeyOnlyKeyValue(key, offset, length));
    }

    @Override
    public int seekTo(Cell key) throws IOException {
      return seekTo(key, true);
    }

    @Override
    public int reseekTo(Cell key) throws IOException {
      int compared;
      if (isSeeked()) {
        compared = compareKey(reader.getComparator(), key);
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        } else {
          // The comparison with no_next_index_key has to be checked
          if (this.nextIndexedKey != null &&
              (this.nextIndexedKey == HConstants.NO_NEXT_INDEXED_KEY || reader
              .getComparator()
                  .compareOnlyKeyPortion(key, nextIndexedKey) < 0)) {
            // The reader shall continue to scan the current data block instead
            // of querying the
            // block index as long as it knows the target key is strictly
            // smaller than
            // the next indexed key or the current data block is the last data
            // block.
            return loadBlockAndSeekToKey(this.block, nextIndexedKey, false, key, false);
          }
        }
      }
      // Don't rewind on a reseek operation, because reseek implies that we are
      // always going forward in the file.
      return seekTo(key, false);
    }


    /**
     * An internal API function. Seek to the given key, optionally rewinding to
     * the first key of the block before doing the seek.
     *
     * @param key - a cell representing the key that we need to fetch
     * @param rewind whether to rewind to the first key of the block before
     *        doing the seek. If this is false, we are assuming we never go
     *        back, otherwise the result is undefined.
     * @return -1 if the key is earlier than the first key of the file,
     *         0 if we are at the given key, 1 if we are past the given key
     *         -2 if the key is earlier than the first key of the file while
     *         using a faked index key
     * @throws IOException
     */
    public int seekTo(Cell key, boolean rewind) throws IOException {
      HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
      BlockWithScanInfo blockWithScanInfo = indexReader.loadDataBlockWithScanInfo(key, block,
          cacheBlocks, pread, isCompaction, getEffectiveDataBlockEncoding());
      if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
        // This happens if the key e.g. falls before the beginning of the file.
        return -1;
      }
      return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
          blockWithScanInfo.getNextIndexedKey(), rewind, key, false);
    }

    @Override
    public boolean seekBefore(byte[] key, int offset, int length) throws IOException {
      return seekBefore(new KeyValue.KeyOnlyKeyValue(key, offset, length));
    }

    @Override
    public boolean seekBefore(Cell key) throws IOException {
      HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, block,
          cacheBlocks, pread, isCompaction,
          ((HFileReaderV2) reader).getEffectiveEncodingInCache(isCompaction));
      if (seekToBlock == null) {
        return false;
      }
      ByteBuffer firstKey = getFirstKeyInBlock(seekToBlock);

      if (reader.getComparator()
          .compareOnlyKeyPortion(
              new KeyValue.KeyOnlyKeyValue(firstKey.array(), firstKey.arrayOffset(),
                  firstKey.limit()), key) >= 0) {
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
            pread, isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      Cell firstKeyInCurrentBlock = new KeyValue.KeyOnlyKeyValue(Bytes.getBytes(firstKey));
      loadBlockAndSeekToKey(seekToBlock, firstKeyInCurrentBlock, true, key, true);
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
            isCompaction, true, null, getEffectiveDataBlockEncoding());
      } while (!curBlock.getBlockType().isData());

      return curBlock;
    }

    public DataBlockEncoding getEffectiveDataBlockEncoding() {
      return ((HFileReaderV2)reader).getEffectiveEncodingInCache(isCompaction);
    }
    /**
     * Compare the given key against the current key
     * @param comparator
     * @param key
     * @param offset
     * @param length
     * @return -1 is the passed key is smaller than the current key, 0 if equal and 1 if greater
     */
    public abstract int compareKey(KVComparator comparator, byte[] key, int offset,
        int length);

    public abstract int compareKey(KVComparator comparator, Cell kv);
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
    public Cell getKeyValue() {
      if (!isSeeked())
        return null;

      return formNoTagsKeyValue();
    }

    protected Cell formNoTagsKeyValue() {
      NoTagsKeyValue ret = new NoTagsKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position(), getCellBufSize());
      if (this.reader.shouldIncludeMemstoreTS()) {
        ret.setSequenceId(currMemstoreTS);
      }
      return ret;
    }

    protected int getCellBufSize() {
      return KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
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
    public int compareKey(KVComparator comparator, byte[] key, int offset, int length) {
      return comparator.compareFlatKey(key, offset, length, blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen);
    }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      return ByteBuffer.wrap(
          blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position()
              + KEY_VALUE_LEN_SIZE + currKeyLen, currValueLen).slice();
    }

    protected void setNonSeekedState() {
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
        blockBuffer.position(getNextCellStartPosition());
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

    protected int getNextCellStartPosition() {
      return blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
          + currMemstoreTSLen;
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
          isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
      if (block.getOffset() < 0) {
        throw new IOException("Invalid block offset: " + block.getOffset());
      }
      updateCurrBlock(block);
      return true;
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
        boolean rewind, Cell key, boolean seekBefore) throws IOException {
      if (block == null || block.getOffset() != seekToBlock.getOffset()) {
        updateCurrBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }

      // Update the nextIndexedKey
      this.nextIndexedKey = nextIndexedKey;
      return blockSeek(key, seekBefore);
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     */
    protected void updateCurrBlock(HFileBlock newBlock) {
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

    protected void readKeyValueLen() {
      blockBuffer.mark();
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      ByteBufferUtils.skip(blockBuffer, currKeyLen + currValueLen);
      readMvccVersion();
      if (currKeyLen < 0 || currValueLen < 0
          || currKeyLen > blockBuffer.limit()
          || currValueLen > blockBuffer.limit()) {
        throw new IllegalStateException("Invalid currKeyLen " + currKeyLen
            + " or currValueLen " + currValueLen + ". Block offset: "
            + block.getOffset() + ", block length: " + blockBuffer.limit()
            + ", position: " + blockBuffer.position() + " (without header).");
      }
      blockBuffer.reset();
    }

    protected void readMvccVersion() {
      if (this.reader.shouldIncludeMemstoreTS()) {
        if (this.reader.decodeMemstoreTS) {
          try {
            currMemstoreTS = Bytes.readVLong(blockBuffer.array(), blockBuffer.arrayOffset()
                + blockBuffer.position());
            currMemstoreTSLen = WritableUtils.getVIntSize(currMemstoreTS);
          } catch (Exception e) {
            throw new RuntimeException("Error reading memstore timestamp", e);
          }
        } else {
          currMemstoreTS = 0;
          currMemstoreTSLen = 1;
        }
      }
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller than
     * (or equal to?) the key we are interested in.
     *
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
     *
     * @param key
     *          the key to find
     * @param seekBefore
     *          find the key before the given key in case of exact match.
     * @return 0 in case of an exact key match, 1 in case of an inexact match,
     *         -2 in case of an inexact match and furthermore, the input key
     *         less than the first key of current block(e.g. using a faked index
     *         key)
     */
    protected int blockSeek(Cell key, boolean seekBefore) {
      int klen, vlen;
      long memstoreTS = 0;
      int memstoreTSLen = 0;
      int lastKeyValueSize = -1;
      KeyValue.KeyOnlyKeyValue keyOnlykv = new KeyValue.KeyOnlyKeyValue();
      do {
        blockBuffer.mark();
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        blockBuffer.reset();
        if (this.reader.shouldIncludeMemstoreTS()) {
          if (this.reader.decodeMemstoreTS) {
            try {
              int memstoreTSOffset = blockBuffer.arrayOffset() + blockBuffer.position()
                  + KEY_VALUE_LEN_SIZE + klen + vlen;
              memstoreTS = Bytes.readVLong(blockBuffer.array(), memstoreTSOffset);
              memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
            } catch (Exception e) {
              throw new RuntimeException("Error reading memstore timestamp", e);
            }
          } else {
            memstoreTS = 0;
            memstoreTSLen = 1;
          }
        }

        int keyOffset = blockBuffer.arrayOffset() + blockBuffer.position() + KEY_VALUE_LEN_SIZE;
        keyOnlykv.setKey(blockBuffer.array(), keyOffset, klen);
        int comp = reader.getComparator().compareOnlyKeyPortion(key, keyOnlykv);

        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key="
                  + CellUtil.getCellKeyAsString(key)
                  + ", blockOffset=" + block.getOffset() + ", onDiskSize="
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
        } else if (comp < 0) {
          if (lastKeyValueSize > 0)
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
          readKeyValueLen();
          if (lastKeyValueSize == -1 && blockBuffer.position() == 0
              && this.reader.trailer.getMinorVersion() >= MINOR_VERSION_WITH_FAKED_KEY) {
            return HConstants.INDEX_KEY_MAGIC;
          }
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

    @Override
    public int compareKey(KVComparator comparator, Cell key) {
      return comparator.compareOnlyKeyPortion(
          key,
          new KeyValue.KeyOnlyKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
              + blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen));
    }
  }

  /**
   * ScannerV2 that operates on encoded data blocks.
   */
  protected static class EncodedScannerV2 extends AbstractScannerV2 {
    private final HFileBlockDecodingContext decodingCtx;
    private final DataBlockEncoder.EncodedSeeker seeker;
    private final DataBlockEncoder dataBlockEncoder;
    protected final HFileContext meta;

    public EncodedScannerV2(HFileReaderV2 reader, boolean cacheBlocks,
        boolean pread, boolean isCompaction, HFileContext meta) {
      super(reader, cacheBlocks, pread, isCompaction);
      DataBlockEncoding encoding = reader.dataBlockEncoder.getDataBlockEncoding();
      dataBlockEncoder = encoding.getEncoder();
      decodingCtx = dataBlockEncoder.newDataBlockDecodingContext(meta);
      seeker = dataBlockEncoder.createSeeker(
        reader.getComparator(), decodingCtx);
      this.meta = meta;
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
     * @throws CorruptHFileException
     */
    private void updateCurrentBlock(HFileBlock newBlock) throws CorruptHFileException {
      block = newBlock;

      // sanity checks
      if (block.getBlockType() != BlockType.ENCODED_DATA) {
        throw new IllegalStateException(
            "EncodedScanner works only on encoded data blocks");
      }
      short dataBlockEncoderId = block.getDataBlockEncodingId();
      if (!DataBlockEncoding.isCorrectEncoder(dataBlockEncoder, dataBlockEncoderId)) {
        String encoderCls = dataBlockEncoder.getClass().getName();
        throw new CorruptHFileException("Encoder " + encoderCls
          + " doesn't support data block encoding "
          + DataBlockEncoding.getNameFromId(dataBlockEncoderId));
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
          isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
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
    public int compareKey(KVComparator comparator, byte[] key, int offset, int length) {
      return seeker.compareKey(comparator, key, offset, length);
    }

    @Override
    public ByteBuffer getValue() {
      assertValidSeek();
      return seeker.getValueShallowCopy();
    }

    @Override
    public Cell getKeyValue() {
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
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
        boolean rewind, Cell key, boolean seekBefore) throws IOException {
      if (block == null || block.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        seeker.rewind();
      }
      this.nextIndexedKey = nextIndexedKey;
      return seeker.seekToKeyInBlock(key, seekBefore);
    }

    @Override
    public int compareKey(KVComparator comparator, Cell key) {
      return seeker.compareKey(comparator, key);
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

  @Override
  public int getMajorVersion() {
    return 2;
  }

  @Override
  public HFileContext getFileContext() {
    return hfileContext;
  }

  /**
   * Returns false if block prefetching was requested for this file and has
   * not completed, true otherwise
   */
  @VisibleForTesting
  boolean prefetchComplete() {
    return PrefetchExecutor.isCompleted(path);
  }
}
