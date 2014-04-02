/*
 * Copyright 2011 The Apache Software Foundation
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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * {@link HFile} reader for version 1. Does not support data block encoding,
 * even in cache only, i.e. HFile v1 blocks are always brought into cache
 * unencoded.
 */
public class HFileReaderV1 extends AbstractHFileReader {
  private static final Log LOG = LogFactory.getLog(HFileReaderV1.class);

  private volatile boolean fileInfoLoaded = false;

  /**
   * Opens a HFile.  You must load the index before you can
   * use it by calling {@link #loadFileInfo()}.
   *
   * @param fsdis input stream.  Caller is responsible for closing the passed
   * stream.
   * @param size Length of the stream.
   * @param cacheConf cache references and configuration

   */
  public HFileReaderV1(Path path, FixedFileTrailer trailer,
      final FSDataInputStream fsdis, final long size,
      final boolean closeIStream,
 final CacheConfig cacheConf,
      Configuration conf) {
    super(path, trailer, fsdis, size, closeIStream, cacheConf, conf);


    trailer.expectVersion(1);
    fsBlockReader = new HFileBlock.FSReaderV1(fsdis, compressAlgo, fileSize);
  }

  private byte[] readAllIndex(final FSDataInputStream in,
      final long indexOffset, final int indexSize) throws IOException {
    byte[] allIndex = new byte[indexSize];
    in.seek(indexOffset);
    IOUtils.readFully(in, allIndex, 0, allIndex.length);

    return allIndex;
  }

  /**
   * Read in the index and file info.
   *
   * @return A map of fileinfo data.
   * @see {@link Writer#appendFileInfo(byte[], byte[])}.
   * @throws IOException
   */
  @Override
  public FileInfo loadFileInfo() throws IOException {
    if (fileInfoLoaded)
      return fileInfo;

    // Read in the fileinfo and get what we need from it.
    istream.seek(trailer.getFileInfoOffset());
    fileInfo = new FileInfo();
    fileInfo.readFields(istream);
    lastKey = fileInfo.get(FileInfo.LASTKEY);
    avgKeyLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_VALUE_LEN));

    // Comparator is stored in the file info in version 1.
    String clazzName = Bytes.toString(fileInfo.get(FileInfo.COMPARATOR));
    comparator = getComparator(clazzName);

    dataBlockIndexReader =
        new HFileBlockIndex.BlockIndexReader(comparator, 1);
    metaBlockIndexReader =
        new HFileBlockIndex.BlockIndexReader(Bytes.BYTES_RAWCOMPARATOR, 1);

    int sizeToLoadOnOpen = (int) (fileSize - trailer.getLoadOnOpenDataOffset() -
        trailer.getTrailerSize());
    byte[] dataAndMetaIndex = readAllIndex(istream,
        trailer.getLoadOnOpenDataOffset(), sizeToLoadOnOpen);

    ByteArrayInputStream bis = new ByteArrayInputStream(dataAndMetaIndex);
    DataInputStream dis = new DataInputStream(bis);

    // Read in the data index.
    if (trailer.getDataIndexCount() > 0)
      BlockType.INDEX_V1.readAndCheck(dis);
    dataBlockIndexReader.readRootIndex(dis, trailer.getDataIndexCount());

    // Read in the metadata index.
    if (trailer.getMetaIndexCount() > 0)
      BlockType.INDEX_V1.readAndCheck(dis);
    metaBlockIndexReader.readRootIndex(dis, trailer.getMetaIndexCount());

    fileInfoLoaded = true;
    return fileInfo;
  }

  /**
   * Creates comparator from the given class name.
   *
   * @param clazzName the comparator class name read from the trailer
   * @return an instance of the comparator to use
   * @throws IOException in case comparator class name is invalid
   */
  @SuppressWarnings("unchecked")
  private RawComparator<byte[]> getComparator(final String clazzName)
  throws IOException {
    if (clazzName == null || clazzName.length() == 0) {
      return null;
    }
    try {
      return (RawComparator<byte[]>)Class.forName(clazzName).newInstance();
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param isCompaction is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks,
      final boolean isCompaction, boolean preloadBlocks) {
    return new ScannerV1(this, cacheBlocks, isCompaction);
  }

  /**
   * @param key Key to search.
   * @return Block number of the block containing the key or -1 if not in this
   * file.
   */
  protected int blockContainingKey(final byte[] key, int offset, int length) {
    Preconditions.checkState(!dataBlockIndexReader.isEmpty(),
        "Block index not loaded");

    return dataBlockIndexReader.rootBlockContainingKey(key, offset, length);
  }

  /**
   * @param metaBlockName
   * @param cacheBlock Add block to cache, if found
   * @return Block wrapped in a ByteBuffer
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

    byte[] nameBytes = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(nameBytes, 0,
        nameBytes.length);
    if (block == -1)
      return null;
    long offset = metaBlockIndexReader.getRootBlockOffset(block);
    long nextOffset;
    if (block == metaBlockIndexReader.getRootBlockCount() - 1) {
      nextOffset = trailer.getFileInfoOffset();
    } else {
      nextOffset = metaBlockIndexReader.getRootBlockOffset(block + 1);
    }

    long startTimeNs = System.nanoTime();

    BlockCacheKey cacheKey = new BlockCacheKey(name, offset);

    BlockCategory effectiveCategory = BlockCategory.META;
    if (metaBlockName.equals(HFileWriterV1.BLOOM_FILTER_META_KEY) ||
        metaBlockName.equals(HFileWriterV1.BLOOM_FILTER_DATA_KEY)) {
      effectiveCategory = BlockCategory.BLOOM;
    }

    // Per meta key from any given file, synchronize reads for said block
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block.  If found return.
      if (cacheConf.isBlockCacheEnabled()) {
        HFileBlock cachedBlock =
          (HFileBlock) cacheConf.getBlockCache().getBlock(cacheKey,
              cacheConf.shouldCacheBlockOnRead(effectiveCategory));
        if (cachedBlock != null) {
          getSchemaMetrics().updateOnBlockRead(effectiveCategory,
                  SchemaMetrics.NO_COMPACTION,
                  TimeUnit.NANOSECONDS.toMillis(
                          System.nanoTime() - startTimeNs), true, false, false);
          return cachedBlock.getBufferWithoutHeader();
        }
        // Cache Miss, please load.
      }

      HFileBlock hfileBlock = fsBlockReader.readBlockData(offset,
          nextOffset - offset, metaBlockIndexReader.getRootBlockDataSize(block),
          false);
      passSchemaMetricsTo(hfileBlock);
      hfileBlock.expectType(BlockType.META);

      long deltaNs = System.nanoTime() - startTimeNs;
      HFile.preadTimeNano.addAndGet(deltaNs);
      HFile.preadOps.incrementAndGet();
      HFile.preadHistogram.addValue(deltaNs);
      getSchemaMetrics().updateOnBlockRead(effectiveCategory, false,
              TimeUnit.NANOSECONDS.toMillis(deltaNs), false, false, false);

      // Cache the block
      if (cacheBlock && cacheConf.shouldCacheBlockOnRead(effectiveCategory)) {
        cacheConf.getBlockCache().cacheBlock(cacheKey, hfileBlock,
            cacheConf.isInMemory());
      }

      return hfileBlock.getBufferWithoutHeader();
    }
  }

  /**
   * Read in a file block.
   * @param block Index of block to read.
   * @param cacheBlock cache block if read from disk
   * @param isCompaction is this block being read as part of a compaction
   * @return Block wrapped in a ByteBuffer.
   * @throws IOException
   */
  ByteBuffer readBlockBuffer(int block, boolean cacheBlock,
      final boolean isCompaction, KeyValueContext kvContext)
          throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException("Block index not loaded");
    }
    if (block < 0 || block >= dataBlockIndexReader.getRootBlockCount()) {
      throw new IOException("Requested block is out of range: " + block +
        ", max: " + dataBlockIndexReader.getRootBlockCount());
    }

    long startTimeNs = System.nanoTime();

    long offset = dataBlockIndexReader.getRootBlockOffset(block);
    BlockCacheKey cacheKey = new BlockCacheKey(name, offset);

    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).
    synchronized (dataBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block.  If found return.
      if (cacheConf.isBlockCacheEnabled()) {
        HFileBlock cachedBlock =
          (HFileBlock) cacheConf.getBlockCache().getBlock(cacheKey,
              cacheConf.shouldCacheDataOnRead());
        if (cachedBlock != null) {
          cacheHits.incrementAndGet();
          if (kvContext != null) {
            kvContext.setObtainedFromCache(true);
          }
          getSchemaMetrics().updateOnBlockRead(
                  cachedBlock.getBlockType().getCategory(), isCompaction,
                  TimeUnit.NANOSECONDS.toMillis(
                          System.nanoTime() - startTimeNs), true, false, false);
          return cachedBlock.getBufferWithoutHeader();
        }
        // Carry on, please load.
      }

      // Load block from filesystem.
      long nextOffset;

      if (block == dataBlockIndexReader.getRootBlockCount() - 1) {
        // last block!  The end of data block is first meta block if there is
        // one or if there isn't, the fileinfo offset.
        nextOffset = (metaBlockIndexReader.getRootBlockCount() == 0) ?
            this.trailer.getFileInfoOffset() :
            metaBlockIndexReader.getRootBlockOffset(0);
      } else {
        nextOffset = dataBlockIndexReader.getRootBlockOffset(block + 1);
      }

      HFileBlock hfileBlock = fsBlockReader.readBlockData(offset, nextOffset
          - offset, dataBlockIndexReader.getRootBlockDataSize(block), false,
          getReadOptions(isCompaction));
      passSchemaMetricsTo(hfileBlock);
      hfileBlock.expectType(BlockType.DATA);

      long deltaNs = System.nanoTime() - startTimeNs;
      if (isCompaction) {
        HFile.preadCompactionTimeNano.addAndGet(deltaNs);
        HFile.preadCompactionHistogram.addValue(deltaNs);
        HFile.preadCompactionOps.incrementAndGet();
      } else {
        HFile.preadTimeNano.addAndGet(deltaNs);
        HFile.preadHistogram.addValue(deltaNs);
        HFile.preadOps.incrementAndGet();
      }
      getSchemaMetrics().updateOnBlockRead(
              hfileBlock.getBlockType().getCategory(), isCompaction,
              TimeUnit.NANOSECONDS.toMillis(deltaNs), false, false, false);
      if (kvContext != null) {
        kvContext.setObtainedFromCache(false);
      }
      // Cache the block
      if (cacheBlock && cacheConf.shouldCacheBlockOnRead(
          hfileBlock.getBlockType().getCategory())) {
        cacheConf.getBlockCache().cacheBlock(cacheKey, hfileBlock,
            cacheConf.isInMemory());
      }
      return hfileBlock.getBufferWithoutHeader();
    }
  }

  /**
   * @return Last key in the file.  May be null if file has no entries.
   * Note that this is not the last rowkey, but rather the byte form of
   * the last KeyValue.
   */
  public byte[] getLastKey() {
    if (!fileInfoLoaded) {
      throw new RuntimeException("Load file info first");
    }
    return dataBlockIndexReader.isEmpty() ? null : lastKey;
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   *
   * @throws IOException
   */
  @Override
  public byte[] midkey() throws IOException {
    Preconditions.checkState(isFileInfoLoaded(), "File info is not loaded");
    Preconditions.checkState(!dataBlockIndexReader.isEmpty(),
        "Data block index is not loaded or is empty");
    return dataBlockIndexReader.midkey();
  }

  @Override
  public void close() throws IOException {
    close(cacheConf.shouldEvictOnClose());
  }

  @Override
  public void close(boolean evictOnClose) throws IOException {
    if (evictOnClose && cacheConf.isBlockCacheEnabled()) {
      int numEvicted = 0;
      for (int i = 0; i < dataBlockIndexReader.getRootBlockCount(); i++) {
        BlockCacheKey key = new BlockCacheKey(name,
                dataBlockIndexReader.getRootBlockOffset(i));
        if (cacheConf.getBlockCache().evictBlock(key)) {
          numEvicted++;
        }
      }
      LOG.debug("On close of file " + name + " evicted " + numEvicted
          + " block(s) of " + dataBlockIndexReader.getRootBlockCount()
          + " total blocks");
    }
    if (this.closeIStream && this.istream != null) {
      this.istream.close();
      this.istream = null;
    }
  }

  @Override
  public void close(boolean evictL1OnClose, boolean evictL2OnClose)
    throws IOException {
    close(evictL1OnClose); // HFileReaderV1 does not support L2 cache
  }

  protected abstract static class AbstractScannerV1
      extends AbstractHFileReader.Scanner {
    protected int currBlock;

    /**
     * This masks a field with the same name in the superclass and saves us the
     * runtime overhead of casting from abstract reader to reader V1.
     */
    protected HFileReaderV1 reader;

    public AbstractScannerV1(HFileReaderV1 reader, boolean cacheBlocks,
        final boolean isCompaction) {
      super(reader, cacheBlocks, isCompaction);
      this.reader = (HFileReaderV1) reader;
    }

    /**
     * Within a loaded block, seek looking for the first key
     * that is smaller than (or equal to?) the key we are interested in.
     *
     * A note on the seekBefore - if you have seekBefore = true, AND the
     * first key in the block = key, then you'll get thrown exceptions.
     * @param key to find
     * @param seekBefore find the key before the exact match.
     * @return
     */
    protected abstract int blockSeek(byte[] key, int offset, int length,
        boolean seekBefore);

    protected abstract void loadBlock(int bloc, boolean rewind)
        throws IOException;

    @Override
    public int seekTo(byte[] key, int offset, int length) throws IOException {
      int b = reader.blockContainingKey(key, offset, length);
      if (b < 0) return -1; // falls before the beginning of the file! :-(
      // Avoid re-reading the same block (that'd be dumb).
      loadBlock(b, true);
      return blockSeek(key, offset, length, false);
    }

    @Override
    public int reseekTo(byte[] key, int offset, int length)
        throws IOException {
      if (blockBuffer != null && currKeyLen != 0) {
        ByteBuffer bb = getKey();
        int compared = reader.getComparator().compare(key, offset,
            length, bb.array(), bb.arrayOffset(), bb.limit());
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        }
      }

      int b = reader.blockContainingKey(key, offset, length);
      if (b < 0) {
        return -1;
      }
      loadBlock(b, false);
      return blockSeek(key, offset, length, false);
    }

    @Override
    public boolean seekBefore(byte[] key, int offset, int length)
        throws IOException {
      int b = reader.blockContainingKey(key, offset, length);
      if (b < 0)
        return false; // key is before the start of the file.

      // Question: does this block begin with 'key'?
      byte[] firstkKey = reader.getDataBlockIndexReader().getRootBlockKey(b);
      if (reader.getComparator().compare(firstkKey, 0, firstkKey.length,
          key, offset, length) == 0) {
        // Ok the key we're interested in is the first of the block, so go back
        // by one.
        if (b == 0) {
          // we have a 'problem', the key we want is the first of the file.
          return false;
        }
        b--;
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      loadBlock(b, true);
      blockSeek(key, offset, length, true);
      return true;
    }
    
    @Override
    public void close() {
      /*
       * Note this function is overridden as we need to have it in HFileScanner interface, so that
       * the store scanner can delegate the close operation(which is required in case of
       * block preloading enabled) to the HFileScanner it owns
       */
    }
  }

  /**
   * Implementation of {@link HFileScanner} interface.
   */

  protected static class ScannerV1 extends AbstractScannerV1 {
    private HFileReaderV1 reader;

    public ScannerV1(HFileReaderV1 reader, boolean cacheBlocks,
        final boolean isCompaction) {
      super(reader, cacheBlocks, isCompaction);
      this.reader = reader;
    }

    @Override
    public KeyValue getKeyValue() {
      if (blockBuffer == null) {
        return null;
      }
      return new KeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
          + blockBuffer.position() - 8);
    }

    @Override
    public ByteBuffer getKey() {
      Preconditions.checkState(blockBuffer != null && currKeyLen > 0,

          "you need to seekTo() before calling getKey()");

      ByteBuffer keyBuff = blockBuffer.slice();
      keyBuff.limit(currKeyLen);
      keyBuff.rewind();
      // Do keyBuff.asReadOnly()?
      return keyBuff;
    }

    @Override
    public ByteBuffer getValue() {
      if (blockBuffer == null || currKeyLen == 0) {
        throw new RuntimeException(
            "you need to seekTo() before calling getValue()");
      }

      // TODO: Could this be done with one ByteBuffer rather than create two?
      ByteBuffer valueBuff = blockBuffer.slice();
      valueBuff.position(currKeyLen);
      valueBuff = valueBuff.slice();
      valueBuff.limit(currValueLen);
      valueBuff.rewind();
      return valueBuff;
    }

    @Override
    public boolean next() throws IOException {

      if (blockBuffer == null) {
        throw new IOException("Next called on non-seeked scanner");
      }

      try {
        blockBuffer.position(blockBuffer.position() + currKeyLen
            + currValueLen);
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position() +
                  "; currKeyLen = " + currKeyLen +
                  "; currValLen = " + currValueLen +
                  "; block limit = " + blockBuffer.limit() +
                  "; HFile name = " + reader.getName() +
                  "; currBlock id = " + currBlock, e);
        throw e;
      }
      if (blockBuffer.remaining() <= 0) {

        currBlock++;
        if (currBlock >= reader.getDataBlockIndexReader().getRootBlockCount()) {
          // damn we are at the end
          currBlock = 0;
          blockBuffer = null;
          return false;
        }
        blockBuffer = reader.readBlockBuffer(currBlock, cacheBlocks, isCompaction,
            kvContext);
        currKeyLen = blockBuffer.getInt();
        currValueLen = blockBuffer.getInt();
        blockFetches++;
        return true;
      }

      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      return true;
    }

    @Override
    protected int blockSeek(byte[] key, int offset, int length,
        boolean seekBefore) {
      int klen, vlen;
      int lastLen = 0;
      do {
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        int comp = reader.getComparator().compare(key, offset, length,
            blockBuffer.array(),
            blockBuffer.arrayOffset() + blockBuffer.position(), klen);
        if (comp == 0) {
          if (seekBefore) {
            blockBuffer.position(blockBuffer.position() - lastLen - 16);
            currKeyLen = blockBuffer.getInt();
            currValueLen = blockBuffer.getInt();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          return 0; // indicate exact match
        }
        if (comp < 0) {
          // go back one key:
          blockBuffer.position(blockBuffer.position() - lastLen - 16);
          currKeyLen = blockBuffer.getInt();
          currValueLen = blockBuffer.getInt();
          return 1;
        }
        blockBuffer.position(blockBuffer.position() + klen + vlen);
        lastLen = klen + vlen;
      } while (blockBuffer.remaining() > 0);

      // ok we are at the end, so go back a littleeeeee....
      // The 8 in the below is intentionally different to the 16s in the above
      // Do the math you you'll figure it.
      blockBuffer.position(blockBuffer.position() - lastLen - 8);
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      return 1; // didn't exactly find it.
    }

    @Override
    public String getKeyString() {
      return Bytes.toStringBinary(blockBuffer.array(),
          blockBuffer.arrayOffset() + blockBuffer.position(), currKeyLen);
    }

    @Override
    public String getValueString() {
      return Bytes.toString(blockBuffer.array(), blockBuffer.arrayOffset() +
        blockBuffer.position() + currKeyLen, currValueLen);
    }

    @Override
    public boolean seekTo() throws IOException {
      if (reader.getDataBlockIndexReader().isEmpty()) {
        return false;
      }
      if (blockBuffer != null && currBlock == 0) {
        blockBuffer.rewind();
        currKeyLen = blockBuffer.getInt();
        currValueLen = blockBuffer.getInt();
        return true;
      }
      currBlock = 0;
      blockBuffer = reader.readBlockBuffer(currBlock, cacheBlocks, isCompaction,
          kvContext);
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      blockFetches++;
      return true;
    }

    @Override
    protected void loadBlock(int bloc, boolean rewind) throws IOException {
      if (blockBuffer == null) {
        blockBuffer = reader.readBlockBuffer(bloc, cacheBlocks, isCompaction,
            kvContext);
        currBlock = bloc;
        blockFetches++;
      } else {
        if (bloc != currBlock) {
          blockBuffer = reader.readBlockBuffer(bloc, cacheBlocks, isCompaction,
              kvContext);
          currBlock = bloc;
          blockFetches++;
        } else {
          // we are already in the same block, just rewind to seek again.
          if (rewind) {
            blockBuffer.rewind();
          }
          else {
            // Go back by (size of rowlength + size of valuelength) = 8 bytes
            blockBuffer.position(blockBuffer.position()-8);
          }
        }
      }
    }

    @Override
    public boolean currKeyValueObtainedFromCache() {
      return this.kvContext.getObtainedFromCache();
    }
    
  }

  @Override
  public HFileBlock readBlock(long offset, long onDiskBlockSize, boolean cacheBlock,
      boolean isCompaction, boolean cacheOnPreload, BlockType expectedBlockType,
      DataBlockEncoding expectedDataBlockEncoding, KeyValueContext kvContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    // Always cache Bloom filter blocks.
    ByteBuffer buf = getMetaBlock(HFileWriterV1.BLOOM_FILTER_META_KEY, true);
    if (buf == null)
      return null;
    ByteArrayInputStream bais = new ByteArrayInputStream(buf.array(),
        buf.arrayOffset(), buf.limit());
    return new DataInputStream(bais);
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    return null;
  }

  @Override
  public boolean isFileInfoLoaded() {
    return fileInfoLoaded;
  }

  @Override
  public DataInput getDeleteColumnBloomFilterMetadata() {
    return null;
  }

  @Override
  public DataInput getRowKeyPrefixBloomFilterMetadata() {
    return null;
  }

  @Override
  public HFileScanner getScanner(boolean cacheBlocks, boolean isCompaction) {
    return getScanner(cacheBlocks, isCompaction, false);
  }

}
