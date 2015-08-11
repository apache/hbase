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
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ByteBufferedKeyOnlyKeyValue;
import org.apache.hadoop.hbase.OffheapKeyValue;
import org.apache.hadoop.hbase.ShareableMemory;
import org.apache.hadoop.hbase.SizeCachedKeyValue;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation that can handle all hfile versions of {@link HFile.Reader}.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class HFileReaderImpl implements HFile.Reader, Configurable {
  // This class is HFileReaderV3 + HFileReaderV2 + AbstractHFileReader all squashed together into
  // one file.  Ditto for all the HFileReader.ScannerV? implementations. I was running up against
  // the MaxInlineLevel limit because too many tiers involved reading from an hfile. Was also hard
  // to navigate the source code when so many classes participating in read.
  private static final Log LOG = LogFactory.getLog(HFileReaderImpl.class);

  /** Data block index reader keeping the root data index in memory */
  private HFileBlockIndex.CellBasedKeyBlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  private HFileBlockIndex.ByteArrayKeyBlockIndexReader metaBlockIndexReader;

  private final FixedFileTrailer trailer;

  /** Filled when we read in the trailer. */
  private final Compression.Algorithm compressAlgo;

  /**
   * What kind of data block encoding should be used while reading, writing,
   * and handling cache.
   */
  private HFileDataBlockEncoder dataBlockEncoder = NoOpDataBlockEncoder.INSTANCE;

  /** Last key in the file. Filled in when we read in the file info */
  private Cell lastKeyCell = null;

  /** Average key length read from file info */
  private int avgKeyLen = -1;

  /** Average value length read from file info */
  private int avgValueLen = -1;

  /** Key comparator */
  private CellComparator comparator = CellComparator.COMPARATOR;

  /** Size of this file. */
  private final long fileSize;

  /** Block cache configuration. */
  private final CacheConfig cacheConf;

  /** Path of file */
  private final Path path;

  /** File name to be used for block names */
  private final String name;

  private FileInfo fileInfo;

  private Configuration conf;

  private HFileContext hfileContext;

  /** Filesystem-level block reader. */
  private HFileBlock.FSReader fsBlockReader;

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

  /**
   * We can read files whose major version is v2 IFF their minor version is at least 3.
   */
  private static final int MIN_V2_MINOR_VERSION_WITH_PB = 3;

  /** Minor versions starting with this number have faked index key */
  static final int MINOR_VERSION_WITH_FAKED_KEY = 3;

  /**
   * Opens a HFile. You must load the index before you can use it by calling
   * {@link #loadFileInfo()}.
   * @param path
   *          Path to HFile.
   * @param trailer
   *          File trailer.
   * @param fsdis
   *          input stream.
   * @param fileSize
   *          Length of the stream.
   * @param cacheConf
   *          Cache configuration.
   * @param hfs
   *          The file system.
   * @param conf
   *          Configuration
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public HFileReaderImpl(final Path path, FixedFileTrailer trailer,
      final FSDataInputStreamWrapper fsdis,
      final long fileSize, final CacheConfig cacheConf, final HFileSystem hfs,
      final Configuration conf)
  throws IOException {
    this.trailer = trailer;
    this.compressAlgo = trailer.getCompressionCodec();
    this.cacheConf = cacheConf;
    this.fileSize = fileSize;
    this.path = path;
    this.name = path.getName();
    this.conf = conf;
    checkFileVersion();
    this.hfileContext = createHFileContext(fsdis, fileSize, hfs, path, trailer);
    this.fsBlockReader = new HFileBlock.FSReaderImpl(fsdis, fileSize, hfs, path, hfileContext);

    // Comparator class name is stored in the trailer in version 2.
    comparator = trailer.createComparator();
    dataBlockIndexReader = new HFileBlockIndex.CellBasedKeyBlockIndexReader(comparator,
        trailer.getNumDataIndexLevels(), this);
    metaBlockIndexReader = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);

    // Parse load-on-open data.

    HFileBlock.BlockIterator blockIter = fsBlockReader.blockRange(
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
    byte[] creationTimeBytes = fileInfo.get(FileInfo.CREATE_TIME_TS);
    this.hfileContext.setFileCreateTime(creationTimeBytes == null?  0:
        Bytes.toLong(creationTimeBytes));
    if (fileInfo.get(FileInfo.LASTKEY) != null) {
      lastKeyCell = new KeyValue.KeyOnlyKeyValue(fileInfo.get(FileInfo.LASTKEY));
    }
    avgKeyLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_VALUE_LEN));
    byte [] keyValueFormatVersion = fileInfo.get(HFileWriterImpl.KEY_VALUE_VERSION);
    includesMemstoreTS = keyValueFormatVersion != null &&
        Bytes.toInt(keyValueFormatVersion) == HFileWriterImpl.KEY_VALUE_VER_WITH_MEMSTORE;
    fsBlockReader.setIncludesMemstoreTS(includesMemstoreTS);
    if (includesMemstoreTS) {
      decodeMemstoreTS = Bytes.toLong(fileInfo.get(HFileWriterImpl.MAX_MEMSTORE_TS_KEY)) > 0;
    }

    // Read data block encoding algorithm name from file info.
    dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo);
    fsBlockReader.setDataBlockEncoder(dataBlockEncoder);

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
              // Need not update the current block. Ideally here the readBlock won't find the
              // block in cache. We call this readBlock so that block data is read from FS and
              // cached in BC. So there is no reference count increment that happens here.
              // The return will ideally be a noop because the block is not of MemoryType SHARED.
              returnBlock(block);
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

    byte[] tmp = fileInfo.get(FileInfo.MAX_TAGS_LEN);
    // max tag length is not present in the HFile means tags were not at all written to file.
    if (tmp != null) {
      hfileContext.setIncludesTags(true);
      tmp = fileInfo.get(FileInfo.TAGS_COMPRESSED);
      if (tmp != null && Bytes.toBoolean(tmp)) {
        hfileContext.setCompressTags(true);
      }
    }
  }

  /**
   * File version check is a little sloppy. We read v3 files but can also read v2 files if their
   * content has been pb'd; files written with 0.98.
   */
  private void checkFileVersion() {
    int majorVersion = trailer.getMajorVersion();
    if (majorVersion == getMajorVersion()) return;
    int minorVersion = trailer.getMinorVersion();
    if (majorVersion == 2 && minorVersion >= MIN_V2_MINOR_VERSION_WITH_PB) return;
    // We can read v3 or v2 versions of hfile.
    throw new IllegalArgumentException("Invalid HFile version: major=" +
      trailer.getMajorVersion() + ", minor=" + trailer.getMinorVersion() + ": expected at least " +
      "major=2 and minor=" + MAX_MINOR_VERSION);
  }

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException extends IllegalStateException {
    public BlockIndexNotLoadedException() {
      // Add a message in case anyone relies on it as opposed to class name.
      super("Block index not loaded");
    }
  }

  private String toStringFirstKey() {
    if(getFirstKey() == null)
      return null;
    return CellUtil.getCellKeyAsString(getFirstKey());
  }

  private String toStringLastKey() {
    return CellUtil.toString(getLastKey(), false);
  }

  @Override
  public String toString() {
    return "reader=" + path.toString() +
        (!isFileInfoLoaded()? "":
          ", compression=" + compressAlgo.getName() +
          ", cacheConf=" + cacheConf +
          ", firstKey=" + toStringFirstKey() +
          ", lastKey=" + toStringLastKey()) +
          ", avgKeyLen=" + avgKeyLen +
          ", avgValueLen=" + avgValueLen +
          ", entries=" + trailer.getEntryCount() +
          ", length=" + fileSize;
  }

  @Override
  public long length() {
    return fileSize;
  }

  @Override
  public void returnBlock(HFileBlock block) {
    BlockCache blockCache = this.cacheConf.getBlockCache();
    if (blockCache != null && block != null) {
      BlockCacheKey cacheKey = new BlockCacheKey(this.getFileContext().getHFileName(),
          block.getOffset());
      blockCache.returnBlock(cacheKey, block);
    }
  }
  /**
   * @return the first key in the file. May be null if file has no entries. Note
   *         that this is not the first row key, but rather the byte form of the
   *         first KeyValue.
   */
  @Override
  public Cell getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException();
    }
    return dataBlockIndexReader.isEmpty() ? null
        : dataBlockIndexReader.getRootBlockKey(0);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's
   * patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public byte[] getFirstRowKey() {
    Cell firstKey = getFirstKey();
    // We have to copy the row part to form the row key alone
    return firstKey == null? null: CellUtil.cloneRow(firstKey);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after
   * Ryan's patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public byte[] getLastRowKey() {
    Cell lastKey = getLastKey();
    return lastKey == null? null: CellUtil.cloneRow(lastKey);
  }

  /** @return number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** @return comparator */
  @Override
  public CellComparator getComparator() {
    return comparator;
  }

  /** @return compression algorithm */
  @Override
  public Compression.Algorithm getCompressionAlgorithm() {
    return compressAlgo;
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does
   *         not take into account non-root blocks of a multilevel data index.
   */
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
        + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize()
            : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public HFileBlockIndex.BlockIndexReader getDataBlockIndexReader() {
    return dataBlockIndexReader;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public FileInfo loadFileInfo() throws IOException {
    return fileInfo;
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked
   * is invoked on a scanner that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException() {
      super("Not seeked to a key/value");
    }
  }

  protected static class HFileScannerImpl implements HFileScanner {
    private ByteBuff blockBuffer;
    protected final boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;
    private int currKeyLen;
    private int currValueLen;
    private int currMemstoreTSLen;
    private long currMemstoreTS;
    // Updated but never read?
    protected volatile int blockFetches;
    protected final HFile.Reader reader;
    private int currTagsLen;
    // buffer backed keyonlyKV
    private ByteBufferedKeyOnlyKeyValue bufBackedKeyOnlyKv = new ByteBufferedKeyOnlyKeyValue();
    // A pair for reusing in blockSeek() so that we don't garbage lot of objects
    final ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<ByteBuffer>();

    /**
     * The next indexed key is to keep track of the indexed key of the next data block.
     * If the nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the
     * current data block is the last data block.
     *
     * If the nextIndexedKey is null, it means the nextIndexedKey has not been loaded yet.
     */
    protected Cell nextIndexedKey;
    // Current block being used
    protected HFileBlock curBlock;
    // Previous blocks that were used in the course of the read
    protected final ArrayList<HFileBlock> prevBlocks = new ArrayList<HFileBlock>();

    public HFileScannerImpl(final HFile.Reader reader, final boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      this.reader = reader;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    void updateCurrBlockRef(HFileBlock block) {
      if (block != null && this.curBlock != null &&
          block.getOffset() == this.curBlock.getOffset()) {
        return;
      }
      if (this.curBlock != null) {
        prevBlocks.add(this.curBlock);
      }
      this.curBlock = block;
    }

    void reset() {
      if (this.curBlock != null) {
        this.prevBlocks.add(this.curBlock);
      }
      this.curBlock = null;
    }

    private void returnBlockToCache(HFileBlock block) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Returning the block : " + block);
      }
      this.reader.returnBlock(block);
    }

    private void returnBlocks(boolean returnAll) {
      for (int i = 0; i < this.prevBlocks.size(); i++) {
        returnBlockToCache(this.prevBlocks.get(i));
      }
      this.prevBlocks.clear();
      if (returnAll && this.curBlock != null) {
        returnBlockToCache(this.curBlock);
        this.curBlock = null;
      }
    }
    @Override
    public boolean isSeeked(){
      return blockBuffer != null;
    }

    @Override
    public String toString() {
      return "HFileScanner for reader " + String.valueOf(getReader());
    }

    protected void assertSeeked() {
      if (!isSeeked())
        throw new NotSeekedException();
    }

    @Override
    public HFile.Reader getReader() {
      return reader;
    }

    protected int getCellBufSize() {
      int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
      if (this.reader.getFileContext().isIncludesTags()) {
        kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return kvBufSize;
    }

    @Override
    public void close() {
      this.returnBlocks(true);
    }

    protected int getCurCellSize() {
      int curCellSize =  KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
          + currMemstoreTSLen;
      if (this.reader.getFileContext().isIncludesTags()) {
        curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return curCellSize;
    }

    protected void readKeyValueLen() {
      // This is a hot method. We go out of our way to make this method short so it can be
      // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
      // because it is faster than going via range-checked ByteBuffer methods or going through a
      // byte buffer array a byte at a time.
      // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
      // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
      // Trying to imitate what was done - need to profile if this is better or
      // earlier way is better by doing mark and reset?
      // But ensure that you read long instead of two ints
      long ll = blockBuffer.getLongAfterPosition(0);
      // Read top half as an int of key length and bottom int as value length
      this.currKeyLen = (int)(ll >> Integer.SIZE);
      this.currValueLen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
      checkKeyValueLen();
      // Move position past the key and value lengths and then beyond the key and value
      int p = (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
      if (reader.getFileContext().isIncludesTags()) {
        // Tags length is a short.
        this.currTagsLen = blockBuffer.getShortAfterPosition(p);
        checkTagsLen();
        p += (Bytes.SIZEOF_SHORT + currTagsLen);
      }
      readMvccVersion(p);
    }

    private final void checkTagsLen() {
      if (checkLen(this.currTagsLen)) {
        throw new IllegalStateException("Invalid currTagsLen " + this.currTagsLen +
          ". Block offset: " + curBlock.getOffset() + ", block length: " +
            this.blockBuffer.limit() +
          ", position: " + this.blockBuffer.position() + " (without header).");
      }
    }

    /**
     * Read mvcc. Does checks to see if we even need to read the mvcc at all.
     * @param offsetFromPos
     */
    protected void readMvccVersion(final int offsetFromPos) {
      // See if we even need to decode mvcc.
      if (!this.reader.shouldIncludeMemstoreTS()) return;
      if (!this.reader.isDecodeMemstoreTS()) {
        currMemstoreTS = 0;
        currMemstoreTSLen = 1;
        return;
      }
      _readMvccVersion(offsetFromPos);
    }

    /**
     * Actually do the mvcc read. Does no checks.
     * @param offsetFromPos
     */
    private void _readMvccVersion(int offsetFromPos) {
      // This is Bytes#bytesToVint inlined so can save a few instructions in this hot method; i.e.
      // previous if one-byte vint, we'd redo the vint call to find int size.
      // Also the method is kept small so can be inlined.
      byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        this.currMemstoreTS = firstByte;
      } else {
        int remaining = len -1;
        long i = 0;
        offsetFromPos++;
        if (remaining >= Bytes.SIZEOF_INT) {
          i = blockBuffer.getIntAfterPosition(offsetFromPos);
          remaining -= Bytes.SIZEOF_INT;
          offsetFromPos += Bytes.SIZEOF_INT;
        }
        if (remaining >= Bytes.SIZEOF_SHORT) {
          short s = blockBuffer.getShortAfterPosition(offsetFromPos);
          i = i << 16;
          i = i | (s & 0xFFFF);
          remaining -= Bytes.SIZEOF_SHORT;
          offsetFromPos += Bytes.SIZEOF_SHORT;
        }
        for (int idx = 0; idx < remaining; idx++) {
          byte b = blockBuffer.getByteAfterPosition(offsetFromPos + idx);
          i = i << 8;
          i = i | (b & 0xFF);
        }
        currMemstoreTS = (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
      }
      this.currMemstoreTSLen = len;
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller than
     * (or equal to?) the key we are interested in.
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
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
      int klen, vlen, tlen = 0;
      int lastKeyValueSize = -1;
      int offsetFromPos;
      do {
        offsetFromPos = 0;
        // Better to ensure that we use the BB Utils here
        long ll = blockBuffer.getLongAfterPosition(offsetFromPos);
        klen = (int)(ll >> Integer.SIZE);
        vlen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
        if (klen < 0 || vlen < 0 || klen > blockBuffer.limit()
            || vlen > blockBuffer.limit()) {
          throw new IllegalStateException("Invalid klen " + klen + " or vlen "
              + vlen + ". Block offset: "
              + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
              + blockBuffer.position() + " (without header).");
        }
        offsetFromPos += Bytes.SIZEOF_LONG;
        blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, klen, pair);
        bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), klen);
        int comp = reader.getComparator().compareKeyIgnoresMvcc(key, bufBackedKeyOnlyKv);
        offsetFromPos += klen + vlen;
        if (this.reader.getFileContext().isIncludesTags()) {
          // Read short as unsigned, high byte first
          tlen = ((blockBuffer.getByteAfterPosition(offsetFromPos) & 0xff) << 8)
              ^ (blockBuffer.getByteAfterPosition(offsetFromPos + 1) & 0xff);
          if (tlen < 0 || tlen > blockBuffer.limit()) {
            throw new IllegalStateException("Invalid tlen " + tlen + ". Block offset: "
                + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
                + blockBuffer.position() + " (without header).");
          }
          // add the two bytes read for the tags.
          offsetFromPos += tlen + (Bytes.SIZEOF_SHORT);
        }
        if (this.reader.shouldIncludeMemstoreTS()) {
          // Directly read the mvcc based on current position
          readMvccVersion(offsetFromPos);
        }
        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key=" + CellUtil.getCellKeyAsString(key)
                  + ", blockOffset=" + curBlock.getOffset() + ", onDiskSize="
                  + curBlock.getOnDiskSizeWithHeader());
            }
            blockBuffer.moveBack(lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          currTagsLen = tlen;
          return 0; // indicate exact match
        } else if (comp < 0) {
          if (lastKeyValueSize > 0) {
            blockBuffer.moveBack(lastKeyValueSize);
          }
          readKeyValueLen();
          if (lastKeyValueSize == -1 && blockBuffer.position() == 0) {
            return HConstants.INDEX_KEY_MAGIC;
          }
          return 1;
        }
        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + currMemstoreTSLen + KEY_VALUE_LEN_SIZE;
        // include tag length also if tags included with KV
        if (reader.getFileContext().isIncludesTags()) {
          lastKeyValueSize += tlen + Bytes.SIZEOF_SHORT;
        }
        blockBuffer.skip(lastKeyValueSize);
      } while (blockBuffer.hasRemaining());

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.moveBack(lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

    @Override
    public Cell getNextIndexedKey() {
      return nextIndexedKey;
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
              .getComparator().compareKeyIgnoresMvcc(key, nextIndexedKey) < 0)) {
            // The reader shall continue to scan the current data block instead
            // of querying the
            // block index as long as it knows the target key is strictly
            // smaller than
            // the next indexed key or the current data block is the last data
            // block.
            return loadBlockAndSeekToKey(this.curBlock, nextIndexedKey, false, key,
                false);
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
      BlockWithScanInfo blockWithScanInfo = indexReader.loadDataBlockWithScanInfo(key, curBlock,
          cacheBlocks, pread, isCompaction, getEffectiveDataBlockEncoding());
      if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
        // This happens if the key e.g. falls before the beginning of the
        // file.
        return -1;
      }
      return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
          blockWithScanInfo.getNextIndexedKey(), rewind, key, false);
    }

    @Override
    public boolean seekBefore(Cell key) throws IOException {
      HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, curBlock,
          cacheBlocks, pread, isCompaction, reader.getEffectiveEncodingInCache(isCompaction));
      if (seekToBlock == null) {
        return false;
      }
      Cell firstKey = getFirstKeyCellInBlock(seekToBlock);
      if (reader.getComparator()
           .compareKeyIgnoresMvcc(firstKey, key) >= 0) {
        long previousBlockOffset = seekToBlock.getPrevBlockOffset();
        // The key we are interested in
        if (previousBlockOffset == -1) {
          // we have a 'problem', the key we want is the first of the file.
          return false;
        }

        // The first key in the current block 'seekToBlock' is greater than the given 
        // seekBefore key. We will go ahead by reading the next block that satisfies the
        // given key. Return the current block before reading the next one.
        reader.returnBlock(seekToBlock);
        // It is important that we compute and pass onDiskSize to the block
        // reader so that it does not have to read the header separately to
        // figure out the size.
        seekToBlock = reader.readBlock(previousBlockOffset,
            seekToBlock.getOffset() - previousBlockOffset, cacheBlocks,
            pread, isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      loadBlockAndSeekToKey(seekToBlock, firstKey, true, key, true);
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
      if (curBlock == null)
        return null;

      HFileBlock block = this.curBlock;

      do {
        if (block.getOffset() >= lastDataBlockOffset)
          return null;

        if (block.getOffset() < 0) {
          throw new IOException("Invalid block file offset: " + block);
        }

        // We are reading the next block without block type validation, because
        // it might turn out to be a non-data block.
        block = reader.readBlock(block.getOffset()
            + block.getOnDiskSizeWithHeader(),
            block.getNextBlockOnDiskSizeWithHeader(), cacheBlocks, pread,
            isCompaction, true, null, getEffectiveDataBlockEncoding());
        if (block != null && !block.getBlockType().isData()) {
          // Whatever block we read we will be returning it unless
          // it is a datablock. Just in case the blocks are non data blocks
          reader.returnBlock(block);
        }
      } while (!block.getBlockType().isData());

      return block;
    }

    public DataBlockEncoding getEffectiveDataBlockEncoding() {
      return this.reader.getEffectiveEncodingInCache(isCompaction);
    }

    @Override
    public Cell getCell() {
      if (!isSeeked())
        return null;

      Cell ret;
      int cellBufSize = getCellBufSize();
      long seqId = 0l;
      if (this.reader.shouldIncludeMemstoreTS()) {
        seqId = currMemstoreTS;
      }
      if (blockBuffer.hasArray()) {
        // TODO : reduce the varieties of KV here. Check if based on a boolean
        // we can handle the 'no tags' case.
        if (currTagsLen > 0) {
          if (this.curBlock.getMemoryType() == MemoryType.SHARED) {
            ret = new ShareableMemoryKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
              + blockBuffer.position(), getCellBufSize(), seqId);
          } else {
            ret = new SizeCachedKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
                    + blockBuffer.position(), cellBufSize, seqId);
          }
        } else {
          if (this.curBlock.getMemoryType() == MemoryType.SHARED) {
            ret = new ShareableMemoryNoTagsKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
                    + blockBuffer.position(), getCellBufSize(), seqId);
          } else {
            ret = new SizeCachedNoTagsKeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
                    + blockBuffer.position(), cellBufSize, seqId);
          }
        }
      } else {
        ByteBuffer buf = blockBuffer.asSubByteBuffer(cellBufSize);
        if (this.curBlock.getMemoryType() == MemoryType.SHARED) {
          ret = new ShareableMemoryOffheapKeyValue(buf, buf.position(), cellBufSize,
            currTagsLen > 0, seqId);
        } else {
          ret = new OffheapKeyValue(buf, buf.position(), cellBufSize, currTagsLen > 0, seqId);
        }
      }
      return ret;
    }

    @Override
    public Cell getKey() {
      assertSeeked();
      // Create a new object so that this getKey is cached as firstKey, lastKey
      ObjectIntPair<ByteBuffer> keyPair = new ObjectIntPair<ByteBuffer>();
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, keyPair);
      ByteBuffer keyBuf = keyPair.getFirst();
      if (keyBuf.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuf.array(), keyBuf.arrayOffset()
            + keyPair.getSecond(), currKeyLen);
      } else {
        return new ByteBufferedKeyOnlyKeyValue(keyBuf, keyPair.getSecond(), currKeyLen);
      }
    }

    private static class ShareableMemoryKeyValue extends SizeCachedKeyValue implements
        ShareableMemory {
      public ShareableMemoryKeyValue(byte[] bytes, int offset, int length, long seqId) {
        super(bytes, offset, length, seqId);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
        return new SizeCachedKeyValue(copy, 0, copy.length, getSequenceId());
      }
    }

    private static class ShareableMemoryNoTagsKeyValue extends SizeCachedNoTagsKeyValue implements
        ShareableMemory {
      public ShareableMemoryNoTagsKeyValue(byte[] bytes, int offset, int length, long seqId) {
        super(bytes, offset, length, seqId);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
        return new SizeCachedNoTagsKeyValue(copy, 0, copy.length, getSequenceId());
      }
    }

    private static class ShareableMemoryOffheapKeyValue extends OffheapKeyValue implements
        ShareableMemory {
      public ShareableMemoryOffheapKeyValue(ByteBuffer buf, int offset, int length,
          boolean hasTags, long seqId) {
        super(buf, offset, length, hasTags, seqId);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = new byte[this.length];
        ByteBufferUtils.copyFromBufferToArray(copy, this.buf, this.offset, 0, this.length);
        return new SizeCachedKeyValue(copy, 0, copy.length, getSequenceId());
      }
    }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      // Okie to create new Pair. Not used in hot path
      ObjectIntPair<ByteBuffer> valuePair = new ObjectIntPair<ByteBuffer>();
      this.blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
        currValueLen, valuePair);
      ByteBuffer valBuf = valuePair.getFirst().duplicate();
      valBuf.position(valuePair.getSecond());
      valBuf.limit(currValueLen + valuePair.getSecond());
      return valBuf.slice();
    }

    protected void setNonSeekedState() {
      reset();
      blockBuffer = null;
      currKeyLen = 0;
      currValueLen = 0;
      currMemstoreTS = 0;
      currMemstoreTSLen = 0;
      currTagsLen = 0;
    }

    /**
     * Set the position on current backing blockBuffer.
     */
    private void positionThisBlockBuffer() {
      try {
        blockBuffer.skip(getCurCellSize());
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position()
            + "; currKeyLen = " + currKeyLen + "; currValLen = "
            + currValueLen + "; block limit = " + blockBuffer.limit()
            + "; HFile name = " + reader.getName()
            + "; currBlock currBlockOffset = " + this.curBlock.getOffset());
        throw e;
      }
    }

    /**
     * Set our selves up for the next 'next' invocation, set up next block.
     * @return True is more to read else false if at the end.
     * @throws IOException
     */
    private boolean positionForNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (this.curBlock.getOffset() >= lastDataBlockOffset) {
        setNonSeekedState();
        return false;
      }
      return isNextBlock();
    }


    private boolean isNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      HFileBlock nextBlock = readNextDataBlock();
      if (nextBlock == null) {
        setNonSeekedState();
        return false;
      }
      updateCurrentBlock(nextBlock);
      return true;
    }

    private final boolean _next() throws IOException {
      // Small method so can be inlined. It is a hot one.
      if (blockBuffer.remaining() <= 0) {
        return positionForNextBlock();
      }

      // We are still in the same block.
      readKeyValueLen();
      return true;
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
      // This is a hot method so extreme measures taken to ensure it is small and inlineable.
      // Checked by setting: -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation
      assertSeeked();
      positionThisBlockBuffer();
      return _next();
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

      long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();
      if (curBlock != null
          && curBlock.getOffset() == firstDataBlockOffset) {
        return processFirstDataBlock();
      }

      readAndUpdateNewBlock(firstDataBlockOffset);
      return true;
    }

    protected boolean processFirstDataBlock() throws IOException{
      blockBuffer.rewind();
      readKeyValueLen();
      return true;
    }

    protected void readAndUpdateNewBlock(long firstDataBlockOffset) throws IOException,
        CorruptHFileException {
      HFileBlock newBlock = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
          isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
      if (newBlock.getOffset() < 0) {
        throw new IOException("Invalid block offset: " + newBlock.getOffset());
      }
      updateCurrentBlock(newBlock);
    }

    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
        boolean rewind, Cell key, boolean seekBefore) throws IOException {
      if (this.curBlock == null
          || this.curBlock.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }

      // Update the nextIndexedKey
      this.nextIndexedKey = nextIndexedKey;
      return blockSeek(key, seekBefore);
    }

    /**
     * @param v
     * @return True if v &lt; 0 or v &gt; current block buffer limit.
     */
    protected final boolean checkLen(final int v) {
      return v < 0 || v > this.blockBuffer.limit();
    }

    /**
     * Check key and value lengths are wholesome.
     */
    protected final void checkKeyValueLen() {
      if (checkLen(this.currKeyLen) || checkLen(this.currValueLen)) {
        throw new IllegalStateException("Invalid currKeyLen " + this.currKeyLen
            + " or currValueLen " + this.currValueLen + ". Block offset: "
            + this.curBlock.getOffset() + ", block length: "
            + this.blockBuffer.limit() + ", position: " + this.blockBuffer.position()
            + " (without header).");
      }
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     */
    protected void updateCurrentBlock(HFileBlock newBlock) throws IOException {
      // Set the active block on the reader
      // sanity check
      if (newBlock.getBlockType() != BlockType.DATA) {
        throw new IllegalStateException("ScannerV2 works only on data " + "blocks, got "
            + newBlock.getBlockType() + "; " + "fileName=" + reader.getName()
            + ", " + "dataBlockEncoder=" + reader.getDataBlockEncoding() + ", " + "isCompaction="
            + isCompaction);
      }

      updateCurrBlockRef(newBlock);
      blockBuffer = newBlock.getBufferWithoutHeader();
      readKeyValueLen();
      blockFetches++;

      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
      ByteBuff buffer = curBlock.getBufferWithoutHeader();
      // It is safe to manipulate this buffer because we own the buffer object.
      buffer.rewind();
      int klen = buffer.getInt();
      buffer.skip(Bytes.SIZEOF_INT);// Skip value len part
      ByteBuffer keyBuff = buffer.asSubByteBuffer(klen);
      if (keyBuff.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuff.array(), keyBuff.arrayOffset()
            + keyBuff.position(), klen);
      } else {
        return new ByteBufferedKeyOnlyKeyValue(keyBuff, keyBuff.position(), klen);
      }
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), false);
    }

    @Override
    public String getValueString() {
      return ByteBufferUtils.toStringBinary(getValue());
    }

    public int compareKey(CellComparator comparator, Cell key) {
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, pair);
      this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), currKeyLen);
      return comparator.compareKeyIgnoresMvcc(key, this.bufBackedKeyOnlyKv);
    }

    @Override
    public void shipped() throws IOException {
      this.returnBlocks(false);
    }
  }

  public Path getPath() {
    return path;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return dataBlockEncoder.getDataBlockEncoding();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** Minor versions in HFile starting with this number have hbase checksums */
  public static final int MINOR_VERSION_WITH_CHECKSUM = 1;
  /** In HFile minor version that does not support checksums */
  public static final int MINOR_VERSION_NO_CHECKSUM = 0;

  /** HFile minor version that introduced pbuf filetrailer */
  public static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  private boolean includesMemstoreTS = false;
  protected boolean decodeMemstoreTS = false;


  public boolean isDecodeMemstoreTS() {
    return this.decodeMemstoreTS;
  }

  public boolean shouldIncludeMemstoreTS() {
    return includesMemstoreTS;
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
           HFileBlock compressedBlock = cachedBlock;
           cachedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
           // In case of compressed block after unpacking we can return the compressed block
          if (compressedBlock != cachedBlock) {
            cache.returnBlock(cacheKey, compressedBlock);
          }
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
           // This mismatch may happen if a Scanner, which is used for say a
           // compaction, tries to read an encoded block from the block cache.
           // The reverse might happen when an EncodedScanner tries to read
           // un-encoded blocks which were cached earlier.
           //
           // Because returning a data block with an implicit BlockType mismatch
           // will cause the requesting scanner to throw a disk read should be
           // forced here. This will potentially cause a significant number of
           // cache misses, so update so we should keep track of this as it might
           // justify the work on a CompoundScanner.
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
             // This is an error scenario. so here we need to decrement the
             // count.
             cache.returnBlock(cacheKey, cachedBlock);
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
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock)
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
    synchronized (metaBlockIndexReader
        .getRootBlockKey(block)) {
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
          return cachedBlock;
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

      return metaBlock;
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
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    BlockCacheKey cacheKey = new BlockCacheKey(name, dataBlockOffset);

    boolean useLock = false;
    IdLock.Entry lockEntry = null;
    TraceScope traceScope = Trace.startSpan("HFileReaderImpl.readBlock");
    try {
      while (true) {
        // Check cache for block. If found return.
        if (cacheConf.shouldReadBlockFromCache(expectedBlockType)) {
          if (useLock) {
            lockEntry = offsetLock.getLockEntry(dataBlockOffset);
          }
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

          if (!useLock && cacheBlock && cacheConf.shouldLockOnCacheMiss(expectedBlockType)) {
            // check cache again with lock
            useLock = true;
            continue;
          }
          // Carry on, please load.
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
   * @return Last key as cell in the file. May be null if file has no entries. Note that
   *         this is not the last row key, but it is the Cell representation of the last
   *         key
   */
  @Override
  public Cell getLastKey() {
    return dataBlockIndexReader.isEmpty() ? null : lastKeyCell;
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   * @throws IOException
   */
  @Override
  public Cell midkey() throws IOException {
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
  public HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }

  /**
   * Scanner that operates on encoded data blocks.
   */
  protected static class EncodedScanner extends HFileScannerImpl {
    private final HFileBlockDecodingContext decodingCtx;
    private final DataBlockEncoder.EncodedSeeker seeker;
    private final DataBlockEncoder dataBlockEncoder;

    public EncodedScanner(HFile.Reader reader, boolean cacheBlocks,
        boolean pread, boolean isCompaction, HFileContext meta) {
      super(reader, cacheBlocks, pread, isCompaction);
      DataBlockEncoding encoding = reader.getDataBlockEncoding();
      dataBlockEncoder = encoding.getEncoder();
      decodingCtx = dataBlockEncoder.newDataBlockDecodingContext(meta);
      seeker = dataBlockEncoder.createSeeker(
        reader.getComparator(), decodingCtx);
    }

    @Override
    public boolean isSeeked(){
      return curBlock != null;
    }

    public void setNonSeekedState() {
      reset();
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to
     * the the first key/value pair.
     *
     * @param newBlock the block to make current
     * @throws CorruptHFileException
     */
    @Override
    protected void updateCurrentBlock(HFileBlock newBlock) throws CorruptHFileException {

      // sanity checks
      if (newBlock.getBlockType() != BlockType.ENCODED_DATA) {
        throw new IllegalStateException("EncodedScanner works only on encoded data blocks");
      }
      short dataBlockEncoderId = newBlock.getDataBlockEncodingId();
      if (!DataBlockEncoding.isCorrectEncoder(dataBlockEncoder, dataBlockEncoderId)) {
        String encoderCls = dataBlockEncoder.getClass().getName();
        throw new CorruptHFileException("Encoder " + encoderCls
          + " doesn't support data block encoding "
          + DataBlockEncoding.getNameFromId(dataBlockEncoderId));
      }
      updateCurrBlockRef(newBlock);
      ByteBuff encodedBuffer = getEncodedBuffer(newBlock);
      seeker.setCurrentBuffer(encodedBuffer);
      blockFetches++;

      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    private ByteBuff getEncodedBuffer(HFileBlock newBlock) {
      ByteBuff origBlock = newBlock.getBufferReadOnly();
      int pos = newBlock.headerSize() + DataBlockEncoding.ID_SIZE;
      origBlock.position(pos);
      origBlock
          .limit(pos + newBlock.getUncompressedSizeWithoutHeader() - DataBlockEncoding.ID_SIZE);
      return origBlock.slice();
    }

    @Override
    protected boolean processFirstDataBlock() throws IOException {
      seeker.rewind();
      return true;
    }

    @Override
    public boolean next() throws IOException {
      boolean isValid = seeker.next();
      if (!isValid) {
        HFileBlock newBlock = readNextDataBlock();
        isValid = newBlock != null;
        if (isValid) {
          updateCurrentBlock(newBlock);
        } else {
          setNonSeekedState();
        }
      }
      return isValid;
    }

    @Override
    public Cell getKey() {
      assertValidSeek();
      return seeker.getKey();
    }

    @Override
    public ByteBuffer getValue() {
      assertValidSeek();
      return seeker.getValueShallowCopy();
    }

    @Override
    public Cell getCell() {
      if (this.curBlock == null) {
        return null;
      }
      return seeker.getCell();
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), true);
    }

    @Override
    public String getValueString() {
      ByteBuffer valueBuffer = getValue();
      return ByteBufferUtils.toStringBinary(valueBuffer);
    }

    private void assertValidSeek() {
      if (this.curBlock == null) {
        throw new NotSeekedException();
      }
    }

    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
      return dataBlockEncoder.getFirstKeyCellInBlock(getEncodedBuffer(curBlock));
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
        boolean rewind, Cell key, boolean seekBefore) throws IOException {
      if (this.curBlock == null
          || this.curBlock.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        seeker.rewind();
      }
      this.nextIndexedKey = nextIndexedKey;
      return seeker.seekToKeyInBlock(key, seekBefore);
    }

    public int compareKey(CellComparator comparator, Cell key) {
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

  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
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
  public boolean prefetchComplete() {
    return PrefetchExecutor.isCompleted(path);
  }

  protected HFileContext createHFileContext(FSDataInputStreamWrapper fsdis, long fileSize,
      HFileSystem hfs, Path path, FixedFileTrailer trailer) throws IOException {
    HFileContextBuilder builder = new HFileContextBuilder()
      .withIncludesMvcc(shouldIncludeMemstoreTS())
      .withHBaseCheckSum(true)
      .withHFileName(this.getName())
      .withCompression(this.compressAlgo);

    // Check for any key material available
    byte[] keyBytes = trailer.getEncryptionKey();
    if (keyBytes != null) {
      Encryption.Context cryptoContext = Encryption.newContext(conf);
      Key key;
      String masterKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
        User.getCurrent().getShortName());
      try {
        // First try the master key
        key = EncryptionUtil.unwrapKey(conf, masterKeyName, keyBytes);
      } catch (KeyException e) {
        // If the current master key fails to unwrap, try the alternate, if
        // one is configured
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
        }
        String alternateKeyName =
          conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
        if (alternateKeyName != null) {
          try {
            key = EncryptionUtil.unwrapKey(conf, alternateKeyName, keyBytes);
          } catch (KeyException ex) {
            throw new IOException(ex);
          }
        } else {
          throw new IOException(e);
        }
      }
      // Use the algorithm the key wants
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
      if (cipher == null) {
        throw new IOException("Cipher '" + key.getAlgorithm() + "' is not available");
      }
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      builder.withEncryptionContext(cryptoContext);
    }

    HFileContext context = builder.build();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Reader" + (path != null? " for " + path: "") +
        " initialized with cacheConf: " + cacheConf +
        " comparator: " + comparator.getClass().getSimpleName() +
        " fileContext: " + context);
    }

    return context;
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient. NOTE: Do not use this overload of getScanner for
   * compactions. See {@link #getScanner(boolean, boolean, boolean)}
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   * @param cacheBlocks
   *          True if we should cache blocks read in by this scanner.
   * @param pread
   *          Use positional read rather than seek+read if true (pread is better
   *          for random reads, seek+read is better scanning).
   * @param isCompaction
   *          is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
      final boolean isCompaction) {
    if (dataBlockEncoder.useEncodedScanner()) {
      return new EncodedScanner(this, cacheBlocks, pread, isCompaction, this.hfileContext);
    }
    return new HFileScannerImpl(this, cacheBlocks, pread, isCompaction);
  }

  public int getMajorVersion() {
    return 3;
  }
}
