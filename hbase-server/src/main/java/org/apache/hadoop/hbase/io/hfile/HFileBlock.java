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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.ByteBufferWriterDataOutputStream;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.IOUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Reads {@link HFile} version 2 blocks to HFiles and via {@link Cacheable} Interface to caches.
 * Version 2 was introduced in hbase-0.92.0. No longer has support for version 1 blocks since
 * hbase-1.3.0.
 *
 * <p>Version 1 was the original file block. Version 2 was introduced when we changed the hbase file
 * format to support multi-level block indexes and compound bloom filters (HBASE-3857).
 *
 * <h3>HFileBlock: Version 2</h3>
 * In version 2, a block is structured as follows:
 * <ul>
 * <li><b>Header:</b> See Writer#putHeader() for where header is written; header total size is
 * HFILEBLOCK_HEADER_SIZE
 * <ul>
 * <li>0. blockType: Magic record identifying the {@link BlockType} (8 bytes):
 * e.g. <code>DATABLK*</code>
 * <li>1. onDiskSizeWithoutHeader: Compressed -- a.k.a 'on disk' -- block size, excluding header,
 * but including tailing checksum bytes (4 bytes)
 * <li>2. uncompressedSizeWithoutHeader: Uncompressed block size, excluding header, and excluding
 * checksum bytes (4 bytes)
 * <li>3. prevBlockOffset: The offset of the previous block of the same type (8 bytes). This is
 * used to navigate to the previous block without having to go to the block index
 * <li>4: For minorVersions &gt;=1, the ordinal describing checksum type (1 byte)
 * <li>5: For minorVersions &gt;=1, the number of data bytes/checksum chunk (4 bytes)
 * <li>6: onDiskDataSizeWithHeader: For minorVersions &gt;=1, the size of data 'on disk', including
 * header, excluding checksums (4 bytes)
 * </ul>
 * </li>
 * <li><b>Raw/Compressed/Encrypted/Encoded data:</b> The compression
 * algorithm is the same for all the blocks in an {@link HFile}. If compression is NONE, this is
 * just raw, serialized Cells.
 * <li><b>Tail:</b> For minorVersions &gt;=1, a series of 4 byte checksums, one each for
 * the number of bytes specified by bytesPerChecksum.
 * </ul>
 *
 * <h3>Caching</h3>
 * Caches cache whole blocks with trailing checksums if any. We then tag on some metadata, the
 * content of BLOCK_METADATA_SPACE which will be flag on if we are doing 'hbase'
 * checksums and then the offset into the file which is needed when we re-make a cache key
 * when we return the block to the cache as 'done'. See {@link Cacheable#serialize(ByteBuffer)} and
 * {@link Cacheable#getDeserializer()}.
 *
 * <p>TODO: Should we cache the checksums? Down in Writer#getBlockForCaching(CacheConfig) where
 * we make a block to cache-on-write, there is an attempt at turning off checksums. This is not the
 * only place we get blocks to cache. We also will cache the raw return from an hdfs read. In this
 * case, the checksums may be present. If the cache is backed by something that doesn't do ECC,
 * say an SSD, we might want to preserve checksums. For now this is open question.
 * <p>TODO: Over in BucketCache, we save a block allocation by doing a custom serialization.
 * Be sure to change it if serialization changes in here. Could we add a method here that takes an
 * IOEngine and that then serializes to it rather than expose our internals over in BucketCache?
 * IOEngine is in the bucket subpackage. Pull it up? Then this class knows about bucketcache. Ugh.
 */
@InterfaceAudience.Private
public class HFileBlock implements Cacheable {
  private static final Log LOG = LogFactory.getLog(HFileBlock.class);

  /** Type of block. Header field 0. */
  private BlockType blockType;

  /**
   * Size on disk excluding header, including checksum. Header field 1.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int onDiskSizeWithoutHeader;

  /**
   * Size of pure data. Does not include header or checksums. Header field 2.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int uncompressedSizeWithoutHeader;

  /**
   * The offset of the previous block on disk. Header field 3.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private long prevBlockOffset;

  /**
   * Size on disk of header + data. Excludes checksum. Header field 6,
   * OR calculated from {@link #onDiskSizeWithoutHeader} when using HDFS checksum.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int onDiskDataSizeWithHeader;


  /**
   * The in-memory representation of the hfile block. Can be on or offheap. Can be backed by
   * a single ByteBuffer or by many. Make no assumptions.
   *
   * <p>Be careful reading from this <code>buf</code>. Duplicate and work on the duplicate or if
   * not, be sure to reset position and limit else trouble down the road.
   *
   * <p>TODO: Make this read-only once made.
   *
   * <p>We are using the ByteBuff type. ByteBuffer is not extensible yet we need to be able to have
   * a ByteBuffer-like API across multiple ByteBuffers reading from a cache such as BucketCache.
   * So, we have this ByteBuff type. Unfortunately, it is spread all about HFileBlock. Would be
   * good if could be confined to cache-use only but hard-to-do.
   */
  private ByteBuff buf;

  /** Meta data that holds meta information on the hfileblock.
   */
  private HFileContext fileContext;

  /**
   * The offset of this block in the file. Populated by the reader for
   * convenience of access. This offset is not part of the block header.
   */
  private long offset = UNSET;

  private MemoryType memType = MemoryType.EXCLUSIVE;

  /**
   * The on-disk size of the next block, including the header and checksums if present, obtained by
   * peeking into the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the next block's
   * header, or UNSET if unknown.
   *
   * Blocks try to carry the size of the next block to read in this data member. They will even have
   * this value when served from cache. Could save a seek in the case where we are iterating through
   * a file and some of the blocks come from cache. If from cache, then having this info to hand
   * will save us doing a seek to read the header so we can read the body of a block.
   * TODO: see how effective this is at saving seeks.
   */
  private int nextBlockOnDiskSize = UNSET;

  /**
   * On a checksum failure, do these many succeeding read requests using hdfs checksums before
   * auto-reenabling hbase checksum verification.
   */
  static final int CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD = 3;

  private static int UNSET = -1;
  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  // How to get the estimate correctly? if it is a singleBB?
  public static final int MULTI_BYTE_BUFFER_HEAP_SIZE =
      (int)ClassSize.estimateBase(MultiByteBuff.class, false);

  /**
   * Space for metadata on a block that gets stored along with the block when we cache it.
   * There are a few bytes stuck on the end of the HFileBlock that we pull in from HDFS (note,
   * when we read from HDFS, we pull in an HFileBlock AND the header of the next block if one).
   * 8 bytes are offset of this block (long) in the file. Offset is important because
   * used when we remake the CacheKey when we return the block to cache when done. There is also
   * a flag on whether checksumming is being done by hbase or not. See class comment for note on
   * uncertain state of checksumming of blocks that come out of cache (should we or should we not?).
   * Finally there 4 bytes to hold the length of the next block which can save a seek on occasion.
   * <p>This EXTRA came in with original commit of the bucketcache, HBASE-7404. Was formerly
   * known as EXTRA_SERIALIZATION_SPACE.
   */
  static final int BLOCK_METADATA_SPACE = Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  static final byte[] DUMMY_HEADER_NO_CHECKSUM =
      new byte[HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM];

  /**
   * Used deserializing blocks from Cache.
   *
   * <code>
   * ++++++++++++++
   * + HFileBlock +
   * ++++++++++++++
   * + Checksums  + <= Optional
   * ++++++++++++++
   * + Metadata!  +
   * ++++++++++++++
   * </code>
   * @see #serialize(ByteBuffer)
   */
  static final CacheableDeserializer<Cacheable> BLOCK_DESERIALIZER =
      new CacheableDeserializer<Cacheable>() {
        public HFileBlock deserialize(ByteBuff buf, boolean reuse, MemoryType memType)
        throws IOException {
          // The buf has the file block followed by block metadata.
          // Set limit to just before the BLOCK_METADATA_SPACE then rewind.
          buf.limit(buf.limit() - BLOCK_METADATA_SPACE).rewind();
          // Get a new buffer to pass the HFileBlock for it to 'own'.
          ByteBuff newByteBuff;
          if (reuse) {
            newByteBuff = buf.slice();
          } else {
            int len = buf.limit();
            newByteBuff = new SingleByteBuff(ByteBuffer.allocate(len));
            newByteBuff.put(0, buf, buf.position(), len);
          }
          // Read out the BLOCK_METADATA_SPACE content and shove into our HFileBlock.
          buf.position(buf.limit());
          buf.limit(buf.limit() + HFileBlock.BLOCK_METADATA_SPACE);
          boolean usesChecksum = buf.get() == (byte)1;
          long offset = buf.getLong();
          int nextBlockOnDiskSize = buf.getInt();
          HFileBlock hFileBlock =
              new HFileBlock(newByteBuff, usesChecksum, memType, offset, nextBlockOnDiskSize, null);
          return hFileBlock;
        }

        @Override
        public int getDeserialiserIdentifier() {
          return DESERIALIZER_IDENTIFIER;
        }

        @Override
        public HFileBlock deserialize(ByteBuff b) throws IOException {
          // Used only in tests
          return deserialize(b, false, MemoryType.EXCLUSIVE);
        }
      };

  private static final int DESERIALIZER_IDENTIFIER;
  static {
    DESERIALIZER_IDENTIFIER =
        CacheableDeserializerIdManager.registerDeserializer(BLOCK_DESERIALIZER);
  }

  // Todo: encapsulate Header related logic in this inner class.
  static class Header {
    // Format of header is:
    // 8 bytes - block magic
    // 4 bytes int - onDiskSizeWithoutHeader
    // 4 bytes int - uncompressedSizeWithoutHeader
    // 8 bytes long - prevBlockOffset
    // The following 3 are only present if header contains checksum information
    // 1 byte - checksum type
    // 4 byte int - bytes per checksum
    // 4 byte int - onDiskDataSizeWithHeader
    static int BLOCK_MAGIC_INDEX = 0;
    static int ON_DISK_SIZE_WITHOUT_HEADER_INDEX = 8;
    static int UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX = 12;
    static int PREV_BLOCK_OFFSET_INDEX = 16;
    static int CHECKSUM_TYPE_INDEX = 24;
    static int BYTES_PER_CHECKSUM_INDEX = 25;
    static int ON_DISK_DATA_SIZE_WITH_HEADER_INDEX = 29;
  }

  /**
   * Copy constructor. Creates a shallow copy of {@code that}'s buffer.
   */
  private HFileBlock(HFileBlock that) {
    this(that, false);
  }

  /**
   * Copy constructor. Creates a shallow/deep copy of {@code that}'s buffer as per the boolean
   * param.
   */
  private HFileBlock(HFileBlock that,boolean bufCopy) {
    this.blockType = that.blockType;
    this.onDiskSizeWithoutHeader = that.onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = that.uncompressedSizeWithoutHeader;
    this.prevBlockOffset = that.prevBlockOffset;
    if (bufCopy) {
      this.buf = new SingleByteBuff(ByteBuffer.wrap(that.buf.toBytes(0, that.buf.limit())));
    } else {
      this.buf = that.buf.duplicate();
    }
    this.offset = that.offset;
    this.onDiskDataSizeWithHeader = that.onDiskDataSizeWithHeader;
    this.fileContext = that.fileContext;
    this.nextBlockOnDiskSize = that.nextBlockOnDiskSize;
  }

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor
   * is used when the block data has already been read and uncompressed,
   * and is sitting in a byte buffer and we want to stuff the block into cache.
   * See {@link Writer#getBlockForCaching(CacheConfig)}.
   *
   * <p>TODO: The caller presumes no checksumming
   * required of this block instance since going into cache; checksum already verified on
   * underlying block data pulled in from filesystem. Is that correct? What if cache is SSD?
   *
   * @param blockType the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader see {@link #onDiskSizeWithoutHeader}
   * @param uncompressedSizeWithoutHeader see {@link #uncompressedSizeWithoutHeader}
   * @param prevBlockOffset see {@link #prevBlockOffset}
   * @param b block header ({@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes) followed by
   *          uncompressed data.
   * @param fillHeader when true, write the first 4 header fields into passed buffer.
   * @param offset the file offset the block was read from
   * @param onDiskDataSizeWithHeader see {@link #onDiskDataSizeWithHeader}
   * @param fileContext HFile meta data
   */
  HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
      long prevBlockOffset, ByteBuffer b, boolean fillHeader, long offset,
      final int nextBlockOnDiskSize, int onDiskDataSizeWithHeader, HFileContext fileContext) {
    init(blockType, onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader,
        prevBlockOffset, offset, onDiskDataSizeWithHeader, nextBlockOnDiskSize, fileContext);
    this.buf = new SingleByteBuff(b);
    if (fillHeader) {
      overwriteHeader();
    }
    this.buf.rewind();
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   * @param buf Has header, content, and trailing checksums if present.
   */
  HFileBlock(ByteBuff buf, boolean usesHBaseChecksum, MemoryType memType, final long offset,
      final int nextBlockOnDiskSize, HFileContext fileContext) throws IOException {
    buf.rewind();
    final BlockType blockType = BlockType.read(buf);
    final int onDiskSizeWithoutHeader = buf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    final int uncompressedSizeWithoutHeader =
        buf.getInt(Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
    final long prevBlockOffset = buf.getLong(Header.PREV_BLOCK_OFFSET_INDEX);
    // This constructor is called when we deserialize a block from cache and when we read a block in
    // from the fs. fileCache is null when deserialized from cache so need to make up one.
    HFileContextBuilder fileContextBuilder = fileContext != null?
        new HFileContextBuilder(fileContext): new HFileContextBuilder();
    fileContextBuilder.withHBaseCheckSum(usesHBaseChecksum);
    int onDiskDataSizeWithHeader;
    if (usesHBaseChecksum) {
      byte checksumType = buf.get(Header.CHECKSUM_TYPE_INDEX);
      int bytesPerChecksum = buf.getInt(Header.BYTES_PER_CHECKSUM_INDEX);
      onDiskDataSizeWithHeader = buf.getInt(Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
      // Use the checksum type and bytes per checksum from header, not from filecontext.
      fileContextBuilder.withChecksumType(ChecksumType.codeToType(checksumType));
      fileContextBuilder.withBytesPerCheckSum(bytesPerChecksum);
    } else {
      fileContextBuilder.withChecksumType(ChecksumType.NULL);
      fileContextBuilder.withBytesPerCheckSum(0);
      // Need to fix onDiskDataSizeWithHeader; there are not checksums after-block-data
      onDiskDataSizeWithHeader = onDiskSizeWithoutHeader + headerSize(usesHBaseChecksum);
    }
    fileContext = fileContextBuilder.build();
    assert usesHBaseChecksum == fileContext.isUseHBaseChecksum();
    init(blockType, onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader,
        prevBlockOffset, offset, onDiskDataSizeWithHeader, nextBlockOnDiskSize, fileContext);
    this.memType = memType;
    this.offset = offset;
    this.buf = buf;
    this.buf.rewind();
  }

  /**
   * Called from constructors.
   */
  private void init(BlockType blockType, int onDiskSizeWithoutHeader,
      int uncompressedSizeWithoutHeader, long prevBlockOffset,
      long offset, int onDiskDataSizeWithHeader, final int nextBlockOnDiskSize,
      HFileContext fileContext) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.offset = offset;
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    this.fileContext = fileContext;
  }

  /**
   * Parse total ondisk size including header and checksum.
   * @param headerBuf Header ByteBuffer. Presumed exact size of header.
   * @param verifyChecksum true if checksum verification is in use.
   * @return Size of the block with header included.
   */
  private static int getOnDiskSizeWithHeader(final ByteBuffer headerBuf, boolean verifyChecksum) {
    return headerBuf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX) +
      headerSize(verifyChecksum);
  }

  /**
   * @return the on-disk size of the next block (including the header size and any checksums if
   * present) read by peeking into the next block's header; use as a hint when doing
   * a read of the next block when scanning or running over a file.
   */
  public int getNextBlockOnDiskSize() {
    return nextBlockOnDiskSize;
  }

  public BlockType getBlockType() {
    return blockType;
  }

  /** @return get data block encoding id that was used to encode this block */
  public short getDataBlockEncodingId() {
    if (blockType != BlockType.ENCODED_DATA) {
      throw new IllegalArgumentException("Querying encoder ID of a block " +
          "of type other than " + BlockType.ENCODED_DATA + ": " + blockType);
    }
    return buf.getShort(headerSize());
  }

  /**
   * @return the on-disk size of header + data part + checksum.
   */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + headerSize();
  }

  /**
   * @return the on-disk size of the data part + checksum (header excluded).
   */
  int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /**
   * @return the uncompressed size of data part (header and checksum excluded).
   */
   int getUncompressedSizeWithoutHeader() {
    return uncompressedSizeWithoutHeader;
  }

  /**
   * @return the offset of the previous block of the same type in the file, or
   *         -1 if unknown
   */
  long getPrevBlockOffset() {
    return prevBlockOffset;
  }

  /**
   * Rewinds {@code buf} and writes first 4 header fields. {@code buf} position
   * is modified as side-effect.
   */
  private void overwriteHeader() {
    buf.rewind();
    blockType.write(buf);
    buf.putInt(onDiskSizeWithoutHeader);
    buf.putInt(uncompressedSizeWithoutHeader);
    buf.putLong(prevBlockOffset);
    if (this.fileContext.isUseHBaseChecksum()) {
      buf.put(fileContext.getChecksumType().getCode());
      buf.putInt(fileContext.getBytesPerChecksum());
      buf.putInt(onDiskDataSizeWithHeader);
    }
  }

  /**
   * Returns a buffer that does not include the header or checksum.
   *
   * @return the buffer with header skipped and checksum omitted.
   */
  public ByteBuff getBufferWithoutHeader() {
    ByteBuff dup = getBufferReadOnly();
    // Now set it up so Buffer spans content only -- no header or no checksums.
    return dup.position(headerSize()).limit(buf.limit() - totalChecksumBytes()).slice();
  }

  /**
   * Returns a read-only duplicate of the buffer this block stores internally ready to be read.
   * Clients must not modify the buffer object though they may set position and limit on the
   * returned buffer since we pass back a duplicate. This method has to be public because it is used
   * in {@link CompoundBloomFilter} to avoid object creation on every Bloom
   * filter lookup, but has to be used with caution. Buffer holds header, block content,
   * and any follow-on checksums if present.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuff getBufferReadOnly() {
    // TODO: ByteBuf does not support asReadOnlyBuffer(). Fix.
    ByteBuff dup = this.buf.duplicate();
    assert dup.position() == 0;
    return dup;
  }

  private void sanityCheckAssertion(long valueFromBuf, long valueFromField,
      String fieldName) throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new AssertionError(fieldName + " in the buffer (" + valueFromBuf
          + ") is different from that in the field (" + valueFromField + ")");
    }
  }

  private void sanityCheckAssertion(BlockType valueFromBuf, BlockType valueFromField)
      throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new IOException("Block type stored in the buffer: " +
        valueFromBuf + ", block type field: " + valueFromField);
    }
  }

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the buffer contain a
   * valid header consistent with the fields. Assumes a packed block structure.
   * This function is primary for testing and debugging, and is not
   * thread-safe, because it alters the internal buffer pointer.
   * Used by tests only.
   */
  @VisibleForTesting
  void sanityCheck() throws IOException {
    // Duplicate so no side-effects
    ByteBuff dup = this.buf.duplicate().rewind();
    sanityCheckAssertion(BlockType.read(dup), blockType);

    sanityCheckAssertion(dup.getInt(), onDiskSizeWithoutHeader, "onDiskSizeWithoutHeader");

    sanityCheckAssertion(dup.getInt(), uncompressedSizeWithoutHeader,
        "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(dup.getLong(), prevBlockOffset, "prevBlockOffset");
    if (this.fileContext.isUseHBaseChecksum()) {
      sanityCheckAssertion(dup.get(), this.fileContext.getChecksumType().getCode(), "checksumType");
      sanityCheckAssertion(dup.getInt(), this.fileContext.getBytesPerChecksum(),
          "bytesPerChecksum");
      sanityCheckAssertion(dup.getInt(), onDiskDataSizeWithHeader, "onDiskDataSizeWithHeader");
    }

    int cksumBytes = totalChecksumBytes();
    int expectedBufLimit = onDiskDataSizeWithHeader + cksumBytes;
    if (dup.limit() != expectedBufLimit) {
      throw new AssertionError("Expected limit " + expectedBufLimit + ", got " + dup.limit());
    }

    // We might optionally allocate HFILEBLOCK_HEADER_SIZE more bytes to read the next
    // block's header, so there are two sensible values for buffer capacity.
    int hdrSize = headerSize();
    if (dup.capacity() != expectedBufLimit && dup.capacity() != expectedBufLimit + hdrSize) {
      throw new AssertionError("Invalid buffer capacity: " + dup.capacity() +
          ", expected " + expectedBufLimit + " or " + (expectedBufLimit + hdrSize));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
      .append("[")
      .append("blockType=").append(blockType)
      .append(", fileOffset=").append(offset)
      .append(", headerSize=").append(headerSize())
      .append(", onDiskSizeWithoutHeader=").append(onDiskSizeWithoutHeader)
      .append(", uncompressedSizeWithoutHeader=").append(uncompressedSizeWithoutHeader)
      .append(", prevBlockOffset=").append(prevBlockOffset)
      .append(", isUseHBaseChecksum=").append(fileContext.isUseHBaseChecksum());
    if (fileContext.isUseHBaseChecksum()) {
      sb.append(", checksumType=").append(ChecksumType.codeToType(this.buf.get(24)))
        .append(", bytesPerChecksum=").append(this.buf.getInt(24 + 1))
        .append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader);
    } else {
      sb.append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader)
        .append("(").append(onDiskSizeWithoutHeader)
        .append("+").append(HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM).append(")");
    }
    String dataBegin = null;
    if (buf.hasArray()) {
      dataBegin = Bytes.toStringBinary(buf.array(), buf.arrayOffset() + headerSize(),
          Math.min(32, buf.limit() - buf.arrayOffset() - headerSize()));
    } else {
      ByteBuff bufWithoutHeader = getBufferWithoutHeader();
      byte[] dataBeginBytes = new byte[Math.min(32,
          bufWithoutHeader.limit() - bufWithoutHeader.position())];
      bufWithoutHeader.get(dataBeginBytes);
      dataBegin = Bytes.toStringBinary(dataBeginBytes);
    }
    sb.append(", getOnDiskSizeWithHeader=").append(getOnDiskSizeWithHeader())
      .append(", totalChecksumBytes=").append(totalChecksumBytes())
      .append(", isUnpacked=").append(isUnpacked())
      .append(", buf=[").append(buf).append("]")
      .append(", dataBeginsWith=").append(dataBegin)
      .append(", fileContext=").append(fileContext)
      .append("]");
    return sb.toString();
  }

  /**
   * Retrieves the decompressed/decrypted view of this block. An encoded block remains in its
   * encoded structure. Internal structures are shared between instances where applicable.
   */
  HFileBlock unpack(HFileContext fileContext, FSReader reader) throws IOException {
    if (!fileContext.isCompressedOrEncrypted()) {
      // TODO: cannot use our own fileContext here because HFileBlock(ByteBuffer, boolean),
      // which is used for block serialization to L2 cache, does not preserve encoding and
      // encryption details.
      return this;
    }

    HFileBlock unpacked = new HFileBlock(this);
    unpacked.allocateBuffer(); // allocates space for the decompressed block

    HFileBlockDecodingContext ctx = blockType == BlockType.ENCODED_DATA ?
      reader.getBlockDecodingContext() : reader.getDefaultBlockDecodingContext();

    ByteBuff dup = this.buf.duplicate();
    dup.position(this.headerSize());
    dup = dup.slice();
    ctx.prepareDecoding(unpacked.getOnDiskSizeWithoutHeader(),
      unpacked.getUncompressedSizeWithoutHeader(), unpacked.getBufferWithoutHeader(),
      dup);
    return unpacked;
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes
   * from the existing buffer. Does not change header fields.
   * Reserve room to keep checksum bytes too.
   */
  private void allocateBuffer() {
    int cksumBytes = totalChecksumBytes();
    int headerSize = headerSize();
    int capacityNeeded = headerSize + uncompressedSizeWithoutHeader + cksumBytes;

    // TODO we need consider allocating offheap here?
    ByteBuffer newBuf = ByteBuffer.allocate(capacityNeeded);

    // Copy header bytes into newBuf.
    // newBuf is HBB so no issue in calling array()
    buf.position(0);
    buf.get(newBuf.array(), newBuf.arrayOffset(), headerSize);

    buf = new SingleByteBuff(newBuf);
    // set limit to exclude next block's header
    buf.limit(headerSize + uncompressedSizeWithoutHeader + cksumBytes);
  }

  /**
   * Return true when this block's buffer has been unpacked, false otherwise. Note this is a
   * calculated heuristic, not tracked attribute of the block.
   */
  public boolean isUnpacked() {
    final int cksumBytes = totalChecksumBytes();
    final int headerSize = headerSize();
    final int expectedCapacity = headerSize + uncompressedSizeWithoutHeader + cksumBytes;
    final int bufCapacity = buf.capacity();
    return bufCapacity == expectedCapacity || bufCapacity == expectedCapacity + headerSize;
  }

  /** An additional sanity-check in case no compression or encryption is being used. */
  public void sanityCheckUncompressedSize() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader + totalChecksumBytes()) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader
          + ", numChecksumbytes=" + totalChecksumBytes());
    }
  }

  /**
   * Cannot be {@link #UNSET}. Must be a legitimate value. Used re-making the {@link CacheKey} when
   * block is returned to the cache.
   * @return the offset of this block in the file it was read from
   */
  long getOffset() {
    if (offset < 0) {
      throw new IllegalStateException("HFile block offset not initialized properly");
    }
    return offset;
  }

  /**
   * @return a byte stream reading the data + checksum of this block
   */
  DataInputStream getByteStream() {
    ByteBuff dup = this.buf.duplicate();
    dup.position(this.headerSize());
    return new DataInputStream(new ByteBuffInputStream(dup));
  }

  @Override
  public long heapSize() {
    long size = ClassSize.align(
        ClassSize.OBJECT +
        // Block type, multi byte buffer, MemoryType and meta references
        4 * ClassSize.REFERENCE +
        // On-disk size, uncompressed size, and next block's on-disk size
        // bytePerChecksum and onDiskDataSize
        4 * Bytes.SIZEOF_INT +
        // This and previous block offset
        2 * Bytes.SIZEOF_LONG +
        // Heap size of the meta object. meta will be always not null.
        fileContext.heapSize()
    );

    if (buf != null) {
      // Deep overhead of the byte buffer. Needs to be aligned separately.
      size += ClassSize.align(buf.capacity() + MULTI_BYTE_BUFFER_HEAP_SIZE);
    }

    return ClassSize.align(size);
  }

  /**
   * Read from an input stream at least <code>necessaryLen</code> and if possible,
   * <code>extraLen</code> also if available. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but specifies a
   * number of "extra" bytes to also optionally read.
   *
   * @param in the input stream to read from
   * @param buf the buffer to read into
   * @param bufOffset the destination offset in the buffer
   * @param necessaryLen the number of bytes that are absolutely necessary to read
   * @param extraLen the number of extra bytes that would be nice to read
   * @return true if succeeded reading the extra bytes
   * @throws IOException if failed to read the necessary bytes
   */
  static boolean readWithExtra(InputStream in, byte[] buf,
      int bufOffset, int necessaryLen, int extraLen) throws IOException {
    int bytesRemaining = necessaryLen + extraLen;
    while (bytesRemaining > 0) {
      int ret = in.read(buf, bufOffset, bytesRemaining);
      if (ret == -1 && bytesRemaining <= extraLen) {
        // We could not read the "extra data", but that is OK.
        break;
      }
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream (read "
            + "returned " + ret + ", was trying to read " + necessaryLen
            + " necessary bytes and " + extraLen + " extra bytes, "
            + "successfully read "
            + (necessaryLen + extraLen - bytesRemaining));
      }
      bufOffset += ret;
      bytesRemaining -= ret;
    }
    return bytesRemaining <= 0;
  }

  /**
   * Read from an input stream at least <code>necessaryLen</code> and if possible,
   * <code>extraLen</code> also if available. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but uses
   * positional read and specifies a number of "extra" bytes that would be
   * desirable but not absolutely necessary to read.
   *
   * @param in the input stream to read from
   * @param position the position within the stream from which to start reading
   * @param buf the buffer to read into
   * @param bufOffset the destination offset in the buffer
   * @param necessaryLen the number of bytes that are absolutely necessary to
   *     read
   * @param extraLen the number of extra bytes that would be nice to read
   * @return true if and only if extraLen is > 0 and reading those extra bytes
   *     was successful
   * @throws IOException if failed to read the necessary bytes
   */
  @VisibleForTesting
  static boolean positionalReadWithExtra(FSDataInputStream in,
      long position, byte[] buf, int bufOffset, int necessaryLen, int extraLen)
      throws IOException {
    int bytesRemaining = necessaryLen + extraLen;
    int bytesRead = 0;
    while (bytesRead < necessaryLen) {
      int ret = in.read(position, buf, bufOffset, bytesRemaining);
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream (positional read "
            + "returned " + ret + ", was trying to read " + necessaryLen
            + " necessary bytes and " + extraLen + " extra bytes, "
            + "successfully read " + bytesRead);
      }
      position += ret;
      bufOffset += ret;
      bytesRemaining -= ret;
      bytesRead += ret;
    }
    return bytesRead != necessaryLen && bytesRemaining <= 0;
  }

  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern
   * is as follows:
   * <ol>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression algorithm.
   * <li>Call {@link Writer#startWriting} and get a data stream to write to.
   * <li>Write your data into the stream.
   * <li>Call Writer#writeHeaderAndData(FSDataOutputStream) as many times as you need to.
   * store the serialized block into an external stream.
   * <li>Repeat to write more blocks.
   * </ol>
   * <p>
   */
  static class Writer {
    private enum State {
      INIT,
      WRITING,
      BLOCK_READY
    };

    /** Writer state. Used to ensure the correct usage protocol. */
    private State state = State.INIT;

    /** Data block encoder used for data blocks */
    private final HFileDataBlockEncoder dataBlockEncoder;

    private HFileBlockEncodingContext dataBlockEncodingCtx;

    /** block encoding context for non-data blocks*/
    private HFileBlockDefaultEncodingContext defaultBlockEncodingCtx;

    /**
     * The stream we use to accumulate data into a block in an uncompressed format.
     * We reset this stream at the end of each block and reuse it. The
     * header is written as the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes into this
     * stream.
     */
    private ByteArrayOutputStream baosInMemory;

    /**
     * Current block type. Set in {@link #startWriting(BlockType)}. Could be
     * changed in {@link #finishBlock()} from {@link BlockType#DATA}
     * to {@link BlockType#ENCODED_DATA}.
     */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to, which compresses them and
     * writes them to {@link #baosInMemory}.
     */
    private DataOutputStream userDataStream;

    // Size of actual data being written. Not considering the block encoding/compression. This
    // includes the header size also.
    private int unencodedDataSizeWritten;

    /**
     * Bytes to be written to the file system, including the header. Compressed
     * if compression is turned on. It also includes the checksum data that
     * immediately follows the block data. (header + data + checksums)
     */
    private byte[] onDiskBlockBytesWithHeader;

    /**
     * The size of the checksum data on disk. It is used only if data is
     * not compressed. If data is compressed, then the checksums are already
     * part of onDiskBytesWithHeader. If data is uncompressed, then this
     * variable stores the checksum data for this block.
     */
    private byte[] onDiskChecksum = HConstants.EMPTY_BYTE_ARRAY;

    /**
     * Valid in the READY state. Contains the header and the uncompressed (but
     * potentially encoded, if this is a data block) bytes, so the length is
     * {@link #uncompressedSizeWithoutHeader} +
     * {@link org.apache.hadoop.hbase.HConstants#HFILEBLOCK_HEADER_SIZE}.
     * Does not store checksums.
     */
    private byte[] uncompressedBlockBytesWithHeader;

    /**
     * Current block's start offset in the {@link HFile}. Set in
     * {@link #writeHeaderAndData(FSDataOutputStream)}.
     */
    private long startOffset;

    /**
     * Offset of previous block by block type. Updated when the next block is
     * started.
     */
    private long[] prevOffsetByType;

    /** The offset of the previous block of the same type */
    private long prevOffset;
    /** Meta data that holds information about the hfileblock**/
    private HFileContext fileContext;

    /**
     * @param dataBlockEncoder data block encoding algorithm to use
     */
    public Writer(HFileDataBlockEncoder dataBlockEncoder, HFileContext fileContext) {
      if (fileContext.getBytesPerChecksum() < HConstants.HFILEBLOCK_HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " +
            " Minimum is " + HConstants.HFILEBLOCK_HEADER_SIZE + " but the configured value is " +
            fileContext.getBytesPerChecksum());
      }
      this.dataBlockEncoder = dataBlockEncoder != null?
          dataBlockEncoder: NoOpDataBlockEncoder.INSTANCE;
      this.dataBlockEncodingCtx = this.dataBlockEncoder.
          newDataBlockEncodingContext(HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: This should be lazily instantiated since we usually do NOT need this default encoder
      this.defaultBlockEncodingCtx = new HFileBlockDefaultEncodingContext(null,
          HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: Set BAOS initial size. Use fileContext.getBlocksize() and add for header/checksum
      baosInMemory = new ByteArrayOutputStream();
      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i) {
        prevOffsetByType[i] = UNSET;
      }
      // TODO: Why fileContext saved away when we have dataBlockEncoder and/or
      // defaultDataBlockEncoder?
      this.fileContext = fileContext;
    }

    /**
     * Starts writing into the block. The previous block's data is discarded.
     *
     * @return the stream the user can write their data into
     * @throws IOException
     */
    DataOutputStream startWriting(BlockType newBlockType)
        throws IOException {
      if (state == State.BLOCK_READY && startOffset != -1) {
        // We had a previous block that was written to a stream at a specific
        // offset. Save that offset as the last offset of a block of that type.
        prevOffsetByType[blockType.getId()] = startOffset;
      }

      startOffset = -1;
      blockType = newBlockType;

      baosInMemory.reset();
      baosInMemory.write(HConstants.HFILEBLOCK_DUMMY_HEADER);

      state = State.WRITING;

      // We will compress it later in finishBlock()
      userDataStream = new ByteBufferWriterDataOutputStream(baosInMemory);
      if (newBlockType == BlockType.DATA) {
        this.dataBlockEncoder.startBlockEncoding(dataBlockEncodingCtx, userDataStream);
      }
      this.unencodedDataSizeWritten = 0;
      return userDataStream;
    }

    /**
     * Writes the Cell to this block
     * @param cell
     * @throws IOException
     */
    void write(Cell cell) throws IOException{
      expectState(State.WRITING);
      this.unencodedDataSizeWritten +=
          this.dataBlockEncoder.encode(cell, dataBlockEncodingCtx, this.userDataStream);
    }

    /**
     * Returns the stream for the user to write to. The block writer takes care
     * of handling compression and buffering for caching on write. Can only be
     * called in the "writing" state.
     *
     * @return the data output stream for the user to write to
     */
    DataOutputStream getUserDataStream() {
      expectState(State.WRITING);
      return userDataStream;
    }

    /**
     * Transitions the block writer from the "writing" state to the "block
     * ready" state.  Does nothing if a block is already finished.
     */
    void ensureBlockReady() throws IOException {
      Preconditions.checkState(state != State.INIT,
          "Unexpected state: " + state);

      if (state == State.BLOCK_READY) {
        return;
      }

      // This will set state to BLOCK_READY.
      finishBlock();
    }

    /**
     * Finish up writing of the block.
     * Flushes the compressing stream (if using compression), fills out the header,
     * does any compression/encryption of bytes to flush out to disk, and manages
     * the cache on write content, if applicable. Sets block write state to "block ready".
     */
    private void finishBlock() throws IOException {
      if (blockType == BlockType.DATA) {
        this.dataBlockEncoder.endBlockEncoding(dataBlockEncodingCtx, userDataStream,
            baosInMemory.getBuffer(), blockType);
        blockType = dataBlockEncodingCtx.getBlockType();
      }
      userDataStream.flush();
      // This does an array copy, so it is safe to cache this byte array when cache-on-write.
      // Header is still the empty, 'dummy' header that is yet to be filled out.
      uncompressedBlockBytesWithHeader = baosInMemory.toByteArray();
      prevOffset = prevOffsetByType[blockType.getId()];

      // We need to set state before we can package the block up for cache-on-write. In a way, the
      // block is ready, but not yet encoded or compressed.
      state = State.BLOCK_READY;
      if (blockType == BlockType.DATA || blockType == BlockType.ENCODED_DATA) {
        onDiskBlockBytesWithHeader = dataBlockEncodingCtx.
            compressAndEncrypt(uncompressedBlockBytesWithHeader);
      } else {
        onDiskBlockBytesWithHeader = defaultBlockEncodingCtx.
            compressAndEncrypt(uncompressedBlockBytesWithHeader);
      }
      // Calculate how many bytes we need for checksum on the tail of the block.
      int numBytes = (int) ChecksumUtil.numBytes(
          onDiskBlockBytesWithHeader.length,
          fileContext.getBytesPerChecksum());

      // Put the header for the on disk bytes; header currently is unfilled-out
      putHeader(onDiskBlockBytesWithHeader, 0,
          onDiskBlockBytesWithHeader.length + numBytes,
          uncompressedBlockBytesWithHeader.length, onDiskBlockBytesWithHeader.length);
      // Set the header for the uncompressed bytes (for cache-on-write) -- IFF different from
      // onDiskBlockBytesWithHeader array.
      if (onDiskBlockBytesWithHeader != uncompressedBlockBytesWithHeader) {
        putHeader(uncompressedBlockBytesWithHeader, 0,
          onDiskBlockBytesWithHeader.length + numBytes,
          uncompressedBlockBytesWithHeader.length, onDiskBlockBytesWithHeader.length);
      }
      if (onDiskChecksum.length != numBytes) {
        onDiskChecksum = new byte[numBytes];
      }
      ChecksumUtil.generateChecksums(
          onDiskBlockBytesWithHeader, 0, onDiskBlockBytesWithHeader.length,
          onDiskChecksum, 0, fileContext.getChecksumType(), fileContext.getBytesPerChecksum());
    }

    /**
     * Put the header into the given byte array at the given offset.
     * @param onDiskSize size of the block on disk header + data + checksum
     * @param uncompressedSize size of the block after decompression (but
     *          before optional data block decoding) including header
     * @param onDiskDataSize size of the block on disk with header
     *        and data but not including the checksums
     */
    private void putHeader(byte[] dest, int offset, int onDiskSize,
        int uncompressedSize, int onDiskDataSize) {
      offset = blockType.put(dest, offset);
      offset = Bytes.putInt(dest, offset, onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putLong(dest, offset, prevOffset);
      offset = Bytes.putByte(dest, offset, fileContext.getChecksumType().getCode());
      offset = Bytes.putInt(dest, offset, fileContext.getBytesPerChecksum());
      Bytes.putInt(dest, offset, onDiskDataSize);
    }

    /**
     * Similar to {@link #writeHeaderAndData(FSDataOutputStream)}, but records
     * the offset of this block so that it can be referenced in the next block
     * of the same type.
     *
     * @param out
     * @throws IOException
     */
    void writeHeaderAndData(FSDataOutputStream out) throws IOException {
      long offset = out.getPos();
      if (startOffset != UNSET && offset != startOffset) {
        throw new IOException("A " + blockType + " block written to a "
            + "stream twice, first at offset " + startOffset + ", then at "
            + offset);
      }
      startOffset = offset;

      finishBlockAndWriteHeaderAndData((DataOutputStream) out);
    }

    /**
     * Writes the header and the compressed data of this block (or uncompressed
     * data when not using compression) into the given stream. Can be called in
     * the "writing" state or in the "block ready" state. If called in the
     * "writing" state, transitions the writer to the "block ready" state.
     *
     * @param out the output stream to write the
     * @throws IOException
     */
    protected void finishBlockAndWriteHeaderAndData(DataOutputStream out)
      throws IOException {
      ensureBlockReady();
      out.write(onDiskBlockBytesWithHeader);
      out.write(onDiskChecksum);
    }

    /**
     * Returns the header or the compressed data (or uncompressed data when not
     * using compression) as a byte array. Can be called in the "writing" state
     * or in the "block ready" state. If called in the "writing" state,
     * transitions the writer to the "block ready" state. This returns
     * the header + data + checksums stored on disk.
     *
     * @return header and data as they would be stored on disk in a byte array
     * @throws IOException
     */
    byte[] getHeaderAndDataForTest() throws IOException {
      ensureBlockReady();
      // This is not very optimal, because we are doing an extra copy.
      // But this method is used only by unit tests.
      byte[] output =
          new byte[onDiskBlockBytesWithHeader.length
              + onDiskChecksum.length];
      System.arraycopy(onDiskBlockBytesWithHeader, 0, output, 0,
          onDiskBlockBytesWithHeader.length);
      System.arraycopy(onDiskChecksum, 0, output,
          onDiskBlockBytesWithHeader.length, onDiskChecksum.length);
      return output;
    }

    /**
     * Releases resources used by this writer.
     */
    void release() {
      if (dataBlockEncodingCtx != null) {
        dataBlockEncodingCtx.close();
        dataBlockEncodingCtx = null;
      }
      if (defaultBlockEncodingCtx != null) {
        defaultBlockEncodingCtx.close();
        defaultBlockEncodingCtx = null;
      }
    }

    /**
     * Returns the on-disk size of the data portion of the block. This is the
     * compressed size if compression is enabled. Can only be called in the
     * "block ready" state. Header is not compressed, and its size is not
     * included in the return value.
     *
     * @return the on-disk size of the block, not including the header.
     */
    int getOnDiskSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.length +
          onDiskChecksum.length - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * Returns the on-disk size of the block. Can only be called in the
     * "block ready" state.
     *
     * @return the on-disk size of the block ready to be written, including the
     *         header size, the data and the checksum data.
     */
    int getOnDiskSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.length + onDiskChecksum.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedBlockBytesWithHeader.length - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    int getUncompressedSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedBlockBytesWithHeader.length;
    }

    /** @return true if a block is being written  */
    boolean isWriting() {
      return state == State.WRITING;
    }

    /**
     * Returns the number of bytes written into the current block so far, or
     * zero if not writing the block at the moment. Note that this will return
     * zero in the "block ready" state as well.
     *
     * @return the number of bytes written
     */
    int blockSizeWritten() {
      if (state != State.WRITING) return 0;
      return this.unencodedDataSizeWritten;
    }

    /**
     * Returns the header followed by the uncompressed data, even if using
     * compression. This is needed for storing uncompressed blocks in the block
     * cache. Can be called in the "writing" state or the "block ready" state.
     * Returns only the header and data, does not include checksum data.
     *
     * @return uncompressed block bytes for caching on write
     */
    ByteBuffer getUncompressedBufferWithHeader() {
      expectState(State.BLOCK_READY);
      return ByteBuffer.wrap(uncompressedBlockBytesWithHeader);
    }

    /**
     * Returns the header followed by the on-disk (compressed/encoded/encrypted) data. This is
     * needed for storing packed blocks in the block cache. Expects calling semantics identical to
     * {@link #getUncompressedBufferWithHeader()}. Returns only the header and data,
     * Does not include checksum data.
     *
     * @return packed block bytes for caching on write
     */
    ByteBuffer getOnDiskBufferWithHeader() {
      expectState(State.BLOCK_READY);
      return ByteBuffer.wrap(onDiskBlockBytesWithHeader);
    }

    private void expectState(State expectedState) {
      if (state != expectedState) {
        throw new IllegalStateException("Expected state: " + expectedState +
            ", actual state: " + state);
      }
    }

    /**
     * Takes the given {@link BlockWritable} instance, creates a new block of
     * its appropriate type, writes the writable into this block, and flushes
     * the block into the output stream. The writer is instructed not to buffer
     * uncompressed bytes for cache-on-write.
     *
     * @param bw the block-writable object to write as a block
     * @param out the file system output stream
     * @throws IOException
     */
    void writeBlock(BlockWritable bw, FSDataOutputStream out)
        throws IOException {
      bw.writeToBlock(startWriting(bw.getBlockType()));
      writeHeaderAndData(out);
    }

    /**
     * Creates a new HFileBlock. Checksums have already been validated, so
     * the byte buffer passed into the constructor of this newly created
     * block does not have checksum data even though the header minor
     * version is MINOR_VERSION_WITH_CHECKSUM. This is indicated by setting a
     * 0 value in bytesPerChecksum.
     *
     * <p>TODO: Should there be an option where a cache can ask that hbase preserve block
     * checksums for checking after a block comes out of the cache? Otehrwise, cache is responsible
     * for blocks being wholesome (ECC memory or if file-backed, it does checksumming).
     */
    HFileBlock getBlockForCaching(CacheConfig cacheConf) {
      HFileContext newContext = new HFileContextBuilder()
                                .withBlockSize(fileContext.getBlocksize())
                                .withBytesPerCheckSum(0)
                                .withChecksumType(ChecksumType.NULL) // no checksums in cached data
                                .withCompression(fileContext.getCompression())
                                .withDataBlockEncoding(fileContext.getDataBlockEncoding())
                                .withHBaseCheckSum(fileContext.isUseHBaseChecksum())
                                .withCompressTags(fileContext.isCompressTags())
                                .withIncludesMvcc(fileContext.isIncludesMvcc())
                                .withIncludesTags(fileContext.isIncludesTags())
                                .build();
       return new HFileBlock(blockType, getOnDiskSizeWithoutHeader(),
          getUncompressedSizeWithoutHeader(), prevOffset,
          cacheConf.shouldCacheCompressed(blockType.getCategory())?
            getOnDiskBufferWithHeader() :
            getUncompressedBufferWithHeader(),
          FILL_HEADER, startOffset, UNSET,
          onDiskBlockBytesWithHeader.length + onDiskChecksum.length, newContext);
    }
  }

  /** Something that can be written into a block. */
  interface BlockWritable {

    /** The type of block this data should use. */
    BlockType getBlockType();

    /**
     * Writes the block to the provided stream. Must not write any magic
     * records.
     *
     * @param out a stream to write uncompressed data into
     */
    void writeToBlock(DataOutput out) throws IOException;
  }

  // Block readers and writers

  /** An interface allowing to iterate {@link HFileBlock}s. */
  interface BlockIterator {

    /**
     * Get the next block, or null if there are no more blocks to iterate.
     */
    HFileBlock nextBlock() throws IOException;

    /**
     * Similar to {@link #nextBlock()} but checks block type, throws an
     * exception if incorrect, and returns the HFile block
     */
    HFileBlock nextBlockWithBlockType(BlockType blockType) throws IOException;
  }

  /** A full-fledged reader with iteration ability. */
  interface FSReader {

    /**
     * Reads the block at the given offset in the file with the given on-disk
     * size and uncompressed size.
     *
     * @param offset
     * @param onDiskSize the on-disk size of the entire block, including all
     *          applicable headers, or -1 if unknown
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize, boolean pread) throws IOException;

    /**
     * Creates a block iterator over the given portion of the {@link HFile}.
     * The iterator returns blocks starting with offset such that offset &lt;=
     * startOffset &lt; endOffset. Returned blocks are always unpacked.
     *
     * @param startOffset the offset of the block to start iteration with
     * @param endOffset the offset to end iteration at (exclusive)
     * @return an iterator of blocks between the two given offsets
     */
    BlockIterator blockRange(long startOffset, long endOffset);

    /** Closes the backing streams */
    void closeStreams() throws IOException;

    /** Get a decoder for {@link BlockType#ENCODED_DATA} blocks from this file. */
    HFileBlockDecodingContext getBlockDecodingContext();

    /** Get the default decoder for blocks from this file. */
    HFileBlockDecodingContext getDefaultBlockDecodingContext();

    void setIncludesMemstoreTS(boolean includesMemstoreTS);
    void setDataBlockEncoder(HFileDataBlockEncoder encoder);
  }

  /**
   * Data-structure to use caching the header of the NEXT block. Only works if next read
   * that comes in here is next in sequence in this block.
   *
   * When we read, we read current block and the next blocks' header. We do this so we have
   * the length of the next block to read if the hfile index is not available (rare).
   * TODO: Review!! This trick of reading next blocks header is a pain, complicates our
   * read path and I don't think it needed given it rare we don't have the block index
   * (it is 'normally' present, gotten from the hfile index). FIX!!!
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte [] header = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
    final ByteBuffer buf = ByteBuffer.wrap(header, 0, HConstants.HFILEBLOCK_HEADER_SIZE);

    @Override
    public String toString() {
      return "offset=" + this.offset + ", header=" + Bytes.toStringBinary(header);
    }
  }

  /**
   * Reads version 2 blocks from the filesystem.
   */
  static class FSReaderImpl implements FSReader {
    /** The file system stream of the underlying {@link HFile} that
     * does or doesn't do checksum validations in the filesystem */
    protected FSDataInputStreamWrapper streamWrapper;

    private HFileBlockDecodingContext encodedBlockDecodingCtx;

    /** Default context used when BlockType != {@link BlockType#ENCODED_DATA}. */
    private final HFileBlockDefaultDecodingContext defaultDecodingCtx;

    /**
     * Cache of the NEXT header after this. Check it is indeed next blocks header
     * before using it. TODO: Review. This overread into next block to fetch
     * next blocks header seems unnecessary given we usually get the block size
     * from the hfile index. Review!
     */
    private AtomicReference<PrefetchedHeader> prefetchedHeader =
        new AtomicReference<PrefetchedHeader>(new PrefetchedHeader());

    /** The size of the file we are reading from, or -1 if unknown. */
    protected long fileSize;

    /** The size of the header */
    protected final int hdrSize;

    /** The filesystem used to access data */
    protected HFileSystem hfs;

    private final Lock streamLock = new ReentrantLock();

    /** The default buffer size for our buffered streams */
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    protected HFileContext fileContext;
    // Cache the fileName
    protected String pathName;

    FSReaderImpl(FSDataInputStreamWrapper stream, long fileSize, HFileSystem hfs, Path path,
        HFileContext fileContext) throws IOException {
      this.fileSize = fileSize;
      this.hfs = hfs;
      if (path != null) {
        this.pathName = path.toString();
      }
      this.fileContext = fileContext;
      this.hdrSize = headerSize(fileContext.isUseHBaseChecksum());

      this.streamWrapper = stream;
      // Older versions of HBase didn't support checksum.
      this.streamWrapper.prepareForBlockReader(!fileContext.isUseHBaseChecksum());
      defaultDecodingCtx = new HFileBlockDefaultDecodingContext(fileContext);
      encodedBlockDecodingCtx = defaultDecodingCtx;
    }

    /**
     * A constructor that reads files with the latest minor version.
     * This is used by unit tests only.
     */
    FSReaderImpl(FSDataInputStream istream, long fileSize, HFileContext fileContext)
    throws IOException {
      this(new FSDataInputStreamWrapper(istream), fileSize, null, null, fileContext);
    }

    public BlockIterator blockRange(final long startOffset, final long endOffset) {
      final FSReader owner = this; // handle for inner class
      return new BlockIterator() {
        private long offset = startOffset;
        // Cache length of next block. Current block has the length of next block in it.
        private long length = -1;

        @Override
        public HFileBlock nextBlock() throws IOException {
          if (offset >= endOffset) {
            return null;
          }
          HFileBlock b = readBlockData(offset, length, false);
          offset += b.getOnDiskSizeWithHeader();
          length = b.getNextBlockOnDiskSize();
          return b.unpack(fileContext, owner);
        }

        @Override
        public HFileBlock nextBlockWithBlockType(BlockType blockType)
            throws IOException {
          HFileBlock blk = nextBlock();
          if (blk.getBlockType() != blockType) {
            throw new IOException("Expected block of type " + blockType
                + " but found " + blk.getBlockType());
          }
          return blk;
        }
      };
    }

    /**
     * Does a positional read or a seek and read into the given buffer. Returns
     * the on-disk size of the next block, or -1 if it could not be read/determined; e.g. EOF.
     *
     * @param dest destination buffer
     * @param destOffset offset into the destination buffer at where to put the bytes we read
     * @param size size of read
     * @param peekIntoNextBlock whether to read the next block's on-disk size
     * @param fileOffset position in the stream to read at
     * @param pread whether we should do a positional read
     * @param istream The input source of data
     * @return the on-disk size of the next block with header size included, or
     *         -1 if it could not be determined; if not -1, the <code>dest</code> INCLUDES the
     *         next header
     * @throws IOException
     */
    protected int readAtOffset(FSDataInputStream istream, byte [] dest, int destOffset, int size,
        boolean peekIntoNextBlock, long fileOffset, boolean pread) throws IOException {
      if (peekIntoNextBlock && destOffset + size + hdrSize > dest.length) {
        // We are asked to read the next block's header as well, but there is
        // not enough room in the array.
        throw new IOException("Attempted to read " + size + " bytes and " +
            hdrSize + " bytes of next header into a " + dest.length +
            "-byte array at offset " + destOffset);
      }

      if (!pread && streamLock.tryLock()) {
        // Seek + read. Better for scanning.
        try {
          istream.seek(fileOffset);

          long realOffset = istream.getPos();
          if (realOffset != fileOffset) {
            throw new IOException("Tried to seek to " + fileOffset + " to "
                + "read " + size + " bytes, but pos=" + realOffset
                + " after seek");
          }

          if (!peekIntoNextBlock) {
            IOUtils.readFully(istream, dest, destOffset, size);
            return -1;
          }

          // Try to read the next block header.
          if (!readWithExtra(istream, dest, destOffset, size, hdrSize)) {
            return -1;
          }
        } finally {
          streamLock.unlock();
        }
      } else {
        // Positional read. Better for random reads; or when the streamLock is already locked.
        int extraSize = peekIntoNextBlock ? hdrSize : 0;
        if (!positionalReadWithExtra(istream, fileOffset, dest, destOffset, size, extraSize)) {
          return -1;
        }
      }

      assert peekIntoNextBlock;
      return Bytes.toInt(dest, destOffset + size + BlockType.MAGIC_LENGTH) + hdrSize;
    }

    /**
     * Reads a version 2 block (version 1 blocks not supported and not expected). Tries to do as
     * little memory allocation as possible, using the provided on-disk size.
     *
     * @param offset the offset in the stream to read at
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including
     *          the header, or -1 if unknown; i.e. when iterating over blocks reading
     *          in the file metadata info.
     * @param pread whether to use a positional read
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL, boolean pread)
    throws IOException {
      // Get a copy of the current state of whether to validate
      // hbase checksums or not for this read call. This is not
      // thread-safe but the one constaint is that if we decide
      // to skip hbase checksum verification then we are
      // guaranteed to use hdfs checksum verification.
      boolean doVerificationThruHBaseChecksum = streamWrapper.shouldUseHBaseChecksum();
      FSDataInputStream is = streamWrapper.getStream(doVerificationThruHBaseChecksum);

      HFileBlock blk = readBlockDataInternal(is, offset,
                         onDiskSizeWithHeaderL, pread,
                         doVerificationThruHBaseChecksum);
      if (blk == null) {
        HFile.LOG.warn("HBase checksum verification failed for file " +
                       pathName + " at offset " +
                       offset + " filesize " + fileSize +
                       ". Retrying read with HDFS checksums turned on...");

        if (!doVerificationThruHBaseChecksum) {
          String msg = "HBase checksum verification failed for file " +
                       pathName + " at offset " +
                       offset + " filesize " + fileSize +
                       " but this cannot happen because doVerify is " +
                       doVerificationThruHBaseChecksum;
          HFile.LOG.warn(msg);
          throw new IOException(msg); // cannot happen case here
        }
        HFile.CHECKSUM_FAILURES.increment(); // update metrics

        // If we have a checksum failure, we fall back into a mode where
        // the next few reads use HDFS level checksums. We aim to make the
        // next CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD reads avoid
        // hbase checksum verification, but since this value is set without
        // holding any locks, it can so happen that we might actually do
        // a few more than precisely this number.
        is = this.streamWrapper.fallbackToFsChecksum(CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD);
        doVerificationThruHBaseChecksum = false;
        blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
                                    doVerificationThruHBaseChecksum);
        if (blk != null) {
          HFile.LOG.warn("HDFS checksum verification suceeded for file " +
                         pathName + " at offset " +
                         offset + " filesize " + fileSize);
        }
      }
      if (blk == null && !doVerificationThruHBaseChecksum) {
        String msg = "readBlockData failed, possibly due to " +
                     "checksum verification failed for file " + pathName +
                     " at offset " + offset + " filesize " + fileSize;
        HFile.LOG.warn(msg);
        throw new IOException(msg);
      }

      // If there is a checksum mismatch earlier, then retry with
      // HBase checksums switched off and use HDFS checksum verification.
      // This triggers HDFS to detect and fix corrupt replicas. The
      // next checksumOffCount read requests will use HDFS checksums.
      // The decrementing of this.checksumOffCount is not thread-safe,
      // but it is harmless because eventually checksumOffCount will be
      // a negative number.
      streamWrapper.checksumOk();
      return blk;
    }

    /**
     * @return Check <code>onDiskSizeWithHeaderL</code> size is healthy and then return it as an int
     * @throws IOException
     */
    private static int checkAndGetSizeAsInt(final long onDiskSizeWithHeaderL, final int hdrSize)
    throws IOException {
      if ((onDiskSizeWithHeaderL < hdrSize && onDiskSizeWithHeaderL != -1)
          || onDiskSizeWithHeaderL >= Integer.MAX_VALUE) {
        throw new IOException("Invalid onDisksize=" + onDiskSizeWithHeaderL
            + ": expected to be at least " + hdrSize
            + " and at most " + Integer.MAX_VALUE + ", or -1");
      }
      return (int)onDiskSizeWithHeaderL;
    }

    /**
     * Verify the passed in onDiskSizeWithHeader aligns with what is in the header else something
     * is not right.
     * @throws IOException
     */
    private void verifyOnDiskSizeMatchesHeader(final int passedIn, final ByteBuffer headerBuf,
        final long offset, boolean verifyChecksum)
    throws IOException {
      // Assert size provided aligns with what is in the header
      int fromHeader = getOnDiskSizeWithHeader(headerBuf, verifyChecksum);
      if (passedIn != fromHeader) {
        throw new IOException("Passed in onDiskSizeWithHeader=" + passedIn + " != " + fromHeader +
            ", offset=" + offset + ", fileContext=" + this.fileContext);
      }
    }

    /**
     * Check atomic reference cache for this block's header. Cache only good if next
     * read coming through is next in sequence in the block. We read next block's
     * header on the tail of reading the previous block to save a seek. Otherwise,
     * we have to do a seek to read the header before we can pull in the block OR
     * we have to backup the stream because we over-read (the next block's header).
     * @see PrefetchedHeader
     * @return The cached block header or null if not found.
     * @see #cacheNextBlockHeader(long, byte[], int, int)
     */
    private ByteBuffer getCachedHeader(final long offset) {
      PrefetchedHeader ph = this.prefetchedHeader.get();
      return ph != null && ph.offset == offset? ph.buf: null;
    }

    /**
     * Save away the next blocks header in atomic reference.
     * @see #getCachedHeader(long)
     * @see PrefetchedHeader
     */
    private void cacheNextBlockHeader(final long offset,
        final byte [] header, final int headerOffset, final int headerLength) {
      PrefetchedHeader ph = new PrefetchedHeader();
      ph.offset = offset;
      System.arraycopy(header, headerOffset, ph.header, 0, headerLength);
      this.prefetchedHeader.set(ph);
    }

    /**
     * Reads a version 2 block.
     *
     * @param offset the offset in the stream to read at. Usually the
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including
     *          the header and checksums if present or -1 if unknown (as a long). Can be -1
     *          if we are doing raw iteration of blocks as when loading up file metadata; i.e.
     *          the first read of a new file (TODO: Fix! See HBASE-17072). Usually non-null gotten
     *          from the file index.
     * @param pread whether to use a positional read
     * @param verifyChecksum Whether to use HBase checksums.
     *        If HBase checksum is switched off, then use HDFS checksum.
     * @return the HFileBlock or null if there is a HBase checksum mismatch
     */
    protected HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
        long onDiskSizeWithHeaderL, boolean pread, boolean verifyChecksum)
     throws IOException {
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read "
            + "block (onDiskSize=" + onDiskSizeWithHeaderL + ")");
      }
      int onDiskSizeWithHeader = checkAndGetSizeAsInt(onDiskSizeWithHeaderL, hdrSize);
      // Try and get cached header. Will serve us in rare case where onDiskSizeWithHeaderL is -1
      // and will save us having to seek the stream backwards to reread the header we
      // read the last time through here.
      ByteBuffer headerBuf = getCachedHeader(offset);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reading " + this.fileContext.getHFileName() + " at offset=" + offset +
          ", pread=" + pread + ", verifyChecksum=" + verifyChecksum + ", cachedHeader=" +
          headerBuf + ", onDiskSizeWithHeader=" + onDiskSizeWithHeader);
      }
      if (onDiskSizeWithHeader <= 0) {
        // We were not passed the block size. Need to get it from the header. If header was not in
        // cache, need to seek to pull it in. This is costly and should happen very rarely.
        // Currently happens on open of a hfile reader where we read the trailer blocks for
        // indices. Otherwise, we are reading block sizes out of the hfile index. To check,
        // enable TRACE in this file and you'll get an exception in a LOG every time we seek.
        // See HBASE-17072 for more detail.
        if (headerBuf == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Extra see to get block size!", new RuntimeException());
          }
          headerBuf = ByteBuffer.allocate(hdrSize);
          readAtOffset(is, headerBuf.array(), headerBuf.arrayOffset(), hdrSize, false,
              offset, pread);
        }
        onDiskSizeWithHeader = getOnDiskSizeWithHeader(headerBuf,
          this.fileContext.isUseHBaseChecksum());
      }
      int preReadHeaderSize = headerBuf == null? 0 : hdrSize;
      // Allocate enough space to fit the next block's header too; saves a seek next time through.
      // onDiskBlock is whole block + header + checksums then extra hdrSize to read next header;
      // onDiskSizeWithHeader is header, body, and any checksums if present. preReadHeaderSize
      // says where to start reading. If we have the header cached, then we don't need to read
      // it again and we can likely read from last place we left off w/o need to backup and reread
      // the header we read last time through here. TODO: Review this overread of the header. Is it necessary
      // when we get the block size from the hfile index? See note on PrefetchedHeader class above.
      // TODO: Make this ByteBuffer-based. Will make it easier to go to HDFS with BBPool (offheap).
      byte [] onDiskBlock = new byte[onDiskSizeWithHeader + hdrSize];
      int nextBlockOnDiskSize = readAtOffset(is, onDiskBlock, preReadHeaderSize,
          onDiskSizeWithHeader - preReadHeaderSize, true, offset + preReadHeaderSize, pread);
      if (headerBuf != null) {
        // The header has been read when reading the previous block OR in a distinct header-only
        // read. Copy to this block's header.
        System.arraycopy(headerBuf.array(), headerBuf.arrayOffset(), onDiskBlock, 0, hdrSize);
      } else {
        headerBuf = ByteBuffer.wrap(onDiskBlock, 0, hdrSize);
      }
      // Do a few checks before we go instantiate HFileBlock.
      assert onDiskSizeWithHeader > this.hdrSize;
      verifyOnDiskSizeMatchesHeader(onDiskSizeWithHeader, headerBuf, offset,
        this.fileContext.isUseHBaseChecksum());
      ByteBuffer onDiskBlockByteBuffer = ByteBuffer.wrap(onDiskBlock, 0, onDiskSizeWithHeader);
      // Verify checksum of the data before using it for building HFileBlock.
      if (verifyChecksum &&
          !validateChecksum(offset, onDiskBlockByteBuffer, hdrSize)) {
        return null;
      }
      // The onDiskBlock will become the headerAndDataBuffer for this block.
      // If nextBlockOnDiskSizeWithHeader is not zero, the onDiskBlock already
      // contains the header of next block, so no need to set next block's header in it.
      HFileBlock hFileBlock =
          new HFileBlock(new SingleByteBuff(onDiskBlockByteBuffer),
              this.fileContext.isUseHBaseChecksum(), MemoryType.EXCLUSIVE, offset,
              nextBlockOnDiskSize, fileContext);
      // Run check on uncompressed sizings.
      if (!fileContext.isCompressedOrEncrypted()) {
        hFileBlock.sanityCheckUncompressed();
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Read " + hFileBlock);
      }
      // Cache next block header if we read it for the next time through here.
      if (nextBlockOnDiskSize != -1) {
        cacheNextBlockHeader(offset + hFileBlock.getOnDiskSizeWithHeader(),
            onDiskBlock, onDiskSizeWithHeader, hdrSize);
      }
      return hFileBlock;
    }

    @Override
    public void setIncludesMemstoreTS(boolean includesMemstoreTS) {
      this.fileContext.setIncludesMvcc(includesMemstoreTS);
    }

    @Override
    public void setDataBlockEncoder(HFileDataBlockEncoder encoder) {
      encodedBlockDecodingCtx = encoder.newDataBlockDecodingContext(this.fileContext);
    }

    @Override
    public HFileBlockDecodingContext getBlockDecodingContext() {
      return this.encodedBlockDecodingCtx;
    }

    @Override
    public HFileBlockDecodingContext getDefaultBlockDecodingContext() {
      return this.defaultDecodingCtx;
    }

    /**
     * Generates the checksum for the header as well as the data and then validates it.
     * If the block doesn't uses checksum, returns false.
     * @return True if checksum matches, else false.
     */
    protected boolean validateChecksum(long offset, ByteBuffer data, int hdrSize)
        throws IOException {
      // If this is an older version of the block that does not have checksums, then return false
      // indicating that checksum verification did not succeed. Actually, this method should never
      // be called when the minorVersion is 0, thus this is a defensive check for a cannot-happen
      // case. Since this is a cannot-happen case, it is better to return false to indicate a
      // checksum validation failure.
      if (!fileContext.isUseHBaseChecksum()) {
        return false;
      }
      return ChecksumUtil.validateChecksum(data, pathName, offset, hdrSize);
    }

    @Override
    public void closeStreams() throws IOException {
      streamWrapper.close();
    }

    @Override
    public String toString() {
      return "hfs=" + hfs + ", path=" + pathName + ", fileContext=" + fileContext;
    }
  }

  /** An additional sanity-check in case no compression or encryption is being used. */
  void sanityCheckUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader +
        totalChecksumBytes()) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader
          + ", numChecksumbytes=" + totalChecksumBytes());
    }
  }

  // Cacheable implementation
  @Override
  public int getSerializedLength() {
    if (buf != null) {
      // Include extra bytes for block metadata.
      return this.buf.limit() + BLOCK_METADATA_SPACE;
    }
    return 0;
  }

  // Cacheable implementation
  @Override
  public void serialize(ByteBuffer destination) {
    // BE CAREFUL!! There is a custom version of this serialization over in BucketCache#doDrain.
    // Make sure any changes in here are reflected over there.
    this.buf.get(destination, 0, getSerializedLength() - BLOCK_METADATA_SPACE);
    destination = addMetaData(destination);

    // Make it ready for reading. flip sets position to zero and limit to current position which
    // is what we want if we do not want to serialize the block plus checksums if present plus
    // metadata.
    destination.flip();
  }

  /**
   * For use by bucketcache. This exposes internals.
   */
  public ByteBuffer getMetaData() {
    ByteBuffer bb = ByteBuffer.allocate(BLOCK_METADATA_SPACE);
    bb = addMetaData(bb);
    bb.flip();
    return bb;
  }

  /**
   * Adds metadata at current position (position is moved forward). Does not flip or reset.
   * @return The passed <code>destination</code> with metadata added.
   */
  private ByteBuffer addMetaData(final ByteBuffer destination) {
    destination.put(this.fileContext.isUseHBaseChecksum() ? (byte) 1 : (byte) 0);
    destination.putLong(this.offset);
    destination.putInt(this.nextBlockOnDiskSize);
    return destination;
  }

  // Cacheable implementation
  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
    return HFileBlock.BLOCK_DESERIALIZER;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + blockType.hashCode();
    result = result * 31 + nextBlockOnDiskSize;
    result = result * 31 + (int) (offset ^ (offset >>> 32));
    result = result * 31 + onDiskSizeWithoutHeader;
    result = result * 31 + (int) (prevBlockOffset ^ (prevBlockOffset >>> 32));
    result = result * 31 + uncompressedSizeWithoutHeader;
    result = result * 31 + buf.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object comparison) {
    if (this == comparison) {
      return true;
    }
    if (comparison == null) {
      return false;
    }
    if (comparison.getClass() != this.getClass()) {
      return false;
    }

    HFileBlock castedComparison = (HFileBlock) comparison;

    if (castedComparison.blockType != this.blockType) {
      return false;
    }
    if (castedComparison.nextBlockOnDiskSize != this.nextBlockOnDiskSize) {
      return false;
    }
    // Offset is important. Needed when we have to remake cachekey when block is returned to cache.
    if (castedComparison.offset != this.offset) {
      return false;
    }
    if (castedComparison.onDiskSizeWithoutHeader != this.onDiskSizeWithoutHeader) {
      return false;
    }
    if (castedComparison.prevBlockOffset != this.prevBlockOffset) {
      return false;
    }
    if (castedComparison.uncompressedSizeWithoutHeader != this.uncompressedSizeWithoutHeader) {
      return false;
    }
    if (ByteBuff.compareTo(this.buf, 0, this.buf.limit(), castedComparison.buf, 0,
        castedComparison.buf.limit()) != 0) {
      return false;
    }
    return true;
  }

  public DataBlockEncoding getDataBlockEncoding() {
    if (blockType == BlockType.ENCODED_DATA) {
      return DataBlockEncoding.getEncodingById(getDataBlockEncodingId());
    }
    return DataBlockEncoding.NONE;
  }

  byte getChecksumType() {
    return this.fileContext.getChecksumType().getCode();
  }

  int getBytesPerChecksum() {
    return this.fileContext.getBytesPerChecksum();
  }

  /** @return the size of data on disk + header. Excludes checksum. */
  int getOnDiskDataSizeWithHeader() {
    return this.onDiskDataSizeWithHeader;
  }

  /**
   * Calculate the number of bytes required to store all the checksums
   * for this block. Each checksum value is a 4 byte integer.
   */
  int totalChecksumBytes() {
    // If the hfile block has minorVersion 0, then there are no checksum
    // data to validate. Similarly, a zero value in this.bytesPerChecksum
    // indicates that cached blocks do not have checksum data because
    // checksums were already validated when the block was read from disk.
    if (!fileContext.isUseHBaseChecksum() || this.fileContext.getBytesPerChecksum() == 0) {
      return 0;
    }
    return (int) ChecksumUtil.numBytes(onDiskDataSizeWithHeader,
        this.fileContext.getBytesPerChecksum());
  }

  /**
   * Returns the size of this block header.
   */
  public int headerSize() {
    return headerSize(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Maps a minor version to the size of the header.
   */
  public static int headerSize(boolean usesHBaseChecksum) {
    return usesHBaseChecksum?
        HConstants.HFILEBLOCK_HEADER_SIZE: HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  byte[] getDummyHeaderForVersion() {
    return getDummyHeaderForVersion(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  static private byte[] getDummyHeaderForVersion(boolean usesHBaseChecksum) {
    return usesHBaseChecksum? HConstants.HFILEBLOCK_DUMMY_HEADER: DUMMY_HEADER_NO_CHECKSUM;
  }

  /**
   * @return This HFileBlocks fileContext which will a derivative of the
   * fileContext for the file from which this block's data was originally read.
   */
  HFileContext getHFileContext() {
    return this.fileContext;
  }

  @Override
  public MemoryType getMemoryType() {
    return this.memType;
  }

  /**
   * @return true if this block is backed by a shared memory area(such as that of a BucketCache).
   */
  boolean usesSharedMemory() {
    return this.memType == MemoryType.SHARED;
  }

  /**
   * Convert the contents of the block header into a human readable string.
   * This is mostly helpful for debugging. This assumes that the block
   * has minor version > 0.
   */
  @VisibleForTesting
  static String toStringHeader(ByteBuff buf) throws IOException {
    byte[] magicBuf = new byte[Math.min(buf.limit() - buf.position(), BlockType.MAGIC_LENGTH)];
    buf.get(magicBuf);
    BlockType bt = BlockType.parse(magicBuf, 0, BlockType.MAGIC_LENGTH);
    int compressedBlockSizeNoHeader = buf.getInt();
    int uncompressedBlockSizeNoHeader = buf.getInt();
    long prevBlockOffset = buf.getLong();
    byte cksumtype = buf.get();
    long bytesPerChecksum = buf.getInt();
    long onDiskDataSizeWithHeader = buf.getInt();
    return " Header dump: magic: " + Bytes.toString(magicBuf) +
                   " blockType " + bt +
                   " compressedBlockSizeNoHeader " +
                   compressedBlockSizeNoHeader +
                   " uncompressedBlockSizeNoHeader " +
                   uncompressedBlockSizeNoHeader +
                   " prevBlockOffset " + prevBlockOffset +
                   " checksumType " + ChecksumType.codeToType(cksumtype) +
                   " bytesPerChecksum " + bytesPerChecksum +
                   " onDiskDataSizeWithHeader " + onDiskDataSizeWithHeader;
  }

  public HFileBlock deepClone() {
    return new HFileBlock(this, true);
  }
}
