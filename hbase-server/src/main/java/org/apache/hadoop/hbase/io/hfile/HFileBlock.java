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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.ByteBufferSupportDataOutputStream;
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
 * Reading {@link HFile} version 1 and 2 blocks, and writing version 2 blocks.
 * <ul>
 * <li>In version 1 all blocks are always compressed or uncompressed, as
 * specified by the {@link HFile}'s compression algorithm, with a type-specific
 * magic record stored in the beginning of the compressed data (i.e. one needs
 * to uncompress the compressed block to determine the block type). There is
 * only a single compression algorithm setting for all blocks. Offset and size
 * information from the block index are required to read a block.
 * <li>In version 2 a block is structured as follows:
 * <ul>
 * <li>header (see Writer#finishBlock())
 * <ul>
 * <li>Magic record identifying the block type (8 bytes)
 * <li>Compressed block size, excluding header, including checksum (4 bytes)
 * <li>Uncompressed block size, excluding header, excluding checksum (4 bytes)
 * <li>The offset of the previous block of the same type (8 bytes). This is
 * used to be able to navigate to the previous block without going to the block
 * <li>For minorVersions &gt;=1, the ordinal describing checksum type (1 byte)
 * <li>For minorVersions &gt;=1, the number of data bytes/checksum chunk (4 bytes)
 * <li>For minorVersions &gt;=1, the size of data on disk, including header,
 * excluding checksums (4 bytes)
 * </ul>
 * </li>
 * <li>Raw/Compressed/Encrypted/Encoded data. The compression algorithm is the
 * same for all the blocks in the {@link HFile}, similarly to what was done in
 * version 1.
 * <li>For minorVersions &gt;=1, a series of 4 byte checksums, one each for
 * the number of bytes specified by bytesPerChecksum.
 * </ul>
 * </ul>
 */
@InterfaceAudience.Private
public class HFileBlock implements Cacheable {

  /**
   * On a checksum failure on a Reader, these many suceeding read
   * requests switch back to using hdfs checksums before auto-reenabling
   * hbase checksum verification.
   */
  static final int CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD = 3;

  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  /**
   * The size of block header when blockType is {@link BlockType#ENCODED_DATA}.
   * This extends normal header by adding the id of encoder.
   */
  public static final int ENCODED_HEADER_SIZE = HConstants.HFILEBLOCK_HEADER_SIZE
      + DataBlockEncoding.ID_SIZE;

  static final byte[] DUMMY_HEADER_NO_CHECKSUM =
     new byte[HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM];

  // How to get the estimate correctly? if it is a singleBB?
  public static final int MULTI_BYTE_BUFFER_HEAP_SIZE =
      (int)ClassSize.estimateBase(MultiByteBuff.class, false);

  // meta.usesHBaseChecksum+offset+nextBlockOnDiskSizeWithHeader
  public static final int EXTRA_SERIALIZATION_SPACE = Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  static final CacheableDeserializer<Cacheable> blockDeserializer =
      new CacheableDeserializer<Cacheable>() {
        public HFileBlock deserialize(ByteBuff buf, boolean reuse, MemoryType memType)
            throws IOException {
          buf.limit(buf.limit() - HFileBlock.EXTRA_SERIALIZATION_SPACE).rewind();
          ByteBuff newByteBuffer;
          if (reuse) {
            newByteBuffer = buf.slice();
          } else {
            // Used only in tests
            int len = buf.limit();
            newByteBuffer = new SingleByteBuff(ByteBuffer.allocate(len));
            newByteBuffer.put(0, buf, buf.position(), len);
          }
          buf.position(buf.limit());
          buf.limit(buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE);
          boolean usesChecksum = buf.get() == (byte)1;
          HFileBlock hFileBlock = new HFileBlock(newByteBuffer, usesChecksum, memType);
          hFileBlock.offset = buf.getLong();
          hFileBlock.nextBlockOnDiskSizeWithHeader = buf.getInt();
          if (hFileBlock.hasNextBlockHeader()) {
            hFileBlock.buf.limit(hFileBlock.buf.limit() - hFileBlock.headerSize());
          }
          return hFileBlock;
        }

        @Override
        public int getDeserialiserIdentifier() {
          return deserializerIdentifier;
        }

        @Override
        public HFileBlock deserialize(ByteBuff b) throws IOException {
          // Used only in tests
          return deserialize(b, false, MemoryType.EXCLUSIVE);
        }
      };
  private static final int deserializerIdentifier;
  static {
    deserializerIdentifier = CacheableDeserializerIdManager
        .registerDeserializer(blockDeserializer);
  }

  /** Type of block. Header field 0. */
  private BlockType blockType;

  /** Size on disk excluding header, including checksum. Header field 1. */
  private int onDiskSizeWithoutHeader;

  /** Size of pure data. Does not include header or checksums. Header field 2. */
  private final int uncompressedSizeWithoutHeader;

  /** The offset of the previous block on disk. Header field 3. */
  private final long prevBlockOffset;

  /**
   * Size on disk of header + data. Excludes checksum. Header field 6,
   * OR calculated from {@link #onDiskSizeWithoutHeader} when using HDFS checksum.
   */
  private final int onDiskDataSizeWithHeader;

  /** The in-memory representation of the hfile block */
  private ByteBuff buf;

  /** Meta data that holds meta information on the hfileblock */
  private HFileContext fileContext;

  /**
   * The offset of this block in the file. Populated by the reader for
   * convenience of access. This offset is not part of the block header.
   */
  private long offset = -1;

  /**
   * The on-disk size of the next block, including the header, obtained by
   * peeking into the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the next block's
   * header, or -1 if unknown.
   */
  private int nextBlockOnDiskSizeWithHeader = -1;

  private MemoryType memType = MemoryType.EXCLUSIVE;

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor
   * is mostly used when the block data has already been read and uncompressed,
   * and is sitting in a byte buffer.
   *
   * @param blockType the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader see {@link #onDiskSizeWithoutHeader}
   * @param uncompressedSizeWithoutHeader see {@link #uncompressedSizeWithoutHeader}
   * @param prevBlockOffset see {@link #prevBlockOffset}
   * @param buf block header ({@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes) followed by
   *          uncompressed data. This
   * @param fillHeader when true, parse {@code buf} and override the first 4 header fields.
   * @param offset the file offset the block was read from
   * @param onDiskDataSizeWithHeader see {@link #onDiskDataSizeWithHeader}
   * @param fileContext HFile meta data
   */
  HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
      long prevBlockOffset, ByteBuff buf, boolean fillHeader, long offset,
      int onDiskDataSizeWithHeader, HFileContext fileContext) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.buf = buf;
    this.offset = offset;
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    this.fileContext = fileContext;
    if (fillHeader)
      overwriteHeader();
    this.buf.rewind();
  }

  HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
      long prevBlockOffset, ByteBuffer buf, boolean fillHeader, long offset,
      int onDiskDataSizeWithHeader, HFileContext fileContext) {
    this(blockType, onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader, prevBlockOffset,
        new SingleByteBuff(buf), fillHeader, offset, onDiskDataSizeWithHeader, fileContext);
  }

  /**
   * Copy constructor. Creates a shallow copy of {@code that}'s buffer.
   */
  HFileBlock(HFileBlock that) {
    this.blockType = that.blockType;
    this.onDiskSizeWithoutHeader = that.onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = that.uncompressedSizeWithoutHeader;
    this.prevBlockOffset = that.prevBlockOffset;
    this.buf = that.buf.duplicate();
    this.offset = that.offset;
    this.onDiskDataSizeWithHeader = that.onDiskDataSizeWithHeader;
    this.fileContext = that.fileContext;
    this.nextBlockOnDiskSizeWithHeader = that.nextBlockOnDiskSizeWithHeader;
  }

  HFileBlock(ByteBuffer b, boolean usesHBaseChecksum) throws IOException {
    this(new SingleByteBuff(b), usesHBaseChecksum);
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   */
  HFileBlock(ByteBuff b, boolean usesHBaseChecksum) throws IOException {
    this(b, usesHBaseChecksum, MemoryType.EXCLUSIVE);
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   */
  HFileBlock(ByteBuff b, boolean usesHBaseChecksum, MemoryType memType) throws IOException {
    b.rewind();
    blockType = BlockType.read(b);
    onDiskSizeWithoutHeader = b.getInt();
    uncompressedSizeWithoutHeader = b.getInt();
    prevBlockOffset = b.getLong();
    HFileContextBuilder contextBuilder = new HFileContextBuilder();
    contextBuilder.withHBaseCheckSum(usesHBaseChecksum);
    if (usesHBaseChecksum) {
      contextBuilder.withChecksumType(ChecksumType.codeToType(b.get()));
      contextBuilder.withBytesPerCheckSum(b.getInt());
      this.onDiskDataSizeWithHeader = b.getInt();
    } else {
      contextBuilder.withChecksumType(ChecksumType.NULL);
      contextBuilder.withBytesPerCheckSum(0);
      this.onDiskDataSizeWithHeader = onDiskSizeWithoutHeader +
                                       HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    }
    this.fileContext = contextBuilder.build();
    this.memType = memType;
    buf = b;
    buf.rewind();
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
  public int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /**
   * @return the uncompressed size of data part (header and checksum excluded).
   */
   public int getUncompressedSizeWithoutHeader() {
    return uncompressedSizeWithoutHeader;
  }

  /**
   * @return the offset of the previous block of the same type in the file, or
   *         -1 if unknown
   */
  public long getPrevBlockOffset() {
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
    ByteBuff dup = this.buf.duplicate();
    dup.position(headerSize());
    dup.limit(buf.limit() - totalChecksumBytes());
    return dup.slice();
  }

  /**
   * Returns the buffer this block stores internally. The clients must not
   * modify the buffer object. This method has to be public because it is used
   * in {@link CompoundBloomFilter} to avoid object creation on every Bloom
   * filter lookup, but has to be used with caution. Checksum data is not
   * included in the returned buffer but header data is.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuff getBufferReadOnly() {
    ByteBuff dup = this.buf.duplicate();
    dup.limit(buf.limit() - totalChecksumBytes());
    return dup.slice();
  }

  /**
   * Returns the buffer of this block, including header data. The clients must
   * not modify the buffer object. This method has to be public because it is
   * used in {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache} to avoid buffer copy.
   *
   * @return the buffer with header and checksum included for read-only operations
   */
  public ByteBuff getBufferReadOnlyWithHeader() {
    ByteBuff dup = this.buf.duplicate();
    return dup.slice();
  }

  /**
   * Returns a byte buffer of this block, including header data and checksum, positioned at
   * the beginning of header. The underlying data array is not copied.
   *
   * @return the byte buffer with header and checksum included
   */
  ByteBuff getBufferWithHeader() {
    ByteBuff dupBuf = buf.duplicate();
    dupBuf.rewind();
    return dupBuf;
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
   */
  void sanityCheck() throws IOException {
    buf.rewind();

    sanityCheckAssertion(BlockType.read(buf), blockType);

    sanityCheckAssertion(buf.getInt(), onDiskSizeWithoutHeader,
        "onDiskSizeWithoutHeader");

    sanityCheckAssertion(buf.getInt(), uncompressedSizeWithoutHeader,
        "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(buf.getLong(), prevBlockOffset, "prevBlocKOffset");
    if (this.fileContext.isUseHBaseChecksum()) {
      sanityCheckAssertion(buf.get(), this.fileContext.getChecksumType().getCode(), "checksumType");
      sanityCheckAssertion(buf.getInt(), this.fileContext.getBytesPerChecksum(),
          "bytesPerChecksum");
      sanityCheckAssertion(buf.getInt(), onDiskDataSizeWithHeader, "onDiskDataSizeWithHeader");
    }

    int cksumBytes = totalChecksumBytes();
    int expectedBufLimit = onDiskDataSizeWithHeader + cksumBytes;
    if (buf.limit() != expectedBufLimit) {
      throw new AssertionError("Expected buffer limit " + expectedBufLimit
          + ", got " + buf.limit());
    }

    // We might optionally allocate HFILEBLOCK_HEADER_SIZE more bytes to read the next
    // block's header, so there are two sensible values for buffer capacity.
    int hdrSize = headerSize();
    if (buf.capacity() != expectedBufLimit &&
        buf.capacity() != expectedBufLimit + hdrSize) {
      throw new AssertionError("Invalid buffer capacity: " + buf.capacity() +
          ", expected " + expectedBufLimit + " or " + (expectedBufLimit + hdrSize));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
      .append("HFileBlock [")
      .append(" fileOffset=").append(offset)
      .append(" headerSize()=").append(headerSize())
      .append(" blockType=").append(blockType)
      .append(" onDiskSizeWithoutHeader=").append(onDiskSizeWithoutHeader)
      .append(" uncompressedSizeWithoutHeader=").append(uncompressedSizeWithoutHeader)
      .append(" prevBlockOffset=").append(prevBlockOffset)
      .append(" isUseHBaseChecksum()=").append(fileContext.isUseHBaseChecksum());
    if (fileContext.isUseHBaseChecksum()) {
      sb.append(" checksumType=").append(ChecksumType.codeToType(this.buf.get(24)))
        .append(" bytesPerChecksum=").append(this.buf.getInt(24 + 1))
        .append(" onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader);
    } else {
      sb.append(" onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader)
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
    sb.append(" getOnDiskSizeWithHeader()=").append(getOnDiskSizeWithHeader())
      .append(" totalChecksumBytes()=").append(totalChecksumBytes())
      .append(" isUnpacked()=").append(isUnpacked())
      .append(" buf=[ ").append(buf).append(" ]")
      .append(" dataBeginsWith=").append(dataBegin)
      .append(" fileContext=").append(fileContext)
      .append(" ]");
    return sb.toString();
  }

  /**
   * Called after reading a block with provided onDiskSizeWithHeader.
   */
  private void validateOnDiskSizeWithoutHeader(int expectedOnDiskSizeWithoutHeader)
  throws IOException {
    if (onDiskSizeWithoutHeader != expectedOnDiskSizeWithoutHeader) {
      String dataBegin = null;
      if (buf.hasArray()) {
        dataBegin = Bytes.toStringBinary(buf.array(), buf.arrayOffset(), Math.min(32, buf.limit()));
      } else {
        ByteBuff bufDup = getBufferReadOnly();
        byte[] dataBeginBytes = new byte[Math.min(32, bufDup.limit() - bufDup.position())];
        bufDup.get(dataBeginBytes);
        dataBegin = Bytes.toStringBinary(dataBeginBytes);
      }
      String blockInfoMsg =
        "Block offset: " + offset + ", data starts with: " + dataBegin;
      throw new IOException("On-disk size without header provided is "
          + expectedOnDiskSizeWithoutHeader + ", but block "
          + "header contains " + onDiskSizeWithoutHeader + ". " +
          blockInfoMsg);
    }
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

    // Preserve the next block's header bytes in the new block if we have them.
    if (unpacked.hasNextBlockHeader()) {
      // Both the buffers are limited till checksum bytes and avoid the next block's header.
      // Below call to copyFromBufferToBuffer() will try positional read/write from/to buffers when
      // any of the buffer is DBB. So we change the limit on a dup buffer. No copying just create
      // new BB objects
      ByteBuff inDup = this.buf.duplicate();
      inDup.limit(inDup.limit() + headerSize());
      ByteBuff outDup = unpacked.buf.duplicate();
      outDup.limit(outDup.limit() + unpacked.headerSize());
      outDup.put(
          unpacked.headerSize() + unpacked.uncompressedSizeWithoutHeader
              + unpacked.totalChecksumBytes(), inDup, this.onDiskDataSizeWithHeader,
          unpacked.headerSize());
    }
    return unpacked;
  }

  /**
   * Return true when this buffer includes next block's header.
   */
  private boolean hasNextBlockHeader() {
    return nextBlockOnDiskSizeWithHeader > 0;
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes
   * from the existing buffer. Does not change header fields.
   * Reserve room to keep checksum bytes too.
   */
  private void allocateBuffer() {
    int cksumBytes = totalChecksumBytes();
    int headerSize = headerSize();
    int capacityNeeded = headerSize + uncompressedSizeWithoutHeader +
        cksumBytes + (hasNextBlockHeader() ? headerSize : 0);

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
  public void assumeUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader +
        totalChecksumBytes()) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader
          + ", numChecksumbytes=" + totalChecksumBytes());
    }
  }

  /**
   * @param expectedType the expected type of this block
   * @throws IOException if this block's type is different than expected
   */
  public void expectType(BlockType expectedType) throws IOException {
    if (blockType != expectedType) {
      throw new IOException("Invalid block type: expected=" + expectedType
          + ", actual=" + blockType);
    }
  }

  /** @return the offset of this block in the file it was read from */
  public long getOffset() {
    if (offset < 0) {
      throw new IllegalStateException(
          "HFile block offset not initialized properly");
    }
    return offset;
  }

  /**
   * @return a byte stream reading the data + checksum of this block
   */
  public DataInputStream getByteStream() {
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
   * Read from an input stream. Analogous to
   * {@link IOUtils#readFully(InputStream, byte[], int, int)}, but specifies a
   * number of "extra" bytes that would be desirable but not absolutely
   * necessary to read.
   *
   * @param in the input stream to read from
   * @param buf the buffer to read into
   * @param bufOffset the destination offset in the buffer
   * @param necessaryLen the number of bytes that are absolutely necessary to
   *          read
   * @param extraLen the number of extra bytes that would be nice to read
   * @return true if succeeded reading the extra bytes
   * @throws IOException if failed to read the necessary bytes
   */
  public static boolean readWithExtra(InputStream in, byte[] buf,
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
   * Read from an input stream. Analogous to
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
   * @return the on-disk size of the next block (including the header size)
   *         that was read by peeking into the next block's header
   */
  public int getNextBlockOnDiskSizeWithHeader() {
    return nextBlockOnDiskSizeWithHeader;
  }

  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern
   * is as follows:
   * <ol>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression algorithm.
   * <li>Call {@link Writer#startWriting} and get a data stream to write to.
   * <li>Write your data into the stream.
   * <li>Call {@link Writer#writeHeaderAndData(FSDataOutputStream)} as many times as you need to.
   * store the serialized block into an external stream.
   * <li>Repeat to write more blocks.
   * </ol>
   * <p>
   */
  public static class Writer {

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

    /** block encoding context for non-data blocks */
    private HFileBlockDefaultEncodingContext defaultBlockEncodingCtx;

    /**
     * The stream we use to accumulate data in uncompressed format for each
     * block. We reset this stream at the end of each block and reuse it. The
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
    private byte[] onDiskBytesWithHeader;

    /**
     * The size of the checksum data on disk. It is used only if data is
     * not compressed. If data is compressed, then the checksums are already
     * part of onDiskBytesWithHeader. If data is uncompressed, then this
     * variable stores the checksum data for this block.
     */
    private byte[] onDiskChecksum;

    /**
     * Valid in the READY state. Contains the header and the uncompressed (but
     * potentially encoded, if this is a data block) bytes, so the length is
     * {@link #uncompressedSizeWithoutHeader} +
     * {@link org.apache.hadoop.hbase.HConstants#HFILEBLOCK_HEADER_SIZE}.
     * Does not store checksums.
     */
    private byte[] uncompressedBytesWithHeader;

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
      this.dataBlockEncoder = dataBlockEncoder != null
          ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;
      defaultBlockEncodingCtx = new HFileBlockDefaultEncodingContext(null,
          HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      dataBlockEncodingCtx = this.dataBlockEncoder
          .newDataBlockEncodingContext(HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);

      if (fileContext.getBytesPerChecksum() < HConstants.HFILEBLOCK_HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " +
            " Minimum is " + HConstants.HFILEBLOCK_HEADER_SIZE + " but the configured value is " +
            fileContext.getBytesPerChecksum());
      }

      baosInMemory = new ByteArrayOutputStream();

      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i)
        prevOffsetByType[i] = -1;

      this.fileContext = fileContext;
    }

    /**
     * Starts writing into the block. The previous block's data is discarded.
     *
     * @return the stream the user can write their data into
     * @throws IOException
     */
    public DataOutputStream startWriting(BlockType newBlockType)
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
      userDataStream = new ByteBufferSupportDataOutputStream(baosInMemory);
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
    public void write(Cell cell) throws IOException{
      expectState(State.WRITING);
      this.unencodedDataSizeWritten += this.dataBlockEncoder.encode(cell, dataBlockEncodingCtx,
          this.userDataStream);
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

      if (state == State.BLOCK_READY)
        return;

      // This will set state to BLOCK_READY.
      finishBlock();
    }

    /**
     * An internal method that flushes the compressing stream (if using
     * compression), serializes the header, and takes care of the separate
     * uncompressed stream for caching on write, if applicable. Sets block
     * write state to "block ready".
     */
    private void finishBlock() throws IOException {
      if (blockType == BlockType.DATA) {
        this.dataBlockEncoder.endBlockEncoding(dataBlockEncodingCtx, userDataStream,
            baosInMemory.getBuffer(), blockType);
        blockType = dataBlockEncodingCtx.getBlockType();
      }
      userDataStream.flush();
      // This does an array copy, so it is safe to cache this byte array.
      uncompressedBytesWithHeader = baosInMemory.toByteArray();
      prevOffset = prevOffsetByType[blockType.getId()];

      // We need to set state before we can package the block up for
      // cache-on-write. In a way, the block is ready, but not yet encoded or
      // compressed.
      state = State.BLOCK_READY;
      if (blockType == BlockType.DATA || blockType == BlockType.ENCODED_DATA) {
        onDiskBytesWithHeader = dataBlockEncodingCtx
            .compressAndEncrypt(uncompressedBytesWithHeader);
      } else {
        onDiskBytesWithHeader = defaultBlockEncodingCtx
            .compressAndEncrypt(uncompressedBytesWithHeader);
      }
      int numBytes = (int) ChecksumUtil.numBytes(
          onDiskBytesWithHeader.length,
          fileContext.getBytesPerChecksum());

      // put the header for on disk bytes
      putHeader(onDiskBytesWithHeader, 0,
          onDiskBytesWithHeader.length + numBytes,
          uncompressedBytesWithHeader.length, onDiskBytesWithHeader.length);
      // set the header for the uncompressed bytes (for cache-on-write)
      putHeader(uncompressedBytesWithHeader, 0,
          onDiskBytesWithHeader.length + numBytes,
          uncompressedBytesWithHeader.length, onDiskBytesWithHeader.length);

      onDiskChecksum = new byte[numBytes];
      ChecksumUtil.generateChecksums(
          onDiskBytesWithHeader, 0, onDiskBytesWithHeader.length,
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
    public void writeHeaderAndData(FSDataOutputStream out) throws IOException {
      long offset = out.getPos();
      if (startOffset != -1 && offset != startOffset) {
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
      out.write(onDiskBytesWithHeader);
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
          new byte[onDiskBytesWithHeader.length
              + onDiskChecksum.length];
      System.arraycopy(onDiskBytesWithHeader, 0, output, 0,
          onDiskBytesWithHeader.length);
      System.arraycopy(onDiskChecksum, 0, output,
          onDiskBytesWithHeader.length, onDiskChecksum.length);
      return output;
    }

    /**
     * Releases resources used by this writer.
     */
    public void release() {
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
      return onDiskBytesWithHeader.length
          + onDiskChecksum.length
          - HConstants.HFILEBLOCK_HEADER_SIZE;
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
      return onDiskBytesWithHeader.length + onDiskChecksum.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedBytesWithHeader.length - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    int getUncompressedSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedBytesWithHeader.length;
    }

    /** @return true if a block is being written  */
    public boolean isWriting() {
      return state == State.WRITING;
    }

    /**
     * Returns the number of bytes written into the current block so far, or
     * zero if not writing the block at the moment. Note that this will return
     * zero in the "block ready" state as well.
     *
     * @return the number of bytes written
     */
    public int blockSizeWritten() {
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
      return ByteBuffer.wrap(uncompressedBytesWithHeader);
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
      return ByteBuffer.wrap(onDiskBytesWithHeader);
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
    public void writeBlock(BlockWritable bw, FSDataOutputStream out)
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
     */
    public HFileBlock getBlockForCaching(CacheConfig cacheConf) {
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
          cacheConf.shouldCacheCompressed(blockType.getCategory()) ?
            getOnDiskBufferWithHeader() :
            getUncompressedBufferWithHeader(),
          FILL_HEADER, startOffset,
          onDiskBytesWithHeader.length + onDiskChecksum.length, newContext);
    }
  }

  /** Something that can be written into a block. */
  public interface BlockWritable {

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
  public interface BlockIterator {

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
  public interface FSReader {

    /**
     * Reads the block at the given offset in the file with the given on-disk
     * size and uncompressed size.
     *
     * @param offset
     * @param onDiskSize the on-disk size of the entire block, including all
     *          applicable headers, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the compressed part of
     *          the block, or -1 if unknown
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize,
        int uncompressedSize, boolean pread) throws IOException;

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
   * We always prefetch the header of the next block, so that we know its
   * on-disk size in advance and can read it in one operation.
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
    final ByteBuffer buf = ByteBuffer.wrap(header, 0, HConstants.HFILEBLOCK_HEADER_SIZE);
  }

  /** Reads version 2 blocks from the filesystem. */
  static class FSReaderImpl implements FSReader {
    /** The file system stream of the underlying {@link HFile} that
     * does or doesn't do checksum validations in the filesystem */
    protected FSDataInputStreamWrapper streamWrapper;

    private HFileBlockDecodingContext encodedBlockDecodingCtx;

    /** Default context used when BlockType != {@link BlockType#ENCODED_DATA}. */
    private final HFileBlockDefaultDecodingContext defaultDecodingCtx;

    private ThreadLocal<PrefetchedHeader> prefetchedHeaderForThread =
        new ThreadLocal<PrefetchedHeader>() {
      @Override
      public PrefetchedHeader initialValue() {
        return new PrefetchedHeader();
      }
    };

    /** Compression algorithm used by the {@link HFile} */

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

    public FSReaderImpl(FSDataInputStreamWrapper stream, long fileSize, HFileSystem hfs, Path path,
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

        @Override
        public HFileBlock nextBlock() throws IOException {
          if (offset >= endOffset)
            return null;
          HFileBlock b = readBlockData(offset, -1, -1, false);
          offset += b.getOnDiskSizeWithHeader();
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
     * the on-disk size of the next block, or -1 if it could not be determined.
     *
     * @param dest destination buffer
     * @param destOffset offset in the destination buffer
     * @param size size of the block to be read
     * @param peekIntoNextBlock whether to read the next block's on-disk size
     * @param fileOffset position in the stream to read at
     * @param pread whether we should do a positional read
     * @param istream The input source of data
     * @return the on-disk size of the next block with header size included, or
     *         -1 if it could not be determined
     * @throws IOException
     */
    protected int readAtOffset(FSDataInputStream istream,
        byte[] dest, int destOffset, int size,
        boolean peekIntoNextBlock, long fileOffset, boolean pread)
        throws IOException {
      if (peekIntoNextBlock &&
          destOffset + size + hdrSize > dest.length) {
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
          if (!readWithExtra(istream, dest, destOffset, size, hdrSize))
            return -1;
        } finally {
          streamLock.unlock();
        }
      } else {
        // Positional read. Better for random reads; or when the streamLock is already locked.
        int extraSize = peekIntoNextBlock ? hdrSize : 0;
        if (!positionalReadWithExtra(istream, fileOffset, dest, destOffset,
            size, extraSize)) {
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
     *          the header, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the the block. Always
     *          expected to be -1. This parameter is only used in version 1.
     * @param pread whether to use a positional read
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL,
        int uncompressedSize, boolean pread)
    throws IOException {

      // get a copy of the current state of whether to validate
      // hbase checksums or not for this read call. This is not
      // thread-safe but the one constaint is that if we decide
      // to skip hbase checksum verification then we are
      // guaranteed to use hdfs checksum verification.
      boolean doVerificationThruHBaseChecksum = streamWrapper.shouldUseHBaseChecksum();
      FSDataInputStream is = streamWrapper.getStream(doVerificationThruHBaseChecksum);

      HFileBlock blk = readBlockDataInternal(is, offset,
                         onDiskSizeWithHeaderL,
                         uncompressedSize, pread,
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
        HFile.checksumFailures.incrementAndGet(); // update metrics

        // If we have a checksum failure, we fall back into a mode where
        // the next few reads use HDFS level checksums. We aim to make the
        // next CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD reads avoid
        // hbase checksum verification, but since this value is set without
        // holding any locks, it can so happen that we might actually do
        // a few more than precisely this number.
        is = this.streamWrapper.fallbackToFsChecksum(CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD);
        doVerificationThruHBaseChecksum = false;
        blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL,
                                    uncompressedSize, pread,
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
     * Reads a version 2 block.
     *
     * @param offset the offset in the stream to read at
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including
     *          the header, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the the block. Always
     *          expected to be -1. This parameter is only used in version 1.
     * @param pread whether to use a positional read
     * @param verifyChecksum Whether to use HBase checksums.
     *        If HBase checksum is switched off, then use HDFS checksum.
     * @return the HFileBlock or null if there is a HBase checksum mismatch
     */
    private HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
        long onDiskSizeWithHeaderL, int uncompressedSize, boolean pread,
        boolean verifyChecksum)
    throws IOException {
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read "
            + "block (onDiskSize=" + onDiskSizeWithHeaderL
            + ", uncompressedSize=" + uncompressedSize + ")");
      }

      if (uncompressedSize != -1) {
        throw new IOException("Version 2 block reader API does not need " +
            "the uncompressed size parameter");
      }

      if ((onDiskSizeWithHeaderL < hdrSize && onDiskSizeWithHeaderL != -1)
          || onDiskSizeWithHeaderL >= Integer.MAX_VALUE) {
        throw new IOException("Invalid onDisksize=" + onDiskSizeWithHeaderL
            + ": expected to be at least " + hdrSize
            + " and at most " + Integer.MAX_VALUE + ", or -1 (offset="
            + offset + ", uncompressedSize=" + uncompressedSize + ")");
      }

      int onDiskSizeWithHeader = (int) onDiskSizeWithHeaderL;
      // See if we can avoid reading the header. This is desirable, because
      // we will not incur a backward seek operation if we have already
      // read this block's header as part of the previous read's look-ahead.
      // And we also want to skip reading the header again if it has already
      // been read.
      // TODO: How often does this optimization fire? Has to be same thread so the thread local
      // is pertinent and we have to be reading next block as in a big scan.
      PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
      ByteBuffer headerBuf = prefetchedHeader.offset == offset? prefetchedHeader.buf: null;

      // Allocate enough space to fit the next block's header too.
      int nextBlockOnDiskSize = 0;
      byte[] onDiskBlock = null;

      HFileBlock b = null;
      if (onDiskSizeWithHeader > 0) {
        // We know the total on-disk size. Read the entire block into memory,
        // then parse the header. This code path is used when
        // doing a random read operation relying on the block index, as well as
        // when the client knows the on-disk size from peeking into the next
        // block's header (e.g. this block's header) when reading the previous
        // block. This is the faster and more preferable case.

        // Size that we have to skip in case we have already read the header.
        int preReadHeaderSize = headerBuf == null ? 0 : hdrSize;
        onDiskBlock = new byte[onDiskSizeWithHeader + hdrSize]; // room for this block plus the
                                                                // next block's header
        nextBlockOnDiskSize = readAtOffset(is, onDiskBlock,
            preReadHeaderSize, onDiskSizeWithHeader - preReadHeaderSize,
            true, offset + preReadHeaderSize, pread);
        if (headerBuf != null) {
          // the header has been read when reading the previous block, copy
          // to this block's header
          // headerBuf is HBB
          assert headerBuf.hasArray();
          System.arraycopy(headerBuf.array(),
              headerBuf.arrayOffset(), onDiskBlock, 0, hdrSize);
        } else {
          headerBuf = ByteBuffer.wrap(onDiskBlock, 0, hdrSize);
        }
        // We know the total on-disk size but not the uncompressed size. Parse the header.
        try {
          // TODO: FIX!!! Expensive parse just to get a length
          b = new HFileBlock(headerBuf, fileContext.isUseHBaseChecksum());
        } catch (IOException ex) {
          // Seen in load testing. Provide comprehensive debug info.
          throw new IOException("Failed to read compressed block at "
              + offset
              + ", onDiskSizeWithoutHeader="
              + onDiskSizeWithHeader
              + ", preReadHeaderSize="
              + hdrSize
              + ", header.length="
              + prefetchedHeader.header.length
              + ", header bytes: "
              + Bytes.toStringBinary(prefetchedHeader.header, 0,
                  hdrSize), ex);
        }
        // if the caller specifies a onDiskSizeWithHeader, validate it.
        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - hdrSize;
        assert onDiskSizeWithoutHeader >= 0;
        b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
      } else {
        // Check headerBuf to see if we have read this block's header as part of
        // reading the previous block. This is an optimization of peeking into
        // the next block's header (e.g.this block's header) when reading the
        // previous block. This is the faster and more preferable case. If the
        // header is already there, don't read the header again.

        // Unfortunately, we still have to do a separate read operation to
        // read the header.
        if (headerBuf == null) {
          // From the header, determine the on-disk size of the given hfile
          // block, and read the remaining data, thereby incurring two read
          // operations. This might happen when we are doing the first read
          // in a series of reads or a random read, and we don't have access
          // to the block index. This is costly and should happen very rarely.
          headerBuf = ByteBuffer.allocate(hdrSize);
          // headerBuf is HBB
          readAtOffset(is, headerBuf.array(), headerBuf.arrayOffset(),
              hdrSize, false, offset, pread);
        }
        // TODO: FIX!!! Expensive parse just to get a length
        b = new HFileBlock(headerBuf, fileContext.isUseHBaseChecksum());
        onDiskBlock = new byte[b.getOnDiskSizeWithHeader() + hdrSize];
        // headerBuf is HBB
        System.arraycopy(headerBuf.array(), headerBuf.arrayOffset(), onDiskBlock, 0, hdrSize);
        nextBlockOnDiskSize =
          readAtOffset(is, onDiskBlock, hdrSize, b.getOnDiskSizeWithHeader()
              - hdrSize, true, offset + hdrSize, pread);
        onDiskSizeWithHeader = b.onDiskSizeWithoutHeader + hdrSize;
      }

      if (!fileContext.isCompressedOrEncrypted()) {
        b.assumeUncompressed();
      }

      if (verifyChecksum && !validateBlockChecksum(b, offset, onDiskBlock, hdrSize)) {
        return null;             // checksum mismatch
      }

      // The onDiskBlock will become the headerAndDataBuffer for this block.
      // If nextBlockOnDiskSizeWithHeader is not zero, the onDiskBlock already
      // contains the header of next block, so no need to set next
      // block's header in it.
      b = new HFileBlock(ByteBuffer.wrap(onDiskBlock, 0, onDiskSizeWithHeader),
        this.fileContext.isUseHBaseChecksum());

      b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSize;

      // Set prefetched header
      if (b.hasNextBlockHeader()) {
        prefetchedHeader.offset = offset + b.getOnDiskSizeWithHeader();
        System.arraycopy(onDiskBlock, onDiskSizeWithHeader, prefetchedHeader.header, 0, hdrSize);
      }

      b.offset = offset;
      b.fileContext.setIncludesTags(this.fileContext.isIncludesTags());
      b.fileContext.setIncludesMvcc(this.fileContext.isIncludesMvcc());
      return b;
    }

    public void setIncludesMemstoreTS(boolean includesMemstoreTS) {
      this.fileContext.setIncludesMvcc(includesMemstoreTS);
    }

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
     * Generates the checksum for the header as well as the data and
     * then validates that it matches the value stored in the header.
     * If there is a checksum mismatch, then return false. Otherwise
     * return true.
     */
    protected boolean validateBlockChecksum(HFileBlock block, long offset, byte[] data,
        int hdrSize)
    throws IOException {
      return ChecksumUtil.validateBlockChecksum(pathName, offset, block, data, hdrSize);
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

  @Override
  public int getSerializedLength() {
    if (buf != null) {
      // include extra bytes for the next header when it's available.
      int extraSpace = hasNextBlockHeader() ? headerSize() : 0;
      return this.buf.limit() + extraSpace + HFileBlock.EXTRA_SERIALIZATION_SPACE;
    }
    return 0;
  }

  @Override
  public void serialize(ByteBuffer destination) {
    this.buf.get(destination, 0, getSerializedLength()
        - EXTRA_SERIALIZATION_SPACE);
    serializeExtraInfo(destination);
  }

  public void serializeExtraInfo(ByteBuffer destination) {
    destination.put(this.fileContext.isUseHBaseChecksum() ? (byte) 1 : (byte) 0);
    destination.putLong(this.offset);
    destination.putInt(this.nextBlockOnDiskSizeWithHeader);
    destination.rewind();
  }

  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
    return HFileBlock.blockDeserializer;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + blockType.hashCode();
    result = result * 31 + nextBlockOnDiskSizeWithHeader;
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
    if (castedComparison.nextBlockOnDiskSizeWithHeader != this.nextBlockOnDiskSizeWithHeader) {
      return false;
    }
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
   * Calcuate the number of bytes required to store all the checksums
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
    if (usesHBaseChecksum) {
      return HConstants.HFILEBLOCK_HEADER_SIZE;
    }
    return HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  public byte[] getDummyHeaderForVersion() {
    return getDummyHeaderForVersion(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  static private byte[] getDummyHeaderForVersion(boolean usesHBaseChecksum) {
    if (usesHBaseChecksum) {
      return HConstants.HFILEBLOCK_DUMMY_HEADER;
    }
    return DUMMY_HEADER_NO_CHECKSUM;
  }

  /**
   * @return the HFileContext used to create this HFileBlock. Not necessary the
   * fileContext for the file from which this block's data was originally read.
   */
  public HFileContext getHFileContext() {
    return this.fileContext;
  }

  @Override
  public MemoryType getMemoryType() {
    return this.memType;
  }

  /**
   * @return true if this block is backed by a shared memory area(such as that of a BucketCache).
   */
  public boolean usesSharedMemory() {
    return this.memType == MemoryType.SHARED;
  }

  /**
   * Convert the contents of the block header into a human readable string.
   * This is mostly helpful for debugging. This assumes that the block
   * has minor version > 0.
   */
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
}
