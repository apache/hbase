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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;
import org.apache.hadoop.io.IOUtils;

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
 * <li>Magic record identifying the block type (8 bytes)
 * <li>Compressed block size, header not included (4 bytes)
 * <li>Uncompressed block size, header not included (4 bytes)
 * <li>The offset of the previous block of the same type (8 bytes). This is
 * used to be able to navigate to the previous block without going to the block
 * <li>For minorVersions >=1, there is an additional 4 byte field 
 * bytesPerChecksum that records the number of bytes in a checksum chunk.
 * <li>For minorVersions >=1, there is a 4 byte value to store the size of
 * data on disk (excluding the checksums)
 * <li>For minorVersions >=1, a series of 4 byte checksums, one each for
 * the number of bytes specified by bytesPerChecksum.
 * index.
 * <li>Compressed data (or uncompressed data if compression is disabled). The
 * compression algorithm is the same for all the blocks in the {@link HFile},
 * similarly to what was done in version 1.
 * </ul>
 * </ul>
 * The version 2 block representation in the block cache is the same as above,
 * except that the data section is always uncompressed in the cache.
 */
@InterfaceAudience.Private
public class HFileBlock implements Cacheable {

  /** Minor versions starting with this number have hbase checksums */
  static final int MINOR_VERSION_WITH_CHECKSUM = 1;

  /** minor version that does not support checksums */
  static final int MINOR_VERSION_NO_CHECKSUM = 0;

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

  public static final int BYTE_BUFFER_HEAP_SIZE = (int) ClassSize.estimateBase(
      ByteBuffer.wrap(new byte[0], 0, 0).getClass(), false);

  // minorVersion+offset+nextBlockOnDiskSizeWithHeader
  public static final int EXTRA_SERIALIZATION_SPACE = 2 * Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  private static final CacheableDeserializer<Cacheable> blockDeserializer =
      new CacheableDeserializer<Cacheable>() {
        public HFileBlock deserialize(ByteBuffer buf, boolean reuse) throws IOException{
          buf.limit(buf.limit() - HFileBlock.EXTRA_SERIALIZATION_SPACE).rewind();
          ByteBuffer newByteBuffer;
          if (reuse) {
            newByteBuffer = buf.slice();
          } else {
           newByteBuffer = ByteBuffer.allocate(buf.limit());
           newByteBuffer.put(buf);
          }
          buf.position(buf.limit());
          buf.limit(buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE);
          int minorVersion=buf.getInt();
          HFileBlock ourBuffer = new HFileBlock(newByteBuffer, minorVersion);
          ourBuffer.offset = buf.getLong();
          ourBuffer.nextBlockOnDiskSizeWithHeader = buf.getInt();
          return ourBuffer;
        }
        
        @Override
        public int getDeserialiserIdentifier() {
          return deserializerIdentifier;
        }

        @Override
        public HFileBlock deserialize(ByteBuffer b) throws IOException {
          return deserialize(b, false);
        }
      };
  private static final int deserializerIdentifier;
  static {
    deserializerIdentifier = CacheableDeserializerIdManager
        .registerDeserializer(blockDeserializer);
  }

  private BlockType blockType;

  /** Size on disk without the header. It includes checksum data too. */
  private int onDiskSizeWithoutHeader;

  /** Size of pure data. Does not include header or checksums */
  private final int uncompressedSizeWithoutHeader;

  /** The offset of the previous block on disk */
  private final long prevBlockOffset;

  /** The Type of checksum, better to store the byte than an object */
  private final byte checksumType;

  /** The number of bytes for which a checksum is computed */
  private final int bytesPerChecksum;

  /** Size on disk of header and data. Does not include checksum data */
  private final int onDiskDataSizeWithHeader;

  /** The minor version of the hfile. */
  private final int minorVersion;

  /** The in-memory representation of the hfile block */
  private ByteBuffer buf;

  /** Whether there is a memstore timestamp after every key/value */
  private boolean includesMemstoreTS;

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

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor
   * is mostly used when the block data has already been read and uncompressed,
   * and is sitting in a byte buffer. 
   *
   * @param blockType the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader compressed size of the block if compression
   *          is used, otherwise uncompressed size, header size not included
   * @param uncompressedSizeWithoutHeader uncompressed size of the block,
   *          header size not included. Equals onDiskSizeWithoutHeader if
   *          compression is disabled.
   * @param prevBlockOffset the offset of the previous block in the
   *          {@link HFile}
   * @param buf block header ({@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes) followed by
   *          uncompressed data. This
   * @param fillHeader true to fill in the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of
   *          the buffer based on the header fields provided
   * @param offset the file offset the block was read from
   * @param minorVersion the minor version of this block
   * @param bytesPerChecksum the number of bytes per checksum chunk
   * @param checksumType the checksum algorithm to use
   * @param onDiskDataSizeWithHeader size of header and data on disk not
   *        including checksum data
   */
  HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
      int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuffer buf,
      boolean fillHeader, long offset, boolean includesMemstoreTS, 
      int minorVersion, int bytesPerChecksum, byte checksumType,
      int onDiskDataSizeWithHeader) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.buf = buf;
    if (fillHeader)
      overwriteHeader();
    this.offset = offset;
    this.includesMemstoreTS = includesMemstoreTS;
    this.minorVersion = minorVersion;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point. The reason this has a minorNumber and not a majorNumber is
   * because majorNumbers indicate the format of a HFile whereas minorNumbers 
   * indicate the format inside a HFileBlock.
   */
  HFileBlock(ByteBuffer b, int minorVersion) throws IOException {
    b.rewind();
    blockType = BlockType.read(b);
    onDiskSizeWithoutHeader = b.getInt();
    uncompressedSizeWithoutHeader = b.getInt();
    prevBlockOffset = b.getLong();
    this.minorVersion = minorVersion;
    if (minorVersion >= MINOR_VERSION_WITH_CHECKSUM) {
      this.checksumType = b.get();
      this.bytesPerChecksum = b.getInt();
      this.onDiskDataSizeWithHeader = b.getInt();
    } else {
      this.checksumType = ChecksumType.NULL.getCode();
      this.bytesPerChecksum = 0;
      this.onDiskDataSizeWithHeader = onDiskSizeWithoutHeader +
                                       HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    }
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
   * @return the on-disk size of the block with header size included. This
   * includes the header, the data and the checksum data.
   */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + headerSize();
  }

  /**
   * Returns the size of the compressed part of the block in case compression
   * is used, or the uncompressed size of the data part otherwise. Header size
   * and checksum data size is not included.
   *
   * @return the on-disk size of the data part of the block, header and
   *         checksum not included. 
   */
  public int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /**
   * @return the uncompressed size of the data part of the block, header not
   *         included
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
   * Writes header fields into the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the
   * buffer. Resets the buffer position to the end of header as side effect.
   */
  private void overwriteHeader() {
    buf.rewind();
    blockType.write(buf);
    buf.putInt(onDiskSizeWithoutHeader);
    buf.putInt(uncompressedSizeWithoutHeader);
    buf.putLong(prevBlockOffset);
  }

  /**
   * Returns a buffer that does not include the header. The array offset points
   * to the start of the block data right after the header. The underlying data
   * array is not copied. Checksum data is not included in the returned buffer.
   *
   * @return the buffer with header skipped
   */
  public ByteBuffer getBufferWithoutHeader() {
    return ByteBuffer.wrap(buf.array(), buf.arrayOffset() + headerSize(),
        buf.limit() - headerSize() - totalChecksumBytes()).slice();
  }

  /**
   * Returns the buffer this block stores internally. The clients must not
   * modify the buffer object. This method has to be public because it is
   * used in {@link CompoundBloomFilter} to avoid object creation on every
   * Bloom filter lookup, but has to be used with caution. Checksum data
   * is not included in the returned buffer.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuffer getBufferReadOnly() {
    return ByteBuffer.wrap(buf.array(), buf.arrayOffset(),
        buf.limit() - totalChecksumBytes()).slice();
  }

  /**
   * Returns the buffer of this block, including header data. The clients must
   * not modify the buffer object. This method has to be public because it is
   * used in {@link BucketCache} to avoid buffer copy.
   * 
   * @return the byte buffer with header included for read-only operations
   */
  public ByteBuffer getBufferReadOnlyWithHeader() {
    return ByteBuffer.wrap(buf.array(), buf.arrayOffset(), buf.limit()).slice();
  }

  /**
   * Returns a byte buffer of this block, including header data, positioned at
   * the beginning of header. The underlying data array is not copied.
   *
   * @return the byte buffer with header included
   */
  ByteBuffer getBufferWithHeader() {
    ByteBuffer dupBuf = buf.duplicate();
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

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the buffer contain a valid header consistent
   * with the fields. This function is primary for testing and debugging, and
   * is not thread-safe, because it alters the internal buffer pointer.
   */
  void sanityCheck() throws IOException {
    buf.rewind();

    {
      BlockType blockTypeFromBuf = BlockType.read(buf);
      if (blockTypeFromBuf != blockType) {
        throw new IOException("Block type stored in the buffer: " +
            blockTypeFromBuf + ", block type field: " + blockType);
      }
    }

    sanityCheckAssertion(buf.getInt(), onDiskSizeWithoutHeader,
        "onDiskSizeWithoutHeader");

    sanityCheckAssertion(buf.getInt(), uncompressedSizeWithoutHeader,
        "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(buf.getLong(), prevBlockOffset, "prevBlocKOffset");
    if (minorVersion >= MINOR_VERSION_WITH_CHECKSUM) {
      sanityCheckAssertion(buf.get(), checksumType, "checksumType");
      sanityCheckAssertion(buf.getInt(), bytesPerChecksum, "bytesPerChecksum");
      sanityCheckAssertion(buf.getInt(), onDiskDataSizeWithHeader, 
                           "onDiskDataSizeWithHeader");
    }

    int cksumBytes = totalChecksumBytes();
    int hdrSize = headerSize();
    int expectedBufLimit = uncompressedSizeWithoutHeader + headerSize() +
                           cksumBytes;
    if (buf.limit() != expectedBufLimit) {
      throw new AssertionError("Expected buffer limit " + expectedBufLimit
          + ", got " + buf.limit());
    }

    // We might optionally allocate HFILEBLOCK_HEADER_SIZE more bytes to read the next
    // block's, header, so there are two sensible values for buffer capacity.
    int size = uncompressedSizeWithoutHeader + hdrSize + cksumBytes;
    if (buf.capacity() != size &&
        buf.capacity() != size + hdrSize) {
      throw new AssertionError("Invalid buffer capacity: " + buf.capacity() +
          ", expected " + size + " or " + (size + hdrSize));
    }
  }

  @Override
  public String toString() {
    return "blockType="
        + blockType
        + ", onDiskSizeWithoutHeader="
        + onDiskSizeWithoutHeader
        + ", uncompressedSizeWithoutHeader="
        + uncompressedSizeWithoutHeader
        + ", prevBlockOffset="
        + prevBlockOffset
        + ", dataBeginsWith="
        + Bytes.toStringBinary(buf.array(), buf.arrayOffset() + headerSize(),
            Math.min(32, buf.limit() - buf.arrayOffset() - headerSize()))
        + ", fileOffset=" + offset;
  }

  private void validateOnDiskSizeWithoutHeader(
      int expectedOnDiskSizeWithoutHeader) throws IOException {
    if (onDiskSizeWithoutHeader != expectedOnDiskSizeWithoutHeader) {
      String blockInfoMsg =
        "Block offset: " + offset + ", data starts with: "
          + Bytes.toStringBinary(buf.array(), buf.arrayOffset(),
              buf.arrayOffset() + Math.min(32, buf.limit()));
      throw new IOException("On-disk size without header provided is "
          + expectedOnDiskSizeWithoutHeader + ", but block "
          + "header contains " + onDiskSizeWithoutHeader + ". " +
          blockInfoMsg);
    }
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes
   * from the existing buffer. Does not change header fields. 
   * Reserve room to keep checksum bytes too.
   *
   * @param extraBytes whether to reserve room in the buffer to read the next
   *          block's header
   */
  private void allocateBuffer(boolean extraBytes) {
    int cksumBytes = totalChecksumBytes();
    int capacityNeeded = headerSize() + uncompressedSizeWithoutHeader +
        cksumBytes +
        (extraBytes ? headerSize() : 0);

    ByteBuffer newBuf = ByteBuffer.allocate(capacityNeeded);

    // Copy header bytes.
    System.arraycopy(buf.array(), buf.arrayOffset(), newBuf.array(),
        newBuf.arrayOffset(), headerSize());

    buf = newBuf;
    buf.limit(headerSize() + uncompressedSizeWithoutHeader + cksumBytes);
  }

  /** An additional sanity-check in case no compression is being used. */
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
   * @return a byte stream reading the data section of this block
   */
  public DataInputStream getByteStream() {
    return new DataInputStream(new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + headerSize(), buf.limit() - headerSize()));
  }

  @Override
  public long heapSize() {
    long size = ClassSize.align(
        ClassSize.OBJECT +
        // Block type and byte buffer references
        2 * ClassSize.REFERENCE +
        // On-disk size, uncompressed size, and next block's on-disk size
        // bytePerChecksum,  onDiskDataSize and minorVersion
        6 * Bytes.SIZEOF_INT +
        // Checksum type
        1 * Bytes.SIZEOF_BYTE +
        // This and previous block offset
        2 * Bytes.SIZEOF_LONG +
        // "Include memstore timestamp" flag
        Bytes.SIZEOF_BOOLEAN
    );

    if (buf != null) {
      // Deep overhead of the byte buffer. Needs to be aligned separately.
      size += ClassSize.align(buf.capacity() + BYTE_BUFFER_HEAP_SIZE);
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
  public static boolean readWithExtra(InputStream in, byte buf[],
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
     * changed in {@link #encodeDataBlockForDisk()} from {@link BlockType#DATA}
     * to {@link BlockType#ENCODED_DATA}.
     */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to, which compresses them and
     * writes them to {@link #baosInMemory}.
     */
    private DataOutputStream userDataStream;

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
     * {@link #uncompressedSizeWithoutHeader} + {@link org.apache.hadoop.hbase.HConstants#HFILEBLOCK_HEADER_SIZE}.
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

    /** Whether we are including memstore timestamp after every key/value */
    private boolean includesMemstoreTS;

    /** Checksum settings */
    private ChecksumType checksumType;
    private int bytesPerChecksum;

    /**
     * @param compressionAlgorithm compression algorithm to use
     * @param dataBlockEncoder data block encoding algorithm to use
     * @param checksumType type of checksum
     * @param bytesPerChecksum bytes per checksum
     */
    public Writer(Compression.Algorithm compressionAlgorithm,
          HFileDataBlockEncoder dataBlockEncoder, boolean includesMemstoreTS,
          ChecksumType checksumType, int bytesPerChecksum) {
      this.dataBlockEncoder = dataBlockEncoder != null
          ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;
      defaultBlockEncodingCtx =
        new HFileBlockDefaultEncodingContext(compressionAlgorithm, null, HConstants.HFILEBLOCK_DUMMY_HEADER);
      dataBlockEncodingCtx =
        this.dataBlockEncoder.newDataBlockEncodingContext(
            compressionAlgorithm, HConstants.HFILEBLOCK_DUMMY_HEADER);

      if (bytesPerChecksum < HConstants.HFILEBLOCK_HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " +
            " Minimum is " + HConstants.HFILEBLOCK_HEADER_SIZE + " but the configured value is " +
            bytesPerChecksum);
      }

      baosInMemory = new ByteArrayOutputStream();
      
      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i)
        prevOffsetByType[i] = -1;

      this.includesMemstoreTS = includesMemstoreTS;
      this.checksumType = checksumType;
      this.bytesPerChecksum = bytesPerChecksum;
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
      userDataStream = new DataOutputStream(baosInMemory);
      return userDataStream;
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
    private void ensureBlockReady() throws IOException {
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
      userDataStream.flush();
      // This does an array copy, so it is safe to cache this byte array.
      uncompressedBytesWithHeader = baosInMemory.toByteArray();
      prevOffset = prevOffsetByType[blockType.getId()];

      // We need to set state before we can package the block up for
      // cache-on-write. In a way, the block is ready, but not yet encoded or
      // compressed.
      state = State.BLOCK_READY;
      if (blockType == BlockType.DATA) {
        encodeDataBlockForDisk();
      } else {
        defaultBlockEncodingCtx.compressAfterEncodingWithBlockType(
            uncompressedBytesWithHeader, blockType);
        onDiskBytesWithHeader =
          defaultBlockEncodingCtx.getOnDiskBytesWithHeader();
      }

      int numBytes = (int) ChecksumUtil.numBytes(
          onDiskBytesWithHeader.length,
          bytesPerChecksum);

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
          onDiskChecksum, 0, checksumType, bytesPerChecksum);
    }

    /**
     * Encodes this block if it is a data block and encoding is turned on in
     * {@link #dataBlockEncoder}.
     */
    private void encodeDataBlockForDisk() throws IOException {
      // do data block encoding, if data block encoder is set
      ByteBuffer rawKeyValues =
          ByteBuffer.wrap(uncompressedBytesWithHeader, HConstants.HFILEBLOCK_HEADER_SIZE,
              uncompressedBytesWithHeader.length - HConstants.HFILEBLOCK_HEADER_SIZE).slice();

      //do the encoding
      dataBlockEncoder.beforeWriteToDisk(rawKeyValues,
              includesMemstoreTS, dataBlockEncodingCtx, blockType);

      uncompressedBytesWithHeader =
          dataBlockEncodingCtx.getUncompressedBytesWithHeader();
      onDiskBytesWithHeader =
          dataBlockEncodingCtx.getOnDiskBytesWithHeader();
      blockType = dataBlockEncodingCtx.getBlockType();
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
      offset = Bytes.putByte(dest, offset, checksumType.getCode());
      offset = Bytes.putInt(dest, offset, bytesPerChecksum);
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
    private void finishBlockAndWriteHeaderAndData(DataOutputStream out)
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
      return onDiskBytesWithHeader.length + onDiskChecksum.length - HConstants.HFILEBLOCK_HEADER_SIZE;
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
      if (state != State.WRITING)
        return 0;
      return userDataStream.size();
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
    public HFileBlock getBlockForCaching() {
      return new HFileBlock(blockType, getOnDiskSizeWithoutHeader(),
          getUncompressedSizeWithoutHeader(), prevOffset,
          getUncompressedBufferWithHeader(), DONT_FILL_HEADER, startOffset,
          includesMemstoreTS, MINOR_VERSION_WITH_CHECKSUM,
          0, ChecksumType.NULL.getCode(),  // no checksums in cached data
          onDiskBytesWithHeader.length + onDiskChecksum.length);
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
     * The iterator returns blocks starting with offset such that offset <=
     * startOffset < endOffset.
     *
     * @param startOffset the offset of the block to start iteration with
     * @param endOffset the offset to end iteration at (exclusive)
     * @return an iterator of blocks between the two given offsets
     */
    BlockIterator blockRange(long startOffset, long endOffset);

    /** Closes the backing streams */
    void closeStreams() throws IOException;
  }

  /**
   * A common implementation of some methods of {@link FSReader} and some
   * tools for implementing HFile format version-specific block readers.
   */
  private abstract static class AbstractFSReader implements FSReader {
    /** Compression algorithm used by the {@link HFile} */
    protected Compression.Algorithm compressAlgo;

    /** The size of the file we are reading from, or -1 if unknown. */
    protected long fileSize;

    /** The minor version of this reader */
    private int minorVersion;

    /** The size of the header */
    protected final int hdrSize;

    /** The filesystem used to access data */
    protected HFileSystem hfs;

    /** The path (if any) where this data is coming from */
    protected Path path;

    private final Lock streamLock = new ReentrantLock();

    /** The default buffer size for our buffered streams */
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    public AbstractFSReader(Algorithm compressAlgo, long fileSize, int minorVersion,
        HFileSystem hfs, Path path) throws IOException {
      this.compressAlgo = compressAlgo;
      this.fileSize = fileSize;
      this.minorVersion = minorVersion;
      this.hfs = hfs;
      this.path = path;
      this.hdrSize = headerSize(minorVersion);
    }

    @Override
    public BlockIterator blockRange(final long startOffset,
        final long endOffset) {
      return new BlockIterator() {
        private long offset = startOffset;

        @Override
        public HFileBlock nextBlock() throws IOException {
          if (offset >= endOffset)
            return null;
          HFileBlock b = readBlockData(offset, -1, -1, false);
          offset += b.getOnDiskSizeWithHeader();
          return b;
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

        int ret = istream.read(fileOffset, dest, destOffset, size + extraSize);
        if (ret < size) {
          throw new IOException("Positional read of " + size + " bytes " +
              "failed at offset " + fileOffset + " (returned " + ret + ")");
        }

        if (ret == size || ret < size + extraSize) {
          // Could not read the next block's header, or did not try.
          return -1;
        }
      }

      assert peekIntoNextBlock;
      return Bytes.toInt(dest, destOffset + size + BlockType.MAGIC_LENGTH) +
          hdrSize;
    }

    /**
     * @return The minorVersion of this HFile
     */
    protected int getMinorVersion() {
      return minorVersion;
    }
  }

  /**
   * We always prefetch the header of the next block, so that we know its
   * on-disk size in advance and can read it in one operation.
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
    ByteBuffer buf = ByteBuffer.wrap(header, 0, HConstants.HFILEBLOCK_HEADER_SIZE);
  }

  /** Reads version 2 blocks from the filesystem. */
  static class FSReaderV2 extends AbstractFSReader {
    /** The file system stream of the underlying {@link HFile} that 
     * does or doesn't do checksum validations in the filesystem */
    protected FSDataInputStreamWrapper streamWrapper;

    /** Whether we include memstore timestamp in data blocks */
    protected boolean includesMemstoreTS;

    /** Data block encoding used to read from file */
    protected HFileDataBlockEncoder dataBlockEncoder =
        NoOpDataBlockEncoder.INSTANCE;

    private HFileBlockDecodingContext encodedBlockDecodingCtx;

    private HFileBlockDefaultDecodingContext defaultDecodingCtx;

    private ThreadLocal<PrefetchedHeader> prefetchedHeaderForThread =
        new ThreadLocal<PrefetchedHeader>() {
          @Override
          public PrefetchedHeader initialValue() {
            return new PrefetchedHeader();
          }
        };

    public FSReaderV2(FSDataInputStreamWrapper stream, Algorithm compressAlgo, long fileSize,
        int minorVersion, HFileSystem hfs, Path path) throws IOException {
      super(compressAlgo, fileSize, minorVersion, hfs, path);
      this.streamWrapper = stream;
      // Older versions of HBase didn't support checksum.
      boolean forceNoHBaseChecksum = (this.getMinorVersion() < MINOR_VERSION_WITH_CHECKSUM);
      this.streamWrapper.prepareForBlockReader(forceNoHBaseChecksum);

      defaultDecodingCtx =
        new HFileBlockDefaultDecodingContext(compressAlgo);
      encodedBlockDecodingCtx =
          new HFileBlockDefaultDecodingContext(compressAlgo);
    }

    /**
     * A constructor that reads files with the latest minor version.
     * This is used by unit tests only.
     */
    FSReaderV2(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) throws IOException {
      this(new FSDataInputStreamWrapper(istream), compressAlgo, fileSize,
           HFileReaderV2.MAX_MINOR_VERSION, null, null);
    }

    /**
     * Reads a version 2 block. Tries to do as little memory allocation as
     * possible, using the provided on-disk size.
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
        int uncompressedSize, boolean pread) throws IOException {

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
                       path + " at offset " +
                       offset + " filesize " + fileSize +
                       ". Retrying read with HDFS checksums turned on...");

        if (!doVerificationThruHBaseChecksum) {
          String msg = "HBase checksum verification failed for file " +
                       path + " at offset " +
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
                         path + " at offset " +
                         offset + " filesize " + fileSize);
        }
      } 
      if (blk == null && !doVerificationThruHBaseChecksum) {
        String msg = "readBlockData failed, possibly due to " +
                     "checksum verification failed for file " + path +
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
        boolean verifyChecksum) throws IOException {
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
      PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
      ByteBuffer headerBuf = prefetchedHeader.offset == offset ?
          prefetchedHeader.buf : null;

      int nextBlockOnDiskSize = 0;
      // Allocate enough space to fit the next block's header too.
      byte[] onDiskBlock = null;

      HFileBlock b = null;
      if (onDiskSizeWithHeader > 0) {
        // We know the total on-disk size but not the uncompressed size. Read
        // the entire block into memory, then parse the header and decompress
        // from memory if using compression. This code path is used when
        // doing a random read operation relying on the block index, as well as
        // when the client knows the on-disk size from peeking into the next
        // block's header (e.g. this block's header) when reading the previous
        // block. This is the faster and more preferable case.

        // Size that we have to skip in case we have already read the header.
        int preReadHeaderSize = headerBuf == null ? 0 : hdrSize;
        onDiskBlock = new byte[onDiskSizeWithHeader + hdrSize];
        nextBlockOnDiskSize = readAtOffset(is, onDiskBlock,
            preReadHeaderSize, onDiskSizeWithHeader - preReadHeaderSize,
            true, offset + preReadHeaderSize, pread);
        if (headerBuf != null) {
          // the header has been read when reading the previous block, copy
          // to this block's header
          System.arraycopy(headerBuf.array(),
              headerBuf.arrayOffset(), onDiskBlock, 0, hdrSize);
        } else {
          headerBuf = ByteBuffer.wrap(onDiskBlock, 0, hdrSize);
        }
        // We know the total on-disk size but not the uncompressed size. Read
        // the entire block into memory, then parse the header and decompress
        // from memory if using compression. Here we have already read the
        // block's header
        try {
          b = new HFileBlock(headerBuf, getMinorVersion());
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
          readAtOffset(is, headerBuf.array(), headerBuf.arrayOffset(),
              hdrSize, false, offset, pread);
        }

        b = new HFileBlock(headerBuf, getMinorVersion());
        onDiskBlock = new byte[b.getOnDiskSizeWithHeader() + hdrSize];
        System.arraycopy(headerBuf.array(),
              headerBuf.arrayOffset(), onDiskBlock, 0, hdrSize);
        nextBlockOnDiskSize =
          readAtOffset(is, onDiskBlock, hdrSize, b.getOnDiskSizeWithHeader()
              - hdrSize, true, offset + hdrSize, pread);
        onDiskSizeWithHeader = b.onDiskSizeWithoutHeader + hdrSize;
      }

      boolean isCompressed =
        compressAlgo != null
            && compressAlgo != Compression.Algorithm.NONE;
      if (!isCompressed) {
        b.assumeUncompressed();
      }

      if (verifyChecksum &&
          !validateBlockChecksum(b, onDiskBlock, hdrSize)) {
        return null;             // checksum mismatch
      }

      if (isCompressed) {
        // This will allocate a new buffer but keep header bytes.
        b.allocateBuffer(nextBlockOnDiskSize > 0);
        if (b.blockType == BlockType.ENCODED_DATA) {
          encodedBlockDecodingCtx.prepareDecoding(b.getOnDiskSizeWithoutHeader(),
              b.getUncompressedSizeWithoutHeader(), b.getBufferWithoutHeader(), onDiskBlock,
              hdrSize);
        } else {
          defaultDecodingCtx.prepareDecoding(b.getOnDiskSizeWithoutHeader(),
              b.getUncompressedSizeWithoutHeader(), b.getBufferWithoutHeader(), onDiskBlock,
              hdrSize);
        }
        if (nextBlockOnDiskSize > 0) {
          // Copy next block's header bytes into the new block if we have them.
          System.arraycopy(onDiskBlock, onDiskSizeWithHeader, b.buf.array(),
              b.buf.arrayOffset() + hdrSize
              + b.uncompressedSizeWithoutHeader + b.totalChecksumBytes(),
              hdrSize);
        }
      } else {
        // The onDiskBlock will become the headerAndDataBuffer for this block.
        // If nextBlockOnDiskSizeWithHeader is not zero, the onDiskBlock already
        // contains the header of next block, so no need to set next
        // block's header in it.
        b = new HFileBlock(ByteBuffer.wrap(onDiskBlock, 0,
                onDiskSizeWithHeader), getMinorVersion());
      }

      b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSize;

      // Set prefetched header
      if (b.nextBlockOnDiskSizeWithHeader > 0) {
        prefetchedHeader.offset = offset + b.getOnDiskSizeWithHeader();
        System.arraycopy(onDiskBlock, onDiskSizeWithHeader,
            prefetchedHeader.header, 0, hdrSize);
      }

      b.includesMemstoreTS = includesMemstoreTS;
      b.offset = offset;
      return b;
    }

    void setIncludesMemstoreTS(boolean enabled) {
      includesMemstoreTS = enabled;
    }

    void setDataBlockEncoder(HFileDataBlockEncoder encoder) {
      this.dataBlockEncoder = encoder;
      encodedBlockDecodingCtx = encoder.newDataBlockDecodingContext(
          this.compressAlgo);
    }

    /**
     * Generates the checksum for the header as well as the data and
     * then validates that it matches the value stored in the header.
     * If there is a checksum mismatch, then return false. Otherwise
     * return true.
     */
    protected boolean validateBlockChecksum(HFileBlock block, 
      byte[] data, int hdrSize) throws IOException {
      return ChecksumUtil.validateBlockChecksum(path, block,
                                                data, hdrSize);
    }

    @Override
    public void closeStreams() throws IOException {
      streamWrapper.close();
    }
  }

  @Override
  public int getSerializedLength() {
    if (buf != null) {
      return this.buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE;
    }
    return 0;
  }

  @Override
  public void serialize(ByteBuffer destination) {
    ByteBuffer dupBuf = this.buf.duplicate();
    dupBuf.rewind();
    destination.put(dupBuf);
    destination.putInt(this.minorVersion);
    destination.putLong(this.offset);
    destination.putInt(this.nextBlockOnDiskSizeWithHeader);
    destination.rewind();
  }

  public void serializeExtraInfo(ByteBuffer destination) {
    destination.putInt(this.minorVersion);
    destination.putLong(this.offset);
    destination.putInt(this.nextBlockOnDiskSizeWithHeader);
    destination.rewind();
  }

  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
    return HFileBlock.blockDeserializer;
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
    if (this.buf.compareTo(castedComparison.buf) != 0) {
      return false;
    }
    if (this.buf.position() != castedComparison.buf.position()){
      return false;
    }
    if (this.buf.limit() != castedComparison.buf.limit()){
      return false;
    }
    return true;
  }

  public boolean doesIncludeMemstoreTS() {
    return includesMemstoreTS;
  }

  public DataBlockEncoding getDataBlockEncoding() {
    if (blockType == BlockType.ENCODED_DATA) {
      return DataBlockEncoding.getEncodingById(getDataBlockEncodingId());
    }
    return DataBlockEncoding.NONE;
  }

  byte getChecksumType() {
    return this.checksumType;
  }

  int getBytesPerChecksum() {
    return this.bytesPerChecksum;
  }

  int getOnDiskDataSizeWithHeader() {
    return this.onDiskDataSizeWithHeader;
  }

  int getMinorVersion() {
    return this.minorVersion;
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
    if (minorVersion < MINOR_VERSION_WITH_CHECKSUM || this.bytesPerChecksum == 0) {
      return 0;
    }
    return (int)ChecksumUtil.numBytes(onDiskDataSizeWithHeader, bytesPerChecksum);
  }

  /**
   * Returns the size of this block header.
   */
  public int headerSize() {
    return headerSize(this.minorVersion);
  }

  /**
   * Maps a minor version to the size of the header.
   */
  public static int headerSize(int minorVersion) {
    if (minorVersion < MINOR_VERSION_WITH_CHECKSUM) {
      return HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    }
    return HConstants.HFILEBLOCK_HEADER_SIZE;
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  public byte[] getDummyHeaderForVersion() {
    return getDummyHeaderForVersion(minorVersion);
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  static private byte[] getDummyHeaderForVersion(int minorVersion) {
    if (minorVersion < MINOR_VERSION_WITH_CHECKSUM) {
      return DUMMY_HEADER_NO_CHECKSUM;
    }
    return HConstants.HFILEBLOCK_DUMMY_HEADER;
  }

  /**
   * Convert the contents of the block header into a human readable string.
   * This is mostly helpful for debugging. This assumes that the block
   * has minor version > 0.
   */
  static String toStringHeader(ByteBuffer buf) throws IOException {
    int offset = buf.arrayOffset();
    byte[] b = buf.array();
    long magic = Bytes.toLong(b, offset); 
    BlockType bt = BlockType.read(buf);
    offset += Bytes.SIZEOF_LONG;
    int compressedBlockSizeNoHeader = Bytes.toInt(b, offset);
    offset += Bytes.SIZEOF_INT;
    int uncompressedBlockSizeNoHeader = Bytes.toInt(b, offset);
    offset += Bytes.SIZEOF_INT;
    long prevBlockOffset = Bytes.toLong(b, offset); 
    offset += Bytes.SIZEOF_LONG;
    byte cksumtype = b[offset];
    offset += Bytes.SIZEOF_BYTE;
    long bytesPerChecksum = Bytes.toInt(b, offset); 
    offset += Bytes.SIZEOF_INT;
    long onDiskDataSizeWithHeader = Bytes.toInt(b, offset); 
    offset += Bytes.SIZEOF_INT;
    return " Header dump: magic: " + magic +
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

