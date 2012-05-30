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

import static org.apache.hadoop.hbase.io.hfile.BlockType.MAGIC_LENGTH;
import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.NONE;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

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
public class HFileBlock extends SchemaConfigured implements Cacheable {

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

  /** The size data structures with minor version is 0 */
  static final int HEADER_SIZE_NO_CHECKSUM = MAGIC_LENGTH + 2 * Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  /** The size of a version 2 {@link HFile} block header, minor version 1.
   * There is a 1 byte checksum type, followed by a 4 byte bytesPerChecksum
   * followed by another 4 byte value to store sizeofDataOnDisk.
   */
  static final int HEADER_SIZE = HEADER_SIZE_NO_CHECKSUM + Bytes.SIZEOF_BYTE +
                                 2 * Bytes.SIZEOF_INT;

  /**
   * The size of block header when blockType is {@link BlockType#ENCODED_DATA}.
   * This extends normal header by adding the id of encoder.
   */
  public static final int ENCODED_HEADER_SIZE = HEADER_SIZE
      + DataBlockEncoding.ID_SIZE;

  /** Just an array of bytes of the right size. */
  static final byte[] DUMMY_HEADER = new byte[HEADER_SIZE];
  static final byte[] DUMMY_HEADER_NO_CHECKSUM = 
     new byte[HEADER_SIZE_NO_CHECKSUM];

  public static final int BYTE_BUFFER_HEAP_SIZE = (int) ClassSize.estimateBase(
      ByteBuffer.wrap(new byte[0], 0, 0).getClass(), false);

  static final int EXTRA_SERIALIZATION_SPACE = Bytes.SIZEOF_LONG +
      Bytes.SIZEOF_INT;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  private static final CacheableDeserializer<Cacheable> blockDeserializer =
      new CacheableDeserializer<Cacheable>() {
        public HFileBlock deserialize(ByteBuffer buf) throws IOException{
          ByteBuffer newByteBuffer = ByteBuffer.allocate(buf.limit()
              - HFileBlock.EXTRA_SERIALIZATION_SPACE);
          buf.limit(buf.limit()
              - HFileBlock.EXTRA_SERIALIZATION_SPACE).rewind();
          newByteBuffer.put(buf);
          HFileBlock ourBuffer = new HFileBlock(newByteBuffer, 
                                   MINOR_VERSION_NO_CHECKSUM);

          buf.position(buf.limit());
          buf.limit(buf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE);
          ourBuffer.offset = buf.getLong();
          ourBuffer.nextBlockOnDiskSizeWithHeader = buf.getInt();
          return ourBuffer;
        }
      };

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
   * peeking into the first {@link HEADER_SIZE} bytes of the next block's
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
   * @param buf block header ({@link #HEADER_SIZE} bytes) followed by
   *          uncompressed data. This
   * @param fillHeader true to fill in the first {@link #HEADER_SIZE} bytes of
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
                                       HEADER_SIZE_NO_CHECKSUM;
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
  int getOnDiskSizeWithoutHeader() {
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
   * Writes header fields into the first {@link HEADER_SIZE} bytes of the
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
  ByteBuffer getBufferWithoutHeader() {
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

  /**
   * Deserializes fields of the given writable using the data portion of this
   * block. Does not check that all the block data has been read.
   */
  void readInto(Writable w) throws IOException {
    Preconditions.checkNotNull(w);

    if (Writables.getWritable(buf.array(), buf.arrayOffset() + headerSize(),
        buf.limit() - headerSize(), w) == null) {
      throw new IOException("Failed to deserialize block " + this + " into a "
          + w.getClass().getSimpleName());
    }
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
   * {@link #HEADER_SIZE} bytes of the buffer contain a valid header consistent
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

    // We might optionally allocate HEADER_SIZE more bytes to read the next
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
        // Base class size, including object overhead.
        SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE +
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
   * <ul>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression
   * algorithm
   * <li>Call {@link Writer#startWriting(BlockType, boolean)} and get a data stream to
   * write to
   * <li>Write your data into the stream
   * <li>Call {@link Writer#writeHeaderAndData(FSDataOutputStream)} as many times as you need to
   * store the serialized block into an external stream, or call
   * {@link Writer#getHeaderAndData()} to get it as a byte array.
   * <li>Repeat to write more blocks
   * </ul>
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

    /** Compression algorithm for all blocks this instance writes. */
    private final Compression.Algorithm compressAlgo;

    /** Data block encoder used for data blocks */
    private final HFileDataBlockEncoder dataBlockEncoder;

    /**
     * The stream we use to accumulate data in uncompressed format for each
     * block. We reset this stream at the end of each block and reuse it. The
     * header is written as the first {@link #HEADER_SIZE} bytes into this
     * stream.
     */
    private ByteArrayOutputStream baosInMemory;

    /** Compressor, which is also reused between consecutive blocks. */
    private Compressor compressor;

    /** Compression output stream */
    private CompressionOutputStream compressionStream;
    
    /** Underlying stream to write compressed bytes to */
    private ByteArrayOutputStream compressedByteStream;

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
     * The size of the data on disk that does not include the checksums.
     * (header + data)
     */
    private int onDiskDataSizeWithHeader;

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
     * {@link #uncompressedSizeWithoutHeader} + {@link HFileBlock#HEADER_SIZE}.
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
     * @param dataBlockEncoderAlgo data block encoding algorithm to use
     * @param checksumType type of checksum
     * @param bytesPerChecksum bytes per checksum
     */
    public Writer(Compression.Algorithm compressionAlgorithm,
          HFileDataBlockEncoder dataBlockEncoder, boolean includesMemstoreTS,
          ChecksumType checksumType, int bytesPerChecksum) {
      compressAlgo = compressionAlgorithm == null ? NONE : compressionAlgorithm;
      this.dataBlockEncoder = dataBlockEncoder != null
          ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;

      baosInMemory = new ByteArrayOutputStream();
      if (compressAlgo != NONE) {
        compressor = compressionAlgorithm.getCompressor();
        compressedByteStream = new ByteArrayOutputStream();
        try {
          compressionStream =
              compressionAlgorithm.createPlainCompressionStream(
                  compressedByteStream, compressor);
        } catch (IOException e) {
          throw new RuntimeException("Could not create compression stream " + 
              "for algorithm " + compressionAlgorithm, e);
        }
      }
      if (bytesPerChecksum < HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " +
            " Minimum is " + HEADER_SIZE + " but the configured value is " +
            bytesPerChecksum);
      }
      
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
      baosInMemory.write(DUMMY_HEADER);

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
      encodeDataBlockForDisk();

      doCompressionAndChecksumming();
    }

    /**
     * Do compression if it is enabled, or re-use the uncompressed buffer if
     * it is not. Fills in the compressed block's header if doing compression.
     * Also, compute the checksums. In the case of no-compression, write the
     * checksums to its own seperate data structure called onDiskChecksum. In
     * the case when compression is enabled, the checksums are written to the
     * outputbyte stream 'baos'.
     */
    private void doCompressionAndChecksumming() throws IOException {
      // do the compression
      if (compressAlgo != NONE) {
        compressedByteStream.reset();
        compressedByteStream.write(DUMMY_HEADER);

        compressionStream.resetState();

        compressionStream.write(uncompressedBytesWithHeader, HEADER_SIZE,
            uncompressedBytesWithHeader.length - HEADER_SIZE);

        compressionStream.flush();
        compressionStream.finish();

        // generate checksums
        onDiskDataSizeWithHeader = compressedByteStream.size(); // data size

        // reserve space for checksums in the output byte stream
        ChecksumUtil.reserveSpaceForChecksums(compressedByteStream, 
          onDiskDataSizeWithHeader, bytesPerChecksum);


        onDiskBytesWithHeader = compressedByteStream.toByteArray();
        putHeader(onDiskBytesWithHeader, 0, onDiskBytesWithHeader.length,
            uncompressedBytesWithHeader.length, onDiskDataSizeWithHeader);

       // generate checksums for header and data. The checksums are
       // part of onDiskBytesWithHeader itself.
       ChecksumUtil.generateChecksums(
         onDiskBytesWithHeader, 0, onDiskDataSizeWithHeader,
         onDiskBytesWithHeader, onDiskDataSizeWithHeader,
         checksumType, bytesPerChecksum);

        // Checksums are already part of onDiskBytesWithHeader
        onDiskChecksum = HConstants.EMPTY_BYTE_ARRAY;

        //set the header for the uncompressed bytes (for cache-on-write)
        putHeader(uncompressedBytesWithHeader, 0,
          onDiskBytesWithHeader.length + onDiskChecksum.length,
          uncompressedBytesWithHeader.length, onDiskDataSizeWithHeader);

      } else {
        // If we are not using any compression, then the
        // checksums are written to its own array onDiskChecksum.
        onDiskBytesWithHeader = uncompressedBytesWithHeader;

        onDiskDataSizeWithHeader = onDiskBytesWithHeader.length;
        int numBytes = (int)ChecksumUtil.numBytes(
                          uncompressedBytesWithHeader.length,
                          bytesPerChecksum);
        onDiskChecksum = new byte[numBytes];

        //set the header for the uncompressed bytes
        putHeader(uncompressedBytesWithHeader, 0,
          onDiskBytesWithHeader.length + onDiskChecksum.length,
          uncompressedBytesWithHeader.length, onDiskDataSizeWithHeader);

        ChecksumUtil.generateChecksums(
          uncompressedBytesWithHeader, 0, uncompressedBytesWithHeader.length,
          onDiskChecksum, 0,
          checksumType, bytesPerChecksum);
      }
    }

    /**
     * Encodes this block if it is a data block and encoding is turned on in
     * {@link #dataBlockEncoder}.
     */
    private void encodeDataBlockForDisk() throws IOException {
      if (blockType != BlockType.DATA) {
        return; // skip any non-data block
      }

      // do data block encoding, if data block encoder is set
      ByteBuffer rawKeyValues = ByteBuffer.wrap(uncompressedBytesWithHeader,
          HEADER_SIZE, uncompressedBytesWithHeader.length -
          HEADER_SIZE).slice();
      Pair<ByteBuffer, BlockType> encodingResult =
          dataBlockEncoder.beforeWriteToDisk(rawKeyValues,
              includesMemstoreTS, DUMMY_HEADER);

      BlockType encodedBlockType = encodingResult.getSecond();
      if (encodedBlockType == BlockType.ENCODED_DATA) {
        uncompressedBytesWithHeader = encodingResult.getFirst().array();
        blockType = BlockType.ENCODED_DATA;
      } else {
        // There is no encoding configured. Do some extra sanity-checking.
        if (encodedBlockType != BlockType.DATA) {
          throw new IOException("Unexpected block type coming out of data " +
              "block encoder: " + encodedBlockType);
        }
        if (userDataStream.size() !=
            uncompressedBytesWithHeader.length - HEADER_SIZE) {
          throw new IOException("Uncompressed size mismatch: "
              + userDataStream.size() + " vs. "
              + (uncompressedBytesWithHeader.length - HEADER_SIZE));
        }
      }
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
      offset = Bytes.putInt(dest, offset, onDiskSize - HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSize - HEADER_SIZE);
      offset = Bytes.putLong(dest, offset, prevOffset);
      offset = Bytes.putByte(dest, offset, checksumType.getCode());
      offset = Bytes.putInt(dest, offset, bytesPerChecksum);
      offset = Bytes.putInt(dest, offset, onDiskDataSizeWithHeader);
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

      writeHeaderAndData((DataOutputStream) out);
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
    private void writeHeaderAndData(DataOutputStream out) throws IOException {
      ensureBlockReady();
      out.write(onDiskBytesWithHeader);
      if (compressAlgo == NONE) {
        if (onDiskChecksum == HConstants.EMPTY_BYTE_ARRAY) {
          throw new IOException("A " + blockType 
              + " without compression should have checksums " 
              + " stored separately.");
        }
        out.write(onDiskChecksum);
      }
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
      if (compressAlgo == NONE) {
        if (onDiskChecksum == HConstants.EMPTY_BYTE_ARRAY) {
          throw new IOException("A " + blockType 
              + " without compression should have checksums " 
              + " stored separately.");
        }
        // This is not very optimal, because we are doing an extra copy.
        // But this method is used only by unit tests.
        byte[] output = new byte[onDiskBytesWithHeader.length +
                                 onDiskChecksum.length];
        System.arraycopy(onDiskBytesWithHeader, 0,
                         output, 0, onDiskBytesWithHeader.length);
        System.arraycopy(onDiskChecksum, 0,
                         output, onDiskBytesWithHeader.length,
                         onDiskChecksum.length);
        return output;
      }
      return onDiskBytesWithHeader;
    }

    /**
     * Releases the compressor this writer uses to compress blocks into the
     * compressor pool. Needs to be called before the writer is discarded.
     */
    public void releaseCompressor() {
      if (compressor != null) {
        compressAlgo.returnCompressor(compressor);
        compressor = null;
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
      return onDiskBytesWithHeader.length + onDiskChecksum.length - HEADER_SIZE;
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
      return uncompressedBytesWithHeader.length - HEADER_SIZE;
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
  }

  /**
   * A common implementation of some methods of {@link FSReader} and some
   * tools for implementing HFile format version-specific block readers.
   */
  private abstract static class AbstractFSReader implements FSReader {

    /** The file system stream of the underlying {@link HFile} that 
     * does checksum validations in the filesystem */
    protected final FSDataInputStream istream;

    /** The file system stream of the underlying {@link HFile} that
     * does not do checksum verification in the file system */
    protected final FSDataInputStream istreamNoFsChecksum;

    /** Compression algorithm used by the {@link HFile} */
    protected Compression.Algorithm compressAlgo;

    /** The size of the file we are reading from, or -1 if unknown. */
    protected long fileSize;

    /** The minor version of this reader */
    private int minorVersion;

    /** The size of the header */
    protected int hdrSize;

    /** The filesystem used to access data */
    protected HFileSystem hfs;

    /** The path (if any) where this data is coming from */
    protected Path path;

    /** The default buffer size for our buffered streams */
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    public AbstractFSReader(FSDataInputStream istream, 
        FSDataInputStream istreamNoFsChecksum,
        Algorithm compressAlgo,
        long fileSize, int minorVersion, HFileSystem hfs, Path path) 
        throws IOException {
      this.istream = istream;
      this.compressAlgo = compressAlgo;
      this.fileSize = fileSize;
      this.minorVersion = minorVersion;
      this.hfs = hfs;
      this.path = path;
      this.hdrSize = headerSize(minorVersion);
      this.istreamNoFsChecksum = istreamNoFsChecksum;
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

      if (pread) {
        // Positional read. Better for random reads.
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
      } else {
        // Seek + read. Better for scanning.
        synchronized (istream) {
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
        }
      }

      assert peekIntoNextBlock;
      return Bytes.toInt(dest, destOffset + size + BlockType.MAGIC_LENGTH) +
          hdrSize;
    }

    /**
     * Decompresses data from the given stream using the configured compression
     * algorithm.
     * @param dest
     * @param destOffset
     * @param bufferedBoundedStream
     *          a stream to read compressed data from, bounded to the exact
     *          amount of compressed data
     * @param uncompressedSize
     *          uncompressed data size, header not included
     * @throws IOException
     */
    protected void decompress(byte[] dest, int destOffset,
        InputStream bufferedBoundedStream,
        int uncompressedSize) throws IOException {
      Decompressor decompressor = null;
      try {
        decompressor = compressAlgo.getDecompressor();
        InputStream is = compressAlgo.createDecompressionStream(
            bufferedBoundedStream, decompressor, 0);

        IOUtils.readFully(is, dest, destOffset, uncompressedSize);
        is.close();
      } finally {
        if (decompressor != null) {
          compressAlgo.returnDecompressor(decompressor);
        }
      }
    }

    /**
     * Creates a buffered stream reading a certain slice of the file system
     * input stream. We need this because the decompression we use seems to
     * expect the input stream to be bounded.
     *
     * @param offset the starting file offset the bounded stream reads from
     * @param size the size of the segment of the file the stream should read
     * @param pread whether to use position reads
     * @return a stream restricted to the given portion of the file
     */
    protected InputStream createBufferedBoundedStream(long offset,
        int size, boolean pread) {
      return new BufferedInputStream(new BoundedRangeFileInputStream(istream,
          offset, size, pread), Math.min(DEFAULT_BUFFER_SIZE, size));
    }

    /**
     * @return The minorVersion of this HFile
     */
    protected int getMinorVersion() {
      return minorVersion;
    }
  }

  /**
   * Reads version 1 blocks from the file system. In version 1 blocks,
   * everything is compressed, including the magic record, if compression is
   * enabled. Everything might be uncompressed if no compression is used. This
   * reader returns blocks represented in the uniform version 2 format in
   * memory.
   */
  static class FSReaderV1 extends AbstractFSReader {

    /** Header size difference between version 1 and 2 */
    private static final int HEADER_DELTA = HEADER_SIZE_NO_CHECKSUM - 
                                            MAGIC_LENGTH;

    public FSReaderV1(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) throws IOException {
      super(istream, istream, compressAlgo, fileSize, 0, null, null);
    }

    /**
     * Read a version 1 block. There is no uncompressed header, and the block
     * type (the magic record) is part of the compressed data. This
     * implementation assumes that the bounded range file input stream is
     * needed to stop the decompressor reading into next block, because the
     * decompressor just grabs a bunch of data without regard to whether it is
     * coming to end of the compressed section.
     *
     * The block returned is still a version 2 block, and in particular, its
     * first {@link #HEADER_SIZE} bytes contain a valid version 2 header.
     *
     * @param offset the offset of the block to read in the file
     * @param onDiskSizeWithMagic the on-disk size of the version 1 block,
     *          including the magic record, which is the part of compressed
     *          data if using compression
     * @param uncompressedSizeWithMagic uncompressed size of the version 1
     *          block, including the magic record
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithMagic,
        int uncompressedSizeWithMagic, boolean pread) throws IOException {
      if (uncompressedSizeWithMagic <= 0) {
        throw new IOException("Invalid uncompressedSize="
            + uncompressedSizeWithMagic + " for a version 1 block");
      }

      if (onDiskSizeWithMagic <= 0 || onDiskSizeWithMagic >= Integer.MAX_VALUE)
      {
        throw new IOException("Invalid onDiskSize=" + onDiskSizeWithMagic
            + " (maximum allowed: " + Integer.MAX_VALUE + ")");
      }

      int onDiskSize = (int) onDiskSizeWithMagic;

      if (uncompressedSizeWithMagic < MAGIC_LENGTH) {
        throw new IOException("Uncompressed size for a version 1 block is "
            + uncompressedSizeWithMagic + " but must be at least "
            + MAGIC_LENGTH);
      }

      // The existing size already includes magic size, and we are inserting
      // a version 2 header.
      ByteBuffer buf = ByteBuffer.allocate(uncompressedSizeWithMagic
          + HEADER_DELTA);

      int onDiskSizeWithoutHeader;
      if (compressAlgo == Compression.Algorithm.NONE) {
        // A special case when there is no compression.
        if (onDiskSize != uncompressedSizeWithMagic) {
          throw new IOException("onDiskSize=" + onDiskSize
              + " and uncompressedSize=" + uncompressedSizeWithMagic
              + " must be equal for version 1 with no compression");
        }

        // The first MAGIC_LENGTH bytes of what this will read will be
        // overwritten.
        readAtOffset(istream, buf.array(), buf.arrayOffset() + HEADER_DELTA,
            onDiskSize, false, offset, pread);

        onDiskSizeWithoutHeader = uncompressedSizeWithMagic - MAGIC_LENGTH;
      } else {
        InputStream bufferedBoundedStream = createBufferedBoundedStream(
            offset, onDiskSize, pread);
        decompress(buf.array(), buf.arrayOffset() + HEADER_DELTA,
            bufferedBoundedStream, uncompressedSizeWithMagic);

        // We don't really have a good way to exclude the "magic record" size
        // from the compressed block's size, since it is compressed as well.
        onDiskSizeWithoutHeader = onDiskSize;
      }

      BlockType newBlockType = BlockType.parse(buf.array(), buf.arrayOffset()
          + HEADER_DELTA, MAGIC_LENGTH);

      // We set the uncompressed size of the new HFile block we are creating
      // to the size of the data portion of the block without the magic record,
      // since the magic record gets moved to the header.
      HFileBlock b = new HFileBlock(newBlockType, onDiskSizeWithoutHeader,
          uncompressedSizeWithMagic - MAGIC_LENGTH, -1L, buf, FILL_HEADER,
          offset, MemStore.NO_PERSISTENT_TS, 0, 0, ChecksumType.NULL.getCode(),
          onDiskSizeWithoutHeader + HEADER_SIZE_NO_CHECKSUM);
      return b;
    }
  }

  /**
   * We always prefetch the header of the next block, so that we know its
   * on-disk size in advance and can read it in one operation.
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HEADER_SIZE];
    ByteBuffer buf = ByteBuffer.wrap(header, 0, HEADER_SIZE);
  }

  /** Reads version 2 blocks from the filesystem. */
  static class FSReaderV2 extends AbstractFSReader {

    // The configuration states that we should validate hbase checksums
    private final boolean useHBaseChecksumConfigured;

    // Record the current state of this reader with respect to
    // validating checkums in HBase. This is originally set the same
    // value as useHBaseChecksumConfigured, but can change state as and when
    // we encounter checksum verification failures.
    private volatile boolean useHBaseChecksum;

    // In the case of a checksum failure, do these many succeeding
    // reads without hbase checksum verification.
    private volatile int checksumOffCount = -1;

    /** Whether we include memstore timestamp in data blocks */
    protected boolean includesMemstoreTS;

    /** Data block encoding used to read from file */
    protected HFileDataBlockEncoder dataBlockEncoder =
        NoOpDataBlockEncoder.INSTANCE;

    private ThreadLocal<PrefetchedHeader> prefetchedHeaderForThread =
        new ThreadLocal<PrefetchedHeader>() {
          @Override
          public PrefetchedHeader initialValue() {
            return new PrefetchedHeader();
          }
        };

    public FSReaderV2(FSDataInputStream istream, 
        FSDataInputStream istreamNoFsChecksum, Algorithm compressAlgo,
        long fileSize, int minorVersion, HFileSystem hfs, Path path) 
      throws IOException {
      super(istream, istreamNoFsChecksum, compressAlgo, fileSize, 
            minorVersion, hfs, path);

      if (hfs != null) {
        // Check the configuration to determine whether hbase-level
        // checksum verification is needed or not.
        useHBaseChecksum = hfs.useHBaseChecksum();
      } else {
        // The configuration does not specify anything about hbase checksum
        // validations. Set it to true here assuming that we will verify
        // hbase checksums for all reads. For older files that do not have 
        // stored checksums, this flag will be reset later.
        useHBaseChecksum = true;
      }

      // for older versions, hbase did not store checksums.
      if (getMinorVersion() < MINOR_VERSION_WITH_CHECKSUM) {
        useHBaseChecksum = false;
      }
      this.useHBaseChecksumConfigured = useHBaseChecksum;
    }

    /**
     * A constructor that reads files with the latest minor version.
     * This is used by unit tests only.
     */
    FSReaderV2(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) throws IOException {
      this(istream, istream, compressAlgo, fileSize, 
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

      // It is ok to get a reference to the stream here without any
      // locks because it is marked final.
      FSDataInputStream is = this.istreamNoFsChecksum;

      // get a copy of the current state of whether to validate
      // hbase checksums or not for this read call. This is not 
      // thread-safe but the one constaint is that if we decide 
      // to skip hbase checksum verification then we are 
      // guaranteed to use hdfs checksum verification.
      boolean doVerificationThruHBaseChecksum = this.useHBaseChecksum;
      if (!doVerificationThruHBaseChecksum) {
        is = this.istream;
      }
                     
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
        this.checksumOffCount = CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD;
        this.useHBaseChecksum = false;
        doVerificationThruHBaseChecksum = false;
        is = this.istream;
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
      if (!this.useHBaseChecksum && this.useHBaseChecksumConfigured) {
        if (this.checksumOffCount-- < 0) {
          this.useHBaseChecksum = true; // auto re-enable hbase checksums
        }
      }
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
        long onDiskSizeWithHeaderL,
        int uncompressedSize, boolean pread, boolean verifyChecksum) 
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

      HFileBlock b;
      if (onDiskSizeWithHeader > 0) {
        // We know the total on-disk size but not the uncompressed size. Read
        // the entire block into memory, then parse the header and decompress
        // from memory if using compression. This code path is used when
        // doing a random read operation relying on the block index, as well as
        // when the client knows the on-disk size from peeking into the next
        // block's header (e.g. this block's header) when reading the previous
        // block. This is the faster and more preferable case.

        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - hdrSize;
        assert onDiskSizeWithoutHeader >= 0;

        // See if we can avoid reading the header. This is desirable, because
        // we will not incur a seek operation to seek back if we have already
        // read this block's header as part of the previous read's look-ahead.
        PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
        byte[] header = prefetchedHeader.offset == offset
            ? prefetchedHeader.header : null;

        // Size that we have to skip in case we have already read the header.
        int preReadHeaderSize = header == null ? 0 : hdrSize;

        if (compressAlgo == Compression.Algorithm.NONE) {
          // Just read the whole thing. Allocate enough space to read the
          // next block's header too.

          ByteBuffer headerAndData = ByteBuffer.allocate(onDiskSizeWithHeader
              + hdrSize);
          headerAndData.limit(onDiskSizeWithHeader);

          if (header != null) {
            System.arraycopy(header, 0, headerAndData.array(), 0,
                hdrSize);
          }

          int nextBlockOnDiskSizeWithHeader = readAtOffset(is,
              headerAndData.array(), headerAndData.arrayOffset()
                  + preReadHeaderSize, onDiskSizeWithHeader
                  - preReadHeaderSize, true, offset + preReadHeaderSize,
                  pread);

          b = new HFileBlock(headerAndData, getMinorVersion());
          b.assumeUncompressed();
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSizeWithHeader;
          if (verifyChecksum &&
              !validateBlockChecksum(b, headerAndData.array(), hdrSize)) {
            return null;             // checksum mismatch
          }
          if (b.nextBlockOnDiskSizeWithHeader > 0)
            setNextBlockHeader(offset, b);
        } else {
          // Allocate enough space to fit the next block's header too.
          byte[] onDiskBlock = new byte[onDiskSizeWithHeader + hdrSize];

          int nextBlockOnDiskSize = readAtOffset(is, onDiskBlock,
              preReadHeaderSize, onDiskSizeWithHeader - preReadHeaderSize,
              true, offset + preReadHeaderSize, pread);

          if (header == null)
            header = onDiskBlock;

          try {
            b = new HFileBlock(ByteBuffer.wrap(header, 0, hdrSize), 
                               getMinorVersion());
          } catch (IOException ex) {
            // Seen in load testing. Provide comprehensive debug info.
            throw new IOException("Failed to read compressed block at "
                + offset + ", onDiskSizeWithoutHeader=" + onDiskSizeWithHeader
                + ", preReadHeaderSize=" + preReadHeaderSize
                + ", header.length=" + header.length + ", header bytes: "
                + Bytes.toStringBinary(header, 0, hdrSize), ex);
          }
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSize;
          if (verifyChecksum && 
              !validateBlockChecksum(b, onDiskBlock, hdrSize)) {
            return null;             // checksum mismatch
          }

          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              onDiskBlock, hdrSize, onDiskSizeWithoutHeader));

          // This will allocate a new buffer but keep header bytes.
          b.allocateBuffer(b.nextBlockOnDiskSizeWithHeader > 0);

          decompress(b.buf.array(), b.buf.arrayOffset() + hdrSize, dis,
              b.uncompressedSizeWithoutHeader);

          // Copy next block's header bytes into the new block if we have them.
          if (nextBlockOnDiskSize > 0) {
            System.arraycopy(onDiskBlock, onDiskSizeWithHeader, b.buf.array(),
                b.buf.arrayOffset() + hdrSize
                    + b.uncompressedSizeWithoutHeader + b.totalChecksumBytes(), 
                hdrSize);

            setNextBlockHeader(offset, b);
          }
        }

      } else {
        // We don't know the on-disk size. Read the header first, determine the
        // on-disk size from it, and read the remaining data, thereby incurring
        // two read operations. This might happen when we are doing the first
        // read in a series of reads or a random read, and we don't have access
        // to the block index. This is costly and should happen very rarely.

        // Check if we have read this block's header as part of reading the
        // previous block. If so, don't read the header again.
        PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
        ByteBuffer headerBuf = prefetchedHeader.offset == offset ?
            prefetchedHeader.buf : null;

        if (headerBuf == null) {
          // Unfortunately, we still have to do a separate read operation to
          // read the header.
          headerBuf = ByteBuffer.allocate(hdrSize);
          readAtOffset(is, headerBuf.array(), headerBuf.arrayOffset(), hdrSize,
              false, offset, pread);
        }

        b = new HFileBlock(headerBuf, getMinorVersion());

        // This will also allocate enough room for the next block's header.
        b.allocateBuffer(true);

        if (compressAlgo == Compression.Algorithm.NONE) {

          // Avoid creating bounded streams and using a "codec" that does
          // nothing.
          b.assumeUncompressed();
          b.nextBlockOnDiskSizeWithHeader = readAtOffset(is, b.buf.array(),
              b.buf.arrayOffset() + hdrSize,
              b.uncompressedSizeWithoutHeader + b.totalChecksumBytes(), 
              true, offset + hdrSize,
              pread);
          if (verifyChecksum && 
              !validateBlockChecksum(b, b.buf.array(), hdrSize)) {
            return null;             // checksum mismatch
          }

          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            setNextBlockHeader(offset, b);
          }
        } else {
          // Allocate enough space for the block's header and compressed data.
          byte[] compressedBytes = new byte[b.getOnDiskSizeWithHeader()
              + hdrSize];

          b.nextBlockOnDiskSizeWithHeader = readAtOffset(is, compressedBytes,
              hdrSize, b.onDiskSizeWithoutHeader, true, offset
                  + hdrSize, pread);
          if (verifyChecksum &&
              !validateBlockChecksum(b, compressedBytes, hdrSize)) {
            return null;             // checksum mismatch
          }
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              compressedBytes, hdrSize, b.onDiskSizeWithoutHeader));

          decompress(b.buf.array(), b.buf.arrayOffset() + hdrSize, dis,
              b.uncompressedSizeWithoutHeader);

          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            // Copy the next block's header into the new block.
            int nextHeaderOffset = b.buf.arrayOffset() + hdrSize
                + b.uncompressedSizeWithoutHeader + b.totalChecksumBytes();
            System.arraycopy(compressedBytes,
                compressedBytes.length - hdrSize,
                b.buf.array(),
                nextHeaderOffset,
                hdrSize);

            setNextBlockHeader(offset, b);
          }
        }
      }

      b.includesMemstoreTS = includesMemstoreTS;
      b.offset = offset;
      return b;
    }

    private void setNextBlockHeader(long offset, HFileBlock b) {
      PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
      prefetchedHeader.offset = offset + b.getOnDiskSizeWithHeader();
      int nextHeaderOffset = b.buf.arrayOffset() + hdrSize
          + b.uncompressedSizeWithoutHeader + b.totalChecksumBytes();
      System.arraycopy(b.buf.array(), nextHeaderOffset,
          prefetchedHeader.header, 0, hdrSize);
    }

    void setIncludesMemstoreTS(boolean enabled) {
      includesMemstoreTS = enabled;
    }

    void setDataBlockEncoder(HFileDataBlockEncoder encoder) {
      this.dataBlockEncoder = encoder;
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
    destination.put(this.buf.duplicate());
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
  static private int headerSize(int minorVersion) {
    if (minorVersion < MINOR_VERSION_WITH_CHECKSUM) {
      return HEADER_SIZE_NO_CHECKSUM;
    }
    return HEADER_SIZE;
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
    return DUMMY_HEADER;
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

