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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
 * index.
 * <li>Compressed data (or uncompressed data if compression is disabled). The
 * compression algorithm is the same for all the blocks in the {@link HFile},
 * similarly to what was done in version 1.
 * </ul>
 * The version 2 block representation in the block cache is the same as above,
 * except that the data section is always uncompressed in the cache.
 */
public class HFileBlock extends SchemaConfigured implements Cacheable {

  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  /** The size of a version 2 {@link HFile} block header */
  public static final int HEADER_SIZE = MAGIC_LENGTH + 2 * Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  /** Just an array of bytes of the right size. */
  public static final byte[] DUMMY_HEADER = new byte[HEADER_SIZE];

  public static final int BYTE_BUFFER_HEAP_SIZE = (int) ClassSize.estimateBase(
      ByteBuffer.wrap(new byte[0], 0, 0).getClass(), false);

  // Static counters
  private static final AtomicLong numSeekRead = new AtomicLong();
  private static final AtomicLong numPositionalRead = new AtomicLong();

  private static final int HFILE_BLOCK_OVERHEAD = ClassSize.align(
      // Base class size, including object overhead.
      SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE +

      // Block type and byte buffer references
      2 * ClassSize.REFERENCE +

      // On-disk size, uncompressed size, and next block's on-disk size
      3 * Bytes.SIZEOF_INT +

      // This and previous block offset
      2 * Bytes.SIZEOF_LONG +

      // "Include memstore timestamp" flag
      Bytes.SIZEOF_BOOLEAN
  );

  // Instance variables
  private BlockType blockType;
  private int onDiskSizeWithoutHeader;
  private final int uncompressedSizeWithoutHeader;
  private final long prevBlockOffset;
  private ByteBuffer buf;
  private boolean includesMemstoreTS;

  /**
   * The offset of this block in the file. Populated by the reader for
   * convenience of access. This offset is not part of the block header.
   */
  private long offset = -1;

  /**
   * The on-disk size of the next block, including the header, obtained by
   * peeking into the first {@link #HEADER_SIZE} bytes of the next block's
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
   */
  public HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
      int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuffer buf,
      boolean fillHeader, long offset, boolean includesMemstoreTS) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.buf = buf;
    if (fillHeader)
      overwriteHeader();
    this.offset = offset;
    this.includesMemstoreTS = includesMemstoreTS;
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   *
   * @param b bytes to include in the block
   * @return the new block
   * @throws IOException
   */
  private HFileBlock(ByteBuffer b) throws IOException {
    b.rewind();
    blockType = BlockType.read(b);
    onDiskSizeWithoutHeader = b.getInt();
    uncompressedSizeWithoutHeader = b.getInt();
    prevBlockOffset = b.getLong();
    buf = b;
    buf.rewind();
  }

  /**
   * A factory method to create an HFileBlock from a raw (compressed and
   * encoded) byte array.
   * </p>
   * The reason that this is static member of HFileBlock is that it needs
   * to be called by classes that read from {@link L2Cache}, but at the mean
   * time needs to have access to private members of HFileBlock (such as the
   * internal buffers).
   *
   * TODO (avf): re-factor HFileBlock such that this method could be
   *             a non-static member of another class
   * @param rawBytes The compressed and encoded byte array (essentially this
   *                 is the block as it would appear on disk or in
   *                 {@link L2Cache}
   * @param compressAlgo Compression algorithm used to encode the block
   * @param includeMemStoreTs Should memstore timestamp be included?
   * @param offset Offset within the HFile at which the block is located
   * @return An instantiated, uncompressed, decoded in-memory representation
   *         of the HFileBlock that can be scanned through or cached in the
   *         L1 block cache
   * @throws IOException If there is an error de-compressed, de-coding, or
   *         otherwise parsing the raw byte array encoding the block.
   */
  public static HFileBlock fromBytes(byte[] rawBytes, Algorithm compressAlgo,
      boolean includeMemStoreTs, long offset) throws IOException {
    HFileBlock b;
    if (compressAlgo == Algorithm.NONE) {
      b = new HFileBlock(ByteBuffer.wrap(rawBytes));
      b.assumeUncompressed();
    } else {
      b = new HFileBlock(ByteBuffer.wrap(rawBytes, 0, HEADER_SIZE));
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
          rawBytes, HEADER_SIZE, rawBytes.length - HEADER_SIZE));
      b.allocateBuffer(true);
      AbstractFSReader.decompress(compressAlgo, b.buf.array(),
          b.buf.arrayOffset() + HEADER_SIZE, dis,
          b.uncompressedSizeWithoutHeader);
    }
    b.includesMemstoreTS = includeMemStoreTs;
    b.offset = offset;
    return b;
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
    return buf.getShort(HEADER_SIZE);
  }

  /**
   * @return the on-disk size of the block with header size included
   */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + HEADER_SIZE;
  }

  /**
   * Returns the size of the compressed part of the block in case compression
   * is used, or the uncompressed size of the data part otherwise. Header size
   * is not included.
   *
   * @return the on-disk size of the data part of the block, header not
   *         included
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
   * Writes header fields into the first {@link #HEADER_SIZE} bytes of the
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
   * array is not copied.
   *
   * @return the buffer with header skipped
   */
  public ByteBuffer getBufferWithoutHeader() {
    return ByteBuffer.wrap(buf.array(), buf.arrayOffset() + HEADER_SIZE,
        buf.limit() - HEADER_SIZE).slice();
  }

  /**
   * Returns the buffer this block stores internally. The clients must not
   * modify the buffer object.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuffer getBufferReadOnly() {
    return buf;
  }

  /**
   * Returns a byte buffer of this block, including header data, positioned at
   * the beginning of header. The underlying data array is not copied.
   *
   * @return the byte buffer with header included
   */
  public ByteBuffer getBufferWithHeader() {
    ByteBuffer dupBuf = buf.duplicate();
    dupBuf.rewind();
    return dupBuf;
  }

  /**
   * Deserializes fields of the given writable using the data portion of this
   * block. Does not check that all the block data has been read.
   *
   * @throws IOException
   */
  public void readInto(Writable w) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + HEADER_SIZE, buf.limit() - HEADER_SIZE);
    DataInputStream dis = new DataInputStream(bais);
    w.readFields(dis);
  }

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link #HEADER_SIZE} bytes of the buffer contain a valid header consistent
   * with the fields. This function is primary for testing and debugging, and
   * is not thread-safe, because it alters the internal buffer pointer. Also,
   * it uses assertions, which are switched off in production.
   *
   * @throws IOException
   */
  void sanityCheck() throws IOException {
    buf.rewind();
    assert BlockType.read(buf) == blockType;
    assert buf.getInt() == onDiskSizeWithoutHeader;
    assert buf.getInt() == uncompressedSizeWithoutHeader;
    assert buf.getLong() == prevBlockOffset;

    int expectedBufLimit = uncompressedSizeWithoutHeader + HEADER_SIZE;
    if (buf.limit() != expectedBufLimit) {
      throw new IOException("Expected buffer limit " + expectedBufLimit
          + ", got " + buf.limit());
    }

    // We might optionally allocate a few more bytes to read the next block's
    // header.
    assert buf.capacity() == uncompressedSizeWithoutHeader + HEADER_SIZE ||
           buf.capacity() == uncompressedSizeWithoutHeader + 2 * HEADER_SIZE;
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
        + Bytes.toStringBinary(buf.array(), buf.arrayOffset() + HEADER_SIZE,
            Math.min(32, buf.limit() - buf.arrayOffset() - HEADER_SIZE))
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
   *
   * @param extraBytes whether to reserve room in the buffer to read the next
   *          block's header
   */
  private void allocateBuffer(boolean extraBytes) {
    int capacityNeeded = HEADER_SIZE + uncompressedSizeWithoutHeader +
        (extraBytes ? HEADER_SIZE : 0);

    ByteBuffer newBuf = ByteBuffer.allocate(capacityNeeded);

    // Copy header bytes.
    System.arraycopy(buf.array(), buf.arrayOffset(), newBuf.array(),
        newBuf.arrayOffset(), HEADER_SIZE);

    buf = newBuf;
    buf.limit(HEADER_SIZE + uncompressedSizeWithoutHeader);
  }

  /** An additional sanity-check in case no compression is being used. */
  public void assumeUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader);
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
        buf.arrayOffset() + HEADER_SIZE, buf.limit() - HEADER_SIZE));
  }

  @Override
  public long heapSize() {
    long size = HFILE_BLOCK_OVERHEAD;

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
   * Used in maintaining total unencoded size stats in the block cache.
   * @return unencoded size of the given cache block
   */
  public static long getUnencodedSize(Cacheable buf) {
    Preconditions.checkNotNull(buf);

    if (buf instanceof HFileBlock) {
      HFileBlock b = (HFileBlock) buf;
      if (b.blockType == BlockType.ENCODED_DATA) {
        short encodingId = b.getDataBlockEncodingId();
        DataBlockEncoding encoding = DataBlockEncoding.getEncodingById(encodingId);
        Preconditions.checkNotNull(encoding);
        DataBlockEncoder encoder = encoding.getEncoder();
        Preconditions.checkNotNull(encoder);
        int unencodedSize = encoder.getUnencodedSize(b.getBufferWithoutHeader());
        Preconditions.checkState(unencodedSize >= -1);
        if (unencodedSize >= 0) {
          return ClassSize.align(HFILE_BLOCK_OVERHEAD + HEADER_SIZE + unencodedSize);
        }
      }
    }
    return buf.heapSize();
  }

  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern
   * is as follows:
   * <ul>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression
   * algorithm
   * <li>Call {@link Writer#startWriting(BlockType)} and get a data stream to
   * write to
   * <li>Write your data into the stream
   * <li>Call {@link Writer#writeHeaderAndData(java.io.DataOutputStream)} as many times as you
   * need to store the serialized block into an external stream, or call
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

    private DataBlockEncoder.EncodedWriter encodedWriter;

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

    /** Current block type. Set in {@link #startWriting(BlockType)}. */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to.
     */
    private DataOutputStream userDataStream;

    /**
     * Bytes to be written to the file system, including the header. Compressed
     * if compression is turned on.
     */
    private byte[] onDiskBytesWithHeader;

    /**
     * Valid in the READY state. Contains the header and the uncompressed (but
     * potentially encoded, if this is a data block) bytes, so the length is
     * {@link #uncompressedSizeWithoutHeader} + {@link HFileBlock#HEADER_SIZE}.
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

    public void appendEncodedKV(final long memstoreTS, final byte[] key,
        final int keyOffset, final int keyLength, final byte[] value,
        final int valueOffset, final int valueLength) throws IOException {
      if (encodedWriter == null) {
        throw new IOException("Must initialize encoded writer");
      }
      encodedWriter.update(memstoreTS, key, keyOffset, keyLength, value,
          valueOffset, valueLength);
    }

    /**
     * @param compressionAlgorithm compression algorithm to use
     * @param dataBlockEncoder data block encoding algorithm to use
     */
    public Writer(Compression.Algorithm compressionAlgorithm,
          HFileDataBlockEncoder dataBlockEncoder, boolean includesMemstoreTS) {
      compressAlgo = compressionAlgorithm == null ? NONE : compressionAlgorithm;
      this.dataBlockEncoder = dataBlockEncoder != null
          ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;
      this.encodedWriter = null;

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

      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i)
        prevOffsetByType[i] = -1;

      this.includesMemstoreTS = includesMemstoreTS;
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

      // We only encode data blocks.
      if (this.blockType == BlockType.DATA) {
        this.encodedWriter = this.dataBlockEncoder.getEncodedWriter(this.userDataStream,
            this.includesMemstoreTS);
        this.encodedWriter.reserveMetadataSpace();
        return null;  // The caller should use appendEncodedKV
      } else {
        this.encodedWriter = null;
        return userDataStream;
      }
    }

    /**
     * Used to get the stream for the user to write data block contents into. This is unsafe and
     * should only be used for testing. Key/value pairs should be appended using
     * {@link #appendEncodedKV(long, byte[], int, int, byte[], int, int)}.
     */
    DataOutputStream getUserDataStreamUnsafe() {
      return userDataStream;
    }

    /**
     * Transitions the block writer from the "writing" state to the "block
     * ready" state.  Does nothing if a block is already finished.
     *
     * @throws IOException
     */
    private void ensureBlockReady() throws IOException {
      if (state == State.INIT) {
        // We don't have any data in this case.
        throw new IllegalStateException(state.toString());
      }
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

      doCompression();
      putHeader(uncompressedBytesWithHeader, 0, onDiskBytesWithHeader.length,
          uncompressedBytesWithHeader.length);
    }

    /**
     * Do compression if it is enabled, or re-use the uncompressed buffer if
     * it is not. Fills in the compressed block's header if doing compression.
     */
    private void doCompression() throws IOException {
      // do the compression
      if (compressAlgo != NONE) {
        compressedByteStream.reset();
        compressedByteStream.write(DUMMY_HEADER);

        compressionStream.resetState();

        compressionStream.write(uncompressedBytesWithHeader, HEADER_SIZE,
            uncompressedBytesWithHeader.length - HEADER_SIZE);

        compressionStream.flush();
        compressionStream.finish();

        onDiskBytesWithHeader = compressedByteStream.toByteArray();
        putHeader(onDiskBytesWithHeader, 0, onDiskBytesWithHeader.length,
            uncompressedBytesWithHeader.length);
      } else {
        onDiskBytesWithHeader = uncompressedBytesWithHeader;
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

      if (this.encodedWriter == null) {
        throw new IOException("All data blocks must use an encoded writer");
      }

      if (this.dataBlockEncoder.finishEncoding(uncompressedBytesWithHeader,
          HEADER_SIZE, uncompressedBytesWithHeader.length -
          HEADER_SIZE, this.encodedWriter)) {
        this.blockType = BlockType.ENCODED_DATA;
      }
      this.encodedWriter = null;
    }

    /**
     * Put the header into the given byte array at the given offset.
     * @param onDiskSize size of the block on disk
     * @param uncompressedSize size of the block after decompression (but
     *          before optional data block decoding)
     */
    private void putHeader(byte[] dest, int offset, int onDiskSize,
        int uncompressedSize) {
      offset = blockType.put(dest, offset);
      offset = Bytes.putInt(dest, offset, onDiskSize - HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSize - HEADER_SIZE);
      Bytes.putLong(dest, offset, prevOffset);
    }

    /**
     * Similar to {@link #writeHeaderAndData(DataOutputStream)}, but records
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
    void writeHeaderAndData(DataOutputStream out) throws IOException {
      ensureBlockReady();
      out.write(onDiskBytesWithHeader);
    }

    /**
     * Returns the header or the compressed data (or uncompressed data when not
     * using compression) as a byte array. Can be called in the "writing" state
     * or in the "block ready" state. If called in the "writing" state,
     * transitions the writer to the "block ready" state.
     *
     * @return header and data as they would be stored on disk in a byte array
     * @throws IOException
     */
    public byte[] getHeaderAndData() throws IOException {
      ensureBlockReady();
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
    public int getOnDiskSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBytesWithHeader.length - HEADER_SIZE;
    }

    /**
     * Returns the on-disk size of the block. Can only be called in the
     * "block ready" state.
     *
     * @return the on-disk size of the block ready to be written, including the
     *         header size
     */
    public int getOnDiskSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBytesWithHeader.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    public int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return uncompressedBytesWithHeader.length - HEADER_SIZE;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    public int getUncompressedSizeWithHeader() {
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
     *
     * @return uncompressed block bytes for caching on write
     */
    private byte[] getUncompressedDataWithHeader() {
      expectState(State.BLOCK_READY);

      return uncompressedBytesWithHeader;
    }

    private void expectState(State expectedState) {
      if (state != expectedState) {
        throw new IllegalStateException("Expected state: " + expectedState +
            ", actual state: " + state);
      }
    }

    /**
     * Similar to {@link #getUncompressedDataWithHeader()} but returns a byte
     * buffer.
     *
     * @return uncompressed block for caching on write in the form of a buffer
     */
    public ByteBuffer getUncompressedBufferWithHeader() {
      byte[] b = getUncompressedDataWithHeader();
      return ByteBuffer.wrap(b, 0, b.length);
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

    public HFileBlock getBlockForCaching() {
      return new HFileBlock(blockType, getOnDiskSizeWithoutHeader(),
          getUncompressedSizeWithoutHeader(), prevOffset,
          getUncompressedBufferWithHeader(), DONT_FILL_HEADER, startOffset,
          includesMemstoreTS);
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
     * exception if incorrect, and returns the data portion of the block as
     * an input stream.
     */
    DataInputStream nextBlockAsStream(BlockType blockType) throws IOException;
  }

  /** A full-fledged reader with an iteration ability. */
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
     * @param addToL2Cache if true add the compressed block to L2 cache
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize,
        int uncompressedSize, boolean addToL2Cache) throws IOException;

    /**
     * Reads the block at the given offset in the file with the given on-disk
     * size and uncompressed size.
     *
     * @param offset
     * @param onDiskSize the on-disk size of the entire block, including all
     *          applicable headers, or -1 if unknown
     * @param uncompressedSize the uncompressed size of the compressed part of
     *          the block, or -1 if unknown
     * @param addToL2Cache if true add the compressed block to L2 cache
     * @param options the options for reading
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize,
        int uncompressedSize, boolean addToL2Cache, ReadOptions options)
        throws IOException;

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
  public abstract static class AbstractFSReader implements FSReader {

    /** The file system stream of the underlying {@link HFile} */
    protected FSDataInputStream istream;

    /** Compression algorithm used by the {@link HFile} */
    protected Compression.Algorithm compressAlgo;

    /** The size of the file we are reading from, or -1 if unknown. */
    protected long fileSize;

    /** The default buffer size for our buffered streams */
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    public AbstractFSReader(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) {
      this.istream = istream;
      this.compressAlgo = compressAlgo;
      this.fileSize = fileSize;
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
          HFileBlock b = readBlockData(offset, -1, -1, false, new ReadOptions());
          offset += b.getOnDiskSizeWithHeader();
          return b;
        }

        @Override
        public DataInputStream nextBlockAsStream(BlockType blockType)
            throws IOException {
          HFileBlock blk = nextBlock();
          if (blk.getBlockType() != blockType) {
            throw new IOException("Expected block of type " + blockType
                + " but " + "found " + blk.getBlockType());
          }
          return blk.getByteStream();
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
     * @return the on-disk size of the next block with header size included, or
     *         -1 if it could not be determined
     * @throws IOException
     */
    protected int readAtOffset(byte[] dest, int destOffset, int size,
        boolean peekIntoNextBlock, long fileOffset, ReadOptions options)
            throws IOException {
      if (peekIntoNextBlock &&
          destOffset + size + HEADER_SIZE > dest.length) {
        // We are asked to read the next block's header as well, but there is
        // not enough room in the array.
        throw new IOException("Attempted to read " + size + " bytes and " +
            HEADER_SIZE + " bytes of next header into a " + dest.length +
            "-byte array at offset " + destOffset);
      }

      Call callContext = HRegionServer.callContext.get();
      ProfilingData pData = (callContext == null)? null : callContext.getProfilingData();

      long t0, t1;
      long timeToRead;
      if (Store.isPread) {
        // Positional read. Better for random reads.
        int extraSize = peekIntoNextBlock ? HEADER_SIZE : 0;

        t0 = EnvironmentEdgeManager.currentTimeMillis();
        numPositionalRead.incrementAndGet();
        int sizeToRead = size + extraSize;
        int sizeRead = 0;
        if (sizeToRead > 0) {
          if (istream instanceof DFSDataInputStream) {
            sizeRead = ((DFSDataInputStream) istream).read(fileOffset, dest,
                destOffset, sizeToRead, options);
          } else {
            sizeRead = istream.read(fileOffset, dest, destOffset, sizeToRead);
          }
          if (pData != null) {
            t1 = EnvironmentEdgeManager.currentTimeMillis();
            timeToRead = t1 - t0;
            pData.addToHist(ProfilingData.HFILE_BLOCK_P_READ_TIME_MS, timeToRead);
          }
          if (size == 0 && sizeRead == -1) {
            // a degenerate case of a zero-size block and no next block header.
            sizeRead = 0;
          }
          if (sizeRead < size) {
            throw new IOException("Positional read of " + sizeToRead + " bytes " +
                "failed at offset " + fileOffset + " (returned " + sizeRead + ")");
          }
        }

        if (sizeRead == size || sizeRead < sizeToRead) {
          // Could not read the next block's header, or did not try.
          return -1;
        }
      } else {
        // Seek + read. Better for scanning.
        synchronized (istream) {
          numSeekRead.incrementAndGet();
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
          if (!readWithExtra(istream, dest, destOffset, size, HEADER_SIZE)){
            return -1;
          }
        }
      }
      assert peekIntoNextBlock;
      return Bytes.toInt(dest, destOffset + size + BlockType.MAGIC_LENGTH) +
          HEADER_SIZE;
    }

    /**
     * Decompresses data from the given stream using the configured compression
     * algorithm.
     *
     * @param uncompressedSize
     *          uncompressed data size, header not included
     * @return the byte buffer containing the given header (optionally) and the
     *         decompressed data
     * @throws IOException
     */
    protected void decompress(byte[] dest, int destOffset,
        InputStream bufferedBoundedStream,
        int uncompressedSize) throws IOException {
      decompress(compressAlgo, dest, destOffset, bufferedBoundedStream,
          uncompressedSize);
    }

    /**
     * Decompresses a given stream using a specified compression algorithm.
     * </p>
     * This method is static so that it can be used by methods that construct
     * an HFileBlock from a raw byte array.
     * @param compressAlgo The specified compression algorithm
     * @param dest Write decompressed bytes into this byte array
     * @param destOffset  Offset within the dest byte array
     * @param bufferedBoundedStream Input stream from which compressed data is
     *                              read
     * @param uncompressedSize The expected un-compressed size of the data
     * @throws IOException If there is an error during de-compression
     */
    protected static void decompress(Algorithm compressAlgo, byte[] dest,
        int destOffset, InputStream bufferedBoundedStream,
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
     * @return a stream restricted to the given portion of the file
     */
    protected InputStream createBufferedBoundedStream(long offset,
        int size) {
      return new BufferedInputStream(new BoundedRangeFileInputStream(istream,
          offset, size), Math.min(DEFAULT_BUFFER_SIZE, size));
    }

  }

  /**
   * Reads version 1 blocks from the file system. In version 1 blocks,
   * everything is compressed, including the magic record, if compression is
   * enabled. Everything might be uncompressed if no compression is used. This
   * reader returns blocks represented in the uniform version 2 format in
   * memory.
   */
  public static class FSReaderV1 extends AbstractFSReader {

    /** Header size difference between version 1 and 2 */
    private static final int HEADER_DELTA = HEADER_SIZE - MAGIC_LENGTH;

    public FSReaderV1(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize) {
      super(istream, compressAlgo, fileSize);
    }

    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithMagic,
        int uncompressedSizeWithMagic, boolean addToL2Cache)
      throws IOException {
      return readBlockData(offset, onDiskSizeWithMagic,
          uncompressedSizeWithMagic, addToL2Cache, new ReadOptions());
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
     * @param options the options for reading
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithMagic,
        int uncompressedSizeWithMagic,
        boolean addToL2Cache, ReadOptions options) throws IOException {
      if (uncompressedSizeWithMagic <= 0) {
        throw new IOException("Invalid uncompressedSize="
            + uncompressedSizeWithMagic + " for a version 1 " + "block");
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
              + " must be equal for version 1 with no " + "compression");
        }

        // The first MAGIC_LENGTH bytes of what this will read will be
        // overwritten.
        readAtOffset(buf.array(), buf.arrayOffset() + HEADER_DELTA,
            onDiskSize, false, offset, options);

        onDiskSizeWithoutHeader = uncompressedSizeWithMagic - MAGIC_LENGTH;
      } else {
        // This is a "seek + read" approach.
        numSeekRead.incrementAndGet();

        InputStream bufferedBoundedStream = createBufferedBoundedStream(
            offset, onDiskSize);
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
          offset, MemStore.NO_PERSISTENT_TS);
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
  public static class FSReaderV2 extends AbstractFSReader {

    /** L2 cache instance or null if l2 cache is disabled */
    private final L2CacheAgent cacheAgent;

    /**
     * Name of the current hfile. Used to compose the key in for the
     * L2 cache if enabled.
     */
    private final String hfileNameForL2Cache;

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

    public FSReaderV2(FSDataInputStream istream, Algorithm compressAlgo,
        long fileSize, L2CacheAgent cacheAgent, String hfileNameForL2Cache) {
      super(istream, compressAlgo, fileSize);
      this.cacheAgent = cacheAgent;
      this.hfileNameForL2Cache = hfileNameForL2Cache;
    }

    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL,
        int uncompressedSize, boolean addToL2Cache) throws IOException {
      return readBlockData(offset, onDiskSizeWithHeaderL, uncompressedSize,
          addToL2Cache, new ReadOptions());
    }

    /**
     * Reads a version 2 block. Tries to do as little memory allocation as
     * possible, using the provided on-disk size.
     *
     * @param offset
     *          the offset in the stream to read at
     * @param onDiskSizeWithHeaderL
     *          the on-disk size of the block, including the header, or -1 if
     *          unknown
     * @param uncompressedSize
     *          the uncompressed size of the the block. Always expected to be
     *          -1. This parameter is only used in version 1.
     * @param addToL2Cache
     *          if true, will cache the block on read in the L2
     *          cache, if the L2 cache is enabled.
     * @param options
     *          the options for reading
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL,
        int uncompressedSize, boolean addToL2Cache,
        ReadOptions options) throws IOException {
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read "
            + "block (onDiskSize=" + onDiskSizeWithHeaderL
            + ", uncompressedSize=" + uncompressedSize + ")");
      }
      if (uncompressedSize != -1) {
        throw new IOException("Version 2 block reader API does not need " +
            "the uncompressed size parameter");
      }

      if ((onDiskSizeWithHeaderL < HEADER_SIZE && onDiskSizeWithHeaderL != -1)
          || onDiskSizeWithHeaderL >= Integer.MAX_VALUE) {
        throw new IOException("Invalid onDisksize=" + onDiskSizeWithHeaderL
            + ": " + "expected to be at least " + HEADER_SIZE
            + " and at most " + Integer.MAX_VALUE + ", or -1 (" + "offset="
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

        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - HEADER_SIZE;
        assert onDiskSizeWithoutHeader >= 0;

        // See if we can avoid reading the header. This is desirable, because
        // we will not incur a seek operation to seek back if we have already
        // read this block's header as part of the previous read's look-ahead.
        PrefetchedHeader prefetchedHeader = prefetchedHeaderForThread.get();
        byte[] header = prefetchedHeader.offset == offset
            ? prefetchedHeader.header : null;

        // Size that we have to skip in case we have already read the header.
        int preReadHeaderSize = header == null ? 0 : HEADER_SIZE;

        if (compressAlgo == Compression.Algorithm.NONE) {
          // Just read the whole thing. Allocate enough space to read the
          // next block's header too.

          ByteBuffer headerAndData = ByteBuffer.allocate(onDiskSizeWithHeader
              + HEADER_SIZE);
          headerAndData.limit(onDiskSizeWithHeader);

          if (header != null) {
            System.arraycopy(header, 0, headerAndData.array(), 0,
                HEADER_SIZE);
          }

          int nextBlockOnDiskSizeWithHeader = readAtOffset(
              headerAndData.array(), headerAndData.arrayOffset()
                  + preReadHeaderSize, onDiskSizeWithHeader
                  - preReadHeaderSize, true, offset + preReadHeaderSize, options);
          b = new HFileBlock(headerAndData);
          b.assumeUncompressed();
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSizeWithHeader;

          if (b.nextBlockOnDiskSizeWithHeader > 0)
            setNextBlockHeader(offset, b);

          if (addToL2Cache) {
            cacheRawBlockBytes(b.getBlockType(), offset, headerAndData.array());
          }
        } else {
          // Allocate enough space to fit the next block's header too.
          byte[] onDiskBlock = new byte[onDiskSizeWithHeader + HEADER_SIZE];

          int nextBlockOnDiskSize = readAtOffset(onDiskBlock,
              preReadHeaderSize, onDiskSizeWithHeader - preReadHeaderSize,
              true, offset + preReadHeaderSize, options);

          if (header == null) {
            header = onDiskBlock;
          }
          try {
            b = new HFileBlock(ByteBuffer.wrap(header, 0, HEADER_SIZE));
          } catch (IOException ex) {
            // Seen in load testing. Provide comprehensive debug info.
            throw new IOException("Failed to read compressed block at "
                + offset + ", onDiskSizeWithoutHeader=" + onDiskSizeWithHeader
                + ", preReadHeaderSize=" + preReadHeaderSize
                + ", header.length=" + header.length + ", " + "header bytes: "
                + Bytes.toStringBinary(header, 0, HEADER_SIZE), ex);
          }
          b.validateOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader);
          b.nextBlockOnDiskSizeWithHeader = nextBlockOnDiskSize;
          if (addToL2Cache && cacheAgent != null &&
                  cacheAgent.isL2CacheEnabled()) {
            if (preReadHeaderSize > 0) {
              // If we plan to add block to L2 cache, we need to copy the
              // header information into the byte array so that it can be
              // cached in the L2 cache.
              System.arraycopy(header, 0, onDiskBlock, 0, preReadHeaderSize);
            }
            cacheRawBlockBytes(b.getBlockType(), offset, onDiskBlock);
          }
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              onDiskBlock, HEADER_SIZE, onDiskSizeWithoutHeader));

          // This will allocate a new buffer but keep header bytes.
          b.allocateBuffer(b.nextBlockOnDiskSizeWithHeader > 0);

          decompress(b.buf.array(), b.buf.arrayOffset() + HEADER_SIZE, dis,
              b.uncompressedSizeWithoutHeader);

          // Copy next block's header bytes into the new block if we have them.
          if (nextBlockOnDiskSize > 0) {
            System.arraycopy(onDiskBlock, onDiskSizeWithHeader, b.buf.array(),
                b.buf.arrayOffset() + HEADER_SIZE
                    + b.uncompressedSizeWithoutHeader, HEADER_SIZE);

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
          headerBuf = ByteBuffer.allocate(HEADER_SIZE);;
          readAtOffset(headerBuf.array(), headerBuf.arrayOffset(), HEADER_SIZE,
              false, offset, options);
        }

        b = new HFileBlock(headerBuf);

        // This will also allocate enough room for the next block's header.
        b.allocateBuffer(true);

        if (compressAlgo == Compression.Algorithm.NONE) {

          // Avoid creating bounded streams and using a "codec" that does
          // nothing.
          b.assumeUncompressed();
          b.nextBlockOnDiskSizeWithHeader = readAtOffset(b.buf.array(),
              b.buf.arrayOffset() + HEADER_SIZE,
              b.uncompressedSizeWithoutHeader, true, offset + HEADER_SIZE,
              options);
          if (addToL2Cache) {
            cacheRawBlockBytes(b.getBlockType(), offset, b.buf.array());
          }
          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            setNextBlockHeader(offset, b);
          }
        } else {
          // Allocate enough space for the block's header and compressed data.
          byte[] compressedBytes = new byte[b.getOnDiskSizeWithHeader()
              + HEADER_SIZE];
          b.nextBlockOnDiskSizeWithHeader = readAtOffset(compressedBytes,
              HEADER_SIZE, b.onDiskSizeWithoutHeader, true, offset
                  + HEADER_SIZE, options);
          if (addToL2Cache && cacheAgent != null &&
                  cacheAgent.isL2CacheEnabled()) {
            // If l2 cache is enabled, we need to copy the header bytes to
            // the compressed bytes array, so that they can be cached in the
            // L2 cache.
            System.arraycopy(headerBuf.array(), 0, compressedBytes, 0,
                HEADER_SIZE);
            cacheRawBlockBytes(b.getBlockType(), offset, compressedBytes);
          }
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
              compressedBytes, HEADER_SIZE, b.onDiskSizeWithoutHeader));

          decompress(b.buf.array(), b.buf.arrayOffset() + HEADER_SIZE, dis,
              b.uncompressedSizeWithoutHeader);

          if (b.nextBlockOnDiskSizeWithHeader > 0) {
            // Copy the next block's header into the new block.
            int nextHeaderOffset = b.buf.arrayOffset() + HEADER_SIZE
                + b.uncompressedSizeWithoutHeader;
            System.arraycopy(compressedBytes,
                compressedBytes.length - HEADER_SIZE,
                b.buf.array(),
                nextHeaderOffset,
                HEADER_SIZE);

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
      int nextHeaderOffset = b.buf.arrayOffset() + HEADER_SIZE
          + b.uncompressedSizeWithoutHeader;
      System.arraycopy(b.buf.array(), nextHeaderOffset,
          prefetchedHeader.header, 0, HEADER_SIZE);
    }

    void setIncludesMemstoreTS(boolean enabled) {
      includesMemstoreTS = enabled;
    }

    void setDataBlockEncoder(HFileDataBlockEncoder encoder) {
      this.dataBlockEncoder = encoder;
    }

    /**
     * If L2 cache is enabled, associates current hfile name and
     * offset with the specified byte array representing the
     * compressed and encoded block.
     * @param offset The block's offset within the hfile
     * @param blockBytes The block's bytes as they appear on disk (i.e.,
     *                   correctly encoded and compressed)
     */
    private void cacheRawBlockBytes(BlockType type, long offset,
                                    byte[] blockBytes) {
      if (cacheAgent != null) {
        BlockCacheKey cacheKey = new BlockCacheKey(hfileNameForL2Cache, offset);
        RawHFileBlock rawBlock = new RawHFileBlock(type, blockBytes);
        cacheAgent.cacheRawBlock(cacheKey, rawBlock);
      }
    }
  }

  public static long getNumSeekAndReadOperations() {
    return numSeekRead.get();
  }

  public static long getNumPositionalReadOperations() {
    return numPositionalRead.get();
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

}
