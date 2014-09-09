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

import static org.apache.hadoop.hbase.io.compress.Compression.Algorithm.GZ;
import static org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Preconditions;

/**
 * This class has unit tests to prove that older versions of
 * HFiles (without checksums) are compatible with current readers.
 */
@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestHFileBlockCompatibility {
  // change this value to activate more logs
  private static final boolean[] BOOLEAN_VALUES = new boolean[] { false, true };

  private static final Log LOG = LogFactory.getLog(TestHFileBlockCompatibility.class);

  private static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = {
      NONE, GZ };

  // The mnior version for pre-checksum files
  private static int MINOR_VERSION = 0;

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private HFileSystem fs;
  private int uncompressedSizeV1;

  private final boolean includesMemstoreTS;
  private final boolean includesTag;

  public TestHFileBlockCompatibility(boolean includesMemstoreTS, boolean includesTag) {
    this.includesMemstoreTS = includesMemstoreTS;
    this.includesTag = includesTag;
  }

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.MEMSTORETS_TAGS_PARAMETRIZED;
  }

  @Before
  public void setUp() throws IOException {
    fs = (HFileSystem)HFileSystem.get(TEST_UTIL.getConfiguration());
  }

  public byte[] createTestV1Block(Compression.Algorithm algo)
      throws IOException {
    Compressor compressor = algo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = algo.createCompressionStream(baos, compressor, 0);
    DataOutputStream dos = new DataOutputStream(os);
    BlockType.META.write(dos); // Let's make this a meta block.
    TestHFileBlock.writeTestBlockContents(dos);
    uncompressedSizeV1 = dos.size();
    dos.flush();
    algo.returnCompressor(compressor);
    return baos.toByteArray();
  }

  private Writer createTestV2Block(Compression.Algorithm algo)
      throws IOException {
    final BlockType blockType = BlockType.DATA;
    Writer hbw = new Writer(algo, null,
        includesMemstoreTS, includesTag);
    DataOutputStream dos = hbw.startWriting(blockType);
    TestHFileBlock.writeTestBlockContents(dos);
    // make sure the block is ready by calling hbw.getHeaderAndData()
    hbw.getHeaderAndData();
    assertEquals(1000 * 4, hbw.getUncompressedSizeWithoutHeader());
    hbw.releaseCompressor();
    return hbw;
  }

 private String createTestBlockStr(Compression.Algorithm algo,
      int correctLength) throws IOException {
    Writer hbw = createTestV2Block(algo);
    byte[] testV2Block = hbw.getHeaderAndData();
    int osOffset = HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM + 9;
    if (testV2Block.length == correctLength) {
      // Force-set the "OS" field of the gzip header to 3 (Unix) to avoid
      // variations across operating systems.
      // See http://www.gzip.org/zlib/rfc-gzip.html for gzip format.
      testV2Block[osOffset] = 3;
    }
    return Bytes.toStringBinary(testV2Block);
  }

  @Test
  public void testNoCompression() throws IOException {
    assertEquals(4000, createTestV2Block(NONE).getBlockForCaching().
        getUncompressedSizeWithoutHeader());
  }

  @Test
  public void testGzipCompression() throws IOException {
    final String correctTestBlockStr =
        "DATABLK*\\x00\\x00\\x00:\\x00\\x00\\x0F\\xA0\\xFF\\xFF\\xFF\\xFF"
            + "\\xFF\\xFF\\xFF\\xFF"
            // gzip-compressed block: http://www.gzip.org/zlib/rfc-gzip.html
            + "\\x1F\\x8B"  // gzip magic signature
            + "\\x08"  // Compression method: 8 = "deflate"
            + "\\x00"  // Flags
            + "\\x00\\x00\\x00\\x00"  // mtime
            + "\\x00"  // XFL (extra flags)
            // OS (0 = FAT filesystems, 3 = Unix). However, this field
            // sometimes gets set to 0 on Linux and Mac, so we reset it to 3.
            + "\\x03"
            + "\\xED\\xC3\\xC1\\x11\\x00 \\x08\\xC00DD\\xDD\\x7Fa"
            + "\\xD6\\xE8\\xA3\\xB9K\\x84`\\x96Q\\xD3\\xA8\\xDB\\xA8e\\xD4c"
            + "\\xD46\\xEA5\\xEA3\\xEA7\\xE7\\x00LI\\x5Cs\\xA0\\x0F\\x00\\x00";
    final int correctGzipBlockLength = 82;

    String returnedStr = createTestBlockStr(GZ, correctGzipBlockLength);
    assertEquals(correctTestBlockStr, returnedStr);
  }

  @Test
  public void testReaderV2() throws IOException {
    if(includesTag) {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    }
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
          LOG.info("testReaderV2: Compression algorithm: " + algo +
                   ", pread=" + pread);
        Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
            + algo);
        FSDataOutputStream os = fs.create(path);
        Writer hbw = new Writer(algo, null,
            includesMemstoreTS, includesTag);
        long totalSize = 0;
        for (int blockId = 0; blockId < 2; ++blockId) {
          DataOutputStream dos = hbw.startWriting(BlockType.DATA);
          for (int i = 0; i < 1234; ++i)
            dos.writeInt(i);
          hbw.writeHeaderAndData(os);
          totalSize += hbw.getOnDiskSizeWithHeader();
        }
        os.close();

        FSDataInputStream is = fs.open(path);
        HFileContext meta = new HFileContextBuilder()
                           .withHBaseCheckSum(false)
                           .withIncludesMvcc(includesMemstoreTS)
                           .withIncludesTags(includesTag)
                           .withCompression(algo)
                           .build();
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(new FSDataInputStreamWrapper(is),
            totalSize, fs, path, meta);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        is.close();

        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936,
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        HFileBlock expected = b;

        if (algo == GZ) {
          is = fs.open(path);
          hbr = new HFileBlock.FSReaderV2(new FSDataInputStreamWrapper(is), totalSize, fs, path,
              meta);
          b = hbr.readBlockData(0, 2173 + HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM +
                                b.totalChecksumBytes(), -1, pread);
          assertEquals(expected, b);
          int wrongCompressedSize = 2172;
          try {
            b = hbr.readBlockData(0, wrongCompressedSize
                + HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM, -1, pread);
            fail("Exception expected");
          } catch (IOException ex) {
            String expectedPrefix = "On-disk size without header provided is "
                + wrongCompressedSize + ", but block header contains "
                + b.getOnDiskSizeWithoutHeader() + ".";
            assertTrue("Invalid exception message: '" + ex.getMessage()
                + "'.\nMessage is expected to start with: '" + expectedPrefix
                + "'", ex.getMessage().startsWith(expectedPrefix));
          }
          is.close();
        }
      }
    }
  }

  /**
   * Test encoding/decoding data blocks.
   * @throws IOException a bug or a problem with temporary files.
   */
  @Test
  public void testDataBlockEncoding() throws IOException {
    if(includesTag) {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    }
    final int numBlocks = 5;
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
          LOG.info("testDataBlockEncoding algo " + algo +
                   " pread = " + pread +
                   " encoding " + encoding);
          Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
              + algo + "_" + encoding.toString());
          FSDataOutputStream os = fs.create(path);
          HFileDataBlockEncoder dataBlockEncoder =
              new HFileDataBlockEncoderImpl(encoding);
          TestHFileBlockCompatibility.Writer hbw =
              new TestHFileBlockCompatibility.Writer(algo,
                  dataBlockEncoder, includesMemstoreTS, includesTag);
          long totalSize = 0;
          final List<Integer> encodedSizes = new ArrayList<Integer>();
          final List<ByteBuffer> encodedBlocks = new ArrayList<ByteBuffer>();
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            DataOutputStream dos = hbw.startWriting(BlockType.DATA);
            TestHFileBlock.writeEncodedBlock(algo, encoding, dos, encodedSizes,
                encodedBlocks, blockId, includesMemstoreTS,
                TestHFileBlockCompatibility.Writer.DUMMY_HEADER, includesTag);

            hbw.writeHeaderAndData(os);
            totalSize += hbw.getOnDiskSizeWithHeader();
          }
          os.close();

          FSDataInputStream is = fs.open(path);
          HFileContext meta = new HFileContextBuilder()
                              .withHBaseCheckSum(false)
                              .withIncludesMvcc(includesMemstoreTS)
                              .withIncludesTags(includesTag)
                              .withCompression(algo)
                              .build();
          HFileBlock.FSReaderV2 hbr = new HFileBlock.FSReaderV2(new FSDataInputStreamWrapper(is),
              totalSize, fs, path, meta);
          hbr.setDataBlockEncoder(dataBlockEncoder);
          hbr.setIncludesMemstoreTS(includesMemstoreTS);

          HFileBlock b;
          int pos = 0;
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            b = hbr.readBlockData(pos, -1, -1, pread);
            b.sanityCheck();
            if (meta.isCompressedOrEncrypted()) {
              assertFalse(b.isUnpacked());
              b = b.unpack(meta, hbr);
            }
            pos += b.getOnDiskSizeWithHeader();

            assertEquals((int) encodedSizes.get(blockId),
                b.getUncompressedSizeWithoutHeader());
            ByteBuffer actualBuffer = b.getBufferWithoutHeader();
            if (encoding != DataBlockEncoding.NONE) {
              // We expect a two-byte big-endian encoding id.
              assertEquals(0, actualBuffer.get(0));
              assertEquals(encoding.getId(), actualBuffer.get(1));
              actualBuffer.position(2);
              actualBuffer = actualBuffer.slice();
            }

            ByteBuffer expectedBuffer = encodedBlocks.get(blockId);
            expectedBuffer.rewind();

            // test if content matches, produce nice message
            TestHFileBlock.assertBuffersEqual(expectedBuffer, actualBuffer,
              algo, encoding, pread);
          }
          is.close();
        }
      }
    }
  }
  /**
   * This is the version of the HFileBlock.Writer that is used to
   * create V2 blocks with minor version 0. These blocks do not
   * have hbase-level checksums. The code is here to test
   * backward compatibility. The reason we do not inherit from
   * HFileBlock.Writer is because we never ever want to change the code
   * in this class but the code in HFileBlock.Writer will continually
   * evolve.
   */
  public static final class Writer {

    // These constants are as they were in minorVersion 0.
    private static final int HEADER_SIZE = HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    private static final boolean DONT_FILL_HEADER = HFileBlock.DONT_FILL_HEADER;
    private static final byte[] DUMMY_HEADER =
      HFileBlock.DUMMY_HEADER_NO_CHECKSUM;

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

    private HFileBlockEncodingContext dataBlockEncodingCtx;
    /** block encoding context for non-data blocks */
    private HFileBlockDefaultEncodingContext defaultBlockEncodingCtx;

    /**
     * The stream we use to accumulate data in uncompressed format for each
     * block. We reset this stream at the end of each block and reuse it. The
     * header is written as the first {@link #HEADER_SIZE} bytes into this
     * stream.
     */
    private ByteArrayOutputStream baosInMemory;

    /** Compressor, which is also reused between consecutive blocks. */
    private Compressor compressor;

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
     * if compression is turned on.
     */
    private byte[] onDiskBytesWithHeader;

    /**
     * Valid in the READY state. Contains the header and the uncompressed (but
     * potentially encoded, if this is a data block) bytes, so the length is
     * {@link #uncompressedSizeWithoutHeader} + {@link org.apache.hadoop.hbase.HConstants#HFILEBLOCK_HEADER_SIZE}.
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

    private HFileContext meta;

    public Writer(Compression.Algorithm compressionAlgorithm,
          HFileDataBlockEncoder dataBlockEncoder, boolean includesMemstoreTS, boolean includesTag) {
      compressAlgo = compressionAlgorithm == null ? NONE : compressionAlgorithm;
      this.dataBlockEncoder = dataBlockEncoder != null
          ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;

      meta = new HFileContextBuilder()
              .withHBaseCheckSum(false)
              .withIncludesMvcc(includesMemstoreTS)
              .withIncludesTags(includesTag)
              .withCompression(compressionAlgorithm)
              .build();
      defaultBlockEncodingCtx = new HFileBlockDefaultEncodingContext(null, DUMMY_HEADER, meta);
      dataBlockEncodingCtx =
          this.dataBlockEncoder.newDataBlockEncodingContext(
              DUMMY_HEADER, meta);
      baosInMemory = new ByteArrayOutputStream();

      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i)
        prevOffsetByType[i] = -1;

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
      if (blockType == BlockType.DATA) {
        encodeDataBlockForDisk();
      } else {
        defaultBlockEncodingCtx.compressAfterEncodingWithBlockType(
            uncompressedBytesWithHeader, blockType);
        onDiskBytesWithHeader =
          defaultBlockEncodingCtx.getOnDiskBytesWithHeader();
      }

      // put the header for on disk bytes
      putHeader(onDiskBytesWithHeader, 0,
          onDiskBytesWithHeader.length,
          uncompressedBytesWithHeader.length);
      //set the header for the uncompressed bytes (for cache-on-write)
      putHeader(uncompressedBytesWithHeader, 0,
          onDiskBytesWithHeader.length,
        uncompressedBytesWithHeader.length);
    }

    /**
     * Encodes this block if it is a data block and encoding is turned on in
     * {@link #dataBlockEncoder}.
     */
    private void encodeDataBlockForDisk() throws IOException {
      // do data block encoding, if data block encoder is set
      ByteBuffer rawKeyValues =
          ByteBuffer.wrap(uncompressedBytesWithHeader, HEADER_SIZE,
              uncompressedBytesWithHeader.length - HEADER_SIZE).slice();

      //do the encoding
      dataBlockEncoder.beforeWriteToDisk(rawKeyValues, dataBlockEncodingCtx, blockType);

      uncompressedBytesWithHeader =
          dataBlockEncodingCtx.getUncompressedBytesWithHeader();
      onDiskBytesWithHeader =
          dataBlockEncodingCtx.getOnDiskBytesWithHeader();
      blockType = dataBlockEncodingCtx.getBlockType();
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
     * Similar to {@link #getUncompressedBufferWithHeader()} but returns a byte
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

    /**
     * Creates a new HFileBlock.
     */
    public HFileBlock getBlockForCaching() {
      HFileContext meta = new HFileContextBuilder()
             .withHBaseCheckSum(false)
             .withChecksumType(ChecksumType.NULL)
             .withBytesPerCheckSum(0)
             .build();
      return new HFileBlock(blockType, getOnDiskSizeWithoutHeader(),
          getUncompressedSizeWithoutHeader(), prevOffset,
          getUncompressedBufferWithHeader(), DONT_FILL_HEADER, startOffset, 
          getOnDiskSizeWithoutHeader(), meta);
    }
  }

}

