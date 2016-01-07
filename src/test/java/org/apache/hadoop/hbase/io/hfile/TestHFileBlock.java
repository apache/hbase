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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.DoubleOutputStream;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestHFileBlock {
  // change this value to activate more logs
  private static final boolean detailedLogging = false;
  private static final boolean[] BOOLEAN_VALUES = new boolean[] { false, true };

  private static final Log LOG = LogFactory.getLog(TestHFileBlock.class);

  static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = {
      NONE, GZ };

  private static final int NUM_TEST_BLOCKS = 1000;
  private static final int NUM_READER_THREADS = 26;

  // Used to generate KeyValues
  private static int NUM_KEYVALUES = 50;
  private static int FIELD_LENGTH = 10;
  private static float CHANCE_TO_REPEAT = 0.6f;

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private FileSystem fs;
  private int uncompressedSizeV1;

  private final boolean includesMemstoreTS;

  public TestHFileBlock(boolean includesMemstoreTS) {
    this.includesMemstoreTS = includesMemstoreTS;
  }

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  @Before
  public void setUp() throws IOException {
    fs = HFileSystem.get(TEST_UTIL.getConfiguration());
  }

  static void writeTestBlockContents(DataOutputStream dos) throws IOException {
    // This compresses really well.
    for (int i = 0; i < 1000; ++i)
      dos.writeInt(i / 100);
  }

 static int writeTestKeyValues(OutputStream dos, int seed, boolean includesMemstoreTS)
      throws IOException {
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    Random randomizer = new Random(42l + seed); // just any fixed number

    // generate keyValues
    for (int i = 0; i < NUM_KEYVALUES; ++i) {
      byte[] row;
      long timestamp;
      byte[] family;
      byte[] qualifier;
      byte[] value;

      // generate it or repeat, it should compress well
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        row = keyValues.get(randomizer.nextInt(keyValues.size())).getRow();
      } else {
        row = new byte[FIELD_LENGTH];
        randomizer.nextBytes(row);
      }
      if (0 == i) {
        family = new byte[FIELD_LENGTH];
        randomizer.nextBytes(family);
      } else {
        family = keyValues.get(0).getFamily();
      }
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        qualifier = keyValues.get(
            randomizer.nextInt(keyValues.size())).getQualifier();
      } else {
        qualifier = new byte[FIELD_LENGTH];
        randomizer.nextBytes(qualifier);
      }
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        value = keyValues.get(randomizer.nextInt(keyValues.size())).getValue();
      } else {
        value = new byte[FIELD_LENGTH];
        randomizer.nextBytes(value);
      }
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        timestamp = keyValues.get(
            randomizer.nextInt(keyValues.size())).getTimestamp();
      } else {
        timestamp = randomizer.nextLong();
      }

      keyValues.add(new KeyValue(row, family, qualifier, timestamp, value));
    }

    // sort it and write to stream
    int totalSize = 0;
    Collections.sort(keyValues, KeyValue.COMPARATOR);
    DataOutputStream dataOutputStream = new DataOutputStream(dos);
    for (KeyValue kv : keyValues) {
      totalSize += kv.getLength();
      dataOutputStream.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      if (includesMemstoreTS) {
        long memstoreTS = randomizer.nextLong();
        WritableUtils.writeVLong(dataOutputStream, memstoreTS);
        totalSize += WritableUtils.getVIntSize(memstoreTS);
      }
    }

    return totalSize;
  }

  public byte[] createTestV1Block(Compression.Algorithm algo)
      throws IOException {
    Compressor compressor = algo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = algo.createCompressionStream(baos, compressor, 0);
    DataOutputStream dos = new DataOutputStream(os);
    BlockType.META.write(dos); // Let's make this a meta block.
    writeTestBlockContents(dos);
    uncompressedSizeV1 = dos.size();
    dos.flush();
    algo.returnCompressor(compressor);
    return baos.toByteArray();
  }

  static HFileBlock.Writer createTestV2Block(Compression.Algorithm algo,
      boolean includesMemstoreTS) throws IOException {
    final BlockType blockType = BlockType.DATA;
    HFileBlock.Writer hbw = new HFileBlock.Writer(algo, null,
        includesMemstoreTS, HFileReaderV2.MAX_MINOR_VERSION,
        HFile.DEFAULT_CHECKSUM_TYPE,
        HFile.DEFAULT_BYTES_PER_CHECKSUM);
    DataOutputStream dos = hbw.startWriting(blockType);
    writeTestBlockContents(dos);
    dos.flush();
    byte[] headerAndData = hbw.getHeaderAndDataForTest();
    assertEquals(1000 * 4, hbw.getUncompressedSizeWithoutHeader());
    hbw.releaseCompressor();
    return hbw;
  }

  public String createTestBlockStr(Compression.Algorithm algo,
      int correctLength) throws IOException {
    HFileBlock.Writer hbw = createTestV2Block(algo, includesMemstoreTS);
    byte[] testV2Block = hbw.getHeaderAndDataForTest();
    int osOffset = HFileBlock.HEADER_SIZE_WITH_CHECKSUMS + 9;
    if (testV2Block.length == correctLength) {
      // Force-set the "OS" field of the gzip header to 3 (Unix) to avoid
      // variations across operating systems.
      // See http://www.gzip.org/zlib/rfc-gzip.html for gzip format.
      // We only make this change when the compressed block length matches.
      // Otherwise, there are obviously other inconsistencies.
      testV2Block[osOffset] = 3;
    }
    return Bytes.toStringBinary(testV2Block);
  }

  @Test
  public void testNoCompression() throws IOException {
    assertEquals(4000, createTestV2Block(NONE, includesMemstoreTS).
                 getBlockForCaching().getUncompressedSizeWithoutHeader());
  }

  @Test
  public void testGzipCompression() throws IOException {
    final String correctTestBlockStr =
        "DATABLK*\\x00\\x00\\x00>\\x00\\x00\\x0F\\xA0\\xFF\\xFF\\xFF\\xFF"
            + "\\xFF\\xFF\\xFF\\xFF"
            + "\\x01\\x00\\x00@\\x00\\x00\\x00\\x00["
            // gzip-compressed block: http://www.gzip.org/zlib/rfc-gzip.html
            + "\\x1F\\x8B"  // gzip magic signature
            + "\\x08"  // Compression method: 8 = "deflate"
            + "\\x00"  // Flags
            + "\\x00\\x00\\x00\\x00"  // mtime
            + "\\x00"  // XFL (extra flags)
            // OS (0 = FAT filesystems, 3 = Unix). However, this field
            // sometimes gets set to 0 on Linux and Mac, so we reset it to 3.
            // This appears to be a difference caused by the availability
            // (and use) of the native GZ codec.
            + "\\x03"
            + "\\xED\\xC3\\xC1\\x11\\x00 \\x08\\xC00DD\\xDD\\x7Fa"
            + "\\xD6\\xE8\\xA3\\xB9K\\x84`\\x96Q\\xD3\\xA8\\xDB\\xA8e\\xD4c"
            + "\\xD46\\xEA5\\xEA3\\xEA7\\xE7\\x00LI\\x5Cs\\xA0\\x0F\\x00\\x00"
            + "\\x00\\x00\\x00\\x00"; //  4 byte checksum (ignored)
    final int correctGzipBlockLength = 95;
    final String testBlockStr = createTestBlockStr(GZ, correctGzipBlockLength);
    // We ignore the block checksum because createTestBlockStr can change the
    // gzip header after the block is produced
    assertEquals(correctTestBlockStr.substring(0, correctGzipBlockLength - 4),
      testBlockStr.substring(0, correctGzipBlockLength - 4));
  }

  @Test
  public void testReaderV1() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        byte[] block = createTestV1Block(algo);
        Path path = new Path(TEST_UTIL.getDataTestDir(),
          "blocks_v1_"+ algo);
        LOG.info("Creating temporary file at " + path);
        FSDataOutputStream os = fs.create(path);
        int totalSize = 0;
        int numBlocks = 50;
        for (int i = 0; i < numBlocks; ++i) {
          os.write(block);
          totalSize += block.length;
        }
        os.close();

        FSDataInputStream is = fs.open(path);
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV1(is, algo,
            totalSize);
        HFileBlock b;
        int numBlocksRead = 0;
        long pos = 0;
        while (pos < totalSize) {
          b = hbr.readBlockData(pos, block.length, uncompressedSizeV1, pread);
          b.sanityCheck();
          pos += block.length;
          numBlocksRead++;
        }
        assertEquals(numBlocks, numBlocksRead);
        is.close();
      }
    }
  }

  @Test
  public void testReaderV2() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
          LOG.info("testReaderV2: Compression algorithm: " + algo + 
                   ", pread=" + pread);
        Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
            + algo);
        FSDataOutputStream os = fs.create(path);
        HFileBlock.Writer hbw = new HFileBlock.Writer(algo, null,
            includesMemstoreTS,
            HFileReaderV2.MAX_MINOR_VERSION,
            HFile.DEFAULT_CHECKSUM_TYPE,
            HFile.DEFAULT_BYTES_PER_CHECKSUM);
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
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
            totalSize);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        is.close();
        assertEquals(0, HFile.getChecksumFailuresCount());

        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936, 
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        String blockStr = b.toString();

        if (algo == GZ) {
          is = fs.open(path);
          hbr = new HFileBlock.FSReaderV2(is, algo, totalSize);
          b = hbr.readBlockData(0, 2173 + HFileBlock.HEADER_SIZE_WITH_CHECKSUMS +
                                b.totalChecksumBytes(), -1, pread);
          assertEquals(blockStr, b.toString());
          int wrongCompressedSize = 2172;
          try {
            b = hbr.readBlockData(0, wrongCompressedSize
                + HFileBlock.HEADER_SIZE_WITH_CHECKSUMS, -1, pread);
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
    final int numBlocks = 5;
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
          Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
              + algo + "_" + encoding.toString());
          FSDataOutputStream os = fs.create(path);
          HFileDataBlockEncoder dataBlockEncoder =
              new HFileDataBlockEncoderImpl(encoding);
          HFileBlock.Writer hbw = new HFileBlock.Writer(algo, dataBlockEncoder,
              includesMemstoreTS,
              HFileReaderV2.MAX_MINOR_VERSION,
              HFile.DEFAULT_CHECKSUM_TYPE,
              HFile.DEFAULT_BYTES_PER_CHECKSUM);
          long totalSize = 0;
          final List<Integer> encodedSizes = new ArrayList<Integer>();
          final List<ByteBuffer> encodedBlocks = new ArrayList<ByteBuffer>();
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            DataOutputStream dos = hbw.startWriting(BlockType.DATA);
            writeEncodedBlock(encoding, dos, encodedSizes, encodedBlocks,
                blockId, includesMemstoreTS);

            hbw.writeHeaderAndData(os);
            totalSize += hbw.getOnDiskSizeWithHeader();
          }
          os.close();

          FSDataInputStream is = fs.open(path);
          HFileBlock.FSReaderV2 hbr = new HFileBlock.FSReaderV2(is, algo,
              totalSize);
          hbr.setDataBlockEncoder(dataBlockEncoder);
          hbr.setIncludesMemstoreTS(includesMemstoreTS);

          HFileBlock b;
          int pos = 0;
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            b = hbr.readBlockData(pos, -1, -1, pread);
            assertEquals(0, HFile.getChecksumFailuresCount());
            b.sanityCheck();
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
            assertBuffersEqual(expectedBuffer, actualBuffer, algo, encoding,
                pread);
          }
          is.close();
        }
      }
    }
  }

  static void writeEncodedBlock(DataBlockEncoding encoding,
      DataOutputStream dos, final List<Integer> encodedSizes,
      final List<ByteBuffer> encodedBlocks, int blockId, 
      boolean includesMemstoreTS) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DoubleOutputStream doubleOutputStream =
        new DoubleOutputStream(dos, baos);

    final int rawBlockSize = writeTestKeyValues(doubleOutputStream,
        blockId, includesMemstoreTS);

    ByteBuffer rawBuf = ByteBuffer.wrap(baos.toByteArray());
    rawBuf.rewind();

    final int encodedSize;
    final ByteBuffer encodedBuf;
    if (encoding == DataBlockEncoding.NONE) {
      encodedSize = rawBlockSize;
      encodedBuf = rawBuf;
    } else {
      ByteArrayOutputStream encodedOut = new ByteArrayOutputStream();
      encoding.getEncoder().compressKeyValues(
          new DataOutputStream(encodedOut),
          rawBuf.duplicate(), includesMemstoreTS);
      // We need to account for the two-byte encoding algorithm ID that
      // comes after the 24-byte block header but before encoded KVs.
      encodedSize = encodedOut.size() + DataBlockEncoding.ID_SIZE;
      encodedBuf = ByteBuffer.wrap(encodedOut.toByteArray());
    }
    encodedSizes.add(encodedSize);
    encodedBlocks.add(encodedBuf);
  }

  static void assertBuffersEqual(ByteBuffer expectedBuffer,
      ByteBuffer actualBuffer, Compression.Algorithm compression,
      DataBlockEncoding encoding, boolean pread) {
    if (!actualBuffer.equals(expectedBuffer)) {
      int prefix = 0;
      int minLimit = Math.min(expectedBuffer.limit(), actualBuffer.limit());
      while (prefix < minLimit &&
          expectedBuffer.get(prefix) == actualBuffer.get(prefix)) {
        prefix++;
      }

      fail(String.format(
          "Content mismath for compression %s, encoding %s, " +
          "pread %s, commonPrefix %d, expected %s, got %s",
          compression, encoding, pread, prefix,
          nextBytesToStr(expectedBuffer, prefix),
          nextBytesToStr(actualBuffer, prefix)));
    }
  }

  /**
   * Convert a few next bytes in the given buffer at the given position to
   * string. Used for error messages.
   */
  private static String nextBytesToStr(ByteBuffer buf, int pos) {
    int maxBytes = buf.limit() - pos;
    int numBytes = Math.min(16, maxBytes);
    return Bytes.toStringBinary(buf.array(), buf.arrayOffset() + pos,
        numBytes) + (numBytes < maxBytes ? "..." : "");
  }

  @Test
  public void testPreviousOffset() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : BOOLEAN_VALUES) {
        for (boolean cacheOnWrite : BOOLEAN_VALUES) {
          Random rand = defaultRandom();
          LOG.info("testPreviousOffset:Compression algorithm: " + algo + 
                   ", pread=" + pread +
                   ", cacheOnWrite=" + cacheOnWrite);
          Path path = new Path(TEST_UTIL.getDataTestDir(), "prev_offset");
          List<Long> expectedOffsets = new ArrayList<Long>();
          List<Long> expectedPrevOffsets = new ArrayList<Long>();
          List<BlockType> expectedTypes = new ArrayList<BlockType>();
          List<ByteBuffer> expectedContents = cacheOnWrite
              ? new ArrayList<ByteBuffer>() : null;
          long totalSize = writeBlocks(rand, algo, path, expectedOffsets,
              expectedPrevOffsets, expectedTypes, expectedContents);

          FSDataInputStream is = fs.open(path);
          HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
              totalSize);
          long curOffset = 0;
          for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
            if (!pread) {
              assertEquals(is.getPos(), curOffset + (i == 0 ? 0 :
                  HFileBlock.HEADER_SIZE_WITH_CHECKSUMS));
            }

            assertEquals(expectedOffsets.get(i).longValue(), curOffset);
            if (detailedLogging) {
              LOG.info("Reading block #" + i + " at offset " + curOffset);
            }
            HFileBlock b = hbr.readBlockData(curOffset, -1, -1, pread);
            if (detailedLogging) {
              LOG.info("Block #" + i + ": " + b);
            }
            assertEquals("Invalid block #" + i + "'s type:",
                expectedTypes.get(i), b.getBlockType());
            assertEquals("Invalid previous block offset for block " + i
                + " of " + "type " + b.getBlockType() + ":",
                (long) expectedPrevOffsets.get(i), b.getPrevBlockOffset());
            b.sanityCheck();
            assertEquals(curOffset, b.getOffset());

            // Now re-load this block knowing the on-disk size. This tests a
            // different branch in the loader.
            HFileBlock b2 = hbr.readBlockData(curOffset,
                b.getOnDiskSizeWithHeader(), -1, pread);
            b2.sanityCheck();

            assertEquals(b.getBlockType(), b2.getBlockType());
            assertEquals(b.getOnDiskSizeWithoutHeader(),
                b2.getOnDiskSizeWithoutHeader());
            assertEquals(b.getOnDiskSizeWithHeader(),
                b2.getOnDiskSizeWithHeader());
            assertEquals(b.getUncompressedSizeWithoutHeader(),
                b2.getUncompressedSizeWithoutHeader());
            assertEquals(b.getPrevBlockOffset(), b2.getPrevBlockOffset());
            assertEquals(curOffset, b2.getOffset());
            assertEquals(b.getBytesPerChecksum(), b2.getBytesPerChecksum());
            assertEquals(b.getOnDiskDataSizeWithHeader(), 
                         b2.getOnDiskDataSizeWithHeader());
            assertEquals(0, HFile.getChecksumFailuresCount());

            curOffset += b.getOnDiskSizeWithHeader();

            if (cacheOnWrite) {
              // In the cache-on-write mode we store uncompressed bytes so we
              // can compare them to what was read by the block reader.
              // b's buffer has header + data + checksum while 
              // expectedContents have header + data only
              ByteBuffer bufRead = b.getBufferWithHeader();
              ByteBuffer bufExpected = expectedContents.get(i);
              boolean bytesAreCorrect = Bytes.compareTo(bufRead.array(),
                  bufRead.arrayOffset(), 
                  bufRead.limit() - b.totalChecksumBytes(),
                  bufExpected.array(), bufExpected.arrayOffset(),
                  bufExpected.limit()) == 0;
              String wrongBytesMsg = "";

              if (!bytesAreCorrect) {
                // Optimization: only construct an error message in case we
                // will need it.
                wrongBytesMsg = "Expected bytes in block #" + i + " (algo="
                    + algo + ", pread=" + pread
                    + ", cacheOnWrite=" + cacheOnWrite + "):\n";
                wrongBytesMsg += Bytes.toStringBinary(bufExpected.array(),
                    bufExpected.arrayOffset(), Math.min(32,
                        bufExpected.limit()))
                    + ", actual:\n"
                    + Bytes.toStringBinary(bufRead.array(),
                        bufRead.arrayOffset(), Math.min(32, bufRead.limit()));
                if (detailedLogging) {
                  LOG.warn("expected header" + 
                           HFileBlock.toStringHeader(bufExpected) +
                           "\nfound    header" + 
                           HFileBlock.toStringHeader(bufRead));
                  LOG.warn("bufread offset " + bufRead.arrayOffset() +
                           " limit " + bufRead.limit() +
                           " expected offset " + bufExpected.arrayOffset() +
                           " limit " + bufExpected.limit());
                  LOG.warn(wrongBytesMsg);
                }
              }
              assertTrue(wrongBytesMsg, bytesAreCorrect);
            }
          }

          assertEquals(curOffset, fs.getFileStatus(path).getLen());
          is.close();
        }
      }
    }
  }

  private Random defaultRandom() {
    return new Random(189237);
  }

  private class BlockReaderThread implements Callable<Boolean> {
    private final String clientId;
    private final HFileBlock.FSReader hbr;
    private final List<Long> offsets;
    private final List<BlockType> types;
    private final long fileSize;

    public BlockReaderThread(String clientId,
        HFileBlock.FSReader hbr, List<Long> offsets, List<BlockType> types,
        long fileSize) {
      this.clientId = clientId;
      this.offsets = offsets;
      this.hbr = hbr;
      this.types = types;
      this.fileSize = fileSize;
    }

    @Override
    public Boolean call() throws Exception {
      Random rand = new Random(clientId.hashCode());
      long endTime = System.currentTimeMillis() + 10000;
      int numBlocksRead = 0;
      int numPositionalRead = 0;
      int numWithOnDiskSize = 0;
      while (System.currentTimeMillis() < endTime) {
        int blockId = rand.nextInt(NUM_TEST_BLOCKS);
        long offset = offsets.get(blockId);
        boolean pread = rand.nextBoolean();
        boolean withOnDiskSize = rand.nextBoolean();
        long expectedSize =
          (blockId == NUM_TEST_BLOCKS - 1 ? fileSize
              : offsets.get(blockId + 1)) - offset;

        HFileBlock b;
        try {
          long onDiskSizeArg = withOnDiskSize ? expectedSize : -1;
          b = hbr.readBlockData(offset, onDiskSizeArg, -1, pread);
        } catch (IOException ex) {
          LOG.error("Error in client " + clientId + " trying to read block at "
              + offset + ", pread=" + pread + ", withOnDiskSize=" +
              withOnDiskSize, ex);
          return false;
        }

        assertEquals(types.get(blockId), b.getBlockType());
        assertEquals(expectedSize, b.getOnDiskSizeWithHeader());
        assertEquals(offset, b.getOffset());

        ++numBlocksRead;
        if (pread)
          ++numPositionalRead;
        if (withOnDiskSize)
          ++numWithOnDiskSize;
      }
      LOG.info("Client " + clientId + " successfully read " + numBlocksRead +
        " blocks (with pread: " + numPositionalRead + ", with onDiskSize " +
        "specified: " + numWithOnDiskSize + ")");

      return true;
    }

  }

  @Test
  public void testConcurrentReading() throws Exception {
    for (Compression.Algorithm compressAlgo : COMPRESSION_ALGORITHMS) {
      Path path =
          new Path(TEST_UTIL.getDataTestDir(), "concurrent_reading");
      Random rand = defaultRandom();
      List<Long> offsets = new ArrayList<Long>();
      List<BlockType> types = new ArrayList<BlockType>();
      writeBlocks(rand, compressAlgo, path, offsets, null, types, null);
      FSDataInputStream is = fs.open(path);
      long fileSize = fs.getFileStatus(path).getLen();
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, compressAlgo,
          fileSize);

      Executor exec = Executors.newFixedThreadPool(NUM_READER_THREADS);
      ExecutorCompletionService<Boolean> ecs =
          new ExecutorCompletionService<Boolean>(exec);

      for (int i = 0; i < NUM_READER_THREADS; ++i) {
        ecs.submit(new BlockReaderThread("reader_" + (char) ('A' + i), hbr,
            offsets, types, fileSize));
      }

      for (int i = 0; i < NUM_READER_THREADS; ++i) {
        Future<Boolean> result = ecs.take();
        assertTrue(result.get());
        if (detailedLogging) {
          LOG.info(String.valueOf(i + 1)
            + " reader threads finished successfully (algo=" + compressAlgo
            + ")");
        }
      }

      is.close();
    }
  }

  private long writeBlocks(Random rand, Compression.Algorithm compressAlgo,
      Path path, List<Long> expectedOffsets, List<Long> expectedPrevOffsets,
      List<BlockType> expectedTypes, List<ByteBuffer> expectedContents
  ) throws IOException {
    boolean cacheOnWrite = expectedContents != null;
    FSDataOutputStream os = fs.create(path);
    HFileBlock.Writer hbw = new HFileBlock.Writer(compressAlgo, null,
        includesMemstoreTS,
        HFileReaderV2.MAX_MINOR_VERSION,
        HFile.DEFAULT_CHECKSUM_TYPE,
        HFile.DEFAULT_BYTES_PER_CHECKSUM);
    Map<BlockType, Long> prevOffsetByType = new HashMap<BlockType, Long>();
    long totalSize = 0;
    for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
      long pos = os.getPos();
      int blockTypeOrdinal = rand.nextInt(BlockType.values().length);
      if (blockTypeOrdinal == BlockType.ENCODED_DATA.ordinal()) {
        blockTypeOrdinal = BlockType.DATA.ordinal();
      }
      BlockType bt = BlockType.values()[blockTypeOrdinal];
      DataOutputStream dos = hbw.startWriting(bt);
      int size = rand.nextInt(500);
      for (int j = 0; j < size; ++j) {
        // This might compress well.
        dos.writeShort(i + 1);
        dos.writeInt(j + 1);
      }

      if (expectedOffsets != null)
        expectedOffsets.add(os.getPos());

      if (expectedPrevOffsets != null) {
        Long prevOffset = prevOffsetByType.get(bt);
        expectedPrevOffsets.add(prevOffset != null ? prevOffset : -1);
        prevOffsetByType.put(bt, os.getPos());
      }

      expectedTypes.add(bt);

      hbw.writeHeaderAndData(os);
      totalSize += hbw.getOnDiskSizeWithHeader();

      if (cacheOnWrite)
        expectedContents.add(hbw.getUncompressedBufferWithHeader());

      if (detailedLogging) {
        LOG.info("Written block #" + i + " of type " + bt
            + ", uncompressed size " + hbw.getUncompressedSizeWithoutHeader()
            + " at offset " + pos);
      }
    }
    os.close();
    LOG.info("Created a temporary file at " + path + ", "
        + fs.getFileStatus(path).getLen() + " byte, compression=" +
        compressAlgo);
    return totalSize;
  }

  @Test
  public void testBlockHeapSize() {
    if (ClassSize.is32BitJVM()) {
      assertTrue(HFileBlock.BYTE_BUFFER_HEAP_SIZE == 64);
    } else {
      assertTrue(HFileBlock.BYTE_BUFFER_HEAP_SIZE == 80);
    }

    for (int size : new int[] { 100, 256, 12345 }) {
      byte[] byteArr = new byte[HFileBlock.HEADER_SIZE_WITH_CHECKSUMS + size];
      ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
      HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
          HFileBlock.FILL_HEADER, -1, includesMemstoreTS, 
          HFileBlock.MINOR_VERSION_NO_CHECKSUM, 0, ChecksumType.NULL.getCode(),
          0);
      long byteBufferExpectedSize =
          ClassSize.align(ClassSize.estimateBase(buf.getClass(), true)
              + HFileBlock.HEADER_SIZE_WITH_CHECKSUMS + size);
      long hfileBlockExpectedSize =
          ClassSize.align(ClassSize.estimateBase(HFileBlock.class, true));
      long expected = hfileBlockExpectedSize + byteBufferExpectedSize;
      assertEquals("Block data size: " + size + ", byte buffer expected " +
          "size: " + byteBufferExpectedSize + ", HFileBlock class expected " +
          "size: " + hfileBlockExpectedSize + ";", expected,
          block.heapSize());
    }
  }


  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

