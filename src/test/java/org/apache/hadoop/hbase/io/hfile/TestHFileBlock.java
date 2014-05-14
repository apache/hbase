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

import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.GZ;
import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHFileBlock {

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
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
    TEST_UTIL.setupClusterTestBuildDir();
  }

  public void writeTestBlockContents(DataOutputStream dos) throws IOException {
    // This compresses really well.
    for (int i = 0; i < 1000; ++i) {
      dos.writeInt(i / 100);
    }
  }

  private void writeTestKeyValues(OutputStream dos, Writer hbw, int seed)
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
    Collections.sort(keyValues, KeyValue.COMPARATOR);
    DataOutputStream dataOutputStream = new DataOutputStream(dos);
    for (KeyValue kv : keyValues) {
      long memstoreTS = randomizer.nextLong();
      hbw.appendEncodedKV(memstoreTS, kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
          kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());

      // Write raw key/value pair for validation.
      dataOutputStream.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      if (includesMemstoreTS) {
        WritableUtils.writeVLong(dataOutputStream, memstoreTS);
      }
    }
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

  private byte[] createTestV2Block(Compression.Algorithm algo)
      throws IOException {
    final BlockType blockType = BlockType.DATA;
    HFileBlock.Writer hbw = new HFileBlock.Writer(algo, null,
        includesMemstoreTS);
    hbw.startWriting(blockType);
    DataOutputStream dos = hbw.getUserDataStreamUnsafe();
    writeTestBlockContents(dos);
    byte[] headerAndData = hbw.getHeaderAndData();
    assertEquals(1000 * 4, hbw.getUncompressedSizeWithoutHeader());
    hbw.releaseCompressor();
    return headerAndData;
  }

  public String createTestBlockStr(Compression.Algorithm algo,
      int correctLength) throws IOException {
    byte[] testV2Block = createTestV2Block(algo);
    int osOffset = HFileBlock.HEADER_SIZE + 9;
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
    assertEquals(4000 + HFileBlock.HEADER_SIZE, createTestV2Block(NONE).length);
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
            + "\\xD46\\xEA5\\xEA3\\xEA7\\xE7\\x00LI\\s\\xA0\\x0F\\x00\\x00";
    final int correctGzipBlockLength = 82;
    assertEquals(correctTestBlockStr, createTestBlockStr(GZ,
        correctGzipBlockLength));
  }

  @Test
  public void testReaderV1() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      byte[] block = createTestV1Block(algo);
      Path path = new Path(TEST_UTIL.getTestDir(), "blocks_v1_"
          + algo);
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
        b = hbr.readBlockData(pos, block.length, uncompressedSizeV1, false);
        b.sanityCheck();
        pos += block.length;
        numBlocksRead++;
      }
      assertEquals(numBlocks, numBlocksRead);
      is.close();

    }
  }

  @Test
  public void testReaderV2() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      Path path = new Path(TEST_UTIL.getTestDir(), "blocks_v2_"
          + algo);
      FSDataOutputStream os = fs.create(path);
      HFileBlock.Writer hbw = new HFileBlock.Writer(algo, null,
          includesMemstoreTS);
      long totalSize = 0;
      for (int blockId = 0; blockId < 2; ++blockId) {
        hbw.startWriting(BlockType.DATA);
        DataOutputStream dos = hbw.getUserDataStreamUnsafe();
        for (int i = 0; i < 1234; ++i) {
          dos.writeInt(i);
        }
        hbw.writeHeaderAndData(os);
        totalSize += hbw.getOnDiskSizeWithHeader();
      }
      os.close();

      FSDataInputStream is = fs.open(path);
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
          totalSize, null, null);
      HFileBlock b = hbr.readBlockData(0, -1, -1, false);
      is.close();

      b.sanityCheck();
      assertEquals(4936, b.getUncompressedSizeWithoutHeader());
      assertEquals(algo == GZ ? 2173 : 4936, b.getOnDiskSizeWithoutHeader());
      String blockStr = b.toString();

      if (algo == GZ) {
        is = fs.open(path);
        hbr = new HFileBlock.FSReaderV2(is, algo, totalSize, null, null);
        b = hbr.readBlockData(0, 2173 + HFileBlock.HEADER_SIZE, -1, false);
        assertEquals(blockStr, b.toString());
        int wrongCompressedSize = 2172;
        try {
          b = hbr.readBlockData(0, wrongCompressedSize
              + HFileBlock.HEADER_SIZE, -1, false);
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

  /**
   * Test encoding/decoding data blocks.
   * @throws IOException a bug or a problem with temporary files.
   */
  @Test
  public void testDataBlockEncoding() throws IOException {
    final int numBlocks = 5;
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
        LOG.info("\n\nUsing includesMemstoreTS: " + includesMemstoreTS +
            ", compression: " + algo +
            ", encoding: " + encoding + "\n");
        Path path = new Path(TEST_UTIL.getTestDir(), "blocks_v2_"
            + algo + "_" + encoding.toString());
        FSDataOutputStream os = fs.create(path);
        HFileDataBlockEncoder dataBlockEncoder =
            new HFileDataBlockEncoderImpl(encoding);
        HFileBlock.Writer hbw = new HFileBlock.Writer(algo, dataBlockEncoder,
            includesMemstoreTS);
        long totalSize = 0;
        final List<Integer> encodedSizes = new ArrayList<Integer>();
        final List<ByteBuffer> encodedBlocks = new ArrayList<ByteBuffer>();
        for (int blockId = 0; blockId < numBlocks; ++blockId) {
          writeEncodedBlock(encoding, hbw, encodedSizes, encodedBlocks,
              blockId);

          hbw.writeHeaderAndData(os);
          totalSize += hbw.getOnDiskSizeWithHeader();
          LOG.info("Wrote block #" + blockId + ": " +
              "onDiskSizeWithHeader=" + hbw.getOnDiskSizeWithHeader() + ", " +
              "uncompressedSizeWithHeader=" + hbw.getUncompressedSizeWithHeader());
        }
        os.close();

        FSDataInputStream is = fs.open(path);
        HFileBlock.FSReaderV2 hbr = new HFileBlock.FSReaderV2(is, algo,
            totalSize, null, null);
        hbr.setDataBlockEncoder(dataBlockEncoder);
        hbr.setIncludesMemstoreTS(includesMemstoreTS);

        HFileBlock b;
        int pos = 0;
        LOG.info("\n\nStarting to read blocks\n");
        for (int blockId = 0; blockId < numBlocks; ++blockId) {
          b = hbr.readBlockData(pos, -1, -1, false);
          b.sanityCheck();
          pos += b.getOnDiskSizeWithHeader();

          LOG.info("Read block #" + blockId + ": " + b);
          assertEquals("Invalid encoded size:", (int) encodedSizes.get(blockId),
              b.getUncompressedSizeWithoutHeader());
          ByteBuffer actualBuffer = b.getBufferWithoutHeader();
          if (encoding != DataBlockEncoding.NONE) {
            // We expect a two-byte big-endian encoding id.
            assertEquals(encoding.getId(), actualBuffer.getShort());
            actualBuffer = actualBuffer.slice();
          }

          ByteBuffer expectedBuffer = encodedBlocks.get(blockId);
          expectedBuffer.rewind();

          // test if content matches, produce nice message
          assertBuffersEqual(expectedBuffer, actualBuffer, algo, encoding);
        }
        is.close();
      }

    }
  }

  private void writeEncodedBlock(DataBlockEncoding encoding,
      HFileBlock.Writer hbw, final List<Integer> encodedSizes,
      final List<ByteBuffer> encodedBlocks, int blockId) throws IOException {
    hbw.startWriting(BlockType.DATA);
    ByteArrayOutputStream rawKVBytes = new ByteArrayOutputStream();

    writeTestKeyValues(rawKVBytes, hbw, blockId);

    byte[] rawBuf = rawKVBytes.toByteArray();
    ByteArrayOutputStream encodedOut = new ByteArrayOutputStream();
    encoding.getEncoder().encodeKeyValues(
        new DataOutputStream(encodedOut),
        ByteBuffer.wrap(rawBuf), includesMemstoreTS);

    // We need to account for the two-byte encoding algorithm ID that
    // comes after the 24-byte block header but before encoded KVs.
    int encodedSize = encoding.encodingIdSize() + encodedOut.size();

    LOG.info("Raw size: " + rawBuf.length + ", encoded size: " + encodedSize);
    encodedSizes.add(encodedSize);
    encodedBlocks.add(ByteBuffer.wrap(encodedOut.toByteArray()));
  }

  private void assertBuffersEqual(ByteBuffer expectedBuffer,
      ByteBuffer actualBuffer, Compression.Algorithm compression,
      DataBlockEncoding encoding) {
    if (!actualBuffer.equals(expectedBuffer)) {
      int prefix = 0;
      int minLimit = Math.min(expectedBuffer.limit(), actualBuffer.limit());
      while (prefix < minLimit &&
          expectedBuffer.get(prefix) == actualBuffer.get(prefix)) {
        prefix++;
      }
      assertEquals(String.format(
          "Content mismatch for compression %s, encoding %s, " +
          "pread %s, commonPrefix %d, expected length %d, actual length %d",
          compression, encoding, prefix,
          expectedBuffer.limit(), actualBuffer.limit()),
          Bytes.toStringBinary(expectedBuffer),
          Bytes.toStringBinary(actualBuffer));
    }
  }

  @Test
  public void testPreviousOffset() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean cacheOnWrite : BOOLEAN_VALUES) {
        Random rand = defaultRandom();
        LOG.info("Compression algorithm: " + algo );
        Path path = new Path(TEST_UTIL.getTestDir(), "prev_offset");
        List<Long> expectedOffsets = new ArrayList<Long>();
        List<Long> expectedPrevOffsets = new ArrayList<Long>();
        List<BlockType> expectedTypes = new ArrayList<BlockType>();
        List<ByteBuffer> expectedContents = cacheOnWrite  ? new ArrayList<ByteBuffer>() : null;
        long totalSize = writeBlocks(rand, algo, path, expectedOffsets,
            expectedPrevOffsets, expectedTypes, expectedContents, true);

        FSDataInputStream is = fs.open(path);
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
            totalSize, null, null);
        long curOffset = 0;
        for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
          assertEquals(expectedOffsets.get(i).longValue(), curOffset);
          LOG.info("Reading block #" + i + " at offset " + curOffset);
          HFileBlock b = hbr.readBlockData(curOffset, -1, -1, false);
          LOG.info("Block #" + i + ": " + b);
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
              b.getOnDiskSizeWithHeader(), -1, false);
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

          curOffset += b.getOnDiskSizeWithHeader();

          if (cacheOnWrite) {
            // In the cache-on-write mode we store uncompressed bytes so we
            // can compare them to what was read by the block reader.

            ByteBuffer bufRead = b.getBufferWithHeader();
            ByteBuffer bufExpected = expectedContents.get(i);
            boolean bytesAreCorrect = Bytes.compareTo(bufRead.array(),
                bufRead.arrayOffset(), bufRead.limit(),
                bufExpected.array(), bufExpected.arrayOffset(),
                bufExpected.limit()) == 0;
            String wrongBytesMsg = "";

            if (!bytesAreCorrect) {
              // Optimization: only construct an error message in case we
              // will need it.
              wrongBytesMsg = "Expected bytes in block #" + i + " (algo="
                  + algo;
              wrongBytesMsg += Bytes.toStringBinary(bufExpected.array(),
                  bufExpected.arrayOffset(), Math.min(32,
                      bufExpected.limit()))
                      + ", actual:\n"
                      + Bytes.toStringBinary(bufRead.array(),
                          bufRead.arrayOffset(), Math.min(32, bufRead.limit()));
            }

            assertTrue(wrongBytesMsg, bytesAreCorrect);
          }
        }
        assertEquals(curOffset, fs.getFileStatus(path).getLen());
        is.close();
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
        boolean withOnDiskSize = rand.nextBoolean();
        long expectedSize =
          (blockId == NUM_TEST_BLOCKS - 1 ? fileSize
              : offsets.get(blockId + 1)) - offset;

        HFileBlock b;
        try {
          long onDiskSizeArg = withOnDiskSize ? expectedSize : -1;
          b = hbr.readBlockData(offset, onDiskSizeArg, -1, false);
        } catch (IOException ex) {
          LOG.error("Error in client " + clientId + " trying to read block at "
              + offset + ", pread=" + ", withOnDiskSize=" +
              withOnDiskSize, ex);
          return false;
        }

        assertEquals(types.get(blockId), b.getBlockType());
        assertEquals(expectedSize, b.getOnDiskSizeWithHeader());
        assertEquals(offset, b.getOffset());

        ++numBlocksRead;
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
          new Path(TEST_UTIL.getTestDir(), "concurrent_reading");
      Random rand = defaultRandom();
      List<Long> offsets = new ArrayList<Long>();
      List<BlockType> types = new ArrayList<BlockType>();
      writeBlocks(rand, compressAlgo, path, offsets, null, types, null, false);
      FSDataInputStream is = fs.open(path);
      long fileSize = fs.getFileStatus(path).getLen();
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, compressAlgo,
          fileSize, null, null);

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
        LOG.info(String.valueOf(i + 1)
            + " reader threads finished successfully (algo=" + compressAlgo
            + ")");
      }

      is.close();
    }
  }

  private long writeBlocks(Random rand, Compression.Algorithm compressAlgo,
      Path path, List<Long> expectedOffsets, List<Long> expectedPrevOffsets,
      List<BlockType> expectedTypes, List<ByteBuffer> expectedContents,
      boolean detailedLogging) throws IOException {
    boolean cacheOnWrite = expectedContents != null;
    FSDataOutputStream os = fs.create(path);
    HFileBlock.Writer hbw = new HFileBlock.Writer(compressAlgo, null,
        includesMemstoreTS);
    Map<BlockType, Long> prevOffsetByType = new HashMap<BlockType, Long>();
    long totalSize = 0;
    for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
      int blockTypeOrdinal = rand.nextInt(BlockType.values().length);
      if (blockTypeOrdinal == BlockType.ENCODED_DATA.ordinal()) {
        blockTypeOrdinal = BlockType.DATA.ordinal();
      }
      BlockType bt = BlockType.values()[blockTypeOrdinal];
      hbw.startWriting(bt);
      DataOutputStream dos = hbw.getUserDataStreamUnsafe(); 
      for (int j = 0; j < rand.nextInt(500); ++j) {
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
        LOG.info("Writing block #" + i + " of type " + bt
            + ", uncompressed size " + hbw.getUncompressedSizeWithoutHeader()
            + " at offset " + os.getPos());
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
    for (int size : new int[] { 100, 256, 12345 }) {
      byte[] byteArr = new byte[HFileBlock.HEADER_SIZE + size];
      ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
      HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
          HFileBlock.FILL_HEADER, -1, includesMemstoreTS);
      long byteBufferExpectedSize =
          ClassSize.align(ClassSize.estimateBase(buf.getClass(), true)
              + HFileBlock.HEADER_SIZE + size);
      long hfileBlockExpectedSize =
          ClassSize.align(ClassSize.estimateBase(HFileBlock.class, true));
      long expected = hfileBlockExpectedSize + byteBufferExpectedSize;
      assertEquals("Block data size: " + size + ", byte buffer expected " +
          "size: " + byteBufferExpectedSize + ", HFileBlock class expected " +
          "size: " + hfileBlockExpectedSize + ";", expected,
          block.heapSize());
    }
  }

}
