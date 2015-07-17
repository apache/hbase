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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@Category({IOTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestHFileBlock {
  // change this value to activate more logs
  private static final boolean detailedLogging = false;
  private static final boolean[] BOOLEAN_VALUES = new boolean[] { false, true };

  private static final Log LOG = LogFactory.getLog(TestHFileBlock.class);

  static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = { NONE, GZ };

  private static final int NUM_TEST_BLOCKS = 1000;
  private static final int NUM_READER_THREADS = 26;

  // Used to generate KeyValues
  private static int NUM_KEYVALUES = 50;
  private static int FIELD_LENGTH = 10;
  private static float CHANCE_TO_REPEAT = 0.6f;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private FileSystem fs;

  private final boolean includesMemstoreTS;
  private final boolean includesTag;
  public TestHFileBlock(boolean includesMemstoreTS, boolean includesTag) {
    this.includesMemstoreTS = includesMemstoreTS;
    this.includesTag = includesTag;
  }

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.MEMSTORETS_TAGS_PARAMETRIZED;
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

  static int writeTestKeyValues(HFileBlock.Writer hbw, int seed, boolean includesMemstoreTS,
      boolean useTag) throws IOException {
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
        row = CellUtil.cloneRow(keyValues.get(randomizer.nextInt(keyValues.size())));
      } else {
        row = new byte[FIELD_LENGTH];
        randomizer.nextBytes(row);
      }
      if (0 == i) {
        family = new byte[FIELD_LENGTH];
        randomizer.nextBytes(family);
      } else {
        family = CellUtil.cloneFamily(keyValues.get(0));
      }
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        qualifier = CellUtil.cloneQualifier(keyValues.get(randomizer.nextInt(keyValues.size())));
      } else {
        qualifier = new byte[FIELD_LENGTH];
        randomizer.nextBytes(qualifier);
      }
      if (0 < i && randomizer.nextFloat() < CHANCE_TO_REPEAT) {
        value = CellUtil.cloneValue(keyValues.get(randomizer.nextInt(keyValues.size())));
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
      if (!useTag) {
        keyValues.add(new KeyValue(row, family, qualifier, timestamp, value));
      } else {
        keyValues.add(new KeyValue(row, family, qualifier, timestamp, value, new Tag[] { new Tag(
            (byte) 1, Bytes.toBytes("myTagVal")) }));
      }
    }

    // sort it and write to stream
    int totalSize = 0;
    Collections.sort(keyValues, CellComparator.COMPARATOR);

    for (KeyValue kv : keyValues) {
      totalSize += kv.getLength();
      if (includesMemstoreTS) {
        long memstoreTS = randomizer.nextLong();
        kv.setSequenceId(memstoreTS);
        totalSize += WritableUtils.getVIntSize(memstoreTS);
      }
      hbw.write(kv);
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
    dos.flush();
    algo.returnCompressor(compressor);
    return baos.toByteArray();
  }

  static HFileBlock.Writer createTestV2Block(Compression.Algorithm algo,
      boolean includesMemstoreTS, boolean includesTag) throws IOException {
    final BlockType blockType = BlockType.DATA;
    HFileContext meta = new HFileContextBuilder()
                        .withCompression(algo)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(includesTag)
                        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                        .build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
    DataOutputStream dos = hbw.startWriting(blockType);
    writeTestBlockContents(dos);
    dos.flush();
    hbw.ensureBlockReady();
    assertEquals(1000 * 4, hbw.getUncompressedSizeWithoutHeader());
    hbw.release();
    return hbw;
  }

  public String createTestBlockStr(Compression.Algorithm algo,
      int correctLength, boolean useTag) throws IOException {
    HFileBlock.Writer hbw = createTestV2Block(algo, includesMemstoreTS, useTag);
    byte[] testV2Block = hbw.getHeaderAndDataForTest();
    int osOffset = HConstants.HFILEBLOCK_HEADER_SIZE + 9;
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
    CacheConfig cacheConf = Mockito.mock(CacheConfig.class);
    Mockito.when(cacheConf.isBlockCacheEnabled()).thenReturn(false);

    HFileBlock block =
      createTestV2Block(NONE, includesMemstoreTS, false).getBlockForCaching(cacheConf);
    assertEquals(4000, block.getUncompressedSizeWithoutHeader());
    assertEquals(4004, block.getOnDiskSizeWithoutHeader());
    assertTrue(block.isUnpacked());
  }

  @Test
  public void testGzipCompression() throws IOException {
    final String correctTestBlockStr =
        "DATABLK*\\x00\\x00\\x00>\\x00\\x00\\x0F\\xA0\\xFF\\xFF\\xFF\\xFF"
            + "\\xFF\\xFF\\xFF\\xFF"
            + "\\x0" + ChecksumType.getDefaultChecksumType().getCode()
            + "\\x00\\x00@\\x00\\x00\\x00\\x00["
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
    final String testBlockStr = createTestBlockStr(GZ, correctGzipBlockLength, false);
    // We ignore the block checksum because createTestBlockStr can change the
    // gzip header after the block is produced
    assertEquals(correctTestBlockStr.substring(0, correctGzipBlockLength - 4),
      testBlockStr.substring(0, correctGzipBlockLength - 4));
  }

  @Test
  public void testReaderV2() throws IOException {
    testReaderV2Internals();
  }

  protected void testReaderV2Internals() throws IOException {
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
        HFileContext meta = new HFileContextBuilder()
                           .withCompression(algo)
                           .withIncludesMvcc(includesMemstoreTS)
                           .withIncludesTags(includesTag)
                           .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                           .build();
        HFileBlock.Writer hbw = new HFileBlock.Writer(null,
           meta);
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
        meta = new HFileContextBuilder()
        .withHBaseCheckSum(true)
        .withIncludesMvcc(includesMemstoreTS)
        .withIncludesTags(includesTag)
        .withCompression(algo).build();
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(is, totalSize, meta);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        is.close();
        assertEquals(0, HFile.getChecksumFailuresCount());

        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936,
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        HFileBlock expected = b;

        if (algo == GZ) {
          is = fs.open(path);
          hbr = new HFileBlock.FSReaderImpl(is, totalSize, meta);
          b = hbr.readBlockData(0, 2173 + HConstants.HFILEBLOCK_HEADER_SIZE +
                                b.totalChecksumBytes(), -1, pread);
          assertEquals(expected, b);
          int wrongCompressedSize = 2172;
          try {
            b = hbr.readBlockData(0, wrongCompressedSize
                + HConstants.HFILEBLOCK_HEADER_SIZE, -1, pread);
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
    testInternals();
  }

  private void testInternals() throws IOException {
    final int numBlocks = 5;
    if(includesTag) {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    }
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
          Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
              + algo + "_" + encoding.toString());
          FSDataOutputStream os = fs.create(path);
          HFileDataBlockEncoder dataBlockEncoder = (encoding != DataBlockEncoding.NONE) ?
              new HFileDataBlockEncoderImpl(encoding) : NoOpDataBlockEncoder.INSTANCE;
          HFileContext meta = new HFileContextBuilder()
                              .withCompression(algo)
                              .withIncludesMvcc(includesMemstoreTS)
                              .withIncludesTags(includesTag)
                              .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                              .build();
          HFileBlock.Writer hbw = new HFileBlock.Writer(dataBlockEncoder, meta);
          long totalSize = 0;
          final List<Integer> encodedSizes = new ArrayList<Integer>();
          final List<ByteBuffer> encodedBlocks = new ArrayList<ByteBuffer>();
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            hbw.startWriting(BlockType.DATA);
            writeTestKeyValues(hbw, blockId, includesMemstoreTS, includesTag);
            hbw.writeHeaderAndData(os);
            int headerLen = HConstants.HFILEBLOCK_HEADER_SIZE;
            byte[] encodedResultWithHeader = hbw.getUncompressedBufferWithHeader().array();
            final int encodedSize = encodedResultWithHeader.length - headerLen;
            if (encoding != DataBlockEncoding.NONE) {
              // We need to account for the two-byte encoding algorithm ID that
              // comes after the 24-byte block header but before encoded KVs.
              headerLen += DataBlockEncoding.ID_SIZE;
            }
            byte[] encodedDataSection =
                new byte[encodedResultWithHeader.length - headerLen];
            System.arraycopy(encodedResultWithHeader, headerLen,
                encodedDataSection, 0, encodedDataSection.length);
            final ByteBuffer encodedBuf =
                ByteBuffer.wrap(encodedDataSection);
            encodedSizes.add(encodedSize);
            encodedBlocks.add(encodedBuf);
            totalSize += hbw.getOnDiskSizeWithHeader();
          }
          os.close();

          FSDataInputStream is = fs.open(path);
          meta = new HFileContextBuilder()
                .withHBaseCheckSum(true)
                .withCompression(algo)
                .withIncludesMvcc(includesMemstoreTS)
                .withIncludesTags(includesTag)
                .build();
          HFileBlock.FSReaderImpl hbr = new HFileBlock.FSReaderImpl(is, totalSize, meta);
          hbr.setDataBlockEncoder(dataBlockEncoder);
          hbr.setIncludesMemstoreTS(includesMemstoreTS);
          HFileBlock blockFromHFile, blockUnpacked;
          int pos = 0;
          for (int blockId = 0; blockId < numBlocks; ++blockId) {
            blockFromHFile = hbr.readBlockData(pos, -1, -1, pread);
            assertEquals(0, HFile.getChecksumFailuresCount());
            blockFromHFile.sanityCheck();
            pos += blockFromHFile.getOnDiskSizeWithHeader();
            assertEquals((int) encodedSizes.get(blockId),
              blockFromHFile.getUncompressedSizeWithoutHeader());
            assertEquals(meta.isCompressedOrEncrypted(), !blockFromHFile.isUnpacked());
            long packedHeapsize = blockFromHFile.heapSize();
            blockUnpacked = blockFromHFile.unpack(meta, hbr);
            assertTrue(blockUnpacked.isUnpacked());
            if (meta.isCompressedOrEncrypted()) {
              LOG.info("packedHeapsize=" + packedHeapsize + ", unpackedHeadsize=" + blockUnpacked
                .heapSize());
              assertFalse(packedHeapsize == blockUnpacked.heapSize());
              assertTrue("Packed heapSize should be < unpacked heapSize",
                packedHeapsize < blockUnpacked.heapSize());
            }
            ByteBuff actualBuffer = blockUnpacked.getBufferWithoutHeader();
            if (encoding != DataBlockEncoding.NONE) {
              // We expect a two-byte big-endian encoding id.
              assertEquals(
                "Unexpected first byte with " + buildMessageDetails(algo, encoding, pread),
                Long.toHexString(0), Long.toHexString(actualBuffer.get(0)));
              assertEquals(
                "Unexpected second byte with " + buildMessageDetails(algo, encoding, pread),
                Long.toHexString(encoding.getId()), Long.toHexString(actualBuffer.get(1)));
              actualBuffer.position(2);
              actualBuffer = actualBuffer.slice();
            }

            ByteBuffer expectedBuffer = encodedBlocks.get(blockId);
            expectedBuffer.rewind();

            // test if content matches, produce nice message
            assertBuffersEqual(new SingleByteBuff(expectedBuffer), actualBuffer, algo, encoding,
                pread);

            // test serialized blocks
            for (boolean reuseBuffer : new boolean[] { false, true }) {
              ByteBuffer serialized = ByteBuffer.allocate(blockFromHFile.getSerializedLength());
              blockFromHFile.serialize(serialized);
              HFileBlock deserialized = (HFileBlock) blockFromHFile.getDeserializer().deserialize(
                  new SingleByteBuff(serialized), reuseBuffer);
              assertEquals(
                "Serialization did not preserve block state. reuseBuffer=" + reuseBuffer,
                blockFromHFile, deserialized);
              // intentional reference comparison
              if (blockFromHFile != blockUnpacked) {
                assertEquals("Deserializaed block cannot be unpacked correctly.",
                  blockUnpacked, deserialized.unpack(meta, hbr));
              }
            }
          }
          is.close();
        }
      }
    }
  }

  static String buildMessageDetails(Algorithm compression, DataBlockEncoding encoding,
      boolean pread) {
    return String.format("compression %s, encoding %s, pread %s", compression, encoding, pread);
  }

  static void assertBuffersEqual(ByteBuff expectedBuffer,
      ByteBuff actualBuffer, Compression.Algorithm compression,
      DataBlockEncoding encoding, boolean pread) {
    if (!actualBuffer.equals(expectedBuffer)) {
      int prefix = 0;
      int minLimit = Math.min(expectedBuffer.limit(), actualBuffer.limit());
      while (prefix < minLimit &&
          expectedBuffer.get(prefix) == actualBuffer.get(prefix)) {
        prefix++;
      }

      fail(String.format(
          "Content mismatch for %s, commonPrefix %d, expected %s, got %s",
          buildMessageDetails(compression, encoding, pread), prefix,
          nextBytesToStr(expectedBuffer, prefix),
          nextBytesToStr(actualBuffer, prefix)));
    }
  }

  /**
   * Convert a few next bytes in the given buffer at the given position to
   * string. Used for error messages.
   */
  private static String nextBytesToStr(ByteBuff buf, int pos) {
    int maxBytes = buf.limit() - pos;
    int numBytes = Math.min(16, maxBytes);
    return Bytes.toStringBinary(buf.array(), buf.arrayOffset() + pos,
        numBytes) + (numBytes < maxBytes ? "..." : "");
  }

  @Test
  public void testPreviousOffset() throws IOException {
    testPreviousOffsetInternals();
  }

  protected void testPreviousOffsetInternals() throws IOException {
    // TODO: parameterize these nested loops.
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
          HFileContext meta = new HFileContextBuilder()
                              .withHBaseCheckSum(true)
                              .withIncludesMvcc(includesMemstoreTS)
                              .withIncludesTags(includesTag)
                              .withCompression(algo).build();
          HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(is, totalSize, meta);
          long curOffset = 0;
          for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
            if (!pread) {
              assertEquals(is.getPos(), curOffset + (i == 0 ? 0 :
                  HConstants.HFILEBLOCK_HEADER_SIZE));
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
              // NOTE: cache-on-write testing doesn't actually involve a BlockCache. It simply
              // verifies that the unpacked value read back off disk matches the unpacked value
              // generated before writing to disk.
              b = b.unpack(meta, hbr);
              // b's buffer has header + data + checksum while
              // expectedContents have header + data only
              ByteBuff bufRead = b.getBufferWithHeader();
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
                  bufExpected.arrayOffset(), Math.min(32 + 10, bufExpected.limit()))
                    + ", actual:\n"
                    + Bytes.toStringBinary(bufRead.array(),
                  bufRead.arrayOffset(), Math.min(32 + 10, bufRead.limit()));
                if (detailedLogging) {
                  LOG.warn("expected header" +
                           HFileBlock.toStringHeader(new SingleByteBuff(bufExpected)) +
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
    testConcurrentReadingInternals();
  }

  protected void testConcurrentReadingInternals() throws IOException,
      InterruptedException, ExecutionException {
    for (Compression.Algorithm compressAlgo : COMPRESSION_ALGORITHMS) {
      Path path =
          new Path(TEST_UTIL.getDataTestDir(), "concurrent_reading");
      Random rand = defaultRandom();
      List<Long> offsets = new ArrayList<Long>();
      List<BlockType> types = new ArrayList<BlockType>();
      writeBlocks(rand, compressAlgo, path, offsets, null, types, null);
      FSDataInputStream is = fs.open(path);
      long fileSize = fs.getFileStatus(path).getLen();
      HFileContext meta = new HFileContextBuilder()
                          .withHBaseCheckSum(true)
                          .withIncludesMvcc(includesMemstoreTS)
                          .withIncludesTags(includesTag)
                          .withCompression(compressAlgo)
                          .build();
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(is, fileSize, meta);

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
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(true)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(includesTag)
                        .withCompression(compressAlgo)
                        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                        .build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
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
            + ", packed size " + hbw.getOnDiskSizeWithoutHeader()
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
    testBlockHeapSizeInternals();
  }

  protected void testBlockHeapSizeInternals() {
    if (ClassSize.is32BitJVM()) {
      assertTrue(HFileBlock.MULTI_BYTE_BUFFER_HEAP_SIZE == 64);
    } else {
      assertTrue(HFileBlock.MULTI_BYTE_BUFFER_HEAP_SIZE == 104);
    }

    for (int size : new int[] { 100, 256, 12345 }) {
      byte[] byteArr = new byte[HConstants.HFILEBLOCK_HEADER_SIZE + size];
      ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
      HFileContext meta = new HFileContextBuilder()
                          .withIncludesMvcc(includesMemstoreTS)
                          .withIncludesTags(includesTag)
                          .withHBaseCheckSum(false)
                          .withCompression(Algorithm.NONE)
                          .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                          .withChecksumType(ChecksumType.NULL).build();
      HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
          HFileBlock.FILL_HEADER, -1,
          0, meta);
      long byteBufferExpectedSize = ClassSize.align(ClassSize.estimateBase(
          new MultiByteBuff(buf).getClass(), true)
          + HConstants.HFILEBLOCK_HEADER_SIZE + size);
      long hfileMetaSize =  ClassSize.align(ClassSize.estimateBase(HFileContext.class, true));
      long hfileBlockExpectedSize =
          ClassSize.align(ClassSize.estimateBase(HFileBlock.class, true));
      long expected = hfileBlockExpectedSize + byteBufferExpectedSize + hfileMetaSize;
      assertEquals("Block data size: " + size + ", byte buffer expected " +
          "size: " + byteBufferExpectedSize + ", HFileBlock class expected " +
          "size: " + hfileBlockExpectedSize + ";", expected,
          block.heapSize());
    }
  }
}
