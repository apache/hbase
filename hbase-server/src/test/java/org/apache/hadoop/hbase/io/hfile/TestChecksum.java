/*
 * Copyright The Apache Software Foundation
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
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestChecksum {
  private static final Log LOG = LogFactory.getLog(TestHFileBlock.class);

  static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = {
      NONE, GZ };

  static final int[] BYTES_PER_CHECKSUM = {
      50, 500, 688, 16*1024, (16*1024+980), 64 * 1024};

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private FileSystem fs;
  private HFileSystem hfs;

  @Before
  public void setUp() throws Exception {
    fs = HFileSystem.get(TEST_UTIL.getConfiguration());
    hfs = (HFileSystem)fs;
    ChecksumUtil.generateExceptionForChecksumFailureForTest(false);
  }

  /**
   * Introduce checksum failures and check that we can still read
   * the data
   */
  @Test
  public void testChecksumCorruption() throws IOException {
    testChecksumCorruptionInternals(false);
    testChecksumCorruptionInternals(true);
  }

  protected void testChecksumCorruptionInternals(boolean useTags) throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        LOG.info("testChecksumCorruption: Compression algorithm: " + algo +
                   ", pread=" + pread);
        Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
            + algo);
        FSDataOutputStream os = fs.create(path);
        HFileContext meta = new HFileContextBuilder()
                            .withCompression(algo)
                            .withIncludesMvcc(true)
                            .withIncludesTags(useTags)
                            .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
                            .build();
        HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
        long totalSize = 0;
        for (int blockId = 0; blockId < 2; ++blockId) {
          DataOutputStream dos = hbw.startWriting(BlockType.DATA);
          for (int i = 0; i < 1234; ++i)
            dos.writeInt(i);
          hbw.writeHeaderAndData(os);
          totalSize += hbw.getOnDiskSizeWithHeader();
        }
        os.close();

        // Use hbase checksums. 
        assertEquals(true, hfs.useHBaseChecksum());

        // Do a read that purposely introduces checksum verification failures.
        FSDataInputStreamWrapper is = new FSDataInputStreamWrapper(fs, path);
        meta = new HFileContextBuilder()
              .withCompression(algo)
              .withIncludesMvcc(true)
              .withIncludesTags(useTags)
              .withHBaseCheckSum(true)
              .build();
        HFileBlock.FSReader hbr = new CorruptedFSReaderImpl(is, totalSize, fs, path, meta);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936, 
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        // read data back from the hfile, exclude header and checksum
        ByteBuffer bb = b.unpack(meta, hbr).getBufferWithoutHeader(); // read back data
        DataInputStream in = new DataInputStream(
                               new ByteArrayInputStream(
                                 bb.array(), bb.arrayOffset(), bb.limit()));

        // assert that we encountered hbase checksum verification failures
        // but still used hdfs checksums and read data successfully.
        assertEquals(1, HFile.getChecksumFailuresCount());
        validateData(in);

        // A single instance of hbase checksum failure causes the reader to
        // switch off hbase checksum verification for the next 100 read
        // requests. Verify that this is correct.
        for (int i = 0; i < 
             HFileBlock.CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD + 1; i++) {
          b = hbr.readBlockData(0, -1, -1, pread);
          assertEquals(0, HFile.getChecksumFailuresCount());
        }
        // The next read should have hbase checksum verification reanabled,
        // we verify this by assertng that there was a hbase-checksum failure.
        b = hbr.readBlockData(0, -1, -1, pread);
        assertEquals(1, HFile.getChecksumFailuresCount());

        // Since the above encountered a checksum failure, we switch
        // back to not checking hbase checksums.
        b = hbr.readBlockData(0, -1, -1, pread);
        assertEquals(0, HFile.getChecksumFailuresCount());
        is.close();

        // Now, use a completely new reader. Switch off hbase checksums in 
        // the configuration. In this case, we should not detect
        // any retries within hbase. 
        HFileSystem newfs = new HFileSystem(TEST_UTIL.getConfiguration(), false);
        assertEquals(false, newfs.useHBaseChecksum());
        is = new FSDataInputStreamWrapper(newfs, path);
        hbr = new CorruptedFSReaderImpl(is, totalSize, newfs, path, meta);
        b = hbr.readBlockData(0, -1, -1, pread);
        is.close();
        b.sanityCheck();
        b = b.unpack(meta, hbr);
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936, 
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        // read data back from the hfile, exclude header and checksum
        bb = b.getBufferWithoutHeader(); // read back data
        in = new DataInputStream(new ByteArrayInputStream(
                                 bb.array(), bb.arrayOffset(), bb.limit()));

        // assert that we did not encounter hbase checksum verification failures
        // but still used hdfs checksums and read data successfully.
        assertEquals(0, HFile.getChecksumFailuresCount());
        validateData(in);
      }
    }
  }

  /**
   * Test different values of bytesPerChecksum
   */
  @Test
  public void testChecksumChunks() throws IOException {
    testChecksumInternals(false);
    testChecksumInternals(true);
  }

  protected void testChecksumInternals(boolean useTags) throws IOException {
    Compression.Algorithm algo = NONE;
    for (boolean pread : new boolean[] { false, true }) {
      for (int bytesPerChecksum : BYTES_PER_CHECKSUM) {
        Path path = new Path(TEST_UTIL.getDataTestDir(), "checksumChunk_" + 
                             algo + bytesPerChecksum);
        FSDataOutputStream os = fs.create(path);
        HFileContext meta = new HFileContextBuilder()
                            .withCompression(algo)
                            .withIncludesMvcc(true)
                            .withIncludesTags(useTags)
                            .withHBaseCheckSum(true)
                            .withBytesPerCheckSum(bytesPerChecksum)
                            .build();
        HFileBlock.Writer hbw = new HFileBlock.Writer(null,
           meta);

        // write one block. The block has data
        // that is at least 6 times more than the checksum chunk size
        long dataSize = 0;
        DataOutputStream dos = hbw.startWriting(BlockType.DATA);
        for (; dataSize < 6 * bytesPerChecksum;) {
          for (int i = 0; i < 1234; ++i) {
            dos.writeInt(i);
            dataSize += 4;
          }
        }
        hbw.writeHeaderAndData(os);
        long totalSize = hbw.getOnDiskSizeWithHeader();
        os.close();

        long expectedChunks = ChecksumUtil.numChunks(
                               dataSize + HConstants.HFILEBLOCK_HEADER_SIZE,
                               bytesPerChecksum);
        LOG.info("testChecksumChunks: pread=" + pread +
                   ", bytesPerChecksum=" + bytesPerChecksum +
                   ", fileSize=" + totalSize +
                   ", dataSize=" + dataSize +
                   ", expectedChunks=" + expectedChunks);

        // Verify hbase checksums. 
        assertEquals(true, hfs.useHBaseChecksum());

        // Read data back from file.
        FSDataInputStream is = fs.open(path);
        FSDataInputStream nochecksum = hfs.getNoChecksumFs().open(path);
        meta = new HFileContextBuilder()
               .withCompression(algo)
               .withIncludesMvcc(true)
               .withIncludesTags(useTags)
               .withHBaseCheckSum(true)
               .withBytesPerCheckSum(bytesPerChecksum)
               .build();
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(new FSDataInputStreamWrapper(
            is, nochecksum), totalSize, hfs, path, meta);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        is.close();
        b.sanityCheck();
        assertEquals(dataSize, b.getUncompressedSizeWithoutHeader());

        // verify that we have the expected number of checksum chunks
        assertEquals(totalSize, HConstants.HFILEBLOCK_HEADER_SIZE + dataSize +
                     expectedChunks * HFileBlock.CHECKSUM_SIZE);

        // assert that we did not encounter hbase checksum verification failures
        assertEquals(0, HFile.getChecksumFailuresCount());
      }
    }
  }

  /** 
   * Test to ensure that these is at least one valid checksum implementation
   */
  @Test
  public void testChecksumAlgorithm() throws IOException {
    ChecksumType type = ChecksumType.CRC32;
    assertEquals(ChecksumType.nameToType(type.getName()), type);
    assertEquals(ChecksumType.valueOf(type.toString()), type);
  }

  private void validateData(DataInputStream in) throws IOException {
    // validate data
    for (int i = 0; i < 1234; i++) {
      int val = in.readInt();
      assertEquals("testChecksumCorruption: data mismatch at index " + i, i, val);
    }
  }

  /**
   * This class is to test checksum behavior when data is corrupted. It mimics the following
   * behavior:
   *  - When fs checksum is disabled, hbase may get corrupted data from hdfs. If verifyChecksum
   *  is true, it means hbase checksum is on and fs checksum is off, so we corrupt the data.
   *  - When fs checksum is enabled, hdfs will get a different copy from another node, and will
   *    always return correct data. So we don't corrupt the data when verifyChecksum for hbase is
   *    off.
   */
  static private class CorruptedFSReaderImpl extends HFileBlock.FSReaderImpl {
    /**
     * If set to true, corrupt reads using readAtOffset(...).
     */
    boolean corruptDataStream = false;

    public CorruptedFSReaderImpl(FSDataInputStreamWrapper istream, long fileSize, FileSystem fs,
        Path path, HFileContext meta) throws IOException {
      super(istream, fileSize, (HFileSystem) fs, path, meta);
    }

    @Override
    protected HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
        int onDiskSizeWithHeaderL, int uncompressedSize, boolean pread, boolean verifyChecksum)
        throws IOException {
      if (verifyChecksum) {
        corruptDataStream = true;
      }
      HFileBlock b = super.readBlockDataInternal(is, offset, onDiskSizeWithHeaderL,
          uncompressedSize, pread, verifyChecksum);
      corruptDataStream = false;
      return b;
    }

    @Override
    protected int readAtOffset(FSDataInputStream istream, byte [] dest, int destOffset, int size,
        boolean peekIntoNextBlock, long fileOffset, boolean pread) throws IOException {
      int returnValue = super.readAtOffset(istream, dest, destOffset, size, peekIntoNextBlock,
          fileOffset, pread);
      if (!corruptDataStream) {
        return returnValue;
      }
      // Corrupt 3rd character of block magic of next block's header.
      if (peekIntoNextBlock) {
        dest[destOffset + size + 3] = 0b00000000;
      }
      // We might be reading this block's header too, corrupt it.
      dest[destOffset + 1] = 0b00000000;
      // Corrupt non header data
      if (size > hdrSize) {
        dest[destOffset + hdrSize + 1] = 0b00000000;
      }
      return returnValue;
    }
  }
}

