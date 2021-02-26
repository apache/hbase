/**
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IOTests.class, SmallTests.class})
public class TestChecksum {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestChecksum.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileBlock.class);

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
  }

  @Test
  public void testNewBlocksHaveDefaultChecksum() throws IOException {
    Path path = new Path(TEST_UTIL.getDataTestDir(), "default_checksum");
    FSDataOutputStream os = fs.create(path);
    HFileContext meta = new HFileContextBuilder().build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
    DataOutputStream dos = hbw.startWriting(BlockType.DATA);
    for (int i = 0; i < 1000; ++i)
      dos.writeInt(i);
    hbw.writeHeaderAndData(os);
    int totalSize = hbw.getOnDiskSizeWithHeader();
    os.close();

    // Use hbase checksums.
    assertEquals(true, hfs.useHBaseChecksum());

    FSDataInputStreamWrapper is = new FSDataInputStreamWrapper(fs, path);
    meta = new HFileContextBuilder().withHBaseCheckSum(true).build();
    ReaderContext context = new ReaderContextBuilder()
        .withInputStreamWrapper(is)
        .withFileSize(totalSize)
        .withFileSystem((HFileSystem) fs)
        .withFilePath(path)
        .build();
    HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(context,
        meta, ByteBuffAllocator.HEAP);
    HFileBlock b = hbr.readBlockData(0, -1, false, false, true);
    assertTrue(!b.isSharedMem());
    assertEquals(b.getChecksumType(), ChecksumType.getDefaultChecksumType().getCode());
  }

  private void verifyMBBCheckSum(ByteBuff buf) throws IOException {
    int size = buf.remaining() / 2 + 1;
    ByteBuff mbb = new MultiByteBuff(ByteBuffer.allocate(size), ByteBuffer.allocate(size))
          .position(0).limit(buf.remaining());
    for (int i = buf.position(); i < buf.limit(); i++) {
      mbb.put(buf.get(i));
    }
    mbb.position(0).limit(buf.remaining());
    assertEquals(mbb.remaining(), buf.remaining());
    assertTrue(mbb.remaining() > size);
    ChecksumUtil.validateChecksum(mbb, "test", 0, HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM);
  }

  private void verifySBBCheckSum(ByteBuff buf) throws IOException {
    ChecksumUtil.validateChecksum(buf, "test", 0, HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM);
  }

  @Test
  public void testVerifyCheckSum() throws IOException {
    int intCount = 10000;
    for (ChecksumType ckt : ChecksumType.values()) {
      Path path = new Path(TEST_UTIL.getDataTestDir(), "checksum" + ckt.getName());
      FSDataOutputStream os = fs.create(path);
      HFileContext meta = new HFileContextBuilder()
            .withChecksumType(ckt)
            .build();
      HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
      DataOutputStream dos = hbw.startWriting(BlockType.DATA);
      for (int i = 0; i < intCount; ++i) {
        dos.writeInt(i);
      }
      hbw.writeHeaderAndData(os);
      int totalSize = hbw.getOnDiskSizeWithHeader();
      os.close();

      // Use hbase checksums.
      assertEquals(true, hfs.useHBaseChecksum());

      FSDataInputStreamWrapper is = new FSDataInputStreamWrapper(fs, path);
      meta = new HFileContextBuilder().withHBaseCheckSum(true).build();
      ReaderContext context = new ReaderContextBuilder()
          .withInputStreamWrapper(is)
          .withFileSize(totalSize)
          .withFileSystem((HFileSystem) fs)
          .withFilePath(path)
          .build();
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderImpl(context,
          meta, ByteBuffAllocator.HEAP);
      HFileBlock b = hbr.readBlockData(0, -1, false, false, true);
      assertTrue(!b.isSharedMem());

      // verify SingleByteBuff checksum.
      verifySBBCheckSum(b.getBufferReadOnly());

      // verify MultiByteBuff checksum.
      verifyMBBCheckSum(b.getBufferReadOnly());

      ByteBuff data = b.getBufferWithoutHeader();
      for (int i = 0; i < intCount; i++) {
        assertEquals(i, data.getInt());
      }
      try {
        data.getInt();
        fail();
      } catch (BufferUnderflowException e) {
        // expected failure
      }
      assertEquals(0, HFile.getAndResetChecksumFailuresCount());
    }
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
        ReaderContext context = new ReaderContextBuilder()
           .withInputStreamWrapper(is)
           .withFileSize(totalSize)
           .withFileSystem(fs)
           .withFilePath(path)
           .build();
        HFileBlock.FSReader hbr = new CorruptedFSReaderImpl(context, meta);
        HFileBlock b = hbr.readBlockData(0, -1, pread, false, true);
        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936,
                     b.getOnDiskSizeWithoutHeader() - b.totalChecksumBytes());
        // read data back from the hfile, exclude header and checksum
        ByteBuff bb = b.unpack(meta, hbr).getBufferWithoutHeader(); // read back data
        DataInputStream in = new DataInputStream(
                               new ByteArrayInputStream(
                                 bb.array(), bb.arrayOffset(), bb.limit()));

        // assert that we encountered hbase checksum verification failures
        // but still used hdfs checksums and read data successfully.
        assertEquals(1, HFile.getAndResetChecksumFailuresCount());
        validateData(in);

        // A single instance of hbase checksum failure causes the reader to
        // switch off hbase checksum verification for the next 100 read
        // requests. Verify that this is correct.
        for (int i = 0; i <
             HFileBlock.CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD + 1; i++) {
          b = hbr.readBlockData(0, -1, pread, false, true);
          assertTrue(b.getBufferReadOnly() instanceof SingleByteBuff);
          assertEquals(0, HFile.getAndResetChecksumFailuresCount());
        }
        // The next read should have hbase checksum verification reanabled,
        // we verify this by assertng that there was a hbase-checksum failure.
        b = hbr.readBlockData(0, -1, pread, false, true);
        assertTrue(b.getBufferReadOnly() instanceof SingleByteBuff);
        assertEquals(1, HFile.getAndResetChecksumFailuresCount());

        // Since the above encountered a checksum failure, we switch
        // back to not checking hbase checksums.
        b = hbr.readBlockData(0, -1, pread, false, true);
        assertTrue(b.getBufferReadOnly() instanceof SingleByteBuff);
        assertEquals(0, HFile.getAndResetChecksumFailuresCount());
        is.close();

        // Now, use a completely new reader. Switch off hbase checksums in
        // the configuration. In this case, we should not detect
        // any retries within hbase.
        HFileSystem newfs = new HFileSystem(TEST_UTIL.getConfiguration(), false);
        assertEquals(false, newfs.useHBaseChecksum());
        is = new FSDataInputStreamWrapper(newfs, path);
        context = new ReaderContextBuilder()
            .withInputStreamWrapper(is)
            .withFileSize(totalSize)
            .withFileSystem(newfs)
            .withFilePath(path)
            .build();
        hbr = new CorruptedFSReaderImpl(context, meta);
        b = hbr.readBlockData(0, -1, pread, false, true);
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
        assertEquals(0, HFile.getAndResetChecksumFailuresCount());
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
        LOG.info("testChecksumChunks: pread={}, bytesPerChecksum={}, fileSize={}, "
                + "dataSize={}, expectedChunks={}, compression={}", pread, bytesPerChecksum,
            totalSize, dataSize, expectedChunks, algo.toString());

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
        ReaderContext context = new ReaderContextBuilder()
            .withInputStreamWrapper(new FSDataInputStreamWrapper(is, nochecksum))
            .withFileSize(totalSize)
            .withFileSystem(hfs)
            .withFilePath(path)
            .build();
        HFileBlock.FSReader hbr =
            new HFileBlock.FSReaderImpl(context, meta, ByteBuffAllocator.HEAP);
        HFileBlock b = hbr.readBlockData(0, -1, pread, false, true);
        assertTrue(b.getBufferReadOnly() instanceof SingleByteBuff);
        is.close();
        b.sanityCheck();
        assertEquals(dataSize, b.getUncompressedSizeWithoutHeader());

        // verify that we have the expected number of checksum chunks
        assertEquals(totalSize, HConstants.HFILEBLOCK_HEADER_SIZE + dataSize +
                     expectedChunks * HFileBlock.CHECKSUM_SIZE);

        // assert that we did not encounter hbase checksum verification failures
        assertEquals(0, HFile.getAndResetChecksumFailuresCount());
      }
    }
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

    public CorruptedFSReaderImpl(ReaderContext context, HFileContext meta) throws IOException {
      super(context, meta, ByteBuffAllocator.HEAP);
    }

    @Override
    protected HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
        long onDiskSizeWithHeaderL, boolean pread, boolean verifyChecksum, boolean updateMetrics,
        boolean useHeap) throws IOException {
      if (verifyChecksum) {
        corruptDataStream = true;
      }
      HFileBlock b = super.readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
        verifyChecksum, updateMetrics, useHeap);
      corruptDataStream = false;
      return b;
    }


    @Override
    protected boolean readAtOffset(FSDataInputStream istream, ByteBuff dest, int size,
        boolean peekIntoNextBlock, long fileOffset, boolean pread) throws IOException {
      int destOffset = dest.position();
      boolean returnValue =
          super.readAtOffset(istream, dest, size, peekIntoNextBlock, fileOffset, pread);
      if (!corruptDataStream) {
        return returnValue;
      }
      // Corrupt 3rd character of block magic of next block's header.
      if (peekIntoNextBlock) {
        dest.put(destOffset + size + 3, (byte) 0b00000000);
      }
      // We might be reading this block's header too, corrupt it.
      dest.put(destOffset + 1, (byte) 0b00000000);
      // Corrupt non header data
      if (size > hdrSize) {
        dest.put(destOffset + hdrSize + 1, (byte) 0b00000000);
      }
      return returnValue;
    }
  }
}
