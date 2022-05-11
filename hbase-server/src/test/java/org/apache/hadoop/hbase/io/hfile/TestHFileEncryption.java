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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RedundantKVGenerator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileEncryption {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileEncryption.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileEncryption.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static FileSystem fs;
  private static Encryption.Context cryptoContext;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Disable block cache in this test.
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setInt("hfile.format.version", 3);

    fs = FileSystem.get(conf);

    cryptoContext = Encryption.newContext(conf);
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher aes = Encryption.getCipher(conf, algorithm);
    assertNotNull(aes);
    cryptoContext.setCipher(aes);
    byte[] key = new byte[aes.getKeyLength()];
    Bytes.secureRandom(key);
    cryptoContext.setKey(key);
  }

  private int writeBlock(FSDataOutputStream os, HFileContext fileContext, int size)
    throws IOException {
    HFileBlock.Writer hbw = new HFileBlock.Writer(null, fileContext);
    DataOutputStream dos = hbw.startWriting(BlockType.DATA);
    for (int j = 0; j < size; j++) {
      dos.writeInt(j);
    }
    hbw.writeHeaderAndData(os);
    LOG.info("Wrote a block at " + os.getPos() + " with" + " onDiskSizeWithHeader="
      + hbw.getOnDiskSizeWithHeader() + " uncompressedSizeWithoutHeader="
      + hbw.getOnDiskSizeWithoutHeader() + " uncompressedSizeWithoutHeader="
      + hbw.getUncompressedSizeWithoutHeader());
    return hbw.getOnDiskSizeWithHeader();
  }

  private long readAndVerifyBlock(long pos, HFileContext ctx, HFileBlock.FSReaderImpl hbr, int size)
    throws IOException {
    HFileBlock b = hbr.readBlockData(pos, -1, false, false, true);
    assertEquals(0, HFile.getAndResetChecksumFailuresCount());
    b.sanityCheck();
    assertFalse(b.isUnpacked());
    b = b.unpack(ctx, hbr);
    LOG.info(
      "Read a block at " + pos + " with" + " onDiskSizeWithHeader=" + b.getOnDiskSizeWithHeader()
        + " uncompressedSizeWithoutHeader=" + b.getOnDiskSizeWithoutHeader()
        + " uncompressedSizeWithoutHeader=" + b.getUncompressedSizeWithoutHeader());
    DataInputStream dis = b.getByteStream();
    for (int i = 0; i < size; i++) {
      int read = dis.readInt();
      if (read != i) {
        fail("Block data corrupt at element " + i);
      }
    }
    return b.getOnDiskSizeWithHeader();
  }

  @Test
  public void testDataBlockEncryption() throws IOException {
    final int blocks = 10;
    final int[] blockSizes = new int[blocks];
    final Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < blocks; i++) {
      blockSizes[i] = (1024 + rand.nextInt(1024 * 63)) / Bytes.SIZEOF_INT;
    }
    for (Compression.Algorithm compression : TestHFileBlock.COMPRESSION_ALGORITHMS) {
      Path path = new Path(TEST_UTIL.getDataTestDir(), "block_v3_" + compression + "_AES");
      LOG.info("testDataBlockEncryption: encryption=AES compression=" + compression);
      long totalSize = 0;
      HFileContext fileContext = new HFileContextBuilder().withCompression(compression)
        .withEncryptionContext(cryptoContext).build();
      FSDataOutputStream os = fs.create(path);
      try {
        for (int i = 0; i < blocks; i++) {
          totalSize += writeBlock(os, fileContext, blockSizes[i]);
        }
      } finally {
        os.close();
      }
      FSDataInputStream is = fs.open(path);
      ReaderContext context =
        new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(is))
          .withFilePath(path).withFileSystem(fs).withFileSize(totalSize).build();
      try {
        HFileBlock.FSReaderImpl hbr =
          new HFileBlock.FSReaderImpl(context, fileContext, ByteBuffAllocator.HEAP);
        long pos = 0;
        for (int i = 0; i < blocks; i++) {
          pos += readAndVerifyBlock(pos, fileContext, hbr, blockSizes[i]);
        }
      } finally {
        is.close();
      }
    }
  }

  @Test
  public void testHFileEncryptionMetadata() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext fileContext =
      new HFileContextBuilder().withEncryptionContext(cryptoContext).build();

    // write a simple encrypted hfile
    Path path = new Path(TEST_UTIL.getDataTestDir(), "cryptometa.hfile");
    FSDataOutputStream out = fs.create(path);
    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf).withOutputStream(out)
      .withFileContext(fileContext).create();
    try {
      KeyValue kv = new KeyValue("foo".getBytes(), "f1".getBytes(), null, "value".getBytes());
      writer.append(kv);
    } finally {
      writer.close();
      out.close();
    }

    // read it back in and validate correct crypto metadata
    HFile.Reader reader = HFile.createReader(fs, path, cacheConf, true, conf);
    try {
      FixedFileTrailer trailer = reader.getTrailer();
      assertNotNull(trailer.getEncryptionKey());
      Encryption.Context readerContext = reader.getFileContext().getEncryptionContext();
      assertEquals(readerContext.getCipher().getName(), cryptoContext.getCipher().getName());
      assertTrue(Bytes.equals(readerContext.getKeyBytes(), cryptoContext.getKeyBytes()));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testHFileEncryption() throws Exception {
    // Create 1000 random test KVs
    RedundantKVGenerator generator = new RedundantKVGenerator();
    List<KeyValue> testKvs = generator.generateTestKeyValues(1000);

    // Iterate through data block encoding and compression combinations
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      for (Compression.Algorithm compression : TestHFileBlock.COMPRESSION_ALGORITHMS) {
        HFileContext fileContext = new HFileContextBuilder().withBlockSize(4096) // small blocks
          .withEncryptionContext(cryptoContext).withCompression(compression)
          .withDataBlockEncoding(encoding).build();
        // write a new test HFile
        LOG.info("Writing with " + fileContext);
        Path path =
          new Path(TEST_UTIL.getDataTestDir(), TEST_UTIL.getRandomUUID().toString() + ".hfile");
        FSDataOutputStream out = fs.create(path);
        HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf).withOutputStream(out)
          .withFileContext(fileContext).create();
        try {
          for (KeyValue kv : testKvs) {
            writer.append(kv);
          }
        } finally {
          writer.close();
          out.close();
        }

        // read it back in
        LOG.info("Reading with " + fileContext);
        int i = 0;
        HFileScanner scanner = null;
        HFile.Reader reader = HFile.createReader(fs, path, cacheConf, true, conf);
        try {
          FixedFileTrailer trailer = reader.getTrailer();
          assertNotNull(trailer.getEncryptionKey());
          scanner = reader.getScanner(false, false);
          assertTrue("Initial seekTo failed", scanner.seekTo());
          do {
            Cell kv = scanner.getCell();
            assertTrue("Read back an unexpected or invalid KV",
              testKvs.contains(KeyValueUtil.ensureKeyValue(kv)));
            i++;
          } while (scanner.next());
        } finally {
          reader.close();
          scanner.close();
        }

        assertEquals("Did not read back as many KVs as written", i, testKvs.size());

        // Test random seeks with pread
        LOG.info("Random seeking with " + fileContext);
        Random rand = ThreadLocalRandom.current();
        reader = HFile.createReader(fs, path, cacheConf, true, conf);
        try {
          scanner = reader.getScanner(false, true);
          assertTrue("Initial seekTo failed", scanner.seekTo());
          for (i = 0; i < 100; i++) {
            KeyValue kv = testKvs.get(rand.nextInt(testKvs.size()));
            assertEquals("Unable to find KV as expected: " + kv, 0, scanner.seekTo(kv));
          }
        } finally {
          scanner.close();
          reader.close();
        }
      }
    }
  }

}
