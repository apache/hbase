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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.test.RedundantKVGenerator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestHFileEncryption {
  private static final Log LOG = LogFactory.getLog(TestHFileEncryption.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final SecureRandom RNG = new SecureRandom();

  private static FileSystem fs;
  private static Encryption.Context cryptoContext;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setInt("hfile.format.version", 3);

    fs = FileSystem.get(conf);

    cryptoContext = Encryption.newContext(conf);
    Cipher aes = Encryption.getCipher(conf, "AES");
    assertNotNull(aes);
    cryptoContext.setCipher(aes);
    byte[] key = new byte[aes.getKeyLength()];
    RNG.nextBytes(key);
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
    LOG.info("Wrote a block at " + os.getPos() + " with" +
        " onDiskSizeWithHeader=" + hbw.getOnDiskSizeWithHeader() +
        " uncompressedSizeWithoutHeader=" + hbw.getOnDiskSizeWithoutHeader() +
        " uncompressedSizeWithoutHeader=" + hbw.getUncompressedSizeWithoutHeader());
    return hbw.getOnDiskSizeWithHeader();
  }

  private long readAndVerifyBlock(long pos, HFileContext ctx, HFileBlock.FSReaderImpl hbr, int size)
      throws IOException {
    HFileBlock b = hbr.readBlockData(pos, -1, -1, false);
    assertEquals(0, HFile.getChecksumFailuresCount());
    b.sanityCheck();
    assertFalse(b.isUnpacked());
    b = b.unpack(ctx, hbr);
    LOG.info("Read a block at " + pos + " with" +
        " onDiskSizeWithHeader=" + b.getOnDiskSizeWithHeader() +
        " uncompressedSizeWithoutHeader=" + b.getOnDiskSizeWithoutHeader() +
        " uncompressedSizeWithoutHeader=" + b.getUncompressedSizeWithoutHeader());
    DataInputStream dis = b.getByteStream();
    for (int i = 0; i < size; i++) {
      int read = dis.readInt();
      if (read != i) {
        fail("Block data corrupt at element " + i);
      }
    }
    return b.getOnDiskSizeWithHeader();
  }

  @Test(timeout=20000)
  public void testDataBlockEncryption() throws IOException {
    final int blocks = 10;
    final int[] blockSizes = new int[blocks];
    for (int i = 0; i < blocks; i++) {
      blockSizes[i] = (1024 + RNG.nextInt(1024 * 63)) / Bytes.SIZEOF_INT;
    }
    for (Compression.Algorithm compression : TestHFileBlock.COMPRESSION_ALGORITHMS) {
      Path path = new Path(TEST_UTIL.getDataTestDir(), "block_v3_" + compression + "_AES");
      LOG.info("testDataBlockEncryption: encryption=AES compression=" + compression);
      long totalSize = 0;
      HFileContext fileContext = new HFileContextBuilder()
        .withCompression(compression)
        .withEncryptionContext(cryptoContext)
        .build();
      FSDataOutputStream os = fs.create(path);
      try {
        for (int i = 0; i < blocks; i++) {
          totalSize += writeBlock(os, fileContext, blockSizes[i]);
        }
      } finally {
        os.close();
      }
      FSDataInputStream is = fs.open(path);
      try {
        HFileBlock.FSReaderImpl hbr = new HFileBlock.FSReaderImpl(is, totalSize, fileContext);
        long pos = 0;
        for (int i = 0; i < blocks; i++) {
          pos += readAndVerifyBlock(pos, fileContext, hbr, blockSizes[i]);
        }
      } finally {
        is.close();
      }
    }
  }

  @Test(timeout=20000)
  public void testHFileEncryptionMetadata() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);

    HFileContext fileContext = new HFileContextBuilder()
    .withEncryptionContext(cryptoContext)
    .build();

    // write a simple encrypted hfile
    Path path = new Path(TEST_UTIL.getDataTestDir(), "cryptometa.hfile");
    FSDataOutputStream out = fs.create(path);
    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
      .withOutputStream(out)
      .withFileContext(fileContext)
      .create();
    KeyValue kv = new KeyValue("foo".getBytes(), "f1".getBytes(), null, "value".getBytes());
    writer.append(kv);
    writer.close();
    out.close();

    // read it back in and validate correct crypto metadata
    HFile.Reader reader = HFile.createReader(fs, path, cacheConf, conf);
    reader.loadFileInfo();
    FixedFileTrailer trailer = reader.getTrailer();
    assertNotNull(trailer.getEncryptionKey());
    Encryption.Context readerContext = reader.getFileContext().getEncryptionContext();
    assertEquals(readerContext.getCipher().getName(), cryptoContext.getCipher().getName());
    assertTrue(Bytes.equals(readerContext.getKeyBytes(),
      cryptoContext.getKeyBytes()));
  }

  @Test(timeout=6000000)
  public void testHFileEncryption() throws Exception {
    // Create 1000 random test KVs
    RedundantKVGenerator generator = new RedundantKVGenerator();
    List<KeyValue> testKvs = generator.generateTestKeyValues(1000);

    // Iterate through data block encoding and compression combinations
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);
    for (DataBlockEncoding encoding: DataBlockEncoding.values()) {
      for (Compression.Algorithm compression: TestHFileBlock.COMPRESSION_ALGORITHMS) {
        HFileContext fileContext = new HFileContextBuilder()
          .withBlockSize(4096) // small blocks
          .withEncryptionContext(cryptoContext)
          .withCompression(compression)
          .withDataBlockEncoding(encoding)
          .build();
        // write a new test HFile
        LOG.info("Writing with " + fileContext);
        Path path = new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString() + ".hfile");
        FSDataOutputStream out = fs.create(path);
        HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(out)
          .withFileContext(fileContext)
          .create();
        for (KeyValue kv: testKvs) {
          writer.append(kv);
        }
        writer.close();
        out.close();

        // read it back in
        LOG.info("Reading with " + fileContext);
        HFile.Reader reader = HFile.createReader(fs, path, cacheConf, conf);
        reader.loadFileInfo();
        FixedFileTrailer trailer = reader.getTrailer();
        assertNotNull(trailer.getEncryptionKey());
        HFileScanner scanner = reader.getScanner(false, false);
        assertTrue("Initial seekTo failed", scanner.seekTo());
        int i = 0;
        do {
          Cell kv = scanner.getKeyValue();
          assertTrue("Read back an unexpected or invalid KV",
              testKvs.contains(KeyValueUtil.ensureKeyValue(kv)));
          i++;
        } while (scanner.next());
        reader.close();

        assertEquals("Did not read back as many KVs as written", i, testKvs.size());

        // Test random seeks with pread
        LOG.info("Random seeking with " + fileContext);
        reader = HFile.createReader(fs, path, cacheConf, conf);
        scanner = reader.getScanner(false, true);
        assertTrue("Initial seekTo failed", scanner.seekTo());
        for (i = 0; i < 100; i++) {
          KeyValue kv = testKvs.get(RNG.nextInt(testKvs.size()));
          assertEquals("Unable to find KV as expected: " + kv, scanner.seekTo(kv), 0);
        }
        reader.close();
      }
    }
  }

}
