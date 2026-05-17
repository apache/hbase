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
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test HFile block-level encryption using AES-256-GCM.
 */
@Category({ IOTests.class, SmallTests.class })
public class TestHFileEncryptionAES256GCM {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileEncryptionAES256GCM.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileEncryptionAES256GCM.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static FileSystem fs;
  private static Encryption.Context cryptoContext;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setInt("hfile.format.version", 3);

    fs = FileSystem.get(conf);

    cryptoContext = Encryption.newContext(conf);
    Cipher gcmCipher = Encryption.getCipher(conf, HConstants.CIPHER_AES_256_GCM);
    assertNotNull("AES_256_GCM cipher should be available", gcmCipher);
    cryptoContext.setCipher(gcmCipher);
    byte[] key = new byte[gcmCipher.getKeyLength()];
    Bytes.secureRandom(key);
    cryptoContext.setKey(key);
  }

  private int writeBlock(Configuration conf, FSDataOutputStream os, HFileContext fileContext,
    int size) throws IOException {
    HFileBlock.Writer hbw = new HFileBlock.Writer(conf, null, fileContext);
    DataOutputStream dos = hbw.startWriting(BlockType.DATA);
    for (int j = 0; j < size; j++) {
      dos.writeInt(j);
    }
    hbw.writeHeaderAndData(os);
    LOG.info("Wrote a block at " + os.getPos() + " with onDiskSizeWithHeader="
      + hbw.getOnDiskSizeWithHeader());
    return hbw.getOnDiskSizeWithHeader();
  }

  private long readAndVerifyBlock(long pos, HFileContext ctx, HFileBlock.FSReaderImpl hbr, int size)
    throws IOException {
    HFileBlock b = hbr.readBlockData(pos, -1, false, false, true);
    assertEquals(0, HFile.getAndResetChecksumFailuresCount());
    b.sanityCheck();
    assertFalse(
      (b.getHFileContext().getCompression() != Compression.Algorithm.NONE) && b.isUnpacked());
    b = b.unpack(ctx, hbr);
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
  public void testDataBlockEncryptionWithGCM() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    HFileContext fileContext = new HFileContextBuilder().withCompression(Compression.Algorithm.NONE)
      .withEncryptionContext(cryptoContext).build();

    int blockCount = 10;
    int blockSize = 256;
    Path path = new Path(TEST_UTIL.getDataTestDir(), "testGCMEncryption");

    FSDataOutputStream os = fs.create(path);
    try {
      for (int i = 0; i < blockCount; i++) {
        writeBlock(conf, os, fileContext, blockSize);
      }
    } finally {
      os.close();
    }

    FSDataInputStream is = fs.open(path);
    long totalSize = fs.getFileStatus(path).getLen();
    ReaderContext readerCtx =
      new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(is))
        .withFilePath(path).withFileSystem(fs).withFileSize(totalSize).build();
    try {
      HFileBlock.FSReaderImpl hbr =
        new HFileBlock.FSReaderImpl(readerCtx, fileContext, ByteBuffAllocator.HEAP, conf);
      long pos = 0;
      for (int i = 0; i < blockCount; i++) {
        pos += readAndVerifyBlock(pos, fileContext, hbr, blockSize);
      }
    } finally {
      is.close();
    }
  }
}
