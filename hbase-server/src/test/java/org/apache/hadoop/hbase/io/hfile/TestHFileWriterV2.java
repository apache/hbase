/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testing writing a version 2 {@link HFile}. This is a low-level test written
 * during the development of {@link HFileWriterV2}.
 */
@Category(SmallTests.class)
public class TestHFileWriterV2 {

  private static final Log LOG = LogFactory.getLog(TestHFileWriterV2.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testHFileFormatV2() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV2");
    final Compression.Algorithm compressAlgo = Compression.Algorithm.GZ;
    final int entryCount = 10000;
    writeDataAndReadFromHFile(hfilePath, compressAlgo, entryCount, false);
  }

  @Test
  public void testMidKeyInHFile() throws IOException{
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
    "testMidKeyInHFile");
    Compression.Algorithm compressAlgo = Compression.Algorithm.NONE;
    int entryCount = 50000;
    writeDataAndReadFromHFile(hfilePath, compressAlgo, entryCount, true);
  }

  private void writeDataAndReadFromHFile(Path hfilePath,
      Algorithm compressAlgo, int entryCount, boolean findMidKey) throws IOException {

    HFileContext context = new HFileContextBuilder()
                           .withBlockSize(4096)
                           .withCompression(compressAlgo)
                           .build();
    HFileWriterV2 writer = (HFileWriterV2)
        new HFileWriterV2.WriterFactoryV2(conf, new CacheConfig(conf))
            .withPath(fs, hfilePath)
            .withFileContext(context)
            .create();

    Random rand = new Random(9713312); // Just a fixed seed.
    List<KeyValue> keyValues = new ArrayList<KeyValue>(entryCount);

    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = randomOrderedKey(rand, i);

      // A random-length random value.
      byte[] valueBytes = randomValue(rand);
      KeyValue keyValue = new KeyValue(keyBytes, null, null, valueBytes);
      writer.append(keyValue);
      keyValues.add(keyValue);
    }

    // Add in an arbitrary order. They will be sorted lexicographically by
    // the key.
    writer.appendMetaBlock("CAPITAL_OF_USA", new Text("Washington, D.C."));
    writer.appendMetaBlock("CAPITAL_OF_RUSSIA", new Text("Moscow"));
    writer.appendMetaBlock("CAPITAL_OF_FRANCE", new Text("Paris"));

    writer.close();
    

    FSDataInputStream fsdis = fs.open(hfilePath);

    // A "manual" version of a new-format HFile reader. This unit test was
    // written before the V2 reader was fully implemented.

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer =
        FixedFileTrailer.readFromStream(fsdis, fileSize);

    assertEquals(2, trailer.getMajorVersion());
    assertEquals(entryCount, trailer.getEntryCount());

    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(true)
                        .withIncludesMvcc(false)
                        .withIncludesTags(false)
                        .withCompression(compressAlgo)
                        .build();
    
    HFileBlock.FSReader blockReader = new HFileBlock.FSReaderImpl(fsdis, fileSize, meta);
    // Comparator class name is stored in the trailer in version 2.
    KVComparator comparator = trailer.createComparator();
    HFileBlockIndex.BlockIndexReader dataBlockIndexReader =
        new HFileBlockIndex.BlockIndexReader(comparator,
            trailer.getNumDataIndexLevels());
    HFileBlockIndex.BlockIndexReader metaBlockIndexReader =
        new HFileBlockIndex.BlockIndexReader(
            KeyValue.RAW_COMPARATOR, 1);

    HFileBlock.BlockIterator blockIter = blockReader.blockRange(
        trailer.getLoadOnOpenDataOffset(),
        fileSize - trailer.getTrailerSize());
    // Data index. We also read statistics about the block index written after
    // the root level.
    dataBlockIndexReader.readMultiLevelIndexRoot(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),
        trailer.getDataIndexCount());
    
    if (findMidKey) {
      byte[] midkey = dataBlockIndexReader.midkey();
      assertNotNull("Midkey should not be null", midkey);
    }
    
    // Meta index.
    metaBlockIndexReader.readRootIndex(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX)
          .getByteStream(), trailer.getMetaIndexCount());
    // File info
    FileInfo fileInfo = new FileInfo();
    fileInfo.read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    byte [] keyValueFormatVersion = fileInfo.get(
        HFileWriterV2.KEY_VALUE_VERSION);
    boolean includeMemstoreTS = keyValueFormatVersion != null &&
        Bytes.toInt(keyValueFormatVersion) > 0;

    // Counters for the number of key/value pairs and the number of blocks
    int entriesRead = 0;
    int blocksRead = 0;
    long memstoreTS = 0;

    // Scan blocks the way the reader would scan them
    fsdis.seek(0);
    long curBlockPos = 0;
    while (curBlockPos <= trailer.getLastDataBlockOffset()) {
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, -1, false);
      assertEquals(BlockType.DATA, block.getBlockType());
      if (meta.isCompressedOrEncrypted()) {
        assertFalse(block.isUnpacked());
        block = block.unpack(meta, blockReader);
      }
      ByteBuffer buf = block.getBufferWithoutHeader();
      while (buf.hasRemaining()) {
        int keyLen = buf.getInt();
        int valueLen = buf.getInt();

        byte[] key = new byte[keyLen];
        buf.get(key);

        byte[] value = new byte[valueLen];
        buf.get(value);

        if (includeMemstoreTS) {
          ByteArrayInputStream byte_input = new ByteArrayInputStream(buf.array(),
                               buf.arrayOffset() + buf.position(), buf.remaining());
          DataInputStream data_input = new DataInputStream(byte_input);

          memstoreTS = WritableUtils.readVLong(data_input);
          buf.position(buf.position() + WritableUtils.getVIntSize(memstoreTS));
        }

        // A brute-force check to see that all keys and values are correct.
        assertTrue(Bytes.compareTo(key, keyValues.get(entriesRead).getKey()) == 0);
        assertTrue(Bytes.compareTo(value, keyValues.get(entriesRead).getValue()) == 0);

        ++entriesRead;
      }
      ++blocksRead;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }
    LOG.info("Finished reading: entries=" + entriesRead + ", blocksRead="
        + blocksRead);
    assertEquals(entryCount, entriesRead);

    // Meta blocks. We can scan until the load-on-open data offset (which is
    // the root block index offset in version 2) because we are not testing
    // intermediate-level index blocks here.

    int metaCounter = 0;
    while (fsdis.getPos() < trailer.getLoadOnOpenDataOffset()) {
      LOG.info("Current offset: " + fsdis.getPos() + ", scanning until " +
          trailer.getLoadOnOpenDataOffset());
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, -1, false)
        .unpack(meta, blockReader);
      assertEquals(BlockType.META, block.getBlockType());
      Text t = new Text();
      ByteBuffer buf = block.getBufferWithoutHeader();
      if (Writables.getWritable(buf.array(), buf.arrayOffset(), buf.limit(), t) == null) {
        throw new IOException("Failed to deserialize block " + this + " into a " + t.getClass().getSimpleName());
      }
      Text expectedText =
          (metaCounter == 0 ? new Text("Paris") : metaCounter == 1 ? new Text(
              "Moscow") : new Text("Washington, D.C."));
      assertEquals(expectedText, t);
      LOG.info("Read meta block data: " + t);
      ++metaCounter;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }

    fsdis.close();
  }


  // Static stuff used by various HFile v2 unit tests

  private static final String COLUMN_FAMILY_NAME = "_-myColumnFamily-_";
  private static final int MIN_ROW_OR_QUALIFIER_LENGTH = 64;
  private static final int MAX_ROW_OR_QUALIFIER_LENGTH = 128;

  /**
   * Generates a random key that is guaranteed to increase as the given index i
   * increases. The result consists of a prefix, which is a deterministic
   * increasing function of i, and a random suffix.
   *
   * @param rand
   *          random number generator to use
   * @param i
   * @return
   */
  public static byte[] randomOrderedKey(Random rand, int i) {
    StringBuilder k = new StringBuilder();

    // The fixed-length lexicographically increasing part of the key.
    for (int bitIndex = 31; bitIndex >= 0; --bitIndex) {
      if ((i & (1 << bitIndex)) == 0)
        k.append("a");
      else
        k.append("b");
    }

    // A random-length random suffix of the key.
    for (int j = 0; j < rand.nextInt(50); ++j)
      k.append(randomReadableChar(rand));

    byte[] keyBytes = k.toString().getBytes();
    return keyBytes;
  }

  public static byte[] randomValue(Random rand) {
    StringBuilder v = new StringBuilder();
    for (int j = 0; j < 1 + rand.nextInt(2000); ++j) {
      v.append((char) (32 + rand.nextInt(95)));
    }

    byte[] valueBytes = v.toString().getBytes();
    return valueBytes;
  }

  public static final char randomReadableChar(Random rand) {
    int i = rand.nextInt(26 * 2 + 10 + 1);
    if (i < 26)
      return (char) ('A' + i);
    i -= 26;

    if (i < 26)
      return (char) ('a' + i);
    i -= 26;

    if (i < 10)
      return (char) ('0' + i);
    i -= 10;

    assert i == 0;
    return '_';
  }

  public static byte[] randomRowOrQualifier(Random rand) {
    StringBuilder field = new StringBuilder();
    int fieldLen = MIN_ROW_OR_QUALIFIER_LENGTH
        + rand.nextInt(MAX_ROW_OR_QUALIFIER_LENGTH
            - MIN_ROW_OR_QUALIFIER_LENGTH + 1);
    for (int i = 0; i < fieldLen; ++i)
      field.append(randomReadableChar(rand));
    return field.toString().getBytes();
  }

  public static KeyValue randomKeyValue(Random rand) {
    return new KeyValue(randomRowOrQualifier(rand),
        COLUMN_FAMILY_NAME.getBytes(), randomRowOrQualifier(rand),
        randomValue(rand));
  }


}

