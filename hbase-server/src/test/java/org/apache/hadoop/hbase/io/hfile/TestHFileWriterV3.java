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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Testing writing a version 3 {@link HFile}.
 */
@RunWith(Parameterized.class)
@Category({IOTests.class, SmallTests.class})
public class TestHFileWriterV3 {

  private static final Log LOG = LogFactory.getLog(TestHFileWriterV3.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;
  private boolean useTags;
  public TestHFileWriterV3(boolean useTags) {
    this.useTags = useTags;
  }
  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testHFileFormatV3() throws IOException {
    testHFileFormatV3Internals(useTags);
  }

  private void testHFileFormatV3Internals(boolean useTags) throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV3");
    final Compression.Algorithm compressAlgo = Compression.Algorithm.GZ;
    final int entryCount = 10000;
    writeDataAndReadFromHFile(hfilePath, compressAlgo, entryCount, false, useTags);
  }

  @Test
  public void testMidKeyInHFile() throws IOException{
    testMidKeyInHFileInternals(useTags);
  }

  private void testMidKeyInHFileInternals(boolean useTags) throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
    "testMidKeyInHFile");
    Compression.Algorithm compressAlgo = Compression.Algorithm.NONE;
    int entryCount = 50000;
    writeDataAndReadFromHFile(hfilePath, compressAlgo, entryCount, true, useTags);
  }

  private void writeDataAndReadFromHFile(Path hfilePath,
      Algorithm compressAlgo, int entryCount, boolean findMidKey, boolean useTags) throws IOException {
    HFileContext context = new HFileContextBuilder()
                           .withBlockSize(4096)
                           .withIncludesTags(useTags)
                           .withCompression(compressAlgo).build();
    HFile.Writer writer = new HFileWriterFactory(conf, new CacheConfig(conf))
            .withPath(fs, hfilePath)
            .withFileContext(context)
            .withComparator(CellComparator.COMPARATOR)
            .create();

    Random rand = new Random(9713312); // Just a fixed seed.
    List<KeyValue> keyValues = new ArrayList<KeyValue>(entryCount);

    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = TestHFileWriterV2.randomOrderedKey(rand, i);

      // A random-length random value.
      byte[] valueBytes = TestHFileWriterV2.randomValue(rand);
      KeyValue keyValue = null;
      if (useTags) {
        ArrayList<Tag> tags = new ArrayList<Tag>();
        for (int j = 0; j < 1 + rand.nextInt(4); j++) {
          byte[] tagBytes = new byte[16];
          rand.nextBytes(tagBytes);
          tags.add(new Tag((byte) 1, tagBytes));
        }
        keyValue = new KeyValue(keyBytes, null, null, HConstants.LATEST_TIMESTAMP,
            valueBytes, tags);
      } else {
        keyValue = new KeyValue(keyBytes, null, null, HConstants.LATEST_TIMESTAMP,
            valueBytes);
      }
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

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer =
        FixedFileTrailer.readFromStream(fsdis, fileSize);

    assertEquals(3, trailer.getMajorVersion());
    assertEquals(entryCount, trailer.getEntryCount());
    HFileContext meta = new HFileContextBuilder()
                        .withCompression(compressAlgo)
                        .withIncludesMvcc(false)
                        .withIncludesTags(useTags)
                        .withHBaseCheckSum(true).build();
    HFileBlock.FSReader blockReader =
        new HFileBlock.FSReaderImpl(fsdis, fileSize, meta);
 // Comparator class name is stored in the trailer in version 2.
    CellComparator comparator = trailer.createComparator();
    HFileBlockIndex.BlockIndexReader dataBlockIndexReader =
        new HFileBlockIndex.CellBasedKeyBlockIndexReader(comparator,
            trailer.getNumDataIndexLevels());
    HFileBlockIndex.BlockIndexReader metaBlockIndexReader =
        new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);

    HFileBlock.BlockIterator blockIter = blockReader.blockRange(
        trailer.getLoadOnOpenDataOffset(),
        fileSize - trailer.getTrailerSize());
    // Data index. We also read statistics about the block index written after
    // the root level.
    dataBlockIndexReader.readMultiLevelIndexRoot(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX), trailer.getDataIndexCount());
    
    if (findMidKey) {
      Cell midkey = dataBlockIndexReader.midkey();
      assertNotNull("Midkey should not be null", midkey);
    }
    
    // Meta index.
    metaBlockIndexReader.readRootIndex(
        blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX)
          .getByteStream(), trailer.getMetaIndexCount());
    // File info
    FileInfo fileInfo = new FileInfo();
    fileInfo.read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    byte [] keyValueFormatVersion = fileInfo.get(HFileWriterImpl.KEY_VALUE_VERSION);
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
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, -1, false)
        .unpack(context, blockReader);
      assertEquals(BlockType.DATA, block.getBlockType());
      ByteBuffer buf = block.getBufferWithoutHeader();
      int keyLen = -1;
      while (buf.hasRemaining()) {

        keyLen = buf.getInt();

        int valueLen = buf.getInt();

        byte[] key = new byte[keyLen];
        buf.get(key);

        byte[] value = new byte[valueLen];
        buf.get(value);
        byte[] tagValue = null;
        if (useTags) {
          int tagLen = ((buf.get() & 0xff) << 8) ^ (buf.get() & 0xff);
          tagValue = new byte[tagLen];
          buf.get(tagValue);
        }
      
        if (includeMemstoreTS) {
          ByteArrayInputStream byte_input = new ByteArrayInputStream(buf.array(), buf.arrayOffset()
              + buf.position(), buf.remaining());
          DataInputStream data_input = new DataInputStream(byte_input);

          memstoreTS = WritableUtils.readVLong(data_input);
          buf.position(buf.position() + WritableUtils.getVIntSize(memstoreTS));
        }

        // A brute-force check to see that all keys and values are correct.
        assertTrue(Bytes.compareTo(key, keyValues.get(entriesRead).getKey()) == 0);
        assertTrue(Bytes.compareTo(value, keyValues.get(entriesRead).getValue()) == 0);
        if (useTags) {
          assertNotNull(tagValue);
          KeyValue tkv =  keyValues.get(entriesRead);
          assertEquals(tagValue.length, tkv.getTagsLength());
          assertTrue(Bytes.compareTo(tagValue, 0, tagValue.length, tkv.getTagsArray(),
              tkv.getTagsOffset(), tkv.getTagsLength()) == 0);
        }
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
        .unpack(context, blockReader);
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

}

