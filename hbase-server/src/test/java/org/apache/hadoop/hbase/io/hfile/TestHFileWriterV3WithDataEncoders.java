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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing writing a version 3 {@link HFile} for all encoded blocks
 */
@RunWith(Parameterized.class)
@Category({IOTests.class, MediumTests.class})
public class TestHFileWriterV3WithDataEncoders {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileWriterV3WithDataEncoders.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestHFileWriterV3WithDataEncoders.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Random RNG = new Random(9713312); // Just a fixed seed.

  private Configuration conf;
  private FileSystem fs;
  private boolean useTags;
  private DataBlockEncoding dataBlockEncoding;

  public TestHFileWriterV3WithDataEncoders(boolean useTags,
      DataBlockEncoding dataBlockEncoding) {
    this.useTags = useTags;
    this.dataBlockEncoding = dataBlockEncoding;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    DataBlockEncoding[] dataBlockEncodings = DataBlockEncoding.values();
    Object[][] params = new Object[dataBlockEncodings.length * 2 - 2][];
    int i = 0;
    for (DataBlockEncoding dataBlockEncoding : dataBlockEncodings) {
      if (dataBlockEncoding == DataBlockEncoding.NONE) {
        continue;
      }
      params[i++] = new Object[]{false, dataBlockEncoding};
      params[i++] = new Object[]{true, dataBlockEncoding};
    }
    return Arrays.asList(params);
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
      Compression.Algorithm compressAlgo, int entryCount, boolean findMidKey, boolean useTags)
      throws IOException {

    HFileContext context = new HFileContextBuilder()
      .withBlockSize(4096)
      .withIncludesTags(useTags)
      .withDataBlockEncoding(dataBlockEncoding)
      .withCellComparator(CellComparatorImpl.COMPARATOR)
      .withCompression(compressAlgo).build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Writer writer = new HFile.WriterFactory(conf, cacheConfig)
      .withPath(fs, hfilePath)
      .withFileContext(context)
      .create();

    List<KeyValue> keyValues = new ArrayList<>(entryCount);
    writeKeyValues(entryCount, useTags, writer, RNG, keyValues);

    FSDataInputStream fsdis = fs.open(hfilePath);

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer =
      FixedFileTrailer.readFromStream(fsdis, fileSize);

    Assert.assertEquals(3, trailer.getMajorVersion());
    Assert.assertEquals(entryCount, trailer.getEntryCount());
    HFileContext meta = new HFileContextBuilder()
      .withCompression(compressAlgo)
      .withIncludesMvcc(true)
      .withIncludesTags(useTags)
      .withDataBlockEncoding(dataBlockEncoding)
      .withHBaseCheckSum(true).build();
    ReaderContext readerContext = new ReaderContextBuilder()
      .withInputStreamWrapper(new FSDataInputStreamWrapper(fsdis))
      .withFilePath(hfilePath)
      .withFileSystem(fs)
      .withFileSize(fileSize).build();
    HFileBlock.FSReader blockReader =
      new HFileBlock.FSReaderImpl(readerContext, meta, ByteBuffAllocator.HEAP);
    // Comparator class name is stored in the trailer in version 3.
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

    FSDataInputStreamWrapper wrapper = new FSDataInputStreamWrapper(fs, hfilePath);
    readerContext = new ReaderContextBuilder()
      .withFilePath(hfilePath)
      .withFileSize(fileSize)
      .withFileSystem(wrapper.getHfs())
      .withInputStreamWrapper(wrapper)
      .build();
    HFileInfo hfile = new HFileInfo(readerContext, conf);
    HFile.Reader reader = new HFilePreadReader(readerContext, hfile, cacheConfig, conf);
    hfile.initMetaAndIndex(reader);
    if (findMidKey) {
      Cell midkey = dataBlockIndexReader.midkey(reader);
      Assert.assertNotNull("Midkey should not be null", midkey);
    }

    // Meta index.
    metaBlockIndexReader.readRootIndex(
      blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX)
        .getByteStream(), trailer.getMetaIndexCount());
    // File info
    HFileInfo fileInfo = new HFileInfo();
    fileInfo.read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    byte [] keyValueFormatVersion = fileInfo.get(HFileWriterImpl.KEY_VALUE_VERSION);
    boolean includeMemstoreTS = keyValueFormatVersion != null &&
      Bytes.toInt(keyValueFormatVersion) > 0;

    // Counters for the number of key/value pairs and the number of blocks
    int entriesRead = 0;
    int blocksRead = 0;
    long memstoreTS = 0;

    DataBlockEncoder encoder = dataBlockEncoding.getEncoder();
    long curBlockPos = scanBlocks(entryCount, context, keyValues, fsdis, trailer,
      meta, blockReader, entriesRead, blocksRead, encoder);


    // Meta blocks. We can scan until the load-on-open data offset (which is
    // the root block index offset in version 2) because we are not testing
    // intermediate-level index blocks here.

    int metaCounter = 0;
    while (fsdis.getPos() < trailer.getLoadOnOpenDataOffset()) {
      LOG.info("Current offset: {}, scanning until {}", fsdis.getPos(),
        trailer.getLoadOnOpenDataOffset());
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, false, false, true)
        .unpack(context, blockReader);
      Assert.assertEquals(BlockType.META, block.getBlockType());
      Text t = new Text();
      ByteBuff buf = block.getBufferWithoutHeader();
      if (Writables.getWritable(buf.array(), buf.arrayOffset(), buf.limit(), t) == null) {
        throw new IOException("Failed to deserialize block " + this +
          " into a " + t.getClass().getSimpleName());
      }
      Text expectedText =
        (metaCounter == 0 ? new Text("Paris") : metaCounter == 1 ? new Text(
          "Moscow") : new Text("Washington, D.C."));
      Assert.assertEquals(expectedText, t);
      LOG.info("Read meta block data: " + t);
      ++metaCounter;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }

    fsdis.close();
    reader.close();
  }

  private long scanBlocks(int entryCount, HFileContext context, List<KeyValue> keyValues,
      FSDataInputStream fsdis, FixedFileTrailer trailer, HFileContext meta,
      HFileBlock.FSReader blockReader, int entriesRead, int blocksRead,
      DataBlockEncoder encoder) throws IOException {
    // Scan blocks the way the reader would scan them
    fsdis.seek(0);
    long curBlockPos = 0;
    while (curBlockPos <= trailer.getLastDataBlockOffset()) {
      HFileBlockDecodingContext ctx = blockReader.getBlockDecodingContext();
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, false, false, true)
        .unpack(context, blockReader);
      Assert.assertEquals(BlockType.ENCODED_DATA, block.getBlockType());
      ByteBuff origBlock = block.getBufferReadOnly();
      int pos = block.headerSize() + DataBlockEncoding.ID_SIZE;
      origBlock.position(pos);
      origBlock.limit(pos + block.getUncompressedSizeWithoutHeader() - DataBlockEncoding.ID_SIZE);
      ByteBuff buf =  origBlock.slice();
      DataBlockEncoder.EncodedSeeker seeker =
        encoder.createSeeker(encoder.newDataBlockDecodingContext(meta));
      seeker.setCurrentBuffer(buf);
      Cell res = seeker.getCell();
      KeyValue kv = keyValues.get(entriesRead);
      Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(res, kv));
      ++entriesRead;
      while(seeker.next()) {
        res = seeker.getCell();
        kv = keyValues.get(entriesRead);
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(res, kv));
        ++entriesRead;
      }
      ++blocksRead;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }
    LOG.info("Finished reading: entries={}, blocksRead = {}", entriesRead, blocksRead);
    Assert.assertEquals(entryCount, entriesRead);
    return curBlockPos;
  }

  private void writeKeyValues(int entryCount, boolean useTags, HFile.Writer writer,
      Random rand, List<KeyValue> keyValues) throws IOException {

    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = RandomKeyValueUtil.randomOrderedKey(rand, i);

      // A random-length random value.
      byte[] valueBytes = RandomKeyValueUtil.randomValue(rand);
      KeyValue keyValue = null;
      if (useTags) {
        ArrayList<Tag> tags = new ArrayList<>();
        for (int j = 0; j < 1 + rand.nextInt(4); j++) {
          byte[] tagBytes = new byte[16];
          rand.nextBytes(tagBytes);
          tags.add(new ArrayBackedTag((byte) 1, tagBytes));
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
  }

}
