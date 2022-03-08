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

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.BUFFER_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MAX_BUFFER_COUNT_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BLOCKCACHE_POLICY_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * test hfile features.
 */
@Category({IOTests.class, SmallTests.class})
public class TestHFile  {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFile.class);

  @Rule public TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestHFile.class);
  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
    TEST_UTIL.getDataTestDir("TestHFile").toString();
  private final int minBlockSize = 512;
  private static String localFormatter = "%010d";
  private static CacheConfig cacheConf;
  private static Configuration conf ;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    cacheConf = new CacheConfig(conf);
    fs = TEST_UTIL.getTestFileSystem();
  }

  public static Reader createReaderFromStream(ReaderContext context, CacheConfig cacheConf,
      Configuration conf) throws IOException {
    HFileInfo fileInfo = new HFileInfo(context, conf);
    Reader preadReader = HFile.createReader(context, fileInfo, cacheConf, conf);
    fileInfo.initMetaAndIndex(preadReader);
    preadReader.close();
    context = new ReaderContextBuilder()
        .withFileSystemAndPath(context.getFileSystem(), context.getFilePath())
        .withReaderType(ReaderType.STREAM)
        .build();
    Reader streamReader = HFile.createReader(context, fileInfo, cacheConf,  conf);
    return streamReader;
  }

  private ByteBuffAllocator initAllocator(boolean reservoirEnabled, int bufSize, int bufCount,
      int minAllocSize) {
    Configuration that = HBaseConfiguration.create(conf);
    that.setInt(BUFFER_SIZE_KEY, bufSize);
    that.setInt(MAX_BUFFER_COUNT_KEY, bufCount);
    // All ByteBuffers will be allocated from the buffers.
    that.setInt(MIN_ALLOCATE_SIZE_KEY, minAllocSize);
    return ByteBuffAllocator.create(that, reservoirEnabled);
  }

  private void fillByteBuffAllocator(ByteBuffAllocator alloc, int bufCount) {
    // Fill the allocator with bufCount ByteBuffer
    List<ByteBuff> buffs = new ArrayList<>();
    for (int i = 0; i < bufCount; i++) {
      buffs.add(alloc.allocateOneBuffer());
      Assert.assertEquals(alloc.getFreeBufferCount(), 0);
    }
    buffs.forEach(ByteBuff::release);
    Assert.assertEquals(alloc.getFreeBufferCount(), bufCount);
  }

  @Test
  public void testReaderWithoutBlockCache() throws Exception {
    int bufCount = 32;
    // AllByteBuffers will be allocated from the buffers.
    ByteBuffAllocator alloc = initAllocator(true, 64 * 1024, bufCount, 0);
    fillByteBuffAllocator(alloc, bufCount);
    // start write to store file.
    Path path = writeStoreFile();
    try {
      readStoreFile(path, conf, alloc);
    } catch (Exception e) {
      // fail test
      assertTrue(false);
    }
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
  }

  /**
   * Test case for HBASE-22127 in LruBlockCache.
   */
  @Test
  public void testReaderWithLRUBlockCache() throws Exception {
    int bufCount = 1024, blockSize = 64 * 1024;
    ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
    fillByteBuffAllocator(alloc, bufCount);
    Path storeFilePath = writeStoreFile();
    // Open the file reader with LRUBlockCache
    BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
    CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConfig, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
      HFileBlock block = reader.readBlock(offset, -1, true, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      // Ensure the block is an heap one.
      Cacheable cachedBlock = lru.getBlock(key, false, false, true);
      Assert.assertNotNull(cachedBlock);
      Assert.assertTrue(cachedBlock instanceof HFileBlock);
      Assert.assertFalse(((HFileBlock) cachedBlock).isSharedMem());
      // Should never allocate off-heap block from allocator because ensure that it's LRU.
      Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
    lru.shutdown();
  }

  private BlockCache initCombinedBlockCache(final String l1CachePolicy) {
    Configuration that = HBaseConfiguration.create(conf);
    that.setFloat(BUCKET_CACHE_SIZE_KEY, 32); // 32MB for bucket cache.
    that.set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
    that.set(BLOCKCACHE_POLICY_KEY, l1CachePolicy);
    BlockCache bc = BlockCacheFactory.createBlockCache(that);
    Assert.assertNotNull(bc);
    Assert.assertTrue(bc instanceof CombinedBlockCache);
    return bc;
  }

  /**
   * Test case for HBASE-22127 in CombinedBlockCache
   */
  @Test
  public void testReaderWithCombinedBlockCache() throws Exception {
    int bufCount = 1024, blockSize = 64 * 1024;
    ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
    fillByteBuffAllocator(alloc, bufCount);
    Path storeFilePath = writeStoreFile();
    // Open the file reader with CombinedBlockCache
    BlockCache combined = initCombinedBlockCache("LRU");
    conf.setBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, true);
    CacheConfig cacheConfig = new CacheConfig(conf, null, combined, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConfig, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
      HFileBlock block = reader.readBlock(offset, -1, true, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      // Read the cached block.
      Cacheable cachedBlock = combined.getBlock(key, false, false, true);
      try {
        Assert.assertNotNull(cachedBlock);
        Assert.assertTrue(cachedBlock instanceof HFileBlock);
        HFileBlock hfb = (HFileBlock) cachedBlock;
        // Data block will be cached in BucketCache, so it should be an off-heap block.
        if (hfb.getBlockType().isData()) {
          Assert.assertTrue(hfb.isSharedMem());
        } else {
          // Non-data block will be cached in LRUBlockCache, so it must be an on-heap block.
          Assert.assertFalse(hfb.isSharedMem());
        }
      } finally {
        cachedBlock.release();
      }
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
    combined.shutdown();
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
  }

  private void readStoreFile(Path storeFilePath, Configuration conf, ByteBuffAllocator alloc)
      throws Exception {
    // Open the file reader with block cache disabled.
    CacheConfig cache = new CacheConfig(conf, null, null, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cache, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
  }

  private Path writeStoreFile() throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), "TestHFile");
    HFileContext meta = new HFileContextBuilder().withBlockSize(64 * 1024).build();
    StoreFileWriter sfw =
        new StoreFileWriter.Builder(conf, fs).withOutputDir(storeFileParentDir)
          .withFileContext(meta).build();
    final int rowLen = 32;
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 1000; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] v = RandomKeyValueUtil.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv =
          new KeyValue(k, 0, rowLen, k, rowLen, cfLen, k, rowLen + cfLen,
              k.length - rowLen - cfLen, rand.nextLong(), generateKeyType(rand), v, 0, v.length);
      sfw.append(kv);
    }

    sfw.close();
    return sfw.getPath();
  }

  public static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
            + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  /**
   * Test empty HFile.
   * Test all features work reasonably when hfile is empty of entries.
   * @throws IOException
   */
  @Test
  public void testEmptyHFile() throws IOException {
    Path f = new Path(ROOT_DIR, testName.getMethodName());
    HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
    Writer w =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, f).withFileContext(context).create();
    w.close();
    Reader r = HFile.createReader(fs, f, cacheConf, true, conf);
    assertFalse(r.getFirstKey().isPresent());
    assertFalse(r.getLastKey().isPresent());
  }

  /**
   * Create 0-length hfile and show that it fails
   */
  @Test
  public void testCorrupt0LengthHFile() throws IOException {
    Path f = new Path(ROOT_DIR, testName.getMethodName());
    FSDataOutputStream fsos = fs.create(f);
    fsos.close();

    try {
      Reader r = HFile.createReader(fs, f, cacheConf, true, conf);
    } catch (CorruptHFileException | IllegalArgumentException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
  }

  @Test
  public void testCorruptOutOfOrderHFileWrite() throws IOException {
    Path path = new Path(ROOT_DIR, testName.getMethodName());
    FSDataOutputStream mockedOutputStream = Mockito.mock(FSDataOutputStream.class);
    String columnFamily = "MyColumnFamily";
    String tableName = "MyTableName";
    HFileContext fileContext = new HFileContextBuilder()
        .withHFileName(testName.getMethodName() + "HFile")
        .withBlockSize(minBlockSize)
        .withColumnFamily(Bytes.toBytes(columnFamily))
        .withTableName(Bytes.toBytes(tableName))
        .withHBaseCheckSum(false)
        .withCompression(Compression.Algorithm.NONE)
        .withCompressTags(false)
        .build();
    HFileWriterImpl writer = new HFileWriterImpl(conf, cacheConf, path, mockedOutputStream,
      fileContext);
    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    byte[] row = Bytes.toBytes("foo");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] cf = Bytes.toBytes(columnFamily);
    byte[] val = Bytes.toBytes("fooVal");
    long firstTS = 100L;
    long secondTS = 101L;
    Cell firstCell = cellBuilder.setRow(row).setValue(val).setTimestamp(firstTS)
        .setQualifier(qualifier).setFamily(cf).setType(Cell.Type.Put).build();
    Cell secondCell= cellBuilder.setRow(row).setValue(val).setTimestamp(secondTS)
        .setQualifier(qualifier).setFamily(cf).setType(Cell.Type.Put).build();
    //second Cell will sort "higher" than the first because later timestamps should come first
    writer.append(firstCell);
    try {
      writer.append(secondCell);
    } catch(IOException ie){
      String message = ie.getMessage();
      Assert.assertTrue(message.contains("not lexically larger"));
      Assert.assertTrue(message.contains(tableName));
      Assert.assertTrue(message.contains(columnFamily));
      return;
    }
    Assert.fail("Exception wasn't thrown even though Cells were appended in the wrong order!");
  }

  public static void truncateFile(FileSystem fs, Path src, Path dst) throws IOException {
    FileStatus fst = fs.getFileStatus(src);
    long len = fst.getLen();
    len = len / 2 ;

    // create a truncated hfile
    FSDataOutputStream fdos = fs.create(dst);
    byte[] buf = new byte[(int)len];
    FSDataInputStream fdis = fs.open(src);
    fdis.read(buf);
    fdos.write(buf);
    fdis.close();
    fdos.close();
  }

  /**
   * Create a truncated hfile and verify that exception thrown.
   */
  @Test
  public void testCorruptTruncatedHFile() throws IOException {
    Path f = new Path(ROOT_DIR, testName.getMethodName());
    HFileContext  context = new HFileContextBuilder().build();
    Writer w = HFile.getWriterFactory(conf, cacheConf).withPath(this.fs, f)
        .withFileContext(context).create();
    writeSomeRecords(w, 0, 100, false);
    w.close();

    Path trunc = new Path(f.getParent(), "trucated");
    truncateFile(fs, w.getPath(), trunc);

    try {
      HFile.createReader(fs, trunc, cacheConf, true, conf);
    } catch (CorruptHFileException | IllegalArgumentException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
  }

  // write some records into the hfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n, boolean useTags)
      throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, Integer.valueOf(i));
      if (useTags) {
        Tag t = new ArrayBackedTag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value + key), tags);
        writer.append(kv);
      } else {
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            Bytes.toBytes(value + key));
        writer.append(kv);
      }
    }
    return (start + n);
  }

  private void readAllRecords(HFileScanner scanner) throws IOException {
    readAndCheckbytes(scanner, 0, 100);
  }

  // read the records and check
  private int readAndCheckbytes(HFileScanner scanner, int start, int n)
      throws IOException {
    String value = "value";
    int i = start;
    for (; i < (start + n); i++) {
      ByteBuffer key = ByteBuffer.wrap(((KeyValue)scanner.getKey()).getKey());
      ByteBuffer val = scanner.getValue();
      String keyStr = String.format(localFormatter, Integer.valueOf(i));
      String valStr = value + keyStr;
      KeyValue kv = new KeyValue(Bytes.toBytes(keyStr), Bytes.toBytes("family"),
          Bytes.toBytes("qual"), Bytes.toBytes(valStr));
      byte[] keyBytes = new KeyValue.KeyOnlyKeyValue(Bytes.toBytes(key), 0,
          Bytes.toBytes(key).length).getKey();
      assertTrue("bytes for keys do not match " + keyStr + " " +
        Bytes.toString(Bytes.toBytes(key)),
          Arrays.equals(kv.getKey(), keyBytes));
      byte [] valBytes = Bytes.toBytes(val);
      assertTrue("bytes for vals do not match " + valStr + " " +
        Bytes.toString(valBytes),
        Arrays.equals(Bytes.toBytes(valStr), valBytes));
      if (!scanner.next()) {
        break;
      }
    }
    assertEquals(i, start + n - 1);
    return (start + n);
  }

  private byte[] getSomeKey(int rowId) {
    KeyValue kv = new KeyValue(String.format(localFormatter, Integer.valueOf(rowId)).getBytes(),
        Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, Type.Put);
    return kv.getKey();
  }

  private void writeRecords(Writer writer, boolean useTags) throws IOException {
    writeSomeRecords(writer, 0, 100, useTags);
    writer.close();
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    //if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  /**
   * test none codecs
   * @param useTags
   */
  void basicWithSomeCodec(String codec, boolean useTags) throws IOException {
    if (useTags) {
      conf.setInt("hfile.format.version", 3);
    }
    Path  ncHFile = new Path(ROOT_DIR, "basic.hfile." + codec.toString() + useTags);
    FSDataOutputStream fout = createFSOutput(ncHFile);
    HFileContext meta = new HFileContextBuilder()
                        .withBlockSize(minBlockSize)
                        .withCompression(HFileWriterImpl.compressionByName(codec))
                        .build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .create();
    LOG.info(Objects.toString(writer));
    writeRecords(writer, useTags);
    fout.close();
    FSDataInputStream fin = fs.open(ncHFile);
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, ncHFile).build();
    Reader reader = createReaderFromStream(context, cacheConf, conf);
    System.out.println(cacheConf.toString());
    // Load up the index.
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, false);
    // Align scanner at start of the file.
    scanner.seekTo();
    readAllRecords(scanner);
    int seekTo = scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(50)));
    System.out.println(seekTo);
    assertTrue("location lookup failed",
        scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(50))) == 0);
    // read the key and see if it matches
    ByteBuffer readKey = ByteBuffer.wrap(((KeyValue)scanner.getKey()).getKey());
    assertTrue("seeked key does not match", Arrays.equals(getSomeKey(50),
      Bytes.toBytes(readKey)));

    scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val1 = scanner.getValue();
    scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val2 = scanner.getValue();
    assertTrue(Arrays.equals(Bytes.toBytes(val1), Bytes.toBytes(val2)));

    reader.close();
    fin.close();
    fs.delete(ncHFile, true);
  }

  @Test
  public void testTFileFeatures() throws IOException {
    testHFilefeaturesInternals(false);
    testHFilefeaturesInternals(true);
  }

  protected void testHFilefeaturesInternals(boolean useTags) throws IOException {
    basicWithSomeCodec("none", useTags);
    basicWithSomeCodec("gz", useTags);
  }

  private void writeNumMetablocks(Writer writer, int n) {
    for (int i = 0; i < n; i++) {
      writer.appendMetaBlock("HFileMeta" + i, new Writable() {
        private int val;
        public Writable setVal(int val) { this.val = val; return this; }

        @Override
        public void write(DataOutput out) throws IOException {
          out.write(("something to test" + val).getBytes());
        }

        @Override
        public void readFields(DataInput in) throws IOException { }
      }.setVal(i));
    }
  }

  private void someTestingWithMetaBlock(Writer writer) {
    writeNumMetablocks(writer, 10);
  }

  private void readNumMetablocks(Reader reader, int n) throws IOException {
    for (int i = 0; i < n; i++) {
      ByteBuff actual = reader.getMetaBlock("HFileMeta" + i, false).getBufferWithoutHeader();
      ByteBuffer expected =
        ByteBuffer.wrap(("something to test" + i).getBytes());
      assertEquals(
          "failed to match metadata",
          Bytes.toStringBinary(expected),
          Bytes.toStringBinary(actual.array(), actual.arrayOffset() + actual.position(),
              actual.capacity()));
    }
  }

  private void someReadingWithMetaBlock(Reader reader) throws IOException {
    readNumMetablocks(reader, 10);
  }

  private void metablocks(final String compress) throws Exception {
    Path mFile = new Path(ROOT_DIR, "meta.hfile");
    FSDataOutputStream fout = createFSOutput(mFile);
    HFileContext meta = new HFileContextBuilder()
                        .withCompression(HFileWriterImpl.compressionByName(compress))
                        .withBlockSize(minBlockSize).build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .create();
    someTestingWithMetaBlock(writer);
    writer.close();
    fout.close();
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, mFile).build();
    Reader reader = createReaderFromStream(context, cacheConf, conf);
    // No data -- this should return false.
    assertFalse(reader.getScanner(false, false).seekTo());
    someReadingWithMetaBlock(reader);
    fs.delete(mFile, true);
    reader.close();
  }

  // test meta blocks for hfiles
  @Test
  public void testMetaBlocks() throws Exception {
    metablocks("none");
    metablocks("gz");
  }

  @Test
  public void testNullMetaBlocks() throws Exception {
    for (Compression.Algorithm compressAlgo :
        HBaseCommonTestingUtility.COMPRESSION_ALGORITHMS) {
      Path mFile = new Path(ROOT_DIR, "nometa_" + compressAlgo + ".hfile");
      FSDataOutputStream fout = createFSOutput(mFile);
      HFileContext meta = new HFileContextBuilder().withCompression(compressAlgo)
                          .withBlockSize(minBlockSize).build();
      Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(fout)
          .withFileContext(meta)
          .create();
      KeyValue kv = new KeyValue("foo".getBytes(), "f1".getBytes(), null, "value".getBytes());
      writer.append(kv);
      writer.close();
      fout.close();
      Reader reader = HFile.createReader(fs, mFile, cacheConf, true, conf);
      assertNull(reader.getMetaBlock("non-existant", false));
    }
  }

  /**
   * Make sure the ordinals for our compression algorithms do not change on us.
   */
  @Test
  public void testCompressionOrdinance() {
    assertTrue(Compression.Algorithm.LZO.ordinal() == 0);
    assertTrue(Compression.Algorithm.GZ.ordinal() == 1);
    assertTrue(Compression.Algorithm.NONE.ordinal() == 2);
    assertTrue(Compression.Algorithm.SNAPPY.ordinal() == 3);
    assertTrue(Compression.Algorithm.LZ4.ordinal() == 4);
  }

  @Test
  public void testShortMidpointSameQual() {
    Cell left = CellUtil.createCell(Bytes.toBytes("a"),
        Bytes.toBytes("a"),
        Bytes.toBytes("a"),
        11,
        KeyValue.Type.Maximum.getCode(),
        HConstants.EMPTY_BYTE_ARRAY);
    Cell right = CellUtil.createCell(Bytes.toBytes("a"),
        Bytes.toBytes("a"),
        Bytes.toBytes("a"),
        9,
        KeyValue.Type.Maximum.getCode(),
        HConstants.EMPTY_BYTE_ARRAY);
    Cell mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) <= 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) == 0);
  }

  @Test
  public void testGetShortMidpoint() {
    Cell left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) <= 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("bbbbbbb"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) < 0);
    assertEquals(1, mid.getRowLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaa"), Bytes.toBytes("b"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) < 0);
    assertEquals(2, mid.getFamilyLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaaa"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) < 0);
    assertEquals(2, mid.getQualifierLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("b"));
    mid = HFileWriterImpl.getMidpoint(CellComparatorImpl.COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) <= 0);
    assertEquals(1, mid.getQualifierLength());

    // Assert that if meta comparator, it returns the right cell -- i.e. no
    // optimization done.
    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(MetaCellComparator.META_COMPARATOR, left, right);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, mid) < 0);
    assertTrue(PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, mid, right) == 0);

    /**
     * See HBASE-7845
     */
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");

    byte[] family = Bytes.toBytes("family");
    byte[] qualA = Bytes.toBytes("qfA");
    byte[] qualB = Bytes.toBytes("qfB");
    final CellComparatorImpl keyComparator = CellComparatorImpl.COMPARATOR;
    // verify that faked shorter rowkey could be generated
    long ts = 5;
    KeyValue kv1 = new KeyValue(Bytes.toBytes("the quick brown fox"), family, qualA, ts, Type.Put);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("the who test text"), family, qualA, ts, Type.Put);
    Cell newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    byte[] expectedArray = Bytes.toBytes("the r");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);

    // verify: same with "row + family + qualifier", return rightKey directly
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 0, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) == 0);
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -10, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) == 0);

    // verify: same with row, different with qualifier
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualB, 5, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    assertTrue(Arrays.equals(CellUtil.cloneFamily(newKey), family));
    assertTrue(Arrays.equals(CellUtil.cloneQualifier(newKey), qualB));
    assertTrue(newKey.getTimestamp() == HConstants.LATEST_TIMESTAMP);
    assertTrue(newKey.getTypeByte() == Type.Maximum.getCode());

    // verify metaKeyComparator's getShortMidpointKey output
    final CellComparatorImpl metaKeyComparator = MetaCellComparator.META_COMPARATOR;
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase123"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase234"), family, qualA, 0, Type.Put);
    newKey = HFileWriterImpl.getMidpoint(metaKeyComparator, kv1, kv2);
    assertTrue(metaKeyComparator.compare(kv1, newKey) < 0);
    assertTrue((metaKeyComparator.compare(kv2, newKey) == 0));

    // verify common fix scenario
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbaseandhdfs"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    expectedArray = Bytes.toBytes("ilovehbasea");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);
    // verify only 1 offset scenario
    kv1 = new KeyValue(Bytes.toBytes("100abcdefg"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("101abcdefg"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    expectedArray = Bytes.toBytes("101");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);
  }

  @Test
  public void testDBEShipped() throws IOException {
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      DataBlockEncoder encoder = encoding.getEncoder();
      if (encoder == null) {
        continue;
      }
      Path f = new Path(ROOT_DIR, testName.getMethodName() + "_" + encoding);
      HFileContext context = new HFileContextBuilder()
          .withIncludesTags(false)
          .withDataBlockEncoding(encoding).build();
      HFileWriterImpl writer = (HFileWriterImpl) HFile.getWriterFactory(conf, cacheConf)
          .withPath(fs, f).withFileContext(context).create();

      KeyValue kv = new KeyValue(Bytes.toBytes("testkey1"), Bytes.toBytes("family"),
          Bytes.toBytes("qual"), Bytes.toBytes("testvalue"));
      KeyValue kv2 = new KeyValue(Bytes.toBytes("testkey2"), Bytes.toBytes("family"),
        Bytes.toBytes("qual"), Bytes.toBytes("testvalue"));
      KeyValue kv3 = new KeyValue(Bytes.toBytes("testkey3"), Bytes.toBytes("family"),
        Bytes.toBytes("qual"), Bytes.toBytes("testvalue"));

      ByteBuffer buffer = ByteBuffer.wrap(kv.getBuffer());
      ByteBuffer buffer2 = ByteBuffer.wrap(kv2.getBuffer());
      ByteBuffer buffer3 = ByteBuffer.wrap(kv3.getBuffer());

      writer.append(new ByteBufferKeyValue(buffer, 0, buffer.remaining()));
      writer.beforeShipped();

      // pollute first cell's backing ByteBuffer
      ByteBufferUtils.copyFromBufferToBuffer(buffer3, buffer);

      // write another cell, if DBE not Shipped, test will fail
      writer.append(new ByteBufferKeyValue(buffer2, 0, buffer2.remaining()));
      writer.close();
    }
  }

  /**
   * Test case for CombinedBlockCache with TinyLfu as L1 cache
   */
  @Test
  public void testReaderWithTinyLfuCombinedBlockCache() throws Exception {
    testReaderCombinedCache("TinyLfu");
  }

  /**
   * Test case for CombinedBlockCache with AdaptiveLRU as L1 cache
   */
  @Test
  public void testReaderWithAdaptiveLruCombinedBlockCache() throws Exception {
    testReaderCombinedCache("AdaptiveLRU");
  }

  /**
   * Test case for CombinedBlockCache with AdaptiveLRU as L1 cache
   */
  @Test
  public void testReaderWithLruCombinedBlockCache() throws Exception {
    testReaderCombinedCache("LRU");
  }

  private void testReaderCombinedCache(final String l1CachePolicy) throws Exception {
    int bufCount = 1024;
    int blockSize = 64 * 1024;
    ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
    fillByteBuffAllocator(alloc, bufCount);
    Path storeFilePath = writeStoreFile();
    // Open the file reader with CombinedBlockCache
    BlockCache combined = initCombinedBlockCache(l1CachePolicy);
    conf.setBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, true);
    CacheConfig cacheConfig = new CacheConfig(conf, null, combined, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConfig, true, conf);
    long offset = 0;
    Cacheable cachedBlock = null;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
      HFileBlock block = reader.readBlock(offset, -1, true, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      // Read the cached block.
      cachedBlock = combined.getBlock(key, false, false, true);
      try {
        Assert.assertNotNull(cachedBlock);
        Assert.assertTrue(cachedBlock instanceof HFileBlock);
        HFileBlock hfb = (HFileBlock) cachedBlock;
        // Data block will be cached in BucketCache, so it should be an off-heap block.
        if (hfb.getBlockType().isData()) {
          Assert.assertTrue(hfb.isSharedMem());
        } else if (!l1CachePolicy.equals("TinyLfu")) {
          Assert.assertFalse(hfb.isSharedMem());
        }
      } finally {
        cachedBlock.release();
      }
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
    combined.shutdown();
    if (cachedBlock != null) {
      Assert.assertEquals(0, cachedBlock.refCnt());
    }
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
  }

}
