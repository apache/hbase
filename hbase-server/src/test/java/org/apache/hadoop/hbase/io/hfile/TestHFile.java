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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.junit.experimental.categories.Category;

/**
 * test hfile features.
 * <p>
 * Copied from
 * <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
 * Remove after tfile is committed and use the tfile version of this class
 * instead.</p>
 */
@Category({IOTests.class, SmallTests.class})
public class TestHFile extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHFile.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
    TEST_UTIL.getDataTestDir("TestHFile").toString();
  private final int minBlockSize = 512;
  private static String localFormatter = "%010d";
  private static CacheConfig cacheConf = null;
  private Map<String, Long> startingMetrics;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }


  /**
   * Test empty HFile.
   * Test all features work reasonably when hfile is empty of entries.
   * @throws IOException
   */
  public void testEmptyHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
    Writer w =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, f).withFileContext(context).create();
    w.close();
    Reader r = HFile.createReader(fs, f, cacheConf, conf);
    r.loadFileInfo();
    assertNull(r.getFirstKey());
    assertNull(r.getLastKey());
  }

  /**
   * Create 0-length hfile and show that it fails
   */
  public void testCorrupt0LengthHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    FSDataOutputStream fsos = fs.create(f);
    fsos.close();

    try {
      Reader r = HFile.createReader(fs, f, cacheConf, conf);
    } catch (CorruptHFileException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
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
  public void testCorruptTruncatedHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    HFileContext  context = new HFileContextBuilder().build();
    Writer w = HFile.getWriterFactory(conf, cacheConf).withPath(this.fs, f)
        .withFileContext(context).create();
    writeSomeRecords(w, 0, 100, false);
    w.close();

    Path trunc = new Path(f.getParent(), "trucated");
    truncateFile(fs, w.getPath(), trunc);

    try {
      Reader r = HFile.createReader(fs, trunc, cacheConf, conf);
    } catch (CorruptHFileException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
  }

  // write some records into the tfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n, boolean useTags)
      throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, Integer.valueOf(i));
      if (useTags) {
        Tag t = new Tag((byte) 1, "myTag1");
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
      ByteBuffer key = scanner.getKey();
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
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path ncTFile = new Path(ROOT_DIR, "basic.hfile." + codec.toString() + useTags);
    FSDataOutputStream fout = createFSOutput(ncTFile);
    HFileContext meta = new HFileContextBuilder()
                        .withBlockSize(minBlockSize)
                        .withCompression(AbstractHFileWriter.compressionByName(codec))
                        .build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .withComparator(new KeyValue.KVComparator())
        .create();
    LOG.info(writer);
    writeRecords(writer, useTags);
    fout.close();
    FSDataInputStream fin = fs.open(ncTFile);
    Reader reader = HFile.createReaderFromStream(ncTFile, fs.open(ncTFile),
      fs.getFileStatus(ncTFile).getLen(), cacheConf, conf);
    System.out.println(cacheConf.toString());
    // Load up the index.
    reader.loadFileInfo();
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, false);
    // Align scanner at start of the file.
    scanner.seekTo();
    readAllRecords(scanner);
    int seekTo = scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(50)));
    System.out.println(seekTo);
    assertTrue("location lookup failed",
        scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(50))) == 0);
    // read the key and see if it matches
    ByteBuffer readKey = scanner.getKey();
    assertTrue("seeked key does not match", Arrays.equals(getSomeKey(50),
      Bytes.toBytes(readKey)));

    scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val1 = scanner.getValue();
    scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val2 = scanner.getValue();
    assertTrue(Arrays.equals(Bytes.toBytes(val1), Bytes.toBytes(val2)));

    reader.close();
    fin.close();
    fs.delete(ncTFile, true);
  }

  public void testTFileFeatures() throws IOException {
    testTFilefeaturesInternals(false);
    testTFilefeaturesInternals(true);
  }

  protected void testTFilefeaturesInternals(boolean useTags) throws IOException {
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
      ByteBuffer actual = reader.getMetaBlock("HFileMeta" + i, false);
      ByteBuffer expected = 
        ByteBuffer.wrap(("something to test" + i).getBytes());
      assertEquals("failed to match metadata",
        Bytes.toStringBinary(expected), Bytes.toStringBinary(actual));
    }
  }

  private void someReadingWithMetaBlock(Reader reader) throws IOException {
    readNumMetablocks(reader, 10);
  }

  private void metablocks(final String compress) throws Exception {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path mFile = new Path(ROOT_DIR, "meta.hfile");
    FSDataOutputStream fout = createFSOutput(mFile);
    HFileContext meta = new HFileContextBuilder()
                        .withCompression(AbstractHFileWriter.compressionByName(compress))
                        .withBlockSize(minBlockSize).build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .create();
    someTestingWithMetaBlock(writer);
    writer.close();
    fout.close();
    FSDataInputStream fin = fs.open(mFile);
    Reader reader = HFile.createReaderFromStream(mFile, fs.open(mFile),
        this.fs.getFileStatus(mFile).getLen(), cacheConf, conf);
    reader.loadFileInfo();
    // No data -- this should return false.
    assertFalse(reader.getScanner(false, false).seekTo());
    someReadingWithMetaBlock(reader);
    fs.delete(mFile, true);
    reader.close();
    fin.close();
  }

  // test meta blocks for tfiles
  public void testMetaBlocks() throws Exception {
    metablocks("none");
    metablocks("gz");
  }

  public void testNullMetaBlocks() throws Exception {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    for (Compression.Algorithm compressAlgo : 
        HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
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
      Reader reader = HFile.createReader(fs, mFile, cacheConf, conf);
      reader.loadFileInfo();
      assertNull(reader.getMetaBlock("non-existant", false));
    }
  }

  /**
   * Make sure the ordinals for our compression algorithms do not change on us.
   */
  public void testCompressionOrdinance() {
    assertTrue(Compression.Algorithm.LZO.ordinal() == 0);
    assertTrue(Compression.Algorithm.GZ.ordinal() == 1);
    assertTrue(Compression.Algorithm.NONE.ordinal() == 2);
    assertTrue(Compression.Algorithm.SNAPPY.ordinal() == 3);
    assertTrue(Compression.Algorithm.LZ4.ordinal() == 4);
  }

}

