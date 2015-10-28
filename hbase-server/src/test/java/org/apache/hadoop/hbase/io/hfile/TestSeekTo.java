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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.OffheapKeyValue;
import org.apache.hadoop.hbase.ShareableMemory;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test {@link HFileScanner#seekTo(byte[])} and its variants.
 */
@Category({IOTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestSeekTo {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final DataBlockEncoding encoding;
  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      paramList.add(new Object[] { encoding });
    }
    return paramList;
  }
  static boolean switchKVs = false;

  public TestSeekTo(DataBlockEncoding encoding) {
    this.encoding = encoding;
  }

  @Before
  public void setUp() {
    //reset
    switchKVs = false;
  }

  static KeyValue toKV(String row, TagUsage tagUsage) {
    if (tagUsage == TagUsage.NO_TAG) {
      return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes.toBytes("qualifier"),
          Bytes.toBytes("value"));
    } else if (tagUsage == TagUsage.ONLY_TAG) {
      Tag t = new Tag((byte) 1, "myTag1");
      Tag[] tags = new Tag[1];
      tags[0] = t;
      return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes.toBytes("qualifier"),
          HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"), tags);
    } else {
      if (!switchKVs) {
        switchKVs = true;
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
            Bytes.toBytes("qualifier"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"));
      } else {
        switchKVs = false;
        Tag t = new Tag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
            Bytes.toBytes("qualifier"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"), tags);
      }
    }
  }
  static String toRowStr(Cell kv) {
    KeyValue c = KeyValueUtil.ensureKeyValue(kv);
    return Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
  }

  Path makeNewFile(TagUsage tagUsage) throws IOException {
    Path ncTFile = new Path(TEST_UTIL.getDataTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
    int blocksize = toKV("a", tagUsage).getLength() * 3;
    HFileContext context = new HFileContextBuilder().withBlockSize(blocksize)
        .withDataBlockEncoding(encoding)
        .withIncludesTags(true).build();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Writer writer = HFile.getWriterFactoryNoCache(conf).withOutputStream(fout)
        .withFileContext(context)
        .withComparator(CellComparator.COMPARATOR).create();
    // 4 bytes * 3 * 2 for each key/value +
    // 3 for keys, 15 for values = 42 (woot)
    writer.append(toKV("c", tagUsage));
    writer.append(toKV("e", tagUsage));
    writer.append(toKV("g", tagUsage));
    // block transition
    writer.append(toKV("i", tagUsage));
    writer.append(toKV("k", tagUsage));
    writer.close();
    fout.close();
    return ncTFile;
  }

  @Test
  public void testSeekBefore() throws Exception {
    testSeekBeforeInternals(TagUsage.NO_TAG);
    testSeekBeforeInternals(TagUsage.ONLY_TAG);
    testSeekBeforeInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekBeforeInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);
    assertFalse(scanner.seekBefore(toKV("a", tagUsage)));

    assertFalse(scanner.seekBefore(toKV("c", tagUsage)));

    assertTrue(scanner.seekBefore(toKV("d", tagUsage)));
    assertEquals("c", toRowStr(scanner.getCell()));

    assertTrue(scanner.seekBefore(toKV("e", tagUsage)));
    assertEquals("c", toRowStr(scanner.getCell()));

    assertTrue(scanner.seekBefore(toKV("f", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));

    assertTrue(scanner.seekBefore(toKV("g", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    assertTrue(scanner.seekBefore(toKV("h", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));
    assertTrue(scanner.seekBefore(toKV("i", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));
    assertTrue(scanner.seekBefore(toKV("j", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));
    Cell cell = scanner.getCell();
    if (tagUsage != TagUsage.NO_TAG && cell.getTagsLength() > 0) {
      Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      while (tagsIterator.hasNext()) {
        Tag next = tagsIterator.next();
        assertEquals("myTag1", Bytes.toString(next.getValue()));
      }
    }
    assertTrue(scanner.seekBefore(toKV("k", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));
    assertTrue(scanner.seekBefore(toKV("l", tagUsage)));
    assertEquals("k", toRowStr(scanner.getCell()));

    reader.close();
    deleteTestDir(fs);
  }

  protected void deleteTestDir(FileSystem fs) throws IOException {
    Path dataTestDir = TEST_UTIL.getDataTestDir();
    if(fs.exists(dataTestDir)) {
      fs.delete(dataTestDir, true);
    }
  }

  @Test
  public void testSeekBeforeWithReSeekTo() throws Exception {
    testSeekBeforeWithReSeekToInternals(TagUsage.NO_TAG);
    testSeekBeforeWithReSeekToInternals(TagUsage.ONLY_TAG);
    testSeekBeforeWithReSeekToInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekBeforeWithReSeekToInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);
    assertFalse(scanner.seekBefore(toKV("a", tagUsage)));
    assertFalse(scanner.seekBefore(toKV("b", tagUsage)));
    assertFalse(scanner.seekBefore(toKV("c", tagUsage)));

    // seekBefore d, so the scanner points to c
    assertTrue(scanner.seekBefore(toKV("d", tagUsage)));
    assertFalse(scanner.getCell() instanceof ShareableMemory);
    assertFalse(scanner.getCell() instanceof OffheapKeyValue);
    assertEquals("c", toRowStr(scanner.getCell()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("c", tagUsage)));
    assertEquals("c", toRowStr(scanner.getCell()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore e, so the scanner points to c
    assertTrue(scanner.seekBefore(toKV("e", tagUsage)));
    assertEquals("c", toRowStr(scanner.getCell()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore f, so the scanner points to e
    assertTrue(scanner.seekBefore(toKV("f", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore g, so the scanner points to e
    assertTrue(scanner.seekBefore(toKV("g", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    // reseekTo e and g again
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getCell()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore h, so the scanner points to g
    assertTrue(scanner.seekBefore(toKV("h", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));
    // reseekTo g
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore i, so the scanner points to g
    assertTrue(scanner.seekBefore(toKV("i", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));
    // reseekTo g
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getCell()));

    // seekBefore j, so the scanner points to i
    assertTrue(scanner.seekBefore(toKV("j", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));
    // reseekTo i
    assertEquals(0, scanner.reseekTo(toKV("i", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));

    // seekBefore k, so the scanner points to i
    assertTrue(scanner.seekBefore(toKV("k", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));
    // reseekTo i and k
    assertEquals(0, scanner.reseekTo(toKV("i", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));
    assertEquals(0, scanner.reseekTo(toKV("k", tagUsage)));
    assertEquals("k", toRowStr(scanner.getCell()));

    // seekBefore l, so the scanner points to k
    assertTrue(scanner.seekBefore(toKV("l", tagUsage)));
    assertEquals("k", toRowStr(scanner.getCell()));
    // reseekTo k
    assertEquals(0, scanner.reseekTo(toKV("k", tagUsage)));
    assertEquals("k", toRowStr(scanner.getCell()));
    deleteTestDir(fs);
  }

  @Test
  public void testSeekTo() throws Exception {
    testSeekToInternals(TagUsage.NO_TAG);
    testSeekToInternals(TagUsage.ONLY_TAG);
    testSeekToInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekToInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    assertEquals(2, reader.getDataBlockIndexReader().getRootBlockCount());
    HFileScanner scanner = reader.getScanner(false, true);
    // lies before the start of the file.
    assertEquals(-1, scanner.seekTo(toKV("a", tagUsage)));

    assertEquals(1, scanner.seekTo(toKV("d", tagUsage)));
    assertEquals("c", toRowStr(scanner.getCell()));

    // Across a block boundary now.
    // 'h' does not exist so we will get a '1' back for not found.
    assertEquals(0, scanner.seekTo(toKV("i", tagUsage)));
    assertEquals("i", toRowStr(scanner.getCell()));

    assertEquals(1, scanner.seekTo(toKV("l", tagUsage)));
    if (encoding == DataBlockEncoding.PREFIX_TREE) {
      // TODO : Fix this
      assertEquals(null, scanner.getCell());
    } else {
      assertEquals("k", toRowStr(scanner.getCell()));
    }

    reader.close();
    deleteTestDir(fs);
  }

  @Test
  public void testBlockContainingKey() throws Exception {
    testBlockContainingKeyInternals(TagUsage.NO_TAG);
    testBlockContainingKeyInternals(TagUsage.ONLY_TAG);
    testBlockContainingKeyInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testBlockContainingKeyInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileBlockIndex.BlockIndexReader blockIndexReader =
      reader.getDataBlockIndexReader();
    System.out.println(blockIndexReader.toString());
    // falls before the start of the file.
    assertEquals(-1, blockIndexReader.rootBlockContainingKey(
        toKV("a", tagUsage)));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("c", tagUsage)));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("d", tagUsage)));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("e", tagUsage)));
    assertEquals(0, blockIndexReader.rootBlockContainingKey(
        toKV("g", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(toKV("h", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("i", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("j", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("k", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("l", tagUsage)));
    reader.close();
    deleteTestDir(fs);
  }
}