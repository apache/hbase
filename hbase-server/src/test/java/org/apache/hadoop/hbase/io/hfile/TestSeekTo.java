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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

/**
 * Test {@link HFileScanner#seekTo(byte[])} and its variants.
 */
@Category({IOTests.class, SmallTests.class})
public class TestSeekTo extends HBaseTestCase {

  static boolean switchKVs = false;

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
            Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
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
    return Bytes.toString(KeyValueUtil.ensureKeyValue(kv).getRow());
  }

  Path makeNewFile(TagUsage tagUsage) throws IOException {
    Path ncTFile = new Path(this.testDir, "basic.hfile");
    if (tagUsage != TagUsage.NO_TAG) {
      conf.setInt("hfile.format.version", 3);
    } else {
      conf.setInt("hfile.format.version", 2);
    }
    FSDataOutputStream fout = this.fs.create(ncTFile);
    int blocksize = toKV("a", tagUsage).getLength() * 3;
    HFileContext context = new HFileContextBuilder().withBlockSize(blocksize)
        .withIncludesTags(true).build();
    HFile.Writer writer = HFile.getWriterFactoryNoCache(conf).withOutputStream(fout)
        .withFileContext(context)
        .withComparator(KeyValue.COMPARATOR).create();
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

  public void testSeekBefore() throws Exception {
    testSeekBeforeInternals(TagUsage.NO_TAG);
    testSeekBeforeInternals(TagUsage.ONLY_TAG);
    testSeekBeforeInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekBeforeInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);
    assertEquals(false, scanner.seekBefore(toKV("a", tagUsage)));

    assertEquals(false, scanner.seekBefore(toKV("c", tagUsage)));

    assertEquals(true, scanner.seekBefore(toKV("d", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("e", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("f", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("g", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));

    assertEquals(true, scanner.seekBefore(toKV("h", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("i", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("j", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("k", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    assertEquals(true, scanner.seekBefore(toKV("l", tagUsage)));
    assertEquals("k", toRowStr(scanner.getKeyValue()));

    reader.close();
  }

  public void testSeekBeforeWithReSeekTo() throws Exception {
    testSeekBeforeWithReSeekToInternals(TagUsage.NO_TAG);
    testSeekBeforeWithReSeekToInternals(TagUsage.ONLY_TAG);
    testSeekBeforeWithReSeekToInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekBeforeWithReSeekToInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, true);
    assertEquals(false, scanner.seekBefore(toKV("a", tagUsage)));
    assertEquals(false, scanner.seekBefore(toKV("b", tagUsage)));
    assertEquals(false, scanner.seekBefore(toKV("c", tagUsage)));

    // seekBefore d, so the scanner points to c
    assertEquals(true, scanner.seekBefore(toKV("d", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("c", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore e, so the scanner points to c
    assertEquals(true, scanner.seekBefore(toKV("e", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore f, so the scanner points to e
    assertEquals(true, scanner.seekBefore(toKV("f", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));
    // reseekTo e and g
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore g, so the scanner points to e
    assertEquals(true, scanner.seekBefore(toKV("g", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));
    // reseekTo e and g again
    assertEquals(0, scanner.reseekTo(toKV("e", tagUsage)));
    assertEquals("e", toRowStr(scanner.getKeyValue()));
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore h, so the scanner points to g
    assertEquals(true, scanner.seekBefore(toKV("h", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    // reseekTo g
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore i, so the scanner points to g
    assertEquals(true, scanner.seekBefore(toKV("i", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));
    // reseekTo g
    assertEquals(0, scanner.reseekTo(toKV("g", tagUsage)));
    assertEquals("g", toRowStr(scanner.getKeyValue()));

    // seekBefore j, so the scanner points to i
    assertEquals(true, scanner.seekBefore(toKV("j", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    // reseekTo i
    assertEquals(0, scanner.reseekTo(toKV("i", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));

    // seekBefore k, so the scanner points to i
    assertEquals(true, scanner.seekBefore(toKV("k", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    // reseekTo i and k
    assertEquals(0, scanner.reseekTo(toKV("i", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));
    assertEquals(0, scanner.reseekTo(toKV("k", tagUsage)));
    assertEquals("k", toRowStr(scanner.getKeyValue()));

    // seekBefore l, so the scanner points to k
    assertEquals(true, scanner.seekBefore(toKV("l", tagUsage)));
    assertEquals("k", toRowStr(scanner.getKeyValue()));
    // reseekTo k
    assertEquals(0, scanner.reseekTo(toKV("k", tagUsage)));
    assertEquals("k", toRowStr(scanner.getKeyValue()));
  }

  public void testSeekTo() throws Exception {
    testSeekToInternals(TagUsage.NO_TAG);
    testSeekToInternals(TagUsage.ONLY_TAG);
    testSeekToInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testSeekToInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    assertEquals(2, reader.getDataBlockIndexReader().getRootBlockCount());
    HFileScanner scanner = reader.getScanner(false, true);
    // lies before the start of the file.
    assertEquals(-1, scanner.seekTo(toKV("a", tagUsage)));

    assertEquals(1, scanner.seekTo(toKV("d", tagUsage)));
    assertEquals("c", toRowStr(scanner.getKeyValue()));

    // Across a block boundary now.
    // h goes to the next block
    assertEquals(-2, scanner.seekTo(toKV("h", tagUsage)));
    assertEquals("i", toRowStr(scanner.getKeyValue()));

    assertEquals(1, scanner.seekTo(toKV("l", tagUsage)));
    assertEquals("k", toRowStr(scanner.getKeyValue()));

    reader.close();
  }
  public void testBlockContainingKey() throws Exception {
    testBlockContainingKeyInternals(TagUsage.NO_TAG);
    testBlockContainingKeyInternals(TagUsage.ONLY_TAG);
    testBlockContainingKeyInternals(TagUsage.PARTIAL_TAG);
  }

  protected void testBlockContainingKeyInternals(TagUsage tagUsage) throws IOException {
    Path p = makeNewFile(tagUsage);
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileBlockIndex.BlockIndexReader blockIndexReader = 
      reader.getDataBlockIndexReader();
    System.out.println(blockIndexReader.toString());
    int klen = toKV("a", tagUsage).getKey().length;
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
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("h", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("i", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("j", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("k", tagUsage)));
    assertEquals(1, blockIndexReader.rootBlockContainingKey(
        toKV("l", tagUsage)));
    reader.close();
  }


}

