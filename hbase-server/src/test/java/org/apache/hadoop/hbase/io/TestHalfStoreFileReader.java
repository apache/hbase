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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestHalfStoreFileReader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHalfStoreFileReader.class);

  private static HBaseTestingUtility TEST_UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test the scanner and reseek of a half hfile scanner. The scanner API demands that seekTo and
   * reseekTo() only return < 0 if the key lies before the start of the file (with no position on
   * the scanner). Returning 0 if perfect match (rare), and return > 1 if we got an imperfect match.
   * The latter case being the most common, we should generally be returning 1, and if we do, there
   * may or may not be a 'next' in the scanner/file. A bug in the half file scanner was returning -1
   * at the end of the bottom half, and that was causing the infrastructure above to go null causing
   * NPEs and other problems. This test reproduces that failure, and also tests both the bottom and
   * top of the file while we are at it. n
   */
  @Test
  public void testHalfScanAndReseek() throws IOException {
    String root_dir = TEST_UTIL.getDataTestDir().toString();
    Path p = new Path(root_dir, "test");

    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(1024).build();
    HFile.Writer w =
      HFile.getWriterFactory(conf, cacheConf).withPath(fs, p).withFileContext(meta).create();

    // write some things.
    List<KeyValue> items = genSomeKeys();
    for (KeyValue kv : items) {
      w.append(kv);
    }
    w.close();

    HFile.Reader r = HFile.createReader(fs, p, cacheConf, true, conf);
    Cell midKV = r.midKey().get();
    byte[] midkey = CellUtil.cloneRow(midKV);

    // System.out.println("midkey: " + midKV + " or: " + Bytes.toStringBinary(midkey));

    Reference bottom = new Reference(midkey, Reference.Range.bottom);
    doTestOfScanAndReseek(p, fs, bottom, cacheConf);

    Reference top = new Reference(midkey, Reference.Range.top);
    doTestOfScanAndReseek(p, fs, top, cacheConf);

    r.close();
  }

  private void doTestOfScanAndReseek(Path p, FileSystem fs, Reference bottom, CacheConfig cacheConf)
    throws IOException {
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, p).build();
    HFileInfo fileInfo = new HFileInfo(context, TEST_UTIL.getConfiguration());
    final HalfStoreFileReader halfreader = new HalfStoreFileReader(context, fileInfo, cacheConf,
      bottom, new AtomicInteger(0), TEST_UTIL.getConfiguration());
    fileInfo.initMetaAndIndex(halfreader.getHFileReader());
    halfreader.loadFileInfo();
    final HFileScanner scanner = halfreader.getScanner(false, false);

    scanner.seekTo();
    Cell curr;
    do {
      curr = scanner.getCell();
      KeyValue reseekKv = getLastOnCol(curr);
      int ret = scanner.reseekTo(reseekKv);
      assertTrue("reseek to returned: " + ret, ret > 0);
      // System.out.println(curr + ": " + ret);
    } while (scanner.next());

    int ret = scanner.reseekTo(getLastOnCol(curr));
    // System.out.println("Last reseek: " + ret);
    assertTrue(ret > 0);

    halfreader.close(true);
  }

  // Tests the scanner on an HFile that is backed by HalfStoreFiles
  @Test
  public void testHalfScanner() throws IOException {
    String root_dir = TEST_UTIL.getDataTestDir().toString();
    Path p = new Path(root_dir, "test");
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(1024).build();
    HFile.Writer w =
      HFile.getWriterFactory(conf, cacheConf).withPath(fs, p).withFileContext(meta).create();

    // write some things.
    List<KeyValue> items = genSomeKeys();
    for (KeyValue kv : items) {
      w.append(kv);
    }
    w.close();

    HFile.Reader r = HFile.createReader(fs, p, cacheConf, true, conf);
    Cell midKV = r.midKey().get();
    byte[] midkey = CellUtil.cloneRow(midKV);

    Reference bottom = new Reference(midkey, Reference.Range.bottom);
    Reference top = new Reference(midkey, Reference.Range.top);

    // Ugly code to get the item before the midkey
    KeyValue beforeMidKey = null;
    for (KeyValue item : items) {
      if (CellComparatorImpl.COMPARATOR.compare(item, midKV) >= 0) {
        break;
      }
      beforeMidKey = item;
    }
    System.out.println("midkey: " + midKV + " or: " + Bytes.toStringBinary(midkey));
    System.out.println("beforeMidKey: " + beforeMidKey);

    // Seek on the splitKey, should be in top, not in bottom
    Cell foundKeyValue = doTestOfSeekBefore(p, fs, bottom, midKV, cacheConf);
    assertEquals(beforeMidKey, foundKeyValue);

    // Seek tot the last thing should be the penultimate on the top, the one before the midkey on
    // the bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, top, items.get(items.size() - 1), cacheConf);
    assertEquals(items.get(items.size() - 2), foundKeyValue);

    foundKeyValue = doTestOfSeekBefore(p, fs, bottom, items.get(items.size() - 1), cacheConf);
    assertEquals(beforeMidKey, foundKeyValue);

    // Try and seek before something that is in the bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, top, items.get(0), cacheConf);
    assertNull(foundKeyValue);

    // Try and seek before the first thing.
    foundKeyValue = doTestOfSeekBefore(p, fs, bottom, items.get(0), cacheConf);
    assertNull(foundKeyValue);

    // Try and seek before the second thing in the top and bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, top, items.get(1), cacheConf);
    assertNull(foundKeyValue);

    foundKeyValue = doTestOfSeekBefore(p, fs, bottom, items.get(1), cacheConf);
    assertEquals(items.get(0), foundKeyValue);

    // Try to seek before the splitKey in the top file
    foundKeyValue = doTestOfSeekBefore(p, fs, top, midKV, cacheConf);
    assertNull(foundKeyValue);
  }

  private Cell doTestOfSeekBefore(Path p, FileSystem fs, Reference bottom, Cell seekBefore,
    CacheConfig cacheConfig) throws IOException {
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, p).build();
    HFileInfo fileInfo = new HFileInfo(context, TEST_UTIL.getConfiguration());
    final HalfStoreFileReader halfreader = new HalfStoreFileReader(context, fileInfo, cacheConfig,
      bottom, new AtomicInteger(0), TEST_UTIL.getConfiguration());
    fileInfo.initMetaAndIndex(halfreader.getHFileReader());
    halfreader.loadFileInfo();
    final HFileScanner scanner = halfreader.getScanner(false, false);
    scanner.seekBefore(seekBefore);
    return scanner.getCell();
  }

  private KeyValue getLastOnCol(Cell curr) {
    return KeyValueUtil.createLastOnRow(curr.getRowArray(), curr.getRowOffset(),
      curr.getRowLength(), curr.getFamilyArray(), curr.getFamilyOffset(), curr.getFamilyLength(),
      curr.getQualifierArray(), curr.getQualifierOffset(), curr.getQualifierLength());
  }

  static final int SIZE = 1000;

  static byte[] _b(String s) {
    return Bytes.toBytes(s);
  }

  List<KeyValue> genSomeKeys() {
    List<KeyValue> ret = new ArrayList<>(SIZE);
    for (int i = 0; i < SIZE; i++) {
      KeyValue kv =
        new KeyValue(_b(String.format("row_%04d", i)), _b("family"), _b("qualifier"), 1000, // timestamp
          _b("value"));
      ret.add(kv);
    }
    return ret;
  }
}
