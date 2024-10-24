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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
// TODO: needs to add more corner case scenarios like single cell fits the range,
// no results fits the range etc..
public class TestRangerStoreFileReader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRangerStoreFileReader.class);

  private static HBaseTestingUtil TEST_UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testRangeScanAndReseek() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    String root_dir = TEST_UTIL.getDataTestDir().toString();
    Path parentPath = new Path(new Path(root_dir, "parent"), "CF");
    fs.mkdirs(parentPath);
    Path range1Path = new Path(new Path(root_dir, "range1"), "CF");
    Path range2Path = new Path(new Path(root_dir, "range2"), "CF");
    Path filePath = StoreFileWriter.getUniqueFile(fs, parentPath);

    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(1024).build();
    HFile.Writer w =
      HFile.getWriterFactory(conf, cacheConf).withPath(fs, filePath).withFileContext(meta).create();

    // write some things.
    List<KeyValue> items = genSomeKeys();
    for (KeyValue kv : items) {
      w.append(kv);
    }
    w.close();

    HFile.Reader r = HFile.createReader(fs, filePath, cacheConf, true, conf);
    Cell firstKV = r.getFirstKey().get();
    byte[] firstkey = CellUtil.cloneRow(firstKV);
    Cell midKV = r.midKey().get();
    byte[] midkey = CellUtil.cloneRow(midKV);
    Cell endKV = r.getLastKey().get();
    byte[] endKey = CellUtil.cloneRow(endKV);

    Path rangeFile1 = new Path(range1Path, filePath.getName() + ".parent");
    Path rangeFile2 = new Path(range2Path, filePath.getName() + ".parent");

    RangeReference range1 = new RangeReference(firstkey, midkey);
    range1.write(fs, rangeFile1);
    doTestOfScanAndReseek(rangeFile1, fs, range1, cacheConf);

    RangeReference range2 = new RangeReference(midkey, endKey);
    range2.write(fs, rangeFile2);
    doTestOfScanAndReseek(rangeFile2, fs, range2, cacheConf);

    r.close();
  }

  private void doTestOfScanAndReseek(Path p, FileSystem fs, RangeReference rangeReference,
    CacheConfig cacheConf) throws IOException {
    Path referencePath = StoreFileInfo.getReferredToFile(p);
    FSDataInputStreamWrapper in = new FSDataInputStreamWrapper(fs, referencePath, false, 0);
    FileStatus status = fs.getFileStatus(referencePath);
    long length = status.getLen();
    ReaderContextBuilder contextBuilder =
      new ReaderContextBuilder().withInputStreamWrapper(in).withFileSize(length)
        .withReaderType(ReaderContext.ReaderType.PREAD).withFileSystem(fs).withFilePath(p);
    ReaderContext context = contextBuilder.build();
    StoreFileInfo storeFileInfo = new StoreFileInfo(TEST_UTIL.getConfiguration(), fs, p, true);
    storeFileInfo.initHFileInfo(context);
    // TODO: this should be created through storeFileInfo.createReader itself
    // after defining proper reference link for range
    RangeStoreFileReader rangeStoreFileReader =
      new RangeStoreFileReader(context, storeFileInfo.getHFileInfo(), cacheConf, rangeReference,
        storeFileInfo, TEST_UTIL.getConfiguration());
    storeFileInfo.getHFileInfo().initMetaAndIndex(rangeStoreFileReader.getHFileReader());
    rangeStoreFileReader.loadFileInfo();
    try (HFileScanner scanner = rangeStoreFileReader.getScanner(false, false, false)) {

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
    }

    rangeStoreFileReader.close(true);
  }

  // Tests the scanner on an HFile that is backed by HalfStoreFiles
  @Test
  public void testRangeScanner() throws IOException {
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
    ExtendedCell firstKV = r.getFirstKey().get();
    byte[] firstKey = CellUtil.cloneRow(firstKV);
    ExtendedCell midKV = r.midKey().get();
    byte[] midkey = CellUtil.cloneRow(midKV);
    ExtendedCell lastKV = r.getLastKey().get();
    byte[] lastKey = CellUtil.cloneRow(lastKV);

    RangeReference range1 = new RangeReference(firstKey, midkey);
    RangeReference range2 = new RangeReference(midkey, lastKey);

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

    // Seek on the midkey, should be in top, not in bottom
    Cell foundKeyValue = doTestOfSeekBefore(p, fs, range1, midKV, cacheConf);
    assertEquals(beforeMidKey, foundKeyValue);

    // Seek tot the last thing should be the penultimate on the top, the one before the midkey on
    // the bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, range2, items.get(items.size() - 1), cacheConf);
    assertEquals(items.get(items.size() - 2), foundKeyValue);

    foundKeyValue = doTestOfSeekBefore(p, fs, range1, items.get(items.size() - 1), cacheConf);
    assertEquals(beforeMidKey, foundKeyValue);

    // Try and seek before something that is in the bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, range2, items.get(0), cacheConf);
    assertNull(foundKeyValue);

    // Try and seek before the first thing.
    foundKeyValue = doTestOfSeekBefore(p, fs, range1, items.get(0), cacheConf);
    assertNull(foundKeyValue);

    // Try and seek before the second thing in the top and bottom.
    foundKeyValue = doTestOfSeekBefore(p, fs, range2, items.get(1), cacheConf);
    assertNull(foundKeyValue);

    foundKeyValue = doTestOfSeekBefore(p, fs, range1, items.get(1), cacheConf);
    assertEquals(items.get(0), foundKeyValue);

    // Try to seek before the splitKey in the top file
    foundKeyValue = doTestOfSeekBefore(p, fs, range2, midKV, cacheConf);
    assertNull(foundKeyValue);
  }

  private Cell doTestOfSeekBefore(Path p, FileSystem fs, RangeReference rangeReference,
    ExtendedCell seekBefore, CacheConfig cacheConfig) throws IOException {
    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, p).build();
    StoreFileInfo storeFileInfo =
      new StoreFileInfo(TEST_UTIL.getConfiguration(), fs, fs.getFileStatus(p), rangeReference);
    storeFileInfo.initHFileInfo(context);
    RangeStoreFileReader rangeStoreFileReader =
      new RangeStoreFileReader(context, storeFileInfo.getHFileInfo(), cacheConfig, rangeReference,
        storeFileInfo, TEST_UTIL.getConfiguration());
    storeFileInfo.getHFileInfo().initMetaAndIndex(rangeStoreFileReader.getHFileReader());
    rangeStoreFileReader.loadFileInfo();
    try (HFileScanner scanner = rangeStoreFileReader.getScanner(false, false, false)) {
      scanner.seekBefore(seekBefore);
      if (scanner.getCell() != null) {
        return KeyValueUtil.copyToNewKeyValue(scanner.getCell());
      } else {
        return null;
      }
    }
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
