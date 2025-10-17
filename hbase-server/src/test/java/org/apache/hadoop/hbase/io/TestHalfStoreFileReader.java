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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
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
   * top of the file while we are at it.
   */
  @Test
  public void testHalfScanAndReseek() throws IOException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    String root_dir = TEST_UTIL.getDataTestDir().toString();
    Path parentPath = new Path(new Path(root_dir, "parent"), "CF");
    fs.mkdirs(parentPath);
    String tableName = Paths.get(root_dir).getFileName().toString();
    RegionInfo splitAHri = RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build();
    Thread.currentThread().sleep(1000);
    RegionInfo splitBHri = RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build();
    Path splitAPath = new Path(new Path(root_dir, splitAHri.getRegionNameAsString()), "CF");
    Path splitBPath = new Path(new Path(root_dir, splitBHri.getRegionNameAsString()), "CF");
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
    Cell midKV = r.midKey().get();
    byte[] midkey = CellUtil.cloneRow(midKV);

    Path splitFileA = new Path(splitAPath, filePath.getName() + ".parent");
    Path splitFileB = new Path(splitBPath, filePath.getName() + ".parent");

    HRegionFileSystem splitAregionFS =
      HRegionFileSystem.create(conf, fs, new Path(root_dir), splitAHri);
    StoreContext splitAStoreContext =
      StoreContext.getBuilder().withColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder.of("CF"))
        .withFamilyStoreDirectoryPath(splitAPath).withRegionFileSystem(splitAregionFS).build();
    StoreFileTracker splitAsft = StoreFileTrackerFactory.create(conf, false, splitAStoreContext);
    Reference bottom = new Reference(midkey, Reference.Range.bottom);
    splitAsft.createReference(bottom, splitFileA);
    doTestOfScanAndReseek(splitFileA, fs, bottom, cacheConf);

    HRegionFileSystem splitBregionFS =
      HRegionFileSystem.create(conf, fs, new Path(root_dir), splitBHri);
    StoreContext splitBStoreContext =
      StoreContext.getBuilder().withColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder.of("CF"))
        .withFamilyStoreDirectoryPath(splitBPath).withRegionFileSystem(splitBregionFS).build();
    StoreFileTracker splitBsft = StoreFileTrackerFactory.create(conf, false, splitBStoreContext);
    Reference top = new Reference(midkey, Reference.Range.top);
    splitBsft.createReference(top, splitFileB);
    doTestOfScanAndReseek(splitFileB, fs, top, cacheConf);

    r.close();
  }

  private void doTestOfScanAndReseek(Path p, FileSystem fs, Reference bottom, CacheConfig cacheConf)
    throws IOException {
    Path referencePath = StoreFileInfo.getReferredToFile(p);
    FSDataInputStreamWrapper in = new FSDataInputStreamWrapper(fs, referencePath, false, 0);
    FileStatus status = fs.getFileStatus(referencePath);
    long length = status.getLen();
    ReaderContextBuilder contextBuilder =
      new ReaderContextBuilder().withInputStreamWrapper(in).withFileSize(length)
        .withReaderType(ReaderContext.ReaderType.PREAD).withFileSystem(fs).withFilePath(p);
    ReaderContext context = contextBuilder.build();
    StoreFileInfo storeFileInfo =
      new StoreFileInfo(TEST_UTIL.getConfiguration(), fs, fs.getFileStatus(p), bottom);
    storeFileInfo.initHFileInfo(context);
    final HalfStoreFileReader halfreader =
      (HalfStoreFileReader) storeFileInfo.createReader(context, cacheConf);
    storeFileInfo.getHFileInfo().initMetaAndIndex(halfreader.getHFileReader());
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
    StoreFileInfo storeFileInfo =
      new StoreFileInfo(TEST_UTIL.getConfiguration(), fs, fs.getFileStatus(p), bottom);
    storeFileInfo.initHFileInfo(context);
    final HalfStoreFileReader halfreader =
      (HalfStoreFileReader) storeFileInfo.createReader(context, cacheConfig);
    storeFileInfo.getHFileInfo().initMetaAndIndex(halfreader.getHFileReader());
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
