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
package org.apache.hadoop.hbase.regionserver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test HStoreFile
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestHStoreFile extends HBaseTestCase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHStoreFile.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHStoreFile.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
  private static String ROOT_DIR = TEST_UTIL.getDataTestDir("TestStoreFile").toString();
  private static final ChecksumType CKTYPE = ChecksumType.CRC32C;
  private static final int CKBYTES = 512;
  private static String TEST_FAMILY = "cf";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Write a file and then assert that we can read from top and bottom halves using two
   * HalfMapFiles, as well as one HalfMapFile and one HFileLink file.
   */
  @Test
  public void testBasicHalfAndHFileLinkMapFile() throws Exception {
    final HRegionInfo hri =
        new HRegionInfo(TableName.valueOf("testBasicHalfAndHFileLinkMapFile"));
    // The locations of HFileLink refers hfiles only should be consistent with the table dir
    // create by CommonFSUtils directory, so we should make the region directory under
    // the mode of CommonFSUtils.getTableDir here.
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs,
      CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), hri.getTable()), hri);

    HFileContext meta = new HFileContextBuilder().withBlockSize(2*1024).build();
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(regionFs.createTempName())
            .withFileContext(meta)
            .build();
    writeStoreFile(writer);

    Path sfPath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());
    HStoreFile sf = new HStoreFile(this.fs, sfPath, conf, cacheConf, BloomType.NONE, true);
    checkHalfHFile(regionFs, sf);
  }

  private void writeStoreFile(final StoreFileWriter writer) throws IOException {
    writeStoreFile(writer, Bytes.toBytes(getName()), Bytes.toBytes(getName()));
  }

  // pick an split point (roughly halfway)
  byte[] SPLITKEY = new byte[] { (LAST_CHAR + FIRST_CHAR) / 2, FIRST_CHAR };

  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and
   * then closes it.
   * @param writer
   * @throws IOException
   */
  public static void writeStoreFile(final StoreFileWriter writer, byte[] fam, byte[] qualifier)
      throws IOException {
    long now = System.currentTimeMillis();
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          writer.append(new KeyValue(b, fam, qualifier, now, b));
        }
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Test that our mechanism of writing store files in one region to reference
   * store files in other regions works.
   */
  @Test
  public void testReference() throws IOException {
    final HRegionInfo hri = new HRegionInfo(TableName.valueOf("testReferenceTb"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(
      conf, fs, new Path(testDir, hri.getTable().getNameAsString()), hri);

    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(regionFs.createTempName())
            .withFileContext(meta)
            .build();
    writeStoreFile(writer);

    Path hsfPath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());
    HStoreFile hsf = new HStoreFile(this.fs, hsfPath, conf, cacheConf, BloomType.NONE, true);
    hsf.initReader();
    StoreFileReader reader = hsf.getReader();
    // Split on a row, not in middle of row.  Midkey returned by reader
    // may be in middle of row.  Create new one with empty column and
    // timestamp.
    byte [] midRow = CellUtil.cloneRow(reader.midKey().get());
    byte [] finalRow = CellUtil.cloneRow(reader.getLastKey().get());
    hsf.closeStoreFile(true);

    // Make a reference
    HRegionInfo splitHri = new HRegionInfo(hri.getTable(), null, midRow);
    Path refPath = splitStoreFile(regionFs, splitHri, TEST_FAMILY, hsf, midRow, true);
    HStoreFile refHsf = new HStoreFile(this.fs, refPath, conf, cacheConf, BloomType.NONE, true);
    refHsf.initReader();
    // Now confirm that I can read from the reference and that it only gets
    // keys from top half of the file.
    HFileScanner s = refHsf.getReader().getScanner(false, false);
    Cell kv = null;
    for (boolean first = true; (!s.isSeeked() && s.seekTo()) || s.next();) {
      ByteBuffer bb = ByteBuffer.wrap(((KeyValue) s.getKey()).getKey());
      kv = KeyValueUtil.createKeyValueFromKey(bb);
      if (first) {
        assertTrue(Bytes.equals(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), midRow, 0,
          midRow.length));
        first = false;
      }
    }
    assertTrue(Bytes.equals(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), finalRow, 0,
      finalRow.length));
  }

  @Test
  public void testStoreFileReference() throws Exception {
    final RegionInfo hri =
        RegionInfoBuilder.newBuilder(TableName.valueOf("testStoreFileReference")).build();
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs,
      new Path(testDir, hri.getTable().getNameAsString()), hri);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();

    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
        .withFilePath(regionFs.createTempName()).withFileContext(meta).build();
    writeStoreFile(writer);
    Path hsfPath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());
    writer.close();

    HStoreFile file = new HStoreFile(this.fs, hsfPath, conf, cacheConf, BloomType.NONE, true);
    file.initReader();
    StoreFileReader r = file.getReader();
    assertNotNull(r);
    StoreFileScanner scanner =
        new StoreFileScanner(r, mock(HFileScanner.class), false, false, 0, 0, false);

    // Verify after instantiating scanner refCount is increased
    assertTrue("Verify file is being referenced", file.isReferencedInReads());
    scanner.close();
    // Verify after closing scanner refCount is decreased
    assertFalse("Verify file is not being referenced", file.isReferencedInReads());
  }

  @Test
  public void testEmptyStoreFileRestrictKeyRanges() throws Exception {
    StoreFileReader reader = mock(StoreFileReader.class);
    HStore store = mock(HStore.class);
    byte[] cf = Bytes.toBytes("ty");
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(cf);
    when(store.getColumnFamilyDescriptor()).thenReturn(cfd);
    StoreFileScanner scanner =
        new StoreFileScanner(reader, mock(HFileScanner.class), false, false, 0, 0, true);
    Scan scan = new Scan();
    scan.setColumnFamilyTimeRange(cf, 0, 1);
    assertFalse(scanner.shouldUseScanner(scan, store, 0));
  }

  @Test
  public void testHFileLink() throws IOException {
    final HRegionInfo hri = new HRegionInfo(TableName.valueOf("testHFileLinkTb"));
    // force temp data in hbase/target/test-data instead of /tmp/hbase-xxxx/
    Configuration testConf = new Configuration(this.conf);
    CommonFSUtils.setRootDir(testConf, testDir);
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(
      testConf, fs, CommonFSUtils.getTableDir(testDir, hri.getTable()), hri);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();

    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(regionFs.createTempName())
            .withFileContext(meta)
            .build();
    writeStoreFile(writer);

    Path storeFilePath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());
    Path dstPath = new Path(regionFs.getTableDir(), new Path("test-region", TEST_FAMILY));
    HFileLink.create(testConf, this.fs, dstPath, hri, storeFilePath.getName());
    Path linkFilePath = new Path(dstPath,
                  HFileLink.createHFileLinkName(hri, storeFilePath.getName()));

    // Try to open store file from link
    StoreFileInfo storeFileInfo = new StoreFileInfo(testConf, this.fs, linkFilePath, true);
    HStoreFile hsf = new HStoreFile(storeFileInfo, BloomType.NONE, cacheConf);
    assertTrue(storeFileInfo.isLink());
    hsf.initReader();

    // Now confirm that I can read from the link
    int count = 1;
    HFileScanner s = hsf.getReader().getScanner(false, false);
    s.seekTo();
    while (s.next()) {
      count++;
    }
    assertEquals((LAST_CHAR - FIRST_CHAR + 1) * (LAST_CHAR - FIRST_CHAR + 1), count);
  }

  /**
   * This test creates an hfile and then the dir structures and files to verify that references
   * to hfilelinks (created by snapshot clones) can be properly interpreted.
   */
  @Test
  public void testReferenceToHFileLink() throws IOException {
    // force temp data in hbase/target/test-data instead of /tmp/hbase-xxxx/
    Configuration testConf = new Configuration(this.conf);
    CommonFSUtils.setRootDir(testConf, testDir);

    // adding legal table name chars to verify regex handles it.
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("_original-evil-name"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(
      testConf, fs, CommonFSUtils.getTableDir(testDir, hri.getTable()), hri);

    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    // Make a store file and write data to it. <root>/<tablename>/<rgn>/<cf>/<file>
    StoreFileWriter writer = new StoreFileWriter.Builder(testConf, cacheConf, this.fs)
            .withFilePath(regionFs.createTempName())
            .withFileContext(meta)
            .build();
    writeStoreFile(writer);
    Path storeFilePath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());

    // create link to store file. <root>/clone/region/<cf>/<hfile>-<region>-<table>
    HRegionInfo hriClone = new HRegionInfo(TableName.valueOf("clone"));
    HRegionFileSystem cloneRegionFs = HRegionFileSystem.createRegionOnFileSystem(
      testConf, fs, CommonFSUtils.getTableDir(testDir, hri.getTable()),
        hriClone);
    Path dstPath = cloneRegionFs.getStoreDir(TEST_FAMILY);
    HFileLink.create(testConf, this.fs, dstPath, hri, storeFilePath.getName());
    Path linkFilePath = new Path(dstPath,
                  HFileLink.createHFileLinkName(hri, storeFilePath.getName()));

    // create splits of the link.
    // <root>/clone/splitA/<cf>/<reftohfilelink>,
    // <root>/clone/splitB/<cf>/<reftohfilelink>
    HRegionInfo splitHriA = new HRegionInfo(hri.getTable(), null, SPLITKEY);
    HRegionInfo splitHriB = new HRegionInfo(hri.getTable(), SPLITKEY, null);
    HStoreFile f = new HStoreFile(fs, linkFilePath, testConf, cacheConf, BloomType.NONE, true);
    f.initReader();
    Path pathA = splitStoreFile(cloneRegionFs, splitHriA, TEST_FAMILY, f, SPLITKEY, true); // top
    Path pathB = splitStoreFile(cloneRegionFs, splitHriB, TEST_FAMILY, f, SPLITKEY, false);// bottom
    f.closeStoreFile(true);
    // OK test the thing
    CommonFSUtils.logFileSystemState(fs, testDir, LOG);

    // There is a case where a file with the hfilelink pattern is actually a daughter
    // reference to a hfile link.  This code in StoreFile that handles this case.

    // Try to open store file from link
    HStoreFile hsfA = new HStoreFile(this.fs, pathA, testConf, cacheConf, BloomType.NONE, true);
    hsfA.initReader();

    // Now confirm that I can read from the ref to link
    int count = 1;
    HFileScanner s = hsfA.getReader().getScanner(false, false);
    s.seekTo();
    while (s.next()) {
      count++;
    }
    assertTrue(count > 0); // read some rows here

    // Try to open store file from link
    HStoreFile hsfB = new HStoreFile(this.fs, pathB, testConf, cacheConf, BloomType.NONE, true);
    hsfB.initReader();

    // Now confirm that I can read from the ref to link
    HFileScanner sB = hsfB.getReader().getScanner(false, false);
    sB.seekTo();

    //count++ as seekTo() will advance the scanner
    count++;
    while (sB.next()) {
      count++;
    }

    // read the rest of the rows
    assertEquals((LAST_CHAR - FIRST_CHAR + 1) * (LAST_CHAR - FIRST_CHAR + 1), count);
  }

  private void checkHalfHFile(final HRegionFileSystem regionFs, final HStoreFile f)
      throws IOException {
    f.initReader();
    Cell midkey = f.getReader().midKey().get();
    KeyValue midKV = (KeyValue) midkey;
    // 1. test using the midRow as the splitKey, this test will generate two Reference files
    // in the children
    byte[] midRow = CellUtil.cloneRow(midKV);
    // Create top split.
    HRegionInfo topHri = new HRegionInfo(regionFs.getRegionInfo().getTable(), null, midRow);
    Path topPath = splitStoreFile(regionFs, topHri, TEST_FAMILY, f, midRow, true);
    // Create bottom split.
    HRegionInfo bottomHri = new HRegionInfo(regionFs.getRegionInfo().getTable(),
        midRow, null);
    Path bottomPath = splitStoreFile(regionFs, bottomHri, TEST_FAMILY, f, midRow, false);
    // Make readers on top and bottom.
    HStoreFile topF = new HStoreFile(this.fs, topPath, conf, cacheConf, BloomType.NONE, true);
    topF.initReader();
    StoreFileReader top = topF.getReader();
    HStoreFile bottomF = new HStoreFile(this.fs, bottomPath, conf, cacheConf, BloomType.NONE, true);
    bottomF.initReader();
    StoreFileReader bottom = bottomF.getReader();
    ByteBuffer previous = null;
    LOG.info("Midkey: " + midKV.toString());
    ByteBuffer bbMidkeyBytes = ByteBuffer.wrap(midKV.getKey());
    try {
      // Now make two HalfMapFiles and assert they can read the full backing
      // file, one from the top and the other from the bottom.
      // Test bottom half first.
      // Now test reading from the top.
      boolean first = true;
      ByteBuffer key = null;
      HFileScanner topScanner = top.getScanner(false, false);
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
             (topScanner.isSeeked() && topScanner.next())) {
        key = ByteBuffer.wrap(((KeyValue) topScanner.getKey()).getKey());

        if ((PrivateCellUtil.compare(topScanner.getReader().getComparator(), midKV, key.array(),
          key.arrayOffset(), key.limit())) > 0) {
          fail("key=" + Bytes.toStringBinary(key) + " < midkey=" +
              midkey);
        }
        if (first) {
          first = false;
          LOG.info("First in top: " + Bytes.toString(Bytes.toBytes(key)));
        }
      }
      LOG.info("Last in top: " + Bytes.toString(Bytes.toBytes(key)));

      first = true;
      HFileScanner bottomScanner = bottom.getScanner(false, false);
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        previous = ByteBuffer.wrap(((KeyValue) bottomScanner.getKey()).getKey());
        key = ByteBuffer.wrap(((KeyValue) bottomScanner.getKey()).getKey());
        if (first) {
          first = false;
          LOG.info("First in bottom: " +
            Bytes.toString(Bytes.toBytes(previous)));
        }
        assertTrue(key.compareTo(bbMidkeyBytes) < 0);
      }
      if (previous != null) {
        LOG.info("Last in bottom: " + Bytes.toString(Bytes.toBytes(previous)));
      }
      // Remove references.
      regionFs.cleanupDaughterRegion(topHri);
      regionFs.cleanupDaughterRegion(bottomHri);

      // 2. test using a midkey which will generate one Reference file and one HFileLink file.
      // First, do a key that is < than first key. Ensure splits behave
      // properly.
      byte [] badmidkey = Bytes.toBytes("  .");
      assertTrue(fs.exists(f.getPath()));
      topPath = splitStoreFile(regionFs, topHri, TEST_FAMILY, f, badmidkey, true);
      bottomPath = splitStoreFile(regionFs, bottomHri, TEST_FAMILY, f, badmidkey, false);

      assertNull(bottomPath);

      topF = new HStoreFile(this.fs, topPath, conf, cacheConf, BloomType.NONE, true);
      topF.initReader();
      top = topF.getReader();
      // Now read from the top.
      first = true;
      topScanner = top.getScanner(false, false);
      KeyValue.KeyOnlyKeyValue keyOnlyKV = new KeyValue.KeyOnlyKeyValue();
      while ((!topScanner.isSeeked() && topScanner.seekTo()) ||
          topScanner.next()) {
        key = ByteBuffer.wrap(((KeyValue) topScanner.getKey()).getKey());
        keyOnlyKV.setKey(key.array(), 0 + key.arrayOffset(), key.limit());
        assertTrue(PrivateCellUtil.compare(topScanner.getReader().getComparator(), keyOnlyKV,
          badmidkey, 0, badmidkey.length) >= 0);
        if (first) {
          first = false;
          KeyValue keyKV = KeyValueUtil.createKeyValueFromKey(key);
          LOG.info("First top when key < bottom: " + keyKV);
          String tmp =
              Bytes.toString(keyKV.getRowArray(), keyKV.getRowOffset(), keyKV.getRowLength());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      KeyValue keyKV = KeyValueUtil.createKeyValueFromKey(key);
      LOG.info("Last top when key < bottom: " + keyKV);
      String tmp = Bytes.toString(keyKV.getRowArray(), keyKV.getRowOffset(), keyKV.getRowLength());
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(tmp.charAt(i) == 'z');
      }
      // Remove references.
      regionFs.cleanupDaughterRegion(topHri);
      regionFs.cleanupDaughterRegion(bottomHri);

      // Test when badkey is > than last key in file ('||' > 'zz').
      badmidkey = Bytes.toBytes("|||");
      topPath = splitStoreFile(regionFs,topHri, TEST_FAMILY, f, badmidkey, true);
      bottomPath = splitStoreFile(regionFs, bottomHri, TEST_FAMILY, f, badmidkey, false);
      assertNull(topPath);

      bottomF = new HStoreFile(this.fs, bottomPath, conf, cacheConf, BloomType.NONE, true);
      bottomF.initReader();
      bottom = bottomF.getReader();
      first = true;
      bottomScanner = bottom.getScanner(false, false);
      while ((!bottomScanner.isSeeked() && bottomScanner.seekTo()) ||
          bottomScanner.next()) {
        key = ByteBuffer.wrap(((KeyValue) bottomScanner.getKey()).getKey());
        if (first) {
          first = false;
          keyKV = KeyValueUtil.createKeyValueFromKey(key);
          LOG.info("First bottom when key > top: " + keyKV);
          tmp = Bytes.toString(keyKV.getRowArray(), keyKV.getRowOffset(), keyKV.getRowLength());
          for (int i = 0; i < tmp.length(); i++) {
            assertTrue(tmp.charAt(i) == 'a');
          }
        }
      }
      keyKV = KeyValueUtil.createKeyValueFromKey(key);
      LOG.info("Last bottom when key > top: " + keyKV);
      for (int i = 0; i < tmp.length(); i++) {
        assertTrue(Bytes.toString(keyKV.getRowArray(), keyKV.getRowOffset(), keyKV.getRowLength())
            .charAt(i) == 'z');
      }
    } finally {
      if (top != null) {
        top.close(true); // evict since we are about to delete the file
      }
      if (bottom != null) {
        bottom.close(true); // evict since we are about to delete the file
      }
      fs.delete(f.getPath(), true);
    }
  }

  private static StoreFileScanner getStoreFileScanner(StoreFileReader reader, boolean cacheBlocks,
      boolean pread) {
    return reader.getStoreFileScanner(cacheBlocks, pread, false, 0, 0, false);
  }

  private static final String localFormatter = "%010d";

  private void bloomWriteRead(StoreFileWriter writer, FileSystem fs) throws Exception {
    float err = conf.getFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, 0);
    Path f = writer.getPath();
    long now = System.currentTimeMillis();
    for (int i = 0; i < 2000; i += 2) {
      String row = String.format(localFormatter, i);
      KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
        Bytes.toBytes("col"), now, Bytes.toBytes("value"));
      writer.append(kv);
    }
    writer.close();

    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, f).build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    StoreFileReader reader =
        new StoreFileReader(context, fileInfo, cacheConf, new AtomicInteger(0), conf);
    fileInfo.initMetaAndIndex(reader.getHFileReader());
    reader.loadFileInfo();
    reader.loadBloomfilter();
    StoreFileScanner scanner = getStoreFileScanner(reader, false, false);

    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      String row = String.format(localFormatter, i);
      TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      columns.add(Bytes.toBytes("family:col"));

      Scan scan = new Scan(Bytes.toBytes(row),Bytes.toBytes(row));
      scan.addColumn(Bytes.toBytes("family"), Bytes.toBytes("family:col"));
      HStore store = mock(HStore.class);
      when(store.getColumnFamilyDescriptor())
          .thenReturn(ColumnFamilyDescriptorBuilder.of("family"));
      boolean exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      if (i % 2 == 0) {
        if (!exists) {
          falseNeg++;
        }
      } else {
        if (exists) {
          falsePos++;
        }
      }
    }
    reader.close(true); // evict because we are about to delete the file
    fs.delete(f, true);
    assertEquals("False negatives: " + falseNeg, 0, falseNeg);
    int maxFalsePos = (int) (2 * 2000 * err);
    assertTrue("Too many false positives: " + falsePos + " (err=" + err + ", expected no more than "
            + maxFalsePos + ")", falsePos <= maxFalsePos);
  }

  private static final int BLOCKSIZE_SMALL = 8192;

  @Test
  public void testBloomFilter() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, (float) 0.01);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);

    // write the file
    Path f = new Path(ROOT_DIR, getName());
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL)
                        .withChecksumType(CKTYPE)
                        .withBytesPerCheckSum(CKBYTES).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(f)
            .withBloomType(BloomType.ROW)
            .withMaxKeyCount(2000)
            .withFileContext(meta)
            .build();
    bloomWriteRead(writer, fs);
  }

  @Test
  public void testDeleteFamilyBloomFilter() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, (float) 0.01);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    float err = conf.getFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, 0);

    // write the file
    Path f = new Path(ROOT_DIR, getName());

    HFileContext meta = new HFileContextBuilder()
                        .withBlockSize(BLOCKSIZE_SMALL)
                        .withChecksumType(CKTYPE)
                        .withBytesPerCheckSum(CKBYTES).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(f)
            .withMaxKeyCount(2000)
            .withFileContext(meta)
            .build();

    // add delete family
    long now = System.currentTimeMillis();
    for (int i = 0; i < 2000; i += 2) {
      String row = String.format(localFormatter, i);
      KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
          Bytes.toBytes("col"), now, KeyValue.Type.DeleteFamily, Bytes.toBytes("value"));
      writer.append(kv);
    }
    writer.close();

    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, f).build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    StoreFileReader reader =
        new StoreFileReader(context, fileInfo, cacheConf, new AtomicInteger(0), conf);
    fileInfo.initMetaAndIndex(reader.getHFileReader());
    reader.loadFileInfo();
    reader.loadBloomfilter();

    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      String row = String.format(localFormatter, i);
      byte[] rowKey = Bytes.toBytes(row);
      boolean exists = reader.passesDeleteFamilyBloomFilter(rowKey, 0, rowKey.length);
      if (i % 2 == 0) {
        if (!exists) {
          falseNeg++;
        }
      } else {
        if (exists) {
          falsePos++;
        }
      }
    }
    assertEquals(1000, reader.getDeleteFamilyCnt());
    reader.close(true); // evict because we are about to delete the file
    fs.delete(f, true);
    assertEquals("False negatives: " + falseNeg, 0, falseNeg);
    int maxFalsePos = (int) (2 * 2000 * err);
    assertTrue("Too many false positives: " + falsePos + " (err=" + err
        + ", expected no more than " + maxFalsePos, falsePos <= maxFalsePos);
  }

  /**
   * Test for HBASE-8012
   */
  @Test
  public void testReseek() throws Exception {
    // write the file
    Path f = new Path(ROOT_DIR, getName());
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(f)
            .withFileContext(meta)
            .build();

    writeStoreFile(writer);
    writer.close();

    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, f).build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    StoreFileReader reader =
        new StoreFileReader(context, fileInfo, cacheConf, new AtomicInteger(0), conf);
    fileInfo.initMetaAndIndex(reader.getHFileReader());

    // Now do reseek with empty KV to position to the beginning of the file

    KeyValue k = KeyValueUtil.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY);
    StoreFileScanner s = getStoreFileScanner(reader, false, false);
    s.reseek(k);

    assertNotNull("Intial reseek should position at the beginning of the file", s.peek());
  }

  @Test
  public void testBloomTypes() throws Exception {
    float err = (float) 0.01;
    FileSystem fs = FileSystem.getLocal(conf);
    conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, err);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);

    int rowCount = 50;
    int colCount = 10;
    int versions = 2;

    // run once using columns and once using rows
    BloomType[] bt = {BloomType.ROWCOL, BloomType.ROW};
    int[] expKeys  = {rowCount*colCount, rowCount};
    // below line deserves commentary.  it is expected bloom false positives
    //  column = rowCount*2*colCount inserts
    //  row-level = only rowCount*2 inserts, but failures will be magnified by
    //              2nd for loop for every column (2*colCount)
    float[] expErr   = {2*rowCount*colCount*err, 2*rowCount*2*colCount*err};

    for (int x : new int[]{0,1}) {
      // write the file
      Path f = new Path(ROOT_DIR, getName() + x);
      HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL)
          .withChecksumType(CKTYPE)
          .withBytesPerCheckSum(CKBYTES).build();
      // Make a store file and write data to it.
      StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
              .withFilePath(f)
              .withBloomType(bt[x])
              .withMaxKeyCount(expKeys[x])
              .withFileContext(meta)
              .build();

      long now = System.currentTimeMillis();
      for (int i = 0; i < rowCount*2; i += 2) { // rows
        for (int j = 0; j < colCount*2; j += 2) {   // column qualifiers
          String row = String.format(localFormatter, i);
          String col = String.format(localFormatter, j);
          for (int k= 0; k < versions; ++k) { // versions
            KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
                Bytes.toBytes("col" + col), now-k, Bytes.toBytes(-1L));
            writer.append(kv);
          }
        }
      }
      writer.close();

      ReaderContext context = new ReaderContextBuilder()
          .withFilePath(f)
          .withFileSize(fs.getFileStatus(f).getLen())
          .withFileSystem(fs)
          .withInputStreamWrapper(new FSDataInputStreamWrapper(fs, f))
          .build();
      HFileInfo fileInfo = new HFileInfo(context, conf);
      StoreFileReader reader =
          new StoreFileReader(context, fileInfo, cacheConf, new AtomicInteger(0), conf);
      fileInfo.initMetaAndIndex(reader.getHFileReader());
      reader.loadFileInfo();
      reader.loadBloomfilter();
      StoreFileScanner scanner = getStoreFileScanner(reader, false, false);
      assertEquals(expKeys[x], reader.getGeneralBloomFilter().getKeyCount());

      HStore store = mock(HStore.class);
      when(store.getColumnFamilyDescriptor())
          .thenReturn(ColumnFamilyDescriptorBuilder.of("family"));
      // check false positives rate
      int falsePos = 0;
      int falseNeg = 0;
      for (int i = 0; i < rowCount*2; ++i) { // rows
        for (int j = 0; j < colCount*2; ++j) {   // column qualifiers
          String row = String.format(localFormatter, i);
          String col = String.format(localFormatter, j);
          TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
          columns.add(Bytes.toBytes("col" + col));

          Scan scan = new Scan(Bytes.toBytes(row),Bytes.toBytes(row));
          scan.addColumn(Bytes.toBytes("family"), Bytes.toBytes(("col"+col)));

          boolean exists =
              scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
          boolean shouldRowExist = i % 2 == 0;
          boolean shouldColExist = j % 2 == 0;
          shouldColExist = shouldColExist || bt[x] == BloomType.ROW;
          if (shouldRowExist && shouldColExist) {
            if (!exists) {
              falseNeg++;
            }
          } else {
            if (exists) {
              falsePos++;
            }
          }
        }
      }
      reader.close(true); // evict because we are about to delete the file
      fs.delete(f, true);
      System.out.println(bt[x].toString());
      System.out.println("  False negatives: " + falseNeg);
      System.out.println("  False positives: " + falsePos);
      assertEquals(0, falseNeg);
      assertTrue(falsePos < 2*expErr[x]);
    }
  }

  @Test
  public void testSeqIdComparator() {
    assertOrdering(StoreFileComparators.SEQ_ID, mockStoreFile(true, 100, 1000, -1, "/foo/123"),
        mockStoreFile(true, 100, 1000, -1, "/foo/124"),
        mockStoreFile(true, 99, 1000, -1, "/foo/126"),
        mockStoreFile(true, 98, 2000, -1, "/foo/126"), mockStoreFile(false, 3453, -1, 1, "/foo/1"),
        mockStoreFile(false, 2, -1, 3, "/foo/2"), mockStoreFile(false, 1000, -1, 5, "/foo/2"),
        mockStoreFile(false, 76, -1, 5, "/foo/3"));
  }

  /**
   * Assert that the given comparator orders the given storefiles in the
   * same way that they're passed.
   */
  private void assertOrdering(Comparator<? super HStoreFile> comparator, HStoreFile ... sfs) {
    ArrayList<HStoreFile> sorted = Lists.newArrayList(sfs);
    Collections.shuffle(sorted);
    Collections.sort(sorted, comparator);
    LOG.debug("sfs: " + Joiner.on(",").join(sfs));
    LOG.debug("sorted: " + Joiner.on(",").join(sorted));
    assertTrue(Iterables.elementsEqual(Arrays.asList(sfs), sorted));
  }

  /**
   * Create a mock StoreFile with the given attributes.
   */
  private HStoreFile mockStoreFile(boolean bulkLoad,
                                  long size,
                                  long bulkTimestamp,
                                  long seqId,
                                  String path) {
    HStoreFile mock = Mockito.mock(HStoreFile.class);
    StoreFileReader reader = Mockito.mock(StoreFileReader.class);

    Mockito.doReturn(size).when(reader).length();

    Mockito.doReturn(reader).when(mock).getReader();
    Mockito.doReturn(bulkLoad).when(mock).isBulkLoadResult();
    Mockito.doReturn(OptionalLong.of(bulkTimestamp)).when(mock).getBulkLoadTimestamp();
    Mockito.doReturn(seqId).when(mock).getMaxSequenceId();
    Mockito.doReturn(new Path(path)).when(mock).getPath();
    String name = "mock storefile, bulkLoad=" + bulkLoad +
      " bulkTimestamp=" + bulkTimestamp +
      " seqId=" + seqId +
      " path=" + path;
    Mockito.doReturn(name).when(mock).toString();
    return mock;
  }

  /**
   * Generate a list of KeyValues for testing based on given parameters
   * @return the rows key-value list
   */
  List<KeyValue> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family) {
    List<KeyValue> kvList = new ArrayList<>();
    for (int i=1;i<=numRows;i++) {
      byte[] b = Bytes.toBytes(i) ;
      LOG.info(Bytes.toString(b));
      LOG.info(Bytes.toString(b));
      for (long timestamp: timestamps) {
        kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
      }
    }
    return kvList;
  }

  /**
   * Test to ensure correctness when using StoreFile with multiple timestamps
   */
  @Test
  public void testMultipleTimestamps() throws IOException {
    byte[] family = Bytes.toBytes("familyname");
    byte[] qualifier = Bytes.toBytes("qualifier");
    int numRows = 10;
    long[] timestamps = new long[] {20,10,5,1};
    Scan scan = new Scan();

    // Make up a directory hierarchy that has a regiondir ("7e0102") and familyname.
    Path storedir = new Path(new Path(testDir, "7e0102"), Bytes.toString(family));
    Path dir = new Path(storedir, "1234567890");
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withOutputDir(dir)
            .withFileContext(meta)
            .build();

    List<KeyValue> kvList = getKeyValueSet(timestamps,numRows,
        qualifier, family);

    for (KeyValue kv : kvList) {
      writer.append(kv);
    }
    writer.appendMetadata(0, false);
    writer.close();

    HStoreFile hsf = new HStoreFile(this.fs, writer.getPath(), conf, cacheConf,
      BloomType.NONE, true);
    HStore store = mock(HStore.class);
    when(store.getColumnFamilyDescriptor()).thenReturn(ColumnFamilyDescriptorBuilder.of(family));
    hsf.initReader();
    StoreFileReader reader = hsf.getReader();
    StoreFileScanner scanner = getStoreFileScanner(reader, false, false);
    TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    columns.add(qualifier);

    scan.setTimeRange(20, 100);
    assertTrue(scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));

    scan.setTimeRange(1, 2);
    assertTrue(scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));

    scan.setTimeRange(8, 10);
    assertTrue(scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));

    // lets make sure it still works with column family time ranges
    scan.setColumnFamilyTimeRange(family, 7, 50);
    assertTrue(scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));

    // This test relies on the timestamp range optimization
    scan = new Scan();
    scan.setTimeRange(27, 50);
    assertTrue(!scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));

    // should still use the scanner because we override the family time range
    scan = new Scan();
    scan.setTimeRange(27, 50);
    scan.setColumnFamilyTimeRange(family, 7, 50);
    assertTrue(scanner.shouldUseScanner(scan, store, Long.MIN_VALUE));
  }

  @Test
  public void testCacheOnWriteEvictOnClose() throws Exception {
    Configuration conf = this.conf;

    // Find a home for our files (regiondir ("7e0102") and familyname).
    Path baseDir = new Path(new Path(testDir, "7e0102"),"twoCOWEOC");

    // Grab the block cache and get the initial hit/miss counts
    BlockCache bc = BlockCacheFactory.createBlockCache(conf);
    assertNotNull(bc);
    CacheStats cs = bc.getStats();
    long startHit = cs.getHitCount();
    long startMiss = cs.getMissCount();
    long startEvicted = cs.getEvictedCount();

    // Let's write a StoreFile with three blocks, with cache on write off
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, false);
    CacheConfig cacheConf = new CacheConfig(conf, bc);
    Path pathCowOff = new Path(baseDir, "123456789");
    StoreFileWriter writer = writeStoreFile(conf, cacheConf, pathCowOff, 3);
    HStoreFile hsf = new HStoreFile(this.fs, writer.getPath(), conf, cacheConf,
      BloomType.NONE, true);
    LOG.debug(hsf.getPath().toString());

    // Read this file, we should see 3 misses
    hsf.initReader();
    StoreFileReader reader = hsf.getReader();
    reader.loadFileInfo();
    StoreFileScanner scanner = getStoreFileScanner(reader, true, true);
    scanner.seek(KeyValue.LOWESTKEY);
    while (scanner.next() != null) {
      continue;
    }
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss + 3, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startMiss += 3;
    scanner.close();
    reader.close(cacheConf.shouldEvictOnClose());

    // Now write a StoreFile with three blocks, with cache on write on
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    cacheConf = new CacheConfig(conf, bc);
    Path pathCowOn = new Path(baseDir, "123456788");
    writer = writeStoreFile(conf, cacheConf, pathCowOn, 3);
    hsf = new HStoreFile(this.fs, writer.getPath(), conf, cacheConf,
      BloomType.NONE, true);

    // Read this file, we should see 3 hits
    hsf.initReader();
    reader = hsf.getReader();
    scanner = getStoreFileScanner(reader, true, true);
    scanner.seek(KeyValue.LOWESTKEY);
    while (scanner.next() != null) {
      continue;
    }
    assertEquals(startHit + 3, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startHit += 3;
    scanner.close();
    reader.close(cacheConf.shouldEvictOnClose());

    // Let's read back the two files to ensure the blocks exactly match
    hsf = new HStoreFile(this.fs, pathCowOff, conf, cacheConf, BloomType.NONE, true);
    hsf.initReader();
    StoreFileReader readerOne = hsf.getReader();
    readerOne.loadFileInfo();
    StoreFileScanner scannerOne = getStoreFileScanner(readerOne, true, true);
    scannerOne.seek(KeyValue.LOWESTKEY);
    hsf = new HStoreFile(this.fs, pathCowOn, conf, cacheConf, BloomType.NONE, true);
    hsf.initReader();
    StoreFileReader readerTwo = hsf.getReader();
    readerTwo.loadFileInfo();
    StoreFileScanner scannerTwo = getStoreFileScanner(readerTwo, true, true);
    scannerTwo.seek(KeyValue.LOWESTKEY);
    Cell kv1 = null;
    Cell kv2 = null;
    while ((kv1 = scannerOne.next()) != null) {
      kv2 = scannerTwo.next();
      assertTrue(kv1.equals(kv2));
      KeyValue keyv1 = KeyValueUtil.ensureKeyValue(kv1);
      KeyValue keyv2 = KeyValueUtil.ensureKeyValue(kv2);
      assertTrue(Bytes.compareTo(
          keyv1.getBuffer(), keyv1.getKeyOffset(), keyv1.getKeyLength(),
          keyv2.getBuffer(), keyv2.getKeyOffset(), keyv2.getKeyLength()) == 0);
      assertTrue(Bytes.compareTo(
          kv1.getValueArray(), kv1.getValueOffset(), kv1.getValueLength(),
          kv2.getValueArray(), kv2.getValueOffset(), kv2.getValueLength()) == 0);
    }
    assertNull(scannerTwo.next());
    assertEquals(startHit + 6, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
    startHit += 6;
    scannerOne.close();
    readerOne.close(cacheConf.shouldEvictOnClose());
    scannerTwo.close();
    readerTwo.close(cacheConf.shouldEvictOnClose());

    // Let's close the first file with evict on close turned on
    conf.setBoolean("hbase.rs.evictblocksonclose", true);
    cacheConf = new CacheConfig(conf, bc);
    hsf = new HStoreFile(this.fs, pathCowOff, conf, cacheConf, BloomType.NONE, true);
    hsf.initReader();
    reader = hsf.getReader();
    reader.close(cacheConf.shouldEvictOnClose());

    // We should have 3 new evictions but the evict count stat should not change. Eviction because
    // of HFile invalidation is not counted along with normal evictions
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());

    // Let's close the second file with evict on close turned off
    conf.setBoolean("hbase.rs.evictblocksonclose", false);
    cacheConf = new CacheConfig(conf, bc);
    hsf = new HStoreFile(this.fs, pathCowOn, conf, cacheConf, BloomType.NONE, true);
    hsf.initReader();
    reader = hsf.getReader();
    reader.close(cacheConf.shouldEvictOnClose());

    // We expect no changes
    assertEquals(startHit, cs.getHitCount());
    assertEquals(startMiss, cs.getMissCount());
    assertEquals(startEvicted, cs.getEvictedCount());
  }

  private Path splitStoreFile(final HRegionFileSystem regionFs, final HRegionInfo hri,
      final String family, final HStoreFile sf, final byte[] splitKey, boolean isTopRef)
      throws IOException {
    FileSystem fs = regionFs.getFileSystem();
    Path path = regionFs.splitStoreFile(hri, family, sf, splitKey, isTopRef, null);
    if (null == path) {
      return null;
    }
    Path regionDir = regionFs.commitDaughterRegion(hri);
    return new Path(new Path(regionDir, family), path.getName());
  }

  private StoreFileWriter writeStoreFile(Configuration conf, CacheConfig cacheConf, Path path,
      int numBlocks) throws IOException {
    // Let's put ~5 small KVs in each block, so let's make 5*numBlocks KVs
    int numKVs = 5 * numBlocks;
    List<KeyValue> kvs = new ArrayList<>(numKVs);
    byte [] b = Bytes.toBytes("x");
    int totalSize = 0;
    for (int i=numKVs;i>0;i--) {
      KeyValue kv = new KeyValue(b, b, b, i, b);
      kvs.add(kv);
      // kv has memstoreTS 0, which takes 1 byte to store.
      totalSize += kv.getLength() + 1;
    }
    int blockSize = totalSize / numBlocks;
    HFileContext meta = new HFileContextBuilder().withBlockSize(blockSize)
                        .withChecksumType(CKTYPE)
                        .withBytesPerCheckSum(CKBYTES)
                        .build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(path)
            .withMaxKeyCount(2000)
            .withFileContext(meta)
            .build();
    // We'll write N-1 KVs to ensure we don't write an extra block
    kvs.remove(kvs.size()-1);
    for (KeyValue kv : kvs) {
      writer.append(kv);
    }
    writer.appendMetadata(0, false);
    writer.close();
    return writer;
  }

  /**
   * Check if data block encoding information is saved correctly in HFile's
   * file info.
   */
  @Test
  public void testDataBlockEncodingMetaData() throws IOException {
    // Make up a directory hierarchy that has a regiondir ("7e0102") and familyname.
    Path dir = new Path(new Path(testDir, "7e0102"), "familyname");
    Path path = new Path(dir, "1234567890");

    DataBlockEncoding dataBlockEncoderAlgo =
        DataBlockEncoding.FAST_DIFF;
    HFileDataBlockEncoder dataBlockEncoder =
        new HFileDataBlockEncoderImpl(
            dataBlockEncoderAlgo);
    cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL)
        .withChecksumType(CKTYPE)
        .withBytesPerCheckSum(CKBYTES)
        .withDataBlockEncoding(dataBlockEncoderAlgo)
        .build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, this.fs)
            .withFilePath(path)
            .withMaxKeyCount(2000)
            .withFileContext(meta)
            .build();
    writer.close();

    HStoreFile storeFile =
        new HStoreFile(fs, writer.getPath(), conf, cacheConf, BloomType.NONE, true);
    storeFile.initReader();
    StoreFileReader reader = storeFile.getReader();

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();
    byte[] value = fileInfo.get(HFileDataBlockEncoder.DATA_BLOCK_ENCODING);
    assertEquals(dataBlockEncoderAlgo.getNameInBytes(), value);
  }
}
