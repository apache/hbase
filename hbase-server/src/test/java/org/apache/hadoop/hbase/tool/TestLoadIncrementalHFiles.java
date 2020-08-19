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
package org.apache.hadoop.hbase.tool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for the "load" half of the HFileOutputFormat bulk load functionality. These tests run
 * faster than the full MR cluster tests in TestHFileOutputFormat
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestLoadIncrementalHFiles extends TestLoadIncrementalHFilesBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLoadIncrementalHFiles.class);

  @Test
  public void testSimpleLoadWithMap() throws Exception {
    runTest("testSimpleLoadWithMap", BloomType.NONE,
      new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("ooo") }, },
      true);
  }

  /**
   * Test case that creates some regions and loads HFiles that fit snugly inside those regions
   */
  @Test
  public void testSimpleLoad() throws Exception {
    runTest("testSimpleLoad", BloomType.NONE,
      new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("ooo") }, });
  }

  @Test
  public void testSimpleLoadWithFileCopy() throws Exception {
    String testName = tn.getMethodName();
    final byte[] TABLE_NAME = Bytes.toBytes("mytable_" + testName);
    runTest(testName, buildHTD(TableName.valueOf(TABLE_NAME), BloomType.NONE),
        false, null, new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("ooo") }, },
      false, true, 2);
  }

  /**
   * Test case that creates some regions and loads HFiles that have different region boundaries than
   * the table pre-split.
   */
  @Test
  public void testSimpleHFileSplit() throws Exception {
    runTest("testHFileSplit", BloomType.NONE,
      new byte[][] { Bytes.toBytes("aaa"), Bytes.toBytes("fff"), Bytes.toBytes("jjj"),
          Bytes.toBytes("ppp"), Bytes.toBytes("uuu"), Bytes.toBytes("zzz"), },
      new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("lll") },
          new byte[][] { Bytes.toBytes("mmm"), Bytes.toBytes("zzz") }, });
  }

  @Test
  public void testSplitALot() throws Exception {
    runTest("testSplitALot", BloomType.NONE,
      new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("bbb"), Bytes.toBytes("ccc"),
          Bytes.toBytes("ddd"), Bytes.toBytes("eee"), Bytes.toBytes("fff"), Bytes.toBytes("ggg"),
          Bytes.toBytes("hhh"), Bytes.toBytes("iii"), Bytes.toBytes("lll"), Bytes.toBytes("mmm"),
          Bytes.toBytes("nnn"), Bytes.toBytes("ooo"), Bytes.toBytes("ppp"), Bytes.toBytes("qqq"),
          Bytes.toBytes("rrr"), Bytes.toBytes("sss"), Bytes.toBytes("ttt"), Bytes.toBytes("uuu"),
          Bytes.toBytes("vvv"), Bytes.toBytes("zzz"), },
      new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("zzz") }, });
  }

  /**
   * Test that tags survive through a bulk load that needs to split hfiles. This test depends on the
   * "hbase.client.rpc.codec" = KeyValueCodecWithTags so that the client can get tags in the
   * responses.
   */
  @Test
  public void testTagsSurviveBulkLoadSplit() throws Exception {
    Path dir = util.getDataTestDirOnTestFS(tn.getMethodName());
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));
    // table has these split points
    byte[][] tableSplitKeys = new byte[][] { Bytes.toBytes("aaa"), Bytes.toBytes("fff"),
        Bytes.toBytes("jjj"), Bytes.toBytes("ppp"), Bytes.toBytes("uuu"), Bytes.toBytes("zzz"), };

    // creating an hfile that has values that span the split points.
    byte[] from = Bytes.toBytes("ddd");
    byte[] to = Bytes.toBytes("ooo");
    HFileTestUtil.createHFileWithTags(util.getConfiguration(), fs,
      new Path(familyDir, tn.getMethodName() + "_hfile"), FAMILY, QUALIFIER, from, to, 1000);
    int expectedRows = 1000;

    TableName tableName = TableName.valueOf(tn.getMethodName());
    TableDescriptor htd = buildHTD(tableName, BloomType.NONE);
    util.getAdmin().createTable(htd, tableSplitKeys);

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    String[] args = { dir.toString(), tableName.toString() };
    loader.run(args);

    Table table = util.getConnection().getTable(tableName);
    try {
      assertEquals(expectedRows, util.countRows(table));
      HFileTestUtil.verifyTags(table);
    } finally {
      table.close();
    }

    util.deleteTable(tableName);
  }

  /**
   * Test loading into a column family that does not exist.
   */
  @Test
  public void testNonexistentColumnFamilyLoad() throws Exception {
    String testName = tn.getMethodName();
    byte[][][] hFileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes("aaa"), Bytes.toBytes("ccc") },
            new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("ooo") }, };

    byte[] TABLE = Bytes.toBytes("mytable_" + testName);
    // set real family name to upper case in purpose to simulate the case that
    // family name in HFiles is invalid
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
        .setColumnFamily(ColumnFamilyDescriptorBuilder
            .of(Bytes.toBytes(new String(FAMILY).toUpperCase(Locale.ROOT))))
        .build();

    try {
      runTest(testName, htd, true, SPLIT_KEYS, hFileRanges, false, false, 2);
      assertTrue("Loading into table with non-existent family should have failed", false);
    } catch (Exception e) {
      assertTrue("IOException expected", e instanceof IOException);
      // further check whether the exception message is correct
      String errMsg = e.getMessage();
      assertTrue(
        "Incorrect exception message, expected message: [" + EXPECTED_MSG_FOR_NON_EXISTING_FAMILY +
            "], current message: [" + errMsg + "]",
        errMsg.contains(EXPECTED_MSG_FOR_NON_EXISTING_FAMILY));
    }
  }

  @Test
  public void testNonHfileFolderWithUnmatchedFamilyName() throws Exception {
    testNonHfileFolder("testNonHfileFolderWithUnmatchedFamilyName", true);
  }

  @Test
  public void testNonHfileFolder() throws Exception {
    testNonHfileFolder("testNonHfileFolder", false);
  }

  /**
   * Write a random data file and a non-file in a dir with a valid family name but not part of the
   * table families. we should we able to bulkload without getting the unmatched family exception.
   * HBASE-13037/HBASE-13227
   */
  private void testNonHfileFolder(String tableName, boolean preCreateTable) throws Exception {
    Path dir = util.getDataTestDirOnTestFS(tableName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());

    Path familyDir = new Path(dir, Bytes.toString(FAMILY));
    HFileTestUtil.createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_0"), FAMILY,
      QUALIFIER, Bytes.toBytes("begin"), Bytes.toBytes("end"), 500);
    createRandomDataFile(fs, new Path(familyDir, "012356789"), 16 * 1024);

    final String NON_FAMILY_FOLDER = "_logs";
    Path nonFamilyDir = new Path(dir, NON_FAMILY_FOLDER);
    fs.mkdirs(nonFamilyDir);
    fs.mkdirs(new Path(nonFamilyDir, "non-file"));
    createRandomDataFile(fs, new Path(nonFamilyDir, "012356789"), 16 * 1024);

    Table table = null;
    try {
      if (preCreateTable) {
        table = util.createTable(TableName.valueOf(tableName), FAMILY);
      } else {
        table = util.getConnection().getTable(TableName.valueOf(tableName));
      }

      final String[] args = { dir.toString(), tableName };
      new LoadIncrementalHFiles(util.getConfiguration()).run(args);
      assertEquals(500, util.countRows(table));
    } finally {
      if (table != null) {
        table.close();
      }
      fs.delete(dir, true);
    }
  }

  private static void createRandomDataFile(FileSystem fs, Path path, int size) throws IOException {
    FSDataOutputStream stream = fs.create(path);
    try {
      byte[] data = new byte[1024];
      for (int i = 0; i < data.length; ++i) {
        data[i] = (byte) (i & 0xff);
      }
      while (size >= data.length) {
        stream.write(data, 0, data.length);
        size -= data.length;
      }
      if (size > 0) {
        stream.write(data, 0, size);
      }
    } finally {
      stream.close();
    }
  }

  @Test
  public void testSplitStoreFile() throws IOException {
    Path dir = util.getDataTestDirOnTestFS("testSplitHFile");
    FileSystem fs = util.getTestFileSystem();
    Path testIn = new Path(dir, "testhfile");
    ColumnFamilyDescriptor familyDesc = ColumnFamilyDescriptorBuilder.of(FAMILY);
    HFileTestUtil.createHFile(util.getConfiguration(), fs, testIn, FAMILY, QUALIFIER,
      Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 1000);

    Path bottomOut = new Path(dir, "bottom.out");
    Path topOut = new Path(dir, "top.out");

    LoadIncrementalHFiles.splitStoreFile(util.getConfiguration(), testIn, familyDesc,
      Bytes.toBytes("ggg"), bottomOut, topOut);

    int rowCount = verifyHFile(bottomOut);
    rowCount += verifyHFile(topOut);
    assertEquals(1000, rowCount);
  }

  @Test
  public void testSplitStoreFileWithNoneToNone() throws IOException {
    testSplitStoreFileWithDifferentEncoding(DataBlockEncoding.NONE, DataBlockEncoding.NONE);
  }

  @Test
  public void testSplitStoreFileWithEncodedToEncoded() throws IOException {
    testSplitStoreFileWithDifferentEncoding(DataBlockEncoding.DIFF, DataBlockEncoding.DIFF);
  }

  @Test
  public void testSplitStoreFileWithEncodedToNone() throws IOException {
    testSplitStoreFileWithDifferentEncoding(DataBlockEncoding.DIFF, DataBlockEncoding.NONE);
  }

  @Test
  public void testSplitStoreFileWithNoneToEncoded() throws IOException {
    testSplitStoreFileWithDifferentEncoding(DataBlockEncoding.NONE, DataBlockEncoding.DIFF);
  }

  private void testSplitStoreFileWithDifferentEncoding(DataBlockEncoding bulkloadEncoding,
      DataBlockEncoding cfEncoding) throws IOException {
    Path dir = util.getDataTestDirOnTestFS("testSplitHFileWithDifferentEncoding");
    FileSystem fs = util.getTestFileSystem();
    Path testIn = new Path(dir, "testhfile");
    ColumnFamilyDescriptor familyDesc =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setDataBlockEncoding(cfEncoding).build();
    HFileTestUtil.createHFileWithDataBlockEncoding(util.getConfiguration(), fs, testIn,
      bulkloadEncoding, FAMILY, QUALIFIER, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 1000);

    Path bottomOut = new Path(dir, "bottom.out");
    Path topOut = new Path(dir, "top.out");

    LoadIncrementalHFiles.splitStoreFile(util.getConfiguration(), testIn, familyDesc,
      Bytes.toBytes("ggg"), bottomOut, topOut);

    int rowCount = verifyHFile(bottomOut);
    rowCount += verifyHFile(topOut);
    assertEquals(1000, rowCount);
  }

  private int verifyHFile(Path p) throws IOException {
    Configuration conf = util.getConfiguration();
    HFile.Reader reader =
        HFile.createReader(p.getFileSystem(conf), p, new CacheConfig(conf), true, conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, false);
    scanner.seekTo();
    int count = 0;
    do {
      count++;
    } while (scanner.next());
    assertTrue(count > 0);
    reader.close();
    return count;
  }

  private void addStartEndKeysForTest(TreeMap<byte[], Integer> map, byte[] first, byte[] last) {
    Integer value = map.containsKey(first) ? map.get(first) : 0;
    map.put(first, value + 1);

    value = map.containsKey(last) ? map.get(last) : 0;
    map.put(last, value - 1);
  }

  @Test
  public void testInferBoundaries() {
    TreeMap<byte[], Integer> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    /*
     * Toy example c---------i o------p s---------t v------x a------e g-----k m-------------q r----s
     * u----w Should be inferred as: a-----------------k m-------------q r--------------t
     * u---------x The output should be (m,r,u)
     */

    String first;
    String last;

    first = "a";
    last = "e";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "r";
    last = "s";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "o";
    last = "p";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "g";
    last = "k";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "v";
    last = "x";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "c";
    last = "i";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "m";
    last = "q";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "s";
    last = "t";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "u";
    last = "w";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    byte[][] keysArray = LoadIncrementalHFiles.inferBoundaries(map);
    byte[][] compare = new byte[3][];
    compare[0] = "m".getBytes();
    compare[1] = "r".getBytes();
    compare[2] = "u".getBytes();

    assertEquals(3, keysArray.length);

    for (int row = 0; row < keysArray.length; row++) {
      assertArrayEquals(keysArray[row], compare[row]);
    }
  }

  @Test
  public void testLoadTooMayHFiles() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("testLoadTooMayHFiles");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    for (int i = 0; i <= MAX_FILES_PER_REGION_PER_FAMILY; i++) {
      HFileTestUtil.createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_" + i),
        FAMILY, QUALIFIER, from, to, 1000);
    }

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    String[] args = { dir.toString(), "mytable_testLoadTooMayHFiles" };
    try {
      loader.run(args);
      fail("Bulk loading too many files should fail");
    } catch (IOException ie) {
      assertTrue(ie.getMessage()
          .contains("Trying to load more than " + MAX_FILES_PER_REGION_PER_FAMILY + " hfiles"));
    }
  }

  @Test(expected = TableNotFoundException.class)
  public void testWithoutAnExistingTableAndCreateTableSetToNo() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.set(LoadIncrementalHFiles.CREATE_TABLE_CONF_KEY, "no");
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    String[] args = { "directory", "nonExistingTable" };
    loader.run(args);
  }

  @Test
  public void testTableWithCFNameStartWithUnderScore() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("cfNameStartWithUnderScore");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    String family = "_cf";
    Path familyDir = new Path(dir, family);

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    Configuration conf = util.getConfiguration();
    String tableName = tn.getMethodName();
    Table table = util.createTable(TableName.valueOf(tableName), family);
    HFileTestUtil.createHFile(conf, fs, new Path(familyDir, "hfile"), Bytes.toBytes(family),
      QUALIFIER, from, to, 1000);

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    String[] args = { dir.toString(), tableName };
    try {
      loader.run(args);
      assertEquals(1000, util.countRows(table));
    } finally {
      if (null != table) {
        table.close();
      }
    }
  }

  @Test
  public void testBulkLoadByFamily() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("testBulkLoadByFamily");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    String tableName = tn.getMethodName();
    String[] families = { "cf1", "cf2", "cf3" };
    for (int i = 0; i < families.length; i++) {
      byte[] from = Bytes.toBytes(i + "begin");
      byte[] to = Bytes.toBytes(i + "end");
      Path familyDir = new Path(dir, families[i]);
      HFileTestUtil.createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile"),
        Bytes.toBytes(families[i]), QUALIFIER, from, to, 1000);
    }
    Table table = util.createTable(TableName.valueOf(tableName), families);
    final AtomicInteger attmptedCalls = new AtomicInteger();
    util.getConfiguration().setBoolean(BulkLoadHFiles.BULK_LOAD_HFILES_BY_FAMILY, true);
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration()) {
      @Override
      protected List<LoadQueueItem> tryAtomicRegionLoad(Connection connection, TableName tableName,
          final byte[] first, Collection<LoadQueueItem> lqis, boolean copyFile) throws IOException {
        attmptedCalls.incrementAndGet();
        return super.tryAtomicRegionLoad(connection, tableName, first, lqis, copyFile);
      }
    };

    String[] args = { dir.toString(), tableName };
    try {
      loader.run(args);
      assertEquals(families.length, attmptedCalls.get());
      assertEquals(1000 * families.length, util.countRows(table));
    } finally {
      if (null != table) {
        table.close();
      }
      util.getConfiguration().setBoolean(BulkLoadHFiles.BULK_LOAD_HFILES_BY_FAMILY, false);
    }
  }
}
