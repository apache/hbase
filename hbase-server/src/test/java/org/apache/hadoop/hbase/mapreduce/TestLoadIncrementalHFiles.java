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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.SecureBulkLoadUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for the "load" half of the HFileOutputFormat bulk load
 * functionality. These tests run faster than the full MR cluster
 * tests in TestHFileOutputFormat
 */
@Category(LargeTests.class)
public class TestLoadIncrementalHFiles {
  private static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  private static final byte[] FAMILY = Bytes.toBytes("myfam");
  private static final String NAMESPACE = "bulkNS";

  static final String EXPECTED_MSG_FOR_NON_EXISTING_FAMILY = "Unmatched family names found";
  static final int MAX_FILES_PER_REGION_PER_FAMILY = 4;

  private static final byte[][] SPLIT_KEYS = new byte[][] {
    Bytes.toBytes("ddd"),
    Bytes.toBytes("ppp")
  };

  static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,"");
    util.getConfiguration().setInt(
      LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
      MAX_FILES_PER_REGION_PER_FAMILY);
    util.startMiniCluster();

    setupNamespace();
  }

  protected static void setupNamespace() throws Exception {
    util.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * Test case that creates some regions and loads
   * HFiles that fit snugly inside those regions
   */
  @Test(timeout = 60000)
  public void testSimpleLoad() throws Exception {
    runTest("testSimpleLoad", BloomType.NONE,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    });
  }

  /**
   * Test case that creates some regions and loads
   * HFiles that cross the boundaries of those regions
   */
  @Test(timeout = 60000)
  public void testRegionCrossingLoad() throws Exception {
    runTest("testRegionCrossingLoad", BloomType.NONE,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  /**
   * Test loading into a column family that has a ROW bloom filter.
   */
  @Test(timeout = 60000)
  public void testRegionCrossingRowBloom() throws Exception {
    runTest("testRegionCrossingLoadRowBloom", BloomType.ROW,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  /**
   * Test loading into a column family that has a ROWCOL bloom filter.
   */
  @Test(timeout = 60000)
  public void testRegionCrossingRowColBloom() throws Exception {
    runTest("testRegionCrossingLoadRowColBloom", BloomType.ROWCOL,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  /**
   * Test case that creates some regions and loads HFiles that have
   * different region boundaries than the table pre-split.
   */
  @Test(timeout = 60000)
  public void testSimpleHFileSplit() throws Exception {
    runTest("testHFileSplit", BloomType.NONE,
        new byte[][] {
          Bytes.toBytes("aaa"), Bytes.toBytes("fff"), Bytes.toBytes("jjj"),
          Bytes.toBytes("ppp"), Bytes.toBytes("uuu"), Bytes.toBytes("zzz"),
        },
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("lll") },
          new byte[][]{ Bytes.toBytes("mmm"), Bytes.toBytes("zzz") },
        }
    );
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries
   * and have different region boundaries than the table pre-split.
   */
  @Test(timeout = 60000)
  public void testRegionCrossingHFileSplit() throws Exception {
    testRegionCrossingHFileSplit(BloomType.NONE);
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries
   * have a ROW bloom filter and a different region boundaries than the table pre-split.
   */
  @Test(timeout = 60000)
  public void testRegionCrossingHFileSplitRowBloom() throws Exception {
    testRegionCrossingHFileSplit(BloomType.ROW);
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries
   * have a ROWCOL bloom filter and a different region boundaries than the table pre-split.
   */
  @Test(timeout = 60000)
  public void testRegionCrossingHFileSplitRowColBloom() throws Exception {
    testRegionCrossingHFileSplit(BloomType.ROWCOL);
  }

  private void testRegionCrossingHFileSplit(BloomType bloomType) throws Exception {
    runTest("testHFileSplit" + bloomType + "Bloom", bloomType,
        new byte[][] {
          Bytes.toBytes("aaa"), Bytes.toBytes("fff"), Bytes.toBytes("jjj"),
          Bytes.toBytes("ppp"), Bytes.toBytes("uuu"), Bytes.toBytes("zzz"),
        },
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
        }
    );
  }

  private void runTest(String testName, BloomType bloomType,
      byte[][][] hfileRanges) throws Exception {
    runTest(testName, bloomType, null, hfileRanges);
  }

  private void runTest(String testName, BloomType bloomType,
      byte[][] tableSplitKeys, byte[][][] hfileRanges) throws Exception {
    final byte[] TABLE_NAME = Bytes.toBytes("mytable_"+testName);
    final boolean preCreateTable = tableSplitKeys != null;

    // Run the test bulkloading the table to the default namespace
    final TableName TABLE_WITHOUT_NS = TableName.valueOf(TABLE_NAME);
    runTest(testName, TABLE_WITHOUT_NS, bloomType, preCreateTable, tableSplitKeys, hfileRanges);

    // Run the test bulkloading the table to the specified namespace
    final TableName TABLE_WITH_NS = TableName.valueOf(Bytes.toBytes(NAMESPACE), TABLE_NAME);
    runTest(testName, TABLE_WITH_NS, bloomType, preCreateTable, tableSplitKeys, hfileRanges);
  }

  private void runTest(String testName, TableName tableName, BloomType bloomType,
      boolean preCreateTable, byte[][] tableSplitKeys, byte[][][] hfileRanges) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    familyDesc.setBloomFilterType(bloomType);
    htd.addFamily(familyDesc);
    runTest(testName, htd, bloomType, preCreateTable, tableSplitKeys, hfileRanges);
  }

  private void runTest(String testName, HTableDescriptor htd, BloomType bloomType,
      boolean preCreateTable, byte[][] tableSplitKeys, byte[][][] hfileRanges) throws Exception {
    Path dir = util.getDataTestDirOnTestFS(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), FAMILY, QUALIFIER, from, to, 1000);
    }
    int expectedRows = hfileIdx * 1000;

    if (preCreateTable) {
      util.getHBaseAdmin().createTable(htd, tableSplitKeys);
    }

    final TableName tableName = htd.getTableName();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    String [] args= {dir.toString(), tableName.toString()};
    loader.run(args);

    Table table = new HTable(util.getConfiguration(), tableName);
    try {
      assertEquals(expectedRows, util.countRows(table));
    } finally {
      table.close();
    }

    // verify staging folder has been cleaned up
    Path stagingBasePath = SecureBulkLoadUtil.getBaseStagingDir(util.getConfiguration());
    if(fs.exists(stagingBasePath)) {
      FileStatus[] files = fs.listStatus(stagingBasePath);
      for(FileStatus file : files) {
        assertTrue("Folder=" + file.getPath() + " is not cleaned up.",
          file.getPath().getName() != "DONOTERASE");
      }
    }
    
    util.deleteTable(tableName);
  }

  /**
   * Test loading into a column family that does not exist.
   */
  @Test(timeout = 60000)
  public void testNonexistentColumnFamilyLoad() throws Exception {
    String testName = "testNonexistentColumnFamilyLoad";
    byte[][][] hFileRanges = new byte[][][] {
      new byte[][]{ Bytes.toBytes("aaa"), Bytes.toBytes("ccc") },
      new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    };

    final byte[] TABLE = Bytes.toBytes("mytable_"+testName);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    // set real family name to upper case in purpose to simulate the case that
    // family name in HFiles is invalid
    HColumnDescriptor family =
        new HColumnDescriptor(Bytes.toBytes(new String(FAMILY).toUpperCase()));
    htd.addFamily(family);

    try {
      runTest(testName, htd, BloomType.NONE, true, SPLIT_KEYS, hFileRanges);
      assertTrue("Loading into table with non-existent family should have failed", false);
    } catch (Exception e) {
      assertTrue("IOException expected", e instanceof IOException);
      // further check whether the exception message is correct
      String errMsg = e.getMessage();
      assertTrue("Incorrect exception message, expected message: ["
          + EXPECTED_MSG_FOR_NON_EXISTING_FAMILY + "], current message: [" + errMsg + "]",
          errMsg.contains(EXPECTED_MSG_FOR_NON_EXISTING_FAMILY));
    }
  }

  @Test(timeout = 60000)
  public void testSplitStoreFile() throws IOException {
    Path dir = util.getDataTestDirOnTestFS("testSplitHFile");
    FileSystem fs = util.getTestFileSystem();
    Path testIn = new Path(dir, "testhfile");
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    HFileTestUtil.createHFile(util.getConfiguration(), fs, testIn, FAMILY, QUALIFIER,
        Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 1000);

    Path bottomOut = new Path(dir, "bottom.out");
    Path topOut = new Path(dir, "top.out");

    LoadIncrementalHFiles.splitStoreFile(
        util.getConfiguration(), testIn,
        familyDesc, Bytes.toBytes("ggg"),
        bottomOut,
        topOut);

    int rowCount = verifyHFile(bottomOut);
    rowCount += verifyHFile(topOut);
    assertEquals(1000, rowCount);
  }

  private int verifyHFile(Path p) throws IOException {
    Configuration conf = util.getConfiguration();
    HFile.Reader reader = HFile.createReader(
        p.getFileSystem(conf), p, new CacheConfig(conf), conf);
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
    Integer value = map.containsKey(first)?map.get(first):0;
    map.put(first, value+1);

    value = map.containsKey(last)?map.get(last):0;
    map.put(last, value-1);
  }

  @Test
  public void testInferBoundaries() {
    TreeMap<byte[], Integer> map = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

    /* Toy example
     *     c---------i            o------p          s---------t     v------x
     * a------e    g-----k   m-------------q   r----s            u----w
     *
     * Should be inferred as:
     * a-----------------k   m-------------q   r--------------t  u---------x
     *
     * The output should be (m,r,u)
     */

    String first;
    String last;

    first = "a"; last = "e";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "r"; last = "s";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "o"; last = "p";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "g"; last = "k";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "v"; last = "x";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "c"; last = "i";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "m"; last = "q";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "s"; last = "t";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    first = "u"; last = "w";
    addStartEndKeysForTest(map, first.getBytes(), last.getBytes());

    byte[][] keysArray = LoadIncrementalHFiles.inferBoundaries(map);
    byte[][] compare = new byte[3][];
    compare[0] = "m".getBytes();
    compare[1] = "r".getBytes();
    compare[2] = "u".getBytes();

    assertEquals(keysArray.length, 3);

    for (int row = 0; row<keysArray.length; row++){
      assertArrayEquals(keysArray[row], compare[row]);
    }
  }

  @Test(timeout = 60000)
  public void testLoadTooMayHFiles() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("testLoadTooMayHFiles");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    for (int i = 0; i <= MAX_FILES_PER_REGION_PER_FAMILY; i++) {
      HFileTestUtil.createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + i), FAMILY, QUALIFIER, from, to, 1000);
    }

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    String [] args= {dir.toString(), "mytable_testLoadTooMayHFiles"};
    try {
      loader.run(args);
      fail("Bulk loading too many files should fail");
    } catch (IOException ie) {
      assertTrue(ie.getMessage().contains("Trying to load more than "
        + MAX_FILES_PER_REGION_PER_FAMILY + " hfiles"));
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

  @Test(timeout = 60000)
  public void testTableWithCFNameStartWithUnderScore() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("cfNameStartWithUnderScore");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    String family = "_cf";
    Path familyDir = new Path(dir, family);

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    Configuration conf = util.getConfiguration();
    String tableName = "mytable_cfNameStartWithUnderScore";
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
}

