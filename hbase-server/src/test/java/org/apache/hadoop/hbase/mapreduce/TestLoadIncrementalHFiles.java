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
import java.util.Collection;
import java.util.TreeMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
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
  static final String EXPECTED_MSG_FOR_NON_EXISTING_FAMILY = "Unmatched family names found";
  static final int MAX_FILES_PER_REGION_PER_FAMILY = 4;

  private static final byte[][] SPLIT_KEYS = new byte[][] {
    Bytes.toBytes("ddd"),
    Bytes.toBytes("ppp")
  };

  public static int BLOCKSIZE = 64*1024;
  public static String COMPRESSION =
    Compression.Algorithm.NONE.getName();

  static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    util.getConfiguration().setInt(
      LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
      MAX_FILES_PER_REGION_PER_FAMILY);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * Test case that creates some regions and loads
   * HFiles that fit snugly inside those regions
   */
  @Test
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
  @Test
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
  @Test
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
  @Test
  public void testRegionCrossingRowColBloom() throws Exception {
    runTest("testRegionCrossingLoadRowColBloom", BloomType.ROWCOL,
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  private void runTest(String testName, BloomType bloomType,
          byte[][][] hfileRanges) throws Exception {
    Path dir = util.getDataTestDirOnTestFS(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), FAMILY, QUALIFIER, from, to, 1000);
    }
    int expectedRows = hfileIdx * 1000;

    final byte[] TABLE = Bytes.toBytes("mytable_"+testName);

    HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    familyDesc.setBloomFilterType(bloomType);
    htd.addFamily(familyDesc);
    admin.createTable(htd, SPLIT_KEYS);

    HTable table = new HTable(util.getConfiguration(), TABLE);
    util.waitTableEnabled(TABLE);
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    loader.doBulkLoad(dir, table);

    assertEquals(expectedRows, util.countRows(table));
  }

  /**
   * Test loading into a column family that does not exist.
   */
  @Test
  public void testNonexistentColumnFamilyLoad() throws Exception {
    String testName = "testNonexistentColumnFamilyLoad";
    byte[][][] hfileRanges = new byte[][][] {
      new byte[][]{ Bytes.toBytes("aaa"), Bytes.toBytes("ccc") },
      new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    };

    Path dir = util.getDataTestDirOnTestFS(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), FAMILY, QUALIFIER, from, to, 1000);
    }

    final byte[] TABLE = Bytes.toBytes("mytable_"+testName);

    HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    // set real family name to upper case in purpose to simulate the case that
    // family name in HFiles is invalid
    HColumnDescriptor family =
        new HColumnDescriptor(Bytes.toBytes(new String(FAMILY).toUpperCase()));
    htd.addFamily(family);
    admin.createTable(htd, SPLIT_KEYS);

    HTable table = new HTable(util.getConfiguration(), TABLE);
    util.waitTableEnabled(TABLE);
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration());
    try {
      loader.doBulkLoad(dir, table);
      assertTrue("Loading into table with non-existent family should have failed", false);
    } catch (Exception e) {
      assertTrue("IOException expected", e instanceof IOException);
      // further check whether the exception message is correct
      String errMsg = e.getMessage();
      assertTrue("Incorrect exception message, expected message: ["
          + EXPECTED_MSG_FOR_NON_EXISTING_FAMILY + "], current message: [" + errMsg + "]",
          errMsg.contains(EXPECTED_MSG_FOR_NON_EXISTING_FAMILY));
    }
    table.close();
    admin.close();
  }

  private void verifyAssignedSequenceNumber(String testName,
      byte[][][] hfileRanges, boolean nonZero) throws Exception {
    Path dir = util.getDataTestDir(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), FAMILY, QUALIFIER, from, to, 1000);
    }

    final byte[] TABLE = Bytes.toBytes("mytable_"+testName);

    HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    htd.addFamily(familyDesc);
    admin.createTable(htd, SPLIT_KEYS);

    HTable table = new HTable(util.getConfiguration(), TABLE);
    util.waitTableEnabled(TABLE);
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
      util.getConfiguration());

    // Do a dummy put to increase the hlog sequence number
    Put put = new Put(Bytes.toBytes("row"));
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("value"));
    table.put(put);

    loader.doBulkLoad(dir, table);

    // Get the store files
    Collection<StoreFile> files = util.getHBaseCluster().
        getRegions(TABLE).get(0).getStore(FAMILY).getStorefiles();
    for (StoreFile file: files) {
      // the sequenceId gets initialized during createReader
      file.createReader();

      if (nonZero)
        assertTrue(file.getMaxSequenceId() > 0);
      else
        assertTrue(file.getMaxSequenceId() == -1);
    }
  }

  @Test
  public void testSplitStoreFile() throws IOException {
    Path dir = util.getDataTestDirOnTestFS("testSplitHFile");
    FileSystem fs = util.getTestFileSystem();
    Path testIn = new Path(dir, "testhfile");
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    createHFile(util.getConfiguration(), fs, testIn, FAMILY, QUALIFIER,
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
        p.getFileSystem(conf), p, new CacheConfig(conf));
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


  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key.
   * TODO put me in an HFileTestUtil or something?
   */
  static void createHFile(
      Configuration conf,
      FileSystem fs, Path path,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException
  {
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path)
        .withBlockSize(BLOCKSIZE)
        .withCompression(COMPRESSION)
        .create();
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows-2)) {
        KeyValue kv = new KeyValue(key, family, qualifier, now, key);
        writer.append(kv);
      }
    } finally {
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(System.currentTimeMillis()));
      writer.close();
    }
  }

  private void addStartEndKeysForTest(TreeMap<byte[], Integer> map, byte[] first, byte[] last) {
    Integer value = map.containsKey(first)?(Integer)map.get(first):0;
    map.put(first, value+1);

    value = map.containsKey(last)?(Integer)map.get(last):0;
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

  @Test
  public void testLoadTooMayHFiles() throws Exception {
    Path dir = util.getDataTestDirOnTestFS("testLoadTooMayHFiles");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    for (int i = 0; i <= MAX_FILES_PER_REGION_PER_FAMILY; i++) {
      createHFile(util.getConfiguration(), fs, new Path(familyDir, "hfile_"
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
}

