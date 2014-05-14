/**
 * Copyright 2010 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TestStore;
import org.apache.hadoop.hbase.regionserver.TestStoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;

/**
 * Test cases for the "load" half of the HFileOutputFormat bulk load
 * functionality. These tests run faster than the full MR cluster
 * tests in TestHFileOutputFormat
 */
@Category(LargeTests.class)
public class TestLoadIncrementalHFiles {

  private static final byte[] TABLE = Bytes.toBytes("mytable");
  private static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  private static final byte[] FAMILY = Bytes.toBytes("myfam");

  private static final byte[][] SPLIT_KEYS = new byte[][] {
    Bytes.toBytes("ddd"),
    Bytes.toBytes("ppp")
  };

  public static int BLOCKSIZE = 64*1024;
  public static String COMPRESSION =
    Compression.Algorithm.NONE.getName();

  private HBaseTestingUtility util = new HBaseTestingUtility();

  /**
   * Test case that creates some regions and loads
   * HFiles that fit snugly inside those regions
   */
  @Test
  public void testSimpleLoad() throws Exception {
    runTest("testSimpleLoad",
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
    runTest("testRegionCrossingLoad",
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
          new byte[][]{ Bytes.toBytes("fff"), Bytes.toBytes("zzz") },
    });
  }

  /**
   * Test case that creates some regions and loads
   * HFiles that fit snugly inside those regions
   */
  @Test
  public void testBulkLoadSequenceNumber() throws Exception {
    util.getConfiguration().setBoolean(LoadIncrementalHFiles.ASSIGN_SEQ_IDS, true);
    verifyAssignedSequenceNumber("testBulkLoadSequenceNumber-WithSeqNum",
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    }, true);
  }
    
  @Test
  public void testBulkLoadSequenceNumberOld() throws Exception {
    util.getConfiguration().setBoolean(LoadIncrementalHFiles.ASSIGN_SEQ_IDS, false);
    verifyAssignedSequenceNumber("testBulkLoadSequenceNumber-WithoutSeqNum",
        new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
    }, false);
  }
  
  private void runTest(String testName, byte[][][] hfileRanges)
  throws Exception {
    Path dir = util.getTestDir(testName);
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


    util.startMiniCluster();
    try {
      HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      htd.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(htd, SPLIT_KEYS);

      HTable table = new HTable(util.getConfiguration(), TABLE);
      util.waitTableAvailable(TABLE, 30000);
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
          util.getConfiguration());
      loader.doBulkLoad(dir, table);

      assertEquals(expectedRows, util.countRows(table));
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test
  public void testLoadWithSeqNumNoFlush() throws Exception {
    testLoadWithSeqNum(false);
  }

  @Test
  public void testLoadWithSeqNumWithFlush() throws Exception {
    testLoadWithSeqNum(true);
  }

  private void testLoadWithSeqNum(boolean flush) throws Exception {
    String testName = "bulkLoadSensibility";
    Path dir1 = util.getTestDir(testName + "1");
    Path dir2 = util.getTestDir(testName + "2");
    FileSystem fs = util.getTestFileSystem();
    dir1 = dir1.makeQualified(fs);
    dir2 = dir2.makeQualified(fs);
    Path familyDir1 = new Path(dir1, Bytes.toString(FAMILY));
    Path familyDir2 = new Path(dir2, Bytes.toString(FAMILY));
    byte[] row = Bytes.toBytes("key");
    long timestamp = 1000;

    int hfileIdx = 0;
    Configuration config = util.getConfiguration();
    Log.info("Creating first bulkloaded file");
    createHFileForKV(config, fs, new Path(familyDir1, "hfile_"
        + hfileIdx++), row, FAMILY, QUALIFIER, timestamp, Bytes.toBytes("bulkValue"));

    util.startMiniCluster();
    try {
      HBaseAdmin admin = new HBaseAdmin(config);
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      htd.addFamily(new HColumnDescriptor(FAMILY));
      // Do not worry about splitting the keys
      admin.createTable(htd);

      HTable table = new HTable(config, TABLE);
      Log.info("Waiting for HTable to be available");
      util.waitTableAvailable(TABLE, 30000);

      // Do a dummy put to increase the hlog sequence number
      Log.info("Doing a htable.put()");
      Put put = new Put(row);
      put.add(FAMILY, QUALIFIER, timestamp, Bytes.toBytes("singlePutValue"));
      table.put(put);

      if (flush) {
        // flush the table
        Log.info("Flushing the table ");
        table.flushRegionForRow(row, 0);
      }

      config.setBoolean(LoadIncrementalHFiles.ASSIGN_SEQ_IDS, false);
      Log.info("Loading the first bulkload file");
      LoadIncrementalHFiles loader1 = new LoadIncrementalHFiles(
          config);
      loader1.doBulkLoad(dir1, table);

      Log.info("Getting after the first bulkloaded file");
      Get get = new Get(row);
      get.addColumn(FAMILY, QUALIFIER);
      get.setTimeStamp(timestamp);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap =
          result.getMap().get(FAMILY).get(QUALIFIER);
      assertEquals("singlePutValue", Bytes.toString(navigableMap.get(timestamp)));

      Log.info("Creating second bulkload file");
      createHFileForKV(config, fs, new Path(familyDir2, "hfile_"
          + hfileIdx++), row, FAMILY, QUALIFIER, timestamp, Bytes.toBytes("bulkValue2"));

      config.setBoolean(LoadIncrementalHFiles.ASSIGN_SEQ_IDS, true);
      LoadIncrementalHFiles loader2 = new LoadIncrementalHFiles(
          config);

      Log.info("Loading the second bulkload file");
      loader2.doBulkLoad(dir2, table);

      Log.info("Verifying get after 2nd bulk load");
      result = table.get(get);
      navigableMap =
          result.getMap().get(FAMILY).get(QUALIFIER);
      if (flush) {
        // no value in memstore
        // the latest bulk load file should win
        assertEquals("bulkValue2", Bytes.toString(navigableMap.get(timestamp)));
      } else {
        // The value in memstore wins
        assertEquals("singlePutValue", Bytes.toString(navigableMap.get(timestamp)));
      }
    } finally {
      util.shutdownMiniCluster();
    }

  }

  private void verifyAssignedSequenceNumber(String testName,
      byte[][][] hfileRanges, boolean nonZero) throws Exception {
    Path dir = util.getTestDir(testName);
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

    util.startMiniCluster();
    try {
      HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
      HTableDescriptor htd = new HTableDescriptor(TABLE);
      htd.addFamily(new HColumnDescriptor(FAMILY));
      // Do not worry about splitting the keys
      admin.createTable(htd);

      HTable table = new HTable(util.getConfiguration(), TABLE);
      util.waitTableAvailable(TABLE, 30000);
      
      // Do a dummy put to increase the hlog sequence number
      Put put = new Put(Bytes.toBytes("row"));
      put.add(FAMILY, QUALIFIER, Bytes.toBytes("value"));
      table.put(put);
      
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
          util.getConfiguration());
      loader.doBulkLoad(dir, table);

      // Get the store files
      List<StoreFile> files = TestStoreFile.getStoreFiles(
          util.getHBaseCluster().getRegions(TABLE).get(0).getStore(FAMILY));
      for (StoreFile file: files) {
        // the sequenceId gets initialized during createReader
        file.createReader();
        
        if (nonZero)
          assertTrue(file.getMaxSequenceId() > 0);
        else
          assertTrue(file.getMaxSequenceId() == -1);
      }
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test
  public void testSplitStoreFile() throws IOException {
    Path dir = util.getTestDir("testSplitHFile");
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
    HFileScanner scanner = reader.getScanner(false, false, false);
    scanner.seekTo();
    int count = 0;
    do {
      count++;
    } while (scanner.next());
    assertTrue(count > 0);
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
        .withComparator(KeyValue.KEY_COMPARATOR)
        .create();
    TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows-2)) {
        KeyValue kv = new KeyValue(key, family, qualifier, now, key);
        writer.append(kv);
        timeRangeTracker.includeTimestamp(kv);
      }
    } finally {
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(System.currentTimeMillis()));
      writer.appendFileInfo(StoreFile.TIMERANGE_KEY,
          WritableUtils.toByteArray(timeRangeTracker));
      writer.close();
    }
  }

  static void createHFileForKV(
      Configuration conf,
      FileSystem fs, Path path,
      byte[] key,
      byte[] family, byte[] qualifier,
      long timestamp, byte[] value) throws IOException
  {
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path)
        .withBlockSize(BLOCKSIZE)
        .withCompression(COMPRESSION)
        .withComparator(KeyValue.KEY_COMPARATOR)
        .create();
    TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
    try {
      KeyValue kv = new KeyValue(key, family, qualifier, timestamp, value);
      writer.append(kv);
      timeRangeTracker.includeTimestamp(kv);
    } finally {
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(System.currentTimeMillis()));
      writer.appendFileInfo(StoreFile.TIMERANGE_KEY,
          WritableUtils.toByteArray(timeRangeTracker));
      writer.close();
    }
  }
}
