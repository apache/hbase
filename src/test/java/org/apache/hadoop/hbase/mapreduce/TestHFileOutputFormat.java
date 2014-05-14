/**
 * Copyright 2009 The Apache Software Foundation
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

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.UnstableTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.master.RegionPlacement;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Simple test for {@link KeyValueSortReducer} and {@link HFileOutputFormat}.
 * Sets up and runs a mapreduce job that writes hfile output.
 * Creates a few inner classes to implement splits and an inputformat that
 * emits keys and values like those of {@link PerformanceEvaluation}.  Makes
 * as many splits as "mapred.map.tasks" maps.
 */
@Category(LargeTests.class)
public class TestHFileOutputFormat  {
  private final static int ROWSPERSPLIT = 1024;

  private static final byte[][] FAMILIES
    = { Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-A"))
      , Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-B"))
      , Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-C"))};
  private static final StringBytes TABLE_NAME = new StringBytes("TestTable");
  private static final String oldValue = "valAAAAA";
  private static final String newValue = "valBBBBB";

  private HBaseTestingUtility util = new HBaseTestingUtility();

  private static Log LOG = LogFactory.getLog(TestHFileOutputFormat.class);

  /**
   * Simple mapper that makes KeyValue output.
   */
  static class RandomKVGeneratingMapper
  extends Mapper<NullWritable, NullWritable,
                 ImmutableBytesWritable, KeyValue> {

    private int keyLength;
    private static final int KEYLEN_DEFAULT=10;
    private static final String KEYLEN_CONF="randomkv.key.length";

    private int valLength;
    private static final int VALLEN_DEFAULT=10;
    private static final String VALLEN_CONF="randomkv.val.length";

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
    }

    @Override
    protected void map(
        NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable,
               ImmutableBytesWritable,KeyValue>.Context context)
        throws java.io.IOException ,InterruptedException
    {
      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

      Random random = new Random();
      for (int i = 0; i < ROWSPERSPLIT; i++) {

        random.nextBytes(keyBytes);
        // Ensure that unique tasks generate unique keys
        keyBytes[keyLength - 1] = (byte)(taskId & 0xFF);
        random.nextBytes(valBytes);
        ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

        for (byte[] family : TestHFileOutputFormat.FAMILIES) {
          KeyValue kv = new KeyValue(keyBytes, family,
              PerformanceEvaluation.QUALIFIER_NAME, valBytes);
          context.write(key, kv);
        }
      }
    }
  }

  private void setupRandomGeneratorMapper(Job job) {
    job.setInputFormatClass(NMapInputFormat.class);
    job.setMapperClass(RandomKVGeneratingMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
  }

  static class RowSorterMapper
 extends
      Mapper<NullWritable, NullWritable, ImmutableBytesWritable, RowMutation> {
    @Override
    protected void map(
        NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable,
               ImmutableBytesWritable, RowMutation>.Context context)
    throws IOException ,InterruptedException
 {
      byte[] row = Bytes.toBytes("row1");

      // need one for every task...
      byte[] key = Bytes.toBytes("key");

      byte[] col1 = Bytes.toBytes("col1");
      byte[] col2 = Bytes.toBytes("col2");
      byte[] col3 = Bytes.toBytes("col3");
      byte[] col4 = Bytes.toBytes("col4");

      // PUT cf=info-A
      Row put1 = new Put(row).add(TestHFileOutputFormat.FAMILIES[0], col1, 10,
          Bytes.toBytes("val10"));
      Row put2 = new Put(row).add(TestHFileOutputFormat.FAMILIES[0], col2, 11,
          Bytes.toBytes("val11"));

      // PUT cf=info-B
      Row put3 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col1, 20,
          Bytes.toBytes("val20"));
      Row put4 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col2, 21,
          Bytes.toBytes("val21"));

      // PUT cf=info-B
      Row put5 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col3, 30,
          Bytes.toBytes("val30"));
      Row put6 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col3, 31,
          Bytes.toBytes("val31"));
      Row put7 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col3, 32,
          Bytes.toBytes("val32"));

      Row put8 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col4, 1,
          Bytes.toBytes(TestHFileOutputFormat.oldValue));
      Row put9 = new Put(row).add(TestHFileOutputFormat.FAMILIES[1], col4, 1,
          Bytes.toBytes(TestHFileOutputFormat.newValue));

      // DELETEs
      Row del1 = new Delete(row).deleteColumn(
          TestHFileOutputFormat.FAMILIES[1], col2, 21);

      Row del2 = new Delete(row)
          .deleteFamily(TestHFileOutputFormat.FAMILIES[0]);

      Row del3 = new Delete(row).deleteColumns(
          TestHFileOutputFormat.FAMILIES[1], col3);

      context.write(new ImmutableBytesWritable(key), new RowMutation(put1));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put2));

      context.write(new ImmutableBytesWritable(key), new RowMutation(put3));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put4));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put5));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put6));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put7));

      // testing for sequence of writes
      context.write(new ImmutableBytesWritable(key), new RowMutation(put8));
      context.write(new ImmutableBytesWritable(key), new RowMutation(put9));

      context.write(new ImmutableBytesWritable(key), new RowMutation(del1));
      context.write(new ImmutableBytesWritable(key), new RowMutation(del2));
      context.write(new ImmutableBytesWritable(key), new RowMutation(del3));
    }
  }

  static class SimpleKVMapper
      extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, KeyValue> {

    static final String testKey = "testKey";
    static final String testValue = "testValue";

    @Override
    protected void map (
        NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable,
            ImmutableBytesWritable, KeyValue>.Context context)
        throws IOException ,InterruptedException
    {
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

      String taskIdString = context.getTaskAttemptID().getTaskID().toString();
      byte[] keyBytes = Bytes.toBytes(testKey + taskIdString);
      byte[] valBytes = Bytes.toBytes(testValue);
      ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

      for (byte[] family : TestHFileOutputFormat.FAMILIES) {
        KeyValue kv = new KeyValue(keyBytes, family,
            PerformanceEvaluation.QUALIFIER_NAME, valBytes);
        context.write(key, kv);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      HTable table = new HTable(conf, TABLE_NAME);

      Path outputPath = FileOutputFormat.getOutputPath(context);
      FileSystem fs = outputPath.getFileSystem(conf);
      Path workOutputPath = FileOutputFormat.getWorkOutputPath(context).makeQualified(fs);

      // Force flushing HFile into working directory
      HFileOutputFormat.closeWriter(context);

      new LoadIncrementalHFiles(conf).doBulkLoad(workOutputPath, table);
    }
  };

  /**
   * Test for the union style MR jobs that runs both Put and Delete requests
   * @throws Exception on job, sorting, IO or fs errors
   */
  @Test
  public void testRowSortReducer()
  throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    conf.setInt("io.sort.mb", 20);
    conf.setInt("mapred.map.tasks", 1);

    Path dir = util.getTestDir("testRowSortReducer");

    try {
      Job job = new Job(conf);

      job.setInputFormatClass(NMapInputFormat.class);
      job.setOutputFormatClass(HFileOutputFormat.class);

      job.setNumReduceTasks(1);

      job.setMapperClass(RowSorterMapper.class); // local
      job.setReducerClass(RowMutationSortReducer.class);

      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(KeyValue.class);

      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(RowMutation.class);

      FileOutputFormat.setOutputPath(job, dir);

      assertTrue(job.waitForCompletion(false));

      FileSystem fs = dir.getFileSystem(conf);

      for (FileStatus status : fs.listStatus(dir)) {
        if (status.isDir()) {
          String cf = status.getPath().getName();

          if (Bytes.toString(TestHFileOutputFormat.FAMILIES[0]).equals(cf)
              || Bytes.toString(TestHFileOutputFormat.FAMILIES[1]).equals(cf)) {
            for (FileStatus stat : fs.listStatus(status.getPath())) {
              Reader r = HFile.createReader(fs, stat.getPath(),
                  new CacheConfig(conf));
              HFileScanner scanner = r.getScanner(false, false, false);
              scanner.seekTo();

              int index = 0;

              // check things for info-A
              if (Bytes.toString(TestHFileOutputFormat.FAMILIES[0]).equals(cf)) {
                do {
                  ++index;

                  KeyValue kv = scanner.getKeyValue();
                  long ts = kv.getTimestamp();

                  switch (index) {
                  case 1:
                    assertTrue(ts <= System.currentTimeMillis());
                    assertEquals(KeyValue.Type.DeleteFamily.getCode(),
                        kv.getType());
                    break;
                  case 2:
                    assertEquals(10, ts);
                    break;
                  case 3:
                    assertEquals(11, ts);
                    break;
                  default:
                    fail("Invalid KeyValue " + kv + " found in HFile "
                        + stat.getPath());
                    break;
                  }
                } while (scanner.next());
                // the default takes care of index being greater than expected,
                // but we need one for failed writes, where index would be
                // smaller
                assertEquals(3, index);

              }

              // check things for info-B
              if (Bytes.toString(TestHFileOutputFormat.FAMILIES[1]).equals(cf)) {
                do {
                  ++index;

                  KeyValue kv = scanner.getKeyValue();
                  long ts = kv.getTimestamp();

                  switch (index) {
                  case 1:
                    assertEquals(20, ts);
                    break;
                  case 2:
                    assertEquals(21, ts);
                    assertEquals(KeyValue.Type.Delete.getCode(), kv.getType());
                    break;
                  case 3:
                    assertEquals(21, ts);
                    assertEquals(KeyValue.Type.Put.getCode(), kv.getType());
                    break;
                  case 4:
                    assertTrue(ts <= System.currentTimeMillis());
                    assertEquals(KeyValue.Type.DeleteColumn.getCode(),
                        kv.getType());
                    break;
                  case 5:
                    assertEquals(32, ts);
                    break;
                  case 6:
                    assertEquals(31, ts);
                    break;
                  case 7:
                    assertEquals(30, ts);
                    break;
                  case 8:
                    assertEquals(1, ts);
                    assertEquals(TestHFileOutputFormat.newValue,
                        Bytes.toString(kv.getValue()));
                    break;
                  case 9:
                    assertEquals(TestHFileOutputFormat.oldValue,
                        Bytes.toString(kv.getValue()));
                    break;
                  default:
                    fail("Invalid KeyValue " + kv + " found in HFile "
                        + stat.getPath());
                    break;
                  }
                } while (scanner.next());
                // the default takes care of index being greater than expected,
                // but we need one for failed writes, where index would be
                // smaller
                assertEquals(9, index);
              }
            }
          }
        }
      }

    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  /**
   * Test that {@link HFileOutputFormat} RecordWriter amends timestamps if
   * passed a keyvalue whose timestamp is {@link HConstants#LATEST_TIMESTAMP}.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2615">HBASE-2615</a>
   */
  @Test
  public void test_LATEST_TIMESTAMP_isReplaced()
  throws IOException, InterruptedException {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      util.getTestDir("test_LATEST_TIMESTAMP_isReplaced");
    try {
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
        new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);
      final byte [] b = Bytes.toBytes("b");

      // Test 1.  Pass a KV that has a ts of LATEST_TIMESTAMP.  It should be
      // changed by call to write.  Check all in kv is same but ts.
      KeyValue kv = new KeyValue(b, b, b);
      KeyValue original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertFalse(original.equals(kv));
      assertTrue(Bytes.equals(original.getRow(), kv.getRow()));
      assertTrue(original.matchingColumn(kv.getFamily(), kv.getQualifier()));
      assertNotSame(original.getTimestamp(), kv.getTimestamp());
      assertNotSame(HConstants.LATEST_TIMESTAMP, kv.getTimestamp());

      // Test 2. Now test passing a kv that has explicit ts.  It should not be
      // changed by call to record write.
      kv = new KeyValue(b, b, b, kv.getTimestamp() - 1, b);
      original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertTrue(original.equals(kv));
    } finally {
      if (writer != null && context != null) writer.close(context);
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  /**
   * Run small MR job.
   */
  @Test
  public void testWritingPEData() throws Exception {
    Configuration conf = util.getConfiguration();
    Path testDir = util.getTestDir("testWritingPEData");
    FileSystem fs = testDir.getFileSystem(conf);

    // Set down this value or we OOME in eclipse.
    conf.setInt("io.sort.mb", 20);
    // Write a few files.
    conf.setLong("hbase.hregion.max.filesize", 64 * 1024);

    Job job = new Job(conf, "testWritingPEData");
    setupRandomGeneratorMapper(job);
    // This partitioner doesn't work well for number keys but using it anyways
    // just to demonstrate how to configure it.
    byte[] startKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];
    byte[] endKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];

    Arrays.fill(startKey, (byte)0);
    Arrays.fill(endKey, (byte)0xff);

    job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
    // Set start and end rows for partitioner.
    SimpleTotalOrderPartitioner.setStartKey(job.getConfiguration(), startKey);
    SimpleTotalOrderPartitioner.setEndKey(job.getConfiguration(), endKey);
    job.setReducerClass(KeyValueSortReducer.class);
    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setNumReduceTasks(4);

    FileOutputFormat.setOutputPath(job, testDir);
    assertTrue(job.waitForCompletion(false));
    FileStatus [] files = fs.listStatus(testDir);
    assertTrue(files.length > 0);
  }

  @Test
  public void testJobConfiguration() throws Exception {
    Job job = new Job();
    HTable table = Mockito.mock(HTable.class);
    setupMockStartKeys(table);
    HFileOutputFormat.configureIncrementalLoad(job, table);
    assertEquals(job.getNumReduceTasks(), 4);
  }

  private byte [][] generateRandomSplitKeys(int numSplits) {
    Random random = new Random();
    byte[][] ret = new byte[numSplits][];
    for (int i = 0; i < numSplits; i++) {
      ret[i] = PerformanceEvaluation.generateValue(random);
    }
    return ret;
  }

  @Test
  public void testMRIncrementalLoad() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoad\n");
    doIncrementalLoadTest(false);
  }

  @Test
  public void testMRIncrementalLoadWithSplit() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoadWithSplit\n");
    doIncrementalLoadTest(true);
  }

  private void doIncrementalLoadTest(
      boolean shouldChangeRegions) throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    Path testDir = util.getTestDir("testLocalMRIncrementalLoad");

    try {
      util.startMiniCluster();
      HBaseAdmin admin = new HBaseAdmin(conf);
      byte[][] splitKeys = generateRandomSplitKeys(4);
      HTable table = util.createTable(TABLE_NAME, FAMILIES, 3, splitKeys);
      assertEquals("Should start with empty table",
          0, util.countRows(table));

      // Generate the bulk load files
      util.startMiniMapReduceCluster();
      runIncrementalPELoad(conf, table, testDir);
      // This doesn't write into the table, just makes files
      assertEquals("HFOF should not touch actual table",
          0, util.countRows(table));

      // Make sure that a directory was created for every CF
      int dir = 0;
      for (FileStatus f : testDir.getFileSystem(conf).listStatus(testDir)) {
        for (byte[] family : FAMILIES) {
          if (Bytes.toString(family).equals(f.getPath().getName())) {
            ++dir;
          }
        }
      }
      assertEquals("Column family not found in FS.", FAMILIES.length, dir);

      // handle the split case
      if (shouldChangeRegions) {
        LOG.info("Changing regions in table");
        admin.disableTable(table.getTableName());
        admin.deleteTable(table.getTableName());

        byte[][] newSplitKeys = generateRandomSplitKeys(14);
        table = util.createTable(TABLE_NAME, FAMILIES, 3, newSplitKeys);
      }

      // Perform the actual load
      new LoadIncrementalHFiles(conf).doBulkLoad(testDir, table);

      // Ensure data shows up
      int expectedRows = conf.getInt("mapred.map.tasks", 1) * ROWSPERSPLIT;
      assertEquals("LoadIncrementalHFiles should put expected data in table",
          expectedRows, util.countRows(table));
      Scan scan = new Scan();
      ResultScanner results = table.getScanner(scan);
      for (Result res : results) {
        assertEquals(FAMILIES.length, res.raw().length);
        KeyValue first = res.raw()[0];
        for (KeyValue kv : res.raw()) {
          assertTrue(KeyValue.COMPARATOR.matchingRows(first, kv));
          assertTrue(Bytes.equals(first.getValue(), kv.getValue()));
        }
      }
      results.close();
      String tableDigestBefore = util.checksumRows(table);

      // Cause regions to reopen
      admin.disableTable(TABLE_NAME.getBytes());
      while (table.getRegionsInfo().size() != 0) {
        Thread.sleep(1000);
        LOG.info("Waiting for table to disable");
      }
      admin.enableTable(TABLE_NAME.getBytes());
      util.waitTableAvailable(TABLE_NAME.getBytes(), 30000);

      assertEquals("Data should remain after reopening of regions",
          tableDigestBefore, util.checksumRows(table));
    } finally {
      util.shutdownMiniMapReduceCluster();
      util.shutdownMiniCluster();
    }
  }

  private void runIncrementalPELoad(
      Configuration conf, HTable table, Path outDir)
  throws Exception {
    Job job = new Job(conf, "testLocalMRIncrementalLoad");
    setupRandomGeneratorMapper(job);
    HFileOutputFormat.configureIncrementalLoad(job, table);
    FileOutputFormat.setOutputPath(job, outDir);

    assertEquals(table.getRegionsInfo().size(),
        job.getNumReduceTasks());

    assertTrue(job.waitForCompletion(true));
  }

  /**
   * Test for uploading map output in cleanup stage of each task
   * @throws Exception
   */
  @Test
  public void testUploadByTask() throws Exception {
    try {
      MiniHBaseCluster cluster = util.startMiniCluster();
      cluster.getMaster();
      HTable table = util.createTable(TABLE_NAME.getBytes(), FAMILIES);
      Configuration conf = table.getConfiguration() ;
      Path testDir = util.getTestDir("testUploadByTask");

      // Generate the bulk load files
      util.startMiniMapReduceCluster();

      Job job = new Job(conf, "testUploadByTask");
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(SimpleKVMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
      job.setOutputFormatClass(HFileOutputFormat.class);
      job.setNumReduceTasks(0);
      HFileOutputFormat.configAsMapOutputFormat(job, table);
      FileOutputFormat.setOutputPath(job, testDir);
      assertTrue(job.waitForCompletion(true));

      // Ensure data shows up
      int expectedRows = conf.getInt("mapred.map.tasks", 1);
      assertEquals("LoadIncrementalHFiles should put a row per task",
          expectedRows, util.countRows(table));
      Scan scan = new Scan();
      ResultScanner results = table.getScanner(scan);
      for (Result res : results) {
        assertEquals(FAMILIES.length, res.raw().length);
        for (KeyValue kv : res.raw()) {
          assertTrue("Key should start with pre-defined test key",
              Bytes.toStringBinary(kv.getRow()).startsWith(SimpleKVMapper.testKey));
          assertEquals("Value should equal to pre-defined value",
              SimpleKVMapper.testValue, Bytes.toString(kv.getValue()));
        }
      }
      results.close();
    } finally {
      util.shutdownMiniMapReduceCluster();
      util.shutdownMiniCluster();
    }
  }

  /**
   * Test for
   * {@link HFileOutputFormat#createFamilyCompressionMap(Configuration)}. Tests
   * that the compression map is correctly deserialized from configuration
   *
   * @throws IOException
   */
  @Test
  public void testCreateFamilyCompressionMap() throws IOException {
    for (int numCfs = 0; numCfs <= 3; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, Compression.Algorithm> familyToCompression = getMockColumnFamilies(numCfs);
      HTable table = Mockito.mock(HTable.class);
      setupMockColumnFamilies(table, familyToCompression);
      HFileOutputFormat.configureCompression(table, conf);

      // read back family specific compression setting from the configuration
      Map<byte[], String> retrievedFamilyToCompressionMap = HFileOutputFormat.createFamilyCompressionMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, Algorithm> entry : familyToCompression.entrySet()) {
        assertEquals("Compression configuration incorrect for column family:" + entry.getKey(), entry.getValue()
                     .getName(), retrievedFamilyToCompressionMap.get(entry.getKey().getBytes()));
      }
    }
  }

  private void setupMockColumnFamilies(HTable table,
    Map<String, Compression.Algorithm> familyToCompression) throws IOException
  {
    HTableDescriptor mockTableDescriptor =
        new HTableDescriptor(TABLE_NAME.getBytes());
    for (Entry<String, Compression.Algorithm> entry : familyToCompression.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey())
          .setMaxVersions(1)
          .setCompressionType(entry.getValue())
          .setBlockCacheEnabled(false)
          .setTimeToLive(0));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  private void setupMockStartKeys(HTable table) throws IOException {
    byte[][] mockKeys = new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY,
        Bytes.toBytes("aaa"),
        Bytes.toBytes("ggg"),
        Bytes.toBytes("zzz")
    };
    Mockito.doReturn(mockKeys).when(table).getStartKeys();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, Compression.Algorithm> getMockColumnFamilies(int numCfs) {
    Map<String, Compression.Algorithm> familyToCompression = new HashMap<String, Compression.Algorithm>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToCompression.put("Family1!@#!@#&", Compression.Algorithm.LZO);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family2=asdads&!AASD", Compression.Algorithm.GZ);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family3", Compression.Algorithm.NONE);
    }
    return familyToCompression;
  }

  /**
   * Test that {@link HFileOutputFormat} RecordWriter uses compression settings
   * from the column family descriptor
   */
  @Test
  public void testColumnFamilyCompression()
      throws IOException, InterruptedException {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
        util.getTestDir("testColumnFamilyCompression");

    HTable table = Mockito.mock(HTable.class);

    Map<String, Compression.Algorithm> configuredCompression =
      new HashMap<String, Compression.Algorithm>();
    Compression.Algorithm[] supportedAlgos = getSupportedCompressionAlgorithms();

    int familyIndex = 0;
    for (byte[] family : FAMILIES) {
      configuredCompression.put(Bytes.toString(family),
                                supportedAlgos[familyIndex++ % supportedAlgos.length]);
    }
    setupMockColumnFamilies(table, configuredCompression);

    // set up the table to return some mock keys
    setupMockStartKeys(table);

    try {
      // partial map red setup to get an operational writer for testing
      // We turn off the sequence file compression, because DefaultCodec
      // pollutes the GZip codec pool with an incompatible compressor.
      conf.set("io.seqfile.compression.type", "NONE");
      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      setupRandomGeneratorMapper(job);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
          new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeRandomKeyValues(writer, context, ROWSPERSPLIT);
      writer.close(context);

      // Make sure that a directory was created for every CF
      FileSystem fileSystem = dir.getFileSystem(conf);

      // commit so that the filesystem has one directory per column family
      hof.getOutputCommitter(context).commitTask(context);
      for (byte[] family : FAMILIES) {
        String familyStr = new String(family);
        boolean found = false;
        for (FileStatus f : fileSystem.listStatus(dir)) {

          if (Bytes.toString(family).equals(f.getPath().getName())) {
            // we found a matching directory
            found = true;

            // verify that the compression on this file matches the configured
            // compression
            Path dataFilePath = fileSystem.listStatus(f.getPath())[0].getPath();
            Reader reader = HFile.createReader(fileSystem, dataFilePath,
                new CacheConfig(conf));
            reader.loadFileInfo();
            assertEquals("Incorrect compression used for column family " + familyStr
                         + "(reader: " + reader + ")",
                         configuredCompression.get(familyStr), reader.getCompressionAlgorithm());
            break;
          }
        }

        if (!found) {
          fail("HFile for column family " + familyStr + " not found");
        }
      }

    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }


  /**
   * @return
   */
  private Compression.Algorithm[] getSupportedCompressionAlgorithms() {
    String[] allAlgos = HFile.getSupportedCompressionAlgorithms();
    List<Compression.Algorithm> supportedAlgos = Lists.newArrayList();

    for (String algoName : allAlgos) {
      try {
        Compression.Algorithm algo = Compression.getCompressionAlgorithmByName(algoName);
        algo.getCompressor();
        supportedAlgos.add(algo);
      }catch (Exception e) {
        // this algo is not available
      }
    }

    return supportedAlgos.toArray(new Compression.Algorithm[0]);
  }

  private void setupColumnFamiliesBloomType(HTable table,
    Map<String, BloomType> familyToBloom) throws IOException
  {
    HTableDescriptor mockTableDescriptor =
        new HTableDescriptor(TABLE_NAME.getBytes());
    for (Entry<String, BloomType> entry : familyToBloom.entrySet()) {
      mockTableDescriptor.addFamily(
          new HColumnDescriptor(entry.getKey().getBytes())
              .setMaxVersions(1)
              .setBlockCacheEnabled(false)
              .setTimeToLive(0)
              .setBloomFilterType(entry.getValue()));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  /**
   * Test that {@link HFileOutputFormat} RecordWriter uses bloomfilter settings
   * from the column family descriptor
   */
  @Test
  public void testColumnFamilyBloomFilter()
      throws IOException, InterruptedException {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
        util.getTestDir("testColumnFamilyBloomFilter");

    HTable table = Mockito.mock(HTable.class);
    Map<String, BloomType> configuredBloomFilter =
      new HashMap<String, BloomType>();
    BloomType [] bloomTypeValues = BloomType.values();

    int familyIndex = 0;
    for (byte[] family : FAMILIES) {
      configuredBloomFilter.put(Bytes.toString(family),
        bloomTypeValues[familyIndex++ % bloomTypeValues.length]);
    }

    setupColumnFamiliesBloomType(table, configuredBloomFilter);

    // set up the table to return some mock keys
    setupMockStartKeys(table);

    try {
      // partial map red setup to get an operational writer for testing
      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      setupRandomGeneratorMapper(job);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
          new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeRandomKeyValues(writer, context, ROWSPERSPLIT);
      writer.close(context);

      // Make sure that a directory was created for every CF
      FileSystem fileSystem = dir.getFileSystem(conf);

      // commit so that the filesystem has one directory per column family
      hof.getOutputCommitter(context).commitTask(context);
      for (byte[] family : FAMILIES) {
        String familyStr = new String(family);
        boolean found = false;
        for (FileStatus f : fileSystem.listStatus(dir)) {

          if (Bytes.toString(family).equals(f.getPath().getName())) {
            // we found a matching directory
            found = true;

            // verify that the bloomfilter type on this file matches the
            // configured bloom type.
            Path dataFilePath = fileSystem.listStatus(f.getPath())[0].getPath();
            StoreFile.Reader reader = new StoreFile.Reader(fileSystem,
                dataFilePath, new CacheConfig(conf), null);
            Map<byte[], byte[]> metadataMap = reader.loadFileInfo();

            assertTrue("timeRange is not set",
      metadataMap.get(StoreFile.TIMERANGE_KEY) != null);
            assertEquals("Incorrect bloom type used for column family " +
           familyStr + "(reader: " + reader + ")",
                         configuredBloomFilter.get(familyStr),
                         reader.getBloomFilterType());
            break;
          }
        }

        if (!found) {
          fail("HFile for column family " + familyStr + " not found");
        }
      }

    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  @SuppressWarnings("deprecation")
  private void setupColumnFamiliesEncodingType(HTable table,
      Map<String, DataBlockEncoding> familyToEncoding) throws IOException {
    HTableDescriptor mockTableDesc = new HTableDescriptor();
    for (Entry<String, DataBlockEncoding> entry : familyToEncoding.entrySet()) {
      mockTableDesc.addFamily(
        new HColumnDescriptor(entry.getKey().getBytes(),
          1,
          Compression.Algorithm.NONE.toString(),
          true,
          entry.getValue().toString(),
          false,
          false,
          HColumnDescriptor.DEFAULT_BLOCKSIZE,
          0,
          BloomType.NONE.toString(),
          HColumnDescriptor.DEFAULT_REPLICATION_SCOPE,
          HColumnDescriptor.DEFAULT_BLOOMFILTER_ERROR_RATE));
    }
    Mockito.doReturn(mockTableDesc).when(table).getTableDescriptor();
  }

  /**
   * Test that {@link HFileOutputFormat} RecordWriter uses encoding settings
   * from the column family descriptor
   */
  @Test
  public void testColumnFamilyEncoding()
      throws IOException, InterruptedException {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir =
        util.getTestDir("testColumnFamilyEncoding");

    HTable table = Mockito.mock(HTable.class);
    Map<String, DataBlockEncoding> configuredEncoding =
      new HashMap<String, DataBlockEncoding>();
    DataBlockEncoding[] EncodingTypeValues = DataBlockEncoding.values();

    int familyIndex = 0;
    for (byte[] family : FAMILIES) {
      configuredEncoding.put(Bytes.toString(family),
        EncodingTypeValues[familyIndex++ % EncodingTypeValues.length]);
    }

    setupColumnFamiliesEncodingType(table, configuredEncoding);

    // set up the table to return some mock keys
    setupMockStartKeys(table);

    try {
      // partial map red setup to get an operational writer for testing
      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      setupRandomGeneratorMapper(job);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
          new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeRandomKeyValues(writer, context, ROWSPERSPLIT);
      writer.close(context);

      // Make sure that a directory was created for every CF
      FileSystem fileSystem = dir.getFileSystem(conf);

      // commit so that the filesystem has one directory per column family
      hof.getOutputCommitter(context).commitTask(context);
      for (byte[] family : FAMILIES) {
        String familyStr = Bytes.toString(family);
        boolean found = false;
        for (FileStatus f : fileSystem.listStatus(dir)) {

          if (Bytes.toString(family).equals(f.getPath().getName())) {
            // we found a matching directory
            found = true;

            // verify that the Encoding type on this file matches the
            // configured Encoding type.
            Path dataFilePath = fileSystem.listStatus(f.getPath())[0].getPath();

            StoreFile.Reader reader = new StoreFile.Reader(fileSystem,
                dataFilePath, new CacheConfig(conf), null);
            Map<byte[], byte[]> metadataMap = reader.loadFileInfo();

            assertTrue("timeRange is not set", metadataMap.get(StoreFile.TIMERANGE_KEY) != null);
            assertEquals("Incorrect Encoding Type used for column family "
              + familyStr + "(reader: " + reader + ")",
              configuredEncoding.get(familyStr),
              reader.getHFileReader().getEncodingOnDisk());
            break;
          }
        }

        if (!found) {
          fail("HFile for column family " + familyStr + " not found");
        }
      }

    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  // Marked as unstable, recorded in #3297517
  @Category(UnstableTests.class)
  @Test
  public void testFavoredNodes() throws Exception {
    Random rand = new Random();
    for (int i=0; i<3; i++) {
      int tmp = (int)'b';
      byte c = (byte)(tmp + (Math.abs(rand.nextInt()))%24);
      testFavoredNodesPerChar(c);
    }
  }
  private static final int FAVORED_NODES_NUM = 3;
  private static final int REGION_SERVERS = 10;
  /**
   * Testing FavoredNodes support for HFileOutputFormat
   */
  public void testFavoredNodesPerChar(byte c) throws Exception{
    util.startMiniCluster(REGION_SERVERS);
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = null;
    TaskAttemptContext context = null;
    Path dir = util.getTestDir("TestFavoredNodes");
    byte[] familyName = Bytes.toBytes("family");
    byte[] tableName = Bytes.toBytes("TestFavoredNodes");
    byte[][] startKeys = util.getTmpKeys();
    String[] nodeNames = new String[REGION_SERVERS];
    List<DataNode> dataNodes = util.getDFSCluster().getDataNodes();
    for (int i = 0; i < REGION_SERVERS; i++) {
      DataNode node = dataNodes.get(i);
      nodeNames[i] =
          node.getSelfAddr().getAddress().getHostAddress() + ":" +
              node.getSelfAddr().getPort();
    }
    int testIndex = 0;
    byte[][] favNodes = new byte[startKeys.length][];
    for (int i = 0; i < startKeys.length; i++) {
      List<HServerAddress> favoredNodes =
          new ArrayList<HServerAddress>(FAVORED_NODES_NUM);
      if (i>0 && startKeys[i][0] == c) {
        testIndex = i;
      }
      for (int j = 0; j < FAVORED_NODES_NUM; j++) {
        favoredNodes.add(new HServerAddress(nodeNames[(i + j) %
                                            REGION_SERVERS]));
      }
      String favoredNodesString =
          RegionPlacement.getFavoredNodes(favoredNodes);
      byte[] favoredNodesBytes = Bytes.toBytes(favoredNodesString);
      favNodes[i] = favoredNodesBytes;
    }
    HTable table = util.createTable(tableName, familyName);
    int countOfRegions = util.createMultiRegionsWithFavoredNodes(conf, table, familyName,
        new Pair<byte[][],byte[][]>(startKeys, favNodes));
    util.waitUntilAllRegionsAssigned(countOfRegions);

    try {
      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      setupRandomGeneratorMapper(job);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      FileOutputFormat.setOutputPath(job, dir);
      context = new TaskAttemptContext(job.getConfiguration(),
        new TaskAttemptID());
      HFileOutputFormat hof = new HFileOutputFormat();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeKVs(writer, familyName, c);
      writer.close(context);

      hof.getOutputCommitter(context).commitTask(context);
      // Make sure that a directory was created for every CF
      FileSystem fileSystem = dir.getFileSystem(conf);
      for (byte[] family : new byte[][]{familyName}) {
        for (FileStatus f : fileSystem.listStatus(dir)) {
          if (Bytes.toString(family).equals(f.getPath().getName())) {
            // verify that the Encoding type on this file matches the
            // configured Encoding type.
            Path dataFilePath = fileSystem.listStatus(f.getPath())[0].getPath();
            LocatedBlocks lbks = util.getDFSCluster().getNameNode()
                .getBlockLocations(dataFilePath.toUri().getPath(), 0, Long.MAX_VALUE);

            for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
              locations:
              for (DatanodeInfo info : lbk.getLocations()) {
                for (int j = 0; j < FAVORED_NODES_NUM; j++) {
                  if (info.getName().equals(nodeNames[(testIndex + j) % REGION_SERVERS])) {
                    continue locations;
                  }
                }
                // This block was at a location that was not a favored location.
                fail("Block location " + info.getName() + " not a favored node");
              }
            }
          }
        }
      }
    } finally {
      dir.getFileSystem(conf).delete(dir, true);
      util.shutdownMiniCluster();
    }
  }

  private void writeKVs(RecordWriter<ImmutableBytesWritable, KeyValue> writer,
    byte[] family, byte keyByte) throws IOException, InterruptedException{
    byte[] k = new byte[3];
    int b1 = (int)keyByte;
    Random rand = new Random();
    int tmp = rand.nextInt();
    int b2 = Math.min(b1 + Math.abs(tmp)%26, (int)'z');
    tmp = rand.nextInt();
    int b3 = Math.min(b1 + Math.abs(tmp)%26, (int)'z');

    for (byte byte2 = (byte)b2; byte2 <= 'z'; byte2++) {
      for (byte byte3 = (byte)b3; byte3 <= 'z'; byte3++) {
        k[0] = (byte)b1;
        k[1] = byte2;
        k[2] = byte3;
        ImmutableBytesWritable key = new ImmutableBytesWritable(k);
        KeyValue kv = new KeyValue(k, family, null, k);
        writer.write(key, kv);
      }
    }
  }

  /**
   * Write random values to the writer assuming a table created using
   * {@link #FAMILIES} as column family descriptors
   */
  private void writeRandomKeyValues(RecordWriter<ImmutableBytesWritable, KeyValue> writer, TaskAttemptContext context,
      int numRows)
      throws IOException, InterruptedException {
    byte keyBytes[] = new byte[Bytes.SIZEOF_INT];
    int valLength = 10;
    byte valBytes[] = new byte[valLength];

    int taskId = context.getTaskAttemptID().getTaskID().getId();
    assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

    Random random = new Random();
    for (int i = 0; i < numRows; i++) {

      Bytes.putInt(keyBytes, 0, i);
      random.nextBytes(valBytes);
      ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

      for (byte[] family : TestHFileOutputFormat.FAMILIES) {
        KeyValue kv = new KeyValue(keyBytes, family,
            PerformanceEvaluation.QUALIFIER_NAME, valBytes);
        writer.write(key, kv);
      }
    }
  }

  public static void main(String args[]) throws Exception {
    new TestHFileOutputFormat().manualTest(args);
  }

  public void manualTest(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    util = new HBaseTestingUtility(conf);
    if ("newtable".equals(args[0])) {
      StringBytes tname = new StringBytes(args[1]);
      byte[][] splitKeys = generateRandomSplitKeys(4);
      util.createTable(tname, FAMILIES, 3, splitKeys);
    } else if ("incremental".equals(args[0])) {
      StringBytes tname = new StringBytes(args[1]);
      HTable table = new HTable(conf, tname);
      Path outDir = new Path("incremental-out");
      runIncrementalPELoad(conf, table, outDir);
    } else {
      throw new RuntimeException(
          "usage: TestHFileOutputFormat newtable | incremental");
    }
  }

  /**
   * Test writing HFile and bulk uploading to HTable without integration of MapReduce
   * @throws Exception
   */
  @Test
  public void testCustomHFileWritingAndBulkUpload() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    util.startMiniCluster();
    Path outputDir = util.getTestDir("/testCustomBulkUpload/");

    // Create a writer
    TaskAttemptContext context = HFileOutputFormat.createFakeTaskAttemptContext(conf, outputDir, 123);
    HFileOutputFormat hFileOutputFormat = new HFileOutputFormat();
    RecordWriter<ImmutableBytesWritable, KeyValue> writer = hFileOutputFormat.getRecordWriter(context);

    // Write 100 KeyValues in sorted order
    byte[] row = Bytes.toBytes("testRow");
    byte[] family = Bytes.toBytes("testFam");
    byte[] qualifier = Bytes.toBytes("testQual");
    byte[] value = Bytes.toBytes("testVal");

    ImmutableBytesWritable someWritable = new ImmutableBytesWritable();
    for (int i = 0; i < 100; i++) {
      KeyValue kv = new KeyValue(
        Bytes.add(row, Bytes.toBytes(i)),
        family,
        qualifier,
        Bytes.add(value, Bytes.toBytes(i)));
      writer.write(someWritable, kv);
    }

    // Close writer
    writer.close(context);

    // Bulk upload HFile to HTable
    HTable table = util.createTable(Bytes.toBytes("testTable"), family);
    Path workPath = new FileOutputCommitter(outputDir, context).getWorkPath();
    new LoadIncrementalHFiles(conf).doBulkLoad(workPath, table);

    // Verify data in HTable
    Scan scan = new Scan().addColumn(family, qualifier);
    ResultScanner scanner = table.getScanner(scan);
    for (int i = 0; i < 100; i++) {
      Result r = scanner.next();
      assertNotNull(r);
      KeyValue[] kvs = r.raw();
      assertEquals(1, kvs.length);
      assertTrue(Bytes.equals(Bytes.add(row, Bytes.toBytes(i)), kvs[0].getRow()));
      assertTrue(Bytes.equals(Bytes.add(value, Bytes.toBytes(i)), kvs[0].getValue()));
    }
  }
}
