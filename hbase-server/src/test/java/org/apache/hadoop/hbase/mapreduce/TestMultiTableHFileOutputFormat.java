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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for{@link MultiTableHFileOutputFormat}. Sets up and runs a mapreduce job that output directories and
 * writes hfiles.
 */
@Category(MediumTests.class)
public class TestMultiTableHFileOutputFormat {
  private static final Log LOG = LogFactory.getLog(TestMultiTableHFileOutputFormat.class);

  private HBaseTestingUtility util = new HBaseTestingUtility();

  private static int ROWSPERSPLIT = 10;

  private static final int KEYLEN_DEFAULT = 10;
  private static final String KEYLEN_CONF = "randomkv.key.length";

  private static final int VALLEN_DEFAULT = 10;
  private static final String VALLEN_CONF = "randomkv.val.length";

  private static final byte[][] TABLES =
      { Bytes.add(Bytes.toBytes(PerformanceEvaluation.TABLE_NAME), Bytes.toBytes("-1")),
          Bytes.add(Bytes.toBytes(PerformanceEvaluation.TABLE_NAME), Bytes.toBytes("-2")) };

  private static final byte[][] FAMILIES =
      { Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-A")),
          Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-B")) };

  private static final byte[] QUALIFIER = Bytes.toBytes("data");

  /**
   * Run small MR job. this MR job will write HFile into
   * testWritingDataIntoHFiles/tableNames/columnFamilies/
   */
  @Test
  public void testWritingDataIntoHFiles() throws Exception {
    Configuration conf = util.getConfiguration();
    util.startMiniCluster();
    Path testDir = util.getDataTestDirOnTestFS("testWritingDataIntoHFiles");
    FileSystem fs = testDir.getFileSystem(conf);
    LOG.info("testWritingDataIntoHFiles dir writing to dir: " + testDir);

    // Set down this value or we OOME in eclipse.
    conf.setInt("mapreduce.task.io.sort.mb", 20);
    // Write a few files by setting max file size.
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 64 * 1024);

    try {
      Job job = Job.getInstance(conf, "testWritingDataIntoHFiles");

      FileOutputFormat.setOutputPath(job, testDir);

      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(Random_TableKV_GeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
      job.setReducerClass(Table_KeyValueSortReducer.class);
      job.setOutputFormatClass(MultiTableHFileOutputFormat.class);
      job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
          MutationSerialization.class.getName(), ResultSerialization.class.getName(),
          KeyValueSerialization.class.getName());

      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.initCredentials(job);
      LOG.info("\nStarting test testWritingDataIntoHFiles\n");
      assertTrue(job.waitForCompletion(true));
      LOG.info("\nWaiting on checking MapReduce output\n");
      assertTrue(checkMROutput(fs, testDir, 0));
    } finally {
      testDir.getFileSystem(conf).delete(testDir, true);
      util.shutdownMiniCluster();
    }
  }

  /**
   * check whether create directory and hfiles as format designed in MultiHFilePartitioner
   * and also check whether the output file has same related configuration as created table
   */
  @Test
  public void testMultiHFilePartitioner() throws Exception {
    Configuration conf = util.getConfiguration();
    util.startMiniCluster();
    Path testDir = util.getDataTestDirOnTestFS("testMultiHFilePartitioner");
    FileSystem fs = testDir.getFileSystem(conf);
    LOG.info("testMultiHFilePartitioner dir writing to : " + testDir);

    // Set down this value or we OOME in eclipse.
    conf.setInt("mapreduce.task.io.sort.mb", 20);
    // Write a few files by setting max file size.
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 64 * 1024);

    // Create several tables for testing
    List<TableName> tables = new ArrayList<TableName>();

    // to store splitKeys for TABLE[0] for testing;
    byte[][] testKeys = new byte[0][0];
    for (int i = 0; i < TABLES.length; i++) {
      TableName tableName = TableName.valueOf(TABLES[i]);
      byte[][] splitKeys = generateRandomSplitKeys(3);
      if (i == 0) {
        testKeys = splitKeys;
      }
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      for (int j = 0; j < FAMILIES.length; j++) {
        HColumnDescriptor familyDescriptor = new HColumnDescriptor(FAMILIES[j]);
        //only set Tables[0] configuration, and specify compression type and DataBlockEncode
        if (i == 0) {
          familyDescriptor.setCompressionType(Compression.Algorithm.GZ);
          familyDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        }
        tableDescriptor.addFamily(familyDescriptor);
      }
      util.createTable(tableDescriptor, splitKeys, conf);
      tables.add(tableName);
    }
    // set up for MapReduce job
    try {
      Job job = Job.getInstance(conf, "testMultiHFilePartitioner");
      FileOutputFormat.setOutputPath(job, testDir);

      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(Random_TableKV_GeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);

      MultiTableHFileOutputFormat.configureIncrementalLoad(job, tables);

      LOG.info("Starting test testWritingDataIntoHFiles");
      assertTrue(job.waitForCompletion(true));
      LOG.info("Waiting on checking MapReduce output");
      assertTrue(checkMROutput(fs, testDir, 0));
      assertTrue(checkFileConfAndSplitKeys(conf, fs, testDir, testKeys));
    } finally {
      for (int i = 0; i < TABLES.length; i++) {
        TableName tName = TableName.valueOf(TABLES[i]);
        util.deleteTable(tName);
      }
      fs.delete(testDir, true);
      fs.close();
      util.shutdownMiniCluster();
    }
  }

  /**
   * check the output hfile has same configuration as created test table
   * and also check whether hfiles get split correctly
   * only check TABLES[0]
   */
  private boolean checkFileConfAndSplitKeys(Configuration conf, FileSystem fs, Path testDir, byte[][] splitKeys) throws IOException {
    FileStatus[] fStats = fs.listStatus(testDir);
    for (FileStatus stats : fStats) {
      if (stats.getPath().getName().equals(new String(TABLES[0]))) {
        FileStatus[] cfStats = fs.listStatus(stats.getPath());
        for (FileStatus cfstat : cfStats) {
          FileStatus[] hfStats = fs.listStatus(cfstat.getPath());

          List<byte[]> firsttKeys = new ArrayList<byte[]>();
          List<byte[]> lastKeys = new ArrayList<byte[]>();
          for (FileStatus hfstat : hfStats) {
            if (HFile.isHFileFormat(fs, hfstat)) {
              HFile.Reader hfr =
                  HFile.createReader(fs, hfstat.getPath(), new CacheConfig(conf), true, conf);
              if (!hfr.getDataBlockEncoding().equals(DataBlockEncoding.FAST_DIFF) || !hfr
                  .getCompressionAlgorithm().equals(Compression.Algorithm.GZ)) return false;
              firsttKeys.add(hfr.getFirstRowKey());
              lastKeys.add(hfr.getLastRowKey());
            }
          }
          if (checkFileSplit(splitKeys, firsttKeys, lastKeys) == false) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Check whether the Hfile has been split by region boundaries
   * @param splitKeys split keys for that table
   * @param firstKeys first rowKey for hfiles
   * @param lastKeys last rowKey for hfiles
   */
  private boolean checkFileSplit(byte[][] splitKeys, List<byte[]> firstKeys, List<byte[]> lastKeys) {
    Collections.sort(firstKeys, Bytes.BYTES_RAWCOMPARATOR);
    Collections.sort(lastKeys, Bytes.BYTES_RAWCOMPARATOR);
    Arrays.sort(splitKeys, Bytes.BYTES_RAWCOMPARATOR);

    int is = 0, il = 0;
    for (byte[] key : lastKeys) {
      while (is < splitKeys.length && Bytes.compareTo(key, splitKeys[is]) >= 0) is++;
      if (is == splitKeys.length) {
        break;
      }
      if (is > 0) {
        if (Bytes.compareTo(firstKeys.get(il), splitKeys[is - 1]) < 0) return false;
      }
      il++;
    }

    if (is == splitKeys.length) {
      return il == lastKeys.size() - 1;
    }
    return true;
  }


  /**
   * MR will output a 3 level directory, tableName->ColumnFamilyName->HFile this method to check the
   * created directory is correct or not A recursion method, the testDir had better be small size
   */
  private boolean checkMROutput(FileSystem fs, Path testDir, int level) throws IOException {
    if (level >= 3) {
      return HFile.isHFileFormat(fs, testDir);
    }
    FileStatus[] fStats = fs.listStatus(testDir);
    if (fStats == null || fStats.length <= 0) {
      LOG.info("Created directory format is not correct");
      return false;
    }

    for (FileStatus stats : fStats) {
      // skip the _SUCCESS file created by MapReduce
      if (level == 0 && stats.getPath().getName().endsWith(FileOutputCommitter.SUCCEEDED_FILE_NAME))
        continue;
      if (level < 2 && !stats.isDirectory()) {
        LOG.info("Created directory format is not correct");
        return false;
      }
      boolean flag = checkMROutput(fs, stats.getPath(), level + 1);
      if (flag == false) return false;
    }
    return true;
  }


  private byte[][] generateRandomSplitKeys(int numKeys) {
    Random random = new Random();
    byte[][] ret = new byte[numKeys][];
    for (int i = 0; i < numKeys; i++) {
      ret[i] = PerformanceEvaluation.generateData(random, KEYLEN_DEFAULT);
    }
    return ret;
  }


  /**
   * Simple mapper that makes <TableName, KeyValue> output. With no input data
   */
  static class Random_TableKV_GeneratingMapper
      extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Cell> {

    private int keyLength;
    private int valLength;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
    }

    @Override
    protected void map(NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable, ImmutableBytesWritable, Cell>.Context context)
        throws java.io.IOException, InterruptedException {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      ArrayList<ImmutableBytesWritable> tables = new ArrayList<>();
      for (int i = 0; i < TABLES.length; i++) {
        tables.add(new ImmutableBytesWritable(TABLES[i]));
      }

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";
      Random random = new Random();

      for (int i = 0; i < ROWSPERSPLIT; i++) {
        random.nextBytes(keyBytes);
        // Ensure that unique tasks generate unique keys
        keyBytes[keyLength - 1] = (byte) (taskId & 0xFF);
        random.nextBytes(valBytes);

        for (ImmutableBytesWritable table : tables) {
          for (byte[] family : FAMILIES) {
            Cell kv = new KeyValue(keyBytes, family, QUALIFIER, valBytes);
            context.write(table, kv);
          }
        }
      }
    }
  }

  /**
   * Simple Reducer that have input <TableName, KeyValue>, with KeyValues have no order. and output
   * <TableName, KeyValue>, with KeyValues are ordered
   */

  static class Table_KeyValueSortReducer
      extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
    protected void reduce(ImmutableBytesWritable table, java.lang.Iterable<KeyValue> kvs,
        org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue>.Context context)
        throws java.io.IOException, InterruptedException {
      TreeSet<KeyValue> map = new TreeSet<>(CellComparator.COMPARATOR);
      for (KeyValue kv : kvs) {
        try {
          map.add(kv.clone());
        } catch (CloneNotSupportedException e) {
          throw new java.io.IOException(e);
        }
      }
      context.setStatus("Read " + map.getClass());
      int index = 0;
      for (KeyValue kv : map) {
        context.write(table, kv);
        if (++index % 100 == 0) context.setStatus("Wrote " + index);
      }
    }
  }
}