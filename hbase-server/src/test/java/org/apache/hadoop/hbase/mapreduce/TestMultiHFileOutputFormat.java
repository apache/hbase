/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
 * Test for{@link MultiHFileOutputFormat}. Sets up and runs a mapreduce job that output directories and
 * writes hfiles.
 */
@Category(MediumTests.class)
public class TestMultiHFileOutputFormat {
    private static final Log LOG = LogFactory.getLog(TestMultiHFileOutputFormat.class);

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

    public static void main(String[] args) throws Exception {
        new TestMultiHFileOutputFormat().testWritingDataIntoHFiles();
    }

    /**
     * Run small MR job. this MR job will write HFile into
     * testWritingDataIntoHFiles/tableNames/columFamilies/
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
            job.setOutputFormatClass(MultiHFileOutputFormat.class);
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
     * MR will output a 3 level directory, tableName->ColumnFamilyName->HFile this method to check the
     * created directory is correct or not A recursion method, the testDir had better be small size
     */
    private boolean checkMROutput(FileSystem fs, Path testDir, int level)
        throws FileNotFoundException, IOException {
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

            ArrayList<ImmutableBytesWritable> tables = new ArrayList<ImmutableBytesWritable>();
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
            TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
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
