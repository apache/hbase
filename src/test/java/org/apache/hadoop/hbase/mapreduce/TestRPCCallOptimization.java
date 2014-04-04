/**
 * Copyright 2007 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;


import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests various scan start and stop row scenarios. This is set in a scan and tested in a MapReduce
 * job to see if that is handed over and done properly too.
 */
public class TestRPCCallOptimization {

  static final Log LOG = LogFactory.getLog(TestTableInputFormatScan.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final byte[] TABLE_NAME = Bytes.toBytes("scantest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  private static HTable table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { INPUT_FAMILY }, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), 25);
    TEST_UTIL.loadTable(table, INPUT_FAMILY);
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    // nothing
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    FileUtil.fullyDelete(new File(c.get("hadoop.tmp.dir")));
  }

  /**
   * Checks the last and first key seen against the scanner boundaries.
   */
  public static class DummyReducer
      extends
      TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, NullWritable> {
    
    @Override
    protected void reduce(ImmutableBytesWritable key,
        Iterable<ImmutableBytesWritable> values, Context context)
        throws IOException, InterruptedException {
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
    }    
  }

  @Test
  public void testMapReduceNumRegionsCache() throws IOException {
    String jobName = "testJob";
    Configuration c = TEST_UTIL.getConfiguration();
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableReducerJob(Bytes.toString(TABLE_NAME),
      DummyReducer.class, job);
    TableMapReduceUtil.limitNumReduceTasks(Bytes.toString(TABLE_NAME), job);
    TableMapReduceUtil.setNumReduceTasks(Bytes.toString(TABLE_NAME), job);
    Job job2 = new Job(c, jobName);
    TableMapReduceUtil.limitNumReduceTasks(Bytes.toString(TABLE_NAME), job2);
  }
}
