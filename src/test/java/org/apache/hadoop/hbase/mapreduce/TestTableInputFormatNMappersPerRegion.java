/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests TableInputFormat with varying numbers of mappers per region.
 */
@Category(LargeTests.class)
public class TestTableInputFormatNMappersPerRegion {

  static final String SPECULATIVE_EXECUTION = "mapred.map.tasks.speculative.execution";
  static final Log LOG = LogFactory.getLog(TestTableInputFormatScan.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");

  private static HTable table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    FileUtil.fullyDelete(new File(c.get("hadoop.tmp.dir")));
  }

  /**
   * Mapper that runs the count.
   */
  public static class RowCounterMapper
  extends TableMapper<ImmutableBytesWritable, Result> {

    /** Counter enumeration to count the actual rows. */
    public static enum Counters {ROWS}

    /**
     * Maps the data.
     *
     * @param row  The current table row key.
     * @param values  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values,
      Context context)
    throws IOException {
      for (KeyValue value: values.list()) {
        if (value.getValue().length > 0) {
          context.getCounter(Counters.ROWS).increment(1);
          break;
        }
      }
    }

  }

  /**
   * Tests whether TableInputFormat works correctly when number of mappers
   * per region is set to 1.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testOneMapperPerRegion()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("testOneMapperPerRegion", 1, 25);
  }

  /**
   * Tests when number of mappers is set to 3.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testThreeMappersPerRegion()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("testThreeMappersPerRegion", 3, 25);
  }

  /**
   * Tests the scenario where there is only one region. Expecting resumption to
   * one mapper per region.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testOnTableWithOneRegion()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("testOnTableWithOneRegion", 5, 1);
  }

  /**
   * Tests the scenario where there is only two regions. Expecting resumption to
   * one mapper per region.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testOnTableWithTwoRegions()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("testOnTableWithTwoRegions", 5, 2);
  }

  /**
   * Tests whether the framework correctly detects illegal inputs.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testIllegalNumberOfMappers()
  throws IOException, InterruptedException, ClassNotFoundException {
    try {
      testScan("testZeroMapper", 0, 25);
      Assert.assertTrue("Should not be able to take 0 as number of mappers "
          + "per region", false);
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      testScan("testNegOneMapper", -1, 25);
      Assert.assertTrue("Should not be able to take -1 as number of mappers "
          + "per region", false);
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  // If numRegions > 2, this creates 25 regions, rather than numRegions regions.
  private void testScan(String tableName, int numMappersPerRegion, int numRegions)
  throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = tableName + "_job";
    Configuration c = TEST_UTIL.getConfiguration();
    // Force disable speculative maps
    c.setBoolean(SPECULATIVE_EXECUTION, false);
    // Set the number of maps per region for this job
    c.setInt(TableInputFormat.MAPPERS_PER_REGION, numMappersPerRegion);
    // Store the number of regions opened
    int regionsOpened = 0;
    if (numRegions == 2) {
      regionsOpened = numRegions;
      table = TEST_UTIL.createTable(Bytes.toBytes(tableName),
          new byte[][] { INPUT_FAMILY }, 3, Bytes.toBytes("mmm"), null,
          regionsOpened);
    } else if (numRegions == 1) {
      table = TEST_UTIL.createTable(Bytes.toBytes(tableName), INPUT_FAMILY);
      regionsOpened = 1;
    } else if (numRegions > 2) {
      regionsOpened = 25;
      table = TEST_UTIL.createTable(Bytes.toBytes(tableName),
          new byte[][] { INPUT_FAMILY }, 3, Bytes.toBytes("bbb"),
          Bytes.toBytes("yyy"), regionsOpened);
    } else {
      throw new IllegalArgumentException("Expect positive number of regions but got " +
          numRegions);
    }
    // Store the number of rows loaded to the table
    int rowsLoaded = TEST_UTIL.loadTable(table, INPUT_FAMILY);
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    scan.setFilter(new FirstKeyOnlyFilter());
    Job job = new Job(c, jobName);
    job.setOutputFormatClass(NullOutputFormat.class);
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.waitForCompletion(true);
    Assert.assertTrue(job.isComplete());
    // Get statistics
    Counters counters = job.getCounters();
    long totalMapCount = counters
        .findCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS).getValue();
    long totalRowCount = counters
        .findCounter(RowCounterMapper.Counters.ROWS).getValue();
    int actualNumMappersPerRegion = (numRegions > 2) ? numMappersPerRegion : 1;
    Assert.assertEquals("Tried to open " + actualNumMappersPerRegion * regionsOpened +
        " maps but got " + totalMapCount,
        actualNumMappersPerRegion * regionsOpened, totalMapCount);
    Assert.assertEquals("Supposed to find " + rowsLoaded + " rows but got " + totalRowCount,
        rowsLoaded, totalRowCount);
  }
}
