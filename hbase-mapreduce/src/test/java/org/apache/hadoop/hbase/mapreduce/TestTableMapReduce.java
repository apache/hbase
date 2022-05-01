/*
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Map/Reduce job over HBase tables. The map/reduce process we're testing on our tables is
 * simple - take every row in the table, reverse the value of a particular cell, and write it back
 * to the table.
 */

@Category({ VerySlowMapReduceTests.class, LargeTests.class })
public class TestTableMapReduce extends TestTableMapReduceBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableMapReduce.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTableMapReduce.class);

  @Override
  protected Logger getLog() {
    return LOG;
  }

  /**
   * Pass the given key and processed record reduce
   */
  static class ProcessContentsMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Pass the key, and reversed value to reduce nnnn
     */
    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cf = value.getMap();
      if (!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException(
          "Wrong input columns. Missing: '" + Bytes.toString(INPUT_FAMILY) + "'.");
      }

      // Get the original value and reverse it
      String originalValue = Bytes.toString(value.getValue(INPUT_FAMILY, INPUT_FAMILY));
      StringBuilder newValue = new StringBuilder(originalValue);
      newValue.reverse();
      // Now set the value to be collected
      Put outval = new Put(key.get());
      outval.addColumn(OUTPUT_FAMILY, null, Bytes.toBytes(newValue.toString()));
      context.write(key, outval);
    }
  }

  @Override
  protected void runTestOnTable(Table table) throws IOException {
    Job job = null;
    try {
      LOG.info("Before map/reduce startup");
      job = new Job(table.getConfiguration(), "process column contents");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      TableMapReduceUtil.initTableMapperJob(table.getName().getNameAsString(), scan,
        ProcessContentsMapper.class, ImmutableBytesWritable.class, Put.class, job);
      TableMapReduceUtil.initTableReducerJob(table.getName().getNameAsString(),
        IdentityTableReducer.class, job);
      FileOutputFormat.setOutputPath(job, new Path("test"));
      LOG.info("Started " + table.getName().getNameAsString());
      assertTrue(job.waitForCompletion(true));
      LOG.info("After map/reduce completion");

      // verify map-reduce results
      verify(table.getName());

      verifyJobCountersAreEmitted(job);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      table.close();
      if (job != null) {
        FileUtil.fullyDelete(new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  /**
   * Verify scan counters are emitted from the job nn
   */
  private void verifyJobCountersAreEmitted(Job job) throws IOException {
    Counters counters = job.getCounters();
    Counter counter =
      counters.findCounter(TableRecordReaderImpl.HBASE_COUNTER_GROUP_NAME, "RPC_CALLS");
    assertNotNull("Unable to find Job counter for HBase scan metrics, RPC_CALLS", counter);
    assertTrue("Counter value for RPC_CALLS should be larger than 0", counter.getValue() > 0);
  }

  @Test(expected = TableNotEnabledException.class)
  public void testWritingToDisabledTable() throws IOException {

    try (Admin admin = UTIL.getConnection().getAdmin();
      Table table = UTIL.getConnection().getTable(TABLE_FOR_NEGATIVE_TESTS)) {
      admin.disableTable(table.getName());
      runTestOnTable(table);
      fail("Should not have reached here, should have thrown an exception");
    }
  }

  @Test(expected = TableNotFoundException.class)
  public void testWritingToNonExistentTable() throws IOException {

    try (Table table = UTIL.getConnection().getTable(TableName.valueOf("table-does-not-exist"))) {
      runTestOnTable(table);
      fail("Should not have reached here, should have thrown an exception");
    }
  }
}
