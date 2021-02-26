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
package org.apache.hadoop.hbase.mapred;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TestTableMapReduceBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Map/Reduce job over HBase tables. The map/reduce process we're testing
 * on our tables is simple - take every row in the table, reverse the value of
 * a particular cell, and write it back to the table.
 */
@Category({MapReduceTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public class TestTableMapReduce extends TestTableMapReduceBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableMapReduce.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestTableMapReduce.class.getName());

  protected Logger getLog() { return LOG; }

  /**
   * Pass the given key and processed record reduce
   */
  static class ProcessContentsMapper extends MapReduceBase implements
      TableMap<ImmutableBytesWritable, Put> {

    /**
     * Pass the key, and reversed value to reduce
     */
    public void map(ImmutableBytesWritable key, Result value,
      OutputCollector<ImmutableBytesWritable, Put> output,
      Reporter reporter)
    throws IOException {
      output.collect(key, TestTableMapReduceBase.map(key, value));
    }
  }

  @Override
  protected void runTestOnTable(Table table) throws IOException {
    JobConf jobConf = null;
    try {
      LOG.info("Before map/reduce startup");
      jobConf = new JobConf(UTIL.getConfiguration(), TestTableMapReduce.class);
      jobConf.setJobName("process column contents");
      jobConf.setNumReduceTasks(1);
      TableMapReduceUtil.initTableMapJob(table.getName().getNameAsString(),
        Bytes.toString(INPUT_FAMILY), ProcessContentsMapper.class,
        ImmutableBytesWritable.class, Put.class, jobConf);
      TableMapReduceUtil.initTableReduceJob(table.getName().getNameAsString(),
        IdentityTableReduce.class, jobConf);

      LOG.info("Started " + table.getName());
      RunningJob job = JobClient.runJob(jobConf);
      assertTrue(job.isSuccessful());
      LOG.info("After map/reduce completion");

      // verify map-reduce results
      verify(table.getName());
    } finally {
      if (jobConf != null) {
        FileUtil.fullyDelete(new File(jobConf.get("hadoop.tmp.dir")));
      }
    }
  }
}

