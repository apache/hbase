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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.experimental.categories.Category;

/**
 * Test Map/Reduce job over HBase tables. The map/reduce process we're testing
 * on our tables is simple - take every row in the table, reverse the value of
 * a particular cell, and write it back to the table.
 */

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestTableMapReduce extends TestTableMapReduceBase {
  private static final Log LOG = LogFactory.getLog(TestTableMapReduce.class);

  protected Log getLog() { return LOG; }

  /**
   * Pass the given key and processed record reduce
   */
  static class ProcessContentsMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Pass the key, and reversed value to reduce
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
    public void map(ImmutableBytesWritable key, Result value,
      Context context)
    throws IOException, InterruptedException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
        cf = value.getMap();
      if(!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException("Wrong input columns. Missing: '" +
          Bytes.toString(INPUT_FAMILY) + "'.");
      }

      // Get the original value and reverse it
      String originalValue = Bytes.toString(value.getValue(INPUT_FAMILY, null));
      StringBuilder newValue = new StringBuilder(originalValue);
      newValue.reverse();
      // Now set the value to be collected
      Put outval = new Put(key.get());
      outval.addColumn(OUTPUT_FAMILY, null, Bytes.toBytes(newValue.toString()));
      context.write(key, outval);
    }
  }

  protected void runTestOnTable(Table table) throws IOException {
    Job job = null;
    try {
      LOG.info("Before map/reduce startup");
      job = new Job(table.getConfiguration(), "process column contents");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      TableMapReduceUtil.initTableMapperJob(
        table.getName().getNameAsString(), scan,
        ProcessContentsMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
      TableMapReduceUtil.initTableReducerJob(
          table.getName().getNameAsString(),
        IdentityTableReducer.class, job);
      FileOutputFormat.setOutputPath(job, new Path("test"));
      LOG.info("Started " + table.getName().getNameAsString());
      assertTrue(job.waitForCompletion(true));
      LOG.info("After map/reduce completion");

      // verify map-reduce results
      verify(table.getName());
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      table.close();
      if (job != null) {
        FileUtil.fullyDelete(
          new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }
}
