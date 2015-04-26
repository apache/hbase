/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapred;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestMultiTableSnapshotInputFormat extends org.apache.hadoop.hbase.mapreduce.TestMultiTableSnapshotInputFormat {

  private static final Log LOG = LogFactory.getLog(TestMultiTableSnapshotInputFormat.class);

  @Override
  protected void runJob(String jobName, Configuration c, List<Scan> scans) throws IOException, InterruptedException, ClassNotFoundException {
    JobConf job = new JobConf(TEST_UTIL.getConfiguration());

    job.setJobName(jobName);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);

    TableMapReduceUtil.initMultiTableSnapshotMapperJob(
        getSnapshotScanMapping(scans), Mapper.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, job,
        true, restoreDir
    );

    TableMapReduceUtil.addDependencyJars(job);

    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(1); // one to get final "first" and "last" key
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    LOG.info("Started " + job.getJobName());

    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();
    assertTrue(runningJob.isSuccessful());
    LOG.info("After map/reduce completion - job " + jobName);
  }

  public static class Mapper extends TestMultiTableSnapshotInputFormat.ScanMapper implements TableMap<ImmutableBytesWritable, ImmutableBytesWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, OutputCollector<ImmutableBytesWritable, ImmutableBytesWritable> outputCollector, Reporter reporter) throws IOException {
      makeAssertions(key, value);
      outputCollector.collect(key, key);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf jobConf) {

    }
  }

  public static class Reducer extends TestMultiTableSnapshotInputFormat.ScanReducer implements org.apache.hadoop.mapred.Reducer<ImmutableBytesWritable, ImmutableBytesWritable,
      NullWritable, NullWritable> {

    private JobConf jobConf;

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<ImmutableBytesWritable> values, OutputCollector<NullWritable, NullWritable> outputCollector, Reporter reporter) throws IOException {
      makeAssertions(key, Lists.newArrayList(values));
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
      super.cleanup(this.jobConf);
    }

    @Override
    public void configure(JobConf jobConf) {
      this.jobConf = jobConf;
    }
  }
}
