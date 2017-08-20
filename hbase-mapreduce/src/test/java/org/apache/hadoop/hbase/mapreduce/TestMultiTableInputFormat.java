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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.mapreduce.Job;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

/**
 * Tests various scan start and stop row scenarios. This is set in a scan and
 * tested in a MapReduce job to see if that is handed over and done properly
 * too.
 */
@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestMultiTableInputFormat extends MultiTableInputFormatTestBase {

  @BeforeClass
  public static void setupLogging() {
    TEST_UTIL.enableDebug(MultiTableInputFormat.class);
      }

    @Override
  protected void initJob(List<Scan> scans, Job job) throws IOException {
    TableMapReduceUtil.initTableMapperJob(scans, ScanMapper.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
  }
}
