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
package org.apache.hadoop.hbase.util.bulkdatagenerator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

public class BulkDataGeneratorInputFormat extends InputFormat<Text, NullWritable> {

  public static final String MAPPER_TASK_COUNT_KEY =
    BulkDataGeneratorInputFormat.class.getName() + "mapper.task.count";

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    // Get the number of mapper tasks configured
    int mapperCount = job.getConfiguration().getInt(MAPPER_TASK_COUNT_KEY, -1);
    Preconditions.checkArgument(mapperCount > 1, MAPPER_TASK_COUNT_KEY + " is not set.");

    // Create a number of input splits equal to the number of mapper tasks
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < mapperCount; ++i) {
      splits.add(new FakeInputSplit());
    }
    return splits;
  }

  @Override
  public RecordReader<Text, NullWritable> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException, InterruptedException {
    BulkDataGeneratorRecordReader bulkDataGeneratorRecordReader =
      new BulkDataGeneratorRecordReader();
    bulkDataGeneratorRecordReader.initialize(split, context);
    return bulkDataGeneratorRecordReader;
  }

  /**
   * Dummy input split to be used by {@link BulkDataGeneratorRecordReader}
   */
  private static class FakeInputSplit extends InputSplit implements Writable {

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }
  }
}
