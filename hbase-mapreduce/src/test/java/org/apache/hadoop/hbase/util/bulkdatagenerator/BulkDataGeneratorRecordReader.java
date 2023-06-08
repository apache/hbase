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

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

public class BulkDataGeneratorRecordReader extends RecordReader<Text, NullWritable> {

  private int numRecordsToCreate = 0;
  private int createdRecords = 0;
  private Text key = new Text();
  private NullWritable value = NullWritable.get();

  public static final String RECORDS_PER_MAPPER_TASK_KEY =
    BulkDataGeneratorInputFormat.class.getName() + "records.per.mapper.task";

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    // Get the number of records to create from the configuration
    this.numRecordsToCreate = context.getConfiguration().getInt(RECORDS_PER_MAPPER_TASK_KEY, -1);
    Preconditions.checkArgument(numRecordsToCreate > 0,
      "Number of records to be created by per mapper should be greater than 0.");
  }

  @Override
  public boolean nextKeyValue() {
    createdRecords++;
    return createdRecords <= numRecordsToCreate;
  }

  @Override
  public Text getCurrentKey() {
    // Set the index of record to be created
    key.set(String.valueOf(createdRecords));
    return key;
  }

  @Override
  public NullWritable getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) createdRecords / (float) numRecordsToCreate;
  }

  @Override
  public void close() throws IOException {

  }
}
