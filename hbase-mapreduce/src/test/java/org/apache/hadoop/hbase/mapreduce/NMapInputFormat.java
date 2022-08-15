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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Input format that creates a configurable number of map tasks each provided with a single row of
 * NullWritables. This can be useful when trying to write mappers which don't have any real input
 * (eg when the mapper is simply producing random data as output)
 */
public class NMapInputFormat extends InputFormat<NullWritable, NullWritable> {
  private static final String NMAPS_KEY = "nmapinputformat.num.maps";

  @Override
  public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
    TaskAttemptContext tac) {
    return new SingleRecordReader<>(NullWritable.get(), NullWritable.get());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    int count = getNumMapTasks(context.getConfiguration());
    List<InputSplit> splits = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      splits.add(new NullInputSplit());
    }
    return splits;
  }

  public static void setNumMapTasks(Configuration conf, int numTasks) {
    conf.setInt(NMAPS_KEY, numTasks);
  }

  public static int getNumMapTasks(Configuration conf) {
    return conf.getInt(NMAPS_KEY, 1);
  }

  private static class NullInputSplit extends InputSplit implements Writable {
    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[] {};
    }

    @Override
    public void readFields(DataInput in) {
    }

    @Override
    public void write(DataOutput out) {
    }
  }

  private static class SingleRecordReader<K, V> extends RecordReader<K, V> {

    private final K key;
    private final V value;
    boolean providedKey = false;

    SingleRecordReader(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public void close() {
    }

    @Override
    public K getCurrentKey() {
      return key;
    }

    @Override
    public V getCurrentValue() {
      return value;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext tac) {
    }

    @Override
    public boolean nextKeyValue() {
      if (providedKey) {
        return false;
      }

      providedKey = true;
      return true;
    }
  }
}
