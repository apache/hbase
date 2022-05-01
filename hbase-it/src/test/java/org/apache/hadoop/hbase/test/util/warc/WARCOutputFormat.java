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
package org.apache.hadoop.hbase.test.util.warc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop OutputFormat for mapreduce jobs ('new' API) that want to write data to WARC files. Usage:
 * ```java Job job = new Job(getConf()); job.setOutputFormatClass(WARCOutputFormat.class);
 * job.setOutputKeyClass(NullWritable.class); job.setOutputValueClass(WARCWritable.class);
 * FileOutputFormat.setCompressOutput(job, true); ``` The tasks generating the output (usually the
 * reducers, but may be the mappers if there are no reducers) should use `NullWritable.get()` as the
 * output key, and the {@link WARCWritable} as the output value.
 */
public class WARCOutputFormat extends FileOutputFormat<NullWritable, WARCWritable> {

  /**
   * Creates a new output file in WARC format, and returns a RecordWriter for writing to it.
   */
  @Override
  public RecordWriter<NullWritable, WARCWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new WARCWriter(context);
  }

  private class WARCWriter extends RecordWriter<NullWritable, WARCWritable> {
    private final WARCFileWriter writer;

    public WARCWriter(TaskAttemptContext context) throws IOException {
      Configuration conf = context.getConfiguration();
      CompressionCodec codec =
        getCompressOutput(context) ? WARCFileWriter.getGzipCodec(conf) : null;
      Path workFile = getDefaultWorkFile(context, "");
      this.writer = new WARCFileWriter(conf, codec, workFile);
    }

    @Override
    public void write(NullWritable key, WARCWritable value)
      throws IOException, InterruptedException {
      writer.write(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      writer.close();
    }
  }

}
