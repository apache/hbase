/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;

import com.google.common.annotations.VisibleForTesting;
/**
 * Create 3 level tree directory, first level is using table name as parent directory and then use
 * family name as child directory, and all related HFiles for one family are under child directory
 * -tableName1
 *   -columnFamilyName1
 *   -columnFamilyName2
 *     -HFiles
 * -tableName2
 *   -columnFamilyName1
 *     -HFiles
 *   -columnFamilyName2
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@VisibleForTesting
public class MultiHFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Cell> {
  private static final Log LOG = LogFactory.getLog(MultiHFileOutputFormat.class);

  @Override
  public RecordWriter<ImmutableBytesWritable, Cell>
  getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return createMultiHFileRecordWriter(context);
  }

  static <V extends Cell> RecordWriter<ImmutableBytesWritable, V>
  createMultiHFileRecordWriter(final TaskAttemptContext context) throws IOException {

    // Get the path of the output directory
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputDir = new FileOutputCommitter(outputPath, context).getWorkPath();
    final Configuration conf = context.getConfiguration();
    final FileSystem fs = outputDir.getFileSystem(conf);

    // Map of tables to writers
    final Map<ImmutableBytesWritable, RecordWriter<ImmutableBytesWritable, V>> tableWriters =
        new HashMap<ImmutableBytesWritable, RecordWriter<ImmutableBytesWritable, V>>();

    return new RecordWriter<ImmutableBytesWritable, V>() {
      @Override
      public void write(ImmutableBytesWritable tableName, V cell)
          throws IOException, InterruptedException {
        RecordWriter<ImmutableBytesWritable, V> tableWriter = tableWriters.get(tableName);
        // if there is new table, verify that table directory exists
        if (tableWriter == null) {
          // using table name as directory name
          final Path tableOutputDir = new Path(outputDir, Bytes.toString(tableName.copyBytes()));
          fs.mkdirs(tableOutputDir);
          LOG.info("Writing Table '" + tableName.toString() + "' data into following directory"
              + tableOutputDir.toString());

          // Create writer for one specific table
          tableWriter = new HFileOutputFormat2.HFileRecordWriter<V>(context, tableOutputDir);
          // Put table into map
          tableWriters.put(tableName, tableWriter);
        }
        // Write <Row, Cell> into tableWriter
        // in the original code, it does not use Row
        tableWriter.write(null, cell);
      }

      @Override
      public void close(TaskAttemptContext c) throws IOException, InterruptedException {
        for (RecordWriter<ImmutableBytesWritable, V> writer : tableWriters.values()) {
          writer.close(c);
        }
      }
    };
  }
}
