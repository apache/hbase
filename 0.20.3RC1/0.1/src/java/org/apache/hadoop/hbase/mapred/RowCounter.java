/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A job with a map to count rows.
 * Map outputs table rows IF the input row has columns that have content.  
 * Uses an {@link IdentityReducer}
 */
public class RowCounter extends TableMap<Text, MapWritable> implements Tool {
  /* Name of this 'program'
   */
  static final String NAME = "rowcounter";
  
  private Configuration conf;
  private final MapWritable EMPTY_VALUE = new MapWritable();
  private static enum Counters {ROWS}
  
  @SuppressWarnings("unchecked")
  @Override
  public void map(final HStoreKey key,
    @SuppressWarnings("unused") final MapWritable value,
    final OutputCollector<Text, MapWritable> output,
    @SuppressWarnings("unused") final Reporter reporter)
  throws IOException {
    Text row = key.getRow();
    boolean content = false;
    for (Map.Entry e: value.entrySet()) {
      ImmutableBytesWritable ibw = (ImmutableBytesWritable)e.getValue();
      if (ibw != null && ibw.get().length > 0) {
        content = true;
        break;
      }
    }
    if (!content) {
      return;
    }
    // Give out same value every time.  We're only interested in the row/key
    reporter.incrCounter(Counters.ROWS, 1);
    output.collect(row, EMPTY_VALUE);
  }

  @SuppressWarnings("unused")
  public JobConf createSubmittableJob(String[] args) throws IOException {
    JobConf c = new JobConf(getConf(), RowCounter.class);
    c.setJobName(NAME);
    // Columns are space delimited
    StringBuilder sb = new StringBuilder();
    final int columnoffset = 2;
    for (int i = columnoffset; i < args.length; i++) {
      if (i > columnoffset) {
        sb.append(" ");
      }
      sb.append(args[i]);
    }
    // Second argument is the table name.
    TableMap.initJob(args[1], sb.toString(), this.getClass(), c);
    c.setReducerClass(IdentityReducer.class);
    // First arg is the output directory.
    c.setOutputPath(new Path(args[0]));
    return c;
  }
  
  static int printUsage() {
    System.out.println(NAME +
      " <outputdir> <tablename> <column1> [<column2>...]");
    return -1;
  }
  
  public int run(final String[] args) throws Exception {
    // Make sure there are at least 3 parameters
    if (args.length < 3) {
      System.err.println("ERROR: Wrong number of parameters: " + args.length);
      return printUsage();
    }
    JobClient.runJob(createSubmittableJob(args));
    return 0;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public void setConf(final Configuration c) {
    this.conf = c;
  }

  public static void main(String[] args) throws Exception {
    HBaseConfiguration c = new HBaseConfiguration();
    c.set("hbase.master", "durruti.local:60000");
    int errCode = ToolRunner.run(c, new RowCounter(), args);
    System.exit(errCode);
  }
}
