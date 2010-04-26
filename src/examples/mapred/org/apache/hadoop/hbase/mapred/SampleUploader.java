/*
 * Copyright 2009 The Apache Software Foundation
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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Sample uploader.
 * 
 * This is EXAMPLE code.  You will need to change it to work for your context.
 * 
 * Uses TableReduce to put the data into hbase. Change the InputFormat to suit
 * your data. Use the map to massage the input so it fits hbase.  Currently its
 * just a pass-through map.  In the reduce, you need to output a row and a
 * map of columns to cells.  Change map and reduce to suit your input.
 * 
 * <p>The below is wired up to handle an input whose format is a text file
 * which has a line format as follow:
 * <pre>
 * row columnname columndata
 * </pre>
 * 
 * <p>The table and columnfamily we're to insert into must preexist.
 * 
 * <p>Do the following to start the MR job:
 * <pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapred.SampleUploader /tmp/input.txt TABLE_NAME
 * </pre>
 * 
 * <p>This code was written against hbase 0.1 branch.
 */
public class SampleUploader extends MapReduceBase
implements Mapper<LongWritable, Text, ImmutableBytesWritable, HbaseMapWritable<byte [], byte []>>,
    Tool {
  private static final String NAME = "SampleUploader";
  private Configuration conf;

  public JobConf createSubmittableJob(String[] args)
  throws IOException {
    JobConf c = new JobConf(getConf(), SampleUploader.class);
    c.setJobName(NAME);
    FileInputFormat.setInputPaths(c, new Path(args[0]));
    c.setMapperClass(this.getClass());
    c.setMapOutputKeyClass(ImmutableBytesWritable.class);
    c.setMapOutputValueClass(HbaseMapWritable.class);
    c.setReducerClass(TableUploader.class);
    TableMapReduceUtil.initTableReduceJob(args[1], TableUploader.class, c);
    return c;
  } 
                                                                                                                                                                                                                                                                       
  public void map(LongWritable k, Text v,
      OutputCollector<ImmutableBytesWritable, HbaseMapWritable<byte [], byte []>> output,
      Reporter r)
  throws IOException {
     // Lines are space-delimited; first item is row, next the columnname and
     // then the third the cell value.
    String tmp = v.toString();
    if (tmp.length() == 0) {
      return;
    }
    String [] splits = v.toString().split(" ");
    HbaseMapWritable<byte [], byte []> mw =
      new HbaseMapWritable<byte [], byte []>();
    mw.put(Bytes.toBytes(splits[1]), Bytes.toBytes(splits[2]));
    byte [] row = Bytes.toBytes(splits[0]);
    r.setStatus("Map emitting " + splits[0] + " for record " + k.toString());
    output.collect(new ImmutableBytesWritable(row), mw);
  }

  public static class TableUploader extends MapReduceBase
  implements TableReduce<ImmutableBytesWritable, HbaseMapWritable<byte [], byte []>> {
    public void reduce(ImmutableBytesWritable k, Iterator<HbaseMapWritable<byte [], byte []>> v,
        OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
        Reporter r)
    throws IOException {
      while (v.hasNext()) {
        r.setStatus("Reducer committing " + k);
        BatchUpdate bu = new BatchUpdate(k.get());
        while (v.hasNext()) {
          HbaseMapWritable<byte [], byte []> hmw = v.next();
          for (Entry<byte [], byte []> e: hmw.entrySet()) {
            bu.put(e.getKey(), e.getValue());
          }
        }
        output.collect(k, bu);
      }
    }
  }

  static int printUsage() {
    System.out.println(NAME + " <input> <table_name>");
    return -1;
  } 

  public int run(@SuppressWarnings("unused") String[] args) throws Exception {
    // Make sure there are exactly 2 parameters left.
    if (args.length != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
        args.length + " instead of 2.");
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
    int errCode = ToolRunner.run(new Configuration(), new SampleUploader(),
      args);
    System.exit(errCode);
  }
}