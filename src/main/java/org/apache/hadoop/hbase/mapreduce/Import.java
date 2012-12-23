/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Import data written by {@link Export}.
 */
public class Import {
  final static String NAME = "import";
  final static String CF_RENAME_PROP = "HBASE_IMPORTER_RENAME_CFS";
  final static String BULK_OUTPUT_CONF_KEY = "import.bulk.output";

  /**
   * A mapper that just writes out KeyValues.
   */
  static class KeyValueImporter
  extends TableMapper<ImmutableBytesWritable, KeyValue> {
    private Map<byte[], byte[]> cfRenameMap;
      
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        for (KeyValue kv : value.raw()) {
          context.write(row, convertKv(kv, cfRenameMap));
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) {
      cfRenameMap = createCfRenameMap(context.getConfiguration());
    }
  }

  /**
   * Write table content out to files in hdfs.
   */
  static class Importer
  extends TableMapper<ImmutableBytesWritable, Mutation> {
    private Map<byte[], byte[]> cfRenameMap;
      
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        writeResult(row, value, context);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private void writeResult(ImmutableBytesWritable key, Result result, Context context)
    throws IOException, InterruptedException {
      Put put = null;
      Delete delete = null;
      for (KeyValue kv : result.raw()) {
        kv = convertKv(kv, cfRenameMap);
        // Deletes and Puts are gathered and written when finished
        if (kv.isDelete()) {
          if (delete == null) {
            delete = new Delete(key.get());
          }
          delete.addDeleteMarker(kv);
        } else {
          if (put == null) { 
            put = new Put(key.get());
          }
          put.add(kv);
        }
      }
      if (put != null) {
        context.write(key, put);
      }
      if (delete != null) {
        context.write(key, delete);
      }
    }

    @Override
    public void setup(Context context) {
      cfRenameMap = createCfRenameMap(context.getConfiguration());
    }
  }

  // helper: create a new KeyValue based on CF rename map
  private static KeyValue convertKv(KeyValue kv, Map<byte[], byte[]> cfRenameMap) {
    if(cfRenameMap != null) {
      // If there's a rename mapping for this CF, create a new KeyValue
      byte[] newCfName = cfRenameMap.get(kv.getFamily());
      if(newCfName != null) {
          kv = new KeyValue(kv.getBuffer(), // row buffer 
                  kv.getRowOffset(),        // row offset
                  kv.getRowLength(),        // row length
                  newCfName,                // CF buffer
                  0,                        // CF offset 
                  newCfName.length,         // CF length 
                  kv.getBuffer(),           // qualifier buffer
                  kv.getQualifierOffset(),  // qualifier offset
                  kv.getQualifierLength(),  // qualifier length
                  kv.getTimestamp(),        // timestamp
                  KeyValue.Type.codeToType(kv.getType()), // KV Type
                  kv.getBuffer(),           // value buffer 
                  kv.getValueOffset(),      // value offset
                  kv.getValueLength());     // value length
      }
    }
    return kv;
  }

  // helper: make a map from sourceCfName to destCfName by parsing a config key
  private static Map<byte[], byte[]> createCfRenameMap(Configuration conf) {
    Map<byte[], byte[]> cfRenameMap = null;
    String allMappingsPropVal = conf.get(CF_RENAME_PROP);
    if(allMappingsPropVal != null) {
      // The conf value format should be sourceCf1:destCf1,sourceCf2:destCf2,...
      String[] allMappings = allMappingsPropVal.split(",");
      for (String mapping: allMappings) {
        if(cfRenameMap == null) {
            cfRenameMap = new TreeMap<byte[],byte[]>(Bytes.BYTES_COMPARATOR);
        }
        String [] srcAndDest = mapping.split(":");
        if(srcAndDest.length != 2) {
            continue;
        }
        cfRenameMap.put(srcAndDest[0].getBytes(), srcAndDest[1].getBytes());
      }
    }
    return cfRenameMap;
  }

  /**
   * <p>Sets a configuration property with key {@link #CF_RENAME_PROP} in conf that tells
   * the mapper how to rename column families.
   * 
   * <p>Alternately, instead of calling this function, you could set the configuration key 
   * {@link #CF_RENAME_PROP} yourself. The value should look like 
   * <pre>srcCf1:destCf1,srcCf2:destCf2,....</pre>. This would have the same effect on
   * the mapper behavior.
   * 
   * @param conf the Configuration in which the {@link #CF_RENAME_PROP} key will be
   *  set
   * @param renameMap a mapping from source CF names to destination CF names
   */
  static public void configureCfRenaming(Configuration conf, 
          Map<String, String> renameMap) {
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<String,String> entry: renameMap.entrySet()) {
      String sourceCf = entry.getKey();
      String destCf = entry.getValue();

      if(sourceCf.contains(":") || sourceCf.contains(",") || 
              destCf.contains(":") || destCf.contains(",")) {
        throw new IllegalArgumentException("Illegal character in CF names: " 
              + sourceCf + ", " + destCf);
      }

      if(sb.length() != 0) {
        sb.append(",");
      }
      sb.append(sourceCf + ":" + destCf);
    }
    conf.set(CF_RENAME_PROP, sb.toString());
  }
  

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    Path inputDir = new Path(args[1]);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(Importer.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      job.setMapperClass(KeyValueImporter.class);
      HTable table = new HTable(conf, tableName);
      job.setReducerClass(KeyValueSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
          com.google.common.base.Preconditions.class);
    } else {
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // because it sets up the TableOutputFormat.
      job.setMapperClass(Importer.class);
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: Import [options] <tablename> <inputdir>");
    System.err.println("By default Import will load data directly into HBase. To instead generate");
    System.err.println("HFiles of data to prepare for a bulk data load, pass the option:");
    System.err.println("  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println("For performance consider the following options:\n"
        + "  -Dmapred.map.tasks.speculative.execution=false\n"
        + "  -Dmapred.reduce.tasks.speculative.execution=false");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}