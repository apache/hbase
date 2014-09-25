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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A tool to replay WAL files as a M/R job.
 * The WAL can be replayed for a set of tables or all tables,
 * and a timerange can be provided (in milliseconds).
 * The WAL is filtered to the passed set of tables and  the output
 * can optionally be mapped to another set of tables.
 *
 * WAL replay can also generate HFiles for later bulk importing,
 * in that case the WAL is replayed for a single table only.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WALPlayer extends Configured implements Tool {
  final static String NAME = "WALPlayer";
  final static String BULK_OUTPUT_CONF_KEY = "hlog.bulk.output";
  final static String HLOG_INPUT_KEY = "hlog.input.dir";
  final static String TABLES_KEY = "hlog.input.tables";
  final static String TABLE_MAP_KEY = "hlog.input.tablesmap";

  /**
   * A mapper that just writes out KeyValues.
   * This one can be used together with {@link KeyValueSortReducer}
   */
  static class HLogKeyValueMapper
  extends Mapper<HLogKey, WALEdit, ImmutableBytesWritable, KeyValue> {
    private byte[] table;

    @Override
    public void map(HLogKey key, WALEdit value,
      Context context)
    throws IOException {
      try {
        // skip all other tables
        if (Bytes.equals(table, key.getTablename().getName())) {
          for (KeyValue kv : value.getKeyValues()) {
            if (WALEdit.isMetaEditFamily(kv.getFamily())) continue;
            context.write(new ImmutableBytesWritable(kv.getRow()), kv);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      // only a single table is supported when HFiles are generated with HFileOutputFormat
      String tables[] = context.getConfiguration().getStrings(TABLES_KEY);
      if (tables == null || tables.length != 1) {
        // this can only happen when HLogMapper is used directly by a class other than WALPlayer
        throw new IOException("Exactly one table must be specified for bulk HFile case.");
      }
      table = Bytes.toBytes(tables[0]);
    }
  }

  /**
   * A mapper that writes out {@link Mutation} to be directly applied to
   * a running HBase instance.
   */
  static class HLogMapper
  extends Mapper<HLogKey, WALEdit, ImmutableBytesWritable, Mutation> {
    private Map<TableName, TableName> tables =
        new TreeMap<TableName, TableName>();

    @Override
    public void map(HLogKey key, WALEdit value,
      Context context)
    throws IOException {
      try {
        if (tables.isEmpty() || tables.containsKey(key.getTablename())) {
          TableName targetTable = tables.isEmpty() ?
                key.getTablename() :
                tables.get(key.getTablename());
          ImmutableBytesWritable tableOut = new ImmutableBytesWritable(targetTable.getName());
          Put put = null;
          Delete del = null;
          KeyValue lastKV = null;
          for (KeyValue kv : value.getKeyValues()) {
            // filtering HLog meta entries
            if (WALEdit.isMetaEditFamily(kv.getFamily())) continue;

            // A WALEdit may contain multiple operations (HBASE-3584) and/or
            // multiple rows (HBASE-5229).
            // Aggregate as much as possible into a single Put/Delete
            // operation before writing to the context.
            if (lastKV == null || lastKV.getType() != kv.getType() || !lastKV.matchingRow(kv)) {
              // row or type changed, write out aggregate KVs.
              if (put != null) context.write(tableOut, put);
              if (del != null) context.write(tableOut, del);

              if (kv.isDelete()) {
                del = new Delete(kv.getRow());
              } else {
                put = new Put(kv.getRow());
              }
            }
            if (kv.isDelete()) {
              del.addDeleteMarker(kv);
            } else {
              put.add(kv);
            }
            lastKV = kv;
          }
          // write residual KVs
          if (put != null) context.write(tableOut, put);
          if (del != null) context.write(tableOut, del);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      String[] tableMap = context.getConfiguration().getStrings(TABLE_MAP_KEY);
      String[] tablesToUse = context.getConfiguration().getStrings(TABLES_KEY);
      if (tablesToUse == null || tableMap == null || tablesToUse.length != tableMap.length) {
        // this can only happen when HLogMapper is used directly by a class other than WALPlayer
        throw new IOException("No tables or incorrect table mapping specified.");
      }
      int i = 0;
      for (String table : tablesToUse) {
        tables.put(TableName.valueOf(table),
            TableName.valueOf(tableMap[i++]));
      }
    }
  }

  /**
   * @param conf The {@link Configuration} to use.
   */
  public WALPlayer(Configuration conf) {
    super(conf);
  }

  void setupTime(Configuration conf, String option) throws IOException {
    String val = conf.get(option);
    if (val == null) return;
    long ms;
    try {
      // first try to parse in user friendly form
      ms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS").parse(val).getTime();
    } catch (ParseException pe) {
      try {
        // then see if just a number of ms's was specified
        ms = Long.parseLong(val);
      } catch (NumberFormatException nfe) {
        throw new IOException(option
            + " must be specified either in the form 2001-02-20T16:35:06.99 "
            + "or as number of milliseconds");
      }
    }
    conf.setLong(option, ms);
  }

  /**
   * Sets up the actual job.
   *
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args)
  throws IOException {
    Configuration conf = getConf();
    setupTime(conf, HLogInputFormat.START_TIME_KEY);
    setupTime(conf, HLogInputFormat.END_TIME_KEY);
    Path inputDir = new Path(args[0]);
    String[] tables = args[1].split(",");
    String[] tableMap;
    if (args.length > 2) {
      tableMap = args[2].split(",");
      if (tableMap.length != tables.length) {
        throw new IOException("The same number of tables and mapping must be provided.");
      }
    } else {
      // if not mapping is specified map each table to itself
      tableMap = tables;
    }
    conf.setStrings(TABLES_KEY, tables);
    conf.setStrings(TABLE_MAP_KEY, tableMap);
    Job job = new Job(conf, NAME + "_" + inputDir);
    job.setJarByClass(WALPlayer.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(HLogInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      // the bulk HFile case
      if (tables.length != 1) {
        throw new IOException("Exactly one table must be specified for the bulk export option");
      }
      HTable table = new HTable(conf, tables[0]);
      job.setMapperClass(HLogKeyValueMapper.class);
      job.setReducerClass(KeyValueSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(KeyValue.class);
      HFileOutputFormat.configureIncrementalLoad(job, table);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
          com.google.common.base.Preconditions.class);
    } else {
      // output to live cluster
      job.setMapperClass(HLogMapper.class);
      job.setOutputFormatClass(MultiTableOutputFormat.class);
      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.initCredentials(job);
      // No reducers.
      job.setNumReduceTasks(0);
    }
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <wal inputdir> <tables> [<tableMappings>]");
    System.err.println("Read all WAL entries for <tables>.");
    System.err.println("If no tables (\"\") are specific, all tables are imported.");
    System.err.println("(Careful, even -ROOT- and hbase:meta entries will be imported in that case.)");
    System.err.println("Otherwise <tables> is a comma separated list of tables.\n");
    System.err.println("The WAL entries can be mapped to new set of tables via <tableMapping>.");
    System.err.println("<tableMapping> is a command separated list of targettables.");
    System.err.println("If specified, each table in <tables> must have a mapping.\n");
    System.err.println("By default " + NAME + " will load data directly into HBase.");
    System.err.println("To generate HFiles for a bulk data load instead, pass the option:");
    System.err.println("  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println("  (Only one table can be specified, and no mapping is allowed!)");
    System.err.println("Other options: (specify time range to WAL edit to consider)");
    System.err.println("  -D" + HLogInputFormat.START_TIME_KEY + "=[date|ms]");
    System.err.println("  -D" + HLogInputFormat.END_TIME_KEY + "=[date|ms]");
    System.err.println("For performance also consider the following options:\n"
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
    int ret = ToolRunner.run(new WALPlayer(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(otherArgs);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
