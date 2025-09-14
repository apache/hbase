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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.apache.hadoop.hbase.mapreduce.WALPlayer.TABLES_KEY;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.TABLE_MAP_KEY;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupFileSystemManager;
import org.apache.hadoop.hbase.backup.util.BulkLoadProcessor;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BulkLoadCollectorJob extends Configured implements Tool {
  public static final String NAME = "BulkLoadCollector";
  public static final String DEFAULT_REDUCERS = "1";

  public BulkLoadCollectorJob() {
  }

  protected BulkLoadCollectorJob(final Configuration c) {
    super(c);
  }

  public static class BulkLoadCollectorMapper extends Mapper<WALKey, WALEdit, Text, NullWritable> {
    private final Map<TableName, TableName> tables = new TreeMap<>();
    private final Text out = new Text();

    @Override
    protected void map(WALKey key, WALEdit value, Context context)
      throws IOException, InterruptedException {
      if (key == null || value == null) return;

      if (!(tables.isEmpty() || tables.containsKey(key.getTableName()))) {
        return;
      }

      // delegate extraction to BulkLoadProcessor helper for a single WAL entry
      List<Path> relativePaths = BulkLoadProcessor.processBulkLoadFiles(key, value);
      if (relativePaths.isEmpty()) return;

      Path walInputPath = ((FileSplit) context.getInputSplit()).getPath();

      for (Path rel : relativePaths) {
        Path full = BackupFileSystemManager.resolveBulkLoadFullPath(walInputPath, rel);
        out.set(full.toString());
        context.write(out, NullWritable.get());
        context.getCounter("BulkCollector", "StoreFilesEmitted").increment(1);
      }
    }

    @Override
    protected void setup(Context context) throws IOException {
      String[] tableMap = context.getConfiguration().getStrings(TABLES_KEY);
      String[] tablesToUse = context.getConfiguration().getStrings(TABLES_KEY);
      if (tableMap == null) {
        tableMap = tablesToUse;
      }
      if (tablesToUse == null) {
        // user requested all tables; tables map remains empty to indicate "all"
        return;
      }

      if (tablesToUse.length != tableMap.length) {
        throw new IOException("Incorrect table mapping specified.");
      }

      int i = 0;
      for (String table : tablesToUse) {
        tables.put(TableName.valueOf(table), TableName.valueOf(tableMap[i++]));
      }
    }
  }

  public static class DedupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context ctx)
      throws IOException, InterruptedException {
      // write the path once
      ctx.write(key, NullWritable.get());
    }
  }

  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();

    setupTime(conf, WALInputFormat.START_TIME_KEY);
    setupTime(conf, WALInputFormat.END_TIME_KEY);

    if (args == null || args.length < 2) {
      throw new IOException(
        "Usage: <WAL inputdir> <bulk-files-output-dir> [<tables> [<tableMappings>]]");
    }

    String inputDirs = args[0];
    String bulkFilesOut = args[1];

    // tables are optional (args[2])
    String[] tables = (args.length == 2) ? new String[] {} : args[2].split(",");
    String[] tableMap;
    if (args.length > 3) {
      tableMap = args[3].split(",");
      if (tableMap.length != tables.length) {
        throw new IOException("The same number of tables and mapping must be provided.");
      }
    } else {
      // if no mapping is specified, map each table to itself
      tableMap = tables;
    }

    conf.setStrings(TABLES_KEY, tables);
    conf.setStrings(TABLE_MAP_KEY, tableMap);
    conf.set(FileInputFormat.INPUT_DIR, inputDirs);

    // create and return the actual Job configured for bulk-file discovery
    return BulkLoadCollectorJob.createSubmittableJob(conf, inputDirs, bulkFilesOut);
  }

  private static Job createSubmittableJob(Configuration conf, String inputDirs, String bulkFilesOut)
    throws IOException {
    if (bulkFilesOut == null || bulkFilesOut.isEmpty()) {
      throw new IOException("bulkFilesOut (output dir) must be provided.");
    }
    if (inputDirs == null || inputDirs.isEmpty()) {
      throw new IOException("inputDirs (WAL input dir) must be provided.");
    }

    Job job = Job.getInstance(conf, NAME + "_" + System.currentTimeMillis());
    job.setJarByClass(BulkLoadCollectorJob.class);

    // Input
    job.setInputFormatClass(WALInputFormat.class);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.setInputPaths(job, inputDirs);

    // Mapper/Reducer (inner classes)
    job.setMapperClass(BulkLoadCollectorMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(DedupReducer.class);
    // default to a single reducer (single deduped file); callers can override conf
    // "mapreduce.job.reduces"
    int reducers = conf.getInt("mapreduce.job.reduces", Integer.parseInt(DEFAULT_REDUCERS));
    job.setNumReduceTasks(reducers);

    job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(bulkFilesOut));

    return job;
  }

  /**
   * Parse time option (supports yyyy-MM-dd'T'HH:mm:ss.SS or milliseconds). Copied behavior from
   * WALPlayer.setupTime.
   */
  private void setupTime(Configuration conf, String option) throws IOException {
    String val = conf.get(option);
    if (val == null) {
      return;
    }
    long ms;
    try {
      // first try to parse in user-friendly form
      ms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS").parse(val).getTime();
    } catch (ParseException pe) {
      try {
        // fallback to milliseconds
        ms = Long.parseLong(val);
      } catch (NumberFormatException nfe) {
        throw new IOException(
          option + " must be specified either in the form 2001-02-20T16:35:06.99 "
            + "or as number of milliseconds");
      }
    }
    conf.setLong(option, ms);
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new BulkLoadCollectorJob(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage("Wrong number of arguments: " + args.length);
      System.exit(-1);
    }

    Job job = createSubmittableJob(args);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * Print usage/help for the BulkLoadCollectorJob CLI/driver. args layout: args[0] = input
   * directory (required) args[1] = output directory (required) args[2] = tables (comma-separated)
   * (optional) args[3] = tableMappings (comma-separated) (optional; must match tables length)
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && !errorMsg.isEmpty()) {
      System.err.println("ERROR: " + errorMsg);
    }

    System.err.println(
      "Usage: " + NAME + " <WAL inputdir> <bulk-files-output-dir> [<tables> [<tableMappings>]]");
    System.err.println(
      "  <WAL inputdir>             directory of WALs to scan (comma-separated list accepted)");
    System.err.println(
      "  <bulk-files-output-dir>    directory to write discovered store-file paths (output)");
    System.err.println(
      "  <tables>                   optional comma-separated list of tables to include; if omitted, all tables are processed");
    System.err.println(
      "  <tableMappings>            optional comma-separated list of mapped target tables; must match number of tables");

    System.err.println();
    System.err.println("Time range options (either milliseconds or yyyy-MM-dd'T'HH:mm:ss.SS):");
    System.err.println("  -D" + WALInputFormat.START_TIME_KEY + "=[date|ms]");
    System.err.println("  -D" + WALInputFormat.END_TIME_KEY + "=[date|ms]");

    System.err.println();
    System.err.println("Configuration alternatives (can be provided via -D):");
    System.err
      .println("  -D" + TABLES_KEY + "=<comma-separated-tables>         (alternative to arg[2])");
    System.err
      .println("  -D" + TABLE_MAP_KEY + "=<comma-separated-mappings>     (alternative to arg[3])");
    System.err.println(
      "  -Dmapreduce.job.reduces=<N>                            (number of reducers; default 1)");
    System.err.println();

    System.err.println("Performance hints:");
    System.err.println("  For large inputs consider disabling speculative execution:");
    System.err
      .println("    -Dmapreduce.map.speculative=false -Dmapreduce.reduce.speculative=false");

    System.err.println();
    System.err.println("Example:");
    System.err.println(
      "  " + NAME + " /wals/input /out/bulkfiles ns:tbl1,ns:tbl2 ns:tbl1_mapped,ns:tbl2_mapped");
  }
}
