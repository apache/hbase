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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.TableInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool to replay WAL files as a M/R job.
 * The WAL can be replayed for a set of tables or all tables,
 * and a time range can be provided (in milliseconds).
 * The WAL is filtered to the passed set of tables and  the output
 * can optionally be mapped to another set of tables.
 *
 * WAL replay can also generate HFiles for later bulk importing,
 * in that case the WAL is replayed for a single table only.
 */
@InterfaceAudience.Public
public class WALPlayer extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(WALPlayer.class);
  final static String NAME = "WALPlayer";
  public final static String BULK_OUTPUT_CONF_KEY = "wal.bulk.output";
  public final static String TABLES_KEY = "wal.input.tables";
  public final static String TABLE_MAP_KEY = "wal.input.tablesmap";
  public final static String INPUT_FILES_SEPARATOR_KEY = "wal.input.separator";
  public final static String IGNORE_MISSING_FILES = "wal.input.ignore.missing.files";
  public final static String MULTI_TABLES_SUPPORT = "wal.multi.tables.support";

  protected static final String tableSeparator = ";";

  // This relies on Hadoop Configuration to handle warning about deprecated configs and
  // to set the correct non-deprecated configs when an old one shows up.
  static {
    Configuration.addDeprecation("hlog.bulk.output", BULK_OUTPUT_CONF_KEY);
    Configuration.addDeprecation("hlog.input.tables", TABLES_KEY);
    Configuration.addDeprecation("hlog.input.tablesmap", TABLE_MAP_KEY);
  }

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public WALPlayer() {
  }

  protected WALPlayer(final Configuration c) {
    super(c);
  }

  /**
   * A mapper that just writes out KeyValues. This one can be used together with
   * {@link CellSortReducer}
   */
  static class WALKeyValueMapper extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, Cell> {
    private Set<String> tableSet = new HashSet<String>();
    private boolean multiTableSupport = false;

    @Override
    public void map(WALKey key, WALEdit value, Context context) throws IOException {
      try {
        // skip all other tables
        TableName table = key.getTableName();
        if (tableSet.contains(table.getNameAsString())) {
          for (Cell cell : value.getCells()) {
            if (WALEdit.isMetaEditFamily(cell)) {
              continue;
            }
            byte[] outKey = multiTableSupport
                ? Bytes.add(table.getName(), Bytes.toBytes(tableSeparator), CellUtil.cloneRow(cell))
                : CellUtil.cloneRow(cell);
            context.write(new ImmutableBytesWritable(outKey), new MapReduceExtendedCell(cell));
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String[] tables = conf.getStrings(TABLES_KEY);
      this.multiTableSupport = conf.getBoolean(MULTI_TABLES_SUPPORT, false);
      for (String table : tables) {
        tableSet.add(table);
      }
    }
  }

  /**
   * A mapper that writes out {@link Mutation} to be directly applied to a running HBase instance.
   */
  protected static class WALMapper
      extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, Mutation> {
    private Map<TableName, TableName> tables = new TreeMap<>();

    @Override
    public void map(WALKey key, WALEdit value, Context context) throws IOException {
      try {
        if (tables.isEmpty() || tables.containsKey(key.getTableName())) {
          TableName targetTable =
              tables.isEmpty() ? key.getTableName() : tables.get(key.getTableName());
          ImmutableBytesWritable tableOut = new ImmutableBytesWritable(targetTable.getName());
          Put put = null;
          Delete del = null;
          Cell lastCell = null;
          for (Cell cell : value.getCells()) {
            // filtering WAL meta entries
            if (WALEdit.isMetaEditFamily(cell)) {
              continue;
            }

            // Allow a subclass filter out this cell.
            if (filter(context, cell)) {
              // A WALEdit may contain multiple operations (HBASE-3584) and/or
              // multiple rows (HBASE-5229).
              // Aggregate as much as possible into a single Put/Delete
              // operation before writing to the context.
              if (lastCell == null || lastCell.getTypeByte() != cell.getTypeByte()
                  || !CellUtil.matchingRows(lastCell, cell)) {
                // row or type changed, write out aggregate KVs.
                if (put != null) {
                  context.write(tableOut, put);
                }
                if (del != null) {
                  context.write(tableOut, del);
                }
                if (CellUtil.isDelete(cell)) {
                  del = new Delete(CellUtil.cloneRow(cell));
                } else {
                  put = new Put(CellUtil.cloneRow(cell));
                }
              }
              if (CellUtil.isDelete(cell)) {
                del.add(cell);
              } else {
                put.add(cell);
              }
            }
            lastCell = cell;
          }
          // write residual KVs
          if (put != null) {
            context.write(tableOut, put);
          }
          if (del != null) {
            context.write(tableOut, del);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    protected boolean filter(Context context, final Cell cell) {
      return true;
    }

    @Override
    protected void
        cleanup(Mapper<WALKey, WALEdit, ImmutableBytesWritable, Mutation>.Context context)
            throws IOException, InterruptedException {
      super.cleanup(context);
    }

    @Override
    public void setup(Context context) throws IOException {
      String[] tableMap = context.getConfiguration().getStrings(TABLE_MAP_KEY);
      String[] tablesToUse = context.getConfiguration().getStrings(TABLES_KEY);
      if (tableMap == null) {
        tableMap = tablesToUse;
      }
      if (tablesToUse == null) {
        // Then user wants all tables.
      } else if (tablesToUse.length != tableMap.length) {
        // this can only happen when WALMapper is used directly by a class other than WALPlayer
        throw new IOException("Incorrect table mapping specified .");
      }
      int i = 0;
      if (tablesToUse != null) {
        for (String table : tablesToUse) {
          tables.put(TableName.valueOf(table), TableName.valueOf(tableMap[i++]));
        }
      }
    }
  }

  void setupTime(Configuration conf, String option) throws IOException {
    String val = conf.get(option);
    if (null == val) {
      return;
    }
    long ms;
    try {
      // first try to parse in user friendly form
      ms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS").parse(val).getTime();
    } catch (ParseException pe) {
      try {
        // then see if just a number of ms's was specified
        ms = Long.parseLong(val);
      } catch (NumberFormatException nfe) {
        throw new IOException(
            option + " must be specified either in the form 2001-02-20T16:35:06.99 "
                + "or as number of milliseconds");
      }
    }
    conf.setLong(option, ms);
  }

  /**
   * Sets up the actual job.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();
    setupTime(conf, WALInputFormat.START_TIME_KEY);
    setupTime(conf, WALInputFormat.END_TIME_KEY);
    String inputDirs = args[0];
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
    conf.set(FileInputFormat.INPUT_DIR, inputDirs);
    Job job =
        Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + System.currentTimeMillis()));
    job.setJarByClass(WALPlayer.class);

    job.setInputFormatClass(WALInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);

    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      LOG.debug("add incremental job :" + hfileOutPath + " from " + inputDirs);

      // the bulk HFile case
      List<TableName> tableNames = getTableNameList(tables);

      job.setMapperClass(WALKeyValueMapper.class);
      job.setReducerClass(CellSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(MapReduceExtendedCell.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);) {
        List<TableInfo> tableInfoList = new ArrayList<TableInfo>();
        for (TableName tableName : tableNames) {
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName);
          tableInfoList.add(new TableInfo(table.getDescriptor(), regionLocator));
        }
        MultiTableHFileOutputFormat.configureIncrementalLoad(job, tableInfoList);
      }
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
        org.apache.hbase.thirdparty.com.google.common.base.Preconditions.class);
    } else {
      // output to live cluster
      job.setMapperClass(WALMapper.class);
      job.setOutputFormatClass(MultiTableOutputFormat.class);
      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.initCredentials(job);
      // No reducers.
      job.setNumReduceTasks(0);
    }
    String codecCls = WALCellCodec.getWALCellCodecClass(conf);
    try {
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
        Class.forName(codecCls));
    } catch (Exception e) {
      throw new IOException("Cannot determine wal codec class " + codecCls, e);
    }
    return job;
  }

  private List<TableName> getTableNameList(String[] tables) {
    List<TableName> list = new ArrayList<TableName>();
    for (String name : tables) {
      list.add(TableName.valueOf(name));
    }
    return list;
  }

  /**
   * Print usage
   * @param errorMsg Error message. Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <wal inputdir> <tables> [<tableMappings>]");
    System.err.println("Replay all WAL files into HBase.");
    System.err.println("<tables> is a comma separated list of tables.");
    System.err.println("If no tables (\"\") are specified, all tables are imported.");
    System.err.println("(Be careful, hbase:meta entries will be imported in this case.)\n");
    System.err.println("WAL entries can be mapped to new set of tables via <tableMappings>.");
    System.err.println("<tableMappings> is a comma separated list of target tables.");
    System.err.println("If specified, each table in <tables> must have a mapping.\n");
    System.err.println("By default " + NAME + " will load data directly into HBase.");
    System.err.println("To generate HFiles for a bulk data load instead, pass the following option:");
    System.err.println("  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println("  (Only one table can be specified, and no mapping is allowed!)");
    System.err.println("Time range options:");
    System.err.println("  -D" + WALInputFormat.START_TIME_KEY + "=[date|ms]");
    System.err.println("  -D" + WALInputFormat.END_TIME_KEY + "=[date|ms]");
    System.err.println("  (The start and the end date of timerange. The dates can be expressed");
    System.err.println("  in milliseconds since epoch or in yyyy-MM-dd'T'HH:mm:ss.SS format.");
    System.err.println("  E.g. 1234567890120 or 2009-02-13T23:32:30.12)");
    System.err.println("Other options:");
    System.err.println("  -D" + JOB_NAME_CONF_KEY + "=jobName");
    System.err.println("  Use the specified mapreduce job name for the wal player");
    System.err.println("For performance also consider the following options:\n"
        + "  -Dmapreduce.map.speculative=false\n"
        + "  -Dmapreduce.reduce.speculative=false");
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new WALPlayer(HBaseConfiguration.create()), args);
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
}
