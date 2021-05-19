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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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


  // This relies on Hadoop Configuration to handle warning about deprecated configs and
  // to set the correct non-deprecated configs when an old one shows up.
  static {
    Configuration.addDeprecation("hlog.bulk.output", BULK_OUTPUT_CONF_KEY);
    Configuration.addDeprecation("hlog.input.tables", TABLES_KEY);
    Configuration.addDeprecation("hlog.input.tablesmap", TABLE_MAP_KEY);
  }

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public WALPlayer(){
  }

  protected WALPlayer(final Configuration c) {
    super(c);
  }

  /**
   * A mapper that just writes out KeyValues.
   * This one can be used together with {@link KeyValueSortReducer}
   * @deprecated Use {@link WALCellMapper}. Will  be removed from 3.0 onwards
   */
  @Deprecated
  static class WALKeyValueMapper
    extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, KeyValue> {
    private byte[] table;

    @Override
    public void map(WALKey key, WALEdit value,
      Context context)
    throws IOException {
      try {
        // skip all other tables
        if (Bytes.equals(table, key.getTableName().getName())) {
          for (Cell cell : value.getCells()) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            if (WALEdit.isMetaEditFamily(kv)) {
              continue;
            }
            context.write(new ImmutableBytesWritable(CellUtil.cloneRow(kv)), kv);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      // only a single table is supported when HFiles are generated with HFileOutputFormat
      String[] tables = context.getConfiguration().getStrings(TABLES_KEY);
      if (tables == null || tables.length != 1) {
        // this can only happen when WALMapper is used directly by a class other than WALPlayer
        throw new IOException("Exactly one table must be specified for bulk HFile case.");
      }
      table = Bytes.toBytes(tables[0]);

    }

  }
  /**
   * A mapper that just writes out Cells.
   * This one can be used together with {@link CellSortReducer}
   */
  static class WALCellMapper
    extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, Cell> {
    private byte[] table;

    @Override
    public void map(WALKey key, WALEdit value,
      Context context)
    throws IOException {
      try {
        // skip all other tables
        if (Bytes.equals(table, key.getTableName().getName())) {
          for (Cell cell : value.getCells()) {
            if (WALEdit.isMetaEditFamily(cell)) {
              continue;
            }
            context.write(new ImmutableBytesWritable(CellUtil.cloneRow(cell)),
              new MapReduceExtendedCell(cell));
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      // only a single table is supported when HFiles are generated with HFileOutputFormat
      String[] tables = context.getConfiguration().getStrings(TABLES_KEY);
      if (tables == null || tables.length != 1) {
        // this can only happen when WALMapper is used directly by a class other than WALPlayer
        throw new IOException("Exactly one table must be specified for bulk HFile case.");
      }
      table = Bytes.toBytes(tables[0]);

    }

  }

  /**
   * Enum for map metrics.  Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {
    /** Number of aggregated writes */
    PUTS,
    /** Number of aggregated deletes */
    DELETES,
    CELLS_READ,
    CELLS_WRITTEN,
    WALEDITS
  }

  /**
   * A mapper that writes out {@link Mutation} to be directly applied to
   * a running HBase instance.
   */
  protected static class WALMapper
  extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, Mutation> {
    private Map<TableName, TableName> tables = new TreeMap<>();

    @Override
    public void map(WALKey key, WALEdit value, Context context)
    throws IOException {
      context.getCounter(Counter.WALEDITS).increment(1);
      try {
        if (tables.isEmpty() || tables.containsKey(key.getTableName())) {
          TableName targetTable = tables.isEmpty() ?
                key.getTableName() :
                tables.get(key.getTableName());
          ImmutableBytesWritable tableOut = new ImmutableBytesWritable(targetTable.getName());
          Put put = null;
          Delete del = null;
          Cell lastCell = null;
          for (Cell cell : value.getCells()) {
            context.getCounter(Counter.CELLS_READ).increment(1);
            // Filtering WAL meta marker entries.
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
                  context.getCounter(Counter.PUTS).increment(1);
                }
                if (del != null) {
                  context.write(tableOut, del);
                  context.getCounter(Counter.DELETES).increment(1);
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
              context.getCounter(Counter.CELLS_WRITTEN).increment(1);
            }
            lastCell = cell;
          }
          // write residual KVs
          if (put != null) {
            context.write(tableOut, put);
            context.getCounter(Counter.PUTS).increment(1);
          }
          if (del != null) {
            context.getCounter(Counter.DELETES).increment(1);
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

    @SuppressWarnings("checkstyle:EmptyBlock")
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
          tables.put(TableName.valueOf(table),
            TableName.valueOf(tableMap[i++]));
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
  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();
    setupTime(conf, WALInputFormat.START_TIME_KEY);
    setupTime(conf, WALInputFormat.END_TIME_KEY);
    String inputDirs = args[0];
    String[] tables = args.length == 1? new String [] {}: args[1].split(",");
    String[] tableMap;
    if (args.length > 2) {
      tableMap = args[2].split(",");
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
    Job job =
        Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + System.currentTimeMillis()));
    job.setJarByClass(WALPlayer.class);

    job.setInputFormatClass(WALInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);

    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      LOG.debug("add incremental job :" + hfileOutPath + " from " + inputDirs);

      // the bulk HFile case
      if (tables.length != 1) {
        throw new IOException("Exactly one table must be specified for the bulk export option");
      }
      TableName tableName = TableName.valueOf(tables[0]);
      job.setMapperClass(WALCellMapper.class);
      job.setReducerClass(CellSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(MapReduceExtendedCell.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
        HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
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
    String codecCls = WALCellCodec.getWALCellCodecClass(conf).getName();
    try {
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(), Class.forName(codecCls));
    } catch (Exception e) {
      throw new IOException("Cannot determine wal codec class " + codecCls, e);
    }
    return job;
  }


  /**
   * Print usage
   * @param errorMsg Error message.  Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <WAL inputdir> [<tables> <tableMappings>]");
    System.err.println(" <WAL inputdir>   directory of WALs to replay.");
    System.err.println(" <tables>         comma separated list of tables. If no tables specified,");
    System.err.println("                  all are imported (even hbase:meta if present).");
    System.err.println(" <tableMappings>  WAL entries can be mapped to a new set of tables by " +
      "passing");
    System.err.println("                  <tableMappings>, a comma separated list of target " +
      "tables.");
    System.err.println("                  If specified, each table in <tables> must have a " +
      "mapping.");
    System.err.println("To generate HFiles to bulk load instead of loading HBase directly, pass:");
    System.err.println(" -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println(" Only one table can be specified, and no mapping allowed!");
    System.err.println("To specify a time range, pass:");
    System.err.println(" -D" + WALInputFormat.START_TIME_KEY + "=[date|ms]");
    System.err.println(" -D" + WALInputFormat.END_TIME_KEY + "=[date|ms]");
    System.err.println(" The start and the end date of timerange (inclusive). The dates can be");
    System.err.println(" expressed in milliseconds-since-epoch or yyyy-MM-dd'T'HH:mm:ss.SS " +
      "format.");
    System.err.println(" E.g. 1234567890120 or 2009-02-13T23:32:30.12");
    System.err.println("Other options:");
    System.err.println(" -D" + JOB_NAME_CONF_KEY + "=jobName");
    System.err.println(" Use the specified mapreduce job name for the wal player");
    System.err.println(" -Dwal.input.separator=' '");
    System.err.println(" Change WAL filename separator (WAL dir names use default ','.)");
    System.err.println("For performance also consider the following options:\n"
        + "  -Dmapreduce.map.speculative=false\n"
        + "  -Dmapreduce.reduce.speculative=false");
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
    if (args.length < 1) {
      usage("Wrong number of arguments: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(args);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
