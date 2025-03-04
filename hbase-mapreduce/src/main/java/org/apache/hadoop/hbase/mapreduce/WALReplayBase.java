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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
public class WALReplayBase<V, T extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, V>,
  U extends OutputFormat<ImmutableBytesWritable, V>> extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(WALReplayBase.class);
  final static String WAL_DIR = "WALs";
  final static String BULKLOAD_FILES = "bulk-load-files";
  public final static String BULK_OUTPUT_CONF_KEY = "wal.bulk.output";
  public final static String TABLES_KEY = "wal.input.tables";
  public final static String TABLE_MAP_KEY = "wal.input.tablesmap";
  public final static String BULKLOAD_BACKUP_LOCATION = "wal.bulk.backup.location";
  public final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  private Class<? extends Mapper> mapperClass;
  private Class<? extends OutputFormat> outputFormatClass;

  // Class<? extends Mapper> cls
  protected WALReplayBase(final Configuration c, Class<? extends Mapper> mapper,
    Class<? extends OutputFormat> outputFormat) {
    super(c);
    this.mapperClass = mapper;
    this.outputFormatClass = outputFormat;
  }

  public WALReplayBase(Class<? extends Mapper> mapper, Class<? extends OutputFormat> outputFormat) {
    this.mapperClass = mapper;
    this.outputFormatClass = outputFormat;
  }

  protected static class WALMapperBase<T>
    extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, T> {
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
    String bulkLoadFilesDir = new Path(inputDirs, BULKLOAD_FILES).toString();
    String[] tables = args.length == 1 ? new String[] {} : args[1].split(",");
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
    conf.set(BULKLOAD_BACKUP_LOCATION, bulkLoadFilesDir);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY,
      this.getClass().getSimpleName() + "_" + EnvironmentEdgeManager.currentTime()));
    job.setJarByClass(this.getClass());
    job.setInputFormatClass(WALInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);

    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      LOG.debug("add incremental job :" + hfileOutPath + " from " + inputDirs);

      // WALPlayer needs ExtendedCellSerialization so that sequenceId can be propagated when
      // sorting cells in CellSortReducer
      job.getConfiguration().setBoolean(HFileOutputFormat2.EXTENDED_CELL_SERIALIZATION_ENABLED_KEY,
        true);

      // the bulk HFile case
      List<TableName> tableNames = getTableNameList(tables);

      job.setMapperClass(WALPlayer.WALKeyValueMapper.class);
      job.setReducerClass(CellSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(MapReduceExtendedCell.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);) {
        List<HFileOutputFormat2.TableInfo> tableInfoList =
          new ArrayList<HFileOutputFormat2.TableInfo>();
        for (TableName tableName : tableNames) {
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName);
          tableInfoList.add(new HFileOutputFormat2.TableInfo(table.getDescriptor(), regionLocator));
        }
        MultiTableHFileOutputFormat.configureIncrementalLoad(job, tableInfoList);
      }
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
        org.apache.hbase.thirdparty.com.google.common.base.Preconditions.class);
    } else {
      job.setMapperClass(mapperClass);
      job.setOutputFormatClass(outputFormatClass);
      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.initCredentials(job);
      // No reducers.
      job.setNumReduceTasks(0);
    }
    String codecCls = WALCellCodec.getWALCellCodecClass(conf).getName();
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

  public int run(String[] args) throws Exception {
    return 0;
  }
}
