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
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Create 3 level tree directory, first level is using table name as parent directory and then use
 * family name as child directory, and all related HFiles for one family are under child directory
 * -tableName1
 *   -columnFamilyName1
 *     -HFile (region1)
 *   -columnFamilyName2
 *     -HFile1 (region1)
 *     -HFile2 (region2)
 *     -HFile3 (region3)
 * -tableName2
 *   -columnFamilyName1
 *     -HFile (region1)
 * family directory and its hfiles match the output of HFileOutputFormat2
 * @see org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
 */

@InterfaceAudience.Public
@VisibleForTesting
public class MultiTableHFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Cell> {
  private static final Log LOG = LogFactory.getLog(MultiTableHFileOutputFormat.class);

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

    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = conn.getAdmin();

    // Map of existing tables, avoid calling getTable() everytime
    final Map<ImmutableBytesWritable, Table> tables = new HashMap<>();

    // Map of tables to writers
    final Map<ImmutableBytesWritable, RecordWriter<ImmutableBytesWritable, V>> tableWriters = new HashMap<>();

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
          // Configure for tableWriter, if table exist, write configuration of table into conf
          Table table = null;
          if (tables.containsKey(tableName)) {
            table = tables.get(tableName);
          } else {
            table = getTable(tableName.copyBytes(), conn, admin);
            tables.put(tableName, table);
          }
          if (table != null) {
            configureForOneTable(conf, table.getTableDescriptor());
          }
          // Create writer for one specific table
          tableWriter = new HFileOutputFormat2.HFileRecordWriter<>(context, tableOutputDir);
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
        if (conn != null) {
          conn.close();
        }
        if (admin != null) {
          admin.close();
        }
      }
    };
  }

  /**
   * Configure for one table, should be used before creating a new HFileRecordWriter,
   * Set compression algorithms and related configuration based on column families
   */
  private static void configureForOneTable(Configuration conf, final HTableDescriptor tableDescriptor)
      throws UnsupportedEncodingException {
    HFileOutputFormat2.configureCompression(conf, tableDescriptor);
    HFileOutputFormat2.configureBlockSize(tableDescriptor, conf);
    HFileOutputFormat2.configureBloomType(tableDescriptor, conf);
    HFileOutputFormat2.configureDataBlockEncoding(tableDescriptor, conf);
  }

  /**
   * Configure a MapReduce Job to output HFiles for performing an incremental load into
   * the multiple tables.
   * <ul>
   *   <li>Inspects the tables to configure a partitioner based on their region boundaries</li>
   *   <li>Writes the partitions file and configures the partitioner</li>
   *   <li>Sets the number of reduce tasks to match the total number of all tables' regions</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (KeyValueSortReducer)</li>
   * </ul>
   *
   * ConfigureIncrementalLoad has set up partitioner and reducer for mapreduce job.
   * Caller needs to setup input path, output path and mapper
   *
   * @param job
   * @param tables A list of tables to inspects
   * @throws IOException
   */
  public static void configureIncrementalLoad(Job job, List<TableName> tables) throws IOException {
    configureIncrementalLoad(job, tables, MultiTableHFileOutputFormat.class);
  }

  public static void configureIncrementalLoad(Job job, List<TableName> tables,
      Class<? extends OutputFormat<?, ?>> cls) throws IOException {

    Configuration conf = job.getConfiguration();
    Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> tableSplitKeys =
        MultiHFilePartitioner.getTablesRegionStartKeys(conf, tables);
    configureIncrementalLoad(job, tableSplitKeys, cls);
  }

  /**
   * Same purpose as configureIncrementalLoad(Job job, List<TableName> tables)
   * Used when region startKeys of each table is available, input as <TableName, List<RegionStartKey>>
   *
   * Caller needs to transfer TableName and byte[] to ImmutableBytesWritable
   */
  public static void configureIncrementalLoad(Job job, Map<ImmutableBytesWritable,
      List<ImmutableBytesWritable>> tableSplitKeys) throws IOException {
    configureIncrementalLoad(job, tableSplitKeys, MultiTableHFileOutputFormat.class);
  }

  public static void configureIncrementalLoad(Job job, Map<ImmutableBytesWritable,
      List<ImmutableBytesWritable>> tableSplitKeys, Class<? extends OutputFormat<?, ?>> cls) throws IOException {
    Configuration conf = job.getConfiguration();

    // file path to store <table, splitKey>
    String hbaseTmpFsDir = conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
        HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    final Path partitionsPath = new Path(hbaseTmpFsDir, "partitions_" + UUID.randomUUID());
    LOG.info("Writing partition info into dir: " + partitionsPath.toString());
    job.setPartitionerClass(MultiHFilePartitioner.class);
    // get split keys for all the tables, and write them into partition file
    MultiHFilePartitioner.writeTableSplitKeys(conf, partitionsPath, tableSplitKeys);
    MultiHFilePartitioner.setPartitionFile(conf, partitionsPath);
    partitionsPath.getFileSystem(conf).makeQualified(partitionsPath);
    partitionsPath.getFileSystem(conf).deleteOnExit(partitionsPath);

    // now only support Mapper output <ImmutableBytesWritable, KeyValue>
    // we can use KeyValueSortReducer directly to sort Mapper output
    if (KeyValue.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(KeyValueSortReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }
    int reducerNum = getReducerNumber(tableSplitKeys);
    job.setNumReduceTasks(reducerNum);
    LOG.info("Configuring " + reducerNum + " reduce partitions " + "to match current region count");

    // setup output format
    job.setOutputFormatClass(cls);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName());
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
  }

  /**
   * Check if table exist, should not dependent on HBase instance
   * @return instance of table, if it exist
   */
  private static Table getTable(final byte[] tableName, Connection conn, Admin admin) {
    if (conn == null || admin == null) {
      LOG.info("can not get Connection or Admin");
      return null;
    }

    try {
      TableName table = TableName.valueOf(tableName);
      if (admin.tableExists(table)) {
        return conn.getTable(table);
      }
    } catch (IOException e) {
      LOG.info("Exception found in getTable()" + e.toString());
      return null;
    }

    LOG.warn("Table: '" + TableName.valueOf(tableName) + "' does not exist");
    return null;
  }

  /**
   * Get the number of reducers by tables' split keys
   */
  private static int getReducerNumber(
      Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> tableSplitKeys) {
    int reducerNum = 0;
    for (Map.Entry<ImmutableBytesWritable, List<ImmutableBytesWritable>> entry : tableSplitKeys.entrySet()) {
      reducerNum += entry.getValue().size();
    }
    return reducerNum;
  }

  /**
   * MultiTableHFileOutputFormat writes files based on partitions created by MultiHFilePartitioner
   * The input is partitioned based on table's name and its region boundaries with the table.
   * Two records are in the same partition if they have same table name and the their cells are
   * in the same region
   */
  static class MultiHFilePartitioner extends Partitioner<ImmutableBytesWritable, Cell>
      implements Configurable {

    public static final String DEFAULT_PATH = "_partition_multihfile.lst";
    public static final String PARTITIONER_PATH = "mapreduce.multihfile.partitioner.path";
    private Configuration conf;
    // map to receive <table, splitKeys> from file
    private Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> table_SplitKeys;
    // each <table,splitKey> pair is map to one unique integer
    private TreeMap<TableSplitKeyPair, Integer> partitionMap;

    @Override
    public void setConf(Configuration conf) {
      try {
        this.conf = conf;
        partitionMap = new TreeMap<>();
        table_SplitKeys = readTableSplitKeys(conf);

        // initiate partitionMap by table_SplitKeys map
        int splitNum = 0;
        for (Map.Entry<ImmutableBytesWritable, List<ImmutableBytesWritable>> entry : table_SplitKeys.entrySet()) {
          ImmutableBytesWritable table = entry.getKey();
          List<ImmutableBytesWritable> list = entry.getValue();
          for (ImmutableBytesWritable splitKey : list) {
            partitionMap.put(new TableSplitKeyPair(table, splitKey), splitNum++);
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't read partitions file", e);
      }
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    /**
     * Set the path to the SequenceFile storing the sorted <table, splitkey>. It must be the case
     * that for <tt>R</tt> reduces, there are <tt>R-1</tt> keys in the SequenceFile.
     */
    public static void setPartitionFile(Configuration conf, Path p) {
      conf.set(PARTITIONER_PATH, p.toString());
    }

    /**
     * Get the path to the SequenceFile storing the sorted <table, splitkey>.
     * @see #setPartitionFile(Configuration, Path)
     */
    public static String getPartitionFile(Configuration conf) {
      return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
    }


    /**
     * Return map of <tableName, the start keys of all of the regions in this table>
     */
    public static Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> getTablesRegionStartKeys(
        Configuration conf, List<TableName> tables) throws IOException {
      final TreeMap<ImmutableBytesWritable, List<ImmutableBytesWritable>> ret = new TreeMap<>();

      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        LOG.info("Looking up current regions for tables");
        for (TableName tName : tables) {
          RegionLocator table = conn.getRegionLocator(tName);
          // if table not exist, use default split keys for this table
          byte[][] byteKeys = { HConstants.EMPTY_BYTE_ARRAY };
          if (admin.tableExists(tName)) {
            byteKeys = table.getStartKeys();
          }
          List<ImmutableBytesWritable> tableStartKeys = new ArrayList<>(byteKeys.length);
          for (byte[] byteKey : byteKeys) {
            tableStartKeys.add(new ImmutableBytesWritable(byteKey));
          }
          ret.put(new ImmutableBytesWritable(tName.toBytes()), tableStartKeys);

        }
        return ret;
      }
    }

    /**
     * write <tableName, start key of each region in table> into sequence file in order,
     * and this format can be parsed by MultiHFilePartitioner
     */
    public static void writeTableSplitKeys(Configuration conf, Path partitionsPath,
        Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> map) throws IOException {
      LOG.info("Writing partition information to " + partitionsPath);

      if (map == null || map.isEmpty()) {
        throw new IllegalArgumentException("No regions passed for all tables");
      }

      SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(partitionsPath),
          Writer.keyClass(ImmutableBytesWritable.class),
          Writer.valueClass(ImmutableBytesWritable.class));

      try {
        for (Map.Entry<ImmutableBytesWritable, List<ImmutableBytesWritable>> entry : map.entrySet()) {
          ImmutableBytesWritable table = entry.getKey();
          List<ImmutableBytesWritable> list = entry.getValue();
          if (list == null) {
            throw new IOException("Split keys for a table can not be null");
          }

          TreeSet<ImmutableBytesWritable> sorted = new TreeSet<>(list);

          ImmutableBytesWritable first = sorted.first();
          if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
            throw new IllegalArgumentException(
                "First region of table should have empty start key. Instead has: "
                    + Bytes.toStringBinary(first.get()));
          }

          for (ImmutableBytesWritable startKey : sorted) {
            writer.append(table, startKey);
          }
        }
      } finally {
        writer.close();
      }
    }

    /**
     * read partition file into map <table, splitKeys of this table>
     */
    private Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> readTableSplitKeys(
        Configuration conf) throws IOException {
      String parts = getPartitionFile(conf);
      LOG.info("Read partition info from file: " + parts);
      final Path partFile = new Path(parts);

      SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(partFile));
      // values are already sorted in file, so use list
      final Map<ImmutableBytesWritable, List<ImmutableBytesWritable>> map =
          new TreeMap<>();
      // key and value have same type
      ImmutableBytesWritable key = ReflectionUtils.newInstance(ImmutableBytesWritable.class, conf);
      ImmutableBytesWritable value =
          ReflectionUtils.newInstance(ImmutableBytesWritable.class, conf);
      try {
        while (reader.next(key, value)) {

          List<ImmutableBytesWritable> list = map.get(key);
          if (list == null) {
            list = new ArrayList<>();
          }
          list.add(value);
          map.put(key, list);

          key = ReflectionUtils.newInstance(ImmutableBytesWritable.class, conf);
          value = ReflectionUtils.newInstance(ImmutableBytesWritable.class, conf);
        }
      } finally {
        IOUtils.cleanup(LOG, reader);
      }
      return map;
    }

    @Override
    public int getPartition(ImmutableBytesWritable table, Cell value, int numPartitions) {
      byte[] row = CellUtil.cloneRow(value);
      final ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row);
      ImmutableBytesWritable splitId = new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);
      //find splitKey by input rowKey
      if (table_SplitKeys.containsKey(table)) {
        List<ImmutableBytesWritable> list = table_SplitKeys.get(table);
        int index = Collections.binarySearch(list, rowKey, new ImmutableBytesWritable.Comparator());
        if (index < 0) {
          index = (index + 1) * (-1) - 1;
        } else if (index == list.size()) {
          index -= 1;
        }
        if (index < 0) {
          index = 0;
          LOG.error("row key can not less than HConstants.EMPTY_BYTE_ARRAY ");
        }
        splitId = list.get(index);
      }

      // find the id of the reducer for the input
      Integer id = partitionMap.get(new TableSplitKeyPair(table, splitId));
      if (id == null) {
          LOG.warn("Can not get reducer id for input record");
          return -1;
      }
      return id.intValue() % numPartitions;
    }

    /**
     * A class store pair<TableName, SplitKey>, has two main usage
     * 1. store tableName and one of its splitKey as a pair
     * 2. implement comparable, so that partitioner can find splitKey of its input cell
     */
    static class TableSplitKeyPair extends Pair<ImmutableBytesWritable, ImmutableBytesWritable>
        implements Comparable<TableSplitKeyPair> {

      private static final long serialVersionUID = -6485999667666325594L;

      public TableSplitKeyPair(ImmutableBytesWritable a, ImmutableBytesWritable b) {
        super(a, b);
      }

      @Override
      public int compareTo(TableSplitKeyPair other) {
        if (this.getFirst().equals(other.getFirst())) {
          return this.getSecond().compareTo(other.getSecond());
        }
        return this.getFirst().compareTo(other.getFirst());
      }
    }
  }
}