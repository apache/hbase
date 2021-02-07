/**
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Import data written by {@link Export}.
 */
@InterfaceAudience.Public
public class Import extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(Import.class);
  final static String NAME = "import";
  public final static String CF_RENAME_PROP = "HBASE_IMPORTER_RENAME_CFS";
  public final static String BULK_OUTPUT_CONF_KEY = "import.bulk.output";
  public final static String FILTER_CLASS_CONF_KEY = "import.filter.class";
  public final static String FILTER_ARGS_CONF_KEY = "import.filter.args";
  public final static String TABLE_NAME = "import.table.name";
  public final static String WAL_DURABILITY = "import.wal.durability";
  public final static String HAS_LARGE_RESULT= "import.bulk.hasLargeResult";

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public static class CellWritableComparablePartitioner
      extends Partitioner<CellWritableComparable, Cell> {
    private static CellWritableComparable[] START_KEYS = null;
    @Override
    public int getPartition(CellWritableComparable key, Cell value,
        int numPartitions) {
      for (int i = 0; i < START_KEYS.length; ++i) {
        if (key.compareTo(START_KEYS[i]) <= 0) {
          return i;
        }
      }
      return START_KEYS.length;
    }

  }

  public static class CellWritableComparable
      implements WritableComparable<CellWritableComparable> {

    private Cell kv = null;

    static {
      // register this comparator
      WritableComparator.define(CellWritableComparable.class,
          new CellWritableComparator());
    }

    public CellWritableComparable() {
    }

    public CellWritableComparable(Cell kv) {
      this.kv = kv;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(PrivateCellUtil.estimatedSerializedSizeOfKey(kv));
      out.writeInt(0);
      PrivateCellUtil.writeFlatKey(kv, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      kv = KeyValue.create(in);
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS",
        justification = "This is wrong, yes, but we should be purging Writables, not fixing them")
    public int compareTo(CellWritableComparable o) {
      return CellComparator.getInstance().compare(this.kv, o.kv);
    }

    public static class CellWritableComparator extends WritableComparator {

      @Override
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
          CellWritableComparable kv1 = new CellWritableComparable();
          kv1.readFields(new DataInputStream(new ByteArrayInputStream(b1, s1, l1)));
          CellWritableComparable kv2 = new CellWritableComparable();
          kv2.readFields(new DataInputStream(new ByteArrayInputStream(b2, s2, l2)));
          return compare(kv1, kv2);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    }

  }

  public static class CellReducer
      extends
      Reducer<CellWritableComparable, Cell, ImmutableBytesWritable, Cell> {
    protected void reduce(
        CellWritableComparable row,
        Iterable<Cell> kvs,
        Reducer<CellWritableComparable,
          Cell, ImmutableBytesWritable, Cell>.Context context)
        throws java.io.IOException, InterruptedException {
      int index = 0;
      for (Cell kv : kvs) {
        context.write(new ImmutableBytesWritable(CellUtil.cloneRow(kv)),
          new MapReduceExtendedCell(kv));
        if (++index % 100 == 0)
          context.setStatus("Wrote " + index + " KeyValues, "
              + "and the rowkey whose is being wrote is " + Bytes.toString(kv.getRowArray()));
      }
    }
  }

  public static class CellSortImporter
      extends TableMapper<CellWritableComparable, Cell> {
    private Map<byte[], byte[]> cfRenameMap;
    private Filter filter;
    private static final Logger LOG = LoggerFactory.getLogger(CellImporter.class);

    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Considering the row."
              + Bytes.toString(row.get(), row.getOffset(), row.getLength()));
        }
        if (filter == null || !filter.filterRowKey(
          PrivateCellUtil.createFirstOnRow(row.get(), row.getOffset(), (short) row.getLength()))) {
          for (Cell kv : value.rawCells()) {
            kv = filterKv(filter, kv);
            // skip if we filtered it out
            if (kv == null) continue;
            Cell ret = convertKv(kv, cfRenameMap);
            context.write(new CellWritableComparable(ret), ret);
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while emitting Cell", e);
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      cfRenameMap = createCfRenameMap(context.getConfiguration());
      filter = instantiateFilter(context.getConfiguration());
      int reduceNum = context.getNumReduceTasks();
      Configuration conf = context.getConfiguration();
      TableName tableName = TableName.valueOf(context.getConfiguration().get(TABLE_NAME));
      try (Connection conn = ConnectionFactory.createConnection(conf);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
        byte[][] startKeys = regionLocator.getStartKeys();
        if (startKeys.length != reduceNum) {
          throw new IOException("Region split after job initialization");
        }
        CellWritableComparable[] startKeyWraps =
            new CellWritableComparable[startKeys.length - 1];
        for (int i = 1; i < startKeys.length; ++i) {
          startKeyWraps[i - 1] =
              new CellWritableComparable(KeyValueUtil.createFirstOnRow(startKeys[i]));
        }
        CellWritableComparablePartitioner.START_KEYS = startKeyWraps;
      }
    }
  }

  /**
   * A mapper that just writes out KeyValues.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EQ_COMPARETO_USE_OBJECT_EQUALS",
      justification="Writables are going away and this has been this way forever")
  public static class CellImporter extends TableMapper<ImmutableBytesWritable, Cell> {
    private Map<byte[], byte[]> cfRenameMap;
    private Filter filter;
    private static final Logger LOG = LoggerFactory.getLogger(CellImporter.class);

    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Considering the row."
              + Bytes.toString(row.get(), row.getOffset(), row.getLength()));
        }
        if (filter == null
            || !filter.filterRowKey(PrivateCellUtil.createFirstOnRow(row.get(), row.getOffset(),
                (short) row.getLength()))) {
          for (Cell kv : value.rawCells()) {
            kv = filterKv(filter, kv);
            // skip if we filtered it out
            if (kv == null) continue;
            context.write(row, new MapReduceExtendedCell(convertKv(kv, cfRenameMap)));
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while emitting Cell", e);
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void setup(Context context) {
      cfRenameMap = createCfRenameMap(context.getConfiguration());
      filter = instantiateFilter(context.getConfiguration());
    }
  }

  /**
   * Write table content out to files in hdfs.
   */
  public static class Importer extends TableMapper<ImmutableBytesWritable, Mutation> {
    private Map<byte[], byte[]> cfRenameMap;
    private List<UUID> clusterIds;
    private Filter filter;
    private Durability durability;

    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        writeResult(row, value, context);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while writing result", e);
        Thread.currentThread().interrupt();
      }
    }

    private void writeResult(ImmutableBytesWritable key, Result result, Context context)
    throws IOException, InterruptedException {
      Put put = null;
      Delete delete = null;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Considering the row."
            + Bytes.toString(key.get(), key.getOffset(), key.getLength()));
      }
      if (filter == null
          || !filter.filterRowKey(PrivateCellUtil.createFirstOnRow(key.get(), key.getOffset(),
              (short) key.getLength()))) {
        processKV(key, result, context, put, delete);
      }
    }

    protected void processKV(ImmutableBytesWritable key, Result result, Context context, Put put,
        Delete delete) throws IOException, InterruptedException {
      for (Cell kv : result.rawCells()) {
        kv = filterKv(filter, kv);
        // skip if we filter it out
        if (kv == null) continue;

        kv = convertKv(kv, cfRenameMap);
        // Deletes and Puts are gathered and written when finished
        /*
         * If there are sequence of mutations and tombstones in an Export, and after Import the same
         * sequence should be restored as it is. If we combine all Delete tombstones into single
         * request then there is chance of ignoring few DeleteFamily tombstones, because if we
         * submit multiple DeleteFamily tombstones in single Delete request then we are maintaining
         * only newest in hbase table and ignoring other. Check - HBASE-12065
         */
        if (PrivateCellUtil.isDeleteFamily(kv)) {
          Delete deleteFamily = new Delete(key.get());
          deleteFamily.add(kv);
          if (durability != null) {
            deleteFamily.setDurability(durability);
          }
          deleteFamily.setClusterIds(clusterIds);
          context.write(key, deleteFamily);
        } else if (CellUtil.isDelete(kv)) {
          if (delete == null) {
            delete = new Delete(key.get());
          }
          delete.add(kv);
        } else {
          if (put == null) {
            put = new Put(key.get());
          }
          addPutToKv(put, kv);
        }
      }
      if (put != null) {
        if (durability != null) {
          put.setDurability(durability);
        }
        put.setClusterIds(clusterIds);
        context.write(key, put);
      }
      if (delete != null) {
        if (durability != null) {
          delete.setDurability(durability);
        }
        delete.setClusterIds(clusterIds);
        context.write(key, delete);
      }
    }

    protected void addPutToKv(Put put, Cell kv) throws IOException {
      put.add(kv);
    }

    @Override
    public void setup(Context context) {
      LOG.info("Setting up " + getClass() + " mapper.");
      Configuration conf = context.getConfiguration();
      cfRenameMap = createCfRenameMap(conf);
      filter = instantiateFilter(conf);
      String durabilityStr = conf.get(WAL_DURABILITY);
      if(durabilityStr != null){
        durability = Durability.valueOf(durabilityStr.toUpperCase(Locale.ROOT));
        LOG.info("setting WAL durability to " + durability);
      } else {
        LOG.info("setting WAL durability to default.");
      }
      // TODO: This is kind of ugly doing setup of ZKW just to read the clusterid.
      ZKWatcher zkw = null;
      Exception ex = null;
      try {
        zkw = new ZKWatcher(conf, context.getTaskAttemptID().toString(), null);
        clusterIds = Collections.singletonList(ZKClusterId.getUUIDForCluster(zkw));
      } catch (ZooKeeperConnectionException e) {
        ex = e;
        LOG.error("Problem connecting to ZooKeper during task setup", e);
      } catch (KeeperException e) {
        ex = e;
        LOG.error("Problem reading ZooKeeper data during task setup", e);
      } catch (IOException e) {
        ex = e;
        LOG.error("Problem setting up task", e);
      } finally {
        if (zkw != null) zkw.close();
      }
      if (clusterIds == null) {
        // exit early if setup fails
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Create a {@link Filter} to apply to all incoming keys ({@link KeyValue KeyValues}) to
   * optionally not include in the job output
   * @param conf {@link Configuration} from which to load the filter
   * @return the filter to use for the task, or <tt>null</tt> if no filter to should be used
   * @throws IllegalArgumentException if the filter is misconfigured
   */
  public static Filter instantiateFilter(Configuration conf) {
    // get the filter, if it was configured
    Class<? extends Filter> filterClass = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
    if (filterClass == null) {
      LOG.debug("No configured filter class, accepting all keyvalues.");
      return null;
    }
    LOG.debug("Attempting to create filter:" + filterClass);
    String[] filterArgs = conf.getStrings(FILTER_ARGS_CONF_KEY);
    ArrayList<byte[]> quotedArgs = toQuotedByteArrays(filterArgs);
    try {
      Method m = filterClass.getMethod("createFilterFromArguments", ArrayList.class);
      return (Filter) m.invoke(null, quotedArgs);
    } catch (IllegalAccessException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    }
  }

  private static ArrayList<byte[]> toQuotedByteArrays(String... stringArgs) {
    ArrayList<byte[]> quotedArgs = new ArrayList<>();
    for (String stringArg : stringArgs) {
      // all the filters' instantiation methods expected quoted args since they are coming from
      // the shell, so add them here, though it shouldn't really be needed :-/
      quotedArgs.add(Bytes.toBytes("'" + stringArg + "'"));
    }
    return quotedArgs;
  }

  /**
   * Attempt to filter out the keyvalue
   * @param c {@link Cell} on which to apply the filter
   * @return <tt>null</tt> if the key should not be written, otherwise returns the original
   *         {@link Cell}
   */
  public static Cell filterKv(Filter filter, Cell c) throws IOException {
    // apply the filter and skip this kv if the filter doesn't apply
    if (filter != null) {
      Filter.ReturnCode code = filter.filterCell(c);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Filter returned:" + code + " for the cell:" + c);
      }
      // if its not an accept type, then skip this kv
      if (!(code.equals(Filter.ReturnCode.INCLUDE) || code
          .equals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL))) {
        return null;
      }
    }
    return c;
  }

  // helper: create a new KeyValue based on CF rename map
  private static Cell convertKv(Cell kv, Map<byte[], byte[]> cfRenameMap) {
    if(cfRenameMap != null) {
      // If there's a rename mapping for this CF, create a new KeyValue
      byte[] newCfName = cfRenameMap.get(CellUtil.cloneFamily(kv));
      if (newCfName != null) {
        kv = new KeyValue(kv.getRowArray(), // row buffer
            kv.getRowOffset(),              // row offset
            kv.getRowLength(),              // row length
            newCfName,                      // CF buffer
            0,                              // CF offset
            newCfName.length,               // CF length
            kv.getQualifierArray(),         // qualifier buffer
            kv.getQualifierOffset(),        // qualifier offset
            kv.getQualifierLength(),        // qualifier length
            kv.getTimestamp(),              // timestamp
            KeyValue.Type.codeToType(kv.getTypeByte()), // KV Type
            kv.getValueArray(),             // value buffer
            kv.getValueOffset(),            // value offset
            kv.getValueLength());           // value length
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
            cfRenameMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        }
        String [] srcAndDest = mapping.split(":");
        if(srcAndDest.length != 2) {
            continue;
        }
        cfRenameMap.put(Bytes.toBytes(srcAndDest[0]), Bytes.toBytes(srcAndDest[1]));
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
   * Add a Filter to be instantiated on import
   * @param conf Configuration to update (will be passed to the job)
   * @param clazz {@link Filter} subclass to instantiate on the server.
   * @param filterArgs List of arguments to pass to the filter on instantiation
   */
  public static void addFilterAndArguments(Configuration conf, Class<? extends Filter> clazz,
      List<String> filterArgs) throws IOException {
    conf.set(Import.FILTER_CLASS_CONF_KEY, clazz.getName());
    conf.setStrings(Import.FILTER_ARGS_CONF_KEY, filterArgs.toArray(new String[filterArgs.size()]));
  }

  /**
   * Sets up the actual job.
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    TableName tableName = TableName.valueOf(args[0]);
    conf.set(TABLE_NAME, tableName.getNameAsString());
    Path inputDir = new Path(args[1]);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(Importer.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);

    // make sure we get the filter in the jars
    try {
      Class<? extends Filter> filter = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
      if (filter != null) {
        TableMapReduceUtil.addDependencyJarsForClasses(conf, filter);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (hfileOutPath != null && conf.getBoolean(HAS_LARGE_RESULT, false)) {
      LOG.info("Use Large Result!!");
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
        HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
        job.setMapperClass(CellSortImporter.class);
        job.setReducerClass(CellReducer.class);
        Path outputDir = new Path(hfileOutPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(CellWritableComparable.class);
        job.setMapOutputValueClass(MapReduceExtendedCell.class);
        job.getConfiguration().setClass("mapreduce.job.output.key.comparator.class",
            CellWritableComparable.CellWritableComparator.class,
            RawComparator.class);
        Path partitionsPath =
            new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration()));
        FileSystem fs = FileSystem.get(job.getConfiguration());
        fs.deleteOnExit(partitionsPath);
        job.setPartitionerClass(CellWritableComparablePartitioner.class);
        job.setNumReduceTasks(regionLocator.getStartKeys().length);
        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
            org.apache.hbase.thirdparty.com.google.common.base.Preconditions.class);
      }
    } else if (hfileOutPath != null) {
      LOG.info("writing to hfiles for bulk load.");
      job.setMapperClass(CellImporter.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)){
        job.setReducerClass(CellSortReducer.class);
        Path outputDir = new Path(hfileOutPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(MapReduceExtendedCell.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
            org.apache.hbase.thirdparty.com.google.common.base.Preconditions.class);
      }
    } else {
      LOG.info("writing directly to table from Mapper.");
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // because it sets up the TableOutputFormat.
      job.setMapperClass(Importer.class);
      TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null, job);
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
    System.err.println("If there is a large result that includes too much Cell "
        + "whitch can occur OOME caused by the memery sort in reducer, pass the option:");
    System.err.println("  -D" + HAS_LARGE_RESULT + "=true");
    System.err
        .println(" To apply a generic org.apache.hadoop.hbase.filter.Filter to the input, use");
    System.err.println("  -D" + FILTER_CLASS_CONF_KEY + "=<name of filter class>");
    System.err.println("  -D" + FILTER_ARGS_CONF_KEY + "=<comma separated list of args for filter");
    System.err.println(" NOTE: The filter will be applied BEFORE doing key renames via the "
        + CF_RENAME_PROP + " property. Futher, filters will only use the"
        + " Filter#filterRowKey(byte[] buffer, int offset, int length) method to identify "
        + " whether the current row needs to be ignored completely for processing and "
        + " Filter#filterCell(Cell) method to determine if the Cell should be added;"
        + " Filter.ReturnCode#INCLUDE and #INCLUDE_AND_NEXT_COL will be considered as including"
        + " the Cell.");
    System.err.println("To import data exported from HBase 0.94, use");
    System.err.println("  -Dhbase.import.version=0.94");
    System.err.println("  -D " + JOB_NAME_CONF_KEY
        + "=jobName - use the specified mapreduce job name for the import");
    System.err.println("For performance consider the following options:\n"
        + "  -Dmapreduce.map.speculative=false\n"
        + "  -Dmapreduce.reduce.speculative=false\n"
        + "  -D" + WAL_DURABILITY + "=<Used while writing data to hbase."
            +" Allowed values are the supported durability values"
            +" like SKIP_WAL/ASYNC_WAL/SYNC_WAL/...>");
  }

  /**
   * If the durability is set to {@link Durability#SKIP_WAL} and the data is imported to hbase, we
   * need to flush all the regions of the table as the data is held in memory and is also not
   * present in the Write Ahead Log to replay in scenarios of a crash. This method flushes all the
   * regions of the table in the scenarios of import data to hbase with {@link Durability#SKIP_WAL}
   */
  public static void flushRegionsIfNecessary(Configuration conf) throws IOException,
      InterruptedException {
    String tableName = conf.get(TABLE_NAME);
    Admin hAdmin = null;
    Connection connection = null;
    String durability = conf.get(WAL_DURABILITY);
    // Need to flush if the data is written to hbase and skip wal is enabled.
    if (conf.get(BULK_OUTPUT_CONF_KEY) == null && durability != null
        && Durability.SKIP_WAL.name().equalsIgnoreCase(durability)) {
      LOG.info("Flushing all data that skipped the WAL.");
      try {
        connection = ConnectionFactory.createConnection(conf);
        hAdmin = connection.getAdmin();
        hAdmin.flush(TableName.valueOf(tableName));
      } finally {
        if (hAdmin != null) {
          hAdmin.close();
        }
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage("Wrong number of arguments: " + args.length);
      return -1;
    }
    String inputVersionString = System.getProperty(ResultSerialization.IMPORT_FORMAT_VER);
    if (inputVersionString != null) {
      getConf().set(ResultSerialization.IMPORT_FORMAT_VER, inputVersionString);
    }
    Job job = createSubmittableJob(getConf(), args);
    boolean isJobSuccessful = job.waitForCompletion(true);
    if(isJobSuccessful){
      // Flush all the regions of the table
      flushRegionsIfNecessary(getConf());
    }
    long inputRecords = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    long outputRecords = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
    if (outputRecords < inputRecords) {
      System.err.println("Warning, not all records were imported (maybe filtered out).");
      if (outputRecords == 0) {
        System.err.println("If the data was exported from HBase 0.94 "+
            "consider using -Dhbase.import.version=0.94.");
      }
    }

    return (isJobSuccessful ? 0 : 1);
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new Import(), args);
    System.exit(errCode);
  }

}
