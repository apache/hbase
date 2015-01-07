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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.zookeeper.KeeperException;


/**
 * Import data written by {@link Export}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Import {
  private static final Log LOG = LogFactory.getLog(Import.class);
  final static String NAME = "import";
  public final static String CF_RENAME_PROP = "HBASE_IMPORTER_RENAME_CFS";
  public final static String BULK_OUTPUT_CONF_KEY = "import.bulk.output";
  public final static String FILTER_CLASS_CONF_KEY = "import.filter.class";
  public final static String FILTER_ARGS_CONF_KEY = "import.filter.args";
  public final static String TABLE_NAME = "import.table.name";
  public final static String WAL_DURABILITY = "import.wal.durability";

  /**
   * A mapper that just writes out KeyValues.
   */
  public static class KeyValueImporter extends TableMapper<ImmutableBytesWritable, KeyValue> {
    private Map<byte[], byte[]> cfRenameMap;
    private Filter filter;
    private static final Log LOG = LogFactory.getLog(KeyValueImporter.class);

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
        if (filter == null || !filter.filterRowKey(row.get(), row.getOffset(), row.getLength())) {
          for (Cell kv : value.rawCells()) {
            kv = filterKv(filter, kv);
            // skip if we filtered it out
            if (kv == null) continue;
            // TODO get rid of ensureKeyValue
            context.write(row, KeyValueUtil.ensureKeyValue(convertKv(kv, cfRenameMap)));
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
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
        e.printStackTrace();
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
      if (filter == null || !filter.filterRowKey(key.get(), key.getOffset(), key.getLength())) {
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
        if (CellUtil.isDeleteFamily(kv)) {
          Delete deleteFamily = new Delete(key.get());
          deleteFamily.addDeleteMarker(kv);
          if (durability != null) {
            deleteFamily.setDurability(durability);
          }
          deleteFamily.setClusterIds(clusterIds);
          context.write(key, deleteFamily);
        } else if (CellUtil.isDelete(kv)) {
          if (delete == null) {
            delete = new Delete(key.get());
          }
          delete.addDeleteMarker(kv);
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
      Configuration conf = context.getConfiguration();
      cfRenameMap = createCfRenameMap(conf);
      filter = instantiateFilter(conf);
      String durabilityStr = conf.get(WAL_DURABILITY);
      if(durabilityStr != null){
        durability = Durability.valueOf(durabilityStr.toUpperCase());
      }
      // TODO: This is kind of ugly doing setup of ZKW just to read the clusterid.
      ZooKeeperWatcher zkw = null;
      Exception ex = null;
      try {
        zkw = new ZooKeeperWatcher(conf, context.getTaskAttemptID().toString(), null);
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
    ArrayList<byte[]> quotedArgs = new ArrayList<byte[]>();
    for (String stringArg : stringArgs) {
      // all the filters' instantiation methods expected quoted args since they are coming from
      // the shell, so add them here, though it shouldn't really be needed :-/
      quotedArgs.add(Bytes.toBytes("'" + stringArg + "'"));
    }
    return quotedArgs;
  }

  /**
   * Attempt to filter out the keyvalue
   * @param kv {@link KeyValue} on which to apply the filter
   * @return <tt>null</tt> if the key should not be written, otherwise returns the original
   *         {@link KeyValue}
   */
  public static Cell filterKv(Filter filter, Cell kv) throws IOException {
    // apply the filter and skip this kv if the filter doesn't apply
    if (filter != null) {
      Filter.ReturnCode code = filter.filterKeyValue(kv);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Filter returned:" + code + " for the key value:" + kv);
      }
      // if its not an accept type, then skip this kv
      if (!(code.equals(Filter.ReturnCode.INCLUDE) || code
          .equals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL))) {
        return null;
      }
    }
    return kv;
  }

  // helper: create a new KeyValue based on CF rename map
  private static Cell convertKv(Cell kv, Map<byte[], byte[]> cfRenameMap) {
    if(cfRenameMap != null) {
      // If there's a rename mapping for this CF, create a new KeyValue
      byte[] newCfName = cfRenameMap.get(CellUtil.cloneFamily(kv));
      if(newCfName != null) {
          kv = new KeyValue(kv.getRowArray(), // row buffer 
                  kv.getRowOffset(),        // row offset
                  kv.getRowLength(),        // row length
                  newCfName,                // CF buffer
                  0,                        // CF offset 
                  newCfName.length,         // CF length 
                  kv.getQualifierArray(),   // qualifier buffer
                  kv.getQualifierOffset(),  // qualifier offset
                  kv.getQualifierLength(),  // qualifier length
                  kv.getTimestamp(),        // timestamp
                  KeyValue.Type.codeToType(kv.getTypeByte()), // KV Type
                  kv.getValueArray(),       // value buffer 
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
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(Importer.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);

    // make sure we get the filter in the jars
    try {
      Class<? extends Filter> filter = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
      if (filter != null) {
        TableMapReduceUtil.addDependencyJars(conf, filter);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (hfileOutPath != null) {
      job.setMapperClass(KeyValueImporter.class);
      try (Connection conn = ConnectionFactory.createConnection(conf); 
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)){
        job.setReducerClass(KeyValueSortReducer.class);
        Path outputDir = new Path(hfileOutPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), regionLocator);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
            com.google.common.base.Preconditions.class);
      }
    } else {
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
    System.err
        .println(" To apply a generic org.apache.hadoop.hbase.filter.Filter to the input, use");
    System.err.println("  -D" + FILTER_CLASS_CONF_KEY + "=<name of filter class>");
    System.err.println("  -D" + FILTER_ARGS_CONF_KEY + "=<comma separated list of args for filter");
    System.err.println(" NOTE: The filter will be applied BEFORE doing key renames via the "
        + CF_RENAME_PROP + " property. Futher, filters will only use the"
        + " Filter#filterRowKey(byte[] buffer, int offset, int length) method to identify "
        + " whether the current row needs to be ignored completely for processing and "
        + " Filter#filterKeyValue(KeyValue) method to determine if the KeyValue should be added;"
        + " Filter.ReturnCode#INCLUDE and #INCLUDE_AND_NEXT_COL will be considered as including"
        + " the KeyValue.");
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
    HBaseAdmin hAdmin = null;
    String durability = conf.get(WAL_DURABILITY);
    // Need to flush if the data is written to hbase and skip wal is enabled.
    if (conf.get(BULK_OUTPUT_CONF_KEY) == null && durability != null
        && Durability.SKIP_WAL.name().equalsIgnoreCase(durability)) {
      try {
        hAdmin = new HBaseAdmin(conf);
        hAdmin.flush(tableName);
      } finally {
        if (hAdmin != null) {
          hAdmin.close();
        }
      }
    }
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
    String inputVersionString = System.getProperty(ResultSerialization.IMPORT_FORMAT_VER);
    if (inputVersionString != null) {
      conf.set(ResultSerialization.IMPORT_FORMAT_VER, inputVersionString);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    boolean isJobSuccessful = job.waitForCompletion(true);
    if(isJobSuccessful){
      // Flush all the regions of the table
      flushRegionsIfNecessary(conf);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
