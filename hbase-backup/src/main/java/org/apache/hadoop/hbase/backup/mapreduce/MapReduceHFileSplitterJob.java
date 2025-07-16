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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellSortReducer;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotRegionLocator;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool to split HFiles into new region boundaries as a MapReduce job. The tool generates HFiles
 * for later bulk importing.
 */
@InterfaceAudience.Private
public class MapReduceHFileSplitterJob extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceHFileSplitterJob.class);
  final static String NAME = "HFileSplitterJob";
  public final static String BULK_OUTPUT_CONF_KEY = "hfile.bulk.output";
  public final static String TABLES_KEY = "hfile.input.tables";
  public final static String TABLE_MAP_KEY = "hfile.input.tablesmap";
  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  
  // Configuration keys for pluggable rack-aware processing (must match BackupCommands)
  public final static String CONF_INPUT_FILE_GROUPER_CLASS = "hfile.backup.input.file.grouper.class";
  public final static String CONF_INPUT_FILE_LOCATION_RESOLVER_CLASS = "hfile.backup.input.file.location.resolver.class";

  public MapReduceHFileSplitterJob() {
  }

  protected MapReduceHFileSplitterJob(final Configuration c) {
    super(c);
  }

  /**
   * A mapper that just writes out cells. This one can be used together with {@link CellSortReducer}
   */
  static class HFileCellMapper extends Mapper<NullWritable, Cell, ImmutableBytesWritable, Cell> {

    @Override
    public void map(NullWritable key, Cell value, Context context)
      throws IOException, InterruptedException {
      context.write(new ImmutableBytesWritable(CellUtil.cloneRow(value)),
        new MapReduceExtendedCell(value));
    }

    @Override
    public void setup(Context context) throws IOException {
      // do nothing
    }
  }

  /**
   * Interface for custom file grouping logic to influence InputSplit creation for HFile processing.
   * Similar to ExportSnapshot's CustomFileGrouper but for HFiles.
   */
  @InterfaceAudience.Public
  public interface HFileCustomFileGrouper {
    /**
     * Groups HFiles into collections that should be processed together.
     * Files in different groups are guaranteed not to be in the same InputSplit.
     * 
     * @param hfiles Collection of HFile paths and their sizes
     * @return Collections of HFiles grouped by custom logic (e.g., rack, host, size)
     */
    Collection<Collection<Pair<String, Long>>> getGroupedInputFiles(
      final Collection<Pair<String, Long>> hfiles);
  }

  /**
   * Interface for resolving file locations to influence InputSplit placement for HFile processing.
   * Similar to ExportSnapshot's FileLocationResolver but for HFiles.
   */
  @InterfaceAudience.Public
  public interface HFileLocationResolver {
    /**
     * Get preferred locations for a group of HFiles to optimize rack-aware processing.
     * 
     * @param hfiles Collection of HFile paths and sizes that will be processed together
     * @return Set of preferred host names for processing these HFiles
     */
    Set<String> getLocationsForInputFiles(final Collection<Pair<String, Long>> hfiles);
  }

  /**
   * Default no-op implementation of HFileCustomFileGrouper.
   * Provides backward compatibility by putting all files in a single group.
   */
  public static class NoopHFileCustomFileGrouper implements HFileCustomFileGrouper {
    @Override
    public Collection<Collection<Pair<String, Long>>> getGroupedInputFiles(
        Collection<Pair<String, Long>> hfiles) {
      // Single group containing all files - maintains original behavior
      return Collections.singletonList(new ArrayList<>(hfiles));
    }
  }

  /**
   * Default no-op implementation of HFileLocationResolver.
   * Provides backward compatibility by returning no location hints.
   */
  public static class NoopHFileLocationResolver implements HFileLocationResolver {
    @Override
    public Set<String> getLocationsForInputFiles(Collection<Pair<String, Long>> hfiles) {
      // No location hints - lets YARN scheduler decide
      return Collections.emptySet();
    }
  }

  /**
   * Rack-aware HFile input format that uses pluggable CustomFileGrouper and FileLocationResolver
   * classes for optimal data locality. Follows the same pattern as ExportSnapshot.
   */
  @InterfaceAudience.Private
  public static class RackAwareHFileInputFormat extends HFileInputFormat {
    
    private static final Logger LOG = LoggerFactory.getLogger(RackAwareHFileInputFormat.class);
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
      try {
        return createRackAwareSplits(context);
      } catch (Exception e) {
        LOG.error("Error creating rack-aware splits, falling back to standard splits", e);
        return super.getSplits(context);
      }
    }
    
    private List<InputSplit> createRackAwareSplits(JobContext context) 
        throws IOException {
      Configuration conf = context.getConfiguration();
      
      // 1. Get all HFiles from input paths
      List<FileStatus> files = listStatus(context);
      Collection<Pair<String, Long>> hfiles = new ArrayList<>();
      for (FileStatus file : files) {
        hfiles.add(new Pair<>(file.getPath().toString(), file.getLen()));
      }
      
      // 2. Load pluggable CustomFileGrouper
      Class<? extends HFileCustomFileGrouper> grouperClass = conf.getClass(
        CONF_INPUT_FILE_GROUPER_CLASS, 
        NoopHFileCustomFileGrouper.class, 
        HFileCustomFileGrouper.class);
      HFileCustomFileGrouper customFileGrouper = 
        ReflectionUtils.newInstance(grouperClass, conf);
      
      // 3. Load pluggable FileLocationResolver
      Class<? extends HFileLocationResolver> resolverClass = conf.getClass(
        CONF_INPUT_FILE_LOCATION_RESOLVER_CLASS,
        NoopHFileLocationResolver.class,
        HFileLocationResolver.class);
      HFileLocationResolver fileLocationResolver = 
        ReflectionUtils.newInstance(resolverClass, conf);
      
      // 4. Group files using custom grouper
      Collection<Collection<Pair<String, Long>>> groupedFiles = 
        customFileGrouper.getGroupedInputFiles(hfiles);
      
      // 5. Create InputSplits from grouped files
      List<InputSplit> splits = new ArrayList<>();
      
      for (Collection<Pair<String, Long>> group : groupedFiles) {
        // Get location hints for this group
        Set<String> locations = fileLocationResolver.getLocationsForInputFiles(group);
        String[] locationArray = locations.toArray(new String[0]);
        
        // Create balanced splits from this group
        List<InputSplit> groupSplits = createBalancedSplits(group, locationArray);
        splits.addAll(groupSplits);
      }

      LOG.info("Created {} rack-aware InputSplits from {} HFiles in {} groups", 
        splits.size(), hfiles.size(), groupedFiles.size());
      
      return splits;
    }
    
    private List<InputSplit> createBalancedSplits(Collection<Pair<String, Long>> hfiles, 
        String[] locations) throws IOException {
      List<InputSplit> splits = new ArrayList<>();
      
      // For now, create one split per HFile (can be enhanced for better balancing)
      for (Pair<String, Long> hfile : hfiles) {
        Path path = new Path(hfile.getFirst());
        long length = hfile.getSecond();
        if (length <= 0){
          LOG.warn("Skipping empty or invalid Hfile: {} with length: {}", path, length);
          continue;
        }

        splits.add(new FileSplit(path, 0, length, locations));
      }
      
      return splits;
    }
  }

  /**
   * Sets up the actual job.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();
    String inputDirs = args[0];
    String tabName = args[1];
    conf.setStrings(TABLES_KEY, tabName);
    conf.set(FileInputFormat.INPUT_DIR, inputDirs);
    Job job = Job.getInstance(conf,
      conf.get(JOB_NAME_CONF_KEY, NAME + "_" + EnvironmentEdgeManager.currentTime()));
    // MapReduceHFileSplitter needs ExtendedCellSerialization so that sequenceId can be propagated
    // when sorting cells in CellSortReducer
    job.getConfiguration().setBoolean(HFileOutputFormat2.EXTENDED_CELL_SERIALIZATION_ENABLED_KEY,
      true);
    job.setJarByClass(MapReduceHFileSplitterJob.class);
    
    // BACKWARD COMPATIBLE: Choose InputFormat based on configuration
    if (isRackAwarenessConfigured(conf)) {
      LOG.info("Using RackAwareHFileInputFormat with pluggable classes");
      job.setInputFormatClass(RackAwareHFileInputFormat.class);
    } else {
      LOG.info("Using standard HFileInputFormat");
      job.setInputFormatClass(HFileInputFormat.class);
    }
    
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      LOG.debug("add incremental job :" + hfileOutPath + " from " + inputDirs);
      TableName tableName = TableName.valueOf(tabName);
      job.setMapperClass(HFileCellMapper.class);
      job.setReducerClass(CellSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputValueClass(MapReduceExtendedCell.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(tableName);
        RegionLocator regionLocator = getRegionLocator(conf, conn, tableName)) {
        HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
      }
      LOG.debug("success configuring load incremental job");

      TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hbase.thirdparty.com.google.common.base.Preconditions.class);
    } else {
      throw new IOException("No bulk output directory specified");
    }
    return job;
  }

  /**
   * Check if rack awareness is configured
   * Both grouper and location resolver should be configured for effective rack awareness
   */
  private boolean isRackAwarenessConfigured(Configuration conf) {
    return conf.get(CONF_INPUT_FILE_GROUPER_CLASS) != null &&
           conf.get(CONF_INPUT_FILE_LOCATION_RESOLVER_CLASS) != null;
  }

  /**
   * Print usage
   * @param errorMsg Error message. Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <HFile inputdir(s)> <table>");
    System.err.println("Read all HFile's for <table> and split them to <table> region boundaries.");
    System.err.println("<table>  table to load.\n");
    System.err.println("To generate HFiles for a bulk data load, pass the option:");
    System.err.println("  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output");
    System.err.println("Other options:");
    System.err.println("   -D " + JOB_NAME_CONF_KEY
      + "=jobName - use the specified mapreduce job name for the HFile splitter");
    
    // NEW: Pluggable class options
    System.err.println("Rack-aware processing options (both required for full rack awareness):");
    System.err.println("  -D" + CONF_INPUT_FILE_GROUPER_CLASS + "=<class> - " + 
      "HFile custom grouper class for rack-aware processing");
    System.err.println("  -D" + CONF_INPUT_FILE_LOCATION_RESOLVER_CLASS + "=<class> - " + 
      "HFile location resolver class for rack-aware processing");
    System.err.println("  Note: Both grouper and location resolver must be configured together");
    
    System.err.println("For performance also consider the following options:\n"
      + "  -Dmapreduce.map.speculative=false\n" + "  -Dmapreduce.reduce.speculative=false");
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new MapReduceHFileSplitterJob(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage("Wrong number of arguments: " + args.length);
      return -1;
    }
    Job job = createSubmittableJob(args);
    int result = job.waitForCompletion(true) ? 0 : 1;
    return result;
  }

  private static RegionLocator getRegionLocator(Configuration conf, Connection conn,
    TableName table) throws IOException {
    if (SnapshotRegionLocator.shouldUseSnapshotRegionLocator(conf, table)) {
      return SnapshotRegionLocator.create(conf, table);
    }

    return conn.getRegionLocator(table);
  }
}
