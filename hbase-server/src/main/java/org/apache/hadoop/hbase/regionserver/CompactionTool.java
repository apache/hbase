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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;

/*
 * The CompactionTool allows to execute a compaction specifying a:
 * <ul>
 *  <li>table folder (all regions and families will be compacted)
 *  <li>region folder (all families in the region will be compacted)
 *  <li>family folder (the store files will be compacted)
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class CompactionTool extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CompactionTool.class);

  private final static String CONF_TMP_DIR = "hbase.tmp.dir";
  private final static String CONF_COMPACT_ONCE = "hbase.compactiontool.compact.once";
  private final static String CONF_COMPACT_MAJOR = "hbase.compactiontool.compact.major";
  private final static String CONF_DELETE_COMPACTED = "hbase.compactiontool.delete";
  private final static String CONF_COMPLETE_COMPACTION = "hbase.hstore.compaction.complete";

  /**
   * Class responsible to execute the Compaction on the specified path.
   * The path can be a table, region or family directory.
   */
  private static class CompactionWorker {
    private final boolean keepCompactedFiles;
    private final boolean deleteCompacted;
    private final Configuration conf;
    private final FileSystem fs;
    private final Path tmpDir;

    public CompactionWorker(final FileSystem fs, final Configuration conf) {
      this.conf = conf;
      this.keepCompactedFiles = !conf.getBoolean(CONF_COMPLETE_COMPACTION, true);
      this.deleteCompacted = conf.getBoolean(CONF_DELETE_COMPACTED, false);
      this.tmpDir = new Path(conf.get(CONF_TMP_DIR));
      this.fs = fs;
    }

    /**
     * Execute the compaction on the specified path.
     *
     * @param path Directory path on which to run compaction.
     * @param compactOnce Execute just a single step of compaction.
     * @param major Request major compaction.
     */
    public void compact(final Path path, final boolean compactOnce, final boolean major) throws IOException {
      if (isFamilyDir(fs, path)) {
        Path regionDir = path.getParent();
        Path tableDir = regionDir.getParent();
        HTableDescriptor htd = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
        HRegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
        compactStoreFiles(tableDir, htd, hri,
            path.getName(), compactOnce, major);
      } else if (isRegionDir(fs, path)) {
        Path tableDir = path.getParent();
        HTableDescriptor htd = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
        compactRegion(tableDir, htd, path, compactOnce, major);
      } else if (isTableDir(fs, path)) {
        compactTable(path, compactOnce, major);
      } else {
        throw new IOException(
          "Specified path is not a table, region or family directory. path=" + path);
      }
    }

    private void compactTable(final Path tableDir, final boolean compactOnce, final boolean major)
        throws IOException {
      HTableDescriptor htd = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
      for (Path regionDir: FSUtils.getRegionDirs(fs, tableDir)) {
        compactRegion(tableDir, htd, regionDir, compactOnce, major);
      }
    }

    private void compactRegion(final Path tableDir, final HTableDescriptor htd,
        final Path regionDir, final boolean compactOnce, final boolean major)
        throws IOException {
      HRegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
      for (Path familyDir: FSUtils.getFamilyDirs(fs, regionDir)) {
        compactStoreFiles(tableDir, htd, hri, familyDir.getName(), compactOnce, major);
      }
    }

    /**
     * Execute the actual compaction job.
     * If the compact once flag is not specified, execute the compaction until
     * no more compactions are needed. Uses the Configuration settings provided.
     */
    private void compactStoreFiles(final Path tableDir, final HTableDescriptor htd,
        final HRegionInfo hri, final String familyName, final boolean compactOnce,
        final boolean major) throws IOException {
      HStore store = getStore(conf, fs, tableDir, htd, hri, familyName, tmpDir);
      LOG.info("Compact table=" + htd.getTableName() +
        " region=" + hri.getRegionNameAsString() +
        " family=" + familyName);
      if (major) {
        store.triggerMajorCompaction();
      }
      do {
        CompactionContext compaction = store.requestCompaction(Store.PRIORITY_USER, null);
        if (compaction == null) break;
        List<StoreFile> storeFiles =
            store.compact(compaction, NoLimitThroughputController.INSTANCE);
        if (storeFiles != null && !storeFiles.isEmpty()) {
          if (keepCompactedFiles && deleteCompacted) {
            for (StoreFile storeFile: storeFiles) {
              fs.delete(storeFile.getPath(), false);
            }
          }
        }
      } while (store.needsCompaction() && !compactOnce);
    }

    /**
     * Create a "mock" HStore that uses the tmpDir specified by the user and
     * the store dir to compact as source.
     */
    private static HStore getStore(final Configuration conf, final FileSystem fs,
        final Path tableDir, final HTableDescriptor htd, final HRegionInfo hri,
        final String familyName, final Path tempDir) throws IOException {
      HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, hri) {
        @Override
        public Path getTempDir() {
          return tempDir;
        }
      };
      HRegion region = new HRegion(regionFs, null, conf, htd, null);
      return new HStore(region, htd.getFamily(Bytes.toBytes(familyName)), conf);
    }
  }

  private static boolean isRegionDir(final FileSystem fs, final Path path) throws IOException {
    Path regionInfo = new Path(path, HRegionFileSystem.REGION_INFO_FILE);
    return fs.exists(regionInfo);
  }

  private static boolean isTableDir(final FileSystem fs, final Path path) throws IOException {
    return FSTableDescriptors.getTableInfoPath(fs, path) != null;
  }

  private static boolean isFamilyDir(final FileSystem fs, final Path path) throws IOException {
    return isRegionDir(fs, path.getParent());
  }

  private static class CompactionMapper
      extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private CompactionWorker compactor = null;
    private boolean compactOnce = false;
    private boolean major = false;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      compactOnce = conf.getBoolean(CONF_COMPACT_ONCE, false);
      major = conf.getBoolean(CONF_COMPACT_MAJOR, false);

      try {
        FileSystem fs = FileSystem.get(conf);
        this.compactor = new CompactionWorker(fs, conf);
      } catch (IOException e) {
        throw new RuntimeException("Could not get the input FileSystem", e);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws InterruptedException, IOException {
      Path path = new Path(value.toString());
      this.compactor.compact(path, compactOnce, major);
    }
  }

  /**
   * Input format that uses store files block location as input split locality.
   */
  private static class CompactionInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
      return true;
    }

    /**
     * Returns a split for each store files directory using the block location
     * of each file as locality reference.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      List<FileStatus> files = listStatus(job);

      Text key = new Text();
      for (FileStatus file: files) {
        Path path = file.getPath();
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        LineReader reader = new LineReader(fs.open(path));
        long pos = 0;
        int n;
        try {
          while ((n = reader.readLine(key)) > 0) {
            String[] hosts = getStoreDirHosts(fs, path);
            splits.add(new FileSplit(path, pos, n, hosts));
            pos += n;
          }
        } finally {
          reader.close();
        }
      }

      return splits;
    }

    /**
     * return the top hosts of the store files, used by the Split
     */
    private static String[] getStoreDirHosts(final FileSystem fs, final Path path)
        throws IOException {
      FileStatus[] files = FSUtils.listStatus(fs, path);
      if (files == null) {
        return new String[] {};
      }

      HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
      for (FileStatus hfileStatus: files) {
        HDFSBlocksDistribution storeFileBlocksDistribution =
          FSUtils.computeHDFSBlocksDistribution(fs, hfileStatus, 0, hfileStatus.getLen());
        hdfsBlocksDistribution.add(storeFileBlocksDistribution);
      }

      List<String> hosts = hdfsBlocksDistribution.getTopHosts();
      return hosts.toArray(new String[hosts.size()]);
    }

    /**
     * Create the input file for the given directories to compact.
     * The file is a TextFile with each line corrisponding to a
     * store files directory to compact.
     */
    public static void createInputFile(final FileSystem fs, final Path path,
        final Set<Path> toCompactDirs) throws IOException {
      // Extract the list of store dirs
      List<Path> storeDirs = new LinkedList<Path>();
      for (Path compactDir: toCompactDirs) {
        if (isFamilyDir(fs, compactDir)) {
          storeDirs.add(compactDir);
        } else if (isRegionDir(fs, compactDir)) {
          for (Path familyDir: FSUtils.getFamilyDirs(fs, compactDir)) {
            storeDirs.add(familyDir);
          }
        } else if (isTableDir(fs, compactDir)) {
          // Lookup regions
          for (Path regionDir: FSUtils.getRegionDirs(fs, compactDir)) {
            for (Path familyDir: FSUtils.getFamilyDirs(fs, regionDir)) {
              storeDirs.add(familyDir);
            }
          }
        } else {
          throw new IOException(
            "Specified path is not a table, region or family directory. path=" + compactDir);
        }
      }

      // Write Input File
      FSDataOutputStream stream = fs.create(path);
      LOG.info("Create input file=" + path + " with " + storeDirs.size() + " dirs to compact.");
      try {
        final byte[] newLine = Bytes.toBytes("\n");
        for (Path storeDir: storeDirs) {
          stream.write(Bytes.toBytes(storeDir.toString()));
          stream.write(newLine);
        }
      } finally {
        stream.close();
      }
    }
  }

  /**
   * Execute compaction, using a Map-Reduce job.
   */
  private int doMapReduce(final FileSystem fs, final Set<Path> toCompactDirs,
      final boolean compactOnce, final boolean major) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(CONF_COMPACT_ONCE, compactOnce);
    conf.setBoolean(CONF_COMPACT_MAJOR, major);

    Job job = new Job(conf);
    job.setJobName("CompactionTool");
    job.setJarByClass(CompactionTool.class);
    job.setMapperClass(CompactionMapper.class);
    job.setInputFormatClass(CompactionInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapSpeculativeExecution(false);
    job.setNumReduceTasks(0);

    // add dependencies (including HBase ones)
    TableMapReduceUtil.addDependencyJars(job);

    Path stagingDir = JobUtil.getStagingDir(conf);
    try {
      // Create input file with the store dirs
      Path inputPath = new Path(stagingDir, "compact-"+ EnvironmentEdgeManager.currentTime());
      CompactionInputFormat.createInputFile(fs, inputPath, toCompactDirs);
      CompactionInputFormat.addInputPath(job, inputPath);

      // Initialize credential for secure cluster
      TableMapReduceUtil.initCredentials(job);

      // Start the MR Job and wait
      return job.waitForCompletion(true) ? 0 : 1;
    } finally {
      fs.delete(stagingDir, true);
    }
  }

  /**
   * Execute compaction, from this client, one path at the time.
   */
  private int doClient(final FileSystem fs, final Set<Path> toCompactDirs,
      final boolean compactOnce, final boolean major) throws IOException {
    CompactionWorker worker = new CompactionWorker(fs, getConf());
    for (Path path: toCompactDirs) {
      worker.compact(path, compactOnce, major);
    }
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    Set<Path> toCompactDirs = new HashSet<Path>();
    boolean compactOnce = false;
    boolean major = false;
    boolean mapred = false;

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    try {
      for (int i = 0; i < args.length; ++i) {
        String opt = args[i];
        if (opt.equals("-compactOnce")) {
          compactOnce = true;
        } else if (opt.equals("-major")) {
          major = true;
        } else if (opt.equals("-mapred")) {
          mapred = true;
        } else if (!opt.startsWith("-")) {
          Path path = new Path(opt);
          FileStatus status = fs.getFileStatus(path);
          if (!status.isDirectory()) {
            printUsage("Specified path is not a directory. path=" + path);
            return 1;
          }
          toCompactDirs.add(path);
        } else {
          printUsage();
        }
      }
    } catch (Exception e) {
      printUsage(e.getMessage());
      return 1;
    }

    if (toCompactDirs.size() == 0) {
      printUsage("No directories to compact specified.");
      return 1;
    }

    // Execute compaction!
    if (mapred) {
      return doMapReduce(fs, toCompactDirs, compactOnce, major);
    } else {
      return doClient(fs, toCompactDirs, compactOnce, major);
    }
  }

  private void printUsage() {
    printUsage(null);
  }

  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err.println("  [-compactOnce] [-major] [-mapred] [-D<property=value>]* files...");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" mapred         Use MapReduce to run compaction.");
    System.err.println(" compactOnce    Execute just one compaction step. (default: while needed)");
    System.err.println(" major          Trigger major compaction.");
    System.err.println();
    System.err.println("Note: -D properties will be applied to the conf used. ");
    System.err.println("For example: ");
    System.err.println(" To preserve input files, pass -D"+CONF_COMPLETE_COMPACTION+"=false");
    System.err.println(" To stop delete of compacted file, pass -D"+CONF_DELETE_COMPACTED+"=false");
    System.err.println(" To set tmp dir, pass -D"+CONF_TMP_DIR+"=ALTERNATE_DIR");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To compact the full 'TestTable' using MapReduce:");
    System.err.println(" $ hbase " + this.getClass().getName() + " -mapred hdfs:///hbase/data/default/TestTable");
    System.err.println();
    System.err.println(" To compact column family 'x' of the table 'TestTable' region 'abc':");
    System.err.println(" $ hbase " + this.getClass().getName() + " hdfs:///hbase/data/default/TestTable/abc/x");
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new CompactionTool(), args));
  }
}
