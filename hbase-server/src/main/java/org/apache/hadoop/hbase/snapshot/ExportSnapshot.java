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

package org.apache.hadoop.hbase.snapshot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Export the specified snapshot to a given FileSystem.
 *
 * The .snapshot/name folder is copied to the destination cluster
 * and then all the hfiles/hlogs are copied using a Map-Reduce Job in the .archive/ location.
 * When everything is done, the second cluster can restore the snapshot.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ExportSnapshot extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ExportSnapshot.class);

  private static final String CONF_FILES_USER = "snapshot.export.files.attributes.user";
  private static final String CONF_FILES_GROUP = "snapshot.export.files.attributes.group";
  private static final String CONF_FILES_MODE = "snapshot.export.files.attributes.mode";
  private static final String CONF_CHECKSUM_VERIFY = "snapshot.export.checksum.verify";
  private static final String CONF_OUTPUT_ROOT = "snapshot.export.output.root";
  private static final String CONF_INPUT_ROOT = "snapshot.export.input.root";
  private static final String CONF_BUFFER_SIZE = "snapshot.export.buffer.size";
  private static final String CONF_MAP_GROUP = "snapshot.export.default.map.group";

  static final String CONF_TEST_FAILURE = "test.snapshot.export.failure";
  static final String CONF_TEST_RETRY = "test.snapshot.export.failure.retry";

  private static final String INPUT_FOLDER_PREFIX = "export-files.";

  // Export Map-Reduce Counters, to keep track of the progress
  public enum Counter { MISSING_FILES, COPY_FAILED, BYTES_EXPECTED, BYTES_COPIED, FILES_COPIED };

  private static class ExportMapper extends Mapper<Text, NullWritable, NullWritable, NullWritable> {
    final static int REPORT_SIZE = 1 * 1024 * 1024;
    final static int BUFFER_SIZE = 64 * 1024;

    private boolean testFailures;
    private Random random;

    private boolean verifyChecksum;
    private String filesGroup;
    private String filesUser;
    private short filesMode;
    private int bufferSize;

    private FileSystem outputFs;
    private Path outputArchive;
    private Path outputRoot;

    private FileSystem inputFs;
    private Path inputArchive;
    private Path inputRoot;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      verifyChecksum = conf.getBoolean(CONF_CHECKSUM_VERIFY, true);

      filesGroup = conf.get(CONF_FILES_GROUP);
      filesUser = conf.get(CONF_FILES_USER);
      filesMode = (short)conf.getInt(CONF_FILES_MODE, 0);
      outputRoot = new Path(conf.get(CONF_OUTPUT_ROOT));
      inputRoot = new Path(conf.get(CONF_INPUT_ROOT));

      inputArchive = new Path(inputRoot, HConstants.HFILE_ARCHIVE_DIRECTORY);
      outputArchive = new Path(outputRoot, HConstants.HFILE_ARCHIVE_DIRECTORY);

      testFailures = conf.getBoolean(CONF_TEST_FAILURE, false);

      try {
        inputFs = FileSystem.get(inputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new IOException("Could not get the input FileSystem with root=" + inputRoot, e);
      }

      try {
        outputFs = FileSystem.get(outputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new IOException("Could not get the output FileSystem with root="+ outputRoot, e);
      }

      // Use the default block size of the outputFs if bigger
      int defaultBlockSize = Math.max((int) outputFs.getDefaultBlockSize(), BUFFER_SIZE);
      bufferSize = conf.getInt(CONF_BUFFER_SIZE, defaultBlockSize);
      LOG.info("Using bufferSize=" + StringUtils.humanReadableInt(bufferSize));
    }

    @Override
    public void map(Text key, NullWritable value, Context context)
        throws InterruptedException, IOException {
      Path inputPath = new Path(key.toString());
      Path outputPath = getOutputPath(inputPath);

      LOG.info("copy file input=" + inputPath + " output=" + outputPath);
      copyFile(context, inputPath, outputPath);
    }

    /**
     * Returns the location where the inputPath will be copied.
     *  - hfiles are encoded as hfile links hfile-region-table
     *  - logs are encoded as serverName/logName
     */
    private Path getOutputPath(final Path inputPath) throws IOException {
      Path path;
      if (HFileLink.isHFileLink(inputPath) || StoreFileInfo.isReference(inputPath)) {
        String family = inputPath.getParent().getName();
        TableName table =
            HFileLink.getReferencedTableName(inputPath.getName());
        String region = HFileLink.getReferencedRegionName(inputPath.getName());
        String hfile = HFileLink.getReferencedHFileName(inputPath.getName());
        path = new Path(FSUtils.getTableDir(new Path("./"), table),
            new Path(region, new Path(family, hfile)));
      } else if (isHLogLinkPath(inputPath)) {
        String logName = inputPath.getName();
        path = new Path(new Path(outputRoot, HConstants.HREGION_OLDLOGDIR_NAME), logName);
      } else {
        path = inputPath;
      }
      return new Path(outputArchive, path);
    }

    /*
     * Used by TestExportSnapshot to simulate a failure
     */
    private void injectTestFailure(final Context context, final Path inputPath)
        throws IOException {
      if (testFailures) {
        if (context.getConfiguration().getBoolean(CONF_TEST_RETRY, false)) {
          if (random == null) {
            random = new Random();
          }

          // FLAKY-TEST-WARN: lower is better, we can get some runs without the
          // retry, but at least we reduce the number of test failures due to
          // this test exception from the same map task.
          if (random.nextFloat() < 0.03) {
            throw new IOException("TEST RETRY FAILURE: Unable to copy input=" + inputPath
                                  + " time=" + System.currentTimeMillis());
          }
        } else {
          context.getCounter(Counter.COPY_FAILED).increment(1);
          throw new IOException("TEST FAILURE: Unable to copy input=" + inputPath);
        }
      }
    }

    private void copyFile(final Context context, final Path inputPath, final Path outputPath)
        throws IOException {
      injectTestFailure(context, inputPath);

      // Get the file information
      FileStatus inputStat = getSourceFileStatus(context, inputPath);

      // Verify if the output file exists and is the same that we want to copy
      if (outputFs.exists(outputPath)) {
        FileStatus outputStat = outputFs.getFileStatus(outputPath);
        if (outputStat != null && sameFile(inputStat, outputStat)) {
          LOG.info("Skip copy " + inputPath + " to " + outputPath + ", same file.");
          return;
        }
      }

      FSDataInputStream in = openSourceFile(context, inputPath);
      try {
        context.getCounter(Counter.BYTES_EXPECTED).increment(inputStat.getLen());

        // Ensure that the output folder is there and copy the file
        outputFs.mkdirs(outputPath.getParent());
        FSDataOutputStream out = outputFs.create(outputPath, true);
        try {
          copyData(context, inputPath, in, outputPath, out, inputStat.getLen());
        } finally {
          out.close();
        }

        // Try to Preserve attributes
        if (!preserveAttributes(outputPath, inputStat)) {
          LOG.warn("You may have to run manually chown on: " + outputPath);
        }
      } finally {
        in.close();
      }
    }

    /**
     * Try to Preserve the files attribute selected by the user copying them from the source file
     * This is only required when you are exporting as a different user than "hbase" or on a system
     * that doesn't have the "hbase" user.
     *
     * This is not considered a blocking failure since the user can force a chmod with the user
     * that knows is available on the system.
     */
    private boolean preserveAttributes(final Path path, final FileStatus refStat) {
      FileStatus stat;
      try {
        stat = outputFs.getFileStatus(path);
      } catch (IOException e) {
        LOG.warn("Unable to get the status for file=" + path);
        return false;
      }

      try {
        if (filesMode > 0 && stat.getPermission().toShort() != filesMode) {
          outputFs.setPermission(path, new FsPermission(filesMode));
        } else if (!stat.getPermission().equals(refStat.getPermission())) {
          outputFs.setPermission(path, refStat.getPermission());
        }
      } catch (IOException e) {
        LOG.warn("Unable to set the permission for file="+ stat.getPath() +": "+ e.getMessage());
        return false;
      }

      String user = stringIsNotEmpty(filesUser) ? filesUser : refStat.getOwner();
      String group = stringIsNotEmpty(filesGroup) ? filesGroup : refStat.getGroup();
      if (stringIsNotEmpty(user) || stringIsNotEmpty(group)) {
        try {
          if (!(user.equals(stat.getOwner()) && group.equals(stat.getGroup()))) {
            outputFs.setOwner(path, user, group);
          }
        } catch (IOException e) {
          LOG.warn("Unable to set the owner/group for file="+ stat.getPath() +": "+ e.getMessage());
          LOG.warn("The user/group may not exist on the destination cluster: user=" +
                   user + " group=" + group);
          return false;
        }
      }

      return true;
    }

    private boolean stringIsNotEmpty(final String str) {
      return str != null && str.length() > 0;
    }

    private void copyData(final Context context,
        final Path inputPath, final FSDataInputStream in,
        final Path outputPath, final FSDataOutputStream out,
        final long inputFileSize)
        throws IOException {
      final String statusMessage = "copied %s/" + StringUtils.humanReadableInt(inputFileSize) +
                                   " (%.1f%%)";

      try {
        byte[] buffer = new byte[bufferSize];
        long totalBytesWritten = 0;
        int reportBytes = 0;
        int bytesRead;

        long stime = System.currentTimeMillis();
        while ((bytesRead = in.read(buffer)) > 0) {
          out.write(buffer, 0, bytesRead);
          totalBytesWritten += bytesRead;
          reportBytes += bytesRead;

          if (reportBytes >= REPORT_SIZE) {
            context.getCounter(Counter.BYTES_COPIED).increment(reportBytes);
            context.setStatus(String.format(statusMessage,
                              StringUtils.humanReadableInt(totalBytesWritten),
                              (totalBytesWritten/(float)inputFileSize) * 100.0f) +
                              " from " + inputPath + " to " + outputPath);
            reportBytes = 0;
          }
        }
        long etime = System.currentTimeMillis();

        context.getCounter(Counter.BYTES_COPIED).increment(reportBytes);
        context.setStatus(String.format(statusMessage,
                          StringUtils.humanReadableInt(totalBytesWritten),
                          (totalBytesWritten/(float)inputFileSize) * 100.0f) +
                          " from " + inputPath + " to " + outputPath);

        // Verify that the written size match
        if (totalBytesWritten != inputFileSize) {
          String msg = "number of bytes copied not matching copied=" + totalBytesWritten +
                       " expected=" + inputFileSize + " for file=" + inputPath;
          throw new IOException(msg);
        }

        LOG.info("copy completed for input=" + inputPath + " output=" + outputPath);
        LOG.info("size=" + totalBytesWritten +
            " (" + StringUtils.humanReadableInt(totalBytesWritten) + ")" +
            " time=" + StringUtils.formatTimeDiff(etime, stime) +
            String.format(" %.3fM/sec", (totalBytesWritten / ((etime - stime)/1000.0))/1048576.0));
        context.getCounter(Counter.FILES_COPIED).increment(1);
      } catch (IOException e) {
        LOG.error("Error copying " + inputPath + " to " + outputPath, e);
        context.getCounter(Counter.COPY_FAILED).increment(1);
        throw e;
      }
    }

    /**
     * Try to open the "source" file.
     * Throws an IOException if the communication with the inputFs fail or
     * if the file is not found.
     */
    private FSDataInputStream openSourceFile(Context context, final Path path) throws IOException {
      try {
        if (HFileLink.isHFileLink(path) || StoreFileInfo.isReference(path)) {
          return new HFileLink(inputRoot, inputArchive, path).open(inputFs);
        } else if (isHLogLinkPath(path)) {
          String serverName = path.getParent().getName();
          String logName = path.getName();
          return new HLogLink(inputRoot, serverName, logName).open(inputFs);
        }
        return inputFs.open(path);
      } catch (IOException e) {
        context.getCounter(Counter.MISSING_FILES).increment(1);
        LOG.error("Unable to open source file=" + path, e);
        throw e;
      }
    }

    private FileStatus getSourceFileStatus(Context context, final Path path) throws IOException {
      try {
        if (HFileLink.isHFileLink(path) || StoreFileInfo.isReference(path)) {
          HFileLink link = new HFileLink(inputRoot, inputArchive, path);
          return link.getFileStatus(inputFs);
        } else if (isHLogLinkPath(path)) {
          String serverName = path.getParent().getName();
          String logName = path.getName();
          return new HLogLink(inputRoot, serverName, logName).getFileStatus(inputFs);
        }
        return inputFs.getFileStatus(path);
      } catch (FileNotFoundException e) {
        context.getCounter(Counter.MISSING_FILES).increment(1);
        LOG.error("Unable to get the status for source file=" + path, e);
        throw e;
      } catch (IOException e) {
        LOG.error("Unable to get the status for source file=" + path, e);
        throw e;
      }
    }

    private FileChecksum getFileChecksum(final FileSystem fs, final Path path) {
      try {
        return fs.getFileChecksum(path);
      } catch (IOException e) {
        LOG.warn("Unable to get checksum for file=" + path, e);
        return null;
      }
    }

    /**
     * Check if the two files are equal by looking at the file length,
     * and at the checksum (if user has specified the verifyChecksum flag).
     */
    private boolean sameFile(final FileStatus inputStat, final FileStatus outputStat) {
      // Not matching length
      if (inputStat.getLen() != outputStat.getLen()) return false;

      // Mark files as equals, since user asked for no checksum verification
      if (!verifyChecksum) return true;

      // If checksums are not available, files are not the same.
      FileChecksum inChecksum = getFileChecksum(inputFs, inputStat.getPath());
      if (inChecksum == null) return false;

      FileChecksum outChecksum = getFileChecksum(outputFs, outputStat.getPath());
      if (outChecksum == null) return false;

      return inChecksum.equals(outChecksum);
    }

    /**
     * HLog files are encoded as serverName/logName
     * and since all the other files should be in /hbase/table/..path..
     * we can rely on the depth, for now.
     */
    private static boolean isHLogLinkPath(final Path path) {
      return path.depth() == 2;
    }
  }

  /**
   * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
   * @return list of files referenced by the snapshot (pair of path and size)
   */
  private List<Pair<Path, Long>> getSnapshotFiles(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    final List<Pair<Path, Long>> files = new ArrayList<Pair<Path, Long>>();
    final TableName table =
        TableName.valueOf(snapshotDesc.getTable());
    final Configuration conf = getConf();

    // Get snapshot files
    SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
      new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          Path path = HFileLink.createPath(table, region, family, hfile);
          long size = new HFileLink(conf, path).getFileStatus(fs).getLen();
          files.add(new Pair<Path, Long>(path, size));
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          // copied with the snapshot referenecs
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          long size = new HLogLink(conf, server, logfile).getFileStatus(fs).getLen();
          files.add(new Pair<Path, Long>(new Path(server, logfile), size));
        }
    });

    return files;
  }

  /**
   * Given a list of file paths and sizes, create around ngroups in as balanced a way as possible.
   * The groups created will have similar amounts of bytes.
   * <p>
   * The algorithm used is pretty straightforward; the file list is sorted by size,
   * and then each group fetch the bigger file available, iterating through groups
   * alternating the direction.
   */
  static List<List<Path>> getBalancedSplits(final List<Pair<Path, Long>> files, int ngroups) {
    // Sort files by size, from small to big
    Collections.sort(files, new Comparator<Pair<Path, Long>>() {
      public int compare(Pair<Path, Long> a, Pair<Path, Long> b) {
        long r = a.getSecond() - b.getSecond();
        return (r < 0) ? -1 : ((r > 0) ? 1 : 0);
      }
    });

    // create balanced groups
    List<List<Path>> fileGroups = new LinkedList<List<Path>>();
    long[] sizeGroups = new long[ngroups];
    int hi = files.size() - 1;
    int lo = 0;

    List<Path> group;
    int dir = 1;
    int g = 0;

    while (hi >= lo) {
      if (g == fileGroups.size()) {
        group = new LinkedList<Path>();
        fileGroups.add(group);
      } else {
        group = fileGroups.get(g);
      }

      Pair<Path, Long> fileInfo = files.get(hi--);

      // add the hi one
      sizeGroups[g] += fileInfo.getSecond();
      group.add(fileInfo.getFirst());

      // change direction when at the end or the beginning
      g += dir;
      if (g == ngroups) {
        dir = -1;
        g = ngroups - 1;
      } else if (g < 0) {
        dir = 1;
        g = 0;
      }
    }

    if (LOG.isDebugEnabled()) {
      for (int i = 0; i < sizeGroups.length; ++i) {
        LOG.debug("export split=" + i + " size=" + StringUtils.humanReadableInt(sizeGroups[i]));
      }
    }

    return fileGroups;
  }

  private static Path getInputFolderPath(Configuration conf)
      throws IOException, InterruptedException {
    Path stagingDir = JobUtil.getStagingDir(conf);
    return new Path(stagingDir, INPUT_FOLDER_PREFIX +
      String.valueOf(EnvironmentEdgeManager.currentTimeMillis()));
  }

  /**
   * Create the input files, with the path to copy, for the MR job.
   * Each input files contains n files, and each input file has a similar amount data to copy.
   * The number of input files created are based on the number of mappers provided as argument
   * and the number of the files to copy.
   */
  private static Path[] createInputFiles(final Configuration conf, final Path inputFolderPath,
      final List<Pair<Path, Long>> snapshotFiles, int mappers)
      throws IOException, InterruptedException {
    FileSystem fs = inputFolderPath.getFileSystem(conf);
    LOG.debug("Input folder location: " + inputFolderPath);

    List<List<Path>> splits = getBalancedSplits(snapshotFiles, mappers);
    Path[] inputFiles = new Path[splits.size()];

    Text key = new Text();
    for (int i = 0; i < inputFiles.length; i++) {
      List<Path> files = splits.get(i);
      inputFiles[i] = new Path(inputFolderPath, String.format("export-%d.seq", i));
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, inputFiles[i],
        Text.class, NullWritable.class);
      LOG.debug("Input split: " + i);
      try {
        for (Path file: files) {
          LOG.debug(file.toString());
          key.set(file.toString());
          writer.append(key, NullWritable.get());
        }
      } finally {
        writer.close();
      }
    }

    return inputFiles;
  }

  /**
   * Run Map-Reduce Job to perform the files copy.
   */
  private void runCopyJob(final FileSystem inputFs, final Path inputRoot,
      final FileSystem outputFs, final Path outputRoot,
      final List<Pair<Path, Long>> snapshotFiles, final boolean verifyChecksum,
      final String filesUser, final String filesGroup, final int filesMode,
      final int mappers) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    if (filesGroup != null) conf.set(CONF_FILES_GROUP, filesGroup);
    if (filesUser != null) conf.set(CONF_FILES_USER, filesUser);
    conf.setInt(CONF_FILES_MODE, filesMode);
    conf.setBoolean(CONF_CHECKSUM_VERIFY, verifyChecksum);
    conf.set(CONF_OUTPUT_ROOT, outputRoot.toString());
    conf.set(CONF_INPUT_ROOT, inputRoot.toString());
    conf.setInt("mapreduce.job.maps", mappers);

    Job job = new Job(conf);
    job.setJobName("ExportSnapshot");
    job.setJarByClass(ExportSnapshot.class);
    TableMapReduceUtil.addDependencyJars(job);
    job.setMapperClass(ExportMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapSpeculativeExecution(false);
    job.setNumReduceTasks(0);

    // Create MR Input
    Path inputFolderPath = getInputFolderPath(conf);
    for (Path path: createInputFiles(conf, inputFolderPath, snapshotFiles, mappers)) {
      LOG.debug("Add Input Path=" + path);
      SequenceFileInputFormat.addInputPath(job, path);
    }

    UserProvider userProvider = UserProvider.instantiate(job.getConfiguration());
    FsDelegationToken inputFsToken = new FsDelegationToken(userProvider, "irenewer");
    FsDelegationToken outputFsToken = new FsDelegationToken(userProvider, "orenewer");
    try {
      // Acquire the delegation Tokens
      inputFsToken.acquireDelegationToken(inputFs);
      outputFsToken.acquireDelegationToken(outputFs);

      // Run the MR Job
      if (!job.waitForCompletion(true)) {
        // TODO: Replace the fixed string with job.getStatus().getFailureInfo()
        // when it will be available on all the supported versions.
        throw new ExportSnapshotException("Copy Files Map-Reduce Job failed");
      }
    } finally {
      inputFsToken.releaseDelegationToken();
      outputFsToken.releaseDelegationToken();

      // Remove MR Input
      try {
        inputFolderPath.getFileSystem(conf).delete(inputFolderPath, true);
      } catch (IOException e) {
        LOG.warn("Unable to remove MR input folder: " + inputFolderPath, e);
      }
    }
  }

  private void verifySnapshot(final Configuration baseConf,
      final FileSystem fs, final Path rootDir, final Path snapshotDir) throws IOException {
    // Update the conf with the current root dir, since may be a different cluster
    Configuration conf = new Configuration(baseConf);
    FSUtils.setRootDir(conf, rootDir);
    FSUtils.setFsDefault(conf, snapshotDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotReferenceUtil.verifySnapshot(conf, fs, snapshotDir, snapshotDesc);
  }

  /**
   * Execute the export snapshot by copying the snapshot metadata, hfiles and hlogs.
   * @return 0 on success, and != 0 upon failure.
   */
  @Override
  public int run(String[] args) throws IOException {
    boolean verifyChecksum = true;
    String snapshotName = null;
    boolean overwrite = false;
    String filesGroup = null;
    String filesUser = null;
    Path outputRoot = null;
    int filesMode = 0;
    int mappers = 0;

    Configuration conf = getConf();

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-snapshot")) {
          snapshotName = args[++i];
        } else if (cmd.equals("-copy-to")) {
          outputRoot = new Path(args[++i]);
        } else if (cmd.equals("-copy-from")) {
          Path sourceDir = new Path(args[++i]);
          URI defaultFs = sourceDir.getFileSystem(conf).getUri();
          FSUtils.setFsDefault(conf, new Path(defaultFs));
          FSUtils.setRootDir(conf, sourceDir);
        } else if (cmd.equals("-no-checksum-verify")) {
          verifyChecksum = false;
        } else if (cmd.equals("-mappers")) {
          mappers = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-chuser")) {
          filesUser = args[++i];
        } else if (cmd.equals("-chgroup")) {
          filesGroup = args[++i];
        } else if (cmd.equals("-chmod")) {
          filesMode = Integer.parseInt(args[++i], 8);
        } else if (cmd.equals("-overwrite")) {
          overwrite = true;
        } else if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit();
      }
    }

    // Check user options
    if (snapshotName == null) {
      System.err.println("Snapshot name not provided.");
      printUsageAndExit();
    }

    if (outputRoot == null) {
      System.err.println("Destination file-system not provided.");
      printUsageAndExit();
    }

    Path inputRoot = FSUtils.getRootDir(conf);
    FileSystem inputFs = FileSystem.get(inputRoot.toUri(), conf);
    LOG.debug("inputFs=" + inputFs.getUri().toString() + " inputRoot=" + inputRoot);
    FileSystem outputFs = FileSystem.get(outputRoot.toUri(), conf);
    LOG.debug("outputFs=" + outputFs.getUri().toString() + " outputRoot=" + outputRoot.toString());

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, inputRoot);
    Path snapshotTmpDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotName, outputRoot);
    Path outputSnapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, outputRoot);

    // Check if the snapshot already exists
    if (outputFs.exists(outputSnapshotDir)) {
      if (overwrite) {
        if (!outputFs.delete(outputSnapshotDir, true)) {
          System.err.println("Unable to remove existing snapshot directory: " + outputSnapshotDir);
          return 1;
        }
      } else {
        System.err.println("The snapshot '" + snapshotName +
          "' already exists in the destination: " + outputSnapshotDir);
        return 1;
      }
    }

    // Check if the snapshot already in-progress
    if (outputFs.exists(snapshotTmpDir)) {
      if (overwrite) {
        if (!outputFs.delete(snapshotTmpDir, true)) {
          System.err.println("Unable to remove existing snapshot tmp directory: " + snapshotTmpDir);
          return 1;
        }
      } else {
        System.err.println("A snapshot with the same name '"+ snapshotName +"' may be in-progress");
        System.err.println("Please check " + snapshotTmpDir + ". If the snapshot has completed, ");
        System.err.println("consider removing "+ snapshotTmpDir +" by using the -overwrite option");
        return 1;
      }
    }

    // Step 0 - Extract snapshot files to copy
    LOG.info("Loading Snapshot hfile list");
    final List<Pair<Path, Long>> files = getSnapshotFiles(inputFs, snapshotDir);
    if (mappers == 0 && files.size() > 0) {
      mappers = 1 + (files.size() / conf.getInt(CONF_MAP_GROUP, 10));
      mappers = Math.min(mappers, files.size());
    }

    // Step 1 - Copy fs1:/.snapshot/<snapshot> to  fs2:/.snapshot/.tmp/<snapshot>
    // The snapshot references must be copied before the hfiles otherwise the cleaner
    // will remove them because they are unreferenced.
    try {
      LOG.info("Copy Snapshot Manifest");
      FileUtil.copy(inputFs, snapshotDir, outputFs, snapshotTmpDir, false, false, conf);
    } catch (IOException e) {
      throw new ExportSnapshotException("Failed to copy the snapshot directory: from=" +
        snapshotDir + " to=" + snapshotTmpDir, e);
    }

    // Step 2 - Start MR Job to copy files
    // The snapshot references must be copied before the files otherwise the files gets removed
    // by the HFileArchiver, since they have no references.
    try {
      if (files.size() == 0) {
        LOG.warn("There are 0 store file to be copied. There may be no data in the table.");
      } else {
        runCopyJob(inputFs, inputRoot, outputFs, outputRoot, files, verifyChecksum,
                   filesUser, filesGroup, filesMode, mappers);
      }

      // Step 3 - Rename fs2:/.snapshot/.tmp/<snapshot> fs2:/.snapshot/<snapshot>
      LOG.info("Finalize the Snapshot Export");
      if (!outputFs.rename(snapshotTmpDir, outputSnapshotDir)) {
        throw new ExportSnapshotException("Unable to rename snapshot directory from=" +
          snapshotTmpDir + " to=" + outputSnapshotDir);
      }

      // Step 4 - Verify snapshot validity
      LOG.info("Verify snapshot validity");
      verifySnapshot(conf, outputFs, outputRoot, outputSnapshotDir);

      LOG.info("Export Completed: " + snapshotName);
      return 0;
    } catch (Exception e) {
      LOG.error("Snapshot export failed", e);
      outputFs.delete(snapshotTmpDir, true);
      outputFs.delete(outputSnapshotDir, true);
      return 1;
    }
  }

  // ExportSnapshot
  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]%n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -snapshot NAME          Snapshot to restore.");
    System.err.println("  -copy-to NAME           Remote destination hdfs://");
    System.err.println("  -copy-from NAME         Input folder hdfs:// (default hbase.rootdir)");
    System.err.println("  -no-checksum-verify     Do not verify checksum.");
    System.err.println("  -overwrite              Rewrite the snapshot manifest if already exists");
    System.err.println("  -chuser USERNAME        Change the owner of the files to the specified one.");
    System.err.println("  -chgroup GROUP          Change the group of the files to the specified one.");
    System.err.println("  -chmod MODE             Change the permission of the files to the specified one.");
    System.err.println("  -mappers                Number of mappers to use during the copy (mapreduce.job.maps).");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase " + getClass().getName() + " \\");
    System.err.println("    -snapshot MySnapshot -copy-to hdfs://srv2:8082/hbase \\");
    System.err.println("    -chuser MyUser -chgroup MyGroup -chmod 700 -mappers 16");
    System.err.println();
    System.err.println("  hbase " + getClass().getName() + " \\");
    System.err.println("    -snapshot MySnapshot -copy-from hdfs://srv2:8082/hbase \\");
    System.err.println("    -copy-to hdfs://srv1:50070/hbase \\");
    System.exit(1);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final Configuration conf, final String [] args) throws Exception {
    return ToolRunner.run(conf, new ExportSnapshot(), args);
  }

  public static void main(String[] args) throws Exception {
    System.exit(innerMain(HBaseConfiguration.create(), args));
  }
}
