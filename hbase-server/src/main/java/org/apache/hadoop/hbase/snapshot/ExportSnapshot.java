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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.io.hadoopbackport.ThrottledInputStream;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
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
public class ExportSnapshot extends Configured implements Tool {
  public static final String NAME = "exportsnapshot";

  private static final Log LOG = LogFactory.getLog(ExportSnapshot.class);

  private static final String MR_NUM_MAPS = "mapreduce.job.maps";
  private static final String CONF_NUM_SPLITS = "snapshot.export.format.splits";
  private static final String CONF_SNAPSHOT_NAME = "snapshot.export.format.snapshot.name";
  private static final String CONF_SNAPSHOT_DIR = "snapshot.export.format.snapshot.dir";
  private static final String CONF_FILES_USER = "snapshot.export.files.attributes.user";
  private static final String CONF_FILES_GROUP = "snapshot.export.files.attributes.group";
  private static final String CONF_FILES_MODE = "snapshot.export.files.attributes.mode";
  private static final String CONF_CHECKSUM_VERIFY = "snapshot.export.checksum.verify";
  private static final String CONF_OUTPUT_ROOT = "snapshot.export.output.root";
  private static final String CONF_INPUT_ROOT = "snapshot.export.input.root";
  private static final String CONF_BANDWIDTH_MB = "snapshot.export.map.bandwidth.mb";
  private static final String CONF_BUFFER_SIZE = "snapshot.export.buffer.size";
  private static final String CONF_MAP_GROUP = "snapshot.export.default.map.group";
  protected static final String CONF_SKIP_TMP = "snapshot.export.skip.tmp";

  static final String CONF_TEST_FAILURE = "test.snapshot.export.failure";
  static final String CONF_TEST_RETRY = "test.snapshot.export.failure.retry";

  private static final String INPUT_FOLDER_PREFIX = "export-files.";

  // Export Map-Reduce Counters, to keep track of the progress
  public enum Counter {
    MISSING_FILES, FILES_COPIED, FILES_SKIPPED, COPY_FAILED,
    BYTES_EXPECTED, BYTES_SKIPPED, BYTES_COPIED
  }

  private static class ExportMapper extends Mapper<BytesWritable, NullWritable,
                                                   NullWritable, NullWritable> {
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
        conf.setBoolean("fs." + inputRoot.toUri().getScheme() + ".impl.disable.cache", true);
        inputFs = FileSystem.get(inputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new IOException("Could not get the input FileSystem with root=" + inputRoot, e);
      }

      try {
        conf.setBoolean("fs." + outputRoot.toUri().getScheme() + ".impl.disable.cache", true);
        outputFs = FileSystem.get(outputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new IOException("Could not get the output FileSystem with root="+ outputRoot, e);
      }

      // Use the default block size of the outputFs if bigger
      int defaultBlockSize = Math.max((int) outputFs.getDefaultBlockSize(), BUFFER_SIZE);
      bufferSize = conf.getInt(CONF_BUFFER_SIZE, defaultBlockSize);
      LOG.info("Using bufferSize=" + StringUtils.humanReadableInt(bufferSize));

      for (Counter c : Counter.values()) {
        context.getCounter(c).increment(0);
      }
    }

    byte[] copyBytes(BytesWritable  bw) {
      byte[] result = new byte[bw.getLength()];
      System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength());
      return result;
    }

    @Override
    protected void cleanup(Context context) {
      IOUtils.closeStream(inputFs);
      IOUtils.closeStream(outputFs);
    }

    @Override
    public void map(BytesWritable key, NullWritable value, Context context)
        throws InterruptedException, IOException {
      SnapshotFileInfo inputInfo = SnapshotFileInfo.parseFrom(copyBytes(key));
      Path outputPath = getOutputPath(inputInfo);

      copyFile(context, inputInfo, outputPath);
    }

    /**
     * Returns the location where the inputPath will be copied.
     */
    private Path getOutputPath(final SnapshotFileInfo inputInfo) throws IOException {
      Path path = null;
      switch (inputInfo.getType()) {
        case HFILE:
          Path inputPath = new Path(inputInfo.getHfile());
          String family = inputPath.getParent().getName();
          TableName table =HFileLink.getReferencedTableName(inputPath.getName());
          String region = HFileLink.getReferencedRegionName(inputPath.getName());
          String hfile = HFileLink.getReferencedHFileName(inputPath.getName());
          path = new Path(FSUtils.getTableDir(new Path("./"), table),
              new Path(region, new Path(family, hfile)));
          break;
        case WAL:
          Path oldLogsDir = new Path(outputRoot, HConstants.HREGION_OLDLOGDIR_NAME);
          path = new Path(oldLogsDir, inputInfo.getWalName());
          break;
        default:
          throw new IOException("Invalid File Type: " + inputInfo.getType().toString());
      }
      return new Path(outputArchive, path);
    }

    /*
     * Used by TestExportSnapshot to simulate a failure
     */
    private void injectTestFailure(final Context context, final SnapshotFileInfo inputInfo)
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
            throw new IOException("TEST RETRY FAILURE: Unable to copy input=" + inputInfo
                                  + " time=" + System.currentTimeMillis());
          }
        } else {
          context.getCounter(Counter.COPY_FAILED).increment(1);
          throw new IOException("TEST FAILURE: Unable to copy input=" + inputInfo);
        }
      }
    }

    private void copyFile(final Context context, final SnapshotFileInfo inputInfo,
        final Path outputPath) throws IOException {
      injectTestFailure(context, inputInfo);

      // Get the file information
      FileStatus inputStat = getSourceFileStatus(context, inputInfo);

      // Verify if the output file exists and is the same that we want to copy
      if (outputFs.exists(outputPath)) {
        FileStatus outputStat = outputFs.getFileStatus(outputPath);
        if (outputStat != null && sameFile(inputStat, outputStat)) {
          LOG.info("Skip copy " + inputStat.getPath() + " to " + outputPath + ", same file.");
          context.getCounter(Counter.FILES_SKIPPED).increment(1);
          context.getCounter(Counter.BYTES_SKIPPED).increment(inputStat.getLen());
          return;
        }
      }

      InputStream in = openSourceFile(context, inputInfo);
      int bandwidthMB = context.getConfiguration().getInt(CONF_BANDWIDTH_MB, 100);
      if (Integer.MAX_VALUE != bandwidthMB) {
        in = new ThrottledInputStream(new BufferedInputStream(in), bandwidthMB * 1024 * 1024);
      }

      try {
        context.getCounter(Counter.BYTES_EXPECTED).increment(inputStat.getLen());

        // Ensure that the output folder is there and copy the file
        createOutputPath(outputPath.getParent());
        FSDataOutputStream out = outputFs.create(outputPath, true);
        try {
          copyData(context, inputStat.getPath(), in, outputPath, out, inputStat.getLen());
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
     * Create the output folder and optionally set ownership.
     */
    private void createOutputPath(final Path path) throws IOException {
      if (filesUser == null && filesGroup == null) {
        outputFs.mkdirs(path);
      } else {
        Path parent = path.getParent();
        if (!outputFs.exists(parent) && !parent.isRoot()) {
          createOutputPath(parent);
        }
        outputFs.mkdirs(path);
        // override the owner when non-null user/group is specified
        outputFs.setOwner(path, filesUser, filesGroup);
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
        } else if (refStat != null && !stat.getPermission().equals(refStat.getPermission())) {
          outputFs.setPermission(path, refStat.getPermission());
        }
      } catch (IOException e) {
        LOG.warn("Unable to set the permission for file="+ stat.getPath() +": "+ e.getMessage());
        return false;
      }

      boolean hasRefStat = (refStat != null);
      String user = stringIsNotEmpty(filesUser) || !hasRefStat ? filesUser : refStat.getOwner();
      String group = stringIsNotEmpty(filesGroup) || !hasRefStat ? filesGroup : refStat.getGroup();
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
        final Path inputPath, final InputStream in,
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
    private FSDataInputStream openSourceFile(Context context, final SnapshotFileInfo fileInfo)
        throws IOException {
      try {
        FileLink link = null;
        switch (fileInfo.getType()) {
          case HFILE:
            Path inputPath = new Path(fileInfo.getHfile());
            link = new HFileLink(inputRoot, inputArchive, inputPath);
            break;
          case WAL:
            String serverName = fileInfo.getWalServer();
            String logName = fileInfo.getWalName();
            link = new HLogLink(inputRoot, serverName, logName);
            break;
          default:
            throw new IOException("Invalid File Type: " + fileInfo.getType().toString());
        }
        return link.open(inputFs);
      } catch (IOException e) {
        context.getCounter(Counter.MISSING_FILES).increment(1);
        LOG.error("Unable to open source file=" + fileInfo.toString(), e);
        throw e;
      }
    }

    private FileStatus getSourceFileStatus(Context context, final SnapshotFileInfo fileInfo)
        throws IOException {
      try {
        FileLink link = null;
        switch (fileInfo.getType()) {
          case HFILE:
            Path inputPath = new Path(fileInfo.getHfile());
            link = new HFileLink(inputRoot, inputArchive, inputPath);
            break;
          case WAL:
            link = new HLogLink(inputRoot, fileInfo.getWalServer(), fileInfo.getWalName());
            break;
          default:
            throw new IOException("Invalid File Type: " + fileInfo.getType().toString());
        }
        return link.getFileStatus(inputFs);
      } catch (FileNotFoundException e) {
        context.getCounter(Counter.MISSING_FILES).increment(1);
        LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
        throw e;
      } catch (IOException e) {
        LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
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
  }

  // ==========================================================================
  //  Input Format
  // ==========================================================================

  /**
   * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
   * @return list of files referenced by the snapshot (pair of path and size)
   */
  private static List<Pair<SnapshotFileInfo, Long>> getSnapshotFiles(final Configuration conf,
      final FileSystem fs, final Path snapshotDir) throws IOException {
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    final List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<Pair<SnapshotFileInfo, Long>>();
    final TableName table = TableName.valueOf(snapshotDesc.getTable());

    // Get snapshot files
    LOG.info("Loading Snapshot '" + snapshotDesc.getName() + "' hfile list");
    SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
      new SnapshotReferenceUtil.SnapshotVisitor() {
        @Override
        public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (storeFile.hasReference()) {
            // copied as part of the manifest
          } else {
            String region = regionInfo.getEncodedName();
            String hfile = storeFile.getName();
            Path path = HFileLink.createPath(table, region, family, hfile);

            SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
              .setType(SnapshotFileInfo.Type.HFILE)
              .setHfile(path.toString())
              .build();

            long size;
            if (storeFile.hasFileSize()) {
              size = storeFile.getFileSize();
            } else {
              size = new HFileLink(conf, path).getFileStatus(fs).getLen();
            }
            files.add(new Pair<SnapshotFileInfo, Long>(fileInfo, size));
          }
        }

        @Override
        public void logFile (final String server, final String logfile)
            throws IOException {
          SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
            .setType(SnapshotFileInfo.Type.WAL)
            .setWalServer(server)
            .setWalName(logfile)
            .build();

          long size = new HLogLink(conf, server, logfile).getFileStatus(fs).getLen();
          files.add(new Pair<SnapshotFileInfo, Long>(fileInfo, size));
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
  static List<List<Pair<SnapshotFileInfo, Long>>> getBalancedSplits(
      final List<Pair<SnapshotFileInfo, Long>> files, final int ngroups) {
    // Sort files by size, from small to big
    Collections.sort(files, new Comparator<Pair<SnapshotFileInfo, Long>>() {
      public int compare(Pair<SnapshotFileInfo, Long> a, Pair<SnapshotFileInfo, Long> b) {
        long r = a.getSecond() - b.getSecond();
        return (r < 0) ? -1 : ((r > 0) ? 1 : 0);
      }
    });

    // create balanced groups
    List<List<Pair<SnapshotFileInfo, Long>>> fileGroups =
      new LinkedList<List<Pair<SnapshotFileInfo, Long>>>();
    long[] sizeGroups = new long[ngroups];
    int hi = files.size() - 1;
    int lo = 0;

    List<Pair<SnapshotFileInfo, Long>> group;
    int dir = 1;
    int g = 0;

    while (hi >= lo) {
      if (g == fileGroups.size()) {
        group = new LinkedList<Pair<SnapshotFileInfo, Long>>();
        fileGroups.add(group);
      } else {
        group = fileGroups.get(g);
      }

      Pair<SnapshotFileInfo, Long> fileInfo = files.get(hi--);

      // add the hi one
      sizeGroups[g] += fileInfo.getSecond();
      group.add(fileInfo);

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

  private static class ExportSnapshotInputFormat extends InputFormat<BytesWritable, NullWritable> {
    @Override
    public RecordReader<BytesWritable, NullWritable> createRecordReader(InputSplit split,
        TaskAttemptContext tac) throws IOException, InterruptedException {
      return new ExportSnapshotRecordReader(((ExportSnapshotInputSplit)split).getSplitKeys());
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String snapshotName = conf.get(CONF_SNAPSHOT_NAME);
      Path snapshotDir = new Path(conf.get(CONF_SNAPSHOT_DIR));
      FileSystem fs = FileSystem.get(snapshotDir.toUri(), conf);

      List<Pair<SnapshotFileInfo, Long>> snapshotFiles = getSnapshotFiles(conf, fs, snapshotDir);
      int mappers = conf.getInt(CONF_NUM_SPLITS, 0);
      if (mappers == 0 && snapshotFiles.size() > 0) {
        mappers = 1 + (snapshotFiles.size() / conf.getInt(CONF_MAP_GROUP, 10));
        mappers = Math.min(mappers, snapshotFiles.size());
        conf.setInt(CONF_NUM_SPLITS, mappers);
        conf.setInt(MR_NUM_MAPS, mappers);
      }

      List<List<Pair<SnapshotFileInfo, Long>>> groups = getBalancedSplits(snapshotFiles, mappers);
      List<InputSplit> splits = new ArrayList(groups.size());
      for (List<Pair<SnapshotFileInfo, Long>> files: groups) {
        splits.add(new ExportSnapshotInputSplit(files));
      }
      return splits;
    }

    private static class ExportSnapshotInputSplit extends InputSplit implements Writable {
      private List<Pair<BytesWritable, Long>> files;
      private long length;

      public ExportSnapshotInputSplit() {
        this.files = null;
      }

      public ExportSnapshotInputSplit(final List<Pair<SnapshotFileInfo, Long>> snapshotFiles) {
        this.files = new ArrayList(snapshotFiles.size());
        for (Pair<SnapshotFileInfo, Long> fileInfo: snapshotFiles) {
          this.files.add(new Pair<BytesWritable, Long>(
            new BytesWritable(fileInfo.getFirst().toByteArray()), fileInfo.getSecond()));
          this.length += fileInfo.getSecond();
        }
      }

      private List<Pair<BytesWritable, Long>> getSplitKeys() {
        return files;
      }

      @Override
      public long getLength() throws IOException, InterruptedException {
        return length;
      }

      @Override
      public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        files = new ArrayList<Pair<BytesWritable, Long>>(count);
        length = 0;
        for (int i = 0; i < count; ++i) {
          BytesWritable fileInfo = new BytesWritable();
          fileInfo.readFields(in);
          long size = in.readLong();
          files.add(new Pair<BytesWritable, Long>(fileInfo, size));
          length += size;
        }
      }

      @Override
      public void write(DataOutput out) throws IOException {
        out.writeInt(files.size());
        for (final Pair<BytesWritable, Long> fileInfo: files) {
          fileInfo.getFirst().write(out);
          out.writeLong(fileInfo.getSecond());
        }
      }
    }

    private static class ExportSnapshotRecordReader
        extends RecordReader<BytesWritable, NullWritable> {
      private final List<Pair<BytesWritable, Long>> files;
      private long totalSize = 0;
      private long procSize = 0;
      private int index = -1;

      ExportSnapshotRecordReader(final List<Pair<BytesWritable, Long>> files) {
        this.files = files;
        for (Pair<BytesWritable, Long> fileInfo: files) {
          totalSize += fileInfo.getSecond();
        }
      }

      @Override
      public void close() { }

      @Override
      public BytesWritable getCurrentKey() { return files.get(index).getFirst(); }

      @Override
      public NullWritable getCurrentValue() { return NullWritable.get(); }

      @Override
      public float getProgress() { return (float)procSize / totalSize; }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext tac) { }

      @Override
      public boolean nextKeyValue() {
        if (index >= 0) {
          procSize += files.get(index).getSecond();
        }
        return(++index < files.size());
      }
    }
  }

  // ==========================================================================
  //  Tool
  // ==========================================================================

  /**
   * Run Map-Reduce Job to perform the files copy.
   */
  private void runCopyJob(final Path inputRoot, final Path outputRoot,
      final String snapshotName, final Path snapshotDir, final boolean verifyChecksum,
      final String filesUser, final String filesGroup, final int filesMode,
      final int mappers, final int bandwidthMB)
          throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    if (filesGroup != null) conf.set(CONF_FILES_GROUP, filesGroup);
    if (filesUser != null) conf.set(CONF_FILES_USER, filesUser);
    if (mappers > 0) {
      conf.setInt(CONF_NUM_SPLITS, mappers);
      conf.setInt(MR_NUM_MAPS, mappers);
    }
    conf.setInt(CONF_FILES_MODE, filesMode);
    conf.setBoolean(CONF_CHECKSUM_VERIFY, verifyChecksum);
    conf.set(CONF_OUTPUT_ROOT, outputRoot.toString());
    conf.set(CONF_INPUT_ROOT, inputRoot.toString());
    conf.setInt(CONF_BANDWIDTH_MB, bandwidthMB);
    conf.set(CONF_SNAPSHOT_NAME, snapshotName);
    conf.set(CONF_SNAPSHOT_DIR, snapshotDir.toString());

    Job job = new Job(conf);
    job.setJobName("ExportSnapshot-" + snapshotName);
    job.setJarByClass(ExportSnapshot.class);
    TableMapReduceUtil.addDependencyJars(job);
    job.setMapperClass(ExportMapper.class);
    job.setInputFormatClass(ExportSnapshotInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapSpeculativeExecution(false);
    job.setNumReduceTasks(0);

    // Acquire the delegation Tokens
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
      new Path[] { inputRoot, outputRoot }, conf);

    // Run the MR Job
    if (!job.waitForCompletion(true)) {
      // TODO: Replace the fixed string with job.getStatus().getFailureInfo()
      // when it will be available on all the supported versions.
      throw new ExportSnapshotException("Copy Files Map-Reduce Job failed");
    }
  }

  private void verifySnapshot(final Configuration baseConf,
      final FileSystem fs, final Path rootDir, final Path snapshotDir) throws IOException {
    // Update the conf with the current root dir, since may be a different cluster
    Configuration conf = new Configuration(baseConf);
    FSUtils.setRootDir(conf, rootDir);
    FSUtils.setFsDefault(conf, FSUtils.getRootDir(conf));
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotReferenceUtil.verifySnapshot(conf, fs, snapshotDir, snapshotDesc);
  }

  /**
   * Set path ownership.
   */
  private void setOwner(final FileSystem fs, final Path path, final String user,
      final String group, final boolean recursive) throws IOException {
    if (user != null || group != null) {
      if (recursive && fs.isDirectory(path)) {
        for (FileStatus child : fs.listStatus(path)) {
          setOwner(fs, child.getPath(), user, group, recursive);
        }
      }
      fs.setOwner(path, user, group);
    }
  }

  /**
   * Execute the export snapshot by copying the snapshot metadata, hfiles and hlogs.
   * @return 0 on success, and != 0 upon failure.
   */
  @Override
  public int run(String[] args) throws IOException {
    boolean verifyTarget = true;
    boolean verifyChecksum = true;
    String snapshotName = null;
    String targetName = null;
    boolean overwrite = false;
    String filesGroup = null;
    String filesUser = null;
    Path outputRoot = null;
    int bandwidthMB = Integer.MAX_VALUE;
    int filesMode = 0;
    int mappers = 0;

    Configuration conf = getConf();
    Path inputRoot = FSUtils.getRootDir(conf);

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-snapshot")) {
        snapshotName = args[++i];
      } else if (cmd.equals("-target")) {
        targetName = args[++i];
      } else if (cmd.equals("-copy-to")) {
        outputRoot = new Path(args[++i]);
      } else if (cmd.equals("-copy-from")) {
        inputRoot = new Path(args[++i]);
        FSUtils.setRootDir(conf, inputRoot);
      } else if (cmd.equals("-no-checksum-verify")) {
        verifyChecksum = false;
      } else if (cmd.equals("-no-target-verify")) {
        verifyTarget = false;
      } else if (cmd.equals("-mappers")) {
        mappers = Integer.parseInt(args[++i]);
      } else if (cmd.equals("-chuser")) {
        filesUser = args[++i];
      } else if (cmd.equals("-chgroup")) {
        filesGroup = args[++i];
      } else if (cmd.equals("-bandwidth")) {
        bandwidthMB = Integer.parseInt(args[++i]);
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

    if (targetName == null) {
      targetName = snapshotName;
    }

    conf.setBoolean("fs." + inputRoot.toUri().getScheme() + ".impl.disable.cache", true);
    FileSystem inputFs = FileSystem.get(inputRoot.toUri(), conf);
    LOG.debug("inputFs=" + inputFs.getUri().toString() + " inputRoot=" + inputRoot);
    conf.setBoolean("fs." + outputRoot.toUri().getScheme() + ".impl.disable.cache", true);
    FileSystem outputFs = FileSystem.get(outputRoot.toUri(), conf);
    LOG.debug("outputFs=" + outputFs.getUri().toString() + " outputRoot=" + outputRoot.toString());

    boolean skipTmp = conf.getBoolean(CONF_SKIP_TMP, false);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, inputRoot);
    Path snapshotTmpDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(targetName, outputRoot);
    Path outputSnapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(targetName, outputRoot);
    Path initialOutputSnapshotDir = skipTmp ? outputSnapshotDir : snapshotTmpDir;

    // Check if the snapshot already exists
    if (outputFs.exists(outputSnapshotDir)) {
      if (overwrite) {
        if (!outputFs.delete(outputSnapshotDir, true)) {
          System.err.println("Unable to remove existing snapshot directory: " + outputSnapshotDir);
          return 1;
        }
      } else {
        System.err.println("The snapshot '" + targetName +
          "' already exists in the destination: " + outputSnapshotDir);
        return 1;
      }
    }

    if (!skipTmp) {
      // Check if the snapshot already in-progress
      if (outputFs.exists(snapshotTmpDir)) {
        if (overwrite) {
          if (!outputFs.delete(snapshotTmpDir, true)) {
            System.err.println("Unable to remove existing snapshot tmp directory: "+snapshotTmpDir);
            return 1;
          }
        } else {
          System.err.println("A snapshot with the same name '"+ targetName +"' may be in-progress");
          System.err.println("Please check "+snapshotTmpDir+". If the snapshot has completed, ");
          System.err.println("consider removing "+snapshotTmpDir+" by using the -overwrite option");
          return 1;
        }
      }
    }

    // Step 1 - Copy fs1:/.snapshot/<snapshot> to  fs2:/.snapshot/.tmp/<snapshot>
    // The snapshot references must be copied before the hfiles otherwise the cleaner
    // will remove them because they are unreferenced.
    try {
      LOG.info("Copy Snapshot Manifest");
      FileUtil.copy(inputFs, snapshotDir, outputFs, initialOutputSnapshotDir, false, false, conf);
      if (filesUser != null || filesGroup != null) {
        setOwner(outputFs, snapshotTmpDir, filesUser, filesGroup, true);
      }
    } catch (IOException e) {
      throw new ExportSnapshotException("Failed to copy the snapshot directory: from=" +
        snapshotDir + " to=" + initialOutputSnapshotDir, e);
    }

    // Write a new .snapshotinfo if the target name is different from the source name
    if (!targetName.equals(snapshotName)) {
      SnapshotDescription snapshotDesc =
        SnapshotDescriptionUtils.readSnapshotInfo(inputFs, snapshotDir)
          .toBuilder()
          .setName(targetName)
          .build();
      SnapshotDescriptionUtils.writeSnapshotInfo(snapshotDesc, snapshotTmpDir, outputFs);
    }

    // Step 2 - Start MR Job to copy files
    // The snapshot references must be copied before the files otherwise the files gets removed
    // by the HFileArchiver, since they have no references.
    try {
      runCopyJob(inputRoot, outputRoot, snapshotName, snapshotDir, verifyChecksum,
                 filesUser, filesGroup, filesMode, mappers, bandwidthMB);

      LOG.info("Finalize the Snapshot Export");
      if (!skipTmp) {
        // Step 3 - Rename fs2:/.snapshot/.tmp/<snapshot> fs2:/.snapshot/<snapshot>
        if (!outputFs.rename(snapshotTmpDir, outputSnapshotDir)) {
          throw new ExportSnapshotException("Unable to rename snapshot directory from=" +
            snapshotTmpDir + " to=" + outputSnapshotDir);
        }
      }

      // Step 4 - Verify snapshot integrity
      if (verifyTarget) {
        LOG.info("Verify snapshot integrity");
        verifySnapshot(conf, outputFs, outputRoot, outputSnapshotDir);
      }

      LOG.info("Export Completed: " + targetName);
      return 0;
    } catch (Exception e) {
      LOG.error("Snapshot export failed", e);
      if (!skipTmp) {
        outputFs.delete(snapshotTmpDir, true);
      }
      outputFs.delete(outputSnapshotDir, true);
      return 1;
    } finally {
      IOUtils.closeStream(inputFs);
      IOUtils.closeStream(outputFs);
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
    System.err.println("  -no-checksum-verify     Do not verify checksum, use name+length only.");
    System.err.println("  -no-target-verify       Do not verify the integrity of the \\" +
        "exported snapshot.");
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
