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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple MR input format for HFiles. This code was borrowed from Apache Crunch project. Updated to
 * the recent version of HBase.
 */
@InterfaceAudience.Private
public class HFileInputFormat extends FileInputFormat<NullWritable, Cell> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileInputFormat.class);

  // Configuration key for pluggable location resolver class name
  // Used to enable rack-aware processing by providing preferred data locality hints
  public static final String CONF_HFILE_LOCATION_RESOLVER_CLASS =
    "hfile.backup.input.file.location.resolver.class";

  /**
   * Interface for resolving file locations to influence InputSplit placement for HFile processing.
   */
  @InterfaceAudience.Public
  public interface HFileLocationResolver {
    /**
     * Get preferred locations for a group of HFiles to optimize rack-aware processing.
     * @param hfiles Collection of HFile paths that will be processed together
     * @return Set of preferred host names for processing these HFiles
     */
    Set<String> getLocationsForInputFiles(final Collection<String> hfiles);
  }

  /**
   * Default no-op implementation of HFileLocationResolver. Provides backward compatibility by
   * returning no location hints.
   */
  public static class NoopHFileLocationResolver implements HFileLocationResolver {
    @Override
    public Set<String> getLocationsForInputFiles(Collection<String> hfiles) {
      // No location hints - lets YARN scheduler decide
      return Collections.emptySet();
    }
  }

  /**
   * File filter that removes all "hidden" files. This might be something worth removing from a more
   * general purpose utility; it accounts for the presence of metadata files created in the way
   * we're doing exports.
   */
  static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Record reader for HFiles.
   */
  private static class HFileRecordReader extends RecordReader<NullWritable, Cell> {

    private Reader in;
    protected Configuration conf;
    private HFileScanner scanner;

    /**
     * A private cache of the key value so it doesn't need to be loaded twice from the scanner.
     */
    private Cell value = null;
    private long count;
    private boolean seeked = false;
    private OptionalLong bulkloadSeqId;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) split;
      conf = context.getConfiguration();
      Path path = fileSplit.getPath();
      FileSystem fs = path.getFileSystem(conf);
      LOG.info("Initialize HFileRecordReader for {}", path);
      this.in = HFile.createReader(fs, path, conf);
      this.bulkloadSeqId = StoreFileInfo.getBulkloadSeqId(path);

      // The file info must be loaded before the scanner can be used.
      // This seems like a bug in HBase, but it's easily worked around.
      this.scanner = in.getScanner(conf, false, false);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean hasNext;
      if (!seeked) {
        LOG.info("Seeking to start");
        hasNext = scanner.seekTo();
        seeked = true;
      } else {
        hasNext = scanner.next();
      }
      if (!hasNext) {
        return false;
      }
      value = scanner.getCell();
      if (value != null && bulkloadSeqId.isPresent()) {
        PrivateCellUtil.setSequenceId(value, bulkloadSeqId.getAsLong());
      }
      count++;
      return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public Cell getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // This would be inaccurate if KVs are not uniformly-sized or we have performed a seek to
      // the start row, but better than nothing anyway.
      return 1.0f * count / in.getEntries();
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        in.close();
        in = null;
      }
    }
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();

    // Explode out directories that match the original FileInputFormat filters
    // Typically these are <regionname>-level dirs, only requiring 1 level of recursion to
    // get the <columnname>-level dirs where the HFile are written, but in some cases
    // <tablename>-level dirs are provided requiring 2 levels of recursion.
    for (FileStatus status : super.listStatus(job)) {
      addFilesRecursively(job, status, result);
    }
    return result;
  }

  private static void addFilesRecursively(JobContext job, FileStatus status,
    List<FileStatus> result) throws IOException {
    if (status.isDirectory()) {
      FileSystem fs = status.getPath().getFileSystem(job.getConfiguration());
      for (FileStatus fileStatus : fs.listStatus(status.getPath(), HIDDEN_FILE_FILTER)) {
        addFilesRecursively(job, fileStatus, result);
      }
    } else {
      result.add(status);
    }
  }

  @Override
  public RecordReader<NullWritable, Cell> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new HFileRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // This file isn't splittable.
    return false;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();

    // Check if location resolver is configured
    String resolverClass = conf.get(CONF_HFILE_LOCATION_RESOLVER_CLASS);
    if (resolverClass == null) {
      LOG.debug("HFile rack-aware processing disabled - no location resolver configured");
      return super.getSplits(context);
    }

    LOG.info("HFile rack-aware processing enabled with location resolver: {}", resolverClass);
    try {
      return createLocationAwareSplits(context, conf);
    } catch (Exception e) {
      LOG.error("Error creating location-aware splits, falling back to standard splits", e);
      return super.getSplits(context);
    }
  }

  private List<InputSplit> createLocationAwareSplits(JobContext context, Configuration conf)
    throws IOException {
    // Get all HFiles from input paths using existing listStatus logic
    List<FileStatus> files = listStatus(context);

    // Load configured location resolver
    Class<? extends HFileLocationResolver> resolverClass =
      conf.getClass(CONF_HFILE_LOCATION_RESOLVER_CLASS, NoopHFileLocationResolver.class,
        HFileLocationResolver.class);
    HFileLocationResolver fileLocationResolver = ReflectionUtils.newInstance(resolverClass, conf);

    // Create InputSplits with location hints
    List<InputSplit> splits = new ArrayList<>();

    for (FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();

      if (length <= 0) {
        LOG.warn("Skipping empty or invalid HFile: {} with length: {}", path, length);
        continue;
      }

      // Get location hints for this file
      Set<String> locations =
        fileLocationResolver.getLocationsForInputFiles(Collections.singletonList(path.toString()));
      String[] locationArray = locations.toArray(new String[0]);

      splits.add(new FileSplit(path, 0, length, locationArray));
    }

    LOG.info("Created {} location-aware InputSplits from {} HFiles", splits.size(), files.size());

    return splits;
  }

}
