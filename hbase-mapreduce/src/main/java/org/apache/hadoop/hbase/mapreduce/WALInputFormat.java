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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple {@link InputFormat} for {@link org.apache.hadoop.hbase.wal.WAL} files.
 */
@InterfaceAudience.Public
public class WALInputFormat extends InputFormat<WALKey, WALEdit> {
  private static final Logger LOG = LoggerFactory.getLogger(WALInputFormat.class);

  public static final String START_TIME_KEY = "wal.start.time";
  public static final String END_TIME_KEY = "wal.end.time";

  /**
   * {@link InputSplit} for {@link WAL} files. Each split represent exactly one log file.
   */
  static class WALSplit extends InputSplit implements Writable {
    private String logFileName;
    private long fileSize;
    private long startTime;
    private long endTime;

    /** for serialization */
    public WALSplit() {
    }

    /**
     * Represent an WALSplit, i.e. a single WAL file. Start- and EndTime are managed by the split,
     * so that WAL files can be filtered before WALEdits are passed to the mapper(s).
     */
    public WALSplit(String logFileName, long fileSize, long startTime, long endTime) {
      this.logFileName = logFileName;
      this.fileSize = fileSize;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return fileSize;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      // TODO: Find the data node with the most blocks for this WAL?
      return new String[] {};
    }

    public String getLogFileName() {
      return logFileName;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      logFileName = in.readUTF();
      fileSize = in.readLong();
      startTime = in.readLong();
      endTime = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(logFileName);
      out.writeLong(fileSize);
      out.writeLong(startTime);
      out.writeLong(endTime);
    }

    @Override
    public String toString() {
      return logFileName + " (" + startTime + ":" + endTime + ") length:" + fileSize;
    }
  }

  /**
   * {@link RecordReader} for an {@link WAL} file. Implementation shared with deprecated
   * HLogInputFormat.
   */
  static abstract class WALRecordReader<K extends WALKey> extends RecordReader<K, WALEdit> {
    private WALStreamReader reader = null;
    // visible until we can remove the deprecated HLogInputFormat
    Entry currentEntry = new Entry();
    private long startTime;
    private long endTime;
    private Configuration conf;
    private Path logFile;
    private long currentPos;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DCN_NULLPOINTER_EXCEPTION",
        justification = "HDFS-4380")
    private WALStreamReader openReader(Path path, long startPosition) throws IOException {
      long retryInterval = 2000; // 2 sec
      int maxAttempts = 30;
      int attempt = 0;
      Exception ee = null;
      WALStreamReader reader = null;
      while (reader == null && attempt++ < maxAttempts) {
        try {
          // Detect if this is a new file, if so get a new reader else
          // reset the current reader so that we see the new data
          reader =
            WALFactory.createStreamReader(path.getFileSystem(conf), path, conf, startPosition);
          return reader;
        } catch (LeaseNotRecoveredException lnre) {
          // HBASE-15019 the WAL was not closed due to some hiccup.
          LOG.warn("Try to recover the WAL lease " + path, lnre);
          AbstractFSWALProvider.recoverLease(conf, path);
          reader = null;
          ee = lnre;
        } catch (NullPointerException npe) {
          // Workaround for race condition in HDFS-4380
          // which throws a NPE if we open a file before any data node has the most recent block
          // Just sleep and retry. Will require re-reading compressed WALs for compressionContext.
          LOG.warn("Got NPE opening reader, will retry.");
          reader = null;
          ee = npe;
        }
        if (reader == null) {
          // sleep before next attempt
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      throw new IOException("Could not open reader", ee);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      WALSplit hsplit = (WALSplit) split;
      logFile = new Path(hsplit.getLogFileName());
      conf = context.getConfiguration();
      LOG.info("Opening {} for {}", logFile, split);
      openReader(logFile);
      this.startTime = hsplit.getStartTime();
      this.endTime = hsplit.getEndTime();
    }

    private void openReader(Path path) throws IOException {
      closeReader();
      reader = openReader(path, currentPos > 0 ? currentPos : -1);
      setCurrentPath(path);
    }

    private void setCurrentPath(Path path) {
      this.logFile = path;
    }

    private void closeReader() throws IOException {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reader == null) {
        return false;
      }
      this.currentPos = reader.getPosition();
      Entry temp;
      long i = -1;
      try {
        do {
          // skip older entries
          try {
            temp = reader.next(currentEntry);
            i++;
          } catch (EOFException x) {
            LOG.warn("Corrupted entry detected. Ignoring the rest of the file."
              + " (This is normal when a RegionServer crashed.)");
            return false;
          }
        } while (temp != null && temp.getKey().getWriteTime() < startTime);

        if (temp == null) {
          if (i > 0) {
            LOG.info("Skipped " + i + " entries.");
          }
          LOG.info("Reached end of file.");
          return false;
        } else if (i > 0) {
          LOG.info("Skipped " + i + " entries, until ts: " + temp.getKey().getWriteTime() + ".");
        }
        boolean res = temp.getKey().getWriteTime() <= endTime;
        if (!res) {
          LOG.info(
            "Reached ts: " + temp.getKey().getWriteTime() + " ignoring the rest of the file.");
        }
        return res;
      } catch (IOException e) {
        Path archivedLog = AbstractFSWALProvider.findArchivedLog(logFile, conf);
        // archivedLog can be null if unable to locate in archiveDir.
        if (archivedLog != null) {
          openReader(archivedLog);
          // Try call again in recursion
          return nextKeyValue();
        } else {
          throw e;
        }
      }
    }

    @Override
    public WALEdit getCurrentValue() throws IOException, InterruptedException {
      return currentEntry.getEdit();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // N/A depends on total number of entries, which is unknown
      return 0;
    }

    @Override
    public void close() throws IOException {
      LOG.info("Closing reader");
      if (reader != null) {
        this.reader.close();
      }
    }
  }

  /**
   * handler for non-deprecated WALKey version. fold into WALRecordReader once we no longer need to
   * support HLogInputFormat.
   */
  static class WALKeyRecordReader extends WALRecordReader<WALKey> {
    @Override
    public WALKey getCurrentKey() throws IOException, InterruptedException {
      return currentEntry.getKey();
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return getSplits(context, START_TIME_KEY, END_TIME_KEY);
  }

  /**
   * implementation shared with deprecated HLogInputFormat
   */
  List<InputSplit> getSplits(final JobContext context, final String startKey, final String endKey)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    boolean ignoreMissing = conf.getBoolean(WALPlayer.IGNORE_MISSING_FILES, false);
    Path[] inputPaths = getInputPaths(conf);
    // get delegation token for the filesystem
    TokenCache.obtainTokensForNamenodes(context.getCredentials(), inputPaths, conf);
    long startTime = conf.getLong(startKey, Long.MIN_VALUE);
    long endTime = conf.getLong(endKey, Long.MAX_VALUE);

    List<FileStatus> allFiles = new ArrayList<FileStatus>();
    for (Path inputPath : inputPaths) {
      FileSystem fs = inputPath.getFileSystem(conf);
      try {
        List<FileStatus> files = getFiles(fs, inputPath, startTime, endTime, conf);
        allFiles.addAll(files);
      } catch (FileNotFoundException e) {
        if (ignoreMissing) {
          LOG.warn("File " + inputPath + " is missing. Skipping it.");
          continue;
        }
        throw e;
      }
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(allFiles.size());
    for (FileStatus file : allFiles) {
      splits.add(new WALSplit(file.getPath().toString(), file.getLen(), startTime, endTime));
    }
    return splits;
  }

  private Path[] getInputPaths(Configuration conf) {
    String inpDirs = conf.get(FileInputFormat.INPUT_DIR);
    return StringUtils
      .stringToPath(inpDirs.split(conf.get(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ",")));
  }

  /**
   * @param startTime If file looks like it has a timestamp in its name, we'll check if newer or
   *                  equal to this value else we will filter out the file. If name does not seem to
   *                  have a timestamp, we will just return it w/o filtering.
   * @param endTime   If file looks like it has a timestamp in its name, we'll check if older or
   *                  equal to this value else we will filter out the file. If name does not seem to
   *                  have a timestamp, we will just return it w/o filtering.
   */
  private List<FileStatus> getFiles(FileSystem fs, Path dir, long startTime, long endTime,
    Configuration conf) throws IOException {
    List<FileStatus> result = new ArrayList<>();
    LOG.debug("Scanning " + dir.toString() + " for WAL files");
    RemoteIterator<LocatedFileStatus> iter = listLocatedFileStatus(fs, dir, conf);
    if (!iter.hasNext()) {
      return Collections.emptyList();
    }
    while (iter.hasNext()) {
      LocatedFileStatus file = iter.next();
      if (file.isDirectory()) {
        // Recurse into sub directories
        result.addAll(getFiles(fs, file.getPath(), startTime, endTime, conf));
      } else {
        addFile(result, file, startTime, endTime);
      }
    }
    // TODO: These results should be sorted? Results could be content of recovered.edits directory
    // -- null padded increasing numeric -- or a WAL file w/ timestamp suffix or timestamp and
    // then meta suffix. See AbstractFSWALProvider#WALStartTimeComparator
    return result;
  }

  static void addFile(List<FileStatus> result, LocatedFileStatus lfs, long startTime,
    long endTime) {
    long timestamp = WAL.getTimestamp(lfs.getPath().getName());
    if (timestamp > 0) {
      // Looks like a valid timestamp.
      if (timestamp <= endTime && timestamp >= startTime) {
        LOG.info("Found {}", lfs.getPath());
        result.add(lfs);
      } else {
        LOG.info("Skipped {}, outside range [{}/{} - {}/{}]", lfs.getPath(), startTime,
          Instant.ofEpochMilli(startTime), endTime, Instant.ofEpochMilli(endTime));
      }
    } else {
      // If no timestamp, add it regardless.
      LOG.info("Found (no-timestamp!) {}", lfs);
      result.add(lfs);
    }
  }

  @Override
  public RecordReader<WALKey, WALEdit> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new WALKeyRecordReader();
  }

  /**
   * Attempts to return the {@link LocatedFileStatus} for the given directory. If the directory does
   * not exist, it will check if the directory is an archived log file and try to find it
   */
  private static RemoteIterator<LocatedFileStatus> listLocatedFileStatus(FileSystem fs, Path dir,
    Configuration conf) throws IOException {
    try {
      return fs.listLocatedStatus(dir);
    } catch (FileNotFoundException e) {
      if (AbstractFSWALProvider.isArchivedLogFile(dir)) {
        throw e;
      }

      LOG.warn("Log file {} not found, trying to find it in archive directory.", dir);
      Path archiveFile = AbstractFSWALProvider.findArchivedLog(dir, conf);
      if (archiveFile == null) {
        LOG.error("Did not find archive file for {}", dir);
        throw e;
      }

      return fs.listLocatedStatus(archiveFile);
    }
  }
}
