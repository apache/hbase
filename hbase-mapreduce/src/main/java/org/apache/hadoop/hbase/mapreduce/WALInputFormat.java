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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * Simple {@link InputFormat} for {@link org.apache.hadoop.hbase.wal.WAL} files.
 */
@InterfaceAudience.Public
public class WALInputFormat extends InputFormat<WALKey, WALEdit> {
  private static final Logger LOG = LoggerFactory.getLogger(WALInputFormat.class);

  public static final String START_TIME_KEY = "wal.start.time";
  public static final String END_TIME_KEY = "wal.end.time";

  /**
   * {@link InputSplit} for {@link WAL} files. Each split represent
   * exactly one log file.
   */
  static class WALSplit extends InputSplit implements Writable {
    private String logFileName;
    private long fileSize;
    private long startTime;
    private long endTime;

    /** for serialization */
    public WALSplit() {}

    /**
     * Represent an WALSplit, i.e. a single WAL file.
     * Start- and EndTime are managed by the split, so that WAL files can be
     * filtered before WALEdits are passed to the mapper(s).
     * @param logFileName
     * @param fileSize
     * @param startTime
     * @param endTime
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
   * {@link RecordReader} for an {@link WAL} file.
   * Implementation shared with deprecated HLogInputFormat.
   */
  static abstract class WALRecordReader<K extends WALKey> extends RecordReader<K, WALEdit> {
    private Reader reader = null;
    // visible until we can remove the deprecated HLogInputFormat
    Entry currentEntry = new Entry();
    private long startTime;
    private long endTime;
    private Configuration conf;
    private Path logFile;
    private long currentPos;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      WALSplit hsplit = (WALSplit)split;
      logFile = new Path(hsplit.getLogFileName());
      conf = context.getConfiguration();
      LOG.info("Opening reader for "+split);
      openReader(logFile);
      this.startTime = hsplit.getStartTime();
      this.endTime = hsplit.getEndTime();
    }

    private void openReader(Path path) throws IOException
    {
      closeReader();
      reader = AbstractFSWALProvider.openReader(path, conf);
      seek();
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

    private void seek() throws IOException {
      if (currentPos != 0) {
        reader.seek(currentPos);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reader == null) return false;
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
          if (i > 0) LOG.info("Skipped " + i + " entries.");
          LOG.info("Reached end of file.");
          return false;
        } else if (i > 0) {
          LOG.info("Skipped " + i + " entries, until ts: " + temp.getKey().getWriteTime() + ".");
        }
        boolean res = temp.getKey().getWriteTime() <= endTime;
        if (!res) {
          LOG.info("Reached ts: " + temp.getKey().getWriteTime()
              + " ignoring the rest of the file.");
        }
        return res;
      } catch (IOException e) {
        Path archivedLog = AbstractFSWALProvider.getArchivedLogPath(logFile, conf);
        if (logFile != archivedLog) {
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
      if (reader != null) this.reader.close();
    }
  }

  /**
   * handler for non-deprecated WALKey version. fold into WALRecordReader once we no longer
   * need to support HLogInputFormat.
   */
  static class WALKeyRecordReader extends WALRecordReader<WALKey> {
    @Override
    public WALKey getCurrentKey() throws IOException, InterruptedException {
      return currentEntry.getKey();
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
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
    long startTime = conf.getLong(startKey, Long.MIN_VALUE);
    long endTime = conf.getLong(endKey, Long.MAX_VALUE);

    List<FileStatus> allFiles = new ArrayList<FileStatus>();
    for(Path inputPath: inputPaths){
      FileSystem fs = inputPath.getFileSystem(conf);
      try {
        List<FileStatus> files = getFiles(fs, inputPath, startTime, endTime);
        allFiles.addAll(files);
      } catch (FileNotFoundException e) {
        if (ignoreMissing) {
          LOG.warn("File "+ inputPath +" is missing. Skipping it.");
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
    return StringUtils.stringToPath(
      inpDirs.split(conf.get(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ",")));
  }

  private List<FileStatus> getFiles(FileSystem fs, Path dir, long startTime, long endTime)
      throws IOException {
    List<FileStatus> result = new ArrayList<>();
    LOG.debug("Scanning " + dir.toString() + " for WAL files");

    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(dir);
    if (!iter.hasNext()) return Collections.emptyList();
    while (iter.hasNext()) {
      LocatedFileStatus file = iter.next();
      if (file.isDirectory()) {
        // recurse into sub directories
        result.addAll(getFiles(fs, file.getPath(), startTime, endTime));
      } else {
        String name = file.getPath().toString();
        int idx = name.lastIndexOf('.');
        if (idx > 0) {
          try {
            long fileStartTime = Long.parseLong(name.substring(idx+1));
            if (fileStartTime <= endTime) {
              LOG.info("Found: " + file);
              result.add(file);
            }
          } catch (NumberFormatException x) {
            idx = 0;
          }
        }
        if (idx == 0) {
          LOG.warn("File " + name + " does not appear to be an WAL file. Skipping...");
        }
      }
    }
    return result;
  }

  @Override
  public RecordReader<WALKey, WALEdit> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new WALKeyRecordReader();
  }
}
