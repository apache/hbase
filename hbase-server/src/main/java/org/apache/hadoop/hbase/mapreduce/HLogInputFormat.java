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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple {@link InputFormat} for {@link HLog} files.
 */
@InterfaceAudience.Public
public class HLogInputFormat extends InputFormat<HLogKey, WALEdit> {
  private static final Log LOG = LogFactory.getLog(HLogInputFormat.class);

  public static final String START_TIME_KEY = "hlog.start.time";
  public static final String END_TIME_KEY = "hlog.end.time";

  /**
   * {@link InputSplit} for {@link HLog} files. Each split represent
   * exactly one log file.
   */
  static class HLogSplit extends InputSplit implements Writable {
    private String logFileName;
    private long fileSize;
    private long startTime;
    private long endTime;

    /** for serialization */
    public HLogSplit() {}

    /**
     * Represent an HLogSplit, i.e. a single HLog file.
     * Start- and EndTime are managed by the split, so that HLog files can be
     * filtered before WALEdits are passed to the mapper(s).
     * @param logFileName
     * @param fileSize
     * @param startTime
     * @param endTime
     */
    public HLogSplit(String logFileName, long fileSize, long startTime, long endTime) {
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
      // TODO: Find the data node with the most blocks for this HLog?
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
   * {@link RecordReader} for an {@link HLog} file.
   */
  static class HLogRecordReader extends RecordReader<HLogKey, WALEdit> {
    private HLog.Reader reader = null;
    private HLog.Entry currentEntry = new HLog.Entry();
    private long startTime;
    private long endTime;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      HLogSplit hsplit = (HLogSplit)split;
      Path logFile = new Path(hsplit.getLogFileName());
      Configuration conf = context.getConfiguration();
      LOG.info("Opening reader for "+split);
      try {
        this.reader = HLogFactory.createReader(logFile.getFileSystem(conf), 
            logFile, conf);
      } catch (EOFException x) {
        LOG.info("Ignoring corrupted HLog file: " + logFile
            + " (This is normal when a RegionServer crashed.)");
      }
      this.startTime = hsplit.getStartTime();
      this.endTime = hsplit.getEndTime();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reader == null) return false;

      HLog.Entry temp;
      long i = -1;
      do {
        // skip older entries
        try {
          temp = reader.next(currentEntry);
          i++;
        } catch (EOFException x) {
          LOG.info("Corrupted entry detected. Ignoring the rest of the file."
              + " (This is normal when a RegionServer crashed.)");
          return false;
        }
      }
      while(temp != null && temp.getKey().getWriteTime() < startTime);

      if (temp == null) {
        if (i > 0) LOG.info("Skipped " + i + " entries.");
        LOG.info("Reached end of file.");
        return false;
      } else if (i > 0) {
        LOG.info("Skipped " + i + " entries, until ts: " + temp.getKey().getWriteTime() + ".");
      }
      boolean res = temp.getKey().getWriteTime() <= endTime;
      if (!res) {
        LOG.info("Reached ts: " + temp.getKey().getWriteTime() + " ignoring the rest of the file.");
      }
      return res;
    }

    @Override
    public HLogKey getCurrentKey() throws IOException, InterruptedException {
      return currentEntry.getKey();
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

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    Path inputDir = new Path(conf.get("mapred.input.dir"));

    long startTime = conf.getLong(START_TIME_KEY, Long.MIN_VALUE);
    long endTime = conf.getLong(END_TIME_KEY, Long.MAX_VALUE);

    FileSystem fs = inputDir.getFileSystem(conf);
    List<FileStatus> files = getFiles(fs, inputDir, startTime, endTime);

    List<InputSplit> splits = new ArrayList<InputSplit>(files.size());
    for (FileStatus file : files) {
      splits.add(new HLogSplit(file.getPath().toString(), file.getLen(), startTime, endTime));
    }
    return splits;
  }

  private List<FileStatus> getFiles(FileSystem fs, Path dir, long startTime, long endTime)
      throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    LOG.debug("Scanning " + dir.toString() + " for HLog files");

    FileStatus[] files = fs.listStatus(dir);
    if (files == null) return Collections.emptyList();
    for (FileStatus file : files) {
      if (file.isDir()) {
        // recurse into sub directories
        result.addAll(getFiles(fs, file.getPath(), startTime, endTime));
      } else {
        String name = file.getPath().toString();
        int idx = name.lastIndexOf('.');
        if (idx > 0) {
          try {
            long fileStartTime = Long.parseLong(name.substring(idx+1));
            if (fileStartTime <= endTime) {
              LOG.info("Found: " + name);
              result.add(file);
            }
          } catch (NumberFormatException x) {
            idx = 0;
          }
        }
        if (idx == 0) {
          LOG.warn("File " + name + " does not appear to be an HLog file. Skipping...");
        }
      }
    }
    return result;
  }

  @Override
  public RecordReader<HLogKey, WALEdit> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new HLogRecordReader();
  }
}
