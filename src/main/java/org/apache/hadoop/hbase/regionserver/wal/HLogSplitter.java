/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.wal;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hbase.util.FSUtils.recoverFileLease;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
public class HLogSplitter {

  private static final String LOG_SPLITTER_IMPL = "hbase.hlog.splitter.impl";


  static final Log LOG = LogFactory.getLog(HLogSplitter.class);


  // Parameters for split process
  protected final Path rootDir;
  protected final Path srcDir;
  protected final Path oldLogDir;
  protected final FileSystem fs;
  protected final Configuration conf;

  // Thread pool for closing LogWriters in parallel
  protected final ExecutorService logCloseThreadPool;
  // For checking the latest flushed sequence id
  protected final HMasterRegionInterface master;

  // If an exception is thrown by one of the other threads, it will be
  // stored here.
  protected AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
  // Wait/notify for when data has been produced by the reader thread,
  // consumed by the reader thread, or an exception occurred
  Object dataAvailable = new Object();

  private MonitoredTask status;

  public HLogSplitter(Configuration conf, Path rootDir, Path srcDir,
      Path oldLogDir, FileSystem fs, ExecutorService logCloseThreadPool,
      HMasterRegionInterface master) {
    this.conf = conf;
    this.rootDir = rootDir;
    this.srcDir = srcDir;
    this.oldLogDir = oldLogDir;
    this.fs = fs;
    this.logCloseThreadPool = logCloseThreadPool;
    this.master = master;
  }

  /**
   * Splits a HLog file into a temporary staging area. tmpname is used to build
   * the name of the staging area where the recovered-edits will be separated
   * out by region and stored.
   * <p>
   * If the log file has N regions then N recovered.edits files will be
   * produced. There is no buffering in this code. Instead it relies on the
   * buffering in the SequenceFileWriter.
   * <p>
   * @param rootDir
   * @param tmpname
   * @param logfile
   * @param fs
   * @param conf
   * @param reporter
   * @return false if it is interrupted by the progress-able.
   * @throws IOException
   */
  static public boolean splitLogFileToTemp(Path rootDir, String tmpname,
      FileStatus logfile, FileSystem fs,
      Configuration conf, CancelableProgressable reporter,
      ExecutorService logCloseThreadPool, HMasterRegionInterface master)
      throws IOException {
    HLogSplitter s = new HLogSplitter(conf, rootDir, null, null /* oldLogDir */,
        fs, logCloseThreadPool, master);
    return s.splitLogFileToTemp(logfile, tmpname, reporter);
  }

  public boolean splitLogFileToTemp(FileStatus logfile, String tmpname,
      CancelableProgressable reporter)  throws IOException {
    final Map<byte[], Object> logWriters = Collections.
    synchronizedMap(new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR));
    boolean isCorrupted = false;

    long t0, t1;
    StringBuilder timingInfo = new StringBuilder();

    Preconditions.checkState(status == null);
    status = TaskMonitor.get().createStatus(
        "Splitting log file " + logfile.getPath() +
        "into a temporary staging area.");

    Object BAD_WRITER = new Object();
    logWriters.put(HLog.DUMMY, BAD_WRITER);

    boolean progress_failed = false;

    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors",
        HLog.SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    // How often to send a progress report (default 1/2 master timeout)
    int period = conf.getInt("hbase.splitlog.report.period",
        conf.getInt("hbase.splitlog.manager.timeout",
            ZKSplitLog.DEFAULT_TIMEOUT) / 2);
    Path logPath = logfile.getPath();
    long logLength = logfile.getLen();
    LOG.info("Splitting hlog: " + logPath + ", length=" + logLength);
    status.setStatus("Opening log file");
    Reader in = null;
    try {
      t0 = System.currentTimeMillis();

      in = getReader(fs, logfile, conf, skipErrors);

      t1  = System.currentTimeMillis();
      timingInfo.append("getReader took " + (t1-t0) + " ms. ");
      t0 = t1;

    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not get reader, corrupted log file " + logPath, e);
      ZKSplitLog.markCorrupted(rootDir, tmpname, fs);
      isCorrupted = true;
    }
    if (in == null) {
      status.markComplete("Was nothing to split in log file");
      LOG.warn("Nothing to split in log file " + logPath);
      return true;
    }
    long t = EnvironmentEdgeManager.currentTimeMillis();
    long last_report_at = t;
    if (reporter != null && reporter.progress() == false) {
      status.markComplete("Failed: reporter.progress asked us to terminate");
      return false;
    }
    Map<byte[], Long> lastFlushedSequenceIds =
        new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    int editsCount = 0;
    int editsSkipped = 0;
    Entry entry;
    try {
      while ((entry = getNextLogLine(in,logPath, skipErrors)) != null) {
        byte[] region = entry.getKey().getRegionName();
        Long lastFlushedSequenceId = -1l;
        if (master != null) {
          lastFlushedSequenceId = lastFlushedSequenceIds.get(region);
          if (lastFlushedSequenceId == null) {
            try {

              t0  = System.currentTimeMillis();

              lastFlushedSequenceId = master.getLastFlushedSequenceId(region);
              lastFlushedSequenceIds.put(region, lastFlushedSequenceId);

              t1  = System.currentTimeMillis();
              timingInfo.append("getLastFlushedSeqId took " + (t1-t0) + " ms. ");
              t0 = t1;

            } catch (ConnectException e) {
              lastFlushedSequenceId = -1l;
              LOG.warn("Unable to connect to the master to check " +
                  "the last flushed sequence id", e);
            }
          }
        }
        if (lastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
          editsSkipped++;
          continue;
        }
        Object o = logWriters.get(region);
        if (o == BAD_WRITER) {
          continue;
        }
        WriterAndPath wap = (WriterAndPath)o;
        if (wap == null) {
          t0  = System.currentTimeMillis();

          wap = createWAP(region, entry, rootDir, tmpname, fs, conf);

          t1  = System.currentTimeMillis();
          timingInfo.append("createWAP took " + (t1-t0) + " ms. ");
          t0 = t1;

          if (wap == null) {
            // ignore edits from this region. It doesn't exist anymore.
            // It was probably already split.
            logWriters.put(region, BAD_WRITER);
            continue;
          } else {
            logWriters.put(region, wap);
          }
        }
        wap.w.append(entry);
        editsCount++;
        if (editsCount % interval == 0) {
          status.setStatus("Split " + (editsCount - editsSkipped) +
              " edits, skipped " + editsSkipped + " edits.");
          long t2 = EnvironmentEdgeManager.currentTimeMillis();
          if ((t2 - last_report_at) > period) {
            last_report_at = t;
            if (reporter != null && reporter.progress() == false) {
              status.setStatus("Failed: reporter.progress asked us to terminate");
              progress_failed = true;
              return false;
            }
          }
        }
      }
    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not parse, corrupted log file " + logPath, e);
      ZKSplitLog.markCorrupted(rootDir, tmpname, fs);
      isCorrupted = true;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      throw e;
    } finally {

      t0  = System.currentTimeMillis();

      int n = 0;
      List<Future<Void>> closeResults = new ArrayList<>();
      for (Object o : logWriters.values()) {
        long t2 = EnvironmentEdgeManager.currentTimeMillis();
        if ((t2 - last_report_at) > period) {
          last_report_at = t;
          if ((progress_failed == false) && (reporter != null) &&
              (reporter.progress() == false)) {
            progress_failed = true;
          }
        }
        if (o == BAD_WRITER) {
          continue;
        }
        n++;

        final WriterAndPath wap = (WriterAndPath)o;
        try {
          Future<Void> closeResult =
              logCloseThreadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                  try {
                    wap.w.close();
                  } catch (IOException ioe) {
                    LOG.warn("Failed to close recovered edits writer " + wap.p, 
                        ioe);
                    throw ioe;
                  }
                  LOG.debug("Closed " + wap.p);
                  return null;
                }
              });
          closeResults.add(closeResult);
        } catch (RejectedExecutionException ree) {
          LOG.warn("Could not close writer " + wap.p + " due to thread pool " +
              "shutting down.", ree);
        }
      }
      try {
        for (Future<Void> closeResult : closeResults) {
          // Uncaught unchecked exception from the threads performing close
          // should be propagated into the main thread.
          closeResult.get();
        }
      } catch (ExecutionException ee) {
        LOG.error("Unexpected exception while closing a log writer", ee);
        throw new IOException("Unexpected exception while closing", ee);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while closing a log writer", ie);
        throw new InterruptedIOException("Interrupted while closing " + ie);
      }

      t1  = System.currentTimeMillis();
      timingInfo.append("closing logWriters took " + (t1-t0) + " ms. ");
      t0 = t1;

      try {
        in.close();
      } catch (IOException ioe) {
        LOG.warn("Failed to close log reader " + logfile.getPath(), ioe);
      }

      t1  = System.currentTimeMillis();
      timingInfo.append("closing inputLog took " + (t1-t0) + " ms. ");
      t0 = t1;

      String msg = "processed " + editsCount + " edits across " + n + 
          " regions" + " threw away edits for " + (logWriters.size() - n) + 
          " regions" + " log file = " + logPath + " is corrupted = " + 
          isCorrupted + " progress interrupted? = " + progress_failed;
      LOG.info(msg);
      LOG.debug(timingInfo);
      status.markComplete(msg);
    }
    return !progress_failed;
  }

  /**
   * Completes the work done by splitLogFileToTemp by moving the
   * recovered.edits from the staging area to the respective region server's
   * directories.
   * <p>
   * It is invoked by SplitLogManager once it knows that one of the
   * SplitLogWorkers have completed the splitLogFileToTemp() part. If the
   * master crashes then this function might get called multiple times.
   * <p>
   * @param tmpname
   * @param conf
   * @throws IOException
   */
  public static void moveRecoveredEditsFromTemp(String tmpname,
      String logfile, Configuration conf)
  throws IOException{
    Path rootdir = FSUtils.getRootDir(conf);
    Path oldLogDir = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    moveRecoveredEditsFromTemp(tmpname, rootdir, oldLogDir, logfile, conf);
  }

  public static void moveRecoveredEditsFromTemp(String tmpname,
      Path rootdir, Path oldLogDir,
      String logfile, Configuration conf)
  throws IOException{
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    FileSystem fs;
    fs = rootdir.getFileSystem(conf);
    Path logPath = new Path(logfile);
    if (ZKSplitLog.isCorrupted(rootdir, tmpname, fs)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    Path stagingDir = ZKSplitLog.getSplitLogDir(rootdir, tmpname);
    List<FileStatus> files = listAll(fs, stagingDir);
    for (FileStatus f : files) {
      Path src = f.getPath();
      Path dst = ZKSplitLog.stripSplitLogTempDir(rootdir, src);
      if (ZKSplitLog.isCorruptFlagFile(dst)) {
        continue;
      }
      if (fs.exists(src)) {
        if (fs.exists(dst)) {
          fs.delete(dst, false);
        } else {
          Path dstdir = dst.getParent();
          if (!fs.exists(dstdir)) {
            if (!fs.mkdirs(dstdir)) LOG.warn("mkdir failed on " + dstdir);
          }
        }
        fs.rename(src, dst);
        LOG.debug(" moved " + src + " => " + dst);
      } else {
        LOG.debug("Could not move recovered edits from " + src +
            " as it doesn't exist");
      }
    }
    HLog.archiveLogs(corruptedLogs, processedLogs, oldLogDir, fs, conf);
    fs.delete(stagingDir, true);
    return;
  }

  private static List<FileStatus> listAll(FileSystem fs, Path dir)
  throws IOException {
    List<FileStatus> fset = new ArrayList<FileStatus>(100);
    FileStatus [] files = fs.listStatus(dir);
    if (files != null) {
      for (FileStatus f : files) {
        if (f.isDir()) {
          fset.addAll(listAll(fs, f.getPath()));
        } else {
          fset.add(f);
        }
      }
    }
    return fset;
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   *
   * @param fs
   * @param file
   * @param conf
   * @return A new Reader instance
   * @throws IOException
   * @throws CorruptedLogFileException
   */
  protected Reader getReader(FileSystem fs, FileStatus file, Configuration conf,
      boolean skipErrors)
      throws IOException, CorruptedLogFileException {
    Path path = file.getPath();
    long length = file.getLen();
    Reader in;


    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File " + path + " might be still open, length is 0");
    }

    try {
      recoverFileLease(fs, path, conf);
      try {
        in = getReader(fs, path, conf);
      } catch (EOFException e) {
        if (length <= 0) {
          // TODO should we ignore an empty, not-last log file if skip.errors
          // is false? Either way, the caller should decide what to do. E.g.
          // ignore if this is the last log in sequence.
          // TODO is this scenario still possible if the log has been
          // recovered (i.e. closed)
          LOG.warn("Could not open " + path + " for reading. File is empty", e);
          return null;
        } else {
          // EOFException being ignored
          return null;
        }
      }
    } catch (IOException e) {
      if (!skipErrors) {
        throw e;
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Could not open hlog " +
            path + " ignoring");
      t.initCause(e);
      throw t;
    }
    return in;
  }

  static private Entry getNextLogLine(Reader in, Path path, boolean skipErrors)
  throws CorruptedLogFileException, IOException {
    try {
      return in.next();
    } catch (EOFException eof) {
      // truncated files are expected if a RS crashes (see HBASE-2643)
      LOG.info("EOF from hlog " + path + ".  continuing");
      return null;
    } catch (IOException e) {
      // If the IOE resulted from bad file format,
      // then this problem is idempotent and retrying won't help
      if (e.getCause() != null &&
          (e.getCause() instanceof ParseException ||
           e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception " + e.getCause().toString() + " from hlog "
           + path + ".  continuing");
        return null;
      }
      if (!skipErrors) {
        throw e;
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Ignoring exception" +
            " while parsing hlog " + path + ". Marking as corrupted");
      t.initCause(e);
      throw t;
    }
  }



  /**
   * Create a new {@link Writer} for writing log splits.
   */
  protected Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
    return HLog.createWriter(fs, logfile, conf);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   */
  protected Reader getReader(FileSystem fs, Path curLogFile, Configuration conf)
      throws IOException {
    return HLog.getReader(fs, curLogFile, conf);
  }


  private WriterAndPath createWAP(byte[] region, Entry entry,
      Path rootdir, String tmpname, FileSystem fs, Configuration conf)
  throws IOException {
    Path regionedits = HLog.getRegionSplitEditsPath(fs, entry, rootdir,
        tmpname == null);
    if (regionedits == null) {
      return null;
    }
    if ((tmpname == null) && fs.exists(regionedits)) {
      LOG.warn("Found existing old edits file. It could be the "
          + "result of a previous failed split attempt. Deleting "
          + regionedits + ", length="
          + fs.getFileStatus(regionedits).getLen());
      if (!fs.delete(regionedits, false)) {
        LOG.warn("Failed delete of old " + regionedits);
      }
    }
    Path editsfile;
    if (tmpname != null) {
      // During distributed log splitting the output by each
      // SplitLogWorker is written to a temporary area.
      editsfile = convertRegionEditsToTemp(rootdir, regionedits, tmpname);
    } else {
      editsfile = regionedits;
    }
    Writer w = createWriter(fs, editsfile, conf);
    LOG.debug("Creating writer path=" + editsfile + " region="
        + Bytes.toStringBinary(region));
    return (new WriterAndPath(editsfile, w));
  }

  Path convertRegionEditsToTemp(Path rootdir, Path edits, String tmpname) {
    List<String> components = new ArrayList<String>(10);
    do {
      components.add(edits.getName());
      edits = edits.getParent();
    } while (edits.depth() > rootdir.depth());
    Path ret = ZKSplitLog.getSplitLogDir(rootdir, tmpname);
    for (int i = components.size() - 1; i >= 0; i--) {
      ret = new Path(ret, components.get(i));
    }
    try {
      if (fs.exists(ret)) {
        LOG.warn("Found existing old temporary edits file. It could be the "
            + "result of a previous failed split attempt. Deleting "
            + ret + ", length="
            + fs.getFileStatus(ret).getLen());
        if (!fs.delete(ret, false)) {
          LOG.warn("Failed delete of old " + ret);
        }
      }
      Path dir = ret.getParent();
      if (!fs.exists(dir)) {
        if (!fs.mkdirs(dir)) LOG.warn("mkdir failed on " + dir);
      }
    } catch (IOException e) {
      LOG.warn("Could not prepare temp staging area ", e);
      // ignore, exceptions will be thrown elsewhere
    }
    return ret;
  }




  /**
   *  Private data structure that wraps a Writer and its Path,
   *  also collecting statistics about the data written to this
   *  output.
   */
  private final static class WriterAndPath {
    final Path p;
    final Writer w;

    WriterAndPath(final Path p, final Writer w) {
      this.p = p;
      this.w = w;
    }
  }

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;
    CorruptedLogFileException(String s) {
      super(s);
    }
  }
}
