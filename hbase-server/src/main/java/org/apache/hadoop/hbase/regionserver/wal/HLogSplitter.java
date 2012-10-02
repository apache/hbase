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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
@InterfaceAudience.Private
public class HLogSplitter {
  private static final String LOG_SPLITTER_IMPL = "hbase.hlog.splitter.impl";

  /**
   * Name of file that holds recovered edits written by the wal log splitting
   * code, one per region
   */
  public static final String RECOVERED_EDITS = "recovered.edits";


  static final Log LOG = LogFactory.getLog(HLogSplitter.class);

  private boolean hasSplit = false;
  private long splitTime = 0;
  private long splitSize = 0;


  // Parameters for split process
  protected final Path rootDir;
  protected final Path srcDir;
  protected final Path oldLogDir;
  protected final FileSystem fs;
  protected final Configuration conf;

  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  OutputSink outputSink;
  EntryBuffers entryBuffers;

  // If an exception is thrown by one of the other threads, it will be
  // stored here.
  protected AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

  // Wait/notify for when data has been produced by the reader thread,
  // consumed by the reader thread, or an exception occurred
  Object dataAvailable = new Object();

  private MonitoredTask status;

  // Used in distributed log splitting
  private DistributedLogSplittingHelper distributedLogSplittingHelper = null;

  // For checking the latest flushed sequence id
  protected final LastSequenceId sequenceIdChecker;

  /**
   * Create a new HLogSplitter using the given {@link Configuration} and the
   * <code>hbase.hlog.splitter.impl</code> property to derived the instance
   * class to use.
   * <p>
   * @param conf
   * @param rootDir hbase directory
   * @param srcDir logs directory
   * @param oldLogDir directory where processed logs are archived to
   * @param fs FileSystem
   * @return New HLogSplitter instance
   */
  public static HLogSplitter createLogSplitter(Configuration conf,
      final Path rootDir, final Path srcDir,
      Path oldLogDir, final FileSystem fs)  {

    @SuppressWarnings("unchecked")
    Class<? extends HLogSplitter> splitterClass = (Class<? extends HLogSplitter>) conf
        .getClass(LOG_SPLITTER_IMPL, HLogSplitter.class);
    try {
       Constructor<? extends HLogSplitter> constructor =
         splitterClass.getConstructor(
          Configuration.class, // conf
          Path.class, // rootDir
          Path.class, // srcDir
          Path.class, // oldLogDir
          FileSystem.class, // fs
          LastSequenceId.class);
      return constructor.newInstance(conf, rootDir, srcDir, oldLogDir, fs, null);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public HLogSplitter(Configuration conf, Path rootDir, Path srcDir,
      Path oldLogDir, FileSystem fs, LastSequenceId idChecker) {
    this.conf = conf;
    this.rootDir = rootDir;
    this.srcDir = srcDir;
    this.oldLogDir = oldLogDir;
    this.fs = fs;
    this.sequenceIdChecker = idChecker;

    entryBuffers = new EntryBuffers(
        conf.getInt("hbase.regionserver.hlog.splitlog.buffersize",
            128*1024*1024));
    outputSink = new OutputSink();
  }

  /**
   * Split up a bunch of regionserver commit log files that are no longer being
   * written to, into new files, one per region for region to replay on startup.
   * Delete the old log files when finished.
   *
   * @throws IOException will throw if corrupted hlogs aren't tolerated
   * @return the list of splits
   */
  public List<Path> splitLog()
      throws IOException {
    Preconditions.checkState(!hasSplit,
        "An HLogSplitter instance may only be used once");
    hasSplit = true;

    status = TaskMonitor.get().createStatus(
        "Splitting logs in " + srcDir);
    
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    
    status.setStatus("Determining files to split...");
    List<Path> splits = null;
    if (!fs.exists(srcDir)) {
      // Nothing to do
      status.markComplete("No log directory existed to split.");
      return splits;
    }
    FileStatus[] logfiles = fs.listStatus(srcDir);
    if (logfiles == null || logfiles.length == 0) {
      // Nothing to do
      return splits;
    }
    logAndReport("Splitting " + logfiles.length + " hlog(s) in "
    + srcDir.toString());
    splits = splitLog(logfiles);

    splitTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
    String msg = "hlog file splitting completed in " + splitTime +
        " ms for " + srcDir.toString();
    status.markComplete(msg);
    LOG.info(msg);
    return splits;
  }
  
  private void logAndReport(String msg) {
    status.setStatus(msg);
    LOG.info(msg);
  }

  /**
   * @return time that this split took
   */
  public long getTime() {
    return this.splitTime;
  }

  /**
   * @return aggregate size of hlogs that were split
   */
  public long getSize() {
    return this.splitSize;
  }

  /**
   * @return a map from encoded region ID to the number of edits written out
   * for that region.
   */
  Map<byte[], Long> getOutputCounts() {
    Preconditions.checkState(hasSplit);
    return outputSink.getOutputCounts();
  }

  void setDistributedLogSplittingHelper(DistributedLogSplittingHelper helper) {
    this.distributedLogSplittingHelper = helper;
  }

  /**
   * Splits the HLog edits in the given list of logfiles (that are a mix of edits
   * on multiple regions) by region and then splits them per region directories,
   * in batches of (hbase.hlog.split.batch.size)
   * <p>
   * This process is split into multiple threads. In the main thread, we loop
   * through the logs to be split. For each log, we:
   * <ul>
   *   <li> Recover it (take and drop HDFS lease) to ensure no other process can write</li>
   *   <li> Read each edit (see {@link #parseHLog}</li>
   *   <li> Mark as "processed" or "corrupt" depending on outcome</li>
   * </ul>
   * <p>
   * Each edit is passed into the EntryBuffers instance, which takes care of
   * memory accounting and splitting the edits by region.
   * <p>
   * The OutputSink object then manages N other WriterThreads which pull chunks
   * of edits from EntryBuffers and write them to the output region directories.
   * <p>
   * After the process is complete, the log files are archived to a separate
   * directory.
   */
  private List<Path> splitLog(final FileStatus[] logfiles) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>(logfiles.length);
    List<Path> corruptedLogs = new ArrayList<Path>(logfiles.length);
    List<Path> splits;

    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors", true);

    countTotalBytes(logfiles);
    splitSize = 0;

    outputSink.startWriterThreads();

    try {
      int i = 0;
      for (FileStatus log : logfiles) {
       Path logPath = log.getPath();
        long logLength = log.getLen();
        splitSize += logLength;
        logAndReport("Splitting hlog " + (i++ + 1) + " of " + logfiles.length
            + ": " + logPath + ", length=" + logLength);
        Reader in = null;
        try {
          in = getReader(fs, log, conf, skipErrors);
          if (in != null) {
            parseHLog(in, logPath, entryBuffers, fs, conf, skipErrors);
          }
          processedLogs.add(logPath);
        } catch (CorruptedLogFileException e) {
          LOG.info("Got while parsing hlog " + logPath +
              ". Marking as corrupted", e);
          corruptedLogs.add(logPath);
        } finally {
          if (in != null) {
            try {
              in.close();
            } catch (IOException e) {
              LOG.warn("Close log reader threw exception -- continuing", e);
            }
          }
        }
      }
      status.setStatus("Log splits complete. Checking for orphaned logs.");
      
      if (fs.listStatus(srcDir).length > processedLogs.size()
          + corruptedLogs.size()) {
        throw new OrphanHLogAfterSplitException(
            "Discovered orphan hlog after split. Maybe the "
            + "HRegionServer was not dead when we started");
      }
    } finally {
      status.setStatus("Finishing writing output logs and closing down.");
      splits = outputSink.finishWritingAndClose();
    }
    status.setStatus("Archiving logs after completed split");
    archiveLogs(srcDir, corruptedLogs, processedLogs, oldLogDir, fs, conf);
    return splits;
  }

  /**
   * @return the total size of the passed list of files.
   */
  private static long countTotalBytes(FileStatus[] logfiles) {
    long ret = 0;
    for (FileStatus stat : logfiles) {
      ret += stat.getLen();
    }
    return ret;
  }

  /**
   * Splits a HLog file into region's recovered-edits directory
   * <p>
   * If the log file has N regions then N recovered.edits files will be
   * produced.
   * <p>
   * @param rootDir
   * @param logfile
   * @param fs
   * @param conf
   * @param reporter
   * @param idChecker
   * @return false if it is interrupted by the progress-able.
   * @throws IOException
   */
  static public boolean splitLogFile(Path rootDir, FileStatus logfile,
      FileSystem fs, Configuration conf, CancelableProgressable reporter,
      LastSequenceId idChecker)
      throws IOException {
    HLogSplitter s = new HLogSplitter(conf, rootDir, null, null /* oldLogDir */, fs, idChecker);
    return s.splitLogFile(logfile, reporter);
  }

  /**
   * Splits a HLog file into region's recovered-edits directory
   * <p>
   * If the log file has N regions then N recovered.edits files will be
   * produced.
   * <p>
   * @param rootDir
   * @param logfile
   * @param fs
   * @param conf
   * @param reporter
   * @return false if it is interrupted by the progress-able.
   * @throws IOException
   */
  static public boolean splitLogFile(Path rootDir, FileStatus logfile,
      FileSystem fs, Configuration conf, CancelableProgressable reporter)
      throws IOException {
    return HLogSplitter.splitLogFile(rootDir, logfile, fs, conf, reporter, null);
  }

  public boolean splitLogFile(FileStatus logfile,
      CancelableProgressable reporter) throws IOException {
    boolean isCorrupted = false;
    Preconditions.checkState(status == null);
    status = TaskMonitor.get().createStatus(
        "Splitting log file " + logfile.getPath() +
        "into a temporary staging area.");
    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors",
        HLog.SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    Path logPath = logfile.getPath();
    long logLength = logfile.getLen();
    LOG.info("Splitting hlog: " + logPath + ", length=" + logLength);
    status.setStatus("Opening log file");
    Reader in = null;
    try {
      in = getReader(fs, logfile, conf, skipErrors);
    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not get reader, corrupted log file " + logPath, e);
      ZKSplitLog.markCorrupted(rootDir, logfile.getPath().getName(), fs);
      isCorrupted = true;
    }
    if (in == null) {
      status.markComplete("Was nothing to split in log file");
      LOG.warn("Nothing to split in log file " + logPath);
      return true;
    }
    this.setDistributedLogSplittingHelper(new DistributedLogSplittingHelper(reporter));
    if (!reportProgressIfIsDistributedLogSplitting()) {
      return false;
    }
    boolean progress_failed = false;
    int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
    int numOpenedFilesLastCheck = 0;
    outputSink.startWriterThreads();
    // Report progress every so many edits and/or files opened (opening a file
    // takes a bit of time).
    Map<byte[], Long> lastFlushedSequenceIds =
      new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    Entry entry;
    int editsCount = 0;
    int editsSkipped = 0;
    try {
      while ((entry = getNextLogLine(in,logPath, skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        Long lastFlushedSequenceId = -1l;
        if (sequenceIdChecker != null) {
          lastFlushedSequenceId = lastFlushedSequenceIds.get(region);
          if (lastFlushedSequenceId == null) {
              lastFlushedSequenceId = sequenceIdChecker.getLastSequenceId(region);
              lastFlushedSequenceIds.put(region, lastFlushedSequenceId);
          }
        }
        if (lastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
          editsSkipped++;
          continue;
        }
        entryBuffers.appendEntry(entry);
        editsCount++;
        // If sufficient edits have passed, check if we should report progress.
        if (editsCount % interval == 0
            || (outputSink.logWriters.size() - numOpenedFilesLastCheck) > numOpenedFilesBeforeReporting) {
          numOpenedFilesLastCheck = outputSink.logWriters.size();
          String countsStr = (editsCount - editsSkipped) +
            " edits, skipped " + editsSkipped + " edits.";
          status.setStatus("Split " + countsStr);
          if (!reportProgressIfIsDistributedLogSplitting()) {
            return false;
          }
        }
      }
    } catch (InterruptedException ie) {
      IOException iie = new InterruptedIOException();
      iie.initCause(ie);
      throw iie;
    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not parse, corrupted log file " + logPath, e);
      ZKSplitLog.markCorrupted(rootDir, logfile.getPath().getName(), fs);
      isCorrupted = true;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      throw e;
    } finally {
      LOG.info("Finishing writing output logs and closing down.");
      progress_failed = outputSink.finishWritingAndClose() == null;
      String msg = "Processed " + editsCount + " edits across "
          + outputSink.getOutputCounts().size() + " regions; log file="
          + logPath + " is corrupted = " + isCorrupted + " progress failed = "
          + progress_failed;
      ;
      LOG.info(msg);
      status.markComplete(msg);
    }
    return !progress_failed;
  }

  /**
   * Completes the work done by splitLogFile by archiving logs
   * <p>
   * It is invoked by SplitLogManager once it knows that one of the
   * SplitLogWorkers have completed the splitLogFile() part. If the master
   * crashes then this function might get called multiple times.
   * <p>
   * @param logfile
   * @param conf
   * @throws IOException
   */
  public static void finishSplitLogFile(String logfile, Configuration conf)
      throws IOException {
    Path rootdir = FSUtils.getRootDir(conf);
    Path oldLogDir = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    finishSplitLogFile(rootdir, oldLogDir, logfile, conf);
  }

  public static void finishSplitLogFile(Path rootdir, Path oldLogDir,
      String logfile, Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    FileSystem fs;
    fs = rootdir.getFileSystem(conf);
    Path logPath = new Path(logfile);
    if (ZKSplitLog.isCorrupted(rootdir, logPath.getName(), fs)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    archiveLogs(null, corruptedLogs, processedLogs, oldLogDir, fs, conf);
    Path stagingDir = ZKSplitLog.getSplitLogDir(rootdir, logPath.getName());
    fs.delete(stagingDir, true);
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
   *
   * @param corruptedLogs
   * @param processedLogs
   * @param oldLogDir
   * @param fs
   * @param conf
   * @throws IOException
   */
  private static void archiveLogs(
      final Path srcDir,
      final List<Path> corruptedLogs,
      final List<Path> processedLogs, final Path oldLogDir,
      final FileSystem fs, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(conf.get(HConstants.HBASE_DIR), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir",  HConstants.CORRUPT_DIR_NAME));

    if (!fs.mkdirs(corruptDir)) {
      LOG.info("Unable to mkdir " + corruptDir);
    }
    fs.mkdirs(oldLogDir);

    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corrupted : corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      if (fs.exists(corrupted)) {
        if (!fs.rename(corrupted, p)) {
          LOG.warn("Unable to move corrupted log " + corrupted + " to " + p);
        } else {
          LOG.warn("Moved corrupted log " + corrupted + " to " + p);
        }
      }
    }

    for (Path p : processedLogs) {
      Path newPath = FSHLog.getHLogArchivePath(oldLogDir, p);
      if (fs.exists(p)) {
        if (!fs.rename(p, newPath)) {
          LOG.warn("Unable to move  " + p + " to " + newPath);
        } else {
          LOG.debug("Archived processed log " + p + " to " + newPath);
        }
      }
    }

    // distributed log splitting removes the srcDir (region's log dir) later
    // when all the log files in that srcDir have been successfully processed
    if (srcDir != null && !fs.delete(srcDir, true)) {
      throw new IOException("Unable to delete src dir: " + srcDir);
    }
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in
   * <code>logEntry</code> named for the sequenceid in the passed
   * <code>logEntry</code>: e.g. /hbase/some_table/2323432434/recovered.edits/2332.
   * This method also ensures existence of RECOVERED_EDITS_DIR under the region
   * creating it if necessary.
   * @param fs
   * @param logEntry
   * @param rootDir HBase root dir.
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  static Path getRegionSplitEditsPath(final FileSystem fs,
      final Entry logEntry, final Path rootDir, boolean isCreate)
  throws IOException {
    Path tableDir = HTableDescriptor.getTableDir(rootDir, logEntry.getKey().getTablename());
    Path regiondir = HRegion.getRegionDir(tableDir,
      Bytes.toString(logEntry.getKey().getEncodedRegionName()));
    Path dir = HLogUtil.getRegionDirRecoveredEditsDir(regiondir);

    if (!fs.exists(regiondir)) {
      LOG.info("This region's directory doesn't exist: "
          + regiondir.toString() + ". It is very likely that it was" +
          " already split so it's safe to discard those edits.");
      return null;
    }
    if (isCreate && !fs.exists(dir)) {
      if (!fs.mkdirs(dir)) LOG.warn("mkdir failed on " + dir);
    }
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName = formatRecoveredEditsFileName(logEntry.getKey().getLogSeqNum());
    fileName = getTmpRecoveredEditsFileName(fileName);
    return new Path(dir, fileName);
  }

  static String getTmpRecoveredEditsFileName(String fileName) {
    return fileName + HLog.RECOVERED_LOG_TMPFILE_SUFFIX;
  }

  /**
   * Get the completed recovered edits file path, renaming it to be by last edit
   * in the file from its first edit. Then we could use the name to skip
   * recovered edits when doing {@link HRegion#replayRecoveredEditsIfAny}.
   * @param srcPath
   * @param maximumEditLogSeqNum
   * @return dstPath take file's last edit log seq num as the name
   */
  static Path getCompletedRecoveredEditsFilePath(Path srcPath,
      Long maximumEditLogSeqNum) {
    String fileName = formatRecoveredEditsFileName(maximumEditLogSeqNum);
    return new Path(srcPath.getParent(), fileName);
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Parse a single hlog and put the edits in entryBuffers
   *
   * @param in the hlog reader
   * @param path the path of the log file
   * @param entryBuffers the buffer to hold the parsed edits
   * @param fs the file system
   * @param conf the configuration
   * @param skipErrors indicator if CorruptedLogFileException should be thrown instead of IOException
   * @throws IOException
   * @throws CorruptedLogFileException if hlog is corrupted
   */
  private void parseHLog(final Reader in, Path path,
		EntryBuffers entryBuffers, final FileSystem fs,
    final Configuration conf, boolean skipErrors)
	throws IOException, CorruptedLogFileException {
    int editsCount = 0;
    try {
      Entry entry;
      while ((entry = getNextLogLine(in, path, skipErrors)) != null) {
        entryBuffers.appendEntry(entry);
        editsCount++;
      }
    } catch (InterruptedException ie) {
      IOException t = new InterruptedIOException();
      t.initCause(ie);
      throw t;
    } finally {
      LOG.debug("Pushed=" + editsCount + " entries from " + path);
    }
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   *
   * @param fs
   * @param file
   * @param conf
   * @return A new Reader instance
   * @throws IOException
   * @throws CorruptedLogFile
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
      FSUtils.getInstance(fs, conf).recoverFileLease(fs, path, conf);
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


  private void writerThreadError(Throwable t) {
    thrown.compareAndSet(null, t);
  }

  /**
   * Check for errors in the writer threads. If any is found, rethrow it.
   */
  private void checkForErrors() throws IOException {
    Throwable thrown = this.thrown.get();
    if (thrown == null) return;
    if (thrown instanceof IOException) {
      throw (IOException)thrown;
    } else {
      throw new RuntimeException(thrown);
    }
  }
  /**
   * Create a new {@link Writer} for writing log splits.
   */
  protected Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
    return HLogFactory.createWriter(fs, logfile, conf);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   */
  protected Reader getReader(FileSystem fs, Path curLogFile, Configuration conf)
      throws IOException {
    return HLogFactory.createReader(fs, curLogFile, conf);
  }

  /**
   * Class which accumulates edits and separates them into a buffer per region
   * while simultaneously accounting RAM usage. Blocks if the RAM usage crosses
   * a predefined threshold.
   *
   * Writer threads then pull region-specific buffers from this class.
   */
  class EntryBuffers {
    Map<byte[], RegionEntryBuffer> buffers =
      new TreeMap<byte[], RegionEntryBuffer>(Bytes.BYTES_COMPARATOR);

    /* Track which regions are currently in the middle of writing. We don't allow
       an IO thread to pick up bytes from a region if we're already writing
       data for that region in a different IO thread. */
    Set<byte[]> currentlyWriting = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    long maxHeapUsage;

    EntryBuffers(long maxHeapUsage) {
      this.maxHeapUsage = maxHeapUsage;
    }

    /**
     * Append a log entry into the corresponding region buffer.
     * Blocks if the total heap usage has crossed the specified threshold.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    void appendEntry(Entry entry) throws InterruptedException, IOException {
      HLogKey key = entry.getKey();

      RegionEntryBuffer buffer;
      long incrHeap;
      synchronized (this) {
        buffer = buffers.get(key.getEncodedRegionName());
        if (buffer == null) {
          buffer = new RegionEntryBuffer(key.getTablename(), key.getEncodedRegionName());
          buffers.put(key.getEncodedRegionName(), buffer);
        }
        incrHeap= buffer.appendEntry(entry);        
      }

      // If we crossed the chunk threshold, wait for more space to be available
      synchronized (dataAvailable) {
        totalBuffered += incrHeap;
        while (totalBuffered > maxHeapUsage && thrown.get() == null) {
          LOG.debug("Used " + totalBuffered + " bytes of buffered edits, waiting for IO threads...");
          dataAvailable.wait(3000);
        }
        dataAvailable.notifyAll();
      }
      checkForErrors();
    }

    synchronized RegionEntryBuffer getChunkToWrite() {
      long biggestSize=0;
      byte[] biggestBufferKey=null;

      for (Map.Entry<byte[], RegionEntryBuffer> entry : buffers.entrySet()) {
        long size = entry.getValue().heapSize();
        if (size > biggestSize && !currentlyWriting.contains(entry.getKey())) {
          biggestSize = size;
          biggestBufferKey = entry.getKey();
        }
      }
      if (biggestBufferKey == null) {
        return null;
      }

      RegionEntryBuffer buffer = buffers.remove(biggestBufferKey);
      currentlyWriting.add(biggestBufferKey);
      return buffer;
    }

    void doneWriting(RegionEntryBuffer buffer) {
      synchronized (this) {
        boolean removed = currentlyWriting.remove(buffer.encodedRegionName);
        assert removed;
      }
      long size = buffer.heapSize();

      synchronized (dataAvailable) {
        totalBuffered -= size;
        // We may unblock writers
        dataAvailable.notifyAll();
      }
    }

    synchronized boolean isRegionCurrentlyWriting(byte[] region) {
      return currentlyWriting.contains(region);
    }
  }

  /**
   * A buffer of some number of edits for a given region.
   * This accumulates edits and also provides a memory optimization in order to
   * share a single byte array instance for the table and region name.
   * Also tracks memory usage of the accumulated edits.
   */
  static class RegionEntryBuffer implements HeapSize {
    long heapInBuffer = 0;
    List<Entry> entryBuffer;
    byte[] tableName;
    byte[] encodedRegionName;

    RegionEntryBuffer(byte[] table, byte[] region) {
      this.tableName = table;
      this.encodedRegionName = region;
      this.entryBuffer = new LinkedList<Entry>();
    }

    long appendEntry(Entry entry) {
      internify(entry);
      entryBuffer.add(entry);
      long incrHeap = entry.getEdit().heapSize() +
        ClassSize.align(2 * ClassSize.REFERENCE) + // HLogKey pointers
        0; // TODO linkedlist entry
      heapInBuffer += incrHeap;
      return incrHeap;
    }

    private void internify(Entry entry) {
      HLogKey k = entry.getKey();
      k.internTableName(this.tableName);
      k.internEncodedRegionName(this.encodedRegionName);
    }

    public long heapSize() {
      return heapInBuffer;
    }
  }


  class WriterThread extends Thread {
    private volatile boolean shouldStop = false;

    WriterThread(int i) {
      super("WriterThread-" + i);
    }

    public void run()  {
      try {
        doRun();
      } catch (Throwable t) {
        LOG.error("Error in log splitting write thread", t);
        writerThreadError(t);
      }
    }

    private void doRun() throws IOException {
      LOG.debug("Writer thread " + this + ": starting");
      while (true) {
        RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          // No data currently available, wait on some more to show up
          synchronized (dataAvailable) {
            if (shouldStop) return;
            try {
              dataAvailable.wait(1000);
            } catch (InterruptedException ie) {
              if (!shouldStop) {
                throw new RuntimeException(ie);
              }
            }
          }
          continue;
        }

        assert buffer != null;
        try {
          writeBuffer(buffer);
        } finally {
          entryBuffers.doneWriting(buffer);
        }
      }
    }


    private void writeBuffer(RegionEntryBuffer buffer) throws IOException {
      List<Entry> entries = buffer.entryBuffer;
      if (entries.isEmpty()) {
        LOG.warn(this.getName() + " got an empty buffer, skipping");
        return;
      }

      WriterAndPath wap = null;

      long startTime = System.nanoTime();
      try {
        int editsCount = 0;

        for (Entry logEntry : entries) {
          if (wap == null) {
            wap = outputSink.getWriterAndPath(logEntry);
            if (wap == null) {
              // getWriterAndPath decided we don't need to write these edits
              // Message was already logged
              return;
            }
          }
          wap.w.append(logEntry);
          outputSink.updateRegionMaximumEditLogSeqNum(logEntry);
          editsCount++;
        }
        // Pass along summary statistics
        wap.incrementEdits(editsCount);
        wap.incrementNanoTime(System.nanoTime() - startTime);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.fatal(this.getName() + " Got while writing log entry to log", e);
        throw e;
      }
    }

    void finish() {
      synchronized (dataAvailable) {
        shouldStop = true;
        dataAvailable.notifyAll();
      }
    }
  }

  private WriterAndPath createWAP(byte[] region, Entry entry, Path rootdir,
      FileSystem fs, Configuration conf)
  throws IOException {
    Path regionedits = getRegionSplitEditsPath(fs, entry, rootdir, true);
    if (regionedits == null) {
      return null;
    }
    if (fs.exists(regionedits)) {
      LOG.warn("Found existing old edits file. It could be the "
          + "result of a previous failed split attempt. Deleting "
          + regionedits + ", length="
          + fs.getFileStatus(regionedits).getLen());
      if (!fs.delete(regionedits, false)) {
        LOG.warn("Failed delete of old " + regionedits);
      }
    }
    Writer w = createWriter(fs, regionedits, conf);
    LOG.debug("Creating writer path=" + regionedits + " region="
        + Bytes.toStringBinary(region));
    return (new WriterAndPath(regionedits, w));
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

  /***
   * @return false if it is a distributed log splitting and it failed to report
   *         progress
   */
  private boolean reportProgressIfIsDistributedLogSplitting() {
    if (this.distributedLogSplittingHelper != null) {
      return distributedLogSplittingHelper.reportProgress();
    } else {
      return true;
    }
  }

  /**
   * A class used in distributed log splitting
   * 
   */
  class DistributedLogSplittingHelper {
    // Report progress, only used in distributed log splitting
    private final CancelableProgressable splitReporter;
    // How often to send a progress report (default 1/2 master timeout)
    private final int report_period;
    private long last_report_at = 0;

    public DistributedLogSplittingHelper(CancelableProgressable reporter) {
      this.splitReporter = reporter;
      report_period = conf.getInt("hbase.splitlog.report.period",
          conf.getInt("hbase.splitlog.manager.timeout",
              SplitLogManager.DEFAULT_TIMEOUT) / 2);
    }

    /***
     * @return false if reporter failed progressing
     */
    private boolean reportProgress() {
      if (splitReporter == null) {
        return true;
      } else {
        long t = EnvironmentEdgeManager.currentTimeMillis();
        if ((t - last_report_at) > report_period) {
          last_report_at = t;
          if (this.splitReporter.progress() == false) {
            LOG.warn("Failed: reporter.progress asked us to terminate");
            return false;
          }
        }
        return true;
      }
    }
  }

  /**
   * Class that manages the output streams from the log splitting process.
   */
  class OutputSink {
    private final Map<byte[], WriterAndPath> logWriters = Collections.synchronizedMap(
          new TreeMap<byte[], WriterAndPath>(Bytes.BYTES_COMPARATOR));
    private final Map<byte[], Long> regionMaximumEditLogSeqNum = Collections
        .synchronizedMap(new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR));
    private final List<WriterThread> writerThreads = Lists.newArrayList();

    /* Set of regions which we've decided should not output edits */
    private final Set<byte[]> blacklistedRegions = Collections.synchronizedSet(
        new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));

    private boolean closeAndCleanCompleted = false;
    
    private boolean logWritersClosed  = false;

    private final int numThreads;

    public OutputSink() {
      // More threads could potentially write faster at the expense
      // of causing more disk seeks as the logs are split.
      // 3. After a certain setting (probably around 3) the
      // process will be bound on the reader in the current
      // implementation anyway.
      numThreads = conf.getInt(
          "hbase.regionserver.hlog.splitlog.writer.threads", 3);
    }

    /**
     * Start the threads that will pump data from the entryBuffers
     * to the output files.
     */
    synchronized void startWriterThreads() {
      for (int i = 0; i < numThreads; i++) {
        WriterThread t = new WriterThread(i);
        t.start();
        writerThreads.add(t);
      }
    }

    /**
     * 
     * @return null if failed to report progress
     * @throws IOException
     */
    List<Path> finishWritingAndClose() throws IOException {
      LOG.info("Waiting for split writer threads to finish");
      boolean progress_failed = false;
      try {
        for (WriterThread t : writerThreads) {
          t.finish();
        }
        for (WriterThread t : writerThreads) {
          if (!progress_failed && !reportProgressIfIsDistributedLogSplitting()) {
            progress_failed = true;
          }
          try {
            t.join();
          } catch (InterruptedException ie) {
            IOException iie = new InterruptedIOException();
            iie.initCause(ie);
            throw iie;
          }
          checkForErrors();
        }
        LOG.info("Split writers finished");
        if (progress_failed) {
          return null;
        }
        return closeStreams();
      } finally {
        List<IOException> thrown = closeLogWriters(null);
        if (thrown != null && !thrown.isEmpty()) {
          throw MultipleIOException.createIOException(thrown);
        }
      }
    }

    /**
     * Close all of the output streams.
     * @return the list of paths written.
     */
    private List<Path> closeStreams() throws IOException {
      Preconditions.checkState(!closeAndCleanCompleted);

      final List<Path> paths = new ArrayList<Path>();
      final List<IOException> thrown = Lists.newArrayList();
      ThreadPoolExecutor closeThreadPool = Threads.getBoundedCachedThreadPool(
          numThreads, 30L, TimeUnit.SECONDS, new ThreadFactory() {
            private int count = 1;
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "split-log-closeStream-" + count++);
              return t;
            }
          });
      CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
          closeThreadPool);
      for (final Map.Entry<byte[], WriterAndPath> logWritersEntry : logWriters
          .entrySet()) {
        completionService.submit(new Callable<Void>() {
          public Void call() throws Exception {
            WriterAndPath wap = logWritersEntry.getValue();
            try {
              wap.w.close();
            } catch (IOException ioe) {
              LOG.error("Couldn't close log at " + wap.p, ioe);
              thrown.add(ioe);
              return null;
            }
            LOG.info("Closed path " + wap.p + " (wrote " + wap.editsWritten
                + " edits in " + (wap.nanosSpent / 1000 / 1000) + "ms)");
            Path dst = getCompletedRecoveredEditsFilePath(wap.p,
                regionMaximumEditLogSeqNum.get(logWritersEntry.getKey()));
            try {
              if (!dst.equals(wap.p) && fs.exists(dst)) {
                LOG.warn("Found existing old edits file. It could be the "
                    + "result of a previous failed split attempt. Deleting "
                    + dst + ", length=" + fs.getFileStatus(dst).getLen());
                if (!fs.delete(dst, false)) {
                  LOG.warn("Failed deleting of old " + dst);
                  throw new IOException("Failed deleting of old " + dst);
                }
              }
              // Skip the unit tests which create a splitter that reads and
              // writes the data without touching disk.
              // TestHLogSplit#testThreading is an example.
              if (fs.exists(wap.p)) {
                if (!fs.rename(wap.p, dst)) {
                  throw new IOException("Failed renaming " + wap.p + " to "
                      + dst);
                }
                LOG.debug("Rename " + wap.p + " to " + dst);
              }
            } catch (IOException ioe) {
              LOG.error("Couldn't rename " + wap.p + " to " + dst, ioe);
              thrown.add(ioe);
              return null;
            }
            paths.add(dst);
            return null;
          }
        });
      }

      boolean progress_failed = false;
      try {
        for (int i = 0, n = logWriters.size(); i < n; i++) {
          Future<Void> future = completionService.take();
          future.get();
          if (!progress_failed && !reportProgressIfIsDistributedLogSplitting()) {
            progress_failed = true;
          }
        }
      } catch (InterruptedException e) {
        IOException iie = new InterruptedIOException();
        iie.initCause(e);
        throw iie;
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      } finally {
        closeThreadPool.shutdownNow();
      }

      if (!thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
      logWritersClosed = true;
      closeAndCleanCompleted = true;
      if (progress_failed) {
        return null;
      }
      return paths;
    }
    
    private List<IOException> closeLogWriters(List<IOException> thrown)
        throws IOException {
      if (!logWritersClosed) {
        if (thrown == null) {
          thrown = Lists.newArrayList();
        }
        try {
          for (WriterThread t : writerThreads) {
            while (t.isAlive()) {
              t.shouldStop = true;
              t.interrupt();
              try {
                t.join(10);
              } catch (InterruptedException e) {
                IOException iie = new InterruptedIOException();
                iie.initCause(e);
                throw iie;
              }
            }
          }
        } finally {
          synchronized (logWriters) {
            for (WriterAndPath wap : logWriters.values()) {
              try {
                wap.w.close();
              } catch (IOException ioe) {
                LOG.error("Couldn't close log at " + wap.p, ioe);
                thrown.add(ioe);
                continue;
              }
              LOG.info("Closed path " + wap.p + " (wrote " + wap.editsWritten
                  + " edits in " + (wap.nanosSpent / 1000 / 1000) + "ms)");
            }
          }
          logWritersClosed = true;
        }
      }
      return thrown;
    }

    /**
     * Get a writer and path for a log starting at the given entry.
     *
     * This function is threadsafe so long as multiple threads are always
     * acting on different regions.
     *
     * @return null if this region shouldn't output any logs
     */
    WriterAndPath getWriterAndPath(Entry entry) throws IOException {
      byte region[] = entry.getKey().getEncodedRegionName();
      WriterAndPath ret = logWriters.get(region);
      if (ret != null) {
        return ret;
      }
      // If we already decided that this region doesn't get any output
      // we don't need to check again.
      if (blacklistedRegions.contains(region)) {
        return null;
      }
      ret = createWAP(region, entry, rootDir, fs, conf);
      if (ret == null) {
        blacklistedRegions.add(region);
        return null;
      }
      logWriters.put(region, ret);
      return ret;
    }

    /**
     * Update region's maximum edit log SeqNum.
     */
    void updateRegionMaximumEditLogSeqNum(Entry entry) {
      synchronized (regionMaximumEditLogSeqNum) {
        Long currentMaxSeqNum=regionMaximumEditLogSeqNum.get(entry.getKey().getEncodedRegionName());
        if (currentMaxSeqNum == null
            || entry.getKey().getLogSeqNum() > currentMaxSeqNum) {
          regionMaximumEditLogSeqNum.put(entry.getKey().getEncodedRegionName(),
              entry.getKey().getLogSeqNum());
        }
      }

    }

    Long getRegionMaximumEditLogSeqNum(byte[] region) {
      return regionMaximumEditLogSeqNum.get(region);
    }

    /**
     * @return a map from encoded region ID to the number of edits written out
     * for that region.
     */
    private Map<byte[], Long> getOutputCounts() {
      TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(
          Bytes.BYTES_COMPARATOR);
      synchronized (logWriters) {
        for (Map.Entry<byte[], WriterAndPath> entry : logWriters.entrySet()) {
          ret.put(entry.getKey(), entry.getValue().editsWritten);
        }
      }
      return ret;
    }
  }



  /**
   *  Private data structure that wraps a Writer and its Path,
   *  also collecting statistics about the data written to this
   *  output.
   */
  private final static class WriterAndPath {
    final Path p;
    final Writer w;

    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    WriterAndPath(final Path p, final Writer w) {
      this.p = p;
      this.w = w;
    }

    void incrementEdits(int edits) {
      editsWritten += edits;
    }

    void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }
  }

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;
    CorruptedLogFileException(String s) {
      super(s);
    }
  }
}
