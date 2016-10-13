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
package org.apache.hadoop.hbase.wal;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALEditsReplaySink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
@InterfaceAudience.Private
public class WALSplitter {
  private static final Log LOG = LogFactory.getLog(WALSplitter.class);

  /** By default we retry errors in splitting, rather than skipping. */
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;

  // Parameters for split process
  protected final Path rootDir;
  protected final FileSystem fs;
  protected final Configuration conf;

  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  PipelineController controller;
  OutputSink outputSink;
  EntryBuffers entryBuffers;

  private Set<TableName> disablingOrDisabledTables =
      new HashSet<TableName>();
  private BaseCoordinatedStateManager csm;
  private final WALFactory walFactory;

  private MonitoredTask status;

  // For checking the latest flushed sequence id
  protected final LastSequenceId sequenceIdChecker;

  protected boolean distributedLogReplay;

  // Map encodedRegionName -> lastFlushedSequenceId
  protected Map<String, Long> lastFlushedSequenceIds = new ConcurrentHashMap<String, Long>();

  // Map encodedRegionName -> maxSeqIdInStores
  protected Map<String, Map<byte[], Long>> regionMaxSeqIdInStores =
      new ConcurrentHashMap<String, Map<byte[], Long>>();

  // Failed region server that the wal file being split belongs to
  protected String failedServerName = "";

  // Number of writer threads
  private final int numWriterThreads;

  // Min batch size when replay WAL edits
  private final int minBatchSize;

  // the file being split currently
  private FileStatus fileBeingSplit;

  @VisibleForTesting
  WALSplitter(final WALFactory factory, Configuration conf, Path rootDir,
      FileSystem fs, LastSequenceId idChecker,
      CoordinatedStateManager csm, RecoveryMode mode) {
    this.conf = HBaseConfiguration.create(conf);
    String codecClassName = conf
        .get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, codecClassName);
    this.rootDir = rootDir;
    this.fs = fs;
    this.sequenceIdChecker = idChecker;
    this.csm = (BaseCoordinatedStateManager)csm;
    this.walFactory = factory;
    this.controller = new PipelineController();

    entryBuffers = new EntryBuffers(controller,
        this.conf.getInt("hbase.regionserver.hlog.splitlog.buffersize",
            128*1024*1024));

    // a larger minBatchSize may slow down recovery because replay writer has to wait for
    // enough edits before replaying them
    this.minBatchSize = this.conf.getInt("hbase.regionserver.wal.logreplay.batch.size", 64);
    this.distributedLogReplay = (RecoveryMode.LOG_REPLAY == mode);

    this.numWriterThreads = this.conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    if (csm != null && this.distributedLogReplay) {
      outputSink = new LogReplayOutputSink(controller, entryBuffers, numWriterThreads);
    } else {
      if (this.distributedLogReplay) {
        LOG.info("ZooKeeperWatcher is passed in as NULL so disable distrubitedLogRepaly.");
      }
      this.distributedLogReplay = false;
      outputSink = new LogRecoveredEditsOutputSink(controller, entryBuffers, numWriterThreads);
    }

  }

  /**
   * Splits a WAL file into region's recovered-edits directory.
   * This is the main entry point for distributed log splitting from SplitLogWorker.
   * <p>
   * If the log file has N regions then N recovered.edits files will be produced.
   * <p>
   * @param rootDir
   * @param logfile
   * @param fs
   * @param conf
   * @param reporter
   * @param idChecker
   * @param cp coordination state manager
   * @return false if it is interrupted by the progress-able.
   * @throws IOException
   */
  public static boolean splitLogFile(Path rootDir, FileStatus logfile, FileSystem fs,
      Configuration conf, CancelableProgressable reporter, LastSequenceId idChecker,
      CoordinatedStateManager cp, RecoveryMode mode, final WALFactory factory) throws IOException {
    WALSplitter s = new WALSplitter(factory, conf, rootDir, fs, idChecker, cp, mode);
    return s.splitLogFile(logfile, reporter);
  }

  // A wrapper to split one log folder using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because UpgradeTo96 and TestWALObserver are in different packages,
  // which uses this method to do log splitting.
  public static List<Path> split(Path rootDir, Path logDir, Path oldLogDir,
      FileSystem fs, Configuration conf, final WALFactory factory) throws IOException {
    final FileStatus[] logfiles = SplitLogManager.getFileList(conf,
        Collections.singletonList(logDir), null);
    List<Path> splits = new ArrayList<Path>();
    if (logfiles != null && logfiles.length > 0) {
      for (FileStatus logfile: logfiles) {
        WALSplitter s = new WALSplitter(factory, conf, rootDir, fs, null, null,
            RecoveryMode.LOG_SPLITTING);
        if (s.splitLogFile(logfile, null)) {
          finishSplitLogFile(rootDir, oldLogDir, logfile.getPath(), conf);
          if (s.outputSink.splits != null) {
            splits.addAll(s.outputSink.splits);
          }
        }
      }
    }
    if (!fs.delete(logDir, true)) {
      throw new IOException("Unable to delete src dir: " + logDir);
    }
    return splits;
  }

  /**
   * log splitting implementation, splits one log file.
   * @param logfile should be an actual log file.
   */
  @VisibleForTesting
  boolean splitLogFile(FileStatus logfile, CancelableProgressable reporter) throws IOException {
    Preconditions.checkState(status == null);
    Preconditions.checkArgument(logfile.isFile(),
        "passed in file status is for something other than a regular file.");
    boolean isCorrupted = false;
    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors",
      SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    Path logPath = logfile.getPath();
    boolean outputSinkStarted = false;
    boolean progress_failed = false;
    int editsCount = 0;
    int editsSkipped = 0;

    status =
        TaskMonitor.get().createStatus(
          "Splitting log file " + logfile.getPath() + "into a temporary staging area.");
    Reader in = null;
    this.fileBeingSplit = logfile;
    try {
      long logLength = logfile.getLen();
      LOG.info("Splitting wal: " + logPath + ", length=" + logLength);
      LOG.info("DistributedLogReplay = " + this.distributedLogReplay);
      status.setStatus("Opening log file");
      if (reporter != null && !reporter.progress()) {
        progress_failed = true;
        return false;
      }
      try {
        in = getReader(logfile, skipErrors, reporter);
      } catch (CorruptedLogFileException e) {
        LOG.warn("Could not get reader, corrupted log file " + logPath, e);
        ZKSplitLog.markCorrupted(rootDir, logfile.getPath().getName(), fs);
        isCorrupted = true;
      }
      if (in == null) {
        LOG.warn("Nothing to split in log file " + logPath);
        return true;
      }
      if (csm != null) {
        try {
          TableStateManager tsm = csm.getTableStateManager();
          disablingOrDisabledTables = tsm.getTablesInStates(
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING);
        } catch (CoordinatedStateException e) {
          throw new IOException("Can't get disabling/disabled tables", e);
        }
      }
      int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
      int numOpenedFilesLastCheck = 0;
      outputSink.setReporter(reporter);
      outputSink.startWriterThreads();
      outputSinkStarted = true;
      Entry entry;
      Long lastFlushedSequenceId = -1L;
      ServerName serverName = DefaultWALProvider.getServerNameFromWALDirectoryName(logPath);
      failedServerName = (serverName == null) ? "" : serverName.getServerName();
      while ((entry = getNextLogLine(in, logPath, skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (this.distributedLogReplay) {
            RegionStoreSequenceIds ids =
                csm.getSplitLogWorkerCoordination().getRegionFlushedSequenceId(failedServerName,
                  encodedRegionNameAsStr);
            if (ids != null) {
              lastFlushedSequenceId = ids.getLastFlushedSequenceId();
              if (LOG.isDebugEnabled()) {
                LOG.debug("DLR Last flushed sequenceid for " + encodedRegionNameAsStr + ": " +
                  TextFormat.shortDebugString(ids));
              }
            }
          } else if (sequenceIdChecker != null) {
            RegionStoreSequenceIds ids = sequenceIdChecker.getLastSequenceId(region);
            Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
            for (StoreSequenceId storeSeqId : ids.getStoreSequenceIdList()) {
              maxSeqIdInStores.put(storeSeqId.getFamilyName().toByteArray(),
                storeSeqId.getSequenceId());
            }
            regionMaxSeqIdInStores.put(encodedRegionNameAsStr, maxSeqIdInStores);
            lastFlushedSequenceId = ids.getLastFlushedSequenceId();
            if (LOG.isDebugEnabled()) {
              LOG.debug("DLS Last flushed sequenceid for " + encodedRegionNameAsStr + ": " +
                  TextFormat.shortDebugString(ids));
            }
          }
          if (lastFlushedSequenceId == null) {
            lastFlushedSequenceId = -1L;
          }
          lastFlushedSequenceIds.put(encodedRegionNameAsStr, lastFlushedSequenceId);
        }
        if (lastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
          editsSkipped++;
          continue;
        }
        // Don't send Compaction/Close/Open region events to recovered edit type sinks.
        if (entry.getEdit().isMetaEdit() && !outputSink.keepRegionEvent(entry)) {
          editsSkipped++;
          continue;
        }
        entryBuffers.appendEntry(entry);
        editsCount++;
        int moreWritersFromLastCheck = this.getNumOpenWriters() - numOpenedFilesLastCheck;
        // If sufficient edits have passed, check if we should report progress.
        if (editsCount % interval == 0
            || moreWritersFromLastCheck > numOpenedFilesBeforeReporting) {
          numOpenedFilesLastCheck = this.getNumOpenWriters();
          String countsStr = (editsCount - (editsSkipped + outputSink.getSkippedEdits()))
              + " edits, skipped " + editsSkipped + " edits.";
          status.setStatus("Split " + countsStr);
          if (reporter != null && !reporter.progress()) {
            progress_failed = true;
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
      csm.getSplitLogWorkerCoordination().markCorrupted(rootDir,
        logfile.getPath().getName(), fs);
      isCorrupted = true;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      throw e;
    } finally {
      LOG.debug("Finishing writing output logs and closing down.");
      try {
        if (null != in) {
          in.close();
        }
      } catch (IOException exception) {
        LOG.warn("Could not close wal reader: " + exception.getMessage());
        LOG.debug("exception details", exception);
      }
      try {
        if (outputSinkStarted) {
          // Set progress_failed to true as the immediate following statement will reset its value
          // when finishWritingAndClose() throws exception, progress_failed has the right value
          progress_failed = true;
          progress_failed = outputSink.finishWritingAndClose() == null;
        }
      } finally {
        String msg =
            "Processed " + editsCount + " edits across " + outputSink.getNumberOfRecoveredRegions()
                + " regions; edits skipped=" + editsSkipped + "; log file=" + logPath +
                ", length=" + logfile.getLen() + // See if length got updated post lease recovery
                ", corrupted=" + isCorrupted + ", progress failed=" + progress_failed;
        LOG.info(msg);
        status.markComplete(msg);
      }
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
  public static void finishSplitLogFile(String logfile,
      Configuration conf)  throws IOException {
    Path rootdir = FSUtils.getRootDir(conf);
    Path oldLogDir = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logPath;
    if (FSUtils.isStartingWithPath(rootdir, logfile)) {
      logPath = new Path(logfile);
    } else {
      logPath = new Path(rootdir, logfile);
    }
    finishSplitLogFile(rootdir, oldLogDir, logPath, conf);
  }

  private static void finishSplitLogFile(Path rootdir, Path oldLogDir,
      Path logPath, Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    FileSystem fs;
    fs = rootdir.getFileSystem(conf);
    if (ZKSplitLog.isCorrupted(rootdir, logPath.getName(), fs)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    archiveLogs(corruptedLogs, processedLogs, oldLogDir, fs, conf);
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
      final List<Path> corruptedLogs,
      final List<Path> processedLogs, final Path oldLogDir,
      final FileSystem fs, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(FSUtils.getRootDir(conf), conf.get(
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
      Path newPath = FSHLog.getWALArchivePath(oldLogDir, p);
      if (fs.exists(p)) {
        if (!FSUtils.renameAndSetModifyTime(fs, p, newPath)) {
          LOG.warn("Unable to move  " + p + " to " + newPath);
        } else {
          LOG.info("Archived processed log " + p + " to " + newPath);
        }
      }
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
   * @param fileBeingSplit the file being split currently. Used to generate tmp file name.
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static Path getRegionSplitEditsPath(final FileSystem fs,
      final Entry logEntry, final Path rootDir, String fileBeingSplit)
  throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, logEntry.getKey().getTablename());
    String encodedRegionName = Bytes.toString(logEntry.getKey().getEncodedRegionName());
    Path regiondir = HRegion.getRegionDir(tableDir, encodedRegionName);
    Path dir = getRegionDirRecoveredEditsDir(regiondir);

    if (!fs.exists(regiondir)) {
      LOG.info("This region's directory doesn't exist: "
          + regiondir.toString() + ". It is very likely that it was" +
          " already split so it's safe to discard those edits.");
      return null;
    }
    if (fs.exists(dir) && fs.isFile(dir)) {
      Path tmp = new Path("/tmp");
      if (!fs.exists(tmp)) {
        fs.mkdirs(tmp);
      }
      tmp = new Path(tmp,
        HConstants.RECOVERED_EDITS_DIR + "_" + encodedRegionName);
      LOG.warn("Found existing old file: " + dir + ". It could be some "
        + "leftover of an old installation. It should be a folder instead. "
        + "So moving it to " + tmp);
      if (!fs.rename(dir, tmp)) {
        LOG.warn("Failed to sideline old file " + dir);
      }
    }

    if (!fs.exists(dir) && !fs.mkdirs(dir)) {
      LOG.warn("mkdir failed on " + dir);
    }
    // Append fileBeingSplit to prevent name conflict since we may have duplicate wal entries now.
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName = formatRecoveredEditsFileName(logEntry.getKey().getLogSeqNum());
    fileName = getTmpRecoveredEditsFileName(fileName + "-" + fileBeingSplit);
    return new Path(dir, fileName);
  }

  private static String getTmpRecoveredEditsFileName(String fileName) {
    return fileName + RECOVERED_LOG_TMPFILE_SUFFIX;
  }

  /**
   * Get the completed recovered edits file path, renaming it to be by last edit
   * in the file from its first edit. Then we could use the name to skip
   * recovered edits when doing {@link HRegion#replayRecoveredEditsIfAny}.
   * @param srcPath
   * @param maximumEditLogSeqNum
   * @return dstPath take file's last edit log seq num as the name
   */
  private static Path getCompletedRecoveredEditsFilePath(Path srcPath,
      long maximumEditLogSeqNum) {
    String fileName = formatRecoveredEditsFileName(maximumEditLogSeqNum);
    return new Path(srcPath.getParent(), fileName);
  }

  @VisibleForTesting
  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  private static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  private static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";

  /**
   * @param regiondir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Returns sorted set of edit files made by splitter, excluding files
   * with '.temp' suffix.
   *
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir))
      return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splitwal thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
          // Skip SeqId Files
          if (isSequenceIdFile(p)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null) {
      return filesSorted;
    }
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   *
   * @param fs
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  private static final String SEQUENCE_ID_FILE_SUFFIX = ".seqid";
  private static final String OLD_SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  private static final int SEQUENCE_ID_FILE_SUFFIX_LENGTH = SEQUENCE_ID_FILE_SUFFIX.length();

  /**
   * Is the given file a region open sequence id file.
   */
  @VisibleForTesting
  public static boolean isSequenceIdFile(final Path file) {
    return file.getName().endsWith(SEQUENCE_ID_FILE_SUFFIX)
        || file.getName().endsWith(OLD_SEQUENCE_ID_FILE_SUFFIX);
  }

  /**
   * Create a file with name as region open sequence id
   * @param fs
   * @param regiondir
   * @param newSeqId
   * @param saftyBumper
   * @return long new sequence Id value
   * @throws IOException
   */
  public static long writeRegionSequenceIdFile(final FileSystem fs, final Path regiondir,
      long newSeqId, long saftyBumper) throws IOException {

    Path editsdir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);
    long maxSeqId = 0;
    FileStatus[] files = null;
    if (fs.exists(editsdir)) {
      files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return isSequenceIdFile(p);
        }
      });
      if (files != null) {
        for (FileStatus status : files) {
          String fileName = status.getPath().getName();
          try {
            Long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
                - SEQUENCE_ID_FILE_SUFFIX_LENGTH));
            maxSeqId = Math.max(tmpSeqId, maxSeqId);
          } catch (NumberFormatException ex) {
            LOG.warn("Invalid SeqId File Name=" + fileName);
          }
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId

    // write a new seqId file
    Path newSeqIdFile = new Path(editsdir, newSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newSeqId != maxSeqId) {
      try {
        if (!fs.createNewFile(newSeqIdFile) && !fs.exists(newSeqIdFile)) {
          throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrote region seqId=" + newSeqIdFile + " to file, newSeqId=" + newSeqId
              + ", maxSeqId=" + maxSeqId);
        }
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    if (files != null) {
      for (FileStatus status : files) {
        if (newSeqIdFile.equals(status.getPath())) {
          continue;
        }
        fs.delete(status.getPath(), false);
      }
    }
    return newSeqId;
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   *
   * @param file
   * @return A new Reader instance, caller should close
   * @throws IOException
   * @throws CorruptedLogFileException
   */
  protected Reader getReader(FileStatus file, boolean skipErrors, CancelableProgressable reporter)
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
      FSUtils.getInstance(fs, conf).recoverFileLease(fs, path, conf, reporter);
      try {
        in = getReader(path, reporter);
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
      if (e instanceof FileNotFoundException) {
        // A wal file may not exist anymore. Nothing can be recovered so move on
        LOG.warn("File " + path + " doesn't exist anymore.", e);
        return null;
      }
      if (!skipErrors || e instanceof InterruptedIOException) {
        throw e; // Don't mark the file corrupted if interrupted, or not skipErrors
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Could not open wal " +
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
      LOG.info("EOF from wal " + path + ".  continuing");
      return null;
    } catch (IOException e) {
      // If the IOE resulted from bad file format,
      // then this problem is idempotent and retrying won't help
      if (e.getCause() != null &&
          (e.getCause() instanceof ParseException ||
           e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception " + e.getCause().toString() + " from wal "
           + path + ".  continuing");
        return null;
      }
      if (!skipErrors) {
        throw e;
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Ignoring exception" +
            " while parsing wal " + path + ". Marking as corrupted");
      t.initCause(e);
      throw t;
    }
  }

  /**
   * Create a new {@link Writer} for writing log splits.
   * @return a new Writer instance, caller should close
   */
  protected Writer createWriter(Path logfile)
      throws IOException {
    return walFactory.createRecoveredEditsWriter(fs, logfile);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  protected Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
    return walFactory.createReader(fs, curLogFile, reporter);
  }

  /**
   * Get current open writers
   */
  private int getNumOpenWriters() {
    int result = 0;
    if (this.outputSink != null) {
      result += this.outputSink.getNumOpenWriters();
    }
    return result;
  }

  /**
   * Contains some methods to control WAL-entries producer / consumer interactions
   */
  public static class PipelineController {
    // If an exception is thrown by one of the other threads, it will be
    // stored here.
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

    // Wait/notify for when data has been produced by the writer thread,
    // consumed by the reader thread, or an exception occurred
    public final Object dataAvailable = new Object();

    void writerThreadError(Throwable t) {
      thrown.compareAndSet(null, t);
    }

    /**
     * Check for errors in the writer threads. If any is found, rethrow it.
     */
    void checkForErrors() throws IOException {
      Throwable thrown = this.thrown.get();
      if (thrown == null) return;
      if (thrown instanceof IOException) {
        throw new IOException(thrown);
      } else {
        throw new RuntimeException(thrown);
      }
    }
  }

  /**
   * Class which accumulates edits and separates them into a buffer per region
   * while simultaneously accounting RAM usage. Blocks if the RAM usage crosses
   * a predefined threshold.
   *
   * Writer threads then pull region-specific buffers from this class.
   */
  public static class EntryBuffers {
    PipelineController controller;

    Map<byte[], RegionEntryBuffer> buffers =
      new TreeMap<byte[], RegionEntryBuffer>(Bytes.BYTES_COMPARATOR);

    /* Track which regions are currently in the middle of writing. We don't allow
       an IO thread to pick up bytes from a region if we're already writing
       data for that region in a different IO thread. */
    Set<byte[]> currentlyWriting = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    long maxHeapUsage;

    public EntryBuffers(PipelineController controller, long maxHeapUsage) {
      this.controller = controller;
      this.maxHeapUsage = maxHeapUsage;
    }

    /**
     * Append a log entry into the corresponding region buffer.
     * Blocks if the total heap usage has crossed the specified threshold.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public void appendEntry(Entry entry) throws InterruptedException, IOException {
      WALKey key = entry.getKey();

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
      synchronized (controller.dataAvailable) {
        totalBuffered += incrHeap;
        while (totalBuffered > maxHeapUsage && controller.thrown.get() == null) {
          LOG.debug("Used " + totalBuffered + " bytes of buffered edits, waiting for IO threads...");
          controller.dataAvailable.wait(2000);
        }
        controller.dataAvailable.notifyAll();
      }
      controller.checkForErrors();
    }

    /**
     * @return RegionEntryBuffer a buffer of edits to be written or replayed.
     */
    synchronized RegionEntryBuffer getChunkToWrite() {
      long biggestSize = 0;
      byte[] biggestBufferKey = null;

      for (Map.Entry<byte[], RegionEntryBuffer> entry : buffers.entrySet()) {
        long size = entry.getValue().heapSize();
        if (size > biggestSize && (!currentlyWriting.contains(entry.getKey()))) {
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

      synchronized (controller.dataAvailable) {
        totalBuffered -= size;
        // We may unblock writers
        controller.dataAvailable.notifyAll();
      }
    }

    synchronized boolean isRegionCurrentlyWriting(byte[] region) {
      return currentlyWriting.contains(region);
    }

    public void waitUntilDrained() {
      synchronized (controller.dataAvailable) {
        while (totalBuffered > 0) {
          try {
            controller.dataAvailable.wait(2000);
          } catch (InterruptedException e) {
            LOG.warn("Got intrerrupted while waiting for EntryBuffers is drained");
            Thread.interrupted();
            break;
          }
        }
      }
    }
  }

  /**
   * A buffer of some number of edits for a given region.
   * This accumulates edits and also provides a memory optimization in order to
   * share a single byte array instance for the table and region name.
   * Also tracks memory usage of the accumulated edits.
   */
  public static class RegionEntryBuffer implements HeapSize {
    long heapInBuffer = 0;
    List<Entry> entryBuffer;
    TableName tableName;
    byte[] encodedRegionName;

    RegionEntryBuffer(TableName tableName, byte[] region) {
      this.tableName = tableName;
      this.encodedRegionName = region;
      this.entryBuffer = new LinkedList<Entry>();
    }

    long appendEntry(Entry entry) {
      internify(entry);
      entryBuffer.add(entry);
      long incrHeap = entry.getEdit().heapSize() +
        ClassSize.align(2 * ClassSize.REFERENCE) + // WALKey pointers
        0; // TODO linkedlist entry
      heapInBuffer += incrHeap;
      return incrHeap;
    }

    private void internify(Entry entry) {
      WALKey k = entry.getKey();
      k.internTableName(this.tableName);
      k.internEncodedRegionName(this.encodedRegionName);
    }

    @Override
    public long heapSize() {
      return heapInBuffer;
    }

    public byte[] getEncodedRegionName() {
      return encodedRegionName;
    }

    public List<Entry> getEntryBuffer() {
      return entryBuffer;
    }

    public TableName getTableName() {
      return tableName;
    }
  }

  public static class WriterThread extends Thread {
    private volatile boolean shouldStop = false;
    private PipelineController controller;
    private EntryBuffers entryBuffers;
    private OutputSink outputSink = null;

    WriterThread(PipelineController controller, EntryBuffers entryBuffers, OutputSink sink, int i){
      super(Thread.currentThread().getName() + "-Writer-" + i);
      this.controller = controller;
      this.entryBuffers = entryBuffers;
      outputSink = sink;
    }

    @Override
    public void run()  {
      try {
        doRun();
      } catch (Throwable t) {
        LOG.error("Exiting thread", t);
        controller.writerThreadError(t);
      }
    }

    private void doRun() throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Writer thread starting");
      while (true) {
        RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          // No data currently available, wait on some more to show up
          synchronized (controller.dataAvailable) {
            if (shouldStop && !this.outputSink.flush()) {
              return;
            }
            try {
              controller.dataAvailable.wait(500);
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
      outputSink.append(buffer);
    }

    void finish() {
      synchronized (controller.dataAvailable) {
        shouldStop = true;
        controller.dataAvailable.notifyAll();
      }
    }
  }

  /**
   * The following class is an abstraction class to provide a common interface to support both
   * existing recovered edits file sink and region server WAL edits replay sink
   */
  public static abstract class OutputSink {

    protected PipelineController controller;
    protected EntryBuffers entryBuffers;

    protected Map<byte[], SinkWriter> writers = Collections
        .synchronizedMap(new TreeMap<byte[], SinkWriter>(Bytes.BYTES_COMPARATOR));;

    protected final Map<byte[], Long> regionMaximumEditLogSeqNum = Collections
        .synchronizedMap(new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR));

    protected final List<WriterThread> writerThreads = Lists.newArrayList();

    /* Set of regions which we've decided should not output edits */
    protected final Set<byte[]> blacklistedRegions = Collections
        .synchronizedSet(new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));

    protected boolean closeAndCleanCompleted = false;

    protected boolean writersClosed = false;

    protected final int numThreads;

    protected CancelableProgressable reporter = null;

    protected AtomicLong skippedEdits = new AtomicLong();

    protected List<Path> splits = null;

    public OutputSink(PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
      numThreads = numWriters;
      this.controller = controller;
      this.entryBuffers = entryBuffers;
    }

    void setReporter(CancelableProgressable reporter) {
      this.reporter = reporter;
    }

    /**
     * Start the threads that will pump data from the entryBuffers to the output files.
     */
    public synchronized void startWriterThreads() {
      for (int i = 0; i < numThreads; i++) {
        WriterThread t = new WriterThread(controller, entryBuffers, this, i);
        t.start();
        writerThreads.add(t);
      }
    }

    /**
     *
     * Update region's maximum edit log SeqNum.
     */
    void updateRegionMaximumEditLogSeqNum(Entry entry) {
      synchronized (regionMaximumEditLogSeqNum) {
        Long currentMaxSeqNum = regionMaximumEditLogSeqNum.get(entry.getKey()
            .getEncodedRegionName());
        if (currentMaxSeqNum == null || entry.getKey().getLogSeqNum() > currentMaxSeqNum) {
          regionMaximumEditLogSeqNum.put(entry.getKey().getEncodedRegionName(), entry.getKey()
              .getLogSeqNum());
        }
      }
    }

    Long getRegionMaximumEditLogSeqNum(byte[] region) {
      return regionMaximumEditLogSeqNum.get(region);
    }

    /**
     * @return the number of currently opened writers
     */
    int getNumOpenWriters() {
      return this.writers.size();
    }

    long getSkippedEdits() {
      return this.skippedEdits.get();
    }

    /**
     * Wait for writer threads to dump all info to the sink
     * @return true when there is no error
     * @throws IOException
     */
    protected boolean finishWriting(boolean interrupt) throws IOException {
      LOG.debug("Waiting for split writer threads to finish");
      boolean progress_failed = false;
      for (WriterThread t : writerThreads) {
        t.finish();
      }
      if (interrupt) {
        for (WriterThread t : writerThreads) {
          t.interrupt(); // interrupt the writer threads. We are stopping now.
        }
      }

      for (WriterThread t : writerThreads) {
        if (!progress_failed && reporter != null && !reporter.progress()) {
          progress_failed = true;
        }
        try {
          t.join();
        } catch (InterruptedException ie) {
          IOException iie = new InterruptedIOException();
          iie.initCause(ie);
          throw iie;
        }
      }
      controller.checkForErrors();
      LOG.info(this.writerThreads.size() + " split writers finished; closing...");
      return (!progress_failed);
    }

    public abstract List<Path> finishWritingAndClose() throws IOException;

    /**
     * @return a map from encoded region ID to the number of edits written out for that region.
     */
    public abstract Map<byte[], Long> getOutputCounts();

    /**
     * @return number of regions we've recovered
     */
    public abstract int getNumberOfRecoveredRegions();

    /**
     * @param buffer A WAL Edit Entry
     * @throws IOException
     */
    public abstract void append(RegionEntryBuffer buffer) throws IOException;

    /**
     * WriterThread call this function to help flush internal remaining edits in buffer before close
     * @return true when underlying sink has something to flush
     */
    public boolean flush() throws IOException {
      return false;
    }

    /**
     * Some WALEdit's contain only KV's for account on what happened to a region.
     * Not all sinks will want to get all of those edits.
     *
     * @return Return true if this sink wants to accept this region-level WALEdit.
     */
    public abstract boolean keepRegionEvent(Entry entry);
  }

  /**
   * Class that manages the output streams from the log splitting process.
   */
  class LogRecoveredEditsOutputSink extends OutputSink {

    public LogRecoveredEditsOutputSink(PipelineController controller, EntryBuffers entryBuffers,
        int numWriters) {
      // More threads could potentially write faster at the expense
      // of causing more disk seeks as the logs are split.
      // 3. After a certain setting (probably around 3) the
      // process will be bound on the reader in the current
      // implementation anyway.
      super(controller, entryBuffers, numWriters);
    }

    /**
     * @return null if failed to report progress
     * @throws IOException
     */
    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      boolean isSuccessful = false;
      List<Path> result = null;
      try {
        isSuccessful = finishWriting(false);
      } finally {
        result = close();
        List<IOException> thrown = closeLogWriters(null);
        if (thrown != null && !thrown.isEmpty()) {
          throw MultipleIOException.createIOException(thrown);
        }
      }
      if (isSuccessful) {
        splits = result;
      }
      return splits;
    }

    // delete the one with fewer wal entries
    private void deleteOneWithFewerEntries(WriterAndPath wap, Path dst) throws IOException {
      long dstMinLogSeqNum = -1L;
      try (WAL.Reader reader = walFactory.createReader(fs, dst)) {
        WAL.Entry entry = reader.next();
        if (entry != null) {
          dstMinLogSeqNum = entry.getKey().getLogSeqNum();
        }
      } catch (EOFException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Got EOF when reading first WAL entry from " + dst + ", an empty or broken WAL file?",
            e);
        }
      }
      if (wap.minLogSeqNum < dstMinLogSeqNum) {
        LOG.warn("Found existing old edits file. It could be the result of a previous failed"
            + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
            + fs.getFileStatus(dst).getLen());
        if (!fs.delete(dst, false)) {
          LOG.warn("Failed deleting of old " + dst);
          throw new IOException("Failed deleting of old " + dst);
        }
      } else {
        LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.p
            + ", length=" + fs.getFileStatus(wap.p).getLen());
        if (!fs.delete(wap.p, false)) {
          LOG.warn("Failed deleting of " + wap.p);
          throw new IOException("Failed deleting of " + wap.p);
        }
      }
    }

    /**
     * Close all of the output streams.
     * @return the list of paths written.
     */
    private List<Path> close() throws IOException {
      Preconditions.checkState(!closeAndCleanCompleted);

      final List<Path> paths = new ArrayList<Path>();
      final List<IOException> thrown = Lists.newArrayList();
      ThreadPoolExecutor closeThreadPool = Threads.getBoundedCachedThreadPool(numThreads, 30L,
        TimeUnit.SECONDS, new ThreadFactory() {
          private int count = 1;

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "split-log-closeStream-" + count++);
            return t;
          }
        });
      CompletionService<Void> completionService =
        new ExecutorCompletionService<Void>(closeThreadPool);
      for (final Map.Entry<byte[], SinkWriter> writersEntry : writers.entrySet()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Submitting close of " + ((WriterAndPath)writersEntry.getValue()).p);
        }
        completionService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            WriterAndPath wap = (WriterAndPath) writersEntry.getValue();
            if (LOG.isTraceEnabled()) LOG.trace("Closing " + wap.p);
            try {
              wap.w.close();
            } catch (IOException ioe) {
              LOG.error("Couldn't close log at " + wap.p, ioe);
              thrown.add(ioe);
              return null;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Closed wap " + wap.p + " (wrote " + wap.editsWritten
                + " edits, skipped " + wap.editsSkipped + " edits in "
                + (wap.nanosSpent / 1000 / 1000) + "ms");
            }
            if (wap.editsWritten == 0) {
              // just remove the empty recovered.edits file
              if (fs.exists(wap.p) && !fs.delete(wap.p, false)) {
                LOG.warn("Failed deleting empty " + wap.p);
                throw new IOException("Failed deleting empty  " + wap.p);
              }
              return null;
            }

            Path dst = getCompletedRecoveredEditsFilePath(wap.p,
              regionMaximumEditLogSeqNum.get(writersEntry.getKey()));
            try {
              if (!dst.equals(wap.p) && fs.exists(dst)) {
                deleteOneWithFewerEntries(wap, dst);
              }
              // Skip the unit tests which create a splitter that reads and
              // writes the data without touching disk.
              // TestHLogSplit#testThreading is an example.
              if (fs.exists(wap.p)) {
                if (!fs.rename(wap.p, dst)) {
                  throw new IOException("Failed renaming " + wap.p + " to " + dst);
                }
                LOG.info("Rename " + wap.p + " to " + dst);
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
        for (int i = 0, n = this.writers.size(); i < n; i++) {
          Future<Void> future = completionService.take();
          future.get();
          if (!progress_failed && reporter != null && !reporter.progress()) {
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
      writersClosed = true;
      closeAndCleanCompleted = true;
      if (progress_failed) {
        return null;
      }
      return paths;
    }

    private List<IOException> closeLogWriters(List<IOException> thrown) throws IOException {
      if (writersClosed) {
        return thrown;
      }

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
        synchronized (writers) {
          WriterAndPath wap = null;
          for (SinkWriter tmpWAP : writers.values()) {
            try {
              wap = (WriterAndPath) tmpWAP;
              wap.w.close();
            } catch (IOException ioe) {
              LOG.error("Couldn't close log at " + wap.p, ioe);
              thrown.add(ioe);
              continue;
            }
            LOG.info("Closed log " + wap.p + " (wrote " + wap.editsWritten + " edits in "
                + (wap.nanosSpent / 1000 / 1000) + "ms)");
          }
        }
        writersClosed = true;
      }

      return thrown;
    }

    /**
     * Get a writer and path for a log starting at the given entry. This function is threadsafe so
     * long as multiple threads are always acting on different regions.
     * @return null if this region shouldn't output any logs
     */
    private WriterAndPath getWriterAndPath(Entry entry) throws IOException {
      byte region[] = entry.getKey().getEncodedRegionName();
      WriterAndPath ret = (WriterAndPath) writers.get(region);
      if (ret != null) {
        return ret;
      }
      // If we already decided that this region doesn't get any output
      // we don't need to check again.
      if (blacklistedRegions.contains(region)) {
        return null;
      }
      ret = createWAP(region, entry, rootDir);
      if (ret == null) {
        blacklistedRegions.add(region);
        return null;
      }
      writers.put(region, ret);
      return ret;
    }

    /**
     * @return a path with a write for that path. caller should close.
     */
    private WriterAndPath createWAP(byte[] region, Entry entry, Path rootdir) throws IOException {
      Path regionedits = getRegionSplitEditsPath(fs, entry, rootdir,
          fileBeingSplit.getPath().getName());
      if (regionedits == null) {
        return null;
      }
      if (fs.exists(regionedits)) {
        LOG.warn("Found old edits file. It could be the "
            + "result of a previous failed split attempt. Deleting " + regionedits + ", length="
            + fs.getFileStatus(regionedits).getLen());
        if (!fs.delete(regionedits, false)) {
          LOG.warn("Failed delete of old " + regionedits);
        }
      }
      Writer w = createWriter(regionedits);
      LOG.debug("Creating writer path=" + regionedits);
      return new WriterAndPath(regionedits, w, entry.getKey().getLogSeqNum());
    }

    private void filterCellByStore(Entry logEntry) {
      Map<byte[], Long> maxSeqIdInStores =
          regionMaxSeqIdInStores.get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
      if (maxSeqIdInStores == null || maxSeqIdInStores.isEmpty()) {
        return;
      }
      // Create the array list for the cells that aren't filtered.
      // We make the assumption that most cells will be kept.
      ArrayList<Cell> keptCells = new ArrayList<Cell>(logEntry.getEdit().getCells().size());
      for (Cell cell : logEntry.getEdit().getCells()) {
        if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          keptCells.add(cell);
        } else {
          byte[] family = CellUtil.cloneFamily(cell);
          Long maxSeqId = maxSeqIdInStores.get(family);
          // Do not skip cell even if maxSeqId is null. Maybe we are in a rolling upgrade,
          // or the master was crashed before and we can not get the information.
          if (maxSeqId == null || maxSeqId.longValue() < logEntry.getKey().getLogSeqNum()) {
            keptCells.add(cell);
          }
        }
      }

      // Anything in the keptCells array list is still live.
      // So rather than removing the cells from the array list
      // which would be an O(n^2) operation, we just replace the list
      logEntry.getEdit().setCells(keptCells);
    }

    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      List<Entry> entries = buffer.entryBuffer;
      if (entries.isEmpty()) {
        LOG.warn("got an empty buffer, skipping");
        return;
      }

      WriterAndPath wap = null;

      long startTime = System.nanoTime();
      try {
        int editsCount = 0;

        for (Entry logEntry : entries) {
          if (wap == null) {
            wap = getWriterAndPath(logEntry);
            if (wap == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("getWriterAndPath decided we don't need to write edits for " + logEntry);
              }
              return;
            }
          }
          filterCellByStore(logEntry);
          if (!logEntry.getEdit().isEmpty()) {
            wap.w.append(logEntry);
            this.updateRegionMaximumEditLogSeqNum(logEntry);
            editsCount++;
          } else {
            wap.incrementSkippedEdits(1);
          }
        }
        // Pass along summary statistics
        wap.incrementEdits(editsCount);
        wap.incrementNanoTime(System.nanoTime() - startTime);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.fatal(" Got while writing log entry to log", e);
        throw e;
      }
    }

    @Override
    public boolean keepRegionEvent(Entry entry) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      for (int i = 0; i < cells.size(); i++) {
        if (WALEdit.isCompactionMarker(cells.get(i))) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return a map from encoded region ID to the number of edits written out for that region.
     */
    @Override
    public Map<byte[], Long> getOutputCounts() {
      TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      synchronized (writers) {
        for (Map.Entry<byte[], SinkWriter> entry : writers.entrySet()) {
          ret.put(entry.getKey(), entry.getValue().editsWritten);
        }
      }
      return ret;
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return writers.size();
    }
  }

  /**
   * Class wraps the actual writer which writes data out and related statistics
   */
  public abstract static class SinkWriter {
    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Count of edits skipped to this path */
    long editsSkipped = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    void incrementEdits(int edits) {
      editsWritten += edits;
    }

    void incrementSkippedEdits(int skipped) {
      editsSkipped += skipped;
    }

    void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }
  }

  /**
   * Private data structure that wraps a Writer and its Path, also collecting statistics about the
   * data written to this output.
   */
  private final static class WriterAndPath extends SinkWriter {
    final Path p;
    final Writer w;
    final long minLogSeqNum;

    WriterAndPath(final Path p, final Writer w, final long minLogSeqNum) {
      this.p = p;
      this.w = w;
      this.minLogSeqNum = minLogSeqNum;
    }
  }

  /**
   * Class that manages to replay edits from WAL files directly to assigned fail over region servers
   */
  class LogReplayOutputSink extends OutputSink {
    private static final double BUFFER_THRESHOLD = 0.35;
    private static final String KEY_DELIMITER = "#";

    private long waitRegionOnlineTimeOut;
    private final Set<String> recoveredRegions = Collections.synchronizedSet(new HashSet<String>());
    private final Map<String, RegionServerWriter> writers =
        new ConcurrentHashMap<String, RegionServerWriter>();
    // online encoded region name -> region location map
    private final Map<String, HRegionLocation> onlineRegions =
        new ConcurrentHashMap<String, HRegionLocation>();

    private Map<TableName, HConnection> tableNameToHConnectionMap = Collections
        .synchronizedMap(new TreeMap<TableName, HConnection>());
    /**
     * Map key -> value layout
     * <servername>:<table name> -> Queue<Row>
     */
    private Map<String, List<Pair<HRegionLocation, Entry>>> serverToBufferQueueMap =
        new ConcurrentHashMap<String, List<Pair<HRegionLocation, Entry>>>();
    private List<Throwable> thrown = new ArrayList<Throwable>();

    // The following sink is used in distrubitedLogReplay mode for entries of regions in a disabling
    // table. It's a limitation of distributedLogReplay. Because log replay needs a region is
    // assigned and online before it can replay wal edits while regions of disabling/disabled table
    // won't be assigned by AM. We can retire this code after HBASE-8234.
    private LogRecoveredEditsOutputSink logRecoveredEditsOutputSink;
    private boolean hasEditsInDisablingOrDisabledTables = false;

    public LogReplayOutputSink(PipelineController controller, EntryBuffers entryBuffers,
        int numWriters) {
      super(controller, entryBuffers, numWriters);
      this.waitRegionOnlineTimeOut = conf.getInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT,
          ZKSplitLogManagerCoordination.DEFAULT_TIMEOUT);
      this.logRecoveredEditsOutputSink = new LogRecoveredEditsOutputSink(controller,
        entryBuffers, numWriters);
      this.logRecoveredEditsOutputSink.setReporter(reporter);
    }

    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      List<Entry> entries = buffer.entryBuffer;
      if (entries.isEmpty()) {
        LOG.warn("got an empty buffer, skipping");
        return;
      }

      // check if current region in a disabling or disabled table
      if (disablingOrDisabledTables.contains(buffer.tableName)) {
        // need fall back to old way
        logRecoveredEditsOutputSink.append(buffer);
        hasEditsInDisablingOrDisabledTables = true;
        // store regions we have recovered so far
        addToRecoveredRegions(Bytes.toString(buffer.encodedRegionName));
        return;
      }

      // group entries by region servers
      groupEditsByServer(entries);

      // process workitems
      String maxLocKey = null;
      int maxSize = 0;
      List<Pair<HRegionLocation, Entry>> maxQueue = null;
      synchronized (this.serverToBufferQueueMap) {
        for (String key : this.serverToBufferQueueMap.keySet()) {
          List<Pair<HRegionLocation, Entry>> curQueue = this.serverToBufferQueueMap.get(key);
          if (curQueue.size() > maxSize) {
            maxSize = curQueue.size();
            maxQueue = curQueue;
            maxLocKey = key;
          }
        }
        if (maxSize < minBatchSize
            && entryBuffers.totalBuffered < BUFFER_THRESHOLD * entryBuffers.maxHeapUsage) {
          // buffer more to process
          return;
        } else if (maxSize > 0) {
          this.serverToBufferQueueMap.remove(maxLocKey);
        }
      }

      if (maxSize > 0) {
        processWorkItems(maxLocKey, maxQueue);
      }
    }

    private void addToRecoveredRegions(String encodedRegionName) {
      if (!recoveredRegions.contains(encodedRegionName)) {
        recoveredRegions.add(encodedRegionName);
      }
    }

    /**
     * Helper function to group WALEntries to individual region servers
     * @throws IOException
     */
    private void groupEditsByServer(List<Entry> entries) throws IOException {
      Set<TableName> nonExistentTables = null;
      Long cachedLastFlushedSequenceId = -1l;
      for (Entry entry : entries) {
        WALEdit edit = entry.getEdit();
        TableName table = entry.getKey().getTablename();
        // clear scopes which isn't needed for recovery
        entry.getKey().setScopes(null);
        String encodeRegionNameStr = Bytes.toString(entry.getKey().getEncodedRegionName());
        // skip edits of non-existent tables
        if (nonExistentTables != null && nonExistentTables.contains(table)) {
          this.skippedEdits.incrementAndGet();
          continue;
        }

        Map<byte[], Long> maxStoreSequenceIds = null;
        boolean needSkip = false;
        HRegionLocation loc = null;
        String locKey = null;
        List<Cell> cells = edit.getCells();
        List<Cell> skippedCells = new ArrayList<Cell>();
        HConnection hconn = this.getConnectionByTableName(table);

        for (Cell cell : cells) {
          byte[] row = CellUtil.cloneRow(cell);
          byte[] family = CellUtil.cloneFamily(cell);
          boolean isCompactionEntry = false;
          if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
            CompactionDescriptor compaction = WALEdit.getCompaction(cell);
            if (compaction != null && compaction.hasRegionName()) {
              try {
                byte[][] regionName = HRegionInfo.parseRegionName(compaction.getRegionName()
                  .toByteArray());
                row = regionName[1]; // startKey of the region
                family = compaction.getFamilyName().toByteArray();
                isCompactionEntry = true;
              } catch (Exception ex) {
                LOG.warn("Unexpected exception received, ignoring " + ex);
                skippedCells.add(cell);
                continue;
              }
            } else {
              skippedCells.add(cell);
              continue;
            }
          }

          try {
            loc =
                locateRegionAndRefreshLastFlushedSequenceId(hconn, table, row,
                  encodeRegionNameStr);
            // skip replaying the compaction if the region is gone
            if (isCompactionEntry && !encodeRegionNameStr.equalsIgnoreCase(
              loc.getRegionInfo().getEncodedName())) {
              LOG.info("Not replaying a compaction marker for an older region: "
                  + encodeRegionNameStr);
              needSkip = true;
            }
          } catch (TableNotFoundException ex) {
            // table has been deleted so skip edits of the table
            LOG.info("Table " + table + " doesn't exist. Skip log replay for region "
                + encodeRegionNameStr);
            lastFlushedSequenceIds.put(encodeRegionNameStr, Long.MAX_VALUE);
            if (nonExistentTables == null) {
              nonExistentTables = new TreeSet<TableName>();
            }
            nonExistentTables.add(table);
            this.skippedEdits.incrementAndGet();
            needSkip = true;
            break;
          }

          cachedLastFlushedSequenceId =
              lastFlushedSequenceIds.get(loc.getRegionInfo().getEncodedName());
          if (cachedLastFlushedSequenceId != null
              && cachedLastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
            // skip the whole WAL entry
            this.skippedEdits.incrementAndGet();
            needSkip = true;
            break;
          } else {
            if (maxStoreSequenceIds == null) {
              maxStoreSequenceIds =
                  regionMaxSeqIdInStores.get(loc.getRegionInfo().getEncodedName());
            }
            if (maxStoreSequenceIds != null) {
              Long maxStoreSeqId = maxStoreSequenceIds.get(family);
              if (maxStoreSeqId == null || maxStoreSeqId >= entry.getKey().getLogSeqNum()) {
                // skip current kv if column family doesn't exist anymore or already flushed
                skippedCells.add(cell);
                continue;
              }
            }
          }
        }

        // skip the edit
        if (loc == null || needSkip) continue;

        if (!skippedCells.isEmpty()) {
          cells.removeAll(skippedCells);
        }

        synchronized (serverToBufferQueueMap) {
          locKey = loc.getHostnamePort() + KEY_DELIMITER + table;
          List<Pair<HRegionLocation, Entry>> queue = serverToBufferQueueMap.get(locKey);
          if (queue == null) {
            queue =
                Collections.synchronizedList(new ArrayList<Pair<HRegionLocation, Entry>>());
            serverToBufferQueueMap.put(locKey, queue);
          }
          queue.add(new Pair<HRegionLocation, Entry>(loc, entry));
        }
        // store regions we have recovered so far
        addToRecoveredRegions(loc.getRegionInfo().getEncodedName());
      }
    }

    /**
     * Locate destination region based on table name & row. This function also makes sure the
     * destination region is online for replay.
     * @throws IOException
     */
    private HRegionLocation locateRegionAndRefreshLastFlushedSequenceId(HConnection hconn,
        TableName table, byte[] row, String originalEncodedRegionName) throws IOException {
      // fetch location from cache
      HRegionLocation loc = onlineRegions.get(originalEncodedRegionName);
      if(loc != null) return loc;
      // fetch location from hbase:meta directly without using cache to avoid hit old dead server
      loc = hconn.getRegionLocation(table, row, true);
      if (loc == null) {
        throw new IOException("Can't locate location for row:" + Bytes.toString(row)
            + " of table:" + table);
      }
      // check if current row moves to a different region due to region merge/split
      if (!originalEncodedRegionName.equalsIgnoreCase(loc.getRegionInfo().getEncodedName())) {
        // originalEncodedRegionName should have already flushed
        lastFlushedSequenceIds.put(originalEncodedRegionName, Long.MAX_VALUE);
        HRegionLocation tmpLoc = onlineRegions.get(loc.getRegionInfo().getEncodedName());
        if (tmpLoc != null) return tmpLoc;
      }

      Long lastFlushedSequenceId = -1l;
      AtomicBoolean isRecovering = new AtomicBoolean(true);
      loc = waitUntilRegionOnline(loc, row, this.waitRegionOnlineTimeOut, isRecovering);
      if (!isRecovering.get()) {
        // region isn't in recovering at all because WAL file may contain a region that has
        // been moved to somewhere before hosting RS fails
        lastFlushedSequenceIds.put(loc.getRegionInfo().getEncodedName(), Long.MAX_VALUE);
        LOG.info("logReplay skip region: " + loc.getRegionInfo().getEncodedName()
            + " because it's not in recovering.");
      } else {
        Long cachedLastFlushedSequenceId =
            lastFlushedSequenceIds.get(loc.getRegionInfo().getEncodedName());

        // retrieve last flushed sequence Id from ZK. Because region postOpenDeployTasks will
        // update the value for the region
        RegionStoreSequenceIds ids =
            csm.getSplitLogWorkerCoordination().getRegionFlushedSequenceId(failedServerName,
              loc.getRegionInfo().getEncodedName());
        if (ids != null) {
          lastFlushedSequenceId = ids.getLastFlushedSequenceId();
          Map<byte[], Long> storeIds = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
          List<StoreSequenceId> maxSeqIdInStores = ids.getStoreSequenceIdList();
          for (StoreSequenceId id : maxSeqIdInStores) {
            storeIds.put(id.getFamilyName().toByteArray(), id.getSequenceId());
          }
          regionMaxSeqIdInStores.put(loc.getRegionInfo().getEncodedName(), storeIds);
        }

        if (cachedLastFlushedSequenceId == null
            || lastFlushedSequenceId > cachedLastFlushedSequenceId) {
          lastFlushedSequenceIds.put(loc.getRegionInfo().getEncodedName(), lastFlushedSequenceId);
        }
      }

      onlineRegions.put(loc.getRegionInfo().getEncodedName(), loc);
      return loc;
    }

    private void processWorkItems(String key, List<Pair<HRegionLocation, Entry>> actions)
        throws IOException {
      RegionServerWriter rsw = null;

      long startTime = System.nanoTime();
      try {
        rsw = getRegionServerWriter(key);
        rsw.sink.replayEntries(actions);

        // Pass along summary statistics
        rsw.incrementEdits(actions.size());
        rsw.incrementNanoTime(System.nanoTime() - startTime);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.fatal(" Got while writing log entry to log", e);
        throw e;
      }
    }

    /**
     * Wait until region is online on the destination region server
     * @param loc
     * @param row
     * @param timeout How long to wait
     * @param isRecovering Recovering state of the region interested on destination region server.
     * @return True when region is online on the destination region server
     * @throws InterruptedException
     */
    private HRegionLocation waitUntilRegionOnline(HRegionLocation loc, byte[] row,
        final long timeout, AtomicBoolean isRecovering)
        throws IOException {
      final long endTime = EnvironmentEdgeManager.currentTime() + timeout;
      final long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
      boolean reloadLocation = false;
      TableName tableName = loc.getRegionInfo().getTable();
      int tries = 0;
      Throwable cause = null;
      while (endTime > EnvironmentEdgeManager.currentTime()) {
        try {
          // Try and get regioninfo from the hosting server.
          HConnection hconn = getConnectionByTableName(tableName);
          if(reloadLocation) {
            loc = hconn.getRegionLocation(tableName, row, true);
          }
          BlockingInterface remoteSvr = hconn.getAdmin(loc.getServerName());
          HRegionInfo region = loc.getRegionInfo();
          try {
            GetRegionInfoRequest request =
                RequestConverter.buildGetRegionInfoRequest(region.getRegionName());
            GetRegionInfoResponse response = remoteSvr.getRegionInfo(null, request);
            if (HRegionInfo.convert(response.getRegionInfo()) != null) {
              isRecovering.set((response.hasIsRecovering()) ? response.getIsRecovering() : true);
              return loc;
            }
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        } catch (IOException e) {
          cause = e.getCause();
          if(!(cause instanceof RegionOpeningException)) {
            reloadLocation = true;
          }
        }
        long expectedSleep = ConnectionUtils.getPauseTime(pause, tries);
        try {
          Thread.sleep(expectedSleep);
        } catch (InterruptedException e) {
          throw new IOException("Interrupted when waiting region " +
              loc.getRegionInfo().getEncodedName() + " online.", e);
        }
        tries++;
      }

      throw new IOException("Timeout when waiting region " + loc.getRegionInfo().getEncodedName() +
        " online for " + timeout + " milliseconds.", cause);
    }

    @Override
    public boolean flush() throws IOException {
      String curLoc = null;
      int curSize = 0;
      List<Pair<HRegionLocation, Entry>> curQueue = null;
      synchronized (this.serverToBufferQueueMap) {
        for (String locationKey : this.serverToBufferQueueMap.keySet()) {
          curQueue = this.serverToBufferQueueMap.get(locationKey);
          if (!curQueue.isEmpty()) {
            curSize = curQueue.size();
            curLoc = locationKey;
            break;
          }
        }
        if (curSize > 0) {
          this.serverToBufferQueueMap.remove(curLoc);
        }
      }

      if (curSize > 0) {
        this.processWorkItems(curLoc, curQueue);
        // We should already have control of the monitor; ensure this is the case.
        synchronized(controller.dataAvailable) {
          controller.dataAvailable.notifyAll();
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean keepRegionEvent(Entry entry) {
      return true;
    }

    void addWriterError(Throwable t) {
      thrown.add(t);
    }

    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      try {
        if (!finishWriting(false)) {
          return null;
        }
        if (hasEditsInDisablingOrDisabledTables) {
          splits = logRecoveredEditsOutputSink.finishWritingAndClose();
        } else {
          splits = new ArrayList<Path>();
        }
        // returns an empty array in order to keep interface same as old way
        return splits;
      } finally {
        List<IOException> thrown = closeRegionServerWriters();
        if (thrown != null && !thrown.isEmpty()) {
          throw MultipleIOException.createIOException(thrown);
        }
      }
    }

    @Override
    int getNumOpenWriters() {
      return this.writers.size() + this.logRecoveredEditsOutputSink.getNumOpenWriters();
    }

    private List<IOException> closeRegionServerWriters() throws IOException {
      List<IOException> result = null;
      if (!writersClosed) {
        result = Lists.newArrayList();
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
          synchronized (writers) {
            for (String locationKey : writers.keySet()) {
              RegionServerWriter tmpW = writers.get(locationKey);
              try {
                tmpW.close();
              } catch (IOException ioe) {
                LOG.error("Couldn't close writer for region server:" + locationKey, ioe);
                result.add(ioe);
              }
            }
          }

          // close connections
          synchronized (this.tableNameToHConnectionMap) {
            for (TableName tableName : this.tableNameToHConnectionMap.keySet()) {
              HConnection hconn = this.tableNameToHConnectionMap.get(tableName);
              try {
                hconn.clearRegionCache();
                hconn.close();
              } catch (IOException ioe) {
                result.add(ioe);
              }
            }
          }
          writersClosed = true;
        }
      }
      return result;
    }

    @Override
    public Map<byte[], Long> getOutputCounts() {
      TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      synchronized (writers) {
        for (Map.Entry<String, RegionServerWriter> entry : writers.entrySet()) {
          ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
        }
      }
      return ret;
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return this.recoveredRegions.size();
    }

    /**
     * Get a writer and path for a log starting at the given entry. This function is threadsafe so
     * long as multiple threads are always acting on different regions.
     * @return null if this region shouldn't output any logs
     */
    private RegionServerWriter getRegionServerWriter(String loc) throws IOException {
      RegionServerWriter ret = writers.get(loc);
      if (ret != null) {
        return ret;
      }

      TableName tableName = getTableFromLocationStr(loc);
      if(tableName == null){
        throw new IOException("Invalid location string:" + loc + " found. Replay aborted.");
      }

      HConnection hconn = getConnectionByTableName(tableName);
      synchronized (writers) {
        ret = writers.get(loc);
        if (ret == null) {
          ret = new RegionServerWriter(conf, tableName, hconn);
          writers.put(loc, ret);
        }
      }
      return ret;
    }

    private HConnection getConnectionByTableName(final TableName tableName) throws IOException {
      HConnection hconn = this.tableNameToHConnectionMap.get(tableName);
      if (hconn == null) {
        synchronized (this.tableNameToHConnectionMap) {
          hconn = this.tableNameToHConnectionMap.get(tableName);
          if (hconn == null) {
            hconn = HConnectionManager.getConnection(conf);
            this.tableNameToHConnectionMap.put(tableName, hconn);
          }
        }
      }
      return hconn;
    }
    private TableName getTableFromLocationStr(String loc) {
      /**
       * location key is in format <server name:port>#<table name>
       */
      String[] splits = loc.split(KEY_DELIMITER);
      if (splits.length != 2) {
        return null;
      }
      return TableName.valueOf(splits[1]);
    }
  }

  /**
   * Private data structure that wraps a receiving RS and collecting statistics about the data
   * written to this newly assigned RS.
   */
  private final static class RegionServerWriter extends SinkWriter {
    final WALEditsReplaySink sink;

    RegionServerWriter(final Configuration conf, final TableName tableName, final HConnection conn)
        throws IOException {
      this.sink = new WALEditsReplaySink(conf, tableName, conn);
    }

    void close() throws IOException {
    }
  }

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;

    CorruptedLogFileException(String s) {
      super(s);
    }
  }

  /** A struct used by getMutationsFromWALEntry */
  public static class MutationReplay {
    public MutationReplay(MutationType type, Mutation mutation, long nonceGroup, long nonce) {
      this.type = type;
      this.mutation = mutation;
      if(this.mutation.getDurability() != Durability.SKIP_WAL) {
        // using ASYNC_WAL for relay
        this.mutation.setDurability(Durability.ASYNC_WAL);
      }
      this.nonceGroup = nonceGroup;
      this.nonce = nonce;
    }

    public final MutationType type;
    public final Mutation mutation;
    public final long nonceGroup;
    public final long nonce;
  }

  /**
   * This function is used to construct mutations from a WALEntry. It also
   * reconstructs WALKey &amp; WALEdit from the passed in WALEntry
   * @param entry
   * @param cells
   * @param logEntry pair of WALKey and WALEdit instance stores WALKey and WALEdit instances
   *          extracted from the passed in WALEntry.
   * @param durability
   * @return list of Pair&lt;MutationType, Mutation&gt; to be replayed
   * @throws IOException
   */
  public static List<MutationReplay> getMutationsFromWALEntry(WALEntry entry, CellScanner cells,
      Pair<WALKey, WALEdit> logEntry, Durability durability)
          throws IOException {
    if (entry == null) {
      // return an empty array
      return new ArrayList<MutationReplay>();
    }

    long replaySeqId = (entry.getKey().hasOrigSequenceNumber()) ?
      entry.getKey().getOrigSequenceNumber() : entry.getKey().getLogSequenceNumber();
    int count = entry.getAssociatedCellCount();
    List<MutationReplay> mutations = new ArrayList<MutationReplay>();
    Cell previousCell = null;
    Mutation m = null;
    WALKey key = null;
    WALEdit val = null;
    if (logEntry != null) val = new WALEdit();

    for (int i = 0; i < count; i++) {
      // Throw index out of bounds if our cell count is off
      if (!cells.advance()) {
        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
      }
      Cell cell = cells.current();
      if (val != null) val.add(cell);

      boolean isNewRowOrType =
          previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
              || !CellUtil.matchingRow(previousCell, cell);
      if (isNewRowOrType) {
        // Create new mutation
        if (CellUtil.isDelete(cell)) {
          m = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Deletes don't have nonces.
          mutations.add(new MutationReplay(
              MutationType.DELETE, m, HConstants.NO_NONCE, HConstants.NO_NONCE));
        } else {
          m = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Puts might come from increment or append, thus we need nonces.
          long nonceGroup = entry.getKey().hasNonceGroup()
              ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          mutations.add(new MutationReplay(MutationType.PUT, m, nonceGroup, nonce));
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) m).addDeleteMarker(cell);
      } else {
        ((Put) m).add(cell);
      }
      m.setDurability(durability);
      previousCell = cell;
    }

    // reconstruct WALKey
    if (logEntry != null) {
      org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey walKeyProto = entry.getKey();
      List<UUID> clusterIds = new ArrayList<UUID>(walKeyProto.getClusterIdsCount());
      for (HBaseProtos.UUID uuid : entry.getKey().getClusterIdsList()) {
        clusterIds.add(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
      }
      // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
      key = new HLogKey(walKeyProto.getEncodedRegionName().toByteArray(), TableName.valueOf(
              walKeyProto.getTableName().toByteArray()), replaySeqId, walKeyProto.getWriteTime(),
              clusterIds, walKeyProto.getNonceGroup(), walKeyProto.getNonce(), null);
      logEntry.setFirst(key);
      logEntry.setSecond(val);
    }

    return mutations;
  }
}
