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
package org.apache.hadoop.hbase.wal;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region, for
 * recovering data on startup. Delete the old log files when finished.
 */
@InterfaceAudience.Private
public class WALSplitter {
  private static final Logger LOG = LoggerFactory.getLogger(WALSplitter.class);

  /** By default we retry errors in splitting, rather than skipping. */
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;

  // Parameters for split process
  protected final Path walDir;
  protected final FileSystem walFS;
  protected final Configuration conf;

  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  OutputSink outputSink;
  private EntryBuffers entryBuffers;

  private SplitLogWorkerCoordination splitLogWorkerCoordination;
  private final WALFactory walFactory;

  private MonitoredTask status;

  // For checking the latest flushed sequence id
  protected final LastSequenceId sequenceIdChecker;

  // Map encodedRegionName -> lastFlushedSequenceId
  protected Map<String, Long> lastFlushedSequenceIds = new ConcurrentHashMap<>();

  // Map encodedRegionName -> maxSeqIdInStores
  protected Map<String, Map<byte[], Long>> regionMaxSeqIdInStores = new ConcurrentHashMap<>();

  // the file being split currently
  private FileStatus fileBeingSplit;

  // if we limit the number of writers opened for sinking recovered edits
  private final boolean splitWriterCreationBounded;

  public final static String SPLIT_WRITER_CREATION_BOUNDED = "hbase.split.writer.creation.bounded";


  @VisibleForTesting
  WALSplitter(final WALFactory factory, Configuration conf, Path walDir,
      FileSystem walFS, LastSequenceId idChecker,
      SplitLogWorkerCoordination splitLogWorkerCoordination) {
    this.conf = HBaseConfiguration.create(conf);
    String codecClassName = conf
        .get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, codecClassName);
    this.walDir = walDir;
    this.walFS = walFS;
    this.sequenceIdChecker = idChecker;
    this.splitLogWorkerCoordination = splitLogWorkerCoordination;

    this.walFactory = factory;
    PipelineController controller = new PipelineController();

    this.splitWriterCreationBounded = conf.getBoolean(SPLIT_WRITER_CREATION_BOUNDED, false);

    entryBuffers = new EntryBuffers(controller,
        this.conf.getLong("hbase.regionserver.hlog.splitlog.buffersize", 128 * 1024 * 1024),
        splitWriterCreationBounded);

    int numWriterThreads = this.conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    if(splitWriterCreationBounded){
      outputSink = new BoundedLogWriterCreationOutputSink(
          controller, entryBuffers, numWriterThreads);
    }else {
      outputSink = new LogRecoveredEditsOutputSink(controller, entryBuffers, numWriterThreads);
    }
  }

  /**
   * Splits a WAL file into region's recovered-edits directory.
   * This is the main entry point for distributed log splitting from SplitLogWorker.
   * <p>
   * If the log file has N regions then N recovered.edits files will be produced.
   * <p>
   * @return false if it is interrupted by the progress-able.
   */
  public static boolean splitLogFile(Path walDir, FileStatus logfile, FileSystem walFS,
      Configuration conf, CancelableProgressable reporter, LastSequenceId idChecker,
      SplitLogWorkerCoordination splitLogWorkerCoordination, final WALFactory factory)
      throws IOException {
    WALSplitter s = new WALSplitter(factory, conf, walDir, walFS, idChecker,
        splitLogWorkerCoordination);
    return s.splitLogFile(logfile, reporter);
  }

  // A wrapper to split one log folder using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because TestWALObserver is in a different package,
  // which uses this method to do log splitting.
  @VisibleForTesting
  public static List<Path> split(Path rootDir, Path logDir, Path oldLogDir,
      FileSystem walFS, Configuration conf, final WALFactory factory) throws IOException {
    final FileStatus[] logfiles = SplitLogManager.getFileList(conf,
        Collections.singletonList(logDir), null);
    List<Path> splits = new ArrayList<>();
    if (ArrayUtils.isNotEmpty(logfiles)) {
      for (FileStatus logfile: logfiles) {
        WALSplitter s = new WALSplitter(factory, conf, rootDir, walFS, null, null);
        if (s.splitLogFile(logfile, null)) {
          finishSplitLogFile(rootDir, oldLogDir, logfile.getPath(), conf);
          if (s.outputSink.splits != null) {
            splits.addAll(s.outputSink.splits);
          }
        }
      }
    }
    if (!walFS.delete(logDir, true)) {
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

    status = TaskMonitor.get().createStatus(
          "Splitting log file " + logfile.getPath() + "into a temporary staging area.");
    Reader logFileReader = null;
    this.fileBeingSplit = logfile;
    try {
      long logLength = logfile.getLen();
      LOG.info("Splitting WAL={}, length={}", logPath, logLength);
      status.setStatus("Opening log file");
      if (reporter != null && !reporter.progress()) {
        progress_failed = true;
        return false;
      }
      logFileReader = getReader(logfile, skipErrors, reporter);
      if (logFileReader == null) {
        LOG.warn("Nothing to split in WAL={}", logPath);
        return true;
      }
      int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
      int numOpenedFilesLastCheck = 0;
      outputSink.setReporter(reporter);
      outputSink.startWriterThreads();
      outputSinkStarted = true;
      Entry entry;
      Long lastFlushedSequenceId = -1L;
      while ((entry = getNextLogLine(logFileReader, logPath, skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (sequenceIdChecker != null) {
            RegionStoreSequenceIds ids = sequenceIdChecker.getLastSequenceId(region);
            Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
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
        if (lastFlushedSequenceId >= entry.getKey().getSequenceId()) {
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
      LOG.warn("Could not parse, corrupted WAL={}", logPath, e);
      if (splitLogWorkerCoordination != null) {
        // Some tests pass in a csm of null.
        splitLogWorkerCoordination.markCorrupted(walDir, logfile.getPath().getName(), walFS);
      } else {
        // for tests only
        ZKSplitLog.markCorrupted(walDir, logfile.getPath().getName(), walFS);
      }
      isCorrupted = true;
    } catch (IOException e) {
      e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
      throw e;
    } finally {
      LOG.debug("Finishing writing output logs and closing down");
      try {
        if (null != logFileReader) {
          logFileReader.close();
        }
      } catch (IOException exception) {
        LOG.warn("Could not close WAL reader", exception);
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
    Path walDir = FSUtils.getWALRootDir(conf);
    Path oldLogDir = new Path(walDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logPath;
    if (FSUtils.isStartingWithPath(walDir, logfile)) {
      logPath = new Path(logfile);
    } else {
      logPath = new Path(walDir, logfile);
    }
    finishSplitLogFile(walDir, oldLogDir, logPath, conf);
  }

  private static void finishSplitLogFile(Path walDir, Path oldLogDir,
      Path logPath, Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<>();
    List<Path> corruptedLogs = new ArrayList<>();
    FileSystem walFS = walDir.getFileSystem(conf);
    if (ZKSplitLog.isCorrupted(walDir, logPath.getName(), walFS)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    archiveLogs(corruptedLogs, processedLogs, oldLogDir, walFS, conf);
    Path stagingDir = ZKSplitLog.getSplitLogDir(walDir, logPath.getName());
    walFS.delete(stagingDir, true);
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
   *
   * @param corruptedLogs
   * @param processedLogs
   * @param oldLogDir
   * @param walFS WAL FileSystem to archive files on.
   * @param conf
   * @throws IOException
   */
  private static void archiveLogs(
      final List<Path> corruptedLogs,
      final List<Path> processedLogs, final Path oldLogDir,
      final FileSystem walFS, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(FSUtils.getWALRootDir(conf), HConstants.CORRUPT_DIR_NAME);
    if (conf.get("hbase.regionserver.hlog.splitlog.corrupt.dir") != null) {
      LOG.warn("hbase.regionserver.hlog.splitlog.corrupt.dir is deprecated. Default to {}",
          corruptDir);
    }
    if (!walFS.mkdirs(corruptDir)) {
      LOG.info("Unable to mkdir {}", corruptDir);
    }
    walFS.mkdirs(oldLogDir);

    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corrupted : corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      if (walFS.exists(corrupted)) {
        if (!walFS.rename(corrupted, p)) {
          LOG.warn("Unable to move corrupted log {} to {}", corrupted, p);
        } else {
          LOG.warn("Moved corrupted log {} to {}", corrupted, p);
        }
      }
    }

    for (Path p : processedLogs) {
      Path newPath = AbstractFSWAL.getWALArchivePath(oldLogDir, p);
      if (walFS.exists(p)) {
        if (!FSUtils.renameAndSetModifyTime(walFS, p, newPath)) {
          LOG.warn("Unable to move {} to {}", p, newPath);
        } else {
          LOG.info("Archived processed log {} to {}", p, newPath);
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
   * @param fileNameBeingSplit the file being split currently. Used to generate tmp file name.
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static Path getRegionSplitEditsPath(final Entry logEntry, String fileNameBeingSplit,
      Configuration conf) throws IOException {
    FileSystem walFS = FSUtils.getWALFileSystem(conf);
    Path tableDir = FSUtils.getWALTableDir(conf, logEntry.getKey().getTableName());
    String encodedRegionName = Bytes.toString(logEntry.getKey().getEncodedRegionName());
    Path regionDir = HRegion.getRegionDir(tableDir, encodedRegionName);
    Path dir = getRegionDirRecoveredEditsDir(regionDir);


    if (walFS.exists(dir) && walFS.isFile(dir)) {
      Path tmp = new Path("/tmp");
      if (!walFS.exists(tmp)) {
        walFS.mkdirs(tmp);
      }
      tmp = new Path(tmp,
        HConstants.RECOVERED_EDITS_DIR + "_" + encodedRegionName);
      LOG.warn("Found existing old file: {}. It could be some "
        + "leftover of an old installation. It should be a folder instead. "
        + "So moving it to {}", dir, tmp);
      if (!walFS.rename(dir, tmp)) {
        LOG.warn("Failed to sideline old file {}", dir);
      }
    }

    if (!walFS.exists(dir) && !walFS.mkdirs(dir)) {
      LOG.warn("mkdir failed on {}", dir);
    }
    // Append fileBeingSplit to prevent name conflict since we may have duplicate wal entries now.
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName = formatRecoveredEditsFileName(logEntry.getKey().getSequenceId());
    fileName = getTmpRecoveredEditsFileName(fileName + "-" + fileNameBeingSplit);
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
   * @param regionDir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regionDir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regionDir) {
    return new Path(regionDir, HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Check whether there is recovered.edits in the region dir
   * @param walFS FileSystem
   * @param conf conf
   * @param regionInfo the region to check
   * @throws IOException IOException
   * @return true if recovered.edits exist in the region dir
   */
  public static boolean hasRecoveredEdits(final FileSystem walFS,
      final Configuration conf, final RegionInfo regionInfo) throws IOException {
    // No recovered.edits for non default replica regions
    if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return false;
    }
    //Only default replica region can reach here, so we can use regioninfo
    //directly without converting it to default replica's regioninfo.
    Path regionDir = FSUtils.getWALRegionDir(conf, regionInfo.getTable(),
        regionInfo.getEncodedName());
    NavigableSet<Path> files = getSplitEditFilesSorted(walFS, regionDir);
    return files != null && !files.isEmpty();
  }


  /**
   * Returns sorted set of edit files made by splitter, excluding files
   * with '.temp' suffix.
   *
   * @param walFS WAL FileSystem used to retrieving split edits files.
   * @param regionDir WAL region dir to look for recovered edits files under.
   * @return Files in passed <code>regionDir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem walFS,
      final Path regionDir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<>();
    Path editsdir = getRegionDirRecoveredEditsDir(regionDir);
    if (!walFS.exists(editsdir)) {
      return filesSorted;
    }
    FileStatus[] files = FSUtils.listStatus(walFS, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = walFS.isFile(p) && m.matches();
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
          LOG.warn("Failed isFile check on {}", p, e);
        }
        return result;
      }
    });
    if (ArrayUtils.isNotEmpty(files)) {
      Arrays.asList(files).forEach(status -> filesSorted.add(status.getPath()));
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   *
   * @param walFS WAL FileSystem used to rename bad edits file.
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem walFS, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!walFS.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from {} to {}", edits, moveAsideName);
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

  private static FileStatus[] getSequenceIdFiles(FileSystem walFS, Path regionDir)
      throws IOException {
    // TODO: Why are we using a method in here as part of our normal region open where
    // there is no splitting involved? Fix. St.Ack 01/20/2017.
    Path editsDir = WALSplitter.getRegionDirRecoveredEditsDir(regionDir);
    try {
      FileStatus[] files = walFS.listStatus(editsDir, WALSplitter::isSequenceIdFile);
      return files != null ? files : new FileStatus[0];
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  private static long getMaxSequenceId(FileStatus[] files) {
    long maxSeqId = -1L;
    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      try {
        maxSeqId = Math.max(maxSeqId, Long
          .parseLong(fileName.substring(0, fileName.length() - SEQUENCE_ID_FILE_SUFFIX_LENGTH)));
      } catch (NumberFormatException ex) {
        LOG.warn("Invalid SeqId File Name={}", fileName);
      }
    }
    return maxSeqId;
  }

  /**
   * Get the max sequence id which is stored in the region directory. -1 if none.
   */
  public static long getMaxRegionSequenceId(FileSystem walFS, Path regionDir) throws IOException {
    return getMaxSequenceId(getSequenceIdFiles(walFS, regionDir));
  }

  /**
   * Create a file with name as region's max sequence id
   */
  public static void writeRegionSequenceIdFile(FileSystem walFS, Path regionDir, long newMaxSeqId)
      throws IOException {
    FileStatus[] files = getSequenceIdFiles(walFS, regionDir);
    long maxSeqId = getMaxSequenceId(files);
    if (maxSeqId > newMaxSeqId) {
      throw new IOException("The new max sequence id " + newMaxSeqId +
        " is less than the old max sequence id " + maxSeqId);
    }
    // write a new seqId file
    Path newSeqIdFile = new Path(WALSplitter.getRegionDirRecoveredEditsDir(regionDir),
      newMaxSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newMaxSeqId != maxSeqId) {
      try {
        if (!walFS.createNewFile(newSeqIdFile) && !walFS.exists(newSeqIdFile)) {
          throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
        }
        LOG.debug("Wrote file={}, newMaxSeqId={}, maxSeqId={}", newSeqIdFile, newMaxSeqId,
          maxSeqId);
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    for (FileStatus status : files) {
      if (!newSeqIdFile.equals(status.getPath())) {
        walFS.delete(status.getPath(), false);
      }
    }
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
      LOG.warn("File {} might be still open, length is 0", path);
    }

    try {
      FSUtils.getInstance(walFS, conf).recoverFileLease(walFS, path, conf, reporter);
      try {
        in = getReader(path, reporter);
      } catch (EOFException e) {
        if (length <= 0) {
          // TODO should we ignore an empty, not-last log file if skip.errors
          // is false? Either way, the caller should decide what to do. E.g.
          // ignore if this is the last log in sequence.
          // TODO is this scenario still possible if the log has been
          // recovered (i.e. closed)
          LOG.warn("Could not open {} for reading. File is empty", path, e);
        }
        // EOFException being ignored
        return null;
      }
    } catch (IOException e) {
      if (e instanceof FileNotFoundException) {
        // A wal file may not exist anymore. Nothing can be recovered so move on
        LOG.warn("File {} does not exist anymore", path, e);
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
      LOG.info("EOF from wal {}. Continuing.", path);
      return null;
    } catch (IOException e) {
      // If the IOE resulted from bad file format,
      // then this problem is idempotent and retrying won't help
      if (e.getCause() != null &&
          (e.getCause() instanceof ParseException ||
           e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception from wal {}. Continuing", path, e);
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
    return walFactory.createRecoveredEditsWriter(walFS, logfile);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  protected Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
    return walFactory.createReader(walFS, curLogFile, reporter);
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
    AtomicReference<Throwable> thrown = new AtomicReference<>();

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

    Map<byte[], RegionEntryBuffer> buffers = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    /* Track which regions are currently in the middle of writing. We don't allow
       an IO thread to pick up bytes from a region if we're already writing
       data for that region in a different IO thread. */
    Set<byte[]> currentlyWriting = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    long maxHeapUsage;
    boolean splitWriterCreationBounded;

    public EntryBuffers(PipelineController controller, long maxHeapUsage) {
      this(controller, maxHeapUsage, false);
    }

    public EntryBuffers(PipelineController controller, long maxHeapUsage,
        boolean splitWriterCreationBounded){
      this.controller = controller;
      this.maxHeapUsage = maxHeapUsage;
      this.splitWriterCreationBounded = splitWriterCreationBounded;
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
          buffer = new RegionEntryBuffer(key.getTableName(), key.getEncodedRegionName());
          buffers.put(key.getEncodedRegionName(), buffer);
        }
        incrHeap= buffer.appendEntry(entry);
      }

      // If we crossed the chunk threshold, wait for more space to be available
      synchronized (controller.dataAvailable) {
        totalBuffered += incrHeap;
        while (totalBuffered > maxHeapUsage && controller.thrown.get() == null) {
          LOG.debug("Used {} bytes of buffered edits, waiting for IO threads", totalBuffered);
          controller.dataAvailable.wait(2000);
        }
        controller.dataAvailable.notifyAll();
      }
      controller.checkForErrors();
    }

    /**
     * @return RegionEntryBuffer a buffer of edits to be written.
     */
    synchronized RegionEntryBuffer getChunkToWrite() {
      // The core part of limiting opening writers is it doesn't return chunk only if the
      // heap size is over maxHeapUsage. Thus it doesn't need to create a writer for each
      // region during splitting. It will flush all the logs in the buffer after splitting
      // through a threadpool, which means the number of writers it created is under control.
      if (splitWriterCreationBounded && totalBuffered < maxHeapUsage) {
        return null;
      }
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
            LOG.warn("Got interrupted while waiting for EntryBuffers is drained");
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
      this.entryBuffer = new ArrayList<>();
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
      WALKeyImpl k = entry.getKey();
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
      LOG.trace("Writer thread starting");
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
   * The following class is an abstraction class to provide a common interface to support
   * different ways of consuming recovered edits.
   */
  public static abstract class OutputSink {

    protected PipelineController controller;
    protected EntryBuffers entryBuffers;

    protected ConcurrentHashMap<String, SinkWriter> writers = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, Long> regionMaximumEditLogSeqNum =
        new ConcurrentHashMap<>();


    protected final List<WriterThread> writerThreads = Lists.newArrayList();

    /* Set of regions which we've decided should not output edits */
    protected final Set<byte[]> blacklistedRegions = Collections
        .synchronizedSet(new TreeSet<>(Bytes.BYTES_COMPARATOR));

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
        String regionName = Bytes.toString(entry.getKey().getEncodedRegionName());
        Long currentMaxSeqNum = regionMaximumEditLogSeqNum.get(regionName);
        if (currentMaxSeqNum == null || entry.getKey().getSequenceId() > currentMaxSeqNum) {
          regionMaximumEditLogSeqNum.put(regionName, entry.getKey().getSequenceId());
        }
      }
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
      LOG.info("{} split writers finished; closing.", this.writerThreads.size());
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
        if (CollectionUtils.isNotEmpty(thrown)) {
          throw MultipleIOException.createIOException(thrown);
        }
      }
      if (isSuccessful) {
        splits = result;
      }
      return splits;
    }

    // delete the one with fewer wal entries
    private void deleteOneWithFewerEntries(WriterAndPath wap, Path dst)
        throws IOException {
      long dstMinLogSeqNum = -1L;
      try (WAL.Reader reader = walFactory.createReader(walFS, dst)) {
        WAL.Entry entry = reader.next();
        if (entry != null) {
          dstMinLogSeqNum = entry.getKey().getSequenceId();
        }
      } catch (EOFException e) {
        LOG.debug("Got EOF when reading first WAL entry from {}, an empty or broken WAL file?",
            dst, e);
      }
      if (wap.minLogSeqNum < dstMinLogSeqNum) {
        LOG.warn("Found existing old edits file. It could be the result of a previous failed"
            + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
            + walFS.getFileStatus(dst).getLen());
        if (!walFS.delete(dst, false)) {
          LOG.warn("Failed deleting of old {}", dst);
          throw new IOException("Failed deleting of old " + dst);
        }
      } else {
        LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.p
            + ", length=" + walFS.getFileStatus(wap.p).getLen());
        if (!walFS.delete(wap.p, false)) {
          LOG.warn("Failed deleting of {}", wap.p);
          throw new IOException("Failed deleting of " + wap.p);
        }
      }
    }

    /**
     * Close all of the output streams.
     * @return the list of paths written.
     */
    List<Path> close() throws IOException {
      Preconditions.checkState(!closeAndCleanCompleted);

      final List<Path> paths = new ArrayList<>();
      final List<IOException> thrown = Lists.newArrayList();
      ThreadPoolExecutor closeThreadPool = Threads
          .getBoundedCachedThreadPool(numThreads, 30L, TimeUnit.SECONDS, new ThreadFactory() {
            private int count = 1;

            @Override public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "split-log-closeStream-" + count++);
              return t;
            }
          });
      CompletionService<Void> completionService = new ExecutorCompletionService<>(closeThreadPool);
      boolean progress_failed;
      try {
        progress_failed = executeCloseTask(completionService, thrown, paths);
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

    /**
     * @param completionService threadPool to execute the closing tasks
     * @param thrown store the exceptions
     * @param paths arrayList to store the paths written
     * @return if close tasks executed successful
     */
    boolean executeCloseTask(CompletionService<Void> completionService,
        List<IOException> thrown, List<Path> paths)
        throws InterruptedException, ExecutionException {
      for (final Map.Entry<String, SinkWriter> writersEntry : writers.entrySet()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Submitting close of " + ((WriterAndPath) writersEntry.getValue()).p);
        }
        completionService.submit(new Callable<Void>() {
          @Override public Void call() throws Exception {
            WriterAndPath wap = (WriterAndPath) writersEntry.getValue();
            Path dst = closeWriter(writersEntry.getKey(), wap, thrown);
            paths.add(dst);
            return null;
          }
        });
      }
      boolean progress_failed = false;
      for (int i = 0, n = this.writers.size(); i < n; i++) {
        Future<Void> future = completionService.take();
        future.get();
        if (!progress_failed && reporter != null && !reporter.progress()) {
          progress_failed = true;
        }
      }
      return progress_failed;
    }

    Path closeWriter(String encodedRegionName, WriterAndPath wap,
        List<IOException> thrown) throws IOException{
      LOG.trace("Closing " + wap.p);
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
        if (walFS.exists(wap.p) && !walFS.delete(wap.p, false)) {
          LOG.warn("Failed deleting empty " + wap.p);
          throw new IOException("Failed deleting empty  " + wap.p);
        }
        return null;
      }

      Path dst = getCompletedRecoveredEditsFilePath(wap.p,
          regionMaximumEditLogSeqNum.get(encodedRegionName));
      try {
        if (!dst.equals(wap.p) && walFS.exists(dst)) {
          deleteOneWithFewerEntries(wap, dst);
        }
        // Skip the unit tests which create a splitter that reads and
        // writes the data without touching disk.
        // TestHLogSplit#testThreading is an example.
        if (walFS.exists(wap.p)) {
          if (!walFS.rename(wap.p, dst)) {
            throw new IOException("Failed renaming " + wap.p + " to " + dst);
          }
          LOG.info("Rename " + wap.p + " to " + dst);
        }
      } catch (IOException ioe) {
        LOG.error("Couldn't rename " + wap.p + " to " + dst, ioe);
        thrown.add(ioe);
        return null;
      }
      return dst;
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
          LOG.info(
              "Closed log " + wap.p + " (wrote " + wap.editsWritten + " edits in " + (wap.nanosSpent
                  / 1000 / 1000) + "ms)");
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
    WriterAndPath getWriterAndPath(Entry entry, boolean reusable) throws IOException {
      byte region[] = entry.getKey().getEncodedRegionName();
      String regionName = Bytes.toString(region);
      WriterAndPath ret = (WriterAndPath) writers.get(regionName);
      if (ret != null) {
        return ret;
      }
      // If we already decided that this region doesn't get any output
      // we don't need to check again.
      if (blacklistedRegions.contains(region)) {
        return null;
      }
      ret = createWAP(region, entry);
      if (ret == null) {
        blacklistedRegions.add(region);
        return null;
      }
      if(reusable) {
        writers.put(regionName, ret);
      }
      return ret;
    }

    /**
     * @return a path with a write for that path. caller should close.
     */
    WriterAndPath createWAP(byte[] region, Entry entry) throws IOException {
      Path regionedits = getRegionSplitEditsPath(entry,
          fileBeingSplit.getPath().getName(), conf);
      if (regionedits == null) {
        return null;
      }
      FileSystem walFs = FSUtils.getWALFileSystem(conf);
      if (walFs.exists(regionedits)) {
        LOG.warn("Found old edits file. It could be the "
            + "result of a previous failed split attempt. Deleting " + regionedits + ", length="
            + walFs.getFileStatus(regionedits).getLen());
        if (!walFs.delete(regionedits, false)) {
          LOG.warn("Failed delete of old {}", regionedits);
        }
      }
      Writer w = createWriter(regionedits);
      LOG.debug("Creating writer path={}", regionedits);
      return new WriterAndPath(regionedits, w, entry.getKey().getSequenceId());
    }

    void filterCellByStore(Entry logEntry) {
      Map<byte[], Long> maxSeqIdInStores =
          regionMaxSeqIdInStores.get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
      if (MapUtils.isEmpty(maxSeqIdInStores)) {
        return;
      }
      // Create the array list for the cells that aren't filtered.
      // We make the assumption that most cells will be kept.
      ArrayList<Cell> keptCells = new ArrayList<>(logEntry.getEdit().getCells().size());
      for (Cell cell : logEntry.getEdit().getCells()) {
        if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          keptCells.add(cell);
        } else {
          byte[] family = CellUtil.cloneFamily(cell);
          Long maxSeqId = maxSeqIdInStores.get(family);
          // Do not skip cell even if maxSeqId is null. Maybe we are in a rolling upgrade,
          // or the master was crashed before and we can not get the information.
          if (maxSeqId == null || maxSeqId.longValue() < logEntry.getKey().getSequenceId()) {
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
      appendBuffer(buffer, true);
    }

    WriterAndPath appendBuffer(RegionEntryBuffer buffer, boolean reusable) throws IOException{
      List<Entry> entries = buffer.entryBuffer;
      if (entries.isEmpty()) {
        LOG.warn("got an empty buffer, skipping");
        return null;
      }

      WriterAndPath wap = null;

      long startTime = System.nanoTime();
      try {
        int editsCount = 0;

        for (Entry logEntry : entries) {
          if (wap == null) {
            wap = getWriterAndPath(logEntry, reusable);
            if (wap == null) {
              if (LOG.isTraceEnabled()) {
                // This log spews the full edit. Can be massive in the log. Enable only debugging
                // WAL lost edit issues.
                LOG.trace("getWriterAndPath decided we don't need to write edits for {}", logEntry);
              }
              return null;
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
          e = e instanceof RemoteException ?
                  ((RemoteException)e).unwrapRemoteException() : e;
        LOG.error(HBaseMarkers.FATAL, "Got while writing log entry to log", e);
        throw e;
      }
      return wap;
    }

    @Override
    public boolean keepRegionEvent(Entry entry) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      for (Cell cell : cells) {
        if (WALEdit.isCompactionMarker(cell)) {
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
      TreeMap<byte[], Long> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<String, SinkWriter> entry : writers.entrySet()) {
        ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
      }
      return ret;
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return writers.size();
    }
  }

  /**
   *
   */
  class BoundedLogWriterCreationOutputSink extends LogRecoveredEditsOutputSink {

    private ConcurrentHashMap<String, Long> regionRecoverStatMap = new ConcurrentHashMap<>();

    public BoundedLogWriterCreationOutputSink(PipelineController controller,
        EntryBuffers entryBuffers, int numWriters) {
      super(controller, entryBuffers, numWriters);
    }

    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      boolean isSuccessful;
      List<Path> result;
      try {
        isSuccessful = finishWriting(false);
      } finally {
        result = close();
      }
      if (isSuccessful) {
        splits = result;
      }
      return splits;
    }

    @Override
    boolean executeCloseTask(CompletionService<Void> completionService,
        List<IOException> thrown, List<Path> paths)
        throws InterruptedException, ExecutionException {
      for (final Map.Entry<byte[], RegionEntryBuffer> buffer : entryBuffers.buffers.entrySet()) {
        LOG.info("Submitting writeThenClose of {}",
            Arrays.toString(buffer.getValue().encodedRegionName));
        completionService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Path dst = writeThenClose(buffer.getValue());
            paths.add(dst);
            return null;
          }
        });
      }
      boolean progress_failed = false;
      for (int i = 0, n = entryBuffers.buffers.size(); i < n; i++) {
        Future<Void> future = completionService.take();
        future.get();
        if (!progress_failed && reporter != null && !reporter.progress()) {
          progress_failed = true;
        }
      }

      return progress_failed;
    }

    /**
     * since the splitting process may create multiple output files, we need a map
     * regionRecoverStatMap to track the output count of each region.
     * @return a map from encoded region ID to the number of edits written out for that region.
     */
    @Override
    public Map<byte[], Long> getOutputCounts() {
      Map<byte[], Long> regionRecoverStatMapResult = new HashMap<>();
      for(Map.Entry<String, Long> entry: regionRecoverStatMap.entrySet()){
        regionRecoverStatMapResult.put(Bytes.toBytes(entry.getKey()), entry.getValue());
      }
      return regionRecoverStatMapResult;
    }

    /**
     * @return the number of recovered regions
     */
    @Override
    public int getNumberOfRecoveredRegions() {
      return regionRecoverStatMap.size();
    }

    /**
     * Append the buffer to a new recovered edits file, then close it after all done
     * @param buffer contain all entries of a certain region
     * @throws IOException when closeWriter failed
     */
    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      writeThenClose(buffer);
    }

    private Path writeThenClose(RegionEntryBuffer buffer) throws IOException {
      WriterAndPath wap = appendBuffer(buffer, false);
      if(wap != null) {
        String encodedRegionName = Bytes.toString(buffer.encodedRegionName);
        Long value = regionRecoverStatMap.putIfAbsent(encodedRegionName, wap.editsWritten);
        if (value != null) {
          Long newValue = regionRecoverStatMap.get(encodedRegionName) + wap.editsWritten;
          regionRecoverStatMap.put(encodedRegionName, newValue);
        }
      }

      Path dst = null;
      List<IOException> thrown = new ArrayList<>();
      if(wap != null){
        dst = closeWriter(Bytes.toString(buffer.encodedRegionName), wap, thrown);
      }
      if (!thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
      return dst;
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

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;

    CorruptedLogFileException(String s) {
      super(s);
    }
  }

  /** A struct used by getMutationsFromWALEntry */
  public static class MutationReplay implements Comparable<MutationReplay> {
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

    @Override
    public int compareTo(final MutationReplay d) {
      return this.mutation.compareTo(d.mutation);
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof MutationReplay)) {
        return false;
      } else {
        return this.compareTo((MutationReplay)obj) == 0;
      }
    }

    @Override
    public int hashCode() {
      return this.mutation.hashCode();
    }
  }

  /**
   * This function is used to construct mutations from a WALEntry. It also
   * reconstructs WALKey &amp; WALEdit from the passed in WALEntry
   * @param entry
   * @param cells
   * @param logEntry pair of WALKey and WALEdit instance stores WALKey and WALEdit instances
   *          extracted from the passed in WALEntry.
   * @return list of Pair&lt;MutationType, Mutation&gt; to be replayed
   * @throws IOException
   */
  public static List<MutationReplay> getMutationsFromWALEntry(WALEntry entry, CellScanner cells,
      Pair<WALKey, WALEdit> logEntry, Durability durability) throws IOException {
    if (entry == null) {
      // return an empty array
      return Collections.emptyList();
    }

    long replaySeqId = (entry.getKey().hasOrigSequenceNumber()) ?
      entry.getKey().getOrigSequenceNumber() : entry.getKey().getLogSequenceNumber();
    int count = entry.getAssociatedCellCount();
    List<MutationReplay> mutations = new ArrayList<>();
    Cell previousCell = null;
    Mutation m = null;
    WALKeyImpl key = null;
    WALEdit val = null;
    if (logEntry != null) {
      val = new WALEdit();
    }

    for (int i = 0; i < count; i++) {
      // Throw index out of bounds if our cell count is off
      if (!cells.advance()) {
        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
      }
      Cell cell = cells.current();
      if (val != null) val.add(cell);

      boolean isNewRowOrType =
          previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
              || !CellUtil.matchingRows(previousCell, cell);
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
        ((Delete) m).add(cell);
      } else {
        ((Put) m).add(cell);
      }
      m.setDurability(durability);
      previousCell = cell;
    }

    // reconstruct WALKey
    if (logEntry != null) {
      org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey walKeyProto =
          entry.getKey();
      List<UUID> clusterIds = new ArrayList<>(walKeyProto.getClusterIdsCount());
      for (HBaseProtos.UUID uuid : entry.getKey().getClusterIdsList()) {
        clusterIds.add(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
      }
      key = new WALKeyImpl(walKeyProto.getEncodedRegionName().toByteArray(), TableName.valueOf(
              walKeyProto.getTableName().toByteArray()), replaySeqId, walKeyProto.getWriteTime(),
              clusterIds, walKeyProto.getNonceGroup(), walKeyProto.getNonce(), null);
      logEntry.setFirst(key);
      logEntry.setSecond(val);
    }

    return mutations;
  }
}
