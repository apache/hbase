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
package org.apache.hadoop.hbase.wal;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;

/**
 * Split RegionServer WAL files. Splits the WAL into new files,
 * one per region, to be picked up on Region reopen. Deletes the split WAL when finished.
 * Create an instance and call {@link #splitWAL(FileStatus, CancelableProgressable)} per file or
 * use static helper methods.
 */
@InterfaceAudience.Private
public class WALSplitter {
  private static final Logger LOG = LoggerFactory.getLogger(WALSplitter.class);
  public static final String SPLIT_SKIP_ERRORS_KEY = "hbase.hlog.split.skip.errors";

  /**
   * By default we retry errors in splitting, rather than skipping.
   */
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;

  // Parameters for split process
  protected final Path walRootDir;
  protected final FileSystem walFS;
  protected final Configuration conf;
  final Path rootDir;
  final FileSystem rootFS;
  final RegionServerServices rsServices;

  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  OutputSink outputSink;
  private EntryBuffers entryBuffers;

  /**
   * Coordinator for split log. Used by the zk-based log splitter.
   * Not used by the procedure v2-based log splitter.
   */
  private SplitLogWorkerCoordination splitLogWorkerCoordination;

  private final WALFactory walFactory;

  // For checking the latest flushed sequence id
  protected final LastSequenceId sequenceIdChecker;

  // Map encodedRegionName -> lastFlushedSequenceId
  protected Map<String, Long> lastFlushedSequenceIds = new ConcurrentHashMap<>();

  // Map encodedRegionName -> maxSeqIdInStores
  protected Map<String, Map<byte[], Long>> regionMaxSeqIdInStores = new ConcurrentHashMap<>();

  // the file being split currently
  private FileStatus fileBeingSplit;

  private final String tmpDirName;

  /**
   * Split WAL directly to hfiles instead of into intermediary 'recovered.edits' files.
   */
  public static final String WAL_SPLIT_TO_HFILE = "hbase.wal.split.to.hfile";
  public static final boolean DEFAULT_WAL_SPLIT_TO_HFILE = false;

  /**
   * True if we are to run with bounded amount of writers rather than let the count blossom.
   * Default is 'false'. Does not apply if you have set 'hbase.wal.split.to.hfile' as that
   * is always bounded. Only applies when you are doing recovery to 'recovered.edits'
   * files (the old default). Bounded writing tends to have higher throughput.
   */
  public final static String SPLIT_WRITER_CREATION_BOUNDED = "hbase.split.writer.creation.bounded";

  public final static String SPLIT_WAL_BUFFER_SIZE = "hbase.regionserver.hlog.splitlog.buffersize";
  public final static String SPLIT_WAL_WRITER_THREADS =
    "hbase.regionserver.hlog.splitlog.writer.threads";

  private final int numWriterThreads;
  private final long bufferSize;
  private final boolean splitWriterCreationBounded;
  private final boolean hfile;
  private final boolean skipErrors;

  WALSplitter(final WALFactory factory, Configuration conf, Path walRootDir,
      FileSystem walFS, Path rootDir, FileSystem rootFS) {
    this(factory, conf, walRootDir, walFS, rootDir, rootFS, null, null, null);
  }

  WALSplitter(final WALFactory factory, Configuration conf, Path walRootDir,
      FileSystem walFS, Path rootDir, FileSystem rootFS, LastSequenceId idChecker,
      SplitLogWorkerCoordination splitLogWorkerCoordination, RegionServerServices rsServices) {
    this.conf = HBaseConfiguration.create(conf);
    String codecClassName =
      conf.get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, codecClassName);
    this.walRootDir = walRootDir;
    this.walFS = walFS;
    this.rootDir = rootDir;
    this.rootFS = rootFS;
    this.sequenceIdChecker = idChecker;
    this.splitLogWorkerCoordination = splitLogWorkerCoordination;
    this.rsServices = rsServices;
    this.walFactory = factory;
    this.tmpDirName =
      conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY, HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    // if we limit the number of writers opened for sinking recovered edits
    this.splitWriterCreationBounded = conf.getBoolean(SPLIT_WRITER_CREATION_BOUNDED, false);
    this.bufferSize = this.conf.getLong(SPLIT_WAL_BUFFER_SIZE, 128 * 1024 * 1024);
    this.numWriterThreads = this.conf.getInt(SPLIT_WAL_WRITER_THREADS, 3);
    this.hfile = conf.getBoolean(WAL_SPLIT_TO_HFILE, DEFAULT_WAL_SPLIT_TO_HFILE);
    this.skipErrors = conf.getBoolean(SPLIT_SKIP_ERRORS_KEY, SPLIT_SKIP_ERRORS_DEFAULT);
  }

  WALFactory getWalFactory() {
    return this.walFactory;
  }

  FileStatus getFileBeingSplit() {
    return fileBeingSplit;
  }

  String getTmpDirName() {
    return this.tmpDirName;
  }

  Map<String, Map<byte[], Long>> getRegionMaxSeqIdInStores() {
    return regionMaxSeqIdInStores;
  }

  /**
   * Splits a WAL file.
   * Used by old {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker} and tests.
   * Not used by new procedure-based WAL splitter.
   *
   * @return false if it is interrupted by the progress-able.
   */
  public static boolean splitLogFile(Path walDir, FileStatus logfile, FileSystem walFS,
      Configuration conf, CancelableProgressable reporter, LastSequenceId idChecker,
      SplitLogWorkerCoordination splitLogWorkerCoordination, WALFactory factory,
      RegionServerServices rsServices) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem rootFS = rootDir.getFileSystem(conf);
    WALSplitter splitter = new WALSplitter(factory, conf, walDir, walFS, rootDir, rootFS, idChecker,
      splitLogWorkerCoordination, rsServices);
    // splitWAL returns a data structure with whether split is finished and if the file is corrupt.
    // We don't need to propagate corruption flag here because it is propagated by the
    // SplitLogWorkerCoordination.
    return splitter.splitWAL(logfile, reporter).isFinished();
  }

  /**
   * Split a folder of WAL files. Delete the directory when done.
   * Used by tools and unit tests. It should be package private.
   * It is public only because TestWALObserver is in a different package,
   * which uses this method to do log splitting.
   * @return List of output files created by the split.
   */
  public static List<Path> split(Path walRootDir, Path walsDir, Path archiveDir, FileSystem walFS,
      Configuration conf, final WALFactory factory) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem rootFS = rootDir.getFileSystem(conf);
    WALSplitter splitter = new WALSplitter(factory, conf, walRootDir, walFS, rootDir, rootFS);
    final List<FileStatus> wals =
      SplitLogManager.getFileList(conf, Collections.singletonList(walsDir), null);
    List<Path> splits = new ArrayList<>();
    if (!wals.isEmpty()) {
      for (FileStatus wal: wals) {
        SplitWALResult splitWALResult = splitter.splitWAL(wal, null);
        if (splitWALResult.isFinished()) {
          WALSplitUtil.archive(wal.getPath(), splitWALResult.isCorrupt(), archiveDir, walFS, conf);
          //splitter.outputSink.splits is mark as final, do not need null check
          splits.addAll(splitter.outputSink.splits);
        }
      }
    }
    if (!walFS.delete(walsDir, true)) {
      throw new IOException("Unable to delete src dir " + walsDir);
    }
    return splits;
  }

  /**
   * Data structure returned as result by #splitWAL(FileStatus, CancelableProgressable).
   * Test {@link #isFinished()} to see if we are done with the WAL and {@link #isCorrupt()} for if
   * the WAL is corrupt.
   */
  static final class SplitWALResult {
    private final boolean finished;
    private final boolean corrupt;

    private SplitWALResult(boolean finished, boolean corrupt) {
      this.finished = finished;
      this.corrupt = corrupt;
    }

    public boolean isFinished() {
      return finished;
    }

    public boolean isCorrupt() {
      return corrupt;
    }
  }

  /**
   * Setup the output sinks and entry buffers ahead of splitting WAL.
   */
  private void createOutputSinkAndEntryBuffers() {
    PipelineController controller = new PipelineController();
    if (this.hfile) {
      this.entryBuffers = new BoundedEntryBuffers(controller, this.bufferSize);
      this.outputSink = new BoundedRecoveredHFilesOutputSink(this, controller,
        this.entryBuffers, this.numWriterThreads);
    } else if (this.splitWriterCreationBounded) {
      this.entryBuffers = new BoundedEntryBuffers(controller, this.bufferSize);
      this.outputSink = new BoundedRecoveredEditsOutputSink(this, controller,
        this.entryBuffers, this.numWriterThreads);
    } else {
      this.entryBuffers = new EntryBuffers(controller, this.bufferSize);
      this.outputSink = new RecoveredEditsOutputSink(this, controller,
        this.entryBuffers, this.numWriterThreads);
    }
  }

  /**
   * WAL splitting implementation, splits one WAL file.
   * @param walStatus should be for an actual WAL file.
   */
  SplitWALResult splitWAL(FileStatus walStatus, CancelableProgressable cancel) throws IOException {
    Path wal = walStatus.getPath();
    Preconditions.checkArgument(walStatus.isFile(), "Not a regular file " + wal.toString());
    boolean corrupt = false;
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    boolean outputSinkStarted = false;
    boolean cancelled = false;
    int editsCount = 0;
    int editsSkipped = 0;
    MonitoredTask status =
      TaskMonitor.get().createStatus("Splitting " + wal + " to temporary staging area.");
    status.enableStatusJournal(true);
    Reader walReader = null;
    this.fileBeingSplit = walStatus;
    long startTS = EnvironmentEdgeManager.currentTime();
    long length = walStatus.getLen();
    String lengthStr = StringUtils.humanSize(length);
    createOutputSinkAndEntryBuffers();
    try {
      String logStr = "Splitting " + wal + ", size=" + lengthStr + " (" + length + "bytes)";
      LOG.info(logStr);
      status.setStatus(logStr);
      if (cancel != null && !cancel.progress()) {
        cancelled = true;
        return new SplitWALResult(false, corrupt);
      }
      walReader = getReader(walStatus, this.skipErrors, cancel);
      if (walReader == null) {
        LOG.warn("Nothing in {}; empty?", wal);
        return new SplitWALResult(true, corrupt);
      }
      LOG.info("Open {} took {}ms", wal, EnvironmentEdgeManager.currentTime() - startTS);
      int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
      int numOpenedFilesLastCheck = 0;
      outputSink.setReporter(cancel);
      outputSink.setStatus(status);
      outputSink.startWriterThreads();
      outputSinkStarted = true;
      Entry entry;
      startTS = EnvironmentEdgeManager.currentTime();
      while ((entry = getNextLogLine(walReader, wal, this.skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        Long lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (!(isRegionDirPresentUnderRoot(entry.getKey().getTableName(),
              encodedRegionNameAsStr))) {
            // The region directory itself is not present in the FS. This indicates that
            // the region/table is already removed. We can just skip all the edits for this
            // region. Setting lastFlushedSequenceId as Long.MAX_VALUE so that all edits
            // will get skipped by the seqId check below.
            // See more details at https://issues.apache.org/jira/browse/HBASE-24189
            LOG.info("{} no longer in filesystem; skipping all edits.", encodedRegionNameAsStr);
            lastFlushedSequenceId = Long.MAX_VALUE;
          } else {
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
                LOG.debug("Last flushed sequenceid for " + encodedRegionNameAsStr + ": "
                    + TextFormat.shortDebugString(ids));
              }
            }
            if (lastFlushedSequenceId == null) {
              lastFlushedSequenceId = -1L;
            }
          }
          lastFlushedSequenceIds.put(encodedRegionNameAsStr, lastFlushedSequenceId);
        }
        editsCount++;
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
        int moreWritersFromLastCheck = this.getNumOpenWriters() - numOpenedFilesLastCheck;
        // If sufficient edits have passed, check if we should report progress.
        if (editsCount % interval == 0
            || moreWritersFromLastCheck > numOpenedFilesBeforeReporting) {
          numOpenedFilesLastCheck = this.getNumOpenWriters();
          String countsStr = (editsCount - (editsSkipped + outputSink.getTotalSkippedEdits()))
              + " edits, skipped " + editsSkipped + " edits.";
          status.setStatus("Split " + countsStr);
          if (cancel != null && !cancel.progress()) {
            cancelled = true;
            return new SplitWALResult(false, corrupt);
          }
        }
      }
    } catch (InterruptedException ie) {
      IOException iie = new InterruptedIOException();
      iie.initCause(ie);
      throw iie;
    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not parse, corrupt WAL={}", wal, e);
      // If splitLogWorkerCoordination, then its old-school zk-coordinated splitting so update
      // zk. Otherwise, it is the newer procedure-based WAL split which has no zk component.
      if (this.splitLogWorkerCoordination != null) {
        // Some tests pass in a csm of null.
        splitLogWorkerCoordination.markCorrupted(walRootDir, wal.getName(), walFS);
      }
      corrupt = true;
    } catch (IOException e) {
      e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
      throw e;
    } finally {
      final String log = "Finishing writing output for " + wal + " so closing down";
      LOG.debug(log);
      status.setStatus(log);
      try {
        if (null != walReader) {
          walReader.close();
        }
      } catch (IOException exception) {
        LOG.warn("Could not close {} reader", wal, exception);
      }
      try {
        if (outputSinkStarted) {
          // Set cancelled to true as the immediate following statement will reset its value.
          // If close() throws an exception, cancelled will have the right value
          cancelled = true;
          cancelled = outputSink.close() == null;
        }
      } finally {
        long processCost = EnvironmentEdgeManager.currentTime() - startTS;
        // See if length got updated post lease recovery
        String msg = "Processed " + editsCount + " edits across " +
          outputSink.getNumberOfRecoveredRegions() + " Regions in " + processCost +
          " ms; skipped=" + editsSkipped + "; WAL=" + wal + ", size=" + lengthStr +
          ", length=" + length + ", corrupted=" + corrupt + ", cancelled=" + cancelled;
        LOG.info(msg);
        status.markComplete(msg);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed split of {}, journal: {}", wal, status.prettyPrintJournal());
        }
      }
    }
    return new SplitWALResult(!cancelled, corrupt);
  }

  private boolean isRegionDirPresentUnderRoot(TableName tn, String region) throws IOException {
    return this.rootFS.exists(CommonFSUtils.getRegionDir(this.rootDir, tn, region));
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return Returns null if file has length zero or file can't be found.
   */
  protected Reader getReader(FileStatus walStatus, boolean skipErrors, CancelableProgressable cancel)
      throws IOException, CorruptedLogFileException {
    Path path = walStatus.getPath();
    long length = walStatus.getLen();
    Reader in;

    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File {} might be still open, length is 0", path);
    }

    try {
      RecoverLeaseFSUtils.recoverFileLease(walFS, path, conf, cancel);
      try {
        in = getReader(path, cancel);
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
      throw new CorruptedLogFileException("skipErrors=true; could not open " + path +
        ", skipping", e);
    }
    return in;
  }

  private Entry getNextLogLine(Reader in, Path path, boolean skipErrors)
      throws CorruptedLogFileException, IOException {
    try {
      return in.next();
    } catch (EOFException eof) {
      // truncated files are expected if a RS crashes (see HBASE-2643)
      LOG.info("EOF from {}; continuing.", path);
      return null;
    } catch (IOException e) {
      // If the IOE resulted from bad file format,
      // then this problem is idempotent and retrying won't help
      if (e.getCause() != null && (e.getCause() instanceof ParseException
          || e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception from {}; continuing", path, e);
        return null;
      }
      if (!skipErrors) {
        throw e;
      }
      throw new CorruptedLogFileException("skipErrors=true Ignoring exception"
        + " while parsing wal " + path + ". Marking as corrupted", e);
    }
  }

  /**
   * Create a new {@link WALProvider.Writer} for writing log splits.
   * @return a new Writer instance, caller should close
   */
  protected WALProvider.Writer createWriter(Path logfile) throws IOException {
    return walFactory.createRecoveredEditsWriter(walFS, logfile);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  private Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
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
    final Object dataAvailable = new Object();

    void writerThreadError(Throwable t) {
      thrown.compareAndSet(null, t);
    }

    /**
     * Check for errors in the writer threads. If any is found, rethrow it.
     */
    void checkForErrors() throws IOException {
      Throwable thrown = this.thrown.get();
      if (thrown == null) {
        return;
      }
      this.thrown.set(null);
      if (thrown instanceof IOException) {
        throw new IOException(thrown);
      } else {
        throw new RuntimeException(thrown);
      }
    }
  }

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;

    CorruptedLogFileException(String s) {
      super(s);
    }

    /**
     * CorruptedLogFileException with cause
     *
     * @param message the message for this exception
     * @param cause the cause for this exception
     */
    CorruptedLogFileException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
