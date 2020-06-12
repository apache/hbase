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

import static org.apache.hadoop.hbase.wal.WALSplitUtil.finishSplitLogFile;

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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
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
  protected final Path rootDir;
  protected final FileSystem walFS;
  protected final FileSystem rootFS;
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
  WALSplitter(final WALFactory factory, Configuration conf, Path walDir, FileSystem walFS,
      Path rootDir, FileSystem rootFS, LastSequenceId idChecker,
      SplitLogWorkerCoordination splitLogWorkerCoordination) {
    this.conf = HBaseConfiguration.create(conf);
    String codecClassName =
        conf.get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, codecClassName);
    this.walDir = walDir;
    this.walFS = walFS;
    this.rootDir = rootDir;
    this.rootFS = rootFS;
    this.sequenceIdChecker = idChecker;
    this.splitLogWorkerCoordination = splitLogWorkerCoordination;

    this.walFactory = factory;
    PipelineController controller = new PipelineController();

    this.splitWriterCreationBounded = conf.getBoolean(SPLIT_WRITER_CREATION_BOUNDED, false);

    entryBuffers = new EntryBuffers(controller,
        this.conf.getLong("hbase.regionserver.hlog.splitlog.buffersize", 128 * 1024 * 1024),
        splitWriterCreationBounded);

    int numWriterThreads = this.conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    if (splitWriterCreationBounded) {
      outputSink =
          new BoundedLogWriterCreationOutputSink(this, controller, entryBuffers, numWriterThreads);
    } else {
      outputSink =
          new LogRecoveredEditsOutputSink(this, controller, entryBuffers, numWriterThreads);
    }
  }

  WALFactory getWalFactory(){
    return this.walFactory;
  }

  FileStatus getFileBeingSplit() {
    return fileBeingSplit;
  }

  Map<String, Map<byte[], Long>> getRegionMaxSeqIdInStores() {
    return regionMaxSeqIdInStores;
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
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem rootFS = rootDir.getFileSystem(conf);
    WALSplitter s = new WALSplitter(factory, conf, walDir, walFS, rootDir, rootFS, idChecker,
        splitLogWorkerCoordination);
    return s.splitLogFile(logfile, reporter);
  }

  // A wrapper to split one log folder using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because TestWALObserver is in a different package,
  // which uses this method to do log splitting.
  @VisibleForTesting
  public static List<Path> split(Path walDir, Path logDir, Path oldLogDir,
      FileSystem walFS, Configuration conf, final WALFactory factory) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem rootFS = rootDir.getFileSystem(conf);
    final FileStatus[] logfiles = SplitLogManager.getFileList(conf,
        Collections.singletonList(logDir), null);
    List<Path> splits = new ArrayList<>();
    if (ArrayUtils.isNotEmpty(logfiles)) {
      for (FileStatus logfile: logfiles) {
        WALSplitter s = new WALSplitter(factory, conf, walDir, walFS, rootDir, rootFS, null, null);
        if (s.splitLogFile(logfile, null)) {
          finishSplitLogFile(walDir, oldLogDir, logfile.getPath(), conf);
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
    long startTS = EnvironmentEdgeManager.currentTime();
    try {
      long logLength = logfile.getLen();
      LOG.info("Splitting WAL={}, size={} ({} bytes)", logPath, StringUtils.humanSize(logLength),
          logLength);
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
      long openCost = EnvironmentEdgeManager.currentTime() - startTS;
      LOG.info("Open WAL={} cost {} ms", logPath, openCost);
      int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
      int numOpenedFilesLastCheck = 0;
      outputSink.setReporter(reporter);
      outputSink.startWriterThreads();
      outputSinkStarted = true;
      Entry entry;
      Long lastFlushedSequenceId = -1L;
      startTS = EnvironmentEdgeManager.currentTime();
      while ((entry = getNextLogLine(logFileReader, logPath, skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (!(isRegionDirPresentUnderRoot(entry.getKey().getTableName(),
              encodedRegionNameAsStr))) {
            // The region directory itself is not present in the FS. This indicates that
            // region/table is already removed. We can skip all the edits for this region.
            // Setting lastFlushedSequenceId as Long.MAX_VALUE so that all edits will get
            // skipped by the seqId check below. See more details in HBASE-24189
            LOG.info("{} no longer available in the FS. Skipping all edits for this region.",
                encodedRegionNameAsStr);
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
                LOG.debug("DLS Last flushed sequenceid for " + encodedRegionNameAsStr + ": "
                    + TextFormat.shortDebugString(ids));
              }
            }
            if (lastFlushedSequenceId == null) {
              lastFlushedSequenceId = -1L;
            }
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
        long processCost = EnvironmentEdgeManager.currentTime() - startTS;
        // See if length got updated post lease recovery
        String msg = "Processed " + editsCount + " edits across " +
            outputSink.getNumberOfRecoveredRegions() + " regions cost " + processCost +
            " ms; edits skipped=" + editsSkipped + "; WAL=" + logPath + ", size=" +
            StringUtils.humanSize(logfile.getLen()) + ", length=" + logfile.getLen() +
            ", corrupted=" + isCorrupted + ", progress failed=" + progress_failed;
        LOG.info(msg);
        status.markComplete(msg);
      }
    }
    return !progress_failed;
  }

  private boolean isRegionDirPresentUnderRoot(TableName tableName, String regionName)
      throws IOException {
    Path regionDirPath = CommonFSUtils.getRegionDir(this.rootDir, tableName, regionName);
    return this.rootFS.exists(regionDirPath);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
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
      if (e.getCause() != null && (e.getCause() instanceof ParseException
          || e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception from wal {}. Continuing", path, e);
        return null;
      }
      if (!skipErrors) {
        throw e;
      }
      CorruptedLogFileException t =
          new CorruptedLogFileException("skipErrors=true Ignoring exception" + " while parsing wal "
              + path + ". Marking as corrupted");
      t.initCause(e);
      throw t;
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
  final static class WriterAndPath extends SinkWriter {
    final Path path;
    final Writer writer;
    final long minLogSeqNum;

    WriterAndPath(final Path path, final Writer writer, final long minLogSeqNum) {
      this.path = path;
      this.writer = writer;
      this.minLogSeqNum = minLogSeqNum;
    }
  }

  static class CorruptedLogFileException extends Exception {
    private static final long serialVersionUID = 1L;

    CorruptedLogFileException(String s) {
      super(s);
    }
  }
}
