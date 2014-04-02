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

import static org.apache.hadoop.hbase.util.FSUtils.recoverFileLease;
import static org.apache.hadoop.hbase.util.FSUtils.tryRecoverFileLease;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.RuntimeExceptionAbortStrategy;
import org.apache.hadoop.hbase.util.RuntimeHaltAbortStrategy;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HLog stores all the edits to the HStore.  Its the hbase write-ahead-log
 * implementation.
 *
 * It performs logfile-rolling, so external callers are not aware that the
 * underlying file is being rolled.
 *
 * <p>
 * There is one HLog per RegionServer.  All edits for all Regions carried by
 * a particular RegionServer are entered first in the HLog.
 *
 * <p>
 * Each HRegion is identified by a unique long <code>int</code>. HRegions do
 * not need to declare themselves before using the HLog; they simply include
 * their HRegion-id in the <code>append</code> or
 * <code>completeCacheFlush</code> calls.
 *
 * <p>
 * An HLog consists of multiple on-disk files, which have a chronological order.
 * As data is flushed to other (better) on-disk structures, the log becomes
 * obsolete. We can destroy all the log messages for a given HRegion-id up to
 * the most-recent CACHEFLUSH message from that HRegion.
 *
 * <p>
 * It's only practical to delete entire files. Thus, we delete an entire on-disk
 * file F when all of the messages in F have a log-sequence-id that's older
 * (smaller) than the most-recent CACHEFLUSH message for every HRegion that has
 * a message in F.
 *
 * <p>
 * Synchronized methods can never execute in parallel. However, between the
 * start of a cache flush and the completion point, appends are allowed but log
 * rolling is not. To prevent log rolling taking place during this period, a
 * separate reentrant lock is used.
 *
 * <p>To read an HLog, call {@link #getReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path, org.apache.hadoop.conf.Configuration)}.
 *
 */
public class HLog implements Syncable {
  static final Log LOG = LogFactory.getLog(HLog.class);
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  public static final byte [] METAROW = Bytes.toBytes("METAROW");
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH");

  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  private static final String RECOVERED_EDITS_DIR = "recovered.edits";
  private static final Pattern EDITFILES_NAME_PATTERN =
    Pattern.compile("-?[0-9]+");

  /** We include all timestamps by default */
  public static final long DEFAULT_LATEST_TS_TO_INCLUDE = Long.MAX_VALUE;

  // If enabled, old logs will be archived into hourly sub-directories instead of
  // server address sub-directories.
  private static boolean ARCHIVE_TO_HOURLY_DIR = false;

  private final FileSystem fs;
  private final Path dir;
  private final Configuration conf;
  private final boolean perColumnFamilyFlushEnabled;
  private final LogRollListener listener;
  private final long optionalFlushInterval;
  private final long blocksize;
  private final int flushlogentries;
  private final String prefix;
  private final AtomicLong unflushedEntries = new AtomicLong(0);
  private final Path oldLogDir;
  private volatile long syncTillHere = 0;
  private final List<LogActionsListener> actionListeners =
      Collections.synchronizedList(new ArrayList<LogActionsListener>());


  private static Class<? extends Writer> logWriterClass;
  private static Class<? extends Reader> logReaderClass;

  private OutputStream hdfs_out;     // OutputStream associated with the current SequenceFile.writer
  private int initialReplication;    // initial replication factor of SequenceFile.writer
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

  private final Abortable syncFailureAbortStrategy;

  // used to indirectly tell syncFs to force the sync
  private volatile boolean forceSync = false;

  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
    boolean isComplete() throws IOException;
  }

  public interface Writer {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    void sync() throws IOException;
    void append(Entry entry) throws IOException;
    long getLength() throws IOException;
  }

  /*
   * Current log file.
   */
  Writer writer;
  boolean writerCloseSyncDone = false;

  /*
   * Map of all log files but the current one.
   */
  final SortedMap<Long, Path> outputfiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, Path>());

  /**
   * The purpose of this lock is to synchronize operations on firstSeqWrittenInCurrentMemstore
   * and firstSeqWrittenInSnapshotMemstore, so that the data transfer and deletion in cache flushing
   * won't cause log rolling to behave wrongly, for example, achieving all old log files which may
   * still contain unflushed entries.
   */
  private final Object oldestSeqNumsLock = new Object();

  /** The lock is used to ensure that close() waits for all log rolls and flushes to finish. */
  private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

  /*
   * Map of regions to first sequence/edit id in their memstore.
   */
  private final ConcurrentNavigableMap<byte [], Long> firstSeqWrittenInCurrentMemstore =
    new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
  private final ConcurrentNavigableMap<byte [], Long> firstSeqWrittenInSnapshotMemstore =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  // The timestamp (in ms) when the log file was created.
  private volatile long filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
  private final long logrollsize;

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  private final Object updateLock = new Object();

  // Lock to guarantee the ordering of log entries in HLOG
  private final Object appendLock = new Object();

  private final boolean enabled;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  private final int hlogIndexID;
  private final String hlogName;

  /**
   * Thread that handles group commit
   */
  private final LogSyncer logSyncerThread;

  private final List<LogEntryVisitor> logEntryVisitors =
      new CopyOnWriteArrayList<LogEntryVisitor>();

  private volatile long lastLogRollStartTimeMillis = 0;
  private volatile long lastLogRollDurationMillis = 0;

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*");

  private static final FileStatus[] NO_FILES = new FileStatus[0];
  protected static final byte[] DUMMY = Bytes.toBytes("");

  public static class Metric {
    public long min = Long.MAX_VALUE;
    public long max = 0;
    public long total = 0;
    public int count = 0;

    synchronized void inc(final long val) {
      min = Math.min(min, val);
      max = Math.max(max, val);
      total += val;
      ++count;
    }

    synchronized Metric get() {
      Metric copy = new Metric();
      copy.min = min;
      copy.max = max;
      copy.total = total;
      copy.count = count;
      this.min = Long.MAX_VALUE;
      this.max = 0;
      this.total = 0;
      this.count = 0;
      return copy;
    }
  }

  // For measuring latency of writes
  private static Metric writeTime = new Metric();
  private static Metric writeSize = new Metric();
  // For measuring latency of syncs
  private static Metric syncTime = new Metric();
  private static Metric gsyncTime = new Metric();

  public static volatile long lastSplitTime = 0;
  public static volatile long lastSplitSize = 0;

  public static Metric getWriteTime() {
    return writeTime.get();
  }

  public static Metric getWriteSize() {
    return writeSize.get();
  }

  public static Metric getSyncTime() {
    return syncTime.get();
  }

  public static Metric getGSyncTime() {
    return gsyncTime.get();
  }

  /**
   * Double list buffer for WAL that allows entries to be
   * appended while sync is in progress
   *
   * CurrentList is for buffering appended entries;
   * syncList contains entries being synced to persistent storage;
   */
  private class DoubleListBuffer {
    private LinkedList<Entry> currentList = new LinkedList<Entry>();
    private LinkedList<Entry> syncList = new LinkedList<Entry>();

    /**
     * Append a log entry into the buffer
     * @param entry log entry
     */
    synchronized private void appendToBuffer(Entry entry) {
      currentList.add(entry);
    }

    /**
     * Sync buffered log entries into persistent storage
     *
     * @return number of log entries synced
     */
    private int appendAndSync() throws IOException {
      synchronized (this) {
        if (currentList.isEmpty()) { // no thing to sync
          return 0;
        }

        // otherwise swap the buffer in preparation for sync
        assert syncList.isEmpty();
        LinkedList<Entry> tmp = syncList;
        syncList = currentList;
        currentList = tmp;
      }

      // append entries to writer
      int syncedEntries = syncList.size();
      while (!syncList.isEmpty()) {
        Entry entry = syncList.remove();
        writer.append(entry);
      }

      // sync the data
      long now = System.currentTimeMillis();
      writer.sync();
      syncTime.inc(System.currentTimeMillis() - now);
      return syncedEntries;
    }
  }

  private DoubleListBuffer logBuffer = new DoubleListBuffer();

  /**
   * HLog creating with a null actions listener.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listener listerner used to request log rolls
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf, final LogRollListener listener)
  throws IOException {
    this(fs, dir, oldLogDir, conf, listener, null, null, 0, 1);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listener listerner used to request log rolls
   * @param actionListener optional listener for hlog actions like archiving
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @param hlogIndexID the index ID for the current HLog
   * @param totalHLogCnt the total number of HLog in the current region server
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf, final LogRollListener listener,
              final LogActionsListener actionListener, final String prefix, int hlogIndexID,
              int totalHLogCnt)
  throws IOException {
    super();
    syncFailureAbortStrategy = conf.getBoolean("hbase.hlog.sync.failure.abort.process", true) ?
        RuntimeHaltAbortStrategy.INSTANCE : RuntimeExceptionAbortStrategy.INSTANCE;
    this.fs = fs;
    this.conf = conf;
    this.perColumnFamilyFlushEnabled = conf.getBoolean(
            HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH,
            HConstants.DEFAULT_HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH);
    this.listener = listener;
    this.flushlogentries =
      conf.getInt("hbase.regionserver.flushlogentries", 1);
    this.blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
      this.fs.getDefaultBlockSize());
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);

    if (!fs.exists(oldLogDir)) {
      fs.mkdirs(oldLogDir);
    }
    this.oldLogDir = oldLogDir;
    ARCHIVE_TO_HOURLY_DIR = conf.getBoolean("hbase.hlog.archive.hourlydir", false);

    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    this.dir = dir;

    this.hlogIndexID = hlogIndexID;
    this.hlogName = "HLog-" + this.hlogIndexID + " ";

    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    LOG.info("HLog configuration: blocksize=" + this.blocksize +
      ", rollsize=" + this.logrollsize +
      ", enabled=" + this.enabled +
      ", flushlogentries=" + this.flushlogentries +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    if (actionListener != null) {
      addLogActionsListerner(actionListener);
    }

    // If prefix is null||empty, then just name it hlog.
    if (conf.getBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, true)) {
      this.prefix = prefix == null || prefix.isEmpty() ? "hlog" : URLEncoder.encode(prefix, "UTF8");
      LOG.warn("Still using old hlog prefix due to HLOG_FORMAT_BACK_COMPATIBILITY: " + this.prefix);
    } else {
      // Also append the current hlogIndexId-totalHLogCnt to the prefix.
      this.prefix = (prefix == null || prefix.isEmpty() ?
          "hlog" : URLEncoder.encode(prefix, "UTF8"))
          + "." + hlogIndexID + "-" + totalHLogCnt;
      LOG.info("HLog prefix is " + this.prefix);
    }

    rollWriter();

    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = null;
    if(this.hdfs_out != null) {
      try {
        this.getNumCurrentReplicas = this.hdfs_out.getClass().
          getMethod("getNumCurrentReplicas", new Class<?> []{});
        this.getNumCurrentReplicas.setAccessible(true);
      } catch (NoSuchMethodException e) {
        // Thrown if getNumCurrentReplicas() function isn't available
      } catch (SecurityException e) {
        // Thrown if we can't get access to getNumCurrentReplicas()
        this.getNumCurrentReplicas = null; // could happen on setAccessible()
      }
    }
    if(this.getNumCurrentReplicas != null) {
      LOG.info("Using getNumCurrentReplicas--HDFS-826");
    } else {
      LOG.info("getNumCurrentReplicas--HDFS-826 not available" );
    }

    logSyncerThread = new LogSyncer(this.optionalFlushInterval);
    Threads.setDaemonThreadRunning(logSyncerThread.getThread(),
        Thread.currentThread().getName() + ".logSyncer-" + hlogIndexID);
  }

  /**
   * @return Current state of the monotonically increasing file id.
   */
  public long getFilenum() {
    return this.filenum;
  }

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   *
   * @param newvalue We'll set log edit/sequence number to this value if it
   * is greater than the current value.
   */
  public void setSequenceNumber(final long newvalue) {
    for (long id = this.logSeqNum.get(); id < newvalue &&
        !this.logSeqNum.compareAndSet(id, newvalue); id = this.logSeqNum.get()) {
      // This could spin on occasion but better the occasional spin than locking
      // every increment of sequence number.
      LOG.debug(hlogName + "Change sequence number from " + logSeqNum + " to " + newvalue);
    }
  }

  /**
   * @return log sequence number
   */
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  // usage: see TestLogRolling.java
  OutputStream getOutputStream() {
    return this.hdfs_out;
  }

  /**
   * @return list of all log files
   */
  public List<String> getHLogsList() {
    List<String> hLogsList = new ArrayList<String>(this.outputfiles.size());
    for (Path hlog : this.outputfiles.values()) {
      hLogsList.add(hlog.toString());
    }
    return hLogsList;
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * This method is synchronized because only one log rolling is allowed at the
   * same time.
   *
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public synchronized byte [][] rollWriter() throws IOException {
    // Return if nothing to flush.
    if (this.closed || this.writer != null && this.numEntries.get() <= 0) {
      LOG.debug(this.hlogName + "HLog closing or no outstanding transactions. Skipping log rolling");
      return null;
    }
    byte [][] regionsToFlush = null;
    // Creating the new file can be done earlier.
    long newFileNum = System.currentTimeMillis();
    Path newPath = computeFilename(newFileNum);
    Writer newWriter;
    int newFileReplication;
    OutputStream newOutStream = null;
    try {
      newWriter = createWriter(fs, newPath, conf);
      newFileReplication = fs.getFileStatus(newPath).getReplication();
      if (newWriter instanceof SequenceFileLogWriter) {
        newOutStream = ((SequenceFileLogWriter) newWriter)
            .getDFSCOutputStream();
        // append a dummy entry and sync. So we perform the costly
        // allocateBlock and sync before we get the lock to roll writers.
        WALEdit edit = new WALEdit();
        HLogKey key = makeKey(DUMMY /* regionName */, DUMMY /* tableName */,
            0, System.currentTimeMillis());
        newWriter.append(new HLog.Entry(key, edit));
        syncWriter(newWriter);
      }
    } catch (IOException ioe) {
      // If we fail to create a new writer, let us clean up the file.
      // Do not worry if the delete fails.
      fs.delete(newPath, false);
      throw ioe;
    }

    this.closeLock.readLock().lock();
    try {
      Writer oldWriter;
      long oldFileLogSeqNum, oldFileNum;
      int oldNumEntries;
      long t0 = 0;
      long t1 = 0;
      synchronized (updateLock) {
        // Ensure hlog is not closed once more since we don't want to enlarge the
        //   scope of updateLock.
        if (this.closed) {
          LOG.debug(this.hlogName + "HLog closing. Skipping log rolling");
          return null;
        }
        t0 = EnvironmentEdgeManager.currentTimeMillis();

        // Clean up current writer.
        syncWriter(this.writer);

        // save the old information required for close()
        oldFileNum = this.filenum;
        oldWriter = this.writer;
        oldNumEntries = this.numEntries.get();
        oldFileLogSeqNum = this.logSeqNum.get();

        this.filenum = newFileNum;
        this.writer = newWriter;
        this.initialReplication = newFileReplication;
        this.hdfs_out = newOutStream;
        this.numEntries.set(0);

        t1 = EnvironmentEdgeManager.currentTimeMillis();
        lastLogRollStartTimeMillis = t0;
        lastLogRollDurationMillis = (t1 - t0);
      }

      Path oldFile = null;
      try {
        oldFile = closeWriter(oldWriter, oldFileNum, oldFileLogSeqNum);
      } catch (IOException e) {
        LOG.info("Ignoring Exception while closing old Log file "
            + FSUtils.getPath(oldFile), e);
      }

      LOG.info(hlogName + (oldFile != null ? "Roll " + FSUtils.getPath(oldFile)
          + ", entries=" + oldNumEntries + ", filesize="
          + this.fs.getFileStatus(oldFile).getLen() + ". " : "")
          + "New hlog " + FSUtils.getPath(newPath)
          + " Held updateLock for " + (t1 -t0) + " ms.");
      // Tell our listeners that a new log was created
      if (!this.actionListeners.isEmpty()) {
        for (LogActionsListener list : this.actionListeners) {
          list.logRolled(newPath);
        }
      }

      // Can we delete any of the old log files?
      if (this.outputfiles.size() > 0) {
        regionsToFlush = cleanOldLogs(getOldestOutstandingSeqNum());
      }
    } finally {
      this.closeLock.readLock().unlock();
    }

    return regionsToFlush;
  }

  /**
   * Get a reader for the WAL.
   * @param fs
   * @param path
   * @param conf
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public static Reader getReader(final FileSystem fs,
    final Path path, Configuration conf)
  throws IOException {
    try {
      if (logReaderClass == null) {
        logReaderClass =conf.getClass("hbase.regionserver.hlog.reader.impl",
                SequenceFileLogReader.class, Reader.class);
      }

      HLog.Reader reader = logReaderClass.newInstance();
      reader.init(fs, path, conf);
      return reader;
    } catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Get a writer for the WAL.
   * @param path
   * @param conf
   * @return A WAL writer.  Close when done with it.
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs,
      final Path path, Configuration conf)
  throws IOException {
    try {
      if (logWriterClass == null) {
        logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
                SequenceFileLogWriter.class, Writer.class);
      }
      HLog.Writer writer = (HLog.Writer) logWriterClass.newInstance();
      writer.init(fs, path, conf);
      return writer;
    } catch (Exception e) {
      IOException ie = new IOException("cannot get log writer");
      ie.initCause(e);
      throw ie;
    }
  }

  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.
   * @throws IOException
   */
  private byte [][] cleanOldLogs(long oldestOutstandingSeqNum) throws IOException {
    // Get the set of all log files whose last sequence number is smaller than
    // the oldest edit's sequence number.
    // If oldestOutstandingSeqNum == Long.MAX_VALUE, then no new writes have come
    // in since all regions were flushed (and removed from the firstSeqWrittenInXXX
    // maps). We can remove all old log files.
    TreeSet<Long> sequenceNumbers = new TreeSet<Long>(this.outputfiles.headMap(
        (oldestOutstandingSeqNum)).keySet());

    // Now remove old log files (if any)
    int logsToRemove = sequenceNumbers.size();
    if (logsToRemove > 0) {
      if (LOG.isDebugEnabled()) {
        // Find associated region; helps debugging.
        byte [] oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
        LOG.debug(hlogName + "Found " + logsToRemove + " hlogs to remove" +
          " out of total " + this.outputfiles.size() + ";" +
          " oldest outstanding sequenceid is " + oldestOutstandingSeqNum +
          " from region " + Bytes.toStringBinary(oldestRegion));
      }
      for (Long seq : sequenceNumbers) {
        archiveLogFile(this.outputfiles.get(seq), seq);
        this.outputfiles.remove(seq);
      }
    }

    // If too many log files, figure which regions we need to flush.
    // Transactions in these regions have not been flushed, which
    // prevent log roller from archiving oldest HLog file. Flush requests
    // will be sent to these regions after log rolling.
    byte [][] regions = null;
    int logCount = this.outputfiles.size();
    if (logCount > this.maxLogs) {
      regions = findMemstoresWithEditsEqualOrOlderThan(
          this.outputfiles.firstKey(),
          this.firstSeqWrittenInCurrentMemstore,
          this.firstSeqWrittenInSnapshotMemstore);

      if (regions != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < regions.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(Bytes.toStringBinary(regions[i]));
        }
        LOG.info(hlogName + ": Too many hlogs: logs=" + logCount + ", maxlogs=" +
            this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
            sb.toString());
      }
    }
    return regions;
  }

  /**
   * Return regions (memstores) that have edits that are equal or less than
   * the passed <code>oldestWALseqid</code>.
   *
   * We don't synchronize on sequence maps because when race condition happens,
   * in worst case we only miss some region that prevents the oldest HLog file
   * from being archived. The affect is ignorable.
   *
   * @param oldestWALseqid
   * @param regionsToCurSeqids
   * @param regionsToPrevSeqids
   * @return All regions whose seqid is < than <code>oldestWALseqid</code> (Not
   * necessarily in order).  Null if no regions found.
   */
  static byte [][] findMemstoresWithEditsEqualOrOlderThan(final long oldestWALseqid,
      final Map<byte [], Long> regionsToCurSeqids,
      final Map<byte [], Long> regionsToPrevSeqids) {
    //  This method is static so it can be unit tested the easier.
    List<byte []> regions = new ArrayList<byte []>();
    for (Map.Entry<byte [], Long> e: regionsToCurSeqids.entrySet()) {
      if (e.getValue() <= oldestWALseqid) {
        byte [] region = e.getKey();
        if (!regions.contains(region))
          regions.add(region);
      }
    }
    for (Map.Entry<byte [], Long> e: regionsToPrevSeqids.entrySet()) {
      if (e.getValue() <= oldestWALseqid) {
        byte [] region = e.getKey();
        if (!regions.contains(region))
          regions.add(region);
      }
    }
    return regions.isEmpty() ?
      null : regions.toArray(new byte [][] {HConstants.EMPTY_BYTE_ARRAY});
  }

  /*
   * @return Logs older than this id are safe to remove.
   */
  private long getOldestOutstandingSeqNum() {
    long oldest = Long.MAX_VALUE;
    synchronized (oldestSeqNumsLock) {
      if (!this.firstSeqWrittenInCurrentMemstore.isEmpty()) {
        oldest = Collections.min(this.firstSeqWrittenInCurrentMemstore.values());
      }
      if (!this.firstSeqWrittenInSnapshotMemstore.isEmpty()) {
        oldest = Math.min(oldest,
            Collections.min(this.firstSeqWrittenInSnapshotMemstore.values()));
      }
    }

    return oldest;
  }

  private byte [] getOldestRegion(final Long oldestOutstandingSeqNum) {
    byte [] oldestRegion = null;
    for (Map.Entry<byte [], Long> e: this.firstSeqWrittenInCurrentMemstore.entrySet()) {
      if (e.getValue().longValue() == oldestOutstandingSeqNum.longValue()) {
        oldestRegion = e.getKey();
        return oldestRegion;
      }
    }
    for (Map.Entry<byte [], Long> e: this.firstSeqWrittenInSnapshotMemstore.entrySet()) {
      if (e.getValue().longValue() == oldestOutstandingSeqNum.longValue()) {
        oldestRegion = e.getKey();
        return oldestRegion;
      }
    }
    return null;
  }

  /*
   * Cleans up current writer closing and adding to outputfiles.
   * Presumes we're operating inside an updateLock scope.
   * @return Path to current writer or null if none.
   * @throws IOException
   */
  Path cleanupCurrentWriter(final long currentfilenum) throws IOException {
    Path hlog = null;
    if (this.writer != null) {
      if (!writerCloseSyncDone) {
        syncWriter(this.writer);
      }
      // Close the current writer
      writerCloseSyncDone = true;
      hlog = closeWriter(this.writer, currentfilenum, this.logSeqNum.get());
      this.writer =  null;
    }
    return hlog;
  }

  /*
   * Sync the edits to the datanodes.
   * If this is called on this.writer, it should be holding the updates lock.
   */
  void syncWriter(Writer writer) throws IOException {
    if (writer != null) {
      try {
          writer.sync();
      } catch (IOException ioe) {
        syncFailureAbortStrategy.abort(hlogName + " log sync failed when trying to close "
            + writer, ioe);
      }
    }
  }

  /*
   * Closes the given writer. Updates the output files using the specified logSeqNum
   * @return The path to the closed file.
   */
  Path closeWriter(Writer writer, final long filenum, long logSeqNum) throws IOException {
    Path oldFile = null;
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException ioe) {
        Path fname = computeFilename(filenum);
        if (!tryRecoverFileLease(fs, fname, conf)) {
          IOException ioe2 =
              new IOException(hlogName + "lease recovery pending for " + fname, ioe);
          throw ioe2;
        }
      }
      writerCloseSyncDone = false;
      if (filenum >= 0) {
        oldFile = computeFilename(filenum);
        this.outputfiles.put(Long.valueOf(logSeqNum), oldFile);
      }
    }
    return oldFile;
  }

  private void archiveLogFile(final Path p, final Long seqno) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p, this.fs, this.conf);
    LOG.info(hlogName + "moving old hlog file " + FSUtils.getPath(p) +
      " whose highest sequence/edit id is " + seqno + " to " +
      FSUtils.getPath(newPath));
    this.fs.rename(p, newPath);
    if (!this.actionListeners.isEmpty()) {
      for (LogActionsListener list : this.actionListeners) {
        list.logArchived(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that is used by tests to get the current
   * filename without knowing the file-number.
   * @return Path
   */
  protected Path computeFilename() {
    return computeFilename(this.filenum);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @return Path
   */
  protected Path computeFilename(long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    return new Path(dir, prefix + "." + filenum);
  }

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    FileStatus[] files = fs.listStatus(this.dir);
    for(FileStatus file : files) {
      Path newPath = getHLogArchivePath(this.oldLogDir, file.getPath(), fs, this.conf);
      fs.rename(file.getPath(), newPath);
      LOG.debug(hlogName + "Moved log file " + file + " to " + newPath);
    }
    fs.delete(dir, true);
  }

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    // Prevent all further flushing and rolling.
    this.closeLock.writeLock().lock();
    try {
      logSyncerThread.syncerShuttingDown = true;
      // wait for 2 seconds to gracefully close the syncer
      logSyncerThread.join(2000);
      if (logSyncerThread.isAlive()) {
        // interrupting the logSyncer will most likely interrupt the dfs
        // sync() call. And interrupting log sync will abort the jvm
        LOG.warn("Could not shutdown log-syncer in 2s. Interrupting it now");
        logSyncerThread.interrupt();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("closing hlog writer in " + this.dir.toString());
      }
      synchronized (updateLock) {
        this.closed = true;
        cleanupCurrentWriter(-1);
      }
    } catch (InterruptedException e) {
      LOG.error(this.hlogName + "Exception while waiting for syncer thread to die", e);
    } finally {
      this.closeLock.writeLock().unlock();
    }
  }

  /**
   * @param now
   * @param regionName
   * @param tableName
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqnum, long now) {
    return new HLogKey(regionName, tableName, seqnum, now);
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by regionName,
   * rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for a
   * given key-range of the HRegion (TODO). Any edits that do not have a
   * matching COMPLETE_CACHEFLUSH message can be discarded.
   *
   * <p>
   * Logs cannot be restarted once closed, or once the HLog process dies. Each
   * time the HLog starts, it must create a new log. This means that other
   * systems should process the log appropriately upon each startup (and prior
   * to initializing HLog).
   *
   * synchronized prevents appends during the completion of a cache flush or for
   * the duration of a log roll.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @return The log sequence number for the edit. -1 if the WAL is disabled.
   *          This sequence number is used for book-keeping for per-CF flush
   *          of the memstore.
   * @throws IOException
   */
  public long append(HRegionInfo info, byte [] tableName, WALEdit edits,
    final long now) throws IOException {
    if (!this.enabled || edits.isEmpty()) {
      return -1L;
    }
    if (logSyncerThread.syncerShuttingDown) {
      // can't acquire lock for the duration of append()
      // so this is just a best-effort check
      throw new IOException("Cannot append; logSyncer shutting down");
    }

    long len = edits.getTotalKeyValueLength();
    long txid = 0;
    long seqNum;

    long start = System.currentTimeMillis();
    byte[] regionName = info.getRegionName();

    synchronized (this.appendLock) {
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). . When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      seqNum = obtainSeqNum();
      this.firstSeqWrittenInCurrentMemstore.putIfAbsent(regionName, seqNum);
      HLogKey logKey = makeKey(regionName, tableName, seqNum, now);

      doWrite(info, logKey, edits);
      // Only count 1 row as an unflushed entry.
      txid = this.unflushedEntries.incrementAndGet();
    }

    // Update the metrics
    this.numEntries.incrementAndGet();
    writeSize.inc(len);

    // sync txn to file system
    start = System.currentTimeMillis();
    this.sync(info.isMetaRegion(), txid);

    // Update the metrics and log down the outliers
    long end = System.currentTimeMillis();
    long syncTime = end - start;
    gsyncTime.inc(syncTime);
    if (syncTime > 1000) {
      LOG.warn(String.format(
        "%s took %d ms appending an edit to hlog; editcount=%d, len~=%s",
        Thread.currentThread().getName(), syncTime, this.numEntries.get(),
        StringUtils.humanReadableInt(len)));
    }

    // Update the per-request profiling data
    Call call = HRegionServer.callContext.get();
    ProfilingData pData = call == null ? null : call.getProfilingData();
    if (pData != null) {
      if (this.lastLogRollStartTimeMillis > start
          && end > this.lastLogRollStartTimeMillis) {
        // We also had a log roll in between
        pData.addLong(ProfilingData.HLOG_ROLL_TIME_MS, this.lastLogRollDurationMillis);
        // Do not account for this as the sync time.
        syncTime = syncTime - this.lastLogRollDurationMillis;
      }
      // update sync time
      pData.addLong(ProfilingData.HLOG_SYNC_TIME_MS, syncTime);
    }
    return seqNum;
  }

  /**
   * This thread is responsible to call syncFs and buffer up the writers while
   * it happens.
   */
   class LogSyncer extends HasThread {

    // Using fairness to make sure locks are given in order
    private final ReentrantLock lock = new ReentrantLock(true);

    // Condition used to wait until we have something to sync
    private final Condition queueEmpty = lock.newCondition();

    // Condition used to signal that the sync is done
    private final Condition syncDone = lock.newCondition();

    private long optionalFlushInterval;

    // this variable is protected by this.lock
    volatile boolean syncerShuttingDown = false;

    LogSyncer(long optionalFlushInterval) {
      this.optionalFlushInterval = optionalFlushInterval;
      if (this.optionalFlushInterval <= 0) {
        LOG.warn("optionalFlushInterval = " + this.optionalFlushInterval +
            " turning off periodic forced flushes");
        this.optionalFlushInterval = Long.MAX_VALUE;
      }
    }

    @Override
    public void run() {
      try {
        long lastHFlushAt = EnvironmentEdgeManager.currentTimeMillis();
        lock.lock();
        // awaiting with a timeout doesn't always
        // throw exceptions on interrupt
        do {
          forceSync = false;
          // wake up every 100ms to check if logsyncer has to shut down
          queueEmpty.awaitNanos(100*1000000);
          if (unflushedEntries.get() == syncTillHere) {
            // call hflush() if we haven't flushed for a while now
            // This force-sync is just a safety mechanism - we being
            // paranoid. If there hasn't been any syncing activity for
            // optionalFlushInterval then force a sync. I am not sure what it
            // is supposed to fix.
            if ((EnvironmentEdgeManager.currentTimeMillis() - lastHFlushAt) >=
                this.optionalFlushInterval) {
              forceSync = true;
            } else {
              continue;
            }
          }
          // We got the signal, let's hflush. We currently own the lock so new
          // writes are waiting to acquire it in addToSyncQueue while the ones
          // we hflush are waiting on await()
          hflush();
          lastHFlushAt = EnvironmentEdgeManager.currentTimeMillis();

          // Release all the clients waiting on the hflush. Notice that we still
          // own the lock until we get back to await at which point all the
          // other threads waiting will first acquire and release locks
          syncDone.signalAll();
        } while (!syncerShuttingDown ||
            (unflushedEntries.get() != syncTillHere));
        // The check above involves synchronization between syncerShuttingDown
        // and unflushedEntries in append(). The check for syncerShutDown has to
        // come before.
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for sync requests");
        if (unflushedEntries.get() != syncTillHere) {
          syncFailureAbortStrategy.abort("LogSyncer interrupted before it" +
              " could sync everything. Aborting JVM", e);
        }
      } finally {
        syncerShuttingDown = true;
        syncDone.signalAll();
        lock.unlock();
        LOG.info(getName() + " exiting");
      }
    }

    /**
     * This method first signals the thread that there's a sync needed
     * and then waits for it to happen before returning.
     * @throws IOException
     */
    public void addToSyncQueue(boolean force, long txid) throws IOException {
      while (true) {
        // Don't bother if somehow our append was already hflushed
        // Check this without even acquiring the lock, in the hope
        // that our edits is already synced.
        if (!force && syncTillHere >= txid) {
          return;
        }
        if (force) {
          forceSync = true;
          force = false;
        }
        lock.lock();
        try {

          if (syncerShuttingDown) {
            throw (new IOException("LogSyncer closing. Aborting the sync"));
          }

          // Wake the thread
          queueEmpty.signal();

          // Wait for it to hflush
          // This check is redone here because it is within the lock
          if (syncTillHere < txid) {
            syncDone.await();
          }
        } catch (InterruptedException e) {
          LOG.warn(getName() + " was interrupted while waiting for sync", e);
        } finally {
          lock.unlock();
        }
      }
    }
  }

  @Override
  public void sync() throws IOException {
    sync(false);
  }

  /**
   * This method calls the LogSyncer in order to group commit the sync
   * with other threads.
   * @param force For catalog regions, force the sync to happen
   * @throws IOException
   */
  public void sync(boolean force) throws IOException {
    sync(force, this.unflushedEntries.get());
  }

  /**
   * This method calls the LogSyncer only if its transaction is not yet flushed.
   *
   * @param txid
   *          The transaction id that this call is interested in.
   * @throws IOException
   */
  public void sync(boolean force, long txid) throws IOException {
    logSyncerThread.addToSyncQueue(force, txid);
  }

  private void hflush() {
    synchronized (this.updateLock) {
      if (this.closed) {
        return;
      }
      boolean isUnderReplication = false;
      if (this.forceSync ||
          this.unflushedEntries.get() - this.syncTillHere >= this.flushlogentries) {
        try {
          this.syncTillHere += this.logBuffer.appendAndSync();
        } catch (IOException e) {
          syncFailureAbortStrategy.abort(hlogName + "Could not sync hlog. Aborting", e);
        }
      }

      // if the number of replicas in HDFS has fallen below the initial
      // value, then roll logs.
      try {
        int numCurrentReplicas = getLogReplication();
        if (numCurrentReplicas != 0 &&
            numCurrentReplicas < this.initialReplication) {
          LOG.warn("HDFS pipeline error detected. " +
              "Found " + numCurrentReplicas + " replicas but expecting " +
              this.initialReplication + " replicas. " +
              " Requesting close of hlog.");
          isUnderReplication = true;
        }
      } catch (Exception e) {
          LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
                   " still proceeding ahead...");
      }

      try {
        if (isUnderReplication || (this.writer.getLength() > this.logrollsize)) {
          requestLogRoll();
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to get length of the log file. Ignoring", ioe);
      }
    }
  }

  /**
   * This method gets the datanode replication count for the current HLog.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   *
   * @throws Exception
   */
  int getLogReplication() throws Exception {
    if(this.getNumCurrentReplicas != null && this.hdfs_out != null) {
      Object repl = this.getNumCurrentReplicas.invoke(this.hdfs_out, NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }

  /**
   * This is a utility method for tests to find the sequence number of the first
   * KV in a given region's memstore
   *
   * @param region
   * @return
   */
  public long getFirstSeqWrittenInCurrentMemstoreForRegion(HRegion region) {
    Long value = firstSeqWrittenInCurrentMemstore.get(region.getRegionName());
    if (value != null) {
      return value.longValue();
    } else {
      return Long.MAX_VALUE;
    }
  }

  /**
   * This is a utility method for tests to find the sequence number of the first
   * KV in a given region's snapshot memstore
   *
   * @param region
   * @return
   */
  public long getFirstSeqWrittenInSnapshotMemstoreForRegion(HRegion region) {
    Long value = firstSeqWrittenInSnapshotMemstore.get(region.getRegionName());
    if (value != null) {
      return value.longValue();
    } else {
      return Long.MAX_VALUE;
    }
  }

  boolean canGetCurReplicas() {
    return this.getNumCurrentReplicas != null;
  }

  private void requestLogRoll() {
    if (this.listener != null) {
      this.listener.logRollRequested();
    }
  }

  protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
  throws IOException {
    this.logBuffer.appendToBuffer(new Entry(logKey, logEdit));
  }

  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries.get();
  }

  /**
   * Obtain a log sequence number.
   */
  public long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }

  /** @return the number of log files in use */
  public int getNumLogFiles() {
    return outputfiles.size();
  }

  /**
   * See {@link #startCacheFlush(byte[], long, long)} (byte[], long, long)}
   * @param regionName
   * @return
   */
  public long startCacheFlush(final byte[] regionName) {
    return startCacheFlush(regionName, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  /**
   * Acquire a lock so that we do not close between the start and
   * completion of a cache-flush or a log rolling.
   *
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   *
   * In this method, by removing the entry in firstSeqWritten for the region
   * being flushed we ensure that the next edit inserted in this region will be
   * correctly recorded in {@link #append(HRegionInfo, HLogKey, WALEdit)}. The
   * lsn of the earliest in-memory lsn - which is now in the memstore snapshot -
   * is saved temporarily in the firstSeqWritten map while the flush is active.
   *
   * In case the per-CF flush is enabled, we cannot simply clear the
   * firstSeqWritten entry for the region to be flushed. There might be certain
   * CFs whose memstores won't be flushed. Therefore, we need the first LSNs for
   * the stores that will be flushed, and first LSNs for the stores that won't
   * be flushed.
   *
   * @param regionName
   * @param firstSeqIdInStoresToFlush
   * @param firstSeqIdInStoresNotToFlush
   * @return sequence ID to pass {@link #completeCacheFlush(byte[], byte[], long, boolean)}
   * (byte[], byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long, boolean)
   * @see #abortCacheFlush()
   */
  public long startCacheFlush(final byte [] regionName,
                              long firstSeqIdInStoresToFlush,
                              long firstSeqIdInStoresNotToFlush) {
    long num = -1;
    this.closeLock.readLock().lock();
    synchronized (oldestSeqNumsLock) {
      if (this.firstSeqWrittenInSnapshotMemstore.containsKey(regionName)) {
        LOG.warn("Requested a startCacheFlush while "
            + "firstSeqWrittenInSnapshotMemstore still contains "
            + Bytes.toStringBinary(regionName) + " . Did the previous flush "
            + "fail? Will try to complete it");
      } else {

        // If we are flushing the entire memstore, remove the entry from the
        // current memstores.
        if (firstSeqIdInStoresNotToFlush == Long.MAX_VALUE) {
          Long seq = this.firstSeqWrittenInCurrentMemstore.remove(regionName);
          if (seq != null) {
            this.firstSeqWrittenInSnapshotMemstore.put(regionName, seq);
          }
          num  = obtainSeqNum();
        } else {
          // Amongst the Stores not being flushed, what is the smallest sequence
          // number? Put that in this map.
          this.firstSeqWrittenInCurrentMemstore.replace(regionName,
            firstSeqIdInStoresNotToFlush);

          // Amongst the Stores being flushed, what is the smallest sequence
          // number? Put that in this map.
          this.firstSeqWrittenInSnapshotMemstore.put(regionName,
            firstSeqIdInStoresToFlush);

          // During Log Replay, we can safely discard any edits that have
          // the sequence number less than the smallest sequence id amongst the
          // stores that we are not flushing. This might re-apply some edits
          // which belonged to stores which are going to be flushed, but we
          // expect these operations to be idempotent anyways, and this is
          // simpler.
          num = firstSeqIdInStoresNotToFlush - 1;
        }
      }
    }
    if (num == -1) {
      num = obtainSeqNum();
    }
    return num;
  }

  /**
   * Complete the cache flush
   *
   * Protected by cacheFlushLock
   *
   * @param regionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  public void completeCacheFlush(final byte [] regionName, final byte [] tableName,
      final long logSeqId, final boolean isMetaRegion) {
    // Cleaning up of lastSeqWritten is in the finally clause because we
    // don't want to confuse getOldestOutstandingSeqNum()
    // Synchronization prevents possible race condition that makes Collections.min()
    // throw NoSuchElementException.
    synchronized (oldestSeqNumsLock) {
      this.firstSeqWrittenInSnapshotMemstore.remove(regionName);
    }
    this.closeLock.readLock().unlock();
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memstore.
   */
  public void abortCacheFlush(byte[] regionName) {
    LOG.debug(hlogName + "Aborting cache flush of region "
        + Bytes.toStringBinary(regionName));
    this.closeLock.readLock().unlock();
  }

  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte [] family) {
    return Bytes.equals(METAFAMILY, family);
  }

  /**
   * @see the more general method {@link #splitLog(Path, Path, Path,
   * FileSystem, Configuration, long, Stoppable)}
   */
  public static List<Path> splitLog(final Path rootDir, final Path srcDir,
    Path oldLogDir, final FileSystem fs, final Configuration conf)
  throws IOException {
    return splitLog(rootDir, srcDir, oldLogDir, fs, conf,
        DEFAULT_LATEST_TS_TO_INCLUDE, null);
  }

  /**
   * Split up a bunch of regionserver commit log files that are no longer
   * being written to, into new files, one per region for region to replay on
   * startup. Delete the old log files when finished.
   *
   * @param rootDir qualified root directory of the HBase instance
   * @param srcDir Directory of log files to split: e.g.
   *                <code>${ROOTDIR}/log_HOST_PORT</code>
   * @param oldLogDir directory where processed (split) logs will be archived to
   * @param fs FileSystem
   * @param conf Configuration
   * @param maxWriteTime ignore entries with ts greater than this
   * @throws IOException will throw if corrupted hlogs aren't tolerated
   * @return the list of splits
   */
  public static List<Path> splitLog(final Path rootDir, final Path srcDir,
    Path oldLogDir, final FileSystem fs, final Configuration conf,
    long maxWriteTime, Stoppable shutdownStatus)
  throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Splitting logs in " + srcDir);
    try {
      long startTime = System.currentTimeMillis();
      List<Path> splits = null;
      status.setStatus("Determining files to split");
      if (!fs.exists(srcDir)) {
        // Nothing to do
        status.markComplete("No log directory existed to split.");
        return splits;
      }
      FileStatus [] logfiles = fs.listStatus(srcDir);
      if (logfiles == null || logfiles.length == 0) {
        // Nothing to do
        status.markComplete("No log files existed to split.");
        return splits;
      }
      LOG.info("Splitting " + logfiles.length + " hlog(s) in " +
        srcDir.toString());
      status.setStatus("Performing split");
      splits = splitLog(rootDir, srcDir, oldLogDir, logfiles, fs, conf,
          maxWriteTime, shutdownStatus);
      try {
        FileStatus[] files = fs.listStatus(srcDir);
        if (files == null) {
          files = NO_FILES;
        }
        for(FileStatus file : files) {
          Path newPath = getHLogArchivePath(oldLogDir, file.getPath(), fs, conf);
          LOG.info("Moving " +  FSUtils.getPath(file.getPath()) + " to " +
                     FSUtils.getPath(newPath));
          fs.rename(file.getPath(), newPath);
        }
        LOG.debug("Moved " + files.length + " log files to " +
          FSUtils.getPath(oldLogDir));
        fs.delete(srcDir, true);
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        IOException io = new IOException("Cannot delete: " + srcDir);
        io.initCause(e);
        throw io;
      }
      lastSplitTime = System.currentTimeMillis() - startTime;
      status.markComplete("Log splits complete.");
      LOG.info("hlog file splitting completed in " + lastSplitTime +
          " ms for " + srcDir.toString());
      return splits;
    } finally {
      // prevent MonitoredTask leaks due to thrown exceptions
      status.cleanup();
    }
  }

  // Private immutable datastructure to hold Writer and its Path.
  private final static class WriterAndPath {
    final Path p;
    final Writer w;
    WriterAndPath(final Path p, final Writer w) {
      this.p = p;
      this.w = w;
    }
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
     return (Class<? extends HLogKey>)
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Sorts the HLog edits in the given list of logfiles (that are a mix of edits on multiple regions)
   * by region and then splits them per region directories, in batches of (hbase.hlog.split.batch.size)
   *
   * A batch consists of a set of log files that will be sorted in a single map of edits indexed by region
   * the resulting map will be concurrently written by multiple threads to their corresponding regions
   *
   * Each batch consists of more more log files that are
   *  - recovered (files is opened for append then closed to ensure no process is writing into it)
   *  - parsed (each edit in the log is appended to a list of edits indexed by region
   *    see {@link #parseHLog} for more details)
   *  - marked as either processed or corrupt depending on parsing outcome
   *  - the resulting edits indexed by region are concurrently written to their corresponding region
   *    region directories
   *  - original files are then archived to a different directory
   *
   *
   *
   * @param rootDir  hbase directory
   * @param srcDir   logs directory
   * @param oldLogDir directory where processed logs are archived to
   * @param logfiles the list of log files to split
   * @param fs
   * @param conf
   * @param maxWriteTime ignore entries with ts greater than this
   * @param shutdownStatus a way to find out if the server is shutting down
   * @return
   * @throws IOException
   */
  private static List<Path> splitLog(final Path rootDir, final Path srcDir,
    Path oldLogDir, final FileStatus[] logfiles, final FileSystem fs,
    final Configuration conf, long maxWriteTime, Stoppable shutdownStatus)
  throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    final Map<byte [], WriterAndPath> logWriters =
      Collections.synchronizedMap(
        new TreeMap<byte [], WriterAndPath>(Bytes.BYTES_COMPARATOR));
    List<Path> splits = null;
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Splitting logs in " + srcDir);

    try {
      // Number of logs in a read batch
      // More means faster but bigger mem consumption
      //TODO make a note on the conf rename and update hbase-site.xml if needed
      int logFilesPerStep = conf.getInt("hbase.hlog.split.batch.size", 3);
      boolean skipErrors =
        conf.getBoolean("hbase.hlog.split.skip.errors",
            SPLIT_SKIP_ERRORS_DEFAULT);

      lastSplitSize = 0;

      status.setStatus("Performing split");
      try {
        int i = -1;
        while (i < logfiles.length) {
          final Map<byte[], LinkedList<Entry>> editsByRegion =
            new TreeMap<byte[], LinkedList<Entry>>(Bytes.BYTES_COMPARATOR);
          for (int j = 0; j < logFilesPerStep; j++) {
            checkForShutdown(shutdownStatus);
            i++;
            if (i == logfiles.length) {
              break;
            }
            FileStatus log = logfiles[i];
            Path logPath = log.getPath();
            long logLength = log.getLen();
            lastSplitSize += logLength;
            LOG.debug("Splitting hlog " + (i + 1) + " of " + logfiles.length +
              ": " + logPath + ", length=" + logLength );
            try {
              recoverFileLease(fs, logPath, conf);
              parseHLog(log, editsByRegion, fs, conf, maxWriteTime,
                  shutdownStatus);
              processedLogs.add(logPath);
            } catch (EOFException eof) {
              // truncated files are expected if a RS crashes (see HBASE-2643)
              LOG.warn("EOF from hlog " + logPath + ".  continuing");
              processedLogs.add(logPath);
            } catch (InterruptedIOException iioe) {
              status.abort(StringUtils.stringifyException(iioe));
              throw iioe;
            } catch (IOException e) {
              // If the IOE resulted from bad file format,
              // then this problem is idempotent and retrying won't help
              if (e.getCause() instanceof ParseException) {
                LOG.warn("ParseException from hlog " +
                    logPath + ".  continuing");
                processedLogs.add(logPath);
              } else {
                if (skipErrors) {
                  LOG.info("Got while parsing hlog " + logPath +
                    ". Marking as corrupted", e);
                  corruptedLogs.add(logPath);
                } else {
                  status.abort(StringUtils.stringifyException(e));
                  throw e;
                }
              }
            }
          }
          writeEditsBatchToRegions(editsByRegion, logWriters,
              rootDir, fs, conf, shutdownStatus);
        }
        Preconditions.checkNotNull(fs);
        Preconditions.checkNotNull(srcDir);
        Preconditions.checkNotNull(processedLogs);
        Preconditions.checkNotNull(corruptedLogs);
        FileStatus[] srcDirList = fs.listStatus(srcDir);
        if (srcDirList != null && srcDirList.length > processedLogs.size() +
            corruptedLogs.size()) {
          status.abort("Discovered orphan hlog after split");
          throw new IOException("Discovered orphan hlog after split. Maybe " +
            "HRegionServer was not dead when we started");
        }
      } finally {
        splits = new ArrayList<Path>(logWriters.size());
        for (WriterAndPath wap : logWriters.values()) {
          wap.w.close();
          splits.add(wap.p);
          LOG.debug("Closed " + wap.p);
        }
      }
      status.setStatus("Archiving logs after completed split");
      archiveLogs(corruptedLogs, processedLogs, oldLogDir, fs, conf);
      status.markComplete("Split completed");
      return splits;
    } finally {
      status.cleanup();
    }
  }


  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /**
   * Construct the HLog directory name
   *
   * @param info HServerInfo for server
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(HServerInfo info) {
    return getHLogDirectoryName(info.getServerName());
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverAddress
   * @param startCode
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverAddress,
      long startCode) {
    if (serverAddress == null || serverAddress.length() == 0) {
      return null;
    }
    return getHLogDirectoryName(
        HServerInfo.getServerName(serverAddress, startCode));
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverName
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  static Path getHLogArchivePath(Path oldLogDir, Path p, FileSystem fs,
                                 Configuration conf) throws IOException {
    String filename = p.getName();

    // if subdirectories are disabled...
    if (conf != null && !conf.getBoolean(
        HConstants.HREGION_OLDLOGDIR_USE_SUBDIR_STRUCTURE,
        HConstants.HREGION_OLDLOGDIR_USE_SUBDIR_STRUCTURE_DEFAULT)) {
      return new Path(oldLogDir, filename);
    }

    if (!validateHLogFilename(filename)) {
      LOG.warn("Malformed Log file name: " + filename);
      return new Path(oldLogDir, filename);
    }

    String subDirectoryName;
    if (ARCHIVE_TO_HOURLY_DIR) {
      // Group into hourly sub-directory
      subDirectoryName = DATE_FORMAT.format(Calendar.getInstance().getTime());
    } else {
      // since the filename is a valid name, we know there
      // is a last '.' (won't return -1)
      subDirectoryName = filename.substring(0, filename.lastIndexOf('.'));
    }
    Path oldLogsSubDir = new Path(oldLogDir, subDirectoryName);
    fs.mkdirs(oldLogsSubDir);

    return new Path(oldLogsSubDir, filename);
  }

  /**
   * Takes splitLogsMap and concurrently writes them to region directories using a thread pool
   *
   * @param splitLogsMap map that contains the log splitting result indexed by region
   * @param logWriters map that contains a writer per region
   * @param rootDir hbase root dir
   * @param fs
   * @param conf
   * @throws IOException
   */
  private static void writeEditsBatchToRegions(
    final Map<byte[], LinkedList<Entry>> splitLogsMap,
    final Map<byte[], WriterAndPath> logWriters,
    final Path rootDir, final FileSystem fs, final Configuration conf,
    Stoppable shutdownStatus)
  throws IOException {
    // Number of threads to use when log splitting to rewrite the logs.
    // More means faster but bigger mem consumption.
    int logWriterThreads =
      conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    boolean skipErrors = conf.getBoolean("hbase.skip.errors", false);
    HashMap<byte[], Future> writeFutureResult = new HashMap<byte[], Future>();
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat((shutdownStatus == null ? "" : shutdownStatus + "-")
        + "SplitWriter-%1$d");
    tfb.setThreadFactory(Executors.defaultThreadFactory());
    ThreadFactory f  = tfb.build();
    ThreadPoolExecutor threadPool = (ThreadPoolExecutor)
        Executors.newFixedThreadPool(logWriterThreads, f);
    for (final byte [] region : splitLogsMap.keySet()) {
      Callable splitter = createNewSplitter(rootDir, logWriters, splitLogsMap,
          region, fs, conf, shutdownStatus);
      writeFutureResult.put(region, threadPool.submit(splitter));
    }

    threadPool.shutdown();
    // Wait for all threads to terminate
    try {
      for (int j = 0; !threadPool.awaitTermination(5, TimeUnit.SECONDS); j++) {
        String message = "Waiting for hlog writers to terminate, elapsed " + j * 5 + " seconds";
        if (j < 30) {
          LOG.debug(message);
        } else {
          LOG.info(message);
        }

      }
    } catch(InterruptedException ex) {
      String errorMsgr = "Hlog writers were interrupted!";
      LOG.info(errorMsgr, ex);
      throw (InterruptedIOException)new InterruptedIOException(
          errorMsgr).initCause(ex);
    }

    for (Map.Entry<byte[], Future> entry : writeFutureResult.entrySet()) {
      try {
        entry.getValue().get();
      } catch (ExecutionException e) {
        throw (new IOException(e.getCause()));
      } catch (InterruptedException e1) {
        String errorMsgr =
            "Writer for region " + Bytes.toStringBinary(entry.getKey())
                + " was interrupted, however the write process should have "
                + "finished. Throwing up ";
        LOG.info(errorMsgr, e1);
        throw (InterruptedIOException)new InterruptedIOException(
            errorMsgr).initCause(e1);
      }
    }
  }

  /*
   * Parse a single hlog and put the edits in @splitLogsMap
   *
   * @param logfile to split
   * @param splitLogsMap output parameter: a map with region names as keys and a
   * list of edits as values
   * @param fs the filesystem
   * @param conf the configuration
   * @param maxWriteTime ignore entries with ts greater than this
   * @throws IOException if hlog is corrupted, or can't be open
   */
  private static void parseHLog(final FileStatus logfile,
    final Map<byte[], LinkedList<Entry>> splitLogsMap, final FileSystem fs,
    final Configuration conf, long maxWriteTime, Stoppable shutdownStatus)
  throws IOException {
    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    long length = logfile.getLen();
    if (length <= 0) {
      LOG.warn("File " + logfile.getPath() + " might be still open, length is 0");
    }
    Path path = logfile.getPath();
    Reader in;
    int editsCount = 0;
    try {
      in = HLog.getReader(fs, path, conf);
    } catch (EOFException e) {
      if (length <= 0) {
        //TODO should we ignore an empty, not-last log file if skip.errors is false?
        //Either way, the caller should decide what to do. E.g. ignore if this is the last
        //log in sequence.
        //TODO is this scenario still possible if the log has been recovered (i.e. closed)
        LOG.warn("Could not open " + path + " for reading. File is empty" + e);
        return;
      } else {
        throw e;
      }
    }
    try {
      Entry entry;
      while ((entry = in.next()) != null) {
        checkForShutdown(shutdownStatus);
        //Ignore entries that have a ts greater than maxWriteTime
        if (entry.getKey().getWriteTime() > maxWriteTime) continue;
        byte[] region = entry.getKey().getRegionName();
        LinkedList<Entry> queue = splitLogsMap.get(region);
        if (queue == null) {
          queue = new LinkedList<Entry>();
          splitLogsMap.put(region, queue);
        }
        queue.addLast(entry);
        editsCount++;
      }
    } finally {
      LOG.debug("Pushed=" + editsCount + " entries from " + path);
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Close log reader in finally threw exception -- continuing", e);
      }
    }
  }

  private static Callable<Void> createNewSplitter(final Path rootDir,
    final Map<byte[], WriterAndPath> logWriters,
    final Map<byte[], LinkedList<Entry>> logEntries,
    final byte[] region, final FileSystem fs, final Configuration conf,
    final Stoppable shutdownStatus) {
    return new Callable<Void>() {
      public String getName() {
        return "Split writer thread for region " + Bytes.toStringBinary(region);
      }

      @Override
      public Void call() throws IOException {
        LinkedList<Entry> entries = logEntries.get(region);
        LOG.debug(this.getName()+" got " + entries.size() + " to process");
        long threadTime = System.currentTimeMillis();
        try {
          int editsCount = 0;
          if (Arrays.equals(region, DUMMY)) { // ignore dummy edits
            return null;
          }
          WriterAndPath wap = logWriters.get(region);
          for (Entry logEntry: entries) {
            checkForShutdown(shutdownStatus);
            if (wap == null) {
              Path regionedits = getRegionSplitEditsPath(fs, logEntry, rootDir,
                  true);
              if (fs.exists(regionedits)) {
                LOG.warn("Found existing old edits file. It could be the " +
                  "result of a previous failed split attempt. Deleting " +
                  regionedits + ", length=" + fs.getFileStatus(regionedits).getLen());
                if (!fs.delete(regionedits, false)) {
                  LOG.warn("Failed delete of old " + regionedits);
                }
              }
              Writer w = createWriter(fs, regionedits, conf);
              wap = new WriterAndPath(regionedits, w);
              logWriters.put(region, wap);
              LOG.debug("Creating writer path=" + regionedits +
                " region=" + Bytes.toStringBinary(region));
            }
            wap.w.append(logEntry);
            editsCount++;
          }
          LOG.debug(this.getName() + " Applied " + editsCount +
            " total edits to " + Bytes.toStringBinary(region) +
            " in " + (System.currentTimeMillis() - threadTime) + "ms");
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.fatal(this.getName() + " Got while writing log entry to log", e);
          throw e;
        }
        return null;
      }
    };
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing
   * Moves corrupted logs (any log that couldn't be successfully parsed
   * to corruptDir (.corrupt) for later investigation
   *
   * @param corruptedLogs
   * @param processedLogs
   * @param oldLogDir
   * @param fs
   * @param conf
   * @throws IOException
   */
  static void archiveLogs(final List<Path> corruptedLogs,
      final List<Path> processedLogs, final Path oldLogDir,
      final FileSystem fs, final Configuration conf)
  throws IOException {
    final Path corruptDir = new Path(conf.get(HConstants.HBASE_DIR),
      conf.get("hbase.regionserver.hlog.splitlog.corrupt.dir", ".corrupt"));

    if (!fs.exists(corruptDir) && !fs.mkdirs(corruptDir)) {
      LOG.warn("Unable to mkdir " + corruptDir);
    }
    if (!fs.exists(oldLogDir) && !fs.mkdirs(oldLogDir)) {
      LOG.warn("Unable to mkdir " + oldLogDir);
    }
    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corrupted: corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      if (fs.exists(corrupted)) {
        if (!fs.rename(corrupted, p)) {
          LOG.warn("Unable to move corrupted log " + corrupted + " to " + p);
        } else {
          LOG.warn("Moving corrupted log " + corrupted + " to " + p);
        }
      }
    }

    for (Path p: processedLogs) {
      Path newPath = getHLogArchivePath(oldLogDir, p, fs, conf);
      if (fs.exists(p)) {
        if (!fs.rename(p, newPath)) {
          LOG.warn("Unable to move  " + p + " to " + newPath);
        } else {
          LOG.debug("Archived processed log " + p + " to " + newPath);
        }
      }
    }
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in
   * <code>logEntry</code> named for the sequenceid in the passed
   * <code>logEntry</code>: e.g.
   * /hbase/some_table/2323432434/recovered.edits/2332. This method also ensures
   * existence of RECOVERED_EDITS_DIR under the region creating it if necessary.
   *
   * @param fs
   * @param logEntry
   * @param rootDir HBase root dir.
   * @param isCreate if true create the directory, otherwise just return the name
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  static Path getRegionSplitEditsPath(final FileSystem fs,
      final Entry logEntry, final Path rootDir, boolean isCreate)
  throws IOException {
    Path tableDir = HTableDescriptor.getTableDir(rootDir, logEntry.getKey()
        .getTablename());
    Path regiondir = HRegion.getRegionDir(tableDir,
        HRegionInfo.encodeRegionName(logEntry.getKey().getRegionName()));
    Path dir = getRegionDirRecoveredEditsDir(regiondir);

    if (isCreate && !fs.exists(dir)) {
      if (!fs.mkdirs(dir)) LOG.warn("mkdir failed on " + dir);
    }
    return new Path(dir, formatRecoveredEditsFileName(logEntry.getKey()
        .getLogSeqNum()));
   }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }


  /**
   * Returns sorted set of edit files made by wal-log splitter.
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    FileStatus [] files = null;
    if (fs.exists(editsdir)) {
      files = fs.listStatus(editsdir, new PathFilter () {
        @Override
        public boolean accept(Path p) {
          boolean result = false;
          try {
            // Return files and only files that match the editfile names pattern.
            // There can be other files in this directory other than edit files.
            // In particular, on error, we'll move aside the bad edit file giving
            // it a timestamp suffix.  See moveAsideBadEditsFile.
            Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
            result = fs.isFile(p) && m.matches();
          } catch (IOException e) {
            LOG.warn("Failed isFile check on " + p);
          }
          return result;
        }
      });
    }
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    if (files == null) return filesSorted;
    for (FileStatus status: files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   * @param fs
   * @param edits Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs,
      final Path edits)
  throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "." +
      System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param regiondir This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   * <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, RECOVERED_EDITS_DIR);
  }

  /**
   *
   * @param visitor
   */
  public void addLogEntryVisitor(LogEntryVisitor visitor) {
    this.logEntryVisitors.add(visitor);
  }

  /**
   *
   * @param visitor
   */
  public void removeLogEntryVisitor(LogEntryVisitor visitor) {
    this.logEntryVisitors.remove(visitor);
  }


  public void addLogActionsListerner(LogActionsListener list) {
    LOG.info("Adding a listener");
    this.actionListeners.add(list);
  }

  public boolean removeLogActionsListener(LogActionsListener list) {
    return this.actionListeners.remove(list);
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void usage() {
    System.err.println("Usage: HLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: HLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: HLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  private static void dump(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.isFile(p)) {
      throw new IOException(p + " is not a file");
    }
    Reader log = getReader(fs, p, conf);
    try {
      int count = 0;
      HLog.Entry entry;
      while ((entry = log.next()) != null) {
        System.out.println("#" + count + ", pos=" + log.getPosition() + " " +
          entry.toString());
        count++;
      }
    } finally {
      log.close();
    }
  }

  private static void checkForShutdown(Stoppable shutdownStatus)
      throws IOException {
    if (shutdownStatus != null &&
        shutdownStatus.isStopped()) {
      throw new InterruptedIOException("Aborting log splitting: "
          + shutdownStatus.getStopReason());
    }
  }

  private static void splitFromCmdLine(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    final Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException(p + " is not a directory");
    }
    splitLog(baseDir, p, oldLogDir, fs, conf, DEFAULT_LATEST_TS_TO_INCLUDE,
        null);
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the HLogPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      HLogPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          conf.set("fs.default.name", args[i]);
          conf.set("fs.defaultFS", args[i]);
          Path logPath = new Path(args[i]);
          splitFromCmdLine(conf, logPath);
        } catch (Throwable t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }

  /** Used in a simulated kill of a regionserver */
  public void kill() {
    logSyncerThread.syncerShuttingDown = true;
    logSyncerThread.interrupt();
  }
}
