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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

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
  static final byte [] METAROW = Bytes.toBytes("METAROW");

  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  /** The META region's HLog filename extension */
  public static final String META_HLOG_FILE_EXTN = ".meta";
  public static final String SEPARATE_HLOG_FOR_META = "hbase.regionserver.separate.hlog.for.meta";

  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  public static final String RECOVERED_EDITS_DIR = "recovered.edits";
  private static final Pattern EDITFILES_NAME_PATTERN =
    Pattern.compile("-?[0-9]+");
  public static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  
  private final FileSystem fs;
  private final Path dir;
  private final Configuration conf;
  private final HLogFileSystem hlogFs;
  // Listeners that are called on WAL events.
  private List<WALActionsListener> listeners =
    new CopyOnWriteArrayList<WALActionsListener>();
  private final long optionalFlushInterval;
  private final long blocksize;
  private final String prefix;
  private final AtomicLong unflushedEntries = new AtomicLong(0);
  private volatile long syncedTillHere = 0;
  private long lastDeferredTxid;
  private final Path oldLogDir;
  private volatile boolean logRollRunning;

  private static Class<? extends Writer> logWriterClass;
  private static Class<? extends Reader> logReaderClass;

  private WALCoprocessorHost coprocessorHost;

  static void resetLogReaderClass() {
    HLog.logReaderClass = null;
  }

  private FSDataOutputStream hdfs_out; // FSDataOutputStream associated with the current SequenceFile.writer
  // Minimum tolerable replicas, if the actual value is lower than it, 
  // rollWriter will be triggered
  private int minTolerableReplication;
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
    void reset() throws IOException;
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

  /*
   * Map of all log files but the current one.
   */
  final SortedMap<Long, Path> outputfiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, Path>());

  /*
   * Map of encoded region names to their most recent sequence/edit id in their
   * memstore.
   */
  private final ConcurrentSkipListMap<byte [], Long> lastSeqWritten =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  private boolean forMeta = false;

  // The timestamp (in ms) when the log file was created.
  private volatile long filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private volatile int consecutiveLogRolls = 0;
  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
  private final long logrollsize;

  // size of current log 
  private long curLogSize = 0;

  // The total size of hlog
  private AtomicLong totalLogSize = new AtomicLong(0);
  
  // This lock prevents starting a log roll during a cache flush.
  // synchronized is insufficient because a cache flush spans two method calls.
  private final Lock cacheFlushLock = new ReentrantLock();

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  // locked during appends
  private final Object updateLock = new Object();
  private final Object flushLock = new Object();

  private final boolean enabled;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /**
   * Thread that handles optional sync'ing
   */
  private final LogSyncer logSyncer;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = 
      Pattern.compile(".*\\.\\d*("+HLog.META_HLOG_FILE_EXTN+")*");

  static byte [] COMPLETE_CACHE_FLUSH;
  static {
    try {
      COMPLETE_CACHE_FLUSH =
        "HBASE::CACHEFLUSH".getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }

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
  //For measuring slow HLog appends
  private static AtomicLong slowHLogAppendCount = new AtomicLong();
  private static Metric slowHLogAppendTime = new Metric();
  
  public static Metric getWriteTime() {
    return writeTime.get();
  }

  public static Metric getWriteSize() {
    return writeSize.get();
  }

  public static Metric getSyncTime() {
    return syncTime.get();
  }

  public static long getSlowAppendCount() {
    return slowHLogAppendCount.get();
  }
  
  public static Metric getSlowAppendTime() {
    return slowHLogAppendTime.get();
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf)
  throws IOException {
    this(fs, dir, oldLogDir, conf, null, true, null, false);
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
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
      final Configuration conf, final List<WALActionsListener> listeners,
      final String prefix) throws IOException {
    this(fs, dir, oldLogDir, conf, listeners, true, prefix, false);
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
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfLogDirExists If true IOException will be thrown if dir already exists.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @param forMeta if this hlog is meant for meta updates
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
      final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfLogDirExists, final String prefix, boolean forMeta)
  throws IOException {
    super();
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;
    this.hlogFs = new HLogFileSystem(conf);
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }
    this.blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.dir));
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);
    boolean dirExists = false;
    if (failIfLogDirExists && (dirExists = this.fs.exists(dir))) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    if (!dirExists && !HBaseFileSystem.makeDirOnFileSystem(fs, dir)) {
      throw new IOException("Unable to mkdir " + dir);
    }
    this.oldLogDir = oldLogDir;
    if (!fs.exists(oldLogDir) && !HBaseFileSystem.makeDirOnFileSystem(fs, oldLogDir)) {
      throw new IOException("Unable to mkdir " + this.oldLogDir);
    }
    this.forMeta = forMeta;
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.minTolerableReplication = conf.getInt(
        "hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(this.fs, this.dir));
    this.lowReplicationRollLimit = conf.getInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    this.closeErrorsTolerated = conf.getInt(
        "hbase.regionserver.logroll.errors.tolerated", 0);

    LOG.info("HLog configuration: blocksize=" +
      StringUtils.byteDesc(this.blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", enabled=" + this.enabled +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");
    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = getGetNumCurrentReplicas(this.hdfs_out);

    logSyncer = new LogSyncer(this.optionalFlushInterval);
    // When optionalFlushInterval is set as 0, don't start a thread for deferred log sync.
    if (this.optionalFlushInterval > 0) {
      Threads.setDaemonThreadRunning(logSyncer.getThread(), Thread.currentThread().getName()
          + ".logSyncer");
    } else {
      LOG.info("hbase.regionserver.optionallogflushinterval is set as "
          + this.optionalFlushInterval + ". Deferred log syncing won't work. "
          + "Any Mutation, marked to be deferred synced, will be flushed immediately.");
    }
    coprocessorHost = new WALCoprocessorHost(this, conf);
  }

  /**
   * Find the 'getNumCurrentReplicas' on the passed <code>os</code> stream.
   * @return Method or null.
   */
  private Method getGetNumCurrentReplicas(final FSDataOutputStream os) {
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream()
          .getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getNumCurrentReplicas",
            new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support"
            + " getNumCurrentReplicas; --HDFS-826 not available; fsOut="
            + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info("Doesn't have access to getNumCurrentReplicas on "
            + "FileSystems's output stream --HDFS-826 not available; fsOut="
            + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    if (m != null) {
      LOG.info("Using getNumCurrentReplicas--HDFS-826");
    }
    return m;
  }

  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
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
      LOG.debug("Changed sequenceid from " + logSeqNum + " to " + newvalue);
    }
  }

  /**
   * @return log sequence number
   */
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  /**
   * Method used internal to this class and for tests only.
   * @return The wrapped stream our writer is using; its not the
   * writer's 'out' FSDatoOutputStream but the stream that this 'out' wraps
   * (In hdfs its an instance of DFSDataOutputStream).
   */
  // usage: see TestLogRolling.java
  OutputStream getOutputStream() {
    return this.hdfs_out.getWrappedStream();
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   *
   * <p>Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   *
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   *
   * <p>Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   *
   * @param force If true, force creation of a new writer even if no entries
   * have been written to the current writer
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter(boolean force)
      throws FailedLogCloseException, IOException {
    // Return if nothing to flush.
    if (!force && this.writer != null && this.numEntries.get() <= 0) {
      return null;
    }
    byte [][] regionsToFlush = null;
    this.cacheFlushLock.lock();
    this.logRollRunning = true;
    try {
      if (closed) {
        LOG.debug("HLog closed.  Skipping rolling of writer");
        return regionsToFlush;
      }
      // Do all the preparation outside of the updateLock to block
      // as less as possible the incoming writes
      long currentFilenum = this.filenum;
      Path oldPath = null;
      if (currentFilenum > 0) {
        //computeFilename  will take care of meta hlog filename
        oldPath = computeFilename(currentFilenum);
      }
      this.filenum = System.currentTimeMillis();
      Path newPath = computeFilename();

      // Tell our listeners that a new log is about to be created
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.preLogRoll(oldPath, newPath);
        }
      }
      HLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
      // Can we get at the dfsclient outputstream?  If an instance of
      // SFLW, it'll have done the necessary reflection to get at the
      // protected field name.
      FSDataOutputStream nextHdfsOut = null;
      if (nextWriter instanceof SequenceFileLogWriter) {
        nextHdfsOut = ((SequenceFileLogWriter)nextWriter).getWriterFSDataOutputStream();
        // perform the costly sync before we get the lock to roll writers.
        try {
          nextWriter.sync();
        } catch (IOException e) {
          // optimization failed, no need to abort here.
          LOG.warn("pre-sync failed", e);
        }
      }

      synchronized (updateLock) {
        // Clean up current writer.
        Path oldFile = cleanupCurrentWriter(currentFilenum);
        this.writer = nextWriter;
        this.hdfs_out = nextHdfsOut;

        long oldFileLen = 0;
        if (oldFile != null) {
          oldFileLen = this.fs.getFileStatus(oldFile).getLen();
          this.totalLogSize.addAndGet(oldFileLen);
        }
        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", filesize=" + oldFileLen + ". ": "") +
          " for " + FSUtils.getPath(newPath));
        this.numEntries.set(0);
      }
      // Tell our listeners that a new log was created
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.postLogRoll(oldPath, newPath);
        }
      }

      // Can we delete any of the old log files?
      if (this.outputfiles.size() > 0) {
        if (this.lastSeqWritten.isEmpty()) {
          LOG.debug("Last sequenceid written is empty. Deleting all old hlogs");
          // If so, then no new writes have come in since all regions were
          // flushed (and removed from the lastSeqWritten map). Means can
          // remove all but currently open log file.
          for (Map.Entry<Long, Path> e : this.outputfiles.entrySet()) {
            Path path = e.getValue();
            this.totalLogSize.addAndGet(-this.fs.getFileStatus(path).getLen());
            archiveLogFile(path, e.getKey());
          }
          this.outputfiles.clear();
        } else {
          regionsToFlush = cleanOldLogs();
        }
      }
    } finally {
      this.logRollRunning = false;
      this.cacheFlushLock.unlock();
    }
    return regionsToFlush;
  }

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   * 
   * @param fs
   * @param path
   * @param conf
   * @return Writer instance
   * @throws IOException
   */
  protected Writer createWriterInstance(final FileSystem fs, final Path path,
      final Configuration conf) throws IOException {
    if (forMeta) {
      //TODO: set a higher replication for the hlog files (HBASE-6773)
    }
    return this.hlogFs.createWriter(fs, conf, path);
  }

  /**
   * Get a reader for the WAL.
   * The proper way to tail a log that can be under construction is to first use this method
   * to get a reader then call {@link HLog.Reader#reset()} to see the new data. It will also
   * take care of keeping implementation-specific context (like compression).
   * @param fs
   * @param path
   * @param conf
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public static Reader getReader(final FileSystem fs, final Path path,
                                 Configuration conf)
      throws IOException {
    try {

      if (logReaderClass == null) {

        logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
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
      throw new IOException("cannot get log writer", e);
    }
  }

  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.  Returns array of
   * encoded region names to flush.
   * @throws IOException
   */
  private byte [][] cleanOldLogs() throws IOException {
    Long oldestOutstandingSeqNum = getOldestOutstandingSeqNum();
    // Get the set of all log files whose last sequence number is smaller than
    // the oldest edit's sequence number.
    TreeSet<Long> sequenceNumbers =
      new TreeSet<Long>(this.outputfiles.headMap(
        (Long.valueOf(oldestOutstandingSeqNum.longValue()))).keySet());
    // Now remove old log files (if any)
    int logsToRemove = sequenceNumbers.size();
    if (logsToRemove > 0) {
      if (LOG.isDebugEnabled()) {
        // Find associated region; helps debugging.
        byte [] oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
        LOG.debug("Found " + logsToRemove + " hlogs to remove" +
          " out of total " + this.outputfiles.size() + ";" +
          " oldest outstanding sequenceid is " + oldestOutstandingSeqNum +
          " from region " + Bytes.toStringBinary(oldestRegion));
      }
      for (Long seq : sequenceNumbers) {
        archiveLogFile(this.outputfiles.remove(seq), seq);
      }
    }

    // If too many log files, figure which regions we need to flush.
    // Array is an array of encoded region names.
    byte [][] regions = null;
    int logCount = this.outputfiles == null? 0: this.outputfiles.size();
    if (logCount > this.maxLogs && logCount > 0) {
      // This is an array of encoded region names.
      regions = findMemstoresWithEditsEqualOrOlderThan(this.outputfiles.firstKey(),
        this.lastSeqWritten);
      if (regions != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < regions.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(Bytes.toStringBinary(regions[i]));
        }
        LOG.info("Too many hlogs: logs=" + logCount + ", maxlogs=" +
           this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
           sb.toString());
      }
    }
    return regions;
  }

  /**
   * Return regions (memstores) that have edits that are equal or less than
   * the passed <code>oldestWALseqid</code>.
   * @param oldestWALseqid
   * @param regionsToSeqids Encoded region names to sequence ids
   * @return All regions whose seqid is < than <code>oldestWALseqid</code> (Not
   * necessarily in order).  Null if no regions found.
   */
  static byte [][] findMemstoresWithEditsEqualOrOlderThan(final long oldestWALseqid,
      final Map<byte [], Long> regionsToSeqids) {
    //  This method is static so it can be unit tested the easier.
    List<byte []> regions = null;
    for (Map.Entry<byte [], Long> e: regionsToSeqids.entrySet()) {
      if (e.getValue().longValue() <= oldestWALseqid) {
        if (regions == null) regions = new ArrayList<byte []>();
        // Key is encoded region name.
        regions.add(e.getKey());
      }
    }
    return regions == null?
      null: regions.toArray(new byte [][] {HConstants.EMPTY_BYTE_ARRAY});
  }

  /*
   * @return Logs older than this id are safe to remove.
   */
  private Long getOldestOutstandingSeqNum() {
    return Collections.min(this.lastSeqWritten.values());
  }

  /**
   * @param oldestOutstandingSeqNum
   * @return (Encoded) name of oldest outstanding region.
   */
  private byte [] getOldestRegion(final Long oldestOutstandingSeqNum) {
    byte [] oldestRegion = null;
    for (Map.Entry<byte [], Long> e: this.lastSeqWritten.entrySet()) {
      if (e.getValue().longValue() == oldestOutstandingSeqNum.longValue()) {
        // Key is encoded region name.
        oldestRegion = e.getKey();
        break;
      }
    }
    return oldestRegion;
  }

  /*
   * Cleans up current writer closing and adding to outputfiles.
   * Presumes we're operating inside an updateLock scope.
   * @return Path to current writer or null if none.
   * @throws IOException
   */
  Path cleanupCurrentWriter(final long currentfilenum) throws IOException {
    Path oldFile = null;
    if (this.writer != null) {
      // Close the current writer, get a new one.
      try {
        // Wait till all current transactions are written to the hlog.
        // No new transactions can occur because we have the updatelock.
        if (this.unflushedEntries.get() != this.syncedTillHere) {
          LOG.debug("cleanupCurrentWriter " +
                   " waiting for transactions to get synced " +
                   " total " + this.unflushedEntries.get() +
                   " synced till here " + syncedTillHere);
          sync();
        }
        this.writer.close();
        this.writer = null;
        closeErrorCount.set(0);
      } catch (IOException e) {
        LOG.error("Failed close of HLog writer", e);
        int errors = closeErrorCount.incrementAndGet();
        if (errors <= closeErrorsTolerated && !hasDeferredEntries()) {
          LOG.warn("Riding over HLog close failure! error count="+errors);
        } else {
          if (hasDeferredEntries()) {
            LOG.error("Aborting due to unflushed edits in HLog");
          }
          // Failed close of log file.  Means we're losing edits.  For now,
          // shut ourselves down to minimize loss.  Alternative is to try and
          // keep going.  See HBASE-930.
          FailedLogCloseException flce =
            new FailedLogCloseException("#" + currentfilenum);
          flce.initCause(e);
          throw flce;
        }
      }
      if (currentfilenum >= 0) {
        oldFile = computeFilename(currentfilenum);
        this.outputfiles.put(Long.valueOf(this.logSeqNum.get()), oldFile);
      }
    }
    return oldFile;
  }

  private void archiveLogFile(final Path p, final Long seqno) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p);
    LOG.info("moving old hlog file " + FSUtils.getPath(p) +
      " whose highest sequenceid is " + seqno + " to " +
      FSUtils.getPath(newPath));

    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    if (!HBaseFileSystem.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current HLog file-number
   * @return Path
   */
  protected Path computeFilename() {
    return computeFilename(this.filenum);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    String child = prefix + "." + filenum;
    if (forMeta) {
      child += HLog.META_HLOG_FILE_EXTN;
    }
    return new Path(dir, child);
  }

  public static boolean isMetaFile(Path p) {
    if (p.getName().endsWith(HLog.META_HLOG_FILE_EXTN)) {
      return true;
    }
    return false;
  }

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    if (!fs.exists(this.dir)) return;
    FileStatus[] files = fs.listStatus(this.dir);
    for(FileStatus file : files) {

      Path p = getHLogArchivePath(this.oldLogDir, file.getPath());
      // Tell our listeners that a log is going to be archived.
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.preLogArchive(file.getPath(), p);
        }
      }
      if (!HBaseFileSystem.renameAndSetModifyTime(fs, file.getPath(), p)) {
        throw new IOException("Unable to rename " + file.getPath() + " to " + p);
      }
      // Tell our listeners that a log was archived.
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.postLogArchive(file.getPath(), p);
        }
      }
    }
    LOG.debug("Moved " + files.length + " log files to " +
      FSUtils.getPath(this.oldLogDir));
    if (!HBaseFileSystem.deleteDirFromFileSystem(fs, dir)) {
      LOG.info("Unable to delete " + dir);
    }
  }

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    // When optionalFlushInterval is 0, the logSyncer is not started as a Thread.
    if (this.optionalFlushInterval > 0) {
      try {
        logSyncer.close();
        // Make sure we synced everything
        logSyncer.join(this.optionalFlushInterval * 2);
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for syncer thread to die", e);
      }
    }

    cacheFlushLock.lock();
    try {
      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.logCloseRequested();
        }
      }
      synchronized (updateLock) {
        this.closed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing hlog writer in " + this.dir.toString());
        }
        if (this.writer != null) {
          this.writer.close();
        }
      }
    } finally {
      cacheFlushLock.unlock();
    }
  }

  /**
   * @param now
   * @param regionName
   * @param tableName
   * @param clusterId
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqnum,
      long now, UUID clusterId) {
    return new HLogKey(regionName, tableName, seqnum, now, clusterId);
  }


  /** Append an entry to the log.
   *
   * @param regionInfo
   * @param logEdit
   * @param logKey
   * @param doSync shall we sync after writing the transaction
   * @return The txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo regionInfo, HLogKey logKey, WALEdit logEdit,
                     HTableDescriptor htd, boolean doSync)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    long txid = 0;
    synchronized (updateLock) {
      long seqNum = obtainSeqNum();
      logKey.setLogSeqNum(seqNum);
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionInfo.getEncodedNameAsBytes(),
        Long.valueOf(seqNum));
      doWrite(regionInfo, logKey, logEdit, htd);
      txid = this.unflushedEntries.incrementAndGet();
      this.numEntries.incrementAndGet();
      if (htd.isDeferredLogFlush()) {
        lastDeferredTxid = txid;
      }
    }

    // Sync if catalog region, and if not then check if that table supports
    // deferred log flushing
    if (doSync &&
        (regionInfo.isMetaRegion() ||
        !htd.isDeferredLogFlush())) {
      // sync txn to file system
      this.sync(txid);
    }
    return txid;
  }

  /**
   * Only used in tests.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @param htd
   * @throws IOException
   */
  public void append(HRegionInfo info, byte [] tableName, WALEdit edits,
    final long now, HTableDescriptor htd)
  throws IOException {
    append(info, tableName, edits, HConstants.DEFAULT_CLUSTER_ID, now, htd);
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id.
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
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @param doSync shall we sync?
   * @return txid of this transaction
   * @throws IOException
   */
  private long append(HRegionInfo info, byte [] tableName, WALEdit edits, UUID clusterId,
      final long now, HTableDescriptor htd, boolean doSync)
    throws IOException {
      if (edits.isEmpty()) return this.unflushedEntries.get();;
      if (this.closed) {
        throw new IOException("Cannot append; log is closed");
      }
      long txid = 0;
      synchronized (this.updateLock) {
        long seqNum = obtainSeqNum();
        // The 'lastSeqWritten' map holds the sequence number of the oldest
        // write for each region (i.e. the first edit added to the particular
        // memstore). . When the cache is flushed, the entry for the
        // region being flushed is removed if the sequence number of the flush
        // is greater than or equal to the value in lastSeqWritten.
        // Use encoded name.  Its shorter, guaranteed unique and a subset of
        // actual  name.
        byte [] encodedRegionName = info.getEncodedNameAsBytes();
        this.lastSeqWritten.putIfAbsent(encodedRegionName, seqNum);
        HLogKey logKey = makeKey(encodedRegionName, tableName, seqNum, now, clusterId);
        doWrite(info, logKey, edits, htd);
        this.numEntries.incrementAndGet();
        txid = this.unflushedEntries.incrementAndGet();
        if (htd.isDeferredLogFlush()) {
          lastDeferredTxid = txid;
        }
      }
      // Sync if catalog region, and if not then check if that table supports
      // deferred log flushing
      if (doSync && 
          (info.isMetaRegion() ||
          !htd.isDeferredLogFlush())) {
        // sync txn to file system
        this.sync(txid);
      }
      return txid;
    }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is not flushed
   * after this transaction is written to the log.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @return txid of this transaction
   * @throws IOException
   */
  public long appendNoSync(HRegionInfo info, byte [] tableName, WALEdit edits, 
    UUID clusterId, final long now, HTableDescriptor htd)
    throws IOException {
    return append(info, tableName, edits, clusterId, now, htd, false);
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is flushed
   * after this transaction is written to the log.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @return txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo info, byte [] tableName, WALEdit edits, 
    UUID clusterId, final long now, HTableDescriptor htd)
    throws IOException {
    return append(info, tableName, edits, clusterId, now, htd, true);
  }

  /**
   * This class is responsible to hold the HLog's appended Entry list
   * and to sync them according to a configurable interval.
   *
   * Deferred log flushing works first by piggy backing on this process by
   * simply not sync'ing the appended Entry. It can also be sync'd by other
   * non-deferred log flushed entries outside of this thread.
   */
  class LogSyncer extends HasThread {

    private final long optionalFlushInterval;

    private AtomicBoolean closeLogSyncer = new AtomicBoolean(false);

    // List of pending writes to the HLog. There corresponds to transactions
    // that have not yet returned to the client. We keep them cached here
    // instead of writing them to HDFS piecemeal, because the HDFS write 
    // method is pretty heavyweight as far as locking is concerned. The 
    // goal is to increase the batchsize for writing-to-hdfs as well as
    // sync-to-hdfs, so that we can get better system throughput.
    private List<Entry> pendingWrites = new LinkedList<Entry>();

    LogSyncer(long optionalFlushInterval) {
      this.optionalFlushInterval = optionalFlushInterval;
    }

    @Override
    public void run() {
      try {
        // awaiting with a timeout doesn't always
        // throw exceptions on interrupt
        while(!this.isInterrupted() && !closeLogSyncer.get()) {

          try {
            if (unflushedEntries.get() <= syncedTillHere) {
              synchronized (closeLogSyncer) {
                closeLogSyncer.wait(this.optionalFlushInterval);
              }
            }
            // Calling sync since we waited or had unflushed entries.
            // Entries appended but not sync'd are taken care of here AKA
            // deferred log flush
            sync();
          } catch (IOException e) {
            LOG.error("Error while syncing, requesting close of hlog ", e);
            requestLogRoll();
          }
        }
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for sync requests");
      } finally {
        LOG.info(getName() + " exiting");
      }
    }

    // appends new writes to the pendingWrites. It is better to keep it in
    // our own queue rather than writing it to the HDFS output stream because
    // HDFSOutputStream.writeChunk is not lightweight at all.
    synchronized void append(Entry e) throws IOException {
      pendingWrites.add(e);
    }

    // Returns all currently pending writes. New writes
    // will accumulate in a new list.
    synchronized List<Entry> getPendingWrites() {
      List<Entry> save = this.pendingWrites;
      this.pendingWrites = new LinkedList<Entry>();
      return save;
    }

    // writes out pending entries to the HLog
    void hlogFlush(Writer writer, List<Entry> pending) throws IOException {
      if (pending == null) return;

      // write out all accumulated Entries to hdfs.
      for (Entry e : pending) {
        writer.append(e);
      }
    }

    void close() {
      synchronized (closeLogSyncer) {
        closeLogSyncer.set(true);
        closeLogSyncer.notifyAll();
      }
    }
  }

  // sync all known transactions
  private void syncer() throws IOException {
    syncer(this.unflushedEntries.get()); // sync all pending items
  }

  // sync all transactions upto the specified txid
  private void syncer(long txid) throws IOException {
    // if the transaction that we are interested in is already
    // synced, then return immediately.
    if (txid <= this.syncedTillHere) {
      return;
    }
    Writer tempWriter;
    synchronized (this.updateLock) {
      if (this.closed) return;
      tempWriter = this.writer; // guaranteed non-null
    }
    try {
      long doneUpto;
      long now = System.currentTimeMillis();
      // First flush all the pending writes to HDFS. Then 
      // issue the sync to HDFS. If sync is successful, then update
      // syncedTillHere to indicate that transactions till this
      // number has been successfully synced.
      IOException ioe = null;
      List<Entry> pending = null;
      synchronized (flushLock) {
        if (txid <= this.syncedTillHere) {
          return;
        }
        doneUpto = this.unflushedEntries.get();
        pending = logSyncer.getPendingWrites();
        try {
          logSyncer.hlogFlush(tempWriter, pending);
        } catch(IOException io) {
          ioe = io;
          LOG.error("syncer encountered error, will retry. txid=" + txid, ioe);
        }
      }
      if (ioe != null && pending != null) {
        synchronized (this.updateLock) {
          synchronized (flushLock) {
            // HBASE-4387, HBASE-5623, retry with updateLock held
            tempWriter = this.writer;
            logSyncer.hlogFlush(tempWriter, pending);
          }
        }
      }
      // another thread might have sync'ed avoid double-sync'ing
      if (txid <= this.syncedTillHere) {
        return;
      }
      try {
        tempWriter.sync();
      } catch (IOException io) {
        synchronized (this.updateLock) {
          // HBASE-4387, HBASE-5623, retry with updateLock held
          tempWriter = this.writer;
          tempWriter.sync();
        }
      }
      this.syncedTillHere = Math.max(this.syncedTillHere, doneUpto);

      syncTime.inc(System.currentTimeMillis() - now);
      if (!this.logRollRunning) {
        checkLowReplication();
        try {
          curLogSize = tempWriter.getLength();
          if (curLogSize > this.logrollsize) {
            requestLogRoll();
          }
        } catch (IOException x) {
          LOG.debug("Log roll failed and will be retried. (This is not an error)");
        }
      }
    } catch (IOException e) {
      LOG.fatal("Could not sync. Requesting close of hlog", e);
      requestLogRoll();
      throw e;
    }
  }

  private void checkLowReplication() {
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0
          && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of hlog.");
            requestLogRoll();
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls++;
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls = 0;
            this.lowReplicationRollEnabled = false;
          }
        }
      } else if (numCurrentReplicas >= this.minTolerableReplication) {

        if (!this.lowReplicationRollEnabled) {
          // The new writer's log replicas is always the default value.
          // So we should not enable LowReplication-Roller. If numEntries
          // is lower than or equals 1, we consider it as a new writer.
          if (this.numEntries.get() <= 1) {
            return;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
          " still proceeding ahead...");
    }
  }

  /**
   * This method gets the datanode replication count for the current HLog.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   *
   * @throws Exception
   */
  int getLogReplication()
  throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    if (this.getNumCurrentReplicas != null && this.hdfs_out != null) {
      Object repl = this.getNumCurrentReplicas.invoke(getOutputStream(), NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }

  boolean canGetCurReplicas() {
    return this.getNumCurrentReplicas != null;
  }

  public void hsync() throws IOException {
    syncer();
  }

  public void hflush() throws IOException {
    syncer();
  }

  public void sync() throws IOException {
    syncer();
  }

  public void sync(long txid) throws IOException {
    syncer(txid);
  }

  private void requestLogRoll() {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.logRollRequested();
      }
    }
  }

  protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit,
                           HTableDescriptor htd)
  throws IOException {
    if (!this.enabled) {
      return;
    }
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.visitLogEntryBeforeWrite(htd, logKey, logEdit);
      }
    }
    try {
      long now = System.currentTimeMillis();
      // coprocessor hook:
      if (!coprocessorHost.preWALWrite(info, logKey, logEdit)) {
        // write to our buffer for the Hlog file.
        logSyncer.append(new HLog.Entry(logKey, logEdit));
      }
      long took = System.currentTimeMillis() - now;
      coprocessorHost.postWALWrite(info, logKey, logEdit);
      writeTime.inc(took);
      long len = 0;
      for (KeyValue kv : logEdit.getKeyValues()) {
        len += kv.getLength();
      }
      writeSize.inc(len);
      if (took > 1000) {
        LOG.warn(String.format(
          "%s took %d ms appending an edit to hlog; editcount=%d, len~=%s",
          Thread.currentThread().getName(), took, this.numEntries.get(),
          StringUtils.humanReadableInt(len)));
        slowHLogAppendCount.incrementAndGet();
        slowHLogAppendTime.inc(took);
      }
    } catch (IOException e) {
      LOG.fatal("Could not append. Requesting close of hlog", e);
      requestLogRoll();
      throw e;
    }
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

  /** @return the number of log files in use, including current one */
  public int getNumLogFiles() {
    return outputfiles.size() + 1;
  }
  
  /** @return the total size of log files in use, including current one */
  public long getNumLogFileSize() {
    return totalLogSize.get() + curLogSize;
  }

  private byte[] getSnapshotName(byte[] encodedRegionName) {
    byte snp[] = new byte[encodedRegionName.length + 3];
    // an encoded region name has only hex digits. s, n or p are not hex
    // and therefore snapshot-names will never collide with
    // encoded-region-names
    snp[0] = 's'; snp[1] = 'n'; snp[2] = 'p';
    for (int i = 0; i < encodedRegionName.length; i++) {
      snp[i+3] = encodedRegionName[i];
    }
    return snp;
  }

  /**
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   *
   * Acquire a lock so that we do not roll the log between the start and
   * completion of a cache-flush. Otherwise the log-seq-id for the flush will
   * not appear in the correct logfile.
   *
   * Ensuring that flushes and log-rolls don't happen concurrently also allows
   * us to temporarily put a log-seq-number in lastSeqWritten against the region
   * being flushed that might not be the earliest in-memory log-seq-number for
   * that region. By the time the flush is completed or aborted and before the
   * cacheFlushLock is released it is ensured that lastSeqWritten again has the
   * oldest in-memory edit's lsn for the region that was being flushed.
   *
   * In this method, by removing the entry in lastSeqWritten for the region
   * being flushed we ensure that the next edit inserted in this region will be
   * correctly recorded in {@link #append(HRegionInfo, byte[], WALEdit, long, HTableDescriptor)} The
   * lsn of the earliest in-memory lsn - which is now in the memstore snapshot -
   * is saved temporarily in the lastSeqWritten map while the flush is active.
   *
   * @return sequence ID to pass
   *         {@link #completeCacheFlush(byte[], byte[], long, boolean)} (byte[],
   *         byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long, boolean)
   * @see #abortCacheFlush(byte[])
   */
  public long startCacheFlush(final byte[] encodedRegionName) {
    this.cacheFlushLock.lock();
    Long seq = this.lastSeqWritten.remove(encodedRegionName);
    // seq is the lsn of the oldest edit associated with this region. If a
    // snapshot already exists - because the last flush failed - then seq will
    // be the lsn of the oldest edit in the snapshot
    if (seq != null) {
      // keeping the earliest sequence number of the snapshot in
      // lastSeqWritten maintains the correctness of
      // getOldestOutstandingSeqNum(). But it doesn't matter really because
      // everything is being done inside of cacheFlush lock.
      Long oldseq =
        lastSeqWritten.put(getSnapshotName(encodedRegionName), seq);
      if (oldseq != null) {
        LOG.error("Logic Error Snapshot seq id from earlier flush still" +
            " present! for region " + Bytes.toString(encodedRegionName) +
            " overwritten oldseq=" + oldseq + "with new seq=" + seq);
        Runtime.getRuntime().halt(1);
      }
    }
    return obtainSeqNum();
  }


  /**
   * Complete the cache flush
   *
   * Protected by cacheFlushLock
   *
   * @param encodedRegionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  public void completeCacheFlush(final byte [] encodedRegionName,
      final byte [] tableName, final long logSeqId, final boolean isMetaRegion)
  throws IOException {
    try {
      if (this.closed) {
        return;
      }
      long txid = 0;
      synchronized (updateLock) {
        long now = System.currentTimeMillis();
        WALEdit edit = completeCacheFlushLogEdit();
        HLogKey key = makeKey(encodedRegionName, tableName, logSeqId,
            System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
        logSyncer.append(new Entry(key, edit));
        txid = this.unflushedEntries.incrementAndGet();
        writeTime.inc(System.currentTimeMillis() - now);
        long len = 0;
        for (KeyValue kv : edit.getKeyValues()) {
          len += kv.getLength();
        }
        writeSize.inc(len);
        this.numEntries.incrementAndGet();
      }
      // sync txn to file system
      this.sync(txid);

    } finally {
      // updateLock not needed for removing snapshot's entry
      // Cleaning up of lastSeqWritten is in the finally clause because we
      // don't want to confuse getOldestOutstandingSeqNum()
      this.lastSeqWritten.remove(getSnapshotName(encodedRegionName));
      this.cacheFlushLock.unlock();
    }
  }

  private WALEdit completeCacheFlushLogEdit() {
    KeyValue kv = new KeyValue(METAROW, METAFAMILY, null,
      System.currentTimeMillis(), COMPLETE_CACHE_FLUSH);
    WALEdit e = new WALEdit();
    e.add(kv);
    return e;
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memstore.
   */
  public void abortCacheFlush(byte[] encodedRegionName) {
    Long snapshot_seq =
      this.lastSeqWritten.remove(getSnapshotName(encodedRegionName));
    if (snapshot_seq != null) {
      // updateLock not necessary because we are racing against
      // lastSeqWritten.putIfAbsent() in append() and we will always win
      // before releasing cacheFlushLock make sure that the region's entry in
      // lastSeqWritten points to the earliest edit in the region
      Long current_memstore_earliest_seq =
        this.lastSeqWritten.put(encodedRegionName, snapshot_seq);
      if (current_memstore_earliest_seq != null &&
          (current_memstore_earliest_seq.longValue() <=
            snapshot_seq.longValue())) {
        LOG.error("Logic Error region " + Bytes.toString(encodedRegionName) +
            "acquired edits out of order current memstore seq=" +
            current_memstore_earliest_seq + " snapshot seq=" + snapshot_seq);
        Runtime.getRuntime().halt(1);
      }
    }
    this.cacheFlushLock.unlock();
  }

  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte [] family) {
    return Bytes.equals(METAFAMILY, family);
  }

  /**
   * Get LowReplication-Roller status
   * 
   * @return lowReplicationRollEnabled
   */
  public boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
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
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Get the directory we are making logs in.
   * 
   * @return dir
   */
  protected Path getDir() {
    return dir;
  }
  
  /**
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an HLog, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir)) return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
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
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splithlog thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
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
    if (!HBaseFileSystem.renameDirForFileSystem(fs, edits, moveAsideName)) {
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

  private static void split(final Configuration conf, final Path p)
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

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(
        conf, baseDir, p, oldLogDir, fs);
    logSplitter.splitLog();
  }

  /**
   * @return Coprocessor host.
   */
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** Provide access to currently deferred sequence num for tests */
  boolean hasDeferredEntries() {
    return lastDeferredTxid > syncedTillHere;
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
          split(conf, logPath);
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
}
