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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
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
@InterfaceAudience.Private
class FSHLog implements HLog, Syncable {
  static final Log LOG = LogFactory.getLog(FSHLog.class);
  
  private final FileSystem fs;
  private final Path rootDir;
  private final Path dir;
  private final Configuration conf;
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
  private boolean failIfLogDirExists;

  private WALCoprocessorHost coprocessorHost;

  private FSDataOutputStream hdfs_out; // FSDataOutputStream associated with the current SequenceFile.writer
  // Minimum tolerable replicas, if the actual value is lower than it, 
  // rollWriter will be triggered
  private int minTolerableReplication;
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

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

  // The timestamp (in ms) when the log file was created.
  private volatile long filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private AtomicInteger consecutiveLogRolls = new AtomicInteger(0);
  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
  private final long logrollsize;

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
  private final LogSyncer logSyncerThread;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();
  
  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived hlogs
   * @param logName dir where hlogs are stored
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logName,
                final Configuration conf)
  throws IOException {
    this(fs, root, logName, HConstants.HREGION_OLDLOGDIR_NAME, 
        conf, null, true, null);
  }
  
  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived hlogs
   * @param logName dir where hlogs are stored
   * @param oldLogName dir where hlogs are archived
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logName,
                final String oldLogName, final Configuration conf)
  throws IOException {
    this(fs, root, logName, oldLogName, 
        conf, null, true, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived hlogs
   * @param logName dir where hlogs are stored
   * @param oldLogName dir where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logName,
      final Configuration conf, final List<WALActionsListener> listeners,
      final String prefix) throws IOException {
    this(fs, root, logName, HConstants.HREGION_OLDLOGDIR_NAME, 
        conf, listeners, true, prefix);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param root path to where logs and oldlogs
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfLogDirExists If true IOException will be thrown if dir already exists.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  private FSHLog(final FileSystem fs, final Path root, final String logName,
      final String oldLogName, final Configuration conf, 
      final List<WALActionsListener> listeners,
      final boolean failIfLogDirExists, final String prefix)
  throws IOException {
    super();
    this.fs = fs;
    this.rootDir = root;
    this.dir = new Path(this.rootDir, logName);
    this.oldLogDir = new Path(this.rootDir, oldLogName);
    this.conf = conf;
   
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }
    
    this.failIfLogDirExists = failIfLogDirExists;
    
    this.blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
        getDefaultBlockSize());
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);
    
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.minTolerableReplication = conf.getInt(
        "hbase.regionserver.hlog.tolerable.lowreplication",
        this.fs.getDefaultReplication());
    this.lowReplicationRollLimit = conf.getInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    this.closeErrorsTolerated = conf.getInt(
        "hbase.regionserver.logroll.errors.tolerated", 0);
    
    this.logSyncerThread = new LogSyncer(this.optionalFlushInterval);
    
    LOG.info("HLog configuration: blocksize=" +
      StringUtils.byteDesc(this.blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", enabled=" + this.enabled +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");
   
    if (failIfLogDirExists && this.fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    if (!fs.mkdirs(dir)) {
      throw new IOException("Unable to mkdir " + dir);
    }

    if (!fs.exists(oldLogDir)) {
      if (!fs.mkdirs(this.oldLogDir)) {
        throw new IOException("Unable to mkdir " + this.oldLogDir);
      }
    }
    // rollWriter sets this.hdfs_out if it can.
    rollWriter();
    
    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = getGetNumCurrentReplicas(this.hdfs_out);

    Threads.setDaemonThreadRunning(logSyncerThread.getThread(),
        Thread.currentThread().getName() + ".logSyncer");
    coprocessorHost = new WALCoprocessorHost(this, conf);
  }
  
  // use reflection to search for getDefaultBlockSize(Path f)
  // if the method doesn't exist, fall back to using getDefaultBlockSize()
  private long getDefaultBlockSize() throws IOException {
    Method m = null;
    Class<? extends FileSystem> cls = this.fs.getClass();
    try {
      m = cls.getMethod("getDefaultBlockSize",
          new Class<?>[] { Path.class });
    } catch (NoSuchMethodException e) {
      LOG.info("FileSystem doesn't support getDefaultBlockSize");
    } catch (SecurityException e) {
      LOG.info("Doesn't have access to getDefaultBlockSize on "
          + "FileSystems", e);
      m = null; // could happen on setAccessible()
    }
    if (null == m) {
      return this.fs.getDefaultBlockSize();
    } else {
      try {
        Object ret = m.invoke(this.fs, this.dir);
        return ((Long)ret).longValue();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
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

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public long getFilenum() {
    return this.filenum;
  }

  @Override
  public void setSequenceNumber(final long newvalue) {
    for (long id = this.logSeqNum.get(); id < newvalue &&
        !this.logSeqNum.compareAndSet(id, newvalue); id = this.logSeqNum.get()) {
      // This could spin on occasion but better the occasional spin than locking
      // every increment of sequence number.
      LOG.debug("Changed sequenceid from " + logSeqNum + " to " + newvalue);
    }
  }

  @Override
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  /**
   * Method used internal to this class and for tests only.
   * @return The wrapped stream our writer is using; its not the
   * writer's 'out' FSDatoOutputStream but the stream that this 'out' wraps
   * (In hdfs its an instance of DFSDataOutputStream).
   * 
   * usage: see TestLogRolling.java
   */
  OutputStream getOutputStream() {
    return this.hdfs_out.getWrappedStream();
  }

  @Override
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  @Override
  public byte [][] rollWriter(boolean force)
      throws FailedLogCloseException, IOException {
    // Return if nothing to flush.
    if (!force && this.writer != null && this.numEntries.get() <= 0) {
      return null;
    }
    byte [][] regionsToFlush = null;
    this.cacheFlushLock.lock();
    try {
      this.logRollRunning = true;
      if (closed) {
        LOG.debug("HLog closed.  Skipping rolling of writer");
        return regionsToFlush;
      }
      // Do all the preparation outside of the updateLock to block
      // as less as possible the incoming writes
      long currentFilenum = this.filenum;
      Path oldPath = null;
      if (currentFilenum > 0) {
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
      FSHLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
      // Can we get at the dfsclient outputstream?  If an instance of
      // SFLW, it'll have done the necessary reflection to get at the
      // protected field name.
      FSDataOutputStream nextHdfsOut = null;
      if (nextWriter instanceof SequenceFileLogWriter) {
        nextHdfsOut = ((SequenceFileLogWriter)nextWriter).getWriterFSDataOutputStream();
      }
      // Tell our listeners that a new log was created
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.postLogRoll(oldPath, newPath);
        }
      }

      synchronized (updateLock) {
        // Clean up current writer.
        Path oldFile = cleanupCurrentWriter(currentFilenum);
        this.writer = nextWriter;
        this.hdfs_out = nextHdfsOut;

        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", filesize=" +
            this.fs.getFileStatus(oldFile).getLen() + ". ": "") +
          " for " + FSUtils.getPath(newPath));
        this.numEntries.set(0);
      }
      // Can we delete any of the old log files?
      if (this.outputfiles.size() > 0) {
        if (this.lastSeqWritten.isEmpty()) {
          LOG.debug("Last sequenceid written is empty. Deleting all old hlogs");
          // If so, then no new writes have come in since all regions were
          // flushed (and removed from the lastSeqWritten map). Means can
          // remove all but currently open log file.
          for (Map.Entry<Long, Path> e : this.outputfiles.entrySet()) {
            archiveLogFile(e.getValue(), e.getKey());
          }
          this.outputfiles.clear();
        } else {
          regionsToFlush = cleanOldLogs();
        }
      }
    } finally {
      try {
        this.logRollRunning = false;
      } finally {
        this.cacheFlushLock.unlock();
      }
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
    return HLogFactory.createWriter(fs, path, conf);
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
    TreeSet<Long> sequenceNumbers = new TreeSet<Long>(this.outputfiles.headMap(
        oldestOutstandingSeqNum).keySet());
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
    int logCount = this.outputfiles.size();
    if (logCount > this.maxLogs && logCount > 0) {
      // This is an array of encoded region names.
      regions = HLogUtil.findMemstoresWithEditsEqualOrOlderThan(this.outputfiles.firstKey(),
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
    if (!this.fs.rename(p, newPath)) {
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
    return new Path(dir, prefix + "." + filenum);
  }

  @Override
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

      if (!fs.rename(file.getPath(),p)) {
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
    if (!fs.delete(dir, true)) {
      LOG.info("Unable to delete " + dir);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      logSyncerThread.close();
      // Make sure we synced everything
      logSyncerThread.join(this.optionalFlushInterval*2);
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for syncer thread to die", e);
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

  @Override
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

  @Override
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

  @Override
  public long appendNoSync(HRegionInfo info, byte [] tableName, WALEdit edits, 
    UUID clusterId, final long now, HTableDescriptor htd)
    throws IOException {
    return append(info, tableName, edits, clusterId, now, htd, false);
  }

  @Override
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
    Writer tempWriter;
    synchronized (this.updateLock) {
      if (this.closed) return;
      tempWriter = this.writer; // guaranteed non-null
    }
    // if the transaction that we are interested in is already 
    // synced, then return immediately.
    if (txid <= this.syncedTillHere) {
      return;
    }
    try {
      long doneUpto;
      long now = System.currentTimeMillis();
      // First flush all the pending writes to HDFS. Then 
      // issue the sync to HDFS. If sync is successful, then update
      // syncedTillHere to indicate that transactions till this
      // number has been successfully synced.
      synchronized (flushLock) {
        if (txid <= this.syncedTillHere) {
          return;
        }
        doneUpto = this.unflushedEntries.get();
        List<Entry> pending = logSyncerThread.getPendingWrites();
        try {
          logSyncerThread.hlogFlush(tempWriter, pending);
        } catch(IOException io) {
          synchronized (this.updateLock) {
            // HBASE-4387, HBASE-5623, retry with updateLock held
            tempWriter = this.writer;
            logSyncerThread.hlogFlush(tempWriter, pending);
          }
        }
      }
      // another thread might have sync'ed avoid double-sync'ing
      if (txid <= this.syncedTillHere) {
        return;
      }
      try {
        tempWriter.sync();
      } catch(IOException io) {
        synchronized (this.updateLock) {
          // HBASE-4387, HBASE-5623, retry with updateLock held
          tempWriter = this.writer;
          tempWriter.sync();
        }
      }
      this.syncedTillHere = Math.max(this.syncedTillHere, doneUpto);

      HLogMetrics.syncTime.inc(System.currentTimeMillis() - now);
      if (!this.logRollRunning) {
        checkLowReplication();
        try {
          if (tempWriter.getLength() > this.logrollsize) {
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
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of hlog.");
            requestLogRoll();
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls.getAndIncrement();
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls.set(0);
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
        logSyncerThread.append(new FSHLog.Entry(logKey, logEdit));
      }
      long took = System.currentTimeMillis() - now;
      coprocessorHost.postWALWrite(info, logKey, logEdit);
      HLogMetrics.writeTime.inc(took);
      long len = 0;
      for (KeyValue kv : logEdit.getKeyValues()) {
        len += kv.getLength();
      }
      HLogMetrics.writeSize.inc(len);
      if (took > 1000) {
        LOG.warn(String.format(
          "%s took %d ms appending an edit to hlog; editcount=%d, len~=%s",
          Thread.currentThread().getName(), took, this.numEntries.get(),
          StringUtils.humanReadableInt(len)));
        HLogMetrics.slowHLogAppendCount.incrementAndGet();
        HLogMetrics.slowHLogAppendTime.inc(took);
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

  @Override
  public long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }

  /** @return the number of log files in use */
  int getNumLogFiles() {
    return outputfiles.size();
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

  @Override
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

  @Override
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
        logSyncerThread.append(new Entry(key, edit));
        txid = this.unflushedEntries.incrementAndGet();
        HLogMetrics.writeTime.inc(System.currentTimeMillis() - now);
        long len = 0;
        for (KeyValue kv : edit.getKeyValues()) {
          len += kv.getLength();
        }
        HLogMetrics.writeSize.inc(len);
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
    KeyValue kv = new KeyValue(HLog.METAROW, HLog.METAFAMILY, null,
      System.currentTimeMillis(), HLogUtil.COMPLETE_CACHE_FLUSH);
    WALEdit e = new WALEdit();
    e.add(kv);
    return e;
  }

  @Override
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

  @Override
  public boolean isLowReplicationRollEnabled() {
      return lowReplicationRollEnabled;
  }

  /**
   * Get the directory we are making logs in.
   * 
   * @return dir
   */
  protected Path getDir() {
    return dir;
  }
  
  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
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
  
  @Override
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
