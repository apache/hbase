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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DrainBarrier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;

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
 * <p>To read an HLog, call {@link HLogFactory#createReader(org.apache.hadoop.fs.FileSystem,
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

  private WALCoprocessorHost coprocessorHost;

  private FSDataOutputStream hdfs_out; // FSDataOutputStream associated with the current SequenceFile.writer
  // Minimum tolerable replicas, if the actual value is lower than it,
  // rollWriter will be triggered
  private int minTolerableReplication;
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

  /** The barrier used to ensure that close() waits for all log rolls and flushes to finish. */
  private DrainBarrier closeBarrier = new DrainBarrier();

  /**
   * Current log file.
   */
  Writer writer;

  /**
   * This lock synchronizes all operations on oldestUnflushedSeqNums and oldestFlushingSeqNums,
   * with the exception of append's putIfAbsent into oldestUnflushedSeqNums.
   * We only use these to find out the low bound seqNum, or to find regions with old seqNums to
   * force flush them, so we don't care about these numbers messing with anything. */
  private final Object oldestSeqNumsLock = new Object();

  /**
   * This lock makes sure only one log roll runs at the same time. Should not be taken while
   * any other lock is held. We don't just use synchronized because that results in bogus and
   * tedious findbugs warning when it thinks synchronized controls writer thread safety */
  private final Object rollWriterLock = new Object();

  /**
   * Map of encoded region names to their most recent sequence/edit id in their memstore.
   */
  private final ConcurrentSkipListMap<byte [], Long> oldestUnflushedSeqNums =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
  /**
   * Map of encoded region names to their most recent sequence/edit id in their memstore;
   * contains the regions that are currently flushing. That way we can store two numbers for
   * flushing and non-flushing (oldestUnflushedSeqNums) memstore for the same region.
   */
  private final Map<byte[], Long> oldestFlushingSeqNums =
    new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private boolean forMeta = false;

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
  private final MetricsWAL metrics;
/**
 * Map of region encoded names to the latest sequence num obtained from them while appending
 * WALEdits to the wal. We create one map for each WAL file at the time it is rolled.
 * <p>
 * When deciding whether to archive a WAL file, we compare the sequence IDs in this map to
 * {@link #oldestFlushingSeqNums} and {@link #oldestUnflushedSeqNums}.
 * See {@link FSHLog#areAllRegionsFlushed(Map, Map, Map)} for more info.
 * <p>
 * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
 * use {@link HRegionInfo#getEncodedNameAsBytes()} as keys. For a given region, it always returns
 * the same array.
 */
  private Map<byte[], Long> latestSequenceNums = new HashMap<byte[], Long>();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name.
   */
  public final Comparator<Path> LOG_NAME_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      long t1 = getFileNumFromFileName(o1);
      long t2 = getFileNumFromFileName(o2);
      if (t1 == t2) return 0;
      return (t1 > t2) ? 1 : -1;
    }
  };

  /**
   * Map of log file to the latest sequence nums of all regions it has entries of.
   * The map is sorted by the log file creation timestamp (contained in the log file name).
   */
  private NavigableMap<Path, Map<byte[], Long>> hlogSequenceNums =
    new ConcurrentSkipListMap<Path, Map<byte[], Long>>(LOG_NAME_COMPARATOR);

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived hlogs
   * @param logDir dir where hlogs are stored
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir,
                final Configuration conf)
  throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME,
        conf, null, true, null, false);
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived hlogs
   * @param logDir dir where hlogs are stored
   * @param oldLogDir dir where hlogs are archived
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir,
                final String oldLogDir, final Configuration conf)
  throws IOException {
    this(fs, root, logDir, oldLogDir,
        conf, null, true, null, false);
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
   * @param logDir dir where hlogs are stored
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir,
      final Configuration conf, final List<WALActionsListener> listeners,
      final String prefix) throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME,
        conf, listeners, true, prefix, false);
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
   * @param logDir dir where hlogs are stored
   * @param oldLogDir dir where hlogs are archived
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
  public FSHLog(final FileSystem fs, final Path root, final String logDir,
      final String oldLogDir, final Configuration conf,
      final List<WALActionsListener> listeners,
      final boolean failIfLogDirExists, final String prefix, boolean forMeta)
  throws IOException {
    super();
    this.fs = fs;
    this.rootDir = root;
    this.dir = new Path(this.rootDir, logDir);
    this.oldLogDir = new Path(this.rootDir, oldLogDir);
    this.forMeta = forMeta;
    this.conf = conf;

    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }

    this.blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.dir));
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);

    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.minTolerableReplication = conf.getInt(
        "hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(fs, this.dir));
    this.lowReplicationRollLimit = conf.getInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    this.closeErrorsTolerated = conf.getInt(
        "hbase.regionserver.logroll.errors.tolerated", 0);

    this.logSyncer = new LogSyncer(this.optionalFlushInterval);

    LOG.info("WAL/HLog configuration: blocksize=" +
      StringUtils.byteDesc(this.blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", enabled=" + this.enabled +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");

    boolean dirExists = false;
    if (failIfLogDirExists && (dirExists = this.fs.exists(dir))) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    if (!dirExists && !fs.mkdirs(dir)) {
      throw new IOException("Unable to mkdir " + dir);
    }

    if (!fs.exists(this.oldLogDir)) {
      if (!fs.mkdirs(this.oldLogDir)) {
        throw new IOException("Unable to mkdir " + this.oldLogDir);
      }
    }
    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = getGetNumCurrentReplicas(this.hdfs_out);

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

    this.metrics = new MetricsWAL();
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
      if (LOG.isTraceEnabled()) LOG.trace("Using getNumCurrentReplicas--HDFS-826");
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
    synchronized (rollWriterLock) {
      // Return if nothing to flush.
      if (!force && this.writer != null && this.numEntries.get() <= 0) {
        return null;
      }
      byte [][] regionsToFlush = null;
      if (closed) {
        LOG.debug("HLog closed. Skipping rolling of writer");
        return null;
      }
      try {
        this.logRollRunning = true;
        if (!closeBarrier.beginOp()) {
          LOG.debug("HLog closing. Skipping rolling of writer");
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
        while (fs.exists(newPath)) {
          this.filenum++;
          newPath = computeFilename();
        }

        // Tell our listeners that a new log is about to be created
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogRoll(oldPath, newPath);
          }
        }
        FSHLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
        // Can we get at the dfsclient outputstream?
        FSDataOutputStream nextHdfsOut = null;
        if (nextWriter instanceof ProtobufLogWriter) {
          nextHdfsOut = ((ProtobufLogWriter)nextWriter).getStream();
        }

        Path oldFile = null;
        int oldNumEntries = 0;
        synchronized (updateLock) {
          // Clean up current writer.
          oldNumEntries = this.numEntries.get();
          oldFile = cleanupCurrentWriter(currentFilenum);
          this.writer = nextWriter;
          this.hdfs_out = nextHdfsOut;
          this.numEntries.set(0);
          if (oldFile != null) {
            this.hlogSequenceNums.put(oldFile, this.latestSequenceNums);
            this.latestSequenceNums = new HashMap<byte[], Long>();
          }
        }
        if (oldFile == null) LOG.info("New WAL " + FSUtils.getPath(newPath));
        else {
          LOG.info("Rolled WAL " + FSUtils.getPath(oldFile) + " with entries=" + oldNumEntries +
          ", filesize=" + StringUtils.humanReadableInt(this.fs.getFileStatus(oldFile).getLen()) +
          "; new WAL " + FSUtils.getPath(newPath));
        }

        // Tell our listeners that a new log was created
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogRoll(oldPath, newPath);
          }
        }

        // Can we delete any of the old log files?
        if (getNumLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } finally {
        this.logRollRunning = false;
        closeBarrier.endOp();
      }
      return regionsToFlush;
    }
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
    return HLogFactory.createWALWriter(fs, path, conf);
  }

  /**
   * Archive old logs that could be archived: a log is eligible for archiving if all its WALEdits
   * are already flushed by the corresponding regions.
   * <p>
   * For each log file, it compares its region to sequenceId map
   * (@link {@link FSHLog#latestSequenceNums} with corresponding region entries in
   * {@link FSHLog#oldestFlushingSeqNums} and {@link FSHLog#oldestUnflushedSeqNums}.
   * If all the regions in the map are flushed past of their value, then the wal is eligible for
   * archiving.
   * @throws IOException
   */
  private void cleanOldLogs() throws IOException {
    Map<byte[], Long> oldestFlushingSeqNumsLocal = null;
    Map<byte[], Long> oldestUnflushedSeqNumsLocal = null;
    List<Path> logsToArchive = new ArrayList<Path>();
    // make a local copy so as to avoid locking when we iterate over these maps.
    synchronized (oldestSeqNumsLock) {
      oldestFlushingSeqNumsLocal = new HashMap<byte[], Long>(this.oldestFlushingSeqNums);
      oldestUnflushedSeqNumsLocal = new HashMap<byte[], Long>(this.oldestUnflushedSeqNums);
    }
    for (Map.Entry<Path, Map<byte[], Long>> e : hlogSequenceNums.entrySet()) {
      // iterate over the log file.
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue();
      // iterate over the map for this log file, and tell whether it should be archive or not.
      if (areAllRegionsFlushed(sequenceNums, oldestFlushingSeqNumsLocal,
        oldestUnflushedSeqNumsLocal)) {
        logsToArchive.add(log);
        LOG.debug("log file is ready for archiving " + log);
      }
    }
    for (Path p : logsToArchive) {
      archiveLogFile(p);
      this.hlogSequenceNums.remove(p);
    }
  }

  /**
   * Takes a region:sequenceId map for a WAL file, and checks whether the file can be archived.
   * It compares the region entries present in the passed sequenceNums map with the local copy of
   * {@link #oldestUnflushedSeqNums} and {@link #oldestFlushingSeqNums}. If, for all regions,
   * the value is lesser than the minimum of values present in the oldestFlushing/UnflushedSeqNums,
   * then the wal file is eligible for archiving.
   * @param sequenceNums for a HLog, at the time when it was rolled.
   * @param oldestFlushingMap
   * @param oldestUnflushedMap
   * @return true if wal is eligible for archiving, false otherwise.
   */
   static boolean areAllRegionsFlushed(Map<byte[], Long> sequenceNums,
      Map<byte[], Long> oldestFlushingMap, Map<byte[], Long> oldestUnflushedMap) {
    for (Map.Entry<byte[], Long> regionSeqIdEntry : sequenceNums.entrySet()) {
      // find region entries in the flushing/unflushed map. If there is no entry, it means
      // a region doesn't have any unflushed entry.
      long oldestFlushing = oldestFlushingMap.containsKey(regionSeqIdEntry.getKey()) ?
          oldestFlushingMap.get(regionSeqIdEntry.getKey()) : Long.MAX_VALUE;
      long oldestUnFlushed = oldestUnflushedMap.containsKey(regionSeqIdEntry.getKey()) ?
          oldestUnflushedMap.get(regionSeqIdEntry.getKey()) : Long.MAX_VALUE;
          // do a minimum to be sure to contain oldest sequence Id
      long minSeqNum = Math.min(oldestFlushing, oldestUnFlushed);
      if (minSeqNum <= regionSeqIdEntry.getValue()) return false;// can't archive
    }
    return true;
  }

  /**
   * Iterates over the given map of regions, and compares their sequence numbers with corresponding
   * entries in {@link #oldestUnflushedSeqNums}. If the sequence number is greater or equal, the
   * region is eligible to flush, otherwise, there is no benefit to flush (from the perspective of
   * passed regionsSequenceNums map), because the region has already flushed the entries present
   * in the WAL file for which this method is called for (typically, the oldest wal file).
   * @param regionsSequenceNums
   * @return regions which should be flushed (whose sequence numbers are larger than their
   * corresponding un-flushed entries.
   */
  private byte[][] findEligibleMemstoresToFlush(Map<byte[], Long> regionsSequenceNums) {
    List<byte[]> regionsToFlush = null;
    // Keeping the old behavior of iterating unflushedSeqNums under oldestSeqNumsLock.
    synchronized (oldestSeqNumsLock) {
      for (Map.Entry<byte[], Long> e : regionsSequenceNums.entrySet()) {
        Long unFlushedVal = this.oldestUnflushedSeqNums.get(e.getKey());
        if (unFlushedVal != null && unFlushedVal <= e.getValue()) {
          if (regionsToFlush == null) regionsToFlush = new ArrayList<byte[]>();
          regionsToFlush.add(e.getKey());
        }
      }
    }
    return regionsToFlush == null ? null : regionsToFlush
        .toArray(new byte[][] { HConstants.EMPTY_BYTE_ARRAY });
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, it checks
   * the first (oldest) WAL file, and returns the regions which should be flushed so that it could
   * be archived.
   * @return regions to flush in order to archive oldest wal file.
   * @throws IOException
   */
  byte[][] findRegionsToForceFlush() throws IOException {
    byte [][] regions = null;
    int logCount = getNumLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, Map<byte[], Long>> firstWALEntry =
        this.hlogSequenceNums.firstEntry();
      regions = findEligibleMemstoresToFlush(firstWALEntry.getValue());
    }
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
    return regions;
  }

  /*
   * Cleans up current writer closing.
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
      }
    }
    return oldFile;
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    if (!FSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
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

/**
 * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}.
 * This helper method returns the creation timestamp from a given log file.
 * It extracts the timestamp assuming the filename is created with the
 * {@link #computeFilename(long filenum)} method.
 * @param fileName
 * @return timestamp, as in the log file name.
 */
  protected long getFileNumFromFileName(Path fileName) {
    if (fileName == null) throw new IllegalArgumentException("file name can't be null");
    // The path should start with dir/<prefix>.
    String prefixPathStr = new Path(dir, prefix + ".").toString();
    if (!fileName.toString().startsWith(prefixPathStr)) {
      throw new IllegalArgumentException("The log file " + fileName + " doesn't belong to" +
      		" this regionserver " + prefixPathStr);
    }
    String chompedPath = fileName.toString().substring(prefixPathStr.length());
    if (forMeta) chompedPath = chompedPath.substring(0, chompedPath.indexOf(META_HLOG_FILE_EXTN));
    return Long.parseLong(chompedPath);
  }

  @Override
  public void closeAndDelete() throws IOException {
    close();
    if (!fs.exists(this.dir)) return;
    FileStatus[] files = fs.listStatus(this.dir);
    if (files != null) {
      for(FileStatus file : files) {

        Path p = getHLogArchivePath(this.oldLogDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(file.getPath(), p);
          }
        }

        if (!FSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(file.getPath(), p);
          }
        }
      }
      LOG.debug("Moved " + files.length + " WAL file(s) to " + FSUtils.getPath(this.oldLogDir));
    }
    if (!fs.delete(dir, true)) {
      LOG.info("Unable to delete " + dir);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    // When optionalFlushInterval is 0, the logSyncer is not started as a Thread.
    if (this.optionalFlushInterval > 0) {
      try {
        logSyncer.close();
        // Make sure we synced everything
        logSyncer.join(this.optionalFlushInterval * 2);
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for syncer thread to die", e);
        Thread.currentThread().interrupt();
      }
    }
    try {
      // Prevent all further flushing and rolling.
      closeBarrier.stopAndDrainOps();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for cache flushes and log rolls", e);
      Thread.currentThread().interrupt();
    }

    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }
    synchronized (updateLock) {
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing WAL writer in " + this.dir.toString());
      }
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
    }
  }

  /**
   * @param now
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tableName
   * @param clusterIds that have consumed the change
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] encodedRegionName, TableName tableName, long seqnum,
      long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    return new HLogKey(encodedRegionName, tableName, seqnum, now, clusterIds, nonceGroup, nonce);
  }

  @Override
  @VisibleForTesting
  public void append(HRegionInfo info, TableName tableName, WALEdit edits,
    final long now, HTableDescriptor htd, AtomicLong sequenceId) throws IOException {
    append(info, tableName, edits, new ArrayList<UUID>(), now, htd, true, true, sequenceId,
        HConstants.NO_NONCE, HConstants.NO_NONCE);
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
   * @param clusterIds that have consumed the change (for replication)
   * @param now
   * @param doSync shall we sync?
   * @param sequenceId of the region.
   * @return txid of this transaction
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private long append(HRegionInfo info, TableName tableName, WALEdit edits, List<UUID> clusterIds,
      final long now, HTableDescriptor htd, boolean doSync, boolean isInMemstore, 
      AtomicLong sequenceId, long nonceGroup, long nonce) throws IOException {
      if (edits.isEmpty()) return this.unflushedEntries.get();
      if (this.closed) {
        throw new IOException("Cannot append; log is closed");
      }
      TraceScope traceScope = Trace.startSpan("FSHlog.append");
      try {
        long txid = 0;
        synchronized (this.updateLock) {
          // get the sequence number from the passed Long. In normal flow, it is coming from the
          // region.
          long seqNum = sequenceId.incrementAndGet();
          // The 'lastSeqWritten' map holds the sequence number of the oldest
          // write for each region (i.e. the first edit added to the particular
          // memstore). . When the cache is flushed, the entry for the
          // region being flushed is removed if the sequence number of the flush
          // is greater than or equal to the value in lastSeqWritten.
          // Use encoded name.  Its shorter, guaranteed unique and a subset of
          // actual  name.
          byte [] encodedRegionName = info.getEncodedNameAsBytes();
          if (isInMemstore) this.oldestUnflushedSeqNums.putIfAbsent(encodedRegionName, seqNum);
          HLogKey logKey = makeKey(
            encodedRegionName, tableName, seqNum, now, clusterIds, nonceGroup, nonce);
          doWrite(info, logKey, edits, htd);
          this.numEntries.incrementAndGet();
          txid = this.unflushedEntries.incrementAndGet();
          if (htd.isDeferredLogFlush()) {
            lastDeferredTxid = txid;
          }
          this.latestSequenceNums.put(encodedRegionName, seqNum);
        }
        // TODO: note that only tests currently call append w/sync.
        //       Therefore, this code here is not actually used by anything.
        // Sync if catalog region, and if not then check if that table supports
        // deferred log flushing
        if (doSync &&
            (info.isMetaRegion() ||
            !htd.isDeferredLogFlush())) {
          // sync txn to file system
          this.sync(txid);
        }
        return txid;
      } finally {
        traceScope.close();
      }
    }

  @Override
  public long appendNoSync(HRegionInfo info, TableName tableName, WALEdit edits,
      List<UUID> clusterIds, final long now, HTableDescriptor htd, AtomicLong sequenceId,
      boolean isInMemstore, long nonceGroup, long nonce) throws IOException {
    return append(info, tableName, edits, clusterIds,
        now, htd, false, isInMemstore, sequenceId, nonceGroup, nonce);
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

    private final AtomicBoolean closeLogSyncer = new AtomicBoolean(false);

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
            Threads.sleep(this.optionalFlushInterval);
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
      // Guaranteed non-null.
      // Note that parallel sync can close tempWriter.
      // The current method of dealing with this is to catch exceptions.
      // See HBASE-4387, HBASE-5623, HBASE-7329.
      tempWriter = this.writer;
    }
    try {
      long doneUpto;
      long now = EnvironmentEdgeManager.currentTimeMillis();
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
          postAppend(pending);
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
            postAppend(pending);
          }
        }
      }
      // another thread might have sync'ed avoid double-sync'ing
      if (txid <= this.syncedTillHere) {
        return;
      }
      try {
        if (tempWriter != null) {
          tempWriter.sync();
          postSync();
        }
      } catch(IOException ex) {
        synchronized (this.updateLock) {
          // HBASE-4387, HBASE-5623, retry with updateLock held
          // TODO: we don't actually need to do it for concurrent close - what is the point
          //       of syncing new unrelated writer? Keep behavior for now.
          tempWriter = this.writer;
          if (tempWriter != null) {
            tempWriter.sync();
            postSync();
          }
        }
      }
      this.syncedTillHere = Math.max(this.syncedTillHere, doneUpto);

      this.metrics.finishSync(EnvironmentEdgeManager.currentTimeMillis() - now);
      // TODO: preserving the old behavior for now, but this check is strange. It's not
      //       protected by any locks here, so for all we know rolling locks might start
      //       as soon as we enter the "if". Is this best-effort optimization check?
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
      LOG.fatal("Could not sync. Requesting roll of hlog", e);
      requestLogRoll();
      throw e;
    }
  }

  @Override
  public void postSync() {}

  @Override
  public void postAppend(List<Entry> entries) {}

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

  @Override
  public void hsync() throws IOException {
    syncer();
  }

  @Override
  public void hflush() throws IOException {
    syncer();
  }

  @Override
  public void sync() throws IOException {
    syncer();
  }

  @Override
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

  // TODO: Remove info.  Unused.
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
      long now = EnvironmentEdgeManager.currentTimeMillis();
      // coprocessor hook:
      if (!coprocessorHost.preWALWrite(info, logKey, logEdit)) {
        if (logEdit.isReplay()) {
          // set replication scope null so that this won't be replicated
          logKey.setScopes(null);
        }
        // write to our buffer for the Hlog file.
        logSyncer.append(new FSHLog.Entry(logKey, logEdit));
      }
      long took = EnvironmentEdgeManager.currentTimeMillis() - now;
      coprocessorHost.postWALWrite(info, logKey, logEdit);
      long len = 0;
      for (KeyValue kv : logEdit.getKeyValues()) {
        len += kv.getLength();
      }
      this.metrics.finishAppend(took, len);
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

  /** @return the number of log files in use */
  int getNumLogFiles() {
    return hlogSequenceNums.size();
  }

  @Override
  public boolean startCacheFlush(final byte[] encodedRegionName) {
    Long oldRegionSeqNum = null;
    if (!closeBarrier.beginOp()) {
      LOG.info("Flush will not be started for " + Bytes.toString(encodedRegionName) +
        " - because the server is closing.");
      return false;
    }
    synchronized (oldestSeqNumsLock) {
      oldRegionSeqNum = this.oldestUnflushedSeqNums.remove(encodedRegionName);
      if (oldRegionSeqNum != null) {
        Long oldValue = this.oldestFlushingSeqNums.put(encodedRegionName, oldRegionSeqNum);
        assert oldValue == null : "Flushing map not cleaned up for "
          + Bytes.toString(encodedRegionName);
      }
    }
    if (oldRegionSeqNum == null) {
      // TODO: if we have no oldRegionSeqNum, and WAL is not disabled, presumably either
      //       the region is already flushing (which would make this call invalid), or there
      //       were no appends after last flush, so why are we starting flush? Maybe we should
      //       assert not null, and switch to "long" everywhere. Less rigorous, but safer,
      //       alternative is telling the caller to stop. For now preserve old logic.
      LOG.warn("Couldn't find oldest seqNum for the region we are about to flush: ["
        + Bytes.toString(encodedRegionName) + "]");
    }
    return true;
  }

  @Override
  public void completeCacheFlush(final byte [] encodedRegionName)
  {
    synchronized (oldestSeqNumsLock) {
      this.oldestFlushingSeqNums.remove(encodedRegionName);
    }
    closeBarrier.endOp();
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    Long currentSeqNum = null, seqNumBeforeFlushStarts = null;
    synchronized (oldestSeqNumsLock) {
      seqNumBeforeFlushStarts = this.oldestFlushingSeqNums.remove(encodedRegionName);
      if (seqNumBeforeFlushStarts != null) {
        currentSeqNum =
          this.oldestUnflushedSeqNums.put(encodedRegionName, seqNumBeforeFlushStarts);
      }
    }
    closeBarrier.endOp();
    if ((currentSeqNum != null)
        && (currentSeqNum.longValue() <= seqNumBeforeFlushStarts.longValue())) {
      String errorStr = "Region " + Bytes.toString(encodedRegionName) +
          "acquired edits out of order current memstore seq=" + currentSeqNum
          + ", previous oldest unflushed id=" + seqNumBeforeFlushStarts;
      LOG.error(errorStr);
      assert false : errorStr;
      Runtime.getRuntime().halt(1);
    }
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
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getRootDir(conf);
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    HLogSplitter.split(baseDir, p, oldLogDir, fs, conf);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** Provide access to currently deferred sequence num for tests */
  boolean hasDeferredEntries() {
    return lastDeferredTxid > syncedTillHere;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    Long result = oldestUnflushedSeqNums.get(encodedRegionName);
    return result == null ? HConstants.NO_SEQNUM : result.longValue();
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
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
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
