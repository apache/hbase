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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.ERROR;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.LOW_REPLICATION;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SLOW_SYNC;
import static org.apache.hadoop.hbase.wal.DefaultWALProvider.WAL_FILE_NAME_DELIMITER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.lmax.disruptor.*;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DrainBarrier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS.
 * Only one WAL is ever being written at a time.  When a WAL hits a configured maximum size,
 * it is rolled.  This is done internal to the implementation.
 *
 * <p>As data is flushed from the MemStore to other on-disk structures (files sorted by
 * key, hfiles), a WAL becomes obsolete. We can let go of all the log edits/entries for a given
 * HRegion-sequence id.  A bunch of work in the below is done keeping account of these region
 * sequence ids -- what is flushed out to hfiles, and what is yet in WAL and in memory only.
 *
 * <p>It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 *
 * <p>To read an WAL, call {@link WALFactory#createReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path)}.
 *
 * <h2>Failure Semantic</h2>
 * If an exception on append or sync, roll the WAL because the current WAL is now a lame duck;
 * any more appends or syncs will fail also with the same original exception. If we have made
 * successful appends to the WAL and we then are unable to sync them, our current semantic is to
 * return error to the client that the appends failed but also to abort the current context,
 * usually the hosting server. We need to replay the WALs. TODO: Change this semantic. A roll of
 * WAL may be sufficient as long as we have flagged client that the append failed. TODO:
 * replication may pick up these last edits though they have been marked as failed append (Need to
 * keep our own file lengths, not rely on HDFS).
 */
@InterfaceAudience.Private
public class FSHLog implements WAL {
  // IMPLEMENTATION NOTES:
  //
  // At the core is a ring buffer.  Our ring buffer is the LMAX Disruptor.  It tries to
  // minimize synchronizations and volatile writes when multiple contending threads as is the case
  // here appending and syncing on a single WAL.  The Disruptor is configured to handle multiple
  // producers but it has one consumer only (the producers in HBase are IPC Handlers calling append
  // and then sync).  The single consumer/writer pulls the appends and syncs off the ring buffer.
  // When a handler calls sync, it is given back a future. The producer 'blocks' on the future so
  // it does not return until the sync completes.  The future is passed over the ring buffer from
  // the producer/handler to the consumer thread where it does its best to batch up the producer
  // syncs so one WAL sync actually spans multiple producer sync invocations.  How well the
  // batching works depends on the write rate; i.e. we tend to batch more in times of
  // high writes/syncs.
  //
  // Calls to append now also wait until the append has been done on the consumer side of the
  // disruptor.  We used to not wait but it makes the implemenation easier to grok if we have
  // the region edit/sequence id after the append returns.
  //
  // TODO: Handlers need to coordinate appending AND syncing.  Can we have the threads contend
  // once only?  Probably hard given syncs take way longer than an append.
  //
  // The consumer threads pass the syncs off to multiple syncing threads in a round robin fashion
  // to ensure we keep up back-to-back FS sync calls (FS sync calls are the long poll writing the
  // WAL).  The consumer thread passes the futures to the sync threads for it to complete
  // the futures when done.
  //
  // The 'sequence' in the below is the sequence of the append/sync on the ringbuffer.  It
  // acts as a sort-of transaction id.  It is always incrementing.
  //
  // The RingBufferEventHandler class hosts the ring buffer consuming code.  The threads that
  // do the actual FS sync are implementations of SyncRunner.  SafePointZigZagLatch is a
  // synchronization class used to halt the consumer at a safe point --  just after all outstanding
  // syncs and appends have completed -- so the log roller can swap the WAL out under it.

  private static final Log LOG = LogFactory.getLog(FSHLog.class);

  static final String SLOW_SYNC_TIME_MS ="hbase.regionserver.wal.slowsync.ms";
  static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms
  static final String ROLL_ON_SYNC_TIME_MS = "hbase.regionserver.wal.roll.on.sync.ms";
  static final int DEFAULT_ROLL_ON_SYNC_TIME_MS = 10000; // in ms
  static final String SLOW_SYNC_ROLL_THRESHOLD = "hbase.regionserver.wal.slowsync.roll.threshold";
  static final int DEFAULT_SLOW_SYNC_ROLL_THRESHOLD = 100; // 100 slow sync warnings
  static final String SLOW_SYNC_ROLL_INTERVAL_MS =
    "hbase.regionserver.wal.slowsync.roll.interval.ms";
  static final int DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS = 60 * 1000; // in ms, 1 minute

  static final String WAL_SYNC_TIMEOUT_MS = "hbase.regionserver.wal.sync.timeout";
  static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  /**
   * The nexus at which all incoming handlers meet.  Does appends and sync with an ordering.
   * Appends and syncs are each put on the ring which means handlers need to
   * smash up against the ring twice (can we make it once only? ... maybe not since time to append
   * is so different from time to sync and sometimes we don't want to sync or we want to async
   * the sync).  The ring is where we make sure of our ordering and it is also where we do
   * batching up of handler sync calls.
   */
  private final Disruptor<RingBufferTruck> disruptor;

  /**
   * An executorservice that runs the disruptor AppendEventHandler append executor.
   */
  private final ExecutorService appendExecutor;

  /**
   * This fellow is run by the above appendExecutor service but it is all about batching up appends
   * and syncs; it may shutdown without cleaning out the last few appends or syncs.  To guard
   * against this, keep a reference to this handler and do explicit close on way out to make sure
   * all flushed out before we exit.
   */
  private final RingBufferEventHandler ringBufferEventHandler;

  /**
   * Map of {@link SyncFuture}s owned by Thread objects. Used so we reuse SyncFutures.
   * Thread local is used so JVM can GC the terminated thread for us. See HBASE-21228
   * <p>
   */
  private final ThreadLocal<SyncFuture> cachedSyncFutures;

  /**
   * The highest known outstanding unsync'd WALEdit sequence number where sequence number is the
   * ring buffer sequence.  Maintained by the ring buffer consumer.
   */
  private volatile long highestUnsyncedSequence = -1;

  /**
   * Updated to the ring buffer sequence of the last successful sync call.  This can be less than
   * {@link #highestUnsyncedSequence} for case where we have an append where a sync has not yet
   * come in for it.  Maintained by the syncing threads.
   */
  private final AtomicLong highestSyncedSequence = new AtomicLong(0);

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  private final Path fullPathLogDir;

  /**
   * dir path where old logs are kept.
   */
  private final Path fullPathArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  private final PathFilter ourFiles;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  private final String logFilePrefix;

  /**
   * Suffix included on generated wal file names
   */
  private final String logFileSuffix;

  /**
   * Prefix used when checking for wal membership.
   */
  private final String prefixPathStr;

  private final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;

  /** Listeners that are called on WAL events. */
  private final List<WALActionsListener> listeners =
    new CopyOnWriteArrayList<WALActionsListener>();

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /**
   * FSDataOutputStream associated with the current SequenceFile.writer
   */
  private FSDataOutputStream hdfs_out;

  // All about log rolling if not enough replicas outstanding.

  // Minimum tolerable replicas, if the actual value is lower than it, rollWriter will be triggered
  private final int minTolerableReplication;

  private final boolean useHsync;

  private final long slowSyncNs, rollOnSyncNs;
  private final int slowSyncRollThreshold;
  private final int slowSyncCheckInterval;
  private final AtomicInteger slowSyncCount = new AtomicInteger();

  private final long walSyncTimeout;

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private final AtomicInteger consecutiveLogRolls = new AtomicInteger(0);

  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  /**
   * Class that does accounting of sequenceids in WAL subsystem. Holds oldest outstanding
   * sequence id as yet not flushed as well as the most recent edit sequence id appended to the
   * WAL. Has facility for answering questions such as "Is it safe to GC a WAL?".
   */
  private SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();

  /**
   * Current log file.
   */
  volatile Writer writer;

  /** The barrier used to ensure that close() waits for all log rolls and flushes to finish. */
  private final DrainBarrier closeBarrier = new DrainBarrier();

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other
   * lock is held. We don't just use synchronized because that results in bogus and tedious
   * findbugs warning when it thinks synchronized controls writer thread safety.  It is held when
   * we are actually rolling the log.  It is checked when we are looking to see if we should roll
   * the log or not.
   */
  private final ReentrantLock rollWriterLock = new ReentrantLock(true);

  private volatile boolean closed = false;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  // The timestamp (in ms) when the log file was created.
  private final AtomicLong filenum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If > than this size, roll the log.
  private final long logrollsize;

  /**
   * The total size of wal
   */
  private AtomicLong totalLogSize = new AtomicLong(0);

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  protected volatile boolean rollRequested;

  // Last time to check low replication on hlog's pipeline
  private volatile long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  // Last time we asked to roll the log due to a slow sync
  private volatile long lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name.
   * Throws an IllegalArgumentException if used to compare paths from different wals.
   */
  final Comparator<Path> LOG_NAME_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      long t1 = getFileNumFromFileName(o1);
      long t2 = getFileNumFromFileName(o2);
      if (t1 == t2) return 0;
      return (t1 > t2) ? 1 : -1;
    }
  };

  /**
   * Map of WAL log file to the latest sequence ids of all regions it has entries of.
   * The map is sorted by the log file creation timestamp (contained in the log file name).
   */
  private NavigableMap<Path, Map<byte[], Long>> byWalRegionSequenceIds =
    new ConcurrentSkipListMap<Path, Map<byte[], Long>>(LOG_NAME_COMPARATOR);

  /**
   * Exception handler to pass the disruptor ringbuffer.  Same as native implementation only it
   * logs using our logger instead of java native logger.
   */
  static class RingBufferExceptionHandler implements ExceptionHandler {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
      LOG.error("Sequence=" + sequence + ", event=" + event, ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived wals
   * @param logDir dir where wals are stored
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir, final Configuration conf)
      throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * WAL object is started up.
   *
   * @param fs filesystem handle
   * @param rootDir path to where logs and oldlogs
   * @param logDir dir where wals are stored
   * @param archiveDir dir where wals are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfWALExists If true IOException will be thrown if files related to this wal
   *        already exist.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "wal" will be used
   * @param suffix will be url encoded. null is treated as empty. non-empty must start with
   *        {@link DefaultWALProvider#WAL_FILE_NAME_DELIMITER}
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf,
      final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix)
      throws IOException {
    this.fs = fs;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathArchiveDir = new Path(rootDir, archiveDir);
    this.conf = conf;

    if (!fs.exists(fullPathLogDir) && !fs.mkdirs(fullPathLogDir)) {
      throw new IOException("Unable to mkdir " + fullPathLogDir);
    }

    if (!fs.exists(this.fullPathArchiveDir)) {
      if (!fs.mkdirs(this.fullPathArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.fullPathArchiveDir);
      }
    }

    // If prefix is null||empty then just name it wal
    this.logFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER +
          "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    String storagePolicy =
        conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
    FSUtils.setStoragePolicy(fs, this.fullPathLogDir, storagePolicy);
    this.logFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.prefixPathStr = new Path(fullPathLogDir,
        logFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (logFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang.StringUtils.isNumeric(
              fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(logFileSuffix)) {
          return false;
        }
        return true;
      }
    };

    if (failIfWALExists) {
      final FileStatus[] walFiles = FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + fullPathLogDir);
      }
    }

    // Register listeners.  TODO: Should this exist anymore?  We have CPs?
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    // Get size to roll log at. Roll at 95% of HDFS block size so we avoid crossing HDFS blocks
    // (it costs a little x'ing bocks)
    final long blocksize = WALUtil.getWALBlockSize(conf, fs, fullPathLogDir, false);
    this.logrollsize =
      (long)(blocksize * conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f));

    float memstoreRatio = conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_KEY,
      conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_OLD_KEY,
        HeapMemorySizeUtil.DEFAULT_MEMSTORE_SIZE));
    boolean maxLogsDefined = conf.get("hbase.regionserver.maxlogs") != null;
    if(maxLogsDefined){
      LOG.warn("'hbase.regionserver.maxlogs' was deprecated.");
    }
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs",
        Math.max(32, calculateMaxLogFiles(memstoreRatio, logrollsize)));
    this.minTolerableReplication = conf.getInt("hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(fs, this.fullPathLogDir));
    this.lowReplicationRollLimit =
      conf.getInt("hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.closeErrorsTolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 0);
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", prefix=" + this.logFilePrefix + ", suffix=" + logFileSuffix + ", logDir=" +
      this.fullPathLogDir + ", archiveDir=" + this.fullPathArchiveDir);

    this.useHsync = conf.getBoolean(HRegion.WAL_HSYNC_CONF_KEY, HRegion.DEFAULT_WAL_HSYNC);

    this.slowSyncNs = TimeUnit.MILLISECONDS.toNanos(conf.getInt(SLOW_SYNC_TIME_MS,
      conf.getInt("hbase.regionserver.hlog.slowsync.ms", DEFAULT_SLOW_SYNC_TIME_MS)));
    this.rollOnSyncNs = TimeUnit.MILLISECONDS.toNanos(conf.getInt(ROLL_ON_SYNC_TIME_MS,
      DEFAULT_ROLL_ON_SYNC_TIME_MS));
    this.slowSyncRollThreshold = conf.getInt(SLOW_SYNC_ROLL_THRESHOLD,
      DEFAULT_SLOW_SYNC_ROLL_THRESHOLD);
    this.slowSyncCheckInterval = conf.getInt(SLOW_SYNC_ROLL_INTERVAL_MS,
      DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS);
    this.walSyncTimeout = conf.getLong(WAL_SYNC_TIMEOUT_MS,
      conf.getLong("hbase.regionserver.hlog.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS));

    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // This is the 'writer' -- a single threaded executor.  This single thread 'consumes' what is
    // put on the ring buffer.
    String hostingThreadName = Thread.currentThread().getName();
    this.appendExecutor = Executors.
      newSingleThreadExecutor(Threads.getNamedThreadFactory(hostingThreadName + ".append"));
    // Preallocate objects to use on the ring buffer.  The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    final int preallocatedEventCount =
      this.conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    // Using BlockingWaitStrategy.  Stuff that is going on here takes so long it makes no sense
    // spinning as other strategies do.
    this.disruptor =
      new Disruptor<RingBufferTruck>(RingBufferTruck.EVENT_FACTORY, preallocatedEventCount,
        this.appendExecutor, ProducerType.MULTI, new BlockingWaitStrategy());
    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    // because SyncFuture.NOT_DONE = 0.
    this.disruptor.getRingBuffer().next();
    this.ringBufferEventHandler =
      new RingBufferEventHandler(conf.getInt("hbase.regionserver.hlog.syncer.count", 5),
        maxHandlersCount);
    this.disruptor.handleExceptionsWith(new RingBufferExceptionHandler());
    this.disruptor.handleEventsWith(new RingBufferEventHandler [] {this.ringBufferEventHandler});
    this.cachedSyncFutures = new ThreadLocal<SyncFuture>() {
      @Override
      protected SyncFuture initialValue() {
        return new SyncFuture();
      }
    };
    // Starting up threads in constructor is a no no; Interface should have an init call.
    this.disruptor.start();
  }

  private int calculateMaxLogFiles(float memstoreSizeRatio, long logRollSize) {
    long max = -1L;
    final MemoryUsage usage = HeapMemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      max = usage.getMax();
    }
    int maxLogs = Math.round(max * memstoreSizeRatio * 2 / logRollSize);
    return maxLogs;
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  protected FileStatus[] getFiles() throws IOException {
    return FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate
   * the default behavior (such as setting the maxRecoveryErrorCount value for example (see
   * {@link TestWALReplay#testReplayEditsWrittenIntoWAL()}). This is done using reflection on the
   * underlying HDFS OutputStream.
   * NOTE: This could be removed once Hadoop1 support is removed.
   * @return null if underlying stream is not ready.
   */
  @VisibleForTesting
  OutputStream getOutputStream() {
    FSDataOutputStream fsdos = this.hdfs_out;
    if (fsdos == null) return null;
    return fsdos.getWrappedStream();
  }

  @Override
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * retrieve the next path to use for writing.
   * Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(System.currentTimeMillis());
    Path newPath = getCurrentFileName();
    while (fs.exists(newPath)) {
      this.filenum.incrementAndGet();
      newPath = getCurrentFileName();
    }
    return newPath;
  }

  Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename  will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * Tell listeners about pre log roll.
   * @throws IOException
   */
  private void tellListenersAboutPreLogRoll(final Path oldPath, final Path newPath)
  throws IOException {
    coprocessorHost.preWALRoll(oldPath, newPath);

    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldPath, newPath);
      }
    }
  }

  /**
   * Tell listeners about post log roll.
   * @throws IOException
   */
  private void tellListenersAboutPostLogRoll(final Path oldPath, final Path newPath)
  throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }

    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  /**
   * Run a sync after opening to set up the pipeline.
   * @param nextWriter
   * @param startTimeNanos
   */
  private void preemptiveSync(final ProtobufLogWriter nextWriter) {
    long startTimeNanos = System.nanoTime();
    try {
      nextWriter.sync(useHsync);
      postSync(System.nanoTime() - startTimeNanos, 0);
    } catch (IOException e) {
      // optimization failed, no need to abort here.
      LOG.warn("pre-sync failed but an optimization so keep going", e);
    }
  }

  @Override
  public byte [][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && (this.writer != null && this.numEntries.get() <= 0)) return null;
      byte [][] regionsToFlush = null;
      if (this.closed) {
        LOG.debug("WAL closed. Skipping rolling of writer");
        return regionsToFlush;
      }
      if (!closeBarrier.beginOp()) {
        LOG.debug("WAL closing. Skipping rolling of writer");
        return regionsToFlush;
      }
      TraceScope scope = Trace.startSpan("FSHLog.rollWriter");
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable so we currently abort.
        Writer nextWriter = this.createWriterInstance(newPath);
        FSDataOutputStream nextHdfsOut = null;
        if (nextWriter instanceof ProtobufLogWriter) {
          nextHdfsOut = ((ProtobufLogWriter)nextWriter).getStream();
          // If a ProtobufLogWriter, go ahead and try and sync to force setup of pipeline.
          // If this fails, we just keep going.... it is an optimization, not the end of the world.
          preemptiveSync((ProtobufLogWriter)nextWriter);
        }
        tellListenersAboutPreLogRoll(oldPath, newPath);
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter, nextHdfsOut);
        tellListenersAboutPostLogRoll(oldPath, newPath);
        // Reset rollRequested status
        rollRequested = false;
        // We got a new writer, so reset the slow sync count
        lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();
        slowSyncCount.set(0);
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } finally {
        closeBarrier.endOp();
        assert scope == NullScope.INSTANCE || !scope.isDetached();
        scope.close();
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   *
   * @return Writer instance
   */
  protected Writer createWriterInstance(final Path path) throws IOException {
    return DefaultWALProvider.createWriter(conf, fs, path, false);
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   * @throws IOException
   */
  private void cleanOldLogs() throws IOException {
    List<Path> logsToArchive = null;
    // For each log file, look at its Map of regions to highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Map.Entry<Path, Map<byte[], Long>> e : this.byWalRegionSequenceIds.entrySet()) {
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue();
      if (this.sequenceIdAccounting.areAllLower(sequenceNums)) {
        if (logsToArchive == null) logsToArchive = new ArrayList<Path>();
        logsToArchive.add(log);
        if (LOG.isTraceEnabled()) LOG.trace("WAL file ready for archiving " + log);
      }
    }
    if (logsToArchive != null) {
      for (Path p : logsToArchive) {
        this.totalLogSize.addAndGet(-this.fs.getFileStatus(p).getLen());
        archiveLogFile(p);
        this.byWalRegionSequenceIds.remove(p);
      }
    }
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, check the first
   * (oldest) WAL file, and returns those regions which should be flushed so that it can
   * be archived.
   * @return regions (encodedRegionNames) to flush in order to archive oldest WAL file.
   * @throws IOException
   */
  byte[][] findRegionsToForceFlush() throws IOException {
    byte [][] regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, Map<byte[], Long>> firstWALEntry =
        this.byWalRegionSequenceIds.firstEntry();
      regions = this.sequenceIdAccounting.findLower(firstWALEntry.getValue());
    }
    if (regions != null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(Bytes.toStringBinary(regions[i]));
      }
      LOG.info("Too many WALs; count=" + logCount + ", max=" + this.maxLogs +
        "; forcing flush of " + regions.length + " regions(s): " + sb.toString());
    }
    return regions;
  }

  /**
   * Used to manufacture race condition reliably. For testing only.
   * @see #beforeWaitOnSafePoint()
   */
  @VisibleForTesting
  protected void afterCreatingZigZagLatch() {}

  /**
   * @see #afterCreatingZigZagLatch()
   */
  @VisibleForTesting
  protected void beforeWaitOnSafePoint() {};

  /**
   * Cleans up current writer closing it and then puts in place the passed in
   * <code>nextWriter</code>.
   *
   * In the case of creating a new WAL, oldPath will be null.
   *
   * In the case of rolling over from one file to the next, none of the params will be null.
   *
   * In the case of closing out this FSHLog with no further use newPath, nextWriter, and
   * nextHdfsOut will be null.
   *
   * @param oldPath may be null
   * @param newPath may be null
   * @param nextWriter may be null
   * @param nextHdfsOut may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  Path replaceWriter(final Path oldPath, final Path newPath, Writer nextWriter,
      final FSDataOutputStream nextHdfsOut)
  throws IOException {
    // Ask the ring buffer writer to pause at a safe point.  Once we do this, the writer
    // thread will eventually pause. An error hereafter needs to release the writer thread
    // regardless -- hence the finally block below.  Note, this method is called from the FSHLog
    // constructor BEFORE the ring buffer is set running so it is null on first time through
    // here; allow for that.
    SyncFuture syncFuture = null;
    SafePointZigZagLatch zigzagLatch = null;
    long sequence = -1L;
    if (this.ringBufferEventHandler != null) {
      // Get sequence first to avoid dead lock when ring buffer is full
      // Considering below sequence
      // 1. replaceWriter is called and zigzagLatch is initialized
      // 2. ringBufferEventHandler#onEvent is called and arrives at #attainSafePoint(long) then wait
      // on safePointReleasedLatch
      // 3. Since ring buffer is full, if we get sequence when publish sync, the replaceWriter
      // thread will wait for the ring buffer to be consumed, but the only consumer is waiting
      // replaceWriter thread to release safePointReleasedLatch, which causes a deadlock
      sequence = getSequenceOnRingBuffer();
      zigzagLatch = this.ringBufferEventHandler.attainSafePoint();
    }
    afterCreatingZigZagLatch();
    TraceScope scope = Trace.startSpan("FSHFile.replaceWriter");
    try {
      // Wait on the safe point to be achieved.  Send in a sync in case nothing has hit the
      // ring buffer between the above notification of writer that we want it to go to
      // 'safe point' and then here where we are waiting on it to attain safe point.  Use
      // 'sendSync' instead of 'sync' because we do not want this thread to block waiting on it
      // to come back.  Cleanup this syncFuture down below after we are ready to run again.
      try {
        if (zigzagLatch != null) {
          // use assert to make sure no change breaks the logic that
          // sequence and zigzagLatch will be set together
          assert sequence > 0L : "Failed to get sequence from ring buffer";
          Trace.addTimelineAnnotation("awaiting safepoint");
          syncFuture = zigzagLatch.waitSafePoint(publishSyncOnRingBuffer(sequence));
        }
      } catch (FailedSyncBeforeLogCloseException e) {
        // If unflushed/unsynced entries on close, it is reason to abort.
        if (isUnflushedEntries()) throw e;
        LOG.warn("Failed sync-before-close but no outstanding appends; closing WAL: " +
          e.getMessage());
      }

      // It is at the safe point.  Swap out writer from under the blocked writer thread.
      // TODO: This is close is inline with critical section.  Should happen in background?
      try {
        if (this.writer != null) {
          Trace.addTimelineAnnotation("closing writer");
          this.writer.close();
          Trace.addTimelineAnnotation("writer closed");
        }
        this.closeErrorCount.set(0);
      } catch (IOException ioe) {
        int errors = closeErrorCount.incrementAndGet();
        if (!isUnflushedEntries() && (errors <= this.closeErrorsTolerated)) {
          LOG.warn("Riding over failed WAL close of " + oldPath + ", cause=\"" +
            ioe.getMessage() + "\", errors=" + errors +
            "; THIS FILE WAS NOT CLOSED BUT ALL EDITS SYNCED SO SHOULD BE OK");
        } else {
          throw ioe;
        }
      }
      this.writer = nextWriter;
      this.hdfs_out = nextHdfsOut;
      int oldNumEntries = this.numEntries.get();
      this.numEntries.set(0);
      final String newPathString = (null == newPath ? null : FSUtils.getPath(newPath));
      if (oldPath != null) {
        this.byWalRegionSequenceIds.put(oldPath, this.sequenceIdAccounting.resetHighest());
        long oldFileLen = this.fs.getFileStatus(oldPath).getLen();
        this.totalLogSize.addAndGet(oldFileLen);
        LOG.info("Rolled WAL " + FSUtils.getPath(oldPath) + " with entries=" + oldNumEntries +
          ", filesize=" + StringUtils.byteDesc(oldFileLen) + "; new WAL " +
          newPathString);
      } else {
        LOG.info("New WAL " + newPathString);
      }
    } catch (InterruptedException ie) {
      // Perpetuate the interrupt
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      long count = getUnflushedEntriesCount();
      LOG.error("Failed close of WAL writer " + oldPath + ", unflushedEntries=" + count, e);
      throw new FailedLogCloseException(oldPath + ", unflushedEntries=" + count, e);
    } finally {
      try {
        // Let the writer thread go regardless, whether error or not.
        if (zigzagLatch != null) {
          zigzagLatch.releaseSafePoint();
          // syncFuture will be null if we failed our wait on safe point above. Otherwise, if
          // latch was obtained successfully, the sync we threw in either trigger the latch or it
          // got stamped with an exception because the WAL was damaged and we could not sync. Now
          // the write pipeline has been opened up again by releasing the safe point, process the
          // syncFuture we got above. This is probably a noop but it may be stale exception from
          // when old WAL was in place. Catch it if so.
          if (syncFuture != null) {
            try {
              blockOnSync(syncFuture);
            } catch (IOException ioe) {
              if (LOG.isTraceEnabled()) LOG.trace("Stale sync exception", ioe);
            }
          }
        }
      } finally {
        scope.close();
      }
    }
    return newPath;
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedSequence.get();
    return highestSynced > this.highestUnsyncedSequence?
      0: this.highestUnsyncedSequence - highestSynced;
  }

  boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.fullPathArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
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
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = logFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + logFileSuffix;
    return new Path(fullPathLogDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current WAL file-number
   * @return Path
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  /**
   * @return current file number (timestamp)
   */
  public long getFilenum() {
    return filenum.get();
  }
  
  @Override
  public String toString() {
    return "FSHLog " + logFilePrefix + ":" + logFileSuffix + "(num " + filenum + ")";
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
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException("The log file " + fileName +
          " doesn't belong to this WAL. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
        (fileNameString.length() - logFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.fullPathArchiveDir, file.getPath());
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
      LOG.debug("Moved " + files.length + " WAL file(s) to " +
        FSUtils.getPath(this.fullPathArchiveDir));
    }
    LOG.info("Closed WAL: " + toString());
  }

  @Override
  public void shutdown() throws IOException {
    if (shutdown.compareAndSet(false, true)) {
      try {
        // Prevent all further flushing and rolling.
        closeBarrier.stopAndDrainOps();
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for cache flushes and log rolls", e);
        Thread.currentThread().interrupt();
      }

      // Shutdown the disruptor.  Will stop after all entries have been processed.  Make sure we
      // have stopped incoming appends before calling this else it will not shutdown.  We are
      // conservative below waiting a long time and if not elapsed, then halting.
      if (this.disruptor != null) {
        long timeoutms = conf.getLong("hbase.wal.disruptor.shutdown.timeout.ms", 60000);
        try {
          this.disruptor.shutdown(timeoutms, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOG.warn("Timed out bringing down disruptor after " + timeoutms + "ms; forcing halt " +
            "(It is a problem if this is NOT an ABORT! -- DATALOSS!!!!)");
          this.disruptor.halt();
          this.disruptor.shutdown();
        }
      }
      // With disruptor down, this is safe to let go.
      if (this.appendExecutor !=  null) this.appendExecutor.shutdown();

      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.logCloseRequested();
        }
      }
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing WAL writer in " + FSUtils.getPath(fullPathLogDir));
      }
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
      justification="Will never be null")
  @Override
  public long append(final HTableDescriptor htd, final HRegionInfo hri, final WALKey key,
      final WALEdit edits, final boolean inMemstore) throws IOException {
    if (this.closed) throw new IOException("Cannot append; log is closed");
    // Make a trace scope for the append.  It is closed on other side of the ring buffer by the
    // single consuming thread.  Don't have to worry about it.
    TraceScope scope = Trace.startSpan("FSHLog.append");
    final MutableLong txidHolder = new MutableLong();
    final RingBuffer<RingBufferTruck> ringBuffer = disruptor.getRingBuffer();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(new Runnable() {
      @Override public void run() {
        txidHolder.setValue(ringBuffer.next());
      }
    });
    long txid = txidHolder.longValue();
    try {
      FSWALEntry entry = new FSWALEntry(txid, key, edits, htd, hri, inMemstore);
      entry.stampRegionSequenceId(we);
      ringBuffer.get(txid).loadPayload(entry, scope.detach());
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  /**
   * Thread to runs the hdfs sync call. This call takes a while to complete.  This is the longest
   * pole adding edits to the WAL and this must complete to be sure all edits persisted.  We run
   * multiple threads sync'ng rather than one that just syncs in series so we have better
   * latencies; otherwise, an edit that arrived just after a sync started, might have to wait
   * almost the length of two sync invocations before it is marked done.
   * <p>When the sync completes, it marks all the passed in futures done.  On the other end of the
   * sync future is a blocked thread, usually a regionserver Handler.  There may be more than one
   * future passed in the case where a few threads arrive at about the same time and all invoke
   * 'sync'.  In this case we'll batch up the invocations and run one filesystem sync only for a
   * batch of Handler sync invocations.  Do not confuse these Handler SyncFutures with the futures
   * an ExecutorService returns when you call submit. We have no use for these in this model. These
   * SyncFutures are 'artificial', something to hold the Handler until the filesystem sync
   * completes.
   */
  private class SyncRunner extends HasThread {
    private volatile long sequence;
    // Keep around last exception thrown. Clear on successful sync.
    private final BlockingQueue<SyncFuture> syncFutures;
    private volatile SyncFuture takeSyncFuture = null;

    /**
     * UPDATE!
     * @param syncs the batch of calls to sync that arrived as this thread was starting; when done,
     * we will put the result of the actual hdfs sync call as the result.
     * @param sequence The sequence number on the ring buffer when this thread was set running.
     * If this actual writer sync completes then all appends up this point have been
     * flushed/synced/pushed to datanodes.  If we fail, then the passed in <code>syncs</code>
     * futures will return the exception to their clients; some of the edits may have made it out
     * to data nodes but we will report all that were part of this session as failed.
     */
    SyncRunner(final String name, final int maxHandlersCount) {
      super(name);
      // LinkedBlockingQueue because of
      // http://www.javacodegeeks.com/2010/09/java-best-practices-queue-battle-and.html
      // Could use other blockingqueues here or concurrent queues.
      //
      // We could let the capacity be 'open' but bound it so we get alerted in pathological case
      // where we cannot sync and we have a bunch of threads all backed up waiting on their syncs
      // to come in.  LinkedBlockingQueue actually shrinks when you remove elements so Q should
      // stay neat and tidy in usual case.  Let the max size be three times the maximum handlers.
      // The passed in maxHandlerCount is the user-level handlers which is what we put up most of
      // but HBase has other handlers running too -- opening region handlers which want to write
      // the meta table when succesful (i.e. sync), closing handlers -- etc.  These are usually
      // much fewer in number than the user-space handlers so Q-size should be user handlers plus
      // some space for these other handlers.  Lets multiply by 3 for good-measure.
      this.syncFutures = new LinkedBlockingQueue<SyncFuture>(maxHandlersCount * 3);
    }

    void offer(final long sequence, final SyncFuture [] syncFutures, final int syncFutureCount) {
      // Set sequence first because the add to the queue will wake the thread if sleeping.
      this.sequence = sequence;
      for (int i = 0; i < syncFutureCount; ++i) {
        this.syncFutures.add(syncFutures[i]);
      }
    }

    /**
     * Release the passed <code>syncFuture</code>
     * @param syncFuture
     * @param currentSequence
     * @param t
     * @return Returns 1.
     */
    private int releaseSyncFuture(final SyncFuture syncFuture, final long currentSequence,
        final Throwable t) {
      if (!syncFuture.done(currentSequence, t)) throw new IllegalStateException();
      // This function releases one sync future only.
      return 1;
    }

    /**
     * Release all SyncFutures whose sequence is <= <code>currentSequence</code>.
     * @param currentSequence
     * @param t May be non-null if we are processing SyncFutures because an exception was thrown.
     * @return Count of SyncFutures we let go.
     */
    private int releaseSyncFutures(final long currentSequence, final Throwable t) {
      int syncCount = 0;
      for (SyncFuture syncFuture; (syncFuture = this.syncFutures.peek()) != null;) {
        if (syncFuture.getRingBufferSequence() > currentSequence) break;
        releaseSyncFuture(syncFuture, currentSequence, t);
        if (!this.syncFutures.remove(syncFuture)) {
          throw new IllegalStateException(syncFuture.toString());
        }
        syncCount++;
      }
      return syncCount;
    }

    /**
     * @param sequence The sequence we ran the filesystem sync against.
     * @return Current highest synced sequence.
     */
    private long updateHighestSyncedSequence(long sequence) {
      long currentHighestSyncedSequence;
      // Set the highestSyncedSequence IFF our current sequence id is the 'highest'.
      do {
        currentHighestSyncedSequence = highestSyncedSequence.get();
        if (currentHighestSyncedSequence >= sequence) {
          // Set the sync number to current highwater mark; might be able to let go more
          // queued sync futures
          sequence = currentHighestSyncedSequence;
          break;
        }
      } while (!highestSyncedSequence.compareAndSet(currentHighestSyncedSequence, sequence));
      return sequence;
    }

    boolean areSyncFuturesReleased() {
      // check whether there is no sync futures offered, and no in-flight sync futures that is being
      // processed.
      return syncFutures.size() <= 0
          && takeSyncFuture == null;
    }

    @Override
    public void run() {
      long currentSequence;
      while (!isInterrupted()) {
        int syncCount = 0;

        try {
          while (true) {
            takeSyncFuture = null;
            // We have to process what we 'take' from the queue
            takeSyncFuture = this.syncFutures.take();
            currentSequence = this.sequence;
            long syncFutureSequence = takeSyncFuture.getRingBufferSequence();
            if (syncFutureSequence > currentSequence) {
              throw new IllegalStateException("currentSequence=" + currentSequence +
                ", syncFutureSequence=" + syncFutureSequence);
            }
            // See if we can process any syncfutures BEFORE we go sync.
            long currentHighestSyncedSequence = highestSyncedSequence.get();
            if (currentSequence < currentHighestSyncedSequence) {
              syncCount += releaseSyncFuture(takeSyncFuture, currentHighestSyncedSequence, null);
              // Done with the 'take'.  Go around again and do a new 'take'.
              continue;
            }
            break;
          }
          // I got something.  Lets run.  Save off current sequence number in case it changes
          // while we run.
          TraceScope scope = Trace.continueSpan(takeSyncFuture.getSpan());
          long start = System.nanoTime();
          Throwable lastException = null;
          try {
            Trace.addTimelineAnnotation("syncing writer");
            writer.sync(takeSyncFuture.isForceSync());
            Trace.addTimelineAnnotation("writer synced");
            currentSequence = updateHighestSyncedSequence(currentSequence);
          } catch (IOException e) {
            LOG.error("Error syncing, request close of WAL", e);
            lastException = e;
          } catch (Exception e) {
            LOG.warn("UNEXPECTED", e);
            lastException = e;
          } finally {
            // reattach the span to the future before releasing.
            takeSyncFuture.setSpan(scope.detach());
            // First release what we 'took' from the queue.
            syncCount += releaseSyncFuture(takeSyncFuture, currentSequence, lastException);
            // Can we release other syncs?
            syncCount += releaseSyncFutures(currentSequence, lastException);
            if (lastException != null) {
              requestLogRoll();
            } else {
              checkLogRoll();
            }
          }
          postSync(System.nanoTime() - start, syncCount);
        } catch (InterruptedException e) {
          // Presume legit interrupt.
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          LOG.warn("UNEXPECTED, continuing", t);
        }
      }
    }
  }

  /**
   * Schedule a log roll if needed.
   */
  public void checkLogRoll() {
    // If we have already requested a roll, do nothing
    if (isLogRollRequested()) {
      return;
    }
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      if (checkLowReplication()) {
        LOG.warn("Requesting log roll because of low replication, current pipeline: " +
            Arrays.toString(getPipeLine()));
        requestLogRoll(LOW_REPLICATION);
      } else if (writer != null && writer.getLength() > logrollsize) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Requesting log roll because of file size threshold; length=" +
            writer.getLength() + ", logrollsize=" + logrollsize);
        }
        requestLogRoll(SIZE);
      } else if (checkSlowSync()) {
        // We log this already in checkSlowSync
        requestLogRoll(SLOW_SYNC);
      }
    } catch (IOException e) {
      LOG.warn("Writer.getLength() failed; continuing", e);
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * @return true if number of replicas for the WAL is lower than threshold
   */
  private boolean checkLowReplication() {
    boolean logRollNeeded = false;
    this.lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0 && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of WAL. current pipeline: "
                + Arrays.toString(getPipeLine()));
            logRollNeeded = true;
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
            return logRollNeeded;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("DFSOutputStream.getNumCurrentReplicas failed because of " + e +
        ", continuing...");
    }
    return logRollNeeded;
  }

  /**
   * @return true if we exceeded the slow sync roll threshold over the last check
   *              interval
   */
  private boolean checkSlowSync() {
    boolean result = false;
    long now = EnvironmentEdgeManager.currentTime();
    long elapsedTime = now - lastTimeCheckSlowSync;
    if (elapsedTime >= slowSyncCheckInterval) {
      if (slowSyncCount.get() >= slowSyncRollThreshold) {
        if (elapsedTime >= (2 * slowSyncCheckInterval)) {
          // If two or more slowSyncCheckInterval have elapsed this is a corner case
          // where a train of slow syncs almost triggered us but then there was a long
          // interval from then until the one more that pushed us over. If so, we
          // should do nothing and let the count reset.
          if (LOG.isDebugEnabled()) {
            LOG.debug("checkSlowSync triggered but we decided to ignore it; " +
              "count=" + slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold +
              ", elapsedTime=" + elapsedTime +  " ms, slowSyncCheckInterval=" +
              slowSyncCheckInterval + " ms");
          }
          // Fall through to count reset below
        } else {
          LOG.warn("Requesting log roll because we exceeded slow sync threshold; count=" +
            slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold +
            ", current pipeline: " + Arrays.toString(getPipeLine()));
          result = true;
        }
      }
      lastTimeCheckSlowSync = now;
      slowSyncCount.set(0);
    }
    return result;
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence) {
    return publishSyncOnRingBuffer(sequence, null, false);
  }

  private long getSequenceOnRingBuffer() {
    return this.disruptor.getRingBuffer().next();
  }

  private SyncFuture publishSyncOnRingBuffer(Span span, boolean forceSync) {
    long sequence = this.disruptor.getRingBuffer().next();
    return publishSyncOnRingBuffer(sequence, span, forceSync);
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence, Span span, boolean forceSync) {
    SyncFuture syncFuture = getSyncFuture(sequence, span).setForceSync(forceSync);
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      truck.loadPayload(syncFuture);
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return syncFuture;
  }

  // Sync all known transactions
  private Span publishSyncThenBlockOnCompletion(Span span, boolean forceSync) throws IOException {
    return blockOnSync(publishSyncOnRingBuffer(span, forceSync));
  }

  private Span blockOnSync(final SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      syncFuture.get(walSyncTimeout);
      return syncFuture.getSpan();
    } catch (TimeoutIOException tioe) {
      // SyncFuture reuse by thread, if TimeoutIOException happens, ringbuffer
      // still refer to it, so if this thread use it next time may get a wrong
      // result.
      this.cachedSyncFutures.remove();
      throw tioe;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private SyncFuture getSyncFuture(final long sequence, Span span) {
    return cachedSyncFutures.get().reset(sequence);
  }

  private void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg =
          new StringBuilder().append("Slow sync cost: ")
              .append(TimeUnit.NANOSECONDS.toMillis(timeInNanos))
              .append(" ms, current pipeline: ")
              .append(Arrays.toString(getPipeLine())).toString();
      Trace.addTimelineAnnotation(msg);
      LOG.info(msg);
      // A single sync took too long.
      // Elsewhere in checkSlowSync, called from checkLogRoll, we will look at cumulative
      // effects. Here we have a single data point that indicates we should take immediate
      // action, so do so.
      if (timeInNanos > this.rollOnSyncNs) {
        LOG.warn("Requesting log roll because we exceeded slow sync threshold; time=" +
          TimeUnit.NANOSECONDS.toMillis(timeInNanos) + " ms, threshold=" +
          TimeUnit.NANOSECONDS.toMillis(rollOnSyncNs) + " ms, current pipeline: " +
          Arrays.toString(getPipeLine()));
        requestLogRoll(SLOW_SYNC);
      }
      slowSyncCount.incrementAndGet(); // it's fine to unconditionally increment this
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  private long postAppend(final Entry e, final long elapsedTime) throws IOException {
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += CellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }


  /**
   * This method gets the datanode replication count for the current WAL.
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
  @VisibleForTesting
  int getLogReplication() {
    try {
      //in standalone mode, it will return 0
      if (this.hdfs_out instanceof HdfsDataOutputStream) {
        return ((HdfsDataOutputStream) this.hdfs_out).getCurrentBlockReplication();
      }
    } catch (IOException e) {
      LOG.info("", e);
    }
    return 0;
  }

  @Override
  public void sync() throws IOException {
    sync(useHsync);
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach(), forceSync));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    sync(txid, useHsync);
  }

  @Override
  public void sync(long txid, boolean forceSync) throws IOException {
    if (this.highestSyncedSequence.get() >= txid) {
      // Already sync'd.
      return;
    }
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach(), forceSync));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  protected boolean isLogRollRequested() {
    return rollRequested;
  }

  // public only until class moves to o.a.h.h.wal
  public void requestLogRoll() {
    requestLogRoll(ERROR);
  }

  private void requestLogRoll(final WALActionsListener.RollRequestReason reason) {
    // If we have already requested a roll, don't do it again
    if (rollRequested) {
      return;
    }
    if (!this.listeners.isEmpty()) {
      rollRequested = true; // No point to assert this unless there is a registered listener
      for (WALActionsListener i: this.listeners) {
        i.logRollRequested(reason);
      }
    }
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of rolled log files */
  public int getNumRolledLogFiles() {
    return byWalRegionSequenceIds.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  @Override
  public Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> families) {
    if (!closeBarrier.beginOp()) {
      LOG.info("Flush not started for " + Bytes.toString(encodedRegionName) + "; server closing.");
      return null;
    }
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public void completeCacheFlush(final byte [] encodedRegionName) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName);
    closeBarrier.endOp();
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
    closeBarrier.endOp();
  }

  @VisibleForTesting
  boolean isLowReplicationRollEnabled() {
      return lowReplicationRollEnabled;
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + (3 * Bytes.SIZEOF_INT) + (4 * Bytes.SIZEOF_LONG));

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = FSUtils.getWALFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getWALRootDir(conf);
    final Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }


  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    // This method is used by tests and for figuring if we should flush or not because our
    // sequenceids are too old. It is also used reporting the master our oldest sequenceid for use
    // figuring what edits can be skipped during log recovery. getEarliestMemStoreSequenceId
    // from this.sequenceIdAccounting is looking first in flushingOldestStoreSequenceIds, the
    // currently flushing sequence ids, and if anything found there, it is returning these. This is
    // the right thing to do for the reporting oldest sequenceids to master; we won't skip edits if
    // we crash during the flush. For figuring what to flush, we might get requeued if our sequence
    // id is old even though we are currently flushing. This may mean we do too much flushing.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }

  /**
   * This class is used coordinating two threads holding one thread at a
   * 'safe point' while the orchestrating thread does some work that requires the first thread
   * paused: e.g. holding the WAL writer while its WAL is swapped out from under it by another
   * thread.
   *
   * <p>Thread A signals Thread B to hold when it gets to a 'safe point'.  Thread A wait until
   * Thread B gets there. When the 'safe point' has been attained, Thread B signals Thread A.
   * Thread B then holds at the 'safe point'.  Thread A on notification that Thread B is paused,
   * goes ahead and does the work it needs to do while Thread B is holding.  When Thread A is done,
   * it flags B and then Thread A and Thread B continue along on their merry way.  Pause and
   * signalling 'zigzags' between the two participating threads.  We use two latches -- one the
   * inverse of the other -- pausing and signaling when states are achieved.
   *
   * <p>To start up the drama, Thread A creates an instance of this class each time it would do
   * this zigzag dance and passes it to Thread B (these classes use Latches so it is one shot
   * only). Thread B notices the new instance (via reading a volatile reference or how ever) and it
   * starts to work toward the 'safe point'.  Thread A calls {@link #waitSafePoint()} when it
   * cannot proceed until the Thread B 'safe point' is attained. Thread A will be held inside in
   * {@link #waitSafePoint()} until Thread B reaches the 'safe point'.  Once there, Thread B
   * frees Thread A by calling {@link #safePointAttained()}.  Thread A now knows Thread B
   * is at the 'safe point' and that it is holding there (When Thread B calls
   * {@link #safePointAttained()} it blocks here until Thread A calls {@link #releaseSafePoint()}).
   * Thread A proceeds to do what it needs to do while Thread B is paused.  When finished,
   * it lets Thread B lose by calling {@link #releaseSafePoint()} and away go both Threads again.
   */
  static class SafePointZigZagLatch {
    /**
     * Count down this latch when safe point attained.
     */
    private volatile CountDownLatch safePointAttainedLatch = new CountDownLatch(1);
    /**
     * Latch to wait on.  Will be released when we can proceed.
     */
    private volatile CountDownLatch safePointReleasedLatch = new CountDownLatch(1);

    private void checkIfSyncFailed(SyncFuture syncFuture) throws FailedSyncBeforeLogCloseException {
      if (syncFuture.isThrowable()) {
        throw new FailedSyncBeforeLogCloseException(syncFuture.getThrowable());
      }
    }

    /**
     * For Thread A to call when it is ready to wait on the 'safe point' to be attained.
     * Thread A will be held in here until Thread B calls {@link #safePointAttained()}
     * @param syncFuture We need this as barometer on outstanding syncs.  If it comes home with
     * an exception, then something is up w/ our syncing.
     * @throws InterruptedException
     * @throws ExecutionException
     * @return The passed <code>syncFuture</code>
     * @throws FailedSyncBeforeLogCloseException
     */
    SyncFuture waitSafePoint(SyncFuture syncFuture) throws InterruptedException,
        FailedSyncBeforeLogCloseException {
      while (!this.safePointAttainedLatch.await(1, TimeUnit.MILLISECONDS)) {
        checkIfSyncFailed(syncFuture);
      }
      checkIfSyncFailed(syncFuture);
      return syncFuture;
    }

    /**
     * Called by Thread B when it attains the 'safe point'.  In this method, Thread B signals
     * Thread A it can proceed. Thread B will be held in here until {@link #releaseSafePoint()}
     * is called by Thread A.
     * @throws InterruptedException
     */
    void safePointAttained() throws InterruptedException {
      this.safePointAttainedLatch.countDown();
      this.safePointReleasedLatch.await();
    }

    /**
     * Called by Thread A when it is done with the work it needs to do while Thread B is
     * halted.  This will release the Thread B held in a call to {@link #safePointAttained()}
     */
    void releaseSafePoint() {
      this.safePointReleasedLatch.countDown();
    }

    /**
     * @return True is this is a 'cocked', fresh instance, and not one that has already fired.
     */
    boolean isCocked() {
      return this.safePointAttainedLatch.getCount() > 0 &&
        this.safePointReleasedLatch.getCount() > 0;
    }
  }

  /**
   * Handler that is run by the disruptor ringbuffer consumer. Consumer is a SINGLE
   * 'writer/appender' thread.  Appends edits and starts up sync runs.  Tries its best to batch up
   * syncs.  There is no discernible benefit batching appends so we just append as they come in
   * because it simplifies the below implementation.  See metrics for batching effectiveness
   * (In measurement, at 100 concurrent handlers writing 1k, we are batching > 10 appends and 10
   * handler sync invocations for every actual dfsclient sync call; at 10 concurrent handlers,
   * YMMV).
   * <p>Herein, we have an array into which we store the sync futures as they come in.  When we
   * have a 'batch', we'll then pass what we have collected to a SyncRunner thread to do the
   * filesystem sync.  When it completes, it will then call
   * {@link SyncFuture#done(long, Throwable)} on each of SyncFutures in the batch to release
   * blocked Handler threads.
   * <p>I've tried various effects to try and make latencies low while keeping throughput high.
   * I've tried keeping a single Queue of SyncFutures in this class appending to its tail as the
   * syncs coming and having sync runner threads poll off the head to 'finish' completed
   * SyncFutures.  I've tried linkedlist, and various from concurrent utils whether
   * LinkedBlockingQueue or ArrayBlockingQueue, etc.  The more points of synchronization, the
   * more 'work' (according to 'perf stats') that has to be done; small increases in stall
   * percentages seem to have a big impact on throughput/latencies.  The below model where we have
   * an array into which we stash the syncs and then hand them off to the sync thread seemed like
   * a decent compromise.  See HBASE-8755 for more detail.
   */
  class RingBufferEventHandler implements EventHandler<RingBufferTruck>, LifecycleAware {
    private final SyncRunner [] syncRunners;
    private final SyncFuture [] syncFutures;
    // Had 'interesting' issues when this was non-volatile.  On occasion, we'd not pass all
    // syncFutures to the next sync'ing thread.
    private volatile int syncFuturesCount = 0;
    private volatile SafePointZigZagLatch zigzagLatch;
    /**
     * Set if we get an exception appending or syncing so that all subsequence appends and syncs
     * on this WAL fail until WAL is replaced.
     */
    private Exception exception = null;
    /**
     * Object to block on while waiting on safe point.
     */
    private final Object safePointWaiter = new Object();
    private volatile boolean shutdown = false;

    /**
     * Which syncrunner to use next.
     */
    private int syncRunnerIndex;

    RingBufferEventHandler(final int syncRunnerCount, final int maxHandlersCount) {
      this.syncFutures = new SyncFuture[maxHandlersCount];
      this.syncRunners = new SyncRunner[syncRunnerCount];
      for (int i = 0; i < syncRunnerCount; i++) {
        this.syncRunners[i] = new SyncRunner("sync." + i, maxHandlersCount);
      }
    }

    private void cleanupOutstandingSyncsOnException(final long sequence, final Exception e) {
      // There could be handler-count syncFutures outstanding.
      for (int i = 0; i < this.syncFuturesCount; i++) this.syncFutures[i].done(sequence, e);
      this.syncFuturesCount = 0;
    }

    /**
     * @return True if outstanding sync futures still
     */
    private boolean isOutstandingSyncs() {
      // Look at SyncFutures in the EventHandler
      for (int i = 0; i < this.syncFuturesCount; i++) {
        if (!this.syncFutures[i].isDone()) return true;
      }

      return false;
    }

    private boolean isOutstandingSyncsFromRunners() {
      // Look at SyncFutures in the SyncRunners
      for (SyncRunner syncRunner: syncRunners) {
        if(syncRunner.isAlive() && !syncRunner.areSyncFuturesReleased()) {
          return true;
        }
      }
      return false;
    }

    @Override
    // We can set endOfBatch in the below method if at end of our this.syncFutures array
    public void onEvent(final RingBufferTruck truck, final long sequence, boolean endOfBatch)
    throws Exception {
      // Appends and syncs are coming in order off the ringbuffer.  We depend on this fact.  We'll
      // add appends to dfsclient as they come in.  Batching appends doesn't give any significant
      // benefit on measurement.  Handler sync calls we will batch up. If we get an exception
      // appending an edit, we fail all subsequent appends and syncs with the same exception until
      // the WAL is reset. It is important that we not short-circuit and exit early this method.
      // It is important that we always go through the attainSafePoint on the end. Another thread,
      // the log roller may be waiting on a signal from us here and will just hang without it.

      try {
        if (truck.hasSyncFuturePayload()) {
          this.syncFutures[this.syncFuturesCount++] = truck.unloadSyncFuturePayload();
          // Force flush of syncs if we are carrying a full complement of syncFutures.
          if (this.syncFuturesCount == this.syncFutures.length) endOfBatch = true;
        } else if (truck.hasFSWALEntryPayload()) {
          TraceScope scope = Trace.continueSpan(truck.unloadSpanPayload());
          try {
            FSWALEntry entry = truck.unloadFSWALEntryPayload();
            if (this.exception != null) {
              // Return to keep processing events coming off the ringbuffer
              return;
            }
            append(entry);
          } catch (Exception e) {
            // Failed append. Record the exception.
            this.exception = e;
            // invoking cleanupOutstandingSyncsOnException when append failed with exception,
            // it will cleanup existing sync requests recorded in syncFutures but not offered to SyncRunner yet,
            // so there won't be any sync future left over if no further truck published to disruptor.
            cleanupOutstandingSyncsOnException(sequence,
                this.exception instanceof DamagedWALException ? this.exception
                    : new DamagedWALException("On sync", this.exception));
            // Return to keep processing events coming off the ringbuffer
            return;
          } finally {
            assert scope == NullScope.INSTANCE || !scope.isDetached();
            scope.close(); // append scope is complete
          }
        } else {
          // What is this if not an append or sync. Fail all up to this!!!
          cleanupOutstandingSyncsOnException(sequence,
            new IllegalStateException("Neither append nor sync"));
          // Return to keep processing.
          return;
        }

        // TODO: Check size and if big go ahead and call a sync if we have enough data.
        // This is a sync. If existing exception, fall through. Else look to see if batch.
        if (this.exception == null) {
          // If not a batch, return to consume more events from the ring buffer before proceeding;
          // we want to get up a batch of syncs and appends before we go do a filesystem sync.
          if (!endOfBatch || this.syncFuturesCount <= 0) return;
          // syncRunnerIndex is bound to the range [0, Integer.MAX_INT - 1] as follows:
          //   * The maximum value possible for syncRunners.length is Integer.MAX_INT
          //   * syncRunnerIndex starts at 0 and is incremented only here
          //   * after the increment, the value is bounded by the '%' operator to [0, syncRunners.length),
          //     presuming the value was positive prior to the '%' operator.
          //   * after being bound to [0, Integer.MAX_INT - 1], the new value is stored in syncRunnerIndex
          //     ensuring that it can't grow without bound and overflow.
          //   * note that the value after the increment must be positive, because the most it could have
          //     been prior was Integer.MAX_INT - 1 and we only increment by 1.
          this.syncRunnerIndex = (this.syncRunnerIndex + 1) % this.syncRunners.length;
          try {
            // Below expects that the offer 'transfers' responsibility for the outstanding syncs to
            // the syncRunner. We should never get an exception in here.
            this.syncRunners[this.syncRunnerIndex].offer(sequence, this.syncFutures,
              this.syncFuturesCount);
          } catch (Exception e) {
            // Should NEVER get here.
            requestLogRoll();
            this.exception = new DamagedWALException("Failed offering sync", e);
          }
        }
        // We may have picked up an exception above trying to offer sync
        if (this.exception != null) {
          cleanupOutstandingSyncsOnException(sequence,
            this.exception instanceof DamagedWALException?
              this.exception:
              new DamagedWALException("On sync", this.exception));
        }
        attainSafePoint(sequence);
        this.syncFuturesCount = 0;
      } catch (Throwable t) {
        LOG.error("UNEXPECTED!!! syncFutures.length=" + this.syncFutures.length, t);
      }
    }

    SafePointZigZagLatch attainSafePoint() {
      this.zigzagLatch = new SafePointZigZagLatch();
      return this.zigzagLatch;
    }

    /**
     * Check if we should attain safe point.  If so, go there and then wait till signalled before
     * we proceeding.
     */
    private void attainSafePoint(final long currentSequence) {
      if (this.zigzagLatch == null || !this.zigzagLatch.isCocked()) return;
      // If here, another thread is waiting on us to get to safe point.  Don't leave it hanging.
      beforeWaitOnSafePoint();
      try {
        // Wait on outstanding syncers; wait for them to finish syncing (unless we've been
        // shutdown or unless our latch has been thrown because we have been aborted or unless
        // this WAL is broken and we can't get a sync/append to complete).
        while ((!this.shutdown && this.zigzagLatch.isCocked()
            && highestSyncedSequence.get() < currentSequence &&
            // We could be in here and all syncs are failing or failed. Check for this. Otherwise
            // we'll just be stuck here for ever. In other words, ensure there syncs running.
            isOutstandingSyncs())
            // Wait for all SyncRunners to finish their work so that we can replace the writer
            || isOutstandingSyncsFromRunners()) {
          synchronized (this.safePointWaiter) {
            this.safePointWaiter.wait(0, 1);
          }
        }
        // Tell waiting thread we've attained safe point. Can clear this.throwable if set here
        // because we know that next event through the ringbuffer will be going to a new WAL
        // after we do the zigzaglatch dance.
        this.exception = null;
        this.zigzagLatch.safePointAttained();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted ", e);
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Append to the WAL.  Does all CP and WAL listener calls.
     * @param entry
     * @throws Exception
     */
    void append(final FSWALEntry entry) throws Exception {
      // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
      atHeadOfRingBufferEventHandlerAppend();

      long start = EnvironmentEdgeManager.currentTime();
      byte [] encodedRegionName = entry.getKey().getEncodedRegionName();
      long regionSequenceId = WALKey.NO_SEQUENCE_ID;
      try {

        regionSequenceId = entry.getKey().getSequenceId();
        // Edits are empty, there is nothing to append.  Maybe empty when we are looking for a
        // region sequence id only, a region edit/sequence id that is not associated with an actual
        // edit. It has to go through all the rigmarole to be sure we have the right ordering.
        if (entry.getEdit().isEmpty()) {
          return;
        }

        // Coprocessor hook.
        if (!coprocessorHost.preWALWrite(entry.getHRegionInfo(), entry.getKey(),
            entry.getEdit())) {
          if (entry.getEdit().isReplay()) {
            // Set replication scope null so that this won't be replicated
            entry.getKey().setScopes(null);
          }
        }
        if (!listeners.isEmpty()) {
          for (WALActionsListener i: listeners) {
            // TODO: Why does listener take a table description and CPs take a regioninfo?  Fix.
            i.visitLogEntryBeforeWrite(entry.getHTableDescriptor(), entry.getKey(),
              entry.getEdit());
          }
        }

        writer.append(entry);
        assert highestUnsyncedSequence < entry.getSequence();
        highestUnsyncedSequence = entry.getSequence();
        sequenceIdAccounting.update(encodedRegionName, entry.getFamilyNames(), regionSequenceId,
          entry.isInMemstore());
        coprocessorHost.postWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit());
        // Update metrics.
        postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
      } catch (Exception e) {
        String msg = "Append sequenceId=" + regionSequenceId + ", requesting roll of WAL";
        LOG.warn(msg, e);
        requestLogRoll();
        throw new DamagedWALException(msg, e);
      }
      numEntries.incrementAndGet();
    }

    @Override
    public void onStart() {
      for (SyncRunner syncRunner: this.syncRunners) syncRunner.start();
    }

    @Override
    public void onShutdown() {
      for (SyncRunner syncRunner: this.syncRunners) syncRunner.interrupt();
    }
  }

  /**
   * Exposed for testing only.  Use to tricks like halt the ring buffer appending.
   */
  @VisibleForTesting
  void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException)? (IOException)t: new IOException(t);
  }

  private static void usage() {
    System.err.println("Usage: FSHLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: " +
      "FSHLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: " +
      "FSHLog --split hdfs://example.com:9000/hbase/.logs/DIR");
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
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.fatal("Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.fatal("\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " +
          args[1]);
      System.exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }

  /**
   * This method gets the pipeline for the current WAL.
   */
  @VisibleForTesting
  DatanodeInfo[] getPipeLine() {
    if (this.hdfs_out != null) {
      if (this.hdfs_out.getWrappedStream() instanceof DFSOutputStream) {
        return ((DFSOutputStream) this.hdfs_out.getWrappedStream()).getPipeline();
      }
    }
    return new DatanodeInfo[0];
  }

  /**
   *
   * @return last time on checking low replication
   */
  public long getLastTimeCheckLowReplication() {
    return this.lastTimeCheckLowReplication;
  }

  @VisibleForTesting
  Writer getWriter() {
    return this.writer;
  }

  @VisibleForTesting
  void setWriter(Writer writer) {
    this.writer = writer;
  }
}
