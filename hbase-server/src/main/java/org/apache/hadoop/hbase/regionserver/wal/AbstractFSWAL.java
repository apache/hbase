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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.ERROR;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.LOW_REPLICATION;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SLOW_SYNC;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.WAL_IMPL;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import com.google.errorprone.annotations.RestrictedApi;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import io.opentelemetry.api.trace.Span;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.MemoryType;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.WriterBase;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS. Only one
 * WAL is ever being written at a time. When a WAL hits a configured maximum size, it is rolled.
 * This is done internal to the implementation.
 * <p>
 * As data is flushed from the MemStore to other on-disk structures (files sorted by key, hfiles), a
 * WAL becomes obsolete. We can let go of all the log edits/entries for a given HRegion-sequence id.
 * A bunch of work in the below is done keeping account of these region sequence ids -- what is
 * flushed out to hfiles, and what is yet in WAL and in memory only.
 * <p>
 * It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 * <p>
 * To read an WAL, call {@link WALFactory#createStreamReader(FileSystem, Path)} for one way read,
 * call {@link WALFactory#createTailingReader(FileSystem, Path, Configuration, long)} for
 * replication where we may want to tail the active WAL file.
 * <h2>Failure Semantic</h2> If an exception on append or sync, roll the WAL because the current WAL
 * is now a lame duck; any more appends or syncs will fail also with the same original exception. If
 * we have made successful appends to the WAL and we then are unable to sync them, our current
 * semantic is to return error to the client that the appends failed but also to abort the current
 * context, usually the hosting server. We need to replay the WALs. <br>
 * TODO: Change this semantic. A roll of WAL may be sufficient as long as we have flagged client
 * that the append failed. <br>
 * TODO: replication may pick up these last edits though they have been marked as failed append
 * (Need to keep our own file lengths, not rely on HDFS).
 */
@InterfaceAudience.Private
public abstract class AbstractFSWAL<W extends WriterBase> implements WAL {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWAL.class);

  private static final Comparator<SyncFuture> SEQ_COMPARATOR =
    Comparator.comparingLong(SyncFuture::getTxid).thenComparingInt(System::identityHashCode);

  private static final String SURVIVED_TOO_LONG_SEC_KEY = "hbase.regionserver.wal.too.old.sec";
  private static final int SURVIVED_TOO_LONG_SEC_DEFAULT = 900;
  /** Don't log blocking regions more frequently than this. */
  private static final long SURVIVED_TOO_LONG_LOG_INTERVAL_NS = TimeUnit.MINUTES.toNanos(5);

  protected static final String SLOW_SYNC_TIME_MS = "hbase.regionserver.wal.slowsync.ms";
  protected static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms
  protected static final String ROLL_ON_SYNC_TIME_MS = "hbase.regionserver.wal.roll.on.sync.ms";
  protected static final int DEFAULT_ROLL_ON_SYNC_TIME_MS = 10000; // in ms
  protected static final String SLOW_SYNC_ROLL_THRESHOLD =
    "hbase.regionserver.wal.slowsync.roll.threshold";
  protected static final int DEFAULT_SLOW_SYNC_ROLL_THRESHOLD = 100; // 100 slow sync warnings
  protected static final String SLOW_SYNC_ROLL_INTERVAL_MS =
    "hbase.regionserver.wal.slowsync.roll.interval.ms";
  protected static final int DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS = 60 * 1000; // in ms, 1 minute

  public static final String WAL_SYNC_TIMEOUT_MS = "hbase.regionserver.wal.sync.timeout";
  protected static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  public static final String WAL_ROLL_MULTIPLIER = "hbase.regionserver.logroll.multiplier";

  public static final String MAX_LOGS = "hbase.regionserver.maxlogs";

  public static final String RING_BUFFER_SLOT_COUNT =
    "hbase.regionserver.wal.disruptor.event.count";

  public static final String WAL_SHUTDOWN_WAIT_TIMEOUT_MS = "hbase.wal.shutdown.wait.timeout.ms";
  public static final int DEFAULT_WAL_SHUTDOWN_WAIT_TIMEOUT_MS = 15 * 1000;

  public static final String WAL_BATCH_SIZE = "hbase.wal.batch.size";
  public static final long DEFAULT_WAL_BATCH_SIZE = 64L * 1024;

  public static final String WAL_AVOID_LOCAL_WRITES_KEY =
    "hbase.regionserver.wal.avoid-local-writes";
  public static final boolean WAL_AVOID_LOCAL_WRITES_DEFAULT = false;

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  protected final Path walDir;

  private final FileSystem remoteFs;

  private final Path remoteWALDir;

  /**
   * dir path where old logs are kept.
   */
  protected final Path walArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  protected final PathFilter ourFiles;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  protected final String walFilePrefix;

  /**
   * Suffix included on generated wal file names
   */
  protected final String walFileSuffix;

  /**
   * Prefix used when checking for wal membership.
   */
  protected final String prefixPathStr;

  protected final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;

  protected final Abortable abortable;

  /** Listeners that are called on WAL events. */
  protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();

  /** Tracks the logs in the process of being closed. */
  protected final Map<String, W> inflightWALClosures = new ConcurrentHashMap<>();

  /**
   * Class that does accounting of sequenceids in WAL subsystem. Holds oldest outstanding sequence
   * id as yet not flushed as well as the most recent edit sequence id appended to the WAL. Has
   * facility for answering questions such as "Is it safe to GC a WAL?".
   */
  protected final SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();

  /** The slow sync will be logged; the very slow sync will cause the WAL to be rolled. */
  protected final long slowSyncNs, rollOnSyncNs;
  protected final int slowSyncRollThreshold;
  protected final int slowSyncCheckInterval;
  protected final AtomicInteger slowSyncCount = new AtomicInteger();

  private final long walSyncTimeoutNs;

  private final long walTooOldNs;

  // If > than this size, roll the log.
  protected final long logrollsize;

  /**
   * Block size to use writing files.
   */
  protected final long blocksize;

  /*
   * If more than this many logs, force flush of oldest region to the oldest edit goes to disk. If
   * too many and we crash, then will take forever replaying. Keep the number of logs tidy.
   */
  protected final int maxLogs;

  protected final boolean useHsync;

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other lock
   * is held. We don't just use synchronized because that results in bogus and tedious findbugs
   * warning when it thinks synchronized controls writer thread safety. It is held when we are
   * actually rolling the log. It is checked when we are looking to see if we should roll the log or
   * not.
   */
  protected final ReentrantLock rollWriterLock = new ReentrantLock(true);

  // The timestamp (in ms) when the log file was created.
  protected final AtomicLong filenum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  protected final AtomicInteger numEntries = new AtomicInteger(0);

  /**
   * The highest known outstanding unsync'd WALEdit transaction id. Usually, we use a queue to pass
   * WALEdit to background consumer thread, and the transaction id is the sequence number of the
   * corresponding entry in queue.
   */
  protected volatile long highestUnsyncedTxid = -1;

  /**
   * Updated to the transaction id of the last successful sync call. This can be less than
   * {@link #highestUnsyncedTxid} for case where we have an append where a sync has not yet come in
   * for it.
   */
  protected final AtomicLong highestSyncedTxid = new AtomicLong(0);

  /**
   * The total size of wal
   */
  protected final AtomicLong totalLogSize = new AtomicLong(0);
  /**
   * Current log file.
   */
  volatile W writer;

  // Last time to check low replication on hlog's pipeline
  private volatile long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  // Last time we asked to roll the log due to a slow sync
  private volatile long lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();

  protected volatile boolean closed = false;

  protected final AtomicBoolean shutdown = new AtomicBoolean(false);

  protected final long walShutdownTimeout;

  private long nextLogTooOldNs = System.nanoTime();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name. Throws
   * an IllegalArgumentException if used to compare paths from different wals.
   */
  final Comparator<Path> LOG_NAME_COMPARATOR =
    (o1, o2) -> Long.compare(getFileNumFromFileName(o1), getFileNumFromFileName(o2));

  private static final class WALProps {

    /**
     * Map the encoded region name to the highest sequence id.
     * <p/>
     * Contains all the regions it has an entry for.
     */
    private final Map<byte[], Long> encodedName2HighestSequenceId;

    /**
     * The log file size. Notice that the size may not be accurate if we do asynchronous close in
     * subclasses.
     */
    private final long logSize;

    /**
     * The nanoTime of the log rolling, used to determine the time interval that has passed since.
     */
    private final long rollTimeNs;

    /**
     * If we do asynchronous close in subclasses, it is possible that when adding WALProps to the
     * rolled map, the file is not closed yet, so in cleanOldLogs we should not archive this file,
     * for safety.
     */
    private volatile boolean closed = false;

    WALProps(Map<byte[], Long> encodedName2HighestSequenceId, long logSize) {
      this.encodedName2HighestSequenceId = encodedName2HighestSequenceId;
      this.logSize = logSize;
      this.rollTimeNs = System.nanoTime();
    }
  }

  /**
   * Map of WAL log file to properties. The map is sorted by the log file creation timestamp
   * (contained in the log file name).
   */
  protected final ConcurrentNavigableMap<Path, WALProps> walFile2Props =
    new ConcurrentSkipListMap<>(LOG_NAME_COMPARATOR);

  /**
   * A cache of sync futures reused by threads.
   */
  protected final SyncFutureCache syncFutureCache;

  /**
   * The class name of the runtime implementation, used as prefix for logging/tracing.
   * <p>
   * Performance testing shows getClass().getSimpleName() might be a bottleneck so we store it here,
   * refer to HBASE-17676 for more details
   * </p>
   */
  protected final String implClassName;

  protected final AtomicBoolean rollRequested = new AtomicBoolean(false);

  protected final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  // Run in caller if we get reject execution exception, to avoid aborting region server when we get
  // reject execution exception. Usually this should not happen but let's make it more robust.
  private final ExecutorService logArchiveExecutor =
    new ThreadPoolExecutor(1, 1, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WAL-Archive-%d").build(),
      new ThreadPoolExecutor.CallerRunsPolicy());

  private final int archiveRetries;

  protected ExecutorService consumeExecutor;

  private final Lock consumeLock = new ReentrantLock();

  protected final Runnable consumer = this::consume;

  // check if there is already a consumer task in the event loop's task queue
  protected Supplier<Boolean> hasConsumerTask;

  private static final int MAX_EPOCH = 0x3FFFFFFF;
  // the lowest bit is waitingRoll, which means new writer is created, and we are waiting for old
  // writer to be closed.
  // the second-lowest bit is writerBroken which means the current writer is broken and rollWriter
  // is needed.
  // all other bits are the epoch number of the current writer, this is used to detect whether the
  // writer is still the one when you issue the sync.
  // notice that, modification to this field is only allowed under the protection of consumeLock.
  private volatile int epochAndState;

  private boolean readyForRolling;

  private final Condition readyForRollingCond = consumeLock.newCondition();

  private final RingBuffer<RingBufferTruck> waitingConsumePayloads;

  private final Sequence waitingConsumePayloadsGatingSequence;

  private final AtomicBoolean consumerScheduled = new AtomicBoolean(false);

  private final long batchSize;

  protected final Deque<FSWALEntry> toWriteAppends = new ArrayDeque<>();

  protected final Deque<FSWALEntry> unackedAppends = new ArrayDeque<>();

  protected final SortedSet<SyncFuture> syncFutures = new TreeSet<>(SEQ_COMPARATOR);

  // the highest txid of WAL entries being processed
  protected long highestProcessedAppendTxid;

  // file length when we issue last sync request on the writer
  private long fileLengthAtLastSync;

  private long highestProcessedAppendTxidAtLastSync;

  private int waitOnShutdownInSeconds;

  private String waitOnShutdownInSecondsConfigKey;

  protected boolean shouldShutDownConsumeExecutorWhenClose = true;

  private volatile boolean skipRemoteWAL = false;

  private volatile boolean markerEditOnly = false;

  public long getFilenum() {
    return this.filenum.get();
  }

  /**
   * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}. This helper
   * method returns the creation timestamp from a given log file. It extracts the timestamp assuming
   * the filename is created with the {@link #computeFilename(long filenum)} method.
   * @return timestamp, as in the log file name.
   */
  protected long getFileNumFromFileName(Path fileName) {
    checkNotNull(fileName, "file name can't be null");
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException(
        "The log file " + fileName + " doesn't belong to this WAL. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
      (fileNameString.length() - walFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  private int calculateMaxLogFiles(Configuration conf, long logRollSize) {
    checkArgument(logRollSize > 0,
      "The log roll size cannot be zero or negative when calculating max log files, "
        + "current value is " + logRollSize);
    Pair<Long, MemoryType> globalMemstoreSize = MemorySizeUtil.getGlobalMemStoreSize(conf);
    return (int) ((globalMemstoreSize.getFirst() * 2) / logRollSize);
  }

  // must be power of 2
  protected final int getPreallocatedEventCount() {
    // Preallocate objects to use on the ring buffer. The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    int preallocatedEventCount = this.conf.getInt(RING_BUFFER_SLOT_COUNT, 1024 * 16);
    checkArgument(preallocatedEventCount >= 0, RING_BUFFER_SLOT_COUNT + " must > 0");
    int floor = Integer.highestOneBit(preallocatedEventCount);
    if (floor == preallocatedEventCount) {
      return floor;
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return floor << 1;
  }

  protected final void setWaitOnShutdownInSeconds(int waitOnShutdownInSeconds,
    String waitOnShutdownInSecondsConfigKey) {
    this.waitOnShutdownInSeconds = waitOnShutdownInSeconds;
    this.waitOnShutdownInSecondsConfigKey = waitOnShutdownInSecondsConfigKey;
  }

  protected final void createSingleThreadPoolConsumeExecutor(String walType, final Path rootDir,
    final String prefix) {
    ThreadPoolExecutor threadPool =
      new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setNameFormat(walType + "-%d-" + rootDir.toString() + "-prefix:"
          + (prefix == null ? "default" : prefix).replace("%", "%%")).setDaemon(true).build());
    hasConsumerTask = () -> threadPool.getQueue().peek() == consumer;
    consumeExecutor = threadPool;
    this.shouldShutDownConsumeExecutorWhenClose = true;
  }

  protected AbstractFSWAL(final FileSystem fs, final Abortable abortable, final Path rootDir,
    final String logDir, final String archiveDir, final Configuration conf,
    final List<WALActionsListener> listeners, final boolean failIfWALExists, final String prefix,
    final String suffix, FileSystem remoteFs, Path remoteWALDir)
    throws FailedLogCloseException, IOException {
    this.fs = fs;
    this.walDir = new Path(rootDir, logDir);
    this.walArchiveDir = new Path(rootDir, archiveDir);
    this.conf = conf;
    this.abortable = abortable;
    this.remoteFs = remoteFs;
    this.remoteWALDir = remoteWALDir;

    if (!fs.exists(walDir) && !fs.mkdirs(walDir)) {
      throw new IOException("Unable to mkdir " + walDir);
    }

    if (!fs.exists(this.walArchiveDir)) {
      if (!fs.mkdirs(this.walArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.walArchiveDir);
      }
    }

    // If prefix is null||empty then just name it wal
    this.walFilePrefix = prefix == null || prefix.isEmpty()
      ? "wal"
      : URLEncoder.encode(prefix, StandardCharsets.UTF_8.name());
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER
        + "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    String storagePolicy =
      conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
    CommonFSUtils.setStoragePolicy(fs, this.walDir, storagePolicy);
    this.walFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.prefixPathStr = new Path(walDir, walFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (walFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang3.StringUtils
            .isNumeric(fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(walFileSuffix)) {
          return false;
        }
        return true;
      }
    };

    if (failIfWALExists) {
      final FileStatus[] walFiles = CommonFSUtils.listStatus(fs, walDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + walDir);
      }
    }

    // Register listeners. TODO: Should this exist anymore? We have CPs?
    if (listeners != null) {
      for (WALActionsListener i : listeners) {
        registerWALActionsListener(i);
      }
    }
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    // Schedule a WAL roll when the WAL is 50% of the HDFS block size. Scheduling at 50% of block
    // size should make it so WAL rolls before we get to the end-of-block (Block transitions cost
    // some latency). In hbase-1 we did this differently. We scheduled a roll when we hit 95% of
    // the block size but experience from the field has it that this was not enough time for the
    // roll to happen before end-of-block. So the new accounting makes WALs of about the same
    // size as those made in hbase-1 (to prevent surprise), we now have default block size as
    // 2 times the DFS default: i.e. 2 * DFS default block size rolling at 50% full will generally
    // make similar size logs to 1 * DFS default block size rolling at 95% full. See HBASE-19148.
    this.blocksize = WALUtil.getWALBlockSize(this.conf, this.fs, this.walDir);
    float multiplier = conf.getFloat(WAL_ROLL_MULTIPLIER, 0.5f);
    this.logrollsize = (long) (this.blocksize * multiplier);
    this.maxLogs = conf.getInt(MAX_LOGS, Math.max(32, calculateMaxLogFiles(conf, logrollsize)));

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) + ", rollsize="
      + StringUtils.byteDesc(this.logrollsize) + ", prefix=" + this.walFilePrefix + ", suffix="
      + walFileSuffix + ", logDir=" + this.walDir + ", archiveDir=" + this.walArchiveDir
      + ", maxLogs=" + this.maxLogs);
    this.slowSyncNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getInt(SLOW_SYNC_TIME_MS, DEFAULT_SLOW_SYNC_TIME_MS));
    this.rollOnSyncNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getInt(ROLL_ON_SYNC_TIME_MS, DEFAULT_ROLL_ON_SYNC_TIME_MS));
    this.slowSyncRollThreshold =
      conf.getInt(SLOW_SYNC_ROLL_THRESHOLD, DEFAULT_SLOW_SYNC_ROLL_THRESHOLD);
    this.slowSyncCheckInterval =
      conf.getInt(SLOW_SYNC_ROLL_INTERVAL_MS, DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS);
    this.walSyncTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(WAL_SYNC_TIMEOUT_MS, DEFAULT_WAL_SYNC_TIMEOUT_MS));
    this.syncFutureCache = new SyncFutureCache(conf);
    this.implClassName = getClass().getSimpleName();
    this.walTooOldNs = TimeUnit.SECONDS
      .toNanos(conf.getInt(SURVIVED_TOO_LONG_SEC_KEY, SURVIVED_TOO_LONG_SEC_DEFAULT));
    this.useHsync = conf.getBoolean(HRegion.WAL_HSYNC_CONF_KEY, HRegion.DEFAULT_WAL_HSYNC);
    archiveRetries = this.conf.getInt("hbase.regionserver.walroll.archive.retries", 0);
    this.walShutdownTimeout =
      conf.getLong(WAL_SHUTDOWN_WAIT_TIMEOUT_MS, DEFAULT_WAL_SHUTDOWN_WAIT_TIMEOUT_MS);

    int preallocatedEventCount =
      conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    waitingConsumePayloads =
      RingBuffer.createMultiProducer(RingBufferTruck::new, preallocatedEventCount);
    waitingConsumePayloadsGatingSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    waitingConsumePayloads.addGatingSequences(waitingConsumePayloadsGatingSequence);

    // inrease the ringbuffer sequence so our txid is start from 1
    waitingConsumePayloads.publish(waitingConsumePayloads.next());
    waitingConsumePayloadsGatingSequence.set(waitingConsumePayloads.getCursor());

    batchSize = conf.getLong(WAL_BATCH_SIZE, DEFAULT_WAL_BATCH_SIZE);
  }

  /**
   * Used to initialize the WAL. Usually just call rollWriter to create the first log writer.
   */
  @Override
  public void init() throws IOException {
    rollWriter();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, familyToSeq);
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName, long maxFlushedSeqId) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName, maxFlushedSeqId);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
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

  @Override
  public Map<byte[], List<byte[]>> rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  @Override
  public final void sync() throws IOException {
    sync(useHsync);
  }

  @Override
  public final void sync(long txid) throws IOException {
    sync(txid, useHsync);
  }

  @Override
  public final void sync(boolean forceSync) throws IOException {
    TraceUtil.trace(() -> doSync(forceSync), () -> createSpan("WAL.sync"));
  }

  @Override
  public final void sync(long txid, boolean forceSync) throws IOException {
    TraceUtil.trace(() -> doSync(txid, forceSync), () -> createSpan("WAL.sync"));
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public SequenceIdAccounting getSequenceIdAccounting() {
    return sequenceIdAccounting;
  }

  /**
   * This is a convenience method that computes a new filename with a given file-number.
   * @param filenum to use
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = walFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + walFileSuffix;
    return new Path(walDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given using the current WAL
   * file-number
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  /**
   * retrieve the next path to use for writing. Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(Math.max(getFilenum() + 1, EnvironmentEdgeManager.currentTime()));
    Path newPath = getCurrentFileName();
    return newPath;
  }

  public Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * Tell listeners about pre log roll.
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

  // public only until class moves to o.a.h.h.wal
  /** Returns the number of rolled log files */
  public int getNumRolledLogFiles() {
    return walFile2Props.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** Returns the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  /**
   * If the number of un-archived WAL files ('live' WALs) is greater than maximum allowed, check the
   * first (oldest) WAL, and return those regions which should be flushed so that it can be
   * let-go/'archived'.
   * @return stores of regions (encodedRegionNames) to flush in order to archive the oldest WAL file
   */
  Map<byte[], List<byte[]>> findRegionsToForceFlush() throws IOException {
    Map<byte[], List<byte[]>> regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, WALProps> firstWALEntry = this.walFile2Props.firstEntry();
      regions =
        this.sequenceIdAccounting.findLower(firstWALEntry.getValue().encodedName2HighestSequenceId);
    }
    if (regions != null) {
      List<String> listForPrint = new ArrayList<>();
      for (Map.Entry<byte[], List<byte[]>> r : regions.entrySet()) {
        StringBuilder families = new StringBuilder();
        for (int i = 0; i < r.getValue().size(); i++) {
          if (i > 0) {
            families.append(",");
          }
          families.append(Bytes.toString(r.getValue().get(i)));
        }
        listForPrint.add(Bytes.toStringBinary(r.getKey()) + "[" + families.toString() + "]");
      }
      LOG.info("Too many WALs; count=" + logCount + ", max=" + this.maxLogs
        + "; forcing (partial) flush of " + regions.size() + " region(s): "
        + StringUtils.join(",", listForPrint));
    }
    return regions;
  }

  /**
   * Mark this WAL file as closed and call cleanOldLogs to see if we can archive this file.
   */
  private void markClosedAndClean(Path path) {
    WALProps props = walFile2Props.get(path);
    // typically this should not be null, but if there is no big issue if it is already null, so
    // let's make the code more robust
    if (props != null) {
      props.closed = true;
      cleanOldLogs();
    }
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   * <p/>
   * Use synchronized because we may call this method in different threads, normally when replacing
   * writer, and since now close writer may be asynchronous, we will also call this method in the
   * closeExecutor, right after we actually close a WAL writer.
   */
  private synchronized void cleanOldLogs() {
    List<Pair<Path, Long>> logsToArchive = null;
    long now = System.nanoTime();
    boolean mayLogTooOld = nextLogTooOldNs <= now;
    ArrayList<byte[]> regionsBlockingWal = null;
    // For each log file, look at its Map of regions to the highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Map.Entry<Path, WALProps> e : this.walFile2Props.entrySet()) {
      if (!e.getValue().closed) {
        LOG.debug("{} is not closed yet, will try archiving it next time", e.getKey());
        continue;
      }
      Path log = e.getKey();
      ArrayList<byte[]> regionsBlockingThisWal = null;
      long ageNs = now - e.getValue().rollTimeNs;
      if (ageNs > walTooOldNs) {
        if (mayLogTooOld && regionsBlockingWal == null) {
          regionsBlockingWal = new ArrayList<>();
        }
        regionsBlockingThisWal = regionsBlockingWal;
      }
      Map<byte[], Long> sequenceNums = e.getValue().encodedName2HighestSequenceId;
      if (this.sequenceIdAccounting.areAllLower(sequenceNums, regionsBlockingThisWal)) {
        if (logsToArchive == null) {
          logsToArchive = new ArrayList<>();
        }
        logsToArchive.add(Pair.newPair(log, e.getValue().logSize));
        if (LOG.isTraceEnabled()) {
          LOG.trace("WAL file ready for archiving " + log);
        }
      } else if (regionsBlockingThisWal != null) {
        StringBuilder sb = new StringBuilder(log.toString()).append(" has not been archived for ")
          .append(TimeUnit.NANOSECONDS.toSeconds(ageNs)).append(" seconds; blocked by: ");
        boolean isFirst = true;
        for (byte[] region : regionsBlockingThisWal) {
          if (!isFirst) {
            sb.append("; ");
          }
          isFirst = false;
          sb.append(Bytes.toString(region));
        }
        LOG.warn(sb.toString());
        nextLogTooOldNs = now + SURVIVED_TOO_LONG_LOG_INTERVAL_NS;
        regionsBlockingThisWal.clear();
      }
    }

    if (logsToArchive != null) {
      final List<Pair<Path, Long>> localLogsToArchive = logsToArchive;
      // make it async
      for (Pair<Path, Long> log : localLogsToArchive) {
        logArchiveExecutor.execute(() -> {
          archive(log);
        });
        this.walFile2Props.remove(log.getFirst());
      }
    }
  }

  protected void archive(final Pair<Path, Long> log) {
    totalLogSize.addAndGet(-log.getSecond());
    int retry = 1;
    while (true) {
      try {
        archiveLogFile(log.getFirst());
        // successful
        break;
      } catch (Throwable e) {
        if (retry > archiveRetries) {
          LOG.error("Failed log archiving for the log {},", log.getFirst(), e);
          if (this.abortable != null) {
            this.abortable.abort("Failed log archiving", e);
            break;
          }
        } else {
          LOG.error("Log archiving failed for the log {} - attempt {}", log.getFirst(), retry, e);
        }
        retry++;
      }
    }
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  protected void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.walArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
    if (!CommonFSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  protected final void logRollAndSetupWalProps(Path oldPath, Path newPath, long oldFileLen) {
    int oldNumEntries = this.numEntries.getAndSet(0);
    String newPathString = newPath != null ? CommonFSUtils.getPath(newPath) : null;
    if (oldPath != null) {
      this.walFile2Props.put(oldPath,
        new WALProps(this.sequenceIdAccounting.resetHighest(), oldFileLen));
      this.totalLogSize.addAndGet(oldFileLen);
      LOG.info("Rolled WAL {} with entries={}, filesize={}; new WAL {}",
        CommonFSUtils.getPath(oldPath), oldNumEntries, StringUtils.byteDesc(oldFileLen),
        newPathString);
    } else {
      LOG.info("New WAL {}", newPathString);
    }
  }

  private Span createSpan(String name) {
    return TraceUtil.createSpan(name).setAttribute(WAL_IMPL, implClassName);
  }

  /**
   * Cleans up current writer closing it and then puts in place the passed in {@code nextWriter}.
   * <p/>
   * <ul>
   * <li>In the case of creating a new WAL, oldPath will be null.</li>
   * <li>In the case of rolling over from one file to the next, none of the parameters will be null.
   * </li>
   * <li>In the case of closing out this FSHLog with no further use newPath and nextWriter will be
   * null.</li>
   * </ul>
   * @param oldPath    may be null
   * @param newPath    may be null
   * @param nextWriter may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  Path replaceWriter(Path oldPath, Path newPath, W nextWriter) throws IOException {
    return TraceUtil.trace(() -> {
      doReplaceWriter(oldPath, newPath, nextWriter);
      return newPath;
    }, () -> createSpan("WAL.replaceWriter"));
  }

  protected final void blockOnSync(SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      if (syncFuture != null) {
        if (closed) {
          throw new IOException("WAL has been closed");
        } else {
          syncFuture.get(walSyncTimeoutNs);
        }
      }
    } catch (TimeoutIOException tioe) {
      throw new WALSyncTimeoutIOException(tioe);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException) ? (IOException) t : new IOException(t);
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private W createCombinedWriter(W localWriter, Path localPath)
    throws IOException, CommonFSUtils.StreamLacksCapabilityException {
    // retry forever if we can not create the remote writer to prevent aborting the RS due to log
    // rolling error, unless the skipRemoteWal is set to true.
    // TODO: since for now we only have one thread doing log rolling, this may block the rolling for
    // other wals
    Path remoteWAL = new Path(remoteWALDir, localPath.getName());
    for (int retry = 0;; retry++) {
      if (skipRemoteWAL) {
        return localWriter;
      }
      W remoteWriter;
      try {
        remoteWriter = createWriterInstance(remoteFs, remoteWAL);
      } catch (IOException e) {
        LOG.warn("create remote writer {} failed, retry = {}", remoteWAL, retry, e);
        try {
          Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
        } catch (InterruptedException ie) {
          // restore the interrupt state
          Thread.currentThread().interrupt();
          // must close local writer here otherwise no one will close it for us
          Closeables.close(localWriter, true);
          throw (IOException) new InterruptedIOException().initCause(ie);
        }
        continue;
      }
      return createCombinedWriter(localWriter, remoteWriter);
    }
  }

  private Map<byte[], List<byte[]>> rollWriterInternal(boolean force) throws IOException {
    rollWriterLock.lock();
    try {
      if (this.closed) {
        throw new WALClosedException("WAL has been closed");
      }
      // Return if nothing to flush.
      if (!force && this.writer != null && this.numEntries.get() <= 0) {
        return null;
      }
      Map<byte[], List<byte[]>> regionsToFlush = null;
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable, so we currently abort.
        W nextWriter = this.createWriterInstance(fs, newPath);
        if (remoteFs != null) {
          // create a remote wal if necessary
          nextWriter = createCombinedWriter(nextWriter, newPath);
        }
        tellListenersAboutPreLogRoll(oldPath, newPath);
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter);
        tellListenersAboutPostLogRoll(oldPath, newPath);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create new " + implClassName + " writer with pipeline: "
            + FanOutOneBlockAsyncDFSOutputHelper
              .getDataNodeInfo(Arrays.stream(getPipeline()).collect(Collectors.toList())));
        }
        // We got a new writer, so reset the slow sync count
        lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();
        slowSyncCount.set(0);
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } catch (CommonFSUtils.StreamLacksCapabilityException exception) {
        // If the underlying FileSystem can't do what we ask, treat as IO failure, so
        // we'll abort.
        throw new IOException(
          "Underlying FileSystem can't meet stream requirements. See RS log " + "for details.",
          exception);
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  @Override
  public Map<byte[], List<byte[]>> rollWriter(boolean force) throws IOException {
    return TraceUtil.trace(() -> rollWriterInternal(force), () -> createSpan("WAL.rollWriter"));
  }

  // public only until class moves to o.a.h.h.wal
  /** Returns the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  // public only until class moves to o.a.h.h.wal
  public void requestLogRoll() {
    requestLogRoll(ERROR);
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  FileStatus[] getFiles() throws IOException {
    return CommonFSUtils.listStatus(fs, walDir, ourFiles);
  }

  @Override
  public void shutdown() throws IOException {
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }
    closed = true;
    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }

    ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WAL-Shutdown-%d").build());

    Future<Void> future = shutdownExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        if (rollWriterLock.tryLock(walShutdownTimeout, TimeUnit.SECONDS)) {
          try {
            doShutdown();
            if (syncFutureCache != null) {
              syncFutureCache.clear();
            }
          } finally {
            rollWriterLock.unlock();
          }
        } else {
          throw new IOException("Waiting for rollWriterLock timeout");
        }
        return null;
      }
    });
    shutdownExecutor.shutdown();

    try {
      future.get(walShutdownTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for shutdown WAL");
    } catch (TimeoutException e) {
      throw new TimeoutIOException("We have waited " + walShutdownTimeout + "ms, but"
        + " the shutdown of WAL doesn't complete! Please check the status of underlying "
        + "filesystem or increase the wait time by the config \"" + WAL_SHUTDOWN_WAIT_TIMEOUT_MS
        + "\"", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    } finally {
      // in shutdown, we may call cleanOldLogs so shutdown this executor in the end.
      // In sync replication implementation, we may shut down a WAL without shutting down the whole
      // region server, if we shut down this executor earlier we may get reject execution exception
      // and abort the region server
      logArchiveExecutor.shutdown();
    }
    // we also need to wait logArchive to finish if we want to a graceful shutdown as we may still
    // have some pending archiving tasks not finished yet, and in close we may archive all the
    // remaining WAL files, there could be race if we do not wait for the background archive task
    // finish
    try {
      if (!logArchiveExecutor.awaitTermination(walShutdownTimeout, TimeUnit.MILLISECONDS)) {
        throw new TimeoutIOException("We have waited " + walShutdownTimeout + "ms, but"
          + " the shutdown of WAL doesn't complete! Please check the status of underlying "
          + "filesystem or increase the wait time by the config \"" + WAL_SHUTDOWN_WAIT_TIMEOUT_MS
          + "\"");
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for shutdown WAL");
    }
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.walArchiveDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(file.getPath(), p);
          }
        }

        if (!CommonFSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(file.getPath(), p);
          }
        }
      }
      LOG.debug(
        "Moved " + files.length + " WAL file(s) to " + CommonFSUtils.getPath(this.walArchiveDir));
    }
    LOG.info("Closed WAL: " + toString());
  }

  /** Returns number of WALs currently in the process of closing. */
  public int getInflightWALCloseCount() {
    return inflightWALClosures.size();
  }

  /**
   * updates the sequence number of a specific store. depending on the flag: replaces current seq
   * number if the given seq id is bigger, or even if it is lower than existing one
   */
  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid,
    boolean onlyIfGreater) {
    sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, onlyIfGreater);
  }

  protected final SyncFuture getSyncFuture(long sequence, boolean forceSync) {
    return syncFutureCache.getIfPresentOrNew().reset(sequence, forceSync);
  }

  protected boolean isLogRollRequested() {
    return rollRequested.get();
  }

  protected final void requestLogRoll(final WALActionsListener.RollRequestReason reason) {
    // If we have already requested a roll, don't do it again
    // And only set rollRequested to true when there is a registered listener
    if (!this.listeners.isEmpty() && rollRequested.compareAndSet(false, true)) {
      for (WALActionsListener i : this.listeners) {
        i.logRollRequested(reason);
      }
    }
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedTxid.get();
    long highestUnsynced = this.highestUnsyncedTxid;
    return highestSynced >= highestUnsynced ? 0 : highestUnsynced - highestSynced;
  }

  boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  /**
   * Exposed for testing only. Use to tricks like halt the ring buffer appending.
   */
  protected void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  protected final boolean appendEntry(W writer, FSWALEntry entry) throws IOException {
    // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
    atHeadOfRingBufferEventHandlerAppend();
    long start = EnvironmentEdgeManager.currentTime();
    byte[] encodedRegionName = entry.getKey().getEncodedRegionName();
    long regionSequenceId = entry.getKey().getSequenceId();

    // Edits are empty, there is nothing to append. Maybe empty when we are looking for a
    // region sequence id only, a region edit/sequence id that is not associated with an actual
    // edit. It has to go through all the rigmarole to be sure we have the right ordering.
    if (entry.getEdit().isEmpty()) {
      return false;
    }

    // Coprocessor hook.
    coprocessorHost.preWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    if (!listeners.isEmpty()) {
      for (WALActionsListener i : listeners) {
        i.visitLogEntryBeforeWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
      }
    }
    doAppend(writer, entry);
    assert highestUnsyncedTxid < entry.getTxid();
    highestUnsyncedTxid = entry.getTxid();
    if (entry.isCloseRegion()) {
      // let's clean all the records of this region
      sequenceIdAccounting.onRegionClose(encodedRegionName);
    } else {
      sequenceIdAccounting.update(encodedRegionName, entry.getFamilyNames(), regionSequenceId,
        entry.isInMemStore());
    }
    coprocessorHost.postWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    // Update metrics.
    postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
    numEntries.incrementAndGet();
    return true;
  }

  private long postAppend(final Entry e, final long elapsedTime) throws IOException {
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }

  protected final void postSync(long timeInNanos, int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg = new StringBuilder().append("Slow sync cost: ")
        .append(TimeUnit.NANOSECONDS.toMillis(timeInNanos)).append(" ms, current pipeline: ")
        .append(Arrays.toString(getPipeline())).toString();
      LOG.info(msg);
      if (timeInNanos > this.rollOnSyncNs) {
        // A single sync took too long.
        // Elsewhere in checkSlowSync, called from checkLogRoll, we will look at cumulative
        // effects. Here we have a single data point that indicates we should take immediate
        // action, so do so.
        LOG.warn("Requesting log roll because we exceeded slow sync threshold; time="
          + TimeUnit.NANOSECONDS.toMillis(timeInNanos) + " ms, threshold="
          + TimeUnit.NANOSECONDS.toMillis(rollOnSyncNs) + " ms, current pipeline: "
          + Arrays.toString(getPipeline()));
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

  protected final long stampSequenceIdAndPublishToRingBuffer(RegionInfo hri, WALKeyImpl key,
    WALEdit edits, boolean inMemstore, RingBuffer<RingBufferTruck> ringBuffer) throws IOException {
    if (this.closed) {
      throw new IOException(
        "Cannot append; log is closed, regionName = " + hri.getRegionNameAsString());
    }
    MutableLong txidHolder = new MutableLong();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(() -> {
      txidHolder.setValue(ringBuffer.next());
    });
    long txid = txidHolder.longValue();
    ServerCall<?> rpcCall = RpcServer.getCurrentServerCallWithCellScanner().orElse(null);
    try {
      FSWALEntry entry = new FSWALEntry(txid, key, edits, hri, inMemstore, rpcCall);
      entry.stampRegionSequenceId(we);
      ringBuffer.get(txid).load(entry);
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  @Override
  public String toString() {
    return implClassName + " " + walFilePrefix + ":" + walFileSuffix + "(num " + filenum + ")";
  }

  /**
   * if the given {@code path} is being written currently, then return its length.
   * <p>
   * This is used by replication to prevent replicating unacked log entries. See
   * https://issues.apache.org/jira/browse/HBASE-14004 for more details.
   */
  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(Path path) {
    rollWriterLock.lock();
    try {
      Path currentPath = getOldPath();
      if (path.equals(currentPath)) {
        // Currently active path.
        W writer = this.writer;
        return writer != null ? OptionalLong.of(writer.getSyncedLength()) : OptionalLong.empty();
      } else {
        W temp = inflightWALClosures.get(path.getName());
        if (temp != null) {
          // In the process of being closed, trailer bytes may or may not be flushed.
          // Ensuring that we read all the bytes in a file is critical for correctness of tailing
          // use cases like replication, see HBASE-25924/HBASE-25932.
          return OptionalLong.of(temp.getSyncedLength());
        }
        // Log rolled successfully.
        return OptionalLong.empty();
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  @Override
  public long appendData(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException {
    return TraceUtil.trace(() -> append(info, key, edits, true),
      () -> createSpan("WAL.appendData"));
  }

  @Override
  public long appendMarker(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException {
    return TraceUtil.trace(() -> append(info, key, edits, false),
      () -> createSpan("WAL.appendMarker"));
  }

  /**
   * Helper that marks the future as DONE and offers it back to the cache.
   */
  protected void markFutureDoneAndOffer(SyncFuture future, long txid, Throwable t) {
    future.done(txid, t);
    syncFutureCache.offer(future);
  }

  private static boolean waitingRoll(int epochAndState) {
    return (epochAndState & 1) != 0;
  }

  private static boolean writerBroken(int epochAndState) {
    return ((epochAndState >>> 1) & 1) != 0;
  }

  private static int epoch(int epochAndState) {
    return epochAndState >>> 2;
  }

  // return whether we have successfully set readyForRolling to true.
  private boolean trySetReadyForRolling() {
    // Check without holding lock first. Usually we will just return here.
    // waitingRoll is volatile and unacedEntries is only accessed inside event loop, so it is safe
    // to check them outside the consumeLock.
    if (!waitingRoll(epochAndState) || !unackedAppends.isEmpty()) {
      return false;
    }
    consumeLock.lock();
    try {
      // 1. a roll is requested
      // 2. all out-going entries have been acked(we have confirmed above).
      if (waitingRoll(epochAndState)) {
        readyForRolling = true;
        readyForRollingCond.signalAll();
        return true;
      } else {
        return false;
      }
    } finally {
      consumeLock.unlock();
    }
  }

  private void syncFailed(long epochWhenSync, Throwable error) {
    LOG.warn("sync failed", error);
    this.onException(epochWhenSync, error);
  }

  private void onException(long epochWhenSync, Throwable error) {
    boolean shouldRequestLogRoll = true;
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (epoch(currentEpochAndState) != epochWhenSync || writerBroken(currentEpochAndState)) {
        // this is not the previous writer which means we have already rolled the writer.
        // or this is still the current writer, but we have already marked it as broken and request
        // a roll.
        return;
      }
      this.epochAndState = currentEpochAndState | 0b10;
      if (waitingRoll(currentEpochAndState)) {
        readyForRolling = true;
        readyForRollingCond.signalAll();
        // this means we have already in the middle of a rollWriter so just tell the roller thread
        // that you can continue without requesting an extra log roll.
        shouldRequestLogRoll = false;
      }
    } finally {
      consumeLock.unlock();
    }
    for (Iterator<FSWALEntry> iter = unackedAppends.descendingIterator(); iter.hasNext();) {
      toWriteAppends.addFirst(iter.next());
    }
    highestUnsyncedTxid = highestSyncedTxid.get();
    if (shouldRequestLogRoll) {
      // request a roll.
      requestLogRoll(ERROR);
    }
  }

  private void syncCompleted(long epochWhenSync, W writer, long processedTxid, long startTimeNs) {
    // Please see the last several comments on HBASE-22761, it is possible that we get a
    // syncCompleted which acks a previous sync request after we received a syncFailed on the same
    // writer. So here we will also check on the epoch and state, if the epoch has already been
    // changed, i.e, we have already rolled the writer, or the writer is already broken, we should
    // just skip here, to avoid mess up the state or accidentally release some WAL entries and
    // cause data corruption.
    // The syncCompleted call is on the critical write path, so we should try our best to make it
    // fast. So here we do not hold consumeLock, for increasing performance. It is safe because
    // there are only 3 possible situations:
    // 1. For normal case, the only place where we change epochAndState is when rolling the writer.
    // Before rolling actually happen, we will only change the state to waitingRoll which is another
    // bit than writerBroken, and when we actually change the epoch, we can make sure that there is
    // no outgoing sync request. So we will always pass the check here and there is no problem.
    // 2. The writer is broken, but we have not called syncFailed yet. In this case, since
    // syncFailed and syncCompleted are executed in the same thread, we will just face the same
    // situation with #1.
    // 3. The writer is broken, and syncFailed has been called. Then when we arrive here, there are
    // only 2 possible situations:
    // a. we arrive before we actually roll the writer, then we will find out the writer is broken
    // and give up.
    // b. we arrive after we actually roll the writer, then we will find out the epoch is changed
    // and give up.
    // For both #a and #b, we do not need to hold the consumeLock as we will always update the
    // epochAndState as a whole.
    // So in general, for all the cases above, we do not need to hold the consumeLock.
    int epochAndState = this.epochAndState;
    if (epoch(epochAndState) != epochWhenSync || writerBroken(epochAndState)) {
      LOG.warn("Got a sync complete call after the writer is broken, skip");
      return;
    }

    if (processedTxid < highestSyncedTxid.get()) {
      return;
    }
    highestSyncedTxid.set(processedTxid);
    for (Iterator<FSWALEntry> iter = unackedAppends.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      if (entry.getTxid() <= processedTxid) {
        entry.release();
        iter.remove();
      } else {
        break;
      }
    }
    postSync(System.nanoTime() - startTimeNs, finishSync());
    /**
     * This method is used to be compatible with the original logic of {@link FSHLog}.
     */
    checkSlowSyncCount();
    if (trySetReadyForRolling()) {
      // we have just finished a roll, then do not need to check for log rolling, the writer will be
      // closed soon.
      return;
    }
    // If we haven't already requested a roll, check if we have exceeded logrollsize
    if (!isLogRollRequested() && writer.getLength() > logrollsize) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Requesting log roll because of file size threshold; length=" + writer.getLength()
          + ", logrollsize=" + logrollsize);
      }
      requestLogRoll(SIZE);
    }
  }

  // find all the sync futures between these two txids to see if we need to issue a hsync, if no
  // sync futures then just use the default one.
  private boolean isHsync(long beginTxid, long endTxid) {
    SortedSet<SyncFuture> futures = syncFutures.subSet(new SyncFuture().reset(beginTxid, false),
      new SyncFuture().reset(endTxid + 1, false));
    if (futures.isEmpty()) {
      return useHsync;
    }
    for (SyncFuture future : futures) {
      if (future.isForceSync()) {
        return true;
      }
    }
    return false;
  }

  private void sync(W writer) {
    fileLengthAtLastSync = writer.getLength();
    long currentHighestProcessedAppendTxid = highestProcessedAppendTxid;
    boolean shouldUseHsync =
      isHsync(highestProcessedAppendTxidAtLastSync, currentHighestProcessedAppendTxid);
    highestProcessedAppendTxidAtLastSync = currentHighestProcessedAppendTxid;
    final long startTimeNs = System.nanoTime();
    final long epoch = (long) epochAndState >>> 2L;
    addListener(doWriterSync(writer, shouldUseHsync, currentHighestProcessedAppendTxid),
      (result, error) -> {
        if (error != null) {
          syncFailed(epoch, error);
        } else {
          long syncedTxid = getSyncedTxid(currentHighestProcessedAppendTxid, result);
          syncCompleted(epoch, writer, syncedTxid, startTimeNs);
        }
      }, consumeExecutor);
  }

  /**
   * This method is to adapt {@link FSHLog} and {@link AsyncFSWAL}. For {@link AsyncFSWAL}, we use
   * {@link AbstractFSWAL#highestProcessedAppendTxid} at the point we calling
   * {@link AsyncFSWAL#doWriterSync} method as successful syncedTxid. For {@link FSHLog}, because we
   * use multi-thread {@code SyncRunner}s, we used the result of {@link CompletableFuture} as
   * successful syncedTxid.
   */
  protected long getSyncedTxid(long processedTxid, long completableFutureResult) {
    return processedTxid;
  }

  protected abstract CompletableFuture<Long> doWriterSync(W writer, boolean shouldUseHsync,
    long txidWhenSyn);

  private int finishSyncLowerThanTxid(long txid) {
    int finished = 0;
    for (Iterator<SyncFuture> iter = syncFutures.iterator(); iter.hasNext();) {
      SyncFuture sync = iter.next();
      if (sync.getTxid() <= txid) {
        markFutureDoneAndOffer(sync, txid, null);
        iter.remove();
        finished++;
      } else {
        break;
      }
    }
    return finished;
  }

  // try advancing the highestSyncedTxid as much as possible
  private int finishSync() {
    if (unackedAppends.isEmpty()) {
      // All outstanding appends have been acked.
      if (toWriteAppends.isEmpty()) {
        // Also no appends that wait to be written out, then just finished all pending syncs.
        long maxSyncTxid = highestSyncedTxid.get();
        for (SyncFuture sync : syncFutures) {
          maxSyncTxid = Math.max(maxSyncTxid, sync.getTxid());
          markFutureDoneAndOffer(sync, maxSyncTxid, null);
        }
        highestSyncedTxid.set(maxSyncTxid);
        int finished = syncFutures.size();
        syncFutures.clear();
        return finished;
      } else {
        // There is no append between highestProcessedAppendTxid and lowestUnprocessedAppendTxid, so
        // if highestSyncedTxid >= highestProcessedAppendTxid, then all syncs whose txid are between
        // highestProcessedAppendTxid and lowestUnprocessedAppendTxid can be finished.
        long lowestUnprocessedAppendTxid = toWriteAppends.peek().getTxid();
        assert lowestUnprocessedAppendTxid > highestProcessedAppendTxid;
        long doneTxid = lowestUnprocessedAppendTxid - 1;
        highestSyncedTxid.set(doneTxid);
        return finishSyncLowerThanTxid(doneTxid);
      }
    } else {
      // There are still unacked appends. So let's move the highestSyncedTxid to the txid of the
      // first unacked append minus 1.
      long lowestUnackedAppendTxid = unackedAppends.peek().getTxid();
      long doneTxid = Math.max(lowestUnackedAppendTxid - 1, highestSyncedTxid.get());
      highestSyncedTxid.set(doneTxid);
      return finishSyncLowerThanTxid(doneTxid);
    }
  }

  // confirm non-empty before calling
  private static long getLastTxid(Deque<FSWALEntry> queue) {
    return queue.peekLast().getTxid();
  }

  private void appendAndSync() throws IOException {
    final W writer = this.writer;
    // maybe a sync request is not queued when we issue a sync, so check here to see if we could
    // finish some.
    finishSync();
    long newHighestProcessedAppendTxid = -1L;
    // this is used to avoid calling peedLast every time on unackedAppends, appendAndAsync is single
    // threaded, this could save us some cycles
    boolean addedToUnackedAppends = false;
    for (Iterator<FSWALEntry> iter = toWriteAppends.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      /**
       * For {@link FSHog},here may throw IOException,but for {@link AsyncFSWAL}, here would not
       * throw any IOException.
       */
      boolean appended = appendEntry(writer, entry);
      newHighestProcessedAppendTxid = entry.getTxid();
      iter.remove();
      if (appended) {
        // This is possible, when we fail to sync, we will add the unackedAppends back to
        // toWriteAppends, so here we may get an entry which is already in the unackedAppends.
        if (
          addedToUnackedAppends || unackedAppends.isEmpty()
            || getLastTxid(unackedAppends) < entry.getTxid()
        ) {
          unackedAppends.addLast(entry);
          addedToUnackedAppends = true;
        }
        // See HBASE-25905, here we need to make sure that, we will always write all the entries in
        // unackedAppends out. As the code in the consume method will assume that, the entries in
        // unackedAppends have all been sent out so if there is roll request and unackedAppends is
        // not empty, we could just return as later there will be a syncCompleted call to clear the
        // unackedAppends, or a syncFailed to lead us to another state.
        // There could be other ways to fix, such as changing the logic in the consume method, but
        // it will break the assumption and then (may) lead to a big refactoring. So here let's use
        // this way to fix first, can optimize later.
        if (
          writer.getLength() - fileLengthAtLastSync >= batchSize
            && (addedToUnackedAppends || entry.getTxid() >= getLastTxid(unackedAppends))
        ) {
          break;
        }
      }
    }
    // if we have a newer transaction id, update it.
    // otherwise, use the previous transaction id.
    if (newHighestProcessedAppendTxid > 0) {
      highestProcessedAppendTxid = newHighestProcessedAppendTxid;
    } else {
      newHighestProcessedAppendTxid = highestProcessedAppendTxid;
    }

    if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
      // sync because buffer size limit.
      sync(writer);
      return;
    }
    if (writer.getLength() == fileLengthAtLastSync) {
      // we haven't written anything out, just advance the highestSyncedSequence since we may only
      // stamp some region sequence id.
      if (unackedAppends.isEmpty()) {
        highestSyncedTxid.set(highestProcessedAppendTxid);
        finishSync();
        trySetReadyForRolling();
      }
      return;
    }
    // reach here means that we have some unsynced data but haven't reached the batch size yet,
    // but we will not issue a sync directly here even if there are sync requests because we may
    // have some new data in the ringbuffer, so let's just return here and delay the decision of
    // whether to issue a sync in the caller method.
  }

  private void consume() {
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (writerBroken(currentEpochAndState)) {
        return;
      }
      if (waitingRoll(currentEpochAndState)) {
        if (writer.getLength() > fileLengthAtLastSync) {
          // issue a sync
          sync(writer);
        } else {
          if (unackedAppends.isEmpty()) {
            readyForRolling = true;
            readyForRollingCond.signalAll();
          }
        }
        return;
      }
    } finally {
      consumeLock.unlock();
    }
    long nextCursor = waitingConsumePayloadsGatingSequence.get() + 1;
    for (long cursorBound = waitingConsumePayloads.getCursor(); nextCursor
        <= cursorBound; nextCursor++) {
      if (!waitingConsumePayloads.isPublished(nextCursor)) {
        break;
      }
      RingBufferTruck truck = waitingConsumePayloads.get(nextCursor);
      switch (truck.type()) {
        case APPEND:
          toWriteAppends.addLast(truck.unloadAppend());
          break;
        case SYNC:
          syncFutures.add(truck.unloadSync());
          break;
        default:
          LOG.warn("RingBufferTruck with unexpected type: " + truck.type());
          break;
      }
      waitingConsumePayloadsGatingSequence.set(nextCursor);
    }

    /**
     * This method is used to be compatible with the original logic of {@link AsyncFSWAL}.
     */
    if (markerEditOnly) {
      drainNonMarkerEditsAndFailSyncs();
    }
    try {
      appendAndSync();
    } catch (IOException exception) {
      /**
       * For {@link FSHog},here may catch IOException,but for {@link AsyncFSWAL}, the code doesn't
       * go in here.
       */
      LOG.error("appendAndSync throws IOException.", exception);
      onAppendEntryFailed(exception);
      return;
    }
    if (hasConsumerTask.get()) {
      return;
    }
    if (toWriteAppends.isEmpty()) {
      if (waitingConsumePayloadsGatingSequence.get() == waitingConsumePayloads.getCursor()) {
        consumerScheduled.set(false);
        // recheck here since in append and sync we do not hold the consumeLock. Thing may
        // happen like
        // 1. we check cursor, no new entry
        // 2. someone publishes a new entry to ringbuffer and the consumerScheduled is true and
        // give up scheduling the consumer task.
        // 3. we set consumerScheduled to false and also give up scheduling consumer task.
        if (waitingConsumePayloadsGatingSequence.get() == waitingConsumePayloads.getCursor()) {
          // we will give up consuming so if there are some unsynced data we need to issue a sync.
          if (
            writer.getLength() > fileLengthAtLastSync && !syncFutures.isEmpty()
              && syncFutures.last().getTxid() > highestProcessedAppendTxidAtLastSync
          ) {
            // no new data in the ringbuffer and we have at least one sync request
            sync(writer);
          }
          return;
        } else {
          // maybe someone has grabbed this before us
          if (!consumerScheduled.compareAndSet(false, true)) {
            return;
          }
        }
      }
    }
    // reschedule if we still have something to write.
    consumeExecutor.execute(consumer);
  }

  private boolean shouldScheduleConsumer() {
    int currentEpochAndState = epochAndState;
    if (writerBroken(currentEpochAndState) || waitingRoll(currentEpochAndState)) {
      return false;
    }
    return consumerScheduled.compareAndSet(false, true);
  }

  /**
   * Append a set of edits to the WAL.
   * <p/>
   * The WAL is not flushed/sync'd after this transaction completes BUT on return this edit must
   * have its region edit/sequence id assigned else it messes up our unification of mvcc and
   * sequenceid. On return <code>key</code> will have the region edit/sequence id filled in.
   * <p/>
   * NOTE: This appends, at a time that is usually after this call returns, starts a mvcc
   * transaction by calling 'begin' wherein which we assign this update a sequenceid. At assignment
   * time, we stamp all the passed in Cells inside WALEdit with their sequenceId. You must
   * 'complete' the transaction this mvcc transaction by calling
   * MultiVersionConcurrencyControl#complete(...) or a variant otherwise mvcc will get stuck. Do it
   * in the finally of a try/finally block within which this appends lives and any subsequent
   * operations like sync or update of memstore, etc. Get the WriteEntry to pass mvcc out of the
   * passed in WALKey <code>walKey</code> parameter. Be warned that the WriteEntry is not
   * immediately available on return from this method. It WILL be available subsequent to a sync of
   * this append; otherwise, you will just have to wait on the WriteEntry to get filled in.
   * @param hri        the regioninfo associated with append
   * @param key        Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits      Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   *                   sequence id that is after all currently appended edits.
   * @param inMemstore Always true except for case where we are writing a region event meta marker
   *                   edit, for example, a compaction completion record into the WAL or noting a
   *                   Region Open event. In these cases the entry is just so we can finish an
   *                   unfinished compaction after a crash when the new Server reads the WAL on
   *                   recovery, etc. These transition event 'Markers' do not go via the memstore.
   *                   When memstore is false, we presume a Marker event edit.
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   *         in it.
   */
  protected long append(RegionInfo hri, WALKeyImpl key, WALEdit edits, boolean inMemstore)
    throws IOException {
    if (markerEditOnly && !edits.isMetaEdit()) {
      throw new IOException("WAL is closing, only marker edit is allowed");
    }
    long txid =
      stampSequenceIdAndPublishToRingBuffer(hri, key, edits, inMemstore, waitingConsumePayloads);
    if (shouldScheduleConsumer()) {
      consumeExecutor.execute(consumer);
    }
    return txid;
  }

  protected void doSync(boolean forceSync) throws IOException {
    long txid = waitingConsumePayloads.next();
    SyncFuture future;
    try {
      future = getSyncFuture(txid, forceSync);
      RingBufferTruck truck = waitingConsumePayloads.get(txid);
      truck.load(future);
    } finally {
      waitingConsumePayloads.publish(txid);
    }
    if (shouldScheduleConsumer()) {
      consumeExecutor.execute(consumer);
    }
    blockOnSync(future);
  }

  protected void doSync(long txid, boolean forceSync) throws IOException {
    if (highestSyncedTxid.get() >= txid) {
      return;
    }
    // here we do not use ring buffer sequence as txid
    long sequence = waitingConsumePayloads.next();
    SyncFuture future;
    try {
      future = getSyncFuture(txid, forceSync);
      RingBufferTruck truck = waitingConsumePayloads.get(sequence);
      truck.load(future);
    } finally {
      waitingConsumePayloads.publish(sequence);
    }
    if (shouldScheduleConsumer()) {
      consumeExecutor.execute(consumer);
    }
    blockOnSync(future);
  }

  private void drainNonMarkerEditsAndFailSyncs() {
    if (toWriteAppends.isEmpty()) {
      return;
    }
    boolean hasNonMarkerEdits = false;
    Iterator<FSWALEntry> iter = toWriteAppends.descendingIterator();
    while (iter.hasNext()) {
      FSWALEntry entry = iter.next();
      if (!entry.getEdit().isMetaEdit()) {
        entry.release();
        hasNonMarkerEdits = true;
        break;
      }
    }
    if (hasNonMarkerEdits) {
      for (;;) {
        iter.remove();
        if (!iter.hasNext()) {
          break;
        }
        iter.next().release();
      }
      for (FSWALEntry entry : unackedAppends) {
        entry.release();
      }
      unackedAppends.clear();
      // fail the sync futures which are under the txid of the first remaining edit, if none, fail
      // all the sync futures.
      long txid = toWriteAppends.isEmpty() ? Long.MAX_VALUE : toWriteAppends.peek().getTxid();
      IOException error = new IOException("WAL is closing, only marker edit is allowed");
      for (Iterator<SyncFuture> syncIter = syncFutures.iterator(); syncIter.hasNext();) {
        SyncFuture future = syncIter.next();
        if (future.getTxid() < txid) {
          markFutureDoneAndOffer(future, future.getTxid(), error);
          syncIter.remove();
        } else {
          break;
        }
      }
    }
  }

  protected abstract W createWriterInstance(FileSystem fs, Path path)
    throws IOException, CommonFSUtils.StreamLacksCapabilityException;

  protected abstract W createCombinedWriter(W localWriter, W remoteWriter);

  protected final void waitForSafePoint() {
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (writerBroken(currentEpochAndState) || this.writer == null) {
        return;
      }
      consumerScheduled.set(true);
      epochAndState = currentEpochAndState | 1;
      readyForRolling = false;
      consumeExecutor.execute(consumer);
      while (!readyForRolling) {
        readyForRollingCond.awaitUninterruptibly();
      }
    } finally {
      consumeLock.unlock();
    }
  }

  private void recoverLease(FileSystem fs, Path p, Configuration conf) {
    try {
      RecoverLeaseFSUtils.recoverFileLease(fs, p, conf, null);
    } catch (IOException ex) {
      LOG.error("Unable to recover lease after several attempts. Give up.", ex);
    }
  }

  protected final void closeWriter(W writer, Path path) {
    inflightWALClosures.put(path.getName(), writer);
    closeExecutor.execute(() -> {
      try {
        writer.close();
      } catch (IOException e) {
        LOG.warn("close old writer failed.", e);
        recoverLease(this.fs, path, conf);
      } finally {
        // call this even if the above close fails, as there is no other chance we can set closed to
        // true, it will not cause big problems.
        markClosedAndClean(path);
        inflightWALClosures.remove(path.getName());
      }
    });
  }

  /**
   * Notice that you need to clear the {@link #rollRequested} flag in this method, as the new writer
   * will begin to work before returning from this method. If we clear the flag after returning from
   * this call, we may miss a roll request. The implementation class should choose a proper place to
   * clear the {@link #rollRequested} flag, so we do not miss a roll request, typically before you
   * start writing to the new writer.
   */
  protected void doReplaceWriter(Path oldPath, Path newPath, W nextWriter) throws IOException {
    Preconditions.checkNotNull(nextWriter);
    waitForSafePoint();
    /**
     * For {@link FSHLog},here would shut down {@link FSHLog.SyncRunner}.
     */
    doCleanUpResources();
    // we will call rollWriter in init method, where we want to create the first writer and
    // obviously the previous writer is null, so here we need this null check. And why we must call
    // logRollAndSetupWalProps before closeWriter is that, we will call markClosedAndClean after
    // closing the writer asynchronously, we need to make sure the WALProps is put into
    // walFile2Props before we call markClosedAndClean
    if (writer != null) {
      long oldFileLen = writer.getLength();
      logRollAndSetupWalProps(oldPath, newPath, oldFileLen);
      closeWriter(writer, oldPath);
    } else {
      logRollAndSetupWalProps(oldPath, newPath, 0);
    }
    this.writer = nextWriter;
    /**
     * Here is used for {@link AsyncFSWAL} and {@link FSHLog} to set the under layer filesystem
     * output after writer is replaced.
     */
    onWriterReplaced(nextWriter);
    this.fileLengthAtLastSync = nextWriter.getLength();
    this.highestProcessedAppendTxidAtLastSync = 0L;
    consumeLock.lock();
    try {
      consumerScheduled.set(true);
      int currentEpoch = epochAndState >>> 2;
      int nextEpoch = currentEpoch == MAX_EPOCH ? 0 : currentEpoch + 1;
      // set a new epoch and also clear waitingRoll and writerBroken
      this.epochAndState = nextEpoch << 2;
      // Reset rollRequested status
      rollRequested.set(false);
      consumeExecutor.execute(consumer);
    } finally {
      consumeLock.unlock();
    }
  }

  protected abstract void onWriterReplaced(W nextWriter);

  protected void doShutdown() throws IOException {
    waitForSafePoint();
    /**
     * For {@link FSHLog},here would shut down {@link FSHLog.SyncRunner}.
     */
    doCleanUpResources();
    if (this.writer != null) {
      closeWriter(this.writer, getOldPath());
      this.writer = null;
    }
    closeExecutor.shutdown();
    try {
      if (!closeExecutor.awaitTermination(waitOnShutdownInSeconds, TimeUnit.SECONDS)) {
        LOG.error("We have waited " + waitOnShutdownInSeconds + " seconds but"
          + " the close of async writer doesn't complete."
          + "Please check the status of underlying filesystem"
          + " or increase the wait time by the config \"" + this.waitOnShutdownInSecondsConfigKey
          + "\"");
      }
    } catch (InterruptedException e) {
      LOG.error("The wait for close of async writer is interrupted");
      Thread.currentThread().interrupt();
    }
    IOException error = new IOException("WAL has been closed");
    long nextCursor = waitingConsumePayloadsGatingSequence.get() + 1;
    // drain all the pending sync requests
    for (long cursorBound = waitingConsumePayloads.getCursor(); nextCursor
        <= cursorBound; nextCursor++) {
      if (!waitingConsumePayloads.isPublished(nextCursor)) {
        break;
      }
      RingBufferTruck truck = waitingConsumePayloads.get(nextCursor);
      switch (truck.type()) {
        case SYNC:
          syncFutures.add(truck.unloadSync());
          break;
        default:
          break;
      }
    }
    // and fail them
    syncFutures.forEach(f -> markFutureDoneAndOffer(f, f.getTxid(), error));
    if (this.shouldShutDownConsumeExecutorWhenClose) {
      consumeExecutor.shutdown();
    }
  }

  protected void doCleanUpResources() {
  };

  protected abstract void doAppend(W writer, FSWALEntry entry) throws IOException;

  /**
   * This method gets the pipeline for the current WAL.
   */
  abstract DatanodeInfo[] getPipeline();

  /**
   * This method gets the datanode replication count for the current WAL.
   */
  abstract int getLogReplication();

  protected abstract boolean doCheckLogLowReplication();

  protected boolean isWriterBroken() {
    return writerBroken(epochAndState);
  }

  private void onAppendEntryFailed(IOException exception) {
    LOG.warn("append entry failed", exception);
    final long currentEpoch = (long) epochAndState >>> 2L;
    this.onException(currentEpoch, exception);
  }

  protected void checkSlowSyncCount() {
  }

  /** Returns true if we exceeded the slow sync roll threshold over the last check interval */
  protected boolean doCheckSlowSync() {
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
            LOG.debug("checkSlowSync triggered but we decided to ignore it; " + "count="
              + slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold + ", elapsedTime="
              + elapsedTime + " ms, slowSyncCheckInterval=" + slowSyncCheckInterval + " ms");
          }
          // Fall through to count reset below
        } else {
          LOG.warn("Requesting log roll because we exceeded slow sync threshold; count="
            + slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold + ", current pipeline: "
            + Arrays.toString(getPipeline()));
          result = true;
        }
      }
      lastTimeCheckSlowSync = now;
      slowSyncCount.set(0);
    }
    return result;
  }

  public void checkLogLowReplication(long checkInterval) {
    long now = EnvironmentEdgeManager.currentTime();
    if (now - lastTimeCheckLowReplication < checkInterval) {
      return;
    }
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      lastTimeCheckLowReplication = now;
      if (doCheckLogLowReplication()) {
        requestLogRoll(LOW_REPLICATION);
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  // Allow temporarily skipping the creation of remote writer. When failing to write to the remote
  // dfs cluster, we need to reopen the regions and switch to use the original wal writer. But we
  // need to write a close marker when closing a region, and if it fails, the whole rs will abort.
  // So here we need to skip the creation of remote writer and make it possible to write the region
  // close marker.
  // Setting markerEdit only to true is for transiting from A to S, where we need to give up writing
  // any pending wal entries as they will be discarded. The remote cluster will replicate the
  // correct data back later. We still need to allow writing marker edits such as close region event
  // to allow closing a region.
  @Override
  public void skipRemoteWAL(boolean markerEditOnly) {
    if (markerEditOnly) {
      this.markerEditOnly = true;
    }
    this.skipRemoteWAL = true;
  }

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = CommonFSUtils.getWALFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = CommonFSUtils.getWALRootDir(conf);
    Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (
      conf.getBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR,
        AbstractFSWALProvider.DEFAULT_SEPARATE_OLDLOGDIR)
    ) {
      archiveDir = new Path(archiveDir, p.getName());
    }
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }

  W getWriter() {
    return this.writer;
  }

  private static void usage() {
    System.err.println("Usage: AbstractFSWAL <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: "
      + "AbstractFSWAL --dump hdfs://example.com:9000/hbase/WALs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println(
      "         For example: AbstractFSWAL --split hdfs://example.com:9000/hbase/WALs/DIR");
  }

  /**
   * Pass one or more log file names, and it will either dump out a text version on
   * <code>stdout</code> or split the specified log files.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      ExitHandler.getInstance().exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.error(HBaseMarkers.FATAL, "Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.error(HBaseMarkers.FATAL,
        "\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " + args[1]);
      ExitHandler.getInstance().exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          CommonFSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          ExitHandler.getInstance().exit(-1);
        }
      }
    } else {
      usage();
      ExitHandler.getInstance().exit(-1);
    }
  }
}
