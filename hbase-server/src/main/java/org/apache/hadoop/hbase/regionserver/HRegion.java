/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.RegionInRecoveryException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter.MutationReplay;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.util.StringUtils;
import org.cliffc.high_scale_lib.Counter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * HRegion stores data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more HRegions.
 *
 * <p>We maintain multiple HStores for a single HRegion.
 *
 * <p>An Store is a set of rows with some column data; together,
 * they make up all the data for the rows.
 *
 * <p>Each HRegion has a 'startKey' and 'endKey'.
 * <p>The first is inclusive, the second is exclusive (except for
 * the final region)  The endKey of region 0 is the same as
 * startKey for region 1 (if it exists).  The startKey for the
 * first region is null. The endKey for the final region is null.
 *
 * <p>Locking at the HRegion level serves only one purpose: preventing the
 * region from being closed (and consequently split) while other operations
 * are ongoing. Each row level operation obtains both a row lock and a region
 * read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully
 * constructed, it holds a read lock until it is closed. A close takes out a
 * write lock and consequently will block for ongoing operations and will block
 * new operations from starting while the close is in progress.
 *
 * <p>An HRegion is defined by its table and its key extent.
 *
 * <p>It consists of at least one Store.  The number of Stores should be
 * configurable, so that data which is accessed together is stored in the same
 * Store.  Right now, we approximate that by building a single Store for
 * each column family.  (This config info will be communicated via the
 * tabledesc.)
 *
 * <p>The HTableDescriptor contains metainfo about the HRegion's table.
 * regionName is a unique identifier for this HRegion. (startKey, endKey]
 * defines the keyspace for this HRegion.
 */
@InterfaceAudience.Private
public class HRegion implements HeapSize { // , Writable{
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  public static final String LOAD_CFS_ON_DEMAND_CONFIG_KEY =
      "hbase.hregion.scan.loadColumnFamiliesOnDemand";

  /**
   * This is the global default value for durability. All tables/mutations not
   * defining a durability or using USE_DEFAULT will default to this value.
   */
  private static final Durability DEFAULT_DURABLITY = Durability.SYNC_WAL;

  final AtomicBoolean closed = new AtomicBoolean(false);
  /* Closing can take some time; use the closing flag if there is stuff we don't
   * want to do while in closing state; e.g. like offer this region up to the
   * master as a region to close if the carrying regionserver is overloaded.
   * Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);

  protected volatile long completeSequenceId = -1L;

  /**
   * Region level sequence Id. It is used for appending WALEdits in HLog. Its default value is -1,
   * as a marker that the region hasn't opened yet. Once it is opened, it is set to
   * {@link #openSeqNum}.
   */
  private final AtomicLong sequenceId = new AtomicLong(-1L);

  /**
   * Operation enum is used in {@link HRegion#startRegionOperation} to provide operation context for
   * startRegionOperation to possibly invoke different checks before any region operations. Not all
   * operations have to be defined here. It's only needed when a special check is need in
   * startRegionOperation
   */
  public enum Operation {
    ANY, GET, PUT, DELETE, SCAN, APPEND, INCREMENT, SPLIT_REGION, MERGE_REGION, BATCH_MUTATE,
    REPLAY_BATCH_MUTATE, COMPACT_REGION
  }

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  // map from a locked row to the context for that lock including:
  // - CountDownLatch for threads waiting on that row
  // - the thread that owns the lock (allow reentrancy)
  // - reference count of (reentrant) locks held by the thread
  // - the row itself
  private final ConcurrentHashMap<HashedBytes, RowLockContext> lockedRows =
      new ConcurrentHashMap<HashedBytes, RowLockContext>();

  protected final Map<byte[], Store> stores = new ConcurrentSkipListMap<byte[], Store>(
      Bytes.BYTES_RAWCOMPARATOR);

  // TODO: account for each registered handler in HeapSize computation
  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  public final AtomicLong memstoreSize = new AtomicLong(0);

  // Debug possible data loss due to WAL off
  final Counter numMutationsWithoutWAL = new Counter();
  final Counter dataInMemoryWithoutWAL = new Counter();

  // Debug why CAS operations are taking a while.
  final Counter checkAndMutateChecksPassed = new Counter();
  final Counter checkAndMutateChecksFailed = new Counter();

  //Number of requests
  final Counter readRequestsCount = new Counter();
  final Counter writeRequestsCount = new Counter();

  // Number of requests blocked by memstore size.
  private final Counter blockedRequestsCount = new Counter();

  /**
   * @return the number of blocked requests count.
   */
  public long getBlockedRequestsCount() {
    return this.blockedRequestsCount.get();
  }

  // Compaction counters
  final AtomicLong compactionsFinished = new AtomicLong(0L);
  final AtomicLong compactionNumFilesCompacted = new AtomicLong(0L);
  final AtomicLong compactionNumBytesCompacted = new AtomicLong(0L);


  private final HLog log;
  private final HRegionFileSystem fs;
  protected final Configuration conf;
  private final Configuration baseConf;
  private final KeyValue.KVComparator comparator;
  private final int rowLockWaitDuration;
  static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

  // The internal wait duration to acquire a lock before read/update
  // from the region. It is not per row. The purpose of this wait time
  // is to avoid waiting a long time while the region is busy, so that
  // we can release the IPC handler soon enough to improve the
  // availability of the region server. It can be adjusted by
  // tuning configuration "hbase.busy.wait.duration".
  final long busyWaitDuration;
  static final long DEFAULT_BUSY_WAIT_DURATION = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;

  // If updating multiple rows in one call, wait longer,
  // i.e. waiting for busyWaitDuration * # of rows. However,
  // we can limit the max multiplier.
  final int maxBusyWaitMultiplier;

  // Max busy wait duration. There is no point to wait longer than the RPC
  // purge timeout, when a RPC call will be terminated by the RPC engine.
  final long maxBusyWaitDuration;

  // negative number indicates infinite timeout
  static final long DEFAULT_ROW_PROCESSOR_TIMEOUT = 60 * 1000L;
  final ExecutorService rowProcessorExecutor = Executors.newCachedThreadPool();

  private final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

  /**
   * The sequence ID that was encountered when this region was opened.
   */
  private long openSeqNum = HConstants.NO_SEQNUM;

  /**
   * The default setting for whether to enable on-demand CF loading for
   * scan requests to this region. Requests can override it.
   */
  private boolean isLoadingCfsOnDemandDefault = false;

  private final AtomicInteger majorInProgress = new AtomicInteger(0);
  private final AtomicInteger minorInProgress = new AtomicInteger(0);

  //
  // Context: During replay we want to ensure that we do not lose any data. So, we
  // have to be conservative in how we replay logs. For each store, we calculate
  // the maxSeqId up to which the store was flushed. And, skip the edits which
  // are equal to or lower than maxSeqId for each store.
  // The following map is populated when opening the region
  Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  /**
   * Config setting for whether to allow writes when a region is in recovering or not.
   */
  private boolean disallowWritesInRecovering = false;

  // when a region is in recovering state, it can only accept writes not reads
  private volatile boolean isRecovering = false;

  /**
   * @return The smallest mvcc readPoint across all the scanners in this
   * region. Writes older than this readPoint, are included  in every
   * read operation.
   */
  public long getSmallestReadPoint() {
    long minimumReadPoint;
    // We need to ensure that while we are calculating the smallestReadPoint
    // no new RegionScanners can grab a readPoint that we are unaware of.
    // We achieve this by synchronizing on the scannerReadPoints object.
    synchronized(scannerReadPoints) {
      minimumReadPoint = mvcc.memstoreReadPoint();

      for (Long readPoint: this.scannerReadPoints.values()) {
        if (readPoint < minimumReadPoint) {
          minimumReadPoint = readPoint;
        }
      }
    }
    return minimumReadPoint;
  }
  /*
   * Data structure of write state flags used coordinating flushes,
   * compactions and closes.
   */
  static class WriteState {
    // Set while a memstore flush is happening.
    volatile boolean flushing = false;
    // Set when a flush has been requested.
    volatile boolean flushRequested = false;
    // Number of compactions running.
    volatile int compacting = 0;
    // Gets set in close. If set, cannot compact or flush again.
    volatile boolean writesEnabled = true;
    // Set if region is read-only
    volatile boolean readOnly = false;

    /**
     * Set flags that make this region read-only.
     *
     * @param onOff flip value for region r/o setting
     */
    synchronized void setReadOnly(final boolean onOff) {
      this.writesEnabled = !onOff;
      this.readOnly = onOff;
    }

    boolean isReadOnly() {
      return this.readOnly;
    }

    boolean isFlushRequested() {
      return this.flushRequested;
    }

    static final long HEAP_SIZE = ClassSize.align(
        ClassSize.OBJECT + 5 * Bytes.SIZEOF_BOOLEAN);
  }

  /**
   * Objects from this class are created when flushing to describe all the different states that
   * that method ends up in. The Result enum describes those states. The sequence id should only
   * be specified if the flush was successful, and the failure message should only be speficied
   * if it didn't flush.
   */
  public static class FlushResult {
    enum Result {
      FLUSHED_NO_COMPACTION_NEEDED,
      FLUSHED_COMPACTION_NEEDED,
      // Special case where a flush didn't run because there's nothing in the memstores. Used when
      // bulk loading to know when we can still load even if a flush didn't happen.
      CANNOT_FLUSH_MEMSTORE_EMPTY,
      CANNOT_FLUSH
      // Be careful adding more to this enum, look at the below methods to make sure
    }

    final Result result;
    final String failureReason;
    final long flushSequenceId;

    /**
     * Convenience constructor to use when the flush is successful, the failure message is set to
     * null.
     * @param result Expecting FLUSHED_NO_COMPACTION_NEEDED or FLUSHED_COMPACTION_NEEDED.
     * @param flushSequenceId Generated sequence id that comes right after the edits in the
     *                        memstores.
     */
    FlushResult(Result result, long flushSequenceId) {
      this(result, flushSequenceId, null);
      assert result == Result.FLUSHED_NO_COMPACTION_NEEDED || result == Result
          .FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience constructor to use when we cannot flush.
     * @param result Expecting CANNOT_FLUSH_MEMSTORE_EMPTY or CANNOT_FLUSH.
     * @param failureReason Reason why we couldn't flush.
     */
    FlushResult(Result result, String failureReason) {
      this(result, -1, failureReason);
      assert result == Result.CANNOT_FLUSH_MEMSTORE_EMPTY || result == Result.CANNOT_FLUSH;
    }

    /**
     * Constructor with all the parameters.
     * @param result Any of the Result.
     * @param flushSequenceId Generated sequence id if the memstores were flushed else -1.
     * @param failureReason Reason why we couldn't flush, or null.
     */
    FlushResult(Result result, long flushSequenceId, String failureReason) {
      this.result = result;
      this.flushSequenceId = flushSequenceId;
      this.failureReason = failureReason;
    }

    /**
     * Convenience method, the equivalent of checking if result is
     * FLUSHED_NO_COMPACTION_NEEDED or FLUSHED_NO_COMPACTION_NEEDED.
     * @return true if the memstores were flushed, else false.
     */
    public boolean isFlushSucceeded() {
      return result == Result.FLUSHED_NO_COMPACTION_NEEDED || result == Result
          .FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience method, the equivalent of checking if result is FLUSHED_COMPACTION_NEEDED.
     * @return True if the flush requested a compaction, else false (doesn't even mean it flushed).
     */
    public boolean isCompactionNeeded() {
      return result == Result.FLUSHED_COMPACTION_NEEDED;
    }
  }

  final WriteState writestate = new WriteState();

  long memstoreFlushSize;
  final long timestampSlop;
  final long rowProcessorTimeout;
  private volatile long lastFlushTime;
  final RegionServerServices rsServices;
  private RegionServerAccounting rsAccounting;
  private List<Pair<Long, Long>> recentFlushes = new ArrayList<Pair<Long,Long>>();
  private long flushCheckInterval;
  // flushPerChanges is to prevent too many changes in memstore
  private long flushPerChanges;
  private long blockingMemStoreSize;
  final long threadWakeFrequency;
  // Used to guard closes
  final ReentrantReadWriteLock lock =
    new ReentrantReadWriteLock();

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock =
    new ReentrantReadWriteLock();
  private boolean splitRequest;
  private byte[] explicitSplitPoint = null;

  private final MultiVersionConsistencyControl mvcc =
      new MultiVersionConsistencyControl();

  // Coprocessor host
  private RegionCoprocessorHost coprocessorHost;

  private HTableDescriptor htableDescriptor = null;
  private RegionSplitPolicy splitPolicy;

  private final MetricsRegion metricsRegion;
  private final MetricsRegionWrapperImpl metricsRegionWrapper;
  private final Durability durability;
  private final boolean regionStatsEnabled;

  /**
   * HRegion constructor. This constructor should only be used for testing and
   * extensions.  Instances of HRegion should be instantiated with the
   * {@link HRegion#createHRegion} or {@link HRegion#openHRegion} method.
   *
   * @param tableDir qualified path of directory where region should be located,
   * usually the table directory.
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.
   * @param confParam is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param htd the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   */
  @Deprecated
  public HRegion(final Path tableDir, final HLog log, final FileSystem fs,
      final Configuration confParam, final HRegionInfo regionInfo,
      final HTableDescriptor htd, final RegionServerServices rsServices) {
    this(new HRegionFileSystem(confParam, fs, tableDir, regionInfo),
      log, confParam, htd, rsServices);
  }

  /**
   * HRegion constructor. This constructor should only be used for testing and
   * extensions.  Instances of HRegion should be instantiated with the
   * {@link HRegion#createHRegion} or {@link HRegion#openHRegion} method.
   *
   * @param fs is the filesystem.
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param confParam is global configuration settings.
   * @param htd the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   */
  public HRegion(final HRegionFileSystem fs, final HLog log, final Configuration confParam,
      final HTableDescriptor htd, final RegionServerServices rsServices) {
    if (htd == null) {
      throw new IllegalArgumentException("Need table descriptor");
    }

    if (confParam instanceof CompoundConfiguration) {
      throw new IllegalArgumentException("Need original base configuration");
    }

    this.comparator = fs.getRegionInfo().getComparator();
    this.log = log;
    this.fs = fs;

    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.baseConf = confParam;
    this.conf = new CompoundConfiguration()
      .add(confParam)
      .addStringMap(htd.getConfiguration())
      .addWritableMap(htd.getValues());
    this.flushCheckInterval = conf.getInt(MEMSTORE_PERIODIC_FLUSH_INTERVAL,
        DEFAULT_CACHE_FLUSH_INTERVAL);
    this.flushPerChanges = conf.getLong(MEMSTORE_FLUSH_PER_CHANGES, DEFAULT_FLUSH_PER_CHANGES);
    if (this.flushPerChanges > MAX_FLUSH_PER_CHANGES) {
      throw new IllegalArgumentException(MEMSTORE_FLUSH_PER_CHANGES + " can not exceed "
          + MAX_FLUSH_PER_CHANGES);
    }

    this.rowLockWaitDuration = conf.getInt("hbase.rowlock.wait.duration",
                    DEFAULT_ROWLOCK_WAIT_DURATION);

    this.isLoadingCfsOnDemandDefault = conf.getBoolean(LOAD_CFS_ON_DEMAND_CONFIG_KEY, true);
    this.htableDescriptor = htd;
    this.rsServices = rsServices;
    this.threadWakeFrequency = conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    setHTableSpecificConf();
    this.scannerReadPoints = new ConcurrentHashMap<RegionScanner, Long>();

    this.busyWaitDuration = conf.getLong(
      "hbase.busy.wait.duration", DEFAULT_BUSY_WAIT_DURATION);
    this.maxBusyWaitMultiplier = conf.getInt("hbase.busy.wait.multiplier.max", 2);
    if (busyWaitDuration * maxBusyWaitMultiplier <= 0L) {
      throw new IllegalArgumentException("Invalid hbase.busy.wait.duration ("
        + busyWaitDuration + ") or hbase.busy.wait.multiplier.max ("
        + maxBusyWaitMultiplier + "). Their product should be positive");
    }
    this.maxBusyWaitDuration = conf.getLong("hbase.ipc.client.call.purge.timeout",
      conf.getLong("ipc.client.call.purge.timeout", 2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT));

    /*
     * timestamp.slop provides a server-side constraint on the timestamp. This
     * assumes that you base your TS around currentTimeMillis(). In this case,
     * throw an error to the user if the user-specified TS is newer than now +
     * slop. LATEST_TIMESTAMP == don't use this functionality
     */
    this.timestampSlop = conf.getLong(
        "hbase.hregion.keyvalue.timestamp.slop.millisecs",
        HConstants.LATEST_TIMESTAMP);

    /**
     * Timeout for the process time in processRowsWithLocks().
     * Use -1 to switch off time bound.
     */
    this.rowProcessorTimeout = conf.getLong(
        "hbase.hregion.row.processor.timeout", DEFAULT_ROW_PROCESSOR_TIMEOUT);
    this.durability = htd.getDurability() == Durability.USE_DEFAULT
        ? DEFAULT_DURABLITY
        : htd.getDurability();
    if (rsServices != null) {
      this.rsAccounting = this.rsServices.getRegionServerAccounting();
      // don't initialize coprocessors if not running within a regionserver
      // TODO: revisit if coprocessors should load in other cases
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
      this.metricsRegionWrapper = new MetricsRegionWrapperImpl(this);
      this.metricsRegion = new MetricsRegion(this.metricsRegionWrapper);

      Map<String, HRegion> recoveringRegions = rsServices.getRecoveringRegions();
      String encodedName = getRegionInfo().getEncodedName();
      if (recoveringRegions != null && recoveringRegions.containsKey(encodedName)) {
        this.isRecovering = true;
        recoveringRegions.put(encodedName, this);
      }
    } else {
      this.metricsRegionWrapper = null;
      this.metricsRegion = null;
    }
    if (LOG.isDebugEnabled()) {
      // Write out region name as string and its encoded name.
      LOG.debug("Instantiated " + this);
    }

    // by default, we allow writes against a region when it's in recovering
    this.disallowWritesInRecovering =
        conf.getBoolean(HConstants.DISALLOW_WRITES_IN_RECOVERING,
          HConstants.DEFAULT_DISALLOW_WRITES_IN_RECOVERING_CONFIG);

    // disable stats tracking system tables, but check the config for everything else
    this.regionStatsEnabled = htd.getTableName().getNamespaceAsString().equals(
      NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) ? false :
        conf.getBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE,
          HConstants.DEFAULT_ENABLE_CLIENT_BACKPRESSURE);
  }

  void setHTableSpecificConf() {
    if (this.htableDescriptor == null) return;
    long flushSize = this.htableDescriptor.getMemStoreFlushSize();

    if (flushSize <= 0) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
    this.memstoreFlushSize = flushSize;
    this.blockingMemStoreSize = this.memstoreFlushSize *
        conf.getLong("hbase.hregion.memstore.block.multiplier", 2);
  }

  /**
   * Initialize this region.
   * Used only by tests and SplitTransaction to reopen the region.
   * You should use createHRegion() or openHRegion()
   * @return What the next sequence (edit) id should be.
   * @throws IOException e
   * @deprecated use HRegion.createHRegion() or HRegion.openHRegion()
   */
  @Deprecated
  public long initialize() throws IOException {
    return initialize(null);
  }

  /**
   * Initialize this region.
   *
   * @param reporter Tickle every so often if initialize is taking a while.
   * @return What the next sequence (edit) id should be.
   * @throws IOException e
   */
  private long initialize(final CancelableProgressable reporter) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this);
    long nextSeqId = -1;
    try {
      nextSeqId = initializeRegionInternals(reporter, status);
      return nextSeqId;
    } finally {
      // nextSeqid will be -1 if the initialization fails.
      // At least it will be 0 otherwise.
      if (nextSeqId == -1) {
        status
            .abort("Exception during region " + this.getRegionNameAsString() + " initialization.");
      }
    }
  }

  private long initializeRegionInternals(final CancelableProgressable reporter,
      final MonitoredTask status) throws IOException, UnsupportedEncodingException {
    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-open hook");
      coprocessorHost.preOpen();
    }

    // Write HRI to a file in case we need to recover hbase:meta
    status.setStatus("Writing region info on filesystem");
    fs.checkRegionInfoOnFilesystem();

    // Remove temporary data left over from old regions
    status.setStatus("Cleaning up temporary data from old regions");
    fs.cleanupTempDir();

    // Initialize all the HStores
    status.setStatus("Initializing all the Stores");
    long maxSeqId = initializeRegionStores(reporter, status);

    status.setStatus("Cleaning up detritus from prior splits");
    // Get rid of any splits or merges that were lost in-progress.  Clean out
    // these directories here on open.  We may be opening a region that was
    // being split but we crashed in the middle of it all.
    fs.cleanupAnySplitDetritus();
    fs.cleanupMergesDir();

    this.writestate.setReadOnly(this.htableDescriptor.isReadOnly());
    this.writestate.flushRequested = false;
    this.writestate.compacting = 0;

    // Initialize split policy
    this.splitPolicy = RegionSplitPolicy.create(this, conf);

    this.lastFlushTime = EnvironmentEdgeManager.currentTimeMillis();
    // Use maximum of log sequenceid or that which was found in stores
    // (particularly if no recovered edits, seqid will be -1).
    long nextSeqid = maxSeqId + 1;
    if (this.isRecovering) {
      // In distributedLogReplay mode, we don't know the last change sequence number because region
      // is opened before recovery completes. So we add a safety bumper to avoid new sequence number
      // overlaps used sequence numbers
      nextSeqid += this.flushPerChanges + 10000000; // add another extra 10million
    }
    LOG.info("Onlined " + this.getRegionInfo().getShortNameToLog() +
      "; next sequenceid=" + nextSeqid);

    // A region can be reopened if failed a split; reset flags
    this.closing.set(false);
    this.closed.set(false);

    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor post-open hooks");
      coprocessorHost.postOpen();
    }

    status.markComplete("Region opened successfully");
    return nextSeqid;
  }

  private long initializeRegionStores(final CancelableProgressable reporter, MonitoredTask status)
      throws IOException, UnsupportedEncodingException {
    // Load in all the HStores.

    long maxSeqId = -1;
    // initialized to -1 so that we pick up MemstoreTS from column families
    long maxMemstoreTS = -1;

    if (!htableDescriptor.getFamilies().isEmpty()) {
      // initialize the thread pool for opening stores in parallel.
      ThreadPoolExecutor storeOpenerThreadPool =
        getStoreOpenAndCloseThreadPool("StoreOpener-" + this.getRegionInfo().getShortNameToLog());
      CompletionService<HStore> completionService =
        new ExecutorCompletionService<HStore>(storeOpenerThreadPool);

      // initialize each store in parallel
      for (final HColumnDescriptor family : htableDescriptor.getFamilies()) {
        status.setStatus("Instantiating store for column family " + family);
        completionService.submit(new Callable<HStore>() {
          @Override
          public HStore call() throws IOException {
            return instantiateHStore(family);
          }
        });
      }
      boolean allStoresOpened = false;
      try {
        for (int i = 0; i < htableDescriptor.getFamilies().size(); i++) {
          Future<HStore> future = completionService.take();
          HStore store = future.get();
          this.stores.put(store.getColumnFamilyName().getBytes(), store);

          long storeMaxSequenceId = store.getMaxSequenceId();
          maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(),
              storeMaxSequenceId);
          if (maxSeqId == -1 || storeMaxSequenceId > maxSeqId) {
            maxSeqId = storeMaxSequenceId;
          }
          long maxStoreMemstoreTS = store.getMaxMemstoreTS();
          if (maxStoreMemstoreTS > maxMemstoreTS) {
            maxMemstoreTS = maxStoreMemstoreTS;
          }
        }
        allStoresOpened = true;
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      } finally {
        storeOpenerThreadPool.shutdownNow();
        if (!allStoresOpened) {
          // something went wrong, close all opened stores
          LOG.error("Could not initialize all stores for the region=" + this);
          for (Store store : this.stores.values()) {
            try {
              store.close();
            } catch (IOException e) {
              LOG.warn(e.getMessage());
            }
          }
        }
      }
    }
    mvcc.initialize(maxMemstoreTS + 1);
    // Recover any edits if available.
    maxSeqId = Math.max(maxSeqId, replayRecoveredEditsIfAny(
        this.fs.getRegionDir(), maxSeqIdInStores, reporter, status));
    return maxSeqId;
  }

  /**
   * @return True if this region has references.
   */
  public boolean hasReferences() {
    for (Store store : this.stores.values()) {
      if (store.hasReferences()) return true;
    }
    return false;
  }

  /**
   * This function will return the HDFS blocks distribution based on the data
   * captured when HFile is created
   * @return The HDFS blocks distribution for the region.
   */
  public HDFSBlocksDistribution getHDFSBlocksDistribution() {
    HDFSBlocksDistribution hdfsBlocksDistribution =
      new HDFSBlocksDistribution();
    synchronized (this.stores) {
      for (Store store : this.stores.values()) {
        for (StoreFile sf : store.getStorefiles()) {
          HDFSBlocksDistribution storeFileBlocksDistribution =
            sf.getHDFSBlockDistribution();
          hdfsBlocksDistribution.add(storeFileBlocksDistribution);
        }
      }
    }
    return hdfsBlocksDistribution;
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf configuration
   * @param tableDescriptor HTableDescriptor of the table
   * @param regionInfo encoded name of the region
   * @return The HDFS blocks distribution for the given region.
   * @throws IOException
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(final Configuration conf,
      final HTableDescriptor tableDescriptor, final HRegionInfo regionInfo) throws IOException {
    Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(conf), tableDescriptor.getTableName());
    return computeHDFSBlocksDistribution(conf, tableDescriptor, regionInfo, tablePath);
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf configuration
   * @param tableDescriptor HTableDescriptor of the table
   * @param regionInfo encoded name of the region
   * @param tablePath the table directory
   * @return The HDFS blocks distribution for the given region.
   * @throws IOException
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(final Configuration conf,
      final HTableDescriptor tableDescriptor, final HRegionInfo regionInfo,  Path tablePath)
      throws IOException {
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    FileSystem fs = tablePath.getFileSystem(conf);

    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tablePath, regionInfo);
    for (HColumnDescriptor family: tableDescriptor.getFamilies()) {
      Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family.getNameAsString());
      if (storeFiles == null) continue;

      for (StoreFileInfo storeFileInfo : storeFiles) {
        hdfsBlocksDistribution.add(storeFileInfo.computeHDFSBlocksDistribution(fs));
      }
    }
    return hdfsBlocksDistribution;
  }

  public AtomicLong getMemstoreSize() {
    return memstoreSize;
  }

  /**
   * Increase the size of mem store in this region and the size of global mem
   * store
   * @param memStoreSize
   * @return the size of memstore in this region
   */
  public long addAndGetGlobalMemstoreSize(long memStoreSize) {
    if (this.rsAccounting != null) {
      rsAccounting.addAndGetGlobalMemstoreSize(memStoreSize);
    }
    return this.memstoreSize.addAndGet(memStoreSize);
  }

  /** @return a HRegionInfo object for this region */
  public HRegionInfo getRegionInfo() {
    return this.fs.getRegionInfo();
  }

  /**
   * @return Instance of {@link RegionServerServices} used by this HRegion.
   * Can be null.
   */
  RegionServerServices getRegionServerServices() {
    return this.rsServices;
  }

  /**
   * @return split policy for this region.
   */
  public RegionSplitPolicy getSplitPolicy() {
    return this.splitPolicy;
  }

  /** @return readRequestsCount for this region */
  long getReadRequestsCount() {
    return this.readRequestsCount.get();
  }

  /** @return writeRequestsCount for this region */
  long getWriteRequestsCount() {
    return this.writeRequestsCount.get();
  }

  public MetricsRegion getMetrics() {
    return metricsRegion;
  }

  /** @return true if region is closed */
  public boolean isClosed() {
    return this.closed.get();
  }

  /**
   * @return True if closing process has started.
   */
  public boolean isClosing() {
    return this.closing.get();
  }

  /**
   * Reset recovering state of current region
   * @param newState
   */
  public void setRecovering(boolean newState) {
    boolean wasRecovering = this.isRecovering;
    this.isRecovering = newState;
    if (wasRecovering && !isRecovering) {
      // Call only when log replay is over.
      coprocessorHost.postLogReplay();
    }
  }

  /**
   * @return True if current region is in recovering
   */
  public boolean isRecovering() {
    return this.isRecovering;
  }

  /** @return true if region is available (not closed and not closing) */
  public boolean isAvailable() {
    return !isClosed() && !isClosing();
  }

  /** @return true if region is splittable */
  public boolean isSplittable() {
    return isAvailable() && !hasReferences();
  }

  /**
   * @return true if region is mergeable
   */
  public boolean isMergeable() {
    if (!isAvailable()) {
      LOG.debug("Region " + this.getRegionNameAsString()
          + " is not mergeable because it is closing or closed");
      return false;
    }
    if (hasReferences()) {
      LOG.debug("Region " + this.getRegionNameAsString()
          + " is not mergeable because it has references");
      return false;
    }

    return true;
  }

  public boolean areWritesEnabled() {
    synchronized(this.writestate) {
      return this.writestate.writesEnabled;
    }
  }

   public MultiVersionConsistencyControl getMVCC() {
     return mvcc;
   }

   /*
    * Returns readpoint considering given IsolationLevel
    */
   public long getReadpoint(IsolationLevel isolationLevel) {
     if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
       // This scan can read even uncommitted transactions
       return Long.MAX_VALUE;
     }
     return mvcc.memstoreReadPoint();
   }

   public boolean isLoadingCfsOnDemandDefault() {
     return this.isLoadingCfsOnDemandDefault;
   }

  /**
   * Close down this HRegion.  Flush the cache, shut down each HStore, don't
   * service any more calls.
   *
   * <p>This method could take some time to execute, so don't call it from a
   * time-sensitive thread.
   *
   * @return Vector of all the storage files that the HRegion's component
   * HStores make use of.  It's a list of all HStoreFile objects. Returns empty
   * vector if already closed and null if judged that it should not close.
   *
   * @throws IOException e
   */
  public Map<byte[], List<StoreFile>> close() throws IOException {
    return close(false);
  }

  private final Object closeLock = new Object();

  /** Conf key for the periodic flush interval */
  public static final String MEMSTORE_PERIODIC_FLUSH_INTERVAL =
      "hbase.regionserver.optionalcacheflushinterval";
  /** Default interval for the memstore flush */
  public static final int DEFAULT_CACHE_FLUSH_INTERVAL = 3600000;

  /** Conf key to force a flush if there are already enough changes for one region in memstore */
  public static final String MEMSTORE_FLUSH_PER_CHANGES =
      "hbase.regionserver.flush.per.changes";
  public static final long DEFAULT_FLUSH_PER_CHANGES = 30000000; // 30 millions
  /**
   * The following MAX_FLUSH_PER_CHANGES is large enough because each KeyValue has 20+ bytes
   * overhead. Therefore, even 1G empty KVs occupy at least 20GB memstore size for a single region
   */
  public static final long MAX_FLUSH_PER_CHANGES = 1000000000; // 1G

  /**
   * Close down this HRegion.  Flush the cache unless abort parameter is true,
   * Shut down each HStore, don't service any more calls.
   *
   * This method could take some time to execute, so don't call it from a
   * time-sensitive thread.
   *
   * @param abort true if server is aborting (only during testing)
   * @return Vector of all the storage files that the HRegion's component
   * HStores make use of.  It's a list of HStoreFile objects.  Can be null if
   * we are not to close at this time or we are already closed.
   *
   * @throws IOException e
   */
  public Map<byte[], List<StoreFile>> close(final boolean abort) throws IOException {
    // Only allow one thread to close at a time. Serialize them so dual
    // threads attempting to close will run up against each other.
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Closing region " + this +
        (abort ? " due to abort" : ""));

    status.setStatus("Waiting for close lock");
    try {
      synchronized (closeLock) {
        return doClose(abort, status);
      }
    } finally {
      status.cleanup();
    }
  }

  private Map<byte[], List<StoreFile>> doClose(final boolean abort, MonitoredTask status)
      throws IOException {
    if (isClosed()) {
      LOG.warn("Region " + this + " already closed");
      return null;
    }

    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-close hooks");
      this.coprocessorHost.preClose(abort);
    }

    status.setStatus("Disabling compacts and flushes for region");
    synchronized (writestate) {
      // Disable compacting and flushing by background threads for this
      // region.
      writestate.writesEnabled = false;
      LOG.debug("Closing " + this + ": disabling compactions & flushes");
      waitForFlushesAndCompactions();
    }
    // If we were not just flushing, is it worth doing a preflush...one
    // that will clear out of the bulk of the memstore before we put up
    // the close flag?
    if (!abort && worthPreFlushing()) {
      status.setStatus("Pre-flushing region before close");
      LOG.info("Running close preflush of " + this.getRegionNameAsString());
      try {
        internalFlushcache(status);
      } catch (IOException ioe) {
        // Failed to flush the region. Keep going.
        status.setStatus("Failed pre-flush " + this + "; " + ioe.getMessage());
      }
    }

    this.closing.set(true);
    status.setStatus("Disabling writes for close");
    // block waiting for the lock for closing
    lock.writeLock().lock();
    try {
      if (this.isClosed()) {
        status.abort("Already got closed by another process");
        // SplitTransaction handles the null
        return null;
      }
      LOG.debug("Updates disabled for region " + this);
      // Don't flush the cache if we are aborting
      if (!abort) {
        int flushCount = 0;
        while (this.getMemstoreSize().get() > 0) {
          try {
            if (flushCount++ > 0) {
              int actualFlushes = flushCount - 1;
              if (actualFlushes > 5) {
                // If we tried 5 times and are unable to clear memory, abort
                // so we do not lose data
                throw new DroppedSnapshotException("Failed clearing memory after " +
                  actualFlushes + " attempts on region: " + Bytes.toStringBinary(getRegionName()));
              }
              LOG.info("Running extra flush, " + actualFlushes +
                " (carrying snapshot?) " + this);
            }
            internalFlushcache(status);
          } catch (IOException ioe) {
            status.setStatus("Failed flush " + this + ", putting online again");
            synchronized (writestate) {
              writestate.writesEnabled = true;
            }
            // Have to throw to upper layers.  I can't abort server from here.
            throw ioe;
          }
        }
      }

      Map<byte[], List<StoreFile>> result =
        new TreeMap<byte[], List<StoreFile>>(Bytes.BYTES_COMPARATOR);
      if (!stores.isEmpty()) {
        // initialize the thread pool for closing stores in parallel.
        ThreadPoolExecutor storeCloserThreadPool =
          getStoreOpenAndCloseThreadPool("StoreCloserThread-" + this.getRegionNameAsString());
        CompletionService<Pair<byte[], Collection<StoreFile>>> completionService =
          new ExecutorCompletionService<Pair<byte[], Collection<StoreFile>>>(storeCloserThreadPool);

        // close each store in parallel
        for (final Store store : stores.values()) {
          assert abort? true: store.getFlushableSize() == 0;
          completionService
              .submit(new Callable<Pair<byte[], Collection<StoreFile>>>() {
                @Override
                public Pair<byte[], Collection<StoreFile>> call() throws IOException {
                  return new Pair<byte[], Collection<StoreFile>>(
                    store.getFamily().getName(), store.close());
                }
              });
        }
        try {
          for (int i = 0; i < stores.size(); i++) {
            Future<Pair<byte[], Collection<StoreFile>>> future = completionService.take();
            Pair<byte[], Collection<StoreFile>> storeFiles = future.get();
            List<StoreFile> familyFiles = result.get(storeFiles.getFirst());
            if (familyFiles == null) {
              familyFiles = new ArrayList<StoreFile>();
              result.put(storeFiles.getFirst(), familyFiles);
            }
            familyFiles.addAll(storeFiles.getSecond());
          }
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        } finally {
          storeCloserThreadPool.shutdownNow();
        }
      }
      this.closed.set(true);
      if (memstoreSize.get() != 0) LOG.error("Memstore size is " + memstoreSize.get());
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor post-close hooks");
        this.coprocessorHost.postClose(abort);
      }
      if ( this.metricsRegion != null) {
        this.metricsRegion.close();
      }
      if ( this.metricsRegionWrapper != null) {
        Closeables.closeQuietly(this.metricsRegionWrapper);
      }
      status.markComplete("Closed");
      LOG.info("Closed " + this);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Wait for all current flushes and compactions of the region to complete.
   * <p>
   * Exposed for TESTING.
   */
  public void waitForFlushesAndCompactions() {
    synchronized (writestate) {
      while (writestate.compacting > 0 || writestate.flushing) {
        LOG.debug("waiting for " + writestate.compacting + " compactions"
            + (writestate.flushing ? " & cache flush" : "") + " to complete for region " + this);
        try {
          writestate.wait();
        } catch (InterruptedException iex) {
          // essentially ignore and propagate the interrupt back up
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  protected ThreadPoolExecutor getStoreOpenAndCloseThreadPool(
      final String threadNamePrefix) {
    int numStores = Math.max(1, this.htableDescriptor.getFamilies().size());
    int maxThreads = Math.min(numStores,
        conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
            HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX));
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  protected ThreadPoolExecutor getStoreFileOpenAndCloseThreadPool(
      final String threadNamePrefix) {
    int numStores = Math.max(1, this.htableDescriptor.getFamilies().size());
    int maxThreads = Math.max(1,
        conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
            HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX)
            / numStores);
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  static ThreadPoolExecutor getOpenAndCloseThreadPool(int maxThreads,
      final String threadNamePrefix) {
    return Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
      new ThreadFactory() {
        private int count = 1;

        @Override
        public Thread newThread(Runnable r) {
          return new Thread(r, threadNamePrefix + "-" + count++);
        }
      });
  }

   /**
    * @return True if its worth doing a flush before we put up the close flag.
    */
  private boolean worthPreFlushing() {
    return this.memstoreSize.get() >
      this.conf.getLong("hbase.hregion.preclose.flush.size", 1024 * 1024 * 5);
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public byte [] getStartKey() {
    return this.getRegionInfo().getStartKey();
  }

  /** @return end key for region */
  public byte [] getEndKey() {
    return this.getRegionInfo().getEndKey();
  }

  /** @return region id */
  public long getRegionId() {
    return this.getRegionInfo().getRegionId();
  }

  /** @return region name */
  public byte [] getRegionName() {
    return this.getRegionInfo().getRegionName();
  }

  /** @return region name as string for logging */
  public String getRegionNameAsString() {
    return this.getRegionInfo().getRegionNameAsString();
  }

  /** @return HTableDescriptor for this region */
  public HTableDescriptor getTableDesc() {
    return this.htableDescriptor;
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /**
   * A split takes the config from the parent region & passes it to the daughter
   * region's constructor. If 'conf' was passed, you would end up using the HTD
   * of the parent region in addition to the new daughter HTD. Pass 'baseConf'
   * to the daughter regions to avoid this tricky dedupe problem.
   * @return Configuration object
   */
  Configuration getBaseConf() {
    return this.baseConf;
  }

  /** @return {@link FileSystem} being used by this region */
  public FileSystem getFilesystem() {
    return fs.getFileSystem();
  }

  /** @return the {@link HRegionFileSystem} used by this region */
  public HRegionFileSystem getRegionFileSystem() {
    return this.fs;
  }

  /** @return the last time the region was flushed */
  public long getLastFlushTime() {
    return this.lastFlushTime;
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.
  //
  // These methods are meant to be called periodically by the HRegionServer for
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////

  /** @return returns size of largest HStore. */
  public long getLargestHStoreSize() {
    long size = 0;
    for (Store h : stores.values()) {
      long storeSize = h.getSize();
      if (storeSize > size) {
        size = storeSize;
      }
    }
    return size;
  }

  /**
   * @return KeyValue Comparator
   */
  public KeyValue.KVComparator getComparator() {
    return this.comparator;
  }

  /*
   * Do preparation for pending compaction.
   * @throws IOException
   */
  protected void doRegionCompactionPrep() throws IOException {
  }

  void triggerMajorCompaction() {
    for (Store h : stores.values()) {
      h.triggerMajorCompaction();
    }
  }

  /**
   * This is a helper function that compact all the stores synchronously
   * It is used by utilities and testing
   *
   * @param majorCompaction True to force a major compaction regardless of thresholds
   * @throws IOException e
   */
  public void compactStores(final boolean majorCompaction)
  throws IOException {
    if (majorCompaction) {
      this.triggerMajorCompaction();
    }
    compactStores();
  }

  /**
   * This is a helper function that compact all the stores synchronously
   * It is used by utilities and testing
   *
   * @throws IOException e
   */
  public void compactStores() throws IOException {
    for (Store s : getStores().values()) {
      CompactionContext compaction = s.requestCompaction();
      if (compaction != null) {
        compact(compaction, s, NoLimitCompactionThroughputController.INSTANCE);
      }
    }
  }

  /**
   * Called by compaction thread and after region is opened to compact the
   * HStores if necessary.
   *
   * <p>This operation could block for a long time, so don't call it from a
   * time-sensitive thread.
   *
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   *
   * @param compaction Compaction details, obtained by requestCompaction()
   * @return whether the compaction completed
   */
  public boolean compact(CompactionContext compaction, Store store,
      CompactionThroughputController throughputController) throws IOException {
    assert compaction != null && compaction.hasSelection();
    assert !compaction.getRequest().getFiles().isEmpty();
    if (this.closing.get() || this.closed.get()) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      store.cancelRequestedCompaction(compaction);
      return false;
    }
    MonitoredTask status = null;
    boolean requestNeedsCancellation = true;
    // block waiting for the lock for compaction
    lock.readLock().lock();
    try {
      byte[] cf = Bytes.toBytes(store.getColumnFamilyName());
      if (stores.get(cf) != store) {
        LOG.warn("Store " + store.getColumnFamilyName() + " on region " + this
            + " has been re-instantiated, cancel this compaction request. "
            + " It may be caused by the roll back of split transaction");
        return false;
      }

      status = TaskMonitor.get().createStatus("Compacting " + store + " in " + this);
      if (this.closed.get()) {
        String msg = "Skipping compaction on " + this + " because closed";
        LOG.debug(msg);
        status.abort(msg);
        return false;
      }
      boolean wasStateSet = false;
      try {
        synchronized (writestate) {
          if (writestate.writesEnabled) {
            wasStateSet = true;
            ++writestate.compacting;
          } else {
            String msg = "NOT compacting region " + this + ". Writes disabled.";
            LOG.info(msg);
            status.abort(msg);
            return false;
          }
        }
        LOG.info("Starting compaction on " + store + " in region " + this
            + (compaction.getRequest().isOffPeak()?" as an off-peak compaction":""));
        doRegionCompactionPrep();
        try {
          status.setStatus("Compacting store " + store);
          // We no longer need to cancel the request on the way out of this
          // method because Store#compact will clean up unconditionally
          requestNeedsCancellation = false;
          store.compact(compaction, throughputController);
        } catch (InterruptedIOException iioe) {
          String msg = "compaction interrupted";
          LOG.info(msg, iioe);
          status.abort(msg);
          return false;
        }
      } finally {
        if (wasStateSet) {
          synchronized (writestate) {
            --writestate.compacting;
            if (writestate.compacting <= 0) {
              writestate.notifyAll();
            }
          }
        }
      }
      status.markComplete("Compaction complete");
      return true;
    } finally {
      try {
        if (requestNeedsCancellation) store.cancelRequestedCompaction(compaction);
        if (status != null) status.cleanup();
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  /**
   * Flush the cache.
   *
   * When this method is called the cache will be flushed unless:
   * <ol>
   *   <li>the cache is empty</li>
   *   <li>the region is closed.</li>
   *   <li>a flush is already in progress</li>
   *   <li>writes are disabled</li>
   * </ol>
   *
   * <p>This method may block for some time, so it should not be called from a
   * time-sensitive thread.
   *
   * @return true if the region needs compacting
   *
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  public FlushResult flushcache() throws IOException {
    // fail-fast instead of waiting on the lock
    if (this.closing.get()) {
      String msg = "Skipping flush on " + this + " because closing";
      LOG.debug(msg);
      return new FlushResult(FlushResult.Result.CANNOT_FLUSH, msg);
    }
    MonitoredTask status = TaskMonitor.get().createStatus("Flushing " + this);
    status.setStatus("Acquiring readlock on region");
    // block waiting for the lock for flushing cache
    lock.readLock().lock();
    try {
      if (this.closed.get()) {
        String msg = "Skipping flush on " + this + " because closed";
        LOG.debug(msg);
        status.abort(msg);
        return new FlushResult(FlushResult.Result.CANNOT_FLUSH, msg);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor pre-flush hooks");
        coprocessorHost.preFlush();
      }
      if (numMutationsWithoutWAL.get() > 0) {
        numMutationsWithoutWAL.set(0);
        dataInMemoryWithoutWAL.set(0);
      }
      synchronized (writestate) {
        if (!writestate.flushing && writestate.writesEnabled) {
          this.writestate.flushing = true;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("NOT flushing memstore for region " + this
                + ", flushing=" + writestate.flushing + ", writesEnabled="
                + writestate.writesEnabled);
          }
          String msg = "Not flushing since "
              + (writestate.flushing ? "already flushing"
              : "writes not enabled");
          status.abort(msg);
          return new FlushResult(FlushResult.Result.CANNOT_FLUSH, msg);
        }
      }
      try {
        FlushResult fs = internalFlushcache(status);

        if (coprocessorHost != null) {
          status.setStatus("Running post-flush coprocessor hooks");
          coprocessorHost.postFlush();
        }

        status.markComplete("Flush successful");
        return fs;
      } finally {
        synchronized (writestate) {
          writestate.flushing = false;
          this.writestate.flushRequested = false;
          writestate.notifyAll();
        }
      }
    } finally {
      lock.readLock().unlock();
      status.cleanup();
    }
  }

  /**
   * Should the memstore be flushed now
   */
  boolean shouldFlush() {
    // This is a rough measure.
    if (this.completeSequenceId > 0
          && (this.completeSequenceId + this.flushPerChanges < this.sequenceId.get())) {
      return true;
    }
    if (flushCheckInterval <= 0) { //disabled
      return false;
    }
    long now = EnvironmentEdgeManager.currentTimeMillis();
    //if we flushed in the recent past, we don't need to do again now
    if ((now - getLastFlushTime() < flushCheckInterval)) {
      return false;
    }
    //since we didn't flush in the recent past, flush now if certain conditions
    //are met. Return true on first such memstore hit.
    for (Store s : this.getStores().values()) {
      if (s.timeOfOldestEdit() < now - flushCheckInterval) {
        // we have an old enough edit in the memstore, flush
        return true;
      }
    }
    return false;
  }

  /**
   * Flush the memstore.
   *
   * Flushing the memstore is a little tricky. We have a lot of updates in the
   * memstore, all of which have also been written to the log. We need to
   * write those updates in the memstore out to disk, while being able to
   * process reads/writes as much as possible during the flush operation. Also,
   * the log has to state clearly the point in time at which the memstore was
   * flushed. (That way, during recovery, we know when we can rely on the
   * on-disk flushed structures and when we have to recover the memstore from
   * the log.)
   *
   * <p>So, we have a three-step process:
   *
   * <ul><li>A. Flush the memstore to the on-disk stores, noting the current
   * sequence ID for the log.<li>
   *
   * <li>B. Write a FLUSHCACHE-COMPLETE message to the log, using the sequence
   * ID that was current at the time of memstore-flush.</li>
   *
   * <li>C. Get rid of the memstore structures that are now redundant, as
   * they've been flushed to the on-disk HStores.</li>
   * </ul>
   * <p>This method is protected, but can be accessed via several public
   * routes.
   *
   * <p> This method may block for some time.
   * @param status
   *
   * @return object describing the flush's state
   *
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  protected FlushResult internalFlushcache(MonitoredTask status)
      throws IOException {
    return internalFlushcache(this.log, -1, status);
  }

  /**
   * @param wal Null if we're NOT to go via hlog/wal.
   * @param myseqid The seqid to use if <code>wal</code> is null writing out
   * flush file.
   * @param status
   * @return true if the region needs compacting
   * @throws IOException
   * @see #internalFlushcache(MonitoredTask)
   */
  protected FlushResult internalFlushcache(
      final HLog wal, final long myseqid, MonitoredTask status)
  throws IOException {
    if (this.rsServices != null && this.rsServices.isAborted()) {
      // Don't flush when server aborting, it's unsafe
      throw new IOException("Aborting flush because server is abortted...");
    }
    final long startTime = EnvironmentEdgeManager.currentTimeMillis();
    // Clear flush flag.
    // If nothing to flush, return and avoid logging start/stop flush.
    if (this.memstoreSize.get() <= 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Empty memstore size for the current region "+this);
      }
      return new FlushResult(FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, "Nothing to flush");
    }

    LOG.info("Started memstore flush for " + this +
      ", current region memstore size " +
      StringUtils.humanReadableInt(this.memstoreSize.get()) +
      ((wal != null)? "": "; wal is null, using passed sequenceid=" + myseqid));

    // Stop updates while we snapshot the memstore of all stores. We only have
    // to do this for a moment.  Its quick.  The subsequent sequence id that
    // goes into the HLog after we've flushed all these snapshots also goes
    // into the info file that sits beside the flushed files.
    // We also set the memstore size to zero here before we allow updates
    // again so its value will represent the size of the updates received
    // during the flush
    MultiVersionConsistencyControl.WriteEntry w = null;

    // We have to take a write lock during snapshot, or else a write could
    // end up in both snapshot and memstore (makes it difficult to do atomic
    // rows then)
    status.setStatus("Obtaining lock to block concurrent updates");
    // block waiting for the lock for internal flush
    this.updatesLock.writeLock().lock();
    long totalFlushableSize = 0;
    status.setStatus("Preparing to flush by snapshotting stores");
    List<StoreFlushContext> storeFlushCtxs = new ArrayList<StoreFlushContext>(stores.size());
    long flushSeqId = -1L;
    try {
      // Record the mvcc for all transactions in progress.
      w = mvcc.beginMemstoreInsert();
      mvcc.advanceMemstore(w);
      // check if it is not closing.
      if (wal != null) {
        if (!wal.startCacheFlush(this.getRegionInfo().getEncodedNameAsBytes())) {
          String msg = "Flush will not be started for ["
              + this.getRegionInfo().getEncodedName() + "] - because the WAL is closing.";
          status.setStatus(msg);
          return new FlushResult(FlushResult.Result.CANNOT_FLUSH, msg);
        }
        flushSeqId = this.sequenceId.incrementAndGet();
      } else {
        // use the provided sequence Id as WAL is not being used for this flush.
        flushSeqId = myseqid;
      }

      for (Store s : stores.values()) {
        totalFlushableSize += s.getFlushableSize();
        storeFlushCtxs.add(s.createFlushContext(flushSeqId));
      }

      // prepare flush (take a snapshot)
      for (StoreFlushContext flush : storeFlushCtxs) {
        flush.prepare();
      }
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    String s = "Finished memstore snapshotting " + this +
      ", syncing WAL and waiting on mvcc, flushsize=" + totalFlushableSize;
    status.setStatus(s);
    if (LOG.isTraceEnabled()) LOG.trace(s);

    // sync unflushed WAL changes when deferred log sync is enabled
    // see HBASE-8208 for details
    if (wal != null && !shouldSyncLog()) {
      wal.sync();
    }

    // wait for all in-progress transactions to commit to HLog before
    // we can start the flush. This prevents
    // uncommitted transactions from being written into HFiles.
    // We have to block before we start the flush, otherwise keys that
    // were removed via a rollbackMemstore could be written to Hfiles.
    mvcc.waitForRead(w);

    s = "Flushing stores of " + this;
    status.setStatus(s);
    if (LOG.isTraceEnabled()) LOG.trace(s);

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    try {
      // A.  Flush memstore to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file. The new flushed file is still in the
      // tmp directory.

      for (StoreFlushContext flush : storeFlushCtxs) {
        flush.flushCache(status);
      }

      // Switch snapshot (in memstore) -> new hfile (thus causing
      // all the store scanners to reset/reseek).
      for (StoreFlushContext flush : storeFlushCtxs) {
        boolean needsCompaction = flush.commit(status);
        if (needsCompaction) {
          compactionRequested = true;
        }
      }
      storeFlushCtxs.clear();

      // Set down the memstore size by amount of flush.
      this.addAndGetGlobalMemstoreSize(-totalFlushableSize);
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The hlog needs to be replayed so its content is restored to memstore.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      if (wal != null) {
        wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
      }
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
          Bytes.toStringBinary(getRegionName()));
      dse.initCause(t);
      status.abort("Flush failed: " + StringUtils.stringifyException(t));
      throw dse;
    }

    // If we get to here, the HStores have been written.
    if (wal != null) {
      wal.completeCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
    }

    // Record latest flush time
    this.lastFlushTime = EnvironmentEdgeManager.currentTimeMillis();

    // Update the last flushed sequence id for region
    completeSequenceId = flushSeqId;

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }

    long time = EnvironmentEdgeManager.currentTimeMillis() - startTime;
    long memstoresize = this.memstoreSize.get();
    String msg = "Finished memstore flush of ~" +
      StringUtils.humanReadableInt(totalFlushableSize) + "/" + totalFlushableSize +
      ", currentsize=" +
      StringUtils.humanReadableInt(memstoresize) + "/" + memstoresize +
      " for region " + this + " in " + time + "ms, sequenceid=" + flushSeqId +
      ", compaction requested=" + compactionRequested +
      ((wal == null)? "; wal=null": "");
    LOG.info(msg);
    status.setStatus(msg);
    this.recentFlushes.add(new Pair<Long,Long>(time/1000, totalFlushableSize));

    return new FlushResult(compactionRequested ? FlushResult.Result.FLUSHED_COMPACTION_NEEDED :
        FlushResult.Result.FLUSHED_NO_COMPACTION_NEEDED, flushSeqId);
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Return all the data for the row that matches <i>row</i> exactly,
   * or the one that immediately preceeds it, at or immediately before
   * <i>ts</i>.
   *
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  Result getClosestRowBefore(final byte [] row)
  throws IOException{
    return getClosestRowBefore(row, HConstants.CATALOG_FAMILY);
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly,
   * or the one that immediately preceeds it, at or immediately before
   * <i>ts</i>.
   *
   * @param row row key
   * @param family column family to find on
   * @return map of values
   * @throws IOException read exceptions
   */
  public Result getClosestRowBefore(final byte [] row, final byte [] family)
  throws IOException {
    if (coprocessorHost != null) {
      Result result = new Result();
      if (coprocessorHost.preGetClosestRowBefore(row, family, result)) {
        return result;
      }
    }
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    checkRow(row, "getClosestRowBefore");
    startRegionOperation(Operation.GET);
    this.readRequestsCount.increment();
    try {
      Store store = getStore(family);
      // get the closest key. (HStore.getRowKeyAtOrBefore can return null)
      KeyValue key = store.getRowKeyAtOrBefore(row);
      Result result = null;
      if (key != null) {
        Get get = new Get(key.getRow());
        get.addFamily(family);
        result = get(get);
      }
      if (coprocessorHost != null) {
        coprocessorHost.postGetClosestRowBefore(row, family, result);
      }
      return result;
    } finally {
      closeRegionOperation(Operation.GET);
    }
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated
   * columns and rows specified by the {@link Scan}.
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @return RegionScanner
   * @throws IOException read exceptions
   */
  public RegionScanner getScanner(Scan scan) throws IOException {
   return getScanner(scan, null);
  }

  void prepareScanner(Scan scan) throws IOException {
    if(!scan.hasFamilies()) {
      // Adding all families to scanner
      for(byte[] family: this.htableDescriptor.getFamiliesKeys()){
        scan.addFamily(family);
      }
    }
  }

  protected RegionScanner getScanner(Scan scan,
      List<KeyValueScanner> additionalScanners) throws IOException {
    startRegionOperation(Operation.SCAN);
    try {
      // Verify families are all valid
      prepareScanner(scan);
      if(scan.hasFamilies()) {
        for(byte [] family : scan.getFamilyMap().keySet()) {
          checkFamily(family);
        }
      }
      return instantiateRegionScanner(scan, additionalScanners);
    } finally {
      closeRegionOperation(Operation.SCAN);
    }
  }

  protected RegionScanner instantiateRegionScanner(Scan scan,
      List<KeyValueScanner> additionalScanners) throws IOException {
    if (scan.isReversed()) {
      if (scan.getFilter() != null) {
        scan.getFilter().setReversed(true);
      }
      return new ReversedRegionScannerImpl(scan, additionalScanners, this);
    }
    return new RegionScannerImpl(scan, additionalScanners, this);
  }

  /*
   * @param delete The passed delete is modified by this method. WARNING!
   */
  void prepareDelete(Delete delete) throws IOException {
    // Check to see if this is a deleteRow insert
    if(delete.getFamilyCellMap().isEmpty()){
      for(byte [] family : this.htableDescriptor.getFamiliesKeys()){
        // Don't eat the timestamp
        delete.deleteFamily(family, delete.getTimeStamp());
      }
    } else {
      for(byte [] family : delete.getFamilyCellMap().keySet()) {
        if(family == null) {
          throw new NoSuchColumnFamilyException("Empty family is invalid");
        }
        checkFamily(family);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  /**
   * @param delete delete object
   * @throws IOException read exceptions
   */
  public void delete(Delete delete)
  throws IOException {
    checkReadOnly();
    checkResources();
    startRegionOperation(Operation.DELETE);
    try {
      delete.getRow();
      // All edits for the given row (across all column families) must happen atomically.
      doBatchMutate(delete);
    } finally {
      closeRegionOperation(Operation.DELETE);
    }
  }

  /**
   * Row needed by below method.
   */
  private static final byte [] FOR_UNIT_TESTS_ONLY = Bytes.toBytes("ForUnitTestsOnly");
  /**
   * This is used only by unit tests. Not required to be a public API.
   * @param familyMap map of family to edits for the given family.
   * @param durability
   * @throws IOException
   */
  void delete(NavigableMap<byte[], List<Cell>> familyMap,
      Durability durability) throws IOException {
    Delete delete = new Delete(FOR_UNIT_TESTS_ONLY);
    delete.setFamilyCellMap(familyMap);
    delete.setDurability(durability);
    doBatchMutate(delete);
  }

  /**
   * Setup correct timestamps in the KVs in Delete object.
   * Caller should have the row and region locks.
   * @param mutation
   * @param familyMap
   * @param byteNow
   * @throws IOException
   */
  void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyMap,
      byte[] byteNow) throws IOException {
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {

      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;

      Map<byte[], Integer> kvCount = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        //  Check if time is LATEST, change to time of most recent addition if so
        //  This is expensive.
        if (kv.isLatestTimestamp() && kv.isDeleteType()) {
          byte[] qual = kv.getQualifier();
          if (qual == null) qual = HConstants.EMPTY_BYTE_ARRAY;

          Integer count = kvCount.get(qual);
          if (count == null) {
            kvCount.put(qual, 1);
          } else {
            kvCount.put(qual, count + 1);
          }
          count = kvCount.get(qual);

          Get get = new Get(kv.getRow());
          get.setMaxVersions(count);
          get.addColumn(family, qual);
          if (coprocessorHost != null) {
            if (!coprocessorHost.prePrepareTimeStampForDeleteVersion(mutation, cell,
                byteNow, get)) {
              updateDeleteLatestVersionTimeStamp(kv, get, count, byteNow);
            }
          } else {
            updateDeleteLatestVersionTimeStamp(kv, get, count, byteNow);
          }
        } else {
          kv.updateLatestStamp(byteNow);
        }
      }
    }
  }

  void updateDeleteLatestVersionTimeStamp(KeyValue kv, Get get, int count, byte[] byteNow)
      throws IOException {
    List<Cell> result = get(get, false);

    if (result.size() < count) {
      // Nothing to delete
      kv.updateLatestStamp(byteNow);
      return;
    }
    if (result.size() > count) {
      throw new RuntimeException("Unexpected size: " + result.size());
    }
    KeyValue getkv = KeyValueUtil.ensureKeyValue(result.get(count - 1));
    Bytes.putBytes(kv.getBuffer(), kv.getTimestampOffset(), getkv.getBuffer(),
        getkv.getTimestampOffset(), Bytes.SIZEOF_LONG);
  }

  /**
   * @param put
   * @throws IOException
   */
  public void put(Put put)
  throws IOException {
    checkReadOnly();

    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();
    startRegionOperation(Operation.PUT);
    try {
      // All edits for the given row (across all column families) must happen atomically.
      doBatchMutate(put);
    } finally {
      closeRegionOperation(Operation.PUT);
    }
  }

  /**
   * Struct-like class that tracks the progress of a batch operation,
   * accumulating status codes and tracking the index at which processing
   * is proceeding.
   */
  private abstract static class BatchOperationInProgress<T> {
    T[] operations;
    int nextIndexToProcess = 0;
    OperationStatus[] retCodeDetails;
    WALEdit[] walEditsFromCoprocessors;

    public BatchOperationInProgress(T[] operations) {
      this.operations = operations;
      this.retCodeDetails = new OperationStatus[operations.length];
      this.walEditsFromCoprocessors = new WALEdit[operations.length];
      Arrays.fill(this.retCodeDetails, OperationStatus.NOT_RUN);
    }

    public abstract Mutation getMutation(int index);
    public abstract long getNonceGroup(int index);
    public abstract long getNonce(int index);
    /** This method is potentially expensive and should only be used for non-replay CP path. */
    public abstract Mutation[] getMutationsForCoprocs();
    public abstract boolean isInReplay();

    public boolean isDone() {
      return nextIndexToProcess == operations.length;
    }
  }

  private static class MutationBatch extends BatchOperationInProgress<Mutation> {
    private long nonceGroup;
    private long nonce;
    public MutationBatch(Mutation[] operations, long nonceGroup, long nonce) {
      super(operations);
      this.nonceGroup = nonceGroup;
      this.nonce = nonce;
    }

    public Mutation getMutation(int index) {
      return this.operations[index];
    }

    @Override
    public long getNonceGroup(int index) {
      return nonceGroup;
    }

    @Override
    public long getNonce(int index) {
      return nonce;
    }

    @Override
    public Mutation[] getMutationsForCoprocs() {
      return this.operations;
    }

    @Override
    public boolean isInReplay() {
      return false;
    }
  }

  private static class ReplayBatch extends BatchOperationInProgress<HLogSplitter.MutationReplay> {
    public ReplayBatch(MutationReplay[] operations) {
      super(operations);
    }

    @Override
    public Mutation getMutation(int index) {
      return this.operations[index].mutation;
    }

    @Override
    public long getNonceGroup(int index) {
      return this.operations[index].nonceGroup;
    }

    @Override
    public long getNonce(int index) {
      return this.operations[index].nonce;
    }

    @Override
    public Mutation[] getMutationsForCoprocs() {
      assert false;
      throw new RuntimeException("Should not be called for replay batch");
    }

    @Override
    public boolean isInReplay() {
      return true;
    }
  }

  /**
   * Perform a batch of mutations.
   * It supports only Put and Delete mutations and will ignore other types passed.
   * @param mutations the list of mutations
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
  public OperationStatus[] batchMutate(
      Mutation[] mutations, long nonceGroup, long nonce) throws IOException {
    // As it stands, this is used for 3 things
    //  * batchMutate with single mutation - put/delete, separate or from checkAndMutate.
    //  * coprocessor calls (see ex. BulkDeleteEndpoint).
    // So nonces are not really ever used by HBase. They could be by coprocs, and checkAnd...
    return batchMutate(new MutationBatch(mutations, nonceGroup, nonce));
  }

  public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException {
    return batchMutate(mutations, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  /**
   * Replay a batch of mutations.
   * @param mutations mutations to replay.
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
  public OperationStatus[] batchReplay(HLogSplitter.MutationReplay[] mutations)
      throws IOException {
    return batchMutate(new ReplayBatch(mutations));
  }

  /**
   * Perform a batch of mutations.
   * It supports only Put and Delete mutations and will ignore other types passed.
   * @param batchOp contains the list of mutations
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
  OperationStatus[] batchMutate(BatchOperationInProgress<?> batchOp) throws IOException {
    boolean initialized = false;
    Operation op = batchOp.isInReplay() ? Operation.REPLAY_BATCH_MUTATE : Operation.BATCH_MUTATE;
    startRegionOperation(op);
    try {
      while (!batchOp.isDone()) {
        if (!batchOp.isInReplay()) {
          checkReadOnly();
        }
        checkResources();

        if (!initialized) {
          this.writeRequestsCount.add(batchOp.operations.length);
          if (!batchOp.isInReplay()) {
            doPreMutationHook(batchOp);
          }
          initialized = true;
        }
        long addedSize = doMiniBatchMutation(batchOp);
        long newSize = this.addAndGetGlobalMemstoreSize(addedSize);
        if (isFlushSize(newSize)) {
          requestFlush();
        }
      }
    } finally {
      closeRegionOperation(op);
    }
    return batchOp.retCodeDetails;
  }


  private void doPreMutationHook(BatchOperationInProgress<?> batchOp)
      throws IOException {
    /* Run coprocessor pre hook outside of locks to avoid deadlock */
    WALEdit walEdit = new WALEdit();
    if (coprocessorHost != null) {
      for (int i = 0 ; i < batchOp.operations.length; i++) {
        Mutation m = batchOp.getMutation(i);
        if (m instanceof Put) {
          if (coprocessorHost.prePut((Put) m, walEdit, m.getDurability())) {
            // pre hook says skip this Put
            // mark as success and skip in doMiniBatchMutation
            batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
          }
        } else if (m instanceof Delete) {
          Delete curDel = (Delete) m;
          if (curDel.getFamilyCellMap().isEmpty()) {
            // handle deleting a row case
            prepareDelete(curDel);
          }
          if (coprocessorHost.preDelete(curDel, walEdit, m.getDurability())) {
            // pre hook says skip this Delete
            // mark as success and skip in doMiniBatchMutation
            batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
          }
        } else {
          // In case of passing Append mutations along with the Puts and Deletes in batchMutate
          // mark the operation return code as failure so that it will not be considered in
          // the doMiniBatchMutation
          batchOp.retCodeDetails[i] = new OperationStatus(OperationStatusCode.FAILURE,
              "Put/Delete mutations only supported in batchMutate() now");
        }
        if (!walEdit.isEmpty()) {
          batchOp.walEditsFromCoprocessors[i] = walEdit;
          walEdit = new WALEdit();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private long doMiniBatchMutation(BatchOperationInProgress<?> batchOp) throws IOException {
    boolean isInReplay = batchOp.isInReplay();
    // variable to note if all Put items are for the same CF -- metrics related
    boolean putsCfSetConsistent = true;
    //The set of columnFamilies first seen for Put.
    Set<byte[]> putsCfSet = null;
    // variable to note if all Delete items are for the same CF -- metrics related
    boolean deletesCfSetConsistent = true;
    //The set of columnFamilies first seen for Delete.
    Set<byte[]> deletesCfSet = null;

    long currentNonceGroup = HConstants.NO_NONCE, currentNonce = HConstants.NO_NONCE;
    WALEdit walEdit = new WALEdit(isInReplay);
    MultiVersionConsistencyControl.WriteEntry w = null;
    long txid = 0;
    boolean doRollBackMemstore = false;
    boolean locked = false;

    /** Keep track of the locks we hold so we can release them in finally clause */
    List<RowLock> acquiredRowLocks = Lists.newArrayListWithCapacity(batchOp.operations.length);
    // reference family maps directly so coprocessors can mutate them if desired
    Map<byte[], List<Cell>>[] familyMaps = new Map[batchOp.operations.length];
    // We try to set up a batch in the range [firstIndex,lastIndexExclusive)
    int firstIndex = batchOp.nextIndexToProcess;
    int lastIndexExclusive = firstIndex;
    boolean success = false;
    int noOfPuts = 0, noOfDeletes = 0;
    try {
      // ------------------------------------
      // STEP 1. Try to acquire as many locks as we can, and ensure
      // we acquire at least one.
      // ----------------------------------
      int numReadyToWrite = 0;
      long now = EnvironmentEdgeManager.currentTimeMillis();
      while (lastIndexExclusive < batchOp.operations.length) {
        Mutation mutation = batchOp.getMutation(lastIndexExclusive);
        boolean isPutMutation = mutation instanceof Put;

        Map<byte[], List<Cell>> familyMap = mutation.getFamilyCellMap();
        // store the family map reference to allow for mutations
        familyMaps[lastIndexExclusive] = familyMap;

        // skip anything that "ran" already
        if (batchOp.retCodeDetails[lastIndexExclusive].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) {
          lastIndexExclusive++;
          continue;
        }

        try {
          if (isPutMutation) {
            // Check the families in the put. If bad, skip this one.
            if (isInReplay) {
              removeNonExistentColumnFamilyForReplay(familyMap);
            } else {
              checkFamilies(familyMap.keySet());
            }
            checkTimestamps(mutation.getFamilyCellMap(), now);
          } else {
            prepareDelete((Delete) mutation);
          }
        } catch (NoSuchColumnFamilyException nscf) {
          LOG.warn("No such column family in batch mutation", nscf);
          batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
              OperationStatusCode.BAD_FAMILY, nscf.getMessage());
          lastIndexExclusive++;
          continue;
        } catch (FailedSanityCheckException fsce) {
          LOG.warn("Batch Mutation did not pass sanity check", fsce);
          batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
              OperationStatusCode.SANITY_CHECK_FAILURE, fsce.getMessage());
          lastIndexExclusive++;
          continue;
        }

        // If we haven't got any rows in our batch, we should block to
        // get the next one.
        boolean shouldBlock = numReadyToWrite == 0;
        RowLock rowLock = null;
        try {
          rowLock = getRowLockInternal(mutation.getRow(), shouldBlock);
        } catch (IOException ioe) {
          LOG.warn("Failed getting lock in batch put, row="
            + Bytes.toStringBinary(mutation.getRow()), ioe);
        }
        if (rowLock == null) {
          // We failed to grab another lock
          assert !shouldBlock : "Should never fail to get lock when blocking";
          break; // stop acquiring more rows for this batch
        } else {
          acquiredRowLocks.add(rowLock);
        }

        lastIndexExclusive++;
        numReadyToWrite++;

        if (isPutMutation) {
          // If Column Families stay consistent through out all of the
          // individual puts then metrics can be reported as a mutliput across
          // column families in the first put.
          if (putsCfSet == null) {
            putsCfSet = mutation.getFamilyCellMap().keySet();
          } else {
            putsCfSetConsistent = putsCfSetConsistent
                && mutation.getFamilyCellMap().keySet().equals(putsCfSet);
          }
        } else {
          if (deletesCfSet == null) {
            deletesCfSet = mutation.getFamilyCellMap().keySet();
          } else {
            deletesCfSetConsistent = deletesCfSetConsistent
                && mutation.getFamilyCellMap().keySet().equals(deletesCfSet);
          }
        }
      }

      // we should record the timestamp only after we have acquired the rowLock,
      // otherwise, newer puts/deletes are not guaranteed to have a newer timestamp
      now = EnvironmentEdgeManager.currentTimeMillis();
      byte[] byteNow = Bytes.toBytes(now);

      // Nothing to put/delete -- an exception in the above such as NoSuchColumnFamily?
      if (numReadyToWrite <= 0) return 0L;

      // We've now grabbed as many mutations off the list as we can

      // ------------------------------------
      // STEP 2. Update any LATEST_TIMESTAMP timestamps
      // ----------------------------------
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        // skip invalid
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) continue;

        Mutation mutation = batchOp.getMutation(i);
        if (mutation instanceof Put) {
          updateKVTimestamps(familyMaps[i].values(), byteNow);
          noOfPuts++;
        } else {
          if (!isInReplay) {
            prepareDeleteTimestamps(mutation, familyMaps[i], byteNow);
          }
          noOfDeletes++;
        }
        rewriteCellTags(familyMaps[i], mutation);
      }

      lock(this.updatesLock.readLock(), numReadyToWrite);
      locked = true;

      //
      // ------------------------------------
      // Acquire the latest mvcc number
      // ----------------------------------
      w = mvcc.beginMemstoreInsert();

      // calling the pre CP hook for batch mutation
      if (!isInReplay && coprocessorHost != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
          new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        if (coprocessorHost.preBatchMutate(miniBatchOp)) return 0L;
      }

      // ------------------------------------
      // STEP 3. Write back to memstore
      // Write to memstore. It is ok to write to memstore
      // first without updating the HLog because we do not roll
      // forward the memstore MVCC. The MVCC will be moved up when
      // the complete operation is done. These changes are not yet
      // visible to scanners till we update the MVCC. The MVCC is
      // moved only when the sync is complete.
      // ----------------------------------
      long addedSize = 0;
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) {
          continue;
        }
        doRollBackMemstore = true; // If we have a failure, we need to clean what we wrote
        addedSize += applyFamilyMapToMemstore(familyMaps[i], w);
      }

      // ------------------------------------
      // STEP 4. Build WAL edit
      // ----------------------------------
      boolean hasWalAppends = false;
      Durability durability = Durability.USE_DEFAULT;
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        // Skip puts that were determined to be invalid during preprocessing
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) {
          continue;
        }
        batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;

        Mutation m = batchOp.getMutation(i);
        Durability tmpDur = getEffectiveDurability(m.getDurability());
        if (tmpDur.ordinal() > durability.ordinal()) {
          durability = tmpDur;
        }
        if (tmpDur == Durability.SKIP_WAL) {
          recordMutationWithoutWal(m.getFamilyCellMap());
          continue;
        }

        long nonceGroup = batchOp.getNonceGroup(i), nonce = batchOp.getNonce(i);
        // In replay, the batch may contain multiple nonces. If so, write WALEdit for each.
        // Given how nonces are originally written, these should be contiguous.
        // They don't have to be, it will still work, just write more WALEdits than needed.
        if (nonceGroup != currentNonceGroup || nonce != currentNonce) {
          if (walEdit.size() > 0) {
            assert isInReplay;
            if (!isInReplay) {
              throw new IOException("Multiple nonces per batch and not in replay");
            }
            // txid should always increase, so having the one from the last call is ok.
            txid = this.log.appendNoSync(this.getRegionInfo(), htableDescriptor.getTableName(),
                  walEdit, m.getClusterIds(), now, htableDescriptor, this.sequenceId, true,
                  currentNonceGroup, currentNonce);
            hasWalAppends = true;
            walEdit = new WALEdit(isInReplay);
          }
          currentNonceGroup = nonceGroup;
          currentNonce = nonce;
        }

        // Add WAL edits by CP
        WALEdit fromCP = batchOp.walEditsFromCoprocessors[i];
        if (fromCP != null) {
          for (KeyValue kv : fromCP.getKeyValues()) {
            walEdit.add(kv);
          }
        }
        addFamilyMapToWALEdit(familyMaps[i], walEdit);
      }

      // -------------------------
      // STEP 5. Append the final edit to WAL. Do not sync wal.
      // -------------------------
      Mutation mutation = batchOp.getMutation(firstIndex);
      if (walEdit.size() > 0) {
        txid = this.log.appendNoSync(this.getRegionInfo(), this.htableDescriptor.getTableName(),
              walEdit, mutation.getClusterIds(), now, this.htableDescriptor, this.sequenceId,
              true, currentNonceGroup, currentNonce);
        hasWalAppends = true;
      }

      // -------------------------------
      // STEP 6. Release row locks, etc.
      // -------------------------------
      if (locked) {
        this.updatesLock.readLock().unlock();
        locked = false;
      }
      releaseRowLocks(acquiredRowLocks);

      // -------------------------
      // STEP 7. Sync wal.
      // -------------------------
      if (hasWalAppends) {
        syncOrDefer(txid, durability);
      }
      doRollBackMemstore = false;
      // calling the post CP hook for batch mutation
      if (!isInReplay && coprocessorHost != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
          new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        coprocessorHost.postBatchMutate(miniBatchOp);
      }

      // ------------------------------------------------------------------
      // STEP 8. Advance mvcc. This will make this put visible to scanners and getters.
      // ------------------------------------------------------------------
      if (w != null) {
        mvcc.completeMemstoreInsert(w);
        w = null;
      }

      // ------------------------------------
      // STEP 9. Run coprocessor post hooks. This should be done after the wal is
      // synced so that the coprocessor contract is adhered to.
      // ------------------------------------
      if (!isInReplay && coprocessorHost != null) {
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          // only for successful puts
          if (batchOp.retCodeDetails[i].getOperationStatusCode()
              != OperationStatusCode.SUCCESS) {
            continue;
          }
          Mutation m = batchOp.getMutation(i);
          if (m instanceof Put) {
            coprocessorHost.postPut((Put) m, walEdit, m.getDurability());
          } else {
            coprocessorHost.postDelete((Delete) m, walEdit, m.getDurability());
          }
        }
      }

      success = true;
      return addedSize;
    } finally {

      // if the wal sync was unsuccessful, remove keys from memstore
      if (doRollBackMemstore) {
        rollbackMemstore(batchOp, familyMaps, firstIndex, lastIndexExclusive);
      }
      if (w != null) mvcc.completeMemstoreInsert(w);

      if (locked) {
        this.updatesLock.readLock().unlock();
      }
      releaseRowLocks(acquiredRowLocks);

      // See if the column families were consistent through the whole thing.
      // if they were then keep them. If they were not then pass a null.
      // null will be treated as unknown.
      // Total time taken might be involving Puts and Deletes.
      // Split the time for puts and deletes based on the total number of Puts and Deletes.

      if (noOfPuts > 0) {
        // There were some Puts in the batch.
        if (this.metricsRegion != null) {
          this.metricsRegion.updatePut();
        }
      }
      if (noOfDeletes > 0) {
        // There were some Deletes in the batch.
        if (this.metricsRegion != null) {
          this.metricsRegion.updateDelete();
        }
      }
      if (!success) {
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          if (batchOp.retCodeDetails[i].getOperationStatusCode() == OperationStatusCode.NOT_RUN) {
            batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
          }
        }
      }
      if (coprocessorHost != null && !batchOp.isInReplay()) {
        // call the coprocessor hook to do any finalization steps
        // after the put is done
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
            new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
                batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex,
                lastIndexExclusive);
        coprocessorHost.postBatchMutateIndispensably(miniBatchOp, success);
      }

      batchOp.nextIndexToProcess = lastIndexExclusive;
    }
  }

  /**
   * Returns effective durability from the passed durability and
   * the table descriptor.
   */
  protected Durability getEffectiveDurability(Durability d) {
    return d == Durability.USE_DEFAULT ? this.durability : d;
  }

  //TODO, Think that gets/puts and deletes should be refactored a bit so that
  //the getting of the lock happens before, so that you would just pass it into
  //the methods. So in the case of checkAndMutate you could just do lockRow,
  //get, put, unlockRow or something
  /**
   *
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param w
   * @param writeToWAL
   * @throws IOException
   * @return true if the new put was executed, false otherwise
   */
  public boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, Mutation w,
      boolean writeToWAL)
  throws IOException{
    checkReadOnly();
    //TODO, add check for value length or maybe even better move this to the
    //client if this becomes a global setting
    checkResources();
    boolean isPut = w instanceof Put;
    if (!isPut && !(w instanceof Delete))
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action must " +
          "be Put or Delete");
    if (!Bytes.equals(row, w.getRow())) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action's " +
          "getRow must match the passed row");
    }

    startRegionOperation();
    try {
      Get get = new Get(row);
      checkFamily(family);
      get.addColumn(family, qualifier);

      // Lock row - note that doBatchMutate will relock this row if called
      RowLock rowLock = getRowLock(get.getRow());
      // wait for all previous transactions to complete (with lock held)
      mvcc.completeMemstoreInsert(mvcc.beginMemstoreInsert());
      try {
        if (this.getCoprocessorHost() != null) {
          Boolean processed = null;
          if (w instanceof Put) {
            processed = this.getCoprocessorHost().preCheckAndPutAfterRowLock(row, family,
                qualifier, compareOp, comparator, (Put) w);
          } else if (w instanceof Delete) {
            processed = this.getCoprocessorHost().preCheckAndDeleteAfterRowLock(row, family,
                qualifier, compareOp, comparator, (Delete) w);
          }
          if (processed != null) {
            return processed;
          }
        }
        List<Cell> result = get(get, false);

        boolean valueIsNull = comparator.getValue() == null ||
          comparator.getValue().length == 0;
        boolean matches = false;
        if (result.size() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 &&
            valueIsNull) {
          matches = true;
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          int compareResult = comparator.compareTo(kv.getValueArray(),
              kv.getValueOffset(), kv.getValueLength());
          switch (compareOp) {
          case LESS:
            matches = compareResult < 0;
            break;
          case LESS_OR_EQUAL:
            matches = compareResult <= 0;
            break;
          case EQUAL:
            matches = compareResult == 0;
            break;
          case NOT_EQUAL:
            matches = compareResult != 0;
            break;
          case GREATER_OR_EQUAL:
            matches = compareResult >= 0;
            break;
          case GREATER:
            matches = compareResult > 0;
            break;
          default:
            throw new RuntimeException("Unknown Compare op " + compareOp.name());
          }
        }
        //If matches put the new put or delete the new delete
        if (matches) {
          // All edits for the given row (across all column families) must
          // happen atomically.
          doBatchMutate((Mutation)w);
          this.checkAndMutateChecksPassed.increment();
          return true;
        }
        this.checkAndMutateChecksFailed.increment();
        return false;
      } finally {
        rowLock.release();
      }
    } finally {
      closeRegionOperation();
    }
  }

  //TODO, Think that gets/puts and deletes should be refactored a bit so that
  //the getting of the lock happens before, so that you would just pass it into
  //the methods. So in the case of checkAndMutate you could just do lockRow,
  //get, put, unlockRow or something
  /**
   *
   * @throws IOException
   * @return true if the new put was executed, false otherwise
   */
  public boolean checkAndRowMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, RowMutations rm,
      boolean writeToWAL)
      throws IOException{
    checkReadOnly();
    //TODO, add check for value length or maybe even better move this to the
    //client if this becomes a global setting
    checkResources();

    startRegionOperation();
    try {
      Get get = new Get(row);
      checkFamily(family);
      get.addColumn(family, qualifier);

      // Lock row - note that doBatchMutate will relock this row if called
      RowLock rowLock = getRowLock(get.getRow());
      // wait for all previous transactions to complete (with lock held)
      mvcc.completeMemstoreInsert(mvcc.beginMemstoreInsert());
      try {
        List<Cell> result = get(get, false);

        boolean valueIsNull = comparator.getValue() == null ||
            comparator.getValue().length == 0;
        boolean matches = false;
        if (result.size() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 &&
            valueIsNull) {
          matches = true;
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          int compareResult = comparator.compareTo(kv.getValueArray(),
              kv.getValueOffset(), kv.getValueLength());
          switch (compareOp) {
          case LESS:
            matches = compareResult < 0;
            break;
          case LESS_OR_EQUAL:
            matches = compareResult <= 0;
            break;
          case EQUAL:
            matches = compareResult == 0;
            break;
          case NOT_EQUAL:
            matches = compareResult != 0;
            break;
          case GREATER_OR_EQUAL:
            matches = compareResult >= 0;
            break;
          case GREATER:
            matches = compareResult > 0;
            break;
          default:
            throw new RuntimeException("Unknown Compare op " + compareOp.name());
          }
        }
        //If matches put the new put or delete the new delete
        if (matches) {
          // All edits for the given row (across all column families) must
          // happen atomically.
          mutateRow(rm);
          this.checkAndMutateChecksPassed.increment();
          return true;
        }
        this.checkAndMutateChecksFailed.increment();
        return false;
      } finally {
        rowLock.release();
      }
    } finally {
      closeRegionOperation();
    }
  }

  private void doBatchMutate(Mutation mutation) throws IOException, DoNotRetryIOException {
    // Currently this is only called for puts and deletes, so no nonces.
    OperationStatus[] batchMutate = this.batchMutate(new Mutation[] { mutation },
        HConstants.NO_NONCE, HConstants.NO_NONCE);
    if (batchMutate[0].getOperationStatusCode().equals(OperationStatusCode.SANITY_CHECK_FAILURE)) {
      throw new FailedSanityCheckException(batchMutate[0].getExceptionMsg());
    } else if (batchMutate[0].getOperationStatusCode().equals(OperationStatusCode.BAD_FAMILY)) {
      throw new NoSuchColumnFamilyException(batchMutate[0].getExceptionMsg());
    }
  }

  /**
   * Complete taking the snapshot on the region. Writes the region info and adds references to the
   * working snapshot directory.
   *
   * TODO for api consistency, consider adding another version with no {@link ForeignExceptionSnare}
   * arg.  (In the future other cancellable HRegion methods could eventually add a
   * {@link ForeignExceptionSnare}, or we could do something fancier).
   *
   * @param desc snasphot description object
   * @param exnSnare ForeignExceptionSnare that captures external exeptions in case we need to
   *   bail out.  This is allowed to be null and will just be ignored in that case.
   * @throws IOException if there is an external or internal error causing the snapshot to fail
   */
  public void addRegionToSnapshot(SnapshotDescription desc,
      ForeignExceptionSnare exnSnare) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir);

    SnapshotManifest manifest = SnapshotManifest.create(conf, getFilesystem(),
                                                        snapshotDir, desc, exnSnare);
    manifest.addRegion(this);
  }

  /**
   * Replaces any KV timestamps set to {@link HConstants#LATEST_TIMESTAMP} with the
   * provided current timestamp.
   */
  void updateKVTimestamps(final Iterable<List<Cell>> keyLists, final byte[] now) {
    for (List<Cell> cells: keyLists) {
      if (cells == null) continue;
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        kv.updateLatestStamp(now);
      }
    }
  }

  /**
   * Possibly rewrite incoming cell tags.
   */
  void rewriteCellTags(Map<byte[], List<Cell>> familyMap, final Mutation m) {
    // Check if we have any work to do and early out otherwise
    // Update these checks as more logic is added here

    if (m.getTTL() == Long.MAX_VALUE) {
      return;
    }

    // From this point we know we have some work to do

    for (Map.Entry<byte[], List<Cell>> e: familyMap.entrySet()) {
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
        Cell cell = cells.get(i);
        List<Tag> newTags = new ArrayList<Tag>();
        Iterator<Tag> tagIterator = CellUtil.tagsIterator(cell.getTagsArray(),
          cell.getTagsOffset(), cell.getTagsLengthUnsigned());

        // Carry forward existing tags

        while (tagIterator.hasNext()) {

          // Add any filters or tag specific rewrites here

          newTags.add(tagIterator.next());
        }

        // Cell TTL handling

        // Check again if we need to add a cell TTL because early out logic
        // above may change when there are more tag based features in core.
        if (m.getTTL() != Long.MAX_VALUE) {
          // Add a cell TTL tag
          newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(m.getTTL())));
        }

        // Rewrite the cell with the updated set of tags

        cells.set(i, new KeyValue(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
          cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
          cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
          newTags));
      }
    }
  }

  /*
   * Check if resources to support an update.
   *
   * We throw RegionTooBusyException if above memstore limit
   * and expect client to retry using some kind of backoff
  */
  private void checkResources()
    throws RegionTooBusyException {
    // If catalog region, do not impose resource constraints or block updates.
    if (this.getRegionInfo().isMetaRegion()) return;

    if (this.memstoreSize.get() > this.blockingMemStoreSize) {
      blockedRequestsCount.increment();
      requestFlush();
      throw new RegionTooBusyException("Above memstore limit, " +
          "regionName=" + (this.getRegionInfo() == null ? "unknown" :
          this.getRegionInfo().getRegionNameAsString()) +
          ", server=" + (this.getRegionServerServices() == null ? "unknown" :
          this.getRegionServerServices().getServerName()) +
          ", memstoreSize=" + memstoreSize.get() +
          ", blockingMemStoreSize=" + blockingMemStoreSize);
    }
  }

  /**
   * @throws IOException Throws exception if region is in read-only mode.
   */
  protected void checkReadOnly() throws IOException {
    if (this.writestate.isReadOnly()) {
      throw new IOException("region is read only");
    }
  }

  /**
   * Add updates first to the hlog and then add values to memstore.
   * Warning: Assumption is caller has lock on passed in row.
   * @param family
   * @param edits Cell updates by column
   * @praram now
   * @throws IOException
   */
  private void put(final byte [] row, byte [] family, List<Cell> edits)
  throws IOException {
    NavigableMap<byte[], List<Cell>> familyMap;
    familyMap = new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);

    familyMap.put(family, edits);
    Put p = new Put(row);
    p.setFamilyCellMap(familyMap);
    doBatchMutate(p);
  }

  /**
   * Atomically apply the given map of family->edits to the memstore.
   * This handles the consistency control on its own, but the caller
   * should already have locked updatesLock.readLock(). This also does
   * <b>not</b> check the families for validity.
   *
   * @param familyMap Map of kvs per family
   * @param localizedWriteEntry The WriteEntry of the MVCC for this transaction.
   *        If null, then this method internally creates a mvcc transaction.
   * @return the additional memory usage of the memstore caused by the
   * new entries.
   */
  private long applyFamilyMapToMemstore(Map<byte[], List<Cell>> familyMap,
    MultiVersionConsistencyControl.WriteEntry localizedWriteEntry) {
    long size = 0;
    boolean freemvcc = false;

    try {
      if (localizedWriteEntry == null) {
        localizedWriteEntry = mvcc.beginMemstoreInsert();
        freemvcc = true;
      }

      for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
        byte[] family = e.getKey();
        List<Cell> cells = e.getValue();
        assert cells instanceof RandomAccess;
        Store store = getStore(family);
        int listSize = cells.size();
        for (int i=0; i < listSize; i++) {
          Cell cell = cells.get(i);
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          kv.setMvccVersion(localizedWriteEntry.getWriteNumber());
          size += store.add(kv);
        }
      }
    } finally {
      if (freemvcc) {
        mvcc.completeMemstoreInsert(localizedWriteEntry);
      }
    }

     return size;
   }

  /**
   * Remove all the keys listed in the map from the memstore. This method is
   * called when a Put/Delete has updated memstore but subequently fails to update
   * the wal. This method is then invoked to rollback the memstore.
   */
  private void rollbackMemstore(BatchOperationInProgress<?> batchOp,
                                Map<byte[], List<Cell>>[] familyMaps,
                                int start, int end) {
    int kvsRolledback = 0;
    for (int i = start; i < end; i++) {
      // skip over request that never succeeded in the first place.
      if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.SUCCESS) {
        continue;
      }

      // Rollback all the kvs for this row.
      Map<byte[], List<Cell>> familyMap  = familyMaps[i];
      for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
        byte[] family = e.getKey();
        List<Cell> cells = e.getValue();

        // Remove those keys from the memstore that matches our
        // key's (row, cf, cq, timestamp, memstoreTS). The interesting part is
        // that even the memstoreTS has to match for keys that will be rolleded-back.
        Store store = getStore(family);
        for (Cell cell: cells) {
          store.rollback(KeyValueUtil.ensureKeyValue(cell));
          kvsRolledback++;
        }
      }
    }
    LOG.debug("rollbackMemstore rolled back " + kvsRolledback +
        " keyvalues from start:" + start + " to end:" + end);
  }

  /**
   * Check the collection of families for validity.
   * @throws NoSuchColumnFamilyException if a family does not exist.
   */
  void checkFamilies(Collection<byte[]> families)
  throws NoSuchColumnFamilyException {
    for (byte[] family : families) {
      checkFamily(family);
    }
  }

  /**
   * During replay, there could exist column families which are removed between region server
   * failure and replay
   */
  private void removeNonExistentColumnFamilyForReplay(
      final Map<byte[], List<Cell>> familyMap) {
    List<byte[]> nonExistentList = null;
    for (byte[] family : familyMap.keySet()) {
      if (!this.htableDescriptor.hasFamily(family)) {
        if (nonExistentList == null) {
          nonExistentList = new ArrayList<byte[]>();
        }
        nonExistentList.add(family);
      }
    }
    if (nonExistentList != null) {
      for (byte[] family : nonExistentList) {
        // Perhaps schema was changed between crash and replay
        LOG.info("No family for " + Bytes.toString(family) + " omit from reply.");
        familyMap.remove(family);
      }
    }
  }

  void checkTimestamps(final Map<byte[], List<Cell>> familyMap,
      long now) throws FailedSanityCheckException {
    if (timestampSlop == HConstants.LATEST_TIMESTAMP) {
      return;
    }
    long maxTs = now + timestampSlop;
    for (List<Cell> kvs : familyMap.values()) {
      assert kvs instanceof RandomAccess;
      int listSize  = kvs.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = kvs.get(i);
        // see if the user-side TS is out of range. latest = server-side
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        if (!kv.isLatestTimestamp() && kv.getTimestamp() > maxTs) {
          throw new FailedSanityCheckException("Timestamp for KV out of range "
              + cell + " (too.new=" + timestampSlop + ")");
        }
      }
    }
  }

  /**
   * Append the given map of family->edits to a WALEdit data structure.
   * This does not write to the HLog itself.
   * @param familyMap map of family->edits
   * @param walEdit the destination entry to append into
   */
  private void addFamilyMapToWALEdit(Map<byte[], List<Cell>> familyMap,
      WALEdit walEdit) {
    for (List<Cell> edits : familyMap.values()) {
      assert edits instanceof RandomAccess;
      int listSize = edits.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = edits.get(i);
        walEdit.add(KeyValueUtil.ensureKeyValue(cell));
      }
    }
  }

  private void requestFlush() {
    if (this.rsServices == null) {
      return;
    }
    synchronized (writestate) {
      if (this.writestate.isFlushRequested()) {
        return;
      }
      writestate.flushRequested = true;
    }
    // Make request outside of synchronize block; HBASE-818.
    this.rsServices.getFlushRequester().requestFlush(this);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flush requested on " + this);
    }
  }

  /*
   * @param size
   * @return True if size is over the flush threshold
   */
  private boolean isFlushSize(final long size) {
    return size > this.memstoreFlushSize;
  }

  /**
   * Read the edits log put under this region by wal log splitting process.  Put
   * the recovered edits back up into this region.
   *
   * <p>We can ignore any log message that has a sequence ID that's equal to or
   * lower than minSeqId.  (Because we know such log messages are already
   * reflected in the HFiles.)
   *
   * <p>While this is running we are putting pressure on memory yet we are
   * outside of our usual accounting because we are not yet an onlined region
   * (this stuff is being run as part of Region initialization).  This means
   * that if we're up against global memory limits, we'll not be flagged to flush
   * because we are not online. We can't be flushed by usual mechanisms anyways;
   * we're not yet online so our relative sequenceids are not yet aligned with
   * HLog sequenceids -- not till we come up online, post processing of split
   * edits.
   *
   * <p>But to help relieve memory pressure, at least manage our own heap size
   * flushing if are in excess of per-region limits.  Flushing, though, we have
   * to be careful and avoid using the regionserver/hlog sequenceid.  Its running
   * on a different line to whats going on in here in this region context so if we
   * crashed replaying these edits, but in the midst had a flush that used the
   * regionserver log with a sequenceid in excess of whats going on in here
   * in this region and with its split editlogs, then we could miss edits the
   * next time we go to recover. So, we have to flush inline, using seqids that
   * make sense in a this single region context only -- until we online.
   *
   * @param regiondir
   * @param maxSeqIdInStores Any edit found in split editlogs needs to be in excess of
   * the maxSeqId for the store to be applied, else its skipped.
   * @param reporter
   * @return the sequence id of the last edit added to this region out of the
   * recovered edits log or <code>minSeqId</code> if nothing added from editlogs.
   * @throws UnsupportedEncodingException
   * @throws IOException
   */
  protected long replayRecoveredEditsIfAny(final Path regiondir,
      Map<byte[], Long> maxSeqIdInStores,
      final CancelableProgressable reporter, final MonitoredTask status)
      throws UnsupportedEncodingException, IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqid = minSeqIdForTheRegion;

    FileSystem fs = this.fs.getFileSystem();
    NavigableSet<Path> files = HLogUtil.getSplitEditFilesSorted(fs, regiondir);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + (files == null ? 0 : files.size())
        + " recovered edits file(s) under " + regiondir);
    }

    if (files == null || files.isEmpty()) return seqid;

    for (Path edits: files) {
      if (edits == null || !fs.exists(edits)) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      if (isZeroLengthThenDelete(fs, edits)) continue;

      long maxSeqId;
      String fileName = edits.getName();
      maxSeqId = Math.abs(Long.parseLong(fileName));
      if (maxSeqId <= minSeqIdForTheRegion) {
        if (LOG.isDebugEnabled()) {
          String msg = "Maximum sequenceid for this log is " + maxSeqId
            + " and minimum sequenceid for the region is " + minSeqIdForTheRegion
            + ", skipped the whole file, path=" + edits;
          LOG.debug(msg);
        }
        continue;
      }

      try {
        // replay the edits. Replay can return -1 if everything is skipped, only update if seqId is greater
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter));
      } catch (IOException e) {
        boolean skipErrors = conf.getBoolean(
            HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
            conf.getBoolean(
                "hbase.skip.errors",
                HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS));
        if (conf.get("hbase.skip.errors") != null) {
          LOG.warn(
              "The property 'hbase.skip.errors' has been deprecated. Please use " +
              HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + " instead.");
        }
        if (skipErrors) {
          Path p = HLogUtil.moveAsideBadEditsFile(fs, edits);
          LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS
              + "=true so continuing. Renamed " + edits +
              " as " + p, e);
        } else {
          throw e;
        }
      }
    }
    // The edits size added into rsAccounting during this replaying will not
    // be required any more. So just clear it.
    if (this.rsAccounting != null) {
      this.rsAccounting.clearRegionReplayEditsSize(this.getRegionName());
    }
    if (seqid > minSeqIdForTheRegion) {
      // Then we added some edits to memory. Flush and cleanup split edit files.
      internalFlushcache(null, seqid, status);
    }
    // Now delete the content of recovered edits.  We're done w/ them.
    for (Path file: files) {
      if (!fs.delete(file, false)) {
        LOG.error("Failed delete of " + file);
      } else {
        LOG.debug("Deleted recovered.edits file=" + file);
      }
    }
    return seqid;
  }

  /*
   * @param edits File of recovered edits.
   * @param maxSeqIdInStores Maximum sequenceid found in each store.  Edits in log
   * must be larger than this to be replayed for each store.
   * @param reporter
   * @return the sequence id of the last edit added to this region out of the
   * recovered edits log or <code>minSeqId</code> if nothing added from editlogs.
   * @throws IOException
   */
  private long replayRecoveredEdits(final Path edits,
      Map<byte[], Long> maxSeqIdInStores, final CancelableProgressable reporter)
    throws IOException {
    String msg = "Replaying edits from " + edits;
    LOG.info(msg);
    MonitoredTask status = TaskMonitor.get().createStatus(msg);
    FileSystem fs = this.fs.getFileSystem();

    status.setStatus("Opening logs");
    HLog.Reader reader = null;
    try {
      reader = HLogFactory.createReader(fs, edits, conf);
      long currentEditSeqId = -1;
      long firstSeqIdInLog = -1;
      long skippedEdits = 0;
      long editsCount = 0;
      long intervalEdits = 0;
      HLog.Entry entry;
      Store store = null;
      boolean reported_once = false;
      ServerNonceManager ng = this.rsServices == null ? null : this.rsServices.getNonceManager();

      try {
        // How many edits seen before we check elapsed time
        int interval = this.conf.getInt("hbase.hstore.report.interval.edits",
            2000);
        // How often to send a progress report (default 1/2 master timeout)
        int period = this.conf.getInt("hbase.hstore.report.period",
          this.conf.getInt(AssignmentManager.ASSIGNMENT_TIMEOUT,
            AssignmentManager.DEFAULT_ASSIGNMENT_TIMEOUT_DEFAULT) / 2);
        long lastReport = EnvironmentEdgeManager.currentTimeMillis();

        while ((entry = reader.next()) != null) {
          HLogKey key = entry.getKey();
          WALEdit val = entry.getEdit();

          if (ng != null) { // some test, or nonces disabled
            ng.reportOperationFromWal(key.getNonceGroup(), key.getNonce(), key.getWriteTime());
          }

          if (reporter != null) {
            intervalEdits += val.size();
            if (intervalEdits >= interval) {
              // Number of edits interval reached
              intervalEdits = 0;
              long cur = EnvironmentEdgeManager.currentTimeMillis();
              if (lastReport + period <= cur) {
                status.setStatus("Replaying edits..." +
                    " skipped=" + skippedEdits +
                    " edits=" + editsCount);
                // Timeout reached
                if(!reporter.progress()) {
                  msg = "Progressable reporter failed, stopping replay";
                  LOG.warn(msg);
                  status.abort(msg);
                  throw new IOException(msg);
                }
                reported_once = true;
                lastReport = cur;
              }
            }
          }

          if (firstSeqIdInLog == -1) {
            firstSeqIdInLog = key.getLogSeqNum();
          }
          currentEditSeqId = key.getLogSeqNum();

          // Start coprocessor replay here. The coprocessor is for each WALEdit
          // instead of a KeyValue.
          if (coprocessorHost != null) {
            status.setStatus("Running pre-WAL-restore hook in coprocessors");
            if (coprocessorHost.preWALRestore(this.getRegionInfo(), key, val)) {
              // if bypass this log entry, ignore it ...
              continue;
            }
          }

          boolean flush = false;
          for (KeyValue kv: val.getKeyValues()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (kv.matchingFamily(WALEdit.METAFAMILY) ||
                !Bytes.equals(key.getEncodedRegionName(),
                  this.getRegionInfo().getEncodedNameAsBytes())) {
              //this is a special edit, we should handle it
              CompactionDescriptor compaction = WALEdit.getCompaction(kv);
              if (compaction != null) {
                //replay the compaction
                completeCompactionMarker(compaction);
              }

              skippedEdits++;
              continue;
            }
            // Figure which store the edit is meant for.
            if (store == null || !kv.matchingFamily(store.getFamily().getName())) {
              store = this.stores.get(kv.getFamily());
            }
            if (store == null) {
              // This should never happen.  Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for " + kv);
              skippedEdits++;
              continue;
            }
            // Now, figure if we should skip this edit.
            if (key.getLogSeqNum() <= maxSeqIdInStores.get(store.getFamily()
                .getName())) {
              skippedEdits++;
              continue;
            }
            // Once we are over the limit, restoreEdit will keep returning true to
            // flush -- but don't flush until we've played all the kvs that make up
            // the WALEdit.
            flush |= restoreEdit(store, kv);
            editsCount++;
          }
          if (flush) {
            internalFlushcache(null, currentEditSeqId, status);
          }

          if (coprocessorHost != null) {
            coprocessorHost.postWALRestore(this.getRegionInfo(), key, val);
          }
        }
      } catch (EOFException eof) {
        Path p = HLogUtil.moveAsideBadEditsFile(fs, edits);
        msg = "Encountered EOF. Most likely due to Master failure during " +
            "log spliting, so we have this data in another edit.  " +
            "Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = HLogUtil.moveAsideBadEditsFile(fs, edits);
          msg = "File corruption encountered!  " +
              "Continuing, but renaming " + edits + " as " + p;
          LOG.warn(msg, ioe);
          status.setStatus(msg);
        } else {
          status.abort(StringUtils.stringifyException(ioe));
          // other IO errors may be transient (bad network connection,
          // checksum exception on one datanode, etc).  throw & retry
          throw ioe;
        }
      }
      if (reporter != null && !reported_once) {
        reporter.progress();
      }
      msg = "Applied " + editsCount + ", skipped " + skippedEdits +
        ", firstSequenceidInLog=" + firstSeqIdInLog +
        ", maxSequenceidInLog=" + currentEditSeqId + ", path=" + edits;
      status.markComplete(msg);
      LOG.debug(msg);
      return currentEditSeqId;
    } finally {
      status.cleanup();
      if (reader != null) {
         reader.close();
      }
    }
  }

  /**
   * Call to complete a compaction. Its for the case where we find in the WAL a compaction
   * that was not finished.  We could find one recovering a WAL after a regionserver crash.
   * See HBASE-2331.
   * @param compaction
   */
  void completeCompactionMarker(CompactionDescriptor compaction)
      throws IOException {
    Store store = this.getStore(compaction.getFamilyName().toByteArray());
    if (store == null) {
      LOG.warn("Found Compaction WAL edit for deleted family:" +
          Bytes.toString(compaction.getFamilyName().toByteArray()));
      return;
    }
    store.completeCompactionMarker(compaction);
  }

  /**
   * Used by tests
   * @param s Store to add edit too.
   * @param kv KeyValue to add.
   * @return True if we should flush.
   */
  protected boolean restoreEdit(final Store s, final KeyValue kv) {
    long kvSize = s.add(kv);
    if (this.rsAccounting != null) {
      rsAccounting.addAndGetRegionReplayEditsSize(this.getRegionName(), kvSize);
    }
    return isFlushSize(this.addAndGetGlobalMemstoreSize(kvSize));
  }

  /*
   * @param fs
   * @param p File to check.
   * @return True if file was zero-length (and if so, we'll delete it in here).
   * @throws IOException
   */
  private static boolean isZeroLengthThenDelete(final FileSystem fs, final Path p)
      throws IOException {
    FileStatus stat = fs.getFileStatus(p);
    if (stat.getLen() > 0) return false;
    LOG.warn("File " + p + " is zero-length, deleting.");
    fs.delete(p, false);
    return true;
  }

  protected HStore instantiateHStore(final HColumnDescriptor family) throws IOException {
    return new HStore(this, family, this.conf);
  }

  /**
   * Return HStore instance.
   * Use with caution.  Exposed for use of fixup utilities.
   * @param column Name of column family hosted by this region.
   * @return Store that goes with the family on passed <code>column</code>.
   * TODO: Make this lookup faster.
   */
  public Store getStore(final byte[] column) {
    return this.stores.get(column);
  }

  public Map<byte[], Store> getStores() {
    return this.stores;
  }

  /**
   * Return list of storeFiles for the set of CFs.
   * Uses closeLock to prevent the race condition where a region closes
   * in between the for loop - closing the stores one by one, some stores
   * will return 0 files.
   * @return List of storeFiles.
   */
  public List<String> getStoreFileList(final byte [][] columns)
    throws IllegalArgumentException {
    List<String> storeFileNames = new ArrayList<String>();
    synchronized(closeLock) {
      for(byte[] column : columns) {
        Store store = this.stores.get(column);
        if (store == null) {
          throw new IllegalArgumentException("No column family : " +
              new String(column) + " available");
        }
        for (StoreFile storeFile: store.getStorefiles()) {
          storeFileNames.add(storeFile.getPath().toString());
        }
      }
    }
    return storeFileNames;
  }
  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  void checkRow(final byte [] row, String op) throws IOException {
    if (!rowIsInRange(getRegionInfo(), row)) {
      throw new WrongRegionException("Requested row out of range for " +
          op + " on HRegion " + this + ", startKey='" +
          Bytes.toStringBinary(getStartKey()) + "', getEndKey()='" +
          Bytes.toStringBinary(getEndKey()) + "', row='" +
          Bytes.toStringBinary(row) + "'");
    }
  }

  /**
   * Tries to acquire a lock on the given row.
   * @param waitForLock if true, will block until the lock is available.
   *        Otherwise, just tries to obtain the lock and returns
   *        false if unavailable.
   * @return the row lock if acquired,
   *   null if waitForLock was false and the lock was not acquired
   * @throws IOException if waitForLock was true and the lock could not be acquired after waiting
   */
  public RowLock getRowLock(byte[] row, boolean waitForLock) throws IOException {
    startRegionOperation();
    try {
      return getRowLockInternal(row, waitForLock);
    } finally {
      closeRegionOperation();
    }
  }

  /**
   * A version of getRowLock(byte[], boolean) to use when a region operation has already been
   * started (the calling thread has already acquired the region-close-lock).
   */
  protected RowLock getRowLockInternal(byte[] row, boolean waitForLock) throws IOException {
    checkRow(row, "row lock");
    HashedBytes rowKey = new HashedBytes(row);
    RowLockContext rowLockContext = new RowLockContext(rowKey);

    // loop until we acquire the row lock (unless !waitForLock)
    while (true) {
      RowLockContext existingContext = lockedRows.putIfAbsent(rowKey, rowLockContext);
      if (existingContext == null) {
        // Row is not already locked by any thread, use newly created context.
        break;
      } else if (existingContext.ownedByCurrentThread()) {
        // Row is already locked by current thread, reuse existing context instead.
        rowLockContext = existingContext;
        break;
      } else {
        // Row is already locked by some other thread, give up or wait for it
        if (!waitForLock) {
          return null;
        }
        try {
          if (!existingContext.latch.await(this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) {
            throw new IOException("Timed out waiting for lock for row: " + rowKey);
          }
        } catch (InterruptedException ie) {
          LOG.warn("Thread interrupted waiting for lock on row: " + rowKey);
          InterruptedIOException iie = new InterruptedIOException();
          iie.initCause(ie);
          throw iie;
        }
      }
    }

    // allocate new lock for this thread
    return rowLockContext.newLock();
  }

  /**
   * Acqures a lock on the given row.
   * The same thread may acquire multiple locks on the same row.
   * @return the acquired row lock
   * @throws IOException if the lock could not be acquired after waiting
   */
  public RowLock getRowLock(byte[] row) throws IOException {
    return getRowLock(row, true);
  }

  /**
   * If the given list of row locks is not null, releases all locks.
   */
  public void releaseRowLocks(List<RowLock> rowLocks) {
    if (rowLocks != null) {
      for (RowLock rowLock : rowLocks) {
        rowLock.release();
      }
      rowLocks.clear();
    }
  }

  /**
   * Determines whether multiple column families are present
   * Precondition: familyPaths is not null
   *
   * @param familyPaths List of Pair<byte[] column family, String hfilePath>
   */
  private static boolean hasMultipleColumnFamilies(
      List<Pair<byte[], String>> familyPaths) {
    boolean multipleFamilies = false;
    byte[] family = null;
    for (Pair<byte[], String> pair : familyPaths) {
      byte[] fam = pair.getFirst();
      if (family == null) {
        family = fam;
      } else if (!Bytes.equals(family, fam)) {
        multipleFamilies = true;
        break;
      }
    }
    return multipleFamilies;
  }


  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
                                boolean assignSeqId) throws IOException {
    return bulkLoadHFiles(familyPaths, assignSeqId, null);
  }

  /**
   * Attempts to atomically load a group of hfiles.  This is critical for loading
   * rows with multiple column families atomically.
   *
   * @param familyPaths List of Pair<byte[] column family, String hfilePath>
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a
   * file about to be bulk loaded
   * @param assignSeqId
   * @return true if successful, false if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths, boolean assignSeqId,
      BulkLoadListener bulkLoadListener) throws IOException {
    Preconditions.checkNotNull(familyPaths);
    // we need writeLock for multi-family bulk load
    startBulkRegionOperation(hasMultipleColumnFamilies(familyPaths));
    try {
      this.writeRequestsCount.increment();

      // There possibly was a split that happend between when the split keys
      // were gathered and before the HReiogn's write lock was taken.  We need
      // to validate the HFile region before attempting to bulk load all of them
      List<IOException> ioes = new ArrayList<IOException>();
      List<Pair<byte[], String>> failures = new ArrayList<Pair<byte[], String>>();
      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();

        Store store = getStore(familyName);
        if (store == null) {
          IOException ioe = new org.apache.hadoop.hbase.DoNotRetryIOException(
              "No such column family " + Bytes.toStringBinary(familyName));
          ioes.add(ioe);
        } else {
          try {
            store.assertBulkLoadHFileOk(new Path(path));
          } catch (WrongRegionException wre) {
            // recoverable (file doesn't fit in region)
            failures.add(p);
          } catch (IOException ioe) {
            // unrecoverable (hdfs problem)
            ioes.add(ioe);
          }
        }
      }

      // validation failed because of some sort of IO problem.
      if (ioes.size() != 0) {
        IOException e = MultipleIOException.createIOException(ioes);
        LOG.error("There were one or more IO errors when checking if the bulk load is ok.", e);
        throw e;
      }

      // validation failed, bail out before doing anything permanent.
      if (failures.size() != 0) {
        StringBuilder list = new StringBuilder();
        for (Pair<byte[], String> p : failures) {
          list.append("\n").append(Bytes.toString(p.getFirst())).append(" : ")
            .append(p.getSecond());
        }
        // problem when validating
        LOG.warn("There was a recoverable bulk load failure likely due to a" +
            " split.  These (family, HFile) pairs were not loaded: " + list);
        return false;
      }

      long seqId = -1;
      // We need to assign a sequential ID that's in between two memstores in order to preserve
      // the guarantee that all the edits lower than the highest sequential ID from all the
      // HFiles are flushed on disk. See HBASE-10958.
      if (assignSeqId) {
        FlushResult fs = this.flushcache();
        if (fs.isFlushSucceeded()) {
          seqId = fs.flushSequenceId;
        } else if (fs.result == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
          seqId = this.sequenceId.incrementAndGet();
        } else {
          throw new IOException("Could not bulk load with an assigned sequential ID because the " +
              "flush didn't run. Reason for not flushing: " + fs.failureReason);
        }
      }

      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();
        Store store = getStore(familyName);
        try {
          String finalPath = path;
          if(bulkLoadListener != null) {
            finalPath = bulkLoadListener.prepareBulkLoad(familyName, path);
          }
          store.bulkLoadHFile(finalPath, seqId);
          if(bulkLoadListener != null) {
            bulkLoadListener.doneBulkLoad(familyName, path);
          }
        } catch (IOException ioe) {
          // A failure here can cause an atomicity violation that we currently
          // cannot recover from since it is likely a failed HDFS operation.

          // TODO Need a better story for reverting partial failures due to HDFS.
          LOG.error("There was a partial failure due to IO when attempting to" +
              " load " + Bytes.toString(p.getFirst()) + " : "+ p.getSecond(), ioe);
          if(bulkLoadListener != null) {
            try {
              bulkLoadListener.failedBulkLoad(familyName, path);
            } catch (Exception ex) {
              LOG.error("Error while calling failedBulkLoad for family "+
                  Bytes.toString(familyName)+" with path "+path, ex);
            }
          }
          throw ioe;
        }
      }
      return true;
    } finally {
      closeBulkRegionOperation();
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof HRegion && Bytes.equals(this.getRegionName(),
                                                ((HRegion) o).getRegionName());
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.getRegionName());
  }

  @Override
  public String toString() {
    return this.getRegionNameAsString();
  }

  /**
   * RegionScannerImpl is used to combine scanners from multiple Stores (aka column families).
   */
  class RegionScannerImpl implements RegionScanner {
    // Package local for testability
    KeyValueHeap storeHeap = null;
    /** Heap of key-values that are not essential for the provided filters and are thus read
     * on demand, if on-demand column family loading is enabled.*/
    KeyValueHeap joinedHeap = null;
    /**
     * If the joined heap data gathering is interrupted due to scan limits, this will
     * contain the row for which we are populating the values.*/
    protected KeyValue joinedContinuationRow = null;
    // KeyValue indicating that limit is reached when scanning
    private final KeyValue KV_LIMIT = new KeyValue();
    protected final byte[] stopRow;
    private final FilterWrapper filter;
    private int batch;
    protected int isScan;
    private boolean filterClosed = false;
    private long readPt;
    private long maxResultSize;
    protected HRegion region;

    @Override
    public HRegionInfo getRegionInfo() {
      return region.getRegionInfo();
    }

    RegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region)
        throws IOException {

      this.region = region;
      this.maxResultSize = scan.getMaxResultSize();
      if (scan.hasFilter()) {
        this.filter = new FilterWrapper(scan.getFilter());
      } else {
        this.filter = null;
      }

      this.batch = scan.getBatch();
      if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) && !scan.isGetScan()) {
        this.stopRow = null;
      } else {
        this.stopRow = scan.getStopRow();
      }
      // If we are doing a get, we want to be [startRow,endRow] normally
      // it is [startRow,endRow) and if startRow=endRow we get nothing.
      this.isScan = scan.isGetScan() ? -1 : 0;

      // synchronize on scannerReadPoints so that nobody calculates
      // getSmallestReadPoint, before scannerReadPoints is updated.
      IsolationLevel isolationLevel = scan.getIsolationLevel();
      synchronized(scannerReadPoints) {
        this.readPt = getReadpoint(isolationLevel);
        scannerReadPoints.put(this, this.readPt);
      }

      // Here we separate all scanners into two lists - scanner that provide data required
      // by the filter to operate (scanners list) and all others (joinedScanners list).
      List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
      List<KeyValueScanner> joinedScanners = new ArrayList<KeyValueScanner>();
      if (additionalScanners != null) {
        scanners.addAll(additionalScanners);
      }

      for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
          scan.getFamilyMap().entrySet()) {
        Store store = stores.get(entry.getKey());
        KeyValueScanner scanner = store.getScanner(scan, entry.getValue(), this.readPt);
        if (this.filter == null || !scan.doLoadColumnFamiliesOnDemand()
          || this.filter.isFamilyEssential(entry.getKey())) {
          scanners.add(scanner);
        } else {
          joinedScanners.add(scanner);
        }
      }
      initializeKVHeap(scanners, joinedScanners, region);
    }

    RegionScannerImpl(Scan scan, HRegion region) throws IOException {
      this(scan, null, region);
    }

    protected void initializeKVHeap(List<KeyValueScanner> scanners,
        List<KeyValueScanner> joinedScanners, HRegion region)
        throws IOException {
      this.storeHeap = new KeyValueHeap(scanners, region.comparator);
      if (!joinedScanners.isEmpty()) {
        this.joinedHeap = new KeyValueHeap(joinedScanners, region.comparator);
      }
    }

    @Override
    public long getMaxResultSize() {
      return maxResultSize;
    }

    @Override
    public long getMvccReadPoint() {
      return this.readPt;
    }

    /**
     * Reset both the filter and the old filter.
     *
     * @throws IOException in case a filter raises an I/O exception.
     */
    protected void resetFilters() throws IOException {
      if (filter != null) {
        filter.reset();
      }
    }

    @Override
    public boolean next(List<Cell> outResults)
        throws IOException {
      // apply the batching limit by default
      return next(outResults, batch);
    }

    @Override
    public synchronized boolean next(List<Cell> outResults, int limit) throws IOException {
      if (this.filterClosed) {
        throw new UnknownScannerException("Scanner was closed (timed out?) " +
            "after we renewed it. Could be caused by a very slow scanner " +
            "or a lengthy garbage collection");
      }
      startRegionOperation(Operation.SCAN);
      readRequestsCount.increment();
      try {
        boolean returnResult = nextRaw(outResults, limit);
        if (region != null && region.metricsRegion != null) {
          long totalSize = 0;
          for (Cell cell: outResults) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            totalSize += kv.getLength();
          }
          region.metricsRegion.updateScanNext(totalSize);
        }
        return returnResult;
      } finally {
        closeRegionOperation(Operation.SCAN);
      }
    }

    @Override
    public boolean nextRaw(List<Cell> outResults)
        throws IOException {
      return nextRaw(outResults, batch);
    }

    @Override
    public boolean nextRaw(List<Cell> outResults, int limit) throws IOException {
      if (storeHeap == null) {
        // scanner is closed
        throw new UnknownScannerException("Scanner was closed");
      }
      boolean returnResult;
      if (outResults.isEmpty()) {
        // Usually outResults is empty. This is true when next is called
        // to handle scan or get operation.
        returnResult = nextInternal(outResults, limit);
      } else {
        List<Cell> tmpList = new ArrayList<Cell>();
        returnResult = nextInternal(tmpList, limit);
        outResults.addAll(tmpList);
      }
      resetFilters();
      if (isFilterDoneInternal()) {
        returnResult = false;
      }
      return returnResult;
    }

    private void populateFromJoinedHeap(List<Cell> results, int limit)
        throws IOException {
      assert joinedContinuationRow != null;
      KeyValue kv = populateResult(results, this.joinedHeap, limit,
          joinedContinuationRow.getBuffer(), joinedContinuationRow.getRowOffset(),
          joinedContinuationRow.getRowLength());
      if (kv != KV_LIMIT) {
        // We are done with this row, reset the continuation.
        joinedContinuationRow = null;
      }
      // As the data is obtained from two independent heaps, we need to
      // ensure that result list is sorted, because Result relies on that.
      Collections.sort(results, comparator);
    }

    /**
     * Fetches records with currentRow into results list, until next row or limit (if not -1).
     * @param results
     * @param heap KeyValueHeap to fetch data from.It must be positioned on correct row before call.
     * @param limit Max amount of KVs to place in result list, -1 means no limit.
     * @param currentRow Byte array with key we are fetching.
     * @param offset offset for currentRow
     * @param length length for currentRow
     * @return KV_LIMIT if limit reached, next KeyValue otherwise.
     */
    private KeyValue populateResult(List<Cell> results, KeyValueHeap heap, int limit,
        byte[] currentRow, int offset, short length) throws IOException {
      KeyValue nextKv;
      do {
        heap.next(results, limit - results.size());
        if (limit > 0 && results.size() == limit) {
          return KV_LIMIT;
        }
        nextKv = heap.peek();
      } while (nextKv != null && nextKv.matchingRow(currentRow, offset, length));

      return nextKv;
    }

    /*
     * @return True if a filter rules the scanner is over, done.
     */
    @Override
    public synchronized boolean isFilterDone() throws IOException {
      return isFilterDoneInternal();
    }

    private boolean isFilterDoneInternal() throws IOException {
      return this.filter != null && this.filter.filterAllRemaining();
    }

    private boolean nextInternal(List<Cell> results, int limit)
    throws IOException {
      if (!results.isEmpty()) {
        throw new IllegalArgumentException("First parameter should be an empty list");
      }
      RpcCallContext rpcCall = RpcServer.getCurrentCall();
      // The loop here is used only when at some point during the next we determine
      // that due to effects of filters or otherwise, we have an empty row in the result.
      // Then we loop and try again. Otherwise, we must get out on the first iteration via return,
      // "true" if there's more data to read, "false" if there isn't (storeHeap is at a stop row,
      // and joinedHeap has no more data to read for the last row (if set, joinedContinuationRow).
      while (true) {
        if (rpcCall != null) {
          // If a user specifies a too-restrictive or too-slow scanner, the
          // client might time out and disconnect while the server side
          // is still processing the request. We should abort aggressively
          // in that case.
          long afterTime = rpcCall.disconnectSince();
          if (afterTime >= 0) {
            throw new CallerDisconnectedException(
                "Aborting on region " + getRegionNameAsString() + ", call " +
                    this + " after " + afterTime + " ms, since " +
                    "caller disconnected");
          }
        }

        // Let's see what we have in the storeHeap.
        KeyValue current = this.storeHeap.peek();

        byte[] currentRow = null;
        int offset = 0;
        short length = 0;
        if (current != null) {
          currentRow = current.getBuffer();
          offset = current.getRowOffset();
          length = current.getRowLength();
        }
        boolean stopRow = isStopRow(currentRow, offset, length);
        // Check if we were getting data from the joinedHeap and hit the limit.
        // If not, then it's main path - getting results from storeHeap.
        if (joinedContinuationRow == null) {
          // First, check if we are at a stop row. If so, there are no more results.
          if (stopRow) {
            if (filter != null && filter.hasFilterRow()) {
              filter.filterRowCells(results);
            }
            return false;
          }

          // Check if rowkey filter wants to exclude this row. If so, loop to next.
          // Technically, if we hit limits before on this row, we don't need this call.
          if (filterRowKey(currentRow, offset, length)) {
            boolean moreRows = nextRow(currentRow, offset, length);
            if (!moreRows) return false;
            results.clear();
            continue;
          }

          KeyValue nextKv = populateResult(results, this.storeHeap, limit, currentRow, offset,
              length);
          // Ok, we are good, let's try to get some results from the main heap.
          if (nextKv == KV_LIMIT) {
            if (this.filter != null && filter.hasFilterRow()) {
              throw new IncompatibleFilterException(
                "Filter whose hasFilterRow() returns true is incompatible with scan with limit!");
            }
            return true; // We hit the limit.
          }

          stopRow = nextKv == null ||
              isStopRow(nextKv.getBuffer(), nextKv.getRowOffset(), nextKv.getRowLength());
          // save that the row was empty before filters applied to it.
          final boolean isEmptyRow = results.isEmpty();

          // We have the part of the row necessary for filtering (all of it, usually).
          // First filter with the filterRow(List).
          FilterWrapper.FilterRowRetCode ret = FilterWrapper.FilterRowRetCode.NOT_CALLED;
          if (filter != null && filter.hasFilterRow()) {
            ret = filter.filterRowCellsWithRet(results);
          }

          if ((isEmptyRow || ret == FilterWrapper.FilterRowRetCode.EXCLUDE) || filterRow()) {
            results.clear();
            boolean moreRows = nextRow(currentRow, offset, length);
            if (!moreRows) return false;

            // This row was totally filtered out, if this is NOT the last row,
            // we should continue on. Otherwise, nothing else to do.
            if (!stopRow) continue;
            return false;
          }

          // Ok, we are done with storeHeap for this row.
          // Now we may need to fetch additional, non-essential data into row.
          // These values are not needed for filter to work, so we postpone their
          // fetch to (possibly) reduce amount of data loads from disk.
          if (this.joinedHeap != null) {
            KeyValue nextJoinedKv = joinedHeap.peek();
            // If joinedHeap is pointing to some other row, try to seek to a correct one.
            boolean mayHaveData =
              (nextJoinedKv != null && nextJoinedKv.matchingRow(currentRow, offset, length))
              || (this.joinedHeap.requestSeek(KeyValue.createFirstOnRow(currentRow, offset, length),
                true, true)
                && joinedHeap.peek() != null
                && joinedHeap.peek().matchingRow(currentRow, offset, length));
            if (mayHaveData) {
              joinedContinuationRow = current;
              populateFromJoinedHeap(results, limit);
            }
          }
        } else {
          // Populating from the joined heap was stopped by limits, populate some more.
          populateFromJoinedHeap(results, limit);
        }

        // We may have just called populateFromJoinedMap and hit the limits. If that is
        // the case, we need to call it again on the next next() invocation.
        if (joinedContinuationRow != null) {
          return true;
        }

        // Finally, we are done with both joinedHeap and storeHeap.
        // Double check to prevent empty rows from appearing in result. It could be
        // the case when SingleColumnValueExcludeFilter is used.
        if (results.isEmpty()) {
          boolean moreRows = nextRow(currentRow, offset, length);
          if (!moreRows) return false;
          if (!stopRow) continue;
        }

        // We are done. Return the result.
        return !stopRow;
      }
    }

    /**
     * This function is to maintain backward compatibility for 0.94 filters. HBASE-6429 combines
     * both filterRow & filterRow(List<KeyValue> kvs) functions. While 0.94 code or older, it may
     * not implement hasFilterRow as HBase-6429 expects because 0.94 hasFilterRow() only returns
     * true when filterRow(List<KeyValue> kvs) is overridden not the filterRow(). Therefore, the
     * filterRow() will be skipped.
     */
    private boolean filterRow() throws IOException {
      // when hasFilterRow returns true, filter.filterRow() will be called automatically inside
      // filterRowCells(List<Cell> kvs) so we skip that scenario here.
      return filter != null && (!filter.hasFilterRow())
          && filter.filterRow();
    }

    private boolean filterRowKey(byte[] row, int offset, short length) throws IOException {
      return filter != null
          && filter.filterRowKey(row, offset, length);
    }

    protected boolean nextRow(byte [] currentRow, int offset, short length) throws IOException {
      assert this.joinedContinuationRow == null: "Trying to go to next row during joinedHeap read.";
      KeyValue next;
      while ((next = this.storeHeap.peek()) != null &&
             next.matchingRow(currentRow, offset, length)) {
        this.storeHeap.next(MOCKED_LIST);
      }
      resetFilters();
      // Calling the hook in CP which allows it to do a fast forward
      return this.region.getCoprocessorHost() == null
          || this.region.getCoprocessorHost()
              .postScannerFilterRow(this, currentRow, offset, length);
    }

    protected boolean isStopRow(byte[] currentRow, int offset, short length) {
      return currentRow == null ||
          (stopRow != null &&
          comparator.compareRows(stopRow, 0, stopRow.length,
            currentRow, offset, length) <= isScan);
    }

    @Override
    public synchronized void close() {
      if (storeHeap != null) {
        storeHeap.close();
        storeHeap = null;
      }
      if (joinedHeap != null) {
        joinedHeap.close();
        joinedHeap = null;
      }
      // no need to sychronize here.
      scannerReadPoints.remove(this);
      this.filterClosed = true;
    }

    KeyValueHeap getStoreHeapForTesting() {
      return storeHeap;
    }

    @Override
    public synchronized boolean reseek(byte[] row) throws IOException {
      if (row == null) {
        throw new IllegalArgumentException("Row cannot be null.");
      }
      boolean result = false;
      startRegionOperation();
      try {
        KeyValue kv = KeyValue.createFirstOnRow(row);
        // use request seek to make use of the lazy seek option. See HBASE-5520
        result = this.storeHeap.requestSeek(kv, true, true);
        if (this.joinedHeap != null) {
          result = this.joinedHeap.requestSeek(kv, true, true) || result;
        }
      } finally {
        closeRegionOperation();
      }
      return result;
    }
  }

  // Utility methods
  /**
   * A utility method to create new instances of HRegion based on the
   * {@link HConstants#REGION_IMPL} configuration property.
   * @param tableDir qualified path of directory where region should be located,
   * usually the table directory.
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param htd the table descriptor
   * @param rsServices
   * @return the new instance
   */
  static HRegion newHRegion(Path tableDir, HLog log, FileSystem fs,
      Configuration conf, HRegionInfo regionInfo, final HTableDescriptor htd,
      RegionServerServices rsServices) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
          (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, HRegion.class);

      Constructor<? extends HRegion> c =
          regionClass.getConstructor(Path.class, HLog.class, FileSystem.class,
              Configuration.class, HRegionInfo.class, HTableDescriptor.class,
              RegionServerServices.class);

      return c.newInstance(tableDir, log, fs, conf, regionInfo, htd, rsServices);
    } catch (Throwable e) {
      // todo: what should I throw here?
      throw new IllegalStateException("Could not instantiate a region instance.", e);
    }
  }

  /**
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor.
   * Note, this method creates an {@link HLog} for the created region. It
   * needs to be closed explicitly.  Use {@link HRegion#getLog()} to get
   * access.  <b>When done with a region created using this method, you will
   * need to explicitly close the {@link HLog} it created too; it will not be
   * done for you.  Not closing the log will leave at least a daemon thread
   * running.</b>  Call {@link #closeHRegion(HRegion)} and it will do
   * necessary cleanup for you.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param hTableDescriptor
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
      final Configuration conf, final HTableDescriptor hTableDescriptor)
  throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, null);
  }

  /**
   * This will do the necessary cleanup a call to
   * {@link #createHRegion(HRegionInfo, Path, Configuration, HTableDescriptor)}
   * requires.  This method will close the region and then close its
   * associated {@link HLog} file.  You use it if you call the other createHRegion,
   * the one that takes an {@link HLog} instance but don't be surprised by the
   * call to the {@link HLog#closeAndDelete()} on the {@link HLog} the
   * HRegion was carrying.
   * @param r
   * @throws IOException
   */
  public static void closeHRegion(final HRegion r) throws IOException {
    if (r == null) return;
    r.close();
    if (r.getLog() == null) return;
    r.getLog().closeAndDelete();
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link HLog} for the created region needs to be closed explicitly.
   * Use {@link HRegion#getLog()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param hTableDescriptor
   * @param hlog shared HLog
   * @param initialize - true to initialize the region
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final HLog hlog,
                                      final boolean initialize)
      throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor,
        hlog, initialize, false);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link HLog} for the created region needs to be closed
   * explicitly, if it is not null.
   * Use {@link HRegion#getLog()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param hTableDescriptor
   * @param hlog shared HLog
   * @param initialize - true to initialize the region
   * @param ignoreHLog - true to skip generate new hlog if it is null, mostly for createTable
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final HLog hlog,
                                      final boolean initialize, final boolean ignoreHLog)
      throws IOException {
      Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
      return createHRegion(info, rootDir, tableDir, conf, hTableDescriptor, hlog, initialize, ignoreHLog);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link HLog} for the created region needs to be closed
   * explicitly, if it is not null.
   * Use {@link HRegion#getLog()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param tableDir table directory
   * @param conf
   * @param hTableDescriptor
   * @param hlog shared HLog
   * @param initialize - true to initialize the region
   * @param ignoreHLog - true to skip generate new hlog if it is null, mostly for createTable
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir, final Path tableDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final HLog hlog,
                                      final boolean initialize, final boolean ignoreHLog)
      throws IOException {
    LOG.info("creating HRegion " + info.getTable().getNameAsString()
        + " HTD == " + hTableDescriptor + " RootDir = " + rootDir +
        " Table name == " + info.getTable().getNameAsString());
    FileSystem fs = FileSystem.get(conf);
    HRegionFileSystem rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, info);
    HLog effectiveHLog = hlog;
    if (hlog == null && !ignoreHLog) {
      effectiveHLog = HLogFactory.createHLog(fs, rfs.getRegionDir(),
                                             HConstants.HREGION_LOGDIR_NAME, conf);
    }
    HRegion region = HRegion.newHRegion(tableDir,
        effectiveHLog, fs, conf, info, hTableDescriptor, null);
    if (initialize) {
      // If initializing, set the sequenceId. It is also required by HLogPerformanceEvaluation when
      // verifying the WALEdits.
      region.setSequenceId(region.initialize());
    }
    return region;
  }

  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final HLog hlog)
    throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, hlog, true);
  }


  /**
   * Open a Region.
   * @param info Info for region to be opened.
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info,
      final HTableDescriptor htd, final HLog wal,
      final Configuration conf)
  throws IOException {
    return openHRegion(info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param info Info for region to be opened
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info,
    final HTableDescriptor htd, final HLog wal, final Configuration conf,
    final RegionServerServices rsServices,
    final CancelableProgressable reporter)
  throws IOException {
    return openHRegion(FSUtils.getRootDir(conf), info, htd, wal, conf, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(Path rootDir, final HRegionInfo info,
      final HTableDescriptor htd, final HLog wal, final Configuration conf)
  throws IOException {
    return openHRegion(rootDir, info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Path rootDir, final HRegionInfo info,
      final HTableDescriptor htd, final HLog wal, final Configuration conf,
      final RegionServerServices rsServices,
      final CancelableProgressable reporter)
  throws IOException {
    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    return openHRegion(conf, fs, rootDir, info, htd, wal, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final HRegionInfo info, final HTableDescriptor htd, final HLog wal)
      throws IOException {
    return openHRegion(conf, fs, rootDir, info, htd, wal, null, null);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final HRegionInfo info, final HTableDescriptor htd, final HLog wal,
      final RegionServerServices rsServices, final CancelableProgressable reporter)
      throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
    return openHRegion(conf, fs, rootDir, tableDir, info, htd, wal, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final Path tableDir, final HRegionInfo info, final HTableDescriptor htd, final HLog wal,
      final RegionServerServices rsServices, final CancelableProgressable reporter)
      throws IOException {
    if (info == null) throw new NullPointerException("Passed region info is null");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region: " + info);
    }
    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, rsServices);
    return r.openHRegion(reporter);
  }


  /**
   * Useful when reopening a closed region (normally for unit tests)
   * @param other original object
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegion other, final CancelableProgressable reporter)
      throws IOException {
    HRegionFileSystem regionFs = other.getRegionFileSystem();
    HRegion r = newHRegion(regionFs.getTableDir(), other.getLog(), regionFs.getFileSystem(),
        other.baseConf, other.getRegionInfo(), other.getTableDesc(), null);
    return r.openHRegion(reporter);
  }

  /**
   * Open HRegion.
   * Calls initialize and sets sequenceid.
   * @param reporter
   * @return Returns <code>this</code>
   * @throws IOException
   */
  protected HRegion openHRegion(final CancelableProgressable reporter)
  throws IOException {
    // Refuse to open the region if we are missing local compression support
    checkCompressionCodecs();
    // Refuse to open the region if encryption configuration is incorrect or
    // codec support is missing
    checkEncryption();
    // Refuse to open the region if a required class cannot be loaded
    checkClassLoading();
    this.openSeqNum = initialize(reporter);
    this.setSequenceId(openSeqNum);
    return this;
  }

  private void checkCompressionCodecs() throws IOException {
    for (HColumnDescriptor fam: this.htableDescriptor.getColumnFamilies()) {
      CompressionTest.testCompression(fam.getCompression());
      CompressionTest.testCompression(fam.getCompactionCompression());
    }
  }

  private void checkEncryption() throws IOException {
    for (HColumnDescriptor fam: this.htableDescriptor.getColumnFamilies()) {
      EncryptionTest.testEncryption(conf, fam.getEncryptionType(), fam.getEncryptionKey());
    }
  }

  private void checkClassLoading() throws IOException {
    RegionSplitPolicy.getSplitPolicyClass(this.htableDescriptor, conf);
    RegionCoprocessorHost.testTableCoprocessorAttrs(conf, this.htableDescriptor);
  }

  /**
   * Create a daughter region from given a temp directory with the region data.
   * @param hri Spec. for daughter region to open.
   * @throws IOException
   */
  HRegion createDaughterRegionFromSplits(final HRegionInfo hri) throws IOException {
    // Move the files from the temporary .splits to the final /table/region directory
    fs.commitDaughterRegion(hri);

    // Create the daughter HRegion instance
    HRegion r = HRegion.newHRegion(this.fs.getTableDir(), this.getLog(), fs.getFileSystem(),
        this.getBaseConf(), hri, this.getTableDesc(), rsServices);
    r.readRequestsCount.set(this.getReadRequestsCount() / 2);
    r.writeRequestsCount.set(this.getWriteRequestsCount() / 2);
    return r;
  }

  /**
   * Create a merged region given a temp directory with the region data.
   * @param mergedRegionInfo
   * @param region_b another merging region
   * @return merged hregion
   * @throws IOException
   */
  HRegion createMergedRegionFromMerges(final HRegionInfo mergedRegionInfo,
      final HRegion region_b) throws IOException {
    HRegion r = HRegion.newHRegion(this.fs.getTableDir(), this.getLog(),
        fs.getFileSystem(), this.getBaseConf(), mergedRegionInfo,
        this.getTableDesc(), this.rsServices);
    r.readRequestsCount.set(this.getReadRequestsCount()
        + region_b.getReadRequestsCount());
    r.writeRequestsCount.set(this.getWriteRequestsCount()

        + region_b.getWriteRequestsCount());
    this.fs.commitMergedRegion(mergedRegionInfo);
    return r;
  }

  /**
   * Inserts a new region's meta information into the passed
   * <code>meta</code> region. Used by the HMaster bootstrap code adding
   * new table to hbase:meta table.
   *
   * @param meta hbase:meta HRegion to be updated
   * @param r HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  // TODO remove since only test and merge use this
  public static void addRegionToMETA(final HRegion meta, final HRegion r) throws IOException {
    meta.checkResources();
    // The row key is the region name
    byte[] row = r.getRegionName();
    final long now = EnvironmentEdgeManager.currentTimeMillis();
    final List<Cell> cells = new ArrayList<Cell>(2);
    cells.add(new KeyValue(row, HConstants.CATALOG_FAMILY,
      HConstants.REGIONINFO_QUALIFIER, now,
      r.getRegionInfo().toByteArray()));
    // Set into the root table the version of the meta table.
    cells.add(new KeyValue(row, HConstants.CATALOG_FAMILY,
      HConstants.META_VERSION_QUALIFIER, now,
      Bytes.toBytes(HConstants.META_VERSION)));
    meta.put(row, HConstants.CATALOG_FAMILY, cells);
  }

  /**
   * Computes the Path of the HRegion
   *
   * @param tabledir qualified path for table
   * @param name ENCODED region name
   * @return Path of HRegion directory
   */
  @Deprecated
  public static Path getRegionDir(final Path tabledir, final String name) {
    return new Path(tabledir, name);
  }

  /**
   * Computes the Path of the HRegion
   *
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for the region
   * @return qualified path of region directory
   */
  @Deprecated
  public static Path getRegionDir(final Path rootdir, final HRegionInfo info) {
    return new Path(
      FSUtils.getTableDir(rootdir, info.getTable()), info.getEncodedName());
  }

  /**
   * Determines if the specified row is within the row range specified by the
   * specified HRegionInfo
   *
   * @param info HRegionInfo that specifies the row range
   * @param row row to be checked
   * @return true if the row is within the range specified by the HRegionInfo
   */
  public static boolean rowIsInRange(HRegionInfo info, final byte [] row) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), row) <= 0)) &&
        ((info.getEndKey().length == 0) ||
            (Bytes.compareTo(info.getEndKey(), row) > 0));
  }

  /**
   * Merge two HRegions.  The regions must be adjacent and must not overlap.
   *
   * @param srcA
   * @param srcB
   * @return new merged HRegion
   * @throws IOException
   */
  public static HRegion mergeAdjacent(final HRegion srcA, final HRegion srcB)
  throws IOException {
    HRegion a = srcA;
    HRegion b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    if (srcA.getStartKey() == null) {
      if (srcB.getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getStartKey() == null) ||
      (Bytes.compareTo(srcA.getStartKey(), srcB.getStartKey()) > 0)) {
      a = srcB;
      b = srcA;
    }

    if (!(Bytes.compareTo(a.getEndKey(), b.getStartKey()) == 0)) {
      throw new IOException("Cannot merge non-adjacent regions");
    }
    return merge(a, b);
  }

  /**
   * Merge two regions whether they are adjacent or not.
   *
   * @param a region a
   * @param b region b
   * @return new merged region
   * @throws IOException
   */
  public static HRegion merge(final HRegion a, final HRegion b) throws IOException {
    if (!a.getRegionInfo().getTable().equals(b.getRegionInfo().getTable())) {
      throw new IOException("Regions do not belong to the same table");
    }

    FileSystem fs = a.getRegionFileSystem().getFileSystem();
    // Make sure each region's cache is empty
    a.flushcache();
    b.flushcache();

    // Compact each region so we only have one store file per family
    a.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + a);
      a.getRegionFileSystem().logFileSystemState(LOG);
    }
    b.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + b);
      b.getRegionFileSystem().logFileSystemState(LOG);
    }

    RegionMergeTransaction rmt = new RegionMergeTransaction(a, b, true);
    if (!rmt.prepare(null)) {
      throw new IOException("Unable to merge regions " + a + " and " + b);
    }
    HRegionInfo mergedRegionInfo = rmt.getMergedRegionInfo();
    LOG.info("starting merge of regions: " + a + " and " + b
        + " into new region " + mergedRegionInfo.getRegionNameAsString()
        + " with start key <"
        + Bytes.toStringBinary(mergedRegionInfo.getStartKey())
        + "> and end key <"
        + Bytes.toStringBinary(mergedRegionInfo.getEndKey()) + ">");
    HRegion dstRegion;
    try {
      dstRegion = rmt.execute(null, null);
    } catch (IOException ioe) {
      rmt.rollback(null, null);
      throw new IOException("Failed merging region " + a + " and " + b
          + ", and succssfully rolled back");
    }
    dstRegion.compactStores(true);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      dstRegion.getRegionFileSystem().logFileSystemState(LOG);
    }

    if (dstRegion.getRegionFileSystem().hasReferences(dstRegion.getTableDesc())) {
      throw new IOException("Merged region " + dstRegion
          + " still has references after the compaction, is compaction canceled?");
    }

    // Archiving the 'A' region
    HFileArchiver.archiveRegion(a.getBaseConf(), fs, a.getRegionInfo());
    // Archiving the 'B' region
    HFileArchiver.archiveRegion(b.getBaseConf(), fs, b.getRegionInfo());

    LOG.info("merge completed. New region is " + dstRegion);
    return dstRegion;
  }

  /**
   * @return True if needs a major compaction.
   * @throws IOException
   */
  boolean isMajorCompaction() throws IOException {
    for (Store store : this.stores.values()) {
      if (store.isMajorCompaction()) {
        return true;
      }
    }
    return false;
  }

  //
  // HBASE-880
  //
  /**
   * @param get get object
   * @return result
   * @throws IOException read exceptions
   */
  public Result get(final Get get) throws IOException {
    checkRow(get.getRow(), "Get");
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte [] family: get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family: this.htableDescriptor.getFamiliesKeys()) {
        get.addFamily(family);
      }
    }
    List<Cell> results = get(get, true);
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null);
  }

  /*
   * Do a get based on the get parameter.
   * @param withCoprocessor invoke coprocessor or not. We don't want to
   * always invoke cp for this private method.
   */
  public List<Cell> get(Get get, boolean withCoprocessor)
  throws IOException {

    List<Cell> results = new ArrayList<Cell>();

    // pre-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
       if (coprocessorHost.preGet(get, results)) {
         return results;
       }
    }

    Scan scan = new Scan(get);

    RegionScanner scanner = null;
    try {
      scanner = getScanner(scan);
      scanner.next(results);
    } finally {
      if (scanner != null)
        scanner.close();
    }

    // post-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      coprocessorHost.postGet(get, results);
    }

    // do after lock
    if (this.metricsRegion != null) {
      long totalSize = 0l;
      if (results != null) {
        for (Cell kv:results) {
          totalSize += KeyValueUtil.ensureKeyValue(kv).getLength();
        }
      }
      this.metricsRegion.updateGet(totalSize);
    }

    return results;
  }

  public ClientProtos.RegionLoadStats mutateRow(RowMutations rm) throws IOException {
    // Don't need nonces here - RowMutations only supports puts and deletes
    return mutateRowsWithLocks(rm.getMutations(), Collections.singleton(rm.getRow()));
  }

  /**
   * Perform atomic mutations within the region w/o nonces.
   * See {@link #mutateRowsWithLocks(Collection, Collection, long, long)}
   */
  public ClientProtos.RegionLoadStats mutateRowsWithLocks(Collection<Mutation> mutations,
      Collection<byte[]> rowsToLock) throws IOException {
    return mutateRowsWithLocks(mutations, rowsToLock, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  /**
   * Perform atomic mutations within the region.
   * @param mutations The list of mutations to perform.
   * <code>mutations</code> can contain operations for multiple rows.
   * Caller has to ensure that all rows are contained in this region.
   * @param rowsToLock Rows to lock
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   * If multiple rows are locked care should be taken that
   * <code>rowsToLock</code> is sorted in order to avoid deadlocks.
   * @throws IOException
   */
  public ClientProtos.RegionLoadStats mutateRowsWithLocks(Collection<Mutation> mutations,
      Collection<byte[]> rowsToLock, long nonceGroup, long nonce) throws IOException {
    MultiRowMutationProcessor proc = new MultiRowMutationProcessor(mutations, rowsToLock);
    processRowsWithLocks(proc, -1, nonceGroup, nonce);
    return getRegionStats();
  }

  /**
   * @return the current load statistics for the the region
   */
  public ClientProtos.RegionLoadStats getRegionStats() {
    if (!regionStatsEnabled) {
      return null;
    }
    ClientProtos.RegionLoadStats.Builder stats = ClientProtos.RegionLoadStats.newBuilder();
    stats.setMemstoreLoad((int) (Math.min(100, (this.memstoreSize.get() * 100) / this
        .memstoreFlushSize)));
    stats.setHeapOccupancy((int)rsServices.getHeapMemoryManager().getHeapOccupancyPercent()*100);
    return stats.build();
  }

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   */
  public void processRowsWithLocks(RowProcessor<?,?> processor, long nonceGroup, long nonce)
      throws IOException {
    processRowsWithLocks(processor, rowProcessorTimeout, nonceGroup, nonce);
  }

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @param timeout The timeout of the processor.process() execution
   *                Use a negative number to switch off the time bound
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   */
  public void processRowsWithLocks(RowProcessor<?,?> processor, long timeout,
      long nonceGroup, long nonce) throws IOException {

    for (byte[] row : processor.getRowsToLock()) {
      checkRow(row, "processRowsWithLocks");
    }
    if (!processor.readOnly()) {
      checkReadOnly();
    }
    checkResources();

    startRegionOperation();
    WALEdit walEdit = new WALEdit();

    // 1. Run pre-process hook
    try {
      processor.preProcess(this, walEdit);
    } catch (IOException e) {
      closeRegionOperation();
      throw e;
    }
    // Short circuit the read only case
    if (processor.readOnly()) {
      try {
        long now = EnvironmentEdgeManager.currentTimeMillis();
        doProcessRowWithTimeout(
            processor, now, this, null, null, timeout);
        processor.postProcess(this, walEdit, true);
      } catch (IOException e) {
        throw e;
      } finally {
        closeRegionOperation();
      }
      return;
    }

    MultiVersionConsistencyControl.WriteEntry writeEntry = null;
    boolean locked = false;
    boolean walSyncSuccessful = false;
    List<RowLock> acquiredRowLocks = null;
    long addedSize = 0;
    List<Mutation> mutations = new ArrayList<Mutation>();
    Collection<byte[]> rowsToLock = processor.getRowsToLock();
    try {
      // 2. Acquire the row lock(s)
      acquiredRowLocks = new ArrayList<RowLock>(rowsToLock.size());
      for (byte[] row : rowsToLock) {
        // Attempt to lock all involved rows, throw if any lock times out
        acquiredRowLocks.add(getRowLock(row));
      }
      // 3. Region lock
      lock(this.updatesLock.readLock(), acquiredRowLocks.size() == 0 ? 1 : acquiredRowLocks.size());
      locked = true;

      long now = EnvironmentEdgeManager.currentTimeMillis();
      try {
        // 4. Let the processor scan the rows, generate mutations and add
        //    waledits
        doProcessRowWithTimeout(
            processor, now, this, mutations, walEdit, timeout);

        if (!mutations.isEmpty()) {
          // 5. Get a mvcc write number
          writeEntry = mvcc.beginMemstoreInsert();
          // 6. Call the preBatchMutate hook
          processor.preBatchMutate(this, walEdit);
          // 7. Apply to memstore
          for (Mutation m : mutations) {
            // Handle any tag based cell features
            rewriteCellTags(m.getFamilyCellMap(), m);

            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cellScanner.current());
              kv.setMvccVersion(writeEntry.getWriteNumber());
              byte[] family = kv.getFamily();
              checkFamily(family);
              addedSize += stores.get(family).add(kv);
            }
          }

          long txid = 0;
          // 8. Append no sync
          if (!walEdit.isEmpty()) {
            txid = this.log.appendNoSync(this.getRegionInfo(),
              this.htableDescriptor.getTableName(), walEdit, processor.getClusterIds(), now,
              this.htableDescriptor, this.sequenceId, true, nonceGroup, nonce);
          }
          // 9. Release region lock
          if (locked) {
            this.updatesLock.readLock().unlock();
            locked = false;
          }

          // 10. Release row lock(s)
          releaseRowLocks(acquiredRowLocks);

          // 11. Sync edit log
          if (txid != 0) {
            syncOrDefer(txid, getEffectiveDurability(processor.useDurability()));
          }
          walSyncSuccessful = true;
          // 12. call postBatchMutate hook
          processor.postBatchMutate(this);
        }
      } finally {
        if (!mutations.isEmpty() && !walSyncSuccessful) {
          LOG.warn("Wal sync failed. Roll back " + mutations.size() +
              " memstore keyvalues for row(s):" +
              processor.getRowsToLock().iterator().next() + "...");
          for (Mutation m : mutations) {
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cellScanner.current());
              stores.get(kv.getFamily()).rollback(kv);
            }
          }
        }
        // 13. Roll mvcc forward
        if (writeEntry != null) {
          mvcc.completeMemstoreInsert(writeEntry);
          writeEntry = null;
        }
        if (locked) {
          this.updatesLock.readLock().unlock();
          locked = false;
        }
        // release locks if some were acquired but another timed out
        releaseRowLocks(acquiredRowLocks);
      }

      // 14. Run post-process hook
      processor.postProcess(this, walEdit, walSyncSuccessful);

    } catch (IOException e) {
      throw e;
    } finally {
      closeRegionOperation();
      if (!mutations.isEmpty() &&
          isFlushSize(this.addAndGetGlobalMemstoreSize(addedSize))) {
        requestFlush();
      }
    }
  }

  private void doProcessRowWithTimeout(final RowProcessor<?,?> processor,
                                       final long now,
                                       final HRegion region,
                                       final List<Mutation> mutations,
                                       final WALEdit walEdit,
                                       final long timeout) throws IOException {
    // Short circuit the no time bound case.
    if (timeout < 0) {
      try {
        processor.process(now, region, mutations, walEdit);
      } catch (IOException e) {
        LOG.warn("RowProcessor:" + processor.getClass().getName() +
            " throws Exception on row(s):" +
            Bytes.toStringBinary(
              processor.getRowsToLock().iterator().next()) + "...", e);
        throw e;
      }
      return;
    }

    // Case with time bound
    FutureTask<Void> task =
      new FutureTask<Void>(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          try {
            processor.process(now, region, mutations, walEdit);
            return null;
          } catch (IOException e) {
            LOG.warn("RowProcessor:" + processor.getClass().getName() +
                " throws Exception on row(s):" +
                Bytes.toStringBinary(
                    processor.getRowsToLock().iterator().next()) + "...", e);
            throw e;
          }
        }
      });
    rowProcessorExecutor.execute(task);
    try {
      task.get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      LOG.error("RowProcessor timeout:" + timeout + " ms on row(s):" +
          Bytes.toStringBinary(processor.getRowsToLock().iterator().next()) +
          "...");
      throw new IOException(te);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Result append(Append append) throws IOException {
    return append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  // TODO: There's a lot of boiler plate code identical to increment.
  // We should refactor append and increment as local get-mutate-put
  // transactions, so all stores only go through one code path for puts.
  /**
   * Perform one or more append operations on a row.
   *
   * @param append
   * @return new keyvalues after increment
   * @throws IOException
   */
  public Result append(Append append, long nonceGroup, long nonce)
      throws IOException {
    byte[] row = append.getRow();
    checkRow(row, "append");
    boolean flush = false;
    Durability durability = getEffectiveDurability(append.getDurability());
    boolean writeToWAL = durability != Durability.SKIP_WAL;
    WALEdit walEdits = null;
    List<Cell> allKVs = new ArrayList<Cell>(append.size());
    Map<Store, List<Cell>> tempMemstore = new HashMap<Store, List<Cell>>();

    long size = 0;
    long txid = 0;

    checkReadOnly();
    checkResources();
    // Lock row
    startRegionOperation(Operation.APPEND);
    this.writeRequestsCount.increment();
    WriteEntry w = null;
    RowLock rowLock;
    try {
      rowLock = getRowLock(row);
      try {
        lock(this.updatesLock.readLock());
        try {
          // wait for all prior MVCC transactions to finish - while we hold the row lock
          // (so that we are guaranteed to see the latest state)
          mvcc.completeMemstoreInsert(mvcc.beginMemstoreInsert());
          if (this.coprocessorHost != null) {
            Result r = this.coprocessorHost.preAppendAfterRowLock(append);
            if(r!= null) {
              return r;
            }
          }
          // now start my own transaction
          w = mvcc.beginMemstoreInsert();
          long now = EnvironmentEdgeManager.currentTimeMillis();
          // Process each family
          for (Map.Entry<byte[], List<Cell>> family : append.getFamilyCellMap().entrySet()) {

            Store store = stores.get(family.getKey());
            List<Cell> kvs = new ArrayList<Cell>(family.getValue().size());

            // Sort the cells so that they match the order that they
            // appear in the Get results. Otherwise, we won't be able to
            // find the existing values if the cells are not specified
            // in order by the client since cells are in an array list.
            Collections.sort(family.getValue(), store.getComparator());
            // Get previous values for all columns in this family
            Get get = new Get(row);
            for (Cell cell : family.getValue()) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
              get.addColumn(family.getKey(), kv.getQualifier());
            }
            List<Cell> results = get(get, false);

            // Iterate the input columns and update existing values if they were
            // found, otherwise add new column initialized to the append value

            // Avoid as much copying as possible. We may need to rewrite and
            // consolidate tags. Bytes are only copied once.
            // Would be nice if KeyValue had scatter/gather logic
            int idx = 0;
            for (Cell cell : family.getValue()) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
              KeyValue newKv;
              KeyValue oldKv = null;
              if (idx < results.size()
                  && CellUtil.matchingQualifier(results.get(idx), kv)) {
                oldKv = KeyValueUtil.ensureKeyValue(results.get(idx));
                long ts = Math.max(now, oldKv.getTimestamp());

                // Process cell tags
                List<Tag> newTags = new ArrayList<Tag>();

                // Make a union of the set of tags in the old and new KVs

                if (oldKv.getTagsLengthUnsigned() > 0) {
                  Iterator<Tag> i = CellUtil.tagsIterator(oldKv.getTagsArray(),
                    oldKv.getTagsOffset(), oldKv.getTagsLengthUnsigned());
                  while (i.hasNext()) {
                    newTags.add(i.next());
                  }
                }
                if (kv.getTagsLengthUnsigned() > 0) {
                  Iterator<Tag> i  = CellUtil.tagsIterator(kv.getTagsArray(), kv.getTagsOffset(),
                    kv.getTagsLengthUnsigned());
                  while (i.hasNext()) {
                    newTags.add(i.next());
                  }
                }

                // Cell TTL handling

                if (append.getTTL() != Long.MAX_VALUE) {
                  // Add the new TTL tag
                  newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(append.getTTL())));
                }

                // Rebuild tags
                byte[] tagBytes = Tag.fromList(newTags);

                // allocate an empty cell once
                newKv = new KeyValue(row.length, kv.getFamilyLength(),
                    kv.getQualifierLength(), ts, KeyValue.Type.Put,
                    oldKv.getValueLength() + kv.getValueLength(),
                    tagBytes.length);
                // copy in row, family, and qualifier
                System.arraycopy(kv.getRowArray(), kv.getRowOffset(),
                  newKv.getRowArray(), newKv.getRowOffset(), kv.getRowLength());
                System.arraycopy(kv.getFamilyArray(), kv.getFamilyOffset(),
                  newKv.getFamilyArray(), newKv.getFamilyOffset(),
                  kv.getFamilyLength());
                System.arraycopy(kv.getQualifierArray(), kv.getQualifierOffset(),
                  newKv.getQualifierArray(), newKv.getQualifierOffset(),
                  kv.getQualifierLength());
                // copy in the value
                System.arraycopy(oldKv.getValueArray(), oldKv.getValueOffset(),
                  newKv.getValueArray(), newKv.getValueOffset(),
                  oldKv.getValueLength());
                System.arraycopy(kv.getValueArray(), kv.getValueOffset(),
                  newKv.getValueArray(),
                  newKv.getValueOffset() + oldKv.getValueLength(),
                  kv.getValueLength());
                // Copy in tag data
                System.arraycopy(tagBytes, 0, newKv.getTagsArray(), newKv.getTagsOffset(),
                  tagBytes.length);
                idx++;
              } else {
                // Append's KeyValue.Type==Put and ts==HConstants.LATEST_TIMESTAMP,
                // so only need to update the timestamp to 'now'
                kv.updateLatestStamp(Bytes.toBytes(now));

                // Cell TTL handling

                if (append.getTTL() != Long.MAX_VALUE) {
                  List<Tag> newTags = new ArrayList<Tag>(1);
                  newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(append.getTTL())));
                  // Add the new TTL tag
                  newKv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                    kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
                    kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                    kv.getTimestamp(), KeyValue.Type.codeToType(kv.getTypeByte()),
                    kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
                    newTags);
                } else {
                  newKv = kv;
                }
              }
              newKv.setMvccVersion(w.getWriteNumber());

              // Give coprocessors a chance to update the new cell
              if (coprocessorHost != null) {
                newKv = KeyValueUtil.ensureKeyValue(coprocessorHost.postMutationBeforeWAL(
                    RegionObserver.MutationType.APPEND, append, oldKv, (Cell) newKv));
              }
              kvs.add(newKv);

              // Append update to WAL
              if (writeToWAL) {
                if (walEdits == null) {
                  walEdits = new WALEdit();
                }
                walEdits.add(newKv);
              }
            }

            //store the kvs to the temporary memstore before writing HLog
            tempMemstore.put(store, kvs);
          }

          // Actually write to WAL now
          if (writeToWAL) {
            // Using default cluster id, as this can only happen in the orginating
            // cluster. A slave cluster receives the final value (not the delta)
            // as a Put.
            txid = this.log.appendNoSync(this.getRegionInfo(),
              this.htableDescriptor.getTableName(), walEdits, new ArrayList<UUID>(),
              EnvironmentEdgeManager.currentTimeMillis(), this.htableDescriptor, this.sequenceId,
              true, nonceGroup, nonce);
          } else {
            recordMutationWithoutWal(append.getFamilyCellMap());
          }

          //Actually write to Memstore now
          for (Map.Entry<Store, List<Cell>> entry : tempMemstore.entrySet()) {
            Store store = entry.getKey();
            if (store.getFamily().getMaxVersions() == 1) {
              // upsert if VERSIONS for this CF == 1
              size += store.upsert(entry.getValue(), getSmallestReadPoint());
            } else {
              // otherwise keep older versions around
              for (Cell cell: entry.getValue()) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                size += store.add(kv);
              }
            }
            allKVs.addAll(entry.getValue());
          }
          size = this.addAndGetGlobalMemstoreSize(size);
          flush = isFlushSize(size);
        } finally {
          this.updatesLock.readLock().unlock();
        }
      } finally {
        rowLock.release();
      }
      if (writeToWAL) {
        // sync the transaction log outside the rowlock
        syncOrDefer(txid, durability);
      }
    } finally {
      if (w != null) {
        mvcc.completeMemstoreInsert(w);
      }
      closeRegionOperation(Operation.APPEND);
    }

    if (this.metricsRegion != null) {
      this.metricsRegion.updateAppend();
    }

    if (flush) {
      // Request a cache flush. Do it outside update lock.
      requestFlush();
    }


    return append.isReturnResults() ? Result.create(allKVs) : null;
  }

  public Result increment(Increment increment) throws IOException {
    return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  // TODO: There's a lot of boiler plate code identical to append.
  // We should refactor append and increment as local get-mutate-put
  // transactions, so all stores only go through one code path for puts.
  /**
   * Perform one or more increment operations on a row.
   * @param increment
   * @return new keyvalues after increment
   * @throws IOException
   */
  public Result increment(Increment increment, long nonceGroup, long nonce)
  throws IOException {
    byte [] row = increment.getRow();
    checkRow(row, "increment");
    TimeRange tr = increment.getTimeRange();
    boolean flush = false;
    Durability durability = getEffectiveDurability(increment.getDurability());
    boolean writeToWAL = durability != Durability.SKIP_WAL;
    WALEdit walEdits = null;
    List<Cell> allKVs = new ArrayList<Cell>(increment.size());
    Map<Store, List<Cell>> tempMemstore = new HashMap<Store, List<Cell>>();

    long size = 0;
    long txid = 0;

    checkReadOnly();
    checkResources();
    // Lock row
    startRegionOperation(Operation.INCREMENT);
    this.writeRequestsCount.increment();
    WriteEntry w = null;
    try {
      RowLock rowLock = getRowLock(row);
      try {
        lock(this.updatesLock.readLock());
        try {
          // wait for all prior MVCC transactions to finish - while we hold the row lock
          // (so that we are guaranteed to see the latest state)
          mvcc.completeMemstoreInsert(mvcc.beginMemstoreInsert());
          if (this.coprocessorHost != null) {
            Result r = this.coprocessorHost.preIncrementAfterRowLock(increment);
            if (r != null) {
              return r;
            }
          }
          // now start my own transaction
          w = mvcc.beginMemstoreInsert();
          long now = EnvironmentEdgeManager.currentTimeMillis();
          // Process each family
          for (Map.Entry<byte [], List<Cell>> family:
              increment.getFamilyCellMap().entrySet()) {

            Store store = stores.get(family.getKey());
            List<Cell> kvs = new ArrayList<Cell>(family.getValue().size());

            // Sort the cells so that they match the order that they
            // appear in the Get results. Otherwise, we won't be able to
            // find the existing values if the cells are not specified
            // in order by the client since cells are in an array list.
            Collections.sort(family.getValue(), store.getComparator());
            // Get previous values for all columns in this family
            Get get = new Get(row);
            for (Cell cell: family.getValue()) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
              get.addColumn(family.getKey(), kv.getQualifier());
            }
            get.setTimeRange(tr.getMin(), tr.getMax());
            List<Cell> results = get(get, false);

            // Iterate the input columns and update existing values if they were
            // found, otherwise add new column initialized to the increment amount
            int idx = 0;
            for (Cell cell: family.getValue()) {
              long amount = Bytes.toLong(CellUtil.cloneValue(cell));
              boolean noWriteBack = (amount == 0);
              List<Tag> newTags = new ArrayList<Tag>();

              // Carry forward any tags that might have been added by a coprocessor
              if (cell.getTagsLengthUnsigned() > 0) {
                Iterator<Tag> i = CellUtil.tagsIterator(cell.getTagsArray(),
                  cell.getTagsOffset(), cell.getTagsLengthUnsigned());
                while (i.hasNext()) {
                  newTags.add(i.next());
                }
              }

              Cell c = null;
              long ts = now;
              if (idx < results.size() && CellUtil.matchingQualifier(results.get(idx), cell)) {
                c = results.get(idx);
                ts = Math.max(now, c.getTimestamp());
                if(c.getValueLength() == Bytes.SIZEOF_LONG) {
                  amount += Bytes.toLong(c.getValueArray(), c.getValueOffset(), Bytes.SIZEOF_LONG);
                } else {
                  // throw DoNotRetryIOException instead of IllegalArgumentException
                  throw new org.apache.hadoop.hbase.DoNotRetryIOException(
                      "Attempted to increment field that isn't 64 bits wide");
                }
                // Carry tags forward from previous version
                if (c.getTagsLength() > 0) {
                  Iterator<Tag> i = CellUtil.tagsIterator(c.getTagsArray(),
                    c.getTagsOffset(), c.getTagsLength());
                  while (i.hasNext()) {
                    newTags.add(i.next());
                  }
                }
                idx++;
              }

              // Append new incremented KeyValue to list
              byte[] q = CellUtil.cloneQualifier(cell);
              byte[] val = Bytes.toBytes(amount);

              // Add the TTL tag if the mutation carried one
              if (increment.getTTL() != Long.MAX_VALUE) {
                newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(increment.getTTL())));
              }

              KeyValue newKv = new KeyValue(row, 0, row.length,
                family.getKey(), 0, family.getKey().length,
                q, 0, q.length,
                ts,
                KeyValue.Type.Put,
                val, 0, val.length,
                newTags);

              newKv.setMvccVersion(w.getWriteNumber());

              // Give coprocessors a chance to update the new cell
              if (coprocessorHost != null) {
                newKv = KeyValueUtil.ensureKeyValue(coprocessorHost.postMutationBeforeWAL(
                    RegionObserver.MutationType.INCREMENT, increment, c, (Cell) newKv));
              }
              allKVs.add(newKv);

              if (!noWriteBack) {
                kvs.add(newKv);

                // Prepare WAL updates
                if (writeToWAL) {
                  if (walEdits == null) {
                    walEdits = new WALEdit();
                  }
                  walEdits.add(newKv);
                }
              }
            }

            //store the kvs to the temporary memstore before writing HLog
            if (!kvs.isEmpty()) {
              tempMemstore.put(store, kvs);
            }
          }

          // Actually write to WAL now
          if (walEdits != null && !walEdits.isEmpty()) {
            if (writeToWAL) {
              // Using default cluster id, as this can only happen in the orginating
              // cluster. A slave cluster receives the final value (not the delta)
              // as a Put.
              txid = this.log.appendNoSync(this.getRegionInfo(),
                  this.htableDescriptor.getTableName(), walEdits, new ArrayList<UUID>(),
                  EnvironmentEdgeManager.currentTimeMillis(), this.htableDescriptor, this.sequenceId,
                  true, nonceGroup, nonce);
            } else {
              recordMutationWithoutWal(increment.getFamilyCellMap());
            }
          }
          //Actually write to Memstore now
          if (!tempMemstore.isEmpty()) {
            for (Map.Entry<Store, List<Cell>> entry : tempMemstore.entrySet()) {
              Store store = entry.getKey();
              if (store.getFamily().getMaxVersions() == 1) {
                // upsert if VERSIONS for this CF == 1
                size += store.upsert(entry.getValue(), getSmallestReadPoint());
              } else {
                // otherwise keep older versions around
                for (Cell cell : entry.getValue()) {
                  KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                  size += store.add(kv);
                }
              }
            }
            size = this.addAndGetGlobalMemstoreSize(size);
            flush = isFlushSize(size);
          }
        } finally {
          this.updatesLock.readLock().unlock();
        }
      } finally {
        rowLock.release();
      }
      if (writeToWAL && (walEdits != null) && !walEdits.isEmpty()) {
        // sync the transaction log outside the rowlock
        syncOrDefer(txid, durability);
      }
    } finally {
      if (w != null) {
        mvcc.completeMemstoreInsert(w);
      }
      closeRegionOperation(Operation.INCREMENT);
      if (this.metricsRegion != null) {
        this.metricsRegion.updateIncrement();
      }
    }

    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }

    return Result.create(allKVs);
  }

  //
  // New HBASE-880 Helpers
  //

  private void checkFamily(final byte [] family)
  throws NoSuchColumnFamilyException {
    if (!this.htableDescriptor.hasFamily(family)) {
      throw new NoSuchColumnFamilyException("Column family " +
          Bytes.toString(family) + " does not exist in region " + this
          + " in table " + this.htableDescriptor);
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT +
      ClassSize.ARRAY +
      42 * ClassSize.REFERENCE + 2 * Bytes.SIZEOF_INT +
      (12 * Bytes.SIZEOF_LONG) +
      4 * Bytes.SIZEOF_BOOLEAN);

  // woefully out of date - currently missing:
  // 1 x HashMap - coprocessorServiceHandlers
  // 6 org.cliffc.high_scale_lib.Counter - numMutationsWithoutWAL, dataInMemoryWithoutWAL,
  //   checkAndMutateChecksPassed, checkAndMutateChecksFailed, readRequestsCount,
  //   writeRequestsCount, updatesBlockedMs
  // 1 x HRegion$WriteState - writestate
  // 1 x RegionCoprocessorHost - coprocessorHost
  // 1 x RegionSplitPolicy - splitPolicy
  // 1 x MetricsRegion - metricsRegion
  // 1 x MetricsRegionWrapperImpl - metricsRegionWrapper
  public static final long DEEP_OVERHEAD = FIXED_OVERHEAD +
      ClassSize.OBJECT + // closeLock
      (2 * ClassSize.ATOMIC_BOOLEAN) + // closed, closing
      (3 * ClassSize.ATOMIC_LONG) + // memStoreSize, numPutsWithoutWAL, dataInMemoryWithoutWAL
      (2 * ClassSize.CONCURRENT_HASHMAP) +  // lockedRows, scannerReadPoints
      WriteState.HEAP_SIZE + // writestate
      ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + // stores
      (2 * ClassSize.REENTRANT_LOCK) + // lock, updatesLock
      ClassSize.ARRAYLIST + // recentFlushes
      MultiVersionConsistencyControl.FIXED_SIZE // mvcc
      + ClassSize.TREEMAP // maxSeqIdInStores
      + 2 * ClassSize.ATOMIC_INTEGER // majorInProgress, minorInProgress
      ;

  @Override
  public long heapSize() {
    long heapSize = DEEP_OVERHEAD;
    for (Store store : this.stores.values()) {
      heapSize += store.heapSize();
    }
    // this does not take into account row locks, recent flushes, mvcc entries, and more
    return heapSize;
  }

  /*
   * This method calls System.exit.
   * @param message Message to print out.  May be null.
   */
  private static void printUsageAndExit(final String message) {
    if (message != null && message.length() > 0) System.out.println(message);
    System.out.println("Usage: HRegion CATLALOG_TABLE_DIR [major_compact]");
    System.out.println("Options:");
    System.out.println(" major_compact  Pass this option to major compact " +
      "passed region.");
    System.out.println("Default outputs scan of passed region.");
    System.exit(1);
  }

  /**
   * Registers a new protocol buffer {@link Service} subclass as a coprocessor endpoint to
   * be available for handling
   * {@link HRegion#execService(com.google.protobuf.RpcController,
   *    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall)}} calls.
   *
   * <p>
   * Only a single instance may be registered per region for a given {@link Service} subclass (the
   * instances are keyed on {@link com.google.protobuf.Descriptors.ServiceDescriptor#getFullName()}.
   * After the first registration, subsequent calls with the same service name will fail with
   * a return value of {@code false}.
   * </p>
   * @param instance the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false}
   * otherwise
   */
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    if (coprocessorServiceHandlers.containsKey(serviceDesc.getFullName())) {
      LOG.error("Coprocessor service "+serviceDesc.getFullName()+
          " already registered, rejecting request from "+instance
      );
      return false;
    }

    coprocessorServiceHandlers.put(serviceDesc.getFullName(), instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered coprocessor service: region="+
          Bytes.toStringBinary(getRegionName())+" service="+serviceDesc.getFullName());
    }
    return true;
  }

  /**
   * Executes a single protocol buffer coprocessor endpoint {@link Service} method using
   * the registered protocol handlers.  {@link Service} implementations must be registered via the
   * {@link HRegion#registerService(com.google.protobuf.Service)}
   * method before they are available.
   *
   * @param controller an {@code RpcContoller} implementation to pass to the invoked service
   * @param call a {@code CoprocessorServiceCall} instance identifying the service, method,
   *     and parameters for the method invocation
   * @return a protocol buffer {@code Message} instance containing the method's result
   * @throws IOException if no registered service handler is found or an error
   *     occurs during the invocation
   * @see org.apache.hadoop.hbase.regionserver.HRegion#registerService(com.google.protobuf.Service)
   */
  public Message execService(RpcController controller, CoprocessorServiceCall call)
      throws IOException {
    String serviceName = call.getServiceName();
    String methodName = call.getMethodName();
    if (!coprocessorServiceHandlers.containsKey(serviceName)) {
      throw new UnknownProtocolException(null,
          "No registered coprocessor service found for name "+serviceName+
          " in region "+Bytes.toStringBinary(getRegionName()));
    }

    Service service = coprocessorServiceHandlers.get(serviceName);
    Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
    Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
    if (methodDesc == null) {
      throw new UnknownProtocolException(service.getClass(),
          "Unknown method "+methodName+" called on service "+serviceName+
              " in region "+Bytes.toStringBinary(getRegionName()));
    }

    Message request = service.getRequestPrototype(methodDesc).newBuilderForType()
        .mergeFrom(call.getRequest()).build();

    if (coprocessorHost != null) {
      request = coprocessorHost.preEndpointInvocation(service, methodName, request);
    }

    final Message.Builder responseBuilder =
        service.getResponsePrototype(methodDesc).newBuilderForType();
    service.callMethod(methodDesc, controller, request, new RpcCallback<Message>() {
      @Override
      public void run(Message message) {
        if (message != null) {
          responseBuilder.mergeFrom(message);
        }
      }
    });

    if (coprocessorHost != null) {
      coprocessorHost.postEndpointInvocation(service, methodName, request, responseBuilder);
    }

    return responseBuilder.build();
  }

  /*
   * Process table.
   * Do major compaction or list content.
   * @param fs
   * @param p
   * @param log
   * @param c
   * @param majorCompact
   * @throws IOException
   */
  private static void processTable(final FileSystem fs, final Path p,
      final HLog log, final Configuration c,
      final boolean majorCompact)
  throws IOException {
    HRegion region = null;
    FSTableDescriptors fst = new FSTableDescriptors(c);
    // Currently expects tables have one region only.
    if (FSUtils.getTableName(p).equals(TableName.META_TABLE_NAME)) {
      region = HRegion.newHRegion(p, log, fs, c,
        HRegionInfo.FIRST_META_REGIONINFO, fst.get(TableName.META_TABLE_NAME), null);
    } else {
      throw new IOException("Not a known catalog table: " + p.toString());
    }
    try {
      region.initialize();
      if (majorCompact) {
        region.compactStores(true);
      } else {
        // Default behavior
        Scan scan = new Scan();
        // scan.addFamily(HConstants.CATALOG_FAMILY);
        RegionScanner scanner = region.getScanner(scan);
        try {
          List<Cell> kvs = new ArrayList<Cell>();
          boolean done;
          do {
            kvs.clear();
            done = scanner.next(kvs);
            if (kvs.size() > 0) LOG.info(kvs);
          } while (done);
        } finally {
          scanner.close();
        }
      }
    } finally {
      region.close();
    }
  }

  boolean shouldForceSplit() {
    return this.splitRequest;
  }

  byte[] getExplicitSplitPoint() {
    return this.explicitSplitPoint;
  }

  void forceSplit(byte[] sp) {
    // This HRegion will go away after the forced split is successful
    // But if a forced split fails, we need to clear forced split.
    this.splitRequest = true;
    if (sp != null) {
      this.explicitSplitPoint = sp;
    }
  }

  void clearSplit() {
    this.splitRequest = false;
    this.explicitSplitPoint = null;
  }

  /**
   * Give the region a chance to prepare before it is split.
   */
  protected void prepareToSplit() {
    // nothing
  }

  /**
   * Return the splitpoint. null indicates the region isn't splittable
   * If the splitpoint isn't explicitly specified, it will go over the stores
   * to find the best splitpoint. Currently the criteria of best splitpoint
   * is based on the size of the store.
   */
  public byte[] checkSplit() {
    // Can't split META
    if (this.getRegionInfo().isMetaTable() ||
        TableName.NAMESPACE_TABLE_NAME.equals(this.getRegionInfo().getTable())) {
      if (shouldForceSplit()) {
        LOG.warn("Cannot split meta region in HBase 0.20 and above");
      }
      return null;
    }

    // Can't split region which is in recovering state
    if (this.isRecovering()) {
      LOG.info("Cannot split region " + this.getRegionInfo().getEncodedName() + " in recovery.");
      return null;
    }

    if (!splitPolicy.shouldSplit()) {
      return null;
    }

    byte[] ret = splitPolicy.getSplitPoint();

    if (ret != null) {
      try {
        checkRow(ret, "calculated split");
      } catch (IOException e) {
        LOG.error("Ignoring invalid split", e);
        return null;
      }
    }
    return ret;
  }

  /**
   * @return The priority that this region should have in the compaction queue
   */
  public int getCompactPriority() {
    int count = Integer.MAX_VALUE;
    for (Store store : stores.values()) {
      count = Math.min(count, store.getCompactPriority());
    }
    return count;
  }

  /**
   * Checks every store to see if one has too many
   * store files
   * @return true if any store has too many store files
   */
  public boolean needsCompaction() {
    for (Store store : stores.values()) {
      if(store.needsCompaction()) {
        return true;
      }
    }
    return false;
  }

  /** @return the coprocessor host */
  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** @param coprocessorHost the new coprocessor host */
  public void setCoprocessorHost(final RegionCoprocessorHost coprocessorHost) {
    this.coprocessorHost = coprocessorHost;
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies data. It has to be called just before a try.
   * #closeRegionOperation needs to be called in the try's finally block
   * Acquires a read lock and checks if the region is closing or closed.
   * @throws IOException
   */
  public void startRegionOperation() throws IOException {
    startRegionOperation(Operation.ANY);
  }

  /**
   * @param op The operation is about to be taken on the region
   * @throws IOException
   */
  protected void startRegionOperation(Operation op) throws IOException {
    switch (op) {
    case INCREMENT:
    case APPEND:
    case GET:
    case SCAN:
    case SPLIT_REGION:
    case MERGE_REGION:
    case PUT:
    case DELETE:
    case BATCH_MUTATE:
    case COMPACT_REGION:
      // when a region is in recovering state, no read, split or merge is allowed
      if (isRecovering() && (this.disallowWritesInRecovering ||
              (op != Operation.PUT && op != Operation.DELETE && op != Operation.BATCH_MUTATE))) {
        throw new RegionInRecoveryException(this.getRegionNameAsString() +
          " is recovering; cannot take reads");
      }
      break;
    default:
      break;
    }
    if (op == Operation.MERGE_REGION || op == Operation.SPLIT_REGION
        || op == Operation.COMPACT_REGION) {
      // split, merge or compact region doesn't need to check the closing/closed state or lock the
      // region
      return;
    }
    if (this.closing.get()) {
      throw new NotServingRegionException(getRegionNameAsString() + " is closing");
    }
    lock(lock.readLock());
    if (this.closed.get()) {
      lock.readLock().unlock();
      throw new NotServingRegionException(getRegionNameAsString() + " is closed");
    }
    try {
      if (coprocessorHost != null) {
        coprocessorHost.postStartRegionOperation(op);
      }
    } catch (Exception e) {
      lock.readLock().unlock();
      throw new IOException(e);
    }
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding
   * to the try block of #startRegionOperation
   * @throws IOException
   */
  public void closeRegionOperation() throws IOException {
    closeRegionOperation(Operation.ANY);
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding
   * to the try block of {@link #startRegionOperation(Operation)}
   * @param operation
   * @throws IOException
   */
  public void closeRegionOperation(Operation operation) throws IOException {
    lock.readLock().unlock();
    if (coprocessorHost != null) {
      coprocessorHost.postCloseRegionOperation(operation);
    }
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies stores in bulk. It has to be called just before a try.
   * #closeBulkRegionOperation needs to be called in the try's finally block
   * Acquires a writelock and checks if the region is closing or closed.
   * @throws NotServingRegionException when the region is closing or closed
   * @throws RegionTooBusyException if failed to get the lock in time
   * @throws InterruptedIOException if interrupted while waiting for a lock
   */
  private void startBulkRegionOperation(boolean writeLockNeeded)
      throws NotServingRegionException, RegionTooBusyException, InterruptedIOException {
    if (this.closing.get()) {
      throw new NotServingRegionException(getRegionNameAsString() + " is closing");
    }
    if (writeLockNeeded) lock(lock.writeLock());
    else lock(lock.readLock());
    if (this.closed.get()) {
      if (writeLockNeeded) lock.writeLock().unlock();
      else lock.readLock().unlock();
      throw new NotServingRegionException(getRegionNameAsString() + " is closed");
    }
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding
   * to the try block of #startRegionOperation
   */
  private void closeBulkRegionOperation(){
    if (lock.writeLock().isHeldByCurrentThread()) lock.writeLock().unlock();
    else lock.readLock().unlock();
  }

  /**
   * Update counters for numer of puts without wal and the size of possible data loss.
   * These information are exposed by the region server metrics.
   */
  private void recordMutationWithoutWal(final Map<byte [], List<Cell>> familyMap) {
    numMutationsWithoutWAL.increment();
    if (numMutationsWithoutWAL.get() <= 1) {
      LOG.info("writing data to region " + this +
               " with WAL disabled. Data may be lost in the event of a crash.");
    }

    long mutationSize = 0;
    for (List<Cell> cells: familyMap.values()) {
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        // TODO we need include tags length also here.
        mutationSize += KeyValueUtil.keyLength(cell) + cell.getValueLength();
      }
    }

    dataInMemoryWithoutWAL.add(mutationSize);
  }

  private void lock(final Lock lock)
      throws RegionTooBusyException, InterruptedIOException {
    lock(lock, 1);
  }

  /**
   * Try to acquire a lock.  Throw RegionTooBusyException
   * if failed to get the lock in time. Throw InterruptedIOException
   * if interrupted while waiting for the lock.
   */
  private void lock(final Lock lock, final int multiplier)
      throws RegionTooBusyException, InterruptedIOException {
    try {
      final long waitTime = Math.min(maxBusyWaitDuration,
          busyWaitDuration * Math.min(multiplier, maxBusyWaitMultiplier));
      if (!lock.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
        throw new RegionTooBusyException(
            "failed to get a lock in " + waitTime + " ms. " +
                "regionName=" + (this.getRegionInfo() == null ? "unknown" :
                this.getRegionInfo().getRegionNameAsString()) +
                ", server=" + (this.getRegionServerServices() == null ? "unknown" :
                this.getRegionServerServices().getServerName()));
      }
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while waiting for a lock");
      InterruptedIOException iie = new InterruptedIOException();
      iie.initCause(ie);
      throw iie;
    }
  }

  /**
   * Calls sync with the given transaction ID if the region's table is not
   * deferring it.
   * @param txid should sync up to which transaction
   * @throws IOException If anything goes wrong with DFS
   */
  private void syncOrDefer(long txid, Durability durability) throws IOException {
    if (this.getRegionInfo().isMetaRegion()) {
      this.log.sync(txid);
    } else {
      switch(durability) {
      case USE_DEFAULT:
        // do what table defaults to
        if (shouldSyncLog()) {
          this.log.sync(txid);
        }
        break;
      case SKIP_WAL:
        // nothing do to
        break;
      case ASYNC_WAL:
        // nothing do to
        break;
      case SYNC_WAL:
      case FSYNC_WAL:
        // sync the WAL edit (SYNC and FSYNC treated the same for now)
        this.log.sync(txid);
        break;
      }
    }
  }

  /**
   * Check whether we should sync the log from the table's durability settings
   */
  private boolean shouldSyncLog() {
    return durability.ordinal() >  Durability.ASYNC_WAL.ordinal();
  }

  /**
   * A mocked list implementaion - discards all updates.
   */
  private static final List<Cell> MOCKED_LIST = new AbstractList<Cell>() {

    @Override
    public void add(int index, Cell element) {
      // do nothing
    }

    @Override
    public boolean addAll(int index, Collection<? extends Cell> c) {
      return false; // this list is never changed as a result of an update
    }

    @Override
    public KeyValue get(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return 0;
    }
  };

  /**
   * Facility for dumping and compacting catalog tables.
   * Only does catalog tables since these are only tables we for sure know
   * schema on.  For usage run:
   * <pre>
   *   ./bin/hbase org.apache.hadoop.hbase.regionserver.HRegion
   * </pre>
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      printUsageAndExit(null);
    }
    boolean majorCompact = false;
    if (args.length > 1) {
      if (!args[1].toLowerCase().startsWith("major")) {
        printUsageAndExit("ERROR: Unrecognized option <" + args[1] + ">");
      }
      majorCompact = true;
    }
    final Path tableDir = new Path(args[0]);
    final Configuration c = HBaseConfiguration.create();
    final FileSystem fs = FileSystem.get(c);
    final Path logdir = new Path(c.get("hbase.tmp.dir"));
    final String logname = "hlog" + FSUtils.getTableName(tableDir) + System.currentTimeMillis();

    final HLog log = HLogFactory.createHLog(fs, logdir, logname, c);
    try {
      processTable(fs, tableDir, log, c, majorCompact);
    } finally {
       log.close();
       // TODO: is this still right?
       BlockCache bc = new CacheConfig(c).getBlockCache();
       if (bc != null) bc.shutdown();
    }
  }

  /**
   * Gets the latest sequence number that was read from storage when this region was opened.
   */
  public long getOpenSeqNum() {
    return this.openSeqNum;
  }

  /**
   * Gets max sequence ids of stores that was read from storage when this region was opened. WAL
   * Edits with smaller or equal sequence number will be skipped from replay.
   */
  public Map<byte[], Long> getMaxStoreSeqIdForLogReplay() {
    return this.maxSeqIdInStores;
  }

  /**
   * @return if a given region is in compaction now.
   */
  public CompactionState getCompactionState() {
    boolean hasMajor = majorInProgress.get() > 0, hasMinor = minorInProgress.get() > 0;
    return (hasMajor ? (hasMinor ? CompactionState.MAJOR_AND_MINOR : CompactionState.MAJOR)
        : (hasMinor ? CompactionState.MINOR : CompactionState.NONE));
  }

  public void reportCompactionRequestStart(boolean isMajor){
    (isMajor ? majorInProgress : minorInProgress).incrementAndGet();
  }

  public void reportCompactionRequestEnd(boolean isMajor, int numFiles, long filesSizeCompacted){
    int newValue = (isMajor ? majorInProgress : minorInProgress).decrementAndGet();

    // metrics
    compactionsFinished.incrementAndGet();
    compactionNumFilesCompacted.addAndGet(numFiles);
    compactionNumBytesCompacted.addAndGet(filesSizeCompacted);

    assert newValue >= 0;
  }

  /**
   * @return sequenceId.
   */
  public AtomicLong getSequenceId() {
    return this.sequenceId;
  }

  /**
   * sets this region's sequenceId.
   * @param value new value
   */
  private void setSequenceId(long value) {
    this.sequenceId.set(value);
  }

  /**
   * Listener class to enable callers of
   * bulkLoadHFile() to perform any necessary
   * pre/post processing of a given bulkload call
   */
  public interface BulkLoadListener {

    /**
     * Called before an HFile is actually loaded
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @return final path to be used for actual loading
     * @throws IOException
     */
    String prepareBulkLoad(byte[] family, String srcPath) throws IOException;

    /**
     * Called after a successful HFile load
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @throws IOException
     */
    void doneBulkLoad(byte[] family, String srcPath) throws IOException;

    /**
     * Called after a failed HFile load
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @throws IOException
     */
    void failedBulkLoad(byte[] family, String srcPath) throws IOException;
  }

  @VisibleForTesting class RowLockContext {
    private final HashedBytes row;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Thread thread;
    private int lockCount = 0;

    RowLockContext(HashedBytes row) {
      this.row = row;
      this.thread = Thread.currentThread();
    }

    boolean ownedByCurrentThread() {
      return thread == Thread.currentThread();
    }

    RowLock newLock() {
      lockCount++;
      return new RowLock(this);
    }

    void releaseLock() {
      if (!ownedByCurrentThread()) {
        throw new IllegalArgumentException("Lock held by thread: " + thread
          + " cannot be released by different thread: " + Thread.currentThread());
      }
      lockCount--;
      if (lockCount == 0) {
        // no remaining locks by the thread, unlock and allow other threads to access
        RowLockContext existingContext = lockedRows.remove(row);
        if (existingContext != this) {
          throw new RuntimeException(
              "Internal row lock state inconsistent, should not happen, row: " + row);
        }
        latch.countDown();
      }
    }
  }

  /**
   * Row lock held by a given thread.
   * One thread may acquire multiple locks on the same row simultaneously.
   * The locks must be released by calling release() from the same thread.
   */
  public static class RowLock {
    @VisibleForTesting final RowLockContext context;
    private boolean released = false;

    @VisibleForTesting RowLock(RowLockContext context) {
      this.context = context;
    }

    /**
     * Release the given lock.  If there are no remaining locks held by the current thread
     * then unlock the row and allow other threads to acquire the lock.
     * @throws IllegalArgumentException if called by a different thread than the lock owning thread
     */
    public void release() {
      if (!released) {
        context.releaseLock();
        released = true;
      }
    }
  }

  /**
   * Lock the updates' readLock first, so that we could safely append logs in coprocessors.
   * @throws RegionTooBusyException
   * @throws InterruptedIOException
   */
  public void updatesLock() throws RegionTooBusyException, InterruptedIOException {
    lock(updatesLock.readLock());
  }

  /**
   * Unlock the updates' readLock after appending logs in coprocessors.
   * @throws InterruptedIOException
   */
  public void updatesUnlock() throws InterruptedIOException {
    updatesLock.readLock().unlock();
  }
}
