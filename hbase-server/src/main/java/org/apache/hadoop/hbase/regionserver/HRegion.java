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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.RandomStringUtils;
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
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.ReplayHLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.wal.WALSplitter.MutationReplay;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.TextFormat;

@InterfaceAudience.Private
public class HRegion implements HeapSize, PropagatingConfigurationObserver, Region {
  private static final Log LOG = LogFactory.getLog(HRegion.class);

  public static final String LOAD_CFS_ON_DEMAND_CONFIG_KEY =
    "hbase.hregion.scan.loadColumnFamiliesOnDemand";

  /**
   * Longest time we'll wait on a sequenceid.
   * Sequenceid comes up out of the WAL subsystem. WAL subsystem can go bad or a test might use
   * it without cleaning up previous usage properly; generally, a WAL roll is needed. The timeout
   * is for a latch in WALKey. There is no global accounting of outstanding WALKeys; intentionally
   * to avoid contention, but it makes it so if an abort or problem, we could be stuck waiting
   * on the WALKey latch. Revisit.
   */
  private final int maxWaitForSeqId;
  private static final String MAX_WAIT_FOR_SEQ_ID_KEY = "hbase.hregion.max.wait.for.sequenceid.ms";
  private static final int DEFAULT_MAX_WAIT_FOR_SEQ_ID = 30000;

  /**
   * This is the global default value for durability. All tables/mutations not
   * defining a durability or using USE_DEFAULT will default to this value.
   */
  private static final Durability DEFAULT_DURABILITY = Durability.SYNC_WAL;

  final AtomicBoolean closed = new AtomicBoolean(false);

  /* Closing can take some time; use the closing flag if there is stuff we don't
   * want to do while in closing state; e.g. like offer this region up to the
   * master as a region to close if the carrying regionserver is overloaded.
   * Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * The max sequence id of flushed data on this region. There is no edit in memory that is
   * less that this sequence id.
   */
  private volatile long maxFlushedSeqId = HConstants.NO_SEQNUM;

  /**
   * Record the sequence id of last flush operation. Can be in advance of
   * {@link #maxFlushedSeqId} when flushing a single column family. In this case,
   * {@link #maxFlushedSeqId} will be older than the oldest edit in memory.
   */
  private volatile long lastFlushOpSeqId = HConstants.NO_SEQNUM;

  /**
   * The sequence id of the last replayed open region event from the primary region. This is used
   * to skip entries before this due to the possibility of replay edits coming out of order from
   * replication.
   */
  protected volatile long lastReplayedOpenRegionSeqId = -1L;
  protected volatile long lastReplayedCompactionSeqId = -1L;

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

  private final AtomicLong memstoreSize = new AtomicLong(0);

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

  // Compaction counters
  final AtomicLong compactionsFinished = new AtomicLong(0L);
  final AtomicLong compactionNumFilesCompacted = new AtomicLong(0L);
  final AtomicLong compactionNumBytesCompacted = new AtomicLong(0L);

  private final WAL wal;
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
  // have to be conservative in how we replay wals. For each store, we calculate
  // the maxSeqId up to which the store was flushed. And, skip the edits which
  // are equal to or lower than maxSeqId for each store.
  // The following map is populated when opening the region
  Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  /** Saved state from replaying prepare flush cache */
  private PrepareFlushResult prepareFlushResult = null;

  /**
   * Config setting for whether to allow writes when a region is in recovering or not.
   */
  private boolean disallowWritesInRecovering = false;

  // when a region is in recovering state, it can only accept writes not reads
  private volatile boolean recovering = false;

  private volatile Optional<ConfigurationManager> configurationManager;

  /**
   * @return The smallest mvcc readPoint across all the scanners in this
   * region. Writes older than this readPoint, are included in every
   * read operation.
   */
  public long getSmallestReadPoint() {
    long minimumReadPoint;
    // We need to ensure that while we are calculating the smallestReadPoint
    // no new RegionScanners can grab a readPoint that we are unaware of.
    // We achieve this by synchronizing on the scannerReadPoints object.
    synchronized(scannerReadPoints) {
      minimumReadPoint = mvcc.getReadPoint();

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
    // whether the reads are enabled. This is different than readOnly, because readOnly is
    // static in the lifetime of the region, while readsEnabled is dynamic
    volatile boolean readsEnabled = true;

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

    void setReadsEnabled(boolean readsEnabled) {
      this.readsEnabled = readsEnabled;
    }

    static final long HEAP_SIZE = ClassSize.align(
        ClassSize.OBJECT + 5 * Bytes.SIZEOF_BOOLEAN);
  }

  /**
   * Objects from this class are created when flushing to describe all the different states that
   * that method ends up in. The Result enum describes those states. The sequence id should only
   * be specified if the flush was successful, and the failure message should only be specified
   * if it didn't flush.
   */
  public static class FlushResultImpl implements FlushResult {
    final Result result;
    final String failureReason;
    final long flushSequenceId;
    final boolean wroteFlushWalMarker;

    /**
     * Convenience constructor to use when the flush is successful, the failure message is set to
     * null.
     * @param result Expecting FLUSHED_NO_COMPACTION_NEEDED or FLUSHED_COMPACTION_NEEDED.
     * @param flushSequenceId Generated sequence id that comes right after the edits in the
     *                        memstores.
     */
    FlushResultImpl(Result result, long flushSequenceId) {
      this(result, flushSequenceId, null, false);
      assert result == Result.FLUSHED_NO_COMPACTION_NEEDED || result == Result
          .FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience constructor to use when we cannot flush.
     * @param result Expecting CANNOT_FLUSH_MEMSTORE_EMPTY or CANNOT_FLUSH.
     * @param failureReason Reason why we couldn't flush.
     */
    FlushResultImpl(Result result, String failureReason, boolean wroteFlushMarker) {
      this(result, -1, failureReason, wroteFlushMarker);
      assert result == Result.CANNOT_FLUSH_MEMSTORE_EMPTY || result == Result.CANNOT_FLUSH;
    }

    /**
     * Constructor with all the parameters.
     * @param result Any of the Result.
     * @param flushSequenceId Generated sequence id if the memstores were flushed else -1.
     * @param failureReason Reason why we couldn't flush, or null.
     */
    FlushResultImpl(Result result, long flushSequenceId, String failureReason,
      boolean wroteFlushMarker) {
      this.result = result;
      this.flushSequenceId = flushSequenceId;
      this.failureReason = failureReason;
      this.wroteFlushWalMarker = wroteFlushMarker;
    }

    /**
     * Convenience method, the equivalent of checking if result is
     * FLUSHED_NO_COMPACTION_NEEDED or FLUSHED_NO_COMPACTION_NEEDED.
     * @return true if the memstores were flushed, else false.
     */
    @Override
    public boolean isFlushSucceeded() {
      return result == Result.FLUSHED_NO_COMPACTION_NEEDED || result == Result
          .FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience method, the equivalent of checking if result is FLUSHED_COMPACTION_NEEDED.
     * @return True if the flush requested a compaction, else false (doesn't even mean it flushed).
     */
    @Override
    public boolean isCompactionNeeded() {
      return result == Result.FLUSHED_COMPACTION_NEEDED;
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("flush result:").append(result).append(", ")
        .append("failureReason:").append(failureReason).append(",")
        .append("flush seq id").append(flushSequenceId).toString();
    }

    @Override
    public Result getResult() {
      return result;
    }
  }

  /** A result object from prepare flush cache stage */
  @VisibleForTesting
  static class PrepareFlushResult {
    final FlushResult result; // indicating a failure result from prepare
    final TreeMap<byte[], StoreFlushContext> storeFlushCtxs;
    final TreeMap<byte[], List<Path>> committedFiles;
    final TreeMap<byte[], Long> storeFlushableSize;
    final long startTime;
    final long flushOpSeqId;
    final long flushedSeqId;
    final long totalFlushableSize;

    /** Constructs an early exit case */
    PrepareFlushResult(FlushResult result, long flushSeqId) {
      this(result, null, null, null, Math.max(0, flushSeqId), 0, 0, 0);
    }

    /** Constructs a successful prepare flush result */
    PrepareFlushResult(
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], Long> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, long totalFlushableSize) {
      this(null, storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
        flushSeqId, flushedSeqId, totalFlushableSize);
    }

    private PrepareFlushResult(
      FlushResult result,
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], Long> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, long totalFlushableSize) {
      this.result = result;
      this.storeFlushCtxs = storeFlushCtxs;
      this.committedFiles = committedFiles;
      this.storeFlushableSize = storeFlushableSize;
      this.startTime = startTime;
      this.flushOpSeqId = flushSeqId;
      this.flushedSeqId = flushedSeqId;
      this.totalFlushableSize = totalFlushableSize;
    }

    public FlushResult getResult() {
      return this.result;
    }
  }

  final WriteState writestate = new WriteState();

  long memstoreFlushSize;
  final long timestampSlop;
  final long rowProcessorTimeout;

  // Last flush time for each Store. Useful when we are flushing for each column
  private final ConcurrentMap<Store, Long> lastStoreFlushTimeMap =
      new ConcurrentHashMap<Store, Long>();

  final RegionServerServices rsServices;
  private RegionServerAccounting rsAccounting;
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

  private final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  // Coprocessor host
  private RegionCoprocessorHost coprocessorHost;

  private HTableDescriptor htableDescriptor = null;
  private RegionSplitPolicy splitPolicy;
  private FlushPolicy flushPolicy;

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
   * @param wal The WAL is the outbound log for any updates to the HRegion
   * The wal file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate wal info for this HRegion. If there is a previous wal file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.
   * @param confParam is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param htd the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   * @deprecated Use other constructors.
   */
  @Deprecated
  @VisibleForTesting
  public HRegion(final Path tableDir, final WAL wal, final FileSystem fs,
      final Configuration confParam, final HRegionInfo regionInfo,
      final HTableDescriptor htd, final RegionServerServices rsServices) {
    this(new HRegionFileSystem(confParam, fs, tableDir, regionInfo),
      wal, confParam, htd, rsServices);
  }

  /**
   * HRegion constructor. This constructor should only be used for testing and
   * extensions.  Instances of HRegion should be instantiated with the
   * {@link HRegion#createHRegion} or {@link HRegion#openHRegion} method.
   *
   * @param fs is the filesystem.
   * @param wal The WAL is the outbound log for any updates to the HRegion
   * The wal file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate wal info for this HRegion. If there is a previous wal file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param confParam is global configuration settings.
   * @param htd the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   */
  public HRegion(final HRegionFileSystem fs, final WAL wal, final Configuration confParam,
      final HTableDescriptor htd, final RegionServerServices rsServices) {
    if (htd == null) {
      throw new IllegalArgumentException("Need table descriptor");
    }

    if (confParam instanceof CompoundConfiguration) {
      throw new IllegalArgumentException("Need original base configuration");
    }

    this.comparator = fs.getRegionInfo().getComparator();
    this.wal = wal;
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

    this.maxWaitForSeqId = conf.getInt(MAX_WAIT_FOR_SEQ_ID_KEY, DEFAULT_MAX_WAIT_FOR_SEQ_ID);
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
      2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

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
        ? DEFAULT_DURABILITY
        : htd.getDurability();
    if (rsServices != null) {
      this.rsAccounting = this.rsServices.getRegionServerAccounting();
      // don't initialize coprocessors if not running within a regionserver
      // TODO: revisit if coprocessors should load in other cases
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
      this.metricsRegionWrapper = new MetricsRegionWrapperImpl(this);
      this.metricsRegion = new MetricsRegion(this.metricsRegionWrapper);

      Map<String, Region> recoveringRegions = rsServices.getRecoveringRegions();
      String encodedName = getRegionInfo().getEncodedName();
      if (recoveringRegions != null && recoveringRegions.containsKey(encodedName)) {
        this.recovering = true;
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
    configurationManager = Optional.absent();

    // disable stats tracking system tables, but check the config for everything else
    this.regionStatsEnabled = htd.getTableName().getNamespaceAsString().equals(
        NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) ?
          false :
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
        conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
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
        status.abort("Exception during region " + getRegionInfo().getRegionNameAsString() +
          " initialization.");
      }
    }
  }

  private long initializeRegionInternals(final CancelableProgressable reporter,
      final MonitoredTask status) throws IOException {
    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-open hook");
      coprocessorHost.preOpen();
    }

    // Write HRI to a file in case we need to recover hbase:meta
    status.setStatus("Writing region info on filesystem");
    fs.checkRegionInfoOnFilesystem();

    // Initialize all the HStores
    status.setStatus("Initializing all the Stores");
    long maxSeqId = initializeStores(reporter, status);
    this.mvcc.advanceTo(maxSeqId);
    if (ServerRegionReplicaUtil.shouldReplayRecoveredEdits(this)) {
      // Recover any edits if available.
      maxSeqId = Math.max(maxSeqId,
        replayRecoveredEditsIfAny(this.fs.getRegionDir(), maxSeqIdInStores, reporter, status));
      // Make sure mvcc is up to max.
      this.mvcc.advanceTo(maxSeqId);
    }
    this.lastReplayedOpenRegionSeqId = maxSeqId;

    this.writestate.setReadOnly(ServerRegionReplicaUtil.isReadOnly(this));
    this.writestate.flushRequested = false;
    this.writestate.compacting = 0;

    if (this.writestate.writesEnabled) {
      // Remove temporary data left over from old regions
      status.setStatus("Cleaning up temporary data from old regions");
      fs.cleanupTempDir();
    }

    if (this.writestate.writesEnabled) {
      status.setStatus("Cleaning up detritus from prior splits");
      // Get rid of any splits or merges that were lost in-progress.  Clean out
      // these directories here on open.  We may be opening a region that was
      // being split but we crashed in the middle of it all.
      fs.cleanupAnySplitDetritus();
      fs.cleanupMergesDir();
    }

    // Initialize split policy
    this.splitPolicy = RegionSplitPolicy.create(this, conf);

    // Initialize flush policy
    this.flushPolicy = FlushPolicyFactory.create(this, conf);

    long lastFlushTime = EnvironmentEdgeManager.currentTime();
    for (Store store: stores.values()) {
      this.lastStoreFlushTimeMap.put(store, lastFlushTime);
    }

    // Use maximum of log sequenceid or that which was found in stores
    // (particularly if no recovered edits, seqid will be -1).
    long nextSeqid = maxSeqId;

    // In distributedLogReplay mode, we don't know the last change sequence number because region
    // is opened before recovery completes. So we add a safety bumper to avoid new sequence number
    // overlaps used sequence numbers
    if (this.writestate.writesEnabled) {
      nextSeqid = WALSplitter.writeRegionSequenceIdFile(this.fs.getFileSystem(), this.fs
          .getRegionDir(), nextSeqid, (this.recovering ? (this.flushPerChanges + 10000000) : 1));
    } else {
      nextSeqid++;
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

  /**
   * Open all Stores.
   * @param reporter
   * @param status
   * @return Highest sequenceId found out in a Store.
   * @throws IOException
   */
  private long initializeStores(final CancelableProgressable reporter, MonitoredTask status)
  throws IOException {
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
          this.stores.put(store.getFamily().getName(), store);

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
    return Math.max(maxSeqId, maxMemstoreTS + 1);
  }

  private void initializeWarmup(final CancelableProgressable reporter) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this);

    // Initialize all the HStores
    status.setStatus("Warming up all the Stores");
    initializeStores(reporter, status);
  }

  private void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
    Map<byte[], List<Path>> storeFiles = new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
    for (Store store: getStores()) {
      ArrayList<Path> storeFileNames = new ArrayList<Path>();
      for (StoreFile storeFile: store.getStorefiles()) {
        storeFileNames.add(storeFile.getPath());
      }
      storeFiles.put(store.getFamily().getName(), storeFileNames);
    }

    RegionEventDescriptor regionOpenDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_OPEN, getRegionInfo(), openSeqId,
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getTableDesc(), getRegionInfo(), regionOpenDesc, mvcc);
  }

  private void writeRegionCloseMarker(WAL wal) throws IOException {
    Map<byte[], List<Path>> storeFiles = new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
    for (Store store: getStores()) {
      ArrayList<Path> storeFileNames = new ArrayList<Path>();
      for (StoreFile storeFile: store.getStorefiles()) {
        storeFileNames.add(storeFile.getPath());
      }
      storeFiles.put(store.getFamily().getName(), storeFileNames);
    }

    RegionEventDescriptor regionEventDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_CLOSE, getRegionInfo(), mvcc.getReadPoint(),
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getTableDesc(), getRegionInfo(), regionEventDesc, mvcc);

    // Store SeqId in HDFS when a region closes
    // checking region folder exists is due to many tests which delete the table folder while a
    // table is still online
    if (this.fs.getFileSystem().exists(this.fs.getRegionDir())) {
      WALSplitter.writeRegionSequenceIdFile(this.fs.getFileSystem(), this.fs.getRegionDir(),
        mvcc.getReadPoint(), 0);
    }
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

  @Override
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
        try {
          hdfsBlocksDistribution.add(storeFileInfo.computeHDFSBlocksDistribution(fs));
        } catch (IOException ioe) {
          LOG.warn("Error getting hdfs block distribution for " + storeFileInfo);
        }
      }
    }
    return hdfsBlocksDistribution;
  }

  /**
   * Increase the size of mem store in this region and the size of global mem
   * store
   * @return the size of memstore in this region
   */
  public long addAndGetGlobalMemstoreSize(long memStoreSize) {
    if (this.rsAccounting != null) {
      rsAccounting.addAndGetGlobalMemstoreSize(memStoreSize);
    }
    return this.memstoreSize.addAndGet(memStoreSize);
  }

  @Override
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

  @Override
  public long getReadRequestsCount() {
    return readRequestsCount.get();
  }

  @Override
  public void updateReadRequestsCount(long i) {
    readRequestsCount.add(i);
  }

  @Override
  public long getWriteRequestsCount() {
    return writeRequestsCount.get();
  }

  @Override
  public void updateWriteRequestsCount(long i) {
    writeRequestsCount.add(i);
  }

  @Override
  public long getMemstoreSize() {
    return memstoreSize.get();
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return numMutationsWithoutWAL.get();
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return dataInMemoryWithoutWAL.get();
  }

  @Override
  public long getBlockedRequestsCount() {
    return blockedRequestsCount.get();
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return checkAndMutateChecksPassed.get();
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return checkAndMutateChecksFailed.get();
  }

  @Override
  public MetricsRegion getMetrics() {
    return metricsRegion;
  }

  @Override
  public boolean isClosed() {
    return this.closed.get();
  }

  @Override
  public boolean isClosing() {
    return this.closing.get();
  }

  @Override
  public boolean isReadOnly() {
    return this.writestate.isReadOnly();
  }

  /**
   * Reset recovering state of current region
   */
  public void setRecovering(boolean newState) {
    boolean wasRecovering = this.recovering;
    // before we flip the recovering switch (enabling reads) we should write the region open
    // event to WAL if needed
    if (wal != null && getRegionServerServices() != null && !writestate.readOnly
        && wasRecovering && !newState) {

      // force a flush only if region replication is set up for this region. Otherwise no need.
      boolean forceFlush = getTableDesc().getRegionReplication() > 1;

      // force a flush first
      MonitoredTask status = TaskMonitor.get().createStatus(
        "Flushing region " + this + " because recovery is finished");
      try {
        if (forceFlush) {
          internalFlushcache(status);
        }

        status.setStatus("Writing region open event marker to WAL because recovery is finished");
        try {
          long seqId = openSeqNum;
          // obtain a new seqId because we possibly have writes and flushes on top of openSeqNum
          if (wal != null) {
            seqId = getNextSequenceId(wal);
          }
          writeRegionOpenMarker(wal, seqId);
        } catch (IOException e) {
          // We cannot rethrow this exception since we are being called from the zk thread. The
          // region has already opened. In this case we log the error, but continue
          LOG.warn(getRegionInfo().getEncodedName() + " : was not able to write region opening "
              + "event to WAL, continueing", e);
        }
      } catch (IOException ioe) {
        // Distributed log replay semantics does not necessarily require a flush, since the replayed
        // data is already written again in the WAL. So failed flush should be fine.
        LOG.warn(getRegionInfo().getEncodedName() + " : was not able to flush "
            + "event to WAL, continueing", ioe);
      } finally {
        status.cleanup();
      }
    }

    this.recovering = newState;
    if (wasRecovering && !recovering) {
      // Call only when wal replay is over.
      coprocessorHost.postLogReplay();
    }
  }

  @Override
  public boolean isRecovering() {
    return this.recovering;
  }

  @Override
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
      LOG.debug("Region " + getRegionInfo().getRegionNameAsString()
          + " is not mergeable because it is closing or closed");
      return false;
    }
    if (hasReferences()) {
      LOG.debug("Region " + getRegionInfo().getRegionNameAsString()
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

   public MultiVersionConcurrencyControl getMVCC() {
     return mvcc;
   }

   @Override
   public long getMaxFlushedSeqId() {
     return maxFlushedSeqId;
   }

   @Override
   public long getReadpoint(IsolationLevel isolationLevel) {
     if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
       // This scan can read even uncommitted transactions
       return Long.MAX_VALUE;
     }
     return mvcc.getReadPoint();
   }

   @Override
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
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
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
  /** Default interval for System tables memstore flush */
  public static final int SYSTEM_CACHE_FLUSH_INTERVAL = 300000; // 5 minutes

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
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
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

  /**
   * Exposed for some very specific unit tests.
   */
  @VisibleForTesting
  public void setClosing(boolean closing) {
    this.closing.set(closing);
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
    boolean canFlush = true;
    synchronized (writestate) {
      // Disable compacting and flushing by background threads for this
      // region.
      canFlush = !writestate.readOnly;
      writestate.writesEnabled = false;
      LOG.debug("Closing " + this + ": disabling compactions & flushes");
      waitForFlushesAndCompactions();
    }
    // If we were not just flushing, is it worth doing a preflush...one
    // that will clear out of the bulk of the memstore before we put up
    // the close flag?
    if (!abort && worthPreFlushing() && canFlush) {
      status.setStatus("Pre-flushing region before close");
      LOG.info("Running close preflush of " + getRegionInfo().getRegionNameAsString());
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
      if (!abort && canFlush) {
        int flushCount = 0;
        while (this.memstoreSize.get() > 0) {
          try {
            if (flushCount++ > 0) {
              int actualFlushes = flushCount - 1;
              if (actualFlushes > 5) {
                // If we tried 5 times and are unable to clear memory, abort
                // so we do not lose data
                throw new DroppedSnapshotException("Failed clearing memory after " +
                  actualFlushes + " attempts on region: " +
                    Bytes.toStringBinary(getRegionInfo().getRegionName()));
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
          getStoreOpenAndCloseThreadPool("StoreCloserThread-" +
            getRegionInfo().getRegionNameAsString());
        CompletionService<Pair<byte[], Collection<StoreFile>>> completionService =
          new ExecutorCompletionService<Pair<byte[], Collection<StoreFile>>>(storeCloserThreadPool);

        // close each store in parallel
        for (final Store store : stores.values()) {
          long flushableSize = store.getFlushableSize();
          if (!(abort || flushableSize == 0 || writestate.readOnly)) {
            if (getRegionServerServices() != null) {
              getRegionServerServices().abort("Assertion failed while closing store "
                + getRegionInfo().getRegionNameAsString() + " " + store
                + ". flushableSize expected=0, actual= " + flushableSize
                + ". Current memstoreSize=" + getMemstoreSize() + ". Maybe a coprocessor "
                + "operation failed and left the memstore in a partially updated state.", null);
            }
          }
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

      status.setStatus("Writing region close event to WAL");
      if (!abort && wal != null && getRegionServerServices() != null && !writestate.readOnly) {
        writeRegionCloseMarker(wal);
      }

      this.closed.set(true);
      if (!canFlush) {
        addAndGetGlobalMemstoreSize(-memstoreSize.get());
      } else if (memstoreSize.get() != 0) {
        LOG.error("Memstore size is " + memstoreSize.get());
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor post-close hooks");
        this.coprocessorHost.postClose(abort);
      }
      if (this.metricsRegion != null) {
        this.metricsRegion.close();
      }
      if (this.metricsRegionWrapper != null) {
        Closeables.closeQuietly(this.metricsRegionWrapper);
      }
      status.markComplete("Closed");
      LOG.info("Closed " + this);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void waitForFlushesAndCompactions() {
    synchronized (writestate) {
      if (this.writestate.readOnly) {
        // we should not wait for replayed flushed if we are read only (for example in case the
        // region is a secondary replica).
        return;
      }
      boolean interrupted = false;
      try {
        while (writestate.compacting > 0 || writestate.flushing) {
          LOG.debug("waiting for " + writestate.compacting + " compactions"
            + (writestate.flushing ? " & cache flush" : "") + " to complete for region " + this);
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // essentially ignore and propagate the interrupt back up
            LOG.warn("Interrupted while waiting");
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) {
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

  @Override
  public HTableDescriptor getTableDesc() {
    return this.htableDescriptor;
  }

  /** @return WAL in use for this region */
  public WAL getWAL() {
    return this.wal;
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

  @Override
  public long getEarliestFlushTimeForAllStores() {
    return lastStoreFlushTimeMap.isEmpty() ? Long.MAX_VALUE : Collections.min(lastStoreFlushTimeMap
        .values());
  }

  @Override
  public long getOldestHfileTs(boolean majorCompactioOnly) throws IOException {
    long result = Long.MAX_VALUE;
    for (Store store : getStores()) {
      for (StoreFile file : store.getStorefiles()) {
        HFile.Reader reader = file.getReader().getHFileReader();
        if (majorCompactioOnly) {
          byte[] val = reader.loadFileInfo().get(StoreFile.MAJOR_COMPACTION_KEY);
          if (val == null || !Bytes.toBoolean(val)) {
            continue;
          }
        }
        result = Math.min(result, reader.getFileContext().getFileCreateTime());
      }
    }
    return result == Long.MAX_VALUE ? 0 : result;
  }

  RegionLoad.Builder setCompleteSequenceId(RegionLoad.Builder regionLoadBldr) {
    long lastFlushOpSeqIdLocal = this.lastFlushOpSeqId;
    byte[] encodedRegionName = this.getRegionInfo().getEncodedNameAsBytes();
    regionLoadBldr.clearStoreCompleteSequenceId();
    for (byte[] familyName : this.stores.keySet()) {
      long earliest = this.wal.getEarliestMemstoreSeqNum(encodedRegionName, familyName);
      // Subtract - 1 to go earlier than the current oldest, unflushed edit in memstore; this will
      // give us a sequence id that is for sure flushed. We want edit replay to start after this
      // sequence id in this region. If NO_SEQNUM, use the regions maximum flush id.
      long csid = (earliest == HConstants.NO_SEQNUM)? lastFlushOpSeqIdLocal: earliest - 1;
      regionLoadBldr.addStoreCompleteSequenceId(StoreSequenceId.
        newBuilder().setFamilyName(ByteString.copyFrom(familyName)).setSequenceId(csid).build());
    }
    return regionLoadBldr.setCompleteSequenceId(getMaxFlushedSeqId());
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

  @Override
  public void triggerMajorCompaction() throws IOException {
    for (Store s : getStores()) {
      s.triggerMajorCompaction();
    }
  }

  @Override
  public void compact(final boolean majorCompaction) throws IOException {
    if (majorCompaction) {
      triggerMajorCompaction();
    }
    for (Store s : getStores()) {
      CompactionContext compaction = s.requestCompaction();
      if (compaction != null) {
        CompactionThroughputController controller = null;
        if (rsServices != null) {
          controller = CompactionThroughputControllerFactory.create(rsServices, conf);
        }
        if (controller == null) {
          controller = NoLimitCompactionThroughputController.INSTANCE;
        }
        compact(compaction, s, controller, null);
      }
    }
  }

  /**
   * This is a helper function that compact all the stores synchronously
   * It is used by utilities and testing
   *
   * @throws IOException e
   */
  public void compactStores() throws IOException {
    for (Store s : getStores()) {
      CompactionContext compaction = s.requestCompaction();
      if (compaction != null) {
        compact(compaction, s, NoLimitCompactionThroughputController.INSTANCE, null);
      }
    }
  }

  /**
   * This is a helper function that compact the given store
   * It is used by utilities and testing
   *
   * @throws IOException e
   */
  @VisibleForTesting
  void compactStore(byte[] family, CompactionThroughputController throughputController)
      throws IOException {
    Store s = getStore(family);
    CompactionContext compaction = s.requestCompaction();
    if (compaction != null) {
      compact(compaction, s, throughputController, null);
    }
  }

  /*
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
   * @param throughputController
   * @return whether the compaction completed
   */
  public boolean compact(CompactionContext compaction, Store store,
      CompactionThroughputController throughputController) throws IOException {
    return compact(compaction, store, throughputController, null);
  }

  public boolean compact(CompactionContext compaction, Store store,
      CompactionThroughputController throughputController, User user) throws IOException {
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
          store.compact(compaction, throughputController, user);
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

  @Override
  public FlushResult flush(boolean force) throws IOException {
    return flushcache(force, false);
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
   * @param forceFlushAllStores whether we want to flush all stores
   * @param writeFlushRequestWalMarker whether to write the flush request marker to WAL
   * @return whether the flush is success and whether the region needs compacting
   *
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
   */
  public FlushResult flushcache(boolean forceFlushAllStores, boolean writeFlushRequestWalMarker)
      throws IOException {
    // fail-fast instead of waiting on the lock
    if (this.closing.get()) {
      String msg = "Skipping flush on " + this + " because closing";
      LOG.debug(msg);
      return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
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
        return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor pre-flush hooks");
        coprocessorHost.preFlush();
      }
      // TODO: this should be managed within memstore with the snapshot, updated only after flush
      // successful
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
          return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
        }
      }

      try {
        Collection<Store> specificStoresToFlush =
            forceFlushAllStores ? stores.values() : flushPolicy.selectStoresToFlush();
        FlushResult fs = internalFlushcache(specificStoresToFlush,
          status, writeFlushRequestWalMarker);

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
   * Should the store be flushed because it is old enough.
   * <p>
   * Every FlushPolicy should call this to determine whether a store is old enough to flush(except
   * that you always flush all stores). Otherwise the {@link #shouldFlush()} method will always
   * returns true which will make a lot of flush requests.
   */
  boolean shouldFlushStore(Store store) {
    long earliest = this.wal.getEarliestMemstoreSeqNum(getRegionInfo().getEncodedNameAsBytes(),
      store.getFamily().getName()) - 1;
    if (earliest > 0 && earliest + flushPerChanges < mvcc.getReadPoint()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush column family " + store.getColumnFamilyName() + " of " +
          getRegionInfo().getEncodedName() + " because unflushed sequenceid=" + earliest +
          " is > " + this.flushPerChanges + " from current=" + mvcc.getReadPoint());
      }
      return true;
    }
    if (this.flushCheckInterval <= 0) {
      return false;
    }
    long now = EnvironmentEdgeManager.currentTime();
    if (store.timeOfOldestEdit() < now - this.flushCheckInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush column family: " + store.getColumnFamilyName() + " of " +
          getRegionInfo().getEncodedName() + " because time of oldest edit=" +
            store.timeOfOldestEdit() + " is > " + this.flushCheckInterval + " from now =" + now);
      }
      return true;
    }
    return false;
  }

  /**
   * Should the memstore be flushed now
   */
  boolean shouldFlush(final StringBuffer whyFlush) {
    whyFlush.setLength(0);
    // This is a rough measure.
    if (this.maxFlushedSeqId > 0
          && (this.maxFlushedSeqId + this.flushPerChanges < this.mvcc.getReadPoint())) {
      whyFlush.append("more than max edits, " + this.flushPerChanges + ", since last flush");
      return true;
    }
    long modifiedFlushCheckInterval = flushCheckInterval;
    if (getRegionInfo().isSystemTable() &&
        getRegionInfo().getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID) {
      modifiedFlushCheckInterval = SYSTEM_CACHE_FLUSH_INTERVAL;
    }
    if (modifiedFlushCheckInterval <= 0) { //disabled
      return false;
    }
    long now = EnvironmentEdgeManager.currentTime();
    //if we flushed in the recent past, we don't need to do again now
    if ((now - getEarliestFlushTimeForAllStores() < modifiedFlushCheckInterval)) {
      return false;
    }
    //since we didn't flush in the recent past, flush now if certain conditions
    //are met. Return true on first such memstore hit.
    for (Store s : getStores()) {
      if (s.timeOfOldestEdit() < now - modifiedFlushCheckInterval) {
        // we have an old enough edit in the memstore, flush
        whyFlush.append(s.toString() + " has an old edit so flush to free WALs");
        return true;
      }
    }
    return false;
  }

  /**
   * Flushing all stores.
   *
   * @see #internalFlushcache(Collection, MonitoredTask, boolean)
   */
  private FlushResult internalFlushcache(MonitoredTask status)
      throws IOException {
    return internalFlushcache(stores.values(), status, false);
  }

  /**
   * Flushing given stores.
   *
   * @see #internalFlushcache(WAL, long, Collection, MonitoredTask, boolean)
   */
  private FlushResult internalFlushcache(final Collection<Store> storesToFlush,
      MonitoredTask status, boolean writeFlushWalMarker) throws IOException {
    return internalFlushcache(this.wal, HConstants.NO_SEQNUM, storesToFlush,
        status, writeFlushWalMarker);
  }

  /**
   * Flush the memstore. Flushing the memstore is a little tricky. We have a lot
   * of updates in the memstore, all of which have also been written to the wal.
   * We need to write those updates in the memstore out to disk, while being
   * able to process reads/writes as much as possible during the flush
   * operation.
   * <p>
   * This method may block for some time. Every time you call it, we up the
   * regions sequence id even if we don't flush; i.e. the returned region id
   * will be at least one larger than the last edit applied to this region. The
   * returned id does not refer to an actual edit. The returned id can be used
   * for say installing a bulk loaded file just ahead of the last hfile that was
   * the result of this flush, etc.
   *
   * @param wal
   *          Null if we're NOT to go via wal.
   * @param myseqid
   *          The seqid to use if <code>wal</code> is null writing out flush
   *          file.
   * @param storesToFlush
   *          The list of stores to flush.
   * @return object describing the flush's state
   * @throws IOException
   *           general io exceptions
   * @throws DroppedSnapshotException
   *           Thrown when replay of wal is required because a Snapshot was not
   *           properly persisted.
   */
  protected FlushResult internalFlushcache(final WAL wal, final long myseqid,
      final Collection<Store> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker)
          throws IOException {
    PrepareFlushResult result
      = internalPrepareFlushCache(wal, myseqid, storesToFlush, status, writeFlushWalMarker);
    if (result.result == null) {
      return internalFlushCacheAndCommit(wal, status, result, storesToFlush);
    } else {
      return result.result; // early exit due to failure from prepare stage
    }
  }

  protected PrepareFlushResult internalPrepareFlushCache(final WAL wal, final long myseqid,
      final Collection<Store> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker)
  throws IOException {
    if (this.rsServices != null && this.rsServices.isAborted()) {
      // Don't flush when server aborting, it's unsafe
      throw new IOException("Aborting flush because server is aborted...");
    }
    final long startTime = EnvironmentEdgeManager.currentTime();
    // If nothing to flush, return, but we need to safely update the region sequence id
    if (this.memstoreSize.get() <= 0) {
      // Take an update lock because am about to change the sequence id and we want the sequence id
      // to be at the border of the empty memstore.
      MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
      this.updatesLock.writeLock().lock();
      try {
        if (this.memstoreSize.get() <= 0) {
          // Presume that if there are still no edits in the memstore, then there are no edits for
          // this region out in the WAL subsystem so no need to do any trickery clearing out
          // edits in the WAL system. Up the sequence number so the resulting flush id is for
          // sure just beyond the last appended region edit (useful as a marker when bulk loading,
          // etc.). NOTE: The writeEntry write number is NOT in the WAL.. there is no WAL writing
          // here.
          if (wal != null) {
            writeEntry = mvcc.begin();
            long flushOpSeqId = writeEntry.getWriteNumber();
            FlushResult flushResult = new FlushResultImpl(
                FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY,
                flushOpSeqId,
                "Nothing to flush",
                writeFlushRequestMarkerToWAL(wal, writeFlushWalMarker));
            // TODO: Lets see if we hang here, if there is a scenario where an outstanding reader
            // with a read point is in advance of this write point.
            mvcc.completeAndWait(writeEntry);
            writeEntry = null;
            return new PrepareFlushResult(flushResult, myseqid);
          } else {
            return new PrepareFlushResult(
              new FlushResultImpl(
                  FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY,
                  "Nothing to flush",
                  false),
              myseqid);
          }
        }
      } finally {
        this.updatesLock.writeLock().unlock();
        if (writeEntry != null) {
          mvcc.complete(writeEntry);
        }
      }
    }

    if (LOG.isInfoEnabled()) {
      // Log a fat line detailing what is being flushed.
      StringBuilder perCfExtras = null;
      if (!isAllFamilies(storesToFlush)) {
        perCfExtras = new StringBuilder();
        for (Store store: storesToFlush) {
          perCfExtras.append("; ").append(store.getColumnFamilyName());
          perCfExtras.append("=").append(StringUtils.byteDesc(store.getMemStoreSize()));
        }
      }
      LOG.info("Flushing " + + storesToFlush.size() + "/" + stores.size() +
        " column families, memstore=" + StringUtils.byteDesc(this.memstoreSize.get()) +
        ((perCfExtras != null && perCfExtras.length() > 0)? perCfExtras.toString(): "") +
        ((wal != null) ? "" : "; WAL is null, using passed sequenceid=" + myseqid));
    }
    // Stop updates while we snapshot the memstore of all of these regions' stores. We only have
    // to do this for a moment.  It is quick. We also set the memstore size to zero here before we
    // allow updates again so its value will represent the size of the updates received
    // during flush

    // We have to take an update lock during snapshot, or else a write could end up in both snapshot
    // and memstore (makes it difficult to do atomic rows then)
    status.setStatus("Obtaining lock to block concurrent updates");
    // block waiting for the lock for internal flush
    this.updatesLock.writeLock().lock();
    status.setStatus("Preparing to flush by snapshotting stores in " +
      getRegionInfo().getEncodedName());
    long totalFlushableSizeOfFlushableStores = 0;

    Set<byte[]> flushedFamilyNames = new HashSet<byte[]>();
    for (Store store: storesToFlush) {
      flushedFamilyNames.add(store.getFamily().getName());
    }

    TreeMap<byte[], StoreFlushContext> storeFlushCtxs
      = new TreeMap<byte[], StoreFlushContext>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], List<Path>> committedFiles = new TreeMap<byte[], List<Path>>(
        Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], Long> storeFlushableSize
        = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    // The sequence id of this flush operation which is used to log FlushMarker and pass to
    // createFlushContext to use as the store file's sequence id. It can be in advance of edits
    // still in the memstore, edits that are in other column families yet to be flushed.
    long flushOpSeqId = HConstants.NO_SEQNUM;
    // The max flushed sequence id after this flush operation completes. All edits in memstore
    // will be in advance of this sequence id.
    long flushedSeqId = HConstants.NO_SEQNUM;
    byte[] encodedRegionName = getRegionInfo().getEncodedNameAsBytes();

    long trxId = 0;
    MultiVersionConcurrencyControl.WriteEntry writeEntry = mvcc.begin();
    try {
      try {
        if (wal != null) {
          Long earliestUnflushedSequenceIdForTheRegion =
            wal.startCacheFlush(encodedRegionName, flushedFamilyNames);
          if (earliestUnflushedSequenceIdForTheRegion == null) {
            // This should never happen. This is how startCacheFlush signals flush cannot proceed.
            String msg = this.getRegionInfo().getEncodedName() + " flush aborted; WAL closing.";
            status.setStatus(msg);
            return new PrepareFlushResult(
              new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false),
              myseqid);
          }
          flushOpSeqId = getNextSequenceId(wal);
          // Back up 1, minus 1 from oldest sequence id in memstore to get last 'flushed' edit
          flushedSeqId =
            earliestUnflushedSequenceIdForTheRegion.longValue() == HConstants.NO_SEQNUM?
              flushOpSeqId: earliestUnflushedSequenceIdForTheRegion.longValue() - 1;
        } else {
          // use the provided sequence Id as WAL is not being used for this flush.
          flushedSeqId = flushOpSeqId = myseqid;
        }

        for (Store s : storesToFlush) {
          totalFlushableSizeOfFlushableStores += s.getFlushableSize();
          storeFlushCtxs.put(s.getFamily().getName(), s.createFlushContext(flushOpSeqId));
          committedFiles.put(s.getFamily().getName(), null); // for writing stores to WAL
          storeFlushableSize.put(s.getFamily().getName(), s.getFlushableSize());
        }

        // write the snapshot start to WAL
        if (wal != null && !writestate.readOnly) {
          FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH,
            getRegionInfo(), flushOpSeqId, committedFiles);
          // no sync. Sync is below where we do not hold the updates lock
          trxId = WALUtil.writeFlushMarker(wal, this.htableDescriptor, getRegionInfo(),
            desc, false, mvcc);
        }

        // Prepare flush (take a snapshot)
        for (StoreFlushContext flush : storeFlushCtxs.values()) {
          flush.prepare();
        }
      } catch (IOException ex) {
        if (wal != null) {
          if (trxId > 0) { // check whether we have already written START_FLUSH to WAL
            try {
              FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
                getRegionInfo(), flushOpSeqId, committedFiles);
              WALUtil.writeFlushMarker(wal, this.htableDescriptor, getRegionInfo(),
                desc, false, mvcc);
            } catch (Throwable t) {
              LOG.warn("Received unexpected exception trying to write ABORT_FLUSH marker to WAL:" +
                  StringUtils.stringifyException(t));
              // ignore this since we will be aborting the RS with DSE.
            }
          }
          // we have called wal.startCacheFlush(), now we have to abort it
          wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
          throw ex; // let upper layers deal with it.
        }
      } finally {
        this.updatesLock.writeLock().unlock();
      }
      String s = "Finished memstore snapshotting " + this +
        ", syncing WAL and waiting on mvcc, flushsize=" + totalFlushableSizeOfFlushableStores;
      status.setStatus(s);
      if (LOG.isTraceEnabled()) LOG.trace(s);
      // sync unflushed WAL changes
      // see HBASE-8208 for details
      if (wal != null) {
        try {
          wal.sync(); // ensure that flush marker is sync'ed
        } catch (IOException ioe) {
          wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
          throw ioe;
        }
      }

      // wait for all in-progress transactions to commit to WAL before
      // we can start the flush. This prevents
      // uncommitted transactions from being written into HFiles.
      // We have to block before we start the flush, otherwise keys that
      // were removed via a rollbackMemstore could be written to Hfiles.
      mvcc.completeAndWait(writeEntry);
      // set writeEntry to null to prevent mvcc.complete from being called again inside finally
      // block
      writeEntry = null;
    } finally {
      if (writeEntry != null) {
        // In case of failure just mark current writeEntry as complete.
        mvcc.complete(writeEntry);
      }
    }
    return new PrepareFlushResult(storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
        flushOpSeqId, flushedSeqId, totalFlushableSizeOfFlushableStores);
  }

  /**
   * @param families
   * @return True if passed Set is all families in the region.
   */
  private boolean isAllFamilies(final Collection<Store> families) {
    return families == null || this.stores.size() == families.size();
  }

  /**
   * Writes a marker to WAL indicating a flush is requested but cannot be complete due to various
   * reasons. Ignores exceptions from WAL. Returns whether the write succeeded.
   * @param wal
   * @return whether WAL write was successful
   */
  private boolean writeFlushRequestMarkerToWAL(WAL wal, boolean writeFlushWalMarker) {
    if (writeFlushWalMarker && wal != null && !writestate.readOnly) {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.CANNOT_FLUSH,
        getRegionInfo(), -1, new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR));
      try {
        WALUtil.writeFlushMarker(wal, this.htableDescriptor, getRegionInfo(),
          desc, true, mvcc);
        return true;
      } catch (IOException e) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Received exception while trying to write the flush request to wal", e);
      }
    }
    return false;
  }

  protected FlushResult internalFlushCacheAndCommit(
        final WAL wal, MonitoredTask status, final PrepareFlushResult prepareResult,
        final Collection<Store> storesToFlush)
    throws IOException {

    // prepare flush context is carried via PrepareFlushResult
    TreeMap<byte[], StoreFlushContext> storeFlushCtxs = prepareResult.storeFlushCtxs;
    TreeMap<byte[], List<Path>> committedFiles = prepareResult.committedFiles;
    long startTime = prepareResult.startTime;
    long flushOpSeqId = prepareResult.flushOpSeqId;
    long flushedSeqId = prepareResult.flushedSeqId;
    long totalFlushableSizeOfFlushableStores = prepareResult.totalFlushableSize;

    String s = "Flushing stores of " + this;
    status.setStatus(s);
    if (LOG.isTraceEnabled()) LOG.trace(s);

    // Any failure from here on out will be catastrophic requiring server
    // restart so wal content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the wal, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    try {
      // A.  Flush memstore to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file. The new flushed file is still in the
      // tmp directory.

      for (StoreFlushContext flush : storeFlushCtxs.values()) {
        flush.flushCache(status);
      }

      // Switch snapshot (in memstore) -> new hfile (thus causing
      // all the store scanners to reset/reseek).
      Iterator<Store> it = storesToFlush.iterator();
      // stores.values() and storeFlushCtxs have same order
      for (StoreFlushContext flush : storeFlushCtxs.values()) {
        boolean needsCompaction = flush.commit(status);
        if (needsCompaction) {
          compactionRequested = true;
        }
        byte[] storeName = it.next().getFamily().getName();
        List<Path> storeCommittedFiles = flush.getCommittedFiles();
        committedFiles.put(storeName, storeCommittedFiles);
        // Flush committed no files, indicating flush is empty or flush was canceled
        if (storeCommittedFiles == null || storeCommittedFiles.isEmpty()) {
          totalFlushableSizeOfFlushableStores -= prepareResult.storeFlushableSize.get(storeName);
        }
      }
      storeFlushCtxs.clear();

      // Set down the memstore size by amount of flush.
      this.addAndGetGlobalMemstoreSize(-totalFlushableSizeOfFlushableStores);

      if (wal != null) {
        // write flush marker to WAL. If fail, we should throw DroppedSnapshotException
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.COMMIT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
        WALUtil.writeFlushMarker(wal, this.htableDescriptor, getRegionInfo(),
          desc, true, mvcc);
      }
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The wal needs to be replayed so its content is restored to memstore.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      if (wal != null) {
        try {
          FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
            getRegionInfo(), flushOpSeqId, committedFiles);
          WALUtil.writeFlushMarker(wal, this.htableDescriptor, getRegionInfo(),
            desc, false, mvcc);
        } catch (Throwable ex) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "failed writing ABORT_FLUSH marker to WAL", ex);
          // ignore this since we will be aborting the RS with DSE.
        }
        wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
      }
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
          Bytes.toStringBinary(getRegionInfo().getRegionName()));
      dse.initCause(t);
      status.abort("Flush failed: " + StringUtils.stringifyException(t));

      // Callers for flushcache() should catch DroppedSnapshotException and abort the region server.
      // However, since we may have the region read lock, we cannot call close(true) here since
      // we cannot promote to a write lock. Instead we are setting closing so that all other region
      // operations except for close will be rejected.
      this.closing.set(true);

      if (rsServices != null) {
        // This is a safeguard against the case where the caller fails to explicitly handle aborting
        rsServices.abort("Replay of WAL required. Forcing server shutdown", dse);
      }

      throw dse;
    }

    // If we get to here, the HStores have been written.
    if (wal != null) {
      wal.completeCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
    }

    // Record latest flush time
    for (Store store: storesToFlush) {
      this.lastStoreFlushTimeMap.put(store, startTime);
    }

    this.maxFlushedSeqId = flushedSeqId;
    this.lastFlushOpSeqId = flushOpSeqId;

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }

    long time = EnvironmentEdgeManager.currentTime() - startTime;
    long memstoresize = this.memstoreSize.get();
    String msg = "Finished memstore flush of ~"
        + StringUtils.byteDesc(totalFlushableSizeOfFlushableStores) + "/"
        + totalFlushableSizeOfFlushableStores + ", currentsize="
        + StringUtils.byteDesc(memstoresize) + "/" + memstoresize
        + " for region " + this + " in " + time + "ms, sequenceid="
        + flushOpSeqId +  ", compaction requested=" + compactionRequested
        + ((wal == null) ? "; wal=null" : "");
    LOG.info(msg);
    status.setStatus(msg);

    return new FlushResultImpl(compactionRequested ?
        FlushResult.Result.FLUSHED_COMPACTION_NEEDED :
          FlushResult.Result.FLUSHED_NO_COMPACTION_NEEDED,
        flushOpSeqId);
  }

  /**
   * Method to safely get the next sequence number.
   * @return Next sequence number unassociated with any actual edit.
   * @throws IOException
   */
  @VisibleForTesting
  protected long getNextSequenceId(final WAL wal) throws IOException {
    // TODO: For review. Putting an empty edit in to get a sequenceid out will not work if the
    // WAL is banjaxed... if it has gotten an exception and the WAL has not yet been rolled or
    // aborted. In this case, we'll just get stuck here. For now, until HBASE-12751, just have
    // a timeout. May happen in tests after we tightened the semantic via HBASE-14317.
    // Also, the getSequenceId blocks on a latch. There is no global list of outstanding latches
    // so if an abort or stop, there is no way to call them in.
    WALKey key = this.appendEmptyEdit(wal);
    mvcc.complete(key.getWriteEntry());
    return key.getSequenceId(this.maxWaitForSeqId);
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public Result getClosestRowBefore(final byte [] row, final byte [] family) throws IOException {
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
      Cell key = store.getRowKeyAtOrBefore(row);
      Result result = null;
      if (key != null) {
        Get get = new Get(CellUtil.cloneRow(key));
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

  @Override
  public RegionScanner getScanner(Scan scan) throws IOException {
   return getScanner(scan, null);
  }

  protected RegionScanner getScanner(Scan scan,
      List<KeyValueScanner> additionalScanners) throws IOException {
    startRegionOperation(Operation.SCAN);
    try {
      // Verify families are all valid
      if (!scan.hasFamilies()) {
        // Adding all families to scanner
        for (byte[] family: this.htableDescriptor.getFamiliesKeys()) {
          scan.addFamily(family);
        }
      } else {
        for (byte [] family : scan.getFamilyMap().keySet()) {
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

  @Override
  public void prepareDelete(Delete delete) throws IOException {
    // Check to see if this is a deleteRow insert
    if(delete.getFamilyCellMap().isEmpty()){
      for(byte [] family : this.htableDescriptor.getFamiliesKeys()){
        // Don't eat the timestamp
        delete.addFamily(family, delete.getTimeStamp());
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

  @Override
  public void delete(Delete delete) throws IOException {
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
   * @throws IOException
   */
  void delete(NavigableMap<byte[], List<Cell>> familyMap,
      Durability durability) throws IOException {
    Delete delete = new Delete(FOR_UNIT_TESTS_ONLY);
    delete.setFamilyCellMap(familyMap);
    delete.setDurability(durability);
    doBatchMutate(delete);
  }

  @Override
  public void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyMap,
      byte[] byteNow) throws IOException {
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {

      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;

      Map<byte[], Integer> kvCount = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        //  Check if time is LATEST, change to time of most recent addition if so
        //  This is expensive.
        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP && CellUtil.isDeleteType(cell)) {
          byte[] qual = CellUtil.cloneQualifier(cell);
          if (qual == null) qual = HConstants.EMPTY_BYTE_ARRAY;

          Integer count = kvCount.get(qual);
          if (count == null) {
            kvCount.put(qual, 1);
          } else {
            kvCount.put(qual, count + 1);
          }
          count = kvCount.get(qual);

          Get get = new Get(CellUtil.cloneRow(cell));
          get.setMaxVersions(count);
          get.addColumn(family, qual);
          if (coprocessorHost != null) {
            if (!coprocessorHost.prePrepareTimeStampForDeleteVersion(mutation, cell,
                byteNow, get)) {
              updateDeleteLatestVersionTimeStamp(cell, get, count, byteNow);
            }
          } else {
            updateDeleteLatestVersionTimeStamp(cell, get, count, byteNow);
          }
        } else {
          CellUtil.updateLatestStamp(cell, byteNow, 0);
        }
      }
    }
  }

  void updateDeleteLatestVersionTimeStamp(Cell cell, Get get, int count, byte[] byteNow)
      throws IOException {
    List<Cell> result = get(get, false);

    if (result.size() < count) {
      // Nothing to delete
      CellUtil.updateLatestStamp(cell, byteNow, 0);
      return;
    }
    if (result.size() > count) {
      throw new RuntimeException("Unexpected size: " + result.size());
    }
    Cell getCell = result.get(count - 1);
    CellUtil.setTimestamp(cell, getCell.getTimestamp());
  }

  @Override
  public void put(Put put) throws IOException {
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
    public abstract long getReplaySequenceId();

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

    @Override
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

    @Override
    public long getReplaySequenceId() {
      return 0;
    }
  }

  private static class ReplayBatch extends BatchOperationInProgress<MutationReplay> {
    private long replaySeqId = 0;
    public ReplayBatch(MutationReplay[] operations, long seqId) {
      super(operations);
      this.replaySeqId = seqId;
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

    @Override
    public long getReplaySequenceId() {
      return this.replaySeqId;
    }
  }

  @Override
  public OperationStatus[] batchMutate(Mutation[] mutations, long nonceGroup, long nonce)
      throws IOException {
    // As it stands, this is used for 3 things
    //  * batchMutate with single mutation - put/delete, separate or from checkAndMutate.
    //  * coprocessor calls (see ex. BulkDeleteEndpoint).
    // So nonces are not really ever used by HBase. They could be by coprocs, and checkAnd...
    return batchMutate(new MutationBatch(mutations, nonceGroup, nonce));
  }

  public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException {
    return batchMutate(mutations, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  @Override
  public OperationStatus[] batchReplay(MutationReplay[] mutations, long replaySeqId)
      throws IOException {
    if (!RegionReplicaUtil.isDefaultReplica(getRegionInfo())
        && replaySeqId < lastReplayedOpenRegionSeqId) {
      // if it is a secondary replica we should ignore these entries silently
      // since they are coming out of order
      if (LOG.isTraceEnabled()) {
        LOG.trace(getRegionInfo().getEncodedName() + " : "
          + "Skipping " + mutations.length + " mutations with replaySeqId=" + replaySeqId
          + " which is < than lastReplayedOpenRegionSeqId=" + lastReplayedOpenRegionSeqId);
        for (MutationReplay mut : mutations) {
          LOG.trace(getRegionInfo().getEncodedName() + " : Skipping : " + mut.mutation);
        }
      }

      OperationStatus[] statuses = new OperationStatus[mutations.length];
      for (int i = 0; i < statuses.length; i++) {
        statuses[i] = OperationStatus.SUCCESS;
      }
      return statuses;
    }
    return batchMutate(new ReplayBatch(mutations, replaySeqId));
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
    MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
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
    WALKey walKey = null;
    long mvccNum = 0;
    try {
      // ------------------------------------
      // STEP 1. Try to acquire as many locks as we can, and ensure
      // we acquire at least one.
      // ----------------------------------
      int numReadyToWrite = 0;
      long now = EnvironmentEdgeManager.currentTime();
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
          checkRow(mutation.getRow(), "doMiniBatchMutation");
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
        } catch (WrongRegionException we) {
          LOG.warn("Batch mutation had a row that does not belong to this region", we);
          batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
              OperationStatusCode.SANITY_CHECK_FAILURE, we.getMessage());
          lastIndexExclusive++;
          continue;
        }

        // If we haven't got any rows in our batch, we should block to
        // get the next one.
        RowLock rowLock = null;
        try {
          rowLock = getRowLock(mutation.getRow(), true);
        } catch (IOException ioe) {
          LOG.warn("Failed getting lock in batch put, row="
            + Bytes.toStringBinary(mutation.getRow()), ioe);
          throw ioe;
        }
        if (rowLock == null) {
          // We failed to grab another lock
          throw new IOException("Failed getting lock in batch put, row=" +
              Bytes.toStringBinary(mutation.getRow()));
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
      now = EnvironmentEdgeManager.currentTime();
      byte[] byteNow = Bytes.toBytes(now);

      // Nothing to put/delete -- an exception in the above such as NoSuchColumnFamily?
      if (numReadyToWrite <= 0) return 0L;

      // We've now grabbed as many mutations off the list as we can

      // ------------------------------------
      // STEP 2. Update any LATEST_TIMESTAMP timestamps
      // ----------------------------------
      for (int i = firstIndex; !isInReplay && i < lastIndexExclusive; i++) {
        // skip invalid
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) continue;

        Mutation mutation = batchOp.getMutation(i);
        if (mutation instanceof Put) {
          updateCellTimestamps(familyMaps[i].values(), byteNow);
          noOfPuts++;
        } else {
          prepareDeleteTimestamps(mutation, familyMaps[i], byteNow);
          noOfDeletes++;
        }
        rewriteCellTags(familyMaps[i], mutation);
      }

      lock(this.updatesLock.readLock(), numReadyToWrite);
      locked = true;

      // calling the pre CP hook for batch mutation
      if (!isInReplay && coprocessorHost != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
          new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        if (coprocessorHost.preBatchMutate(miniBatchOp)) return 0L;
      }

      // ------------------------------------
      // STEP 3. Build WAL edit
      // ----------------------------------
      Durability durability = Durability.USE_DEFAULT;
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        // Skip puts that were determined to be invalid during preprocessing
        if (batchOp.retCodeDetails[i].getOperationStatusCode() != OperationStatusCode.NOT_RUN) {
          continue;
        }

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
            // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
            walKey = new ReplayHLogKey(this.getRegionInfo().getEncodedNameAsBytes(),
              this.htableDescriptor.getTableName(), now, m.getClusterIds(),
              currentNonceGroup, currentNonce, mvcc);
            txid = this.wal.append(this.htableDescriptor,  this.getRegionInfo(),  walKey,
              walEdit, true);
            walEdit = new WALEdit(isInReplay);
            walKey = null;
          }
          currentNonceGroup = nonceGroup;
          currentNonce = nonce;
        }

        // Add WAL edits by CP
        WALEdit fromCP = batchOp.walEditsFromCoprocessors[i];
        if (fromCP != null) {
          for (Cell cell : fromCP.getCells()) {
            walEdit.add(cell);
          }
        }
        addFamilyMapToWALEdit(familyMaps[i], walEdit);
      }

      // -------------------------
      // STEP 4. Append the final edit to WAL. Do not sync wal.
      // -------------------------
      Mutation mutation = batchOp.getMutation(firstIndex);
      if (isInReplay) {
        // use wal key from the original
        walKey = new ReplayHLogKey(this.getRegionInfo().getEncodedNameAsBytes(),
          this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
          mutation.getClusterIds(), currentNonceGroup, currentNonce, mvcc);
        long replaySeqId = batchOp.getReplaySequenceId();
        walKey.setOrigLogSeqNum(replaySeqId);
      }
      if (walEdit.size() > 0) {
        if (!isInReplay) {
        // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
        walKey = new HLogKey(this.getRegionInfo().getEncodedNameAsBytes(),
            this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
            mutation.getClusterIds(), currentNonceGroup, currentNonce, mvcc);
        }
        txid = this.wal.append(this.htableDescriptor, this.getRegionInfo(), walKey, walEdit, true);
      }
      // ------------------------------------
      // Acquire the latest mvcc number
      // ----------------------------------
      if (walKey == null) {
        // If this is a skip wal operation just get the read point from mvcc
        walKey = this.appendEmptyEdit(this.wal);
      }
      if (!isInReplay) {
        writeEntry = walKey.getWriteEntry();
        mvccNum = writeEntry.getWriteNumber();
      } else {
        mvccNum = batchOp.getReplaySequenceId();
      }

      // ------------------------------------
      // STEP 5. Write back to memstore
      // Write to memstore. It is ok to write to memstore
      // first without syncing the WAL because we do not roll
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
        addedSize += applyFamilyMapToMemstore(familyMaps[i], mvccNum, isInReplay);
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
      if (txid != 0) {
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
      if (writeEntry != null) {
        mvcc.completeAndWait(writeEntry);
        writeEntry = null;
      } else if (isInReplay) {
        // ensure that the sequence id of the region is at least as big as orig log seq id
        mvcc.advanceTo(mvccNum);
      }

      for (int i = firstIndex; i < lastIndexExclusive; i ++) {
        if (batchOp.retCodeDetails[i] == OperationStatus.NOT_RUN) {
          batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
        }
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
        for (int j = 0; j < familyMaps.length; j++) {
          for(List<Cell> cells:familyMaps[j].values()) {
            rollbackMemstore(cells);
          }
        }
        if (writeEntry != null) mvcc.complete(writeEntry);
      } else if (writeEntry != null) {
        mvcc.completeAndWait(writeEntry);
      }

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

  @Override
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
      mvcc.await();
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
        long cellTs = 0;
        if (result.size() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 &&
            valueIsNull) {
          matches = true;
          cellTs = result.get(0).getTimestamp();
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          cellTs = kv.getTimestamp();
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
          // We have acquired the row lock already. If the system clock is NOT monotonically
          // non-decreasing (see HBASE-14070) we should make sure that the mutation has a
          // larger timestamp than what was observed via Get. doBatchMutate already does this, but
          // there is no way to pass the cellTs. See HBASE-14054.
          long now = EnvironmentEdgeManager.currentTime();
          long ts = Math.max(now, cellTs); // ensure write is not eclipsed
          byte[] byteTs = Bytes.toBytes(ts);

          if (w instanceof Put) {
            updateCellTimestamps(w.getFamilyCellMap().values(), byteTs);
          }
          // else delete is not needed since it already does a second get, and sets the timestamp
          // from get (see prepareDeleteTimestamps).

          // All edits for the given row (across all column families) must
          // happen atomically.
          doBatchMutate(w);
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

  @Override
  public boolean checkAndRowMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, RowMutations rm,
      boolean writeToWAL) throws IOException {
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
      mvcc.await();
      try {
        List<Cell> result = get(get, false);

        boolean valueIsNull = comparator.getValue() == null ||
            comparator.getValue().length == 0;
        boolean matches = false;
        long cellTs = 0;
        if (result.size() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 &&
            valueIsNull) {
          matches = true;
          cellTs = result.get(0).getTimestamp();
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          cellTs = kv.getTimestamp();
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
          // We have acquired the row lock already. If the system clock is NOT monotonically
          // non-decreasing (see HBASE-14070) we should make sure that the mutation has a
          // larger timestamp than what was observed via Get. doBatchMutate already does this, but
          // there is no way to pass the cellTs. See HBASE-14054.
          long now = EnvironmentEdgeManager.currentTime();
          long ts = Math.max(now, cellTs); // ensure write is not eclipsed
          byte[] byteTs = Bytes.toBytes(ts);

          for (Mutation w : rm.getMutations()) {
            if (w instanceof Put) {
              updateCellTimestamps(w.getFamilyCellMap().values(), byteTs);
            }
            // else delete is not needed since it already does a second get, and sets the timestamp
            // from get (see prepareDeleteTimestamps).
          }

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

  private void doBatchMutate(Mutation mutation) throws IOException {
    // Currently this is only called for puts and deletes, so no nonces.
    OperationStatus[] batchMutate = this.batchMutate(new Mutation[]{mutation});
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
   * @param desc snapshot description object
   * @param exnSnare ForeignExceptionSnare that captures external exceptions in case we need to
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

  @Override
  public void updateCellTimestamps(final Iterable<List<Cell>> cellItr, final byte[] now)
      throws IOException {
    for (List<Cell> cells: cellItr) {
      if (cells == null) continue;
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
        CellUtil.updateLatestStamp(cells.get(i), now, 0);
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
          cell.getTagsOffset(), cell.getTagsLength());

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
  private void checkResources() throws RegionTooBusyException {
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
    if (isReadOnly()) {
      throw new DoNotRetryIOException("region is read only");
    }
  }

  protected void checkReadsEnabled() throws IOException {
    if (!this.writestate.readsEnabled) {
      throw new IOException(getRegionInfo().getEncodedName()
        + ": The region's reads are disabled. Cannot serve the request");
    }
  }

  public void setReadsEnabled(boolean readsEnabled) {
   if (readsEnabled && !this.writestate.readsEnabled) {
     LOG.info(getRegionInfo().getEncodedName() + " : Enabling reads for region.");
    }
    this.writestate.setReadsEnabled(readsEnabled);
  }

  /**
   * Add updates first to the wal and then add values to memstore.
   * Warning: Assumption is caller has lock on passed in row.
   * @param edits Cell updates by column
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
   * @param output newly added KVs into memstore
   * @param isInReplay true when adding replayed KVs into memstore
   * @return the additional memory usage of the memstore caused by the
   * new entries.
   * @throws IOException
   */
  private long applyFamilyMapToMemstore(Map<byte[], List<Cell>> familyMap,
    long mvccNum, boolean isInReplay) throws IOException {
    long size = 0;

    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;
      Store store = getStore(family);
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        if (cell.getSequenceId() == 0 || isInReplay) {
          CellUtil.setSequenceId(cell, mvccNum);
        }
        size += store.add(cell);
      }
    }

     return size;
   }

  /**
   * Remove all the keys listed in the map from the memstore. This method is
   * called when a Put/Delete has updated memstore but subsequently fails to update
   * the wal. This method is then invoked to rollback the memstore.
   */
  private void rollbackMemstore(List<Cell> memstoreCells) {
    int kvsRolledback = 0;

    for (Cell cell : memstoreCells) {
      byte[] family = CellUtil.cloneFamily(cell);
      Store store = getStore(family);
      store.rollback(cell);
      kvsRolledback++;
    }
    LOG.debug("rollbackMemstore rolled back " + kvsRolledback);
  }

  @Override
  public void checkFamilies(Collection<byte[]> families) throws NoSuchColumnFamilyException {
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

  @Override
  public void checkTimestamps(final Map<byte[], List<Cell>> familyMap, long now)
      throws FailedSanityCheckException {
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
        long ts = cell.getTimestamp();
        if (ts != HConstants.LATEST_TIMESTAMP && ts > maxTs) {
          throw new FailedSanityCheckException("Timestamp for KV out of range "
              + cell + " (too.new=" + timestampSlop + ")");
        }
      }
    }
  }

  /**
   * Append the given map of family->edits to a WALEdit data structure.
   * This does not write to the WAL itself.
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
        walEdit.add(cell);
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
    this.rsServices.getFlushRequester().requestFlush(this, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flush requested on " + this.getRegionInfo().getEncodedName());
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
   * Read the edits put under this region by wal splitting process.  Put
   * the recovered edits back up into this region.
   *
   * <p>We can ignore any wal message that has a sequence ID that's equal to or
   * lower than minSeqId.  (Because we know such messages are already
   * reflected in the HFiles.)
   *
   * <p>While this is running we are putting pressure on memory yet we are
   * outside of our usual accounting because we are not yet an onlined region
   * (this stuff is being run as part of Region initialization).  This means
   * that if we're up against global memory limits, we'll not be flagged to flush
   * because we are not online. We can't be flushed by usual mechanisms anyways;
   * we're not yet online so our relative sequenceids are not yet aligned with
   * WAL sequenceids -- not till we come up online, post processing of split
   * edits.
   *
   * <p>But to help relieve memory pressure, at least manage our own heap size
   * flushing if are in excess of per-region limits.  Flushing, though, we have
   * to be careful and avoid using the regionserver/wal sequenceid.  Its running
   * on a different line to whats going on in here in this region context so if we
   * crashed replaying these edits, but in the midst had a flush that used the
   * regionserver wal with a sequenceid in excess of whats going on in here
   * in this region and with its split editlogs, then we could miss edits the
   * next time we go to recover. So, we have to flush inline, using seqids that
   * make sense in a this single region context only -- until we online.
   *
   * @param maxSeqIdInStores Any edit found in split editlogs needs to be in excess of
   * the maxSeqId for the store to be applied, else its skipped.
   * @return the sequence id of the last edit added to this region out of the
   * recovered edits log or <code>minSeqId</code> if nothing added from editlogs.
   * @throws IOException
   */
  protected long replayRecoveredEditsIfAny(final Path regiondir,
      Map<byte[], Long> maxSeqIdInStores,
      final CancelableProgressable reporter, final MonitoredTask status)
      throws IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqid = minSeqIdForTheRegion;

    FileSystem fs = this.fs.getFileSystem();
    NavigableSet<Path> files = WALSplitter.getSplitEditFilesSorted(fs, regiondir);
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
          String msg = "Maximum sequenceid for this wal is " + maxSeqId
            + " and minimum sequenceid for the region is " + minSeqIdForTheRegion
            + ", skipped the whole file, path=" + edits;
          LOG.debug(msg);
        }
        continue;
      }

      try {
        // replay the edits. Replay can return -1 if everything is skipped, only update
        // if seqId is greater
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
          Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
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
      this.rsAccounting.clearRegionReplayEditsSize(getRegionInfo().getRegionName());
    }
    if (seqid > minSeqIdForTheRegion) {
      // Then we added some edits to memory. Flush and cleanup split edit files.
      internalFlushcache(null, seqid, stores.values(), status, false);
    }
    // Now delete the content of recovered edits.  We're done w/ them.
    if (files.size() > 0 && this.conf.getBoolean("hbase.region.archive.recovered.edits", false)) {
      // For debugging data loss issues!
      // If this flag is set, make use of the hfile archiving by making recovered.edits a fake
      // column family. Have to fake out file type too by casting our recovered.edits as storefiles
      String fakeFamilyName = WALSplitter.getRegionDirRecoveredEditsDir(regiondir).getName();
      Set<StoreFile> fakeStoreFiles = new HashSet<StoreFile>(files.size());
      for (Path file: files) {
        fakeStoreFiles.add(new StoreFile(getRegionFileSystem().getFileSystem(), file, this.conf,
          null, null));
      }
      getRegionFileSystem().removeStoreFiles(fakeFamilyName, fakeStoreFiles);
    } else {
      for (Path file: files) {
        if (!fs.delete(file, false)) {
          LOG.error("Failed delete of " + file);
        } else {
          LOG.debug("Deleted recovered.edits file=" + file);
        }
      }
    }
    return seqid;
  }

  /*
   * @param edits File of recovered edits.
   * @param maxSeqIdInStores Maximum sequenceid found in each store.  Edits in wal
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

    status.setStatus("Opening recovered edits");
    WAL.Reader reader = null;
    try {
      reader = WALFactory.createReader(fs, edits, conf);
      long currentEditSeqId = -1;
      long currentReplaySeqId = -1;
      long firstSeqIdInLog = -1;
      long skippedEdits = 0;
      long editsCount = 0;
      long intervalEdits = 0;
      WAL.Entry entry;
      Store store = null;
      boolean reported_once = false;
      ServerNonceManager ng = this.rsServices == null ? null : this.rsServices.getNonceManager();

      try {
        // How many edits seen before we check elapsed time
        int interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
        // How often to send a progress report (default 1/2 master timeout)
        int period = this.conf.getInt("hbase.hstore.report.period", 300000);
        long lastReport = EnvironmentEdgeManager.currentTime();

        while ((entry = reader.next()) != null) {
          WALKey key = entry.getKey();
          WALEdit val = entry.getEdit();

          if (ng != null) { // some test, or nonces disabled
            ng.reportOperationFromWal(key.getNonceGroup(), key.getNonce(), key.getWriteTime());
          }

          if (reporter != null) {
            intervalEdits += val.size();
            if (intervalEdits >= interval) {
              // Number of edits interval reached
              intervalEdits = 0;
              long cur = EnvironmentEdgeManager.currentTime();
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
          if (currentEditSeqId > key.getLogSeqNum()) {
            // when this condition is true, it means we have a serious defect because we need to
            // maintain increasing SeqId for WAL edits per region
            LOG.error(getRegionInfo().getEncodedName() + " : "
                 + "Found decreasing SeqId. PreId=" + currentEditSeqId + " key=" + key
                + "; edit=" + val);
          } else {
            currentEditSeqId = key.getLogSeqNum();
          }
          currentReplaySeqId = (key.getOrigLogSeqNum() > 0) ?
            key.getOrigLogSeqNum() : currentEditSeqId;

          // Start coprocessor replay here. The coprocessor is for each WALEdit
          // instead of a KeyValue.
          if (coprocessorHost != null) {
            status.setStatus("Running pre-WAL-restore hook in coprocessors");
            if (coprocessorHost.preWALRestore(this.getRegionInfo(), key, val)) {
              // if bypass this wal entry, ignore it ...
              continue;
            }
          }
          // Check this edit is for this region.
          if (!Bytes.equals(key.getEncodedRegionName(),
              this.getRegionInfo().getEncodedNameAsBytes())) {
            skippedEdits++;
            continue;
          }

          boolean flush = false;
          for (Cell cell: val.getCells()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
              //this is a special edit, we should handle it
              CompactionDescriptor compaction = WALEdit.getCompaction(cell);
              if (compaction != null) {
                //replay the compaction
                replayWALCompactionMarker(compaction, false, true, Long.MAX_VALUE);
              }
              skippedEdits++;
              continue;
            }
            // Figure which store the edit is meant for.
            if (store == null || !CellUtil.matchingFamily(cell, store.getFamily().getName())) {
              store = getStore(cell);
            }
            if (store == null) {
              // This should never happen.  Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for " + cell);
              skippedEdits++;
              continue;
            }
            // Now, figure if we should skip this edit.
            if (key.getLogSeqNum() <= maxSeqIdInStores.get(store.getFamily()
                .getName())) {
              skippedEdits++;
              continue;
            }
            CellUtil.setSequenceId(cell, currentReplaySeqId);

            // Once we are over the limit, restoreEdit will keep returning true to
            // flush -- but don't flush until we've played all the kvs that make up
            // the WALEdit.
            flush |= restoreEdit(store, cell);
            editsCount++;
          }
          if (flush) {
            internalFlushcache(null, currentEditSeqId, stores.values(), status, false);
          }

          if (coprocessorHost != null) {
            coprocessorHost.postWALRestore(this.getRegionInfo(), key, val);
          }
        }
      } catch (EOFException eof) {
        Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
        msg = "Encountered EOF. Most likely due to Master failure during " +
            "wal splitting, so we have this data in another edit.  " +
            "Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
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
        ", firstSequenceIdInLog=" + firstSeqIdInLog +
        ", maxSequenceIdInLog=" + currentEditSeqId + ", path=" + edits;
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
   */
  void replayWALCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,
      boolean removeFiles, long replaySeqId)
      throws IOException {
    checkTargetRegion(compaction.getEncodedRegionName().toByteArray(),
      "Compaction marker from WAL ", compaction);

    synchronized (writestate) {
      if (replaySeqId < lastReplayedOpenRegionSeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Skipping replaying compaction event :" + TextFormat.shortDebugString(compaction)
            + " because its sequence id " + replaySeqId + " is smaller than this regions "
            + "lastReplayedOpenRegionSeqId of " + lastReplayedOpenRegionSeqId);
        return;
      }
      if (replaySeqId < lastReplayedCompactionSeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Skipping replaying compaction event :" + TextFormat.shortDebugString(compaction)
            + " because its sequence id " + replaySeqId + " is smaller than this regions "
            + "lastReplayedCompactionSeqId of " + lastReplayedCompactionSeqId);
        return;
      } else {
        lastReplayedCompactionSeqId = replaySeqId;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(getRegionInfo().getEncodedName() + " : "
            + "Replaying compaction marker " + TextFormat.shortDebugString(compaction)
            + " with seqId=" + replaySeqId + " and lastReplayedOpenRegionSeqId="
            + lastReplayedOpenRegionSeqId);
      }

      startRegionOperation(Operation.REPLAY_EVENT);
      try {
        Store store = this.getStore(compaction.getFamilyName().toByteArray());
        if (store == null) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Found Compaction WAL edit for deleted family:"
              + Bytes.toString(compaction.getFamilyName().toByteArray()));
          return;
        }
        store.replayCompactionMarker(compaction, pickCompactionFiles, removeFiles);
        logRegionFiles();
      } catch (FileNotFoundException ex) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "At least one of the store files in compaction: "
            + TextFormat.shortDebugString(compaction)
            + " doesn't exist any more. Skip loading the file(s)", ex);
      } finally {
        closeRegionOperation(Operation.REPLAY_EVENT);
      }
    }
  }

  void replayWALFlushMarker(FlushDescriptor flush, long replaySeqId) throws IOException {
    checkTargetRegion(flush.getEncodedRegionName().toByteArray(),
      "Flush marker from WAL ", flush);

    if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : "
          + "Replaying flush marker " + TextFormat.shortDebugString(flush));
    }

    startRegionOperation(Operation.REPLAY_EVENT); // use region close lock to guard against close
    try {
      FlushAction action = flush.getAction();
      switch (action) {
      case START_FLUSH:
        replayWALFlushStartMarker(flush);
        break;
      case COMMIT_FLUSH:
        replayWALFlushCommitMarker(flush);
        break;
      case ABORT_FLUSH:
        replayWALFlushAbortMarker(flush);
        break;
      case CANNOT_FLUSH:
        replayWALFlushCannotFlushMarker(flush, replaySeqId);
        break;
      default:
        LOG.warn(getRegionInfo().getEncodedName() + " : " +
          "Received a flush event with unknown action, ignoring. " +
          TextFormat.shortDebugString(flush));
        break;
      }

      logRegionFiles();
    } finally {
      closeRegionOperation(Operation.REPLAY_EVENT);
    }
  }

  /** Replay the flush marker from primary region by creating a corresponding snapshot of
   * the store memstores, only if the memstores do not have a higher seqId from an earlier wal
   * edit (because the events may be coming out of order).
   */
  @VisibleForTesting
  PrepareFlushResult replayWALFlushStartMarker(FlushDescriptor flush) throws IOException {
    long flushSeqId = flush.getFlushSequenceNumber();

    HashSet<Store> storesToFlush = new HashSet<Store>();
    for (StoreFlushDescriptor storeFlush : flush.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      Store store = getStore(family);
      if (store == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Received a flush start marker from primary, but the family is not found. Ignoring"
          + " StoreFlushDescriptor:" + TextFormat.shortDebugString(storeFlush));
        continue;
      }
      storesToFlush.add(store);
    }

    MonitoredTask status = TaskMonitor.get().createStatus("Preparing flush " + this);

    // we will use writestate as a coarse-grain lock for all the replay events
    // (flush, compaction, region open etc)
    synchronized (writestate) {
      try {
        if (flush.getFlushSequenceNumber() < lastReplayedOpenRegionSeqId) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Skipping replaying flush event :" + TextFormat.shortDebugString(flush)
              + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
              + " of " + lastReplayedOpenRegionSeqId);
          return null;
        }
        if (numMutationsWithoutWAL.get() > 0) {
          numMutationsWithoutWAL.set(0);
          dataInMemoryWithoutWAL.set(0);
        }

        if (!writestate.flushing) {
          // we do not have an active snapshot and corresponding this.prepareResult. This means
          // we can just snapshot our memstores and continue as normal.

          // invoke prepareFlushCache. Send null as wal since we do not want the flush events in wal
          PrepareFlushResult prepareResult = internalPrepareFlushCache(null,
            flushSeqId, storesToFlush, status, false);
          if (prepareResult.result == null) {
            // save the PrepareFlushResult so that we can use it later from commit flush
            this.writestate.flushing = true;
            this.prepareFlushResult = prepareResult;
            status.markComplete("Flush prepare successful");
            if (LOG.isDebugEnabled()) {
              LOG.debug(getRegionInfo().getEncodedName() + " : "
                  + " Prepared flush with seqId:" + flush.getFlushSequenceNumber());
            }
          } else {
            // special case empty memstore. We will still save the flush result in this case, since
            // our memstore ie empty, but the primary is still flushing
            if (prepareResult.getResult().getResult() ==
                  FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
              this.writestate.flushing = true;
              this.prepareFlushResult = prepareResult;
              if (LOG.isDebugEnabled()) {
                LOG.debug(getRegionInfo().getEncodedName() + " : "
                  + " Prepared empty flush with seqId:" + flush.getFlushSequenceNumber());
              }
            }
            status.abort("Flush prepare failed with " + prepareResult.result);
            // nothing much to do. prepare flush failed because of some reason.
          }
          return prepareResult;
        } else {
          // we already have an active snapshot.
          if (flush.getFlushSequenceNumber() == this.prepareFlushResult.flushOpSeqId) {
            // They define the same flush. Log and continue.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a flush prepare marker with the same seqId: " +
                + flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
                + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // ignore
          } else if (flush.getFlushSequenceNumber() < this.prepareFlushResult.flushOpSeqId) {
            // We received a flush with a smaller seqNum than what we have prepared. We can only
            // ignore this prepare flush request.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a flush prepare marker with a smaller seqId: " +
                + flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
                + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // ignore
          } else {
            // We received a flush with a larger seqNum than what we have prepared
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a flush prepare marker with a larger seqId: " +
                + flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
                + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // We do not have multiple active snapshots in the memstore or a way to merge current
            // memstore snapshot with the contents and resnapshot for now. We cannot take
            // another snapshot and drop the previous one because that will cause temporary
            // data loss in the secondary. So we ignore this for now, deferring the resolution
            // to happen when we see the corresponding flush commit marker. If we have a memstore
            // snapshot with x, and later received another prepare snapshot with y (where x < y),
            // when we see flush commit for y, we will drop snapshot for x, and can also drop all
            // the memstore edits if everything in memstore is < y. This is the usual case for
            // RS crash + recovery where we might see consequtive prepare flush wal markers.
            // Otherwise, this will cause more memory to be used in secondary replica until a
            // further prapare + commit flush is seen and replayed.
          }
        }
      } finally {
        status.cleanup();
        writestate.notifyAll();
      }
    }
    return null;
  }

  @VisibleForTesting
  void replayWALFlushCommitMarker(FlushDescriptor flush) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Committing flush " + this);

    // check whether we have the memstore snapshot with the corresponding seqId. Replay to
    // secondary region replicas are in order, except for when the region moves or then the
    // region server crashes. In those cases, we may receive replay requests out of order from
    // the original seqIds.
    synchronized (writestate) {
      try {
        if (flush.getFlushSequenceNumber() < lastReplayedOpenRegionSeqId) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Skipping replaying flush event :" + TextFormat.shortDebugString(flush)
            + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
            + " of " + lastReplayedOpenRegionSeqId);
          return;
        }

        if (writestate.flushing) {
          PrepareFlushResult prepareFlushResult = this.prepareFlushResult;
          if (flush.getFlushSequenceNumber() == prepareFlushResult.flushOpSeqId) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(getRegionInfo().getEncodedName() + " : "
                  + "Received a flush commit marker with seqId:" + flush.getFlushSequenceNumber()
                  + " and a previous prepared snapshot was found");
            }
            // This is the regular case where we received commit flush after prepare flush
            // corresponding to the same seqId.
            replayFlushInStores(flush, prepareFlushResult, true);

            // Set down the memstore size by amount of flush.
            this.addAndGetGlobalMemstoreSize(-prepareFlushResult.totalFlushableSize);

            this.prepareFlushResult = null;
            writestate.flushing = false;
          } else if (flush.getFlushSequenceNumber() < prepareFlushResult.flushOpSeqId) {
            // This should not happen normally. However, lets be safe and guard against these cases
            // we received a flush commit with a smaller seqId than what we have prepared
            // we will pick the flush file up from this commit (if we have not seen it), but we
            // will not drop the memstore
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a flush commit marker with smaller seqId: "
                + flush.getFlushSequenceNumber() + " than what we have prepared with seqId: "
                + prepareFlushResult.flushOpSeqId + ". Picking up new file, but not dropping"
                +"  prepared memstore snapshot");
            replayFlushInStores(flush, prepareFlushResult, false);

            // snapshot is not dropped, so memstore sizes should not be decremented
            // we still have the prepared snapshot, flushing should still be true
          } else {
            // This should not happen normally. However, lets be safe and guard against these cases
            // we received a flush commit with a larger seqId than what we have prepared
            // we will pick the flush file for this. We will also obtain the updates lock and
            // look for contents of the memstore to see whether we have edits after this seqId.
            // If not, we will drop all the memstore edits and the snapshot as well.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a flush commit marker with larger seqId: "
                + flush.getFlushSequenceNumber() + " than what we have prepared with seqId: " +
                prepareFlushResult.flushOpSeqId + ". Picking up new file and dropping prepared"
                +" memstore snapshot");

            replayFlushInStores(flush, prepareFlushResult, true);

            // Set down the memstore size by amount of flush.
            this.addAndGetGlobalMemstoreSize(-prepareFlushResult.totalFlushableSize);

            // Inspect the memstore contents to see whether the memstore contains only edits
            // with seqId smaller than the flush seqId. If so, we can discard those edits.
            dropMemstoreContentsForSeqId(flush.getFlushSequenceNumber(), null);

            this.prepareFlushResult = null;
            writestate.flushing = false;
          }
          // If we were waiting for observing a flush or region opening event for not showing
          // partial data after a secondary region crash, we can allow reads now. We can only make
          // sure that we are not showing partial data (for example skipping some previous edits)
          // until we observe a full flush start and flush commit. So if we were not able to find
          // a previous flush we will not enable reads now.
          this.setReadsEnabled(true);
        } else {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush commit marker with seqId:" + flush.getFlushSequenceNumber()
              + ", but no previous prepared snapshot was found");
          // There is no corresponding prepare snapshot from before.
          // We will pick up the new flushed file
          replayFlushInStores(flush, null, false);

          // Inspect the memstore contents to see whether the memstore contains only edits
          // with seqId smaller than the flush seqId. If so, we can discard those edits.
          dropMemstoreContentsForSeqId(flush.getFlushSequenceNumber(), null);
        }

        status.markComplete("Flush commit successful");

        // Update the last flushed sequence id for region.
        this.maxFlushedSeqId = flush.getFlushSequenceNumber();

        // advance the mvcc read point so that the new flushed file is visible.
        mvcc.advanceTo(flush.getFlushSequenceNumber());

      } catch (FileNotFoundException ex) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "At least one of the store files in flush: " + TextFormat.shortDebugString(flush)
            + " doesn't exist any more. Skip loading the file(s)", ex);
      }
      finally {
        status.cleanup();
        writestate.notifyAll();
      }
    }

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }
  }

  /**
   * Replays the given flush descriptor by opening the flush files in stores and dropping the
   * memstore snapshots if requested.
   * @param flush
   * @param prepareFlushResult
   * @param dropMemstoreSnapshot
   * @throws IOException
   */
  private void replayFlushInStores(FlushDescriptor flush, PrepareFlushResult prepareFlushResult,
      boolean dropMemstoreSnapshot)
      throws IOException {
    for (StoreFlushDescriptor storeFlush : flush.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      Store store = getStore(family);
      if (store == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Received a flush commit marker from primary, but the family is not found."
            + "Ignoring StoreFlushDescriptor:" + storeFlush);
        continue;
      }
      List<String> flushFiles = storeFlush.getFlushOutputList();
      StoreFlushContext ctx = null;
      long startTime = EnvironmentEdgeManager.currentTime();
      if (prepareFlushResult == null || prepareFlushResult.storeFlushCtxs == null) {
        ctx = store.createFlushContext(flush.getFlushSequenceNumber());
      } else {
        ctx = prepareFlushResult.storeFlushCtxs.get(family);
        startTime = prepareFlushResult.startTime;
      }

      if (ctx == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Unexpected: flush commit marker received from store "
            + Bytes.toString(family) + " but no associated flush context. Ignoring");
        continue;
      }

      ctx.replayFlush(flushFiles, dropMemstoreSnapshot); // replay the flush

      // Record latest flush time
      this.lastStoreFlushTimeMap.put(store, startTime);
    }
  }

  /**
   * Drops the memstore contents after replaying a flush descriptor or region open event replay
   * if the memstore edits have seqNums smaller than the given seq id
   * @throws IOException
   */
  private long dropMemstoreContentsForSeqId(long seqId, Store store) throws IOException {
    long totalFreedSize = 0;
    this.updatesLock.writeLock().lock();
    try {

      long currentSeqId = mvcc.getReadPoint();
      if (seqId >= currentSeqId) {
        // then we can drop the memstore contents since everything is below this seqId
        LOG.info(getRegionInfo().getEncodedName() + " : "
            + "Dropping memstore contents as well since replayed flush seqId: "
            + seqId + " is greater than current seqId:" + currentSeqId);

        // Prepare flush (take a snapshot) and then abort (drop the snapshot)
        if (store == null) {
          for (Store s : stores.values()) {
            totalFreedSize += doDropStoreMemstoreContentsForSeqId(s, currentSeqId);
          }
        } else {
          totalFreedSize += doDropStoreMemstoreContentsForSeqId(store, currentSeqId);
        }
      } else {
        LOG.info(getRegionInfo().getEncodedName() + " : "
            + "Not dropping memstore contents since replayed flush seqId: "
            + seqId + " is smaller than current seqId:" + currentSeqId);
      }
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    return totalFreedSize;
  }

  private long doDropStoreMemstoreContentsForSeqId(Store s, long currentSeqId) throws IOException {
    long snapshotSize = s.getFlushableSize();
    this.addAndGetGlobalMemstoreSize(-snapshotSize);
    StoreFlushContext ctx = s.createFlushContext(currentSeqId);
    ctx.prepare();
    ctx.abort();
    return snapshotSize;
  }

  private void replayWALFlushAbortMarker(FlushDescriptor flush) {
    // nothing to do for now. A flush abort will cause a RS abort which means that the region
    // will be opened somewhere else later. We will see the region open event soon, and replaying
    // that will drop the snapshot
  }

  private void replayWALFlushCannotFlushMarker(FlushDescriptor flush, long replaySeqId) {
    synchronized (writestate) {
      if (this.lastReplayedOpenRegionSeqId > replaySeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Skipping replaying flush event :" + TextFormat.shortDebugString(flush)
          + " because its sequence id " + replaySeqId + " is smaller than this regions "
          + "lastReplayedOpenRegionSeqId of " + lastReplayedOpenRegionSeqId);
        return;
      }

      // If we were waiting for observing a flush or region opening event for not showing partial
      // data after a secondary region crash, we can allow reads now. This event means that the
      // primary was not able to flush because memstore is empty when we requested flush. By the
      // time we observe this, we are guaranteed to have up to date seqId with our previous
      // assignment.
      this.setReadsEnabled(true);
    }
  }

  @VisibleForTesting
  PrepareFlushResult getPrepareFlushResult() {
    return prepareFlushResult;
  }

  void replayWALRegionEventMarker(RegionEventDescriptor regionEvent) throws IOException {
    checkTargetRegion(regionEvent.getEncodedRegionName().toByteArray(),
      "RegionEvent marker from WAL ", regionEvent);

    startRegionOperation(Operation.REPLAY_EVENT);
    try {
      if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
        return; // if primary nothing to do
      }

      if (regionEvent.getEventType() == EventType.REGION_CLOSE) {
        // nothing to do on REGION_CLOSE for now.
        return;
      }
      if (regionEvent.getEventType() != EventType.REGION_OPEN) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Unknown region event received, ignoring :"
            + TextFormat.shortDebugString(regionEvent));
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(getRegionInfo().getEncodedName() + " : "
          + "Replaying region open event marker " + TextFormat.shortDebugString(regionEvent));
      }

      // we will use writestate as a coarse-grain lock for all the replay events
      synchronized (writestate) {
        // Replication can deliver events out of order when primary region moves or the region
        // server crashes, since there is no coordination between replication of different wal files
        // belonging to different region servers. We have to safe guard against this case by using
        // region open event's seqid. Since this is the first event that the region puts (after
        // possibly flushing recovered.edits), after seeing this event, we can ignore every edit
        // smaller than this seqId
        if (this.lastReplayedOpenRegionSeqId <= regionEvent.getLogSequenceNumber()) {
          this.lastReplayedOpenRegionSeqId = regionEvent.getLogSequenceNumber();
        } else {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Skipping replaying region event :" + TextFormat.shortDebugString(regionEvent)
            + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
            + " of " + lastReplayedOpenRegionSeqId);
          return;
        }

        // region open lists all the files that the region has at the time of the opening. Just pick
        // all the files and drop prepared flushes and empty memstores
        for (StoreDescriptor storeDescriptor : regionEvent.getStoresList()) {
          // stores of primary may be different now
          byte[] family = storeDescriptor.getFamilyName().toByteArray();
          Store store = getStore(family);
          if (store == null) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a region open marker from primary, but the family is not found. "
                + "Ignoring. StoreDescriptor:" + storeDescriptor);
            continue;
          }

          long storeSeqId = store.getMaxSequenceId();
          List<String> storeFiles = storeDescriptor.getStoreFileList();
          try {
            store.refreshStoreFiles(storeFiles); // replace the files with the new ones
          } catch (FileNotFoundException ex) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                    + "At least one of the store files: " + storeFiles
                    + " doesn't exist any more. Skip loading the file(s)", ex);
            continue;
          }
          if (store.getMaxSequenceId() != storeSeqId) {
            // Record latest flush time if we picked up new files
            lastStoreFlushTimeMap.put(store, EnvironmentEdgeManager.currentTime());
          }

          if (writestate.flushing) {
            // only drop memstore snapshots if they are smaller than last flush for the store
            if (this.prepareFlushResult.flushOpSeqId <= regionEvent.getLogSequenceNumber()) {
              StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null ?
                  null : this.prepareFlushResult.storeFlushCtxs.get(family);
              if (ctx != null) {
                long snapshotSize = store.getFlushableSize();
                ctx.abort();
                this.addAndGetGlobalMemstoreSize(-snapshotSize);
                this.prepareFlushResult.storeFlushCtxs.remove(family);
              }
            }
          }

          // Drop the memstore contents if they are now smaller than the latest seen flushed file
          dropMemstoreContentsForSeqId(regionEvent.getLogSequenceNumber(), store);
          if (storeSeqId > this.maxFlushedSeqId) {
            this.maxFlushedSeqId = storeSeqId;
          }
        }

        // if all stores ended up dropping their snapshots, we can safely drop the
        // prepareFlushResult
        dropPrepareFlushIfPossible();

        // advance the mvcc read point so that the new flushed file is visible.
        mvcc.await();

        // If we were waiting for observing a flush or region opening event for not showing partial
        // data after a secondary region crash, we can allow reads now.
        this.setReadsEnabled(true);

        // C. Finally notify anyone waiting on memstore to clear:
        // e.g. checkResources().
        synchronized (this) {
          notifyAll(); // FindBugs NN_NAKED_NOTIFY
        }
      }
      logRegionFiles();
    } finally {
      closeRegionOperation(Operation.REPLAY_EVENT);
    }
  }

  void replayWALBulkLoadEventMarker(WALProtos.BulkLoadDescriptor bulkLoadEvent) throws IOException {
    checkTargetRegion(bulkLoadEvent.getEncodedRegionName().toByteArray(),
      "BulkLoad marker from WAL ", bulkLoadEvent);

    if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : "
              +  "Replaying bulkload event marker " + TextFormat.shortDebugString(bulkLoadEvent));
    }
    // check if multiple families involved
    boolean multipleFamilies = false;
    byte[] family = null;
    for (StoreDescriptor storeDescriptor : bulkLoadEvent.getStoresList()) {
      byte[] fam = storeDescriptor.getFamilyName().toByteArray();
      if (family == null) {
        family = fam;
      } else if (!Bytes.equals(family, fam)) {
        multipleFamilies = true;
        break;
      }
    }

    startBulkRegionOperation(multipleFamilies);
    try {
      // we will use writestate as a coarse-grain lock for all the replay events
      synchronized (writestate) {
        // Replication can deliver events out of order when primary region moves or the region
        // server crashes, since there is no coordination between replication of different wal files
        // belonging to different region servers. We have to safe guard against this case by using
        // region open event's seqid. Since this is the first event that the region puts (after
        // possibly flushing recovered.edits), after seeing this event, we can ignore every edit
        // smaller than this seqId
        if (bulkLoadEvent.getBulkloadSeqNum() >= 0
            && this.lastReplayedOpenRegionSeqId >= bulkLoadEvent.getBulkloadSeqNum()) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Skipping replaying bulkload event :"
              + TextFormat.shortDebugString(bulkLoadEvent)
              + " because its sequence id is smaller than this region's lastReplayedOpenRegionSeqId"
              + " =" + lastReplayedOpenRegionSeqId);

          return;
        }

        for (StoreDescriptor storeDescriptor : bulkLoadEvent.getStoresList()) {
          // stores of primary may be different now
          family = storeDescriptor.getFamilyName().toByteArray();
          Store store = getStore(family);
          if (store == null) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                    + "Received a bulk load marker from primary, but the family is not found. "
                    + "Ignoring. StoreDescriptor:" + storeDescriptor);
            continue;
          }

          List<String> storeFiles = storeDescriptor.getStoreFileList();
          for (String storeFile : storeFiles) {
            StoreFileInfo storeFileInfo = null;
            try {
              storeFileInfo = fs.getStoreFileInfo(Bytes.toString(family), storeFile);
              store.bulkLoadHFile(storeFileInfo);
            } catch(FileNotFoundException ex) {
              LOG.warn(getRegionInfo().getEncodedName() + " : "
                      + ((storeFileInfo != null) ? storeFileInfo.toString() :
                            (new Path(Bytes.toString(family), storeFile)).toString())
                      + " doesn't exist any more. Skip loading the file");
            }
          }
        }
      }
      if (bulkLoadEvent.getBulkloadSeqNum() > 0) {
        mvcc.advanceTo(bulkLoadEvent.getBulkloadSeqNum());
      }
    } finally {
      closeBulkRegionOperation();
    }
  }

  /**
   * If all stores ended up dropping their snapshots, we can safely drop the prepareFlushResult
   */
  private void dropPrepareFlushIfPossible() {
    if (writestate.flushing) {
      boolean canDrop = true;
      if (prepareFlushResult.storeFlushCtxs != null) {
        for (Entry<byte[], StoreFlushContext> entry
            : prepareFlushResult.storeFlushCtxs.entrySet()) {
          Store store = getStore(entry.getKey());
          if (store == null) {
            continue;
          }
          if (store.getSnapshotSize() > 0) {
            canDrop = false;
            break;
          }
        }
      }

      // this means that all the stores in the region has finished flushing, but the WAL marker
      // may not have been written or we did not receive it yet.
      if (canDrop) {
        writestate.flushing = false;
        this.prepareFlushResult = null;
      }
    }
  }

  @Override
  public boolean refreshStoreFiles() throws IOException {
    return refreshStoreFiles(false);
  }

  protected boolean refreshStoreFiles(boolean force) throws IOException {
    if (!force && ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return false; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : "
          + "Refreshing store files to see whether we can free up memstore");
    }

    long totalFreedSize = 0;

    long smallestSeqIdInStores = Long.MAX_VALUE;

    startRegionOperation(); // obtain region close lock
    try {
      synchronized (writestate) {
        for (Store store : getStores()) {
          // TODO: some stores might see new data from flush, while others do not which
          // MIGHT break atomic edits across column families.
          long maxSeqIdBefore = store.getMaxSequenceId();

          // refresh the store files. This is similar to observing a region open wal marker.
          store.refreshStoreFiles();

          long storeSeqId = store.getMaxSequenceId();
          if (storeSeqId < smallestSeqIdInStores) {
            smallestSeqIdInStores = storeSeqId;
          }

          // see whether we can drop the memstore or the snapshot
          if (storeSeqId > maxSeqIdBefore) {

            if (writestate.flushing) {
              // only drop memstore snapshots if they are smaller than last flush for the store
              if (this.prepareFlushResult.flushOpSeqId <= storeSeqId) {
                StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null ?
                    null : this.prepareFlushResult.storeFlushCtxs.get(store.getFamily().getName());
                if (ctx != null) {
                  long snapshotSize = store.getFlushableSize();
                  ctx.abort();
                  this.addAndGetGlobalMemstoreSize(-snapshotSize);
                  this.prepareFlushResult.storeFlushCtxs.remove(store.getFamily().getName());
                  totalFreedSize += snapshotSize;
                }
              }
            }

            // Drop the memstore contents if they are now smaller than the latest seen flushed file
            totalFreedSize += dropMemstoreContentsForSeqId(storeSeqId, store);
          }
        }

        // if all stores ended up dropping their snapshots, we can safely drop the
        // prepareFlushResult
        dropPrepareFlushIfPossible();

        // advance the mvcc read point so that the new flushed files are visible.
          // either greater than flush seq number or they were already picked up via flush.
          for (Store s : getStores()) {
            mvcc.advanceTo(s.getMaxMemstoreTS());
          }


        // smallestSeqIdInStores is the seqId that we have a corresponding hfile for. We can safely
        // skip all edits that are to be replayed in the future with that has a smaller seqId
        // than this. We are updating lastReplayedOpenRegionSeqId so that we can skip all edits
        // that we have picked the flush files for
        if (this.lastReplayedOpenRegionSeqId < smallestSeqIdInStores) {
          this.lastReplayedOpenRegionSeqId = smallestSeqIdInStores;
        }
      }
      // C. Finally notify anyone waiting on memstore to clear:
      // e.g. checkResources().
      synchronized (this) {
        notifyAll(); // FindBugs NN_NAKED_NOTIFY
      }
      return totalFreedSize > 0;
    } finally {
      closeRegionOperation();
    }
  }

  private void logRegionFiles() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(getRegionInfo().getEncodedName() + " : Store files for region: ");
      for (Store s : stores.values()) {
        for (StoreFile sf : s.getStorefiles()) {
          LOG.trace(getRegionInfo().getEncodedName() + " : " + sf);
        }
      }
    }
  }

  /** Checks whether the given regionName is either equal to our region, or that
   * the regionName is the primary region to our corresponding range for the secondary replica.
   */
  private void checkTargetRegion(byte[] encodedRegionName, String exceptionMsg, Object payload)
      throws WrongRegionException {
    if (Bytes.equals(this.getRegionInfo().getEncodedNameAsBytes(), encodedRegionName)) {
      return;
    }

    if (!RegionReplicaUtil.isDefaultReplica(this.getRegionInfo()) &&
        Bytes.equals(encodedRegionName,
          this.fs.getRegionInfoForFS().getEncodedNameAsBytes())) {
      return;
    }

    throw new WrongRegionException(exceptionMsg + payload
      + " targetted for region " + Bytes.toStringBinary(encodedRegionName)
      + " does not match this region: " + this.getRegionInfo());
  }

  /**
   * Used by tests
   * @param s Store to add edit too.
   * @param cell Cell to add.
   * @return True if we should flush.
   */
  protected boolean restoreEdit(final Store s, final Cell cell) {
    long kvSize = s.add(cell);
    if (this.rsAccounting != null) {
      rsAccounting.addAndGetRegionReplayEditsSize(getRegionInfo().getRegionName(), kvSize);
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

  @Override
  public Store getStore(final byte[] column) {
    return this.stores.get(column);
  }

  /**
   * Return HStore instance. Does not do any copy: as the number of store is limited, we
   *  iterate on the list.
   */
  private Store getStore(Cell cell) {
    for (Map.Entry<byte[], Store> famStore : stores.entrySet()) {
      if (Bytes.equals(
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          famStore.getKey(), 0, famStore.getKey().length)) {
        return famStore.getValue();
      }
    }

    return null;
  }

  @Override
  public List<Store> getStores() {
    List<Store> list = new ArrayList<Store>(stores.size());
    list.addAll(stores.values());
    return list;
  }

  @Override
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

        logRegionFiles();
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
          Bytes.toStringBinary(getRegionInfo().getStartKey()) + "', getEndKey()='" +
          Bytes.toStringBinary(getRegionInfo().getEndKey()) + "', row='" +
          Bytes.toStringBinary(row) + "'");
    }
  }


  /**
   * Get an exclusive ( write lock ) lock on a given row.
   * @param row Which row to lock.
   * @return A locked RowLock. The lock is exclusive and already aqquired.
   * @throws IOException
   */
  public RowLock getRowLock(byte[] row) throws IOException {
    return getRowLock(row, false);
  }

  /**
   *
   * Get a row lock for the specified row. All locks are reentrant.
   *
   * Before calling this function make sure that a region operation has already been
   * started (the calling thread has already acquired the region-close-guard lock).
   * @param row The row actions will be performed against
   * @param readLock is the lock reader or writer. True indicates that a non-exlcusive
   *                 lock is requested
   */
  @Override
  public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
    // Make sure the row is inside of this region before getting the lock for it.
    checkRow(row, "row lock");
    // create an object to use a a key in the row lock map
    HashedBytes rowKey = new HashedBytes(row);

    RowLockContext rowLockContext = null;
    RowLockImpl result = null;
    TraceScope traceScope = null;

    // If we're tracing start a span to show how long this took.
    if (Trace.isTracing()) {
      traceScope = Trace.startSpan("HRegion.getRowLock");
      traceScope.getSpan().addTimelineAnnotation("Getting a " + (readLock?"readLock":"writeLock"));
    }

    try {
      // Keep trying until we have a lock or error out.
      // TODO: do we need to add a time component here?
      while (result == null) {

        // Try adding a RowLockContext to the lockedRows.
        // If we can add it then there's no other transactions currently running.
        rowLockContext = new RowLockContext(rowKey);
        RowLockContext existingContext = lockedRows.putIfAbsent(rowKey, rowLockContext);

        // if there was a running transaction then there's already a context.
        if (existingContext != null) {
          rowLockContext = existingContext;
        }

        // Now try an get the lock.
        //
        // This can fail as
        if (readLock) {
          result = rowLockContext.newReadLock();
        } else {
          result = rowLockContext.newWriteLock();
        }
      }
      if (!result.getLock().tryLock(this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) {
        if (traceScope != null) {
          traceScope.getSpan().addTimelineAnnotation("Failed to get row lock");
        }
        result = null;
        // Clean up the counts just in case this was the thing keeping the context alive.
        rowLockContext.cleanUp();
        throw new IOException("Timed out waiting for lock for row: " + rowKey);
      }
      return result;
    } catch (InterruptedException ie) {
      LOG.warn("Thread interrupted waiting for lock on row: " + rowKey);
      InterruptedIOException iie = new InterruptedIOException();
      iie.initCause(ie);
      if (traceScope != null) {
        traceScope.getSpan().addTimelineAnnotation("Interrupted exception getting row lock");
      }
      Thread.currentThread().interrupt();
      throw iie;
    } finally {
      if (traceScope != null) {
        traceScope.close();
      }
    }
  }

  @Override
  public void releaseRowLocks(List<RowLock> rowLocks) {
    if (rowLocks != null) {
      for (RowLock rowLock : rowLocks) {
        rowLock.release();
      }
      rowLocks.clear();
    }
  }

  @VisibleForTesting
  class RowLockContext {
    private final HashedBytes row;
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    final AtomicBoolean usable = new AtomicBoolean(true);
    final AtomicInteger count = new AtomicInteger(0);
    final Object lock = new Object();

    RowLockContext(HashedBytes row) {
      this.row = row;
    }

    RowLockImpl newWriteLock() {
      Lock l = readWriteLock.writeLock();
      return getRowLock(l);
    }
    RowLockImpl newReadLock() {
      Lock l = readWriteLock.readLock();
      return getRowLock(l);
    }

    private RowLockImpl getRowLock(Lock l) {
      count.incrementAndGet();
      synchronized (lock) {
        if (usable.get()) {
          return new RowLockImpl(this, l);
        } else {
          return null;
        }
      }
    }

    void cleanUp() {
      long c = count.decrementAndGet();
      if (c <= 0) {
        synchronized (lock) {
          if (count.get() <= 0 ){
            usable.set(false);
            RowLockContext removed = lockedRows.remove(row);
            assert removed == this: "we should never remove a different context";
          }
        }
      }
    }

    @Override
    public String toString() {
      return "RowLockContext{" +
          "row=" + row +
          ", readWriteLock=" + readWriteLock +
          ", count=" + count +
          '}';
    }
  }

  /**
   * Class used to represent a lock on a row.
   */
  public static class RowLockImpl implements RowLock {
    private final RowLockContext context;
    private final Lock lock;

    public RowLockImpl(RowLockContext context, Lock lock) {
      this.context = context;
      this.lock = lock;
    }

    public Lock getLock() {
      return lock;
    }

    @VisibleForTesting
    public RowLockContext getContext() {
      return context;
    }

    @Override
    public void release() {
      lock.unlock();
      context.cleanUp();
    }

    @Override
    public String toString() {
      return "RowLockImpl{" +
          "context=" + context +
          ", lock=" + lock +
          '}';
    }
  }

  /**
   * Determines whether multiple column families are present
   * Precondition: familyPaths is not null
   *
   * @param familyPaths List of Pair<byte[] column family, String hfilePath>
   */
  private static boolean hasMultipleColumnFamilies(Collection<Pair<byte[], String>> familyPaths) {
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

  @Override
  public boolean bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths, boolean assignSeqId,
      BulkLoadListener bulkLoadListener) throws IOException {
    long seqId = -1;
    Map<byte[], List<Path>> storeFiles = new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
    Preconditions.checkNotNull(familyPaths);
    // we need writeLock for multi-family bulk load
    startBulkRegionOperation(hasMultipleColumnFamilies(familyPaths));
    try {
      this.writeRequestsCount.increment();

      // There possibly was a split that happened between when the split keys
      // were gathered and before the HRegion's write lock was taken.  We need
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

      // We need to assign a sequential ID that's in between two memstores in order to preserve
      // the guarantee that all the edits lower than the highest sequential ID from all the
      // HFiles are flushed on disk. See HBASE-10958.  The sequence id returned when we flush is
      // guaranteed to be one beyond the file made when we flushed (or if nothing to flush, it is
      // a sequence id that we can be sure is beyond the last hfile written).
      if (assignSeqId) {
        FlushResult fs = flushcache(true, false);
        if (fs.isFlushSucceeded()) {
          seqId = ((FlushResultImpl)fs).flushSequenceId;
        } else if (fs.getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
          seqId = ((FlushResultImpl)fs).flushSequenceId;
        } else {
          throw new IOException("Could not bulk load with an assigned sequential ID because the "+
            "flush didn't run. Reason for not flushing: " + ((FlushResultImpl)fs).failureReason);
        }
      }

      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();
        Store store = getStore(familyName);
        try {
          String finalPath = path;
          if (bulkLoadListener != null) {
            finalPath = bulkLoadListener.prepareBulkLoad(familyName, path);
          }
          Path commitedStoreFile = store.bulkLoadHFile(finalPath, seqId);

          if(storeFiles.containsKey(familyName)) {
            storeFiles.get(familyName).add(commitedStoreFile);
          } else {
            List<Path> storeFileNames = new ArrayList<Path>();
            storeFileNames.add(commitedStoreFile);
            storeFiles.put(familyName, storeFileNames);
          }
          if (bulkLoadListener != null) {
            bulkLoadListener.doneBulkLoad(familyName, path);
          }
        } catch (IOException ioe) {
          // A failure here can cause an atomicity violation that we currently
          // cannot recover from since it is likely a failed HDFS operation.

          // TODO Need a better story for reverting partial failures due to HDFS.
          LOG.error("There was a partial failure due to IO when attempting to" +
              " load " + Bytes.toString(p.getFirst()) + " : " + p.getSecond(), ioe);
          if (bulkLoadListener != null) {
            try {
              bulkLoadListener.failedBulkLoad(familyName, path);
            } catch (Exception ex) {
              LOG.error("Error while calling failedBulkLoad for family " +
                  Bytes.toString(familyName) + " with path " + path, ex);
            }
          }
          throw ioe;
        }
      }

      return true;
    } finally {
      if (wal != null && !storeFiles.isEmpty()) {
        // write a bulk load event when not all hfiles are loaded
        try {
          WALProtos.BulkLoadDescriptor loadDescriptor = ProtobufUtil.toBulkLoadDescriptor(
              this.getRegionInfo().getTable(),
              ByteStringer.wrap(this.getRegionInfo().getEncodedNameAsBytes()), storeFiles, seqId);
          WALUtil.writeBulkLoadMarkerAndSync(wal, this.htableDescriptor, getRegionInfo(),
              loadDescriptor, mvcc);
        } catch (IOException ioe) {
          if (this.rsServices != null) {
            // Have to abort region server because some hfiles has been loaded but we can't write
            // the event into WAL
            this.rsServices.abort("Failed to write bulk load event into WAL.", ioe);
          }
        }
      }

      closeBulkRegionOperation();
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof HRegion && Bytes.equals(getRegionInfo().getRegionName(),
                                                ((HRegion) o).getRegionInfo().getRegionName());
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(getRegionInfo().getRegionName());
  }

  @Override
  public String toString() {
    return getRegionInfo().getRegionNameAsString();
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
    protected Cell joinedContinuationRow = null;
    private boolean filterClosed = false;

    protected final int isScan;
    protected final byte[] stopRow;
    protected final HRegion region;

    private final long readPt;
    private final long maxResultSize;
    private final ScannerContext defaultScannerContext;
    private final FilterWrapper filter;

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

      /**
       * By default, calls to next/nextRaw must enforce the batch limit. Thus, construct a default
       * scanner context that can be used to enforce the batch limit in the event that a
       * ScannerContext is not specified during an invocation of next/nextRaw
       */
      defaultScannerContext = ScannerContext.newBuilder().setBatchLimit(scan.getBatch()).build();

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

      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
        Store store = stores.get(entry.getKey());
        KeyValueScanner scanner;
        try {
          scanner = store.getScanner(scan, entry.getValue(), this.readPt);
        } catch (FileNotFoundException e) {
          throw handleFileNotFound(e);
        }
        if (this.filter == null || !scan.doLoadColumnFamiliesOnDemand()
          || this.filter.isFamilyEssential(entry.getKey())) {
          scanners.add(scanner);
        } else {
          joinedScanners.add(scanner);
        }
      }
      initializeKVHeap(scanners, joinedScanners, region);
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

    @Override
    public int getBatch() {
      return this.defaultScannerContext.getBatchLimit();
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
      return next(outResults, defaultScannerContext);
    }

    @Override
    public synchronized boolean next(List<Cell> outResults, ScannerContext scannerContext)
    throws IOException {
      if (this.filterClosed) {
        throw new UnknownScannerException("Scanner was closed (timed out?) " +
            "after we renewed it. Could be caused by a very slow scanner " +
            "or a lengthy garbage collection");
      }
      startRegionOperation(Operation.SCAN);
      readRequestsCount.increment();
      try {
        return nextRaw(outResults, scannerContext);
      } finally {
        closeRegionOperation(Operation.SCAN);
      }
    }

    @Override
    public boolean nextRaw(List<Cell> outResults) throws IOException {
      // Use the RegionScanner's context by default
      return nextRaw(outResults, defaultScannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> outResults, ScannerContext scannerContext)
        throws IOException {
      if (storeHeap == null) {
        // scanner is closed
        throw new UnknownScannerException("Scanner was closed");
      }
      boolean moreValues;
      if (outResults.isEmpty()) {
        // Usually outResults is empty. This is true when next is called
        // to handle scan or get operation.
        moreValues = nextInternal(outResults, scannerContext);
      } else {
        List<Cell> tmpList = new ArrayList<Cell>();
        moreValues = nextInternal(tmpList, scannerContext);
        outResults.addAll(tmpList);
      }

      // If the size limit was reached it means a partial Result is being returned. Returning a
      // partial Result means that we should not reset the filters; filters should only be reset in
      // between rows
      if (!scannerContext.partialResultFormed()) resetFilters();

      if (isFilterDoneInternal()) {
        moreValues = false;
      }
      return moreValues;
    }

    /**
     * @return true if more cells exist after this batch, false if scanner is done
     */
    private boolean populateFromJoinedHeap(List<Cell> results, ScannerContext scannerContext)
            throws IOException {
      assert joinedContinuationRow != null;
      boolean moreValues =
          populateResult(results, this.joinedHeap, scannerContext,
          joinedContinuationRow.getRowArray(), joinedContinuationRow.getRowOffset(),
          joinedContinuationRow.getRowLength());

      if (!scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
        // We are done with this row, reset the continuation.
        joinedContinuationRow = null;
      }
      // As the data is obtained from two independent heaps, we need to
      // ensure that result list is sorted, because Result relies on that.
      Collections.sort(results, comparator);
      return moreValues;
    }

    /**
     * Fetches records with currentRow into results list, until next row, batchLimit (if not -1) is
     * reached, or remainingResultSize (if not -1) is reaced
     * @param heap KeyValueHeap to fetch data from.It must be positioned on correct row before call.
     * @param scannerContext
     * @param currentRow Byte array with key we are fetching.
     * @param offset offset for currentRow
     * @param length length for currentRow
     * @return state of last call to {@link KeyValueHeap#next()}
     */
    private boolean populateResult(List<Cell> results, KeyValueHeap heap,
        ScannerContext scannerContext, byte[] currentRow, int offset, short length)
        throws IOException {
      Cell nextKv;
      boolean moreCellsInRow = false;
      boolean tmpKeepProgress = scannerContext.getKeepProgress();
      // Scanning between column families and thus the scope is between cells
      LimitScope limitScope = LimitScope.BETWEEN_CELLS;
      try {
        do {
          // We want to maintain any progress that is made towards the limits while scanning across
          // different column families. To do this, we toggle the keep progress flag on during calls
          // to the StoreScanner to ensure that any progress made thus far is not wiped away.
          scannerContext.setKeepProgress(true);
          heap.next(results, scannerContext);
          scannerContext.setKeepProgress(tmpKeepProgress);

          nextKv = heap.peek();
          moreCellsInRow = moreCellsInRow(nextKv, currentRow, offset, length);
          if (!moreCellsInRow) incrementCountOfRowsScannedMetric(scannerContext);

          if (scannerContext.checkBatchLimit(limitScope)) {
            return scannerContext.setScannerState(NextState.BATCH_LIMIT_REACHED).hasMoreValues();
          } else if (scannerContext.checkSizeLimit(limitScope)) {
            ScannerContext.NextState state =
                moreCellsInRow? NextState.SIZE_LIMIT_REACHED_MID_ROW: NextState.SIZE_LIMIT_REACHED;
            return scannerContext.setScannerState(state).hasMoreValues();
          } else if (scannerContext.checkTimeLimit(limitScope)) {
            ScannerContext.NextState state =
                moreCellsInRow? NextState.TIME_LIMIT_REACHED_MID_ROW: NextState.TIME_LIMIT_REACHED;
            return scannerContext.setScannerState(state).hasMoreValues();
          }
        } while (moreCellsInRow);
      } catch (FileNotFoundException e) {
        throw handleFileNotFound(e);
      }
      return nextKv != null;
    }

    /**
     * Based on the nextKv in the heap, and the current row, decide whether or not there are more
     * cells to be read in the heap. If the row of the nextKv in the heap matches the current row
     * then there are more cells to be read in the row.
     * @param nextKv
     * @param currentRow
     * @param offset
     * @param length
     * @return true When there are more cells in the row to be read
     */
    private boolean moreCellsInRow(final Cell nextKv, byte[] currentRow, int offset,
        short length) {
      return nextKv != null && CellUtil.matchingRow(nextKv, currentRow, offset, length);
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

    private boolean nextInternal(List<Cell> results, ScannerContext scannerContext)
        throws IOException {
      if (!results.isEmpty()) {
        throw new IllegalArgumentException("First parameter should be an empty list");
      }
      if (scannerContext == null) {
        throw new IllegalArgumentException("Scanner context cannot be null");
      }
      RpcCallContext rpcCall = RpcServer.getCurrentCall();

      // Save the initial progress from the Scanner context in these local variables. The progress
      // may need to be reset a few times if rows are being filtered out so we save the initial
      // progress.
      int initialBatchProgress = scannerContext.getBatchProgress();
      long initialSizeProgress = scannerContext.getSizeProgress();
      long initialTimeProgress = scannerContext.getTimeProgress();

      // The loop here is used only when at some point during the next we determine
      // that due to effects of filters or otherwise, we have an empty row in the result.
      // Then we loop and try again. Otherwise, we must get out on the first iteration via return,
      // "true" if there's more data to read, "false" if there isn't (storeHeap is at a stop row,
      // and joinedHeap has no more data to read for the last row (if set, joinedContinuationRow).
      while (true) {
        // Starting to scan a new row. Reset the scanner progress according to whether or not
        // progress should be kept.
        if (scannerContext.getKeepProgress()) {
          // Progress should be kept. Reset to initial values seen at start of method invocation.
          scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
            initialTimeProgress);
        } else {
          scannerContext.clearProgress();
        }

        if (rpcCall != null) {
          // If a user specifies a too-restrictive or too-slow scanner, the
          // client might time out and disconnect while the server side
          // is still processing the request. We should abort aggressively
          // in that case.
          long afterTime = rpcCall.disconnectSince();
          if (afterTime >= 0) {
            throw new CallerDisconnectedException(
                "Aborting on region " + getRegionInfo().getRegionNameAsString() + ", call " +
                    this + " after " + afterTime + " ms, since " +
                    "caller disconnected");
          }
        }

        // Let's see what we have in the storeHeap.
        Cell current = this.storeHeap.peek();

        byte[] currentRow = null;
        int offset = 0;
        short length = 0;
        if (current != null) {
          currentRow = current.getRowArray();
          offset = current.getRowOffset();
          length = current.getRowLength();
        }

        boolean stopRow = isStopRow(currentRow, offset, length);
        // When has filter row is true it means that the all the cells for a particular row must be
        // read before a filtering decision can be made. This means that filters where hasFilterRow
        // run the risk of encountering out of memory errors in the case that they are applied to a
        // table that has very large rows.
        boolean hasFilterRow = this.filter != null && this.filter.hasFilterRow();

        // If filter#hasFilterRow is true, partial results are not allowed since allowing them
        // would prevent the filters from being evaluated. Thus, if it is true, change the
        // scope of any limits that could potentially create partial results to
        // LimitScope.BETWEEN_ROWS so that those limits are not reached mid-row
        if (hasFilterRow) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("filter#hasFilterRow is true which prevents partial results from being "
                + " formed. Changing scope of limits that may create partials");
          }
          scannerContext.setSizeLimitScope(LimitScope.BETWEEN_ROWS);
          scannerContext.setTimeLimitScope(LimitScope.BETWEEN_ROWS);
        }

        // Check if we were getting data from the joinedHeap and hit the limit.
        // If not, then it's main path - getting results from storeHeap.
        if (joinedContinuationRow == null) {
          // First, check if we are at a stop row. If so, there are no more results.
          if (stopRow) {
            if (hasFilterRow) {
              filter.filterRowCells(results);
            }
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }

          // Check if rowkey filter wants to exclude this row. If so, loop to next.
          // Technically, if we hit limits before on this row, we don't need this call.
          if (filterRowKey(currentRow, offset, length)) {
            incrementCountOfRowsFilteredMetric(scannerContext);
            // Typically the count of rows scanned is incremented inside #populateResult. However,
            // here we are filtering a row based purely on its row key, preventing us from calling
            // #populateResult. Thus, perform the necessary increment here to rows scanned metric
            incrementCountOfRowsScannedMetric(scannerContext);
            boolean moreRows = nextRow(scannerContext, currentRow, offset, length);
            if (!moreRows) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            results.clear();
            continue;
          }

          // Ok, we are good, let's try to get some results from the main heap.
          populateResult(results, this.storeHeap, scannerContext, currentRow, offset, length);

          if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
            if (hasFilterRow) {
              throw new IncompatibleFilterException(
                  "Filter whose hasFilterRow() returns true is incompatible with scans that must "
                      + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
            }
            return true;
          }

          Cell nextKv = this.storeHeap.peek();
          stopRow = nextKv == null ||
              isStopRow(nextKv.getRowArray(), nextKv.getRowOffset(), nextKv.getRowLength());
          // save that the row was empty before filters applied to it.
          final boolean isEmptyRow = results.isEmpty();

          // We have the part of the row necessary for filtering (all of it, usually).
          // First filter with the filterRow(List).
          FilterWrapper.FilterRowRetCode ret = FilterWrapper.FilterRowRetCode.NOT_CALLED;
          if (hasFilterRow) {
            ret = filter.filterRowCellsWithRet(results);

            // We don't know how the results have changed after being filtered. Must set progress
            // according to contents of results now. However, a change in the results should not
            // affect the time progress. Thus preserve whatever time progress has been made
            long timeProgress = scannerContext.getTimeProgress();
            if (scannerContext.getKeepProgress()) {
              scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
                initialTimeProgress);
            } else {
              scannerContext.clearProgress();
            }
            scannerContext.setTimeProgress(timeProgress);
            scannerContext.incrementBatchProgress(results.size());
            for (Cell cell : results) {
              scannerContext.incrementSizeProgress(CellUtil.estimatedHeapSizeOfWithoutTags(cell));
            }
          }

          if (isEmptyRow || ret == FilterWrapper.FilterRowRetCode.EXCLUDE || filterRow()) {
            incrementCountOfRowsFilteredMetric(scannerContext);
            results.clear();
            boolean moreRows = nextRow(scannerContext, currentRow, offset, length);
            if (!moreRows) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }

            // This row was totally filtered out, if this is NOT the last row,
            // we should continue on. Otherwise, nothing else to do.
            if (!stopRow) continue;
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }

          // Ok, we are done with storeHeap for this row.
          // Now we may need to fetch additional, non-essential data into row.
          // These values are not needed for filter to work, so we postpone their
          // fetch to (possibly) reduce amount of data loads from disk.
          if (this.joinedHeap != null) {
            boolean mayHaveData = joinedHeapMayHaveData(currentRow, offset, length);
            if (mayHaveData) {
              joinedContinuationRow = current;
              populateFromJoinedHeap(results, scannerContext);

              if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
                return true;
              }
            }
          }
        } else {
          // Populating from the joined heap was stopped by limits, populate some more.
          populateFromJoinedHeap(results, scannerContext);
          if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
            return true;
          }
        }
        // We may have just called populateFromJoinedMap and hit the limits. If that is
        // the case, we need to call it again on the next next() invocation.
        if (joinedContinuationRow != null) {
          return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
        }

        // Finally, we are done with both joinedHeap and storeHeap.
        // Double check to prevent empty rows from appearing in result. It could be
        // the case when SingleColumnValueExcludeFilter is used.
        if (results.isEmpty()) {
          incrementCountOfRowsFilteredMetric(scannerContext);
          boolean moreRows = nextRow(scannerContext, currentRow, offset, length);
          if (!moreRows) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          if (!stopRow) continue;
        }

        if (stopRow) {
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        } else {
          return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
        }
      }
    }

    protected void incrementCountOfRowsFilteredMetric(ScannerContext scannerContext) {
      if (scannerContext == null || !scannerContext.isTrackingMetrics()) return;

      scannerContext.getMetrics().countOfRowsFiltered.incrementAndGet();
    }

    protected void incrementCountOfRowsScannedMetric(ScannerContext scannerContext) {
      if (scannerContext == null || !scannerContext.isTrackingMetrics()) return;

      scannerContext.getMetrics().countOfRowsScanned.incrementAndGet();
    }

    /**
     * @param currentRow
     * @param offset
     * @param length
     * @return true when the joined heap may have data for the current row
     * @throws IOException
     */
    private boolean joinedHeapMayHaveData(byte[] currentRow, int offset, short length)
        throws IOException {
      Cell nextJoinedKv = joinedHeap.peek();
      boolean matchCurrentRow =
          nextJoinedKv != null && CellUtil.matchingRow(nextJoinedKv, currentRow, offset, length);
      boolean matchAfterSeek = false;

      // If the next value in the joined heap does not match the current row, try to seek to the
      // correct row
      if (!matchCurrentRow) {
        Cell firstOnCurrentRow = KeyValueUtil.createFirstOnRow(currentRow, offset, length);
        boolean seekSuccessful = this.joinedHeap.requestSeek(firstOnCurrentRow, true, true);
        matchAfterSeek =
            seekSuccessful && joinedHeap.peek() != null
                && CellUtil.matchingRow(joinedHeap.peek(), currentRow, offset, length);
      }

      return matchCurrentRow || matchAfterSeek;
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

    protected boolean nextRow(ScannerContext scannerContext, byte[] currentRow, int offset,
        short length) throws IOException {
      assert this.joinedContinuationRow == null:
        "Trying to go to next row during joinedHeap read.";
      Cell next;
      while ((next = this.storeHeap.peek()) != null &&
             CellUtil.matchingRow(next, currentRow, offset, length)) {
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
      // no need to synchronize here.
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
      KeyValue kv = KeyValueUtil.createFirstOnRow(row);
      try {
        // use request seek to make use of the lazy seek option. See HBASE-5520
        result = this.storeHeap.requestSeek(kv, true, true);
        if (this.joinedHeap != null) {
          result = this.joinedHeap.requestSeek(kv, true, true) || result;
        }
      } catch (FileNotFoundException e) {
        throw handleFileNotFound(e);
      } finally {
        closeRegionOperation();
      }
      return result;
    }

    private IOException handleFileNotFound(FileNotFoundException fnfe) throws IOException {
      // tries to refresh the store files, otherwise shutdown the RS.
      // TODO: add support for abort() of a single region and trigger reassignment.
      try {
        region.refreshStoreFiles(true);
        return new IOException("unable to read store file");
      } catch (IOException e) {
        String msg = "a store file got lost: " + fnfe.getMessage();
        LOG.error(msg);
        LOG.error("unable to refresh store files", e);
        abortRegionServer(msg);
        return new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " closing");
      }
    }

    private void abortRegionServer(String msg) throws IOException {
      if (rsServices instanceof HRegionServer) {
        ((HRegionServer)rsServices).abort(msg);
      }
      throw new UnsupportedOperationException("not able to abort RS after: " + msg);
    }
  }

  // Utility methods
  /**
   * A utility method to create new instances of HRegion based on the
   * {@link HConstants#REGION_IMPL} configuration property.
   * @param tableDir qualified path of directory where region should be located,
   * usually the table directory.
   * @param wal The WAL is the outbound log for any updates to the HRegion
   * The wal file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate wal info for this HRegion. If there is a previous file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param htd the table descriptor
   * @return the new instance
   */
  static HRegion newHRegion(Path tableDir, WAL wal, FileSystem fs,
      Configuration conf, HRegionInfo regionInfo, final HTableDescriptor htd,
      RegionServerServices rsServices) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
          (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, HRegion.class);

      Constructor<? extends HRegion> c =
          regionClass.getConstructor(Path.class, WAL.class, FileSystem.class,
              Configuration.class, HRegionInfo.class, HTableDescriptor.class,
              RegionServerServices.class);

      return c.newInstance(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
    } catch (Throwable e) {
      // todo: what should I throw here?
      throw new IllegalStateException("Could not instantiate a region instance.", e);
    }
  }

  /**
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor.
   * Note, this method creates an {@link WAL} for the created region. It
   * needs to be closed explicitly.  Use {@link HRegion#getWAL()} to get
   * access.  <b>When done with a region created using this method, you will
   * need to explicitly close the {@link WAL} it created too; it will not be
   * done for you.  Not closing the wal will leave at least a daemon thread
   * running.</b>  Call {@link #closeHRegion(HRegion)} and it will do
   * necessary cleanup for you.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
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
   * associated {@link WAL} file.  You can still use it if you call the other createHRegion,
   * the one that takes an {@link WAL} instance but don't be surprised by the
   * call to the {@link WAL#close()} on the {@link WAL} the
   * HRegion was carrying.
   * @throws IOException
   */
  public static void closeHRegion(final HRegion r) throws IOException {
    if (r == null) return;
    r.close();
    if (r.getWAL() == null) return;
    r.getWAL().close();
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link WAL} for the created region needs to be closed explicitly.
   * Use {@link HRegion#getWAL()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final WAL wal,
                                      final boolean initialize)
      throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor,
        wal, initialize, false);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link WAL} for the created region needs to be closed
   * explicitly, if it is not null.
   * Use {@link HRegion#getWAL()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @param ignoreWAL - true to skip generate new wal if it is null, mostly for createTable
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final WAL wal,
                                      final boolean initialize, final boolean ignoreWAL)
      throws IOException {
      Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
      return createHRegion(info, rootDir, tableDir, conf, hTableDescriptor, wal, initialize,
          ignoreWAL);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * The {@link WAL} for the created region needs to be closed
   * explicitly, if it is not null.
   * Use {@link HRegion#getWAL()} to get access.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param tableDir table directory
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @param ignoreWAL - true to skip generate new wal if it is null, mostly for createTable
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
      final Path tableDir, final Configuration conf, final HTableDescriptor hTableDescriptor,
      final WAL wal, final boolean initialize, final boolean ignoreWAL)
  throws IOException {
    LOG.info("creating HRegion " + info.getTable().getNameAsString()
        + " HTD == " + hTableDescriptor + " RootDir = " + rootDir +
        " Table name == " + info.getTable().getNameAsString());
    FileSystem fs = FileSystem.get(conf);
    HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, info);
    WAL effectiveWAL = wal;
    if (wal == null && !ignoreWAL) {
      // TODO HBASE-11983 There'll be no roller for this wal?
      // The WAL subsystem will use the default rootDir rather than the passed in rootDir
      // unless I pass along via the conf.
      Configuration confForWAL = new Configuration(conf);
      confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
      effectiveWAL = (new WALFactory(confForWAL,
          Collections.<WALActionsListener>singletonList(new MetricsWAL()),
          "hregion-" + RandomStringUtils.randomNumeric(8))).
            getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());
    }
    HRegion region = HRegion.newHRegion(tableDir,
        effectiveWAL, fs, conf, info, hTableDescriptor, null);
    if (initialize) region.initialize(null);
    return region;
  }

  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final HTableDescriptor hTableDescriptor,
                                      final WAL wal)
    throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, true);
  }


  /**
   * Open a Region.
   * @param info Info for region to be opened.
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info,
      final HTableDescriptor htd, final WAL wal,
      final Configuration conf)
  throws IOException {
    return openHRegion(info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param info Info for region to be opened
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info,
    final HTableDescriptor htd, final WAL wal, final Configuration conf,
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
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(Path rootDir, final HRegionInfo info,
      final HTableDescriptor htd, final WAL wal, final Configuration conf)
  throws IOException {
    return openHRegion(rootDir, info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param rootDir Root directory for HBase instance
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Path rootDir, final HRegionInfo info,
      final HTableDescriptor htd, final WAL wal, final Configuration conf,
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
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final HRegionInfo info, final HTableDescriptor htd, final WAL wal)
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
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final HRegionInfo info, final HTableDescriptor htd, final WAL wal,
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
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final Path tableDir, final HRegionInfo info, final HTableDescriptor htd,
      final WAL wal, final RegionServerServices rsServices,
      final CancelableProgressable reporter)
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
    HRegion r = newHRegion(regionFs.getTableDir(), other.getWAL(), regionFs.getFileSystem(),
        other.baseConf, other.getRegionInfo(), other.getTableDesc(), null);
    return r.openHRegion(reporter);
  }

  public static Region openHRegion(final Region other, final CancelableProgressable reporter)
        throws IOException {
    return openHRegion((HRegion)other, reporter);
  }

  /**
   * Open HRegion.
   * Calls initialize and sets sequenceId.
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
    this.mvcc.advanceTo(openSeqNum);
    if (wal != null && getRegionServerServices() != null && !writestate.readOnly
        && !recovering) {
      // Only write the region open event marker to WAL if (1) we are not read-only
      // (2) dist log replay is off or we are not recovering. In case region is
      // recovering, the open event will be written at setRecovering(false)
      writeRegionOpenMarker(wal, openSeqNum);
    }
    return this;
  }

  public static void warmupHRegion(final HRegionInfo info,
      final HTableDescriptor htd, final WAL wal, final Configuration conf,
      final RegionServerServices rsServices,
      final CancelableProgressable reporter)
      throws IOException {

    if (info == null) throw new NullPointerException("Passed region info is null");

    if (LOG.isDebugEnabled()) {
      LOG.debug("HRegion.Warming up region: " + info);
    }

    Path rootDir = FSUtils.getRootDir(conf);
    Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());

    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = FileSystem.get(conf);
    }

    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, rsServices);
    r.initializeWarmup(reporter);
    r.close();
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
    HRegion r = HRegion.newHRegion(this.fs.getTableDir(), this.getWAL(), fs.getFileSystem(),
        this.getBaseConf(), hri, this.getTableDesc(), rsServices);
    r.readRequestsCount.set(this.getReadRequestsCount() / 2);
    r.writeRequestsCount.set(this.getWriteRequestsCount() / 2);
    return r;
  }

  /**
   * Create a merged region given a temp directory with the region data.
   * @param region_b another merging region
   * @return merged HRegion
   * @throws IOException
   */
  HRegion createMergedRegionFromMerges(final HRegionInfo mergedRegionInfo,
      final HRegion region_b) throws IOException {
    HRegion r = HRegion.newHRegion(this.fs.getTableDir(), this.getWAL(),
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
    byte[] row = r.getRegionInfo().getRegionName();
    final long now = EnvironmentEdgeManager.currentTime();
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
   * @deprecated For tests only; to be removed.
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
   * @deprecated For tests only; to be removed.
   */
  @Deprecated
  @VisibleForTesting
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
   * @return new merged HRegion
   * @throws IOException
   */
  public static HRegion mergeAdjacent(final HRegion srcA, final HRegion srcB)
  throws IOException {
    HRegion a = srcA;
    HRegion b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    if (srcA.getRegionInfo().getStartKey() == null) {
      if (srcB.getRegionInfo().getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getRegionInfo().getStartKey() == null) ||
      (Bytes.compareTo(srcA.getRegionInfo().getStartKey(),
        srcB.getRegionInfo().getStartKey()) > 0)) {
      a = srcB;
      b = srcA;
    }

    if (!(Bytes.compareTo(a.getRegionInfo().getEndKey(),
        b.getRegionInfo().getStartKey()) == 0)) {
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
    a.flush(true);
    b.flush(true);

    // Compact each region so we only have one store file per family
    a.compact(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + a);
      a.getRegionFileSystem().logFileSystemState(LOG);
    }
    b.compact(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + b);
      b.getRegionFileSystem().logFileSystemState(LOG);
    }

    RegionMergeTransactionImpl rmt = new RegionMergeTransactionImpl(a, b, true);
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
      dstRegion = (HRegion)rmt.execute(null, null);
    } catch (IOException ioe) {
      rmt.rollback(null, null);
      throw new IOException("Failed merging region " + a + " and " + b
          + ", and successfully rolled back");
    }
    dstRegion.compact(true);

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

  @Override
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
    boolean stale = this.getRegionInfo().getReplicaId() != 0;
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  @Override
  public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {

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
      long totalSize = 0L;
      for (Cell cell : results) {
        totalSize += CellUtil.estimatedSerializedSizeOf(cell);
      }
      this.metricsRegion.updateGet(totalSize);
    }

    return results;
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    // Don't need nonces here - RowMutations only supports puts and deletes
    mutateRowsWithLocks(rm.getMutations(), Collections.singleton(rm.getRow()));
  }

  /**
   * Perform atomic mutations within the region w/o nonces.
   * See {@link #mutateRowsWithLocks(Collection, Collection, long, long)}
   */
  public void mutateRowsWithLocks(Collection<Mutation> mutations,
      Collection<byte[]> rowsToLock) throws IOException {
    mutateRowsWithLocks(mutations, rowsToLock, HConstants.NO_NONCE, HConstants.NO_NONCE);
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
  @Override
  public void mutateRowsWithLocks(Collection<Mutation> mutations,
      Collection<byte[]> rowsToLock, long nonceGroup, long nonce) throws IOException {
    MultiRowMutationProcessor proc = new MultiRowMutationProcessor(mutations, rowsToLock);
    processRowsWithLocks(proc, -1, nonceGroup, nonce);
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

  @Override
  public void processRowsWithLocks(RowProcessor<?,?> processor) throws IOException {
    processRowsWithLocks(processor, rowProcessorTimeout, HConstants.NO_NONCE,
      HConstants.NO_NONCE);
  }

  @Override
  public void processRowsWithLocks(RowProcessor<?,?> processor, long nonceGroup, long nonce)
      throws IOException {
    processRowsWithLocks(processor, rowProcessorTimeout, nonceGroup, nonce);
  }

  @Override
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
        long now = EnvironmentEdgeManager.currentTime();
        doProcessRowWithTimeout(
            processor, now, this, null, null, timeout);
        processor.postProcess(this, walEdit, true);
      } finally {
        closeRegionOperation();
      }
      return;
    }

    MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
    boolean locked;
    boolean walSyncSuccessful = false;
    List<RowLock> acquiredRowLocks;
    long addedSize = 0;
    List<Mutation> mutations = new ArrayList<Mutation>();
    Collection<byte[]> rowsToLock = processor.getRowsToLock();
    long mvccNum = 0;
    WALKey walKey = null;
    try {
      // 2. Acquire the row lock(s)
      acquiredRowLocks = new ArrayList<RowLock>(rowsToLock.size());
      for (byte[] row : rowsToLock) {
        // Attempt to lock all involved rows, throw if any lock times out
        // use a writer lock for mixed reads and writes
        acquiredRowLocks.add(getRowLock(row));
      }
      // 3. Region lock
      lock(this.updatesLock.readLock(), acquiredRowLocks.size() == 0 ? 1 : acquiredRowLocks.size());
      locked = true;

      long now = EnvironmentEdgeManager.currentTime();
      try {
        // 4. Let the processor scan the rows, generate mutations and add
        //    waledits
        doProcessRowWithTimeout(
            processor, now, this, mutations, walEdit, timeout);

        if (!mutations.isEmpty()) {

          // 5. Call the preBatchMutate hook
          processor.preBatchMutate(this, walEdit);

          long txid = 0;
          // 6. Append no sync
          if (!walEdit.isEmpty()) {
            // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
            walKey = new HLogKey(this.getRegionInfo().getEncodedNameAsBytes(),
              this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
              processor.getClusterIds(), nonceGroup, nonce, mvcc);
            txid = this.wal.append(this.htableDescriptor, this.getRegionInfo(),
                walKey, walEdit, false);
          }
          if(walKey == null){
            // since we use wal sequence Id as mvcc, for SKIP_WAL changes we need a "faked" WALEdit
            // to get a sequence id assigned which is done by FSWALEntry#stampRegionSequenceId
            walKey = this.appendEmptyEdit(this.wal);
          }

          // 7. Start mvcc transaction
          writeEntry = walKey.getWriteEntry();
          mvccNum = walKey.getSequenceId();



          // 8. Apply to memstore
          for (Mutation m : mutations) {
            // Handle any tag based cell features
            rewriteCellTags(m.getFamilyCellMap(), m);

            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              CellUtil.setSequenceId(cell, mvccNum);
              Store store = getStore(cell);
              if (store == null) {
                checkFamily(CellUtil.cloneFamily(cell));
                // unreachable
              }
              addedSize += store.add(cell);
            }
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
        // TODO: Make this method look like all other methods that are doing append/sync and
        // memstore rollback such as append and doMiniBatchMutation. Currently it is a little
        // different. Make them all share same code!
        if (!mutations.isEmpty() && !walSyncSuccessful) {
          LOG.warn("Wal sync failed. Roll back " + mutations.size() +
              " memstore keyvalues for row(s):" + StringUtils.byteToHexString(
              processor.getRowsToLock().iterator().next()) + "...");
          for (Mutation m : mutations) {
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              getStore(cell).rollback(cell);
            }
          }
          if (writeEntry != null) {
            mvcc.complete(writeEntry);
            writeEntry = null;
          }
        }
        // 13. Roll mvcc forward
        if (writeEntry != null) {
          mvcc.completeAndWait(writeEntry);
        }
        if (locked) {
          this.updatesLock.readLock().unlock();
        }
        // release locks if some were acquired but another timed out
        releaseRowLocks(acquiredRowLocks);
      }

      // 14. Run post-process hook
      processor.postProcess(this, walEdit, walSyncSuccessful);

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

  /**
   * @param cell
   * @param tags
   * @return The passed-in List<Tag> but with the tags from <code>cell</code> added.
   */
  private static List<Tag> carryForwardTags(final Cell cell, final List<Tag> tags) {
    if (cell.getTagsLength() <= 0) return tags;
    List<Tag> newTags = tags == null? new ArrayList<Tag>(): /*Append Tags*/tags;
    Iterator<Tag> i =
        CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
    while (i.hasNext()) newTags.add(i.next());
    return newTags;
  }

  /**
   * Run a Get against passed in <code>store</code> on passed <code>row</code>, etc.
   * @param store
   * @param row
   * @param family
   * @param tr
   * @return Get result.
   * @throws IOException
   */
  private List<Cell> doGet(final Store store, final byte [] row,
      final Map.Entry<byte[], List<Cell>> family, final TimeRange tr)
  throws IOException {
    // Sort the cells so that they match the order that they
    // appear in the Get results. Otherwise, we won't be able to
    // find the existing values if the cells are not specified
    // in order by the client since cells are in an array list.
    Collections.sort(family.getValue(), store.getComparator());
    // Get previous values for all columns in this family
    Get get = new Get(row);
    for (Cell cell : family.getValue()) {
      get.addColumn(family.getKey(), CellUtil.cloneQualifier(cell));
    }
    if (tr != null) get.setTimeRange(tr.getMin(), tr.getMax());
    return get(get, false);
  }

  public Result append(Append append) throws IOException {
    return append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  // TODO: There's a lot of boiler plate code identical to increment.
  // We should refactor append and increment as local get-mutate-put
  // transactions, so all stores only go through one code path for puts.

  @Override
  public Result append(Append mutate, long nonceGroup, long nonce) throws IOException {
    Operation op = Operation.APPEND;
    byte[] row = mutate.getRow();
    checkRow(row, op.toString());
    checkFamilies(mutate.getFamilyCellMap().keySet());
    boolean flush = false;
    Durability durability = getEffectiveDurability(mutate.getDurability());
    boolean writeToWAL = durability != Durability.SKIP_WAL;
    WALEdit walEdits = null;
    List<Cell> allKVs = new ArrayList<Cell>(mutate.size());
    Map<Store, List<Cell>> tempMemstore = new HashMap<Store, List<Cell>>();
    long size = 0;
    long txid = 0;
    checkReadOnly();
    checkResources();
    // Lock row
    startRegionOperation(op);
    this.writeRequestsCount.increment();
    RowLock rowLock = null;
    WALKey walKey = null;
    MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
    boolean doRollBackMemstore = false;
    try {
      rowLock = getRowLock(row);
      assert rowLock != null;
      try {
        lock(this.updatesLock.readLock());
        try {
          // Wait for all prior MVCC transactions to finish - while we hold the row lock
          // (so that we are guaranteed to see the latest state when we do our Get)
          mvcc.await();
          if (this.coprocessorHost != null) {
            Result r = this.coprocessorHost.preAppendAfterRowLock(mutate);
            if (r!= null) {
              return r;
            }
          }
          long now = EnvironmentEdgeManager.currentTime();
          // Process each family
          for (Map.Entry<byte[], List<Cell>> family : mutate.getFamilyCellMap().entrySet()) {
            Store store = stores.get(family.getKey());
            List<Cell> kvs = new ArrayList<Cell>(family.getValue().size());

            List<Cell> results = doGet(store, row, family, null);

            // Iterate the input columns and update existing values if they were
            // found, otherwise add new column initialized to the append value

            // Avoid as much copying as possible. We may need to rewrite and
            // consolidate tags. Bytes are only copied once.
            // Would be nice if KeyValue had scatter/gather logic
            int idx = 0;
            for (Cell cell : family.getValue()) {
              Cell newCell;
              Cell oldCell = null;
              if (idx < results.size()
                  && CellUtil.matchingQualifier(results.get(idx), cell)) {
                oldCell = results.get(idx);
                long ts = Math.max(now, oldCell.getTimestamp());

                // Process cell tags
                // Make a union of the set of tags in the old and new KVs
                List<Tag> newTags = carryForwardTags(oldCell, new ArrayList<Tag>());
                newTags = carryForwardTags(cell, newTags);

                // Cell TTL handling

                if (mutate.getTTL() != Long.MAX_VALUE) {
                  // Add the new TTL tag
                  newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(mutate.getTTL())));
                }

                // Rebuild tags
                byte[] tagBytes = Tag.fromList(newTags);

                // allocate an empty cell once
                newCell = new KeyValue(row.length, cell.getFamilyLength(),
                    cell.getQualifierLength(), ts, KeyValue.Type.Put,
                    oldCell.getValueLength() + cell.getValueLength(),
                    tagBytes.length);
                // copy in row, family, and qualifier
                System.arraycopy(cell.getRowArray(), cell.getRowOffset(),
                  newCell.getRowArray(), newCell.getRowOffset(), cell.getRowLength());
                System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(),
                  newCell.getFamilyArray(), newCell.getFamilyOffset(),
                  cell.getFamilyLength());
                System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(),
                  newCell.getQualifierArray(), newCell.getQualifierOffset(),
                  cell.getQualifierLength());
                // copy in the value
                System.arraycopy(oldCell.getValueArray(), oldCell.getValueOffset(),
                  newCell.getValueArray(), newCell.getValueOffset(),
                  oldCell.getValueLength());
                System.arraycopy(cell.getValueArray(), cell.getValueOffset(),
                  newCell.getValueArray(),
                  newCell.getValueOffset() + oldCell.getValueLength(),
                  cell.getValueLength());
                // Copy in tag data
                System.arraycopy(tagBytes, 0, newCell.getTagsArray(), newCell.getTagsOffset(),
                  tagBytes.length);
                idx++;
              } else {
                // Append's KeyValue.Type==Put and ts==HConstants.LATEST_TIMESTAMP
                CellUtil.updateLatestStamp(cell, now);

                // Cell TTL handling

                if (mutate.getTTL() != Long.MAX_VALUE) {
                  List<Tag> newTags = new ArrayList<Tag>(1);
                  newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(mutate.getTTL())));
                  // Add the new TTL tag
                  newCell = new KeyValue(cell.getRowArray(), cell.getRowOffset(),
                      cell.getRowLength(),
                    cell.getFamilyArray(), cell.getFamilyOffset(),
                      cell.getFamilyLength(),
                    cell.getQualifierArray(), cell.getQualifierOffset(),
                      cell.getQualifierLength(),
                    cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                    cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                    newTags);
                } else {
                  newCell = cell;
                }
              }

              // Give coprocessors a chance to update the new cell
              if (coprocessorHost != null) {
                newCell = coprocessorHost.postMutationBeforeWAL(RegionObserver.MutationType.APPEND,
                    mutate, oldCell, newCell);
              }
              kvs.add(newCell);

              // Append update to WAL
              if (writeToWAL) {
                if (walEdits == null) {
                  walEdits = new WALEdit();
                }
                walEdits.add(newCell);
              }
            }

            //store the kvs to the temporary memstore before writing WAL
            tempMemstore.put(store, kvs);
          }

          // Actually write to WAL now
          if (walEdits != null && !walEdits.isEmpty()) {
            if (writeToWAL) {
              // Using default cluster id, as this can only happen in the originating
              // cluster. A slave cluster receives the final value (not the delta)
              // as a Put.
              // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
              walKey = new HLogKey(
                  getRegionInfo().getEncodedNameAsBytes(),
                  this.htableDescriptor.getTableName(),
                  WALKey.NO_SEQUENCE_ID,
                  nonceGroup,
                  nonce,
                  mvcc);
              txid =
                this.wal.append(this.htableDescriptor, getRegionInfo(), walKey, walEdits, true);
            } else {
              recordMutationWithoutWal(mutate.getFamilyCellMap());
            }
          }
          if (walKey == null) {
            // Append a faked WALEdit in order for SKIP_WAL updates to get mvcc assigned
            walKey = this.appendEmptyEdit(this.wal);
          }

          // now start my own transaction
          writeEntry = walKey.getWriteEntry();


          // Actually write to Memstore now
          if (!tempMemstore.isEmpty()) {
            for (Map.Entry<Store, List<Cell>> entry : tempMemstore.entrySet()) {
              Store store = entry.getKey();
              if (store.getFamily().getMaxVersions() == 1) {
                // upsert if VERSIONS for this CF == 1
                // Is this right? It immediately becomes visible? St.Ack 20150907
                size += store.upsert(entry.getValue(), getSmallestReadPoint());
              } else {
                // otherwise keep older versions around
                for (Cell cell: entry.getValue()) {
                  CellUtil.setSequenceId(cell, writeEntry.getWriteNumber());
                  size += store.add(cell);
                  doRollBackMemstore = true;
                }
              }
              // We add to all KVs here whereas when doing increment, we do it
              // earlier... why?
              allKVs.addAll(entry.getValue());
            }

            size = this.addAndGetGlobalMemstoreSize(size);
            flush = isFlushSize(size);
          }
        } finally {
          this.updatesLock.readLock().unlock();
        }

      } finally {
        rowLock.release();
        rowLock = null;
      }
      // sync the transaction log outside the rowlock
      if(txid != 0){
        syncOrDefer(txid, durability);
      }
      doRollBackMemstore = false;
    } finally {
      if (rowLock != null) {
        rowLock.release();
      }
      // if the wal sync was unsuccessful, remove keys from memstore
      if (doRollBackMemstore) {
        rollbackMemstore(allKVs);
        if (writeEntry != null) mvcc.complete(writeEntry);
      } else if (writeEntry != null) {
        mvcc.completeAndWait(writeEntry);
      }

      closeRegionOperation(op);
    }

    if (this.metricsRegion != null) {
      this.metricsRegion.updateAppend();
    }

    if (flush) {
      // Request a cache flush. Do it outside update lock.
      requestFlush();
    }

    return mutate.isReturnResults() ? Result.create(allKVs) : null;
  }

  public Result increment(Increment increment) throws IOException {
    return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  // TODO: There's a lot of boiler plate code identical to append.
  // We should refactor append and increment as local get-mutate-put
  // transactions, so all stores only go through one code path for puts.

  // They are subtley different in quiet a few ways. This came out only
  // after study. I am not sure that many of the differences are intentional.
  // TODO: St.Ack 20150907

  @Override
  public Result increment(Increment mutation, long nonceGroup, long nonce)
  throws IOException {
    Operation op = Operation.INCREMENT;
    byte [] row = mutation.getRow();
    checkRow(row, op.toString());
    checkFamilies(mutation.getFamilyCellMap().keySet());
    boolean flush = false;
    Durability durability = getEffectiveDurability(mutation.getDurability());
    boolean writeToWAL = durability != Durability.SKIP_WAL;
    WALEdit walEdits = null;
    List<Cell> allKVs = new ArrayList<Cell>(mutation.size());

    Map<Store, List<Cell>> tempMemstore = new HashMap<Store, List<Cell>>();
    long size = 0;
    long txid = 0;
    checkReadOnly();
    checkResources();
    // Lock row
    startRegionOperation(op);
    this.writeRequestsCount.increment();
    RowLock rowLock = null;
    WALKey walKey = null;
    MultiVersionConcurrencyControl.WriteEntry writeEntry = null;
    boolean doRollBackMemstore = false;
    TimeRange tr = mutation.getTimeRange();
    try {
      rowLock = getRowLock(row);
      assert rowLock != null;
      try {
        lock(this.updatesLock.readLock());
        try {
          // wait for all prior MVCC transactions to finish - while we hold the row lock
          // (so that we are guaranteed to see the latest state)
          mvcc.await();
          if (this.coprocessorHost != null) {
            Result r = this.coprocessorHost.preIncrementAfterRowLock(mutation);
            if (r != null) {
              return r;
            }
          }
          long now = EnvironmentEdgeManager.currentTime();
          // Process each family
          for (Map.Entry<byte [], List<Cell>> family: mutation.getFamilyCellMap().entrySet()) {
            Store store = stores.get(family.getKey());
            List<Cell> kvs = new ArrayList<Cell>(family.getValue().size());

            List<Cell> results = doGet(store, row, family, tr);

            // Iterate the input columns and update existing values if they were
            // found, otherwise add new column initialized to the increment amount

            // Avoid as much copying as possible. We may need to rewrite and
            // consolidate tags. Bytes are only copied once.
            // Would be nice if KeyValue had scatter/gather logic
            int idx = 0;
            // HERE WE DIVERGE FROM APPEND
            List<Cell> edits = family.getValue();
            for (int i = 0; i < edits.size(); i++) {
              Cell cell = edits.get(i);
              long amount = Bytes.toLong(CellUtil.cloneValue(cell));
              boolean noWriteBack = (amount == 0);

              List<Tag> newTags = carryForwardTags(cell, new ArrayList<Tag>());

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
                newTags = carryForwardTags(c, newTags);
                if (i < (edits.size() - 1) && !CellUtil.matchingQualifier(cell, edits.get(i + 1))) {
                  idx++;
                }
              }

              // Append new incremented KeyValue to list
              byte[] q = CellUtil.cloneQualifier(cell);
              byte[] val = Bytes.toBytes(amount);

              // Add the TTL tag if the mutation carried one
              if (mutation.getTTL() != Long.MAX_VALUE) {
                newTags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(mutation.getTTL())));
              }

              Cell newKV = new KeyValue(row, 0, row.length,
                family.getKey(), 0, family.getKey().length,
                q, 0, q.length,
                ts,
                KeyValue.Type.Put,
                val, 0, val.length,
                newTags);

              // Give coprocessors a chance to update the new cell
              if (coprocessorHost != null) {
                newKV = coprocessorHost.postMutationBeforeWAL(
                    RegionObserver.MutationType.INCREMENT, mutation, c, newKV);
              }
              allKVs.add(newKV);

              if (!noWriteBack) {
                kvs.add(newKV);

                // Prepare WAL updates
                if (writeToWAL) {
                  if (walEdits == null) {
                    walEdits = new WALEdit();
                  }
                  walEdits.add(newKV);
                }
              }
            }

            //store the kvs to the temporary memstore before writing WAL
            if (!kvs.isEmpty()) {
              tempMemstore.put(store, kvs);
            }
          }

          // Actually write to WAL now
          if (walEdits != null && !walEdits.isEmpty()) {
            if (writeToWAL) {
              // Using default cluster id, as this can only happen in the originating
              // cluster. A slave cluster receives the final value (not the delta)
              // as a Put.
              // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
              walKey = new HLogKey(this.getRegionInfo().getEncodedNameAsBytes(),
                  this.htableDescriptor.getTableName(),
                  WALKey.NO_SEQUENCE_ID,
                  nonceGroup,
                  nonce,
                  mvcc);
              txid = this.wal.append(this.htableDescriptor, this.getRegionInfo(),
                  walKey, walEdits, true);
            } else {
              recordMutationWithoutWal(mutation.getFamilyCellMap());
            }
          }
          if (walKey == null) {
            // Append a faked WALEdit in order for SKIP_WAL updates to get mvccNum assigned
            walKey = this.appendEmptyEdit(this.wal);
          }

          // now start my own transaction
          writeEntry = walKey.getWriteEntry();

          // Actually write to Memstore now
          if (!tempMemstore.isEmpty()) {
            for (Map.Entry<Store, List<Cell>> entry : tempMemstore.entrySet()) {
              Store store = entry.getKey();
              if (store.getFamily().getMaxVersions() == 1) {
                // upsert if VERSIONS for this CF == 1
                // Is this right? It immediately becomes visible? St.Ack 20150907
                size += store.upsert(entry.getValue(), getSmallestReadPoint());
              } else {
                // otherwise keep older versions around
                for (Cell cell : entry.getValue()) {
                  CellUtil.setSequenceId(cell, writeEntry.getWriteNumber());
                  size += store.add(cell);
                  doRollBackMemstore = true;
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
        rowLock = null;
      }
      // sync the transaction log outside the rowlock
      if(txid != 0){
        syncOrDefer(txid, durability);
      }
      doRollBackMemstore = false;
    } finally {
      if (rowLock != null) {
        rowLock.release();
      }
      // if the wal sync was unsuccessful, remove keys from memstore
      if (doRollBackMemstore) {
        for(List<Cell> cells: tempMemstore.values()) {
          rollbackMemstore(cells);
        }
        if (writeEntry != null) mvcc.complete(writeEntry);
      } else if (writeEntry != null) {
        mvcc.completeAndWait(writeEntry);
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
    return mutation.isReturnResults() ? Result.create(allKVs) : null;
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
      44 * ClassSize.REFERENCE + 3 * Bytes.SIZEOF_INT +
      (14 * Bytes.SIZEOF_LONG) +
      5 * Bytes.SIZEOF_BOOLEAN);

  // woefully out of date - currently missing:
  // 1 x HashMap - coprocessorServiceHandlers
  // 6 x Counter - numMutationsWithoutWAL, dataInMemoryWithoutWAL,
  //   checkAndMutateChecksPassed, checkAndMutateChecksFailed, readRequestsCount,
  //   writeRequestsCount
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
      MultiVersionConcurrencyControl.FIXED_SIZE // mvcc
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
    System.out.println("Usage: HRegion CATALOG_TABLE_DIR [major_compact]");
    System.out.println("Options:");
    System.out.println(" major_compact  Pass this option to major compact " +
      "passed region.");
    System.out.println("Default outputs scan of passed region.");
    System.exit(1);
  }

  @Override
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    if (coprocessorServiceHandlers.containsKey(serviceDesc.getFullName())) {
      LOG.error("Coprocessor service " + serviceDesc.getFullName() +
              " already registered, rejecting request from " + instance
      );
      return false;
    }

    coprocessorServiceHandlers.put(serviceDesc.getFullName(), instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered coprocessor service: region=" +
          Bytes.toStringBinary(getRegionInfo().getRegionName()) +
          " service=" + serviceDesc.getFullName());
    }
    return true;
  }

  @Override
  public Message execService(RpcController controller, CoprocessorServiceCall call)
      throws IOException {
    String serviceName = call.getServiceName();
    String methodName = call.getMethodName();
    if (!coprocessorServiceHandlers.containsKey(serviceName)) {
      throw new UnknownProtocolException(null,
          "No registered coprocessor service found for name "+serviceName+
          " in region "+Bytes.toStringBinary(getRegionInfo().getRegionName()));
    }

    Service service = coprocessorServiceHandlers.get(serviceName);
    Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
    Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
    if (methodDesc == null) {
      throw new UnknownProtocolException(service.getClass(),
          "Unknown method "+methodName+" called on service "+serviceName+
              " in region "+Bytes.toStringBinary(getRegionInfo().getRegionName()));
    }

    Message.Builder builder = service.getRequestPrototype(methodDesc).newBuilderForType();
    ProtobufUtil.mergeFrom(builder, call.getRequest());
    Message request = builder.build();

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

    IOException exception = ResponseConverter.getControllerException(controller);
    if (exception != null) {
      throw exception;
    }

    return responseBuilder.build();
  }

  /*
   * Process table.
   * Do major compaction or list content.
   * @throws IOException
   */
  private static void processTable(final FileSystem fs, final Path p,
      final WALFactory walFactory, final Configuration c,
      final boolean majorCompact)
  throws IOException {
    HRegion region;
    FSTableDescriptors fst = new FSTableDescriptors(c);
    // Currently expects tables have one region only.
    if (FSUtils.getTableName(p).equals(TableName.META_TABLE_NAME)) {
      final WAL wal = walFactory.getMetaWAL(
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes());
      region = HRegion.newHRegion(p, wal, fs, c,
        HRegionInfo.FIRST_META_REGIONINFO, fst.get(TableName.META_TABLE_NAME), null);
    } else {
      throw new IOException("Not a known catalog table: " + p.toString());
    }
    try {
      region.mvcc.advanceTo(region.initialize(null));
      if (majorCompact) {
        region.compact(true);
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


  /** @return the coprocessor host */
  @Override
  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** @param coprocessorHost the new coprocessor host */
  public void setCoprocessorHost(final RegionCoprocessorHost coprocessorHost) {
    this.coprocessorHost = coprocessorHost;
  }

  @Override
  public void startRegionOperation() throws IOException {
    startRegionOperation(Operation.ANY);
  }

  @Override
  public void startRegionOperation(Operation op) throws IOException {
    switch (op) {
    case GET:  // read operations
    case SCAN:
      checkReadsEnabled();
    case INCREMENT: // write operations
    case APPEND:
    case SPLIT_REGION:
    case MERGE_REGION:
    case PUT:
    case DELETE:
    case BATCH_MUTATE:
    case COMPACT_REGION:
      // when a region is in recovering state, no read, split or merge is allowed
      if (isRecovering() && (this.disallowWritesInRecovering ||
              (op != Operation.PUT && op != Operation.DELETE && op != Operation.BATCH_MUTATE))) {
        throw new RegionInRecoveryException(getRegionInfo().getRegionNameAsString() +
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
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
    }
    lock(lock.readLock());
    if (this.closed.get()) {
      lock.readLock().unlock();
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
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

  @Override
  public void closeRegionOperation() throws IOException {
    closeRegionOperation(Operation.ANY);
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding
   * to the try block of {@link #startRegionOperation(Operation)}
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
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
    }
    if (writeLockNeeded) lock(lock.writeLock());
    else lock(lock.readLock());
    if (this.closed.get()) {
      if (writeLockNeeded) lock.writeLock().unlock();
      else lock.readLock().unlock();
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
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
   * Update counters for number of puts without wal and the size of possible data loss.
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
      this.wal.sync(txid);
    } else {
      switch(durability) {
      case USE_DEFAULT:
        // do what table defaults to
        if (shouldSyncWAL()) {
          this.wal.sync(txid);
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
        this.wal.sync(txid);
        break;
      default:
        throw new RuntimeException("Unknown durability " + durability);
      }
    }
  }

  /**
   * Check whether we should sync the wal from the table's durability settings
   */
  private boolean shouldSyncWAL() {
    return durability.ordinal() >  Durability.ASYNC_WAL.ordinal();
  }

  /**
   * A mocked list implementation - discards all updates.
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
    final String logname = "wal" + FSUtils.getTableName(tableDir) + System.currentTimeMillis();

    final Configuration walConf = new Configuration(c);
    FSUtils.setRootDir(walConf, logdir);
    final WALFactory wals = new WALFactory(walConf, null, logname);
    try {
      processTable(fs, tableDir, wals, c, majorCompact);
    } finally {
       wals.close();
       // TODO: is this still right?
       BlockCache bc = new CacheConfig(c).getBlockCache();
       if (bc != null) bc.shutdown();
    }
  }

  @Override
  public long getOpenSeqNum() {
    return this.openSeqNum;
  }

  @Override
  public Map<byte[], Long> getMaxStoreSeqId() {
    return this.maxSeqIdInStores;
  }

  @Override
  public long getOldestSeqIdOfStore(byte[] familyName) {
    return wal.getEarliestMemstoreSeqNum(getRegionInfo().getEncodedNameAsBytes(), familyName);
  }

  @Override
  public CompactionState getCompactionState() {
    boolean hasMajor = majorInProgress.get() > 0, hasMinor = minorInProgress.get() > 0;
    return (hasMajor ? (hasMinor ? CompactionState.MAJOR_AND_MINOR : CompactionState.MAJOR)
        : (hasMinor ? CompactionState.MINOR : CompactionState.NONE));
  }

  public void reportCompactionRequestStart(boolean isMajor){
    (isMajor ? majorInProgress : minorInProgress).incrementAndGet();
  }

  public void reportCompactionRequestEnd(boolean isMajor, int numFiles, long filesSizeCompacted) {
    int newValue = (isMajor ? majorInProgress : minorInProgress).decrementAndGet();

    // metrics
    compactionsFinished.incrementAndGet();
    compactionNumFilesCompacted.addAndGet(numFiles);
    compactionNumBytesCompacted.addAndGet(filesSizeCompacted);

    assert newValue >= 0;
  }

  /**
   * Do not change this sequence id.
   * @return sequenceId
   */
  @VisibleForTesting
  public long getSequenceId() {
    return this.mvcc.getReadPoint();
  }


  /**
   * Append a faked WALEdit in order to get a long sequence number and wal syncer will just ignore
   * the WALEdit append later.
   * @param wal
   * @return Return the key used appending with no sync and no append.
   * @throws IOException
   */
  private WALKey appendEmptyEdit(final WAL wal) throws IOException {
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    @SuppressWarnings("deprecation")
    WALKey key = new HLogKey(getRegionInfo().getEncodedNameAsBytes(),
      getRegionInfo().getTable(), WALKey.NO_SEQUENCE_ID, 0, null,
      HConstants.NO_NONCE, HConstants.NO_NONCE, getMVCC());

    // Call append but with an empty WALEdit.  The returned sequence id will not be associated
    // with any edit and we can be sure it went in after all outstanding appends.
    try {
      wal.append(getTableDesc(), getRegionInfo(), key, WALEdit.EMPTY_WALEDIT, false);
    } catch (Throwable t) {
      // If exception, our mvcc won't get cleaned up by client, so do it here.
      getMVCC().complete(key.getWriteEntry());
    }
    return key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    // Do nothing for now.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    configurationManager = Optional.of(manager);
    for (Store s : this.stores.values()) {
      configurationManager.get().registerObserver(s);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    for (Store s : this.stores.values()) {
      configurationManager.get().deregisterObserver(s);
    }
  }

  /**
   * @return split policy for this region.
   */
  public RegionSplitPolicy getSplitPolicy() {
    return this.splitPolicy;
  }
}
