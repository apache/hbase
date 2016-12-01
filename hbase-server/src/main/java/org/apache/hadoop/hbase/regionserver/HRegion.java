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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

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
import java.util.Comparator;
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
import java.util.UUID;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
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
import org.apache.hadoop.hbase.TagUtil;
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
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver.MutationType;
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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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


@SuppressWarnings("deprecation")
@InterfaceAudience.Private
public class HRegion implements HeapSize, PropagatingConfigurationObserver, Region {
  private static final Log LOG = LogFactory.getLog(HRegion.class);

  public static final String LOAD_CFS_ON_DEMAND_CONFIG_KEY =
    "hbase.hregion.scan.loadColumnFamiliesOnDemand";

  /** Config key for using mvcc pre-assign feature for put */
  public static final String HREGION_MVCC_PRE_ASSIGN = "hbase.hregion.mvcc.preassign";
  public static final boolean DEFAULT_HREGION_MVCC_PRE_ASSIGN = true;

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

  // collects Map(s) of Store to sequence Id when handleFileNotFound() is involved
  protected List<Map> storeSeqIds = new ArrayList<>();

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
  private Map<String, com.google.protobuf.Service> coprocessorServiceHandlers = Maps.newHashMap();

  private final AtomicLong memstoreDataSize = new AtomicLong(0);// Track data size in all memstores
  private final RegionServicesForStores regionServicesForStores = new RegionServicesForStores(this);

  // Debug possible data loss due to WAL off
  final LongAdder numMutationsWithoutWAL = new LongAdder();
  final LongAdder dataInMemoryWithoutWAL = new LongAdder();

  // Debug why CAS operations are taking a while.
  final LongAdder checkAndMutateChecksPassed = new LongAdder();
  final LongAdder checkAndMutateChecksFailed = new LongAdder();

  // Number of requests
  final LongAdder readRequestsCount = new LongAdder();
  final LongAdder filteredReadRequestsCount = new LongAdder();
  final LongAdder writeRequestsCount = new LongAdder();

  // Number of requests blocked by memstore size.
  private final LongAdder blockedRequestsCount = new LongAdder();

  // Compaction LongAdders
  final AtomicLong compactionsFinished = new AtomicLong(0L);
  final AtomicLong compactionsFailed = new AtomicLong(0L);
  final AtomicLong compactionNumFilesCompacted = new AtomicLong(0L);
  final AtomicLong compactionNumBytesCompacted = new AtomicLong(0L);

  private final WAL wal;
  private final HRegionFileSystem fs;
  protected final Configuration conf;
  private final Configuration baseConf;
  private final int rowLockWaitDuration;
  private CompactedHFilesDischarger compactedFileDischarger;
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
   * The sequence ID that was enLongAddered when this region was opened.
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

  // When a region is in recovering state, it can only accept writes not reads
  private volatile boolean recovering = false;

  private volatile Optional<ConfigurationManager> configurationManager;

  // Used for testing.
  private volatile Long timeoutForWriteLock = null;

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
    synchronized (scannerReadPoints) {
      minimumReadPoint = mvcc.getReadPoint();
      for (Long readPoint : this.scannerReadPoints.values()) {
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
    AtomicInteger compacting = new AtomicInteger(0);
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
    final TreeMap<byte[], MemstoreSize> storeFlushableSize;
    final long startTime;
    final long flushOpSeqId;
    final long flushedSeqId;
    final MemstoreSize totalFlushableSize;

    /** Constructs an early exit case */
    PrepareFlushResult(FlushResult result, long flushSeqId) {
      this(result, null, null, null, Math.max(0, flushSeqId), 0, 0, new MemstoreSize());
    }

    /** Constructs a successful prepare flush result */
    PrepareFlushResult(
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], MemstoreSize> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, MemstoreSize totalFlushableSize) {
      this(null, storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
        flushSeqId, flushedSeqId, totalFlushableSize);
    }

    private PrepareFlushResult(
      FlushResult result,
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], MemstoreSize> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, MemstoreSize totalFlushableSize) {
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
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock = new ReentrantReadWriteLock();
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
  // Stores the replication scope of the various column families of the table
  // that has non-default scope
  private final NavigableMap<byte[], Integer> replicationScope = new TreeMap<byte[], Integer>(
      Bytes.BYTES_COMPARATOR);
  // flag and lock for MVCC preassign
  private final boolean mvccPreAssign;
  private final ReentrantLock preAssignMvccLock;

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

    this.wal = wal;
    this.fs = fs;

    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.baseConf = confParam;
    this.conf = new CompoundConfiguration()
      .add(confParam)
      .addStringMap(htd.getConfiguration())
      .addBytesMap(htd.getValues());
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
    Set<byte[]> families = this.htableDescriptor.getFamiliesKeys();
    for (byte[] family : families) {
      if (!replicationScope.containsKey(family)) {
        int scope = htd.getFamily(family).getScope();
        // Only store those families that has NON-DEFAULT scope
        if (scope != REPLICATION_SCOPE_LOCAL) {
          // Do a copy before storing it here.
          replicationScope.put(Bytes.copy(family), scope);
        }
      }
    }
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

    // get mvcc pre-assign flag and lock
    this.mvccPreAssign = conf.getBoolean(HREGION_MVCC_PRE_ASSIGN, DEFAULT_HREGION_MVCC_PRE_ASSIGN);
    if (this.mvccPreAssign) {
      this.preAssignMvccLock = new ReentrantLock();
    } else {
      this.preAssignMvccLock = null;
    }
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

    //Refuse to open the region if there is no column family in the table
    if (htableDescriptor.getColumnFamilies().length == 0) {
      throw new DoNotRetryIOException("Table " + htableDescriptor.getNameAsString() +
          " should have at least one column family.");
    }

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
    this.writestate.compacting.set(0);

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
      boolean hasSloppyStores = false;
      try {
        for (int i = 0; i < htableDescriptor.getFamilies().size(); i++) {
          Future<HStore> future = completionService.take();
          HStore store = future.get();
          this.stores.put(store.getFamily().getName(), store);
          if (store.isSloppyMemstore()) {
            hasSloppyStores = true;
          }

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
        if(hasSloppyStores) {
          htableDescriptor.setFlushPolicyClassName(FlushNonSloppyStoresFirstPolicy.class
              .getName());
          LOG.info("Setting FlushNonSloppyStoresFirstPolicy for the region=" + this);
        }
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
    try {
      initializeStores(reporter, status);
    } finally {
      status.markComplete("Done warming up.");
    }
  }

  /**
   * @return Map of StoreFiles by column family
   */
  private NavigableMap<byte[], List<Path>> getStoreFiles() {
    NavigableMap<byte[], List<Path>> allStoreFiles =
      new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
    for (Store store: getStores()) {
      Collection<StoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) continue;
      List<Path> storeFileNames = new ArrayList<Path>();
      for (StoreFile storeFile: storeFiles) {
        storeFileNames.add(storeFile.getPath());
      }
      allStoreFiles.put(store.getFamily().getName(), storeFileNames);
    }
    return allStoreFiles;
  }

  private void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionOpenDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_OPEN, getRegionInfo(), openSeqId,
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionOpenDesc,
        mvcc);
  }

  private void writeRegionCloseMarker(WAL wal) throws IOException {
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionEventDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_CLOSE, getRegionInfo(), mvcc.getReadPoint(),
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionEventDesc,
        mvcc);

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

  public void blockUpdates() {
    this.updatesLock.writeLock().lock();
  }

  public void unblockUpdates() {
    this.updatesLock.writeLock().unlock();
  }

  @Override
  public HDFSBlocksDistribution getHDFSBlocksDistribution() {
    HDFSBlocksDistribution hdfsBlocksDistribution =
      new HDFSBlocksDistribution();
    synchronized (this.stores) {
      for (Store store : this.stores.values()) {
        Collection<StoreFile> storeFiles = store.getStorefiles();
        if (storeFiles == null) continue;
        for (StoreFile sf : storeFiles) {
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
  public long addAndGetMemstoreSize(MemstoreSize memstoreSize) {
    if (this.rsAccounting != null) {
      rsAccounting.incGlobalMemstoreSize(memstoreSize);
    }
    long size = this.memstoreDataSize.addAndGet(memstoreSize.getDataSize());
    checkNegativeMemstoreDataSize(size, memstoreSize.getDataSize());
    return size;
  }

  public void decrMemstoreSize(MemstoreSize memstoreSize) {
    if (this.rsAccounting != null) {
      rsAccounting.decGlobalMemstoreSize(memstoreSize);
    }
    long size = this.memstoreDataSize.addAndGet(-memstoreSize.getDataSize());
    checkNegativeMemstoreDataSize(size, -memstoreSize.getDataSize());
  }

  private void checkNegativeMemstoreDataSize(long memstoreDataSize, long delta) {
    // This is extremely bad if we make memstoreSize negative. Log as much info on the offending
    // caller as possible. (memStoreSize might be a negative value already -- freeing memory)
    if (memstoreDataSize < 0) {
      LOG.error("Asked to modify this region's (" + this.toString()
          + ") memstoreSize to a negative value which is incorrect. Current memstoreSize="
          + (memstoreDataSize - delta) + ", delta=" + delta, new Exception());
    }
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
    return readRequestsCount.sum();
  }

  @Override
  public void updateReadRequestsCount(long i) {
    readRequestsCount.add(i);
  }

  @Override
  public long getFilteredReadRequestsCount() {
    return filteredReadRequestsCount.sum();
  }

  @Override
  public long getWriteRequestsCount() {
    return writeRequestsCount.sum();
  }

  @Override
  public void updateWriteRequestsCount(long i) {
    writeRequestsCount.add(i);
  }

  @Override
  public long getMemstoreSize() {
    return memstoreDataSize.get();
  }

  @Override
  public RegionServicesForStores getRegionServicesForStores() {
    return regionServicesForStores;
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return numMutationsWithoutWAL.sum();
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return dataInMemoryWithoutWAL.sum();
  }

  @Override
  public long getBlockedRequestsCount() {
    return blockedRequestsCount.sum();
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return checkAndMutateChecksPassed.sum();
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return checkAndMutateChecksFailed.sum();
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
    // Before we flip the recovering switch (enabling reads) we should write the region open
    // event to WAL if needed
    if (wal != null && getRegionServerServices() != null && !writestate.readOnly
        && wasRecovering && !newState) {

      // force a flush only if region replication is set up for this region. Otherwise no need.
      boolean forceFlush = getTableDesc().getRegionReplication() > 1;

      MonitoredTask status = TaskMonitor.get().createStatus("Recovering region " + this);

      try {
        // force a flush first
        if (forceFlush) {
          status.setStatus("Flushing region " + this + " because recovery is finished");
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
              + "event to WAL, continuing", e);
        }
      } catch (IOException ioe) {
        // Distributed log replay semantics does not necessarily require a flush, since the replayed
        // data is already written again in the WAL. So failed flush should be fine.
        LOG.warn(getRegionInfo().getEncodedName() + " : was not able to flush "
            + "event to WAL, continuing", ioe);
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

  @VisibleForTesting
  public MultiVersionConcurrencyControl getMVCC() {
    return mvcc;
  }

  @Override
  public long getMaxFlushedSeqId() {
    return maxFlushedSeqId;
  }

  @Override
  public long getReadPoint(IsolationLevel isolationLevel) {
    if (isolationLevel != null && isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
      // This scan can read even uncommitted transactions
      return Long.MAX_VALUE;
    }
    return mvcc.getReadPoint();
  }

  @Override
  public long getReadpoint(IsolationLevel isolationLevel) {
    return getReadPoint(isolationLevel);
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

  /**
   * The {@link HRegion#doClose} will block forever if someone tries proving the dead lock via the unit test.
   * Instead of blocking, the {@link HRegion#doClose} will throw exception if you set the timeout.
   * @param timeoutForWriteLock the second time to wait for the write lock in {@link HRegion#doClose}
   */
  @VisibleForTesting
  public void setTimeoutForWriteLock(long timeoutForWriteLock) {
    assert timeoutForWriteLock >= 0;
    this.timeoutForWriteLock = timeoutForWriteLock;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK_EXCEPTION_PATH",
      justification="I think FindBugs is confused")
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

    if (timeoutForWriteLock == null
        || timeoutForWriteLock == Long.MAX_VALUE) {
      // block waiting for the lock for closing
      lock.writeLock().lock(); // FindBugs: Complains UL_UNRELEASED_LOCK_EXCEPTION_PATH but seems fine
    } else {
      try {
        boolean succeed = lock.writeLock().tryLock(timeoutForWriteLock, TimeUnit.SECONDS);
        if (!succeed) {
          throw new IOException("Failed to get write lock when closing region");
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
    }
    this.closing.set(true);
    status.setStatus("Disabling writes for close");
    try {
      if (this.isClosed()) {
        status.abort("Already got closed by another process");
        // SplitTransaction handles the null
        return null;
      }
      LOG.debug("Updates disabled for region " + this);
      // Don't flush the cache if we are aborting
      if (!abort && canFlush) {
        int failedfFlushCount = 0;
        int flushCount = 0;
        long tmp = 0;
        long remainingSize = this.memstoreDataSize.get();
        while (remainingSize > 0) {
          try {
            internalFlushcache(status);
            if(flushCount >0) {
              LOG.info("Running extra flush, " + flushCount +
                  " (carrying snapshot?) " + this);
            }
            flushCount++;
            tmp = this.memstoreDataSize.get();
            if (tmp >= remainingSize) {
              failedfFlushCount++;
            }
            remainingSize = tmp;
            if (failedfFlushCount > 5) {
              // If we failed 5 times and are unable to clear memory, abort
              // so we do not lose data
              throw new DroppedSnapshotException("Failed clearing memory after " +
                  flushCount + " attempts on region: " +
                  Bytes.toStringBinary(getRegionInfo().getRegionName()));
            }
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
          MemstoreSize flushableSize = store.getSizeToFlush();
          if (!(abort || flushableSize.getDataSize() == 0 || writestate.readOnly)) {
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
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          }
          throw new IOException(cause);
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
        this.decrMemstoreSize(new MemstoreSize(memstoreDataSize.get(), getMemstoreHeapOverhead()));
      } else if (memstoreDataSize.get() != 0) {
        LOG.error("Memstore size is " + memstoreDataSize.get());
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
      // stop the Compacted hfile discharger
      if (this.compactedFileDischarger != null) this.compactedFileDischarger.cancel(true);

      status.markComplete("Closed");
      LOG.info("Closed " + this);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private long getMemstoreHeapOverhead() {
    long overhead = 0;
    for (Store s : this.stores.values()) {
      overhead += s.getSizeOfMemStore().getHeapOverhead();
    }
    return overhead;
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
        while (writestate.compacting.get() > 0 || writestate.flushing) {
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
    return this.memstoreDataSize.get() >
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
   * @return split policy for this region.
   */
  public RegionSplitPolicy getSplitPolicy() {
    return this.splitPolicy;
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
    return Collections.min(lastStoreFlushTimeMap.values());
  }

  @Override
  public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
    long result = Long.MAX_VALUE;
    for (Store store : getStores()) {
      Collection<StoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) continue;
      for (StoreFile file : storeFiles) {
        StoreFileReader sfReader = file.getReader();
        if (sfReader == null) continue;
        HFile.Reader reader = sfReader.getHFileReader();
        if (reader == null) continue;
        if (majorCompactionOnly) {
          byte[] val = reader.loadFileInfo().get(StoreFile.MAJOR_COMPACTION_KEY);
          if (val == null || !Bytes.toBoolean(val)) continue;
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
      regionLoadBldr.addStoreCompleteSequenceId(StoreSequenceId.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(familyName)).setSequenceId(csid).build());
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
        ThroughputController controller = null;
        if (rsServices != null) {
          controller = CompactionThroughputControllerFactory.create(rsServices, conf);
        }
        if (controller == null) {
          controller = NoLimitThroughputController.INSTANCE;
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
        compact(compaction, s, NoLimitThroughputController.INSTANCE, null);
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
  void compactStore(byte[] family, ThroughputController throughputController)
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
      ThroughputController throughputController) throws IOException {
    return compact(compaction, store, throughputController, null);
  }

  public boolean compact(CompactionContext compaction, Store store,
      ThroughputController throughputController, User user) throws IOException {
    assert compaction != null && compaction.hasSelection();
    assert !compaction.getRequest().getFiles().isEmpty();
    if (this.closing.get() || this.closed.get()) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      store.cancelRequestedCompaction(compaction);
      return false;
    }
    MonitoredTask status = null;
    boolean requestNeedsCancellation = true;
    /*
     * We are trying to remove / relax the region read lock for compaction.
     * Let's see what are the potential race conditions among the operations (user scan,
     * region split, region close and region bulk load).
     *
     *  user scan ---> region read lock
     *  region split --> region close first --> region write lock
     *  region close --> region write lock
     *  region bulk load --> region write lock
     *
     * read lock is compatible with read lock. ---> no problem with user scan/read
     * region bulk load does not cause problem for compaction (no consistency problem, store lock
     *  will help the store file accounting).
     * They can run almost concurrently at the region level.
     *
     * The only remaining race condition is between the region close and compaction.
     * So we will evaluate, below, how region close intervenes with compaction if compaction does
     * not acquire region read lock.
     *
     * Here are the steps for compaction:
     * 1. obtain list of StoreFile's
     * 2. create StoreFileScanner's based on list from #1
     * 3. perform compaction and save resulting files under tmp dir
     * 4. swap in compacted files
     *
     * #1 is guarded by store lock. This patch does not change this --> no worse or better
     * For #2, we obtain smallest read point (for region) across all the Scanners (for both default
     * compactor and stripe compactor).
     * The read points are for user scans. Region keeps the read points for all currently open
     * user scanners.
     * Compaction needs to know the smallest read point so that during re-write of the hfiles,
     * it can remove the mvcc points for the cells if their mvccs are older than the smallest
     * since they are not needed anymore.
     * This will not conflict with compaction.
     * For #3, it can be performed in parallel to other operations.
     * For #4 bulk load and compaction don't conflict with each other on the region level
     *   (for multi-family atomicy).
     * Region close and compaction are guarded pretty well by the 'writestate'.
     * In HRegion#doClose(), we have :
     * synchronized (writestate) {
     *   // Disable compacting and flushing by background threads for this
     *   // region.
     *   canFlush = !writestate.readOnly;
     *   writestate.writesEnabled = false;
     *   LOG.debug("Closing " + this + ": disabling compactions & flushes");
     *   waitForFlushesAndCompactions();
     * }
     * waitForFlushesAndCompactions() would wait for writestate.compacting to come down to 0.
     * and in HRegion.compact()
     *  try {
     *    synchronized (writestate) {
     *    if (writestate.writesEnabled) {
     *      wasStateSet = true;
     *      ++writestate.compacting;
     *    } else {
     *      String msg = "NOT compacting region " + this + ". Writes disabled.";
     *      LOG.info(msg);
     *      status.abort(msg);
     *      return false;
     *    }
     *  }
     * Also in compactor.performCompaction():
     * check periodically to see if a system stop is requested
     * if (closeCheckInterval > 0) {
     *   bytesWritten += len;
     *   if (bytesWritten > closeCheckInterval) {
     *     bytesWritten = 0;
     *     if (!store.areWritesEnabled()) {
     *       progress.cancel();
     *       return false;
     *     }
     *   }
     * }
     */
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
            writestate.compacting.incrementAndGet();
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
            writestate.compacting.decrementAndGet();
            if (writestate.compacting.get() <= 0) {
              writestate.notifyAll();
            }
          }
        }
      }
      status.markComplete("Compaction complete");
      return true;
    } finally {
      if (requestNeedsCancellation) store.cancelRequestedCompaction(compaction);
      if (status != null) status.cleanup();
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
      if (numMutationsWithoutWAL.sum() > 0) {
        numMutationsWithoutWAL.reset();
        dataInMemoryWithoutWAL.reset();
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
   * Every FlushPolicy should call this to determine whether a store is old enough to flush (except
   * that you always flush all stores). Otherwise the method will always
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
   * @param wal Null if we're NOT to go via wal.
   * @param myseqid The seqid to use if <code>wal</code> is null writing out flush file.
   * @param storesToFlush The list of stores to flush.
   * @return object describing the flush's state
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of WAL is required.
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DLS_DEAD_LOCAL_STORE",
      justification="FindBugs seems confused about trxId")
  protected PrepareFlushResult internalPrepareFlushCache(final WAL wal, final long myseqid,
      final Collection<Store> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker)
  throws IOException {
    if (this.rsServices != null && this.rsServices.isAborted()) {
      // Don't flush when server aborting, it's unsafe
      throw new IOException("Aborting flush because server is aborted...");
    }
    final long startTime = EnvironmentEdgeManager.currentTime();
    // If nothing to flush, return, but return with a valid unused sequenceId.
    // Its needed by bulk upload IIRC. It flushes until no edits in memory so it can insert a
    // bulk loaded file between memory and existing hfiles. It wants a good seqeunceId that belongs
    // to no other that it can use to associate with the bulk load. Hence this little dance below
    // to go get one.
    if (this.memstoreDataSize.get() <= 0) {
      // Take an update lock so no edits can come into memory just yet.
      this.updatesLock.writeLock().lock();
      WriteEntry writeEntry = null;
      try {
        if (this.memstoreDataSize.get() <= 0) {
          // Presume that if there are still no edits in the memstore, then there are no edits for
          // this region out in the WAL subsystem so no need to do any trickery clearing out
          // edits in the WAL sub-system. Up the sequence number so the resulting flush id is for
          // sure just beyond the last appended region edit and not associated with any edit
          // (useful as marker when bulk loading, etc.).
          FlushResult flushResult = null;
          if (wal != null) {
            writeEntry = mvcc.begin();
            long flushOpSeqId = writeEntry.getWriteNumber();
            flushResult = new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY,
              flushOpSeqId, "Nothing to flush",
            writeFlushRequestMarkerToWAL(wal, writeFlushWalMarker));
            mvcc.completeAndWait(writeEntry);
            // Set to null so we don't complete it again down in finally block.
            writeEntry = null;
            return new PrepareFlushResult(flushResult, myseqid);
          } else {
            return new PrepareFlushResult(new FlushResultImpl(
              FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, "Nothing to flush", false), myseqid);
          }
        }
      } finally {
        if (writeEntry != null) {
          // If writeEntry is non-null, this operation failed; the mvcc transaction failed...
          // but complete it anyways so it doesn't block the mvcc queue.
          mvcc.complete(writeEntry);
        }
        this.updatesLock.writeLock().unlock();
      }
    }
    logFatLineOnFlush(storesToFlush, myseqid);
    // Stop updates while we snapshot the memstore of all of these regions' stores. We only have
    // to do this for a moment.  It is quick. We also set the memstore size to zero here before we
    // allow updates again so its value will represent the size of the updates received
    // during flush

    // We have to take an update lock during snapshot, or else a write could end up in both snapshot
    // and memstore (makes it difficult to do atomic rows then)
    status.setStatus("Obtaining lock to block concurrent updates");
    // block waiting for the lock for internal flush
    this.updatesLock.writeLock().lock();
    status.setStatus("Preparing flush snapshotting stores in " + getRegionInfo().getEncodedName());
    MemstoreSize totalSizeOfFlushableStores = new MemstoreSize();

    Set<byte[]> flushedFamilyNames = new HashSet<byte[]>();
    for (Store store: storesToFlush) {
      flushedFamilyNames.add(store.getFamily().getName());
    }

    TreeMap<byte[], StoreFlushContext> storeFlushCtxs
      = new TreeMap<byte[], StoreFlushContext>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], List<Path>> committedFiles = new TreeMap<byte[], List<Path>>(
        Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], MemstoreSize> storeFlushableSize
        = new TreeMap<byte[], MemstoreSize>(Bytes.BYTES_COMPARATOR);
    // The sequence id of this flush operation which is used to log FlushMarker and pass to
    // createFlushContext to use as the store file's sequence id. It can be in advance of edits
    // still in the memstore, edits that are in other column families yet to be flushed.
    long flushOpSeqId = HConstants.NO_SEQNUM;
    // The max flushed sequence id after this flush operation completes. All edits in memstore
    // will be in advance of this sequence id.
    long flushedSeqId = HConstants.NO_SEQNUM;
    byte[] encodedRegionName = getRegionInfo().getEncodedNameAsBytes();
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
        MemstoreSize flushableSize = s.getSizeToFlush();
        totalSizeOfFlushableStores.incMemstoreSize(flushableSize);
        storeFlushCtxs.put(s.getFamily().getName(), s.createFlushContext(flushOpSeqId));
        committedFiles.put(s.getFamily().getName(), null); // for writing stores to WAL
        storeFlushableSize.put(s.getFamily().getName(), flushableSize);
      }

      // write the snapshot start to WAL
      if (wal != null && !writestate.readOnly) {
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH,
            getRegionInfo(), flushOpSeqId, committedFiles);
        // No sync. Sync is below where no updates lock and we do FlushAction.COMMIT_FLUSH
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false,
            mvcc);
      }

      // Prepare flush (take a snapshot)
      for (StoreFlushContext flush : storeFlushCtxs.values()) {
        flush.prepare();
      }
    } catch (IOException ex) {
      doAbortFlushToWAL(wal, flushOpSeqId, committedFiles);
      throw ex;
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    String s = "Finished memstore snapshotting " + this + ", syncing WAL and waiting on mvcc, " +
        "flushsize=" + totalSizeOfFlushableStores;
    status.setStatus(s);
    doSyncOfUnflushedWALChanges(wal, getRegionInfo());
    return new PrepareFlushResult(storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
        flushOpSeqId, flushedSeqId, totalSizeOfFlushableStores);
  }

  /**
   * Utility method broken out of internalPrepareFlushCache so that method is smaller.
   */
  private void logFatLineOnFlush(final Collection<Store> storesToFlush, final long sequenceId) {
    if (!LOG.isInfoEnabled()) {
      return;
    }
    // Log a fat line detailing what is being flushed.
    StringBuilder perCfExtras = null;
    if (!isAllFamilies(storesToFlush)) {
      perCfExtras = new StringBuilder();
      for (Store store: storesToFlush) {
        perCfExtras.append("; ").append(store.getColumnFamilyName());
        perCfExtras.append("=")
            .append(StringUtils.byteDesc(store.getSizeToFlush().getDataSize()));
      }
    }
    LOG.info("Flushing " + + storesToFlush.size() + "/" + stores.size() +
        " column families, memstore=" + StringUtils.byteDesc(this.memstoreDataSize.get()) +
        ((perCfExtras != null && perCfExtras.length() > 0)? perCfExtras.toString(): "") +
        ((wal != null) ? "" : "; WAL is null, using passed sequenceid=" + sequenceId));
  }

  private void doAbortFlushToWAL(final WAL wal, final long flushOpSeqId,
      final Map<byte[], List<Path>> committedFiles) {
    if (wal == null) return;
    try {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
      WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false,
          mvcc);
    } catch (Throwable t) {
      LOG.warn("Received unexpected exception trying to write ABORT_FLUSH marker to WAL:" +
          StringUtils.stringifyException(t));
      // ignore this since we will be aborting the RS with DSE.
    }
    // we have called wal.startCacheFlush(), now we have to abort it
    wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
  }

  /**
   * Sync unflushed WAL changes. See HBASE-8208 for details
   */
  private static void doSyncOfUnflushedWALChanges(final WAL wal, final HRegionInfo hri)
  throws IOException {
    if (wal == null) {
      return;
    }
    try {
      wal.sync(); // ensure that flush marker is sync'ed
    } catch (IOException ioe) {
      wal.abortCacheFlush(hri.getEncodedNameAsBytes());
      throw ioe;
    }
  }

  /**
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
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true,
            mvcc);
        return true;
      } catch (IOException e) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Received exception while trying to write the flush request to wal", e);
      }
    }
    return false;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Intentional; notify is about completed flush")
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

    String s = "Flushing stores of " + this;
    status.setStatus(s);
    if (LOG.isTraceEnabled()) LOG.trace(s);

    // Any failure from here on out will be catastrophic requiring server
    // restart so wal content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the wal, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    long flushedOutputFileSize = 0;
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
          MemstoreSize storeFlushableSize = prepareResult.storeFlushableSize.get(storeName);
          prepareResult.totalFlushableSize.decMemstoreSize(storeFlushableSize);
        }
        flushedOutputFileSize += flush.getOutputFileSize();
      }
      storeFlushCtxs.clear();

      // Set down the memstore size by amount of flush.
      this.decrMemstoreSize(prepareResult.totalFlushableSize);

      if (wal != null) {
        // write flush marker to WAL. If fail, we should throw DroppedSnapshotException
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.COMMIT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true,
            mvcc);
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
          WALUtil.writeFlushMarker(wal, this.replicationScope, getRegionInfo(), desc, false, mvcc);
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
    for(Store storeToFlush :storesToFlush) {
      ((HStore) storeToFlush).finalizeFlush();
    }
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
    long memstoresize = this.memstoreDataSize.get();
    String msg = "Finished memstore flush of ~"
        + StringUtils.byteDesc(prepareResult.totalFlushableSize.getDataSize()) + "/"
        + prepareResult.totalFlushableSize.getDataSize() + ", currentsize="
        + StringUtils.byteDesc(memstoresize) + "/" + memstoresize
        + " for region " + this + " in " + time + "ms, sequenceid="
        + flushOpSeqId +  ", compaction requested=" + compactionRequested
        + ((wal == null) ? "; wal=null" : "");
    LOG.info(msg);
    status.setStatus(msg);

    if (rsServices != null && rsServices.getMetrics() != null) {
      rsServices.getMetrics().updateFlush(time - startTime,
          prepareResult.totalFlushableSize.getDataSize(), flushedOutputFileSize);
    }

    return new FlushResultImpl(compactionRequested ?
        FlushResult.Result.FLUSHED_COMPACTION_NEEDED :
          FlushResult.Result.FLUSHED_NO_COMPACTION_NEEDED, flushOpSeqId);
  }

  /**
   * Method to safely get the next sequence number.
   * @return Next sequence number unassociated with any actual edit.
   * @throws IOException
   */
  @VisibleForTesting
  protected long getNextSequenceId(final WAL wal) throws IOException {
    WriteEntry we = mvcc.begin();
    mvcc.completeAndWait(we);
    return we.getWriteNumber();
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public RegionScanner getScanner(Scan scan) throws IOException {
   return getScanner(scan, null);
  }

  @Override
  public RegionScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners)
      throws IOException {
    return getScanner(scan, additionalScanners, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private RegionScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners,
      long nonceGroup, long nonce) throws IOException {
    startRegionOperation(Operation.SCAN);
    try {
      // Verify families are all valid
      if (!scan.hasFamilies()) {
        // Adding all families to scanner
        for (byte[] family : this.htableDescriptor.getFamiliesKeys()) {
          scan.addFamily(family);
        }
      } else {
        for (byte[] family : scan.getFamilyMap().keySet()) {
          checkFamily(family);
        }
      }
      return instantiateRegionScanner(scan, additionalScanners, nonceGroup, nonce);
    } finally {
      closeRegionOperation(Operation.SCAN);
    }
  }

  protected RegionScanner instantiateRegionScanner(Scan scan,
      List<KeyValueScanner> additionalScanners) throws IOException {
    return instantiateRegionScanner(scan, additionalScanners, HConstants.NO_NONCE,
      HConstants.NO_NONCE);
  }

  protected RegionScanner instantiateRegionScanner(Scan scan,
      List<KeyValueScanner> additionalScanners, long nonceGroup, long nonce) throws IOException {
    if (scan.isReversed()) {
      if (scan.getFilter() != null) {
        scan.getFilter().setReversed(true);
      }
      return new ReversedRegionScannerImpl(scan, additionalScanners, this);
    }
    return new RegionScannerImpl(scan, additionalScanners, this, nonceGroup, nonce);
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
  private abstract static class BatchOperation<T> {
    T[] operations;
    int nextIndexToProcess = 0;
    OperationStatus[] retCodeDetails;
    WALEdit[] walEditsFromCoprocessors;

    public BatchOperation(T[] operations) {
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

  private static class MutationBatch extends BatchOperation<Mutation> {
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

  private static class ReplayBatch extends BatchOperation<MutationReplay> {
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
  OperationStatus[] batchMutate(BatchOperation<?> batchOp) throws IOException {
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
            doPreBatchMutateHook(batchOp);
          }
          initialized = true;
        }
        doMiniBatchMutate(batchOp);
        long newSize = this.getMemstoreSize();
        requestFlushIfNeeded(newSize);
      }
    } finally {
      closeRegionOperation(op);
    }
    return batchOp.retCodeDetails;
  }

  private void doPreBatchMutateHook(BatchOperation<?> batchOp)
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

  /**
   * Called to do a piece of the batch that came in to {@link #batchMutate(Mutation[], long, long)}
   * In here we also handle replay of edits on region recover.
   * @return Change in size brought about by applying <code>batchOp</code>
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK",
      justification="Findbugs seems to be confused on this.")
  @SuppressWarnings("unchecked")
  // TODO: This needs a rewrite. Doesn't have to be this long. St.Ack 20160120
  private void doMiniBatchMutate(BatchOperation<?> batchOp) throws IOException {
    boolean replay = batchOp.isInReplay();
    // Variable to note if all Put items are for the same CF -- metrics related
    boolean putsCfSetConsistent = true;
    // Variable to note if all Delete items are for the same CF -- metrics related
    boolean deletesCfSetConsistent = true;
    // The set of columnFamilies first seen for Put.
    Set<byte[]> putsCfSet = null;
    // The set of columnFamilies first seen for Delete.
    Set<byte[]> deletesCfSet = null;
    long currentNonceGroup = HConstants.NO_NONCE;
    long currentNonce = HConstants.NO_NONCE;
    WALEdit walEdit = null;
    boolean locked = false;
    // reference family maps directly so coprocessors can mutate them if desired
    Map<byte[], List<Cell>>[] familyMaps = new Map[batchOp.operations.length];
    // We try to set up a batch in the range [firstIndex,lastIndexExclusive)
    int firstIndex = batchOp.nextIndexToProcess;
    int lastIndexExclusive = firstIndex;
    boolean success = false;
    int noOfPuts = 0;
    int noOfDeletes = 0;
    WriteEntry writeEntry = null;
    int cellCount = 0;
    /** Keep track of the locks we hold so we can release them in finally clause */
    List<RowLock> acquiredRowLocks = Lists.newArrayListWithCapacity(batchOp.operations.length);
    MemstoreSize memstoreSize = new MemstoreSize();
    try {
      // STEP 1. Try to acquire as many locks as we can, and ensure we acquire at least one.
      int numReadyToWrite = 0;
      long now = EnvironmentEdgeManager.currentTime();
      while (lastIndexExclusive < batchOp.operations.length) {
        if (checkBatchOp(batchOp, lastIndexExclusive, familyMaps, now)) {
          lastIndexExclusive++;
          continue;
        }
        Mutation mutation = batchOp.getMutation(lastIndexExclusive);
        // If we haven't got any rows in our batch, we should block to get the next one.
        RowLock rowLock = null;
        try {
          rowLock = getRowLockInternal(mutation.getRow(), true);
        } catch (IOException ioe) {
          LOG.warn("Failed getting lock, row=" + Bytes.toStringBinary(mutation.getRow()), ioe);
        }
        if (rowLock == null) {
          // We failed to grab another lock
          break; // Stop acquiring more rows for this batch
        } else {
          acquiredRowLocks.add(rowLock);
        }

        lastIndexExclusive++;
        numReadyToWrite++;
        if (replay) {
          for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
            cellCount += cells.size();
          }
        }
        if (mutation instanceof Put) {
          // If Column Families stay consistent through out all of the
          // individual puts then metrics can be reported as a multiput across
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

      // We've now grabbed as many mutations off the list as we can

      // STEP 2. Update any LATEST_TIMESTAMP timestamps
      // We should record the timestamp only after we have acquired the rowLock,
      // otherwise, newer puts/deletes are not guaranteed to have a newer timestamp
      now = EnvironmentEdgeManager.currentTime();
      byte[] byteNow = Bytes.toBytes(now);

      // Nothing to put/delete -- an exception in the above such as NoSuchColumnFamily?
      if (numReadyToWrite <= 0) {
        return;
      }

      for (int i = firstIndex; !replay && i < lastIndexExclusive; i++) {
        // skip invalid
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
            != OperationStatusCode.NOT_RUN) {
          // lastIndexExclusive was incremented above.
          continue;
        }

        Mutation mutation = batchOp.getMutation(i);
        if (mutation instanceof Put) {
          updateCellTimestamps(familyMaps[i].values(), byteNow);
          noOfPuts++;
        } else {
          prepareDeleteTimestamps(mutation, familyMaps[i], byteNow);
          noOfDeletes++;
        }
        rewriteCellTags(familyMaps[i], mutation);
        WALEdit fromCP = batchOp.walEditsFromCoprocessors[i];
        if (fromCP != null) {
          cellCount += fromCP.size();
        }
        for (List<Cell> cells : familyMaps[i].values()) {
          cellCount += cells.size();
        }
      }
      walEdit = new WALEdit(cellCount, replay);
      lock(this.updatesLock.readLock(), numReadyToWrite);
      locked = true;

      // calling the pre CP hook for batch mutation
      if (!replay && coprocessorHost != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
          new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        if (coprocessorHost.preBatchMutate(miniBatchOp)) {
          return;
        } else {
          for (int i = firstIndex; i < lastIndexExclusive; i++) {
            if (batchOp.retCodeDetails[i].getOperationStatusCode() != OperationStatusCode.NOT_RUN) {
              // lastIndexExclusive was incremented above.
              continue;
            }
            // we pass (i - firstIndex) below since the call expects a relative index
            Mutation[] cpMutations = miniBatchOp.getOperationsFromCoprocessors(i - firstIndex);
            if (cpMutations == null) {
              continue;
            }
            // Else Coprocessor added more Mutations corresponding to the Mutation at this index.
            for (int j = 0; j < cpMutations.length; j++) {
              Mutation cpMutation = cpMutations[j];
              Map<byte[], List<Cell>> cpFamilyMap = cpMutation.getFamilyCellMap();
              checkAndPrepareMutation(cpMutation, replay, cpFamilyMap, now);

              // Acquire row locks. If not, the whole batch will fail.
              acquiredRowLocks.add(getRowLockInternal(cpMutation.getRow(), true));

              // Returned mutations from coprocessor correspond to the Mutation at index i. We can
              // directly add the cells from those mutations to the familyMaps of this mutation.
              mergeFamilyMaps(familyMaps[i], cpFamilyMap); // will get added to the memstore later
            }
          }
        }
      }

      // STEP 3. Build WAL edit
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
        // we use durability of the original mutation for the mutation passed by CP.
        if (tmpDur == Durability.SKIP_WAL) {
          recordMutationWithoutWal(m.getFamilyCellMap());
          continue;
        }

        long nonceGroup = batchOp.getNonceGroup(i);
        long nonce = batchOp.getNonce(i);
        // In replay, the batch may contain multiple nonces. If so, write WALEdit for each.
        // Given how nonces are originally written, these should be contiguous.
        // They don't have to be, it will still work, just write more WALEdits than needed.
        if (nonceGroup != currentNonceGroup || nonce != currentNonce) {
          // Write what we have so far for nonces out to WAL
          appendCurrentNonces(m, replay, walEdit, now, currentNonceGroup, currentNonce);
          walEdit = new WALEdit(cellCount, replay);
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

      // STEP 4. Append the final edit to WAL and sync.
      Mutation mutation = batchOp.getMutation(firstIndex);
      WALKey walKey = null;
      long txid;
      if (replay) {
        // use wal key from the original
        walKey = new WALKey(this.getRegionInfo().getEncodedNameAsBytes(),
          this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
          mutation.getClusterIds(), currentNonceGroup, currentNonce, mvcc);
        walKey.setOrigLogSeqNum(batchOp.getReplaySequenceId());
        if (!walEdit.isEmpty()) {
          txid = this.wal.append(this.getRegionInfo(), walKey, walEdit, true);
          if (txid != 0) {
            sync(txid, durability);
          }
        }
      } else {
        try {
          if (!walEdit.isEmpty()) {
            try {
              if (this.mvccPreAssign) {
                preAssignMvccLock.lock();
                writeEntry = mvcc.begin();
              }
              // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
              walKey = new WALKey(this.getRegionInfo().getEncodedNameAsBytes(),
                  this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
                  mutation.getClusterIds(), currentNonceGroup, currentNonce, mvcc,
                  this.getReplicationScope());
              if (this.mvccPreAssign) {
                walKey.setPreAssignedWriteEntry(writeEntry);
              }
              // TODO: Use the doAppend methods below... complicated by the replay stuff above.
              txid = this.wal.append(this.getRegionInfo(), walKey, walEdit, true);
            } finally {
              if (mvccPreAssign) {
                preAssignMvccLock.unlock();
              }
            }
            if (txid != 0) {
              sync(txid, durability);
            }
            if (writeEntry == null) {
              // if MVCC not preassigned, wait here until assigned
              writeEntry = walKey.getWriteEntry();
            }
          }
        } catch (IOException ioe) {
          if (walKey != null && writeEntry == null) {
            // the writeEntry is not preassigned and error occurred during append or sync
            mvcc.complete(walKey.getWriteEntry());
          }
          throw ioe;
        }
      }
      if (walKey == null) {
        // If no walKey, then not in replay and skipping WAL or some such. Begin an MVCC transaction
        // to get sequence id.
        writeEntry = mvcc.begin();
      }

      // STEP 5. Write back to memstore
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        if (batchOp.retCodeDetails[i].getOperationStatusCode() != OperationStatusCode.NOT_RUN) {
          continue;
        }
        // We need to update the sequence id for following reasons.
        // 1) If the op is in replay mode, FSWALEntry#stampRegionSequenceId won't stamp sequence id.
        // 2) If no WAL, FSWALEntry won't be used
        // we use durability of the original mutation for the mutation passed by CP.
        boolean updateSeqId = replay || batchOp.getMutation(i).getDurability() == Durability.SKIP_WAL;
        if (updateSeqId) {
          this.updateSequenceId(familyMaps[i].values(),
            replay? batchOp.getReplaySequenceId(): writeEntry.getWriteNumber());
        }
        applyFamilyMapToMemstore(familyMaps[i], memstoreSize);
      }

      // calling the post CP hook for batch mutation
      if (!replay && coprocessorHost != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
          new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        coprocessorHost.postBatchMutate(miniBatchOp);
      }

      // STEP 6. Complete mvcc.
      if (replay) {
        this.mvcc.advanceTo(batchOp.getReplaySequenceId());
      } else {
        // writeEntry won't be empty if not in replay mode
        mvcc.completeAndWait(writeEntry);
        writeEntry = null;
      }

      // STEP 7. Release row locks, etc.
      if (locked) {
        this.updatesLock.readLock().unlock();
        locked = false;
      }
      releaseRowLocks(acquiredRowLocks);

      for (int i = firstIndex; i < lastIndexExclusive; i ++) {
        if (batchOp.retCodeDetails[i] == OperationStatus.NOT_RUN) {
          batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
        }
      }

      // STEP 8. Run coprocessor post hooks. This should be done after the wal is
      // synced so that the coprocessor contract is adhered to.
      if (!replay && coprocessorHost != null) {
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
    } finally {
      // Call complete rather than completeAndWait because we probably had error if walKey != null
      if (writeEntry != null) mvcc.complete(writeEntry);
      this.addAndGetMemstoreSize(memstoreSize);
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
          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        coprocessorHost.postBatchMutateIndispensably(miniBatchOp, success);
      }

      batchOp.nextIndexToProcess = lastIndexExclusive;
    }
  }

  private void mergeFamilyMaps(Map<byte[], List<Cell>> familyMap,
      Map<byte[], List<Cell>> toBeMerged) {
    for (Map.Entry<byte[], List<Cell>> entry : toBeMerged.entrySet()) {
      List<Cell> cells = familyMap.get(entry.getKey());
      if (cells == null) {
        familyMap.put(entry.getKey(), entry.getValue());
      } else {
        cells.addAll(entry.getValue());
      }
    }
  }

  private void appendCurrentNonces(final Mutation mutation, final boolean replay,
      final WALEdit walEdit, final long now, final long currentNonceGroup, final long currentNonce)
  throws IOException {
    if (walEdit.isEmpty()) return;
    if (!replay) throw new IOException("Multiple nonces per batch and not in replay");
    WALKey walKey = new WALKey(this.getRegionInfo().getEncodedNameAsBytes(),
        this.htableDescriptor.getTableName(), now, mutation.getClusterIds(),
        currentNonceGroup, currentNonce, mvcc, this.getReplicationScope());
    this.wal.append(this.getRegionInfo(), walKey, walEdit, true);
    // Complete the mvcc transaction started down in append else it will block others
    this.mvcc.complete(walKey.getWriteEntry());
  }

  private boolean checkBatchOp(BatchOperation<?> batchOp, final int lastIndexExclusive,
      final Map<byte[], List<Cell>>[] familyMaps, final long now)
  throws IOException {
    boolean skip = false;
    // Skip anything that "ran" already
    if (batchOp.retCodeDetails[lastIndexExclusive].getOperationStatusCode()
        != OperationStatusCode.NOT_RUN) {
      return true;
    }
    Mutation mutation = batchOp.getMutation(lastIndexExclusive);
    Map<byte[], List<Cell>> familyMap = mutation.getFamilyCellMap();
    // store the family map reference to allow for mutations
    familyMaps[lastIndexExclusive] = familyMap;

    try {
      checkAndPrepareMutation(mutation, batchOp.isInReplay(), familyMap, now);
    } catch (NoSuchColumnFamilyException nscf) {
      LOG.warn("No such column family in batch mutation", nscf);
      batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
          OperationStatusCode.BAD_FAMILY, nscf.getMessage());
      skip = true;
    } catch (FailedSanityCheckException fsce) {
      LOG.warn("Batch Mutation did not pass sanity check", fsce);
      batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
          OperationStatusCode.SANITY_CHECK_FAILURE, fsce.getMessage());
      skip = true;
    } catch (WrongRegionException we) {
      LOG.warn("Batch mutation had a row that does not belong to this region", we);
      batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
          OperationStatusCode.SANITY_CHECK_FAILURE, we.getMessage());
      skip = true;
    }
    return skip;
  }

  private void checkAndPrepareMutation(Mutation mutation, boolean replay,
      final Map<byte[], List<Cell>> familyMap, final long now)
          throws IOException {
    if (mutation instanceof Put) {
      // Check the families in the put. If bad, skip this one.
      if (replay) {
        removeNonExistentColumnFamilyForReplay(familyMap);
      } else {
        checkFamilies(familyMap.keySet());
      }
      checkTimestamps(mutation.getFamilyCellMap(), now);
    } else {
      prepareDelete((Delete)mutation);
    }
    checkRow(mutation.getRow(), "doMiniBatchMutation");
  }

  /**
   * During replay, there could exist column families which are removed between region server
   * failure and replay
   */
  private void removeNonExistentColumnFamilyForReplay(final Map<byte[], List<Cell>> familyMap) {
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

  /**
   * Returns effective durability from the passed durability and
   * the table descriptor.
   */
  protected Durability getEffectiveDurability(Durability d) {
    return d == Durability.USE_DEFAULT ? this.durability : d;
  }

  @Override
  public boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, Mutation mutation,
      boolean writeToWAL)
  throws IOException{
    checkMutationType(mutation, row);
    return doCheckAndRowMutate(row, family, qualifier, compareOp, comparator, null,
      mutation, writeToWAL);
  }

  @Override
  public boolean checkAndRowMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, RowMutations rm,
      boolean writeToWAL)
  throws IOException {
    return doCheckAndRowMutate(row, family, qualifier, compareOp, comparator, rm, null,
      writeToWAL);
  }

  /**
   * checkAndMutate and checkAndRowMutate are 90% the same. Rather than copy/paste, below has
   * switches in the few places where there is deviation.
   */
  private boolean doCheckAndRowMutate(byte [] row, byte [] family, byte [] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator, RowMutations rowMutations,
      Mutation mutation, boolean writeToWAL)
  throws IOException {
    // Could do the below checks but seems wacky with two callers only. Just comment out for now.
    // One caller passes a Mutation, the other passes RowMutation. Presume all good so we don't
    // need these commented out checks.
    // if (rowMutations == null && mutation == null) throw new DoNotRetryIOException("Both null");
    // if (rowMutations != null && mutation != null) throw new DoNotRetryIOException("Both set");
    checkReadOnly();
    // TODO, add check for value length also move this check to the client
    checkResources();
    startRegionOperation();
    try {
      Get get = new Get(row);
      checkFamily(family);
      get.addColumn(family, qualifier);
      // Lock row - note that doBatchMutate will relock this row if called
      checkRow(row, "doCheckAndRowMutate");
      RowLock rowLock = getRowLockInternal(get.getRow(), false);
      try {
        if (mutation != null && this.getCoprocessorHost() != null) {
          // Call coprocessor.
          Boolean processed = null;
          if (mutation instanceof Put) {
            processed = this.getCoprocessorHost().preCheckAndPutAfterRowLock(row, family,
                qualifier, compareOp, comparator, (Put)mutation);
          } else if (mutation instanceof Delete) {
            processed = this.getCoprocessorHost().preCheckAndDeleteAfterRowLock(row, family,
                qualifier, compareOp, comparator, (Delete)mutation);
          }
          if (processed != null) {
            return processed;
          }
        }
        // NOTE: We used to wait here until mvcc caught up:  mvcc.await();
        // Supposition is that now all changes are done under row locks, then when we go to read,
        // we'll get the latest on this row.
        List<Cell> result = get(get, false);
        boolean valueIsNull = comparator.getValue() == null || comparator.getValue().length == 0;
        boolean matches = false;
        long cellTs = 0;
        if (result.size() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 && valueIsNull) {
          matches = true;
          cellTs = result.get(0).getTimestamp();
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          cellTs = kv.getTimestamp();
          int compareResult = CellComparator.compareValue(kv, comparator);
          matches = matches(compareOp, compareResult);
        }
        // If matches put the new put or delete the new delete
        if (matches) {
          // We have acquired the row lock already. If the system clock is NOT monotonically
          // non-decreasing (see HBASE-14070) we should make sure that the mutation has a
          // larger timestamp than what was observed via Get. doBatchMutate already does this, but
          // there is no way to pass the cellTs. See HBASE-14054.
          long now = EnvironmentEdgeManager.currentTime();
          long ts = Math.max(now, cellTs); // ensure write is not eclipsed
          byte[] byteTs = Bytes.toBytes(ts);
          if (mutation != null) {
            if (mutation instanceof Put) {
              updateCellTimestamps(mutation.getFamilyCellMap().values(), byteTs);
            }
            // And else 'delete' is not needed since it already does a second get, and sets the
            // timestamp from get (see prepareDeleteTimestamps).
          } else {
            for (Mutation m: rowMutations.getMutations()) {
              if (m instanceof Put) {
                updateCellTimestamps(m.getFamilyCellMap().values(), byteTs);
              }
            }
            // And else 'delete' is not needed since it already does a second get, and sets the
            // timestamp from get (see prepareDeleteTimestamps).
          }
          // All edits for the given row (across all column families) must happen atomically.
          if (mutation != null) {
            doBatchMutate(mutation);
          } else {
            mutateRow(rowMutations);
          }
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

  private void checkMutationType(final Mutation mutation, final byte [] row)
  throws DoNotRetryIOException {
    boolean isPut = mutation instanceof Put;
    if (!isPut && !(mutation instanceof Delete)) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action must be Put or Delete");
    }
    if (!Bytes.equals(row, mutation.getRow())) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action's getRow must match");
    }
  }

  private boolean matches(final CompareOp compareOp, final int compareResult) {
    boolean matches = false;
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
    return matches;
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

    // The regionserver holding the first region of the table is responsible for taking the
    // manifest of the mob dir.
    if (!Bytes.equals(getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW))
      return;

    // if any cf's have is mob enabled, add the "mob region" to the manifest.
    List<Store> stores = getStores();
    for (Store store : stores) {
      boolean hasMobStore = store.getFamily().isMobEnabled();
      if (hasMobStore) {
        // use the .mob as the start key and 0 as the regionid
        HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(this.getTableDesc().getTableName());
        mobRegionInfo.setOffline(true);
        manifest.addMobRegion(mobRegionInfo, this.getTableDesc().getColumnFamilies());
        return;
      }
    }
  }

  private void updateSequenceId(final Iterable<List<Cell>> cellItr, final long sequenceId)
      throws IOException {
    for (List<Cell> cells: cellItr) {
      if (cells == null) return;
      for (Cell cell : cells) {
        CellUtil.setSequenceId(cell, sequenceId);
      }
    }
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
        List<Tag> newTags = TagUtil.carryForwardTags(null, cell);
        newTags = TagUtil.carryForwardTTLTag(newTags, m.getTTL());
        // Rewrite the cell with the updated set of tags
        cells.set(i, CellUtil.createCell(cell, newTags));
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

    if (this.memstoreDataSize.get() > this.blockingMemStoreSize) {
      blockedRequestsCount.increment();
      requestFlush();
      throw new RegionTooBusyException("Above memstore limit, " +
          "regionName=" + (this.getRegionInfo() == null ? "unknown" :
          this.getRegionInfo().getRegionNameAsString()) +
          ", server=" + (this.getRegionServerServices() == null ? "unknown" :
          this.getRegionServerServices().getServerName()) +
          ", memstoreSize=" + memstoreDataSize.get() +
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

  /*
   * Atomically apply the given map of family->edits to the memstore.
   * This handles the consistency control on its own, but the caller
   * should already have locked updatesLock.readLock(). This also does
   * <b>not</b> check the families for validity.
   *
   * @param familyMap Map of Cells by family
   * @param memstoreSize
   */
  private void applyFamilyMapToMemstore(Map<byte[], List<Cell>> familyMap,
      MemstoreSize memstoreSize) throws IOException {
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;
      applyToMemstore(getStore(family), cells, false, memstoreSize);
    }
  }

  /*
   * @param delta If we are doing delta changes -- e.g. increment/append -- then this flag will be
   *  set; when set we will run operations that make sense in the increment/append scenario but
   *  that do not make sense otherwise.
   * @see #applyToMemstore(Store, Cell, long)
   */
  private void applyToMemstore(final Store store, final List<Cell> cells, final boolean delta,
      MemstoreSize memstoreSize) throws IOException {
    // Any change in how we update Store/MemStore needs to also be done in other applyToMemstore!!!!
    boolean upsert = delta && store.getFamily().getMaxVersions() == 1;
    if (upsert) {
      ((HStore) store).upsert(cells, getSmallestReadPoint(), memstoreSize);
    } else {
      ((HStore) store).add(cells, memstoreSize);
    }
  }

  /*
   * @see #applyToMemstore(Store, List, boolean, boolean, long)
   */
  private void applyToMemstore(final Store store, final Cell cell, MemstoreSize memstoreSize)
  throws IOException {
    // Any change in how we update Store/MemStore needs to also be done in other applyToMemstore!!!!
    if (store == null) {
      checkFamily(CellUtil.cloneFamily(cell));
      // Unreachable because checkFamily will throw exception
    }
    ((HStore) store).add(cell, memstoreSize);
  }

  @Override
  public void checkFamilies(Collection<byte[]> families) throws NoSuchColumnFamilyException {
    for (byte[] family : families) {
      checkFamily(family);
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

  private void requestFlushIfNeeded(long memstoreTotalSize) throws RegionTooBusyException {
    if(memstoreTotalSize > this.getMemstoreFlushSize()) {
      requestFlush();
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
      HStore store = null;
      boolean reported_once = false;
      ServerNonceManager ng = this.rsServices == null ? null : this.rsServices.getNonceManager();

      try {
        // How many edits seen before we check elapsed time
        int interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
        // How often to send a progress report (default 1/2 master timeout)
        int period = this.conf.getInt("hbase.hstore.report.period", 300000);
        long lastReport = EnvironmentEdgeManager.currentTime();

        if (coprocessorHost != null) {
          coprocessorHost.preReplayWALs(this.getRegionInfo(), edits);
        }

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
          boolean checkRowWithinBoundary = false;
          // Check this edit is for this region.
          if (!Bytes.equals(key.getEncodedRegionName(),
              this.getRegionInfo().getEncodedNameAsBytes())) {
            checkRowWithinBoundary = true;
          }

          boolean flush = false;
          MemstoreSize memstoreSize = new MemstoreSize();
          for (Cell cell: val.getCells()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
              // if region names don't match, skipp replaying compaction marker
              if (!checkRowWithinBoundary) {
                //this is a special edit, we should handle it
                CompactionDescriptor compaction = WALEdit.getCompaction(cell);
                if (compaction != null) {
                  //replay the compaction
                  replayWALCompactionMarker(compaction, false, true, Long.MAX_VALUE);
                }
              }
              skippedEdits++;
              continue;
            }
            // Figure which store the edit is meant for.
            if (store == null || !CellUtil.matchingFamily(cell, store.getFamily().getName())) {
              store = getHStore(cell);
            }
            if (store == null) {
              // This should never happen.  Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for " + cell);
              skippedEdits++;
              continue;
            }
            if (checkRowWithinBoundary && !rowIsInRange(this.getRegionInfo(),
              cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())) {
              LOG.warn("Row of " + cell + " is not within region boundary");
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

            restoreEdit(store, cell, memstoreSize);
            editsCount++;
          }
          if (this.rsAccounting != null) {
            rsAccounting.addRegionReplayEditsSize(getRegionInfo().getRegionName(),
                memstoreSize);
          }
          flush = isFlushSize(this.addAndGetMemstoreSize(memstoreSize));
          if (flush) {
            internalFlushcache(null, currentEditSeqId, stores.values(), status, false);
          }

          if (coprocessorHost != null) {
            coprocessorHost.postWALRestore(this.getRegionInfo(), key, val);
          }
        }

        if (coprocessorHost != null) {
          coprocessorHost.postReplayWALs(this.getRegionInfo(), edits);
        }
      } catch (EOFException eof) {
        Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
        msg = "EnLongAddered EOF. Most likely due to Master failure during " +
            "wal splitting, so we have this data in another edit.  " +
            "Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
          msg = "File corruption enLongAddered!  " +
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
    try {
      checkTargetRegion(compaction.getEncodedRegionName().toByteArray(),
        "Compaction marker from WAL ", compaction);
    } catch (WrongRegionException wre) {
      if (RegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
        // skip the compaction marker since it is not for this region
        return;
      }
      throw wre;
    }

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
        HStore store = this.getHStore(compaction.getFamilyName().toByteArray());
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
        if (numMutationsWithoutWAL.sum() > 0) {
          numMutationsWithoutWAL.reset();
          dataInMemoryWithoutWAL.reset();
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
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
    justification="Intentional; post memstore flush")
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
            this.decrMemstoreSize(prepareFlushResult.totalFlushableSize);

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
            this.decrMemstoreSize(prepareFlushResult.totalFlushableSize);

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
  private MemstoreSize dropMemstoreContentsForSeqId(long seqId, Store store) throws IOException {
    MemstoreSize totalFreedSize = new MemstoreSize();
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
            totalFreedSize.incMemstoreSize(doDropStoreMemstoreContentsForSeqId(s, currentSeqId));
          }
        } else {
          totalFreedSize.incMemstoreSize(doDropStoreMemstoreContentsForSeqId(store, currentSeqId));
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

  private MemstoreSize doDropStoreMemstoreContentsForSeqId(Store s, long currentSeqId)
      throws IOException {
    MemstoreSize flushableSize = s.getSizeToFlush();
    this.decrMemstoreSize(flushableSize);
    StoreFlushContext ctx = s.createFlushContext(currentSeqId);
    ctx.prepare();
    ctx.abort();
    return flushableSize;
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Intentional; cleared the memstore")
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
                MemstoreSize snapshotSize = store.getSizeToFlush();
                ctx.abort();
                this.decrMemstoreSize(snapshotSize);
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
          HStore store = getHStore(family);
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
          if (store.getSizeOfSnapshot().getDataSize() > 0) {
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Notify is about post replay. Intentional")
  protected boolean refreshStoreFiles(boolean force) throws IOException {
    if (!force && ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return false; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : "
          + "Refreshing store files to see whether we can free up memstore");
    }

    long totalFreedDataSize = 0;

    long smallestSeqIdInStores = Long.MAX_VALUE;

    startRegionOperation(); // obtain region close lock
    try {
      Map<Store, Long> map = new HashMap<Store, Long>();
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
                  MemstoreSize snapshotSize = store.getSizeToFlush();
                  ctx.abort();
                  this.decrMemstoreSize(snapshotSize);
                  this.prepareFlushResult.storeFlushCtxs.remove(store.getFamily().getName());
                  totalFreedDataSize += snapshotSize.getDataSize();
                }
              }
            }

            map.put(store, storeSeqId);
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
      if (!map.isEmpty()) {
        if (!force) {
          for (Map.Entry<Store, Long> entry : map.entrySet()) {
            // Drop the memstore contents if they are now smaller than the latest seen flushed file
            totalFreedDataSize += dropMemstoreContentsForSeqId(entry.getValue(), entry.getKey())
                .getDataSize();
          }
        } else {
          synchronized (storeSeqIds) {
            // don't try to acquire write lock of updatesLock now
            storeSeqIds.add(map);
          }
        }
      }
      // C. Finally notify anyone waiting on memstore to clear:
      // e.g. checkResources().
      synchronized (this) {
        notifyAll(); // FindBugs NN_NAKED_NOTIFY
      }
      return totalFreedDataSize > 0;
    } finally {
      closeRegionOperation();
    }
  }

  private void logRegionFiles() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(getRegionInfo().getEncodedName() + " : Store files for region: ");
      for (Store s : stores.values()) {
        Collection<StoreFile> storeFiles = s.getStorefiles();
        if (storeFiles == null) continue;
        for (StoreFile sf : storeFiles) {
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

  /*
   * Used by tests
   * @param s Store to add edit too.
   * @param cell Cell to add.
   * @param memstoreSize
   */
  protected void restoreEdit(final HStore s, final Cell cell, MemstoreSize memstoreSize) {
    s.add(cell, memstoreSize);
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
    if (family.isMobEnabled()) {
      if (HFile.getFormatVersion(this.conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
        throw new IOException("A minimum HFile version of "
            + HFile.MIN_FORMAT_VERSION_WITH_TAGS
            + " is required for MOB feature. Consider setting " + HFile.FORMAT_VERSION_KEY
            + " accordingly.");
      }
      return new HMobStore(this, family, this.conf);
    }
    return new HStore(this, family, this.conf);
  }

  @Override
  public Store getStore(final byte[] column) {
    return getHStore(column);
  }

  public HStore getHStore(final byte[] column) {
    return (HStore) this.stores.get(column);
  }

  /**
   * Return HStore instance. Does not do any copy: as the number of store is limited, we
   *  iterate on the list.
   */
  private HStore getHStore(Cell cell) {
    for (Map.Entry<byte[], Store> famStore : stores.entrySet()) {
      if (CellUtil.matchingFamily(cell, famStore.getKey(), 0, famStore.getKey().length)) {
        return (HStore) famStore.getValue();
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
        Collection<StoreFile> storeFiles = store.getStorefiles();
        if (storeFiles == null) continue;
        for (StoreFile storeFile: storeFiles) {
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

  @Override
  public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
    checkRow(row, "row lock");
    return getRowLockInternal(row, readLock);
  }

  protected RowLock getRowLockInternal(byte[] row, boolean readLock) throws IOException {
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
        rowLockContext = computeIfAbsent(lockedRows, rowKey, () -> new RowLockContext(rowKey));
        // Now try an get the lock.
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
        throw new IOException("Timed out waiting for lock for row: " + rowKey + " in region "
            + getRegionInfo().getEncodedName());
      }
      rowLockContext.setThreadName(Thread.currentThread().getName());
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
      for (int i = 0; i < rowLocks.size(); i++) {
        rowLocks.get(i).release();
      }
      rowLocks.clear();
    }
  }

  @VisibleForTesting
  public int getReadLockCount() {
    return lock.getReadLockCount();
  }

  public ConcurrentHashMap<HashedBytes, RowLockContext> getLockedRows() {
    return lockedRows;
  }

  @VisibleForTesting
  class RowLockContext {
    private final HashedBytes row;
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    final AtomicBoolean usable = new AtomicBoolean(true);
    final AtomicInteger count = new AtomicInteger(0);
    final Object lock = new Object();
    private String threadName;

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
          if (count.get() <= 0){
            usable.set(false);
            RowLockContext removed = lockedRows.remove(row);
            assert removed == this: "we should never remove a different context";
          }
        }
      }
    }

    public void setThreadName(String threadName) {
      this.threadName = threadName;
    }

    @Override
    public String toString() {
      return "RowLockContext{" +
          "row=" + row +
          ", readWriteLock=" + readWriteLock +
          ", count=" + count +
          ", threadName=" + threadName +
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
   * @param familyPaths List of (column family, hfilePath)
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
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths, boolean assignSeqId,
      BulkLoadListener bulkLoadListener) throws IOException {
    return bulkLoadHFiles(familyPaths, assignSeqId, bulkLoadListener, false);
  }

  @Override
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths,
      boolean assignSeqId, BulkLoadListener bulkLoadListener, boolean copyFile) throws IOException {
    long seqId = -1;
    Map<byte[], List<Path>> storeFiles = new TreeMap<byte[], List<Path>>(Bytes.BYTES_COMPARATOR);
    Map<String, Long> storeFilesSizes = new HashMap<String, Long>();
    Preconditions.checkNotNull(familyPaths);
    // we need writeLock for multi-family bulk load
    startBulkRegionOperation(hasMultipleColumnFamilies(familyPaths));
    boolean isSuccessful = false;
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

        HStore store = getHStore(familyName);
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
        return null;
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
        HStore store = getHStore(familyName);
        try {
          String finalPath = path;
          if (bulkLoadListener != null) {
            finalPath = bulkLoadListener.prepareBulkLoad(familyName, path, copyFile);
          }
          Path commitedStoreFile = store.bulkLoadHFile(finalPath, seqId);

          // Note the size of the store file
          try {
            FileSystem fs = commitedStoreFile.getFileSystem(baseConf);
            storeFilesSizes.put(commitedStoreFile.getName(), fs.getFileStatus(commitedStoreFile)
                .getLen());
          } catch (IOException e) {
            LOG.warn("Failed to find the size of hfile " + commitedStoreFile);
            storeFilesSizes.put(commitedStoreFile.getName(), 0L);
          }

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

      isSuccessful = true;
    } finally {
      if (wal != null && !storeFiles.isEmpty()) {
        // Write a bulk load event for hfiles that are loaded
        try {
          WALProtos.BulkLoadDescriptor loadDescriptor =
              ProtobufUtil.toBulkLoadDescriptor(this.getRegionInfo().getTable(),
                  UnsafeByteOperations.unsafeWrap(this.getRegionInfo().getEncodedNameAsBytes()),
                  storeFiles,
                storeFilesSizes, seqId);
          WALUtil.writeBulkLoadMarkerAndSync(this.wal, this.getReplicationScope(), getRegionInfo(),
              loadDescriptor, mvcc);
        } catch (IOException ioe) {
          if (this.rsServices != null) {
            // Have to abort region server because some hfiles has been loaded but we can't write
            // the event into WAL
            isSuccessful = false;
            this.rsServices.abort("Failed to write bulk load event into WAL.", ioe);
          }
        }
      }

      closeBulkRegionOperation();
    }
    return isSuccessful ? storeFiles : null;
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
  class RegionScannerImpl implements RegionScanner, org.apache.hadoop.hbase.ipc.RpcCallback {
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
    protected final CellComparator comparator;

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
      this(scan, additionalScanners, region, HConstants.NO_NONCE, HConstants.NO_NONCE);
    }

    RegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region,
        long nonceGroup, long nonce) throws IOException {
      this.region = region;
      this.maxResultSize = scan.getMaxResultSize();
      if (scan.hasFilter()) {
        this.filter = new FilterWrapper(scan.getFilter());
      } else {
        this.filter = null;
      }
      this.comparator = region.getCellCompartor();
      /**
       * By default, calls to next/nextRaw must enforce the batch limit. Thus, construct a default
       * scanner context that can be used to enforce the batch limit in the event that a
       * ScannerContext is not specified during an invocation of next/nextRaw
       */
      defaultScannerContext = ScannerContext.newBuilder()
          .setBatchLimit(scan.getBatch()).build();

      if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) && !scan.isGetScan()) {
        this.stopRow = null;
      } else {
        this.stopRow = scan.getStopRow();
      }
      // If we are doing a get, we want to be [startRow,endRow]. Normally
      // it is [startRow,endRow) and if startRow=endRow we get nothing.
      this.isScan = scan.isGetScan() ? 1 : 0;

      // synchronize on scannerReadPoints so that nobody calculates
      // getSmallestReadPoint, before scannerReadPoints is updated.
      IsolationLevel isolationLevel = scan.getIsolationLevel();
      long mvccReadPoint = PackagePrivateFieldAccessor.getMvccReadPoint(scan);
      synchronized (scannerReadPoints) {
        if (mvccReadPoint > 0) {
          this.readPt = mvccReadPoint;
        } else if (nonce == HConstants.NO_NONCE || rsServices == null
            || rsServices.getNonceManager() == null) {
          this.readPt = getReadPoint(isolationLevel);
        } else {
          this.readPt = rsServices.getNonceManager().getMvccFromOperationContext(nonceGroup, nonce);
        }
        scannerReadPoints.put(this, this.readPt);
      }
      initializeScanners(scan, additionalScanners);
    }

    protected void initializeScanners(Scan scan, List<KeyValueScanner> additionalScanners)
        throws IOException {
      // Here we separate all scanners into two lists - scanner that provide data required
      // by the filter to operate (scanners list) and all others (joinedScanners list).
      List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>(scan.getFamilyMap().size());
      List<KeyValueScanner> joinedScanners =
          new ArrayList<KeyValueScanner>(scan.getFamilyMap().size());
      // Store all already instantiated scanners for exception handling
      List<KeyValueScanner> instantiatedScanners = new ArrayList<KeyValueScanner>();
      // handle additionalScanners
      if (additionalScanners != null && !additionalScanners.isEmpty()) {
        scanners.addAll(additionalScanners);
        instantiatedScanners.addAll(additionalScanners);
      }

      try {
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
          Store store = stores.get(entry.getKey());
          KeyValueScanner scanner;
          try {
            scanner = store.getScanner(scan, entry.getValue(), this.readPt);
          } catch (FileNotFoundException e) {
            throw handleFileNotFound(e);
          }
          instantiatedScanners.add(scanner);
          if (this.filter == null || !scan.doLoadColumnFamiliesOnDemand()
              || this.filter.isFamilyEssential(entry.getKey())) {
            scanners.add(scanner);
          } else {
            joinedScanners.add(scanner);
          }
        }
        initializeKVHeap(scanners, joinedScanners, region);
      } catch (Throwable t) {
        throw handleException(instantiatedScanners, t);
      }
    }

    protected void initializeKVHeap(List<KeyValueScanner> scanners,
        List<KeyValueScanner> joinedScanners, HRegion region)
        throws IOException {
      this.storeHeap = new KeyValueHeap(scanners, comparator);
      if (!joinedScanners.isEmpty()) {
        this.joinedHeap = new KeyValueHeap(joinedScanners, comparator);
      }
    }

    private IOException handleException(List<KeyValueScanner> instantiatedScanners,
        Throwable t) {
      // remove scaner read point before throw the exception
      scannerReadPoints.remove(this);
      if (storeHeap != null) {
        storeHeap.close();
        storeHeap = null;
        if (joinedHeap != null) {
          joinedHeap.close();
          joinedHeap = null;
        }
      } else {
        // close all already instantiated scanners before throwing the exception
        for (KeyValueScanner scanner : instantiatedScanners) {
          scanner.close();
        }
      }
      return t instanceof IOException ? (IOException) t : new IOException(t);
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
      boolean moreValues = false;
      if (outResults.isEmpty()) {
        // Usually outResults is empty. This is true when next is called
        // to handle scan or get operation.
        moreValues = nextInternal(outResults, scannerContext);
      } else {
        List<Cell> tmpList = new ArrayList<Cell>();
        moreValues = nextInternal(tmpList, scannerContext);
        outResults.addAll(tmpList);
      }

      // If the size limit was reached it means a partial Result is being
      // returned. Returning a
      // partial Result means that we should not reset the filters; filters
      // should only be reset in
      // between rows
      if (!scannerContext.midRowResultFormed())
        resetFilters();

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
      boolean moreValues = populateResult(results, this.joinedHeap, scannerContext,
          joinedContinuationRow);

      if (!scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
        // We are done with this row, reset the continuation.
        joinedContinuationRow = null;
      }
      // As the data is obtained from two independent heaps, we need to
      // ensure that result list is sorted, because Result relies on that.
      sort(results, comparator);
      return moreValues;
    }

    /**
     * Fetches records with currentRow into results list, until next row, batchLimit (if not -1) is
     * reached, or remainingResultSize (if not -1) is reaced
     * @param heap KeyValueHeap to fetch data from.It must be positioned on correct row before call.
     * @param scannerContext
     * @param currentRowCell
     * @return state of last call to {@link KeyValueHeap#next()}
     */
    private boolean populateResult(List<Cell> results, KeyValueHeap heap,
        ScannerContext scannerContext, Cell currentRowCell) throws IOException {
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
          moreCellsInRow = moreCellsInRow(nextKv, currentRowCell);
          if (!moreCellsInRow) incrementCountOfRowsScannedMetric(scannerContext);
          if (moreCellsInRow && scannerContext.checkBatchLimit(limitScope)) {
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
     * @param currentRowCell
     * @return true When there are more cells in the row to be read
     */
    private boolean moreCellsInRow(final Cell nextKv, Cell currentRowCell) {
      return nextKv != null && CellUtil.matchingRow(nextKv, currentRowCell);
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

        boolean stopRow = isStopRow(current);
        // When has filter row is true it means that the all the cells for a particular row must be
        // read before a filtering decision can be made. This means that filters where hasFilterRow
        // run the risk of enLongAddering out of memory errors in the case that they are applied to a
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
          if (filterRowKey(current)) {
            incrementCountOfRowsFilteredMetric(scannerContext);
            // early check, see HBASE-16296
            if (isFilterDoneInternal()) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            // Typically the count of rows scanned is incremented inside #populateResult. However,
            // here we are filtering a row based purely on its row key, preventing us from calling
            // #populateResult. Thus, perform the necessary increment here to rows scanned metric
            incrementCountOfRowsScannedMetric(scannerContext);
            boolean moreRows = nextRow(scannerContext, current);
            if (!moreRows) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            results.clear();
            continue;
          }

          // Ok, we are good, let's try to get some results from the main heap.
          populateResult(results, this.storeHeap, scannerContext, current);

          if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
            if (hasFilterRow) {
              throw new IncompatibleFilterException(
                  "Filter whose hasFilterRow() returns true is incompatible with scans that must "
                      + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
            }
            return true;
          }

          Cell nextKv = this.storeHeap.peek();
          stopRow = nextKv == null || isStopRow(nextKv);
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
              scannerContext.incrementSizeProgress(CellUtil.estimatedHeapSizeOf(cell));
            }
          }

          if (isEmptyRow || ret == FilterWrapper.FilterRowRetCode.EXCLUDE || filterRow()) {
            incrementCountOfRowsFilteredMetric(scannerContext);
            results.clear();
            boolean moreRows = nextRow(scannerContext, current);
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
            boolean mayHaveData = joinedHeapMayHaveData(current);
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
          boolean moreRows = nextRow(scannerContext, current);
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
      filteredReadRequestsCount.increment();

      if (scannerContext == null || !scannerContext.isTrackingMetrics()) return;

      scannerContext.getMetrics().countOfRowsFiltered.incrementAndGet();
    }

    protected void incrementCountOfRowsScannedMetric(ScannerContext scannerContext) {
      if (scannerContext == null || !scannerContext.isTrackingMetrics()) return;

      scannerContext.getMetrics().countOfRowsScanned.incrementAndGet();
    }

    /**
     * @param currentRowCell
     * @return true when the joined heap may have data for the current row
     * @throws IOException
     */
    private boolean joinedHeapMayHaveData(Cell currentRowCell)
        throws IOException {
      Cell nextJoinedKv = joinedHeap.peek();
      boolean matchCurrentRow =
          nextJoinedKv != null && CellUtil.matchingRow(nextJoinedKv, currentRowCell);
      boolean matchAfterSeek = false;

      // If the next value in the joined heap does not match the current row, try to seek to the
      // correct row
      if (!matchCurrentRow) {
        Cell firstOnCurrentRow = CellUtil.createFirstOnRow(currentRowCell);
        boolean seekSuccessful = this.joinedHeap.requestSeek(firstOnCurrentRow, true, true);
        matchAfterSeek =
            seekSuccessful && joinedHeap.peek() != null
                && CellUtil.matchingRow(joinedHeap.peek(), currentRowCell);
      }

      return matchCurrentRow || matchAfterSeek;
    }

    /**
     * This function is to maintain backward compatibility for 0.94 filters. HBASE-6429 combines
     * both filterRow & filterRow({@code List<KeyValue> kvs}) functions. While 0.94 code or older,
     * it may not implement hasFilterRow as HBase-6429 expects because 0.94 hasFilterRow() only
     * returns true when filterRow({@code List<KeyValue> kvs}) is overridden not the filterRow().
     * Therefore, the filterRow() will be skipped.
     */
    private boolean filterRow() throws IOException {
      // when hasFilterRow returns true, filter.filterRow() will be called automatically inside
      // filterRowCells(List<Cell> kvs) so we skip that scenario here.
      return filter != null && (!filter.hasFilterRow())
          && filter.filterRow();
    }

    private boolean filterRowKey(Cell current) throws IOException {
      return filter != null && filter.filterRowKey(current);
    }

    protected boolean nextRow(ScannerContext scannerContext, Cell curRowCell) throws IOException {
      assert this.joinedContinuationRow == null: "Trying to go to next row during joinedHeap read.";
      Cell next;
      while ((next = this.storeHeap.peek()) != null &&
             CellUtil.matchingRow(next, curRowCell)) {
        this.storeHeap.next(MOCKED_LIST);
      }
      resetFilters();

      // Calling the hook in CP which allows it to do a fast forward
      return this.region.getCoprocessorHost() == null
          || this.region.getCoprocessorHost()
              .postScannerFilterRow(this, curRowCell);
    }

    protected boolean isStopRow(Cell currentRowCell) {
      return currentRowCell == null
          || (stopRow != null && comparator.compareRows(currentRowCell, stopRow, 0, stopRow
          .length) >= isScan);
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
      Cell kv = CellUtil.createFirstOnRow(row, 0, (short) row.length);
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
        LOG.error("unable to refresh store files", e);
        abortRegionServer(msg);
        return new NotServingRegionException(
          getRegionInfo().getRegionNameAsString() + " is closing");
      }
    }

    private void abortRegionServer(String msg) throws IOException {
      if (rsServices instanceof HRegionServer) {
        ((HRegionServer)rsServices).abort(msg);
      }
      throw new UnsupportedOperationException("not able to abort RS after: " + msg);
    }

    @Override
    public void shipped() throws IOException {
      if (storeHeap != null) {
        storeHeap.shipped();
      }
      if (joinedHeap != null) {
        joinedHeap.shipped();
      }
    }

    @Override
    public void run() throws IOException {
      // This is the RPC callback method executed. We do the close in of the scanner in this
      // callback
      this.close();
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
   * Convenience method creating new HRegions. Used by createTable.
   *
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @return new HRegion
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
        final Configuration conf, final HTableDescriptor hTableDescriptor,
        final WAL wal, final boolean initialize)
  throws IOException {
    LOG.info("creating HRegion " + info.getTable().getNameAsString()
        + " HTD == " + hTableDescriptor + " RootDir = " + rootDir +
        " Table name == " + info.getTable().getNameAsString());
    FileSystem fs = FileSystem.get(conf);
    Path tableDir = FSUtils.getTableDir(rootDir, info.getTable());
    HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, info);
    HRegion region = HRegion.newHRegion(tableDir, wal, fs, conf, info, hTableDescriptor, null);
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

  @VisibleForTesting
  public NavigableMap<byte[], Integer> getReplicationScope() {
    return this.replicationScope;
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

    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, null);
    r.initializeWarmup(reporter);
  }


  private void checkCompressionCodecs() throws IOException {
    for (HColumnDescriptor fam: this.htableDescriptor.getColumnFamilies()) {
      CompressionTest.testCompression(fam.getCompressionType());
      CompressionTest.testCompression(fam.getCompactionCompressionType());
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
  public HRegion createDaughterRegionFromSplits(final HRegionInfo hri) throws IOException {
    // Move the files from the temporary .splits to the final /table/region directory
    fs.commitDaughterRegion(hri);

    // Create the daughter HRegion instance
    HRegion r = HRegion.newHRegion(this.fs.getTableDir(), this.getWAL(), fs.getFileSystem(),
        this.getBaseConf(), hri, this.getTableDesc(), rsServices);
    r.readRequestsCount.add(this.getReadRequestsCount() / 2);
    r.filteredReadRequestsCount.add(this.getFilteredReadRequestsCount() / 2);
    r.writeRequestsCount.add(this.getWriteRequestsCount() / 2);
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
    r.readRequestsCount.add(this.getReadRequestsCount()
        + region_b.getReadRequestsCount());
    r.filteredReadRequestsCount.add(this.getFilteredReadRequestsCount()
      + region_b.getFilteredReadRequestsCount());
    r.writeRequestsCount.add(this.getWriteRequestsCount()

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

  public static boolean rowIsInRange(HRegionInfo info, final byte [] row, final int offset,
      final short length) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), 0, info.getStartKey().length,
          row, offset, length) <= 0)) &&
        ((info.getEndKey().length == 0) ||
          (Bytes.compareTo(info.getEndKey(), 0, info.getEndKey().length, row, offset, length) > 0));
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

    // clear the compacted files if any
    for (Store s : dstRegion.getStores()) {
      s.closeAndArchiveCompactedFiles();
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
    prepareGet(get);
    List<Cell> results = get(get, true);
    boolean stale = this.getRegionInfo().getReplicaId() != 0;
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  void prepareGet(final Get get) throws IOException, NoSuchColumnFamilyException {
    checkRow(get.getRow(), "Get");
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte[] family : get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family : this.htableDescriptor.getFamiliesKeys()) {
        get.addFamily(family);
      }
    }
  }

  @Override
  public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
    return get(get, withCoprocessor, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  @Override
  public List<Cell> get(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
      throws IOException {
    List<Cell> results = new ArrayList<Cell>();

    // pre-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      if (coprocessorHost.preGet(get, results)) {
        return results;
      }
    }
    long before =  EnvironmentEdgeManager.currentTime();
    Scan scan = new Scan(get);
    if (scan.getLoadColumnFamiliesOnDemandValue() == null) {
      scan.setLoadColumnFamiliesOnDemand(isLoadingCfsOnDemandDefault());
    }
    RegionScanner scanner = null;
    try {
      scanner = getScanner(scan, null, nonceGroup, nonce);
      scanner.next(results);
    } finally {
      if (scanner != null)
        scanner.close();
    }

    // post-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      coprocessorHost.postGet(get, results);
    }

    metricsUpdateForGet(results, before);

    return results;
  }

  void metricsUpdateForGet(List<Cell> results, long before) {
    if (this.metricsRegion != null) {
      this.metricsRegion.updateGet(EnvironmentEdgeManager.currentTime() - before);
    }
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
   * @return statistics about the current load of the region
   */
  public ClientProtos.RegionLoadStats getLoadStatistics() {
    if (!regionStatsEnabled) {
      return null;
    }
    ClientProtos.RegionLoadStats.Builder stats = ClientProtos.RegionLoadStats.newBuilder();
    stats.setMemstoreLoad((int) (Math.min(100, (this.memstoreDataSize.get() * 100) / this
        .memstoreFlushSize)));
    if (rsServices.getHeapMemoryManager() != null) {
      stats.setHeapOccupancy(
          (int) rsServices.getHeapMemoryManager().getHeapOccupancyPercent() * 100);
    }
    stats.setCompactionPressure((int)rsServices.getCompactionPressure()*100 > 100 ? 100 :
                (int)rsServices.getCompactionPressure()*100);
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

    // STEP 1. Run pre-process hook
    preProcess(processor, walEdit);
    // Short circuit the read only case
    if (processor.readOnly()) {
      try {
        long now = EnvironmentEdgeManager.currentTime();
        doProcessRowWithTimeout(processor, now, this, null, null, timeout);
        processor.postProcess(this, walEdit, true);
      } finally {
        closeRegionOperation();
      }
      return;
    }

    boolean locked = false;
    List<RowLock> acquiredRowLocks = null;
    List<Mutation> mutations = new ArrayList<Mutation>();
    Collection<byte[]> rowsToLock = processor.getRowsToLock();
    // This is assigned by mvcc either explicity in the below or in the guts of the WAL append
    // when it assigns the edit a sequencedid (A.K.A the mvcc write number).
    WriteEntry writeEntry = null;
    MemstoreSize memstoreSize = new MemstoreSize();
    try {
      boolean success = false;
      try {
        // STEP 2. Acquire the row lock(s)
        acquiredRowLocks = new ArrayList<>(rowsToLock.size());
        for (byte[] row : rowsToLock) {
          // Attempt to lock all involved rows, throw if any lock times out
          // use a writer lock for mixed reads and writes
          acquiredRowLocks.add(getRowLockInternal(row, false));
        }
        // STEP 3. Region lock
        lock(this.updatesLock.readLock(), acquiredRowLocks.isEmpty() ? 1 : acquiredRowLocks.size());
        locked = true;
        long now = EnvironmentEdgeManager.currentTime();
        // STEP 4. Let the processor scan the rows, generate mutations and add waledits
        doProcessRowWithTimeout(processor, now, this, mutations, walEdit, timeout);
        if (!mutations.isEmpty()) {
          // STEP 5. Call the preBatchMutate hook
          processor.preBatchMutate(this, walEdit);

          // STEP 6. Append and sync if walEdit has data to write out.
          if (!walEdit.isEmpty()) {
            writeEntry = doWALAppend(walEdit, getEffectiveDurability(processor.useDurability()),
                processor.getClusterIds(), now, nonceGroup, nonce);
          } else {
            // We are here if WAL is being skipped.
            writeEntry = this.mvcc.begin();
          }

          // STEP 7. Apply to memstore
          long sequenceId = writeEntry.getWriteNumber();
          for (Mutation m : mutations) {
            // Handle any tag based cell features.
            // TODO: Do we need to call rewriteCellTags down in applyToMemstore()? Why not before
            // so tags go into WAL?
            rewriteCellTags(m.getFamilyCellMap(), m);
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              if (walEdit.isEmpty()) {
                // If walEdit is empty, we put nothing in WAL. WAL stamps Cells with sequence id.
                // If no WAL, need to stamp it here.
                CellUtil.setSequenceId(cell, sequenceId);
              }
              applyToMemstore(getHStore(cell), cell, memstoreSize);
            }
          }

          // STEP 8. call postBatchMutate hook
          processor.postBatchMutate(this);

          // STEP 9. Complete mvcc.
          mvcc.completeAndWait(writeEntry);
          writeEntry = null;

          // STEP 10. Release region lock
          if (locked) {
            this.updatesLock.readLock().unlock();
            locked = false;
          }

          // STEP 11. Release row lock(s)
          releaseRowLocks(acquiredRowLocks);
        }
        success = true;
      } finally {
        // Call complete rather than completeAndWait because we probably had error if walKey != null
        if (writeEntry != null) mvcc.complete(writeEntry);
        if (locked) {
          this.updatesLock.readLock().unlock();
        }
        // release locks if some were acquired but another timed out
        releaseRowLocks(acquiredRowLocks);
      }

      // 12. Run post-process hook
      processor.postProcess(this, walEdit, success);
    } finally {
      closeRegionOperation();
      if (!mutations.isEmpty()) {
        long newSize = this.addAndGetMemstoreSize(memstoreSize);
        requestFlushIfNeeded(newSize);
      }
    }
  }

  private void preProcess(final RowProcessor<?,?> processor, final WALEdit walEdit)
  throws IOException {
    try {
      processor.preProcess(this, walEdit);
    } catch (IOException e) {
      closeRegionOperation();
      throw e;
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

  @Override
  public Result append(Append mutation, long nonceGroup, long nonce) throws IOException {
    return doDelta(Operation.APPEND, mutation, nonceGroup, nonce, mutation.isReturnResults());
  }

  public Result increment(Increment increment) throws IOException {
    return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  // dropMemstoreContentsForSeqId() would acquire write lock of updatesLock
  // We perform this operation outside of the read lock of updatesLock to avoid dead lock
  // See HBASE-16304
  @SuppressWarnings("unchecked")
  private void dropMemstoreContents() throws IOException {
    MemstoreSize totalFreedSize = new MemstoreSize();
    while (!storeSeqIds.isEmpty()) {
      Map<Store, Long> map = null;
      synchronized (storeSeqIds) {
        if (storeSeqIds.isEmpty()) break;
        map = storeSeqIds.remove(storeSeqIds.size()-1);
      }
      for (Map.Entry<Store, Long> entry : map.entrySet()) {
        // Drop the memstore contents if they are now smaller than the latest seen flushed file
        totalFreedSize
            .incMemstoreSize(dropMemstoreContentsForSeqId(entry.getValue(), entry.getKey()));
      }
    }
    if (totalFreedSize.getDataSize() > 0) {
      LOG.debug("Freed " + totalFreedSize.getDataSize() + " bytes from memstore");
    }
  }

  @Override
  public Result increment(Increment mutation, long nonceGroup, long nonce)
  throws IOException {
    return doDelta(Operation.INCREMENT, mutation, nonceGroup, nonce, mutation.isReturnResults());
  }

  /**
   * Add "deltas" to Cells. Deltas are increments or appends. Switch on <code>op</code>.
   *
   * <p>If increment, add deltas to current values or if an append, then
   * append the deltas to the current Cell values.
   *
   * <p>Append and Increment code paths are mostly the same. They differ in just a few places.
   * This method does the code path for increment and append and then in key spots, switches
   * on the passed in <code>op</code> to do increment or append specific paths.
   */
  private Result doDelta(Operation op, Mutation mutation, long nonceGroup, long nonce,
      boolean returnResults) throws IOException {
    checkReadOnly();
    checkResources();
    checkRow(mutation.getRow(), op.toString());
    checkFamilies(mutation.getFamilyCellMap().keySet());
    this.writeRequestsCount.increment();
    WriteEntry writeEntry = null;
    startRegionOperation(op);
    List<Cell> results = returnResults? new ArrayList<Cell>(mutation.size()): null;
    RowLock rowLock = null;
    MemstoreSize memstoreSize = new MemstoreSize();
    try {
      rowLock = getRowLockInternal(mutation.getRow(), false);
      lock(this.updatesLock.readLock());
      try {
        Result cpResult = doCoprocessorPreCall(op, mutation);
        if (cpResult != null) {
          return returnResults? cpResult: null;
        }
        Durability effectiveDurability = getEffectiveDurability(mutation.getDurability());
        Map<Store, List<Cell>> forMemStore =
            new HashMap<Store, List<Cell>>(mutation.getFamilyCellMap().size());
        // Reckon Cells to apply to WAL --  in returned walEdit -- and what to add to memstore and
        // what to return back to the client (in 'forMemStore' and 'results' respectively).
        WALEdit walEdit = reckonDeltas(op, mutation, effectiveDurability, forMemStore, results);
        // Actually write to WAL now if a walEdit to apply.
        if (walEdit != null && !walEdit.isEmpty()) {
          writeEntry = doWALAppend(walEdit, effectiveDurability, nonceGroup, nonce);
        } else {
          // If walEdits is empty, it means we skipped the WAL; update LongAdders and start an mvcc
          // transaction.
          recordMutationWithoutWal(mutation.getFamilyCellMap());
          writeEntry = mvcc.begin();
          updateSequenceId(forMemStore.values(), writeEntry.getWriteNumber());
        }
        // Now write to MemStore. Do it a column family at a time.
        for (Map.Entry<Store, List<Cell>> e : forMemStore.entrySet()) {
          applyToMemstore(e.getKey(), e.getValue(), true, memstoreSize);
        }
        mvcc.completeAndWait(writeEntry);
        if (rsServices != null && rsServices.getNonceManager() != null) {
          rsServices.getNonceManager().addMvccToOperationContext(nonceGroup, nonce,
            writeEntry.getWriteNumber());
        }
        writeEntry = null;
      } finally {
        this.updatesLock.readLock().unlock();
        // For increment/append, a region scanner for doing a get operation could throw
        // FileNotFoundException. So we call dropMemstoreContents() in finally block
        // after releasing read lock
        dropMemstoreContents();
      }
      // If results is null, then client asked that we not return the calculated results.
      return results != null && returnResults? Result.create(results): Result.EMPTY_RESULT;
    } finally {
      // Call complete always, even on success. doDelta is doing a Get READ_UNCOMMITTED when it goes
      // to get current value under an exclusive lock so no need so no need to wait to return to
      // the client. Means only way to read-your-own-increment or append is to come in with an
      // a 0 increment.
      if (writeEntry != null) mvcc.complete(writeEntry);
      if (rowLock != null) {
        rowLock.release();
      }
      // Request a cache flush if over the limit.  Do it outside update lock.
      if (isFlushSize(addAndGetMemstoreSize(memstoreSize))) {
        requestFlush();
      }
      closeRegionOperation(op);
      if (this.metricsRegion != null) {
        switch (op) {
          case INCREMENT:
            this.metricsRegion.updateIncrement();
            break;
          case APPEND:
            this.metricsRegion.updateAppend();
            break;
          default:
            break;
        }
      }
    }
  }

  private WriteEntry doWALAppend(WALEdit walEdit, Durability durability, long nonceGroup,
      long nonce)
  throws IOException {
    return doWALAppend(walEdit, durability, WALKey.EMPTY_UUIDS, System.currentTimeMillis(),
      nonceGroup, nonce);
  }

  /**
   * @return writeEntry associated with this append
   */
  private WriteEntry doWALAppend(WALEdit walEdit, Durability durability, List<UUID> clusterIds,
      long now, long nonceGroup, long nonce)
  throws IOException {
    WriteEntry writeEntry = null;
    // Using default cluster id, as this can only happen in the originating cluster.
    // A slave cluster receives the final value (not the delta) as a Put. We use HLogKey
    // here instead of WALKey directly to support legacy coprocessors.
    WALKey walKey = new WALKey(this.getRegionInfo().getEncodedNameAsBytes(),
      this.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now, clusterIds,
      nonceGroup, nonce, mvcc, this.getReplicationScope());
    try {
      long txid =
        this.wal.append(this.getRegionInfo(), walKey, walEdit, true);
      // Call sync on our edit.
      if (txid != 0) sync(txid, durability);
      writeEntry = walKey.getWriteEntry();
    } catch (IOException ioe) {
      if (walKey != null) mvcc.complete(walKey.getWriteEntry());
      throw ioe;
    }
    return writeEntry;
  }

  /**
   * Do coprocessor pre-increment or pre-append call.
   * @return Result returned out of the coprocessor, which means bypass all further processing and
   *  return the proffered Result instead, or null which means proceed.
   */
  private Result doCoprocessorPreCall(final Operation op, final Mutation mutation)
  throws IOException {
    Result result = null;
    if (this.coprocessorHost != null) {
      switch(op) {
        case INCREMENT:
          result = this.coprocessorHost.preIncrementAfterRowLock((Increment)mutation);
          break;
        case APPEND:
          result = this.coprocessorHost.preAppendAfterRowLock((Append)mutation);
          break;
        default: throw new UnsupportedOperationException(op.toString());
      }
    }
    return result;
  }

  /**
   * Reckon the Cells to apply to WAL, memstore, and to return to the Client; these Sets are not
   * always the same dependent on whether to write WAL or if the amount to increment is zero (in
   * this case we write back nothing, just return latest Cell value to the client).
   *
   * @param results Fill in here what goes back to the Client if it is non-null (if null, client
   *  doesn't want results).
   * @param forMemStore Fill in here what to apply to the MemStore (by Store).
   * @return A WALEdit to apply to WAL or null if we are to skip the WAL.
   */
  private WALEdit reckonDeltas(final Operation op, final Mutation mutation,
      final Durability effectiveDurability, final Map<Store, List<Cell>> forMemStore,
      final List<Cell> results)
  throws IOException {
    WALEdit walEdit = null;
    long now = EnvironmentEdgeManager.currentTime();
    final boolean writeToWAL = effectiveDurability != Durability.SKIP_WAL;
    // Process a Store/family at a time.
    for (Map.Entry<byte [], List<Cell>> entry: mutation.getFamilyCellMap().entrySet()) {
      final byte [] columnFamilyName = entry.getKey();
      List<Cell> deltas = entry.getValue();
      Store store = this.stores.get(columnFamilyName);
      // Reckon for the Store what to apply to WAL and MemStore.
      List<Cell> toApply =
        reckonDeltasByStore(store, op, mutation, effectiveDurability, now, deltas, results);
      if (!toApply.isEmpty()) {
        forMemStore.put(store, toApply);
        if (writeToWAL) {
          if (walEdit == null) {
            walEdit = new WALEdit();
          }
          walEdit.getCells().addAll(toApply);
        }
      }
    }
    return walEdit;
  }

  /**
   * Reckon the Cells to apply to WAL, memstore, and to return to the Client in passed
   * column family/Store.
   *
   * Does Get of current value and then adds passed in deltas for this Store returning the result.
   *
   * @param op Whether Increment or Append
   * @param mutation The encompassing Mutation object
   * @param deltas Changes to apply to this Store; either increment amount or data to append
   * @param results In here we accumulate all the Cells we are to return to the client; this List
   *  can be larger than what we return in case where delta is zero; i.e. don't write
   *  out new values, just return current value. If null, client doesn't want results returned.
   * @return Resulting Cells after <code>deltas</code> have been applied to current
   *  values. Side effect is our filling out of the <code>results</code> List.
   */
  private List<Cell> reckonDeltasByStore(final Store store, final Operation op,
      final Mutation mutation, final Durability effectiveDurability, final long now,
      final List<Cell> deltas, final List<Cell> results)
  throws IOException {
    byte [] columnFamily = store.getFamily().getName();
    List<Cell> toApply = new ArrayList<Cell>(deltas.size());
    // Get previous values for all columns in this family.
    List<Cell> currentValues = get(mutation, store, deltas,
        null/*Default IsolationLevel*/,
        op == Operation.INCREMENT? ((Increment)mutation).getTimeRange(): null);
    // Iterate the input columns and update existing values if they were found, otherwise
    // add new column initialized to the delta amount
    int currentValuesIndex = 0;
    for (int i = 0; i < deltas.size(); i++) {
      Cell delta = deltas.get(i);
      Cell currentValue = null;
      if (currentValuesIndex < currentValues.size() &&
          CellUtil.matchingQualifier(currentValues.get(currentValuesIndex), delta)) {
        currentValue = currentValues.get(currentValuesIndex);
        if (i < (deltas.size() - 1) && !CellUtil.matchingQualifier(delta, deltas.get(i + 1))) {
          currentValuesIndex++;
        }
      }
      // Switch on whether this an increment or an append building the new Cell to apply.
      Cell newCell = null;
      MutationType mutationType = null;
      boolean apply = true;
      switch (op) {
        case INCREMENT:
          mutationType = MutationType.INCREMENT;
          // If delta amount to apply is 0, don't write WAL or MemStore.
          long deltaAmount = getLongValue(delta);
          apply = deltaAmount != 0;
          newCell = reckonIncrement(delta, deltaAmount, currentValue, columnFamily, now,
            (Increment)mutation);
          break;
        case APPEND:
          mutationType = MutationType.APPEND;
          // Always apply Append. TODO: Does empty delta value mean reset Cell? It seems to.
          newCell = reckonAppend(delta, currentValue, now, (Append)mutation);
          break;
        default: throw new UnsupportedOperationException(op.toString());
      }

      // Give coprocessors a chance to update the new cell
      if (coprocessorHost != null) {
        newCell =
            coprocessorHost.postMutationBeforeWAL(mutationType, mutation, currentValue, newCell);
      }
      // If apply, we need to update memstore/WAL with new value; add it toApply.
      if (apply) {
        toApply.add(newCell);
      }
      // Add to results to get returned to the Client. If null, cilent does not want results.
      if (results != null) {
        results.add(newCell);
      }
    }
    return toApply;
  }

  /**
   * Calculate new Increment Cell.
   * @return New Increment Cell with delta applied to currentValue if currentValue is not null;
   *  otherwise, a new Cell with the delta set as its value.
   */
  private Cell reckonIncrement(final Cell delta, final long deltaAmount, final Cell currentValue,
      byte [] columnFamily, final long now, Mutation mutation)
  throws IOException {
    // Forward any tags found on the delta.
    List<Tag> tags = TagUtil.carryForwardTags(delta);
    long newValue = deltaAmount;
    long ts = now;
    if (currentValue != null) {
      tags = TagUtil.carryForwardTags(tags, currentValue);
      ts = Math.max(now, currentValue.getTimestamp() + 1);
      newValue += getLongValue(currentValue);
    }
    // Now make up the new Cell. TODO: FIX. This is carnel knowledge of how KeyValues are made...
    // doesn't work well with offheaping or if we are doing a different Cell type.
    byte [] incrementAmountInBytes = Bytes.toBytes(newValue);
    tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
    byte [] row = mutation.getRow();
    return new KeyValue(row, 0, row.length,
      columnFamily, 0, columnFamily.length,
      delta.getQualifierArray(), delta.getQualifierOffset(), delta.getQualifierLength(),
      ts, KeyValue.Type.Put,
      incrementAmountInBytes, 0, incrementAmountInBytes.length,
      tags);
  }

  private Cell reckonAppend(final Cell delta, final Cell currentValue, final long now,
      Append mutation)
  throws IOException {
    // Forward any tags found on the delta.
    List<Tag> tags = TagUtil.carryForwardTags(delta);
    long ts = now;
    Cell newCell = null;
    byte [] row = mutation.getRow();
    if (currentValue != null) {
      tags = TagUtil.carryForwardTags(tags, currentValue);
      ts = Math.max(now, currentValue.getTimestamp() + 1);
      tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
      byte[] tagBytes = TagUtil.fromList(tags);
      // Allocate an empty cell and copy in all parts.
      // TODO: This is intimate knowledge of how a KeyValue is made. Undo!!! Prevents our doing
      // other Cell types. Copying on-heap too if an off-heap Cell.
      newCell = new KeyValue(row.length, delta.getFamilyLength(),
        delta.getQualifierLength(), ts, KeyValue.Type.Put,
        delta.getValueLength() + currentValue.getValueLength(),
        tagBytes == null? 0: tagBytes.length);
      // Copy in row, family, and qualifier
      System.arraycopy(row, 0, newCell.getRowArray(), newCell.getRowOffset(), row.length);
      System.arraycopy(delta.getFamilyArray(), delta.getFamilyOffset(),
          newCell.getFamilyArray(), newCell.getFamilyOffset(), delta.getFamilyLength());
      System.arraycopy(delta.getQualifierArray(), delta.getQualifierOffset(),
          newCell.getQualifierArray(), newCell.getQualifierOffset(), delta.getQualifierLength());
      // Copy in the value
      CellUtil.copyValueTo(currentValue, newCell.getValueArray(), newCell.getValueOffset());
      System.arraycopy(delta.getValueArray(), delta.getValueOffset(),
          newCell.getValueArray(), newCell.getValueOffset() + currentValue.getValueLength(),
          delta.getValueLength());
      // Copy in tag data
      if (tagBytes != null) {
        System.arraycopy(tagBytes, 0,
            newCell.getTagsArray(), newCell.getTagsOffset(), tagBytes.length);
      }
    } else {
      // Append's KeyValue.Type==Put and ts==HConstants.LATEST_TIMESTAMP
      CellUtil.updateLatestStamp(delta, now);
      newCell = delta;
      tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
      if (tags != null) {
        newCell = CellUtil.createCell(delta, tags);
      }
    }
    return newCell;
  }

  /**
   * @return Get the long out of the passed in Cell
   */
  private static long getLongValue(final Cell cell) throws DoNotRetryIOException {
    int len = cell.getValueLength();
    if (len != Bytes.SIZEOF_LONG) {
      // throw DoNotRetryIOException instead of IllegalArgumentException
      throw new DoNotRetryIOException("Field is not a long, it's " + len + " bytes wide");
    }
    return CellUtil.getValueAsLong(cell);
  }

  /**
   * Do a specific Get on passed <code>columnFamily</code> and column qualifiers.
   * @param mutation Mutation we are doing this Get for.
   * @param store Which column family on row (TODO: Go all Gets in one go)
   * @param coordinates Cells from <code>mutation</code> used as coordinates applied to Get.
   * @return Return list of Cells found.
   */
  private List<Cell> get(final Mutation mutation, final Store store,
          final List<Cell> coordinates, final IsolationLevel isolation, final TimeRange tr)
  throws IOException {
    // Sort the cells so that they match the order that they appear in the Get results. Otherwise,
    // we won't be able to find the existing values if the cells are not specified in order by the
    // client since cells are in an array list.
    // TODO: I don't get why we are sorting. St.Ack 20150107
    sort(coordinates, store.getComparator());
    Get get = new Get(mutation.getRow());
    if (isolation != null) {
      get.setIsolationLevel(isolation);
    }
    for (Cell cell: coordinates) {
      get.addColumn(store.getFamily().getName(), CellUtil.cloneQualifier(cell));
    }
    // Increments carry time range. If an Increment instance, put it on the Get.
    if (tr != null) {
      get.setTimeRange(tr.getMin(), tr.getMax());
    }
    return get(get, false);
  }

  /**
   * @return Sorted list of <code>cells</code> using <code>comparator</code>
   */
  private static List<Cell> sort(List<Cell> cells, final Comparator<Cell> comparator) {
    Collections.sort(cells, comparator);
    return cells;
  }

  //
  // New HBASE-880 Helpers
  //

  void checkFamily(final byte [] family)
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
      50 * ClassSize.REFERENCE + 2 * Bytes.SIZEOF_INT +
      (14 * Bytes.SIZEOF_LONG) +
      6 * Bytes.SIZEOF_BOOLEAN);

  // woefully out of date - currently missing:
  // 1 x HashMap - coprocessorServiceHandlers
  // 6 x LongAdder - numMutationsWithoutWAL, dataInMemoryWithoutWAL,
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
      (4 * ClassSize.ATOMIC_LONG) + // memStoreSize, numPutsWithoutWAL, dataInMemoryWithoutWAL,
                                    // compactionsFailed
      (2 * ClassSize.CONCURRENT_HASHMAP) +  // lockedRows, scannerReadPoints
      WriteState.HEAP_SIZE + // writestate
      ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + // stores
      (2 * ClassSize.REENTRANT_LOCK) + // lock, updatesLock
      MultiVersionConcurrencyControl.FIXED_SIZE // mvcc
      + 2 * ClassSize.TREEMAP // maxSeqIdInStores, replicationScopes
      + 2 * ClassSize.ATOMIC_INTEGER // majorInProgress, minorInProgress
      + ClassSize.STORE_SERVICES // store services
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

  @Override
  public boolean registerService(com.google.protobuf.Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor service " + serviceName +
              " already registered, rejecting request from " + instance
      );
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered coprocessor service: region=" +
          Bytes.toStringBinary(getRegionInfo().getRegionName()) +
          " service=" + serviceName);
    }
    return true;
  }

  @Override
  public com.google.protobuf.Message execService(com.google.protobuf.RpcController controller,
      CoprocessorServiceCall call)
  throws IOException {
    String serviceName = call.getServiceName();
    com.google.protobuf.Service service = coprocessorServiceHandlers.get(serviceName);
    if (service == null) {
      throw new UnknownProtocolException(null, "No registered coprocessor service found for " +
          serviceName + " in region " + Bytes.toStringBinary(getRegionInfo().getRegionName()));
    }
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();

    String methodName = call.getMethodName();
    com.google.protobuf.Descriptors.MethodDescriptor methodDesc =
        CoprocessorRpcUtils.getMethodDescriptor(methodName, serviceDesc);

    com.google.protobuf.Message.Builder builder =
        service.getRequestPrototype(methodDesc).newBuilderForType();

    org.apache.hadoop.hbase.protobuf.ProtobufUtil.mergeFrom(builder,
        call.getRequest().toByteArray());
    com.google.protobuf.Message request =
        CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());

    if (coprocessorHost != null) {
      request = coprocessorHost.preEndpointInvocation(service, methodName, request);
    }

    final com.google.protobuf.Message.Builder responseBuilder =
        service.getResponsePrototype(methodDesc).newBuilderForType();
    service.callMethod(methodDesc, controller, request,
        new com.google.protobuf.RpcCallback<com.google.protobuf.Message>() {
      @Override
      public void run(com.google.protobuf.Message message) {
        if (message != null) {
          responseBuilder.mergeFrom(message);
        }
      }
    });

    if (coprocessorHost != null) {
      coprocessorHost.postEndpointInvocation(service, methodName, request, responseBuilder);
    }
    IOException exception =
        org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.getControllerException(controller);
    if (exception != null) {
      throw exception;
    }

    return responseBuilder.build();
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
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SF_SWITCH_FALLTHROUGH",
    justification="Intentional")
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
   * Update LongAdders for number of puts without wal and the size of possible data loss.
   * These information are exposed by the region server metrics.
   */
  private void recordMutationWithoutWal(final Map<byte [], List<Cell>> familyMap) {
    numMutationsWithoutWAL.increment();
    if (numMutationsWithoutWAL.sum() <= 1) {
      LOG.info("writing data to region " + this +
               " with WAL disabled. Data may be lost in the event of a crash.");
    }

    long mutationSize = 0;
    for (List<Cell> cells: familyMap.values()) {
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        mutationSize += KeyValueUtil.length(cell);
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
   * Calls sync with the given transaction ID
   * @param txid should sync up to which transaction
   * @throws IOException If anything goes wrong with DFS
   */
  private void sync(long txid, Durability durability) throws IOException {
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

  public void reportCompactionRequestFailure() {
    compactionsFailed.incrementAndGet();
  }

  @VisibleForTesting
  public long getReadPoint() {
    return getReadPoint(IsolationLevel.READ_COMMITTED);
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

  @Override
  public CellComparator getCellCompartor() {
    return this.getRegionInfo().isMetaRegion() ? CellComparator.META_COMPARATOR
        : CellComparator.COMPARATOR;
  }

  public long getMemstoreFlushSize() {
    return this.memstoreFlushSize;
  }

  //// method for debugging tests
  void throwException(String title, String regionName) {
    StringBuffer buf = new StringBuffer();
    buf.append(title + ", ");
    buf.append(getRegionInfo().toString());
    buf.append(getRegionInfo().isMetaRegion() ? " meta region " : " ");
    buf.append(getRegionInfo().isMetaTable() ? " meta table " : " ");
    buf.append("stores: ");
    for (Store s : getStores()) {
      buf.append(s.getFamily().getNameAsString());
      buf.append(" size: ");
      buf.append(s.getSizeOfMemStore().getDataSize());
      buf.append(" ");
    }
    buf.append("end-of-stores");
    buf.append(", memstore size ");
    buf.append(getMemstoreSize());
    if (getRegionInfo().getRegionNameAsString().startsWith(regionName)) {
      throw new RuntimeException(buf.toString());
    }
  }
}
