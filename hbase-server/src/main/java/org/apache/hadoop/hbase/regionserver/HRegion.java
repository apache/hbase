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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAJOR_COMPACTION_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.REGION_NAMES_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ROW_LOCK_READ_LOCK_KEY;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import com.google.errorprone.annotations.RestrictedApi;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.opentelemetry.api.trace.Span;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
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
import java.util.Objects;
import java.util.Optional;
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
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ReadOnlyConfiguration;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.ForbidMajorCompactionChecker;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationSink;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.StoreHotnessProtector;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationObserver;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.wal.WALSplitUtil.MutationReplay;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.ServiceDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * Regions store data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more Regions.
 *
 * <p>An Region is defined by its table and its key extent.
 *
 * <p>Locking at the Region level serves only one purpose: preventing the
 * region from being closed (and consequently split) while other operations
 * are ongoing. Each row level operation obtains both a row lock and a region
 * read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully
 * constructed, it holds a read lock until it is closed. A close takes out a
 * write lock and consequently will block for ongoing operations and will block
 * new operations from starting while the close is in progress.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Private
public class HRegion implements HeapSize, PropagatingConfigurationObserver, Region {
  private static final Logger LOG = LoggerFactory.getLogger(HRegion.class);

  public static final String LOAD_CFS_ON_DEMAND_CONFIG_KEY =
    "hbase.hregion.scan.loadColumnFamiliesOnDemand";

  public static final String HBASE_MAX_CELL_SIZE_KEY = "hbase.server.keyvalue.maxsize";
  public static final int DEFAULT_MAX_CELL_SIZE = 10485760;

  public static final String HBASE_REGIONSERVER_MINIBATCH_SIZE =
      "hbase.regionserver.minibatch.size";
  public static final int DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE = 20000;

  public static final String WAL_HSYNC_CONF_KEY = "hbase.wal.hsync";
  public static final boolean DEFAULT_WAL_HSYNC = false;

  /** Parameter name for compaction after bulkload */
  public static final String COMPACTION_AFTER_BULKLOAD_ENABLE =
      "hbase.compaction.after.bulkload.enable";

  /** Config for allow split when file count greater than the configured blocking file count*/
  public static final String SPLIT_IGNORE_BLOCKING_ENABLED_KEY =
      "hbase.hregion.split.ignore.blocking.enabled";

  /**
   * This is for for using HRegion as a local storage, where we may put the recovered edits in a
   * special place. Once this is set, we will only replay the recovered edits under this directory
   * and ignore the original replay directory configs.
   */
  public static final String SPECIAL_RECOVERED_EDITS_DIR =
    "hbase.hregion.special.recovered.edits.dir";

  /**
   * Whether to use {@link MetaCellComparator} even if we are not meta region. Used when creating
   * master local region.
   */
  public static final String USE_META_CELL_COMPARATOR = "hbase.region.use.meta.cell.comparator";

  public static final boolean DEFAULT_USE_META_CELL_COMPARATOR = false;

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
      new ConcurrentHashMap<>();

  protected final Map<byte[], HStore> stores =
      new ConcurrentSkipListMap<>(Bytes.BYTES_RAWCOMPARATOR);

  // TODO: account for each registered handler in HeapSize computation
  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // Track data size in all memstores
  private final MemStoreSizing memStoreSizing = new ThreadSafeMemStoreSizing();
  RegionServicesForStores regionServicesForStores;

  // Debug possible data loss due to WAL off
  final LongAdder numMutationsWithoutWAL = new LongAdder();
  final LongAdder dataInMemoryWithoutWAL = new LongAdder();

  // Debug why CAS operations are taking a while.
  final LongAdder checkAndMutateChecksPassed = new LongAdder();
  final LongAdder checkAndMutateChecksFailed = new LongAdder();

  // Number of requests
  // Count rows for scan
  final LongAdder readRequestsCount = new LongAdder();
  final LongAdder cpRequestsCount = new LongAdder();
  final LongAdder filteredReadRequestsCount = new LongAdder();
  // Count rows for multi row mutations
  final LongAdder writeRequestsCount = new LongAdder();

  // Number of requests blocked by memstore size.
  private final LongAdder blockedRequestsCount = new LongAdder();

  // Compaction LongAdders
  final LongAdder compactionsFinished = new LongAdder();
  final LongAdder compactionsFailed = new LongAdder();
  final LongAdder compactionNumFilesCompacted = new LongAdder();
  final LongAdder compactionNumBytesCompacted = new LongAdder();
  final LongAdder compactionsQueued = new LongAdder();
  final LongAdder flushesQueued = new LongAdder();

  private BlockCache blockCache;
  private MobFileCache mobFileCache;
  private final WAL wal;
  private final HRegionFileSystem fs;
  protected final Configuration conf;
  private final Configuration baseConf;
  private final int rowLockWaitDuration;
  static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

  private Path regionDir;
  private FileSystem walFS;

  // set to true if the region is restored from snapshot for reading by ClientSideRegionScanner
  private boolean isRestoredRegion = false;

  public void setRestoredRegion(boolean restoredRegion) {
    isRestoredRegion = restoredRegion;
  }

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

  // Max cell size. If nonzero, the maximum allowed size for any given cell
  // in bytes
  final long maxCellSize;

  // Number of mutations for minibatch processing.
  private final int miniBatchSize;

  final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

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
  Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  // lock used to protect the replay operation for secondary replicas, so the below two fields does
  // not need to be volatile.
  private Lock replayLock;

  /** Saved state from replaying prepare flush cache */
  private PrepareFlushResult prepareFlushResult = null;

  private long lastReplayedSequenceId = HConstants.NO_SEQNUM;

  private volatile ConfigurationManager configurationManager;

  // Used for testing.
  private volatile Long timeoutForWriteLock = null;

  private final CellComparator cellComparator;

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
  protected static class PrepareFlushResult {
    final FlushResultImpl result; // indicating a failure result from prepare
    final TreeMap<byte[], StoreFlushContext> storeFlushCtxs;
    final TreeMap<byte[], List<Path>> committedFiles;
    final TreeMap<byte[], MemStoreSize> storeFlushableSize;
    final long startTime;
    final long flushOpSeqId;
    final long flushedSeqId;
    final MemStoreSizing totalFlushableSize;

    /** Constructs an early exit case */
    PrepareFlushResult(FlushResultImpl result, long flushSeqId) {
      this(result, null, null, null, Math.max(0, flushSeqId), 0, 0, MemStoreSizing.DUD);
    }

    /** Constructs a successful prepare flush result */
    PrepareFlushResult(
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], MemStoreSize> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, MemStoreSizing totalFlushableSize) {
      this(null, storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
        flushSeqId, flushedSeqId, totalFlushableSize);
    }

    private PrepareFlushResult(
        FlushResultImpl result,
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], MemStoreSize> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, MemStoreSizing totalFlushableSize) {
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

  /**
   * A class that tracks exceptions that have been observed in one batch. Not thread safe.
   */
  static class ObservedExceptionsInBatch {
    private boolean wrongRegion = false;
    private boolean failedSanityCheck = false;
    private boolean wrongFamily = false;

    /**
     * @return If a {@link WrongRegionException} has been observed.
     */
    boolean hasSeenWrongRegion() {
      return wrongRegion;
    }

    /**
     * Records that a {@link WrongRegionException} has been observed.
     */
    void sawWrongRegion() {
      wrongRegion = true;
    }

    /**
     * @return If a {@link FailedSanityCheckException} has been observed.
     */
    boolean hasSeenFailedSanityCheck() {
      return failedSanityCheck;
    }

    /**
     * Records that a {@link FailedSanityCheckException} has been observed.
     */
    void sawFailedSanityCheck() {
      failedSanityCheck = true;
    }

    /**
     * @return If a {@link NoSuchColumnFamilyException} has been observed.
     */
    boolean hasSeenNoSuchFamily() {
      return wrongFamily;
    }

    /**
     * Records that a {@link NoSuchColumnFamilyException} has been observed.
     */
    void sawNoSuchFamily() {
      wrongFamily = true;
    }
  }

  final WriteState writestate = new WriteState();

  long memstoreFlushSize;
  final long timestampSlop;

  // Last flush time for each Store. Useful when we are flushing for each column
  private final ConcurrentMap<HStore, Long> lastStoreFlushTimeMap = new ConcurrentHashMap<>();

  protected RegionServerServices rsServices;
  private RegionServerAccounting rsAccounting;
  private long flushCheckInterval;
  // flushPerChanges is to prevent too many changes in memstore
  private long flushPerChanges;
  private long blockingMemStoreSize;
  // Used to guard closes
  final ReentrantReadWriteLock lock;
  // Used to track interruptible holders of the region lock. Currently that is only RPC handler
  // threads. Boolean value in map determines if lock holder can be interrupted, normally true,
  // but may be false when thread is transiting a critical section.
  final ConcurrentHashMap<Thread, Boolean> regionLockHolders;

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock = new ReentrantReadWriteLock();

  private final MultiVersionConcurrencyControl mvcc;

  // Coprocessor host
  private volatile RegionCoprocessorHost coprocessorHost;

  private TableDescriptor htableDescriptor = null;
  private RegionSplitPolicy splitPolicy;
  private RegionSplitRestriction splitRestriction;
  private FlushPolicy flushPolicy;

  private final MetricsRegion metricsRegion;
  private final MetricsRegionWrapperImpl metricsRegionWrapper;
  private final Durability regionDurability;
  private final boolean regionStatsEnabled;
  // Stores the replication scope of the various column families of the table
  // that has non-default scope
  private final NavigableMap<byte[], Integer> replicationScope = new TreeMap<>(
      Bytes.BYTES_COMPARATOR);

  private final StoreHotnessProtector storeHotnessProtector;

  protected Optional<RegionReplicationSink> regionReplicationSink = Optional.empty();

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
   * @param regionInfo - RegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param htd the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   * @deprecated Use other constructors.
   */
  @Deprecated
  public HRegion(final Path tableDir, final WAL wal, final FileSystem fs,
      final Configuration confParam, final RegionInfo regionInfo,
      final TableDescriptor htd, final RegionServerServices rsServices) {
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
      final TableDescriptor htd, final RegionServerServices rsServices) {
    if (htd == null) {
      throw new IllegalArgumentException("Need table descriptor");
    }

    if (confParam instanceof CompoundConfiguration) {
      throw new IllegalArgumentException("Need original base configuration");
    }

    this.wal = wal;
    this.fs = fs;
    this.mvcc = new MultiVersionConcurrencyControl(getRegionInfo().getShortNameToLog());

    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.baseConf = confParam;
    this.conf = new CompoundConfiguration().add(confParam).addBytesMap(htd.getValues());
    this.cellComparator = htd.isMetaTable() ||
      conf.getBoolean(USE_META_CELL_COMPARATOR, DEFAULT_USE_META_CELL_COMPARATOR) ?
        MetaCellComparator.META_COMPARATOR : CellComparatorImpl.COMPARATOR;
    this.lock = new ReentrantReadWriteLock(conf.getBoolean(FAIR_REENTRANT_CLOSE_LOCK,
        DEFAULT_FAIR_REENTRANT_CLOSE_LOCK));
    this.regionLockHolders = new ConcurrentHashMap<>();
    this.flushCheckInterval = conf.getInt(MEMSTORE_PERIODIC_FLUSH_INTERVAL,
        DEFAULT_CACHE_FLUSH_INTERVAL);
    this.flushPerChanges = conf.getLong(MEMSTORE_FLUSH_PER_CHANGES, DEFAULT_FLUSH_PER_CHANGES);
    if (this.flushPerChanges > MAX_FLUSH_PER_CHANGES) {
      throw new IllegalArgumentException(MEMSTORE_FLUSH_PER_CHANGES + " can not exceed "
          + MAX_FLUSH_PER_CHANGES);
    }
    int tmpRowLockDuration = conf.getInt("hbase.rowlock.wait.duration",
        DEFAULT_ROWLOCK_WAIT_DURATION);
    if (tmpRowLockDuration <= 0) {
      LOG.info("Found hbase.rowlock.wait.duration set to {}. values <= 0 will cause all row " +
          "locking to fail. Treating it as 1ms to avoid region failure.", tmpRowLockDuration);
      tmpRowLockDuration = 1;
    }
    this.rowLockWaitDuration = tmpRowLockDuration;

    this.isLoadingCfsOnDemandDefault = conf.getBoolean(LOAD_CFS_ON_DEMAND_CONFIG_KEY, true);
    this.htableDescriptor = htd;
    Set<byte[]> families = this.htableDescriptor.getColumnFamilyNames();
    for (byte[] family : families) {
      if (!replicationScope.containsKey(family)) {
        int scope = htd.getColumnFamily(family).getScope();
        // Only store those families that has NON-DEFAULT scope
        if (scope != REPLICATION_SCOPE_LOCAL) {
          // Do a copy before storing it here.
          replicationScope.put(Bytes.copy(family), scope);
        }
      }
    }

    this.rsServices = rsServices;
    if (this.rsServices != null) {
      this.blockCache = rsServices.getBlockCache().orElse(null);
      this.mobFileCache = rsServices.getMobFileCache().orElse(null);
    }
    this.regionServicesForStores = new RegionServicesForStores(this, rsServices);

    setHTableSpecificConf();
    this.scannerReadPoints = new ConcurrentHashMap<>();

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
     * assumes that you base your TS around EnvironmentEdgeManager.currentTime(). In this case,
     * throw an error to the user if the user-specified TS is newer than now +
     * slop. LATEST_TIMESTAMP == don't use this functionality
     */
    this.timestampSlop = conf.getLong(
        "hbase.hregion.keyvalue.timestamp.slop.millisecs",
        HConstants.LATEST_TIMESTAMP);

    this.storeHotnessProtector = new StoreHotnessProtector(this, conf);

    boolean forceSync = conf.getBoolean(WAL_HSYNC_CONF_KEY, DEFAULT_WAL_HSYNC);
    /**
     * This is the global default value for durability. All tables/mutations not defining a
     * durability or using USE_DEFAULT will default to this value.
     */
    Durability defaultDurability = forceSync ? Durability.FSYNC_WAL : Durability.SYNC_WAL;
    this.regionDurability =
        this.htableDescriptor.getDurability() == Durability.USE_DEFAULT ? defaultDurability :
          this.htableDescriptor.getDurability();

    decorateRegionConfiguration(conf);
    if (rsServices != null) {
      this.rsAccounting = this.rsServices.getRegionServerAccounting();
      // don't initialize coprocessors if not running within a regionserver
      // TODO: revisit if coprocessors should load in other cases
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
      this.metricsRegionWrapper = new MetricsRegionWrapperImpl(this);
      this.metricsRegion = new MetricsRegion(this.metricsRegionWrapper, conf);
    } else {
      this.metricsRegionWrapper = null;
      this.metricsRegion = null;
    }
    if (LOG.isDebugEnabled()) {
      // Write out region name, its encoded name and storeHotnessProtector as string.
      LOG.debug("Instantiated " + this +"; "+ storeHotnessProtector.toString());
    }

    configurationManager = null;

    // disable stats tracking system tables, but check the config for everything else
    this.regionStatsEnabled = htd.getTableName().getNamespaceAsString().equals(
        NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) ?
          false :
          conf.getBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE,
              HConstants.DEFAULT_ENABLE_CLIENT_BACKPRESSURE);

    this.maxCellSize = conf.getLong(HBASE_MAX_CELL_SIZE_KEY, DEFAULT_MAX_CELL_SIZE);
    this.miniBatchSize = conf.getInt(HBASE_REGIONSERVER_MINIBATCH_SIZE,
        DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE);

    // recover the metrics of read and write requests count if they were retained
    if (rsServices != null && rsServices.getRegionServerAccounting() != null) {
      Pair<Long, Long> retainedRWRequestsCnt = rsServices.getRegionServerAccounting()
        .getRetainedRegionRWRequestsCnt().get(getRegionInfo().getEncodedName());
      if (retainedRWRequestsCnt != null) {
        this.addReadRequestsCount(retainedRWRequestsCnt.getFirst());
        this.addWriteRequestsCount(retainedRWRequestsCnt.getSecond());
        // remove them since won't use again
        rsServices.getRegionServerAccounting().getRetainedRegionRWRequestsCnt()
          .remove(getRegionInfo().getEncodedName());
      }
    }
  }

  private void setHTableSpecificConf() {
    if (this.htableDescriptor == null) {
      return;
    }
    long flushSize = this.htableDescriptor.getMemStoreFlushSize();

    if (flushSize <= 0) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
    this.memstoreFlushSize = flushSize;
    long mult = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
      HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
    this.blockingMemStoreSize = this.memstoreFlushSize * mult;
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
  long initialize(final CancelableProgressable reporter) throws IOException {

    //Refuse to open the region if there is no column family in the table
    if (htableDescriptor.getColumnFamilyCount() == 0) {
      throw new DoNotRetryIOException("Table " + htableDescriptor.getTableName().getNameAsString()+
          " should have at least one column family.");
    }

    MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this);
    status.enableStatusJournal(true);
    long nextSeqId = -1;
    try {
      nextSeqId = initializeRegionInternals(reporter, status);
      return nextSeqId;
    } catch (IOException e) {
      LOG.warn("Failed initialize of region= {}, starting to roll back memstore",
          getRegionInfo().getRegionNameAsString(), e);
      // global memstore size will be decreased when dropping memstore
      try {
        //drop the memory used by memstore if open region fails
        dropMemStoreContents();
      } catch (IOException ioE) {
        if (conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
          LOG.warn("Failed drop memstore of region= {}, "
                  + "some chunks may not released forever since MSLAB is enabled",
              getRegionInfo().getRegionNameAsString());
        }

      }
      throw e;
    } finally {
      // nextSeqid will be -1 if the initialization fails.
      // At least it will be 0 otherwise.
      if (nextSeqId == -1) {
        status.abort("Exception during region " + getRegionInfo().getRegionNameAsString() +
          " initialization.");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region open journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      status.cleanup();
    }
  }

  private long initializeRegionInternals(final CancelableProgressable reporter,
      final MonitoredTask status) throws IOException {
    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-open hook");
      coprocessorHost.preOpen();
    }

    // Write HRI to a file in case we need to recover hbase:meta
    // Only the primary replica should write .regioninfo
    if (this.getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      status.setStatus("Writing region info on filesystem");
      fs.checkRegionInfoOnFilesystem();
    }

    // Initialize all the HStores
    status.setStatus("Initializing all the Stores");
    long maxSeqId = initializeStores(reporter, status);
    this.mvcc.advanceTo(maxSeqId);
    if (!isRestoredRegion && ServerRegionReplicaUtil.shouldReplayRecoveredEdits(this)) {
      Collection<HStore> stores = this.stores.values();
      try {
        // update the stores that we are replaying
        LOG.debug("replaying wal for " + this.getRegionInfo().getEncodedName());
        stores.forEach(HStore::startReplayingFromWAL);
        // Recover any edits if available.
        maxSeqId = Math.max(maxSeqId,
          replayRecoveredEditsIfAny(maxSeqIdInStores, reporter, status));
        // Recover any hfiles if available
        maxSeqId = Math.max(maxSeqId, loadRecoveredHFilesIfAny(stores));
        // Make sure mvcc is up to max.
        this.mvcc.advanceTo(maxSeqId);
      } finally {
        LOG.debug("stopping wal replay for " + this.getRegionInfo().getEncodedName());
        // update the stores that we are done replaying
        stores.forEach(HStore::stopReplayingFromWAL);
      }
    }
    this.lastReplayedOpenRegionSeqId = maxSeqId;

    this.writestate.setReadOnly(ServerRegionReplicaUtil.isReadOnly(this));
    this.writestate.flushRequested = false;
    this.writestate.compacting.set(0);

    if (this.writestate.writesEnabled) {
      LOG.debug("Cleaning up temporary data for " + this.getRegionInfo().getEncodedName());
      // Remove temporary data left over from old regions
      status.setStatus("Cleaning up temporary data from old regions");
      fs.cleanupTempDir();
    }

    // Initialize split policy
    this.splitPolicy = RegionSplitPolicy.create(this, conf);

    // Initialize split restriction
    splitRestriction = RegionSplitRestriction.create(getTableDescriptor(), conf);

    // Initialize flush policy
    this.flushPolicy = FlushPolicyFactory.create(this, conf);

    long lastFlushTime = EnvironmentEdgeManager.currentTime();
    for (HStore store: stores.values()) {
      this.lastStoreFlushTimeMap.put(store, lastFlushTime);
    }

    // Use maximum of log sequenceid or that which was found in stores
    // (particularly if no recovered edits, seqid will be -1).
    long nextSeqId = maxSeqId + 1;
    if (!isRestoredRegion) {
      // always get openSeqNum from the default replica, even if we are secondary replicas
      long maxSeqIdFromFile = WALSplitUtil.getMaxRegionSequenceId(conf,
        RegionReplicaUtil.getRegionInfoForDefaultReplica(getRegionInfo()), this::getFilesystem,
        this::getWalFileSystem);
      nextSeqId = Math.max(maxSeqId, maxSeqIdFromFile) + 1;
      // The openSeqNum will always be increase even for read only region, as we rely on it to
      // determine whether a region has been successfully reopened, so here we always need to update
      // the max sequence id file.
      if (RegionReplicaUtil.isDefaultReplica(getRegionInfo())) {
        LOG.debug("writing seq id for {}", this.getRegionInfo().getEncodedName());
        WALSplitUtil.writeRegionSequenceIdFile(getWalFileSystem(), getWALRegionDir(),
          nextSeqId - 1);
        // This means we have replayed all the recovered edits and also written out the max sequence
        // id file, let's delete the wrong directories introduced in HBASE-20734, see HBASE-22617
        // for more details.
        Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(conf,
          getRegionInfo().getTable(), getRegionInfo().getEncodedName());
        FileSystem walFs = getWalFileSystem();
        if (walFs.exists(wrongRegionWALDir)) {
          if (!walFs.delete(wrongRegionWALDir, true)) {
            LOG.debug("Failed to clean up wrong region WAL directory {}", wrongRegionWALDir);
          }
        }
      } else {
        lastReplayedSequenceId = nextSeqId - 1;
        replayLock = new ReentrantLock();
      }
      initializeRegionReplicationSink(reporter, status);
    }

    LOG.info("Opened {}; next sequenceid={}; {}, {}", this.getRegionInfo().getShortNameToLog(),
        nextSeqId, this.splitPolicy, this.flushPolicy);

    // A region can be reopened if failed a split; reset flags
    this.closing.set(false);
    this.closed.set(false);

    if (coprocessorHost != null) {
      LOG.debug("Running coprocessor post-open hooks for " + this.getRegionInfo().getEncodedName());
      status.setStatus("Running coprocessor post-open hooks");
      coprocessorHost.postOpen();
    }
    status.markComplete("Region opened successfully");
    return nextSeqId;
  }

  private void initializeRegionReplicationSink(CancelableProgressable reporter,
    MonitoredTask status) {
    RegionServerServices rss = getRegionServerServices();
    TableDescriptor td = getTableDescriptor();
    int regionReplication = td.getRegionReplication();
    RegionInfo regionInfo = getRegionInfo();
    if (regionReplication <= 1 || !RegionReplicaUtil.isDefaultReplica(regionInfo) ||
      !ServerRegionReplicaUtil.isRegionReplicaReplicationEnabled(conf, regionInfo.getTable()) ||
      rss == null) {
      regionReplicationSink = Optional.empty();
      return;
    }
    status.setStatus("Initializaing region replication sink");
    regionReplicationSink = Optional.of(new RegionReplicationSink(conf, regionInfo, td,
      rss.getRegionReplicationBufferManager(), () -> rss.getFlushRequester().requestFlush(this,
        new ArrayList<>(td.getColumnFamilyNames()), FlushLifeCycleTracker.DUMMY),
      rss.getAsyncClusterConnection()));
  }

  /**
   * Open all Stores.
   * @param reporter
   * @param status
   * @return Highest sequenceId found out in a Store.
   * @throws IOException
   */
  private long initializeStores(CancelableProgressable reporter, MonitoredTask status)
      throws IOException {
    return initializeStores(reporter, status, false);
  }

  private long initializeStores(CancelableProgressable reporter, MonitoredTask status,
      boolean warmup) throws IOException {
    // Load in all the HStores.
    long maxSeqId = -1;
    // initialized to -1 so that we pick up MemstoreTS from column families
    long maxMemstoreTS = -1;

    if (htableDescriptor.getColumnFamilyCount() != 0) {
      // initialize the thread pool for opening stores in parallel.
      ThreadPoolExecutor storeOpenerThreadPool =
        getStoreOpenAndCloseThreadPool("StoreOpener-" + this.getRegionInfo().getShortNameToLog());
      CompletionService<HStore> completionService = new ExecutorCompletionService<>(storeOpenerThreadPool);

      // initialize each store in parallel
      for (final ColumnFamilyDescriptor family : htableDescriptor.getColumnFamilies()) {
        status.setStatus("Instantiating store for column family " + family);
        completionService.submit(new Callable<HStore>() {
          @Override
          public HStore call() throws IOException {
            return instantiateHStore(family, warmup);
          }
        });
      }
      boolean allStoresOpened = false;
      boolean hasSloppyStores = false;
      try {
        for (int i = 0; i < htableDescriptor.getColumnFamilyCount(); i++) {
          Future<HStore> future = completionService.take();
          HStore store = future.get();
          this.stores.put(store.getColumnFamilyDescriptor().getName(), store);
          if (store.isSloppyMemStore()) {
            hasSloppyStores = true;
          }

          long storeMaxSequenceId = store.getMaxSequenceId().orElse(0L);
          maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()),
              storeMaxSequenceId);
          if (maxSeqId == -1 || storeMaxSequenceId > maxSeqId) {
            maxSeqId = storeMaxSequenceId;
          }
          long maxStoreMemstoreTS = store.getMaxMemStoreTS().orElse(0L);
          if (maxStoreMemstoreTS > maxMemstoreTS) {
            maxMemstoreTS = maxStoreMemstoreTS;
          }
        }
        allStoresOpened = true;
        if(hasSloppyStores) {
          htableDescriptor = TableDescriptorBuilder.newBuilder(htableDescriptor)
                  .setFlushPolicyClassName(FlushNonSloppyStoresFirstPolicy.class.getName())
                  .build();
          LOG.info("Setting FlushNonSloppyStoresFirstPolicy for the region=" + this);
        }
      } catch (InterruptedException e) {
        throw throwOnInterrupt(e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      } finally {
        storeOpenerThreadPool.shutdownNow();
        if (!allStoresOpened) {
          // something went wrong, close all opened stores
          LOG.error("Could not initialize all stores for the region=" + this);
          for (HStore store : this.stores.values()) {
            try {
              store.close();
            } catch (IOException e) {
              LOG.warn("close store {} failed in region {}", store.toString(), this, e);
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
    status.setStatus("Warmup all stores of " + this.getRegionInfo().getRegionNameAsString());
    try {
      initializeStores(reporter, status, true);
    } finally {
      status.markComplete("Warmed up " + this.getRegionInfo().getRegionNameAsString());
    }
  }

  /**
   * @return Map of StoreFiles by column family
   */
  private NavigableMap<byte[], List<Path>> getStoreFiles() {
    NavigableMap<byte[], List<Path>> allStoreFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (HStore store : stores.values()) {
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) {
        continue;
      }
      List<Path> storeFileNames = new ArrayList<>();
      for (HStoreFile storeFile : storeFiles) {
        storeFileNames.add(storeFile.getPath());
      }
      allStoreFiles.put(store.getColumnFamilyDescriptor().getName(), storeFileNames);
    }
    return allStoreFiles;
  }

  protected void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionOpenDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_OPEN, getRegionInfo(), openSeqId,
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionOpenDesc,
        mvcc, regionReplicationSink.orElse(null));
  }

  private void writeRegionCloseMarker(WAL wal) throws IOException {
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionEventDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_CLOSE, getRegionInfo(), mvcc.getReadPoint(),
      getRegionServerServices().getServerName(), storeFiles);
    // we do not care region close event at secondary replica side so just pass a null
    // RegionReplicationSink
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionEventDesc,
      mvcc, null);

    // Store SeqId in WAL FileSystem when a region closes
    // checking region folder exists is due to many tests which delete the table folder while a
    // table is still online
    if (getWalFileSystem().exists(getWALRegionDir())) {
      WALSplitUtil.writeRegionSequenceIdFile(getWalFileSystem(), getWALRegionDir(),
        mvcc.getReadPoint());
    }
  }

  /**
   * @return True if this region has references.
   */
  public boolean hasReferences() {
    return stores.values().stream().anyMatch(HStore::hasReferences);
  }

  public void blockUpdates() {
    this.updatesLock.writeLock().lock();
  }

  public void unblockUpdates() {
    this.updatesLock.writeLock().unlock();
  }

  public HDFSBlocksDistribution getHDFSBlocksDistribution() {
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    stores.values().stream().filter(s -> s.getStorefiles() != null)
        .flatMap(s -> s.getStorefiles().stream()).map(HStoreFile::getHDFSBlockDistribution)
        .forEachOrdered(hdfsBlocksDistribution::add);
    return hdfsBlocksDistribution;
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf configuration
   * @param tableDescriptor TableDescriptor of the table
   * @param regionInfo encoded name of the region
   * @return The HDFS blocks distribution for the given region.
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
    Path tablePath =
      CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), tableDescriptor.getTableName());
    return computeHDFSBlocksDistribution(conf, tableDescriptor, regionInfo, tablePath);
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf configuration
   * @param tableDescriptor TableDescriptor of the table
   * @param regionInfo encoded name of the region
   * @param tablePath the table directory
   * @return The HDFS blocks distribution for the given region.
   * @throws IOException
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
      TableDescriptor tableDescriptor, RegionInfo regionInfo, Path tablePath) throws IOException {
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    FileSystem fs = tablePath.getFileSystem(conf);

    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tablePath, regionInfo);
    for (ColumnFamilyDescriptor family : tableDescriptor.getColumnFamilies()) {
      List<LocatedFileStatus> locatedFileStatusList = HRegionFileSystem
          .getStoreFilesLocatedStatus(regionFs, family.getNameAsString(), true);
      if (locatedFileStatusList == null) {
        continue;
      }

      for (LocatedFileStatus status : locatedFileStatusList) {
        Path p = status.getPath();
        if (StoreFileInfo.isReference(p) || HFileLink.isHFileLink(p)) {
          // Only construct StoreFileInfo object if its not a hfile, save obj
          // creation
          StoreFileInfo storeFileInfo = new StoreFileInfo(conf, fs, status);
          hdfsBlocksDistribution.add(storeFileInfo
              .computeHDFSBlocksDistribution(fs));
        } else if (StoreFileInfo.isHFile(p)) {
          // If its a HFile, then lets just add to the block distribution
          // lets not create more objects here, not even another HDFSBlocksDistribution
          FSUtils.addToHDFSBlocksDistribution(hdfsBlocksDistribution,
              status.getBlockLocations());
        } else {
          throw new IOException("path=" + p
              + " doesn't look like a valid StoreFile");
        }
      }
    }
    return hdfsBlocksDistribution;
  }

  /**
   * Increase the size of mem store in this region and the size of global mem
   * store
   */
  private void incMemStoreSize(MemStoreSize mss) {
    incMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
  }

  void incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
    if (this.rsAccounting != null) {
      rsAccounting.incGlobalMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta);
    }
    long dataSize = this.memStoreSizing.incMemStoreSize(dataSizeDelta, heapSizeDelta,
      offHeapSizeDelta, cellsCountDelta);
    checkNegativeMemStoreDataSize(dataSize, dataSizeDelta);
  }

  void decrMemStoreSize(MemStoreSize mss) {
    decrMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
  }

  private void decrMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
    if (this.rsAccounting != null) {
      rsAccounting.decGlobalMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta);
    }
    long dataSize = this.memStoreSizing.decMemStoreSize(dataSizeDelta, heapSizeDelta,
      offHeapSizeDelta, cellsCountDelta);
    checkNegativeMemStoreDataSize(dataSize, -dataSizeDelta);
  }

  private void checkNegativeMemStoreDataSize(long memStoreDataSize, long delta) {
    // This is extremely bad if we make memStoreSizing negative. Log as much info on the offending
    // caller as possible. (memStoreSizing might be a negative value already -- freeing memory)
    if (memStoreDataSize < 0) {
      LOG.error("Asked to modify this region's (" + this.toString()
          + ") memStoreSizing to a negative value which is incorrect. Current memStoreSizing="
          + (memStoreDataSize - delta) + ", delta=" + delta, new Exception());
    }
  }

  @Override
  public RegionInfo getRegionInfo() {
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
  public long getCpRequestsCount() {
    return cpRequestsCount.sum();
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
  public long getMemStoreDataSize() {
    return memStoreSizing.getDataSize();
  }

  @Override
  public long getMemStoreHeapSize() {
    return memStoreSizing.getHeapSize();
  }

  @Override
  public long getMemStoreOffHeapSize() {
    return memStoreSizing.getOffHeapSize();
  }

  /** @return store services for this region, to access services required by store level needs */
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

  // TODO Needs to check whether we should expose our metrics system to CPs. If CPs themselves doing
  // the op and bypassing the core, this might be needed? Should be stop supporting the bypass
  // feature?
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

  @Override
  public boolean isAvailable() {
    return !isClosed() && !isClosing();
  }

  @Override
  public boolean isSplittable() {
    return splitPolicy.canSplit();
  }

  @Override
  public boolean isMergeable() {
    if (!isAvailable()) {
      LOG.debug("Region " + this
          + " is not mergeable because it is closing or closed");
      return false;
    }
    if (hasReferences()) {
      LOG.debug("Region " + this
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

  /**
   * @return readpoint considering given IsolationLevel. Pass {@code null} for default
   */
  public long getReadPoint(IsolationLevel isolationLevel) {
    if (isolationLevel != null && isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
      // This scan can read even uncommitted transactions
      return Long.MAX_VALUE;
    }
    return mvcc.getReadPoint();
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
   * HStores make use of.  It's a list of all StoreFile objects. Returns empty
   * vector if already closed and null if judged that it should not close.
   *
   * @throws IOException e
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
   */
  public Map<byte[], List<HStoreFile>> close() throws IOException {
    return close(false);
  }

  private final Object closeLock = new Object();

  /** Conf key for fair locking policy */
  public static final String FAIR_REENTRANT_CLOSE_LOCK =
      "hbase.regionserver.fair.region.close.lock";
  public static final boolean DEFAULT_FAIR_REENTRANT_CLOSE_LOCK = true;
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

  public static final String CLOSE_WAIT_ABORT = "hbase.regionserver.close.wait.abort";
  public static final boolean DEFAULT_CLOSE_WAIT_ABORT = true;
  public static final String CLOSE_WAIT_TIME = "hbase.regionserver.close.wait.time.ms";
  public static final long DEFAULT_CLOSE_WAIT_TIME = 60000;     // 1 minute
  public static final String CLOSE_WAIT_INTERVAL = "hbase.regionserver.close.wait.interval.ms";
  public static final long DEFAULT_CLOSE_WAIT_INTERVAL = 10000; // 10 seconds

  public Map<byte[], List<HStoreFile>> close(boolean abort) throws IOException {
    return close(abort, false);
  }

  /**
   * Close down this HRegion.  Flush the cache unless abort parameter is true,
   * Shut down each HStore, don't service any more calls.
   *
   * This method could take some time to execute, so don't call it from a
   * time-sensitive thread.
   *
   * @param abort true if server is aborting (only during testing)
   * @param ignoreStatus true if ignore the status (wont be showed on task list)
   * @return Vector of all the storage files that the HRegion's component
   * HStores make use of.  It's a list of StoreFile objects.  Can be null if
   * we are not to close at this time or we are already closed.
   *
   * @throws IOException e
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
   */
  public Map<byte[], List<HStoreFile>> close(boolean abort, boolean ignoreStatus)
      throws IOException {
    // Only allow one thread to close at a time. Serialize them so dual
    // threads attempting to close will run up against each other.
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Closing region " + this.getRegionInfo().getEncodedName() +
        (abort ? " due to abort" : ""), ignoreStatus);
    status.enableStatusJournal(true);
    status.setStatus("Waiting for close lock");
    try {
      synchronized (closeLock) {
        return doClose(abort, status);
      }
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region close journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      status.cleanup();
    }
  }

  /**
   * Exposed for some very specific unit tests.
   */
  public void setClosing(boolean closing) {
    this.closing.set(closing);
  }

  /**
   * The {@link HRegion#doClose} will block forever if someone tries proving the dead lock via the unit test.
   * Instead of blocking, the {@link HRegion#doClose} will throw exception if you set the timeout.
   * @param timeoutForWriteLock the second time to wait for the write lock in {@link HRegion#doClose}
   */
  public void setTimeoutForWriteLock(long timeoutForWriteLock) {
    assert timeoutForWriteLock >= 0;
    this.timeoutForWriteLock = timeoutForWriteLock;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK_EXCEPTION_PATH",
      justification="I think FindBugs is confused")
  private Map<byte[], List<HStoreFile>> doClose(boolean abort, MonitoredTask status)
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
      LOG.debug("Closing {}, disabling compactions & flushes",
          this.getRegionInfo().getEncodedName());
      waitForFlushesAndCompactions();
    }
    // If we were not just flushing, is it worth doing a preflush...one
    // that will clear out of the bulk of the memstore before we put up
    // the close flag?
    if (!abort && worthPreFlushing() && canFlush) {
      status.setStatus("Pre-flushing region before close");
      LOG.info("Running close preflush of {}", this.getRegionInfo().getEncodedName());
      try {
        internalFlushcache(status);
      } catch (IOException ioe) {
        // Failed to flush the region. Keep going.
        status.setStatus("Failed pre-flush " + this + "; " + ioe.getMessage());
      }
    }
    if (regionReplicationSink.isPresent()) {
      // stop replicating to secondary replicas
      // the open event marker can make secondary replicas refresh store files and catch up
      // everything, so here we just give up replicating later edits, to speed up the reopen process
      RegionReplicationSink sink = regionReplicationSink.get();
      sink.stop();
      try {
        regionReplicationSink.get().waitUntilStopped();
      } catch (InterruptedException e) {
        throw throwOnInterrupt(e);
      }
    }
    // Set the closing flag
    // From this point new arrivals at the region lock will get NSRE.

    this.closing.set(true);
    LOG.info("Closing region {}", this);

    // Acquire the close lock

    // The configuration parameter CLOSE_WAIT_ABORT is overloaded to enable both
    // the new regionserver abort condition and interrupts for running requests.
    // If CLOSE_WAIT_ABORT is not enabled there is no change from earlier behavior,
    // we will not attempt to interrupt threads servicing requests nor crash out
    // the regionserver if something remains stubborn.

    final boolean canAbort = conf.getBoolean(CLOSE_WAIT_ABORT, DEFAULT_CLOSE_WAIT_ABORT);
    boolean useTimedWait = false;
    if (timeoutForWriteLock != null && timeoutForWriteLock != Long.MAX_VALUE) {
      // convert legacy use of timeoutForWriteLock in seconds to new use in millis
      timeoutForWriteLock = TimeUnit.SECONDS.toMillis(timeoutForWriteLock);
      useTimedWait = true;
    } else if (canAbort) {
      timeoutForWriteLock = conf.getLong(CLOSE_WAIT_TIME, DEFAULT_CLOSE_WAIT_TIME);
      useTimedWait = true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug((useTimedWait ? "Time limited wait" : "Waiting without time limit") +
        " for close lock on " + this);
    }
    final long closeWaitInterval = conf.getLong(CLOSE_WAIT_INTERVAL, DEFAULT_CLOSE_WAIT_INTERVAL);
    long elapsedWaitTime = 0;
    if (useTimedWait) {
      // Sanity check configuration
      long remainingWaitTime = timeoutForWriteLock;
      if (remainingWaitTime < closeWaitInterval) {
        LOG.warn("Time limit for close wait of " + timeoutForWriteLock +
          " ms is less than the configured lock acquisition wait interval " +
          closeWaitInterval + " ms, using wait interval as time limit");
        remainingWaitTime = closeWaitInterval;
      }
      boolean acquired = false;
      do {
        long start = EnvironmentEdgeManager.currentTime();
        try {
          acquired = lock.writeLock().tryLock(Math.min(remainingWaitTime, closeWaitInterval),
            TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // Interrupted waiting for close lock. More likely the server is shutting down, not
          // normal operation, so aborting upon interrupt while waiting on this lock would not
          // provide much value. Throw an IOE (as IIOE) like we would in the case where we
          // fail to acquire the lock.
          String msg = "Interrupted while waiting for close lock on " + this;
          LOG.warn(msg, e);
          throw (InterruptedIOException) new InterruptedIOException(msg).initCause(e);
        }
        long elapsed = EnvironmentEdgeManager.currentTime() - start;
        elapsedWaitTime += elapsed;
        remainingWaitTime -= elapsed;
        if (canAbort && !acquired && remainingWaitTime > 0) {
          // Before we loop to wait again, interrupt all region operations that might
          // still be in progress, to encourage them to break out of waiting states or
          // inner loops, throw an exception to clients, and release the read lock via
          // endRegionOperation.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupting region operations after waiting for close lock for " +
              elapsedWaitTime + " ms on " + this + ", " + remainingWaitTime +
              " ms remaining");
          }
          interruptRegionOperations();
        }
      } while (!acquired && remainingWaitTime > 0);

      // If we fail to acquire the lock, trigger an abort if we can; otherwise throw an IOE
      // to let the caller know we could not proceed with the close.
      if (!acquired) {
        String msg = "Failed to acquire close lock on " + this + " after waiting " +
          elapsedWaitTime + " ms";
        LOG.error(msg);
        if (canAbort) {
          // If we failed to acquire the write lock, abort the server
          rsServices.abort(msg, null);
        }
        throw new IOException(msg);
      }

    } else {

      long start = EnvironmentEdgeManager.currentTime();
      lock.writeLock().lock();
      elapsedWaitTime = EnvironmentEdgeManager.currentTime() - start;

    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquired close lock on " + this + " after waiting " +
        elapsedWaitTime + " ms");
    }

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
        long remainingSize = this.memStoreSizing.getDataSize();
        while (remainingSize > 0) {
          try {
            internalFlushcache(status);
            if(flushCount >0) {
              LOG.info("Running extra flush, " + flushCount +
                  " (carrying snapshot?) " + this);
            }
            flushCount++;
            tmp = this.memStoreSizing.getDataSize();
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

      Map<byte[], List<HStoreFile>> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      if (!stores.isEmpty()) {
        // initialize the thread pool for closing stores in parallel.
        ThreadPoolExecutor storeCloserThreadPool =
          getStoreOpenAndCloseThreadPool("StoreCloser-" +
            getRegionInfo().getRegionNameAsString());
        CompletionService<Pair<byte[], Collection<HStoreFile>>> completionService =
          new ExecutorCompletionService<>(storeCloserThreadPool);

        // close each store in parallel
        for (HStore store : stores.values()) {
          MemStoreSize mss = store.getFlushableSize();
          if (!(abort || mss.getDataSize() == 0 || writestate.readOnly)) {
            if (getRegionServerServices() != null) {
              getRegionServerServices().abort("Assertion failed while closing store "
                + getRegionInfo().getRegionNameAsString() + " " + store
                + ". flushableSize expected=0, actual={" + mss
                + "}. Current memStoreSize=" + this.memStoreSizing.getMemStoreSize() +
                  ". Maybe a coprocessor "
                + "operation failed and left the memstore in a partially updated state.", null);
            }
          }
          completionService
              .submit(new Callable<Pair<byte[], Collection<HStoreFile>>>() {
                @Override
                public Pair<byte[], Collection<HStoreFile>> call() throws IOException {
                  return new Pair<>(store.getColumnFamilyDescriptor().getName(), store.close());
                }
              });
        }
        try {
          for (int i = 0; i < stores.size(); i++) {
            Future<Pair<byte[], Collection<HStoreFile>>> future = completionService.take();
            Pair<byte[], Collection<HStoreFile>> storeFiles = future.get();
            List<HStoreFile> familyFiles = result.get(storeFiles.getFirst());
            if (familyFiles == null) {
              familyFiles = new ArrayList<>();
              result.put(storeFiles.getFirst(), familyFiles);
            }
            familyFiles.addAll(storeFiles.getSecond());
          }
        } catch (InterruptedException e) {
          throw throwOnInterrupt(e);
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
      // Always write close marker to wal even for read only table. This is not a big problem as we
      // do not write any data into the region; it is just a meta edit in the WAL file.
      if (!abort && wal != null && getRegionServerServices() != null &&
        RegionReplicaUtil.isDefaultReplica(getRegionInfo())) {
        writeRegionCloseMarker(wal);
      }
      this.closed.set(true);
      if (!canFlush) {
        decrMemStoreSize(this.memStoreSizing.getMemStoreSize());
      } else if (this.memStoreSizing.getDataSize() != 0) {
        LOG.error("Memstore data size is {} in region {}", this.memStoreSizing.getDataSize(), this);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor post-close hooks");
        this.coprocessorHost.postClose(abort);
      }
      if (this.metricsRegion != null) {
        this.metricsRegion.close();
      }
      if (this.metricsRegionWrapper != null) {
        Closeables.close(this.metricsRegionWrapper, true);
      }
      status.markComplete("Closed");
      LOG.info("Closed {}", this);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Wait for all current flushes and compactions of the region to complete */
  // TODO HBASE-18906. Check the usage (if any) in Phoenix and expose this or give alternate way for
  // Phoenix needs.
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
            LOG.warn("Interrupted while waiting in region {}", this);
            interrupted = true;
            break;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Wait for all current flushes of the region to complete
   */
  public void waitForFlushes() {
    waitForFlushes(0);// Unbound wait
  }

  @Override
  public boolean waitForFlushes(long timeout) {
    synchronized (writestate) {
      if (this.writestate.readOnly) {
        // we should not wait for replayed flushed if we are read only (for example in case the
        // region is a secondary replica).
        return true;
      }
      if (!writestate.flushing) return true;
      long start = EnvironmentEdgeManager.currentTime();
      long duration = 0;
      boolean interrupted = false;
      LOG.debug("waiting for cache flush to complete for region " + this);
      try {
        while (writestate.flushing) {
          if (timeout > 0 && duration >= timeout) break;
          try {
            long toWait = timeout == 0 ? 0 : (timeout - duration);
            writestate.wait(toWait);
          } catch (InterruptedException iex) {
            // essentially ignore and propagate the interrupt back up
            LOG.warn("Interrupted while waiting in region {}", this);
            interrupted = true;
            break;
          } finally {
            duration = EnvironmentEdgeManager.currentTime() - start;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      LOG.debug("Waited {} ms for region {} flush to complete", duration, this);
      return !(writestate.flushing);
    }
  }

  @Override
  public Configuration getReadOnlyConfiguration() {
    return new ReadOnlyConfiguration(this.conf);
  }

  private ThreadPoolExecutor getStoreOpenAndCloseThreadPool(
      final String threadNamePrefix) {
    int numStores = Math.max(1, this.htableDescriptor.getColumnFamilyCount());
    int maxThreads = Math.min(numStores,
        conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
            HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX));
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  ThreadPoolExecutor getStoreFileOpenAndCloseThreadPool(
      final String threadNamePrefix) {
    int numStores = Math.max(1, this.htableDescriptor.getColumnFamilyCount());
    int maxThreads = Math.max(1,
        conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
            HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX)
            / numStores);
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  private static ThreadPoolExecutor getOpenAndCloseThreadPool(int maxThreads,
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
    return this.memStoreSizing.getDataSize() >
      this.conf.getLong("hbase.hregion.preclose.flush.size", 1024 * 1024 * 5);
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public TableDescriptor getTableDescriptor() {
    return this.htableDescriptor;
  }

  public void setTableDescriptor(TableDescriptor desc) {
    htableDescriptor = desc;
  }

  /** @return WAL in use for this region */
  public WAL getWAL() {
    return this.wal;
  }

  public BlockCache getBlockCache() {
    return this.blockCache;
  }

  /**
   * Only used for unit test which doesn't start region server.
   */
  public void setBlockCache(BlockCache blockCache) {
    this.blockCache = blockCache;
  }

  public MobFileCache getMobFileCache() {
    return this.mobFileCache;
  }

  /**
   * Only used for unit test which doesn't start region server.
   */
  public void setMobFileCache(MobFileCache mobFileCache) {
    this.mobFileCache = mobFileCache;
  }

  /**
   * @return split policy for this region.
   */
  RegionSplitPolicy getSplitPolicy() {
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

  /** @return the WAL {@link HRegionFileSystem} used by this region */
  HRegionWALFileSystem getRegionWALFileSystem() throws IOException {
    return new HRegionWALFileSystem(conf, getWalFileSystem(),
      CommonFSUtils.getWALTableDir(conf, htableDescriptor.getTableName()), fs.getRegionInfo());
  }

  /** @return the WAL {@link FileSystem} being used by this region */
  FileSystem getWalFileSystem() throws IOException {
    if (walFS == null) {
      walFS = CommonFSUtils.getWALFileSystem(conf);
    }
    return walFS;
  }

  /**
   * @return the Region directory under WALRootDirectory
   * @throws IOException if there is an error getting WALRootDir
   */
  public Path getWALRegionDir() throws IOException {
    if (regionDir == null) {
      regionDir = CommonFSUtils.getWALRegionDir(conf, getRegionInfo().getTable(),
        getRegionInfo().getEncodedName());
    }
    return regionDir;
  }

  @Override
  public long getEarliestFlushTimeForAllStores() {
    return Collections.min(lastStoreFlushTimeMap.values());
  }

  @Override
  public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
    long result = Long.MAX_VALUE;
    for (HStore store : stores.values()) {
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) {
        continue;
      }
      for (HStoreFile file : storeFiles) {
        StoreFileReader sfReader = file.getReader();
        if (sfReader == null) {
          continue;
        }
        HFile.Reader reader = sfReader.getHFileReader();
        if (reader == null) {
          continue;
        }
        if (majorCompactionOnly) {
          byte[] val = reader.getHFileInfo().get(MAJOR_COMPACTION_KEY);
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
      long earliest = this.wal.getEarliestMemStoreSeqNum(encodedRegionName, familyName);
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
  /**
   * Do preparation for pending compaction.
   * @throws IOException
   */
  protected void doRegionCompactionPrep() throws IOException {
  }

  /**
   * Synchronously compact all stores in the region.
   * <p>This operation could block for a long time, so don't call it from a
   * time-sensitive thread.
   * <p>Note that no locks are taken to prevent possible conflicts between
   * compaction and splitting activities. The regionserver does not normally compact
   * and split in parallel. However by calling this method you may introduce
   * unexpected and unhandled concurrency. Don't do this unless you know what
   * you are doing.
   *
   * @param majorCompaction True to force a major compaction regardless of thresholds
   * @throws IOException
   */
  public void compact(boolean majorCompaction) throws IOException {
    if (majorCompaction) {
      stores.values().forEach(HStore::triggerMajorCompaction);
    }
    for (HStore s : stores.values()) {
      Optional<CompactionContext> compaction = s.requestCompaction();
      if (compaction.isPresent()) {
        ThroughputController controller = null;
        if (rsServices != null) {
          controller = CompactionThroughputControllerFactory.create(rsServices, conf);
        }
        if (controller == null) {
          controller = NoLimitThroughputController.INSTANCE;
        }
        compact(compaction.get(), s, controller, null);
      }
    }
  }

  /**
   * This is a helper function that compact all the stores synchronously.
   * <p>
   * It is used by utilities and testing
   */
  public void compactStores() throws IOException {
    for (HStore s : stores.values()) {
      Optional<CompactionContext> compaction = s.requestCompaction();
      if (compaction.isPresent()) {
        compact(compaction.get(), s, NoLimitThroughputController.INSTANCE, null);
      }
    }
  }

  /**
   * This is a helper function that compact the given store.
   * <p>
   * It is used by utilities and testing
   */
  void compactStore(byte[] family, ThroughputController throughputController) throws IOException {
    HStore s = getStore(family);
    Optional<CompactionContext> compaction = s.requestCompaction();
    if (compaction.isPresent()) {
      compact(compaction.get(), s, throughputController, null);
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
   * @param throughputController
   * @return whether the compaction completed
   */
  public boolean compact(CompactionContext compaction, HStore store,
      ThroughputController throughputController) throws IOException {
    return compact(compaction, store, throughputController, null);
  }

  private boolean shouldForbidMajorCompaction() {
    if (rsServices != null && rsServices.getReplicationSourceService() != null) {
      return rsServices.getReplicationSourceService().getSyncReplicationPeerInfoProvider()
          .checkState(getRegionInfo().getTable(), ForbidMajorCompactionChecker.get());
    }
    return false;
  }

  /**
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
   * will help the store file accounting).
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
   * if (closeChecker != null && closeChecker.isTimeLimit(store, now)) {
   *    progress.cancel();
   *    return false;
   * }
   * if (closeChecker != null && closeChecker.isSizeLimit(store, len)) {
   *   progress.cancel();
   *   return false;
   * }
   */
  public boolean compact(CompactionContext compaction, HStore store,
      ThroughputController throughputController, User user) throws IOException {
    assert compaction != null && compaction.hasSelection();
    assert !compaction.getRequest().getFiles().isEmpty();
    if (this.closing.get() || this.closed.get()) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      store.cancelRequestedCompaction(compaction);
      return false;
    }

    if (compaction.getRequest().isAllFiles() && shouldForbidMajorCompaction()) {
      LOG.warn("Skipping major compaction on " + this
          + " because this cluster is transiting sync replication state"
          + " from STANDBY to DOWNGRADE_ACTIVE");
      store.cancelRequestedCompaction(compaction);
      return false;
    }

    MonitoredTask status = null;
    boolean requestNeedsCancellation = true;
    try {
      byte[] cf = Bytes.toBytes(store.getColumnFamilyName());
      if (stores.get(cf) != store) {
        LOG.warn("Store " + store.getColumnFamilyName() + " on region " + this
            + " has been re-instantiated, cancel this compaction request. "
            + " It may be caused by the roll back of split transaction");
        return false;
      }

      status = TaskMonitor.get().createStatus("Compacting " + store + " in " + this);
      status.enableStatusJournal(false);
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
        LOG.info("Starting compaction of {} in {}{}", store, this,
            (compaction.getRequest().isOffPeak()?" as an off-peak compaction":""));
        doRegionCompactionPrep();
        try {
          status.setStatus("Compacting store " + store);
          // We no longer need to cancel the request on the way out of this
          // method because Store#compact will clean up unconditionally
          requestNeedsCancellation = false;
          store.compact(compaction, throughputController, user);
        } catch (InterruptedIOException iioe) {
          String msg = "region " + this + " compaction interrupted";
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
      if (status != null) {
        LOG.debug("Compaction status journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
        status.cleanup();
      }
    }
  }

  /**
   * Flush the cache.
   *
   * <p>When this method is called the cache will be flushed unless:
   * <ol>
   *   <li>the cache is empty</li>
   *   <li>the region is closed.</li>
   *   <li>a flush is already in progress</li>
   *   <li>writes are disabled</li>
   * </ol>
   *
   * <p>This method may block for some time, so it should not be called from a
   * time-sensitive thread.
   * @param flushAllStores whether we want to force a flush of all stores
   * @return FlushResult indicating whether the flush was successful or not and if
   * the region needs compacting
   *
   * @throws IOException general io exceptions
   * because a snapshot was not properly persisted.
   */
  // TODO HBASE-18905. We might have to expose a requestFlush API for CPs
  public FlushResult flush(boolean flushAllStores) throws IOException {
    return flushcache(flushAllStores, false, FlushLifeCycleTracker.DUMMY);
  }

  public interface FlushResult {
    enum Result {
      FLUSHED_NO_COMPACTION_NEEDED,
      FLUSHED_COMPACTION_NEEDED,
      // Special case where a flush didn't run because there's nothing in the memstores. Used when
      // bulk loading to know when we can still load even if a flush didn't happen.
      CANNOT_FLUSH_MEMSTORE_EMPTY,
      CANNOT_FLUSH
    }

    /** @return the detailed result code */
    Result getResult();

    /** @return true if the memstores were flushed, else false */
    boolean isFlushSucceeded();

    /** @return True if the flush requested a compaction, else false */
    boolean isCompactionNeeded();
  }

  public FlushResultImpl flushcache(boolean flushAllStores, boolean writeFlushRequestWalMarker,
    FlushLifeCycleTracker tracker) throws IOException {
    List<byte[]> families = null;
    if (flushAllStores) {
      families = new ArrayList<>();
      families.addAll(this.getTableDescriptor().getColumnFamilyNames());
    }
    return this.flushcache(families, writeFlushRequestWalMarker, tracker);
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
   * @param families stores of region to flush.
   * @param writeFlushRequestWalMarker whether to write the flush request marker to WAL
   * @param tracker used to track the life cycle of this flush
   * @return whether the flush is success and whether the region needs compacting
   *
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of wal is required
   * because a Snapshot was not properly persisted. The region is put in closing mode, and the
   * caller MUST abort after this.
   */
  public FlushResultImpl flushcache(List<byte[]> families,
      boolean writeFlushRequestWalMarker, FlushLifeCycleTracker tracker) throws IOException {
    // fail-fast instead of waiting on the lock
    if (this.closing.get()) {
      String msg = "Skipping flush on " + this + " because closing";
      LOG.debug(msg);
      return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
    }
    MonitoredTask status = TaskMonitor.get().createStatus("Flushing " + this);
    status.enableStatusJournal(false);
    status.setStatus("Acquiring readlock on region");
    // block waiting for the lock for flushing cache
    lock.readLock().lock();
    boolean flushed = true;
    try {
      if (this.closed.get()) {
        String msg = "Skipping flush on " + this + " because closed";
        LOG.debug(msg);
        status.abort(msg);
        flushed = false;
        return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor pre-flush hooks");
        coprocessorHost.preFlush(tracker);
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
          String msg = "NOT flushing " + this + " as " + (writestate.flushing ? "already flushing"
            : "writes are not enabled");
          LOG.debug(msg);
          status.abort(msg);
          flushed = false;
          return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
        }
      }

      try {
        // The reason that we do not always use flushPolicy is, when the flush is
        // caused by logRoller, we should select stores which must be flushed
        // rather than could be flushed.
        Collection<HStore> specificStoresToFlush = null;
        if (families != null) {
          specificStoresToFlush = getSpecificStores(families);
        } else {
          specificStoresToFlush = flushPolicy.selectStoresToFlush();
        }
        FlushResultImpl fs =
            internalFlushcache(specificStoresToFlush, status, writeFlushRequestWalMarker, tracker);

        if (coprocessorHost != null) {
          status.setStatus("Running post-flush coprocessor hooks");
          coprocessorHost.postFlush(tracker);
        }

        if(fs.isFlushSucceeded()) {
          flushesQueued.reset();
        }

        status.markComplete("Flush successful " + fs.toString());
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
      if (flushed) {
        // Don't log this journal stuff if no flush -- confusing.
        LOG.debug("Flush status journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      status.cleanup();
    }
  }

  /**
   * get stores which matches the specified families
   *
   * @return the stores need to be flushed.
   */
  private Collection<HStore> getSpecificStores(List<byte[]> families) {
    Collection<HStore> specificStoresToFlush = new ArrayList<>();
    for (byte[] family : families) {
      specificStoresToFlush.add(stores.get(family));
    }
    return specificStoresToFlush;
  }

  /**
   * Should the store be flushed because it is old enough.
   * <p>
   * Every FlushPolicy should call this to determine whether a store is old enough to flush (except
   * that you always flush all stores). Otherwise the method will always
   * returns true which will make a lot of flush requests.
   */
  boolean shouldFlushStore(HStore store) {
    long earliest = this.wal.getEarliestMemStoreSeqNum(getRegionInfo().getEncodedNameAsBytes(),
      store.getColumnFamilyDescriptor().getName()) - 1;
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
  boolean shouldFlush(final StringBuilder whyFlush) {
    whyFlush.setLength(0);
    // This is a rough measure.
    if (this.maxFlushedSeqId > 0
          && (this.maxFlushedSeqId + this.flushPerChanges < this.mvcc.getReadPoint())) {
      whyFlush.append("more than max edits, " + this.flushPerChanges + ", since last flush");
      return true;
    }
    long modifiedFlushCheckInterval = flushCheckInterval;
    if (getRegionInfo().getTable().isSystemTable() &&
        getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
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
    for (HStore s : stores.values()) {
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
   * @see #internalFlushcache(Collection, MonitoredTask, boolean, FlushLifeCycleTracker)
   */
  private FlushResult internalFlushcache(MonitoredTask status) throws IOException {
    return internalFlushcache(stores.values(), status, false, FlushLifeCycleTracker.DUMMY);
  }

  /**
   * Flushing given stores.
   * @see #internalFlushcache(WAL, long, Collection, MonitoredTask, boolean, FlushLifeCycleTracker)
   */
  private FlushResultImpl internalFlushcache(Collection<HStore> storesToFlush, MonitoredTask status,
      boolean writeFlushWalMarker, FlushLifeCycleTracker tracker) throws IOException {
    return internalFlushcache(this.wal, HConstants.NO_SEQNUM, storesToFlush, status,
      writeFlushWalMarker, tracker);
  }

  /**
   * Flush the memstore. Flushing the memstore is a little tricky. We have a lot of updates in the
   * memstore, all of which have also been written to the wal. We need to write those updates in the
   * memstore out to disk, while being able to process reads/writes as much as possible during the
   * flush operation.
   * <p>
   * This method may block for some time. Every time you call it, we up the regions sequence id even
   * if we don't flush; i.e. the returned region id will be at least one larger than the last edit
   * applied to this region. The returned id does not refer to an actual edit. The returned id can
   * be used for say installing a bulk loaded file just ahead of the last hfile that was the result
   * of this flush, etc.
   * @param wal Null if we're NOT to go via wal.
   * @param myseqid The seqid to use if <code>wal</code> is null writing out flush file.
   * @param storesToFlush The list of stores to flush.
   * @return object describing the flush's state
   * @throws IOException general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of WAL is required.
   */
  protected FlushResultImpl internalFlushcache(WAL wal, long myseqid,
      Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
      FlushLifeCycleTracker tracker) throws IOException {
    PrepareFlushResult result =
        internalPrepareFlushCache(wal, myseqid, storesToFlush, status, writeFlushWalMarker, tracker);
    if (result.result == null) {
      return internalFlushCacheAndCommit(wal, status, result, storesToFlush);
    } else {
      return result.result; // early exit due to failure from prepare stage
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DLS_DEAD_LOCAL_STORE",
      justification="FindBugs seems confused about trxId")
  protected PrepareFlushResult internalPrepareFlushCache(WAL wal, long myseqid,
      Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
      FlushLifeCycleTracker tracker) throws IOException {
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
    if (this.memStoreSizing.getDataSize() <= 0) {
      // Take an update lock so no edits can come into memory just yet.
      this.updatesLock.writeLock().lock();
      WriteEntry writeEntry = null;
      try {
        if (this.memStoreSizing.getDataSize() <= 0) {
          // Presume that if there are still no edits in the memstore, then there are no edits for
          // this region out in the WAL subsystem so no need to do any trickery clearing out
          // edits in the WAL sub-system. Up the sequence number so the resulting flush id is for
          // sure just beyond the last appended region edit and not associated with any edit
          // (useful as marker when bulk loading, etc.).
          if (wal != null) {
            writeEntry = mvcc.begin();
            long flushOpSeqId = writeEntry.getWriteNumber();
            FlushResultImpl flushResult =
                new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, flushOpSeqId,
                    "Nothing to flush", writeFlushRequestMarkerToWAL(wal, writeFlushWalMarker));
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
    MemStoreSizing totalSizeOfFlushableStores = new NonThreadSafeMemStoreSizing();

    Map<byte[], Long> flushedFamilyNamesToSeq = new HashMap<>();
    for (HStore store : storesToFlush) {
      flushedFamilyNamesToSeq.put(store.getColumnFamilyDescriptor().getName(),
        store.preFlushSeqIDEstimation());
    }

    TreeMap<byte[], StoreFlushContext> storeFlushCtxs = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], List<Path>> committedFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], MemStoreSize> storeFlushableSize = new TreeMap<>(Bytes.BYTES_COMPARATOR);
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
            wal.startCacheFlush(encodedRegionName, flushedFamilyNamesToSeq);
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

      for (HStore s : storesToFlush) {
        storeFlushCtxs.put(s.getColumnFamilyDescriptor().getName(),
          s.createFlushContext(flushOpSeqId, tracker));
        // for writing stores to WAL
        committedFiles.put(s.getColumnFamilyDescriptor().getName(), null);
      }

      // write the snapshot start to WAL
      if (wal != null && !writestate.readOnly) {
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH,
            getRegionInfo(), flushOpSeqId, committedFiles);
        // No sync. Sync is below where no updates lock and we do FlushAction.COMMIT_FLUSH
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false,
          mvcc, regionReplicationSink.orElse(null));
      }

      // Prepare flush (take a snapshot)
      storeFlushCtxs.forEach((name, flush) -> {
        MemStoreSize snapshotSize = flush.prepare();
        totalSizeOfFlushableStores.incMemStoreSize(snapshotSize);
        storeFlushableSize.put(name, snapshotSize);
      });
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
  private void logFatLineOnFlush(Collection<HStore> storesToFlush, long sequenceId) {
    if (!LOG.isInfoEnabled()) {
      return;
    }
    // Log a fat line detailing what is being flushed.
    StringBuilder perCfExtras = null;
    if (!isAllFamilies(storesToFlush)) {
      perCfExtras = new StringBuilder();
      for (HStore store: storesToFlush) {
        MemStoreSize mss = store.getFlushableSize();
        perCfExtras.append("; ").append(store.getColumnFamilyName());
        perCfExtras.append("={dataSize=")
            .append(StringUtils.byteDesc(mss.getDataSize()));
        perCfExtras.append(", heapSize=")
            .append(StringUtils.byteDesc(mss.getHeapSize()));
        perCfExtras.append(", offHeapSize=")
            .append(StringUtils.byteDesc(mss.getOffHeapSize()));
        perCfExtras.append("}");
      }
    }
    MemStoreSize mss = this.memStoreSizing.getMemStoreSize();
    LOG.info("Flushing " + this.getRegionInfo().getEncodedName() + " " +
        storesToFlush.size() + "/" + stores.size() + " column families," +
        " dataSize=" + StringUtils.byteDesc(mss.getDataSize()) +
        " heapSize=" + StringUtils.byteDesc(mss.getHeapSize()) +
        ((perCfExtras != null && perCfExtras.length() > 0)? perCfExtras.toString(): "") +
        ((wal != null) ? "" : "; WAL is null, using passed sequenceid=" + sequenceId));
  }

  private void doAbortFlushToWAL(final WAL wal, final long flushOpSeqId,
      final Map<byte[], List<Path>> committedFiles) {
    if (wal == null) return;
    try {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
      WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false, mvcc,
        null);
    } catch (Throwable t) {
      LOG.warn("Received unexpected exception trying to write ABORT_FLUSH marker to WAL: {} in "
        + " region {}", StringUtils.stringifyException(t), this);
      // ignore this since we will be aborting the RS with DSE.
    }
    // we have called wal.startCacheFlush(), now we have to abort it
    wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
  }

  /**
   * Sync unflushed WAL changes. See HBASE-8208 for details
   */
  private static void doSyncOfUnflushedWALChanges(final WAL wal, final RegionInfo hri)
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
  private boolean isAllFamilies(Collection<HStore> families) {
    return families == null || this.stores.size() == families.size();
  }

  /**
   * Writes a marker to WAL indicating a flush is requested but cannot be complete due to various
   * reasons. Ignores exceptions from WAL. Returns whether the write succeeded.
   * @return whether WAL write was successful
   */
  private boolean writeFlushRequestMarkerToWAL(WAL wal, boolean writeFlushWalMarker) {
    if (writeFlushWalMarker && wal != null && !writestate.readOnly) {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.CANNOT_FLUSH,
        getRegionInfo(), -1, new TreeMap<>(Bytes.BYTES_COMPARATOR));
      try {
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true, mvcc,
          regionReplicationSink.orElse(null));
        return true;
      } catch (IOException e) {
        LOG.warn(getRegionInfo().getEncodedName() + " : " +
          "Received exception while trying to write the flush request to wal", e);
      }
    }
    return false;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="Intentional; notify is about completed flush")
  FlushResultImpl internalFlushCacheAndCommit(WAL wal, MonitoredTask status,
      PrepareFlushResult prepareResult, Collection<HStore> storesToFlush) throws IOException {
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
      for (Map.Entry<byte[], StoreFlushContext> flushEntry : storeFlushCtxs.entrySet()) {
        StoreFlushContext sfc = flushEntry.getValue();
        boolean needsCompaction = sfc.commit(status);
        if (needsCompaction) {
          compactionRequested = true;
        }
        byte[] storeName = flushEntry.getKey();
        List<Path> storeCommittedFiles = sfc.getCommittedFiles();
        committedFiles.put(storeName, storeCommittedFiles);
        // Flush committed no files, indicating flush is empty or flush was canceled
        if (storeCommittedFiles == null || storeCommittedFiles.isEmpty()) {
          MemStoreSize storeFlushableSize = prepareResult.storeFlushableSize.get(storeName);
          prepareResult.totalFlushableSize.decMemStoreSize(storeFlushableSize);
        }
        flushedOutputFileSize += sfc.getOutputFileSize();
      }
      storeFlushCtxs.clear();

      // Set down the memstore size by amount of flush.
      MemStoreSize mss = prepareResult.totalFlushableSize.getMemStoreSize();
      this.decrMemStoreSize(mss);

      // Increase the size of this Region for the purposes of quota. Noop if quotas are disabled.
      // During startup, quota manager may not be initialized yet.
      if (rsServices != null) {
        RegionServerSpaceQuotaManager quotaManager = rsServices.getRegionServerSpaceQuotaManager();
        if (quotaManager != null) {
          quotaManager.getRegionSizeStore().incrementRegionSize(
              this.getRegionInfo(), flushedOutputFileSize);
        }
      }

      if (wal != null) {
        // write flush marker to WAL. If fail, we should throw DroppedSnapshotException
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.COMMIT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true, mvcc,
          regionReplicationSink.orElse(null));
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
          WALUtil.writeFlushMarker(wal, this.replicationScope, getRegionInfo(), desc, false, mvcc,
            null);
        } catch (Throwable ex) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "failed writing ABORT_FLUSH marker to WAL", ex);
          // ignore this since we will be aborting the RS with DSE.
        }
        wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
      }
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
        Bytes.toStringBinary(getRegionInfo().getRegionName()), t);
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
      wal.completeCacheFlush(this.getRegionInfo().getEncodedNameAsBytes(), flushedSeqId);
    }

    // Record latest flush time
    for (HStore store: storesToFlush) {
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
    MemStoreSize mss = prepareResult.totalFlushableSize.getMemStoreSize();
    long memstoresize = this.memStoreSizing.getMemStoreSize().getDataSize();
    String msg = "Finished flush of"
        + " dataSize ~" + StringUtils.byteDesc(mss.getDataSize()) + "/" + mss.getDataSize()
        + ", heapSize ~" + StringUtils.byteDesc(mss.getHeapSize()) + "/" + mss.getHeapSize()
        + ", currentSize=" + StringUtils.byteDesc(memstoresize) + "/" + memstoresize
        + " for " + this.getRegionInfo().getEncodedName() + " in " + time + "ms, sequenceid="
        + flushOpSeqId +  ", compaction requested=" + compactionRequested
        + ((wal == null) ? "; wal=null" : "");
    LOG.info(msg);
    status.setStatus(msg);

    if (rsServices != null && rsServices.getMetrics() != null) {
      rsServices.getMetrics().updateFlush(getTableDescriptor().getTableName().getNameAsString(),
          time,
          mss.getDataSize(), flushedOutputFileSize);
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
  protected long getNextSequenceId(final WAL wal) throws IOException {
    WriteEntry we = mvcc.begin();
    mvcc.completeAndWait(we);
    return we.getWriteNumber();
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public RegionScannerImpl getScanner(Scan scan) throws IOException {
   return getScanner(scan, null);
  }

  @Override
  public RegionScannerImpl getScanner(Scan scan, List<KeyValueScanner> additionalScanners)
      throws IOException {
    return getScanner(scan, additionalScanners, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private RegionScannerImpl getScanner(Scan scan, List<KeyValueScanner> additionalScanners,
    long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      startRegionOperation(Operation.SCAN);
      try {
        // Verify families are all valid
        if (!scan.hasFamilies()) {
          // Adding all families to scanner
          for (byte[] family : this.htableDescriptor.getColumnFamilyNames()) {
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
    }, () -> createRegionSpan("Region.getScanner"));
  }

  protected RegionScannerImpl instantiateRegionScanner(Scan scan,
    List<KeyValueScanner> additionalScanners, long nonceGroup, long nonce) throws IOException {
    if (scan.isReversed()) {
      if (scan.getFilter() != null) {
        scan.getFilter().setReversed(true);
      }
      return new ReversedRegionScannerImpl(scan, additionalScanners, this, nonceGroup, nonce);
    }
    return new RegionScannerImpl(scan, additionalScanners, this, nonceGroup, nonce);
  }

  /**
   * Prepare a delete for a row mutation processor
   * @param delete The passed delete is modified by this method. WARNING!
   */
  private void prepareDelete(Delete delete) throws IOException {
    // Check to see if this is a deleteRow insert
    if(delete.getFamilyCellMap().isEmpty()){
      for(byte [] family : this.htableDescriptor.getColumnFamilyNames()){
        // Don't eat the timestamp
        delete.addFamily(family, delete.getTimestamp());
      }
    } else {
      for(byte [] family : delete.getFamilyCellMap().keySet()) {
        if(family == null) {
          throw new NoSuchColumnFamilyException("Empty family is invalid");
        }
        checkFamily(family, delete.getDurability());
      }
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.DELETE);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(delete);
      } finally {
        closeRegionOperation(Operation.DELETE);
      }
    }, () -> createRegionSpan("Region.delete"));
  }

  /**
   * Set up correct timestamps in the KVs in Delete object.
   * <p/>
   * Caller should have the row and region locks.
   */
  private void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyMap,
      byte[] byteNow) throws IOException {
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {

      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;

      Map<byte[], Integer> kvCount = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        //  Check if time is LATEST, change to time of most recent addition if so
        //  This is expensive.
        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP
            && PrivateCellUtil.isDeleteType(cell)) {
          byte[] qual = CellUtil.cloneQualifier(cell);

          Integer count = kvCount.get(qual);
          if (count == null) {
            kvCount.put(qual, 1);
          } else {
            kvCount.put(qual, count + 1);
          }
          count = kvCount.get(qual);

          Get get = new Get(CellUtil.cloneRow(cell));
          get.readVersions(count);
          get.addColumn(family, qual);
          if (coprocessorHost != null) {
            if (!coprocessorHost.prePrepareTimeStampForDeleteVersion(mutation, cell,
                byteNow, get)) {
              updateDeleteLatestVersionTimestamp(cell, get, count, byteNow);
            }
          } else {
            updateDeleteLatestVersionTimestamp(cell, get, count, byteNow);
          }
        } else {
          PrivateCellUtil.updateLatestStamp(cell, byteNow);
        }
      }
    }
  }

  private void updateDeleteLatestVersionTimestamp(Cell cell, Get get, int count, byte[] byteNow)
      throws IOException {
    try (RegionScanner scanner = getScanner(new Scan(get))) {
      // NOTE: Please don't use HRegion.get() instead,
      // because it will copy cells to heap. See HBASE-26036
      List<Cell> result = new ArrayList<>();
      scanner.next(result);

      if (result.size() < count) {
        // Nothing to delete
        PrivateCellUtil.updateLatestStamp(cell, byteNow);
        return;
      }
      if (result.size() > count) {
        throw new RuntimeException("Unexpected size: " + result.size());
      }
      Cell getCell = result.get(count - 1);
      PrivateCellUtil.setTimestamp(cell, getCell.getTimestamp());
    }
  }

  @Override
  public void put(Put put) throws IOException {
    TraceUtil.trace(() -> {
      checkReadOnly();

      // Do a rough check that we have resources to accept a write. The check is
      // 'rough' in that between the resource check and the call to obtain a
      // read lock, resources may run out. For now, the thought is that this
      // will be extremely rare; we'll deal with it when it happens.
      checkResources();
      startRegionOperation(Operation.PUT);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(put);
      } finally {
        closeRegionOperation(Operation.PUT);
      }
    }, () -> createRegionSpan("Region.put"));
  }

  /**
   * Class that tracks the progress of a batch operations, accumulating status codes and tracking
   * the index at which processing is proceeding. These batch operations may get split into
   * mini-batches for processing.
   */
  private abstract static class BatchOperation<T> {
    protected final T[] operations;
    protected final OperationStatus[] retCodeDetails;
    protected final WALEdit[] walEditsFromCoprocessors;
    // reference family cell maps directly so coprocessors can mutate them if desired
    protected final Map<byte[], List<Cell>>[] familyCellMaps;
    // For Increment/Append operations
    protected final Result[] results;

    protected final HRegion region;
    protected int nextIndexToProcess = 0;
    protected final ObservedExceptionsInBatch observedExceptions;
    //Durability of the batch (highest durability of all operations)
    protected Durability durability;
    protected boolean atomic = false;

    public BatchOperation(final HRegion region, T[] operations) {
      this.operations = operations;
      this.retCodeDetails = new OperationStatus[operations.length];
      Arrays.fill(this.retCodeDetails, OperationStatus.NOT_RUN);
      this.walEditsFromCoprocessors = new WALEdit[operations.length];
      familyCellMaps = new Map[operations.length];
      this.results = new Result[operations.length];

      this.region = region;
      observedExceptions = new ObservedExceptionsInBatch();
      durability = Durability.USE_DEFAULT;
    }

    /**
     * Visitor interface for batch operations
     */
    @FunctionalInterface
    interface Visitor {
      /**
       * @param index operation index
       * @return If true continue visiting remaining entries, break otherwise
       */
      boolean visit(int index) throws IOException;
    }

    /**
     * Helper method for visiting pending/ all batch operations
     */
    public void visitBatchOperations(boolean pendingOnly, int lastIndexExclusive, Visitor visitor)
        throws IOException {
      assert lastIndexExclusive <= this.size();
      for (int i = nextIndexToProcess; i < lastIndexExclusive; i++) {
        if (!pendingOnly || isOperationPending(i)) {
          if (!visitor.visit(i)) {
            break;
          }
        }
      }
    }

    public abstract Mutation getMutation(int index);

    public abstract long getNonceGroup(int index);

    public abstract long getNonce(int index);

    /**
     * This method is potentially expensive and useful mostly for non-replay CP path.
     */
    public abstract Mutation[] getMutationsForCoprocs();

    public abstract boolean isInReplay();

    public abstract long getOrigLogSeqNum();

    public abstract void startRegionOperation() throws IOException;

    public abstract void closeRegionOperation() throws IOException;

    /**
     * Validates each mutation and prepares a batch for write. If necessary (non-replay case), runs
     * CP prePut()/preDelete()/preIncrement()/preAppend() hooks for all mutations in a batch. This
     * is intended to operate on entire batch and will be called from outside of class to check
     * and prepare batch. This can be implemented by calling helper method
     * {@link #checkAndPrepareMutation(int, long)} in a 'for' loop over mutations.
     */
    public abstract void checkAndPrepare() throws IOException;

    /**
     * Implement any Put request specific check and prepare logic here. Please refer to
     * {@link #checkAndPrepareMutation(Mutation, long)} for how its used.
     */
    protected abstract void checkAndPreparePut(final Put p) throws IOException;

    /**
     * If necessary, calls preBatchMutate() CP hook for a mini-batch and updates metrics, cell
     * count, tags and timestamp for all cells of all operations in a mini-batch.
     */
    public abstract void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation>
        miniBatchOp, long timestamp, final List<RowLock> acquiredRowLocks) throws IOException;

    /**
     * Write mini-batch operations to MemStore
     */
    public abstract WriteEntry writeMiniBatchOperationsToMemStore(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
        throws IOException;

    protected void writeMiniBatchOperationsToMemStore(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final long writeNumber)
        throws IOException {
      MemStoreSizing memStoreAccounting = new NonThreadSafeMemStoreSizing();
      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        // We need to update the sequence id for following reasons.
        // 1) If the op is in replay mode, FSWALEntry#stampRegionSequenceId won't stamp sequence id.
        // 2) If no WAL, FSWALEntry won't be used
        // we use durability of the original mutation for the mutation passed by CP.
        if (isInReplay() || getMutation(index).getDurability() == Durability.SKIP_WAL) {
          region.updateSequenceId(familyCellMaps[index].values(), writeNumber);
        }
        applyFamilyMapToMemStore(familyCellMaps[index], memStoreAccounting);
        return true;
      });
      // update memStore size
      region.incMemStoreSize(memStoreAccounting.getDataSize(), memStoreAccounting.getHeapSize(),
        memStoreAccounting.getOffHeapSize(), memStoreAccounting.getCellsCount());
    }

    public boolean isDone() {
      return nextIndexToProcess == operations.length;
    }

    public int size() {
      return operations.length;
    }

    public boolean isOperationPending(int index) {
      return retCodeDetails[index].getOperationStatusCode() == OperationStatusCode.NOT_RUN;
    }

    public List<UUID> getClusterIds() {
      assert size() != 0;
      return getMutation(0).getClusterIds();
    }

    boolean isAtomic() {
      return atomic;
    }

    /**
     * Helper method that checks and prepares only one mutation. This can be used to implement
     * {@link #checkAndPrepare()} for entire Batch.
     * NOTE: As CP prePut()/preDelete()/preIncrement()/preAppend() hooks may modify mutations,
     * this method should be called after prePut()/preDelete()/preIncrement()/preAppend() CP hooks
     * are run for the mutation
     */
    protected void checkAndPrepareMutation(Mutation mutation, final long timestamp)
        throws IOException {
      region.checkRow(mutation.getRow(), "batchMutate");
      if (mutation instanceof Put) {
        // Check the families in the put. If bad, skip this one.
        checkAndPreparePut((Put) mutation);
        region.checkTimestamps(mutation.getFamilyCellMap(), timestamp);
      } else if (mutation instanceof Delete) {
        region.prepareDelete((Delete) mutation);
      } else if (mutation instanceof Increment || mutation instanceof Append) {
        region.checkFamilies(mutation.getFamilyCellMap().keySet(), mutation.getDurability());
      }
    }

    protected void checkAndPrepareMutation(int index, long timestamp) throws IOException {
      Mutation mutation = getMutation(index);
      try {
        this.checkAndPrepareMutation(mutation, timestamp);

        if (mutation instanceof Put || mutation instanceof Delete) {
          // store the family map reference to allow for mutations
          familyCellMaps[index] = mutation.getFamilyCellMap();
        }

        // store durability for the batch (highest durability of all operations in the batch)
        Durability tmpDur = region.getEffectiveDurability(mutation.getDurability());
        if (tmpDur.ordinal() > durability.ordinal()) {
          durability = tmpDur;
        }
      } catch (NoSuchColumnFamilyException nscfe) {
        final String msg = "No such column family in batch mutation in region " + this;
        if (observedExceptions.hasSeenNoSuchFamily()) {
          LOG.warn(msg + nscfe.getMessage());
        } else {
          LOG.warn(msg, nscfe);
          observedExceptions.sawNoSuchFamily();
        }
        retCodeDetails[index] = new OperationStatus(
            OperationStatusCode.BAD_FAMILY, nscfe.getMessage());
        if (isAtomic()) { // fail, atomic means all or none
          throw nscfe;
        }
      } catch (FailedSanityCheckException fsce) {
        final String msg = "Batch Mutation did not pass sanity check in region " + this;
        if (observedExceptions.hasSeenFailedSanityCheck()) {
          LOG.warn(msg + fsce.getMessage());
        } else {
          LOG.warn(msg, fsce);
          observedExceptions.sawFailedSanityCheck();
        }
        retCodeDetails[index] = new OperationStatus(
            OperationStatusCode.SANITY_CHECK_FAILURE, fsce.getMessage());
        if (isAtomic()) {
          throw fsce;
        }
      } catch (WrongRegionException we) {
        final String msg = "Batch mutation had a row that does not belong to this region " + this;
        if (observedExceptions.hasSeenWrongRegion()) {
          LOG.warn(msg + we.getMessage());
        } else {
          LOG.warn(msg, we);
          observedExceptions.sawWrongRegion();
        }
        retCodeDetails[index] = new OperationStatus(
            OperationStatusCode.SANITY_CHECK_FAILURE, we.getMessage());
        if (isAtomic()) {
          throw we;
        }
      }
    }

    /**
     * Creates Mini-batch of all operations [nextIndexToProcess, lastIndexExclusive) for which
     * a row lock can be acquired. All mutations with locked rows are considered to be
     * In-progress operations and hence the name {@link MiniBatchOperationInProgress}. Mini batch
     * is window over {@link BatchOperation} and contains contiguous pending operations.
     *
     * @param acquiredRowLocks keeps track of rowLocks acquired.
     */
    public MiniBatchOperationInProgress<Mutation> lockRowsAndBuildMiniBatch(
        List<RowLock> acquiredRowLocks) throws IOException {
      int readyToWriteCount = 0;
      int lastIndexExclusive = 0;
      RowLock prevRowLock = null;
      for (; lastIndexExclusive < size(); lastIndexExclusive++) {
        // It reaches the miniBatchSize, stop here and process the miniBatch
        // This only applies to non-atomic batch operations.
        if (!isAtomic() && (readyToWriteCount == region.miniBatchSize)) {
          break;
        }

        if (!isOperationPending(lastIndexExclusive)) {
          continue;
        }

        // HBASE-19389 Limit concurrency of put with dense (hundreds) columns to avoid exhausting
        // RS handlers, covering both MutationBatchOperation and ReplayBatchOperation
        // The BAD_FAMILY/SANITY_CHECK_FAILURE cases are handled in checkAndPrepare phase and won't
        // pass the isOperationPending check
        Map<byte[], List<Cell>> curFamilyCellMap =
            getMutation(lastIndexExclusive).getFamilyCellMap();
        try {
          // start the protector before acquiring row lock considering performance, and will finish
          // it when encountering exception
          region.storeHotnessProtector.start(curFamilyCellMap);
        } catch (RegionTooBusyException rtbe) {
          region.storeHotnessProtector.finish(curFamilyCellMap);
          if (isAtomic()) {
            throw rtbe;
          }
          retCodeDetails[lastIndexExclusive] =
              new OperationStatus(OperationStatusCode.STORE_TOO_BUSY, rtbe.getMessage());
          continue;
        }

        Mutation mutation = getMutation(lastIndexExclusive);
        // If we haven't got any rows in our batch, we should block to get the next one.
        RowLock rowLock = null;
        boolean throwException = false;
        try {
          // if atomic then get exclusive lock, else shared lock
          rowLock = region.getRowLock(mutation.getRow(), !isAtomic(), prevRowLock);
        } catch (TimeoutIOException | InterruptedIOException e) {
          // NOTE: We will retry when other exceptions, but we should stop if we receive
          // TimeoutIOException or InterruptedIOException as operation has timed out or
          // interrupted respectively.
          throwException = true;
          throw e;
        } catch (IOException ioe) {
          LOG.warn("Failed getting lock, row={}, in region {}",
            Bytes.toStringBinary(mutation.getRow()), this, ioe);
          if (isAtomic()) { // fail, atomic means all or none
            throwException = true;
            throw ioe;
          }
        } catch (Throwable throwable) {
          throwException = true;
          throw throwable;
        } finally {
          if (throwException) {
            region.storeHotnessProtector.finish(curFamilyCellMap);
          }
        }
        if (rowLock == null) {
          // We failed to grab another lock
          if (isAtomic()) {
            region.storeHotnessProtector.finish(curFamilyCellMap);
            throw new IOException("Can't apply all operations atomically!");
          }
          break; // Stop acquiring more rows for this batch
        } else {
          if (rowLock != prevRowLock) {
            // It is a different row now, add this to the acquiredRowLocks and
            // set prevRowLock to the new returned rowLock
            acquiredRowLocks.add(rowLock);
            prevRowLock = rowLock;
          }
        }

        readyToWriteCount++;
      }
      return createMiniBatch(lastIndexExclusive, readyToWriteCount);
    }

    protected MiniBatchOperationInProgress<Mutation> createMiniBatch(final int lastIndexExclusive,
        final int readyToWriteCount) {
      return new MiniBatchOperationInProgress<>(getMutationsForCoprocs(), retCodeDetails,
          walEditsFromCoprocessors, nextIndexToProcess, lastIndexExclusive, readyToWriteCount);
    }

    /**
     * Builds separate WALEdit per nonce by applying input mutations. If WALEdits from CP are
     * present, they are merged to result WALEdit.
     */
    public List<Pair<NonceKey, WALEdit>> buildWALEdits(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      List<Pair<NonceKey, WALEdit>> walEdits = new ArrayList<>();

      visitBatchOperations(true, nextIndexToProcess + miniBatchOp.size(), new Visitor() {
        private Pair<NonceKey, WALEdit> curWALEditForNonce;

        @Override
        public boolean visit(int index) throws IOException {
          Mutation m = getMutation(index);
          // we use durability of the original mutation for the mutation passed by CP.
          if (region.getEffectiveDurability(m.getDurability()) == Durability.SKIP_WAL) {
            region.recordMutationWithoutWal(m.getFamilyCellMap());
            return true;
          }

          // the batch may contain multiple nonce keys (replay case). If so, write WALEdit for each.
          // Given how nonce keys are originally written, these should be contiguous.
          // They don't have to be, it will still work, just write more WALEdits than needed.
          long nonceGroup = getNonceGroup(index);
          long nonce = getNonce(index);
          if (curWALEditForNonce == null ||
              curWALEditForNonce.getFirst().getNonceGroup() != nonceGroup ||
              curWALEditForNonce.getFirst().getNonce() != nonce) {
            curWALEditForNonce = new Pair<>(new NonceKey(nonceGroup, nonce),
                new WALEdit(miniBatchOp.getCellCount(), isInReplay()));
            walEdits.add(curWALEditForNonce);
          }
          WALEdit walEdit = curWALEditForNonce.getSecond();

          // Add WAL edits from CPs.
          WALEdit fromCP = walEditsFromCoprocessors[index];
          if (fromCP != null) {
            for (Cell cell : fromCP.getCells()) {
              walEdit.add(cell);
            }
          }
          walEdit.add(familyCellMaps[index]);

          return true;
        }
      });
      return walEdits;
    }

    /**
     * This method completes mini-batch operations by calling postBatchMutate() CP hook (if
     * required) and completing mvcc.
     */
    public void completeMiniBatchOperations(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
        throws IOException {
      if (writeEntry != null) {
        region.mvcc.completeAndWait(writeEntry);
      }
    }

    public void doPostOpCleanupForMiniBatch(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WALEdit walEdit,
        boolean success) throws IOException {
      doFinishHotnessProtector(miniBatchOp);
    }

    private void doFinishHotnessProtector(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp) {
      // check and return if the protector is not enabled
      if (!region.storeHotnessProtector.isEnable()) {
        return;
      }
      // miniBatchOp is null, if and only if lockRowsAndBuildMiniBatch throwing exception.
      // This case was handled.
      if (miniBatchOp == null) {
        return;
      }

      final int finalLastIndexExclusive = miniBatchOp.getLastIndexExclusive();

      for (int i = nextIndexToProcess; i < finalLastIndexExclusive; i++) {
        switch (retCodeDetails[i].getOperationStatusCode()) {
          case SUCCESS:
          case FAILURE:
            region.storeHotnessProtector.finish(getMutation(i).getFamilyCellMap());
            break;
          default:
            // do nothing
            // We won't start the protector for NOT_RUN/BAD_FAMILY/SANITY_CHECK_FAILURE and the
            // STORE_TOO_BUSY case is handled in StoreHotnessProtector#start
            break;
        }
      }
    }

    /**
     * Atomically apply the given map of family->edits to the memstore.
     * This handles the consistency control on its own, but the caller
     * should already have locked updatesLock.readLock(). This also does
     * <b>not</b> check the families for validity.
     *
     * @param familyMap Map of Cells by family
     */
    protected void applyFamilyMapToMemStore(Map<byte[], List<Cell>> familyMap,
        MemStoreSizing memstoreAccounting) throws IOException {
      for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
        byte[] family = e.getKey();
        List<Cell> cells = e.getValue();
        assert cells instanceof RandomAccess;
        region.applyToMemStore(region.getStore(family), cells, false, memstoreAccounting);
      }
    }
  }


  /**
   * Batch of mutation operations. Base class is shared with {@link ReplayBatchOperation} as most of
   * the logic is same.
   */
  private static class MutationBatchOperation extends BatchOperation<Mutation> {

    // For nonce operations
    private long nonceGroup;
    private long nonce;
    protected boolean canProceed;

    public MutationBatchOperation(final HRegion region, Mutation[] operations, boolean atomic,
      long nonceGroup, long nonce) {
      super(region, operations);
      this.atomic = atomic;
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
    public long getOrigLogSeqNum() {
      return SequenceId.NO_SEQUENCE_ID;
    }

    @Override
    public void startRegionOperation() throws IOException {
      region.startRegionOperation(Operation.BATCH_MUTATE);
    }

    @Override
    public void closeRegionOperation() throws IOException {
      region.closeRegionOperation(Operation.BATCH_MUTATE);
    }

    @Override
    public void checkAndPreparePut(Put p) throws IOException {
      region.checkFamilies(p.getFamilyCellMap().keySet(), p.getDurability());
    }

    @Override
    public void checkAndPrepare() throws IOException {
      // index 0: puts, index 1: deletes, index 2: increments, index 3: append
      final int[] metrics = {0, 0, 0, 0};

      visitBatchOperations(true, this.size(), new Visitor() {
        private long now = EnvironmentEdgeManager.currentTime();
        private WALEdit walEdit;
        @Override
        public boolean visit(int index) throws IOException {
          // Run coprocessor pre hook outside of locks to avoid deadlock
          if (region.coprocessorHost != null) {
            if (walEdit == null) {
              walEdit = new WALEdit();
            }
            callPreMutateCPHook(index, walEdit, metrics);
            if (!walEdit.isEmpty()) {
              walEditsFromCoprocessors[index] = walEdit;
              walEdit = null;
            }
          }
          if (isOperationPending(index)) {
            // TODO: Currently validation is done with current time before acquiring locks and
            // updates are done with different timestamps after acquiring locks. This behavior is
            // inherited from the code prior to this change. Can this be changed?
            checkAndPrepareMutation(index, now);
          }
          return true;
        }
      });

      // FIXME: we may update metrics twice! here for all operations bypassed by CP and later in
      // normal processing.
      // Update metrics in same way as it is done when we go the normal processing route (we now
      // update general metrics though a Coprocessor did the work).
      if (region.metricsRegion != null) {
        if (metrics[0] > 0) {
          // There were some Puts in the batch.
          region.metricsRegion.updatePut();
        }
        if (metrics[1] > 0) {
          // There were some Deletes in the batch.
          region.metricsRegion.updateDelete();
        }
        if (metrics[2] > 0) {
          // There were some Increment in the batch.
          region.metricsRegion.updateIncrement();
        }
        if (metrics[3] > 0) {
          // There were some Append in the batch.
          region.metricsRegion.updateAppend();
        }
      }
    }

    @Override
    public void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
        long timestamp, final List<RowLock> acquiredRowLocks) throws IOException {
      // For nonce operations
      canProceed = startNonceOperation();

      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        Mutation mutation = getMutation(index);
        if (mutation instanceof Put) {
          HRegion.updateCellTimestamps(familyCellMaps[index].values(), Bytes.toBytes(timestamp));
          miniBatchOp.incrementNumOfPuts();
        } else if (mutation instanceof Delete) {
          region.prepareDeleteTimestamps(mutation, familyCellMaps[index],
            Bytes.toBytes(timestamp));
          miniBatchOp.incrementNumOfDeletes();
        } else if (mutation instanceof Increment || mutation instanceof Append) {
          boolean returnResults;
          if (mutation instanceof Increment) {
            returnResults = ((Increment) mutation).isReturnResults();
          } else {
            returnResults = ((Append) mutation).isReturnResults();
          }

          // For nonce operations
          if (!canProceed) {
            Result result;
            if (returnResults) {
              // convert duplicate increment/append to get
              List<Cell> results = region.get(toGet(mutation), false, nonceGroup, nonce);
              result = Result.create(results);
            } else {
              result = Result.EMPTY_RESULT;
            }
            retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
            return true;
          }

          Result result = null;
          if (region.coprocessorHost != null) {
            if (mutation instanceof Increment) {
              result = region.coprocessorHost.preIncrementAfterRowLock((Increment) mutation);
            } else {
              result = region.coprocessorHost.preAppendAfterRowLock((Append) mutation);
            }
          }
          if (result != null) {
            retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS,
              returnResults ? result : Result.EMPTY_RESULT);
            return true;
          }

          List<Cell> results = returnResults ? new ArrayList<>(mutation.size()) : null;
          familyCellMaps[index] = reckonDeltas(mutation, results, timestamp);
          this.results[index] = results != null ? Result.create(results) : Result.EMPTY_RESULT;

          if (mutation instanceof Increment) {
            miniBatchOp.incrementNumOfIncrements();
          } else {
            miniBatchOp.incrementNumOfAppends();
          }
        }
        region.rewriteCellTags(familyCellMaps[index], mutation);

        // update cell count
        if (region.getEffectiveDurability(mutation.getDurability()) != Durability.SKIP_WAL) {
          for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
            miniBatchOp.addCellCount(cells.size());
          }
        }

        WALEdit fromCP = walEditsFromCoprocessors[index];
        if (fromCP != null) {
          miniBatchOp.addCellCount(fromCP.size());
        }
        return true;
      });

      if (region.coprocessorHost != null) {
        // calling the pre CP hook for batch mutation
        region.coprocessorHost.preBatchMutate(miniBatchOp);
        checkAndMergeCPMutations(miniBatchOp, acquiredRowLocks, timestamp);
      }
    }

    /**
     * Starts the nonce operation for a mutation, if needed.
     * @return whether to proceed this mutation.
     */
    private boolean startNonceOperation() throws IOException {
      if (region.rsServices == null || region.rsServices.getNonceManager() == null
        || nonce == HConstants.NO_NONCE) {
        return true;
      }
      boolean canProceed;
      try {
        canProceed = region.rsServices.getNonceManager()
          .startOperation(nonceGroup, nonce, region.rsServices);
      } catch (InterruptedException ex) {
        throw new InterruptedIOException("Nonce start operation interrupted");
      }
      return canProceed;
    }

    /**
     * Ends nonce operation for a mutation, if needed.
     * @param success Whether the operation for this nonce has succeeded.
     */
    private void endNonceOperation(boolean success) {
      if (region.rsServices != null && region.rsServices.getNonceManager() != null
        && nonce != HConstants.NO_NONCE) {
        region.rsServices.getNonceManager().endOperation(nonceGroup, nonce, success);
      }
    }

    private static Get toGet(final Mutation mutation) throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      Get get = new Get(mutation.getRow());
      CellScanner cellScanner = mutation.cellScanner();
      while (!cellScanner.advance()) {
        Cell cell = cellScanner.current();
        get.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
      }
      if (mutation instanceof Increment) {
        // Increment
        Increment increment = (Increment) mutation;
        get.setTimeRange(increment.getTimeRange().getMin(), increment.getTimeRange().getMax());
      } else {
        // Append
        Append append = (Append) mutation;
        get.setTimeRange(append.getTimeRange().getMin(), append.getTimeRange().getMax());
      }
      for (Entry<String, byte[]> entry : mutation.getAttributesMap().entrySet()) {
        get.setAttribute(entry.getKey(), entry.getValue());
      }
      return get;
    }

    private Map<byte[], List<Cell>> reckonDeltas(Mutation mutation, List<Cell> results,
      long now) throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      Map<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      // Process a Store/family at a time.
      for (Map.Entry<byte [], List<Cell>> entry: mutation.getFamilyCellMap().entrySet()) {
        final byte[] columnFamilyName = entry.getKey();
        List<Cell> deltas = entry.getValue();
        // Reckon for the Store what to apply to WAL and MemStore.
        List<Cell> toApply = reckonDeltasByStore(region.stores.get(columnFamilyName), mutation,
          now, deltas, results);
        if (!toApply.isEmpty()) {
          for (Cell cell : toApply) {
            HStore store = region.getStore(cell);
            if (store == null) {
              region.checkFamily(CellUtil.cloneFamily(cell));
            } else {
              ret.computeIfAbsent(store.getColumnFamilyDescriptor().getName(),
                key -> new ArrayList<>()).add(cell);
            }
          }
        }
      }
      return ret;
    }

    /**
     * Reckon the Cells to apply to WAL, memstore, and to return to the Client in passed
     * column family/Store.
     *
     * Does Get of current value and then adds passed in deltas for this Store returning the
     * result.
     *
     * @param mutation The encompassing Mutation object
     * @param deltas Changes to apply to this Store; either increment amount or data to append
     * @param results In here we accumulate all the Cells we are to return to the client. If null,
     *   client doesn't want results returned.
     * @return Resulting Cells after <code>deltas</code> have been applied to current
     *   values. Side effect is our filling out of the <code>results</code> List.
     */
    private List<Cell> reckonDeltasByStore(HStore store, Mutation mutation, long now,
      List<Cell> deltas, List<Cell> results) throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      byte[] columnFamily = store.getColumnFamilyDescriptor().getName();
      List<Pair<Cell, Cell>> cellPairs = new ArrayList<>(deltas.size());

      // Sort the cells so that they match the order that they appear in the Get results.
      // Otherwise, we won't be able to find the existing values if the cells are not specified
      // in order by the client since cells are in an array list.
      deltas.sort(store.getComparator());

      // Get previous values for all columns in this family.
      Get get = new Get(mutation.getRow());
      for (Cell cell: deltas) {
        get.addColumn(columnFamily, CellUtil.cloneQualifier(cell));
      }
      TimeRange tr;
      if (mutation instanceof Increment) {
        tr = ((Increment) mutation).getTimeRange();
      } else {
        tr = ((Append) mutation).getTimeRange();
      }

      if (tr != null) {
        get.setTimeRange(tr.getMin(), tr.getMax());
      }

      try (RegionScanner scanner = region.getScanner(new Scan(get))) {
        // NOTE: Please don't use HRegion.get() instead,
        // because it will copy cells to heap. See HBASE-26036
        List<Cell> currentValues = new ArrayList<>();
        scanner.next(currentValues);
        // Iterate the input columns and update existing values if they were found, otherwise
        // add new column initialized to the delta amount
        int currentValuesIndex = 0;
        for (int i = 0; i < deltas.size(); i++) {
          Cell delta = deltas.get(i);
          Cell currentValue = null;
          if (currentValuesIndex < currentValues.size() && CellUtil
            .matchingQualifier(currentValues.get(currentValuesIndex), delta)) {
            currentValue = currentValues.get(currentValuesIndex);
            if (i < (deltas.size() - 1) && !CellUtil.matchingQualifier(delta, deltas.get(i + 1))) {
              currentValuesIndex++;
            }
          }
          // Switch on whether this an increment or an append building the new Cell to apply.
          Cell newCell;
          if (mutation instanceof Increment) {
            long deltaAmount = getLongValue(delta);
            final long newValue = currentValue == null ? deltaAmount :
              getLongValue(currentValue) + deltaAmount;
            newCell = reckonDelta(delta, currentValue, columnFamily, now, mutation,
              (oldCell) -> Bytes.toBytes(newValue));
          } else {
            newCell = reckonDelta(delta, currentValue, columnFamily, now, mutation,
              (oldCell) -> ByteBuffer.wrap(new byte[delta.getValueLength() +
                oldCell.getValueLength()])
                .put(oldCell.getValueArray(), oldCell.getValueOffset(), oldCell.getValueLength())
                .put(delta.getValueArray(), delta.getValueOffset(), delta.getValueLength())
                .array());
          }
          if (region.maxCellSize > 0) {
            int newCellSize = PrivateCellUtil.estimatedSerializedSizeOf(newCell);
            if (newCellSize > region.maxCellSize) {
              String msg =
                "Cell with size " + newCellSize + " exceeds limit of " + region.maxCellSize +
                  " bytes in region " + this;
              LOG.debug(msg);
              throw new DoNotRetryIOException(msg);
            }
          }
          cellPairs.add(new Pair<>(currentValue, newCell));
          // Add to results to get returned to the Client. If null, cilent does not want results.
          if (results != null) {
            results.add(newCell);
          }
        }
        // Give coprocessors a chance to update the new cells before apply to WAL or memstore
        if (region.coprocessorHost != null) {
          // Here the operation must be increment or append.
          cellPairs = mutation instanceof Increment ?
            region.coprocessorHost.postIncrementBeforeWAL(mutation, cellPairs) :
            region.coprocessorHost.postAppendBeforeWAL(mutation, cellPairs);
        }
      }
      return cellPairs.stream().map(Pair::getSecond).collect(Collectors.toList());
    }

    private static Cell reckonDelta(final Cell delta, final Cell currentCell,
      final byte[] columnFamily, final long now, Mutation mutation,
      Function<Cell, byte[]> supplier) throws IOException {
      // Forward any tags found on the delta.
      List<Tag> tags = TagUtil.carryForwardTags(delta);
      if (currentCell != null) {
        tags = TagUtil.carryForwardTags(tags, currentCell);
        tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
        byte[] newValue = supplier.apply(currentCell);
        return ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(mutation.getRow(), 0, mutation.getRow().length)
          .setFamily(columnFamily, 0, columnFamily.length)
          // copy the qualifier if the cell is located in shared memory.
          .setQualifier(CellUtil.cloneQualifier(delta))
          .setTimestamp(Math.max(currentCell.getTimestamp() + 1, now))
          .setType(KeyValue.Type.Put.getCode())
          .setValue(newValue, 0, newValue.length)
          .setTags(TagUtil.fromList(tags))
          .build();
      } else {
        tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
        PrivateCellUtil.updateLatestStamp(delta, now);
        return CollectionUtils.isEmpty(tags) ? delta : PrivateCellUtil.createCell(delta, tags);
      }
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
      return PrivateCellUtil.getValueAsLong(cell);
    }

    @Override
    public List<Pair<NonceKey, WALEdit>> buildWALEdits(final MiniBatchOperationInProgress<Mutation>
        miniBatchOp) throws IOException {
      List<Pair<NonceKey, WALEdit>> walEdits = super.buildWALEdits(miniBatchOp);
      // for MutationBatchOperation, more than one nonce is not allowed
      if (walEdits.size() > 1) {
        throw new IOException("Found multiple nonce keys per batch!");
      }
      return walEdits;
    }

    @Override
    public WriteEntry writeMiniBatchOperationsToMemStore(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, @Nullable WriteEntry writeEntry)
        throws IOException {
      if (writeEntry == null) {
        writeEntry = region.mvcc.begin();
      }
      super.writeMiniBatchOperationsToMemStore(miniBatchOp, writeEntry.getWriteNumber());
      return writeEntry;
    }

    @Override
    public void completeMiniBatchOperations(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
        throws IOException {
      // TODO: can it be done after completing mvcc?
      // calling the post CP hook for batch mutation
      if (region.coprocessorHost != null) {
        region.coprocessorHost.postBatchMutate(miniBatchOp);
      }
      super.completeMiniBatchOperations(miniBatchOp, writeEntry);

      if (nonce != HConstants.NO_NONCE) {
        if (region.rsServices != null && region.rsServices.getNonceManager() != null) {
          region.rsServices.getNonceManager()
            .addMvccToOperationContext(nonceGroup, nonce, writeEntry.getWriteNumber());
        }
      }
    }

    @Override
    public void doPostOpCleanupForMiniBatch(MiniBatchOperationInProgress<Mutation> miniBatchOp,
        final WALEdit walEdit, boolean success) throws IOException {

      super.doPostOpCleanupForMiniBatch(miniBatchOp, walEdit, success);
      if (miniBatchOp != null) {
        // synced so that the coprocessor contract is adhered to.
        if (region.coprocessorHost != null) {
          visitBatchOperations(false, miniBatchOp.getLastIndexExclusive(), (int i) -> {
            // only for successful puts/deletes/increments/appends
            if (retCodeDetails[i].getOperationStatusCode() == OperationStatusCode.SUCCESS) {
              Mutation m = getMutation(i);
              if (m instanceof Put) {
                region.coprocessorHost.postPut((Put) m, walEdit);
              } else if (m instanceof Delete) {
                region.coprocessorHost.postDelete((Delete) m, walEdit);
              } else if (m instanceof Increment) {
                Result result = region.getCoprocessorHost().postIncrement((Increment) m,
                  results[i], walEdit);
                if (result != results[i]) {
                  retCodeDetails[i] =
                    new OperationStatus(retCodeDetails[i].getOperationStatusCode(), result);
                }
              } else if (m instanceof Append) {
                Result result = region.getCoprocessorHost().postAppend((Append) m, results[i],
                  walEdit);
                if (result != results[i]) {
                  retCodeDetails[i] =
                    new OperationStatus(retCodeDetails[i].getOperationStatusCode(), result);
                }
              }
            }
            return true;
          });
        }

        // For nonce operations
        if (canProceed && nonce != HConstants.NO_NONCE) {
          boolean[] areAllIncrementsAndAppendsSuccessful = new boolean[]{true};
          visitBatchOperations(false, miniBatchOp.getLastIndexExclusive(), (int i) -> {
            Mutation mutation = getMutation(i);
            if (mutation instanceof Increment || mutation instanceof Append) {
              if (retCodeDetails[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
                areAllIncrementsAndAppendsSuccessful[0] = false;
                return false;
              }
            }
            return true;
          });
          endNonceOperation(areAllIncrementsAndAppendsSuccessful[0]);
        }

        // See if the column families were consistent through the whole thing.
        // if they were then keep them. If they were not then pass a null.
        // null will be treated as unknown.
        // Total time taken might be involving Puts, Deletes, Increments and Appends.
        // Split the time for puts and deletes based on the total number of Puts, Deletes,
        // Increments and Appends.
        if (region.metricsRegion != null) {
          if (miniBatchOp.getNumOfPuts() > 0) {
            // There were some Puts in the batch.
            region.metricsRegion.updatePut();
          }
          if (miniBatchOp.getNumOfDeletes() > 0) {
            // There were some Deletes in the batch.
            region.metricsRegion.updateDelete();
          }
          if (miniBatchOp.getNumOfIncrements() > 0) {
            // There were some Increments in the batch.
            region.metricsRegion.updateIncrement();
          }
          if (miniBatchOp.getNumOfAppends() > 0) {
            // There were some Appends in the batch.
            region.metricsRegion.updateAppend();
          }
        }
      }

      if (region.coprocessorHost != null) {
        // call the coprocessor hook to do any finalization steps after the put is done
        region.coprocessorHost.postBatchMutateIndispensably(
            miniBatchOp != null ? miniBatchOp : createMiniBatch(size(), 0), success);
      }
    }

    /**
     * Runs prePut/preDelete/preIncrement/preAppend coprocessor hook for input mutation in a batch
     * @param metrics Array of 2 ints. index 0: count of puts, index 1: count of deletes, index 2:
     *   count of increments and 3: count of appends
     */
    private void callPreMutateCPHook(int index, final WALEdit walEdit, final int[] metrics)
        throws IOException {
      Mutation m = getMutation(index);
      if (m instanceof Put) {
        if (region.coprocessorHost.prePut((Put) m, walEdit)) {
          // pre hook says skip this Put
          // mark as success and skip in doMiniBatchMutation
          metrics[0]++;
          retCodeDetails[index] = OperationStatus.SUCCESS;
        }
      } else if (m instanceof Delete) {
        Delete curDel = (Delete) m;
        if (curDel.getFamilyCellMap().isEmpty()) {
          // handle deleting a row case
          // TODO: prepareDelete() has been called twice, before and after preDelete() CP hook.
          // Can this be avoided?
          region.prepareDelete(curDel);
        }
        if (region.coprocessorHost.preDelete(curDel, walEdit)) {
          // pre hook says skip this Delete
          // mark as success and skip in doMiniBatchMutation
          metrics[1]++;
          retCodeDetails[index] = OperationStatus.SUCCESS;
        }
      } else if (m instanceof Increment) {
        Increment increment = (Increment) m;
        Result result = region.coprocessorHost.preIncrement(increment, walEdit);
        if (result != null) {
          // pre hook says skip this Increment
          // mark as success and skip in doMiniBatchMutation
          metrics[2]++;
          retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
        }
      } else if (m instanceof Append) {
        Append append = (Append) m;
        Result result = region.coprocessorHost.preAppend(append, walEdit);
        if (result != null) {
          // pre hook says skip this Append
          // mark as success and skip in doMiniBatchMutation
          metrics[3]++;
          retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
        }
      } else {
        String msg = "Put/Delete/Increment/Append mutations only supported in a batch";
        retCodeDetails[index] = new OperationStatus(OperationStatusCode.FAILURE, msg);
        if (isAtomic()) { // fail, atomic means all or none
          throw new IOException(msg);
        }
      }
    }

    // TODO Support Increment/Append operations
    private void checkAndMergeCPMutations(final MiniBatchOperationInProgress<Mutation> miniBatchOp,
        final List<RowLock> acquiredRowLocks, final long timestamp) throws IOException {
      visitBatchOperations(true, nextIndexToProcess + miniBatchOp.size(), (int i) -> {
        // we pass (i - firstIndex) below since the call expects a relative index
        Mutation[] cpMutations = miniBatchOp.getOperationsFromCoprocessors(i - nextIndexToProcess);
        if (cpMutations == null) {
          return true;
        }
        // Else Coprocessor added more Mutations corresponding to the Mutation at this index.
        Mutation mutation = getMutation(i);
        for (Mutation cpMutation : cpMutations) {
          this.checkAndPrepareMutation(cpMutation, timestamp);

          // Acquire row locks. If not, the whole batch will fail.
          acquiredRowLocks.add(region.getRowLock(cpMutation.getRow(), true, null));

          // Returned mutations from coprocessor correspond to the Mutation at index i. We can
          // directly add the cells from those mutations to the familyMaps of this mutation.
          Map<byte[], List<Cell>> cpFamilyMap = cpMutation.getFamilyCellMap();
          region.rewriteCellTags(cpFamilyMap, mutation);
          // will get added to the memStore later
          mergeFamilyMaps(familyCellMaps[i], cpFamilyMap);

          // The durability of returned mutation is replaced by the corresponding mutation.
          // If the corresponding mutation contains the SKIP_WAL, we shouldn't count the
          // cells of returned mutation.
          if (region.getEffectiveDurability(mutation.getDurability()) != Durability.SKIP_WAL) {
            for (List<Cell> cells : cpFamilyMap.values()) {
              miniBatchOp.addCellCount(cells.size());
            }
          }
        }
        return true;
      });
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
  }

  /**
   * Batch of mutations for replay. Base class is shared with {@link MutationBatchOperation} as most
   * of the logic is same.
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Now we will not use this operation to apply
   *             edits at secondary replica side.
   */
  @Deprecated
  private static final class ReplayBatchOperation extends BatchOperation<MutationReplay> {

    private long origLogSeqNum = 0;

    public ReplayBatchOperation(final HRegion region, MutationReplay[] operations,
      long origLogSeqNum) {
      super(region, operations);
      this.origLogSeqNum = origLogSeqNum;
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
      return null;
    }

    @Override
    public boolean isInReplay() {
      return true;
    }

    @Override
    public long getOrigLogSeqNum() {
      return this.origLogSeqNum;
    }

    @Override
    public void startRegionOperation() throws IOException {
      region.startRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    }

    @Override
    public void closeRegionOperation() throws IOException {
      region.closeRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    }

    /**
     * During replay, there could exist column families which are removed between region server
     * failure and replay
     */
    @Override
    protected void checkAndPreparePut(Put p) throws IOException {
      Map<byte[], List<Cell>> familyCellMap = p.getFamilyCellMap();
      List<byte[]> nonExistentList = null;
      for (byte[] family : familyCellMap.keySet()) {
        if (!region.htableDescriptor.hasColumnFamily(family)) {
          if (nonExistentList == null) {
            nonExistentList = new ArrayList<>();
          }
          nonExistentList.add(family);
        }
      }
      if (nonExistentList != null) {
        for (byte[] family : nonExistentList) {
          // Perhaps schema was changed between crash and replay
          LOG.info("No family for {} omit from reply in region {}.", Bytes.toString(family), this);
          familyCellMap.remove(family);
        }
      }
    }

    @Override
    public void checkAndPrepare() throws IOException {
      long now = EnvironmentEdgeManager.currentTime();
      visitBatchOperations(true, this.size(), (int index) -> {
        checkAndPrepareMutation(index, now);
        return true;
      });
    }

    @Override
    public void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
        long timestamp, final List<RowLock> acquiredRowLocks) throws IOException {
      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        // update cell count
        for (List<Cell> cells : getMutation(index).getFamilyCellMap().values()) {
          miniBatchOp.addCellCount(cells.size());
        }
        return true;
      });
    }

    @Override
    public WriteEntry writeMiniBatchOperationsToMemStore(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
        throws IOException {
      super.writeMiniBatchOperationsToMemStore(miniBatchOp, getOrigLogSeqNum());
      return writeEntry;
    }

    @Override
    public void completeMiniBatchOperations(
        final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
        throws IOException {
      super.completeMiniBatchOperations(miniBatchOp, writeEntry);
      region.mvcc.advanceTo(getOrigLogSeqNum());
    }
  }

  public OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic, long nonceGroup,
    long nonce) throws IOException {
    // As it stands, this is used for 3 things
    // * batchMutate with single mutation - put/delete/increment/append, separate or from
    // checkAndMutate.
    // * coprocessor calls (see ex. BulkDeleteEndpoint).
    // So nonces are not really ever used by HBase. They could be by coprocs, and checkAnd...
    return batchMutate(new MutationBatchOperation(this, mutations, atomic, nonceGroup, nonce));
  }

  @Override
  public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException {
    // If the mutations has any Increment/Append operations, we need to do batchMutate atomically
    boolean atomic =
      Arrays.stream(mutations).anyMatch(m -> m instanceof Increment || m instanceof Append);
    return batchMutate(mutations, atomic);
  }

  OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic) throws IOException {
    return TraceUtil.trace(
      () -> batchMutate(mutations, atomic, HConstants.NO_NONCE, HConstants.NO_NONCE),
      () -> createRegionSpan("Region.batchMutate"));
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Now we use
   *             {@link #replayWALEntry(WALEntry, CellScanner)} for replaying edits at secondary
   *             replica side.
   */
  @Deprecated
  OperationStatus[] batchReplay(MutationReplay[] mutations, long replaySeqId) throws IOException {
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
    return batchMutate(new ReplayBatchOperation(this, mutations, replaySeqId));
  }

  /**
   * Perform a batch of mutations.
   * <p/>
   * Operations in a batch are stored with highest durability specified of for all operations in a
   * batch, except for {@link Durability#SKIP_WAL}.
   * <p/>
   * This function is called from {@link #batchReplay(WALSplitUtil.MutationReplay[], long)} with
   * {@link ReplayBatchOperation} instance and {@link #batchMutate(Mutation[])} with
   * {@link MutationBatchOperation} instance as an argument. As the processing of replay batch and
   * mutation batch is very similar, lot of code is shared by providing generic methods in base
   * class {@link BatchOperation}. The logic for this method and
   * {@link #doMiniBatchMutate(BatchOperation)} is implemented using methods in base class which are
   * overridden by derived classes to implement special behavior.
   * @param batchOp contains the list of mutations
   * @return an array of OperationStatus which internally contains the OperationStatusCode and the
   *         exceptionMessage if any.
   * @throws IOException if an IO problem is encountered
   */
  private OperationStatus[] batchMutate(BatchOperation<?> batchOp) throws IOException {
    boolean initialized = false;
    batchOp.startRegionOperation();
    try {
      while (!batchOp.isDone()) {
        if (!batchOp.isInReplay()) {
          checkReadOnly();
        }
        checkResources();

        if (!initialized) {
          this.writeRequestsCount.add(batchOp.size());
          // validate and prepare batch for write, for MutationBatchOperation it also calls CP
          // prePut()/preDelete()/preIncrement()/preAppend() hooks
          batchOp.checkAndPrepare();
          initialized = true;
        }
        doMiniBatchMutate(batchOp);
        requestFlushIfNeeded();
      }
    } finally {
      if (rsServices != null && rsServices.getMetrics() != null) {
        rsServices.getMetrics().updateWriteQueryMeter(this.htableDescriptor.
          getTableName(), batchOp.size());
      }
      batchOp.closeRegionOperation();
    }
    return batchOp.retCodeDetails;
  }

  /**
   * Called to do a piece of the batch that came in to {@link #batchMutate(Mutation[])}
   * In here we also handle replay of edits on region recover. Also gets change in size brought
   * about by applying {@code batchOp}.
   */
  private void doMiniBatchMutate(BatchOperation<?> batchOp) throws IOException {
    boolean success = false;
    WALEdit walEdit = null;
    WriteEntry writeEntry = null;
    boolean locked = false;
    // We try to set up a batch in the range [batchOp.nextIndexToProcess,lastIndexExclusive)
    MiniBatchOperationInProgress<Mutation> miniBatchOp = null;
    /** Keep track of the locks we hold so we can release them in finally clause */
    List<RowLock> acquiredRowLocks = Lists.newArrayListWithCapacity(batchOp.size());

    // Check for thread interrupt status in case we have been signaled from
    // #interruptRegionOperation.
    checkInterrupt();

    try {
      // STEP 1. Try to acquire as many locks as we can and build mini-batch of operations with
      // locked rows
      miniBatchOp = batchOp.lockRowsAndBuildMiniBatch(acquiredRowLocks);

      // We've now grabbed as many mutations off the list as we can
      // Ensure we acquire at least one.
      if (miniBatchOp.getReadyToWriteCount() <= 0) {
        // Nothing to put/delete/increment/append -- an exception in the above such as
        // NoSuchColumnFamily?
        return;
      }

      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation. Do it before we take the lock and disable interrupts for
      // the WAL append.
      checkInterrupt();

      lock(this.updatesLock.readLock(), miniBatchOp.getReadyToWriteCount());
      locked = true;

      // From this point until memstore update this operation should not be interrupted.
      disableInterrupts();

      // STEP 2. Update mini batch of all operations in progress with LATEST_TIMESTAMP timestamp
      // We should record the timestamp only after we have acquired the rowLock,
      // otherwise, newer puts/deletes/increment/append are not guaranteed to have a newer
      // timestamp

      long now = EnvironmentEdgeManager.currentTime();
      batchOp.prepareMiniBatchOperations(miniBatchOp, now, acquiredRowLocks);

      // STEP 3. Build WAL edit

      List<Pair<NonceKey, WALEdit>> walEdits = batchOp.buildWALEdits(miniBatchOp);

      // STEP 4. Append the WALEdits to WAL and sync.

      for(Iterator<Pair<NonceKey, WALEdit>> it = walEdits.iterator(); it.hasNext();) {
        Pair<NonceKey, WALEdit> nonceKeyWALEditPair = it.next();
        walEdit = nonceKeyWALEditPair.getSecond();
        NonceKey nonceKey = nonceKeyWALEditPair.getFirst();

        if (walEdit != null && !walEdit.isEmpty()) {
          writeEntry = doWALAppend(walEdit, batchOp.durability, batchOp.getClusterIds(), now,
              nonceKey.getNonceGroup(), nonceKey.getNonce(), batchOp.getOrigLogSeqNum());
        }

        // Complete mvcc for all but last writeEntry (for replay case)
        if (it.hasNext() && writeEntry != null) {
          mvcc.complete(writeEntry);
          writeEntry = null;
        }
      }

      // STEP 5. Write back to memStore
      // NOTE: writeEntry can be null here
      writeEntry = batchOp.writeMiniBatchOperationsToMemStore(miniBatchOp, writeEntry);

      // STEP 6. Complete MiniBatchOperations: If required calls postBatchMutate() CP hook and
      // complete mvcc for last writeEntry
      batchOp.completeMiniBatchOperations(miniBatchOp, writeEntry);
      writeEntry = null;
      success = true;
    } finally {
      // Call complete rather than completeAndWait because we probably had error if walKey != null
      if (writeEntry != null) mvcc.complete(writeEntry);

      if (locked) {
        this.updatesLock.readLock().unlock();
      }
      releaseRowLocks(acquiredRowLocks);

      enableInterrupts();

      final int finalLastIndexExclusive =
          miniBatchOp != null ? miniBatchOp.getLastIndexExclusive() : batchOp.size();
      final boolean finalSuccess = success;
      batchOp.visitBatchOperations(true, finalLastIndexExclusive,
        (int i) -> {
          Mutation mutation = batchOp.getMutation(i);
          if (mutation instanceof Increment || mutation instanceof Append) {
            if (finalSuccess) {
              batchOp.retCodeDetails[i] = new OperationStatus(OperationStatusCode.SUCCESS,
                batchOp.results[i]);
            } else {
              batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
            }
          } else {
            batchOp.retCodeDetails[i] =
              finalSuccess ? OperationStatus.SUCCESS : OperationStatus.FAILURE;
          }
          return true;
        });

      batchOp.doPostOpCleanupForMiniBatch(miniBatchOp, walEdit, finalSuccess);

      batchOp.nextIndexToProcess = finalLastIndexExclusive;
    }
  }

  /**
   * Returns effective durability from the passed durability and
   * the table descriptor.
   */
  private Durability getEffectiveDurability(Durability d) {
    return d == Durability.USE_DEFAULT ? this.regionDurability : d;
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation) throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(row)
        .ifMatches(family, qualifier, op, comparator.getValue()).timeRange(timeRange);
      if (mutation instanceof Put) {
        checkAndMutate = builder.build((Put) mutation);
      } else if (mutation instanceof Delete) {
        checkAndMutate = builder.build((Delete) mutation);
      } else {
        throw new DoNotRetryIOException("Unsupported mutate type: " + mutation.getClass()
          .getSimpleName().toUpperCase());
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, Filter filter, TimeRange timeRange, Mutation mutation)
    throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(row).ifMatches(filter)
        .timeRange(timeRange);
      if (mutation instanceof Put) {
        checkAndMutate = builder.build((Put) mutation);
      } else if (mutation instanceof Delete) {
        checkAndMutate = builder.build((Delete) mutation);
      } else {
        throw new DoNotRetryIOException("Unsupported mutate type: " + mutation.getClass()
          .getSimpleName().toUpperCase());
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    ByteArrayComparable comparator, TimeRange timeRange, RowMutations rm) throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      checkAndMutate = CheckAndMutate.newBuilder(row)
        .ifMatches(family, qualifier, op, comparator.getValue()).timeRange(timeRange).build(rm);
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndRowMutate(byte[] row, Filter filter, TimeRange timeRange, RowMutations rm)
    throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      checkAndMutate = CheckAndMutate.newBuilder(row).ifMatches(filter).timeRange(timeRange)
        .build(rm);
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    return checkAndMutate(checkAndMutate, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate, long nonceGroup,
    long nonce) throws IOException {
    return TraceUtil.trace(() -> checkAndMutateInternal(checkAndMutate, nonceGroup, nonce),
      () -> createRegionSpan("Region.checkAndMutate"));
  }

  private CheckAndMutateResult checkAndMutateInternal(CheckAndMutate checkAndMutate,
    long nonceGroup, long nonce) throws IOException {
    byte[] row = checkAndMutate.getRow();
    Filter filter = null;
    byte[] family = null;
    byte[] qualifier = null;
    CompareOperator op = null;
    ByteArrayComparable comparator = null;
    if (checkAndMutate.hasFilter()) {
      filter = checkAndMutate.getFilter();
    } else {
      family = checkAndMutate.getFamily();
      qualifier = checkAndMutate.getQualifier();
      op = checkAndMutate.getCompareOp();
      comparator = new BinaryComparator(checkAndMutate.getValue());
    }
    TimeRange timeRange = checkAndMutate.getTimeRange();

    Mutation mutation = null;
    RowMutations rowMutations = null;
    if (checkAndMutate.getAction() instanceof Mutation) {
      mutation = (Mutation) checkAndMutate.getAction();
    } else {
      rowMutations = (RowMutations) checkAndMutate.getAction();
    }

    if (mutation != null) {
      checkMutationType(mutation);
      checkRow(mutation, row);
    } else {
      checkRow(rowMutations, row);
    }
    checkReadOnly();
    // TODO, add check for value length also move this check to the client
    checkResources();
    startRegionOperation();
    try {
      Get get = new Get(row);
      if (family != null) {
        checkFamily(family);
        get.addColumn(family, qualifier);
      }
      if (filter != null) {
        get.setFilter(filter);
      }
      if (timeRange != null) {
        get.setTimeRange(timeRange.getMin(), timeRange.getMax());
      }
      // Lock row - note that doBatchMutate will relock this row if called
      checkRow(row, "doCheckAndRowMutate");
      RowLock rowLock = getRowLock(get.getRow(), false, null);
      try {
        if (this.getCoprocessorHost() != null) {
          CheckAndMutateResult result =
            getCoprocessorHost().preCheckAndMutateAfterRowLock(checkAndMutate);
          if (result != null) {
            return result;
          }
        }

        // NOTE: We used to wait here until mvcc caught up: mvcc.await();
        // Supposition is that now all changes are done under row locks, then when we go to read,
        // we'll get the latest on this row.
        boolean matches = false;
        long cellTs = 0;
        try (RegionScanner scanner = getScanner(new Scan(get))) {
          // NOTE: Please don't use HRegion.get() instead,
          // because it will copy cells to heap. See HBASE-26036
          List<Cell> result = new ArrayList<>(1);
          scanner.next(result);
          if (filter != null) {
            if (!result.isEmpty()) {
              matches = true;
              cellTs = result.get(0).getTimestamp();
            }
          } else {
            boolean valueIsNull =
              comparator.getValue() == null || comparator.getValue().length == 0;
            if (result.isEmpty() && valueIsNull) {
              matches = op != CompareOperator.NOT_EQUAL;
            } else if (result.size() > 0 && valueIsNull) {
              matches = (result.get(0).getValueLength() == 0) == (op != CompareOperator.NOT_EQUAL);
              cellTs = result.get(0).getTimestamp();
            } else if (result.size() == 1) {
              Cell kv = result.get(0);
              cellTs = kv.getTimestamp();
              int compareResult = PrivateCellUtil.compareValue(kv, comparator);
              matches = matches(op, compareResult);
            }
          }
        }

        // If matches, perform the mutation or the rowMutations
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
            for (Mutation m : rowMutations.getMutations()) {
              if (m instanceof Put) {
                updateCellTimestamps(m.getFamilyCellMap().values(), byteTs);
              }
            }
            // And else 'delete' is not needed since it already does a second get, and sets the
            // timestamp from get (see prepareDeleteTimestamps).
          }
          // All edits for the given row (across all column families) must happen atomically.
          Result r;
          if (mutation != null) {
            r = mutate(mutation, true, nonceGroup, nonce).getResult();
          } else {
            r = mutateRow(rowMutations, nonceGroup, nonce);
          }
          this.checkAndMutateChecksPassed.increment();
          return new CheckAndMutateResult(true, r);
        }
        this.checkAndMutateChecksFailed.increment();
        return new CheckAndMutateResult(false, null);
      } finally {
        rowLock.release();
      }
    } finally {
      closeRegionOperation();
    }
  }

  private void checkMutationType(final Mutation mutation) throws DoNotRetryIOException {
    if (!(mutation instanceof Put) && !(mutation instanceof Delete) &&
      !(mutation instanceof Increment) && !(mutation instanceof Append)) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException(
        "Action must be Put or Delete or Increment or Delete");
    }
  }

  private void checkRow(final Row action, final byte[] row)
    throws DoNotRetryIOException {
    if (!Bytes.equals(row, action.getRow())) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action's getRow must match");
    }
  }

  private boolean matches(final CompareOperator op, final int compareResult) {
    boolean matches = false;
    switch (op) {
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
        throw new RuntimeException("Unknown Compare op " + op.name());
    }
    return matches;
  }

  private OperationStatus mutate(Mutation mutation) throws IOException {
    return mutate(mutation, false);
  }

  private OperationStatus mutate(Mutation mutation, boolean atomic) throws IOException {
    return mutate(mutation, atomic, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private OperationStatus mutate(Mutation mutation, boolean atomic, long nonceGroup, long nonce)
    throws IOException {
    OperationStatus[] status =
      this.batchMutate(new Mutation[] { mutation }, atomic, nonceGroup, nonce);
    if (status[0].getOperationStatusCode().equals(OperationStatusCode.SANITY_CHECK_FAILURE)) {
      throw new FailedSanityCheckException(status[0].getExceptionMsg());
    } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.BAD_FAMILY)) {
      throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
    } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.STORE_TOO_BUSY)) {
      throw new RegionTooBusyException(status[0].getExceptionMsg());
    }
    return status[0];
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
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir, conf);

    SnapshotManifest manifest = SnapshotManifest.create(conf, getFilesystem(),
            snapshotDir, desc, exnSnare);
    manifest.addRegion(this);
  }

  private void updateSequenceId(final Iterable<List<Cell>> cellItr, final long sequenceId)
      throws IOException {
    for (List<Cell> cells: cellItr) {
      if (cells == null) return;
      for (Cell cell : cells) {
        PrivateCellUtil.setSequenceId(cell, sequenceId);
      }
    }
  }

  /**
   * Replace any cell timestamps set to {@link org.apache.hadoop.hbase.HConstants#LATEST_TIMESTAMP}
   * provided current timestamp.
   * @param cellItr
   * @param now
   */
  private static void updateCellTimestamps(final Iterable<List<Cell>> cellItr, final byte[] now)
      throws IOException {
    for (List<Cell> cells: cellItr) {
      if (cells == null) continue;
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
        PrivateCellUtil.updateLatestStamp(cells.get(i), now);
      }
    }
  }

  /**
   * Possibly rewrite incoming cell tags.
   */
  private void rewriteCellTags(Map<byte[], List<Cell>> familyMap, final Mutation m) {
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
        cells.set(i, PrivateCellUtil.createCell(cell, newTags));
      }
    }
  }

  /**
   * Check if resources to support an update.
   * <p/>
   * We throw RegionTooBusyException if above memstore limit and expect client to retry using some
   * kind of backoff
   */
  private void checkResources() throws RegionTooBusyException {
    // If catalog region, do not impose resource constraints or block updates.
    if (this.getRegionInfo().isMetaRegion()) {
      return;
    }

    MemStoreSize mss = this.memStoreSizing.getMemStoreSize();
    if (mss.getHeapSize() + mss.getOffHeapSize() > this.blockingMemStoreSize) {
      blockedRequestsCount.increment();
      requestFlush();
      // Don't print current limit because it will vary too much. The message is used as a key
      // over in RetriesExhaustedWithDetailsException processing.
      final String regionName =
        this.getRegionInfo() == null ? "unknown" : this.getRegionInfo().getEncodedName();
      final String serverName = this.getRegionServerServices() == null ?
        "unknown" : (this.getRegionServerServices().getServerName() == null ? "unknown" :
          this.getRegionServerServices().getServerName().toString());
      RegionTooBusyException rtbe = new RegionTooBusyException(
        "Over memstore limit=" + org.apache.hadoop.hbase.procedure2.util.StringUtils
          .humanSize(this.blockingMemStoreSize) + ", regionName=" + regionName + ", server="
          + serverName);
      LOG.warn("Region is too busy due to exceeding memstore size limit.", rtbe);
      throw rtbe;
    }
  }

  /**
   * @throws IOException Throws exception if region is in read-only mode.
   */
  private void checkReadOnly() throws IOException {
    if (isReadOnly()) {
      throw new DoNotRetryIOException("region is read only");
    }
  }

  private void checkReadsEnabled() throws IOException {
    if (!this.writestate.readsEnabled) {
      throw new IOException(getRegionInfo().getEncodedName()
        + ": The region's reads are disabled. Cannot serve the request");
    }
  }

  public void setReadsEnabled(boolean readsEnabled) {
   if (readsEnabled && !this.writestate.readsEnabled) {
     LOG.info("Enabling reads for {}", getRegionInfo().getEncodedName());
    }
    this.writestate.setReadsEnabled(readsEnabled);
  }

  /**
   * @param delta If we are doing delta changes -- e.g. increment/append -- then this flag will be
   *          set; when set we will run operations that make sense in the increment/append scenario
   *          but that do not make sense otherwise.
   */
  private void applyToMemStore(HStore store, List<Cell> cells, boolean delta,
      MemStoreSizing memstoreAccounting) throws IOException {
    // Any change in how we update Store/MemStore needs to also be done in other applyToMemStore!!!!
    boolean upsert = delta && store.getColumnFamilyDescriptor().getMaxVersions() == 1;
    if (upsert) {
      store.upsert(cells, getSmallestReadPoint(), memstoreAccounting);
    } else {
      store.add(cells, memstoreAccounting);
    }
  }

  private void checkFamilies(Collection<byte[]> families, Durability durability)
      throws NoSuchColumnFamilyException, InvalidMutationDurabilityException {
    for (byte[] family : families) {
      checkFamily(family, durability);
    }
  }

  private void checkFamily(final byte[] family, Durability durability)
      throws NoSuchColumnFamilyException, InvalidMutationDurabilityException {
    checkFamily(family);
    if (durability.equals(Durability.SKIP_WAL)
        && htableDescriptor.getColumnFamily(family).getScope()
        != HConstants.REPLICATION_SCOPE_LOCAL) {
      throw new InvalidMutationDurabilityException(
          "Mutation's durability is SKIP_WAL but table's column family " + Bytes.toString(family)
              + " need replication");
    }
  }

  private void checkFamily(final byte[] family) throws NoSuchColumnFamilyException {
    if (!this.htableDescriptor.hasColumnFamily(family)) {
      throw new NoSuchColumnFamilyException(
          "Column family " + Bytes.toString(family) + " does not exist in region " + this
              + " in table " + this.htableDescriptor);
    }
  }

  /**
   * Check the collection of families for valid timestamps
   * @param familyMap
   * @param now current timestamp
   * @throws FailedSanityCheckException
   */
  public void checkTimestamps(final Map<byte[], List<Cell>> familyMap, long now)
      throws FailedSanityCheckException {
    if (timestampSlop == HConstants.LATEST_TIMESTAMP) {
      return;
    }
    long maxTs = now + timestampSlop;
    for (List<Cell> kvs : familyMap.values()) {
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
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

  /*
   * @param size
   * @return True if size is over the flush threshold
   */
  private boolean isFlushSize(MemStoreSize size) {
    return size.getHeapSize() + size.getOffHeapSize() > getMemStoreFlushSize();
  }

  private void deleteRecoveredEdits(FileSystem fs, Iterable<Path> files) throws IOException {
    for (Path file : files) {
      if (!fs.delete(file, false)) {
        LOG.error("Failed delete of {}", file);
      } else {
        LOG.debug("Deleted recovered.edits file={}", file);
      }
    }
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
   */
  long replayRecoveredEditsIfAny(Map<byte[], Long> maxSeqIdInStores,
    final CancelableProgressable reporter, final MonitoredTask status) throws IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqId = minSeqIdForTheRegion;
    String specialRecoveredEditsDirStr = conf.get(SPECIAL_RECOVERED_EDITS_DIR);
    if (org.apache.commons.lang3.StringUtils.isBlank(specialRecoveredEditsDirStr)) {
      FileSystem walFS = getWalFileSystem();
      FileSystem rootFS = getFilesystem();
      Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(conf, getRegionInfo().getTable(),
        getRegionInfo().getEncodedName());
      Path regionWALDir = getWALRegionDir();
      Path regionDir =
        FSUtils.getRegionDirFromRootDir(CommonFSUtils.getRootDir(conf), getRegionInfo());

      // We made a mistake in HBASE-20734 so we need to do this dirty hack...
      NavigableSet<Path> filesUnderWrongRegionWALDir =
        WALSplitUtil.getSplitEditFilesSorted(walFS, wrongRegionWALDir);
      seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS,
        filesUnderWrongRegionWALDir, reporter, regionDir));
      // This is to ensure backwards compatability with HBASE-20723 where recovered edits can appear
      // under the root dir even if walDir is set.
      NavigableSet<Path> filesUnderRootDir = Collections.emptyNavigableSet();
      if (!regionWALDir.equals(regionDir)) {
        filesUnderRootDir = WALSplitUtil.getSplitEditFilesSorted(rootFS, regionDir);
        seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, rootFS,
          filesUnderRootDir, reporter, regionDir));
      }

      NavigableSet<Path> files = WALSplitUtil.getSplitEditFilesSorted(walFS, regionWALDir);
      seqId = Math.max(seqId,
        replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS, files, reporter, regionWALDir));
      if (seqId > minSeqIdForTheRegion) {
        // Then we added some edits to memory. Flush and cleanup split edit files.
        internalFlushcache(null, seqId, stores.values(), status, false,
          FlushLifeCycleTracker.DUMMY);
      }
      // Now delete the content of recovered edits. We're done w/ them.
      if (files.size() > 0 && this.conf.getBoolean("hbase.region.archive.recovered.edits", false)) {
        // For debugging data loss issues!
        // If this flag is set, make use of the hfile archiving by making recovered.edits a fake
        // column family. Have to fake out file type too by casting our recovered.edits as
        // storefiles
        String fakeFamilyName = WALSplitUtil.getRegionDirRecoveredEditsDir(regionWALDir).getName();
        Set<HStoreFile> fakeStoreFiles = new HashSet<>(files.size());
        for (Path file : files) {
          fakeStoreFiles.add(new HStoreFile(walFS, file, this.conf, null, null, true));
        }
        getRegionWALFileSystem().archiveRecoveredEdits(fakeFamilyName, fakeStoreFiles);
      } else {
        deleteRecoveredEdits(walFS, Iterables.concat(files, filesUnderWrongRegionWALDir));
        deleteRecoveredEdits(rootFS, filesUnderRootDir);
      }
    } else {
      Path recoveredEditsDir = new Path(specialRecoveredEditsDirStr);
      FileSystem fs = recoveredEditsDir.getFileSystem(conf);
      FileStatus[] files = fs.listStatus(recoveredEditsDir);
      LOG.debug("Found {} recovered edits file(s) under {}", files == null ? 0 : files.length,
        recoveredEditsDir);
      if (files != null) {
        for (FileStatus file : files) {
          // it is safe to trust the zero-length in this case because we've been through rename and
          // lease recovery in the above.
          if (isZeroLengthThenDelete(fs, file, file.getPath())) {
            continue;
          }
          seqId =
            Math.max(seqId, replayRecoveredEdits(file.getPath(), maxSeqIdInStores, reporter, fs));
        }
      }
      if (seqId > minSeqIdForTheRegion) {
        // Then we added some edits to memory. Flush and cleanup split edit files.
        internalFlushcache(null, seqId, stores.values(), status, false,
          FlushLifeCycleTracker.DUMMY);
      }
      deleteRecoveredEdits(fs,
        Stream.of(files).map(FileStatus::getPath).collect(Collectors.toList()));
    }

    return seqId;
  }

  private long replayRecoveredEditsForPaths(long minSeqIdForTheRegion, FileSystem fs,
      final NavigableSet<Path> files, final CancelableProgressable reporter, final Path regionDir)
      throws IOException {
    long seqid = minSeqIdForTheRegion;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + (files == null ? 0 : files.size())
          + " recovered edits file(s) under " + regionDir);
    }

    if (files == null || files.isEmpty()) {
      return minSeqIdForTheRegion;
    }

    for (Path edits: files) {
      if (edits == null || !fs.exists(edits)) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      if (isZeroLengthThenDelete(fs, fs.getFileStatus(edits), edits)) {
        continue;
      }

      long maxSeqId;
      String fileName = edits.getName();
      maxSeqId = Math.abs(Long.parseLong(fileName));
      if (maxSeqId <= minSeqIdForTheRegion) {
        if (LOG.isDebugEnabled()) {
          String msg = "Maximum sequenceid for this wal is " + maxSeqId
              + " and minimum sequenceid for the region " + this + "  is " + minSeqIdForTheRegion
              + ", skipped the whole file, path=" + edits;
          LOG.debug(msg);
        }
        continue;
      }

      try {
        // replay the edits. Replay can return -1 if everything is skipped, only update
        // if seqId is greater
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter, fs));
      } catch (IOException e) {
        handleException(fs, edits, e);
      }
    }
    return seqid;
  }

  private void handleException(FileSystem fs, Path edits, IOException e) throws IOException {
    boolean skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
      conf.getBoolean("hbase.skip.errors", HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS));
    if (conf.get("hbase.skip.errors") != null) {
      LOG.warn("The property 'hbase.skip.errors' has been deprecated. Please use "
          + HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + " instead.");
    }
    if (skipErrors) {
      Path p = WALSplitUtil.moveAsideBadEditsFile(fs, edits);
      LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + "=true so continuing. Renamed "
          + edits + " as " + p,
        e);
    } else {
      throw e;
    }
  }

  /**
   * @param edits File of recovered edits.
   * @param maxSeqIdInStores Maximum sequenceid found in each store. Edits in wal must be larger
   *          than this to be replayed for each store.
   * @return the sequence id of the last edit added to this region out of the recovered edits log or
   *         <code>minSeqId</code> if nothing added from editlogs.
   */
  private long replayRecoveredEdits(final Path edits, Map<byte[], Long> maxSeqIdInStores,
    final CancelableProgressable reporter, FileSystem fs) throws IOException {
    String msg = "Replaying edits from " + edits;
    LOG.info(msg);
    MonitoredTask status = TaskMonitor.get().createStatus(msg);

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
                  msg = "Progressable reporter failed, stopping replay for region " + this;
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
            firstSeqIdInLog = key.getSequenceId();
          }
          if (currentEditSeqId > key.getSequenceId()) {
            // when this condition is true, it means we have a serious defect because we need to
            // maintain increasing SeqId for WAL edits per region
            LOG.error(getRegionInfo().getEncodedName() + " : "
                 + "Found decreasing SeqId. PreId=" + currentEditSeqId + " key=" + key
                + "; edit=" + val);
          } else {
            currentEditSeqId = key.getSequenceId();
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
          MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
          for (Cell cell: val.getCells()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (WALEdit.isMetaEditFamily(cell)) {
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
            if (store == null || !CellUtil.matchingFamily(cell,
                store.getColumnFamilyDescriptor().getName())) {
              store = getStore(cell);
            }
            if (store == null) {
              // This should never happen.  Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for cell {} in region {}", cell, this);
              skippedEdits++;
              continue;
            }
            if (checkRowWithinBoundary && !rowIsInRange(this.getRegionInfo(),
              cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())) {
              LOG.warn("Row of {} is not within region boundary for region {}", cell, this);
              skippedEdits++;
              continue;
            }
            // Now, figure if we should skip this edit.
            if (key.getSequenceId() <= maxSeqIdInStores.get(store.getColumnFamilyDescriptor()
                .getName())) {
              skippedEdits++;
              continue;
            }
            PrivateCellUtil.setSequenceId(cell, currentReplaySeqId);

            restoreEdit(store, cell, memStoreSizing);
            editsCount++;
          }
          MemStoreSize mss = memStoreSizing.getMemStoreSize();
          incMemStoreSize(mss);
          flush = isFlushSize(this.memStoreSizing.getMemStoreSize());
          if (flush) {
            internalFlushcache(null, currentEditSeqId, stores.values(), status, false,
              FlushLifeCycleTracker.DUMMY);
          }

          if (coprocessorHost != null) {
            coprocessorHost.postWALRestore(this.getRegionInfo(), key, val);
          }
        }

        if (coprocessorHost != null) {
          coprocessorHost.postReplayWALs(this.getRegionInfo(), edits);
        }
      } catch (EOFException eof) {
        Path p = WALSplitUtil.moveAsideBadEditsFile(walFS, edits);
        msg = "EnLongAddered EOF. Most likely due to Master failure during "
            + "wal splitting, so we have this data in another edit. Continuing, but renaming "
            + edits + " as " + p + " for region " + this;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitUtil.moveAsideBadEditsFile(walFS, edits);
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
        HStore store = this.getStore(compaction.getFamilyName().toByteArray());
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

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
  void replayWALFlushMarker(FlushDescriptor flush, long replaySeqId) throws IOException {
    checkTargetRegion(flush.getEncodedRegionName().toByteArray(), "Flush marker from WAL ", flush);

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

  private Collection<HStore> getStoresToFlush(FlushDescriptor flushDesc) {
    List<HStore> storesToFlush = new ArrayList<>();
    for (StoreFlushDescriptor storeFlush : flushDesc.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      HStore store = getStore(family);
      if (store == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : " +
          "Received a flush start marker from primary, but the family is not found. Ignoring" +
          " StoreFlushDescriptor:" + TextFormat.shortDebugString(storeFlush));
        continue;
      }
      storesToFlush.add(store);
    }
    return storesToFlush;
  }

  /**
   * Replay the flush marker from primary region by creating a corresponding snapshot of the store
   * memstores, only if the memstores do not have a higher seqId from an earlier wal edit (because
   * the events may be coming out of order).
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
  PrepareFlushResult replayWALFlushStartMarker(FlushDescriptor flush) throws IOException {
    long flushSeqId = flush.getFlushSequenceNumber();

    Collection<HStore> storesToFlush = getStoresToFlush(flush);

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
          PrepareFlushResult prepareResult = internalPrepareFlushCache(null, flushSeqId,
            storesToFlush, status, false, FlushLifeCycleTracker.DUMMY);
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

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
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
            this.decrMemStoreSize(prepareFlushResult.totalFlushableSize.getMemStoreSize());
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
            this.decrMemStoreSize(prepareFlushResult.totalFlushableSize.getMemStoreSize());

            // Inspect the memstore contents to see whether the memstore contains only edits
            // with seqId smaller than the flush seqId. If so, we can discard those edits.
            dropMemStoreContentsForSeqId(flush.getFlushSequenceNumber(), null);

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
          dropMemStoreContentsForSeqId(flush.getFlushSequenceNumber(), null);
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
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
  private void replayFlushInStores(FlushDescriptor flush, PrepareFlushResult prepareFlushResult,
      boolean dropMemstoreSnapshot)
      throws IOException {
    for (StoreFlushDescriptor storeFlush : flush.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      HStore store = getStore(family);
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
        ctx = store.createFlushContext(flush.getFlushSequenceNumber(), FlushLifeCycleTracker.DUMMY);
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

  private long loadRecoveredHFilesIfAny(Collection<HStore> stores) throws IOException {
    Path regionDir = fs.getRegionDir();
    long maxSeqId = -1;
    for (HStore store : stores) {
      String familyName = store.getColumnFamilyName();
      FileStatus[] files =
          WALSplitUtil.getRecoveredHFiles(fs.getFileSystem(), regionDir, familyName);
      if (files != null && files.length != 0) {
        for (FileStatus file : files) {
          Path filePath = file.getPath();
          // If file length is zero then delete it
          if (isZeroLengthThenDelete(fs.getFileSystem(), file, filePath)) {
            continue;
          }
          try {
            HStoreFile storefile = store.tryCommitRecoveredHFile(file.getPath());
            maxSeqId = Math.max(maxSeqId, storefile.getReader().getSequenceID());
          } catch (IOException e) {
            handleException(fs.getFileSystem(), filePath, e);
            continue;
          }
        }
        if (this.rsServices != null && store.needsCompaction()) {
          this.rsServices.getCompactionRequestor()
              .requestCompaction(this, store, "load recovered hfiles request compaction",
                  Store.PRIORITY_USER + 1, CompactionLifeCycleTracker.DUMMY, null);
        }
      }
    }
    return maxSeqId;
  }

  /**
   * Be careful, this method will drop all data in the memstore of this region.
   * Currently, this method is used to drop memstore to prevent memory leak
   * when replaying recovered.edits while opening region.
   */
  private MemStoreSize dropMemStoreContents() throws IOException {
    MemStoreSizing totalFreedSize = new NonThreadSafeMemStoreSizing();
    this.updatesLock.writeLock().lock();
    try {
      for (HStore s : stores.values()) {
        MemStoreSize memStoreSize = doDropStoreMemStoreContentsForSeqId(s, HConstants.NO_SEQNUM);
        LOG.info("Drop memstore for Store " + s.getColumnFamilyName() + " in region "
                + this.getRegionInfo().getRegionNameAsString()
                + " , dropped memstoresize: [" + memStoreSize + " }");
        totalFreedSize.incMemStoreSize(memStoreSize);
      }
      return totalFreedSize.getMemStoreSize();
    } finally {
      this.updatesLock.writeLock().unlock();
    }
  }

  /**
   * Drops the memstore contents after replaying a flush descriptor or region open event replay
   * if the memstore edits have seqNums smaller than the given seq id
   */
  private MemStoreSize dropMemStoreContentsForSeqId(long seqId, HStore store) throws IOException {
    MemStoreSizing totalFreedSize = new NonThreadSafeMemStoreSizing();
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
          for (HStore s : stores.values()) {
            totalFreedSize.incMemStoreSize(doDropStoreMemStoreContentsForSeqId(s, currentSeqId));
          }
        } else {
          totalFreedSize.incMemStoreSize(doDropStoreMemStoreContentsForSeqId(store, currentSeqId));
        }
      } else {
        LOG.info(getRegionInfo().getEncodedName() + " : "
            + "Not dropping memstore contents since replayed flush seqId: "
            + seqId + " is smaller than current seqId:" + currentSeqId);
      }
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    return totalFreedSize.getMemStoreSize();
  }

  private MemStoreSize doDropStoreMemStoreContentsForSeqId(HStore s, long currentSeqId)
      throws IOException {
    MemStoreSize flushableSize = s.getFlushableSize();
    this.decrMemStoreSize(flushableSize);
    StoreFlushContext ctx = s.createFlushContext(currentSeqId, FlushLifeCycleTracker.DUMMY);
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

  PrepareFlushResult getPrepareFlushResult() {
    return prepareFlushResult;
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
    justification = "Intentional; cleared the memstore")
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
          HStore store = getStore(family);
          if (store == null) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                + "Received a region open marker from primary, but the family is not found. "
                + "Ignoring. StoreDescriptor:" + storeDescriptor);
            continue;
          }

          long storeSeqId = store.getMaxSequenceId().orElse(0L);
          List<String> storeFiles = storeDescriptor.getStoreFileList();
          try {
            store.refreshStoreFiles(storeFiles); // replace the files with the new ones
          } catch (FileNotFoundException ex) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
                    + "At least one of the store files: " + storeFiles
                    + " doesn't exist any more. Skip loading the file(s)", ex);
            continue;
          }
          if (store.getMaxSequenceId().orElse(0L) != storeSeqId) {
            // Record latest flush time if we picked up new files
            lastStoreFlushTimeMap.put(store, EnvironmentEdgeManager.currentTime());
          }

          if (writestate.flushing) {
            // only drop memstore snapshots if they are smaller than last flush for the store
            if (this.prepareFlushResult.flushOpSeqId <= regionEvent.getLogSequenceNumber()) {
              StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null ?
                  null : this.prepareFlushResult.storeFlushCtxs.get(family);
              if (ctx != null) {
                MemStoreSize mss = store.getFlushableSize();
                ctx.abort();
                this.decrMemStoreSize(mss);
                this.prepareFlushResult.storeFlushCtxs.remove(family);
              }
            }
          }

          // Drop the memstore contents if they are now smaller than the latest seen flushed file
          dropMemStoreContentsForSeqId(regionEvent.getLogSequenceNumber(), store);
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

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. Only for keep compatibility for old region
   *             replica implementation.
   */
  @Deprecated
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
          HStore store = getStore(family);
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
   * Replay the batch mutate for secondary replica.
   * <p/>
   * We will directly apply the cells to the memstore. This is because:
   * <ol>
   * <li>All the cells are gotten from {@link WALEdit}, so we only have {@link Put} and
   * {@link Delete} here</li>
   * <li>The replay is single threaded, we do not need to acquire row lock, as the region is read
   * only so no one else can write it.</li>
   * <li>We do not need to write WAL.</li>
   * <li>We will advance MVCC in the caller directly.</li>
   * </ol>
   */
  private void replayWALBatchMutate(Map<byte[], List<Cell>> family2Cells) throws IOException {
    startRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    try {
      for (Map.Entry<byte[], List<Cell>> entry : family2Cells.entrySet()) {
        applyToMemStore(getStore(entry.getKey()), entry.getValue(), false, memStoreSizing);
      }
    } finally {
      closeRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    }
  }

  /**
   * Replay the meta edits, i.e, flush marker, compaction marker, bulk load marker, region event
   * marker, etc.
   * <p/>
   * For all events other than start flush, we will just call {@link #refreshStoreFiles()} as the
   * logic is straight-forward and robust. For start flush, we need to snapshot the memstore, so
   * later {@link #refreshStoreFiles()} call could drop the snapshot, otherwise we may run out of
   * memory.
   */
  private void replayWALMetaEdit(Cell cell) throws IOException {
    startRegionOperation(Operation.REPLAY_EVENT);
    try {
      FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(cell);
      if (flushDesc != null) {
        switch (flushDesc.getAction()) {
          case START_FLUSH:
            // for start flush, we need to take a snapshot of the current memstore
            synchronized (writestate) {
              if (!writestate.flushing) {
                this.writestate.flushing = true;
              } else {
                // usually this should not happen but let's make the code more robust, it is not a
                // big deal to just ignore it, the refreshStoreFiles call should have the ability to
                // clean up the inconsistent state.
                LOG.debug("NOT flushing {} as already flushing", getRegionInfo());
                break;
              }
            }
            MonitoredTask status =
              TaskMonitor.get().createStatus("Preparing flush " + getRegionInfo());
            Collection<HStore> storesToFlush = getStoresToFlush(flushDesc);
            try {
              PrepareFlushResult prepareResult =
                internalPrepareFlushCache(null, flushDesc.getFlushSequenceNumber(), storesToFlush,
                  status, false, FlushLifeCycleTracker.DUMMY);
              if (prepareResult.result == null) {
                // save the PrepareFlushResult so that we can use it later from commit flush
                this.prepareFlushResult = prepareResult;
                status.markComplete("Flush prepare successful");
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} prepared flush with seqId: {}", getRegionInfo(),
                    flushDesc.getFlushSequenceNumber());
                }
              } else {
                // special case empty memstore. We will still save the flush result in this case,
                // since our memstore is empty, but the primary is still flushing
                if (prepareResult.getResult()
                  .getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
                  this.prepareFlushResult = prepareResult;
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("{} prepared empty flush with seqId: {}", getRegionInfo(),
                      flushDesc.getFlushSequenceNumber());
                  }
                }
                status.abort("Flush prepare failed with " + prepareResult.result);
                // nothing much to do. prepare flush failed because of some reason.
              }
            } finally {
              status.cleanup();
            }
            break;
          case ABORT_FLUSH:
            // do nothing, an abort flush means the source region server will crash itself, after
            // the primary region online, it will send us an open region marker, then we can clean
            // up the memstore.
            synchronized (writestate) {
              writestate.flushing = false;
            }
            break;
          case COMMIT_FLUSH:
          case CANNOT_FLUSH:
            // just call refreshStoreFiles
            refreshStoreFiles();
            logRegionFiles();
            synchronized (writestate) {
              writestate.flushing = false;
            }
            break;
          default:
            LOG.warn("{} received a flush event with unknown action: {}", getRegionInfo(),
              TextFormat.shortDebugString(flushDesc));
        }
      } else {
        // for all other region events, we will do a refreshStoreFiles
        refreshStoreFiles();
        logRegionFiles();
      }
    } finally {
      closeRegionOperation(Operation.REPLAY_EVENT);
    }
  }

  /**
   * Replay remote wal entry sent by primary replica.
   * <p/>
   * Should only call this method on secondary replicas.
   */
  void replayWALEntry(WALEntry entry, CellScanner cells) throws IOException {
    long timeout = -1L;
    Optional<RpcCall> call = RpcServer.getCurrentCall();
    if (call.isPresent()) {
      long deadline = call.get().getDeadline();
      if (deadline < Long.MAX_VALUE) {
        timeout = deadline - EnvironmentEdgeManager.currentTime();
        if (timeout <= 0) {
          throw new TimeoutIOException("Timeout while replaying edits for " + getRegionInfo());
        }
      }
    }
    if (timeout > 0) {
      try {
        if (!replayLock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
          throw new TimeoutIOException(
            "Timeout while waiting for lock when replaying edits for " + getRegionInfo());
        }
      } catch (InterruptedException e) {
        throw throwOnInterrupt(e);
      }
    } else {
      replayLock.lock();
    }
    try {
      int count = entry.getAssociatedCellCount();
      long sequenceId = entry.getKey().getLogSequenceNumber();
      if (lastReplayedSequenceId >= sequenceId) {
        // we have already replayed this edit, skip
        // remember to advance the CellScanner, as we may have multiple WALEntries, we may still
        // need apply later WALEntries
        for (int i = 0; i < count; i++) {
          // Throw index out of bounds if our cell count is off
          if (!cells.advance()) {
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }
        }
        return;
      }
      Map<byte[], List<Cell>> family2Cells = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (int i = 0; i < count; i++) {
        // Throw index out of bounds if our cell count is off
        if (!cells.advance()) {
          throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
        }
        Cell cell = cells.current();
        if (WALEdit.isMetaEditFamily(cell)) {
          // If there is meta edit, i.e, we have done flush/compaction/open, then we need to apply
          // the previous cells first, and then replay the special meta edit. The meta edit is like
          // a barrier, We need to keep the order. For example, the flush marker will contain a
          // flush sequence number, which makes us possible to drop memstore content, but if we
          // apply some edits which have greater sequence id first, then we can not drop the
          // memstore content when replaying the flush marker, which is not good as we could run out
          // of memory.
          // And usually, a meta edit will have a special WALEntry for it, so this is just a safe
          // guard logic to make sure we do not break things in the worst case.
          if (!family2Cells.isEmpty()) {
            replayWALBatchMutate(family2Cells);
            family2Cells.clear();
          }
          replayWALMetaEdit(cell);
        } else {
          family2Cells
            .computeIfAbsent(CellUtil.cloneFamily(cell), k -> new ArrayList<>())
            .add(cell);
        }
      }
      // do not forget to apply the remaining cells
      if (!family2Cells.isEmpty()) {
        replayWALBatchMutate(family2Cells);
      }
      mvcc.advanceTo(sequenceId);
      lastReplayedSequenceId = sequenceId;
    } finally {
      replayLock.unlock();
    }
  }

  /**
   * If all stores ended up dropping their snapshots, we can safely drop the prepareFlushResult
   */
  private void dropPrepareFlushIfPossible() {
    if (writestate.flushing) {
      boolean canDrop = true;
      if (prepareFlushResult.storeFlushCtxs != null) {
        for (Entry<byte[], StoreFlushContext> entry : prepareFlushResult.storeFlushCtxs
            .entrySet()) {
          HStore store = getStore(entry.getKey());
          if (store == null) {
            continue;
          }
          if (store.getSnapshotSize().getDataSize() > 0) {
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
      justification = "Notify is about post replay. Intentional")
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
      Map<HStore, Long> map = new HashMap<>();
      synchronized (writestate) {
        for (HStore store : stores.values()) {
          // TODO: some stores might see new data from flush, while others do not which
          // MIGHT break atomic edits across column families.
          long maxSeqIdBefore = store.getMaxSequenceId().orElse(0L);

          // refresh the store files. This is similar to observing a region open wal marker.
          store.refreshStoreFiles();

          long storeSeqId = store.getMaxSequenceId().orElse(0L);
          if (storeSeqId < smallestSeqIdInStores) {
            smallestSeqIdInStores = storeSeqId;
          }

          // see whether we can drop the memstore or the snapshot
          if (storeSeqId > maxSeqIdBefore) {
            if (writestate.flushing) {
              // only drop memstore snapshots if they are smaller than last flush for the store
              if (this.prepareFlushResult.flushOpSeqId <= storeSeqId) {
                StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null ?
                    null : this.prepareFlushResult.storeFlushCtxs.get(
                            store.getColumnFamilyDescriptor().getName());
                if (ctx != null) {
                  MemStoreSize mss = store.getFlushableSize();
                  ctx.abort();
                  this.decrMemStoreSize(mss);
                  this.prepareFlushResult.storeFlushCtxs.
                      remove(store.getColumnFamilyDescriptor().getName());
                  totalFreedDataSize += mss.getDataSize();
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
        for (HStore s : stores.values()) {
          mvcc.advanceTo(s.getMaxMemStoreTS().orElse(0L));
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
        for (Map.Entry<HStore, Long> entry : map.entrySet()) {
          // Drop the memstore contents if they are now smaller than the latest seen flushed file
          totalFreedDataSize += dropMemStoreContentsForSeqId(entry.getValue(), entry.getKey())
              .getDataSize();
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
      stores.values().stream().filter(s -> s.getStorefiles() != null)
          .flatMap(s -> s.getStorefiles().stream())
          .forEachOrdered(sf -> LOG.trace(getRegionInfo().getEncodedName() + " : " + sf));
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
   */
  protected void restoreEdit(HStore s, Cell cell, MemStoreSizing memstoreAccounting) {
    s.add(cell, memstoreAccounting);
  }

  /**
   * make sure have been through lease recovery before get file status, so the file length can be
   * trusted.
   * @param p File to check.
   * @return True if file was zero-length (and if so, we'll delete it in here).
   * @throws IOException
   */
  private static boolean isZeroLengthThenDelete(final FileSystem fs, final FileStatus stat,
      final Path p) throws IOException {
    if (stat.getLen() > 0) {
      return false;
    }
    LOG.warn("File " + p + " is zero-length, deleting.");
    fs.delete(p, false);
    return true;
  }

  protected HStore instantiateHStore(final ColumnFamilyDescriptor family, boolean warmup)
      throws IOException {
    if (family.isMobEnabled()) {
      if (HFile.getFormatVersion(this.conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
        throw new IOException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS +
            " is required for MOB feature. Consider setting " + HFile.FORMAT_VERSION_KEY +
            " accordingly.");
      }
      return new HMobStore(this, family, this.conf, warmup);
    }
    return new HStore(this, family, this.conf, warmup);
  }

  @Override
  public HStore getStore(byte[] column) {
    return this.stores.get(column);
  }

  /**
   * Return HStore instance. Does not do any copy: as the number of store is limited, we iterate on
   * the list.
   */
  private HStore getStore(Cell cell) {
    return stores.entrySet().stream().filter(e -> CellUtil.matchingFamily(cell, e.getKey()))
        .map(e -> e.getValue()).findFirst().orElse(null);
  }

  @Override
  public List<HStore> getStores() {
    return new ArrayList<>(stores.values());
  }

  @Override
  public List<String> getStoreFileList(byte[][] columns) throws IllegalArgumentException {
    List<String> storeFileNames = new ArrayList<>();
    synchronized (closeLock) {
      for (byte[] column : columns) {
        HStore store = this.stores.get(column);
        if (store == null) {
          throw new IllegalArgumentException(
              "No column family : " + new String(column, StandardCharsets.UTF_8) + " available");
        }
        Collection<HStoreFile> storeFiles = store.getStorefiles();
        if (storeFiles == null) {
          continue;
        }
        for (HStoreFile storeFile : storeFiles) {
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
  void checkRow(byte[] row, String op) throws IOException {
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
    return getRowLock(row, readLock, null);
  }

  Span createRegionSpan(String name) {
    return TraceUtil.createSpan(name).setAttribute(REGION_NAMES_KEY,
      Collections.singletonList(getRegionInfo().getRegionNameAsString()));
  }

  // will be override in tests
  protected RowLock getRowLockInternal(byte[] row, boolean readLock, RowLock prevRowLock)
    throws IOException {
    // create an object to use a a key in the row lock map
    HashedBytes rowKey = new HashedBytes(row);

    RowLockContext rowLockContext = null;
    RowLockImpl result = null;

    boolean success = false;
    try {
      // Keep trying until we have a lock or error out.
      // TODO: do we need to add a time component here?
      while (result == null) {
        rowLockContext = computeIfAbsent(lockedRows, rowKey, () -> new RowLockContext(rowKey));
        // Now try an get the lock.
        // This can fail as
        if (readLock) {
          // For read lock, if the caller has locked the same row previously, it will not try
          // to acquire the same read lock. It simply returns the previous row lock.
          RowLockImpl prevRowLockImpl = (RowLockImpl)prevRowLock;
          if ((prevRowLockImpl != null) && (prevRowLockImpl.getLock() ==
              rowLockContext.readWriteLock.readLock())) {
            success = true;
            return prevRowLock;
          }
          result = rowLockContext.newReadLock();
        } else {
          result = rowLockContext.newWriteLock();
        }
      }

      int timeout = rowLockWaitDuration;
      boolean reachDeadlineFirst = false;
      Optional<RpcCall> call = RpcServer.getCurrentCall();
      if (call.isPresent()) {
        long deadline = call.get().getDeadline();
        if (deadline < Long.MAX_VALUE) {
          int timeToDeadline = (int) (deadline - EnvironmentEdgeManager.currentTime());
          if (timeToDeadline <= this.rowLockWaitDuration) {
            reachDeadlineFirst = true;
            timeout = timeToDeadline;
          }
        }
      }

      if (timeout <= 0 || !result.getLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        String message = "Timed out waiting for lock for row: " + rowKey + " in region "
            + getRegionInfo().getEncodedName();
        if (reachDeadlineFirst) {
          throw new TimeoutIOException(message);
        } else {
          // If timeToDeadline is larger than rowLockWaitDuration, we can not drop the request.
          throw new IOException(message);
        }
      }
      rowLockContext.setThreadName(Thread.currentThread().getName());
      success = true;
      return result;
    } catch (InterruptedException ie) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Thread interrupted waiting for lock on row: {}, in region {}", rowKey,
          getRegionInfo().getRegionNameAsString());
      }
      throw throwOnInterrupt(ie);
    } catch (Error error) {
      // The maximum lock count for read lock is 64K (hardcoded), when this maximum count
      // is reached, it will throw out an Error. This Error needs to be caught so it can
      // go ahead to process the minibatch with lock acquired.
      LOG.warn("Error to get row lock for {}, in region {}, cause: {}", Bytes.toStringBinary(row),
        getRegionInfo().getRegionNameAsString(), error);
      IOException ioe = new IOException(error);
      throw ioe;
    } finally {
      // Clean up the counts just in case this was the thing keeping the context alive.
      if (!success && rowLockContext != null) {
        rowLockContext.cleanUp();
      }
    }
  }

  private RowLock getRowLock(byte[] row, boolean readLock, final RowLock prevRowLock)
    throws IOException {
    return TraceUtil.trace(() -> getRowLockInternal(row, readLock, prevRowLock),
      () -> createRegionSpan("Region.getRowLock").setAttribute(ROW_LOCK_READ_LOCK_KEY,
        readLock));
  }

  private void releaseRowLocks(List<RowLock> rowLocks) {
    if (rowLocks != null) {
      for (RowLock rowLock : rowLocks) {
        rowLock.release();
      }
      rowLocks.clear();
    }
  }

  public int getReadLockCount() {
    return lock.getReadLockCount();
  }

  public ConcurrentHashMap<HashedBytes, RowLockContext> getLockedRows() {
    return lockedRows;
  }

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
          if (count.get() <= 0 && usable.get()){ // Don't attempt to remove row if already removed
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

  /**
   * Attempts to atomically load a group of hfiles.  This is critical for loading
   * rows with multiple column families atomically.
   *
   * @param familyPaths List of Pair&lt;byte[] column family, String hfilePath&gt;
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a
   * file about to be bulk loaded
   * @param assignSeqId
   * @return Map from family to List of store file paths if successful, null if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths, boolean assignSeqId,
      BulkLoadListener bulkLoadListener) throws IOException {
    return bulkLoadHFiles(familyPaths, assignSeqId, bulkLoadListener, false,
      null, true);
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
    String prepareBulkLoad(byte[] family, String srcPath, boolean copyFile, String customStaging)
        throws IOException;

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

  /**
   * Attempts to atomically load a group of hfiles.  This is critical for loading
   * rows with multiple column families atomically.
   *
   * @param familyPaths List of Pair&lt;byte[] column family, String hfilePath&gt;
   * @param assignSeqId
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a
   * file about to be bulk loaded
   * @param copyFile always copy hfiles if true
   * @param  clusterIds ids from clusters that had already handled the given bulkload event.
   * @return Map from family to List of store file paths if successful, null if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths,
      boolean assignSeqId, BulkLoadListener bulkLoadListener, boolean copyFile,
      List<String> clusterIds, boolean replicate) throws IOException {
    long seqId = -1;
    Map<byte[], List<Path>> storeFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    Map<String, Long> storeFilesSizes = new HashMap<>();
    Preconditions.checkNotNull(familyPaths);
    // we need writeLock for multi-family bulk load
    startBulkRegionOperation(hasMultipleColumnFamilies(familyPaths));
    boolean isSuccessful = false;
    try {
      this.writeRequestsCount.increment();

      // There possibly was a split that happened between when the split keys
      // were gathered and before the HRegion's write lock was taken. We need
      // to validate the HFile region before attempting to bulk load all of them
      IOException ioException = null;
      List<Pair<byte[], String>> failures = new ArrayList<>();
      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();

        HStore store = getStore(familyName);
        if (store == null) {
          ioException = new org.apache.hadoop.hbase.DoNotRetryIOException(
              "No such column family " + Bytes.toStringBinary(familyName));
        } else {
          try {
            store.assertBulkLoadHFileOk(new Path(path));
          } catch (WrongRegionException wre) {
            // recoverable (file doesn't fit in region)
            failures.add(p);
          } catch (IOException ioe) {
            // unrecoverable (hdfs problem)
            ioException = ioe;
          }
        }

        // validation failed because of some sort of IO problem.
        if (ioException != null) {
          LOG.error("There was IO error when checking if the bulk load is ok in region {}.", this,
            ioException);
          throw ioException;
        }
      }
      // validation failed, bail out before doing anything permanent.
      if (failures.size() != 0) {
        StringBuilder list = new StringBuilder();
        for (Pair<byte[], String> p : failures) {
          list.append("\n").append(Bytes.toString(p.getFirst())).append(" : ")
              .append(p.getSecond());
        }
        // problem when validating
        LOG.warn("There was a recoverable bulk load failure likely due to a split. These (family,"
          + " HFile) pairs were not loaded: {}, in region {}", list.toString(), this);
        return null;
      }

      // We need to assign a sequential ID that's in between two memstores in order to preserve
      // the guarantee that all the edits lower than the highest sequential ID from all the
      // HFiles are flushed on disk. See HBASE-10958.  The sequence id returned when we flush is
      // guaranteed to be one beyond the file made when we flushed (or if nothing to flush, it is
      // a sequence id that we can be sure is beyond the last hfile written).
      if (assignSeqId) {
        FlushResult fs = flushcache(true, false, FlushLifeCycleTracker.DUMMY);
        if (fs.isFlushSucceeded()) {
          seqId = ((FlushResultImpl)fs).flushSequenceId;
        } else if (fs.getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
          seqId = ((FlushResultImpl)fs).flushSequenceId;
        } else if (fs.getResult() == FlushResult.Result.CANNOT_FLUSH) {
          // CANNOT_FLUSH may mean that a flush is already on-going
          // we need to wait for that flush to complete
          waitForFlushes();
        } else {
          throw new IOException("Could not bulk load with an assigned sequential ID because the "+
            "flush didn't run. Reason for not flushing: " + ((FlushResultImpl)fs).failureReason);
        }
      }

      Map<byte[], List<Pair<Path, Path>>> familyWithFinalPath =
          new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();
        HStore store = getStore(familyName);
        if (!familyWithFinalPath.containsKey(familyName)) {
          familyWithFinalPath.put(familyName, new ArrayList<>());
        }
        List<Pair<Path, Path>> lst = familyWithFinalPath.get(familyName);
        String finalPath = path;
        try {
          boolean reqTmp = store.storeEngine.requireWritingToTmpDirFirst();
          if (bulkLoadListener != null) {
            finalPath = bulkLoadListener.prepareBulkLoad(familyName, path, copyFile,
              reqTmp ? null : regionDir.toString());
          }
          Pair<Path, Path> pair = null;
          if (reqTmp) {
            pair = store.preBulkLoadHFile(finalPath, seqId);
          }
          else {
            Path livePath = new Path(finalPath);
            pair = new Pair<>(livePath, livePath);
          }
          lst.add(pair);
        } catch (IOException ioe) {
          // A failure here can cause an atomicity violation that we currently
          // cannot recover from since it is likely a failed HDFS operation.

          LOG.error("There was a partial failure due to IO when attempting to" +
              " load " + Bytes.toString(p.getFirst()) + " : " + p.getSecond(), ioe);
          if (bulkLoadListener != null) {
            try {
              bulkLoadListener.failedBulkLoad(familyName, finalPath);
            } catch (Exception ex) {
              LOG.error("Error while calling failedBulkLoad for family " +
                  Bytes.toString(familyName) + " with path " + path, ex);
            }
          }
          throw ioe;
        }
      }

      if (this.getCoprocessorHost() != null) {
        for (Map.Entry<byte[], List<Pair<Path, Path>>> entry : familyWithFinalPath.entrySet()) {
          this.getCoprocessorHost().preCommitStoreFile(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<byte[], List<Pair<Path, Path>>> entry : familyWithFinalPath.entrySet()) {
        byte[] familyName = entry.getKey();
        for (Pair<Path, Path> p : entry.getValue()) {
          String path = p.getFirst().toString();
          Path commitedStoreFile = p.getSecond();
          HStore store = getStore(familyName);
          try {
            store.bulkLoadHFile(familyName, path, commitedStoreFile);
            // Note the size of the store file
            try {
              FileSystem fs = commitedStoreFile.getFileSystem(baseConf);
              storeFilesSizes.put(commitedStoreFile.getName(), fs.getFileStatus(commitedStoreFile)
                  .getLen());
            } catch (IOException e) {
              LOG.warn("Failed to find the size of hfile " + commitedStoreFile, e);
              storeFilesSizes.put(commitedStoreFile.getName(), 0L);
            }

            if(storeFiles.containsKey(familyName)) {
              storeFiles.get(familyName).add(commitedStoreFile);
            } else {
              List<Path> storeFileNames = new ArrayList<>();
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
                " load " + Bytes.toString(familyName) + " : " + p.getSecond(), ioe);
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
      }

      isSuccessful = true;
      if (conf.getBoolean(COMPACTION_AFTER_BULKLOAD_ENABLE, false)) {
        // request compaction
        familyWithFinalPath.keySet().forEach(family -> {
          HStore store = getStore(family);
          try {
            if (this.rsServices != null && store.needsCompaction()) {
              this.rsServices.getCompactionRequestor().requestSystemCompaction(this, store,
                  "bulkload hfiles request compaction", true);
              LOG.info("Request compaction for region {} family {} after bulk load",
                  this.getRegionInfo().getEncodedName(), store.getColumnFamilyName());
            }
          } catch (IOException e) {
            LOG.error("bulkload hfiles request compaction error ", e);
          }
        });
      }
    } finally {
      if (wal != null && !storeFiles.isEmpty()) {
        // Write a bulk load event for hfiles that are loaded
        try {
          WALProtos.BulkLoadDescriptor loadDescriptor =
              ProtobufUtil.toBulkLoadDescriptor(this.getRegionInfo().getTable(),
              UnsafeByteOperations.unsafeWrap(this.getRegionInfo().getEncodedNameAsBytes()),
              storeFiles, storeFilesSizes, seqId, clusterIds, replicate);
          WALUtil.writeBulkLoadMarkerAndSync(this.wal, this.getReplicationScope(), getRegionInfo(),
            loadDescriptor, mvcc, regionReplicationSink.orElse(null));
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

  // Utility methods
  /**
   * A utility method to create new instances of HRegion based on the {@link HConstants#REGION_IMPL}
   * configuration property.
   * @param tableDir qualified path of directory where region should be located, usually the table
   *          directory.
   * @param wal The WAL is the outbound log for any updates to the HRegion The wal file is a logfile
   *          from the previous execution that's custom-computed for this HRegion. The HRegionServer
   *          computes and sorts the appropriate wal info for this HRegion. If there is a previous
   *          file (implying that the HRegion has been written-to before), then read it from the
   *          supplied path.
   * @param fs is the filesystem.
   * @param conf is global configuration settings.
   * @param regionInfo - RegionInfo that describes the region is new), then read them from the
   *          supplied path.
   * @param htd the table descriptor
   * @return the new instance
   */
  public static HRegion newHRegion(Path tableDir, WAL wal, FileSystem fs,
      Configuration conf, RegionInfo regionInfo, final TableDescriptor htd,
      RegionServerServices rsServices) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
          (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, HRegion.class);

      Constructor<? extends HRegion> c =
          regionClass.getConstructor(Path.class, WAL.class, FileSystem.class,
              Configuration.class, RegionInfo.class, TableDescriptor.class,
              RegionServerServices.class);

      return c.newInstance(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
    } catch (Throwable e) {
      // todo: what should I throw here?
      throw new IllegalStateException("Could not instantiate a region instance.", e);
    }
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @return new HRegion
   */
  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
      final boolean initialize) throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, initialize, null);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param wal shared WAL
   * @param initialize - true to initialize the region
   * @param rsRpcServices An interface we can request flushes against.
   * @return new HRegion
   */
  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
      final boolean initialize, RegionServerServices rsRpcServices) throws IOException {
    LOG.info("creating " + info + ", tableDescriptor="
        + (hTableDescriptor == null ? "null" : hTableDescriptor) + ", regionDir=" + rootDir);
    createRegionDir(conf, info, rootDir);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    HRegion region =
        HRegion.newHRegion(tableDir, wal, fs, conf, info, hTableDescriptor, rsRpcServices);
    if (initialize) {
      region.initialize(null);
    }
    return region;
  }

  /**
   * Create a region under the given table directory.
   */
  public static HRegion createHRegion(Configuration conf, RegionInfo regionInfo, FileSystem fs,
    Path tableDir, TableDescriptor tableDesc) throws IOException {
    LOG.info("Creating {}, tableDescriptor={}, under table dir {}", regionInfo, tableDesc,
      tableDir);
    HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, regionInfo);
    HRegion region = HRegion.newHRegion(tableDir, null, fs, conf, regionInfo, tableDesc, null);
    return region;
  }

  /**
   * Create the region directory in the filesystem.
   */
  public static HRegionFileSystem createRegionDir(Configuration configuration, RegionInfo ri,
        Path rootDir)
      throws IOException {
    FileSystem fs = rootDir.getFileSystem(configuration);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, ri.getTable());
    // If directory already exists, will log warning and keep going. Will try to create
    // .regioninfo. If one exists, will overwrite.
    return HRegionFileSystem.createRegionOnFileSystem(configuration, fs, tableDir, ri);
  }

  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
                                      final Configuration conf,
                                      final TableDescriptor hTableDescriptor,
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
  public static HRegion openHRegion(final RegionInfo info,
      final TableDescriptor htd, final WAL wal,
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
  public static HRegion openHRegion(final RegionInfo info,
    final TableDescriptor htd, final WAL wal, final Configuration conf,
    final RegionServerServices rsServices,
    final CancelableProgressable reporter)
  throws IOException {
    return openHRegion(CommonFSUtils.getRootDir(conf), info, htd, wal, conf, rsServices, reporter);
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
  public static HRegion openHRegion(Path rootDir, final RegionInfo info,
      final TableDescriptor htd, final WAL wal, final Configuration conf)
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
  public static HRegion openHRegion(final Path rootDir, final RegionInfo info,
      final TableDescriptor htd, final WAL wal, final Configuration conf,
      final RegionServerServices rsServices,
      final CancelableProgressable reporter)
  throws IOException {
    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = rootDir.getFileSystem(conf);
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
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
      final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal)
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
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
    final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final RegionServerServices rsServices, final CancelableProgressable reporter)
    throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    return openHRegionFromTableDir(conf, fs, tableDir, info, htd, wal, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @param wal WAL for region to use. This method will call
   * WAL#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the wal id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   * @throws NullPointerException if {@code info} is {@code null}
   */
  public static HRegion openHRegionFromTableDir(final Configuration conf, final FileSystem fs,
    final Path tableDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final RegionServerServices rsServices, final CancelableProgressable reporter)
    throws IOException {
    Objects.requireNonNull(info, "RegionInfo cannot be null");
    LOG.debug("Opening region: {}", info);
    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, rsServices);
    return r.openHRegion(reporter);
  }

  public NavigableMap<byte[], Integer> getReplicationScope() {
    return this.replicationScope;
  }

  /**
   * Useful when reopening a closed region (normally for unit tests)
   * @param other original object
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegion(final HRegion other, final CancelableProgressable reporter)
      throws IOException {
    HRegionFileSystem regionFs = other.getRegionFileSystem();
    HRegion r = newHRegion(regionFs.getTableDir(), other.getWAL(), regionFs.getFileSystem(),
        other.baseConf, other.getRegionInfo(), other.getTableDescriptor(), null);
    return r.openHRegion(reporter);
  }

  public static Region openHRegion(final Region other, final CancelableProgressable reporter)
        throws IOException {
    return openHRegion((HRegion)other, reporter);
  }

  /**
   * Open HRegion.
   * <p/>
   * Calls initialize and sets sequenceId.
   * @return Returns <code>this</code>
   */
  private HRegion openHRegion(final CancelableProgressable reporter) throws IOException {
    try {
      // Refuse to open the region if we are missing local compression support
      TableDescriptorChecker.checkCompression(htableDescriptor);
      // Refuse to open the region if encryption configuration is incorrect or
      // codec support is missing
      LOG.debug("checking encryption for " + this.getRegionInfo().getEncodedName());
      TableDescriptorChecker.checkEncryption(conf, htableDescriptor);
      // Refuse to open the region if a required class cannot be loaded
      LOG.debug("checking classloading for " + this.getRegionInfo().getEncodedName());
      TableDescriptorChecker.checkClassLoading(conf, htableDescriptor);
      this.openSeqNum = initialize(reporter);
      this.mvcc.advanceTo(openSeqNum);
      // The openSeqNum must be increased every time when a region is assigned, as we rely on it to
      // determine whether a region has been successfully reopened. So here we always write open
      // marker, even if the table is read only.
      if (wal != null && getRegionServerServices() != null &&
        RegionReplicaUtil.isDefaultReplica(getRegionInfo())) {
        writeRegionOpenMarker(wal, openSeqNum);
      }
    } catch (Throwable t) {
      // By coprocessor path wrong region will open failed,
      // MetricsRegionWrapperImpl is already init and not close,
      // add region close when open failed
      try {
        // It is not required to write sequence id file when region open is failed.
        // Passing true to skip the sequence id file write.
        this.close(true);
      } catch (Throwable e) {
        LOG.warn("Open region: {} failed. Try close region but got exception ", this.getRegionInfo(),
          e);
      }
      throw t;
    }
    return this;
  }

  /**
   * Open a Region on a read-only file-system (like hdfs snapshots)
   * @param conf The Configuration object to use.
   * @param fs Filesystem to use
   * @param info Info for region to be opened.
   * @param htd the table descriptor
   * @return new HRegion
   * @throws NullPointerException if {@code info} is {@code null}
   */
  public static HRegion openReadOnlyFileSystemHRegion(final Configuration conf, final FileSystem fs,
      final Path tableDir, RegionInfo info, final TableDescriptor htd) throws IOException {
    Objects.requireNonNull(info, "RegionInfo cannot be null");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region (readOnly filesystem): " + info);
    }
    if (info.getReplicaId() <= 0) {
      info = RegionReplicaUtil.getRegionInfoForReplica(info, 1);
    }
    HRegion r = HRegion.newHRegion(tableDir, null, fs, conf, info, htd, null);
    r.writestate.setReadOnly(true);
    return r.openHRegion(null);
  }

  public static void warmupHRegion(final RegionInfo info,
      final TableDescriptor htd, final WAL wal, final Configuration conf,
      final RegionServerServices rsServices,
      final CancelableProgressable reporter)
      throws IOException {

    Objects.requireNonNull(info, "RegionInfo cannot be null");
    LOG.debug("Warmup {}", info);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = rootDir.getFileSystem(conf);
    }
    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, null);
    r.initializeWarmup(reporter);
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
   * Determines if the specified row is within the row range specified by the
   * specified RegionInfo
   *
   * @param info RegionInfo that specifies the row range
   * @param row row to be checked
   * @return true if the row is within the range specified by the RegionInfo
   */
  public static boolean rowIsInRange(RegionInfo info, final byte [] row) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), row) <= 0)) &&
        ((info.getEndKey().length == 0) ||
            (Bytes.compareTo(info.getEndKey(), row) > 0));
  }

  public static boolean rowIsInRange(RegionInfo info, final byte [] row, final int offset,
      final short length) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), 0, info.getStartKey().length,
          row, offset, length) <= 0)) &&
        ((info.getEndKey().length == 0) ||
          (Bytes.compareTo(info.getEndKey(), 0, info.getEndKey().length, row, offset, length) > 0));
  }

  @Override
  public Result get(final Get get) throws IOException {
    prepareGet(get);
    List<Cell> results = get(get, true);
    boolean stale = this.getRegionInfo().getReplicaId() != 0;
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  void prepareGet(final Get get) throws IOException {
    checkRow(get.getRow(), "Get");
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte[] family : get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family : this.htableDescriptor.getColumnFamilyNames()) {
        get.addFamily(family);
      }
    }
  }

  @Override
  public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
    return get(get, withCoprocessor, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private List<Cell> get(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
    throws IOException {
    return TraceUtil.trace(() -> getInternal(get, withCoprocessor, nonceGroup, nonce),
      () -> createRegionSpan("Region.get"));
  }

  private List<Cell> getInternal(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
    throws IOException {
    List<Cell> results = new ArrayList<>();
    long before = EnvironmentEdgeManager.currentTime();

    // pre-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      if (coprocessorHost.preGet(get, results)) {
        metricsUpdateForGet(results, before);
        return results;
      }
    }
    Scan scan = new Scan(get);
    if (scan.getLoadColumnFamiliesOnDemandValue() == null) {
      scan.setLoadColumnFamiliesOnDemand(isLoadingCfsOnDemandDefault());
    }
    try (RegionScanner scanner = getScanner(scan, null, nonceGroup, nonce)) {
      List<Cell> tmp = new ArrayList<>();
      scanner.next(tmp);
      // Copy EC to heap, then close the scanner.
      // This can be an EXPENSIVE call. It may make an extra copy from offheap to onheap buffers.
      // See more details in HBASE-26036.
      for (Cell cell : tmp) {
        results.add(
          CellUtil.cloneIfNecessary(cell));
      }
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
    if (this.rsServices != null && this.rsServices.getMetrics() != null) {
      rsServices.getMetrics().updateReadQueryMeter(getRegionInfo().getTable(), 1);
    }
    
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    return mutateRow(rm, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result mutateRow(RowMutations rm, long nonceGroup, long nonce) throws IOException {
    final List<Mutation> m = rm.getMutations();
    OperationStatus[] statuses = batchMutate(m.toArray(new Mutation[0]), true, nonceGroup, nonce);

    List<Result> results = new ArrayList<>();
    for (OperationStatus status : statuses) {
      if (status.getResult() != null) {
        results.add(status.getResult());
      }
    }

    if (results.isEmpty()) {
      return null;
    }

    // Merge the results of the Increment/Append operations
    List<Cell> cells = new ArrayList<>();
    for (Result result : results) {
      if (result.rawCells() != null) {
        cells.addAll(Arrays.asList(result.rawCells()));
      }
    }
    return Result.create(cells);
  }

  /**
   * Perform atomic (all or none) mutations within the region.
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
    batchMutate(new MutationBatchOperation(this, mutations.toArray(new Mutation[mutations.size()]),
        true, nonceGroup, nonce) {
      @Override
      public MiniBatchOperationInProgress<Mutation> lockRowsAndBuildMiniBatch(
          List<RowLock> acquiredRowLocks) throws IOException {
        RowLock prevRowLock = null;
        for (byte[] row : rowsToLock) {
          try {
            RowLock rowLock = region.getRowLock(row, false, prevRowLock); // write lock
            if (rowLock != prevRowLock) {
              acquiredRowLocks.add(rowLock);
              prevRowLock = rowLock;
            }
          } catch (IOException ioe) {
            LOG.warn("Failed getting lock, row={}, in region {}", Bytes.toStringBinary(row), this,
              ioe);
            throw ioe;
          }
        }
        return createMiniBatch(size(), size());
      }
    });
  }

  /**
   * @return statistics about the current load of the region
   */
  public ClientProtos.RegionLoadStats getLoadStatistics() {
    if (!regionStatsEnabled) {
      return null;
    }
    ClientProtos.RegionLoadStats.Builder stats = ClientProtos.RegionLoadStats.newBuilder();
    stats.setMemStoreLoad((int) (Math.min(100,
        (this.memStoreSizing.getMemStoreSize().getHeapSize() * 100) / this.memstoreFlushSize)));
    if (rsServices.getHeapMemoryManager() != null) {
      // the HeapMemoryManager uses -0.0 to signal a problem asking the JVM,
      // so we could just do the calculation below and we'll get a 0.
      // treating it as a special case analogous to no HMM instead so that it can be
      // programatically treated different from using <1% of heap.
      final float occupancy = rsServices.getHeapMemoryManager().getHeapOccupancyPercent();
      if (occupancy != HeapMemoryManager.HEAP_OCCUPANCY_ERROR_VALUE) {
        stats.setHeapOccupancy((int)(occupancy * 100));
      }
    }
    stats.setCompactionPressure((int) (rsServices.getCompactionPressure() * 100 > 100 ? 100
        : rsServices.getCompactionPressure() * 100));
    return stats.build();
  }

  @Override
  public Result append(Append append) throws IOException {
    return append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result append(Append append, long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.APPEND);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(append, true, nonceGroup, nonce).getResult();
      } finally {
        closeRegionOperation(Operation.APPEND);
      }
    }, () -> createRegionSpan("Region.append"));
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result increment(Increment increment, long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.INCREMENT);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(increment, true, nonceGroup, nonce).getResult();
      } finally {
        closeRegionOperation(Operation.INCREMENT);
      }
    }, () -> createRegionSpan("Region.increment"));
  }

  /**
   * @return writeEntry associated with this append
   */
  private WriteEntry doWALAppend(WALEdit walEdit, Durability durability, List<UUID> clusterIds,
      long now, long nonceGroup, long nonce, long origLogSeqNum) throws IOException {
    Preconditions.checkArgument(walEdit != null && !walEdit.isEmpty(),
        "WALEdit is null or empty!");
    Preconditions.checkArgument(!walEdit.isReplay() || origLogSeqNum != SequenceId.NO_SEQUENCE_ID,
        "Invalid replay sequence Id for replay WALEdit!");
    // Using default cluster id, as this can only happen in the originating cluster.
    // A slave cluster receives the final value (not the delta) as a Put. We use HLogKey
    // here instead of WALKeyImpl directly to support legacy coprocessors.
    WALKeyImpl walKey = walEdit.isReplay()?
        new WALKeyImpl(this.getRegionInfo().getEncodedNameAsBytes(),
          this.htableDescriptor.getTableName(), SequenceId.NO_SEQUENCE_ID, now, clusterIds,
            nonceGroup, nonce, mvcc) :
        new WALKeyImpl(this.getRegionInfo().getEncodedNameAsBytes(),
            this.htableDescriptor.getTableName(), SequenceId.NO_SEQUENCE_ID, now, clusterIds,
            nonceGroup, nonce, mvcc, this.getReplicationScope());
    if (walEdit.isReplay()) {
      walKey.setOrigLogSeqNum(origLogSeqNum);
    }
    //don't call the coproc hook for writes to the WAL caused by
    //system lifecycle events like flushes or compactions
    if (this.coprocessorHost != null && !walEdit.isMetaEdit()) {
      this.coprocessorHost.preWALAppend(walKey, walEdit);
    }
    ServerCall<?> rpcCall = RpcServer.getCurrentServerCallWithCellScanner().orElse(null);
    try {
      long txid = this.wal.appendData(this.getRegionInfo(), walKey, walEdit);
      WriteEntry writeEntry = walKey.getWriteEntry();
      regionReplicationSink.ifPresent(sink -> writeEntry.attachCompletionAction(() -> {
        sink.add(walKey, walEdit, rpcCall);
      }));
      // Call sync on our edit.
      if (txid != 0) {
        sync(txid, durability);
      }
      return writeEntry;
    } catch (IOException ioe) {
      if (walKey.getWriteEntry() != null) {
        mvcc.complete(walKey.getWriteEntry());
      }
      throw ioe;
    }

  }

  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HRegion.class, false);

  // woefully out of date - currently missing:
  // 1 x HashMap - coprocessorServiceHandlers
  // 6 x LongAdder - numMutationsWithoutWAL, dataInMemoryWithoutWAL,
  //   checkAndMutateChecksPassed, checkAndMutateChecksFailed, readRequestsCount,
  //   writeRequestsCount, cpRequestsCount
  // 1 x HRegion$WriteState - writestate
  // 1 x RegionCoprocessorHost - coprocessorHost
  // 1 x RegionSplitPolicy - splitPolicy
  // 1 x MetricsRegion - metricsRegion
  // 1 x MetricsRegionWrapperImpl - metricsRegionWrapper
  public static final long DEEP_OVERHEAD = FIXED_OVERHEAD +
      ClassSize.OBJECT + // closeLock
      (2 * ClassSize.ATOMIC_BOOLEAN) + // closed, closing
      (3 * ClassSize.ATOMIC_LONG) + // numPutsWithoutWAL, dataInMemoryWithoutWAL,
                                    // compactionsFailed
      (3 * ClassSize.CONCURRENT_HASHMAP) +  // lockedRows, scannerReadPoints, regionLockHolders
      WriteState.HEAP_SIZE + // writestate
      ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + // stores
      (2 * ClassSize.REENTRANT_LOCK) + // lock, updatesLock
      MultiVersionConcurrencyControl.FIXED_SIZE // mvcc
      + 2 * ClassSize.TREEMAP // maxSeqIdInStores, replicationScopes
      + 2 * ClassSize.ATOMIC_INTEGER // majorInProgress, minorInProgress
      + ClassSize.STORE_SERVICES // store services
      + StoreHotnessProtector.FIXED_SIZE
      ;

  @Override
  public long heapSize() {
    // this does not take into account row locks, recent flushes, mvcc entries, and more
    return DEEP_OVERHEAD + stores.values().stream().mapToLong(HStore::heapSize).sum();
  }

  /**
   * Registers a new protocol buffer {@link Service} subclass as a coprocessor endpoint to be
   * available for handling {@link #execService(RpcController, CoprocessorServiceCall)} calls.
   * <p/>
   * Only a single instance may be registered per region for a given {@link Service} subclass (the
   * instances are keyed on {@link ServiceDescriptor#getFullName()}.. After the first registration,
   * subsequent calls with the same service name will fail with a return value of {@code false}.
   * @param instance the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false} otherwise
   */
  public boolean registerService(Service instance) {
    // No stacking of instances is allowed for a single service name
    ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor service {} already registered, rejecting request from {} in region {}",
        serviceName, instance, this);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered coprocessor service: region=" +
        Bytes.toStringBinary(getRegionInfo().getRegionName()) + " service=" + serviceName);
    }
    return true;
  }

  /**
   * Executes a single protocol buffer coprocessor endpoint {@link Service} method using
   * the registered protocol handlers.  {@link Service} implementations must be registered via the
   * {@link #registerService(Service)}
   * method before they are available.
   *
   * @param controller an {@code RpcContoller} implementation to pass to the invoked service
   * @param call a {@code CoprocessorServiceCall} instance identifying the service, method,
   *     and parameters for the method invocation
   * @return a protocol buffer {@code Message} instance containing the method's result
   * @throws IOException if no registered service handler is found or an error
   *     occurs during the invocation
   * @see #registerService(Service)
   */
  public Message execService(RpcController controller, CoprocessorServiceCall call)
    throws IOException {
    String serviceName = call.getServiceName();
    Service service = coprocessorServiceHandlers.get(serviceName);
    if (service == null) {
      throw new UnknownProtocolException(null, "No registered coprocessor service found for " +
          serviceName + " in region " + Bytes.toStringBinary(getRegionInfo().getRegionName()));
    }
    ServiceDescriptor serviceDesc = service.getDescriptorForType();

    cpRequestsCount.increment();
    String methodName = call.getMethodName();
    MethodDescriptor methodDesc =
        CoprocessorRpcUtils.getMethodDescriptor(methodName, serviceDesc);

    Message.Builder builder =
        service.getRequestPrototype(methodDesc).newBuilderForType();

    ProtobufUtil.mergeFrom(builder,
        call.getRequest().toByteArray());
    Message request =
        CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());

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
    IOException exception =
        org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.getControllerException(controller);
    if (exception != null) {
      throw exception;
    }

    return responseBuilder.build();
  }

  public Optional<byte[]> checkSplit() {
    return checkSplit(false);
  }

  /**
   * Return the split point. An empty result indicates the region isn't splittable.
   */
  public Optional<byte[]> checkSplit(boolean force) {
    // Can't split META
    if (this.getRegionInfo().isMetaRegion()) {
      return Optional.empty();
    }

    // Can't split a region that is closing.
    if (this.isClosing()) {
      return Optional.empty();
    }

    if (!force && !splitPolicy.shouldSplit()) {
      return Optional.empty();
    }

    byte[] ret = splitPolicy.getSplitPoint();
    if (ret != null && ret.length > 0) {
      ret = splitRestriction.getRestrictedSplitPoint(ret);
    }

    if (ret != null) {
      try {
        checkRow(ret, "calculated split");
      } catch (IOException e) {
        LOG.error("Ignoring invalid split for region {}", this, e);
        return Optional.empty();
      }
      return Optional.of(ret);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @return The priority that this region should have in the compaction queue
   */
  public int getCompactPriority() {
    if (checkSplit().isPresent() && conf.getBoolean(SPLIT_IGNORE_BLOCKING_ENABLED_KEY, false)) {
      // if a region should split, split it before compact
      return Store.PRIORITY_USER;
    }
    return stores.values().stream().mapToInt(HStore::getCompactPriority).min()
        .orElse(Store.NO_PRIORITY);
  }

  /** @return the coprocessor host */
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
    boolean isInterruptableOp = false;
    switch (op) {
      case GET:  // interruptible read operations
      case SCAN:
        isInterruptableOp = true;
        checkReadsEnabled();
        break;
      case INCREMENT: // interruptible write operations
      case APPEND:
      case PUT:
      case DELETE:
      case BATCH_MUTATE:
      case CHECK_AND_MUTATE:
        isInterruptableOp = true;
        break;
      default:  // all others
        break;
    }
    if (op == Operation.MERGE_REGION || op == Operation.SPLIT_REGION
        || op == Operation.COMPACT_REGION || op == Operation.COMPACT_SWITCH) {
      // split, merge or compact region doesn't need to check the closing/closed state or lock the
      // region
      return;
    }
    if (this.closing.get()) {
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
    }
    lock(lock.readLock());
    // Update regionLockHolders ONLY for any startRegionOperation call that is invoked from
    // an RPC handler
    Thread thisThread = Thread.currentThread();
    if (isInterruptableOp) {
      regionLockHolders.put(thisThread, true);
    }
    if (this.closed.get()) {
      lock.readLock().unlock();
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
    }
    // The unit for snapshot is a region. So, all stores for this region must be
    // prepared for snapshot operation before proceeding.
    if (op == Operation.SNAPSHOT) {
      stores.values().forEach(HStore::preSnapshotOperation);
    }
    try {
      if (coprocessorHost != null) {
        coprocessorHost.postStartRegionOperation(op);
      }
    } catch (Exception e) {
      if (isInterruptableOp) {
        // would be harmless to remove what we didn't add but we know by 'isInterruptableOp'
        // if we added this thread to regionLockHolders
        regionLockHolders.remove(thisThread);
      }
      lock.readLock().unlock();
      throw new IOException(e);
    }
  }

  @Override
  public void closeRegionOperation() throws IOException {
    closeRegionOperation(Operation.ANY);
  }

  @Override
  public void closeRegionOperation(Operation operation) throws IOException {
    if (operation == Operation.SNAPSHOT) {
      stores.values().forEach(HStore::postSnapshotOperation);
    }
    Thread thisThread = Thread.currentThread();
    regionLockHolders.remove(thisThread);
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
  private void startBulkRegionOperation(boolean writeLockNeeded) throws IOException {
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
    regionLockHolders.put(Thread.currentThread(), true);
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding
   * to the try block of #startRegionOperation
   */
  private void closeBulkRegionOperation(){
    regionLockHolders.remove(Thread.currentThread());
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
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i=0; i < listSize; i++) {
        Cell cell = cells.get(i);
        mutationSize += cell.getSerializedSize();
      }
    }

    dataInMemoryWithoutWAL.add(mutationSize);
  }

  private void lock(final Lock lock) throws IOException {
    lock(lock, 1);
  }

  /**
   * Try to acquire a lock.  Throw RegionTooBusyException
   * if failed to get the lock in time. Throw InterruptedIOException
   * if interrupted while waiting for the lock.
   */
  private void lock(final Lock lock, final int multiplier) throws IOException {
    try {
      final long waitTime = Math.min(maxBusyWaitDuration,
          busyWaitDuration * Math.min(multiplier, maxBusyWaitMultiplier));
      if (!lock.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
        // Don't print millis. Message is used as a key over in
        // RetriesExhaustedWithDetailsException processing.
        final String regionName =
          this.getRegionInfo() == null ? "unknown" : this.getRegionInfo().getRegionNameAsString();
        final String serverName = this.getRegionServerServices() == null ?
          "unknown" : (this.getRegionServerServices().getServerName() == null ?
            "unknown" : this.getRegionServerServices().getServerName().toString());
        RegionTooBusyException rtbe = new RegionTooBusyException(
          "Failed to obtain lock; regionName=" + regionName + ", server=" + serverName);
        LOG.warn("Region is too busy to allow lock acquisition.", rtbe);
        throw rtbe;
      }
    } catch (InterruptedException ie) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Interrupted while waiting for a lock in region {}", this);
      }
      throw throwOnInterrupt(ie);
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
          this.wal.sync(txid, false);
          break;
      case FSYNC_WAL:
          this.wal.sync(txid, true);
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
    return regionDurability.ordinal() >  Durability.ASYNC_WAL.ordinal();
  }

  /** @return the latest sequence number that was read from storage when this region was opened */
  public long getOpenSeqNum() {
    return this.openSeqNum;
  }

  @Override
  public Map<byte[], Long> getMaxStoreSeqId() {
    return this.maxSeqIdInStores;
  }

  public long getOldestSeqIdOfStore(byte[] familyName) {
    return wal.getEarliestMemStoreSeqNum(getRegionInfo().getEncodedNameAsBytes(), familyName);
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
    compactionsFinished.increment();
    compactionNumFilesCompacted.add(numFiles);
    compactionNumBytesCompacted.add(filesSizeCompacted);

    assert newValue >= 0;
  }

  public void reportCompactionRequestFailure() {
    compactionsFailed.increment();
  }

  public void incrementCompactionsQueuedCount() {
    compactionsQueued.increment();
  }

  public void decrementCompactionsQueuedCount() {
    compactionsQueued.decrement();
  }

  public void incrementFlushesQueuedCount() {
    flushesQueued.increment();
  }

  protected void decrementFlushesQueuedCount() {
    flushesQueued.decrement();
  }

  /**
   * If a handler thread is eligible for interrupt, make it ineligible. Should be paired
   * with {{@link #enableInterrupts()}.
   */
  void disableInterrupts() {
    regionLockHolders.computeIfPresent(Thread.currentThread(), (t,b) -> false);
  }

  /**
   * If a handler thread was made ineligible for interrupt via {{@link #disableInterrupts()},
   * make it eligible again. No-op if interrupts are already enabled.
   */
  void enableInterrupts() {
    regionLockHolders.computeIfPresent(Thread.currentThread(), (t,b) -> true);
  }

  /**
   * Interrupt any region options that have acquired the region lock via
   * {@link #startRegionOperation(org.apache.hadoop.hbase.regionserver.Region.Operation)},
   * or {@link #startBulkRegionOperation(boolean)}.
   */
  private void interruptRegionOperations() {
    for (Map.Entry<Thread, Boolean> entry: regionLockHolders.entrySet()) {
      // An entry in this map will have a boolean value indicating if it is currently
      // eligible for interrupt; if so, we should interrupt it.
      if (entry.getValue().booleanValue()) {
        entry.getKey().interrupt();
      }
    }
  }

  /**
   * Check thread interrupt status and throw an exception if interrupted.
   * @throws NotServingRegionException if region is closing
   * @throws InterruptedIOException if interrupted but region is not closing
   */
  // Package scope for tests
  void checkInterrupt() throws NotServingRegionException, InterruptedIOException {
    if (Thread.interrupted()) {
      if (this.closing.get()) {
        throw new NotServingRegionException(
          getRegionInfo().getRegionNameAsString() + " is closing");
      }
      throw new InterruptedIOException();
    }
  }

  /**
   * Throw the correct exception upon interrupt
   * @param t cause
   */
  // Package scope for tests
  IOException throwOnInterrupt(Throwable t) {
    if (this.closing.get()) {
      return (NotServingRegionException) new NotServingRegionException(
          getRegionInfo().getRegionNameAsString() + " is closing")
        .initCause(t);
    }
    return (InterruptedIOException) new InterruptedIOException().initCause(t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    this.storeHotnessProtector.update(conf);
    // update coprocessorHost if the configuration has changed.
    if (CoprocessorConfigurationUtil.checkConfigurationChange(getReadOnlyConfiguration(), conf,
      CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)) {
      LOG.info("Update the system coprocessors because the configuration has changed");
      decorateRegionConfiguration(conf);
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    configurationManager = manager;
    stores.values().forEach(manager::registerObserver);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    stores.values().forEach(configurationManager::deregisterObserver);
  }

  @Override
  public CellComparator getCellComparator() {
    return cellComparator;
  }

  public long getMemStoreFlushSize() {
    return this.memstoreFlushSize;
  }


  //// method for debugging tests
  void throwException(String title, String regionName) {
    StringBuilder buf = new StringBuilder();
    buf.append(title + ", ");
    buf.append(getRegionInfo().toString());
    buf.append(getRegionInfo().isMetaRegion() ? " meta region " : " ");
    buf.append("stores: ");
    for (HStore s : stores.values()) {
      buf.append(s.getColumnFamilyDescriptor().getNameAsString());
      buf.append(" size: ");
      buf.append(s.getMemStoreSize().getDataSize());
      buf.append(" ");
    }
    buf.append("end-of-stores");
    buf.append(", memstore size ");
    buf.append(getMemStoreDataSize());
    if (getRegionInfo().getRegionNameAsString().startsWith(regionName)) {
      throw new RuntimeException(buf.toString());
    }
  }

  @Override
  public void requestCompaction(String why, int priority, boolean major,
      CompactionLifeCycleTracker tracker) throws IOException {
    if (major) {
      stores.values().forEach(HStore::triggerMajorCompaction);
    }
    rsServices.getCompactionRequestor().requestCompaction(this, why, priority, tracker,
        RpcServer.getRequestUser().orElse(null));
  }

  @Override
  public void requestCompaction(byte[] family, String why, int priority, boolean major,
      CompactionLifeCycleTracker tracker) throws IOException {
    HStore store = stores.get(family);
    if (store == null) {
      throw new NoSuchColumnFamilyException("column family " + Bytes.toString(family) +
          " does not exist in region " + getRegionInfo().getRegionNameAsString());
    }
    if (major) {
      store.triggerMajorCompaction();
    }
    rsServices.getCompactionRequestor().requestCompaction(this, store, why, priority, tracker,
        RpcServer.getRequestUser().orElse(null));
  }

  private void requestFlushIfNeeded() throws RegionTooBusyException {
    if(isFlushSize(this.memStoreSizing.getMemStoreSize())) {
      requestFlush();
    }
  }

  private void requestFlush() {
    if (this.rsServices == null) {
      return;
    }
    requestFlush0(FlushLifeCycleTracker.DUMMY);
  }

  private void requestFlush0(FlushLifeCycleTracker tracker) {
    boolean shouldFlush = false;
    synchronized (writestate) {
      if (!this.writestate.isFlushRequested()) {
        shouldFlush = true;
        writestate.flushRequested = true;
      }
    }
    if (shouldFlush) {
      // Make request outside of synchronize block; HBASE-818.
      this.rsServices.getFlushRequester().requestFlush(this, tracker);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush requested on " + this.getRegionInfo().getEncodedName());
      }
    } else {
      tracker.notExecuted("Flush already requested on " + this);
    }
  }

  @Override
  public void requestFlush(FlushLifeCycleTracker tracker) throws IOException {
    requestFlush0(tracker);
  }

  /**
   * This method modifies the region's configuration in order to inject replication-related
   * features
   * @param conf region configurations
   */
  private static void decorateRegionConfiguration(Configuration conf) {
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      String plugins = conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,"");
      String replicationCoprocessorClass = ReplicationObserver.class.getCanonicalName();
      if (!plugins.contains(replicationCoprocessorClass)) {
        conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
            (plugins.equals("") ? "" : (plugins + ",")) + replicationCoprocessorClass);
      }
    }
  }

  public Optional<RegionReplicationSink> getRegionReplicationSink() {
    return regionReplicationSink;
  }

  public void addReadRequestsCount(long readRequestsCount) {
    this.readRequestsCount.add(readRequestsCount);
  }

  public void addWriteRequestsCount(long writeRequestsCount) {
    this.writeRequestsCount.add(writeRequestsCount);
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  boolean isReadsEnabled() {
    return this.writestate.readsEnabled;
  }
}
