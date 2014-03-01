 /*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.L2Cache;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
public class HRegion implements HeapSize {
  public static final Log LOG = LogFactory.getLog(HRegion.class);
  static final String SPLITDIR = "splits";
  static final String MERGEDIR = "merges";

  static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  final AtomicBoolean closed = new AtomicBoolean(false);
  /* Closing can take some time; use the closing flag if there is stuff we don't
   * want to do while in closing state; e.g. like offer this region up to the
   * master as a region to close if the carrying regionserver is overloaded.
   * Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  private final Set<byte[]> lockedRows =
    new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  private final Map<Integer, byte []> lockIds =
    new HashMap<Integer, byte []>();
  private int lockIdGenerator = 1;
  static private Random rand = new Random();

  protected final Map<byte [], Store> stores =
    new ConcurrentSkipListMap<byte [], Store>(Bytes.BYTES_RAWCOMPARATOR);

  //These variable are just used for getting data out of the region, to test on
  //client side
  // private int numStores = 0;
  // private int [] storeSize = null;
  // private byte [] name = null;

  protected final AtomicLong memstoreSize = new AtomicLong(0);

  // The number of rows are read
  protected final AtomicInteger rowReadCnt = new AtomicInteger(0);

  // The number of rows are updated
  protected final AtomicInteger rowUpdateCnt = new AtomicInteger(0);

  private HRegionServer regionServer = null;
  /**
   * The open date of the region is stored as a long obtained by currentTimeMillis()
   */
  private long openDate = 0;

  /**
   * The directory for the table this region is part of.
   * This directory contains the directory for this region.
   */
  final Path tableDir;

  final HLog log;
  final FileSystem fs;
  final Configuration conf;
  final Configuration baseConf;
  final HRegionInfo regionInfo;
  final Path regiondir;
  KeyValue.KVComparator comparator;

  private ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

  /*
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

  // When writing store files for this region, replicas will preferrably be
  // placed on these nodes, if non-null.
  private InetSocketAddress[] favoredNodes = null;

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
  }

  final WriteState writestate = new WriteState();

  final long timestampTooNew;
  final long memstoreFlushSize;
  // The maximum size a column family's memstore can grow up to,
  // before being flushed.
  volatile long columnfamilyMemstoreFlushSize =
      HTableDescriptor.DEFAULT_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE;
  // Last flush time for each Store. Useful when we are flushing for each column
  private Map<Store, Long> lastStoreFlushTimeMap
    = new ConcurrentHashMap<Store, Long>();
  private List<Pair<Long,Long>> recentFlushes
    = new ArrayList<Pair<Long,Long>>();
  final FlushRequester flushListener;
  private final long blockingMemStoreSize;
  private final boolean waitOnMemstoreBlock;
  final long threadWakeFrequency;
   // Selective flushing of Column Families which dominate the memstore?
  final boolean perColumnFamilyFlushEnabled;
  // Used to guard splits and closes
  private final ReentrantReadWriteLock splitsAndClosesLock =
    new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock newScannerLock =
    new ReentrantReadWriteLock();

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock =
    new ReentrantReadWriteLock();
  private final Object splitLock = new Object();
  private boolean splitRequest;
  private byte[] splitPoint = null;

  private final MultiVersionConsistencyControl mvcc =
      new MultiVersionConsistencyControl();
  private boolean disableWAL;

  public static volatile AtomicLong writeOps = new AtomicLong(0);
  public static volatile AtomicLong rowLockTime = new AtomicLong(0);
  public static volatile AtomicLong mvccWaitTime = new AtomicLong(0);
  public static volatile AtomicLong memstoreInsertTime = new AtomicLong(0);

  // for simple numeric metrics (# of blocks read from block cache)
  public static final ConcurrentMap<String, AtomicLong>
    numericMetrics = new ConcurrentHashMap<String, AtomicLong>();

  public static final String METRIC_GETSIZE = "getsize";
  public static final String METRIC_NEXTSIZE = "nextsize";

  // for simple numeric metrics (current block cache size)
  // These ones are not reset to zero when queried, unlike the previous.
  public static final ConcurrentMap<String, AtomicLong>
    numericPersistentMetrics = new ConcurrentHashMap<String, AtomicLong>();

  // Used for metrics where we want track a metrics (such as latency)
  // over a number of operations.
  public static final ConcurrentMap<String,
                                  Pair<AtomicLong, AtomicInteger>>
    timeVaryingMetrics =
      new ConcurrentHashMap<String,
                            Pair<AtomicLong, AtomicInteger>>();

  public static void incrNumericMetric(String key, long amount) {
    AtomicLong oldVal = numericMetrics.get(key);
    if (oldVal == null) {
      oldVal = numericMetrics.putIfAbsent(key, new AtomicLong(amount));
      if (oldVal == null)
        return;
    }
    oldVal.addAndGet(amount);
  }

  public static void setNumericMetric(String key, long amount) {
    numericMetrics.put(key, new AtomicLong(amount));
  }

  public static void incrNumericPersistentMetric(String key, long amount) {
    AtomicLong oldVal = numericPersistentMetrics.get(key);
    if (oldVal == null) {
      oldVal = numericPersistentMetrics.putIfAbsent(key, new AtomicLong(amount));
      if (oldVal == null)
        return;
    }
    oldVal.addAndGet(amount);
  }

  public static void clearNumericPersistentMetric(String key) {
    numericPersistentMetrics.remove(key);
  }

  public static void incrTimeVaryingMetric(String key, long amount) {
    Pair<AtomicLong, AtomicInteger> oldVal = timeVaryingMetrics.get(key);
    if (oldVal == null) {
      oldVal = timeVaryingMetrics.putIfAbsent(key,
           new Pair<AtomicLong, AtomicInteger>(new AtomicLong(amount),
                                              new AtomicInteger(1)));
      if (oldVal == null)
        return;
    }
    oldVal.getFirst().addAndGet(amount);  // total time
    oldVal.getSecond().incrementAndGet(); // increment ops by 1
  }

  public static long getNumericMetric(String key) {
    AtomicLong m = numericMetrics.get(key);
    if (m == null)
      return 0;
    return m.get();
  }

  public static Pair<Long, Integer> getTimeVaryingMetric(String key) {
    Pair<AtomicLong, AtomicInteger> pair = timeVaryingMetrics.get(key);
    if (pair == null) {
      return new Pair<Long, Integer>(0L, 0);
    }

    return new Pair<Long, Integer>(pair.getFirst().get(),
        pair.getSecond().get());
  }

  public static long getNumericPersistentMetric(String key) {
    AtomicLong m = numericPersistentMetrics.get(key);
    if (m == null)
      return 0;
    return m.get();
  }

  public String getOpenDateAsString() {
    Date date = new Date(openDate);
    return formatter.format(date);
  }

  public long getopenDate() {
    return openDate;
  }

  public void setOpenDate(long dt) {
    openDate = dt;
  }

  public static final long getWriteOps() {
    return writeOps.getAndSet(0);
  }

  public static final long getRowLockTime()
  {
    return rowLockTime.getAndSet(0);
  }

  public static final long getMVCCWaitTime()
  {
    return mvccWaitTime.getAndSet(0);
  }

  public static final long getMemstoreInsertTime() {
    return memstoreInsertTime.getAndSet(0);
  }

  /**
   * Name of the region info file that resides just under the region directory.
   */
  public final static String REGIONINFO_FILE = ".regioninfo";

  /**
   * Should only be used for testing purposes
   */
  public HRegion(){
    this.tableDir = null;
    this.blockingMemStoreSize = 0L;
    this.waitOnMemstoreBlock = true;
    this.conf = null;
    this.baseConf = null;
    this.flushListener = null;
    this.fs = null;
    this.timestampTooNew = HConstants.LATEST_TIMESTAMP;
    this.memstoreFlushSize = 0L;
    this.columnfamilyMemstoreFlushSize = 0L;
    this.log = null;
    this.regiondir = null;
    this.regionInfo = null;
    this.threadWakeFrequency = 0L;
    this.perColumnFamilyFlushEnabled = false;
    this.scannerReadPoints = new ConcurrentHashMap<RegionScanner, Long>();
    this.openDate = 0;
  }

  /**
   * HRegion copy constructor. Useful when reopening a closed region (normally
   * for unit tests)
   * @param other original object
   */
  public HRegion(HRegion other) {
    this(other.getTableDir(), other.getLog(), other.getFilesystem(),
        other.baseConf, other.getRegionInfo(), null);
  }

  /**
   * HRegion constructor. This constructor should only be used for testing and
   * extensions.  Instances of HRegion should be instantiated with the
   * {@link HRegion#newHRegion(Path, HLog, FileSystem, Configuration, org.apache.hadoop.hbase.HRegionInfo, FlushRequester)} method.
   *
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
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * is new), then read them from the supplied path.
   * @param flushListener an object that implements CacheFlushListener or null
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   *
   * @see HRegion#newHRegion(Path, HLog, FileSystem, Configuration, org.apache.hadoop.hbase.HRegionInfo, FlushRequester)
   */
  public HRegion(Path tableDir, HLog log, FileSystem fs,
      Configuration confParam, final HRegionInfo regionInfo,
      FlushRequester flushListener) {
    this.tableDir = tableDir;
    this.comparator = regionInfo.getComparator();
    this.log = log;
    this.fs = fs;
    if (confParam instanceof CompoundConfiguration) {
      throw new IllegalArgumentException("Need original base configuration");
    }
    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.baseConf = confParam;
    this.conf = new CompoundConfiguration()
        .add(confParam)
        .add(regionInfo.getTableDesc().getValues());
    this.regionInfo = regionInfo;
    this.flushListener = flushListener;
    this.threadWakeFrequency = conf.getLong(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000);
    this.perColumnFamilyFlushEnabled = conf.getBoolean(
            HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH,
            HConstants.DEFAULT_HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH);
    LOG.debug("Per Column Family Flushing: " + perColumnFamilyFlushEnabled);
    String encodedNameStr = this.regionInfo.getEncodedName();
    this.regiondir = new Path(tableDir, encodedNameStr);
    if (LOG.isDebugEnabled()) {
      // Write out region name as string and its encoded name.
      LOG.debug("Creating region " + this);
    }

    /*
     * timestamp.slop provides a server-side constraint on the timestamp. This
     * assumes that you base your TS around currentTimeMillis(). In this case,
     * throw an error to the user if the user-specified TS is newer than now +
     * slop. LATEST_TIMESTAMP == don't use this functionality
     */
    this.timestampTooNew = conf.getLong(
        "hbase.hregion.keyvalue.timestamp.slop.millisecs",
        HConstants.LATEST_TIMESTAMP);

    long flushSize = regionInfo.getTableDesc().getMemStoreFlushSize();
    if (flushSize == HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
         HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
    this.disableWAL = regionInfo.getTableDesc().isWALDisabled();
    this.memstoreFlushSize = flushSize;
    this.blockingMemStoreSize = (long)(this.memstoreFlushSize *
      conf.getFloat(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 2));
    this.waitOnMemstoreBlock =
        conf.getBoolean(HConstants.HREGION_MEMSTORE_WAIT_ON_BLOCK, true);
    this.scannerReadPoints = new ConcurrentHashMap<RegionScanner, Long>();
    // initialize dynamic parameters with current configuration
    this.loadDynamicConf(conf);
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    LOG.info("Online configuration changed!");
    this.loadDynamicConf(conf);
  }

  private static void logIfChange(String varName, long orgV, long newV) {
    if (orgV != newV) {
      LOG.info(String.format("%s changed from %d to %d", varName, orgV, newV));
    }
  }
  /**
   * Load online configurable parameters from a specified Configuration
   */
  private void loadDynamicConf(Configuration conf) {
    long newColumnfamilyMemstoreFlushSize = conf.getLong(
        HConstants.HREGION_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE,
        HTableDescriptor.DEFAULT_MEMSTORE_COLUMNFAMILY_FLUSH_SIZE);
    logIfChange("columnfamilyMemstoreFlushSize",
        this.columnfamilyMemstoreFlushSize, newColumnfamilyMemstoreFlushSize);
    this.columnfamilyMemstoreFlushSize = newColumnfamilyMemstoreFlushSize;
  }

  /**
   * Initialize this region.
   * @return What the next sequence (edit) id should be.
   * @throws IOException e
   */
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
  public long initialize(final Progressable reporter)
  throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Initializing region " + this);
    try {
      // Write HRI to a file in case we need to recover .META.
      status.setStatus("Writing region info on filesystem");
      checkRegioninfoOnFilesystem();

      // Remove temporary data left over from old regions
      status.setStatus("Cleaning up temporary data from old regions");
      cleanupTmpDir();

      // Load in all the HStores.
      //
      // Context: During replay we want to ensure that we do not lose any data. So, we
      // have to be conservative in how we replay logs. For each store, we calculate
      // the maximum seqId up to which the store was flushed. We skip the edits which
      // are equal to or lower than the maxSeqId for each store.
      //
      // We cannot just choose the minimum maxSeqId for all stores, because doing so
      // can cause correctness issues if redundant edits replayed from the logs are
      // not contiguous.

      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(
            Bytes.BYTES_COMPARATOR);

      long maxSeqId = -1;
      // initialized to -1 so that we pick up MemstoreTS from column families
      long maxMemstoreTS = -1;
      Collection<HColumnDescriptor> families =
        this.regionInfo.getTableDesc().getFamilies();

      if (!families.isEmpty()) {
        // initialize the thread pool for opening stores in parallel.
        ThreadPoolExecutor storeOpenerThreadPool =
          StoreThreadUtils.getStoreOpenAndCloseThreadPool(
            "StoreOpenerThread-" + this.regionInfo.getRegionNameAsString(),
            this.getRegionInfo(), this.conf);
        CompletionService<Store> completionService =
          new ExecutorCompletionService<Store>(storeOpenerThreadPool);

        // initialize each store in parallel
        for (final HColumnDescriptor family : families) {
          status.setStatus("Instantiating store for column family " + family);
          completionService.submit(new Callable<Store>() {
            @Override
            public Store call() throws IOException {
              return instantiateHStore(tableDir, family);
            }
          });
        }
        try {
          for (int i = 0; i < families.size(); i++) {
            Future<Store> future = completionService.take();
            Store store = future.get();

            this.stores.put(store.getColumnFamilyName().getBytes(), store);
            // Do not include bulk loaded files when determining seqIdForReplay
            long storeSeqIdforReplay = store.getMaxSequenceId(false);
            maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(),
                storeSeqIdforReplay);

            // Include bulk loaded files when determining seqIdForAssignment
            long storeSeqIdForAssignment = store.getMaxSequenceId(true);
            if (maxSeqId == -1 || storeSeqIdForAssignment > maxSeqId) {
              maxSeqId = storeSeqIdForAssignment;
            }
            long maxStoreMemstoreTS = store.getMaxMemstoreTS();
            if (maxStoreMemstoreTS > maxMemstoreTS) {
              maxMemstoreTS = maxStoreMemstoreTS;
            }
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        } finally {
          storeOpenerThreadPool.shutdownNow();
        }
      }
      mvcc.initialize(maxMemstoreTS + 1);
      // Recover any edits if available.
      maxSeqId = Math.max(maxSeqId, replayRecoveredEditsIfAny(
          this.regiondir, maxSeqIdInStores, reporter, status));

      // Get rid of any splits or merges that were lost in-progress.  Clean out
      // these directories here on open.  We may be opening a region that was
      // being split but we crashed in the middle of it all.
      status.setStatus("Cleaning up detritus from prior splits");
      FSUtils.deleteDirectory(this.fs, new Path(regiondir, SPLITDIR));
      FSUtils.deleteDirectory(this.fs, new Path(regiondir, MERGEDIR));

      // See if region is meant to run read-only.
      if (this.regionInfo.getTableDesc().isReadOnly()) {
        this.writestate.setReadOnly(true);
      }

      this.writestate.compacting = 0;
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      for (Store store : stores.values()) {
        this.lastStoreFlushTimeMap.put(store, startTime);
      }

      // Use maximum of log sequenceid or that which was found in stores
      // (particularly if no recovered edits, seqid will be -1).
      long nextSeqid = maxSeqId + 1;
      LOG.info("Onlined " + this.toString() + "; next sequenceid=" + nextSeqid);
      status.markComplete("Region opened successfully");
      return nextSeqid;
    } finally {
      // prevent MonitoredTask leaks due to thrown exceptions
      status.cleanup();
    }
  }

  /*
   * Move any passed HStore files into place (if any).  Used to pick up split
   * files and any merges from splits and merges dirs.
   * @param initialFiles
   * @throws IOException
   */
  private static void moveInitialFilesIntoPlace(final FileSystem fs,
    final Path initialFiles, final Path regiondir)
  throws IOException {
    if (initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, regiondir);
    }
  }

  /**
   * @return True if this region has references.
   */
  boolean hasReferences() {
    for (Store store : this.stores.values()) {
      for (StoreFile sf : store.getStorefiles()) {
        // Found a reference, return.
        if (sf.isReference()) return true;
      }
    }
    return false;
  }

  /*
   * Write out an info file under the region directory.  Useful recovering
   * mangled regions.
   * @throws IOException
   */
  private void checkRegioninfoOnFilesystem() throws IOException {
    // Name of this file has two leading and trailing underscores so it doesn't
    // clash w/ a store/family name.  There is possibility, but assumption is
    // that its slim (don't want to use control character in filename because
    //
    Path regioninfo = new Path(this.regiondir, REGIONINFO_FILE);
    if (this.fs.exists(regioninfo) &&
        this.fs.getFileStatus(regioninfo).getLen() > 0) {
      return;
    }
    FSDataOutputStream out = this.fs.create(regioninfo, true);
    try {
      this.regionInfo.write(out);
      out.write('\n');
      out.write('\n');
      out.write(Bytes.toBytes(this.regionInfo.toString()));
    } finally {
      out.close();
    }
  }

  public AtomicLong getMemstoreSize() {
    return memstoreSize;
  }

  public HRegionServer getRegionServer() {
    return regionServer;
  }

  public void setRegionServer(HRegionServer regionServer) {
    this.regionServer = regionServer;
    regionServer.getGlobalMemstoreSize().getAndAdd(this.memstoreSize.get());
  }

  /**
   * Increase the size of mem store in this region and the sum of global mem
   * stores' size
   * @param delta
   * @return the size of memstore in this region
   */
  public long incMemoryUsage(long delta) {
    if (this.regionServer != null)
      this.regionServer.getGlobalMemstoreSize().addAndGet(delta);
    long vl = this.memstoreSize.addAndGet(delta);
    if (vl < 0) {
      LOG.error(String.format("memstoreSize of region %s becomes negative %d "
          + "increased by %d called by %s", this.toString(), vl, delta, Thread
          .currentThread().getStackTrace()[2]));
    }
    return vl;
  }

  /** @return a HRegionInfo object for this region */
  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
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

  boolean areWritesEnabled() {
    synchronized(this.writestate) {
      return this.writestate.writesEnabled;
    }
  }

   public MultiVersionConsistencyControl getMVCC() {
     return mvcc;
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
  public List<StoreFile> close() throws IOException {
    return close(false);
  }

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
  public List<StoreFile> close(final boolean abort) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Closing region " + this + (abort ? " due to abort" : ""));
    if (isClosed()) {
      status.abort("Already got closed by another process");
      LOG.warn("region " + this + " already closed");
      return null;
    }
    status.setStatus("Waiting for split lock");
    synchronized (splitLock) {
      status.setStatus("Disabling compacts and flushes for region");
      synchronized (writestate) {
        // Disable compacting and flushing by background threads for this
        // region.
        writestate.writesEnabled = false;
        LOG.debug("Closing " + this + ": disabling compactions & flushes");
        while (writestate.compacting > 0 || writestate.flushing) {
          LOG.debug("waiting for " + writestate.compacting + " compactions" +
              (writestate.flushing ? " & cache flush" : "") +
              " to complete for region " + this);
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
      }

      // First flush clears content in either snapshots or current memstores
      if (!abort) {
        status.setStatus("Pre-flushing region before close");
        LOG.info("Running close preflush of " + this.getRegionNameAsString());

        try {
          internalFlushcache(status);
        } catch (IOException ioe) {
          // Failed to flush the region but probably it is still able to serve request,
          // so re-enable writes to it.
          status.setStatus("Failed to flush the region, putting it online again");
          synchronized (writestate) {
            writestate.writesEnabled = true;
          }
          throw ioe;
        }
      }
      newScannerLock.writeLock().lock();
      this.closing.set(true);
      status.setStatus("Disabling writes for close");
      try {
        splitsAndClosesLock.writeLock().lock();
        LOG.debug("Updates disabled for region, no outstanding scanners on " +
          this);
        try {
          // Write lock means no more row locks can be given out.  Wait on
          // outstanding row locks to come in before we close so we do not drop
          // outstanding updates.
          waitOnRowLocks();
          LOG.debug("No more row locks outstanding on region " + this);

          // Second flush to ensure no unflushed data in memory.
          if (!abort) {
            try {
              internalFlushcache(status);
            } catch (IOException ioe) {
              status.setStatus("Failed to flush the region, putting it online again");
              synchronized (writestate) {
                writestate.writesEnabled = true;
              }
              this.closing.set(false);
              throw ioe;
            }
          }
          List<StoreFile> result = new ArrayList<StoreFile>();

          if (!stores.isEmpty()) {
            // initialize the thread pool for closing stores in parallel.
            ThreadPoolExecutor storeCloserThreadPool =
              StoreThreadUtils.getStoreOpenAndCloseThreadPool("StoreCloserThread-"
                + this.regionInfo.getRegionNameAsString(), this.getRegionInfo(), this.conf);
            CompletionService<ImmutableList<StoreFile>> completionService =
              new ExecutorCompletionService<ImmutableList<StoreFile>>(
                storeCloserThreadPool);

            // close each store in parallel
            for (final Store store : stores.values()) {
              assert store.getFlushableMemstoreSize() == 0;
              completionService
                  .submit(new Callable<ImmutableList<StoreFile>>() {
                    @Override
                    public ImmutableList<StoreFile> call() throws IOException {
                      ImmutableList<StoreFile> result = store.close();
                      HRegionServer.configurationManager.
                              deregisterObserver(store);
                      return result;
                    }
                  });
            }
            try {
              for (int i = 0; i < stores.size(); i++) {
                Future<ImmutableList<StoreFile>> future = completionService
                    .take();
                ImmutableList<StoreFile> storeFileList = future.get();
                result.addAll(storeFileList);
              }
            } catch (InterruptedException e) {
              throw new IOException(e);
            } catch (ExecutionException e) {
              throw new IOException(e.getCause());
            } finally {
              storeCloserThreadPool.shutdownNow();
            }
          }
          this.closed.set(true);
          if (memstoreSize.get() != 0) {
            LOG.error("Memstore size should be 0 after clean region close, but is " + memstoreSize.get());
          }
          status.markComplete("Closed");
          LOG.info("Closed " + this);
          return result;
        } finally {
          splitsAndClosesLock.writeLock().unlock();
        }
      } finally {
        newScannerLock.writeLock().unlock();
        // prevent MonitoredTask leaks due to thrown exceptions
        status.cleanup();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public byte [] getStartKey() {
    return this.regionInfo.getStartKey();
  }

  /** @return end key for region */
  public byte [] getEndKey() {
    return this.regionInfo.getEndKey();
  }

  /** @return region id */
  public long getRegionId() {
    return this.regionInfo.getRegionId();
  }

  /** @return region name */
  public byte [] getRegionName() {
    return this.regionInfo.getRegionName();
  }

  /** @return region name as string for logging */
  public String getRegionNameAsString() {
    return this.regionInfo.getRegionNameAsString();
  }

  /** @return HTableDescriptor for this region */
  public HTableDescriptor getTableDesc() {
    return this.regionInfo.getTableDesc();
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /** @return region directory Path */
  public Path getRegionDir() {
    return this.regiondir;
  }

  /** @return FileSystem being used by this region */
  public FileSystem getFilesystem() {
    return this.fs;
  }

  /**
   * @return Returns the earliest time a store in the region
   *         was flushed. All other stores in the region would
   *         have been flushed either at, or after this time.
   */
  public long getMinFlushTimeForAllStores() {
    return Collections.min(this.lastStoreFlushTimeMap.values());
  }

  /**
   * Returns the last time a particular store was flushed
   * @param store The store in question
   * @return The last time this store was flushed
   */
  public long getLastStoreFlushTime(Store store) {
    return this.lastStoreFlushTimeMap.get(store);
  }

  /** @return how info about the last flushes <time, size> */
  public List<Pair<Long,Long>> getRecentFlushInfo() {
    // only MemStoreFlusher thread should be calling this, so read lock is okay
    this.splitsAndClosesLock.readLock().lock();

    List<Pair<Long,Long>> ret = this.recentFlushes;
    this.recentFlushes = new ArrayList<Pair<Long,Long>>();

    this.splitsAndClosesLock.readLock().unlock();
    return ret;
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
    for (Store h: stores.values()) {
      long storeSize = h.getSize();
      if (storeSize > size) {
        size = storeSize;
      }
    }
    return size;
  }

  /*
   * Split the HRegion to create two brand-new ones.  This also closes
   * current HRegion.  Split should be fast since we don't rewrite store files
   * but instead create new 'reference' store files that read off the top and
   * bottom ranges of parent store files.
   * @param splitRow row on which to split region
   * @return two brand-new HRegions or null if a split is not needed
   * @throws IOException
   */
  HRegion [] splitRegion(final byte [] splitRow) throws IOException {
    prepareToSplit();
    synchronized (splitLock) {
      if (closed.get()) {
        return null;
      }
      // Add start/end key checking: hbase-428.
      byte [] startKey = this.regionInfo.getStartKey();
      byte [] endKey = this.regionInfo.getEndKey();
      if (this.comparator.matchingRows(startKey, 0, startKey.length,
          splitRow, 0, splitRow.length)) {
        LOG.debug("Startkey and midkey are same, not splitting");
        return null;
      }
      if (this.comparator.matchingRows(splitRow, 0, splitRow.length,
          endKey, 0, endKey.length)) {
        LOG.debug("Endkey and midkey are same, not splitting");
        return null;
      }
      LOG.info("Starting split of region " + this);
      Path splits = new Path(this.regiondir, SPLITDIR);
      if(!this.fs.exists(splits)) {
        this.fs.mkdirs(splits);
      }
      // Calculate regionid to use.  Can't be less than that of parent else
      // it'll insert into wrong location over in .META. table: HBASE-710.
      long rid = EnvironmentEdgeManager.currentTimeMillis();
      if (rid < this.regionInfo.getRegionId()) {
        LOG.warn("Clock skew; parent regions id is " +
          this.regionInfo.getRegionId() + " but current time here is " + rid);
        rid = this.regionInfo.getRegionId() + 1;
      }
      HRegionInfo regionAInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        startKey, splitRow, false, rid);
      Path dirA = getSplitDirForDaughter(splits, regionAInfo);
      HRegionInfo regionBInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        splitRow, endKey, false, rid);
      Path dirB = getSplitDirForDaughter(splits, regionBInfo);

      // Now close the HRegion.  Close returns all store files or null if not
      // supposed to close (? What to do in this case? Implement abort of close?)
      // Close also does wait on outstanding rows and calls a flush just-in-case.
      List<StoreFile> hstoreFilesToSplit = close(false);
      if (hstoreFilesToSplit == null) {
        LOG.warn("Close came back null (Implement abort of close?)");
        throw new RuntimeException("close returned empty vector of HStoreFiles");
      }

      // Split each store file.
      for(StoreFile h: hstoreFilesToSplit) {
        StoreFile.split(fs,
          Store.getStoreHomedir(splits, regionAInfo.getEncodedName(),
            h.getFamily()),
          h, splitRow, Range.bottom);
        StoreFile.split(fs,
          Store.getStoreHomedir(splits, regionBInfo.getEncodedName(),
            h.getFamily()),
          h, splitRow, Range.top);
      }

      // Create a region instance and then move the splits into place under
      // regionA and regionB.
      HRegion regionA =
        HRegion.newHRegion(tableDir, log, fs, baseConf, regionAInfo, null);
      moveInitialFilesIntoPlace(this.fs, dirA, regionA.getRegionDir());
      HRegion regionB =
        HRegion.newHRegion(tableDir, log, fs, baseConf, regionBInfo, null);
      moveInitialFilesIntoPlace(this.fs, dirB, regionB.getRegionDir());

      return new HRegion [] {regionA, regionB};
    }
  }

  /*
   * Get the daughter directories in the splits dir.  The splits dir is under
   * the parent regions' directory.
   * @param splits
   * @param hri
   * @return Path to split dir.
   * @throws IOException
   */
  private Path getSplitDirForDaughter(final Path splits, final HRegionInfo hri)
  throws IOException {
    Path d =
      new Path(splits, hri.getEncodedName());
    if (fs.exists(d)) {
      // This should never happen; the splits dir will be newly made when we
      // come in here.  Even if we crashed midway through a split, the reopen
      // of the parent region clears out the dir in its initialize method.
      throw new IOException("Cannot split; target file collision at " + d);
    }
    return d;
  }

  protected void prepareToSplit() {
    // nothing
  }

  /*
   * Do preparation for pending compaction.
   * @throws IOException
   */
  void doRegionCompactionPrep() throws IOException {
  }

  /*
   * Removes the temporary directory for this Store.
   */
  private void cleanupTmpDir() throws IOException {
    FSUtils.deleteDirectory(this.fs, getTmpDir());
  }

  /**
   * Get the temporary diretory for this region. This directory
   * will have its contents removed when the region is reopened.
   */
  Path getTmpDir() {
    return new Path(getRegionDir(), ".tmp");
  }

  void triggerMajorCompaction() {
    for (Store h: stores.values()) {
      h.triggerMajorCompaction();
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
   * @param majorCompaction True to force a major compaction regardless of thresholds
   * @return split row if split is needed
   * @throws IOException e
   */
  public byte [] compactStores(final boolean majorCompaction)
  throws IOException {
    if (majorCompaction) {
      this.triggerMajorCompaction();
    }
    return compactStores();
  }

  /**
   * Compact all the stores and return the split key of the first store that needs
   * to be split.
   */
  public byte[] compactStores() throws IOException {

    // first compact all stores
    for (Store s : getStores().values()) {
      CompactionRequest cr = s.requestCompaction();
      if(cr != null) {
        try {
          compact(cr);
        } finally {
          s.finishRequest(cr);
        }
      }
    }

    // if for some reason we still have references, we can't split further
    if (hasReferences()) {
      return null;
    }

    // check if we need to split now; and if so find the midkey
    for (Store s : getStores().values()) {
      byte[] splitRow = s.checkSplit();
      if (splitRow != null) {
        return splitRow;
      }
    }
    return null;
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
   * @param cr Compaction details, obtained by requestCompaction()
   * @return whether the compaction completed
   * @throws IOException e
   */
  public boolean compact(CompactionRequest cr)
  throws IOException {
    if (cr == null) {
      return false;
    }
    if (this.closing.get() || this.closed.get()) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      return false;
    }
    Preconditions.checkArgument(cr.getHRegion().equals(this));
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        return false;
      }
      try {
        synchronized (writestate) {
          if (writestate.writesEnabled) {
            ++writestate.compacting;
          } else {
            LOG.info("NOT compacting region " + this + ". Writes disabled.");
            return false;
          }
        }
        LOG.info("Starting compaction on " + cr.getStore() + " in region "
            + this + (cr.getCompactSelection().isOffPeakCompaction()?" as an off-peak compaction":""));
        doRegionCompactionPrep();
        boolean completed = false;
        try {
          cr.getStore().compact(cr);
          completed = true;
        } catch (InterruptedIOException iioe) {
          LOG.info("compaction interrupted by user: ", iioe);
        }
        return completed;
      } finally {
        synchronized (writestate) {
          --writestate.compacting;
          if (writestate.compacting <= 0) {
            writestate.notifyAll();
          }
        }
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Flush the cache, while disabling selective flushing.
   *
   * @return
   * @throws IOException
   */
  public boolean flushcache() throws IOException {
    return flushcache(false);
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
   * @param selectiveFlushRequest If true, selectively flush column families
   *                              which dominate the memstore size, provided it
   *                              is enabled in the configuration.
   *
   * @return true if cache was flushed
   *
   * @throws IOException general io exceptions
   */
  public boolean flushcache(boolean selectiveFlushRequest) throws IOException {
    // If a selective flush was requested, but the per-column family switch is
    // off, we cannot do a selective flush.
    if (selectiveFlushRequest && !perColumnFamilyFlushEnabled) {
      LOG.debug("Disabling selective flushing of Column Families' memstores.");
      selectiveFlushRequest = false;
    }

    MonitoredTask status = TaskMonitor.get().createStatus("Flushing " + this);
    try {
      if (this.closed.get()) {
        status.abort("Skipped: closed");
        return false;
      }
      synchronized (writestate) {
        if (!writestate.flushing && writestate.writesEnabled) {
          this.writestate.flushing = true;
        } else {
          if(LOG.isDebugEnabled()) {
            LOG.debug("NOT flushing memstore for region " + this +
              ", flushing=" +
                writestate.flushing + ", writesEnabled=" +
                writestate.writesEnabled);
          }
          status.abort("Not flushing since " + (writestate.flushing ?
                "already flushing" : "writes not enabled"));
          return false;
        }
      }
      Collection<Store> specificStoresToFlush = null;
      try {
        // Prevent splits and closes
        status.setStatus("Acquiring readlock on region");
        splitsAndClosesLock.readLock().lock();
        try {
          // We now have to flush the memstore since it has
          // reached the threshold, however, we might not need
          // to flush the entire memstore. If there are certain
          // column families that are dominating the memstore size,
          // we will flush just those. The second behavior only
          // happens when selectiveFlushRequest is true.
          boolean result;

          // If it is okay to flush the memstore by selecting the
          // column families which dominate the size, we are going
          // to populate the specificStoresToFlush set.
          if (selectiveFlushRequest) {
            specificStoresToFlush = new HashSet<Store>();
            for (Store store : stores.values()) {
              if (shouldFlushStore(store)) {
                specificStoresToFlush.add(store);
                LOG.debug("Column Family: " + store.getColumnFamilyName() +
                          " of region " + this + " will be flushed");
              }
            }
            // Didn't find any CFs which were above the threshold for selection.
            if (specificStoresToFlush.size() == 0) {
              LOG.debug("Since none of the CFs were above the size, flushing all.");
              specificStoresToFlush = stores.values();
            }
          } else {
            specificStoresToFlush = stores.values();
          }
          result = internalFlushcache(specificStoresToFlush, status);
          status.markComplete("Flush successful");
          return result;
        } finally {
          splitsAndClosesLock.readLock().unlock();
        }
      } finally {
        synchronized (writestate) {
          writestate.flushing = false;
          this.writestate.flushRequested = false;
          writestate.notifyAll();
        }
      }
    } finally {
      // prevent MonitoredTask leaks due to thrown exceptions
      status.cleanup();
    }
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
   * @return true if the region needs compacting
   *
   * @throws IOException general io exceptions
   */
  protected boolean internalFlushcache(MonitoredTask status)
          throws IOException {
    return internalFlushcache(this.log, -1L, stores.values(), status);
  }

  /**
   * See {@link #internalFlushcache(org.apache.hadoop.hbase.monitoring.MonitoredTask)}
   * @param storesToFlush The specific stores to flush.
   * @param status
   * @return
   * @throws IOException
   */
  protected boolean internalFlushcache(Collection<Store> storesToFlush,
                                       MonitoredTask status)
          throws IOException {
    return internalFlushcache(this.log, -1L, storesToFlush, status);
  }

  protected boolean internalFlushcache(final HLog wal, final long myseqid,
                                       MonitoredTask status)
          throws IOException {
    return internalFlushcache(wal, myseqid, stores.values(), status);
  }

  /**
   * @param wal Null if we're NOT to go via hlog/wal.
   * @param myseqid The seqid to use if <code>wal</code> is null writing out
   * flush file.
   * @param storesToFlush The list of stores to flush.
   * @param status
   * @return true if the region needs compacting
   * @throws IOException
   * @see {@link #internalFlushcache()}
   */
  protected boolean internalFlushcache(final HLog wal, final long myseqid,
      Collection<Store> storesToFlush, MonitoredTask status) throws IOException {
    final long startTime = EnvironmentEdgeManager.currentTimeMillis();
    // If nothing to flush, return and avoid logging start/stop flush.
    if (this.memstoreSize.get() <= 0) {
      if (this.memstoreSize.get() < 0) {
        LOG.error(String.format(
            "HRegion.memstoreSize is a negative number %ld for region %s",
            memstoreSize.get(), this.toString()));
      }
      // Since there is nothing to flush, we will reset the flush times for all
      // the stores.
      for (Store store : stores.values()) {
        lastStoreFlushTimeMap.put(store, startTime);
      }
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Started a " +
        "memstore flush for region " + this + ", with current memstore size: " +
        StringUtils.humanReadableInt(this.memstoreSize.get()) +
        ", and " + storesToFlush.size() + "/" + stores.size() +
        " column families' memstores are being flushed." +
        ((wal != null)? "": "; wal is null, using passed myseqid=" + myseqid));
      for (Store store : storesToFlush) {
        LOG.debug("Flushing Column Family: " + store.getColumnFamilyName() +
                  " which was occupying " +
                  StringUtils.humanReadableInt(store.getFlushableMemstoreSize()) +
                  " of memstore.");
      }
    }

    // Stop updates while we snapshot the memstore of all stores. We only have
    // to do this for a moment.  Its quick.  The subsequent sequence id that
    // goes into the HLog after we've flushed all these snapshots also goes
    // into the info file that sits beside the flushed files.
    // We also set the memstore size to zero here before we allow updates
    // again so its value will represent the size of the updates received
    // during the flush
    long sequenceId = -1L;

    // We have to take a write lock during snapshot, or else a write could
    // end up in both snapshot and memstore (makes it difficult to do atomic
    // rows then)
    status.setStatus("Obtaining lock to block concurrent updates");
    long t0, t1;

    this.updatesLock.writeLock().lock();
    t0 = EnvironmentEdgeManager.currentTimeMillis();
    status.setStatus("Preparing to flush by snapshotting stores");
    long totalMemstoreSizeOfFlushableStores = 0;

    Set<Store> storesNotToFlush = new HashSet<Store>(stores.values());
    storesNotToFlush.removeAll(storesToFlush);

    // Calculate the smallest LSN numbers for edits in the stores that will
    // be flushed and the ones which won't be. This will be used to populate
    // the firstSeqWrittenInCurrentMemstore and
    // firstSeqWrittenInSnapshotMemstore maps correctly.
    long firstSeqIdInStoresToFlush = Long.MAX_VALUE;
    for (Store store : storesToFlush) {
      firstSeqIdInStoresToFlush = Math.min(firstSeqIdInStoresToFlush,
        store.getSmallestSeqNumberInMemstore());
    }

    long firstSeqIdInStoresNotToFlush = Long.MAX_VALUE;
    for (Store store : storesNotToFlush) {
      firstSeqIdInStoresNotToFlush = Math.min(firstSeqIdInStoresNotToFlush,
        store.getSmallestSeqNumberInMemstore());
    }

    //copy the array of per column family memstore values
    List<StoreFlusher> storeFlushers = new ArrayList<StoreFlusher>(
        storesToFlush.size());
    try {
      if (wal != null) {
        sequenceId = wal.startCacheFlush(this.regionInfo.getRegionName(),
          firstSeqIdInStoresToFlush, firstSeqIdInStoresNotToFlush);
      } else {
        sequenceId = myseqid;
      }
      for (Store s : storesToFlush) {
        totalMemstoreSizeOfFlushableStores += s.getFlushableMemstoreSize();
        storeFlushers.add(s.getStoreFlusher(sequenceId));
      }

      // prepare flush (take a snapshot)
      for (StoreFlusher flusher : storeFlushers) {
        flusher.prepare();
      }
    } finally {
      this.updatesLock.writeLock().unlock();
      t1 = EnvironmentEdgeManager.currentTimeMillis();
      LOG.debug("Finished snapshotting. Held region-wide updates lock for "
        + (t1-t0) + " ms.");
    }

    status.setStatus("Flushing stores");
    LOG.debug("Finished snapshotting, commencing flushing stores");

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;

    // A.  Flush memstore to all the HStores.
    // Keep running vector of all store files that includes both old and the
    // just-made new flush store file.
    try {
      for (StoreFlusher flusher : storeFlushers) {
        flusher.flushCache(status);
      }
    } catch (IOException ioe) {
      if (wal != null) {
        wal.abortCacheFlush(this.regionInfo.getRegionName());
      }
      status.abort("Flush failed: " + StringUtils.stringifyException(ioe));
      // The caller can recover from this IOException. No harm done if
      // memstore flush fails.
      for (StoreFlusher flusher : storeFlushers) {
        flusher.cancel();
      }
      throw ioe;
    }

    try {

      Callable<Void> atomicWork = internalPreFlushcacheCommit();

      LOG.debug("Caches flushed, doing commit now (which includes update scanners)");

      /**
       * Switch between memstore(snapshot) and the new store file
       */
      if (atomicWork != null) {
        LOG.debug("internalPreFlushcacheCommit gives us work to do, acquiring newScannerLock");
        newScannerLock.writeLock().lock();
      }

      try {
        if (atomicWork != null) {
          atomicWork.call();
        }

        // Switch snapshot (in memstore) -> new hfile (thus causing
        // all the store scanners to reset/reseek).
        for (StoreFlusher flusher : storeFlushers) {
          boolean needsCompaction = flusher.commit();
          if (needsCompaction) {
            compactionRequested = true;
          }
        }
      } finally {
        if (atomicWork != null) {
          newScannerLock.writeLock().unlock();
        }
      }

      storeFlushers.clear();

      // Set down the memstore size by amount of flush.
      this.incMemoryUsage(-totalMemstoreSizeOfFlushableStores);
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The hlog needs to be replayed so its content is restored to memstore.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.

      // There is no recovery possible from failures in this block. Abort.
      // But now that we have factored out memstore flushing in a separate
      // block, there shouldn't really be any exception here.
      LOG.fatal("failed to commit flushed memstore. Aborting.", t);
      Runtime.getRuntime().halt(1);
    }

    // If we get to here, the HStores have been written. If we get an
    // error in completeCacheFlush it will release the lock it is holding
    // update lastFlushTime after the HStores have been written.
    for (Store store : storesToFlush) {
      this.lastStoreFlushTimeMap.put(store, startTime);
    }

    // B.  Write a FLUSHCACHE-COMPLETE message to the log.
    //     This tells future readers that the HStores were emitted correctly,
    //     and that all updates to the log for this regionName that have lower
    //     log-sequence-ids can be safely ignored.
    if (wal != null) {
      wal.completeCacheFlush(getRegionName(),
        regionInfo.getTableDesc().getName(), sequenceId,
        this.getRegionInfo().isMetaRegion());
    }

    // Update the last flushed sequence id for region
    if (this.regionServer != null) {
      this.regionServer.getServerInfo().setFlushedSequenceIdForRegion(
          getRegionName(),
          sequenceId);
    }
    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }

    long time = EnvironmentEdgeManager.currentTimeMillis() - startTime;
    if (LOG.isDebugEnabled()) {
      LOG.info("Finished memstore flush of ~" +
        StringUtils.humanReadableInt(totalMemstoreSizeOfFlushableStores) +
        " for region " + this + " in " + time + "ms, sequence id=" +
        sequenceId + ", compaction requested=" + compactionRequested +
        ((wal == null)? "; wal=null": ""));
      status.setStatus("Finished memstore flush");
    }
    this.recentFlushes.add(
            new Pair<Long,Long>(time/1000, totalMemstoreSizeOfFlushableStores));

    return compactionRequested;
  }

   /**
    * A hook for sub classed wishing to perform operations prior to the cache
    * flush commit stage.
    *
    * If a subclass wishes that an atomic update of their work and the
    * flush commit stage happens, they should return a callable. The new scanner
    * lock will be acquired and released.

    * @throws java.io.IOException allow children to throw exception
    */
   protected Callable<Void> internalPreFlushcacheCommit() throws IOException {
     return null;
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
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    KeyValue key = null;
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      Store store = getStore(family);
      KeyValue kv = new KeyValue(row, HConstants.LATEST_TIMESTAMP);
      // get the closest key. (HStore.getRowKeyAtOrBefore can return null)
      key = store.getRowKeyAtOrBefore(kv);
      if (key == null) {
        return null;
      }
      Get get = new Get(key.getRow());
      get.addFamily(family);
      return get(get, null);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated
   * columns and rows specified by the {@link Scan}.
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @return InternalScanner
   * @throws IOException read exceptions
   */
  public InternalScanner getScanner(Scan scan) throws IOException {
    return getScanner(scan, null);
  }

  protected InternalScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners) throws IOException {
    newScannerLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new NotServingRegionException("Region " + this + " closed");
      }
      // Verify families are all valid
      if(scan.hasFamilies()) {
        for(byte [] family : scan.getFamilyMap().keySet()) {
          checkFamily(family);
        }
      } else { // Adding all families to scanner
        for(byte[] family: regionInfo.getTableDesc().getFamiliesKeys()){
          scan.addFamily(family);
        }
      }
      return instantiateInternalScanner(scan, additionalScanners);

    } finally {
      newScannerLock.readLock().unlock();
    }
  }

  protected InternalScanner instantiateInternalScanner(Scan scan, List<KeyValueScanner> additionalScanners) throws IOException {
    // return new RegionScanner(scan, additionalScanners);
    RegionContext regionContext = new RegionContext(stores, scannerReadPoints,
        comparator, mvcc, closing, closed, regionInfo, rowReadCnt);
    return new RegionScanner(scan, additionalScanners, regionContext,
        HRegionServer.scanPrefetchThreadPool);
  }

  /*
   * @param delete The passed delete is modified by this method. WARNING!
   */
  private void prepareDeleteFamilyMap(Delete delete) throws IOException {
    // Check to see if this is a deleteRow insert
    if(delete.getFamilyMap().isEmpty()){
      for(byte [] family : regionInfo.getTableDesc().getFamiliesKeys()){
        // Don't eat the timestamp
        delete.deleteFamily(family, delete.getTimeStamp());
      }
    } else {
      for(byte [] family : delete.getFamilyMap().keySet()) {
        if(family == null) {
          throw new NoSuchColumnFamilyException("Empty family is invalid");
        }
        checkFamily(family);
      }
    }
  }

  /**
   * @return the nodes on which to place replicas of all store files, or null if
   * there are no favored nodes.
   */
  public InetSocketAddress[] getFavoredNodes() {
    return this.favoredNodes;
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Set the favored nodes on which to place replicas of all store files. The
   * array can be null to set no preference for favored nodes, but elements of
   * the array must not be null. Placement of replicas on favored nodes is best-
   * effort only and the filesystem may choose different nodes.
   * @param favoredNodes the favored nodes, or null
   */
  public void setFavoredNodes(InetSocketAddress[] favoredNodes) {
    this.favoredNodes = favoredNodes;
  }

  /**
   * @param delete delete object
   * @param lockid existing lock id, or null for grab a lock
   * @param writeToWAL append to the write ahead lock or not
   * @throws IOException read exceptions
   */
  public void delete(Delete delete, Integer lockid, boolean writeToWAL)
  throws IOException {
    checkReadOnly();
    checkResources();
    Integer lid = null;
    splitsAndClosesLock.readLock().lock();
    try {
      byte [] row = delete.getRow();
      // If we did not pass an existing row lock, obtain a new one
      lid = getLock(lockid, row, true);

      // All edits for the given row (across all column families) must happen atomically.
      prepareDeleteFamilyMap(delete);
      delete(delete.getFamilyMap(), writeToWAL);

    } finally {
      if(lockid == null) releaseRowLock(lid);
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Setup a Delete object with correct timestamps.
   * Caller should the row and region locks.
   * @param delete
   * @param now
   * @throws IOException
   */
  private void prepareDeleteTimestamps(Map<byte[], List<KeyValue>> familyMap, byte[] byteNow)
      throws IOException {
    for (Map.Entry<byte[], List<KeyValue>> e : familyMap.entrySet()) {

      byte[] family = e.getKey();
      List<KeyValue> kvs = e.getValue();
      Map<byte[], Integer> kvCount = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

      for (KeyValue kv: kvs) {
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

          List<KeyValue> result = get(get);

          if (result.size() < count) {
            // Nothing to delete
            kv.updateLatestStamp(byteNow);
            continue;
          }
          if (result.size() > count) {
            throw new RuntimeException("Unexpected size: " + result.size());
          }
          KeyValue getkv = result.get(count - 1);
          Bytes.putBytes(kv.getBuffer(), kv.getTimestampOffset(),
              getkv.getBuffer(), getkv.getTimestampOffset(), Bytes.SIZEOF_LONG);
        } else {
          kv.updateLatestStamp(byteNow);
        }
      }
    }
  }


  /**
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL
   * @throws IOException
   */
  public void delete(Map<byte[], List<KeyValue>> familyMap, boolean writeToWAL)
  throws IOException {
    long now = EnvironmentEdgeManager.currentTimeMillis();

    byte [] byteNow = Bytes.toBytes(now);
    boolean flush = false;

    updatesLock.readLock().lock();

    try {
      prepareDeleteTimestamps(familyMap, byteNow);

      long seqNum = -1;
      if (!this.disableWAL && writeToWAL) {
        // write/sync to WAL should happen before we touch memstore.
        //
        // If order is reversed, i.e. we write to memstore first, and
        // for some reason fail to write/sync to commit log, the memstore
        // will contain uncommitted transactions.
        //
        // bunch up all edits across all column families into a
        // single WALEdit.
        WALEdit walEdit = new WALEdit();
        addFamilyMapToWALEdit(familyMap, walEdit);
        seqNum = this.log.append(regionInfo,
                regionInfo.getTableDesc().getName(), walEdit, now);
      }

      // Now make changes to the memstore.
      long addedSize = applyFamilyMapToMemstore(familyMap, seqNum);
      flush = isFlushSize(this.incMemoryUsage(addedSize));
    } finally {
      this.updatesLock.readLock().unlock();
    }

    // do after lock
    long after = EnvironmentEdgeManager.currentTimeMillis();
    String signature = SchemaMetrics.generateSchemaMetricsPrefix(
        this.getTableDesc().getNameAsString(), familyMap.keySet());
    HRegion.incrTimeVaryingMetric(signature + "delete_", after - now);

    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }
    HRegion.writeOps.incrementAndGet();
  }


  /**
   * @param put
   * @throws IOException
   */
  public void put(Put put) throws IOException {
    this.put(put, null, put.getWriteToWAL());
  }

  /**
   * @param put
   * @param writeToWAL
   * @throws IOException
   */
  public void put(Put put, boolean writeToWAL) throws IOException {
    this.put(put, null, writeToWAL);
  }

  /**
   * @param put
   * @param lockid
   * @throws IOException
   */
  public void put(Put put, Integer lockid) throws IOException {
    this.put(put, lockid, put.getWriteToWAL());
  }



  /**
   * @param put
   * @param lockid
   * @param writeToWAL
   * @throws IOException
   */
  public void put(Put put, Integer lockid, boolean writeToWAL)
  throws IOException {
    checkReadOnly();

    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();
    splitsAndClosesLock.readLock().lock();

    try {
      // We obtain a per-row lock, so other clients will block while one client
      // performs an update. The read lock is released by the client calling
      // #commit or #abort or if the HRegionServer lease on the lock expires.
      // See HRegionServer#RegionListener for how the expire on HRegionServer
      // invokes a HRegion#abort.
      byte [] row = put.getRow();
      // If we did not pass an existing row lock, obtain a new one
      Integer lid = getLock(lockid, row, true);

      try {
        // All edits for the given row (across all column families) must happen atomically.
        put(put.getFamilyMap(), writeToWAL);
      } finally {
        if (lockid == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
      HRegion.writeOps.incrementAndGet();
    }
  }

  /**
   * Struct-like class that tracks the progress of a batch operation,
   * accumulating status codes and tracking the index at which processing
   * is proceeding.
   */
  private static class BatchOperationInProgress<T> {
    T[] operations;
    OperationStatusCode[] retCodes;
    int nextIndexToProcess = 0;

    public BatchOperationInProgress(T[] operations) {
      this.operations = operations;
      retCodes = new OperationStatusCode[operations.length];
      Arrays.fill(retCodes, OperationStatusCode.NOT_RUN);
    }

    public boolean isDone() {
      return nextIndexToProcess == operations.length;
    }
  }

  /**
   * Perform a batch put with no pre-specified locks
   * @see HRegion#put(Pair[])
   */
  public OperationStatusCode[] put(Put[] puts) throws IOException {
    @SuppressWarnings("unchecked")
    Pair<Mutation, Integer> putsAndLocks[] = new Pair[puts.length];

    for (int i = 0; i < puts.length; i++) {
      putsAndLocks[i] = new Pair<Mutation, Integer>(puts[i], null);
    }
    return batchMutateWithLocks(putsAndLocks, "multiput_");
  }

  /**
   * Perform a batch of puts.
   * @param putsAndLocks the list of puts paired with their requested lock IDs.
   * @param methodName "multiput_/multidelete_" to update metrics correctly.
   * @throws IOException
   */
  public OperationStatusCode[] batchMutateWithLocks(Pair<Mutation, Integer>[] putsAndLocks,
      String methodName) throws IOException {
    BatchOperationInProgress<Pair<Mutation, Integer>> batchOp =
      new BatchOperationInProgress<Pair<Mutation,Integer>>(putsAndLocks);

    while (!batchOp.isDone()) {
      checkReadOnly();
      checkResources();

      long newSize;
      splitsAndClosesLock.readLock().lock();
      try {
        long addedSize = doMiniBatchOp(batchOp, methodName);
        newSize = this.incMemoryUsage(addedSize);
      } finally {
        splitsAndClosesLock.readLock().unlock();
      }
      if (isFlushSize(newSize)) {
        requestFlush();
      }
    }
    HRegion.writeOps.incrementAndGet();
    return batchOp.retCodes;
  }

  private long doMiniBatchOp(BatchOperationInProgress<Pair<Mutation, Integer>> batchOp,
      String methodNameForMetricsUpdate) throws IOException {
    String signature = null;
    // variable to note if all Put items are for the same CF -- metrics related
    boolean isSignatureClear = true;

    long now = EnvironmentEdgeManager.currentTimeMillis();

    byte[] byteNow = Bytes.toBytes(now);
    boolean locked = false;

    /** Keep track of the locks we hold so we can release them in finally clause */
    List<Integer> acquiredLocks = Lists.newArrayListWithCapacity(batchOp.operations.length);
    // We try to set up a batch in the range [firstIndex,lastIndexExclusive)
    int firstIndex = batchOp.nextIndexToProcess;
    int lastIndexExclusive = firstIndex;
    boolean success = false;
    try {
      // ------------------------------------
      // STEP 1. Try to acquire as many locks as we can, and ensure
      // we acquire at least one.
      // ----------------------------------
      int numReadyToWrite = 0;
      byte[] previousRow = null;
      Integer previousLockID = null;
      Integer currentLockID = null;

      while (lastIndexExclusive < batchOp.operations.length) {
        Pair<Mutation, Integer> nextPair = batchOp.operations[lastIndexExclusive];
        Mutation op = nextPair.getFirst();
        Integer providedLockId = nextPair.getSecond();

        // Check the families in the put. If bad, skip this one.
        if (op instanceof Put) {
          checkFamilies(op.getFamilyMap().keySet());
          checkTimestamps(op, now);
        }

        if (previousRow == null || !Bytes.equals(previousRow, op.getRow()) ||
            (providedLockId != null && !previousLockID.equals(providedLockId))) {
          // If we haven't got any rows in our batch, we should block to
          // get the next one.
          boolean shouldBlock = numReadyToWrite == 0;
          currentLockID = getLock(providedLockId, op.getRow(), shouldBlock);
          if (currentLockID == null) {
            // We failed to grab another lock
            assert !shouldBlock : "Should never fail to get lock when blocking";
            break; // stop acquiring more rows for this batch
          }

          if (providedLockId == null) {
            acquiredLocks.add(currentLockID);
          }

          // reset the previous row and lockID with the current one
          previousRow = op.getRow();
          previousLockID = currentLockID;
        }
        lastIndexExclusive++;
        numReadyToWrite++;

        // if first time around, designate expected signature for metric
        // else, if all have been consistent so far, check if it still holds
        // all else, designate failure signature and mark as unclear
        if (null == signature) {
          signature = SchemaMetrics.generateSchemaMetricsPrefix(
              this.getTableDesc().getNameAsString(), op.getFamilyMap()
              .keySet());
        } else {
          if (isSignatureClear) {
            if (!signature.equals(SchemaMetrics.generateSchemaMetricsPrefix(
                this.getTableDesc().getNameAsString(),
                op.getFamilyMap().keySet()))) {
              isSignatureClear = false;
              signature = SchemaMetrics.CF_UNKNOWN_PREFIX;
            }
          }
        }
      }
      // We've now grabbed as many puts off the list as we can
      assert numReadyToWrite > 0;

      this.updatesLock.readLock().lock();
      locked = true;

      // ------------------------------------
      // STEP 2. Update any LATEST_TIMESTAMP timestamps
      // ----------------------------------
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        Mutation op = batchOp.operations[i].getFirst();

        if (op instanceof Put) {
          updateKVTimestamps(
              op.getFamilyMap().values(),
              byteNow);
        }
        else if (op instanceof Delete) {
          prepareDeleteFamilyMap((Delete)op);
          prepareDeleteTimestamps(op.getFamilyMap(), byteNow);
        }
      }

      long seqNum = -1;
      // ------------------------------------
      // STEP 3. Write to WAL
      // ----------------------------------
      if (!this.disableWAL) {
        WALEdit walEdit = new WALEdit();
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          // Skip puts that were determined to be invalid during preprocessing
          if (batchOp.retCodes[i] != OperationStatusCode.NOT_RUN) continue;

          Mutation op = batchOp.operations[i].getFirst();
          if (!op.getWriteToWAL()) continue;
          addFamilyMapToWALEdit(op.getFamilyMap(), walEdit);
        }

        // Append the edit to WAL
        seqNum = this.log.append(regionInfo,
                regionInfo.getTableDesc().getName(),
                walEdit, now);
      }

      // ------------------------------------
      // STEP 4. Write back to memstore
      // ----------------------------------

      long addedSize = 0;
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        if (batchOp.retCodes[i] != OperationStatusCode.NOT_RUN) continue;

        Mutation op = batchOp.operations[i].getFirst();
        addedSize += applyFamilyMapToMemstore(op.getFamilyMap(), seqNum);
        batchOp.retCodes[i] = OperationStatusCode.SUCCESS;
      }
      success = true;
      return addedSize;
    } finally {
      if (locked) {
        this.updatesLock.readLock().unlock();
      }

      releaseRowLocks(acquiredLocks);

      // do after lock
      long after = EnvironmentEdgeManager.currentTimeMillis();
      if (null == signature) {
        signature = SchemaMetrics.CF_BAD_FAMILY_PREFIX;
      }
      HRegion.incrTimeVaryingMetric(signature + methodNameForMetricsUpdate, after - now);

      if (!success) {
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          if (batchOp.retCodes[i] == OperationStatusCode.NOT_RUN) {
            batchOp.retCodes[i] = OperationStatusCode.FAILURE;
          }
        }
      }
      batchOp.nextIndexToProcess = lastIndexExclusive;
    }
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
   * @param expectedValue
   * @param lockId
   * @param writeToWAL
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier,
      byte [] expectedValue, Writable w, Integer lockId, boolean writeToWAL)
  throws IOException{
    checkReadOnly();
    //TODO, add check for value length or maybe even better move this to the
    //client if this becomes a global setting
    checkResources();
    boolean isPut = w instanceof Put;
    if (!isPut && !(w instanceof Delete))
      throw new IOException("Action must be Put or Delete");

    splitsAndClosesLock.readLock().lock();
    try {
      RowLock lock = ((Mutation) w).getRowLock();
      Get get = new Get(row, lock);
      checkFamily(family);
      get.addColumn(family, qualifier);
      // Lock row
      Integer lid = getLock(lockId, get.getRow(), true);
      List<KeyValue> result = new ArrayList<KeyValue>();
      try {
        result = get(get);
        boolean matches = false;

        if (result.size() == 0) {
          if (expectedValue == null ) {
            matches = true;
          }
        } else if (result.size() == 1) {
          if (expectedValue != null) {
            // Compare the expected value with the actual value without copying anything
            KeyValue kv = result.get(0);
            matches = Bytes.equals(expectedValue, 0, expectedValue.length,
                kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
          }
        } else {
          throw new IOException("Internal error: more than one result returned for row/column");
        }
        //If matches put the new put or delete the new delete
        if (matches) {
          // All edits for the given row (across all column families) must happen atomically.
          if (isPut) {
            put(((Put)w).getFamilyMap(), writeToWAL);
          } else {
            Delete d = (Delete)w;
            prepareDeleteFamilyMap(d);
            delete(d.getFamilyMap(), writeToWAL);
          }
          return true;
        }
        return false;
      } finally {
        if(lockId == null) releaseRowLock(lid);
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }


  /**
   * Replaces any KV timestamps set to {@link HConstants#LATEST_TIMESTAMP}
   * with the provided current timestamp.
   */
  private void updateKVTimestamps(
      final Iterable<List<KeyValue>> keyLists, final byte[] now) {
    for (List<KeyValue> keys: keyLists) {
      if (keys == null) continue;
      for (KeyValue key : keys) {
        key.updateLatestStamp(now);
      }
    }
  }

  /*
   * Check if resources to support an update.
   *
   * Here we synchronize on HRegion, a broad scoped lock.  Its appropriate
   * given we're figuring in here whether this region is able to take on
   * writes.  This is only method with a synchronize (at time of writing),
   * this and the synchronize on 'this' inside in internalFlushCache to send
   * the notify.
   */
  private void checkResources() throws RegionOverloadedException {

    // If catalog region, do not impose resource constraints or block updates.
    if (this.getRegionInfo().isMetaRegion()) return;

    boolean blocked = false;
    while (this.memstoreSize.get() > this.blockingMemStoreSize) {
      requestFlush();
      String msg = "Region " + Bytes.toStringBinary(getRegionName()) +
          ": memstore size " +
          StringUtils.humanReadableInt(this.memstoreSize.get()) +
          " is >= than blocking " +
          StringUtils.humanReadableInt(this.blockingMemStoreSize) + " size";
      if (!this.waitOnMemstoreBlock) {
        throw new RegionOverloadedException("Cannot accept mutations: " + msg, threadWakeFrequency);
      } else if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName()
            + "'. "+ msg);
      }
      blocked = true;
      synchronized(this) {
        try {
          wait(threadWakeFrequency);
        } catch (InterruptedException e) {
          // continue;
        }
      }
    }
    if (blocked) {
      LOG.info("Unblocking updates for region " + this + " '"
          + Thread.currentThread().getName() + "'");
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
  private void put(final byte [] family, final List<KeyValue> edits)
  throws IOException {
    Map<byte[], List<KeyValue>> familyMap = new HashMap<byte[], List<KeyValue>>();
    familyMap.put(family, edits);
    this.put(familyMap, true);
  }

  /**
   * Add updates first to the hlog (if writeToWal) and then add values to memstore.
   * Warning: Assumption is caller has lock on passed in row.
   * @param familyMap map of family to edits for the given family.
   * @param writeToWAL if true, then we should write to the log
   * @throws IOException
   */
  private void put(final Map<byte [], List<KeyValue>> familyMap,
      boolean writeToWAL) throws IOException {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    byte[] byteNow = Bytes.toBytes(now);
    boolean flush = false;
    this.updatesLock.readLock().lock();
    try {
      checkFamilies(familyMap.keySet());
      checkTimestamps(familyMap, now);
      updateKVTimestamps(familyMap.values(), byteNow);
      // write/sync to WAL should happen before we touch memstore.
      //
      // If order is reversed, i.e. we write to memstore first, and
      // for some reason fail to write/sync to commit log, the memstore
      // will contain uncommitted transactions.
      long seqNum = -1;
      if (!this.disableWAL && writeToWAL) {
        WALEdit walEdit = new WALEdit();
        addFamilyMapToWALEdit(familyMap, walEdit);
        seqNum = this.log.append(regionInfo,
                regionInfo.getTableDesc().getName(), walEdit, now);
      }

      long addedSize = applyFamilyMapToMemstore(familyMap, seqNum);
      flush = isFlushSize(this.incMemoryUsage(addedSize));
    } finally {
      this.updatesLock.readLock().unlock();
    }

    // do after lock
    long after = EnvironmentEdgeManager.currentTimeMillis();
    String signature = SchemaMetrics.generateSchemaMetricsPrefix(
        this.getTableDesc().getNameAsString(), familyMap.keySet());
    HRegion.incrTimeVaryingMetric(signature + "put_", after - now);

    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }
  }

  /**
   * Atomically apply the given map of family->edits to the memstore.
   * This handles the consistency control on its own, but the caller
   * should already have locked updatesLock.readLock(). This also does
   * <b>not</b> check the families for validity.
   *
   * @param familyMap
   * @param seqNum The log sequence number associated with the edits.
   *
   * @return the additional memory usage of the memstore caused by the
   * new entries.
   */
  private long applyFamilyMapToMemstore(Map<byte[], List<KeyValue>> familyMap,
                                        long seqNum) {
    return applyFamilyMapToMemstore(familyMap, null, seqNum);
  }

  private long applyFamilyMapToMemstore(Map<byte[], List<KeyValue>> familyMap,
                 MultiVersionConsistencyControl.WriteEntry writeEntryToUse,
                 long seqNum) {
    // Increment the rowUpdatedCnt
    this.rowUpdateCnt.incrementAndGet();

    long start = EnvironmentEdgeManager.currentTimeMillis();

    MultiVersionConsistencyControl.WriteEntry w = null;
    long size = 0;
    try {
      w = (writeEntryToUse == null)? mvcc.beginMemstoreInsert(): writeEntryToUse;

      for (Map.Entry<byte[], List<KeyValue>> e : familyMap.entrySet()) {
        byte[] family = e.getKey();
        List<KeyValue> edits = e.getValue();

        Store store = getStore(family);
        for (KeyValue kv: edits) {
          kv.setMemstoreTS(w.getWriteNumber());
          size += store.add(kv, seqNum);
        }
      }
    } finally {
      if (writeEntryToUse == null) {
        long now = EnvironmentEdgeManager.currentTimeMillis();
        HRegion.memstoreInsertTime.addAndGet(now - start);
        start = now;
        mvcc.completeMemstoreInsert(w);
        now = EnvironmentEdgeManager.currentTimeMillis();
        HRegion.mvccWaitTime.addAndGet(now - start);
      }
      // else the calling function will take care of the mvcc completion and metrics.
    }

    return size;
  }

  /**
   * Check the collection of families for validity.
   * @throws NoSuchColumnFamilyException if a family does not exist.
   */
  private void checkFamilies(Collection<byte[]> families)
  throws NoSuchColumnFamilyException {
    for (byte[] family : families) {
      checkFamily(family);
    }
  }
  private void checkTimestamps(Mutation op, long now) throws DoNotRetryIOException {
    checkTimestamps(op.getFamilyMap(), now);
  }

  private void checkTimestamps(final Map<byte[], List<KeyValue>> familyMap,
      long now) throws DoNotRetryIOException {
    if (timestampTooNew == HConstants.LATEST_TIMESTAMP) {
      return;
    }
    long maxTs = now + timestampTooNew;
    for (List<KeyValue> kvs : familyMap.values()) {
      for (KeyValue kv : kvs) {
        // see if the user-side TS is out of range. latest = server-side
        if (!kv.isLatestTimestamp() && kv.getTimestamp() > maxTs) {
          throw new DoNotRetryIOException("Timestamp for KV out of range "
              + kv + " (too.new=" + timestampTooNew + ")");
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
  private void addFamilyMapToWALEdit(Map<byte[], List<KeyValue>> familyMap,
      WALEdit walEdit) {
    for (List<KeyValue> edits : familyMap.values()) {
      for (KeyValue kv : edits) {
        walEdit.add(kv);
      }
    }
  }

  private void requestFlush() {
    if (this.flushListener == null) {
      return;
    }
    synchronized (writestate) {
      if (this.writestate.isFlushRequested()) {
        return;
      }
      writestate.flushRequested = true;
    }
    // Make request outside of synchronize block; HBASE-818.
    // Request for a selective flush of the memstore, if possible.
    // This function is called by put(), delete(), etc.
    this.flushListener.request(this, this.perColumnFamilyFlushEnabled);
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
   * @param store
   * @return true if the size of the store is above the flush threshold for column families
   */
  private boolean shouldFlushStore(Store store) {
    return store.getSnapshotSize() > 0 ||
        store.getMemStoreSize() > this.columnfamilyMemstoreFlushSize;
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
   * @param maxSeqIdInStores Any edit found in split editlogs needs to be in
   * excess of the maxSeqId for the store to be applied, else its skipped.
   * @param reporter
   * @param status
   * @return the maximum sequence id of the edits replayed or -1 if nothing
   * added from editlogs.
   * @throws UnsupportedEncodingException
   * @throws IOException
   */
  protected long replayRecoveredEditsIfAny(final Path regiondir,
      final Map<byte[], Long> maxSeqIdInStores, final Progressable reporter, MonitoredTask status)
  throws UnsupportedEncodingException, IOException {

    long seqid = -1;

    NavigableSet<Path> files = HLog.getSplitEditFilesSorted(this.fs, regiondir);
    if (files == null || files.isEmpty()) return seqid;
    for (Path edits: files) {
      if (edits == null || !this.fs.exists(edits)) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      if (isZeroLengthThenDelete(this.fs, edits)) {
        continue;
      }
      try {
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter));
      } catch (IOException e) {
        boolean skipErrors = conf.getBoolean("hbase.skip.errors", false);
        if (skipErrors) {
          Path p = HLog.moveAsideBadEditsFile(fs, edits);
          LOG.error("hbase.skip.errors=true so continuing. Renamed " + edits +
            " as " + p, e);
        } else {
          throw e;
        }
      }
    }

    if (seqid > -1){
      // In case we added some edits to memory, we should flush
      // We do not want to write to the log again, hence passing the log
      // parameter as null.
      internalFlushcache(null, seqid, status);
    }

    // Now delete the content of recovered edits.  We're done w/ them.
    for (Path file: files) {
      if (!this.fs.delete(file, false)) {
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
   * @return the sequence id of the max edit log entry seen in the HLog or -1
   * if no edits have been replayed
   * @throws IOException
   */
  private long replayRecoveredEdits(final Path edits,
      final Map<byte[], Long> maxSeqIdInStores, final Progressable reporter)
    throws IOException {
    String msg = "Replaying edits from " + edits;
    LOG.info(msg);
    MonitoredTask status = TaskMonitor.get().createStatus(msg);
    status.setStatus("Opening logs");
    HLog.Reader reader = HLog.getReader(this.fs, edits, conf);
    try {
      long currentEditSeqId = -1;
      long firstSeqIdInLog = -1;
      long skippedEdits = 0;
      long editsCount = 0;
      HLog.Entry entry;
      Store store = null;

      try {
        // How many edits to apply before we send a progress report.
        int interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
        while ((entry = reader.next()) != null) {
          HLogKey key = entry.getKey();
          WALEdit val = entry.getEdit();
          if (firstSeqIdInLog == -1) {
            firstSeqIdInLog = key.getLogSeqNum();
          }
          currentEditSeqId = key.getLogSeqNum();
          boolean flush = false;
          for (KeyValue kv: val.getKeyValues()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (kv.matchingFamily(HLog.METAFAMILY) ||
                !Bytes.equals(key.getRegionName(), this.regionInfo.getRegionName())) {
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
            flush = restoreEdit(store, kv, key.getLogSeqNum());
            editsCount++;
          }
          // We do not want to write to the WAL again, and hence setting the WAL
          // parameter to null.
          if (flush) internalFlushcache(null, currentEditSeqId, status);

          // Every 'interval' edits, tell the reporter we're making progress.
          // Have seen 60k edits taking 3minutes to complete.
          if (reporter != null && (editsCount % interval) == 0) {
            status.setStatus("Replaying edits..." +
                " skipped=" + skippedEdits +
                " edits=" + editsCount);
            reporter.progress();
          }
        }
      } catch (EOFException eof) {
        Path p = HLog.moveAsideBadEditsFile(fs, edits);
        msg = "Encountered EOF. Most likely due to Master failure during " +
            "log spliting, so we have this data in another edit.  " +
            "Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.setStatus(msg);
      } catch (IOException ioe) {
          // If the IOE resulted from bad file format,
          // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = HLog.moveAsideBadEditsFile(fs, edits);
          msg = "File corruption encountered!  " +
              "Continuing, but renaming " + edits + " as " + p;
          LOG.warn(msg, ioe);
          status.setStatus(msg);
        } else {
          // other IO errors may be transient (bad network connection,
          // checksum exception on one datanode, etc).  throw & retry
          status.abort(StringUtils.stringifyException(ioe));
          throw ioe;
        }
      }
      msg = "Applied " + editsCount + ", skipped " + skippedEdits +
          ", firstSeqIdInLog=" + firstSeqIdInLog +
          ", maxSeqIdInLog=" + currentEditSeqId;
      status.markComplete(msg);
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg);
      }
      if(editsCount == 0){
        return -1;
      } else {
        return currentEditSeqId;
      }
    } finally {
      reader.close();
      status.cleanup();
    }
  }

  /**
   * Used by tests
   * @param s Store to add edit too.
   * @param kv KeyValue to add.
   * @param seqNum The sequence number for the edit.
   * @return True if we should flush.
   */
  protected boolean restoreEdit(final Store s, final KeyValue kv, long seqNum) {
    return isFlushSize(this.incMemoryUsage(s.add(kv, seqNum)));
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

  protected Store instantiateHStore(Path tableDir, HColumnDescriptor c)
  throws IOException {
    Store store = new Store(tableDir, this, c, this.fs, this.conf);
    // Register this store with the configuration manager.
    HRegionServer.configurationManager.registerObserver(store);
    return store;
  }

  /**
   * Return HStore instance.
   * Use with caution.  Exposed for use of fixup utilities.
   * @param column Name of column family hosted by this region.
   * @return Store that goes with the family on passed <code>column</code>.
   * TODO: Make this lookup faster.
   */
  public Store getStore(final byte [] column) {
    return this.stores.get(column);
  }

  public Map<byte[], Store> getStores() {
    return this.stores;
  }

  /**
   * Return list of storeFiles for the set of CFs.
   * Uses splitLock to prevent the race condition where a region closes
   * in between the for loop - closing the stores one by one, some stores
   * will return 0 files.
   * @return List of storeFiles.
   */
  public List<String> getStoreFileList(final byte [][] columns)
    throws IllegalArgumentException {
    List<String> storeFileNames = new ArrayList<String>();
    synchronized(splitLock) {
      for(byte[] column : columns) {
        Store store = this.stores.get(column);
        if (store == null) {
          throw new IllegalArgumentException("No column family : " +
              new String(column) + " available");
        }
        List<StoreFile> storeFiles = store.getStorefiles();
        for (StoreFile storeFile: storeFiles) {
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
  private void checkRow(final byte [] row) throws IOException {
    if(!rowIsInRange(regionInfo, row)) {
      throw new WrongRegionException("Requested row out of range for " +
          "HRegion " + this + ", startKey='" +
          Bytes.toStringBinary(regionInfo.getStartKey()) + "', getEndKey()='" +
          Bytes.toStringBinary(regionInfo.getEndKey()) + "', row='" +
          Bytes.toStringBinary(row) + "'");
    }
  }

  /**
   * Obtain a lock on the given row.  Blocks until success.
   *
   * I know it's strange to have two mappings:
   * <pre>
   *   ROWS  ==> LOCKS
   * </pre>
   * as well as
   * <pre>
   *   LOCKS ==> ROWS
   * </pre>
   *
   * But it acts as a guard on the client; a miswritten client just can't
   * submit the name of a row and start writing to it; it must know the correct
   * lockid, which matches the lock list in memory.
   *
   * <p>It would be more memory-efficient to assume a correctly-written client,
   * which maybe we'll do in the future.
   *
   * @param row Name of row to lock.
   * @throws IOException
   * @return The id of the held lock.
   */
  public Integer obtainRowLock(final byte [] row) throws IOException {
    return internalObtainRowLock(row, true);
  }

  /**
   * Tries to obtain a row lock on the given row, but does not block if the
   * row lock is not available. If the lock is not available, returns false.
   * Otherwise behaves the same as the above method.
   * @see HRegion#obtainRowLock(byte[])
   */
  public Integer tryObtainRowLock(final byte[] row) throws IOException {
    return internalObtainRowLock(row, false);
  }

  /**
   * Obtains or tries to obtain the given row lock.
   * @param waitForLock if true, will block until the lock is available.
   *        Otherwise, just tries to obtain the lock and returns
   *        null if unavailable.
   */
  private Integer internalObtainRowLock(final byte[] row, boolean waitForLock)
  throws IOException {
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new NotServingRegionException(this + " is closed");
      }
      synchronized (lockedRows) {
        while (lockedRows.contains(row)) {
          if (!waitForLock) {
            return null;
          }
          try {
            lockedRows.wait();
          } catch (InterruptedException ie) {
            // Empty
          }
        }
        // generate a new lockid. Attempt to insert the new [lockid, row].
        // if this lockid already exists in the map then revert and retry
        // We could have first done a lockIds.get, and if it does not exist only
        // then do a lockIds.put, but the hope is that the lockIds.put will
        // mostly return null the first time itself because there won't be
        // too many lockId collisions.
        byte [] prev = null;
        Integer lockId = null;
        do {
          lockId = new Integer(lockIdGenerator++);
          prev = lockIds.put(lockId, row);
          if (prev != null) {
            lockIds.put(lockId, prev);    // revert old value
            lockIdGenerator = rand.nextInt(); // generate new start point
          }
        } while (prev != null);

        lockedRows.add(row);
        lockedRows.notifyAll();
        return lockId;
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Used by unit tests.
   * @param lockid
   * @return Row that goes with <code>lockid</code>
   */
  byte [] getRowFromLock(final Integer lockid) {
    synchronized (lockedRows) {
      return lockIds.get(lockid);
    }
  }

  /**
   * Release the row lock!
   * @param lockid  The lock ID to release.
   */
  public void releaseRowLock(final Integer lockid) {
    synchronized (lockedRows) {
      byte[] row = lockIds.remove(lockid);
      lockedRows.remove(row);
      lockedRows.notifyAll();
    }
  }

  /**
   * Release the row locks!
   * @param lockidList The list of the lock ID to release.
   */
  void releaseRowLocks(final List<Integer> lockidList) {
    synchronized (lockedRows) {
      for (Integer lockid : lockidList) {
        byte[] row = lockIds.remove(lockid);
        lockedRows.remove(row);
      }
      lockedRows.notifyAll();
    }
  }

  /**
   * See if row is currently locked.
   * @param lockid
   * @return boolean
   */
  boolean isRowLocked(final Integer lockid) {
    synchronized (lockedRows) {
      if (lockIds.get(lockid) != null) {
        return true;
      }
      return false;
    }
  }

  /**
   * Returns existing row lock if found, otherwise
   * obtains a new row lock and returns it.
   * @param lockid requested by the user, or null if the user didn't already hold lock
   * @param row the row to lock
   * @param waitForLock if true, will block until the lock is available, otherwise will
   * simply return null if it could not acquire the lock.
   * @return lockid or null if waitForLock is false and the lock was unavailable.
   */
  private Integer getLock(Integer lockid, byte [] row, boolean waitForLock)
  throws IOException {
    Integer lid = null;
    if (lockid == null) {
      long start = EnvironmentEdgeManager.currentTimeMillis();
      lid = internalObtainRowLock(row, waitForLock);
      long end = EnvironmentEdgeManager.currentTimeMillis();
      HRegion.rowLockTime.addAndGet(end - start);
    } else {
      if (!isRowLocked(lockid)) {
        throw new IOException("Invalid row lock");
      }
      lid = lockid;
    }
    return lid;
  }

  private void waitOnRowLocks() {
    synchronized (lockedRows) {
      while (!this.lockedRows.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting on " + this.lockedRows.size() + " row locks");
        }
        try {
          this.lockedRows.wait();
        } catch (InterruptedException e) {
          // Catch. Let while test determine loop-end.
        }
      }
    }
  }

  public void bulkLoadHFile(String hfilePath, byte[] familyName, boolean assignSeqId)
  throws IOException {
    long seqId = this.log.obtainSeqNum();

    splitsAndClosesLock.readLock().lock();
    try {
      Store store = getStore(familyName);
      if (store == null) {
        throw new DoNotRetryIOException(
            "No such column family " + Bytes.toStringBinary(familyName));
      }
      store.bulkLoadHFile(hfilePath, assignSeqId ? seqId : -1);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  public void bulkLoadHFile(String hfilePath, byte[] familyName)
  throws IOException {
    bulkLoadHFile(hfilePath, familyName, false);
  }


  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HRegion)) {
      return false;
    }
    return Bytes.equals(this.getRegionName(), ((HRegion) o).getRegionName());
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.regionInfo.getRegionName());
  }

  @Override
  public String toString() {
    return this.regionInfo.getRegionNameAsString();
  }

  /** @return Path of region base directory */
  public Path getTableDir() {
    return this.tableDir;
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
   * @param flushListener an object that implements CacheFlushListener or null
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   * @return the new instance
   */
  public static HRegion newHRegion(Path tableDir, HLog log, FileSystem fs, Configuration conf,
                                   HRegionInfo regionInfo, FlushRequester flushListener) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
          (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, HRegion.class);

      Constructor<? extends HRegion> c =
          regionClass.getConstructor(Path.class, HLog.class, FileSystem.class,
              Configuration.class, HRegionInfo.class, FlushRequester.class);

      return c.newInstance(tableDir, log, fs, conf, regionInfo, flushListener);
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
   * access.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
    final Configuration conf)
  throws IOException {
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    HRegion region = HRegion.newHRegion(tableDir,
      new HLog(fs, new Path(regionDir, HConstants.HREGION_LOGDIR_NAME),
          new Path(regionDir, HConstants.HREGION_OLDLOGDIR_NAME), conf, null),
      fs, conf, info, null);
    region.initialize();
    return region;
  }

  /**
   * Convenience method to open a HRegion outside of an HRegionServer context.
   * @param info Info for region to be opened.
   * @param rootDir Root directory for HBase instance
   * @param log HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf
   * @return new HRegion
   *
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info, final Path rootDir,
    final HLog log, final Configuration conf)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region: " + info);
    }
    if (info == null) {
      throw new NullPointerException("Passed region info is null");
    }
    HRegion r = HRegion.newHRegion(
        HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName()),
        log, FileSystem.get(conf), conf, info, null);
    long seqid = r.initialize();
    if (log != null) {
      log.setSequenceNumber(seqid);
    }
    return r;
  }

  /**
   * Inserts a new region's meta information into the passed
   * <code>meta</code> region. Used by the HMaster bootstrap code adding
   * new table to ROOT table.
   *
   * @param meta META HRegion to be updated
   * @param r HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  public static void addRegionToMETA(HRegion meta, HRegion r)
  throws IOException {
    meta.checkResources();
    // The row key is the region name
    byte[] row = r.getRegionName();
    Integer lid = meta.obtainRowLock(row);
    try {
      final List<KeyValue> edits = new ArrayList<KeyValue>(1);
      edits.add(new KeyValue(row, HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER,
          EnvironmentEdgeManager.currentTimeMillis(),
          Writables.getBytes(r.getRegionInfo())));
      meta.put(HConstants.CATALOG_FAMILY, edits);
    } finally {
      meta.releaseRowLock(lid);
    }
  }

  /**
   * Delete a region's meta information from the passed
   * <code>meta</code> region.  Deletes the row.
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param regionName HRegion to remove from <code>meta</code>
   *
   * @throws IOException
   */
  public static void removeRegionFromMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final byte [] regionName)
  throws IOException {
    Delete delete = new Delete(regionName);
    srvr.delete(metaRegionName, delete);
  }

  /**
   * Utility method used by HMaster marking regions offlined.
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param info HRegion to update in <code>meta</code>
   *
   * @throws IOException
   */
  public static void offlineRegionInMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final HRegionInfo info)
  throws IOException {
    // Puts and Deletes used to be "atomic" here.  We can use row locks if
    // we need to keep that property, or we can expand Puts and Deletes to
    // allow them to be committed at once.
    byte [] row = info.getRegionName();
    Put put = new Put(row);
    info.setOffline(true);
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(info));
    srvr.put(metaRegionName, put);
    cleanRegionInMETA(srvr, metaRegionName, info);
  }

  /**
   * Clean COL_SERVER and COL_STARTCODE for passed <code>info</code> in
   * <code>.META.</code>
   * @param srvr
   * @param metaRegionName
   * @param info
   * @throws IOException
   */
  public static void cleanRegionInMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final HRegionInfo info)
  throws IOException {
    Delete del = new Delete(info.getRegionName());
    del.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    del.deleteColumns(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER);
    srvr.delete(metaRegionName, del);
  }

  /**
   * Deletes all the files for a HRegion
   *
   * @param fs the file system object
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   */
  public static void deleteRegion(FileSystem fs, Path rootdir, HRegionInfo info)
  throws IOException {
    deleteRegion(fs, HRegion.getRegionDir(rootdir, info));
  }

  private static void deleteRegion(FileSystem fs, Path regiondir)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regiondir.toString());
    }
    if (!fs.delete(regiondir, true)) {
      LOG.warn("Failed delete of " + regiondir);
    }
  }

  /**
   * Computes the Path of the HRegion
   *
   * @param tabledir qualified path for table
   * @param name ENCODED region name
   * @return Path of HRegion directory
   */
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
  public static Path getRegionDir(final Path rootdir, final HRegionInfo info) {
    return new Path(
      HTableDescriptor.getTableDir(rootdir, info.getTableDesc().getName()),
                                   info.getEncodedName());
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
   * Make the directories for a specific column family
   *
   * @param fs the file system
   * @param tabledir base directory where region will live (usually the table dir)
   * @param hri
   * @param colFamily the column family
   * @throws IOException
   */
  public static void makeColumnFamilyDirs(FileSystem fs, Path tabledir,
    final HRegionInfo hri, byte [] colFamily)
  throws IOException {
    Path dir = Store.getStoreHomedir(tabledir, hri.getEncodedName(), colFamily);
    if (!fs.mkdirs(dir)) {
      LOG.warn("Failed to create " + dir);
    }
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
  public static HRegion merge(HRegion a, HRegion b) throws IOException {
    if (!a.getRegionInfo().getTableDesc().getNameAsString().equals(
        b.getRegionInfo().getTableDesc().getNameAsString())) {
      throw new IOException("Regions do not belong to the same table");
    }

    FileSystem fs = a.getFilesystem();

    // Make sure each region's cache is empty

    a.flushcache();
    b.flushcache();

    // Compact each region so we only have one store file per family

    a.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + a);
      listPaths(fs, a.getRegionDir());
    }
    b.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + b);
      listPaths(fs, b.getRegionDir());
    }

    Configuration conf = a.baseConf;
    HTableDescriptor tabledesc = a.getTableDesc();
    HLog log = a.getLog();
    Path tableDir = a.getTableDir();
    // Presume both are of same region type -- i.e. both user or catalog
    // table regions.  This way can use comparator.
    final byte[] startKey =
      (a.comparator.matchingRows(a.getStartKey(), 0, a.getStartKey().length,
           HConstants.EMPTY_BYTE_ARRAY, 0, HConstants.EMPTY_BYTE_ARRAY.length)
       || b.comparator.matchingRows(b.getStartKey(), 0,
              b.getStartKey().length, HConstants.EMPTY_BYTE_ARRAY, 0,
              HConstants.EMPTY_BYTE_ARRAY.length))
      ? HConstants.EMPTY_BYTE_ARRAY
      : (a.comparator.compareRows(a.getStartKey(), 0, a.getStartKey().length,
             b.getStartKey(), 0, b.getStartKey().length) <= 0
         ? a.getStartKey()
         : b.getStartKey());
    final byte[] endKey =
      (a.comparator.matchingRows(a.getEndKey(), 0, a.getEndKey().length,
           HConstants.EMPTY_BYTE_ARRAY, 0, HConstants.EMPTY_BYTE_ARRAY.length)
       || a.comparator.matchingRows(b.getEndKey(), 0, b.getEndKey().length,
              HConstants.EMPTY_BYTE_ARRAY, 0,
              HConstants.EMPTY_BYTE_ARRAY.length))
      ? HConstants.EMPTY_BYTE_ARRAY
      : (a.comparator.compareRows(a.getEndKey(), 0, a.getEndKey().length,
             b.getEndKey(), 0, b.getEndKey().length) <= 0
         ? b.getEndKey()
         : a.getEndKey());

    HRegionInfo newRegionInfo = new HRegionInfo(tabledesc, startKey, endKey);
    LOG.info("Creating new region " + newRegionInfo.toString());
    String encodedName = newRegionInfo.getEncodedName();
    Path newRegionDir = HRegion.getRegionDir(a.getTableDir(), encodedName);
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
          newRegionDir);
    }
    fs.mkdirs(newRegionDir);

    LOG.info("starting merge of regions: " + a + " and " + b +
      " into new region " + newRegionInfo.toString() +
        " with start key <" + Bytes.toString(startKey) + "> and end key <" +
        Bytes.toString(endKey) + ">");

    // Move HStoreFiles under new region directory
    Map<byte [], List<StoreFile>> byFamily =
      new TreeMap<byte [], List<StoreFile>>(Bytes.BYTES_COMPARATOR);
    byFamily = filesByFamily(byFamily, a.close());
    byFamily = filesByFamily(byFamily, b.close());
    for (Map.Entry<byte [], List<StoreFile>> es : byFamily.entrySet()) {
      byte [] colFamily = es.getKey();
      makeColumnFamilyDirs(fs, tableDir, newRegionInfo, colFamily);
      // Because we compacted the source regions we should have no more than two
      // HStoreFiles per family and there will be no reference store
      List<StoreFile> srcFiles = es.getValue();
      if (srcFiles.size() == 2) {
        long seqA = srcFiles.get(0).getMaxSequenceId();
        long seqB = srcFiles.get(1).getMaxSequenceId();
        if (seqA == seqB) {
          // Can't have same sequenceid since on open of a store, this is what
          // distingushes the files (see the map of stores how its keyed by
          // sequenceid).
          throw new IOException("Files have same sequenceid: " + seqA);
        }
      }
      for (StoreFile hsf: srcFiles) {
        StoreFile.rename(fs, hsf.getPath(),
          StoreFile.getUniqueFile(fs, Store.getStoreHomedir(tableDir,
            newRegionInfo.getEncodedName(), colFamily)));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, newRegionDir);
    }
    HRegion dstRegion = HRegion.newHRegion(tableDir, log, fs, conf, newRegionInfo, null);
    dstRegion.initialize();
    dstRegion.compactStores();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, dstRegion.getRegionDir());
    }
    deleteRegion(fs, a.getRegionDir());
    deleteRegion(fs, b.getRegionDir());

    LOG.info("merge completed. New region is " + dstRegion);

    return dstRegion;
  }

  /*
   * Fills a map with a vector of store files keyed by column family.
   * @param byFamily Map to fill.
   * @param storeFiles Store files to process.
   * @param family
   * @return Returns <code>byFamily</code>
   */
  private static Map<byte [], List<StoreFile>> filesByFamily(
      Map<byte [], List<StoreFile>> byFamily, List<StoreFile> storeFiles) {
    for (StoreFile src: storeFiles) {
      byte [] family = src.getFamily();
      List<StoreFile> v = byFamily.get(family);
      if (v == null) {
        v = new ArrayList<StoreFile>();
        byFamily.put(family, v);
      }
      v.add(src);
    }
    return byFamily;
  }

  /**
   * @return True if needs a major compaction.
   * @throws IOException
   */
  boolean isMajorCompaction() throws IOException {
    for (Store store: this.stores.values()) {
      if (store.isMajorCompaction()) {
        return true;
      }
    }
    return false;
  }

  /*
   * List the files under the specified directory
   *
   * @param fs
   * @param dir
   * @throws IOException
   */
  private static void listPaths(FileSystem fs, Path dir) throws IOException {
    if (LOG.isDebugEnabled()) {
      FileStatus[] stats = fs.listStatus(dir);
      if (stats == null || stats.length == 0) {
        return;
      }
      for (FileStatus stat : stats) {
        String path = stat.getPath().toString();
        if (stat.isDir()) {
          LOG.debug("d " + path);
          listPaths(fs, stat.getPath());
        } else {
          LOG.debug("f " + path + " size=" + stat.getLen());
        }
      }
    }
  }


  //
  // HBASE-880
  //
  /**
   * @param get get object
   * @param lockid existing lock id, or null for no previous lock
   * @return result
   * @throws IOException read exceptions
   */
  public Result get(final Get get, final Integer lockid) throws IOException {
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte [] family: get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family: regionInfo.getTableDesc().getFamiliesKeys()) {
        get.addFamily(family);
      }
    }
    List<KeyValue> result = get(get);

    return new Result(result);
  }

  /*
   * Do a get based on the get parameter.
   */
  private List<KeyValue> get(final Get get) throws IOException {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    Scan scan = new Scan(get);

    List<KeyValue> results = new ArrayList<KeyValue>();

    InternalScanner scanner = null;
    try {
      scanner = getScanner(scan);
      scanner.next(results, HRegion.METRIC_GETSIZE);
    } finally {
      if (scanner != null)
        scanner.close();
    }

    // do after lock
    long after = EnvironmentEdgeManager.currentTimeMillis();
    String signature = SchemaMetrics.generateSchemaMetricsPrefix(
        this.getTableDesc().getNameAsString(), get.familySet());
    HRegion.incrTimeVaryingMetric(signature + "get_", after - now);

    return results;
  }


  public void mutateRow(RowMutations rm) throws IOException {
    boolean flush = false;
    Integer lid = null;
    splitsAndClosesLock.readLock().lock();
    try {
      // 1. run all pre-hooks before the atomic operation
      // NOT required for 0.89

      // one WALEdit is used for all edits.
      WALEdit walEdit = new WALEdit();

      // 2. acquire the row lock
      lid = getLock(null, rm.getRow(), true);

      // 3. acquire the region lock
      this.updatesLock.readLock().lock();

      // 4. Get a mvcc write number
      MultiVersionConsistencyControl.WriteEntry w = mvcc.beginMemstoreInsert();

      long now = EnvironmentEdgeManager.currentTimeMillis();
      byte[] byteNow = Bytes.toBytes(now);
      try {
        // 5. Check mutations and apply edits to a single WALEdit
        for (Mutation m : rm.getMutations()) {
          if (m instanceof Put) {
            Map<byte[], List<KeyValue>> familyMap = m.getFamilyMap();
            checkFamilies(familyMap.keySet());
            checkTimestamps(familyMap, now);
            updateKVTimestamps(familyMap.values(), byteNow);
          } else if (m instanceof Delete) {
            Delete d = (Delete) m;
            prepareDeleteFamilyMap(d);
            Map<byte[], List<KeyValue>> familyMap = m.getFamilyMap();
            prepareDeleteTimestamps(familyMap, byteNow);
          } else {
            throw new DoNotRetryIOException(
                "Action must be Put or Delete. But was: "
                    + m.getClass().getName());
          }
          if (m.getWriteToWAL()) {
            addFamilyMapToWALEdit(m.getFamilyMap(), walEdit);
          }
        }

        // 6. append/sync all edits at once
        // TODO: Do batching as in doMiniBatchPut
        long seqNum = this.log.append(regionInfo, this.getTableDesc().getName(),
                walEdit, now);

        // 7. apply to memstore
        long addedSize = 0;
        for (Mutation m : rm.getMutations()) {
          addedSize += applyFamilyMapToMemstore(m.getFamilyMap(), w, seqNum);
        }
        flush = isFlushSize(this.incMemoryUsage(addedSize));
      } finally {
        // 8. roll mvcc forward
        long start = now;
        now = EnvironmentEdgeManager.currentTimeMillis();
        HRegion.memstoreInsertTime.addAndGet(now - start);

        mvcc.completeMemstoreInsert(w);
        now = EnvironmentEdgeManager.currentTimeMillis();
        HRegion.mvccWaitTime.addAndGet(now - start);

        // 9. release region lock
        this.updatesLock.readLock().unlock();
      }
      // 10. run all coprocessor post hooks, after region lock is released
      // NOT required in 0.89. coprocessors are not supported.

    } finally {
      if (lid != null) {
        // 11. release the row lock
        releaseRowLock(lid);
      }
      if (flush) {
        // 12. Flush cache if needed. Do it outside update lock.
        requestFlush();
      }
      splitsAndClosesLock.readLock().unlock();
    }
  }
  /**
   *
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @param writeToWAL
   * @return The new value.
   * @throws IOException
   */
  public long incrementColumnValue(byte [] row, byte [] family,
      byte [] qualifier, long amount, boolean writeToWAL)
  throws IOException {
    // to be used for metrics
    long before = EnvironmentEdgeManager.currentTimeMillis();
    this.rowUpdateCnt.incrementAndGet();

    checkRow(row);
    boolean flush = false;
    // Lock row
    Integer lid = obtainRowLock(row);
    this.updatesLock.readLock().lock();
    long result = amount;
    try {
      Store store = stores.get(family);

      // Get the old value:
      Get get = new Get(row);
      get.addColumn(family, qualifier);

      List<KeyValue> results = get(get);

      if (!results.isEmpty()) {
        KeyValue kv = results.get(0);
        byte [] buffer = kv.getBuffer();
        int valueOffset = kv.getValueOffset();
        result += Bytes.toLong(buffer, valueOffset, Bytes.SIZEOF_LONG);
      }

      // bulid the KeyValue now:
      KeyValue newKv = new KeyValue(row, family,
          qualifier, EnvironmentEdgeManager.currentTimeMillis(),
          Bytes.toBytes(result));

      long seqNum = -1;
      // now log it:
      if (writeToWAL) {
        long now = EnvironmentEdgeManager.currentTimeMillis();
        WALEdit walEdit = new WALEdit();
        walEdit.add(newKv);
        seqNum = this.log.append(regionInfo, regionInfo.getTableDesc().getName(),
          walEdit, now);
      }

      // Now request the ICV to the store, this will set the timestamp
      // appropriately depending on if there is a value in memcache or not.
      // returns the
      long size = store.updateColumnValue(row, family, qualifier, result,
                                          seqNum);

      size = this.incMemoryUsage(size);
      flush = isFlushSize(size);
    } finally {
      this.updatesLock.readLock().unlock();
      releaseRowLock(lid);
      HRegion.writeOps.incrementAndGet();
    }

    // do after lock
    long after = EnvironmentEdgeManager.currentTimeMillis();
    String signature = SchemaMetrics.generateSchemaMetricsPrefix(
        this.getTableDesc().getNameAsString(), Bytes.toString(family));
    HRegion.incrTimeVaryingMetric(signature + "increment_", after - before);

    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }

    return result;
  }


  //
  // New HBASE-880 Helpers
  //

  private void checkFamily(final byte [] family)
  throws NoSuchColumnFamilyException {
    if(!regionInfo.getTableDesc().hasFamily(family)) {
      throw new NoSuchColumnFamilyException("Column family " +
          Bytes.toString(family) + " does not exist in region " + this
            + " in table " + regionInfo.getTableDesc());
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      (2 * Bytes.SIZEOF_BOOLEAN) +
      (6 * Bytes.SIZEOF_LONG) + 2 * ClassSize.ARRAY +
      (29 * ClassSize.REFERENCE) + ClassSize.OBJECT + Bytes.SIZEOF_INT);

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.OBJECT + (2 * ClassSize.ATOMIC_BOOLEAN) +
      ClassSize.ATOMIC_LONG + ClassSize.ATOMIC_INTEGER +

      // Using TreeMap for TreeSet
      ClassSize.TREEMAP +

      // Using TreeMap for HashMap
      ClassSize.TREEMAP +

      ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY +
      ClassSize.CONCURRENT_HASHMAP +
      ClassSize.align(ClassSize.OBJECT +
        (4 * Bytes.SIZEOF_BOOLEAN)) +
        (3 * ClassSize.REENTRANT_LOCK));

  @Override
  public long heapSize() {
    long heapSize = DEEP_OVERHEAD;
    for(Store store : this.stores.values()) {
      heapSize += store.heapSize();
    }
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
    String rootStr = Bytes.toString(HConstants.ROOT_TABLE_NAME);
    String metaStr = Bytes.toString(HConstants.META_TABLE_NAME);
    // Currently expects tables have one region only.
    if (p.getName().startsWith(rootStr)) {
      region = HRegion.newHRegion(p, log, fs, c, HRegionInfo.ROOT_REGIONINFO, null);
    } else if (p.getName().startsWith(metaStr)) {
      region = HRegion.newHRegion(p, log, fs, c, HRegionInfo.FIRST_META_REGIONINFO,
          null);
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
        InternalScanner scanner = region.getScanner(scan);
        try {
          List<KeyValue> kvs = new ArrayList<KeyValue>();
          boolean done = false;
          do {
            kvs.clear();
            done = scanner.next(kvs);
            if (kvs.size() > 0) LOG.info(kvs);
          } while (done);
        } finally {
          scanner.close();
        }
        // System.out.println(region.getClosestRowBefore(Bytes.toBytes("GeneratedCSVContent2,E3652782193BC8D66A0BA1629D0FAAAB,9993372036854775807")));
      }
    } finally {
      region.close();
    }
  }

  /**
   * For internal use in forcing splits ahead of file size limit.
   * @param b
   * @return previous value
   */
  void triggerSplit() {
    this.splitRequest = true;
  }

  boolean shouldSplit() {
    return this.splitRequest;
  }

  byte[] getSplitPoint() {
    return this.splitPoint;
  }

  void setSplitPoint(byte[] sp) {
    this.splitPoint = sp;
  }

  byte[] checkSplit() {
    if (this.splitPoint != null) {
      return this.splitPoint;
    }
    byte[] splitPoint = null;
    for (Store s : stores.values()) {
      splitPoint = s.checkSplit();
      if (splitPoint != null) {
        return splitPoint;
      }
    }
    return null;
  }

  /**
   * @return The priority that this region should have in the compaction queue
   */
  public int getCompactPriority() {
    int count = Integer.MAX_VALUE;
    for(Store store : stores.values()) {
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
    for(Store store : stores.values()) {
      if(store.needsCompaction()) {
        return true;
      }
    }
    return false;
  }

  public int getAndResetRowReadCnt() {
    int readCnt = this.rowReadCnt.get();
    this.rowReadCnt.addAndGet(-readCnt);
    return readCnt;
  }

  public int getAndResetRowUpdateCnt() {
    int updateCnt = this.rowUpdateCnt.get();
    this.rowUpdateCnt.addAndGet(-updateCnt);
    return updateCnt;
  }

  /**
   * A mocked list implementaion - discards all updates.
   */
  public static final List<KeyValue> MOCKED_LIST = new AbstractList<KeyValue>() {

    @Override
    public void add(int index, KeyValue element) {
      // do nothing
    }

    @Override
    public boolean addAll(int index, Collection<? extends KeyValue> c) {
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
   * Returns null is no data is available.
   * @return
   * @throws IOException
   */
  public HFileHistogram getHistogram() throws IOException {
    List<HFileHistogram> histograms = new ArrayList<HFileHistogram>();
    if (stores.size() == 0) return null;
    for (Store s : stores.values()) {
      HFileHistogram hist = s.getHistogram();
      if (hist != null) histograms.add(hist);
    }
    // If none of the stores produce a histogram returns null.
    if (histograms.size() == 0) return null;
    HFileHistogram h = histograms.get(0).compose(histograms);
    return h;
  }

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
    final Path logdir = new Path(c.get("hbase.tmp.dir"),
        "hlog" + tableDir.getName()
        + EnvironmentEdgeManager.currentTimeMillis());
    final Path oldLogDir = new Path(c.get("hbase.tmp.dir"),
        HConstants.HREGION_OLDLOGDIR_NAME);
    final HLog log = new HLog(fs, logdir, oldLogDir, c, null);
    try {
      processTable(fs, tableDir, log, c, majorCompact);
     } finally {
       log.close();
       // TODO: is this still right?
       BlockCache bc = new CacheConfig(c).getBlockCache();
       L2Cache l2c = new CacheConfig(c).getL2Cache();
       try {
         if (bc != null) bc.shutdown();
       } finally {
         if (l2c != null) l2c.shutdown();
       }
     }
  }

  void updateConfiguration() {
    for (Store s : stores.values()) {
      s.updateConfiguration();
    }
  }

}
