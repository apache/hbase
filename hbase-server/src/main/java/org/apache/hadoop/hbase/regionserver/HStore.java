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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.FailedArchiveException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.ReadOnlyConfiguration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.InvalidHFileException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.IterableUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;

/**
 * A Store holds a column family in a Region.  Its a memstore and a set of zero
 * or more StoreFiles, which stretch backwards over time.
 *
 * <p>There's no reason to consider append-logging at this level; all logging
 * and locking is handled at the HRegion level.  Store just provides
 * services to manage sets of StoreFiles.  One of the most important of those
 * services is compaction services where files are aggregated once they pass
 * a configurable threshold.
 *
 * <p>Locking and transactions are handled at a higher level.  This API should
 * not be called directly but by an HRegion manager.
 */
@InterfaceAudience.Private
public class HStore implements Store, HeapSize, StoreConfigInformation,
    PropagatingConfigurationObserver {
  public static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";
  public static final String COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY =
      "hbase.server.compactchecker.interval.multiplier";
  public static final String BLOCKING_STOREFILES_KEY = "hbase.hstore.blockingStoreFiles";
  public static final String BLOCK_STORAGE_POLICY_KEY = "hbase.hstore.block.storage.policy";
  // keep in accordance with HDFS default storage policy
  public static final String DEFAULT_BLOCK_STORAGE_POLICY = "HOT";
  public static final int DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER = 1000;
  public static final int DEFAULT_BLOCKING_STOREFILE_COUNT = 16;

  // HBASE-24428 : Update compaction priority for recently split daughter regions
  // so as to prioritize their compaction.
  // Any compaction candidate with higher priority than compaction of newly split daugher regions
  // should have priority value < (Integer.MIN_VALUE + 1000)
  private static final int SPLIT_REGION_COMPACTION_PRIORITY = Integer.MIN_VALUE + 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HStore.class);

  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final HRegion region;
  protected Configuration conf;
  private long lastCompactSize = 0;
  volatile boolean forceMajor = false;
  /* how many bytes to write between status checks */
  static int closeCheckInterval = 0;
  private AtomicLong storeSize = new AtomicLong();
  private AtomicLong totalUncompressedBytes = new AtomicLong();
  private LongAdder memstoreOnlyRowReadsCount = new LongAdder();
  // rows that has cells from both memstore and files (or only files)
  private LongAdder mixedRowReadsCount = new LongAdder();

  private boolean cacheOnWriteLogged;

  /**
   * RWLock for store operations.
   * Locked in shared mode when the list of component stores is looked at:
   *   - all reads/writes to table data
   *   - checking for split
   * Locked in exclusive mode when the list of component stores is modified:
   *   - closing
   *   - completing a compaction
   */
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  /**
   * Lock specific to archiving compacted store files.  This avoids races around
   * the combination of retrieving the list of compacted files and moving them to
   * the archive directory.  Since this is usually a background process (other than
   * on close), we don't want to handle this with the store write lock, which would
   * block readers and degrade performance.
   *
   * Locked by:
   *   - CompactedHFilesDispatchHandler via closeAndArchiveCompactedFiles()
   *   - close()
   */
  final ReentrantLock archiveLock = new ReentrantLock();

  private final boolean verifyBulkLoads;

  /**
   * Use this counter to track concurrent puts. If TRACE-log is enabled, if we are over the
   * threshold set by hbase.region.store.parallel.put.print.threshold (Default is 50) we will
   * log a message that identifies the Store experience this high-level of concurrency.
   */
  private final AtomicInteger currentParallelPutCount = new AtomicInteger(0);
  private final int parallelPutCountPrintThreshold;

  private ScanInfo scanInfo;

  // All access must be synchronized.
  // TODO: ideally, this should be part of storeFileManager, as we keep passing this to it.
  private final List<HStoreFile> filesCompacting = Lists.newArrayList();

  // All access must be synchronized.
  private final Set<ChangedReadersObserver> changedReaderObservers =
    Collections.newSetFromMap(new ConcurrentHashMap<ChangedReadersObserver, Boolean>());

  private HFileDataBlockEncoder dataBlockEncoder;

  final StoreEngine<?, ?, ?, ?> storeEngine;

  private static final AtomicBoolean offPeakCompactionTracker = new AtomicBoolean();
  private volatile OffPeakHours offPeakHours;

  private static final int DEFAULT_FLUSH_RETRIES_NUMBER = 10;
  private int flushRetriesNumber;
  private int pauseTime;

  private long blockingFileCount;
  private int compactionCheckMultiplier;

  private AtomicLong flushedCellsCount = new AtomicLong();
  private AtomicLong compactedCellsCount = new AtomicLong();
  private AtomicLong majorCompactedCellsCount = new AtomicLong();
  private AtomicLong flushedCellsSize = new AtomicLong();
  private AtomicLong flushedOutputFileSize = new AtomicLong();
  private AtomicLong compactedCellsSize = new AtomicLong();
  private AtomicLong majorCompactedCellsSize = new AtomicLong();

  private final StoreContext storeContext;

  /**
   * Constructor
   * @param family HColumnDescriptor for this column
   * @param confParam configuration object failed.  Can be null.
   */
  protected HStore(final HRegion region, final ColumnFamilyDescriptor family,
      final Configuration confParam, boolean warmup) throws IOException {

    this.conf = StoreUtils.createStoreConfiguration(confParam, region.getTableDescriptor(), family);

    this.region = region;
    this.storeContext = initializeStoreContext(family);

    // Assemble the store's home directory and Ensure it exists.
    region.getRegionFileSystem().createStoreDir(family.getNameAsString());

    // set block storage policy for store directory
    String policyName = family.getStoragePolicy();
    if (null == policyName) {
      policyName = this.conf.get(BLOCK_STORAGE_POLICY_KEY, DEFAULT_BLOCK_STORAGE_POLICY);
    }
    region.getRegionFileSystem().setStoragePolicy(family.getNameAsString(), policyName.trim());

    this.dataBlockEncoder = new HFileDataBlockEncoderImpl(family.getDataBlockEncoding());

    // used by ScanQueryMatcher
    long timeToPurgeDeletes =
        Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    LOG.trace("Time to purge deletes set to {}ms in {}", timeToPurgeDeletes, this);
    // Get TTL
    long ttl = determineTTLFromFamily(family);
    // Why not just pass a HColumnDescriptor in here altogether?  Even if have
    // to clone it?
    scanInfo = new ScanInfo(conf, family, ttl, timeToPurgeDeletes, region.getCellComparator());
    this.memstore = getMemstore();

    this.offPeakHours = OffPeakHours.getInstance(conf);

    this.verifyBulkLoads = conf.getBoolean("hbase.hstore.bulkload.verify", false);

    this.blockingFileCount =
        conf.getInt(BLOCKING_STOREFILES_KEY, DEFAULT_BLOCKING_STOREFILE_COUNT);
    this.compactionCheckMultiplier = conf.getInt(
        COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
    if (this.compactionCheckMultiplier <= 0) {
      LOG.error("Compaction check period multiplier must be positive, setting default: {}",
          DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
      this.compactionCheckMultiplier = DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER;
    }

    if (HStore.closeCheckInterval == 0) {
      HStore.closeCheckInterval = conf.getInt(
          "hbase.hstore.close.check.interval", 10*1000*1000 /* 10 MB */);
    }

    this.storeEngine = createStoreEngine(this, this.conf, region.getCellComparator());
    List<HStoreFile> hStoreFiles = loadStoreFiles(warmup);
    // Move the storeSize calculation out of loadStoreFiles() method, because the secondary read
    // replica's refreshStoreFiles() will also use loadStoreFiles() to refresh its store files and
    // update the storeSize in the completeCompaction(..) finally (just like compaction) , so
    // no need calculate the storeSize twice.
    this.storeSize.addAndGet(getStorefilesSize(hStoreFiles, sf -> true));
    this.totalUncompressedBytes.addAndGet(getTotalUncompressedBytes(hStoreFiles));
    this.storeEngine.getStoreFileManager().loadFiles(hStoreFiles);

    flushRetriesNumber = conf.getInt(
        "hbase.hstore.flush.retries.number", DEFAULT_FLUSH_RETRIES_NUMBER);
    pauseTime = conf.getInt(HConstants.HBASE_SERVER_PAUSE, HConstants.DEFAULT_HBASE_SERVER_PAUSE);
    if (flushRetriesNumber <= 0) {
      throw new IllegalArgumentException(
          "hbase.hstore.flush.retries.number must be > 0, not "
              + flushRetriesNumber);
    }

    int confPrintThreshold =
        this.conf.getInt("hbase.region.store.parallel.put.print.threshold", 50);
    if (confPrintThreshold < 10) {
      confPrintThreshold = 10;
    }
    this.parallelPutCountPrintThreshold = confPrintThreshold;

    LOG.info("Store={},  memstore type={}, storagePolicy={}, verifyBulkLoads={}, "
            + "parallelPutCountPrintThreshold={}, encoding={}, compression={}",
        this, memstore.getClass().getSimpleName(), policyName, verifyBulkLoads,
        parallelPutCountPrintThreshold, family.getDataBlockEncoding(),
        family.getCompressionType());
    cacheOnWriteLogged = false;
  }

  private StoreContext initializeStoreContext(ColumnFamilyDescriptor family) throws IOException {
    return new StoreContext.Builder()
      .withBlockSize(family.getBlocksize())
      .withEncryptionContext(EncryptionUtil.createEncryptionContext(conf, family))
      .withBloomType(family.getBloomFilterType())
      .withCacheConfig(createCacheConf(family))
      .withCellComparator(region.getCellComparator())
      .withColumnFamilyDescriptor(family)
      .withCompactedFilesSupplier(this::getCompactedFiles)
      .withRegionFileSystem(region.getRegionFileSystem())
      .withFavoredNodesSupplier(this::getFavoredNodes)
      .withFamilyStoreDirectoryPath(region.getRegionFileSystem()
        .getStoreDir(family.getNameAsString()))
      .withRegionCoprocessorHost(region.getCoprocessorHost())
      .build();
  }

  private InetSocketAddress[] getFavoredNodes() {
    InetSocketAddress[] favoredNodes = null;
    if (region.getRegionServerServices() != null) {
      favoredNodes = region.getRegionServerServices().getFavoredNodesForRegion(
          region.getRegionInfo().getEncodedName());
    }
    return favoredNodes;
  }

  /**
   * @return MemStore Instance to use in this store.
   */
  private MemStore getMemstore() {
    MemStore ms = null;
    // Check if in-memory-compaction configured. Note MemoryCompactionPolicy is an enum!
    MemoryCompactionPolicy inMemoryCompaction = null;
    if (this.getTableName().isSystemTable()) {
      inMemoryCompaction = MemoryCompactionPolicy
        .valueOf(conf.get("hbase.systemtables.compacting.memstore.type", "NONE").toUpperCase());
    } else {
      inMemoryCompaction = getColumnFamilyDescriptor().getInMemoryCompaction();
    }
    if (inMemoryCompaction == null) {
      inMemoryCompaction =
          MemoryCompactionPolicy.valueOf(conf.get(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
              CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT).toUpperCase());
    }

    switch (inMemoryCompaction) {
      case NONE:
        Class<? extends MemStore> memStoreClass =
            conf.getClass(MEMSTORE_CLASS_NAME, DefaultMemStore.class, MemStore.class);
        ms = ReflectionUtils.newInstance(memStoreClass,
            new Object[] { conf, getComparator(),
                this.getHRegion().getRegionServicesForStores()});
        break;
      default:
        Class<? extends CompactingMemStore> compactingMemStoreClass =
            conf.getClass(MEMSTORE_CLASS_NAME, CompactingMemStore.class, CompactingMemStore.class);
        ms = ReflectionUtils.newInstance(compactingMemStoreClass,
          new Object[] { conf, getComparator(), this,
              this.getHRegion().getRegionServicesForStores(), inMemoryCompaction });
    }
    return ms;
  }

  /**
   * Creates the cache config.
   * @param family The current column family.
   */
  protected CacheConfig createCacheConf(final ColumnFamilyDescriptor family) {
    CacheConfig cacheConf = new CacheConfig(conf, family, region.getBlockCache(),
        region.getRegionServicesForStores().getByteBuffAllocator());
    LOG.info("Created cacheConfig: {}, for column family {} of region {} ", cacheConf,
      family.getNameAsString(), region.getRegionInfo().getEncodedName());
    return cacheConf;
  }

  /**
   * Creates the store engine configured for the given Store.
   * @param store The store. An unfortunate dependency needed due to it
   *              being passed to coprocessors via the compactor.
   * @param conf Store configuration.
   * @param kvComparator KVComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  protected StoreEngine<?, ?, ?, ?> createStoreEngine(HStore store, Configuration conf,
      CellComparator kvComparator) throws IOException {
    return StoreEngine.create(store, conf, kvComparator);
  }

  /**
   * @return TTL in seconds of the specified family
   */
  public static long determineTTLFromFamily(final ColumnFamilyDescriptor family) {
    // HCD.getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    long ttl = family.getTimeToLive();
    if (ttl == HConstants.FOREVER) {
      // Default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else {
      // Second -> ms adjust for user data
      ttl *= 1000;
    }
    return ttl;
  }

  StoreContext getStoreContext() {
    return storeContext;
  }

  @Override
  public String getColumnFamilyName() {
    return this.storeContext.getFamily().getNameAsString();
  }

  @Override
  public TableName getTableName() {
    return this.getRegionInfo().getTable();
  }

  @Override
  public FileSystem getFileSystem() {
    return storeContext.getRegionFileSystem().getFileSystem();
  }

  public HRegionFileSystem getRegionFileSystem() {
    return storeContext.getRegionFileSystem();
  }

  /* Implementation of StoreConfigInformation */
  @Override
  public long getStoreFileTtl() {
    // TTL only applies if there's no MIN_VERSIONs setting on the column.
    return (this.scanInfo.getMinVersions() == 0) ? this.scanInfo.getTtl() : Long.MAX_VALUE;
  }

  @Override
  public long getMemStoreFlushSize() {
    // TODO: Why is this in here?  The flushsize of the region rather than the store?  St.Ack
    return this.region.memstoreFlushSize;
  }

  @Override
  public MemStoreSize getFlushableSize() {
    return this.memstore.getFlushableSize();
  }

  @Override
  public MemStoreSize getSnapshotSize() {
    return this.memstore.getSnapshotSize();
  }

  @Override
  public long getCompactionCheckMultiplier() {
    return this.compactionCheckMultiplier;
  }

  @Override
  public long getBlockingFileCount() {
    return blockingFileCount;
  }
  /* End implementation of StoreConfigInformation */


  /**
   * @return how many bytes to write between status checks
   */
  public static int getCloseCheckInterval() {
    return closeCheckInterval;
  }

  @Override
  public ColumnFamilyDescriptor getColumnFamilyDescriptor() {
    return this.storeContext.getFamily();
  }

  @Override
  public OptionalLong getMaxSequenceId() {
    return StoreUtils.getMaxSequenceIdInList(this.getStorefiles());
  }

  @Override
  public OptionalLong getMaxMemStoreTS() {
    return StoreUtils.getMaxMemStoreTSInList(this.getStorefiles());
  }

  /**
   * @return the data block encoder
   */
  public HFileDataBlockEncoder getDataBlockEncoder() {
    return dataBlockEncoder;
  }

  /**
   * Should be used only in tests.
   * @param blockEncoder the block delta encoder to use
   */
  void setDataBlockEncoderInTest(HFileDataBlockEncoder blockEncoder) {
    this.dataBlockEncoder = blockEncoder;
  }

  /**
   * Creates an unsorted list of StoreFile loaded in parallel
   * from the given directory.
   */
  private List<HStoreFile> loadStoreFiles(boolean warmup) throws IOException {
    Collection<StoreFileInfo> files = getRegionFileSystem().getStoreFiles(getColumnFamilyName());
    return openStoreFiles(files, warmup);
  }

  private List<HStoreFile> openStoreFiles(Collection<StoreFileInfo> files, boolean warmup)
      throws IOException {
    if (CollectionUtils.isEmpty(files)) {
      return Collections.emptyList();
    }
    // initialize the thread pool for opening store files in parallel..
    ThreadPoolExecutor storeFileOpenerThreadPool =
      this.region.getStoreFileOpenAndCloseThreadPool("StoreFileOpener-" +
          this.getColumnFamilyName());
    CompletionService<HStoreFile> completionService =
      new ExecutorCompletionService<>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (StoreFileInfo storeFileInfo : files) {
      // open each store file in parallel
      completionService.submit(() -> this.createStoreFileAndReader(storeFileInfo));
      totalValidStoreFile++;
    }

    Set<String> compactedStoreFiles = new HashSet<>();
    ArrayList<HStoreFile> results = new ArrayList<>(files.size());
    IOException ioe = null;
    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        try {
          HStoreFile storeFile = completionService.take().get();
          if (storeFile != null) {
            LOG.debug("loaded {}", storeFile);
            results.add(storeFile);
            compactedStoreFiles.addAll(storeFile.getCompactedStoreFiles());
          }
        } catch (InterruptedException e) {
          if (ioe == null) {
            ioe = new InterruptedIOException(e.getMessage());
          }
        } catch (ExecutionException e) {
          if (ioe == null) {
            ioe = new IOException(e.getCause());
          }
        }
      }
    } finally {
      storeFileOpenerThreadPool.shutdownNow();
    }
    if (ioe != null) {
      // close StoreFile readers
      boolean evictOnClose =
          getCacheConfig() != null? getCacheConfig().shouldEvictOnClose(): true;
      for (HStoreFile file : results) {
        try {
          if (file != null) {
            file.closeStoreFile(evictOnClose);
          }
        } catch (IOException e) {
          LOG.warn("Could not close store file {}", file, e);
        }
      }
      throw ioe;
    }

    // Should not archive the compacted store files when region warmup. See HBASE-22163.
    if (!warmup) {
      // Remove the compacted files from result
      List<HStoreFile> filesToRemove = new ArrayList<>(compactedStoreFiles.size());
      for (HStoreFile storeFile : results) {
        if (compactedStoreFiles.contains(storeFile.getPath().getName())) {
          LOG.warn("Clearing the compacted storefile {} from {}", storeFile, this);
          storeFile.getReader().close(storeFile.getCacheConf() != null ?
            storeFile.getCacheConf().shouldEvictOnClose() : true);
          filesToRemove.add(storeFile);
        }
      }
      results.removeAll(filesToRemove);
      if (!filesToRemove.isEmpty() && this.isPrimaryReplicaStore()) {
        LOG.debug("Moving the files {} to archive", filesToRemove);
        getRegionFileSystem().removeStoreFiles(this.getColumnFamilyDescriptor().getNameAsString(),
            filesToRemove);
      }
    }

    return results;
  }

  @Override
  public void refreshStoreFiles() throws IOException {
    Collection<StoreFileInfo> newFiles = getRegionFileSystem().getStoreFiles(getColumnFamilyName());
    refreshStoreFilesInternal(newFiles);
  }

  /**
   * Replaces the store files that the store has with the given files. Mainly used by secondary
   * region replicas to keep up to date with the primary region files.
   */
  public void refreshStoreFiles(Collection<String> newFiles) throws IOException {
    List<StoreFileInfo> storeFiles = new ArrayList<>(newFiles.size());
    for (String file : newFiles) {
      storeFiles.add(getRegionFileSystem().getStoreFileInfo(getColumnFamilyName(), file));
    }
    refreshStoreFilesInternal(storeFiles);
  }

  /**
   * Checks the underlying store files, and opens the files that  have not
   * been opened, and removes the store file readers for store files no longer
   * available. Mainly used by secondary region replicas to keep up to date with
   * the primary region files.
   */
  private void refreshStoreFilesInternal(Collection<StoreFileInfo> newFiles) throws IOException {
    StoreFileManager sfm = storeEngine.getStoreFileManager();
    Collection<HStoreFile> currentFiles = sfm.getStorefiles();
    Collection<HStoreFile> compactedFiles = sfm.getCompactedfiles();
    if (currentFiles == null) {
      currentFiles = Collections.emptySet();
    }
    if (newFiles == null) {
      newFiles = Collections.emptySet();
    }
    if (compactedFiles == null) {
      compactedFiles = Collections.emptySet();
    }

    HashMap<StoreFileInfo, HStoreFile> currentFilesSet = new HashMap<>(currentFiles.size());
    for (HStoreFile sf : currentFiles) {
      currentFilesSet.put(sf.getFileInfo(), sf);
    }
    HashMap<StoreFileInfo, HStoreFile> compactedFilesSet = new HashMap<>(compactedFiles.size());
    for (HStoreFile sf : compactedFiles) {
      compactedFilesSet.put(sf.getFileInfo(), sf);
    }

    Set<StoreFileInfo> newFilesSet = new HashSet<StoreFileInfo>(newFiles);
    // Exclude the files that have already been compacted
    newFilesSet = Sets.difference(newFilesSet, compactedFilesSet.keySet());
    Set<StoreFileInfo> toBeAddedFiles = Sets.difference(newFilesSet, currentFilesSet.keySet());
    Set<StoreFileInfo> toBeRemovedFiles = Sets.difference(currentFilesSet.keySet(), newFilesSet);

    if (toBeAddedFiles.isEmpty() && toBeRemovedFiles.isEmpty()) {
      return;
    }

    LOG.info("Refreshing store files for " + this + " files to add: "
      + toBeAddedFiles + " files to remove: " + toBeRemovedFiles);

    Set<HStoreFile> toBeRemovedStoreFiles = new HashSet<>(toBeRemovedFiles.size());
    for (StoreFileInfo sfi : toBeRemovedFiles) {
      toBeRemovedStoreFiles.add(currentFilesSet.get(sfi));
    }

    // try to open the files
    List<HStoreFile> openedFiles = openStoreFiles(toBeAddedFiles, false);

    // propogate the file changes to the underlying store file manager
    replaceStoreFiles(toBeRemovedStoreFiles, openedFiles); //won't throw an exception

    // Advance the memstore read point to be at least the new store files seqIds so that
    // readers might pick it up. This assumes that the store is not getting any writes (otherwise
    // in-flight transactions might be made visible)
    if (!toBeAddedFiles.isEmpty()) {
      // we must have the max sequence id here as we do have several store files
      region.getMVCC().advanceTo(this.getMaxSequenceId().getAsLong());
    }

    completeCompaction(toBeRemovedStoreFiles);
  }

  protected HStoreFile createStoreFileAndReader(final Path p) throws IOException {
    StoreFileInfo info = new StoreFileInfo(conf, this.getFileSystem(),
        p, isPrimaryReplicaStore());
    return createStoreFileAndReader(info);
  }

  private HStoreFile createStoreFileAndReader(StoreFileInfo info) throws IOException {
    info.setRegionCoprocessorHost(this.region.getCoprocessorHost());
    HStoreFile storeFile = new HStoreFile(info, getColumnFamilyDescriptor().getBloomFilterType(),
            getCacheConfig());
    storeFile.initReader();
    return storeFile;
  }

  /**
   * This message intends to inform the MemStore that next coming updates
   * are going to be part of the replaying edits from WAL
   */
  public void startReplayingFromWAL(){
    this.memstore.startReplayingFromWAL();
  }

  /**
   * This message intends to inform the MemStore that the replaying edits from WAL
   * are done
   */
  public void stopReplayingFromWAL(){
    this.memstore.stopReplayingFromWAL();
  }

  /**
   * Adds a value to the memstore
   */
  public void add(final Cell cell, MemStoreSizing memstoreSizing) {
    lock.readLock().lock();
    try {
      if (this.currentParallelPutCount.getAndIncrement() > this.parallelPutCountPrintThreshold) {
        LOG.trace("tableName={}, encodedName={}, columnFamilyName={} is too busy!",
            this.getTableName(), this.getRegionInfo().getEncodedName(), this.getColumnFamilyName());
      }
      this.memstore.add(cell, memstoreSizing);
    } finally {
      lock.readLock().unlock();
      currentParallelPutCount.decrementAndGet();
    }
  }

  /**
   * Adds the specified value to the memstore
   */
  public void add(final Iterable<Cell> cells, MemStoreSizing memstoreSizing) {
    lock.readLock().lock();
    try {
      if (this.currentParallelPutCount.getAndIncrement() > this.parallelPutCountPrintThreshold) {
        LOG.trace("tableName={}, encodedName={}, columnFamilyName={} is too busy!",
            this.getTableName(), this.getRegionInfo().getEncodedName(), this.getColumnFamilyName());
      }
      memstore.add(cells, memstoreSizing);
    } finally {
      lock.readLock().unlock();
      currentParallelPutCount.decrementAndGet();
    }
  }

  @Override
  public long timeOfOldestEdit() {
    return memstore.timeOfOldestEdit();
  }

  /**
   * @return All store files.
   */
  @Override
  public Collection<HStoreFile> getStorefiles() {
    return this.storeEngine.getStoreFileManager().getStorefiles();
  }

  @Override
  public Collection<HStoreFile> getCompactedFiles() {
    return this.storeEngine.getStoreFileManager().getCompactedfiles();
  }

  /**
   * This throws a WrongRegionException if the HFile does not fit in this region, or an
   * InvalidHFileException if the HFile is not valid.
   */
  public void assertBulkLoadHFileOk(Path srcPath) throws IOException {
    HFile.Reader reader  = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in " + this);
      FileSystem srcFs = srcPath.getFileSystem(conf);
      srcFs.access(srcPath, FsAction.READ_WRITE);
      reader = HFile.createReader(srcFs, srcPath, getCacheConfig(), isPrimaryReplicaStore(), conf);

      Optional<byte[]> firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey.isPresent(), "First key can not be null");
      Optional<Cell> lk = reader.getLastKey();
      Preconditions.checkState(lk.isPresent(), "Last key can not be null");
      byte[] lastKey =  CellUtil.cloneRow(lk.get());

      if (LOG.isDebugEnabled()) {
        LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey.get()) +
            " last=" + Bytes.toStringBinary(lastKey));
        LOG.debug("Region bounds: first=" +
            Bytes.toStringBinary(getRegionInfo().getStartKey()) +
            " last=" + Bytes.toStringBinary(getRegionInfo().getEndKey()));
      }

      if (!this.getRegionInfo().containsRange(firstKey.get(), lastKey)) {
        throw new WrongRegionException(
            "Bulk load file " + srcPath.toString() + " does not fit inside region "
            + this.getRegionInfo().getRegionNameAsString());
      }

      if(reader.length() > conf.getLong(HConstants.HREGION_MAX_FILESIZE,
          HConstants.DEFAULT_MAX_FILE_SIZE)) {
        LOG.warn("Trying to bulk load hfile " + srcPath + " with size: " +
            reader.length() + " bytes can be problematic as it may lead to oversplitting.");
      }

      if (verifyBulkLoads) {
        long verificationStartTime = EnvironmentEdgeManager.currentTime();
        LOG.info("Full verification started for bulk load hfile: {}", srcPath);
        Cell prevCell = null;
        HFileScanner scanner = reader.getScanner(false, false, false);
        scanner.seekTo();
        do {
          Cell cell = scanner.getCell();
          if (prevCell != null) {
            if (getComparator().compareRows(prevCell, cell) > 0) {
              throw new InvalidHFileException("Previous row is greater than"
                  + " current row: path=" + srcPath + " previous="
                  + CellUtil.getCellKeyAsString(prevCell) + " current="
                  + CellUtil.getCellKeyAsString(cell));
            }
            if (CellComparator.getInstance().compareFamilies(prevCell, cell) != 0) {
              throw new InvalidHFileException("Previous key had different"
                  + " family compared to current key: path=" + srcPath
                  + " previous="
                  + Bytes.toStringBinary(prevCell.getFamilyArray(), prevCell.getFamilyOffset(),
                      prevCell.getFamilyLength())
                  + " current="
                  + Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(),
                      cell.getFamilyLength()));
            }
          }
          prevCell = cell;
        } while (scanner.next());
        LOG.info("Full verification complete for bulk load hfile: " + srcPath.toString() +
          " took " + (EnvironmentEdgeManager.currentTime() - verificationStartTime) + " ms");
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * This method should only be called from Region. It is assumed that the ranges of values in the
   * HFile fit within the stores assigned region. (assertBulkLoadHFileOk checks this)
   *
   * @param seqNum sequence Id associated with the HFile
   */
  public Pair<Path, Path> preBulkLoadHFile(String srcPathStr, long seqNum) throws IOException {
    Path srcPath = new Path(srcPathStr);
    return getRegionFileSystem().bulkLoadStoreFile(getColumnFamilyName(), srcPath, seqNum);
  }

  public Path bulkLoadHFile(byte[] family, String srcPathStr, Path dstPath) throws IOException {
    Path srcPath = new Path(srcPathStr);
    try {
      getRegionFileSystem().commitStoreFile(srcPath, dstPath);
    } finally {
      if (this.getCoprocessorHost() != null) {
        this.getCoprocessorHost().postCommitStoreFile(family, srcPath, dstPath);
      }
    }

    LOG.info("Loaded HFile " + srcPath + " into " + this + " as "
        + dstPath + " - updating store file list.");

    HStoreFile sf = createStoreFileAndReader(dstPath);
    bulkLoadHFile(sf);

    LOG.info("Successfully loaded {} into {} (new location: {})",
        srcPath, this, dstPath);

    return dstPath;
  }

  public void bulkLoadHFile(StoreFileInfo fileInfo) throws IOException {
    HStoreFile sf = createStoreFileAndReader(fileInfo);
    bulkLoadHFile(sf);
  }

  private void bulkLoadHFile(HStoreFile sf) throws IOException {
    StoreFileReader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

    // Append the new storefile into the list
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().insertNewFiles(Lists.newArrayList(sf));
    } finally {
      // We need the lock, as long as we are updating the storeFiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      this.lock.writeLock().unlock();
    }
    LOG.info("Loaded HFile " + sf.getFileInfo() + " into " + this);
    if (LOG.isTraceEnabled()) {
      String traceMessage = "BULK LOAD time,size,store size,store files ["
          + EnvironmentEdgeManager.currentTime() + "," + r.length() + "," + storeSize
          + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
  }

  /**
   * Close all the readers We don't need to worry about subsequent requests because the Region holds
   * a write lock that will prevent any more reads or writes.
   * @return the {@link StoreFile StoreFiles} that were previously being used.
   * @throws IOException on failure
   */
  public ImmutableCollection<HStoreFile> close() throws IOException {
    this.archiveLock.lock();
    this.lock.writeLock().lock();
    try {
      // Clear so metrics doesn't find them.
      ImmutableCollection<HStoreFile> result = storeEngine.getStoreFileManager().clearFiles();
      Collection<HStoreFile> compactedfiles =
          storeEngine.getStoreFileManager().clearCompactedFiles();
      // clear the compacted files
      if (CollectionUtils.isNotEmpty(compactedfiles)) {
        removeCompactedfiles(compactedfiles, getCacheConfig() != null ?
            getCacheConfig().shouldEvictOnClose() : true);
      }
      if (!result.isEmpty()) {
        // initialize the thread pool for closing store files in parallel.
        ThreadPoolExecutor storeFileCloserThreadPool = this.region
            .getStoreFileOpenAndCloseThreadPool("StoreFileCloser-"
                + this.getColumnFamilyName());

        // close each store file in parallel
        CompletionService<Void> completionService =
          new ExecutorCompletionService<>(storeFileCloserThreadPool);
        for (HStoreFile f : result) {
          completionService.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
              boolean evictOnClose =
                  getCacheConfig() != null? getCacheConfig().shouldEvictOnClose(): true;
              f.closeStoreFile(evictOnClose);
              return null;
            }
          });
        }

        IOException ioe = null;
        try {
          for (int i = 0; i < result.size(); i++) {
            try {
              Future<Void> future = completionService.take();
              future.get();
            } catch (InterruptedException e) {
              if (ioe == null) {
                ioe = new InterruptedIOException();
                ioe.initCause(e);
              }
            } catch (ExecutionException e) {
              if (ioe == null) {
                ioe = new IOException(e.getCause());
              }
            }
          }
        } finally {
          storeFileCloserThreadPool.shutdownNow();
        }
        if (ioe != null) {
          throw ioe;
        }
      }
      LOG.trace("Closed {}", this);
      return result;
    } finally {
      this.lock.writeLock().unlock();
      this.archiveLock.unlock();
    }
  }

  /**
   * Snapshot this stores memstore. Call before running
   * {@link #flushCache(long, MemStoreSnapshot, MonitoredTask, ThroughputController,
   * FlushLifeCycleTracker)}
   *  so it has some work to do.
   */
  void snapshot() {
    this.lock.writeLock().lock();
    try {
      this.memstore.snapshot();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Write out current snapshot. Presumes {@link #snapshot()} has been called previously.
   * @param logCacheFlushId flush sequence number
   * @return The path name of the tmp file to which the store was flushed
   * @throws IOException if exception occurs during process
   */
  protected List<Path> flushCache(final long logCacheFlushId, MemStoreSnapshot snapshot,
      MonitoredTask status, ThroughputController throughputController,
      FlushLifeCycleTracker tracker) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    // Retry after catching exception when flushing, otherwise server will abort
    // itself
    StoreFlusher flusher = storeEngine.getStoreFlusher();
    IOException lastException = null;
    for (int i = 0; i < flushRetriesNumber; i++) {
      try {
        List<Path> pathNames =
            flusher.flushSnapshot(snapshot, logCacheFlushId, status, throughputController, tracker);
        Path lastPathName = null;
        try {
          for (Path pathName : pathNames) {
            lastPathName = pathName;
            validateStoreFile(pathName);
          }
          return pathNames;
        } catch (Exception e) {
          LOG.warn("Failed validating store file {}, retrying num={}", lastPathName, i, e);
          if (e instanceof IOException) {
            lastException = (IOException) e;
          } else {
            lastException = new IOException(e);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed flushing store file for {}, retrying num={}", this, i, e);
        lastException = e;
      }
      if (lastException != null && i < (flushRetriesNumber - 1)) {
        try {
          Thread.sleep(pauseTime);
        } catch (InterruptedException e) {
          IOException iie = new InterruptedIOException();
          iie.initCause(e);
          throw iie;
        }
      }
    }
    throw lastException;
  }

  public HStoreFile tryCommitRecoveredHFile(Path path) throws IOException {
    LOG.info("Validating recovered hfile at {} for inclusion in store {}", path, this);
    FileSystem srcFs = path.getFileSystem(conf);
    srcFs.access(path, FsAction.READ_WRITE);
    try (HFile.Reader reader =
        HFile.createReader(srcFs, path, getCacheConfig(), isPrimaryReplicaStore(), conf)) {
      Optional<byte[]> firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey.isPresent(), "First key can not be null");
      Optional<Cell> lk = reader.getLastKey();
      Preconditions.checkState(lk.isPresent(), "Last key can not be null");
      byte[] lastKey = CellUtil.cloneRow(lk.get());
      if (!this.getRegionInfo().containsRange(firstKey.get(), lastKey)) {
        throw new WrongRegionException("Recovered hfile " + path.toString() +
            " does not fit inside region " + this.getRegionInfo().getRegionNameAsString());
      }
    }

    Path dstPath = getRegionFileSystem().commitStoreFile(getColumnFamilyName(), path);
    HStoreFile sf = createStoreFileAndReader(dstPath);
    StoreFileReader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().insertNewFiles(Lists.newArrayList(sf));
    } finally {
      this.lock.writeLock().unlock();
    }

    LOG.info("Loaded recovered hfile to {}, entries={}, sequenceid={}, filesize={}", sf,
      r.getEntries(), r.getSequenceID(), TraditionalBinaryPrefix.long2String(r.length(), "B", 1));
    return sf;
  }

  /**
   * @param path The pathname of the tmp file into which the store was flushed
   * @return store file created.
   */
  private HStoreFile commitFile(Path path, long logCacheFlushId, MonitoredTask status)
      throws IOException {
    // Write-out finished successfully, move into the right spot
    Path dstPath = getRegionFileSystem().commitStoreFile(getColumnFamilyName(), path);

    status.setStatus("Flushing " + this + ": reopening flushed file");
    HStoreFile sf = createStoreFileAndReader(dstPath);

    StoreFileReader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

    if (LOG.isInfoEnabled()) {
      LOG.info("Added " + sf + ", entries=" + r.getEntries() +
        ", sequenceid=" + logCacheFlushId +
        ", filesize=" + TraditionalBinaryPrefix.long2String(r.length(), "", 1));
    }
    return sf;
  }

  public StoreFileWriter createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
    boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
    boolean shouldDropBehind) throws IOException {
    return createWriterInTmp(maxKeyCount, compression, isCompaction, includeMVCCReadpoint,
      includesTag, shouldDropBehind, -1, HConstants.EMPTY_STRING);
  }

  /**
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includeMVCCReadpoint - whether to include MVCC or not
   * @param includesTag - includesTag or not
   * @return Writer for a new StoreFile in the tmp dir.
   */
  // TODO : allow the Writer factory to create Writers of ShipperListener type only in case of
  // compaction
  public StoreFileWriter createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
      boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
      boolean shouldDropBehind, long totalCompactedFilesSize, String fileStoragePolicy)
        throws IOException {
    // creating new cache config for each new writer
    final CacheConfig cacheConf = getCacheConfig();
    final CacheConfig writerCacheConf = new CacheConfig(cacheConf);
    if (isCompaction) {
      // Don't cache data on write on compactions, unless specifically configured to do so
      // Cache only when total file size remains lower than configured threshold
      final boolean cacheCompactedBlocksOnWrite =
        getCacheConfig().shouldCacheCompactedBlocksOnWrite();
      // if data blocks are to be cached on write
      // during compaction, we should forcefully
      // cache index and bloom blocks as well
      if (cacheCompactedBlocksOnWrite && totalCompactedFilesSize <= cacheConf
        .getCacheCompactedBlocksOnWriteThreshold()) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheCompactedBlocksOnWrite is true, hence enabled " +
              "cacheOnWrite for Data blocks, Index blocks and Bloom filter blocks", this);
          cacheOnWriteLogged = true;
        }
      } else {
        writerCacheConf.setCacheDataOnWrite(false);
        if (totalCompactedFilesSize > cacheConf.getCacheCompactedBlocksOnWriteThreshold()) {
          // checking condition once again for logging
          LOG.debug(
            "For {}, setting cacheCompactedBlocksOnWrite as false as total size of compacted "
              + "files - {}, is greater than cacheCompactedBlocksOnWriteThreshold - {}",
            this, totalCompactedFilesSize,
            cacheConf.getCacheCompactedBlocksOnWriteThreshold());
        }
      }
    } else {
      final boolean shouldCacheDataOnWrite = cacheConf.shouldCacheDataOnWrite();
      if (shouldCacheDataOnWrite) {
        writerCacheConf.enableCacheOnWrite();
        if (!cacheOnWriteLogged) {
          LOG.info("For {} , cacheDataOnWrite is true, hence enabled cacheOnWrite for " +
            "Index blocks and Bloom filter blocks", this);
          cacheOnWriteLogged = true;
        }
      }
    }
    Encryption.Context encryptionContext = storeContext.getEncryptionContext();
    HFileContext hFileContext = createFileContext(compression, includeMVCCReadpoint, includesTag,
      encryptionContext);
    Path familyTempDir = new Path(getRegionFileSystem().getTempDir(), getColumnFamilyName());
    StoreFileWriter.Builder builder =
      new StoreFileWriter.Builder(conf, writerCacheConf, getFileSystem())
        .withOutputDir(familyTempDir)
        .withBloomType(storeContext.getBloomFilterType())
        .withMaxKeyCount(maxKeyCount)
        .withFavoredNodes(storeContext.getFavoredNodes())
        .withFileContext(hFileContext)
        .withShouldDropCacheBehind(shouldDropBehind)
        .withCompactedFilesSupplier(storeContext.getCompactedFilesSupplier())
        .withFileStoragePolicy(fileStoragePolicy);
    return builder.build();
  }

  private HFileContext createFileContext(Compression.Algorithm compression,
    boolean includeMVCCReadpoint, boolean includesTag, Encryption.Context encryptionContext) {
    if (compression == null) {
      compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    ColumnFamilyDescriptor family = getColumnFamilyDescriptor();
    HFileContext hFileContext = new HFileContextBuilder()
      .withIncludesMvcc(includeMVCCReadpoint)
      .withIncludesTags(includesTag)
      .withCompression(compression)
      .withCompressTags(family.isCompressTags())
      .withChecksumType(StoreUtils.getChecksumType(conf))
      .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf))
      .withBlockSize(family.getBlocksize())
      .withHBaseCheckSum(true)
      .withDataBlockEncoding(family.getDataBlockEncoding())
      .withEncryptionContext(encryptionContext)
      .withCreateTime(EnvironmentEdgeManager.currentTime())
      .withColumnFamily(getColumnFamilyDescriptor().getName())
      .withTableName(getTableName().getName())
      .withCellComparator(getComparator())
      .build();
    return hFileContext;
  }

  private long getTotalSize(Collection<HStoreFile> sfs) {
    return sfs.stream().mapToLong(sf -> sf.getReader().length()).sum();
  }

  /**
   * Change storeFiles adding into place the Reader produced by this new flush.
   * @param sfs Store files
   * @return Whether compaction is required.
   */
  private boolean updateStorefiles(List<HStoreFile> sfs, long snapshotId) throws IOException {
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().insertNewFiles(sfs);
      /**
       * NOTE:we should keep clearSnapshot method inside the write lock because clearSnapshot may
       * close {@link DefaultMemStore#snapshot}, which may be used by
       * {@link DefaultMemStore#getScanners}.
       */
      if (snapshotId > 0) {
        this.memstore.clearSnapshot(snapshotId);
      }
    } finally {
      // We need the lock, as long as we are updating the storeFiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      this.lock.writeLock().unlock();
    }

    // notify to be called here - only in case of flushes
    notifyChangedReadersObservers(sfs);
    if (LOG.isTraceEnabled()) {
      long totalSize = getTotalSize(sfs);
      String traceMessage = "FLUSH time,count,size,store size,store files ["
          + EnvironmentEdgeManager.currentTime() + "," + sfs.size() + "," + totalSize
          + "," + storeSize + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
    return needsCompaction();
  }

  /**
   * Notify all observers that set of Readers has changed.
   */
  private void notifyChangedReadersObservers(List<HStoreFile> sfs) throws IOException {
    for (ChangedReadersObserver o : this.changedReaderObservers) {
      List<KeyValueScanner> memStoreScanners;
      this.lock.readLock().lock();
      try {
        memStoreScanners = this.memstore.getScanners(o.getReadPoint());
      } finally {
        this.lock.readLock().unlock();
      }
      o.updateReaders(sfs, memStoreScanners);
    }
  }

  /**
   * Get all scanners with no filtering based on TTL (that happens further down the line).
   * @param cacheBlocks cache the blocks or not
   * @param usePread true to use pread, false if not
   * @param isCompaction true if the scanner is created for compaction
   * @param matcher the scan query matcher
   * @param startRow the start row
   * @param stopRow the stop row
   * @param readPt the read point of the current scan
   * @return all scanners for this store
   */
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean isGet, boolean usePread,
      boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, byte[] stopRow, long readPt)
      throws IOException {
    return getScanners(cacheBlocks, usePread, isCompaction, matcher, startRow, true, stopRow, false,
      readPt);
  }

  /**
   * Get all scanners with no filtering based on TTL (that happens further down the line).
   * @param cacheBlocks cache the blocks or not
   * @param usePread true to use pread, false if not
   * @param isCompaction true if the scanner is created for compaction
   * @param matcher the scan query matcher
   * @param startRow the start row
   * @param includeStartRow true to include start row, false if not
   * @param stopRow the stop row
   * @param includeStopRow true to include stop row, false if not
   * @param readPt the read point of the current scan
   * @return all scanners for this store
   */
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean usePread,
      boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, boolean includeStartRow,
      byte[] stopRow, boolean includeStopRow, long readPt) throws IOException {
    Collection<HStoreFile> storeFilesToScan;
    List<KeyValueScanner> memStoreScanners;
    this.lock.readLock().lock();
    try {
      storeFilesToScan = this.storeEngine.getStoreFileManager().getFilesForScan(startRow,
        includeStartRow, stopRow, includeStopRow);
      memStoreScanners = this.memstore.getScanners(readPt);
    } finally {
      this.lock.readLock().unlock();
    }

    try {
      // First the store file scanners

      // TODO this used to get the store files in descending order,
      // but now we get them in ascending order, which I think is
      // actually more correct, since memstore get put at the end.
      List<StoreFileScanner> sfScanners = StoreFileScanner
        .getScannersForStoreFiles(storeFilesToScan, cacheBlocks, usePread, isCompaction, false,
          matcher, readPt);
      List<KeyValueScanner> scanners = new ArrayList<>(sfScanners.size() + 1);
      scanners.addAll(sfScanners);
      // Then the memstore scanners
      scanners.addAll(memStoreScanners);
      return scanners;
    } catch (Throwable t) {
      clearAndClose(memStoreScanners);
      throw t instanceof IOException ? (IOException) t : new IOException(t);
    }
  }

  private static void clearAndClose(List<KeyValueScanner> scanners) {
    if (scanners == null) {
      return;
    }
    for (KeyValueScanner s : scanners) {
      s.close();
    }
    scanners.clear();
  }

  /**
   * Create scanners on the given files and if needed on the memstore with no filtering based on TTL
   * (that happens further down the line).
   * @param files the list of files on which the scanners has to be created
   * @param cacheBlocks cache the blocks or not
   * @param usePread true to use pread, false if not
   * @param isCompaction true if the scanner is created for compaction
   * @param matcher the scan query matcher
   * @param startRow the start row
   * @param stopRow the stop row
   * @param readPt the read point of the current scan
   * @param includeMemstoreScanner true if memstore has to be included
   * @return scanners on the given files and on the memstore if specified
   */
  public List<KeyValueScanner> getScanners(List<HStoreFile> files, boolean cacheBlocks,
      boolean isGet, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
      byte[] startRow, byte[] stopRow, long readPt, boolean includeMemstoreScanner)
      throws IOException {
    return getScanners(files, cacheBlocks, usePread, isCompaction, matcher, startRow, true, stopRow,
      false, readPt, includeMemstoreScanner);
  }

  /**
   * Create scanners on the given files and if needed on the memstore with no filtering based on TTL
   * (that happens further down the line).
   * @param files the list of files on which the scanners has to be created
   * @param cacheBlocks ache the blocks or not
   * @param usePread true to use pread, false if not
   * @param isCompaction true if the scanner is created for compaction
   * @param matcher the scan query matcher
   * @param startRow the start row
   * @param includeStartRow true to include start row, false if not
   * @param stopRow the stop row
   * @param includeStopRow true to include stop row, false if not
   * @param readPt the read point of the current scan
   * @param includeMemstoreScanner true if memstore has to be included
   * @return scanners on the given files and on the memstore if specified
   */
  public List<KeyValueScanner> getScanners(List<HStoreFile> files, boolean cacheBlocks,
      boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow,
      boolean includeStartRow, byte[] stopRow, boolean includeStopRow, long readPt,
      boolean includeMemstoreScanner) throws IOException {
    List<KeyValueScanner> memStoreScanners = null;
    if (includeMemstoreScanner) {
      this.lock.readLock().lock();
      try {
        memStoreScanners = this.memstore.getScanners(readPt);
      } finally {
        this.lock.readLock().unlock();
      }
    }
    try {
      List<StoreFileScanner> sfScanners = StoreFileScanner
        .getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction, false, matcher,
          readPt);
      List<KeyValueScanner> scanners = new ArrayList<>(sfScanners.size() + 1);
      scanners.addAll(sfScanners);
      // Then the memstore scanners
      if (memStoreScanners != null) {
        scanners.addAll(memStoreScanners);
      }
      return scanners;
    } catch (Throwable t) {
      clearAndClose(memStoreScanners);
      throw t instanceof IOException ? (IOException) t : new IOException(t);
    }
  }

  /**
   * @param o Observer who wants to know about changes in set of Readers
   */
  public void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }

  /**
   * @param o Observer no longer interested in changes in set of Readers.
   */
  public void deleteChangedReaderObserver(ChangedReadersObserver o) {
    // We don't check if observer present; it may not be (legitimately)
    this.changedReaderObservers.remove(o);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Compact the StoreFiles.  This method may take some time, so the calling
   * thread must be able to block for long periods.
   *
   * <p>During this time, the Store can work as usual, getting values from
   * StoreFiles and writing new StoreFiles from the memstore.
   *
   * Existing StoreFiles are not destroyed until the new compacted StoreFile is
   * completely written-out to disk.
   *
   * <p>The compactLock prevents multiple simultaneous compactions.
   * The structureLock prevents us from interfering with other write operations.
   *
   * <p>We don't want to hold the structureLock for the whole time, as a compact()
   * can be lengthy and we want to allow cache-flushes during this period.
   *
   * <p> Compaction event should be idempotent, since there is no IO Fencing for
   * the region directory in hdfs. A region server might still try to complete the
   * compaction after it lost the region. That is why the following events are carefully
   * ordered for a compaction:
   *  1. Compaction writes new files under region/.tmp directory (compaction output)
   *  2. Compaction atomically moves the temporary file under region directory
   *  3. Compaction appends a WAL edit containing the compaction input and output files.
   *  Forces sync on WAL.
   *  4. Compaction deletes the input files from the region directory.
   *
   * Failure conditions are handled like this:
   *  - If RS fails before 2, compaction wont complete. Even if RS lives on and finishes
   *  the compaction later, it will only write the new data file to the region directory.
   *  Since we already have this data, this will be idempotent but we will have a redundant
   *  copy of the data.
   *  - If RS fails between 2 and 3, the region will have a redundant copy of the data. The
   *  RS that failed won't be able to finish sync() for WAL because of lease recovery in WAL.
   *  - If RS fails after 3, the region region server who opens the region will pick up the
   *  the compaction marker from the WAL and replay it by removing the compaction input files.
   *  Failed RS can also attempt to delete those files, but the operation will be idempotent
   *
   * See HBASE-2231 for details.
   *
   * @param compaction compaction details obtained from requestCompaction()
   * @return Storefile we compacted into or null if we failed or opted out early.
   */
  public List<HStoreFile> compact(CompactionContext compaction,
    ThroughputController throughputController, User user) throws IOException {
    assert compaction != null;
    CompactionRequestImpl cr = compaction.getRequest();
    try {
      // Do all sanity checking in here if we have a valid CompactionRequestImpl
      // because we need to clean up after it on the way out in a finally
      // block below
      long compactionStartTime = EnvironmentEdgeManager.currentTime();
      assert compaction.hasSelection();
      Collection<HStoreFile> filesToCompact = cr.getFiles();
      assert !filesToCompact.isEmpty();
      synchronized (filesCompacting) {
        // sanity check: we're compacting files that this store knows about
        // TODO: change this to LOG.error() after more debugging
        Preconditions.checkArgument(filesCompacting.containsAll(filesToCompact));
      }

      // Ready to go. Have list of files to compact.
      LOG.info("Starting compaction of " + filesToCompact +
        " into tmpdir=" + getRegionFileSystem().getTempDir() + ", totalSize=" +
          TraditionalBinaryPrefix.long2String(cr.getSize(), "", 1));

      return doCompaction(cr, filesToCompact, user, compactionStartTime,
          compaction.compact(throughputController, user));
    } finally {
      finishCompactionRequest(cr);
    }
  }

  protected List<HStoreFile> doCompaction(CompactionRequestImpl cr,
      Collection<HStoreFile> filesToCompact, User user, long compactionStartTime,
      List<Path> newFiles) throws IOException {
    // Do the steps necessary to complete the compaction.
    setStoragePolicyFromFileName(newFiles);
    List<HStoreFile> sfs = moveCompactedFilesIntoPlace(cr, newFiles, user);
    writeCompactionWalRecord(filesToCompact, sfs);
    replaceStoreFiles(filesToCompact, sfs);
    if (cr.isMajor()) {
      majorCompactedCellsCount.addAndGet(getCompactionProgress().getTotalCompactingKVs());
      majorCompactedCellsSize.addAndGet(getCompactionProgress().totalCompactedSize);
    } else {
      compactedCellsCount.addAndGet(getCompactionProgress().getTotalCompactingKVs());
      compactedCellsSize.addAndGet(getCompactionProgress().totalCompactedSize);
    }
    long outputBytes = getTotalSize(sfs);

    // At this point the store will use new files for all new scanners.
    completeCompaction(filesToCompact); // update store size.

    long now = EnvironmentEdgeManager.currentTime();
    if (region.getRegionServerServices() != null
        && region.getRegionServerServices().getMetrics() != null) {
      region.getRegionServerServices().getMetrics().updateCompaction(
          region.getTableDescriptor().getTableName().getNameAsString(),
          cr.isMajor(), now - compactionStartTime, cr.getFiles().size(),
          newFiles.size(), cr.getSize(), outputBytes);

    }

    logCompactionEndMessage(cr, sfs, now, compactionStartTime);
    return sfs;
  }

  // Set correct storage policy from the file name of DTCP.
  // Rename file will not change the storage policy.
  private void setStoragePolicyFromFileName(List<Path> newFiles) throws IOException {
    String prefix = HConstants.STORAGE_POLICY_PREFIX;
    for (Path newFile : newFiles) {
      if (newFile.getParent().getName().startsWith(prefix)) {
        CommonFSUtils.setStoragePolicy(getRegionFileSystem().getFileSystem(), newFile,
            newFile.getParent().getName().substring(prefix.length()));
      }
    }
  }

  private List<HStoreFile> moveCompactedFilesIntoPlace(CompactionRequestImpl cr,
      List<Path> newFiles, User user) throws IOException {
    List<HStoreFile> sfs = new ArrayList<>(newFiles.size());
    for (Path newFile : newFiles) {
      assert newFile != null;
      HStoreFile sf = moveFileIntoPlace(newFile);
      if (this.getCoprocessorHost() != null) {
        getCoprocessorHost().postCompact(this, sf, cr.getTracker(), cr, user);
      }
      assert sf != null;
      sfs.add(sf);
    }
    return sfs;
  }

  // Package-visible for tests
  HStoreFile moveFileIntoPlace(Path newFile) throws IOException {
    validateStoreFile(newFile);
    // Move the file into the right spot
    Path destPath = getRegionFileSystem().commitStoreFile(getColumnFamilyName(), newFile);
    return createStoreFileAndReader(destPath);
  }

  /**
   * Writes the compaction WAL record.
   * @param filesCompacted Files compacted (input).
   * @param newFiles Files from compaction.
   */
  private void writeCompactionWalRecord(Collection<HStoreFile> filesCompacted,
      Collection<HStoreFile> newFiles) throws IOException {
    if (region.getWAL() == null) {
      return;
    }
    List<Path> inputPaths =
        filesCompacted.stream().map(HStoreFile::getPath).collect(Collectors.toList());
    List<Path> outputPaths =
        newFiles.stream().map(HStoreFile::getPath).collect(Collectors.toList());
    RegionInfo info = this.region.getRegionInfo();
    CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(info,
        getColumnFamilyDescriptor().getName(), inputPaths, outputPaths,
        getRegionFileSystem().getStoreDir(getColumnFamilyDescriptor().getNameAsString()));
    // Fix reaching into Region to get the maxWaitForSeqId.
    // Does this method belong in Region altogether given it is making so many references up there?
    // Could be Region#writeCompactionMarker(compactionDescriptor);
    WALUtil.writeCompactionMarker(this.region.getWAL(), this.region.getReplicationScope(),
        this.region.getRegionInfo(), compactionDescriptor, this.region.getMVCC());
  }

  void replaceStoreFiles(Collection<HStoreFile> compactedFiles, Collection<HStoreFile> result)
      throws IOException {
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().addCompactionResults(compactedFiles, result);
      synchronized (filesCompacting) {
        filesCompacting.removeAll(compactedFiles);
      }

      // These may be null when the RS is shutting down. The space quota Chores will fix the Region
      // sizes later so it's not super-critical if we miss these.
      RegionServerServices rsServices = region.getRegionServerServices();
      if (rsServices != null && rsServices.getRegionServerSpaceQuotaManager() != null) {
        updateSpaceQuotaAfterFileReplacement(
            rsServices.getRegionServerSpaceQuotaManager().getRegionSizeStore(), getRegionInfo(),
            compactedFiles, result);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Updates the space quota usage for this region, removing the size for files compacted away
   * and adding in the size for new files.
   *
   * @param sizeStore The object tracking changes in region size for space quotas.
   * @param regionInfo The identifier for the region whose size is being updated.
   * @param oldFiles Files removed from this store's region.
   * @param newFiles Files added to this store's region.
   */
  void updateSpaceQuotaAfterFileReplacement(
      RegionSizeStore sizeStore, RegionInfo regionInfo, Collection<HStoreFile> oldFiles,
      Collection<HStoreFile> newFiles) {
    long delta = 0;
    if (oldFiles != null) {
      for (HStoreFile compactedFile : oldFiles) {
        if (compactedFile.isHFile()) {
          delta -= compactedFile.getReader().length();
        }
      }
    }
    if (newFiles != null) {
      for (HStoreFile newFile : newFiles) {
        if (newFile.isHFile()) {
          delta += newFile.getReader().length();
        }
      }
    }
    sizeStore.incrementRegionSize(regionInfo, delta);
  }

  /**
   * Log a very elaborate compaction completion message.
   * @param cr Request.
   * @param sfs Resulting files.
   * @param compactionStartTime Start time.
   */
  private void logCompactionEndMessage(
      CompactionRequestImpl cr, List<HStoreFile> sfs, long now, long compactionStartTime) {
    StringBuilder message = new StringBuilder(
      "Completed" + (cr.isMajor() ? " major" : "") + " compaction of "
      + cr.getFiles().size() + (cr.isAllFiles() ? " (all)" : "") + " file(s) in "
      + this + " of " + this.getRegionInfo().getShortNameToLog() + " into ");
    if (sfs.isEmpty()) {
      message.append("none, ");
    } else {
      for (HStoreFile sf: sfs) {
        message.append(sf.getPath().getName());
        message.append("(size=");
        message.append(TraditionalBinaryPrefix.long2String(sf.getReader().length(), "", 1));
        message.append("), ");
      }
    }
    message.append("total size for store is ")
      .append(StringUtils.TraditionalBinaryPrefix.long2String(storeSize.get(), "", 1))
      .append(". This selection was in queue for ")
      .append(StringUtils.formatTimeDiff(compactionStartTime, cr.getSelectionTime()))
      .append(", and took ").append(StringUtils.formatTimeDiff(now, compactionStartTime))
      .append(" to execute.");
    LOG.info(message.toString());
    if (LOG.isTraceEnabled()) {
      int fileCount = storeEngine.getStoreFileManager().getStorefileCount();
      long resultSize = getTotalSize(sfs);
      String traceMessage = "COMPACTION start,end,size out,files in,files out,store size,"
        + "store files [" + compactionStartTime + "," + now + "," + resultSize + ","
          + cr.getFiles().size() + "," + sfs.size() + "," +  storeSize + "," + fileCount + "]";
      LOG.trace(traceMessage);
    }
  }

  /**
   * Call to complete a compaction. Its for the case where we find in the WAL a compaction
   * that was not finished.  We could find one recovering a WAL after a regionserver crash.
   * See HBASE-2231.
   */
  public void replayCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,
      boolean removeFiles) throws IOException {
    LOG.debug("Completing compaction from the WAL marker");
    List<String> compactionInputs = compaction.getCompactionInputList();
    List<String> compactionOutputs = Lists.newArrayList(compaction.getCompactionOutputList());

    // The Compaction Marker is written after the compaction is completed,
    // and the files moved into the region/family folder.
    //
    // If we crash after the entry is written, we may not have removed the
    // input files, but the output file is present.
    // (The unremoved input files will be removed by this function)
    //
    // If we scan the directory and the file is not present, it can mean that:
    //   - The file was manually removed by the user
    //   - The file was removed as consequence of subsequent compaction
    // so, we can't do anything with the "compaction output list" because those
    // files have already been loaded when opening the region (by virtue of
    // being in the store's folder) or they may be missing due to a compaction.

    String familyName = this.getColumnFamilyName();
    Set<String> inputFiles = new HashSet<>();
    for (String compactionInput : compactionInputs) {
      Path inputPath = getRegionFileSystem().getStoreFilePath(familyName, compactionInput);
      inputFiles.add(inputPath.getName());
    }

    //some of the input files might already be deleted
    List<HStoreFile> inputStoreFiles = new ArrayList<>(compactionInputs.size());
    for (HStoreFile sf : this.getStorefiles()) {
      if (inputFiles.contains(sf.getPath().getName())) {
        inputStoreFiles.add(sf);
      }
    }

    // check whether we need to pick up the new files
    List<HStoreFile> outputStoreFiles = new ArrayList<>(compactionOutputs.size());

    if (pickCompactionFiles) {
      for (HStoreFile sf : this.getStorefiles()) {
        compactionOutputs.remove(sf.getPath().getName());
      }
      for (String compactionOutput : compactionOutputs) {
        StoreFileInfo storeFileInfo =
            getRegionFileSystem().getStoreFileInfo(getColumnFamilyName(), compactionOutput);
        HStoreFile storeFile = createStoreFileAndReader(storeFileInfo);
        outputStoreFiles.add(storeFile);
      }
    }

    if (!inputStoreFiles.isEmpty() || !outputStoreFiles.isEmpty()) {
      LOG.info("Replaying compaction marker, replacing input files: " +
          inputStoreFiles + " with output files : " + outputStoreFiles);
      this.replaceStoreFiles(inputStoreFiles, outputStoreFiles);
      this.completeCompaction(inputStoreFiles);
    }
  }

  /**
   * This method tries to compact N recent files for testing.
   * Note that because compacting "recent" files only makes sense for some policies,
   * e.g. the default one, it assumes default policy is used. It doesn't use policy,
   * but instead makes a compaction candidate list by itself.
   * @param N Number of files.
   */
  public void compactRecentForTestingAssumingDefaultPolicy(int N) throws IOException {
    List<HStoreFile> filesToCompact;
    boolean isMajor;

    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        filesToCompact = Lists.newArrayList(storeEngine.getStoreFileManager().getStorefiles());
        if (!filesCompacting.isEmpty()) {
          // exclude all files older than the newest file we're currently
          // compacting. this allows us to preserve contiguity (HBASE-2856)
          HStoreFile last = filesCompacting.get(filesCompacting.size() - 1);
          int idx = filesToCompact.indexOf(last);
          Preconditions.checkArgument(idx != -1);
          filesToCompact.subList(0, idx + 1).clear();
        }
        int count = filesToCompact.size();
        if (N > count) {
          throw new RuntimeException("Not enough files");
        }

        filesToCompact = filesToCompact.subList(count - N, count);
        isMajor = (filesToCompact.size() == storeEngine.getStoreFileManager().getStorefileCount());
        filesCompacting.addAll(filesToCompact);
        Collections.sort(filesCompacting, storeEngine.getStoreFileManager()
            .getStoreFileComparator());
      }
    } finally {
      this.lock.readLock().unlock();
    }

    try {
      // Ready to go. Have list of files to compact.
      List<Path> newFiles = ((DefaultCompactor)this.storeEngine.getCompactor())
          .compactForTesting(filesToCompact, isMajor);
      for (Path newFile: newFiles) {
        // Move the compaction into place.
        HStoreFile sf = moveFileIntoPlace(newFile);
        if (this.getCoprocessorHost() != null) {
          this.getCoprocessorHost().postCompact(this, sf, null, null, null);
        }
        replaceStoreFiles(filesToCompact, Collections.singletonList(sf));
        completeCompaction(filesToCompact);
      }
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
    }
  }

  @Override
  public boolean hasReferences() {
    // Grab the read lock here, because we need to ensure that: only when the atomic
    // replaceStoreFiles(..) finished, we can get all the complete store file list.
    this.lock.readLock().lock();
    try {
      // Merge the current store files with compacted files here due to HBASE-20940.
      Collection<HStoreFile> allStoreFiles = new ArrayList<>(getStorefiles());
      allStoreFiles.addAll(getCompactedFiles());
      return StoreUtils.hasReferences(allStoreFiles);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * getter for CompactionProgress object
   * @return CompactionProgress object; can be null
   */
  public CompactionProgress getCompactionProgress() {
    return this.storeEngine.getCompactor().getProgress();
  }

  @Override
  public boolean shouldPerformMajorCompaction() throws IOException {
    for (HStoreFile sf : this.storeEngine.getStoreFileManager().getStorefiles()) {
      // TODO: what are these reader checks all over the place?
      if (sf.getReader() == null) {
        LOG.debug("StoreFile {} has null Reader", sf);
        return false;
      }
    }
    return storeEngine.getCompactionPolicy().shouldPerformMajorCompaction(
        this.storeEngine.getStoreFileManager().getStorefiles());
  }

  public Optional<CompactionContext> requestCompaction() throws IOException {
    return requestCompaction(NO_PRIORITY, CompactionLifeCycleTracker.DUMMY, null);
  }

  public Optional<CompactionContext> requestCompaction(int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    // don't even select for compaction if writes are disabled
    if (!this.areWritesEnabled()) {
      return Optional.empty();
    }
    // Before we do compaction, try to get rid of unneeded files to simplify things.
    removeUnneededFiles();

    final CompactionContext compaction = storeEngine.createCompaction();
    CompactionRequestImpl request = null;
    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        // First, see if coprocessor would want to override selection.
        if (this.getCoprocessorHost() != null) {
          final List<HStoreFile> candidatesForCoproc = compaction.preSelect(this.filesCompacting);
          boolean override = getCoprocessorHost().preCompactSelection(this,
              candidatesForCoproc, tracker, user);
          if (override) {
            // Coprocessor is overriding normal file selection.
            compaction.forceSelect(new CompactionRequestImpl(candidatesForCoproc));
          }
        }

        // Normal case - coprocessor is not overriding file selection.
        if (!compaction.hasSelection()) {
          boolean isUserCompaction = priority == Store.PRIORITY_USER;
          boolean mayUseOffPeak = offPeakHours.isOffPeakHour() &&
              offPeakCompactionTracker.compareAndSet(false, true);
          try {
            compaction.select(this.filesCompacting, isUserCompaction,
              mayUseOffPeak, forceMajor && filesCompacting.isEmpty());
          } catch (IOException e) {
            if (mayUseOffPeak) {
              offPeakCompactionTracker.set(false);
            }
            throw e;
          }
          assert compaction.hasSelection();
          if (mayUseOffPeak && !compaction.getRequest().isOffPeak()) {
            // Compaction policy doesn't want to take advantage of off-peak.
            offPeakCompactionTracker.set(false);
          }
        }
        if (this.getCoprocessorHost() != null) {
          this.getCoprocessorHost().postCompactSelection(
              this, ImmutableList.copyOf(compaction.getRequest().getFiles()), tracker,
              compaction.getRequest(), user);
        }
        // Finally, we have the resulting files list. Check if we have any files at all.
        request = compaction.getRequest();
        Collection<HStoreFile> selectedFiles = request.getFiles();
        if (selectedFiles.isEmpty()) {
          return Optional.empty();
        }

        addToCompactingFiles(selectedFiles);

        // If we're enqueuing a major, clear the force flag.
        this.forceMajor = this.forceMajor && !request.isMajor();

        // Set common request properties.
        // Set priority, either override value supplied by caller or from store.
        final int compactionPriority =
          (priority != Store.NO_PRIORITY) ? priority : getCompactPriority();
        request.setPriority(compactionPriority);

        if (request.isAfterSplit()) {
          // If the store belongs to recently splitted daughter regions, better we consider
          // them with the higher priority in the compaction queue.
          // Override priority if it is lower (higher int value) than
          // SPLIT_REGION_COMPACTION_PRIORITY
          final int splitHousekeepingPriority =
            Math.min(compactionPriority, SPLIT_REGION_COMPACTION_PRIORITY);
          request.setPriority(splitHousekeepingPriority);
          LOG.info("Keeping/Overriding Compaction request priority to {} for CF {} since it"
              + " belongs to recently split daughter region {}", splitHousekeepingPriority,
            this.getColumnFamilyName(), getRegionInfo().getRegionNameAsString());
        }
        request.setDescription(getRegionInfo().getRegionNameAsString(), getColumnFamilyName());
        request.setTracker(tracker);
      }
    } finally {
      this.lock.readLock().unlock();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(this + " is initiating " + (request.isMajor() ? "major" : "minor") + " compaction"
          + (request.isAllFiles() ? " (all files)" : ""));
    }
    this.region.reportCompactionRequestStart(request.isMajor());
    return Optional.of(compaction);
  }

  /** Adds the files to compacting files. filesCompacting must be locked. */
  private void addToCompactingFiles(Collection<HStoreFile> filesToAdd) {
    if (CollectionUtils.isEmpty(filesToAdd)) {
      return;
    }
    // Check that we do not try to compact the same StoreFile twice.
    if (!Collections.disjoint(filesCompacting, filesToAdd)) {
      Preconditions.checkArgument(false, "%s overlaps with %s", filesToAdd, filesCompacting);
    }
    filesCompacting.addAll(filesToAdd);
    Collections.sort(filesCompacting, storeEngine.getStoreFileManager().getStoreFileComparator());
  }

  private void removeUnneededFiles() throws IOException {
    if (!conf.getBoolean("hbase.store.delete.expired.storefile", true)) {
      return;
    }
    if (getColumnFamilyDescriptor().getMinVersions() > 0) {
      LOG.debug("Skipping expired store file removal due to min version of {} being {}",
          this, getColumnFamilyDescriptor().getMinVersions());
      return;
    }
    this.lock.readLock().lock();
    Collection<HStoreFile> delSfs = null;
    try {
      synchronized (filesCompacting) {
        long cfTtl = getStoreFileTtl();
        if (cfTtl != Long.MAX_VALUE) {
          delSfs = storeEngine.getStoreFileManager().getUnneededFiles(
              EnvironmentEdgeManager.currentTime() - cfTtl, filesCompacting);
          addToCompactingFiles(delSfs);
        }
      }
    } finally {
      this.lock.readLock().unlock();
    }

    if (CollectionUtils.isEmpty(delSfs)) {
      return;
    }

    Collection<HStoreFile> newFiles = Collections.emptyList(); // No new files.
    writeCompactionWalRecord(delSfs, newFiles);
    replaceStoreFiles(delSfs, newFiles);
    completeCompaction(delSfs);
    LOG.info("Completed removal of " + delSfs.size() + " unnecessary (expired) file(s) in "
        + this + "; total size is "
        + TraditionalBinaryPrefix.long2String(storeSize.get(), "", 1));
  }

  public void cancelRequestedCompaction(CompactionContext compaction) {
    finishCompactionRequest(compaction.getRequest());
  }

  private void finishCompactionRequest(CompactionRequestImpl cr) {
    this.region.reportCompactionRequestEnd(cr.isMajor(), cr.getFiles().size(), cr.getSize());
    if (cr.isOffPeak()) {
      offPeakCompactionTracker.set(false);
      cr.setOffPeak(false);
    }
    synchronized (filesCompacting) {
      filesCompacting.removeAll(cr.getFiles());
    }
  }

  /**
   * Validates a store file by opening and closing it. In HFileV2 this should not be an expensive
   * operation.
   * @param path the path to the store file
   */
  private void validateStoreFile(Path path) throws IOException {
    HStoreFile storeFile = null;
    try {
      storeFile = createStoreFileAndReader(path);
    } catch (IOException e) {
      LOG.error("Failed to open store file : {}, keeping it in tmp location", path, e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeStoreFile(false);
      }
    }
  }

  /**
   * Update counts.
   * @param compactedFiles list of files that were compacted
   */
  protected void completeCompaction(Collection<HStoreFile> compactedFiles)
  // Rename this method! TODO.
    throws IOException {
    this.storeSize.set(0L);
    this.totalUncompressedBytes.set(0L);
    for (HStoreFile hsf : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFileReader r = hsf.getReader();
      if (r == null) {
        LOG.warn("StoreFile {} has a null Reader", hsf);
        continue;
      }
      this.storeSize.addAndGet(r.length());
      this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());
    }
  }

  /*
   * @param wantedVersions How many versions were asked for.
   * @return wantedVersions or this families' {@link HConstants#VERSIONS}.
   */
  int versionsToReturn(final int wantedVersions) {
    if (wantedVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    // Make sure we do not return more than maximum versions for this store.
    int maxVersions = getColumnFamilyDescriptor().getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions: wantedVersions;
  }

  @Override
  public boolean canSplit() {
    // Not split-able if we find a reference store file present in the store.
    boolean result = !hasReferences();
    if (!result) {
      LOG.trace("Not splittable; has references: {}", this);
    }
    return result;
  }

  /**
   * Determines if Store should be split.
   */
  public Optional<byte[]> getSplitPoint() {
    this.lock.readLock().lock();
    try {
      // Should already be enforced by the split policy!
      assert !this.getRegionInfo().isMetaRegion();
      // Not split-able if we find a reference store file present in the store.
      if (hasReferences()) {
        LOG.trace("Not splittable; has references: {}", this);
        return Optional.empty();
      }
      return this.storeEngine.getStoreFileManager().getSplitPoint();
    } catch(IOException e) {
      LOG.warn("Failed getting store size for {}", this, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return Optional.empty();
  }

  @Override
  public long getLastCompactSize() {
    return this.lastCompactSize;
  }

  @Override
  public long getSize() {
    return storeSize.get();
  }

  public void triggerMajorCompaction() {
    this.forceMajor = true;
  }

  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memstore and the HStore files. Assumes we are not in a
   * compaction.
   * @param scan Scan to apply when scanning the stores
   * @param targetCols columns to scan
   * @return a scanner over the current key values
   * @throws IOException on failure
   */
  public KeyValueScanner getScanner(Scan scan, final NavigableSet<byte[]> targetCols, long readPt)
      throws IOException {
    lock.readLock().lock();
    try {
      ScanInfo scanInfo;
      if (this.getCoprocessorHost() != null) {
        scanInfo = this.getCoprocessorHost().preStoreScannerOpen(this, scan);
      } else {
        scanInfo = getScanInfo();
      }
      return createScanner(scan, scanInfo, targetCols, readPt);
    } finally {
      lock.readLock().unlock();
    }
  }

  // HMobStore will override this method to return its own implementation.
  protected KeyValueScanner createScanner(Scan scan, ScanInfo scanInfo,
      NavigableSet<byte[]> targetCols, long readPt) throws IOException {
    return scan.isReversed() ? new ReversedStoreScanner(this, scanInfo, scan, targetCols, readPt)
        : new StoreScanner(this, scanInfo, scan, targetCols, readPt);
  }

  /**
   * Recreates the scanners on the current list of active store file scanners
   * @param currentFileScanners the current set of active store file scanners
   * @param cacheBlocks cache the blocks or not
   * @param usePread use pread or not
   * @param isCompaction is the scanner for compaction
   * @param matcher the scan query matcher
   * @param startRow the scan's start row
   * @param includeStartRow should the scan include the start row
   * @param stopRow the scan's stop row
   * @param includeStopRow should the scan include the stop row
   * @param readPt the read point of the current scane
   * @param includeMemstoreScanner whether the current scanner should include memstorescanner
   * @return list of scanners recreated on the current Scanners
   */
  public List<KeyValueScanner> recreateScanners(List<KeyValueScanner> currentFileScanners,
      boolean cacheBlocks, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
      byte[] startRow, boolean includeStartRow, byte[] stopRow, boolean includeStopRow, long readPt,
      boolean includeMemstoreScanner) throws IOException {
    this.lock.readLock().lock();
    try {
      Map<String, HStoreFile> name2File =
          new HashMap<>(getStorefilesCount() + getCompactedFilesCount());
      for (HStoreFile file : getStorefiles()) {
        name2File.put(file.getFileInfo().getActiveFileName(), file);
      }
      Collection<HStoreFile> compactedFiles = getCompactedFiles();
      for (HStoreFile file : IterableUtils.emptyIfNull(compactedFiles)) {
        name2File.put(file.getFileInfo().getActiveFileName(), file);
      }
      List<HStoreFile> filesToReopen = new ArrayList<>();
      for (KeyValueScanner kvs : currentFileScanners) {
        assert kvs.isFileScanner();
        if (kvs.peek() == null) {
          continue;
        }
        filesToReopen.add(name2File.get(kvs.getFilePath().getName()));
      }
      if (filesToReopen.isEmpty()) {
        return null;
      }
      return getScanners(filesToReopen, cacheBlocks, false, false, matcher, startRow,
        includeStartRow, stopRow, includeStopRow, readPt, false);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return this.getRegionInfo().getShortNameToLog()+ "/" + this.getColumnFamilyName();
  }

  @Override
  public int getStorefilesCount() {
    return this.storeEngine.getStoreFileManager().getStorefileCount();
  }

  @Override
  public int getCompactedFilesCount() {
    return this.storeEngine.getStoreFileManager().getCompactedFilesCount();
  }

  private LongStream getStoreFileAgeStream() {
    return this.storeEngine.getStoreFileManager().getStorefiles().stream().filter(sf -> {
      if (sf.getReader() == null) {
        LOG.warn("StoreFile {} has a null Reader", sf);
        return false;
      } else {
        return true;
      }
    }).filter(HStoreFile::isHFile).mapToLong(sf -> sf.getFileInfo().getCreatedTimestamp())
        .map(t -> EnvironmentEdgeManager.currentTime() - t);
  }

  @Override
  public OptionalLong getMaxStoreFileAge() {
    return getStoreFileAgeStream().max();
  }

  @Override
  public OptionalLong getMinStoreFileAge() {
    return getStoreFileAgeStream().min();
  }

  @Override
  public OptionalDouble getAvgStoreFileAge() {
    return getStoreFileAgeStream().average();
  }

  @Override
  public long getNumReferenceFiles() {
    return this.storeEngine.getStoreFileManager().getStorefiles().stream()
        .filter(HStoreFile::isReference).count();
  }

  @Override
  public long getNumHFiles() {
    return this.storeEngine.getStoreFileManager().getStorefiles().stream()
        .filter(HStoreFile::isHFile).count();
  }

  @Override
  public long getStoreSizeUncompressed() {
    return this.totalUncompressedBytes.get();
  }

  @Override
  public long getStorefilesSize() {
    // Include all StoreFiles
    return getStorefilesSize(this.storeEngine.getStoreFileManager().getStorefiles(), sf -> true);
  }

  @Override
  public long getHFilesSize() {
    // Include only StoreFiles which are HFiles
    return getStorefilesSize(this.storeEngine.getStoreFileManager().getStorefiles(),
      HStoreFile::isHFile);
  }

  private long getTotalUncompressedBytes(List<HStoreFile> files) {
    return files.stream()
      .mapToLong(file -> getStorefileFieldSize(file, StoreFileReader::getTotalUncompressedBytes))
      .sum();
  }

  private long getStorefilesSize(Collection<HStoreFile> files, Predicate<HStoreFile> predicate) {
    return files.stream().filter(predicate)
      .mapToLong(file -> getStorefileFieldSize(file, StoreFileReader::length)).sum();
  }

  private long getStorefileFieldSize(HStoreFile file, ToLongFunction<StoreFileReader> f) {
    if (file == null) {
      return 0L;
    }
    StoreFileReader reader = file.getReader();
    if (reader == null) {
      return 0L;
    }
    return f.applyAsLong(reader);
  }

  private long getStorefilesFieldSize(ToLongFunction<StoreFileReader> f) {
    return this.storeEngine.getStoreFileManager().getStorefiles().stream()
      .mapToLong(file -> getStorefileFieldSize(file, f)).sum();
  }

  @Override
  public long getStorefilesRootLevelIndexSize() {
    return getStorefilesFieldSize(StoreFileReader::indexSize);
  }

  @Override
  public long getTotalStaticIndexSize() {
    return getStorefilesFieldSize(StoreFileReader::getUncompressedDataIndexSize);
  }

  @Override
  public long getTotalStaticBloomSize() {
    return getStorefilesFieldSize(StoreFileReader::getTotalBloomSize);
  }

  @Override
  public MemStoreSize getMemStoreSize() {
    return this.memstore.size();
  }

  @Override
  public int getCompactPriority() {
    int priority = this.storeEngine.getStoreFileManager().getStoreCompactionPriority();
    if (priority == PRIORITY_USER) {
      LOG.warn("Compaction priority is USER despite there being no user compaction");
    }
    return priority;
  }

  public boolean throttleCompaction(long compactionSize) {
    return storeEngine.getCompactionPolicy().throttleCompaction(compactionSize);
  }

  public HRegion getHRegion() {
    return this.region;
  }

  public RegionCoprocessorHost getCoprocessorHost() {
    return this.region.getCoprocessorHost();
  }

  @Override
  public RegionInfo getRegionInfo() {
    return getRegionFileSystem().getRegionInfo();
  }

  @Override
  public boolean areWritesEnabled() {
    return this.region.areWritesEnabled();
  }

  @Override
  public long getSmallestReadPoint() {
    return this.region.getSmallestReadPoint();
  }

  /**
   * Adds or replaces the specified KeyValues.
   * <p>
   * For each KeyValue specified, if a cell with the same row, family, and qualifier exists in
   * MemStore, it will be replaced. Otherwise, it will just be inserted to MemStore.
   * <p>
   * This operation is atomic on each KeyValue (row/family/qualifier) but not necessarily atomic
   * across all of them.
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   */
  public void upsert(Iterable<Cell> cells, long readpoint, MemStoreSizing memstoreSizing)
      throws IOException {
    this.lock.readLock().lock();
    try {
      this.memstore.upsert(cells, readpoint, memstoreSizing);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public StoreFlushContext createFlushContext(long cacheFlushId, FlushLifeCycleTracker tracker) {
    return new StoreFlusherImpl(cacheFlushId, tracker);
  }

  private final class StoreFlusherImpl implements StoreFlushContext {

    private final FlushLifeCycleTracker tracker;
    private final long cacheFlushSeqNum;
    private MemStoreSnapshot snapshot;
    private List<Path> tempFiles;
    private List<Path> committedFiles;
    private long cacheFlushCount;
    private long cacheFlushSize;
    private long outputFileSize;

    private StoreFlusherImpl(long cacheFlushSeqNum, FlushLifeCycleTracker tracker) {
      this.cacheFlushSeqNum = cacheFlushSeqNum;
      this.tracker = tracker;
    }

    /**
     * This is not thread safe. The caller should have a lock on the region or the store.
     * If necessary, the lock can be added with the patch provided in HBASE-10087
     */
    @Override
    public MemStoreSize prepare() {
      // passing the current sequence number of the wal - to allow bookkeeping in the memstore
      this.snapshot = memstore.snapshot();
      this.cacheFlushCount = snapshot.getCellsCount();
      this.cacheFlushSize = snapshot.getDataSize();
      committedFiles = new ArrayList<>(1);
      return snapshot.getMemStoreSize();
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      RegionServerServices rsService = region.getRegionServerServices();
      ThroughputController throughputController =
          rsService == null ? null : rsService.getFlushThroughputController();
      tempFiles =
          HStore.this.flushCache(cacheFlushSeqNum, snapshot, status, throughputController, tracker);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      if (CollectionUtils.isEmpty(this.tempFiles)) {
        return false;
      }
      List<HStoreFile> storeFiles = new ArrayList<>(this.tempFiles.size());
      for (Path storeFilePath : tempFiles) {
        try {
          HStoreFile sf = HStore.this.commitFile(storeFilePath, cacheFlushSeqNum, status);
          outputFileSize += sf.getReader().length();
          storeFiles.add(sf);
        } catch (IOException ex) {
          LOG.error("Failed to commit store file {}", storeFilePath, ex);
          // Try to delete the files we have committed before.
          for (HStoreFile sf : storeFiles) {
            Path pathToDelete = sf.getPath();
            try {
              sf.deleteStoreFile();
            } catch (IOException deleteEx) {
              LOG.error(HBaseMarkers.FATAL, "Failed to delete store file we committed, "
                  + "halting {}", pathToDelete, ex);
              Runtime.getRuntime().halt(1);
            }
          }
          throw new IOException("Failed to commit the flush", ex);
        }
      }

      for (HStoreFile sf : storeFiles) {
        if (HStore.this.getCoprocessorHost() != null) {
          HStore.this.getCoprocessorHost().postFlush(HStore.this, sf, tracker);
        }
        committedFiles.add(sf.getPath());
      }

      HStore.this.flushedCellsCount.addAndGet(cacheFlushCount);
      HStore.this.flushedCellsSize.addAndGet(cacheFlushSize);
      HStore.this.flushedOutputFileSize.addAndGet(outputFileSize);

      // Add new file to store files.  Clear snapshot too while we have the Store write lock.
      return HStore.this.updateStorefiles(storeFiles, snapshot.getId());
    }

    @Override
    public long getOutputFileSize() {
      return outputFileSize;
    }

    @Override
    public List<Path> getCommittedFiles() {
      return committedFiles;
    }

    /**
     * Similar to commit, but called in secondary region replicas for replaying the
     * flush cache from primary region. Adds the new files to the store, and drops the
     * snapshot depending on dropMemstoreSnapshot argument.
     * @param fileNames names of the flushed files
     * @param dropMemstoreSnapshot whether to drop the prepared memstore snapshot
     */
    @Override
    public void replayFlush(List<String> fileNames, boolean dropMemstoreSnapshot)
        throws IOException {
      List<HStoreFile> storeFiles = new ArrayList<>(fileNames.size());
      for (String file : fileNames) {
        // open the file as a store file (hfile link, etc)
        StoreFileInfo storeFileInfo =
          getRegionFileSystem().getStoreFileInfo(getColumnFamilyName(), file);
        HStoreFile storeFile = createStoreFileAndReader(storeFileInfo);
        storeFiles.add(storeFile);
        HStore.this.storeSize.addAndGet(storeFile.getReader().length());
        HStore.this.totalUncompressedBytes
            .addAndGet(storeFile.getReader().getTotalUncompressedBytes());
        if (LOG.isInfoEnabled()) {
          LOG.info(this + " added " + storeFile + ", entries=" + storeFile.getReader().getEntries() +
              ", sequenceid=" + storeFile.getReader().getSequenceID() + ", filesize="
              + TraditionalBinaryPrefix.long2String(storeFile.getReader().length(), "", 1));
        }
      }

      long snapshotId = -1; // -1 means do not drop
      if (dropMemstoreSnapshot && snapshot != null) {
        snapshotId = snapshot.getId();
      }
      HStore.this.updateStorefiles(storeFiles, snapshotId);
    }

    /**
     * Abort the snapshot preparation. Drops the snapshot if any.
     */
    @Override
    public void abort() throws IOException {
      if (snapshot != null) {
        HStore.this.updateStorefiles(Collections.emptyList(), snapshot.getId());
      }
    }
  }

  @Override
  public boolean needsCompaction() {
    List<HStoreFile> filesCompactingClone = null;
    synchronized (filesCompacting) {
      filesCompactingClone = Lists.newArrayList(filesCompacting);
    }
    return this.storeEngine.needsCompaction(filesCompactingClone);
  }

  /**
   * Used for tests.
   * @return cache configuration for this Store.
   */
  public CacheConfig getCacheConfig() {
    return storeContext.getCacheConf();
  }

  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HStore.class, false);

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD
      + ClassSize.OBJECT + ClassSize.REENTRANT_LOCK
      + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT
      + ScanInfo.FIXED_OVERHEAD);

  @Override
  public long heapSize() {
    MemStoreSize memstoreSize = this.memstore.size();
    return DEEP_OVERHEAD + memstoreSize.getHeapSize() + storeContext.heapSize();
  }

  @Override
  public CellComparator getComparator() {
    return storeContext.getComparator();
  }

  public ScanInfo getScanInfo() {
    return scanInfo;
  }

  /**
   * Set scan info, used by test
   * @param scanInfo new scan info to use for test
   */
  void setScanInfo(ScanInfo scanInfo) {
    this.scanInfo = scanInfo;
  }

  @Override
  public boolean hasTooManyStoreFiles() {
    return getStorefilesCount() > this.blockingFileCount;
  }

  @Override
  public long getFlushedCellsCount() {
    return flushedCellsCount.get();
  }

  @Override
  public long getFlushedCellsSize() {
    return flushedCellsSize.get();
  }

  @Override
  public long getFlushedOutputFileSize() {
    return flushedOutputFileSize.get();
  }

  @Override
  public long getCompactedCellsCount() {
    return compactedCellsCount.get();
  }

  @Override
  public long getCompactedCellsSize() {
    return compactedCellsSize.get();
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return majorCompactedCellsCount.get();
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return majorCompactedCellsSize.get();
  }

  /**
   * Returns the StoreEngine that is backing this concrete implementation of Store.
   * @return Returns the {@link StoreEngine} object used internally inside this HStore object.
   */
  public StoreEngine<?, ?, ?, ?> getStoreEngine() {
    return this.storeEngine;
  }

  protected OffPeakHours getOffPeakHours() {
    return this.offPeakHours;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    Configuration storeConf = StoreUtils.createStoreConfiguration(conf, region.getTableDescriptor(),
      getColumnFamilyDescriptor());
    this.conf = storeConf;
    this.storeEngine.compactionPolicy.setConf(storeConf);
    this.offPeakHours = OffPeakHours.getInstance(storeConf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    // No children to register
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    // No children to deregister
  }

  @Override
  public double getCompactionPressure() {
    return storeEngine.getStoreFileManager().getCompactionPressure();
  }

  @Override
  public boolean isPrimaryReplicaStore() {
    return getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID;
  }

  /**
   * Sets the store up for a region level snapshot operation.
   * @see #postSnapshotOperation()
   */
  public void preSnapshotOperation() {
    archiveLock.lock();
  }

  /**
   * Perform tasks needed after the completion of snapshot operation.
   * @see #preSnapshotOperation()
   */
  public void postSnapshotOperation() {
    archiveLock.unlock();
  }

  /**
   * Closes and archives the compacted files under this store
   */
  public synchronized void closeAndArchiveCompactedFiles() throws IOException {
    // ensure other threads do not attempt to archive the same files on close()
    archiveLock.lock();
    try {
      lock.readLock().lock();
      Collection<HStoreFile> copyCompactedfiles = null;
      try {
        Collection<HStoreFile> compactedfiles =
            this.getStoreEngine().getStoreFileManager().getCompactedfiles();
        if (CollectionUtils.isNotEmpty(compactedfiles)) {
          // Do a copy under read lock
          copyCompactedfiles = new ArrayList<>(compactedfiles);
        } else {
          LOG.trace("No compacted files to archive");
        }
      } finally {
        lock.readLock().unlock();
      }
      if (CollectionUtils.isNotEmpty(copyCompactedfiles)) {
        removeCompactedfiles(copyCompactedfiles, true);
      }
    } finally {
      archiveLock.unlock();
    }
  }

  /**
   * Archives and removes the compacted files
   * @param compactedfiles The compacted files in this store that are not active in reads
   * @param evictOnClose true if blocks should be evicted from the cache when an HFile reader is
   *   closed, false if not
   */
  private void removeCompactedfiles(Collection<HStoreFile> compactedfiles, boolean evictOnClose)
      throws IOException {
    final List<HStoreFile> filesToRemove = new ArrayList<>(compactedfiles.size());
    final List<Long> storeFileSizes = new ArrayList<>(compactedfiles.size());
    for (final HStoreFile file : compactedfiles) {
      synchronized (file) {
        try {
          StoreFileReader r = file.getReader();
          if (r == null) {
            LOG.debug("The file {} was closed but still not archived", file);
            // HACK: Temporarily re-open the reader so we can get the size of the file. Ideally,
            // we should know the size of an HStoreFile without having to ask the HStoreFileReader
            // for that.
            long length = getStoreFileSize(file);
            filesToRemove.add(file);
            storeFileSizes.add(length);
            continue;
          }

          if (file.isCompactedAway() && !file.isReferencedInReads()) {
            // Even if deleting fails we need not bother as any new scanners won't be
            // able to use the compacted file as the status is already compactedAway
            LOG.trace("Closing and archiving the file {}", file);
            // Copy the file size before closing the reader
            final long length = r.length();
            r.close(evictOnClose);
            // Just close and return
            filesToRemove.add(file);
            // Only add the length if we successfully added the file to `filesToRemove`
            storeFileSizes.add(length);
          } else {
            LOG.info("Can't archive compacted file " + file.getPath()
                + " because of either isCompactedAway=" + file.isCompactedAway()
                + " or file has reference, isReferencedInReads=" + file.isReferencedInReads()
                + ", refCount=" + r.getRefCount() + ", skipping for now.");
          }
        } catch (Exception e) {
          LOG.error("Exception while trying to close the compacted store file {}", file.getPath(),
              e);
        }
      }
    }
    if (this.isPrimaryReplicaStore()) {
      // Only the primary region is allowed to move the file to archive.
      // The secondary region does not move the files to archive. Any active reads from
      // the secondary region will still work because the file as such has active readers on it.
      if (!filesToRemove.isEmpty()) {
        LOG.debug("Moving the files {} to archive", filesToRemove);
        // Only if this is successful it has to be removed
        try {
          getRegionFileSystem()
            .removeStoreFiles(this.getColumnFamilyDescriptor().getNameAsString(), filesToRemove);
        } catch (FailedArchiveException fae) {
          // Even if archiving some files failed, we still need to clear out any of the
          // files which were successfully archived.  Otherwise we will receive a
          // FileNotFoundException when we attempt to re-archive them in the next go around.
          Collection<Path> failedFiles = fae.getFailedFiles();
          Iterator<HStoreFile> iter = filesToRemove.iterator();
          Iterator<Long> sizeIter = storeFileSizes.iterator();
          while (iter.hasNext()) {
            sizeIter.next();
            if (failedFiles.contains(iter.next().getPath())) {
              iter.remove();
              sizeIter.remove();
            }
          }
          if (!filesToRemove.isEmpty()) {
            clearCompactedfiles(filesToRemove);
          }
          throw fae;
        }
      }
    }
    if (!filesToRemove.isEmpty()) {
      // Clear the compactedfiles from the store file manager
      clearCompactedfiles(filesToRemove);
      // Try to send report of this archival to the Master for updating quota usage faster
      reportArchivedFilesForQuota(filesToRemove, storeFileSizes);
    }
  }

  /**
   * Computes the length of a store file without succumbing to any errors along the way. If an
   * error is encountered, the implementation returns {@code 0} instead of the actual size.
   *
   * @param file The file to compute the size of.
   * @return The size in bytes of the provided {@code file}.
   */
  long getStoreFileSize(HStoreFile file) {
    long length = 0;
    try {
      file.initReader();
      length = file.getReader().length();
    } catch (IOException e) {
      LOG.trace("Failed to open reader when trying to compute store file size for {}, ignoring",
        file, e);
    } finally {
      try {
        file.closeStoreFile(
            file.getCacheConf() != null ? file.getCacheConf().shouldEvictOnClose() : true);
      } catch (IOException e) {
        LOG.trace("Failed to close reader after computing store file size for {}, ignoring",
          file, e);
      }
    }
    return length;
  }

  public Long preFlushSeqIDEstimation() {
    return memstore.preFlushSeqIDEstimation();
  }

  @Override
  public boolean isSloppyMemStore() {
    return this.memstore.isSloppy();
  }

  private void clearCompactedfiles(List<HStoreFile> filesToRemove) throws IOException {
    LOG.trace("Clearing the compacted file {} from this store", filesToRemove);
    try {
      lock.writeLock().lock();
      this.getStoreEngine().getStoreFileManager().removeCompactedFiles(filesToRemove);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getCurrentParallelPutCount() {
    return currentParallelPutCount.get();
  }

  public int getStoreRefCount() {
    return this.storeEngine.getStoreFileManager().getStorefiles().stream()
      .filter(sf -> sf.getReader() != null).filter(HStoreFile::isHFile)
      .mapToInt(HStoreFile::getRefCount).sum();
  }

  /**
   * @return get maximum ref count of storeFile among all compacted HStore Files for the HStore
   */
  public int getMaxCompactedStoreFileRefCount() {
    OptionalInt maxCompactedStoreFileRefCount = this.storeEngine.getStoreFileManager()
      .getCompactedfiles()
      .stream()
      .filter(sf -> sf.getReader() != null)
      .filter(HStoreFile::isHFile)
      .mapToInt(HStoreFile::getRefCount)
      .max();
    return maxCompactedStoreFileRefCount.isPresent()
      ? maxCompactedStoreFileRefCount.getAsInt() : 0;
  }

  void reportArchivedFilesForQuota(List<? extends StoreFile> archivedFiles, List<Long> fileSizes) {
    // Sanity check from the caller
    if (archivedFiles.size() != fileSizes.size()) {
      throw new RuntimeException("Coding error: should never see lists of varying size");
    }
    RegionServerServices rss = this.region.getRegionServerServices();
    if (rss == null) {
      return;
    }
    List<Entry<String, Long>> filesWithSizes = new ArrayList<>(archivedFiles.size());
    Iterator<Long> fileSizeIter = fileSizes.iterator();
    for (StoreFile storeFile : archivedFiles) {
      final long fileSize = fileSizeIter.next();
      if (storeFile.isHFile() && fileSize != 0) {
        filesWithSizes.add(Maps.immutableEntry(storeFile.getPath().getName(), fileSize));
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Files archived: " + archivedFiles + ", reporting the following to the Master: "
          + filesWithSizes);
    }
    boolean success = rss.reportFileArchivalForQuotas(getTableName(), filesWithSizes);
    if (!success) {
      LOG.warn("Failed to report archival of files: " + filesWithSizes);
    }
  }
  @Override
  public long getMemstoreOnlyRowReadsCount() {
    return memstoreOnlyRowReadsCount.sum();
  }

  @Override
  public long getMixedRowReadsCount() {
    return mixedRowReadsCount.sum();
  }

  @Override
  public Configuration getReadOnlyConfiguration() {
    return new ReadOnlyConfiguration(this.conf);
  }

  void updateMetricsStore(boolean memstoreRead) {
    if (memstoreRead) {
      memstoreOnlyRowReadsCount.increment();
    } else {
      mixedRowReadsCount.increment();
    }
  }
}
