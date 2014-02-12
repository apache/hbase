/**
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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.UniformSplitHFileHistogram;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.compactionhook.CompactionHook;
import org.apache.hadoop.hbase.regionserver.kvaggregator.DefaultKeyValueAggregator;
import org.apache.hadoop.hbase.regionserver.kvaggregator.KeyValueAggregator;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
 * <p>The only thing having to do with logs that Store needs to deal with is
 * the reconstructionLog.  This is a segment of an HRegion's log that might
 * NOT be present upon startup.  If the param is NULL, there's nothing to do.
 * If the param is non-NULL, we need to process the log to reconstruct
 * a TreeMap that might not have been written to disk before the process
 * died.
 *
 * <p>It's assumed that after this constructor returns, the reconstructionLog
 * file will be deleted (by whoever has instantiated the Store).
 *
 * <p>Locking and transactions are handled at a higher level.  This API should
 * not be called directly but by an HRegion manager.
 */
public class Store extends SchemaConfigured implements HeapSize,
        ConfigurationObserver {
  static final Log LOG = LogFactory.getLog(Store.class);
  protected final MemStore memstore;
  // This stores directory in the filesystem.
  protected final Path homedir;
  private final HRegion region;
  private final HColumnDescriptor family;
  CompactionManager compactionManager;
  final FileSystem fs;
  Configuration conf;
  final CacheConfig cacheConf;
  // ttl in milliseconds.
  protected long ttl;
  // The amount of time in seconds in the past upto which we support FlashBack
  // queries. Ex. 60 * 60 * 24 indicates we support FlashBack queries upto 1 day
  // ago.
  public final long flashBackQueryLimit;
  private long timeToPurgeDeletes;
  private long lastCompactSize = 0;
  volatile boolean forceMajor = false;
  /* how many bytes to write between status checks */
  static int closeCheckInterval = 0;
  private final long desiredMaxFileSize;
  private final int blockingStoreFileCount;
  private volatile long storeSize = 0L;
  private final Object flushLock = new Object();
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final String storeNameStr;
  private int peakStartHour;
  private int peakEndHour;

  /*
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  protected ImmutableList<StoreFile> storefiles = null;

  List<StoreFile> filesCompacting = Lists.newArrayList();

  // All access must be synchronized.
  private final CopyOnWriteArraySet<ChangedReadersObserver> changedReaderObservers =
    new CopyOnWriteArraySet<ChangedReadersObserver>();

  private final int blocksize;
  private final boolean blockcache;
  private final Compression.Algorithm compression;
  private final Compression.Algorithm mcCompression;

  private HFileDataBlockEncoder dataBlockEncoder;

  // Comparing KeyValues
  final KeyValue.KVComparator comparator;

  private Class<KeyValueAggregator> aggregatorClass = null;
  private CompactionHook compactHook = null;

  private final HRegionInfo info;
  private boolean writeHFileHistogram = false;

  // The number of non static class variables.
  private static final int NUM_REFERENCES = 22;
  // Number of primitive non static fields in Store.
  private static final int NUM_LONG_CNT = 6;
  private static final int NUM_INT_CNT = 4;
  private static final int NUM_BOOLEAN_CNT = 2;
  // This should account for the Store's non static variables. So, when there
  // is an addition to the member variables to Store, this value should be
  // adjusted accordingly.
  public static final long FIXED_OVERHEAD =
      ClassSize.align(SchemaConfigured.SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE +
          + (NUM_REFERENCES * ClassSize.REFERENCE)
          + (NUM_LONG_CNT * Bytes.SIZEOF_LONG)
          + (NUM_INT_CNT * Bytes.SIZEOF_INT)
          + (NUM_BOOLEAN_CNT * Bytes.SIZEOF_BOOLEAN));

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.OBJECT + ClassSize.REENTRANT_LOCK +
      ClassSize.CONCURRENT_SKIPLISTMAP +
      ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT);

  /*
   * Set to
   * true  : for using pread
   * false : for using seek+read
   */
  public static boolean isPread = true;

  /**
   * Constructor
   * @param basedir qualified path under which the region directory lives;
   * generally the table subdirectory
   * @param region
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param confParam configuration object
   * failed.  Can be null.
   * @throws IOException
   */
  protected Store(Path basedir, HColumnDescriptor family, FileSystem fs,
      Configuration confParam, HRegion region, HRegionInfo regionInfo)
          throws IOException{
    super(new CompoundConfiguration().add(confParam).add(
        family.getValues()), regionInfo.getTableDesc().getNameAsString(),
        Bytes.toString(family.getName()));
    this.info = regionInfo;
    this.region = region;
    this.fs = fs;
    this.homedir = Store.getStoreHomedir(basedir, info.getEncodedName(), family.getName());
    if (!this.fs.exists(this.homedir)) {
      LOG.info("No directory exists for family " + family);
      // region being null is the case where we are creating a read only store.
      if (region != null) {
        LOG.info("Creating one");
        if (!this.fs.mkdirs(this.homedir))
          throw new IOException("Failed create of: " + this.homedir.toString());
      } else {
        throw new IOException("Failed create a read only store. " +
            "Possibly an inconsistent region");
      }
    }
    this.family = family;
    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.conf = new CompoundConfiguration()
      .add(confParam)
      .add(family.getValues());
    this.blockcache = family.isBlockCacheEnabled();
    this.blocksize = family.getBlocksize();
    this.compression = family.getCompression();
    String mcStr = conf.get("hbase.hstore.majorcompaction.compression", compression.getName());
    this.mcCompression = Compression.Algorithm.valueOf(mcStr.toUpperCase());

    this.dataBlockEncoder =
        new HFileDataBlockEncoderImpl(family.getDataBlockEncodingOnDisk(),
            family.getDataBlockEncoding());

    this.comparator = info.getComparator();
    this.flashBackQueryLimit = family.getFlashBackQueryLimit() * 1000;
    // getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    this.ttl = family.getTimeToLive();
    if (ttl == HConstants.FOREVER) {
      // default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else {
      // second -> ms adjust for user data
      this.ttl *= 1000;
    }

    // used by ScanQueryMatcher
    timeToPurgeDeletes =
        Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    LOG.info("time to purge deletes set to " + timeToPurgeDeletes +
        "ms in store " + this);

    this.memstore = new MemStore(conf, this.comparator);
    this.storeNameStr = getColumnFamilyName();

    // Setting up cache configuration for this family
    this.cacheConf = new CacheConfig.CacheConfigBuilder(conf)
        .withCacheDataOnRead(family.isBlockCacheEnabled())
        .withInMemory(family.isInMemory())
        .build();
    // By default we split region if a file > HConstants.DEFAULT_MAX_FILE_SIZE.
    long maxFileSize = info.getTableDesc().getMaxFileSize();
    if (maxFileSize == HConstants.DEFAULT_MAX_FILE_SIZE) {
      maxFileSize = conf.getLong("hbase.hregion.max.filesize",
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.desiredMaxFileSize = maxFileSize;
    this.blockingStoreFileCount =
      conf.getInt("hbase.hstore.blockingStoreFiles", -1);

    if (Store.closeCheckInterval == 0) {
      Store.closeCheckInterval = conf.getInt(
          "hbase.hstore.close.check.interval", 10*1000*1000 /* 10 MB */);
    }

    writeHFileHistogram  = conf.getBoolean(HConstants.USE_HFILEHISTOGRAM,
        HConstants.DEFAULT_USE_HFILEHISTOGRAM);
  }
  /**
   * Constructor
   * @param basedir qualified path under which the region directory lives;
   * generally the table subdirectory
   * @param region
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param conf configuration object
   * failed.  Can be null.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected Store(Path basedir, HRegion region, HColumnDescriptor family,
      FileSystem fs, Configuration confParam)
  throws IOException {
    this(basedir, family, fs, confParam, region, region.regionInfo);
    this.storefiles = sortAndClone(loadStoreFiles(this.fs.listStatus(this.homedir)));

    // Peak time is from [peakStartHour, peakEndHour). Valid numbers are [0, 23]
    this.peakStartHour = conf.getInt("hbase.peak.start.hour", -1);
    this.peakEndHour = conf.getInt("hbase.peak.end.hour", -1);
    if (!isValidHour(this.peakStartHour) || !isValidHour(this.peakEndHour)) {
      if (!(this.peakStartHour == -1 && this.peakEndHour == -1)) {
        LOG.warn("Invalid start/end hour for peak hour : start = " +
            this.peakStartHour + " end = " + this.peakEndHour +
            ". Valid numbers are [0-23]");
      }
      this.peakStartHour = this.peakEndHour = -1;
    }
    setCompactionPolicy(conf.get(HConstants.COMPACTION_MANAGER_CLASS,
                                 HConstants.DEFAULT_COMPACTION_MANAGER_CLASS));

    String aggregatorString = conf.get(HConstants.KV_AGGREGATOR);
    if (aggregatorString != null && !aggregatorString.isEmpty()) {
      try {
        this.aggregatorClass = ((Class<KeyValueAggregator>) Class
            .forName(aggregatorString));
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    }
    String compactHookString = conf.get(HConstants.COMPACTION_HOOK);
    if (compactHookString != null && !compactHookString.isEmpty()) {
      String compactionHookJar = conf.get(HConstants.COMPACTION_HOOK_JAR);
      if (compactionHookJar != null && !compactionHookJar.isEmpty()) {
        try {
          DynamicClassLoader.load(compactionHookJar, compactHookString);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        }
      }

      try {
        this.compactHook = (CompactionHook) Class.forName(compactHookString).newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  /**
   * This setter is used for unit testing
   * TODO: Fix this for online configuration updating
   */
  void setCompactionPolicy(String managerClassName) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends CompactionManager> managerClass =
        (Class<? extends CompactionManager>) Class.forName(managerClassName);
      compactionManager = managerClass.getDeclaredConstructor(
          new Class[] {Configuration.class, Store.class } ).newInstance(
          new Object[] { conf, this } );
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find region server interface " + managerClassName, e);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
          "Unable to access specified class " + managerClassName, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
          "Unable to instantiate specified class " + managerClassName, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Unable to invoke specified target class constructor " + managerClassName, e);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "Unable to find suitable constructor for class " + managerClassName, e);
    }
  }

 /**
  * @return A hash code depending on the state of the current store files.
  * This is used as seed for deterministic random generator for selecting major compaction time
  */
  Integer getDeterministicRandomSeed() {
    ImmutableList<StoreFile> snapshot = storefiles;
    if (snapshot != null && !snapshot.isEmpty()) {
      return snapshot.get(0).getPath().getName().hashCode();
    }
    return null;
  }

  private boolean isValidHour(int hour) {
    return (hour >= 0 && hour <= 23);
  }

  public HColumnDescriptor getFamily() {
    return this.family;
  }

  /**
   * @return The maximum sequence id in all store files.
   */
  long getMaxSequenceId(boolean includeBulkLoadedFiles) {
    return StoreFile.getMaxSequenceIdInList(this.getStorefiles(), includeBulkLoadedFiles);
  }

  /**
   * @return The maximum memstoreTS in all store files.
   */
  public long getMaxMemstoreTS() {
    return StoreFile.getMaxMemstoreTSInList(this.getStorefiles());
  }

  /**
   * @param tabledir
   * @param encodedName Encoded region name.
   * @param family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir,
      final String encodedName, final byte [] family) {
    return new Path(tabledir, new Path(encodedName,
      new Path(Bytes.toString(family))));
  }

  /**
   * Return the directory in which this store stores its
   * StoreFiles
   */
  public Path getHomedir() {
    return homedir;
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
   * @throws IOException
   */
  protected List<StoreFile> loadStoreFiles(FileStatus[] files)
      throws IOException {
    ArrayList<StoreFile> results = new ArrayList<StoreFile>();

    if (files == null || files.length == 0) {
      return results;
    }
    // initialize the thread pool for opening store files in parallel..
    ThreadPoolExecutor storeFileOpenerThreadPool =
        StoreThreadUtils.getStoreFileOpenAndCloseThreadPool("StoreFileOpenerThread-" +
          this.family.getNameAsString(), this.getHRegionInfo(), this.conf);
    CompletionService<StoreFile> completionService =
      new ExecutorCompletionService<StoreFile>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (int i = 0; files != null && i < files.length; i++) {
      // Skip directories.
      if (files[i].isDir()) {
        continue;
      }
      final Path p = files[i].getPath();
      // Check for empty file. Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      if (this.fs.getFileStatus(p).getLen() <= 0) {
        LOG.warn("Skipping " + p + " because its empty. HBASE-646 DATA LOSS?");
        continue;
      }

      // open each store file in parallel
      completionService.submit(new Callable<StoreFile>() {
        public StoreFile call() throws IOException {
          StoreFile storeFile = new StoreFile(fs, p, conf, cacheConf,
              family.getBloomFilterType(), dataBlockEncoder);
          passSchemaMetricsTo(storeFile);
          storeFile.createReader();
          return storeFile;
        }
      });
      totalValidStoreFile++;
    }

    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        Future<StoreFile> future = completionService.take();
        StoreFile storeFile = future.get();
        long length = storeFile.getReader().length();
        this.storeSize += length;
        if (LOG.isDebugEnabled()) {
          LOG.debug("loaded " + storeFile.toStringDetailed());
        }
        results.add(storeFile);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      storeFileOpenerThreadPool.shutdownNow();
    }

    return results;
  }

  /**
   * Adds a value to the memstore
   *
   * @param kv The KV to be added.
   * @param seqNum The LSN associated with the key.
   * @return
   */
  protected long add(final KeyValue kv, long seqNum) {
    lock.readLock().lock();
    try {
      return this.memstore.add(kv, seqNum);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Deletes a value to the memstore
   *
   * @param kv
   * @return memstore size delta
   */
  protected long delete(final KeyValue kv, long seqNum) {
    lock.readLock().lock();
    try {
      return this.memstore.delete(kv, seqNum);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return All store files.
   */
  List<StoreFile> getStorefiles() {
    return this.storefiles;
  }

  public void bulkLoadHFile(String srcPathStr, long sequenceId) throws IOException {
    Path srcPath = new Path(srcPathStr);

    HFile.Reader reader  = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in "
          + "store " + this + " region " + this.region);
      reader = HFile.createReader(srcPath.getFileSystem(conf),
          srcPath, cacheConf);
      reader.loadFileInfo();

      byte[] firstKey = reader.getFirstRowKey();
      byte[] lk = reader.getLastKey();
      byte[] lastKey =
          (lk == null) ? null :
              KeyValue.createKeyValueFromKey(lk).getRow();

      LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey) +
          " last=" + Bytes.toStringBinary(lastKey));
      LOG.debug("Region bounds: first=" +
          Bytes.toStringBinary(region.getStartKey()) +
          " last=" + Bytes.toStringBinary(region.getEndKey()));

      HRegionInfo hri = region.getRegionInfo();
      if (!hri.containsRange(firstKey, lastKey)) {
        throw new WrongRegionException(
            "Bulk load file " + srcPathStr + " does not fit inside region "
            + this.region);
      }
    } finally {
      if (reader != null) reader.close();
    }

    // Move the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    if (!srcFs.equals(fs)) {
      LOG.info("File " + srcPath + " on different filesystem than " +
          "destination store - moving to this filesystem.");
      Path tmpPath = getTmpPath();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied to temporary path on dst filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    Path dstPath = StoreFile.getRandomFilename(fs, homedir,
        (sequenceId >= 0) ? ("_SeqId_" + sequenceId + "_") : null);
    LOG.info("Renaming bulk load file " + srcPath + " to " + dstPath);
    StoreFile.rename(fs, srcPath, dstPath);

    StoreFile sf = new StoreFile(fs, dstPath, this.conf, this.cacheConf,
        this.family.getBloomFilterType(), this.dataBlockEncoder);
    passSchemaMetricsTo(sf);

    sf.createReader();

    LOG.info("Moved hfile " + srcPath + " into store directory " +
        homedir + " - updating store file list.");

    // Append the new storefile into the list
    this.lock.writeLock().lock();
    try {
      ArrayList<StoreFile> newFiles = new ArrayList<StoreFile>(storefiles);
      newFiles.add(sf);
      this.storefiles = sortAndClone(newFiles);
    } finally {
      // We need the lock, as long as we are updating the storefiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      this.lock.writeLock().unlock();
    }
    notifyChangedReadersObservers();
    LOG.info("Successfully loaded store file " + srcPath
        + " into store " + this + " (new location: " + dstPath + ")");
  }

  /**
   * Get a temporary path in this region. These temporary files
   * will get cleaned up when the region is re-opened if they are
   * still around.
   */
  private Path getTmpPath() throws IOException {
    return StoreFile.getRandomFilename(
        fs, region.getTmpDir());
  }

  /**
   * Close all the readers
   *
   * We don't need to worry about subsequent requests because the HRegion holds
   * a write lock that will prevent any more reads or writes.
   *
   * @throws IOException
   */
  ImmutableList<StoreFile> close() throws IOException {
    return close(false);
  }

  /**
   * Closes the Store. In addition it also physically deletes the corresponding
   * store files underlying this store depending upon argument.
   * @param deleteStoreFile : True - deletes the store files
   * @return
   * @throws IOException
   */
  public ImmutableList<StoreFile> close(final boolean deleteStoreFile) throws IOException {
    this.lock.writeLock().lock();
    try {
      ImmutableList<StoreFile> result = storefiles;

      // Clear so metrics doesn't find them.
      storefiles = ImmutableList.of();
      if (!result.isEmpty()) {
        // initialize the thread pool for closing store files in parallel.
        ThreadPoolExecutor storeFileCloserThreadPool =
            StoreThreadUtils.getStoreFileOpenAndCloseThreadPool("StoreFileCloserThread-"
                + this.family.getNameAsString(), this.getHRegionInfo(), this.conf);

        // close each store file in parallel
        CompletionService<Void> completionService =
          new ExecutorCompletionService<Void>(storeFileCloserThreadPool);
        for (final StoreFile f : result) {
          completionService.submit(new Callable<Void>() {
            public Void call() throws IOException {
              if (deleteStoreFile) {
                f.deleteReader();
              } else {
                f.closeReader(true);
              }
              return null;
            }
          });
        }

        try {
          for (int i = 0; i < result.size(); i++) {
            Future<Void> future = completionService.take();
            future.get();
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        } finally {
          storeFileCloserThreadPool.shutdownNow();
        }
      }
      LOG.debug("closed " + this.storeNameStr);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Snapshot this stores memstore.  Call before running
   * {@link #flushCache(long, java.util.SortedSet, TimeRangeTracker,
   *                    org.apache.hadoop.hbase.monitoring.MonitoredTask)}
   * so it has some work to do.
   */
  void snapshot() {
    this.memstore.snapshot();
  }

  /**
   * Write out current snapshot.  Presumes {@link #snapshot()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @param snapshot
   * @param snapshotTimeRangeTracker
   * @param status
   * @return true if a compaction is needed
   * @throws IOException
   */
  private StoreFile flushCache(final long logCacheFlushId,
      SortedSet<KeyValue> snapshot,
      TimeRangeTracker snapshotTimeRangeTracker,
      MonitoredTask status,
      CacheFlushInfo info) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    return internalFlushCache(snapshot, logCacheFlushId,
        snapshotTimeRangeTracker, status, info);
  }

  /*
   * @param cache
   * @param logCacheFlushId
   * @param snapshotTimeRangeTracker
   * @param status
   * @return StoreFile created.
   * @throws IOException
   */
  private StoreFile internalFlushCache(final SortedSet<KeyValue> snapshot,
      final long logCacheFlushId,
      TimeRangeTracker snapshotTimeRangeTracker,
      MonitoredTask status,
      CacheFlushInfo info) throws IOException {
    StoreFile.Writer writer;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = region.getSmallestReadPoint();
    long flushed = 0;
    // Don't flush if there are no entries.
    if (snapshot.size() == 0) {
      return null;
    }

    // Use a store scanner from snapshot to find out which rows to flush
    // Note that we need to retain deletes.
    Scan scan = new Scan();
    scan.setMaxVersions(family.getMaxVersions());
    InternalScanner scanner = new StoreScanner(this, scan,
        MemStore.getSnapshotScanners(snapshot, this.comparator),
        this.region.getSmallestReadPoint(),
        Long.MIN_VALUE, getAggregator(),
        flashBackQueryLimit); // include all deletes
    HFileHistogram hist = new UniformSplitHFileHistogram(
        this.conf.getInt(HFileHistogram.HFILEHISTOGRAM_BINCOUNT,
            HFileHistogram.DEFAULT_HFILEHISTOGRAM_BINCOUNT));
    String fileName;
    try {
      // TODO:  We can fail in the below block before we complete adding this
      // flush to list of store files.  Add cleanup of anything put on filesystem
      // if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + this + ": creating writer");
        // A. Write the map out to the disk
        writer = createWriterInTmp(snapshot.size(), this.compression, false);
        info.tmpPath = writer.getPath();
        writer.setTimeRangeTracker(snapshotTimeRangeTracker);
        fileName = writer.getPath().getName();
        try {
          byte[] lastRow = new byte[0];
          final List<KeyValue> kvs = new ArrayList<KeyValue>();
          boolean hasMore;
          do {
            hasMore = scanner.next(kvs);
            if (!kvs.isEmpty()) {
              for (KeyValue kv : kvs) {
                if (writeHFileHistogram) {
                  byte[] thisRow = kv.getRow();
                  if (!Bytes.equals(lastRow, thisRow)) {
                    hist.add(kv);
                  }
                  lastRow = thisRow;
                }
                // If we know that this KV is going to be included always, then let us
                // set its memstoreTS to 0. This will help us save space when writing to disk.
                if (kv.getMemstoreTS() <= smallestReadPoint) {
                  // let us not change the original KV. It could be in the memstore
                  // changing its memstoreTS could affect other threads/scanners.
                  kv = kv.shallowCopy();
                  kv.setMemstoreTS(0);
                }
                writer.append(kv);
                flushed += this.memstore.heapSizeChange(kv, true);
              }
              kvs.clear();
            }
          } while (hasMore);
        } finally {
          // Write out the log sequence number that corresponds to this output
          // Write current time in metadata as minFlushTime
          // hfile.  The hfile is current up to and including logCacheFlushId.
          status.setStatus("Flushing " + this + ": appending metadata");
          writer.appendMetadata(EnvironmentEdgeManager.currentTimeMillis(), logCacheFlushId, false);
          if (writeHFileHistogram) {
            writer.appendHFileHistogram(hist);
          }
          status.setStatus("Flushing " + this + ": closing flushed file");
          writer.close();
          InjectionHandler.processEventIO(InjectionEvent.STOREFILE_AFTER_WRITE_CLOSE, writer.getPath());
        }
      }
    } finally {
      scanner.close();
    }

    Path dstPath = new Path(homedir, fileName);

    validateStoreFile(writer.getPath());

    // Write-out finished successfully, move into the right spot
    LOG.info("Renaming flushed file at " + writer.getPath() + " to " + dstPath);
    fs.rename(writer.getPath(), dstPath);
    InjectionHandler.processEventIO(InjectionEvent.STOREFILE_AFTER_RENAME, writer.getPath(), dstPath);
    info.dstPath = dstPath;

    StoreFile sf = new StoreFile(this.fs, dstPath, this.conf, this.cacheConf,
        this.family.getBloomFilterType(), this.dataBlockEncoder);
    passSchemaMetricsTo(sf);

    StoreFile.Reader r = sf.createReader();
    info.sequenceId = logCacheFlushId;
    info.entries = r.getEntries();
    info.memSize = flushed;
    info.fileSize = r.length();
    return sf;
  }

  private boolean commit(StoreFile storeFile, SortedSet<KeyValue> snapshot, CacheFlushInfo info)
    throws IOException {
    this.storeSize += info.fileSize;
    // This increments the metrics associated with total flushed bytes for this
    // family. The overall flush count is stored in the static metrics and
    // retrieved from HRegion.recentFlushes, which is set within
    // HRegion.internalFlushcache, which indirectly calls this to actually do
    // the flushing through the StoreFlusherImpl class
    getSchemaMetrics().updatePersistentStoreMetric(
        SchemaMetrics.StoreMetricType.FLUSH_SIZE, info.memSize);
    if (LOG.isInfoEnabled()) {
      LOG.info("Added " + storeFile + ", entries=" + info.entries +
        ", sequenceid=" + info.sequenceId +
        ", memsize=" + StringUtils.humanReadableInt(info.memSize) +
        ", filesize=" + StringUtils.humanReadableInt(info.fileSize) +
        " to " + this.region.regionInfo.getRegionNameAsString());
    }
    // Add new file to store files.  Clear snapshot too while we have
    // the Store write lock.
    return updateStorefiles(storeFile, snapshot);
  }

  private void cancel(CacheFlushInfo info) {
    if (info.tmpPath != null) {
      try {
        fs.delete(info.tmpPath, false);
      } catch (IOException e) {
        // that's ok
      }
    }
    if (info.dstPath != null) {
      try {
        fs.delete(info.dstPath, false);
      } catch (IOException e) {
        // that's ok
      }
    }
  }

  /*
   * @return Writer for a new StoreFile in the tmp dir.
   */
  private StoreFile.Writer createWriterInTmp(long maxKeyCount,
      Compression.Algorithm compression, boolean isCompaction)
      throws IOException {
    // This method is only invoked during write for flush/compaction and hence
    // on the DFS level we have a sync_file_range(WAIT_AFTER) to throttle these
    // background writers.
    WriteOptions options = new WriteOptions();
    if (conf.getBoolean(HConstants.HBASE_ENABLE_SYNCFILERANGE_THROTTLING_KEY,
        false)) {
      options.setSyncFileRange(NativeIO.SYNC_FILE_RANGE_WAIT_AFTER
          | NativeIO.SYNC_FILE_RANGE_WRITE);
    }
    if (conf.getBoolean(HConstants.HBASE_ENABLE_QOS_KEY, false)) {
      int priority = (isCompaction) ? HConstants.COMPACT_PRIORITY
          : HConstants.FLUSH_PRIORITY;
      options.setIoprio(HConstants.IOPRIO_CLASSOF_SERVICE, priority);
    }
    return createWriterInTmp(maxKeyCount, compression, isCompaction,
        options);
  }

  /*
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @return Writer for a new StoreFile in the tmp dir.
   */
  private StoreFile.Writer createWriterInTmp(long maxKeyCount,
      Compression.Algorithm compression, boolean isCompaction,
      WriteOptions options)
  throws IOException {
    final CacheConfig writerCacheConf;
    if (isCompaction) {
      // Don't cache data on write on compactions.
      writerCacheConf = new CacheConfig(cacheConf);
      writerCacheConf.setCacheDataOnFlush(false);
    } else {
      writerCacheConf = cacheConf;
    }
    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf,
        fs, blocksize)
            .withOutputDir(region.getTmpDir())
            .withDataBlockEncoder(dataBlockEncoder)
            .withComparator(comparator)
            .withBloomType(family.getBloomFilterType())
            .withRowKeyPrefixLengthForBloom(family.getRowPrefixLengthForBloom())
            .withMaxKeyCount(maxKeyCount)
            .withFavoredNodes(region.getFavoredNodes())
            .withCompression(compression)
            .withWriteOptions(options)
            .build();
    w.getHFileWriter().setCompactionWriter(isCompaction);
    // The store file writer's path does not include the CF name, so we need
    // to configure the HFile writer directly.
    SchemaConfigured sc = (SchemaConfigured) w.writer;
    passSchemaMetricsTo(sc);
    return w;
  }

  /*
   * Change storefiles adding into place the Reader produced by this new flush.
   * @param sf
   * @param set That was used to make the passed file <code>p</code>.
   * @throws IOException
   * @return Whether compaction is required.
   */
  private boolean updateStorefiles(final StoreFile sf,
                                   final SortedSet<KeyValue> set)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      ArrayList<StoreFile> newList = new ArrayList<StoreFile>(storefiles);
      newList.add(sf);
      storefiles = sortAndClone(newList);

      this.memstore.clearSnapshot(set);
    } finally {
      // We need the lock, as long as we are updating the storefiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      this.lock.writeLock().unlock();
    }

    // Tell listeners of the change in readers.
    notifyChangedReadersObservers();

    return needsCompaction();
  }

  /*
   * Notify all observers that set of Readers has changed.
   * @throws IOException
   */
  private void notifyChangedReadersObservers() throws IOException {
    for (ChangedReadersObserver o: this.changedReaderObservers) {
      o.updateReaders();
    }
  }

  /**
   * Get all scanners with no filtering based on TTL (that happens further down
   * the line).
   * @param cacheBlocks whether those scanners should cache the blocks into the block cache or not.
   * @param isCompaction whether those scanners is being used for compaction or not.
   * @param preloadDataBlocks whether those scanners should preload the next data blocks into the block cache or not.
   * @return all scanners for this store
   */
  protected List<KeyValueScanner>
      getScanners(boolean cacheBlocks, boolean isCompaction,
          boolean preloadBlocks, ScanQueryMatcher matcher)
          throws IOException {
    List<StoreFile> storeFiles;
    List<KeyValueScanner> memStoreScanners;
    this.lock.readLock().lock();
    try {
      storeFiles = this.getStorefiles();
      memStoreScanners = this.memstore.getScanners();
    } finally {
      this.lock.readLock().unlock();
    }

    // First the store file scanners

    // TODO this used to get the store files in descending order,
    // but now we get them in ascending order, which I think is
    // actually more correct, since memstore get put at the end.

    boolean isClientSideScanOrCompaction =
        this.conf.getBoolean(HConstants.CLIENT_SIDE_SCAN,
        HConstants.DEFAULT_CLIENT_SIDE_SCAN) || isCompaction;
    // useMVCC = !isClientSideScanOrCompaction

    List<StoreFileScanner> sfScanners =
        StoreFileScanner.getScannersForStoreFiles(storeFiles, cacheBlocks,
          isCompaction, preloadBlocks, matcher, isClientSideScanOrCompaction);
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(sfScanners.size()+1);
    scanners.addAll(sfScanners);
    // Then the memstore scanners
    scanners.addAll(memStoreScanners);
    return scanners;
  }

  /*
   * @param o Observer who wants to know about changes in set of Readers
   */
  void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }

  /*
   * @param o Observer no longer interested in changes in set of Readers.
   */
  void deleteChangedReaderObserver(ChangedReadersObserver o) {
    if (!this.changedReaderObservers.remove(o)) {
      LOG.warn("Not in set " + o);
    }
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
   * @param compactionRequest
   *          compaction details obtained from requestCompaction()
   * @throws IOException
   */
  void compact(CompactionRequest compactionRequest) throws IOException {
    if (compactionRequest == null || compactionRequest.getFiles().isEmpty()) {
      return;
    }
    Preconditions.checkArgument(compactionRequest.getStore().toString()
        .equals(this.toString()));

    List<StoreFile> filesToCompact = compactionRequest.getFiles();

    synchronized (filesCompacting) {
      // sanity check: we're compacting files that this store knows about
      // TODO: change this to LOG.error() after more debugging
      Preconditions.checkArgument(filesCompacting.containsAll(filesToCompact));
    }

    // Max-sequenceID is the last key in the files we're compacting
    long maxId = StoreFile.getMaxSequenceIdInList(filesToCompact, true);

    // Ready to go. Have list of files to compact.
    MonitoredTask status = TaskMonitor.get().createStatus(
        (compactionRequest.isMajor() ? "Major " : "")
        + "Compaction (ID: " + compactionRequest.getCompactSelectionID() + ") of "
        + this.storeNameStr + " on "
        + this.region.getRegionInfo().getRegionNameAsString());
    LOG.info("Starting compaction (ID: " + compactionRequest.getCompactSelectionID() + ") of "
        + filesToCompact.size() + " file(s) in " + this.storeNameStr + " of "
        + this.region.getRegionInfo().getRegionNameAsString()
        + " into " + region.getTmpDir() + ", seqid=" + maxId + ", totalSize="
        + StringUtils.humanReadableInt(compactionRequest.getSize()));

    StoreFile sf = null;
    try {
      status.setStatus("Compacting " + filesToCompact.size() + " file(s)");
      long compactionStartTime = EnvironmentEdgeManager.currentTimeMillis();
      StoreFile.Writer writer = compactStores(filesToCompact, compactionRequest.isMajor(), maxId);
      // Move the compaction into place.
      sf = completeCompaction(filesToCompact, writer);

      // Report that the compaction is complete.
      status.markComplete("Completed compaction");
      LOG.info("Completed" + (compactionRequest.isMajor() ? " major " : " ")
          + "compaction (ID: " + compactionRequest.getCompactSelectionID() + ") of "
          + filesToCompact.size() + " file(s) in " + this.storeNameStr + " of "
          + this.region.getRegionInfo().getRegionNameAsString()
          + "; This selection was in queue for "
          + StringUtils.formatTimeDiff(compactionStartTime, compactionRequest.getSelectionTime()) + ", and took "
          + StringUtils.formatTimeDiff(EnvironmentEdgeManager.currentTimeMillis(),
                                       compactionStartTime)
          + " to execute. New storefile name="
          + (sf == null ? "(none)" : sf.getReader().getHFileReader().toShortString())
          + "; total size for store is "
          + StringUtils.humanReadableInt(storeSize));
      if (writer != null) {
        LOG.info(", number of blocks precached="
            + writer.getHFileWriter().getNumBlocksCachedPerCompaction());
      }
    } catch (IOException ioe) {
      // rather than leak the status, we abort here, then rethrow the exception
      status.abort(StringUtils.stringifyException(ioe));
      throw ioe;
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
      status.cleanup();
    }
  }

  /**
   * Compact the most recent N files.
   *
   * @param N
   *          the number of store files to compact, pass -1 to compact
   *          everything
   */
  public void compactRecentForTesting(int N) throws IOException {
    List<StoreFile> filesToCompact;
    long maxId;
    boolean isMajor;

    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        filesToCompact = Lists.newArrayList(storefiles);
        if (!filesCompacting.isEmpty()) {
          // exclude all files older than the newest file we're currently
          // compacting. this allows us to preserve contiguity (HBASE-2856)
          StoreFile last = filesCompacting.get(filesCompacting.size() - 1);
          int idx = filesToCompact.indexOf(last);
          Preconditions.checkArgument(idx != -1);
          filesToCompact.subList(0, idx + 1).clear();
        }
        int count = filesToCompact.size();
        if (N > count && N != -1) {
          throw new RuntimeException("Not enough files");
        }

        if (N != -1) {
          filesToCompact = filesToCompact.subList(count - N, count);
        }
        maxId = StoreFile.getMaxSequenceIdInList(filesToCompact, true);
        isMajor = (filesToCompact.size() == storefiles.size());
        filesCompacting.addAll(filesToCompact);
        Collections.sort(filesCompacting, StoreFile.Comparators.SEQ_ID);
      }
    } finally {
      this.lock.readLock().unlock();
    }

    try {
      // Ready to go. Have list of files to compact.
      StoreFile.Writer writer = compactStores(filesToCompact, isMajor, maxId);
      // Move the compaction into place.
      completeCompaction(filesToCompact, writer);
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
    }
  }

  boolean hasReferences() {
    return hasReferences(this.storefiles);
  }

  /*
   * @param files
   * @return True if any of the files in <code>files</code> are References.
   */
  boolean hasReferences(Collection<StoreFile> files) {
    if (files != null && files.size() > 0) {
      for (StoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }

  /*
   * @return True if we should run a major compaction.
   */
  boolean isMajorCompaction() throws IOException {
    for (StoreFile sf : this.storefiles) {
      if (sf.getReader() == null) {
        LOG.debug("StoreFile " + sf + " has null Reader");
        return false;
      }
    }

    List<StoreFile> candidates = new ArrayList<StoreFile>(this.storefiles);
    return compactionManager.isMajorCompaction(candidates);
  }

  boolean isPeakTime(int currentHour) {
    // Peak time checking is disabled just return false.
    if (this.peakStartHour == this.peakEndHour) {
      return false;
    }
    if (this.peakStartHour <= this.peakEndHour) {
      return (currentHour >= this.peakStartHour && currentHour < this.peakEndHour);
    }
    return (currentHour >= this.peakStartHour || currentHour < this.peakEndHour);
  }

  public CompactionRequest requestCompaction() {
    // don't even select for compaction if writes are disabled
    if (!this.region.areWritesEnabled()) {
      return null;
    }

    CompactionRequest ret = null;
    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        // candidates = all storefiles not already in compaction queue
        List<StoreFile> candidates = Lists.newArrayList(storefiles);
        if (!filesCompacting.isEmpty()) {
          // exclude all files older than the newest file we're currently
          // compacting. this allows us to preserve contiguity (HBASE-2856)
          StoreFile last = filesCompacting.get(filesCompacting.size() - 1);
          int idx = candidates.indexOf(last);
          Preconditions.checkArgument(idx != -1);
          candidates.subList(0, idx + 1).clear();
        }
        CompactSelection filesToCompact;
        filesToCompact = compactionManager.selectCompaction(candidates,
          forceMajor && filesCompacting.isEmpty());

        // no files to compact
        if (filesToCompact.getFilesToCompact().isEmpty()) {
          return null;
        }

        // basic sanity check: do not try to compact the same StoreFile twice.
        if (!Collections.disjoint(filesCompacting, filesToCompact.getFilesToCompact())) {
          // TODO: change this from an IAE to LOG.error after sufficient testing
          Preconditions.checkArgument(false, "%s overlaps with %s",
              filesToCompact, filesCompacting);
        }
        filesCompacting.addAll(filesToCompact.getFilesToCompact());
        Collections.sort(filesCompacting, StoreFile.Comparators.SEQ_ID);

        // major compaction iff all StoreFiles are included
        boolean isMajor = (filesToCompact.getFilesToCompact().size() == this.storefiles.size());
        if (isMajor) {
          // since we're enqueuing a major, update the compaction wait interval
          this.forceMajor = false;
        }

        // everything went better than expected. create a compaction request
        int pri = getCompactPriority();
        ret = new CompactionRequest(region, this, filesToCompact, isMajor, pri);
      }
    } catch (IOException ex) {
      LOG.error("Compaction Request failed for region " + region + ", store "
          + this, RemoteExceptionHandler.checkIOException(ex));
    } finally {
      this.lock.readLock().unlock();
    }
    return ret;
  }

  public void finishRequest(CompactionRequest cr) {
    cr.finishRequest();
    synchronized (filesCompacting) {
      filesCompacting.removeAll(cr.getFiles());
    }
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles.
   * Uses the scan infrastructure to make it easy.
   *
   * @param filesToCompact which files to compact
   * @param majorCompaction true to major compact (prune all deletes, max versions, etc)
   * @param maxCompactingSequcenceId The maximum sequence id among the filesToCompact
   * @return Product of compaction or null if all cells expired or deleted and
   * nothing made it through the compaction.
   * @throws IOException
   */
  StoreFile.Writer compactStores(final Collection<StoreFile> filesToCompact,
                               final boolean majorCompaction, final long maxCompactingSequcenceId)
      throws IOException {
    // calculate maximum key count (for blooms), and minFlushTime after compaction
    long maxKeyCount = 0;
    // how much bytes the compaction hook saved
    long bytesSaved = 0;
    // how many KVs were converted with compaction hook
    long kvsConverted = 0;
    long minFlushTime = Long.MAX_VALUE;
    for (StoreFile file : filesToCompact) {
      if (file.hasMinFlushTime() && file.getMinFlushTime() < minFlushTime) {
          minFlushTime = file.getMinFlushTime();
      }
      StoreFile.Reader r = file.getReader();
      if (r != null) {
        // NOTE: getFilterEntries could cause under-sized blooms if the user
        //       switches bloom type (e.g. from ROW to ROWCOL)
        long keyCount = (r.getBloomFilterType() == family.getBloomFilterType())
            ? r.getFilterEntries() : r.getEntries();
        maxKeyCount += keyCount;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Compacting " + r.getHFileReader().toShortString());
        }
      }
    }
    LOG.info("Estimated total keyCount for output of compaction = " + maxKeyCount);

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners = StoreFileScanner
      .getScannersForStoreFiles(filesToCompact, false, true);

    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    // determine compression type (may be different for major compaction)
    Compression.Algorithm compression = (majorCompaction) ? this.mcCompression : this.compression;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = region.getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    HFileHistogram hist = new UniformSplitHFileHistogram(
        this.conf.getInt(HFileHistogram.HFILEHISTOGRAM_BINCOUNT,
            HFileHistogram.DEFAULT_HFILEHISTOGRAM_BINCOUNT));
    try {
      InternalScanner scanner = null;
      try {
        Scan scan = new Scan();
        scan.setMaxVersions(family.getMaxVersions());
        /* include deletes, unless we are doing a major compaction */
        long retainDeletesUntil = (majorCompaction) ?
          (this.timeToPurgeDeletes <= 0 ? Long.MAX_VALUE :
             (System.currentTimeMillis() - this.timeToPurgeDeletes))
          : Long.MIN_VALUE;
        scanner = new StoreScanner(this, scan, scanners, smallestReadPoint,
            retainDeletesUntil, getAggregator(), flashBackQueryLimit);
        int bytesWritten = 0;
        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
        boolean hasMore;
        // Create the writer whether or not there are output KVs,
        // iff the maxSequenceID among the compaction candidates is
        // equal to the maxSequenceID among all the on-disk hfiles. [HBASE-7267]
        if (maxCompactingSequcenceId == this.getMaxSequenceId(true)) {
          writer = createWriterInTmp(maxKeyCount, compression, true);
        }
        KeyValueContext kvContext = new KeyValueContext();

        byte[] lastRow = new byte[0];
        do {
          hasMore = scanner.next(kvs, 1, kvContext);
          if (!kvs.isEmpty()) {
            if (writer == null) {
              writer = createWriterInTmp(maxKeyCount, compression, true);
            }
            // output to writer:
            for (KeyValue kv : kvs) {
              if (writeHFileHistogram) {
                byte[] thisRow = kv.getRow();
                if (!Bytes.equals(lastRow, thisRow)) {
                  hist.add(kv);
                }
                lastRow = thisRow;
              }
              if (kv.getMemstoreTS() <= smallestReadPoint) {
                kv.setMemstoreTS(0);
              }
              if (compactHook != null && kv.isPut()) {
                RestrictedKeyValue restrictedKv = new RestrictedKeyValue(kv);
                RestrictedKeyValue modifiedKv = compactHook.transform(restrictedKv);
                if (modifiedKv != null) {
                  writer.append(modifiedKv.getKeyValue(), kvContext);
                  bytesSaved += modifiedKv.differenceInBytes(kv);
                } else {
                  if (kv != null) {
                    bytesSaved += kv.getLength();
                  }
                }
                if (!restrictedKv.equals(modifiedKv)) {
                  kvsConverted++;

                }
              } else {
                writer.append(kv, kvContext);
              }
              // check periodically to see if a system stop is requested
              if (Store.closeCheckInterval > 0) {
                bytesWritten += kv.getLength();
                if (bytesWritten > Store.closeCheckInterval) {
                  getSchemaMetrics().updatePersistentStoreMetric(
                    SchemaMetrics.StoreMetricType.COMPACTION_WRITE_SIZE,
                    bytesWritten);
                  getSchemaMetrics().updatePersistentStoreMetric(
                      SchemaMetrics.StoreMetricType.STORE_COMPHOOK_BYTES_SAVED,
                      bytesSaved);
                  getSchemaMetrics()
                      .updatePersistentStoreMetric(
                          SchemaMetrics.StoreMetricType.STORE_COMPHOOK_KVS_TRANSFORMED,
                          kvsConverted);
                  bytesWritten = 0;
                  bytesSaved = 0;
                  kvsConverted = 0;
                  if (!this.region.areWritesEnabled()) {
                    writer.close();
                    fs.delete(writer.getPath(), false);
                    throw new InterruptedIOException(
                        "Aborting compaction of store " + this +
                        " in region " + this.region +
                    " because user requested stop.");
                  }
                }
              }
            }
          }
          kvs.clear();
        } while (hasMore);
        getSchemaMetrics().updatePersistentStoreMetric(
            SchemaMetrics.StoreMetricType.COMPACTION_WRITE_SIZE, bytesWritten);
        getSchemaMetrics().updatePersistentStoreMetric(
            SchemaMetrics.StoreMetricType.STORE_COMPHOOK_BYTES_SAVED,
            bytesSaved);
        getSchemaMetrics().updatePersistentStoreMetric(
            SchemaMetrics.StoreMetricType.STORE_COMPHOOK_KVS_TRANSFORMED,
            kvsConverted);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (writer != null) {
        if (minFlushTime == Long.MAX_VALUE) {
          minFlushTime = HConstants.NO_MIN_FLUSH_TIME;
        }
        writer.appendMetadata(minFlushTime, maxCompactingSequcenceId, majorCompaction);
        if (writeHFileHistogram) {
          writer.appendHFileHistogram(hist);
        }
        writer.close();
      }
    }
    return writer;
  }

  private HFileHistogram hist = null;
  /**
   *
   * @return HFileHistogram for this store.
   * Returns null if no data available.
   * @throws IOException
   */
  public synchronized HFileHistogram getHistogram() throws IOException {
    if (hist != null) return hist;
    List<HFileHistogram> histograms = new ArrayList<HFileHistogram>();
    if (storefiles.size() == 0) return hist;
    for (StoreFile file : this.storefiles) {
      HFileHistogram hist = file.getHistogram();
      if (hist != null) histograms.add(hist);
    }
    if (histograms.size() == 0) return hist;
    HFileHistogram h = histograms.get(0).compose(histograms);
    this.hist = h;
    return hist;
  }

  /**
   * Validates a store file by opening and closing it. In HFileV2 this should
   * not be an expensive operation.
   *
   * @param path
   *          the path to the store file
   */
  private void validateStoreFile(Path path)
      throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(this.fs, path, this.conf,
          this.cacheConf, this.family.getBloomFilterType(),
          NoOpDataBlockEncoder.INSTANCE);
      passSchemaMetricsTo(storeFile);
      storeFile.createReader();
    } catch (IOException e) {
      LOG.error("Failed to open store file : " + path
          + ", keeping it in tmp location", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }

  /*
   * <p>It works by processing a compaction that's been written to disk.
   *
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   *
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Moving the new compacted StoreFile into place
   * 2) Unload all replaced StoreFile, close and collect list to delete.
   * 3) Loading the new TreeMap.
   * 4) Compute new store size
   * </pre>
   *
   * @param compactedFiles list of files that were compacted
   * @param compactedFile StoreFile that is the result of the compaction
   * @return StoreFile created. May be null.
   * @throws IOException
   */
  StoreFile completeCompaction(final Collection<StoreFile> compactedFiles,
                                       final StoreFile.Writer compactedFile)
      throws IOException {
    // 1. Moving the new files into place -- if there is a new file (may not
    // be if all cells were expired or deleted).
    StoreFile result = null;
    if (compactedFile != null) {
      Path origPath = compactedFile.getPath();
      Path destPath = new Path(homedir, origPath.getName());

      validateStoreFile(origPath);

      // Move file into the right spot
      LOG.info("Renaming compacted file at " + origPath + " to " + destPath);
      if (!fs.rename(origPath, destPath)) {
        LOG.error("Failed move of compacted file " + origPath + " to " +
            destPath);
      }
      result = new StoreFile(this.fs, destPath, this.conf, this.cacheConf,
          this.family.getBloomFilterType(), this.dataBlockEncoder);
      passSchemaMetricsTo(result);
      result.createReader();
    }
    try {
      try {
      this.lock.writeLock().lock();
        // Change this.storefiles so it reflects new state but do not
        // delete old store files until we have sent out notification of
        // change in case old files are still being accessed by outstanding
        // scanners.
        ArrayList<StoreFile> newStoreFiles = Lists.newArrayList(storefiles);
        newStoreFiles.removeAll(compactedFiles);
        filesCompacting.removeAll(compactedFiles); // safe bc: lock.writeLock()

        // If a StoreFile result, move it into place.  May be null.
        if (result != null) {
          newStoreFiles.add(result);
        }

        this.storefiles = sortAndClone(newStoreFiles);
      } finally {
        // We need the lock, as long as we are updating the storefiles
        // or changing the memstore. Let us release it before calling
        // notifyChangeReadersObservers. See HBASE-4485 for a possible
        // deadlock scenario that could have happened if continue to hold
        // the lock.
        this.lock.writeLock().unlock();
      }

      // Tell observers that list of StoreFiles has changed.
      notifyChangedReadersObservers();

      InjectionHandler.processEvent(
          InjectionEvent.STORESCANNER_COMPACTION_RACE, new Object[] {2});

      // Finally, delete old store files.
      for (StoreFile hsf: compactedFiles) {
        hsf.deleteReader();
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("Failed replacing compacted files in " + this.storeNameStr +
        ". Compacted file is " + (result == null? "none": result.toString()) +
        ".  Files replaced " + compactedFiles.toString() +
        " some of which may have been already removed", e);
    }
    // 4. Compute new store size
    this.storeSize = 0L;
    for (StoreFile hsf : this.storefiles) {
      StoreFile.Reader r = hsf.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + hsf + " has a null Reader");
        continue;
      }
      this.storeSize += r.length();
    }
    return result;
  }

  public ImmutableList<StoreFile> sortAndClone(List<StoreFile> storeFiles) {
    Collections.sort(storeFiles, StoreFile.Comparators.SEQ_ID);
    ImmutableList<StoreFile> newList = ImmutableList.copyOf(storeFiles);
    return newList;
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Accessors.
  // (This is the only section that is directly useful!)
  //////////////////////////////////////////////////////////////////////////////
  /**
   * @return the number of files in this store
   */
  public int getNumberOfStoreFiles() {
    return this.storefiles.size();
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
    int maxVersions = this.family.getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions: wantedVersions;
  }

  static boolean isExpired(final KeyValue key, final long oldestTimestamp) {
    return key.getTimestamp() < oldestTimestamp;
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately
   * precedes it. WARNING: Only use this method on a table where writes occur
   * with strictly increasing timestamps. This method assumes this pattern of
   * writes in order to make it reasonably performant.  Also our search is
   * dependent on the axiom that deletes are for cells that are in the container
   * that follows whether a memstore snapshot or a storefile, not for the
   * current container: i.e. we'll see deletes before we come across cells we
   * are to delete. Presumption is that the memstore#kvset is processed before
   * memstore#snapshot and so on.
   * @param kv First possible item on targeted row; i.e. empty columns, latest
   * timestamp and maximum type.
   * @return Found keyvalue or null if none found.
   * @throws IOException
   */
  KeyValue getRowKeyAtOrBefore(final KeyValue kv) throws IOException {
    GetClosestRowBeforeTracker state = new GetClosestRowBeforeTracker(
      this.comparator, kv, this.ttl, this.region.getRegionInfo().isMetaRegion());
    this.lock.readLock().lock();
    try {
      // First go to the memstore.  Pick up deletes and candidates.
      this.memstore.getRowKeyAtOrBefore(state);
      // Check if match, if we got a candidate on the asked for 'kv' row.
      // Process each store file. Run through from newest to oldest.
      for (StoreFile sf : storefiles.reverse()) {
        // Update the candidate keys from the current map file
        rowAtOrBeforeFromStoreFile(sf, state);
      }
      return state.getCandidate();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * Check an individual MapFile for the row at or before a given row.
   * @param f
   * @param state
   * @throws IOException
   */
  private void rowAtOrBeforeFromStoreFile(final StoreFile f,
                                          final GetClosestRowBeforeTracker state)
      throws IOException {
    StoreFile.Reader r = f.getReader();
    if (r == null) {
      LOG.warn("StoreFile " + f + " has a null Reader");
      return;
    }
    // TODO: Cache these keys rather than make each time?
    byte [] fk = r.getFirstKey();
    KeyValue firstKV = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
    byte [] lk = r.getLastKey();
    KeyValue lastKV = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
    KeyValue firstOnRow = state.getTargetKey();
    if (this.comparator.compareRows(lastKV, firstOnRow) < 0) {
      // If last key in file is not of the target table, no candidates in this
      // file.  Return.
      if (!state.isTargetTable(lastKV)) return;
      // If the row we're looking for is past the end of file, set search key to
      // last key. TODO: Cache last and first key rather than make each time.
      firstOnRow = new KeyValue(lastKV.getRow(), HConstants.LATEST_TIMESTAMP);
    }
    // Get a scanner that caches blocks and that uses pread.
    @SuppressWarnings("deprecation")
    HFileScanner scanner = r.getScanner(true, false, false);
    // Seek scanner.  If can't seek it, return.
    if (!seekToScanner(scanner, firstOnRow, firstKV)) return;
    // If we found candidate on firstOnRow, just return. THIS WILL NEVER HAPPEN!
    // Unlikely that there'll be an instance of actual first row in table.
    if (walkForwardInSingleRow(scanner, firstOnRow, state)) return;
    // If here, need to start backing up.
    while (scanner.seekBefore(firstOnRow.getBuffer(), firstOnRow.getKeyOffset(),
       firstOnRow.getKeyLength())) {
      KeyValue kv = scanner.getKeyValue();
      if (!state.isTargetTable(kv)) break;
      if (!state.isBetterCandidate(kv)) break;
      // Make new first on row.
      firstOnRow = new KeyValue(kv.getRow(), HConstants.LATEST_TIMESTAMP);
      // Seek scanner.  If can't seek it, break.
      if (!seekToScanner(scanner, firstOnRow, firstKV)) break;
      // If we find something, break;
      if (walkForwardInSingleRow(scanner, firstOnRow, state)) break;
    }
  }

  /*
   * Seek the file scanner to firstOnRow or first entry in file.
   * @param scanner
   * @param firstOnRow
   * @param firstKV
   * @return True if we successfully seeked scanner.
   * @throws IOException
   */
  private boolean seekToScanner(final HFileScanner scanner,
                                final KeyValue firstOnRow,
                                final KeyValue firstKV)
      throws IOException {
    KeyValue kv = firstOnRow;
    // If firstOnRow < firstKV, set to firstKV
    if (this.comparator.compareRows(firstKV, firstOnRow) == 0) kv = firstKV;
    int result = scanner.seekTo(kv.getBuffer(), kv.getKeyOffset(),
      kv.getKeyLength());
    return result >= 0;
  }

  /*
   * When we come in here, we are probably at the kv just before we break into
   * the row that firstOnRow is on.  Usually need to increment one time to get
   * on to the row we are interested in.
   * @param scanner
   * @param firstOnRow
   * @param state
   * @return True we found a candidate.
   * @throws IOException
   */
  private boolean walkForwardInSingleRow(final HFileScanner scanner,
                                         final KeyValue firstOnRow,
                                         final GetClosestRowBeforeTracker state)
      throws IOException {
    boolean foundCandidate = false;
    do {
      KeyValue kv = scanner.getKeyValue();
      // If we are not in the row, skip.
      if (this.comparator.compareRows(kv, firstOnRow) < 0) continue;
      // Did we go beyond the target row? If so break.
      if (state.isTooFar(kv, firstOnRow)) break;
      if (state.isExpired(kv)) {
        continue;
      }
      // If we added something, this row is a contender. break.
      if (state.handle(kv)) {
        foundCandidate = true;
        break;
      }
    } while(scanner.next());
    return foundCandidate;
  }

  /**
   * Determines if Store should be split
   * @return byte[] if store should be split, null otherwise.
   */
  public byte[] checkSplit() {
    this.lock.readLock().lock();
    try {
      boolean force = this.region.shouldSplit();
      // sanity checks
      if (!force) {
        if (storeSize < this.desiredMaxFileSize || this.storefiles.isEmpty()) {
          return null;
        }
      }

      if (this.region.getRegionInfo().isMetaRegion()) {
        if (force) {
          LOG.warn("Cannot split meta regions in HBase 0.20");
        }
        return null;
      }

      // Not splitable if we find a reference store file present in the store.
      boolean splitable = true;
      long maxSize = 0L;
      StoreFile largestSf = null;
      for (StoreFile sf : storefiles) {
        if (splitable) {
          splitable = !sf.isReference();
          if (!splitable) {
            // RETURN IN MIDDLE OF FUNCTION!!! If not splitable, just return.
            if (LOG.isDebugEnabled()) {
              LOG.debug(sf +  " is not splittable");
            }
            return null;
          }
        }
        StoreFile.Reader r = sf.getReader();
        if (r == null) {
          LOG.warn("Storefile " + sf + " Reader is null");
          continue;
        }
        long size = r.length();
        if (size > maxSize) {
          // This is the largest one so far
          maxSize = size;
          largestSf = sf;
        }
      }
      // if the user explicit set a split point, use that
      if (this.region.getSplitPoint() != null) {
        return this.region.getSplitPoint();
      }
      StoreFile.Reader r = largestSf.getReader();
      if (r == null) {
        LOG.warn("Storefile " + largestSf + " Reader is null");
        return null;
      }
      // Get first, last, and mid keys.  Midkey is the key that starts block
      // in middle of hfile.  Has column and timestamp.  Need to return just
      // the row we want to split on as midkey.
      byte [] midkey = r.midkey();
      if (midkey != null) {
        KeyValue mk = KeyValue.createKeyValueFromKey(midkey, 0, midkey.length);
        byte [] fk = r.getFirstKey();
        KeyValue firstKey = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
        byte [] lk = r.getLastKey();
        KeyValue lastKey = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region.
        if (this.comparator.compareRows(mk, firstKey) == 0 &&
            this.comparator.compareRows(mk, lastKey) == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot split because midkey is the same as first or " +
              "last row");
          }
          return null;
        }
        return mk.getRow();
      }
    } catch(IOException e) {
      LOG.warn("Failed getting store size for " + this.storeNameStr, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return null;
  }

  /** @return aggregate size of all HStores used in the last compaction */
  public long getLastCompactSize() {
    return this.lastCompactSize;
  }

  /** @return aggregate size of HStore */
  public long getSize() {
    return storeSize;
  }

  public void triggerMajorCompaction() {
    this.forceMajor = true;
  }

  boolean getForceMajorCompaction() {
    return this.forceMajor;
  }

  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memstore and the HStore files. Assumes we
   * are not in a compaction.
   * @throws IOException
   */
  public StoreScanner getScanner(Scan scan,
      final NavigableSet<byte []> targetCols) throws IOException {
    return StoreScanner.createScanner(this, scan, targetCols, getAggregator());
  }

  @Override
  public String toString() {
    return this.storeNameStr;
  }

  /**
   * @return Count of store files
   */
  int getStorefilesCount() {
    return this.storefiles.size();
  }

  /**
   * @return The size of the store files, in bytes.
   */
  long getStorefilesSize() {
    long size = 0;
    for (StoreFile s: storefiles) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      size += r.length();
    }
    return size;
  }

  /**
   * @return The size of the store file indexes, in bytes.
   */
  long getStorefilesIndexSize() {
    long size = 0;
    for (StoreFile s: storefiles) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      size += r.indexSize();
    }
    return size;
  }

  /**
   * Returns the total size of all index blocks in the data block indexes,
   * including the root level, intermediate levels, and the leaf level for
   * multi-level indexes, or just the root level for single-level indexes.
   *
   * @return the total size of block indexes in the store
   */
  long getTotalStaticIndexSize() {
    long size = 0;
    for (StoreFile s : storefiles) {
      size += s.getReader().getUncompressedDataIndexSize();
    }
    return size;
  }

  /**
   * Returns the total byte size of all Bloom filter bit arrays. For compound
   * Bloom filters even the Bloom blocks currently not loaded into the block
   * cache are counted.
   *
   * @return the total size of all Bloom filters in the store
   */
  long getTotalStaticBloomSize() {
    long size = 0;
    for (StoreFile s : storefiles) {
      StoreFile.Reader r = s.getReader();
      size += r.getTotalBloomSize();
    }
    return size;
  }

  /**
   * @return The size of this store's memstore, in bytes
   */
  public long getMemStoreSize() {
    // Use memstore.keySize() instead of heapSize() since heapSize() gives the
    // size of the keys + size of the map.
    return this.memstore.keySize();
  }

  public long getSnapshotSize() {
    return this.memstore.getSnapshotSize();
  }

  public long getFlushableMemstoreSize() {
    return this.memstore.getFlushableSize();
  }

  /**
   * A helper function to get the smallest LSN in the mestore.
   * @return
   */
  public long getSmallestSeqNumberInMemstore() {
    return this.memstore.getSmallestSeqNumber();
  }

  /**
   * @return The priority that this store should have in the compaction queue
   */
  public int getCompactPriority() {
    return this.blockingStoreFileCount - this.storefiles.size();
  }

  boolean throttleCompaction(long compactionSize) {
    return compactionManager.throttleCompaction(compactionSize);
  }

  HRegion getHRegion() {
    return this.region;
  }

  HRegionInfo getHRegionInfo() {
    return this.info;
  }

  /**
   * Increments the value for the given row/family/qualifier.
   *
   * This function will always be seen as atomic by other readers
   * because it only puts a single KV to memstore. Thus no
   * read/write control necessary.
   *
   * @param row
   * @param f
   * @param qualifier
   * @param newValue
   * @param seqNum The LSN associated with the edit.
   * @return
   * @throws IOException
   */
  public long updateColumnValue(byte [] row, byte [] f,
                                byte [] qualifier, long newValue, long seqNum)
      throws IOException {

    this.lock.readLock().lock();
    try {
      long now = System.currentTimeMillis();

      return this.memstore.updateColumnValue(row,
          f,
          qualifier,
          newValue,
          now,
          seqNum);

    } finally {
      this.lock.readLock().unlock();
    }
  }

  public StoreFlusher getStoreFlusher(long cacheFlushId) {
    return new StoreFlusherImpl(cacheFlushId);
  }

  private class CacheFlushInfo {
    Path tmpPath;
    Path dstPath;

    long entries;
    long sequenceId;
    long memSize;
    long fileSize;
  }

  private class StoreFlusherImpl implements StoreFlusher {

    private long cacheFlushId;
    private SortedSet<KeyValue> snapshot;
    private StoreFile storeFile;
    private TimeRangeTracker snapshotTimeRangeTracker;

    CacheFlushInfo cacheFlushInfo;

    private StoreFlusherImpl(long cacheFlushId) {
      this.cacheFlushId = cacheFlushId;
    }

    @Override
    public void prepare() {
      memstore.snapshot();
      this.snapshot = memstore.getSnapshot();
      this.snapshotTimeRangeTracker = memstore.getSnapshotTimeRangeTracker();
      this.cacheFlushInfo = new CacheFlushInfo();
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      storeFile = Store.this.flushCache(cacheFlushId, snapshot,
          snapshotTimeRangeTracker, status, cacheFlushInfo);
    }

    @Override
    public boolean commit() throws IOException {
      if (storeFile == null) {
        return false;
      }
      return Store.this.commit(storeFile, snapshot, cacheFlushInfo);
    }

    @Override
    public void cancel() {
      Store.this.cancel(cacheFlushInfo);
    }
  }

  /**
   * See if there's too much store files in this store
   * @return true if number of store files is greater than
   *  the number defined in minFilesToCompact
   */
  public boolean needsCompaction() {
    return compactionManager.needsCompaction(storefiles.size() - filesCompacting.size());
  }

  /**
   * Used for tests. Get the cache configuration for this Store.
   */
  public CacheConfig getCacheConfig() {
    return this.cacheConf;
  }

  @Override
  public long heapSize() {
    return DEEP_OVERHEAD + this.memstore.heapSize();
  }

  public KeyValue.KVComparator getComparator() {
    return comparator;
  }

  void updateConfiguration() {
    setCompactionPolicy(conf.get(HConstants.COMPACTION_MANAGER_CLASS,
                                 HConstants.DEFAULT_COMPACTION_MANAGER_CLASS));
  }

  /**
   * Gets the aggregator which is specified in the Configuration. If none is
   * specified, returns the DefaultKeyValueAggregator
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  private KeyValueAggregator getAggregator() {
   if (aggregatorClass == null) {
     return DefaultKeyValueAggregator.getInstance();
   } else {
     try {
      return aggregatorClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
   }
  }

  @Override
  public void notifyOnChange(Configuration confParam) {
    // Re-create the CompoundConfiguration in the manner that it is created in
    // the hierarchy. Add the conf first, then add the values from the table
    // descriptor, and then the column family descriptor.
    this.conf = new CompoundConfiguration()
            .add(confParam)
            .add(getHRegionInfo().getTableDesc().getValues())
            .add(family.getValues());

    compactionManager.updateConfiguration(this.conf);
  }
}
