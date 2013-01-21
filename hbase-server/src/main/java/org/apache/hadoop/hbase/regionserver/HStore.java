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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.InvalidHFileException;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.CompactSelection;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
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
//TODO: move StoreConfiguration implementation into a separate class.
@InterfaceAudience.Private
public class HStore implements Store, StoreConfiguration {
  static final Log LOG = LogFactory.getLog(HStore.class);

  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final Path homedir;
  private final HRegion region;
  private final HColumnDescriptor family;
  CompactionPolicy compactionPolicy;
  final FileSystem fs;
  final Configuration conf;
  final CacheConfig cacheConf;
  // ttl in milliseconds. TODO: can this be removed? Already stored in scanInfo.
  private long ttl;
  private long lastCompactSize = 0;
  volatile boolean forceMajor = false;
  /* how many bytes to write between status checks */
  static int closeCheckInterval = 0;
  private final int blockingStoreFileCount;
  private volatile long storeSize = 0L;
  private volatile long totalUncompressedBytes = 0L;
  private final Object flushLock = new Object();
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final boolean verifyBulkLoads;

  // not private for testing
  /* package */ScanInfo scanInfo;
  /*
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  private volatile ImmutableList<StoreFile> storefiles = null;

  final List<StoreFile> filesCompacting = Lists.newArrayList();

  // All access must be synchronized.
  private final CopyOnWriteArraySet<ChangedReadersObserver> changedReaderObservers =
    new CopyOnWriteArraySet<ChangedReadersObserver>();

  private final int blocksize;
  private HFileDataBlockEncoder dataBlockEncoder;

  /** Checksum configuration */
  private ChecksumType checksumType;
  private int bytesPerChecksum;

  // Comparing KeyValues
  final KeyValue.KVComparator comparator;

  private final Compactor compactor;

  private static final int DEFAULT_FLUSH_RETRIES_NUMBER = 10;
  private static int flush_retries_number;
  private static int pauseTime;

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
  protected HStore(Path basedir, HRegion region, HColumnDescriptor family,
      FileSystem fs, Configuration confParam)
  throws IOException {

    HRegionInfo info = region.getRegionInfo();
    this.fs = fs;
    // Assemble the store's home directory.
    Path p = getStoreHomedir(basedir, info.getEncodedName(), family.getName());
    // Ensure it exists.
    this.homedir = createStoreHomeDir(this.fs, p);
    this.region = region;
    this.family = family;
    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.conf = new CompoundConfiguration()
      .add(confParam)
      .addWritableMap(family.getValues());
    this.blocksize = family.getBlocksize();

    this.dataBlockEncoder =
        new HFileDataBlockEncoderImpl(family.getDataBlockEncodingOnDisk(),
            family.getDataBlockEncoding());

    this.comparator = info.getComparator();
    // Get TTL
    this.ttl = determineTTLFromFamily(family);
    // used by ScanQueryMatcher
    long timeToPurgeDeletes =
        Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    LOG.trace("Time to purge deletes set to " + timeToPurgeDeletes +
        "ms in store " + this);
    // Why not just pass a HColumnDescriptor in here altogether?  Even if have
    // to clone it?
    scanInfo = new ScanInfo(family, ttl, timeToPurgeDeletes, this.comparator);
    this.memstore = new MemStore(conf, this.comparator);

    // Setting up cache configuration for this family
    this.cacheConf = new CacheConfig(conf, family);
    this.blockingStoreFileCount =
      conf.getInt("hbase.hstore.blockingStoreFiles", 7);

    this.verifyBulkLoads = conf.getBoolean("hbase.hstore.bulkload.verify", false);

    if (HStore.closeCheckInterval == 0) {
      HStore.closeCheckInterval = conf.getInt(
          "hbase.hstore.close.check.interval", 10*1000*1000 /* 10 MB */);
    }
    this.storefiles = sortAndClone(loadStoreFiles());

    // Initialize checksum type from name. The names are CRC32, CRC32C, etc.
    this.checksumType = getChecksumType(conf);
    // initilize bytes per checksum
    this.bytesPerChecksum = getBytesPerChecksum(conf);
    // Create a compaction tool instance
    this.compactor = new Compactor(conf);
    // Create a compaction manager.
    this.compactionPolicy = new CompactionPolicy(conf, this);
    if (HStore.flush_retries_number == 0) {
      HStore.flush_retries_number = conf.getInt(
          "hbase.hstore.flush.retries.number", DEFAULT_FLUSH_RETRIES_NUMBER);
      HStore.pauseTime = conf.getInt(HConstants.HBASE_SERVER_PAUSE,
          HConstants.DEFAULT_HBASE_SERVER_PAUSE);
      if (HStore.flush_retries_number <= 0) {
        throw new IllegalArgumentException(
            "hbase.hstore.flush.retries.number must be > 0, not "
                + HStore.flush_retries_number);
      }
    }
  }

  /**
   * @param family
   * @return
   */
  private static long determineTTLFromFamily(final HColumnDescriptor family) {
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

  public String getColumnFamilyName() {
    return this.family.getNameAsString();
  }

  @Override
  public String getTableName() {
    return this.region.getTableDesc().getNameAsString();
  }

  /**
   * Create this store's homedir
   * @param fs
   * @param homedir
   * @return Return <code>homedir</code>
   * @throws IOException
   */
  Path createStoreHomeDir(final FileSystem fs,
      final Path homedir) throws IOException {
    if (!fs.exists(homedir)) {
      if (!fs.mkdirs(homedir))
        throw new IOException("Failed create of: " + homedir.toString());
    }
    return homedir;
  }

  FileSystem getFileSystem() {
    return this.fs;
  }

  /* Implementation of StoreConfiguration */
  public long getStoreFileTtl() {
    // TTL only applies if there's no MIN_VERSIONs setting on the column.
    return (this.scanInfo.getMinVersions() == 0) ? this.ttl : Long.MAX_VALUE;
  }

  public Long getMajorCompactionPeriod() {
    String strCompactionTime = this.family.getValue(HConstants.MAJOR_COMPACTION_PERIOD);
    return (strCompactionTime != null) ? new Long(strCompactionTime) : null;
  }

  public long getMemstoreFlushSize() {
    return this.region.memstoreFlushSize;
  }
  /* End implementation of StoreConfiguration */

  /**
   * Returns the configured bytesPerChecksum value.
   * @param conf The configuration
   * @return The bytesPerChecksum that is set in the configuration
   */
  public static int getBytesPerChecksum(Configuration conf) {
    return conf.getInt(HConstants.BYTES_PER_CHECKSUM,
                       HFile.DEFAULT_BYTES_PER_CHECKSUM);
  }

  /**
   * Returns the configured checksum algorithm.
   * @param conf The configuration
   * @return The checksum algorithm that is set in the configuration
   */
  public static ChecksumType getChecksumType(Configuration conf) {
    String checksumName = conf.get(HConstants.CHECKSUM_TYPE_NAME);
    if (checksumName == null) {
      return HFile.DEFAULT_CHECKSUM_TYPE;
    } else {
      return ChecksumType.nameToType(checksumName);
    }
  }

  public HColumnDescriptor getFamily() {
    return this.family;
  }

  /**
   * @return The maximum sequence id in all store files.
   */
  long getMaxSequenceId(boolean includeBulkFiles) {
    return StoreFile.getMaxSequenceIdInList(this.getStorefiles(), includeBulkFiles);
  }

  @Override
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
    return getStoreHomedir(tabledir, encodedName, Bytes.toString(family));
  }

  /**
   * @param tabledir
   * @param encodedName Encoded region name.
   * @param family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir,
      final String encodedName, final String family) {
    return new Path(tabledir, new Path(encodedName, new Path(family)));
  }

  /**
   * @param parentRegionDirectory directory for the parent region
   * @param family family name of this store
   * @return Path to the family/Store home directory
   */
  public static Path getStoreHomedir(final Path parentRegionDirectory,
      final byte[] family) {
    return new Path(parentRegionDirectory, new Path(Bytes.toString(family)));
  }
  /**
   * Return the directory in which this store stores its
   * StoreFiles
   */
  Path getHomedir() {
    return homedir;
  }

  @Override
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

  FileStatus[] getStoreFiles() throws IOException {
    return FSUtils.listStatus(this.fs, this.homedir, null);
  }

  /**
   * Creates an unsorted list of StoreFile loaded in parallel
   * from the given directory.
   * @throws IOException
   */
  private List<StoreFile> loadStoreFiles() throws IOException {
    ArrayList<StoreFile> results = new ArrayList<StoreFile>();
    FileStatus files[] = getStoreFiles();

    if (files == null || files.length == 0) {
      return results;
    }
    // initialize the thread pool for opening store files in parallel..
    ThreadPoolExecutor storeFileOpenerThreadPool =
      this.region.getStoreFileOpenAndCloseThreadPool("StoreFileOpenerThread-" +
          this.family.getNameAsString());
    CompletionService<StoreFile> completionService =
      new ExecutorCompletionService<StoreFile>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (int i = 0; i < files.length; i++) {
      // Skip directories.
      if (files[i].isDir()) {
        continue;
      }
      final Path p = files[i].getPath();
      // Check for empty hfile. Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      // NOTE: that the HFileLink is just a name, so it's an empty file.
      if (!HFileLink.isHFileLink(p) && this.fs.getFileStatus(p).getLen() <= 0) {
        LOG.warn("Skipping " + p + " because its empty. HBASE-646 DATA LOSS?");
        continue;
      }

      // open each store file in parallel
      completionService.submit(new Callable<StoreFile>() {
        public StoreFile call() throws IOException {
          StoreFile storeFile = new StoreFile(fs, p, conf, cacheConf,
              family.getBloomFilterType(), dataBlockEncoder);
          storeFile.createReader();
          return storeFile;
        }
      });
      totalValidStoreFile++;
    }

    IOException ioe = null;
    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        try {
          Future<StoreFile> future = completionService.take();
          StoreFile storeFile = future.get();
          long length = storeFile.getReader().length();
          this.storeSize += length;
          this.totalUncompressedBytes +=
              storeFile.getReader().getTotalUncompressedBytes();
          if (LOG.isDebugEnabled()) {
            LOG.debug("loaded " + storeFile.toStringDetailed());
          }
          results.add(storeFile);
        } catch (InterruptedException e) {
          if (ioe == null) ioe = new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
          if (ioe == null) ioe = new IOException(e.getCause());
        } 
      } 
    } finally {
      storeFileOpenerThreadPool.shutdownNow();
    }
    if (ioe != null) {
      // close StoreFile readers
      try {
        for (StoreFile file : results) {
          if (file != null) file.closeReader(true);
        }
      } catch (IOException e) { }
      throw ioe;
    }

    return results;
  }

  @Override
  public long add(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.add(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds a value to the memstore
   *
   * @param kv
   * @return memstore size delta
   */
  protected long delete(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.delete(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void rollback(final KeyValue kv) {
    lock.readLock().lock();
    try {
      this.memstore.rollback(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return All store files.
   */
  @Override
  public List<StoreFile> getStorefiles() {
    return this.storefiles;
  }

  @Override
  public void assertBulkLoadHFileOk(Path srcPath) throws IOException {
    HFile.Reader reader  = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in "
          + "store " + this + " region " + this.region);
      reader = HFile.createReader(srcPath.getFileSystem(conf),
          srcPath, cacheConf);
      reader.loadFileInfo();

      byte[] firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey != null, "First key can not be null");
      byte[] lk = reader.getLastKey();
      Preconditions.checkState(lk != null, "Last key can not be null");
      byte[] lastKey =  KeyValue.createKeyValueFromKey(lk).getRow();

      LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey) +
          " last=" + Bytes.toStringBinary(lastKey));
      LOG.debug("Region bounds: first=" +
          Bytes.toStringBinary(region.getStartKey()) +
          " last=" + Bytes.toStringBinary(region.getEndKey()));

      HRegionInfo hri = region.getRegionInfo();
      if (!hri.containsRange(firstKey, lastKey)) {
        throw new WrongRegionException(
            "Bulk load file " + srcPath.toString() + " does not fit inside region "
            + this.region);
      }

      if (verifyBulkLoads) {
        KeyValue prevKV = null;
        HFileScanner scanner = reader.getScanner(false, false, false);
        scanner.seekTo();
        do {
          KeyValue kv = scanner.getKeyValue();
          if (prevKV != null) {
            if (Bytes.compareTo(prevKV.getBuffer(), prevKV.getRowOffset(),
                prevKV.getRowLength(), kv.getBuffer(), kv.getRowOffset(),
                kv.getRowLength()) > 0) {
              throw new InvalidHFileException("Previous row is greater than"
                  + " current row: path=" + srcPath + " previous="
                  + Bytes.toStringBinary(prevKV.getKey()) + " current="
                  + Bytes.toStringBinary(kv.getKey()));
            }
            if (Bytes.compareTo(prevKV.getBuffer(), prevKV.getFamilyOffset(),
                prevKV.getFamilyLength(), kv.getBuffer(), kv.getFamilyOffset(),
                kv.getFamilyLength()) != 0) {
              throw new InvalidHFileException("Previous key had different"
                  + " family compared to current key: path=" + srcPath
                  + " previous=" + Bytes.toStringBinary(prevKV.getFamily())
                  + " current=" + Bytes.toStringBinary(kv.getFamily()));
            }
          }
          prevKV = kv;
        } while (scanner.next());
      }
    } finally {
      if (reader != null) reader.close();
    }
  }

  @Override
  public void bulkLoadHFile(String srcPathStr, long seqNum) throws IOException {
    Path srcPath = new Path(srcPathStr);

    // Copy the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem)fs).getBackingFs() : fs;
    //We can't compare FileSystem instances as
    //equals() includes UGI instance as part of the comparison
    //and won't work when doing SecureBulkLoad
    //TODO deal with viewFS
    if (!srcFs.getUri().equals(desFs.getUri())) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than " +
          "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = getTmpPath();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied " + srcPath
          + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    Path dstPath = StoreFile.getRandomFilename(fs, homedir,
        (seqNum == -1) ? null : "_SeqId_" + seqNum + "_");
    LOG.debug("Renaming bulk load file " + srcPath + " to " + dstPath);
    StoreFile.rename(fs, srcPath, dstPath);

    StoreFile sf = new StoreFile(fs, dstPath, this.conf, this.cacheConf,
        this.family.getBloomFilterType(), this.dataBlockEncoder);

    StoreFile.Reader r = sf.createReader();
    this.storeSize += r.length();
    this.totalUncompressedBytes += r.getTotalUncompressedBytes();

    LOG.info("Moved HFile " + srcPath + " into store directory " +
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

  @Override
  public ImmutableList<StoreFile> close() throws IOException {
    this.lock.writeLock().lock();
    try {
      ImmutableList<StoreFile> result = storefiles;

      // Clear so metrics doesn't find them.
      storefiles = ImmutableList.of();

      if (!result.isEmpty()) {
        // initialize the thread pool for closing store files in parallel.
        ThreadPoolExecutor storeFileCloserThreadPool = this.region
            .getStoreFileOpenAndCloseThreadPool("StoreFileCloserThread-"
                + this.family.getNameAsString());

        // close each store file in parallel
        CompletionService<Void> completionService =
          new ExecutorCompletionService<Void>(storeFileCloserThreadPool);
        for (final StoreFile f : result) {
          completionService.submit(new Callable<Void>() {
            public Void call() throws IOException {
              f.closeReader(true);
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
              if (ioe == null) ioe = new IOException(e.getCause());
            }
          }
        } finally {
          storeFileCloserThreadPool.shutdownNow();
        }
        if (ioe != null) throw ioe;
      }
      LOG.info("Closed " + this);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Snapshot this stores memstore. Call before running
   * {@link #flushCache(long, SortedSet, TimeRangeTracker, AtomicLong, MonitoredTask)}
   *  so it has some work to do.
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
   * @param flushedSize The number of bytes flushed
   * @param status
   * @return Path The path name of the tmp file to which the store was flushed
   * @throws IOException
   */
  private Path flushCache(final long logCacheFlushId,
      SortedSet<KeyValue> snapshot,
      TimeRangeTracker snapshotTimeRangeTracker,
      AtomicLong flushedSize,
      MonitoredTask status) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    // Retry after catching exception when flushing, otherwise server will abort
    // itself
    IOException lastException = null;
    for (int i = 0; i < HStore.flush_retries_number; i++) {
      try {
        Path pathName = internalFlushCache(snapshot, logCacheFlushId,
            snapshotTimeRangeTracker, flushedSize, status);
        try {
          // Path name is null if there is no entry to flush
          if (pathName != null) {
            validateStoreFile(pathName);
          }
          return pathName;
        } catch (Exception e) {
          LOG.warn("Failed validating store file " + pathName
              + ", retring num=" + i, e);
          if (e instanceof IOException) {
            lastException = (IOException) e;
          } else {
            lastException = new IOException(e);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed flushing store file, retring num=" + i, e);
        lastException = e;
      }
      if (lastException != null) {
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

  /*
   * @param cache
   * @param logCacheFlushId
   * @param snapshotTimeRangeTracker
   * @param flushedSize The number of bytes flushed
   * @return Path The path name of the tmp file to which the store was flushed
   * @throws IOException
   */
  private Path internalFlushCache(final SortedSet<KeyValue> set,
      final long logCacheFlushId,
      TimeRangeTracker snapshotTimeRangeTracker,
      AtomicLong flushedSize,
      MonitoredTask status)
      throws IOException {
    StoreFile.Writer writer;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = region.getSmallestReadPoint();
    long flushed = 0;
    Path pathName;
    // Don't flush if there are no entries.
    if (set.size() == 0) {
      return null;
    }
    // Use a store scanner to find which rows to flush.
    // Note that we need to retain deletes, hence
    // treat this as a minor compaction.
    InternalScanner scanner = null;
    KeyValueScanner memstoreScanner = new CollectionBackedScanner(set, this.comparator);
    if (getHRegion().getCoprocessorHost() != null) {
      scanner = getHRegion().getCoprocessorHost()
          .preFlushScannerOpen(this, memstoreScanner);
    }
    if (scanner == null) {
      Scan scan = new Scan();
      scan.setMaxVersions(scanInfo.getMaxVersions());
      scanner = new StoreScanner(this, scanInfo, scan,
          Collections.singletonList(memstoreScanner), ScanType.MINOR_COMPACT,
          this.region.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
    }
    if (getHRegion().getCoprocessorHost() != null) {
      InternalScanner cpScanner =
        getHRegion().getCoprocessorHost().preFlush(this, scanner);
      // NULL scanner returned from coprocessor hooks means skip normal processing
      if (cpScanner == null) {
        return null;
      }
      scanner = cpScanner;
    }
    try {
      int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX, 10);
      // TODO:  We can fail in the below block before we complete adding this
      // flush to list of store files.  Add cleanup of anything put on filesystem
      // if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + this + ": creating writer");
        // A. Write the map out to the disk
        writer = createWriterInTmp(set.size());
        writer.setTimeRangeTracker(snapshotTimeRangeTracker);
        pathName = writer.getPath();
        try {
          List<KeyValue> kvs = new ArrayList<KeyValue>();
          boolean hasMore;
          do {
            hasMore = scanner.next(kvs, compactionKVMax);
            if (!kvs.isEmpty()) {
              for (KeyValue kv : kvs) {
                // If we know that this KV is going to be included always, then let us
                // set its memstoreTS to 0. This will help us save space when writing to
                // disk.
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
          // hfile. Also write current time in metadata as minFlushTime.
          // The hfile is current up to and including logCacheFlushId.
          status.setStatus("Flushing " + this + ": appending metadata");
          writer.appendMetadata(logCacheFlushId, false);
          status.setStatus("Flushing " + this + ": closing flushed file");
          writer.close();
        }
      }
    } finally {
      flushedSize.set(flushed);
      scanner.close();
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Flushed " +
               ", sequenceid=" + logCacheFlushId +
               ", memsize=" + StringUtils.humanReadableInt(flushed) +
               ", into tmp file " + pathName);
    }
    return pathName;
  }

  /*
   * @param path The pathname of the tmp file into which the store was flushed
   * @param logCacheFlushId
   * @return StoreFile created.
   * @throws IOException
   */
  private StoreFile commitFile(final Path path,
      final long logCacheFlushId,
      TimeRangeTracker snapshotTimeRangeTracker,
      AtomicLong flushedSize,
      MonitoredTask status)
      throws IOException {
    // Write-out finished successfully, move into the right spot
    String fileName = path.getName();
    Path dstPath = new Path(homedir, fileName);
    String msg = "Renaming flushed file at " + path + " to " + dstPath;
    LOG.debug(msg);
    status.setStatus("Flushing " + this + ": " + msg);
    if (!fs.rename(path, dstPath)) {
      LOG.warn("Unable to rename " + path + " to " + dstPath);
    }

    status.setStatus("Flushing " + this + ": reopening flushed file");
    StoreFile sf = new StoreFile(this.fs, dstPath, this.conf, this.cacheConf,
        this.family.getBloomFilterType(), this.dataBlockEncoder);

    StoreFile.Reader r = sf.createReader();
    this.storeSize += r.length();
    this.totalUncompressedBytes += r.getTotalUncompressedBytes();

    if (LOG.isInfoEnabled()) {
      LOG.info("Added " + sf + ", entries=" + r.getEntries() +
        ", sequenceid=" + logCacheFlushId +
        ", filesize=" + StringUtils.humanReadableInt(r.length()));
    }
    return sf;
  }

  /*
   * @param maxKeyCount
   * @return Writer for a new StoreFile in the tmp dir.
   */
  private StoreFile.Writer createWriterInTmp(int maxKeyCount)
  throws IOException {
    return createWriterInTmp(maxKeyCount, this.family.getCompression(), false);
  }

  /*
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @return Writer for a new StoreFile in the tmp dir.
   */
  StoreFile.Writer createWriterInTmp(int maxKeyCount,
    Compression.Algorithm compression, boolean isCompaction)
  throws IOException {
    final CacheConfig writerCacheConf;
    if (isCompaction) {
      // Don't cache data on write on compactions.
      writerCacheConf = new CacheConfig(cacheConf);
      writerCacheConf.setCacheDataOnWrite(false);
    } else {
      writerCacheConf = cacheConf;
    }
    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf,
        fs, blocksize)
            .withOutputDir(region.getTmpDir())
            .withDataBlockEncoder(dataBlockEncoder)
            .withComparator(comparator)
            .withBloomType(family.getBloomFilterType())
            .withMaxKeyCount(maxKeyCount)
            .withChecksumType(checksumType)
            .withBytesPerChecksum(bytesPerChecksum)
            .withCompression(compression)
            .build();
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
   * @return all scanners for this store
   */
  protected List<KeyValueScanner> getScanners(boolean cacheBlocks,
      boolean isGet,
      boolean isCompaction,
      ScanQueryMatcher matcher) throws IOException {
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
    List<StoreFileScanner> sfScanners = StoreFileScanner
      .getScannersForStoreFiles(storeFiles, cacheBlocks, isGet, isCompaction, matcher);
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
   * @param cr
   *          compaction details obtained from requestCompaction()
   * @throws IOException
   * @return Storefile we compacted into or null if we failed or opted out early.
   */
  StoreFile compact(CompactionRequest cr) throws IOException {
    if (cr == null || cr.getFiles().isEmpty()) return null;
    Preconditions.checkArgument(cr.getStore().toString().equals(this.toString()));
    List<StoreFile> filesToCompact = cr.getFiles();
    synchronized (filesCompacting) {
      // sanity check: we're compacting files that this store knows about
      // TODO: change this to LOG.error() after more debugging
      Preconditions.checkArgument(filesCompacting.containsAll(filesToCompact));
    }

    // Max-sequenceID is the last key in the files we're compacting
    long maxId = StoreFile.getMaxSequenceIdInList(filesToCompact, true);

    // Ready to go. Have list of files to compact.
    LOG.info("Starting compaction of " + filesToCompact.size() + " file(s) in "
        + this + " of " + this.region.getRegionInfo().getRegionNameAsString()
        + " into tmpdir=" + region.getTmpDir() + ", seqid=" + maxId + ", totalSize="
        + StringUtils.humanReadableInt(cr.getSize()));

    StoreFile sf = null;
    long compactionStartTime = EnvironmentEdgeManager.currentTimeMillis();
    try {
      StoreFile.Writer writer =
        this.compactor.compact(this, filesToCompact, cr.isMajor(), maxId);
      // Move the compaction into place.
      if (this.conf.getBoolean("hbase.hstore.compaction.complete", true)) {
        sf = completeCompaction(filesToCompact, writer);
        if (region.getCoprocessorHost() != null) {
          region.getCoprocessorHost().postCompact(this, sf);
        }
      } else {
        // Create storefile around what we wrote with a reader on it.
        sf = new StoreFile(this.fs, writer.getPath(), this.conf, this.cacheConf,
          this.family.getBloomFilterType(), this.dataBlockEncoder);
        sf.createReader();
      }
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
    }

    long now = EnvironmentEdgeManager.currentTimeMillis();
    LOG.info("Completed" + (cr.isMajor() ? " major " : " ") + "compaction of "
        + filesToCompact.size() + " file(s) in " + this + " of "
        + this.region.getRegionInfo().getRegionNameAsString()
        + " into " +
        (sf == null ? "none" : sf.getPath().getName()) +
        ", size=" + (sf == null ? "none" :
          StringUtils.humanReadableInt(sf.getReader().length()))
        + "; total size for store is " + StringUtils.humanReadableInt(storeSize)
        + ". This selection was in queue for "
        + StringUtils.formatTimeDiff(compactionStartTime, cr.getSelectionTime())
        + ", and took " + StringUtils.formatTimeDiff(now, compactionStartTime)
        + " to execute.");
    return sf;
  }

  @Override
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
        if (N > count) {
          throw new RuntimeException("Not enough files");
        }

        filesToCompact = filesToCompact.subList(count - N, count);
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
      StoreFile.Writer writer =
        this.compactor.compact(this, filesToCompact, isMajor, maxId);
      // Move the compaction into place.
      StoreFile sf = completeCompaction(filesToCompact, writer);
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postCompact(this, sf);
      }
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
    }
  }

  @Override
  public boolean hasReferences() {
    return StoreUtils.hasReferences(this.storefiles);
  }

  @Override
  public CompactionProgress getCompactionProgress() {
    return this.compactor.getProgress();
  }

  @Override
  public boolean isMajorCompaction() throws IOException {
    for (StoreFile sf : this.storefiles) {
      if (sf.getReader() == null) {
        LOG.debug("StoreFile " + sf + " has null Reader");
        return false;
      }
    }

    List<StoreFile> candidates = new ArrayList<StoreFile>(this.storefiles);
    return compactionPolicy.isMajorCompaction(candidates);
  }

  public CompactionRequest requestCompaction() throws IOException {
    return requestCompaction(Store.NO_PRIORITY);
  }

  public CompactionRequest requestCompaction(int priority) throws IOException {
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

        boolean override = false;
        if (region.getCoprocessorHost() != null) {
          override = region.getCoprocessorHost().preCompactSelection(
              this, candidates);
        }
        CompactSelection filesToCompact;
        if (override) {
          // coprocessor is overriding normal file selection
          filesToCompact = new CompactSelection(candidates);
        } else {
          boolean isUserCompaction = priority == Store.PRIORITY_USER;
          filesToCompact = compactionPolicy.selectCompaction(candidates, isUserCompaction,
              forceMajor && filesCompacting.isEmpty());
        }

        if (region.getCoprocessorHost() != null) {
          region.getCoprocessorHost().postCompactSelection(this,
              ImmutableList.copyOf(filesToCompact.getFilesToCompact()));
        }

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
        boolean isMajor =
            (filesToCompact.getFilesToCompact().size() == this.storefiles.size());
        if (isMajor) {
          // since we're enqueuing a major, update the compaction wait interval
          this.forceMajor = false;
        }

        LOG.debug(getHRegionInfo().getEncodedName() + " - " +
            getColumnFamilyName() + ": Initiating " +
            (isMajor ? "major" : "minor") + " compaction");

        // everything went better than expected. create a compaction request
        int pri = getCompactPriority(priority);
        ret = new CompactionRequest(region, this, filesToCompact, isMajor, pri);
      }
    } finally {
      this.lock.readLock().unlock();
    }
    if (ret != null) {
      CompactionRequest.preRequest(ret);
    }
    return ret;
  }

  public void finishRequest(CompactionRequest cr) {
    CompactionRequest.postRequest(cr);
    cr.finishRequest();
    synchronized (filesCompacting) {
      filesCompacting.removeAll(cr.getFiles());
    }
  }

  /**
   * Validates a store file by opening and closing it. In HFileV2 this should
   * not be an expensive operation.
   *
   * @param path the path to the store file
   */
  private void validateStoreFile(Path path)
      throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(this.fs, path, this.conf,
          this.cacheConf, this.family.getBloomFilterType(),
          NoOpDataBlockEncoder.INSTANCE);
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
      validateStoreFile(compactedFile.getPath());
      // Move the file into the right spot
      Path origPath = compactedFile.getPath();
      Path destPath = new Path(homedir, origPath.getName());
      LOG.info("Renaming compacted file at " + origPath + " to " + destPath);
      if (!fs.rename(origPath, destPath)) {
        LOG.error("Failed move of compacted file " + origPath + " to " +
            destPath);
        throw new IOException("Failed move of compacted file " + origPath +
            " to " + destPath);
      }
      result = new StoreFile(this.fs, destPath, this.conf, this.cacheConf,
          this.family.getBloomFilterType(), this.dataBlockEncoder);
      result.createReader();
    }
    try {
      this.lock.writeLock().lock();
      try {
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

      // let the archive util decide if we should archive or delete the files
      LOG.debug("Removing store files after compaction...");
      HFileArchiver.archiveStoreFiles(this.conf, this.fs, this.region,
        this.family.getName(), compactedFiles);

    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("Failed replacing compacted files in " + this +
        ". Compacted file is " + (result == null? "none": result.toString()) +
        ".  Files replaced " + compactedFiles.toString() +
        " some of which may have been already removed", e);
    }

    // 4. Compute new store size
    this.storeSize = 0L;
    this.totalUncompressedBytes = 0L;
    for (StoreFile hsf : this.storefiles) {
      StoreFile.Reader r = hsf.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + hsf + " has a null Reader");
        continue;
      }
      this.storeSize += r.length();
      this.totalUncompressedBytes += r.getTotalUncompressedBytes();
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
  @Override
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

  @Override
  public KeyValue getRowKeyAtOrBefore(final byte[] row) throws IOException {
    // If minVersions is set, we will not ignore expired KVs.
    // As we're only looking for the latest matches, that should be OK.
    // With minVersions > 0 we guarantee that any KV that has any version
    // at all (expired or not) has at least one version that will not expire.
    // Note that this method used to take a KeyValue as arguments. KeyValue
    // can be back-dated, a row key cannot.
    long ttlToUse = scanInfo.getMinVersions() > 0 ? Long.MAX_VALUE : this.ttl;

    KeyValue kv = new KeyValue(row, HConstants.LATEST_TIMESTAMP);

    GetClosestRowBeforeTracker state = new GetClosestRowBeforeTracker(
      this.comparator, kv, ttlToUse, this.region.getRegionInfo().isMetaRegion());
    this.lock.readLock().lock();
    try {
      // First go to the memstore.  Pick up deletes and candidates.
      this.memstore.getRowKeyAtOrBefore(state);
      // Check if match, if we got a candidate on the asked for 'kv' row.
      // Process each store file. Run through from newest to oldest.
      for (StoreFile sf : Lists.reverse(storefiles)) {
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
    if (r.getEntries() == 0) {
      LOG.warn("StoreFile " + f + " is a empty store file");
      return;
    }
    // TODO: Cache these keys rather than make each time?
    byte [] fk = r.getFirstKey();
    if (fk == null) return;
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
    HFileScanner scanner = r.getScanner(true, true, false);
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

  public boolean canSplit() {
    this.lock.readLock().lock();
    try {
      // Not splitable if we find a reference store file present in the store.
      for (StoreFile sf : storefiles) {
        if (sf.isReference()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(sf + " is not splittable");
          }
          return false;
        }
      }

      return true;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public byte[] getSplitPoint() {
    this.lock.readLock().lock();
    try {
      // sanity checks
      if (this.storefiles.isEmpty()) {
        return null;
      }
      // Should already be enforced by the split policy!
      assert !this.region.getRegionInfo().isMetaRegion();

      // Not splitable if we find a reference store file present in the store.
      long maxSize = 0L;
      StoreFile largestSf = null;
      for (StoreFile sf : storefiles) {
        if (sf.isReference()) {
          // Should already be enforced since we return false in this case
          assert false : "getSplitPoint() called on a region that can't split!";
          return null;
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
        // if the midkey is the same as the first or last keys, then we cannot
        // (ever) split this region.
        if (this.comparator.compareRows(mk, firstKey) == 0 ||
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
      LOG.warn("Failed getting store size for " + this, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return null;
  }

  @Override
  public long getLastCompactSize() {
    return this.lastCompactSize;
  }

  @Override
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

  @Override
  public KeyValueScanner getScanner(Scan scan,
      final NavigableSet<byte []> targetCols) throws IOException {
    lock.readLock().lock();
    try {
      KeyValueScanner scanner = null;
      if (getHRegion().getCoprocessorHost() != null) {
        scanner = getHRegion().getCoprocessorHost().preStoreScannerOpen(this, scan, targetCols);
      }
      if (scanner == null) {
        scanner = new StoreScanner(this, getScanInfo(), scan, targetCols);
      }
      return scanner;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return this.getColumnFamilyName();
  }

  @Override
  public int getStorefilesCount() {
    return this.storefiles.size();
  }

  @Override
  public long getStoreSizeUncompressed() {
    return this.totalUncompressedBytes;
  }

  @Override
  public long getStorefilesSize() {
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

  @Override
  public long getStorefilesIndexSize() {
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

  @Override
  public long getTotalStaticIndexSize() {
    long size = 0;
    for (StoreFile s : storefiles) {
      size += s.getReader().getUncompressedDataIndexSize();
    }
    return size;
  }

  @Override
  public long getTotalStaticBloomSize() {
    long size = 0;
    for (StoreFile s : storefiles) {
      StoreFile.Reader r = s.getReader();
      size += r.getTotalBloomSize();
    }
    return size;
  }

  @Override
  public long getMemStoreSize() {
    return this.memstore.heapSize();
  }

  public int getCompactPriority() {
    return getCompactPriority(Store.NO_PRIORITY);
  }

  @Override
  public int getCompactPriority(int priority) {
    // If this is a user-requested compaction, leave this at the highest priority
    if(priority == Store.PRIORITY_USER) {
      return Store.PRIORITY_USER;
    } else {
      return this.blockingStoreFileCount - this.storefiles.size();
    }
  }

  @Override
  public boolean throttleCompaction(long compactionSize) {
    return compactionPolicy.throttleCompaction(compactionSize);
  }

  @Override
  public HRegion getHRegion() {
    return this.region;
  }

  HRegionInfo getHRegionInfo() {
    return this.region.getRegionInfo();
  }

  /**
   * Used in tests. TODO: Remove
   *
   * Updates the value for the given row/family/qualifier. This function will always be seen as
   * atomic by other readers because it only puts a single KV to memstore. Thus no read/write
   * control necessary.
   * @param row row to update
   * @param f family to update
   * @param qualifier qualifier to update
   * @param newValue the new value to set into memstore
   * @return memstore size delta
   * @throws IOException
   */
  public long updateColumnValue(byte [] row, byte [] f,
                                byte [] qualifier, long newValue)
      throws IOException {

    this.lock.readLock().lock();
    try {
      long now = EnvironmentEdgeManager.currentTimeMillis();

      return this.memstore.updateColumnValue(row,
          f,
          qualifier,
          newValue,
          now);

    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public long upsert(Iterable<KeyValue> kvs, long readpoint) throws IOException {
    this.lock.readLock().lock();
    try {
      return this.memstore.upsert(kvs, readpoint);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public StoreFlusher getStoreFlusher(long cacheFlushId) {
    return new StoreFlusherImpl(cacheFlushId);
  }

  private class StoreFlusherImpl implements StoreFlusher {

    private long cacheFlushId;
    private SortedSet<KeyValue> snapshot;
    private StoreFile storeFile;
    private Path storeFilePath;
    private TimeRangeTracker snapshotTimeRangeTracker;
    private AtomicLong flushedSize;

    private StoreFlusherImpl(long cacheFlushId) {
      this.cacheFlushId = cacheFlushId;
      this.flushedSize = new AtomicLong();
    }

    @Override
    public void prepare() {
      memstore.snapshot();
      this.snapshot = memstore.getSnapshot();
      this.snapshotTimeRangeTracker = memstore.getSnapshotTimeRangeTracker();
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      storeFilePath = HStore.this.flushCache(
        cacheFlushId, snapshot, snapshotTimeRangeTracker, flushedSize, status);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      if (storeFilePath == null) {
        return false;
      }
      storeFile = HStore.this.commitFile(storeFilePath, cacheFlushId,
                               snapshotTimeRangeTracker, flushedSize, status);
      if (HStore.this.getHRegion().getCoprocessorHost() != null) {
        HStore.this.getHRegion()
            .getCoprocessorHost()
            .postFlush(HStore.this, storeFile);
      }

      // Add new file to store files.  Clear snapshot too while we have
      // the Store write lock.
      return HStore.this.updateStorefiles(storeFile, snapshot);
    }
  }

  @Override
  public boolean needsCompaction() {
    return compactionPolicy.needsCompaction(storefiles.size() - filesCompacting.size());
  }

  @Override
  public CacheConfig getCacheConfig() {
    return this.cacheConf;
  }

  public static final long FIXED_OVERHEAD =
      ClassSize.align((20 * ClassSize.REFERENCE) + (4 * Bytes.SIZEOF_LONG)
              + (3 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_BOOLEAN);

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD
      + ClassSize.OBJECT + ClassSize.REENTRANT_LOCK
      + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT
      + ScanInfo.FIXED_OVERHEAD);

  @Override
  public long heapSize() {
    return DEEP_OVERHEAD + this.memstore.heapSize();
  }

  public KeyValue.KVComparator getComparator() {
    return comparator;
  }

  public ScanInfo getScanInfo() {
    return scanInfo;
  }

  /**
   * Immutable information for scans over a store.
   */
  public static class ScanInfo {
    private byte[] family;
    private int minVersions;
    private int maxVersions;
    private long ttl;
    private boolean keepDeletedCells;
    private long timeToPurgeDeletes;
    private KVComparator comparator;

    public static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (2 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_INT)
        + Bytes.SIZEOF_LONG + Bytes.SIZEOF_BOOLEAN);

    /**
     * @param family {@link HColumnDescriptor} describing the column family
     * @param ttl Store's TTL (in ms)
     * @param timeToPurgeDeletes duration in ms after which a delete marker can
     *        be purged during a major compaction.
     * @param comparator The store's comparator
     */
    public ScanInfo(HColumnDescriptor family, long ttl, long timeToPurgeDeletes, KVComparator comparator) {
      this(family.getName(), family.getMinVersions(), family.getMaxVersions(), ttl, family
          .getKeepDeletedCells(), timeToPurgeDeletes, comparator);
    }
    /**
     * @param family Name of this store's column family
     * @param minVersions Store's MIN_VERSIONS setting
     * @param maxVersions Store's VERSIONS setting
     * @param ttl Store's TTL (in ms)
     * @param timeToPurgeDeletes duration in ms after which a delete marker can
     *        be purged during a major compaction.
     * @param keepDeletedCells Store's keepDeletedCells setting
     * @param comparator The store's comparator
     */
    public ScanInfo(byte[] family, int minVersions, int maxVersions, long ttl,
        boolean keepDeletedCells, long timeToPurgeDeletes,
        KVComparator comparator) {

      this.family = family;
      this.minVersions = minVersions;
      this.maxVersions = maxVersions;
      this.ttl = ttl;
      this.keepDeletedCells = keepDeletedCells;
      this.timeToPurgeDeletes = timeToPurgeDeletes;
      this.comparator = comparator;
    }

    public byte[] getFamily() {
      return family;
    }

    public int getMinVersions() {
      return minVersions;
    }

    public int getMaxVersions() {
      return maxVersions;
    }

    public long getTtl() {
      return ttl;
    }

    public boolean getKeepDeletedCells() {
      return keepDeletedCells;
    }

    public long getTimeToPurgeDeletes() {
      return timeToPurgeDeletes;
    }

    public KVComparator getComparator() {
      return comparator;
    }
  }

}
