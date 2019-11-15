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
import java.net.InetSocketAddress;
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.backup.FailedArchiveException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.InvalidHFileException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
public class HStore implements Store {
  private static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";
  public static final String COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY =
      "hbase.server.compactchecker.interval.multiplier";
  public static final String BLOCKING_STOREFILES_KEY = "hbase.hstore.blockingStoreFiles";
  public static final String BLOCK_STORAGE_POLICY_KEY = "hbase.hstore.block.storage.policy";
  // keep in accordance with HDFS default storage policy
  public static final String DEFAULT_BLOCK_STORAGE_POLICY = "HOT";
  public static final int DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER = 1000;
  public static final int DEFAULT_BLOCKING_STOREFILE_COUNT = 7;

  private static final Log LOG = LogFactory.getLog(HStore.class);

  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final HRegion region;
  private final HColumnDescriptor family;
  private final HRegionFileSystem fs;
  private Configuration conf;
  private final CacheConfig cacheConf;
  private long lastCompactSize = 0;
  volatile boolean forceMajor = false;
  /* how many bytes to write between status checks */
  static int closeCheckInterval = 0;
  private AtomicLong storeSize = new AtomicLong();
  private AtomicLong totalUncompressedBytes = new AtomicLong();

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

  private ScanInfo scanInfo;

  // All access must be synchronized.
  // TODO: ideally, this should be part of storeFileManager, as we keep passing this to it.
  private final List<StoreFile> filesCompacting = Lists.newArrayList();

  // All access must be synchronized.
  private final Set<ChangedReadersObserver> changedReaderObservers =
    Collections.newSetFromMap(new ConcurrentHashMap<ChangedReadersObserver, Boolean>());

  private final int blocksize;
  private HFileDataBlockEncoder dataBlockEncoder;

  /** Checksum configuration */
  private ChecksumType checksumType;
  private int bytesPerChecksum;

  // Comparing KeyValues
  private final KeyValue.KVComparator comparator;

  final StoreEngine<?, ?, ?, ?> storeEngine;

  private static final AtomicBoolean offPeakCompactionTracker = new AtomicBoolean();
  private volatile OffPeakHours offPeakHours;

  private static final int DEFAULT_FLUSH_RETRIES_NUMBER = 10;
  private int flushRetriesNumber;
  private int pauseTime;

  private long blockingFileCount;
  private int compactionCheckMultiplier;

  private Encryption.Context cryptoContext = Encryption.Context.NONE;

  private AtomicLong flushedCellsCount = new AtomicLong();
  private AtomicLong compactedCellsCount = new AtomicLong();
  private AtomicLong majorCompactedCellsCount = new AtomicLong();
  private AtomicLong flushedCellsSize = new AtomicLong();
  private AtomicLong flushedOutputFileSize = new AtomicLong();
  private AtomicLong compactedCellsSize = new AtomicLong();
  private AtomicLong majorCompactedCellsSize = new AtomicLong();

  /**
   * Constructor
   * @param region
   * @param family HColumnDescriptor for this column
   * @param confParam configuration object
   * failed.  Can be null.
   * @throws IOException
   */
  protected HStore(final HRegion region, final HColumnDescriptor family,
      final Configuration confParam) throws IOException {

    HRegionInfo info = region.getRegionInfo();
    this.fs = region.getRegionFileSystem();

    // Assemble the store's home directory and Ensure it exists.
    fs.createStoreDir(family.getNameAsString());
    this.region = region;
    this.family = family;
    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    // CompoundConfiguration will look for keys in reverse order of addition, so we'd
    // add global config first, then table and cf overrides, then cf metadata.
    this.conf = new CompoundConfiguration()
      .add(confParam)
      .addStringMap(region.getTableDesc().getConfiguration())
      .addStringMap(family.getConfiguration())
      .addWritableMap(family.getValues());
    this.blocksize = family.getBlocksize();

    // set block storage policy for store directory
    String policyName = family.getStoragePolicy();
    if (null == policyName) {
      policyName = this.conf.get(BLOCK_STORAGE_POLICY_KEY, DEFAULT_BLOCK_STORAGE_POLICY);
    }
    this.fs.setStoragePolicy(family.getNameAsString(), policyName.trim());

    this.dataBlockEncoder =
        new HFileDataBlockEncoderImpl(family.getDataBlockEncoding());

    this.comparator = info.getComparator();
    // used by ScanQueryMatcher
    long timeToPurgeDeletes =
        Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    LOG.trace("Time to purge deletes set to " + timeToPurgeDeletes +
        "ms in store " + this);
    // Get TTL
    long ttl = determineTTLFromFamily(family);
    // Why not just pass a HColumnDescriptor in here altogether?  Even if have
    // to clone it?
    scanInfo = new ScanInfo(conf, family, ttl, timeToPurgeDeletes, this.comparator);
    String className = conf.get(MEMSTORE_CLASS_NAME, DefaultMemStore.class.getName());
    this.memstore = ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {
        Configuration.class, KeyValue.KVComparator.class }, new Object[] { conf, this.comparator });
    this.offPeakHours = OffPeakHours.getInstance(conf);

    // Setting up cache configuration for this family
    this.cacheConf = new CacheConfig(conf, family);

    this.verifyBulkLoads = conf.getBoolean("hbase.hstore.bulkload.verify", false);

    this.blockingFileCount =
        conf.getInt(BLOCKING_STOREFILES_KEY, DEFAULT_BLOCKING_STOREFILE_COUNT);
    this.compactionCheckMultiplier = conf.getInt(
        COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
    if (this.compactionCheckMultiplier <= 0) {
      LOG.error("Compaction check period multiplier must be positive, setting default: "
          + DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
      this.compactionCheckMultiplier = DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER;
    }

    if (HStore.closeCheckInterval == 0) {
      HStore.closeCheckInterval = conf.getInt(
          "hbase.hstore.close.check.interval", 10*1000*1000 /* 10 MB */);
    }

    this.storeEngine = StoreEngine.create(this, this.conf, this.comparator);
    List<StoreFile> storeFiles = loadStoreFiles();
    // Move the storeSize calculation out of loadStoreFiles() method, because the secondary read
    // replica's refreshStoreFiles() will also use loadStoreFiles() to refresh its store files and
    // update the storeSize in the completeCompaction(..) finally (just like compaction) , so
    // no need calculate the storeSize twice.
    this.storeSize.addAndGet(getStorefilesSize(storeFiles));
    this.totalUncompressedBytes.addAndGet(getTotalUmcompressedBytes(storeFiles));
    this.storeEngine.getStoreFileManager().loadFiles(storeFiles);

    // Initialize checksum type from name. The names are CRC32, CRC32C, etc.
    this.checksumType = getChecksumType(conf);
    // initilize bytes per checksum
    this.bytesPerChecksum = getBytesPerChecksum(conf);
    flushRetriesNumber = conf.getInt(
        "hbase.hstore.flush.retries.number", DEFAULT_FLUSH_RETRIES_NUMBER);
    pauseTime = conf.getInt(HConstants.HBASE_SERVER_PAUSE, HConstants.DEFAULT_HBASE_SERVER_PAUSE);
    if (flushRetriesNumber <= 0) {
      throw new IllegalArgumentException(
          "hbase.hstore.flush.retries.number must be > 0, not "
              + flushRetriesNumber);
    }

    // Crypto context for new store files
    String cipherName = family.getEncryptionType();
    if (cipherName != null) {
      Cipher cipher;
      Key key;
      byte[] keyBytes = family.getEncryptionKey();
      if (keyBytes != null) {
        // Family provides specific key material
        String masterKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
          User.getCurrent().getShortName());
        try {
          // First try the master key
          key = EncryptionUtil.unwrapKey(conf, masterKeyName, keyBytes);
        } catch (KeyException e) {
          // If the current master key fails to unwrap, try the alternate, if
          // one is configured
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
          }
          String alternateKeyName =
            conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
          if (alternateKeyName != null) {
            try {
              key = EncryptionUtil.unwrapKey(conf, alternateKeyName, keyBytes);
            } catch (KeyException ex) {
              throw new IOException(ex);
            }
          } else {
            throw new IOException(e);
          }
        }
        // Use the algorithm the key wants
        cipher = Encryption.getCipher(conf, key.getAlgorithm());
        if (cipher == null) {
          throw new RuntimeException("Cipher '" + key.getAlgorithm() + "' is not available");
        }
        // Fail if misconfigured
        // We use the encryption type specified in the column schema as a sanity check on
        // what the wrapped key is telling us
        if (!cipher.getName().equalsIgnoreCase(cipherName)) {
          throw new RuntimeException("Encryption for family '" + family.getNameAsString() +
            "' configured with type '" + cipherName +
            "' but key specifies algorithm '" + cipher.getName() + "'");
        }
      } else {
        // Family does not provide key material, create a random key
        cipher = Encryption.getCipher(conf, cipherName);
        if (cipher == null) {
          throw new RuntimeException("Cipher '" + cipherName + "' is not available");
        }
        key = cipher.getRandomKey();
      }
      cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
    }

    LOG.info("Store=" + getColumnFamilyName() +
      ", memstore type=" + this.memstore.getClass().getSimpleName() +
      ", storagePolicy=" + policyName + ", verifyBulkLoads=" + verifyBulkLoads +
      ", encoding=" + family.getDataBlockEncoding() +
      ", compression=" + family.getCompressionType());
  }

  /**
   * @param family
   * @return TTL in seconds of the specified family
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

  @Override
  public String getColumnFamilyName() {
    return this.family.getNameAsString();
  }

  @Override
  public TableName getTableName() {
    return this.getRegionInfo().getTable();
  }

  @Override
  public FileSystem getFileSystem() {
    return this.fs.getFileSystem();
  }

  public HRegionFileSystem getRegionFileSystem() {
    return this.fs;
  }

  /* Implementation of StoreConfigInformation */
  @Override
  public long getStoreFileTtl() {
    // TTL only applies if there's no MIN_VERSIONs setting on the column.
    return (this.scanInfo.getMinVersions() == 0) ? this.scanInfo.getTtl() : Long.MAX_VALUE;
  }

  @Override
  public long getMemstoreFlushSize() {
    // TODO: Why is this in here?  The flushsize of the region rather than the store?  St.Ack
    return this.region.memstoreFlushSize;
  }

  @Override
  public long getFlushableSize() {
    return this.memstore.getFlushableSize();
  }

  @Override
  public long getSnapshotSize() {
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
      return ChecksumType.getDefaultChecksumType();
    } else {
      return ChecksumType.nameToType(checksumName);
    }
  }

  /**
   * @return how many bytes to write between status checks
   */
  public static int getCloseCheckInterval() {
    return closeCheckInterval;
  }

  @Override
  public HColumnDescriptor getFamily() {
    return this.family;
  }

  /**
   * @return The maximum sequence id in all store files. Used for log replay.
   */
  @Override
  public long getMaxSequenceId() {
    return StoreFile.getMaxSequenceIdInList(this.getStorefiles());
  }

  @Override
  public long getMaxMemstoreTS() {
    return StoreFile.getMaxMemstoreTSInList(this.getStorefiles());
  }

  /**
   * @param tabledir {@link Path} to where the table is being stored
   * @param hri {@link HRegionInfo} for the region.
   * @param family {@link HColumnDescriptor} describing the column family
   * @return Path to family/Store home directory.
   */
  @Deprecated
  public static Path getStoreHomedir(final Path tabledir,
      final HRegionInfo hri, final byte[] family) {
    return getStoreHomedir(tabledir, hri.getEncodedName(), family);
  }

  /**
   * @param tabledir {@link Path} to where the table is being stored
   * @param encodedName Encoded region name.
   * @param family {@link HColumnDescriptor} describing the column family
   * @return Path to family/Store home directory.
   */
  @Deprecated
  public static Path getStoreHomedir(final Path tabledir,
      final String encodedName, final byte[] family) {
    return new Path(tabledir, new Path(encodedName, Bytes.toString(family)));
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

  /**
   * Creates an unsorted list of StoreFile loaded in parallel
   * from the given directory.
   * @throws IOException
   */
  private List<StoreFile> loadStoreFiles() throws IOException {
    Collection<StoreFileInfo> files = fs.getStoreFiles(getColumnFamilyName());
    return openStoreFiles(files);
  }

  private List<StoreFile> openStoreFiles(Collection<StoreFileInfo> files) throws IOException {
    if (files == null || files.size() == 0) {
      return new ArrayList<StoreFile>();
    }
    // initialize the thread pool for opening store files in parallel..
    ThreadPoolExecutor storeFileOpenerThreadPool =
      this.region.getStoreFileOpenAndCloseThreadPool("StoreFileOpenerThread-" +
          this.getColumnFamilyName());
    CompletionService<StoreFile> completionService =
      new ExecutorCompletionService<StoreFile>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (final StoreFileInfo storeFileInfo: files) {
      // open each store file in parallel
      completionService.submit(new Callable<StoreFile>() {
        @Override
        public StoreFile call() throws IOException {
          StoreFile storeFile = createStoreFileAndReader(storeFileInfo);
          return storeFile;
        }
      });
      totalValidStoreFile++;
    }

    Set<String> compactedStoreFiles = new HashSet<>();
    ArrayList<StoreFile> results = new ArrayList<StoreFile>(files.size());
    IOException ioe = null;
    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        try {
          Future<StoreFile> future = completionService.take();
          StoreFile storeFile = future.get();
          if (storeFile != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("loaded " + storeFile.toStringDetailed());
            }
            results.add(storeFile);
            compactedStoreFiles.addAll(storeFile.getCompactedStoreFiles());
          }
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
      boolean evictOnClose =
          cacheConf != null? cacheConf.shouldEvictOnClose(): true;
      for (StoreFile file : results) {
        try {
          if (file != null) file.closeReader(evictOnClose);
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
      }
      throw ioe;
    }

    // Remove the compacted files from result
    List<StoreFile> filesToRemove = new ArrayList<>(compactedStoreFiles.size());
    for (StoreFile storeFile : results) {
      if (compactedStoreFiles.contains(storeFile.getPath().getName())) {
        LOG.warn("Clearing the compacted storefile " + storeFile + " from this store");
        storeFile.getReader().close(true);
        filesToRemove.add(storeFile);
      }
    }
    results.removeAll(filesToRemove);
    if (!filesToRemove.isEmpty() && this.isPrimaryReplicaStore()) {
      LOG.debug("Moving the files " + filesToRemove + " to archive");
      this.fs.removeStoreFiles(this.getColumnFamilyName(), filesToRemove);
    }

    return results;
  }

  /**
   * Checks the underlying store files, and opens the files that  have not
   * been opened, and removes the store file readers for store files no longer
   * available. Mainly used by secondary region replicas to keep up to date with
   * the primary region files.
   * @throws IOException
   */
  @Override
  public void refreshStoreFiles() throws IOException {
    Collection<StoreFileInfo> newFiles = fs.getStoreFiles(getColumnFamilyName());
    refreshStoreFilesInternal(newFiles);
  }

  @Override
  public void refreshStoreFiles(Collection<String> newFiles) throws IOException {
    List<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(newFiles.size());
    for (String file : newFiles) {
      storeFiles.add(fs.getStoreFileInfo(getColumnFamilyName(), file));
    }
    refreshStoreFilesInternal(storeFiles);
  }

  /**
   * Checks the underlying store files, and opens the files that  have not
   * been opened, and removes the store file readers for store files no longer
   * available. Mainly used by secondary region replicas to keep up to date with
   * the primary region files.
   * @throws IOException
   */
  private void refreshStoreFilesInternal(Collection<StoreFileInfo> newFiles) throws IOException {
    StoreFileManager sfm = storeEngine.getStoreFileManager();
    Collection<StoreFile> currentFiles = sfm.getStorefiles();
    Collection<StoreFile> compactedFiles = sfm.getCompactedfiles();
    if (currentFiles == null) currentFiles = Collections.emptySet();
    if (newFiles == null) newFiles = Collections.emptySet();
    if (compactedFiles == null) compactedFiles = Collections.emptySet();

    HashMap<StoreFileInfo, StoreFile> currentFilesSet =
        new HashMap<StoreFileInfo, StoreFile>(currentFiles.size());
    for (StoreFile sf : currentFiles) {
      currentFilesSet.put(sf.getFileInfo(), sf);
    }
    HashMap<StoreFileInfo, StoreFile> compactedFilesSet =
        new HashMap<StoreFileInfo, StoreFile>(compactedFiles.size());
    for (StoreFile sf : compactedFiles) {
      compactedFilesSet.put(sf.getFileInfo(), sf);
    }

    Set<StoreFileInfo> newFilesSet = new HashSet<StoreFileInfo>(newFiles);
    //Exclude the files that have already been compacted
    newFilesSet = Sets.difference(newFilesSet, compactedFilesSet.keySet());
    Set<StoreFileInfo> toBeAddedFiles = Sets.difference(newFilesSet, currentFilesSet.keySet());
    Set<StoreFileInfo> toBeRemovedFiles = Sets.difference(currentFilesSet.keySet(), newFilesSet);

    if (toBeAddedFiles.isEmpty() && toBeRemovedFiles.isEmpty()) {
      return;
    }

    LOG.info("Refreshing store files for region " + this.getRegionInfo().getRegionNameAsString()
      + " files to add: " + toBeAddedFiles + " files to remove: " + toBeRemovedFiles);

    Set<StoreFile> toBeRemovedStoreFiles = new HashSet<StoreFile>(toBeRemovedFiles.size());
    for (StoreFileInfo sfi : toBeRemovedFiles) {
      toBeRemovedStoreFiles.add(currentFilesSet.get(sfi));
    }

    // try to open the files
    List<StoreFile> openedFiles = openStoreFiles(toBeAddedFiles);

    // propogate the file changes to the underlying store file manager
    replaceStoreFiles(toBeRemovedStoreFiles, openedFiles); //won't throw an exception

    // Advance the memstore read point to be at least the new store files seqIds so that
    // readers might pick it up. This assumes that the store is not getting any writes (otherwise
    // in-flight transactions might be made visible)
    if (!toBeAddedFiles.isEmpty()) {
      region.getMVCC().advanceTo(this.getMaxSequenceId());
    }

    completeCompaction(toBeRemovedStoreFiles);
  }

  private StoreFile createStoreFileAndReader(final Path p) throws IOException {
    StoreFileInfo info = new StoreFileInfo(conf, this.getFileSystem(), p);
    return createStoreFileAndReader(info);
  }

  private StoreFile createStoreFileAndReader(final StoreFileInfo info)
      throws IOException {
    info.setRegionCoprocessorHost(this.region.getCoprocessorHost());
    StoreFile storeFile = new StoreFile(this.getFileSystem(), info, this.conf, this.cacheConf,
      this.family.getBloomFilterType());
    StoreFile.Reader r = storeFile.createReader();
    r.setReplicaStoreFile(isPrimaryReplicaStore());
    return storeFile;
  }

  @Override
  public long add(final Cell cell) {
    lock.readLock().lock();
    try {
       return this.memstore.add(cell);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long add(Iterable<Cell> cells) {
    lock.readLock().lock();
    try {
       return this.memstore.add(cells);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long timeOfOldestEdit() {
    return memstore.timeOfOldestEdit();
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
  public void rollback(final Cell cell) {
    lock.readLock().lock();
    try {
      this.memstore.rollback(cell);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return All store files.
   */
  @Override
  public Collection<StoreFile> getStorefiles() {
    return this.storeEngine.getStoreFileManager().getStorefiles();
  }

  public Collection<StoreFile> getCompactedfiles() {
    Collection<StoreFile> compactedFiles =
        this.storeEngine.getStoreFileManager().getCompactedfiles();
    return compactedFiles == null ? new ArrayList<StoreFile>() : compactedFiles;
  }

  @Override
  public void assertBulkLoadHFileOk(Path srcPath) throws IOException {
    HFile.Reader reader  = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in "
          + "store " + this + " region " + this.getRegionInfo().getRegionNameAsString());
      reader = HFile.createReader(srcPath.getFileSystem(conf),
          srcPath, cacheConf, conf);
      reader.loadFileInfo();

      byte[] firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey != null, "First key can not be null");
      byte[] lk = reader.getLastKey();
      Preconditions.checkState(lk != null, "Last key can not be null");
      byte[] lastKey =  KeyValue.createKeyValueFromKey(lk).getRow();

      LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey) +
          " last=" + Bytes.toStringBinary(lastKey));
      LOG.debug("Region bounds: first=" +
          Bytes.toStringBinary(getRegionInfo().getStartKey()) +
          " last=" + Bytes.toStringBinary(getRegionInfo().getEndKey()));

      if (!this.getRegionInfo().containsRange(firstKey, lastKey)) {
        throw new WrongRegionException(
            "Bulk load file " + srcPath.toString() + " does not fit inside region "
            + this.getRegionInfo().getRegionNameAsString());
      }

      if(reader.length() > conf.getLong(HConstants.HREGION_MAX_FILESIZE,
          HConstants.DEFAULT_MAX_FILE_SIZE)) {
        LOG.warn("Trying to bulk load hfile " + srcPath.toString() + " with size: " +
            reader.length() + " bytes can be problematic as it may lead to oversplitting.");
      }

      if (verifyBulkLoads) {
        long verificationStartTime = EnvironmentEdgeManager.currentTime();
        LOG.info("Full verification started for bulk load hfile: " + srcPath.toString());
        Cell prevCell = null;
        HFileScanner scanner = reader.getScanner(false, false, false);
        scanner.seekTo();
        do {
          Cell cell = scanner.getKeyValue();
          if (prevCell != null) {
            if (CellComparator.compareRows(prevCell, cell) > 0) {
              throw new InvalidHFileException("Previous row is greater than"
                  + " current row: path=" + srcPath + " previous="
                  + CellUtil.getCellKeyAsString(prevCell) + " current="
                  + CellUtil.getCellKeyAsString(cell));
            }
            if (CellComparator.compareFamilies(prevCell, cell) != 0) {
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
      LOG.info("Full verification complete for bulk load hfile: " + srcPath.toString()
         + " took " + (EnvironmentEdgeManager.currentTime() - verificationStartTime)
         + " ms");
      }
    } finally {
      if (reader != null) reader.close();
    }
  }

  public Pair<Path, Path> preBulkLoadHFile(String srcPathStr, long seqNum) throws IOException {
    Path srcPath = new Path(srcPathStr);
    return fs.bulkLoadStoreFile(getColumnFamilyName(), srcPath, seqNum);
  }

  @Override
  public Path bulkLoadHFile(byte[] family, String srcPathStr, Path dstPath) throws IOException {
    Path srcPath = new Path(srcPathStr);
    try {
      fs.commitStoreFile(srcPath, dstPath);
    } finally {
      if (this.getCoprocessorHost() != null) {
        this.getCoprocessorHost().postCommitStoreFile(family, srcPath, dstPath);
      }
    }

    LOG.info("Loaded HFile " + srcPath + " into store '" + getColumnFamilyName() + "' as "
        + dstPath + " - updating store file list.");

    StoreFile sf = createStoreFileAndReader(dstPath);
    bulkLoadHFile(sf);

    LOG.info("Successfully loaded store file " + srcPath + " into store " + this
        + " (new location: " + dstPath + ")");

    return dstPath;
  }

  @Override
  public void bulkLoadHFile(StoreFileInfo fileInfo) throws IOException {
    StoreFile sf = createStoreFileAndReader(fileInfo);
    bulkLoadHFile(sf);
  }

  private void bulkLoadHFile(StoreFile sf) throws IOException {
    StoreFile.Reader r = sf.getReader();
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
    LOG.info("Loaded HFile " + sf.getFileInfo() + " into store '" + getColumnFamilyName());
    if (LOG.isTraceEnabled()) {
      String traceMessage = "BULK LOAD time,size,store size,store files ["
          + EnvironmentEdgeManager.currentTime() + "," + r.length() + "," + storeSize
          + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
  }

  @Override
  public ImmutableCollection<StoreFile> close() throws IOException {
    this.archiveLock.lock();
    this.lock.writeLock().lock();
    try {
      // Clear so metrics doesn't find them.
      ImmutableCollection<StoreFile> result = storeEngine.getStoreFileManager().clearFiles();
      Collection<StoreFile> compactedfiles =
          storeEngine.getStoreFileManager().clearCompactedFiles();
      // clear the compacted files
      if (compactedfiles != null && !compactedfiles.isEmpty()) {
        removeCompactedfiles(compactedfiles);
      }
      if (!result.isEmpty()) {
        // initialize the thread pool for closing store files in parallel.
        ThreadPoolExecutor storeFileCloserThreadPool = this.region
            .getStoreFileOpenAndCloseThreadPool("StoreFileCloserThread-"
                + this.getColumnFamilyName());

        // close each store file in parallel
        CompletionService<Void> completionService =
          new ExecutorCompletionService<Void>(storeFileCloserThreadPool);
        for (final StoreFile f : result) {
          completionService.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
              boolean evictOnClose =
                  cacheConf != null? cacheConf.shouldEvictOnClose(): true;
              f.closeReader(evictOnClose);
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
      this.archiveLock.unlock();
    }
  }

  /**
   * Snapshot this stores memstore. Call before running
   * {@link #flushCache(long, MemStoreSnapshot, MonitoredTask, ThroughputController)}
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
   * @param snapshot
   * @param status
   * @param throughputController
   * @return The path name of the tmp file to which the store was flushed
   * @throws IOException if exception occurs during process
   */
  protected List<Path> flushCache(final long logCacheFlushId, MemStoreSnapshot snapshot,
      MonitoredTask status, ThroughputController throughputController) throws IOException {
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
            flusher.flushSnapshot(snapshot, logCacheFlushId, status, throughputController);
        Path lastPathName = null;
        try {
          for (Path pathName : pathNames) {
            lastPathName = pathName;
            validateStoreFile(pathName);
          }
          return pathNames;
        } catch (Exception e) {
          LOG.warn("Failed validating store file " + lastPathName + ", retrying num=" + i, e);
          if (e instanceof IOException) {
            lastException = (IOException) e;
          } else {
            lastException = new IOException(e);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed flushing store file, retrying num=" + i, e);
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

  /*
   * @param path The pathname of the tmp file into which the store was flushed
   * @param logCacheFlushId
   * @param status
   * @return StoreFile created.
   * @throws IOException
   */
  private StoreFile commitFile(final Path path, final long logCacheFlushId, MonitoredTask status)
      throws IOException {
    // Write-out finished successfully, move into the right spot
    Path dstPath = fs.commitStoreFile(getColumnFamilyName(), path);

    status.setStatus("Flushing " + this + ": reopening flushed file");
    StoreFile sf = createStoreFileAndReader(dstPath);

    StoreFile.Reader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

    if (LOG.isInfoEnabled()) {
      LOG.info("Added " + sf + ", entries=" + r.getEntries() +
        ", sequenceid=" + logCacheFlushId +
        ", filesize=" + TraditionalBinaryPrefix.long2String(r.length(), "", 1));
    }
    return sf;
  }

  @Override
  public StoreFile.Writer createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
                                            boolean isCompaction, boolean includeMVCCReadpoint,
                                            boolean includesTag)
      throws IOException {
    return createWriterInTmp(maxKeyCount, compression, isCompaction, includeMVCCReadpoint,
        includesTag, false);
  }

  /*
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includesMVCCReadPoint - whether to include MVCC or not
   * @param includesTag - includesTag or not
   * @return Writer for a new StoreFile in the tmp dir.
   */
  @Override
  public StoreFile.Writer createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
      boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
      boolean shouldDropBehind)
  throws IOException {
    return createWriterInTmp(maxKeyCount, compression, isCompaction, includeMVCCReadpoint,
        includesTag, shouldDropBehind, null);
  }

  /*
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includesMVCCReadPoint - whether to include MVCC or not
   * @param includesTag - includesTag or not
   * @return Writer for a new StoreFile in the tmp dir.
   */
  @Override
  public StoreFile.Writer createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
      boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
      boolean shouldDropBehind, final TimeRangeTracker trt)
  throws IOException {
    final CacheConfig writerCacheConf;
    if (isCompaction) {
      // Don't cache data on write on compactions.
      writerCacheConf = new CacheConfig(cacheConf);
      writerCacheConf.setCacheDataOnWrite(false);
    } else {
      writerCacheConf = cacheConf;
    }
    InetSocketAddress[] favoredNodes = null;
    if (region.getRegionServerServices() != null) {
      favoredNodes = region.getRegionServerServices().getFavoredNodesForRegion(
          region.getRegionInfo().getEncodedName());
    }
    Path familyTempDir = new Path(fs.getTempDir(), family.getNameAsString());
    HFileContext hFileContext = createFileContext(compression, includeMVCCReadpoint, includesTag,
      cryptoContext);
    StoreFile.WriterBuilder builder = new StoreFile.WriterBuilder(conf, writerCacheConf,
        this.getFileSystem())
            .withOutputDir(familyTempDir)
            .withComparator(comparator)
            .withBloomType(family.getBloomFilterType())
            .withMaxKeyCount(maxKeyCount)
            .withFavoredNodes(favoredNodes)
            .withFileContext(hFileContext)
            .withShouldDropCacheBehind(shouldDropBehind)
            .withCompactedFiles(this.getCompactedfiles());
    if (trt != null) {
      builder.withTimeRangeTracker(trt);
    }
    return builder.build();
  }

  private HFileContext createFileContext(Compression.Algorithm compression,
      boolean includeMVCCReadpoint, boolean includesTag, Encryption.Context cryptoContext) {
    if (compression == null) {
      compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    HFileContext hFileContext = new HFileContextBuilder()
                                .withIncludesMvcc(includeMVCCReadpoint)
                                .withIncludesTags(includesTag)
                                .withCompression(compression)
                                .withCompressTags(family.isCompressTags())
                                .withChecksumType(checksumType)
                                .withBytesPerCheckSum(bytesPerChecksum)
                                .withBlockSize(blocksize)
                                .withHBaseCheckSum(true)
                                .withDataBlockEncoding(family.getDataBlockEncoding())
                                .withEncryptionContext(cryptoContext)
                                .withCreateTime(EnvironmentEdgeManager.currentTime())
                                .withColumnFamily(family.getName())
                                .withTableName(region.getTableDesc().
                                    getTableName().getName())
                                .build();
    return hFileContext;
  }


  /*
   * Change storeFiles adding into place the Reader produced by this new flush.
   * @param sfs Store files
   * @param snapshotId
   * @throws IOException
   * @return Whether compaction is required.
   */
  private boolean updateStorefiles(final List<StoreFile> sfs, final long snapshotId)
      throws IOException {
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().insertNewFiles(sfs);
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
      long totalSize = 0;
      for (StoreFile sf : sfs) {
        totalSize += sf.getReader().length();
      }
      String traceMessage = "FLUSH time,count,size,store size,store files ["
          + EnvironmentEdgeManager.currentTime() + "," + sfs.size() + "," + totalSize
          + "," + storeSize + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
    return needsCompaction();
  }

  /*
   * Notify all observers that set of Readers has changed.
   * @throws IOException
   */
  private void notifyChangedReadersObservers(List<StoreFile> sfs) throws IOException {
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
   * Get all scanners with no filtering based on TTL (that happens further down
   * the line).
   * @return all scanners for this store
   */
  @Override
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean isGet,
      boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow,
      byte[] stopRow, long readPt) throws IOException {
    Collection<StoreFile> storeFilesToScan;
    List<KeyValueScanner> memStoreScanners;
    this.lock.readLock().lock();
    try {
      // As in branch-1 we need to support JDK7 so we can not add default methods to the Store
      // interface, but add new methods directly in interface will break the compatibility, so here
      // we always pass true to StoreFileManager to include more files. And for now, there is no
      // performance issue as the DefaultStoreFileManager just returns all storefile, and
      // StripeStoreFileManager just ignores the inclusive hints.
      storeFilesToScan = this.storeEngine.getStoreFileManager().getFilesForScanOrGet(startRow, true,
        stopRow, true);
      memStoreScanners = this.memstore.getScanners(readPt);
    } finally {
      this.lock.readLock().unlock();
    }

    // First the store file scanners

    // TODO this used to get the store files in descending order,
    // but now we get them in ascending order, which I think is
    // actually more correct, since memstore get put at the end.
    List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(storeFilesToScan,
        cacheBlocks, usePread, isCompaction, false, matcher, readPt, isPrimaryReplicaStore());
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(sfScanners.size()+1);
    scanners.addAll(sfScanners);
    // Then the memstore scanners
    if (memStoreScanners != null) {
      scanners.addAll(memStoreScanners);
    }
    return scanners;
  }

  @Override
  public List<KeyValueScanner> getScanners(List<StoreFile> files, boolean cacheBlocks,
      boolean isGet, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
      byte[] startRow, byte[] stopRow, long readPt, boolean includeMemstoreScanner) throws IOException {
    List<KeyValueScanner> memStoreScanners = null;
    if (includeMemstoreScanner) {
      this.lock.readLock().lock();
      try {
        memStoreScanners = this.memstore.getScanners(readPt);
      } finally {
        this.lock.readLock().unlock();
      }
    }
    List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(files,
      cacheBlocks, usePread, isCompaction, false, matcher, readPt, isPrimaryReplicaStore());
    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>(sfScanners.size() + 1);
    scanners.addAll(sfScanners);
    // Then the memstore scanners
    if (memStoreScanners != null) {
      scanners.addAll(memStoreScanners);
    }
    return scanners;
  }

  @Override
  public void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }

  @Override
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
   *  RS that failed won't be able to finish snyc() for WAL because of lease recovery in WAL.
   *  - If RS fails after 3, the region region server who opens the region will pick up the
   *  the compaction marker from the WAL and replay it by removing the compaction input files.
   *  Failed RS can also attempt to delete those files, but the operation will be idempotent
   *
   * See HBASE-2231 for details.
   *
   * @param compaction compaction details obtained from requestCompaction()
   * @throws IOException
   * @return Storefile we compacted into or null if we failed or opted out early.
   */
  @Override
  public List<StoreFile> compact(CompactionContext compaction,
      ThroughputController throughputController) throws IOException {
    return compact(compaction, throughputController, null);
  }

  @Override
  public List<StoreFile> compact(CompactionContext compaction,
    ThroughputController throughputController, User user) throws IOException {
    assert compaction != null;
    List<StoreFile> sfs = null;
    CompactionRequest cr = compaction.getRequest();
    try {
      // Do all sanity checking in here if we have a valid CompactionRequest
      // because we need to clean up after it on the way out in a finally
      // block below
      long compactionStartTime = EnvironmentEdgeManager.currentTime();
      assert compaction.hasSelection();
      Collection<StoreFile> filesToCompact = cr.getFiles();
      assert !filesToCompact.isEmpty();
      synchronized (filesCompacting) {
        // sanity check: we're compacting files that this store knows about
        // TODO: change this to LOG.error() after more debugging
        Preconditions.checkArgument(filesCompacting.containsAll(filesToCompact));
      }

      // Ready to go. Have list of files to compact.
      LOG.info("Starting compaction of " + filesToCompact.size() + " file(s) in "
          + this + " of " + this.getRegionInfo().getRegionNameAsString()
          + " into tmpdir=" + fs.getTempDir() + ", totalSize="
          + TraditionalBinaryPrefix.long2String(cr.getSize(), "", 1));

      // Commence the compaction.
      List<Path> newFiles = compaction.compact(throughputController, user);

      long outputBytes = 0L;
      // TODO: get rid of this!
      if (!this.conf.getBoolean("hbase.hstore.compaction.complete", true)) {
        LOG.warn("hbase.hstore.compaction.complete is set to false");
        sfs = new ArrayList<StoreFile>(newFiles.size());
        final boolean evictOnClose =
            cacheConf != null? cacheConf.shouldEvictOnClose(): true;
        for (Path newFile : newFiles) {
          // Create storefile around what we wrote with a reader on it.
          StoreFile sf = createStoreFileAndReader(newFile);
          sf.closeReader(evictOnClose);
          sfs.add(sf);
        }
        return sfs;
      }
      // Do the steps necessary to complete the compaction.
      sfs = moveCompatedFilesIntoPlace(cr, newFiles, user);
      writeCompactionWalRecord(filesToCompact, sfs);
      replaceStoreFiles(filesToCompact, sfs);
      if (cr.isMajor()) {
        majorCompactedCellsCount.addAndGet(getCompactionProgress().totalCompactingKVs);
        majorCompactedCellsSize.addAndGet(getCompactionProgress().totalCompactedSize);
      } else {
        compactedCellsCount.addAndGet(getCompactionProgress().totalCompactingKVs);
        compactedCellsSize.addAndGet(getCompactionProgress().totalCompactedSize);
      }

      for (StoreFile sf : sfs) {
        outputBytes += sf.getReader().length();
      }

      // At this point the store will use new files for all new scanners.
      completeCompaction(filesToCompact); // update store size.

      long now = EnvironmentEdgeManager.currentTime();
      if (region.getRegionServerServices() != null
          && region.getRegionServerServices().getMetrics() != null) {
        region.getRegionServerServices().getMetrics().updateCompaction(cr.isMajor(),
          now - compactionStartTime, cr.getFiles().size(), newFiles.size(), cr.getSize(),
          outputBytes);
      }

      logCompactionEndMessage(cr, sfs, now, compactionStartTime);
      return sfs;
    } finally {
      finishCompactionRequest(cr);
    }
  }

  private List<StoreFile> moveCompatedFilesIntoPlace(
      final CompactionRequest cr, List<Path> newFiles, User user) throws IOException {
    List<StoreFile> sfs = new ArrayList<StoreFile>(newFiles.size());
    for (Path newFile : newFiles) {
      assert newFile != null;
      final StoreFile sf = moveFileIntoPlace(newFile);
      if (this.getCoprocessorHost() != null) {
        final Store thisStore = this;
        getCoprocessorHost().postCompact(thisStore, sf, cr, user);
      }
      assert sf != null;
      sfs.add(sf);
    }
    return sfs;
  }

  // Package-visible for tests
  StoreFile moveFileIntoPlace(final Path newFile) throws IOException {
    validateStoreFile(newFile);
    // Move the file into the right spot
    Path destPath = fs.commitStoreFile(getColumnFamilyName(), newFile);
    return createStoreFileAndReader(destPath);
  }

  /**
   * Writes the compaction WAL record.
   * @param filesCompacted Files compacted (input).
   * @param newFiles Files from compaction.
   */
  private void writeCompactionWalRecord(Collection<StoreFile> filesCompacted,
      Collection<StoreFile> newFiles) throws IOException {
    if (region.getWAL() == null) return;
    List<Path> inputPaths = new ArrayList<Path>(filesCompacted.size());
    for (StoreFile f : filesCompacted) {
      inputPaths.add(f.getPath());
    }
    List<Path> outputPaths = new ArrayList<Path>(newFiles.size());
    for (StoreFile f : newFiles) {
      outputPaths.add(f.getPath());
    }
    HRegionInfo info = this.region.getRegionInfo();
    CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(info,
        family.getName(), inputPaths, outputPaths, fs.getStoreDir(getFamily().getNameAsString()));
    WALUtil.writeCompactionMarker(region.getWAL(), this.region.getTableDesc(),
        this.region.getRegionInfo(), compactionDescriptor, region.getMVCC());
  }

  @VisibleForTesting
  void replaceStoreFiles(final Collection<StoreFile> compactedFiles,
      final Collection<StoreFile> result) throws IOException {
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().addCompactionResults(compactedFiles, result);
      synchronized (filesCompacting) {
        filesCompacting.removeAll(compactedFiles);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Log a very elaborate compaction completion message.
   * @param cr Request.
   * @param sfs Resulting files.
   * @param compactionStartTime Start time.
   */
  private void logCompactionEndMessage(
      CompactionRequest cr, List<StoreFile> sfs, long now, long compactionStartTime) {
    StringBuilder message = new StringBuilder(
      "Completed" + (cr.isMajor() ? " major" : "") + " compaction of "
      + cr.getFiles().size() + (cr.isAllFiles() ? " (all)" : "") + " file(s) in "
      + this + " of " + this.getRegionInfo().getRegionNameAsString() + " into ");
    if (sfs.isEmpty()) {
      message.append("none, ");
    } else {
      for (StoreFile sf: sfs) {
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
      long resultSize = 0;
      for (StoreFile sf : sfs) {
        resultSize += sf.getReader().length();
      }
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
   * @param compaction
   */
  @Override
  public void replayCompactionMarker(CompactionDescriptor compaction,
      boolean pickCompactionFiles, boolean removeFiles)
      throws IOException {
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
    List<String> inputFiles = new ArrayList<String>(compactionInputs.size());
    for (String compactionInput : compactionInputs) {
      Path inputPath = fs.getStoreFilePath(familyName, compactionInput);
      inputFiles.add(inputPath.getName());
    }

    //some of the input files might already be deleted
    List<StoreFile> inputStoreFiles = new ArrayList<StoreFile>(compactionInputs.size());
    for (StoreFile sf : this.getStorefiles()) {
      if (inputFiles.contains(sf.getPath().getName())) {
        inputStoreFiles.add(sf);
      }
    }

    // check whether we need to pick up the new files
    List<StoreFile> outputStoreFiles = new ArrayList<StoreFile>(compactionOutputs.size());

    if (pickCompactionFiles) {
      for (StoreFile sf : this.getStorefiles()) {
        compactionOutputs.remove(sf.getPath().getName());
      }
      for (String compactionOutput : compactionOutputs) {
        StoreFileInfo storeFileInfo = fs.getStoreFileInfo(getColumnFamilyName(), compactionOutput);
        StoreFile storeFile = createStoreFileAndReader(storeFileInfo);
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
    List<StoreFile> filesToCompact;
    boolean isMajor;

    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        filesToCompact = Lists.newArrayList(storeEngine.getStoreFileManager().getStorefiles());
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
        StoreFile sf = moveFileIntoPlace(newFile);
        if (this.getCoprocessorHost() != null) {
          this.getCoprocessorHost().postCompact(this, sf, null, null);
        }
        replaceStoreFiles(filesToCompact, Lists.newArrayList(sf));
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
      Collection<StoreFile> allStoreFiles = new ArrayList<>(getStorefiles());
      allStoreFiles.addAll(getCompactedfiles());
      return StoreUtils.hasReferences(allStoreFiles);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public CompactionProgress getCompactionProgress() {
    return this.storeEngine.getCompactor().getProgress();
  }

  @Override
  public boolean isMajorCompaction() throws IOException {
    for (StoreFile sf : this.storeEngine.getStoreFileManager().getStorefiles()) {
      // TODO: what are these reader checks all over the place?
      if (sf.getReader() == null) {
        LOG.debug("StoreFile " + sf + " has null Reader");
        return false;
      }
    }
    return storeEngine.getCompactionPolicy().shouldPerformMajorCompaction(
        this.storeEngine.getStoreFileManager().getStorefiles());
  }

  @Override
  public CompactionContext requestCompaction() throws IOException {
    return requestCompaction(Store.NO_PRIORITY, null);
  }

  @Override
  public CompactionContext requestCompaction(int priority, CompactionRequest baseRequest)
      throws IOException {
    return requestCompaction(priority, baseRequest, null);
  }
  @Override
  public CompactionContext requestCompaction(int priority, final CompactionRequest baseRequest,
      User user) throws IOException {
    // don't even select for compaction if writes are disabled
    if (!this.areWritesEnabled()) {
      return null;
    }

    // Before we do compaction, try to get rid of unneeded files to simplify things.
    removeUnneededFiles();

    final CompactionContext compaction = storeEngine.createCompaction();
    CompactionRequest request = null;
    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        // First, see if coprocessor would want to override selection.
        if (this.getCoprocessorHost() != null) {
          final List<StoreFile> candidatesForCoproc = compaction.preSelect(this.filesCompacting);
          boolean override = false;
          override = getCoprocessorHost().preCompactSelection(this, candidatesForCoproc,
              baseRequest, user);
          if (override) {
            // Coprocessor is overriding normal file selection.
            compaction.forceSelect(new CompactionRequest(candidatesForCoproc));
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
              this, ImmutableList.copyOf(compaction.getRequest().getFiles()), baseRequest, user);
        }

        // Selected files; see if we have a compaction with some custom base request.
        if (baseRequest != null) {
          // Update the request with what the system thinks the request should be;
          // its up to the request if it wants to listen.
          compaction.forceSelect(
              baseRequest.combineWith(compaction.getRequest()));
        }
        // Finally, we have the resulting files list. Check if we have any files at all.
        request = compaction.getRequest();
        final Collection<StoreFile> selectedFiles = request.getFiles();
        if (selectedFiles.isEmpty()) {
          return null;
        }

        addToCompactingFiles(selectedFiles);

        // If we're enqueuing a major, clear the force flag.
        this.forceMajor = this.forceMajor && !request.isMajor();

        // Set common request properties.
        // Set priority, either override value supplied by caller or from store.
        request.setPriority((priority != Store.NO_PRIORITY) ? priority : getCompactPriority());
        request.setDescription(getRegionInfo().getRegionNameAsString(), getColumnFamilyName());
      }
    } finally {
      this.lock.readLock().unlock();
    }

    LOG.debug(getRegionInfo().getEncodedName() + " - "  + getColumnFamilyName()
        + ": Initiating " + (request.isMajor() ? "major" : "minor") + " compaction"
        + (request.isAllFiles() ? " (all files)" : ""));
    this.region.reportCompactionRequestStart(request.isMajor());
    return compaction;
  }

  /** Adds the files to compacting files. filesCompacting must be locked. */
  private void addToCompactingFiles(final Collection<StoreFile> filesToAdd) {
    if (filesToAdd == null) return;
    // Check that we do not try to compact the same StoreFile twice.
    if (!Collections.disjoint(filesCompacting, filesToAdd)) {
      Preconditions.checkArgument(false, "%s overlaps with %s", filesToAdd, filesCompacting);
    }
    filesCompacting.addAll(filesToAdd);
    Collections.sort(filesCompacting, storeEngine.getStoreFileManager().getStoreFileComparator());
  }

  private void removeUnneededFiles() throws IOException {
    if (!conf.getBoolean("hbase.store.delete.expired.storefile", true)) return;
    if (getFamily().getMinVersions() > 0) {
      LOG.debug("Skipping expired store file removal due to min version being " +
          getFamily().getMinVersions());
      return;
    }
    this.lock.readLock().lock();
    Collection<StoreFile> delSfs = null;
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
    if (delSfs == null || delSfs.isEmpty()) return;

    Collection<StoreFile> newFiles = new ArrayList<StoreFile>(); // No new files.
    writeCompactionWalRecord(delSfs, newFiles);
    replaceStoreFiles(delSfs, newFiles);
    completeCompaction(delSfs);
    LOG.info("Completed removal of " + delSfs.size() + " unnecessary (expired) file(s) in "
        + this + " of " + this.getRegionInfo().getRegionNameAsString()
        + "; total size for store is " + TraditionalBinaryPrefix.long2String(storeSize.get(), "", 1));
  }

  @Override
  public void cancelRequestedCompaction(CompactionContext compaction) {
    finishCompactionRequest(compaction.getRequest());
  }

  private void finishCompactionRequest(CompactionRequest cr) {
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
   * Validates a store file by opening and closing it. In HFileV2 this should
   * not be an expensive operation.
   *
   * @param path the path to the store file
   */
  private void validateStoreFile(Path path)
      throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = createStoreFileAndReader(path);
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

  /**
   * <p>It works by processing a compaction that's been written to disk.
   *
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   *
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Unload all replaced StoreFile, close and collect list to delete.
   * 2) Compute new store size
   * </pre>
   *
   * @param compactedFiles list of files that were compacted
   */
  @VisibleForTesting
  protected void completeCompaction(final Collection<StoreFile> compactedFiles)
    throws IOException {
    completeCompaction(compactedFiles, true);
  }


  /**
   * <p>It works by processing a compaction that's been written to disk.
   *
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   *
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Unload all replaced StoreFile, close and collect list to delete.
   * 2) Compute new store size
   * </pre>
   *
   * @param compactedFiles list of files that were compacted
   */
  @VisibleForTesting
  protected void completeCompaction(final Collection<StoreFile> compactedFiles, boolean removeFiles)
      throws IOException {
    LOG.debug("Completing compaction...");
    this.storeSize.set(0L);
    this.totalUncompressedBytes.set(0L);
    for (StoreFile hsf : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = hsf.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + hsf + " has a null Reader");
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
    int maxVersions = this.family.getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions: wantedVersions;
  }

  /**
   * @param cell
   * @param oldestTimestamp
   * @return true if the cell is expired
   */
  public static boolean isCellTTLExpired(final Cell cell, final long oldestTimestamp, final long now) {
    // Do not create an Iterator or Tag objects unless the cell actually has tags.
    // TODO: This check for tags is really expensive. We decode an int for key and value. Costs.
    if (cell.getTagsLength() > 0) {
      // Look for a TTL tag first. Use it instead of the family setting if
      // found. If a cell has multiple TTLs, resolve the conflict by using the
      // first tag encountered.
      Iterator<Tag> i = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
      while (i.hasNext()) {
        Tag t = i.next();
        if (TagType.TTL_TAG_TYPE == t.getType()) {
          // Unlike in schema cell TTLs are stored in milliseconds, no need
          // to convert
          long ts = cell.getTimestamp();
          assert t.getTagLength() == Bytes.SIZEOF_LONG;
          long ttl = Bytes.toLong(t.getBuffer(), t.getTagOffset(), t.getTagLength());
          if (ts + ttl < now) {
            return true;
          }
          // Per cell TTLs cannot extend lifetime beyond family settings, so
          // fall through to check that
          break;
        }
      }
    }
    return false;
  }

  @Override
  public Cell getRowKeyAtOrBefore(final byte[] row) throws IOException {
    // If minVersions is set, we will not ignore expired KVs.
    // As we're only looking for the latest matches, that should be OK.
    // With minVersions > 0 we guarantee that any KV that has any version
    // at all (expired or not) has at least one version that will not expire.
    // Note that this method used to take a KeyValue as arguments. KeyValue
    // can be back-dated, a row key cannot.
    long ttlToUse = scanInfo.getMinVersions() > 0 ? Long.MAX_VALUE : this.scanInfo.getTtl();

    KeyValue kv = new KeyValue(row, HConstants.LATEST_TIMESTAMP);

    GetClosestRowBeforeTracker state = new GetClosestRowBeforeTracker(
      this.comparator, kv, ttlToUse, this.getRegionInfo().isMetaRegion());
    this.lock.readLock().lock();
    try {
      // First go to the memstore.  Pick up deletes and candidates.
      this.memstore.getRowKeyAtOrBefore(state);
      // Check if match, if we got a candidate on the asked for 'kv' row.
      // Process each relevant store file. Run through from newest to oldest.
      Iterator<StoreFile> sfIterator = this.storeEngine.getStoreFileManager()
          .getCandidateFilesForRowKeyBefore(state.getTargetKey());
      while (sfIterator.hasNext()) {
        StoreFile sf = sfIterator.next();
        sfIterator.remove(); // Remove sf from iterator.
        boolean haveNewCandidate = rowAtOrBeforeFromStoreFile(sf, state);
        Cell candidate = state.getCandidate();
        // we have an optimization here which stops the search if we find exact match.
        if (candidate != null && CellUtil.matchingRow(candidate, row)) {
          return candidate;
        }
        if (haveNewCandidate) {
          sfIterator = this.storeEngine.getStoreFileManager().updateCandidateFilesForRowKeyBefore(
              sfIterator, state.getTargetKey(), candidate);
        }
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
   * @return True iff the candidate has been updated in the state.
   */
  private boolean rowAtOrBeforeFromStoreFile(final StoreFile f,
                                          final GetClosestRowBeforeTracker state)
      throws IOException {
    StoreFile.Reader r = f.getReader();
    if (r == null) {
      LOG.warn("StoreFile " + f + " has a null Reader");
      return false;
    }
    if (r.getEntries() == 0) {
      LOG.warn("StoreFile " + f + " is a empty store file");
      return false;
    }
    // TODO: Cache these keys rather than make each time?
    byte [] fk = r.getFirstKey();
    if (fk == null) return false;
    KeyValue firstKV = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
    byte [] lk = r.getLastKey();
    KeyValue lastKV = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
    KeyValue firstOnRow = state.getTargetKey();
    if (this.comparator.compareRows(lastKV, firstOnRow) < 0) {
      // If last key in file is not of the target table, no candidates in this
      // file.  Return.
      if (!state.isTargetTable(lastKV)) return false;
      // If the row we're looking for is past the end of file, set search key to
      // last key. TODO: Cache last and first key rather than make each time.
      firstOnRow = new KeyValue(lastKV.getRow(), HConstants.LATEST_TIMESTAMP);
    }
    // Get a scanner that caches blocks and that uses pread.
    HFileScanner scanner = r.getScanner(true, true, false);
    // Seek scanner.  If can't seek it, return.
    if (!seekToScanner(scanner, firstOnRow, firstKV)) return false;
    // If we found candidate on firstOnRow, just return. THIS WILL NEVER HAPPEN!
    // Unlikely that there'll be an instance of actual first row in table.
    if (walkForwardInSingleRow(scanner, firstOnRow, state)) return true;
    // If here, need to start backing up.
    while (scanner.seekBefore(firstOnRow.getBuffer(), firstOnRow.getKeyOffset(),
       firstOnRow.getKeyLength())) {
      Cell kv = scanner.getKeyValue();
      if (!state.isTargetTable(kv)) break;
      if (!state.isBetterCandidate(kv)) break;
      // Make new first on row.
      firstOnRow = new KeyValue(kv.getRow(), HConstants.LATEST_TIMESTAMP);
      // Seek scanner.  If can't seek it, break.
      if (!seekToScanner(scanner, firstOnRow, firstKV)) return false;
      // If we find something, break;
      if (walkForwardInSingleRow(scanner, firstOnRow, state)) return true;
    }
    return false;
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
    int result = scanner.seekTo(kv);
    return result != -1;
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
      Cell kv = scanner.getKeyValue();
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

  @Override
  public boolean canSplit() {
    this.lock.readLock().lock();
    try {
      // Not split-able if we find a reference store file present in the store.
      boolean result = !hasReferences();
      if (!result && LOG.isDebugEnabled()) {
        LOG.debug("Cannot split region due to reference files being there");
      }
      return result;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public byte[] getSplitPoint() {
    this.lock.readLock().lock();
    try {
      // Should already be enforced by the split policy!
      assert !this.getRegionInfo().isMetaRegion();
      // Not split-able if we find a reference store file present in the store.
      if (hasReferences()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Not splittable; has references: " + this);
        }
        return null;
      }
      return this.storeEngine.getStoreFileManager().getSplitPoint();
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
    return storeSize.get();
  }

  @Override
  public void triggerMajorCompaction() {
    this.forceMajor = true;
  }


  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public KeyValueScanner getScanner(Scan scan,
      final NavigableSet<byte []> targetCols, long readPt) throws IOException {
    lock.readLock().lock();
    try {
      KeyValueScanner scanner = null;
      if (this.getCoprocessorHost() != null) {
        scanner = this.getCoprocessorHost().preStoreScannerOpen(this, scan, targetCols);
      }
      if (scanner == null) {
        scanner = scan.isReversed() ? new ReversedStoreScanner(this,
            getScanInfo(), scan, targetCols, readPt) : new StoreScanner(this,
            getScanInfo(), scan, targetCols, readPt);
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
    return this.storeEngine.getStoreFileManager().getStorefileCount();
  }

  @Override
  public long getMaxStoreFileAge() {
    long earliestTS = Long.MAX_VALUE;
    for (StoreFile s: this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      if (!s.isHFile()) {
        continue;
      }
      long createdTS = s.getFileInfo().getCreatedTimestamp();
      earliestTS = (createdTS < earliestTS) ? createdTS : earliestTS;
    }
    long now = EnvironmentEdgeManager.currentTime();
    return now - earliestTS;
  }

  @Override
  public long getMinStoreFileAge() {
    long latestTS = 0;
    for (StoreFile s: this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      if (!s.isHFile()) {
        continue;
      }
      long createdTS = s.getFileInfo().getCreatedTimestamp();
      latestTS = (createdTS > latestTS) ? createdTS : latestTS;
    }
    long now = EnvironmentEdgeManager.currentTime();
    return now - latestTS;
  }

  @Override
  public long getAvgStoreFileAge() {
    long sum = 0, count = 0;
    for (StoreFile s: this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      if (!s.isHFile()) {
        continue;
      }
      sum += s.getFileInfo().getCreatedTimestamp();
      count++;
    }
    if (count == 0) {
      return 0;
    }
    long avgTS = sum / count;
    long now = EnvironmentEdgeManager.currentTime();
    return now - avgTS;
  }

  @Override
  public long getNumReferenceFiles() {
    long numRefFiles = 0;
    for (StoreFile s : this.storeEngine.getStoreFileManager().getStorefiles()) {
      if (s.isReference()) {
        numRefFiles++;
      }
    }
    return numRefFiles;
  }

  @Override
  public long getNumHFiles() {
    long numHFiles = 0;
    for (StoreFile s : this.storeEngine.getStoreFileManager().getStorefiles()) {
      if (s.isHFile()) {
        numHFiles++;
      }
    }
    return numHFiles;
  }

  @Override
  public long getStoreSizeUncompressed() {
    return this.totalUncompressedBytes.get();
  }

  private long getTotalUmcompressedBytes(Collection<StoreFile> files) {
    long size = 0;
    for (StoreFile sf : files) {
      if (sf != null && sf.getReader() != null) {
        size += sf.getReader().getTotalUncompressedBytes();
      }
    }
    return size;
  }

  private long getStorefilesSize(Collection<StoreFile> files) {
    long size = 0;
    for (StoreFile sf : files) {
      if (sf != null) {
        if (sf.getReader() == null) {
          LOG.warn("StoreFile " + sf + " has a null Reader");
          continue;
        }
        size += sf.getReader().length();
      }
    }
    return size;
  }

  @Override
  public long getStorefilesSize() {
    return getStorefilesSize(storeEngine.getStoreFileManager().getStorefiles());
  }

  @Override
  public long getStorefilesIndexSize() {
    long size = 0;
    for (StoreFile s: this.storeEngine.getStoreFileManager().getStorefiles()) {
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
    for (StoreFile s : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        continue;
      }
      size += r.getUncompressedDataIndexSize();
    }
    return size;
  }

  @Override
  public long getTotalStaticBloomSize() {
    long size = 0;
    for (StoreFile s : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        continue;
      }
      size += r.getTotalBloomSize();
    }
    return size;
  }

  @Override
  public long getMemStoreSize() {
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

  @Override
  public boolean throttleCompaction(long compactionSize) {
    return storeEngine.getCompactionPolicy().throttleCompaction(compactionSize);
  }

  public HRegion getHRegion() {
    return this.region;
  }

  @Override
  public RegionCoprocessorHost getCoprocessorHost() {
    return this.region.getCoprocessorHost();
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return this.fs.getRegionInfo();
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
      long now = EnvironmentEdgeManager.currentTime();

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
  public long upsert(Iterable<Cell> cells, long readpoint,
        List<Cell> removedCells) throws IOException {
    this.lock.readLock().lock();
    try {
      return this.memstore.upsert(cells, readpoint, removedCells);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public StoreFlushContext createFlushContext(long cacheFlushId) {
    return new StoreFlusherImpl(cacheFlushId);
  }

  private final class StoreFlusherImpl implements StoreFlushContext {

    private long cacheFlushSeqNum;
    private MemStoreSnapshot snapshot;
    private List<Path> tempFiles;
    private List<Path> committedFiles;
    private long cacheFlushCount;
    private long cacheFlushSize;
    private long outputFileSize;

    private StoreFlusherImpl(long cacheFlushSeqNum) {
      this.cacheFlushSeqNum = cacheFlushSeqNum;
    }

    /**
     * This is not thread safe. The caller should have a lock on the region or the store.
     * If necessary, the lock can be added with the patch provided in HBASE-10087
     */
    @Override
    public void prepare() {
      this.snapshot = memstore.snapshot();
      this.cacheFlushCount = snapshot.getCellsCount();
      this.cacheFlushSize = snapshot.getSize();
      committedFiles = new ArrayList<Path>(1);
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      RegionServerServices rsService = region.getRegionServerServices();
      ThroughputController throughputController =
          rsService == null ? null : rsService.getFlushThroughputController();
      tempFiles = HStore.this.flushCache(cacheFlushSeqNum, snapshot, status, throughputController);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      if (this.tempFiles == null || this.tempFiles.isEmpty()) {
        return false;
      }
      List<StoreFile> storeFiles = new ArrayList<StoreFile>(this.tempFiles.size());
      for (Path storeFilePath : tempFiles) {
        try {
          StoreFile sf = HStore.this.commitFile(storeFilePath, cacheFlushSeqNum, status);
          outputFileSize += sf.getReader().length();
          storeFiles.add(sf);
        } catch (IOException ex) {
          LOG.error("Failed to commit store file " + storeFilePath, ex);
          // Try to delete the files we have committed before.
          for (StoreFile sf : storeFiles) {
            Path pathToDelete = sf.getPath();
            try {
              sf.deleteReader();
            } catch (IOException deleteEx) {
              LOG.fatal("Failed to delete store file we committed, halting " + pathToDelete, ex);
              Runtime.getRuntime().halt(1);
            }
          }
          throw new IOException("Failed to commit the flush", ex);
        }
      }

      for (StoreFile sf : storeFiles) {
        if (HStore.this.getCoprocessorHost() != null) {
          HStore.this.getCoprocessorHost().postFlush(HStore.this, sf);
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
     * @throws IOException
     */
    @Override
    public void replayFlush(List<String> fileNames, boolean dropMemstoreSnapshot)
        throws IOException {
      List<StoreFile> storeFiles = new ArrayList<StoreFile>(fileNames.size());
      for (String file : fileNames) {
        // open the file as a store file (hfile link, etc)
        StoreFileInfo storeFileInfo = fs.getStoreFileInfo(getColumnFamilyName(), file);
        StoreFile storeFile = createStoreFileAndReader(storeFileInfo);
        storeFiles.add(storeFile);
        HStore.this.storeSize.addAndGet(storeFile.getReader().length());
        HStore.this.totalUncompressedBytes.addAndGet(
          storeFile.getReader().getTotalUncompressedBytes());
        if (LOG.isInfoEnabled()) {
          LOG.info("Region: " + HStore.this.getRegionInfo().getEncodedName() +
            " added " + storeFile + ", entries=" + storeFile.getReader().getEntries() +
            ", sequenceid=" + storeFile.getReader().getSequenceID() +
            ", filesize=" + StringUtils.humanReadableInt(storeFile.getReader().length()));
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
     * @throws IOException
     */
    @Override
    public void abort() throws IOException {
      if (snapshot == null) {
        return;
      }
      HStore.this.updateStorefiles(new ArrayList<StoreFile>(0), snapshot.getId());
    }
  }

  @Override
  public boolean needsCompaction() {
    List<StoreFile> filesCompactingClone = null;
    synchronized (filesCompacting) {
      filesCompactingClone = Lists.newArrayList(filesCompacting);
    }
    return this.storeEngine.needsCompaction(filesCompactingClone);
  }

  @Override
  public CacheConfig getCacheConfig() {
    return this.cacheConf;
  }

  public static final long FIXED_OVERHEAD =
      ClassSize.align(ClassSize.OBJECT + (26 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG)
              + (5 * Bytes.SIZEOF_INT) + (2 * Bytes.SIZEOF_BOOLEAN));

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD
      + ClassSize.OBJECT + ClassSize.REENTRANT_LOCK
      + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT
      + ScanInfo.FIXED_OVERHEAD);

  @Override
  public long heapSize() {
    return DEEP_OVERHEAD + this.memstore.heapSize();
  }

  @Override
  public KeyValue.KVComparator getComparator() {
    return comparator;
  }

  @Override
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
  @VisibleForTesting
  public StoreEngine<?, ?, ?, ?> getStoreEngine() {
    return this.storeEngine;
  }

  protected OffPeakHours getOffPeakHours() {
    return this.offPeakHours;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = new CompoundConfiguration()
            .add(conf)
            .addWritableMap(family.getValues());
    this.storeEngine.compactionPolicy.setConf(conf);
    this.offPeakHours = OffPeakHours.getInstance(conf);
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
	   return getRegionInfo().getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID;
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

  @Override
  public synchronized void closeAndArchiveCompactedFiles() throws IOException {
    // ensure other threads do not attempt to archive the same files on close()
    archiveLock.lock();
    try {
      lock.readLock().lock();
      Collection<StoreFile> copyCompactedfiles = null;
      try {
        Collection<StoreFile> compactedfiles = getCompactedfiles();
        if (compactedfiles != null && compactedfiles.size() != 0) {
          // Do a copy under read lock
          copyCompactedfiles = new ArrayList<StoreFile>(compactedfiles);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("No compacted files to archive");
            return;
          }
        }
      } finally {
        lock.readLock().unlock();
      }
      if (copyCompactedfiles != null && !copyCompactedfiles.isEmpty()) {
        removeCompactedfiles(copyCompactedfiles);
      }
    } finally {
      archiveLock.unlock();
    }
  }

  /**
   * Archives and removes the compacted files
   * @param compactedfiles The compacted files in this store that are not active in reads
   * @throws IOException
   */
  private void removeCompactedfiles(Collection<StoreFile> compactedfiles)
      throws IOException {
    final List<StoreFile> filesToRemove = new ArrayList<StoreFile>(compactedfiles.size());
    for (final StoreFile file : compactedfiles) {
      synchronized (file) {
        try {
          StoreFile.Reader r = file.getReader();

          if (r == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("The file " + file + " was closed but still not archived.");
            }
            filesToRemove.add(file);
          }

          if (r != null && r.isCompactedAway() && !r.isReferencedInReads()) {
            // Even if deleting fails we need not bother as any new scanners won't be
            // able to use the compacted file as the status is already compactedAway
            if (LOG.isTraceEnabled()) {
              LOG.trace("Closing and archiving the file " + file.getPath());
            }
            r.close(true);
            // Just close and return
            filesToRemove.add(file);
          } else {
            if (r != null) {
              LOG.info("Can't archive compacted file " + file.getPath()
                + " because of either isCompactedAway=" + r.isCompactedAway()
                + " or file has reference, isReferencedInReads=" + r.isReferencedInReads()
                + ", refCount=" + r.getRefCount() + ", skipping for now.");
            } else {
              LOG.info("Can't archive compacted file " + file.getPath() + ", skipping for now.");
            }
          }
        } catch (Exception e) {
          LOG.error(
            "Exception while trying to close the compacted store file " + file.getPath().getName());
        }
      }
    }
    if (this.isPrimaryReplicaStore()) {
      // Only the primary region is allowed to move the file to archive.
      // The secondary region does not move the files to archive. Any active reads from
      // the secondary region will still work because the file as such has active readers on it.
      if (!filesToRemove.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Moving the files " + filesToRemove + " to archive");
        }
        // Only if this is successful it has to be removed
        try {
          this.fs.removeStoreFiles(this.getFamily().getNameAsString(), filesToRemove);
        } catch (FailedArchiveException fae) {
          // Even if archiving some files failed, we still need to clear out any of the
          // files which were successfully archived.  Otherwise we will receive a
          // FileNotFoundException when we attempt to re-archive them in the next go around.
          Collection<Path> failedFiles = fae.getFailedFiles();
          Iterator<StoreFile> iter = filesToRemove.iterator();
          while (iter.hasNext()) {
            if (failedFiles.contains(iter.next().getPath())) {
              iter.remove();
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
    }
  }

  private void clearCompactedfiles(final List<StoreFile> filesToRemove) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Clearing the compacted file " + filesToRemove + " from this store");
    }
    try {
      lock.writeLock().lock();
      this.getStoreEngine().getStoreFileManager().removeCompactedFiles(filesToRemove);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int getStoreRefCount() {
    int refCount = 0;
    for (StoreFile store: storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = store.getReader();
      if (r != null) {
        refCount += r.getRefCount();
      }
    }
    return refCount;
  }

  /**
   * @return get maximum ref count of storeFile among all HStore Files
   *   for the HStore
   */
  public int getMaxStoreFileRefCount() {
    int maxStoreFileRefCount = 0;
    for (StoreFile store : storeEngine.getStoreFileManager().getStorefiles()) {
      if (store.isHFile()) {
        StoreFile.Reader storeReader = store.getReader();
        if (storeReader != null) {
          maxStoreFileRefCount = Math.max(maxStoreFileRefCount, storeReader.getRefCount());
        }
      }
    }
    return maxStoreFileRefCount;
  }

}
