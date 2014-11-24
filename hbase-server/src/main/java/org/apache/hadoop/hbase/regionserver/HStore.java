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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
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
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
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
@InterfaceAudience.Private
public class HStore implements Store {
  private static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";
  public static final String COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY =
      "hbase.server.compactchecker.interval.multiplier";
  public static final String BLOCKING_STOREFILES_KEY = "hbase.hstore.blockingStoreFiles";
  public static final int DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER = 1000;
  public static final int DEFAULT_BLOCKING_STOREFILE_COUNT = 7;

  static final Log LOG = LogFactory.getLog(HStore.class);

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
  private volatile long storeSize = 0L;
  private volatile long totalUncompressedBytes = 0L;

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
  private final boolean verifyBulkLoads;

  private ScanInfo scanInfo;

  // TODO: ideally, this should be part of storeFileManager, as we keep passing this to it.
  final List<StoreFile> filesCompacting = Lists.newArrayList();

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

  private volatile long flushedCellsCount = 0;
  private volatile long compactedCellsCount = 0;
  private volatile long majorCompactedCellsCount = 0;
  private volatile long flushedCellsSize = 0;
  private volatile long compactedCellsSize = 0;
  private volatile long majorCompactedCellsSize = 0;

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
      .addBytesMap(family.getValues());
    this.blocksize = family.getBlocksize();

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
    scanInfo = new ScanInfo(family, ttl, timeToPurgeDeletes, this.comparator);
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
    this.storeEngine.getStoreFileManager().loadFiles(loadStoreFiles());

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
          throw new RuntimeException("Cipher '" + cipher + "' is not available");
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
          throw new RuntimeException("Cipher '" + cipher + "' is not available");
        }
        key = cipher.getRandomKey();
      }
      cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
    }
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
      return HFile.DEFAULT_CHECKSUM_TYPE;
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
  long getMaxSequenceId() {
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

    ArrayList<StoreFile> results = new ArrayList<StoreFile>(files.size());
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
      for (StoreFile file : results) {
        try {
          if (file != null) file.closeReader(true);
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
      }
      throw ioe;
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
    StoreFileManager sfm = storeEngine.getStoreFileManager();
    Collection<StoreFile> currentFiles = sfm.getStorefiles();
    if (currentFiles == null) currentFiles = new ArrayList<StoreFile>(0);

    Collection<StoreFileInfo> newFiles = fs.getStoreFiles(getColumnFamilyName());
    if (newFiles == null) newFiles = new ArrayList<StoreFileInfo>(0);

    HashMap<StoreFileInfo, StoreFile> currentFilesSet = new HashMap<StoreFileInfo, StoreFile>(currentFiles.size());
    for (StoreFile sf : currentFiles) {
      currentFilesSet.put(sf.getFileInfo(), sf);
    }
    HashSet<StoreFileInfo> newFilesSet = new HashSet<StoreFileInfo>(newFiles);

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
      region.getMVCC().advanceMemstoreReadPointIfNeeded(this.getMaxSequenceId());
    }

    // notify scanners, close file readers, and recompute store size
    completeCompaction(toBeRemovedStoreFiles, false);
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
    storeFile.createReader();
    return storeFile;
  }

  @Override
  public Pair<Long, Cell> add(final Cell cell) {
    lock.readLock().lock();
    try {
       return this.memstore.add(cell);
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
                  + CellUtil.getCellKey(prevCell) + " current="
                  + CellUtil.getCellKey(cell));
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

  @Override
  public void bulkLoadHFile(String srcPathStr, long seqNum) throws IOException {
    Path srcPath = new Path(srcPathStr);
    Path dstPath = fs.bulkLoadStoreFile(getColumnFamilyName(), srcPath, seqNum);

    StoreFile sf = createStoreFileAndReader(dstPath);

    StoreFile.Reader r = sf.getReader();
    this.storeSize += r.length();
    this.totalUncompressedBytes += r.getTotalUncompressedBytes();

    LOG.info("Loaded HFile " + srcPath + " into store '" + getColumnFamilyName() +
        "' as " + dstPath + " - updating store file list.");

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
    notifyChangedReadersObservers();
    LOG.info("Successfully loaded store file " + srcPath
        + " into store " + this + " (new location: " + dstPath + ")");
    if (LOG.isTraceEnabled()) {
      String traceMessage = "BULK LOAD time,size,store size,store files ["
          + EnvironmentEdgeManager.currentTime() + "," + r.length() + "," + storeSize
          + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
  }

  @Override
  public ImmutableCollection<StoreFile> close() throws IOException {
    this.lock.writeLock().lock();
    try {
      // Clear so metrics doesn't find them.
      ImmutableCollection<StoreFile> result = storeEngine.getStoreFileManager().clearFiles();

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
   * {@link #flushCache(long, MemStoreSnapshot, MonitoredTask)}
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
   * Write out current snapshot.  Presumes {@link #snapshot()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @param snapshot
   * @param status
   * @return The path name of the tmp file to which the store was flushed
   * @throws IOException
   */
  protected List<Path> flushCache(final long logCacheFlushId, MemStoreSnapshot snapshot,
      MonitoredTask status) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    // Retry after catching exception when flushing, otherwise server will abort
    // itself
    StoreFlusher flusher = storeEngine.getStoreFlusher();
    IOException lastException = null;
    for (int i = 0; i < flushRetriesNumber; i++) {
      try {
        List<Path> pathNames = flusher.flushSnapshot(snapshot, logCacheFlushId, status);
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
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includesMVCCReadPoint - whether to include MVCC or not
   * @param includesTag - includesTag or not
   * @return Writer for a new StoreFile in the tmp dir.
   */
  @Override
  public StoreFile.Writer createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
      boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag)
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
    HFileContext hFileContext = createFileContext(compression, includeMVCCReadpoint, includesTag,
      cryptoContext);
    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf,
        this.getFileSystem())
            .withFilePath(fs.createTempName())
            .withComparator(comparator)
            .withBloomType(family.getBloomFilterType())
            .withMaxKeyCount(maxKeyCount)
            .withFavoredNodes(favoredNodes)
            .withFileContext(hFileContext)
            .build();
    return w;
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
                                .withCompressTags(family.shouldCompressTags())
                                .withChecksumType(checksumType)
                                .withBytesPerCheckSum(bytesPerChecksum)
                                .withBlockSize(blocksize)
                                .withHBaseCheckSum(true)
                                .withDataBlockEncoding(family.getDataBlockEncoding())
                                .withEncryptionContext(cryptoContext)
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
      this.memstore.clearSnapshot(snapshotId);
    } finally {
      // We need the lock, as long as we are updating the storeFiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      this.lock.writeLock().unlock();
    }

    // Tell listeners of the change in readers.
    notifyChangedReadersObservers();

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
  @Override
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean isGet,
      boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow,
      byte[] stopRow, long readPt) throws IOException {
    Collection<StoreFile> storeFilesToScan;
    List<KeyValueScanner> memStoreScanners;
    this.lock.readLock().lock();
    try {
      storeFilesToScan =
          this.storeEngine.getStoreFileManager().getFilesForScanOrGet(isGet, startRow, stopRow);
      memStoreScanners = this.memstore.getScanners(readPt);
    } finally {
      this.lock.readLock().unlock();
    }

    // First the store file scanners

    // TODO this used to get the store files in descending order,
    // but now we get them in ascending order, which I think is
    // actually more correct, since memstore get put at the end.
    List<StoreFileScanner> sfScanners = StoreFileScanner
      .getScannersForStoreFiles(storeFilesToScan, cacheBlocks, usePread, isCompaction, matcher,
        readPt);
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(sfScanners.size()+1);
    scanners.addAll(sfScanners);
    // Then the memstore scanners
    scanners.addAll(memStoreScanners);
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
  public List<StoreFile> compact(CompactionContext compaction) throws IOException {
    assert compaction != null && compaction.hasSelection();
    CompactionRequest cr = compaction.getRequest();
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
        + StringUtils.humanReadableInt(cr.getSize()));

    long compactionStartTime = EnvironmentEdgeManager.currentTime();
    List<StoreFile> sfs = null;
    try {
      // Commence the compaction.
      List<Path> newFiles = compaction.compact();

      // TODO: get rid of this!
      if (!this.conf.getBoolean("hbase.hstore.compaction.complete", true)) {
        LOG.warn("hbase.hstore.compaction.complete is set to false");
        sfs = new ArrayList<StoreFile>(newFiles.size());
        for (Path newFile : newFiles) {
          // Create storefile around what we wrote with a reader on it.
          StoreFile sf = createStoreFileAndReader(newFile);
          sf.closeReader(true);
          sfs.add(sf);
        }
        return sfs;
      }
      // Do the steps necessary to complete the compaction.
      sfs = moveCompatedFilesIntoPlace(cr, newFiles);
      writeCompactionWalRecord(filesToCompact, sfs);
      replaceStoreFiles(filesToCompact, sfs);
      if (cr.isMajor()) {
        majorCompactedCellsCount += getCompactionProgress().totalCompactingKVs;
        majorCompactedCellsSize += getCompactionProgress().totalCompactedSize;
      } else {
        compactedCellsCount += getCompactionProgress().totalCompactingKVs;
        compactedCellsSize += getCompactionProgress().totalCompactedSize;
      }
      // At this point the store will use new files for all new scanners.
      completeCompaction(filesToCompact, true); // Archive old files & update store size.
    } finally {
      finishCompactionRequest(cr);
    }
    logCompactionEndMessage(cr, sfs, compactionStartTime);
    return sfs;
  }

  private List<StoreFile> moveCompatedFilesIntoPlace(
      CompactionRequest cr, List<Path> newFiles) throws IOException {
    List<StoreFile> sfs = new ArrayList<StoreFile>(newFiles.size());
    for (Path newFile : newFiles) {
      assert newFile != null;
      StoreFile sf = moveFileIntoPlace(newFile);
      if (this.getCoprocessorHost() != null) {
        this.getCoprocessorHost().postCompact(this, sf, cr);
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
    if (region.getLog() == null) return;
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
    HLogUtil.writeCompactionMarker(region.getLog(), this.region.getTableDesc(),
        this.region.getRegionInfo(), compactionDescriptor, this.region.getSequenceId());
  }

  @VisibleForTesting
  void replaceStoreFiles(final Collection<StoreFile> compactedFiles,
      final Collection<StoreFile> result) throws IOException {
    this.lock.writeLock().lock();
    try {
      this.storeEngine.getStoreFileManager().addCompactionResults(compactedFiles, result);
      filesCompacting.removeAll(compactedFiles); // safe bc: lock.writeLock();
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
      CompactionRequest cr, List<StoreFile> sfs, long compactionStartTime) {
    long now = EnvironmentEdgeManager.currentTime();
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
        message.append(StringUtils.humanReadableInt(sf.getReader().length()));
        message.append("), ");
      }
    }
    message.append("total size for store is ")
      .append(StringUtils.humanReadableInt(storeSize))
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
  public void completeCompactionMarker(CompactionDescriptor compaction)
      throws IOException {
    LOG.debug("Completing compaction from the WAL marker");
    List<String> compactionInputs = compaction.getCompactionInputList();

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
    List<Path> inputPaths = new ArrayList<Path>(compactionInputs.size());
    for (String compactionInput : compactionInputs) {
      Path inputPath = fs.getStoreFilePath(familyName, compactionInput);
      inputPaths.add(inputPath);
    }

    //some of the input files might already be deleted
    List<StoreFile> inputStoreFiles = new ArrayList<StoreFile>(compactionInputs.size());
    for (StoreFile sf : this.getStorefiles()) {
      if (inputPaths.contains(sf.getQualifiedPath())) {
        inputStoreFiles.add(sf);
      }
    }

    this.replaceStoreFiles(inputStoreFiles, Collections.EMPTY_LIST);
    this.completeCompaction(inputStoreFiles);
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
        Collections.sort(filesCompacting, StoreFile.Comparators.SEQ_ID);
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
          this.getCoprocessorHost().postCompact(this, sf, null);
        }
        replaceStoreFiles(filesToCompact, Lists.newArrayList(sf));
        completeCompaction(filesToCompact, true);
      }
    } finally {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(filesToCompact);
      }
    }
  }

  @Override
  public boolean hasReferences() {
    return StoreUtils.hasReferences(this.storeEngine.getStoreFileManager().getStorefiles());
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
    return storeEngine.getCompactionPolicy().isMajorCompaction(
        this.storeEngine.getStoreFileManager().getStorefiles());
  }

  @Override
  public CompactionContext requestCompaction() throws IOException {
    return requestCompaction(Store.NO_PRIORITY, null);
  }

  @Override
  public CompactionContext requestCompaction(int priority, CompactionRequest baseRequest)
      throws IOException {
    // don't even select for compaction if writes are disabled
    if (!this.areWritesEnabled()) {
      return null;
    }

    // Before we do compaction, try to get rid of unneeded files to simplify things.
    removeUnneededFiles();

    CompactionContext compaction = storeEngine.createCompaction();
    CompactionRequest request = null;
    this.lock.readLock().lock();
    try {
      synchronized (filesCompacting) {
        // First, see if coprocessor would want to override selection.
        if (this.getCoprocessorHost() != null) {
          List<StoreFile> candidatesForCoproc = compaction.preSelect(this.filesCompacting);
          boolean override = this.getCoprocessorHost().preCompactSelection(
              this, candidatesForCoproc, baseRequest);
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
              this, ImmutableList.copyOf(compaction.getRequest().getFiles()), baseRequest);
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
    Collections.sort(filesCompacting, StoreFile.Comparators.SEQ_ID);
  }

  private void removeUnneededFiles() throws IOException {
    if (!conf.getBoolean("hbase.store.delete.expired.storefile", true)) return;
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
        + "; total size for store is " + StringUtils.humanReadableInt(storeSize));
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
    try {
      // Do not delete old store files until we have sent out notification of
      // change in case old files are still being accessed by outstanding scanners.
      // Don't do this under writeLock; see HBASE-4485 for a possible deadlock
      // scenario that could have happened if continue to hold the lock.
      notifyChangedReadersObservers();
      // At this point the store will use new files for all scanners.

      // let the archive util decide if we should archive or delete the files
      LOG.debug("Removing store files after compaction...");
      for (StoreFile compactedFile : compactedFiles) {
        compactedFile.closeReader(true);
      }
      if (removeFiles) {
        this.fs.removeStoreFiles(this.getColumnFamilyName(), compactedFiles);
      }
    } catch (IOException e) {
      e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
      LOG.error("Failed removing compacted files in " + this +
        ". Files we were trying to remove are " + compactedFiles.toString() +
        "; some of them may have been already removed", e);
    }

    // 4. Compute new store size
    this.storeSize = 0L;
    this.totalUncompressedBytes = 0L;
    for (StoreFile hsf : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = hsf.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + hsf + " has a null Reader");
        continue;
      }
      this.storeSize += r.length();
      this.totalUncompressedBytes += r.getTotalUncompressedBytes();
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

  static boolean isExpired(final Cell key, final long oldestTimestamp) {
    return key.getTimestamp() < oldestTimestamp;
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
    return storeSize;
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
  // TODO: why is there this and also getNumberOfStorefiles?! Remove one.
  public int getStorefilesCount() {
    return this.storeEngine.getStoreFileManager().getStorefileCount();
  }

  @Override
  public long getStoreSizeUncompressed() {
    return this.totalUncompressedBytes;
  }

  @Override
  public long getStorefilesSize() {
    long size = 0;
    for (StoreFile s: this.storeEngine.getStoreFileManager().getStorefiles()) {
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
      size += s.getReader().getUncompressedDataIndexSize();
    }
    return size;
  }

  @Override
  public long getTotalStaticBloomSize() {
    long size = 0;
    for (StoreFile s : this.storeEngine.getStoreFileManager().getStorefiles()) {
      StoreFile.Reader r = s.getReader();
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
  public long upsert(Iterable<Cell> cells, long readpoint) throws IOException {
    this.lock.readLock().lock();
    try {
      return this.memstore.upsert(cells, readpoint);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public StoreFlushContext createFlushContext(long cacheFlushId) {
    return new StoreFlusherImpl(cacheFlushId);
  }

  private class StoreFlusherImpl implements StoreFlushContext {

    private long cacheFlushSeqNum;
    private MemStoreSnapshot snapshot;
    private List<Path> tempFiles;
    private List<Path> committedFiles;
    private long cacheFlushCount;
    private long cacheFlushSize;

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
      tempFiles = HStore.this.flushCache(cacheFlushSeqNum, snapshot, status);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      if (this.tempFiles == null || this.tempFiles.isEmpty()) {
        return false;
      }
      List<StoreFile> storeFiles = new ArrayList<StoreFile>(this.tempFiles.size());
      for (Path storeFilePath : tempFiles) {
        try {
          storeFiles.add(HStore.this.commitFile(storeFilePath, cacheFlushSeqNum, status));
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

      HStore.this.flushedCellsCount += cacheFlushCount;
      HStore.this.flushedCellsSize += cacheFlushSize;

      // Add new file to store files.  Clear snapshot too while we have the Store write lock.
      return HStore.this.updateStorefiles(storeFiles, snapshot.getId());
    }

    @Override
    public List<Path> getCommittedFiles() {
      return committedFiles;
    }
  }

  @Override
  public boolean needsCompaction() {
    return this.storeEngine.needsCompaction(this.filesCompacting);
  }

  @Override
  public CacheConfig getCacheConfig() {
    return this.cacheConf;
  }

  public static final long FIXED_OVERHEAD =
      ClassSize.align(ClassSize.OBJECT + (16 * ClassSize.REFERENCE) + (10 * Bytes.SIZEOF_LONG)
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
    return flushedCellsCount;
  }

  @Override
  public long getFlushedCellsSize() {
    return flushedCellsSize;
  }

  @Override
  public long getCompactedCellsCount() {
    return compactedCellsCount;
  }

  @Override
  public long getCompactedCellsSize() {
    return compactedCellsSize;
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return majorCompactedCellsCount;
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return majorCompactedCellsSize;
  }

  /**
   * Returns the StoreEngine that is backing this concrete implementation of Store.
   * @return Returns the {@link StoreEngine} object used internally inside this HStore object.
   */
  protected StoreEngine<?, ?, ?, ?> getStoreEngine() {
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
            .addBytesMap(family.getValues());
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
}
