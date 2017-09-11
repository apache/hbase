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
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, instantiate a writer using {@link StoreFileWriter.Builder}
 * and append data. Be sure to add any metadata before calling close on the
 * Writer (Use the appendMetadata convenience methods). On close, a StoreFile
 * is sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #initReader()}
 * <p>StoreFiles may also reference store files in another Store.
 *
 * The reason for this weird pattern where you use a different instance for the
 * writer and a reader is that we write once but read a lot more.
 */
@InterfaceAudience.Private
public class HStoreFile implements StoreFile {

  private static final Log LOG = LogFactory.getLog(HStoreFile.class.getName());

  private static final boolean DEFAULT_STORE_FILE_READER_NO_READAHEAD = false;

  private final StoreFileInfo fileInfo;
  private final FileSystem fs;

  // Block cache configuration and reference.
  private final CacheConfig cacheConf;

  // Counter that is incremented every time a scanner is created on the
  // store file. It is decremented when the scan on the store file is
  // done.
  private final AtomicInteger refCount = new AtomicInteger(0);

  private final boolean noReadahead;

  private final boolean primaryReplica;

  // Indicates if the file got compacted
  private volatile boolean compactedAway = false;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // max of the MemstoreTS in the KV's in this store
  // Set when we obtain a Reader.
  private long maxMemstoreTS = -1;

  // firstKey, lastkey and cellComparator will be set when openReader.
  private Cell firstKey;

  private Cell lastKey;

  private Comparator<Cell> comparator;

  @Override
  public CacheConfig getCacheConf() {
    return cacheConf;
  }

  @Override
  public Cell getFirstKey() {
    return firstKey;
  }

  @Override
  public Cell getLastKey() {
    return lastKey;
  }

  @Override
  public Comparator<Cell> getComparator() {
    return comparator;
  }

  @Override
  public long getMaxMemstoreTS() {
    return maxMemstoreTS;
  }

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  // If true, this file should not be included in minor compactions.
  // It's set whenever you get a Reader.
  private boolean excludeFromMinorCompaction = false;

  /**
   * Map of the metadata entries in the corresponding HFile. Populated when Reader is opened
   * after which it is not modified again.
   */
  private Map<byte[], byte[]> metadataMap;

  // StoreFile.Reader
  private volatile StoreFileReader reader;

  /**
   * Bloom filter type specified in column family configuration. Does not
   * necessarily correspond to the Bloom filter type present in the HFile.
   */
  private final BloomType cfBloomType;

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs The current file system to use.
   * @param p The path of the file.
   * @param conf The current configuration.
   * @param cacheConf The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified by column family
   *          configuration. This may or may not be the same as the Bloom filter type actually
   *          present in the HFile, because column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @deprecated Now we will specific whether the StoreFile is for primary replica when
   *             constructing, so please use {@link #HStoreFile(FileSystem, Path, Configuration,
   *             CacheConfig, BloomType, boolean)} directly.
   */
  @Deprecated
  public HStoreFile(final FileSystem fs, final Path p, final Configuration conf,
      final CacheConfig cacheConf, final BloomType cfBloomType) throws IOException {
    this(fs, new StoreFileInfo(conf, fs, p), conf, cacheConf, cfBloomType);
  }

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs The current file system to use.
   * @param p The path of the file.
   * @param conf The current configuration.
   * @param cacheConf The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified by column family
   *          configuration. This may or may not be the same as the Bloom filter type actually
   *          present in the HFile, because column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   * @throws IOException
   */
  public HStoreFile(FileSystem fs, Path p, Configuration conf, CacheConfig cacheConf,
      BloomType cfBloomType, boolean primaryReplica) throws IOException {
    this(fs, new StoreFileInfo(conf, fs, p), conf, cacheConf, cfBloomType, primaryReplica);
  }

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs The current file system to use.
   * @param fileInfo The store file information.
   * @param conf The current configuration.
   * @param cacheConf The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified by column family
   *          configuration. This may or may not be the same as the Bloom filter type actually
   *          present in the HFile, because column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @deprecated Now we will specific whether the StoreFile is for primary replica when
   *             constructing, so please use {@link #HStoreFile(FileSystem, StoreFileInfo,
   *             Configuration, CacheConfig, BloomType, boolean)} directly.
   */
  @Deprecated
  public HStoreFile(final FileSystem fs, final StoreFileInfo fileInfo, final Configuration conf,
      final CacheConfig cacheConf, final BloomType cfBloomType) throws IOException {
    this(fs, fileInfo, conf, cacheConf, cfBloomType, true);
  }

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs fs The current file system to use.
   * @param fileInfo The store file information.
   * @param conf The current configuration.
   * @param cacheConf The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified by column
   *          family configuration. This may or may not be the same as the Bloom filter type
   *          actually present in the HFile, because column family configuration might change. If
   *          this is {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   */
  public HStoreFile(FileSystem fs, StoreFileInfo fileInfo, Configuration conf, CacheConfig cacheConf,
      BloomType cfBloomType, boolean primaryReplica) {
    this.fs = fs;
    this.fileInfo = fileInfo;
    this.cacheConf = cacheConf;
    this.noReadahead =
        conf.getBoolean(STORE_FILE_READER_NO_READAHEAD, DEFAULT_STORE_FILE_READER_NO_READAHEAD);
    if (BloomFilterFactory.isGeneralBloomEnabled(conf)) {
      this.cfBloomType = cfBloomType;
    } else {
      LOG.info("Ignoring bloom filter check for file " + this.getPath() + ": " + "cfBloomType=" +
          cfBloomType + " (disabled in config)");
      this.cfBloomType = BloomType.NONE;
    }
    this.primaryReplica = primaryReplica;
  }

  @Override
  public StoreFileInfo getFileInfo() {
    return this.fileInfo;
  }

  @Override
  public Path getPath() {
    return this.fileInfo.getPath();
  }

  @Override
  public Path getQualifiedPath() {
    return this.fileInfo.getPath().makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  @Override
  public boolean isReference() {
    return this.fileInfo.isReference();
  }

  @Override
  public boolean isHFile() {
    return StoreFileInfo.isHFile(this.fileInfo.getPath());
  }

  @Override
  public boolean isMajorCompactionResult() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  @Override
  public boolean excludeFromMinorCompaction() {
    return this.excludeFromMinorCompaction;
  }

  @Override
  public long getMaxSequenceId() {
    return this.sequenceid;
  }

  @Override
  public long getModificationTimeStamp() throws IOException {
    return fileInfo.getModificationTime();
  }

  @Override
  public byte[] getMetadataValue(byte[] key) {
    return metadataMap.get(key);
  }

  @Override
  public boolean isBulkLoadResult() {
    boolean bulkLoadedHFile = false;
    String fileName = this.getPath().getName();
    int startPos = fileName.indexOf("SeqId_");
    if (startPos != -1) {
      bulkLoadedHFile = true;
    }
    return bulkLoadedHFile || (metadataMap != null && metadataMap.containsKey(BULKLOAD_TIME_KEY));
  }

  @Override
  public boolean isCompactedAway() {
    return compactedAway;
  }

  @VisibleForTesting
  public int getRefCount() {
    return refCount.get();
  }

  @Override
  public boolean isReferencedInReads() {
    int rc = refCount.get();
    assert rc >= 0; // we should not go negative.
    return rc > 0;
  }

  @Override
  public OptionalLong getBulkLoadTimestamp() {
    byte[] bulkLoadTimestamp = metadataMap.get(BULKLOAD_TIME_KEY);
    return bulkLoadTimestamp == null ? OptionalLong.empty()
        : OptionalLong.of(Bytes.toLong(bulkLoadTimestamp));
  }

  @Override
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.fileInfo.getHDFSBlockDistribution();
  }

  /**
   * Opens reader on this store file. Called by Constructor.
   * @throws IOException
   * @see #closeReader(boolean)
   */
  private void open() throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }

    // Open the StoreFile.Reader
    this.reader = fileInfo.open(this.fs, this.cacheConf, false, noReadahead ? 0L : -1L,
      primaryReplica, refCount, true);

    // Load up indices and fileinfo. This also loads Bloom filter type.
    metadataMap = Collections.unmodifiableMap(this.reader.loadFileInfo());

    // Read in our metadata.
    byte [] b = metadataMap.get(MAX_SEQ_ID_KEY);
    if (b != null) {
      // By convention, if halfhfile, top half has a sequence number > bottom
      // half. Thats why we add one in below. Its done for case the two halves
      // are ever merged back together --rare.  Without it, on open of store,
      // since store files are distinguished by sequence id, the one half would
      // subsume the other.
      this.sequenceid = Bytes.toLong(b);
      if (fileInfo.isTopReference()) {
        this.sequenceid += 1;
      }
    }

    if (isBulkLoadResult()){
      // generate the sequenceId from the fileName
      // fileName is of the form <randomName>_SeqId_<id-when-loaded>_
      String fileName = this.getPath().getName();
      // Use lastIndexOf() to get the last, most recent bulk load seqId.
      int startPos = fileName.lastIndexOf("SeqId_");
      if (startPos != -1) {
        this.sequenceid = Long.parseLong(fileName.substring(startPos + 6,
            fileName.indexOf('_', startPos + 6)));
        // Handle reference files as done above.
        if (fileInfo.isTopReference()) {
          this.sequenceid += 1;
        }
      }
      // SKIP_RESET_SEQ_ID only works in bulk loaded file.
      // In mob compaction, the hfile where the cells contain the path of a new mob file is bulk
      // loaded to hbase, these cells have the same seqIds with the old ones. We do not want
      // to reset new seqIds for them since this might make a mess of the visibility of cells that
      // have the same row key but different seqIds.
      boolean skipResetSeqId = isSkipResetSeqId(metadataMap.get(SKIP_RESET_SEQ_ID));
      if (skipResetSeqId) {
        // increase the seqId when it is a bulk loaded file from mob compaction.
        this.sequenceid += 1;
      }
      this.reader.setSkipResetSeqId(skipResetSeqId);
      this.reader.setBulkLoaded(true);
    }
    this.reader.setSequenceID(this.sequenceid);

    b = metadataMap.get(HFile.Writer.MAX_MEMSTORE_TS_KEY);
    if (b != null) {
      this.maxMemstoreTS = Bytes.toLong(b);
    }

    b = metadataMap.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    } else {
      // Presume it is not major compacted if it doesn't explicity say so
      // HFileOutputFormat explicitly sets the major compacted key.
      this.majorCompaction = new AtomicBoolean(false);
    }

    b = metadataMap.get(EXCLUDE_FROM_MINOR_COMPACTION_KEY);
    this.excludeFromMinorCompaction = (b != null && Bytes.toBoolean(b));

    BloomType hfileBloomType = reader.getBloomFilterType();
    if (cfBloomType != BloomType.NONE) {
      reader.loadBloomfilter(BlockType.GENERAL_BLOOM_META);
      if (hfileBloomType != cfBloomType) {
        LOG.info("HFile Bloom filter type for "
            + reader.getHFileReader().getName() + ": " + hfileBloomType
            + ", but " + cfBloomType + " specified in column family "
            + "configuration");
      }
    } else if (hfileBloomType != BloomType.NONE) {
      LOG.info("Bloom filter turned off by CF config for "
          + reader.getHFileReader().getName());
    }

    // load delete family bloom filter
    reader.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META);

    try {
      this.reader.timeRange = TimeRangeTracker.getTimeRange(metadataMap.get(TIMERANGE_KEY));
    } catch (IllegalArgumentException e) {
      LOG.error("Error reading timestamp range data from meta -- " +
          "proceeding without", e);
      this.reader.timeRange = null;
    }
    // initialize so we can reuse them after reader closed.
    firstKey = reader.getFirstKey();
    lastKey = reader.getLastKey();
    comparator = reader.getComparator();
  }

  @Override
  public void initReader() throws IOException {
    if (reader == null) {
      try {
        open();
      } catch (Exception e) {
        try {
          boolean evictOnClose = cacheConf != null ? cacheConf.shouldEvictOnClose() : true;
          this.closeReader(evictOnClose);
        } catch (IOException ee) {
          LOG.warn("failed to close reader", ee);
        }
        throw e;
      }
    }
  }

  private StoreFileReader createStreamReader(boolean canUseDropBehind) throws IOException {
    initReader();
    StoreFileReader reader = fileInfo.open(this.fs, this.cacheConf, canUseDropBehind, -1L,
      primaryReplica, refCount, false);
    reader.copyFields(this.reader);
    return reader;
  }

  @Override
  public StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder,
      boolean canOptimizeForNonNullColumn) {
    return getReader().getStoreFileScanner(cacheBlocks, true, false, readPt, scannerOrder,
      canOptimizeForNonNullColumn);
  }

  @Override
  public StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn)
      throws IOException {
    return createStreamReader(canUseDropBehind).getStoreFileScanner(cacheBlocks, false,
      isCompaction, readPt, scannerOrder, canOptimizeForNonNullColumn);
  }

  @Override
  public StoreFileReader getReader() {
    return this.reader;
  }

  @Override
  public synchronized void closeReader(boolean evictOnClose)
      throws IOException {
    if (this.reader != null) {
      this.reader.close(evictOnClose);
      this.reader = null;
    }
  }

  @Override
  public void markCompactedAway() {
    this.compactedAway = true;
  }

  @Override
  public void deleteReader() throws IOException {
    boolean evictOnClose =
        cacheConf != null? cacheConf.shouldEvictOnClose(): true;
    closeReader(evictOnClose);
    this.fs.delete(getPath(), true);
  }

  @Override
  public String toString() {
    return this.fileInfo.toString();
  }

  @Override
  public String toStringDetailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getPath().toString());
    sb.append(", isReference=").append(isReference());
    sb.append(", isBulkLoadResult=").append(isBulkLoadResult());
    if (isBulkLoadResult()) {
      sb.append(", bulkLoadTS=");
      OptionalLong bulkLoadTS = getBulkLoadTimestamp();
      if (bulkLoadTS.isPresent()) {
        sb.append(bulkLoadTS.getAsLong());
      } else {
        sb.append("NotPresent");
      }
    } else {
      sb.append(", seqid=").append(getMaxSequenceId());
    }
    sb.append(", majorCompaction=").append(isMajorCompactionResult());

    return sb.toString();
  }

  /**
   * Gets whether to skip resetting the sequence id for cells.
   * @param skipResetSeqId The byte array of boolean.
   * @return Whether to skip resetting the sequence id.
   */
  private boolean isSkipResetSeqId(byte[] skipResetSeqId) {
    if (skipResetSeqId != null && skipResetSeqId.length == 1) {
      return Bytes.toBoolean(skipResetSeqId);
    }
    return false;
  }

  @Override
  public OptionalLong getMinimumTimestamp() {
    TimeRange tr = getReader().timeRange;
    return tr != null ? OptionalLong.of(tr.getMin()) : OptionalLong.empty();
  }

  @Override
  public OptionalLong getMaximumTimestamp() {
    TimeRange tr = getReader().timeRange;
    return tr != null ? OptionalLong.of(tr.getMax()) : OptionalLong.empty();
  }
}
