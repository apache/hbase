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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

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

  private static final Logger LOG = LoggerFactory.getLogger(HStoreFile.class.getName());

  // Keys for fileinfo values in HFile

  /** Max Sequence ID in FileInfo */
  public static final byte[] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");

  /** Major compaction flag in FileInfo */
  public static final byte[] MAJOR_COMPACTION_KEY = Bytes.toBytes("MAJOR_COMPACTION_KEY");

  /** Minor compaction flag in FileInfo */
  public static final byte[] EXCLUDE_FROM_MINOR_COMPACTION_KEY =
      Bytes.toBytes("EXCLUDE_FROM_MINOR_COMPACTION");

  /**
   * Key for compaction event which contains the compacted storefiles in FileInfo
   */
  public static final byte[] COMPACTION_EVENT_KEY = Bytes.toBytes("COMPACTION_EVENT_KEY");

  /** Bloom filter Type in FileInfo */
  public static final byte[] BLOOM_FILTER_TYPE_KEY = Bytes.toBytes("BLOOM_FILTER_TYPE");

  /** Bloom filter param in FileInfo */
  public static final byte[] BLOOM_FILTER_PARAM_KEY = Bytes.toBytes("BLOOM_FILTER_PARAM");

  /** Delete Family Count in FileInfo */
  public static final byte[] DELETE_FAMILY_COUNT = Bytes.toBytes("DELETE_FAMILY_COUNT");

  /** Last Bloom filter key in FileInfo */
  public static final byte[] LAST_BLOOM_KEY = Bytes.toBytes("LAST_BLOOM_KEY");

  /** Key for Timerange information in metadata */
  public static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  /** Key for timestamp of earliest-put in metadata */
  public static final byte[] EARLIEST_PUT_TS = Bytes.toBytes("EARLIEST_PUT_TS");

  /** Key for the number of mob cells in metadata */
  public static final byte[] MOB_CELLS_COUNT = Bytes.toBytes("MOB_CELLS_COUNT");

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY = Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY = Bytes.toBytes("BULKLOAD_TIMESTAMP");

  /**
   * Key for skipping resetting sequence id in metadata. For bulk loaded hfiles, the scanner resets
   * the cell seqId with the latest one, if this metadata is set as true, the reset is skipped.
   */
  public static final byte[] SKIP_RESET_SEQ_ID = Bytes.toBytes("SKIP_RESET_SEQ_ID");

  private final StoreFileInfo fileInfo;

  // StoreFile.Reader
  private volatile StoreFileReader initialReader;

  // Block cache configuration and reference.
  private final CacheConfig cacheConf;

  // Indicates if the file got compacted
  private volatile boolean compactedAway = false;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // max of the MemstoreTS in the KV's in this store
  // Set when we obtain a Reader.
  private long maxMemstoreTS = -1;

  // firstKey, lastkey and cellComparator will be set when openReader.
  private Optional<Cell> firstKey;

  private Optional<Cell> lastKey;

  private CellComparator comparator;

  public CacheConfig getCacheConf() {
    return this.cacheConf;
  }

  @Override
  public Optional<Cell> getFirstKey() {
    return firstKey;
  }

  @Override
  public Optional<Cell> getLastKey() {
    return lastKey;
  }

  @Override
  public CellComparator getComparator() {
    return comparator;
  }

  @Override
  public long getMaxMemStoreTS() {
    return maxMemstoreTS;
  }

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  // If true, this file should not be included in minor compactions.
  // It's set whenever you get a Reader.
  private boolean excludeFromMinorCompaction = false;

  // This file was product of these compacted store files
  private final Set<String> compactedStoreFiles = new HashSet<>();

  /**
   * Map of the metadata entries in the corresponding HFile. Populated when Reader is opened
   * after which it is not modified again.
   */
  private Map<byte[], byte[]> metadataMap;

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
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   * @throws IOException
   */
  public HStoreFile(FileSystem fs, Path p, Configuration conf, CacheConfig cacheConf,
      BloomType cfBloomType, boolean primaryReplica) throws IOException {
    this(new StoreFileInfo(conf, fs, p, primaryReplica), cfBloomType, cacheConf);
  }

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fileInfo The store file information.
   * @param cfBloomType The bloom type to use for this store file as specified by column
   *          family configuration. This may or may not be the same as the Bloom filter type
   *          actually present in the HFile, because column family configuration might change. If
   *          this is {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @param cacheConf The cache configuration and block cache reference.
   */
  public HStoreFile(StoreFileInfo fileInfo, BloomType cfBloomType, CacheConfig cacheConf) {
    this.fileInfo = fileInfo;
    this.cacheConf = cacheConf;
    if (BloomFilterFactory.isGeneralBloomEnabled(fileInfo.getConf())) {
      this.cfBloomType = cfBloomType;
    } else {
      LOG.info("Ignoring bloom filter check for file " + this.getPath() + ": " + "cfBloomType=" +
          cfBloomType + " (disabled in config)");
      this.cfBloomType = BloomType.NONE;
    }
  }

  /**
   * @return the StoreFile object associated to this StoreFile. null if the StoreFile is not a
   *         reference.
   */
  public StoreFileInfo getFileInfo() {
    return this.fileInfo;
  }

  @Override
  public Path getPath() {
    return this.fileInfo.getPath();
  }

  @Override
  public Path getEncodedPath() {
    try {
      return new Path(URLEncoder.encode(fileInfo.getPath().toString(), HConstants.UTF8_ENCODING));
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("URLEncoder doesn't support UTF-8", ex);
    }
  }

  @Override
  public Path getQualifiedPath() {
    FileSystem fs = fileInfo.getFileSystem();
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
    return getModificationTimestamp();
  }

  @Override
  public long getModificationTimestamp() throws IOException {
    return fileInfo.getModificationTime();
  }

  /**
   * Only used by the Striped Compaction Policy
   * @param key
   * @return value associated with the metadata key
   */
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

  public boolean isCompactedAway() {
    return compactedAway;
  }

  public int getRefCount() {
    return fileInfo.refCount.get();
  }

  /**
   * @return true if the file is still used in reads
   */
  public boolean isReferencedInReads() {
    int rc = fileInfo.refCount.get();
    assert rc >= 0; // we should not go negative.
    return rc > 0;
  }

  @Override
  public OptionalLong getBulkLoadTimestamp() {
    byte[] bulkLoadTimestamp = metadataMap.get(BULKLOAD_TIME_KEY);
    return bulkLoadTimestamp == null ? OptionalLong.empty()
        : OptionalLong.of(Bytes.toLong(bulkLoadTimestamp));
  }

  /**
   * @return the cached value of HDFS blocks distribution. The cached value is calculated when store
   *         file is opened.
   */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.fileInfo.getHDFSBlockDistribution();
  }

  /**
   * Opens reader on this store file. Called by Constructor.
   * @see #closeStoreFile(boolean)
   */
  private void open() throws IOException {
    fileInfo.initHDFSBlocksDistribution();
    long readahead = fileInfo.isNoReadahead() ? 0L : -1L;
    ReaderContext context = fileInfo.createReaderContext(false, readahead, ReaderType.PREAD);
    fileInfo.initHFileInfo(context);
    StoreFileReader reader = fileInfo.preStoreFileReaderOpen(context, cacheConf);
    if (reader == null) {
      reader = fileInfo.createReader(context, cacheConf);
      fileInfo.getHFileInfo().initMetaAndIndex(reader.getHFileReader());
    }
    this.initialReader = fileInfo.postStoreFileReaderOpen(context, cacheConf, reader);
    // Load up indices and fileinfo. This also loads Bloom filter type.
    metadataMap = Collections.unmodifiableMap(initialReader.loadFileInfo());

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
      initialReader.setSkipResetSeqId(skipResetSeqId);
      initialReader.setBulkLoaded(true);
    }
    initialReader.setSequenceID(this.sequenceid);

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

    BloomType hfileBloomType = initialReader.getBloomFilterType();
    if (cfBloomType != BloomType.NONE) {
      initialReader.loadBloomfilter(BlockType.GENERAL_BLOOM_META);
      if (hfileBloomType != cfBloomType) {
        LOG.debug("HFile Bloom filter type for "
            + initialReader.getHFileReader().getName() + ": " + hfileBloomType
            + ", but " + cfBloomType + " specified in column family "
            + "configuration");
      }
    } else if (hfileBloomType != BloomType.NONE) {
      LOG.info("Bloom filter turned off by CF config for "
          + initialReader.getHFileReader().getName());
    }

    // load delete family bloom filter
    initialReader.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META);

    try {
      byte[] data = metadataMap.get(TIMERANGE_KEY);
      initialReader.timeRange = data == null ? null :
          TimeRangeTracker.parseFrom(data).toTimeRange();
    } catch (IllegalArgumentException e) {
      LOG.error("Error reading timestamp range data from meta -- " +
          "proceeding without", e);
      this.initialReader.timeRange = null;
    }

    try {
      byte[] data = metadataMap.get(COMPACTION_EVENT_KEY);
      this.compactedStoreFiles.addAll(ProtobufUtil.toCompactedStoreFiles(data));
    } catch (IOException e) {
      LOG.error("Error reading compacted storefiles from meta data", e);
    }

    // initialize so we can reuse them after reader closed.
    firstKey = initialReader.getFirstKey();
    lastKey = initialReader.getLastKey();
    comparator = initialReader.getComparator();
  }

  /**
   * Initialize the reader used for pread.
   */
  public void initReader() throws IOException {
    if (initialReader == null) {
      synchronized (this) {
        if (initialReader == null) {
          try {
            open();
          } catch (Exception e) {
            try {
              boolean evictOnClose = cacheConf != null ? cacheConf.shouldEvictOnClose() : true;
              this.closeStoreFile(evictOnClose);
            } catch (IOException ee) {
              LOG.warn("failed to close reader", ee);
            }
            throw e;
          }
        }
      }
    }
  }

  private StoreFileReader createStreamReader(boolean canUseDropBehind) throws IOException {
    initReader();
    final boolean doDropBehind = canUseDropBehind && cacheConf.shouldDropBehindCompaction();
    ReaderContext context = fileInfo.createReaderContext(doDropBehind, -1, ReaderType.STREAM);
    StoreFileReader reader = fileInfo.preStoreFileReaderOpen(context, cacheConf);
    if (reader == null) {
      reader = fileInfo.createReader(context, cacheConf);
      // steam reader need copy stuffs from pread reader
      reader.copyFields(initialReader);
    }
    return fileInfo.postStoreFileReaderOpen(context, cacheConf, reader);
  }

  /**
   * Get a scanner which uses pread.
   * <p>
   * Must be called after initReader.
   */
  public StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder,
      boolean canOptimizeForNonNullColumn) {
    return getReader().getStoreFileScanner(cacheBlocks, true, false, readPt, scannerOrder,
      canOptimizeForNonNullColumn);
  }

  /**
   * Get a scanner which uses streaming read.
   * <p>
   * Must be called after initReader.
   */
  public StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn)
      throws IOException {
    return createStreamReader(canUseDropBehind)
        .getStoreFileScanner(cacheBlocks, false, isCompaction, readPt, scannerOrder,
            canOptimizeForNonNullColumn);
  }

  /**
   * @return Current reader. Must call initReader first else returns null.
   * @see #initReader()
   */
  public StoreFileReader getReader() {
    return this.initialReader;
  }

  /**
   * @param evictOnClose whether to evict blocks belonging to this file
   * @throws IOException
   */
  public synchronized void closeStoreFile(boolean evictOnClose) throws IOException {
    if (this.initialReader != null) {
      this.initialReader.close(evictOnClose);
      this.initialReader = null;
    }
  }

  /**
   * Delete this file
   * @throws IOException
   */
  public void deleteStoreFile() throws IOException {
    boolean evictOnClose = cacheConf != null ? cacheConf.shouldEvictOnClose() : true;
    closeStoreFile(evictOnClose);
    this.fileInfo.getFileSystem().delete(getPath(), true);
  }

  public void markCompactedAway() {
    this.compactedAway = true;
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

  Set<String> getCompactedStoreFiles() {
    return Collections.unmodifiableSet(this.compactedStoreFiles);
  }
}
