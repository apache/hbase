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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, instantiate a writer using {@link StoreFileWriter.Builder}
 * and append data. Be sure to add any metadata before calling close on the
 * Writer (Use the appendMetadata convenience methods). On close, a StoreFile
 * is sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #createReader()}.
 * <p>StoreFiles may also reference store files in another Store.
 *
 * The reason for this weird pattern where you use a different instance for the
 * writer and a reader is that we write once but read a lot more.
 */
@InterfaceAudience.LimitedPrivate("Coprocessor")
public class StoreFile {
  private static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  // Keys for fileinfo values in HFile

  /** Max Sequence ID in FileInfo */
  public static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");

  /** Major compaction flag in FileInfo */
  public static final byte[] MAJOR_COMPACTION_KEY =
      Bytes.toBytes("MAJOR_COMPACTION_KEY");

  /** Minor compaction flag in FileInfo */
  public static final byte[] EXCLUDE_FROM_MINOR_COMPACTION_KEY =
      Bytes.toBytes("EXCLUDE_FROM_MINOR_COMPACTION");

  /** Bloom filter Type in FileInfo */
  public static final byte[] BLOOM_FILTER_TYPE_KEY =
      Bytes.toBytes("BLOOM_FILTER_TYPE");

  /** Delete Family Count in FileInfo */
  public static final byte[] DELETE_FAMILY_COUNT =
      Bytes.toBytes("DELETE_FAMILY_COUNT");

  /** Last Bloom filter key in FileInfo */
  public static final byte[] LAST_BLOOM_KEY = Bytes.toBytes("LAST_BLOOM_KEY");

  /** Key for Timerange information in metadata*/
  public static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  /** Key for timestamp of earliest-put in metadata*/
  public static final byte[] EARLIEST_PUT_TS = Bytes.toBytes("EARLIEST_PUT_TS");

  /** Key for the number of mob cells in metadata*/
  public static final byte[] MOB_CELLS_COUNT = Bytes.toBytes("MOB_CELLS_COUNT");

  private final StoreFileInfo fileInfo;
  private final FileSystem fs;

  // Block cache configuration and reference.
  private final CacheConfig cacheConf;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // max of the MemstoreTS in the KV's in this store
  // Set when we obtain a Reader.
  private long maxMemstoreTS = -1;

  // firstKey, lastkey and cellComparator will be set when openReader.
  private Cell firstKey;

  private Cell lastKey;

  private Comparator comparator;

  CacheConfig getCacheConf() {
    return cacheConf;
  }

  public Cell getFirstKey() {
    return firstKey;
  }

  public Cell getLastKey() {
    return lastKey;
  }

  public Comparator getComparator() {
    return comparator;
  }

  public long getMaxMemstoreTS() {
    return maxMemstoreTS;
  }

  public void setMaxMemstoreTS(long maxMemstoreTS) {
    this.maxMemstoreTS = maxMemstoreTS;
  }

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  // If true, this file should not be included in minor compactions.
  // It's set whenever you get a Reader.
  private boolean excludeFromMinorCompaction = false;

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY =
    Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY =
    Bytes.toBytes("BULKLOAD_TIMESTAMP");

  /**
   * Map of the metadata entries in the corresponding HFile
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
   * Key for skipping resetting sequence id in metadata.
   * For bulk loaded hfiles, the scanner resets the cell seqId with the latest one,
   * if this metadata is set as true, the reset is skipped.
   */
  public static final byte[] SKIP_RESET_SEQ_ID = Bytes.toBytes("SKIP_RESET_SEQ_ID");

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a
   * substantial amount of ram depending on the underlying files (10-20MB?).
   *
   * @param fs  The current file system to use.
   * @param p  The path of the file.
   * @param conf  The current configuration.
   * @param cacheConf  The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified
   *          by column family configuration. This may or may not be the same
   *          as the Bloom filter type actually present in the HFile, because
   *          column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @throws IOException When opening the reader fails.
   */
  public StoreFile(final FileSystem fs, final Path p, final Configuration conf,
        final CacheConfig cacheConf, final BloomType cfBloomType) throws IOException {
    this(fs, new StoreFileInfo(conf, fs, p), conf, cacheConf, cfBloomType);
  }


  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a
   * substantial amount of ram depending on the underlying files (10-20MB?).
   *
   * @param fs  The current file system to use.
   * @param fileInfo  The store file information.
   * @param conf  The current configuration.
   * @param cacheConf  The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified
   *          by column family configuration. This may or may not be the same
   *          as the Bloom filter type actually present in the HFile, because
   *          column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @throws IOException When opening the reader fails.
   */
  public StoreFile(final FileSystem fs, final StoreFileInfo fileInfo, final Configuration conf,
      final CacheConfig cacheConf,  final BloomType cfBloomType) throws IOException {
    this.fs = fs;
    this.fileInfo = fileInfo;
    this.cacheConf = cacheConf;

    if (BloomFilterFactory.isGeneralBloomEnabled(conf)) {
      this.cfBloomType = cfBloomType;
    } else {
      LOG.info("Ignoring bloom filter check for file " + this.getPath() + ": " +
          "cfBloomType=" + cfBloomType + " (disabled in config)");
      this.cfBloomType = BloomType.NONE;
    }
  }

  /**
   * Clone
   * @param other The StoreFile to clone from
   */
  public StoreFile(final StoreFile other) {
    this.fs = other.fs;
    this.fileInfo = other.fileInfo;
    this.cacheConf = other.cacheConf;
    this.cfBloomType = other.cfBloomType;
  }

  /**
   * Clone a StoreFile for opening private reader.
   */
  public StoreFile cloneForReader() {
    return new StoreFile(this);
  }

  /**
   * @return the StoreFile object associated to this StoreFile.
   *         null if the StoreFile is not a reference.
   */
  public StoreFileInfo getFileInfo() {
    return this.fileInfo;
  }

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  public Path getPath() {
    return this.fileInfo.getPath();
  }

  /**
   * @return Returns the qualified path of this StoreFile
   */
  public Path getQualifiedPath() {
    return this.fileInfo.getPath().makeQualified(fs);
  }

  /**
   * @return True if this is a StoreFile Reference; call
   * after {@link #open(boolean canUseDropBehind)} else may get wrong answer.
   */
  public boolean isReference() {
    return this.fileInfo.isReference();
  }

  /**
   * @return True if this is HFile.
   */
  public boolean isHFile() {
    return StoreFileInfo.isHFile(this.fileInfo.getPath());
  }

  /**
   * @return True if this file was made by a major compaction.
   */
  public boolean isMajorCompaction() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  /**
   * @return True if this file should not be part of a minor compaction.
   */
  public boolean excludeFromMinorCompaction() {
    return this.excludeFromMinorCompaction;
  }

  /**
   * @return This files maximum edit sequence id.
   */
  public long getMaxSequenceId() {
    return this.sequenceid;
  }

  public long getModificationTimeStamp() throws IOException {
    return (fileInfo == null) ? 0 : fileInfo.getModificationTime();
  }

  /**
   * Only used by the Striped Compaction Policy
   * @param key
   * @return value associated with the metadata key
   */
  public byte[] getMetadataValue(byte[] key) {
    return metadataMap.get(key);
  }

  /**
   * Return the largest memstoreTS found across all storefiles in
   * the given list. Store files that were created by a mapreduce
   * bulk load are ignored, as they do not correspond to any specific
   * put operation, and thus do not have a memstoreTS associated with them.
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxMemstoreTSInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      if (!sf.isBulkLoadResult()) {
        max = Math.max(max, sf.getMaxMemstoreTS());
      }
    }
    return max;
  }

  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list.
   * @param sfs
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxSequenceIdInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      max = Math.max(max, sf.getMaxSequenceId());
    }
    return max;
  }

  /**
   * Check if this storefile was created by bulk load.
   * When a hfile is bulk loaded into HBase, we append
   * {@code '_SeqId_<id-when-loaded>'} to the hfile name, unless
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is
   * explicitly turned off.
   * If "hbase.mapreduce.bulkload.assign.sequenceNumbers"
   * is turned off, fall back to BULKLOAD_TIME_KEY.
   * @return true if this storefile was created by bulk load.
   */
  public boolean isBulkLoadResult() {
    boolean bulkLoadedHFile = false;
    String fileName = this.getPath().getName();
    int startPos = fileName.indexOf("SeqId_");
    if (startPos != -1) {
      bulkLoadedHFile = true;
    }
    return bulkLoadedHFile || metadataMap.containsKey(BULKLOAD_TIME_KEY);
  }

  @VisibleForTesting
  public boolean isCompactedAway() {
    if (this.reader != null) {
      return this.reader.isCompactedAway();
    }
    return true;
  }

  @VisibleForTesting
  public int getRefCount() {
    return this.reader.getRefCount().get();
  }

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  public long getBulkLoadTimestamp() {
    byte[] bulkLoadTimestamp = metadataMap.get(BULKLOAD_TIME_KEY);
    return (bulkLoadTimestamp == null) ? 0 : Bytes.toLong(bulkLoadTimestamp);
  }

  /**
   * @return the cached value of HDFS blocks distribution. The cached value is
   * calculated when store file is opened.
   */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.fileInfo.getHDFSBlockDistribution();
  }

  /**
   * Opens reader on this store file.  Called by Constructor.
   * @return Reader for the store file.
   * @throws IOException
   * @see #closeReader(boolean)
   */
  private StoreFileReader open(boolean canUseDropBehind) throws IOException {
    if (this.reader != null) {
      throw new IllegalAccessError("Already open");
    }

    // Open the StoreFile.Reader
    this.reader = fileInfo.open(this.fs, this.cacheConf, canUseDropBehind);

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
      this.reader.setSkipResetSeqId(isSkipResetSeqId(metadataMap.get(SKIP_RESET_SEQ_ID)));
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
    return this.reader;
  }

  public StoreFileReader createReader() throws IOException {
    return createReader(false);
  }

  /**
   * @return Reader for StoreFile. creates if necessary
   * @throws IOException
   */
  public StoreFileReader createReader(boolean canUseDropBehind) throws IOException {
    if (this.reader == null) {
      try {
        this.reader = open(canUseDropBehind);
      } catch (IOException e) {
        try {
          boolean evictOnClose =
              cacheConf != null? cacheConf.shouldEvictOnClose(): true;
          this.closeReader(evictOnClose);
        } catch (IOException ee) {
        }
        throw e;
      }

    }
    return this.reader;
  }

  /**
   * @return Current reader.  Must call createReader first else returns null.
   * @see #createReader()
   */
  public StoreFileReader getReader() {
    return this.reader;
  }

  /**
   * @param evictOnClose whether to evict blocks belonging to this file
   * @throws IOException
   */
  public synchronized void closeReader(boolean evictOnClose)
      throws IOException {
    if (this.reader != null) {
      this.reader.close(evictOnClose);
      this.reader = null;
    }
  }

  /**
   * Marks the status of the file as compactedAway.
   */
  public void markCompactedAway() {
    if (this.reader != null) {
      this.reader.markCompactedAway();
    }
  }

  /**
   * Delete this file
   * @throws IOException
   */
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

  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  public String toStringDetailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getPath().toString());
    sb.append(", isReference=").append(isReference());
    sb.append(", isBulkLoadResult=").append(isBulkLoadResult());
    if (isBulkLoadResult()) {
      sb.append(", bulkLoadTS=").append(getBulkLoadTimestamp());
    } else {
      sb.append(", seqid=").append(getMaxSequenceId());
    }
    sb.append(", majorCompaction=").append(isMajorCompaction());

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

  /**
   * @param fs
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  public static Path getUniqueFile(final FileSystem fs, final Path dir)
      throws IOException {
    if (!fs.getFileStatus(dir).isDirectory()) {
      throw new IOException("Expecting " + dir.toString() +
        " to be a directory");
    }
    return new Path(dir, UUID.randomUUID().toString().replaceAll("-", ""));
  }

  public Long getMinimumTimestamp() {
    return getReader().timeRange == null? null: getReader().timeRange.getMin();
  }

  public Long getMaximumTimestamp() {
    return getReader().timeRange == null? null: getReader().timeRange.getMax();
  }


  /**
   * Gets the approximate mid-point of this file that is optimal for use in splitting it.
   * @param comparator Comparator used to compare KVs.
   * @return The split point row, or null if splitting is not possible, or reader is null.
   */
  byte[] getFileSplitPoint(CellComparator comparator) throws IOException {
    if (this.reader == null) {
      LOG.warn("Storefile " + this + " Reader is null; cannot get split point");
      return null;
    }
    // Get first, last, and mid keys.  Midkey is the key that starts block
    // in middle of hfile.  Has column and timestamp.  Need to return just
    // the row we want to split on as midkey.
    Cell midkey = this.reader.midkey();
    if (midkey != null) {
      Cell firstKey = this.reader.getFirstKey();
      Cell lastKey = this.reader.getLastKey();
      // if the midkey is the same as the first or last keys, we cannot (ever) split this region.
      if (comparator.compareRows(midkey, firstKey) == 0
          || comparator.compareRows(midkey, lastKey) == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("cannot split because midkey is the same as first or last row");
        }
        return null;
      }
      return CellUtil.cloneRow(midkey);
    }
    return null;
  }

  /**
   * Useful comparators for comparing StoreFiles.
   */
  public abstract static class Comparators {
    /**
     * Comparator that compares based on the Sequence Ids of the
     * the StoreFiles. Bulk loads that did not request a seq ID
     * are given a seq id of -1; thus, they are placed before all non-
     * bulk loads, and bulk loads with sequence Id. Among these files,
     * the size is used to determine the ordering, then bulkLoadTime.
     * If there are ties, the path name is used as a tie-breaker.
     */
    public static final Comparator<StoreFile> SEQ_ID =
      Ordering.compound(ImmutableList.of(
          Ordering.natural().onResultOf(new GetSeqId()),
          Ordering.natural().onResultOf(new GetFileSize()).reverse(),
          Ordering.natural().onResultOf(new GetBulkTime()),
          Ordering.natural().onResultOf(new GetPathName())
      ));

    /**
     * Comparator for time-aware compaction. SeqId is still the first
     *   ordering criterion to maintain MVCC.
     */
    public static final Comparator<StoreFile> SEQ_ID_MAX_TIMESTAMP =
      Ordering.compound(ImmutableList.of(
        Ordering.natural().onResultOf(new GetSeqId()),
        Ordering.natural().onResultOf(new GetMaxTimestamp()),
        Ordering.natural().onResultOf(new GetFileSize()).reverse(),
        Ordering.natural().onResultOf(new GetBulkTime()),
        Ordering.natural().onResultOf(new GetPathName())
      ));

    private static class GetSeqId implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        return sf.getMaxSequenceId();
      }
    }

    private static class GetFileSize implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (sf.getReader() != null) {
          return sf.getReader().length();
        } else {
          // the reader may be null for the compacted files and if the archiving
          // had failed.
          return -1L;
        }
      }
    }

    private static class GetBulkTime implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (!sf.isBulkLoadResult()) return Long.MAX_VALUE;
        return sf.getBulkLoadTimestamp();
      }
    }

    private static class GetPathName implements Function<StoreFile, String> {
      @Override
      public String apply(StoreFile sf) {
        return sf.getPath().getName();
      }
    }

    private static class GetMaxTimestamp implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        return sf.getMaximumTimestamp() == null? (Long)Long.MAX_VALUE : sf.getMaximumTimestamp();
      }
    }
  }
}
