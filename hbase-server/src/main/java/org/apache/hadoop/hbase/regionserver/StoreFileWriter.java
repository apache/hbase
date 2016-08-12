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
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.util.BloomContext;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RowBloomContext;
import org.apache.hadoop.hbase.util.RowColBloomContext;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * A StoreFile writer.  Use this to read/write HBase Store Files. It is package
 * local because it is an implementation detail of the HBase regionserver.
 */
@InterfaceAudience.Private
public class StoreFileWriter implements Compactor.CellSink {
  private static final Log LOG = LogFactory.getLog(StoreFileWriter.class.getName());

  private final BloomFilterWriter generalBloomFilterWriter;
  private final BloomFilterWriter deleteFamilyBloomFilterWriter;
  private final BloomType bloomType;
  private long earliestPutTs = HConstants.LATEST_TIMESTAMP;
  private long deleteFamilyCnt = 0;
  private BloomContext bloomContext = null;
  private BloomContext deleteFamilyBloomContext = null;

  /**
   * timeRangeTrackerSet is used to figure if we were passed a filled-out TimeRangeTracker or not.
   * When flushing a memstore, we set the TimeRangeTracker that it accumulated during updates to
   * memstore in here into this Writer and use this variable to indicate that we do not need to
   * recalculate the timeRangeTracker bounds; it was done already as part of add-to-memstore.
   * A completed TimeRangeTracker is not set in cases of compactions when it is recalculated.
   */
   private final boolean timeRangeTrackerSet;
   final TimeRangeTracker timeRangeTracker;

  protected HFile.Writer writer;

  /**
   * Creates an HFile.Writer that also write helpful meta data.
   * @param fs file system to write to
   * @param path file name to create
   * @param conf user configuration
   * @param comparator key comparator
   * @param bloomType bloom filter setting
   * @param maxKeys the expected maximum number of keys to be added. Was used
   *        for Bloom filter size in {@link HFile} format version 1.
   * @param fileContext - The HFile context
   * @throws IOException problem writing to FS
   */
  StoreFileWriter(FileSystem fs, Path path, final Configuration conf, CacheConfig cacheConf,
      final CellComparator comparator, BloomType bloomType, long maxKeys,
      InetSocketAddress[] favoredNodes, HFileContext fileContext)
          throws IOException {
      this(fs, path, conf, cacheConf, comparator, bloomType, maxKeys, favoredNodes, fileContext,
          null);
    }

    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs file system to write to
     * @param path file name to create
     * @param conf user configuration
     * @param comparator key comparator
     * @param bloomType bloom filter setting
     * @param maxKeys the expected maximum number of keys to be added. Was used
     *        for Bloom filter size in {@link HFile} format version 1.
     * @param favoredNodes
     * @param fileContext - The HFile context
   * @param trt Ready-made timetracker to use.
     * @throws IOException problem writing to FS
     */
    private StoreFileWriter(FileSystem fs, Path path,
        final Configuration conf,
        CacheConfig cacheConf,
        final CellComparator comparator, BloomType bloomType, long maxKeys,
        InetSocketAddress[] favoredNodes, HFileContext fileContext,
        final TimeRangeTracker trt)
            throws IOException {
    // If passed a TimeRangeTracker, use it. Set timeRangeTrackerSet so we don't destroy it.
    // TODO: put the state of the TRT on the TRT; i.e. make a read-only version (TimeRange) when
    // it no longer writable.
    this.timeRangeTrackerSet = trt != null;
    this.timeRangeTracker = this.timeRangeTrackerSet? trt: new TimeRangeTracker();
    writer = HFile.getWriterFactory(conf, cacheConf)
        .withPath(fs, path)
        .withComparator(comparator)
        .withFavoredNodes(favoredNodes)
        .withFileContext(fileContext)
        .create();

    generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(
        conf, cacheConf, bloomType,
        (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);

    if (generalBloomFilterWriter != null) {
      this.bloomType = bloomType;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Bloom filter type for " + path + ": " + this.bloomType + ", " +
            generalBloomFilterWriter.getClass().getSimpleName());
      }
      // init bloom context
      switch (bloomType) {
      case ROW:
        bloomContext = new RowBloomContext(generalBloomFilterWriter);
        break;
      case ROWCOL:
        bloomContext = new RowColBloomContext(generalBloomFilterWriter);
        break;
      default:
        throw new IOException(
            "Invalid Bloom filter type: " + bloomType + " (ROW or ROWCOL expected)");
      }
    } else {
      // Not using Bloom filters.
      this.bloomType = BloomType.NONE;
    }

    // initialize delete family Bloom filter when there is NO RowCol Bloom
    // filter
    if (this.bloomType != BloomType.ROWCOL) {
      this.deleteFamilyBloomFilterWriter = BloomFilterFactory
          .createDeleteBloomAtWrite(conf, cacheConf,
              (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);
      deleteFamilyBloomContext = new RowBloomContext(deleteFamilyBloomFilterWriter);
    } else {
      deleteFamilyBloomFilterWriter = null;
    }
    if (deleteFamilyBloomFilterWriter != null && LOG.isTraceEnabled()) {
      LOG.trace("Delete Family Bloom filter type for " + path + ": " +
          deleteFamilyBloomFilterWriter.getClass().getSimpleName());
    }
  }

  /**
   * Writes meta data.
   * Call before {@link #close()} since its written as meta data to this file.
   * @param maxSequenceId Maximum sequence id.
   * @param majorCompaction True if this file is product of a major compaction
   * @throws IOException problem writing to FS
   */
  public void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
  throws IOException {
    writer.appendFileInfo(StoreFile.MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
    writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
        Bytes.toBytes(majorCompaction));
    appendTrackedTimestampsToMetadata();
  }

  /**
   * Writes meta data.
   * Call before {@link #close()} since its written as meta data to this file.
   * @param maxSequenceId Maximum sequence id.
   * @param majorCompaction True if this file is product of a major compaction
   * @param mobCellsCount The number of mob cells.
   * @throws IOException problem writing to FS
   */
  public void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
      final long mobCellsCount) throws IOException {
    writer.appendFileInfo(StoreFile.MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
    writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
    writer.appendFileInfo(StoreFile.MOB_CELLS_COUNT, Bytes.toBytes(mobCellsCount));
    appendTrackedTimestampsToMetadata();
  }

  /**
   * Add TimestampRange and earliest put timestamp to Metadata
   */
  public void appendTrackedTimestampsToMetadata() throws IOException {
    appendFileInfo(StoreFile.TIMERANGE_KEY, WritableUtils.toByteArray(timeRangeTracker));
    appendFileInfo(StoreFile.EARLIEST_PUT_TS, Bytes.toBytes(earliestPutTs));
  }

  /**
   * Record the earlest Put timestamp.
   *
   * If the timeRangeTracker is not set,
   * update TimeRangeTracker to include the timestamp of this key
   */
  public void trackTimestamps(final Cell cell) {
    if (KeyValue.Type.Put.getCode() == cell.getTypeByte()) {
      earliestPutTs = Math.min(earliestPutTs, cell.getTimestamp());
    }
    if (!timeRangeTrackerSet) {
      timeRangeTracker.includeTimestamp(cell);
    }
  }

  private void appendGeneralBloomfilter(final Cell cell) throws IOException {
    if (this.generalBloomFilterWriter != null) {
      /*
       * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.png
       * Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + TimeStamp
       *
       * 2 Types of Filtering:
       *  1. Row = Row
       *  2. RowCol = Row + Qualifier
       */
      bloomContext.writeBloom(cell);
    }
  }

  private void appendDeleteFamilyBloomFilter(final Cell cell)
      throws IOException {
    if (!CellUtil.isDeleteFamily(cell) && !CellUtil.isDeleteFamilyVersion(cell)) {
      return;
    }

    // increase the number of delete family in the store file
    deleteFamilyCnt++;
    if (this.deleteFamilyBloomFilterWriter != null) {
      deleteFamilyBloomContext.writeBloom(cell);
    }
  }

  public void append(final Cell cell) throws IOException {
    appendGeneralBloomfilter(cell);
    appendDeleteFamilyBloomFilter(cell);
    writer.append(cell);
    trackTimestamps(cell);
  }

  public Path getPath() {
    return this.writer.getPath();
  }

  public boolean hasGeneralBloom() {
    return this.generalBloomFilterWriter != null;
  }

  /**
   * For unit testing only.
   *
   * @return the Bloom filter used by this writer.
   */
  BloomFilterWriter getGeneralBloomWriter() {
    return generalBloomFilterWriter;
  }

  private boolean closeBloomFilter(BloomFilterWriter bfw) throws IOException {
    boolean haveBloom = (bfw != null && bfw.getKeyCount() > 0);
    if (haveBloom) {
      bfw.compactBloom();
    }
    return haveBloom;
  }

  private boolean closeGeneralBloomFilter() throws IOException {
    boolean hasGeneralBloom = closeBloomFilter(generalBloomFilterWriter);

    // add the general Bloom filter writer and append file info
    if (hasGeneralBloom) {
      writer.addGeneralBloomFilter(generalBloomFilterWriter);
      writer.appendFileInfo(StoreFile.BLOOM_FILTER_TYPE_KEY,
          Bytes.toBytes(bloomType.toString()));
      bloomContext.addLastBloomKey(writer);
    }
    return hasGeneralBloom;
  }

  private boolean closeDeleteFamilyBloomFilter() throws IOException {
    boolean hasDeleteFamilyBloom = closeBloomFilter(deleteFamilyBloomFilterWriter);

    // add the delete family Bloom filter writer
    if (hasDeleteFamilyBloom) {
      writer.addDeleteFamilyBloomFilter(deleteFamilyBloomFilterWriter);
    }

    // append file info about the number of delete family kvs
    // even if there is no delete family Bloom.
    writer.appendFileInfo(StoreFile.DELETE_FAMILY_COUNT,
        Bytes.toBytes(this.deleteFamilyCnt));

    return hasDeleteFamilyBloom;
  }

  public void close() throws IOException {
    boolean hasGeneralBloom = this.closeGeneralBloomFilter();
    boolean hasDeleteFamilyBloom = this.closeDeleteFamilyBloomFilter();

    writer.close();

    // Log final Bloom filter statistics. This needs to be done after close()
    // because compound Bloom filters might be finalized as part of closing.
    if (LOG.isTraceEnabled()) {
      LOG.trace((hasGeneralBloom ? "" : "NO ") + "General Bloom and " +
        (hasDeleteFamilyBloom ? "" : "NO ") + "DeleteFamily" + " was added to HFile " +
        getPath());
    }

  }

  public void appendFileInfo(byte[] key, byte[] value) throws IOException {
    writer.appendFileInfo(key, value);
  }

  /** For use in testing.
   */
  HFile.Writer getHFileWriter() {
    return writer;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification="Will not overflow")
  public static class Builder {
    private final Configuration conf;
    private final CacheConfig cacheConf;
    private final FileSystem fs;

    private CellComparator comparator = CellComparator.COMPARATOR;
    private BloomType bloomType = BloomType.NONE;
    private long maxKeyCount = 0;
    private Path dir;
    private Path filePath;
    private InetSocketAddress[] favoredNodes;
    private HFileContext fileContext;
    private TimeRangeTracker trt;

    public Builder(Configuration conf, CacheConfig cacheConf,
        FileSystem fs) {
      this.conf = conf;
      this.cacheConf = cacheConf;
      this.fs = fs;
    }

    /**
     * @param trt A premade TimeRangeTracker to use rather than build one per append (building one
     * of these is expensive so good to pass one in if you have one).
     * @return this (for chained invocation)
     */
    public Builder withTimeRangeTracker(final TimeRangeTracker trt) {
      Preconditions.checkNotNull(trt);
      this.trt = trt;
      return this;
    }

    /**
     * Use either this method or {@link #withFilePath}, but not both.
     * @param dir Path to column family directory. The directory is created if
     *          does not exist. The file is given a unique name within this
     *          directory.
     * @return this (for chained invocation)
     */
    public Builder withOutputDir(Path dir) {
      Preconditions.checkNotNull(dir);
      this.dir = dir;
      return this;
    }

    /**
     * Use either this method or {@link #withOutputDir}, but not both.
     * @param filePath the StoreFile path to write
     * @return this (for chained invocation)
     */
    public Builder withFilePath(Path filePath) {
      Preconditions.checkNotNull(filePath);
      this.filePath = filePath;
      return this;
    }

    /**
     * @param favoredNodes an array of favored nodes or possibly null
     * @return this (for chained invocation)
     */
    public Builder withFavoredNodes(InetSocketAddress[] favoredNodes) {
      this.favoredNodes = favoredNodes;
      return this;
    }

    public Builder withComparator(CellComparator comparator) {
      Preconditions.checkNotNull(comparator);
      this.comparator = comparator;
      return this;
    }

    public Builder withBloomType(BloomType bloomType) {
      Preconditions.checkNotNull(bloomType);
      this.bloomType = bloomType;
      return this;
    }

    /**
     * @param maxKeyCount estimated maximum number of keys we expect to add
     * @return this (for chained invocation)
     */
    public Builder withMaxKeyCount(long maxKeyCount) {
      this.maxKeyCount = maxKeyCount;
      return this;
    }

    public Builder withFileContext(HFileContext fileContext) {
      this.fileContext = fileContext;
      return this;
    }

    public Builder withShouldDropCacheBehind(boolean shouldDropCacheBehind/*NOT USED!!*/) {
      // TODO: HAS NO EFFECT!!! FIX!!
      return this;
    }
    /**
     * Create a store file writer. Client is responsible for closing file when
     * done. If metadata, add BEFORE closing using
     * {@link StoreFileWriter#appendMetadata}.
     */
    public StoreFileWriter build() throws IOException {
      if ((dir == null ? 0 : 1) + (filePath == null ? 0 : 1) != 1) {
        throw new IllegalArgumentException("Either specify parent directory " +
            "or file path");
      }

      if (dir == null) {
        dir = filePath.getParent();
      }

      if (!fs.exists(dir)) {
        fs.mkdirs(dir);
      }

      if (filePath == null) {
        filePath = StoreFile.getUniqueFile(fs, dir);
        if (!BloomFilterFactory.isGeneralBloomEnabled(conf)) {
          bloomType = BloomType.NONE;
        }
      }

      if (comparator == null) {
        comparator = CellComparator.COMPARATOR;
      }
      return new StoreFileWriter(fs, filePath,
          conf, cacheConf, comparator, bloomType, maxKeyCount, favoredNodes, fileContext, trt);
    }
  }
}
