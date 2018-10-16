/**
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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_PARAM_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_TYPE_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.DELETE_FAMILY_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.EARLIEST_PUT_TS;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAJOR_COMPACTION_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAX_SEQ_ID_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MOB_CELLS_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.TIMERANGE_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.BloomContext;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RowBloomContext;
import org.apache.hadoop.hbase.util.RowColBloomContext;
import org.apache.hadoop.hbase.util.RowPrefixDelimiterBloomContext;
import org.apache.hadoop.hbase.util.RowPrefixFixedLengthBloomContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A StoreFile writer.  Use this to read/write HBase Store Files. It is package
 * local because it is an implementation detail of the HBase regionserver.
 */
@InterfaceAudience.Private
public class StoreFileWriter implements CellSink, ShipperListener {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileWriter.class.getName());
  private static final Pattern dash = Pattern.compile("-");
  private final BloomFilterWriter generalBloomFilterWriter;
  private final BloomFilterWriter deleteFamilyBloomFilterWriter;
  private final BloomType bloomType;
  private byte[] bloomParam = null;
  private long earliestPutTs = HConstants.LATEST_TIMESTAMP;
  private long deleteFamilyCnt = 0;
  private BloomContext bloomContext = null;
  private BloomContext deleteFamilyBloomContext = null;
  private final TimeRangeTracker timeRangeTracker;

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
     * @param favoredNodes
     * @param fileContext - The HFile context
     * @param shouldDropCacheBehind Drop pages written to page cache after writing the store file.
     * @throws IOException problem writing to FS
     */
    private StoreFileWriter(FileSystem fs, Path path,
        final Configuration conf,
        CacheConfig cacheConf,
        final CellComparator comparator, BloomType bloomType, long maxKeys,
        InetSocketAddress[] favoredNodes, HFileContext fileContext,
        boolean shouldDropCacheBehind)
            throws IOException {
    this.timeRangeTracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
    // TODO : Change all writers to be specifically created for compaction context
    writer = HFile.getWriterFactory(conf, cacheConf)
        .withPath(fs, path)
        .withComparator(comparator)
        .withFavoredNodes(favoredNodes)
        .withFileContext(fileContext)
        .withShouldDropCacheBehind(shouldDropCacheBehind)
        .create();

    generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(
        conf, cacheConf, bloomType,
        (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);

    if (generalBloomFilterWriter != null) {
      this.bloomType = bloomType;
      this.bloomParam = BloomFilterUtil.getBloomFilterParam(bloomType, conf);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Bloom filter type for " + path + ": " + this.bloomType + ", param: "
            + (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH?
               Bytes.toInt(bloomParam):Bytes.toStringBinary(bloomParam))
            + ", " + generalBloomFilterWriter.getClass().getSimpleName());
      }
      // init bloom context
      switch (bloomType) {
        case ROW:
          bloomContext = new RowBloomContext(generalBloomFilterWriter, comparator);
          break;
        case ROWCOL:
          bloomContext = new RowColBloomContext(generalBloomFilterWriter, comparator);
          break;
        case ROWPREFIX_FIXED_LENGTH:
          bloomContext = new RowPrefixFixedLengthBloomContext(generalBloomFilterWriter, comparator,
              Bytes.toInt(bloomParam));
          break;
        case ROWPREFIX_DELIMITED:
          bloomContext = new RowPrefixDelimiterBloomContext(generalBloomFilterWriter, comparator,
              bloomParam);
          break;
        default:
          throw new IOException("Invalid Bloom filter type: "
              + bloomType + " (ROW or ROWCOL or ROWPREFIX or ROWPREFIX_DELIMITED expected)");
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
      deleteFamilyBloomContext = new RowBloomContext(deleteFamilyBloomFilterWriter, comparator);
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
    writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
    writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
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
    writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
    writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
    writer.appendFileInfo(MOB_CELLS_COUNT, Bytes.toBytes(mobCellsCount));
    appendTrackedTimestampsToMetadata();
  }

  /**
   * Add TimestampRange and earliest put timestamp to Metadata
   */
  public void appendTrackedTimestampsToMetadata() throws IOException {
    // TODO: The StoreFileReader always converts the byte[] to TimeRange
    // via TimeRangeTracker, so we should write the serialization data of TimeRange directly.
    appendFileInfo(TIMERANGE_KEY, TimeRangeTracker.toByteArray(timeRangeTracker));
    appendFileInfo(EARLIEST_PUT_TS, Bytes.toBytes(earliestPutTs));
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
    timeRangeTracker.includeTimestamp(cell);
  }

  private void appendGeneralBloomfilter(final Cell cell) throws IOException {
    if (this.generalBloomFilterWriter != null) {
      /*
       * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.png
       * Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + Timestamp
       *
       * 4 Types of Filtering:
       *  1. Row = Row
       *  2. RowCol = Row + Qualifier
       *  3. RowPrefixFixedLength  = Fixed Length Row Prefix
       *  4. RowPrefixDelimiter = Delimited Row Prefix
       */
      bloomContext.writeBloom(cell);
    }
  }

  private void appendDeleteFamilyBloomFilter(final Cell cell)
      throws IOException {
    if (!PrivateCellUtil.isDeleteFamily(cell) && !PrivateCellUtil.isDeleteFamilyVersion(cell)) {
      return;
    }

    // increase the number of delete family in the store file
    deleteFamilyCnt++;
    if (this.deleteFamilyBloomFilterWriter != null) {
      deleteFamilyBloomContext.writeBloom(cell);
    }
  }

  @Override
  public void append(final Cell cell) throws IOException {
    appendGeneralBloomfilter(cell);
    appendDeleteFamilyBloomFilter(cell);
    writer.append(cell);
    trackTimestamps(cell);
  }

  @Override
  public void beforeShipped() throws IOException {
    // For now these writer will always be of type ShipperListener true.
    // TODO : Change all writers to be specifically created for compaction context
    writer.beforeShipped();
    if (generalBloomFilterWriter != null) {
      generalBloomFilterWriter.beforeShipped();
    }
    if (deleteFamilyBloomFilterWriter != null) {
      deleteFamilyBloomFilterWriter.beforeShipped();
    }
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
      writer.appendFileInfo(BLOOM_FILTER_TYPE_KEY, Bytes.toBytes(bloomType.toString()));
      if (bloomParam != null) {
        writer.appendFileInfo(BLOOM_FILTER_PARAM_KEY, bloomParam);
      }
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
    writer.appendFileInfo(DELETE_FAMILY_COUNT, Bytes.toBytes(this.deleteFamilyCnt));

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

  /**
   * @param fs
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  static Path getUniqueFile(final FileSystem fs, final Path dir) throws IOException {
    if (!fs.getFileStatus(dir).isDirectory()) {
      throw new IOException("Expecting " + dir.toString() + " to be a directory");
    }
    return new Path(dir, dash.matcher(UUID.randomUUID().toString()).replaceAll(""));
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification="Will not overflow")
  public static class Builder {
    private final Configuration conf;
    private final CacheConfig cacheConf;
    private final FileSystem fs;

    private CellComparator comparator = CellComparator.getInstance();
    private BloomType bloomType = BloomType.NONE;
    private long maxKeyCount = 0;
    private Path dir;
    private Path filePath;
    private InetSocketAddress[] favoredNodes;
    private HFileContext fileContext;
    private boolean shouldDropCacheBehind;

    public Builder(Configuration conf, CacheConfig cacheConf,
        FileSystem fs) {
      this.conf = conf;
      this.cacheConf = cacheConf;
      this.fs = fs;
    }

    /**
     * Creates Builder with cache configuration disabled
     */
    public Builder(Configuration conf, FileSystem fs) {
      this.conf = conf;
      this.cacheConf = CacheConfig.DISABLED;
      this.fs = fs;
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

    public Builder withShouldDropCacheBehind(boolean shouldDropCacheBehind) {
      this.shouldDropCacheBehind = shouldDropCacheBehind;
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
        // Handle permission for non-HDFS filesystem properly
        // See HBASE-17710
        HRegionFileSystem.mkdirs(fs, conf, dir);
      }

      // set block storage policy for temp path
      String policyName = this.conf.get(ColumnFamilyDescriptorBuilder.STORAGE_POLICY);
      if (null == policyName) {
        policyName = this.conf.get(HStore.BLOCK_STORAGE_POLICY_KEY);
      }
      FSUtils.setStoragePolicy(this.fs, dir, policyName);

      if (filePath == null) {
        filePath = getUniqueFile(fs, dir);
        if (!BloomFilterFactory.isGeneralBloomEnabled(conf)) {
          bloomType = BloomType.NONE;
        }
      }

      if (comparator == null) {
        comparator = CellComparator.getInstance();
      }
      return new StoreFileWriter(fs, filePath,
          conf, cacheConf, comparator, bloomType, maxKeyCount, favoredNodes, fileContext,
          shouldDropCacheBehind);
    }
  }
}
