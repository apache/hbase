/*
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

import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_PARAM_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_TYPE_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.COMPACTION_EVENT_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.DELETE_FAMILY_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.HISTORICAL_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAJOR_COMPACTION_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAX_SEQ_ID_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MOB_CELLS_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MOB_FILE_REFS;
import static org.apache.hadoop.hbase.regionserver.StoreEngine.STORE_ENGINE_CLASS_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.util.BloomContext;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RowBloomContext;
import org.apache.hadoop.hbase.util.RowColBloomContext;
import org.apache.hadoop.hbase.util.RowPrefixFixedLengthBloomContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.SetMultimap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * A StoreFile writer. Use this to read/write HBase Store Files. It is package local because it is
 * an implementation detail of the HBase regionserver.
 */
@InterfaceAudience.Private
public class StoreFileWriter implements CellSink, ShipperListener {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileWriter.class.getName());
  public static final String ENABLE_HISTORICAL_COMPACTION_FILES =
    "hbase.enable.historical.compaction.files";
  public static final boolean DEFAULT_ENABLE_HISTORICAL_COMPACTION_FILES = false;
  private static final Pattern dash = Pattern.compile("-");
  private SingleStoreFileWriter liveFileWriter;
  private SingleStoreFileWriter historicalFileWriter;
  private final FileSystem fs;
  private final Path historicalFilePath;
  private final Configuration conf;
  private final CacheConfig cacheConf;
  private final BloomType bloomType;
  private final long maxKeys;
  private final InetSocketAddress[] favoredNodes;
  private final HFileContext fileContext;
  private final boolean shouldDropCacheBehind;
  private final Supplier<Collection<HStoreFile>> compactedFilesSupplier;
  private final CellComparator comparator;
  private ExtendedCell lastCell;
  // The first (latest) delete family marker of the current row
  private ExtendedCell deleteFamily;
  // The list of delete family version markers of the current row
  private List<ExtendedCell> deleteFamilyVersionList = new ArrayList<>();
  // The first (latest) delete column marker of the current column
  private ExtendedCell deleteColumn;
  // The list of delete column version markers of the current column
  private List<ExtendedCell> deleteColumnVersionList = new ArrayList<>();
  // The live put cell count for the current column
  private int livePutCellCount;
  private final int maxVersions;
  private final boolean newVersionBehavior;

  /**
   * Creates an HFile.Writer that also write helpful meta data.
   * @param fs                     file system to write to
   * @param liveFilePath           the name of the live file to create
   * @param historicalFilePath     the name of the historical file name to create
   * @param conf                   user configuration
   * @param bloomType              bloom filter setting
   * @param maxKeys                the expected maximum number of keys to be added. Was used for
   *                               Bloom filter size in {@link HFile} format version 1.
   * @param favoredNodes           an array of favored nodes or possibly null
   * @param fileContext            The HFile context
   * @param shouldDropCacheBehind  Drop pages written to page cache after writing the store file.
   * @param compactedFilesSupplier Returns the {@link HStore} compacted files which not archived
   * @param comparator             Cell comparator
   * @param maxVersions            max cell versions
   * @param newVersionBehavior     enable new version behavior
   * @throws IOException problem writing to FS
   */
  private StoreFileWriter(FileSystem fs, Path liveFilePath, Path historicalFilePath,
    final Configuration conf, CacheConfig cacheConf, BloomType bloomType, long maxKeys,
    InetSocketAddress[] favoredNodes, HFileContext fileContext, boolean shouldDropCacheBehind,
    Supplier<Collection<HStoreFile>> compactedFilesSupplier, CellComparator comparator,
    int maxVersions, boolean newVersionBehavior) throws IOException {
    this.fs = fs;
    this.historicalFilePath = historicalFilePath;
    this.conf = conf;
    this.cacheConf = cacheConf;
    this.bloomType = bloomType;
    this.maxKeys = maxKeys;
    this.favoredNodes = favoredNodes;
    this.fileContext = fileContext;
    this.shouldDropCacheBehind = shouldDropCacheBehind;
    this.compactedFilesSupplier = compactedFilesSupplier;
    this.comparator = comparator;
    this.maxVersions = maxVersions;
    this.newVersionBehavior = newVersionBehavior;
    liveFileWriter = new SingleStoreFileWriter(fs, liveFilePath, conf, cacheConf, bloomType,
      maxKeys, favoredNodes, fileContext, shouldDropCacheBehind, compactedFilesSupplier);
  }

  public static boolean shouldEnableHistoricalCompactionFiles(Configuration conf) {
    if (
      conf.getBoolean(ENABLE_HISTORICAL_COMPACTION_FILES,
        DEFAULT_ENABLE_HISTORICAL_COMPACTION_FILES)
    ) {
      // Historical compaction files are supported only for default store engine with
      // default compactor.
      String storeEngine = conf.get(STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
      if (!storeEngine.equals(DefaultStoreEngine.class.getName())) {
        LOG.warn("Historical compaction file generation is ignored for " + storeEngine
          + ". hbase.enable.historical.compaction.files can be set to true only for the "
          + "default compaction (DefaultStoreEngine and DefaultCompactor)");
        return false;
      }
      String compactor = conf.get(DEFAULT_COMPACTOR_CLASS_KEY, DefaultCompactor.class.getName());
      if (!compactor.equals(DefaultCompactor.class.getName())) {
        LOG.warn("Historical compaction file generation is ignored for " + compactor
          + ". hbase.enable.historical.compaction.files can be set to true only for the "
          + "default compaction (DefaultStoreEngine and DefaultCompactor)");
        return false;
      }
      return true;
    }
    return false;
  }

  public long getPos() throws IOException {
    return liveFileWriter.getPos();
  }

  /**
   * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
   * @param maxSequenceId   Maximum sequence id.
   * @param majorCompaction True if this file is product of a major compaction
   * @throws IOException problem writing to FS
   */
  public void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
    throws IOException {
    liveFileWriter.appendMetadata(maxSequenceId, majorCompaction);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendMetadata(maxSequenceId, majorCompaction);
    }
  }

  /**
   * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
   * @param maxSequenceId   Maximum sequence id.
   * @param majorCompaction True if this file is product of a major compaction
   * @param storeFiles      The compacted store files to generate this new file
   * @throws IOException problem writing to FS
   */
  public void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
    final Collection<HStoreFile> storeFiles) throws IOException {
    liveFileWriter.appendMetadata(maxSequenceId, majorCompaction, storeFiles);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendMetadata(maxSequenceId, majorCompaction, storeFiles);
    }
  }

  /**
   * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
   * @param maxSequenceId   Maximum sequence id.
   * @param majorCompaction True if this file is product of a major compaction
   * @param mobCellsCount   The number of mob cells.
   * @throws IOException problem writing to FS
   */
  public void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
    final long mobCellsCount) throws IOException {
    liveFileWriter.appendMetadata(maxSequenceId, majorCompaction, mobCellsCount);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendMetadata(maxSequenceId, majorCompaction, mobCellsCount);
    }
  }

  /**
   * Appends MOB - specific metadata (even if it is empty)
   * @param mobRefSet - original table -> set of MOB file names
   * @throws IOException problem writing to FS
   */
  public void appendMobMetadata(SetMultimap<TableName, String> mobRefSet) throws IOException {
    liveFileWriter.appendMobMetadata(mobRefSet);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendMobMetadata(mobRefSet);
    }
  }

  /**
   * Add TimestampRange and earliest put timestamp to Metadata
   */
  public void appendTrackedTimestampsToMetadata() throws IOException {
    // TODO: The StoreFileReader always converts the byte[] to TimeRange
    // via TimeRangeTracker, so we should write the serialization data of TimeRange directly.
    liveFileWriter.appendTrackedTimestampsToMetadata();
    if (historicalFileWriter != null) {
      historicalFileWriter.appendTrackedTimestampsToMetadata();
    }
  }

  public void appendCustomCellTimestampsToMetadata(TimeRangeTracker timeRangeTracker)
    throws IOException {
    liveFileWriter.appendCustomCellTimestampsToMetadata(timeRangeTracker);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendCustomCellTimestampsToMetadata(timeRangeTracker);
    }
  }

  @Override
  public void beforeShipped() throws IOException {
    liveFileWriter.beforeShipped();
    if (historicalFileWriter != null) {
      historicalFileWriter.beforeShipped();
    }
  }

  public Path getPath() {
    return liveFileWriter.getPath();
  }

  public List<Path> getPaths() {
    if (historicalFileWriter == null) {
      return Lists.newArrayList(liveFileWriter.getPath());
    }
    return Lists.newArrayList(liveFileWriter.getPath(), historicalFileWriter.getPath());
  }

  public boolean hasGeneralBloom() {
    return liveFileWriter.hasGeneralBloom();
  }

  /**
   * For unit testing only.
   * @return the Bloom filter used by this writer.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public BloomFilterWriter getGeneralBloomWriter() {
    return liveFileWriter.generalBloomFilterWriter;
  }

  public void close() throws IOException {
    liveFileWriter.appendFileInfo(HISTORICAL_KEY, Bytes.toBytes(false));
    liveFileWriter.close();
    if (historicalFileWriter != null) {
      historicalFileWriter.appendFileInfo(HISTORICAL_KEY, Bytes.toBytes(true));
      historicalFileWriter.close();
    }
  }

  public void appendFileInfo(byte[] key, byte[] value) throws IOException {
    liveFileWriter.appendFileInfo(key, value);
    if (historicalFileWriter != null) {
      historicalFileWriter.appendFileInfo(key, value);
    }
  }

  /**
   * For use in testing.
   */
  HFile.Writer getLiveFileWriter() {
    return liveFileWriter.getHFileWriter();
  }

  /**
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  public static Path getUniqueFile(final FileSystem fs, final Path dir) throws IOException {
    if (!fs.getFileStatus(dir).isDirectory()) {
      throw new IOException("Expecting " + dir.toString() + " to be a directory");
    }
    return new Path(dir, dash.matcher(UUID.randomUUID().toString()).replaceAll(""));
  }

  private SingleStoreFileWriter getHistoricalFileWriter() throws IOException {
    if (historicalFileWriter == null) {
      historicalFileWriter =
        new SingleStoreFileWriter(fs, historicalFilePath, conf, cacheConf, bloomType, maxKeys,
          favoredNodes, fileContext, shouldDropCacheBehind, compactedFilesSupplier);
    }
    return historicalFileWriter;
  }

  private void initRowState() {
    deleteFamily = null;
    deleteFamilyVersionList.clear();
    lastCell = null;
  }

  private void initColumnState() {
    livePutCellCount = 0;
    deleteColumn = null;
    deleteColumnVersionList.clear();

  }

  private boolean isDeletedByDeleteFamily(ExtendedCell cell) {
    return deleteFamily != null && (deleteFamily.getTimestamp() > cell.getTimestamp()
      || (deleteFamily.getTimestamp() == cell.getTimestamp()
        && (!newVersionBehavior || cell.getSequenceId() < deleteFamily.getSequenceId())));
  }

  private boolean isDeletedByDeleteFamilyVersion(ExtendedCell cell) {
    for (ExtendedCell deleteFamilyVersion : deleteFamilyVersionList) {
      if (
        deleteFamilyVersion.getTimestamp() == cell.getTimestamp()
          && (!newVersionBehavior || cell.getSequenceId() < deleteFamilyVersion.getSequenceId())
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean isDeletedByDeleteColumn(ExtendedCell cell) {
    return deleteColumn != null && (deleteColumn.getTimestamp() > cell.getTimestamp()
      || (deleteColumn.getTimestamp() == cell.getTimestamp()
        && (!newVersionBehavior || cell.getSequenceId() < deleteColumn.getSequenceId())));
  }

  private boolean isDeletedByDeleteColumnVersion(ExtendedCell cell) {
    for (ExtendedCell deleteColumnVersion : deleteColumnVersionList) {
      if (
        deleteColumnVersion.getTimestamp() == cell.getTimestamp()
          && (!newVersionBehavior || cell.getSequenceId() < deleteColumnVersion.getSequenceId())
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean isDeleted(ExtendedCell cell) {
    return isDeletedByDeleteFamily(cell) || isDeletedByDeleteColumn(cell)
      || isDeletedByDeleteFamilyVersion(cell) || isDeletedByDeleteColumnVersion(cell);
  }

  private void appendCell(ExtendedCell cell) throws IOException {
    if ((lastCell == null || !CellUtil.matchingColumn(lastCell, cell))) {
      initColumnState();
    }
    if (cell.getType() == Cell.Type.DeleteFamily) {
      if (deleteFamily == null) {
        deleteFamily = cell;
        liveFileWriter.append(cell);
      } else {
        getHistoricalFileWriter().append(cell);
      }
    } else if (cell.getType() == Cell.Type.DeleteFamilyVersion) {
      if (!isDeletedByDeleteFamily(cell)) {
        deleteFamilyVersionList.add(cell);
        if (deleteFamily != null && deleteFamily.getTimestamp() == cell.getTimestamp()) {
          // This means both the delete-family and delete-family-version markers have the same
          // timestamp but the sequence id of delete-family-version marker is higher than that of
          // the delete-family marker. In this case, there is no need to add the
          // delete-family-version marker to the live version file. This case happens only with
          // the new version behavior.
          liveFileWriter.append(cell);
        } else {
          liveFileWriter.append(cell);
        }
      } else {
        getHistoricalFileWriter().append(cell);
      }
    } else if (cell.getType() == Cell.Type.DeleteColumn) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumn = cell;
        liveFileWriter.append(cell);
      } else {
        getHistoricalFileWriter().append(cell);
      }
    } else if (cell.getType() == Cell.Type.Delete) {
      if (!isDeletedByDeleteFamily(cell) && deleteColumn == null) {
        deleteColumnVersionList.add(cell);
        if (deleteFamily != null && deleteFamily.getTimestamp() == cell.getTimestamp()) {
          // This means both the delete-family and delete-column-version markers have the same
          // timestamp but the sequence id of delete-column-version marker is higher than that of
          // the delete-family marker. In this case, there is no need to add the
          // delete-column-version marker to the live version file. This case happens only with
          // the new version behavior.
          getHistoricalFileWriter().append(cell);
        } else {
          liveFileWriter.append(cell);
        }
      } else {
        getHistoricalFileWriter().append(cell);
      }
    } else if (cell.getType() == Cell.Type.Put) {
      if (livePutCellCount < maxVersions) {
        // This is a live put cell (i.e., the latest version) of a column. Is it deleted?
        if (!isDeleted(cell)) {
          liveFileWriter.append(cell);
          livePutCellCount++;
        } else {
          // It is deleted
          getHistoricalFileWriter().append(cell);
          if (newVersionBehavior) {
            // Deleted versions are considered toward total version count when newVersionBehavior
            livePutCellCount++;
          }
        }
      } else {
        // It is an older put cell
        getHistoricalFileWriter().append(cell);
      }
    }
    lastCell = cell;
  }

  @Override
  public void appendAll(List<ExtendedCell> cellList) throws IOException {
    if (historicalFilePath == null) {
      // The dual writing is not enabled and all cells are written to one file. We use
      // the live version file in this case
      for (ExtendedCell cell : cellList) {
        liveFileWriter.append(cell);
      }
      return;
    }
    if (cellList.isEmpty()) {
      return;
    }
    if (lastCell != null && comparator.compareRows(lastCell, cellList.get(0)) != 0) {
      // It is a new row and thus time to reset the state
      initRowState();
    }
    for (ExtendedCell cell : cellList) {
      appendCell(cell);
    }
  }

  @Override
  public void append(ExtendedCell cell) throws IOException {
    if (historicalFilePath == null) {
      // The dual writing is not enabled and all cells are written to one file. We use
      // the live version file in this case
      liveFileWriter.append(cell);
      return;
    }
    appendCell(cell);
  }

  private final static class SingleStoreFileWriter {
    private final BloomFilterWriter generalBloomFilterWriter;
    private final BloomFilterWriter deleteFamilyBloomFilterWriter;
    private final BloomType bloomType;
    private byte[] bloomParam = null;
    private long deleteFamilyCnt = 0;
    private BloomContext bloomContext = null;
    private BloomContext deleteFamilyBloomContext = null;
    private final Supplier<Collection<HStoreFile>> compactedFilesSupplier;

    private HFile.Writer writer;

    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs                     file system to write to
     * @param path                   file name to create
     * @param conf                   user configuration
     * @param bloomType              bloom filter setting
     * @param maxKeys                the expected maximum number of keys to be added. Was used for
     *                               Bloom filter size in {@link HFile} format version 1.
     * @param favoredNodes           an array of favored nodes or possibly null
     * @param fileContext            The HFile context
     * @param shouldDropCacheBehind  Drop pages written to page cache after writing the store file.
     * @param compactedFilesSupplier Returns the {@link HStore} compacted files which not archived
     * @throws IOException problem writing to FS
     */
    private SingleStoreFileWriter(FileSystem fs, Path path, final Configuration conf,
      CacheConfig cacheConf, BloomType bloomType, long maxKeys, InetSocketAddress[] favoredNodes,
      HFileContext fileContext, boolean shouldDropCacheBehind,
      Supplier<Collection<HStoreFile>> compactedFilesSupplier) throws IOException {
      this.compactedFilesSupplier = compactedFilesSupplier;
      // TODO : Change all writers to be specifically created for compaction context
      writer =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, path).withFavoredNodes(favoredNodes)
          .withFileContext(fileContext).withShouldDropCacheBehind(shouldDropCacheBehind).create();

      generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(conf, cacheConf,
        bloomType, (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);

      if (generalBloomFilterWriter != null) {
        this.bloomType = bloomType;
        this.bloomParam = BloomFilterUtil.getBloomFilterParam(bloomType, conf);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Bloom filter type for " + path + ": " + this.bloomType + ", param: "
            + (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH
              ? Bytes.toInt(bloomParam)
              : Bytes.toStringBinary(bloomParam))
            + ", " + generalBloomFilterWriter.getClass().getSimpleName());
        }
        // init bloom context
        switch (bloomType) {
          case ROW:
            bloomContext =
              new RowBloomContext(generalBloomFilterWriter, fileContext.getCellComparator());
            break;
          case ROWCOL:
            bloomContext =
              new RowColBloomContext(generalBloomFilterWriter, fileContext.getCellComparator());
            break;
          case ROWPREFIX_FIXED_LENGTH:
            bloomContext = new RowPrefixFixedLengthBloomContext(generalBloomFilterWriter,
              fileContext.getCellComparator(), Bytes.toInt(bloomParam));
            break;
          default:
            throw new IOException(
              "Invalid Bloom filter type: " + bloomType + " (ROW or ROWCOL or ROWPREFIX expected)");
        }
      } else {
        // Not using Bloom filters.
        this.bloomType = BloomType.NONE;
      }

      // initialize delete family Bloom filter when there is NO RowCol Bloom filter
      if (this.bloomType != BloomType.ROWCOL) {
        this.deleteFamilyBloomFilterWriter = BloomFilterFactory.createDeleteBloomAtWrite(conf,
          cacheConf, (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);
        deleteFamilyBloomContext =
          new RowBloomContext(deleteFamilyBloomFilterWriter, fileContext.getCellComparator());
      } else {
        deleteFamilyBloomFilterWriter = null;
      }
      if (deleteFamilyBloomFilterWriter != null && LOG.isTraceEnabled()) {
        LOG.trace("Delete Family Bloom filter type for " + path + ": "
          + deleteFamilyBloomFilterWriter.getClass().getSimpleName());
      }
    }

    private long getPos() throws IOException {
      return ((HFileWriterImpl) writer).getPos();
    }

    /**
     * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId   Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @throws IOException problem writing to FS
     */
    private void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
      throws IOException {
      appendMetadata(maxSequenceId, majorCompaction, Collections.emptySet());
    }

    /**
     * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId   Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @param storeFiles      The compacted store files to generate this new file
     * @throws IOException problem writing to FS
     */
    private void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
      final Collection<HStoreFile> storeFiles) throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
      writer.appendFileInfo(COMPACTION_EVENT_KEY, toCompactionEventTrackerBytes(storeFiles));
      appendTrackedTimestampsToMetadata();
    }

    /**
     * Used when write {@link HStoreFile#COMPACTION_EVENT_KEY} to new file's file info. The
     * compacted store files's name is needed. But if the compacted store file is a result of
     * compaction, it's compacted files which still not archived is needed, too. And don't need to
     * add compacted files recursively. If file A, B, C compacted to new file D, and file D
     * compacted to new file E, will write A, B, C, D to file E's compacted files. So if file E
     * compacted to new file F, will add E to F's compacted files first, then add E's compacted
     * files: A, B, C, D to it. And no need to add D's compacted file, as D's compacted files has
     * been in E's compacted files, too. See HBASE-20724 for more details.
     * @param storeFiles The compacted store files to generate this new file
     * @return bytes of CompactionEventTracker
     */
    private byte[] toCompactionEventTrackerBytes(Collection<HStoreFile> storeFiles) {
      Set<String> notArchivedCompactedStoreFiles = this.compactedFilesSupplier.get().stream()
        .map(sf -> sf.getPath().getName()).collect(Collectors.toSet());
      Set<String> compactedStoreFiles = new HashSet<>();
      for (HStoreFile storeFile : storeFiles) {
        compactedStoreFiles.add(storeFile.getFileInfo().getPath().getName());
        for (String csf : storeFile.getCompactedStoreFiles()) {
          if (notArchivedCompactedStoreFiles.contains(csf)) {
            compactedStoreFiles.add(csf);
          }
        }
      }
      return ProtobufUtil.toCompactionEventTrackerBytes(compactedStoreFiles);
    }

    /**
     * Writes meta data. Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId   Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @param mobCellsCount   The number of mob cells.
     * @throws IOException problem writing to FS
     */
    private void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
      final long mobCellsCount) throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
      writer.appendFileInfo(MOB_CELLS_COUNT, Bytes.toBytes(mobCellsCount));
      appendTrackedTimestampsToMetadata();
    }

    /**
     * Appends MOB - specific metadata (even if it is empty)
     * @param mobRefSet - original table -> set of MOB file names
     * @throws IOException problem writing to FS
     */
    private void appendMobMetadata(SetMultimap<TableName, String> mobRefSet) throws IOException {
      writer.appendFileInfo(MOB_FILE_REFS, MobUtils.serializeMobFileRefs(mobRefSet));
    }

    /**
     * Add TimestampRange and earliest put timestamp to Metadata
     */
    private void appendTrackedTimestampsToMetadata() throws IOException {
      writer.appendTrackedTimestampsToMetadata();
    }

    public void appendCustomCellTimestampsToMetadata(TimeRangeTracker timeRangeTracker)
      throws IOException {
      writer.appendCustomCellTimestampsToMetadata(timeRangeTracker);
    }

    private void appendGeneralBloomfilter(final ExtendedCell cell) throws IOException {
      if (this.generalBloomFilterWriter != null) {
        /*
         * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.
         * png Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + Timestamp 3 Types of
         * Filtering: 1. Row = Row 2. RowCol = Row + Qualifier 3. RowPrefixFixedLength = Fixed
         * Length Row Prefix
         */
        bloomContext.writeBloom(cell);
      }
    }

    private void appendDeleteFamilyBloomFilter(final ExtendedCell cell) throws IOException {
      if (!PrivateCellUtil.isDeleteFamily(cell) && !PrivateCellUtil.isDeleteFamilyVersion(cell)) {
        return;
      }

      // increase the number of delete family in the store file
      deleteFamilyCnt++;
      if (this.deleteFamilyBloomFilterWriter != null) {
        deleteFamilyBloomContext.writeBloom(cell);
      }
    }

    private void append(final ExtendedCell cell) throws IOException {
      appendGeneralBloomfilter(cell);
      appendDeleteFamilyBloomFilter(cell);
      writer.append(cell);
    }

    private void beforeShipped() throws IOException {
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

    private Path getPath() {
      return this.writer.getPath();
    }

    private boolean hasGeneralBloom() {
      return this.generalBloomFilterWriter != null;
    }

    /**
     * For unit testing only.
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

    private void close() throws IOException {
      boolean hasGeneralBloom = this.closeGeneralBloomFilter();
      boolean hasDeleteFamilyBloom = this.closeDeleteFamilyBloomFilter();

      writer.close();

      // Log final Bloom filter statistics. This needs to be done after close()
      // because compound Bloom filters might be finalized as part of closing.
      if (LOG.isTraceEnabled()) {
        LOG.trace((hasGeneralBloom ? "" : "NO ") + "General Bloom and "
          + (hasDeleteFamilyBloom ? "" : "NO ") + "DeleteFamily" + " was added to HFile "
          + getPath());
      }

    }

    private void appendFileInfo(byte[] key, byte[] value) throws IOException {
      writer.appendFileInfo(key, value);
    }

    /**
     * For use in testing.
     */
    private HFile.Writer getHFileWriter() {
      return writer;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification = "Will not overflow")
  public static class Builder {
    private final Configuration conf;
    private final CacheConfig cacheConf;
    private final FileSystem fs;

    private BloomType bloomType = BloomType.NONE;
    private long maxKeyCount = 0;
    private Path dir;
    private Path liveFilePath;
    private Path historicalFilePath;

    private InetSocketAddress[] favoredNodes;
    private HFileContext fileContext;
    private boolean shouldDropCacheBehind;
    private Supplier<Collection<HStoreFile>> compactedFilesSupplier = () -> Collections.emptySet();
    private String fileStoragePolicy;
    // this is used to track the creation of the StoreFileWriter, mainly used for the SFT
    // implementation where we will write store files directly to the final place, instead of
    // writing a tmp file first. Under this scenario, we will have a background task to purge the
    // store files which are not recorded in the SFT, but for the newly created store file writer,
    // they are not tracked in SFT, so here we need to record them and treat them specially.
    private Consumer<Path> writerCreationTracker;
    private int maxVersions;
    private boolean newVersionBehavior;
    private CellComparator comparator;
    private boolean isCompaction;

    public Builder(Configuration conf, CacheConfig cacheConf, FileSystem fs) {
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
     * @param dir Path to column family directory. The directory is created if does not exist. The
     *            file is given a unique name within this directory.
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
      this.liveFilePath = filePath;
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

    public Builder
      withCompactedFilesSupplier(Supplier<Collection<HStoreFile>> compactedFilesSupplier) {
      this.compactedFilesSupplier = compactedFilesSupplier;
      return this;
    }

    public Builder withFileStoragePolicy(String fileStoragePolicy) {
      this.fileStoragePolicy = fileStoragePolicy;
      return this;
    }

    public Builder withWriterCreationTracker(Consumer<Path> writerCreationTracker) {
      this.writerCreationTracker = writerCreationTracker;
      return this;
    }

    public Builder withMaxVersions(int maxVersions) {
      this.maxVersions = maxVersions;
      return this;
    }

    public Builder withNewVersionBehavior(boolean newVersionBehavior) {
      this.newVersionBehavior = newVersionBehavior;
      return this;
    }

    public Builder withCellComparator(CellComparator comparator) {
      this.comparator = comparator;
      return this;
    }

    public Builder withIsCompaction(boolean isCompaction) {
      this.isCompaction = isCompaction;
      return this;
    }

    /**
     * Create a store file writer. Client is responsible for closing file when done. If metadata,
     * add BEFORE closing using {@link StoreFileWriter#appendMetadata}.
     */
    public StoreFileWriter build() throws IOException {
      if ((dir == null ? 0 : 1) + (liveFilePath == null ? 0 : 1) != 1) {
        throw new IllegalArgumentException("Either specify parent directory " + "or file path");
      }

      if (dir == null) {
        dir = liveFilePath.getParent();
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
      CommonFSUtils.setStoragePolicy(this.fs, dir, policyName);

      if (liveFilePath == null) {
        // The stored file and related blocks will used the directory based StoragePolicy.
        // Because HDFS DistributedFileSystem does not support create files with storage policy
        // before version 3.3.0 (See HDFS-13209). Use child dir here is to make stored files
        // satisfy the specific storage policy when writing. So as to avoid later data movement.
        // We don't want to change whole temp dir to 'fileStoragePolicy'.
        if (!Strings.isNullOrEmpty(fileStoragePolicy)) {
          dir = new Path(dir, HConstants.STORAGE_POLICY_PREFIX + fileStoragePolicy);
          if (!fs.exists(dir)) {
            HRegionFileSystem.mkdirs(fs, conf, dir);
            LOG.info(
              "Create tmp dir " + dir.toString() + " with storage policy: " + fileStoragePolicy);
          }
          CommonFSUtils.setStoragePolicy(this.fs, dir, fileStoragePolicy);
        }
        liveFilePath = getUniqueFile(fs, dir);
        if (!BloomFilterFactory.isGeneralBloomEnabled(conf)) {
          bloomType = BloomType.NONE;
        }
      }

      if (isCompaction && shouldEnableHistoricalCompactionFiles(conf)) {
        historicalFilePath = getUniqueFile(fs, dir);
      }

      // make sure we call this before actually create the writer
      // in fact, it is not a big deal to even add an inexistent file to the track, as we will never
      // try to delete it and finally we will clean the tracker up after compaction. But if the file
      // cleaner find the file but we haven't recorded it yet, it may accidentally delete the file
      // and cause problem.
      if (writerCreationTracker != null) {
        writerCreationTracker.accept(liveFilePath);
        if (historicalFilePath != null) {
          writerCreationTracker.accept(historicalFilePath);
        }
      }
      return new StoreFileWriter(fs, liveFilePath, historicalFilePath, conf, cacheConf, bloomType,
        maxKeyCount, favoredNodes, fileContext, shouldDropCacheBehind, compactedFilesSupplier,
        comparator, maxVersions, newVersionBehavior);
    }
  }
}
