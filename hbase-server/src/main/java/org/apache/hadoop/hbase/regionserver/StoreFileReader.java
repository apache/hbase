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
import static org.apache.hadoop.hbase.regionserver.HStoreFile.LAST_BLOOM_KEY;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader for a StoreFile.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.PHOENIX)
@InterfaceStability.Evolving
public class StoreFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileReader.class.getName());

  protected BloomFilter generalBloomFilter = null;
  protected BloomFilter deleteFamilyBloomFilter = null;
  protected BloomType bloomFilterType;
  private final HFile.Reader reader;
  protected long sequenceID = -1;
  protected TimeRange timeRange = null;
  private byte[] lastBloomKey;
  private long deleteFamilyCnt = -1;
  private boolean bulkLoadResult = false;
  private KeyValue.KeyOnlyKeyValue lastBloomKeyOnlyKV = null;
  private boolean skipResetSeqId = true;
  private int prefixLength = -1;

  // Counter that is incremented every time a scanner is created on the
  // store file. It is decremented when the scan on the store file is
  // done. All StoreFileReader for the same StoreFile will share this counter.
  private final AtomicInteger refCount;
  private final ReaderContext context;

  private StoreFileReader(HFile.Reader reader, AtomicInteger refCount, ReaderContext context) {
    this.reader = reader;
    bloomFilterType = BloomType.NONE;
    this.refCount = refCount;
    this.context = context;
  }

  public StoreFileReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
      AtomicInteger refCount, Configuration conf) throws IOException {
    this(HFile.createReader(context, fileInfo, cacheConf, conf), refCount, context);
  }

  void copyFields(StoreFileReader storeFileReader) throws IOException {
    this.generalBloomFilter = storeFileReader.generalBloomFilter;
    this.deleteFamilyBloomFilter = storeFileReader.deleteFamilyBloomFilter;
    this.bloomFilterType = storeFileReader.bloomFilterType;
    this.sequenceID = storeFileReader.sequenceID;
    this.timeRange = storeFileReader.timeRange;
    this.lastBloomKey = storeFileReader.lastBloomKey;
    this.bulkLoadResult = storeFileReader.bulkLoadResult;
    this.lastBloomKeyOnlyKV = storeFileReader.lastBloomKeyOnlyKV;
    this.skipResetSeqId = storeFileReader.skipResetSeqId;
    this.prefixLength = storeFileReader.prefixLength;
  }

  public boolean isPrimaryReplicaReader() {
    return reader.isPrimaryReplicaReader();
  }

  /**
   * ONLY USE DEFAULT CONSTRUCTOR FOR UNIT TESTS
   */
  @InterfaceAudience.Private
  StoreFileReader() {
    this.refCount = new AtomicInteger(0);
    this.reader = null;
    this.context = null;
  }

  public CellComparator getComparator() {
    return reader.getComparator();
  }

  /**
   * Get a scanner to scan over this StoreFile.
   * @param cacheBlocks should this scanner cache blocks?
   * @param pread use pread (for highly concurrent small readers)
   * @param isCompaction is scanner being used for compaction?
   * @param scannerOrder Order of this scanner relative to other scanners. See
   *          {@link KeyValueScanner#getScannerOrder()}.
   * @param canOptimizeForNonNullColumn {@code true} if we can make sure there is no null column,
   *          otherwise {@code false}. This is a hint for optimization.
   * @return a scanner
   */
  public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) {
    return new StoreFileScanner(this, getScanner(cacheBlocks, pread, isCompaction),
        !isCompaction, reader.hasMVCCInfo(), readPt, scannerOrder, canOptimizeForNonNullColumn);
  }

  /**
   * Return the ref count associated with the reader whenever a scanner associated with the
   * reader is opened.
   */
  int getRefCount() {
    return refCount.get();
  }

  /**
   * Indicate that the scanner has started reading with this reader. We need to increment the ref
   * count so reader is not close until some object is holding the lock
   */
  void incrementRefCount() {
    refCount.incrementAndGet();
  }

  /**
   * Indicate that the scanner has finished reading with this reader. We need to decrement the ref
   * count, and also, if this is not the common pread reader, we should close it.
   */
  void readCompleted() {
    refCount.decrementAndGet();
    if (context.getReaderType() == ReaderType.STREAM) {
      try {
        reader.close(false);
      } catch (IOException e) {
        LOG.warn("failed to close stream reader", e);
      }
    }
  }

  /**
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Do not write further code which depends
   *   on this call. Instead use getStoreFileScanner() which uses the StoreFileScanner
   *   class/interface which is the preferred way to scan a store with higher level concepts.
   *
   * @param cacheBlocks should we cache the blocks?
   * @param pread use pread (for concurrent small readers)
   * @return the underlying HFileScanner
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15296">HBASE-15296</a>
   */
  @Deprecated
  public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Do not write further code which depends
   *   on this call. Instead use getStoreFileScanner() which uses the StoreFileScanner
   *   class/interface which is the preferred way to scan a store with higher level concepts.
   *
   * @param cacheBlocks
   *          should we cache the blocks?
   * @param pread
   *          use pread (for concurrent small readers)
   * @param isCompaction
   *          is scanner being used for compaction?
   * @return the underlying HFileScanner
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15296">HBASE-15296</a>
   */
  @Deprecated
  public HFileScanner getScanner(boolean cacheBlocks, boolean pread,
      boolean isCompaction) {
    return reader.getScanner(cacheBlocks, pread, isCompaction);
  }

  public void close(boolean evictOnClose) throws IOException {
    reader.close(evictOnClose);
  }

  /**
   * Check if this storeFile may contain keys within the TimeRange that
   * have not expired (i.e. not older than oldestUnexpiredTS).
   * @param tr the timeRange to restrict
   * @param oldestUnexpiredTS the oldest timestamp that is not expired, as
   *          determined by the column family's TTL
   * @return false if queried keys definitely don't exist in this StoreFile
   */
  boolean passesTimerangeFilter(TimeRange tr, long oldestUnexpiredTS) {
    return this.timeRange == null? true:
      this.timeRange.includesTimeRange(tr) && this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  /**
   * Checks whether the given scan passes the Bloom filter (if present). Only
   * checks Bloom filters for single-row or single-row-column scans. Bloom
   * filter checking for multi-gets is implemented as part of the store
   * scanner system (see {@link StoreFileScanner#seek(Cell)} and uses
   * the lower-level API {@link #passesGeneralRowBloomFilter(byte[], int, int)}
   * and {@link #passesGeneralRowColBloomFilter(Cell)}.
   *
   * @param scan the scan specification. Used to determine the row, and to
   *          check whether this is a single-row ("get") scan.
   * @param columns the set of columns. Only used for row-column Bloom
   *          filters.
   * @return true if the scan with the given column set passes the Bloom
   *         filter, or if the Bloom filter is not applicable for the scan.
   *         False if the Bloom filter is applicable and the scan fails it.
   */
  boolean passesBloomFilter(Scan scan, final SortedSet<byte[]> columns) {
    byte[] row = scan.getStartRow();
    switch (this.bloomFilterType) {
      case ROW:
        if (!scan.isGetScan()) {
          return true;
        }
        return passesGeneralRowBloomFilter(row, 0, row.length);

      case ROWCOL:
        if (!scan.isGetScan()) {
          return true;
        }
        if (columns != null && columns.size() == 1) {
          byte[] column = columns.first();
          // create the required fake key
          Cell kvKey = PrivateCellUtil.createFirstOnRow(row, HConstants.EMPTY_BYTE_ARRAY, column);
          return passesGeneralRowColBloomFilter(kvKey);
        }

        // For multi-column queries the Bloom filter is checked from the
        // seekExact operation.
        return true;
      case ROWPREFIX_FIXED_LENGTH:
        return passesGeneralRowPrefixBloomFilter(scan);
      default:
        return true;
    }
  }

  public boolean passesDeleteFamilyBloomFilter(byte[] row, int rowOffset,
      int rowLen) {
    // Cache Bloom filter as a local variable in case it is set to null by
    // another thread on an IO error.
    BloomFilter bloomFilter = this.deleteFamilyBloomFilter;

    // Empty file or there is no delete family at all
    if (reader.getTrailer().getEntryCount() == 0 || deleteFamilyCnt == 0) {
      return false;
    }

    if (bloomFilter == null) {
      return true;
    }

    try {
      if (!bloomFilter.supportsAutoLoading()) {
        return true;
      }
      return bloomFilter.contains(row, rowOffset, rowLen, null);
    } catch (IllegalArgumentException e) {
      LOG.error("Bad Delete Family bloom filter data -- proceeding without",
          e);
      setDeleteFamilyBloomFilterFaulty();
    }

    return true;
  }

  /**
   * A method for checking Bloom filters. Called directly from
   * StoreFileScanner in case of a multi-column query.
   *
   * @return True if passes
   */
  private boolean passesGeneralRowBloomFilter(byte[] row, int rowOffset, int rowLen) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      return true;
    }

    // Used in ROW bloom
    byte[] key = null;
    if (rowOffset != 0 || rowLen != row.length) {
      throw new AssertionError(
          "For row-only Bloom filters the row must occupy the whole array");
    }
    key = row;
    return checkGeneralBloomFilter(key, null, bloomFilter);
  }

  /**
   * A method for checking Bloom filters. Called directly from
   * StoreFileScanner in case of a multi-column query.
   *
   * @param cell
   *          the cell to check if present in BloomFilter
   * @return True if passes
   */
  public boolean passesGeneralRowColBloomFilter(Cell cell) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      return true;
    }
    // Used in ROW_COL bloom
    Cell kvKey = null;
    // Already if the incoming key is a fake rowcol key then use it as it is
    if (cell.getTypeByte() == KeyValue.Type.Maximum.getCode() && cell.getFamilyLength() == 0) {
      kvKey = cell;
    } else {
      kvKey = PrivateCellUtil.createFirstOnRowCol(cell);
    }
    return checkGeneralBloomFilter(null, kvKey, bloomFilter);
  }

  /**
   * A method for checking Bloom filters. Called directly from
   * StoreFileScanner in case of a multi-column query.
   *
   * @return True if passes
   */
  private boolean passesGeneralRowPrefixBloomFilter(Scan scan) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      return true;
    }

    byte[] row = scan.getStartRow();
    byte[] rowPrefix;
    if (scan.isGetScan()) {
      rowPrefix = Bytes.copy(row, 0, Math.min(prefixLength, row.length));
    } else {
      // For non-get scans
      // Find out the common prefix of startRow and stopRow.
      int commonLength = Bytes.findCommonPrefix(scan.getStartRow(), scan.getStopRow(),
          scan.getStartRow().length, scan.getStopRow().length, 0, 0);
      // startRow and stopRow don't have the common prefix.
      // Or the common prefix length is less than prefixLength
      if (commonLength <= 0 || commonLength < prefixLength) {
        return true;
      }
      rowPrefix = Bytes.copy(row, 0, prefixLength);
    }
    return checkGeneralBloomFilter(rowPrefix, null, bloomFilter);
  }

  private boolean checkGeneralBloomFilter(byte[] key, Cell kvKey, BloomFilter bloomFilter) {
    // Empty file
    if (reader.getTrailer().getEntryCount() == 0) {
      return false;
    }
    HFileBlock bloomBlock = null;
    try {
      boolean shouldCheckBloom;
      ByteBuff bloom;
      if (bloomFilter.supportsAutoLoading()) {
        bloom = null;
        shouldCheckBloom = true;
      } else {
        bloomBlock = reader.getMetaBlock(HFile.BLOOM_FILTER_DATA_KEY, true);
        bloom = bloomBlock.getBufferWithoutHeader();
        shouldCheckBloom = bloom != null;
      }

      if (shouldCheckBloom) {
        boolean exists;

        // Whether the primary Bloom key is greater than the last Bloom key
        // from the file info. For row-column Bloom filters this is not yet
        // a sufficient condition to return false.
        boolean keyIsAfterLast = (lastBloomKey != null);
        // hbase:meta does not have blooms. So we need not have special interpretation
        // of the hbase:meta cells.  We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
        if (keyIsAfterLast) {
          if (bloomFilterType == BloomType.ROWCOL) {
            keyIsAfterLast = (CellComparator.getInstance().compare(kvKey, lastBloomKeyOnlyKV)) > 0;
          } else {
            keyIsAfterLast = (Bytes.BYTES_RAWCOMPARATOR.compare(key, lastBloomKey) > 0);
          }
        }

        if (bloomFilterType == BloomType.ROWCOL) {
          // Since a Row Delete is essentially a DeleteFamily applied to all
          // columns, a file might be skipped if using row+col Bloom filter.
          // In order to ensure this file is included an additional check is
          // required looking only for a row bloom.
          Cell rowBloomKey = PrivateCellUtil.createFirstOnRow(kvKey);
          // hbase:meta does not have blooms. So we need not have special interpretation
          // of the hbase:meta cells.  We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
          if (keyIsAfterLast
              && (CellComparator.getInstance().compare(rowBloomKey, lastBloomKeyOnlyKV)) > 0) {
            exists = false;
          } else {
            exists =
                bloomFilter.contains(kvKey, bloom, BloomType.ROWCOL) ||
                bloomFilter.contains(rowBloomKey, bloom, BloomType.ROWCOL);
          }
        } else {
          exists = !keyIsAfterLast
              && bloomFilter.contains(key, 0, key.length, bloom);
        }

        return exists;
      }
    } catch (IOException e) {
      LOG.error("Error reading bloom filter data -- proceeding without",
          e);
      setGeneralBloomFilterFaulty();
    } catch (IllegalArgumentException e) {
      LOG.error("Bad bloom filter data -- proceeding without", e);
      setGeneralBloomFilterFaulty();
    } finally {
      // Release the bloom block so that its ref count can be decremented.
      if (bloomBlock != null) {
        bloomBlock.release();
      }
    }
    return true;
  }

  /**
   * Checks whether the given scan rowkey range overlaps with the current storefile's
   * @param scan the scan specification. Used to determine the rowkey range.
   * @return true if there is overlap, false otherwise
   */
  public boolean passesKeyRangeFilter(Scan scan) {
    Optional<Cell> firstKeyKV = this.getFirstKey();
    Optional<Cell> lastKeyKV = this.getLastKey();
    if (!firstKeyKV.isPresent() || !lastKeyKV.isPresent()) {
      // the file is empty
      return false;
    }
    if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW) &&
        Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      return true;
    }
    byte[] smallestScanRow = scan.isReversed() ? scan.getStopRow() : scan.getStartRow();
    byte[] largestScanRow = scan.isReversed() ? scan.getStartRow() : scan.getStopRow();
    boolean nonOverLapping = (getComparator()
        .compareRows(firstKeyKV.get(), largestScanRow, 0, largestScanRow.length) > 0 &&
        !Bytes.equals(scan.isReversed() ? scan.getStartRow() : scan.getStopRow(),
          HConstants.EMPTY_END_ROW)) ||
        getComparator().compareRows(lastKeyKV.get(), smallestScanRow, 0,
          smallestScanRow.length) < 0;
    return !nonOverLapping;
  }

  public Map<byte[], byte[]> loadFileInfo() throws IOException {
    Map<byte [], byte []> fi = reader.getHFileInfo();

    byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
    if (b != null) {
      bloomFilterType = BloomType.valueOf(Bytes.toString(b));
    }

    byte[] p = fi.get(BLOOM_FILTER_PARAM_KEY);
    if (bloomFilterType ==  BloomType.ROWPREFIX_FIXED_LENGTH) {
      prefixLength = Bytes.toInt(p);
    }

    lastBloomKey = fi.get(LAST_BLOOM_KEY);
    if(bloomFilterType == BloomType.ROWCOL) {
      lastBloomKeyOnlyKV = new KeyValue.KeyOnlyKeyValue(lastBloomKey, 0, lastBloomKey.length);
    }
    byte[] cnt = fi.get(DELETE_FAMILY_COUNT);
    if (cnt != null) {
      deleteFamilyCnt = Bytes.toLong(cnt);
    }

    return fi;
  }

  public void loadBloomfilter() {
    this.loadBloomfilter(BlockType.GENERAL_BLOOM_META);
    this.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META);
  }

  public void loadBloomfilter(BlockType blockType) {
    try {
      if (blockType == BlockType.GENERAL_BLOOM_META) {
        if (this.generalBloomFilter != null)
          return; // Bloom has been loaded

        DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
        if (bloomMeta != null) {
          // sanity check for NONE Bloom filter
          if (bloomFilterType == BloomType.NONE) {
            throw new IOException(
                "valid bloom filter type not found in FileInfo");
          } else {
            generalBloomFilter = BloomFilterFactory.createFromMeta(bloomMeta,
                reader);
            if (LOG.isTraceEnabled()) {
              LOG.trace("Loaded " + bloomFilterType.toString() + " "
                + generalBloomFilter.getClass().getSimpleName()
                + " metadata for " + reader.getName());
            }
          }
        }
      } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
        if (this.deleteFamilyBloomFilter != null)
          return; // Bloom has been loaded

        DataInput bloomMeta = reader.getDeleteBloomFilterMetadata();
        if (bloomMeta != null) {
          deleteFamilyBloomFilter = BloomFilterFactory.createFromMeta(
              bloomMeta, reader);
          LOG.info("Loaded Delete Family Bloom ("
              + deleteFamilyBloomFilter.getClass().getSimpleName()
              + ") metadata for " + reader.getName());
        }
      } else {
        throw new RuntimeException("Block Type: " + blockType.toString()
            + "is not supported for Bloom filter");
      }
    } catch (IOException e) {
      LOG.error("Error reading bloom filter meta for " + blockType
          + " -- proceeding without", e);
      setBloomFilterFaulty(blockType);
    } catch (IllegalArgumentException e) {
      LOG.error("Bad bloom filter meta " + blockType
          + " -- proceeding without", e);
      setBloomFilterFaulty(blockType);
    }
  }

  private void setBloomFilterFaulty(BlockType blockType) {
    if (blockType == BlockType.GENERAL_BLOOM_META) {
      setGeneralBloomFilterFaulty();
    } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
      setDeleteFamilyBloomFilterFaulty();
    }
  }

  /**
   * The number of Bloom filter entries in this store file, or an estimate
   * thereof, if the Bloom filter is not loaded. This always returns an upper
   * bound of the number of Bloom filter entries.
   *
   * @return an estimate of the number of Bloom filter entries in this file
   */
  public long getFilterEntries() {
    return generalBloomFilter != null ? generalBloomFilter.getKeyCount()
        : reader.getEntries();
  }

  public void setGeneralBloomFilterFaulty() {
    generalBloomFilter = null;
  }

  public void setDeleteFamilyBloomFilterFaulty() {
    this.deleteFamilyBloomFilter = null;
  }

  public Optional<Cell> getLastKey() {
    return reader.getLastKey();
  }

  public Optional<byte[]> getLastRowKey() {
    return reader.getLastRowKey();
  }

  public Optional<Cell> midKey() throws IOException {
    return reader.midKey();
  }

  public long length() {
    return reader.length();
  }

  public long getTotalUncompressedBytes() {
    return reader.getTrailer().getTotalUncompressedBytes();
  }

  public long getEntries() {
    return reader.getEntries();
  }

  public long getDeleteFamilyCnt() {
    return deleteFamilyCnt;
  }

  public Optional<Cell> getFirstKey() {
    return reader.getFirstKey();
  }

  public long indexSize() {
    return reader.indexSize();
  }

  public BloomType getBloomFilterType() {
    return this.bloomFilterType;
  }

  public long getSequenceID() {
    return sequenceID;
  }

  public void setSequenceID(long sequenceID) {
    this.sequenceID = sequenceID;
  }

  public void setBulkLoaded(boolean bulkLoadResult) {
    this.bulkLoadResult = bulkLoadResult;
  }

  public boolean isBulkLoaded() {
    return this.bulkLoadResult;
  }

  BloomFilter getGeneralBloomFilter() {
    return generalBloomFilter;
  }

  long getUncompressedDataIndexSize() {
    return reader.getTrailer().getUncompressedDataIndexSize();
  }

  public long getTotalBloomSize() {
    if (generalBloomFilter == null)
      return 0;
    return generalBloomFilter.getByteSize();
  }

  public int getHFileVersion() {
    return reader.getTrailer().getMajorVersion();
  }

  public int getHFileMinorVersion() {
    return reader.getTrailer().getMinorVersion();
  }

  public HFile.Reader getHFileReader() {
    return reader;
  }

  void disableBloomFilterForTesting() {
    generalBloomFilter = null;
    this.deleteFamilyBloomFilter = null;
  }

  public long getMaxTimestamp() {
    return timeRange == null ? TimeRange.INITIAL_MAX_TIMESTAMP: timeRange.getMax();
  }

  boolean isSkipResetSeqId() {
    return skipResetSeqId;
  }

  void setSkipResetSeqId(boolean skipResetSeqId) {
    this.skipResetSeqId = skipResetSeqId;
  }

  public int getPrefixLength() {
    return prefixLength;
  }

  public ReaderContext getReaderContext() {
    return this.context;
  }
}
