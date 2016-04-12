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
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader for a StoreFile.
 */
@InterfaceAudience.Private
public class StoreFileReader {
  private static final Log LOG = LogFactory.getLog(StoreFileReader.class.getName());

  protected BloomFilter generalBloomFilter = null;
  protected BloomFilter deleteFamilyBloomFilter = null;
  protected BloomType bloomFilterType;
  private final HFile.Reader reader;
  protected TimeRangeTracker timeRangeTracker = null;
  protected long sequenceID = -1;
  private byte[] lastBloomKey;
  private long deleteFamilyCnt = -1;
  private boolean bulkLoadResult = false;
  private KeyValue.KeyOnlyKeyValue lastBloomKeyOnlyKV = null;
  private boolean skipResetSeqId = true;

  public AtomicInteger getRefCount() {
    return refCount;
  }

  // Counter that is incremented every time a scanner is created on the
  // store file.  It is decremented when the scan on the store file is
  // done.
  private AtomicInteger refCount = new AtomicInteger(0);
  // Indicates if the file got compacted
  private volatile boolean compactedAway = false;

  public StoreFileReader(FileSystem fs, Path path, CacheConfig cacheConf, Configuration conf)
      throws IOException {
    reader = HFile.createReader(fs, path, cacheConf, conf);
    bloomFilterType = BloomType.NONE;
  }

  void markCompactedAway() {
    this.compactedAway = true;
  }

  public StoreFileReader(FileSystem fs, Path path, FSDataInputStreamWrapper in, long size,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    reader = HFile.createReader(fs, path, in, size, cacheConf, conf);
    bloomFilterType = BloomType.NONE;
  }

  public void setReplicaStoreFile(boolean isPrimaryReplicaStoreFile) {
    reader.setPrimaryReplicaReader(isPrimaryReplicaStoreFile);
  }
  public boolean isPrimaryReplicaReader() {
    return reader.isPrimaryReplicaReader();
  }

  /**
   * ONLY USE DEFAULT CONSTRUCTOR FOR UNIT TESTS
   */
  StoreFileReader() {
    this.reader = null;
  }

  public CellComparator getComparator() {
    return reader.getComparator();
  }

  /**
   * Get a scanner to scan over this StoreFile. Do not use
   * this overload if using this scanner for compactions.
   *
   * @param cacheBlocks should this scanner cache blocks?
   * @param pread use pread (for highly concurrent small readers)
   * @return a scanner
   */
  public StoreFileScanner getStoreFileScanner(boolean cacheBlocks,
                                             boolean pread) {
    return getStoreFileScanner(cacheBlocks, pread, false,
      // 0 is passed as readpoint because this method is only used by test
      // where StoreFile is directly operated upon
      0);
  }

  /**
   * Get a scanner to scan over this StoreFile.
   *
   * @param cacheBlocks should this scanner cache blocks?
   * @param pread use pread (for highly concurrent small readers)
   * @param isCompaction is scanner being used for compaction?
   * @return a scanner
   */
  public StoreFileScanner getStoreFileScanner(boolean cacheBlocks,
                                             boolean pread,
                                             boolean isCompaction, long readPt) {
    // Increment the ref count
    refCount.incrementAndGet();
    return new StoreFileScanner(this,
                               getScanner(cacheBlocks, pread, isCompaction),
                               !isCompaction, reader.hasMVCCInfo(), readPt);
  }

  /**
   * Decrement the ref count associated with the reader when ever a scanner associated
   * with the reader is closed
   */
  void decrementRefCount() {
    refCount.decrementAndGet();
  }

  /**
   * @return true if the file is still used in reads
   */
  public boolean isReferencedInReads() {
    return refCount.get() != 0;
  }

  /**
   * @return true if the file is compacted
   */
  public boolean isCompactedAway() {
    return this.compactedAway;
  }

  /**
   * @deprecated Do not write further code which depends on this call. Instead
   *   use getStoreFileScanner() which uses the StoreFileScanner class/interface
   *   which is the preferred way to scan a store with higher level concepts.
   *
   * @param cacheBlocks should we cache the blocks?
   * @param pread use pread (for concurrent small readers)
   * @return the underlying HFileScanner
   */
  @Deprecated
  public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * @deprecated Do not write further code which depends on this call. Instead
   *   use getStoreFileScanner() which uses the StoreFileScanner class/interface
   *   which is the preferred way to scan a store with higher level concepts.
   *
   * @param cacheBlocks
   *          should we cache the blocks?
   * @param pread
   *          use pread (for concurrent small readers)
   * @param isCompaction
   *          is scanner being used for compaction?
   * @return the underlying HFileScanner
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
   * @param timeRange the timeRange to restrict
   * @param oldestUnexpiredTS the oldest timestamp that is not expired, as
   *          determined by the column family's TTL
   * @return false if queried keys definitely don't exist in this StoreFile
   */
  boolean passesTimerangeFilter(TimeRange timeRange, long oldestUnexpiredTS) {
    if (timeRangeTracker == null) {
      return true;
    } else {
      return timeRangeTracker.includesTimeRange(timeRange) &&
          timeRangeTracker.getMaximumTimestamp() >= oldestUnexpiredTS;
    }
  }

  /**
   * Checks whether the given scan passes the Bloom filter (if present). Only
   * checks Bloom filters for single-row or single-row-column scans. Bloom
   * filter checking for multi-gets is implemented as part of the store
   * scanner system (see {@link StoreFileScanner#seekExactly}) and uses
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
    // Multi-column non-get scans will use Bloom filters through the
    // lower-level API function that this function calls.
    if (!scan.isGetScan()) {
      return true;
    }

    byte[] row = scan.getStartRow();
    switch (this.bloomFilterType) {
      case ROW:
        return passesGeneralRowBloomFilter(row, 0, row.length);

      case ROWCOL:
        if (columns != null && columns.size() == 1) {
          byte[] column = columns.first();
          // create the required fake key
          Cell kvKey = KeyValueUtil.createFirstOnRow(row, 0, row.length,
            HConstants.EMPTY_BYTE_ARRAY, 0, 0, column, 0,
            column.length);
          return passesGeneralRowColBloomFilter(kvKey);
        }

        // For multi-column queries the Bloom filter is checked from the
        // seekExact operation.
        return true;

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
  public boolean passesGeneralRowBloomFilter(byte[] row, int rowOffset, int rowLen) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      return true;
    }

    // Used in ROW bloom
    byte[] key = null;
    if (rowOffset != 0 || rowLen != row.length) {
      throw new AssertionError(
          "For row-only Bloom filters the row " + "must occupy the whole array");
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
      kvKey = CellUtil.createFirstOnRowCol(cell);
    }
    return checkGeneralBloomFilter(null, kvKey, bloomFilter);
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
          if (bloomFilterType == BloomType.ROW) {
            keyIsAfterLast = (Bytes.BYTES_RAWCOMPARATOR.compare(key, lastBloomKey) > 0);
          } else {
            keyIsAfterLast = (CellComparator.COMPARATOR.compare(kvKey, lastBloomKeyOnlyKV)) > 0;
          }
        }

        if (bloomFilterType == BloomType.ROWCOL) {
          // Since a Row Delete is essentially a DeleteFamily applied to all
          // columns, a file might be skipped if using row+col Bloom filter.
          // In order to ensure this file is included an additional check is
          // required looking only for a row bloom.
          Cell rowBloomKey = CellUtil.createFirstOnRow(kvKey);
          // hbase:meta does not have blooms. So we need not have special interpretation
          // of the hbase:meta cells.  We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
          if (keyIsAfterLast
              && (CellComparator.COMPARATOR.compare(rowBloomKey, lastBloomKeyOnlyKV)) > 0) {
            exists = false;
          } else {
            exists =
                bloomFilter.contains(kvKey, bloom) ||
                bloomFilter.contains(rowBloomKey, bloom);
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
      // Return the bloom block so that its ref count can be decremented.
      reader.returnBlock(bloomBlock);
    }
    return true;
  }

  /**
   * Checks whether the given scan rowkey range overlaps with the current storefile's
   * @param scan the scan specification. Used to determine the rowkey range.
   * @return true if there is overlap, false otherwise
   */
  public boolean passesKeyRangeFilter(Scan scan) {
    if (this.getFirstKey() == null || this.getLastKey() == null) {
      // the file is empty
      return false;
    }
    if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
        && Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      return true;
    }
    byte[] smallestScanRow = scan.isReversed() ? scan.getStopRow() : scan.getStartRow();
    byte[] largestScanRow = scan.isReversed() ? scan.getStartRow() : scan.getStopRow();
    Cell firstKeyKV = this.getFirstKey();
    Cell lastKeyKV = this.getLastKey();
    boolean nonOverLapping = (getComparator().compareRows(firstKeyKV,
        largestScanRow, 0, largestScanRow.length) > 0
        && !Bytes
        .equals(scan.isReversed() ? scan.getStartRow() : scan.getStopRow(),
            HConstants.EMPTY_END_ROW))
        || getComparator().compareRows(lastKeyKV, smallestScanRow, 0, smallestScanRow.length) < 0;
    return !nonOverLapping;
  }

  public Map<byte[], byte[]> loadFileInfo() throws IOException {
    Map<byte [], byte []> fi = reader.loadFileInfo();

    byte[] b = fi.get(StoreFile.BLOOM_FILTER_TYPE_KEY);
    if (b != null) {
      bloomFilterType = BloomType.valueOf(Bytes.toString(b));
    }

    lastBloomKey = fi.get(StoreFile.LAST_BLOOM_KEY);
    if(bloomFilterType == BloomType.ROWCOL) {
      lastBloomKeyOnlyKV = new KeyValue.KeyOnlyKeyValue(lastBloomKey, 0, lastBloomKey.length);
    }
    byte[] cnt = fi.get(StoreFile.DELETE_FAMILY_COUNT);
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

  public Cell getLastKey() {
    return reader.getLastKey();
  }

  public byte[] getLastRowKey() {
    return reader.getLastRowKey();
  }

  public Cell midkey() throws IOException {
    return reader.midkey();
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

  public Cell getFirstKey() {
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
    return timeRangeTracker == null ? Long.MAX_VALUE : timeRangeTracker.getMaximumTimestamp();
  }

  boolean isSkipResetSeqId() {
    return skipResetSeqId;
  }

  void setSkipResetSeqId(boolean skipResetSeqId) {
    this.skipResetSeqId = skipResetSeqId;
  }
}
