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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * KeyValueScanner adaptor over the Reader. It also provides hooks into bloom filter things.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.PHOENIX)
@InterfaceStability.Evolving
public class StoreFileScanner implements KeyValueScanner {
  // the reader it comes from:
  private final StoreFileReader reader;
  private final HFileScanner hfs;
  private Cell cur = null;
  private boolean closed = false;

  private boolean realSeekDone;
  private boolean delayedReseek;
  private Cell delayedSeekKV;

  private final boolean enforceMVCC;
  private final boolean hasMVCCInfo;
  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;

  private static LongAdder seekCount;

  private final boolean canOptimizeForNonNullColumn;

  private final long readPt;

  // Order of this scanner relative to other scanners when duplicate key-value is found.
  // Higher values means scanner has newer data.
  private final long scannerOrder;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param useMVCC                     If true, scanner will filter out updates with MVCC larger
   *                                    than {@code readPt}.
   * @param readPt                      MVCC value to use to filter out the updates newer than this
   *                                    scanner.
   * @param hasMVCC                     Set to true if underlying store file reader has MVCC info.
   * @param scannerOrder                Order of the scanner relative to other scanners. See
   *                                    {@link KeyValueScanner#getScannerOrder()}.
   * @param canOptimizeForNonNullColumn {@code true} if we can make sure there is no null column,
   *                                    otherwise {@code false}. This is a hint for optimization.
   */
  public StoreFileScanner(StoreFileReader reader, HFileScanner hfs, boolean useMVCC,
    boolean hasMVCC, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) {
    this.readPt = readPt;
    this.reader = reader;
    this.hfs = hfs;
    this.enforceMVCC = useMVCC;
    this.hasMVCCInfo = hasMVCC;
    this.scannerOrder = scannerOrder;
    this.canOptimizeForNonNullColumn = canOptimizeForNonNullColumn;
    this.reader.incrementRefCount();
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(Collection<HStoreFile> files,
    boolean cacheBlocks, boolean usePread, boolean isCompaction, boolean useDropBehind, long readPt)
    throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction, useDropBehind, null,
      readPt);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files, And set the
   * ScanQueryMatcher for each store file scanner for further optimization
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(Collection<HStoreFile> files,
    boolean cacheBlocks, boolean usePread, boolean isCompaction, boolean canUseDrop,
    ScanQueryMatcher matcher, long readPt) throws IOException {
    if (files.isEmpty()) {
      return Collections.emptyList();
    }
    List<StoreFileScanner> scanners = new ArrayList<>(files.size());
    boolean canOptimizeForNonNullColumn = matcher != null ? !matcher.hasNullColumnInQuery() : false;
    PriorityQueue<HStoreFile> sortedFiles =
      new PriorityQueue<>(files.size(), StoreFileComparators.SEQ_ID);
    for (HStoreFile file : files) {
      // The sort function needs metadata so we need to open reader first before sorting the list.
      file.initReader();
      sortedFiles.add(file);
    }
    boolean succ = false;
    try {
      for (int i = 0, n = files.size(); i < n; i++) {
        HStoreFile sf = sortedFiles.remove();
        StoreFileScanner scanner;
        if (usePread) {
          scanner = sf.getPreadScanner(cacheBlocks, readPt, i, canOptimizeForNonNullColumn);
        } else {
          scanner = sf.getStreamScanner(canUseDrop, cacheBlocks, isCompaction, readPt, i,
            canOptimizeForNonNullColumn);
        }
        scanners.add(scanner);
      }
      succ = true;
    } finally {
      if (!succ) {
        for (StoreFileScanner scanner : scanners) {
          scanner.close();
        }
      }
    }
    return scanners;
  }

  /**
   * Get scanners for compaction. We will create a separated reader for each store file to avoid
   * contention with normal read request.
   */
  public static List<StoreFileScanner> getScannersForCompaction(Collection<HStoreFile> files,
    boolean canUseDropBehind, long readPt) throws IOException {
    List<StoreFileScanner> scanners = new ArrayList<>(files.size());
    List<HStoreFile> sortedFiles = new ArrayList<>(files);
    Collections.sort(sortedFiles, StoreFileComparators.SEQ_ID);
    boolean succ = false;
    try {
      for (int i = 0, n = sortedFiles.size(); i < n; i++) {
        scanners.add(
          sortedFiles.get(i).getStreamScanner(canUseDropBehind, false, true, readPt, i, false));
      }
      succ = true;
    } finally {
      if (!succ) {
        for (StoreFileScanner scanner : scanners) {
          scanner.close();
        }
      }
    }
    return scanners;
  }

  @Override
  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  @Override
  public Cell peek() {
    return cur;
  }

  @Override
  public Cell next() throws IOException {
    Cell retKey = cur;

    try {
      // only seek if we aren't at the end. cur == null implies 'end'.
      if (cur != null) {
        hfs.next();
        setCurrentCell(hfs.getCell());
        if (hasMVCCInfo || this.reader.isBulkLoaded()) {
          skipKVsNewerThanReadpoint();
        }
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    if (seekCount != null) seekCount.increment();

    try {
      try {
        if (!seekAtOrAfter(hfs, key)) {
          this.cur = null;
          return false;
        }

        setCurrentCell(hfs.getCell());

        if (!hasMVCCInfo && this.reader.isBulkLoaded()) {
          return skipKVsNewerThanReadpoint();
        } else {
          return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
        }
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this + " to key " + key, ioe);
    }
  }

  @Override
  public boolean reseek(Cell key) throws IOException {
    if (seekCount != null) seekCount.increment();

    try {
      try {
        if (!reseekAtOrAfter(hfs, key)) {
          this.cur = null;
          return false;
        }
        setCurrentCell(hfs.getCell());

        if (!hasMVCCInfo && this.reader.isBulkLoaded()) {
          return skipKVsNewerThanReadpoint();
        } else {
          return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
        }
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not reseek " + this + " to key " + key, ioe);
    }
  }

  protected void setCurrentCell(Cell newVal) throws IOException {
    this.cur = newVal;
    if (this.cur != null && this.reader.isBulkLoaded() && !this.reader.isSkipResetSeqId()) {
      PrivateCellUtil.setSequenceId(cur, this.reader.getSequenceID());
    }
  }

  protected boolean skipKVsNewerThanReadpoint() throws IOException {
    // We want to ignore all key-values that are newer than our current
    // readPoint
    Cell startKV = cur;
    while (enforceMVCC && cur != null && (cur.getSequenceId() > readPt)) {
      boolean hasNext = hfs.next();
      setCurrentCell(hfs.getCell());
      if (
        hasNext && this.stopSkippingKVsIfNextRow && getComparator().compareRows(cur, startKV) > 0
      ) {
        return false;
      }
    }

    if (cur == null) {
      return false;
    }

    return true;
  }

  @Override
  public void close() {
    if (closed) return;
    cur = null;
    this.hfs.close();
    if (this.reader != null) {
      this.reader.readCompleted();
    }
    closed = true;
  }

  /**
   * nn * @return false if not found or if k is after the end. n
   */
  public static boolean seekAtOrAfter(HFileScanner s, Cell k) throws IOException {
    int result = s.seekTo(k);
    if (result < 0) {
      if (result == HConstants.INDEX_KEY_MAGIC) {
        // using faked key
        return true;
      }
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if (result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }

  static boolean reseekAtOrAfter(HFileScanner s, Cell k) throws IOException {
    // This function is similar to seekAtOrAfter function
    int result = s.reseekTo(k);
    if (result <= 0) {
      if (result == HConstants.INDEX_KEY_MAGIC) {
        // using faked key
        return true;
      }
      // If up to now scanner is not seeked yet, this means passed KV is smaller
      // than first KV in file, and it is the first time we seek on this file.
      // So we also need to work from the start of file.
      if (!s.isSeeked()) {
        return s.seekTo();
      }
      return true;
    }
    // passed KV is larger than current KV in file, if there is a next
    // it is after, if not then this scanner is done.
    return s.next();
  }

  /**
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return scannerOrder;
  }

  /**
   * Pretend we have done a seek but don't do it yet, if possible. The hope is that we find
   * requested columns in more recent files and won't have to seek in older files. Creates a fake
   * key/value with the given row/column and the highest (most recent) possible timestamp we might
   * get from this file. When users of such "lazy scanner" need to know the next KV precisely (e.g.
   * when this scanner is at the top of the heap), they run {@link #enforceSeek()}.
   * <p>
   * Note that this function does guarantee that the current KV of this scanner will be advanced to
   * at least the given KV. Because of this, it does have to do a real seek in cases when the seek
   * timestamp is older than the highest timestamp of the file, e.g. when we are trying to seek to
   * the next row/column and use OLDEST_TIMESTAMP in the seek key.
   */
  @Override
  public boolean requestSeek(Cell kv, boolean forward, boolean useBloom) throws IOException {
    if (kv.getFamilyLength() == 0) {
      useBloom = false;
    }

    boolean haveToSeek = true;
    if (useBloom) {
      // check ROWCOL Bloom filter first.
      if (reader.getBloomFilterType() == BloomType.ROWCOL) {
        haveToSeek = reader.passesGeneralRowColBloomFilter(kv);
      } else if (
        canOptimizeForNonNullColumn
          && ((PrivateCellUtil.isDeleteFamily(kv) || PrivateCellUtil.isDeleteFamilyVersion(kv)))
      ) {
        // if there is no such delete family kv in the store file,
        // then no need to seek.
        haveToSeek = reader.passesDeleteFamilyBloomFilter(kv.getRowArray(), kv.getRowOffset(),
          kv.getRowLength());
      }
    }

    delayedReseek = forward;
    delayedSeekKV = kv;

    if (haveToSeek) {
      // This row/column might be in this store file (or we did not use the
      // Bloom filter), so we still need to seek.
      realSeekDone = false;
      long maxTimestampInFile = reader.getMaxTimestamp();
      long seekTimestamp = kv.getTimestamp();
      if (seekTimestamp > maxTimestampInFile) {
        // Create a fake key that is not greater than the real next key.
        // (Lower timestamps correspond to higher KVs.)
        // To understand this better, consider that we are asked to seek to
        // a higher timestamp than the max timestamp in this file. We know that
        // the next point when we have to consider this file again is when we
        // pass the max timestamp of this file (with the same row/column).
        setCurrentCell(PrivateCellUtil.createFirstOnRowColTS(kv, maxTimestampInFile));
      } else {
        // This will be the case e.g. when we need to seek to the next
        // row/column, and we don't know exactly what they are, so we set the
        // seek key's timestamp to OLDEST_TIMESTAMP to skip the rest of this
        // row/column.
        enforceSeek();
      }
      return cur != null;
    }

    // Multi-column Bloom filter optimization.
    // Create a fake key/value, so that this scanner only bubbles up to the top
    // of the KeyValueHeap in StoreScanner after we scanned this row/column in
    // all other store files. The query matcher will then just skip this fake
    // key/value and the store scanner will progress to the next column. This
    // is obviously not a "real real" seek, but unlike the fake KV earlier in
    // this method, we want this to be propagated to ScanQueryMatcher.
    setCurrentCell(PrivateCellUtil.createLastOnRowCol(kv));

    realSeekDone = true;
    return true;
  }

  StoreFileReader getReader() {
    return reader;
  }

  CellComparator getComparator() {
    return reader.getComparator();
  }

  @Override
  public boolean realSeekDone() {
    return realSeekDone;
  }

  @Override
  public void enforceSeek() throws IOException {
    if (realSeekDone) return;

    if (delayedReseek) {
      reseek(delayedSeekKV);
    } else {
      seek(delayedSeekKV);
    }
  }

  @Override
  public boolean isFileScanner() {
    return true;
  }

  @Override
  public Path getFilePath() {
    return reader.getHFileReader().getPath();
  }

  // Test methods
  static final long getSeekCount() {
    return seekCount.sum();
  }

  static final void instrument() {
    seekCount = new LongAdder();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
    // if the file has no entries, no need to validate or create a scanner.
    byte[] cf = store.getColumnFamilyDescriptor().getName();
    TimeRange timeRange = scan.getColumnFamilyTimeRange().get(cf);
    if (timeRange == null) {
      timeRange = scan.getTimeRange();
    }
    return reader.passesTimerangeFilter(timeRange, oldestUnexpiredTS)
      && reader.passesKeyRangeFilter(scan)
      && reader.passesBloomFilter(scan, scan.getFamilyMap().get(cf));
  }

  @Override
  public boolean seekToPreviousRow(Cell originalKey) throws IOException {
    try {
      try {
        boolean keepSeeking = false;
        Cell key = originalKey;
        do {
          Cell seekKey = PrivateCellUtil.createFirstOnRow(key);
          if (seekCount != null) seekCount.increment();
          if (!hfs.seekBefore(seekKey)) {
            this.cur = null;
            return false;
          }
          Cell curCell = hfs.getCell();
          Cell firstKeyOfPreviousRow = PrivateCellUtil.createFirstOnRow(curCell);

          if (seekCount != null) seekCount.increment();
          if (!seekAtOrAfter(hfs, firstKeyOfPreviousRow)) {
            this.cur = null;
            return false;
          }

          setCurrentCell(hfs.getCell());
          this.stopSkippingKVsIfNextRow = true;
          boolean resultOfSkipKVs;
          try {
            resultOfSkipKVs = skipKVsNewerThanReadpoint();
          } finally {
            this.stopSkippingKVsIfNextRow = false;
          }
          if (!resultOfSkipKVs || getComparator().compareRows(cur, firstKeyOfPreviousRow) > 0) {
            keepSeeking = true;
            key = firstKeyOfPreviousRow;
            continue;
          } else {
            keepSeeking = false;
          }
        } while (keepSeeking);
        return true;
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seekToPreviousRow " + this + " to key " + originalKey, ioe);
    }
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    Optional<byte[]> lastRow = reader.getLastRowKey();
    if (!lastRow.isPresent()) {
      return false;
    }
    Cell seekKey = PrivateCellUtil.createFirstOnRow(lastRow.get());
    if (seek(seekKey)) {
      return true;
    } else {
      return seekToPreviousRow(seekKey);
    }
  }

  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    seek(key);
    if (cur == null || getComparator().compareRows(cur, key) > 0) {
      return seekToPreviousRow(key);
    }
    return true;
  }

  @Override
  public Cell getNextIndexedKey() {
    return hfs.getNextIndexedKey();
  }

  @Override
  public void shipped() throws IOException {
    this.hfs.shipped();
  }
}
