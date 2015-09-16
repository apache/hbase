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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;

/**
 * KeyValueScanner adaptor over the Reader.  It also provides hooks into
 * bloom filter things.
 */
@InterfaceAudience.LimitedPrivate("Coprocessor")
public class StoreFileScanner implements KeyValueScanner {
  // the reader it comes from:
  private final StoreFile.Reader reader;
  private final HFileScanner hfs;
  private final byte[] columnFamily;
  private Cell cur = null;

  private boolean realSeekDone;
  private boolean delayedReseek;
  private Cell delayedSeekKV;

  private boolean enforceMVCC = false;
  private boolean hasMVCCInfo = false;
  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;

  private static AtomicLong seekCount;

  private ScanQueryMatcher matcher;


  private long readPt;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileScanner(StoreFile.Reader reader, HFileScanner hfs, boolean useMVCC,
      boolean hasMVCC, long readPt) {
    this(reader, hfs, null, useMVCC, hasMVCC, readPt);
  }

  public StoreFileScanner(StoreFile.Reader reader, HFileScanner hfs, byte[] columnFamily,
      boolean useMVCC, boolean hasMVCC, long readPt) {
    this.readPt = readPt;
    this.reader = reader;
    this.hfs = hfs;
    this.columnFamily = columnFamily;
    this.enforceMVCC = useMVCC;
    this.hasMVCCInfo = hasMVCC;
  }

  boolean isPrimaryReplica() {
    return reader.isPrimaryReplicaReader();
  }

  /**
   * Return an array of scanners corresponding to the given
   * set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files,
      boolean cacheBlocks,
      boolean usePread, long readPt) throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks,
                                   usePread, false, false, readPt);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
      boolean isCompaction, boolean useDropBehind, long readPt) throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction,
        useDropBehind, null, readPt);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files,
   * And set the ScanQueryMatcher for each store file scanner for further
   * optimization
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
      boolean isCompaction, boolean canUseDrop,
      ScanQueryMatcher matcher, long readPt, boolean isPrimaryReplica) throws IOException {
    List<StoreFileScanner> scanners = new ArrayList<StoreFileScanner>(
        files.size());
    for (StoreFile file : files) {
      StoreFile.Reader r = file.createReader(canUseDrop);
      r.setReplicaStoreFile(isPrimaryReplica);
      StoreFileScanner scanner = r.getStoreFileScanner(cacheBlocks, usePread,
          isCompaction, readPt);
      scanner.setScanQueryMatcher(matcher);
      scanners.add(scanner);
    }
    return scanners;
  }

  public static List<StoreFileScanner> getScannersForStoreFiles(
    Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
    boolean isCompaction, boolean canUseDrop,
    ScanQueryMatcher matcher, long readPt) throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction, canUseDrop,
      matcher, readPt, true);
  }

  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  public Cell peek() {
    return cur;
  }

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
    } catch(IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(Cell key) throws IOException {
    if (seekCount != null) seekCount.incrementAndGet();

    try {
      try {
        if(!seekAtOrAfter(hfs, key)) {
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

  public boolean reseek(Cell key) throws IOException {
    if (seekCount != null) seekCount.incrementAndGet();

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
      throw new IOException("Could not reseek " + this + " to key " + key,
          ioe);
    }
  }

  protected void setCurrentCell(Cell newVal) throws IOException {
    this.cur = newVal;
    if (this.cur != null && this.reader.isBulkLoaded() && !this.reader.isSkipResetSeqId()) {
      CellUtil.setSequenceId(cur, this.reader.getSequenceID());
    }
  }

  protected boolean skipKVsNewerThanReadpoint() throws IOException {
    // We want to ignore all key-values that are newer than our current
    // readPoint
    Cell startKV = cur;
    while(enforceMVCC
        && cur != null
        && (cur.getSequenceId() > readPt)) {
      hfs.next();
      setCurrentCell(hfs.getCell());
      if (this.stopSkippingKVsIfNextRow
          && getComparator().compareRows(cur, startKV) > 0) {
        return false;
      }
    }

    if (cur == null) {
      return false;
    }

    return true;
  }

  public void close() {
    cur = null;
    this.hfs.close();
  }

  /**
   *
   * @param s
   * @param k
   * @return false if not found or if k is after the end.
   * @throws IOException
   */
  public static boolean seekAtOrAfter(HFileScanner s, Cell k)
  throws IOException {
    int result = s.seekTo(k);
    if(result < 0) {
      if (result == HConstants.INDEX_KEY_MAGIC) {
        // using faked key
        return true;
      }
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if(result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }

  static boolean reseekAtOrAfter(HFileScanner s, Cell k)
  throws IOException {
    //This function is similar to seekAtOrAfter function
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
        return  s.seekTo();
      }
      return true;
    }
    // passed KV is larger than current KV in file, if there is a next
    // it is after, if not then this scanner is done.
    return s.next();
  }

  @Override
  public long getSequenceID() {
    return reader.getSequenceID();
  }

  /**
   * Pretend we have done a seek but don't do it yet, if possible. The hope is
   * that we find requested columns in more recent files and won't have to seek
   * in older files. Creates a fake key/value with the given row/column and the
   * highest (most recent) possible timestamp we might get from this file. When
   * users of such "lazy scanner" need to know the next KV precisely (e.g. when
   * this scanner is at the top of the heap), they run {@link #enforceSeek()}.
   * <p>
   * Note that this function does guarantee that the current KV of this scanner
   * will be advanced to at least the given KV. Because of this, it does have
   * to do a real seek in cases when the seek timestamp is older than the
   * highest timestamp of the file, e.g. when we are trying to seek to the next
   * row/column and use OLDEST_TIMESTAMP in the seek key.
   */
  @Override
  public boolean requestSeek(Cell kv, boolean forward, boolean useBloom)
      throws IOException {
    if (kv.getFamilyLength() == 0) {
      useBloom = false;
    }

    boolean haveToSeek = true;
    if (useBloom) {
      // check ROWCOL Bloom filter first.
      if (reader.getBloomFilterType() == BloomType.ROWCOL) {
        haveToSeek = reader.passesGeneralRowColBloomFilter(kv);
      } else if (this.matcher != null && !matcher.hasNullColumnInQuery() &&
          ((CellUtil.isDeleteFamily(kv) || CellUtil.isDeleteFamilyVersion(kv)))) {
        // if there is no such delete family kv in the store file,
        // then no need to seek.
        haveToSeek = reader.passesDeleteFamilyBloomFilter(kv.getRowArray(),
            kv.getRowOffset(), kv.getRowLength());
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
        setCurrentCell(CellUtil.createFirstOnRowColTS(kv, maxTimestampInFile));
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
    setCurrentCell(CellUtil.createLastOnRowCol(kv));

    realSeekDone = true;
    return true;
  }

  Reader getReader() {
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
    if (realSeekDone)
      return;

    if (delayedReseek) {
      reseek(delayedSeekKV);
    } else {
      seek(delayedSeekKV);
    }
  }

  public void setScanQueryMatcher(ScanQueryMatcher matcher) {
    this.matcher = matcher;
  }

  @Override
  public boolean isFileScanner() {
    return true;
  }

  // Test methods

  static final long getSeekCount() {
    return seekCount.get();
  }
  static final void instrument() {
    seekCount = new AtomicLong();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
    validateScanRestrictions(scan);
    TimeRange timeRange = scan.getTimeRangeForColumnFamily(columnFamily);
    return reader.passesTimerangeFilter(timeRange, oldestUnexpiredTS)
        && reader.passesKeyRangeFilter(scan) && reader.passesBloomFilter(scan, columns);
  }

  @Override
  public boolean seekToPreviousRow(Cell key) throws IOException {
    try {
      try {
        Cell seekKey = CellUtil.createFirstOnRow(key);
        if (seekCount != null) seekCount.incrementAndGet();
        if (!hfs.seekBefore(seekKey)) {
          this.cur = null;
          return false;
        }
        Cell curCell = hfs.getCell();
        Cell firstKeyOfPreviousRow = CellUtil.createFirstOnRow(curCell);

        if (seekCount != null) seekCount.incrementAndGet();
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
        if (!resultOfSkipKVs
            || getComparator().compareRows(cur, firstKeyOfPreviousRow) > 0) {
          return seekToPreviousRow(firstKeyOfPreviousRow);
        }

        return true;
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seekToPreviousRow " + this + " to key "
          + key, ioe);
    }
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    byte[] lastRow = reader.getLastRowKey();
    if (lastRow == null) {
      return false;
    }
    KeyValue seekKey = KeyValueUtil.createFirstOnRow(lastRow);
    if (seek(seekKey)) {
      return true;
    } else {
      return seekToPreviousRow(seekKey);
    }
  }

  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    seek(key);
    if (cur == null
        || getComparator().compareRows(cur, key) > 0) {
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

  // if we have a scan that restricts per column family time ranges but our
  // store file scanner does not have the HColumnDescriptor set we can not
  // figure out which CF we are scanning thus we throw an exception
  @VisibleForTesting void validateScanRestrictions(Scan scan) {
    if (!scan.getColumnFamilyTimeRange().isEmpty() && this.columnFamily == null) {
      throw new UnsupportedOperationException("Scan is trying to restrict time ranges on a per "
          + "column family basis but null column family passed to the StoreFileScannerConstructor");
    }

  }
}
