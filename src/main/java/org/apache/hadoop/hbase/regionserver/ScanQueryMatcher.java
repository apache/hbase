/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

/**
 * A query matcher that is specifically designed for the scan case.
 */
public class ScanQueryMatcher {
  // Optimization so we can skip lots of compares when we decide to skip
  // to the next row.
  private boolean stickyNextRow;
  private byte[] stopRow;

  protected TimeRange tr;

  protected Filter filter;

  /** Keeps track of deletes */
  protected DeleteTracker deletes;
  protected long retainDeletesInOutputUntil;

  /** Keeps track of columns and versions */
  protected ColumnTracker columns;

  /** Key to seek to in memstore and StoreFiles */
  protected KeyValue startKey;

  /** Oldest allowed version stamp for TTL enforcement */
  protected long oldestStamp;

  /** Row comparator for the region this query is for */
  KeyValue.KeyComparator rowComparator;

  /** Row the query is on */
  protected byte [] row;

  /** readPoint over which the KVs are unconditionally included */
  protected long maxReadPointToTrackVersions;

  /**
   * This variable shows whether there is an null column in the query. There
   * always exists a null column in the wildcard column query.
   * There maybe exists a null column in the explicit column query based on the
   * first column.
   * */
  private boolean hasNullColumn = true;

  private final long oldestFlashBackTS;

  private final long effectiveTS;

  /**
   * Constructs a ScanQueryMatcher for a Scan.
   * @param scan
   * @param family
   * @param columnSet
   * @param rowComparator
   */
  public ScanQueryMatcher(Scan scan, byte [] family,
      NavigableSet<byte[]> columnSet, KeyValue.KeyComparator rowComparator,
      int maxVersions, long readPointToUse,
      long retainDeletesInOutputUntil,
      long oldestUnexpiredTS) {
    this(scan, family, columnSet, rowComparator, maxVersions, readPointToUse,
        retainDeletesInOutputUntil, oldestUnexpiredTS,
        HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Constructs a ScanQueryMatcher for a Scan.
   *
   * @param scan
   * @param family
   * @param columnSet
   * @param rowComparator
   */
  public ScanQueryMatcher(Scan scan, byte[] family,
      NavigableSet<byte[]> columnSet, KeyValue.KeyComparator rowComparator,
      int maxVersions, long readPointToUse, long retainDeletesInOutputUntil,
      long oldestUnexpiredTS, long oldestFlashBackTS) {
    this.tr = scan.getTimeRange();
    this.oldestStamp = oldestUnexpiredTS;
    this.rowComparator = rowComparator;
    this.deletes =  new ScanDeleteTracker();
    this.stopRow = scan.getStopRow();
    this.effectiveTS = scan.getEffectiveTS();
    this.startKey = KeyValue.createDeleteFamilyOnRow(scan.getStartRow(),
        family, effectiveTS);
    this.filter = scan.getFilter();
    this.retainDeletesInOutputUntil = retainDeletesInOutputUntil;
    this.maxReadPointToTrackVersions = readPointToUse;
    this.oldestFlashBackTS = oldestFlashBackTS;

    // Single branch to deal with two types of reads (columns vs all in family)
    if (columnSet == null || columnSet.size() == 0) {
      // there is always a null column in the wildcard column query.
      hasNullColumn = true;

      // use a specialized scan for wildcard column tracker.
      this.columns = new ScanWildcardColumnTracker(maxVersions,
          this.oldestFlashBackTS);
    } else {
      // whether there is null column in the explicit column query
      hasNullColumn = (columnSet.first().length == 0);

      // We can share the ExplicitColumnTracker, diff is we reset
      // between rows, not between storefiles.
      // Note that we do not use oldestFlashBackTS here since
      // ExplicitColumnTracker is never used in Compactions/Flushes.
      this.columns = new ExplicitColumnTracker(columnSet, maxVersions);
    }
  }

  /**
   *
   * @return  whether there is an null column in the query
   */
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  public ScanQueryMatcher(Scan scan, byte [] family,
      NavigableSet<byte[]> columns, KeyValue.KeyComparator rowComparator,
      int maxVersions, long oldestUnexpiredTS) {
      // By default we will not include deletes.
      // Deletes are included explicitly (for minor compaction).
      this(scan, family, columns, rowComparator, maxVersions,
          Long.MAX_VALUE, // max Readpoint to Track versions
          Long.MAX_VALUE, // do not include deletes
          oldestUnexpiredTS);
  }

  /**
   * Determines if the caller should do one of several things:
   * - seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)
   * - seek/skip to the next column (MatchCode.SEEK_NEXT_COL)
   * - include the current KeyValue (MatchCode.INCLUDE)
   * - ignore the current KeyValue (MatchCode.SKIP)
   * - got to the next row (MatchCode.DONE)
   *
   * @param kv KeyValue to check
   * @return The match code instance.
   * @throws IOException in case there is an internal consistency problem
   *      caused by a data corruption.
   */
  public MatchCode match(KeyValue kv) throws IOException {
    if (kv.getTimestamp() > effectiveTS) {
      return MatchCode.SEEK_TO_EFFECTIVE_TS;
    }
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }

    byte [] bytes = kv.getBuffer();
    int offset = kv.getOffset();
    int initialOffset = offset;

    int keyLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
    offset += KeyValue.ROW_OFFSET;

    short rowLength = Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT);
    offset += Bytes.SIZEOF_SHORT;

    int ret = this.rowComparator.compareRows(row, 0, row.length,
        bytes, offset, rowLength);
    if (ret <= -1) {
      return MatchCode.DONE;
    } else if (ret >= 1) {
      // could optimize this, if necessary?
      // Could also be called SEEK_TO_CURRENT_ROW, but this
      // should be rare/never happens.
      return MatchCode.SEEK_NEXT_ROW;
    }

    // optimize case.
    if (this.stickyNextRow)
        return MatchCode.SEEK_NEXT_ROW;

    if (this.columns.done()) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    //Passing rowLength
    offset += rowLength;

    //Skipping family
    byte familyLength = bytes [offset];
    offset += familyLength + 1;

    int qualLength = keyLength + KeyValue.ROW_OFFSET -
      (offset - initialOffset) - KeyValue.TIMESTAMP_TYPE_SIZE;

    long timestamp = kv.getTimestamp();
    if (isExpired(timestamp)) {
      // done, the rest of this column will also be expired as well.
      return getNextRowOrNextColumn(bytes, offset, qualLength);
    }

    byte type = kv.getType();
    if (isDelete(type) && kv.getTimestamp() > this.oldestFlashBackTS) {
      // During Flushes and Compactions we don't want to process deletes unless
      // they are older than the FLASHBACK_QUERY_LIMIT.
      return MatchCode.INCLUDE;
    } else if (isDelete(type)) {
      if (tr.withinOrAfterTimeRange(timestamp)) {
        this.deletes.add(bytes, offset, qualLength, timestamp, type);
        // Can't early out now, because DelFam come before any other keys
      }
      if (timestamp >= this.retainDeletesInOutputUntil) {
        return MatchCode.INCLUDE;
      }
      else {
        return MatchCode.SKIP;
      }
    }

    if (!this.deletes.isEmpty()) {
      DeleteResult deleteResult = deletes.isDeleted(bytes, offset, qualLength,
          timestamp);
      switch (deleteResult) {
        case FAMILY_DELETED:
        case COLUMN_DELETED:
          return getNextRowOrNextColumn(bytes, offset, qualLength);
        case VERSION_DELETED:
          return MatchCode.SKIP;
        case NOT_DELETED:
          break;
        default:
          throw new RuntimeException("UNEXPECTED");
        }
    }

    int timestampComparison = tr.compare(timestamp);
    if (timestampComparison >= 1) {
      return MatchCode.SKIP;
    } else if (timestampComparison <= -1) {
      return getNextRowOrNextColumn(bytes, offset, qualLength);
    }

    /**
     * Filters should be checked before checking column trackers. If we do
     * otherwise, as was previously being done, ColumnTracker may increment its
     * counter for even that KV which may be discarded later on by Filter. This
     * would lead to incorrect results in certain cases.
     */
    if (filter != null) {
      ReturnCode filterResponse = filter.filterKeyValue(kv, allScanners);
      if (filterResponse == ReturnCode.SKIP) {
        return MatchCode.SKIP;
      } else if (filterResponse == ReturnCode.NEXT_COL) {
        return getNextRowOrNextColumn(bytes, offset, qualLength);
      } else if (filterResponse == ReturnCode.NEXT_ROW) {
        stickyNextRow = true;
        return MatchCode.SEEK_NEXT_ROW;
      } else if (filterResponse == ReturnCode.SEEK_NEXT_USING_HINT) {
        return MatchCode.SEEK_NEXT_USING_HINT;
      }
    }

    MatchCode colChecker = columns.checkColumn(bytes, offset, qualLength, timestamp,
         kv.getMemstoreTS() > maxReadPointToTrackVersions);
    /*
     * According to current implementation, colChecker can only be
     * SEEK_NEXT_COL, SEEK_NEXT_ROW, SKIP or INCLUDE. Therefore, always return
     * the MatchCode. If it is SEEK_NEXT_ROW, also set stickyNextRow.
     */
    if (colChecker == MatchCode.SEEK_NEXT_ROW) {
      stickyNextRow = true;
    }
    return colChecker;

  }

  public MatchCode getNextRowOrNextColumn(byte[] bytes, int offset,
      int qualLength) {
    if (columns instanceof ExplicitColumnTracker) {
      //We only come here when we know that columns is an instance of
      //ExplicitColumnTracker so we should never have a cast exception
      ((ExplicitColumnTracker)columns).doneWithColumn(bytes, offset,
          qualLength);
      if (columns.getColumnHint() == null) {
        return MatchCode.SEEK_NEXT_ROW;
      } else {
        return MatchCode.SEEK_NEXT_COL;
      }
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  public boolean moreRowsMayExistAfter(KeyValue kv) {
    if (!Bytes.equals(stopRow , HConstants.EMPTY_END_ROW) &&
        rowComparator.compareRows(kv.getBuffer(),kv.getRowOffset(),
            kv.getRowLength(), stopRow, 0, stopRow.length) >= 0) {
      // KV >= STOPROW
      // then NO there is nothing left.
      return false;
    } else {
      return true;
    }
  }

  /**
   * Set current row
   * @param row
   */
  public void setRow(byte [] row) {
    this.row = row;
    reset();
  }

  public void reset() {
    this.deletes.reset();
    this.columns.reset();

    stickyNextRow = false;
  }

  // should be in KeyValue.
  protected boolean isDelete(byte type) {
    return (type != KeyValue.Type.Put.getCode());
  }

  protected boolean isExpired(long timestamp) {
    return (timestamp < oldestStamp);
  }

  /**
   *
   * @return the start key
   */
  public KeyValue getStartKey() {
    return this.startKey;
  }

  public KeyValue getNextKeyHint(KeyValue kv) {
    if (filter == null) {
      return null;
    } else {
      return filter.getNextKeyHint(kv);
    }
  }

  /**
   * If there is no next column, we return a KV which is the last possible key
   * for the current row. Otherwise, we proceed to the next kv (the first kv on
   * the next column). if there is no delete column blooom filter enabled, we
   * return the next kv on the column. If the delete column bloom filter is
   * enabled, we first check whether the scanners contain delete information
   * about the next kv. If there is no delete, we proceed to the searched kv
   * with highest timestamp.
   *
   * @param kv - current keyvalue
   * @param allScanners - all scanners including the scanners from memstore and
   * storefiles
   * @param deleteColBloomFilterEnabled - is the delete column bloom filter
   * ebnabled
   * @return - kv in the next column
   */
  public KeyValue getKeyForNextColumn(KeyValue kv, List<KeyValueScanner> allScanners, boolean deleteColBloomFilterEnabled) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return KeyValue.createLastOnRow(
          kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
          kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    } else {
      KeyValue nextKV = KeyValue.createFirstOnRow(kv.getBuffer(),
          kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
          kv.getFamilyOffset(), kv.getFamilyLength(), nextColumn.getBuffer(),
          nextColumn.getOffset(), nextColumn.getLength());
      if (!deleteColBloomFilterEnabled) {
        return nextKV;
      }
      for (KeyValueScanner kvScanner : allScanners) {
        if (kvScanner.passesDeleteColumnCheck(nextKV))
          return nextKV;
      }
      HRegionServer.numOptimizedSeeks.incrementAndGet();
      return KeyValue.createFirstOnRow(nextKV.getBuffer(), nextKV.getRowOffset(), nextKV.getRowLength(),
          nextKV.getBuffer(), nextKV.getFamilyOffset(), nextKV.getFamilyLength(),
          nextColumn.getBuffer(), nextColumn.getOffset(), nextColumn.getLength(),
          Math.max(tr.getMax() -1, tr.getMin()));
    }
  }

  public KeyValue getKeyForNextRow(KeyValue kv) {
    return KeyValue.createLastOnRow(
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
        null, 0, 0,
        null, 0, 0);
  }

  public KeyValue getKeyForEffectiveTSOnRow(KeyValue kv) {
    return kv.createFirstOnRowColTS(effectiveTS);
  }

  /**
   * {@link #match} return codes.  These instruct the scanner moving through
   * memstores and StoreFiles what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to
   * move on to the next File (memstore or Storefile), or to return immediately.
   */
  public static enum MatchCode {
    /**
     * Include KeyValue in the returned result
     */
    INCLUDE,

    /**
     * Do not include KeyValue in the returned result
     */
    SKIP,

    /**
     * Do not include, jump to next StoreFile or memstore (in time order)
     */
    NEXT,

    /**
     * Do not include, return current result
     */
    DONE,

    /**
     * These codes are used by the ScanQueryMatcher
     */

    /**
     * Done with the row, seek there.
     */
    SEEK_NEXT_ROW,
    /**
     * Done with column, seek to next.
     */
    SEEK_NEXT_COL,

    /**
     * Done with scan, thanks to the row filter.
     */
    DONE_SCAN,

    /*
     * Seek to next key which is given as hint.
     */
    SEEK_NEXT_USING_HINT,

    SEEK_TO_EFFECTIVE_TS,

    /**
     * Include KeyValue and done with column, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_COL,

    /**
     * Include KeyValue and done with row, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  }
}
