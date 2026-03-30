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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for normal user scan.
 */
@InterfaceAudience.Private
public abstract class NormalUserScanQueryMatcher extends UserScanQueryMatcher {

  /**
   * Number of consecutive range delete markers (DeleteColumn/DeleteFamily) to skip before switching
   * to seek. Seeking is more expensive than skipping for a single marker, but much faster when
   * markers accumulate. This threshold avoids the seek overhead for the common case (one delete per
   * row/column) while still kicking in when markers pile up.
   */
  private static final int SEEK_ON_DELETE_MARKER_THRESHOLD = 3;

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /** True if we are doing a 'Get' Scan. Every Get is actually a one-row Scan. */
  private final boolean get;

  /** whether time range queries can see rows "behind" a delete */
  protected final boolean seePastDeleteMarkers;

  /** Whether seek optimization for range delete markers is applicable */
  private final boolean canSeekOnDeleteMarker;

  /** Count of consecutive range delete markers seen */
  private int rangeDeleteCount;

  protected NormalUserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
    boolean hasNullColumn, DeleteTracker deletes, long oldestUnexpiredTS, long now) {
    super(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS, now);
    this.deletes = deletes;
    this.get = scan.isGetScan();
    this.seePastDeleteMarkers = scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE;
    this.canSeekOnDeleteMarker =
      !seePastDeleteMarkers && deletes.getClass() == ScanDeleteTracker.class;
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    deletes.beforeShipped();
  }

  @Override
  public MatchCode match(ExtendedCell cell) throws IOException {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    long timestamp = cell.getTimestamp();
    byte typeByte = cell.getTypeByte();
    if (PrivateCellUtil.isDelete(typeByte)) {
      boolean includeDeleteMarker =
        seePastDeleteMarkers ? tr.withinTimeRange(timestamp) : tr.withinOrAfterTimeRange(timestamp);
      if (includeDeleteMarker) {
        this.deletes.add(cell);
        // A DeleteColumn or DeleteFamily masks all remaining cells for this column/family.
        // Seek past them instead of skipping one cell at a time, but only after seeing
        // enough consecutive markers to justify the seek overhead.
        // Only safe with plain ScanDeleteTracker. Not safe with newVersionBehavior (sequence
        // IDs determine visibility), visibility labels (delete/put label mismatch), or
        // seePastDeleteMarkers (KEEP_DELETED_CELLS).
        if (
          canSeekOnDeleteMarker && (typeByte == KeyValue.Type.DeleteFamily.getCode()
            || (typeByte == KeyValue.Type.DeleteColumn.getCode() && cell.getQualifierLength() > 0))
        ) {
          if (++rangeDeleteCount >= SEEK_ON_DELETE_MARKER_THRESHOLD) {
            rangeDeleteCount = 0;
            return columns.getNextRowOrNextColumn(cell);
          }
        } else {
          rangeDeleteCount = 0;
        }
      }
      return MatchCode.SKIP;
    }
    rangeDeleteCount = 0;
    returnCode = checkDeleted(deletes, cell);
    if (returnCode != null) {
      return returnCode;
    }
    return matchColumn(cell, timestamp, typeByte);
  }

  @Override
  protected void reset() {
    deletes.reset();
    rangeDeleteCount = 0;
  }

  @Override
  protected boolean isGet() {
    return get;
  }

  public static NormalUserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
    ColumnTracker columns, DeleteTracker deletes, boolean hasNullColumn, long oldestUnexpiredTS,
    long now) throws IOException {
    if (scan.isReversed()) {
      if (scan.includeStopRow()) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow >= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow > 0;
          }
        };
      }
    } else {
      if (scan.includeStopRow()) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow <= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow < 0;
          }
        };
      }
    }
  }
}
