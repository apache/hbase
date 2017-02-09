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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

/**
 * Query matcher for normal user scan.
 */
@InterfaceAudience.Private
public abstract class NormalUserScanQueryMatcher extends UserScanQueryMatcher {

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /** True if we are doing a 'Get' Scan. Every Get is actually a one-row Scan. */
  private final boolean get;

  /** whether time range queries can see rows "behind" a delete */
  private final boolean seePastDeleteMarkers;

  protected NormalUserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, DeleteTracker deletes, long oldestUnexpiredTS, long now) {
    super(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS, now);
    this.deletes = deletes;
    this.get = scan.isGetScan();
    this.seePastDeleteMarkers = scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE;
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    long timestamp = cell.getTimestamp();
    byte typeByte = cell.getTypeByte();
    if (CellUtil.isDelete(typeByte)) {
      boolean includeDeleteMarker = seePastDeleteMarkers ? tr.withinTimeRange(timestamp)
          : tr.withinOrAfterTimeRange(timestamp);
      if (includeDeleteMarker) {
        this.deletes.add(cell);
      }
      return MatchCode.SKIP;
    }
    returnCode = checkDeleted(deletes, cell);
    if (returnCode != null) {
      return returnCode;
    }
    return matchColumn(cell, timestamp, typeByte);
  }

  @Override
  protected void reset() {
    deletes.reset();
  }

  @Override
  protected boolean isGet() {
    return get;
  }

  public static NormalUserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
      ColumnTracker columns, boolean hasNullColumn, long oldestUnexpiredTS, long now,
      RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    DeleteTracker deletes = instantiateDeleteTracker(regionCoprocessorHost);
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
