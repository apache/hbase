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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

/**
 * Query matcher for raw scan.
 */
@InterfaceAudience.Private
public class RawScanQueryMatcher extends UserScanQueryMatcher {

  protected RawScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, long oldestUnexpiredTS, long now) {
    super(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS, now);
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
    // For a raw scan, we do not filter out any cells by delete marker, and delete marker is also
    // returned, so we do not need to track delete.
    return matchColumn(cell);
  }

  @Override
  protected void reset() {
  }

  @Override
  protected boolean isGet() {
    return false;
  }

  public static RawScanQueryMatcher create(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, long oldestUnexpiredTS, long now) {
    if (scan.isReversed()) {
      return new RawScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS,
          now) {

        @Override
        protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
          return cmpToStopRow > 0;
        }
      };
    } else {
      return new RawScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS,
          now);
    }
  }
}
