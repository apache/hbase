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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

/**
 * Query matcher for stripe compaction if range drop deletes is used.
 */
@InterfaceAudience.Private
public class StripeCompactionScanQueryMatcher extends DropDeletesCompactionScanQueryMatcher {

  private final byte[] dropDeletesFromRow;

  private final byte[] dropDeletesToRow;

  private enum DropDeletesInOutput {
    BEFORE, IN, AFTER
  }

  private DropDeletesInOutput dropDeletesInOutput = DropDeletesInOutput.BEFORE;

  public StripeCompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow) {
    super(scanInfo, deletes, readPointToUse, earliestPutTs, oldestUnexpiredTS, now);
    this.dropDeletesFromRow = dropDeletesFromRow;
    this.dropDeletesToRow = dropDeletesToRow;
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    long mvccVersion = cell.getSequenceId();
    if (CellUtil.isDelete(cell)) {
      if (mvccVersion > maxReadPointToTrackVersions) {
        return MatchCode.INCLUDE;
      }
      trackDelete(cell);
      if (dropDeletesInOutput == DropDeletesInOutput.IN) {
        // here we are running like major compaction
        trackDelete(cell);
        returnCode = tryDropDelete(cell);
        if (returnCode != null) {
          return returnCode;
        }
      } else {
        return MatchCode.INCLUDE;
      }
    } else {
      returnCode = checkDeleted(deletes, cell);
      if (returnCode != null) {
        return returnCode;
      }
    }
    // Skip checking column since we do not remove column during compaction.
    return columns.checkVersions(cell, cell.getTimestamp(), cell.getTypeByte(),
      mvccVersion > maxReadPointToTrackVersions);
  }

  private boolean entered() {
    return dropDeletesFromRow.length == 0 || rowComparator.compareRows(currentRow,
      dropDeletesFromRow, 0, dropDeletesFromRow.length) >= 0;
  }

  private boolean left() {
    return dropDeletesToRow.length > 0
        && rowComparator.compareRows(currentRow, dropDeletesToRow, 0, dropDeletesToRow.length) >= 0;
  }

  @Override
  protected void reset() {
    super.reset();
    // Check if we are about to enter or leave the drop deletes range.
    switch (dropDeletesInOutput) {
      case BEFORE:
        if (entered()) {
          if (left()) {
            // Already out of range, which means there are no rows within the range.
            dropDeletesInOutput = DropDeletesInOutput.AFTER;
          } else {
            dropDeletesInOutput = DropDeletesInOutput.IN;
          }
        }
        break;
      case IN:
        if (left()) {
          dropDeletesInOutput = DropDeletesInOutput.AFTER;
        }
        break;
      default:
        break;
    }
  }
}
