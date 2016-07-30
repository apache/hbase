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
 * Query matcher for minor compaction.
 */
@InterfaceAudience.Private
public class MinorCompactionScanQueryMatcher extends CompactionScanQueryMatcher {

  public MinorCompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
      long readPointToUse, long oldestUnexpiredTS, long now) {
    super(scanInfo, deletes, readPointToUse, oldestUnexpiredTS, now);
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
        // we should not use this delete marker to mask any cell yet.
        return MatchCode.INCLUDE;
      }
      trackDelete(cell);
      return MatchCode.INCLUDE;
    }
    returnCode = checkDeleted(deletes, cell);
    if (returnCode != null) {
      return returnCode;
    }
    // Skip checking column since we do not remove column during compaction.
    return columns.checkVersions(cell, cell.getTimestamp(), cell.getTypeByte(),
      mvccVersion > maxReadPointToTrackVersions);
  }
}
