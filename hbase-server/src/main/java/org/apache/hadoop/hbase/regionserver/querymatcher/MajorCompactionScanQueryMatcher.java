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
 * Query matcher for major compaction.
 */
@InterfaceAudience.Private
public class MajorCompactionScanQueryMatcher extends DropDeletesCompactionScanQueryMatcher {

  public MajorCompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now) {
    super(scanInfo, deletes, readPointToUse, earliestPutTs, oldestUnexpiredTS, now);
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    long timestamp = cell.getTimestamp();
    long mvccVersion = cell.getSequenceId();

    // The delete logic is pretty complicated now.
    // This is corroborated by the following:
    // 1. The store might be instructed to keep deleted rows around.
    // 2. A scan can optionally see past a delete marker now.
    // 3. If deleted rows are kept, we have to find out when we can
    // remove the delete markers.
    // 4. Family delete markers are always first (regardless of their TS)
    // 5. Delete markers should not be counted as version
    // 6. Delete markers affect puts of the *same* TS
    // 7. Delete marker need to be version counted together with puts
    // they affect
    //
    if (CellUtil.isDelete(cell)) {
      if (mvccVersion > maxReadPointToTrackVersions) {
        // We can not drop this delete marker yet, and also we should not use this delete marker to
        // mask any cell yet.
        return MatchCode.INCLUDE;
      }
      trackDelete(cell);
      returnCode = tryDropDelete(cell);
      if (returnCode != null) {
        return returnCode;
      }
    } else {
      returnCode = checkDeleted(deletes, cell);
      if (returnCode != null) {
        return returnCode;
      }
    }
    // Skip checking column since we do not remove column during compaction.
    return columns.checkVersions(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength(), timestamp, cell.getTypeByte(),
      mvccVersion > maxReadPointToTrackVersions);
  }
}
