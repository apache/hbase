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

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A list of segment managers coupled with the version of the memstore (version at the time it was
 * created).
 * This structure helps to guarantee that the compaction pipeline updates after the compaction is
 * updated in a consistent (atomic) way.
 * Specifically, swapping some of the elements in a compaction pipeline with a new compacted
 * element is permitted only if the pipeline version is the same as the version attached to the
 * elements.
 *
 */
@InterfaceAudience.Private
public class VersionedSegmentsList {

  private final List<ImmutableSegment> storeSegments;
  private final long version;

  public VersionedSegmentsList(List<ImmutableSegment> storeSegments, long version) {
    this.storeSegments = storeSegments;
    this.version = version;
  }

  public List<ImmutableSegment> getStoreSegments() {
    return storeSegments;
  }

  public long getVersion() {
    return version;
  }

  public int getNumOfCells() {
    int totalCells = 0;
    for (ImmutableSegment s : storeSegments) {
      totalCells += s.getCellsCount();
    }
    return totalCells;
  }

  public int getNumOfSegments() {
    return storeSegments.size();
  }

  // Estimates fraction of unique keys
  @VisibleForTesting
  double getEstimatedUniquesFrac() {
    int segmentCells = 0;
    int maxCells = 0;
    double est = 0;

    for (ImmutableSegment s : storeSegments) {
      double segmentUniques = s.getNumUniqueKeys();
      if(segmentUniques != CellSet.UNKNOWN_NUM_UNIQUES) {
        segmentCells = s.getCellsCount();
        if(segmentCells > maxCells) {
          maxCells = segmentCells;
          est = segmentUniques / segmentCells;
        }
      }
      // else ignore this segment specifically since if the unique number is unknown counting
      // cells can be expensive
    }
    if(maxCells == 0) {
      return 1.0;
    }
    return est;
  }
}
