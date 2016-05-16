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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}. Specifically, the method
 * {@link ImmutableSegment#getKeyValueScanner()} builds a special scanner for the
 * {@link MemStoreSnapshot} object.
 */
@InterfaceAudience.Private
public class ImmutableSegment extends Segment {
  /**
   * This is an immutable segment so use the read-only TimeRange rather than the heavy-weight
   * TimeRangeTracker with all its synchronization when doing time range stuff.
   */
  private final TimeRange timeRange;

  protected ImmutableSegment(Segment segment) {
    super(segment);
    TimeRangeTracker trt = getTimeRangeTracker();
    this.timeRange =  trt == null? null: trt.toTimeRange();
  }

  /**
   * Builds a special scanner for the MemStoreSnapshot object that is different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public KeyValueScanner getKeyValueScanner() {
    return new CollectionBackedScanner(getCellSet(), getComparator());
  }

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return this.timeRange.includesTimeRange(scan.getTimeRange()) &&
        this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  @Override
  public long getMinTimestamp() {
    return this.timeRange.getMin();
  }

}