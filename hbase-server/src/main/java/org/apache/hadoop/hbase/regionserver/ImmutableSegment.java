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


import org.apache.hadoop.hbase.CellComparator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.io.TimeRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}.
 */
@InterfaceAudience.Private
public abstract class ImmutableSegment extends Segment {

  public static final long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD
      + ClassSize.align(ClassSize.REFERENCE // Referent to timeRange
      + ClassSize.TIMERANGE);

  /**
   * This is an immutable segment so use the read-only TimeRange rather than the heavy-weight
   * TimeRangeTracker with all its synchronization when doing time range stuff.
   */
  private final TimeRange timeRange;

  // each sub-type of immutable segment knows whether it is flat or not
  protected abstract boolean canBeFlattened();

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * Empty C-tor to be used only for CompositeImmutableSegment
   */
  protected ImmutableSegment(CellComparator comparator) {
    super(comparator);
    this.timeRange = null;
  }

  /**------------------------------------------------------------------------
   * C-tor to be used to build the derived classes
   */
  protected ImmutableSegment(CellSet cs, CellComparator comparator, MemStoreLAB memStoreLAB) {
    super(cs, comparator, memStoreLAB);
    this.timeRange = this.timeRangeTracker == null ? null : this.timeRangeTracker.toTimeRange();
  }

  /**------------------------------------------------------------------------
   * Copy C-tor to be used when new CSLMImmutableSegment (derived) is being built from a Mutable one.
   * This C-tor should be used when active MutableSegment is pushed into the compaction
   * pipeline and becomes an ImmutableSegment.
   */
  protected ImmutableSegment(Segment segment) {
    super(segment);
    this.timeRange = this.timeRangeTracker == null ? null : this.timeRangeTracker.toTimeRange();
  }


  /////////////////////  PUBLIC METHODS  /////////////////////
  @Override
  public boolean shouldSeek(TimeRange tr, long oldestUnexpiredTS) {
    return this.timeRange.includesTimeRange(tr) &&
        this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  @Override
  public long getMinTimestamp() {
    return this.timeRange.getMin();
  }

  public int getNumOfSegments() {
    return 1;
  }

  public List<Segment> getAllSegments() {
    List<Segment> res = new ArrayList<>(Arrays.asList(this));
    return res;
  }
}
