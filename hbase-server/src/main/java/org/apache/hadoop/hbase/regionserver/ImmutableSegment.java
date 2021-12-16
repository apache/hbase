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

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}.
 */
@InterfaceAudience.Private
public abstract class ImmutableSegment extends Segment {

  public static final long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD + ClassSize.NON_SYNC_TIMERANGE_TRACKER;

  // each sub-type of immutable segment knows whether it is flat or not
  protected abstract boolean canBeFlattened();

  public int getNumUniqueKeys() {
    return getCellSet().getNumUniqueKeys();
  }

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * Empty C-tor to be used only for CompositeImmutableSegment
   */
  protected ImmutableSegment(CellComparator comparator) {
    super(comparator, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
  }

  protected ImmutableSegment(CellComparator comparator, List<ImmutableSegment> segments) {
    super(comparator, segments, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
  }

  /**------------------------------------------------------------------------
   * C-tor to be used to build the derived classes
   */
  protected ImmutableSegment(CellSet cs, CellComparator comparator, MemStoreLAB memStoreLAB) {
    super(cs, comparator, memStoreLAB, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
  }

  /**------------------------------------------------------------------------
   * Copy C-tor to be used when new CSLMImmutableSegment (derived) is being built from a Mutable one.
   * This C-tor should be used when active MutableSegment is pushed into the compaction
   * pipeline and becomes an ImmutableSegment.
   */
  protected ImmutableSegment(Segment segment) {
    super(segment);
  }

  /////////////////////  PUBLIC METHODS  /////////////////////

  public int getNumOfSegments() {
    return 1;
  }

  public List<Segment> getAllSegments() {
    return Collections.singletonList(this);
  }

  @Override
  public String toString() {
    String res = super.toString();
    res += "Num uniques "+getNumUniqueKeys()+"; ";
    return res;
  }

  /**
   * We create a new {@link SnapshotSegmentScanner} to increase the reference count of
   * {@link MemStoreLABImpl} used by this segment.
   */
  List<KeyValueScanner> getSnapshotScanners() {
    return Collections.singletonList(new SnapshotSegmentScanner(this));
  }
}
