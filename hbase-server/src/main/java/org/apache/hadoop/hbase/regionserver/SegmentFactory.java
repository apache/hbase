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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A singleton store segment factory.
 * Generate concrete store segments.
 */
@InterfaceAudience.Private
public final class SegmentFactory {

  private SegmentFactory() {}
  private static SegmentFactory instance = new SegmentFactory();

  public static SegmentFactory instance() {
    return instance;
  }

  // create skip-list-based (non-flat) immutable segment from compacting old immutable segments
  public ImmutableSegment createImmutableSegment(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator) {
    return new ImmutableSegment(comparator, iterator, MemStoreLAB.newInstance(conf));
  }

  // create composite immutable segment from a list of segments
  public CompositeImmutableSegment createCompositeImmutableSegment(
      final CellComparator comparator, List<ImmutableSegment> segments) {
    return new CompositeImmutableSegment(comparator, segments);

  }

  // create new flat immutable segment from compacting old immutable segments
  public ImmutableSegment createImmutableSegmentByCompaction(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
      ImmutableSegment.Type segmentType)
      throws IOException {
    Preconditions.checkArgument(segmentType == ImmutableSegment.Type.ARRAY_MAP_BASED,
        "wrong immutable segment type");
    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return
        // the last parameter "false" means not to merge, but to compact the pipeline
        // in order to create the new segment
        new ImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, segmentType, false);
  }

  // create empty immutable segment
  public ImmutableSegment createImmutableSegment(CellComparator comparator) {
    MutableSegment segment = generateMutableSegment(null, comparator, null);
    return createImmutableSegment(segment);
  }

  // create immutable segment from mutable segment
  public ImmutableSegment createImmutableSegment(MutableSegment segment) {
    return new ImmutableSegment(segment);
  }

  // create mutable segment
  public MutableSegment createMutableSegment(final Configuration conf, CellComparator comparator) {
    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return generateMutableSegment(conf, comparator, memStoreLAB);
  }

  // create new flat immutable segment from merging old immutable segments
  public ImmutableSegment createImmutableSegmentByMerge(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
      ImmutableSegment.Type segmentType, List<ImmutableSegment> segments)
      throws IOException {
    Preconditions.checkArgument(segmentType == ImmutableSegment.Type.ARRAY_MAP_BASED,
        "wrong immutable segment type");
    MemStoreLAB memStoreLAB = getMergedMemStoreLAB(conf, segments);
    return
        // the last parameter "true" means to merge the compaction pipeline
        // in order to create the new segment
        new ImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, segmentType, true);
  }
  //****** private methods to instantiate concrete store segments **********//

  private MutableSegment generateMutableSegment(final Configuration conf, CellComparator comparator,
      MemStoreLAB memStoreLAB) {
    // TBD use configuration to set type of segment
    CellSet set = new CellSet(comparator);
    return new MutableSegment(set, comparator, memStoreLAB);
  }

  private MemStoreLAB getMergedMemStoreLAB(Configuration conf, List<ImmutableSegment> segments) {
    List<MemStoreLAB> mslabs = new ArrayList<MemStoreLAB>();
    if (!conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
      return null;
    }
    for (ImmutableSegment segment : segments) {
      mslabs.add(segment.getMemStoreLAB());
    }
    return new ImmutableMemStoreLAB(mslabs);
  }
}
