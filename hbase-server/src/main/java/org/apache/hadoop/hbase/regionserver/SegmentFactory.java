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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.yetus.audience.InterfaceAudience;

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

  // create composite immutable segment from a list of segments
  // for snapshot consisting of multiple segments
  public CompositeImmutableSegment createCompositeImmutableSegment(
      final CellComparator comparator, List<ImmutableSegment> segments) {
    return new CompositeImmutableSegment(comparator, segments);
  }

  // create new flat immutable segment from compacting old immutable segments
  // for compaction
  public ImmutableSegment createImmutableSegmentByCompaction(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
      CompactingMemStore.IndexType idxType, MemStoreCompactionStrategy.Action action)
      throws IOException {

    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return
        createImmutableSegment(
            conf,comparator,iterator,memStoreLAB,numOfCells,action,idxType);
  }

  /**
   * create empty immutable segment for initializations
   * This ImmutableSegment is used as a place holder for snapshot in Memstore.
   * It won't flush later, So it is not necessary to record the initial size
   * for it.
   * @param comparator comparator
   * @return ImmutableSegment
   */
  public ImmutableSegment createImmutableSegment(CellComparator comparator) {
    MutableSegment segment = generateMutableSegment(null, comparator, null, null);
    return createImmutableSegment(segment, null);
  }

  // create not-flat immutable segment from mutable segment
  public ImmutableSegment createImmutableSegment(MutableSegment segment,
      MemStoreSizing memstoreSizing) {
    return new CSLMImmutableSegment(segment, memstoreSizing);
  }

  // create mutable segment
  public MutableSegment createMutableSegment(final Configuration conf,
      CellComparator comparator, MemStoreSizing memstoreSizing) {
    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return generateMutableSegment(conf, comparator, memStoreLAB, memstoreSizing);
  }

  // create new flat immutable segment from merging old immutable segments
  // for merge
  public ImmutableSegment createImmutableSegmentByMerge(final Configuration conf,
      final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
      List<ImmutableSegment> segments, CompactingMemStore.IndexType idxType,
      MemStoreCompactionStrategy.Action action)
      throws IOException {

    MemStoreLAB memStoreLAB = getMergedMemStoreLAB(conf, segments);
    return
        createImmutableSegment(
            conf,comparator,iterator,memStoreLAB,numOfCells,action,idxType);

  }

  // create flat immutable segment from non-flat immutable segment
  // for flattening
  public ImmutableSegment createImmutableSegmentByFlattening(
      CSLMImmutableSegment segment, CompactingMemStore.IndexType idxType,
      MemStoreSizing memstoreSizing, MemStoreCompactionStrategy.Action action) {
    ImmutableSegment res = null;
    switch (idxType) {
      case CHUNK_MAP:
        res = new CellChunkImmutableSegment(segment, memstoreSizing, action);
        break;
      case CSLM_MAP:
        assert false; // non-flat segment can not be the result of flattening
        break;
      case ARRAY_MAP:
        res = new CellArrayImmutableSegment(segment, memstoreSizing, action);
        break;
    }
    return res;
  }


  //****** private methods to instantiate concrete store segments **********//
  private ImmutableSegment createImmutableSegment(final Configuration conf, final CellComparator comparator,
      MemStoreSegmentsIterator iterator, MemStoreLAB memStoreLAB, int numOfCells,
      MemStoreCompactionStrategy.Action action, CompactingMemStore.IndexType idxType) {

    ImmutableSegment res = null;
    switch (idxType) {
    case CHUNK_MAP:
      res = new CellChunkImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, action);
      break;
    case CSLM_MAP:
      assert false; // non-flat segment can not be created here
      break;
    case ARRAY_MAP:
      res = new CellArrayImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, action);
      break;
    }
    return res;
  }

  private MutableSegment generateMutableSegment(final Configuration conf, CellComparator comparator,
      MemStoreLAB memStoreLAB, MemStoreSizing memstoreSizing) {
    // TBD use configuration to set type of segment
    CellSet set = new CellSet(comparator);
    return new MutableSegment(set, comparator, memStoreLAB, memstoreSizing);
  }

  private MemStoreLAB getMergedMemStoreLAB(Configuration conf, List<ImmutableSegment> segments) {
    List<MemStoreLAB> mslabs = new ArrayList<>();
    if (!conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
      return null;
    }
    for (ImmutableSegment segment : segments) {
      mslabs.add(segment.getMemStoreLAB());
    }
    return new ImmutableMemStoreLAB(mslabs);
  }
}
